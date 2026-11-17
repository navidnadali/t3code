import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";

import * as KeyedLock from "./KeyedLock.ts";
import { CurrentWaitObserver, type WaitObserver } from "./WaitObserver.ts";

const recordingObserver = () => {
  const calls: Array<ReadonlyArray<unknown>> = [];
  const observer: WaitObserver = {
    wait: (...args) => void calls.push(["wait", ...args]),
    hold: (...args) => void calls.push(["hold", ...args]),
    depth: (...args) => void calls.push(["depth", ...args]),
    register: () => () => {},
    registerKeyed: () => () => {},
    sizeKeys: () => () => {},
  };
  return { calls, observer };
};

describe("KeyedLock", () => {
  it.effect("runs one holder of a key at a time, waiters in the order they queued", () =>
    Effect.gen(function* () {
      const lock = yield* KeyedLock.make<string>();
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const record = (event: string) => Ref.update(events, (current) => [...current, event]);
      const firstEntered = yield* Deferred.make<void>();
      const releaseFirst = yield* Deferred.make<void>();

      const first = yield* lock
        .withLock(
          "key",
          record("first:start").pipe(
            Effect.andThen(Deferred.succeed(firstEntered, undefined)),
            Effect.andThen(Deferred.await(releaseFirst)),
            Effect.andThen(record("first:end")),
          ),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(firstEntered);
      const second = yield* lock.withLock("key", record("second")).pipe(Effect.forkChild);
      const third = yield* lock.withLock("key", record("third")).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.deepStrictEqual(yield* Ref.get(events), ["first:start"]);

      yield* Deferred.succeed(releaseFirst, undefined);
      yield* Fiber.joinAll([first, second, third]);
      assert.deepStrictEqual(yield* Ref.get(events), [
        "first:start",
        "first:end",
        "second",
        "third",
      ]);
    }),
  );

  it.effect("runs holders of different keys concurrently", () =>
    Effect.gen(function* () {
      const lock = yield* KeyedLock.make<string>();
      const entered = yield* Queue.unbounded<string>();
      const release = yield* Deferred.make<void>();
      const hold = (key: string) =>
        lock.withLock(key, Queue.offer(entered, key).pipe(Effect.andThen(Deferred.await(release))));

      const holders = yield* Effect.forEach(["a", "b"], hold, { concurrency: "unbounded" }).pipe(
        Effect.forkChild,
      );
      const both = new Set([yield* Queue.take(entered), yield* Queue.take(entered)]);
      assert.deepStrictEqual(both, new Set(["a", "b"]));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holders);
    }),
  );

  it.effect("keeps no lock for a key once its holders and waiters are gone", () =>
    Effect.gen(function* () {
      const lock = yield* KeyedLock.make<string>();
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const holder = yield* lock
        .withLock(
          "key",
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const abandoned = yield* lock.withLock("key", Effect.void).pipe(Effect.forkChild);
      const waiting = yield* lock.withLock("key", Effect.void).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.deepStrictEqual(yield* lock.activeKeys, ["key"]);

      yield* Fiber.interrupt(abandoned);
      assert.deepStrictEqual(yield* lock.activeKeys, ["key"]);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.joinAll([holder, waiting]);
      assert.strictEqual(
        yield* lock.withLock("other", Effect.fail("boom")).pipe(Effect.flip),
        "boom",
      );

      assert.deepStrictEqual(yield* lock.activeKeys, []);
    }),
  );

  it.effect("reports each acquisition's wait with the holder's operation, and keyed holds", () => {
    const { calls, observer } = recordingObserver();
    return Effect.gen(function* () {
      const lock = yield* KeyedLock.make<string>({ site: "Probe.lock", rankKeys: true });
      const unnamed = yield* KeyedLock.make<string>();
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const holder = yield* lock
        .withLock(
          "key",
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        .pipe(Effect.withSpan("Holder.run"), Effect.forkChild);
      yield* Deferred.await(entered);
      // A waiter that gives up reports nothing.
      const abandoned = yield* lock.withLock("key", Effect.void).pipe(Effect.forkChild);
      const waiter = yield* lock
        .withLock("key", Effect.void)
        .pipe(Effect.withSpan("Waiter.run"), Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(abandoned);
      yield* TestClock.adjust("2 seconds");
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.joinAll([holder, waiter]);
      yield* unnamed.withLock("key", Effect.void);

      // A wait behind a holder also names its waiter's operation.
      assert.deepStrictEqual(calls, [
        ["wait", "Probe.lock", 0, undefined, "key", undefined],
        ["hold", "Probe.lock", "key", 2e9],
        ["wait", "Probe.lock", 2e9, "Holder.run", "key", "Waiter.run"],
        ["hold", "Probe.lock", "key", 0],
      ]);
    }).pipe(Effect.provideService(CurrentWaitObserver, observer));
  });

  it.effect("reports a reused lock effect's hold only for a run that took the key", () => {
    const { calls, observer } = recordingObserver();
    return Effect.gen(function* () {
      const lock = yield* KeyedLock.make<string>({ site: "Probe.lock", rankKeys: true });
      const locked = lock.withLock("key", Effect.void);
      yield* locked;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holder = yield* lock
        .withLock(
          "key",
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      // The same effect run again gives up before it takes the key.
      const abandoned = yield* locked.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* Fiber.interrupt(abandoned);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holder);
      assert.strictEqual(calls.filter(([kind]) => kind === "hold").length, 2);
    }).pipe(Effect.provideService(CurrentWaitObserver, observer));
  });

  it.effect("runner keeps each key's effects in run order and never holds up another key", () =>
    Effect.gen(function* () {
      const runner = yield* KeyedLock.makeRunner<string>();
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const record = (event: string) => Ref.update(events, (current) => [...current, event]);
      const release = yield* Deferred.make<void>();
      const otherRan = yield* Deferred.make<void>();
      const busyRan = yield* Deferred.make<void>();

      yield* runner.run("busy", Effect.andThen(Deferred.await(release), record("busy:1")));
      yield* runner.run("busy", record("busy:2"));
      yield* runner.run(
        "busy",
        Effect.andThen(record("busy:3"), Deferred.succeed(busyRan, undefined)).pipe(Effect.asVoid),
      );
      yield* runner.run(
        "other",
        Effect.andThen(record("other"), Deferred.succeed(otherRan, undefined)).pipe(Effect.asVoid),
      );
      yield* Deferred.await(otherRan);
      assert.deepStrictEqual(yield* Ref.get(events), ["other"]);

      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(busyRan);
      assert.deepStrictEqual(yield* Ref.get(events), ["other", "busy:1", "busy:2", "busy:3"]);
    }),
  );

  it.effect("runs the effect in the caller's scope", () =>
    Effect.gen(function* () {
      const lock = yield* KeyedLock.make<string>();
      const finalized = yield* Ref.make(false);
      const callerScope = yield* Scope.make();

      yield* lock
        .withLock(
          "key",
          Effect.addFinalizer(() => Ref.set(finalized, true)),
        )
        .pipe(Scope.provide(callerScope));
      assert.isFalse(yield* Ref.get(finalized));
      yield* Scope.close(callerScope, Exit.void);
      assert.isTrue(yield* Ref.get(finalized));
    }),
  );
});

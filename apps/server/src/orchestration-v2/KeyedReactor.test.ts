import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";

import { makeKeyedReactor } from "./KeyedReactor.ts";

interface Item {
  readonly key: string;
  readonly value: number;
}

const settled = (latch: Deferred.Deferred<void>) => Deferred.await(latch);

it.effect("runs each key's items in order and frees the key once its items are done", () =>
  Effect.gen(function* () {
    const seen = yield* Ref.make<ReadonlyArray<Item>>([]);
    const done = yield* Deferred.make<void>();
    const reactor = yield* makeKeyedReactor<string, Item>({
      concurrency: 2,
      handle: (item) =>
        Ref.updateAndGet(seen, (items) => [...items, item]).pipe(
          Effect.flatMap((items) =>
            items.length === 6 ? Deferred.succeed(done, undefined) : Effect.void,
          ),
        ),
    });
    for (const item of [
      { key: "a", value: 1 },
      { key: "b", value: 1 },
      { key: "a", value: 2 },
      { key: "c", value: 1 },
      { key: "a", value: 3 },
      { key: "b", value: 2 },
    ]) {
      yield* reactor.offer(item.key, item);
    }
    yield* settled(done);
    yield* reactor.whenIdle;
    const items = yield* Ref.get(seen);
    for (const key of ["a", "b", "c"]) {
      const values = items.filter((item) => item.key === key).map((item) => item.value);
      assert.deepEqual(
        values,
        values.toSorted((left, right) => left - right),
        key,
      );
    }
    assert.equal(yield* reactor.pendingKeys, 0);
  }),
);

it.effect("runs at most its concurrency of keys, and a key waiting never holds up another", () =>
  Effect.gen(function* () {
    const releaseBusy = yield* Deferred.make<void>();
    const busyStarted = yield* Deferred.make<void>();
    const running = yield* Ref.make(0);
    const peak = yield* Ref.make(0);
    const finished = yield* Ref.make<ReadonlyArray<string>>([]);
    const othersDone = yield* Deferred.make<void>();
    const reactor = yield* makeKeyedReactor<string, Item>({
      concurrency: 2,
      handle: (item) =>
        Effect.gen(function* () {
          const now = yield* Ref.updateAndGet(running, (count) => count + 1);
          yield* Ref.update(peak, (max) => Math.max(max, now));
          if (item.key === "busy") {
            yield* Deferred.succeed(busyStarted, undefined);
            yield* Deferred.await(releaseBusy);
          }
          yield* Ref.update(running, (count) => count - 1);
          const all = yield* Ref.updateAndGet(finished, (keys) => [
            ...keys,
            `${item.key}:${item.value}`,
          ]);
          if (all.filter((key) => !key.startsWith("busy")).length === 20) {
            yield* Deferred.succeed(othersDone, undefined);
          }
        }),
    });
    // One key with a burst of items waits on its first; twenty other keys still get through.
    for (let value = 0; value < 50; value += 1) {
      yield* reactor.offer("busy", { key: "busy", value });
    }
    yield* Deferred.await(busyStarted);
    for (let index = 0; index < 20; index += 1) {
      yield* reactor.offer(`other-${index}`, { key: `other-${index}`, value: 0 });
    }
    yield* Deferred.await(othersDone);
    assert.isFalse((yield* Ref.get(finished)).some((key) => key.startsWith("busy")));
    assert.equal(yield* Ref.get(peak), 2);
    yield* Deferred.succeed(releaseBusy, undefined);
    yield* reactor.whenIdle;
    assert.deepEqual(
      (yield* Ref.get(finished)).filter((key) => key.startsWith("busy")),
      Array.from({ length: 50 }, (_, value) => `busy:${value}`),
    );
    assert.equal(yield* reactor.pendingKeys, 0);
  }),
);

it.effect("keeps reacting after an item fails or dies", () =>
  Effect.gen(function* () {
    const handled = yield* Ref.make<ReadonlyArray<number>>([]);
    const done = yield* Deferred.make<void>();
    const reactor = yield* makeKeyedReactor<string, Item, string>({
      concurrency: 1,
      handle: (item) =>
        item.value === 1
          ? Effect.fail("failed")
          : item.value === 2
            ? Effect.die("died")
            : Ref.update(handled, (values) => [...values, item.value]).pipe(
                Effect.andThen(item.value === 4 ? Deferred.succeed(done, undefined) : Effect.void),
              ),
    });
    for (const value of [0, 1, 2, 3, 4]) {
      yield* reactor.offer("key", { key: "key", value });
    }
    yield* Deferred.await(done);
    yield* reactor.whenIdle;
    assert.deepEqual(yield* Ref.get(handled), [0, 3, 4]);
    assert.equal(yield* reactor.pendingKeys, 0);
  }),
);

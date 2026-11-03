import { assert, it } from "@effect/vitest";
import {
  ThreadId,
  type ApplicationStoredEvent,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Scheduler from "effect/Scheduler";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ShellDeltaHub from "./ShellDeltaHub.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

const threadId = ThreadId.make("thread:hub");
const shell = {
  id: threadId,
  archivedAt: null,
  title: "a",
} as unknown as OrchestrationV2ThreadShell;

// The hub reads nothing but the sequence and thread of a live event.
const threadEvent = (id: ThreadId, sequence: number) =>
  ({
    sequence,
    event: { type: "provider-session.detached", threadId: id },
  }) as unknown as ApplicationStoredEvent;

const layerStore = OrchestrationEventStore.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

// The hub over mocked shell reads (`title()` names the shell read); `onStart` runs when it
// subscribes to committed events.
const layerHub = (input: {
  readonly reads: Array<number>;
  readonly title?: () => string;
  /** The title a thread's shell read returns (over `title`). */
  readonly titleOf?: (id: string) => string;
  /** The threads a base read returns, taken when the read completes. */
  readonly baseThreads?: () => ReadonlyArray<OrchestrationV2ThreadShell>;
  /** The store's latest sequence as the hub reads it (the real one, 0 with no rows, otherwise). */
  readonly latestSequence?: () => Effect.Effect<number>;
  readonly onStart?: () => void;
  readonly afterSubscribe?: Effect.Effect<void>;
  readonly onRelease?: () => void;
  /** Holds every base read for a resync until it completes. */
  readonly readGate?: Effect.Effect<void>;
}) =>
  ShellDeltaHub.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          listActiveSubagentThreadIds: () => Effect.succeed([]),
          readShellSnapshot: () =>
            (input.readGate ?? Effect.void).pipe(
              Effect.map(() =>
                Effect.succeed({
                  schemaVersion: 1,
                  snapshotSequence: 0,
                  threads: input.baseThreads?.() ?? [],
                  archivedThreads: [],
                }),
              ),
            ),
          getThreadShellForEvent: ({ threadId: id, sequence }) =>
            Effect.sync(() => {
              input.reads.push(sequence);
              return { ...shell, id, title: input.titleOf?.(id) ?? input.title?.() ?? shell.title };
            }),
        }),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed([]) }),
        Layer.effect(
          OrchestrationEventStore.OrchestrationEventStore,
          Effect.gen(function* () {
            const store = yield* OrchestrationEventStore.OrchestrationEventStore;
            const latestSequence = input.latestSequence;
            return {
              ...store,
              ...(latestSequence === undefined
                ? {}
                : { latestApplicationSequence: Effect.suspend(latestSequence) }),
              subscribeCommitted: store.subscribeCommitted.pipe(
                Effect.tap(() => Effect.sync(() => input.onStart?.())),
                Effect.tap(() => Effect.addFinalizer(() => Effect.sync(() => input.onRelease?.()))),
                Effect.tap(() => input.afterSubscribe ?? Effect.void),
              ),
            };
          }),
        ),
      ),
    ),
    Layer.provideMerge(layerStore),
  );

const shellOf = (id: string, title: string) =>
  ({ id, archivedAt: null, title }) as unknown as OrchestrationV2ThreadShell;

/** `<thread>@<sequence>=<title>` per delta; a resync lists its base's threads. */
const describe = (items: ReadonlyArray<ShellDeltaHub.ShellSubscriptionItem>) =>
  items.map((item) =>
    item.kind === "resync"
      ? `resync(${item.base.threads.map((thread) => `${thread.id}=${thread.title}`).join(",")})`
      : item.kind === "thread.updated"
        ? `${item.thread.id}@${item.sequence}=${item.thread.title}`
        : `${item.kind}@${item.sequence}`,
  );

const publish = (id: ThreadId, sequence: number) =>
  Effect.gen(function* () {
    const store = yield* OrchestrationEventStore.OrchestrationEventStore;
    yield* store.publishCommitted([threadEvent(id, sequence)]);
    yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
  });

it.effect("deltas buffered before the stream is taken keep the latest per aggregate", () =>
  Effect.gen(function* () {
    const reads: Array<number> = [];
    yield* Effect.gen(function* () {
      const hub = yield* ShellDeltaHub.ShellDeltaHub;
      const subscription = yield* hub.subscribe();
      for (let sequence = 1; sequence <= 5; sequence++) yield* publish(threadId, sequence);
      const items = yield* subscription.stream(0).pipe(Stream.take(1), Stream.runCollect);
      assert.deepEqual(
        items.flatMap((item) => (item.kind === "resync" ? [] : [item.sequence])),
        [5],
      );
      assert.deepEqual(reads, [1, 2, 3, 4, 5]);
    }).pipe(Effect.provide(layerHub({ reads })));
  }).pipe(Effect.scoped),
);

it.effect("an overflowing subscription empties its buffer and resyncs on its next pull", () =>
  Effect.gen(function* () {
    const reads: Array<number> = [];
    let starts = 0;
    yield* Effect.gen(function* () {
      const hub = yield* ShellDeltaHub.ShellDeltaHub;
      const scope = yield* Scope.make();
      const stalled = yield* hub.subscribe({ maxItems: 1 }).pipe(Scope.provide(scope));
      // One delta delivered and never acknowledged; the next one would overflow the budget.
      const pull = yield* Stream.toPull(stalled.stream(0)).pipe(Scope.provide(scope));
      const first = yield* Effect.forkScoped(pull);
      yield* publish(threadId, 1);
      assert.deepEqual(
        (yield* Fiber.join(first)).flatMap((item) =>
          item.kind === "resync" ? [] : [item.sequence],
        ),
        [1],
      );
      yield* publish(ThreadId.make("thread:other"), 2);
      yield* TestClock.adjust(0);
      // The subscriber keeps counting: another one joins the running hub.
      yield* hub.subscribe().pipe(Scope.provide(scope));
      yield* TestClock.adjust(0);
      assert.equal(starts, 1);
      // Its next pull brings the base, read after the reset, which covers what was dropped;
      // deltas after it are delivered as before.
      assert.deepEqual(
        (yield* pull).map((item) => item.kind),
        ["resync"],
      );
      // (An unchanged shell is skipped as before; the resync covered `thread:other`.)
      yield* publish(ThreadId.make("thread:third"), 3);
      assert.deepEqual(
        (yield* pull).flatMap((item) => (item.kind === "resync" ? [] : [item.sequence])),
        [3],
      );
      yield* Scope.close(scope, Exit.void);
    }).pipe(
      Effect.provide(
        layerHub({
          reads,
          onStart: () => {
            starts += 1;
          },
        }),
      ),
    );
  }).pipe(Effect.scoped),
);

const sequences = (items: ReadonlyArray<ShellDeltaHub.ShellSubscriptionItem>) =>
  items.flatMap((item) => (item.kind === "resync" ? [] : [item.sequence]));

it.effect("deltas offered while the base is read follow the resync", () =>
  Effect.gen(function* () {
    const reads: Array<number> = [];
    const gate = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const hub = yield* ShellDeltaHub.ShellDeltaHub;
      const scope = yield* Scope.make();
      const stalled = yield* hub.subscribe({ maxItems: 3 }).pipe(Scope.provide(scope));
      const pull = yield* Stream.toPull(stalled.stream(0)).pipe(Scope.provide(scope));
      const first = yield* Effect.forkScoped(pull);
      yield* publish(threadId, 1);
      assert.deepEqual(sequences(yield* Fiber.join(first)), [1]);
      // One delivered and two buffered fill the budget; the fourth empties the buffer.
      yield* publish(ThreadId.make("thread:b"), 2);
      yield* publish(ThreadId.make("thread:c"), 3);
      yield* publish(ThreadId.make("thread:d"), 4);
      const resync = yield* Effect.forkScoped(pull);
      yield* TestClock.adjust(0);
      // Committed while the base is read: it goes into the emptied buffer, admitted once.
      yield* publish(ThreadId.make("thread:e"), 5);
      yield* Deferred.succeed(gate, undefined);
      assert.deepEqual(
        (yield* Fiber.join(resync)).map((item) => item.kind),
        ["resync"],
      );
      // What the base does not cover is still buffered, so the next pull has it at once.
      const next = yield* Effect.forkScoped(pull.pipe(Effect.timeoutOption("1 second")));
      yield* TestClock.adjust("1 second");
      assert.deepEqual(Option.map(yield* Fiber.join(next), sequences), Option.some([4, 5]));
      yield* Scope.close(scope, Exit.void);
    }).pipe(Effect.provide(layerHub({ reads, readGate: Deferred.await(gate) })));
  }).pipe(Effect.scoped),
);

/**
 * A scheduler like `base` that parks a fiber `target` accepts once, at the `at`-th op those
 * fibers run between them, and never again.
 */
const yieldOnceAt = (
  base: Scheduler.Scheduler,
  at: number,
  target: (fiber: Fiber.Fiber<unknown, unknown>) => boolean,
): Scheduler.Scheduler => {
  let ops = 0;
  return {
    executionMode: base.executionMode,
    shouldYield: (fiber) => target(fiber) && ++ops === at,
    makeDispatcher: () => base.makeDispatcher(),
  };
};

it.effect("a subscriber cancelled as it starts the shared base read leaves no read behind", () =>
  Effect.gen(function* () {
    const base = yield* Scheduler.Scheduler;
    // The pull that starts the read runs it in a fiber of its own; that fiber is parked at its
    // n-th op and the pull cancelled there, for every n from before the read is published to the
    // others until after it started: the other subscriber the same batch reset must get its
    // resync, from that read or its own, every time.
    for (let at = 1; at <= 200; at++) {
      const reads: Array<number> = [];
      yield* Effect.gen(function* () {
        const hub = yield* ShellDeltaHub.ShellDeltaHub;
        const scope = yield* Scope.make();
        const starter = yield* hub.subscribe({ maxItems: 1 }).pipe(Scope.provide(scope));
        const other = yield* hub.subscribe({ maxItems: 1 }).pipe(Scope.provide(scope));
        // Armed for the pull that starts the read; the stream's own fibers run under it.
        let armed = false;
        const parkInside = yieldOnceAt(base, at, () => armed);
        const pullStarter = yield* Stream.toPull(
          starter.stream(0).pipe(Stream.provideService(Scheduler.Scheduler, parkInside)),
        ).pipe(Effect.provideService(Scheduler.Scheduler, parkInside), Scope.provide(scope));
        const pullOther = yield* Stream.toPull(other.stream(0)).pipe(Scope.provide(scope));
        yield* publish(threadId, 1);
        assert.deepEqual(sequences(yield* pullStarter), [1]);
        assert.deepEqual(sequences(yield* pullOther), [1]);
        // The delivered delta is still charged: one more resets both.
        yield* publish(ThreadId.make("thread:b"), 2);
        armed = true;
        const starting = yield* Effect.forkScoped(pullStarter);
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(starting);
        const resync = yield* Effect.forkScoped(pullOther.pipe(Effect.timeoutOption("1 second")));
        yield* TestClock.adjust("1 second");
        assert.deepEqual(
          Option.map(yield* Fiber.join(resync), (items) => items.map((item) => item.kind)),
          Option.some(["resync"]),
          `cancelled at op ${at}`,
        );
        yield* Scope.close(scope, Exit.void);
      }).pipe(Effect.provide(layerHub({ reads })), Effect.scoped);
    }
  }).pipe(Effect.scoped),
);

it.effect(
  "a resync forgets which shells were sent: a base that saw a transient shell is not left stale",
  () =>
    Effect.gen(function* () {
      const reads: Array<number> = [];
      const titles = new Map<string, string>([[threadId, "X"]]);
      let base: ReadonlyArray<OrchestrationV2ThreadShell> = [];
      yield* Effect.gen(function* () {
        const hub = yield* ShellDeltaHub.ShellDeltaHub;
        const scope = yield* Scope.make();
        const subscription = yield* hub.subscribe({ maxItems: 2 }).pipe(Scope.provide(scope));
        const pull = yield* Stream.toPull(subscription.stream(0)).pipe(Scope.provide(scope));
        const first = yield* Effect.forkScoped(pull);
        yield* publish(threadId, 1);
        assert.deepEqual(describe(yield* Fiber.join(first)), ["thread:hub@1=X"]);
        // Two more distinct threads while the first chunk is unacknowledged: the buffer resets.
        titles.set("thread:b", "b");
        titles.set("thread:c", "c");
        yield* publish(ThreadId.make("thread:b"), 2);
        yield* publish(ThreadId.make("thread:c"), 3);
        // The base catches the thread in a state the hub never projected (X -> Y -> X within one
        // batch window, say a runtime request opened and resolved).
        base = [shellOf(threadId, "Y"), shellOf("thread:b", "b"), shellOf("thread:c", "c")];
        assert.deepEqual(describe(yield* pull), ["resync(thread:hub=Y,thread:b=b,thread:c=c)"]);
        assert.deepEqual(describe(yield* pull), ["thread:c@3=c"]);
        // The thread's next event projects X again, the shell last sent before the reset: the
        // client shows Y now, so it must get it.
        yield* publish(threadId, 4);
        const next = yield* Effect.forkScoped(pull.pipe(Effect.timeoutOption("1 second")));
        yield* TestClock.adjust("1 second");
        assert.deepEqual(
          Option.map(yield* Fiber.join(next), describe),
          Option.some(["thread:hub@4=X"]),
        );
        yield* Scope.close(scope, Exit.void);
      }).pipe(
        Effect.provide(
          layerHub({ reads, titleOf: (id) => titles.get(id) ?? "?", baseThreads: () => base }),
        ),
      );
    }).pipe(Effect.scoped),
);

it.effect(
  "the base is one read: a delta from between its parts cannot take a shell backwards",
  () =>
    Effect.gen(function* () {
      const reads: Array<number> = [];
      const titles = new Map<string, string>();
      let sequence = 0;
      let betweenReads: Effect.Effect<void> = Effect.void;
      yield* Effect.gen(function* () {
        const hub = yield* ShellDeltaHub.ShellDeltaHub;
        const store = yield* OrchestrationEventStore.OrchestrationEventStore;
        // A commit as the hub sees it: the latest sequence, the thread's shell, the committed event.
        const commit = (id: ThreadId, title: string, projected: boolean) =>
          Effect.gen(function* () {
            sequence += 1;
            titles.set(id, title);
            yield* store.publishCommitted([threadEvent(id, sequence)]);
            if (projected) yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
          });
        const scope = yield* Scope.make();
        const subscription = yield* hub.subscribe({ maxItems: 2 }).pipe(Scope.provide(scope));
        const pull = yield* Stream.toPull(subscription.stream(0)).pipe(Scope.provide(scope));
        const first = yield* Effect.forkScoped(pull);
        yield* commit(threadId, "e0", true);
        const log = describe(yield* Fiber.join(first));
        yield* commit(ThreadId.make("thread:b"), "b", true);
        yield* commit(ThreadId.make("thread:c"), "c", true);
        // Right after the base's sequence read: one event on the thread is committed and projected,
        // the next committed. A base read in parts would read the thread's shell after both.
        betweenReads = Effect.gen(function* () {
          betweenReads = Effect.void;
          yield* commit(threadId, "e1", true);
          yield* commit(threadId, "e2", false);
        });
        log.push(...describe(yield* pull));
        // What the buffer holds comes at once; the event projected after it, a window later.
        for (let round = 0; round < 2; round++) {
          const next = yield* Effect.forkScoped(pull.pipe(Effect.timeoutOption("1 second")));
          yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
          yield* TestClock.adjust("1 second");
          log.push(...Option.getOrElse(Option.map(yield* Fiber.join(next), describe), () => []));
        }
        // The thread as the client shows it after each item never goes back.
        const shown = log.flatMap((entry) => {
          const inBase = entry.startsWith("resync(") ? entry.match(/thread:hub=(\w+)/) : null;
          return inBase
            ? [inBase[1]!]
            : entry.startsWith("thread:hub@")
              ? [entry.split("=")[1]!]
              : [];
        });
        assert.isTrue(
          shown.every((value, index) => index === 0 || value >= shown[index - 1]!),
          `thread:hub went backwards: ${shown.join(" -> ")} (${log.join(", ")})`,
        );
        assert.equal(shown.at(-1), "e2");
        yield* Scope.close(scope, Exit.void);
      }).pipe(
        Effect.provide(
          layerHub({
            reads,
            titleOf: (id) => titles.get(id) ?? "?",
            baseThreads: () => Array.from(titles, ([id, title]) => shellOf(id, title)),
            latestSequence: () => {
              const value = sequence;
              return betweenReads.pipe(Effect.as(value));
            },
          }),
        ),
      );
    }).pipe(Effect.scoped),
);

it.effect(
  "a stalled client whose delivered chunk fills the budget: one reset warning, not one per delta",
  () =>
    Effect.gen(function* () {
      const reads: Array<number> = [];
      let resetWarnings = 0;
      const logger = Logger.make(({ message }) => {
        if (String(message).includes("live buffer reset for a resync")) resetWarnings += 1;
      });
      yield* Effect.gen(function* () {
        const hub = yield* ShellDeltaHub.ShellDeltaHub;
        const scope = yield* Scope.make();
        const subscription = yield* hub.subscribe({ maxItems: 4 }).pipe(Scope.provide(scope));
        const pull = yield* Stream.toPull(subscription.stream(0)).pipe(Scope.provide(scope));
        // Four deltas buffered and delivered in one pull: the unacknowledged chunk fills the budget.
        for (let index = 1; index <= 4; index++)
          yield* publish(ThreadId.make(`thread:f${index}`), index);
        assert.equal((yield* pull).length, 4);
        // The client never acknowledges; every further delta finds the buffer full.
        for (let index = 5; index <= 54; index++) {
          yield* publish(ThreadId.make(`thread:g${index}`), index);
        }
        assert.equal(resetWarnings, 1);
        yield* Scope.close(scope, Exit.void);
      }).pipe(
        Effect.provide(layerHub({ reads })),
        Effect.provide(Logger.layer([logger], { mergeWithExisting: true })),
      );
    }).pipe(Effect.scoped),
);

it.effect("a reset the opening snapshot covers needs no resync", () =>
  Effect.gen(function* () {
    const reads: Array<number> = [];
    yield* Effect.gen(function* () {
      const hub = yield* ShellDeltaHub.ShellDeltaHub;
      const scope = yield* Scope.make();
      const held = yield* hub.subscribe({ maxItems: 2 }).pipe(Scope.provide(scope));
      const other = yield* hub.subscribe({ maxItems: 1 }).pipe(Scope.provide(scope));
      const otherPull = yield* Stream.toPull(other.stream(0)).pipe(Scope.provide(scope));
      yield* publish(ThreadId.make("thread:a"), 1);
      assert.deepEqual(sequences(yield* otherPull), [1]);
      yield* publish(ThreadId.make("thread:b"), 2);
      // The third held delta empties `held`; it also resets `other`, whose resync reads a base
      // that covers sequence 3 (in this mock, one behind every sequence).
      yield* publish(ThreadId.make("thread:c"), 3);
      assert.deepEqual(
        (yield* otherPull).map((item) => item.kind),
        ["resync"],
      );
      // The snapshot `held` opens with was read at sequence 3: it covers what the reset dropped,
      // so the stream continues from it instead of resyncing to the base behind it.
      const pull = yield* Stream.toPull(held.stream(3)).pipe(Scope.provide(scope));
      const first = yield* Effect.forkScoped(pull);
      yield* TestClock.adjust(0);
      yield* publish(ThreadId.make("thread:d"), 4);
      assert.deepEqual(
        (yield* Fiber.join(first)).map((item) =>
          item.kind === "resync" ? "resync" : item.sequence,
        ),
        [4],
      );
      yield* Scope.close(scope, Exit.void);
    }).pipe(Effect.provide(layerHub({ reads })));
  }).pipe(Effect.scoped),
);

it.effect(
  "unchanged shells are skipped before the budget, so an unacknowledged batch cannot overflow on them",
  () =>
    Effect.gen(function* () {
      const reads: Array<number> = [];
      let title = "a";
      const other = ThreadId.make("thread:other");
      yield* Effect.gen(function* () {
        const hub = yield* ShellDeltaHub.ShellDeltaHub;
        const subscription = yield* hub.subscribe({ maxItems: 3 });
        const pull = yield* Stream.toPull(subscription.stream(0));
        yield* publish(threadId, 1);
        yield* publish(other, 2);
        // Delivered and charged until the next pull acknowledges them.
        assert.deepEqual(
          (yield* pull).flatMap((item) => (item.kind === "resync" ? [] : [item.sequence])),
          [1, 2],
        );
        yield* publish(threadId, 3);
        yield* publish(other, 4);
        title = "b";
        yield* publish(threadId, 5);
        const next = yield* pull;
        assert.deepEqual(
          next.flatMap((item) =>
            item.kind === "resync"
              ? []
              : [[item.sequence, item.kind === "thread.updated" ? item.thread.title : null]],
          ),
          [[5, "b"]],
        );
        assert.deepEqual(reads, [1, 2, 3, 4, 5]);
      }).pipe(Effect.provide(layerHub({ reads, title: () => title })));
    }).pipe(Effect.scoped),
);

it.effect("deltas held before the stream is taken count against the budget", () =>
  Effect.gen(function* () {
    const reads: Array<number> = [];
    let starts = 0;
    yield* Effect.gen(function* () {
      const hub = yield* ShellDeltaHub.ShellDeltaHub;
      const scope = yield* Scope.make();
      const stalled = yield* hub.subscribe({ maxItems: 2 }).pipe(Scope.provide(scope));
      for (const [index, id] of ["thread:a", "thread:b", "thread:c"].entries()) {
        yield* publish(ThreadId.make(id), index + 1);
      }
      yield* TestClock.adjust(0);
      yield* hub.subscribe().pipe(Scope.provide(scope));
      yield* TestClock.adjust(0);
      assert.equal(starts, 1);
      // The third delta emptied the buffer: the stream opens with a resync.
      const pull = yield* Stream.toPull(stalled.stream(0)).pipe(Scope.provide(scope));
      assert.deepEqual(
        (yield* pull).map((item) => item.kind),
        ["resync"],
      );
      yield* Scope.close(scope, Exit.void);
    }).pipe(
      Effect.provide(
        layerHub({
          reads,
          onStart: () => {
            starts += 1;
          },
        }),
      ),
    );
  }).pipe(Effect.scoped),
);

it.effect("held deltas the snapshot covers are released when the stream starts", () =>
  Effect.gen(function* () {
    const reads: Array<number> = [];
    yield* Effect.gen(function* () {
      const hub = yield* ShellDeltaHub.ShellDeltaHub;
      const subscription = yield* hub.subscribe({ maxItems: 2 });
      yield* publish(ThreadId.make("thread:a"), 1);
      yield* publish(ThreadId.make("thread:b"), 2);
      // The snapshot read at sequence 2 covers both: starting the stream frees their budget.
      const first = yield* Effect.forkScoped(
        subscription.stream(2).pipe(Stream.take(1), Stream.runCollect),
      );
      yield* TestClock.adjust(0);
      yield* publish(ThreadId.make("thread:c"), 3);
      assert.deepEqual(
        (yield* Fiber.join(first)).flatMap((item) =>
          item.kind === "resync" ? [] : [item.sequence],
        ),
        [3],
      );
    }).pipe(Effect.provide(layerHub({ reads })));
  }).pipe(Effect.scoped),
);

it.effect(
  "a subscriber cancelled while the hub starts leaves no hub and no committed subscription behind",
  () =>
    Effect.gen(function* () {
      const reads: Array<number> = [];
      let starts = 0;
      let releases = 0;
      const subscribing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const hub = yield* ShellDeltaHub.ShellDeltaHub;
        const subscriber = yield* Effect.forkScoped(
          Effect.scoped(hub.subscribe().pipe(Effect.andThen(Effect.never))),
        );
        yield* Deferred.await(subscribing);
        yield* Fiber.interrupt(subscriber).pipe(Effect.forkScoped);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.await(subscriber);
        yield* TestClock.adjust(0);
        assert.equal(starts, 1);
        assert.equal(releases, 1);
        // Nothing is left: the next subscriber starts a fresh hub.
        yield* Effect.scoped(hub.subscribe());
        yield* TestClock.adjust(0);
        assert.equal(starts, 2);
        assert.equal(releases, 2);
      }).pipe(
        Effect.provide(
          layerHub({
            reads,
            onStart: () => {
              starts += 1;
            },
            // Holds the start between acquiring the committed subscription and forking the consumer.
            afterSubscribe: Deferred.succeed(subscribing, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
            ),
            onRelease: () => {
              releases += 1;
            },
          }),
        ),
      );
    }).pipe(Effect.scoped),
);

it.effect("a subscriber whose scope closes while the hub starts leaves no hub behind", () =>
  Effect.gen(function* () {
    const reads: Array<number> = [];
    let starts = 0;
    let releases = 0;
    const subscribing = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const hub = yield* ShellDeltaHub.ShellDeltaHub;
      const scope = yield* Scope.make();
      const subscriber = yield* Effect.forkScoped(hub.subscribe().pipe(Scope.provide(scope)));
      yield* Deferred.await(subscribing);
      yield* Scope.close(scope, Exit.void);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.await(subscriber);
      yield* TestClock.adjust(0);
      assert.equal(starts, 1);
      assert.equal(releases, 1);
      yield* Effect.scoped(hub.subscribe());
      yield* TestClock.adjust(0);
      assert.equal(starts, 2);
      assert.equal(releases, 2);
    }).pipe(
      Effect.provide(
        layerHub({
          reads,
          onStart: () => {
            starts += 1;
          },
          afterSubscribe: Deferred.succeed(subscribing, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
          ),
          onRelease: () => {
            releases += 1;
          },
        }),
      ),
    );
  }).pipe(Effect.scoped),
);

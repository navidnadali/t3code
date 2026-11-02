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
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectService from "../project/ProjectService.ts";
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
  readonly onStart?: () => void;
  readonly afterSubscribe?: Effect.Effect<void>;
  readonly onRelease?: () => void;
}) =>
  ShellDeltaHub.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          listActiveSubagentThreadIds: () => Effect.succeed([]),
          readShellSnapshot: () =>
            Effect.succeed(
              Effect.succeed({
                schemaVersion: 1,
                snapshotSequence: 0,
                threads: [],
                archivedThreads: [],
              }),
            ),
          getThreadShellForEvent: ({ threadId: id, sequence }) =>
            Effect.sync(() => {
              input.reads.push(sequence);
              return { ...shell, id, title: input.title?.() ?? shell.title };
            }),
        }),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.effect(
          OrchestrationEventStore.OrchestrationEventStore,
          Effect.gen(function* () {
            const store = yield* OrchestrationEventStore.OrchestrationEventStore;
            return {
              ...store,
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

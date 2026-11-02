import { assert, it } from "@effect/vitest";
import {
  ThreadId,
  type ApplicationStoredEvent,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
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
const shell = { id: threadId, archivedAt: null } as unknown as OrchestrationV2ThreadShell;

// The hub reads nothing but the sequence and thread of a live event.
const threadEvent = (id: ThreadId, sequence: number) =>
  ({
    sequence,
    event: { type: "provider-session.detached", threadId: id },
  }) as unknown as ApplicationStoredEvent;

const layerStore = OrchestrationEventStore.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

// The hub over mocked shell reads; `onStart` runs when it subscribes to committed events.
const layerHub = (input: { readonly reads: Array<number>; readonly onStart?: () => void }) =>
  ShellDeltaHub.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ThreadManagementService.ThreadManagementService)({
          listActiveSubagentThreadIds: () => Effect.succeed([]),
          getThreadShellForEvent: ({ threadId: id, sequence }) =>
            Effect.sync(() => {
              input.reads.push(sequence);
              return { ...shell, id };
            }),
        }),
        Layer.mock(ProjectService.ProjectService)({}),
        Layer.effect(
          OrchestrationEventStore.OrchestrationEventStore,
          Effect.gen(function* () {
            const store = yield* OrchestrationEventStore.OrchestrationEventStore;
            return {
              ...store,
              subscribeCommitted: Effect.tap(store.subscribeCommitted, () =>
                Effect.sync(() => input.onStart?.()),
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
        items.map((item) => item.sequence),
        [5],
      );
      assert.deepEqual(reads, [1, 2, 3, 4, 5]);
    }).pipe(Effect.provide(layerHub({ reads })));
  }).pipe(Effect.scoped),
);

it.effect("an overflowing subscription ends and stops counting before its scope closes", () =>
  Effect.gen(function* () {
    const reads: Array<number> = [];
    let starts = 0;
    yield* Effect.gen(function* () {
      const hub = yield* ShellDeltaHub.ShellDeltaHub;
      const scope = yield* Scope.make();
      const stalled = yield* hub.subscribe({ maxItems: 1 }).pipe(Scope.provide(scope));
      yield* publish(threadId, 1);
      yield* publish(ThreadId.make("thread:other"), 2);
      yield* TestClock.adjust(0);
      // The stalled subscriber no longer counts: the next one starts a fresh hub.
      yield* hub.subscribe().pipe(Scope.provide(scope));
      yield* TestClock.adjust(0);
      assert.equal(starts, 2);
      const exit = yield* stalled.stream(0).pipe(Stream.runCollect, Effect.exit);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        assert.equal((Cause.squash(exit.cause) as { _tag?: string })._tag, "LiveStreamBufferError");
      }
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

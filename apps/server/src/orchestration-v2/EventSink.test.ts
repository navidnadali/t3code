import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as Sqlite from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const layer = EventSink.layer.pipe(
  Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
  Layer.provide(Sqlite.layerMemory),
);

it.effect("rejects a planned fork whose source was deleted before it committed", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const now = yield* DateTime.now;
    const sourceId = ThreadId.make("fork-source");
    const targetId = ThreadId.make("fork-target");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const source: OrchestrationV2AppThread = {
      id: sourceId,
      projectId: ProjectId.make("project"),
      title: "Source",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId,
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { rootThreadId: sourceId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      deletedAt: null,
      settledOverride: null,
      settledAt: null,
      snoozedUntil: null,
      snoozedAt: null,
      lastVisitedAt: null,
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make("source-created"),
          type: "thread.created",
          threadId: sourceId,
          occurredAt: now,
          payload: source,
        },
      ],
    });
    const plannedFork = {
      commandId: CommandId.make("fork"),
      commandType: "thread.fork",
      threadId: targetId,
      acceptedAt: now,
      effects: [],
      events: [
        {
          id: EventId.make("fork-created"),
          type: "thread.created" as const,
          threadId: targetId,
          occurredAt: now,
          payload: {
            ...source,
            id: targetId,
            forkedFrom: { type: "run" as const, threadId: sourceId, runId: RunId.make("run") },
          },
        },
      ],
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make("source-deleted"),
          type: "thread.deleted",
          threadId: sourceId,
          occurredAt: now,
          payload: { ...source, deletedAt: now },
        },
      ],
    });
    const rejected = yield* sink.commitCommand(plannedFork);
    assert.equal(rejected.receipt.status, "rejected");
    assert.isFalse(rejected.committed);
    const replayed = yield* sink.commitCommand(plannedFork);
    assert.deepEqual(replayed.receipt, rejected.receipt);
    assert.isFalse(replayed.committed);
    const missing = yield* projections.getThread(targetId).pipe(Effect.flip);
    assert.instanceOf(missing, ProjectionStore.ProjectionStoreThreadNotFoundError);
  }).pipe(Effect.provide(layer)),
);

it.effect("a stream of several event types gets only those types, in commit order", () =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make("typed-stream");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const thread: OrchestrationV2AppThread = {
      id: threadId,
      projectId: ProjectId.make("project"),
      title: "Typed stream",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId,
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      deletedAt: null,
      settledOverride: null,
      settledAt: null,
      snoozedUntil: null,
      snoozedAt: null,
      lastVisitedAt: null,
    };
    const run = (ordinal: number): OrchestrationV2Run => ({
      id: RunId.make(`typed-stream:${ordinal}`),
      threadId,
      ordinal,
      providerInstanceId,
      modelSelection: thread.modelSelection,
      providerThreadId: null,
      userMessageId: MessageId.make(`typed-stream:${ordinal}`),
      rootNodeId: null,
      activeAttemptId: null,
      status: "queued",
      queuePosition: ordinal,
      requestedAt: now,
      startedAt: null,
      completedAt: null,
      checkpointId: null,
      contextHandoffId: null,
    });
    const retitled = (title: string) => ({
      id: EventId.make(`typed-stream:${title}`),
      type: "thread.metadata-updated" as const,
      threadId,
      occurredAt: now,
      payload: { ...thread, title },
    });
    yield* sink.write({
      events: [
        {
          id: EventId.make("typed-stream:created"),
          type: "thread.created",
          threadId,
          occurredAt: now,
          payload: thread,
        },
      ],
    });
    const afterSequence = yield* sink.latestSequence();
    const eventTypes = ["run.created", "run.updated"] as const;
    // The first run event may arrive by replay; once it has, later ones arrive live.
    const subscribed = yield* Deferred.make<void>();
    const live = yield* sink.stream({ afterSequence, eventTypes }).pipe(
      Stream.tap(() => Deferred.succeed(subscribed, undefined)),
      Stream.take(4),
      Stream.runCollect,
      Effect.forkScoped,
    );
    yield* sink.write({
      events: [
        {
          id: EventId.make("typed-stream:3"),
          type: "run.created",
          threadId,
          occurredAt: now,
          payload: run(3),
        },
      ],
    });
    yield* Deferred.await(subscribed);
    yield* sink.write({
      events: [
        {
          id: EventId.make("typed-stream:1"),
          type: "run.created",
          threadId,
          occurredAt: now,
          payload: run(1),
        },
        retitled("first"),
        {
          id: EventId.make("typed-stream:1:cancelled"),
          type: "run.updated",
          threadId,
          occurredAt: now,
          payload: { ...run(1), status: "cancelled", queuePosition: null, completedAt: now },
        },
      ],
    });
    yield* sink.write({
      events: [
        retitled("second"),
        {
          id: EventId.make("typed-stream:2"),
          type: "run.created",
          threadId,
          occurredAt: now,
          payload: run(2),
        },
      ],
    });
    const summary = (
      events: Iterable<{ readonly sequence: number; readonly event: { readonly type: string } }>,
    ) => Array.from(events, ({ event }) => event.type);
    const expected = ["run.created", "run.created", "run.updated", "run.created"];
    const delivered = yield* Fiber.join(live);
    assert.deepEqual(summary(delivered), expected);
    const sequences = Array.from(delivered, ({ sequence }) => sequence);
    assert.deepEqual(
      sequences,
      sequences.toSorted((left, right) => left - right),
    );
    // A subscriber that starts later replays the same events from the store.
    const replayed = yield* sink
      .stream({ afterSequence, eventTypes })
      .pipe(Stream.take(4), Stream.runCollect);
    assert.deepEqual(summary(replayed), expected);
    assert.deepEqual(
      Array.from(replayed, ({ sequence }) => sequence),
      sequences,
    );
  }).pipe(Effect.provide(layer)),
);

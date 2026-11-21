import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import { CurrentWaitObserver } from "@t3tools/shared/WaitObserver";
import type * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as Sqlite from "../../persistence/Sqlite.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import { type LaneItem, makeCommitLatency } from "./CommitLatency.ts";

const threadId = ThreadId.make("thread:commit-latency");
const providerInstanceId = ProviderInstanceId.make("claudeAgent");

const created = (now: DateTime.Utc): OrchestrationV2DomainEvent => ({
  id: EventId.make("event:commit-latency:created"),
  type: "thread.created",
  threadId,
  providerInstanceId,
  occurredAt: now,
  payload: {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make("project:commit-latency"),
    title: "Commit latency",
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "claude-sonnet-4-6" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  },
});

const turnItem = (at: DateTime.Utc, step: number): OrchestrationV2DomainEvent => ({
  id: EventId.make(`event:commit-latency:${step}`),
  type: "turn-item.updated",
  threadId,
  occurredAt: at,
  payload: {
    id: TurnItemId.make("turn-item:commit-latency"),
    threadId,
    runId: RunId.make("run:commit-latency"),
    nodeId: NodeId.make("node:commit-latency"),
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "running",
    title: null,
    startedAt: at,
    completedAt: null,
    updatedAt: at,
    type: "assistant_message",
    messageId: MessageId.make("message:commit-latency"),
    text: `text ${step}`,
    streaming: true,
  },
});

// A bench's event latency is the time to the event's commit: each one is recorded only once a
// separate connection reads the event, never while its batch is still open.
it.effect("the commit lane's latency is recorded for each event once its batch has committed", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const filename = path.join(
      yield* fs.makeTempDirectoryScoped({ prefix: "commit-latency-" }),
      "statev2.sqlite",
    );
    const events = 12;
    const visibleAtRecord: Array<number> = [];
    let reader: NodeSqlite.DatabaseSync | undefined;
    const visible = () =>
      Number(
        (reader ??= new NodeSqlite.DatabaseSync(filename, { readOnly: true }))
          .prepare(`SELECT COUNT(*) AS count FROM orchestration_events WHERE stream_id = ?`)
          .get(threadId)?.count ?? 0,
      );
    const meter = makeCommitLatency(() => visibleAtRecord.push(visible()));
    const program = Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      yield* sink.write({ events: [created(now)] });
      const queue = yield* Queue.unbounded<LaneItem, Cause.Done>();
      const consumer = yield* sink
        .consumeProviderEvents(
          Stream.fromQueue(queue),
          (item) => meter.process(sink, item),
          Effect.succeed(false),
          () => Effect.succeed(true),
        )
        .pipe(Effect.forkScoped);
      for (let step = 1; step <= events; step++) {
        const event = turnItem(now, step);
        const item = { type: event.type, event };
        yield* meter.arrived(item);
        yield* Queue.offer(queue, item);
      }
      yield* Queue.end(queue);
      yield* Fiber.join(consumer);
    });
    yield* program.pipe(
      Effect.provide(
        EventSink.layer.pipe(
          Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
          Layer.provideMerge(Sqlite.layerFromPath(filename)),
        ),
      ),
      Effect.scoped,
      Effect.provideService(CurrentWaitObserver, meter.observer),
      Effect.ensuring(Effect.sync(() => reader?.close())),
    );
    assert.lengthOf(visibleAtRecord, events);
    // The thread's creation and every event recorded so far, all committed.
    for (const [index, count] of visibleAtRecord.entries()) {
      assert.isAtLeast(count, index + 2, `record ${index + 1}`);
    }
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

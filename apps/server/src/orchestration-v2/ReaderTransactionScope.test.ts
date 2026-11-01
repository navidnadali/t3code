import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  ContextTransferId,
  EventId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const providerInstanceId = ProviderInstanceId.make("codex");
const providerDriver = ProviderDriverKind.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} satisfies ModelSelection;
const at = (second: number) => DateTime.makeUnsafe(Date.UTC(2026, 0, 1, 0, 0, second));

const layerFile = (filename: string) =>
  EventSink.layer.pipe(
    Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
    Layer.provideMerge(SqlitePersistence.layerFromPath(filename)),
  );

const withFileStore = <A, E>(
  prefix: string,
  body: Effect.Effect<
    A,
    E,
    EventSink.EventSinkV2 | ProjectionStore.ProjectionStoreV2 | EventStore.EventStoreV2
  >,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix });
    return yield* body.pipe(Effect.provide(layerFile(path.join(directory, "statev2.sqlite"))));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

function makeThread(
  threadId: ThreadId,
  forkedFrom: OrchestrationV2AppThread["forkedFrom"],
): OrchestrationV2AppThread {
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make("project:reader-scope"),
    title: `Thread ${threadId}`,
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom,
    createdAt: at(0),
    updatedAt: at(0),
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

const event = (
  id: string,
  rest: Omit<OrchestrationV2DomainEvent, "id" | "occurredAt">,
): OrchestrationV2DomainEvent =>
  ({ id: EventId.make(id), occurredAt: at(1), ...rest }) as OrchestrationV2DomainEvent;

// One run with a root node, its checkpoint scope, a user and an assistant message and tool items.
function runEvents(input: {
  readonly threadId: ThreadId;
  readonly name: string;
  readonly ordinal: number;
  readonly toolItems: number;
}): Array<OrchestrationV2DomainEvent> {
  const { threadId, name } = input;
  const runId = RunId.make(`run:${name}`);
  const rootNodeId = NodeId.make(`node:${name}:root`);
  const scopeId = CheckpointScopeId.make(`scope:${name}`);
  const userMessageId = MessageId.make(`message:${name}:user`);
  const assistantMessageId = MessageId.make(`message:${name}:assistant`);
  const itemBase = (index: number) => ({
    id: TurnItemId.make(`item:${name}:${index}`),
    threadId,
    runId,
    nodeId: rootNodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: index,
    status: "completed" as const,
    title: null,
    startedAt: at(2),
    completedAt: at(2),
    updatedAt: at(2),
  });
  return [
    event(`event:${name}:run`, {
      type: "run.updated",
      threadId,
      runId,
      payload: {
        id: runId,
        threadId,
        ordinal: input.ordinal,
        providerInstanceId,
        modelSelection,
        providerThreadId: null,
        userMessageId,
        rootNodeId,
        activeAttemptId: null,
        status: "completed",
        requestedAt: at(1),
        startedAt: at(1),
        completedAt: at(3),
        checkpointId: null,
        contextHandoffId: null,
      },
    } as never),
    event(`event:${name}:root`, {
      type: "node.updated",
      threadId,
      payload: {
        id: rootNodeId,
        threadId,
        runId,
        parentNodeId: null,
        rootNodeId,
        kind: "root_turn",
        status: "completed",
        countsForRun: true,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: scopeId,
        startedAt: at(1),
        completedAt: at(3),
      },
    } as never),
    event(`event:${name}:scope`, {
      type: "checkpoint-scope.created",
      threadId,
      payload: {
        id: scopeId,
        threadId,
        runId,
        nodeId: rootNodeId,
        parentScopeId: null,
        providerThreadId: null,
        kind: "root_run",
        ordinalWithinParent: input.ordinal,
        advancesAppRunCount: true,
        cwd: "/repo",
        createdAt: at(1),
      },
    } as never),
    // The assistant reply is stored first, so only the turn-item order puts the user message first.
    ...[
      { id: assistantMessageId, role: "assistant" as const, createdAt: at(1) },
      { id: userMessageId, role: "user" as const, createdAt: at(2) },
    ].map((message) =>
      event(`event:${name}:message:${message.role}`, {
        type: "message.updated",
        threadId,
        payload: {
          createdBy: message.role === "user" ? "user" : "agent",
          creationSource: message.role === "user" ? "web" : "provider",
          id: message.id,
          threadId,
          runId,
          nodeId: rootNodeId,
          role: message.role,
          text: `${name} ${message.role}`,
          attachments: [],
          streaming: false,
          createdAt: message.createdAt,
          updatedAt: message.createdAt,
        },
      } as never),
    ),
    event(`event:${name}:item:user`, {
      type: "turn-item.updated",
      threadId,
      payload: {
        ...itemBase(1),
        type: "user_message",
        createdBy: "user",
        creationSource: "web",
        messageId: userMessageId,
        inputIntent: "turn_start",
        text: `${name} user`,
        attachments: [],
      },
    } as never),
    event(`event:${name}:item:assistant`, {
      type: "turn-item.updated",
      threadId,
      payload: {
        ...itemBase(2),
        type: "assistant_message",
        messageId: assistantMessageId,
        text: `${name} assistant`,
        streaming: false,
      },
    } as never),
    ...Array.from({ length: input.toolItems }, (_, offset) =>
      event(`event:${name}:item:tool:${offset}`, {
        type: "turn-item.updated",
        threadId,
        payload: {
          ...itemBase(3 + offset),
          type: "dynamic_tool",
          toolName: `tool-${offset}`,
          input: { offset },
          output: { offset, text: "x".repeat(2_048) },
        },
      } as never),
    ),
  ];
}

interface Fixture {
  readonly source: ThreadId;
  readonly fork: ThreadId;
  readonly forkRunId: RunId;
  readonly child: ThreadId;
}

// A run fork of a source thread, with a subagent, its spawn transfer and a provider thread.
const writeFixture = (name: string, toolItems: number) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const source = ThreadId.make(`thread:${name}:source`);
    const fork = ThreadId.make(`thread:${name}:fork`);
    const child = ThreadId.make(`thread:${name}:child`);
    const subagentId = NodeId.make(`node:${name}:subagent`);
    const forkRunId = RunId.make(`run:${name}:fork`);
    yield* sink.write({
      events: [
        event(`event:${name}:source`, {
          type: "thread.created",
          threadId: source,
          providerInstanceId,
          payload: makeThread(source, null),
        } as never),
        ...runEvents({ threadId: source, name: `${name}:source`, ordinal: 1, toolItems }),
        event(`event:${name}:fork`, {
          type: "thread.created",
          threadId: fork,
          providerInstanceId,
          payload: makeThread(fork, {
            type: "run",
            threadId: source,
            runId: RunId.make(`run:${name}:source`),
          }),
        } as never),
        ...runEvents({ threadId: fork, name: `${name}:fork`, ordinal: 1, toolItems }),
        event(`event:${name}:subagent`, {
          type: "subagent.updated",
          threadId: fork,
          nodeId: subagentId,
          providerInstanceId,
          payload: {
            id: subagentId,
            threadId: fork,
            runId: forkRunId,
            parentNodeId: NodeId.make(`node:${name}:fork:root`),
            origin: "app_owned",
            createdBy: "agent",
            driver: providerDriver,
            providerInstanceId,
            providerThreadId: ProviderThreadId.make(`provider-thread:${name}:child`),
            childThreadId: child,
            nativeTaskRef: null,
            prompt: "Inspect",
            title: "Child",
            model: modelSelection.model,
            status: "completed",
            result: "done",
            startedAt: at(2),
            completedAt: at(3),
            updatedAt: at(3),
          },
        } as never),
        event(`event:${name}:transfer`, {
          type: "context-transfer.created",
          threadId: fork,
          providerInstanceId,
          payload: {
            id: ContextTransferId.make(`transfer:${name}:spawn`),
            type: "subagent_spawn",
            sourceThreadId: fork,
            targetThreadId: child,
            sourcePoint: { threadId: fork },
            basePoint: null,
            sourceProviderInstanceId: providerInstanceId,
            targetProviderInstanceId: providerInstanceId,
            targetRunId: null,
            status: "consumed",
            resolution: null,
            createdBy: "agent",
            error: null,
            createdAt: at(2),
            updatedAt: at(2),
            consumedAt: at(2),
          },
        } as never),
        event(`event:${name}:provider-thread`, {
          type: "provider-thread.updated",
          threadId: fork,
          driver: providerDriver,
          providerInstanceId,
          payload: {
            id: ProviderThreadId.make(`provider-thread:${name}:fork`),
            driver: providerDriver,
            providerInstanceId,
            providerSessionId: null,
            appThreadId: fork,
            ownerNodeId: null,
            nativeThreadRef: null,
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: at(1),
            updatedAt: at(1),
          },
        } as never),
      ],
    });
    return { source, fork, forkRunId, child } satisfies Fixture;
  });

it.effect("keeps the results of the reads that hold reader transactions", () =>
  withFileStore(
    "reader-scope-results-",
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const fixture = yield* writeFixture("results", 400);

      const projection = yield* store.getThreadProjection(fixture.fork);
      assert.lengthOf(projection.turnItems, 402);
      assert.deepEqual(
        projection.messages.map((message) => message.id),
        [
          MessageId.make("message:results:fork:user"),
          MessageId.make("message:results:fork:assistant"),
        ],
      );
      assert.deepEqual(
        projection.subagents.map((subagent) => subagent.childThreadId),
        [fixture.child],
      );
      assert.lengthOf(projection.contextTransfers, 1);
      assert.lengthOf(projection.checkpointScopes, 1);
      // The fork shows the source run's items before its own, behind a fork marker.
      assert.isAbove(projection.visibleTurnItems.length, projection.turnItems.length);
      assert.deepEqual(
        projection.visibleTurnItems.map((row) => row.position),
        projection.visibleTurnItems.map((_, index) => index),
      );
      assert.strictEqual(
        projection.visibleTurnItems.at(-1)?.item.id,
        TurnItemId.make("item:results:fork:402"),
      );

      const snapshot = yield* store.getThreadSnapshot(fixture.fork);
      assert.deepStrictEqual(snapshot.projection, projection);
      assert.isAbove(snapshot.snapshotSequence, 0);

      // Without turn items, only the message ordinal read puts the user message first.
      const records = yield* store.getThreadRecords(fixture.fork, [
        "messages",
        "subagents",
        "contextTransfers",
      ]);
      assert.deepEqual(
        records.messages.map((message) => message.id),
        [
          MessageId.make("message:results:fork:user"),
          MessageId.make("message:results:fork:assistant"),
        ],
      );
      assert.deepStrictEqual(records.subagents, projection.subagents);
      assert.deepStrictEqual(records.contextTransfers, projection.contextTransfers);
      assert.deepStrictEqual(records.thread, projection.thread);

      const turnStart = yield* store.getTurnStartContext(fixture.fork, fixture.forkRunId);
      assert.isTrue(turnStart.hasConversation);
      assert.deepEqual(
        turnStart.nodes.map((node) => node.id),
        [NodeId.make("node:results:fork:root")],
      );
      assert.deepStrictEqual(turnStart.checkpointScopes, projection.checkpointScopes);
      assert.lengthOf(turnStart.turnItems, 402);
      assert.deepEqual(
        turnStart.messages.map((message) => message.id),
        [MessageId.make("message:results:fork:user")],
      );
      assert.deepStrictEqual(turnStart.subagents, projection.subagents);
      assert.deepStrictEqual(turnStart.contextTransfers, projection.contextTransfers);

      const shell = yield* store.getThreadShell(fixture.fork);
      assert.strictEqual(shell?.id, fixture.fork);

      const candidates = yield* store.getSettlementCandidates(fixture.fork);
      assert.deepEqual(
        candidates.map((candidate) => candidate.id),
        [fixture.fork],
      );
    }),
  ),
);

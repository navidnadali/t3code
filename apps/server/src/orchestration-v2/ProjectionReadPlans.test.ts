import { assert, it } from "@effect/vitest";
import {
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
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const layerTest = Layer.mergeAll(
  ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
  SqlitePersistence.layerMemory,
);
const providerInstanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} satisfies ModelSelection;
const at = (second: number) => DateTime.makeUnsafe(Date.UTC(2026, 0, 1, 0, 0, second));

const traceSqlStatements = () => {
  const statements: Array<string> = [];
  const tracer = Tracer.make({
    span(options) {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      span.end = (endTime, exit) => {
        end(endTime, exit);
        const query = span.attributes.get("db.query.text");
        if (typeof query === "string") statements.push(query);
      };
      return span;
    },
  });
  return { statements, tracer };
};

const queryPlan = (statement: string | undefined) =>
  Effect.gen(function* () {
    assert.isDefined(statement);
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql.unsafe<{ readonly detail: string }>(`EXPLAIN QUERY PLAN ${statement}`);
    return rows.map((row) => row.detail);
  });

const event = (id: string, rest: Omit<OrchestrationV2DomainEvent, "id" | "occurredAt">) =>
  ({ id: EventId.make(id), occurredAt: at(1), ...rest }) as OrchestrationV2DomainEvent;

const threadCreated = (threadId: ThreadId) => {
  const payload: OrchestrationV2AppThread = {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make("project:read-plans"),
    title: `Thread ${threadId}`,
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: at(0),
    updatedAt: at(0),
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  return event(`event:${threadId}:created`, {
    type: "thread.created",
    threadId,
    providerInstanceId,
    payload,
  } as never);
};

// A run's user and assistant messages and their items; the assistant message is stored first,
// so only the items' ordinals put the user message first.
const conversation = (threadId: ThreadId, name: string, firstOrdinal: number) => {
  const runId = RunId.make(`run:${name}`);
  const nodeId = NodeId.make(`node:${name}:root`);
  const user = MessageId.make(`message:${name}:user`);
  const assistant = MessageId.make(`message:${name}:assistant`);
  const item = (index: number) => ({
    id: TurnItemId.make(`item:${name}:${index}`),
    threadId,
    runId,
    nodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: firstOrdinal + index,
    status: "completed" as const,
    title: null,
    startedAt: at(2),
    completedAt: at(2),
    updatedAt: at(2),
  });
  const events = [
    ...[
      { id: assistant, role: "assistant" as const, createdAt: at(1) },
      { id: user, role: "user" as const, createdAt: at(2) },
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
          nodeId,
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
        ...item(0),
        type: "user_message",
        createdBy: "user",
        creationSource: "web",
        messageId: user,
        inputIntent: "turn_start",
        text: `${name} user`,
        attachments: [],
      },
    } as never),
    // A message streamed over two items keeps its first ordinal.
    ...[1, 2].map((index) =>
      event(`event:${name}:item:assistant:${index}`, {
        type: "turn-item.updated",
        threadId,
        payload: {
          ...item(index),
          type: "assistant_message",
          messageId: assistant,
          text: `${name} assistant ${index}`,
          streaming: false,
        },
      } as never),
    ),
    event(`event:${name}:item:tool`, {
      type: "turn-item.updated",
      threadId,
      payload: {
        ...item(3),
        type: "dynamic_tool",
        toolName: "tool",
        input: {},
        output: { messageId: user },
      },
    } as never),
  ];
  return { events, user, assistant };
};

const subagentTask = (
  parent: ThreadId,
  name: string,
  childThreadId: ThreadId | null,
  startedAt: number,
) =>
  event(`event:${parent}:subagent:${name}`, {
    type: "subagent.updated",
    threadId: parent,
    nodeId: NodeId.make(`node:${name}`),
    providerInstanceId,
    payload: {
      id: NodeId.make(`node:${name}`),
      threadId: parent,
      runId: null,
      parentNodeId: NodeId.make(`node:${parent}:root`),
      origin: childThreadId === null ? "provider_native" : "app_owned",
      createdBy: "agent",
      driver,
      providerInstanceId,
      providerThreadId: null,
      childThreadId,
      nativeTaskRef: null,
      prompt: name,
      title: name,
      model: modelSelection.model,
      status: "completed",
      result: null,
      startedAt: at(startedAt),
      completedAt: at(9),
      updatedAt: at(9),
    },
  } as never);

const transfer = (
  name: string,
  type: "subagent_spawn" | "subagent_result" | "fork",
  sourceThreadId: ThreadId,
  targetThreadId: ThreadId,
) =>
  event(`event:transfer:${name}`, {
    type: "context-transfer.created",
    threadId: targetThreadId,
    providerInstanceId,
    payload: {
      id: ContextTransferId.make(`transfer:${name}`),
      type,
      sourceThreadId,
      targetThreadId,
      sourcePoint: { threadId: sourceThreadId },
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
  } as never);

it.effect("reads message ordinals through their index and orders messages as without it", () =>
  Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:message-ordinals");
    const first = conversation(threadId, "first", 1);
    const second = conversation(threadId, "second", 10);
    for (const applied of [threadCreated(threadId), ...first.events, ...second.events]) {
      yield* store.apply(applied);
    }

    const { statements, tracer } = traceSqlStatements();
    const indexed = yield* store
      .getThreadRecords(threadId, ["messages"])
      .pipe(Effect.withTracer(tracer));
    const plan = yield* queryPlan(
      statements.find((statement) => statement.includes("MIN(ordinal) AS ordinal")),
    );
    assert.deepEqual(plan, [
      "SEARCH orchestration_v2_projection_turn_items USING INDEX orchestration_v2_projection_turn_items_message_ordinal_idx (thread_id=?)",
    ]);
    assert.deepEqual(
      indexed.messages.map((message) => message.id),
      [first.user, first.assistant, second.user, second.assistant],
    );

    yield* sql`DROP INDEX orchestration_v2_projection_turn_items_message_ordinal_idx`;
    assert.deepEqual(
      (yield* store.getThreadRecords(threadId, ["messages"])).messages,
      indexed.messages,
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect("narrows a parent read to one child's tasks and transfers, seeking that child", () =>
  Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const parent = ThreadId.make("thread:parent");
    const child = ThreadId.make("thread:child");
    const sibling = ThreadId.make("thread:sibling");
    const elsewhere = ThreadId.make("thread:elsewhere");
    for (const applied of [
      threadCreated(parent),
      subagentTask(parent, "child-late", child, 5),
      subagentTask(parent, "sibling", sibling, 3),
      subagentTask(parent, "child-early", child, 1),
      subagentTask(parent, "native", null, 2),
      transfer("child-spawn", "subagent_spawn", parent, child),
      transfer("child-result", "subagent_result", child, parent),
      transfer("sibling-result", "subagent_result", sibling, parent),
      transfer("child-elsewhere", "fork", child, elsewhere),
      transfer("fork-in", "fork", elsewhere, parent),
    ]) {
      yield* store.apply(applied);
    }

    const full = yield* store.getThreadRecords(parent, ["subagents", "contextTransfers"]);
    const { statements, tracer } = traceSqlStatements();
    const narrowed = yield* store
      .getThreadRecords(parent, ["subagents", "contextTransfers"], {
        subagentChildThreadIds: [child],
        contextTransferSourceThreadIds: [child],
      })
      .pipe(Effect.withTracer(tracer));
    assert.deepEqual(
      narrowed.subagents,
      full.subagents.filter((task) => task.childThreadId === child),
    );
    assert.deepEqual(
      narrowed.subagents.map((task) => task.id),
      [NodeId.make("node:child-early"), NodeId.make("node:child-late")],
    );
    assert.deepEqual(
      narrowed.contextTransfers,
      full.contextTransfers.filter((candidate) => candidate.sourceThreadId === child),
    );
    assert.deepEqual(
      narrowed.contextTransfers.map((candidate) => candidate.id),
      [ContextTransferId.make("transfer:child-result")],
    );

    const subagentPlan = yield* queryPlan(
      statements.find((statement) => statement.includes("child_thread_id IN")),
    );
    assert.include(
      subagentPlan[0],
      "USING INDEX orchestration_v2_projection_subagents_child_thread_idx (child_thread_id=?)",
    );
    const transferPlan = yield* queryPlan(
      statements.find((statement) =>
        statement.includes("FROM orchestration_v2_projection_context_transfers"),
      ),
    );
    assert.include(
      transferPlan[0],
      "USING INDEX orchestration_v2_projection_context_transfers_source_thread_idx (source_thread_id=?)",
    );

    const none = yield* store.getThreadRecords(parent, ["subagents", "contextTransfers"], {
      subagentChildThreadIds: [],
      contextTransferSourceThreadIds: [],
    });
    assert.deepEqual([none.subagents, none.contextTransfers], [[], []]);
  }).pipe(Effect.provide(layerTest)),
);

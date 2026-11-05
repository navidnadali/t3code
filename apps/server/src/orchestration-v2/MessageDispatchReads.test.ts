import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelA = { instanceId, model: "gpt-a" } satisfies ModelSelection;
const modelB = { instanceId, model: "gpt-b" } satisfies ModelSelection;
const adapter = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("No provider process needed to dispatch"),
} as ProviderAdapter.ProviderAdapterV2["Service"];
const layerDatabase = SqlitePersistence.layerMemory;
const layerTest = Layer.mergeAll(
  layerDatabase,
  ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
  EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
  ProviderReplayHarness.layerWithRegistry(
    { name: "message-dispatch-reads" },
    ProviderAdapterRegistry.layerFromAdapters([adapter]),
    { databaseLayer: layerDatabase, runEffectWorker: false },
  ),
);

const base = Date.parse("2026-01-01T00:00:00.000Z");
const at = (seconds: number) => DateTime.makeUnsafe(base + seconds * 1000);

interface HistoryRun {
  /** A person's message the timeline shows; otherwise a delegated-completion wake. */
  readonly shown?: boolean;
  readonly modelSelection?: ModelSelection;
  readonly status?: OrchestrationV2Run["status"];
  readonly queueHeld?: boolean;
  readonly startedAt?: number | null;
  readonly completedAt?: number | null;
  readonly workspacePreparation?: OrchestrationV2Run["workspacePreparation"];
  /** Settled tasks it delegated, each with its node, card, transfers and child provider thread. */
  readonly tasks?: number;
}

/**
 * A thread and its history, written straight to the projection: each run has its attempt, turn,
 * root node, message and card, a tool call, and settled delegated tasks with everything they keep.
 */
const seedThread = Effect.fn("seedThread")(function* (input: {
  readonly threadId: ThreadId;
  readonly worktreePath?: string | null;
  readonly modelSelection?: ModelSelection;
  readonly runs: ReadonlyArray<HistoryRun>;
}) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const { threadId } = input;
  const providerThreadId = ProviderThreadId.make(`provider-thread:${threadId}`);
  const sessionId = ProviderSessionId.make(`session:${threadId}`);
  const modelSelection = input.modelSelection ?? modelB;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:${threadId}`),
    threadId,
    projectId: ProjectId.make(`project:${threadId}`),
    title: "Dispatcher",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: input.worktreePath === undefined ? `/repo/${threadId}` : input.worktreePath,
    createdBy: "user",
    creationSource: "web",
  });
  let sequence = 0;
  const apply = (event: Omit<OrchestrationV2DomainEvent, "id">) =>
    projections.apply({
      ...event,
      id: EventId.make(`event:${threadId}:${++sequence}`),
    } as OrchestrationV2DomainEvent);
  const thread = yield* projections.getThread(threadId);
  yield* apply({
    type: "provider-session.attached",
    threadId,
    occurredAt: at(0),
    payload: {
      id: sessionId,
      driver,
      providerInstanceId: instanceId,
      status: "ready",
      cwd: `/repo/${threadId}`,
      model: modelSelection.model,
      capabilities: CodexProviderCapabilitiesV2,
      createdAt: at(0),
      updatedAt: at(0),
      lastError: null,
    },
  });
  yield* apply({
    type: "provider-thread.updated",
    threadId,
    occurredAt: at(0),
    payload: {
      id: providerThreadId,
      driver,
      providerInstanceId: instanceId,
      providerSessionId: sessionId,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: { driver, nativeId: `native:${threadId}`, strength: "strong" },
      nativeConversationHeadRef: null,
      status: "active",
      firstRunOrdinal: 1,
      lastRunOrdinal: input.runs.length,
      handoffIds: [],
      forkedFrom: null,
      createdAt: at(0),
      updatedAt: at(0),
    },
  });
  yield* apply({
    type: "thread.metadata-updated",
    threadId,
    occurredAt: at(0),
    payload: { ...thread, activeProviderThreadId: providerThreadId },
  });
  let ordinal = 0;
  const runs: Array<OrchestrationV2Run> = [];
  for (const [index, history] of input.runs.entries()) {
    const n = index + 1;
    const runId = RunId.make(`run:${threadId}:${n}`);
    const attemptId = RunAttemptId.make(`attempt:${threadId}:${n}`);
    const rootNodeId = NodeId.make(`node:${threadId}:root:${n}`);
    const turnId = ProviderTurnId.make(`turn:${threadId}:${n}`);
    const messageId = MessageId.make(`message:${threadId}:${n}`);
    const status = history.status ?? "completed";
    const live = status === "running" || status === "waiting" || status === "starting";
    const startedAt =
      history.startedAt === undefined
        ? status === "queued"
          ? null
          : at(n * 100)
        : history.startedAt === null
          ? null
          : at(history.startedAt);
    const completedAt =
      history.completedAt === undefined
        ? status === "queued" || live
          ? null
          : at(n * 100 + 50)
        : history.completedAt === null
          ? null
          : at(history.completedAt);
    const run: OrchestrationV2Run = {
      id: runId,
      threadId,
      ordinal: n,
      providerInstanceId: instanceId,
      modelSelection: history.modelSelection ?? modelSelection,
      providerThreadId,
      userMessageId: messageId,
      rootNodeId,
      activeAttemptId: attemptId,
      status,
      queuePosition: status === "queued" ? n : null,
      ...(history.queueHeld === undefined ? {} : { queueHeld: history.queueHeld }),
      requestedAt: at(n * 100 - 10),
      startedAt,
      completedAt,
      checkpointId: null,
      contextHandoffId: null,
      ...(history.workspacePreparation === undefined
        ? {}
        : { workspacePreparation: history.workspacePreparation }),
    };
    runs.push(run);
    yield* apply({
      type: "run.created",
      threadId,
      runId,
      nodeId: rootNodeId,
      occurredAt: run.requestedAt,
      payload: run,
    });
    yield* apply({
      type: "run-attempt.created",
      threadId,
      runId,
      occurredAt: run.requestedAt,
      payload: {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId,
        providerInstanceId: instanceId,
        providerThreadId,
        providerTurnId: status === "queued" ? null : turnId,
        reason: "initial",
        status: status === "queued" ? "pending" : live ? "running" : "completed",
        startedAt,
        completedAt,
      },
    });
    yield* apply({
      type: "node.updated",
      threadId,
      runId,
      nodeId: rootNodeId,
      occurredAt: run.requestedAt,
      payload: {
        id: rootNodeId,
        threadId,
        runId,
        parentNodeId: null,
        rootNodeId,
        kind: "root_turn",
        status: status === "queued" ? "pending" : live ? "running" : "completed",
        countsForRun: true,
        providerThreadId,
        providerTurnId: status === "queued" ? null : turnId,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt,
        completedAt,
      },
    });
    if (status !== "queued") {
      yield* apply({
        type: "provider-turn.updated",
        threadId,
        nodeId: rootNodeId,
        occurredAt: run.requestedAt,
        payload: {
          id: turnId,
          providerThreadId,
          nodeId: rootNodeId,
          runAttemptId: attemptId,
          nativeTurnRef: null,
          ordinal: n,
          status: live ? "running" : "completed",
          startedAt,
          completedAt,
        },
      });
    }
    const shown = history.shown === true;
    const itemBase = {
      threadId,
      runId,
      nodeId: rootNodeId,
      providerThreadId,
      providerTurnId: status === "queued" ? null : turnId,
      nativeItemRef: null,
      parentItemId: null,
      title: null,
      startedAt: run.requestedAt,
      completedAt: run.requestedAt,
      updatedAt: run.requestedAt,
    };
    yield* apply({
      type: "message.updated",
      threadId,
      runId,
      nodeId: rootNodeId,
      occurredAt: run.requestedAt,
      payload: {
        createdBy: shown ? "user" : "agent",
        creationSource: shown ? "web" : "server",
        id: messageId,
        threadId,
        runId,
        nodeId: rootNodeId,
        role: "user",
        text: shown ? `Prompt ${n}` : `Results ${n}`,
        attachments: [],
        streaming: false,
        createdAt: run.requestedAt,
        updatedAt: run.requestedAt,
        ...(shown
          ? {}
          : {
              notification: {
                source: { kind: "background_task" },
                outcome: "completed",
                summary: `Wake ${n}`,
              },
            }),
      },
    });
    yield* apply({
      type: "turn-item.updated",
      threadId,
      runId,
      nodeId: rootNodeId,
      occurredAt: run.requestedAt,
      payload: shown
        ? {
            ...itemBase,
            id: TurnItemId.make(`item:${threadId}:user:${n}`),
            ordinal: ++ordinal,
            status: "completed",
            type: "user_message",
            messageId,
            inputIntent: status === "queued" ? "queued_turn" : "turn_start",
            text: `Prompt ${n}`,
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          }
        : {
            ...itemBase,
            id: TurnItemId.make(`item:${threadId}:wake:${n}`),
            ordinal: ++ordinal,
            status: "completed",
            type: "notification",
            source: { kind: "background_task" },
            outcome: "completed",
            summary: `Wake ${n}`,
          },
    });
    const toolNodeId = NodeId.make(`node:${threadId}:tool:${n}`);
    yield* apply({
      type: "node.updated",
      threadId,
      runId,
      nodeId: toolNodeId,
      occurredAt: run.requestedAt,
      payload: {
        id: toolNodeId,
        threadId,
        runId,
        parentNodeId: rootNodeId,
        rootNodeId,
        kind: "tool_call",
        status: "completed",
        countsForRun: true,
        providerThreadId,
        providerTurnId: turnId,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: run.requestedAt,
        completedAt: run.requestedAt,
      },
    });
    yield* apply({
      type: "turn-item.updated",
      threadId,
      runId,
      nodeId: toolNodeId,
      occurredAt: run.requestedAt,
      payload: {
        ...itemBase,
        nodeId: toolNodeId,
        id: TurnItemId.make(`item:${threadId}:tool:${n}`),
        ordinal: ++ordinal,
        status: "completed",
        type: "command_execution",
        input: `command ${n}`,
        output: `output ${n}`,
        exitCode: 0,
      },
    });
    for (let task = 1; task <= (history.tasks ?? 2); task++) {
      const taskId = NodeId.make(`node:${threadId}:task:${n}:${task}`);
      const childThreadId = ThreadId.make(`thread:${threadId}:child:${n}:${task}`);
      const childProviderThreadId = ProviderThreadId.make(`provider-thread:${childThreadId}`);
      yield* apply({
        type: "node.updated",
        threadId,
        runId,
        nodeId: taskId,
        occurredAt: run.requestedAt,
        payload: {
          id: taskId,
          threadId,
          runId,
          parentNodeId: rootNodeId,
          rootNodeId,
          kind: "subagent",
          status: "completed",
          countsForRun: false,
          providerThreadId: childProviderThreadId,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: run.requestedAt,
          completedAt: run.requestedAt,
        },
      });
      yield* apply({
        type: "subagent.updated",
        threadId,
        runId,
        nodeId: taskId,
        occurredAt: run.requestedAt,
        payload: {
          id: taskId,
          threadId,
          runId,
          parentNodeId: rootNodeId,
          origin: "app_owned",
          createdBy: "agent",
          driver,
          providerInstanceId: instanceId,
          providerThreadId: childProviderThreadId,
          childThreadId,
          nativeTaskRef: null,
          prompt: `Task ${n}.${task}`,
          title: `Task ${n}.${task}`,
          model: modelSelection.model,
          completionDelivery: { state: "delivered", observedByRunId: runId },
          status: "completed",
          result: `Result ${n}.${task}`,
          startedAt: run.requestedAt,
          completedAt: run.requestedAt,
          updatedAt: run.requestedAt,
        },
      });
      yield* apply({
        type: "turn-item.updated",
        threadId,
        runId,
        nodeId: taskId,
        occurredAt: run.requestedAt,
        payload: {
          ...itemBase,
          nodeId: taskId,
          id: TurnItemId.make(`item:${threadId}:task:${n}:${task}`),
          ordinal: ++ordinal,
          status: "completed",
          type: "subagent",
          subagentId: taskId,
          origin: "app_owned",
          driver,
          providerInstanceId: instanceId,
          childThreadId,
          prompt: `Task ${n}.${task}`,
          result: `Result ${n}.${task}`,
        },
      });
      yield* apply({
        type: "provider-thread.updated",
        threadId: childThreadId,
        occurredAt: run.requestedAt,
        payload: {
          id: childProviderThreadId,
          driver,
          providerInstanceId: instanceId,
          providerSessionId: null,
          appThreadId: childThreadId,
          ownerNodeId: null,
          nativeThreadRef: null,
          nativeConversationHeadRef: null,
          status: "idle",
          firstRunOrdinal: 1,
          lastRunOrdinal: 1,
          handoffIds: [],
          forkedFrom: null,
          createdAt: run.requestedAt,
          updatedAt: run.requestedAt,
        },
      });
      for (const [type, source, target] of [
        ["subagent_spawn", threadId, childThreadId],
        ["subagent_result", childThreadId, threadId],
      ] as const) {
        yield* apply({
          type: "context-transfer.created",
          threadId: target,
          occurredAt: run.requestedAt,
          payload: {
            id: `transfer:${type}:${taskId}`,
            type,
            sourceThreadId: source,
            targetThreadId: target,
            sourcePoint: { threadId: source },
            basePoint: null,
            sourceProviderInstanceId: instanceId,
            targetProviderInstanceId: instanceId,
            targetRunId: null,
            status: "consumed",
            resolution: null,
            createdBy: "agent",
            error: null,
            createdAt: run.requestedAt,
            updatedAt: run.requestedAt,
            consumedAt: run.requestedAt,
          },
        } as Omit<OrchestrationV2DomainEvent, "id">);
      }
    }
  }
  return { runs, providerThreadId };
});

/** A finished task of the active run, claimed by the open delivery its result joins. */
const openDelivery = Effect.fn("openDelivery")(function* (
  threadId: ThreadId,
  active: OrchestrationV2Run,
) {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const taskId = NodeId.make(`node:${threadId}:delivered-task`);
  const deliveryId = MessageId.make(`message:${threadId}:delivery`);
  yield* projections.apply({
    id: EventId.make(`event:${threadId}:delivered-task`),
    type: "subagent.updated",
    threadId,
    runId: active.id,
    nodeId: taskId,
    occurredAt: at(10_000),
    payload: {
      id: taskId,
      threadId,
      runId: active.id,
      parentNodeId: active.rootNodeId!,
      origin: "app_owned",
      createdBy: "agent",
      driver,
      providerInstanceId: instanceId,
      providerThreadId: null,
      childThreadId: ThreadId.make(`thread:${threadId}:delivered-task`),
      nativeTaskRef: null,
      prompt: "Look into it.",
      title: "Look",
      model: null,
      completionDelivery: { state: "claimed", observedByRunId: null },
      status: "completed",
      result: "Found it.",
      startedAt: at(10_000),
      completedAt: at(10_000),
      updatedAt: at(10_000),
    },
  });
  yield* projections.apply({
    id: EventId.make(`event:${threadId}:cohort`),
    type: "run.updated",
    threadId,
    runId: active.id,
    occurredAt: at(10_000),
    payload: {
      ...active,
      delegatedCompletion: {
        disposition: "open",
        nextGeneration: 2,
        delivery: { generation: 1, messageId: deliveryId, taskIds: [taskId] },
      },
    },
  });
  return {
    taskId,
    command: {
      type: "message.dispatch",
      commandId: CommandId.make(`command:${threadId}:delivery`),
      threadId,
      messageId: deliveryId,
      text: `Delegated task ${taskId} reached a terminal state.`,
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
      delegatedCompletion: { parentRunId: active.id, generation: 1, taskIds: [taskId] },
    } as const,
  };
});

// One person's message in five, the last of them four runs from the end, whatever the length.
const history = (runs: number, tail: ReadonlyArray<HistoryRun>): ReadonlyArray<HistoryRun> => [
  ...Array.from({ length: runs }, (_, index) => ({ shown: (runs - 1 - index) % 5 === 3 })),
  ...tail,
];

/**
 * Every SELECT a dispatch issues on the thread, and how many rows each returns when run again
 * once the dispatch committed: a read that walks the history returns more rows on a longer one.
 */
const dispatchReads = Effect.fn("dispatchReads")(function* <A, E>(
  threadId: ThreadId,
  dispatch: Effect.Effect<A, E>,
) {
  const sql = yield* SqlClient.SqlClient;
  const statements: Array<readonly [string, ReadonlyArray<unknown>]> = [];
  const observe: Statement.Transformer = (statement) => {
    const [query, params] = statement.compile();
    if (/^\s*(SELECT|WITH)\b/i.test(query) && params.includes(threadId)) {
      statements.push([query, params]);
    }
    return Effect.succeed(statement);
  };
  yield* dispatch.pipe(Effect.provideService(Statement.CurrentTransformer, observe));
  const reads: Array<{ readonly query: string; readonly rows: number }> = [];
  for (const [query, params] of statements) {
    const rows = yield* sql.unsafe(query, params as Array<never>);
    reads.push({ query: query.replace(/\s+/g, " ").trim().slice(0, 160), rows: rows.length });
  }
  return reads;
});

const assertSameReads = (
  short: ReadonlyArray<{ readonly query: string; readonly rows: number }>,
  long: ReadonlyArray<{ readonly query: string; readonly rows: number }>,
) => {
  const grew = long.filter((read, index) => read.rows !== short[index]?.rows);
  assert.deepEqual(
    grew.map((read) => `${read.rows} rows: ${read.query}`),
    [],
    "a dispatch read returns more rows on a longer history",
  );
  assert.equal(long.length, short.length);
};

it.effect("dispatch reads as many rows on a long history as on a short one", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const reads = {
      queued: [] as Array<Array<{ readonly query: string; readonly rows: number }>>,
      delivery: [] as Array<Array<{ readonly query: string; readonly rows: number }>>,
      started: [] as Array<Array<{ readonly query: string; readonly rows: number }>>,
    };
    for (const size of [40, 80]) {
      // Running: a message queues behind the active run, and a delegated result joins the queue.
      const busy = ThreadId.make(`thread:dispatch-reads:busy:${size}`);
      const seeded = yield* seedThread({
        threadId: busy,
        runs: history(size, [{ shown: true, status: "running", tasks: 0 }]),
      });
      const active = seeded.runs.at(-1)!;
      reads.queued.push(
        yield* dispatchReads(
          busy,
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`dispatch-reads:queued:${size}`),
            threadId: busy,
            messageId: MessageId.make(`message:dispatch-reads:queued:${size}`),
            text: "And then this.",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          }),
        ),
      );
      const delivery = yield* openDelivery(busy, active);
      reads.delivery.push(yield* dispatchReads(busy, orchestrator.dispatch(delivery.command)));
      // Idle: a message starts a run, measuring its model switch from the history.
      const idle = ThreadId.make(`thread:dispatch-reads:idle:${size}`);
      yield* seedThread({ threadId: idle, runs: history(size, []) });
      reads.started.push(
        yield* dispatchReads(
          idle,
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`dispatch-reads:started:${size}`),
            threadId: idle,
            messageId: MessageId.make(`message:dispatch-reads:started:${size}`),
            text: "Start over.",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          }),
        ),
      );
    }
    for (const [short, long] of Object.values(reads)) assertSameReads(short!, long!);
  }).pipe(Effect.provide(layerTest)),
);

// The runs, tasks and messages a dispatch decides on, on a thread whose history holds many more.
it.effect("a message from the web releases held runs and queues after them", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:dispatch-held");
    yield* seedThread({
      threadId,
      runs: history(40, [
        { shown: true, status: "running", tasks: 0 },
        { shown: true, status: "queued", queueHeld: true, tasks: 0 },
        { shown: true, status: "queued", queueHeld: true, tasks: 0 },
      ]),
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("dispatch-held"),
      threadId,
      messageId: MessageId.make("message:dispatch-held"),
      text: "Carry on.",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
    });
    const queued = (yield* projections.getThreadProjection(threadId)).runs
      .filter((run) => run.status === "queued")
      .map((run) => [
        run.ordinal,
        run.queuePosition,
        run.queueHeld === true,
        String(run.userMessageId),
      ]);
    assert.deepEqual(queued, [
      [42, 42, false, `message:${threadId}:42`],
      [43, 43, false, `message:${threadId}:43`],
      [44, 44, false, "message:dispatch-held"],
    ]);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a wake takes the work start of the run that started last", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:dispatch-wake");
    yield* seedThread({
      threadId,
      // The last run by ordinal started before the one ahead of it, which finished first.
      runs: history(40, [
        { startedAt: 9_000, completedAt: 9_100 },
        { startedAt: 8_000, completedAt: 9_200 },
      ]),
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("dispatch-wake"),
      threadId,
      messageId: MessageId.make("message:dispatch-wake"),
      text: "Background task completed.",
      notification: {
        source: { kind: "background_task" },
        outcome: "completed",
        summary: "Background task completed",
      },
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
    });
    const run = (yield* projections.getThreadProjection(threadId)).runs.at(-1)!;
    assert.equal(run.userMessageId, "message:dispatch-wake");
    assert.equal(run.status, "starting");
    assert.deepEqual(run.workStartedAt, at(9_000));
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a message after many wakes marks the switch from the last message shown", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:dispatch-model-switch");
    yield* seedThread({
      threadId,
      modelSelection: modelB,
      runs: [
        { shown: true, modelSelection: modelB },
        { shown: true, modelSelection: modelA },
        ...Array.from({ length: 60 }, () => ({ modelSelection: modelB })),
      ],
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("dispatch-model-switch"),
      threadId,
      messageId: MessageId.make("message:dispatch-model-switch"),
      text: "Next.",
      attachments: [],
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    const item = (yield* projections.getThreadProjection(threadId)).turnItems.find(
      (candidate) =>
        candidate.type === "user_message" &&
        candidate.messageId === "message:dispatch-model-switch",
    );
    assert.deepEqual(item?.type === "user_message" ? item.modelSwitch : undefined, {
      from: modelA,
      to: modelB,
    });
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a delegated result queues behind the active run with its task's report", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:dispatch-delivery");
    const seeded = yield* seedThread({
      threadId,
      runs: history(40, [{ shown: true, status: "running", tasks: 0 }]),
    });
    const delivery = yield* openDelivery(threadId, seeded.runs.at(-1)!);
    yield* orchestrator.dispatch(delivery.command);
    const projection = yield* projections.getThreadProjection(threadId);
    const message = projection.messages.find(
      (candidate) => candidate.id === delivery.command.messageId,
    );
    assert.equal(message?.text, `Delegated task ${delivery.taskId} (Look) [completed]:\nFound it.`);
    assert.deepEqual(message?.delegatedCompletion, delivery.command.delegatedCompletion);
    const run = projection.runs.find((candidate) => candidate.userMessageId === message?.id);
    assert.deepEqual(
      [run?.status, run?.ordinal, run?.queuePosition, run?.queueHeld],
      ["queued", 42, 1, undefined],
    );
  }).pipe(Effect.provide(layerTest)),
);

it.effect("a message waits, held, behind a worktree setup that failed", () =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threadId = ThreadId.make("thread:dispatch-worktree");
    yield* seedThread({
      threadId,
      worktreePath: null,
      runs: history(40, [
        {
          shown: true,
          status: "failed",
          tasks: 0,
          workspacePreparation: { type: "worktree", baseRef: "main" },
        },
      ]),
    });
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("dispatch-worktree"),
      threadId,
      messageId: MessageId.make("message:dispatch-worktree"),
      text: "Try again.",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "web",
    });
    const run = (yield* projections.getThreadProjection(threadId)).runs.at(-1)!;
    assert.deepEqual(
      [run.userMessageId, run.status, run.queueHeld, run.workspaceCwd],
      ["message:dispatch-worktree", "queued", true, undefined],
    );
  }).pipe(Effect.provide(layerTest)),
);

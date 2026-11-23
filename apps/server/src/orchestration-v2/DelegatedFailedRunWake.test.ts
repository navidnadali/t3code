// A delegated child's run that fails after its task settled, or that no
// task owns (a Claude Monitor update's wake), is told to the parent once, as a task of its own.
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it, vi } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2PendingBackgroundTask,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../config.ts";
import * as McpSessionRegistryTestkit from "../mcp/McpSessionRegistry.testkit.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ModelManifest from "../provider/ModelManifest.ts";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderTurnStartServiceTestkit from "./ProviderTurnStartService.testkit.ts";
import * as RuntimeLayer from "./runtimeLayer.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

const layerPlatformTest = Layer.merge(
  Layer.mergeAll(NodeServices.layer, NodeCrypto.layer),
  Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
    resolveLink: () => Effect.die("unused title link"),
  }),
);
const layerServerConfig = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-orchestration-v2-delegated-failed-run-",
});
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.4",
} satisfies ModelSelection;
const driver = ProviderDriverKind.make("codex");
const providerInstance = {
  instanceId: modelSelection.instanceId,
  driverKind: driver,
  continuationIdentity: { driverKind: driver, continuationKey: "codex:test" },
  displayName: "Codex test",
  enabled: true,
  snapshot: { getSnapshot: Effect.succeed({}) } as unknown as ProviderInstance["snapshot"],
  orchestrationAdapter: {
    instanceId: modelSelection.instanceId,
    driver,
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: () => Effect.die("sessions are not used by delegated delivery tests"),
  } as ProviderAdapter.ProviderAdapterV2["Service"],
  textGeneration: {} as ProviderInstance["textGeneration"],
} satisfies ProviderInstance;

const layerTest = Layer.mergeAll(
  RuntimeLayer.layer,
  RuntimeLayer.layerEventSink,
  ThreadCommandExecutor.layer,
  EffectOutbox.layer,
).pipe(
  Layer.provideMerge(RuntimeLayer.layerProjectService),
  Layer.provide(
    Layer.mock(WorkspacePaths.WorkspacePaths)({
      normalizeWorkspaceRoot: (workspaceRoot) => Effect.succeed(workspaceRoot),
    }),
  ),
  Layer.provide(ProviderTurnStartServiceTestkit.layer),
  Layer.provide(
    Layer.succeed(ProjectEnrichmentService.ProjectEnrichmentService, {
      peek: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      request: () => Effect.void,
      readRepositoryIdentity: () => Effect.succeed(null),
      getAvailable: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: false,
        }),
      invalidate: () => Effect.void,
      refreshRepositoryIdentity: () => Effect.void,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provide(McpProviderSessions.layer),
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(
    CheckpointStore.layer.pipe(
      Layer.provide(
        VcsDriverRegistry.layer.pipe(
          Layer.provide(VcsProcess.layer),
          Layer.provide(layerServerConfig),
          Layer.provide(layerPlatformTest),
        ),
      ),
    ),
  ),
  Layer.provide(layerServerConfig),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provide(ModelManifest.layerTest),
  Layer.provide(
    Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
      getInstance: (instanceId) =>
        Effect.succeed(instanceId === providerInstance.instanceId ? providerInstance : undefined),
      listInstances: Effect.succeed([providerInstance]),
      listUnavailable: Effect.succeed([]),
      streamChanges: Stream.empty,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(layerPlatformTest),
);

const command = (name: string) => CommandId.make(`command:runtime-reconcile:${name}`);

/** A parent whose spawning run has ended, and its child whose delegated task was delivered. */
const seedLane = (name: string) =>
  Effect.gen(function* () {
    const projects = yield* ProjectService.ProjectService;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const eventSink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const parentThreadId = ThreadId.make(`thread:${name}-parent`);
    const projectId = ProjectId.make(`project:${name}`);
    const parentRunId = RunId.make(`run:${name}-parent`);
    const rootNodeId = NodeId.make(`node:${name}-parent-root`);
    const taskId = NodeId.make(`node:${name}-task`);
    const childThreadId = ThreadId.make(`thread:${name}-child`);
    const providerThreadId = ProviderThreadId.make(`provider-thread:${name}-parent`);
    yield* projects.create({
      commandId: CommandId.make(`command:seed-project:${name}`),
      projectId,
      title: "Delegated failed run",
      workspaceRoot: `/workspace/${name}`,
    });
    yield* orchestrator.dispatch({
      type: "thread.create",
      createdBy: "user",
      creationSource: "web",
      commandId: CommandId.make(`command:seed-create:${name}`),
      threadId: parentThreadId,
      projectId,
      title: "Orchestrator",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
    });
    yield* eventSink.write({
      commandId: CommandId.make(`command:seed-parent:${name}`),
      events: [
        {
          id: EventId.make(`event:${name}:provider-thread`),
          type: "provider-thread.updated",
          threadId: parentThreadId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: {
            id: providerThreadId,
            driver,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId: null,
            appThreadId: parentThreadId,
            ownerNodeId: rootNodeId,
            nativeThreadRef: { driver, nativeId: `native:${name}`, strength: "strong" },
            nativeConversationHeadRef: null,
            status: "active",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`event:${name}:parent-run`),
          type: "run.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: rootNodeId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: {
            id: parentRunId,
            threadId: parentThreadId,
            ordinal: 1,
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            providerThreadId,
            userMessageId: MessageId.make(`message:${name}-parent-user`),
            rootNodeId,
            activeAttemptId: null,
            status: "completed",
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
            delegatedCompletion: { disposition: "open", nextGeneration: 1, delivery: null },
          },
        },
        {
          id: EventId.make(`event:${name}:child-thread`),
          type: "thread.created",
          threadId: childThreadId,
          occurredAt: now,
          payload: {
            createdBy: "agent",
            creationSource: "server",
            id: childThreadId,
            projectId,
            title: `${name} lane`,
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: {
              parentThreadId,
              relationshipToParent: "subagent",
              rootThreadId: parentThreadId,
            },
            forkedFrom: { type: "node", nodeId: taskId },
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
        },
        {
          id: EventId.make(`event:${name}:task`),
          type: "subagent.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: taskId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: {
            id: taskId,
            threadId: parentThreadId,
            runId: parentRunId,
            parentNodeId: rootNodeId,
            origin: "app_owned",
            createdBy: "agent",
            driver,
            providerInstanceId: modelSelection.instanceId,
            providerThreadId: null,
            childThreadId,
            nativeTaskRef: null,
            prompt: `Run ${name}.`,
            title: `${name} lane`,
            model: null,
            completionWake: "always",
            status: "running",
            result: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
          },
        },
      ],
    });
    return { name, parentThreadId, parentRunId, taskId, childThreadId };
  });

type Lane = Effect.Success<ReturnType<typeof seedLane>>;

const childRun = (
  lane: Lane,
  ordinal: number,
  status: OrchestrationV2Run["status"],
  userMessageId: MessageId,
  now: DateTime.Utc,
) => ({
  id: RunId.make(`run:${lane.childThreadId}:${ordinal}`),
  threadId: lane.childThreadId,
  ordinal,
  providerInstanceId: modelSelection.instanceId,
  modelSelection,
  providerThreadId: null,
  userMessageId,
  rootNodeId: null,
  activeAttemptId: null,
  status,
  requestedAt: now,
  startedAt: now,
  completedAt: status === "running" ? null : now,
  checkpointId: null,
  contextHandoffId: null,
});

/** Ends a child run with `text` as its final assistant message. */
const finishRun = (
  lane: Lane,
  run: OrchestrationV2Run,
  text: string,
  status: "completed" | "failed" = "completed",
) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    yield* eventSink.write({
      commandId: command(`${run.id}-finish`),
      events: [
        {
          id: EventId.make(`event:${run.id}:result`),
          type: "message.updated",
          threadId: lane.childThreadId,
          runId: run.id,
          occurredAt: now,
          payload: {
            id: MessageId.make(`message:${run.id}:result`),
            threadId: lane.childThreadId,
            runId: run.id,
            nodeId: null,
            role: "assistant",
            text,
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "server",
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`event:${run.id}:finished`),
          type: "run.updated",
          threadId: lane.childThreadId,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status, startedAt: now, completedAt: now },
        },
      ],
    });
  });

/** The child's first run ends, its task settles, and the parent acknowledges that wake. */
const settleOriginal = (lane: Lane, text: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const now = yield* DateTime.now;
    const run = childRun(
      lane,
      1,
      "running",
      MessageId.make(`message:${lane.childThreadId}:1`),
      now,
    );
    yield* finishRun(lane, run, text);
    yield* orchestrator.recoverDelegatedTasks;
    return yield* readTask(lane, lane.taskId);
  });

const acknowledge = (lane: Lane, taskId: NodeId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "delegated_task.completion-delivery.acknowledge",
      commandId: CommandId.make(`command:${taskId}-ack`),
      parentThreadId: lane.parentThreadId,
      taskId,
      observedByRunId: lane.parentRunId,
    });
  });

const readTask = (lane: Lane, taskId: NodeId) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projection = yield* orchestrator.getThreadProjection(lane.parentThreadId);
    return projection.subagents.find((task) => task.id === taskId) as OrchestrationV2Subagent;
  });

type Start = "background" | "parent-send" | "operator";

/** A new run on the child, started as `start` says; returns the run its follow-up task tags. */
const startFollowUp = (lane: Lane, name: string, start: Start) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const messageId = MessageId.make(`message:${lane.childThreadId}:${name}`);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId:
        start === "background"
          ? CommandId.make(`provider-continuation:${messageId}`)
          : CommandId.make(`command:${lane.childThreadId}:${name}`),
      threadId: lane.childThreadId,
      messageId,
      text: start === "background" ? "Background task completed." : "Check again.",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      ...(start === "background"
        ? {
            createdBy: "agent" as const,
            creationSource: "provider" as const,
            notification: {
              source: { kind: "background_task" as const },
              outcome: "completed" as const,
              summary: "Background task completed.",
            },
          }
        : start === "parent-send"
          ? {
              createdBy: "agent" as const,
              creationSource: "mcp" as const,
              senderThreadId: lane.parentThreadId,
            }
          : { createdBy: "user" as const, creationSource: "web" as const }),
    });
    const run = (yield* orchestrator.getThreadProjection(lane.childThreadId)).runs.find(
      (candidate) => candidate.userMessageId === messageId,
    )!;
    assert.isDefined(run.delegatedTaskId);
    return run;
  });

/** A new run on the child, started as `start` says, ends with `text`; returns its follow-up task. */
const followUp = (
  lane: Lane,
  name: string,
  start: Start,
  text: string,
  status: "completed" | "failed" = "completed",
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const run = yield* startFollowUp(lane, name, start);
    yield* finishRun(lane, run, text, status);
    yield* orchestrator.recoverDelegatedTasks;
    return yield* readTask(lane, run.delegatedTaskId!);
  });

/** The wake text the parent's delivery dispatches for the cohort's current delivery. */
const dispatchWake = (lane: Lane) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const parent = yield* orchestrator.getThreadProjection(lane.parentThreadId);
    const delivery = parent.runs.find((run) => run.id === lane.parentRunId)?.delegatedCompletion
      ?.delivery;
    assert.isDefined(delivery);
    assert.isNotNull(delivery);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`command:${delivery!.messageId}-wake`),
      threadId: lane.parentThreadId,
      messageId: delivery!.messageId,
      text: "Delegated tasks reached a terminal state.",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
      delegatedCompletion: {
        parentRunId: lane.parentRunId,
        generation: delivery!.generation,
        taskIds: delivery!.taskIds,
      },
    });
    const projection = yield* orchestrator.getThreadProjection(lane.parentThreadId);
    return projection.messages.find((message) => message.id === delivery!.messageId)!.text;
  });

const offersTo = (
  offer: { readonly mock: { readonly calls: ReadonlyArray<ReadonlyArray<unknown>> } },
  lane: Lane,
) =>
  offer.mock.calls.filter(
    ([request]) =>
      (request as ProviderContinuationRequests.ProviderContinuationRequest).threadId ===
      lane.parentThreadId,
  ).length;

// A delegated child's Claude Monitor wake run, in which the agent stops the monitor its
// task awaited, ends after the task already settled from the tick before it; when that run
// failed, no task reported it and the parent was never woken.

/** A command whose run updates the run-end reactor takes as a provider's report. */
const reported = (name: string) => CommandId.make(`command:provider-report:${name}`);

/** The child's provider thread lists `tasks` as its background work from now on. */
const childBackgroundWorkEvent = (
  lane: Lane,
  name: string,
  tasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask>,
  now: DateTime.Utc,
) => ({
  id: EventId.make(`event:${lane.childThreadId}:work-${name}`),
  type: "provider-thread.updated" as const,
  threadId: lane.childThreadId,
  driver,
  providerInstanceId: modelSelection.instanceId,
  occurredAt: now,
  payload: {
    id: ProviderThreadId.make(`provider-thread:${lane.name}-child`),
    driver,
    providerInstanceId: modelSelection.instanceId,
    providerSessionId: null,
    appThreadId: lane.childThreadId,
    ownerNodeId: null,
    nativeThreadRef: { driver, nativeId: `native:${lane.name}-child`, strength: "strong" as const },
    nativeConversationHeadRef: null,
    status: "active" as const,
    firstRunOrdinal: 1,
    lastRunOrdinal: null,
    handoffIds: [],
    forkedFrom: null,
    pendingBackgroundTasks: tasks,
    createdAt: now,
    updatedAt: now,
  },
});

const childBackgroundWork = (
  lane: Lane,
  name: string,
  tasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask>,
) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    yield* eventSink.write({
      commandId: command(`${lane.childThreadId}-work-${name}`),
      events: [childBackgroundWorkEvent(lane, name, tasks, yield* DateTime.now)],
    });
  });

const awaitedMonitor = (run: OrchestrationV2Run): OrchestrationV2PendingBackgroundTask => ({
  taskId: "monitor-train",
  kind: "monitor",
  awaited: true,
  runId: run.id,
});

/** A Claude Monitor event wakes the child: its run has no task, as a Monitor update starts none. */
const monitorWake = (lane: Lane, name: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const messageId = MessageId.make(`message:${lane.childThreadId}:${name}`);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`provider-continuation:${messageId}`),
      threadId: lane.childThreadId,
      messageId,
      text: "Background task completed.",
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "provider",
      notification: {
        source: { kind: "monitor" },
        outcome: "updated",
        summary: "Monitor event",
      },
    });
    const run = (yield* orchestrator.getThreadProjection(lane.childThreadId)).runs.find(
      (candidate) => candidate.userMessageId === messageId,
    )!;
    assert.isUndefined(run.delegatedTaskId, "a Monitor update's run belongs to no task");
    return run;
  });

/** The run is live in its provider, as a wake run is while the agent works in it. */
const markRunning = (lane: Lane, run: OrchestrationV2Run, commandId?: CommandId) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const running = { ...run, status: "running" as const, startedAt: now, completedAt: null };
    const writeId = commandId ?? command(`${run.id}-running`);
    yield* eventSink.write({
      commandId: writeId,
      events: [
        {
          id: EventId.make(`event:${writeId}:running`),
          type: "run.updated",
          threadId: lane.childThreadId,
          runId: run.id,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: running,
        },
      ],
    });
    return running;
  });

const STREAM_CLOSED = "The provider event stream closed unexpectedly. Retry the turn.";

/** The run fails as a closed event stream fails it; `commandId` decides whether the run-end reactor sees it. */
const streamClosed = (lane: Lane, run: OrchestrationV2Run, commandId: CommandId) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const afterSequence = yield* eventSink.latestSequence();
    yield* eventSink.write({
      commandId,
      events: [
        {
          id: EventId.make(`event:${commandId}:error`),
          type: "turn-item.updated",
          threadId: lane.childThreadId,
          runId: run.id,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: {
            id: TurnItemId.make(`turn-item:${run.id}:stream-closed`),
            type: "error",
            threadId: lane.childThreadId,
            runId: run.id,
            nodeId: run.rootNodeId,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: run.ordinal * 100,
            status: "failed",
            title: "Provider failure",
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            failure: { class: "unknown", message: STREAM_CLOSED, code: null, retryable: null },
          },
        },
        {
          id: EventId.make(`event:${commandId}:failed`),
          type: "run.updated",
          threadId: lane.childThreadId,
          runId: run.id,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: { ...run, status: "failed", startedAt: run.startedAt ?? now, completedAt: now },
        },
      ],
    });
    return afterSequence;
  });

/**
 * Returns once the run-end reactions already queued for the lane's parent have run. Its children
 * share one reaction queue, in order: a sibling child's run ends behind them, and its reaction
 * settling the sibling's task marks that theirs are done. The sibling's task belongs to no parent
 * run, so it is never delivered.
 */
const reactionBarrier = (lane: Lane, name: string) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const childThreadId = ThreadId.make(`thread:${lane.name}-barrier-${name}`);
    const taskId = NodeId.make(`node:${lane.name}-barrier-${name}`);
    const runId = RunId.make(`run:${childThreadId}:1`);
    yield* eventSink.write({
      commandId: command(`${childThreadId}-seed`),
      events: [
        {
          id: EventId.make(`event:${childThreadId}:thread`),
          type: "thread.created",
          threadId: childThreadId,
          occurredAt: now,
          payload: {
            createdBy: "agent",
            creationSource: "server",
            id: childThreadId,
            projectId: ProjectId.make(`project:${lane.name}`),
            title: `${lane.name} barrier`,
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: {
              parentThreadId: lane.parentThreadId,
              relationshipToParent: "subagent",
              rootThreadId: lane.parentThreadId,
            },
            forkedFrom: { type: "node", nodeId: taskId },
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
        },
        {
          id: EventId.make(`event:${childThreadId}:task`),
          type: "subagent.updated",
          threadId: lane.parentThreadId,
          nodeId: taskId,
          driver,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: {
            id: taskId,
            threadId: lane.parentThreadId,
            runId: RunId.make(`run:${childThreadId}:no-parent-run`),
            parentNodeId: NodeId.make(`node:${childThreadId}:no-parent-root`),
            origin: "app_owned",
            createdBy: "agent",
            driver,
            providerInstanceId: modelSelection.instanceId,
            providerThreadId: null,
            childThreadId,
            nativeTaskRef: null,
            prompt: "Barrier.",
            title: `${lane.name} barrier`,
            model: null,
            completionWake: "always",
            status: "running",
            result: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
          },
        },
      ],
    });
    const afterSequence = yield* eventSink.latestSequence();
    yield* eventSink.write({
      commandId: reported(`${childThreadId}-end`),
      events: [
        {
          id: EventId.make(`event:${childThreadId}:result`),
          type: "message.updated",
          threadId: childThreadId,
          runId,
          occurredAt: now,
          payload: {
            id: MessageId.make(`message:${childThreadId}:result`),
            threadId: childThreadId,
            runId,
            nodeId: null,
            role: "assistant",
            text: "Barrier.",
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "server",
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`event:${childThreadId}:ended`),
          type: "run.updated",
          threadId: childThreadId,
          runId,
          occurredAt: now,
          payload: {
            id: runId,
            threadId: childThreadId,
            ordinal: 1,
            providerInstanceId: modelSelection.instanceId,
            modelSelection,
            providerThreadId: null,
            userMessageId: MessageId.make(`message:${childThreadId}:1`),
            rootNodeId: null,
            activeAttemptId: null,
            status: "completed",
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
          },
        },
      ],
    });
    yield* eventSink.stream({ afterSequence, eventType: "subagent.updated" }).pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "subagent.updated" &&
          stored.event.payload.id === taskId &&
          stored.event.payload.result !== null,
      ),
      Stream.take(1),
      Stream.runDrain,
    );
  });

const childTasks = (lane: Lane) =>
  Orchestrator.OrchestratorV2.pipe(
    Effect.flatMap((orchestrator) => orchestrator.getThreadProjection(lane.parentThreadId)),
    Effect.map((projection) =>
      projection.subagents.filter((task) => task.childThreadId === lane.childThreadId),
    ),
  );

/**
 * A parent-send follow-up whose child starts a Monitor it awaits and ends its turn; one Monitor
 * tick runs and reports, and a second tick's run is live when the agent stops the Monitor.
 */
const monitorStoppedMidRun = (name: string, earlierResult?: string) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const lane = yield* seedLane(name);
    yield* settleOriginal(lane, "First report.");
    yield* acknowledge(lane, lane.taskId);
    const earlier =
      earlierResult === undefined
        ? undefined
        : yield* followUp(lane, "earlier", "parent-send", earlierResult);
    const send = yield* startFollowUp(lane, "land", "parent-send");
    const taskId = send.delegatedTaskId!;
    yield* childBackgroundWork(lane, "monitor-started", [awaitedMonitor(send)]);
    yield* finishRun(lane, send, "Landing 5777; watching the merge train.");
    yield* orchestrator.recoverDelegatedTasks;
    assert.isNull((yield* readTask(lane, taskId)).result, "the task awaits its Monitor");
    const tick = yield* monitorWake(lane, "tick-1");
    yield* finishRun(lane, tick, "Train still running.");
    yield* orchestrator.recoverDelegatedTasks;
    assert.isNull((yield* readTask(lane, taskId)).result, "a tick keeps the task awaiting");
    const stopping = yield* markRunning(lane, yield* monitorWake(lane, "tick-2"));
    // The agent's TaskStop on its Monitor: the work the task awaited ends inside this run.
    yield* childBackgroundWork(lane, "monitor-stopped", []);
    yield* orchestrator.recoverDelegatedTasks;
    return { lane, taskId, tick, stopping, earlier };
  });

/** The tasks of the lane's child that report `runId`. */
const toldBy = (lane: Lane, runId: RunId) =>
  childTasks(lane).pipe(Effect.map((rows) => rows.filter((row) => row.resultRunId === runId)));

it.layer(Layer.merge(layerTest, ProviderContinuationRequests.layer))(
  "a delegated run that fails with no task to report it",
  (it) => {
    it.effect("a parent hears once that the run that stopped the Monitor failed", () =>
      Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        const { lane, taskId, stopping } = yield* monitorStoppedMidRun("pub-ps046a");
        using offer = vi.spyOn(requests, "offer");
        yield* streamClosed(lane, stopping, reported(`${stopping.id}-closed`));
        yield* reactionBarrier(lane, "failed");
        const told = yield* toldBy(lane, stopping.id);
        assert.lengthOf(told, 1, "the failed run is reported by no task");
        const failure = told[0]!;
        assert.notEqual(failure.id, taskId);
        assert.equal(failure.status, "failed");
        assert.include(failure.result ?? "", "event stream closed unexpectedly");
        assert.equal(failure.completionDelivery?.state, "claimed");
        assert.isAbove(offersTo(offer, lane), 0);
        const childRuns = (yield* Orchestrator.OrchestratorV2.pipe(
          Effect.flatMap((orchestrator) => orchestrator.getThreadProjection(lane.childThreadId)),
        )).runs;
        assert.equal(
          childRuns.find((run) => run.id === stopping.id)?.delegatedTaskId,
          failure.id,
          "the failed run is the failure task's run",
        );
        const text = yield* dispatchWake(lane);
        assert.include(text, "[failed]");
        assert.include(text, String(failure.id));
        assert.include(text, "event stream closed unexpectedly");
      }),
    );

    it.effect(
      "a failed run's own task tells that failure though a later Monitor tick ended before its reaction",
      () =>
        Effect.gen(function* () {
          const eventSink = yield* EventSink.EventSinkV2;
          const lane = yield* seedLane("pub-ps046-later-tick");
          yield* settleOriginal(lane, "First report.");
          yield* acknowledge(lane, lane.taskId);
          const failing = yield* markRunning(lane, yield* monitorWake(lane, "tick-1"));
          const later = yield* monitorWake(lane, "tick-2");
          const now = yield* DateTime.now;
          yield* streamClosed(lane, failing, command(`${failing.id}-closed`));
          // The failure's reaction runs only after the next tick has run and ended.
          yield* eventSink.write({
            commandId: reported(`${failing.id}-closed-then-tick`),
            events: [
              {
                id: EventId.make(`event:${failing.id}:closed-again`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: failing.id,
                providerInstanceId: modelSelection.instanceId,
                occurredAt: now,
                payload: { ...failing, status: "failed", completedAt: now },
              },
              {
                id: EventId.make(`event:${later.id}:tick-result`),
                type: "message.updated",
                threadId: lane.childThreadId,
                runId: later.id,
                occurredAt: now,
                payload: {
                  id: MessageId.make(`message:${later.id}:result`),
                  threadId: lane.childThreadId,
                  runId: later.id,
                  nodeId: null,
                  role: "assistant",
                  text: "Train still running.",
                  attachments: [],
                  streaming: false,
                  createdBy: "agent",
                  creationSource: "server",
                  createdAt: now,
                  updatedAt: now,
                },
              },
              {
                id: EventId.make(`event:${later.id}:tick-ended`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: later.id,
                occurredAt: now,
                payload: { ...later, status: "completed", startedAt: now, completedAt: now },
              },
            ],
          });
          yield* reactionBarrier(lane, "failed-then-tick");
          const told = yield* toldBy(lane, failing.id);
          assert.lengthOf(told, 1, "the failure was told by no task");
          assert.equal(told[0]!.status, "failed");
          assert.include(told[0]!.result ?? "", "event stream closed unexpectedly");
          assert.lengthOf(yield* toldBy(lane, later.id), 0);
        }),
    );

    // The server can stop between a failed run's commit and its run-end reaction; the reactor of
    // the next start reads only later events, so startup recovery tells such a failure.
    it.effect(
      "restart: a failure committed before its run-end reaction is told once by startup recovery",
      () =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const { lane, stopping } = yield* monitorStoppedMidRun("pub-ps046-restart");
          // A reconcile's run end gets no reaction, as a run end committed before a stop does not.
          yield* streamClosed(lane, stopping, command(`${stopping.id}-closed`));
          yield* reactionBarrier(lane, "failed");
          assert.lengthOf(yield* toldBy(lane, stopping.id), 0, "the run end reacted");
          yield* orchestrator.recoverDelegatedTasks;
          const told = yield* toldBy(lane, stopping.id);
          assert.lengthOf(told, 1, "startup recovery told no failure");
          assert.equal(told[0]!.status, "failed");
          assert.include(told[0]!.result ?? "", "event stream closed unexpectedly");
          yield* orchestrator.recoverDelegatedTasks;
          assert.deepEqual(
            (yield* toldBy(lane, stopping.id)).map((row) => row.id),
            [told[0]!.id],
            "a second recovery told it again",
          );
        }),
    );

    // The untold failure need not be the child's last run: a Monitor tick can run and end after it,
    // reported by no task, before recovery looks.
    it.effect(
      "restart: startup recovery tells a cut-off failure a later unreported run ended after",
      () =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const eventSink = yield* EventSink.EventSinkV2;
          const { lane, stopping } = yield* monitorStoppedMidRun("pub-ps046-restart-later");
          yield* streamClosed(lane, stopping, command(`${stopping.id}-closed`));
          const later = yield* monitorWake(lane, "tick-after-failure");
          const now = yield* DateTime.now;
          yield* eventSink.write({
            commandId: reported(`${later.id}-ended`),
            events: [
              {
                id: EventId.make(`event:${later.id}:tick-ended`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: later.id,
                occurredAt: now,
                payload: { ...later, status: "completed", startedAt: now, completedAt: now },
              },
            ],
          });
          yield* reactionBarrier(lane, "failed-then-tick");
          assert.lengthOf(yield* toldBy(lane, stopping.id), 0, "the run end reacted");
          yield* orchestrator.recoverDelegatedTasks;
          const told = yield* toldBy(lane, stopping.id);
          assert.lengthOf(told, 1, "startup recovery told no failure");
          assert.equal(told[0]!.status, "failed");
          assert.lengthOf(yield* toldBy(lane, later.id), 0);
          yield* orchestrator.recoverDelegatedTasks;
          assert.lengthOf(yield* toldBy(lane, stopping.id), 1, "a second recovery told it again");
        }),
    );

    it.effect(
      "exactly once: recovery and a repeated failure record add no task; a later failed run is told once more",
      () =>
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const { lane, stopping } = yield* monitorStoppedMidRun("pub-ps046-once");
          yield* streamClosed(lane, stopping, reported(`${stopping.id}-closed`));
          yield* reactionBarrier(lane, "failed");
          const first = yield* toldBy(lane, stopping.id);
          assert.lengthOf(first, 1, "the failure is told once");
          const deliveryOf = Effect.gen(function* () {
            const parent = yield* orchestrator.getThreadProjection(lane.parentThreadId);
            return parent.runs.find((run) => run.id === lane.parentRunId)?.delegatedCompletion
              ?.delivery;
          });
          const delivery = yield* deliveryOf;
          assert.include(delivery?.taskIds ?? [], first[0]!.id);
          yield* orchestrator.recoverDelegatedTasks;
          yield* markRunning(lane, stopping, reported(`${stopping.id}-running-again`));
          yield* streamClosed(lane, stopping, reported(`${stopping.id}-closed-again`));
          yield* reactionBarrier(lane, "failed-again");
          yield* orchestrator.recoverDelegatedTasks;
          assert.deepEqual(
            (yield* toldBy(lane, stopping.id)).map((row) => row.id),
            [first[0]!.id],
            "recovery or a repeated record told the failure again",
          );
          assert.deepEqual(yield* deliveryOf, delivery, "the wake was planned again");
          const later = yield* markRunning(lane, yield* monitorWake(lane, "tick-3"));
          yield* streamClosed(lane, later, reported(`${later.id}-closed`));
          yield* reactionBarrier(lane, "later-failed");
          const laterTold = yield* toldBy(lane, later.id);
          assert.lengthOf(laterTold, 1, "a later failed run is told once");
          assert.equal(laterTold[0]!.status, "failed");
        }),
    );

    it.effect(
      "a failed Monitor wake run is told at once though a task still awaits other work",
      () =>
        Effect.gen(function* () {
          const lane = yield* seedLane("pub-ps046-awaits");
          yield* settleOriginal(lane, "First report.");
          yield* acknowledge(lane, lane.taskId);
          const send = yield* startFollowUp(lane, "land", "parent-send");
          const taskId = send.delegatedTaskId!;
          const pipeline: OrchestrationV2PendingBackgroundTask = {
            taskId: "pipeline-watch",
            kind: "command",
            awaited: true,
            runId: send.id,
          };
          yield* childBackgroundWork(lane, "started", [awaitedMonitor(send), pipeline]);
          yield* finishRun(lane, send, "Landing 5777; watching the train and the pipeline.");
          const stopping = yield* markRunning(lane, yield* monitorWake(lane, "tick-1"));
          yield* childBackgroundWork(lane, "monitor-stopped", [pipeline]);
          yield* streamClosed(lane, stopping, reported(`${stopping.id}-closed`));
          yield* reactionBarrier(lane, "failed");
          assert.isNull(
            (yield* readTask(lane, taskId)).result,
            "the task still awaits the pipeline",
          );
          // The task reports its own runs only, so the Monitor wake's failure is its own record.
          const told = yield* toldBy(lane, stopping.id);
          assert.lengthOf(told, 1);
          assert.equal(told[0]!.status, "failed");
        }),
    );

    it.effect("guard: a failed run the open original task reports is not told again", () =>
      Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const lane = yield* seedLane("pub-ps046-original");
        const now = yield* DateTime.now;
        const run = yield* markRunning(
          lane,
          childRun(lane, 1, "running", MessageId.make(`message:${lane.childThreadId}:1`), now),
        );
        yield* streamClosed(lane, run, reported(`${run.id}-closed`));
        yield* reactionBarrier(lane, "failed");
        yield* orchestrator.recoverDelegatedTasks;
        const told = yield* toldBy(lane, run.id);
        assert.deepEqual(
          told.map((row) => row.id),
          [lane.taskId],
        );
        assert.equal(told[0]!.status, "failed");
      }),
    );

    it.effect.each([{ end: "cancelled" }, { end: "interrupted" }] as const)(
      "guard: a Monitor wake run that was $end is no failure",
      ({ end }) =>
        Effect.gen(function* () {
          const eventSink = yield* EventSink.EventSinkV2;
          const { lane, taskId, stopping } = yield* monitorStoppedMidRun(`pub-ps046-${end}`);
          const now = yield* DateTime.now;
          yield* eventSink.write({
            commandId: reported(`${stopping.id}-${end}`),
            events: [
              {
                id: EventId.make(`event:${stopping.id}:${end}`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: stopping.id,
                providerInstanceId: modelSelection.instanceId,
                occurredAt: now,
                payload: { ...stopping, status: end, completedAt: now },
              },
            ],
          });
          yield* reactionBarrier(lane, end);
          assert.isFalse(
            (yield* childTasks(lane)).some((row) => row.id !== taskId && row.id !== lane.taskId),
            "a stopped run became a failure task",
          );
        }),
    );

    it.effect("guard: a failed run a person stopped tells no failure", () =>
      Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const lane = yield* seedLane("pub-ps046-person-stop");
        yield* settleOriginal(lane, "First report.");
        yield* acknowledge(lane, lane.taskId);
        const wake = yield* markRunning(lane, yield* monitorWake(lane, "tick-1"));
        using offer = vi.spyOn(requests, "offer");
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make(`command:${lane.name}-person-stop`),
          threadId: lane.childThreadId,
        });
        yield* streamClosed(lane, wake, reported(`${wake.id}-closed`));
        yield* reactionBarrier(lane, "stopped-failed");
        assert.lengthOf(yield* toldBy(lane, wake.id), 0, "a run a person stopped was told");
        assert.equal(offersTo(offer, lane), 0);
      }),
    );

    it.effect(
      "guard: an agent's cancel of a task whose run then fails tells no one of the failure",
      () =>
        Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const lane = yield* seedLane("pub-ps046-agent-cancel");
          yield* settleOriginal(lane, "First report.");
          yield* acknowledge(lane, lane.taskId);
          const send = yield* markRunning(lane, yield* startFollowUp(lane, "land", "parent-send"));
          const taskId = send.delegatedTaskId!;
          using offer = vi.spyOn(requests, "offer");
          yield* orchestrator.cancelDelegatedTask({
            parentThreadId: lane.parentThreadId,
            taskId,
            command: {
              type: "thread.stop",
              commandId: CommandId.make(`command:${lane.name}-cancel`),
              threadId: lane.childThreadId,
              stopDelegatedTasks: false,
            },
            stopDescendants: () => Effect.void,
          });
          yield* streamClosed(lane, send, reported(`${send.id}-closed`));
          yield* reactionBarrier(lane, "failed");
          const tasks = yield* childTasks(lane);
          assert.equal(tasks.find((row) => row.id === taskId)?.cancelledRunId, send.id);
          assert.isFalse(
            tasks.some((row) => row.id !== taskId && row.id !== lane.taskId),
            "the cancelled run's failure became a failure task",
          );
          assert.equal(offersTo(offer, lane), 0);
        }),
    );

    it.effect(
      "a Monitor wake's failure is told though many later ticks ended before its reaction, with the original task open",
      () =>
        Effect.gen(function* () {
          const eventSink = yield* EventSink.EventSinkV2;
          const lane = yield* seedLane("pub-ps046-many-ticks");
          const now = yield* DateTime.now;
          const first = childRun(
            lane,
            1,
            "running",
            MessageId.make(`message:${lane.childThreadId}:1`),
            now,
          );
          // The original task's run starts a Monitor it awaits and ends: the task stays open.
          yield* childBackgroundWork(lane, "monitor-started", [awaitedMonitor(first)]);
          yield* finishRun(lane, first, "Watching the train.");
          const failing = yield* markRunning(lane, yield* monitorWake(lane, "tick-1"));
          const later: Array<OrchestrationV2Run> = [];
          for (let tick = 2; tick <= 19; tick++)
            later.push(yield* monitorWake(lane, `tick-${tick}`));
          const ended = yield* DateTime.now;
          // The failure's reaction runs only after 18 later ticks have run and ended.
          yield* streamClosed(lane, failing, command(`${failing.id}-closed`));
          yield* eventSink.write({
            commandId: reported(`${failing.id}-closed-then-ticks`),
            events: [
              {
                id: EventId.make(`event:${failing.id}:closed-again`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: failing.id,
                providerInstanceId: modelSelection.instanceId,
                occurredAt: ended,
                payload: { ...failing, status: "failed", completedAt: ended },
              },
              ...later.map((run) => ({
                id: EventId.make(`event:${run.id}:tick-ended`),
                type: "run.updated" as const,
                threadId: lane.childThreadId,
                runId: run.id,
                occurredAt: ended,
                payload: {
                  ...run,
                  status: "completed" as const,
                  startedAt: ended,
                  completedAt: ended,
                },
              })),
            ],
          });
          yield* reactionBarrier(lane, "failed-then-ticks");
          assert.isNull(
            (yield* readTask(lane, lane.taskId)).result,
            "the original task awaits its Monitor",
          );
          const told = yield* toldBy(lane, failing.id);
          assert.lengthOf(told, 1, "the failure was told by no task");
          assert.equal(told[0]!.status, "failed");
        }),
    );

    it.effect(
      "guard: the restart continuation of a run a promoted follow-up joined is that follow-up's to report",
      () =>
        Effect.gen(function* () {
          const eventSink = yield* EventSink.EventSinkV2;
          const lane = yield* seedLane("pub-ps046-joined-continuation");
          yield* settleOriginal(lane, "First report.");
          yield* acknowledge(lane, lane.taskId);
          const send = yield* startFollowUp(lane, "land", "parent-send");
          const followUpId = send.delegatedTaskId!;
          // The follow-up's run starts work it awaits and ends: the follow-up stays open.
          yield* childBackgroundWork(lane, "pipeline", [
            { taskId: "pipeline-watch", kind: "command", awaited: true, runId: send.id },
          ]);
          yield* finishRun(lane, send, "Landing 5777; watching the pipeline.");
          // A later message of the follow-up was steered into a Monitor wake's run, which a
          // restart cut; the restart's continuation of that run then fails.
          const joined = yield* markRunning(lane, yield* monitorWake(lane, "tick-1"));
          const now = yield* DateTime.now;
          const continuationId = RunId.make(`run:${lane.childThreadId}:continuation`);
          const continuationMessageId = MessageId.make(`message:${lane.childThreadId}:continue`);
          const continuation: OrchestrationV2Run = {
            ...joined,
            id: continuationId,
            ordinal: joined.ordinal + 1,
            userMessageId: continuationMessageId,
            restartContinuationOfRunId: joined.id,
            status: "running",
            requestedAt: now,
            startedAt: now,
            completedAt: null,
          };
          yield* eventSink.write({
            commandId: command(`${lane.name}-joined-cut`),
            events: [
              {
                id: EventId.make(`event:${joined.id}:promoted-message`),
                type: "message.updated",
                threadId: lane.childThreadId,
                runId: joined.id,
                occurredAt: now,
                payload: {
                  id: MessageId.make(`message:${joined.id}:promoted`),
                  threadId: lane.childThreadId,
                  runId: joined.id,
                  nodeId: null,
                  role: "user",
                  text: "Then land 5800.",
                  attachments: [],
                  streaming: false,
                  createdBy: "agent",
                  creationSource: "mcp",
                  senderThreadId: lane.parentThreadId,
                  promotedTaskId: followUpId,
                  createdAt: now,
                  updatedAt: now,
                },
              },
              {
                id: EventId.make(`event:${joined.id}:cut`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: joined.id,
                occurredAt: now,
                payload: { ...joined, status: "interrupted", completedAt: now },
              },
              {
                id: EventId.make(`event:${continuationId}:message`),
                type: "message.updated",
                threadId: lane.childThreadId,
                runId: continuationId,
                occurredAt: now,
                payload: {
                  id: continuationMessageId,
                  threadId: lane.childThreadId,
                  runId: continuationId,
                  nodeId: null,
                  role: "user",
                  text: "Continue where the restart cut you off.",
                  attachments: [],
                  streaming: false,
                  createdBy: "agent",
                  creationSource: "server",
                  createdAt: now,
                  updatedAt: now,
                },
              },
              {
                id: EventId.make(`event:${continuationId}:running`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: continuationId,
                occurredAt: now,
                payload: continuation,
              },
            ],
          });
          // A failure the continuation's start never retries (a permission error).
          yield* eventSink.write({
            commandId: reported(`${continuationId}-failed`),
            events: [
              {
                id: EventId.make(`event:${continuationId}:error`),
                type: "turn-item.updated",
                threadId: lane.childThreadId,
                runId: continuationId,
                providerInstanceId: modelSelection.instanceId,
                occurredAt: now,
                payload: {
                  id: TurnItemId.make(`turn-item:${continuationId}:error`),
                  type: "error",
                  threadId: lane.childThreadId,
                  runId: continuationId,
                  // The run's root failure, so its class (no start retry) is read.
                  nodeId: continuation.rootNodeId,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 9_000,
                  status: "failed",
                  title: "Provider failure",
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                  failure: {
                    class: "permission_error",
                    message: "Permission denied.",
                    code: null,
                    retryable: null,
                  },
                },
              },
              {
                id: EventId.make(`event:${continuationId}:failed`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: continuationId,
                providerInstanceId: modelSelection.instanceId,
                occurredAt: now,
                payload: { ...continuation, status: "failed", completedAt: now },
              },
            ],
          });
          yield* reactionBarrier(lane, "continuation-failed");
          assert.isNull(
            (yield* readTask(lane, followUpId)).result,
            "the follow-up still awaits its pipeline",
          );
          assert.isFalse(
            (yield* childTasks(lane)).some(
              (row) => row.id !== followUpId && row.id !== lane.taskId,
            ),
            "the follow-up's own continuation was told as an untold failure",
          );
        }),
    );

    it.effect(
      "the failed restart continuation of a follow-up's result wake is no task's, so it is told",
      () =>
        Effect.gen(function* () {
          const eventSink = yield* EventSink.EventSinkV2;
          const lane = yield* seedLane("pub-ps046-wake-continuation");
          yield* settleOriginal(lane, "First report.");
          yield* acknowledge(lane, lane.taskId);
          const send = yield* startFollowUp(lane, "land", "parent-send");
          const followUpId = send.delegatedTaskId!;
          // The follow-up's run starts work it awaits and ends: the follow-up stays open.
          yield* childBackgroundWork(lane, "pipeline", [
            { taskId: "pipeline-watch", kind: "command", awaited: true, runId: send.id },
          ]);
          yield* finishRun(lane, send, "Landing 5777; watching the pipeline.");
          // A result wake for the follow-up's run ran untagged and a restart cut it; the restart's
          // continuation of it then fails. delegatedTaskRuns gives the wake to the follow-up, but
          // not its continuation, which no task reports.
          const now = yield* DateTime.now;
          const base = (yield* Orchestrator.OrchestratorV2.pipe(
            Effect.flatMap((orchestrator) => orchestrator.getThreadProjection(lane.childThreadId)),
          )).runs.find((run) => run.id === send.id)!;
          const { delegatedTaskId: _tag, ...untagged } = base;
          const wakeId = RunId.make(`run:${lane.childThreadId}:result-wake`);
          const wakeMessageId = MessageId.make(`message:${lane.childThreadId}:result-wake`);
          const continuationId = RunId.make(`run:${lane.childThreadId}:wake-continuation`);
          const continuationMessageId = MessageId.make(`message:${lane.childThreadId}:continue`);
          const wake: OrchestrationV2Run = {
            ...untagged,
            id: wakeId,
            ordinal: base.ordinal + 1,
            userMessageId: wakeMessageId,
            status: "interrupted",
            requestedAt: now,
            startedAt: now,
            completedAt: now,
          };
          const continuation: OrchestrationV2Run = {
            ...untagged,
            id: continuationId,
            ordinal: base.ordinal + 2,
            userMessageId: continuationMessageId,
            restartContinuationOfRunId: wakeId,
            status: "running",
            requestedAt: now,
            startedAt: now,
            completedAt: null,
          };
          const userMessage = (id: MessageId, runId: RunId, text: string) => ({
            id,
            threadId: lane.childThreadId,
            runId,
            nodeId: null,
            role: "user" as const,
            text,
            attachments: [],
            streaming: false,
            createdBy: "agent" as const,
            creationSource: "server" as const,
            createdAt: now,
            updatedAt: now,
          });
          yield* eventSink.write({
            commandId: command(`${lane.name}-wake-cut`),
            events: [
              {
                id: EventId.make(`event:${wakeId}:message`),
                type: "message.updated",
                threadId: lane.childThreadId,
                runId: wakeId,
                occurredAt: now,
                payload: {
                  ...userMessage(wakeMessageId, wakeId, "Delegated task finished."),
                  delegatedCompletion: {
                    parentRunId: send.id,
                    generation: 1,
                    taskIds: [NodeId.make(`node:${lane.name}-grandchild`)],
                  },
                },
              },
              {
                id: EventId.make(`event:${wakeId}:cut`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: wakeId,
                occurredAt: now,
                payload: wake,
              },
              {
                id: EventId.make(`event:${continuationId}:message`),
                type: "message.updated",
                threadId: lane.childThreadId,
                runId: continuationId,
                occurredAt: now,
                payload: userMessage(
                  continuationMessageId,
                  continuationId,
                  "Continue where the restart cut you off.",
                ),
              },
              {
                id: EventId.make(`event:${continuationId}:running`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: continuationId,
                occurredAt: now,
                payload: continuation,
              },
            ],
          });
          yield* eventSink.write({
            commandId: reported(`${continuationId}-failed`),
            events: [
              {
                id: EventId.make(`event:${continuationId}:error`),
                type: "turn-item.updated",
                threadId: lane.childThreadId,
                runId: continuationId,
                providerInstanceId: modelSelection.instanceId,
                occurredAt: now,
                payload: {
                  id: TurnItemId.make(`turn-item:${continuationId}:error`),
                  type: "error",
                  threadId: lane.childThreadId,
                  runId: continuationId,
                  // The run's root failure, so its class (no start retry) is read.
                  nodeId: continuation.rootNodeId,
                  providerThreadId: null,
                  providerTurnId: null,
                  nativeItemRef: null,
                  parentItemId: null,
                  ordinal: 9_000,
                  status: "failed",
                  title: "Provider failure",
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                  failure: {
                    class: "permission_error",
                    message: "Permission denied.",
                    code: null,
                    retryable: null,
                  },
                },
              },
              {
                id: EventId.make(`event:${continuationId}:failed`),
                type: "run.updated",
                threadId: lane.childThreadId,
                runId: continuationId,
                providerInstanceId: modelSelection.instanceId,
                occurredAt: now,
                payload: { ...continuation, status: "failed", completedAt: now },
              },
            ],
          });
          yield* reactionBarrier(lane, "wake-continuation-failed");
          assert.isNull(
            (yield* readTask(lane, followUpId)).result,
            "the follow-up still awaits its pipeline",
          );
          const told = yield* toldBy(lane, continuationId);
          assert.lengthOf(
            told,
            1,
            "the continuation's failure, which no task reports, was not told",
          );
          assert.notEqual(told[0]!.id, followUpId);
          assert.equal(told[0]!.status, "failed");
        }),
    );

    it.effect(
      "guard: a Monitor wake's run a promoted follow-up joined is that follow-up's to report when it fails",
      () =>
        Effect.gen(function* () {
          const eventSink = yield* EventSink.EventSinkV2;
          const lane = yield* seedLane("pub-ps046-joined-monitor");
          yield* settleOriginal(lane, "First report.");
          yield* acknowledge(lane, lane.taskId);
          const send = yield* startFollowUp(lane, "land", "parent-send");
          const followUpId = send.delegatedTaskId!;
          yield* childBackgroundWork(lane, "pipeline", [
            { taskId: "pipeline-watch", kind: "command", awaited: true, runId: send.id },
          ]);
          yield* finishRun(lane, send, "Landing 5777; watching the pipeline.");
          // A later message of the follow-up was steered into a Monitor wake's run, which fails.
          const joined = yield* markRunning(lane, yield* monitorWake(lane, "tick-1"));
          const now = yield* DateTime.now;
          yield* eventSink.write({
            commandId: command(`${lane.name}-joined`),
            events: [
              {
                id: EventId.make(`event:${joined.id}:promoted-message`),
                type: "message.updated",
                threadId: lane.childThreadId,
                runId: joined.id,
                occurredAt: now,
                payload: {
                  id: MessageId.make(`message:${joined.id}:promoted`),
                  threadId: lane.childThreadId,
                  runId: joined.id,
                  nodeId: null,
                  role: "user",
                  text: "Then land 5800.",
                  attachments: [],
                  streaming: false,
                  createdBy: "agent",
                  creationSource: "mcp",
                  senderThreadId: lane.parentThreadId,
                  promotedTaskId: followUpId,
                  createdAt: now,
                  updatedAt: now,
                },
              },
            ],
          });
          yield* streamClosed(lane, joined, reported(`${joined.id}-closed`));
          yield* reactionBarrier(lane, "joined-failed");
          assert.isNull(
            (yield* readTask(lane, followUpId)).result,
            "the follow-up still awaits its pipeline",
          );
          assert.isFalse(
            (yield* childTasks(lane)).some(
              (row) => row.id !== followUpId && row.id !== lane.taskId,
            ),
            "the follow-up's own run was told as an untold failure",
          );
        }),
    );
  },
);

import type { SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ClaudeSettings,
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ScheduledTaskUpsertInput,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ClaudeAdapterV2 from "./Adapters/ClaudeAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

const sessionId = "waiting-run-delivery-session";
const settings = Schema.decodeSync(ClaudeSettings)({});
const decodeScheduledTask = Schema.decodeEffect(ScheduledTaskUpsertInput);
const modelSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-sonnet-4-6",
};
const taskCount = 3;

const result = (uuid: string) =>
  ({
    type: "result",
    subtype: "success",
    uuid,
    session_id: sessionId,
    is_error: false,
    num_turns: 1,
    result: "",
    stop_reason: "end_turn",
    terminal_reason: "completed",
    permission_denials: [],
    duration_ms: 1,
    duration_api_ms: 1,
    total_cost_usd: 0,
    usage: { input_tokens: 1, output_tokens: 1 },
    modelUsage: {},
  }) as unknown as SDKMessage;

const offerText = (offer: SDKUserMessage) => {
  const content = offer.message.content;
  return typeof content === "string"
    ? content
    : content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n");
};

const scheduledTasks = ScheduledTaskService.layer.pipe(
  Layer.provide(ThreadManagementService.layer),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(
    Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
      ensureTranscript: () => Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 }),
    }),
  ),
  Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
  Layer.provide(ManagedPolicy.layerTest()),
  Layer.provide(
    Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
      namedProjectsRoot: "/projects",
      isScratchProject: () => Effect.succeed(false),
    }),
  ),
  Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
  Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
  Layer.provide(Layer.mergeAll(NodeCrypto.layer, Scheduler.layer, SqlitePersistence.layerMemory)),
);

// A Claude orchestrator delegates work and ends its turn; its run then waits for its checkpoint
// capture, which a busy server can hold in its effect outbox for minutes.
it.effect.each(["a user message", "an automation"] as const)(
  "delivers %s to a Claude orchestrator waiting on its delegated tasks at once",
  (sender) =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace(`waiting-run-delivery-${sender.replaceAll(" ", "-")}`);
      const sdkMessages = yield* Queue.unbounded<SDKMessage>();
      const offers: SDKUserMessage[] = [];
      const adapter = yield* ClaudeAdapterV2.makeClaudeAdapterV2({
        instanceId: modelSelection.instanceId,
        settings,
        environment: {},
        attachmentsDir: cwd,
        fileSystem: yield* FileSystem.FileSystem,
        path: yield* Path.Path,
        random: yield* SecureRandom.SecureRandom,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        queryRunner: {
          allocateSessionId: Effect.succeed(sessionId),
          open: () =>
            Effect.succeed({
              messages: Stream.fromQueue(sdkMessages),
              offer: (message) => Effect.sync(() => void offers.push(message)),
              setModel: () => Effect.void,
              applyFlagSettings: () => Effect.void,
              setPermissionMode: () => Effect.void,
              renameSession: () => Effect.void,
              backgroundTasks: () => Effect.succeed(false),
              mcpServerStatus: Effect.succeed([]),
              readMcpResource: () => Effect.die("unexpected MCP resource read"),
              callMcpTool: () => Effect.die("unexpected MCP tool call"),
              listPermissionRules: Effect.die("unexpected permission rules read"),
              stopTask: () => Effect.void,
              interrupt: Effect.die("delivery to a waiting run must never interrupt"),
              close: Effect.void,
            }),
          forkSession: () => Effect.die("unused"),
          subagentLaunchToolUseId: () => Effect.succeed(null),
          subagentModel: () => Effect.succeed(null),
          assertComplete: Effect.void,
        },
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make(
          `thread:waiting-run-delivery-${sender.replaceAll(" ", "-")}`,
        );
        const projectId = ProjectId.make("project:waiting-run-delivery");
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          Effect.gen(function* () {
            const afterSequence = yield* sink.latestSequence({ threadId });
            return yield* sink.stream({ threadId, afterSequence }).pipe(
              Stream.filter((stored) => predicate(stored.event)),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
          });
        const runOf = (messageId: MessageId) =>
          Effect.map(orchestrator.getThreadProjection(threadId), ({ runs }) =>
            runs.find((run) => run.userMessageId === messageId)!,
          );
        // Ends the provider turn the run is in; the run then waits for its capture.
        const endTurn = (run: OrchestrationV2Run, uuid: string) =>
          Effect.gen(function* () {
            const waiting = yield* watch(
              (event) =>
                event.type === "run.updated" &&
                event.payload.id === run.id &&
                event.payload.status === "waiting",
            );
            yield* Queue.offer(sdkMessages, result(uuid));
            yield* Fiber.join(waiting);
          });
        const turnRunning = (runId: OrchestrationV2Run["id"]) =>
          watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === runId &&
              event.payload.status === "running",
          );

        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId,
          title: "Waiting orchestrator",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const firstMessageId = MessageId.make("message:orchestrate");
        const running = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("orchestrate"),
          threadId,
          messageId: firstMessageId,
          text: "Delegate the work and wait for it.",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* Fiber.join(running);
        const parent = yield* runOf(firstMessageId);
        const now = yield* DateTime.now;
        const tasks: ReadonlyArray<OrchestrationV2Subagent> = Array.from(
          { length: taskCount },
          (_, index) => ({
            id: NodeId.make(`task:${index + 1}`),
            threadId,
            runId: parent.id,
            parentNodeId: parent.rootNodeId!,
            origin: "app_owned",
            createdBy: "agent",
            driver: ClaudeAdapterV2.CLAUDE_PROVIDER,
            providerInstanceId: modelSelection.instanceId,
            providerThreadId: null,
            childThreadId: null,
            nativeTaskRef: null,
            prompt: `Task ${index + 1}`,
            title: `Task ${index + 1}`,
            model: null,
            completionWake: "settled_only",
            status: "running",
            result: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
          }),
        );
        yield* sink.write({
          events: tasks.map((task) => ({
            id: EventId.make(`delegate:${task.id}`),
            type: "subagent.updated" as const,
            threadId,
            runId: parent.id,
            nodeId: task.id,
            occurredAt: now,
            payload: task,
          })),
        });
        yield* endTurn(parent, "orchestrator-turn");
        assert.equal((yield* runOf(firstMessageId)).status, "waiting");

        // Completes `taskIds` and hands their results to the parent as one queued wake.
        let generation = 0;
        const complete = (taskIds: ReadonlyArray<NodeId>) =>
          Effect.gen(function* () {
            generation += 1;
            const at = yield* DateTime.now;
            const messageId = MessageId.make(`message:completion-${generation}`);
            const current = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
              (run) => run.id === parent.id,
            )!;
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`cohort:${generation}`),
                  type: "run.updated",
                  threadId,
                  runId: parent.id,
                  occurredAt: at,
                  payload: {
                    ...current,
                    delegatedCompletion: {
                      disposition: "open",
                      nextGeneration: generation + 1,
                      delivery: { generation, messageId, taskIds },
                    },
                  },
                },
                ...tasks
                  .filter((task) => taskIds.includes(task.id))
                  .map((task) => ({
                    id: EventId.make(`completed:${task.id}`),
                    type: "subagent.updated" as const,
                    threadId,
                    runId: parent.id,
                    nodeId: task.id,
                    occurredAt: at,
                    payload: {
                      ...task,
                      status: "completed" as const,
                      result: `${task.title} done`,
                      completionDelivery: { state: "claimed" as const, observedByRunId: null },
                      completedAt: at,
                      updatedAt: at,
                    },
                  })),
              ],
            });
            const command = {
              type: "message.dispatch" as const,
              commandId: CommandId.make(`completion-${generation}`),
              threadId,
              messageId,
              text: "Tasks completed",
              attachments: [],
              dispatchMode: { type: "queue_after_active" as const },
              createdBy: "agent" as const,
              creationSource: "server" as const,
              delegatedCompletion: { parentRunId: parent.id, generation, taskIds: [...taskIds] },
            };
            yield* orchestrator.dispatch(command);
            yield* orchestrator.dispatch(command);
            return messageId;
          });

        // The first result arrives while the orchestrator waits: a hidden wake keeps waiting.
        const firstWake = yield* complete([tasks[0]!.id]);
        assert.equal((yield* runOf(firstMessageId)).status, "waiting");
        assert.equal((yield* runOf(firstWake)).status, "queued");

        const sentAt = yield* DateTime.now;
        const visibleText =
          sender === "a user message" ? "Change of plan: stop at the review." : "Status check";
        const visibleRunning = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id !== parent.id &&
            event.payload.status === "starting",
        );
        if (sender === "a user message") {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("user-send"),
            threadId,
            messageId: MessageId.make("message:user-send"),
            text: visibleText,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            deliveryIntent: "auto",
            createdBy: "user",
            creationSource: "web",
          });
        } else {
          yield* Effect.gen(function* () {
            const service = yield* ScheduledTaskService.ScheduledTaskService;
            const { task } = yield* service.upsert(
              yield* decodeScheduledTask({
                title: "Status check",
                prompt: visibleText,
                enabled: true,
                schedule: { type: "interval", everyMs: 60_000 },
                projectId,
                threadId,
                workspaceStrategy: { type: "root" },
                modelSelection,
                runtimeMode: "full-access",
                interactionMode: "default",
              }),
            );
            const ran = yield* service.runNow({ id: task.id });
            assert.equal(ran.task.lastRunStatus, "succeeded");
          }).pipe(Effect.provide(scheduledTasks));
        }
        yield* Fiber.join(visibleRunning);
        const ended = yield* runOf(firstMessageId);
        assert.equal(ended.status, "completed");
        assert.isNull(ended.checkpointId);
        // The results still running are untouched by the wait's end.
        const midway = yield* orchestrator.getThreadProjection(threadId);
        for (const task of tasks.slice(1)) {
          assert.equal(midway.subagents.find((row) => row.id === task.id)?.status, "running");
        }

        // The capture goes first; the visible message's turn takes the provider next.
        const visible = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "starting",
        )!;
        const visibleTurn = yield* turnRunning(visible.id);
        yield* worker.drain();
        yield* Fiber.join(visibleTurn);
        assert.deepEqual(offers.map(offerText).slice(1), [visibleText]);
        assert.notEqual(offers[1]?.priority, "now");
        assert.isAtMost(
          Duration.toMillis(DateTime.distance(sentAt, yield* DateTime.now)),
          Duration.toMillis(Duration.seconds(60)),
        );
        assert.isNotNull((yield* runOf(firstMessageId)).checkpointId);

        // Afterwards each result reaches the orchestrator exactly once.
        // The capture's run end starts the wake, whose start may run in the same drain.
        const startAfterCapture = (messageId: MessageId) =>
          Effect.gen(function* () {
            const run = yield* runOf(messageId);
            const starting = yield* watch(
              (event) =>
                event.type === "run.updated" &&
                event.payload.id === run.id &&
                event.payload.status === "starting",
            );
            const turn = yield* turnRunning(run.id);
            yield* worker.drain();
            yield* Fiber.join(starting);
            yield* worker.drain();
            yield* Fiber.join(turn);
          });
        yield* endTurn(visible, "visible-turn");
        yield* startAfterCapture(firstWake);
        yield* endTurn(yield* runOf(firstWake), "first-wake-turn");
        yield* worker.drain();
        const secondWake = yield* complete(tasks.slice(1).map((task) => task.id));
        const secondWakeTurn = yield* turnRunning((yield* runOf(secondWake)).id);
        yield* worker.drain();
        yield* Fiber.join(secondWakeTurn);
        yield* endTurn(yield* runOf(secondWake), "second-wake-turn");
        yield* worker.drain();
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        const after = yield* orchestrator.getThreadProjection(threadId);
        for (const task of tasks) {
          assert.equal(
            after.subagents.find((row) => row.id === task.id)?.completionDelivery?.state,
            "delivered",
            task.id,
          );
        }
        const deliveredTaskIds = after.messages.flatMap(
          (message) => message.delegatedCompletion?.taskIds ?? [],
        );
        assert.deepEqual(
          deliveredTaskIds.toSorted(),
          tasks.map((task) => task.id),
        );
        assert.lengthOf(offers, 4);
        assert.deepEqual(
          after.runs.map((run) => run.status),
          ["completed", "completed", "completed", "completed"],
        );
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "waiting-run-delivery" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(
          IdAllocator.layer,
          McpProviderSessions.layer,
          NodeServices.layer,
          NodeCrypto.layer,
        ),
      ),
    ),
);

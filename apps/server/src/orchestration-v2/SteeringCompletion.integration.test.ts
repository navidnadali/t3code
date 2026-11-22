import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Option from "effect/Option";
import * as TestClock from "effect/testing/TestClock";
import {
  ChatAttachmentId,
  CheckpointId,
  COMPOSER_CONTEXT_TERMINAL_TEXT_MAX_CHARS,
  ComposerContextId,
  CommandId,
  EnvironmentId,
  MessageId,
  EventId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  RunId,
  RuntimeRequestId,
  ScheduledTaskId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2Command,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import {
  expandAssistantCitationsForProvider,
  serializeAssistantCitation,
} from "@t3tools/shared/assistantCitations";
import { projectComposerContextForProvider } from "@t3tools/shared/composerContextReferences";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import { queuedRunsInDeliveryOrder } from "./QueuedRunOrder.ts";
import { QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX } from "./QueuedDeliveryPromotion.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import { userFacingDispatchErrorMessage } from "./UserFacingErrors.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import { makeCursorAdapterV2 } from "@t3tools/provider-cursor/testing";
import * as CursorAgentSdk from "@t3tools/provider-cursor/server/CursorAgentSdk";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";

// Agent, automation and tool messages reach the provider under T3 Code's sender note.
// These rows check delivery, so they compare the text beneath it.
const withoutSenderNote = (text: string) => text.replace(/^\[T3 Code: [^\n]*\]\n\n/, "");

const driver = ProviderDriverKind.make("codex");

const instanceId = ProviderInstanceId.make("codex");

const modelSelection = { instanceId, model: "test-model" };

it.effect.each(
  (
    [
      [false, "during delivery"],
      ["auto", "during delivery"],
      ["auto", "during missing session"],
      ["auto", "before dispatch"],
      ["auto", "after delivery"],
      [false, "before dispatch"],
      [true, "after delivery"],
      [true, "during compaction"],
      [true, "without native steering"],
      [true, "with interrupting native steering"],
      [true, "settled only"],
      [false, "during continuation"],
      ["question", "during delivery"],
    ] as const
  ).map(([kind, timing]) => ({
    mailbox: kind === true,
    automatic: kind === "auto",
    question: kind === "question",
    timing,
    label:
      kind === "question"
        ? "delegated task question"
        : kind === "auto"
          ? "automatic steering"
          : kind
            ? "mailbox notification"
            : "steering",
  })),
)("delivers $label when completion wins $timing", ({ mailbox, automatic, question, timing }) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace(`steering-completion-${timing.replaceAll(" ", "-")}`);
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
    const steerEntered = yield* Deferred.make<void>();
    const rejectSteer = yield* Deferred.make<void>();
    let steerCalls = 0;
    const capabilities = {
      ...CodexProviderCapabilitiesV2,
      turns: {
        ...CodexProviderCapabilitiesV2.turns,
        supportsActiveSteering: timing !== "without native steering",
        activeSteeringInterruptsTools: timing === "with interrupting native steering",
      },
    };
    const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(capabilities),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          return {
            instanceId,
            driver,
            providerSessionId: input.providerSessionId,
            providerSession: {
              id: input.providerSessionId,
              driver,
              providerInstanceId: instanceId,
              status: "ready",
              cwd,
              model: modelSelection.model,
              capabilities,
              createdAt: now,
              updatedAt: now,
              lastError: null,
            },
            events: Stream.fromQueue(events),
            ensureThread: ({ threadId }) =>
              Effect.succeed({
                id: ProviderThreadId.make(`provider-thread:${threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: input.providerSessionId,
                appThreadId: threadId,
                ownerNodeId: null,
                nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                createdAt: now,
                updatedAt: now,
              }),
            resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
            startTurn: (turn) =>
              Effect.gen(function* () {
                started.push(turn);
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: {
                      driver,
                      nativeId: `native:${turn.attemptId}`,
                      strength: "strong",
                    },
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                });
              }),
            steerTurn: (turn) =>
              Effect.gen(function* () {
                if (turn.beforeSubmit !== undefined) yield* turn.beforeSubmit;
                steerCalls += 1;
                if (timing === "after delivery" || timing === "during continuation") return;
                yield* Deferred.succeed(steerEntered, undefined);
                yield* Deferred.await(rejectSteer);
                return yield* new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver,
                  providerThreadId: turn.providerThread.id,
                  providerTurnId: turn.providerTurnId,
                  deliveryNotSent: true,
                  cause:
                    timing === "during missing session"
                      ? "SessionNotFoundError: native prompt returned 404"
                      : "turn already completed",
                });
              }),
            interruptTurn: () => Effect.void,
            respondToRuntimeRequest: () => Effect.void,
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = ThreadId.make("thread:steering-completion");
      const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter(predicate),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create"),
        threadId,
        projectId: ProjectId.make("project:steering-completion"),
        title: "Steering race",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("first"),
        threadId,
        messageId: MessageId.make("message:first"),
        text: "first",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      const running = yield* watch(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      );
      yield* worker.drain();
      yield* Fiber.join(running);
      const first = started[0]!;
      const messageId = MessageId.make("message:steering");
      const taskId = NodeId.make("task:mailbox");
      if (mailbox) {
        const sink = yield* EventSink.EventSinkV2;
        const current = yield* orchestrator.getThreadProjection(threadId);
        const parentRun = current.runs.find((run) => run.id === first.runId)!;
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("mailbox:cohort"),
              type: "run.updated",
              threadId,
              runId: first.runId,
              occurredAt: now,
              payload: {
                ...parentRun,
                delegatedCompletion: {
                  disposition: "open",
                  nextGeneration: 2,
                  delivery: { generation: 1, messageId, taskIds: [taskId] },
                },
              },
            },
            {
              id: EventId.make("mailbox:task"),
              type: "subagent.updated",
              threadId,
              runId: first.runId,
              nodeId: taskId,
              occurredAt: now,
              payload: {
                id: taskId,
                threadId,
                runId: first.runId,
                parentNodeId: first.rootNodeId,
                origin: "app_owned",
                createdBy: "agent",
                driver,
                providerInstanceId: instanceId,
                providerThreadId: null,
                childThreadId: null,
                nativeTaskRef: null,
                prompt: "Do background work",
                title: "Background test",
                model: null,
                completionWake: timing === "settled only" ? "settled_only" : "always",
                completionDelivery: { state: "claimed", observedByRunId: null },
                status: "completed",
                result: "done",
                startedAt: now,
                completedAt: now,
                updatedAt: now,
              },
            },
          ],
        });
      }
      if (question) {
        const { thread } = yield* orchestrator.getThreadProjection(threadId);
        yield* (yield* EventSink.EventSinkV2).write({
          events: [
            {
              id: EventId.make("event:composer-selection"),
              type: "thread.model-selection-updated",
              threadId,
              providerInstanceId: instanceId,
              occurredAt: yield* DateTime.now,
              payload: { ...thread, modelSelection: composerSelection },
            },
          ],
        });
      }
      const dispatchSteer = orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer"),
        threadId,
        messageId,
        text: "fix the popover",
        attachments: [
          {
            type: "image",
            id: "steering-screenshot",
            name: "image.png",
            mimeType: "image/png",
            sizeBytes: 10,
          },
        ],
        dispatchMode:
          mailbox || question
            ? { type: "queue_after_active" }
            : automatic
              ? { type: "start_immediately" }
              : { type: "steer_active", targetRunId: first.runId },
        ...(automatic ? { deliveryIntent: "auto" as const } : {}),
        createdBy: mailbox || question ? "agent" : "user",
        creationSource: mailbox || question ? "server" : "web",
        ...(question
          ? {
              notification: {
                source: { kind: "delegated_task", taskIds: [taskId] },
                outcome: "updated",
                summary: "Delegated task has a question",
              },
            }
          : {}),
        ...(mailbox
          ? {
              delegatedCompletion: {
                parentRunId: first.runId,
                generation: 1,
                taskIds: [taskId],
              },
            }
          : {}),
      });
      if (timing !== "before dispatch") yield* dispatchSteer;
      if (timing === "during compaction") {
        const pending = yield* worker.runOnce().pipe(Effect.forkScoped);
        yield* Deferred.await(steerEntered);
        yield* Deferred.succeed(rejectSteer, undefined);
        yield* Fiber.join(pending);
        const after = yield* orchestrator.getThreadProjection(threadId);
        const queued = after.runs.filter(
          (run) => run.status === "queued" && run.userMessageId === messageId,
        );
        assert.lengthOf(queued, 1);
        assert.equal(after.runs.find((run) => run.id === first.runId)?.status, "running");
        assert.lengthOf(
          after.messages.filter((message) => message.id === messageId),
          1,
        );
        assert.equal(
          after.messages.find((message) => message.id === messageId)?.delegatedCompletion
            ?.generation,
          1,
        );
        yield* worker.drain();
        assert.equal(steerCalls, 1);
        return;
      }
      if (timing === "during continuation") {
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const turn = projection.providerTurns[0]!;
        const failed = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "failed",
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "failed", completedAt: yield* DateTime.now },
        });
        yield* Fiber.join(failed);
        yield* worker.drain();
        assert.equal(steerCalls, 1);
        assert.lengthOf(started, 1);
        const continued = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(continued.runs[0]?.status, "running");
        assert.equal(
          continued.messages.find((message) => message.id === messageId)?.runId,
          first.runId,
        );
        return;
      }
      if (timing === "after delivery") yield* worker.drain();
      const delivery =
        timing === "during delivery" || timing === "during missing session"
          ? yield* worker.runOnce().pipe(Effect.forkScoped)
          : null;
      if (delivery !== null) yield* Deferred.await(steerEntered);
      const completed = yield* watch(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === first.runId &&
          event.payload.status === (timing === "during missing session" ? "failed" : "waiting"),
      );
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const turn = projection.providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: {
          ...turn,
          status: timing === "during missing session" ? "failed" : "completed",
          completedAt: yield* DateTime.now,
        },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        ...(timing === "during missing session"
          ? {
              status: "failed" as const,
              failure: makeProviderFailure({
                message: "Session not found",
                class: "provider_error",
              }),
              failureItemOrdinal: 100,
              threadDisposition: "broken" as const,
            }
          : {
              status: "completed" as const,
              failure: null,
              threadDisposition: "reusable" as const,
            }),
      });
      yield* Fiber.join(completed);
      if (delivery !== null) {
        yield* Deferred.succeed(rejectSteer, undefined);
        yield* Fiber.join(delivery);
      }
      if (delivery !== null && !question && timing !== "during missing session") {
        const requeued = yield* orchestrator.getThreadProjection(threadId);
        // The steer's follow-up takes its place and starts the queue once it commits.
        assert.isTrue(
          requeued.runs.some(
            (run) =>
              run.userMessageId === messageId &&
              run.steerFollowUpOfRunId === first.runId &&
              (run.status === "queued" || run.status === "starting"),
          ),
        );
        assert.isFalse(
          requeued.visibleTurnItems.some(
            ({ item }) =>
              item.type === "user_message" &&
              item.messageId === messageId &&
              item.runId === first.runId,
          ),
          "a steer that fell back to the queue must not appear delivered to the run it missed",
        );
        const item = requeued.turnItems.find(
          (item) => item.type === "user_message" && item.messageId === messageId,
        );
        assert.notEqual(item?.runId, first.runId);
        // Until its follow-up starts, the message reads as not yet sent.
        if (requeued.runs.find((run) => run.userMessageId === messageId)?.status === "queued") {
          assert.equal(item?.status, "pending");
          assert.isNull(item?.startedAt);
          assert.isNull(item?.completedAt);
        }
      }
      if (timing === "before dispatch") yield* dispatchSteer;
      yield* worker.drain();
      yield* orchestrator.resumeQueuedRuns;
      yield* worker.drain();
      if (timing === "after delivery") {
        assert.equal(steerCalls, 1);
        assert.equal(started.length, 1);
        if (mailbox) {
          const delivered = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(delivered.subagents[0]?.completionDelivery?.state, "delivered");
          assert.equal(delivered.subagents[0]?.completionDelivery?.observedByRunId, null);
          assert.equal(delivered.runs[0]?.delegatedCompletion?.delivery, null);
          assert.equal(
            delivered.turnItems.filter((item) => item.type === "notification").length,
            1,
          );
          yield* orchestrator.dispatch({
            type: "notification.delivery.accept",
            commandId: CommandId.make("duplicate-acceptance"),
            threadId,
            messageId,
          });
          yield* worker.drain();
          assert.equal(steerCalls, 1);
          yield* orchestrator.dispatch({
            type: "delegated_task.completion-delivery.acknowledge",
            commandId: CommandId.make("read-result"),
            parentThreadId: threadId,
            taskId,
            observedByRunId: first.runId,
          });
          const acknowledged = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(acknowledged.subagents[0]?.completionDelivery?.state, "acknowledged");
        }
        return;
      }
      assert.equal(started.length, 2);
      assert.equal(started[1]?.message.messageId, messageId);
      if (mailbox) assert.include(started[1]?.message.text ?? "", String(taskId));
      else assert.equal(started[1]?.message.text, "fix the popover");
      assert.deepEqual(started[1]?.message.attachments, [
        {
          type: "image",
          id: "steering-screenshot",
          name: "image.png",
          mimeType: "image/png",
          sizeBytes: 10,
        },
      ]);
      assert.equal(
        steerCalls,
        timing === "during delivery" || timing === "during missing session" ? 1 : 0,
      );
      const final = yield* orchestrator.getThreadProjection(threadId);
      if (question) {
        assert.deepEqual(final.thread.modelSelection, composerSelection);
        assert.deepEqual(started[1]?.modelSelection, composerSelection);
      }
      if (!mailbox && !question) {
        assert.equal(
          final.turnItems.find(
            (item) => item.type === "user_message" && item.messageId === messageId,
          )?.status,
          "completed",
        );
      }
      assert.equal(final.messages.filter((message) => message.id === messageId).length, 1);
      assert.equal(
        final.messages.find((message) => message.id === messageId)?.runId,
        started[1]?.runId,
      );
      assert.equal(
        final.turnItems.filter((item) =>
          mailbox || question
            ? item.type === "notification"
            : item.type === "user_message" && item.messageId === messageId,
        ).length,
        1,
      );
      if (!mailbox && !question) {
        assert.equal(
          final.visibleTurnItems.filter(
            ({ item }) => item.type === "user_message" && item.messageId === messageId,
          ).length,
          1,
        );
      }
      yield* worker.drain();
      assert.equal(started.length, 2);
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerWithRegistry(
          { name: `steering-completion-${timing}` },
          ProviderAdapterRegistry.layerSingle(adapter),
          { runEffectWorker: false },
        ),
      ),
    );
  }),
);

const runSelection = {
  instanceId,
  model: "test-model",
  options: [{ id: "effort", value: "xhigh" }],
};

const composerSelection = {
  ...runSelection,
  options: [...runSelection.options, { id: "fastMode", value: false }],
};

const nextTurnSelectionHarness = Effect.fn("nextTurnSelectionHarness")(function* (
  name: string,
  canRestart = false,
  canSteer = true,
  interruptTurn: Effect.Effect<void> = Effect.void,
  steeringAcceptsAttachments = true,
  nativeGoals?: boolean,
  onSteer: (
    turn: ProviderAdapter.ProviderAdapterV2SteerInput,
  ) => Effect.Effect<void, ProviderAdapter.ProviderAdapterV2Error> = () => Effect.void,
  extraAdapters: ReadonlyArray<ProviderAdapter.ProviderAdapterV2["Service"]> = [],
  databaseLayer?: typeof SqlitePersistence.layerMemory,
  driverKind?: ProviderDriverKind,
  steerPreparation: Effect.Effect<void, ProviderAdapter.ProviderAdapterSteerRunError> = Effect.void,
  steerAcknowledgement: Effect.Effect<
    void,
    ProviderAdapter.ProviderAdapterSteerRunError
  > = Effect.void,
  startPreparation: Effect.Effect<void> = Effect.void,
  announceBeforePreparation = false,
) {
  const driver =
    driverKind ?? ProviderDriverKind.make(nativeGoals === undefined ? "codex" : "opencode");
  const cwd = yield* checkpointWorkspace(name);
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
  const announced: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
  const steered: string[] = [];
  const steeredSelections: Array<ModelSelection | undefined> = [];
  const interrupted: ProviderTurnId[] = [];
  const capabilities = {
    ...CodexProviderCapabilitiesV2,
    turns: {
      ...CodexProviderCapabilitiesV2.turns,
      supportsActiveSteering: canSteer,
      supportsSteeringByInterruptRestart: canRestart,
      activeSteeringAcceptsAttachments: steeringAcceptsAttachments,
      ...(nativeGoals === undefined ? {} : { supportsNativeGoals: nativeGoals }),
    },
  };
  const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(capabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const turnUpdated = (
          turn: ProviderAdapter.ProviderAdapterV2TurnInput,
          status: "running" | "interrupted",
        ) =>
          Queue.offer(events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: {
              id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
              providerThreadId: turn.providerThread.id,
              nodeId: turn.rootNodeId,
              runAttemptId: turn.attemptId,
              nativeTurnRef: {
                driver,
                nativeId: `native:${turn.attemptId}`,
                strength: "strong",
              },
              ordinal: turn.providerTurnOrdinal,
              status,
              startedAt: now,
              completedAt: status === "running" ? null : now,
            },
          });
        const startTurn = (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
          Effect.gen(function* () {
            if (started.length > 0) {
              if (announceBeforePreparation) {
                announced.push(turn);
                yield* turnUpdated(turn, "running");
              }
              yield* startPreparation;
            }
            if (turn.submit === undefined) started.push(turn);
            else
              yield* turn.submit(() => {
                started.push(turn);
              });
            yield* turnUpdated(turn, "running");
          });
        return {
          instanceId,
          driver,
          providerSessionId: input.providerSessionId,
          providerSession: {
            id: input.providerSessionId,
            driver,
            providerInstanceId: instanceId,
            status: "ready",
            cwd,
            model: runSelection.model,
            capabilities,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(events),
          ensureThread: ({ threadId }) =>
            Effect.succeed({
              id: ProviderThreadId.make(`provider-thread:${threadId}`),
              driver,
              providerInstanceId: instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn,
          compactThread: startTurn,
          steerTurn: (turn) =>
            steerPreparation.pipe(
              Effect.andThen(turn.beforeSubmit ?? Effect.void),
              Effect.andThen(steerAcknowledgement),
              Effect.andThen(
                Effect.sync(() => {
                  steered.push(turn.message.text);
                  steeredSelections.push(turn.modelSelection);
                }).pipe(Effect.andThen(onSteer(turn))),
              ),
            ),
          interruptTurn: ({ providerTurnId }) =>
            Effect.sync(() => {
              interrupted.push(providerTurnId);
            }).pipe(
              Effect.andThen(interruptTurn),
              Effect.andThen(
                Effect.forEach(
                  // An announced turn ends with its interrupt, like one that started.
                  [...started, ...announced.filter((turn) => !started.includes(turn))].filter(
                    (turn) =>
                      ProviderTurnId.make(`provider-turn:${turn.attemptId}`) === providerTurnId,
                  ),
                  (turn) =>
                    turnUpdated(turn, "interrupted").pipe(
                      Effect.andThen(
                        Queue.offer(events, {
                          type: "turn.terminal",
                          driver,
                          providerThreadId: turn.providerThread.id,
                          providerTurnId,
                          runOrdinal: turn.runOrdinal,
                          status: "interrupted",
                          failure: null,
                          threadDisposition: "reusable",
                        }),
                      ),
                    ),
                  { discard: true },
                ),
              ),
            ),
          respondToRuntimeRequest: () => Effect.void,
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: ({ providerThread }) =>
            Effect.succeed({
              providerThread,
              providerTurns: [],
              messages: [],
              runtimeRequests: [],
            }),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };
  const layer = ProviderReplayHarness.layerWithRegistryInternals(
    { name },
    ProviderAdapterRegistry.layerFromAdapters([adapter, ...extraAdapters]),
    { runEffectWorker: false, ...(databaseLayer === undefined ? {} : { databaseLayer }) },
  );
  const startFirstTurn = Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const threadId = ThreadId.make(`thread:${name}`);
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make("create"),
      threadId,
      projectId: ProjectId.make(`project:${name}`),
      title: "Steer with changed options",
      modelSelection: runSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: cwd,
      createdBy: "user",
      creationSource: "web",
    });
    const running = yield* orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("first"),
      threadId,
      messageId: MessageId.make("message:first"),
      text: "first",
      attachments: [],
      modelSelection: runSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* worker.drain();
    yield* Fiber.join(running);
    // Ingestion checks turn ownership under this lock after committing the running turn.
    yield* (yield* ThreadCommandExecutor.ThreadCommandExecutor).withLock(threadId, Effect.void);
    return threadId;
  });
  return { cwd, events, started, steered, steeredSelections, interrupted, layer, startFirstTurn };
});

it.effect.each([
  { status: "running", restart: false, released: false },
  { status: "waiting", restart: false, released: false },
  { status: "running", restart: true, released: false },
  { status: "waiting", restart: true, released: false },
  { status: "waiting", restart: false, released: true },
] as const)(
  "delivers an automatic MCP message to a live $status turn (restart: $restart, released: $released) despite a stale preflight",
  ({ status, restart, released }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const database = SqlitePersistence.layerMemory;
        const { started, steered, interrupted, layer, startFirstTurn } =
          yield* nextTurnSelectionHarness(
            `mcp-auto-${status}`,
            restart,
            !restart,
            Effect.void,
            true,
            undefined,
            undefined,
            undefined,
            database,
          );
        let staleRead = true;
        const management = ThreadManagementService.layer.pipe(
          Layer.provide(
            Layer.effect(
              Orchestrator.OrchestratorV2,
              Effect.map(Orchestrator.OrchestratorV2, (orchestrator) => ({
                ...orchestrator,
                getThreadRecords: (threadId, selection) =>
                  orchestrator.getThreadRecords(threadId, selection).pipe(
                    Effect.map((projection) => {
                      if (!staleRead) return projection;
                      staleRead = false;
                      // The read happened while the turn was starting; dispatch sees the live turn.
                      return {
                        ...projection,
                        runs: (
                          projection as typeof projection &
                            Pick<OrchestrationV2ThreadProjection, "runs">
                        ).runs.map((run) => ({ ...run, status: "starting" as const })),
                        providerTurns: [],
                      };
                    }),
                  ),
              })),
            ).pipe(Layer.provide(layer)),
          ),
          Layer.provide(layer),
          Layer.provide(ServerSettings.layerTest()),
          Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(database))),
        );
        yield* Effect.gen(function* () {
          const threadId = yield* startFirstTurn;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threads = yield* ThreadManagementService.ThreadManagementService;
          if (status === "waiting") {
            const current = yield* orchestrator.getThreadProjection(threadId);
            const run = current.runs[0]!;
            yield* (yield* EventSink.EventSinkV2).write({
              events: [
                {
                  id: EventId.make("waiting-with-live-turn"),
                  type: "run.updated",
                  threadId,
                  runId: run.id,
                  occurredAt: yield* DateTime.now,
                  payload: { ...run, status },
                },
              ],
            });
          }
          if (released) {
            const current = yield* orchestrator.getThreadProjection(threadId);
            const sessions = yield* ProviderSessionManagerV2;
            const providerThread = current.providerThreads[0]!;
            yield* sessions.release({
              providerSessionId: providerThread.providerSessionId!,
              reason: "manual_shutdown",
            });
            assert.isFalse(yield* sessions.isLive(providerThread.providerSessionId!));
            // The runtime is gone while persisted lifecycle events still advertise a live turn.
            yield* (yield* EventSink.EventSinkV2).write({
              events: [
                {
                  id: EventId.make("stale-provider-thread"),
                  type: "provider-thread.updated",
                  threadId,
                  occurredAt: yield* DateTime.now,
                  payload: providerThread,
                },
                {
                  id: EventId.make("stale-provider-turn"),
                  type: "provider-turn.updated",
                  threadId,
                  occurredAt: yield* DateTime.now,
                  payload: current.providerTurns[0]!,
                },
                {
                  id: EventId.make("stale-waiting-run"),
                  type: "run.updated",
                  threadId,
                  occurredAt: yield* DateTime.now,
                  payload: current.runs[0]!,
                },
              ],
            });
          }
          const input = {
            projectId: ProjectId.make(`project:mcp-auto-${status}`),
            threadId,
            commandId: CommandId.make("mcp-auto-send"),
            messageId: MessageId.make("mcp-auto-message"),
            text: "Coordinate the deployment now",
            attachments: [],
            mode: "auto" as const,
            createdBy: "agent" as const,
            creationSource: "mcp" as const,
          };
          const sent = yield* threads.sendToThread(input);
          assert.equal(sent.delivery, released ? "queued" : "steered");
          if (!released) yield* worker.drain();
          yield* threads.sendToThread(input);
          if (!released) yield* worker.drain();
          if (released) {
            assert.deepEqual(steered, []);
            assert.lengthOf(started, 1);
          } else if (restart) {
            assert.deepEqual(steered, []);
            assert.lengthOf(interrupted, 1);
            assert.lengthOf(started, 2);
            assert.include(started[1]!.message.text, input.text);
          } else {
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            const delivered = (yield* outbox.listByCommandId(input.commandId)).find(
              (item) => item.request.type === "provider-turn.steer",
            );
            assert.isTrue(
              delivered?.request.type === "provider-turn.steer" &&
                delivered.request.deliveryPhase === "accepted",
            );
            assert.deepEqual(steered.map(withoutSenderNote), [input.text]);
            assert.lengthOf(started, 1);
          }
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.lengthOf(
            after.messages.filter((message) => message.id === input.messageId),
            1,
          );
          assert.equal(
            after.runs.some((run) => run.status === "queued"),
            released,
          );
        }).pipe(Effect.provide(Layer.mergeAll(layer, management)));
      }),
    ),
);

it.effect.each([false, true])(
  "does not resend an accepted auto steer after bookkeeping failure (bookkeeping: %s)",
  (bookkeeping) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { layer, startFirstTurn, steered } =
          yield* nextTurnSelectionHarness("accepted-steer-retry");
        yield* Effect.gen(function* () {
          const threadId = yield* startFirstTurn;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
          const commandId = CommandId.make("accepted-retry");
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId,
            threadId,
            messageId: MessageId.make("message:accepted-retry"),
            text: "One delivery",
            attachments: [],
            createdBy: "agent",
            creationSource: "mcp",
            deliveryIntent: "auto",
            dispatchMode: { type: "start_immediately" },
          });
          let claimed = Option.getOrThrow(yield* outbox.claimNext({ workerId: "accepted-retry" }));
          assert.equal(claimed.request.type, "provider-turn.steer");
          if (claimed.request.type !== "provider-turn.steer") throw new Error("Expected steer");
          if (bookkeeping) {
            // Exercise the legacy/delegation bookkeeping path with a real durable outbox row.
            yield* outbox.succeed({ effectId: claimed.id, workerId: "accepted-retry" });
            yield* outbox.enqueue([
              {
                id: `${claimed.id}:bookkeeping`,
                commandId,
                threadId,
                request: { ...claimed.request, acceptDelegatedCompletion: true },
              },
            ]);
            claimed = Option.getOrThrow(yield* outbox.claimNext({ workerId: "accepted-retry" }));
          }
          const threads = yield* ThreadManagementService.ThreadManagementService;
          using readFailure = vi.spyOn(threads, "getThreadRecords").mockImplementationOnce(() =>
            Effect.fail(
              new Orchestrator.OrchestratorProjectionError({
                threadId,
                cause: "post-accept read failure",
              }),
            ),
          );
          if (bookkeeping) {
            yield* executor.execute(claimed).pipe(Effect.flip);
            assert.equal(readFailure.mock.calls.length, 1);
          } else {
            yield* executor.execute(claimed);
            assert.equal(readFailure.mock.calls.length, 0);
          }
          readFailure.mockRestore();
          assert.deepEqual(steered.map(withoutSenderNote), ["One delivery"]);
          assert.isTrue(
            yield* outbox.retry({
              effectId: claimed.id,
              workerId: "accepted-retry",
              error: "retry bookkeeping",
              delayMs: 0,
            }),
          );
          claimed = Option.getOrThrow(yield* outbox.claimNext({ workerId: "accepted-retry" }));
          assert.isTrue(
            claimed.request.type === "provider-turn.steer" &&
              claimed.request.deliveryPhase === "accepted",
          );
          yield* executor.execute(claimed);
          assert.deepEqual(steered.map(withoutSenderNote), ["One delivery"]);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect(
  "preserves an auto steer whose target attempt is replaced by a same-session restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { layer, startFirstTurn, steered, started } = yield* nextTurnSelectionHarness(
          "superseded-steer",
          true,
        );
        yield* Effect.gen(function* () {
          const threadId = yield* startFirstTurn;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
          const messageId = MessageId.make("message:superseded-steer");
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("auto-before-restart"),
            threadId,
            messageId,
            text: "Preserve this accepted message",
            attachments: [],
            createdBy: "agent",
            creationSource: "mcp",
            deliveryIntent: "auto",
            dispatchMode: { type: "start_immediately" },
          });
          const claimed = Option.getOrThrow(yield* outbox.claimNext({ workerId: "superseded" }));
          assert.equal(claimed.request.type, "provider-turn.steer");
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("replace-target-attempt"),
            threadId,
            messageId: MessageId.make("message:restart"),
            text: "Restart this turn",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "restart_active", targetRunId: started[0]!.runId },
          });
          yield* executor.execute(claimed);
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(steered, []);
          assert.lengthOf(
            after.messages.filter((m) => m.id === messageId),
            1,
          );
          const runs = after.runs.filter((r) => r.userMessageId === messageId);
          assert.lengthOf(runs, 1);
          assert.equal(runs[0]?.status, "queued");
          assert.isFalse(runs[0]?.queueHeld === true);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect.each([
  {
    mode: "auto",
    preparing: true,
    submitted: false,
    running: false,
    resumedEarly: false,
    stopAgain: false,
  },
  {
    mode: "auto",
    preparing: false,
    submitted: true,
    running: false,
    resumedEarly: false,
    stopAgain: false,
  },
  {
    mode: "restart",
    preparing: false,
    submitted: false,
    running: false,
    resumedEarly: false,
    stopAgain: false,
  },
  {
    mode: "restart",
    preparing: false,
    submitted: false,
    running: "projected",
    resumedEarly: false,
    stopAgain: false,
  },
  {
    mode: "restart",
    preparing: false,
    submitted: false,
    running: "native",
    resumedEarly: false,
    stopAgain: false,
  },
  {
    mode: "restart",
    preparing: false,
    submitted: false,
    running: "announced",
    resumedEarly: false,
    stopAgain: false,
  },
  {
    mode: "restart",
    preparing: false,
    submitted: false,
    running: "claiming",
    resumedEarly: false,
    stopAgain: false,
  },
  {
    mode: "promoted",
    preparing: false,
    submitted: false,
    running: false,
    resumedEarly: false,
    stopAgain: false,
  },
  {
    mode: "explicit",
    preparing: false,
    submitted: false,
    running: false,
    resumedEarly: false,
    stopAgain: false,
  },
  {
    mode: "auto",
    preparing: false,
    submitted: true,
    running: false,
    resumedEarly: true,
    stopAgain: false,
  },
  {
    mode: "auto",
    preparing: false,
    submitted: true,
    running: false,
    resumedEarly: true,
    stopAgain: true,
  },
  {
    mode: "auto",
    preparing: false,
    submitted: true,
    running: false,
    resumedEarly: "client",
    stopAgain: false,
  },
] as const)(
  "preserves $mode steering through Stop (preparing: $preparing, submitted: $submitted, running: $running, resumedEarly: $resumedEarly, stopAgain: $stopAgain)",
  ({ mode, preparing, submitted, running, resumedEarly, stopAgain }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const preparation = preparing
          ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Effect.void;
        const { layer, startFirstTurn, started, steered, interrupted } =
          yield* nextTurnSelectionHarness(
            "stop-before-submit",
            mode === "restart",
            mode !== "restart",
            Effect.void,
            true,
            undefined,
            undefined,
            undefined,
            undefined,
            mode === "restart" ? ProviderDriverKind.make("cursor") : driver,
            preparation,
            submitted
              ? Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(
                    Effect.fail(
                      new ProviderAdapter.ProviderAdapterSteerRunError({
                        driver,
                        providerThreadId: ProviderThreadId.make(
                          "provider-thread:stop-before-submit",
                        ),
                        providerTurnId: ProviderTurnId.make("provider-turn:stop-before-submit"),
                        deliveryNotSent: true,
                        cause: "Pi refused the submitted prompt during compaction",
                      }),
                    ),
                  ),
                )
              : Effect.void,
            running === "native" || running === "announced"
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
            running === "announced",
          );
        yield* Effect.gen(function* () {
          const threadId = yield* startFirstTurn;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sink = yield* EventSink.EventSinkV2;
          const write = sink.writeIfRunCurrent;
          const waitForStop = Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
          );
          using _startGate = running
            ? vi
                .spyOn(sink, "writeIfRunCurrent")
                .mockImplementation((input) =>
                  running === "claiming" &&
                  input.events.some(
                    (event) =>
                      event.type === "run-attempt.updated" &&
                      event.payload.providerStartState === "invoked",
                  )
                    ? waitForStop.pipe(Effect.andThen(write(input)))
                    : write(input).pipe(
                        Effect.tap((result) =>
                          running === "projected" &&
                          result.committed &&
                          input.expectedStatus === "starting"
                            ? waitForStop
                            : Effect.void,
                        ),
                      ),
                )
            : undefined;
          const messageId = MessageId.make("message:stop-before-submit");
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("stop-before-submit"),
            threadId,
            messageId,
            text: "Do this once after resume",
            attachments: [],
            createdBy: "user",
            creationSource: "mcp",
            ...(mode === "auto" || mode === "restart" ? { deliveryIntent: "auto" as const } : {}),
            dispatchMode:
              mode === "promoted"
                ? { type: "queue_after_active" }
                : mode === "auto" || mode === "restart"
                  ? { type: "start_immediately" }
                  : { type: "steer_active", targetRunId: started[0]!.runId },
          });
          if (mode === "promoted") {
            const p = yield* orchestrator.getThreadProjection(threadId);
            const queued = p.runs.find(
              (run) => run.userMessageId === messageId && run.status === "queued",
            )!;
            yield* orchestrator.dispatch({
              type: "queued-message.promote-to-steer",
              commandId: CommandId.make("promote-before-stop"),
              threadId,
              queuedRunId: queued.id,
              targetRunId: started[0]!.runId,
            });
          }
          const announced =
            running === "announced"
              ? yield* orchestrator.streamDomainEvents.pipe(
                  Stream.filter(
                    (event) =>
                      event.type === "provider-turn.updated" &&
                      event.payload.runAttemptId !== started[0]!.attemptId,
                  ),
                  Stream.take(1),
                  Stream.runDrain,
                  Effect.forkScoped,
                )
              : undefined;
          const pending =
            preparing || submitted || running
              ? yield* worker.runOnce().pipe(Effect.forkScoped)
              : null;
          if (preparing || submitted || running) yield* Deferred.await(entered);
          if (announced !== undefined) yield* Fiber.join(announced);
          if (running) {
            const before = yield* orchestrator.getThreadProjection(threadId);
            const replacement = before.runs.find((run) => run.userMessageId === messageId)!;
            const attempt = before.attempts.find(
              (candidate) => candidate.id === replacement.activeAttemptId,
            )!;
            assert.equal(replacement.status, "running");
            assert.equal(attempt.status, "running");
            assert.isNull(attempt.providerTurnId);
            assert.lengthOf(started, 1);
          }
          const stop = orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make("stop-before-submit-commit"),
            threadId,
          });
          if (running === "claiming") {
            const stopping = yield* Deferred.make<void>();
            const stopFiber = yield* Deferred.succeed(stopping, undefined).pipe(
              Effect.andThen(stop),
              Effect.forkScoped,
            );
            yield* Deferred.await(stopping);
            assert.isUndefined(stopFiber.pollUnsafe());
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(stopFiber);
          } else yield* stop;
          if (submitted) {
            // Stop must reach the provider while the steer still awaits its native reply.
            yield* worker.runOnce();
            assert.lengthOf(interrupted, 1);
            assert.isUndefined(pending!.pollUnsafe());
          }
          if (resumedEarly) {
            yield* orchestrator.dispatch(
              resumedEarly === "client"
                ? {
                    type: "message.dispatch",
                    commandId: CommandId.make("resume-before-refusal"),
                    threadId,
                    messageId: MessageId.make("client-release"),
                    text: "Continue",
                    attachments: [],
                    createdBy: "user",
                    creationSource: "web",
                    deliveryIntent: "auto",
                    dispatchMode: { type: "start_immediately" },
                  }
                : {
                    type: "queue.resume",
                    commandId: CommandId.make("resume-before-refusal"),
                    threadId,
                  },
            );
            if (stopAgain)
              yield* orchestrator.dispatch({
                type: "thread.stop",
                commandId: CommandId.make("stop-after-early-resume"),
                threadId,
              });
          }
          if (pending !== null) {
            yield* Deferred.succeed(release, undefined);
            yield* Fiber.join(pending);
          }
          yield* worker.drain();
          const paused = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(steered, []);
          const held = paused.runs.filter(
            (run) => run.userMessageId === messageId && run.status === "queued",
          );
          const resumed = resumedEarly && !stopAgain;
          assert.lengthOf(
            held,
            running === "claiming" || (resumed && resumedEarly !== "client") ? 0 : 1,
          );
          if (running !== "claiming" && !resumed) assert.isTrue(held[0]?.queueHeld);
          assert.lengthOf(
            paused.messages.filter((message) => message.id === messageId),
            1,
          );
          if (resumedEarly === "client") {
            // The new client turn owns the provider; the original follows it without another Resume.
            assert.isFalse(held[0]?.queueHeld ?? false);
            assert.equal(started.at(-1)?.message.messageId, MessageId.make("client-release"));
            return;
          }
          yield* orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make("resume-after-steer-stop"),
            threadId,
          });
          yield* worker.drain();
          assert.lengthOf(
            started.filter((turn) => turn.message.messageId === messageId),
            1,
          );
          assert.deepEqual(steered, []);
        }).pipe(
          Effect.provide(
            EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(Layer.provideMerge(layer)),
          ),
        );
      }),
    ),
);

it.effect("keeps a linked environment's sender on a restart that Stop holds", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "linked-restart-stop",
        true,
        false,
        Effect.void,
        true,
        undefined,
        undefined,
        undefined,
        undefined,
        ProviderDriverKind.make("cursor"),
      );
      yield* Effect.gen(function* () {
        const threadId = yield* startFirstTurn;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const messageId = MessageId.make("message:linked-restart-stop");
        const linkedSender = {
          environmentId: EnvironmentId.make("environment:linked-restart-stop"),
          threadId: ThreadId.make("thread:linked-restart-sender"),
          title: "Linked sending agent",
        };
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("linked-restart-stop"),
          threadId,
          messageId,
          text: "Restart with this",
          attachments: [],
          createdBy: "agent",
          creationSource: "mcp",
          linkedSender,
          deliveryIntent: "auto",
          dispatchMode: { type: "start_immediately" },
        });
        const restarting = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          restarting.attempts.find(
            (attempt) =>
              attempt.id ===
              restarting.runs.find((run) => run.userMessageId === messageId)?.activeAttemptId,
          )?.reason,
          "steering_restart",
        );
        assert.deepStrictEqual(
          restarting.messages.find((message) => message.id === messageId)?.linkedSender,
          linkedSender,
        );
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("linked-restart-stop-commit"),
          threadId,
        });
        yield* worker.drain();
        const paused = yield* orchestrator.getThreadProjection(threadId);
        const held = paused.runs.filter(
          (run) => run.userMessageId === messageId && run.status === "queued",
        );
        assert.lengthOf(held, 1);
        assert.isTrue(held[0]?.queueHeld);
        assert.deepStrictEqual(
          paused.messages.find((message) => message.id === messageId)?.linkedSender,
          linkedSender,
        );
      }).pipe(
        Effect.provide(
          EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(Layer.provideMerge(layer)),
        ),
      );
    }),
  ),
);

it.effect.each([
  { released: true, detached: false, stopped: false, goal: false },
  { released: false, detached: false, stopped: true, goal: false },
  { released: true, detached: true, stopped: false, goal: false },
  { released: true, detached: false, stopped: true, goal: false },
  { released: true, detached: true, stopped: true, goal: false },
  { released: true, detached: false, stopped: false, goal: true },
  { released: true, detached: false, stopped: true, goal: true },
])(
  "preserves accepted auto input when its session disappears before delivery (released: $released, detached: $detached, stopped: $stopped, goal: $goal)",
  ({ released, detached, stopped, goal }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { started, steered, layer, startFirstTurn } =
          yield* nextTurnSelectionHarness("auto-outbox-release");
        yield* Effect.gen(function* () {
          const threadId = yield* startFirstTurn;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
          const command = {
            type: "message.dispatch" as const,
            commandId: CommandId.make("auto-outbox-message"),
            threadId,
            messageId: MessageId.make("message:auto-outbox"),
            text: "Deliver exactly once",
            attachments: [],
            createdBy: "agent" as const,
            creationSource: "mcp" as const,
            dispatchMode: { type: "start_immediately" as const },
            deliveryIntent: "auto" as const,
          };
          yield* orchestrator.dispatch(command);
          let effect = Option.getOrThrow(yield* outbox.claimNext({ workerId: "steer-race" }));
          assert.equal(effect.request.type, "provider-turn.steer");
          if (goal) {
            yield* orchestrator.dispatch({
              ...command,
              commandId: CommandId.make("goal-before-delivery"),
              messageId: MessageId.make("message:goal-before-delivery"),
              text: "/goal finish the task",
            });
          }
          const accepted = yield* orchestrator.getThreadProjection(threadId);
          const providerThread = accepted.providerThreads[0]!;
          const sessions = yield* ProviderSessionManagerV2;
          if (released)
            yield* sessions.release({
              providerSessionId: providerThread.providerSessionId!,
              reason: "manual_shutdown",
            });
          assert.equal(yield* sessions.isLive(providerThread.providerSessionId!), !released);
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("release-thread"),
                type: "provider-thread.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: { ...providerThread, ...(detached ? { providerSessionId: null } : {}) },
              },
              {
                id: EventId.make("release-turn"),
                type: "provider-turn.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: accepted.providerTurns[0]!,
              },
              {
                id: EventId.make("release-run"),
                type: "run.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: { ...accepted.runs[0]!, status: released ? "waiting" : "running" },
              },
            ],
          });
          if (stopped && goal) {
            const current = yield* orchestrator.getThreadProjection(threadId);
            yield* (yield* EventSink.EventSinkV2).write({
              events: [
                {
                  id: EventId.make("goal-interrupted-run"),
                  type: "run.updated",
                  threadId,
                  occurredAt: yield* DateTime.now,
                  payload: {
                    ...current.runs.find((run) => run.id === started[0]!.runId)!,
                    status: "interrupted",
                    completedAt: yield* DateTime.now,
                  },
                },
              ],
            });
            yield* orchestrator.dispatch({
              type: "thread.stop",
              commandId: CommandId.make("stop-after-goal"),
              threadId,
            });
            const stoppedProjection = yield* orchestrator.getThreadProjection(threadId);
            // A lower-ordinal queued turn can execute last after manual reordering. Its
            // Stop still governs an older higher-ordinal steering source.
            const end = yield* DateTime.now;
            yield* (yield* EventSink.EventSinkV2).write({
              events: stoppedProjection.runs.map((run) => ({
                id: EventId.make(`reordered:${run.id}`),
                type: "run.updated" as const,
                threadId,
                occurredAt: end,
                payload: {
                  ...run,
                  status: "interrupted" as const,
                  queuePosition: null,
                  queueHeld: false,
                  startedAt: end,
                  ...(run.id === started[0]!.runId
                    ? { ordinal: 999, completedAt: DateTime.subtract(end, { seconds: 1 }) }
                    : { completedAt: end }),
                },
              })),
            });
            assert.isTrue(
              stoppedProjection.turnItems.some(
                (item) => item.type === "run_interrupt_request" && item.holdQueue === true,
              ),
            );
          } else if (stopped && !released) {
            yield* orchestrator.dispatch({
              type: "thread.stop",
              commandId: CommandId.make("live-stop-before-delivery"),
              threadId,
            });
            // A worker that already captured the old payload must fail its invocation CAS.
            yield* outbox
              .recordSteerDelivery(effect.id, effect.leaseOwner, "invoked")
              .pipe(Effect.flip);
            assert.deepEqual(steered, []);
          } else if (stopped) {
            yield* orchestrator.dispatch({
              type: "run.interrupt",
              commandId: CommandId.make("stop-before-delivery"),
              threadId,
              runId: started[0]!.runId,
              holdQueue: true,
            });
          }
          if (stopped && !goal) {
            // An internal interrupt cannot erase the user's earlier Stop barrier.
            yield* orchestrator.dispatch({
              type: "run.interrupt",
              commandId: CommandId.make("nonholding-interrupt-after-stop"),
              threadId,
              runId: started[0]!.runId,
              holdQueue: false,
            });
          }
          effect = Option.getOrThrow(yield* outbox.get(effect.id));
          if (effect.request.type !== "provider-turn.steer")
            throw new Error("Expected steer effect");
          const uncertain = yield* executor
            .execute({ ...effect, request: { ...effect.request, deliveryPhase: "invoked" } })
            .pipe(Effect.flip);
          assert.equal(uncertain._tag, "OrchestrationEffectExecutionError");
          if (!stopped && !goal) {
            const threads = yield* ThreadManagementService.ThreadManagementService;
            using _readFailure = vi.spyOn(threads, "getThreadRecords").mockImplementationOnce(() =>
              Effect.fail(
                new Orchestrator.OrchestratorProjectionError({
                  threadId,
                  cause: "transient read failure",
                }),
              ),
            );
            const failed = yield* executor.execute(effect).pipe(Effect.flip);
            assert.equal(failed._tag, "OrchestrationEffectExecutionError");
            assert.isTrue(
              yield* outbox.retry({
                effectId: effect.id,
                workerId: "steer-race",
                error: "transient read failure",
                delayMs: 0,
              }),
            );
            effect = Option.getOrThrow(yield* outbox.claimNext({ workerId: "steer-race" }));
            assert.equal(effect.attemptCount, 2);
          }
          // Fail inside locked fallback planning, after reading the accepted message.
          const store = yield* ProjectionStore.ProjectionStoreV2;
          const readRecords = store.getThreadRecords;
          let failedPlanning = false;
          using _planningFailure = vi
            .spyOn(store, "getThreadRecords")
            .mockImplementation((id, collections, selection) => {
              if (!failedPlanning && selection?.turnItemTypes?.includes("run_interrupt_request")) {
                failedPlanning = true;
                return Effect.fail(
                  new ProjectionStore.ProjectionStoreReadError({
                    threadId,
                    cause: "transient planning failure",
                  }),
                );
              }
              return readRecords(id, collections, selection);
            });
          yield* executor.execute(effect).pipe(Effect.flip);
          assert.isTrue(failedPlanning);
          effect = Option.getOrThrow(yield* outbox.get(effect.id));
          yield* executor.execute(effect);
          effect = Option.getOrThrow(yield* outbox.get(effect.id));
          yield* executor.execute(effect);
          yield* orchestrator.dispatch(command);
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(steered, []);
          assert.lengthOf(started, 1);
          const messages = after.messages.filter((message) => message.id === command.messageId);
          assert.lengthOf(messages, 1);
          assert.equal(messages[0]?.text, command.text);
          const queued = after.runs.filter((run) => run.userMessageId === command.messageId);
          assert.lengthOf(queued, 1);
          assert.equal(queued[0]?.status, "queued");
          assert.equal(queued[0]?.queueHeld === true, stopped);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect.each([
  { text: "/compact", driverKind: driver },
  { text: "/logout", driverKind: driver },
  { text: "/goal finish the task", driverKind: ProviderDriverKind.make("cursor") },
])(
  "queues automatic command $text for $driverKind when dispatch sees a live turn",
  ({ text, driverKind }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { started, steered, interrupted, layer, startFirstTurn } =
          yield* nextTurnSelectionHarness(
            "auto-maintenance",
            false,
            true,
            Effect.void,
            true,
            undefined,
            undefined,
            undefined,
            undefined,
            driverKind,
          );
        yield* Effect.gen(function* () {
          const threadId = yield* startFirstTurn;
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const command = {
            type: "message.dispatch" as const,
            commandId: CommandId.make("maintenance"),
            threadId,
            messageId: MessageId.make("message:maintenance"),
            text,
            attachments: [],
            createdBy: "agent" as const,
            creationSource: "mcp" as const,
            dispatchMode: { type: "start_immediately" as const },
            deliveryIntent: "auto" as const,
          };
          yield* orchestrator.dispatch(command);
          yield* orchestrator.dispatch(command);
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            after.runs.find((run) => run.userMessageId === command.messageId)?.status,
            "queued",
          );
          assert.deepEqual(steered, []);
          assert.deepEqual(interrupted, []);
          assert.lengthOf(started, 1);
          // A command sent to steer waits as a turn of its own; a restart refuses it.
          yield* orchestrator.dispatch({
            ...command,
            commandId: CommandId.make("steer_active"),
            messageId: MessageId.make("steer_active"),
            deliveryIntent: undefined,
            dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
          });
          assert.equal(
            (yield* orchestrator.getThreadProjection(threadId)).runs.find(
              (run) => run.userMessageId === MessageId.make("steer_active"),
            )?.status,
            "queued",
          );
          assert.deepEqual(steered, []);
          const failure = yield* orchestrator
            .dispatch({
              ...command,
              commandId: CommandId.make("restart_active"),
              messageId: MessageId.make("restart_active"),
              deliveryIntent: undefined,
              dispatchMode: { type: "restart_active", targetRunId: started[0]!.runId },
            })
            .pipe(Effect.flip);
          assert.equal(failure._tag, "OrchestratorDispatchError");
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("queues a fresh-context MCP goal behind older work without interrupting", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const database = SqlitePersistence.layerMemory;
      const { started, steered, interrupted, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness(
          "mcp-fresh-goal",
          false,
          true,
          Effect.void,
          true,
          undefined,
          undefined,
          undefined,
          database,
        );
      const management = ThreadManagementService.layer.pipe(
        Layer.provide(layer),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(database))),
      );
      yield* Effect.gen(function* () {
        const threadId = yield* startFirstTurn;
        const threads = yield* ThreadManagementService.ThreadManagementService;
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const base = {
          projectId: ProjectId.make("project:mcp-fresh-goal"),
          threadId,
          attachments: [],
          createdBy: "agent" as const,
          creationSource: "mcp" as const,
        };
        yield* threads.sendToThread({
          ...base,
          commandId: CommandId.make("older"),
          messageId: MessageId.make("message:older"),
          text: "older",
          mode: "queue",
        });
        const sent = yield* threads.sendToThread({
          ...base,
          commandId: CommandId.make("fresh-goal"),
          messageId: MessageId.make("message:fresh-goal"),
          text: "/goal finish the task",
          mode: "auto",
          freshContext: true,
        });
        assert.equal(sent.delivery, "queued");
        yield* worker.drain();
        assert.deepEqual(steered, []);
        assert.deepEqual(interrupted, []);
        assert.lengthOf(started, 1);
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          projection.runs
            .filter((run) => run.status === "queued")
            .sort((a, b) => (a.queuePosition ?? a.ordinal) - (b.queuePosition ?? b.ordinal))
            .map(
              (run) =>
                projection.messages.find((message) => message.id === run.userMessageId)?.text,
            ),
          ["older", "/goal finish the task"],
        );
      }).pipe(Effect.provide(Layer.mergeAll(layer, management)));
    }),
  ),
);

it.effect.each([
  { notice: "delegated task question", goal: null, steers: true },
  { notice: "delegated task question", goal: null, steers: false, summary: "/review is ready" },
  { notice: "background command", goal: "active", steers: true },
  { notice: "background command", goal: null, steers: false },
] as const)(
  "a $notice notice steers into the running turn: $steers (goal $goal)",
  ({ notice, goal, steers, ...row }) =>
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `notice-steer-${notice.replaceAll(" ", "-")}-${goal ?? "none"}-${steers}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const { thread } = yield* orchestrator.getThreadProjection(threadId);
        yield* (yield* EventSink.EventSinkV2).write({
          events: [
            {
              id: EventId.make("event:composer-selection"),
              type: "thread.model-selection-updated",
              threadId,
              providerInstanceId: instanceId,
              occurredAt: yield* DateTime.now,
              payload: { ...thread, modelSelection: composerSelection },
            },
          ],
        });
        if (goal !== null) {
          const providerThread = (yield* orchestrator.getThreadProjection(threadId))
            .providerThreads[0]!;
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("event:goal-active"),
                type: "provider-thread.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: { ...providerThread, goal: { objective: "Ship it", status: goal } },
              },
            ],
          });
        }
        const messageId = MessageId.make("message:notice");
        const notification =
          notice === "delegated task question"
            ? {
                source: {
                  kind: "delegated_task" as const,
                  taskIds: [NodeId.make("task:question")],
                  childThreadId: ThreadId.make("thread:question-child"),
                },
                outcome: "updated" as const,
                summary: "summary" in row ? row.summary : "Delegated task has a question",
              }
            : {
                source: { kind: "command" as const },
                outcome: "completed" as const,
                summary: "Background command finished",
              };
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("notice-wake"),
          threadId,
          messageId,
          text: `${notification.summary}\nexit 0: built 12 files`,
          notification,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        });
        yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          steered,
          steers ? [`${notification.summary}\nexit 0: built 12 files`] : [],
        );
        assert.lengthOf(started, 1);
        assert.equal(
          after.runs.some((run) => run.status === "queued"),
          !steers,
        );
        const message = after.messages.find((candidate) => candidate.id === messageId);
        assert.deepEqual(message?.notification, notification);
        if (steers) assert.equal(message?.runId, started[0]?.runId);
        assert.deepEqual(after.thread.modelSelection, composerSelection);
      }).pipe(Effect.provide(layer));
    }),
);

it.effect.each([false, true])("delivers a delegated question once, refused=%s", (refused) =>
  Effect.scoped(
    Effect.gen(function* () {
      let steerCalls = 0;
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "delegated-question-steer",
        false,
        true,
        Effect.void,
        true,
        undefined,
        undefined,
        undefined,
        undefined,
        ProviderDriverKind.make("pi"),
        Effect.gen(function* () {
          steerCalls += 1;
          if (refused)
            return yield* new ProviderAdapter.ProviderAdapterSteerRunError({
              driver: ProviderDriverKind.make("pi"),
              providerThreadId: ProviderThreadId.make("provider-thread:delegated-question-steer"),
              providerTurnId: ProviderTurnId.make("provider-turn:delegated-question-steer"),
              deliveryNotSent: true,
              cause: "Pi is compacting",
            });
        }),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const { thread } = yield* orchestrator.getThreadProjection(threadId);
        yield* (yield* EventSink.EventSinkV2).write({
          events: [
            {
              id: EventId.make("event:composer-selection"),
              type: "thread.model-selection-updated",
              threadId,
              providerInstanceId: instanceId,
              occurredAt: yield* DateTime.now,
              payload: { ...thread, modelSelection: composerSelection },
            },
          ],
        });
        const messageId = MessageId.make("message:delegated-task-question:child-question");
        const notification = {
          source: {
            kind: "delegated_task" as const,
            taskIds: [NodeId.make("task:question")],
            childThreadId: ThreadId.make("thread:question-child"),
          },
          outcome: "updated" as const,
          summary: "Delegated task has a question",
        };
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("question-wake"),
          threadId,
          messageId,
          text: "Delegated task asked a question",
          notification,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        });
        yield* worker.runOnce();
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(steerCalls, 1);
        assert.deepEqual(steered, refused ? [] : ["Delegated task asked a question"]);
        assert.lengthOf(started, 1);
        const queued = after.runs.filter((run) => run.status === "queued");
        assert.lengthOf(queued, refused ? 1 : 0);
        if (refused) assert.equal(queued[0]?.userMessageId, messageId);
        assert.lengthOf(
          after.messages.filter((message) => message.id === messageId),
          1,
        );
        yield* worker.drain();
        assert.equal(steerCalls, 1);
        const message = after.messages.find((candidate) => candidate.id === messageId);
        assert.equal(message?.runId, refused ? queued[0]?.id : started[0]?.runId);
        assert.deepEqual(message?.notification, notification);
        assert.deepEqual(after.thread.modelSelection, composerSelection);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("sends a selected held message once and preserves the remaining queue order", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("send-held-message");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* Effect.forEach(["queued-first", "selected", "queued-last"], (text) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(text),
          threadId,
          messageId: MessageId.make(`message:${text}`),
          text,
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        }),
      );
      const before = yield* orchestrator.getThreadProjection(threadId);
      const selected = before.runs.find(
        (run) => run.userMessageId === MessageId.make("message:selected"),
      )!;
      const sink = yield* EventSink.EventSinkV2;
      const now = yield* DateTime.now;
      // Restart recovery cancels the active run and holds every queued message.
      yield* sink.write({
        events: [
          ...before.runs.map((run) => ({
            id: EventId.make(`restart:${run.id}`),
            type: "run.updated" as const,
            threadId,
            runId: run.id,
            occurredAt: now,
            payload:
              run.status === "queued"
                ? { ...run, queueHeld: true }
                : { ...run, status: "cancelled" as const, completedAt: now },
          })),
          ...before.providerTurns.map((turn) => ({
            id: EventId.make(`restart:${turn.id}`),
            type: "provider-turn.updated" as const,
            threadId,
            occurredAt: now,
            payload: { ...turn, status: "cancelled" as const, completedAt: now },
          })),
        ],
      });
      const command = {
        type: "queued-run.start" as const,
        commandId: CommandId.make("send-selected"),
        threadId,
        runId: selected.id,
      };
      yield* orchestrator.dispatch(command);
      yield* worker.drain();
      yield* orchestrator.dispatch(command);
      yield* worker.drain();
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(after.runs.find((run) => run.id === selected.id)?.status, "running");
      assert.deepEqual(
        started.map((input) => input.message.text),
        ["first", "selected"],
      );
      assert.equal(started[1]?.runId, selected.id);
      assert.equal(started[1]?.message.messageId, selected.userMessageId);
      assert.deepEqual(started[1]?.modelSelection, runSelection);
      assert.deepEqual(steered, []);
      assert.lengthOf(
        after.messages.filter((message) => message.id === selected.userMessageId),
        1,
      );
      assert.deepEqual(
        after.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
        [MessageId.make("message:queued-first"), MessageId.make("message:queued-last")],
      );
      assert.isFalse(after.runs.some((run) => run.status === "queued" && run.queueHeld));
      const duplicate = yield* orchestrator
        .dispatch({
          ...command,
          commandId: CommandId.make("send-selected-again"),
        })
        .pipe(Effect.flip);
      assert.equal(duplicate._tag, "OrchestratorDispatchError");
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(
  (["preparing", "starting", "running"] as const).flatMap((status) =>
    ([false, "steer", "auto"] as const).map((direct) => ({ status, direct })),
  ),
)("delivers a deferred steer for a $status run (direct: $direct)", ({ status, direct }) =>
  Effect.gen(function* () {
    const restarting = status === "starting" && !direct;
    const { started, steered, layer, startFirstTurn, events } = yield* nextTurnSelectionHarness(
      `deferred-steer-${status}-${direct}`,
      restarting,
      !restarting,
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const before = yield* orchestrator.getThreadProjection(threadId);
      const run = before.runs[0]!;
      const turn = before.providerTurns[0]!;
      const now = yield* DateTime.now;
      yield* (yield* EventSink.EventSinkV2).write({
        events: [
          {
            id: EventId.make("setup-run"),
            type: "run.updated",
            threadId,
            occurredAt: now,
            payload: { ...run, status },
          },
          {
            id: EventId.make("setup-turn"),
            type: "provider-turn.updated",
            threadId,
            occurredAt: now,
            payload: { ...turn, status: "pending" },
          },
        ],
      });
      if (status === "preparing" && !direct) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("edited-steer"),
          threadId,
          messageId: MessageId.make("message:edited-steer"),
          text: "edit this steer",
          attachments: [],
          deliveryIntent: "steer",
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const edited = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.userMessageId === MessageId.make("message:edited-steer"),
        )!;
        assert.equal(edited.steerWhenReadyRunId, run.id);
        yield* orchestrator.dispatch({
          type: "queued-run.edit",
          commandId: CommandId.make("edit-steer-to-compact"),
          threadId,
          runId: edited.id,
          text: "/compact",
        });
        const compact = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.id === edited.id,
        )!;
        assert.isUndefined(compact.steerWhenReadyRunId);
        assert.equal(compact.queuePosition, edited.queuePosition);
      }
      if (direct) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("earlier-queue"),
          threadId,
          messageId: MessageId.make("message:earlier-queue"),
          text: "earlier queued selection",
          modelSelection: composerSelection,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
      }
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queue"),
        threadId,
        messageId: MessageId.make("message:steer-after-setup"),
        text: "steer after setup",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        ...(direct ? { deliveryIntent: direct } : {}),
        createdBy: direct === "auto" ? "agent" : "user",
        creationSource: direct === "auto" ? "mcp" : "web",
      });
      const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (candidate) => candidate.userMessageId === MessageId.make("message:steer-after-setup"),
      )!;
      if (direct) assert.equal(queued.steerWhenReadyRunId, run.id);
      assert.deepEqual(queued.modelSelection, runSelection);
      for (const text of ["/compact", "/logout"]) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`queue-${text}`),
          threadId,
          messageId: MessageId.make(`message:${text}`),
          text,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const maintenance = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.userMessageId === MessageId.make(`message:${text}`),
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make(`promote-${text}`),
          threadId,
          queuedRunId: maintenance.id,
          targetRunId: run.id,
        });
        const promoted = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.id === maintenance.id,
        )!;
        assert.equal(promoted.status, "queued");
        assert.equal(promoted.queuePosition, 1);
      }
      const command = {
        type: "queued-message.promote-to-steer" as const,
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: queued.id,
        targetRunId: run.id,
      };
      if (!direct) {
        yield* orchestrator.dispatch(command);
        yield* orchestrator.dispatch(command);
      }
      let secondQueuedId: typeof queued.id | undefined;
      if (restarting) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("second"),
          threadId,
          messageId: MessageId.make("message:second"),
          text: "second steer",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        secondQueuedId = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.userMessageId === MessageId.make("message:second"),
        )!.id;
        yield* orchestrator.dispatch({
          ...command,
          commandId: CommandId.make("promote-second"),
          queuedRunId: secondQueuedId,
        });
      }
      const secondReDeferred = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === secondQueuedId &&
            event.payload.status === "queued",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      const secondPromoted = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === secondQueuedId &&
            event.payload.status === "cancelled",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* worker.drain();
      assert.deepEqual(steered, []);
      const promoted = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === queued.id &&
            event.payload.status === "cancelled",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* (yield* EventSink.EventSinkV2).write({
        events: [
          {
            id: EventId.make("ready-run"),
            type: "run.updated",
            threadId,
            occurredAt: now,
            payload: run,
          },
        ],
      });
      yield* Queue.offer(events, { type: "provider_turn.updated", driver, providerTurn: turn });
      yield* Fiber.join(promoted);
      if (restarting) yield* Fiber.join(secondReDeferred);
      yield* worker.drain();
      if (restarting) {
        yield* Fiber.join(secondPromoted);
        yield* worker.drain();
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "steer after setup", "second steer"],
        );
      } else {
        assert.deepEqual(steered.map(withoutSenderNote), ["steer after setup"]);
        assert.lengthOf(started, 1);
      }
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        after.messages.find((message) => message.id === queued.userMessageId)?.runId,
        run.id,
      );
      assert.equal(after.runs.find((candidate) => candidate.id === run.id)?.status, "running");
    }).pipe(Effect.provide(layer));
  }),
);

// The steered run can end before any turn of it runs; its deferred steer must not wait on it.
it.effect.each(["steer", "auto"] as const)(
  "starts a deferred %s in queue order when its run ends before a turn runs",
  (deliveryIntent) =>
    Effect.gen(function* () {
      const { started, steered, events, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `deferred-${deliveryIntent}-run-ended`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = yield* startFirstTurn;
        const before = yield* orchestrator.getThreadProjection(threadId);
        const run = before.runs[0]!;
        const turn = before.providerTurns[0]!;
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("gap-run"),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: { ...run, status: "starting" },
            },
            {
              id: EventId.make("gap-turn"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: now,
              payload: { ...turn, status: "pending" },
            },
          ],
        });
        const send = (key: string, intent?: "steer" | "auto") =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(key),
            threadId,
            messageId: MessageId.make(key),
            text: key,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            ...(intent === undefined ? {} : { deliveryIntent: intent }),
            createdBy: "user",
            creationSource: "web",
          });
        const runFor = (key: string) =>
          Effect.map(orchestrator.getThreadProjection(threadId), ({ runs }) =>
            runs.find((candidate) => candidate.userMessageId === key)!,
          );
        // Replays from the current sequence, so an event the reactor writes first is still seen.
        const watch = Effect.fn("watch")(function* (
          predicate: (event: OrchestrationV2DomainEvent) => boolean,
        ) {
          const afterSequence = yield* sink.latestSequence({ threadId });
          return yield* sink.stream({ threadId, afterSequence }).pipe(
            Stream.filter((stored) => predicate(stored.event)),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        });
        const watchRun = (key: string, status: "starting" | "waiting") =>
          watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.userMessageId === key &&
              event.payload.status === status,
          );
        yield* send("queued");
        yield* send("deferred", deliveryIntent);
        assert.equal((yield* runFor("deferred")).steerWhenReadyRunId, run.id);

        const queuedStarting = yield* watchRun("queued", "starting");
        yield* sink.write({
          events: [
            {
              id: EventId.make("gap-turn-cancelled"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: now,
              payload: { ...turn, status: "cancelled", completedAt: now },
            },
            {
              id: EventId.make("gap-run-completed"),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: { ...run, status: "completed", completedAt: now },
            },
          ],
        });
        yield* Fiber.join(queuedStarting);
        const queuedTurnRunning = yield* watch(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.payload.status === "running" &&
            event.payload.runAttemptId === started[1]?.attemptId,
        );
        yield* worker.drain();
        yield* Fiber.join(queuedTurnRunning);
        assert.equal((yield* runFor("deferred")).status, "queued");

        const queuedRun = started[1]!;
        const queuedTurn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns.find(
          (candidate) => candidate.runAttemptId === queuedRun.attemptId,
        )!;
        const queuedSettled = yield* watchRun("queued", "waiting");
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...queuedTurn, status: "completed", completedAt: now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: queuedTurn.providerThreadId,
          providerTurnId: queuedTurn.id,
          runOrdinal: queuedRun.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(queuedSettled);
        // Created behind the queued run, it starts by an update; created after that run ended,
        // it is created starting.
        const deferredStarting = yield* watch(
          (event) =>
            (event.type === "run.created" || event.type === "run.updated") &&
            event.payload.userMessageId === "deferred" &&
            event.payload.status === "starting",
        );
        yield* worker.drain();
        yield* Fiber.join(deferredStarting);
        yield* worker.drain();
        assert.deepEqual(
          started.map((input) => input.message.text),
          ["first", "queued", "deferred"],
        );
        assert.deepEqual(steered, []);
      }).pipe(Effect.provide(layer));
    }),
);

// Rules that queue an automatic message beside a live turn also queue it beside a turn gap.
it.effect.each([
  { kind: "a fresh session" },
  { kind: "/compact" },
  { kind: "/logout" },
  { kind: "a goal command without native goals" },
  { kind: "Stop" },
  { kind: "a released session" },
  { kind: "a provider that cannot steer" },
  { kind: "a waiting run" },
] as const)("queues an automatic message sent in a turn gap for $kind", ({ kind }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `auto-gap-queues-${kind.replaceAll(/[^a-z]+/g, "-")}`,
        false,
        kind !== "a provider that cannot steer",
        Effect.void,
        true,
        kind === "a goal command without native goals" ? false : undefined,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = yield* startFirstTurn;
        const before = yield* orchestrator.getThreadProjection(threadId);
        const run = before.runs[0]!;
        const now = yield* DateTime.now;
        if (kind === "a released session") {
          const providerThread = before.providerThreads[0]!;
          // The release ends the run's turn through its event consumer, after release returns;
          // the gap below is written once that has landed, or it would be overwritten.
          const runEnded = yield* sink.stream({ afterSequence: 0 }).pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" &&
                stored.event.payload.id === run.id &&
                stored.event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* (yield* ProviderSessionManagerV2).release({
            providerSessionId: providerThread.providerSessionId!,
            reason: "manual_shutdown",
          });
          yield* Fiber.join(runEnded);
          yield* sink.write({
            events: [
              {
                id: EventId.make("released-thread"),
                type: "provider-thread.updated",
                threadId,
                occurredAt: now,
                payload: providerThread,
              },
            ],
          });
        }
        yield* sink.write({
          events: [
            {
              id: EventId.make("gap-run"),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: {
                ...run,
                status:
                  kind === "a waiting run"
                    ? "waiting"
                    : kind === "a released session"
                      ? "running"
                      : "starting",
              },
            },
            {
              id: EventId.make("gap-turn"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: now,
              payload: { ...before.providerTurns[0]!, status: "pending" },
            },
          ],
        });
        if (kind === "Stop") {
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make("stop-in-gap"),
            threadId,
          });
        }
        const messageId = MessageId.make("message:auto-in-gap");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("auto-in-gap"),
          threadId,
          messageId,
          text:
            kind === "/compact" || kind === "/logout"
              ? kind
              : kind === "a goal command without native goals"
                ? "/goal ship it"
                : "automatic message",
          attachments: [],
          ...(kind === "a fresh session" ? { freshContext: true } : {}),
          dispatchMode: { type: "start_immediately" },
          deliveryIntent: "auto",
          createdBy: "agent",
          creationSource: "mcp",
        });
        const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (candidate) => candidate.userMessageId === messageId,
        );
        assert.equal(queued?.status, "queued");
        assert.isUndefined(queued?.steerWhenReadyRunId);
        assert.deepEqual(steered, []);
        assert.lengthOf(started, 1);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("force sends multiple queued messages and can force send a requeued message again", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "multiple-promoted-messages",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const queue = (text: string, commandId: string) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(commandId),
          threadId,
          messageId: MessageId.make(`message:${text}`),
          text,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
      for (const text of ["one", "two", "remaining"]) yield* queue(text, `queue-${text}`);
      const promote = Effect.fn("test.promote")(function* (text: string, commandId: string) {
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const queued = projection.runs.find(
          (run) =>
            run.status === "queued" && run.userMessageId === MessageId.make(`message:${text}`),
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make(commandId),
          threadId,
          queuedRunId: queued.id,
          targetRunId: started[0]!.runId,
        });
      });
      yield* promote("one", "promote-one");
      yield* promote("two", "promote-two");
      yield* worker.drain();
      assert.deepEqual(steered, ["one", "two"]);

      // The completion-race fallback redispatches the same message into the queue.
      yield* queue("one", "requeue-one");
      const requeued = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        requeued.turnItems.find(
          (item) => item.type === "user_message" && item.messageId === "message:one",
        )?.status,
        "pending",
      );
      yield* promote("one", "promote-one-again");
      yield* worker.drain();
      assert.deepEqual(steered, ["one", "two", "one"]);
      assert.lengthOf(started, 1);
      const final = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        final.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
        [MessageId.make("message:remaining")],
      );
      for (const text of ["one", "two"]) {
        const items = final.turnItems.filter(
          (item) => item.type === "user_message" && item.messageId === `message:${text}`,
        );
        assert.lengthOf(items, 1);
        assert.equal(items[0]?.status, "completed");
      }
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("keeps a message queued when send now races with a running turn", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn } = yield* nextTurnSelectionHarness("send-queued-race");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queue"),
        threadId,
        messageId: MessageId.make("message:queued"),
        text: "queued",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const before = yield* orchestrator.getThreadProjection(threadId);
      const queued = before.runs.find((run) => run.status === "queued")!;
      const error = yield* orchestrator
        .dispatch({
          type: "queued-run.start",
          commandId: CommandId.make("send-now"),
          threadId,
          runId: queued.id,
        })
        .pipe(Effect.flip);
      yield* worker.drain();
      assert.equal(error._tag, "OrchestratorDispatchError");
      assert.equal(started.length, 1);
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(after.runs, before.runs);
    }).pipe(Effect.provide(layer));
  }),
);

// regression: #19210 a provider that can restart the turn must still steer, since restarting ends its native subagents
it.effect(
  "steers a changed turn-scoped selection into a running turn the provider could restart",
  () =>
    Effect.gen(function* () {
      const { started, steered, steeredSelections, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("steering-selection-change", true);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const steer = (id: string, selection: ModelSelection) =>
          orchestrator
            .dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(id),
              threadId,
              messageId: MessageId.make(`message:${id}`),
              text: id,
              attachments: [],
              modelSelection: selection,
              dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
              createdBy: "user",
              creationSource: "web",
            })
            .pipe(Effect.andThen(worker.drain()));

        yield* steer("steer-changed", composerSelection);
        const changed = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, ["steer-changed"]);
        assert.equal(started.length, 1);
        assert.lengthOf(changed.attempts, 1);
        assert.equal(changed.runs[0]?.status, "running");
        assert.deepEqual(changed.runs[0]?.modelSelection, runSelection);
        assert.deepEqual(changed.thread.modelSelection, composerSelection);

        yield* steer("steer-reverted", runSelection);
        const reverted = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, ["steer-changed", "steer-reverted"]);
        assert.lengthOf(reverted.attempts, 1);
        assert.deepEqual(reverted.thread.modelSelection, runSelection);

        const otherSelection = {
          instanceId: ProviderInstanceId.make("codex-work"),
          model: "other",
        };
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.write({
          events: [
            {
              id: EventId.make("switched-away"),
              type: "thread.provider-switched",
              threadId,
              providerInstanceId: otherSelection.instanceId,
              occurredAt: yield* DateTime.now,
              payload: {
                ...reverted.thread,
                providerInstanceId: otherSelection.instanceId,
                modelSelection: otherSelection,
              },
            },
          ],
        });
        yield* steer("steer-back", runSelection);
        const back = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, ["steer-changed", "steer-reverted", "steer-back"]);
        // The adapter gets each steer's selection so it can switch the live turn, and back.
        assert.deepEqual(steeredSelections, [composerSelection, runSelection, runSelection]);
        assert.equal(back.thread.providerInstanceId, instanceId);
        assert.deepEqual(back.thread.modelSelection, runSelection);
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("restarts the turn for an image when native steering takes text only", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "steering-text-only-attachment",
      true,
      true,
      Effect.void,
      false,
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const before = yield* orchestrator.getThreadProjection(threadId);
      const originalStart = before.runs[0]!.startedAt;
      assert.isNotNull(originalStart);
      yield* TestClock.adjust("10 seconds");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer-image"),
        threadId,
        messageId: MessageId.make("message:steer-image"),
        text: "look at this",
        attachments: [
          {
            type: "image",
            id: ChatAttachmentId.make("steer-image"),
            name: "screenshot.png",
            mimeType: "image/png",
            sizeBytes: 4,
          },
        ],
        modelSelection: runSelection,
        dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(after.runs[0]?.id, before.runs[0]?.id);
      assert.deepEqual(after.runs[0]?.startedAt, originalStart);
      assert.equal(after.attempts[0]?.status, "superseded");
      const restartedAt = yield* DateTime.now;
      assert.deepEqual(after.attempts[1]?.startedAt, restartedAt);
      assert.deepEqual(
        after.nodes.find((node) => node.id === after.runs[0]?.rootNodeId)?.startedAt,
        restartedAt,
      );
      assert.deepEqual(steered, []);
      assert.deepEqual(
        started.map((turn) => [turn.message.text, turn.message.attachments.length]),
        [
          ["first", 0],
          ["look at this", 1],
        ],
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(["complete", "stop-before-end"] as const)(
  "starts a steer that missed the turn in its place on the saved next-turn selection: %s",
  (timing) =>
    Effect.gen(function* () {
      const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `steering-selection-follow-up-${timing}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId: MessageId.make("message:steer"),
          text: "late steer",
          attachments: [],
          modelSelection: composerSelection,
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queued-later"),
          threadId,
          messageId: MessageId.make("message:queued-later"),
          text: "queued later",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        if (timing === "stop-before-end") {
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop"),
            threadId,
            runId: first.runId,
            holdQueue: true,
          });
        }
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        if (timing === "stop-before-end") {
          const queued = (yield* orchestrator.getThreadProjection(threadId)).runs
            .filter((run) => run.status === "queued")
            .toSorted((left, right) => (left.queuePosition ?? 0) - (right.queuePosition ?? 0));
          // The Stop holds what was queued behind the stopped turn, the missed steer included, until
          // the person resumes; the steer keeps its place ahead of the later message.
          assert.deepEqual(
            queued.map((run) => [run.userMessageId, run.queueHeld === true]),
            [
              [MessageId.make("message:steer"), true],
              [MessageId.make("message:queued-later"), true],
            ],
          );
          return;
        }
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, []);
        assert.equal(started.length, 2);
        assert.equal(started[1]?.message.messageId, MessageId.make("message:steer"));
        assert.deepEqual(started[1]?.modelSelection, composerSelection);
        assert.deepEqual(projection.thread.modelSelection, composerSelection);
        const message = projection.turnItems.find(
          (item) =>
            item.type === "user_message" && item.messageId === MessageId.make("message:steer"),
        );
        assert.equal(message?.runId, started[1]!.runId);
        assert.isAbove(
          message!.ordinal,
          Math.max(
            ...projection.turnItems
              .filter((item) => item.runId === first.runId)
              .map((item) => item.ordinal),
          ),
        );
      }).pipe(Effect.provide(layer));
    }),
);

const declinedSteerTimings = [
  "complete",
  "stop-before-decline",
  "stop-after-decline",
  "queued-model",
  "two-steers",
] as const;

it.effect.each(declinedSteerTimings)(
  "persists a declined steer without blocking the worker: %s",
  (timing) =>
    Effect.gen(function* () {
      let beforeDecline: Effect.Effect<void> = Effect.void;
      const { events, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `steering-declined-${timing}`,
        false,
        true,
        Effect.void,
        true,
        undefined,
        (turn) =>
          Effect.suspend(() => beforeDecline).pipe(
            Effect.andThen(
              Effect.fail(
                new ProviderAdapter.ProviderAdapterSteerDeclinedError({
                  driver,
                  providerThreadId: turn.providerThread.id,
                  providerTurnId: turn.providerTurnId,
                }),
              ),
            ),
          ),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        const stop = orchestrator
          .dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop"),
            threadId,
            runId: first.runId,
            holdQueue: true,
          })
          .pipe(Effect.asVoid, Effect.orDie);
        if (timing === "stop-before-decline") beforeDecline = stop;
        const steerSelection = { ...runSelection, model: "steer-model" };
        if (timing === "queued-model") {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("queued-model"),
            threadId,
            messageId: MessageId.make("message:queued-model"),
            text: "already queued",
            attachments: [],
            modelSelection: composerSelection,
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          beforeDecline = Effect.forEach(["queued-later", "steer-later", "queued-last"], (id) =>
            orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(id),
              threadId,
              messageId: MessageId.make(`message:${id}`),
              text: id,
              attachments: [],
              dispatchMode:
                id === "steer-later"
                  ? { type: "steer_active", targetRunId: first.runId }
                  : { type: "queue_after_active" },
              createdBy: "user",
              creationSource: "web",
            }),
          ).pipe(Effect.asVoid, Effect.orDie);
        }
        if (timing === "two-steers") {
          // The second steer arrives before the first is declined.
          beforeDecline = orchestrator
            .dispatch({
              type: "message.dispatch",
              commandId: CommandId.make("steer-second"),
              threadId,
              messageId: MessageId.make("message:steer-second"),
              text: "second",
              attachments: [],
              dispatchMode: { type: "steer_active", targetRunId: first.runId },
              createdBy: "user",
              creationSource: "web",
            })
            .pipe(
              Effect.andThen(Effect.sync(() => (beforeDecline = Effect.void))),
              Effect.asVoid,
              Effect.orDie,
            );
        }
        const messageId = MessageId.make("message:declined");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId,
          text: "follow up",
          attachments: [],
          ...(timing === "queued-model" ? { modelSelection: steerSelection } : {}),
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        const pending = yield* orchestrator.getThreadProjection(threadId);
        const queued = pending.runs.find(
          (run) => run.userMessageId === messageId && run.id !== first.runId,
        );
        const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(
          CommandId.make("steer"),
        );
        assert.deepEqual(
          effects.map((effect) => [effect.request.type, effect.status]),
          [["provider-turn.steer", "succeeded"]],
        );
        assert.equal(started.length, 1);
        if (timing === "stop-before-decline") {
          assert.isUndefined(queued);
          return;
        }
        assert.equal(queued?.status, "queued");
        assert.equal(queued?.userMessageId, messageId);
        if (timing === "queued-model") {
          assert.deepEqual(queued?.modelSelection, steerSelection);
          assert.deepEqual(
            pending.runs
              .filter((run) => run.status === "queued")
              .toSorted(
                (left, right) =>
                  (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal),
              )
              .map((run) => run.userMessageId),
            [
              MessageId.make("message:queued-model"),
              messageId,
              MessageId.make("message:queued-later"),
              MessageId.make("message:steer-later"),
              MessageId.make("message:queued-last"),
            ],
          );
          return;
        }
        if (timing === "two-steers") {
          assert.deepEqual(
            pending.runs
              .filter((run) => run.status === "queued")
              .toSorted(
                (left, right) =>
                  (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal),
              )
              .map((run) => run.userMessageId),
            [messageId, MessageId.make("message:steer-second")],
          );
          return;
        }
        if (timing === "stop-after-decline") {
          yield* stop;
          yield* worker.drain();
          yield* orchestrator.resumeQueuedRuns;
          yield* worker.drain();
          const held = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(held.runs.find((run) => run.id === queued?.id)?.queueHeld, true);
          assert.equal(started.length, 1);
          return;
        }
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const turn = pending.providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
        assert.equal(started.length, 2);
        assert.equal(started[1]?.message.messageId, messageId);
        assert.equal(started[1]?.runId, queued?.id);
      }).pipe(Effect.provide(layer));
    }),
);

// A declined steer never reached the provider: its follow-up's effect succeeds with the follow-up
// recorded, and a provider failure of the turn it missed does not hold it.
it.effect("starts a declined steer's follow-up when the turn it missed then fails", () =>
  Effect.gen(function* () {
    const { events, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "steering-declined-then-failed",
      false,
      true,
      Effect.void,
      true,
      undefined,
      (turn) =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterSteerDeclinedError({
            driver,
            providerThreadId: turn.providerThread.id,
            providerTurnId: turn.providerTurnId,
          }),
        ),
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const messageId = MessageId.make("message:declined-then-failed");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer"),
        threadId,
        messageId,
        text: "follow up",
        attachments: [],
        dispatchMode: { type: "steer_active", targetRunId: first.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      const pending = yield* orchestrator.getThreadProjection(threadId);
      const queued = pending.runs.find(
        (run) => run.userMessageId === messageId && run.id !== first.runId,
      );
      assert.equal(queued?.status, "queued");
      const steers = yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(
        CommandId.make("steer"),
      );
      assert.deepEqual(
        steers.map((effect) => [
          effect.status,
          effect.request.type === "provider-turn.steer" ? effect.request.deliveryPhase : null,
        ]),
        [["succeeded", "follow_up"]],
      );

      // The failed run's end starts the queue or holds it; either way the follow-up says so.
      const decided = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === queued!.id &&
            (event.payload.status === "starting" || event.payload.queueHeld === true),
        ),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const turn = pending.providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "failed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "failed",
        failure: makeProviderFailure({ message: "Provider overloaded", class: "provider_error" }),
        failureItemOrdinal: 100,
        threadDisposition: "reusable",
      });
      const [decision] = yield* Fiber.join(decided);
      assert.deepEqual(
        decision?.type === "run.updated"
          ? [decision.payload.status, decision.payload.queueHeld === true]
          : undefined,
        ["starting", false],
      );
      yield* worker.drain();
      assert.equal(started.length, 2);
      assert.equal(started[1]?.message.messageId, messageId);
    }).pipe(Effect.provide(layer));
  }),
);

// An automatic Send now merges queued messages into one steer: the first names it, the rest ride
// along. When the session is gone, none of them reached the provider, so each one's follow-up
// starts after the turn fails, the merged ones too.
it.effect("starts every merged steer's follow-up when its session is gone and the turn fails", () =>
  Effect.gen(function* () {
    const { events, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "steering-merged-missing-session",
      false,
      true,
      Effect.void,
      true,
      undefined,
      (turn) =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterSteerRunError({
            driver,
            providerThreadId: turn.providerThread.id,
            providerTurnId: turn.providerTurnId,
            deliveryNotSent: true,
            cause: "SessionNotFoundError: native prompt returned 404",
          }),
        ),
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const messageIds = [
        MessageId.make("message:merged:one"),
        MessageId.make("message:merged:two"),
      ];
      for (const messageId of messageIds)
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`queue:${messageId}`),
          threadId,
          messageId,
          text: `queued ${messageId}`,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
      const queuedRuns = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
        (run) => run.status === "queued",
      );
      assert.lengthOf(queuedRuns, 2);
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make(`${QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX}merged`),
        threadId,
        queuedRunId: queuedRuns[0]!.id,
        mergedQueuedRunIds: [queuedRuns[1]!.id],
        targetRunId: first.runId,
      });
      yield* worker.drain();
      const pending = yield* orchestrator.getThreadProjection(threadId);
      const followUps = () =>
        messageIds.map((messageId) =>
          pending.runs.find(
            (run) => run.userMessageId === messageId && run.steerFollowUpOfRunId === first.runId,
          ),
        );
      assert.deepEqual(
        followUps().map((run) => run?.status),
        ["queued", "queued"],
      );

      const [oneRun, twoRun] = followUps();
      // The failed run's end decides the queue on its own: the first follow-up starts, and every
      // follow-up's hold is recorded on the way.
      const decided = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            (event.payload.id === oneRun!.id || event.payload.id === twoRun!.id),
        ),
        Stream.takeUntil(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === oneRun!.id &&
            event.payload.status === "starting",
        ),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const turn = pending.providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "failed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "failed",
        failure: makeProviderFailure({ message: "Session not found", class: "provider_error" }),
        failureItemOrdinal: 100,
        threadDisposition: "reusable",
      });
      const updates = yield* Fiber.join(decided);
      // Neither follow-up is held behind the failure: the provider received neither part.
      assert.deepEqual(
        updates.flatMap((event) =>
          event.type === "run.updated" && event.payload.queueHeld === true
            ? [event.payload.userMessageId]
            : [],
        ),
        [],
      );
      yield* worker.drain();
      assert.equal(started.length, 2);
      assert.equal(started[1]?.message.messageId, messageIds[0]);
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.notEqual(
        after.runs.find((run) => run.id === twoRun!.id)?.queueHeld,
        true,
        "the merged part's follow-up waits held",
      );
      assert.equal(started[1]?.message.messageId, messageIds[0]);
    }).pipe(Effect.provide(layer));
  }),
);

// A message's later steer is the one its run's end decides on: declined once, then promoted
// again and accepted, the provider had it, so a provider failure holds its follow-up.
it.effect("holds an accepted steer's follow-up when the turn fails, after an earlier decline", () =>
  Effect.gen(function* () {
    let steerCalls = 0;
    const { events, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "steering-declined-then-accepted-then-failed",
      false,
      true,
      Effect.void,
      true,
      undefined,
      (turn) =>
        Effect.suspend(() =>
          (steerCalls += 1) === 1
            ? Effect.fail(
                new ProviderAdapter.ProviderAdapterSteerDeclinedError({
                  driver,
                  providerThreadId: turn.providerThread.id,
                  providerTurnId: turn.providerTurnId,
                }),
              )
            : Effect.void,
        ),
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const sink = yield* EventSink.EventSinkV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const messageId = MessageId.make("message:declined-then-accepted");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer"),
        threadId,
        messageId,
        text: "follow up",
        attachments: [],
        dispatchMode: { type: "steer_active", targetRunId: first.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      const declined = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.userMessageId === messageId && run.status === "queued",
      );
      assert.isDefined(declined);
      // Sent again into the same turn, the provider takes it this time.
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: declined!.id,
        targetRunId: first.runId,
      });
      yield* worker.drain();
      assert.equal(steerCalls, 2);

      // The provider then fails before reading it: the steer is left unread.
      const pending = yield* orchestrator.getThreadProjection(threadId);
      const item = pending.turnItems.find(
        (candidate) => candidate.type === "user_message" && candidate.messageId === messageId,
      )!;
      const now = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make("unread-steer"),
            type: "turn-item.updated",
            threadId,
            runId: first.runId,
            ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
            occurredAt: now,
            payload: { ...item, status: "pending", completedAt: null, updatedAt: now },
          },
        ],
      });
      const decided = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            (event.type === "run.updated" || event.type === "run.created") &&
            event.payload.userMessageId === messageId &&
            event.payload.id !== first.runId &&
            event.payload.id !== declined!.id &&
            (event.payload.status === "starting" || event.payload.queueHeld === true),
        ),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const turn = pending.providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "failed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "failed",
        failure: makeProviderFailure({ message: "Provider overloaded", class: "provider_error" }),
        failureItemOrdinal: 100,
        threadDisposition: "reusable",
      });
      const [decision] = yield* Fiber.join(decided);
      assert.deepEqual(
        decision?.type === "run.updated" || decision?.type === "run.created"
          ? [decision.payload.status, decision.payload.queueHeld === true]
          : undefined,
        ["queued", true],
      );
      yield* worker.drain();
      assert.equal(started.length, 1);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(["archive", "delete"] as const)(
  "drops an accepted steer's follow-up after a thread %s",
  (removal) =>
    Effect.gen(function* () {
      const name = `steering-follow-up-${removal}`;
      let beforeNonDelivery: Effect.Effect<void> = Effect.void;
      const { started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        name,
        false,
        true,
        Effect.void,
        true,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        Effect.suspend(() => beforeNonDelivery).pipe(
          Effect.andThen(
            Effect.fail(
              new ProviderAdapter.ProviderAdapterSteerRunError({
                driver,
                providerThreadId: ProviderThreadId.make(`provider-thread:thread:${name}`),
                providerTurnId: ProviderTurnId.make(`provider-turn:${name}`),
                deliveryNotSent: true,
                cause: "turn already completed",
              }),
            ),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        const commandId = CommandId.make(removal);
        beforeNonDelivery = orchestrator
          .dispatch(
            removal === "archive"
              ? { type: "thread.archive", commandId, threadId }
              : { type: "thread.delete", commandId, threadId },
          )
          .pipe(Effect.asVoid, Effect.orDie);
        const messageId = MessageId.make("message:steer");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId,
          text: "late steer",
          attachments: [],
          dispatchMode: { type: "steer_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(
          CommandId.make("steer"),
        );
        assert.deepEqual(
          effects.map((effect) => [effect.request.type, effect.status]),
          [["provider-turn.steer", "succeeded"]],
        );
        const after = yield* orchestrator.getThreadProjection(threadId);
        if (removal === "archive") assert.isNotNull(after.thread.archivedAt);
        else assert.isNotNull(after.thread.deletedAt);
        assert.deepEqual(
          after.runs.filter((run) => run.userMessageId === messageId),
          [],
        );
        assert.equal(
          after.messages.find((message) => message.id === messageId)?.runId,
          first.runId,
        );
        assert.lengthOf(started, 1);
      }).pipe(Effect.provide(layer));
    }),
);

// regression: #26695 cancelling a Copilot prompt to steer it kills its background agents
it.effect("queues a steer until the turn ends when the provider cannot steer it", () =>
  Effect.gen(function* () {
    const { events, started, steered, interrupted, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("steering-unsupported", false, false);
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const messageId = MessageId.make("message:steer");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer"),
        threadId,
        messageId,
        text: "follow up",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "start_immediately" },
        deliveryIntent: "steer",
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();

      const held = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(steered, []);
      assert.deepEqual(interrupted, []);
      assert.equal(started.length, 1);
      assert.equal(held.runs.find((run) => run.id === first.runId)?.status, "running");
      const queued = held.runs.find((run) => run.userMessageId === messageId);
      assert.equal(queued?.status, "queued");

      const settled = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "waiting",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      const turn = held.providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();
      yield* orchestrator.resumeQueuedRuns;
      yield* worker.drain();

      assert.deepEqual(interrupted, []);
      assert.equal(started.length, 2);
      assert.equal(started[1]?.runId, queued?.id);
      assert.equal(started[1]?.message.text, "follow up");
    }).pipe(Effect.provide(layer));
  }),
);

// regression: #39404 a later agent message steers ahead of one queued behind /compact
it.effect("delivers agent messages queued behind /compact before a later one steers", () =>
  Effect.gen(function* () {
    const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "agent-order-after-compact",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const nextRunning = Effect.fn("nextRunning")(function* () {
        return yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
      });
      const lock = (yield* ThreadCommandExecutor.ThreadCommandExecutor).withLock(
        threadId,
        Effect.void,
      );
      const finishTurn = Effect.fn("finishTurn")(function* (index: number) {
        const turnInput = started[index]!;
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns.find(
          (candidate) => candidate.runAttemptId === turnInput.attemptId,
        )!;
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === turnInput.runId &&
              (event.payload.status === "waiting" || event.payload.status === "completed"),
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: turnInput.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
      });
      yield* finishTurn(0);
      const compactRunning = yield* nextRunning();
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("compact"),
        threadId,
        messageId: MessageId.make("message:compact"),
        text: "/compact",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      yield* Fiber.join(compactRunning);
      yield* lock;
      const sendFromAgent = (codeword: string) =>
        Effect.gen(function* () {
          const active = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.status === "running",
          )!;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`send-${codeword}`),
            threadId,
            messageId: MessageId.make(`message:${codeword}`),
            text: codeword,
            attachments: [],
            dispatchMode: { type: "steer_active", targetRunId: active.id },
            deliveryIntent: "auto",
            createdBy: "agent",
            creationSource: "mcp",
            senderThreadId: ThreadId.make("thread:orchestrator"),
          });
          yield* worker.drain();
        });
      yield* sendFromAgent("ALPHA");
      yield* sendFromAgent("BRAVO");
      const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
        (run) => run.status === "queued",
      );
      assert.lengthOf(queued, 2);
      assert.deepEqual(steered, []);

      const alphaRunning = yield* nextRunning();
      yield* finishTurn(1);
      yield* Fiber.join(alphaRunning);
      yield* lock;
      assert.deepEqual(
        started.map((turn) => turn.message.text.split("\n").at(-1)),
        ["first", "/compact", "ALPHA"],
      );

      yield* sendFromAgent("CHARLIE");
      const delivered = [...steered, ...started.slice(3).map((turn) => turn.message.text)]
        .map((text) => text.split("\n").at(-1))
        .filter((text) => text === "BRAVO" || text === "CHARLIE");
      // Whatever reaches the agent so far does so in send order: CHARLIE never before BRAVO.
      assert.deepEqual(delivered, ["BRAVO", "CHARLIE"].slice(0, delivered.length));
    }).pipe(Effect.provide(layer));
  }),
);

// Send order is kept per sender: a linked environment's queued message holds back only that
// sender's automatic input, and one that names no sender waits behind any agent message.
it.effect("keeps send order per linked sender, not across linked senders", () =>
  Effect.gen(function* () {
    const { steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "agent-order-linked-senders",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const linked = (name: string) => ({
        environmentId: EnvironmentId.make(`environment:${name}`),
        threadId: ThreadId.make("thread:remote"),
        title: `Remote ${name}`,
      });
      const send = (
        codeword: string,
        sender: { readonly linkedSender?: ReturnType<typeof linked> },
        automatic: boolean,
      ) =>
        Effect.gen(function* () {
          const active = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.status === "running",
          )!;
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`send-${codeword}`),
            threadId,
            messageId: MessageId.make(`message:${codeword}`),
            text: codeword,
            attachments: [],
            dispatchMode: automatic
              ? { type: "steer_active", targetRunId: active.id }
              : { type: "queue_after_active" },
            ...(automatic ? { deliveryIntent: "auto" as const } : {}),
            createdBy: "agent",
            creationSource: "mcp",
            ...sender,
          });
          yield* worker.drain();
        });
      const queuedTexts = Effect.gen(function* () {
        const projection = yield* orchestrator.getThreadProjection(threadId);
        return projection.runs
          .filter((run) => run.status === "queued")
          .map(
            (run) => projection.messages.find((message) => message.id === run.userMessageId)?.text,
          );
      });
      const steeredTexts = () => steered.map((text) => text.split("\n").at(-1));

      // Two environments' agents both name a thread with the same id; each is its own sender.
      yield* send("ALPHA", { linkedSender: linked("a") }, false);
      yield* send("BRAVO", { linkedSender: linked("b") }, true);
      assert.deepEqual(steeredTexts(), ["BRAVO"]);
      yield* send("CHARLIE", { linkedSender: linked("a") }, true);
      assert.deepEqual(steeredTexts(), ["BRAVO"]);
      assert.deepEqual(yield* queuedTexts, ["ALPHA", "CHARLIE"]);
      // A message that names no sender may be any sender's, so it waits behind them all.
      yield* send("DELTA", {}, true);
      assert.deepEqual(steeredTexts(), ["BRAVO"]);
      assert.deepEqual(yield* queuedTexts, ["ALPHA", "CHARLIE", "DELTA"]);
    }).pipe(Effect.provide(layer));
  }),
);

// regression: #33454 a steer that restarts a completion wake run left its cohort claimed forever
const restartCompletionWake = Effect.fn("restartCompletionWake")(function* (
  harness: Pick<
    Effect.Success<ReturnType<typeof nextTurnSelectionHarness>>,
    "events" | "started" | "startFirstTurn"
  >,
  siblingFinishesFirst: boolean,
  userRunBeforeWake = false,
  intentNamesTarget = false,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const sink = yield* EventSink.EventSinkV2;
  const threadId = yield* harness.startFirstTurn;
  const watchRun = (runId: string, status: string) =>
    orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === runId &&
          event.payload.status === status,
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  const watchTurnRunning = (ordinal: number) =>
    orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "provider-turn.updated" &&
          event.payload.status === "running" &&
          event.payload.ordinal === ordinal,
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  const finishTurn = (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
    Effect.gen(function* () {
      const providerTurn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns.find(
        (candidate) => candidate.runAttemptId === turn.attemptId,
      )!;
      const settled = yield* watchRun(turn.runId, "waiting");
      yield* Queue.offer(harness.events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: {
          ...providerTurn,
          status: "completed",
          completedAt: yield* DateTime.now,
        },
      });
      yield* Queue.offer(harness.events, {
        type: "turn.terminal",
        driver,
        providerThreadId: providerTurn.providerThreadId,
        providerTurnId: providerTurn.id,
        runOrdinal: turn.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();
    });

  const origin = harness.started[0]!;
  const firstTaskId = NodeId.make("task:steered-wake-first");
  const siblingTaskId = NodeId.make("task:steered-wake-sibling");
  const now = yield* DateTime.now;
  yield* sink.write({
    events: [firstTaskId, siblingTaskId].map((taskId) => ({
      id: EventId.make(`event:seed:${taskId}`),
      type: "subagent.updated" as const,
      threadId,
      runId: origin.runId,
      nodeId: taskId,
      occurredAt: now,
      payload: {
        id: taskId,
        threadId,
        runId: origin.runId,
        parentNodeId: origin.rootNodeId,
        origin: "app_owned" as const,
        createdBy: "agent" as const,
        driver,
        providerInstanceId: instanceId,
        providerThreadId: null,
        childThreadId: null,
        remoteChild: {
          environmentId: EnvironmentId.make("environment:box"),
          threadId: ThreadId.make(`thread:remote:${taskId}`),
          label: "box",
        },
        nativeTaskRef: null,
        prompt: "Do background work",
        title: null,
        model: null,
        completionWake: "always" as const,
        completionDelivery: { state: "pending" as const, observedByRunId: null },
        status: "running" as const,
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      },
    })),
  });
  yield* finishTurn(origin);
  // The wake queues behind a user run that started after the origin run finished.
  if (userRunBeforeWake) {
    const userRunning = yield* watchTurnRunning(2);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make("command:user-run"),
      threadId,
      messageId: MessageId.make("message:user-run"),
      text: "unrelated question",
      attachments: [],
      modelSelection: runSelection,
      dispatchMode: { type: "start_immediately" },
      createdBy: "user",
      creationSource: "web",
    });
    yield* worker.drain();
    yield* Fiber.join(userRunning);
  }

  const completeTask = (taskId: NodeId) =>
    orchestrator.dispatch({
      type: "delegated_task.remote.complete",
      commandId: CommandId.make(`command:remote-complete:${taskId}`),
      parentThreadId: threadId,
      taskId,
      status: "completed",
      result: `${taskId} finished`,
      stoppedThere: "ran-out",
    });
  yield* completeTask(firstTaskId);
  const firstDelivery = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
    (run) => run.id === origin.runId,
  )!.delegatedCompletion!.delivery!;
  assert.deepEqual(firstDelivery.taskIds, [firstTaskId]);
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("command:first-wake"),
    threadId,
    messageId: firstDelivery.messageId,
    text: "Delegated task completed",
    attachments: [],
    modelSelection: runSelection,
    dispatchMode: { type: "queue_after_active" },
    delegatedCompletion: {
      parentRunId: origin.runId,
      generation: firstDelivery.generation,
      taskIds: firstDelivery.taskIds,
    },
    createdBy: "agent",
    creationSource: "server",
  });
  const wakeOrdinal = userRunBeforeWake ? 3 : 2;
  const wakeRunning = yield* watchTurnRunning(wakeOrdinal);
  if (userRunBeforeWake) yield* finishTurn(harness.started[1]!);
  yield* orchestrator.resumeQueuedRuns;
  yield* worker.drain();
  yield* Fiber.join(wakeRunning);
  const wake = harness.started[wakeOrdinal - 1]!;
  assert.equal(wake.message.messageId, firstDelivery.messageId);
  if (userRunBeforeWake) {
    // The wake's run holds a compaction, so the origin run's records are older than the window.
    const compactedAt = yield* DateTime.now;
    yield* sink.write({
      events: [
        {
          id: EventId.make("event:wake-compaction"),
          type: "turn-item.updated" as const,
          threadId,
          runId: wake.runId,
          nodeId: wake.rootNodeId,
          occurredAt: compactedAt,
          payload: {
            id: TurnItemId.make("item:wake-compaction"),
            threadId,
            runId: wake.runId,
            nodeId: wake.rootNodeId,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1_000,
            status: "completed" as const,
            title: null,
            startedAt: compactedAt,
            completedAt: compactedAt,
            updatedAt: compactedAt,
            type: "compaction" as const,
            driver,
          },
        },
      ],
    });
  }

  if (siblingFinishesFirst) {
    yield* completeTask(siblingTaskId);
  }

  // The provider cannot steer natively, so the steer restarts the wake run.
  const steeredRunning = yield* watchTurnRunning(wakeOrdinal + 1);
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make("command:steer-wake"),
    threadId,
    messageId: MessageId.make("message:steer-wake"),
    text: "also check the logs",
    attachments: [],
    modelSelection: runSelection,
    ...(intentNamesTarget
      ? { dispatchMode: { type: "start_immediately" as const }, deliveryIntent: "restart" as const }
      : { dispatchMode: { type: "steer_active" as const, targetRunId: wake.runId } }),
    createdBy: "user",
    creationSource: "web",
  });
  yield* worker.drain();
  yield* Fiber.join(steeredRunning);
  const steered = harness.started[wakeOrdinal]!;
  assert.equal(steered.runId, wake.runId);
  assert.equal(
    (yield* orchestrator.getThreadProjection(threadId)).attempts.find(
      (attempt) => attempt.id === steered.attemptId,
    )?.reason,
    "steering_restart",
  );
  return {
    threadId,
    origin,
    wake,
    steered,
    firstTaskId,
    siblingTaskId,
    watchRun,
    finishTurn,
    completeTask,
  };
});

it.effect("wakes the parent for a sibling result after a steer restarts the completion wake", () =>
  Effect.gen(function* () {
    const harness = yield* nextTurnSelectionHarness("steered-completion-wake", true, false);
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const {
        threadId,
        origin,
        wake,
        steered,
        firstTaskId,
        siblingTaskId,
        watchRun,
        finishTurn,
        completeTask,
      } = yield* restartCompletionWake(harness, false);

      // A follow-up queued behind the wake starts once the terminal-run listener has settled it.
      const followUp = MessageId.make("message:follow-up");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("command:follow-up"),
        threadId,
        messageId: followUp,
        text: "follow up",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const followUpRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.userMessageId === followUp,
      )!;
      const followUpStarted = yield* watchRun(followUpRun.id, "starting");
      yield* finishTurn(steered);
      yield* Fiber.join(followUpStarted);
      assert.equal(
        (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.id === wake.runId,
        )?.status,
        "completed",
      );

      yield* completeTask(siblingTaskId);
      const final = yield* orchestrator.getThreadProjection(threadId);
      const tasks = new Map(
        final.subagents.map((task) => [task.id, task.completionDelivery?.state]),
      );
      assert.deepInclude(
        final.runs.find((run) => run.id === origin.runId)?.delegatedCompletion?.delivery ?? {},
        { generation: 2, taskIds: [siblingTaskId] },
        "the sibling finished after the steered wake but no new completion wake was reserved",
      );
      assert.equal(tasks.get(siblingTaskId), "claimed");
      assert.equal(
        tasks.get(firstTaskId),
        "delivered",
        "the steered wake run's own task stays claimed after that run completed",
      );
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect.each([false, true])(
  "settles a steered completion wake whose parent run is before the last compaction (restart by intent: %s)",
  (intentNamesTarget) =>
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness(
        `steered-wake-after-compaction-${intentNamesTarget}`,
        true,
        false,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const { threadId, steered, firstTaskId, finishTurn } = yield* restartCompletionWake(
          harness,
          false,
          true,
          intentNamesTarget,
        );
        yield* finishTurn(steered);
        const tasks = (yield* orchestrator.getThreadProjection(threadId)).subagents;
        assert.equal(
          tasks.find((task) => task.id === firstTaskId)?.completionDelivery?.state,
          "delivered",
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect(
  "offers the next completion wake when a steer restarts a wake a sibling finished behind",
  () =>
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness(
        "steered-completion-wake-behind",
        true,
        false,
      );
      const offers: ProviderContinuationRequests.ProviderContinuationRequest[] = [];
      const requests = Layer.succeed(ProviderContinuationRequests.ProviderContinuationRequests, {
        offer: (request) => Effect.sync(() => void offers.push(request)),
        take: Effect.never,
        settle: () => Effect.void,
        progress: () => Effect.succeed({ pending: 0, settles: 0 }),
        awaitSettle: () => Effect.void,
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const { threadId, origin, firstTaskId, siblingTaskId } = yield* restartCompletionWake(
          harness,
          true,
        );
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const delivery = projection.runs.find((run) => run.id === origin.runId)?.delegatedCompletion
          ?.delivery;
        assert.deepInclude(delivery ?? {}, { generation: 2, taskIds: [siblingTaskId] });
        const tasks = new Map(
          projection.subagents.map((task) => [task.id, task.completionDelivery?.state]),
        );
        assert.equal(tasks.get(firstTaskId), "delivered");
        assert.equal(tasks.get(siblingTaskId), "claimed");
        assert.deepEqual(offers.at(-1)?.delegatedCompletion, {
          parentRunId: origin.runId,
          generation: 2,
          messageId: delivery!.messageId,
        });
      }).pipe(Effect.provide(harness.layer.pipe(Layer.provide(requests))));
    }),
);

// Codex flushes held drafts after the final answer; reserve their pre-steer position before turn end.
it.effect("keeps a held commentary draft before the steer when turn end flushes it", () =>
  Effect.gen(function* () {
    const { events, started, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("steering-held-draft");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      const now = yield* DateTime.now;
      const assistantItem = (id: string, text: string, streaming: boolean) =>
        ({
          type: "turn_item.updated",
          driver,
          turnItem: {
            id: TurnItemId.make(id),
            threadId,
            runId: first.runId,
            nodeId: first.rootNodeId,
            providerThreadId: turn.providerThreadId,
            providerTurnId: turn.id,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 0,
            status: streaming ? "running" : "completed",
            title: null,
            startedAt: now,
            completedAt: streaming ? null : now,
            updatedAt: now,
            type: "assistant_message",
            messageId: MessageId.make(`message:${id}`),
            text,
            streaming,
          },
        }) satisfies ProviderAdapter.ProviderAdapterV2Event;
      const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter(predicate),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );

      yield* Queue.offer(events, assistantItem("draft", "Let me look at the", true));
      const commentaryWritten = yield* watch(
        (event) => event.type === "turn-item.updated" && event.payload.id === "commentary",
      );
      yield* Queue.offer(events, assistantItem("commentary", "Checking the popover.", false));
      yield* Fiber.join(commentaryWritten);

      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer"),
        threadId,
        messageId: MessageId.make("message:steer"),
        text: "fix the popover",
        attachments: [],
        dispatchMode: { type: "steer_active", targetRunId: first.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();

      const settled = yield* watch(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === first.runId &&
          event.payload.status === "waiting",
      );
      yield* Queue.offer(events, assistantItem("answer", "All fixed.", false));
      yield* Queue.offer(events, assistantItem("draft", "Let me look at the", false));
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();

      const items = (yield* orchestrator.getThreadProjection(threadId)).turnItems
        .toSorted((left, right) => left.ordinal - right.ordinal)
        .flatMap((item) =>
          item.type === "user_message"
            ? [String(item.messageId)]
            : item.type === "assistant_message"
              ? [String(item.id)]
              : [],
        );
      assert.deepEqual(items, ["message:first", "draft", "commentary", "message:steer", "answer"]);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(["auto", "deferred", "promote-edited"] as const)(
  "rejects expanded steering input before commit: %s",
  (delivery) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { started, steered, interrupted, layer, startFirstTurn } =
          yield* nextTurnSelectionHarness(`oversized-steer-${delivery}`, true);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const outbox = yield* EffectOutbox.EffectOutboxV2;
          const sink = yield* EventSink.EventSinkV2;
          const threadId = yield* startFirstTurn;
          const first = started[0]!;
          const citation = serializeAssistantCitation({
            version: 1,
            environmentId: EnvironmentId.make("env"),
            threadId,
            messageId: MessageId.make("assistant"),
            text: "\u0001".repeat(8_000),
            start: 0,
            end: 8_000,
            prefix: "",
            suffix: "",
          });
          const text = `${"x".repeat(80_000)}${citation}`;
          // Context excerpts are not part of the limit, matching the web composer's check.
          const largeContextText =
            "Read [one](t3-context://v1/terminal/one) and [two](t3-context://v1/terminal/two)";
          const context = {
            version: 1 as const,
            records: (["one", "two"] as const).map((id) => ({
              version: 1 as const,
              kind: "terminal" as const,
              contextId: ComposerContextId.make(id),
              label: id,
              terminalId: "terminal",
              terminalLabel: "Terminal",
              lineStart: 1,
              lineEnd: 1,
              text: "y".repeat(COMPOSER_CONTEXT_TERMINAL_TEXT_MAX_CHARS),
            })),
          };
          assert.isBelow(text.length, PROVIDER_SEND_TURN_MAX_INPUT_CHARS);
          assert.isAbove(
            expandAssistantCitationsForProvider(text).length,
            PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
          );
          assert.isAbove(
            projectComposerContextForProvider({
              text: largeContextText,
              records: context.records,
            }).length,
            PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
          );
          if (delivery === "deferred") {
            const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
            yield* sink.write({
              events: [
                {
                  id: EventId.make("turn-not-ready"),
                  type: "provider-turn.updated",
                  threadId,
                  occurredAt: yield* DateTime.now,
                  payload: { ...turn, status: "pending" },
                },
              ],
            });
          }
          const commandId = CommandId.make("oversized-steer");
          let command: Parameters<typeof orchestrator.dispatch>[0] = {
            type: "message.dispatch",
            commandId,
            threadId,
            messageId: MessageId.make("oversized"),
            text,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            deliveryIntent: delivery === "deferred" ? "steer" : "auto",
            createdBy: "user",
            creationSource: "mobile",
          };
          if (delivery === "promote-edited") {
            yield* orchestrator.dispatch({
              ...command,
              commandId: CommandId.make("queue"),
              text: "Initially valid queued message",
              dispatchMode: { type: "queue_after_active" },
              deliveryIntent: undefined,
            });
            const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
              (run) => run.status === "queued",
            )!;
            yield* orchestrator.dispatch({
              type: "queued-run.edit",
              commandId: CommandId.make("edit"),
              threadId,
              runId: queued.id,
              text,
            });
            command = {
              type: "queued-message.promote-to-steer",
              commandId,
              threadId,
              queuedRunId: queued.id,
              targetRunId: first.runId,
            };
          }
          const before = yield* orchestrator.getThreadProjection(threadId);
          const sequence = yield* orchestrator.getThreadEventSequence(threadId);
          const error = yield* orchestrator.dispatch(command).pipe(Effect.flip);
          assert.instanceOf(error, Orchestrator.OrchestratorCommandRejectedError);
          assert.equal(
            userFacingDispatchErrorMessage(error),
            "Message exceeds 120,000 characters after expanding quotes. Shorten it and try again.",
          );
          assert.deepEqual(yield* outbox.listByCommandId(commandId), []);
          assert.deepEqual(yield* sink.readByCommandId({ commandId }).pipe(Stream.runCollect), []);
          yield* worker.drain();
          assert.equal(yield* orchestrator.getThreadEventSequence(threadId), sequence);
          assert.deepEqual(yield* orchestrator.getThreadProjection(threadId), before);
          assert.deepEqual(steered, []);
          assert.deepEqual(interrupted, []);
          assert.lengthOf(started, 1);
          if (delivery === "auto") {
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make("large-context-steer"),
              threadId,
              messageId: MessageId.make("large-context"),
              text: largeContextText,
              context,
              attachments: [],
              dispatchMode: { type: "start_immediately" },
              deliveryIntent: "auto",
              createdBy: "user",
              creationSource: "mobile",
            });
            yield* worker.drain();
            assert.lengthOf(steered, 1);
          }
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("steers a promoted queued message with the selection it was queued with", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "promote-queued-selection",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queued"),
        threadId,
        messageId: MessageId.make("message:queued"),
        text: "queued",
        attachments: [],
        modelSelection: composerSelection,
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.status === "queued",
      )!;
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: queuedRun.id,
        targetRunId: started[0]!.runId,
      });
      yield* worker.drain();

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(steered, ["queued"]);
      assert.deepEqual(projection.thread.modelSelection, composerSelection);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("steers a promoted scheduled delivery with the thread's current selection", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "promote-scheduled-selection",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        // Bound scheduled tasks send like ScheduledTaskService: no model of their own.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("scheduled"),
          threadId,
          messageId: MessageId.make("message:scheduled"),
          scheduledTaskId: ScheduledTaskId.make("task:scheduled"),
          text: "scheduled",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        assert.deepEqual(queuedRun.modelSelection, runSelection);
        yield* orchestrator.dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("select-composer"),
          threadId,
          modelSelection: composerSelection,
        });
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote"),
          threadId,
          queuedRunId: queuedRun.id,
          targetRunId: started[0]!.runId,
        });
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, [
          '[T3 Code: scheduled task "task:scheduled" sent this prompt, not the user typing in this chat.]\n\nscheduled',
        ]);
        assert.deepEqual(projection.thread.modelSelection, composerSelection);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// A bound automation sends through ThreadManagementService like ScheduledTaskService does in production.
const scheduledDeliveryHarness = Effect.fn("scheduledDeliveryHarness")(function* (name: string) {
  const database = SqlitePersistence.layerMemory;
  const harness = yield* nextTurnSelectionHarness(
    name,
    false,
    true,
    Effect.void,
    true,
    undefined,
    undefined,
    undefined,
    database,
  );
  const runDueTasks = yield* Ref.make<Effect.Effect<void>>(Effect.void);
  const management = ThreadManagementService.layer.pipe(
    Layer.provide(harness.layer),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(database))),
  );
  const scheduledTasks = ScheduledTaskService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        management,
        database,
        NodeCrypto.layer,
        ManagedPolicy.layerTest(),
        // Capture the poll pass so the test drives each due interval directly.
        Layer.mock(Scheduler.Scheduler)({
          register: (job, work) =>
            job !== "scheduled-tasks"
              ? Effect.void
              : Effect.context<Effect.Services<typeof work>>().pipe(
                  Effect.flatMap((context) =>
                    Ref.set(runDueTasks, work.pipe(Effect.provideContext(context), Effect.ignore)),
                  ),
                ),
        }),
        Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
          namedProjectsRoot: "/projects",
          isScratchProject: () => Effect.succeed(false),
        }),
        Layer.mock(SecretRequests.SecretRequests)({}),
      ),
    ),
  );
  const fireDueTasks = Effect.gen(function* () {
    yield* TestClock.adjust("61 seconds");
    yield* Effect.flatten(Ref.get(runDueTasks));
  });
  const bind = (threadId: ThreadId, threadMode: "continue" | "fresh_session") =>
    Effect.flatMap(ScheduledTaskService.ScheduledTaskService, (service) =>
      service.upsert({
        id: ScheduledTaskId.make(`task:${name}`),
        title: "Check the deploy",
        prompt: "Check the deploy",
        enabled: true,
        schedule: { type: "interval", everyMs: 60_000 },
        projectId: ProjectId.make(`project:${name}`),
        threadId,
        threadMode,
        workspaceStrategy: { type: "root" },
        modelSelection: runSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
      }),
    );
  return {
    ...harness,
    layer: Layer.mergeAll(harness.layer, management, scheduledTasks),
    fireDueTasks,
    bind,
  };
});

it.effect("steers each fire of a bound automation into the thread's live turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn, fireDueTasks, bind } =
        yield* scheduledDeliveryHarness("automation-live-turn");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* bind(threadId, "continue");
        yield* fireDueTasks;
        yield* worker.drain();
        assert.deepEqual(steered.map(withoutSenderNote), ["Check the deploy"]);
        // A copy the live turn already took does not hold back the next fire.
        yield* fireDueTasks;
        yield* worker.drain();
        assert.deepEqual(steered.map(withoutSenderNote), ["Check the deploy", "Check the deploy"]);
        assert.lengthOf(started, 1);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.isFalse(after.runs.some((run) => run.status === "queued"));
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("defers a bound automation fired in a turn gap to the next turn, sending once", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, events, layer, startFirstTurn, fireDueTasks, bind } =
        yield* scheduledDeliveryHarness("automation-turn-gap");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = yield* startFirstTurn;
        const before = yield* orchestrator.getThreadProjection(threadId);
        const run = before.runs[0]!;
        const turn = before.providerTurns[0]!;
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("gap-run"),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: { ...run, status: "starting" },
            },
            {
              id: EventId.make("gap-turn"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: now,
              payload: { ...turn, status: "pending" },
            },
          ],
        });
        yield* bind(threadId, "continue");
        yield* fireDueTasks;
        const queuedCopies = Effect.map(orchestrator.getThreadProjection(threadId), ({ runs }) =>
          runs.filter((candidate) => candidate.status === "queued"),
        );
        const [deferred] = yield* queuedCopies;
        assert.equal(deferred?.steerWhenReadyRunId, run.id);
        // The deferred copy has not reached the agent yet, so the next fire is skipped.
        yield* fireDueTasks;
        assert.lengthOf(yield* queuedCopies, 1);

        const afterSequence = yield* sink.latestSequence({ threadId });
        const promoted = yield* sink.stream({ threadId, afterSequence }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === deferred!.id &&
              event.payload.status === "cancelled",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* sink.write({
          events: [
            {
              id: EventId.make("ready-run"),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: run,
            },
          ],
        });
        yield* Queue.offer(events, { type: "provider_turn.updated", driver, providerTurn: turn });
        yield* Fiber.join(promoted);
        yield* worker.drain();
        assert.deepEqual(steered.map(withoutSenderNote), ["Check the deploy"]);
        assert.lengthOf(yield* queuedCopies, 0);
        // Delivered, so the next fire goes out again.
        yield* fireDueTasks;
        yield* worker.drain();
        assert.deepEqual(steered.map(withoutSenderNote), ["Check the deploy", "Check the deploy"]);
        assert.lengthOf(started, 1);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("starts a fresh-session automation as its own turn beside a live turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { steered, layer, startFirstTurn, fireDueTasks, bind } =
        yield* scheduledDeliveryHarness("automation-fresh-session");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* bind(threadId, "fresh_session");
        yield* fireDueTasks;
        yield* fireDueTasks;
        yield* worker.drain();
        assert.deepEqual(steered, []);
        const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
          (run) => run.status === "queued",
        );
        assert.lengthOf(queued, 1);
        assert.isTrue(queued[0]?.freshContext);
        assert.isUndefined(queued[0]?.steerWhenReadyRunId);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("steers a deferred agent message on a pick made while it waited", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, steeredSelections, layer, startFirstTurn, events } =
        yield* nextTurnSelectionHarness("deferred-agent-steer-selection");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        yield* (yield* EventSink.EventSinkV2).write({
          events: [
            {
              id: EventId.make("turn-not-ready"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: yield* DateTime.now,
              payload: { ...turn, status: "pending" },
            },
          ],
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("agent-steer"),
          threadId,
          messageId: MessageId.make("message:agent-steer"),
          text: "agent steer",
          attachments: [],
          dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
          createdBy: "agent",
          creationSource: "mcp",
        });
        const deferred = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        assert.equal(deferred.steerWhenReadyRunId, started[0]!.runId);
        yield* orchestrator.dispatch({
          type: "thread.model-selection.set",
          commandId: CommandId.make("select-composer"),
          threadId,
          modelSelection: composerSelection,
        });
        const promoted = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === deferred.id &&
              event.payload.status === "cancelled",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, { type: "provider_turn.updated", driver, providerTurn: turn });
        yield* Fiber.join(promoted);
        yield* worker.drain();

        assert.lengthOf(steered, 1);
        assert.deepEqual(steeredSelections, [composerSelection]);
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(threadId)).thread.modelSelection,
          composerSelection,
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

const claudeSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-test",
};

const claudeAdapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
  instanceId: claudeSelection.instanceId,
  driver: ProviderDriverKind.make("claudeAgent"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("the Claude instance never starts a turn here"),
};

// Queues a scheduled delivery behind the Codex turn, stops that turn (Stop holds the queue), then
// switches the thread to Claude: a thread changes instance only while no run is active.
const scheduledDeliveryAfterClaudeSwitch = (name: string, text: string) =>
  Effect.gen(function* () {
    const harness = yield* nextTurnSelectionHarness(
      name,
      false,
      true,
      Effect.void,
      true,
      undefined,
      undefined,
      [claudeAdapter],
    );
    const queueAndSwitch = Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = yield* harness.startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("scheduled"),
        threadId,
        messageId: MessageId.make("message:scheduled"),
        scheduledTaskId: ScheduledTaskId.make("task:scheduled"),
        text,
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.status === "queued",
      )!;
      assert.equal(queuedRun.providerInstanceId, instanceId);
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const first = harness.started[0]!;
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("stop"),
        threadId,
        runId: first.runId,
        holdQueue: true,
      });
      yield* worker.drain();
      const stopped = (yield* orchestrator.getThreadProjection(threadId)).runs;
      assert.equal(stopped.find((run) => run.id === first.runId)?.status, "interrupted");
      const held = stopped.find((run) => run.id === queuedRun.id);
      assert.equal(held?.status, "queued");
      assert.isTrue(held?.queueHeld);
      yield* orchestrator.dispatch({
        type: "thread.model-selection.set",
        commandId: CommandId.make("select-claude"),
        threadId,
        modelSelection: claudeSelection,
      });
      return { threadId, queuedRun };
    });
    return { ...harness, queueAndSwitch };
  });

it.effect("checks an edited scheduled /goal against the thread's current provider", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { layer, queueAndSwitch } = yield* scheduledDeliveryAfterClaudeSwitch(
        "edit-scheduled-goal",
        "scheduled",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const { threadId, queuedRun } = yield* queueAndSwitch;
        const editError = yield* orchestrator
          .dispatch({
            type: "queued-run.edit",
            commandId: CommandId.make("edit"),
            threadId,
            runId: queuedRun.id,
            text: "/goal pause",
          })
          .pipe(Effect.flip);

        assert.match(String(editError.cause), /Claude goals cannot be paused or resumed/);
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).messages.find(
            (message) => message.id === queuedRun.userMessageId,
          )?.text,
          "scheduled",
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("checks a queued scheduled /goal against the thread's current provider at start", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, layer, queueAndSwitch } = yield* scheduledDeliveryAfterClaudeSwitch(
        "start-scheduled-goal",
        "/goal pause",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const { threadId, queuedRun } = yield* queueAndSwitch;
        yield* orchestrator.dispatch({
          type: "queue.resume",
          commandId: CommandId.make("resume"),
          threadId,
        });
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        const failure = projection.turnItems.find(
          (item) => item.type === "error" && item.runId === queuedRun.id,
        );
        assert.equal(started.length, 1);
        assert.equal(projection.runs.find((run) => run.id === queuedRun.id)?.status, "failed");
        assert.equal(
          failure?.type === "error" ? failure.failure.code : null,
          "queued_start_failed",
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("marks the model switch on a promoted message that restarts the turn", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "promote-restart-switch",
      true,
      false,
    );
    const otherSelection = { ...runSelection, model: "other-model" };
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("queued"),
        threadId,
        messageId: MessageId.make("message:queued"),
        text: "queued",
        attachments: [],
        modelSelection: otherSelection,
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.status === "queued",
      )!;
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: queuedRun.id,
        targetRunId: started[0]!.runId,
      });
      yield* worker.drain();

      const promoted = (yield* orchestrator.getThreadProjection(threadId)).turnItems.find(
        (item) => item.type === "user_message" && item.messageId === "message:queued",
      );
      assert.deepEqual(promoted?.type === "user_message" ? promoted.modelSwitch : undefined, {
        from: runSelection,
        to: otherSelection,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(["promoted", "settling", "settled", "resumed", "client"] as const)(
  "honors an empty-queue Stop when a message arrives (%s)",
  (flow) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { started, steered, layer, startFirstTurn } =
          yield* nextTurnSelectionHarness("steering-after-stop");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          const first = started[0]!;
          const messageId = MessageId.make("message:queued");
          const send = orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("queued"),
            threadId,
            messageId,
            text: "queued",
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "queue_after_active" },
            ...(flow === "promoted" ? {} : { deliveryIntent: "auto" as const }),
            createdBy: flow === "client" || flow === "promoted" ? "user" : "agent",
            creationSource: flow === "client" || flow === "promoted" ? "web" : "mcp",
          });
          if (flow === "promoted") yield* send;
          // The worker has not delivered the interrupt yet, so the run still reads as running.
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("stop"),
            threadId,
            runId: first.runId,
            holdQueue: true,
          });
          if (flow === "settled" || flow === "resumed") yield* worker.drain();
          if (flow === "resumed")
            yield* orchestrator.dispatch({
              type: "queue.resume",
              commandId: CommandId.make("early-resume"),
              threadId,
            });
          if (flow !== "promoted") yield* send;
          else {
            const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
              (run) => run.status === "queued",
            )!;
            const promoteError = yield* orchestrator
              .dispatch({
                type: "queued-message.promote-to-steer",
                commandId: CommandId.make("promote"),
                threadId,
                queuedRunId: queuedRun.id,
                targetRunId: first.runId,
              })
              .pipe(Effect.flip);
            assert.equal(promoteError._tag, "OrchestratorDispatchError");
          }
          yield* worker.drain();
          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(steered, []);
          if (flow !== "resumed" && flow !== "client") {
            const queued = projection.runs.find((run) => run.userMessageId === messageId);
            assert.equal(queued?.status, "queued");
            assert.isTrue(queued?.queueHeld);
            assert.lengthOf(started, 1);
          }
          yield* orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make("resume-original"),
            threadId,
          });
          yield* worker.drain();
          assert.lengthOf(
            started.filter((turn) => turn.message.messageId === messageId),
            1,
          );
        }).pipe(
          Effect.provide(
            EffectWorker.layerWithOptions({ maxAttempts: 1 }).pipe(Layer.provideMerge(layer)),
          ),
        );
      }),
    ),
);

// regression: #32891 a restart that Stop already reached is lost
it.effect.each([false, true])(
  "refuses a steer after Stop (provider turn ready: %s)",
  (providerTurnReady) =>
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("restart-after-stop");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        // The worker has not delivered the interrupt yet, so the run still reads as running.
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("stop"),
          threadId,
          runId: first.runId,
        });
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        if (!providerTurnReady) {
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("turn-not-ready"),
                type: "provider-turn.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: { ...turn, status: "pending" },
              },
            ],
          });
        }
        const steerError = yield* orchestrator
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("steer"),
            threadId,
            messageId: MessageId.make("message:steer"),
            text: "steer",
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "steer_active", targetRunId: first.runId },
            createdBy: "user",
            creationSource: "web",
          })
          .pipe(Effect.flip);
        assert.equal(steerError._tag, "OrchestratorDispatchError");
        assert.include(String(steerError.cause), "is stopping and cannot be steered");
        if (!providerTurnReady) {
          yield* (yield* EventSink.EventSinkV2).write({
            events: [
              {
                id: EventId.make("restore-ready-turn"),
                type: "provider-turn.updated",
                threadId,
                occurredAt: yield* DateTime.now,
                payload: turn,
              },
            ],
          });
        }
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("restart"),
          threadId,
          messageId: MessageId.make("message:restart"),
          text: "restart",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "restart_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();

        assert.deepEqual(steered, []);
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "restart"],
        );
      }).pipe(Effect.provide(layer));
    }),
);

// regression: #32891 a restart that arrives after the turn ended is lost
it.effect.each([
  { status: "completed", settled: true },
  { status: "completed", settled: false },
  { status: "interrupted", settled: true },
  { status: "interrupted", settled: false },
  { status: "cancelled", settled: false },
] as const)(
  "starts a restart that missed the turn ($status, run settled: $settled) as a new turn",
  ({ status, settled: runSettled }) =>
    Effect.gen(function* () {
      const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `restart-after-${status}-${runSettled ? "settled" : "unsettled"}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        const turnEnded = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.id === turn.id &&
              event.payload.status === status,
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status, completedAt: yield* DateTime.now },
        });
        yield* Fiber.join(turnEnded);
        const settleRun = Effect.gen(function* () {
          const settled = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.payload.id === first.runId &&
                event.payload.status !== "running",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* Queue.offer(events, {
            type: "turn.terminal",
            driver,
            providerThreadId: turn.providerThreadId,
            providerTurnId: turn.id,
            runOrdinal: first.runOrdinal,
            status,
            failure: null,
            threadDisposition: "reusable",
          });
          yield* Fiber.join(settled);
          yield* worker.drain();
        });
        if (runSettled) yield* settleRun;

        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("restart"),
          threadId,
          messageId: MessageId.make("message:restart"),
          text: "restart",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "restart_active", targetRunId: first.runId },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        if (!runSettled) {
          const restartLeftQueue = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "run.updated" &&
                event.payload.userMessageId === "message:restart" &&
                event.payload.status !== "queued",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* settleRun;
          yield* Fiber.join(restartLeftQueue);
          yield* worker.drain();
        }

        assert.deepEqual(steered, []);
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "restart"],
        );
      }).pipe(Effect.provide(layer));
    }),
);

// regression: #32891 a restart into a failed provider turn is lost
it.effect("queues a restart sent into a provider turn that failed before its run settled", () =>
  Effect.gen(function* () {
    const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "restart-after-failed-unsettled",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      const turnFailed = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.payload.id === turn.id &&
            event.payload.status === "failed",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "failed", completedAt: yield* DateTime.now },
      });
      yield* Fiber.join(turnFailed);

      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restart"),
        threadId,
        messageId: MessageId.make("message:restart"),
        text: "restart",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "restart_active", targetRunId: first.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(
        projection.runs.find((run) => run.userMessageId === "message:restart")?.status,
        "queued",
      );
    }).pipe(Effect.provide(layer));
  }),
);

// A worktree of the workspace's repo on a branch of its own: it shares the repo's refs.
const repoWorktree = (cwd: string, branch: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const parent = yield* fs.makeTempDirectoryScoped({ prefix: `t3-orchestrator-v2-${branch}-` });
    const worktree = path.join(parent, "worktree");
    const added = yield* spawner.exitCode(
      ChildProcess.make("git", ["worktree", "add", "-b", branch, worktree], { cwd }),
    );
    assert.equal(Number(added), 0);
    return worktree;
  });

// The next root run is scoped in the switched-to worktree before the stopped run's capture: sent
// after Stop, or queued while the run ends (branch selector). That worktree gets a scope of its own;
// the stopped run's checkpoint and the thread start stay where they were taken (once, the next run
// re-created the thread's only scope there, and the capture used it).
it.effect.each(
  (["sent after Stop", "queued while it ends"] as const).flatMap((flow) =>
    (["another repo", "a worktree of the same repo"] as const).map((switchedTo) => ({
      flow,
      switchedTo,
    })),
  ),
)(
  "a stopped run's checkpoint is its own after a worktree switch ($flow, $switchedTo)",
  ({ flow, switchedTo }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `own-workspace-${flow}-${switchedTo}`.replaceAll(" ", "-");
        const { cwd, events, started, layer, startFirstTurn } =
          yield* nextTurnSelectionHarness(name);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const other =
            switchedTo === "another repo"
              ? yield* checkpointWorkspace(`${name}-other`)
              : yield* repoWorktree(cwd, "switched");
          const threadId = yield* startFirstTurn;
          const first = started[0]!;
          yield* fs.writeFileString(path.join(cwd, "edited-by-first.txt"), "first\n");
          const switchWorktree = orchestrator.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("switch"),
            threadId,
            worktreePath: other,
          });
          const sendNext = orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("next"),
            threadId,
            messageId: MessageId.make("message:next"),
            text: "next",
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          if (flow === "sent after Stop") {
            yield* orchestrator.dispatch({
              type: "run.interrupt",
              commandId: CommandId.make("stop"),
              threadId,
              runId: first.runId,
            });
            // Delivering the interrupt stops the run and queues its checkpoint capture behind it.
            assert.isTrue(yield* worker.runOnce());
            yield* switchWorktree;
            yield* sendNext;
            yield* worker.drain();
          } else {
            yield* switchWorktree;
            yield* sendNext;
            // The session the selector stopped ends the turn, and the handed-off run completes.
            const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
            const stopped = yield* orchestrator.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "run.updated" &&
                  event.payload.id === first.runId &&
                  event.payload.status === "waiting",
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
            yield* Queue.offer(events, {
              type: "provider_turn.updated",
              driver,
              providerTurn: { ...turn, status: "interrupted", completedAt: yield* DateTime.now },
            });
            yield* Queue.offer(events, {
              type: "turn.terminal",
              driver,
              providerThreadId: turn.providerThreadId,
              providerTurnId: turn.id,
              runOrdinal: first.runOrdinal,
              status: "interrupted",
              failure: null,
              threadDisposition: "reusable",
            });
            yield* Fiber.join(stopped);
            yield* worker.drain();
            yield* orchestrator.resumeQueuedRuns;
            yield* worker.drain();
          }

          const projection = yield* orchestrator.getThreadProjection(threadId);
          assert.deepEqual(
            projection.checkpointScopes
              .toSorted((left, right) => left.ordinalWithinParent - right.ordinalWithinParent)
              .map((scope) => scope.cwd),
            [cwd, other],
          );
          const threadStart = projection.checkpoints.find(
            (checkpoint) => checkpoint.ordinalWithinScope === 0,
          );
          const stopped = projection.checkpoints.find(
            (checkpoint) => checkpoint.runId === first.runId,
          );
          // Revert to thread start stays available, and the stopped run's checkpoint is its own.
          assert.equal(threadStart?.status, "ready");
          assert.equal(stopped?.status, "ready");
          assert.deepEqual(
            stopped?.files.map((file) => file.path),
            ["edited-by-first.txt"],
          );
        }).pipe(Effect.provide(Layer.merge(layer, NodeServices.layer)));
      }),
    ),
);

it.effect("a run records its workspace when it is scoped, before its turn starts", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { cwd, layer } = yield* nextTurnSelectionHarness("workspace-when-scoped");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const threadId = ThreadId.make("thread:workspace-when-scoped");
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:workspace-when-scoped"),
          title: "Workspace when scoped",
          modelSelection: runSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("first"),
          threadId,
          messageId: MessageId.make("message:first"),
          text: "first",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        // Its turn has not started (the effect worker has not run), so Stop could still end it.
        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(projection.runs[0]?.status, "starting");
        assert.equal(projection.runs[0]?.workspaceCwd, cwd);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// The provider completes the run's turn; the run waits for its checkpoint, which the worker takes
// after beforeCapture.
const endTurnOf = (
  harness: Effect.Success<ReturnType<typeof nextTurnSelectionHarness>>,
  threadId: ThreadId,
  runId: Effect.Success<ReturnType<typeof nextTurnSelectionHarness>>["started"][number]["runId"],
  runOrdinal: number,
  beforeCapture: Effect.Effect<void> = Effect.void,
) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const projection = yield* orchestrator.getThreadProjection(threadId);
    const run = projection.runs.find((candidate) => candidate.id === runId)!;
    const turn = projection.providerTurns.find(
      (candidate) => candidate.runAttemptId === run.activeAttemptId,
    )!;
    const waiting = yield* orchestrator.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === runId &&
          event.payload.status === "waiting",
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    yield* Queue.offer(harness.events, {
      type: "provider_turn.updated",
      driver,
      providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
    });
    yield* Queue.offer(harness.events, {
      type: "turn.terminal",
      driver,
      providerThreadId: turn.providerThreadId,
      providerTurnId: turn.id,
      runOrdinal,
      status: "completed",
      failure: null,
      threadDisposition: "reusable",
    });
    yield* Fiber.join(waiting);
    yield* beforeCapture;
    yield* worker.drain();
  });

// A thread whose runs each edit a file in the worktree the thread is in when the run is sent:
// run 1 in worktree A (ended unless firstEnds is false), then switchTo and nextRun. B is a worktree
// of the same repo (one branch each, refs shared) with a file of its own committed.
const worktreeThread = (
  harness: Effect.Success<ReturnType<typeof nextTurnSelectionHarness>>,
  options: { readonly firstEnds?: boolean } = {},
) =>
  Effect.gen(function* () {
    const { cwd, started, startFirstTurn } = harness;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const git = (dir: string, ...args: ReadonlyArray<string>) =>
      spawner
        .exitCode(
          ChildProcess.make(
            "git",
            ["-c", "user.name=T3", "-c", "user.email=t3@example.com", ...args],
            { cwd: dir },
          ),
        )
        .pipe(Effect.map(Number));
    const other = yield* repoWorktree(cwd, "switched");
    yield* fs.writeFileString(path.join(other, "only-on-b.txt"), "b\n");
    assert.equal(yield* git(other, "add", "only-on-b.txt"), 0);
    assert.equal(yield* git(other, "commit", "-qm", "only on b"), 0);
    const endTurn = (runId: (typeof started)[number]["runId"], runOrdinal: number) =>
      endTurnOf(harness, threadId, runId, runOrdinal);

    const threadId = yield* startFirstTurn;
    const first = started[0]!;
    yield* fs.writeFileString(path.join(cwd, "edited-by-first.txt"), "first\n");
    if (options.firstEnds !== false) yield* endTurn(first.runId, first.runOrdinal);
    let switches = 0;
    const switchTo = (worktreePath: string) =>
      orchestrator.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make(`switch:${switches++}`),
        threadId,
        worktreePath,
      });
    const nextRun = (dir: string, file: string, beforeStart: Effect.Effect<void> = Effect.void) =>
      Effect.gen(function* () {
        const index = started.length;
        const earlier = new Set(started.map((turn) => turn.attemptId));
        const running = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.status === "running" &&
              event.payload.runAttemptId !== null &&
              !earlier.has(event.payload.runAttemptId),
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`next:${index}`),
          threadId,
          messageId: MessageId.make(`message:next:${index}`),
          text: `next ${index}`,
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* beforeStart;
        yield* worker.drain();
        yield* Fiber.join(running);
        const run = started[index]!;
        yield* fs.writeFileString(path.join(dir, file), `${file}\n`);
        yield* endTurn(run.runId, run.runOrdinal);
        return run;
      });
    const checkpointOf = (runId: (typeof started)[number]["runId"]) =>
      orchestrator
        .getThreadProjection(threadId)
        .pipe(
          Effect.map((projection) =>
            projection.checkpoints.find((checkpoint) => checkpoint.runId === runId)!,
          ),
        );
    return { threadId, other, first, switchTo, nextRun, endTurn, checkpointOf, git };
  });

// Run 1 in A, then run 2 in B.
const switchedWorktreeThread = (
  harness: Effect.Success<ReturnType<typeof nextTurnSelectionHarness>>,
) =>
  Effect.gen(function* () {
    const thread = yield* worktreeThread(harness);
    yield* thread.switchTo(thread.other);
    const second = yield* thread.nextRun(thread.other, "edited-by-second.txt");
    return { ...thread, second };
  });

it.effect("the run after a worktree switch lists only its own changes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness("own-changes-after-switch");
      yield* Effect.gen(function* () {
        const { checkpointOf, second } = yield* switchedWorktreeThread(harness);
        assert.deepEqual(
          (yield* checkpointOf(second.runId)).files.map((file) => file.path),
          ["edited-by-second.txt"],
        );
      }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
    }),
  ),
);

it.effect("a file revert to a checkpoint taken in another worktree is refused", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness("revert-across-switch");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const { threadId, other, checkpointOf, first } = yield* switchedWorktreeThread(harness);
        const target = yield* checkpointOf(first.runId);
        const refused = yield* orchestrator
          .dispatch({
            type: "checkpoint.rollback",
            commandId: CommandId.make("revert-files"),
            threadId,
            scopeId: target.scopeId,
            checkpointId: target.id,
            restoreFiles: true,
          })
          .pipe(Effect.flip);
        assert.include(String(refused.cause), "another worktree");
        // Worktree B keeps its own branch's file: run 1's tree is not restored over it.
        assert.isTrue(yield* fs.exists(path.join(other, "only-on-b.txt")));
      }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
    }),
  ),
);

// A -> B -> A, with A changed outside T3 while the thread is in B: A keeps the thread's existing
// scope id, B gets its own, and run 3 diffs against A's state when it starts, in A's scope.
it.effect(
  "a thread back in its first worktree reuses its scope; the run lists only its own changes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness("back-to-first-worktree");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* switchedWorktreeThread(harness);
          yield* fs.writeFileString(path.join(harness.cwd, "outside.txt"), "outside\n");
          yield* thread.switchTo(harness.cwd);
          const third = yield* thread.nextRun(harness.cwd, "edited-by-third.txt");
          const [first, second, last] = yield* Effect.forEach(
            [thread.first, thread.second, third],
            (run) => thread.checkpointOf(run.runId),
          );
          const rootScopeId = yield* IdAllocator.IdAllocatorV2.pipe(
            Effect.flatMap((ids) =>
              ids.allocate.checkpointScope({ threadId: thread.threadId, name: "root" }),
            ),
            Effect.provide(IdAllocator.layer),
          );
          assert.equal(first!.scopeId, rootScopeId);
          assert.notEqual(second!.scopeId, rootScopeId);
          assert.equal(last!.scopeId, rootScopeId);
          assert.deepEqual(
            last!.files.map((file) => file.path),
            ["edited-by-third.txt"],
          );
          const projection = yield* orchestrator.getThreadProjection(thread.threadId);
          const parent = projection.checkpoints.find(
            (checkpoint) => checkpoint.id === last!.parentCheckpointId,
          );
          assert.deepEqual([parent?.scopeId, parent?.status], [rootScopeId, "ready"]);
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// The client's thread-start target is the last ready ordinal-0 baseline (findRevertCheckpoint).
it.effect(
  "after a worktree switch, thread start is the first worktree's: files refused, conversation rewound",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness("thread-start-after-switch");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* switchedWorktreeThread(harness);
          const firstScopeId = (yield* thread.checkpointOf(thread.first.runId)).scopeId;
          const projection = yield* orchestrator.getThreadProjection(thread.threadId);
          const starts = projection.checkpoints.filter(
            (checkpoint) =>
              checkpoint.ordinalWithinScope === 0 && checkpoint.appRunOrdinal === null,
          );
          assert.deepEqual(
            starts.map((checkpoint) => [checkpoint.scopeId, checkpoint.status]),
            [[firstScopeId, "ready"]],
          );
          const start = starts[0]!;
          const rollback = (commandId: string, restoreFiles: boolean) =>
            orchestrator.dispatch({
              type: "checkpoint.rollback",
              commandId: CommandId.make(commandId),
              threadId: thread.threadId,
              scopeId: start.scopeId,
              checkpointId: start.id,
              restoreFiles,
            });
          const refused = yield* rollback("revert-files", true).pipe(Effect.flip);
          assert.include(String(refused.cause), "another worktree");
          yield* rollback("revert-conversation", false);
          yield* worker.drain();
          const after = yield* orchestrator.getThreadProjection(thread.threadId);
          assert.deepEqual(
            after.runs.map((run) => run.status),
            ["rolled_back", "rolled_back"],
          );
          assert.isNull(after.thread.rollbackFailure);
          assert.isTrue(yield* fs.exists(path.join(thread.other, "only-on-b.txt")));
          assert.isTrue(yield* fs.exists(path.join(thread.other, "edited-by-second.txt")));
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// A (1) -> B (2) -> A (3), then a conversation rewind to run 1: what came after it is stale in both
// workspaces' scopes, and its refs are gone, so later baselines are taken afresh.
it.effect("a rollback marks stale the later checkpoints of every workspace scope", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness("rollback-two-scopes");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const thread = yield* switchedWorktreeThread(harness);
        yield* thread.switchTo(harness.cwd);
        yield* thread.nextRun(harness.cwd, "edited-by-third.txt");
        const target = yield* thread.checkpointOf(thread.first.runId);
        const before = yield* orchestrator.getThreadProjection(thread.threadId);
        const later = before.checkpoints.filter(
          (checkpoint) => checkpoint.status === "ready" && checkpoint.ordinalWithinScope > 1,
        );
        assert.equal(new Set(later.map((checkpoint) => checkpoint.scopeId)).size, 2);
        yield* orchestrator.dispatch({
          type: "checkpoint.rollback",
          commandId: CommandId.make("revert-conversation"),
          threadId: thread.threadId,
          scopeId: target.scopeId,
          checkpointId: target.id,
          restoreFiles: false,
        });
        yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(thread.threadId);
        assert.deepEqual(
          later.map(
            (checkpoint) =>
              after.checkpoints.find((candidate) => candidate.id === checkpoint.id)?.status,
          ),
          later.map(() => "stale" as const),
        );
        for (const checkpoint of later) {
          assert.notEqual(
            yield* thread.git(harness.cwd, "rev-parse", "--verify", "--quiet", checkpoint.ref),
            0,
          );
        }
      }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
    }),
  ),
);

// Prompts queued before and after a worktree switch both start in the worktree the thread is in
// then (B); each is checkpointed there, not in the worktree it was queued in.
it.effect("prompts queued around a worktree switch are checkpointed where they start", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness("queued-around-switch");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const { started } = harness;
        const thread = yield* worktreeThread(harness, { firstEnds: false });
        const queue = (text: string) =>
          orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(text),
            threadId: thread.threadId,
            messageId: MessageId.make(`message:${text}`),
            text,
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
        yield* queue("queued-in-a");
        yield* thread.switchTo(thread.other);
        yield* queue("queued-in-b");
        const queued: Array<(typeof started)[number]> = [];
        let ending = thread.first;
        for (const file of ["edited-by-queued-in-a.txt", "edited-by-queued-in-b.txt"]) {
          const index = started.length;
          const earlier = new Set(started.map((turn) => turn.attemptId));
          const running = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.status === "running" &&
                event.payload.runAttemptId !== null &&
                !earlier.has(event.payload.runAttemptId),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* thread.endTurn(ending.runId, ending.runOrdinal);
          yield* orchestrator.resumeQueuedRuns;
          yield* worker.drain();
          yield* Fiber.join(running);
          ending = started[index]!;
          queued.push(ending);
          yield* fs.writeFileString(path.join(thread.other, file), `${file}\n`);
        }
        yield* thread.endTurn(ending.runId, ending.runOrdinal);
        const firstScopeId = (yield* thread.checkpointOf(thread.first.runId)).scopeId;
        const checkpoints = yield* Effect.forEach(queued, (run) => thread.checkpointOf(run.runId));
        assert.deepEqual(
          checkpoints.map((checkpoint) => checkpoint.files.map((file) => file.path)),
          [["edited-by-queued-in-a.txt"], ["edited-by-queued-in-b.txt"]],
        );
        assert.equal(checkpoints[0]!.scopeId, checkpoints[1]!.scopeId);
        assert.notEqual(checkpoints[0]!.scopeId, firstScopeId);
      }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
    }),
  ),
);

// Run 1 in A, runs 2 and 3 in B; back in A, B is removed, then a rewind to run 1. B's stale refs
// are dropped through A (one repository), so B recreated at the same path takes a fresh baseline.
it.effect("a rewind drops a removed worktree's stale refs; recreated, it diffs afresh", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness("rewind-removed-worktree");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const thread = yield* switchedWorktreeThread(harness);
        yield* thread.nextRun(thread.other, "edited-by-third.txt");
        yield* thread.switchTo(harness.cwd);
        assert.equal(
          yield* thread.git(harness.cwd, "worktree", "remove", "--force", thread.other),
          0,
        );
        const target = yield* thread.checkpointOf(thread.first.runId);
        const laterInB = (yield* orchestrator.getThreadProjection(
          thread.threadId,
        )).checkpoints.filter(
          (checkpoint) =>
            checkpoint.status === "ready" &&
            checkpoint.scopeId !== target.scopeId &&
            checkpoint.ordinalWithinScope > 1,
        );
        assert.lengthOf(laterInB, 2);
        yield* orchestrator.dispatch({
          type: "checkpoint.rollback",
          commandId: CommandId.make("revert-conversation"),
          threadId: thread.threadId,
          scopeId: target.scopeId,
          checkpointId: target.id,
          restoreFiles: false,
        });
        yield* worker.drain();
        for (const checkpoint of laterInB) {
          assert.notEqual(
            yield* thread.git(harness.cwd, "rev-parse", "--verify", "--quiet", checkpoint.ref),
            0,
          );
        }
        assert.equal(
          yield* thread.git(harness.cwd, "worktree", "add", thread.other, "switched"),
          0,
        );
        yield* thread.switchTo(thread.other);
        const fourth = yield* thread.nextRun(thread.other, "edited-by-fourth.txt");
        assert.deepEqual(
          (yield* thread.checkpointOf(fourth.runId)).files.map((file) => file.path),
          ["edited-by-fourth.txt"],
        );
      }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
    }),
  ),
);

// Before the upgrade a thread had one root scope, re-created in each run's workspace, and its runs
// recorded no workspace. This drops the runs' recorded workspaces, as old runs never had one.
const recordedBeforeUpgrade = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE orchestration_v2_projection_runs
      SET payload_json = json_remove(payload_json, '$.workspaceCwd') WHERE thread_id = ${threadId}`;
  });

// Run 1 in A; then, before the upgrade, the thread moved to B (the old dispatch re-created the one
// root scope there). Which worktree its unrecorded checkpoints hold is unknown: no file restore.
it.effect(
  "a thread that switched worktree before the upgrade restores no files it cannot place; the conversation rewinds",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const database = SqlitePersistence.layerMemory;
        const harness = yield* nextTurnSelectionHarness(
          "switched-before-upgrade",
          false,
          true,
          Effect.void,
          true,
          undefined,
          undefined,
          undefined,
          database,
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sink = yield* EventSink.EventSinkV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* worktreeThread(harness);
          const root = (yield* orchestrator.getThreadProjection(thread.threadId))
            .checkpointScopes[0]!;
          yield* sink.write({
            events: [
              {
                id: EventId.make("legacy-root-scope-in-b"),
                type: "checkpoint-scope.created",
                threadId: thread.threadId,
                providerInstanceId: instanceId,
                occurredAt: yield* DateTime.now,
                payload: { ...root, cwd: thread.other },
              },
            ],
          });
          yield* recordedBeforeUpgrade(thread.threadId);
          yield* thread.switchTo(thread.other);
          const projection = yield* orchestrator.getThreadProjection(thread.threadId);
          const threadStart = projection.checkpoints.find(
            (checkpoint) =>
              checkpoint.ordinalWithinScope === 0 && checkpoint.appRunOrdinal === null,
          )!;
          const run1 = yield* thread.checkpointOf(thread.first.runId);
          const rollback = (commandId: string, target: typeof run1, restoreFiles: boolean) =>
            orchestrator.dispatch({
              type: "checkpoint.rollback",
              commandId: CommandId.make(commandId),
              threadId: thread.threadId,
              scopeId: target.scopeId,
              checkpointId: target.id,
              restoreFiles,
            });
          for (const [commandId, target] of [
            ["start-files", threadStart],
            ["run-1-files", run1],
          ] as const) {
            const refused = yield* rollback(commandId, target, true).pipe(Effect.flip);
            assert.include(String(refused.cause), "can't tell which worktree");
          }
          assert.isTrue(yield* fs.exists(path.join(thread.other, "only-on-b.txt")));
          yield* rollback("start-conversation", threadStart, false);
          yield* worker.drain();
          const after = yield* orchestrator.getThreadProjection(thread.threadId);
          assert.deepEqual(
            after.runs.map((run) => run.status),
            ["rolled_back"],
          );
          assert.isNull(after.thread.rollbackFailure);
        }).pipe(Effect.provide(Layer.mergeAll(harness.layer, NodeServices.layer, database)));
      }),
    ),
);

// Run 1 recorded its workspace (A), but the thread's one root scope was last re-created in B before
// the upgrade; back in A, run 2 gets a scope of its own. A file revert to run 1 restores A, where run
// 1's checkpoint was taken, not the B its scope names.
it.effect(
  "a checkpoint whose run recorded its workspace restores there, whatever its old scope names",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness("recorded-run-old-scope");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sink = yield* EventSink.EventSinkV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* worktreeThread(harness);
          const root = (yield* orchestrator.getThreadProjection(thread.threadId))
            .checkpointScopes[0]!;
          yield* sink.write({
            events: [
              {
                id: EventId.make("old-root-scope-in-b"),
                type: "checkpoint-scope.created",
                threadId: thread.threadId,
                providerInstanceId: instanceId,
                occurredAt: yield* DateTime.now,
                payload: { ...root, cwd: thread.other },
              },
            ],
          });
          yield* thread.nextRun(harness.cwd, "edited-by-second.txt");
          yield* fs.writeFileString(path.join(harness.cwd, "after.txt"), "later\n");
          const run1 = yield* thread.checkpointOf(thread.first.runId);
          yield* orchestrator.dispatch({
            type: "checkpoint.rollback",
            commandId: CommandId.make("run-1-files"),
            threadId: thread.threadId,
            scopeId: run1.scopeId,
            checkpointId: run1.id,
            restoreFiles: true,
          });
          yield* worker.drain();
          assert.isNull(
            (yield* orchestrator.getThreadProjection(thread.threadId)).thread.rollbackFailure,
          );
          assert.isTrue(yield* fs.exists(path.join(harness.cwd, "edited-by-first.txt")));
          assert.isFalse(yield* fs.exists(path.join(harness.cwd, "edited-by-second.txt")));
          assert.isFalse(yield* fs.exists(path.join(harness.cwd, "after.txt")));
          assert.isTrue(yield* fs.exists(path.join(thread.other, "only-on-b.txt")));
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// The control: recorded before the upgrade, but only ever in A, so its checkpoints are A's.
it.effect("a thread recorded before the upgrade that never switched still restores its files", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const database = SqlitePersistence.layerMemory;
      const harness = yield* nextTurnSelectionHarness(
        "never-switched-before-upgrade",
        false,
        true,
        Effect.void,
        true,
        undefined,
        undefined,
        undefined,
        database,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const thread = yield* worktreeThread(harness);
        yield* recordedBeforeUpgrade(thread.threadId);
        yield* fs.writeFileString(path.join(harness.cwd, "after-run-1.txt"), "later\n");
        const run1 = yield* thread.checkpointOf(thread.first.runId);
        yield* orchestrator.dispatch({
          type: "checkpoint.rollback",
          commandId: CommandId.make("run-1-files"),
          threadId: thread.threadId,
          scopeId: run1.scopeId,
          checkpointId: run1.id,
          restoreFiles: true,
        });
        yield* worker.drain();
        assert.isTrue(yield* fs.exists(path.join(harness.cwd, "edited-by-first.txt")));
        assert.isFalse(yield* fs.exists(path.join(harness.cwd, "after-run-1.txt")));
      }).pipe(Effect.provide(Layer.mergeAll(harness.layer, NodeServices.layer, database)));
    }),
  ),
);

// Run 1 is sent (and scoped) in A; the thread moves to B before its turn starts, with no provider
// session open, so the turn opens one in B: the run is checkpointed in B, where it works.
it.effect("a run whose turn starts after the thread moved is checkpointed where it works", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness("moved-before-turn-start");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const other = yield* repoWorktree(harness.cwd, "switched");
        const threadId = ThreadId.make("thread:moved-before-turn-start");
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:moved-before-turn-start"),
          title: "Moved before its turn starts",
          modelSelection: runSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: harness.cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const running = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("first"),
          threadId,
          messageId: MessageId.make("message:first"),
          text: "first",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("switch"),
          threadId,
          worktreePath: other,
        });
        yield* worker.drain();
        yield* Fiber.join(running);
        const first = harness.started[0]!;
        yield* fs.writeFileString(path.join(other, "edited-in-b.txt"), "b\n");
        yield* endTurnOf(harness, threadId, first.runId, first.runOrdinal);
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const checkpoint = projection.checkpoints.find((c) => c.runId === first.runId)!;
        assert.deepEqual(
          checkpoint.files.map((file) => file.path),
          ["edited-in-b.txt"],
        );
        assert.equal(
          projection.checkpointScopes.find((scope) => scope.id === checkpoint.scopeId)?.cwd,
          other,
        );
      }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
    }),
  ),
);

// Run 2 is sent (and scoped) in A while run 1's session is live; the thread moves to B before run
// 2's turn starts. A shared session (Codex) resumes the thread and starts the turn in the turn's
// cwd, B, though its process lives on: the run is checkpointed in B.
it.effect(
  "a run whose turn starts in a live shared session after the thread moved is checkpointed where it works",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness("moved-before-live-turn-start");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* worktreeThread(harness);
          const earlier = new Set(harness.started.map((turn) => turn.attemptId));
          const running = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.status === "running" &&
                event.payload.runAttemptId !== null &&
                !earlier.has(event.payload.runAttemptId),
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("second"),
            threadId: thread.threadId,
            messageId: MessageId.make("message:second"),
            text: "second",
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* thread.switchTo(thread.other);
          yield* worker.drain();
          yield* Fiber.join(running);
          const second = harness.started[1]!;
          yield* fs.writeFileString(path.join(thread.other, "edited-in-b.txt"), "b\n");
          yield* endTurnOf(harness, thread.threadId, second.runId, second.runOrdinal);
          const projection = yield* orchestrator.getThreadProjection(thread.threadId);
          const checkpoint = projection.checkpoints.find((c) => c.runId === second.runId)!;
          assert.deepEqual(
            checkpoint.files.map((file) => file.path),
            ["edited-in-b.txt"],
          );
          assert.equal(
            projection.checkpointScopes.find((scope) => scope.id === checkpoint.scopeId)?.cwd,
            thread.other,
          );
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// The same worktree under another spelling (a symlink here; on macOS, /var is /private/var).
it.effect("the same worktree under another path spelling keeps its scope", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness("worktree-other-spelling");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const thread = yield* worktreeThread(harness);
        const link = path.join(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-other-spelling-" }),
          "link",
        );
        yield* fs.symlink(harness.cwd, link);
        yield* thread.switchTo(link);
        const second = yield* thread.nextRun(link, "edited-by-second.txt");
        const [first, last] = yield* Effect.forEach([thread.first, second], (run) =>
          thread.checkpointOf(run.runId),
        );
        const scopes = (yield* orchestrator.getThreadProjection(thread.threadId)).checkpointScopes;
        assert.lengthOf(scopes, 1);
        // The scope keeps the spelling it was created with.
        assert.equal(scopes[0]!.cwd, harness.cwd);
        assert.equal(last!.scopeId, first!.scopeId);
        assert.deepEqual(
          last!.files.map((file) => file.path),
          ["edited-by-second.txt"],
        );
      }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
    }),
  ),
);

// B is first reached through a symlink, so its scope keeps that spelling; the thread returns to B by
// its real path after the symlink is gone, or points elsewhere. Run 4 works in B's real path, so its
// baseline, checkpoint and a file revert to it must use that path, not the scope's spelling.
it.effect.each(["removed", "retargeted"] as const)(
  "a run back in a workspace whose first spelling was a symlink, now %s, is checkpointed and restored where it runs",
  (symlink) =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness(`symlink-${symlink}`);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* worktreeThread(harness);
          const link = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: `t3-symlink-${symlink}-` }),
            "link",
          );
          yield* fs.symlink(thread.other, link);
          yield* thread.switchTo(link);
          yield* thread.nextRun(link, "edited-by-second.txt");
          yield* thread.switchTo(harness.cwd);
          yield* thread.nextRun(harness.cwd, "edited-by-third.txt");
          yield* fs.remove(link);
          const elsewhere = yield* checkpointWorkspace(`symlink-${symlink}-elsewhere`);
          if (symlink === "retargeted") yield* fs.symlink(elsewhere, link);
          yield* thread.switchTo(thread.other);
          const fourth = yield* thread.nextRun(thread.other, "edited-by-fourth.txt");
          const checkpoint = yield* thread.checkpointOf(fourth.runId);
          assert.deepEqual(
            [checkpoint.status, checkpoint.files.map((file) => file.path)],
            ["ready", ["edited-by-fourth.txt"]],
          );
          yield* fs.writeFileString(path.join(thread.other, "after.txt"), "later\n");
          yield* orchestrator.dispatch({
            type: "checkpoint.rollback",
            commandId: CommandId.make("run-4-files"),
            threadId: thread.threadId,
            scopeId: checkpoint.scopeId,
            checkpointId: checkpoint.id,
            restoreFiles: true,
          });
          yield* worker.drain();
          assert.isNull(
            (yield* orchestrator.getThreadProjection(thread.threadId)).thread.rollbackFailure,
          );
          assert.isTrue(yield* fs.exists(path.join(thread.other, "edited-by-fourth.txt")));
          assert.isFalse(yield* fs.exists(path.join(thread.other, "after.txt")));
          assert.isFalse(yield* fs.exists(path.join(elsewhere, "edited-by-fourth.txt")));
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// Run 3 works in B through a symlink; back in A, the symlink is pointed at A. Run 3's checkpoint
// was taken in B, so a file revert to it from A is refused, though its recorded spelling names A now.
it.effect(
  "a file revert to a run that worked through a symlink, since pointed at this worktree, is refused",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness("symlink-pointed-here");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* switchedWorktreeThread(harness);
          const link = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: "t3-symlink-pointed-here-" }),
            "link",
          );
          yield* fs.symlink(thread.other, link);
          yield* thread.switchTo(link);
          const third = yield* thread.nextRun(link, "edited-by-third.txt");
          yield* thread.switchTo(harness.cwd);
          yield* fs.remove(link);
          yield* fs.symlink(harness.cwd, link);
          const checkpoint = yield* thread.checkpointOf(third.runId);
          const refused = yield* orchestrator
            .dispatch({
              type: "checkpoint.rollback",
              commandId: CommandId.make("run-3-files"),
              threadId: thread.threadId,
              scopeId: checkpoint.scopeId,
              checkpointId: checkpoint.id,
              restoreFiles: true,
            })
            .pipe(Effect.flip);
          assert.include(String(refused.cause), "another worktree");
          assert.isFalse(yield* fs.exists(path.join(harness.cwd, "only-on-b.txt")));
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// Run 2 works in B through a symlinked folder, and its checkpoint records B's real path; storage
// cleanup then removes B. A file revert to run 2 is admitted while B is gone, and restores run 2's
// files once B is checked out there again (rollback recreates a managed worktree the same way).
it.effect(
  "a file revert into a removed worktree reached through a symlinked folder is admitted",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness("removed-worktree-symlinked-folder");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* worktreeThread(harness);
          const folder = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: "t3-removed-worktree-folder-" }),
            "link",
          );
          yield* fs.symlink(path.dirname(thread.other), folder);
          const worktreePath = path.join(folder, path.basename(thread.other));
          yield* thread.switchTo(worktreePath);
          const second = yield* thread.nextRun(worktreePath, "edited-by-second.txt");
          const checkpoint = yield* thread.checkpointOf(second.runId);
          assert.equal(
            yield* thread.git(harness.cwd, "worktree", "remove", "--force", thread.other),
            0,
          );
          yield* orchestrator.dispatch({
            type: "checkpoint.rollback",
            commandId: CommandId.make("run-2-files"),
            threadId: thread.threadId,
            scopeId: checkpoint.scopeId,
            checkpointId: checkpoint.id,
            restoreFiles: true,
          });
          assert.equal(
            yield* thread.git(harness.cwd, "worktree", "add", worktreePath, "switched"),
            0,
          );
          assert.isFalse(yield* fs.exists(path.join(worktreePath, "edited-by-second.txt")));
          yield* worker.drain();
          assert.isNull(
            (yield* orchestrator.getThreadProjection(thread.threadId)).thread.rollbackFailure,
          );
          assert.isTrue(yield* fs.exists(path.join(worktreePath, "edited-by-second.txt")));
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// Run 2 is sent to B through a symlinked folder after storage cleanup removed B, which is checked out
// there again before the turn starts. Its scope records B's real path, so once the folder is pointed
// at another worktree of the repo under the same name, a file revert to run 2 is refused there.
it.effect(
  "a run sent to a removed worktree through a symlinked folder is not restored into another worktree",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness(
          "sent-to-removed-worktree-symlinked-folder",
        );
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* worktreeThread(harness);
          const folder = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: "t3-sent-removed-folder-" }),
            "link",
          );
          yield* fs.symlink(path.dirname(thread.other), folder);
          const worktreePath = path.join(folder, path.basename(thread.other));
          yield* thread.switchTo(worktreePath);
          assert.equal(
            yield* thread.git(harness.cwd, "worktree", "remove", "--force", thread.other),
            0,
          );
          const second = yield* thread.nextRun(
            worktreePath,
            "edited-by-second.txt",
            thread.git(harness.cwd, "worktree", "add", worktreePath, "switched").pipe(
              Effect.orDie,
              Effect.map((code) => assert.equal(code, 0)),
            ),
          );
          const checkpoint = yield* thread.checkpointOf(second.runId);
          assert.deepEqual(
            [checkpoint.status, checkpoint.files.map((file) => file.path)],
            ["ready", ["edited-by-second.txt"]],
          );
          const elsewhere = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sent-removed-other-" });
          const sameName = path.join(elsewhere, path.basename(thread.other));
          assert.equal(
            yield* thread.git(harness.cwd, "worktree", "add", "-b", "other", sameName),
            0,
          );
          yield* fs.remove(folder);
          yield* fs.symlink(elsewhere, folder);
          const revert = yield* orchestrator
            .dispatch({
              type: "checkpoint.rollback",
              commandId: CommandId.make("run-2-files"),
              threadId: thread.threadId,
              scopeId: checkpoint.scopeId,
              checkpointId: checkpoint.id,
              restoreFiles: true,
            })
            .pipe(Effect.result);
          yield* worker.drain();
          assert.deepEqual(
            {
              refused:
                revert._tag === "Failure" &&
                String(revert.failure.cause).includes("another worktree"),
              overwritten: yield* fs.exists(path.join(sameName, "edited-by-second.txt")),
            },
            { refused: true, overwritten: false },
          );
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// The thread starts in A through a symlink; the symlink is then pointed at B and the thread moves to
// B's real path. B is another workspace: run 2 gets its own scope and lists only its own file.
it.effect(
  "a thread whose first workspace was a symlink, since pointed elsewhere, keeps the workspaces apart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness("first-workspace-symlink");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const other = yield* repoWorktree(harness.cwd, "switched");
          const link = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: "t3-first-workspace-symlink-" }),
            "link",
          );
          yield* fs.symlink(harness.cwd, link);
          const threadId = ThreadId.make("thread:first-workspace-symlink");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create"),
            threadId,
            projectId: ProjectId.make("project:first-workspace-symlink"),
            title: "First workspace through a symlink",
            modelSelection: runSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: link,
            createdBy: "user",
            creationSource: "web",
          });
          const startRun = (index: number, text: string) =>
            Effect.gen(function* () {
              const earlier = new Set(harness.started.map((turn) => turn.attemptId));
              const running = yield* orchestrator.streamDomainEvents.pipe(
                Stream.filter(
                  (event) =>
                    event.type === "provider-turn.updated" &&
                    event.payload.status === "running" &&
                    event.payload.runAttemptId !== null &&
                    !earlier.has(event.payload.runAttemptId),
                ),
                Stream.take(1),
                Stream.runDrain,
                Effect.forkScoped,
              );
              yield* orchestrator.dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(text),
                threadId,
                messageId: MessageId.make(`message:${text}`),
                text,
                attachments: [],
                modelSelection: runSelection,
                dispatchMode: { type: "start_immediately" },
                createdBy: "user",
                creationSource: "web",
              });
              yield* worker.drain();
              yield* Fiber.join(running);
              return harness.started[index]!;
            });
          const first = yield* startRun(0, "first");
          yield* fs.writeFileString(path.join(harness.cwd, "edited-by-first.txt"), "first\n");
          yield* endTurnOf(harness, threadId, first.runId, first.runOrdinal);
          yield* fs.remove(link);
          yield* fs.symlink(other, link);
          yield* orchestrator.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make("switch"),
            threadId,
            worktreePath: other,
          });
          const second = yield* startRun(1, "second");
          yield* fs.writeFileString(path.join(other, "edited-by-second.txt"), "second\n");
          yield* endTurnOf(harness, threadId, second.runId, second.runOrdinal);
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const checkpointOf = (runId: typeof first.runId) =>
            projection.checkpoints.find((checkpoint) => checkpoint.runId === runId)!;
          assert.notEqual(checkpointOf(second.runId).scopeId, checkpointOf(first.runId).scopeId);
          assert.deepEqual(
            checkpointOf(second.runId).files.map((file) => file.path),
            ["edited-by-second.txt"],
          );
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// Run 1 works in A through a symlink; after its turn ends and before its checkpoint is taken, the
// symlink is pointed at B, a worktree of the same repo. The checkpoint is A's, and the run keeps A.
it.effect(
  "a run's checkpoint is taken where it ran, though its symlink is pointed elsewhere before the capture",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness("symlink-retargeted-before-capture");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const other = yield* repoWorktree(harness.cwd, "switched");
          yield* fs.writeFileString(path.join(other, "only-on-b.txt"), "b\n");
          const link = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: "t3-retargeted-before-capture-" }),
            "link",
          );
          yield* fs.symlink(harness.cwd, link);
          const threadId = ThreadId.make("thread:symlink-retargeted-before-capture");
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make("create"),
            threadId,
            projectId: ProjectId.make("project:symlink-retargeted-before-capture"),
            title: "Symlink retargeted before the capture",
            modelSelection: runSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: link,
            createdBy: "user",
            creationSource: "web",
          });
          const running = yield* orchestrator.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "provider-turn.updated" &&
                event.payload.status === "running" &&
                event.payload.runAttemptId !== null,
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("first"),
            threadId,
            messageId: MessageId.make("message:first"),
            text: "first",
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          yield* worker.drain();
          yield* Fiber.join(running);
          const first = harness.started[0]!;
          yield* fs.writeFileString(path.join(harness.cwd, "edited-by-first.txt"), "first\n");
          yield* endTurnOf(
            harness,
            threadId,
            first.runId,
            first.runOrdinal,
            fs.remove(link).pipe(Effect.andThen(fs.symlink(other, link)), Effect.orDie),
          );
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const checkpoint = projection.checkpoints.find(
            (candidate) => candidate.runId === first.runId,
          )!;
          assert.deepEqual(
            [checkpoint.status, checkpoint.files.map((file) => file.path)],
            ["ready", ["edited-by-first.txt"]],
          );
          assert.equal(
            projection.runs.find((run) => run.id === first.runId)!.workspaceCwd,
            yield* fs.realPath(harness.cwd),
          );
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// Recorded before the upgrade: run 1 ended in A with its capture still pending when the one root
// scope was re-created in B. Captured after the upgrade, run 1 still has no recorded workspace, so
// its thread start, which is A's, is not placed in B by the capture.
it.effect("a run recorded before the upgrade, captured after it, keeps its workspace unknown", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const database = SqlitePersistence.layerMemory;
      const harness = yield* nextTurnSelectionHarness(
        "pending-legacy-capture",
        false,
        true,
        Effect.void,
        true,
        undefined,
        undefined,
        undefined,
        database,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const other = yield* repoWorktree(harness.cwd, "switched");
        yield* fs.writeFileString(path.join(other, "only-on-b.txt"), "b\n");
        const threadId = yield* harness.startFirstTurn;
        const first = harness.started[0]!;
        yield* fs.writeFileString(path.join(harness.cwd, "edited-by-first.txt"), "first\n");
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const run = projection.runs.find((candidate) => candidate.id === first.runId)!;
        const turn = projection.providerTurns.find(
          (candidate) => candidate.runAttemptId === run.activeAttemptId,
        )!;
        const waiting = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(harness.events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(harness.events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(waiting);
        // Before the upgrade: runs kept no workspace, and the next run re-created the scope in B.
        yield* recordedBeforeUpgrade(threadId);
        const root = (yield* orchestrator.getThreadProjection(threadId)).checkpointScopes[0]!;
        yield* sink.write({
          events: [
            {
              id: EventId.make("legacy-root-scope-in-b"),
              type: "checkpoint-scope.created",
              threadId,
              providerInstanceId: instanceId,
              occurredAt: yield* DateTime.now,
              payload: { ...root, cwd: other },
            },
          ],
        });
        yield* orchestrator.dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make("switch"),
          threadId,
          worktreePath: other,
        });
        yield* worker.drain();
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.isUndefined(
          after.runs.find((candidate) => candidate.id === first.runId)!.workspaceCwd,
        );
        const threadStart = after.checkpoints.find(
          (checkpoint) => checkpoint.ordinalWithinScope === 0 && checkpoint.appRunOrdinal === null,
        )!;
        const refused = yield* orchestrator
          .dispatch({
            type: "checkpoint.rollback",
            commandId: CommandId.make("start-files"),
            threadId,
            scopeId: threadStart.scopeId,
            checkpointId: threadStart.id,
            restoreFiles: true,
          })
          .pipe(Effect.flip);
        assert.include(String(refused.cause), "can't tell which worktree");
        assert.isTrue(yield* fs.exists(path.join(other, "only-on-b.txt")));
      }).pipe(Effect.provide(Layer.mergeAll(harness.layer, NodeServices.layer, database)));
    }),
  ),
);

// B is reached through a symlink for runs 2 and 3; back in A, B is removed and the symlink pointed at
// another repository. A rewind to run 1 must still drop B's stale refs, through A (one repository).
it.effect(
  "a rewind drops a removed worktree's stale refs though its symlink now names another repository",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* nextTurnSelectionHarness("rewind-removed-symlinked-worktree");
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const thread = yield* worktreeThread(harness);
          const link = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: "t3-rewind-symlinked-" }),
            "link",
          );
          yield* fs.symlink(thread.other, link);
          yield* thread.switchTo(link);
          yield* thread.nextRun(link, "edited-by-second.txt");
          yield* thread.nextRun(link, "edited-by-third.txt");
          yield* thread.switchTo(harness.cwd);
          assert.equal(
            yield* thread.git(harness.cwd, "worktree", "remove", "--force", thread.other),
            0,
          );
          yield* fs.remove(link);
          yield* fs.symlink(yield* checkpointWorkspace("rewind-removed-symlinked-elsewhere"), link);
          const target = yield* thread.checkpointOf(thread.first.runId);
          const laterInB = (yield* orchestrator.getThreadProjection(
            thread.threadId,
          )).checkpoints.filter(
            (checkpoint) =>
              checkpoint.status === "ready" &&
              checkpoint.scopeId !== target.scopeId &&
              checkpoint.ordinalWithinScope > 1,
          );
          assert.lengthOf(laterInB, 2);
          yield* orchestrator.dispatch({
            type: "checkpoint.rollback",
            commandId: CommandId.make("revert-conversation"),
            threadId: thread.threadId,
            scopeId: target.scopeId,
            checkpointId: target.id,
            restoreFiles: false,
          });
          yield* worker.drain();
          for (const checkpoint of laterInB) {
            assert.notEqual(
              yield* thread.git(harness.cwd, "rev-parse", "--verify", "--quiet", checkpoint.ref),
              0,
            );
          }
        }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
      }),
    ),
);

// The scope choice at dispatch uses the thread's scopes from the command's own projection: no
// second read of them inside the serialized command section.
it.effect("a message dispatch reads the thread's checkpoint scopes once", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const harness = yield* nextTurnSelectionHarness("dispatch-scope-reads");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const thread = yield* worktreeThread(harness);
        const scopeReads: Array<string> = [];
        const observe: Statement.Transformer = (statement) => {
          const [query] = statement.compile();
          if (/FROM orchestration_v2_projection_checkpoint_scopes/.test(query)) {
            scopeReads.push(query);
          }
          return Effect.succeed(statement);
        };
        yield* orchestrator
          .dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("counted"),
            threadId: thread.threadId,
            messageId: MessageId.make("message:counted"),
            text: "counted",
            attachments: [],
            modelSelection: runSelection,
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          })
          .pipe(Effect.provideService(Statement.CurrentTransformer, observe));
        assert.lengthOf(scopeReads, 1);
      }).pipe(Effect.provide(Layer.merge(harness.layer, NodeServices.layer)));
    }),
  ),
);

it.effect("queues a plain send behind the stopped queue and releases its hold", () =>
  Effect.gen(function* () {
    const interruptReached = yield* Deferred.make<void>();
    const releaseInterrupt = yield* Deferred.make<void>();
    const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "plain-send-after-stop",
      false,
      true,
      Deferred.succeed(interruptReached, undefined).pipe(
        Effect.andThen(Deferred.await(releaseInterrupt)),
      ),
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const send = (id: string, delivery: "queue" | "auto") =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(id),
          threadId,
          messageId: MessageId.make(`message:${id}`),
          text: id,
          attachments: [],
          modelSelection: runSelection,
          ...(delivery === "queue"
            ? { dispatchMode: { type: "queue_after_active" } }
            : { dispatchMode: { type: "start_immediately" }, deliveryIntent: "auto" }),
          createdBy: "user",
          creationSource: "web",
        });
      yield* send("queued", "queue");
      yield* orchestrator.dispatch({
        type: "run.interrupt",
        commandId: CommandId.make("stop"),
        threadId,
        runId: started[0]!.runId,
        holdQueue: true,
      });
      // The provider has not stopped yet, so a plain send still resolves to a steer.
      yield* send("sent", "auto");
      // Once the stop settles, the run-update reactor promotes the queue on
      // its own fiber, so read the queue while the provider is still stopping.
      const draining = yield* worker.drain().pipe(Effect.forkScoped);
      yield* Deferred.await(interruptReached);

      const projection = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(started.length, 1);
      const queued = projection.runs
        .filter((run) => run.status === "queued")
        .sort((left, right) => (left.queuePosition ?? 0) - (right.queuePosition ?? 0));
      assert.deepEqual(
        queued.map(
          (run) => projection.messages.find((message) => message.id === run.userMessageId)?.text,
        ),
        ["queued", "sent"],
      );
      // The user sent it, so the queue they stopped runs again once the provider stops.
      assert.isFalse(queued.some((run) => run.queueHeld === true));
      yield* Deferred.succeed(releaseInterrupt, undefined);
      yield* Fiber.join(draining);
      assert.deepEqual(steered, []);
    }).pipe(Effect.provide(layer));
  }),
);

const goalHarness = Effect.fn("goalHarness")(function* (name: string, nativeGoals?: boolean) {
  const harness = yield* nextTurnSelectionHarness(
    name,
    false,
    true,
    Effect.void,
    true,
    nativeGoals,
  );
  const send = (id: string, text: string, delivery: "queue" | "auto", freshContext = false) =>
    Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(id),
        threadId: ThreadId.make(`thread:${name}`),
        messageId: MessageId.make(`message:${id}`),
        text,
        attachments: [],
        modelSelection: runSelection,
        ...(freshContext ? { freshContext } : {}),
        ...(delivery === "queue"
          ? { dispatchMode: { type: "queue_after_active" } }
          : { dispatchMode: { type: "start_immediately" }, deliveryIntent: "auto" }),
        createdBy: "user",
        creationSource: "web",
      });
    });
  const read = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const projection = yield* (yield* Orchestrator.OrchestratorV2).getThreadProjection(threadId);
      const textOf = (run: (typeof projection.runs)[number]) =>
        projection.messages.find((message) => message.id === run.userMessageId)?.text;
      return {
        queue: projection.runs
          .filter((run) => run.status === "queued")
          .toSorted((left, right) => (left.queuePosition ?? 0) - (right.queuePosition ?? 0))
          .map(textOf),
        started: harness.started.map((turn) => turn.message.text),
        stopRequested: projection.turnItems.some(
          (item) =>
            item.type === "run_interrupt_request" && item.runId === harness.started[0]?.runId,
        ),
      };
    });
  return { ...harness, send, read };
});

it.effect.each([undefined, false, true])(
  "goal dispatch stops the active turn only with native support: %s",
  (nativeGoals) =>
    Effect.gen(function* () {
      const { steered, layer, startFirstTurn, send, read } = yield* goalHarness(
        "goal-steer",
        nativeGoals,
      );
      yield* Effect.gen(function* () {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* send("queued", "queued", "queue");
        yield* send("goal", "/goal all tests pass", "auto");
        yield* worker.drain();

        assert.deepEqual(steered, []);
        assert.deepEqual(yield* read(threadId), {
          queue: nativeGoals === false ? ["queued", "/goal all tests pass"] : ["queued"],
          started: nativeGoals === false ? ["first"] : ["first", "/goal all tests pass"],
          stopRequested: nativeGoals !== false,
        });
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("starts a goal that names no model on the thread's model ahead of a queued pick", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn, read } = yield* goalHarness("goal-ahead-of-pick");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      for (const [id, text, modelSelection] of [
        ["picked", "picked", composerSelection],
        ["goal", "/goal all tests pass", undefined],
      ] as const) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(id),
          threadId,
          messageId: MessageId.make(`message:${id}`),
          text,
          attachments: [],
          ...(modelSelection === undefined ? {} : { modelSelection }),
          dispatchMode:
            id === "picked" ? { type: "queue_after_active" } : { type: "start_immediately" },
          ...(id === "picked" ? {} : { deliveryIntent: "auto" as const }),
          createdBy: "user",
          creationSource: "web",
        });
      }
      yield* worker.drain();

      assert.deepEqual(yield* read(threadId), {
        queue: ["picked"],
        started: ["first", "/goal all tests pass"],
        stopRequested: true,
      });
      assert.deepEqual(started[1]?.modelSelection, runSelection);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("restarts by stopping the running turn when the provider cannot interrupt-restart", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn, send, read } = yield* goalHarness(
      "restart-without-interrupt-restart",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restart"),
        threadId,
        messageId: MessageId.make("message:restart"),
        text: "do this instead",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "restart_active", targetRunId: started[0]!.runId },
        createdBy: "agent",
        creationSource: "server",
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: ["queued"],
        started: ["first", "do this instead"],
        stopRequested: true,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("holds an agent's send and restart after Stop behind the older held queue", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn, send, read } =
      yield* goalHarness("restart-past-held-queue");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop"),
        threadId,
      });
      yield* worker.drain();
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("second"),
        threadId,
        messageId: MessageId.make("message:second"),
        text: "second",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "agent",
        creationSource: "mcp",
      });
      yield* worker.drain();
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restart"),
        threadId,
        messageId: MessageId.make("message:restart"),
        text: "do this instead",
        attachments: [],
        dispatchMode: { type: "restart_active", targetRunId: started[0]!.runId },
        createdBy: "agent",
        creationSource: "mcp",
      });
      yield* worker.drain();

      const held = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
        (run) => run.status === "queued",
      );
      assert.deepEqual(
        held.map((run) => run.queueHeld === true),
        [true, true, true],
      );
      const { queue, started: startedTexts } = yield* read(threadId);
      assert.deepEqual(
        { queue, started: startedTexts },
        { queue: ["queued", "second", "do this instead"], started: ["first"] },
      );

      yield* orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("resume"),
        threadId,
      });
      yield* worker.drain();
      assert.deepEqual(yield* read(threadId), {
        queue: ["second", "do this instead"],
        started: ["first", "queued"],
        stopRequested: true,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("reports a restart that stops the running turn as restarted", () =>
  Effect.gen(function* () {
    const { started, layer, startFirstTurn } = yield* goalHarness("restart-send-delivery");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const threadId = yield* startFirstTurn;
      const input = {
        projectId: ProjectId.make("project:restart-send-delivery"),
        commandId: CommandId.make("restart"),
        threadId,
        messageId: MessageId.make("message:restart"),
        text: "do this instead",
        attachments: [],
        mode: "restart",
        createdBy: "agent",
        creationSource: "mcp",
      } as const;
      const sent = yield* threadManagement.sendToThread(input);
      // Stop holds the replacement before it starts; a replay must still report the restart.
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-after-restart"),
        threadId,
      });
      const replayed = yield* threadManagement.sendToThread(input);

      assert.equal(started.length, 1);
      assert.deepEqual([sent.delivery, replayed.delivery], ["restarted", "restarted"]);
    }).pipe(
      Effect.provide(
        ThreadManagementService.layer.pipe(
          Layer.provideMerge(layer),
          Layer.provide(ServerSettings.layerTest()),
          Layer.provide(
            CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
          ),
        ),
      ),
    );
  }),
);

it.effect("replays an accepted steer after its target turn completes", () =>
  Effect.gen(function* () {
    const name = "steer-retry-after-completion";
    const { events, started, steered, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness(name);
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadManagement = yield* ThreadManagementService.ThreadManagementService;
      const threadId = yield* startFirstTurn;
      const input = {
        projectId: ProjectId.make(`project:${name}`),
        commandId: CommandId.make("steer"),
        threadId,
        messageId: MessageId.make("message:steer"),
        text: "Please report the test result",
        attachments: [],
        mode: "steer",
        createdBy: "agent",
        creationSource: "mcp",
      } as const;
      const accepted = yield* threadManagement.sendToThread(input);
      assert.equal(accepted.delivery, "steered");
      yield* worker.drain();

      const waiting = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === accepted.run.id &&
            event.payload.status === "waiting",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped({ startImmediately: true }),
      );
      const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: accepted.run.ordinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(waiting);
      yield* worker.drain();

      const replayed = yield* threadManagement.sendToThread(input);
      assert.equal(replayed.dispatch.replayed, true);
      assert.equal(replayed.delivery, accepted.delivery);
      assert.equal(replayed.message.id, accepted.message.id);
      assert.equal(replayed.run.id, accepted.run.id);
      assert.equal(replayed.run.status, "completed");

      const fresh = yield* threadManagement
        .sendToThread({
          ...input,
          commandId: CommandId.make("fresh-steer"),
          messageId: MessageId.make("message:fresh-steer"),
        })
        .pipe(Effect.flip);
      assert.equal(fresh._tag, "ThreadManagementNoSteerableRunError");
      yield* worker.drain();
      assert.deepEqual(steered, [
        `[T3 Code: this message came through T3 Code's tools, not from the user typing in this chat.]\n\n${input.text}`,
      ]);
      assert.lengthOf(started, 1);
    }).pipe(
      Effect.provide(
        ThreadManagementService.layer.pipe(
          Layer.provideMerge(layer),
          Layer.provide(ServerSettings.layerTest()),
          Layer.provide(
            CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
          ),
        ),
      ),
    );
  }),
);

it.effect("queues a goal clear without stopping work when no goal drives it", () =>
  Effect.gen(function* () {
    const { steered, layer, startFirstTurn, send, read } = yield* goalHarness("goal-clear");
    yield* Effect.gen(function* () {
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* send("clear", "/goal clear", "auto");
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: ["queued", "/goal clear"],
        started: ["first"],
        stopRequested: false,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("promoting a queued /context moves it first and does not steer", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn, send, read } =
      yield* goalHarness("slash-promote");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* send("context", "/context", "queue");
      const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.userMessageId === MessageId.make("message:context"),
      )!;
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: queuedRun.id,
        targetRunId: started[0]!.runId,
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: ["/context", "queued"],
        started: ["first"],
        stopRequested: false,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("restarts the turn for a slash command sent as an explicit restart", () =>
  Effect.gen(function* () {
    const { started, steered, interrupted, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("slash-restart", true);
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("restart-context"),
        threadId,
        messageId: MessageId.make("message:restart-context"),
        text: "/context",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "restart_active", targetRunId: started[0]!.runId },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.lengthOf(interrupted, 1);
      assert.deepEqual(
        started.map((turn) => turn.message.text),
        ["first", "/context"],
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("promotes a queued goal by stopping the running turn", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn, send, read } =
      yield* goalHarness("goal-promote");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      yield* send("queued", "queued", "queue");
      yield* send("goal", "/goal ship it", "queue");
      const goalRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.userMessageId === MessageId.make("message:goal"),
      )!;
      yield* orchestrator.dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make("promote"),
        threadId,
        queuedRunId: goalRun.id,
        targetRunId: started[0]!.runId,
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: ["queued"],
        started: ["first", "/goal ship it"],
        stopRequested: true,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("stops the running turn for a goal an agent steers into it", () =>
  Effect.gen(function* () {
    const { started, steered, layer, startFirstTurn, read } = yield* goalHarness("goal-raw-steer");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      // Agent sends carry a raw steer target and no delivery intent.
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("goal"),
        threadId,
        messageId: MessageId.make("message:goal"),
        text: "/goal ship it",
        attachments: [],
        modelSelection: runSelection,
        dispatchMode: { type: "steer_active", targetRunId: started[0]!.runId },
        createdBy: "agent",
        creationSource: "server",
      });
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: [],
        started: ["first", "/goal ship it"],
        stopRequested: true,
      });
    }).pipe(Effect.provide(layer));
  }),
);

// A fresh message skips compaction as its own turn, which steering a running turn would not.
it.effect.each(["next", "/goal ship it", "fresh next"])(
  "promoting %s into a compaction stops it and sends it next",
  (promoted) =>
    Effect.gen(function* () {
      const { events, layer, send, read, started, steered, startFirstTurn } = yield* goalHarness(
        `compaction-promote-${promoted.length}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* send("compact", "/compact", "queue");
        yield* send("queued", "queued", "queue");
        yield* send("next", promoted, "queue", promoted.startsWith("fresh"));
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === started[0]!.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const compacting = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.status === "running" &&
              event.payload.runAttemptId !== started[0]!.attemptId,
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: started[0]!.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
        yield* Fiber.join(compacting);
        const compactRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:compact"),
        )!;
        const nextRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:next"),
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("promote"),
          threadId,
          queuedRunId: nextRun.id,
          targetRunId: compactRun.id,
        });
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, []);
        assert.isTrue(
          projection.turnItems.some(
            (item) => item.type === "run_interrupt_request" && item.runId === compactRun.id,
          ),
        );
        assert.deepEqual((yield* read(threadId)).queue, ["queued"]);
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", "/compact", promoted],
        );
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("stops a turn that is still starting when a goal is sent into it", () =>
  Effect.gen(function* () {
    const { cwd, layer, send, started } = yield* goalHarness("goal-during-start");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = ThreadId.make("thread:goal-during-start");
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create"),
        threadId,
        projectId: ProjectId.make("project:goal-during-start"),
        title: "Goal during start",
        modelSelection: runSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      // The stopped run's end starts the queue, which then needs one more drain.
      const goalStarting = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) => event.type === "run.updated" && event.payload.status === "starting",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* send("first", "first", "auto");
      yield* send("goal", "/goal all tests pass", "auto");
      yield* worker.drain();
      yield* Fiber.join(goalStarting);
      yield* worker.drain();

      const projection = yield* orchestrator.getThreadProjection(threadId);
      const statusOf = (text: string) =>
        projection.runs.find(
          (run) =>
            projection.messages.find((message) => message.id === run.userMessageId)?.text === text,
        )?.status;
      assert.equal(statusOf("first"), "interrupted");
      assert.deepEqual(
        started.map((turn) => turn.message.text),
        ["/goal all tests pass"],
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("stops the running turn when a goal clear ends the goal driving it", () =>
  Effect.gen(function* () {
    const { events, layer, startFirstTurn, send, read, steered } =
      yield* goalHarness("goal-clear-active");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const providerThread = (yield* orchestrator.getThreadProjection(threadId))
        .providerThreads[0]!;
      const goalSeen = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "provider-thread.updated" && event.payload.goal?.status === "active",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* Queue.offer(events, {
        type: "provider_thread.updated",
        driver,
        providerThread: {
          ...providerThread,
          goal: { objective: "all tests pass", status: "active" },
        },
      });
      yield* Fiber.join(goalSeen);
      yield* send("clear", "/goal clear", "auto");
      yield* worker.drain();

      assert.deepEqual(steered, []);
      assert.deepEqual(yield* read(threadId), {
        queue: [],
        started: ["first", "/goal clear"],
        stopRequested: true,
      });
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("a side chat that opens with /goal gets its context on the next question", () =>
  Effect.gen(function* () {
    const { events, layer, started, startFirstTurn } = yield* goalHarness("side-chat-goal");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const parentId = yield* startFirstTurn;
      const sideChatId = ThreadId.make("thread:side-chat-goal-child");
      yield* orchestrator.dispatch({
        type: "thread.fork",
        commandId: CommandId.make("side-chat"),
        sourceThreadId: parentId,
        targetThreadId: sideChatId,
        sourcePoint: { type: "latest_stable" },
        sideChat: true,
        createdBy: "user",
        creationSource: "web",
      });
      const ask = (id: string, text: string) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(id),
          threadId: sideChatId,
          messageId: MessageId.make(`message:${id}`),
          text,
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
      const sideChatEvent = (matches: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter((event) => event.threadId === sideChatId && matches(event)),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped({ startImmediately: true }),
        );
      const running = yield* sideChatEvent(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      );
      const settled = yield* sideChatEvent(
        (event) => event.type === "run.updated" && event.payload.status === "waiting",
      );
      yield* ask("goal", "/goal clear");
      yield* worker.drain();
      yield* Fiber.join(running);
      const turn = (yield* orchestrator.getThreadProjection(sideChatId)).providerTurns[0]!;
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
      });
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: started[1]!.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();
      yield* ask("question", "What is the main thread doing?");
      yield* worker.drain();

      const [first, goal, question] = started.map((turn) => turn.message.text);
      assert.deepEqual([first, goal], ["first", "/goal clear"]);
      assert.include(question, "You are in a side chat beside the main thread.");
      assert.include(question, "- User: first");
      assert.isTrue(question?.endsWith("What is the main thread doing?"));
    }).pipe(Effect.provide(layer));
  }),
);

// Cursor has its own SDK path. A real adapter must propagate the transport claim;
// an accepted replacement must never become a second prompt when Resume runs.
it.effect.each([true, false])(
  "preserves Cursor restart ownership through Stop, preparing=%s",
  (preparing) =>
    Effect.scoped(
      Effect.gen(function* () {
        const cwd = yield* checkpointWorkspace("cursor-restart-stop");
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const sent: string[] = [];
        const cursorId = ProviderInstanceId.make("cursor");
        const threadId = ThreadId.make("thread:cursor-restart-stop");
        const pick = { instanceId: cursorId, model: "composer-2.5" };
        const runner = Layer.succeed(CursorAgentSdk.CursorAgentSdkRunner, {
          assertComplete: Effect.void,
          open: () =>
            Effect.succeed({
              agentId: "cursor-restart",
              listMessages: Effect.succeed([]),
              close: Effect.void,
              send: (input) =>
                Effect.gen(function* () {
                  if (preparing && sent.length > 0) {
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(release);
                  }
                  const enqueue = () => {
                    sent.push(
                      typeof input.message === "string" ? input.message : input.message.text,
                    );
                  };
                  if (input.submit === undefined) enqueue();
                  else
                    yield* input.submit(enqueue).pipe(
                      Effect.mapError(
                        (cause) =>
                          new CursorAgentSdk.CursorAgentSdkRunnerError({
                            method: "run.start",
                            cause,
                          }),
                      ),
                    );
                  const runId = `cursor-native-${sent.length}`;
                  const cancelled = yield* Deferred.make<void>();
                  return {
                    runId,
                    agentId: "cursor-restart",
                    wait: Deferred.await(cancelled).pipe(
                      Effect.as({
                        id: runId,
                        requestId: runId,
                        status: "cancelled" as const,
                        model: { id: pick.model },
                        durationMs: 1,
                      }),
                    ),
                    cancel: Deferred.succeed(cancelled, undefined).pipe(Effect.asVoid),
                  };
                }),
            }),
        });
        const adapter = Layer.unwrap(
          makeCursorAdapterV2({ instanceId: cursorId, environment: {} }).pipe(
            Effect.map(ProviderAdapterRegistry.layerSingle),
            Effect.provide(
              Layer.mergeAll(
                runner,
                TestProviderHost.layer({ cwd }),
                IdAllocator.layer,
                McpProviderSessions.layer,
                NodeCrypto.layer,
              ).pipe(Layer.provideMerge(NodeServices.layer)),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const o = yield* Orchestrator.OrchestratorV2;
          const w = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const watch = (ordinal: number) =>
            o.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "provider-turn.updated" &&
                  event.payload.status === "running" &&
                  event.payload.ordinal === ordinal,
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped,
            );
          const message = (name: string) => ({
            type: "message.dispatch" as const,
            threadId,
            commandId: CommandId.make(name),
            messageId: MessageId.make(`message:${name}`),
            text: name,
            attachments: [],
            createdBy: "agent" as const,
            creationSource: "mcp" as const,
            dispatchMode: { type: "start_immediately" as const },
          });
          yield* o.dispatch({
            type: "thread.create",
            threadId,
            commandId: CommandId.make("cursor-create"),
            projectId: ProjectId.make("project:cursor-restart"),
            title: "Cursor restart",
            modelSelection: pick,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: cwd,
            createdBy: "user",
            creationSource: "web",
          });
          const first = yield* watch(1);
          yield* o.dispatch(message("Initial cursor prompt"));
          yield* w.drain();
          yield* Fiber.join(first);
          const second = preparing ? undefined : yield* watch(2);
          const replacement = message("Original cursor replacement");
          yield* o.dispatch({ ...replacement, deliveryIntent: "restart" });
          const pending = yield* w.drain().pipe(Effect.forkScoped);
          if (preparing) yield* Deferred.await(entered);
          else {
            yield* Fiber.join(pending);
            yield* Fiber.join(second!);
          }
          yield* o.dispatch({
            type: "thread.stop",
            threadId,
            commandId: CommandId.make("cursor-stop"),
          });
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(pending);
          yield* w.drain();
          const paused = yield* o.getThreadProjection(threadId);
          const held = paused.runs.filter(
            (run) => run.userMessageId === replacement.messageId && run.status === "queued",
          );
          assert.lengthOf(held, preparing ? 1 : 0);
          if (preparing) assert.isTrue(held[0]?.queueHeld);
          yield* o.dispatch({
            type: "queue.resume",
            threadId,
            commandId: CommandId.make("cursor-resume"),
          });
          yield* w.drain();
          assert.lengthOf(
            sent.filter((text) => text.includes(replacement.text)),
            1,
          );
        }).pipe(
          Effect.provide(
            ProviderReplayHarness.layerWithRegistry({ name: "cursor-restart-stop" }, adapter, {
              runEffectWorker: false,
            }),
          ),
        );
      }),
    ),
);

const queueFrom = (
  orchestrator: Orchestrator.OrchestratorV2["Service"],
  threadId: ThreadId,
  name: string,
  origin: "automation" | "cross-thread" | "linked" | "self" | "user" | "wake",
) =>
  orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make(`queue-${name}`),
    threadId,
    messageId: MessageId.make(`message:${name}`),
    text: name,
    attachments: [],
    dispatchMode: { type: "queue_after_active" },
    ...(origin === "automation"
      ? { scheduledTaskId: ScheduledTaskId.make("task-auto-send-now") }
      : origin === "cross-thread"
        ? { senderThreadId: ThreadId.make("thread:sender") }
        : origin === "self"
          ? { senderThreadId: threadId }
          : origin === "linked"
            ? {
                linkedSender: {
                  environmentId: EnvironmentId.make("environment:linked"),
                  threadId: ThreadId.make("thread:linked"),
                  title: "Linked sender",
                },
              }
            : origin === "wake"
              ? {
                  notification: {
                    source: { kind: "background_task" as const },
                    outcome: "updated" as const,
                    summary: "Background activity updated",
                  },
                }
              : {}),
    createdBy: origin === "user" || origin === "automation" ? "user" : "agent",
    creationSource:
      origin === "user" || origin === "automation" ? "web" : origin === "wake" ? "server" : "mcp",
  });

const steersInto = (
  orchestrator: Orchestrator.OrchestratorV2["Service"],
  runId: string,
  count: number,
) =>
  orchestrator.streamDomainEvents.pipe(
    Stream.filter(
      (event) =>
        event.type === "message.updated" &&
        event.payload.runId === runId &&
        event.payload.role === "user" &&
        event.payload.id !== MessageId.make("message:first"),
    ),
    Stream.take(count),
    Stream.runCollect,
    Effect.forkScoped,
  );

it.effect(
  "steers messages a user, an automation or another thread queued into one busy turn with the first automatic steer, one steer a minute",
  () =>
    Effect.gen(function* () {
      const { steered, started, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("auto-send-now-live");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const queuedAt = yield* DateTime.now;
        for (const [name, origin] of [
          ["wake", "wake"],
          ["user", "user"],
          ["self", "self"],
          ["automation", "automation"],
          ["cross-thread", "cross-thread"],
          ["linked", "linked"],
        ] as const)
          yield* queueFrom(orchestrator, threadId, name, origin);
        const promoted = yield* steersInto(orchestrator, started[0]!.runId, 5);
        // This provider's steering leaves running tools alone, so the scheduled prompt is due at
        // once, and its steer carries the messages still in their grace.
        yield* TestClock.adjust("10 seconds");
        yield* worker.drain();
        yield* queueFrom(orchestrator, threadId, "late", "user");
        yield* TestClock.adjust("49 seconds");
        yield* worker.drain();
        assert.deepEqual(steered, [
          [
            "4 queued messages, sent together:",
            "[1/4] From the user:\nuser",
            "[2/4] From automation task-auto-send-now:\nautomation",
            "[3/4] From thread thread:sender:\ncross-thread",
            '[4/4] From thread "Linked sender" (thread:linked) in linked environment environment:linked:\nlinked',
          ].join("\n\n"),
        ]);
        yield* TestClock.adjust("60 seconds");
        const promotions = Array.from(yield* Fiber.join(promoted));
        yield* worker.drain();
        assert.equal(steered[1], "late");
        const delays = promotions.map((event) =>
          Duration.toMillis(DateTime.distance(queuedAt, event.occurredAt)),
        );
        assert.deepEqual(delays, [5_000, 5_000, 5_000, 5_000, 70_000]);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          after.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
          ["message:wake", "message:self"].map((id) => MessageId.make(id)),
        );
        assert.lengthOf(started, 1);
      }).pipe(Effect.provide(layer));
    }),
);

it.effect(
  "waits in a gap and steers a due message when the turn starts, never one the provider would restart for",
  () =>
    Effect.gen(function* () {
      const { events, steered, started, interrupted, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("auto-send-now-gap", true, true, Effect.void, false);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const before = yield* orchestrator.getThreadProjection(threadId);
        const run = before.runs[0]!;
        const turn = before.providerTurns[0]!;
        const now = yield* DateTime.now;
        yield* (yield* EventSink.EventSinkV2).write({
          events: [
            {
              id: EventId.make("gap-run"),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: { ...run, status: "starting" },
            },
            {
              id: EventId.make("gap-turn"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: now,
              payload: { ...turn, status: "pending" },
            },
          ],
        });
        yield* queueFrom(orchestrator, threadId, "automation", "automation");
        yield* queueFrom(orchestrator, threadId, "cross-thread", "cross-thread");
        yield* TestClock.adjust("65 seconds");
        yield* worker.drain();
        const waiting = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
          (candidate) => candidate.status === "queued",
        );
        assert.deepEqual(
          waiting.map((candidate) => [candidate.userMessageId, candidate.steerWhenReadyRunId]),
          [
            [MessageId.make("message:automation"), undefined],
            [MessageId.make("message:cross-thread"), undefined],
          ],
        );
        // An image would make this provider restart the turn instead of steering it.
        const automation = waiting[0]!;
        yield* orchestrator.dispatch({
          type: "queued-run.edit",
          commandId: CommandId.make("edit-automation"),
          threadId,
          runId: automation.id,
          text: "automation",
          attachments: [
            {
              type: "image",
              id: ChatAttachmentId.make("automation-image"),
              name: "screenshot.png",
              mimeType: "image/png",
              sizeBytes: 4,
            },
          ],
        });
        const outcome = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              (event.type === "run.updated" &&
                event.payload.id === automation.id &&
                event.payload.status !== "queued") ||
              (event.type === "message.updated" &&
                event.payload.runId === run.id &&
                event.payload.id === MessageId.make("message:cross-thread")),
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* (yield* EventSink.EventSinkV2).write({
          events: [
            {
              id: EventId.make("ready-run"),
              type: "run.updated",
              threadId,
              occurredAt: now,
              payload: run,
            },
          ],
        });
        yield* Queue.offer(events, { type: "provider_turn.updated", driver, providerTurn: turn });
        yield* Fiber.join(outcome);
        yield* worker.drain();
        assert.deepEqual(steered.map(withoutSenderNote), ["cross-thread"]);
        assert.deepEqual(interrupted, []);
        assert.lengthOf(started, 1);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          after.runs.find((candidate) => candidate.id === automation.id)?.status,
          "queued",
        );
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("steers a deferred steer into the next run when its own run ends first", () =>
  Effect.gen(function* () {
    const { events, steered, started, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("auto-send-now-run-ended");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!;
      yield* queueFrom(orchestrator, threadId, "user", "user");
      const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      const turnPaused = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.payload.id === turn.id &&
            event.payload.status === "pending",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* Queue.offer(events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "pending" },
      });
      yield* Fiber.join(turnPaused);
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("steer-in-gap"),
        threadId,
        messageId: MessageId.make("message:steer-in-gap"),
        text: "steer-in-gap",
        attachments: [],
        deliveryIntent: "steer",
        dispatchMode: { type: "queue_after_active" },
        createdBy: "user",
        creationSource: "web",
      });
      assert.equal(
        (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:steer-in-gap"),
        )?.steerWhenReadyRunId,
        first.runId,
      );
      const settled = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "waiting",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      yield* Queue.offer(events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(settled);
      yield* worker.drain();
      yield* orchestrator.resumeQueuedRuns;
      const next = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (run) => run.userMessageId === MessageId.make("message:user"),
      )!;
      const promoted = yield* steersInto(orchestrator, next.id, 1);
      yield* worker.drain();
      yield* Fiber.join(promoted);
      yield* worker.drain();
      assert.deepEqual(
        started.map((input) => input.message.text),
        ["first", "user"],
      );
      assert.deepEqual(steered, ["steer-in-gap"]);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect(
  "retries a promoted steer that missed its turn on the next turn, not the one it missed",
  () =>
    Effect.gen(function* () {
      let attempts = 0;
      const { events, steered, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "auto-send-now-missed",
        false,
        true,
        Effect.void,
        true,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        Effect.void,
        Effect.suspend(() =>
          attempts++ > 0
            ? Effect.void
            : Effect.fail(
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver,
                  providerThreadId: ProviderThreadId.make("provider-thread:auto-send-now-missed"),
                  providerTurnId: ProviderTurnId.make("provider-turn:auto-send-now-missed"),
                  deliveryNotSent: true,
                  cause: "The turn ended before the steer reached it",
                }),
              ),
        ),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        yield* queueFrom(orchestrator, threadId, "cross-thread", "cross-thread");
        // The agent's message to its own thread never steers; it starts the next run.
        yield* queueFrom(orchestrator, threadId, "self", "self");
        const firstPromotion = yield* steersInto(orchestrator, first.runId, 1);
        yield* TestClock.adjust("60 seconds");
        yield* Fiber.join(firstPromotion);
        yield* worker.drain();
        assert.deepEqual(steered, []);
        const followUp = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) =>
            run.status === "queued" && run.userMessageId === MessageId.make("message:cross-thread"),
        );
        assert.equal(followUp?.steerFollowUpOfRunId, first.runId);
        yield* TestClock.adjust("30 seconds");
        yield* worker.drain();
        assert.equal(attempts, 1);
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        const next = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:self"),
        )!;
        const retry = yield* steersInto(orchestrator, next.id, 1);
        yield* worker.drain();
        yield* Fiber.join(retry);
        yield* worker.drain();
        assert.deepEqual(
          started.map((input) => withoutSenderNote(input.message.text)),
          ["first", "self"],
        );
        assert.deepEqual(steered.map(withoutSenderNote), ["cross-thread"]);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.isFalse(after.runs.some((run) => run.status === "queued"));
      }).pipe(Effect.provide(layer));
    }),
);

it.effect(
  "queues every message of a merged steer again, in order, when it misses the turn, and delivers each once",
  () =>
    Effect.gen(function* () {
      let attempts = 0;
      const { events, steered, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "auto-send-now-merged-missed",
        false,
        true,
        Effect.void,
        true,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        Effect.void,
        Effect.suspend(() =>
          attempts++ > 0
            ? Effect.void
            : Effect.fail(
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver,
                  providerThreadId: ProviderThreadId.make(
                    "provider-thread:auto-send-now-merged-missed",
                  ),
                  providerTurnId: ProviderTurnId.make("provider-turn:auto-send-now-merged-missed"),
                  deliveryNotSent: true,
                  cause: "The turn ended before the steer reached it",
                }),
              ),
        ),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        yield* queueFrom(orchestrator, threadId, "self", "self");
        yield* queueFrom(orchestrator, threadId, "cross-thread", "cross-thread");
        yield* queueFrom(orchestrator, threadId, "linked", "linked");
        yield* queueFrom(orchestrator, threadId, "user", "user");
        const firstPromotion = yield* steersInto(orchestrator, first.runId, 3);
        yield* TestClock.adjust("60 seconds");
        yield* Fiber.join(firstPromotion);
        yield* worker.drain();
        assert.equal(attempts, 1);
        assert.deepEqual(steered, []);
        const requeued = (yield* orchestrator.getThreadProjection(threadId)).runs
          .filter((run) => run.status === "queued" && run.steerFollowUpOfRunId === first.runId)
          .toSorted((left, right) => left.ordinal - right.ordinal)
          .map((run) => run.userMessageId);
        assert.deepEqual(
          requeued,
          ["message:user", "message:cross-thread", "message:linked"].map((id) =>
            MessageId.make(id),
          ),
        );
        const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        const settled = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "waiting",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: yield* DateTime.now },
        });
        yield* Queue.offer(events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(settled);
        yield* worker.drain();
        yield* orchestrator.resumeQueuedRuns;
        const next = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:self"),
        )!;
        const retry = yield* steersInto(orchestrator, next.id, 3);
        yield* worker.drain();
        yield* Fiber.join(retry);
        yield* worker.drain();
        assert.deepEqual(
          started.map((input) => withoutSenderNote(input.message.text)),
          ["first", "self"],
        );
        assert.deepEqual(steered, [
          [
            "3 queued messages, sent together:",
            "[1/3] From the user:\nuser",
            "[2/3] From thread thread:sender:\ncross-thread",
            '[3/3] From thread "Linked sender" (thread:linked) in linked environment environment:linked:\nlinked',
          ].join("\n\n"),
        ]);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.isFalse(after.runs.some((run) => run.status === "queued"));
      }).pipe(Effect.provide(layer));
    }),
);

const queueAgentMessages = (
  orchestrator: Orchestrator.OrchestratorV2["Service"],
  threadId: ThreadId,
  name: string,
  count: number,
) =>
  Effect.forEach(
    Array.from({ length: count }, (_, index) => index),
    (index) =>
      orchestrator
        .dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`queue-${name}-${index}`),
          threadId,
          messageId: MessageId.make(`message:${name}-${index}`),
          text: `${name} ${index}`,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "mcp",
        })
        .pipe(Effect.as(MessageId.make(`message:${name}-${index}`))),
  );

/** Gives every promotion sweep, idle-queue check and retry its turn, one tick at a time. */
const stepClock = (threadId: ThreadId, minutes: number, drain: boolean) =>
  Effect.gen(function* () {
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
    for (let step = 0; step < minutes * 12; step++) {
      yield* TestClock.adjust("5 seconds");
      yield* executor.withLock(threadId, Effect.void);
      if (drain) yield* worker.drain();
    }
  });

const ACTIVE_RUN_STATUSES = new Set(["preparing", "starting", "running", "waiting"]);

it.effect.each([
  { name: "queued only", promote: false, deliverBeforeStop: false },
  { name: "steered before the Stop", promote: true, deliverBeforeStop: true },
  { name: "steer still pending at the Stop", promote: true, deliverBeforeStop: false },
])(
  "a Stop holds every queued message automatic Send now has not delivered, and no steer reaches the stopped run ($name)",
  ({ name, promote, deliverBeforeStop }) =>
    Effect.gen(function* () {
      const { steered, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        `stop-holds-${name.replaceAll(" ", "-")}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!.runId;
        // Lagbench's held thread: agent messages queued behind a live turn, then Stop.
        const messageIds = yield* queueAgentMessages(orchestrator, threadId, "held", 30);
        if (promote) {
          const promoted = yield* steersInto(orchestrator, first, 25);
          yield* TestClock.adjust("61 seconds");
          yield* Fiber.join(promoted);
          if (deliverBeforeStop) yield* worker.drain();
        }
        assert.lengthOf(steered, deliverBeforeStop ? 1 : 0);
        const interrupted = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first &&
              event.payload.status === "interrupted",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("stop-held"),
          threadId,
          reason: "lagbench hold",
        });
        const queuedAt = (runs: OrchestrationV2ThreadProjection["runs"]) =>
          runs.filter((run) => run.status === "queued" && messageIds.includes(run.userMessageId));
        // The provider has not confirmed the Stop yet: its turn stays live while every
        // remaining message comes due.
        yield* stepClock(threadId, 10, false);
        const stopping = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(stopping.runs.find((run) => run.id === first)?.status, "running");
        assert.lengthOf(queuedAt(stopping.runs), promote ? 5 : 30);
        assert.isTrue(queuedAt(stopping.runs).every((run) => run.queueHeld === true));
        assert.lengthOf(steered, deliverBeforeStop ? 1 : 0);

        yield* worker.drain();
        yield* Fiber.join(interrupted);
        yield* stepClock(threadId, 10, true);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          after.runs.filter((run) => ACTIVE_RUN_STATUSES.has(run.status)),
          [],
        );
        const placement = messageIds.map((messageId) => {
          const runId = after.messages.find((message) => message.id === messageId)?.runId;
          const run = after.runs.find((candidate) => candidate.id === runId);
          if (runId === first) return "steered";
          return run?.status === "queued" && run.queueHeld === true
            ? run.steerFollowUpOfRunId === first
              ? "held again"
              : "held"
            : `${run?.status}`;
        });
        assert.deepEqual(
          placement,
          messageIds.map((_, index) =>
            index >= 25 || !promote ? "held" : deliverBeforeStop ? "steered" : "held again",
          ),
        );
        assert.lengthOf(steered, deliverBeforeStop ? 1 : 0);
        assert.lengthOf(started, 1);
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("automatic Send now never promotes a held message into a live turn", () =>
  Effect.gen(function* () {
    const { steered, started, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("held-never-promoted");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const sink = yield* EventSink.EventSinkV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!.runId;
      const messageIds = yield* queueAgentMessages(orchestrator, threadId, "kept", 3);
      const setHeld = (queueHeld: boolean) =>
        Effect.gen(function* () {
          const projection = yield* orchestrator.getThreadProjection(threadId);
          const now = yield* DateTime.now;
          yield* sink.write({
            events: projection.runs
              .filter((run) => run.status === "queued")
              .map((run) => ({
                id: EventId.make(`hold-${queueHeld}-${run.id}`),
                type: "run.updated" as const,
                threadId,
                runId: run.id,
                occurredAt: now,
                payload: { ...run, queueHeld },
              })),
          });
        });
      // A hold the orchestrator wrote while the turn runs, as its queue-start holds do.
      yield* setHeld(true);
      yield* stepClock(threadId, 10, true);
      const held = yield* orchestrator.getThreadProjection(threadId);
      assert.deepEqual(
        held.runs
          .filter((run) => messageIds.includes(run.userMessageId))
          .map((run) => `${run.status}:${run.queueHeld === true}`),
        ["queued:true", "queued:true", "queued:true"],
      );
      assert.deepEqual(steered, []);

      // Released, the same messages steer at the next tick.
      const promoted = yield* steersInto(orchestrator, first, 3);
      yield* setHeld(false);
      yield* TestClock.adjust("5 seconds");
      yield* Fiber.join(promoted);
      yield* worker.drain();
      assert.lengthOf(steered, 1);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("automatic Send now never steers into a run Stop reached, even after queue.resume", () =>
  Effect.gen(function* () {
    const { steered, started, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("stopping-never-steered");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const first = started[0]!.runId;
      const messageIds = yield* queueAgentMessages(orchestrator, threadId, "resumed", 3);
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make("stop-before-resume"),
        threadId,
      });
      yield* orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make("resume-while-stopping"),
        threadId,
      });
      // The provider has not confirmed the Stop; the resumed messages come due meanwhile.
      yield* stepClock(threadId, 10, false);
      const stopping = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(stopping.runs.find((run) => run.id === first)?.status, "running");
      assert.deepEqual(
        stopping.runs
          .filter((run) => messageIds.includes(run.userMessageId))
          .map((run) => `${run.status}:${run.queueHeld === true}`),
        ["queued:false", "queued:false", "queued:false"],
      );
      assert.deepEqual(steered, []);

      // Once the Stop lands, the resumed queue starts as its own run and the rest steer into it.
      yield* worker.drain();
      yield* stepClock(threadId, 1, true);
      assert.deepEqual(
        started.map((turn) => turn.message.text),
        ["first", "resumed 0"],
      );
      const after = yield* orchestrator.getThreadProjection(threadId);
      assert.equal(after.runs.find((run) => run.id === first)?.status, "interrupted");
      assert.deepEqual(
        messageIds.map((id) => after.messages.find((message) => message.id === id)?.runId),
        messageIds.map(() => started[1]!.runId),
      );
      assert.lengthOf(steered, 1);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("merges queued messages into a steer only for automatic Send now", () =>
  Effect.gen(function* () {
    const { steered, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "auto-send-now-merge-refused",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const threadId = yield* startFirstTurn;
      yield* queueFrom(orchestrator, threadId, "user", "user");
      yield* queueFrom(orchestrator, threadId, "cross-thread", "cross-thread");
      const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
        (run) => run.status === "queued",
      );
      const refused = yield* orchestrator
        .dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("client-merge"),
          threadId,
          queuedRunId: queued[0]!.id,
          mergedQueuedRunIds: [queued[1]!.id],
          targetRunId: started[0]!.runId,
        })
        .pipe(Effect.flip);
      assert.equal(refused._tag, "OrchestratorDispatchError");
      assert.include(String((refused as { readonly cause?: unknown }).cause), "cannot join");
      assert.deepEqual(steered, []);
      assert.lengthOf(
        (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
          (run) => run.status === "queued",
        ),
        2,
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect("steers a due message the moment the spacing ends, not at the next tick", () =>
  Effect.gen(function* () {
    const { steered, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "auto-send-now-spacing-end",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const queuedAt = yield* DateTime.now;
      yield* queueFrom(orchestrator, threadId, "user", "user");
      const promoted = yield* steersInto(orchestrator, started[0]!.runId, 1);
      // An automatic steer between two ticks, as a turn start can send one.
      yield* TestClock.adjust("2500 millis");
      const before = yield* orchestrator.getThreadProjection(threadId);
      const run = before.runs[0]!;
      const steeredAt = yield* DateTime.now;
      yield* (yield* EventSink.EventSinkV2).write({
        events: [
          {
            id: EventId.make("earlier-automatic-steer"),
            type: "turn-item.updated",
            threadId,
            runId: run.id,
            occurredAt: steeredAt,
            payload: {
              id: TurnItemId.make("item:earlier-automatic-steer"),
              threadId,
              runId: run.id,
              nodeId: run.rootNodeId!,
              providerThreadId: before.providerTurns[0]!.providerThreadId,
              providerTurnId: before.providerTurns[0]!.id,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 100,
              status: "completed",
              title: null,
              startedAt: steeredAt,
              completedAt: steeredAt,
              updatedAt: steeredAt,
              type: "user_message",
              createdBy: "agent",
              creationSource: "mcp",
              messageId: MessageId.make("message:earlier-automatic-steer"),
              inputIntent: "promoted_queued_to_steer",
              automaticDelivery: true,
              text: "earlier",
              attachments: [],
            },
          },
        ],
      });
      // The next tick after the spacing ends comes 2.5 s later.
      yield* TestClock.adjust("62500 millis");
      const [promotion] = Array.from(yield* Fiber.join(promoted));
      yield* worker.drain();
      assert.deepEqual(steered, ["user"]);
      assert.equal(Duration.toMillis(DateTime.distance(queuedAt, promotion!.occurredAt)), 62_500);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect(
  "retries an automatic promotion whose planning read failed once, delivering it once in the same turn",
  () =>
    Effect.gen(function* () {
      const { steered, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "auto-send-now-transient-read",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const threadId = yield* startFirstTurn;
        yield* queueFrom(orchestrator, threadId, "cross-thread", "cross-thread");
        yield* TestClock.adjust("55 seconds");
        yield* worker.drain();
        using readFailure = vi
          .spyOn(store, "getQueuedRunRecords")
          .mockImplementationOnce(() =>
            Effect.fail(new ProjectionStore.ProjectionStoreReadError({ threadId, cause: "busy" })),
          );
        yield* TestClock.adjust("5 seconds");
        yield* worker.drain();
        assert.equal(readFailure.mock.calls.length, 1);
        assert.deepEqual(steered, []);
        yield* TestClock.adjust("2 minutes");
        yield* worker.drain();
        assert.deepEqual(steered.map(withoutSenderNote), ["cross-thread"]);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.isFalse(after.runs.some((run) => run.status === "queued"));
        assert.equal(after.runs.find((run) => run.id === started[0]!.runId)?.status, "running");
      }).pipe(Effect.provide(layer));
    }),
);

it.effect(
  "delivers an overdue message while a new slash command queues before every automatic steer",
  () =>
    Effect.gen(function* () {
      const { steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "auto-send-now-command-riders",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* queueFrom(orchestrator, threadId, "cross-thread", "cross-thread");
        for (const round of [1, 2, 3, 4]) {
          yield* TestClock.adjust(round === 1 ? "55 seconds" : "60 seconds");
          yield* worker.drain();
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`review-${round}`),
            threadId,
            messageId: MessageId.make(`message:review-${round}`),
            text: `/review round ${round}`,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
        }
        yield* TestClock.adjust("2 minutes");
        yield* worker.drain();
        // Slash commands never steer: each waits for a turn of its own.
        assert.deepEqual(steered.map(withoutSenderNote), ["cross-thread"]);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          after.runs
            .filter((run) => run.status === "queued")
            .map((run) => after.messages.find((message) => message.id === run.userMessageId)?.text)
            .toSorted(),
          ["/review round 1", "/review round 2", "/review round 3", "/review round 4"],
        );
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("promotes once per turn start, however often the running turn reports", () =>
  Effect.gen(function* () {
    const { events, steered, started, layer, startFirstTurn } =
      yield* nextTurnSelectionHarness("auto-send-now-snapshots");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const queuedAt = yield* DateTime.now;
      yield* queueFrom(orchestrator, threadId, "user", "user");
      const promoted = yield* steersInto(orchestrator, started[0]!.runId, 2);
      yield* TestClock.adjust("60 seconds");
      yield* worker.drain();
      // Queued after that steer, it waits out its grace and the spacing.
      yield* queueFrom(orchestrator, threadId, "cross-thread", "cross-thread");
      const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
      const reported = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) => event.type === "provider-turn.updated" && event.payload.model === "reported-3",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      for (const index of [1, 2, 3])
        yield* Queue.offer(events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, model: `reported-${index}` },
        });
      yield* Fiber.join(reported);
      yield* worker.drain();
      yield* TestClock.adjust("60 seconds");
      const promotions = Array.from(yield* Fiber.join(promoted));
      yield* worker.drain();
      assert.deepEqual(steered.map(withoutSenderNote), ["user", "cross-thread"]);
      assert.deepEqual(
        promotions.map((event) => Duration.toMillis(DateTime.distance(queuedAt, event.occurredAt))),
        [60_000, 120_000],
      );
    }).pipe(Effect.provide(layer));
  }),
);

it.effect(
  "steers into a turn that started in a gap within a tick while the turn-start reactor lags",
  () =>
    Effect.gen(function* () {
      const { events, steered, started, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "auto-send-now-reactor-lag",
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = yield* startFirstTurn;
        const before = yield* orchestrator.getThreadProjection(threadId);
        const run = before.runs[0]!;
        const turn = before.providerTurns[0]!;
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("lag-gap-turn"),
              type: "provider-turn.updated",
              threadId,
              occurredAt: now,
              payload: { ...turn, status: "pending" },
            },
          ],
        });
        yield* queueFrom(orchestrator, threadId, "cross-thread", "cross-thread");
        yield* TestClock.adjust("62 seconds");
        yield* worker.drain();
        assert.deepEqual(steered, []);
        // Turn reports react a few threads at a time, each under its thread's lock. Every slot is
        // taken by a thread whose lock is held, so this thread's turn start waits behind them.
        const release = yield* Deferred.make<void>();
        for (let index = 0; index < Orchestrator.REACTION_CONCURRENCY; index++) {
          const busyThreadId = ThreadId.make(`thread:auto-send-now-busy-${index}`);
          yield* orchestrator.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`create-busy-${index}`),
            threadId: busyThreadId,
            projectId: before.thread.projectId,
            title: "Busy",
            modelSelection: runSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            createdBy: "user",
            creationSource: "web",
          });
          const held = yield* Deferred.make<void>();
          yield* executor
            .withLock(
              busyThreadId,
              Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
            )
            .pipe(Effect.forkScoped);
          yield* Deferred.await(held);
          yield* sink.write({
            events: [
              {
                id: EventId.make(`lag-busy-turn-${index}`),
                type: "provider-turn.updated",
                threadId: busyThreadId,
                occurredAt: now,
                payload: {
                  ...turn,
                  id: ProviderTurnId.make(`provider-turn:busy-${index}`),
                  providerThreadId: ProviderThreadId.make(`provider-thread:busy-${index}`),
                  status: "running",
                },
              },
            ],
          });
        }
        const promoted = yield* steersInto(orchestrator, run.id, 1);
        const resumed = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.threadId === threadId &&
              event.payload.status === "running",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* Queue.offer(events, { type: "provider_turn.updated", driver, providerTurn: turn });
        yield* Fiber.join(resumed);
        yield* TestClock.adjust("5 seconds");
        const [promotion] = Array.from(yield* Fiber.join(promoted));
        yield* worker.drain();
        assert.deepEqual(steered.map(withoutSenderNote), ["cross-thread"]);
        assert.isAtMost(Duration.toMillis(DateTime.distance(now, promotion!.occurredAt)), 67_000);
        assert.lengthOf(started, 1);
        yield* Deferred.succeed(release, undefined);
      }).pipe(Effect.provide(layer));
    }),
);

it.effect("an automatic promotion into a turn that just ended is refused, never deferred", () =>
  Effect.gen(function* () {
    const { steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
      "auto-send-now-ended-turn",
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const threadId = yield* startFirstTurn;
      const before = yield* orchestrator.getThreadProjection(threadId);
      const run = before.runs[0]!;
      const turn = before.providerTurns[0]!;
      yield* (yield* EventSink.EventSinkV2).write({
        events: [
          {
            id: EventId.make("ended-turn"),
            type: "provider-turn.updated",
            threadId,
            occurredAt: yield* DateTime.now,
            payload: { ...turn, status: "completed" },
          },
        ],
      });
      yield* queueFrom(orchestrator, threadId, "automation", "automation");
      const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
        (candidate) => candidate.userMessageId === MessageId.make("message:automation"),
      )!;
      // Ingestion ended the turn after the sweep chose it, before its command ran.
      const error = yield* orchestrator
        .dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make(`command:auto-send-now:${queued.id}:${run.id}:${turn.id}`),
          threadId,
          queuedRunId: queued.id,
          targetRunId: run.id,
        })
        .pipe(Effect.flip);
      assert.equal(error._tag, "OrchestratorDispatchError");
      yield* worker.drain();
      const after = yield* orchestrator.getThreadProjection(threadId);
      const stillQueued = after.runs.find((candidate) => candidate.id === queued.id);
      assert.equal(stillQueued?.status, "queued");
      assert.isUndefined(stillQueued?.steerWhenReadyRunId);
      assert.deepEqual(steered, []);
    }).pipe(Effect.provide(layer));
  }),
);

// A delegated child whose original task settled: its parent's first follow-up runs as the
// child's turn, and later follow-ups queue behind it with tasks of their own.
const delegatedFollowupHarness = Effect.fn("delegatedFollowupHarness")(function* (
  name: string,
  steerAcknowledgement: Effect.Effect<
    void,
    ProviderAdapter.ProviderAdapterSteerRunError
  > = Effect.void,
) {
  const harness = yield* nextTurnSelectionHarness(
    name,
    false,
    true,
    Effect.void,
    true,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    Effect.void,
    steerAcknowledgement,
  );
  const parentThreadId = ThreadId.make(`thread:${name}:parent`);
  const childThreadId = ThreadId.make(`thread:${name}`);
  const parentRunId = RunId.make(`run:${name}:parent`);
  const originalTaskId = NodeId.make(`node:${name}:original`);
  /** The parent's thread_send to its child, or the user's own message there. */
  const send = Effect.fn("send")(function* (
    key: string,
    dispatchMode: Extract<OrchestrationV2Command, { type: "message.dispatch" }>["dispatchMode"],
    options: { readonly fromUser?: boolean; readonly deliveryIntent?: "auto" } = {},
  ) {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`send-${key}`),
      threadId: childThreadId,
      messageId: MessageId.make(`message:${key}`),
      text: key,
      attachments: [],
      dispatchMode,
      ...(options.deliveryIntent === undefined ? {} : { deliveryIntent: options.deliveryIntent }),
      ...(options.fromUser === true
        ? { createdBy: "user" as const, creationSource: "web" as const }
        : {
            senderThreadId: parentThreadId,
            createdBy: "agent" as const,
            creationSource: "mcp" as const,
          }),
    });
  });
  // Replays the child's events from the start, so one already written is found too.
  const childEvent = <A>(match: (event: OrchestrationV2DomainEvent) => A | undefined) =>
    Effect.gen(function* () {
      const sink = yield* EventSink.EventSinkV2;
      return yield* sink.stream({ threadId: childThreadId, afterSequence: 0 }).pipe(
        Stream.filterMap(({ event }) => {
          const value = match(event);
          return value === undefined ? Result.failVoid : Result.succeed(value);
        }),
        Stream.take(1),
        Stream.runCollect,
        Effect.map((values) => values[0]!),
      );
    });
  /** Starts the child's next queued run once its run-end reaction picked it. */
  const startRun = Effect.fn("startRun")(function* (runId: RunId) {
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const attemptId = yield* childEvent((event) =>
      event.type === "run.updated" &&
      event.payload.id === runId &&
      event.payload.status !== "queued"
        ? (event.payload.activeAttemptId ?? undefined)
        : undefined,
    );
    yield* worker.drain();
    yield* childEvent((event) =>
      event.type === "provider-turn.updated" &&
      event.payload.runAttemptId === attemptId &&
      event.payload.status === "running"
        ? true
        : undefined,
    );
  });
  const startChildTurn = Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create-${name}-parent`),
      threadId: parentThreadId,
      projectId: ProjectId.make(`project:${name}`),
      title: "Coordinator",
      modelSelection: runSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: harness.cwd,
      createdBy: "user",
      creationSource: "web",
    });
    const now = yield* DateTime.now;
    yield* sink.write({
      events: [
        {
          id: EventId.make(`${name}:parent-run`),
          type: "run.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          occurredAt: now,
          payload: {
            id: parentRunId,
            threadId: parentThreadId,
            ordinal: 1,
            providerInstanceId: instanceId,
            modelSelection: runSelection,
            providerThreadId: null,
            userMessageId: MessageId.make(`message:${name}:parent`),
            rootNodeId: NodeId.make(`node:${name}:parent-root`),
            activeAttemptId: null,
            status: "completed",
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
            delegatedCompletion: { disposition: "open", nextGeneration: 2, delivery: null },
          },
        },
        {
          id: EventId.make(`${name}:original-task`),
          type: "subagent.updated",
          threadId: parentThreadId,
          runId: parentRunId,
          nodeId: originalTaskId,
          occurredAt: now,
          payload: {
            id: originalTaskId,
            threadId: parentThreadId,
            runId: parentRunId,
            parentNodeId: NodeId.make(`node:${name}:parent-root`),
            origin: "app_owned",
            createdBy: "agent",
            driver,
            providerInstanceId: instanceId,
            providerThreadId: null,
            childThreadId,
            nativeTaskRef: null,
            prompt: "Start the work.",
            title: "Worker",
            model: null,
            completionWake: "always",
            completionDelivery: { state: "acknowledged", observedByRunId: parentRunId },
            status: "completed",
            result: "Started.",
            startedAt: now,
            completedAt: now,
            updatedAt: now,
          },
        },
        {
          id: EventId.make(`${name}:child`),
          type: "thread.created",
          threadId: childThreadId,
          occurredAt: now,
          payload: {
            createdBy: "agent",
            creationSource: "server",
            id: childThreadId,
            projectId: ProjectId.make(`project:${name}`),
            title: "Worker",
            providerInstanceId: instanceId,
            modelSelection: runSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: harness.cwd,
            activeProviderThreadId: null,
            lineage: {
              parentThreadId,
              relationshipToParent: "subagent",
              rootThreadId: parentThreadId,
            },
            forkedFrom: { type: "node", nodeId: originalTaskId },
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
        },
      ],
    });
    yield* send("first", { type: "start_immediately" });
    const firstRun = (yield* orchestrator.getThreadProjection(childThreadId)).runs[0]!;
    yield* startRun(firstRun.id);
    yield* (yield* ThreadCommandExecutor.ThreadCommandExecutor).withLock(
      childThreadId,
      Effect.void,
    );
  });
  /** The task the parent got for a message it sent its child. */
  const taskOf = Effect.fn("taskOf")(function* (key: string) {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const child = yield* orchestrator.getThreadProjection(childThreadId);
    const taskId = child.runs.find(
      (run) => run.userMessageId === MessageId.make(`message:${key}`),
    )?.delegatedTaskId;
    assert.isDefined(taskId, `no task for ${key}`);
    return taskId!;
  });
  /** The parent's next update of a task that matches, from now on. */
  const nextTaskUpdate = Effect.fn("nextTaskUpdate")(function* (
    taskId: NodeId,
    matches: (task: OrchestrationV2Subagent) => boolean,
  ) {
    const sink = yield* EventSink.EventSinkV2;
    const afterSequence = yield* sink.latestSequence({ threadId: parentThreadId });
    return yield* sink.stream({ threadId: parentThreadId, afterSequence }).pipe(
      Stream.filterMap(({ event }) =>
        event.type === "subagent.updated" && event.payload.id === taskId && matches(event.payload)
          ? Result.succeed(event.payload)
          : Result.failVoid,
      ),
      Stream.take(1),
      Stream.runCollect,
      Effect.map((updates) => updates[0]!),
      Effect.forkScoped,
    );
  });
  /** Ends a child turn with an answer; its checkpoint capture completes its run. */
  const endTurn = Effect.fn("endTurn")(function* (index: number, answer: string) {
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
    const sink = yield* EventSink.EventSinkV2;
    const turnInput = harness.started[index]!;
    const turn = yield* childEvent((event) =>
      event.type === "provider-turn.updated" && event.payload.runAttemptId === turnInput.attemptId
        ? event.payload
        : undefined,
    );
    const runReaches = (status: OrchestrationV2Run["status"]) =>
      Effect.flatMap(sink.latestSequence({ threadId: childThreadId }), (afterSequence) =>
        sink.stream({ threadId: childThreadId, afterSequence }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === turnInput.runId &&
              event.payload.status === status,
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        ),
      );
    const waiting = yield* runReaches("waiting");
    const now = yield* DateTime.now;
    yield* Queue.offer(harness.events, {
      type: "turn_item.updated",
      driver,
      turnItem: {
        id: TurnItemId.make(`answer:${turnInput.runId}`),
        threadId: childThreadId,
        runId: turnInput.runId,
        nodeId: turnInput.rootNodeId,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 0,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "assistant_message",
        messageId: MessageId.make(`message:answer:${turnInput.runId}`),
        text: answer,
        streaming: false,
      },
    });
    yield* Queue.offer(harness.events, {
      type: "provider_turn.updated",
      driver,
      providerTurn: { ...turn, status: "completed", completedAt: now },
    });
    yield* Queue.offer(harness.events, {
      type: "turn.terminal",
      driver,
      providerThreadId: turn.providerThreadId,
      providerTurnId: turn.id,
      runOrdinal: turnInput.runOrdinal,
      status: "completed",
      failure: null,
      threadDisposition: "reusable",
    });
    yield* Fiber.join(waiting);
    // Its checkpoint capture ends the wait.
    const completed = yield* runReaches("completed");
    yield* worker.drain();
    yield* Fiber.join(completed);
  });
  /** What the parent holds for the child: its tasks, result transfers and wake cohort. */
  const parentView = Effect.gen(function* () {
    const parent = yield* (yield* Orchestrator.OrchestratorV2).getThreadProjection(parentThreadId);
    return {
      tasks: parent.subagents.filter((task) => task.childThreadId === childThreadId),
      transfers: parent.contextTransfers.filter(
        (transfer) =>
          transfer.type === "subagent_result" && transfer.sourceThreadId === childThreadId,
      ),
      delivery:
        parent.runs.find((run) => run.id === parentRunId)?.delegatedCompletion?.delivery ?? null,
    };
  });
  return {
    ...harness,
    parentThreadId,
    childThreadId,
    originalTaskId,
    send,
    startRun,
    startChildTurn,
    taskOf,
    nextTaskUpdate,
    endTurn,
    parentView,
  };
});

// A queued follow-up that automatic Send now (A) or a deferred steer (B) promotes into the
// child's running turn cancels its own run before it starts. Its task settles with the turn it
// joined, which got the message, never as cancelled.
it.effect.each(["automatic Send now", "a deferred steer"] as const)(
  "a parent's follow-up that %s steers into its child's turn settles with that turn, told once",
  (promotion) =>
    Effect.gen(function* () {
      const harness = yield* delegatedFollowupHarness(
        `r1-${promotion === "automatic Send now" ? "auto-send-now" : "deferred-steer"}`,
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        yield* harness.startChildTurn;
        const first = harness.started[0]!;
        const firstTaskId = yield* harness.taskOf("first");
        const live = yield* orchestrator.getThreadProjection(harness.childThreadId);
        const run = live.runs.find((candidate) => candidate.id === first.runId)!;
        const turn = live.providerTurns.find(
          (candidate) => candidate.runAttemptId === first.attemptId,
        )!;
        if (promotion === "automatic Send now") {
          yield* harness.send("second", { type: "queue_after_active" });
        } else {
          // The child's turn is between reports; an automatic send waits for it to run again.
          const now = yield* DateTime.now;
          yield* sink.write({
            events: [
              {
                id: EventId.make("r1-gap-run"),
                type: "run.updated",
                threadId: harness.childThreadId,
                occurredAt: now,
                payload: { ...run, status: "starting" },
              },
              {
                id: EventId.make("r1-gap-turn"),
                type: "provider-turn.updated",
                threadId: harness.childThreadId,
                occurredAt: now,
                payload: { ...turn, status: "pending" },
              },
            ],
          });
          yield* harness.send(
            "second",
            { type: "steer_active", targetRunId: first.runId },
            { deliveryIntent: "auto" },
          );
          const deferred = (yield* orchestrator.getThreadProjection(
            harness.childThreadId,
          )).runs.find((candidate) => candidate.userMessageId === MessageId.make("message:second"));
          assert.equal(deferred?.steerWhenReadyRunId, first.runId);
          yield* sink.write({
            events: [
              {
                id: EventId.make("r1-ready-run"),
                type: "run.updated",
                threadId: harness.childThreadId,
                occurredAt: now,
                payload: run,
              },
            ],
          });
        }
        const taskId = yield* harness.taskOf("second");
        assert.equal(
          (yield* harness.parentView).tasks.find((task) => task.id === taskId)?.status,
          "pending",
        );
        const promoted = yield* steersInto(orchestrator, first.runId, 1);
        const joined = yield* harness.nextTaskUpdate(taskId, (task) => task.status !== "pending");
        if (promotion === "automatic Send now") {
          yield* TestClock.adjust("61 seconds");
        } else {
          yield* Queue.offer(harness.events, {
            type: "provider_turn.updated",
            driver,
            providerTurn: turn,
          });
        }
        yield* Fiber.join(promoted);
        yield* worker.drain();
        assert.deepEqual(harness.steered.map(withoutSenderNote), ["second"]);
        // The parent hears nothing yet: the child is still answering in the turn it joined.
        const running = yield* Fiber.join(joined);
        assert.equal(running.status, "running");
        assert.isNull(running.result);

        const settled = yield* harness.nextTaskUpdate(taskId, (task) => task.result !== null);
        yield* harness.endTurn(0, "Both follow-ups answered.");
        const answered = yield* Fiber.join(settled);
        assert.equal(answered.status, "completed");
        assert.equal(answered.result, "Both follow-ups answered.");

        const view = yield* harness.parentView;
        assert.deepEqual(
          view.tasks.map((task) => [task.id, task.status, task.result]).toSorted(),
          [
            [harness.originalTaskId, "completed", "Started."],
            [firstTaskId, "completed", "Both follow-ups answered."],
            [taskId, "completed", "Both follow-ups answered."],
          ].toSorted(),
        );
        // One result for the shared turn, and one wake that names each task once.
        assert.deepEqual(
          view.transfers.map((transfer) => transfer.sourcePoint.runId),
          [first.runId],
        );
        assert.deepEqual(view.delivery?.taskIds.toSorted(), [firstTaskId, taskId].toSorted());
        assert.lengthOf(harness.started, 1);
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect(
  "a merged automatic steer settles each follow-up it carries once, with the turn they joined",
  () =>
    Effect.gen(function* () {
      const harness = yield* delegatedFollowupHarness("r1-merged");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* harness.startChildTurn;
        const first = harness.started[0]!;
        const firstTaskId = yield* harness.taskOf("first");
        // The parent's follow-up and the user's own message to the child each get a task.
        yield* harness.send("second", { type: "queue_after_active" });
        yield* harness.send("third", { type: "queue_after_active" }, { fromUser: true });
        const secondTaskId = yield* harness.taskOf("second");
        const thirdTaskId = yield* harness.taskOf("third");
        const promoted = yield* steersInto(orchestrator, first.runId, 2);
        const joined = yield* Effect.forEach([secondTaskId, thirdTaskId], (taskId) =>
          harness.nextTaskUpdate(taskId, (task) => task.status !== "pending"),
        );
        yield* TestClock.adjust("61 seconds");
        yield* Fiber.join(promoted);
        yield* worker.drain();
        assert.deepEqual(harness.steered, [
          [
            "2 queued messages, sent together:",
            "[1/2] From the user:\nthird",
            `[2/2] From thread ${harness.parentThreadId}:\nsecond`,
          ].join("\n\n"),
        ]);
        assert.deepEqual(
          (yield* Effect.forEach(joined, Fiber.join)).map((task) => [task.status, task.result]),
          [
            ["running", null],
            ["running", null],
          ],
        );

        const settled = yield* Effect.forEach([secondTaskId, thirdTaskId], (taskId) =>
          harness.nextTaskUpdate(taskId, (task) => task.result !== null),
        );
        yield* harness.endTurn(0, "All three answered.");
        yield* Effect.forEach(settled, Fiber.join);
        // Recovery finds nothing left to settle or tell.
        const before = yield* harness.parentView;
        yield* orchestrator.recoverDelegatedTasks;
        const view = yield* harness.parentView;
        assert.deepEqual(view, before);
        assert.deepEqual(
          view.tasks
            .filter((task) => task.id !== harness.originalTaskId)
            .map((task) => [task.id, task.status, task.result])
            .toSorted(),
          [firstTaskId, secondTaskId, thirdTaskId]
            .map((id) => [id, "completed", "All three answered."])
            .toSorted(),
        );
        assert.deepEqual(
          view.transfers.map((transfer) => transfer.sourcePoint.runId),
          [first.runId],
        );
        assert.deepEqual(
          view.delivery?.taskIds.toSorted(),
          [firstTaskId, secondTaskId, thirdTaskId].toSorted(),
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect(
  "a promoted follow-up whose steer misses the turn settles once, from the run it queues again as",
  () =>
    Effect.gen(function* () {
      let attempts = 0;
      const harness = yield* delegatedFollowupHarness(
        "r1-missed",
        Effect.suspend(() =>
          attempts++ > 0
            ? Effect.void
            : Effect.fail(
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver,
                  providerThreadId: ProviderThreadId.make("provider-thread:r1-missed"),
                  providerTurnId: ProviderTurnId.make("provider-turn:r1-missed"),
                  deliveryNotSent: true,
                  cause: "The turn ended before the steer reached it",
                }),
              ),
        ),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* harness.startChildTurn;
        const first = harness.started[0]!;
        const firstTaskId = yield* harness.taskOf("first");
        yield* harness.send("second", { type: "queue_after_active" });
        const taskId = yield* harness.taskOf("second");
        const promoted = yield* steersInto(orchestrator, first.runId, 1);
        yield* TestClock.adjust("61 seconds");
        yield* Fiber.join(promoted);
        yield* worker.drain();
        assert.deepEqual(harness.steered, []);
        // The message queues again, as the same follow-up.
        const again = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs.find(
          (run) =>
            run.status === "queued" && run.userMessageId === MessageId.make("message:second"),
        );
        assert.equal(again?.steerFollowUpOfRunId, first.runId);
        assert.equal(again?.delegatedTaskId, taskId);

        yield* harness.endTurn(0, "First answered.");
        yield* harness.startRun(again!.id);
        assert.deepEqual(
          harness.started.map((turn) => withoutSenderNote(turn.message.text)),
          ["first", "second"],
        );
        const settled = yield* harness.nextTaskUpdate(taskId, (task) => task.result !== null);
        yield* harness.endTurn(1, "Second answered.");
        yield* Fiber.join(settled);
        const view = yield* harness.parentView;
        assert.deepEqual(
          view.tasks.map((task) => [task.id, task.status, task.result]).toSorted(),
          [
            [harness.originalTaskId, "completed", "Started."],
            [firstTaskId, "completed", "First answered."],
            [taskId, "completed", "Second answered."],
          ].toSorted(),
        );
        assert.deepEqual(
          view.transfers.map((transfer) => transfer.sourcePoint.runId).toSorted(),
          [first.runId, again!.id].toSorted(),
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect.each([
  { name: "steered before the Stop", deliverBeforeStop: true },
  { name: "steer still pending at the Stop", deliverBeforeStop: false },
])("a Stop leaves a promoted follow-up's task with its message ($name)", ({ deliverBeforeStop }) =>
  Effect.gen(function* () {
    const harness = yield* delegatedFollowupHarness(`r1-stop-${deliverBeforeStop}`);
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      yield* harness.startChildTurn;
      const first = harness.started[0]!;
      const firstTaskId = yield* harness.taskOf("first");
      yield* harness.send("second", { type: "queue_after_active" });
      const taskId = yield* harness.taskOf("second");
      const promoted = yield* steersInto(orchestrator, first.runId, 1);
      yield* TestClock.adjust("61 seconds");
      yield* Fiber.join(promoted);
      if (deliverBeforeStop) yield* worker.drain();
      assert.lengthOf(harness.steered, deliverBeforeStop ? 1 : 0);
      const interrupted = yield* orchestrator.streamDomainEvents.pipe(
        Stream.filter(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "interrupted",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      const firstStopped = yield* harness.nextTaskUpdate(
        firstTaskId,
        (task) => task.result !== null,
      );
      yield* orchestrator.dispatch({
        type: "thread.stop",
        commandId: CommandId.make(`stop-${deliverBeforeStop}`),
        threadId: harness.childThreadId,
        reason: "held by the user",
      });
      yield* worker.drain();
      yield* Fiber.join(interrupted);
      assert.equal((yield* Fiber.join(firstStopped)).status, "interrupted");
      yield* stepClock(harness.childThreadId, 1, true);
      const task = (yield* harness.parentView).tasks.find((candidate) => candidate.id === taskId);
      if (deliverBeforeStop) {
        // The child had the message when it was stopped; the task ends with that turn.
        assert.equal(task?.status, "interrupted");
        // The result keeps the reason the child was stopped with.
        assert.equal(task?.result, "Child task ended with status interrupted.\n\nheld by the user");
        assert.lengthOf(harness.started, 1);
        return;
      }
      // The message is held again, and its task waits with it until the queue resumes.
      const held = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs.find(
        (run) => run.status === "queued" && run.userMessageId === MessageId.make("message:second"),
      )!;
      assert.equal(held.queueHeld, true);
      assert.equal(task?.status, "idle");
      assert.isNull(task?.result ?? null);
      yield* orchestrator.dispatch({
        type: "queue.resume",
        commandId: CommandId.make(`resume-${deliverBeforeStop}`),
        threadId: harness.childThreadId,
      });
      yield* harness.startRun(held.id);
      assert.deepEqual(
        harness.started.map((turn) => withoutSenderNote(turn.message.text)),
        ["first", "second"],
      );
      const settled = yield* harness.nextTaskUpdate(
        taskId,
        (candidate) => candidate.result !== null,
      );
      yield* harness.endTurn(1, "Second answered after the Stop.");
      const answered = yield* Fiber.join(settled);
      assert.equal(answered.status, "completed");
      assert.equal(answered.result, "Second answered after the Stop.");
      assert.lengthOf((yield* harness.parentView).tasks, 3);
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect(
  "a promoted follow-up waits for its steer when the turn it joined ends before the steer is sent",
  () =>
    Effect.gen(function* () {
      const harness = yield* delegatedFollowupHarness("r1-ended-first");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const sink = yield* EventSink.EventSinkV2;
        yield* harness.startChildTurn;
        const first = harness.started[0]!;
        const firstTaskId = yield* harness.taskOf("first");
        yield* harness.send("second", { type: "queue_after_active" });
        const taskId = yield* harness.taskOf("second");
        const promoted = yield* steersInto(orchestrator, first.runId, 1);
        yield* TestClock.adjust("61 seconds");
        yield* Fiber.join(promoted);
        // The steer is still to be sent when the turn ends, and the user's message waiting behind
        // the turn ends its run at once.
        yield* harness.send("third", { type: "queue_after_active" }, { fromUser: true });
        const thirdTaskId = yield* harness.taskOf("third");
        const firstSettled = yield* harness.nextTaskUpdate(
          firstTaskId,
          (task) => task.result !== null,
        );
        const turn = (yield* orchestrator.getThreadProjection(harness.childThreadId))
          .providerTurns[0]!;
        const afterSequence = yield* sink.latestSequence({ threadId: harness.childThreadId });
        const ended = yield* sink.stream({ threadId: harness.childThreadId, afterSequence }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const now = yield* DateTime.now;
        yield* Queue.offer(harness.events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: now },
        });
        yield* Queue.offer(harness.events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(ended);
        yield* Fiber.join(firstSettled);
        yield* executor.withLock(harness.parentThreadId, Effect.void);
        // The turn ended without the message, so its follow-up has no answer yet.
        const waiting = (yield* harness.parentView).tasks.find((task) => task.id === taskId);
        assert.isNull(waiting?.result ?? null);

        // The steer finds the turn over and queues the message again, as the same follow-up. It
        // is due at once, so it steers into the next turn: the user's message's.
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* worker.drain();
        assert.deepEqual(harness.steered, []);
        const runs = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs;
        const third = runs.find((run) => run.userMessageId === MessageId.make("message:third"))!;
        const again = runs.find(
          (run) =>
            run.status === "queued" && run.userMessageId === MessageId.make("message:second"),
        )!;
        assert.equal(again.delegatedTaskId, taskId);
        const steeredAgain = yield* steersInto(orchestrator, third.id, 1);
        yield* harness.startRun(third.id);
        yield* Fiber.join(steeredAgain);
        yield* worker.drain();
        assert.deepEqual(harness.steered.map(withoutSenderNote), ["second"]);
        const settled = yield* harness.nextTaskUpdate(taskId, (task) => task.result !== null);
        yield* harness.endTurn(1, "Third and second answered.");
        const answered = yield* Fiber.join(settled);
        assert.equal(answered.status, "completed");
        assert.equal(answered.result, "Third and second answered.");
        const tasks = (yield* harness.parentView).tasks;
        assert.deepEqual(
          tasks.map((task) => task.id).toSorted(),
          [harness.originalTaskId, firstTaskId, taskId, thirdTaskId].toSorted(),
        );
        assert.equal(
          tasks.find((task) => task.id === thirdTaskId)?.result,
          "Third and second answered.",
        );
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect("a promoted follow-up held for its steer settles once that steer fails for good", () =>
  Effect.gen(function* () {
    const harness = yield* delegatedFollowupHarness(
      "r1-steer-fails",
      Effect.fail(
        new ProviderAdapter.ProviderAdapterSteerRunError({
          driver,
          providerThreadId: ProviderThreadId.make("provider-thread:r1-steer-fails"),
          providerTurnId: ProviderTurnId.make("provider-turn:r1-steer-fails"),
          cause: "The provider dropped the connection mid-steer",
        }),
      ),
    );
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
      const sink = yield* EventSink.EventSinkV2;
      yield* harness.startChildTurn;
      const first = harness.started[0]!;
      const firstTaskId = yield* harness.taskOf("first");
      yield* harness.send("second", { type: "queue_after_active" });
      const taskId = yield* harness.taskOf("second");
      const promoted = yield* steersInto(orchestrator, first.runId, 1);
      yield* TestClock.adjust("61 seconds");
      yield* Fiber.join(promoted);
      // The provider fails mid-steer, so whether the message arrived is unknown.
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      yield* worker.drain();
      assert.deepEqual(harness.steered, []);
      // The turn then fails, so no checkpoint capture follows it to finalize the child again.
      const firstSettled = yield* harness.nextTaskUpdate(
        firstTaskId,
        (task) => task.result !== null,
      );
      const turn = (yield* orchestrator.getThreadProjection(harness.childThreadId))
        .providerTurns[0]!;
      const afterSequence = yield* sink.latestSequence({ threadId: harness.childThreadId });
      const ended = yield* sink.stream({ threadId: harness.childThreadId, afterSequence }).pipe(
        Stream.filter(
          ({ event }) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "failed",
        ),
        Stream.take(1),
        Stream.runDrain,
        Effect.forkScoped,
      );
      const now = yield* DateTime.now;
      yield* Queue.offer(harness.events, {
        type: "provider_turn.updated",
        driver,
        providerTurn: { ...turn, status: "failed", completedAt: now },
      });
      yield* Queue.offer(harness.events, {
        type: "turn.terminal",
        driver,
        providerThreadId: turn.providerThreadId,
        providerTurnId: turn.id,
        runOrdinal: first.runOrdinal,
        status: "failed",
        failure: makeProviderFailure({ message: "The provider crashed", class: "provider_error" }),
        failureItemOrdinal: 100,
        threadDisposition: "reusable",
      });
      yield* Fiber.join(ended);
      yield* Fiber.join(firstSettled);
      yield* executor.withLock(harness.parentThreadId, Effect.void);
      assert.isNull(
        (yield* harness.parentView).tasks.find((task) => task.id === taskId)?.result ?? null,
      );

      // The steer is never sent again while its delivery is uncertain; once it has failed for
      // good, the follow-up settles with the turn it joined.
      const settled = yield* harness.nextTaskUpdate(taskId, (task) => task.result !== null);
      yield* stepClock(harness.childThreadId, 2, true);
      assert.deepEqual(harness.steered, []);
      const answered = yield* Fiber.join(settled);
      assert.equal(answered.status, "failed");
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect("cancelling a promoted follow-up before its steer runs leaves nothing of it queued", () =>
  Effect.gen(function* () {
    const harness = yield* delegatedFollowupHarness("r1-cancel-undelivered");
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      yield* harness.startChildTurn;
      const first = harness.started[0]!;
      yield* harness.send("second", { type: "queue_after_active" });
      const taskId = yield* harness.taskOf("second");
      const promoted = yield* steersInto(orchestrator, first.runId, 1);
      yield* TestClock.adjust("61 seconds");
      yield* Fiber.join(promoted);
      // The parent cancels the follow-up while the steer carrying it is still to run.
      yield* orchestrator.cancelDelegatedTask({
        parentThreadId: harness.parentThreadId,
        taskId,
        command: {
          type: "thread.stop",
          commandId: CommandId.make("cancel-undelivered"),
          threadId: harness.childThreadId,
          stopDelegatedTasks: false,
        },
        stopDescendants: () => Effect.void,
      });
      yield* worker.drain();
      yield* stepClock(harness.childThreadId, 1, true);
      assert.deepEqual(harness.steered, []);
      const child = yield* orchestrator.getThreadProjection(harness.childThreadId);
      assert.deepEqual(
        child.runs
          .filter(
            (run) =>
              run.status === "queued" && run.userMessageId === MessageId.make("message:second"),
          )
          .map((run) => run.id),
        [],
      );
      const cancelled = (yield* harness.parentView).tasks.find((task) => task.id === taskId);
      assert.equal(cancelled?.status, "interrupted");
      assert.equal(cancelled?.completionDelivery?.state, "disposed");
    }).pipe(Effect.provide(harness.layer));
  }),
);

it.effect(
  "a self-send sent into another task's turn queues again as its own task when its steer misses",
  () =>
    Effect.gen(function* () {
      let attempts = 0;
      const harness = yield* delegatedFollowupHarness(
        "r1-self-send",
        Effect.suspend(() =>
          attempts++ > 0
            ? Effect.void
            : Effect.fail(
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver,
                  providerThreadId: ProviderThreadId.make("provider-thread:r1-self-send"),
                  providerTurnId: ProviderTurnId.make("provider-turn:r1-self-send"),
                  deliveryNotSent: true,
                  cause: "The turn ended before the steer reached it",
                }),
              ),
        ),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* harness.startChildTurn;
        const firstTaskId = yield* harness.taskOf("first");
        // The child queues a message to itself, which continues its running follow-up.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("send-self"),
          threadId: harness.childThreadId,
          messageId: MessageId.make("message:self"),
          text: "self",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          senderThreadId: harness.childThreadId,
          createdBy: "agent",
          creationSource: "mcp",
        });
        assert.equal(yield* harness.taskOf("self"), firstTaskId);
        // The parent's next follow-up is moved ahead of it, so it runs next.
        yield* harness.send("second", { type: "queue_after_active" });
        const secondTaskId = yield* harness.taskOf("second");
        const queued = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs;
        const self = queued.find((run) => run.userMessageId === MessageId.make("message:self"))!;
        const second = queued.find(
          (run) => run.userMessageId === MessageId.make("message:second"),
        )!;
        yield* orchestrator.dispatch({
          type: "queued-run.reorder",
          commandId: CommandId.make("reorder-second"),
          threadId: harness.childThreadId,
          runId: second.id,
          beforeRunId: self.id,
        });
        yield* harness.endTurn(0, "First answered.");
        yield* harness.startRun(second.id);
        // The user sends the self-send into that turn; the steer misses it.
        const joined = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs.find(
          (run) => run.id === second.id,
        )!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("send-now-self"),
          threadId: harness.childThreadId,
          queuedRunId: self.id,
          targetRunId: joined.id,
        });
        yield* worker.drain();
        assert.deepEqual(harness.steered, []);
        const again = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs.find(
          (run) => run.status === "queued" && run.userMessageId === MessageId.make("message:self"),
        );
        assert.notEqual(again?.delegatedTaskId, secondTaskId);
        assert.equal(again?.delegatedTaskId, firstTaskId);
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect(
  "a resumed self-send of a stopped task, sent into another task's turn, queues again when its steer misses",
  () =>
    Effect.gen(function* () {
      let attempts = 0;
      const harness = yield* delegatedFollowupHarness(
        "r1-stopped-self-send",
        Effect.suspend(() =>
          attempts++ > 0
            ? Effect.void
            : Effect.fail(
                new ProviderAdapter.ProviderAdapterSteerRunError({
                  driver,
                  providerThreadId: ProviderThreadId.make("provider-thread:r1-stopped-self-send"),
                  providerTurnId: ProviderTurnId.make("provider-turn:r1-stopped-self-send"),
                  deliveryNotSent: true,
                  cause: "The turn ended before the steer reached it",
                }),
              ),
        ),
      );
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* harness.startChildTurn;
        const first = harness.started[0]!;
        const firstTaskId = yield* harness.taskOf("first");
        // The child queues a message to itself, which continues its running follow-up.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("send-self"),
          threadId: harness.childThreadId,
          messageId: MessageId.make("message:self"),
          text: "self",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          senderThreadId: harness.childThreadId,
          createdBy: "agent",
          creationSource: "mcp",
        });
        assert.equal(yield* harness.taskOf("self"), firstTaskId);
        // The parent's next follow-up is moved ahead of it.
        yield* harness.send("second", { type: "queue_after_active" });
        const queued = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs;
        const self = queued.find((run) => run.userMessageId === MessageId.make("message:self"))!;
        const second = queued.find(
          (run) => run.userMessageId === MessageId.make("message:second"),
        )!;
        yield* orchestrator.dispatch({
          type: "queued-run.reorder",
          commandId: CommandId.make("reorder-second"),
          threadId: harness.childThreadId,
          runId: second.id,
          beforeRunId: self.id,
        });
        // Stop holds both queued messages, and the first task ends interrupted.
        const firstSettled = yield* harness.nextTaskUpdate(
          firstTaskId,
          (task) => task.result !== null,
        );
        const interrupted = yield* orchestrator.streamDomainEvents.pipe(
          Stream.filter(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "interrupted",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("stop-first"),
          threadId: harness.childThreadId,
          reason: "held by the user",
        });
        yield* worker.drain();
        yield* Fiber.join(interrupted);
        assert.equal((yield* Fiber.join(firstSettled)).status, "interrupted");
        // The user resumes the queue; the parent's follow-up starts, and the user sends the
        // retained self-send into it. The steer misses its turn.
        yield* orchestrator.dispatch({
          type: "queue.resume",
          commandId: CommandId.make("resume-queue"),
          threadId: harness.childThreadId,
        });
        yield* harness.startRun(second.id);
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("send-now-self"),
          threadId: harness.childThreadId,
          queuedRunId: self.id,
          targetRunId: second.id,
        });
        yield* worker.drain();
        assert.deepEqual(harness.steered, []);
        // The message is queued again, not lost because its old task has ended.
        const again = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs.find(
          (run) => run.status === "queued" && run.userMessageId === MessageId.make("message:self"),
        );
        assert.isDefined(again);
        assert.equal(again?.delegatedTaskId, firstTaskId);
      }).pipe(Effect.provide(harness.layer));
    }),
);

// A task moved on to its next run while the steer carrying an earlier self-send of it is still
// to run, and the turn that steer was sent into has ended.
it.effect.each([
  {
    name: "task_cancel after its next run started queues nothing of the self-send",
    userFirst: false,
    cancel: true,
  },
  {
    name: "task_cancel while its next run waits behind a user's turn settles it cancelled",
    userFirst: true,
    cancel: true,
  },
  {
    name: "an interrupt of its next run leaves it waiting for the self-send's answer",
    userFirst: false,
    cancel: false,
  },
])(
  "a task that moved on past an earlier self-send's pending steer: $name",
  ({ userFirst, cancel }) =>
    Effect.gen(function* () {
      const harness = yield* delegatedFollowupHarness(`r1-moved-on-${userFirst}-${cancel}`);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        yield* harness.startChildTurn;
        const first = harness.started[0]!;
        const firstTaskId = yield* harness.taskOf("first");
        // The child queues two messages to itself; both continue its running follow-up. The user's
        // message can be queued between them.
        for (const name of userFirst ? ["self-a", "user", "self-b"] : ["self-a", "self-b"])
          if (name === "user")
            yield* harness.send("user", { type: "queue_after_active" }, { fromUser: true });
          else
            yield* orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`send-${name}`),
              threadId: harness.childThreadId,
              messageId: MessageId.make(`message:${name}`),
              text: name,
              attachments: [],
              dispatchMode: { type: "queue_after_active" },
              senderThreadId: harness.childThreadId,
              createdBy: "agent",
              creationSource: "mcp",
            });
        const queued = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs;
        const selfA = queued.find((run) => run.userMessageId === MessageId.make("message:self-a"))!;
        const next = queued.find(
          (run) => run.userMessageId === MessageId.make(`message:${userFirst ? "user" : "self-b"}`),
        )!;
        assert.equal(selfA.delegatedTaskId, firstTaskId);
        // The user sends the first into the running turn; the turn ends before that steer runs,
        // and the next queued message starts.
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          commandId: CommandId.make("send-now-self-a"),
          threadId: harness.childThreadId,
          queuedRunId: selfA.id,
          targetRunId: first.runId,
        });
        const turn = (yield* orchestrator.getThreadProjection(harness.childThreadId))
          .providerTurns[0]!;
        const afterSequence = yield* sink.latestSequence({ threadId: harness.childThreadId });
        const nextStarted = yield* sink
          .stream({ threadId: harness.childThreadId, afterSequence })
          .pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.id === next.id &&
                event.payload.status !== "queued",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        const now = yield* DateTime.now;
        yield* Queue.offer(harness.events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: now },
        });
        yield* Queue.offer(harness.events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(nextStarted);
        if (!cancel) {
          // The user interrupts the task's next run before the first self-send's steer runs.
          const settled = yield* harness.nextTaskUpdate(
            firstTaskId,
            (task) => task.result !== null,
          );
          yield* orchestrator.dispatch({
            type: "run.interrupt",
            commandId: CommandId.make("interrupt-moved-on"),
            threadId: harness.childThreadId,
            runId: next.id,
          });
          yield* executor.withLock(harness.parentThreadId, Effect.void);
          // The self-send has no answer yet, so the task does not settle from the interrupted run.
          assert.isNull(
            (yield* harness.parentView).tasks.find((task) => task.id === firstTaskId)?.result ??
              null,
          );
          // Its steer finds the turn over and queues it again, as the same task, which settles
          // once from it.
          yield* worker.drain();
          assert.deepEqual(harness.steered, []);
          const again = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs.find(
            (run) =>
              run.userMessageId === MessageId.make("message:self-a") &&
              run.id !== selfA.id &&
              run.status !== "cancelled",
          )!;
          assert.equal(again.delegatedTaskId, firstTaskId);
          yield* harness.startRun(again.id);
          yield* harness.endTurn(harness.started.length - 1, "Self-a answered.");
          const answered = yield* Fiber.join(settled);
          assert.equal(answered.status, "completed");
          assert.equal(answered.result, "Self-a answered.");
          return;
        }
        // The parent cancels the task now, before the first self-send's steer runs.
        yield* orchestrator.cancelDelegatedTask({
          parentThreadId: harness.parentThreadId,
          taskId: firstTaskId,
          command: {
            type: "thread.stop",
            commandId: CommandId.make("cancel-moved-on"),
            threadId: harness.childThreadId,
            stopDelegatedTasks: false,
          },
          stopDescendants: () => Effect.void,
        });
        yield* worker.drain();
        yield* stepClock(harness.childThreadId, 1, true);
        assert.deepEqual(harness.steered, []);
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs
            .filter(
              (run) =>
                run.status === "queued" && run.userMessageId === MessageId.make("message:self-a"),
            )
            .map((run) => run.id),
          [],
        );
        // The task ends cancelled, and its parent is woken for none of it.
        const cancelled = (yield* harness.parentView).tasks.find((task) => task.id === firstTaskId);
        assert.equal(cancelled?.status, "interrupted");
        assert.equal(cancelled?.completionDelivery?.state, "disposed");
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect(
  "cancelling a promoted follow-up held for its steer after its turn ended queues nothing of it",
  () =>
    Effect.gen(function* () {
      const harness = yield* delegatedFollowupHarness("r1-cancel-held");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        yield* harness.startChildTurn;
        const first = harness.started[0]!;
        const firstTaskId = yield* harness.taskOf("first");
        yield* harness.send("second", { type: "queue_after_active" });
        const taskId = yield* harness.taskOf("second");
        const promoted = yield* steersInto(orchestrator, first.runId, 1);
        yield* TestClock.adjust("61 seconds");
        yield* Fiber.join(promoted);
        // The turn ends before the steer runs; the user's waiting message ends its run at once.
        yield* harness.send("third", { type: "queue_after_active" }, { fromUser: true });
        const firstSettled = yield* harness.nextTaskUpdate(
          firstTaskId,
          (task) => task.result !== null,
        );
        const turn = (yield* orchestrator.getThreadProjection(harness.childThreadId))
          .providerTurns[0]!;
        const afterSequence = yield* sink.latestSequence({ threadId: harness.childThreadId });
        const ended = yield* sink.stream({ threadId: harness.childThreadId, afterSequence }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === first.runId &&
              event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
        const now = yield* DateTime.now;
        yield* Queue.offer(harness.events, {
          type: "provider_turn.updated",
          driver,
          providerTurn: { ...turn, status: "completed", completedAt: now },
        });
        yield* Queue.offer(harness.events, {
          type: "turn.terminal",
          driver,
          providerThreadId: turn.providerThreadId,
          providerTurnId: turn.id,
          runOrdinal: first.runOrdinal,
          status: "completed",
          failure: null,
          threadDisposition: "reusable",
        });
        yield* Fiber.join(ended);
        yield* Fiber.join(firstSettled);
        yield* executor.withLock(harness.parentThreadId, Effect.void);
        // Held for its steer, the follow-up is cancelled before that steer runs.
        yield* orchestrator.cancelDelegatedTask({
          parentThreadId: harness.parentThreadId,
          taskId,
          command: {
            type: "thread.stop",
            commandId: CommandId.make("cancel-held"),
            threadId: harness.childThreadId,
            stopDelegatedTasks: false,
          },
          stopDescendants: () => Effect.void,
        });
        yield* worker.drain();
        yield* stepClock(harness.childThreadId, 1, true);
        assert.deepEqual(harness.steered, []);
        assert.deepEqual(
          (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs
            .filter(
              (run) =>
                run.status === "queued" && run.userMessageId === MessageId.make("message:second"),
            )
            .map((run) => run.id),
          [],
        );
        const cancelled = (yield* harness.parentView).tasks.find((task) => task.id === taskId);
        assert.equal(cancelled?.status, "interrupted");
        assert.equal(cancelled?.completionDelivery?.state, "disposed");
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect(
  "a promoted follow-up still waits for a subagent the turn it joined started, after a wake of its own",
  () =>
    Effect.gen(function* () {
      const harness = yield* delegatedFollowupHarness("r1-joined-subagent");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const executor = yield* ThreadCommandExecutor.ThreadCommandExecutor;
        const sink = yield* EventSink.EventSinkV2;
        yield* harness.startChildTurn;
        const first = harness.started[0]!;
        yield* harness.send("second", { type: "queue_after_active" });
        const taskId = yield* harness.taskOf("second");
        const promoted = yield* steersInto(orchestrator, first.runId, 1);
        yield* TestClock.adjust("61 seconds");
        yield* Fiber.join(promoted);
        yield* worker.drain();
        assert.deepEqual(harness.steered.map(withoutSenderNote), ["second"]);
        // The joined turn delegates work of its own.
        const now = yield* DateTime.now;
        const subagent = {
          id: NodeId.make("node:r1-joined-subagent:grandchild"),
          threadId: harness.childThreadId,
          runId: first.runId,
          parentNodeId: first.rootNodeId,
          origin: "app_owned" as const,
          createdBy: "agent" as const,
          driver,
          providerInstanceId: instanceId,
          providerThreadId: null,
          childThreadId: ThreadId.make("thread:r1-joined-subagent:grandchild"),
          nativeTaskRef: null,
          prompt: "Check the other half.",
          title: null,
          model: null,
          status: "running" as const,
          result: null,
          startedAt: now,
          completedAt: null,
          updatedAt: now,
        };
        yield* sink.write({
          events: [
            {
              id: EventId.make("r1-joined-subagent:grandchild"),
              type: "subagent.updated",
              threadId: harness.childThreadId,
              runId: first.runId,
              nodeId: subagent.id,
              occurredAt: now,
              payload: subagent,
            },
          ],
        });
        // A command the turn started finishes; its wake continues the latest open follow-up.
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("r1-joined-subagent:wake"),
          threadId: harness.childThreadId,
          messageId: MessageId.make("message:wake"),
          text: "Background activity updated",
          attachments: [],
          notification: {
            source: { kind: "command" },
            outcome: "completed",
            summary: 'Command "build" finished',
          },
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        });
        const wake = (yield* orchestrator.getThreadProjection(harness.childThreadId)).runs.find(
          (run) => run.userMessageId === MessageId.make("message:wake"),
        )!;
        assert.equal(wake.delegatedTaskId, taskId);
        yield* harness.endTurn(0, "Both follow-ups answered; a check still runs.");
        yield* harness.startRun(wake.id);
        yield* harness.endTurn(1, "The build finished.");
        yield* executor.withLock(harness.parentThreadId, Effect.void);
        const held = (yield* harness.parentView).tasks.find((task) => task.id === taskId);
        assert.isNull(
          held?.result ?? null,
          "the promoted follow-up settled while a subagent of the turn it joined still ran",
        );

        const settled = yield* harness.nextTaskUpdate(taskId, (task) => task.result !== null);
        yield* sink.write({
          events: [
            {
              id: EventId.make("r1-joined-subagent:grandchild-done"),
              type: "subagent.updated",
              threadId: harness.childThreadId,
              runId: first.runId,
              nodeId: subagent.id,
              occurredAt: now,
              payload: { ...subagent, status: "completed", result: "Checked.", completedAt: now },
            },
          ],
        });
        const answered = yield* Fiber.join(settled);
        assert.equal(answered.status, "completed");
        assert.equal(answered.result, "The build finished.");
      }).pipe(Effect.provide(harness.layer));
    }),
);

it.effect(
  "cancelling a promoted follow-up stops the turn it joined and wakes its parent for none of it",
  () =>
    Effect.gen(function* () {
      const harness = yield* delegatedFollowupHarness("r1-cancel");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* harness.startChildTurn;
        const first = harness.started[0]!;
        yield* harness.send("second", { type: "queue_after_active" });
        const taskId = yield* harness.taskOf("second");
        const promoted = yield* steersInto(orchestrator, first.runId, 1);
        const joined = yield* harness.nextTaskUpdate(taskId, (task) => task.status !== "pending");
        yield* TestClock.adjust("61 seconds");
        yield* Fiber.join(promoted);
        yield* worker.drain();
        assert.equal((yield* Fiber.join(joined)).status, "running");

        const settled = yield* harness.nextTaskUpdate(taskId, (task) => task.result !== null);
        yield* orchestrator.cancelDelegatedTask({
          parentThreadId: harness.parentThreadId,
          taskId,
          command: {
            type: "thread.stop",
            commandId: CommandId.make("cancel-second"),
            threadId: harness.childThreadId,
            stopDelegatedTasks: false,
          },
          stopDescendants: () => Effect.void,
        });
        yield* worker.drain();
        const cancelled = yield* Fiber.join(settled);
        assert.equal(cancelled.cancelledRunId, first.runId);
        assert.equal(cancelled.status, "interrupted");
        assert.equal(cancelled.completionDelivery?.state, "disposed");
      }).pipe(Effect.provide(harness.layer));
    }),
);

// A waiting run's turn is over; until its checkpoint capture runs, nothing else may start.
const waitingRunHarness = Effect.fn("waitingRunHarness")(function* (
  name: string,
  steerAcknowledgement?: Effect.Effect<void, ProviderAdapter.ProviderAdapterSteerRunError>,
) {
  // Later turns edit the workspace as they start, so a checkpoint taken after one shows the edit.
  const nextTurnEdit = { write: Effect.void as Effect.Effect<void>, file: "next-turn.txt" };
  const harness = yield* nextTurnSelectionHarness(
    name,
    false,
    true,
    Effect.void,
    true,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    steerAcknowledgement,
    Effect.suspend(() => nextTurnEdit.write),
  );
  nextTurnEdit.write = Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fileSystem.writeFileString(
      path.join(harness.cwd, nextTurnEdit.file),
      "edited by the next turn\n",
    );
  }).pipe(Effect.provide(NodeServices.layer), Effect.orDie);
  const settleTurn = Effect.fn("settleTurn")(function* (threadId: ThreadId, index: number) {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const sink = yield* EventSink.EventSinkV2;
    const turnInput = harness.started[index]!;
    const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns.find(
      (candidate) => candidate.runAttemptId === turnInput.attemptId,
    )!;
    const afterSequence = yield* sink.latestSequence({ threadId });
    const waiting = yield* sink.stream({ threadId, afterSequence }).pipe(
      Stream.filter(
        ({ event }) =>
          event.type === "run.updated" &&
          event.payload.activeAttemptId === turnInput.attemptId &&
          event.payload.status === "waiting",
      ),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
    const now = yield* DateTime.now;
    yield* Queue.offer(harness.events, {
      type: "provider_turn.updated",
      driver,
      providerTurn: { ...turn, status: "completed", completedAt: now },
    });
    yield* Queue.offer(harness.events, {
      type: "turn.terminal",
      driver,
      providerThreadId: turn.providerThreadId,
      providerTurnId: turn.id,
      runOrdinal: turnInput.runOrdinal,
      status: "completed",
      failure: null,
      threadDisposition: "reusable",
    });
    yield* Fiber.join(waiting);
  });
  // Replays from the current sequence, so an event a reactor writes first is still seen.
  const watch = Effect.fn("watch")(function* (
    threadId: ThreadId,
    predicate: (event: OrchestrationV2DomainEvent) => boolean,
  ) {
    const sink = yield* EventSink.EventSinkV2;
    const afterSequence = yield* sink.latestSequence({ threadId });
    return yield* sink.stream({ threadId, afterSequence }).pipe(
      Stream.filter((stored) => predicate(stored.event)),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  });
  const storedEvents = Effect.fn("storedEvents")(function* (threadId: ThreadId) {
    const sink = yield* EventSink.EventSinkV2;
    const latest = yield* sink.latestSequence({ threadId });
    return yield* sink.stream({ threadId, afterSequence: 0 }).pipe(
      Stream.takeUntil((stored) => stored.sequence >= latest),
      Stream.map((stored) => stored.event),
      Stream.runCollect,
    );
  });
  // The reaction to a run reaching its wait reads the thread's queue when its reactor gets to it.
  // The returned effect resolves once that read is done, so a later queue change cannot reach it.
  const waitReactionRead = Effect.fn("waitReactionRead")(function* (threadId: ThreadId) {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const getThreadRecords = projections.getThreadRecords;
    const read = yield* Deferred.make<void>();
    const spy = vi.spyOn(projections, "getThreadRecords").mockImplementation(((
      ...args: Parameters<typeof getThreadRecords>
    ) =>
      getThreadRecords(...args).pipe(
        Effect.tap(() => {
          const statuses = args[2]?.runStatuses ?? [];
          return args[0] === threadId && statuses.length === 1 && statuses[0] === "queued"
            ? Deferred.succeed(read, undefined)
            : Effect.void;
        }),
      )) as typeof getThreadRecords);
    return Deferred.await(read).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())));
  });
  return { ...harness, settleTurn, watch, storedEvents, waitReactionRead, nextTurnEdit };
});

// A provider's own wake turn, which already runs in the CLI; its continuation run takes it in.
const providerWake = {
  createdBy: "agent",
  creationSource: "provider",
  notification: {
    source: { kind: "background_task" },
    outcome: "updated",
    summary: "Background activity updated",
  },
} as const;

const visibleSends = [
  { kind: "a user send", deliveryIntent: "auto", createdBy: "user", creationSource: "web" },
  { kind: "an explicit steer", deliveryIntent: "steer", createdBy: "user", creationSource: "web" },
  { kind: "a queued send", deliveryIntent: undefined, createdBy: "user", creationSource: "web" },
  {
    kind: "an automation",
    deliveryIntent: "auto",
    createdBy: "user",
    creationSource: "web",
    scheduledTaskId: ScheduledTaskId.make("scheduled-task:waiting"),
  },
  {
    kind: "a cross-thread message",
    deliveryIntent: "auto",
    createdBy: "agent",
    creationSource: "mcp",
    senderThreadId: ThreadId.make("thread:waiting-sender"),
  },
] as const;

it.effect.each(visibleSends)(
  "starts $kind sent while the run waits for its checkpoint, once that checkpoint is taken",
  (send) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `waiting-ends-${send.kind.replaceAll(/[^a-z]+/g, "-")}`;
        const { started, steered, layer, startFirstTurn, settleTurn, watch, storedEvents } =
          yield* waitingRunHarness(name);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          yield* settleTurn(threadId, 0);
          const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
          assert.equal(first.status, "waiting");
          const messageId = MessageId.make(`message:${name}`);
          const starting = yield* watch(
            threadId,
            (event) =>
              event.type === "run.updated" &&
              event.payload.userMessageId === messageId &&
              event.payload.status === "starting",
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(name),
            threadId,
            messageId,
            text: send.kind,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            ...(send.deliveryIntent === undefined ? {} : { deliveryIntent: send.deliveryIntent }),
            createdBy: send.createdBy,
            creationSource: send.creationSource,
            ...("scheduledTaskId" in send ? { scheduledTaskId: send.scheduledTaskId } : {}),
            ...("senderThreadId" in send ? { senderThreadId: send.senderThreadId } : {}),
          });
          yield* Fiber.join(starting);
          const ended = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
          assert.equal(ended.status, "completed");
          assert.isNull(ended.checkpointId);
          const nextTurnRunning = yield* watch(
            threadId,
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.status === "running" &&
              event.payload.runAttemptId !== first.activeAttemptId,
          );
          yield* worker.drain();
          yield* Fiber.join(nextTurnRunning);
          assert.deepEqual(
            started.map((turn) => withoutSenderNote(turn.message.text)),
            ["first", send.kind],
          );
          assert.deepEqual(steered, []);
          const after = yield* orchestrator.getThreadProjection(threadId);
          const captured = after.runs.find((run) => run.id === first.id)!;
          assert.equal(captured.status, "completed");
          assert.isNotNull(captured.checkpointId);
          assert.equal(
            after.nodes.find((node) => node.id === first.rootNodeId)?.status,
            "completed",
          );
          // The capture ran ahead of the next turn, so it holds the waiting run's workspace.
          const events = yield* storedEvents(threadId);
          const capturedAt = events.findIndex(
            (event) => event.type === "checkpoint.captured" && event.runId === first.id,
          );
          const nextTurnAt = events.findIndex(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.runAttemptId === started[1]!.attemptId &&
              event.payload.status === "running",
          );
          assert.isAtLeast(capturedAt, 0);
          assert.isAbove(nextTurnAt, capturedAt);
          const checkpoint = after.checkpoints.find((row) => row.id === captured.checkpointId);
          assert.equal(checkpoint?.status, "ready");
          assert.notInclude(
            (checkpoint?.files ?? []).map((file) => file.path),
            "next-turn.txt",
          );
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect.each([
  { kind: "a completion wake" },
  // Its run would start beside the provider's background work, whose later updates it can miss.
  { kind: "a provider's wake turn" },
  { kind: "a notice T3 delivers" },
  { kind: "Stop" },
  { kind: "a request waiting on the user" },
] as const)("keeps a run waiting for its checkpoint despite $kind", ({ kind }) =>
  Effect.scoped(
    Effect.gen(function* () {
      const name = `waiting-kept-${kind.replaceAll(/[^a-z]+/g, "-")}`;
      const { started, layer, startFirstTurn, settleTurn } = yield* waitingRunHarness(name);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = yield* startFirstTurn;
        yield* settleTurn(threadId, 0);
        const before = yield* orchestrator.getThreadProjection(threadId);
        const first = before.runs[0]!;
        const now = yield* DateTime.now;
        if (kind === "Stop") {
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make(`${name}:stop`),
            threadId,
          });
        }
        if (kind === "a request waiting on the user") {
          yield* sink.write({
            events: [
              {
                id: EventId.make(`${name}:request`),
                type: "runtime-request.updated",
                threadId,
                occurredAt: now,
                payload: {
                  id: RuntimeRequestId.make(`${name}:request`),
                  nodeId: first.rootNodeId!,
                  providerTurnId: before.providerTurns[0]!.id,
                  nativeRequestRef: null,
                  kind: "user_input",
                  status: "pending",
                  responseCapability: { type: "message" },
                  createdAt: now,
                  resolvedAt: null,
                },
              },
            ],
          });
        }
        const messageId = MessageId.make(`message:${name}`);
        if (kind === "a completion wake") {
          const taskId = NodeId.make(`${name}:task`);
          yield* sink.write({
            events: [
              {
                id: EventId.make(`${name}:cohort`),
                type: "run.updated",
                threadId,
                runId: first.id,
                occurredAt: now,
                payload: {
                  ...first,
                  delegatedCompletion: {
                    disposition: "open",
                    nextGeneration: 2,
                    delivery: { generation: 1, messageId, taskIds: [taskId] },
                  },
                },
              },
              {
                id: EventId.make(`${name}:task`),
                type: "subagent.updated",
                threadId,
                runId: first.id,
                nodeId: taskId,
                occurredAt: now,
                payload: {
                  id: taskId,
                  threadId,
                  runId: first.id,
                  parentNodeId: first.rootNodeId!,
                  origin: "app_owned",
                  createdBy: "agent",
                  driver,
                  providerInstanceId: instanceId,
                  providerThreadId: null,
                  childThreadId: null,
                  nativeTaskRef: null,
                  prompt: "Background work",
                  title: "Child",
                  model: null,
                  completionWake: "always",
                  completionDelivery: { state: "claimed", observedByRunId: null },
                  status: "completed",
                  result: "done",
                  startedAt: now,
                  completedAt: now,
                  updatedAt: now,
                },
              },
            ],
          });
        }
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(name),
          threadId,
          messageId,
          text: kind,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          ...(kind === "a completion wake"
            ? {
                createdBy: "agent" as const,
                creationSource: "server" as const,
                delegatedCompletion: {
                  parentRunId: first.id,
                  generation: 1,
                  taskIds: [NodeId.make(`${name}:task`)],
                },
              }
            : kind === "a provider's wake turn"
              ? providerWake
              : kind === "a notice T3 delivers"
                ? { ...providerWake, creationSource: "server" as const }
                : {
                    deliveryIntent: "auto" as const,
                    createdBy: "user" as const,
                    creationSource: "web" as const,
                  }),
        });
        // Reactions to the send settle under the thread's lock.
        yield* (yield* ThreadCommandExecutor.ThreadCommandExecutor).withLock(threadId, Effect.void);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(after.runs.find((run) => run.id === first.id)?.status, "waiting");
        assert.equal(after.runs.find((run) => run.userMessageId === messageId)?.status, "queued");
        assert.lengthOf(started, 1);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect.each([
  { kind: "a user send" },
  { kind: "an automation", scheduledTaskId: ScheduledTaskId.make("scheduled-task:queued") },
] as const)(
  "starts $kind queued behind a live turn as soon as that turn ends, before its checkpoint",
  (send) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `waiting-queued-${send.kind.replaceAll(/[^a-z]+/g, "-")}`;
        const { started, layer, startFirstTurn, settleTurn, watch } =
          yield* waitingRunHarness(name);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          const messageId = MessageId.make(`message:${name}`);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(name),
            threadId,
            messageId,
            text: send.kind,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
            ...("scheduledTaskId" in send ? { scheduledTaskId: send.scheduledTaskId } : {}),
          });
          const starting = yield* watch(
            threadId,
            (event) =>
              event.type === "run.updated" &&
              event.payload.userMessageId === messageId &&
              event.payload.status === "starting",
          );
          yield* settleTurn(threadId, 0);
          yield* Fiber.join(starting);
          const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
          assert.equal(first.status, "completed");
          assert.isNull(first.checkpointId);
          yield* worker.drain();
          assert.deepEqual(
            started.map((turn) => withoutSenderNote(turn.message.text)),
            ["first", send.kind],
          );
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.isNotNull(after.runs.find((run) => run.id === first.id)?.checkpointId);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("keeps a run waiting for its checkpoint when only a completion wake is queued", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const name = "waiting-queued-wake";
      const { started, layer, startFirstTurn, settleTurn, watch, storedEvents } =
        yield* waitingRunHarness(name);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = yield* startFirstTurn;
        const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const now = yield* DateTime.now;
        const messageId = MessageId.make(`message:${name}`);
        const taskId = NodeId.make(`${name}:task`);
        yield* sink.write({
          events: [
            {
              id: EventId.make(`${name}:cohort`),
              type: "run.updated",
              threadId,
              runId: first.id,
              occurredAt: now,
              payload: {
                ...first,
                delegatedCompletion: {
                  disposition: "open",
                  nextGeneration: 2,
                  delivery: { generation: 1, messageId, taskIds: [taskId] },
                },
              },
            },
            {
              id: EventId.make(`${name}:task`),
              type: "subagent.updated",
              threadId,
              runId: first.id,
              nodeId: taskId,
              occurredAt: now,
              payload: {
                id: taskId,
                threadId,
                runId: first.id,
                parentNodeId: first.rootNodeId!,
                origin: "app_owned",
                createdBy: "agent",
                driver,
                providerInstanceId: instanceId,
                providerThreadId: null,
                childThreadId: null,
                nativeTaskRef: null,
                prompt: "Background work",
                title: "Child",
                model: null,
                completionWake: "always",
                completionDelivery: { state: "claimed", observedByRunId: null },
                status: "completed",
                result: "done",
                startedAt: now,
                completedAt: now,
                updatedAt: now,
              },
            },
          ],
        });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(name),
          threadId,
          messageId,
          text: "Child completed",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
          delegatedCompletion: { parentRunId: first.id, generation: 1, taskIds: [taskId] },
        });
        const wakeStarting = yield* watch(
          threadId,
          (event) =>
            event.type === "run.updated" &&
            event.payload.userMessageId === messageId &&
            event.payload.status === "starting",
        );
        yield* settleTurn(threadId, 0);
        yield* worker.drain();
        yield* Fiber.join(wakeStarting);
        yield* worker.drain();
        assert.lengthOf(started, 2);
        // The run completed only when its capture ran.
        const completions = (yield* storedEvents(threadId)).flatMap((event) =>
          event.type === "run.updated" &&
          event.payload.id === first.id &&
          event.payload.status === "completed"
            ? [event.payload]
            : [],
        );
        assert.isAbove(completions.length, 0);
        assert.isNotNull(completions[0]!.checkpointId);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// A held message never ends a wait by itself; Send now (promote-to-steer) on it does.
it.effect.each([
  { kind: "a held message", freshContext: false, stopped: false },
  { kind: "a held fresh-session message", freshContext: true, stopped: false },
  { kind: "a held message after Stop", freshContext: false, stopped: true },
] as const)(
  "sends $kind now into a run waiting for its checkpoint",
  ({ kind, freshContext, stopped }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `waiting-send-now-${kind.replaceAll(/[^a-z]+/g, "-")}`;
        const { started, steered, layer, startFirstTurn, settleTurn, watch } =
          yield* waitingRunHarness(name);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sink = yield* EventSink.EventSinkV2;
          const threadId = yield* startFirstTurn;
          const messageId = MessageId.make(`message:${name}`);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(name),
            threadId,
            messageId,
            text: kind,
            attachments: [],
            ...(freshContext ? { freshContext: true } : {}),
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.userMessageId === messageId,
          )!;
          yield* sink.write({
            events: [
              {
                id: EventId.make(`${name}:held`),
                type: "run.updated",
                threadId,
                runId: queued.id,
                occurredAt: yield* DateTime.now,
                payload: { ...queued, queueHeld: true },
              },
            ],
          });
          yield* settleTurn(threadId, 0);
          if (stopped) {
            yield* orchestrator.dispatch({
              type: "thread.stop",
              commandId: CommandId.make(`${name}:stop`),
              threadId,
            });
          }
          const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
          assert.equal(first.status, "waiting");
          const starting = yield* watch(
            threadId,
            (event) =>
              event.type === "run.updated" &&
              event.payload.userMessageId === messageId &&
              event.payload.status === "starting",
          );
          const promoted = yield* Effect.exit(
            orchestrator.dispatch({
              type: "queued-message.promote-to-steer",
              commandId: CommandId.make(`${name}:send-now`),
              threadId,
              queuedRunId: queued.id,
              targetRunId: first.id,
            }),
          );
          if (stopped) {
            assert.equal(promoted._tag, "Failure");
            const after = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(after.runs.find((run) => run.id === first.id)?.status, "waiting");
            assert.equal(after.runs.find((run) => run.id === queued.id)?.status, "queued");
            return;
          }
          assert.equal(promoted._tag, "Success");
          yield* Fiber.join(starting);
          yield* worker.drain();
          assert.deepEqual(
            started.map((turn) => turn.message.text),
            ["first", kind],
          );
          assert.deepEqual(steered, []);
          const after = yield* orchestrator.getThreadProjection(threadId);
          const ended = after.runs.find((run) => run.id === first.id)!;
          assert.equal(ended.status, "completed");
          assert.isNotNull(ended.checkpointId);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

// A waiting run no reaction ended (one that waited across a restart, say) yields on the sweep.
it.effect.each([false, true])(
  "ends the wait of a run waiting for its checkpoint for a message queued 60 s (fresh session: %s)",
  (freshContext) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = freshContext ? "waiting-auto-send-now-fresh" : "waiting-auto-send-now";
        const { started, steered, layer, startFirstTurn, settleTurn, watch, waitReactionRead } =
          yield* waitingRunHarness(name);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sink = yield* EventSink.EventSinkV2;
          const threadId = yield* startFirstTurn;
          const messageId = MessageId.make(`message:${name}`);
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(name),
            threadId,
            messageId,
            text: name,
            attachments: [],
            ...(freshContext ? { freshContext: true } : {}),
            dispatchMode: { type: "queue_after_active" },
            createdBy: "user",
            creationSource: "web",
          });
          const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.userMessageId === messageId,
          )!;
          const hold = (queueHeld: boolean) =>
            Effect.gen(function* () {
              yield* sink.write({
                events: [
                  {
                    id: EventId.make(`${name}:held:${queueHeld}`),
                    type: "run.updated",
                    threadId,
                    runId: queued.id,
                    occurredAt: yield* DateTime.now,
                    payload: { ...queued, queueHeld },
                  },
                ],
              });
            });
          // Release the message only once the wait's reaction found it held: no event ends the wait.
          const reactionRead = yield* waitReactionRead(threadId);
          yield* hold(true);
          yield* settleTurn(threadId, 0);
          yield* reactionRead;
          yield* hold(false);
          const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
          assert.equal(first.status, "waiting");
          const starting = yield* watch(
            threadId,
            (event) =>
              event.type === "run.updated" &&
              event.payload.userMessageId === messageId &&
              event.payload.status === "starting",
          );
          yield* TestClock.adjust("55 seconds");
          assert.equal(
            (yield* orchestrator.getThreadProjection(threadId)).runs[0]!.status,
            "waiting",
          );
          yield* TestClock.adjust("10 seconds");
          yield* Fiber.join(starting);
          assert.isNull((yield* orchestrator.getThreadProjection(threadId)).runs[0]!.checkpointId);
          const startedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.id === queued.id,
          )!;
          const delayMs = Duration.toMillis(
            DateTime.distance(queued.requestedAt, startedRun.startedAt ?? (yield* DateTime.now)),
          );
          assert.isAtLeast(delayMs, 60_000);
          assert.isAtMost(delayMs, 90_000);
          yield* worker.drain();
          assert.deepEqual(
            started.map((turn) => turn.message.text),
            ["first", name],
          );
          assert.deepEqual(steered, []);
          assert.isNotNull(
            (yield* orchestrator.getThreadProjection(threadId)).runs[0]!.checkpointId,
          );
        }).pipe(Effect.provide(layer));
      }),
    ),
);

// The capture commits outside the thread's lock, so it can land between the read that finds the
// run waiting and the write that ends that wait; the checkpoint it recorded must survive.
it.effect.each(["the send's own read", "the wait's read"] as const)(
  "keeps a checkpoint the capture records right after %s",
  (when) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `waiting-race-${when.replaceAll(/[^a-z]+/g, "-")}`;
        const { started, layer, startFirstTurn, settleTurn, watch } =
          yield* waitingRunHarness(name);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sink = yield* EventSink.EventSinkV2;
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const threadId = yield* startFirstTurn;
          // The run's own wait reaction reads the queue first; the race is armed after it.
          const reacted = yield* Deferred.make<void>();
          let armed = false;
          const checkpointId = CheckpointId.make(`checkpoint:${name}`);
          // Stands in for the capture's commit: the run completes with its checkpoint.
          const capture = Effect.gen(function* () {
            const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
            yield* sink.write({
              events: [
                {
                  id: EventId.make(`${name}:captured`),
                  type: "run.updated",
                  threadId,
                  runId: first.id,
                  occurredAt: yield* DateTime.now,
                  payload: {
                    ...first,
                    status: "completed",
                    completedAt: yield* DateTime.now,
                    checkpointId,
                  },
                },
              ],
            });
          }).pipe(Effect.orDie);
          const getMessageDispatchRecords = projections.getMessageDispatchRecords;
          const getThreadRecords = projections.getThreadRecords;
          const dispatchSpy = vi
            .spyOn(projections, "getMessageDispatchRecords")
            .mockImplementation((...args) =>
              getMessageDispatchRecords(...args).pipe(
                Effect.tap(() => {
                  if (!armed || when !== "the send's own read") return Effect.void;
                  armed = false;
                  return capture;
                }),
              ),
            );
          const recordsSpy = vi.spyOn(projections, "getThreadRecords").mockImplementation(((
            ...args: Parameters<typeof getThreadRecords>
          ) =>
            getThreadRecords(...args).pipe(
              Effect.tap(() => {
                const statuses = args[2]?.runStatuses ?? [];
                // The wait's read of the queue comes after it found the run waiting.
                if (args[0] !== threadId || statuses.length !== 1 || statuses[0] !== "queued")
                  return Effect.void;
                if (!armed) return Deferred.succeed(reacted, undefined);
                if (when !== "the wait's read") return Effect.void;
                armed = false;
                return capture;
              }),
            )) as typeof getThreadRecords);
          yield* settleTurn(threadId, 0);
          yield* Deferred.await(reacted);
          armed = true;
          const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
          const messageId = MessageId.make(`message:${name}`);
          const starting = yield* watch(
            threadId,
            (event) =>
              event.type === "run.updated" &&
              event.payload.userMessageId === messageId &&
              event.payload.status === "starting",
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(name),
            threadId,
            messageId,
            text: when,
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            deliveryIntent: "auto",
            createdBy: "user",
            creationSource: "web",
          });
          yield* Fiber.join(starting);
          dispatchSpy.mockRestore();
          recordsSpy.mockRestore();
          assert.isFalse(armed);
          yield* worker.drain();
          assert.deepEqual(
            started.map((turn) => turn.message.text),
            ["first", when],
          );
          const after = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
            (run) => run.id === first.id,
          )!;
          assert.equal(after.status, "completed");
          assert.equal(after.checkpointId, checkpointId);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("ends the wait for a message queued 60 s once a request on the user is dismissed", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const name = "waiting-auto-send-now-request";
      const { started, layer, startFirstTurn, settleTurn, watch } = yield* waitingRunHarness(name);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = yield* startFirstTurn;
        const messageId = MessageId.make(`message:${name}`);
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(name),
          threadId,
          messageId,
          text: name,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.userMessageId === messageId,
        )!;
        const now = yield* DateTime.now;
        const write = (event: Omit<OrchestrationV2DomainEvent, "id">, key: string) =>
          sink.write({ events: [{ ...event, id: EventId.make(`${name}:${key}`) } as never] });
        yield* write(
          {
            type: "run.updated",
            threadId,
            runId: queued.id,
            occurredAt: now,
            payload: { ...queued, queueHeld: true },
          } as never,
          "held",
        );
        yield* settleTurn(threadId, 0);
        const before = yield* orchestrator.getThreadProjection(threadId);
        const first = before.runs[0]!;
        const request = {
          id: RuntimeRequestId.make(`${name}:request`),
          nodeId: first.rootNodeId!,
          providerTurnId: before.providerTurns[0]!.id,
          nativeRequestRef: null,
          kind: "user_input" as const,
          status: "pending" as const,
          responseCapability: { type: "message" as const },
          createdAt: now,
          resolvedAt: null,
        };
        yield* write(
          { type: "runtime-request.updated", threadId, occurredAt: now, payload: request } as never,
          "request",
        );
        yield* write(
          {
            type: "run.updated",
            threadId,
            runId: queued.id,
            occurredAt: now,
            payload: { ...queued, queueHeld: false },
          } as never,
          "released",
        );
        yield* TestClock.adjust("65 seconds");
        assert.equal(
          (yield* orchestrator.getThreadProjection(threadId)).runs[0]!.status,
          "waiting",
        );
        const starting = yield* watch(
          threadId,
          (event) =>
            event.type === "run.updated" &&
            event.payload.userMessageId === messageId &&
            event.payload.status === "starting",
        );
        yield* write(
          {
            type: "runtime-request.updated",
            threadId,
            occurredAt: yield* DateTime.now,
            payload: { ...request, status: "cancelled", resolvedAt: yield* DateTime.now },
          } as never,
          "dismissed",
        );
        yield* TestClock.adjust("5 seconds");
        yield* Fiber.join(starting);
        assert.lengthOf(started, 1);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// Ending a wait starts the queue in its own order: the sweep never moves its pick ahead of an
// older message that it does not promote itself, such as one starting a fresh session.
it.effect(
  "ends the wait for the sweep without moving its pick ahead of an older fresh session",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = "waiting-auto-send-now-order";
        const { started, layer, startFirstTurn, settleTurn, watch, waitReactionRead } =
          yield* waitingRunHarness(name);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const sink = yield* EventSink.EventSinkV2;
          const threadId = yield* startFirstTurn;
          const queue = (key: string, freshContext: boolean) =>
            orchestrator.dispatch({
              type: "message.dispatch",
              commandId: CommandId.make(`${name}:${key}`),
              threadId,
              messageId: MessageId.make(`message:${name}:${key}`),
              text: key,
              attachments: [],
              ...(freshContext ? { freshContext: true } : {}),
              dispatchMode: { type: "queue_after_active" },
              createdBy: "user",
              creationSource: "web",
            });
          yield* queue("fresh", true);
          yield* queue("ordinary", false);
          const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.filter(
            (run) => run.status === "queued",
          );
          const hold = (queueHeld: boolean) =>
            Effect.gen(function* () {
              const now = yield* DateTime.now;
              yield* sink.write({
                events: queued.map((run) => ({
                  id: EventId.make(`${name}:${run.id}:held:${queueHeld}`),
                  type: "run.updated" as const,
                  threadId,
                  runId: run.id,
                  occurredAt: now,
                  payload: { ...run, queueHeld },
                })),
              });
            });
          // Release the messages only once the wait's reaction found them held.
          const reactionRead = yield* waitReactionRead(threadId);
          yield* hold(true);
          yield* settleTurn(threadId, 0);
          yield* reactionRead;
          yield* hold(false);
          const starting = yield* watch(
            threadId,
            (event) =>
              event.type === "run.updated" &&
              event.payload.status === "starting" &&
              event.payload.id !== started[0]!.runId,
          );
          yield* TestClock.adjust("55 seconds");
          assert.equal(
            (yield* orchestrator.getThreadProjection(threadId)).runs[0]!.status,
            "waiting",
          );
          yield* TestClock.adjust("10 seconds");
          yield* Fiber.join(starting);
          yield* worker.drain();
          assert.deepEqual(
            started.map((turn) => turn.message.text),
            ["first", "fresh"],
          );
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.equal(
            after.runs.find((run) => run.userMessageId === `message:${name}:ordinary`)?.status,
            "queued",
          );
        }).pipe(Effect.provide(layer));
      }),
    ),
);

// A capture already reading the workspace records the run's end itself; no wait ends under it.
it.effect("leaves a waiting run to its checkpoint capture once that capture runs", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const name = "waiting-capture-running";
      const { started, layer, startFirstTurn, settleTurn, watch } = yield* waitingRunHarness(name);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const outbox = yield* EffectOutbox.EffectOutboxV2;
        const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
        const threadId = yield* startFirstTurn;
        yield* settleTurn(threadId, 0);
        const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const claimed = Option.getOrThrow(yield* outbox.claimNext({ workerId: name }));
        assert.equal(claimed.id, `effect:checkpoint.capture:${first.id}`);
        const messageId = MessageId.make(`message:${name}`);
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(name),
          threadId,
          messageId,
          text: name,
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          deliveryIntent: "auto",
          createdBy: "user",
          creationSource: "web",
        });
        const during = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(during.runs.find((run) => run.id === first.id)?.status, "waiting");
        assert.equal(during.runs.find((run) => run.userMessageId === messageId)?.status, "queued");
        const starting = yield* watch(
          threadId,
          (event) =>
            event.type === "run.updated" &&
            event.payload.userMessageId === messageId &&
            event.payload.status === "starting",
        );
        yield* executor.execute(claimed);
        yield* outbox.succeed({ effectId: claimed.id, workerId: name });
        yield* Fiber.join(starting);
        yield* worker.drain();
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", name],
        );
        const ended = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.id === first.id,
        )!;
        assert.equal(ended.status, "completed");
        assert.isNotNull(ended.checkpointId);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// A steer into a wake or notice continuation can reach the provider after
// the turn it targeted ended, so the provider reports it not delivered and it follows up. The
// follow-up may start no earlier than that run's wait, which records its finalize and capture.
const missesFirstSteer = (name: string) => {
  const steers = { attempts: 0 };
  const acknowledgement = Effect.suspend(() =>
    steers.attempts++ > 0
      ? Effect.void
      : Effect.fail(
          new ProviderAdapter.ProviderAdapterSteerRunError({
            driver,
            providerThreadId: ProviderThreadId.make(`provider-thread:thread:${name}`),
            providerTurnId: ProviderTurnId.make(`provider-turn:${name}`),
            deliveryNotSent: true,
            cause: "The turn ended before the steer reached it",
          }),
        ),
  );
  return { steers, acknowledgement };
};

const crossThreadAutoSend = (threadId: ThreadId, name: string) => ({
  type: "message.dispatch" as const,
  commandId: CommandId.make(name),
  threadId,
  messageId: MessageId.make(`message:${name}`),
  text: name,
  attachments: [],
  deliveryIntent: "auto" as const,
  dispatchMode: { type: "start_immediately" as const },
  senderThreadId: ThreadId.make("thread:sender"),
  createdBy: "agent" as const,
  creationSource: "mcp" as const,
});

/**
 * The shape a running server shows: a notice continuation runs before its provider turn reports running, a
 * cross-thread message sent then is deferred into it, and the turn's running report promotes it.
 * A delegated lane's continuation carries its task. Returns the continuation's turn input.
 */
const deferredSteerIntoContinuation = Effect.fn("deferredSteerIntoContinuation")(function* (
  harness: Effect.Success<ReturnType<typeof waitingRunHarness>>,
  threadId: ThreadId,
  name: string,
  lane: "root" | "delegated",
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const sink = yield* EventSink.EventSinkV2;
  // The notice queues behind the first run's wait, which a hidden wake does not end.
  yield* harness.settleTurn(threadId, 0);
  yield* queueFrom(orchestrator, threadId, `${name}-notice`, "wake");
  const queued = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
    (candidate) => candidate.userMessageId === MessageId.make(`message:${name}-notice`),
  )!;
  assert.equal(queued.status, "queued");
  if (lane === "delegated") {
    yield* sink.write({
      events: [
        {
          id: EventId.make(`${name}:lane-task`),
          type: "run.updated",
          threadId,
          runId: queued.id,
          occurredAt: yield* DateTime.now,
          payload: { ...queued, delegatedTaskId: NodeId.make(`${name}:task`) },
        },
      ],
    });
  }
  const continuationRunning = yield* harness.watch(
    threadId,
    (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
  );
  yield* worker.drain();
  yield* orchestrator.resumeQueuedRuns;
  yield* worker.drain();
  yield* Fiber.join(continuationRunning);
  const continuation = harness.started[1]!;
  assert.equal(continuation.runId, queued.id);
  const projection = yield* orchestrator.getThreadProjection(threadId);
  const run = projection.runs.find((candidate) => candidate.id === continuation.runId)!;
  const turn = projection.providerTurns.find(
    (candidate) => candidate.runAttemptId === continuation.attemptId,
  )!;
  const paused = yield* harness.watch(
    threadId,
    (event) =>
      event.type === "provider-turn.updated" &&
      event.payload.id === turn.id &&
      event.payload.status === "pending",
  );
  yield* Queue.offer(harness.events, {
    type: "provider_turn.updated",
    driver,
    providerTurn: { ...turn, status: "pending" },
  });
  yield* Fiber.join(paused);
  yield* orchestrator.dispatch(crossThreadAutoSend(threadId, name));
  const deferred = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
    (candidate) =>
      candidate.status === "queued" &&
      candidate.userMessageId === MessageId.make(`message:${name}`),
  );
  assert.equal(deferred?.steerWhenReadyRunId, run.id);
  const promoted = yield* steersInto(orchestrator, run.id, 1);
  yield* Queue.offer(harness.events, {
    type: "provider_turn.updated",
    driver,
    providerTurn: { ...turn, status: "running" },
  });
  yield* Fiber.join(promoted);
  return continuation;
});

it.effect.each([
  { lane: "root", declinedWhile: "running", backgroundWork: false },
  { lane: "root", declinedWhile: "waiting", backgroundWork: false },
  { lane: "delegated", declinedWhile: "running", backgroundWork: false },
  { lane: "delegated", declinedWhile: "waiting", backgroundWork: false },
  { lane: "root", declinedWhile: "running", backgroundWork: true },
] as const)(
  "starts a not-delivered steer's follow-up at its $lane continuation's wait, declined while $declinedWhile (background work: $backgroundWork)",
  ({ lane, declinedWhile, backgroundWork }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `follow-up-at-wait-${lane}-${declinedWhile}${backgroundWork ? "-background" : ""}`;
        const { steers, acknowledgement } = missesFirstSteer(name);
        const harness = yield* waitingRunHarness(name, acknowledgement);
        const { started, steered, layer, startFirstTurn, settleTurn, watch, storedEvents } =
          harness;
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          const continuation = yield* deferredSteerIntoContinuation(harness, threadId, name, lane);
          const messageId = MessageId.make(`message:${name}`);
          // The follow-up's turn edits the workspace; the continuation's checkpoint must not hold it.
          harness.nextTurnEdit.file = "follow-up-turn.txt";
          const followUpStarting = yield* watch(
            threadId,
            (event) =>
              event.type === "run.updated" &&
              event.payload.userMessageId === messageId &&
              event.payload.steerFollowUpOfRunId === continuation.runId &&
              event.payload.status === "starting",
          );
          if (declinedWhile === "running") {
            yield* worker.drain();
            const declined = yield* orchestrator.getThreadProjection(threadId);
            assert.equal(steers.attempts, 1);
            assert.equal(
              declined.runs.find((run) => run.id === continuation.runId)?.status,
              "running",
            );
            assert.equal(
              declined.runs.find(
                (run) => run.status === "queued" && run.userMessageId === messageId,
              )?.steerFollowUpOfRunId,
              continuation.runId,
            );
            // Background work the continuation started still runs when its turn ends.
            const providerThreadOf = Effect.map(
              orchestrator.getThreadProjection(threadId),
              ({ providerThreads }) =>
                providerThreads.find(
                  (candidate) => candidate.id === continuation.providerThread.id,
                )!,
            );
            if (backgroundWork) {
              const reported = yield* watch(
                threadId,
                (event) =>
                  event.type === "provider-thread.updated" &&
                  (event.payload.pendingBackgroundTasks?.length ?? 0) > 0,
              );
              yield* Queue.offer(harness.events, {
                type: "provider_thread.updated",
                driver,
                providerThread: {
                  ...(yield* providerThreadOf),
                  status: "active",
                  pendingBackgroundTasks: [
                    { taskId: `${name}:build`, kind: "background_task", description: "build" },
                  ],
                },
                pendingBackgroundWork: true,
              });
              yield* Fiber.join(reported);
            }
            // The turn ends: the run's wait starts the follow-up, with no clock or capture first.
            yield* settleTurn(threadId, 1);
            yield* Fiber.join(followUpStarting);
            if (backgroundWork) {
              // The roster this work leaves behind is another test's subject; here it only must not hold the
              // follow-up back or deliver it twice.
              yield* Queue.offer(harness.events, {
                type: "provider_thread.updated",
                driver,
                providerThread: {
                  ...(yield* providerThreadOf),
                  status: "idle",
                  pendingBackgroundTasks: [],
                },
                pendingBackgroundWork: false,
              });
            }
          } else {
            // The run reaches its wait before the steer comes back; the decline starts it at once.
            yield* settleTurn(threadId, 1);
            assert.equal(
              (yield* orchestrator.getThreadProjection(threadId)).runs.find(
                (run) => run.id === continuation.runId,
              )?.status,
              "waiting",
            );
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
            const steer = Option.getOrThrow(yield* outbox.claimNext({ workerId: name }));
            assert.equal(steer.request.type, "provider-turn.steer");
            yield* executor.execute(steer);
            yield* outbox.succeed({ effectId: steer.id, workerId: name });
            yield* Fiber.join(followUpStarting);
          }
          const atWait = yield* orchestrator.getThreadProjection(threadId);
          const ended = atWait.runs.find((run) => run.id === continuation.runId)!;
          assert.equal(ended.status, "completed");
          assert.isNull(ended.checkpointId);
          assert.equal(
            ended.delegatedTaskId,
            lane === "delegated" ? NodeId.make(`${name}:task`) : undefined,
          );
          const followUp = atWait.runs.find(
            (run) => run.userMessageId === messageId && run.steerFollowUpOfRunId !== undefined,
          )!;
          const followUpTurnRunning = yield* watch(
            threadId,
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.status === "running" &&
              event.payload.runAttemptId === followUp.activeAttemptId,
          );
          yield* worker.drain();
          yield* Fiber.join(followUpTurnRunning);
          // Delivered once, as its own turn, after the continuation's capture.
          assert.deepEqual(
            started.map((turn) => withoutSenderNote(turn.message.text)),
            ["first", `${name}-notice`, name],
          );
          assert.deepEqual(steered, []);
          const after = yield* orchestrator.getThreadProjection(threadId);
          assert.lengthOf(
            after.runs.filter(
              (run) =>
                run.userMessageId === messageId &&
                (run.status === "queued" || ACTIVE_RUN_STATUSES.has(run.status)),
            ),
            1,
          );
          const captured = after.runs.find((run) => run.id === continuation.runId)!;
          assert.isNotNull(captured.checkpointId);
          const events = yield* storedEvents(threadId);
          const capturedAt = events.findIndex(
            (event) => event.type === "checkpoint.captured" && event.runId === continuation.runId,
          );
          const followUpTurnAt = events.findIndex(
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.runAttemptId === followUp.activeAttemptId &&
              event.payload.status === "running",
          );
          assert.isAtLeast(capturedAt, 0);
          assert.isAbove(followUpTurnAt, capturedAt);
          const checkpoint = after.checkpoints.find((row) => row.id === captured.checkpointId);
          assert.equal(checkpoint?.status, "ready");
          assert.notInclude(
            (checkpoint?.files ?? []).map((file) => file.path),
            "follow-up-turn.txt",
          );
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("keeps a not-delivered steer's follow-up ahead of what was queued after the steer", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const name = "follow-up-keeps-place";
      const { steers, acknowledgement } = missesFirstSteer(name);
      const { started, steered, layer, startFirstTurn, settleTurn, watch } =
        yield* waitingRunHarness(name, acknowledgement);
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        // A person's message queued before the steer is sent (an agent's would hold the automatic
        // send behind it), and the agent's own message after it, while the steer is still
        // on its way: the agent's messages to its own thread never steer.
        yield* queueFrom(orchestrator, threadId, `${name}-before`, "user");
        yield* orchestrator.dispatch(crossThreadAutoSend(threadId, name));
        yield* queueFrom(orchestrator, threadId, `${name}-after`, "self");
        yield* worker.drain();
        assert.equal(steers.attempts, 1);
        const queued = queuedRunsInDeliveryOrder(yield* orchestrator.getThreadProjection(threadId));
        assert.deepEqual(
          queued.map((run) => [run.userMessageId, run.steerFollowUpOfRunId !== undefined]),
          [
            [MessageId.make(`message:${name}-before`), false],
            [MessageId.make(`message:${name}`), true],
            [MessageId.make(`message:${name}-after`), false],
          ],
        );
        // The run's wait starts the message queued before the steer; the follow-up then steers
        // into that turn once, ahead of the later message.
        const beforeRunning = yield* watch(
          threadId,
          (event) =>
            event.type === "provider-turn.updated" &&
            event.payload.status === "running" &&
            event.payload.runAttemptId !== started[0]!.attemptId,
        );
        yield* settleTurn(threadId, 0);
        yield* worker.drain();
        yield* Fiber.join(beforeRunning);
        yield* TestClock.adjust("5 seconds");
        yield* worker.drain();
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["first", `${name}-before`],
        );
        assert.deepEqual(steered.map(withoutSenderNote), [name]);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          after.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
          [MessageId.make(`message:${name}-after`)],
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// A provider's background work, such as a Muse workflow, outlives the turn that started it and
// reports itself on the provider thread's root snapshot. A visible message that ends the run's
// wait starts the next run beside that work, and the work's later snapshots must still land.
// The turn start reads the provider thread long before it stores it running.
it.effect.each([
  { ends: "", afterTurnStartRead: false },
  { ends: ", ending after the next turn start read the thread", afterTurnStartRead: true },
] as const)(
  "keeps the provider's background work current when a visible message ends a wait$ends",
  ({ afterTurnStartRead }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const name = `waiting-ends-beside-background-work${afterTurnStartRead ? "-read" : ""}`;
        const { started, events, layer, startFirstTurn, settleTurn, watch } =
          yield* waitingRunHarness(name);
        yield* Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const threadId = yield* startFirstTurn;
          const providerThreadOf = Effect.map(
            orchestrator.getThreadProjection(threadId),
            ({ providerThreads }) =>
              providerThreads.find((candidate) => candidate.id === started[0]!.providerThread.id)!,
          );
          const workflow = {
            taskId: "task:workflow",
            kind: "background_task" as const,
            description: "ping-pong",
          };
          const reported = yield* watch(
            threadId,
            (event) =>
              event.type === "provider-thread.updated" &&
              (event.payload.pendingBackgroundTasks?.length ?? 0) > 0,
          );
          yield* Queue.offer(events, {
            type: "provider_thread.updated",
            driver,
            providerThread: {
              ...(yield* providerThreadOf),
              status: "active",
              pendingBackgroundTasks: [workflow],
            },
            pendingBackgroundWork: true,
          });
          yield* Fiber.join(reported);
          yield* settleTurn(threadId, 0);
          const first = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
          assert.equal(first.status, "waiting");

          const messageId = MessageId.make(`message:${name}`);
          const starting = yield* watch(
            threadId,
            (event) =>
              event.type === "run.updated" &&
              event.payload.userMessageId === messageId &&
              event.payload.status === "starting",
          );
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(name),
            threadId,
            messageId,
            text: "Anything else?",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            deliveryIntent: "auto",
            createdBy: "user",
            creationSource: "web",
          });
          yield* Fiber.join(starting);

          // The workflow finishes after the message's run started.
          const finished = Queue.offer(events, {
            type: "provider_thread.updated",
            driver,
            providerThread: {
              ...(yield* providerThreadOf),
              status: "idle",
              pendingBackgroundTasks: [],
            },
            pendingBackgroundWork: false,
          });
          let turnStartRead: { mockRestore: () => void } | undefined;
          if (afterTurnStartRead) {
            const cleared = yield* watch(
              threadId,
              (event) =>
                event.type === "provider-thread.updated" &&
                (event.payload.pendingBackgroundTasks ?? []).length === 0,
            );
            const projections = yield* ProjectionStore.ProjectionStoreV2;
            const getTurnStartContext = projections.getTurnStartContext;
            turnStartRead = vi
              .spyOn(projections, "getTurnStartContext")
              .mockImplementationOnce(((...args: Parameters<typeof getTurnStartContext>) =>
                getTurnStartContext(...args).pipe(
                  Effect.tap(() =>
                    finished.pipe(Effect.andThen(Fiber.join(cleared)), Effect.orDie),
                  ),
                )) as typeof getTurnStartContext);
          } else {
            yield* finished;
          }
          const nextTurnRunning = yield* watch(
            threadId,
            (event) =>
              event.type === "provider-turn.updated" &&
              event.payload.status === "running" &&
              event.payload.runAttemptId !== first.activeAttemptId,
          );
          yield* worker.drain();
          yield* Fiber.join(nextTurnRunning);
          turnStartRead?.mockRestore();
          assert.deepEqual(
            started.map((turn) => turn.message.text),
            ["first", "Anything else?"],
          );
          assert.deepEqual((yield* providerThreadOf).pendingBackgroundTasks ?? [], []);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

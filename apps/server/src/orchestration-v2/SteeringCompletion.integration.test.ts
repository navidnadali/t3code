import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  EventId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

it.effect.each(
  (
    [
      [false, "during delivery"],
      [false, "before dispatch"],
      [true, "after delivery"],
      [true, "without native steering"],
      [true, "with interrupting native steering"],
      [true, "settled only"],
      [false, "during continuation"],
    ] as const
  ).map(([mailbox, timing]) => ({
    mailbox,
    timing,
    label: mailbox ? "mailbox notification" : "steering",
  })),
)("delivers $label when completion wins $timing", ({ mailbox, timing }) =>
  Effect.scoped(
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
                  steerCalls += 1;
                  if (timing === "after delivery" || timing === "during continuation") return;
                  yield* Deferred.succeed(steerEntered, undefined);
                  yield* Deferred.await(rejectSteer);
                  return yield* new ProviderAdapter.ProviderAdapterSteerRunError({
                    driver,
                    providerThreadId: turn.providerThread.id,
                    providerTurnId: turn.providerTurnId,
                    cause: "turn already completed",
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
          dispatchMode: mailbox
            ? { type: "queue_after_active" }
            : { type: "steer_active", targetRunId: first.runId },
          createdBy: mailbox ? "agent" : "user",
          creationSource: mailbox ? "server" : "web",
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
          timing === "during delivery" ? yield* worker.runOnce.pipe(Effect.forkScoped) : null;
        if (delivery !== null) yield* Deferred.await(steerEntered);
        const completed = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === first.runId &&
            event.payload.status === "waiting",
        );
        const projection = yield* orchestrator.getThreadProjection(threadId);
        const turn = projection.providerTurns[0]!;
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
        yield* Fiber.join(completed);
        if (delivery !== null) {
          yield* Deferred.succeed(rejectSteer, undefined);
          yield* Fiber.join(delivery);
          const requeued = yield* orchestrator.getThreadProjection(threadId);
          const item = requeued.turnItems.find(
            (item) => item.type === "user_message" && item.messageId === messageId,
          );
          assert.equal(item?.status, "pending");
          assert.isNull(item?.startedAt);
          assert.isNull(item?.completedAt);
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
        assert.equal(steerCalls, timing === "during delivery" ? 1 : 0);
        const final = yield* orchestrator.getThreadProjection(threadId);
        if (!mailbox) {
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
            mailbox
              ? item.type === "notification"
              : item.type === "user_message" && item.messageId === messageId,
          ).length,
          1,
        );
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
  ),
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
  databaseLayer?: typeof SqlitePersistence.layerMemory,
) {
  const cwd = yield* checkpointWorkspace(name);
  const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
  const started: ProviderAdapter.ProviderAdapterV2TurnInput[] = [];
  const steered: string[] = [];
  const interrupted: ProviderTurnId[] = [];
  const capabilities = {
    ...CodexProviderCapabilitiesV2,
    turns: {
      ...CodexProviderCapabilitiesV2.turns,
      supportsActiveSteering: canSteer,
      supportsSteeringByInterruptRestart: canRestart,
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
        const startTurn = (turn: ProviderAdapter.ProviderAdapterV2TurnInput) =>
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
            Effect.sync(() => {
              steered.push(turn.message.text);
            }),
          interruptTurn: ({ providerTurnId }) =>
            Effect.sync(() => {
              interrupted.push(providerTurnId);
            }).pipe(Effect.andThen(interruptTurn)),
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
  const layer = ProviderReplayHarness.layerWithRegistry(
    { name },
    ProviderAdapterRegistry.layerSingle(adapter),
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
    return threadId;
  });
  return { cwd, events, started, steered, interrupted, layer, startFirstTurn };
});

it.effect("sends a selected held message once and preserves the remaining queue order", () =>
  Effect.scoped(
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
  ),
);

it.effect("force sends multiple queued messages and can force send a requeued message again", () =>
  Effect.scoped(
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
  ),
);

it.effect("keeps a message queued when send now races with a running turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("send-queued-race");
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
  ),
);

// regression: #19210 a provider that can restart the turn must still steer, since restarting ends its native subagents
it.effect(
  "steers a changed turn-scoped selection into a running turn the provider could restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
          "steering-selection-change",
          true,
        );
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
          assert.equal(back.thread.providerInstanceId, instanceId);
          assert.deepEqual(back.thread.modelSelection, runSelection);
        }).pipe(Effect.provide(layer));
      }),
    ),
);

it.effect("starts a steer that missed the turn on the saved next-turn selection", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { events, started, steered, layer, startFirstTurn } = yield* nextTurnSelectionHarness(
        "steering-selection-follow-up",
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
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(steered, []);
        assert.equal(started.length, 2);
        assert.equal(started[1]?.message.messageId, MessageId.make("message:steer"));
        assert.deepEqual(started[1]?.modelSelection, composerSelection);
        assert.deepEqual(projection.thread.modelSelection, composerSelection);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

// regression: #26695 cancelling a Copilot prompt to steer it kills its background agents
it.effect("queues a steer until the turn ends when the provider cannot steer it", () =>
  Effect.scoped(
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
  ),
);

// Codex flushes held drafts after the final answer; reserve their pre-steer position before turn end.
it.effect("keeps a held commentary draft before the steer when turn end flushes it", () =>
  Effect.scoped(
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
        assert.deepEqual(items, [
          "message:first",
          "draft",
          "commentary",
          "message:steer",
          "answer",
        ]);
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("steers a promoted queued message with the selection it was queued with", () =>
  Effect.scoped(
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
  ),
);

it.effect("marks the model switch on a promoted message that restarts the turn", () =>
  Effect.scoped(
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
  ),
);

it.effect("keeps a queued message queued when promoted into a run that Stop reached", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { started, steered, layer, startFirstTurn } =
        yield* nextTurnSelectionHarness("steering-after-stop");
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        const first = started[0]!;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("queued"),
          threadId,
          messageId: MessageId.make("message:queued"),
          text: "queued",
          attachments: [],
          modelSelection: runSelection,
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
        const queuedRun = (yield* orchestrator.getThreadProjection(threadId)).runs.find(
          (run) => run.status === "queued",
        )!;
        // The worker has not delivered the interrupt yet, so the run still reads as running.
        yield* orchestrator.dispatch({
          type: "run.interrupt",
          commandId: CommandId.make("stop"),
          threadId,
          runId: first.runId,
          holdQueue: true,
        });

        const promoteError = yield* orchestrator
          .dispatch({
            type: "queued-message.promote-to-steer",
            commandId: CommandId.make("promote"),
            threadId,
            queuedRunId: queuedRun.id,
            targetRunId: first.runId,
          })
          .pipe(Effect.flip);
        yield* worker.drain();

        const projection = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(promoteError._tag, "OrchestratorDispatchError");
        assert.deepEqual(steered, []);
        const queued = projection.runs.find((run) => run.id === queuedRun.id);
        assert.equal(queued?.status, "queued");
        assert.isTrue(queued?.queueHeld);
      }).pipe(Effect.provide(layer));
    }),
  ),
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
            assert.isTrue(yield* worker.runOnce);
            yield* switchWorktree;
            yield* sendNext;
            yield* worker.drain();
          } else {
            yield* switchWorktree;
            yield* sendNext;
            // The session the selector stopped ends the turn.
            const turn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
            const stopped = yield* orchestrator.streamDomainEvents.pipe(
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

// The provider completes the run's turn; the run waits for its checkpoint, which the worker takes.
const endTurnOf = (
  harness: Effect.Success<ReturnType<typeof nextTurnSelectionHarness>>,
  threadId: ThreadId,
  runId: Effect.Success<ReturnType<typeof nextTurnSelectionHarness>>["started"][number]["runId"],
  runOrdinal: number,
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
    const nextRun = (dir: string, file: string) =>
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
  Effect.scoped(
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
  ),
);

const goalHarness = Effect.fn("goalHarness")(function* (name: string) {
  const harness = yield* nextTurnSelectionHarness(name);
  const send = (id: string, text: string, delivery: "queue" | "auto") =>
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

it.effect("stops the running turn and starts a goal sent into it next", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { steered, layer, startFirstTurn, send, read } = yield* goalHarness("goal-steer");
      yield* Effect.gen(function* () {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const threadId = yield* startFirstTurn;
        yield* send("queued", "queued", "queue");
        yield* send("goal", "/goal all tests pass", "auto");
        yield* worker.drain();

        assert.deepEqual(steered, []);
        assert.deepEqual(yield* read(threadId), {
          queue: ["queued"],
          started: ["first", "/goal all tests pass"],
          stopRequested: true,
        });
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("queues a goal clear without stopping work when no goal drives it", () =>
  Effect.scoped(
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
  ),
);

it.effect("promotes a queued goal by stopping the running turn", () =>
  Effect.scoped(
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
  ),
);

it.effect.each(["next", "/goal ship it"])(
  "promoting %s into a compaction stops it and sends it next",
  (promoted) =>
    Effect.scoped(
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
          yield* send("next", promoted, "queue");
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
    ),
);

it.effect("stops a turn that is still starting when a goal is sent into it", () =>
  Effect.scoped(
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
              projection.messages.find((message) => message.id === run.userMessageId)?.text ===
              text,
          )?.status;
        assert.equal(statusOf("first"), "interrupted");
        assert.deepEqual(
          started.map((turn) => turn.message.text),
          ["/goal all tests pass"],
        );
      }).pipe(Effect.provide(layer));
    }),
  ),
);

it.effect("stops the running turn when a goal clear ends the goal driving it", () =>
  Effect.scoped(
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
  ),
);

it.effect("a side chat that opens with /goal gets its context on the next question", () =>
  Effect.scoped(
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
  ),
);

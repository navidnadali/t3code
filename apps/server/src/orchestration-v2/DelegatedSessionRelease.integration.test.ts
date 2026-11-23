import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  type RunId,
  ThreadId,
  TurnItemId,
  isOrchestrationV2WorkActive,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2PendingBackgroundTask,
} from "@t3tools/contracts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { ClaudeProviderCapabilitiesV2 } from "./Adapters/ClaudeAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const driver = ProviderDriverKind.make("claudeAgent");
const instanceId = ProviderInstanceId.make("claudeAgent");
const modelSelection = { instanceId, model: "claude-test" };
// A Monitor the child awaits, so its task's result waits for the watch to end.
const monitor = {
  taskId: "monitor-deploy",
  description: "Watch the deploy log",
  kind: "monitor",
  awaited: true,
} as const satisfies OrchestrationV2PendingBackgroundTask;

interface Turn {
  readonly turn: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly events: Queue.Queue<
    ProviderAdapter.ProviderAdapterV2Event,
    ProviderAdapter.ProviderAdapterV2Error
  >;
}

/**
 * A Claude parent that delegated one task, on a provider with one process per thread, as
 * Claude runs in production. A run's provider turn starts, then reports only what the test offers.
 */
const delegatedClaudeChild = <A, E, R>(
  name: string,
  body: (input: {
    readonly turns: ReadonlyArray<Turn>;
    readonly parentThreadId: ThreadId;
    readonly child: Turn;
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace(name);
    const turns: Array<Turn> = [];
    const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
      instanceId,
      driver,
      getCapabilities: () => Effect.succeed(ClaudeProviderCapabilitiesV2),
      planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
      openSession: (input) =>
        Effect.gen(function* () {
          const now = yield* DateTime.now;
          const events = yield* Queue.unbounded<
            ProviderAdapter.ProviderAdapterV2Event,
            ProviderAdapter.ProviderAdapterV2Error
          >();
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
              capabilities: ClaudeProviderCapabilitiesV2,
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
                nativeThreadRef: { driver, nativeId: `native:${threadId}`, strength: "strong" },
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
                turns.push({ turn, events });
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  providerTurn: {
                    id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                    providerThreadId: turn.providerThread.id,
                    nodeId: turn.rootNodeId,
                    runAttemptId: turn.attemptId,
                    nativeTurnRef: null,
                    ordinal: turn.providerTurnOrdinal,
                    status: "running",
                    startedAt: now,
                    completedAt: null,
                  },
                });
              }),
            steerTurn: () => Effect.die("unused"),
            interruptTurn: () => Effect.die("unused"),
            respondToRuntimeRequest: () => Effect.die("unused"),
            readThreadSnapshot: () => Effect.die("unused"),
            rollbackThread: () => Effect.die("unused"),
            forkThread: () => Effect.die("unused"),
          };
        }),
    };
    return yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const parentThreadId = ThreadId.make(`thread:${name}-parent`);
      const parentRunning = yield* turnRunning();
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make(`${name}-create-parent`),
        threadId: parentThreadId,
        projectId: ProjectId.make(`project:${name}`),
        title: "Parent",
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
        commandId: CommandId.make(`${name}-start-parent`),
        threadId: parentThreadId,
        messageId: MessageId.make(`message:${name}-start-parent`),
        text: "Delegate the deploy",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      yield* Fiber.join(parentRunning);
      const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs[0]!;
      const childRunning = yield* turnRunning();
      yield* orchestrator.dispatch({
        type: "delegated_task.request",
        commandId: CommandId.make(`${name}-delegate`),
        parentThreadId,
        parentRunId: parentRun.id,
        parentNodeId: parentRun.rootNodeId!,
        task: "Deploy and watch the log",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        completionWake: "always",
        createdBy: "agent",
        creationSource: "mcp",
      });
      yield* worker.drain();
      yield* Fiber.join(childRunning);
      // The parent's own turn ends, so the child's result wakes it in a run of its own.
      const parent = turns.find((entry) => entry.turn.threadId === parentThreadId)!;
      const parentEnded = yield* runEnded(parent.turn.runId);
      yield* endTurn(parent, "Delegated.");
      yield* Fiber.join(parentEnded);
      yield* worker.drain();
      const child = turns.find((entry) => entry.turn.threadId !== parentThreadId)!;
      return yield* body({ turns, parentThreadId, child });
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerWithRegistry(
          { name },
          ProviderAdapterRegistry.layerSingle(adapter),
          { runEffectWorker: false },
        ),
      ),
    );
  });

// Read the cursor before forking: events offered right after a watch can commit before a
// live-tail subscriber has read its starting sequence.
const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const afterSequence = yield* eventSink.latestSequence();
    return yield* eventSink.stream({ afterSequence }).pipe(
      Stream.map((stored) => stored.event),
      Stream.filter(predicate),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  });

const turnRunning = () =>
  watch((event) => event.type === "provider-turn.updated" && event.payload.status === "running");

/** The run left its turn: ended, failed, or waiting for its checkpoint. */
const runEnded = (runId: RunId) =>
  watch(
    (event) =>
      event.type === "run.updated" &&
      event.payload.id === runId &&
      event.payload.status !== "queued" &&
      event.payload.status !== "preparing" &&
      event.payload.status !== "starting" &&
      event.payload.status !== "running",
  );

/** The parent's task settled, which is when a finished delegate's process is released. */
const taskSettled = (parentThreadId: ThreadId) =>
  watch(
    (event) =>
      event.type === "subagent.updated" &&
      event.threadId === parentThreadId &&
      event.payload.origin === "app_owned" &&
      ["completed", "failed", "cancelled", "interrupted"].includes(event.payload.status),
  );

const endTurn = Effect.fnUntraced(function* (entry: Turn, reply: string) {
  const now = yield* DateTime.now;
  const providerTurnId = ProviderTurnId.make(`provider-turn:${entry.turn.attemptId}`);
  const messageId = MessageId.make(`message:reply:${entry.turn.runId}`);
  const at = { threadId: entry.turn.threadId, runId: entry.turn.runId };
  for (const event of [
    {
      type: "message.updated",
      driver,
      message: {
        ...at,
        id: messageId,
        nodeId: entry.turn.rootNodeId,
        role: "assistant",
        text: reply,
        attachments: [],
        streaming: false,
        createdBy: "agent",
        creationSource: "provider",
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      type: "turn_item.updated",
      driver,
      turnItem: {
        ...at,
        id: TurnItemId.make(`turn-item:reply:${entry.turn.runId}`),
        nodeId: entry.turn.rootNodeId,
        providerThreadId: entry.turn.providerThread.id,
        providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 50,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "assistant_message",
        messageId,
        text: reply,
        streaming: false,
      },
    },
    {
      type: "provider_turn.updated",
      driver,
      providerTurn: {
        id: providerTurnId,
        providerThreadId: entry.turn.providerThread.id,
        nodeId: entry.turn.rootNodeId,
        runAttemptId: entry.turn.attemptId,
        nativeTurnRef: null,
        ordinal: entry.turn.providerTurnOrdinal,
        status: "completed",
        startedAt: now,
        completedAt: now,
      },
    },
    {
      type: "turn.terminal",
      driver,
      providerThreadId: entry.turn.providerThread.id,
      providerTurnId,
      runOrdinal: entry.turn.runOrdinal,
      status: "completed",
      failure: null,
      threadDisposition: "reusable",
    },
  ] satisfies ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) {
    yield* Queue.offer(entry.events, event);
  }
});

/** Claude reports its background task roster on the provider thread. */
const reportBackgroundTasks = (
  entry: Turn,
  pendingBackgroundTasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask>,
) =>
  Effect.flatMap(DateTime.now, (now) =>
    Queue.offer(entry.events, {
      type: "provider_thread.updated",
      driver,
      providerThread: {
        ...entry.turn.providerThread,
        status: "active",
        pendingBackgroundTasks,
        updatedAt: now,
      },
    }),
  );

const childOutcome = Effect.fnUntraced(function* (childThreadId: ThreadId, runId: RunId) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
  const child = yield* orchestrator.getThreadProjection(childThreadId);
  const live = yield* Effect.forEach(child.providerSessions, (session) =>
    sessions.isLive(session.id),
  );
  return {
    run: child.runs.find((run) => run.id === runId)?.status,
    errors: child.turnItems.flatMap((item) =>
      item.type === "error" && item.runId === runId ? [item.failure.message] : [],
    ),
    replies: child.messages.flatMap((message) =>
      message.role === "assistant" && message.runId === runId ? [message.text] : [],
    ),
    stops: child.turnItems.flatMap((item) =>
      item.type === "run_interrupt_result" && item.runId === runId ? [item.message] : [],
    ),
    attachedSessions: child.providerSessions.length,
    liveSessions: live.filter(Boolean).length,
  };
});

// A Monitor event wakes the child in a continuation run. When the agent stops the
// Monitor in that run, or the Monitor ends by itself, the task's result is ready while the run
// is still live. Releasing the finished delegate's process then failed the live run with "The
// provider event stream closed unexpectedly".
it.effect.each([
  { name: "the agent stops it with TaskStop", stoppedByAgent: true },
  { name: "it exits by itself", stoppedByAgent: false },
])(
  "a Monitor wake run whose last background task ends mid-run ($name) keeps its process and completes",
  ({ stoppedByAgent }) =>
    delegatedClaudeChild(
      stoppedByAgent ? "monitor-task-stop" : "monitor-exit",
      Effect.fnUntraced(function* ({ turns, parentThreadId, child }) {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const childThreadId = child.turn.threadId;
        // The child's first run starts a Monitor it awaits, then ends its turn.
        const firstEnded = yield* runEnded(child.turn.runId);
        yield* reportBackgroundTasks(child, [monitor]);
        yield* endTurn(child, "Deploy started; watching the log.");
        yield* Fiber.join(firstEnded);
        yield* worker.drain();
        const waiting = yield* orchestrator.getThreadProjection(parentThreadId);
        assert.isTrue(
          waiting.subagents.some(
            (task) => task.origin === "app_owned" && isOrchestrationV2WorkActive(task.status),
          ),
        );

        // A Monitor event wakes the child, as ProviderContinuationService dispatches it.
        const wakeRunning = yield* turnRunning();
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("monitor-event"),
          threadId: childThreadId,
          messageId: MessageId.make("message:monitor-event"),
          text: "Background activity updated",
          notification: {
            source: { kind: "monitor" },
            outcome: "updated",
            summary: "deploy step 3 of 3 done",
          },
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "provider",
        });
        yield* worker.drain();
        yield* Fiber.join(wakeRunning);
        const wake = turns.at(-1)!;
        assert.equal(wake.turn.threadId, childThreadId);
        assert.notEqual(wake.turn.runId, child.turn.runId);

        // Mid-run, the Monitor ends and the roster empties: the task's result is now ready.
        const settled = yield* taskSettled(parentThreadId);
        const wakeEnded = yield* runEnded(wake.turn.runId);
        if (stoppedByAgent) {
          const now = yield* DateTime.now;
          yield* Queue.offer(wake.events, {
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: TurnItemId.make("turn-item:task-stop"),
              threadId: childThreadId,
              runId: wake.turn.runId,
              nodeId: wake.turn.rootNodeId,
              providerThreadId: wake.turn.providerThread.id,
              providerTurnId: ProviderTurnId.make(`provider-turn:${wake.turn.attemptId}`),
              nativeItemRef: { driver, nativeId: "toolu_task_stop", strength: "strong" },
              parentItemId: null,
              ordinal: 10,
              status: "completed",
              title: "TaskStop",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              type: "dynamic_tool",
              toolName: "TaskStop",
              input: { task_id: monitor.taskId },
              output: "Stopped the monitor.",
            },
          });
        }
        yield* reportBackgroundTasks(wake, []);
        yield* Fiber.join(settled);
        // The settle holds the parent's lock through its release; recovery queues behind it.
        yield* orchestrator.recoverDelegatedTasks;
        yield* worker.drain();

        // The wake run goes on to its final message.
        yield* endTurn(wake, "Deploy verified.");
        yield* Fiber.join(wakeEnded);
        yield* worker.drain();
        assert.deepEqual(yield* childOutcome(childThreadId, wake.turn.runId), {
          run: "completed",
          errors: [],
          replies: ["Deploy verified."],
          stops: [],
          attachedSessions: 1,
          liveSessions: 1,
        });
        const parent = yield* orchestrator.getThreadProjection(parentThreadId);
        assert.equal(
          parent.subagents.find((task) => task.childThreadId === childThreadId)?.status,
          "completed",
        );
      }),
    ),
);

// A finished delegate with nothing left running releases its process when it settles.
it.effect("a settled delegate with no live run and no background work is released", () =>
  delegatedClaudeChild(
    "settled-delegate",
    Effect.fnUntraced(function* ({ parentThreadId, child }) {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const settled = yield* taskSettled(parentThreadId);
      const ended = yield* runEnded(child.turn.runId);
      yield* endTurn(child, "Deployed.");
      yield* Fiber.join(ended);
      yield* worker.drain();
      yield* Fiber.join(settled);
      yield* orchestrator.recoverDelegatedTasks;
      yield* worker.drain();
      assert.deepEqual(yield* childOutcome(child.turn.threadId, child.turn.runId), {
        run: "completed",
        errors: [],
        replies: ["Deployed."],
        stops: [],
        attachedSessions: 0,
        liveSessions: 0,
      });
    }),
  ),
);

// T3 releases a process under a live run when the provider is logged out, a thread detaches
// from it, or the session is closed. The run reports T3's own reason and ends interrupted; a
// provider whose event stream fails still fails the run with the stream message.
it.effect.each([
  {
    name: "a logout T3 ran",
    cut: "logout",
    expected: {
      run: "interrupted",
      errors: [],
      stops: [
        "T3 Code ended this turn when it closed the provider session: Provider instance claudeAgent logged out.",
      ],
    },
  },
  {
    name: "the provider's event stream failing",
    cut: "stream",
    expected: {
      run: "failed",
      errors: [
        "The provider event stream closed unexpectedly. Retry the turn; if it keeps failing, check the provider and server logs.",
      ],
      stops: [],
    },
  },
] as const)("a live delegate run cut by $name reports why", ({ cut, expected }) =>
  delegatedClaudeChild(
    `cut-by-${cut}`,
    Effect.fnUntraced(function* ({ child }) {
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const ended = yield* runEnded(child.turn.runId);
      if (cut === "logout") {
        yield* sessions.closeInstance(instanceId);
      } else {
        yield* Queue.fail(
          child.events,
          new ProviderAdapter.ProviderAdapterEventStreamError({
            driver,
            providerSessionId: child.turn.providerThread.providerSessionId!,
            cause: "provider process exited",
          }),
        );
      }
      yield* Fiber.join(ended);
      yield* worker.drain();
      const { run, errors, stops } = yield* childOutcome(child.turn.threadId, child.turn.runId);
      const outcome: {
        readonly run: string | undefined;
        readonly errors: ReadonlyArray<string>;
        readonly stops: ReadonlyArray<string>;
      } = { run, errors, stops };
      assert.deepEqual(outcome, expected);
    }),
  ),
);

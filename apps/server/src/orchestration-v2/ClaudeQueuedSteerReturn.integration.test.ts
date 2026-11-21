import type { SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ClaudeSettings,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ClaudeAdapterV2 from "./Adapters/ClaudeAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX } from "./QueuedDeliveryPromotion.ts";

// At priority "next" Claude Code reads a steer with the next tool result. With no tool call left
// it reports the steer queued and answers it in a turn of its own after the current one. A steer
// it queued but never ran, because Claude cancelled it, exited, or was stopped, comes back to the
// thread as its next queued message, exactly once.
const sessionId = "queued-steer-session";
const settings = Schema.decodeSync(ClaudeSettings)({});
const modelSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-sonnet-4-6",
};
const STEER_TEXT = "Also name the owner.";

function frame(value: unknown): SDKMessage {
  return value as SDKMessage;
}

const reply = frame({
  type: "assistant",
  uuid: "reply",
  session_id: sessionId,
  parent_tool_use_id: null,
  message: {
    id: "reply",
    role: "assistant",
    type: "message",
    model: modelSelection.model,
    content: [{ type: "text", text: "Summary written." }],
    stop_reason: "end_turn",
    stop_sequence: null,
    usage: { input_tokens: 1, output_tokens: 1 },
  },
});

const result = (uuid: string, echo: string) =>
  frame({
    type: "result",
    subtype: "success",
    uuid,
    session_id: sessionId,
    is_error: false,
    num_turns: 1,
    result: "Summary written.",
    stop_reason: "end_turn",
    terminal_reason: "completed",
    permission_denials: [],
    duration_ms: 1,
    duration_api_ms: 1,
    total_cost_usd: 0,
    usage: { input_tokens: 1, output_tokens: 1 },
    modelUsage: {},
    user_message_uuid: echo,
  });

const lifecycle = (commandUuid: string, state: string) =>
  frame({ type: "command_lifecycle", command_uuid: commandUuid, state, session_id: sessionId });

const ends = ["Claude cancels it", "Claude exits", "a person stops the thread"] as const;

it.effect.each(ends)(
  "a steer Claude queued and never ran comes back as the thread's next message, once (%s)",
  (end) =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("claude-queued-steer-return");
      // One stream per Claude process: a returned steer's own turn opens a new one.
      const processes: Array<Queue.Queue<SDKMessage>> = [];
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
            Effect.gen(function* () {
              const sdkMessages = yield* Queue.unbounded<SDKMessage>();
              processes.push(sdkMessages);
              return {
                messages: Stream.fromQueue(sdkMessages),
                offer: (message: SDKUserMessage) =>
                  Effect.sync(() => {
                    offers.push(message);
                  }),
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
                interrupt: Effect.void,
                close: Queue.shutdown(sdkMessages),
              };
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
        const threadId = ThreadId.make("thread:queued-steer-return");
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
          projectId: ProjectId.make("project:queued-steer-return"),
          title: "Queued steer",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const running = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("first"),
          threadId,
          messageId: MessageId.make("first"),
          text: "Write the summary.",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* Fiber.join(running);
        const parent = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
        const steerId = MessageId.make("steer");
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId: steerId,
          text: STEER_TEXT,
          attachments: [],
          dispatchMode: { type: "steer_active", targetRunId: parent.id },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        assert.equal(offers[1]?.priority, "next");
        const claude = processes[0]!;
        // Claude reports the steer queued, then writes its reply; once the reply is in, so is that.
        const replied = yield* watch(
          (event) => event.type === "message.updated" && event.payload.text === "Summary written.",
        );
        yield* Queue.offer(claude, lifecycle(offers[1]!.uuid!, "queued"));
        yield* Queue.offer(claude, reply);
        yield* Fiber.join(replied);
        // The steer's message in a run of its own: queued again.
        const requeued = yield* watch(
          (event) =>
            (event.type === "run.created" || event.type === "run.updated") &&
            event.payload.userMessageId === steerId,
        );
        const ended = yield* watch(
          (event) =>
            event.type === "run.updated" &&
            event.payload.id === parent.id &&
            (event.payload.status === "waiting" ||
              event.payload.status === "completed" ||
              event.payload.status === "failed" ||
              event.payload.status === "interrupted"),
        );
        if (end === "Claude cancels it") {
          yield* Queue.offer(claude, result("first-result", offers[0]!.uuid!));
          yield* Queue.offer(claude, lifecycle(offers[1]!.uuid!, "cancelled"));
        } else if (end === "Claude exits") {
          yield* Queue.shutdown(claude);
        } else {
          yield* orchestrator.dispatch({
            type: "thread.stop",
            commandId: CommandId.make("stop"),
            threadId,
          });
          yield* worker.drain();
        }
        yield* Fiber.join(ended);
        yield* worker.drain();
        const steerRuns = Effect.map(orchestrator.getThreadProjection(threadId), (projection) =>
          projection.runs.filter((run) => run.userMessageId === steerId),
        );
        yield* Fiber.join(requeued);
        yield* worker.drain();
        const returned = yield* steerRuns;
        assert.lengthOf(returned, 1, "the steer is queued again as its own run");
        assert.notEqual(returned[0]!.id, parent.id);
        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.equal(
          after.messages.find((message) => message.id === steerId)?.runId,
          returned[0]!.id,
        );
        if (end === "a person stops the thread") {
          assert.equal(returned[0]!.status, "queued");
          assert.isTrue(returned[0]!.queueHeld, "Stop holds it, for the person to resume");
        }
        // Recovery and the queue's own passes find it already back: exactly once.
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.resumeQueuedRuns;
        yield* worker.drain();
        assert.lengthOf(yield* steerRuns, 1);
        if (end === "a person stops the thread") {
          // The person resumes; the steer runs once, as its own turn, and its end returns nothing.
          const steerRunning = yield* watch(
            (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
          );
          yield* orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make("resume"),
            threadId,
          });
          yield* worker.drain();
          yield* Fiber.join(steerRunning);
          const own = offers.at(-1)!;
          assert.equal(JSON.stringify(own.message.content).includes("Also name the owner."), true);
          const offered = offers.length;
          const steerRun = (yield* steerRuns)[0]!;
          const done = yield* watch(
            (event) =>
              event.type === "run.updated" &&
              event.payload.id === steerRun.id &&
              (event.payload.status === "waiting" || event.payload.status === "completed"),
          );
          yield* Queue.offer(processes.at(-1)!, result("steer-own-result", own.uuid!));
          yield* Fiber.join(done);
          yield* worker.drain();
          yield* orchestrator.recoverDelegatedTasks;
          yield* worker.drain();
          assert.lengthOf(yield* steerRuns, 1, "no second run for the steer");
          assert.equal(offers.length, offered, "no second prompt");
        }
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "claude-queued-steer-return" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }).pipe(
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

// A returned steer comes back as the thread's next message, ahead of anything queued (a merged
// steer's messages in the order it carried them).
const orders = [
  "two queued messages merged into the steer",
  "a message queued after the steer went",
  "Stop, with a notice queued before it and a message after it",
] as const;

it.effect.each(orders)("a returned steer comes back ahead of the queue (%s)", (order) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace("claude-queued-steer-order");
    const processes: Array<Queue.Queue<SDKMessage>> = [];
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
          Effect.gen(function* () {
            const sdkMessages = yield* Queue.unbounded<SDKMessage>();
            processes.push(sdkMessages);
            return {
              messages: Stream.fromQueue(sdkMessages),
              offer: (message: SDKUserMessage) =>
                Effect.sync(() => {
                  offers.push(message);
                }),
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
              interrupt: Effect.void,
              close: Queue.shutdown(sdkMessages),
            };
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
      const threadId = ThreadId.make("thread:queued-steer-order");
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
        projectId: ProjectId.make("project:queued-steer-order"),
        title: "Queued steer order",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      const running = yield* watch(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      );
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("first"),
        threadId,
        messageId: MessageId.make("first"),
        text: "Write the summary.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      yield* Fiber.join(running);
      const parent = (yield* orchestrator.getThreadProjection(threadId)).runs[0]!;
      const queue = (id: string) =>
        orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(id),
          threadId,
          messageId: MessageId.make(id),
          text: `Message ${id}.`,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "user",
          creationSource: "web",
        });
      const runOf = (id: string) =>
        Effect.map(orchestrator.getThreadProjection(threadId), (projection) =>
          // A promoted message's first run was cancelled; its newest run is the one it has now.
          projection.runs.findLast(
            (run) => run.userMessageId === MessageId.make(id) && run.id !== parent.id,
          ),
        );
      // The messages the steer carries, in order, and one that must stay behind them.
      let steered: ReadonlyArray<string>;
      let behind: string | undefined;
      if (order === "two queued messages merged into the steer") {
        yield* queue("merged-a");
        yield* queue("merged-b");
        const a = (yield* runOf("merged-a"))!;
        const b = (yield* runOf("merged-b"))!;
        yield* orchestrator.dispatch({
          type: "queued-message.promote-to-steer",
          // Only automatic Send now merges queued messages into one steer.
          commandId: CommandId.make(`${QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX}merged`),
          threadId,
          queuedRunId: a.id,
          targetRunId: parent.id,
          mergedQueuedRunIds: [b.id],
        });
        steered = ["merged-a", "merged-b"];
      } else {
        const stop = order === "Stop, with a notice queued before it and a message after it";
        // A hidden notice waits in the queue ahead of the steer.
        if (stop)
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("notice"),
            threadId,
            messageId: MessageId.make("notice"),
            text: "A notice for the agent.",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "server",
            notification: {
              source: { kind: "system" },
              outcome: "updated",
              summary: "A notice",
            },
          });
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("steer"),
          threadId,
          messageId: MessageId.make("steer"),
          text: STEER_TEXT,
          attachments: [],
          dispatchMode: { type: "steer_active", targetRunId: parent.id },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* queue("later");
        steered = ["steer"];
        behind = "later";
      }
      yield* worker.drain();
      assert.equal(offers[1]?.priority, "next");
      const claude = processes[0]!;
      const replied = yield* watch(
        (event) => event.type === "message.updated" && event.payload.text === "Summary written.",
      );
      yield* Queue.offer(claude, lifecycle(offers[1]!.uuid!, "queued"));
      yield* Queue.offer(claude, reply);
      yield* Fiber.join(replied);
      const last = MessageId.make(steered.at(-1)!);
      const requeued = yield* watch(
        (event) =>
          (event.type === "run.created" || event.type === "run.updated") &&
          event.payload.userMessageId === last,
      );
      // The run ends (its checkpoint is captured), then the steer's messages come back.
      const ended = yield* watch(
        (event) =>
          event.type === "run.updated" &&
          event.payload.id === parent.id &&
          (event.payload.status === "waiting" ||
            event.payload.status === "completed" ||
            event.payload.status === "interrupted"),
      );
      const stopped = order === "Stop, with a notice queued before it and a message after it";
      if (stopped) {
        yield* orchestrator.dispatch({
          type: "thread.stop",
          commandId: CommandId.make("stop"),
          threadId,
        });
        yield* worker.drain();
      } else {
        yield* Queue.offer(claude, result("first-result", offers[0]!.uuid!));
        yield* Queue.offer(claude, lifecycle(offers[1]!.uuid!, "cancelled"));
      }
      yield* Fiber.join(ended);
      yield* worker.drain();
      yield* Fiber.join(requeued);
      yield* worker.drain();
      const head = (yield* runOf(steered[0]!))!;
      if (stopped) {
        // Stop holds the queue; the steer is still its next visible message, ahead of the one
        // queued after it went (a move before a hidden notice would put it last).
        assert.isTrue(head.queueHeld);
        const after = yield* orchestrator.getThreadProjection(threadId);
        const visible = after.runs
          .filter(
            (run) => run.status === "queued" && run.userMessageId !== MessageId.make("notice"),
          )
          .toSorted(
            (left, right) =>
              (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal) ||
              left.ordinal - right.ordinal,
          )
          .map((run) => run.userMessageId);
        assert.deepEqual(visible, [MessageId.make("steer"), MessageId.make("later")]);
        return;
      }
      assert.notEqual(head.status, "queued", `${steered[0]} runs first`);
      for (const id of [...steered.slice(1), ...(behind === undefined ? [] : [behind])]) {
        const run = (yield* runOf(id))!;
        assert.equal(run.status, "queued", `${id} waits behind it`);
      }
      if (steered.length > 1) {
        const second = (yield* runOf(steered[1]!))!;
        assert.isAbove(second.ordinal, head.ordinal);
      }
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerWithRegistry(
          { name: "claude-queued-steer-order" },
          ProviderAdapterRegistry.layerSingle(adapter),
          { runEffectWorker: false },
        ),
      ),
    );
  }).pipe(
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

// A merged steer's first message was queued again (held by Stop, ahead of the queue) before the
// server stopped; the second comes back behind it and ahead of the rest, in one command.
it.effect("startup brings back the rest of a merged steer behind what came back before", () =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace("claude-queued-steer-merged-recovery");
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
        open: () => Effect.die("this row starts no Claude process"),
        forkSession: () => Effect.die("unused"),
        subagentLaunchToolUseId: () => Effect.succeed(null),
        subagentModel: () => Effect.succeed(null),
        assertComplete: Effect.void,
      },
    });
    yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const eventSink = yield* EventSink.EventSinkV2;
      const threadId = ThreadId.make("thread:queued-steer-merged-recovery");
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create"),
        threadId,
        projectId: ProjectId.make("project:queued-steer-merged-recovery"),
        title: "Merged steer recovery",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      const now = yield* DateTime.now;
      const stoppedRunId = RunId.make("run:merged-recovery:stopped");
      const firstRunId = RunId.make("run:merged-recovery:first-back");
      const olderRunId = RunId.make("run:merged-recovery:older");
      const older = MessageId.make("merged-recovery:older");
      const first = MessageId.make("merged-recovery:a");
      const second = MessageId.make("merged-recovery:b");
      // The stopped run's provider thread, which a queued message goes back to.
      const providerThreadId = ProviderThreadId.make("provider-thread:merged-recovery");
      const driver = ClaudeAdapterV2.CLAUDE_PROVIDER;
      const run = (id: RunId, ordinal: number, userMessageId: MessageId, extra: object) => ({
        id: EventId.make(`event:${id}`),
        type: "run.updated" as const,
        threadId,
        runId: id,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: {
          id,
          threadId,
          ordinal,
          providerInstanceId: modelSelection.instanceId,
          modelSelection,
          providerThreadId,
          userMessageId,
          rootNodeId: null,
          activeAttemptId: null,
          requestedAt: now,
          checkpointId: null,
          contextHandoffId: null,
          ...extra,
        } as never,
      });
      const message = (id: MessageId, runId: RunId) => ({
        id: EventId.make(`event:message:${id}`),
        type: "message.updated" as const,
        threadId,
        runId,
        occurredAt: now,
        payload: {
          id,
          threadId,
          runId,
          nodeId: null,
          role: "user" as const,
          text: `Message ${id}.`,
          attachments: [],
          streaming: false,
          createdBy: "user" as const,
          creationSource: "web" as const,
          createdAt: now,
          updatedAt: now,
        },
      });
      const item = (id: MessageId, runId: RunId, ordinal: number, returned: boolean) => ({
        id: EventId.make(`event:item:${id}`),
        type: "turn-item.updated" as const,
        threadId,
        runId,
        occurredAt: now,
        payload: {
          id: TurnItemId.make(`turn-item:${id}`),
          threadId,
          runId,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal,
          status: returned ? ("cancelled" as const) : ("pending" as const),
          title: null,
          startedAt: now,
          completedAt: returned ? now : null,
          updatedAt: now,
          type: "user_message" as const,
          createdBy: "user" as const,
          creationSource: "web" as const,
          messageId: id,
          inputIntent: "promoted_queued_to_steer" as const,
          text: `Message ${id}.`,
          attachments: [],
          ...(returned ? { returnedToQueue: true as const } : {}),
        },
      });
      yield* eventSink.write({
        commandId: CommandId.make("command:runtime-reconcile:merged-recovery"),
        events: [
          {
            id: EventId.make("event:merged-recovery:provider-thread"),
            type: "provider-thread.updated" as const,
            threadId,
            driver,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              id: providerThreadId,
              driver,
              providerInstanceId: modelSelection.instanceId,
              providerSessionId: null,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: { driver, nativeId: "native:merged-recovery", strength: "strong" },
              nativeConversationHeadRef: null,
              status: "active",
              firstRunOrdinal: 1,
              lastRunOrdinal: 1,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            } as never,
          },
          run(stoppedRunId, 1, MessageId.make("merged-recovery:first"), {
            status: "interrupted",
            startedAt: now,
            completedAt: now,
          }),
          // A message queued before the steer went, held by Stop, behind the first message.
          run(olderRunId, 2, older, {
            status: "queued",
            startedAt: null,
            completedAt: null,
            queueHeld: true,
            queuePosition: 2,
          }),
          message(older, olderRunId),
          // The first message came back, held and placed ahead (its own dispatch placed it),
          // before the server stopped.
          run(firstRunId, 3, first, {
            status: "queued",
            startedAt: null,
            completedAt: null,
            queueHeld: true,
            queuePosition: 1,
            steerFollowUpOfRunId: stoppedRunId,
          }),
          message(first, firstRunId),
          message(second, stoppedRunId),
          item(first, firstRunId, 2, false),
          item(second, stoppedRunId, 3, true),
          {
            id: EventId.make("event:merged-recovery:stop"),
            type: "turn-item.updated" as const,
            threadId,
            runId: stoppedRunId,
            occurredAt: now,
            payload: {
              id: TurnItemId.make("turn-item:merged-recovery:stop"),
              type: "run_interrupt_request" as const,
              threadId,
              runId: stoppedRunId,
              nodeId: null,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: 4,
              status: "completed" as const,
              title: "Interrupt requested",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              message: "Stopped by user",
              holdQueue: true,
              queueHoldGeneration: 0,
            },
          },
        ],
      });
      // One dispatch places a returned message: right after it commits, before any other
      // command (a stop could come next), the queue's order is already right.
      const orderNow = Effect.map(orchestrator.getThreadProjection(threadId), (projection) =>
        projection.runs
          .filter((candidate) => candidate.status === "queued")
          .toSorted(
            (left, right) =>
              (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal) ||
              left.ordinal - right.ordinal,
          )
          .map((candidate) => candidate.userMessageId),
      );
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`command:steer-returned:${stoppedRunId}:${second}`),
        threadId,
        messageId: second,
        text: `Message ${second}.`,
        attachments: [],
        steerFollowUpOfRunId: stoppedRunId,
        steerFollowUpAhead: true,
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      assert.deepEqual(yield* orderNow, [first, second, older]);
      yield* orchestrator.recoverDelegatedTasks;
      const after = yield* orchestrator.getThreadProjection(threadId);
      const order = after.runs
        .filter((candidate) => candidate.status === "queued")
        .toSorted(
          (left, right) =>
            (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal) ||
            left.ordinal - right.ordinal,
        )
        .map((candidate) => candidate.userMessageId);
      assert.deepEqual(order, [first, second, older]);

      // A person moved a message ahead of the first returned one; a third returned message goes
      // right behind its run's others, and the person's order stands.
      const moved = MessageId.make("merged-recovery:moved");
      const third = MessageId.make("merged-recovery:c");
      const movedRunId = RunId.make("run:merged-recovery:moved");
      const placed = Effect.map(orchestrator.getThreadProjection(threadId), (projection) =>
        projection.runs.filter((candidate) => candidate.status === "queued"),
      );
      const before = yield* placed;
      yield* eventSink.write({
        commandId: CommandId.make("command:runtime-reconcile:merged-recovery-reordered"),
        events: [
          run(movedRunId, 10, moved, {
            status: "queued",
            startedAt: null,
            completedAt: null,
            queueHeld: true,
            queuePosition: 1,
          }),
          message(moved, movedRunId),
          ...before.map((candidate) => ({
            id: EventId.make(`event:reordered:${candidate.id}`),
            type: "run.updated" as const,
            threadId,
            runId: candidate.id,
            providerInstanceId: modelSelection.instanceId,
            occurredAt: now,
            payload: {
              ...candidate,
              queuePosition: (candidate.queuePosition ?? candidate.ordinal) + 3,
            },
          })),
          message(third, stoppedRunId),
          item(third, stoppedRunId, 5, true),
        ],
      });
      yield* orchestrator.recoverDelegatedTasks;
      const reordered = (yield* placed)
        .toSorted(
          (left, right) =>
            (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal) ||
            left.ordinal - right.ordinal,
        )
        .map((candidate) => candidate.userMessageId);
      assert.deepEqual(reordered, [moved, first, second, third, older]);

      // A message tied with the last one back (same position, a later ordinal) is not let between
      // them: a fourth returned message goes right behind the third.
      const thirdRun = (yield* placed).find((candidate) => candidate.userMessageId === third)!;
      const tied = MessageId.make("merged-recovery:tied");
      const fourth = MessageId.make("merged-recovery:d");
      yield* eventSink.write({
        commandId: CommandId.make("command:runtime-reconcile:merged-recovery-tied"),
        events: [
          run(RunId.make("run:merged-recovery:tied"), 20, tied, {
            status: "queued",
            startedAt: null,
            completedAt: null,
            queueHeld: true,
            queuePosition: thirdRun.queuePosition,
          }),
          message(tied, RunId.make("run:merged-recovery:tied")),
          message(fourth, stoppedRunId),
          item(fourth, stoppedRunId, 6, true),
        ],
      });
      yield* orchestrator.recoverDelegatedTasks;
      const withTie = (yield* placed)
        .toSorted(
          (left, right) =>
            (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal) ||
            left.ordinal - right.ordinal,
        )
        .map((candidate) => candidate.userMessageId);
      assert.deepEqual(withTie, [moved, first, second, third, fourth, tied, older]);
    }).pipe(
      Effect.provide(
        ProviderReplayHarness.layerWithRegistry(
          { name: "claude-queued-steer-merged-recovery" },
          ProviderAdapterRegistry.layerSingle(adapter),
          { runEffectWorker: false },
        ),
      ),
    );
  }).pipe(
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

// A stop between the run end and the re-queue, then a restart: soon after, or days later.
it.effect.each([0, 48])(
  "startup queues again, once, a returned steer whose run ended %i hours before the restart",
  (hoursBefore) =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("claude-queued-steer-recovery");
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
          open: () => Effect.die("this row starts no Claude process"),
          forkSession: () => Effect.die("unused"),
          subagentLaunchToolUseId: () => Effect.succeed(null),
          subagentModel: () => Effect.succeed(null),
          assertComplete: Effect.void,
        },
      });
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const eventSink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make("thread:queued-steer-recovery");
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:queued-steer-recovery"),
          title: "Queued steer recovery",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const now = yield* DateTime.now;
        const ended = DateTime.subtractDuration(now, Duration.hours(hoursBefore));
        const runId = RunId.make("run:queued-steer-recovery");
        const steerId = MessageId.make("steer:queued-steer-recovery");
        // As the server stopped: the run ended with its returned steer still attached to it.
        yield* eventSink.write({
          commandId: CommandId.make("command:runtime-reconcile:queued-steer-recovery"),
          events: [
            {
              id: EventId.make("event:queued-steer-recovery:run"),
              type: "run.updated",
              threadId,
              runId,
              providerInstanceId: modelSelection.instanceId,
              occurredAt: ended,
              payload: {
                id: runId,
                threadId,
                ordinal: 1,
                providerInstanceId: modelSelection.instanceId,
                modelSelection,
                providerThreadId: null,
                userMessageId: MessageId.make("first:queued-steer-recovery"),
                rootNodeId: null,
                activeAttemptId: null,
                status: "completed",
                requestedAt: ended,
                startedAt: ended,
                completedAt: ended,
                checkpointId: null,
                contextHandoffId: null,
              },
            },
            {
              id: EventId.make("event:queued-steer-recovery:message"),
              type: "message.updated",
              threadId,
              runId,
              occurredAt: ended,
              payload: {
                id: steerId,
                threadId,
                runId,
                nodeId: null,
                role: "user",
                text: STEER_TEXT,
                attachments: [],
                streaming: false,
                createdBy: "user",
                creationSource: "web",
                createdAt: ended,
                updatedAt: ended,
              },
            },
            {
              id: EventId.make("event:queued-steer-recovery:item"),
              type: "turn-item.updated",
              threadId,
              runId,
              occurredAt: ended,
              payload: {
                id: TurnItemId.make("turn-item:queued-steer-recovery"),
                threadId,
                runId,
                nodeId: null,
                providerThreadId: null,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: 2,
                status: "cancelled",
                title: null,
                startedAt: ended,
                completedAt: ended,
                updatedAt: ended,
                type: "user_message",
                createdBy: "user",
                creationSource: "web",
                messageId: steerId,
                inputIntent: "steer",
                text: STEER_TEXT,
                attachments: [],
                returnedToQueue: true,
              },
            },
          ],
        });
        const steerRuns = Effect.map(orchestrator.getThreadProjection(threadId), (projection) =>
          projection.runs.filter((run) => run.userMessageId === steerId),
        );
        assert.lengthOf(yield* steerRuns, 0);
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.recoverDelegatedTasks;
        const returned = yield* steerRuns;
        assert.lengthOf(returned, 1);
        const messageRun = Effect.map(
          orchestrator.getThreadProjection(threadId),
          (projection) => projection.messages.find((message) => message.id === steerId)?.runId,
        );
        assert.equal(yield* messageRun, returned[0]!.id);

        // The same message, steered into a later run and returned by it too, comes back again.
        const second = yield* orchestrator.getThreadProjection(threadId);
        const requeuedRun = second.runs.find((run) => run.id === returned[0]!.id)!;
        const laterRunId = RunId.make("run:queued-steer-recovery:later");
        const item = second.turnItems.find(
          (candidate): candidate is Extract<typeof candidate, { readonly type: "user_message" }> =>
            candidate.type === "user_message" && candidate.messageId === steerId,
        )!;
        const message = second.messages.find((candidate) => candidate.id === steerId)!;
        yield* eventSink.write({
          commandId: CommandId.make("command:runtime-reconcile:queued-steer-recovery-again"),
          events: [
            {
              id: EventId.make("event:queued-steer-recovery:requeued-cancelled"),
              type: "run.updated",
              threadId,
              runId: requeuedRun.id,
              providerInstanceId: modelSelection.instanceId,
              occurredAt: now,
              payload: { ...requeuedRun, status: "cancelled", completedAt: now },
            },
            {
              id: EventId.make("event:queued-steer-recovery:later-run"),
              type: "run.updated",
              threadId,
              runId: laterRunId,
              providerInstanceId: modelSelection.instanceId,
              occurredAt: now,
              payload: {
                id: laterRunId,
                threadId,
                ordinal: requeuedRun.ordinal + 1,
                providerInstanceId: modelSelection.instanceId,
                modelSelection,
                providerThreadId: null,
                userMessageId: MessageId.make("later:queued-steer-recovery"),
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
            {
              id: EventId.make("event:queued-steer-recovery:message-again"),
              type: "message.updated",
              threadId,
              runId: laterRunId,
              occurredAt: now,
              payload: { ...message, runId: laterRunId, updatedAt: now },
            },
            {
              id: EventId.make("event:queued-steer-recovery:item-again"),
              type: "turn-item.updated",
              threadId,
              runId: laterRunId,
              occurredAt: now,
              payload: {
                ...item,
                runId: laterRunId,
                status: "cancelled",
                returnedToQueue: true,
                updatedAt: now,
              },
            },
          ],
        });
        assert.equal(yield* messageRun, laterRunId);
        yield* orchestrator.recoverDelegatedTasks;
        yield* orchestrator.recoverDelegatedTasks;
        const back = yield* messageRun;
        assert.notEqual(back, laterRunId, "returned by the later run, it comes back again");
        assert.lengthOf(yield* steerRuns, 2);
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "claude-queued-steer-recovery" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }).pipe(
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

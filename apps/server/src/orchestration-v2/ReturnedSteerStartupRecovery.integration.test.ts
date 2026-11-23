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
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as ClaudeAdapterV2 from "./Adapters/ClaudeAdapterV2.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

// Older builds (0.1.9-perf.14 and perf.15) returned a steer Claude queued and never ran, then queued it again
// at the run's end; a server stopped between the two leaves it on its ended run, and startup queues it again
// exactly once. This build returns no steer (requeueUnreadSteers sends an unread one), so these rows seed the
// older builds' state directly.
const sessionId = "queued-steer-session";
const settings = Schema.decodeSync(ClaudeSettings)({});
const modelSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-sonnet-4-6",
};
const STEER_TEXT = "Also name the owner.";

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

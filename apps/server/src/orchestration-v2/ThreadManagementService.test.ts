import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  type OrchestrationV2Command,
  type OrchestrationV2Run,
  type OrchestrationV2ProviderThread,
  ProviderThreadId,
  ProviderDriverKind,
  ProviderSessionId,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Tracer from "effect/Tracer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import { AgentScope } from "@t3tools/shared/AgentScope";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import {
  ThreadWorktreeRestore,
  WorktreeRecreateError,
} from "../workspace/restoreMissingWorktree.ts";

it("stamps authoritative provenance on commands that create threads or messages", () => {
  const command: OrchestrationV2Command = {
    type: "thread.create",
    createdBy: "agent",
    creationSource: "mcp",
    commandId: CommandId.make("command:thread-management:create"),
    threadId: ThreadId.make("thread:thread-management:create"),
    projectId: ProjectId.make("project:thread-management"),
    title: "Thread management",
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5-codex",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    // A client cannot claim a linked environment's origin, or a parent there.
    linkOrigin: { sessionId: "forged", label: "Somewhere else" },
    delegatedFrom: {
      environmentId: EnvironmentId.make("environment:elsewhere"),
      threadId: ThreadId.make("thread:elsewhere"),
      title: "Somewhere else",
    },
    // A client's message goes through intake; only the server folds one into the creation.
    initialMessage: {
      messageId: MessageId.make("message:thread-management:forged"),
      text: "Forged",
      attachments: [],
    },
    // Nor a workflow run it is a step of.
    workflowRun: {
      environmentId: EnvironmentId.make("environment:elsewhere"),
      runId: "run-1",
      workflow: "fix",
      step: "write",
    },
  };

  const stamped = ThreadManagementService.withCreationProvenance(command, {
    createdBy: "user",
    creationSource: "web",
  });
  expect(stamped).toMatchObject({
    createdBy: "user",
    creationSource: "web",
  });
  expect(stamped).not.toHaveProperty("linkOrigin");
  expect(stamped).not.toHaveProperty("delegatedFrom");
  expect(stamped).not.toHaveProperty("initialMessage");
  expect(stamped).not.toHaveProperty("workflowRun");
});

const layerReceiptsTest = CommandReceiptStore.layer.pipe(
  Layer.provide(SqlitePersistence.layerMemory),
);

const layerDispatchChokepointTest =
  (settingsLayer: ReturnType<typeof ServerSettings.layerTest>) => (dispatched: Array<string>) =>
    ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: {
                id: ThreadId.make("thread:thread-management:chokepoint"),
                projectId: ProjectId.make("project:thread-management:chokepoint"),
                deletedAt: null,
                modelSelection: {
                  instanceId: ProviderInstanceId.make("codex_work"),
                  model: "test",
                },
              },
            } as unknown as OrchestrationV2ThreadProjection),
          dispatch: (command) =>
            Effect.sync(() => {
              dispatched.push(command.type);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(settingsLayer),
      Layer.provideMerge(layerReceiptsTest),
    );

it.effect(
  "the dispatch chokepoint refuses a continuation message.dispatch on a project-disabled instance before reaching the orchestrator",
  () => {
    const projectId = ProjectId.make("project:thread-management:chokepoint");
    const threadId = ThreadId.make("thread:thread-management:chokepoint");
    const instanceId = ProviderInstanceId.make("codex_work");
    const commandId = CommandId.make("command:thread-management:chokepoint-disabled");
    const dispatched: Array<string> = [];
    const layerTest = layerDispatchChokepointTest(
      ServerSettings.layerTest({
        providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
        },
      }),
    )(dispatched);

    return Effect.gen(function* () {
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const error = yield* Effect.flip(
        threads.dispatch({
          type: "message.dispatch",
          commandId,
          threadId,
          messageId: MessageId.make("message:thread-management:chokepoint-disabled"),
          text: "Background task completed.",
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        }),
      );

      expect(error).toBeInstanceOf(
        ThreadManagementService.ThreadManagementProviderInstanceDisabledError,
      );
      expect(error).toMatchObject({ projectId, instanceId });
      expect(dispatched).toEqual([]);
    }).pipe(Effect.provide(layerTest));
  },
);

it.effect(
  "a replay of an already-accepted command id dispatches despite the instance now being disabled",
  () => {
    const projectId = ProjectId.make("project:thread-management:chokepoint");
    const threadId = ThreadId.make("thread:thread-management:chokepoint");
    const instanceId = ProviderInstanceId.make("codex_work");
    const commandId = CommandId.make("command:thread-management:chokepoint-replay");
    const dispatched: Array<string> = [];
    const layerTest = layerDispatchChokepointTest(
      ServerSettings.layerTest({
        providerInstances: { [instanceId]: { driver: "codex", enabled: true } },
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
        },
      }),
    )(dispatched);

    return Effect.gen(function* () {
      const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
      yield* receipts.insertIfAbsent({
        commandId,
        threadId,
        commandType: "message.dispatch",
        acceptedAt: yield* DateTime.now,
        resultSequence: 1,
        status: "accepted",
        error: null,
      });

      const threads = yield* ThreadManagementService.ThreadManagementService;
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId,
        threadId,
        messageId: MessageId.make("message:thread-management:chokepoint-replay"),
        text: "Background task completed.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "server",
      });

      expect(dispatched).toEqual(["message.dispatch"]);
    }).pipe(Effect.provide(layerTest));
  },
);

it.effect.each([
  { label: "fails a send that starts a turn", running: false },
  { label: "still steers a running turn", running: true },
])("sendToThread with a missing worktree that cannot be recreated $label", ({ running }) => {
  const projectId = ProjectId.make("project:thread-management:worktree-gone");
  const threadId = ThreadId.make("thread:thread-management:worktree-gone");
  const dispatched: Array<string> = [];
  const layerTest = ThreadManagementService.layerWithLegacyImporter.pipe(
    Layer.provide(
      Layer.succeed(ThreadWorktreeRestore, {
        restore: () =>
          Effect.fail(
            new WorktreeRecreateError({
              worktreePath: "/worktrees/gone",
              branch: "t3/gone",
              cause: new Error("branch_checked_out_in_worktree"),
            }),
          ),
      }),
    ),
    Layer.provide(
      Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
        ensureTranscript: () => Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 }),
      }),
    ),
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(
      Layer.mock(Orchestrator.OrchestratorV2)({
        getThreadRecords: () =>
          Effect.succeed({
            thread: {
              id: threadId,
              projectId,
              deletedAt: null,
              archivedAt: null,
              worktreePath: "/worktrees/gone",
              branch: "t3/gone",
              modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
            },
            runs: running
              ? [
                  {
                    id: RunId.make("run:thread-management:worktree-gone"),
                    status: "running",
                    activeAttemptId: "attempt:thread-management:worktree-gone",
                  },
                ]
              : [],
            providerTurns: running
              ? [
                  {
                    runAttemptId: "attempt:thread-management:worktree-gone",
                    status: "running",
                  },
                ]
              : [],
            messages: [],
            turnItems: [],
          } as unknown as OrchestrationV2ThreadProjection),
        dispatch: (command) =>
          Effect.sync(() => {
            dispatched.push(command.type);
            return { sequence: 1, storedEvents: [] } as never;
          }),
      }),
    ),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provideMerge(layerReceiptsTest),
  );

  return Effect.gen(function* () {
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const error = yield* Effect.flip(
      threads.sendToThread({
        projectId,
        threadId,
        commandId: CommandId.make("command:thread-management:worktree-gone"),
        messageId: MessageId.make("message:thread-management:worktree-gone"),
        text: "Reply with ok.",
        attachments: [],
        mode: "auto",
        createdBy: "agent",
        creationSource: "mcp",
      }),
    );
    // The mock records no message, so a dispatched send ends in the projection read.
    expect(error._tag).toBe(
      running
        ? "ThreadManagementDurableRunProjectionError"
        : "ThreadManagementWorktreeRestoreError",
    );
    expect(dispatched).toEqual(running ? ["message.dispatch"] : []);
  }).pipe(Effect.provide(layerTest));
});

it.effect("waitForThread follows a restart continuation instead of the cut run", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-restart");
    const threadId = ThreadId.make("thread:thread-management:wait-restart");
    const runId = RunId.make("run:thread-management:wait-restart");
    const continuationId = RunId.make("run:thread-management:wait-restart:2");
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    let runs: Array<{
      id: RunId;
      status: OrchestrationV2Run["status"];
      restartContinuationOfRunId?: RunId;
    }> = [{ id: runId, status: "cancelled" }];
    let pending = true;
    const stored = (sequence: number, event: object) =>
      ({ sequence, event: { threadId, ...event } }) as unknown as OrchestrationV2StoredEvent;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: (run) => Effect.succeed(pending && run.id === runId),
          awaitMessageSteersSettled: () => Effect.void,
          awaitRestartContinuation: () => Effect.never,
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    const continuation = { id: continuationId, restartContinuationOfRunId: runId };
    runs = [...runs, { ...continuation, status: "running" }];
    pending = false;
    yield* Queue.offer(
      events,
      stored(1, { type: "run.updated", payload: { ...continuation, status: "running" } }),
    );
    runs = [runs[0]!, { ...continuation, status: "completed" }];
    yield* Queue.offer(
      events,
      stored(2, { type: "run.updated", payload: { ...continuation, status: "completed" } }),
    );
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({
      timedOut: false,
      run: { id: continuationId, status: "completed" },
    });
  }),
);

it.effect(
  "waitForThread follows a queued message sent into a running turn to that turn's run",
  () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("project:thread-management:wait-promoted");
      const threadId = ThreadId.make("thread:thread-management:wait-promoted");
      // `t3 thread send --wait` waits on the queued run; the automatic steer cancels it and moves its
      // message into the busy run, which answers it.
      const queuedRunId = RunId.make("run:thread-management:wait-promoted:queued");
      const busyRunId = RunId.make("run:thread-management:wait-promoted:busy");
      const messageId = MessageId.make("message:thread-management:wait-promoted");
      const subscribed = yield* Deferred.make<void>();
      const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
      let runs: Array<{
        id: RunId;
        status: OrchestrationV2Run["status"];
        userMessageId: MessageId;
      }> = [
        { id: queuedRunId, status: "cancelled", userMessageId: messageId },
        { id: busyRunId, status: "running", userMessageId: MessageId.make("message:busy") },
      ];
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadEventSequence: () => Effect.succeed(0),
            restartContinuationPending: () => Effect.succeed(false),
            awaitMessageSteersSettled: () => Effect.void,
            streamStoredEventsFrom: (input) =>
              input?.eventType === "run.updated"
                ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                    Stream.drain,
                    Stream.concat(Stream.fromQueue(events)),
                  )
                : Stream.never,
            getThreadRecords: () =>
              Effect.sync(
                () =>
                  ({
                    thread: { id: threadId, projectId, deletedAt: null },
                    runs,
                    messages: [{ id: messageId, runId: busyRunId }],
                    turnItems: [],
                  }) as unknown as OrchestrationV2ThreadProjection,
              ),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const service = yield* ThreadManagementService.ThreadManagementService.pipe(
        Effect.provide(layerTest),
      );
      const fiber = yield* service
        .waitForThread({ projectId, threadId, runId: queuedRunId, timeoutMs: 60 * 60 * 1_000 })
        .pipe(Effect.forkChild);
      // A wait that stops at the cancelled queued run never subscribes; its answer is checked below.
      yield* Effect.raceFirst(Deferred.await(subscribed), Fiber.await(fiber));
      runs = [runs[0]!, { ...runs[1]!, status: "completed" }];
      yield* Queue.offer(events, {
        sequence: 1,
        event: { threadId, type: "run.updated", payload: runs[1] },
      } as unknown as OrchestrationV2StoredEvent);

      expect(yield* Fiber.join(fiber)).toMatchObject({
        timedOut: false,
        run: { id: busyRunId, status: "completed" },
      });
    }),
);

it.effect("waitForThread follows a sent message on to the follow-up its missed steer became", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-follow-up");
    const threadId = ThreadId.make("thread:thread-management:wait-follow-up");
    const queuedRunId = RunId.make("run:thread-management:wait-follow-up:queued");
    const busyRunId = RunId.make("run:thread-management:wait-follow-up:busy");
    const followUpRunId = RunId.make("run:thread-management:wait-follow-up:follow-up");
    const messageId = MessageId.make("message:thread-management:wait-follow-up");
    const busyMessageId = MessageId.make("message:thread-management:wait-follow-up:busy");
    let subscriptions = 0;
    const subscribed = yield* Queue.unbounded<number>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    let carrier = busyRunId;
    let runs: Array<{
      id: RunId;
      status: OrchestrationV2Run["status"];
      userMessageId: MessageId;
      steerFollowUpOfRunId?: RunId;
    }> = [
      { id: queuedRunId, status: "cancelled", userMessageId: messageId },
      { id: busyRunId, status: "running", userMessageId: busyMessageId },
    ];
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          awaitMessageSteersSettled: () => Effect.void,
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Queue.offer(subscribed, ++subscriptions)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                  messages: [{ id: messageId, runId: carrier }],
                  turnItems: [],
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId: queuedRunId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    const runUpdated = (payload: object, sequence: number) =>
      Queue.offer(events, {
        sequence,
        event: { threadId, type: "run.updated", payload },
      } as unknown as OrchestrationV2StoredEvent);
    yield* Queue.take(subscribed);
    // The busy turn ended without taking the steer, so the message went on to its follow-up.
    const followUp = {
      id: followUpRunId,
      status: "running" as const,
      userMessageId: messageId,
      steerFollowUpOfRunId: busyRunId,
    };
    runs = [runs[0]!, { ...runs[1]!, status: "completed" }, followUp];
    carrier = followUpRunId;
    yield* runUpdated(runs[1]!, 1);
    yield* Effect.raceFirst(Queue.take(subscribed), Fiber.await(fiber));
    runs = [runs[0]!, runs[1]!, { ...followUp, status: "completed" }];
    yield* runUpdated(runs[2]!, 2);

    expect(yield* Fiber.join(fiber)).toMatchObject({
      timedOut: false,
      run: { id: followUpRunId, status: "completed" },
    });
  }),
);

it.effect("waitForThread waits for an unread steer's re-send after the busy run ends", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-unread");
    const threadId = ThreadId.make("thread:thread-management:wait-unread");
    const queuedRunId = RunId.make("run:thread-management:wait-unread:queued");
    const busyRunId = RunId.make("run:thread-management:wait-unread:busy");
    const followUpRunId = RunId.make("run:thread-management:wait-unread:follow-up");
    const messageId = MessageId.make("message:thread-management:wait-unread");
    let subscriptions = 0;
    const subscribed = yield* Queue.unbounded<number>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    let carrier = busyRunId;
    let steerStatus: "pending" | "completed" = "pending";
    let runs: Array<{
      id: RunId;
      status: OrchestrationV2Run["status"];
      userMessageId: MessageId;
      steerFollowUpOfRunId?: RunId;
    }> = [
      { id: queuedRunId, status: "cancelled", userMessageId: messageId },
      { id: busyRunId, status: "running", userMessageId: MessageId.make("message:busy") },
    ];
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          awaitMessageSteersSettled: () => Effect.void,
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Queue.offer(subscribed, ++subscriptions)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                  messages: [{ id: messageId, runId: carrier }],
                  turnItems: [
                    {
                      type: "user_message",
                      runId: busyRunId,
                      messageId,
                      inputIntent: "promoted_queued_to_steer",
                      status: steerStatus,
                    },
                  ],
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId: queuedRunId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    const runUpdated = (payload: object, sequence: number) =>
      Queue.offer(events, {
        sequence,
        event: { threadId, type: "run.updated", payload },
      } as unknown as OrchestrationV2StoredEvent);
    yield* Queue.take(subscribed);
    // The busy run ends with the steer unread; the run-end reaction re-sends it a moment later.
    runs = [runs[0]!, { ...runs[1]!, status: "completed" }];
    yield* runUpdated(runs[1]!, 1);
    yield* Effect.raceFirst(Queue.take(subscribed), Fiber.await(fiber));
    const followUp = {
      id: followUpRunId,
      status: "running" as const,
      userMessageId: messageId,
      steerFollowUpOfRunId: busyRunId,
    };
    runs = [...runs, followUp];
    carrier = followUpRunId;
    steerStatus = "completed";
    yield* runUpdated(followUp, 2);
    yield* Effect.raceFirst(Queue.take(subscribed), Fiber.await(fiber));
    runs = [runs[0]!, runs[1]!, { ...followUp, status: "completed" }];
    yield* runUpdated(runs[2]!, 3);

    expect(yield* Fiber.join(fiber)).toMatchObject({
      timedOut: false,
      run: { id: followUpRunId, status: "completed" },
    });
  }),
);

it.effect(
  "waitForThread reads the sent message and its steer together while the re-send lands",
  () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("project:thread-management:wait-interleaved");
      const threadId = ThreadId.make("thread:thread-management:wait-interleaved");
      const queuedRunId = RunId.make("run:thread-management:wait-interleaved:queued");
      const busyRunId = RunId.make("run:thread-management:wait-interleaved:busy");
      const followUpRunId = RunId.make("run:thread-management:wait-interleaved:follow-up");
      const messageId = MessageId.make("message:thread-management:wait-interleaved");
      let subscriptions = 0;
      const subscribed = yield* Queue.unbounded<number>();
      const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
      const followUp = {
        id: followUpRunId,
        status: "running" as const,
        userMessageId: messageId,
        steerFollowUpOfRunId: busyRunId,
      };
      // The busy run has ended with the steer unread; the re-send commits right after the wait's
      // second read of the sent message (the busy run's check), before anything else is read.
      let resent = false;
      let messageReads = 0;
      const state = () => ({
        runs: [
          { id: queuedRunId, status: "cancelled" as const, userMessageId: messageId },
          {
            id: busyRunId,
            status: "completed" as const,
            userMessageId: MessageId.make("message:busy"),
          },
          ...(resent ? [followUp] : []),
        ],
        messages: [{ id: messageId, runId: resent ? followUpRunId : busyRunId }],
        turnItems: [
          {
            type: "user_message",
            runId: resent ? followUpRunId : busyRunId,
            messageId,
            inputIntent: "promoted_queued_to_steer",
            status: resent ? "running" : "pending",
          },
        ],
      });
      const afterMessageRead = <A>(value: A) => {
        if (++messageReads === 2) resent = true;
        return value;
      };
      let followUpDone = false;
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadEventSequence: () => Effect.succeed(0),
            restartContinuationPending: () => Effect.succeed(false),
            awaitMessageSteersSettled: () => Effect.void,
            streamStoredEventsFrom: (input) =>
              input?.eventType === "run.updated"
                ? Stream.fromEffect(Queue.offer(subscribed, ++subscriptions)).pipe(
                    Stream.drain,
                    Stream.concat(Stream.fromQueue(events)),
                  )
                : Stream.never,
            getMessagesById: () =>
              Effect.sync(() =>
                afterMessageRead(
                  new Map(state().messages.map((message) => [message.id, message])) as never,
                ),
              ),
            getThreadRecords: (_threadId, fields) =>
              Effect.sync(() => {
                const current = state();
                const records = {
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs: current.runs.map((run) =>
                    run.id === followUpRunId && followUpDone
                      ? { ...run, status: "completed" }
                      : run,
                  ),
                  turnItems: current.turnItems,
                  messages: current.messages,
                };
                const readsMessages = (fields as ReadonlyArray<string> | undefined)?.includes(
                  "messages",
                );
                return (readsMessages
                  ? afterMessageRead(records)
                  : records) as unknown as OrchestrationV2ThreadProjection;
              }),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const service = yield* ThreadManagementService.ThreadManagementService.pipe(
        Effect.provide(layerTest),
      );
      const fiber = yield* service
        .waitForThread({ projectId, threadId, runId: queuedRunId, timeoutMs: 60 * 60 * 1_000 })
        .pipe(Effect.forkChild);
      const runUpdated = (payload: object, sequence: number) =>
        Queue.offer(events, {
          sequence,
          event: { threadId, type: "run.updated", payload },
        } as unknown as OrchestrationV2StoredEvent);
      yield* Effect.raceFirst(Queue.take(subscribed), Fiber.await(fiber));
      yield* runUpdated(followUp, 1);
      yield* Effect.raceFirst(Queue.take(subscribed), Fiber.await(fiber));
      followUpDone = true;
      yield* runUpdated({ ...followUp, status: "completed" }, 2);

      expect(yield* Fiber.join(fiber)).toMatchObject({
        timedOut: false,
        run: { id: followUpRunId, status: "completed" },
      });
    }),
);

it.effect("waitForThread waits for a sent message's steer to settle before answering", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-steer-effect");
    const threadId = ThreadId.make("thread:thread-management:wait-steer-effect");
    const queuedRunId = RunId.make("run:thread-management:wait-steer-effect:queued");
    const busyRunId = RunId.make("run:thread-management:wait-steer-effect:busy");
    const followUpRunId = RunId.make("run:thread-management:wait-steer-effect:follow-up");
    const messageId = MessageId.make("message:thread-management:wait-steer-effect");
    let subscriptions = 0;
    const subscribed = yield* Queue.unbounded<number>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    // The steer is recorded before its delivery effect runs; the busy turn ended first, so the
    // effect, once it runs, re-sends the message as a follow-up.
    const steerSettles = yield* Deferred.make<void>();
    const settleAsked = yield* Deferred.make<void>();
    let carrier = busyRunId;
    const followUp = {
      id: followUpRunId,
      status: "running" as OrchestrationV2Run["status"],
      userMessageId: messageId,
      steerFollowUpOfRunId: busyRunId,
    };
    let runs: Array<{
      id: RunId;
      status: OrchestrationV2Run["status"];
      userMessageId: MessageId;
      steerFollowUpOfRunId?: RunId;
    }> = [
      { id: queuedRunId, status: "cancelled", userMessageId: messageId },
      { id: busyRunId, status: "completed", userMessageId: MessageId.make("message:busy") },
    ];
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          awaitMessageSteersSettled: () =>
            Deferred.succeed(settleAsked, undefined).pipe(
              Effect.andThen(Deferred.await(steerSettles)),
            ),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Queue.offer(subscribed, ++subscriptions)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                  messages: [{ id: messageId, runId: carrier }],
                  turnItems: [
                    {
                      type: "user_message",
                      runId: carrier,
                      messageId,
                      inputIntent: "promoted_queued_to_steer",
                      status: "completed",
                    },
                  ],
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId: queuedRunId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Effect.raceFirst(Deferred.await(settleAsked), Fiber.await(fiber));
    runs = [...runs, followUp];
    carrier = followUpRunId;
    yield* Deferred.succeed(steerSettles, undefined);
    yield* Effect.raceFirst(Queue.take(subscribed), Fiber.await(fiber));
    runs = [runs[0]!, runs[1]!, { ...followUp, status: "completed" }];
    yield* Queue.offer(events, {
      sequence: 1,
      event: { threadId, type: "run.updated", payload: runs[2] },
    } as unknown as OrchestrationV2StoredEvent);

    expect(yield* Fiber.join(fiber)).toMatchObject({
      timedOut: false,
      run: { id: followUpRunId, status: "completed" },
    });
  }),
);

it.effect("waitForThread follows a re-sent message into another busy run it was sent into", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-chain");
    const threadId = ThreadId.make("thread:thread-management:wait-chain");
    const queuedRunId = RunId.make("run:thread-management:wait-chain:queued");
    const firstBusyRunId = RunId.make("run:thread-management:wait-chain:busy-1");
    const followUpRunId = RunId.make("run:thread-management:wait-chain:follow-up");
    const secondBusyRunId = RunId.make("run:thread-management:wait-chain:busy-2");
    const messageId = MessageId.make("message:thread-management:wait-chain");
    let subscriptions = 0;
    const subscribed = yield* Queue.unbounded<number>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    // The first busy turn ended without reading it; its follow-up was then sent into the next
    // busy run, all before the waiter looked.
    let runs: Array<{
      id: RunId;
      status: OrchestrationV2Run["status"];
      userMessageId: MessageId;
      steerFollowUpOfRunId?: RunId;
    }> = [
      { id: queuedRunId, status: "cancelled", userMessageId: messageId },
      { id: firstBusyRunId, status: "completed", userMessageId: MessageId.make("message:busy-1") },
      {
        id: followUpRunId,
        status: "cancelled",
        userMessageId: messageId,
        steerFollowUpOfRunId: firstBusyRunId,
      },
      { id: secondBusyRunId, status: "running", userMessageId: MessageId.make("message:busy-2") },
    ];
    let carrier = firstBusyRunId;
    let settleChecks = 0;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          // The queued run's check finds it in the first busy run; by that run's check, it moved on.
          awaitMessageSteersSettled: () =>
            Effect.sync(() => {
              if (++settleChecks === 2) carrier = secondBusyRunId;
            }),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Queue.offer(subscribed, ++subscriptions)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                  messages: [{ id: messageId, runId: carrier }],
                  turnItems: [
                    {
                      type: "user_message",
                      runId: carrier,
                      messageId,
                      inputIntent: "promoted_queued_to_steer",
                      status: "completed",
                    },
                  ],
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId: queuedRunId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Effect.raceFirst(Queue.take(subscribed), Fiber.await(fiber));
    runs = runs.map((run) => (run.id === secondBusyRunId ? { ...run, status: "completed" } : run));
    yield* Queue.offer(events, {
      sequence: 1,
      event: { threadId, type: "run.updated", payload: runs[3] },
    } as unknown as OrchestrationV2StoredEvent);

    expect(yield* Fiber.join(fiber)).toMatchObject({
      timedOut: false,
      run: { id: secondBusyRunId, status: "completed" },
    });
  }),
);

// A message can move again and again (each busy turn it reaches ends without reading it); the waiter
// follows it within its own time to the run that answers it, never stopping on one it moved out of.
it.effect(
  "waitForThread follows a sent message through many busy runs to the one that answers it",
  () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("project:thread-management:wait-long-chain");
      const threadId = ThreadId.make("thread:thread-management:wait-long-chain");
      const queuedRunId = RunId.make("run:thread-management:wait-long-chain:queued");
      const messageId = MessageId.make("message:thread-management:wait-long-chain");
      const busyRunIds = Array.from({ length: 40 }, (_, index) =>
        RunId.make(`run:thread-management:wait-long-chain:busy-${index}`),
      );
      const runs = [
        { id: queuedRunId, status: "cancelled", userMessageId: messageId },
        ...busyRunIds.map((id, index) => ({
          id,
          status: "completed",
          userMessageId: MessageId.make(`message:wait-long-chain:busy-${index}`),
        })),
      ];
      // Each settle check finds the message gone on to the next busy run, until the last reads it.
      let carrier = 0;
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadEventSequence: () => Effect.succeed(0),
            restartContinuationPending: () => Effect.succeed(false),
            awaitMessageSteersSettled: () =>
              Effect.sync(() => {
                carrier = Math.min(carrier + 1, busyRunIds.length - 1);
              }),
            streamStoredEventsFrom: () => Stream.never,
            getThreadRecords: () =>
              Effect.sync(
                () =>
                  ({
                    thread: { id: threadId, projectId, deletedAt: null },
                    runs,
                    messages: [{ id: messageId, runId: busyRunIds[carrier] }],
                    turnItems: [
                      {
                        type: "user_message",
                        runId: busyRunIds[carrier],
                        messageId,
                        inputIntent: "promoted_queued_to_steer",
                        status: "completed",
                      },
                    ],
                  }) as unknown as OrchestrationV2ThreadProjection,
              ),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const service = yield* ThreadManagementService.ThreadManagementService.pipe(
        Effect.provide(layerTest),
      );
      expect(
        yield* service.waitForThread({
          projectId,
          threadId,
          runId: queuedRunId,
          timeoutMs: 60 * 60 * 1_000,
        }),
      ).toMatchObject({ timedOut: false, run: { id: busyRunIds.at(-1) } });
    }),
);

// A run the message left cannot carry it again; a record that says so is not an answer.
it.effect(
  "waitForThread reports a wait unfinished when its message comes back to a run it left",
  () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("project:thread-management:wait-cycle");
      const threadId = ThreadId.make("thread:thread-management:wait-cycle");
      const queuedRunId = RunId.make("run:thread-management:wait-cycle:queued");
      const messageId = MessageId.make("message:thread-management:wait-cycle");
      const busyRunIds = [0, 1].map((index) =>
        RunId.make(`run:thread-management:wait-cycle:busy-${index}`),
      );
      const runs = [
        { id: queuedRunId, status: "cancelled", userMessageId: messageId },
        ...busyRunIds.map((id, index) => ({
          id,
          status: "completed",
          userMessageId: MessageId.make(`message:wait-cycle:busy-${index}`),
        })),
      ];
      let carrier = 0;
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadEventSequence: () => Effect.succeed(0),
            restartContinuationPending: () => Effect.succeed(false),
            awaitMessageSteersSettled: () =>
              Effect.sync(() => {
                carrier = 1 - carrier;
              }),
            streamStoredEventsFrom: () => Stream.never,
            getThreadRecords: () =>
              Effect.sync(
                () =>
                  ({
                    thread: { id: threadId, projectId, deletedAt: null },
                    runs,
                    messages: [{ id: messageId, runId: busyRunIds[carrier] }],
                    turnItems: [
                      {
                        type: "user_message",
                        runId: busyRunIds[carrier],
                        messageId,
                        inputIntent: "promoted_queued_to_steer",
                        status: "completed",
                      },
                    ],
                  }) as unknown as OrchestrationV2ThreadProjection,
              ),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const service = yield* ThreadManagementService.ThreadManagementService.pipe(
        Effect.provide(layerTest),
      );
      expect(
        yield* service.waitForThread({
          projectId,
          threadId,
          runId: queuedRunId,
          timeoutMs: 60 * 60 * 1_000,
        }),
      ).toMatchObject({ timedOut: true });
    }),
);

it.effect("waitForThread gives up on time while a sent message's steer has not settled", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-steer-timeout");
    const threadId = ThreadId.make("thread:thread-management:wait-steer-timeout");
    const queuedRunId = RunId.make("run:thread-management:wait-steer-timeout:queued");
    const busyRunId = RunId.make("run:thread-management:wait-steer-timeout:busy");
    const messageId = MessageId.make("message:thread-management:wait-steer-timeout");
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          // The steer's delivery effect never settles.
          awaitMessageSteersSettled: () => Effect.never,
          streamStoredEventsFrom: () => Stream.never,
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, projectId, deletedAt: null },
              runs: [
                { id: queuedRunId, status: "cancelled", userMessageId: messageId },
                { id: busyRunId, status: "completed", userMessageId: MessageId.make("m:busy") },
              ],
              messages: [{ id: messageId, runId: busyRunId }],
              turnItems: [],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId: queuedRunId, timeoutMs: 1_000 })
      .pipe(Effect.forkChild);
    yield* TestClock.adjust(Duration.seconds(2));
    expect(yield* Fiber.join(fiber)).toMatchObject({ timedOut: true });
  }),
);

it.effect("waitForThread follows a sent message out of a busy run that failed", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-failed-carrier");
    const threadId = ThreadId.make("thread:thread-management:wait-failed-carrier");
    const queuedRunId = RunId.make("run:thread-management:wait-failed-carrier:queued");
    const busyRunId = RunId.make("run:thread-management:wait-failed-carrier:busy");
    const followUpRunId = RunId.make("run:thread-management:wait-failed-carrier:follow-up");
    const messageId = MessageId.make("message:thread-management:wait-failed-carrier");
    let settleChecks = 0;
    let carrier = busyRunId;
    // The busy turn failed before reading the steer; the message went on to a follow-up, which ran.
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          awaitMessageSteersSettled: () =>
            Effect.sync(() => {
              if (++settleChecks === 2) carrier = followUpRunId;
            }),
          streamStoredEventsFrom: () => Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs: [
                    { id: queuedRunId, status: "cancelled", userMessageId: messageId },
                    { id: busyRunId, status: "failed", userMessageId: MessageId.make("m:busy") },
                    {
                      id: followUpRunId,
                      status: "completed",
                      userMessageId: messageId,
                      steerFollowUpOfRunId: busyRunId,
                    },
                  ],
                  messages: [{ id: messageId, runId: carrier }],
                  turnItems: [],
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    expect(
      yield* service.waitForThread({
        projectId,
        threadId,
        runId: queuedRunId,
        timeoutMs: 60 * 60 * 1_000,
      }),
    ).toMatchObject({ timedOut: false, run: { id: followUpRunId, status: "completed" } });
  }),
);

it.effect("waitForThread with no run follows the latest run's message when a steer moved it", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-latest-promoted");
    const threadId = ThreadId.make("thread:thread-management:wait-latest-promoted");
    const queuedRunId = RunId.make("run:thread-management:wait-latest-promoted:queued");
    const busyRunId = RunId.make("run:thread-management:wait-latest-promoted:busy");
    const messageId = MessageId.make("message:thread-management:wait-latest-promoted");
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    // The latest run is the queued one the steer cancelled; its message went into the busy run.
    let runs: Array<{
      id: RunId;
      ordinal: number;
      status: OrchestrationV2Run["status"];
      userMessageId: MessageId;
    }> = [
      { id: busyRunId, ordinal: 1, status: "running", userMessageId: MessageId.make("m:busy") },
      { id: queuedRunId, ordinal: 2, status: "cancelled", userMessageId: messageId },
    ];
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          awaitMessageSteersSettled: () => Effect.void,
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                  messages: [{ id: messageId, runId: busyRunId }],
                  turnItems: [],
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Effect.raceFirst(Deferred.await(subscribed), Fiber.await(fiber));
    runs = [{ ...runs[0]!, status: "completed" }, runs[1]!];
    yield* Queue.offer(events, {
      sequence: 1,
      event: { threadId, type: "run.updated", payload: runs[0] },
    } as unknown as OrchestrationV2StoredEvent);

    expect(yield* Fiber.join(fiber)).toMatchObject({
      timedOut: false,
      run: { id: busyRunId, status: "completed" },
    });
  }),
);

it.effect("waitForThread can wait past an ended run for the thread's delegated work", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-delegated");
    const threadId = ThreadId.make("thread:thread-management:wait-delegated");
    const runId = RunId.make("run:thread-management:wait-delegated");
    const wakeRunId = RunId.make("run:thread-management:wait-delegated:wake");
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    const at = (seconds: number) => DateTime.makeUnsafe(seconds * 1_000);
    const run = (id: RunId, ordinal: number, status: OrchestrationV2Run["status"], end: number) =>
      ({
        id,
        ordinal,
        status,
        startedAt: at(end - 1),
        completedAt: status === "running" ? null : at(end),
      }) as unknown as OrchestrationV2Run;
    // The run ended, but a task it delegated still runs and will wake the thread.
    let runs = [run(runId, 1, "completed", 1)];
    let subagents = [{ status: "running" }];
    const stored = (sequence: number, event: object) =>
      ({ sequence, event: { threadId, ...event } }) as unknown as OrchestrationV2StoredEvent;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          awaitMessageSteersSettled: () => Effect.void,
          delegatedTaskResultPending: () => Effect.succeed(false),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "subagent.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.fromQueue(events)),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs,
                  messages: [],
                  subagents,
                  providerThreads: [],
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    // Without the option, the ended run is the answer, as before.
    expect(
      yield* service.waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 }),
    ).toMatchObject({ timedOut: false, run: { id: runId } });
    expect(
      yield* service.waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 }),
    ).not.toHaveProperty("resultRun");

    const fiber = yield* service
      .waitForThread({
        projectId,
        threadId,
        runId,
        timeoutMs: 60 * 60 * 1_000,
        untilDelegatedWorkSettled: true,
      })
      .pipe(Effect.forkChild);
    // A wait that returns early fails below instead of hanging here.
    yield* Effect.raceFirst(Deferred.await(subscribed), Fiber.await(fiber));
    // The task finishes; its delivery wakes the thread, whose next run answers.
    subagents = [{ status: "completed" }];
    runs = [...runs, run(wakeRunId, 2, "completed", 3)];
    yield* Queue.offer(events, stored(1, { type: "subagent.updated", payload: {} }));
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({
      timedOut: false,
      run: { id: runId, status: "completed" },
      resultRun: { id: wakeRunId, status: "completed" },
    });
  }),
);

it.effect(
  "a turn that ended without its result is waited past, and a later turn answers for it",
  () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("project:thread-management:owed");
      const threadId = ThreadId.make("thread:thread-management:owed");
      const runId = RunId.make("run:thread-management:owed");
      const wakeRunId = RunId.make("run:thread-management:owed:wake");
      const subscribed = yield* Deferred.make<void>();
      const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
      const outputSchema = { type: "object", required: ["head"] };
      const run = (id: RunId, ordinal: number, fields: object) =>
        ({ id, ordinal, ...fields }) as unknown as OrchestrationV2Run;
      // The agent ended its turn to wait for something, without the result it was asked for.
      let runs = [run(runId, 1, { status: "completed", outputSchema })];
      const submitted: Array<unknown> = [];
      const stored = (sequence: number, event: object) =>
        ({ sequence, event: { threadId, ...event } }) as unknown as OrchestrationV2StoredEvent;
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getThreadEventSequence: () => Effect.succeed(0),
            restartContinuationPending: () => Effect.succeed(false),
            streamStoredEventsFrom: (input) =>
              input?.eventType === "run.output-submitted"
                ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                    Stream.drain,
                    Stream.concat(Stream.fromQueue(events)),
                  )
                : Stream.never,
            getThreadRecords: () =>
              Effect.sync(
                () =>
                  ({
                    thread: { id: threadId, projectId, deletedAt: null },
                    runs,
                  }) as unknown as OrchestrationV2ThreadProjection,
              ),
            dispatch: (command) =>
              Effect.sync(() => {
                submitted.push(command);
                return { sequence: 1 } as never;
              }),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const service = yield* ThreadManagementService.ThreadManagementService.pipe(
        Effect.provide(layerTest),
      );
      const wait = { projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 };
      // Without the option the ended turn is the answer, as before.
      expect(yield* service.waitForThread(wait)).toMatchObject({ timedOut: false });
      // A turn asked for nothing owes nothing, so the option does not hold it.
      const plainRunId = RunId.make("run:thread-management:owed:plain");
      runs = [run(plainRunId, 0, { status: "completed" }), ...runs];
      expect(
        yield* Effect.raceFirst(
          service.waitForThread({ ...wait, runId: plainRunId, untilOutput: true }),
          Effect.as(Deferred.await(subscribed), "waited for a result"),
        ),
      ).toMatchObject({ timedOut: false, run: { id: plainRunId } });
      runs = runs.slice(1);

      const fiber = yield* service
        .waitForThread({ ...wait, untilOutput: true })
        .pipe(Effect.forkChild);
      yield* Effect.raceFirst(Deferred.await(subscribed), Fiber.await(fiber));
      // Something wakes the thread. The turn it starts was asked for nothing, and submits anyway.
      // A request queued behind it asks for a result of its own, and is not the one owed.
      const queuedRunId = RunId.make("run:thread-management:owed:queued");
      runs = [
        ...runs,
        run(wakeRunId, 2, { status: "running" }),
        run(queuedRunId, 3, { status: "queued", outputSchema }),
      ];
      const accepted = yield* service.submitRunOutput({
        threadId,
        commandId: CommandId.make("command:thread-management:owed"),
        json: '{"head":"abc"}',
      });
      expect(accepted.runId).toBe(runId);
      expect(submitted).toMatchObject([
        { type: "run.output.submit", runId, value: { head: "abc" } },
      ]);
      const output = { value: { head: "abc" }, submittedAt: "2026-01-01T00:00:00.000Z" };
      runs = [run(runId, 1, { status: "completed", outputSchema, output }), runs[1]!, runs[2]!];
      yield* Queue.offer(events, stored(1, { type: "run.output-submitted", payload: {} }));
      expect(yield* Fiber.join(fiber)).toMatchObject({
        timedOut: false,
        run: { id: runId, output },
      });

      // The result is given once: a later plain turn has nothing to submit to.
      const again = yield* service
        .submitRunOutput({
          threadId,
          commandId: CommandId.make("command:thread-management:owed:again"),
          json: '{"head":"def"}',
        })
        .pipe(Effect.flip);
      expect(again.message).toContain("not asked for a result");
    }),
);

it.effect("waitForThread settles delegated work on a thread rolled back past its only run", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-rolled-back");
    const threadId = ThreadId.make("thread:thread-management:wait-rolled-back");
    const runId = RunId.make("run:thread-management:wait-rolled-back");
    const rolledBack = {
      id: runId,
      ordinal: 1,
      status: "rolled_back",
      startedAt: DateTime.makeUnsafe(0),
      completedAt: DateTime.makeUnsafe(1_000),
    } as unknown as OrchestrationV2Run;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          awaitMessageSteersSettled: () => Effect.void,
          delegatedTaskResultPending: () => Effect.succeed(false),
          streamStoredEventsFrom: () => Stream.never,
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, projectId, deletedAt: null },
              runs: [rolledBack],
              messages: [],
              subagents: [],
              providerThreads: [],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({
        projectId,
        threadId,
        runId,
        timeoutMs: 60_000,
        untilDelegatedWorkSettled: true,
      })
      .pipe(Effect.forkChild);
    // Nothing will ever run there again, so a wait still open runs out here.
    yield* TestClock.adjust(Duration.millis(60_000));
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({
      timedOut: false,
      run: { id: runId, status: "rolled_back" },
      resultRun: { id: runId, status: "rolled_back" },
    });
  }),
);

it.effect("waitForThread waits for output only from a run that was asked for one", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-output");
    const threadId = ThreadId.make("thread:thread-management:wait-output");
    const askedRunId = RunId.make("run:thread-management:wait-output:asked");
    const plainRunId = RunId.make("run:thread-management:wait-output:plain");
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(false),
          awaitMessageSteersSettled: () => Effect.void,
          streamStoredEventsFrom: () => Stream.never,
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, projectId, deletedAt: null },
              runs: [
                { id: askedRunId, status: "completed", outputSchema: { type: "object" } },
                { id: plainRunId, status: "completed" },
              ],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const wait = (runId: RunId) =>
      service
        .waitForThread({ projectId, threadId, runId, timeoutMs: 60_000, untilOutput: true })
        .pipe(Effect.forkChild);
    const asked = yield* wait(askedRunId);
    const plain = yield* wait(plainRunId);
    yield* TestClock.adjust(Duration.millis(60_000));

    // A later turn may still submit the result the first run owes.
    expect(yield* Fiber.join(asked)).toMatchObject({ timedOut: true, run: { id: askedRunId } });
    // Nothing can submit one for a run that never asked, so its end is the answer.
    expect(yield* Fiber.join(plain)).toMatchObject({
      timedOut: false,
      run: { id: plainRunId, status: "completed" },
    });
  }),
);

it.effect("waitForThread returns the cut run once its continuation is declined", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:wait-declined");
    const threadId = ThreadId.make("thread:thread-management:wait-declined");
    const runId = RunId.make("run:thread-management:wait-declined");
    const subscribed = yield* Deferred.make<void>();
    const declined = yield* Deferred.make<void>();
    let pending = true;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () => Effect.succeed(0),
          restartContinuationPending: () => Effect.succeed(pending),
          awaitMessageSteersSettled: () => Effect.void,
          awaitRestartContinuation: () => Deferred.await(declined),
          streamStoredEventsFrom: (input) =>
            input?.eventType === "run.updated"
              ? Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
                  Stream.drain,
                  Stream.concat(Stream.never),
                )
              : Stream.never,
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, projectId, deletedAt: null },
              runs: [{ id: runId, status: "cancelled" }],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .waitForThread({ projectId, threadId, runId, timeoutMs: 60 * 60 * 1_000 })
      .pipe(Effect.forkChild);
    yield* Deferred.await(subscribed);
    pending = false;
    yield* Deferred.succeed(declined, undefined);
    const result = yield* Fiber.join(fiber);

    expect(result).toMatchObject({ timedOut: false, run: { id: runId, status: "cancelled" } });
  }),
);

it.effect.each([
  "live",
  "no-match",
  "no-match-clear",
  "no-session",
  "unsupported-runtime",
] as const)(
  "marks background work awaited on the thread's active provider thread with state %s",
  (state) =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread:background-wait");
      const providerThreadId = ProviderThreadId.make("provider-thread:background-wait");
      const providerSessionId = ProviderSessionId.make("provider-session:background-wait");
      const driver = ProviderDriverKind.make("claudeAgent");
      const now = yield* DateTime.now;
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        providerSessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [{ taskId: "tests", kind: "command" }],
        contextUsage: null,
        nativeMetadata: null,
        createdAt: now,
        updatedAt: now,
      };
      const calls: Array<{ providerThreadId: ProviderThreadId; awaited: boolean }> = [];
      const runtime = {
        driver,
        providerSessionId,
        ...(state === "unsupported-runtime"
          ? {}
          : {
              awaitBackgroundTasks: (input: {
                readonly providerThread: OrchestrationV2ProviderThread;
                readonly taskIds: ReadonlyArray<string>;
                readonly awaited: boolean;
              }) =>
                Effect.sync(() => {
                  calls.push({
                    providerThreadId: input.providerThread.id,
                    awaited: input.awaited,
                  });
                  return { matched: input.taskIds.filter((taskId) => taskId === "tests") };
                }),
            }),
      } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(Orchestrator.OrchestratorV2)({
              getThreadRecords: () =>
                Effect.succeed({
                  thread: {
                    id: threadId,
                    deletedAt: null,
                    activeProviderThreadId: providerThreadId,
                  },
                  providerThreads: [providerThread],
                } as unknown as OrchestrationV2ThreadProjection),
            }),
            Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
              get: () =>
                Effect.succeed(state === "no-session" ? Option.none() : Option.some(runtime)),
            }),
          ),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );
      const result = yield* Effect.gen(function* () {
        const service = yield* ThreadManagementService.ThreadManagementService;
        return yield* service
          .awaitBackgroundTasks({
            threadId,
            taskIds: state.startsWith("no-match") ? ["finished"] : ["tests", "finished"],
            awaited: state !== "no-match-clear",
          })
          .pipe(Effect.result);
      }).pipe(Effect.provide(Layer.merge(layerTest, ThreadCommandExecutor.layer)));

      if (state === "live") {
        expect(result).toMatchObject({
          success: { matched: ["tests"], unmatched: ["finished"] },
        });
        expect(calls).toEqual([{ providerThreadId, awaited: true }]);
      } else if (state === "no-match") {
        expect(result).toMatchObject({
          failure: { _tag: "ThreadManagementBackgroundWaitError", reason: "unmatched" },
        });
        expect(calls).toEqual([{ providerThreadId, awaited: true }]);
      } else if (state === "no-match-clear") {
        expect(result).toMatchObject({ success: { matched: [], unmatched: ["finished"] } });
        expect(calls).toEqual([{ providerThreadId, awaited: false }]);
      } else {
        expect(result).toMatchObject({
          failure: {
            _tag: "ThreadManagementBackgroundWaitError",
            reason: state === "no-session" ? "unavailable" : "unsupported",
          },
        });
        expect(calls).toEqual([]);
      }
    }),
);

it.effect.each([
  { status: "completed" as const, followUp: "none", settles: true },
  { status: "failed" as const, followUp: "none", settles: false },
  { status: "interrupted" as const, followUp: "none", settles: false },
  { status: "completed" as const, followUp: "notification", settles: true },
  { status: "completed" as const, followUp: "replayed-notification", settles: true },
  { status: "completed" as const, followUp: "delegated", settles: true },
  { status: "completed" as const, followUp: "failed-notification", settles: false },
  { status: "completed" as const, followUp: "held-notification", settles: true },
  { status: "completed" as const, followUp: "user", settles: false },
  { status: "completed" as const, followUp: "steered-notification", settles: false },
  // Send now cancels the queued run; its message joins the run that asked to settle.
  { status: "completed" as const, followUp: "promoted-to-steer", settles: true },
  // A delegated-completion wake cancelled unstarted because the turn already took the result.
  { status: "completed" as const, followUp: "cancelled-delegated", settles: true },
  { status: "completed" as const, followUp: "stopped-delegated", settles: false },
])("settleAfterRun handles $status with $followUp follow-up", ({ status, followUp, settles }) =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("project:thread-management:settle-after-run");
    const threadId = ThreadId.make("thread:thread-management:settle-after-run");
    const runId = RunId.make("run:thread-management:settle-after-run");
    const followUpId = RunId.make(`${runId}:follow-up`);
    const subscribed = yield* Deferred.make<void>();
    const events = yield* Queue.unbounded<OrchestrationV2StoredEvent>();
    const replayRejected = yield* Deferred.make<void>();
    let sequence = 0;
    let sequenceReads = 0;
    let rejectedCommandId: CommandId | undefined;
    const runs = [{ id: runId, ordinal: 1, status, userMessageId: MessageId.make("original") }];
    const nextRun = {
      id: followUpId,
      ordinal: 2,
      status: "running" as OrchestrationV2Run["status"],
      userMessageId: MessageId.make("follow-up"),
      startedAt: followUp === "stopped-delegated" ? "2026-01-01T00:00:00.000Z" : null,
    };
    const dispatched: Array<string> = [];
    // The notification starts after the completed run was observed, before settlement commits.
    let raced = false;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadEventSequence: () =>
            Effect.sync(() => {
              if (followUp === "replayed-notification" && sequenceReads++ === 2) sequence = 1;
              return sequence;
            }),
          restartContinuationPending: () => Effect.succeed(false),
          awaitMessageSteersSettled: () => Effect.void,
          streamStoredEventsFrom: () =>
            Stream.fromEffect(Deferred.succeed(subscribed, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.fromQueue(events)),
            ),
          getThreadRecords: (_threadId, _fields, filter) =>
            Effect.sync(
              () =>
                ({
                  thread: { id: threadId, projectId, deletedAt: null },
                  runs: raced ? [...runs, nextRun] : runs,
                  messages: [
                    { id: MessageId.make("original"), runId, role: "user" as const },
                    {
                      id: nextRun.userMessageId,
                      runId: followUp === "promoted-to-steer" ? runId : followUpId,
                      role: "user" as const,
                      ...(followUp.endsWith("notification") ? { notification: {} } : {}),
                      ...(followUp.endsWith("delegated") ? { delegatedCompletion: {} } : {}),
                    },
                    { id: MessageId.make("reply"), runId: followUpId, role: "assistant" as const },
                    ...(followUp === "steered-notification"
                      ? [
                          {
                            id: MessageId.make("user-steer"),
                            runId: followUpId,
                            role: "user" as const,
                          },
                        ]
                      : []),
                  ].filter(
                    (message) =>
                      (filter?.messageIds === undefined ||
                        filter.messageIds.includes(message.id)) &&
                      (filter?.messageRunIds === undefined ||
                        filter.messageRunIds.includes(message.runId)) &&
                      (filter?.messageRoles === undefined ||
                        filter.messageRoles.includes(message.role)),
                  ),
                }) as unknown as OrchestrationV2ThreadProjection,
            ),
          dispatch: (command) =>
            Effect.gen(function* () {
              if (followUp !== "none" && !raced) {
                raced = true;
                rejectedCommandId = command.commandId;
                return yield* new Orchestrator.OrchestratorDispatchError({
                  commandId: command.commandId,
                  commandType: command.type,
                  cause: "Thread is still running",
                });
              }
              if (followUp === "replayed-notification" && command.commandId === rejectedCommandId) {
                yield* Deferred.succeed(replayRejected, undefined);
                return yield* new Orchestrator.OrchestratorCommandPreviouslyRejectedError({
                  commandId: command.commandId,
                  commandType: command.type,
                  detail: "Thread is still running",
                });
              }
              dispatched.push(command.type);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const service = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerTest),
    );
    const fiber = yield* service
      .settleAfterRun({ projectId, threadId, runId })
      .pipe(Effect.forkChild);
    if (followUp !== "none") {
      yield* Deferred.await(subscribed);
      expect(dispatched).toEqual([]);
      nextRun.status =
        followUp === "failed-notification"
          ? "failed"
          : followUp === "held-notification"
            ? "queued"
            : followUp === "promoted-to-steer" || followUp.endsWith("ed-delegated")
              ? "cancelled"
              : "completed";
      yield* Queue.offer(events, {
        sequence: 1,
        event: { type: "run.updated", threadId, payload: nextRun },
      } as unknown as OrchestrationV2StoredEvent);
    }
    if (followUp === "replayed-notification") {
      yield* Deferred.await(replayRejected);
      expect(dispatched).toEqual([]);
      sequence = 2;
      yield* Queue.offer(events, {
        sequence,
        event: { type: "run.updated", threadId, payload: nextRun },
      } as unknown as OrchestrationV2StoredEvent);
    }
    yield* Fiber.join(fiber);
    expect(dispatched).toEqual(settles ? ["thread.settle"] : []);
  }),
);

it.effect.each([
  {
    name: "kills the thread's own agent and ends its turn",
    others: [] as ReadonlyArray<string>,
    killed: true,
    expected: { outcome: "killed", sharedWithThreadIds: [] },
    teardown: ["release"],
  },
  {
    name: "keeps a process another working thread shares and only ends this turn",
    others: ["thread:thread-management:force-stop:other"],
    killed: false,
    expected: {
      outcome: "shared",
      sharedWithThreadIds: ["thread:thread-management:force-stop:other"],
    },
    teardown: ["detach"],
  },
  {
    name: "still settles a finished run's background work in a shared process",
    others: ["thread:thread-management:force-stop:other"],
    killed: false,
    settledRun: true,
    expected: {
      outcome: "shared",
      sharedWithThreadIds: ["thread:thread-management:force-stop:other"],
    },
    teardown: ["detach"],
  },
  {
    name: "detaches when a sibling starts a turn just before the session is released",
    others: [] as ReadonlyArray<string>,
    killed: false,
    siblingBusy: true,
    expected: { outcome: "shared", sharedWithThreadIds: [] },
    teardown: ["release", "detach"],
  },
  {
    name: "reports a teardown that has not finished in time as still releasing",
    others: [] as ReadonlyArray<string>,
    killed: false,
    releaseHangs: true,
    expected: { outcome: "releasing", sharedWithThreadIds: [] },
    teardown: ["release"],
  },
])("Force stop $name", ({ others, killed, expected, teardown, ...row }) =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:thread-management:force-stop");
    const providerSessionId = ProviderSessionId.make("provider-session:force-stop");
    const providerThreadId = ProviderThreadId.make("provider-thread:force-stop");
    const runId = RunId.make("run:thread-management:force-stop");
    const dispatched: Array<OrchestrationV2Command | { readonly type: string }> = [];
    const teardowns: Array<string> = [];
    const tornDown = yield* Deferred.make<void>();
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(
        Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({
          isLive: () => Effect.succeed(true),
          otherWorkingThreads: () => Effect.succeed(others.map((id) => ThreadId.make(id))),
          releaseUnlessBusy: () =>
            Effect.sync(() => teardowns.push("release")).pipe(
              Effect.andThen(
                "releaseHangs" in row ? Effect.never : Effect.succeed(!("siblingBusy" in row)),
              ),
            ),
          detach: () =>
            Effect.sync(() => teardowns.push("detach")).pipe(
              Effect.andThen(Deferred.succeed(tornDown, undefined)),
              Effect.asVoid,
            ),
        }),
      ),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, deletedAt: null },
              runs: [
                {
                  id: runId,
                  ordinal: 1,
                  status: "settledRun" in row ? "completed" : "running",
                  activeAttemptId: "attempt",
                },
                // A queued follow-up neither supersedes the request nor becomes the target.
                { id: RunId.make("run:queued-follow-up"), ordinal: 2, status: "queued" },
              ],
              providerThreads: [{ id: providerThreadId, appThreadId: threadId, providerSessionId }],
              providerTurns: [
                {
                  id: "provider-turn:force-stop",
                  providerThreadId,
                  runAttemptId: "attempt",
                  status: "settledRun" in row ? "completed" : "running",
                },
              ],
            } as unknown as OrchestrationV2ThreadProjection),
          dispatch: (command) =>
            Effect.sync(() => {
              dispatched.push(command);
              return { sequence: 1, storedEvents: [] } as never;
            }),
        }),
      ),
      Layer.provide(
        Layer.succeed(AgentScope, {
          ...AgentScope.defaultValue(),
          kill: () => Effect.succeed(killed),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    yield* Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      const stopping = yield* service.forceStopThread({ threadId, runId }).pipe(Effect.forkChild);
      yield* TestClock.adjust("10 seconds");
      expect(yield* Fiber.join(stopping)).toEqual(expected);
      expect(dispatched).toMatchObject([
        {
          type: "thread.background-work.settle",
          providerThreadId,
          reason: "Force stopped by user",
        },
      ]);
      if (teardown.at(-1) === "detach") yield* Deferred.await(tornDown);
      expect(teardowns).toEqual(teardown);
    }).pipe(Effect.provide(layerTest));
  }),
);

it.effect("Force stop refuses once a newer run has started, before killing anything", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:thread-management:force-stop-superseded");
    let killed = false;
    const layerTest = ThreadManagementService.layer.pipe(
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            Effect.succeed({
              thread: { id: threadId, deletedAt: null },
              runs: [
                { id: RunId.make("run:stopped"), ordinal: 1, status: "running" },
                { id: RunId.make("run:newer"), ordinal: 2, status: "running" },
              ],
              providerThreads: [],
              providerTurns: [],
            } as unknown as OrchestrationV2ThreadProjection),
        }),
      ),
      Layer.provide(
        Layer.succeed(AgentScope, {
          ...AgentScope.defaultValue(),
          kill: () => Effect.sync(() => (killed = true)),
        }),
      ),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(layerReceiptsTest),
    );
    const error = yield* Effect.gen(function* () {
      const service = yield* ThreadManagementService.ThreadManagementService;
      return yield* Effect.flip(
        service.forceStopThread({ threadId, runId: RunId.make("run:stopped") }),
      );
    }).pipe(Effect.provide(layerTest));
    expect(error.reason).toBe("superseded");
    expect(killed).toBe(false);
  }),
);

type GetThreadShell = Orchestrator.OrchestratorV2["Service"]["getThreadShell"];

const layerSharedShellReadsTest = (getThreadShell: GetThreadShell) =>
  ThreadManagementService.layer.pipe(
    Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
    Layer.provide(Layer.mock(Orchestrator.OrchestratorV2)({ getThreadShell })),
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(layerReceiptsTest),
  );

const sharedShellReadsTest = (
  getThreadShell: GetThreadShell,
  test: (
    forEvent: (threadId: ThreadId, sequence: number) => ReturnType<GetThreadShell>,
  ) => Effect.Effect<void, Orchestrator.OrchestratorV2Error>,
) =>
  Effect.gen(function* () {
    const threads = yield* ThreadManagementService.ThreadManagementService;
    return yield* test((threadId, sequence) =>
      threads.getThreadShellForEvent({ threadId, sequence }),
    );
  }).pipe(Effect.provide(layerSharedShellReadsTest(getThreadShell)));

const shellRead = (threadId: ThreadId, read: number) =>
  ({ id: threadId, title: `read ${read}` }) as unknown as OrchestrationV2ThreadShell;

it.effect("callers share an in-flight thread shell read, even after one leaves", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:shared-shell:in-flight");
    const release = yield* Deferred.make<void>();
    let reads = 0;
    yield* sharedShellReadsTest(
      (id) => Deferred.await(release).pipe(Effect.map(() => shellRead(id, ++reads))),
      (forEvent) =>
        Effect.gen(function* () {
          const leaving = yield* Effect.forkChild(forEvent(threadId, 5), {
            startImmediately: true,
          });
          const staying = yield* Effect.forkChild(forEvent(threadId, 5), {
            startImmediately: true,
          });
          yield* Fiber.interrupt(leaving);
          yield* Deferred.succeed(release, undefined);
          expect(yield* Fiber.join(staying)).toEqual(shellRead(threadId, 1));
          expect(reads).toBe(1);
        }),
    );
  }),
);

it.effect("a thread's shell reads run in order, and a queued read covers newer events", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:shared-shell:order");
    const firstStarted = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const log: Array<string> = [];
    let reads = 0;
    yield* sharedShellReadsTest(
      (id) =>
        Effect.gen(function* () {
          const read = ++reads;
          log.push(`start ${read}`);
          if (read === 1) {
            yield* Deferred.succeed(firstStarted, undefined);
            yield* Deferred.await(release);
          }
          log.push(`end ${read}`);
          return shellRead(id, read);
        }),
      (forEvent) =>
        Effect.gen(function* () {
          const first = yield* Effect.forkChild(forEvent(threadId, 1));
          yield* Deferred.await(firstStarted);
          const second = yield* Effect.forkChild(forEvent(threadId, 2), {
            startImmediately: true,
          });
          const third = yield* Effect.forkChild(forEvent(threadId, 3), {
            startImmediately: true,
          });
          yield* Deferred.succeed(release, undefined);
          expect(yield* Fiber.join(first)).toEqual(shellRead(threadId, 1));
          expect(yield* Fiber.join(second)).toEqual(shellRead(threadId, 2));
          expect(yield* Fiber.join(third)).toEqual(shellRead(threadId, 2));
          expect(log).toEqual(["start 1", "end 1", "start 2", "end 2"]);
        }),
    );
  }),
);

it.effect("a thread shell read serves its event and older ones until a newer event or expiry", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:shared-shell:sequence");
    let reads = 0;
    yield* sharedShellReadsTest(
      (id) => Effect.sync(() => shellRead(id, ++reads)),
      (forEvent) =>
        Effect.gen(function* () {
          expect(yield* forEvent(threadId, 5)).toEqual(shellRead(threadId, 1));
          expect(yield* forEvent(threadId, 5)).toEqual(shellRead(threadId, 1));
          expect(yield* forEvent(threadId, 3)).toEqual(shellRead(threadId, 1));
          expect(yield* forEvent(threadId, 6)).toEqual(shellRead(threadId, 2));
          expect(yield* forEvent(threadId, 5)).toEqual(shellRead(threadId, 2));
          yield* TestClock.adjust(
            Duration.millis(ThreadManagementService.SHARED_SHELL_READ_TTL_MS),
          );
          expect(yield* forEvent(threadId, 6)).toEqual(shellRead(threadId, 3));
        }),
    );
  }),
);

it.effect("a thread shell read requested after the service closed fails instead of waiting", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:shared-shell:closed");
    let reads = 0;
    const threads = yield* ThreadManagementService.ThreadManagementService.pipe(
      Effect.provide(layerSharedShellReadsTest((id) => Effect.sync(() => shellRead(id, ++reads)))),
    );
    for (const sequence of [1, 1]) {
      const exit = yield* Effect.exit(threads.getThreadShellForEvent({ threadId, sequence }));
      expect(Exit.hasInterrupts(exit)).toBe(true);
    }
    expect(reads).toBe(0);
  }),
);

it.effect("a failed thread shell read is not shared with later callers", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:shared-shell:failure");
    let reads = 0;
    yield* sharedShellReadsTest(
      (id) =>
        Effect.suspend(() =>
          ++reads === 1
            ? Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId: id }))
            : Effect.succeed(shellRead(id, reads)),
        ),
      (forEvent) =>
        Effect.gen(function* () {
          expect(Exit.isFailure(yield* Effect.exit(forEvent(threadId, 5)))).toBe(true);
          expect(yield* forEvent(threadId, 5)).toEqual(shellRead(threadId, 2));
        }),
    );
  }),
);

it.effect("shared thread shell reads keep only the most recent threads once reads settle", () =>
  Effect.gen(function* () {
    const threadIds = Array.from(
      { length: ThreadManagementService.SHARED_SHELL_READ_CAPACITY + 1 },
      (_, index) => ThreadId.make(`thread:shared-shell:capacity:${index}`),
    );
    const firstReleased = yield* Deferred.make<void>();
    const secondReleased = yield* Deferred.make<void>();
    const othersReleased = yield* Deferred.make<void>();
    const reads: Array<ThreadId> = [];
    yield* sharedShellReadsTest(
      (id) =>
        Deferred.await(
          id === threadIds[0]
            ? firstReleased
            : id === threadIds[1]
              ? secondReleased
              : othersReleased,
        ).pipe(Effect.andThen(Effect.sync(() => reads.push(id))), Effect.as(shellRead(id, 1))),
      (forEvent) =>
        Effect.gen(function* () {
          const callers = yield* Effect.forEach(threadIds, (threadId) =>
            Effect.forkChild(forEvent(threadId, 1), { startImmediately: true }),
          );
          yield* Deferred.succeed(firstReleased, undefined);
          yield* Fiber.join(callers[0]!);
          yield* Deferred.succeed(secondReleased, undefined);
          yield* Fiber.join(callers[1]!);
          yield* Deferred.succeed(othersReleased, undefined);
          yield* Fiber.joinAll(callers);
          yield* forEvent(threadIds.at(-1)!, 1);
          yield* forEvent(threadIds[0]!, 1);
          yield* forEvent(threadIds[1]!, 1);
        }),
    );
    expect(reads).toHaveLength(threadIds.length + 2);
    expect(new Set(reads.slice(0, threadIds.length))).toEqual(new Set(threadIds));
    expect(reads.slice(threadIds.length)).toEqual([threadIds[0], threadIds[1]]);
  }),
);

it.effect("hydrates a legacy transcript on the writer, then reads the projection on a reader", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "thread-management-legacy-" });
    const layerStores = EventSink.layer.pipe(
      Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
      Layer.provideMerge(SqlitePersistence.layerFromPath(path.join(directory, "statev2.sqlite"))),
    );
    const layerOrchestrator = Layer.unwrap(
      Effect.gen(function* () {
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        return Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadProjection: (threadId) =>
            projections
              .getThreadProjection(threadId)
              .pipe(
                Effect.mapError(
                  (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
                ),
              ),
        });
      }),
    );
    const layerTest = ThreadManagementService.layerWithLegacyImporter.pipe(
      Layer.provide(Layer.succeed(ThreadWorktreeRestore, { restore: () => Effect.void })),
      Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
      Layer.provide(layerOrchestrator),
      Layer.provide(ServerSettings.layerTest()),
      Layer.provide(CommandReceiptStore.layer),
      Layer.provideMerge(LegacyV1ThreadImporter.layer),
      Layer.provideMerge(layerStores),
    );
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threads = yield* ThreadManagementService.ThreadManagementService;
      const threadId = ThreadId.make("thread:thread-management:legacy");
      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, scripts_json, created_at, updated_at
        ) VALUES (
          'project:legacy', 'Legacy project', '/tmp/legacy', '[]',
          '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode,
          interaction_mode, created_at, updated_at
        ) VALUES (
          ${threadId}, 'project:legacy', 'Legacy thread',
          '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', 'default',
          '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO projection_thread_messages (
          message_id, thread_id, role, text, is_streaming, created_at, updated_at
        ) VALUES
          ('message:legacy:old', ${threadId}, 'user', 'Old question', 0,
            '2026-01-01T01:00:00.000Z', '2026-01-01T01:00:00.000Z'),
          ('message:legacy:latest', ${threadId}, 'user', 'Latest question', 0,
            '2026-01-02T01:00:00.000Z', '2026-01-02T01:00:00.000Z')
      `;
      yield* importer.reconcileShells;
      const shell = yield* projections.getThreadProjection(threadId);
      expect(shell.messages.map((message) => message.text)).toEqual(["Latest question"]);

      const statements: Array<{ readonly query: string; readonly span: Tracer.Span }> = [];
      const hydrated = yield* threads.getThreadProjection(threadId).pipe(
        Effect.provideService(Statement.CurrentTransformer, (statement, _sql, _fiber, span) => {
          statements.push({ query: statement.compile()[0], span });
          return Effect.succeed(statement);
        }),
        Effect.provideService(Statement.SpanPropagationEnabled, true),
      );
      expect(hydrated.messages.map((message) => message.text)).toEqual([
        "Old question",
        "Latest question",
      ]);
      const lineage = (span: Tracer.Span) => {
        const spans = [span];
        let parent = Option.getOrUndefined(span.parent);
        while (parent?._tag === "Span") {
          spans.push(parent);
          parent = Option.getOrUndefined(parent.parent);
        }
        return spans;
      };
      const connections = (matches: (query: string, spans: Array<Tracer.Span>) => boolean) => [
        ...new Set(
          statements
            .filter(({ query, span }) => matches(query, lineage(span)))
            .map(({ span }) =>
              lineage(span)
                .find((candidate) => candidate.attributes.has("db.connection"))
                ?.attributes.get("db.connection"),
            ),
        ),
      ];
      expect(connections((query) => /^\s*(INSERT|UPDATE)\b/.test(query))).toEqual(["writer"]);
      expect(
        connections((_query, spans) =>
          spans.some((span) => span.name === "ProjectionStore.readProjection"),
        ),
      ).toEqual(["reader"]);
    }).pipe(Effect.provide(layerTest));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "admits two transcript exports at a time and frees a slot on failure or interruption",
  () =>
    Effect.gen(function* () {
      const started = yield* Queue.unbounded<string>();
      const gates = new Map(
        ["a", "b", "c", "d", "e", "f"].map((id) => [
          id,
          Deferred.makeUnsafe<void, Orchestrator.OrchestratorV2Error>(),
        ]),
      );
      const layerTest = ThreadManagementService.layer.pipe(
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            getTranscriptByteFloor: () => Effect.succeed(0),
            getThreadSnapshot: (threadId) =>
              Queue.offer(started, threadId).pipe(
                Effect.andThen(Deferred.await(gates.get(threadId)!)),
                Effect.as({
                  schemaVersion: 1,
                  snapshotSequence: 1,
                  projection: {
                    thread: { id: threadId, title: threadId },
                    updatedAt: DateTime.makeUnsafe(0),
                    visibleTurnItems: [],
                  } as unknown as OrchestrationV2ThreadProjection,
                }),
              ),
          }),
        ),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(layerReceiptsTest),
      );

      yield* Effect.gen(function* () {
        const threads = yield* ThreadManagementService.ThreadManagementService;
        const exportThread = (id: string) =>
          threads.getThreadTranscript(ThreadId.make(id)).pipe(
            Effect.provideService(ServerEnvironment.ServerEnvironmentIdentity, {
              getEnvironmentId: Effect.succeed(EnvironmentId.make("environment:export")),
            }),
            Effect.forkChild({ startImmediately: true }),
          );
        const a = yield* exportThread("a");
        const b = yield* exportThread("b");
        const c = yield* exportThread("c");
        const d = yield* exportThread("d");
        const e = yield* exportThread("e");
        const f = yield* exportThread("f");
        expect(yield* Queue.takeAll(started)).toEqual(["a", "b"]);

        yield* Fiber.interrupt(c);
        yield* Deferred.fail(
          gates.get("a")!,
          new Orchestrator.OrchestratorProjectionError({ threadId: ThreadId.make("a"), cause: "" }),
        );
        expect(yield* Queue.take(started)).toBe("d");
        yield* Fiber.interrupt(b);
        expect(yield* Queue.take(started)).toBe("e");
        yield* Deferred.succeed(gates.get("d")!, undefined);
        expect(yield* Queue.take(started)).toBe("f");
        yield* Deferred.succeed(gates.get("e")!, undefined);
        yield* Deferred.succeed(gates.get("f")!, undefined);

        expect((yield* Fiber.await(a))._tag).toBe("Failure");
        expect((yield* Fiber.join(d)).threadId).toBe("d");
        expect((yield* Fiber.join(e)).threadId).toBe("e");
        expect((yield* Fiber.join(f)).threadId).toBe("f");
        expect(yield* Queue.size(started)).toBe(0);
      }).pipe(Effect.provide(layerTest));
    }),
);

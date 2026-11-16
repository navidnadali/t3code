import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderFailure,
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
import * as Result from "effect/Result";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX } from "./QueuedDeliveryPromotion.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

const driver = ProviderDriverKind.make("codex");
const instanceA = ProviderInstanceId.make("codex");
const instanceB = ProviderInstanceId.make("codex-b");
const models: ReadonlyArray<ModelSelection> = [
  { instanceId: instanceA, model: "gpt-a" },
  { instanceId: instanceA, model: "gpt-b" },
  { instanceId: instanceB, model: "gpt-b" },
];
const adapterFor = (instanceId: ProviderInstanceId) =>
  ({
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    openSession: () => Effect.die("No provider process needed to start a queued run"),
  }) as ProviderAdapter.ProviderAdapterV2["Service"];

// The command projection startNextQueuedRun read before it was bounded.
const wholeQueueStartRead = (store: ProjectionStore.ProjectionStoreV2Shape, threadId: ThreadId) =>
  store.getThreadRecords(
    threadId,
    [
      "runs",
      "attempts",
      "nodes",
      "subagents",
      "providerSessions",
      "providerThreads",
      "providerTurns",
      "runtimeRequests",
      "messages",
      "turnItems",
      "checkpointScopes",
      "contextTransfers",
    ],
    {
      turnItemTypes: ["user_message", "error"],
      messageRoles: ["user"],
      omitSettledLeafNodes: true,
      omitSettledSubagents: true,
      omitSettledSubagentRecords: true,
      contextTransferStatuses: ["pending"],
    },
  );

// Threads whose whole command projection the bounded runtime read.
const readWhole = new Set<ThreadId>();

/** `whole` swaps the bounded read for the whole one and takes every decision from it. */
const layerFor = (whole: boolean) => {
  const layerDatabase = SqlitePersistence.layerMemory;
  return Layer.mergeAll(
    layerDatabase,
    ProjectionStore.layer.pipe(Layer.provide(layerDatabase)),
    EffectOutbox.layer.pipe(Layer.provide(layerDatabase)),
    ProviderReplayHarness.layerWithRegistry(
      { name: whole ? "queue-start-whole" : "queue-start-bounded" },
      ProviderAdapterRegistry.layerFromAdapters([adapterFor(instanceA), adapterFor(instanceB)]),
      {
        databaseLayer: layerDatabase,
        runEffectWorker: false,
        wrapProjectionStore: (store) =>
          whole
            ? {
                ...store,
                getQueuedRunRecords: (threadId) => wholeQueueStartRead(store, threadId),
                hasPendingRestartContinuation: () => Effect.succeed(true),
              }
            : {
                ...store,
                getThreadRecords: ((threadId, fields, filter) => {
                  if (filter?.omitSettledSubagentRecords === true && fields.length === 12)
                    readWhole.add(threadId);
                  return store.getThreadRecords(threadId, fields, filter);
                }) as typeof store.getThreadRecords,
              },
      },
    ),
  );
};

// The clock never moves, so no idle check wakes; each fixture places its runs around it.
const NOW = Date.UTC(2000, 0, 1);

/** A bounded runtime and a reference runtime for a block of fixtures; each fixture has a thread. */
const runtimes = Effect.gen(function* () {
  yield* TestClock.setTime(NOW);
  return {
    bounded: yield* Layer.build(layerFor(false)),
    reference: yield* Layer.build(layerFor(true)),
  };
});

/** A small deterministic generator, so a failing fixture can be replayed by its seed. */
const random = (seed: number) => {
  let state = seed >>> 0;
  const next = () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    chance: (p: number) => next() < p,
    int: (min: number, max: number) => min + Math.floor(next() * (max - min + 1)),
    pick: <A>(values: ReadonlyArray<A>) => values[Math.floor(next() * values.length)]!,
  };
};

interface SeedRun {
  readonly status: OrchestrationV2Run["status"];
  readonly model: ModelSelection;
  readonly startedAt: number | null;
  readonly completedAt: number | null;
  /** A person's message the timeline shows; otherwise a background notice or a result wake. */
  readonly shown: boolean;
  readonly queueHeld?: boolean;
  readonly queuePosition?: number;
  readonly freshContext?: boolean;
  readonly worktree?: boolean;
  readonly failure?: OrchestrationV2ProviderFailure;
  /** The run, by index, whose task result this queued wake delivers. */
  readonly resultOf?: number;
  /** Runs whose results an earlier merge added to this wake. */
  readonly mergedResultOf?: ReadonlyArray<number>;
  /** The prompt, when not the default. */
  readonly text?: string;
  /** A starting, running or waiting run whose provider turn is still running. */
  readonly liveTurn?: boolean;
  /** A pull request watch wake (a final notice when `watchEnded`). */
  readonly pullRequest?: {
    readonly number: number;
    readonly watch: number;
    readonly watchEnded?: true;
  };
}

interface Fixture {
  readonly seed: number;
  readonly threadModel: ModelSelection;
  readonly worktreePath: string | null;
  readonly runs: ReadonlyArray<SeedRun>;
  readonly restartPendingFor?: number;
  /** The clock, in seconds like the runs' times; 0 otherwise. */
  readonly now?: number;
  readonly trigger: "message" | "resume";
  /** A queued run, by index, that this fixture's start must leave queued. */
  readonly staysQueued?: number;
}

const fixture = (seed: number): Fixture => {
  const rng = random(seed);
  const history = rng.int(2, 14);
  const runs: Array<SeedRun> = [];
  let clock = 1_000;
  for (let index = 0; index < history; index++) {
    const status = rng.pick([
      "completed",
      "completed",
      "completed",
      "failed",
      "interrupted",
      "cancelled",
    ] as const);
    const neverStarted = status === "cancelled" ? rng.chance(0.6) : rng.chance(0.05);
    clock += rng.int(10, 100);
    const startedAt = neverStarted ? null : clock - rng.int(0, 300);
    const completedAt = clock + rng.int(-40, 40);
    runs.push({
      status,
      model: rng.chance(0.8) ? models[0]! : rng.pick(models),
      startedAt,
      completedAt,
      shown: rng.chance(0.5),
      ...(rng.chance(0.15)
        ? {
            pullRequest: { number: rng.int(1, 3), watch: rng.int(1, 2), watchEnded: true as const },
          }
        : {}),
      ...(rng.chance(0.1) ? { freshContext: true } : {}),
      ...((status === "failed" || status === "interrupted") && rng.chance(0.2)
        ? { worktree: true }
        : {}),
      ...(status === "failed" && rng.chance(0.7)
        ? {
            failure: {
              class: rng.pick(["usage_limit", "usage_limit", "provider_error", "validation_error"]),
              message: "The provider failed.",
              code: null,
              retryable: null,
              resetAt: rng.chance(0.5)
                ? DateTime.formatIso(DateTime.makeUnsafe(rng.pick([0, 9_999_999_999_999])))
                : null,
            },
          }
        : {}),
    });
  }
  const parents = runs.flatMap((run, index) => (run.status === "completed" ? [index] : []));
  const queued = rng.int(1, 6);
  for (let index = 0; index < queued; index++) {
    const resultOf = rng.chance(0.4)
      ? parents.splice(rng.int(0, parents.length - 1), 1)[0]
      : undefined;
    const pullRequest =
      resultOf === undefined && rng.chance(0.4)
        ? { number: rng.int(1, 3), watch: rng.int(1, 2) }
        : undefined;
    runs.push({
      status: "queued",
      model: rng.chance(0.85) ? models[0]! : rng.pick(models),
      startedAt: null,
      completedAt: null,
      shown: resultOf === undefined && pullRequest === undefined && rng.chance(0.5),
      ...(pullRequest === undefined ? {} : { pullRequest }),
      ...(rng.chance(0.2) ? { queueHeld: true } : {}),
      ...(rng.chance(0.3) ? { queuePosition: rng.int(1, queued) } : {}),
      ...(rng.chance(0.05) ? { freshContext: true } : {}),
      ...(resultOf === undefined ? {} : { resultOf }),
      ...(resultOf !== undefined && parents.length > 0 && rng.chance(0.4)
        ? { mergedResultOf: parents.splice(0, rng.int(1, Math.min(2, parents.length))) }
        : {}),
    });
  }
  // Failed runs in a row, which space out the retries of hidden wakes, and a clock among them.
  const later = random(seed + 90_000);
  const streak = later.chance(0.3) ? later.int(2, 8) : 0;
  for (let index = Math.max(0, history - streak); index < history; index++) {
    runs[index] = { ...runs[index]!, status: "failed" };
  }
  const lastEnd = Math.max(
    ...runs.flatMap((run) => (run.completedAt === null ? [] : [run.completedAt])),
  );
  return {
    seed,
    threadModel: rng.chance(0.85) ? models[0]! : rng.pick(models),
    worktreePath: rng.chance(0.15) ? null : "/repo/worktree",
    runs,
    ...(rng.chance(0.15) ? { restartPendingFor: rng.int(0, history - 1) } : {}),
    trigger: rng.chance(0.8) ? "message" : "resume",
    ...(later.chance(0.5) ? { now: lastEnd + later.int(0, 1_200) } : {}),
  };
};

const completedRun = (model: ModelSelection, startedAt: number, completedAt: number): SeedRun => ({
  status: "completed",
  model,
  startedAt,
  completedAt,
  shown: true,
});
const queuedRun = (model: ModelSelection): SeedRun => ({
  status: "queued",
  model,
  startedAt: null,
  completedAt: null,
  shown: true,
});

/**
 * Runs from second 1000 on. Six push an earlier run out of the latest runs a queue start reads
 * (WAKE_RETRY_FAILURES_COUNTED by end, MAX_VISIBLE_STARTS_AHEAD_OF_WAKE by start).
 */
const laterRuns = (count: number) =>
  Array.from({ length: count }, (_, index) =>
    completedRun(models[0]!, 1_000 + index * 100, 1_100 + index * 100),
  );

/** Corners the random fixtures rarely reach, each read only through its own part of the read. */
const corners: ReadonlyArray<Fixture> = [
  // The other provider's latest run hit its limit, but neither starts last nor ends last.
  {
    seed: 1_001,
    threadModel: models[2]!,
    worktreePath: "/repo/worktree",
    runs: [
      {
        status: "failed",
        model: models[2]!,
        startedAt: 1_000,
        completedAt: 4_000,
        shown: true,
        failure: {
          class: "usage_limit",
          message: "Limit reached.",
          code: null,
          retryable: null,
          resetAt: "2286-11-20T17:46:39.999Z",
        },
      },
      completedRun(models[0]!, 2_000, 5_000),
      completedRun(models[0]!, 2_100, 2_200),
      completedRun(models[0]!, 2_200, 2_300),
      completedRun(models[0]!, 2_300, 2_400),
      completedRun(models[2]!, 2_500, 2_600),
      queuedRun(models[2]!),
    ],
    trigger: "message",
  },
  // The latest run a handoff starts from ran early, on the other provider.
  {
    seed: 1_002,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      completedRun(models[2]!, 50, 300),
      ...laterRuns(7),
      completedRun(models[2]!, 100, 200),
      queuedRun(models[0]!),
    ],
    trigger: "message",
  },
  // The run a restart continuation resumes still waits for it.
  {
    seed: 1_003,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      completedRun(models[0]!, 1_000, 1_100),
      completedRun(models[0]!, 1_100, 1_200),
      queuedRun(models[0]!),
    ],
    restartPendingFor: 1,
    trigger: "message",
    staysQueued: 2,
  },
  // Five failed runs in a row space a hidden wake's retry to 16 intervals; four would be 8.
  {
    seed: 1_007,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      completedRun(models[0]!, 1_000, 1_100),
      ...[1_200, 1_300, 1_400, 1_500, 1_600].map((at) => ({
        ...completedRun(models[0]!, at, at + 50),
        status: "failed" as const,
      })),
      { ...queuedRun(models[0]!), shown: false },
    ],
    trigger: "message",
    now: 1_650 + 300,
  },
  // The ended watch's final notice, from an early run; then a wake carrying an earlier merge.
  {
    seed: 1_004,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      completedRun(models[0]!, 50, 60),
      completedRun(models[0]!, 100, 150),
      completedRun(models[0]!, 120, 160),
      {
        ...completedRun(models[0]!, 130, 170),
        shown: false,
        pullRequest: { number: 2, watch: 1, watchEnded: true },
      },
      ...laterRuns(6),
      { ...queuedRun(models[0]!), shown: false, pullRequest: { number: 2, watch: 1 } },
    ],
    trigger: "message",
  },
  {
    seed: 1_005,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      completedRun(models[0]!, 50, 60),
      completedRun(models[0]!, 100, 150),
      completedRun(models[0]!, 120, 160),
      ...laterRuns(6),
      { ...queuedRun(models[0]!), shown: false, resultOf: 2, mergedResultOf: [1] },
      { ...queuedRun(models[0]!), shown: false, resultOf: 3 },
    ],
    trigger: "message",
  },
  // The last message shown, on another model, came before many wakes.
  {
    seed: 1_006,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      completedRun(models[1]!, 50, 100),
      ...[200, 300, 400, 500, 600, 700, 800].map((at) => ({
        ...completedRun(models[0]!, at, at + 50),
        shown: false,
      })),
      queuedRun(models[0]!),
    ],
    trigger: "message",
  },
  // Four visible starts passed the wake, but six earlier runs ended after them all.
  {
    seed: 1_008,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      ...[70, 71, 72, 73, 74, 75].map((at) => completedRun(models[0]!, at, at + 3_000)),
      ...[1_000, 1_100, 1_200, 1_300].map((at) => completedRun(models[0]!, at, at + 10)),
      { ...queuedRun(models[0]!), shown: false },
      queuedRun(models[0]!),
    ],
    trigger: "message",
  },
];

const watchStart = (watch: number) =>
  DateTime.formatIso(DateTime.makeUnsafe(Date.UTC(2025, 11, watch)));
const pullRequestKey = (number: number) => ({ host: "github.com", repository: "acme/app", number });

const seedFixture = Effect.fn("seedFixture")(function* (threadId: ThreadId, input: Fixture) {
  const at = (seconds: number) => DateTime.makeUnsafe(NOW + (seconds - (input.now ?? 0)) * 1000);
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const eventSink = yield* EventSink.EventSinkV2;
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`create:${threadId}`),
    threadId,
    projectId: ProjectId.make(`project:${threadId}`),
    title: "Queue start",
    modelSelection: input.threadModel,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: input.worktreePath,
    createdBy: "user",
    creationSource: "web",
  });
  let sequence = 0;
  const apply = (event: Omit<OrchestrationV2DomainEvent, "id">) =>
    projections.apply({
      ...event,
      id: EventId.make(`event:${threadId}:${++sequence}`),
    } as OrchestrationV2DomainEvent);
  const providerThreadOf = (instanceId: ProviderInstanceId) =>
    ProviderThreadId.make(`provider-thread:${threadId}:${instanceId}`);
  for (const instanceId of [instanceA, instanceB]) {
    const sessionId = ProviderSessionId.make(`session:${threadId}:${instanceId}`);
    yield* apply({
      type: "provider-session.attached",
      threadId,
      occurredAt: at(0),
      payload: {
        id: sessionId,
        driver,
        providerInstanceId: instanceId,
        status: "ready",
        cwd: "/repo/worktree",
        model: "gpt-a",
        capabilities: CodexProviderCapabilitiesV2,
        createdAt: at(0),
        updatedAt: at(instanceId === instanceA ? 1 : 0),
        lastError: null,
      },
    });
    yield* apply({
      type: "provider-thread.updated",
      threadId,
      occurredAt: at(0),
      payload: {
        id: providerThreadOf(instanceId),
        driver,
        providerInstanceId: instanceId,
        providerSessionId: sessionId,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver,
          nativeId: `native:${threadId}:${instanceId}`,
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: input.runs.length,
        handoffIds: [],
        forkedFrom: null,
        createdAt: at(0),
        updatedAt: at(0),
      },
    });
  }
  const thread = yield* projections.getThread(threadId);
  yield* apply({
    type: "thread.metadata-updated",
    threadId,
    occurredAt: at(0),
    payload: {
      ...thread,
      activeProviderThreadId: providerThreadOf(input.threadModel.instanceId),
      // Pull request 1 is watched, 2's watch ended, 3 is not linked.
      pullRequests: [1, 2].map((number) => ({
        ...pullRequestKey(number),
        url: `https://github.com/acme/app/pull/${number}`,
        source: "agent" as const,
        linkedAt: watchStart(1),
        snapshot: null,
        stack: null,
        [number === 1 ? "watch" : "endedWatch"]: {
          startedAt: watchStart(1),
          headSha: null,
          failedChecks: [],
          passed: false,
          passedChecks: [],
          remarksThrough: watchStart(1),
          remarkIds: [],
          conflicting: false,
          wakes: 0,
        },
      })),
    },
  });
  const runs: Array<OrchestrationV2Run> = [];
  for (const [index, seed] of input.runs.entries()) {
    const n = index + 1;
    const runId = RunId.make(`run:${threadId}:${n}`);
    const attemptId = RunAttemptId.make(`attempt:${threadId}:${n}`);
    const rootNodeId = NodeId.make(`node:${threadId}:root:${n}`);
    const turnId = ProviderTurnId.make(`turn:${threadId}:${n}`);
    const messageId = MessageId.make(`message:${threadId}:${n}`);
    const providerThreadId = providerThreadOf(seed.model.instanceId);
    const queued = seed.status === "queued";
    const live =
      seed.status === "starting" || seed.status === "running" || seed.status === "waiting";
    const requestedAt = at((index + 1) * 10);
    const startedAt = seed.startedAt === null ? null : at(seed.startedAt);
    const completedAt = seed.completedAt === null ? null : at(seed.completedAt);
    const run: OrchestrationV2Run = {
      id: runId,
      threadId,
      ordinal: n,
      providerInstanceId: seed.model.instanceId,
      modelSelection: seed.model,
      providerThreadId,
      userMessageId: messageId,
      rootNodeId,
      activeAttemptId: attemptId,
      status: seed.status,
      queuePosition: queued ? (seed.queuePosition ?? n) : null,
      ...(seed.queueHeld === undefined ? {} : { queueHeld: seed.queueHeld }),
      ...(seed.freshContext === undefined ? {} : { freshContext: seed.freshContext }),
      ...(seed.worktree === true
        ? { workspacePreparation: { type: "worktree" as const, baseRef: "main" } }
        : {}),
      requestedAt,
      startedAt,
      completedAt,
      checkpointId: null,
      contextHandoffId: null,
    };
    runs.push(run);
    yield* apply({
      type: "run.created",
      threadId,
      runId,
      nodeId: rootNodeId,
      occurredAt: requestedAt,
      payload: run,
    });
    yield* apply({
      type: "run-attempt.created",
      threadId,
      runId,
      occurredAt: requestedAt,
      payload: {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId,
        providerInstanceId: seed.model.instanceId,
        providerThreadId,
        providerTurnId: startedAt === null ? null : turnId,
        reason: "initial",
        status: queued ? "pending" : live ? "running" : "completed",
        startedAt,
        completedAt,
      },
    });
    yield* apply({
      type: "node.updated",
      threadId,
      runId,
      nodeId: rootNodeId,
      occurredAt: requestedAt,
      payload: {
        id: rootNodeId,
        threadId,
        runId,
        parentNodeId: null,
        rootNodeId,
        kind: "root_turn",
        status: queued ? "pending" : live ? "running" : "completed",
        countsForRun: true,
        providerThreadId,
        providerTurnId: startedAt === null ? null : turnId,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt,
        completedAt,
      },
    });
    if (startedAt !== null) {
      yield* apply({
        type: "provider-turn.updated",
        threadId,
        nodeId: rootNodeId,
        occurredAt: requestedAt,
        payload: {
          id: turnId,
          providerThreadId,
          nodeId: rootNodeId,
          runAttemptId: attemptId,
          nativeTurnRef: { driver, nativeId: `native:turn:${n}`, strength: "strong" },
          ordinal: n,
          status: seed.liveTurn === true ? "running" : "completed",
          startedAt,
          completedAt,
        },
      });
    }
    const taskId = NodeId.make(`node:${threadId}:task:${n}`);
    const parent = seed.resultOf === undefined ? undefined : runs[seed.resultOf]!;
    const merged = (seed.mergedResultOf ?? []).map((index) => ({
      parentRun: runs[index]!,
      taskId: NodeId.make(`node:${threadId}:task:${n}:${index + 1}`),
    }));
    yield* apply({
      type: "message.updated",
      threadId,
      runId,
      nodeId: rootNodeId,
      occurredAt: requestedAt,
      payload: {
        id: messageId,
        threadId,
        runId,
        nodeId: rootNodeId,
        role: "user",
        text: seed.text ?? (seed.shown ? `Prompt ${n}` : `Wake ${n}`),
        attachments: [],
        streaming: false,
        createdAt: requestedAt,
        updatedAt: requestedAt,
        ...(seed.shown
          ? { createdBy: "user", creationSource: "web" }
          : parent !== undefined
            ? {
                createdBy: "agent",
                creationSource: "server",
                delegatedCompletion: {
                  parentRunId: parent.id,
                  generation: 1,
                  taskIds: [taskId, ...merged.map((part) => part.taskId)],
                  ...(merged.length === 0
                    ? {}
                    : {
                        merged: merged.map((part) => ({
                          parentRunId: part.parentRun.id,
                          generation: 1,
                          taskIds: [part.taskId],
                        })),
                      }),
                },
              }
            : seed.pullRequest !== undefined
              ? {
                  createdBy: "agent",
                  creationSource: "server",
                  notification: {
                    source: {
                      kind: "monitor",
                      pullRequest: {
                        ...pullRequestKey(seed.pullRequest.number),
                        watchStartedAt: watchStart(seed.pullRequest.watch),
                        headSha: null,
                        ...(seed.pullRequest.watchEnded ? { watchEnded: true } : {}),
                      },
                    },
                    outcome: "updated",
                    summary: `Pull request ${seed.pullRequest.number}`,
                  },
                }
              : {
                  createdBy: "agent",
                  creationSource: "server",
                  notification: {
                    source: { kind: "background_task" },
                    outcome: "completed",
                    summary: `Wake ${n}`,
                  },
                }),
      },
    } as Omit<OrchestrationV2DomainEvent, "id">);
    const itemBase = {
      threadId,
      runId,
      nodeId: rootNodeId,
      providerThreadId,
      providerTurnId: startedAt === null ? null : turnId,
      nativeItemRef: null,
      parentItemId: null,
      title: null,
      startedAt: requestedAt,
      completedAt: requestedAt,
      updatedAt: requestedAt,
    };
    if (!queued) {
      yield* apply({
        type: "turn-item.updated",
        threadId,
        runId,
        nodeId: rootNodeId,
        occurredAt: requestedAt,
        payload: seed.shown
          ? {
              ...itemBase,
              id: TurnItemId.make(`item:${threadId}:user:${n}`),
              ordinal: n * 100,
              status: "completed",
              type: "user_message",
              messageId,
              inputIntent: "turn_start",
              text: seed.text ?? `Prompt ${n}`,
              attachments: [],
              createdBy: "user",
              creationSource: "web",
            }
          : {
              ...itemBase,
              id: TurnItemId.make(`item:${threadId}:wake:${n}`),
              ordinal: n * 100,
              status: "completed",
              type: "notification",
              source: { kind: "background_task" },
              outcome: "completed",
              summary: `Wake ${n}`,
            },
      });
    }
    if (seed.failure !== undefined) {
      yield* apply({
        type: "turn-item.updated",
        threadId,
        runId,
        nodeId: rootNodeId,
        occurredAt: requestedAt,
        payload: {
          ...itemBase,
          id: TurnItemId.make(`item:${threadId}:error:${n}`),
          ordinal: n * 100 + 1,
          status: "failed",
          title: "Provider failure",
          type: "error",
          failure: seed.failure,
        },
      });
    }
    for (const owner of parent === undefined ? [] : [{ parentRun: parent, taskId }, ...merged]) {
      yield* apply({
        type: "subagent.updated",
        threadId,
        runId: owner.parentRun.id,
        nodeId: owner.taskId,
        occurredAt: requestedAt,
        payload: {
          id: owner.taskId,
          threadId,
          runId: owner.parentRun.id,
          parentNodeId: owner.parentRun.rootNodeId!,
          origin: "app_owned",
          createdBy: "agent",
          driver,
          providerInstanceId: instanceA,
          providerThreadId: null,
          childThreadId: ThreadId.make(`thread:${threadId}:child:${owner.taskId}`),
          nativeTaskRef: null,
          prompt: `Task ${n}`,
          title: `Task ${n}`,
          model: null,
          completionDelivery: { state: "claimed", observedByRunId: null },
          status: "completed",
          result: `Result ${n}`,
          startedAt: requestedAt,
          completedAt: requestedAt,
          updatedAt: requestedAt,
        },
      });
      yield* apply({
        type: "run.updated",
        threadId,
        runId: owner.parentRun.id,
        occurredAt: requestedAt,
        payload: {
          ...owner.parentRun,
          delegatedCompletion: {
            disposition: "open",
            nextGeneration: 2,
            delivery: { generation: 1, messageId, taskIds: [owner.taskId] },
          },
        },
      });
    }
  }
  if (input.restartPendingFor !== undefined) {
    const source = runs[input.restartPendingFor]!;
    yield* eventSink.writeWithEffects({
      events: [],
      effects: [
        {
          id: `effect:restart-continuation:${source.id}`,
          commandId: CommandId.make(`command:restart-prepare:${source.id}`),
          threadId,
          request: {
            type: "provider-runtime.continue",
            sourceRunId: source.id,
            processLossCount: 0,
          },
        },
      ],
    });
  }
});

// Context transfers and handoffs take random ids; name each by its first appearance.
const withoutRandomIds = <A>(value: A): A => {
  const seen = new Map<string, string>();
  return JSON.parse(
    JSON.stringify(value).replace(
      /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g,
      (id) => seen.get(id) ?? (seen.set(id, `random-${seen.size}`), `random-${seen.size - 1}`),
    ),
  );
};

const queueStartThread = (input: Fixture) => ThreadId.make(`thread:queue-start:${input.seed}`);

/** Seeds the fixture, starts the queue, and returns everything the start wrote. */
const startQueue = (input: Fixture) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const threadId = queueStartThread(input);
    yield* seedFixture(threadId, input);
    // The old ordinal move read every newer run's prompt items; the store now asks SQL.
    const before = yield* wholeQueueStartRead(projections, threadId);
    const prompts = (yield* projections.getThreadRecords(threadId, ["turnItems"], {
      turnItemTypes: ["user_message", "notification"],
    })).turnItems;
    const promptsAfter = yield* Effect.forEach(
      before.runs.filter((run) => run.status === "queued"),
      (queuedRun) =>
        projections
          .hasRunPromptAfter(threadId, queuedRun.ordinal)
          .pipe(
            Effect.map((found) => [
              found,
              before.runs.some(
                (run) =>
                  run.status !== "queued" &&
                  run.ordinal > queuedRun.ordinal &&
                  prompts.some((item) => item.runId === run.id),
              ),
            ]),
          ),
    );
    const outcome = yield* (
      input.trigger === "message"
        ? orchestrator.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`trigger:${input.seed}`),
            threadId,
            messageId: MessageId.make(`message:trigger:${input.seed}`),
            text: "And this.",
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "mcp",
            senderThreadId: ThreadId.make("thread:queue-start:sender"),
          })
        : orchestrator.dispatch({
            type: "queue.resume",
            commandId: CommandId.make(`trigger:${input.seed}`),
            threadId,
          })
    ).pipe(Effect.result);
    const after = yield* projections.getThreadProjection(threadId);
    const effects = yield* Effect.forEach(after.runs, (run) =>
      outbox.listByCommandId(CommandId.make(`command:system:start-queued:${run.id}`)),
    );
    return withoutRandomIds({
      outcome: Result.isSuccess(outcome) ? "accepted" : outcome.failure._tag,
      projection: { ...after, updatedAt: null },
      effects: effects.flat().map((effect) => [effect.status, effect.request]),
      promptsAfter,
    });
  });

/** Checks each fixture's start against the reference, and counts how the starts read. */
const checkQueueStarts = Effect.fn("checkQueueStarts")(function* (
  fixtures: ReadonlyArray<Fixture>,
) {
  const { bounded: boundedRuntime, reference: referenceRuntime } = yield* runtimes;
  let started = 0;
  let startedBounded = 0;
  for (const input of fixtures) {
    const seed = input.seed;
    const bounded = yield* startQueue(input).pipe(Effect.provide(boundedRuntime));
    const reference = yield* startQueue(input).pipe(Effect.provide(referenceRuntime));
    for (const [found, expected] of bounded.promptsAfter) {
      assert.equal(found, expected, `seed ${seed}: a prompt after the queued run`);
    }
    assert.deepEqual(bounded, reference, `seed ${seed}`);
    if (input.staysQueued !== undefined) {
      assert.equal(bounded.projection.runs[input.staysQueued]?.status, "queued", `seed ${seed}`);
    }
    const start = bounded.projection.runs.some((run) => run.status === "starting");
    started += Number(start);
    startedBounded += Number(start && !readWhole.has(queueStartThread(input)));
  }
  const wholeRead = fixtures.filter((input) => readWhole.has(queueStartThread(input))).length;
  return { started, startedBounded, wholeRead };
});

it.effect("a queue start at a corner decides as the whole command projection would", () =>
  Effect.asVoid(checkQueueStarts(corners)),
);

it.effect.each([1, 51, 101])(
  "queue starts from 50 seeds from %i decide as the whole command projection would",
  (first) =>
    Effect.gen(function* () {
      const counts = yield* checkQueueStarts(
        Array.from({ length: 50 }, (_, index) => fixture(first + index)),
      );
      // Most fixtures start a run from the bounded read alone; some need the whole one.
      assert.isAbove(counts.started, 40);
      assert.isAbove(counts.startedBounded, 20);
      assert.isAbove(counts.wholeRead, 5);
    }),
);

interface PromoteFixture extends Fixture {
  /** The queued run, by index, the command promotes. */
  readonly promotes: number;
  /** The run, by index, it steers. */
  readonly target: number;
  readonly automatic: boolean;
  /** Further queued runs, by index, merged into the same steer. */
  readonly merges?: ReadonlyArray<number>;
}

const promoteFixture = (seed: number): PromoteFixture => {
  const rng = random(seed + 50_000);
  const base = fixture(seed);
  const history = base.runs.filter((run) => run.status !== "queued");
  const queued = base.runs
    .filter((run) => run.status === "queued")
    .map((run) =>
      run.shown && rng.chance(0.2)
        ? { ...run, text: rng.pick(["/compact", "/goal ship it"]) }
        : run,
    );
  const status = rng.pick(["running", "running", "starting", "waiting"] as const);
  const target: SeedRun = {
    status,
    model: base.threadModel,
    startedAt: status === "starting" ? null : 5_000,
    completedAt: null,
    shown: rng.chance(0.8),
    liveTurn: status !== "starting" && rng.chance(0.7),
    ...(rng.chance(0.25) ? { text: "/compact" } : {}),
  };
  const runs = [...history, target, ...queued];
  const visible = queued.flatMap((run, index) => (run.shown ? [history.length + 1 + index] : []));
  const promotes =
    visible.length > 0 && rng.chance(0.85)
      ? rng.pick(visible)
      : rng.int(history.length + 1, runs.length - 1);
  const targetIndex = rng.chance(0.9) ? history.length : rng.int(0, history.length - 1);
  const automatic = rng.chance(0.3);
  const others = Array.from(
    { length: runs.length - history.length - 1 },
    (_, index) => history.length + 1 + index,
  ).filter((index) => index !== promotes);
  // Automatic Send now merges every due message, now and then one a refusal names.
  const merges =
    automatic && others.length > 0 && rng.chance(0.5) ? others.filter(() => rng.chance(0.6)) : [];
  return {
    ...base,
    runs,
    promotes,
    target: targetIndex,
    automatic,
    ...(merges.length === 0 ? {} : { merges }),
  };
};

/** Seeds the fixture, promotes the queued run, and returns everything the command wrote. */
const promote = (input: PromoteFixture) =>
  Effect.gen(function* () {
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const threadId = ThreadId.make(`thread:promote:${input.seed}`);
    yield* seedFixture(threadId, input);
    const runId = (index: number) => RunId.make(`run:${threadId}:${index + 1}`);
    const outcome = yield* orchestrator
      .dispatch({
        type: "queued-message.promote-to-steer",
        commandId: CommandId.make(
          `${input.automatic ? QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX : "promote:"}${input.seed}`,
        ),
        threadId,
        queuedRunId: runId(input.promotes),
        ...(input.merges === undefined ? {} : { mergedQueuedRunIds: input.merges.map(runId) }),
        targetRunId: runId(input.target),
      })
      .pipe(Effect.result);
    // The command's own events: a wait it ends lets the run-end reaction start the queue on
    // another fiber, which the queue-start fixtures cover.
    return withoutRandomIds({
      outcome: Result.isSuccess(outcome) ? "accepted" : outcome.failure._tag,
      // Why it was refused: a bounded read missing a merged run would refuse for another reason.
      cause: Result.isSuccess(outcome)
        ? null
        : String((outcome.failure as { readonly cause?: unknown }).cause),
      events: Result.isSuccess(outcome)
        ? outcome.success.storedEvents.map(({ event }) => ({
            type: event.type,
            payload: event.payload,
          }))
        : [],
    });
  });

// Skipping a compaction reorders the queue, where four visible starts passed the oldest wake.
const promoteCorners: ReadonlyArray<PromoteFixture> = [
  {
    seed: 2_001,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      completedRun(models[0]!, 1_000, 1_100),
      completedRun(models[0]!, 1_100, 1_200),
      completedRun(models[0]!, 1_200, 1_300),
      completedRun(models[0]!, 1_300, 1_400),
      {
        status: "running",
        model: models[0]!,
        startedAt: 5_000,
        completedAt: null,
        shown: true,
        liveTurn: true,
        text: "/compact",
      },
      { ...queuedRun(models[0]!), shown: false },
      queuedRun(models[0]!),
      queuedRun(models[0]!),
    ],
    trigger: "message",
    promotes: 7,
    target: 4,
    automatic: false,
  },
  // Send now on a run that only waits for its checkpoint: the message goes first, in queue order.
  ...[false, true].map((automatic): PromoteFixture => ({
    seed: automatic ? 2_003 : 2_002,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      completedRun(models[0]!, 1_000, 1_100),
      completedRun(models[0]!, 1_100, 1_200),
      completedRun(models[0]!, 1_200, 1_300),
      completedRun(models[0]!, 1_300, 1_400),
      {
        status: "waiting",
        model: models[0]!,
        startedAt: 5_000,
        completedAt: null,
        shown: true,
        liveTurn: false,
      },
      { ...queuedRun(models[0]!), shown: false },
      queuedRun(models[0]!),
      queuedRun(models[0]!),
    ],
    trigger: "message",
    promotes: 7,
    target: 4,
    automatic,
  })),
  // Automatic Send now merges the due messages behind a live turn into one steer.
  ...[false, true].map((automatic): PromoteFixture => ({
    seed: automatic ? 2_005 : 2_004,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [
      completedRun(models[0]!, 1_000, 1_100),
      completedRun(models[0]!, 1_100, 1_200),
      {
        status: "running",
        model: models[0]!,
        startedAt: 5_000,
        completedAt: null,
        shown: true,
        liveTurn: true,
      },
      { ...queuedRun(models[0]!), shown: false },
      queuedRun(models[0]!),
      queuedRun(models[0]!),
      queuedRun(models[0]!),
    ],
    trigger: "message",
    promotes: 4,
    target: 2,
    automatic,
    merges: [5, 6],
  })),
  // The target is an early run, outside the latest runs the read takes in.
  {
    seed: 2_006,
    threadModel: models[0]!,
    worktreePath: "/repo/worktree",
    runs: [completedRun(models[0]!, 50, 60), ...laterRuns(7), queuedRun(models[0]!)],
    trigger: "message",
    promotes: 8,
    target: 0,
    automatic: false,
  },
];

it.effect("a queued message promotes as the whole command projection would decide", () =>
  Effect.gen(function* () {
    const { bounded: boundedRuntime, reference: referenceRuntime } = yield* runtimes;
    const outcomes = new Map<string, number>();
    for (const input of [
      ...promoteCorners,
      ...Array.from({ length: 80 }, (_, index) => promoteFixture(index + 1)),
    ]) {
      const seed = input.seed;
      const bounded = yield* promote(input).pipe(Effect.provide(boundedRuntime));
      const reference = yield* promote(input).pipe(Effect.provide(referenceRuntime));
      assert.deepEqual(bounded, reference, `seed ${seed}`);
      const promoted = bounded.events.find(
        (event) =>
          event.type === "run.updated" &&
          (event.payload as { readonly ordinal?: number }).ordinal === input.promotes + 1,
      )?.payload as { readonly steerWhenReadyRunId?: string } | undefined;
      const kind =
        bounded.outcome !== "accepted"
          ? "rejected"
          : promoted?.steerWhenReadyRunId !== undefined
            ? "deferred"
            : bounded.events.some((event) => event.type === "thread.metadata-updated")
              ? "left queued"
              : "moved first";
      outcomes.set(kind, (outcomes.get(kind) ?? 0) + 1);
    }
    // Deferred steers, moves to the front (stops, ended waits), no-ops and refusals all occur.
    assert.includeMembers(
      [...outcomes.keys()],
      ["deferred", "moved first", "left queued", "rejected"],
    );
  }),
);

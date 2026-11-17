import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  NodeId,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2Subagent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import {
  delegatedTaskProgress,
  subagentHoldsCompletion,
} from "@t3tools/provider-core/server/subagentProjection";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import {
  delegatedTaskAwaitsWork,
  delegatedTaskChildRun,
  delegatedTaskReadCovers,
  restartNoteOnly,
} from "./RestartBackgroundNote.ts";

const layerTest = ProjectionStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory));
const instanceId = ProviderInstanceId.make("codex");
const driver = ProviderDriverKind.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };

// Deterministic, so a failing history can be rebuilt from its seed.
const random = (seed: number) => () => {
  seed = (seed + 0x6d2b79f5) | 0;
  let value = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
  return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
};

const RUN_STATUSES: ReadonlyArray<OrchestrationV2Run["status"]> = [
  "completed",
  "completed",
  "completed",
  "completed",
  "failed",
  "cancelled",
  "cancelled",
  "interrupted",
  "rolled_back",
  "queued",
  "queued",
  "preparing",
  "starting",
  "running",
  "waiting",
];
const TASK_STATUSES: ReadonlyArray<OrchestrationV2Subagent["status"]> = [
  "idle",
  "pending",
  "running",
  "waiting",
  "completed",
  "failed",
  "cancelled",
  "interrupted",
];
const UNFINISHED: ReadonlyArray<OrchestrationV2Run["status"]> = [
  "queued",
  "preparing",
  "starting",
  "running",
  "waiting",
];
const DELIVERY_STATES = ["pending", "claimed", "acknowledged", "delivered", "disposed"] as const;

/** A random delegated child: tagged and untagged runs, wakes, restarts, monitors and held work. */
const seedChild = Effect.fnUntraced(function* (seed: number) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const next = random(seed);
  const pick = <A>(values: ReadonlyArray<A>): A => values[Math.floor(next() * values.length)]!;
  const start = yield* DateTime.now;
  const at = (seconds: number) => DateTime.add(start, { seconds });
  const name = `equivalence-${seed}`;
  const threadId = ThreadId.make(`thread:${name}`);
  const originalTaskId = NodeId.make(`node:${name}:original`);
  const taskIds = [originalTaskId, ...[1, 2, 3].map((n) => NodeId.make(`node:${name}:task-${n}`))];
  const events: Array<Omit<OrchestrationV2DomainEvent, "id">> = [
    {
      type: "thread.created",
      threadId,
      occurredAt: start,
      payload: {
        createdBy: "agent",
        creationSource: "server",
        id: threadId,
        projectId: ProjectId.make(`project:${name}`),
        title: name,
        providerInstanceId: instanceId,
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: null,
        lineage: {
          parentThreadId: ThreadId.make(`thread:${name}:parent`),
          relationshipToParent: "subagent",
          rootThreadId: ThreadId.make(`thread:${name}:parent`),
        },
        forkedFrom: { type: "node", nodeId: originalTaskId },
        createdAt: start,
        updatedAt: start,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
    },
  ];
  const runIds: Array<RunId> = [];
  // A separate stream, so the histories above stay what they were for each seed.
  const nextNote = random(seed + 100_000);
  const turns: Array<Omit<OrchestrationV2DomainEvent, "id">> = [];
  const runCount = 1 + Math.floor(next() * 40);
  // Some children ran mostly monitor wakes since their earlier work.
  const monitorsFrom = next() < 0.3 ? Math.ceil(runCount * next()) : runCount + 1;
  for (let ordinal = 1; ordinal <= runCount; ordinal += 1) {
    const runId = RunId.make(`run:${name}:${ordinal}`);
    const status = pick(RUN_STATUSES);
    const unfinished = UNFINISHED.includes(status);
    const earlier = runIds.length === 0 ? undefined : pick(runIds);
    const tagged = next() < 0.55;
    // Ends out of ordinal order and on shared seconds; a few finished runs record no end.
    const completedAt = unfinished || next() < 0.05 ? null : at(Math.floor(next() * runCount * 2));
    const startedAt =
      status === "queued" || (status === "cancelled" && next() < 0.5) || next() < 0.05
        ? null
        : at(ordinal);
    events.push({
      type: "run.updated",
      threadId,
      runId,
      occurredAt: start,
      payload: {
        id: runId,
        threadId,
        ordinal,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId: null,
        userMessageId: MessageId.make(`message:${name}:${ordinal}`),
        rootNodeId: null,
        activeAttemptId: null,
        status,
        ...(status === "queued" && next() < 0.5 ? { queueHeld: true } : {}),
        requestedAt: at(ordinal),
        startedAt,
        completedAt,
        checkpointId: null,
        contextHandoffId: null,
        ...(tagged ? { delegatedTaskId: pick(taskIds) } : {}),
        ...(earlier !== undefined && next() < 0.15 ? { restartContinuationOfRunId: earlier } : {}),
        // What a restart leaves on a run: lost work, awaited or not, and its attempt's turn.
        ...(nextNote() < 0.8 ? { activeAttemptId: RunAttemptId.make(`attempt:${runId}`) } : {}),
        ...(nextNote() < 0.5
          ? {
              restartCancelledBackgroundWork: [
                {
                  kind: nextNote() < 0.7 ? ("shell" as const) : ("monitor" as const),
                  label: "tail -F inbox",
                  ...(nextNote() < 0.3 ? { awaited: true as const } : {}),
                },
              ],
            }
          : {}),
      },
    });
    if (nextNote() < 0.7)
      turns.push({
        type: "provider-turn.updated",
        threadId,
        runId,
        occurredAt: start,
        payload: {
          id: ProviderTurnId.make(`turn:${runId}`),
          providerThreadId: ProviderThreadId.make(`provider-thread:${name}:turns`),
          nodeId: NodeId.make(`node:${runId}:root`),
          runAttemptId: RunAttemptId.make(`attempt:${runId}`),
          nativeTurnRef: null,
          ordinal,
          status: (["completed", "cancelled", "running"] as const)[Math.floor(nextNote() * 3)]!,
          startedAt: at(ordinal),
          completedAt: null,
        },
      });
    const messages = next() < 0.1 ? 0 : next() < 0.15 ? 2 : 1;
    for (let index = 0; index < messages; index += 1) {
      const wakeFor = !tagged && earlier !== undefined && next() < 0.35 ? earlier : undefined;
      events.push({
        type: "message.updated",
        threadId,
        runId,
        occurredAt: start,
        payload: {
          id: MessageId.make(`message:${name}:${ordinal}${index === 0 ? "" : `:${index}`}`),
          threadId,
          runId,
          nodeId: null,
          role: "user",
          text: `Message ${ordinal}`,
          attachments: [],
          streaming: false,
          createdBy: "agent",
          creationSource: "server",
          createdAt: at(ordinal),
          updatedAt: at(ordinal),
          ...((ordinal >= monitorsFrom ? next() < 0.9 : next() < 0.2)
            ? {
                notification: {
                  source: { kind: "monitor" as const },
                  outcome: "updated" as const,
                  summary: "Monitor output",
                },
              }
            : {}),
          ...(wakeFor === undefined
            ? {}
            : { delegatedCompletion: { parentRunId: wakeFor, generation: 1, taskIds: [] } }),
        },
      });
    }
    runIds.push(runId);
  }
  const providerThread = (id: ProviderThreadId, owner: ThreadId) => ({
    type: "provider-thread.updated" as const,
    threadId: owner,
    occurredAt: start,
    payload: {
      id,
      driver,
      providerInstanceId: instanceId,
      providerSessionId: null,
      appThreadId: owner,
      ownerNodeId: null,
      nativeThreadRef: null,
      nativeConversationHeadRef: null,
      status: "idle" as const,
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      createdAt: start,
      updatedAt: start,
      pendingBackgroundTasks:
        next() < 0.6
          ? []
          : [
              {
                taskId: `background:${id}`,
                kind: pick(["command", "monitor", "subagent", "background_task"] as const),
                ...(next() < 0.3 ? { awaited: true as const } : {}),
              },
            ],
    },
  });
  for (let index = 0; index < Math.floor(next() * 3); index += 1) {
    events.push(
      providerThread(ProviderThreadId.make(`provider-thread:${name}:${index}`), threadId),
    );
  }
  // The child's own subagents, some with a provider thread of their own child.
  for (let index = 0; index < Math.floor(next() * 5); index += 1) {
    const grandchild = ThreadId.make(`thread:${name}:grandchild-${index}`);
    const providerThreadId =
      next() < 0.5 ? ProviderThreadId.make(`provider-thread:${name}:grandchild-${index}`) : null;
    if (providerThreadId !== null) events.push(providerThread(providerThreadId, grandchild));
    const state = next() < 0.2 ? undefined : pick(DELIVERY_STATES);
    events.push({
      type: "subagent.updated",
      threadId,
      nodeId: NodeId.make(`node:${name}:subagent-${index}`),
      occurredAt: start,
      payload: {
        id: NodeId.make(`node:${name}:subagent-${index}`),
        threadId,
        runId: next() < 0.15 ? null : pick(runIds),
        parentNodeId: NodeId.make(`node:${name}:root`),
        origin: pick(["app_owned", "provider_native"] as const),
        createdBy: "agent",
        driver,
        providerInstanceId: instanceId,
        providerThreadId,
        childThreadId: grandchild,
        nativeTaskRef: null,
        prompt: "Sub-work",
        title: null,
        model: null,
        ...(state === undefined ? {} : { completionDelivery: { state, observedByRunId: null } }),
        status: pick(TASK_STATUSES),
        result: null,
        startedAt: start,
        completedAt: null,
        updatedAt: start,
      },
    });
  }
  events.push(...turns);
  for (const [index, event] of events.entries()) {
    yield* store.apply({
      ...event,
      id: EventId.make(`event:${name}:${index}`),
    } as OrchestrationV2DomainEvent);
  }
  // Restart continuations in every state, beside an unrelated effect of the same run.
  const sql = yield* SqlClient.SqlClient;
  const createdAt = DateTime.formatIso(start);
  const awaiting = new Set<RunId>();
  for (const runId of runIds) {
    if (next() >= 0.2) continue;
    const status = pick(["pending", "running", "succeeded", "failed", "cancelled"] as const);
    if (status === "pending" || status === "running") awaiting.add(runId);
    for (const [effectId, effectType] of [
      [`effect:restart-continuation:${runId}`, "provider-runtime.continue"],
      [`effect:checkpoint:${runId}`, "checkpoint.capture"],
    ] as const) {
      yield* sql`
        INSERT INTO orchestration_v2_effect_outbox (
          effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count,
          available_at, created_at, updated_at
        )
        VALUES (
          ${effectId}, ${`command:${name}`}, ${threadId}, ${effectType}, '{}',
          ${effectType === "checkpoint.capture" ? "pending" : status}, 0,
          ${createdAt}, ${createdAt}, ${createdAt}
        )
      `;
    }
  }
  return { threadId, originalTaskId, taskIds, awaiting };
});

type ChildRecords = ProjectionStore.DelegatedTaskRecords["records"];

// The runs childAwaitsRestartContinuation finds holding the result for a continuation.
const awaitingSince = (
  runs: ReadonlyArray<OrchestrationV2Run>,
  run: OrchestrationV2Run,
  awaiting: ReadonlySet<RunId>,
) =>
  runs
    .filter(
      (candidate) =>
        (candidate.id === run.id || runRanAfter(candidate, run)) && awaiting.has(candidate.id),
    )
    .map((candidate) => candidate.id)
    .toSorted();

/** What finalizing a child decides from its records, for one task. */
const taskDecisions = (
  child: ChildRecords,
  taskId: NodeId,
  originalTaskId: NodeId,
  resultRun: OrchestrationV2Run | undefined,
  awaiting: ReadonlySet<RunId>,
) => {
  const { taskRuns, childRun } = delegatedTaskChildRun(child, taskId, originalTaskId, resultRun);
  return {
    hasRuns: taskRuns.length > 0,
    childRun: childRun?.id,
    awaitsWork: delegatedTaskAwaitsWork(taskRuns, child),
    awaitsSubagents: delegatedTaskAwaitsWork(taskRuns, { ...child, providerThreads: [] }),
    // Only a finished result run is checked for a restart continuation.
    restartChecks:
      childRun === undefined || UNFINISHED.includes(childRun.status)
        ? []
        : awaitingSince(taskRuns.length === 0 ? child.runs : taskRuns, childRun, awaiting),
  };
};

const childDecisions = (child: ChildRecords, awaiting: ReadonlySet<RunId>) => {
  const progress = delegatedTaskProgress(child);
  const chain: Array<RunId> = [];
  for (
    let run = progress.resultRun;
    run !== undefined && !chain.includes(run.id);
    run = child.runs.find((candidate) => candidate.id === run?.restartContinuationOfRunId)
  )
    chain.push(run.id);
  return {
    state: progress.state,
    resultRun: progress.resultRun?.id,
    holds: child.subagents.some(subagentHoldsCompletion),
    background: child.providerThreads.some((thread) =>
      backgroundWorkHoldsCompletion(thread.pendingBackgroundTasks ?? []),
    ),
    pendingBackground: child.providerThreads.some(
      (thread) => (thread.pendingBackgroundTasks ?? []).length > 0,
    ),
    restartChecks:
      progress.resultRun === undefined
        ? []
        : awaitingSince(child.runs, progress.resultRun, awaiting),
    restartChain: chain,
  };
};

it.layer(layerTest)("delegated task records", (it) => {
  // Every decision a windowed read reports covered matches the one on the child's full read.
  it.effect("decide as the full read does wherever the window covers them", () =>
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      let windowedTasks = 0;
      let windowedTails = 0;
      let noteCandidates = 0;
      for (let seed = 1; seed <= 250; seed += 1) {
        const { threadId, originalTaskId, taskIds, awaiting } = yield* seedChild(seed);
        const full = yield* store.getThreadRecords(
          threadId,
          ["runs", "messages", "subagents", "providerThreads"],
          { messageRoles: ["user"] },
        );
        // A pending note that a restart cut only unawaited work holds no result. The full read has
        // every provider turn; the orchestrator reads only a candidate run's own attempt's turns.
        const fullTurns = (yield* store.getThreadRecords(threadId, ["providerTurns"]))
          .providerTurns;
        const holding = new Set(
          full.runs.flatMap((run) =>
            awaiting.has(run.id) && !restartNoteOnly(run, fullTurns) ? [run.id] : [],
          ),
        );
        const fullDecisions = childDecisions(full, holding);
        const { resultRun } = delegatedTaskProgress(full);
        for (const runWindow of [1, 2, 3, 6]) {
          const read = yield* store.getDelegatedTaskRecords(threadId, runWindow);
          const windowHolding = new Set<RunId>();
          for (const run of read.records.runs) {
            if (!awaiting.has(run.id)) continue;
            if (run.activeAttemptId !== null && restartNoteOnly(run, [])) {
              noteCandidates += 1;
              const { providerTurns } = yield* store.getThreadRecords(threadId, ["providerTurns"], {
                providerTurnAttemptIds: [run.activeAttemptId],
              });
              if (restartNoteOnly(run, providerTurns)) continue;
            }
            windowHolding.add(run.id);
          }
          // Every awaited run is read, whatever the window.
          assert.deepEqual([...windowHolding].toSorted(), [...holding].toSorted());
          const exact = yield* store.getDelegatedTaskRecords(threadId, runWindow, {
            resultRun: true,
          });
          const context = `seed ${seed}, window ${runWindow}`;
          // The state needs no window; its result run needs `resultRun`, the runs after it a window.
          const { resultRun: _, ...state } = childDecisions(read.records, windowHolding);
          const { resultRun: __, ...fullState } = fullDecisions;
          assert.deepEqual(
            { ...state, restartChecks: [], restartChain: [] },
            { ...fullState, restartChecks: [], restartChain: [] },
            context,
          );
          const decisions = childDecisions(exact.records, windowHolding);
          assert.deepEqual(
            { ...decisions, restartChecks: [], restartChain: [] },
            { ...fullDecisions, restartChecks: [], restartChain: [] },
            context,
          );
          const tail = { originalTaskId, resultRun, stoppedResultRun: resultRun };
          if (delegatedTaskReadCovers(exact, { ...tail, tasks: [] })) {
            if (exact.ordinalFloor !== null) windowedTails += 1;
            assert.deepEqual(decisions, fullDecisions, context);
          }
          for (const taskId of taskIds) {
            // Only the original task reports the result run, which finalizing reads it for.
            const taskRead = taskId === originalTaskId ? exact : read;
            if (
              !delegatedTaskReadCovers(taskRead, {
                ...tail,
                resultRun: delegatedTaskProgress(taskRead.records).resultRun,
                stoppedResultRun: undefined,
                tasks: [{ id: taskId }],
              })
            )
              continue;
            if (taskRead.ordinalFloor !== null) windowedTasks += 1;
            assert.deepEqual(
              taskDecisions(
                taskRead.records,
                taskId,
                originalTaskId,
                delegatedTaskProgress(taskRead.records).resultRun,
                windowHolding,
              ),
              taskDecisions(full, taskId, originalTaskId, resultRun, holding),
              `${context}, task ${taskId}`,
            );
          }
        }
      }
      // Many covered decisions came from a window smaller than the child.
      assert.isAbove(windowedTasks, 500);
      assert.isAbove(windowedTails, 50);
      assert.isAbove(noteCandidates, 50);
    }),
  );

  // A follow-up promoted to Steer owns no run of its own that does work: its queued run was
  // cancelled and its message moved to the run it joined, which another task may own.
  it.effect.each([
    "joined run live",
    "joined run's subagent holds",
    "joined run restarted",
    "queued again",
    "an older queued run of its own",
  ] as const)(
    "decides a promoted follow-up as the full read does wherever the window covers it (%s)",
    (shape) =>
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const start = yield* DateTime.now;
        const at = (seconds: number) => DateTime.add(start, { seconds });
        const name = `promoted-${shape.replaceAll(" ", "-").replaceAll("'", "")}`;
        const threadId = ThreadId.make(`thread:${name}`);
        const originalTaskId = NodeId.make(`node:${name}:original`);
        const joinedTaskId = NodeId.make(`node:${name}:joined`);
        const promotedTaskId = NodeId.make(`node:${name}:promoted`);
        const runId = (ordinal: number) => RunId.make(`run:${name}:${ordinal}`);
        const events: Array<Omit<OrchestrationV2DomainEvent, "id">> = [];
        const run = (
          ordinal: number,
          status: OrchestrationV2Run["status"],
          fields: Partial<OrchestrationV2Run> = {},
        ) =>
          events.push({
            type: "run.updated",
            threadId,
            runId: runId(ordinal),
            occurredAt: start,
            payload: {
              id: runId(ordinal),
              threadId,
              ordinal,
              providerInstanceId: instanceId,
              modelSelection,
              providerThreadId: null,
              userMessageId: MessageId.make(`message:${name}:${ordinal}`),
              rootNodeId: null,
              activeAttemptId: null,
              status,
              requestedAt: at(ordinal),
              startedAt: status === "queued" ? null : at(ordinal),
              completedAt: UNFINISHED.includes(status) ? null : at(ordinal + 1),
              checkpointId: null,
              contextHandoffId: null,
              ...fields,
            },
          });
        const message = (
          ordinal: number,
          onRun: number,
          fields: Partial<OrchestrationV2ConversationMessage> = {},
        ) =>
          events.push({
            type: "message.updated",
            threadId,
            runId: runId(onRun),
            occurredAt: start,
            payload: {
              id: MessageId.make(`message:${name}:${ordinal}`),
              threadId,
              runId: runId(onRun),
              nodeId: null,
              role: "user",
              text: `Message ${ordinal}`,
              attachments: [],
              streaming: false,
              createdBy: "agent",
              creationSource: "mcp",
              createdAt: at(ordinal),
              updatedAt: at(ordinal),
              ...fields,
            },
          });
        events.push({
          type: "thread.created",
          threadId,
          occurredAt: start,
          payload: {
            createdBy: "agent",
            creationSource: "server",
            id: threadId,
            projectId: ProjectId.make(`project:${name}`),
            title: name,
            providerInstanceId: instanceId,
            modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: {
              parentThreadId: ThreadId.make(`thread:${name}:parent`),
              relationshipToParent: "subagent",
              rootThreadId: ThreadId.make(`thread:${name}:parent`),
            },
            forkedFrom: { type: "node", nodeId: originalTaskId },
            createdAt: start,
            updatedAt: start,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
        });
        // The original task's run, then the follow-up whose turn the promoted one joined.
        run(1, "completed");
        message(1, 1);
        let next = 4;
        if (shape === "an older queued run of its own") {
          // The task's own turn queued two messages to itself. Another task's run was moved ahead
          // of them, and the newer one was sent into that run.
          run(2, "completed", { delegatedTaskId: promotedTaskId });
          message(2, 2);
          run(3, "queued", { delegatedTaskId: promotedTaskId });
          message(3, 3);
          run(4, "cancelled", { delegatedTaskId: promotedTaskId, startedAt: null });
          message(4, 5, { promotedTaskId });
          run(5, "running", { delegatedTaskId: joinedTaskId });
          message(5, 5);
          next = 6;
        } else {
          run(2, shape === "joined run live" ? "running" : "completed", {
            delegatedTaskId: joinedTaskId,
            ...(shape === "joined run restarted" ? { status: "cancelled" } : {}),
          });
          message(2, 2);
          // The promoted follow-up: cancelled before it started, its message steered into run 2.
          run(3, "cancelled", { delegatedTaskId: promotedTaskId, startedAt: null });
          message(3, 2, { promotedTaskId });
        }
        if (shape === "joined run restarted") {
          run(next, "running", {
            delegatedTaskId: joinedTaskId,
            restartContinuationOfRunId: runId(2),
          });
          message(next, next);
          next += 1;
        }
        if (shape === "queued again") {
          // The steer missed its turn: the message queues again as the same follow-up.
          run(next, "queued", {
            delegatedTaskId: promotedTaskId,
            userMessageId: MessageId.make(`message:${name}:3`),
            steerFollowUpOfRunId: runId(2),
          });
          message(3, next);
          next += 1;
        }
        if (shape === "joined run's subagent holds")
          events.push({
            type: "subagent.updated",
            threadId,
            nodeId: NodeId.make(`node:${name}:subagent`),
            occurredAt: start,
            payload: {
              id: NodeId.make(`node:${name}:subagent`),
              threadId,
              runId: runId(2),
              parentNodeId: NodeId.make(`node:${name}:root`),
              origin: "app_owned",
              createdBy: "agent",
              driver,
              providerInstanceId: instanceId,
              providerThreadId: null,
              childThreadId: ThreadId.make(`thread:${name}:grandchild`),
              nativeTaskRef: null,
              prompt: "Sub-work",
              title: null,
              model: null,
              status: "running",
              result: null,
              startedAt: start,
              completedAt: null,
              updatedAt: start,
            },
          });
        // Monitor wakes since, which no task owns; queued behind a live run.
        for (const ordinal of Array.from({ length: 20 }, (_, index) => next + index)) {
          run(ordinal, shape === "an older queued run of its own" ? "queued" : "completed");
          message(ordinal, ordinal, {
            createdBy: "agent",
            creationSource: "server",
            notification: {
              source: { kind: "monitor" },
              outcome: "updated",
              summary: "Monitor output",
            },
          });
        }
        for (const [index, event] of events.entries()) {
          yield* store.apply({
            ...event,
            id: EventId.make(`event:${name}:${index}`),
          } as OrchestrationV2DomainEvent);
        }
        const full = yield* store.getThreadRecords(
          threadId,
          ["runs", "messages", "subagents", "providerThreads"],
          { messageRoles: ["user"] },
        );
        const fullDecision = (taskId: NodeId) =>
          taskDecisions(full, taskId, originalTaskId, undefined, new Set());
        // The promoted task works where its message went.
        assert.equal(
          fullDecision(promotedTaskId).childRun,
          {
            "joined run live": runId(2),
            "joined run's subagent holds": runId(2),
            "joined run restarted": runId(4),
            "queued again": runId(4),
            "an older queued run of its own": runId(5),
          }[shape],
        );
        assert.equal(
          fullDecision(promotedTaskId).awaitsSubagents,
          shape === "joined run's subagent holds",
        );
        let covered = 0;
        for (const runWindow of [1, 2, 3, 6, 16, 64]) {
          const read = yield* store.getDelegatedTaskRecords(threadId, runWindow);
          for (const taskId of [joinedTaskId, promotedTaskId]) {
            if (
              !delegatedTaskReadCovers(read, {
                originalTaskId,
                resultRun: undefined,
                stoppedResultRun: undefined,
                tasks: [{ id: taskId }],
              })
            )
              continue;
            if (taskId === promotedTaskId) covered += 1;
            assert.deepEqual(
              taskDecisions(read.records, taskId, originalTaskId, undefined, new Set()),
              fullDecision(taskId),
              `window ${runWindow}, task ${taskId}`,
            );
          }
        }
        assert.isAbove(covered, 0);
      }),
  );
});

import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  isOrchestrationV2WorkActive,
  latestProviderTurnForAttempt,
  MessageId,
  type OrchestrationV2Run,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import type {
  ProjectionRecordField,
  ProjectionRecordFilter,
  ProjectionRecords,
  ProjectionRuntimeRecoveryState,
} from "./ProjectionStore.ts";

import * as ServerSettings from "../serverSettings.ts";
import { isNativeMaintenanceCommand } from "./CommandPolicy.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import {
  type BackgroundWorkLossCause,
  awaitingProviderThreadId,
  cancelledRosterTaskWork,
  cancelledTurnItemWork,
  continuationFailedBeforePrompt,
  isAwaitedWork,
  isDelegatedSeat,
  isRestartContinuationSource,
  cancelledBeforeStart,
  isRestartNoteSource,
  restartCancelledBackgroundWorkNote,
  restartContinuationNote,
  restartContinuationSource,
  restartNoteOnly,
} from "./RestartBackgroundNote.ts";

const CONTINUE_PROMPT =
  "Continue where you left off. Your last turn was cut short, so a command or other action it started may have finished or may still be running. Before you repeat one that changes state, confirm it did not take effect and is not still running; if you cannot tell, say so instead of repeating it.";

const MAX_CONTINUATION_CHAIN = 3;
const CONTINUATION_LOOP_WINDOW_MS = 5 * 60 * 1_000;
/** The idle pin's bound: a seat idle longer would have lost its session, and its waiter, anyway. */
const SEAT_RESTART_WAKE_WINDOW_MS = 4 * 60 * 60 * 1_000;

export function restartContinuationBlocked(
  projection: Pick<
    ProjectionRuntimeRecoveryState,
    "thread" | "runtimeRequests" | "providerTurns" | "turnItems"
  >,
  source: OrchestrationV2Run,
  now: DateTime.Utc,
  settled: boolean,
): boolean {
  const { thread } = projection;
  if (
    thread.archivedAt !== null ||
    thread.deletedAt !== null ||
    thread.settledOverride === "settled" ||
    (thread.snoozedUntil != null && DateTime.isGreaterThan(thread.snoozedUntil, now))
  )
    return true;
  const turn = latestProviderTurnForAttempt(projection.providerTurns, source.activeAttemptId);
  if (
    !settled &&
    turn !== undefined &&
    (turn.status === "completed" || turn.status === "failed" || turn.status === "interrupted")
  )
    return true;
  const waitingRequest = projection.runtimeRequests.some(
    (request) =>
      request.kind !== "dynamic_tool_call" &&
      request.kind !== "auth_refresh" &&
      request.responseCapability.type !== "message" &&
      (request.status === "pending" ||
        ((request.status === "expired" || request.status === "cancelled") &&
          request.responseCapability.type === "not_resumable" &&
          turn !== undefined &&
          turn.id === request.providerTurnId)),
  );
  if (waitingRequest) return true;
  return projection.turnItems.some(
    (item) =>
      item.type === "secret_request" &&
      item.runId === source.id &&
      (item.secretStatus === "pending" ||
        (item.secretStatus === "cancelled" &&
          item.completedAt !== null &&
          source.completedAt !== null &&
          DateTime.Equivalence(item.completedAt, source.completedAt))),
  );
}

/** A Stop from the user, or from the parent that delegated the task, that a restart cut short. */
export const restartStopRequested = Effect.fn("RestartContinuation.restartStopRequested")(
  function* <E>(
    getThreadRecords: <K extends ProjectionRecordField>(
      threadId: ThreadId,
      fields: ReadonlyArray<K>,
      filter: ProjectionRecordFilter,
    ) => Effect.Effect<ProjectionRecords<K>, E>,
    thread: Pick<ProjectionRuntimeRecoveryState["thread"], "id" | "lineage" | "forkedFrom">,
    source: Pick<OrchestrationV2Run, "id" | "delegatedTaskId" | "startedAt" | "requestedAt">,
    // A seat's note continues the run its task settled from, so a parent's Stop requested since
    // still applies; the marker's start is the request, later updates request nothing.
    settledTask = false,
  ) {
    const { turnItems } = yield* getThreadRecords(thread.id, ["turnItems"], {
      turnItemRunIds: [source.id],
      turnItemTypes: ["run_interrupt_request"],
    });
    if (turnItems.some((item) => item.runId === source.id && item.type === "run_interrupt_request"))
      return true;
    const { lineage, forkedFrom } = thread;
    const taskId =
      source.delegatedTaskId ?? (forkedFrom?.type === "node" ? forkedFrom.nodeId : undefined);
    if (
      lineage.relationshipToParent !== "subagent" ||
      lineage.parentThreadId === null ||
      taskId === undefined
    )
      return false;
    const task = (yield* getThreadRecords(lineage.parentThreadId, ["subagents"], {
      subagentIds: [taskId],
    })).subagents.find((candidate) => candidate.id === taskId);
    // A settled task no longer owns later work in its thread.
    const open =
      task !== undefined && task.result === null && isOrchestrationV2WorkActive(task.status);
    if (task === undefined || task.runId === null || (!open && !settledTask)) return false;
    const stops = (yield* getThreadRecords(lineage.parentThreadId, ["turnItems"], {
      turnItemTypes: ["run_interrupt_request"],
      turnItemRunIds: [task.runId],
    })).turnItems;
    const since = source.startedAt ?? source.requestedAt;
    return open
      ? stops.length > 0
      : stops.some((item) =>
          DateTime.isGreaterThanOrEqualTo(item.startedAt ?? item.updatedAt, since),
        );
  },
);

/** App-owned delegations continue in their own threads and report back. */
function hasOpenWork(
  projection: Pick<ProjectionRuntimeRecoveryState, "runs" | "turnItems">,
  providerThread: ProjectionRuntimeRecoveryState["providerThreads"][number],
  awaited: boolean,
): boolean {
  const openItem = projection.turnItems.some((item) => {
    if (item.status !== "pending" && item.status !== "running" && item.status !== "waiting")
      return false;
    if (item.type === "subagent" && item.origin === "app_owned" && item.childThreadId !== null)
      return false;
    const work = cancelledTurnItemWork(item);
    return (
      work !== undefined &&
      isAwaitedWork(work) === awaited &&
      awaitingProviderThreadId(item, projection.runs) === providerThread.id
    );
  });
  return (
    openItem ||
    (providerThread.ownerNodeId === null &&
      (providerThread.pendingBackgroundTasks ?? []).some(
        (task) => isAwaitedWork(cancelledRosterTaskWork(task)) === awaited,
      ))
  );
}

type RestartProjection = Pick<
  ProjectionRuntimeRecoveryState,
  | "thread"
  | "runs"
  | "providerThreads"
  | "providerSessions"
  | "providerTurns"
  | "runtimeRequests"
  | "turnItems"
>;

/** Settled runs require a live session so stale crash records cannot wake them. */
export function restartContinuationRun(
  projection: RestartProjection,
  now: DateTime.Utc,
): OrchestrationV2Run | undefined {
  const source = liveRestartSource(projection, now);
  if (source === undefined) return;
  const { run, providerThread, settled, liveTurnRequired } = source;
  if (settled) return hasOpenWork(projection, providerThread, true) ? run : undefined;
  if (
    liveTurnRequired &&
    !projection.providerTurns.some(
      (turn) =>
        turn.providerThreadId === providerThread.id &&
        turn.runAttemptId === run.activeAttemptId &&
        turn.status === "running",
    )
  )
    return;
  return run;
}

/**
 * A delegated seat's settled run whose restart cut only unawaited work, such as its inbox waiter.
 * Its note wakes it while its parent still waits on the task, or when it ran within the window.
 * Stop is checked here, before startup settles the task and the handler stops seeing a parent's.
 */
export const restartSeatWakeRun = Effect.fn("RestartContinuation.restartSeatWakeRun")(function* <E>(
  getThreadRecords: <K extends ProjectionRecordField>(
    threadId: ThreadId,
    fields: ReadonlyArray<K>,
    filter: ProjectionRecordFilter,
  ) => Effect.Effect<ProjectionRecords<K>, E>,
  projection: RestartProjection,
  now: DateTime.Utc,
) {
  const { thread } = projection;
  const source = liveRestartSource(projection, now);
  if (
    source === undefined ||
    !source.settled ||
    hasOpenWork(projection, source.providerThread, true) ||
    !hasOpenWork(projection, source.providerThread, false) ||
    !isDelegatedSeat(thread)
  )
    return;
  const { run } = source;
  if (yield* restartStopRequested(getThreadRecords, thread, run, true)) return;
  const ranAt = run.completedAt ?? run.startedAt ?? run.requestedAt;
  if (DateTime.toEpochMillis(now) - DateTime.toEpochMillis(ranAt) <= SEAT_RESTART_WAKE_WINDOW_MS)
    return run;
  const taskId =
    run.delegatedTaskId ??
    (thread.forkedFrom?.type === "node" ? thread.forkedFrom.nodeId : undefined);
  if (taskId === undefined || thread.lineage.parentThreadId === null) return;
  const task = (yield* getThreadRecords(thread.lineage.parentThreadId, ["subagents"], {
    subagentIds: [taskId],
  })).subagents.find((candidate) => candidate.id === taskId);
  return task !== undefined &&
    task.origin === "app_owned" &&
    task.childThreadId === thread.id &&
    task.result === null &&
    isOrchestrationV2WorkActive(task.status)
    ? run
    : undefined;
});

function liveRestartSource(projection: RestartProjection, now: DateTime.Utc) {
  const run = restartContinuationSource(projection.runs);
  if (!run) return;
  const preparedContinuation =
    run.status === "starting" && run.restartContinuationOfRunId !== undefined;
  const settled = run.status === "completed" || run.status === "waiting";
  if (run.status !== "running" && !preparedContinuation && !settled) return;
  if (restartContinuationBlocked(projection, run, now, settled)) return;
  const liveTurnRequired = !preparedContinuation && !settled;
  if (projection.thread.providerInstanceId !== run.providerInstanceId) return;
  const providerThread = projection.providerThreads.find(
    (thread) => thread.id === run.providerThreadId,
  );
  if (
    !providerThread ||
    providerThread.appThreadId !== projection.thread.id ||
    providerThread.ownerNodeId !== null ||
    providerThread.providerInstanceId !== run.providerInstanceId ||
    providerThread.nativeThreadRef?.nativeId == null ||
    providerThread.nativeThreadRef.strength !== "strong" ||
    providerThread.nativeThreadRef.driver !== providerThread.driver ||
    (liveTurnRequired && providerThread.status !== "active") ||
    providerThread.status === "closed" ||
    providerThread.status === "archived"
  )
    return;
  const session = projection.providerSessions.find(
    (candidate) => candidate.id === providerThread.providerSessionId,
  );
  // Adapters remain ready during turns; only stopped or failed sessions rule out liveness.
  if (
    session === undefined ||
    session.providerInstanceId !== run.providerInstanceId ||
    session.driver !== providerThread.driver ||
    (!preparedContinuation && (session.status === "stopped" || session.status === "error"))
  )
    return;
  return { run, providerThread, settled, liveTurnRequired };
}

export const continueRestartedRun = Effect.fn("RestartContinuation.continueRestartedRun")(
  function* (input: {
    readonly threadId: ThreadId;
    readonly sourceRunId: RunId;
    readonly processLossCount?: number;
    readonly cause?: BackgroundWorkLossCause;
  }) {
    const settings = yield* ServerSettings.ServerSettingsService;
    const enabled = yield* settings.getSettings.pipe(Effect.orElseSucceed(() => null));
    if (!enabled) return;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const messageId = MessageId.make(`message:restart-continuation:${input.sourceRunId}`);
    const projection = yield* threads.getThreadRecords(
      input.threadId,
      ["messages", "runs", "providerTurns", "attempts", "runtimeRequests", "turnItems"],
      { messageIds: [messageId], turnItemTypes: ["secret_request"] },
    );
    if (
      !resolveProjectSettings(enabled, projection.thread.projectId).settings
        .continueThreadsAfterServerUpdate
    )
      return;
    if (projection.messages.some((message) => message.id === messageId)) return;
    const source = projection.runs.find((run) => run.id === input.sourceRunId);
    if (
      !source ||
      !(
        isRestartContinuationSource(source, projection.providerTurns, projection.thread) ||
        continuationFailedBeforePrompt(source, projection.providerTurns)
      ) ||
      restartContinuationBlocked(
        projection,
        source,
        yield* DateTime.now,
        isRestartNoteSource(source, projection.providerTurns),
      )
    )
      return;
    if (
      projection.runs.some(
        (run) =>
          run.id !== source.id &&
          run.status !== "queued" &&
          !cancelledBeforeStart(run) &&
          runRanAfter(run, source),
      )
    )
      return;
    if (projection.thread.providerInstanceId !== source.providerInstanceId) return;
    // A turn that keeps crashing the server would otherwise restart it forever.
    let chainLength = 0;
    const loopWindowStart =
      DateTime.toEpochMillis(source.completedAt ?? source.requestedAt) -
      CONTINUATION_LOOP_WINDOW_MS;
    for (
      let run: OrchestrationV2Run | undefined = source;
      run?.restartContinuationOfRunId !== undefined && chainLength < MAX_CONTINUATION_CHAIN;
      run = projection.runs.find((candidate) => candidate.id === run?.restartContinuationOfRunId)
    ) {
      if (DateTime.toEpochMillis(run.requestedAt) < loopWindowStart) break;
      chainLength += 1;
    }
    if (
      input.processLossCount === undefined
        ? chainLength >= MAX_CONTINUATION_CHAIN
        : input.processLossCount > MAX_CONTINUATION_CHAIN
    ) {
      yield* Effect.logWarning("Not continuing a run restarted too many times in a row", {
        threadId: input.threadId,
        sourceRunId: input.sourceRunId,
      });
      return;
    }
    if (
      yield* restartStopRequested(
        threads.getThreadRecords,
        projection.thread,
        source,
        isDelegatedSeat(projection.thread) && restartNoteOnly(source, projection.providerTurns),
      )
    )
      return;
    const sourceMessage = (yield* threads.getThreadRecords(input.threadId, ["messages"], {
      messageIds: [source.userMessageId],
    })).messages.find((message) => message.id === source.userMessageId);
    if (sourceMessage !== undefined && isNativeMaintenanceCommand(sourceMessage)) return;
    const note = restartContinuationNote(
      source,
      projection.runs,
      projection.providerTurns,
      projection.attempts,
    );
    const noteText =
      note.work.length === 0
        ? undefined
        : restartCancelledBackgroundWorkNote(note.work, input.cause);
    const text =
      noteText === undefined
        ? CONTINUE_PROMPT
        : note.settled
          ? noteText
          : `${noteText}\n\n${CONTINUE_PROMPT}`;
    yield* threads
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`command:restart-continuation:${input.sourceRunId}`),
        threadId: input.threadId,
        messageId,
        text,
        notification: {
          source: { kind: "system" },
          outcome: "updated",
          summary:
            input.cause === "provider_exit"
              ? "The agent runtime exited and stopped background work"
              : note.settled
                ? "T3 Code restarted and stopped background work"
                : "T3 Code restarted and resumed this turn",
          detail: text,
        },
        attachments: [],
        modelSelection: source.modelSelection,
        dispatchMode: { type: "start_immediately" },
        createdBy: "agent",
        creationSource: "server",
        restartContinuationOfRunId: input.sourceRunId,
      })
      .pipe(
        // Disabled instances, and threads above a policy ceiling, cannot resume; still run success
        // cleanup so delegated children recover without futile retries.
        Effect.catchTags({
          ThreadManagementProviderInstanceDisabledError: (error) =>
            Effect.logInfo("orchestration-v2.restart-continuation.instance-disabled", {
              threadId: input.threadId,
              sourceRunId: input.sourceRunId,
              projectId: error.projectId,
              instanceId: error.instanceId,
            }),
          OrchestratorRuntimeModeAboveCeilingError: () =>
            Effect.logInfo("orchestration-v2.restart-continuation.above-ceiling", {
              threadId: input.threadId,
              sourceRunId: input.sourceRunId,
            }),
        }),
      );
  },
  // Declined child continuations still owe parent results; existing continuation runs settle them instead.
  (effect, input) =>
    effect.pipe(
      Effect.andThen(
        Effect.gen(function* () {
          const threads = yield* ThreadManagementService.ThreadManagementService;
          yield* threads.recoverDelegatedTask(input.threadId, { sourceRunId: input.sourceRunId });
        }),
      ),
    ),
);

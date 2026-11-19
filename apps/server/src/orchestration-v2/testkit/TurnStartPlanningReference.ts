/**
 * The turn start's planning as it was before it was made linear, kept
 * as the reference the differential row compares against. Quadratic on long histories: rows only.
 */
import type {
  OrchestrationV2ProviderTurn,
  OrchestrationV2Run,
  OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import { runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";

import { mergeRestartCancelledBackgroundWork } from "../RestartBackgroundNote.ts";

type Work = NonNullable<OrchestrationV2Run["restartCancelledBackgroundWork"]>[number];
type Attempt = Pick<OrchestrationV2RunAttempt, "id" | "runId">;
type ProviderTurnState = Pick<
  OrchestrationV2ProviderTurn,
  "runAttemptId" | "providerThreadId" | "status"
>;

const NON_COMPLETING_TERMINAL_RUN_STATUSES: ReadonlySet<OrchestrationV2Run["status"]> = new Set([
  "failed",
  "interrupted",
  "cancelled",
  "rolled_back",
]);

export function referenceAbandonedProviderTurns(
  projection: {
    readonly runs: ReadonlyArray<OrchestrationV2Run>;
    readonly attempts: ReadonlyArray<Attempt>;
    readonly providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
  },
  run: Pick<OrchestrationV2Run, "ordinal">,
  providerThreadId: OrchestrationV2Run["providerThreadId"],
) {
  const abandonedRuns = new Map<string | null, OrchestrationV2Run>();
  for (const previous of projection.runs) {
    if (
      previous.ordinal >= run.ordinal ||
      previous.providerThreadId !== providerThreadId ||
      !NON_COMPLETING_TERMINAL_RUN_STATUSES.has(previous.status)
    ) {
      continue;
    }
    if (previous.activeAttemptId !== null) {
      abandonedRuns.set(previous.activeAttemptId, previous);
    }
    for (const attempt of projection.attempts) {
      if (attempt.runId === previous.id) abandonedRuns.set(attempt.id, previous);
    }
  }
  return projection.providerTurns.flatMap((turn) => {
    const previous = abandonedRuns.get(turn.runAttemptId);
    return turn.providerThreadId === providerThreadId &&
      turn.status === "running" &&
      previous !== undefined
      ? [{ turn, run: previous }]
      : [];
  });
}

function isRestartNoteSource(
  source: Pick<OrchestrationV2Run, "status" | "activeAttemptId" | "restartCancelledBackgroundWork">,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "status">>,
): boolean {
  return (
    (source.status === "completed" ||
      source.status === "waiting" ||
      source.status === "cancelled") &&
    (source.restartCancelledBackgroundWork?.length ?? 0) > 0 &&
    !providerTurns.some(
      (turn) => turn.runAttemptId === source.activeAttemptId && turn.status === "cancelled",
    )
  );
}

export function referenceRestartContinuationNote(
  source: OrchestrationV2Run,
  runs: ReadonlyArray<OrchestrationV2Run>,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "status">>,
  attempts: ReadonlyArray<Attempt>,
): { readonly work: ReadonlyArray<Work>; readonly settled: boolean } {
  const completedAttempts = new Set(
    providerTurns.filter((turn) => turn.status === "completed").map((turn) => turn.runAttemptId),
  );
  const completedRuns = new Set(
    attempts.filter((attempt) => completedAttempts.has(attempt.id)).map((attempt) => attempt.runId),
  );
  let current = source;
  let work = current.restartCancelledBackgroundWork ?? [];
  const visited = new Set([current.id]);
  while (
    current.restartContinuationOfRunId !== undefined &&
    !completedRuns.has(current.id) &&
    !(current.activeAttemptId !== null && completedAttempts.has(current.activeAttemptId))
  ) {
    const previous = runs.find((candidate) => candidate.id === current.restartContinuationOfRunId);
    if (previous === undefined || visited.has(previous.id)) break;
    visited.add(previous.id);
    current = previous;
    work = mergeRestartCancelledBackgroundWork(current.restartCancelledBackgroundWork ?? [], work);
  }
  return { work, settled: isRestartNoteSource(current, providerTurns) };
}

export function referenceIsRestartNoteContinuation(
  run: Pick<OrchestrationV2Run, "restartContinuationOfRunId">,
  runs: ReadonlyArray<OrchestrationV2Run>,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "status">>,
  attempts: ReadonlyArray<Attempt>,
): boolean {
  const source =
    run.restartContinuationOfRunId === undefined
      ? undefined
      : runs.find((candidate) => candidate.id === run.restartContinuationOfRunId);
  return (
    source !== undefined &&
    referenceRestartContinuationNote(source, runs, providerTurns, attempts).work.length > 0
  );
}

interface RestartNoteInput {
  readonly runs: ReadonlyArray<OrchestrationV2Run>;
  readonly providerTurns: ReadonlyArray<ProviderTurnState>;
  readonly run: OrchestrationV2Run;
  readonly attempts: ReadonlyArray<Attempt>;
}

function restartNoteRuns(
  input: RestartNoteInput,
  providerThreadId: NonNullable<OrchestrationV2Run["providerThreadId"]>,
) {
  const deliveredAttemptIds = new Set(
    input.providerTurns
      .filter((turn) => turn.providerThreadId === providerThreadId && turn.status === "completed")
      .map((turn) => turn.runAttemptId),
  );
  const sameThread = input.runs.filter((run) => run.providerThreadId === providerThreadId);
  const promptedUnlessCompaction = (candidate: OrchestrationV2Run) =>
    candidate.id !== input.run.id &&
    candidate.activeAttemptId !== null &&
    candidate.status !== "rolled_back" &&
    (deliveredAttemptIds.has(candidate.activeAttemptId) ||
      input.attempts.some(
        (attempt) => attempt.runId === candidate.id && deliveredAttemptIds.has(attempt.id),
      )) &&
    (candidate.restartContinuationOfRunId === undefined ||
      referenceIsRestartNoteContinuation(
        candidate,
        input.runs,
        input.providerTurns,
        input.attempts,
      ));
  return { deliveredAttemptIds, sameThread, promptedUnlessCompaction };
}

export function referencePendingRestartCancelledBackgroundWork(
  input: RestartNoteInput & { readonly compactionMessageIds: ReadonlySet<string> },
): ReadonlyArray<Work> {
  const isCompaction = (run: typeof input.run) => input.compactionMessageIds.has(run.userMessageId);
  if (
    input.run.providerThreadId === null ||
    input.run.restartContinuationOfRunId !== undefined ||
    isCompaction(input.run)
  )
    return [];
  const { deliveredAttemptIds, sameThread, promptedUnlessCompaction } = restartNoteRuns(
    input,
    input.run.providerThreadId,
  );
  const prompted = sameThread.filter(
    (candidate) => promptedUnlessCompaction(candidate) && !isCompaction(candidate),
  );
  const alreadyDelivered = input.attempts.some(
    (attempt) =>
      attempt.runId === input.run.id &&
      attempt.id !== input.run.activeAttemptId &&
      deliveredAttemptIds.has(attempt.id),
  );
  if (alreadyDelivered) return [];
  return sameThread
    .filter(
      (source) =>
        runRanAfter(input.run, source) &&
        (source.restartCancelledBackgroundWork?.length ?? 0) > 0 &&
        !prompted.some((later) => runRanAfter(later, source)),
    )
    .reduce<ReadonlyArray<Work>>(
      (work, source) =>
        mergeRestartCancelledBackgroundWork(work, source.restartCancelledBackgroundWork ?? []),
      [],
    );
}

export function referenceRestartNoteCompactionCandidates(
  input: RestartNoteInput,
): ReadonlyArray<OrchestrationV2Run> {
  if (input.run.providerThreadId === null || input.run.restartContinuationOfRunId !== undefined)
    return [];
  const { sameThread, promptedUnlessCompaction } = restartNoteRuns(
    input,
    input.run.providerThreadId,
  );
  if (
    !sameThread.some(
      (source) =>
        runRanAfter(input.run, source) && (source.restartCancelledBackgroundWork?.length ?? 0) > 0,
    )
  )
    return [];
  return sameThread
    .filter(promptedUnlessCompaction)
    .toSorted((left, right) => (runRanAfter(left, right) ? -1 : runRanAfter(right, left) ? 1 : 0));
}

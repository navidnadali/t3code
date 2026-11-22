import {
  isDeliveredProviderTurn,
  type NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2PendingBackgroundTask,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RestartCancelledBackgroundWork,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { isProviderNativeSubagentThread } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { subagentHoldsCompletion } from "@t3tools/provider-core/server/subagentProjection";
import { backgroundWorkHoldsCompletion } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import { latestRootProviderFailure, runRanAfter } from "@t3tools/shared/orchestrationV2ThreadError";

type Work = OrchestrationV2RestartCancelledBackgroundWork;
type Attempt = Pick<OrchestrationV2RunAttempt, "id" | "runId">;

const MAX_LABEL_LENGTH = 160;

function compactLabel(value: unknown, maxLength = MAX_LABEL_LENGTH): string | undefined {
  if (typeof value !== "string") return undefined;
  const text = value.replaceAll(/\s+/g, " ").trim();
  if (text.length === 0) return undefined;
  if (text.length <= maxLength) return text;
  // Half an emoji serializes to a lone surrogate escape, which provider JSON parsers reject.
  const last = text.charCodeAt(maxLength - 2);
  const end = last >= 0xd800 && last <= 0xdbff ? maxLength - 2 : maxLength - 1;
  return `${text.slice(0, end)}…`;
}

/** A bounded label that keeps its id suffix whole whenever it fits. */
function labelWithId(name: unknown, fallback: string, suffix: string): string {
  const budget = MAX_LABEL_LENGTH - suffix.length;
  if (budget < 2) return compactLabel(`${fallback}${suffix}`) ?? fallback;
  return `${compactLabel(name, budget) ?? compactLabel(fallback, budget)}${suffix}`;
}

/** resumeId overrides the item id for native resume, such as Codex's child provider-thread id. */
export function cancelledTurnItemWork(
  item: OrchestrationV2TurnItem,
  resumeId?: string,
): Work | undefined {
  switch (item.type) {
    case "subagent": {
      const nativeId =
        item.origin === "provider_native" ? (resumeId ?? item.nativeItemRef?.nativeId) : undefined;
      return {
        kind: "subagent",
        label: labelWithId(
          compactLabel(item.title) === undefined ? item.prompt : item.title,
          "subagent",
          nativeId == null
            ? ""
            : item.nativeItemRef?.driver === "claudeAgent"
              ? ` (id ${nativeId}; resume it with SendMessage to '${nativeId}')`
              : ` (id ${nativeId})`,
        ),
        id: item.id,
      };
    }
    case "command_execution":
      return {
        kind: item.monitor === true ? "monitor" : "shell",
        label: compactLabel(item.input) ?? compactLabel(item.title) ?? "background command",
        id: item.id,
        ...(item.completionWake === "always" ? { awaited: true as const } : {}),
      };
    case "dynamic_tool": {
      const monitor =
        item.input !== null &&
        typeof item.input === "object" &&
        Reflect.get(item.input, "persistent") === true;
      return {
        kind: monitor ? "monitor" : "task",
        label: compactLabel(item.title) ?? compactLabel(item.toolName) ?? "background tool",
        id: item.id,
      };
    }
    default:
      return undefined;
  }
}

/** Returns the awaiting provider thread; native subagents can run on children while their launching turn awaits them. */
export function awaitingProviderThreadId(
  item: OrchestrationV2TurnItem,
  runs: ReadonlyArray<Pick<OrchestrationV2Run, "id" | "providerThreadId">>,
): OrchestrationV2Run["providerThreadId"] | undefined {
  const runProviderThreadId = runs.find((run) => run.id === item.runId)?.providerThreadId;
  return item.type === "subagent" && item.origin === "provider_native"
    ? (runProviderThreadId ?? item.providerThreadId)
    : (item.providerThreadId ?? runProviderThreadId);
}

function restartWorkKind(task: OrchestrationV2PendingBackgroundTask): Work["kind"] {
  switch (task.kind) {
    case "command":
      return "shell";
    case "subagent":
      return "subagent";
    case "monitor":
      return "monitor";
    case "background_task":
      return "task";
    default:
      task satisfies never;
      return "task";
  }
}

export function cancelledRosterTaskWork(task: OrchestrationV2PendingBackgroundTask): Work {
  if (task.parentTaskId !== undefined) {
    return {
      kind: "subagent",
      label: labelWithId(task.description, "background task", ` (subagent ${task.parentTaskId})`),
      id: task.parentTaskId,
    };
  }
  const kind = restartWorkKind(task);
  const description = compactLabel(task.description);
  return {
    kind,
    label:
      compactLabel(
        description === undefined ? task.taskId : `${description} (id ${task.taskId})`,
      ) ?? "background task",
    id: task.taskId,
    ...(task.awaited === true ? { awaited: true as const } : {}),
  };
}

const MAX_NOTE_ENTRIES = 10;

/** Why background work stopped: a T3 server restart, or the provider process exiting under a live server. */
export type BackgroundWorkLossCause = "server_restart" | "provider_exit";

/** Bounds entries and labels to protect the context budget. */
export function restartCancelledBackgroundWorkNote(
  work: ReadonlyArray<Work>,
  cause: BackgroundWorkLossCause = "server_restart",
): string {
  const omitted = work.length - MAX_NOTE_ENTRIES;
  return [
    `Note: ${cause === "provider_exit" ? "the agent runtime process exited" : "the T3 server restarted"}, and this background work was cancelled before it finished. It will not report back on its own, so resume or restart what you still need and wait for its result:`,
    ...work.slice(0, MAX_NOTE_ENTRIES).map((entry) => `- ${entry.kind}: ${entry.label}`),
    ...(omitted > 0 ? [`- and ${omitted} more`] : []),
  ].join("\n");
}

type ProviderTurnState = Pick<
  OrchestrationV2ProviderTurn,
  "runAttemptId" | "providerThreadId" | "status" | "promptDropped"
>;

export function isRestartNoteSource(
  source: Pick<OrchestrationV2Run, "status" | "activeAttemptId" | "restartCancelledBackgroundWork">,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "status">>,
): boolean {
  return isNoteSource(source, (attemptId) =>
    providerTurns.some((turn) => turn.runAttemptId === attemptId && turn.status === "cancelled"),
  );
}

function isNoteSource(
  source: Pick<OrchestrationV2Run, "status" | "activeAttemptId" | "restartCancelledBackgroundWork">,
  attemptCancelled: (attemptId: OrchestrationV2Run["activeAttemptId"]) => boolean,
): boolean {
  return (
    // Failed and interrupted turns never continue; recovery-cancelled waiting runs still qualify.
    (source.status === "completed" ||
      source.status === "waiting" ||
      source.status === "cancelled") &&
    (source.restartCancelledBackgroundWork?.length ?? 0) > 0 &&
    !attemptCancelled(source.activeAttemptId)
  );
}

/**
 * Matches the Working-section hold: a shell or monitor wakes it only when the agent said it
 * waits on it. Most are dev servers and their watchdogs, so their note waits for the next
 * user message.
 */
export function isAwaitedWork(work: Pick<Work, "kind" | "awaited">): boolean {
  return work.awaited === true || (work.kind !== "shell" && work.kind !== "monitor");
}

/**
 * A delegate_task child. Its parent reaches it through waiters it leaves running, unawaited, so
 * a restart that cuts them wakes it with the note instead of waiting for a message.
 */
export function isDelegatedSeat(
  thread: Pick<OrchestrationV2AppThread, "lineage" | "forkedFrom" | "creationSource">,
): boolean {
  return (
    thread.lineage.relationshipToParent === "subagent" &&
    thread.lineage.parentThreadId !== null &&
    thread.forkedFrom?.type === "node" &&
    !isProviderNativeSubagentThread(thread)
  );
}

export function runOutcome(
  run: Pick<OrchestrationV2Run, "status" | "restartCancelledBackgroundWork">,
): OrchestrationV2Run["status"] {
  return (run.status === "completed" || run.status === "cancelled") &&
    (run.restartCancelledBackgroundWork ?? []).some(isAwaitedWork)
    ? "interrupted"
    : run.status;
}

/** The run whose continuation carries a provider thread's lost background work. */
export function latestStartedRun(
  runs: ReadonlyArray<OrchestrationV2Run>,
  providerThreadId: OrchestrationV2Run["providerThreadId"],
): OrchestrationV2Run | undefined {
  return runs.reduce<OrchestrationV2Run | undefined>(
    (latest, run) =>
      run.providerThreadId === providerThreadId &&
      run.status !== "queued" &&
      run.status !== "rolled_back" &&
      !neverStarted(run) &&
      (latest === undefined || runRanAfter(run, latest))
        ? run
        : latest,
    undefined,
  );
}

/** Unstarted runs have no lost work to continue, except prepared continuations carrying a source run. */
export function neverStarted(
  run: Pick<OrchestrationV2Run, "startedAt" | "restartContinuationOfRunId">,
): boolean {
  return run.startedAt === null && run.restartContinuationOfRunId === undefined;
}

/** The run a restart continuation resumes: the thread's latest run that started. */
export function restartContinuationSource<Run extends OrchestrationV2Run>(
  runs: ReadonlyArray<Run>,
): Run | undefined {
  return runs.reduce<Run | undefined>(
    (latest, candidate) =>
      candidate.status !== "queued" &&
      !neverStarted(candidate) &&
      (!latest || runRanAfter(candidate, latest))
        ? candidate
        : latest,
    undefined,
  );
}

/** Restart-cancelled unstarted runs supersede nothing; user-stopped unstarted runs do. */
export function cancelledBeforeStart(
  run: Pick<OrchestrationV2Run, "status" | "startedAt" | "restartContinuationOfRunId">,
): boolean {
  return run.status === "cancelled" && neverStarted(run);
}

/**
 * The runs a follow-up promoted to Steer works in: each run holding a message one of its queued
 * runs was sent into a running turn as, and that run's restart continuations. The mark is on the
 * message, which every read of the run holds, so a windowed read decides as the full read does.
 */
export function delegatedTaskJoinedRuns(
  projection: Parameters<typeof delegatedTaskRuns>[0],
  taskId: NodeId,
): ReadonlyArray<OrchestrationV2Run> {
  const joinedRunIds = new Set<string | null>(
    projection.messages.flatMap((message) =>
      message.promotedTaskId === taskId ? [message.runId] : [],
    ),
  );
  if (joinedRunIds.size === 0) return [];
  const chain = projection.runs.filter((run) => joinedRunIds.has(run.id));
  for (let index = 0; index < chain.length; index += 1) {
    for (const run of projection.runs) {
      if (run.restartContinuationOfRunId === chain[index]!.id && !chain.includes(run))
        chain.push(run);
    }
  }
  return chain;
}

/**
 * A follow-up task's runs: those tagged with it, the runs its promoted message joined, and the
 * untagged wakes for subagents they started. A merged wake carries several parent runs' results,
 * all for the same tasks; any of them links it.
 */
export function delegatedTaskRuns(
  projection: {
    readonly runs: ReadonlyArray<OrchestrationV2Run>;
    readonly messages: ReadonlyArray<
      Pick<OrchestrationV2ConversationMessage, "runId" | "delegatedCompletion" | "promotedTaskId">
    >;
  },
  taskId: NodeId,
): ReadonlyArray<OrchestrationV2Run> {
  const owned = new Set(
    projection.runs.filter((run) => run.delegatedTaskId === taskId).map((run) => run.id),
  );
  for (const run of delegatedTaskJoinedRuns(projection, taskId)) owned.add(run.id);
  const untagged = new Set(
    projection.runs.filter((run) => run.delegatedTaskId === undefined).map((run) => run.id),
  );
  for (let grew = owned.size > 0; grew;) {
    grew = false;
    for (const { runId, delegatedCompletion } of projection.messages) {
      if (
        runId === null ||
        !untagged.has(runId) ||
        owned.has(runId) ||
        delegatedCompletion === undefined ||
        !(
          owned.has(delegatedCompletion.parentRunId) ||
          (delegatedCompletion.merged ?? []).some((part) => owned.has(part.parentRunId))
        )
      )
        continue;
      owned.add(runId);
      grew = true;
    }
  }
  return projection.runs.filter((run) => owned.has(run.id));
}

/**
 * A follow-up task's runs still owe a subagent they started its result, or still await
 * background work they started, as the original task waits on both.
 */
export function delegatedTaskAwaitsWork(
  runs: ReadonlyArray<OrchestrationV2Run>,
  child: {
    readonly subagents: ReadonlyArray<
      Pick<OrchestrationV2Subagent, "runId" | "status" | "completionDelivery">
    >;
    readonly providerThreads: ReadonlyArray<
      Pick<OrchestrationV2ProviderThread, "pendingBackgroundTasks">
    >;
  },
): boolean {
  const runIds = new Set<string | null>(runs.map((run) => run.id));
  return (
    child.subagents.some((task) => runIds.has(task.runId) && subagentHoldsCompletion(task)) ||
    child.providerThreads.some((thread) =>
      backgroundWorkHoldsCompletion(
        (thread.pendingBackgroundTasks ?? []).filter(
          (task) => task.runId !== undefined && runIds.has(task.runId),
        ),
      ),
    )
  );
}

/** A follow-up task's latest run, passing over queued work it no longer waits on, as the original task does. */
export function delegatedTaskResultRun(
  runs: ReadonlyArray<OrchestrationV2Run>,
): OrchestrationV2Run | undefined {
  const taskRuns = runs.toSorted((a, b) => b.ordinal - a.ordinal);
  const awaited = taskRuns.filter(
    (run) => !cancelledBeforeStart(run) && !(run.status === "queued" && run.queueHeld === true),
  );
  // Executing work reports the task's state; a reordered queue can finish a newer run before an older one.
  const executing = awaited.find((run) =>
    ["preparing", "starting", "running", "waiting"].includes(run.status),
  );
  return executing ?? awaited.find((run) => run.status === "queued") ?? awaited[0] ?? taskRuns[0];
}

/** A task's runs and the run its result comes from, as finalizing the task reads them. */
export function delegatedTaskChildRun(
  child: Parameters<typeof delegatedTaskRuns>[0],
  taskId: NodeId,
  originalTaskId: NodeId,
  resultRun: OrchestrationV2Run | undefined,
): {
  readonly taskRuns: ReadonlyArray<OrchestrationV2Run>;
  readonly childRun: OrchestrationV2Run | undefined;
} {
  const taskRuns = delegatedTaskRuns(child, taskId);
  const childRun =
    taskRuns.length === 0
      ? taskId === originalTaskId
        ? resultRun
        : undefined
      : delegatedTaskResultRun(taskRuns);
  return { taskRuns, childRun };
}

const UNFINISHED_RUN_STATUSES: ReadonlySet<OrchestrationV2Run["status"]> = new Set([
  "queued",
  "preparing",
  "starting",
  "running",
  "waiting",
]);

/**
 * Whether a windowed read of a delegated child holds every run finalizing it decides on (see
 * `ProjectionStore.getDelegatedTaskRecords`). A task's result run is an awaited unfinished run,
 * which the read always holds, or its newest awaited run. A finished result run is checked for a
 * restart continuation against every run that ended after it; the read holds those that await one.
 */
export function delegatedTaskReadCovers(
  read: {
    readonly records: Parameters<typeof delegatedTaskRuns>[0];
    readonly ordinalFloor: number | null;
    readonly endedFloor: OrchestrationV2Run["id"] | null;
  },
  input: {
    readonly tasks: ReadonlyArray<Pick<OrchestrationV2Subagent, "id">>;
    readonly originalTaskId: NodeId;
    readonly resultRun: OrchestrationV2Run | undefined;
    readonly stoppedResultRun: OrchestrationV2Run | undefined;
  },
): boolean {
  const { records, ordinalFloor, endedFloor } = read;
  if (ordinalFloor === null) return true;
  const floor = records.runs.find((run) => run.id === endedFloor);
  const endedInRead = (run: OrchestrationV2Run) =>
    endedFloor === null ||
    (floor !== undefined && (run.id === floor.id || runRanAfter(run, floor)));
  for (const task of input.tasks) {
    const { taskRuns, childRun } = delegatedTaskChildRun(
      records,
      task.id,
      input.originalTaskId,
      input.resultRun,
    );
    if (
      !taskRuns.some(
        (run) =>
          !cancelledBeforeStart(run) &&
          !(run.status === "queued" && run.queueHeld === true) &&
          (UNFINISHED_RUN_STATUSES.has(run.status) || run.ordinal >= ordinalFloor),
      )
    )
      return false;
    if (
      childRun !== undefined &&
      !UNFINISHED_RUN_STATUSES.has(childRun.status) &&
      !endedInRead(childRun)
    )
      return false;
  }
  return input.stoppedResultRun === undefined || endedInRead(input.stoppedResultRun);
}

/** The provider never accepted this continuation's prompt, so starting it again repeats nothing. */
export function continuationFailedBeforePrompt(
  run: Pick<OrchestrationV2Run, "status" | "activeAttemptId" | "restartContinuationOfRunId">,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "promptDropped">>,
): boolean {
  return (
    run.status === "failed" &&
    run.restartContinuationOfRunId !== undefined &&
    !providerTurns.some(
      (turn) => turn.runAttemptId === run.activeAttemptId && isDeliveredProviderTurn(turn),
    )
  );
}

const CONTINUATION_START_RETRY_DELAYS_MS = [60_000, 180_000];

/** Backoff before restarting a continuation that failed to start, or undefined to let it fail. */
export function continuationStartRetryDelayMs(
  run: OrchestrationV2Run,
  projection: {
    readonly runs: ReadonlyArray<OrchestrationV2Run>;
    readonly providerTurns: ReadonlyArray<
      Pick<ProviderTurnState, "runAttemptId" | "promptDropped">
    >;
    readonly turnItems: Parameters<typeof latestRootProviderFailure>[1];
  },
): number | undefined {
  if (!continuationFailedBeforePrompt(run, projection.providerTurns)) return;
  // These fail the same way however often they start.
  const failureClass = latestRootProviderFailure(run, projection.turnItems)?.class;
  if (
    failureClass === "usage_limit" ||
    failureClass === "permission_error" ||
    failureClass === "validation_error"
  )
    return;
  let failedStarts = 0;
  for (
    let current: OrchestrationV2Run | undefined = run;
    current !== undefined && continuationFailedBeforePrompt(current, projection.providerTurns);
    current = projection.runs.find(
      (candidate) => candidate.id === current?.restartContinuationOfRunId,
    )
  )
    failedStarts += 1;
  return CONTINUATION_START_RETRY_DELAYS_MS[failedStarts - 1];
}

/**
 * Its restart cut only work nobody awaited, so its note wakes a seat but leaves its outcome.
 * Recovery records lost work only on a started run, so the run has an attempt to read turns by.
 */
export function restartNoteOnly(
  source: Pick<OrchestrationV2Run, "status" | "activeAttemptId" | "restartCancelledBackgroundWork">,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "status">>,
): boolean {
  return (
    source.activeAttemptId !== null &&
    isRestartNoteSource(source, providerTurns) &&
    !isRestartContinuationSource(source, providerTurns)
  );
}

export function isRestartContinuationSource(
  source: Pick<OrchestrationV2Run, "status" | "activeAttemptId" | "restartCancelledBackgroundWork">,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "status">>,
  thread?: Parameters<typeof isDelegatedSeat>[0],
): boolean {
  return isRestartNoteSource(source, providerTurns)
    ? (source.restartCancelledBackgroundWork ?? []).some(isAwaitedWork) ||
        (thread !== undefined && isDelegatedSeat(thread))
    : source.status === "cancelled";
}

type Note = { readonly work: ReadonlyArray<Work>; readonly settled: boolean };

/**
 * The lookups a turn start's planning shares over one projection, in projection order (a run id
 * keeps its first run, as `find` did), each built in one pass on use. A start often asks about one
 * attempt's turns or one run's attempts only, so the first such question scans: comparing ids is
 * cheaper than hashing every one.
 */
export function turnStartIndex(
  runs: ReadonlyArray<OrchestrationV2Run>,
  providerTurns: ReadonlyArray<
    Pick<ProviderTurnState, "runAttemptId" | "status"> & {
      readonly providerThreadId?: ProviderTurnState["providerThreadId"];
    }
  >,
  attempts: ReadonlyArray<Attempt>,
) {
  type Turns = { completed: boolean; cancelled: boolean; completedOn: Array<string> };
  let runsById: Map<string, OrchestrationV2Run> | undefined;
  let attemptsByRun: Map<string, Array<Attempt>> | undefined;
  let turnsByAttempt: Map<string | null, Turns> | undefined;
  const none: ReadonlyArray<Attempt> = [];
  const add = (of: Turns, turn: (typeof providerTurns)[number]) => {
    if (turn.status === "completed") {
      of.completed = true;
      if (turn.providerThreadId !== undefined) of.completedOn.push(turn.providerThreadId);
    } else if (turn.status === "cancelled") of.cancelled = true;
  };
  let turnQuestions = 0;
  let attemptQuestions = 0;
  const turnsOf = (attemptId: string | null): Turns | undefined => {
    if (turnsByAttempt === undefined) {
      if (turnQuestions++ === 0) {
        let of: Turns | undefined;
        for (const turn of providerTurns)
          if (turn.runAttemptId === attemptId)
            add((of ??= { completed: false, cancelled: false, completedOn: [] }), turn);
        return of;
      }
      turnsByAttempt = new Map();
      for (const turn of providerTurns) {
        let of = turnsByAttempt.get(turn.runAttemptId);
        if (of === undefined) {
          of = { completed: false, cancelled: false, completedOn: [] };
          turnsByAttempt.set(turn.runAttemptId, of);
        }
        add(of, turn);
      }
    }
    return turnsByAttempt.get(attemptId);
  };
  const attemptsOf = (runId: string): ReadonlyArray<Attempt> => {
    if (attemptsByRun === undefined) {
      if (attemptQuestions++ === 0) return attempts.filter((attempt) => attempt.runId === runId);
      attemptsByRun = new Map();
      for (const attempt of attempts) {
        const of = attemptsByRun.get(attempt.runId);
        if (of === undefined) attemptsByRun.set(attempt.runId, [attempt]);
        else of.push(attempt);
      }
    }
    return attemptsByRun.get(runId) ?? none;
  };
  const completed = (attemptId: string | null) => turnsOf(attemptId)?.completed === true;
  return {
    runById: (id: string) => {
      if (runsById === undefined) {
        runsById = new Map();
        for (const run of runs) if (!runsById.has(run.id)) runsById.set(run.id, run);
      }
      return runsById.get(id);
    },
    attemptsOf,
    completed,
    cancelled: (attemptId: string | null) => turnsOf(attemptId)?.cancelled === true,
    /** A completed provider turn of the attempt on that provider thread. */
    completedOn: (attemptId: string | null, providerThreadId: string) =>
      turnsOf(attemptId)?.completedOn.includes(providerThreadId) === true,
    /** Any of the run's attempts has a completed provider turn. */
    runCompleted: (runId: string) => attemptsOf(runId).some((attempt) => completed(attempt.id)),
  };
}

/**
 * What the restart-note checks look up over one projection, with each run's continuation verdict
 * remembered: a turn start on a thread with tens of thousands of runs must not rescan them per
 * run.
 */
export function restartNoteIndex(
  runs: ReadonlyArray<OrchestrationV2Run>,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "status">>,
  attempts: ReadonlyArray<Attempt>,
  shared: ReturnType<typeof turnStartIndex> = turnStartIndex(runs, providerTurns, attempts),
) {
  const isSource = (
    source: Pick<
      OrchestrationV2Run,
      "status" | "activeAttemptId" | "restartCancelledBackgroundWork"
    >,
  ) =>
    (source.status === "completed" ||
      source.status === "waiting" ||
      source.status === "cancelled") &&
    (source.restartCancelledBackgroundWork?.length ?? 0) > 0 &&
    !shared.cancelled(source.activeAttemptId);
  const continues = (current: OrchestrationV2Run) =>
    current.restartContinuationOfRunId !== undefined &&
    !shared.runCompleted(current.id) &&
    !(current.activeAttemptId !== null && shared.completed(current.activeAttemptId));
  // The runs a continuation's walk passes, as `restartContinuationNote` walks them: through
  // runs that continue an unfinished run, stopping at a missing run or one it already passed.
  const walk = (source: OrchestrationV2Run, stopAt: (run: OrchestrationV2Run) => boolean) => {
    const path = [source];
    const visited = new Set([source.id]);
    // Where the path's loop starts, when it loops: the position of the run it came back to.
    let loopsFrom = -1;
    let stopped = false;
    let current = source;
    while (continues(current)) {
      const previous = shared.runById(current.restartContinuationOfRunId!);
      if (previous === undefined) break;
      if (visited.has(previous.id)) {
        loopsFrom = path.findIndex((run) => run.id === previous.id);
        break;
      }
      if (stopAt(previous)) {
        stopped = true;
        break;
      }
      visited.add(previous.id);
      path.push(previous);
      current = previous;
    }
    return { path, loopsFrom, stopped, end: current };
  };
  const noteFrom = (source: OrchestrationV2Run): Note => {
    const { path, end } = walk(source, () => false);
    if (path.length === 1)
      return { work: source.restartCancelledBackgroundWork ?? [], settled: isSource(end) };
    // Merging is associative: the walk's note is its last run's work, then each earlier run's new
    // entries, latest first, read once each.
    const work = [...(end.restartCancelledBackgroundWork ?? [])];
    const seen = new Set(work.map(workIdentity));
    for (let index = path.length - 2; index >= 0; index -= 1) {
      for (const entry of path[index]!.restartCancelledBackgroundWork ?? []) {
        const identity = workIdentity(entry);
        if (seen.has(identity)) continue;
        seen.add(identity);
        work.push(entry);
      }
    }
    return { work, settled: isSource(end) };
  };
  // Whether the walk from one of the projection's runs carries any work, remembered per run: a
  // note's work is empty only when every run it passes holds none. Every run on a loop passes the
  // whole loop, wherever its walk starts, so they share one answer.
  const carries = new Map<string, boolean>();
  const holds = (run: OrchestrationV2Run) => (run.restartCancelledBackgroundWork?.length ?? 0) > 0;
  const carriesWork = (source: OrchestrationV2Run): boolean => {
    const remembered = carries.get(source.id);
    if (remembered !== undefined) return remembered;
    const { path, loopsFrom, stopped, end } = walk(source, (run) => carries.has(run.id));
    let carried =
      stopped && end.restartContinuationOfRunId !== undefined
        ? carries.get(shared.runById(end.restartContinuationOfRunId)!.id)!
        : false;
    let before = path.length;
    if (loopsFrom !== -1) {
      const loop = path.slice(loopsFrom);
      carried = loop.some(holds);
      for (const run of loop) carries.set(run.id, carried);
      before = loopsFrom;
    }
    for (let index = before - 1; index >= 0; index -= 1) {
      const run = path[index]!;
      carried ||= holds(run);
      carries.set(run.id, carried);
    }
    return carried;
  };
  const isContinuation = (run: Pick<OrchestrationV2Run, "restartContinuationOfRunId">) => {
    const source =
      run.restartContinuationOfRunId === undefined
        ? undefined
        : shared.runById(run.restartContinuationOfRunId);
    return source !== undefined && carriesWork(source);
  };
  return { noteFrom, isContinuation };
}

/** Running does not prove prompt acceptance; carry continuation notes until a completed turn proves delivery. */
export function restartContinuationNote(
  source: OrchestrationV2Run,
  runs: ReadonlyArray<OrchestrationV2Run>,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "status">>,
  attempts: ReadonlyArray<Attempt>,
): Note {
  return restartNoteIndex(runs, providerTurns, attempts).noteFrom(source);
}

export function isRestartNoteContinuation(
  run: Pick<OrchestrationV2Run, "restartContinuationOfRunId">,
  runs: ReadonlyArray<OrchestrationV2Run>,
  providerTurns: ReadonlyArray<Pick<ProviderTurnState, "runAttemptId" | "status">>,
  attempts: ReadonlyArray<Attempt>,
): boolean {
  return restartNoteIndex(runs, providerTurns, attempts).isContinuation(run);
}

interface RestartNoteInput {
  readonly runs: ReadonlyArray<OrchestrationV2Run>;
  readonly providerTurns: ReadonlyArray<ProviderTurnState>;
  readonly run: Pick<
    OrchestrationV2Run,
    | "id"
    | "ordinal"
    | "completedAt"
    | "userMessageId"
    | "providerThreadId"
    | "restartContinuationOfRunId"
    | "activeAttemptId"
  >;

  readonly attempts: ReadonlyArray<Attempt>;
}

// `runRanAfter`'s order as a key read once per run: end (unfinished last), then ordinal.
const ranAfterKey = (run: Pick<OrchestrationV2Run, "ordinal" | "completedAt">) => ({
  end: !run.completedAt ? Number.POSITIVE_INFINITY : DateTime.toEpochMillis(run.completedAt),
  ordinal: run.ordinal,
});
const keyRanAfter = (
  left: ReturnType<typeof ranAfterKey>,
  right: ReturnType<typeof ranAfterKey>,
) => (left.end === right.end ? left.ordinal > right.ordinal : left.end > right.end);

/**
 * Positions latest-ran first by the runs' cached ran-after keys, ties in position order (a stable
 * sort). A thread's runs are in ran order already, as a rule, and the sort (V8's TimSort) takes a
 * single pass over them then.
 */
export const latestRanFirst = (ends: ArrayLike<number>, ordinals: ArrayLike<number>) =>
  Array.from({ length: ends.length }, (_, position) => position).sort((left, right) =>
    ends[left] === ends[right]
      ? ordinals[right]! - ordinals[left]!
      : ends[left]! > ends[right]!
        ? -1
        : 1,
  );

/**
 * A turn start's restart-note decisions over one projection, in the order it makes them: whether its
 * run continues a restart note, the runs whose compaction the note can turn on, and the pending note.
 * The provider thread's runs and which of them prompted it are worked out once, for both.
 */
export function planRestartNote(
  input: RestartNoteInput,
  shared: ReturnType<typeof turnStartIndex> = turnStartIndex(
    input.runs,
    input.providerTurns,
    input.attempts,
  ),
) {
  const index = restartNoteIndex(input.runs, input.providerTurns, input.attempts, shared);
  let threadRuns:
    | {
        readonly delivered: (attemptId: string | null) => boolean;
        readonly sameThread: ReadonlyArray<OrchestrationV2Run>;
        // By position in `sameThread`: whether the run prompted the thread unless a compaction,
        // worked out when first asked, and its ran-after key.
        readonly prompted: (position: number) => boolean;
        readonly ends: Float64Array;
        readonly ordinals: Float64Array;
      }
    | undefined;
  // The provider thread's runs, and whether each prompted it apart from being a compaction.
  const restartNoteRuns = (
    providerThreadId: NonNullable<OrchestrationV2Run["providerThreadId"]>,
  ) => {
    if (threadRuns !== undefined) return threadRuns;
    // An attempt whose provider turn on this thread completed delivered its prompt.
    const delivered = (attemptId: string | null) => shared.completedOn(attemptId, providerThreadId);
    const sameThread = input.runs.filter((run) => run.providerThreadId === providerThreadId);
    const ends = new Float64Array(sameThread.length);
    const ordinals = new Float64Array(sameThread.length);
    sameThread.forEach((candidate, position) => {
      const key = ranAfterKey(candidate);
      ends[position] = key.end;
      ordinals[position] = key.ordinal;
    });
    // A start asks about the latest-ran runs, as a rule, so each verdict waits until asked for.
    const verdicts = new Int8Array(sameThread.length);
    const prompted = (position: number) => {
      if (verdicts[position] === 0) {
        const candidate = sameThread[position]!;
        verdicts[position] =
          candidate.id !== input.run.id &&
          candidate.activeAttemptId !== null &&
          candidate.status !== "rolled_back" &&
          (delivered(candidate.activeAttemptId) ||
            shared.attemptsOf(candidate.id).some((attempt) => delivered(attempt.id))) &&
          (candidate.restartContinuationOfRunId === undefined || index.isContinuation(candidate))
            ? 1
            : 2;
      }
      return verdicts[position] === 1;
    };
    threadRuns = { delivered, sameThread, prompted, ends, ordinals };
    return threadRuns;
  };
  const holdsWork = (source: OrchestrationV2Run) =>
    (source.restartCancelledBackgroundWork?.length ?? 0) > 0;
  // Whether a run on the provider thread that ran before this one holds restart-note work.
  const holdsEarlierWork = () => {
    if (input.run.providerThreadId === null || input.run.restartContinuationOfRunId !== undefined)
      return false;
    const { sameThread, ends, ordinals } = restartNoteRuns(input.run.providerThreadId);
    const runKey = ranAfterKey(input.run);
    return sameThread.some(
      (source, position) =>
        holdsWork(source) &&
        keyRanAfter(runKey, { end: ends[position]!, ordinal: ordinals[position]! }),
    );
  };
  return {
    isContinuation: index.isContinuation,
    holdsEarlierWork,
    /**
     * The runs whose compaction `pending` can turn on, latest-ran first: each prompted the provider
     * thread unless its message is a compaction. Only those up to the first that is not one can, as
     * it ran after the rest; none when no earlier run there holds the work.
     */
    *compactionCandidates(): Generator<OrchestrationV2Run> {
      if (input.run.providerThreadId === null || input.run.restartContinuationOfRunId !== undefined)
        return;
      if (!holdsEarlierWork()) return;
      const { sameThread, prompted, ends, ordinals } = restartNoteRuns(input.run.providerThreadId);
      // Each run's verdict is worked out as it is taken: a caller that stops at the first run that
      // is not a compaction pays for the runs it takes.
      for (const position of latestRanFirst(ends, ordinals))
        if (prompted(position)) yield sameThread[position]!;
    },
    /** Derive delivery from later completed turns on the same provider thread, excluding compactions, resumes, and rolled-back runs. */
    pending: (compactionMessageIds: ReadonlySet<string>): ReadonlyArray<Work> => {
      const isCompaction = (run: Pick<OrchestrationV2Run, "userMessageId">) =>
        compactionMessageIds.has(run.userMessageId);
      if (
        input.run.providerThreadId === null ||
        input.run.restartContinuationOfRunId !== undefined ||
        isCompaction(input.run)
      )
        return [];
      const { delivered, sameThread, prompted, ends, ordinals } = restartNoteRuns(
        input.run.providerThreadId,
      );
      // The latest-ran run that prompted the thread: a source has a later prompt only if it ran before.
      // Scanned from the end, asking only about runs that ran no earlier than the latest so far; a
      // tie goes to the earlier position, as a forward scan keeping the first gave it.
      let latest = -1;
      for (let position = sameThread.length - 1; position >= 0; position--) {
        if (
          latest !== -1 &&
          (ends[position] === ends[latest]
            ? ordinals[position]! < ordinals[latest]!
            : ends[position]! < ends[latest]!)
        )
          continue;
        if (prompted(position) && !isCompaction(sameThread[position]!)) latest = position;
      }
      // Earlier attempts may already have delivered the note; a steer's replacement attempt must not repeat it.
      const alreadyDelivered = shared
        .attemptsOf(input.run.id)
        .some((attempt) => attempt.id !== input.run.activeAttemptId && delivered(attempt.id));
      if (alreadyDelivered) return [];
      const runKey = ranAfterKey(input.run);
      const latestKey =
        latest === -1 ? undefined : { end: ends[latest]!, ordinal: ordinals[latest]! };
      const work: Array<Work> = [];
      const seen = new Set<string>();
      for (let position = 0; position < sameThread.length; position++) {
        const source = sameThread[position]!;
        const key = { end: ends[position]!, ordinal: ordinals[position]! };
        if (
          !keyRanAfter(runKey, key) ||
          !holdsWork(source) ||
          (latestKey !== undefined && keyRanAfter(latestKey, key))
        )
          continue;
        for (const entry of source.restartCancelledBackgroundWork ?? []) {
          const identity = workIdentity(entry);
          if (seen.has(identity)) continue;
          seen.add(identity);
          work.push(entry);
        }
      }
      return work;
    },
  };
}

/** Derive delivery from later completed turns on the same provider thread, excluding compactions, resumes, and rolled-back runs. */
export function pendingRestartCancelledBackgroundWork(
  input: RestartNoteInput & {
    /** Called only when an earlier run on the provider thread carries restart-note work. */
    readonly compactionMessageIds: () => ReadonlySet<string>;
  },
): ReadonlyArray<Work> {
  const plan = planRestartNote(input);
  // Threads whose server never restarted carry no notes; skip every other lookup for them.
  return plan.holdsEarlierWork() ? plan.pending(input.compactionMessageIds()) : [];
}

/**
 * The runs whose compaction `pendingRestartCancelledBackgroundWork` can turn on, latest-ran first:
 * each prompted the provider thread unless its message is a compaction. Only those up to the first
 * that is not one can, as it ran after the rest; none when no earlier run there holds the work.
 */
export function restartNoteCompactionCandidates(
  input: RestartNoteInput,
): ReadonlyArray<OrchestrationV2Run> {
  return [...planRestartNote(input).compactionCandidates()];
}

const workIdentity = (entry: Work) => entry.id ?? `${entry.kind}\u0000${entry.label}`;

export function mergeRestartCancelledBackgroundWork(
  current: ReadonlyArray<Work>,
  added: ReadonlyArray<Work>,
): ReadonlyArray<Work> {
  const seen = new Set(current.map(workIdentity));
  const merged = [...current];
  for (const entry of added) {
    const key = workIdentity(entry);
    if (seen.has(key)) continue;
    seen.add(key);
    merged.push(entry);
  }
  return merged;
}

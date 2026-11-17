import type { NodeId, OrchestrationV2ConversationMessage, RunId } from "@t3tools/contracts";

export type CompletionWakeOwnership = NonNullable<
  OrchestrationV2ConversationMessage["delegatedCompletion"]
>;

export interface CompletionWakePart {
  readonly parentRunId: RunId;
  readonly generation: number;
  readonly taskIds: ReadonlyArray<NodeId>;
}

/** The parent runs whose results one completion wake delivers: its own first, then merged ones. */
export function completionWakeParts(
  ownership: CompletionWakeOwnership,
): ReadonlyArray<CompletionWakePart> {
  const merged = ownership.merged ?? [];
  const mergedTaskIds = new Set<NodeId>(merged.flatMap((part) => part.taskIds));
  return [
    {
      parentRunId: ownership.parentRunId,
      generation: ownership.generation,
      taskIds: ownership.taskIds.filter((taskId) => !mergedTaskIds.has(taskId)),
    },
    ...merged,
  ];
}

export function completionWakeOwnership(
  parts: ReadonlyArray<CompletionWakePart>,
): CompletionWakeOwnership | undefined {
  const [own, ...merged] = parts;
  if (own === undefined) return undefined;
  return {
    parentRunId: own.parentRunId,
    generation: own.generation,
    taskIds: Array.from(new Set(parts.flatMap((part) => part.taskIds))),
    ...(merged.length === 0
      ? {}
      : {
          merged: merged.map(({ parentRunId, generation, taskIds }) => ({
            parentRunId,
            generation,
            taskIds: [...taskIds],
          })),
        }),
  };
}

/** Replaces the part of the same parent run, or adds it last. */
export function withCompletionWakePart(
  ownership: CompletionWakeOwnership,
  part: CompletionWakePart,
): CompletionWakeOwnership {
  const parts = completionWakeParts(ownership);
  return completionWakeOwnership(
    parts.some((candidate) => candidate.parentRunId === part.parentRunId)
      ? parts.map((candidate) => (candidate.parentRunId === part.parentRunId ? part : candidate))
      : [...parts, part],
  )!;
}

export function withoutCompletionWakePart(
  ownership: CompletionWakeOwnership,
  parentRunId: RunId,
): CompletionWakeOwnership | undefined {
  return completionWakeOwnership(
    completionWakeParts(ownership).filter((part) => part.parentRunId !== parentRunId),
  );
}

/** A run's place in the chain that attributes completion wakes to a delegated thread's tasks. */
export interface CompletionWakeLineage {
  readonly runId: RunId;
  readonly delegatedTaskId: NodeId | null;
  /** For an untagged run, the parent runs of every completion wake it ran. */
  readonly parentRunIds: ReadonlyArray<RunId>;
  readonly restartContinuationOfRunId?: RunId;
  /** The follow-ups promoted to Steer into this run, whose tasks it works for too. */
  readonly promotedTaskIds?: ReadonlyArray<NodeId>;
}

/**
 * Keys the follow-up tasks a wake for these parent runs counts for, as `delegatedTaskRuns` links
 * them; empty is the original task. Delegated threads merge only wakes with equal keys, so each
 * task still reads the wake that carried its own results.
 */
export function completionWakeOwnerKeys(
  lineage: ReadonlyArray<CompletionWakeLineage>,
): (parentRunIds: ReadonlyArray<RunId>) => string {
  const rows = new Map(lineage.map((row) => [row.runId, row]));
  // A follow-up promoted into a run owns it and that run's restart continuations.
  const promotedInto = (runId: RunId): ReadonlyArray<NodeId> => {
    const taskIds: Array<NodeId> = [];
    const seen = new Set<RunId>();
    for (
      let row = rows.get(runId);
      row !== undefined && !seen.has(row.runId);
      row =
        row.restartContinuationOfRunId === undefined
          ? undefined
          : rows.get(row.restartContinuationOfRunId)
    ) {
      seen.add(row.runId);
      taskIds.push(...(row.promotedTaskIds ?? []));
    }
    return taskIds;
  };
  const owners = new Map<RunId, ReadonlySet<NodeId>>();
  // Iterative: a seat's chain runs hundreds of wakes deep.
  const ownersOf = (start: RunId): ReadonlySet<NodeId> => {
    const stack: Array<{ readonly runId: RunId; expanded: boolean }> = [
      { runId: start, expanded: false },
    ];
    const onPath = new Set<RunId>();
    while (stack.length > 0) {
      const top = stack[stack.length - 1]!;
      if (owners.has(top.runId)) {
        stack.pop();
        continue;
      }
      const row = rows.get(top.runId);
      if (row === undefined || row.delegatedTaskId !== null) {
        owners.set(
          top.runId,
          new Set([
            ...(row?.delegatedTaskId == null ? [] : [row.delegatedTaskId]),
            ...promotedInto(top.runId),
          ]),
        );
        stack.pop();
        continue;
      }
      if (!top.expanded) {
        top.expanded = true;
        onPath.add(top.runId);
        for (const parentRunId of row.parentRunIds) {
          if (!owners.has(parentRunId) && !onPath.has(parentRunId))
            stack.push({ runId: parentRunId, expanded: false });
        }
        continue;
      }
      owners.set(
        top.runId,
        new Set([
          ...row.parentRunIds.flatMap((parentRunId) => [...(owners.get(parentRunId) ?? [])]),
          ...promotedInto(top.runId),
        ]),
      );
      onPath.delete(top.runId);
      stack.pop();
    }
    return owners.get(start)!;
  };
  return (parentRunIds) =>
    JSON.stringify(
      Array.from(new Set(parentRunIds.flatMap((runId) => [...ownersOf(runId)]))).toSorted(),
    );
}

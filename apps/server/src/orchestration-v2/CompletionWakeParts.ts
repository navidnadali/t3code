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

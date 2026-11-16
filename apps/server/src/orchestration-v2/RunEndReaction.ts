import type { OrchestrationV2Run, RunId } from "@t3tools/contracts";

/**
 * What a run update asks of the run-end reactor. "none": a rewrite of an ended run that changes
 * neither its status nor its delegated completions; "settle": a rewrite that changes only its
 * delegated completions, or a queued run cancelled before it ran, which settle deliveries but end
 * no work; "full": anything else, including a run this reactor has not seen before.
 */
export type RunEndReaction = "none" | "settle" | "full";

// Longer cohorts are not kept, so a rewrite of one always settles.
const MAX_REMEMBERED_COHORT = 512;

/** Remembers the last status and cohort of up to `capacity` runs, in the order updates arrive. */
export function makeRunEndReactions(capacity: number) {
  const seen = new Map<
    RunId,
    { readonly status: OrchestrationV2Run["status"]; readonly cohort: string | null }
  >();
  return {
    observe(
      run: Pick<OrchestrationV2Run, "id" | "status" | "delegatedCompletion">,
    ): RunEndReaction {
      const previous = seen.get(run.id);
      const encoded = JSON.stringify(run.delegatedCompletion ?? null);
      const cohort = encoded.length <= MAX_REMEMBERED_COHORT ? encoded : null;
      seen.delete(run.id);
      seen.set(run.id, { status: run.status, cohort });
      if (seen.size > capacity) seen.delete(seen.keys().next().value!);
      if (previous === undefined) return "full";
      if (previous.status === run.status) {
        if (!isEnded(run.status)) return "full";
        return cohort !== null && previous.cohort === cohort ? "none" : "settle";
      }
      return previous.status === "queued" && run.status === "cancelled" ? "settle" : "full";
    },
  };
}

function isEnded(status: OrchestrationV2Run["status"]): boolean {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "interrupted" ||
    status === "cancelled" ||
    status === "rolled_back"
  );
}

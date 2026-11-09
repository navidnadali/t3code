import type { OrchestrationV2Run, OrchestrationV2ThreadProjection } from "@t3tools/contracts";

export function queuedRunsInDeliveryOrder(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "messages">,
): ReadonlyArray<OrchestrationV2Run> {
  // Completion wakes share the queue with user input. Giving them unconditional
  // priority starves input when a busy orchestrator keeps receiving child results.
  return projection.runs
    .filter((run) => run.status === "queued")
    .toSorted((left, right) => {
      return (
        (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal) ||
        left.ordinal - right.ordinal
      );
    });
}

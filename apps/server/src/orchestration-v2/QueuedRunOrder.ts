import type {
  MessageId,
  OrchestrationV2ConversationMessage,
  OrchestrationV2Run,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

/**
 * On a busy orchestrator visible messages take only a few of an hour's starts, so this rarely
 * binds. Under a visible flood a wake still gets every fifth start.
 */
export const MAX_VISIBLE_STARTS_AHEAD_OF_WAKE = 4;

/** Completion and notice wakes: clients do not list them. */
export function isHiddenWake(
  message: Pick<OrchestrationV2ConversationMessage, "delegatedCompletion" | "notification">,
): boolean {
  return message.delegatedCompletion !== undefined || message.notification !== undefined;
}

const queueOrder = (left: OrchestrationV2Run, right: OrchestrationV2Run) =>
  (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal) ||
  left.ordinal - right.ordinal;

// Derived from stored start times, so the count survives restarts. A start whose message was
// not read does not count.
function visibleStartsPassedWake(
  runs: ReadonlyArray<OrchestrationV2Run>,
  messages: ReadonlyMap<MessageId, OrchestrationV2ConversationMessage>,
  wake: OrchestrationV2Run,
): boolean {
  const since = runs
    .flatMap((run) =>
      run.status !== "queued" &&
      run.startedAt !== null &&
      DateTime.isGreaterThan(run.startedAt, wake.requestedAt)
        ? [{ run, startedAt: DateTime.toEpochMillis(run.startedAt) }]
        : [],
    )
    .toSorted(
      (left, right) => right.startedAt - left.startedAt || right.run.ordinal - left.run.ordinal,
    )
    .slice(0, MAX_VISIBLE_STARTS_AHEAD_OF_WAKE);
  return (
    since.length === MAX_VISIBLE_STARTS_AHEAD_OF_WAKE &&
    since.every(({ run }) => {
      const message = messages.get(run.userMessageId);
      return message !== undefined && !isHiddenWake(message);
    })
  );
}

/**
 * Visible messages go ahead of hidden wakes, each class in queue order. The oldest wake goes next
 * once MAX_VISIBLE_STARTS_AHEAD_OF_WAKE visible starts in a row passed it, behind only a message
 * moved to the front. That needs the thread's started runs in `projection.runs`; without them the
 * order is by class alone.
 */
export function queuedRunsInDeliveryOrder(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "messages">,
): ReadonlyArray<OrchestrationV2Run> {
  const messages = new Map(projection.messages.map((message) => [message.id, message]));
  const visible: Array<OrchestrationV2Run> = [];
  const hidden: Array<OrchestrationV2Run> = [];
  for (const run of projection.runs.filter((run) => run.status === "queued").toSorted(queueOrder)) {
    const message = messages.get(run.userMessageId);
    (message !== undefined && isHiddenWake(message) ? hidden : visible).push(run);
  }
  const [wake, ...laterWakes] = hidden;
  if (
    wake === undefined ||
    visible.length === 0 ||
    !visibleStartsPassedWake(projection.runs, messages, wake)
  ) {
    return [...visible, ...hidden];
  }
  const ahead = visible[0]?.queuePosition === 1 ? 1 : 0;
  return [...visible.slice(0, ahead), wake, ...visible.slice(ahead), ...laterWakes];
}

/** The run to start next; held runs wait, so only the others compete. */
export function nextQueuedRun(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "messages">,
): OrchestrationV2Run | undefined {
  return queuedRunsInDeliveryOrder({
    runs: projection.runs.filter((run) => run.status !== "queued" || run.queueHeld !== true),
    messages: projection.messages,
  })[0];
}

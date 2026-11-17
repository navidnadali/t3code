import {
  CommandId,
  isCompactionCommandMessage,
  latestProviderTurnForAttempt,
  parseGoalCommand,
  type ModelSelection,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnCapabilities,
  type OrchestrationV2TurnItem,
  type RunId,
} from "@t3tools/contracts";
import { projectComposerContextForProvider } from "@t3tools/shared/composerContextReferences";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";

import {
  isActiveRun,
  isNativeMaintenanceCommand,
  scheduledPromptSteersLiveTurn,
} from "./CommandPolicy.ts";
import { isHiddenWake, queuedRunsInDeliveryOrder } from "./QueuedRunOrder.ts";

/** How long a queued message from the user, an automation or another thread waits before it steers. */
export const QUEUED_DELIVERY_PROMOTION_GRACE = Duration.seconds(60);
/** A due message steers within one tick; a tick sends at most one steer per thread. */
export const QUEUED_DELIVERY_PROMOTION_TICK = Duration.seconds(5);
/**
 * A steer can cancel the turn's running tools, so one turn takes at most one automatic steer a
 * minute. Steers a user or agent sends themselves do not count.
 */
export const QUEUED_DELIVERY_PROMOTION_SPACING = Duration.seconds(60);
/** One automatic steer carries every due message up to this many (C2's merged-wake bound)... */
export const QUEUED_DELIVERY_MERGE_MAX_ITEMS = 25;
/** ...and this much text; a longer message still goes, alone. The rest go in the next steer. */
export const QUEUED_DELIVERY_MERGE_MAX_BYTES = 32 * 1024;
/** Marks this module's promote commands, which may only steer a live turn. */
export const QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX = "command:auto-send-now:";

/** The run automatic delivery steers into: the latest active one. */
export const queuedDeliveryTarget = (runs: ReadonlyArray<OrchestrationV2Run>) =>
  runs
    .filter(isActiveRun)
    .toSorted((left, right) => left.ordinal - right.ordinal)
    .at(-1);

/** The live turn automatic delivery may steer into. */
function steerableTurn(
  target: OrchestrationV2Run,
  providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>,
): OrchestrationV2ProviderTurn | undefined {
  if (target.status !== "running" && target.status !== "waiting") return undefined;
  return providerTurns.find(
    (candidate) =>
      candidate.runAttemptId === target.activeAttemptId && candidate.status === "running",
  );
}

/** A waiting run whose turn is over is held only by its checkpoint capture. */
function waitOutlivesTurn(
  target: OrchestrationV2Run,
  providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>,
): boolean {
  const turn = latestProviderTurnForAttempt(
    providerTurns.filter((candidate) => candidate.nodeId === target.rootNodeId),
    target.activeAttemptId,
  );
  return (
    target.status === "waiting" &&
    turn !== undefined &&
    turn.status !== "pending" &&
    turn.status !== "running"
  );
}

/** When a visible message may end a wait that only a checkpoint capture holds: it starts a run. */
function waitEndDueAt(
  run: OrchestrationV2Run,
  message: OrchestrationV2ConversationMessage,
): DateTime.Utc | null {
  if (run.status !== "queued" || run.queueHeld === true || isHiddenWake(message)) return null;
  // A steer that missed its turn was already sent to be delivered now.
  return run.steerFollowUpOfRunId !== undefined
    ? run.requestedAt
    : DateTime.addDuration(run.requestedAt, QUEUED_DELIVERY_PROMOTION_GRACE);
}

/** When a queued run may be promoted into the thread's active work, or null when never. */
function queuedDeliveryDueAt(
  run: OrchestrationV2Run,
  message: OrchestrationV2ConversationMessage,
  activeRunIds: ReadonlySet<RunId>,
  target: { readonly id: RunId; readonly liveTurnStartedAt: DateTime.Utc | null },
  turns: OrchestrationV2TurnCapabilities,
): DateTime.Utc | null {
  if (run.status !== "queued" || run.queueHeld === true || run.freshContext === true) return null;
  if (
    isHiddenWake(message) ||
    isNativeMaintenanceCommand(message) ||
    parseGoalCommand(message) !== null
  )
    return null;
  // The turn-start reactor delivers it; a turn that ran a whole tick without it means that reactor lags.
  if (run.steerWhenReadyRunId !== undefined && activeRunIds.has(run.steerWhenReadyRunId))
    return run.steerWhenReadyRunId === target.id && target.liveTurnStartedAt !== null
      ? DateTime.addDuration(target.liveTurnStartedAt, QUEUED_DELIVERY_PROMOTION_TICK)
      : null;
  // A steer that missed its turn was already sent to be delivered now, but never into the turn
  // that ended or declined it: another run's turn, or one of its own run's that started since.
  if (run.steerFollowUpOfRunId !== undefined)
    return run.steerFollowUpOfRunId !== target.id ||
      (target.liveTurnStartedAt !== null &&
        DateTime.isGreaterThanOrEqualTo(target.liveTurnStartedAt, run.requestedAt))
      ? run.requestedAt
      : null;
  if (run.steerWhenReadyRunId !== undefined) return run.requestedAt;
  // Where a scheduled prompt would have steered at dispatch, it need not wait.
  if (message.scheduledTaskId !== undefined && scheduledPromptSteersLiveTurn(turns))
    return run.requestedAt;
  return message.createdBy === "user" ||
    message.scheduledTaskId !== undefined ||
    (message.senderThreadId !== undefined && message.senderThreadId !== run.threadId) ||
    message.linkedSender !== undefined
    ? DateTime.addDuration(run.requestedAt, QUEUED_DELIVERY_PROMOTION_GRACE)
    : null;
}

export interface QueuedDeliveryPromotion {
  readonly commandId: CommandId;
  readonly queuedRunId: RunId;
  /** Further due messages sent in the same steer, in this order. */
  readonly mergedQueuedRunIds?: ReadonlyArray<RunId>;
  readonly targetRunId: RunId;
}

/** The user's own messages go first in a merged steer, then automations, then other threads'. */
function deliveryClass(message: OrchestrationV2ConversationMessage): number {
  if (message.scheduledTaskId !== undefined) return 1;
  if (
    (message.senderThreadId !== undefined && message.senderThreadId !== message.threadId) ||
    message.linkedSender !== undefined
  )
    return 2;
  return message.createdBy === "user" &&
    (message.creationSource === "web" || message.creationSource === "mobile")
    ? 0
    : 3;
}

const utf8 = new TextEncoder();

/** What the provider gets for one queued message: its text, composer context expanded. */
export const steerProviderText = (
  message: Pick<OrchestrationV2ConversationMessage, "text" | "context">,
) =>
  projectComposerContextForProvider({
    text: message.text,
    records: message.context?.records ?? [],
  });

/** A slash command runs only at the start of the provider's text, so it never shares a steer. */
export const startsWithSlashCommand = (message: Pick<OrchestrationV2ConversationMessage, "text">) =>
  message.text.trimStart().startsWith("/");

// Stable across retries of the same set, so a replayed command keeps its receipt.
function mergedRunsDigest(runIds: ReadonlyArray<RunId>): string {
  let hash = 0x811c9dc5;
  for (const char of runIds.join("\n")) {
    hash = Math.imul(hash ^ char.codePointAt(0)!, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

/** When the live turn's last automatic steer lets the next one in, or null when none limits it. */
function automaticSteerWindowOpensAt(
  targetItems: ReadonlyArray<OrchestrationV2TurnItem>,
  turn: OrchestrationV2ProviderTurn,
): number | null {
  let last: number | null = null;
  for (const item of targetItems) {
    if (
      item.type === "user_message" &&
      item.providerTurnId === turn.id &&
      item.automaticDelivery === true &&
      item.startedAt !== null
    )
      last = Math.max(last ?? -Infinity, DateTime.toEpochMillis(item.startedAt));
  }
  return last === null ? null : last + Duration.toMillis(QUEUED_DELIVERY_PROMOTION_SPACING);
}

export interface QueuedDeliveryInput {
  readonly now: DateTime.Utc;
  /** The thread's active runs and the queued runs to consider. */
  readonly runs: ReadonlyArray<OrchestrationV2Run>;
  readonly messages: ReadonlyArray<OrchestrationV2ConversationMessage>;
  readonly providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
  /** The thread's pending requests; one waiting on the user keeps a waiting run's wait. */
  readonly runtimeRequests: ReadonlyArray<Pick<OrchestrationV2RuntimeRequest, "status">>;
  /** The target's Stop requests, compaction items and user messages. */
  readonly targetTurnItems: ReadonlyArray<OrchestrationV2TurnItem>;
  readonly capabilities: OrchestrationV2TurnCapabilities;
  readonly modelSelectionOf: (run: OrchestrationV2Run) => ModelSelection;
}

export interface QueuedDeliveryDecision {
  readonly promotion: QueuedDeliveryPromotion | null;
  /** When the spacing ends or the first queued message comes due; a tick could be one late. */
  readonly retryAt: DateTime.Utc | null;
}

const none: QueuedDeliveryDecision = { promotion: null, retryAt: null };

export const decideQueuedDeliveryPromotion = (input: QueuedDeliveryInput) =>
  decideQueuedDelivery(input).promotion;

/**
 * The queued messages to promote now, or null. Into a live turn every due message goes as one
 * steer, the user's own first, each class in queue order, up to the merge bounds. A run waiting
 * for its checkpoint after its turn ended takes one by ending that wait, which starts the queue
 * as the next run. Any other run without a live turn gets none: its turn's start decides again.
 */
export function decideQueuedDelivery(input: QueuedDeliveryInput): QueuedDeliveryDecision {
  const target = queuedDeliveryTarget(input.runs);
  if (target === undefined) return none;
  const turn = steerableTurn(target, input.providerTurns);
  // Send now refuses a wait it cannot end, and its fixed command id keeps that refusal.
  const endsWait =
    turn === undefined &&
    waitOutlivesTurn(target, input.providerTurns) &&
    input.runs.filter(isActiveRun).length === 1 &&
    !input.runtimeRequests.some((request) => request.status === "pending");
  if (turn === undefined ? !endsWait : !input.capabilities.supportsActiveSteering) return none;
  // A turn that cannot take a steer still ends; only Stop keeps its wait.
  const targetMessage = input.messages.find((message) => message.id === target.userMessageId);
  if (
    !endsWait &&
    targetMessage !== undefined &&
    (isNativeMaintenanceCommand(targetMessage) || isCompactionCommandMessage(targetMessage))
  )
    return none;
  const targetItems = input.targetTurnItems.filter((item) => item.runId === target.id);
  if (
    targetItems.some((item) => item.type === "run_interrupt_request") ||
    (!endsWait &&
      input.capabilities.activeSteeringDuringCompaction !== true &&
      targetItems.some((item) => item.type === "compaction" && item.status === "running"))
  )
    return none;
  const windowOpensAtMs =
    turn === undefined ? null : automaticSteerWindowOpensAt(targetItems, turn);
  const activeRunIds = new Set(input.runs.filter(isActiveRun).map((run) => run.id));
  const nowMs = DateTime.toEpochMillis(input.now);
  // An automatic steer also carries what comes due before the next one may go: it interrupts
  // the turn anyway, and those would otherwise wait out a whole spacing more.
  const ridesUntilMs = nowMs + Duration.toMillis(QUEUED_DELIVERY_PROMOTION_SPACING);
  let firstDueMs: number | null = null;
  const due: Array<{
    readonly run: OrchestrationV2Run;
    readonly message: OrchestrationV2ConversationMessage;
  }> = [];
  const riders: Array<(typeof due)[number]> = [];
  const ordered = queuedRunsInDeliveryOrder(input);
  for (const run of ordered) {
    const message = input.messages.find((candidate) => candidate.id === run.userMessageId);
    if (message === undefined) continue;
    const dueAt = endsWait
      ? waitEndDueAt(run, message)
      : queuedDeliveryDueAt(
          run,
          message,
          activeRunIds,
          { id: target.id, liveTurnStartedAt: turn?.startedAt ?? null },
          input.capabilities,
        );
    // A message that starts its own run takes any attachments and model.
    if (
      dueAt === null ||
      DateTime.toEpochMillis(dueAt) > (endsWait ? nowMs : ridesUntilMs) ||
      (!endsWait &&
        ((message.attachments.length > 0 &&
          input.capabilities.activeSteeringAcceptsAttachments === false) ||
          !modelSelectionsEqual(input.modelSelectionOf(run), target.modelSelection)))
    )
      continue;
    if (turn === undefined)
      return {
        promotion: {
          commandId: CommandId.make(
            `${QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX}${run.id}:${target.id}:wait`,
          ),
          queuedRunId: run.id,
          targetRunId: target.id,
        },
        retryAt: null,
      };
    const dueMs = DateTime.toEpochMillis(dueAt);
    if (dueMs <= nowMs) due.push({ run, message });
    else {
      riders.push({ run, message });
      firstDueMs = Math.min(firstDueMs ?? Infinity, dueMs);
    }
  }
  if (turn === undefined) return none;
  if (due.length === 0)
    return firstDueMs === null
      ? none
      : { promotion: null, retryAt: DateTime.makeUnsafe(firstDueMs) };
  if (windowOpensAtMs !== null && windowOpensAtMs > nowMs)
    return { promotion: null, retryAt: DateTime.makeUnsafe(windowOpensAtMs) };
  // The oldest due message always goes, so nothing that outranks it can starve it; the other
  // due messages fill the steer by class, and riders only what is left. A slash command goes
  // alone, once it is the oldest.
  const order = new Map(ordered.map((run, index) => [run.id, index]));
  const byClass = (items: typeof due) =>
    items.toSorted(
      (left, right) =>
        deliveryClass(left.message) - deliveryClass(right.message) ||
        order.get(left.run.id)! - order.get(right.run.id)!,
    );
  const [oldest, ...laterDue] = due as [(typeof due)[number], ...typeof due];
  const taken: typeof due = [];
  // Counted as the provider gets it, framing included, with the widest part labels.
  const max = QUEUED_DELIVERY_MERGE_MAX_ITEMS;
  let bytes = utf8.encode(mergedSteerHeader(max)).byteLength;
  for (const item of [oldest, ...byClass(laterDue), ...byClass(riders)]) {
    const command = startsWithSlashCommand(item.message);
    const size =
      utf8.encode(mergedSteerPart(max - 1, max, item.message, steerProviderText(item.message)))
        .byteLength + 2;
    if (
      taken.length > 0 &&
      (command || taken.length === max || bytes + size > QUEUED_DELIVERY_MERGE_MAX_BYTES)
    )
      break;
    taken.push(item);
    bytes += size;
    if (command) break;
  }
  const steered = byClass(taken).map((item) => item.run.id);
  const [queuedRunId, ...mergedQueuedRunIds] = steered as [RunId, ...Array<RunId>];
  return {
    promotion: {
      commandId: CommandId.make(
        `${QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX}${queuedRunId}:${target.id}:${turn.id}${
          mergedQueuedRunIds.length === 0
            ? ""
            : `:merged:${steered.length}:${mergedRunsDigest(steered)}`
        }`,
      ),
      queuedRunId,
      ...(mergedQueuedRunIds.length === 0 ? {} : { mergedQueuedRunIds }),
      targetRunId: target.id,
    },
    retryAt: null,
  };
}

function sentBy(
  message: Pick<
    OrchestrationV2ConversationMessage,
    "threadId" | "createdBy" | "scheduledTaskId" | "senderThreadId" | "linkedSender"
  >,
): string {
  if (message.scheduledTaskId !== undefined) return `automation ${message.scheduledTaskId}`;
  if (message.linkedSender !== undefined)
    return `thread "${message.linkedSender.title}" (${message.linkedSender.threadId}) in linked environment ${message.linkedSender.environmentId}`;
  if (message.senderThreadId !== undefined && message.senderThreadId !== message.threadId)
    return `thread ${message.senderThreadId}`;
  return message.createdBy === "user"
    ? "the user"
    : message.createdBy === "agent"
      ? "an agent"
      : "T3 Code";
}

const mergedSteerHeader = (count: number) => `${count} queued messages, sent together:`;

const mergedSteerPart = (
  index: number,
  count: number,
  message: Parameters<typeof sentBy>[0],
  text: string,
) => `[${index + 1}/${count}] From ${sentBy(message)}:\n${text}`;

/** One provider message for a merged steer; each part names who sent it. */
export function mergedSteerText(
  parts: ReadonlyArray<{
    readonly text: string;
    readonly message: Parameters<typeof sentBy>[0];
  }>,
): string {
  return [
    mergedSteerHeader(parts.length),
    ...parts.map((part, index) => mergedSteerPart(index, parts.length, part.message, part.text)),
  ].join("\n\n");
}

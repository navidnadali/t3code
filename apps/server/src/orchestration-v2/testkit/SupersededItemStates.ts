import type { OrchestrationV2StoredEvent } from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

import type { ProviderEventIngestorV2Shape } from "../ProviderEventIngestor.ts";

const ACTIVE_STATUSES: ReadonlySet<unknown> = new Set(["pending", "running", "waiting"]);
const TEXT_ITEM_TYPES: ReadonlySet<unknown> = new Set(["assistant_message", "reasoning"]);
const ENDED_TURN_STATUSES: ReadonlySet<unknown> = new Set([
  "completed",
  "interrupted",
  "failed",
  "cancelled",
]);

/** The item a stored event carries the whole state of, and whether it is final; null for a control. */
function storedItem(
  event: OrchestrationV2StoredEvent["event"],
): { readonly key: string; readonly final: boolean } | null {
  const payload = event.payload as Readonly<Record<string, unknown>>;
  switch (event.type) {
    case "node.updated":
      return { key: `node ${String(payload.id)}`, final: !ACTIVE_STATUSES.has(payload.status) };
    case "message.updated":
      return { key: `message ${String(payload.id)}`, final: payload.streaming !== true };
    case "turn-item.updated":
      // A user message item is stored for a steer, which the mailbox keeps in order.
      if (payload.type === "user_message") return null;
      return {
        key: `turn-item ${String(payload.id)}`,
        final: TEXT_ITEM_TYPES.has(payload.type)
          ? payload.streaming !== true
          : !ACTIVE_STATUSES.has(payload.status),
      };
    case "provider-turn.updated":
      return ENDED_TURN_STATUSES.has(payload.status)
        ? null
        : { key: `provider-turn ${String(payload.id)}`, final: false };
    default:
      return null;
  }
}

/** As the mailbox does: a usage report taking another's place keeps the usage it lacks. */
function keepUsage(
  replaced: OrchestrationV2StoredEvent,
  newer: OrchestrationV2StoredEvent,
): OrchestrationV2StoredEvent {
  const payload = newer.event.payload as Readonly<Record<string, unknown>>;
  const earlier = (replaced.event.payload as Readonly<Record<string, unknown>>).tokenUsage;
  return newer.event.type !== "provider-turn.updated" ||
    payload.tokenUsage !== undefined ||
    earlier === undefined
    ? newer
    : ({
        ...newer,
        event: { ...newer.event, payload: { ...payload, tokenUsage: earlier } },
      } as OrchestrationV2StoredEvent);
}

/** A still-streaming text step: after the root terminal, the only state the mailbox replaces. */
function streamedStep(event: OrchestrationV2StoredEvent["event"]): boolean {
  const payload = event.payload as Readonly<Record<string, unknown>>;
  return (
    payload.streaming === true &&
    (event.type === "message.updated" ||
      (event.type === "turn-item.updated" && TEXT_ITEM_TYPES.has(payload.type)))
  );
}

const SETTLED_RUN_STATUSES: ReadonlySet<unknown> = new Set([
  "waiting",
  "completed",
  "interrupted",
  "failed",
  "cancelled",
  "rolled_back",
]);

// Controls a provider sends to the consuming run; they bound an inherited item (an earlier run's id)
// on the same thread too. Other writes, such as the session manager's session updates, bound only
// their own run's items.
const PROVIDER_CONTROL_TYPES: ReadonlySet<string> = new Set([
  "provider-thread.updated",
  "provider-turn.updated",
  "runtime-request.updated",
  "subagent.updated",
  "plan.updated",
]);

// Only these runs pass their open background items to the thread's next run
// (RunExecutionService.selectInheritedBackgroundTurnItems); other runs ingest their own late states.
const PASSING_ON_RUN_STATUSES: ReadonlySet<unknown> = new Set([
  "interrupted",
  "failed",
  "cancelled",
]);
const BACKGROUND_ITEM_TYPES: ReadonlySet<unknown> = new Set([
  "command_execution",
  "dynamic_tool",
  "subagent",
]);

/** Whether a settled run's state would pass to its thread's next run: a background turn item. */
function inheritable(event: OrchestrationV2StoredEvent["event"]): boolean {
  const payload = event.payload as Readonly<Record<string, unknown>>;
  return event.type === "turn-item.updated" && BACKGROUND_ITEM_TYPES.has(payload.type);
}

interface OpenState {
  readonly slot: number;
  readonly threadId: string;
  readonly runId: string;
  /** The run ingesting it: its own, or the thread's newer run for a settled run's item. */
  readonly consumer: string;
}

/**
 * The mailbox's coalescing applied to stored writes: a later state of an item takes its
 * waiting state's place until a control of its run or thread; a settled run replaces streamed text
 * steps only. Modes that coalesced differently compare equal after this; finals and controls stay.
 */
/**
 * A control a run's mailbox ingested that stored nothing (a background-work update, say): a barrier
 * the stored writes cannot show. It came after the run's stored write `afterEventId` (null: none).
 */
export interface NoStoreControl {
  readonly afterEventId: string | null;
  readonly runId: string;
  readonly threadId: string;
  readonly type: string;
}

/** Set by a comparison to collect each run's no-store controls while a replay ingests. */
export const NoStoreControlRecorder = Context.Reference<Array<NoStoreControl> | undefined>(
  "t3/orchestration-v2/testkit/NoStoreControlRecorder",
  { defaultValue: () => undefined },
);

const ITEM_STATE_TYPES: ReadonlySet<string> = new Set([
  "node.updated",
  "message.updated",
  "turn_item.updated",
]);

/** Whether the mailbox keys a provider event as an item state; anything else is a control. */
function isItemState(event: ProviderAdapter.ProviderAdapterV2Event): boolean {
  return (
    ITEM_STATE_TYPES.has(event.type) ||
    (event.type === "provider_turn.updated" && !ENDED_TURN_STATUSES.has(event.providerTurn.status))
  );
}

/** The ingestor, recording each control a run ingests that stores nothing. */
export const recordNoStoreControls = (
  ingestor: ProviderEventIngestorV2Shape,
  recorded: Array<NoStoreControl>,
): ProviderEventIngestorV2Shape => {
  const lastStored = new Map<string, string>();
  return {
    ...ingestor,
    ingestNormalized: (input) =>
      ingestor.ingestNormalized(input).pipe(
        Effect.tap((stored) =>
          Effect.sync(() => {
            if (input.runId === undefined) return;
            const last = stored.at(-1);
            if (last !== undefined) lastStored.set(input.runId, last.event.id);
            else if (!isItemState(input.event))
              recorded.push({
                afterEventId: lastStored.get(input.runId) ?? null,
                runId: input.runId,
                threadId: input.threadId,
                type: input.event.type,
              });
          }),
        ),
      ),
  };
};

export function collapseSupersededItemStates(
  stored: ReadonlyArray<OrchestrationV2StoredEvent>,
  controls: ReadonlyArray<NoStoreControl> = [],
): ReadonlyArray<OrchestrationV2StoredEvent> {
  const kept: Array<OrchestrationV2StoredEvent> = [];
  const controlsAfter = Map.groupBy(controls, (control) => control.afterEventId);
  const open = new Map<string, OpenState>();
  // Each settled run's status.
  const settledRuns = new Map<string, unknown>();
  // The run each thread started last: an earlier settled run's item it inherits is consumed by it.
  const latestRun = new Map<string, string>();
  const consumerOf = (threadId: string, runId: string, inherits: boolean) =>
    inherits && PASSING_ON_RUN_STATUSES.has(settledRuns.get(runId))
      ? (latestRun.get(threadId) ?? runId)
      : runId;
  const consumerSettled = (threadId: string, runId: string, inherits: boolean) =>
    settledRuns.has(consumerOf(threadId, runId, inherits));
  const barrier = (type: string, threadId: string, runId: string) => {
    const consumer = consumerOf(threadId, runId, true);
    for (const [key, state] of open)
      if (
        (runId !== "" &&
          (state.runId === runId || state.consumer === runId || state.consumer === consumer)) ||
        (PROVIDER_CONTROL_TYPES.has(type) && state.threadId === threadId)
      )
        open.delete(key);
  };
  // A control that stored nothing ends what waits in its run's mailbox, as any control does.
  const endRunWaits = (written: OrchestrationV2StoredEvent) => {
    for (const control of controlsAfter.get(written.event.id) ?? [])
      for (const [key, state] of open)
        if (state.consumer === control.runId || state.runId === control.runId) open.delete(key);
  };
  const take = (write: OrchestrationV2StoredEvent) => {
    const event = write.event as OrchestrationV2StoredEvent["event"] & { readonly runId?: unknown };
    const threadId = String(event.threadId);
    const runId = String(event.runId ?? "");
    const item = storedItem(event);
    const inherits = inheritable(event);
    const replaceable =
      item !== null && (!consumerSettled(threadId, runId, inherits) || streamedStep(event));
    if (!replaceable) {
      barrier(event.type, threadId, runId);
      kept.push(write);
      const payload = event.payload as Readonly<Record<string, unknown>>;
      if (event.type === "run.updated" && SETTLED_RUN_STATUSES.has(payload.status))
        settledRuns.set(String(payload.id), payload.status);
      else if (
        (event.type === "run.updated" || event.type === "run.created") &&
        (payload.status === "starting" || payload.status === "running")
      )
        latestRun.set(threadId, String(payload.id));
      return;
    }
    const waiting = open.get(item.key);
    if (waiting === undefined) {
      const consumer = consumerOf(threadId, runId, inherits);
      if (!item.final) open.set(item.key, { slot: kept.length, threadId, runId, consumer });
      kept.push(write);
      return;
    }
    kept[waiting.slot] = keepUsage(kept[waiting.slot]!, write);
    if (item.final) open.delete(item.key);
  };
  for (const write of stored) {
    take(write);
    endRunWaits(write);
  }
  return kept;
}

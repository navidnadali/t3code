import type { ProviderAdapterV2EventSubscription } from "@t3tools/provider-core/server/ProviderAdapter";
import type { WaitObserver } from "@t3tools/shared/WaitObserver";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

// Arrival times of provider events, read when their commit lands. Weak, so a dropped event
// takes its time with it; written only while a wait observer is present.
const arrivals = new WeakMap<object, bigint>();

/** Records when the session manager took `event` from its adapter, in monotonic nanoseconds. */
export const markArrival = (event: object, monotonicNs: bigint): void => {
  arrivals.set(event, monotonicNs);
};

export const arrivalOf = (event: object): bigint | undefined => arrivals.get(event);

/** Sites the ingest lag is reported under: one per event class. */
const lagSite = (eventType: string): string => {
  switch (eventType) {
    case "message.updated":
    case "turn_item.updated":
      return "ProviderIngest.lag.delta";
    case "subagent.updated":
    case "node.updated":
      return "ProviderIngest.lag.subagent";
    case "provider_turn.updated":
    case "provider_thread.updated":
    case "provider_session.updated":
    case "turn.terminal":
    case "background_work.updated":
    case "steer.updated":
    case "runtime_request.updated":
      return "ProviderIngest.lag.lifecycle";
    default:
      return "ProviderIngest.lag.other";
  }
};

/** The lag from the adapter's own stamp, where the event carries one. */
const adapterLagSite = (eventType: string): string =>
  lagSite(eventType).replace("ProviderIngest.lag.", "ProviderIngest.lagFromAdapter.");

const stampOf = (value: unknown, field: "updatedAt" | "createdAt"): number | undefined => {
  if (typeof value !== "object" || value === null || !(field in value)) return undefined;
  const stamp: unknown = (value as Record<string, unknown>)[field];
  return DateTime.isDateTime(stamp) ? DateTime.toEpochMillis(stamp) : undefined;
};

/**
 * When the adapter made the snapshot an event carries, in epoch ms: its `updatedAt`, or a
 * runtime request's `createdAt`. Turn, node and plan updates, terminals and work notices carry none.
 */
const adapterStampMs = (event: { readonly type: string }): number | undefined => {
  const fields = event as Readonly<Record<string, unknown>>;
  switch (event.type) {
    case "message.updated":
      return stampOf(fields.message, "updatedAt");
    case "turn_item.updated":
      return stampOf(fields.turnItem, "updatedAt");
    case "subagent.updated":
      return stampOf(fields.subagent, "updatedAt");
    case "provider_thread.updated":
      return stampOf(fields.providerThread, "updatedAt");
    case "provider_session.updated":
      return stampOf(fields.providerSession, "updatedAt");
    case "runtime_request.updated":
      return stampOf(fields.runtimeRequest, "createdAt");
    default:
      return undefined;
  }
};

/** The subscriptions each provider event is published to. */
export const FANOUT_SITE = "ProviderIngest.fanout";

/**
 * Events a run dropped before ingestion, unprocessed and uncommitted, because another run on its
 * shared session owns them: neither backlog nor lag.
 */
export const FANOUT_DROPPED_SITE = "ProviderIngest.fanout.dropped";

/** The events a turn terminal found queued ahead of it in each subscription. */
export const TERMINAL_BACKLOG_SITE = "ProviderIngest.terminalBacklog";

/**
 * Set while a consumer processes one provider event; a write of it marks it persisted. A shared
 * session hands every run every thread's events, and only the run that writes one reports it.
 */
export const CurrentIngest = Context.Reference<{ persisted: boolean } | undefined>(
  "t3/orchestration-v2/IngestLag/CurrentIngest",
  { defaultValue: () => undefined },
);

export const markPersisted: Effect.Effect<void> = Effect.withFiber((fiber) => {
  const ingest = fiber.getRef(CurrentIngest);
  if (ingest !== undefined) ingest.persisted = true;
  return Effect.void;
});

/**
 * Reports a stamped event's lag once its writes commit: from the session manager's take, and
 * from the adapter's own stamp where the event carries one. Unstamped events report nothing.
 */
export const reportCommitted = (
  observer: WaitObserver | undefined,
  event: { readonly type: string },
): Effect.Effect<void> => {
  if (observer === undefined) return Effect.void;
  const arrivedNs = arrivalOf(event);
  if (arrivedNs === undefined) return Effect.void;
  return Effect.gen(function* () {
    const nowNs = yield* Clock.monotonicTimeNanos;
    observer.wait(lagSite(event.type), Number(nowNs - arrivedNs), undefined);
    // The adapter's stamp is wall-clock; it adds the time before the manager took the event.
    const stampMs = adapterStampMs(event);
    if (stampMs === undefined) return;
    const nowMs = yield* Clock.currentTimeMillis;
    observer.wait(adapterLagSite(event.type), (nowMs - stampMs) * 1e6, undefined);
  });
};

/** Events a run's subscription has not taken yet, per thread. */
export const BACKLOG_SITE = "ProviderIngest.backlog";

/**
 * Reports a run's backlog under its thread until its subscription closes: what the subscription
 * holds, plus what the run took from it and has not processed (`buffered`).
 */
export const observeBacklog = (
  observer: WaitObserver | undefined,
  subscription: ProviderAdapterV2EventSubscription,
  threadId: string,
  buffered: () => number = () => 0,
): ProviderAdapterV2EventSubscription => {
  const pending = subscription.pending;
  if (observer === undefined || pending === undefined) return subscription;
  const unregister = observer.registerKeyed(BACKLOG_SITE, threadId, () => pending() + buffered());
  return { ...subscription, close: Effect.ensuring(subscription.close, Effect.sync(unregister)) };
};

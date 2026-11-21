import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import type { WaitObserver } from "@t3tools/shared/WaitObserver";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

import type * as EventSink from "../EventSink.ts";
import * as IngestLag from "../IngestLag.ts";

/** One event a bench consumer takes through the provider commit lane. */
export interface LaneItem {
  readonly type: string;
  readonly event: OrchestrationV2DomainEvent;
}

const ignored = () => () => {};

/**
 * Event commit latency on the commit lane, as the lane itself reports it once an event's write has
 * committed (its ingest lag): `observer` is provided while the event sink is built, `arrived` marks
 * an item as it is queued, and `process` writes it; `record` gets each event's latency in ms.
 */
export const makeCommitLatency = (record: (latencyMs: number) => void) => {
  const observer: WaitObserver = {
    wait: (site, waitNs) => {
      // The lag from the event's arrival, one site per event class (not the adapter-stamped one).
      if (site.startsWith("ProviderIngest.lag.")) record(waitNs / 1e6);
    },
    hold: () => {},
    depth: () => {},
    register: ignored,
    registerKeyed: ignored,
    sizeKeys: ignored,
  };
  const arrived = (item: LaneItem) =>
    Effect.flatMap(Clock.monotonicTimeNanos, (now) =>
      Effect.sync(() => IngestLag.markArrival(item, now)),
    );
  const process = (sink: EventSink.EventSinkV2Shape, item: LaneItem) =>
    sink.write({ events: [item.event] }).pipe(Effect.andThen(IngestLag.markPersisted));
  return { observer, arrived, process };
};

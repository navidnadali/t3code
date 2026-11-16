import { percentile } from "./sampling.ts";

/** A growable sample buffer; latencies stay exact so percentiles need no bucketing error. */
export class Samples {
  private buffer = new Float64Array(1024);
  private size = 0;

  push(value: number): void {
    if (this.size === this.buffer.length) {
      const grown = new Float64Array(this.buffer.length * 2);
      grown.set(this.buffer);
      this.buffer = grown;
    }
    this.buffer[this.size++] = value;
  }

  get length(): number {
    return this.size;
  }

  /** Appends every value of `other`. */
  pushAll(other: Samples): void {
    for (let index = 0; index < other.size; index++) this.push(other.buffer[index] as number);
  }

  sorted(): Float64Array {
    return this.buffer.slice(0, this.size).sort();
  }
}

/** Log-spaced buckets, four per doubling, so runs merge and compare bucket by bucket. */
export function bucketUpperBound(value: number): number {
  if (value <= 0.01) return 0.01;
  return 2 ** (Math.ceil(Math.log2(value) * 4) / 4);
}

export interface LatencySummary {
  readonly count: number;
  readonly errors: number;
  /** Of the errors, how many were timeouts (no answer in time) rather than failed answers. */
  readonly timeouts: number;
  /** Started in the window but still running when the grace period after it ran out. */
  readonly unfinished: number;
  readonly perSecond: number;
  readonly p50: number | null;
  readonly p95: number | null;
  readonly p99: number | null;
  readonly max: number | null;
  readonly mean: number | null;
  /** [bucket upper bound in ms, count] for each non-empty bucket, ascending. */
  readonly histogram: ReadonlyArray<readonly [number, number]>;
  readonly errorSamples: ReadonlyArray<string>;
}

const round = (value: number) => Math.round(value * 1000) / 1000;

export function summarizeLatencies(
  samples: Samples,
  errors: number,
  timeouts: number,
  unfinished: number,
  errorSamples: ReadonlyArray<string>,
  windowSeconds: number,
): LatencySummary {
  const sorted = samples.sorted();
  const buckets = new Map<number, number>();
  let total = 0;
  for (const value of sorted) {
    total += value;
    const bound = round(bucketUpperBound(value));
    buckets.set(bound, (buckets.get(bound) ?? 0) + 1);
  }
  const at = (fraction: number) =>
    sorted.length === 0 ? null : round(percentile(sorted, fraction));
  return {
    count: sorted.length,
    errors,
    timeouts,
    unfinished,
    perSecond: round((sorted.length + errors + unfinished) / windowSeconds),
    p50: at(0.5),
    p95: at(0.95),
    p99: at(0.99),
    max: sorted.length === 0 ? null : round(sorted[sorted.length - 1] as number),
    mean: sorted.length === 0 ? null : round(total / sorted.length),
    histogram: [...buckets.entries()],
    errorSamples,
  };
}

interface Series {
  readonly samples: Samples;
  errors: number;
  timeouts: number;
  inFlight: number;
  unfinished: number;
  readonly errorSamples: Array<string>;
}

interface Counter {
  count: number;
  bytes: number;
}

/** A call the recorder will attribute to the window; null for calls it ignores. */
export type CallHandle = {
  readonly type: string;
  readonly owner?: string;
  settled: boolean;
} | null;

/**
 * Collects the measured window. Calls belong to it by start time: one started during warm-up
 * never counts, and one started in the window counts when it ends, as long as it ends before
 * `seal()` (the grace period after the window). Events and counters count while the window is
 * open. Warm-up and drain traffic is driven the same way and dropped here.
 */
export class Recorder {
  private readonly series = new Map<string, Series>();
  /** Calls a session owns (its turns, dispatches, first items), by type and owner, so a summary can keep only some. */
  private readonly owned = new Map<string, Map<string, Series>>();
  private readonly counters = new Map<string, Counter>();
  private open = false;
  private sealed = false;

  openWindow(): void {
    this.open = true;
  }

  closeWindow(): void {
    this.open = false;
  }

  /** Ends the grace period: calls still running are reported as unfinished. */
  seal(): void {
    this.sealed = true;
    for (const entry of this.allSeries()) entry.unfinished = entry.inFlight;
  }

  private *allSeries(): Iterable<Series> {
    yield* this.series.values();
    for (const byOwner of this.owned.values()) yield* byOwner.values();
  }

  get isOpen(): boolean {
    return this.open;
  }

  private seriesFor(type: string, owner?: string): Series {
    const fresh = (): Series => ({
      samples: new Samples(),
      errors: 0,
      timeouts: 0,
      inFlight: 0,
      unfinished: 0,
      errorSamples: [],
    });
    if (owner === undefined) {
      let entry = this.series.get(type);
      if (entry === undefined) {
        entry = fresh();
        this.series.set(type, entry);
      }
      return entry;
    }
    let byOwner = this.owned.get(type);
    if (byOwner === undefined) {
      byOwner = new Map();
      this.owned.set(type, byOwner);
    }
    let entry = byOwner.get(owner);
    if (entry === undefined) {
      entry = fresh();
      byOwner.set(owner, entry);
    }
    return entry;
  }

  private noteError(entry: Series, reason: string, timeout = false): void {
    entry.errors++;
    if (timeout) entry.timeouts++;
    if (entry.errorSamples.length < 5 && !entry.errorSamples.includes(reason)) {
      entry.errorSamples.push(reason);
    }
  }

  /** Starts a call now; it counts only if the window is open at this moment. `owner` names the session it belongs to. */
  begin(type: string, owner?: string): CallHandle {
    if (!this.open) return null;
    this.seriesFor(type, owner).inFlight++;
    return owner === undefined ? { type, settled: false } : { type, owner, settled: false };
  }

  private settle(handle: CallHandle): Series | null {
    if (handle === null || handle.settled) return null;
    handle.settled = true;
    const entry = this.seriesFor(handle.type, handle.owner);
    entry.inFlight--;
    return this.sealed ? null : entry;
  }

  succeed(handle: CallHandle, milliseconds: number): void {
    this.settle(handle)?.samples.push(milliseconds);
  }

  /** A call that failed; `timeout` when it got no answer in time rather than a failed answer. */
  fail(handle: CallHandle, reason: string, options: { readonly timeout?: boolean } = {}): void {
    const entry = this.settle(handle);
    if (entry !== null) this.noteError(entry, reason, options.timeout);
  }

  /** Forgets a call that never really happened, such as a turn whose message was not sent. */
  discard(handle: CallHandle): void {
    this.settle(handle);
  }

  /** Calls started in the window that have not ended yet. */
  get inFlight(): number {
    let total = 0;
    for (const entry of this.allSeries()) total += entry.inFlight;
    return total;
  }

  /** A measurement taken when something arrives, such as an event's delivery lag. */
  sample(type: string, value: number, owner?: string): void {
    if (this.open) this.seriesFor(type, owner).samples.push(value);
  }

  /** A failure noticed when it happens rather than at the end of a timed call. */
  error(
    type: string,
    reason: string,
    options: { readonly owner?: string; readonly timeout?: boolean } = {},
  ): void {
    if (this.open) this.noteError(this.seriesFor(type, options.owner), reason, options.timeout);
  }

  /** The window's errors that were not timeouts, over every call type and owner (a 5xx, a decode error, ...). */
  nonTimeoutErrors(): number {
    let total = 0;
    for (const entry of this.merged().values()) total += entry.errors - entry.timeouts;
    return total;
  }

  /**
   * Each call type's calls merged: those without an owner, plus those of the owners `keep` accepts (every owner
   * without it). A type whose calls are all filtered out still appears, empty.
   */
  private merged(keep?: (owner: string) => boolean, unowned = true): Map<string, Series> {
    const merged = new Map<string, Series>();
    const into = (type: string, entry: Series) => {
      let target = merged.get(type);
      if (target === undefined) {
        target = {
          samples: new Samples(),
          errors: 0,
          timeouts: 0,
          inFlight: 0,
          unfinished: 0,
          errorSamples: [],
        };
        merged.set(type, target);
      }
      target.samples.pushAll(entry.samples);
      target.errors += entry.errors;
      target.timeouts += entry.timeouts;
      target.inFlight += entry.inFlight;
      target.unfinished += entry.unfinished;
      for (const sample of entry.errorSamples) {
        if (target.errorSamples.length < 5 && !target.errorSamples.includes(sample)) {
          target.errorSamples.push(sample);
        }
      }
    };
    if (unowned) for (const [type, entry] of this.series) into(type, entry);
    for (const [type, byOwner] of this.owned) {
      if (!merged.has(type)) {
        merged.set(type, {
          samples: new Samples(),
          errors: 0,
          timeouts: 0,
          inFlight: 0,
          unfinished: 0,
          errorSamples: [],
        });
      }
      for (const [owner, entry] of byOwner)
        if (keep === undefined || keep(owner)) into(type, entry);
    }
    return merged;
  }

  count(name: string, count = 1, bytes = 0): void {
    if (!this.open) return;
    const entry = this.counters.get(name) ?? { count: 0, bytes: 0 };
    entry.count += count;
    entry.bytes += bytes;
    this.counters.set(name, entry);
  }

  /** Error rate of the call type with the most errors relative to its attempts. */
  worstErrorRate(): { readonly type: string; readonly rate: number } | null {
    let worst: { type: string; rate: number } | null = null;
    for (const [type, entry] of this.merged()) {
      const attempts = entry.samples.length + entry.errors;
      const rate = attempts === 0 ? 0 : entry.errors / attempts;
      if (worst === null || rate > worst.rate) worst = { type, rate };
    }
    return worst;
  }

  private summarizeSeries(series: Map<string, Series>, windowSeconds: number) {
    const calls: Record<string, LatencySummary> = {};
    for (const type of [...series.keys()].toSorted()) {
      const entry = series.get(type) as Series;
      calls[type] = summarizeLatencies(
        entry.samples,
        entry.errors,
        entry.timeouts,
        entry.unfinished,
        entry.errorSamples,
        windowSeconds,
      );
    }
    return { calls };
  }

  /** The statistics of the owned calls of the owners `keepOwner` accepts only (one provider's sessions, say). */
  summarizeOwned(windowSeconds: number, keepOwner: (owner: string) => boolean) {
    return this.summarizeSeries(this.merged(keepOwner, false), windowSeconds);
  }

  /** The window's statistics; with `keepOwner`, session-owned calls only of the owners it accepts. */
  summarize(windowSeconds: number, keepOwner?: (owner: string) => boolean) {
    const { calls } = this.summarizeSeries(this.merged(keepOwner), windowSeconds);
    const counters: Record<string, { count: number; perSecond: number; bytes: number }> = {};
    for (const name of [...this.counters.keys()].toSorted()) {
      const entry = this.counters.get(name) as Counter;
      counters[name] = {
        count: entry.count,
        perSecond: round(entry.count / windowSeconds),
        bytes: entry.bytes,
      };
    }
    return { calls, counters };
  }
}

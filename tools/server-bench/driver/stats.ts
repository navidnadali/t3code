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
  inFlight: number;
  unfinished: number;
  readonly errorSamples: Array<string>;
}

interface Counter {
  count: number;
  bytes: number;
}

/** A call the recorder will attribute to the window; null for calls it ignores. */
export type CallHandle = { readonly type: string; settled: boolean } | null;

/**
 * Collects the measured window. Calls belong to it by start time: one started during warm-up
 * never counts, and one started in the window counts when it ends, as long as it ends before
 * `seal()` (the grace period after the window). Events and counters count while the window is
 * open. Warm-up and drain traffic is driven the same way and dropped here.
 */
export class Recorder {
  private readonly series = new Map<string, Series>();
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
    for (const entry of this.series.values()) entry.unfinished = entry.inFlight;
  }

  get isOpen(): boolean {
    return this.open;
  }

  private seriesFor(type: string): Series {
    let entry = this.series.get(type);
    if (entry === undefined) {
      entry = { samples: new Samples(), errors: 0, inFlight: 0, unfinished: 0, errorSamples: [] };
      this.series.set(type, entry);
    }
    return entry;
  }

  private noteError(entry: Series, reason: string): void {
    entry.errors++;
    if (entry.errorSamples.length < 5 && !entry.errorSamples.includes(reason)) {
      entry.errorSamples.push(reason);
    }
  }

  /** Starts a call now; it counts only if the window is open at this moment. */
  begin(type: string): CallHandle {
    if (!this.open) return null;
    this.seriesFor(type).inFlight++;
    return { type, settled: false };
  }

  private settle(handle: CallHandle): Series | null {
    if (handle === null || handle.settled) return null;
    handle.settled = true;
    const entry = this.seriesFor(handle.type);
    entry.inFlight--;
    return this.sealed ? null : entry;
  }

  succeed(handle: CallHandle, milliseconds: number): void {
    this.settle(handle)?.samples.push(milliseconds);
  }

  fail(handle: CallHandle, reason: string): void {
    const entry = this.settle(handle);
    if (entry !== null) this.noteError(entry, reason);
  }

  /** Forgets a call that never really happened, such as a turn whose message was not sent. */
  discard(handle: CallHandle): void {
    this.settle(handle);
  }

  /** Calls started in the window that have not ended yet. */
  get inFlight(): number {
    let total = 0;
    for (const entry of this.series.values()) total += entry.inFlight;
    return total;
  }

  /** A measurement taken when something arrives, such as an event's delivery lag. */
  sample(type: string, value: number): void {
    if (this.open) this.seriesFor(type).samples.push(value);
  }

  /** A failure noticed when it happens rather than at the end of a timed call. */
  error(type: string, reason: string): void {
    if (this.open) this.noteError(this.seriesFor(type), reason);
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
    for (const [type, entry] of this.series) {
      const attempts = entry.samples.length + entry.errors;
      const rate = attempts === 0 ? 0 : entry.errors / attempts;
      if (worst === null || rate > worst.rate) worst = { type, rate };
    }
    return worst;
  }

  summarize(windowSeconds: number) {
    const calls: Record<string, LatencySummary> = {};
    for (const type of [...this.series.keys()].toSorted()) {
      const entry = this.series.get(type) as Series;
      calls[type] = summarizeLatencies(
        entry.samples,
        entry.errors,
        entry.unfinished,
        entry.errorSamples,
        windowSeconds,
      );
    }
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

// @effect-diagnostics nodeBuiltinImport:off - node:perf_hooks has the event loop delay histogram and GC entries; node:zlib gzips on libuv's thread pool.
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeZlib from "node:zlib";

import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import type { PlatformError } from "effect/PlatformError";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Tracer from "effect/Tracer";

export const SPAN_MINUTES_FILE_NAME = "server.span-minutes.ndjson";

const MINUTE_MS = 60_000;
// A busy server ends a few hundred span names a minute, a line of tens of KB with the waits: a 4 MiB
// file closes every hour or two and gzips several times smaller, so 96 archives hold several days.
// The byte cap bounds the disk for any names.
export const SPAN_MINUTE_LOG_LIMITS = {
  maxBytes: 4 * 1024 * 1024,
  maxArchives: 96,
  maxArchiveBytes: 96 * 1024 * 1024,
} as const;

// Span names come from code; these caps bound a line should a name ever carry data.
export const MAX_SPAN_NAMES = 2_000;
export const MAX_SPAN_NAME_LENGTH = 200;
export const OTHER_SPAN_NAME = "(other)";

// Eight buckets per power of two nanoseconds: a bucket's midpoint is within 1/16 of each value in it.
const SUB_BUCKET_BITS = 3;
const SUB_BUCKETS = 1 << SUB_BUCKET_BITS;
// 2^46 ns is ~19.5 hours. Longer spans share the last bucket; max keeps their exact length.
const MAX_EXPONENT = 45;
const BUCKET_COUNT = (MAX_EXPONENT - SUB_BUCKET_BITS + 2) * SUB_BUCKETS;
const HISTOGRAM_LIMIT_NS = 2 ** (MAX_EXPONENT + 1);
const TWO_TO_32 = 2 ** 32;
const POWERS_OF_TWO = Array.from({ length: MAX_EXPONENT + 1 }, (_, power) => 2 ** power);

// `ns` is a whole number of nanoseconds; below 2^32 (~4.3 s) integer shifts suffice.
const bucketOf = (ns: number) => {
  if (ns < SUB_BUCKETS) return ns;
  if (ns < TWO_TO_32) {
    const exponent = 31 - Math.clz32(ns);
    return (
      ((exponent - SUB_BUCKET_BITS + 1) << SUB_BUCKET_BITS) +
      (ns >>> (exponent - SUB_BUCKET_BITS)) -
      SUB_BUCKETS
    );
  }
  const exponent = 63 - Math.clz32(ns / TWO_TO_32);
  return (
    (exponent - SUB_BUCKET_BITS + 1) * SUB_BUCKETS +
    Math.floor(ns / POWERS_OF_TWO[exponent - SUB_BUCKET_BITS]!) -
    SUB_BUCKETS
  );
};

// The first buckets hold one whole number each, so they are exact: a zero wait reads as zero.
const bucketMidpointNs = (bucket: number) => {
  if (bucket < SUB_BUCKETS) return bucket;
  const width = POWERS_OF_TWO[(bucket >>> SUB_BUCKET_BITS) - 1]!;
  return (SUB_BUCKETS + (bucket & (SUB_BUCKETS - 1))) * width + width / 2;
};

// Four significant digits below a second, whole ms above: finer than a p95 bucket's 1/16.
export const roundMs = (ms: number) => (ms >= 1000 ? Math.round(ms) : Number(ms.toPrecision(4)));

/** A count, total, max and log-linear histogram of whole numbers: nanoseconds, or a queue length. */
export interface Tally {
  count: number;
  totalNs: number;
  maxNs: number;
  readonly buckets: Uint32Array;
}

export const makeTally = (): Tally => ({
  count: 0,
  totalNs: 0,
  maxNs: 0,
  buckets: new Uint32Array(BUCKET_COUNT),
});

export const addToTally = (tally: Tally, ns: number): void => {
  tally.count++;
  tally.totalNs += ns;
  if (ns > tally.maxNs) tally.maxNs = ns;
  tally.buckets[ns < HISTOGRAM_LIMIT_NS ? bucketOf(ns) : BUCKET_COUNT - 1]!++;
};

export const resetTally = (tally: Tally): void => {
  tally.count = 0;
  tally.totalNs = 0;
  tally.maxNs = 0;
  tally.buckets.fill(0);
};

/**
 * Nearest rank for ascending quantiles in one pass, in the tally's unit. A rank equal to the count
 * is the largest value, known exactly (the p95 up to 19 values), as is anything in the last bucket.
 */
export const tallyPercentiles = (tally: Tally, quantiles: ReadonlyArray<number>): Array<number> => {
  const values: Array<number> = [];
  let seen = 0;
  let bucket = 0;
  for (const quantile of quantiles) {
    const rank = Math.ceil(tally.count * quantile);
    while (rank < tally.count && seen < rank && bucket < BUCKET_COUNT - 1) {
      seen += tally.buckets[bucket++]!;
    }
    values.push(
      rank < tally.count && seen >= rank
        ? Math.min(bucketMidpointNs(bucket - 1), tally.maxNs)
        : tally.maxNs,
    );
  }
  return values;
};

/** Count, then total, max and p95 duration in ms. */
export type SpanStat = readonly [count: number, totalMs: number, maxMs: number, p95Ms: number];

/** p50, p90 and p99 duration in ms, apart from `SpanStat` so its readers keep four fields. */
export type SpanPercentiles = readonly [p50Ms: number, p90Ms: number, p99Ms: number];

const SPAN_QUANTILES = [0.5, 0.9, 0.95, 0.99];
// Under ten spans the nearest-rank p90 and p99 are the max; leaving them out keeps the line short.
const PERCENTILES_FROM = 10;

export const makeSpanAggregate = () => {
  const tallies = new Map<string, Tally>();
  const addTally = (name: string): Tally => {
    const key = name.length > MAX_SPAN_NAME_LENGTH ? name.slice(0, MAX_SPAN_NAME_LENGTH) : name;
    const existing = tallies.get(key);
    if (existing !== undefined) return existing;
    const target = tallies.size < MAX_SPAN_NAMES ? key : OTHER_SPAN_NAME;
    const tally = tallies.get(target) ?? makeTally();
    tallies.set(target, tally);
    return tally;
  };
  return {
    record(name: string, startTime: bigint, endTime: bigint): void {
      addToTally(tallies.get(name) ?? addTally(name), Math.max(0, Number(endTime - startTime)));
    },
    /**
     * Returns the minute's stats and starts the next minute; names idle for a minute are dropped.
     * Only names ended ten times or more have percentiles.
     */
    drain(): {
      readonly spans: Record<string, SpanStat>;
      readonly spanPercentiles: Record<string, SpanPercentiles>;
    } {
      const spans: Record<string, SpanStat> = {};
      const spanPercentiles: Record<string, SpanPercentiles> = {};
      for (const [name, tally] of tallies) {
        if (tally.count === 0) {
          tallies.delete(name);
          continue;
        }
        const [p50 = 0, p90 = 0, p95 = 0, p99 = 0] = tallyPercentiles(tally, SPAN_QUANTILES).map(
          (ns) => roundMs(ns / 1e6),
        );
        spans[name] = [tally.count, roundMs(tally.totalNs / 1e6), roundMs(tally.maxNs / 1e6), p95];
        if (tally.count >= PERCENTILES_FROM) spanPercentiles[name] = [p50, p90, p99];
        resetTally(tally);
      }
      return { spans, spanPercentiles };
    },
  };
};

export interface ProcessReadings {
  readonly loop: {
    readonly delayP50Ms: number;
    readonly delayP99Ms: number;
    readonly delayMaxMs: number;
    readonly utilization: number;
  };
  readonly cpu: { readonly userMs: number; readonly systemMs: number };
  readonly memory: {
    readonly rssBytes: number;
    readonly heapUsedBytes: number;
    readonly heapTotalBytes: number;
  };
}

// Twice the effect admission gate's period, so the extra timer wakes an idle loop less.
const LOOP_DELAY_RESOLUTION_MS = 20;

const nodeSampler: Effect.Effect<Effect.Effect<ProcessReadings>, never, Scope.Scope> = Effect.gen(
  function* () {
    const histogram = yield* Effect.acquireRelease(
      Effect.sync(() => {
        const created = NodePerfHooks.monitorEventLoopDelay({
          resolution: LOOP_DELAY_RESOLUTION_MS,
        });
        created.enable();
        return created;
      }),
      (created) => Effect.sync(() => created.disable()),
    );
    // Each sample is the timer's whole period; the loop's delay is what exceeds it.
    const delayMs = (ns: number) => roundMs(Math.max(0, ns / 1e6 - LOOP_DELAY_RESOLUTION_MS));
    let elu = NodePerfHooks.performance.eventLoopUtilization();
    let cpu = process.cpuUsage();
    return Effect.sync(() => {
      const nextElu = NodePerfHooks.performance.eventLoopUtilization();
      const nextCpu = process.cpuUsage();
      const memory = process.memoryUsage();
      const readings: ProcessReadings = {
        loop: {
          delayP50Ms: delayMs(histogram.percentile(50)),
          delayP99Ms: delayMs(histogram.percentile(99)),
          delayMaxMs: delayMs(histogram.max),
          utilization:
            Math.round(
              NodePerfHooks.performance.eventLoopUtilization(nextElu, elu).utilization * 1000,
            ) / 1000,
        },
        cpu: {
          userMs: Math.round((nextCpu.user - cpu.user) / 1000),
          systemMs: Math.round((nextCpu.system - cpu.system) / 1000),
        },
        memory: {
          rssBytes: memory.rss,
          heapUsedBytes: memory.heapUsed,
          heapTotalBytes: memory.heapTotal,
        },
      };
      histogram.reset();
      elu = nextElu;
      cpu = nextCpu;
      return readings;
    });
  },
);

const GC_KIND_NAMES: ReadonlyMap<number, string> = new Map([
  [NodePerfHooks.constants.NODE_PERFORMANCE_GC_MINOR, "minor"],
  [NodePerfHooks.constants.NODE_PERFORMANCE_GC_MAJOR, "major"],
  [NodePerfHooks.constants.NODE_PERFORMANCE_GC_INCREMENTAL, "incremental"],
  [NodePerfHooks.constants.NODE_PERFORMANCE_GC_WEAKCB, "weakcb"],
]);

/** Count, then total and longest pause in ms. */
export type GcStat = readonly [count: number, totalMs: number, maxMs: number];

export const makeGcAggregate = () => {
  const kinds = new Map<string, { count: number; totalMs: number; maxMs: number }>();
  return {
    record(kind: number, durationMs: number): void {
      const name = GC_KIND_NAMES.get(kind) ?? "other";
      const stat = kinds.get(name) ?? { count: 0, totalMs: 0, maxMs: 0 };
      stat.count++;
      stat.totalMs += durationMs;
      if (durationMs > stat.maxMs) stat.maxMs = durationMs;
      kinds.set(name, stat);
    },
    drain(): Record<string, GcStat> {
      const drained = Object.fromEntries(
        Array.from(kinds, ([name, stat]) => [
          name,
          [stat.count, roundMs(stat.totalMs), roundMs(stat.maxMs)] as const,
        ]),
      );
      kinds.clear();
      return drained;
    },
  };
};

/** The process's garbage collections each minute, from perf_hooks' gc entries. */
const nodeGcSection: Effect.Effect<
  Effect.Effect<{ readonly gc: Record<string, GcStat> }>,
  never,
  Scope.Scope
> = Effect.gen(function* () {
  const gc = makeGcAggregate();
  yield* Effect.acquireRelease(
    Effect.sync(() => {
      const observer = new NodePerfHooks.PerformanceObserver((entries) => {
        for (const entry of entries.getEntries()) {
          // `detail.kind` is the GC type; this @types/node leaves `detail` off PerformanceEntry.
          const detail: unknown = Reflect.get(entry, "detail");
          const kind: unknown =
            typeof detail === "object" && detail !== null ? Reflect.get(detail, "kind") : undefined;
          gc.record(typeof kind === "number" ? kind : -1, entry.duration);
        }
      });
      observer.observe({ entryTypes: ["gc"] });
      return observer;
    }),
    (observer) => Effect.sync(() => observer.disconnect()),
  );
  return Effect.sync(() => ({ gc: gc.drain() }));
});

export interface SpanMinuteLogOptions {
  readonly filePath: string;
  /** The open file is closed before a line would take it past this. */
  readonly maxBytes: number;
  readonly maxArchives: number;
  readonly maxArchiveBytes: number;
}

const CLOSED_SUFFIX = /^\d{8}T\d{6}Z(\.gz)?$/;

const isoTime = (epochMs: number) => DateTime.formatIso(DateTime.makeUnsafe(epochMs));

const closedStamp = (epochMs: number) => `${isoTime(epochMs).slice(0, 19).replace(/[-:]/g, "")}Z`;

/** Appends lines; a closed file is gzipped to `<file>.<UTC close time>.gz`, oldest pruned first. */
export const makeSpanMinuteLog = Effect.fnUntraced(function* (options: SpanMinuteLogOptions) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = path.dirname(options.filePath);
  const prefix = `${path.basename(options.filePath)}.`;
  yield* fs.makeDirectory(directory, { recursive: true });

  const closedSuffixes = fs.readDirectory(directory).pipe(
    Effect.map((entries) =>
      entries
        .filter((entry) => entry.startsWith(prefix))
        .map((entry) => entry.slice(prefix.length))
        .toSorted(),
    ),
  );
  // A temporary name keeps a crash from leaving a truncated archive.
  const compress = Effect.fnUntraced(function* (closed: string) {
    const data = yield* fs.readFile(closed);
    const compressed = yield* Effect.callback<Uint8Array, Error>((resume) => {
      NodeZlib.gzip(data, (error, result) =>
        resume(error ? Effect.fail(error) : Effect.succeed(result)),
      );
    });
    yield* fs.writeFile(`${closed}.gz.tmp`, compressed);
    yield* fs.rename(`${closed}.gz.tmp`, `${closed}.gz`);
    yield* fs.remove(closed);
  });
  // Also finishes what a crash interrupted. An uncompressed file still counts towards the caps.
  const archive = Effect.gen(function* () {
    for (const suffix of yield* closedSuffixes) {
      const closed = `${options.filePath}.${suffix}`;
      if (suffix.endsWith(".gz.tmp")) yield* fs.remove(closed, { force: true });
      else if (CLOSED_SUFFIX.test(suffix) && !suffix.endsWith(".gz"))
        yield* compress(closed).pipe(Effect.ignore);
    }
    const kept = (yield* closedSuffixes).filter((suffix) => CLOSED_SUFFIX.test(suffix));
    const sizes = yield* Effect.forEach(kept, (suffix) =>
      fs.stat(`${options.filePath}.${suffix}`).pipe(Effect.map((info) => Number(info.size))),
    );
    let count = kept.length;
    let bytes = sizes.reduce((sum, size) => sum + size, 0);
    for (let index = 0; count > options.maxArchives || bytes > options.maxArchiveBytes; index++) {
      yield* fs.remove(`${options.filePath}.${kept[index]}`, { force: true });
      count--;
      bytes -= sizes[index]!;
    }
  });

  // A crash in an append can leave part of a line, which would corrupt the next one.
  const existing = yield* fs.readFile(options.filePath).pipe(
    Effect.catchIf(
      (cause) => cause.reason._tag === "NotFound",
      () => Effect.succeed(new Uint8Array(0)),
    ),
  );
  let size = existing.lastIndexOf(10) + 1;
  if (size < existing.length) yield* fs.truncate(options.filePath, size);
  yield* archive;

  // Set while a failed append may have left part of a line past `size`.
  let torn = false;
  // Set from a close until an archive pass succeeds, so a failed append cannot skip one.
  let archivePending = false;
  const repair = Effect.suspend(() => fs.truncate(options.filePath, size)).pipe(
    Effect.catchIf(
      (cause) => cause.reason._tag === "NotFound",
      () => Effect.void,
    ),
    Effect.tap(() =>
      Effect.sync(() => {
        torn = false;
      }),
    ),
  );

  const close = Effect.gen(function* () {
    const closed = `${options.filePath}.${closedStamp(yield* Clock.currentTimeMillis)}`;
    // Only a clock stepped back can repeat a name; the file then closes a minute later.
    if ((yield* fs.exists(closed)) || (yield* fs.exists(`${closed}.gz`))) return false;
    yield* fs.rename(options.filePath, closed);
    size = 0;
    return true;
  });
  const semaphore = yield* Semaphore.make(1);
  const append = (line: string) =>
    semaphore
      .withPermits(1)(
        Effect.gen(function* () {
          if (torn) yield* repair;
          const data = Buffer.from(line);
          if (size > 0 && size + data.length > options.maxBytes && (yield* close)) {
            archivePending = true;
          }
          yield* fs.writeFile(options.filePath, data, { flag: "a" }).pipe(
            Effect.tapError(() =>
              Effect.sync(() => {
                torn = true;
              }).pipe(Effect.andThen(repair), Effect.ignore),
            ),
          );
          size += data.length;
          if (archivePending) {
            yield* archive;
            archivePending = false;
          }
        }),
      )
      .pipe(Effect.uninterruptible);
  return { append };
});

export interface SpanMinuteLine extends ProcessReadings {
  readonly start: string;
  readonly end: string;
  readonly pid: number;
  readonly spans: Record<string, SpanStat>;
  readonly spanPercentiles: Record<string, SpanPercentiles>;
  /** Fields from the line's other sources, such as waits; each adds its own. */
  readonly [section: string]: unknown;
}

const quiet = <A, E>(effect: Effect.Effect<A, E>) =>
  effect.pipe(
    Effect.withTracerEnabled(false),
    Effect.updateContext(Context.omit(Tracer.ParentSpan)<never>),
  );

const encodeLine = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

/**
 * Counts every span by name and writes one line per wall-clock minute, plus the partial minute
 * when the scope closes. `record` runs as each span ends, so it stays O(1).
 */
export const makeWith = Effect.fnUntraced(function* (options: {
  readonly sampler: Effect.Effect<Effect.Effect<ProcessReadings>, never, Scope.Scope>;
  readonly append: (line: string) => Effect.Effect<void, PlatformError>;
  /** Drained into each line after the span fields, in order. */
  readonly sections?: ReadonlyArray<Effect.Effect<object>>;
}) {
  const aggregate = makeSpanAggregate();
  const sample = yield* options.sampler;
  let windowStart = yield* Clock.currentTimeMillis;
  let failing = false;
  const flush = Effect.gen(function* () {
    const end = yield* Clock.currentTimeMillis;
    const { spans, spanPercentiles } = aggregate.drain();
    const line: SpanMinuteLine = {
      start: isoTime(windowStart),
      end: isoTime(end),
      pid: process.pid,
      spans,
      ...(yield* sample),
      spanPercentiles,
      ...Object.assign({}, ...(yield* Effect.all(options.sections ?? []))),
    };
    windowStart = end;
    yield* encodeLine(line).pipe(
      Effect.flatMap((json) => options.append(`${json}\n`)),
      Effect.andThen(
        Effect.suspend(() => {
          if (!failing) return Effect.void;
          failing = false;
          return Effect.logInfo("Span minute writes recovered");
        }),
      ),
      Effect.catchCause((cause) => {
        if (failing) return Effect.void;
        failing = true;
        return Effect.logWarning("Span minute write failed; retrying each minute", cause);
      }),
    );
  }).pipe(Effect.uninterruptible, quiet);

  yield* Effect.addFinalizer(() => flush);
  yield* Effect.forkScoped(
    Effect.gen(function* () {
      while (true) {
        const now = yield* Clock.currentTimeMillis;
        yield* Effect.sleep(Duration.millis(MINUTE_MS - (now % MINUTE_MS)));
        yield* flush;
      }
    }).pipe(quiet),
  );
  return { record: aggregate.record };
});

export const make = Effect.fnUntraced(function* (
  filePath: string,
  sections: ReadonlyArray<Effect.Effect<object>> = [],
) {
  const log = yield* makeSpanMinuteLog({ filePath, ...SPAN_MINUTE_LOG_LIMITS });
  return yield* makeWith({
    sampler: nodeSampler,
    append: log.append,
    sections: [yield* nodeGcSection, ...sections],
  });
});

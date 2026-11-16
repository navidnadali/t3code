// @effect-diagnostics nodeBuiltinImport:off - node:perf_hooks has the event loop delay histogram; node:zlib gzips on libuv's thread pool.
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
// A busy server ends a few hundred span names a minute, a line of tens of KB: a 4 MiB file then
// closes every few hours and gzips to well under its size, so 48 archives hold several days. The byte cap bounds the disk for any names.
const ACTIVE_MAX_BYTES = 4 * 1024 * 1024;
const MAX_ARCHIVES = 48;
const MAX_ARCHIVE_BYTES = 48 * 1024 * 1024;

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

const bucketMidpointNs = (bucket: number) => {
  if (bucket < SUB_BUCKETS) return bucket + 0.5;
  const width = POWERS_OF_TWO[(bucket >>> SUB_BUCKET_BITS) - 1]!;
  return (SUB_BUCKETS + (bucket & (SUB_BUCKETS - 1))) * width + width / 2;
};

// Four significant digits below a second, whole ms above: finer than a p95 bucket's 1/16.
const roundMs = (ms: number) => (ms >= 1000 ? Math.round(ms) : Number(ms.toPrecision(4)));

interface Tally {
  count: number;
  totalNs: number;
  maxNs: number;
  readonly buckets: Uint32Array;
}

/** Count, then total, max and p95 duration in ms. */
export type SpanStat = readonly [count: number, totalMs: number, maxMs: number, p95Ms: number];

// Nearest rank: up to 19 spans that is the slowest, known exactly, as is anything in the last bucket.
const p95Ms = (tally: Tally) => {
  const rank = Math.ceil(tally.count * 0.95);
  let seen = 0;
  for (let bucket = 0; rank < tally.count && bucket < BUCKET_COUNT - 1; bucket++) {
    seen += tally.buckets[bucket]!;
    if (seen >= rank) return Math.min(bucketMidpointNs(bucket), tally.maxNs) / 1e6;
  }
  return tally.maxNs / 1e6;
};

export const makeSpanAggregate = () => {
  const tallies = new Map<string, Tally>();
  const addTally = (name: string): Tally => {
    const key = name.length > MAX_SPAN_NAME_LENGTH ? name.slice(0, MAX_SPAN_NAME_LENGTH) : name;
    const existing = tallies.get(key);
    if (existing !== undefined) return existing;
    const target = tallies.size < MAX_SPAN_NAMES ? key : OTHER_SPAN_NAME;
    const tally = tallies.get(target) ?? {
      count: 0,
      totalNs: 0,
      maxNs: 0,
      buckets: new Uint32Array(BUCKET_COUNT),
    };
    tallies.set(target, tally);
    return tally;
  };
  return {
    record(name: string, startTime: bigint, endTime: bigint): void {
      const durationNs = Math.max(0, Number(endTime - startTime));
      const tally = tallies.get(name) ?? addTally(name);
      tally.count++;
      tally.totalNs += durationNs;
      if (durationNs > tally.maxNs) tally.maxNs = durationNs;
      tally.buckets[durationNs < HISTOGRAM_LIMIT_NS ? bucketOf(durationNs) : BUCKET_COUNT - 1]!++;
    },
    /** Returns the minute's stats and starts the next minute; names idle for a minute are dropped. */
    drain(): Record<string, SpanStat> {
      const stats: Array<readonly [string, SpanStat]> = [];
      for (const [name, tally] of tallies) {
        if (tally.count === 0) {
          tallies.delete(name);
          continue;
        }
        stats.push([
          name,
          [
            tally.count,
            roundMs(tally.totalNs / 1e6),
            roundMs(tally.maxNs / 1e6),
            roundMs(p95Ms(tally)),
          ],
        ]);
        tally.count = 0;
        tally.totalNs = 0;
        tally.maxNs = 0;
        tally.buckets.fill(0);
      }
      return Object.fromEntries(stats);
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
}) {
  const aggregate = makeSpanAggregate();
  const sample = yield* options.sampler;
  let windowStart = yield* Clock.currentTimeMillis;
  let failing = false;
  const flush = Effect.gen(function* () {
    const end = yield* Clock.currentTimeMillis;
    const line: SpanMinuteLine = {
      start: isoTime(windowStart),
      end: isoTime(end),
      pid: process.pid,
      spans: aggregate.drain(),
      ...(yield* sample),
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

export const make = Effect.fnUntraced(function* (filePath: string) {
  const log = yield* makeSpanMinuteLog({
    filePath,
    maxBytes: ACTIVE_MAX_BYTES,
    maxArchives: MAX_ARCHIVES,
    maxArchiveBytes: MAX_ARCHIVE_BYTES,
  });
  return yield* makeWith({ sampler: nodeSampler, append: log.append });
});

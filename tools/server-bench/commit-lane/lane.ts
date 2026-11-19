// Commit-lane bench: streams live-tail events (gen-live.ts) through the server's own EventSink commit lane
// (ProviderCommitBatch, the writer worker, savepoints, projections) on a large, history-heavy database, at a fixed
// event rate, with a main-thread ballast at a target event-loop utilization. Reports the lane's busy ratio, flush
// costs, per-job cost, queue waits, event commit latency, and (with the bench hook applied to nodeSqliteClient)
// the writer worker's own time per statement shape against its round trip. Aggregates only.
// usage: node tools/server-bench/commit-lane.ts lane --db <db> --tails <ndjson> --out <json>
//          [--seconds 240] [--warmup 60] [--rate 37] [--threads 0=all] [--elu 0.95] [--short-ms 0.5]
//          [--long-ms 10] [--long-share 0.3] [--xlong-ms 40] [--xlong-share 0.05] [--cache-mb 0] [--seed 1]
// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalTimers:off globalJsonInEffect:off globalConsoleInEffect:off preferSchemaOverJson:off - Standalone bench tooling outside the Effect runtime; it owns its clock, timers and output.
import * as NodeFS from "node:fs";
import * as NodePerfHooks from "node:perf_hooks";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  OrchestrationV2DomainEventJson,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";

import * as EventSink from "../../../apps/server/src/orchestration-v2/EventSink.ts";
import * as EventStore from "../../../apps/server/src/orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../../apps/server/src/orchestration-v2/ProjectStore.ts";
import * as Sqlite from "../../../apps/server/src/persistence/Sqlite.ts";

const args = process.argv.slice(2);
const take = (name: string, fallback?: string) => {
  const at = args.indexOf(name);
  if (at < 0 || at + 1 >= args.length) {
    if (fallback !== undefined) return fallback;
    throw new Error(`missing ${name}`);
  }
  return args[at + 1] as string;
};
const dbPath = take("--db");
const tailsPath = take("--tails");
const outPath = take("--out");
const seconds = Number(take("--seconds", "240"));
const warmup = Number(take("--warmup", "60"));
const rate = Number(take("--rate", "37"));
const threadLimit = Number(take("--threads", "0"));
const targetElu = Number(take("--elu", "0.95"));
const shortMs = Number(take("--short-ms", "0.5"));
const longMs = Number(take("--long-ms", "10"));
const longShare = Number(take("--long-share", "0.3"));
const xlongMs = Number(take("--xlong-ms", "40"));
const xlongShare = Number(take("--xlong-share", "0.05"));
const cacheMb = Number(take("--cache-mb", "0"));
const snapshotRepeat = Number(take("--snapshot-repeat", "3"));
const providerThreadKeep = Number(take("--provider-thread-keep", "0.07"));
let seed = Number(take("--seed", "1")) >>> 0 || 1;
const random = () => {
  seed = (seed + 0x6d2b79f5) >>> 0;
  let t = seed;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};

// Domain events the provider ingestor commits as batched snapshots; every other one is a control (a barrier).
const SNAPSHOT = new Set(["node.updated", "message.updated", "turn-item.updated", "plan.updated"]);
const decodeEvent = Schema.decodeUnknownSync(OrchestrationV2DomainEventJson);
// The synth writes each item's final snapshot once and many provider-thread updates; production streams several
// snapshots per item. Each snapshot repeats (fresh event ids) and provider-thread updates are thinned to that mix.
type TailEvent = { readonly id: string; readonly type: string };
const toProductionMix = (events: ReadonlyArray<TailEvent>) =>
  events.flatMap((event) => {
    if (SNAPSHOT.has(event.type))
      return Array.from({ length: snapshotRepeat }, (_, copy) =>
        copy === 0 ? event : { ...event, id: `${event.id}:stream-${copy}` },
      );
    if (event.type === "provider-thread.updated")
      return random() < providerThreadKeep ? [event] : [];
    return [event];
  });
const tails = NodeFS.readFileSync(tailsPath, "utf8")
  .split("\n")
  .filter((line) => line.length > 0)
  .map((line) => JSON.parse(line) as { thread: number; items: number; events: Array<TailEvent> })
  .slice(0, threadLimit > 0 ? threadLimit : undefined)
  .map((tail) => ({
    ...tail,
    // The synth writes a run's events grouped by kind; production interleaves them in time.
    events: toProductionMix(tail.events)
      .map((event) => decodeEvent(event))
      .toSorted(
        (a, b) => DateTime.toEpochMillis(a.occurredAt) - DateTime.toEpochMillis(b.occurredAt),
      ),
  }));
const mix = new Map<string, number>();
for (const tail of tails)
  for (const event of tail.events) mix.set(event.type, (mix.get(event.type) ?? 0) + 1);
const totalTail = tails.reduce((acc, tail) => acc + tail.events.length, 0);
const needed = Math.ceil(rate * (seconds + warmup));
if (totalTail < needed) throw new Error(`tails hold ${totalTail} events, the run needs ${needed}`);

const nowMs = () => NodePerfHooks.performance.now();
let measureFrom = Number.POSITIVE_INFINITY;
let measureTo = Number.POSITIVE_INFINITY;
const inWindow = (startMs: number) => startMs >= measureFrom && startMs < measureTo;
const series = new Map<string, Array<number>>();
const push = (key: string, value: number) => {
  let values = series.get(key);
  if (values === undefined) series.set(key, (values = []));
  values.push(value);
};
const flushes: Array<{
  start: number;
  end: number;
  reason: string;
  items: number;
  waitMs: number;
}> = [];
const writerTransactions: Array<{ start: number; end: number }> = [];
// performance.timeOrigin-relative ms from a span's bigint ns clock (Effect uses the wall clock in ns).
const spanMs = (ns: bigint) => Number(ns) / 1e6 - NodePerfHooks.performance.timeOrigin;
const tracer = Tracer.make({
  span(options) {
    const span = new Tracer.NativeSpan(options);
    const end = span.end.bind(span);
    span.end = (endTime, exit) => {
      end(endTime, exit);
      const startMs = spanMs(span.status._tag === "Ended" ? span.status.startTime : endTime);
      if (!inWindow(startMs)) return;
      const durMs =
        span.status._tag === "Ended"
          ? Number(span.status.endTime - span.status.startTime) / 1e6
          : 0;
      const name = span.name;
      if (name === "orchestrationV2.ProviderCommitBatch.flush") {
        flushes.push({
          start: startMs,
          end: startMs + durMs,
          reason: String(span.attributes.get("orchestration_v2.batch_flush_reason") ?? "?"),
          items: Number(span.attributes.get("orchestration_v2.work_item_count") ?? 0),
          waitMs: Number(span.attributes.get("orchestration_v2.batch_wait_ms") ?? 0),
        });
      } else if (name === "sql.transaction") {
        if (span.attributes.get("db.connection") === "writer")
          writerTransactions.push({ start: startMs, end: startMs + durMs });
      } else if (
        name === "SAVEPOINT" ||
        name === "BEGIN" ||
        name === "COMMIT" ||
        name === "ROLLBACK"
      ) {
        push(`span ${name}`, durMs);
      } else if (name.startsWith("orchestrationV2.EventSink.")) {
        push(`span ${name}`, durMs);
      } else if (name === "sqlite") {
        const query = String(span.attributes.get("db.query.text") ?? "");
        push(`traced ${shapeOf(query)}`, durMs);
      }
    };
    return span;
  },
});
function shapeOf(sql: string): string {
  const text = sql.replace(/\s+/g, " ").trim();
  const write = /^(INSERT(?: OR \w+)? INTO|UPDATE|DELETE FROM|REPLACE INTO)\s+"?(\w+)/i.exec(text);
  if (write) {
    const verb = (write[1] as string).split(" ")[0]!.toUpperCase();
    const upsert = /ON CONFLICT/i.test(text) && /DO UPDATE/i.test(text) ? " UPSERT" : "";
    return `${verb}${upsert} ${write[2]}`;
  }
  const head = /^(SAVEPOINT|RELEASE|BEGIN|COMMIT|ROLLBACK|PRAGMA)\b/i.exec(text);
  if (head) return (head[1] as string).toUpperCase();
  const from = /\bFROM\s+"?(\w+)/i.exec(text);
  return `SELECT ${from ? from[1] : "?"}`;
}
// Bench hook (apply-bench-hooks.mjs): the writer worker's own time per statement, and the post-to-reply time.
(globalThis as Record<string, unknown>).__t3LaneBench = (
  role: string,
  sql: string,
  workerMs: number,
  roundTripMs: number,
) => {
  if (role !== "writer" || !inWindow(nowMs() - roundTripMs)) return;
  const shape = shapeOf(sql);
  push(`worker ${shape}`, workerMs);
  push(`roundtrip ${shape}`, roundTripMs);
  push("worker (all)", workerMs);
  push("roundtrip-minus-worker (all)", roundTripMs - workerMs);
  statements.push({ posted: nowMs() - roundTripMs, workerMs, roundTripMs, shape });
};
// Writer statements in the window, for attributing slow flushes; and the checkpoint worker's PASSIVE checkpoints
// (bench hook; wall-clock ms from the worker, moved to this clock).
const statements: Array<{ posted: number; workerMs: number; roundTripMs: number; shape: string }> =
  [];
const checkpoints: Array<{ start: number; end: number }> = [];
(globalThis as Record<string, unknown>).__t3LaneBenchCheckpoint = (
  wallStart: number,
  wallEnd: number,
) => {
  const start = wallStart - NodePerfHooks.performance.timeOrigin;
  if (inWindow(start))
    checkpoints.push({ start, end: wallEnd - NodePerfHooks.performance.timeOrigin });
};
// Process RSS (main thread, writer and reader workers, their SQLite page caches) once a second.
const rssSamples: Array<number> = [];
const rssTimer = setInterval(() => {
  if (inWindow(nowMs())) rssSamples.push(process.memoryUsage().rss / 2 ** 20);
}, 1000);

// Ballast: main-thread tasks of three sizes (short, long, extra long by share of busy time) at a target utilization.
// A writer reply waits behind the running task, as production's spans show.
let stopBallast = false;
const spin = (ms: number) => {
  const until = nowMs() + ms;
  while (nowMs() < until) {
    // busy
  }
};
const startBallast = () => {
  if (targetElu <= 0) return;
  // Probability of each size so that its share of busy time matches the request.
  const sizes = [
    { ms: shortMs, share: Math.max(0, 1 - longShare - xlongShare) },
    { ms: longMs, share: longShare },
    { ms: xlongMs, share: xlongShare },
  ].filter((size) => size.share > 0 && size.ms > 0);
  const weights = sizes.map((size) => size.share / size.ms);
  const totalWeight = weights.reduce((acc, weight) => acc + weight, 0);
  const pick = () => {
    let draw = random() * totalWeight;
    for (const [index, size] of sizes.entries()) {
      draw -= weights[index] as number;
      if (draw <= 0) return size.ms;
    }
    return (sizes.at(-1) as { ms: number }).ms;
  };
  // Integral control: the share of tasks followed by a 1 ms idle gap moves with the last 100 ms's error.
  let window = NodePerfHooks.performance.eventLoopUtilization();
  let windowAt = nowMs();
  let gapShare = 0.5;
  const task = () => {
    if (stopBallast) return;
    spin(pick());
    if (nowMs() - windowAt >= 100) {
      const utilization = NodePerfHooks.performance.eventLoopUtilization(window).utilization;
      window = NodePerfHooks.performance.eventLoopUtilization();
      windowAt = nowMs();
      gapShare = Math.min(1, Math.max(0, gapShare + 0.5 * (utilization - targetElu)));
    }
    if (random() < gapShare) setTimeout(task, 1);
    else setImmediate(task);
  };
  task();
};
const eluSamples: Array<number> = [];

const run = Effect.gen(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const queues = tails.map(() =>
    Queue.unbounded<{ type: string; event: OrchestrationV2DomainEvent; at: number }, Cause.Done>(),
  );
  const live = yield* Effect.forEach(queues, (queue) => queue);
  const latency: Array<number> = [];
  const consumers = yield* Effect.forEach(live, (queue) =>
    sink
      .consumeProviderEvents(
        Stream.fromQueue(queue),
        ({ event, at }) =>
          sink.write({ events: [event] }).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                if (inWindow(at)) latency.push(nowMs() - at);
              }),
            ),
          ),
        Effect.succeed(false),
        ({ event }) => Effect.succeed(SNAPSHOT.has(event.type)),
      )
      .pipe(Effect.forkChild),
  );
  const cursors = tails.map(() => 0);
  const open = tails.map((_, index) => index);
  const t0 = nowMs();
  measureFrom = t0 + warmup * 1000;
  measureTo = measureFrom + seconds * 1000;
  startBallast();
  let eluLast = NodePerfHooks.performance.eventLoopUtilization();
  let emitted = 0;
  let nextElu = t0 + 1000;
  while (nowMs() < measureTo && open.length > 0) {
    const due = Math.floor(((nowMs() - t0) / 1000) * rate);
    while (emitted < due && open.length > 0) {
      const slot = Math.floor(random() * open.length);
      const thread = open[slot] as number;
      const event = tails[thread]!.events[cursors[thread]!] as OrchestrationV2DomainEvent;
      cursors[thread]! += 1;
      if (cursors[thread] === tails[thread]!.events.length) open.splice(slot, 1);
      yield* Queue.offer(live[thread]!, { type: event.type, event, at: nowMs() });
      emitted += 1;
    }
    if (nowMs() >= nextElu) {
      const elu = NodePerfHooks.performance.eventLoopUtilization(eluLast);
      eluLast = NodePerfHooks.performance.eventLoopUtilization();
      if (inWindow(nowMs() - 1000)) eluSamples.push(elu.utilization);
      nextElu += 1000;
    }
    yield* Effect.sleep("5 millis");
  }
  const end = nowMs();
  for (const queue of live) yield* Queue.end(queue);
  yield* Effect.forEach(consumers, Fiber.join, { discard: true });
  stopBallast = true;
  return { emitted, latency, measuredSeconds: (Math.min(end, measureTo) - measureFrom) / 1000 };
});

const stats = (values: ReadonlyArray<number>) => {
  if (values.length === 0) return { n: 0 };
  const sorted = values.toSorted((a, b) => a - b);
  const at = (p: number) =>
    sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] as number;
  const total = sorted.reduce((acc, value) => acc + value, 0);
  return {
    n: sorted.length,
    total: Number(total.toFixed(1)),
    mean: Number((total / sorted.length).toFixed(3)),
    p50: Number(at(0.5).toFixed(3)),
    p90: Number(at(0.9).toFixed(3)),
    p99: Number(at(0.99).toFixed(3)),
    max: Number((sorted.at(-1) as number).toFixed(3)),
  };
};

const cpu0 = process.cpuUsage();
const result = await Effect.runPromise(
  // The tracer wraps the layer too: the lane's flush loop is forked while the layer is built.
  run.pipe(
    Effect.provide(
      EventSink.layer.pipe(
        Layer.provideMerge(
          Layer.mergeAll(EventStore.layer, ProjectionStore.layer, ProjectStore.layer),
        ),
        Layer.provideMerge(
          Sqlite.layerFromPath(dbPath, cacheMb > 0 ? { cacheSizeMb: cacheMb } : {}),
        ),
        Layer.provide(NodeServices.layer),
      ),
    ),
    Effect.scoped,
    Effect.withTracer(tracer),
  ),
);
const cpu = process.cpuUsage(cpu0);
clearInterval(rssTimer);

// Lane busy: the union of flush spans in the window, as the span attribution measures it.
const sorted = flushes.toSorted((a, b) => a.start - b.start);
let busy = 0;
let cs: number | undefined;
let ce = 0;
for (const flush of sorted) {
  if (cs === undefined) [cs, ce] = [flush.start, flush.end];
  else if (flush.start <= ce) ce = Math.max(ce, flush.end);
  else {
    busy += ce - cs;
    [cs, ce] = [flush.start, flush.end];
  }
}
if (cs !== undefined) busy += ce - cs;
const unionMs = (intervals: ReadonlyArray<{ start: number; end: number }>) => {
  let total = 0;
  let open: { start: number; end: number } | undefined;
  for (const interval of intervals.toSorted((a, b) => a.start - b.start)) {
    if (open === undefined) open = { ...interval };
    else if (interval.start <= open.end) open.end = Math.max(open.end, interval.end);
    else {
      total += open.end - open.start;
      open = { ...interval };
    }
  }
  return total + (open === undefined ? 0 : open.end - open.start);
};
const writerBusy = unionMs(writerTransactions);
const minutes = result.measuredSeconds / 60;
const byReason: Record<string, unknown> = {};
for (const reason of new Set(sorted.map((flush) => flush.reason))) {
  const group = sorted.filter((flush) => flush.reason === reason);
  byReason[reason] = {
    perMin: Number((group.length / minutes).toFixed(1)),
    laneSecondsPerMin: Number(
      (group.reduce((acc, f) => acc + f.end - f.start, 0) / 1000 / minutes).toFixed(2),
    ),
    itemsMean: Number((group.reduce((acc, f) => acc + f.items, 0) / group.length).toFixed(2)),
    flushMs: stats(group.map((flush) => flush.end - flush.start)),
    batchWaitMs: stats(group.map((flush) => flush.waitMs)),
  };
}
// The slowest 1% of flushes: their time split into the writer's own statement time, the rest of their round trips
// (event-loop wait), and everything else (main-thread work between statements); whether a PASSIVE checkpoint
// overlapped them; and the statement shapes with the most writer time in them.
function attributeSlowFlushes() {
  const byDuration = sorted.toSorted((a, b) => b.end - b.start - (a.end - a.start));
  if (byDuration.length === 0) return { count: 0 };
  const slow = byDuration.slice(0, Math.max(1, Math.ceil(byDuration.length / 100)));
  const postedOrder = statements.toSorted((a, b) => a.posted - b.posted);
  let total = 0;
  let worker = 0;
  let roundTrip = 0;
  let overlapped = 0;
  const shapes = new Map<string, number>();
  const reasons = new Map<string, number>();
  for (const flush of slow) {
    total += flush.end - flush.start;
    reasons.set(flush.reason, (reasons.get(flush.reason) ?? 0) + 1);
    for (const statement of postedOrder) {
      if (statement.posted < flush.start) continue;
      if (statement.posted > flush.end) break;
      worker += statement.workerMs;
      roundTrip += statement.roundTripMs - statement.workerMs;
      shapes.set(statement.shape, (shapes.get(statement.shape) ?? 0) + statement.workerMs);
    }
    if (checkpoints.some((c) => c.start < flush.end && c.end > flush.start)) overlapped += 1;
  }
  return {
    count: slow.length,
    thresholdMs: Number((slow.at(-1)!.end - slow.at(-1)!.start).toFixed(1)),
    meanMs: Number((total / slow.length).toFixed(1)),
    reasons: Object.fromEntries(reasons),
    meanItems: Number((slow.reduce((acc, f) => acc + f.items, 0) / slow.length).toFixed(1)),
    workerShare: Number((worker / total).toFixed(3)),
    roundTripWaitShare: Number((roundTrip / total).toFixed(3)),
    otherShare: Number((1 - (worker + roundTrip) / total).toFixed(3)),
    checkpointOverlapShare: Number((overlapped / slow.length).toFixed(3)),
    topWorkerShapes: [...shapes.entries()]
      .toSorted((a, b) => b[1] - a[1])
      .slice(0, 6)
      .map(([shape, ms]) => [shape, Number((ms / slow.length).toFixed(1))]),
  };
}
const snapshotFlushes = sorted.filter((flush) => flush.reason !== "control");
const snapshotJobs = snapshotFlushes.reduce((acc, f) => acc + f.items, 0);
const out = {
  tool: "commit-lane",
  args: {
    dbPath,
    tailsPath,
    seconds,
    warmup,
    rate,
    threads: tails.length,
    targetElu,
    shortMs,
    longMs,
    longShare,
    xlongMs,
    xlongShare,
    cacheMb,
  },
  tailMix: Object.fromEntries([...mix.entries()].toSorted((a, b) => b[1] - a[1])),
  emitted: result.emitted,
  measuredSeconds: result.measuredSeconds,
  laneBusyRatio: Number((busy / (result.measuredSeconds * 1000)).toFixed(4)),
  laneBusySecondsPerMin: Number((busy / 1000 / minutes).toFixed(2)),
  writerTransactionBusyRatio: Number((writerBusy / (result.measuredSeconds * 1000)).toFixed(4)),
  writerTransactionsPerMin: Number((writerTransactions.length / minutes).toFixed(1)),
  flushesPerMin: Number((sorted.length / minutes).toFixed(1)),
  byReason,
  snapshotJobsPerMin: Number((snapshotJobs / minutes).toFixed(1)),
  snapshotMsPerJob: Number(
    (
      snapshotFlushes.reduce((acc, f) => acc + f.end - f.start, 0) / Math.max(1, snapshotJobs)
    ).toFixed(3),
  ),
  eventLatencyMs: stats(result.latency),
  elu: stats(eluSamples),
  processCpuSecondsPerMin: Number(
    ((cpu.user + cpu.system) / 1e6 / ((seconds + warmup) / 60)).toFixed(2),
  ),
  rssMiB: stats(rssSamples),
  checkpoints: {
    perMin: Number((checkpoints.length / minutes).toFixed(1)),
    ms: stats(checkpoints.map((c) => c.end - c.start)),
  },
  slowFlushes: attributeSlowFlushes(),
  series: Object.fromEntries([...series.entries()].map(([key, values]) => [key, stats(values)])),
};
NodeFS.writeFileSync(outPath, `${JSON.stringify(out, null, 1)}\n`, { flag: "wx" });
console.log(
  `LANE-OK busy ${out.laneBusyRatio} (${out.laneBusySecondsPerMin} s/min), snapshot ${out.snapshotMsPerJob} ms/job, latency p50 ${(out.eventLatencyMs as { p50?: number }).p50} p99 ${(out.eventLatencyMs as { p99?: number }).p99} ms, elu mean ${(out.elu as { mean?: number }).mean}`,
);
process.exit(0);

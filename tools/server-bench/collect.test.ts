// @effect-diagnostics nodeBuiltinImport:off - Tests write real trace files, as the server does.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import {
  classifyThreads,
  knobsFromEnv,
  parseOtlpGauges,
  parseProcIo,
  parseProcStat,
  parsePsTime,
  readTraceWindow,
  summariseGauges,
  summariseSpans,
  summariseWindow,
  type GaugePoint,
  type IoCounters,
  type Snapshot,
  type ThreadSample,
} from "./collect.ts";

const MIB = 1024 * 1024;
const PID = 1000;

const io = (rchar: number, wchar: number, syscr = 0, syscw = 0): IoCounters => ({
  rchar,
  wchar,
  syscr,
  syscw,
  read_bytes: 0,
  write_bytes: 0,
});

const thread = (
  tid: number,
  comm: string,
  starttime: number,
  counters: IoCounters | null,
  cpu: { readonly userS?: number; readonly sysS?: number } = {},
): ThreadSample => ({
  tid,
  comm,
  starttime,
  userS: cpu.userS ?? 0,
  sysS: cpu.sysS ?? 0,
  io: counters,
});

const roles = (threads: ReadonlyArray<ThreadSample>) =>
  Object.fromEntries(
    [...classifyThreads(PID, threads)].map(([key, label]) => [key.split(":")[0], label]),
  );

describe("parseProcStat", () => {
  it("reads a comm with spaces and parentheses and converts ticks to seconds", () => {
    const fields = Array.from({ length: 40 }, (_, index) => String(index));
    fields[0] = "R";
    fields[11] = "250";
    fields[12] = "50";
    fields[19] = "123456";
    const parsed = parseProcStat(`42 (node (worker) 1) ${fields.join(" ")}\n`, 100);

    expect(parsed).toEqual({
      comm: "node (worker) 1",
      state: "R",
      userS: 2.5,
      sysS: 0.5,
      starttime: 123456,
    });
  });

  it("reads /proc io counters and ps CPU times", () => {
    expect(
      parseProcIo(
        "rchar: 10\nwchar: 20\nsyscr: 3\nsyscw: 4\nread_bytes: 5\nwrite_bytes: 6\ncancelled_write_bytes: 0\n",
      ),
    ).toEqual({ rchar: 10, wchar: 20, syscr: 3, syscw: 4, read_bytes: 5, write_bytes: 6 });
    expect(parseProcIo("")).toBeNull();
    expect(parsePsTime("0:01.50")).toBe(1.5);
    expect(parsePsTime("1:02:03.25")).toBe(3723.25);
    expect(parsePsTime("2-00:00:01")).toBe(172_801);
    expect(parsePsTime("garbage")).toBeNull();
  });
});

describe("classifyThreads", () => {
  it("finds the checkpoint worker by its copy pattern and labels the readers", () => {
    expect(
      roles([
        thread(PID, "node-MainThread", 10, io(5 * MIB, 50 * MIB)),
        thread(1001, "WorkerThread", 20, io(40 * MIB, 0)),
        thread(1002, "WorkerThread", 30, io(200 * MIB, 190 * MIB)),
        thread(1003, "WorkerThread", 40, io(80 * MIB, 4096)),
        thread(1004, "node-V8Worker", 11, io(0, 0)),
      ]),
    ).toEqual({
      [PID]: { role: "main", basis: "pid" },
      1001: { role: "sqlite-reader", basis: "io-pattern" },
      1002: { role: "checkpoint", basis: "io-pattern" },
      1003: { role: "sqlite-reader", basis: "io-pattern" },
      1004: { role: "other", basis: "comm" },
    });
  });

  it("falls back to the earliest worker that is not reading like a reader", () => {
    expect(
      roles([
        thread(PID, "node-MainThread", 10, io(0, 0)),
        thread(1001, "WorkerThread", 20, io(30 * MIB, 0)),
        thread(1002, "WorkerThread", 25, io(8192, 0)),
        thread(1003, "WorkerThread", 30, io(4096, 0)),
        thread(1004, "WorkerThread", 35, io(10 * MIB, 30 * MIB)),
      ]),
    ).toEqual({
      [PID]: { role: "main", basis: "pid" },
      1001: { role: "sqlite-reader", basis: "io-pattern" },
      1002: { role: "checkpoint", basis: "start-order" },
      1003: { role: "sqlite-reader", basis: "default" },
      1004: { role: "other", basis: "io-pattern" },
    });
  });
});

const snapshot = (
  second: number,
  processIo: IoCounters,
  threads: ReadonlyArray<ThreadSample>,
  rssKb: number,
): Snapshot => ({
  mono: second * 1000,
  wall: 1_793_000_000_000 + second * 1000,
  source: "proc",
  userS: threads.reduce((sum, entry) => sum + entry.userS, 0),
  sysS: threads.reduce((sum, entry) => sum + entry.sysS, 0),
  io: processIo,
  threads: new Map(threads.map((entry) => [`${entry.tid}:${entry.starttime}`, entry])),
  rssKb,
  hwmKb: 4096 * 1024,
});

describe("summariseWindow", () => {
  it("separates the main thread, the readers and the process-wide counters", () => {
    const main = (second: number) =>
      thread(
        PID,
        "node-MainThread",
        10,
        io(second * 1000, second * 2000, second * 300, second * 800),
        {
          userS: second * 0.25,
          sysS: second * 0.4,
        },
      );
    const reader = (second: number) =>
      thread(1001, "WorkerThread", 20, io(10 * MIB + second * MIB, 0, second * 4000, 0), {
        userS: second * 0.02,
        sysS: second * 0.03,
      });
    const snapshots = [0, 1, 2, 3, 4].map((second) =>
      snapshot(
        second,
        // Reaped children add 1,000 read syscalls/s on top of the live threads.
        io(0, 0, second * (300 + 4000 + 1000), second * 800),
        [main(second), reader(second)],
        (second === 2 ? 3000 : 2000) * 1024,
      ),
    );
    const summary = summariseWindow(PID, snapshots, {
      startMs: 1_793_000_000_000,
      endMs: 1_793_000_004_000,
    });

    expect(summary.problems).toEqual([]);
    expect(summary.elapsed_s).toBe(4);
    expect(summary.cpu.main).toEqual({ user_pct: 25, sys_pct: 40, busy_pct: 65 });
    expect(summary.cpu.roles?.["sqlite-reader"]).toEqual({
      user_pct: 2,
      sys_pct: 3,
      busy_pct: 5,
      threads: 1,
    });
    expect(summary.cpu.main_busy_1s).toEqual({ p50_pct: 65, p95_pct: 65, max_pct: 65 });
    expect(summary.io.main?.syscr_per_s).toBe(300);
    expect(summary.io.main?.syscw_per_s).toBe(800);
    expect(summary.io.threads_sum?.syscr_per_s).toBe(4300);
    expect(summary.io.process?.syscr_per_s).toBe(5300);
    expect(summary.io.roles?.["sqlite-reader"]?.syscr_per_s).toBe(4000);
    expect(summary.mem.rss_peak_mib).toBe(3000);
  });

  it("counts a thread that starts mid-window from zero", () => {
    const main = thread(PID, "node-MainThread", 10, io(0, 0), { userS: 1 });
    const late = (userS: number, syscr: number) =>
      thread(1001, "WorkerThread", 500, io(2 * MIB, 0, syscr, 0), { userS });
    const snapshots = [
      snapshot(0, io(0, 0), [main], 1024),
      // Born between the first two samples: 0.8 s CPU and 10,000 reads before it was seen.
      snapshot(1, io(0, 0, 10_000), [main, late(0.8, 10_000)], 1024),
      snapshot(2, io(0, 0, 10_000), [main, late(0.8, 10_000)], 1024),
    ];
    const summary = summariseWindow(PID, snapshots, {
      startMs: 1_793_000_000_000,
      endMs: 1_793_000_002_000,
    });

    expect(summary.cpu.roles?.["sqlite-reader"]).toEqual({
      user_pct: 40,
      sys_pct: 0,
      busy_pct: 40,
      threads: 1,
    });
    expect(summary.io.threads_sum?.syscr_per_s).toBe(5000);
  });

  it("leaves per-thread rows null when only ps data exists", () => {
    const ps = (second: number): Snapshot => ({
      mono: second * 1000,
      wall: 1_793_000_000_000 + second * 1000,
      source: "ps",
      userS: second * 0.5,
      sysS: second * 0.25,
      io: null,
      threads: null,
      rssKb: 1024 * 1024,
      hwmKb: null,
    });
    const summary = summariseWindow(PID, [ps(0), ps(1), ps(2)], {
      startMs: 1_793_000_000_000,
      endMs: 1_793_000_002_000,
    });

    expect(summary.cpu.process).toEqual({ user_pct: 50, sys_pct: 25, busy_pct: 75 });
    expect(summary.cpu.main).toBeNull();
    expect(summary.io.main).toBeNull();
    expect(summary.io.process).toBeNull();
    expect(summary.problems).toEqual(["per_thread_unavailable"]);
  });
});

const T0 = 1_793_000_000_000;

const spanLine = (name: string, endMs: number, durationMs: number, attributes = {}) =>
  `${JSON.stringify({
    type: "effect-span",
    name,
    startTimeUnixNano: String(BigInt(Math.round((endMs - durationMs) * 1e6))),
    endTimeUnixNano: String(BigInt(Math.round(endMs * 1e6))),
    durationMs,
    attributes,
    exit: { _tag: "Success" },
  })}\n`;

const withTraceDir = async (run: (trace: string) => Promise<void>) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-trace-"));
  try {
    await run(NodePath.join(directory, "server.trace.ndjson"));
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
};

describe("readTraceWindow", () => {
  it("reads the rotated files once each and keeps only spans ending in the window", async () => {
    await withTraceDir(async (trace) => {
      // .3 is oldest; .2 is a second name for .1, as when a rotation is caught mid-rename.
      NodeFS.writeFileSync(`${trace}.3`, spanLine("old", T0 - 5000, 1));
      let rotated = "";
      for (let index = 0; index < 100; index++) {
        rotated += spanLine("sqlite", T0 + 10 + index, index + 1, {
          "db.connection": "reader",
          "db.lock_wait_ms": (index + 1) / 2,
          "db.lock_owner_at_start.name": "ProjectionStore.getThreadShell",
        });
      }
      NodeFS.writeFileSync(`${trace}.1`, rotated);
      NodeFS.linkSync(`${trace}.1`, `${trace}.2`);
      NodeFS.writeFileSync(
        trace,
        spanLine("McpHttpServer.authenticateRequest", T0 + 500, 1000) +
          spanLine("late", T0 + 60_000, 1) +
          "not json\n",
      );
      const read = await readTraceWindow(trace, { startMs: T0, endMs: T0 + 1000 });
      const summary = summariseSpans(read.spans);

      expect(read.valid).toBe(true);
      expect(read.files.map((file) => file.name)).toEqual([
        "server.trace.ndjson",
        "server.trace.ndjson.1",
        "server.trace.ndjson.3",
      ]);
      expect(read.bad_lines).toBe(1);
      expect(summary.spans).toBe(101);
      // The prod-sample.sh selftest numbers: same definitions, same results.
      expect(summary.sql["sqlite|reader"]).toEqual({
        count: 100,
        errors: 0,
        total_ms: 5050,
        avg_ms: 50.5,
        p50_ms: 50,
        p95_ms: 95,
        max_ms: 100,
        lock_wait_n: 100,
        lock_wait_avg_ms: 25.25,
        lock_wait_p50_ms: 25,
        lock_wait_p95_ms: 47.5,
        lock_wait_max_ms: 50,
      });
      expect(summary.by_name["McpHttpServer.authenticateRequest"]?.p95_ms).toBe(1000);
      expect(summary.lock_owners[0]).toEqual({
        connection: "reader",
        owner: "ProjectionStore.getThreadShell",
        count: 100,
        total_wait_ms: 2525,
        max_wait_ms: 50,
      });
    });
  });

  it("rejects a trace that stops before the window ends", async () => {
    await withTraceDir(async (trace) => {
      let lines = "";
      // Spans from 10 s before the window to 60 s into a 120 s window, then nothing.
      for (let second = -10; second <= 60; second++) {
        lines += spanLine("sql.transaction", T0 + second * 1000, 5, { "db.connection": "writer" });
      }
      NodeFS.writeFileSync(trace, lines);
      const partial = await readTraceWindow(trace, { startMs: T0, endMs: T0 + 120_000 });
      const old = await readTraceWindow(trace, { startMs: T0 + 300_000, endMs: T0 + 400_000 });

      expect(partial.valid).toBe(false);
      expect(partial.problems).toEqual(["trace_stale"]);
      expect(partial.spans).toEqual([]);
      expect(old.problems).toEqual(["trace_stale"]);
      expect(old.spans).toEqual([]);
    });
  });

  it("rejects a window that starts before the oldest file on disk", async () => {
    await withTraceDir(async (trace) => {
      NodeFS.writeFileSync(
        trace,
        spanLine("sql.transaction", T0 + 500, 5, { "db.connection": "writer" }),
      );
      const read = await readTraceWindow(trace, { startMs: T0, endMs: T0 + 1000 }, { attempts: 1 });

      expect(read.valid).toBe(false);
      expect(read.problems).toEqual(["trace_coverage"]);
      expect(read.spans).toEqual([]);
    });
  });
});

describe("knobsFromEnv", () => {
  it("keeps different long values apart and never shows a secret", () => {
    const a = knobsFromEnv({
      BENCH_TRANSCRIPT: "/srv/bench/transcripts/large-tool-payload.ndjson",
    });
    const b = knobsFromEnv({
      BENCH_TRANSCRIPT: "/srv/bench/transcripts/simple-turn-replay.ndjson",
    });

    expect(a.BENCH_TRANSCRIPT).toMatch(/^<redacted sha256:[0-9a-f]{12}>$/);
    expect(a.BENCH_TRANSCRIPT).not.toBe(b.BENCH_TRANSCRIPT);
    expect(
      knobsFromEnv({ T3CODE_DEV_AUTH_TOKEN: "x".repeat(40), T3CODE_LOG_LEVEL: "Info" }),
    ).toEqual({
      T3CODE_DEV_AUTH_TOKEN: "<redacted>",
      T3CODE_LOG_LEVEL: "Info",
    });
  });
});

describe("summariseGauges", () => {
  const exportAt = (second: number, values: Record<string, number>) =>
    parseOtlpGauges(
      {
        resourceMetrics: [
          {
            scopeMetrics: [
              {
                metrics: Object.entries(values).map(([name, value]) => ({
                  name,
                  gauge: {
                    dataPoints: [
                      { asDouble: value, timeUnixNano: String((T0 + second * 1000) * 1e6) },
                    ],
                  },
                })),
              },
            ],
          },
        ],
      },
      T0 + second * 1000,
    );

  it("weights each server sample by the time it covers, inside the window only", () => {
    const sample = (elu: number, rssMib: number) => ({
      t3_server_event_loop_utilization: elu,
      t3_server_event_loop_delay_max: elu * 1000,
      t3_server_cpu_utilization: elu,
      t3_server_resident_bytes: rssMib * MIB,
      t3_server_heap_used_bytes: (rssMib / 2) * MIB,
      t3_rpc_requests_total: 5,
    });
    // Timer samples at 0, 30 and 70 s, a client-connect sample at 40 s; every 1 s export repeats
    // the latest values.
    const changes: ReadonlyArray<readonly [number, number, number]> = [
      [0, 0.8, 100],
      [30, 0.5, 400],
      [40, 0.9, 200],
      [70, 0.1, 300],
    ];
    const points: Array<GaugePoint> = [];
    for (let second = 0; second < 100; second++) {
      const [, elu, rss] = changes.findLast(([at]) => at <= second)!;
      points.push(...exportAt(second, sample(elu, rss)));
    }
    const summary = summariseGauges(points, { startMs: T0 + 10_000, endMs: T0 + 95_000 });

    expect(summary.exports).toBe(100);
    expect(summary.samples).toBe(4);
    // 30-40 s at 0.9 and 40-70 s at 0.1; the 0-30 s sample starts before the window.
    expect(summary.intervals_in_window).toBe(2);
    expect(summary.elu_mean).toBe(0.3);
    expect(summary.elu_max).toBe(0.9);
    expect(summary.event_loop_delay_max_ms).toBe(900);
    expect(summary.rss_peak_mib).toBe(400);
    expect(summary.heap_used_peak_mib).toBe(200);
  });

  it("returns null, not 0, without samples", () => {
    const summary = summariseGauges([], { startMs: T0, endMs: T0 + 60_000 });

    expect(summary.elu_mean).toBeNull();
    expect(summary.rss_peak_mib).toBeNull();
    expect(summary.samples_in_window).toBe(0);
  });
});

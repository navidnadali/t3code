import { describe, expect, it } from "vite-plus/test";

import {
  aggregateRuns,
  comparabilityProblems,
  compareResults,
  formatNumber,
  renderComparison,
  renderResult,
  tableRows,
  valueAt,
  type BenchResult,
  type MetricTree,
} from "./report.ts";

const BASE_SHA = "15fe2832727893cbdd2214383f9f971474849695";

const metrics = (scale: number): MetricTree => ({
  cpu: {
    main: { user_pct: 21.25 * scale, sys_pct: 39.5 * scale, busy_pct: 60.75 * scale },
    process: { user_pct: 30, sys_pct: 45, busy_pct: 75 },
    roles: { "sqlite-reader": { user_pct: 2.7, sys_pct: 3.6, busy_pct: 6.3, threads: 4 } },
  },
  io: {
    main: {
      syscr_per_s: 230.2,
      syscw_per_s: 733 * scale,
      rchar_mb_s: 0.965,
      wchar_mb_s: 1.463,
      read_bytes_mb_s: 0.08,
    },
    threads_sum: {
      syscr_per_s: 4765.2,
      syscw_per_s: 1398.3,
      rchar_mb_s: 19.5,
      wchar_mb_s: 4.3,
      read_bytes_mb_s: 0.18,
    },
    process: {
      syscr_per_s: 7792.6,
      syscw_per_s: 1454.6,
      rchar_mb_s: 22.4,
      wchar_mb_s: 4.6,
      read_bytes_mb_s: 0.18,
    },
    roles: null,
  },
  mem: { rss_peak_mib: 2150.4, hwm_mib: 2200 },
  otlp: {
    elu_mean: 0.712,
    elu_max: 0.9,
    event_loop_delay_max_ms: 1840,
    heap_used_peak_mib: 1536.5,
  },
  trace: { spans: 152_340, errors: 3 },
  spans: {
    "OrchestratorV2.getThreadShell": {
      count: 900,
      p50_ms: 120,
      p95_ms: 812.4 * scale,
      max_ms: 2000,
    },
    "McpHttpServer.authenticateRequest": { count: 40, p50_ms: 900, p95_ms: 9120, max_ms: 39_000 },
  },
  sql: {
    "sql.transaction|writer": {
      count: 5000,
      p50_ms: 66.5 * scale,
      p95_ms: 274,
      lock_wait_p50_ms: 60.1,
      lock_wait_p95_ms: 254,
    },
    "sql.transaction|reader": {
      count: 2460,
      p50_ms: 441,
      p95_ms: 2900,
      lock_wait_p50_ms: 225,
      lock_wait_p95_ms: 2300,
    },
  },
  driver: {
    events_per_s: 412.5,
    calls: {
      "http.environment": { count: 600, p50_ms: 520, p95_ms: 4400 },
      "mcp.t3_project_list": { count: 120, p50_ms: 950, p95_ms: 9800 * scale },
    },
  },
});

const result = (
  label: string,
  scale: number,
  overrides: Partial<BenchResult> = {},
): BenchResult => ({
  schema: "t3-server-bench/result/1",
  label,
  created_at: "2026-10-31T21:00:00.000Z",
  identity: {
    server_sha: scale === 1 ? BASE_SHA : "9fc90220c0000000000000000000000000000000",
    server_ref: scale === 1 ? "perf/base" : "perf/p1-012",
    server_dirty: false,
    server_version: "t3 0.1.6",
    base_sha: BASE_SHA,
    base_ref: "perf/base",
    harness_sha: "aaaaaaaa",
    fixture: { kind: "file", sha256: "ab".repeat(32), bytes: 1024 },
  },
  sampling: {
    trace_min_level: "Info",
    trace_sql_min_duration_ms: "0",
    trace_timing_enabled: "true",
    otlp_export_interval_ms: 1000,
    collect_interval_ms: 1000,
  },
  knobs: {
    server: { T3CODE_TRACE_MAX_BYTES: "67108864" },
    workload: {
      driver: "driver.ts",
      driver_sha256: "cd".repeat(32),
      warmup_s: 60,
      window_s: 300,
      driver_args: ["--sessions", "80"],
    },
    run: { runs: 3 },
  },
  host: {},
  runs: { requested: 3, attempted: 3, valid: 3 },
  metrics: metrics(scale),
  spread: { min: metrics(scale), max: metrics(scale) },
  run_details: [],
  ...overrides,
});

const before = result("perf-base", 1, {
  spread: {
    min: { cpu: { main: { user_pct: 20.1 } } },
    max: { cpu: { main: { user_pct: 22 } } },
  },
});
const after = result("p1-012", 0.5);

const line = (markdown: string, label: string) =>
  markdown.split("\n").find((row) => row.startsWith(`| ${label} |`));

describe("renderComparison", () => {
  it("renders the medians of the raw JSON in every row", () => {
    const markdown = renderComparison(before, after);

    expect(line(markdown, "Main thread CPU % user")).toBe(
      "| Main thread CPU % user | 21.3 (20.1–22) | 10.6 | -10.6 (-50%) |",
    );
    expect(line(markdown, "`sql.transaction` writer p50 ms")).toBe(
      "| `sql.transaction` writer p50 ms | 66.5 | 33.3 | -33.3 (-50%) |",
    );
    expect(line(markdown, "Client `mcp.t3_project_list` p95 ms")).toBe(
      "| Client `mcp.t3_project_list` p95 ms | 9,800 | 4,900 | -4,900 (-50%) |",
    );
    expect(line(markdown, "Process read syscalls/s (incl. reaped children)")).toBe(
      "| Process read syscalls/s (incl. reaped children) | 7,793 | 7,793 | 0 (0.0%) |",
    );
    const rows = tableRows([before, after]);
    expect(rows.length).toBe(
      markdown.split("\n").filter((row) => /^\| (?!Metric|---)/.test(row)).length,
    );
    for (const row of rows) {
      const cells = line(markdown, row.label)?.split(" | ");
      expect(cells?.[1]?.split(" (")[0]).toBe(formatNumber(valueAt(before.metrics, row.path)));
      expect(cells?.[2]?.split(" (")[0]).toBe(formatNumber(valueAt(after.metrics, row.path)));
    }
  });

  it("marks rows without trace or gauge data n/a, never 0", () => {
    const missing = result("no-trace", 0.5, {
      metrics: { ...metrics(0.5), trace: null, spans: null, sql: null, otlp: { elu_mean: null } },
    });
    const markdown = renderComparison(before, missing);

    for (const label of [
      "`sql.transaction` writer p50 ms",
      "`sql.transaction` reader lock wait p95 ms",
      "`OrchestratorV2.getThreadShell` p95 ms",
      "`McpHttpServer.authenticateRequest` p95 ms",
      "ELU (server gauge, mean)",
      "Spans in window",
    ]) {
      const cells = line(markdown, label)?.split(" | ");
      expect(cells?.[2]).toBe("n/a");
      expect(cells?.[3]).toBe("n/a |");
    }
    expect(line(renderResult(missing), "`sql.transaction` writer p95 ms")).toBe(
      "| `sql.transaction` writer p95 ms | n/a |",
    );
    expect(formatNumber(null)).toBe("n/a");
    expect(formatNumber(0)).toBe("0");
  });
});

describe("comparabilityProblems", () => {
  it("accepts results on the same base, fixture, sampling and workload", () => {
    expect(comparabilityProblems(before, after)).toEqual([]);
    expect(compareResults(before, after).ok).toBe(true);
  });

  it("refuses a different base SHA, fixture or trace sampling", () => {
    const otherBase = result("other-base", 0.5, {
      identity: { ...after.identity, base_sha: "0000000000000000000000000000000000000000" },
    });
    const otherFixture = result("other-fixture", 0.5, {
      identity: {
        ...after.identity,
        fixture: { kind: "file", sha256: "ef".repeat(32), bytes: 1024 },
      },
    });
    const filtered = result("filtered", 0.5, {
      sampling: { ...after.sampling, trace_sql_min_duration_ms: "50" },
    });
    const warnOnly = result("warn", 0.5, {
      sampling: { ...after.sampling, trace_min_level: "Warn" },
    });

    expect(comparabilityProblems(before, otherBase)).toEqual([
      `base SHA differs: ${BASE_SHA} vs 0000000000000000000000000000000000000000`,
    ]);
    expect(comparabilityProblems(before, otherFixture)).toEqual([
      `fixture differs: ${"ab".repeat(32)} vs ${"ef".repeat(32)}`,
    ]);
    expect(comparabilityProblems(before, filtered)).toEqual([
      'trace sampling differs: trace_sql_min_duration_ms "0" vs "50"',
    ]);
    expect(comparabilityProblems(before, warnOnly)).toEqual([
      'trace sampling differs: trace_min_level "Info" vs "Warn"',
    ]);
    const outcome = compareResults(before, otherBase);
    expect(outcome.ok).toBe(false);
  });

  it("refuses results that do not record what they ran on", () => {
    const unrecorded = result("old", 0.5, {
      identity: { ...after.identity, base_sha: null, fixture: null },
      sampling: null,
    });

    expect(comparabilityProblems(before, unrecorded)).toEqual([
      "base SHA not recorded",
      "fixture hash not recorded",
      "trace sampling not recorded",
    ]);
  });

  it("refuses a different workload", () => {
    const lighter = result("lighter", 0.5, {
      knobs: {
        ...after.knobs,
        workload: { ...after.knobs.workload, driver_args: ["--sessions", "40"] },
      },
    });

    expect(comparabilityProblems(before, lighter)).toEqual([
      'workload differs: driver_args ["--sessions","80"] vs ["--sessions","40"]',
    ]);
  });
});

describe("aggregateRuns", () => {
  it("takes the median per leaf and keeps a leaf null unless every run has it", () => {
    const { metrics: median, spread } = aggregateRuns([
      { cpu: { main: { user_pct: 10 } }, sql: { writer: { p95_ms: 5 } } },
      { cpu: { main: { user_pct: 30 } }, sql: null },
      { cpu: { main: { user_pct: 20 } }, sql: { writer: { p95_ms: 7 } } },
    ]);

    expect(median).toEqual({ cpu: { main: { user_pct: 20 } }, sql: null });
    expect(spread.min).toEqual({ cpu: { main: { user_pct: 10 } }, sql: null });
    expect(spread.max).toEqual({ cpu: { main: { user_pct: 30 } }, sql: null });
  });
});

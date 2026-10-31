// @effect-diagnostics nodeBuiltinImport:off globalConsole:off preferSchemaOverJson:off - standalone bench report outside the Effect runtime.
import * as NodeFS from "node:fs";

export const HELP = `Usage:
  node tools/server-bench/report.ts compare <before.result.json> <after.result.json>
  node tools/server-bench/report.ts show <result.json>

compare  Prints one markdown table: the median of the valid runs per metric, its min–max across
         runs, and after − before. It refuses (exit 2) results that differ in, or lack, any of:
         base SHA; fixture hash; trace sampling (T3CODE_TRACE_MIN_LEVEL,
         T3CODE_TRACE_SQL_MIN_DURATION_MS, T3CODE_TRACE_TIMING_ENABLED, OTLP export interval);
         workload (driver files, its arguments, the BENCH_* load knobs); host (hostname
         fingerprint, CPU model); Node version; harness (git SHA and the harness files' hash);
         server entry (built dist vs source) and the server/driver CPU pinning. Server knobs may
         differ (that is what a knob A/B measures); the ones that do, and any other run setting
         that differs, are listed under the table.
show     The same table for one result.

A metric without data (no spans of that kind, no per-thread data on macOS, a trace that does not
cover the window) renders n/a, never 0. result.json files are written by run.ts.`;

export const RESULT_SCHEMA = "t3-server-bench/result/1";

export type MetricTree = { readonly [key: string]: MetricTree | number | null };

export interface BenchResult {
  readonly schema: string;
  readonly label: string;
  readonly created_at: string;
  readonly identity: {
    readonly server_sha: string | null;
    readonly server_ref: string | null;
    readonly server_dirty: boolean | null;
    readonly server_tree?: string | null;
    readonly server_version: string | null;
    readonly base_sha: string | null;
    readonly base_ref: string | null;
    readonly harness_sha: string | null;
    /** sha256 of the harness files themselves, which also catches uncommitted edits. */
    readonly harness_files_sha256: string | null;
    readonly fixture: {
      readonly kind: "file" | "dir" | "empty";
      readonly sha256: string | null;
      readonly bytes: number | null;
    } | null;
  };
  /** Settings that decide which spans and samples exist. */
  readonly sampling: Readonly<Record<string, string | number | null>> | null;
  readonly knobs: {
    readonly server: Readonly<Record<string, string>>;
    readonly workload: Readonly<Record<string, unknown>> | null;
    readonly run: Readonly<Record<string, unknown>>;
  };
  readonly host: Readonly<Record<string, unknown>>;
  readonly runs: {
    readonly requested: number;
    readonly attempted: number;
    readonly valid: number;
    /** `<label>#<attempt>` in the order the attempts ran, all refs interleaved. */
    readonly order?: ReadonlyArray<string>;
  };
  readonly metrics: MetricTree;
  readonly spread: { readonly min: MetricTree; readonly max: MetricTree };
  readonly run_details: ReadonlyArray<unknown>;
}

const isTree = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * Merges the runs' metric trees leaf by leaf. A leaf is reduced only when every run has a
 * finite number there; otherwise it is null, so a value missing from one run never turns into
 * a number.
 */
export function aggregate(
  values: ReadonlyArray<unknown>,
  reduce: (numbers: Array<number>) => number,
): MetricTree | number | null {
  if (values.length === 0) return null;
  if (values.every((value) => typeof value === "number" && Number.isFinite(value))) {
    return round(reduce(values as Array<number>));
  }
  if (!values.every(isTree)) return null;
  const keys = new Set(values.flatMap((value) => Object.keys(value)));
  const merged: Record<string, MetricTree | number | null> = {};
  for (const key of [...keys].toSorted()) {
    merged[key] = aggregate(
      values.map((value) => value[key] ?? null),
      reduce,
    );
  }
  return merged;
}

export const median = (numbers: Array<number>) => {
  const sorted = numbers.toSorted((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2;
};

const round = (value: number) =>
  Math.abs(value) < 100 ? Math.round(value * 1000) / 1000 : Math.round(value * 10) / 10;

export function aggregateRuns(runs: ReadonlyArray<MetricTree>): {
  readonly metrics: MetricTree;
  readonly spread: { readonly min: MetricTree; readonly max: MetricTree };
} {
  const tree = (value: MetricTree | number | null): MetricTree => (isTree(value) ? value : {});
  return {
    metrics: tree(aggregate(runs, median)),
    spread: {
      min: tree(aggregate(runs, (numbers) => Math.min(...numbers))),
      max: tree(aggregate(runs, (numbers) => Math.max(...numbers))),
    },
  };
}

const stable = (value: unknown): string =>
  JSON.stringify(value, (_key, inner: unknown) =>
    isTree(inner)
      ? Object.fromEntries(Object.entries(inner).toSorted(([a], [b]) => a.localeCompare(b)))
      : inner,
  ) ?? "undefined";

const sameMaps = (
  what: string,
  before: Readonly<Record<string, unknown>> | null | undefined,
  after: Readonly<Record<string, unknown>> | null | undefined,
): Array<string> => {
  if (!isTree(before) || !isTree(after)) return [`${what} not recorded`];
  const keys = [...new Set([...Object.keys(before), ...Object.keys(after)])].toSorted();
  return keys.flatMap((key) =>
    before[key] === undefined || after[key] === undefined
      ? [`${what} not recorded: ${key}`]
      : stable(before[key]) !== stable(after[key])
        ? [`${what} differs: ${key} ${stable(before[key])} vs ${stable(after[key])}`]
        : [],
  );
};

const HOST_KEYS = ["host_id", "cpu_model", "node"] as const;
const HARNESS_KEYS = ["harness_sha", "harness_files_sha256"] as const;
const RUN_KEYS = ["server_entry", "server_cpus", "driver_cpus"] as const;

/** The listed keys of a record; a key it lacks maps to undefined, which reads "not recorded". */
const pick = (
  record: Readonly<Record<string, unknown>> | null | undefined,
  keys: ReadonlyArray<string>,
): Record<string, unknown> | null =>
  isTree(record) ? Object.fromEntries(keys.map((key) => [key, record[key]])) : null;

/** Why two results must not be compared; empty when they may. */
export function comparabilityProblems(before: BenchResult, after: BenchResult): Array<string> {
  const problems: Array<string> = [];
  for (const result of [before, after]) {
    if (result.schema !== RESULT_SCHEMA) {
      problems.push(`${result.label}: schema ${String(result.schema)}, expected ${RESULT_SCHEMA}`);
    }
  }
  const baseBefore = before.identity?.base_sha ?? null;
  const baseAfter = after.identity?.base_sha ?? null;
  if (baseBefore === null || baseAfter === null) problems.push("base SHA not recorded");
  else if (baseBefore !== baseAfter)
    problems.push(`base SHA differs: ${baseBefore} vs ${baseAfter}`);
  const fixtureBefore = before.identity?.fixture?.sha256 ?? null;
  const fixtureAfter = after.identity?.fixture?.sha256 ?? null;
  if (fixtureBefore === null || fixtureAfter === null) problems.push("fixture hash not recorded");
  else if (fixtureBefore !== fixtureAfter) {
    problems.push(`fixture differs: ${fixtureBefore} vs ${fixtureAfter}`);
  }
  problems.push(...sameMaps("trace sampling", before.sampling, after.sampling));
  problems.push(...sameMaps("workload", before.knobs?.workload, after.knobs?.workload));
  problems.push(...sameMaps("host", pick(before.host, HOST_KEYS), pick(after.host, HOST_KEYS)));
  problems.push(
    ...sameMaps("harness", pick(before.identity, HARNESS_KEYS), pick(after.identity, HARNESS_KEYS)),
  );
  problems.push(
    ...sameMaps("run setup", pick(before.knobs?.run, RUN_KEYS), pick(after.knobs?.run, RUN_KEYS)),
  );
  return problems;
}

interface Row {
  readonly label: string;
  readonly path: ReadonlyArray<string>;
}

const SQL_ROWS: ReadonlyArray<Row> = (["writer", "reader"] as const).flatMap((connection) => {
  const key = `sql.transaction|${connection}`;
  const row = (what: string, field: string) => ({
    label: `\`sql.transaction\` ${connection} ${what} ms`,
    path: ["sql", key, field],
  });
  return [
    row("p50", "p50_ms"),
    row("p95", "p95_ms"),
    row("avg", "avg_ms"),
    row("avg lock wait", "avg_lock_wait_ms"),
    row("p95 lock wait", "p95_lock_wait_ms"),
  ];
});

const LEADING_ROWS: ReadonlyArray<Row> = [
  { label: "Main thread CPU % user", path: ["cpu", "main", "user_pct"] },
  { label: "Main thread CPU % sys", path: ["cpu", "main", "sys_pct"] },
  { label: "Main thread read syscalls/s", path: ["io", "main", "syscr_per_s"] },
  { label: "Main thread write syscalls/s", path: ["io", "main", "syscw_per_s"] },
  { label: "Server threads read syscalls/s", path: ["io", "threads_sum", "syscr_per_s"] },
  {
    label: "Process read syscalls/s (incl. reaped children)",
    path: ["io", "process", "syscr_per_s"],
  },
  {
    label: "WorkerThread CPU % (SQLite readers + checkpoint)",
    path: ["cpu", "worker_threads", "busy_pct"],
  },
  { label: "WorkerThread read syscalls/s", path: ["io", "worker_threads", "syscr_per_s"] },
  { label: "Process CPU % (all threads)", path: ["cpu", "process", "busy_pct"] },
  { label: "ELU (server gauge, mean)", path: ["otlp", "elu_mean"] },
  { label: "Event loop delay max ms", path: ["otlp", "event_loop_delay_max_ms"] },
  ...SQL_ROWS,
  {
    label: "`OrchestratorV2.getThreadShell` p95 ms",
    path: ["spans", "OrchestratorV2.getThreadShell", "p95_ms"],
  },
  {
    label: "`McpHttpServer.authenticateRequest` p95 ms",
    path: ["spans", "McpHttpServer.authenticateRequest", "p95_ms"],
  },
];

const TRAILING_ROWS: ReadonlyArray<Row> = [
  { label: "Thread events/s (client)", path: ["driver", "thread_events_per_s"] },
  { label: "Shell events/s (client)", path: ["driver", "shell_events_per_s"] },
  { label: "Peak RSS MiB", path: ["mem", "rss_peak_mib"] },
  { label: "Peak heap used MiB", path: ["otlp", "heap_used_peak_mib"] },
  { label: "Spans in window", path: ["trace", "spans"] },
];

/** The table rows; one p50 and one p95 row per client call type any result recorded. */
export function tableRows(results: ReadonlyArray<BenchResult>): Array<Row> {
  const callTypes = new Set<string>();
  for (const result of results) {
    const calls = (result.metrics.driver as MetricTree | null | undefined)?.calls;
    if (isTree(calls)) for (const type of Object.keys(calls)) callTypes.add(type);
  }
  const callRows = [...callTypes].toSorted().flatMap((type) => [
    { label: `Client \`${type}\` p50 ms`, path: ["driver", "calls", type, "p50_ms"] },
    { label: `Client \`${type}\` p95 ms`, path: ["driver", "calls", type, "p95_ms"] },
  ]);
  return [...LEADING_ROWS, ...callRows, ...TRAILING_ROWS];
}

export function valueAt(
  tree: MetricTree | null | undefined,
  path: ReadonlyArray<string>,
): number | null {
  let node: MetricTree | number | null | undefined = tree;
  for (const key of path) {
    if (!isTree(node)) return null;
    node = (node as MetricTree)[key];
  }
  return typeof node === "number" && Number.isFinite(node) ? node : null;
}

const grouped = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

/** summarise.py's fmt: n/a for no data, never 0. */
export function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "n/a";
  const magnitude = Math.abs(value);
  if (Number.isInteger(value) || magnitude >= 1000) return grouped.format(value);
  if (magnitude >= 100) return value.toFixed(0);
  if (magnitude >= 10) return value.toFixed(1);
  return value.toFixed(2);
}

const signed = (value: number) => (value > 0 ? `+${formatNumber(value)}` : formatNumber(value));

export function formatDelta(before: number | null, after: number | null): string {
  if (before === null || after === null) return "n/a";
  const delta = round(after - before);
  if (before === 0) return signed(delta);
  const percent = (delta / Math.abs(before)) * 100;
  const shown = Math.abs(percent) >= 10 ? percent.toFixed(0) : percent.toFixed(1);
  return `${signed(delta)} (${percent > 0 ? "+" : ""}${shown}%)`;
}

const cell = (result: BenchResult, path: ReadonlyArray<string>) => {
  const value = valueAt(result.metrics, path);
  if (value === null) return "n/a";
  const low = valueAt(result.spread?.min, path);
  const high = valueAt(result.spread?.max, path);
  return result.runs.valid > 1 && low !== null && high !== null && low !== high
    ? `${formatNumber(value)} (${formatNumber(low)}–${formatNumber(high)})`
    : formatNumber(value);
};

const escapeCell = (text: string) => text.replaceAll("|", "\\|");
const short = (sha: string | null | undefined) => (sha ? sha.slice(0, 8) : "?");

const describeTarget = (result: BenchResult) =>
  `\`${escapeCell(result.identity.server_ref ?? result.label)}\` @ \`${short(result.identity.server_sha)}\`` +
  (result.identity.server_dirty ? " (dirty)" : "");

const samplingNote = (result: BenchResult) => {
  const sampling = result.sampling ?? {};
  return (
    `trace ${String(sampling.trace_min_level ?? "?")}, SQL spans ≥ ${String(sampling.trace_sql_min_duration_ms ?? "?")} ms, ` +
    `OTLP every ${String(sampling.otlp_export_interval_ms ?? "?")} ms`
  );
};

const shown = (value: unknown) =>
  value === undefined || value === null
    ? "unset"
    : typeof value === "string"
      ? value
      : stable(value);

const differences = (
  before: Readonly<Record<string, unknown>> | undefined,
  after: Readonly<Record<string, unknown>> | undefined,
) => {
  const a = before ?? {};
  const b = after ?? {};
  return [...new Set([...Object.keys(a), ...Object.keys(b)])]
    .toSorted()
    .filter((key) => stable(a[key]) !== stable(b[key]))
    .map((key) => `\`${key}\` ${shown(a[key])} → ${shown(b[key])}`);
};

export function renderComparison(before: BenchResult, after: BenchResult): string {
  const lines = [
    `| Metric | before: ${escapeCell(before.label)} | after: ${escapeCell(after.label)} | Δ after − before |`,
    "|---|---:|---:|---:|",
  ];
  for (const row of tableRows([before, after])) {
    lines.push(
      `| ${row.label} | ${cell(before, row.path)} | ${cell(after, row.path)} | ${formatDelta(
        valueAt(before.metrics, row.path),
        valueAt(after.metrics, row.path),
      )} |`,
    );
  }
  lines.push(
    "",
    `Medians of the valid runs (before ${before.runs.valid}/${before.runs.requested}, after ${after.runs.valid}/${after.runs.requested}), ` +
      `(min–max) across runs, n/a = no data. Base \`${short(before.identity.base_sha)}\`, fixture \`${short(before.identity.fixture?.sha256)}\`, ` +
      `${samplingNote(before)}. Before ${describeTarget(before)}, after ${describeTarget(after)}.`,
  );
  const knobs = differences(before.knobs?.server, after.knobs?.server);
  if (knobs.length > 0) lines.push(`Server knobs that differ: ${knobs.join(", ")}.`);
  const settings = differences(before.knobs?.run, after.knobs?.run).filter(
    (line) => !RUN_KEYS.some((key) => line.startsWith(`\`${key}\``)),
  );
  if (settings.length > 0) lines.push(`Run settings that differ: ${settings.join(", ")}.`);
  return `${lines.join("\n")}\n`;
}

export function renderResult(result: BenchResult): string {
  const lines = [`| Metric | ${escapeCell(result.label)} |`, "|---|---:|"];
  for (const row of tableRows([result])) lines.push(`| ${row.label} | ${cell(result, row.path)} |`);
  lines.push(
    "",
    `Median of ${result.runs.valid}/${result.runs.requested} valid runs, (min–max) across runs, n/a = no data. ` +
      `${describeTarget(result)}, base \`${short(result.identity.base_sha)}\`, fixture \`${short(result.identity.fixture?.sha256)}\`, ${samplingNote(result)}.`,
  );
  return `${lines.join("\n")}\n`;
}

export type CompareOutcome =
  | { readonly ok: true; readonly markdown: string }
  | { readonly ok: false; readonly problems: ReadonlyArray<string> };

export function compareResults(before: BenchResult, after: BenchResult): CompareOutcome {
  const problems = comparabilityProblems(before, after);
  return problems.length > 0
    ? { ok: false, problems }
    : { ok: true, markdown: renderComparison(before, after) };
}

export function readResult(path: string): BenchResult {
  const parsed: unknown = JSON.parse(NodeFS.readFileSync(path, "utf8"));
  if (
    !isTree(parsed) ||
    !isTree(parsed.metrics) ||
    !isTree(parsed.identity) ||
    !isTree(parsed.runs)
  ) {
    throw new Error(`${path} is not a server-bench result.json`);
  }
  return parsed as unknown as BenchResult;
}

function main(argv: ReadonlyArray<string>): number {
  const [command, ...files] = argv;
  if (command === "compare" && files.length === 2) {
    const outcome = compareResults(readResult(files[0]!), readResult(files[1]!));
    if (!outcome.ok) {
      console.error(`refusing to compare ${files[0]} and ${files[1]}:`);
      for (const problem of outcome.problems) console.error(`  - ${problem}`);
      return 2;
    }
    process.stdout.write(outcome.markdown);
    return 0;
  }
  if (command === "show" && files.length === 1) {
    process.stdout.write(renderResult(readResult(files[0]!)));
    return 0;
  }
  console.error(HELP);
  return command === "--help" || command === "-h" ? 0 : 2;
}

if (import.meta.main) {
  try {
    process.exitCode = main(process.argv.slice(2));
  } catch (error) {
    console.error(`report: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 2;
  }
}

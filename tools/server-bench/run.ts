// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalTimers:off globalFetch:off preferSchemaOverJson:off cryptoRandomUUID:off - standalone bench orchestrator outside the Effect runtime; it owns child processes and deadlines.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

import {
  Collector,
  knobsFromEnv,
  procAvailable,
  readTraceWindow,
  startOtlpReceiver,
  summariseGauges,
  summariseSpans,
  summariseWindow,
  traceSampling,
  type WindowBounds,
} from "./collect.ts";
import {
  RESULT_SCHEMA,
  aggregateRuns,
  compareResults,
  type BenchResult,
  type MetricTree,
} from "./report.ts";

export const HELP = `Usage:
  node tools/server-bench/run.ts --ref <[label=]git-ref|checkout-dir> [--ref ...] --fixture <path|none>
                                 --out <dir> [options]

Per ref: restore the pristine fixture into a fresh base dir, start the server, sample it at 1 Hz
(collect.ts), run the load driver, stop the server, read the trace. --runs rounds, counterbalanced
(A B, B A, A B ...). An attempt is invalid when the server fails or exits, the driver fails, or,
unless T3CODE_TRACE_MIN_LEVEL is Warn or above or T3CODE_TRACE_TIMING_ENABLED is false (untimed
spans; span metrics are then n/a), the trace does not cover the window (trace_missing /
trace_coverage / trace_stale / trace_rotated_during_read); it is retried in its own slot up to
--retries times, so the order holds. Then <out>/<label>.result.json (median of the
valid runs, min/max spread, the attempt order, every attempt's detail) and, with two or more refs,
<out>/compare.md (the first ref against each other one, via report.ts; a refused comparison
exits 2).

Refs
  --ref <git-ref>          worktree at <work>/trees/<sha>, built once with --build-cmd; runs
                           apps/server/dist/bin.mjs.
  --ref <dir>              an existing checkout (HEAD SHA, dirty flag and a hash of its changes
                           recorded). It runs dist/bin.mjs only when the stamp run.ts writes after
                           a build (apps/server/dist/.server-bench-build.json) matches the tree as
                           it is now and the bundle on disk; otherwise it rebuilds with --build-cmd
                           first. Without dist/ it runs src/bin.ts (smoke only; compare refuses
                           dist against source).
  label=...                names the result (default: the ref with / as -).
  --base <ref>             the base every result records; compare refuses different bases
                           [perf/base].
  --build-cmd <sh>         [vp i && vp run --filter t3 build]

Fixture (restored for every run with a reflink copy where the filesystem supports it)
  --fixture <file>         a checkpointed statev2.sqlite (VACUUM INTO output; a non-empty -wal next
                           to it is refused), copied to <base>/userdata/statev2.sqlite.
  --fixture <dir>          a whole base-dir template (userdata/statev2.sqlite, settings.json ...);
                           regular files and directories only (a symlink is refused).
  --fixture none           an empty database (smoke only).
  The fixture's sha256 (cached by path, size and mtime in <work>/fixture-hashes.json) is recorded.

Load
  --driver <path|stub>     [driver.ts beside this file when it exists, else driver-stub.ts]
  --warmup 60s --window 300s, --sessions/--subscribers/--pollers N (passed through when given),
  --driver-arg=<arg>       repeated, appended verbatim (e.g. --driver-arg=--mode --driver-arg=fresh).
  The driver (the P1-004 contract) is started as: node <driver> --url <origin> --out <run>/driver
  --server-pid <pid> --warmup <n>s --window <n>s [--sessions N] [--subscribers N] [--pollers N]
  [driver args], with T3_BENCH_TOKEN (a bearer token from the server's bootstrap grant) in its
  environment. It writes <out>/driver.json: { status: "ok"|"failed", failure?: { reason },
  timeline: { window: <ISO> }, windowSeconds, calls: { <type>: { count, errors, unfinished, p50,
  p95, p99, max } }, counters: { "events.thread": { perSecond }, ... }, knobs }; status "failed", a
  driver.failed marker or no file marks the run invalid. Without a window in it, the run uses
  [driver start + warmup, + window].
  The workload fingerprint hashes the driver (its file, module directory and the local modules
  they import), the fake providers (fake-*/ and what they import) and the transcripts they replay
  (the server's recorded replay fixtures, and every absolute BENCH_*TRANSCRIPT path in the server
  env or the fixture's settings.json).

Server
  --server-env K=V         every ref; --ref-env <label>:K=V one ref (wins). The fake claude's BENCH_*
                           load knobs go here too (recorded as workload). T3CODE_*, T3_*, OTEL_*,
                           BENCH_*, NODE_OPTIONS and UV_THREADPOOL_SIZE from this shell are NOT
                           passed on, so the recorded knobs are exactly the applied ones.
                           Defaults: T3CODE_TRACE_MAX_BYTES=67108864, T3CODE_TRACE_MAX_FILES=64 (no
                           window rotates away), T3CODE_TRACE_SQL_MIN_DURATION_MS=0 (all SQL spans).
  --otlp-interval-ms N     server gauge export interval to the local receiver; 0 = off [1000].
  --server-cpus / --driver-cpus <list>   taskset CPU lists (Linux); the harness itself joins the
                           driver CPUs.
  --startup-timeout 300s, --runs 3, --retries 2 (per run slot), --work <out>/work, --keep-runs
  (keep each run's base dir with its database and trace; deleted by default).

<work>/trees/<sha> are git worktrees of this repository, kept so a SHA is built once; remove them
with git worktree remove <dir> when done.`;

const MIB = 1024 * 1024;
const harnessDir = import.meta.dirname;
const repoRoot = NodePath.resolve(harnessDir, "../..");

const OWNED_ENV = new Set([
  "T3CODE_HOME",
  "T3CODE_PORT",
  "T3CODE_HOST",
  "T3CODE_MODE",
  "T3CODE_BOOTSTRAP_FD",
  "T3CODE_OTLP_METRICS_URL",
  "T3CODE_OTLP_PROTOCOL",
  "T3CODE_OTLP_EXPORT_INTERVAL_MS",
]);
const STRIPPED_PREFIXES = ["T3CODE_", "T3_", "OTEL_", "BENCH_"];
const STRIPPED_NAMES = new Set([
  "NODE_OPTIONS",
  "UV_THREADPOOL_SIZE",
  "VITE_DEV_SERVER_URL",
  "ELECTRON_RUN_AS_NODE",
]);
const BENCH_DEFAULT_ENV: Readonly<Record<string, string>> = {
  T3CODE_TRACE_MAX_BYTES: String(64 * MIB),
  T3CODE_TRACE_MAX_FILES: "64",
  T3CODE_TRACE_SQL_MIN_DURATION_MS: "0",
};
/** BENCH_* variables that locate things rather than shape the load. */
const isPlumbing = (name: string) => name === "BENCH_NODE" || /_(DIR|PATH|FILE)$/.test(name);

const FATAL = new Set([
  "server_start_failed",
  "server_auth_failed",
  "server_exited",
  "driver_failed",
  "driver_timeout",
  "pid_gone",
  "window_too_short",
  "main_thread_missing",
]);
const TRACE_PROBLEMS = new Set([
  "trace_missing",
  "trace_coverage",
  "trace_stale",
  "trace_rotated_during_read",
]);
/** The values the server's Config.Boolean reads as false. */
const FALSE_LITERALS = new Set(["false", "no", "off", "0", "n"]);
/**
 * Below Warn the server writes timed spans continuously, so a trace problem voids the attempt.
 * With timing off its spans carry no times and no window can be read from them.
 */
export const traceRequired = (env: Readonly<Record<string, string>>) =>
  !FALSE_LITERALS.has(env.T3CODE_TRACE_TIMING_ENABLED ?? "true") &&
  !["warn", "error", "fatal", "none"].includes(
    (env.T3CODE_TRACE_MIN_LEVEL ?? "Info").trim().toLowerCase(),
  );

/** Rounds alternate direction (A B, B A, A B ...), so drift over time hits every ref alike. */
export function runOrder(labels: ReadonlyArray<string>, runs: number): Array<string> {
  return Array.from({ length: runs }, (_, round) =>
    round % 2 === 0 ? [...labels] : labels.toReversed(),
  ).flat();
}

export function parseDuration(value: string, flag: string): number {
  const match = /^(\d+(?:\.\d+)?)(s|m)?$/.exec(value.trim());
  if (match === null) throw new Error(`${flag} must be a duration such as 300, 300s or 5m`);
  return Number(match[1]) * (match[2] === "m" ? 60 : 1);
}

const positiveInt = (value: string | undefined, flag: string) => {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 1)
    throw new Error(`${flag} must be a positive integer`);
  return number;
};

const parseAssignment = (text: string, flag: string): [string, string] => {
  const match = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/s.exec(text);
  if (match === null) throw new Error(`${flag} expects NAME=VALUE, got ${text}`);
  if (OWNED_ENV.has(match[1]!)) throw new Error(`${flag}: ${match[1]} is set by run.ts`);
  return [match[1]!, match[2]!];
};

const CPU_LIST = /^\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*$/;

function git(cwd: string, args: ReadonlyArray<string>): string {
  const result = NodeChildProcess.spawnSync("git", ["-C", cwd, ...args], { encoding: "utf8" });
  if (result.status !== 0) {
    throw new Error(`git ${args.join(" ")} failed: ${(result.stderr ?? "").trim()}`);
  }
  return result.stdout.trim();
}

const resolveCommit = (cwd: string, ref: string) =>
  git(cwd, ["rev-parse", "--verify", `${ref}^{commit}`]);

async function sha256File(path: string): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  for await (const chunk of NodeFS.createReadStream(path)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}

export function benchEnvHash(env: Readonly<Record<string, string>>): string {
  const entries = Object.entries(env)
    .filter(([name]) => name.startsWith("BENCH_") && !isPlumbing(name))
    .toSorted(([a], [b]) => a.localeCompare(b));
  return NodeCrypto.createHash("sha256").update(JSON.stringify(entries)).digest("hex");
}

const HARNESS_FILES = ["collect.ts", "report.ts", "run.ts", "driver-stub.ts"];

async function filesHash(paths: ReadonlyArray<string>): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  for (const path of paths) hash.update(`${NodePath.basename(path)}\0${await sha256File(path)}\n`);
  return hash.digest("hex");
}

const isFile = (path: string) =>
  NodeFS.statSync(path, { throwIfNoEntry: false })?.isFile() === true;

/** Relative specifiers of static, re-export, side-effect and literal dynamic imports. */
const LOCAL_IMPORT = /(?:\bfrom|\bimport)\s*\(?\s*["'](\.{1,2}\/[^"'\n]+)["']/g;

/** `entries` and the local modules they import, transitively; absolute paths, sorted. */
function withLocalImports(entries: ReadonlyArray<string>): Array<string> {
  const seen = new Set<string>();
  const pending = [...entries];
  for (let file = pending.pop(); file !== undefined; file = pending.pop()) {
    if (seen.has(file) || !isFile(file)) continue;
    seen.add(file);
    for (const match of NodeFS.readFileSync(file, "utf8").matchAll(LOCAL_IMPORT)) {
      pending.push(NodePath.resolve(NodePath.dirname(file), match[1]!));
    }
  }
  return [...seen].toSorted();
}

/**
 * The driver file, its module directory when present (driver.ts + driver/), and the local modules
 * they import from elsewhere (driver.ts imports providers.ts).
 */
export async function driverHash(path: string): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  hash.update(`${NodePath.basename(path)}\0${await sha256File(path)}\n`);
  const covered = new Set([path]);
  const modules = path.replace(/\.[cm]?[jt]s$/, "");
  if (modules !== path && NodeFS.existsSync(modules) && NodeFS.statSync(modules).isDirectory()) {
    const files = (NodeFS.readdirSync(modules, { recursive: true }) as Array<string>)
      .filter((relative) => NodeFS.statSync(NodePath.join(modules, relative)).isFile())
      .toSorted();
    for (const relative of files) {
      covered.add(NodePath.join(modules, relative));
      hash.update(`${relative}\0${await sha256File(NodePath.join(modules, relative))}\n`);
    }
  }
  for (const file of withLocalImports([...covered])) {
    if (covered.has(file)) continue;
    hash.update(`${NodePath.relative(NodePath.dirname(path), file)}\0${await sha256File(file)}\n`);
  }
  return hash.digest("hex");
}

const FAKE_DIRS = ["fake-claude", "fake-codex", "fake-acp", "fake-common"];

/**
 * The replaying fake providers: their directories' files but tests and READMEs (launchers, code,
 * generated transcripts), and the local modules they import from elsewhere.
 */
export async function fakesHash(benchDir: string): Promise<string> {
  const files = FAKE_DIRS.flatMap((dir) => {
    const root = NodePath.join(benchDir, dir);
    if (!NodeFS.existsSync(root)) return [];
    return (NodeFS.readdirSync(root, { recursive: true }) as Array<string>)
      .map((relative) => NodePath.join(root, relative))
      .filter((file) => isFile(file) && !/\.test\.[cm]?[jt]s$|\.md$/.test(file));
  });
  const hash = NodeCrypto.createHash("sha256");
  for (const file of withLocalImports(files)) {
    hash.update(`${NodePath.relative(benchDir, file)}\0${await sha256File(file)}\n`);
  }
  return hash.digest("hex");
}

const REPLAY_FIXTURES = NodePath.join(
  repoRoot,
  "apps/server/src/orchestration-v2/testkit/fixtures",
);
const TRANSCRIPT_VAR = /^BENCH_\w*TRANSCRIPT$/;

/** The BENCH_*TRANSCRIPT values of `env` and of the instances in a directory fixture's settings. */
function transcriptSettings(
  env: Readonly<Record<string, string>>,
  fixture: FixtureInfo,
): Array<string> {
  const values = Object.entries(env)
    .filter(([name]) => TRANSCRIPT_VAR.test(name))
    .map(([, value]) => value);
  const settings =
    fixture.kind === "dir" ? NodePath.join(fixture.path!, "userdata", "settings.json") : null;
  if (settings === null || !isFile(settings)) return values;
  let parsed: Record<string, unknown>;
  try {
    parsed = recordOf(JSON.parse(NodeFS.readFileSync(settings, "utf8")));
  } catch (error) {
    throw new Error(`${settings}: ${error instanceof Error ? error.message : String(error)}`);
  }
  for (const instance of Object.values(recordOf(parsed.providerInstances))) {
    const environment = recordOf(instance).environment;
    for (const entry of Array.isArray(environment) ? environment.map(recordOf) : []) {
      if (typeof entry.name === "string" && TRANSCRIPT_VAR.test(entry.name)) {
        if (typeof entry.value === "string") values.push(entry.value);
      }
    }
  }
  return values;
}

/**
 * The transcripts the fakes replay: every recording they load by name (the `*.ndjson` of the
 * server's replay fixtures) and each absolute path a BENCH_*TRANSCRIPT mix names, in the server
 * environment or the fixture's settings. Generated transcripts belong to fakesHash.
 */
export async function transcriptsHash(
  env: Readonly<Record<string, string>>,
  fixture: FixtureInfo,
  fixturesDir: string,
): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  const recorded = NodeFS.existsSync(fixturesDir)
    ? (NodeFS.readdirSync(fixturesDir, { recursive: true }) as Array<string>)
        .filter((relative) => relative.endsWith(".ndjson"))
        .toSorted()
    : [];
  for (const relative of recorded) {
    hash.update(`${relative}\0${await sha256File(NodePath.join(fixturesDir, relative))}\n`);
  }
  const named = new Set<string>();
  for (const mix of transcriptSettings(env, fixture)) {
    for (const entry of mix.split(",")) {
      const name = entry.trim().replace(/:\d+$/, "").trim();
      if (NodePath.isAbsolute(name)) named.add(name);
    }
  }
  for (const path of [...named].toSorted()) {
    hash.update(`${path}\0${isFile(path) ? await sha256File(path) : "missing"}\n`);
  }
  return hash.digest("hex");
}

export interface FixtureInfo {
  readonly kind: "file" | "dir" | "empty";
  readonly path: string | null;
  readonly sha256: string;
  readonly bytes: number | null;
}

export async function fixtureInfo(spec: string, workDir: string): Promise<FixtureInfo> {
  if (spec === "none") return { kind: "empty", path: null, sha256: "empty", bytes: 0 };
  const path = NodeFS.realpathSync(spec);
  const cachePath = NodePath.join(workDir, "fixture-hashes.json");
  let cache: Record<string, { size: number; mtimeMs: number; sha256: string }> = {};
  try {
    cache = JSON.parse(NodeFS.readFileSync(cachePath, "utf8")) as typeof cache;
  } catch {
    cache = {};
  }
  const hashCached = async (file: string) => {
    const stat = NodeFS.statSync(file);
    const hit = cache[file];
    if (hit !== undefined && hit.size === stat.size && hit.mtimeMs === stat.mtimeMs) return hit;
    const entry = { size: stat.size, mtimeMs: stat.mtimeMs, sha256: await sha256File(file) };
    cache[file] = entry;
    return entry;
  };
  let info: FixtureInfo;
  if (NodeFS.statSync(path).isDirectory()) {
    const files: Array<string> = [];
    for (const relative of (
      NodeFS.readdirSync(path, { recursive: true }) as Array<string>
    ).toSorted()) {
      const stat = NodeFS.lstatSync(NodePath.join(path, relative));
      if (stat.isDirectory()) continue;
      // A copied link would let the server write into its target, outside the run's base dir.
      if (!stat.isFile()) {
        throw new Error(
          `fixture ${path} has a ${stat.isSymbolicLink() ? "symlink" : "special file"} at ${relative}; a template holds only regular files and directories`,
        );
      }
      files.push(relative);
    }
    const hash = NodeCrypto.createHash("sha256");
    let bytes = 0;
    for (const relative of files) {
      const entry = await hashCached(NodePath.join(path, relative));
      hash.update(`${relative}\0${entry.sha256}\n`);
      bytes += entry.size;
    }
    info = { kind: "dir", path, sha256: hash.digest("hex"), bytes };
  } else {
    const wal = `${path}-wal`;
    if (NodeFS.existsSync(wal) && NodeFS.statSync(wal).size > 0) {
      throw new Error(`${path} has a non-empty WAL; use a checkpointed copy (VACUUM INTO)`);
    }
    const entry = await hashCached(path);
    info = { kind: "file", path, sha256: entry.sha256, bytes: entry.size };
  }
  NodeFS.writeFileSync(cachePath, `${JSON.stringify(cache, null, 2)}\n`);
  return info;
}

function restoreFixture(fixture: FixtureInfo, baseDir: string) {
  NodeFS.mkdirSync(NodePath.join(baseDir, "userdata"), { recursive: true });
  if (fixture.kind === "file") {
    NodeFS.copyFileSync(
      fixture.path!,
      NodePath.join(baseDir, "userdata", "statev2.sqlite"),
      NodeFS.constants.COPYFILE_FICLONE,
    );
  } else if (fixture.kind === "dir") {
    NodeFS.cpSync(fixture.path!, baseDir, {
      recursive: true,
      mode: NodeFS.constants.COPYFILE_FICLONE,
    });
  }
}

interface Target {
  readonly label: string;
  readonly ref: string;
  readonly dir: string;
  readonly sha: string;
  readonly dirty: boolean;
  /** HEAD, plus a hash of uncommitted changes for a dirty tree. */
  readonly tree: string;
  readonly entry: string;
  readonly entryKind: "dist" | "source";
  readonly version: string | null;
  readonly env: Readonly<Record<string, string>>;
}

const sanitiseLabel = (text: string) =>
  text.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");

/** HEAD and, for a tree with changes (tracked or untracked), a hash of them. */
function treeState(dir: string) {
  const sha = git(dir, ["rev-parse", "HEAD"]);
  const dirty = git(dir, ["status", "--porcelain"]).length > 0;
  if (!dirty) return { sha, dirty, tree: sha };
  const hash = NodeCrypto.createHash("sha256");
  hash.update(
    NodeChildProcess.spawnSync("git", ["-C", dir, "diff", "HEAD", "--binary"], {
      maxBuffer: 1024 * MIB,
    }).stdout,
  );
  const untracked = git(dir, ["ls-files", "--others", "--exclude-standard", "-z"])
    .split("\0")
    .filter((file) => file.length > 0)
    .toSorted();
  for (const file of untracked) {
    hash.update(`\0${file}\0`);
    hash.update(NodeFS.readFileSync(NodePath.join(dir, file)));
  }
  return { sha, dirty, tree: `${sha}+${hash.digest("hex").slice(0, 16)}` };
}

const DIST_ENTRY = "apps/server/dist/bin.mjs";
const BUILD_STAMP = "apps/server/dist/.server-bench-build.json";

/** The stamp a build leaves: the tree it was built from and the bundle it produced. */
const distStamp = (dir: string, tree: string) => {
  const stat = NodeFS.statSync(NodePath.join(dir, DIST_ENTRY));
  return { tree, size: stat.size, mtimeMs: stat.mtimeMs };
};

function builtFrom(dir: string, tree: string): boolean {
  try {
    const stamp = JSON.parse(
      NodeFS.readFileSync(NodePath.join(dir, BUILD_STAMP), "utf8"),
    ) as unknown;
    return JSON.stringify(stamp) === JSON.stringify(distStamp(dir, tree));
  } catch {
    return false;
  }
}

function build(dir: string, tree: string, buildCmd: string, log: string, what: string) {
  console.error(`building ${what} (${tree.slice(0, 12)}): ${buildCmd} > ${log}`);
  const fd = NodeFS.openSync(log, "w");
  const result = NodeChildProcess.spawnSync("sh", ["-c", buildCmd], {
    cwd: dir,
    stdio: ["ignore", fd, fd],
  });
  NodeFS.closeSync(fd);
  if (result.status !== 0) throw new Error(`build of ${what} failed; see ${log}`);
  if (NodeFS.existsSync(NodePath.join(dir, DIST_ENTRY))) {
    NodeFS.writeFileSync(NodePath.join(dir, BUILD_STAMP), JSON.stringify(distStamp(dir, tree)));
  }
}

/**
 * A git ref gets its own worktree, built once per SHA. A checkout directory runs its built
 * bundle only when the stamp says it was built from the tree as it is now (HEAD plus any
 * changes), and is rebuilt otherwise; without a bundle it runs from source.
 */
function prepareTarget(spec: string, workDir: string, buildCmd: string): Omit<Target, "env"> {
  const eq = spec.indexOf("=");
  const explicitLabel =
    eq > 0 && /^[A-Za-z0-9._-]+$/.test(spec.slice(0, eq)) ? spec.slice(0, eq) : undefined;
  const value = explicitLabel === undefined ? spec : spec.slice(eq + 1);
  const isCheckout = NodeFS.existsSync(value) && NodeFS.statSync(value).isDirectory();
  let dir: string;
  if (isCheckout) {
    dir = NodeFS.realpathSync(value);
  } else {
    const sha = resolveCommit(repoRoot, value);
    dir = NodePath.join(workDir, "trees", sha.slice(0, 12));
    if (!NodeFS.existsSync(dir)) git(repoRoot, ["worktree", "add", "--detach", dir, sha]);
  }
  const state = treeState(dir);
  const hasDist = NodeFS.existsSync(NodePath.join(dir, DIST_ENTRY));
  if ((!isCheckout || hasDist) && !(hasDist && builtFrom(dir, state.tree))) {
    const log = NodePath.join(workDir, `build-${state.sha.slice(0, 12)}.log`);
    build(dir, state.tree, buildCmd, log, value);
  }
  const entryKind = NodeFS.existsSync(NodePath.join(dir, DIST_ENTRY)) ? "dist" : "source";
  if (entryKind === "dist" && !builtFrom(dir, state.tree)) {
    throw new Error(`${value}: ${DIST_ENTRY} was not built from ${state.tree}`);
  }
  const entry = NodePath.join(dir, entryKind === "dist" ? DIST_ENTRY : "apps/server/src/bin.ts");
  if (!NodeFS.existsSync(entry)) throw new Error(`${value}: no server entry at ${entry}`);
  const version = NodeChildProcess.spawnSync(process.execPath, [entry, "--version"], {
    cwd: dir,
    encoding: "utf8",
    timeout: 120_000,
  });
  return {
    label: explicitLabel ?? sanitiseLabel(isCheckout ? NodePath.basename(dir) : value),
    ref: value,
    dir,
    sha: state.sha,
    dirty: state.dirty,
    tree: state.tree,
    entry,
    entryKind,
    version: version.status === 0 ? (version.stdout.split("\n")[0]?.trim() ?? null) : null,
  };
}

export interface CallStats {
  readonly count: number | null;
  readonly errors: number | null;
  /** Calls still running when the driver's grace period ended; latencies leave them out. */
  readonly unfinished: number | null;
  readonly per_s: number | null;
  readonly p50_ms: number | null;
  readonly p95_ms: number | null;
  readonly p99_ms: number | null;
  readonly max_ms: number | null;
}

export interface DriverSummary {
  readonly ok: boolean;
  readonly error: string | null;
  readonly window: WindowBounds | null;
  readonly thread_events_per_s: number | null;
  readonly shell_events_per_s: number | null;
  readonly calls: Readonly<Record<string, CallStats>> | null;
  readonly knobs: Readonly<Record<string, unknown>> | null;
}

const finiteOrNull = (value: unknown) =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

const recordOf = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

/** driver.json (contract in HELP) to the fields the result keeps; missing fields are null. */
export function readDriverResult(raw: unknown): DriverSummary {
  const record = recordOf(raw);
  const timeline = recordOf(record.timeline);
  const start = typeof timeline.window === "string" ? Date.parse(timeline.window) : Number.NaN;
  const seconds = finiteOrNull(record.windowSeconds);
  const end =
    seconds !== null
      ? start + seconds * 1000
      : typeof timeline.measured === "string"
        ? Date.parse(timeline.measured)
        : Number.NaN;
  const calls: Record<string, CallStats> = {};
  for (const [type, stats] of Object.entries(recordOf(record.calls))) {
    const entry = recordOf(stats);
    calls[type] = {
      count: finiteOrNull(entry.count),
      errors: finiteOrNull(entry.errors),
      unfinished: finiteOrNull(entry.unfinished),
      per_s: finiteOrNull(entry.perSecond),
      p50_ms: finiteOrNull(entry.p50),
      p95_ms: finiteOrNull(entry.p95),
      p99_ms: finiteOrNull(entry.p99),
      max_ms: finiteOrNull(entry.max),
    };
  }
  const counters = recordOf(record.counters);
  const failure = recordOf(record.failure);
  return {
    ok: record.status === "ok",
    error: typeof failure.reason === "string" ? failure.reason : null,
    window:
      Number.isFinite(start) && Number.isFinite(end) && end > start
        ? { startMs: start, endMs: end }
        : null,
    thread_events_per_s: finiteOrNull(recordOf(counters["events.thread"]).perSecond),
    shell_events_per_s: finiteOrNull(recordOf(counters["events.shell"]).perSecond),
    calls: Object.keys(calls).length === 0 ? null : calls,
    knobs: Object.keys(recordOf(record.knobs)).length === 0 ? null : recordOf(record.knobs),
  };
}

/** The bearer token the driver uses, from the unbounded bootstrap grant run.ts seeded. */
async function exchangeBootstrap(origin: string, bootstrap: string): Promise<string | undefined> {
  try {
    const response = await fetch(`${origin}/oauth/token`, {
      method: "POST",
      body: new URLSearchParams({
        grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
        subject_token: bootstrap,
        subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
        requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
      }),
      signal: AbortSignal.timeout(30_000),
    });
    const body = recordOf(await response.json());
    return response.ok && typeof body.access_token === "string" ? body.access_token : undefined;
  } catch {
    return undefined;
  }
}

const freePort = () =>
  new Promise<number>((resolve, reject) => {
    const server = NodeNet.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() =>
        address !== null && typeof address === "object"
          ? resolve(address.port)
          : reject(new Error("no port")),
      );
    });
  });

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, ms)));

const exited = (child: NodeChildProcess.ChildProcess) =>
  child.exitCode !== null || child.signalCode !== null
    ? Promise.resolve()
    : new Promise<void>((resolve) => child.once("exit", () => resolve()));

function killGroup(child: NodeChildProcess.ChildProcess, signal: NodeJS.Signals) {
  if (child.pid === undefined) return;
  try {
    process.kill(-child.pid, signal);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
}

async function stopGroup(child: NodeChildProcess.ChildProcess, graceMs: number) {
  if (child.exitCode === null && child.signalCode === null) {
    killGroup(child, "SIGTERM");
    await Promise.race([exited(child), sleep(graceMs)]);
  }
  // Provider children (fake claude) live in the same group and must not outlive the run.
  killGroup(child, "SIGKILL");
  await exited(child);
}

const live = new Set<NodeChildProcess.ChildProcess>();

const pinned = (cpus: string | undefined, command: ReadonlyArray<string>) =>
  cpus === undefined ? command : ["taskset", "-c", cpus, ...command];

function cleanEnv(): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value === undefined || STRIPPED_NAMES.has(name)) continue;
    if (STRIPPED_PREFIXES.some((prefix) => name.startsWith(prefix))) continue;
    env[name] = value;
  }
  return env;
}

interface RunOptions {
  readonly fixture: FixtureInfo;
  readonly workDir: string;
  readonly warmupS: number;
  readonly windowS: number;
  readonly driverPath: string;
  readonly driverArgs: ReadonlyArray<string>;
  readonly serverCpus: string | undefined;
  readonly driverCpus: string | undefined;
  readonly otlpIntervalMs: number;
  readonly startupTimeoutS: number;
  readonly keepRuns: boolean;
}

async function waitReady(origin: string, child: NodeChildProcess.ChildProcess, timeoutS: number) {
  const deadline = Date.now() + timeoutS * 1000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null || child.signalCode !== null) return "server_exited";
    try {
      const response = await fetch(`${origin}/.well-known/t3/environment`, {
        signal: AbortSignal.timeout(5000),
      });
      await response.arrayBuffer();
      if (response.ok) return undefined;
    } catch {
      // Not listening yet.
    }
    await sleep(250);
  }
  return "server_start_failed";
}

async function runOnce(target: Target, attempt: number, options: RunOptions) {
  const runDir = NodePath.join(options.workDir, "runs", target.label, `attempt-${attempt}`);
  NodeFS.rmSync(runDir, { recursive: true, force: true });
  const baseDir = NodePath.join(runDir, "base");
  NodeFS.mkdirSync(baseDir, { recursive: true });
  restoreFixture(options.fixture, baseDir);
  const problems: Array<string> = [];
  const receiver = options.otlpIntervalMs > 0 ? await startOtlpReceiver() : undefined;
  const port = await freePort();
  const origin = `http://127.0.0.1:${port}`;
  const bootstrap = NodeCrypto.randomUUID();
  const env = {
    ...cleanEnv(),
    ...target.env,
    ...(receiver === undefined
      ? {}
      : {
          T3CODE_OTLP_METRICS_URL: receiver.url,
          T3CODE_OTLP_PROTOCOL: "http/json",
          T3CODE_OTLP_EXPORT_INTERVAL_MS: String(options.otlpIntervalMs),
        }),
  };
  const serverLog = NodeFS.openSync(NodePath.join(runDir, "server.log"), "w");
  const [command, ...args] = pinned(options.serverCpus, [
    process.execPath,
    target.entry,
    "serve",
    "--mode",
    "web",
    "--base-dir",
    baseDir,
    "--port",
    String(port),
    "--host",
    "127.0.0.1",
    "--bootstrap-fd",
    "3",
  ]);
  const server = NodeChildProcess.spawn(command!, args, {
    cwd: runDir,
    env,
    detached: true,
    stdio: ["ignore", "pipe", "pipe", "pipe"],
  });
  live.add(server);
  let logOpen = true;
  for (const pipe of [server.stdout, server.stderr]) {
    pipe?.on("data", (data: Buffer) => {
      if (logOpen) NodeFS.writeSync(serverLog, data.toString().replaceAll(bootstrap, "[REDACTED]"));
    });
  }
  const bootstrapPipe = server.stdio[3] as NodeJS.WritableStream | null;
  bootstrapPipe?.end(
    JSON.stringify({
      mode: "desktop",
      noBrowser: true,
      port,
      t3Home: baseDir,
      host: "127.0.0.1",
      desktopBootstrapToken: bootstrap,
      tailscaleServeEnabled: false,
      tailscaleServePort: 443,
    }),
  );
  const startedAt = new Date().toISOString();
  const startProblem = await waitReady(origin, server, options.startupTimeoutS);
  let window: WindowBounds | null = null;
  let driver: DriverSummary | null = null;
  let collector: Collector | undefined;
  const token = startProblem === undefined ? await exchangeBootstrap(origin, bootstrap) : undefined;
  if (startProblem !== undefined) {
    problems.push(startProblem);
  } else if (token === undefined) {
    problems.push("server_auth_failed");
  } else {
    collector = new Collector(server.pid!);
    collector.start();
    const driverOut = NodePath.join(runDir, "driver");
    const driverLog = NodeFS.openSync(NodePath.join(runDir, "driver.log"), "w");
    const [driverCommand, ...driverArgs] = pinned(options.driverCpus, [
      process.execPath,
      options.driverPath,
      "--url",
      origin,
      "--out",
      driverOut,
      "--server-pid",
      String(server.pid),
      "--warmup",
      `${options.warmupS}s`,
      "--window",
      `${options.windowS}s`,
      ...options.driverArgs,
    ]);
    const driverStart = Date.now();
    const child = NodeChildProcess.spawn(driverCommand!, driverArgs, {
      cwd: runDir,
      env: { ...cleanEnv(), T3_BENCH_TOKEN: token },
      detached: true,
      stdio: ["ignore", driverLog, driverLog],
    });
    live.add(child);
    // Setup (projects, sessions, subscribers) comes before the warm-up and is not bounded by it.
    const budgetMs = (options.warmupS + options.windowS + Math.max(600, options.windowS)) * 1000;
    const outcome = await Promise.race([
      exited(child).then(() => "driver" as const),
      exited(server).then(() => "server" as const),
      sleep(budgetMs).then(() => "timeout" as const),
    ]);
    if (outcome === "server") {
      problems.push("server_exited");
      await Promise.race([exited(child), sleep(30_000)]);
    } else if (outcome === "timeout") {
      problems.push("driver_timeout");
    }
    await stopGroup(child, 5000);
    live.delete(child);
    NodeFS.closeSync(driverLog);
    collector.stop();
    if (collector.gone && !problems.includes("server_exited")) problems.push("pid_gone");
    try {
      driver = readDriverResult(
        JSON.parse(NodeFS.readFileSync(NodePath.join(driverOut, "driver.json"), "utf8")),
      );
    } catch {
      driver = null;
    }
    if (
      driver === null ||
      !driver.ok ||
      NodeFS.existsSync(NodePath.join(driverOut, "driver.failed"))
    ) {
      problems.push("driver_failed");
    }
    window = driver?.window ?? {
      startMs: driverStart + options.warmupS * 1000,
      endMs: driverStart + (options.warmupS + options.windowS) * 1000,
    };
  }
  const serverExit = { code: server.exitCode, signal: server.signalCode };
  await stopGroup(server, 30_000);
  live.delete(server);
  logOpen = false;
  NodeFS.closeSync(serverLog);
  await receiver?.close();

  const proc =
    collector === undefined || window === null
      ? null
      : summariseWindow(server.pid!, collector.snapshots, window);
  problems.push(...(proc?.problems ?? []));
  const tracePath =
    target.env.T3CODE_TRACE_FILE === undefined
      ? NodePath.join(baseDir, "userdata", "logs", "server.trace.ndjson")
      : NodePath.resolve(runDir, target.env.T3CODE_TRACE_FILE);
  const trace = window === null ? null : await readTraceWindow(tracePath, window);
  problems.push(...(trace?.problems ?? []));
  const spans = trace?.valid ? summariseSpans(trace.spans) : null;
  const otlp =
    receiver === undefined || window === null ? null : summariseGauges(receiver.points, window);
  if (otlp !== null && otlp.samples_in_window === 0) problems.push("otlp_no_samples");

  const metrics = JSON.parse(
    JSON.stringify({
      cpu: proc?.cpu ?? null,
      io: proc?.io ?? null,
      mem: proc?.mem ?? null,
      otlp,
      trace: spans === null ? null : { spans: spans.spans, errors: spans.errors },
      spans: spans?.by_name ?? null,
      sql: spans?.sql ?? null,
      driver:
        driver === null
          ? null
          : {
              thread_events_per_s: driver.thread_events_per_s,
              shell_events_per_s: driver.shell_events_per_s,
              calls: driver.calls,
            },
    }),
  ) as MetricTree;
  const required = traceRequired(target.env);
  const valid = !problems.some(
    (problem) => FATAL.has(problem) || (required && TRACE_PROBLEMS.has(problem)),
  );
  const record = {
    schema: "t3-server-bench/run/1",
    label: target.label,
    attempt,
    valid,
    problems: [...new Set(problems)],
    started_at: startedAt,
    window:
      window === null
        ? null
        : {
            start: new Date(window.startMs).toISOString(),
            end: new Date(window.endMs).toISOString(),
          },
    server: { pid: server.pid ?? null, exit_before_stop: serverExit },
    driver: driver === null ? null : { ok: driver.ok, error: driver.error, knobs: driver.knobs },
    proc:
      proc === null
        ? null
        : {
            source: proc.source,
            samples: proc.samples,
            elapsed_s: proc.elapsed_s,
            threads: proc.threads,
          },
    otlp_receiver: receiver?.stats ?? null,
    trace:
      trace === null
        ? null
        : {
            valid: trace.valid,
            problems: trace.problems,
            files: trace.files,
            bad_lines: trace.bad_lines,
            newest_after_window_end_s: trace.newest_after_window_end_s,
            lock_owners: spans?.lock_owners ?? null,
          },
    metrics,
  };
  NodeFS.writeFileSync(NodePath.join(runDir, "run.json"), `${JSON.stringify(record, null, 2)}\n`);
  if (!options.keepRuns) NodeFS.rmSync(baseDir, { recursive: true, force: true });
  console.error(
    `${target.label} attempt ${attempt}: ${valid ? "valid" : "INVALID"}${record.problems.length > 0 ? ` (${record.problems.join(", ")})` : ""}`,
  );
  return record;
}

const hostInfo = () => ({
  // A fingerprint, so results can be matched to a machine without publishing its name.
  host_id: NodeCrypto.createHash("sha256").update(NodeOS.hostname()).digest("hex").slice(0, 16),
  os: `${NodeOS.type()} ${NodeOS.release()}`,
  machine: NodeOS.machine(),
  cpu_model: NodeOS.cpus()[0]?.model ?? null,
  cpus: NodeOS.availableParallelism(),
  mem_total_gib: Math.round((NodeOS.totalmem() / 1024 ** 3) * 10) / 10,
  proc: procAvailable(),
  node: process.version,
});

async function main(argv: ReadonlyArray<string>): Promise<number> {
  const { values } = NodeUtil.parseArgs({
    args: [...argv],
    options: {
      ref: { type: "string", multiple: true },
      base: { type: "string", default: "perf/base" },
      fixture: { type: "string" },
      runs: { type: "string", default: "3" },
      retries: { type: "string", default: "2" },
      warmup: { type: "string", default: "60s" },
      window: { type: "string", default: "300s" },
      driver: { type: "string" },
      "driver-arg": { type: "string", multiple: true },
      sessions: { type: "string" },
      subscribers: { type: "string" },
      pollers: { type: "string" },
      "server-env": { type: "string", multiple: true },
      "ref-env": { type: "string", multiple: true },
      "server-cpus": { type: "string" },
      "driver-cpus": { type: "string" },
      "otlp-interval-ms": { type: "string", default: "1000" },
      out: { type: "string" },
      work: { type: "string" },
      "build-cmd": { type: "string", default: "vp i && vp run --filter t3 build" },
      "startup-timeout": { type: "string", default: "300s" },
      "keep-runs": { type: "boolean", default: false },
      help: { type: "boolean", short: "h" },
    },
  });
  if (values.help) {
    console.log(HELP);
    return 0;
  }
  const refs = values.ref ?? [];
  if (refs.length === 0) throw new Error("at least one --ref is required (see --help)");
  if (values.fixture === undefined) throw new Error("--fixture <file|dir|none> is required");
  if (values.out === undefined) throw new Error("--out <dir> is required");
  const runs = positiveInt(values.runs, "--runs");
  const retries = Number(values.retries);
  if (!Number.isSafeInteger(retries) || retries < 0) {
    throw new Error("--retries must be a whole number");
  }
  const warmupS = parseDuration(values.warmup, "--warmup");
  const windowS = parseDuration(values.window, "--window");
  if (windowS < 2) throw new Error("--window must be at least 2 s");
  const startupTimeoutS = parseDuration(values["startup-timeout"], "--startup-timeout");
  const otlpIntervalMs = Number(values["otlp-interval-ms"]);
  if (!Number.isInteger(otlpIntervalMs) || otlpIntervalMs < 0) {
    throw new Error("--otlp-interval-ms must be a whole number of ms (0 = off)");
  }
  for (const flag of ["server-cpus", "driver-cpus"] as const) {
    const cpus = values[flag];
    if (cpus === undefined) continue;
    if (!CPU_LIST.test(cpus)) throw new Error(`--${flag} must be a CPU list such as 0-3 or 0,2`);
    if (NodeChildProcess.spawnSync("taskset", ["-V"]).status !== 0) {
      throw new Error(`--${flag} needs taskset (Linux util-linux)`);
    }
  }
  for (const flag of ["sessions", "subscribers", "pollers"] as const) {
    if (values[flag] !== undefined) positiveInt(values[flag], `--${flag}`);
  }
  const outDir = NodePath.resolve(values.out);
  const workDir = NodePath.resolve(values.work ?? NodePath.join(outDir, "work"));
  NodeFS.mkdirSync(workDir, { recursive: true });
  if (values["driver-cpus"] !== undefined) {
    NodeChildProcess.spawnSync(
      "taskset",
      ["-a", "-p", "-c", values["driver-cpus"], String(process.pid)],
      {
        stdio: "ignore",
      },
    );
  }

  const globalEnv: Record<string, string> = { ...BENCH_DEFAULT_ENV };
  for (const assignment of values["server-env"] ?? []) {
    const [name, value] = parseAssignment(assignment, "--server-env");
    globalEnv[name] = value;
  }
  const baseSha = resolveCommit(repoRoot, values.base);
  const harnessSha = git(repoRoot, ["rev-parse", "HEAD"]);
  const harnessFilesSha = await filesHash(
    HARNESS_FILES.map((file) => NodePath.join(harnessDir, file)),
  );
  const fixture = await fixtureInfo(values.fixture, workDir);
  const prepared = refs.map((spec) => prepareTarget(spec, workDir, values["build-cmd"]));
  const labels = new Set(prepared.map((target) => target.label));
  if (labels.size !== prepared.length)
    throw new Error("two --ref values share a label; name them label=ref");
  const refEnv = new Map<string, Record<string, string>>();
  for (const entry of values["ref-env"] ?? []) {
    const colon = entry.indexOf(":");
    const label = entry.slice(0, colon);
    if (colon <= 0 || !labels.has(label))
      throw new Error(`--ref-env ${entry}: unknown label ${label}`);
    const [name, value] = parseAssignment(entry.slice(colon + 1), "--ref-env");
    refEnv.set(label, { ...refEnv.get(label), [name]: value });
  }
  const targets: Array<Target> = prepared.map((target) => ({
    ...target,
    env: { ...globalEnv, ...refEnv.get(target.label) },
  }));

  const driverPath =
    values.driver === undefined
      ? NodeFS.existsSync(NodePath.join(harnessDir, "driver.ts"))
        ? NodePath.join(harnessDir, "driver.ts")
        : NodePath.join(harnessDir, "driver-stub.ts")
      : values.driver === "stub"
        ? NodePath.join(harnessDir, "driver-stub.ts")
        : NodePath.resolve(values.driver);
  const driverArgs = [
    ...(["sessions", "subscribers", "pollers"] as const).flatMap((flag) =>
      values[flag] === undefined ? [] : [`--${flag}`, values[flag]!],
    ),
    ...(values["driver-arg"] ?? []),
  ];
  const driverSha = await driverHash(driverPath);
  const fakesSha = await fakesHash(harnessDir);
  const transcriptsSha = new Map<string, string>();
  for (const target of targets) {
    transcriptsSha.set(target.label, await transcriptsHash(target.env, fixture, REPLAY_FIXTURES));
  }
  const options: RunOptions = {
    fixture,
    workDir,
    warmupS,
    windowS,
    driverPath,
    driverArgs,
    serverCpus: values["server-cpus"],
    driverCpus: values["driver-cpus"],
    otlpIntervalMs,
    startupTimeoutS,
    keepRuns: values["keep-runs"],
  };

  const onSignal = () => {
    for (const child of live) killGroup(child, "SIGKILL");
    process.exit(130);
  };
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);

  const records = new Map<string, Array<Awaited<ReturnType<typeof runOnce>>>>(
    targets.map((target) => [target.label, []]),
  );
  const order: Array<string> = [];
  // A failed attempt is retried in its own slot, so the counterbalanced order holds.
  for (const [slot, label] of runOrder(
    targets.map((target) => target.label),
    runs,
  ).entries()) {
    const target = targets.find((candidate) => candidate.label === label)!;
    const done = records.get(label)!;
    for (let retry = 0; retry <= retries; retry++) {
      const record = { ...(await runOnce(target, done.length + 1, options)), slot };
      done.push(record);
      order.push(`${label}#${record.attempt}${record.valid ? "" : " (invalid)"}`);
      if (record.valid) break;
    }
  }

  NodeFS.mkdirSync(outDir, { recursive: true });
  const results: Array<BenchResult> = [];
  for (const target of targets) {
    const all = records.get(target.label)!;
    const valid = all.filter((record) => record.valid);
    const { metrics, spread } = aggregateRuns(valid.map((record) => record.metrics));
    const knobEnv = {
      ...target.env,
      ...(otlpIntervalMs > 0
        ? {
            T3CODE_OTLP_PROTOCOL: "http/json",
            T3CODE_OTLP_EXPORT_INTERVAL_MS: String(otlpIntervalMs),
          }
        : {}),
    };
    const serverKnobs = knobsFromEnv(knobEnv);
    const result: BenchResult = {
      schema: RESULT_SCHEMA,
      label: target.label,
      created_at: new Date().toISOString(),
      identity: {
        server_sha: target.sha,
        server_ref: target.ref,
        server_dirty: target.dirty,
        server_tree: target.tree,
        server_version: target.version,
        base_sha: baseSha,
        base_ref: values.base,
        harness_sha: harnessSha,
        harness_files_sha256: harnessFilesSha,
        fixture: { kind: fixture.kind, sha256: fixture.sha256, bytes: fixture.bytes },
      },
      sampling: {
        ...traceSampling(target.env),
        otlp_export_interval_ms: otlpIntervalMs,
        collect_interval_ms: 1000,
      },
      knobs: {
        server: Object.fromEntries(
          Object.entries(serverKnobs).filter(([name]) => !name.startsWith("BENCH_")),
        ),
        workload: {
          driver: NodePath.basename(driverPath),
          driver_sha256: driverSha,
          fakes_sha256: fakesSha,
          transcripts_sha256: transcriptsSha.get(target.label),
          warmup_s: warmupS,
          window_s: windowS,
          driver_args: driverArgs,
          ...Object.fromEntries(
            Object.entries(serverKnobs).filter(
              ([name]) => name.startsWith("BENCH_") && !isPlumbing(name),
            ),
          ),
          // The values above may be redacted; this compares the raw load settings.
          bench_env_sha256: benchEnvHash(target.env),
        },
        run: {
          runs,
          retries,
          server_entry: target.entryKind,
          server_cpus: values["server-cpus"] ?? null,
          driver_cpus: values["driver-cpus"] ?? null,
          startup_timeout_s: startupTimeoutS,
        },
      },
      host: hostInfo(),
      runs: { requested: runs, attempted: all.length, valid: valid.length, order },
      metrics,
      spread,
      run_details: all,
    };
    NodeFS.writeFileSync(
      NodePath.join(outDir, `${target.label}.result.json`),
      `${JSON.stringify(result, null, 2)}\n`,
    );
    results.push(result);
  }

  let status = results.every((result) => result.runs.valid === runs) ? 0 : 1;
  if (results.length >= 2) {
    const sections: Array<string> = [];
    for (const after of results.slice(1)) {
      const outcome = compareResults(results[0]!, after);
      if (outcome.ok) {
        sections.push(outcome.markdown);
      } else {
        status = 2;
        sections.push(
          `Refused to compare ${results[0]!.label} and ${after.label}:\n${outcome.problems.map((problem) => `- ${problem}`).join("\n")}\n`,
        );
      }
    }
    const markdown = sections.join("\n");
    NodeFS.writeFileSync(NodePath.join(outDir, "compare.md"), markdown);
    process.stdout.write(markdown);
  }
  console.error(`results in ${outDir}`);
  return status;
}

if (import.meta.main) {
  main(process.argv.slice(2)).then(
    (code) => process.exit(code),
    (error: unknown) => {
      for (const child of live) killGroup(child, "SIGKILL");
      console.error(`run: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(2);
    },
  );
}

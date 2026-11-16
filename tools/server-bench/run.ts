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
import { compareTree } from "./archive-compare.ts";
import {
  RESULT_SCHEMA,
  aggregateRuns,
  compareResults,
  type BenchResult,
  type MetricTree,
} from "./report.ts";

/** The --base every result records when none is given. */
export const DEFAULT_BASE = "main";

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
  --ref sea:<dir>          the shipped single executable: <dir> is an unpacked archive (t3, client,
                           node_modules, resource-monitor, lib) with, beside t3, either
                           - release.json, written by release.ts when it verified and unpacked an
                             upstream release archive (tag, commit, archive sha256, t3's sha256), or
                           - a fork build's build-manifest.txt (extracted_t3_sha256 names t3's sha256).
                           t3 is hashed again and refused when it differs from what was unpacked. Before
                           t3 first runs it must be a Node single executable for the platform (ELF or
                           Mach-O, single-executable fuse set), and an upstream release needs
                           --archive: its sha256 must be release.json's and the whole unpacked tree must
                           be the archive's: every file's bytes and permission bits and every link, with
                           nothing added but release.json (which alone is a claim anyone can write).
                           Runs <dir>/t3 serve as shipped (server_entry "sea"; the archive and binary
                           sha256 in identity, server_archive_verified). A release must ship an executable
                           resource-monitor/<platform>/t3-resource-monitor; T3CODE_RESOURCE_MONITOR_PATH
                           points at it unless the run sets it (then to an executable file). A
                           SEA honours NODE_OPTIONS V8 flags and --require but skips --import: pass
                           preloads with --require. A SEA built with Node options baked in and
                           execArgvExtension "cli" (exec-argv.json beside t3, recorded as
                           identity.server_exec_argv) ignores NODE_OPTIONS: it is started with them as a
                           leading --node-options=<NODE_OPTIONS>, as its own service launcher does, and
                           its children still get NODE_OPTIONS from the environment.
  Startup flags: every ref is asked once for the V8 flags its build wants (<entry> __node-flags; a
  build without it answers "Unknown command" and runs as before; any other probe failure stops
  the bench). The server is started with a
  bootstrap descriptor, so it does not relaunch itself with them; the harness sets them as the
  build's own launcher does (NODE_OPTIONS for the server, T3CODE_INHERITED_NODE_OPTIONS for its
  children) and records them in identity.server_node_flags. Each run then checks the server
  process: <run>/heap-config.json (written by the bench preload: heap_size_limit_bytes,
  node_options, exec_argv) must show those NODE_OPTIONS and the semi-space size the build
  reported, measured as (heap limit - --max-old-space-size) / 3. A mismatch, or a run that
  cannot be measured (no heap-config.json, no --max-old-space-size), is invalid
  (node_flags_mismatch / node_flags_unverified). run.json keeps the check and the server's own
  server.v8 log entry when it logs at Info.
  label=...                names the result (default: the ref with / as -).
  --archive [<label>=]<path>  the verified archive a sea: ref was unpacked from (required for an
                           upstream release; repeatable, the label naming its --ref; without one it
                           applies to the only --ref).
  --base <ref>             the base every result records; compare refuses different bases
                           [${DEFAULT_BASE}].
  --build-cmd <sh>         [vp i && vp run --filter t3 build]

Fixture (restored for every run with a reflink copy where the filesystem supports it)
  --fixture <file>         a checkpointed statev2.sqlite (VACUUM INTO output; a non-empty -wal next
                           to it is refused), copied to <base>/userdata/statev2.sqlite.
  --fixture <dir>          a whole base-dir template (userdata/statev2.sqlite, settings.json ...);
                           regular files and directories only (a symlink is refused).
  --fixture none           an empty database (the server creates it).
  --settings <file>        a settings.json copied to every run's <base>/userdata/settings.json (over a
                           template's): the bench provider instances (providers.ts settings). The driver
                           needs them, so an empty-database run is --fixture none --settings <file>.
                           Its sha256 is recorded; the launchers and transcripts it names are
                           fingerprinted as a template's are.
  --seed-compacted-at      write <base>/userdata/compacted-at = now before the server starts, so no
                           background compaction pass is due during the run (the server compacts 6 h
                           after the marker); run.json records it and what the fixture had.
  The fixture's sha256 (cached by path, size and mtime in <work>/fixture-hashes.json) is recorded.

Load
  --driver <path|stub>     [driver.ts beside this file when it exists, else driver-stub.ts]
  --warmup 60s --window 300s, --sessions N (>= 1), --subscribers/--pollers N (>= 0; 0 pollers is
  the one-session profile), --turn-gap DUR (each session's pause between turns), all passed through
  to the driver when given,
  --driver-arg=<arg>       repeated, appended verbatim (e.g. --driver-arg=--mode --driver-arg=fresh).
  The driver (the load-driver contract) is started as: node <driver> --url <origin> --out <run>/driver
  --server-pid <pid> --warmup <n>s --window <n>s [--sessions N] [--subscribers N] [--pollers N]
  [driver args], with T3_BENCH_TOKEN (a bearer token from the server's bootstrap grant) in its
  environment. It writes <out>/driver.json: { status: "ok"|"failed", failure?: { reason },
  timeline: { window: <ISO> }, windowSeconds, calls: { <type>: { count, errors, unfinished, p50,
  p95, p99, max } }, counters: { "events.thread": { perSecond }, ... }, knobs }; status "failed", a
  driver.failed marker or no file is driver_failed (run.json driver_failure says why). A complete
  window the driver judged unpublishable gives each of its findings as a problem instead:
  sessions_starved, session_mcp_missing, call_timeouts (errors over the limit, all timeouts),
  error_rate (errors over the limit, some not timeouts), driver_saturated, distribution_mismatch.
  Every run.json problem is {code, kind}: with the server's window ELU at or above 0.95 (the
  preload's, else the server gauge's) the first three are "saturation" and the attempt stays valid
  and saturated; everything else is a "defect", and a fatal defect voids the attempt whatever the
  load. Session statistics are over the sessions the server served; run.json records
  sessions_served, sessions_total, saturated and saturation {elu, elu_source, threshold}.
  Without a window in it, the run uses
  [driver start + warmup, + window].
  The workload fingerprint hashes the driver (its file, module directory and the local modules
  they import), the fake providers (fake-*/ and what they import) and the transcripts they replay
  (the server's recorded replay fixtures, and every absolute BENCH_*TRANSCRIPT path in the server
  env or the fixture's settings.json), and launchers_sha256 what the fixture's settings.json runs
  (enabled instances' binaryPath, commandPath, absolute commandArgs, e.g. from providers.ts
  --claude-launcher): a launcher in this harness's fake-*/ adds its path, one in another
  checkout's fake-*/ that checkout's fakes and recorded replay fixtures, a native executable (ELF,
  Mach-O, PE) its file, a directory (e.g. an agent node runs) every file under it; anything else
  (a script, JavaScript, data) is refused, as what it runs cannot be hashed.

Server
  --server-env K=V         every ref; --ref-env <label>:K=V one ref (wins). The fake claude's BENCH_*
                           load knobs go here too (recorded as workload). T3CODE_*, T3_*, OTEL_*,
                           BENCH_*, NODE_OPTIONS and UV_THREADPOOL_SIZE from this shell are NOT
                           passed on, so the recorded knobs are exactly the applied ones.
                           Defaults: T3CODE_TRACE_MAX_BYTES=67108864, T3CODE_TRACE_MAX_FILES=64 (no
                           window rotates away), T3CODE_TRACE_SQL_MIN_DURATION_MS=0 (all SQL spans).
  --preload <file|none>    loaded into the server with NODE_OPTIONS --require=<file> (appended to the
                           server's NODE_OPTIONS; a single executable honours --require, not --import)
                           [preload/bench-preload.cjs beside this file]. It writes heap-config.json,
                           loop-1hz.ndjson and, over the measured window, loop-window.json into the run
                           directory; run.ts touches <run>/ctl/window-start and window-end on the
                           driver's "phase: window" / "phase: drain" lines. run.json records whether it
                           loaded (preload.loaded) and metrics.loop (event-loop delay, ELU, GC) from
                           its window; the result records preload: true only when every valid run
                           loaded it. A NODE_OPTIONS that already requires a bench preload is refused.
  --now <ISO>              the record's pinned date-time, recorded in identity.now and run.json. The
                           server keeps the real clock; the fixture's dates are pinned by its seed.
  --otlp-interval-ms N     server gauge export interval to the local receiver; 0 = off [1000].
  --server-cpus / --driver-cpus <list>   taskset CPU lists (Linux); the harness itself joins the
                           driver CPUs.
  --startup-timeout 300s, --stop-timeout 120s (SIGTERM to the server's process group, then SIGKILL
  after this; run.json server.stop records forced and signal_to_exit_ms for every run: a forced
  stop may lose shutdown spans; 120 s is over 6 times the longest saturated stop measured to end on
  SIGTERM, 19.6 s, and 4 times the 30 s at which three saturated 250-session stops were cut off),
  --runs 3, --retries 2 (per run slot), --work <out>/work, --keep-runs
  (keep each run's base dir with its database and trace; deleted by default).

Harness identity: the result records the harness version, its commit and the sha256 of its run-time
closure (tools/server-bench without tests, docs and install state; the replay agent and recordings
the fakes load; package.json). In a git checkout the commit is HEAD; an installed harness (a git
archive unpacked by install-harness.sh, no .git) reads it from t3cp-harness.json at the repository
root and is refused when its files no longer hash to that file's files_sha256. Without git,
--base is recorded as given (its SHA only when it is one).

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
  "node_flags_mismatch",
  "node_flags_unverified",
  "db_setup_failed",
  "db_ready_timeout",
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

/** A whole number of at least `minimum` (0 where the flag allows none, e.g. --pollers 0). */
export function parseCount(value: string | undefined, flag: string, minimum: number): number {
  const number = Number(value);
  if (value === undefined || value.trim() === "" || !Number.isSafeInteger(number)) {
    throw new Error(`${flag} must be a whole number of at least ${minimum}`);
  }
  if (number < minimum) throw new Error(`${flag} must be a whole number of at least ${minimum}`);
  return number;
}

const positiveInt = (value: string | undefined, flag: string) => parseCount(value, flag, 1);

/** The driver's duration syntax (60s, 5m, 1500ms, 1h; a bare number is seconds). */
const DRIVER_DURATION = /^\d+(?:\.\d+)?(?:ms|s|m|h)?$/;

/**
 * --now: an ISO 8601 date-time with seconds and a zone (Z or ±hh:mm), normalised to UTC. The record's pinned date;
 * the server itself keeps the real clock.
 */
export function parseNow(value: string): string {
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.exec(
      value.trim(),
    );
  const ms = Date.parse(value.trim());
  if (match === null || !Number.isFinite(ms)) {
    throw new Error(`--now must be an ISO date-time with a zone, e.g. 2026-11-17T00:00:00Z`);
  }
  const [, year, month, day, hour, minute, second] = match.map(Number) as Array<number>;
  // Date.parse rolls over impossible dates (2026-02-30) on some engines: the fields must survive the round trip.
  if (
    month! < 1 ||
    month! > 12 ||
    day! < 1 ||
    day! > 31 ||
    hour! > 23 ||
    minute! > 59 ||
    second! > 59 ||
    new Date(Date.UTC(year!, month! - 1, day!)).getUTCDate() !== day
  ) {
    throw new Error(`--now must be an ISO date-time with a zone, e.g. 2026-11-17T00:00:00Z`);
  }
  return new Date(ms).toISOString();
}

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

/**
 * The harness's run-time closure, relative to the repository root: what install-harness.sh installs and the runs
 * execute (the harness, driver, fakes, preload and installer under tools/server-bench, the replay agent and recordings
 * the fakes load, and the root package.json that makes the .ts files ES modules). Its sha256 is
 * harness_files_sha256, the key that says two records' harness behaved alike.
 */
const harnessClosure = () => [
  "package.json",
  "tools/server-bench",
  "apps/server/scripts/dev-replay-agent.ts",
  RECORDINGS,
];

/** Not part of the closure: tests, docs, test data and what an install generates. */
const outsideClosure = (relative: string) =>
  /\.test\.(?:ts|mjs)$/.test(relative) ||
  relative.endsWith(".md") ||
  relative.endsWith(".server-log.txt") ||
  relative.startsWith("tools/server-bench/settings/") ||
  relative.startsWith("tools/server-bench/state/") ||
  relative.split("/").some((part) => part.startsWith("."));

/** The closure's regular files under `root`, as sorted repository-relative paths. */
function closureFiles(root: string): Array<string> {
  const files: Array<string> = [];
  const visit = (relative: string) => {
    const stat = NodeFS.lstatSync(NodePath.join(root, relative), { throwIfNoEntry: false });
    if (stat === undefined || outsideClosure(relative)) return;
    if (stat.isDirectory()) {
      for (const name of NodeFS.readdirSync(NodePath.join(root, relative))) {
        visit(`${relative}/${name}`);
      }
    } else if (stat.isFile()) {
      files.push(relative);
    }
  };
  for (const entry of harnessClosure()) visit(entry);
  return files.toSorted();
}

/** The harness release this tree is; a record carries it with the commit and the files' sha256. */
export const HARNESS_VERSION = "t3cp-harness/2.0.2";
/** Written at the repository root by install-harness.sh, where a git archive has no .git to ask. */
export const HARNESS_IDENTITY_FILE = "t3cp-harness.json";

/**
 * The harness version and commit: HEAD in a git checkout, else t3cp-harness.json (an installed git archive). Neither is
 * an error: a record must name the harness that produced it.
 */
export async function harnessIdentity(root: string): Promise<{
  readonly version: string;
  readonly sha: string;
  /** A snapshot commit's source: the branch commit whose tree it holds; null when the commit is the branch's own. */
  readonly source_sha: string | null;
  readonly source: "git" | "file";
}> {
  const head = NodeChildProcess.spawnSync("git", ["-C", root, "rev-parse", "--verify", "HEAD"], {
    encoding: "utf8",
  });
  const inGit =
    head.status === 0 &&
    NodeChildProcess.spawnSync("git", ["-C", root, "rev-parse", "--show-toplevel"], {
      encoding: "utf8",
    }).stdout.trim() === NodeFS.realpathSync(root);
  if (inGit) {
    return { version: HARNESS_VERSION, sha: head.stdout.trim(), source_sha: null, source: "git" };
  }
  const file = NodePath.join(root, HARNESS_IDENTITY_FILE);
  if (!NodeFS.existsSync(file)) {
    throw new Error(
      `${root}: no git checkout and no ${HARNESS_IDENTITY_FILE} (install the harness with install-harness.sh)`,
    );
  }
  const parsed = recordOf(JSON.parse(NodeFS.readFileSync(file, "utf8")) as unknown);
  const sha = parsed.sha;
  const version = parsed.version;
  if (typeof sha !== "string" || !/^[0-9a-f]{40}$/.test(sha)) {
    throw new Error(`${file}: no 40-hex sha`);
  }
  if (typeof version !== "string" || version !== HARNESS_VERSION) {
    throw new Error(
      `${file}: version ${String(version)} is not this tree's ${HARNESS_VERSION} (reinstall the harness)`,
    );
  }
  const sourceSha = parsed.source_sha ?? null;
  if (sourceSha !== null && (typeof sourceSha !== "string" || !/^[0-9a-f]{40}$/.test(sourceSha))) {
    throw new Error(`${file}: source_sha is not a 40-hex commit`);
  }
  // The installed files must still be the ones the identity names (an edit after install is another harness).
  const files = parsed.files_sha256;
  if (typeof files !== "string" || !/^[0-9a-f]{64}$/.test(files)) {
    throw new Error(`${file}: no 64-hex files_sha256 (reinstall the harness)`);
  }
  const computed = await harnessFilesSha256(root);
  if (computed !== files) {
    throw new Error(
      `${file}: files_sha256 ${files} differs from the harness files' ${computed} (the installed harness was changed)`,
    );
  }
  return { version, sha, source_sha: sourceSha, source: "file" };
}

/** sha256 over the harness's run-time closure under `root` (each file's path and bytes), as every result records it. */
export async function harnessFilesSha256(root: string = repoRoot): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  for (const relative of closureFiles(root)) {
    hash.update(`${relative}\0${await sha256File(NodePath.join(root, relative))}\n`);
  }
  return hash.digest("hex");
}

const sha256Sync = (path: string) =>
  NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(path)).digest("hex");

async function filesHash(paths: ReadonlyArray<string>): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  for (const path of paths) hash.update(`${NodePath.basename(path)}\0${await sha256File(path)}\n`);
  return hash.digest("hex");
}

const isFile = (path: string) =>
  NodeFS.statSync(path, { throwIfNoEntry: false })?.isFile() === true;

/** Relative specifiers of static, re-export, side-effect and literal dynamic imports and requires. */
const LOCAL_IMPORT = /(?:\bfrom|\bimport|\brequire)\s*\(?\s*["'](\.{1,2}\/[^"'\n]+)["']/g;

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

const RECORDINGS = "apps/server/src/orchestration-v2/testkit/fixtures";
const REPLAY_FIXTURES = NodePath.join(repoRoot, RECORDINGS);
const TRANSCRIPT_VAR = /^BENCH_\w*TRANSCRIPT$/;

/** The settings.json every run gets: --settings when given, else a directory fixture's own; null when none. */
function settingsFile(fixture: FixtureInfo): string | null {
  if (fixture.settings != null) return fixture.settings.path;
  return fixture.kind === "dir" ? NodePath.join(fixture.path!, "userdata", "settings.json") : null;
}

/** The provider instances of the settings.json the runs get (--settings or a directory fixture's); none otherwise. */
function settingsInstances(fixture: FixtureInfo): Array<Record<string, unknown>> {
  const settings = settingsFile(fixture);
  if (settings === null || !isFile(settings)) return [];
  let parsed: Record<string, unknown>;
  try {
    parsed = recordOf(JSON.parse(NodeFS.readFileSync(settings, "utf8")));
  } catch (error) {
    throw new Error(`${settings}: ${error instanceof Error ? error.message : String(error)}`, {
      cause: error,
    });
  }
  return Object.values(recordOf(parsed.providerInstances)).map(recordOf);
}

/** The BENCH_*TRANSCRIPT values of `env` and of the instances in a directory fixture's settings. */
function transcriptSettings(
  env: Readonly<Record<string, string>>,
  fixture: FixtureInfo,
): Array<string> {
  const values = Object.entries(env)
    .filter(([name]) => TRANSCRIPT_VAR.test(name))
    .map(([, value]) => value);
  for (const instance of settingsInstances(fixture)) {
    const environment = instance.environment;
    for (const entry of Array.isArray(environment) ? environment.map(recordOf) : []) {
      if (typeof entry.name === "string" && TRANSCRIPT_VAR.test(entry.name)) {
        if (typeof entry.value === "string") values.push(entry.value);
      }
    }
  }
  return values;
}

/** Adds the `*.ndjson` recordings under `fixturesDir` (the fakes load them by name) to `hash`. */
async function hashRecordings(hash: NodeCrypto.Hash, fixturesDir: string): Promise<void> {
  const recorded = NodeFS.existsSync(fixturesDir)
    ? (NodeFS.readdirSync(fixturesDir, { recursive: true }) as Array<string>)
        .filter((relative) => relative.endsWith(".ndjson"))
        .toSorted()
    : [];
  for (const relative of recorded) {
    hash.update(`${relative}\0${await sha256File(NodePath.join(fixturesDir, relative))}\n`);
  }
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
  await hashRecordings(hash, fixturesDir);
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

// ELF, Mach-O (both byte orders, 32/64-bit, universal) and PE: a self-contained executable.
const NATIVE_MAGIC = ["7f454c46", "feedface", "feedfacf", "cefaedfe", "cffaedfe", "cafebabe"];

const isNativeExecutable = (path: string) => {
  const head = Buffer.alloc(4);
  const fd = NodeFS.openSync(path, "r");
  try {
    const read = NodeFS.readSync(fd, head, 0, 4, 0);
    const hex = head.subarray(0, read).toString("hex");
    return NATIVE_MAGIC.includes(hex) || hex.startsWith("4d5a");
  } finally {
    NodeFS.closeSync(fd);
  }
};

/** Every file under `root` (links followed, a cycle cut) by sorted relative path, with its bytes. */
async function directoryHash(root: string): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  const ancestors = new Set<string>();
  const walk = async (directory: string, prefix: string): Promise<void> => {
    const real = NodeFS.realpathSync(directory);
    if (ancestors.has(real)) {
      hash.update(`${prefix}\0cycle\n`);
      return;
    }
    ancestors.add(real);
    for (const name of NodeFS.readdirSync(directory).toSorted()) {
      const path = NodePath.join(directory, name);
      const relative = prefix === "" ? name : `${prefix}/${name}`;
      const stat = NodeFS.statSync(path, { throwIfNoEntry: false });
      if (stat === undefined) hash.update(`${relative}\0missing\n`);
      else if (stat.isDirectory()) await walk(path, relative);
      else if (stat.isFile()) hash.update(`${relative}\0${await sha256File(path)}\n`);
    }
    ancestors.delete(real);
  };
  await walk(root, "");
  return hash.digest("hex");
}

const realPath = (path: string) => {
  try {
    return NodeFS.realpathSync(path);
  } catch {
    return null;
  }
};

/**
 * The absolute binaryPath, commandPath and commandArgs of the fixture settings' enabled instances:
 * one in this harness's fake-* dirs adds its path (fakes_sha256 has it), one in another
 * checkout's adds that checkout's fakes and recordings, a native executable its file, a
 * directory every file under it; anything else is refused.
 */
export async function launchersHash(fixture: FixtureInfo, benchDir: string): Promise<string> {
  const launchers = new Set<string>();
  for (const instance of settingsInstances(fixture)) {
    const config = recordOf(instance.config);
    // A disabled instance runs nothing (providers.ts settings keeps built-ins' config, disabled).
    if (instance.enabled === false || config.enabled === false) continue;
    const args = Array.isArray(config.commandArgs) ? config.commandArgs : [];
    for (const value of [config.binaryPath, config.commandPath, ...args]) {
      if (typeof value === "string" && NodePath.isAbsolute(value)) launchers.add(value);
    }
  }
  const harness = realPath(benchDir);
  const checkouts = new Map<string, string>();
  const checkoutHash = async (checkout: string) => {
    if (!checkouts.has(checkout)) {
      const recordings = NodeCrypto.createHash("sha256");
      await hashRecordings(recordings, NodePath.resolve(checkout, "../..", RECORDINGS));
      checkouts.set(checkout, `${await fakesHash(checkout)}+${recordings.digest("hex")}`);
    }
    return checkouts.get(checkout)!;
  };
  const hash = NodeCrypto.createHash("sha256");
  for (const launcher of [...launchers].toSorted()) {
    const real = realPath(launcher);
    let content: string;
    if (real !== null && NodeFS.statSync(real).isDirectory()) {
      content = `directory ${await directoryHash(real)}`;
    } else if (real === null || !isFile(real)) {
      content = "not a file";
    } else if (FAKE_DIRS.includes(NodePath.basename(NodePath.dirname(real)))) {
      const checkout = NodePath.dirname(NodePath.dirname(real));
      content =
        checkout === harness ? "harness" : `checkout ${checkout} ${await checkoutHash(checkout)}`;
    } else if (!isNativeExecutable(real)) {
      throw new Error(
        `${launcher} (fixture settings) is outside a fake-* directory and not a native executable: what it runs cannot be fingerprinted; move it into a fake-* directory`,
      );
    } else {
      content = await sha256File(real);
    }
    hash.update(`${launcher}\0${content}\n`);
  }
  return hash.digest("hex");
}

export interface FixtureInfo {
  readonly kind: "file" | "dir" | "empty";
  readonly path: string | null;
  readonly sha256: string;
  readonly bytes: number | null;
  /** --settings: the settings.json copied into every run's userdata (over a template's); null or absent when none. */
  readonly settings?: { readonly path: string; readonly sha256: string } | null;
}

/** The fixture, plus the --settings file every run gets (a regular file, hashed). */
export async function fixtureInfo(
  spec: string,
  workDir: string,
  settingsSpec?: string,
): Promise<FixtureInfo> {
  let settings: FixtureInfo["settings"] = null;
  if (settingsSpec !== undefined) {
    const settingsPath = NodeFS.realpathSync(settingsSpec);
    if (!isFile(settingsPath)) throw new Error(`--settings ${settingsSpec} is not a file`);
    try {
      recordOf(JSON.parse(NodeFS.readFileSync(settingsPath, "utf8")) as unknown);
    } catch (error) {
      throw new Error(`--settings ${settingsSpec}: not JSON: ${String(error)}`, { cause: error });
    }
    settings = { path: settingsPath, sha256: await sha256File(settingsPath) };
  }
  const info = await fixtureFiles(spec, workDir);
  return { ...info, settings };
}

async function fixtureFiles(spec: string, workDir: string): Promise<FixtureInfo> {
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

export function restoreFixture(fixture: FixtureInfo, baseDir: string) {
  NodeFS.mkdirSync(NodePath.join(baseDir, "userdata"), { recursive: true });
  // A copy keeps its source's mode, and a read-only fixture would leave the server a read-only database.
  if (fixture.kind === "file") {
    const target = NodePath.join(baseDir, "userdata", "statev2.sqlite");
    NodeFS.rmSync(target, { force: true });
    NodeFS.copyFileSync(fixture.path!, target, NodeFS.constants.COPYFILE_FICLONE);
    NodeFS.chmodSync(target, 0o600);
  } else if (fixture.kind === "dir") {
    NodeFS.cpSync(fixture.path!, baseDir, {
      recursive: true,
      mode: NodeFS.constants.COPYFILE_FICLONE,
    });
    ownerWritable(baseDir);
  }
  if (fixture.settings != null) {
    const target = NodePath.join(baseDir, "userdata", "settings.json");
    NodeFS.rmSync(target, { force: true });
    NodeFS.copyFileSync(fixture.settings.path, target);
    NodeFS.chmodSync(target, 0o600);
  }
}

/** Adds the owner's read and write bits to every file under `dir`, and read, write and search to every directory. */
function ownerWritable(dir: string): void {
  NodeFS.chmodSync(dir, NodeFS.statSync(dir).mode | 0o700);
  for (const entry of NodeFS.readdirSync(dir, { withFileTypes: true })) {
    const path = NodePath.join(dir, entry.name);
    if (entry.isDirectory()) ownerWritable(path);
    else if (entry.isFile()) NodeFS.chmodSync(path, NodeFS.statSync(path).mode | 0o600);
  }
}

const COMPACTED_AT = "compacted-at";

/**
 * Writes `<base>/userdata/compacted-at` = `nowMs` (the server's compaction marker; it compacts 6 h after it), so no
 * background compaction pass falls in the run. Returns what the fixture carried and the value written.
 */
export function seedCompactedAt(
  baseDir: string,
  nowMs: number,
): {
  readonly in_fixture: boolean;
  readonly fixture_value: string | null;
  readonly seeded_ms: number;
} {
  const marker = NodePath.join(baseDir, "userdata", COMPACTED_AT);
  const previous = NodeFS.existsSync(marker) ? NodeFS.readFileSync(marker, "utf8").trim() : null;
  NodeFS.mkdirSync(NodePath.dirname(marker), { recursive: true });
  NodeFS.writeFileSync(marker, String(nowMs));
  return { in_fixture: previous !== null, fixture_value: previous, seeded_ms: nowMs };
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
  readonly entryKind: "dist" | "source" | "sea";
  readonly version: string | null;
  /** A SEA ref (the shipped single executable from an unpacked archive): its build manifest and binary hash. */
  readonly archive: SeaArchive | null;
  readonly env: Readonly<Record<string, string>>;
  /** What the build's `__node-flags` asks its launcher for; set once per ref before the first run. */
  readonly nodeFlags?: NodeFlags;
  /** The resource monitor the server is pointed at (T3CODE_RESOURCE_MONITOR_PATH); null when none is set. */
  readonly resourceMonitor?: ResourceMonitorChoice | null;
  /** A sea: ref whose t3 was proven from its --archive. */
  readonly archiveVerified?: boolean;
}

/**
 * `<entry> __node-flags`: the V8 startup flags a build wants. A server started with a bootstrap descriptor (as here) does
 * not relaunch itself with them, so the harness sets them as the build's launcher does (`serverChildEnvironment`).
 * Builds without the command answer "Unknown command" and are launched as before.
 */
export interface NodeFlagsReport {
  readonly applied: boolean;
  /** The --max-semi-space-size value in effect (the flag's last value), null when none is set. */
  readonly maxSemiSpaceMb: number | null;
  readonly nodeOptionsAtStart: string | null;
  readonly nodeOptionsForChildren: string | null;
  readonly keptReason: string | null;
}

export interface NodeFlags {
  readonly supported: boolean;
  readonly report: NodeFlagsReport | null;
  /** Why the build has no report (exit status, or output that is not a report); null when supported. */
  readonly unsupportedReason: string | null;
  /** Added to the server's env for every run of this ref; empty when the build asks for nothing. */
  readonly env: Readonly<Record<string, string>>;
  /** The semi-space size (MiB, as V8 applies it: rounded up to a power of two) the server must run with; null = Node's default. */
  readonly expectedSemiSpaceMb: number | null;
}

export const NODE_FLAGS_COMMAND = "__node-flags";
const INHERITED_NODE_OPTIONS_ENV = "T3CODE_INHERITED_NODE_OPTIONS";

/**
 * The report on the last non-empty stdout line, or null when the output is not a complete, consistent report:
 * `applied` true needs the size it applied (a positive number), the server's NODE_OPTIONS carrying that size as the
 * flag's last value, and the children's NODE_OPTIONS (string or null); `applied` false needs the reason it kept the
 * process as it was, and its size (number or null) must be the one in its NODE_OPTIONS.
 */
export function parseNodeFlagsReport(stdout: string): NodeFlagsReport | null {
  const last = stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .at(-1);
  if (last === undefined) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(last);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;
  const record = parsed as Record<string, unknown>;
  const applied = record["applied"];
  const mb = record["maxSemiSpaceMb"];
  const atStart = record["nodeOptionsAtStart"];
  const forChildren = record["nodeOptionsForChildren"];
  const kept = record["keptReason"];
  const stringOrNull = (value: unknown) => typeof value === "string" || value === null;
  if (typeof applied !== "boolean") return null;
  if (!stringOrNull(atStart) || !stringOrNull(forChildren)) return null;
  // 0 is valid: Node takes --max-semi-space-size=0 as "use the default".
  if (!(mb === null || (typeof mb === "number" && Number.isFinite(mb) && mb >= 0))) return null;
  if (applied && (mb === null || typeof atStart !== "string")) return null;
  if (!applied && typeof kept !== "string") return null;
  if (kept !== undefined && typeof kept !== "string") return null;
  const execArgv = record["execArgv"];
  if (
    execArgv !== undefined &&
    !(Array.isArray(execArgv) && execArgv.every((arg) => typeof arg === "string"))
  ) {
    return null;
  }
  // The size it reports must be what its flags set (the server derives it from execArgv, then NODE_OPTIONS).
  const flags = (execArgv ?? []) as ReadonlyArray<string>;
  if (lastSizeFlag("--max-semi-space-size", atStart as string | null, flags) !== mb) return null;
  return {
    applied,
    maxSemiSpaceMb: mb as number | null,
    nodeOptionsAtStart: atStart as string | null,
    nodeOptionsForChildren: forChildren as string | null,
    keptReason: typeof kept === "string" ? kept : null,
  };
}

/** V8 rounds a semi-space size up to a power of two (48 runs as 64). */
export const effectiveSemiSpaceMb = (mb: number) => 2 ** Math.ceil(Math.log2(Math.max(1, mb)));

/** The env the build's own launcher would give the server, and the semi-space size it must then run with. */
export function nodeFlagsFromReport(
  report: NodeFlagsReport | null,
  nodeOptions: string | undefined,
): Pick<NodeFlags, "env" | "expectedSemiSpaceMb"> {
  // 0 selects Node's default, which is not checked (as with no flag at all).
  const expectedSemiSpaceMb =
    report?.maxSemiSpaceMb == null || report.maxSemiSpaceMb === 0
      ? null
      : effectiveSemiSpaceMb(report.maxSemiSpaceMb);
  if (report === null || !report.applied || report.nodeOptionsAtStart === null) {
    return { env: {}, expectedSemiSpaceMb };
  }
  if (report.nodeOptionsAtStart === (nodeOptions ?? "")) return { env: {}, expectedSemiSpaceMb };
  return {
    env: {
      NODE_OPTIONS: report.nodeOptionsAtStart,
      [INHERITED_NODE_OPTIONS_ENV]: report.nodeOptionsForChildren ?? "",
    },
    expectedSemiSpaceMb,
  };
}

/** The part of the server's NODE_OPTIONS the launcher added (for the report), or null. */
export function addedNodeOptions(flags: NodeFlags | undefined): string | null {
  const after = flags?.env.NODE_OPTIONS;
  if (after === undefined) return null;
  const before = flags?.report?.nodeOptionsForChildren ?? "";
  return before !== "" && after.startsWith(before)
    ? after.slice(before.length).trim() || null
    : after;
}

export interface SeaArchive {
  /** "release": an upstream release archive unpacked by release.ts (release.json); "build": a fork build (build-manifest.txt). */
  readonly layout: "release" | "build";
  /** Every `key=value` line of a fork build's build-manifest.txt, as written beside the executable; empty for a release. */
  readonly manifest: Readonly<Record<string, string>>;
  /** The release the archive came from (release.json); null for a fork build. */
  readonly release: SeaRelease | null;
  readonly archive_sha256: string | null;
  /** sha256 of the `t3` executable as found, computed at prepare time. */
  readonly binary_sha256: string;
  /** exec-argv.json beside the executable (the Node options baked into it, and how it takes more); null when absent. */
  readonly exec_argv: SeaExecArgv | null;
  /** The archive's own resource monitor binary; null when the layout has none. */
  readonly resource_monitor: ResourceMonitorBinary | null;
}

/** What release.ts recorded when it verified and unpacked an upstream release archive (release.json beside t3). */
export interface SeaRelease {
  readonly tag: string;
  readonly version: string;
  readonly commit: string;
  readonly platform: string;
  readonly archive_name: string;
  readonly archive_bytes: number | null;
  readonly t3_sha256: string;
  /** Which checks release.ts made on the archive (expected sha256, SHA256SUMS, API digest). */
  readonly checks: Readonly<Record<string, unknown>> | null;
}

export interface ResourceMonitorBinary {
  readonly path: string;
  /** Its path inside the unpacked archive. */
  readonly relative: string;
  readonly sha256: string;
}

/**
 * A single executable's baked Node options (exec-argv.json, written by the build beside `t3`). With execArgvExtension
 * "cli" Node ignores NODE_OPTIONS and takes further options only from a leading `--node-options=…` argument.
 */
export interface SeaExecArgv {
  readonly execArgv: ReadonlyArray<string>;
  readonly execArgvExtension: string | null;
}

const SEA_EXEC_ARGV = "exec-argv.json";

/** exec-argv.json's content, or null when the file is absent; a file that is not such a declaration is an error. */
export function readSeaExecArgv(dir: string): SeaExecArgv | null {
  const path = NodePath.join(dir, SEA_EXEC_ARGV);
  if (!NodeFS.existsSync(path)) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(NodeFS.readFileSync(path, "utf8"));
  } catch (error) {
    throw new Error(`${SEA_PREFIX}${dir}: ${SEA_EXEC_ARGV} is not JSON: ${String(error)}`, {
      cause: error,
    });
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`${SEA_PREFIX}${dir}: ${SEA_EXEC_ARGV} is not a declaration object`);
  }
  const record = parsed as Record<string, unknown>;
  // A field may be absent; present, it must have its type (a null execArgv is a broken declaration, not an empty one).
  const execArgv = "execArgv" in record ? record["execArgv"] : [];
  const extension = "execArgvExtension" in record ? record["execArgvExtension"] : null;
  if (
    !Array.isArray(execArgv) ||
    !execArgv.every((arg) => typeof arg === "string") ||
    !(extension === null || typeof extension === "string")
  ) {
    throw new Error(
      `${SEA_PREFIX}${dir}: ${SEA_EXEC_ARGV} has no string execArgv / execArgvExtension`,
    );
  }
  return { execArgv, execArgvExtension: extension };
}

/**
 * The command that starts a SEA ref's executable with the given NODE_OPTIONS. A single executable built with
 * execArgvExtension "cli" ignores NODE_OPTIONS, so they go first on its command line as `--node-options=…`, as the
 * build's own service launcher passes them; any other build reads them from the environment.
 */
export function seaCommand(
  target: { readonly entry: string; readonly archive: SeaArchive | null },
  nodeOptions: string | undefined,
): Array<string> {
  const cli = target.archive?.exec_argv?.execArgvExtension === "cli";
  return cli && nodeOptions !== undefined && nodeOptions.trim() !== ""
    ? [target.entry, `--node-options=${nodeOptions}`]
    : [target.entry];
}

const SEA_PREFIX = "sea:";
const SEA_EXECUTABLE = "t3";
const SEA_MANIFEST = "build-manifest.txt";
export const RELEASE_FILE = "release.json";
const RESOURCE_MONITOR = "t3-resource-monitor";

const HEX40 = /^[0-9a-f]{40}$/;
const HEX64 = /^[0-9a-f]{64}$/;

/** The platform key a release names its binaries by (linux-x64, darwin-arm64). */
// oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone bench tooling has no Effect runtime.
const hostPlatformKey = () => `${process.platform}-${process.arch}`;

/**
 * The archive's own resource monitor: `resource-monitor/<platform>/t3-resource-monitor` as upstream ships it (also the
 * flat `resource-monitor/t3-resource-monitor` and a fork build's `<platform>/t3-resource-monitor`). Null when absent.
 */
function resourceMonitorIn(
  dir: string,
  platformKey: string,
  layout: "release" | "build",
): ResourceMonitorBinary | null {
  // An upstream release ships it at exactly one place; a fork build's layout has varied.
  const canonical = NodePath.join("resource-monitor", platformKey, RESOURCE_MONITOR);
  const candidates =
    layout === "release"
      ? [canonical]
      : [
          canonical,
          NodePath.join("resource-monitor", RESOURCE_MONITOR),
          NodePath.join(platformKey, RESOURCE_MONITOR),
        ];
  for (const relative of candidates) {
    const path = NodePath.join(dir, relative);
    const stat = NodeFS.statSync(path, { throwIfNoEntry: false });
    if (stat === undefined) continue;
    const shown = relative.split(NodePath.sep).join("/");
    if (!stat.isFile()) throw new Error(`${SEA_PREFIX}${dir}: ${shown} is not a file`);
    if ((stat.mode & 0o111) === 0) {
      throw new Error(`${SEA_PREFIX}${dir}: ${shown} is not executable`);
    }
    return { path, relative: shown, sha256: sha256Sync(path) };
  }
  if (layout === "release") {
    throw new Error(
      `${SEA_PREFIX}${dir}: no ${canonical.split(NodePath.sep).join("/")} in the release (the server would run without its resource monitor)`,
    );
  }
  return null;
}

/** release.json, checked: a release must name its commit, its archive's sha256 and the sha256 of the t3 it held. */
function readRelease(dir: string): SeaRelease & { readonly archive_sha256: string } {
  const path = NodePath.join(dir, RELEASE_FILE);
  let record: Record<string, unknown>;
  try {
    record = recordOf(JSON.parse(NodeFS.readFileSync(path, "utf8")) as unknown);
  } catch (error) {
    throw new Error(`${SEA_PREFIX}${dir}: ${RELEASE_FILE} is not JSON: ${String(error)}`, {
      cause: error,
    });
  }
  const text = (key: string) => (typeof record[key] === "string" ? (record[key] as string) : null);
  const need = (key: string, pattern: RegExp, what: string) => {
    const value = text(key);
    if (value === null || !pattern.test(value)) {
      throw new Error(`${SEA_PREFIX}${dir}: ${RELEASE_FILE} has no ${what} ${key}`);
    }
    return value;
  };
  const commit = need("commit", HEX40, "40-hex");
  const archive_sha256 = need("archive_sha256", HEX64, "64-hex");
  const t3_sha256 = need("t3_sha256", HEX64, "64-hex");
  const tag = need("tag", /^\S+$/, "non-empty");
  const version = need("version", /^\S+$/, "non-empty");
  const platform = need("platform", /^[a-z0-9]+-[a-z0-9_]+$/, "<os>-<arch>");
  const archive_name = need("archive_name", /^\S+$/, "non-empty");
  const bytes = record["archive_bytes"];
  return {
    tag,
    version,
    commit,
    platform,
    archive_name,
    archive_bytes: typeof bytes === "number" && Number.isSafeInteger(bytes) ? bytes : null,
    archive_sha256,
    t3_sha256,
    checks:
      typeof record["checks"] === "object" && record["checks"] !== null
        ? (record["checks"] as Record<string, unknown>)
        : null,
  };
}

/**
 * A SEA ref: an unpacked archive with release.json (an upstream release, from release.ts) or a fork build's
 * build-manifest.txt beside `t3`. t3 is hashed again and refused when it differs from what was unpacked, so a changed
 * binary is never attributed to the release. A single executable honours --require but not --import.
 */
export function seaTarget(
  dir: string,
): Pick<Target, "dir" | "sha" | "dirty" | "tree" | "entry" | "entryKind" | "archive"> {
  const entry = NodePath.join(dir, SEA_EXECUTABLE);
  let stat: NodeFS.Stats;
  try {
    stat = NodeFS.statSync(entry);
  } catch {
    throw new Error(`${SEA_PREFIX}${dir}: no ${SEA_EXECUTABLE} executable in it`);
  }
  if (!stat.isFile() || (stat.mode & 0o111) === 0) {
    throw new Error(`${SEA_PREFIX}${dir}: ${SEA_EXECUTABLE} is not an executable file`);
  }
  const manifestPath = NodePath.join(dir, SEA_MANIFEST);
  if (!NodeFS.existsSync(manifestPath)) {
    if (!NodeFS.existsSync(NodePath.join(dir, RELEASE_FILE))) {
      throw new Error(
        `${SEA_PREFIX}${dir}: no ${SEA_MANIFEST} (a fork build: copy the build's manifest there) or ${RELEASE_FILE} (an upstream release: unpack it with release.ts) beside ${SEA_EXECUTABLE}`,
      );
    }
    return releaseTarget(dir, entry);
  }
  const manifest: Record<string, string> = {};
  for (const line of NodeFS.readFileSync(manifestPath, "utf8").split("\n")) {
    const eq = line.indexOf("=");
    if (eq > 0) manifest[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  }
  const commit = manifest["commit"];
  if (commit === undefined || !HEX40.test(commit)) {
    throw new Error(`${SEA_PREFIX}${dir}: ${SEA_MANIFEST} has no 40-hex commit`);
  }
  const binary_sha256 = sha256Sync(entry);
  // The manifest must name the executable it describes; without that hash any binary could be attributed to the commit.
  const expected = manifest["extracted_t3_sha256"];
  if (expected === undefined || !HEX64.test(expected)) {
    throw new Error(`${SEA_PREFIX}${dir}: ${SEA_MANIFEST} has no 64-hex extracted_t3_sha256`);
  }
  if (expected !== binary_sha256) {
    throw new Error(
      `${SEA_PREFIX}${dir}: ${SEA_EXECUTABLE} sha256 ${binary_sha256} differs from the manifest's extracted_t3_sha256 ${expected}`,
    );
  }
  const dirty = manifest["dirty"] !== undefined && manifest["dirty"] !== "no";
  return {
    dir,
    sha: commit,
    dirty,
    tree: dirty ? `${commit}+dirty` : commit,
    entry,
    entryKind: "sea",
    archive: {
      layout: "build",
      manifest,
      release: null,
      archive_sha256: manifest["archive_sha256"] ?? null,
      binary_sha256,
      exec_argv: readSeaExecArgv(dir),
      resource_monitor: resourceMonitorIn(dir, hostPlatformKey(), "build"),
    },
  };
}

/** An upstream release archive as release.ts unpacked it (t3 + node_modules + resource-monitor, release.json). */
function releaseTarget(
  dir: string,
  entry: string,
): Pick<Target, "dir" | "sha" | "dirty" | "tree" | "entry" | "entryKind" | "archive"> {
  const { archive_sha256, ...release } = readRelease(dir);
  if (
    !NodeFS.statSync(NodePath.join(dir, "node_modules"), { throwIfNoEntry: false })?.isDirectory()
  ) {
    throw new Error(
      `${SEA_PREFIX}${dir}: no node_modules beside ${SEA_EXECUTABLE} (not a release layout: the executable loads its native packages from there)`,
    );
  }
  const binary_sha256 = sha256Sync(entry);
  if (binary_sha256 !== release.t3_sha256) {
    throw new Error(
      `${SEA_PREFIX}${dir}: ${SEA_EXECUTABLE} sha256 ${binary_sha256} differs from ${RELEASE_FILE}'s t3_sha256 ${release.t3_sha256} (the unpacked archive was changed after it was verified)`,
    );
  }
  return {
    dir,
    sha: release.commit,
    dirty: false,
    tree: release.commit,
    entry,
    entryKind: "sea",
    archive: {
      layout: "release",
      manifest: {},
      release,
      archive_sha256,
      binary_sha256,
      exec_argv: readSeaExecArgv(dir),
      resource_monitor: resourceMonitorIn(dir, release.platform, "release"),
    },
  };
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
async function prepareTarget(
  spec: string,
  workDir: string,
  buildCmd: string,
  archivePath: string | undefined,
): Promise<Omit<Target, "env">> {
  const eq = spec.indexOf("=");
  const explicitLabel =
    eq > 0 && /^[A-Za-z0-9._-]+$/.test(spec.slice(0, eq)) ? spec.slice(0, eq) : undefined;
  const value = explicitLabel === undefined ? spec : spec.slice(eq + 1);
  if (value.startsWith(SEA_PREFIX)) {
    const seaDir = NodeFS.realpathSync(value.slice(SEA_PREFIX.length));
    const sea = seaTarget(seaDir);
    // Before the executable first runs (--version, __node-flags): a release proves itself from its archive.
    await checkSeaRef(sea, archivePath);
    const version = NodeChildProcess.spawnSync(sea.entry, ["--version"], {
      cwd: seaDir,
      encoding: "utf8",
      timeout: 120_000,
    });
    return {
      label: explicitLabel ?? sanitiseLabel(`sea-${sea.sha.slice(0, 9)}`),
      ref: value,
      ...sea,
      archiveVerified: archivePath !== undefined,
      version: version.status === 0 ? (version.stdout.split("\n")[0]?.trim() ?? null) : null,
    };
  }
  if (archivePath !== undefined) throw new Error(`--archive applies to a sea: ref, not ${value}`);
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
    archive: null,
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
  /** The driver's mode, how its sessions split between fixture and new threads, and the match. */
  readonly workload: DriverWorkload | null;
  /** The driver's stable failure code (e.g. sessions_starved); null when it ran clean or gave none. */
  readonly failure_code: string | null;
  /** The phase the driver failed in (setup, warmup, window, ...); null when it ran clean or gave none. */
  readonly failure_phase: string | null;
  /** A judged window's findings (every reason it fell short), in order; null when it gave none. */
  readonly findings: ReadonlyArray<{ readonly code: string; readonly reason: string }> | null;
  /** Sessions that produced provider output in the window, and all sessions; null when not measured. */
  readonly sessions_served: number | null;
  readonly sessions_total: number | null;
}

export interface DriverWorkload {
  readonly mode: string | null;
  readonly sessions: number | null;
  readonly fixture_thread_sessions: number | null;
  readonly new_thread_sessions: number | null;
  readonly session_candidates: number | null;
  /** p50/p95 turn items of the fixture threads exercised against the fixture's (history/mixed). */
  readonly distribution_match: Readonly<Record<string, unknown>> | null;
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
    workload: driverWorkload(record),
    failure_code: typeof failure.code === "string" ? failure.code : null,
    failure_phase: typeof failure.phase === "string" ? failure.phase : null,
    findings: windowFindingsOf(failure),
    sessions_served:
      finiteOrNull(failure.sessionsServed) ??
      finiteOrNull(recordOf(record.sessions).withOutputInWindow),
    sessions_total:
      finiteOrNull(failure.sessionsTotal) ?? finiteOrNull(recordOf(record.sessions).count),
  };
}

/** A judged window's findings from driver.json (findings[], or the one code and reason an older driver wrote). */
function windowFindingsOf(
  failure: Record<string, unknown>,
): Array<{ readonly code: string; readonly reason: string }> | null {
  if (failure.phase !== "window") return null;
  const listed = Array.isArray(failure.findings)
    ? failure.findings
        .map(recordOf)
        .filter((finding) => typeof finding.code === "string")
        .map((finding) => ({
          code: finding.code as string,
          reason: typeof finding.reason === "string" ? finding.reason : "",
        }))
    : [];
  if (listed.length > 0) return listed;
  return typeof failure.code === "string"
    ? [{ code: failure.code, reason: typeof failure.reason === "string" ? failure.reason : "" }]
    : null;
}

/**
 * The driver's problems for the attempt: none for a clean run; each finding's code for a complete window the driver
 * judged unpublishable (classifyProblems decides which of them a saturated server explains); driver_failed for
 * anything else (setup, an error, a timeout, a missing or failed result).
 */
export function driverProblems(driver: DriverSummary | null, failedMarker: boolean): Array<string> {
  if (driver !== null && driver.ok && !failedMarker) return [];
  const findings = driver !== null && !driver.ok ? (driver.findings ?? []) : [];
  // Only window findings run.ts knows how to classify; anything else from the driver is a failed driver.
  if (findings.length > 0 && findings.every((finding) => WINDOW_FINDING_CODES.has(finding.code))) {
    return findings.map((finding) => finding.code);
  }
  return ["driver_failed"];
}

/**
 * How many sessions the server served in the window, of all: the record's agents served, and `saturated` when not
 * every session produced output (a saturated server; the attempt stays valid). Nulls when the window was not measured.
 */
export function saturation(driver: DriverSummary | null): {
  readonly sessions_served: number | null;
  readonly sessions_total: number | null;
} {
  return {
    sessions_served: driver?.sessions_served ?? null,
    sessions_total: driver?.sessions_total ?? null,
  };
}

const sha256FileSync = (path: string) => {
  const hash = NodeCrypto.createHash("sha256");
  const fd = NodeFS.openSync(path, "r");
  try {
    const buffer = Buffer.alloc(16 * MIB);
    for (;;) {
      const read = NodeFS.readSync(fd, buffer, 0, buffer.length, null);
      if (read === 0) break;
      hash.update(buffer.subarray(0, read));
    }
  } finally {
    NodeFS.closeSync(fd);
  }
  return hash.digest("hex");
};

/** Files a layout writes beside the archive's members: release.ts writes release.json; a fork build's manifest and
 * exec-argv.json are copied beside t3. Nothing else may be on disk that the archive does not have. */
const RELEASE_EXTRAS: ReadonlySet<string> = new Set([RELEASE_FILE]);
const BUILD_EXTRAS: ReadonlySet<string> = new Set([SEA_MANIFEST, SEA_EXEC_ARGV]);
/** How many differences a refusal names before "and N more". */
const ARCHIVE_DIFFERENCES_SHOWN = 10;

/**
 * Proves an unpacked sea: directory came from `archivePath` (verified against the release's SHA256SUMS and API digest):
 * its sha256 is the one release.json (or a build manifest) names, and the whole tree, `t3` included, is the archive's.
 * release.json alone is a claim: a forged one with a wrapper `t3`, or a changed native module, would run unseen.
 */
export async function verifyArchive(
  target: { readonly entry: string; readonly archive: SeaArchive | null },
  archivePath: string,
): Promise<void> {
  const dir = NodePath.dirname(target.entry);
  const archive = target.archive;
  if (archive === null) throw new Error(`--archive ${archivePath} applies to a sea: ref only`);
  const record = archive.layout === "release" ? RELEASE_FILE : SEA_MANIFEST;
  if (!isFile(archivePath)) throw new Error(`--archive ${archivePath} is not a file`);
  if (archive.archive_sha256 === null) {
    throw new Error(
      `${SEA_PREFIX}${dir}: ${record} names no archive_sha256 to check --archive against`,
    );
  }
  const archiveSha = sha256FileSync(archivePath);
  if (archiveSha !== archive.archive_sha256) {
    throw new Error(
      `${SEA_PREFIX}${dir}: archive sha256 ${archiveSha} differs from ${record}'s archive_sha256 ${archive.archive_sha256} (--archive ${archivePath})`,
    );
  }
  // One pass over the archive: every member against the tree on disk, and t3's own sha256 from the archive.
  const comparison = await compareTree({
    archive: archivePath,
    dir,
    extras: archive.layout === "release" ? RELEASE_EXTRAS : BUILD_EXTRAS,
    hash: new Set([SEA_EXECUTABLE]),
  }).catch((error: unknown) => {
    throw new Error(
      `--archive ${archivePath} cannot be read as one tree: ${error instanceof Error ? error.message : String(error)}`,
      { cause: error },
    );
  });
  const top = comparison.top;
  const release = archive.release;
  if (release !== null && top !== `t3-${release.version}-${release.platform}`) {
    throw new Error(
      `--archive ${archivePath}: its top directory ${top} is not release.json's t3-${release.version}-${release.platform}`,
    );
  }
  const archived = comparison.hashes.get(SEA_EXECUTABLE);
  if (archived === undefined) {
    throw new Error(`--archive ${archivePath}: no ${top}/${SEA_EXECUTABLE} file in it`);
  }
  if (archived !== archive.binary_sha256) {
    throw new Error(
      `${SEA_PREFIX}${dir}: ${SEA_EXECUTABLE} sha256 ${archive.binary_sha256} differs from the archive's ${SEA_EXECUTABLE} sha256 ${archived} (--archive ${archivePath})`,
    );
  }
  const { differences } = comparison;
  if (differences.length > 0) {
    const shown = differences.slice(0, ARCHIVE_DIFFERENCES_SHOWN).join("; ");
    const more =
      differences.length > ARCHIVE_DIFFERENCES_SHOWN
        ? `; and ${differences.length - ARCHIVE_DIFFERENCES_SHOWN} more`
        : "";
    throw new Error(
      `${SEA_PREFIX}${dir}: the unpacked tree differs from the archive in ${differences.length} ${differences.length === 1 ? "place" : "places"} (--archive ${archivePath}): ${shown}${more}`,
    );
  }
}

/** Node's single-executable fuse; a build with its blob injected carries it set (":1"). */
const SEA_FUSE = Buffer.from("NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2:1");
const BINARY_FORMATS: Readonly<
  Record<string, { readonly name: string; readonly magics: ReadonlyArray<string> }>
> = {
  linux: { name: "an ELF", magics: ["7f454c46"] },
  darwin: {
    name: "a Mach-O",
    magics: ["feedface", "feedfacf", "cefaedfe", "cffaedfe", "cafebabe"],
  },
  win32: { name: "a PE", magics: ["4d5a"] },
};

/**
 * Refuses a `t3` that is not a Node single executable for `platformKey` (<os>-<arch>): the binary format of the OS,
 * and Node's single-executable fuse set. A script or a wrapper around the real binary fails here before it ever runs.
 */
export function checkSeaExecutable(path: string, platformKey: string): void {
  const os = platformKey.split("-")[0] ?? "";
  const format = BINARY_FORMATS[os];
  if (format === undefined) throw new Error(`${path}: unsupported platform ${platformKey}`);
  const bytes = NodeFS.readFileSync(path);
  const head = bytes.subarray(0, 4).toString("hex");
  if (!format.magics.some((magic) => head.startsWith(magic))) {
    throw new Error(
      `${path} is not ${format.name} executable (a ${platformKey} Node single executable is one)`,
    );
  }
  if (bytes.indexOf(SEA_FUSE) === -1) {
    throw new Error(
      `${path} is not a Node single executable (its single-executable fuse is not set)`,
    );
  }
}

/** The explicit label of a --ref value (label=...), or null. */
const refLabel = (spec: string) => {
  const eq = spec.indexOf("=");
  return eq > 0 && /^[A-Za-z0-9._-]+$/.test(spec.slice(0, eq)) ? spec.slice(0, eq) : null;
};

/**
 * --archive [<label>=]<path>, repeatable: the verified archive each sea: ref was unpacked from, by the index of its
 * --ref. Without a label it applies to the only --ref.
 */
export function parseArchiveSpecs(
  specs: ReadonlyArray<string>,
  refs: ReadonlyArray<string>,
): Map<number, string> {
  const archives = new Map<number, string>();
  const labels = refs.map(refLabel);
  for (const spec of specs) {
    const label = refLabel(spec);
    const path = NodePath.resolve(label === null ? spec : spec.slice(label.length + 1));
    let index: number;
    if (label === null) {
      if (refs.length !== 1) {
        throw new Error(
          `--archive needs <label>=<path> when there are several --ref (got ${spec})`,
        );
      }
      index = 0;
    } else {
      index = labels.indexOf(label);
      if (index === -1) throw new Error(`--archive ${spec}: no --ref labelled ${label}`);
    }
    if (archives.has(index)) throw new Error(`two --archive values for ${label ?? refs[index]}`);
    archives.set(index, path);
  }
  return archives;
}

/**
 * The checks a sea: ref passes before its executable first runs: an upstream release needs its verified archive;
 * `t3` must be a Node single executable for the platform; and with an archive, `t3` must be the archive's own.
 */
export async function checkSeaRef(
  target: { readonly entry: string; readonly archive: SeaArchive | null },
  archivePath: string | undefined,
): Promise<void> {
  const archive = target.archive;
  if (archive?.layout === "release" && archivePath === undefined) {
    throw new Error(
      `${SEA_PREFIX}${NodePath.dirname(target.entry)}: an upstream release needs --archive <the verified archive> (release.json alone is a claim anyone can write)`,
    );
  }
  checkSeaExecutable(target.entry, archive?.release?.platform ?? hostPlatformKey());
  if (archivePath !== undefined) await verifyArchive(target, archivePath);
}

/** A resource monitor set through the environment must be an executable file (the archive's own is checked on load). */
export function checkResourceMonitor(choice: ResourceMonitorChoice | null): void {
  if (choice === null || choice.source !== "env") return;
  const stat = NodeFS.statSync(choice.path, { throwIfNoEntry: false });
  if (stat === undefined || !stat.isFile() || (stat.mode & 0o111) === 0) {
    throw new Error(`T3CODE_RESOURCE_MONITOR_PATH ${choice.path} is not an executable file`);
  }
}

/** --preload: none, a file, or the harness's own preload/bench-preload.cjs; a missing one is named. */
export function resolvePreload(value: string | undefined, dir: string): string | null {
  if (value === "none") return null;
  const path = value ?? NodePath.join(dir, "preload", "bench-preload.cjs");
  if (!NodeFS.existsSync(path)) throw new Error(`--preload ${path} does not exist`);
  const real = NodeFS.realpathSync(path);
  if (!isFile(real)) throw new Error(`--preload ${path} is not a file`);
  return real;
}

/**
 * Why an attempt is driver_failed, for run.json: the driver's own phase, code and reason when its driver.json gives
 * them, else what run.ts knows (no or an unreadable driver.json, the driver's exit code or signal, a time budget that
 * ran out). A failure is then diagnosable from run.json alone.
 */
export function driverFailure(
  driver: DriverSummary | null,
  exit: { readonly code: number | null; readonly signal: string | null } | null,
  resultFile: "present" | "missing" | "unreadable",
  timedOut: boolean,
): {
  readonly phase: string | null;
  readonly code: string | null;
  readonly reason: string;
  readonly exit_code: number | null;
  readonly signal: string | null;
  readonly result_file: "present" | "missing" | "unreadable";
} {
  const how = timedOut
    ? "the driver did not finish within its time budget and was stopped"
    : exit?.signal != null
      ? `the driver was ended by ${exit.signal}`
      : `the driver exited with code ${String(exit?.code ?? "unknown")}`;
  const reason =
    driver === null
      ? `${resultFile === "missing" ? "no driver.json" : "driver.json could not be read"} (${how})`
      : driver.ok
        ? "driver.json says ok but a driver.failed marker is beside it"
        : (driver.error ?? `driver.json says failed without a reason (${how})`);
  return {
    phase: driver?.failure_phase ?? null,
    code: driver?.failure_code ?? null,
    reason,
    exit_code: exit?.code ?? null,
    signal: exit?.signal ?? null,
    result_file: resultFile,
  };
}

/** The server event-loop utilisation over the window at or above which a run counts as saturated. */
export const SATURATION_ELU = 0.95;

/**
 * The server's event-loop utilisation over the measured window, and where it came from: the preload's window ELU
 * (exact over the window, every era once the preload loads), else the server's own gauge (30 s readings averaged over
 * the window, builds from 2026-10-20 on). Null when neither exists: saturation cannot then be shown.
 */
export function serverElu(metrics: unknown): {
  readonly elu: number | null;
  readonly source: "preload" | "server_gauge" | null;
} {
  const tree = recordOf(metrics);
  const preload = recordOf(tree.loop).elu;
  if (typeof preload === "number" && Number.isFinite(preload))
    return { elu: preload, source: "preload" };
  const gauge = recordOf(tree.otlp).elu_mean;
  if (typeof gauge === "number" && Number.isFinite(gauge))
    return { elu: gauge, source: "server_gauge" };
  return { elu: null, source: null };
}

/** What a saturated server explains: sessions it did not serve, MCP clients not signed in, calls that timed out. */
const SATURATION_CODES = new Set(["sessions_starved", "session_mcp_missing", "call_timeouts"]);
/** A judged window's other findings: always defects (the driver itself, a failed answer, threads unlike the fixture). */
const WINDOW_DEFECT_CODES = new Set(["driver_saturated", "error_rate", "distribution_mismatch"]);
const WINDOW_FINDING_CODES = new Set([...SATURATION_CODES, ...WINDOW_DEFECT_CODES]);

/**
 * Each problem's kind and the attempt's validity: at a server window ELU of SATURATION_ELU or more, SATURATION_CODES are
 * "saturation" (valid, saturated); every other problem is a "defect", and a fatal defect voids the attempt whatever the
 * load. The host guard is the runner's.
 */
export function classifyProblems(input: {
  readonly problems: ReadonlyArray<string>;
  readonly elu: number | null;
  readonly traceRequired: boolean;
}): {
  readonly problems: Array<{ readonly code: string; readonly kind: "saturation" | "defect" }>;
  readonly valid: boolean;
  readonly saturated: boolean;
} {
  const saturatedServer = input.elu !== null && input.elu >= SATURATION_ELU;
  let valid = true;
  let anySaturation = false;
  const problems = input.problems.map((code) => {
    const kind: "saturation" | "defect" =
      saturatedServer && SATURATION_CODES.has(code) ? "saturation" : "defect";
    if (kind === "saturation") anySaturation = true;
    const fatal =
      kind === "defect" &&
      (FATAL.has(code) ||
        SATURATION_CODES.has(code) ||
        WINDOW_DEFECT_CODES.has(code) ||
        (input.traceRequired && TRACE_PROBLEMS.has(code)));
    if (fatal) valid = false;
    return { code, kind };
  });
  return { problems, valid, saturated: valid && anySaturation };
}

function driverWorkload(record: Record<string, unknown>): DriverWorkload | null {
  const knobs = recordOf(record.knobs);
  const details = recordOf(record.workload);
  const sessions = finiteOrNull(recordOf(record.sessions).count);
  const mode = typeof knobs.mode === "string" ? knobs.mode : null;
  if (mode === null && sessions === null) return null;
  const seeded = Array.isArray(details.sessionThreadIds) ? details.sessionThreadIds.length : null;
  const match = recordOf(recordOf(record.distribution).match);
  return {
    mode,
    sessions,
    fixture_thread_sessions: mode === "fresh" ? 0 : seeded,
    new_thread_sessions:
      mode === "fresh"
        ? sessions
        : (finiteOrNull(details.freshSessions) ?? (seeded === null ? null : 0)),
    session_candidates: finiteOrNull(details.sessionCandidates),
    distribution_match: Object.keys(match).length === 0 ? null : match,
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

/**
 * Stops a process group: SIGTERM, up to `graceMs` for the leader to exit, then SIGKILL for the whole group (provider
 * children live in it and must not outlive the run). Reports whether the grace ran out (forced: the leader was
 * SIGKILLed, so whatever it would have written on shutdown may be missing), the time from SIGTERM to the leader's exit,
 * and whether it had exited before any signal (then nothing was sent).
 */
export async function stopGroup(
  child: NodeChildProcess.ChildProcess,
  graceMs: number,
  clock: () => number = () => performance.now(),
): Promise<{
  readonly forced: boolean;
  readonly signal_to_exit_ms: number | null;
  readonly exited_before_stop: boolean;
}> {
  if (child.exitCode !== null || child.signalCode !== null) {
    killGroup(child, "SIGKILL");
    return { forced: false, signal_to_exit_ms: null, exited_before_stop: true };
  }
  const signalled = clock();
  killGroup(child, "SIGTERM");
  const stopped = await Promise.race([
    exited(child).then(() => true),
    sleep(graceMs).then(() => false),
  ]);
  killGroup(child, "SIGKILL");
  await exited(child);
  return {
    forced: !stopped,
    signal_to_exit_ms: Math.round(clock() - signalled),
    exited_before_stop: false,
  };
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

/** The resource monitor a ref's server is pointed at, and where the path came from. */
export interface ResourceMonitorChoice {
  readonly path: string;
  readonly relative: string | null;
  readonly sha256: string | null;
  /** "archive": the archive's own binary (the default); "env": --server-env / --ref-env set it. */
  readonly source: "archive" | "env";
}

/** A bench preload file name (bench-preload.cjs / .mjs, wherever it lives). */
const BENCH_PRELOAD = /(^|[\\/])bench-preload\.[cm]?js$/;

/** The --require targets of a NODE_OPTIONS string (--require=X, --require X, -r X). */
function requiredModules(nodeOptions: string | undefined): Array<string> {
  const tokens = nodeOptionTokens(nodeOptions);
  const required: Array<string> = [];
  for (const [index, token] of tokens.entries()) {
    if (token.startsWith("--require=")) required.push(token.slice("--require=".length));
    else if ((token === "--require" || token === "-r") && tokens[index + 1] !== undefined) {
      required.push(tokens[index + 1]!);
    }
  }
  return required;
}

/**
 * A ref's server environment: the bench defaults, then the archive's own resource monitor
 * (T3CODE_RESOURCE_MONITOR_PATH, a default the run may override), then --server-env and --ref-env; and the preload
 * appended to NODE_OPTIONS as --require=<file> (quoted when the path has spaces). A NODE_OPTIONS that already requires a
 * bench preload is refused: the preload would run twice and write every sample twice.
 */
export function composeServerEnv(input: {
  readonly defaults: Readonly<Record<string, string>>;
  readonly serverEnv?: Readonly<Record<string, string>> | undefined;
  readonly refEnv?: Readonly<Record<string, string>> | undefined;
  readonly archive: Pick<SeaArchive, "resource_monitor"> | null;
  readonly preload: string | null;
}): {
  readonly env: Record<string, string>;
  readonly resourceMonitor: ResourceMonitorChoice | null;
} {
  const env: Record<string, string> = { ...input.defaults };
  const own = input.archive?.resource_monitor ?? null;
  if (own !== null) env.T3CODE_RESOURCE_MONITOR_PATH = own.path;
  Object.assign(env, input.serverEnv, input.refEnv);
  // An override is resolved here: the server runs in another directory than the one it was given in.
  if (env.T3CODE_RESOURCE_MONITOR_PATH !== undefined && env.T3CODE_RESOURCE_MONITOR_PATH !== "") {
    env.T3CODE_RESOURCE_MONITOR_PATH = NodePath.resolve(env.T3CODE_RESOURCE_MONITOR_PATH);
  }
  const monitorPath = env.T3CODE_RESOURCE_MONITOR_PATH;
  const resourceMonitor: ResourceMonitorChoice | null =
    monitorPath === undefined
      ? null
      : own !== null && monitorPath === own.path
        ? { ...own, source: "archive" }
        : { path: monitorPath, relative: null, sha256: null, source: "env" };
  if (input.preload !== null) {
    const already = requiredModules(env.NODE_OPTIONS).find((module) => BENCH_PRELOAD.test(module));
    if (already !== undefined) {
      throw new Error(
        `NODE_OPTIONS already requires a bench preload (${already}); run.ts adds ${input.preload} itself (--preload), so leave it out of NODE_OPTIONS or pass --preload none`,
      );
    }
    if (/["\\]/.test(input.preload)) {
      throw new Error(
        `--preload ${input.preload}: a path with " or \\ cannot be quoted in NODE_OPTIONS`,
      );
    }
    const require = /\s/.test(input.preload)
      ? `--require="${input.preload}"`
      : `--require=${input.preload}`;
    const before = env.NODE_OPTIONS?.trim() ?? "";
    env.NODE_OPTIONS = before === "" ? require : `${before} ${require}`;
  }
  return { env, resourceMonitor };
}

/** What the preload left in a run directory: whether it loaded, its 1 Hz samples and its window histogram. */
export function preloadOutcome(
  runDir: string,
  requested: boolean,
): {
  readonly requested: boolean;
  readonly loaded: boolean;
  readonly entry_kind: string | null;
  readonly node_options: string | null;
  readonly samples: number;
  readonly window: boolean;
} {
  const heap = readJson(NodePath.join(runDir, "heap-config.json"));
  const heapRecord =
    typeof heap === "object" && heap !== null ? (heap as Record<string, unknown>) : null;
  const lines = readText(NodePath.join(runDir, "loop-1hz.ndjson"))
    .split("\n")
    .filter((line) => line.trim().length > 0).length;
  return {
    requested,
    loaded: heapRecord !== null,
    entry_kind: typeof heapRecord?.entry_kind === "string" ? heapRecord.entry_kind : null,
    node_options: typeof heapRecord?.node_options === "string" ? heapRecord.node_options : null,
    samples: lines,
    window: loopMetrics(runDir) !== null,
  };
}

/**
 * The event loop over the measured window, from the preload's loop-window.json: delay percentiles (10 ms histogram),
 * ELU, GC time per second and the longest GC pause. Null when the preload wrote no window.
 */
export function loopMetrics(runDir: string): MetricTree | null {
  const raw = readJson(NodePath.join(runDir, "loop-window.json"));
  if (typeof raw !== "object" || raw === null) return null;
  const window = raw as Record<string, unknown>;
  const delay = recordOf(window.delay);
  const num = (value: unknown) =>
    typeof value === "number" && Number.isFinite(value) ? value : null;
  const start = num(window.start);
  const end = num(window.end);
  const gc = Object.values(recordOf(window.gc)).map(recordOf);
  const seconds = start !== null && end !== null && end > start ? (end - start) / 1000 : null;
  const gcMs = gc.reduce((sum, kind) => sum + (num(kind.ms) ?? 0), 0);
  const round = (value: number) => Math.round(value * 1000) / 1000;
  return {
    elu: num(window.elu),
    eld_p50_ms: num(delay.p50_ms),
    eld_p90_ms: num(delay.p90_ms),
    eld_p99_ms: num(delay.p99_ms),
    eld_p999_ms: num(delay.p999_ms),
    eld_max_ms: num(delay.max_ms),
    eld_mean_ms: num(delay.mean_ms),
    gc_ms_per_s: seconds === null ? null : round(gcMs / seconds),
    gc_max_pause_ms: gc.length === 0 ? null : Math.max(...gc.map((kind) => num(kind.max_ms) ?? 0)),
  };
}

/**
 * After the driver exits: once ctl/window-end exists, waits (polling every 100 ms, up to `timeoutMs`) for the preload's
 * next 1 Hz tick to write loop-window.json, so a drain shorter than a second still keeps the window histogram. Returns
 * whether the file exists; does not wait when the window never closed.
 */
export async function awaitLoopWindow(
  runDir: string,
  timeoutMs: number,
  pause: (ms: number) => Promise<void> = sleep,
): Promise<boolean> {
  const written = () => NodeFS.existsSync(NodePath.join(runDir, "loop-window.json"));
  if (written()) return true;
  if (!NodeFS.existsSync(NodePath.join(runDir, "ctl", "window-end"))) return false;
  const step = 100;
  for (let waited = 0; waited < timeoutMs; waited += step) {
    await pause(step);
    if (written()) return true;
  }
  return false;
}

/** The driver logs `[driver <ISO>] phase: <name>`; the window opens at "window" and closes at "drain". */
export function windowControlFor(line: string): "window-start" | "window-end" | null {
  const match = /^\[driver [^\]]+\] phase: (\S+)\s*$/.exec(line);
  if (match === null) return null;
  return match[1] === "window" ? "window-start" : match[1] === "drain" ? "window-end" : null;
}

/**
 * Follows a run's driver.log and touches <runDir>/ctl/window-start and window-end on the driver's phase lines, so the
 * preload's window histogram covers the measured window. Returns a stop function.
 */
function followWindowPhases(driverLog: string, runDir: string): () => void {
  const ctl = NodePath.join(runDir, "ctl");
  NodeFS.mkdirSync(ctl, { recursive: true });
  let offset = 0;
  let partial = "";
  const poll = () => {
    let fd: number | undefined;
    try {
      fd = NodeFS.openSync(driverLog, "r");
      const size = NodeFS.fstatSync(fd).size;
      if (size <= offset) return;
      const buffer = Buffer.alloc(size - offset);
      const read = NodeFS.readSync(fd, buffer, 0, buffer.length, offset);
      offset += read;
      const text = partial + buffer.subarray(0, read).toString("utf8");
      const lines = text.split("\n");
      partial = lines.pop() ?? "";
      for (const line of lines) {
        const control = windowControlFor(line);
        if (control !== null) NodeFS.writeFileSync(NodePath.join(ctl, control), String(Date.now()));
      }
    } catch {
      // Not written yet.
    } finally {
      if (fd !== undefined) NodeFS.closeSync(fd);
    }
  };
  const timer = setInterval(poll, 200);
  return () => {
    clearInterval(timer);
    poll();
  };
}

/** What a build without the command prints (stderr, exit 1): the CLI's unknown-command error naming it. */
export const isUnknownNodeFlagsCommand = (exitCode: number | null, stderr: string) =>
  exitCode === 1 && stderr.includes(`Unknown command "${NODE_FLAGS_COMMAND}"`);

/**
 * The probe's outcome. Only a build that rejects the command as unknown runs without flags; any other failure
 * (timeout, another exit status, output that is not a report) stops the bench, because a flag-aware build whose
 * probe failed would otherwise be launched without its default and still pass the per-run check.
 */
export function nodeFlagsFromProbe(
  outcome: {
    readonly timedOut: boolean;
    readonly exitCode: number | null;
    readonly signal: string | null;
    readonly stdout: string;
    readonly stderr: string;
  },
  nodeOptions: string | undefined,
): NodeFlags {
  const what = `${NODE_FLAGS_COMMAND} probe`;
  if (outcome.timedOut) throw new Error(`${what}: no answer within 30 s`);
  if (outcome.exitCode !== 0) {
    if (isUnknownNodeFlagsCommand(outcome.exitCode, outcome.stderr)) {
      // A build without the command runs with the bench's NODE_OPTIONS; an explicit semi-space flag there is still checked.
      const explicitSemi = lastSizeFlag("--max-semi-space-size", nodeOptions, []);
      return {
        supported: false,
        report: null,
        unsupportedReason: `unknown command`,
        env: {},
        expectedSemiSpaceMb:
          explicitSemi === null || explicitSemi === 0 ? null : effectiveSemiSpaceMb(explicitSemi),
      };
    }
    throw new Error(
      `${what}: exit ${String(outcome.exitCode ?? outcome.signal)}: ${outcome.stderr.trim().slice(-400)}`,
    );
  }
  const report = parseNodeFlagsReport(outcome.stdout);
  if (report === null) {
    throw new Error(
      `${what}: exit 0 but no report on stdout: ${outcome.stdout.trim().slice(-400)}`,
    );
  }
  return {
    supported: true,
    report,
    unsupportedReason: null,
    ...nodeFlagsFromReport(report, nodeOptions),
  };
}

/** Asks the build which startup flags it wants (`<entry> __node-flags`), with the env every run of the ref gets. */
async function probeNodeFlags(target: Target): Promise<NodeFlags> {
  const home = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-node-flags-"));
  const env: NodeJS.ProcessEnv = { ...cleanEnv(), ...target.env, T3CODE_HOME: home };
  // Asked as the server is started: a SEA that takes Node options on its command line gets the bench's there too.
  const launcher =
    target.entryKind === "sea"
      ? seaCommand(target, env.NODE_OPTIONS)
      : [process.execPath, target.entry];
  try {
    const child = NodeChildProcess.spawn(launcher[0]!, [...launcher.slice(1), NODE_FLAGS_COMMAND], {
      cwd: home,
      env,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    live.add(child);
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (data: Buffer) => {
      if (stdout.length < 64 * 1024) stdout += data.toString();
    });
    child.stderr?.on("data", (data: Buffer) => {
      if (stderr.length < 64 * 1024) stderr += data.toString();
    });
    const closed = new Promise<void>((resolve) => child.once("close", () => resolve()));
    const timedOut = await Promise.race([closed.then(() => false), sleep(30_000).then(() => true)]);
    await stopGroup(child, 1000);
    live.delete(child);
    return nodeFlagsFromProbe(
      { timedOut, exitCode: child.exitCode, signal: child.signalCode, stdout, stderr },
      target.env.NODE_OPTIONS,
    );
  } catch (error) {
    throw new Error(`${target.label}: ${error instanceof Error ? error.message : String(error)}`, {
      cause: error,
    });
  } finally {
    NodeFS.rmSync(home, { recursive: true, force: true });
  }
}

/** Splits NODE_OPTIONS as Node does: spaces separate, double quotes group, a backslash inside quotes escapes. */
export function nodeOptionTokens(nodeOptions: string | null | undefined): Array<string> {
  const tokens: Array<string> = [];
  if (!nodeOptions) return tokens;
  let token = "";
  let quoted = false;
  let hasToken = false;
  for (let index = 0; index < nodeOptions.length; index++) {
    let char = nodeOptions[index]!;
    if (char === "\\" && quoted && index + 1 < nodeOptions.length) {
      char = nodeOptions[++index]!;
    } else if (char === " " && !quoted) {
      if (hasToken) tokens.push(token);
      token = "";
      hasToken = false;
      continue;
    } else if (char === '"') {
      quoted = !quoted;
      hasToken = true;
      continue;
    }
    token += char;
    hasToken = true;
  }
  if (hasToken) tokens.push(token);
  return tokens;
}

/** A V8 size flag's last value in NODE_OPTIONS then execArgv (Node applies them in that order), dashes or underscores. */
export function lastSizeFlag(
  name: string,
  nodeOptions: string | null | undefined,
  execArgv: ReadonlyArray<string>,
): number | null {
  const args = [...nodeOptionTokens(nodeOptions), ...execArgv];
  let value: number | null = null;
  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    const eq = arg.indexOf("=");
    if ((eq === -1 ? arg : arg.slice(0, eq)).replaceAll("_", "-") !== name) continue;
    const parsed = Number(eq === -1 ? args[index + 1] : arg.slice(eq + 1));
    value = Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
  }
  return value;
}

/**
 * The semi-space size a server process runs with, from what it recorded about itself (heap-config.json, written by
 * the bench preload): V8's heap limit is the old-generation limit plus the young generation, which is three
 * semi-spaces (two plus the new large-object space), so (heap_size_limit - --max-old-space-size) / 3. Null when the
 * process set no --max-old-space-size (its old-generation limit is then derived from the host's memory).
 */
export function measuredSemiSpaceMb(heapConfig: unknown): {
  readonly heap_size_limit_mib: number | null;
  readonly max_old_space_mb: number | null;
  readonly semi_space_mb: number | null;
  readonly node_options: string | null;
} {
  const record =
    typeof heapConfig === "object" && heapConfig !== null
      ? (heapConfig as Record<string, unknown>)
      : {};
  const limitBytes = record["heap_size_limit_bytes"];
  const limitMib =
    typeof limitBytes === "number"
      ? limitBytes / MIB
      : typeof record["heap_size_limit_mib"] === "number"
        ? record["heap_size_limit_mib"]
        : null;
  const nodeOptions = typeof record["node_options"] === "string" ? record["node_options"] : null;
  const execArgv = Array.isArray(record["exec_argv"])
    ? record["exec_argv"].filter((arg): arg is string => typeof arg === "string")
    : [];
  const maxOld = lastSizeFlag("--max-old-space-size", nodeOptions, execArgv);
  const semi =
    limitMib === null || maxOld === null || maxOld === 0
      ? null
      : Math.round(((limitMib - maxOld) / 3) * 100) / 100;
  return {
    heap_size_limit_mib: limitMib === null ? null : Math.round(limitMib * 100) / 100,
    max_old_space_mb: maxOld,
    semi_space_mb: semi,
    node_options: nodeOptions,
  };
}

export interface NodeFlagsCheck {
  readonly expected_semi_space_mb: number | null;
  readonly expected_node_options: string | null;
  readonly measured: ReturnType<typeof measuredSemiSpaceMb> | null;
  readonly problem: "node_flags_mismatch" | "node_flags_unverified" | null;
}

/**
 * A run whose build asks for startup flags must show them in the server process: the NODE_OPTIONS it saw are the
 * ones the harness set, and its measured semi-space size is the one the build reported. Without heap-config.json
 * (no bench preload) or without --max-old-space-size the size cannot be measured, and such a run is not valid.
 */
export function checkNodeFlags(flags: NodeFlags | undefined, heapConfig: unknown): NodeFlagsCheck {
  const expectedSemi = flags?.expectedSemiSpaceMb ?? null;
  const expectedOptions = flags?.env.NODE_OPTIONS ?? null;
  const measured = heapConfig === undefined ? null : measuredSemiSpaceMb(heapConfig);
  const wants = expectedSemi !== null || expectedOptions !== null;
  let problem: NodeFlagsCheck["problem"] = null;
  if (wants) {
    if (measured === null) problem = "node_flags_unverified";
    else if (expectedOptions !== null && measured.node_options !== expectedOptions) {
      problem = "node_flags_mismatch";
    } else if (expectedSemi !== null && measured.semi_space_mb === null) {
      problem = "node_flags_unverified";
    } else if (expectedSemi !== null && measured.semi_space_mb !== expectedSemi) {
      problem = "node_flags_mismatch";
    }
  }
  return {
    expected_semi_space_mb: expectedSemi,
    expected_node_options: expectedOptions,
    measured,
    problem,
  };
}

const readText = (file: string): string => {
  try {
    return NodeFS.readFileSync(file, "utf8");
  } catch {
    return "";
  }
};

const readJson = (file: string): unknown => {
  try {
    return JSON.parse(NodeFS.readFileSync(file, "utf8")) as unknown;
  } catch {
    return undefined;
  }
};

/** The server's own startup line about its runtime flags (logged at Info and below), or null. */
export function serverV8LogEntry(log: string): string | null {
  const start = log.indexOf("server.v8");
  if (start === -1) return null;
  const lineStart = log.lastIndexOf("\n", start) + 1;
  const close = log.indexOf("\n  }", start);
  const end = close === -1 ? log.indexOf("\n", start) : close + 4;
  return log.slice(lineStart, end === -1 ? undefined : end).slice(0, 2048);
}

/**
 * Database readiness from the server's own trace. A server can answer HTTP on its port before its database exists or its
 * migrations have run (seen on upstream 60aec28a9), so load must not start at the port. `Sqlite.setup` wraps both migration
 * chains (upstream's and, on fork builds, the perf fork's) and the statistics refresh; older builds have only
 * `runMigrations`, which is accepted when no `Sqlite.setup` follows it within DB_READY_FALLBACK_MS.
 */
export const DB_READY_SPANS = ["Sqlite.setup", "runMigrations"] as const;
const DB_READY_FALLBACK_MS = 30_000;

export interface DbReadySpan {
  readonly name: (typeof DB_READY_SPANS)[number];
  readonly endMs: number;
  readonly ok: boolean;
}

/**
 * The readiness spans in a chunk of trace lines (NDJSON) that ended at or after `notBeforeMs` (this server's spawn): a trace
 * file kept from an earlier start (a directory fixture's logs, an absolute T3CODE_TRACE_FILE) must not count, and a span
 * without times (timing off: end 0) never counts.
 */
export function dbReadySpans(text: string, notBeforeMs = 1): Array<DbReadySpan> {
  const found: Array<DbReadySpan> = [];
  for (const line of text.split("\n")) {
    if (!line.includes('"Sqlite.setup"') && !line.includes('"runMigrations"')) continue;
    let span: unknown;
    try {
      span = JSON.parse(line);
    } catch {
      continue;
    }
    if (typeof span !== "object" || span === null) continue;
    const record = span as Record<string, unknown>;
    const name = record["name"];
    if (name !== "Sqlite.setup" && name !== "runMigrations") continue;
    const end = Number(record["endTimeUnixNano"] ?? Number.NaN) / 1e6;
    if (!Number.isFinite(end) || end < notBeforeMs) continue;
    const exit = record["exit"] as { readonly _tag?: unknown } | undefined;
    found.push({ name, endMs: end, ok: (exit?._tag ?? "Success") === "Success" });
  }
  return found;
}

/**
 * The readiness decision from the spans seen so far: Sqlite.setup decides (ready, or failed); without it, a finished
 * runMigrations decides once DB_READY_FALLBACK_MS have passed since it ended (a build without Sqlite.setup).
 */
export function dbReadiness(
  spans: ReadonlyArray<DbReadySpan>,
  nowMs: number,
): { readonly state: "ready" | "failed" | "waiting"; readonly span?: DbReadySpan | undefined } {
  const setup = spans.find((span) => span.name === "Sqlite.setup");
  if (setup !== undefined) return { state: setup.ok ? "ready" : "failed", span: setup };
  const migrations = spans.find((span) => span.name === "runMigrations");
  if (migrations !== undefined) {
    if (!migrations.ok) return { state: "failed", span: migrations };
    if (nowMs - migrations.endMs >= DB_READY_FALLBACK_MS)
      return { state: "ready", span: migrations };
  }
  return { state: "waiting" };
}

/**
 * Reads the bytes appended since the last scan to the trace file and its rotated siblings, whole lines only, and returns the
 * readiness spans in them. Offsets are kept per file (device and inode), not per name: rotation renames the active file and
 * starts a new one under the old name, which must be read from its start.
 */
export function scanTraceForReadiness(
  tracePath: string,
  offsets: Map<string, number>,
  notBeforeMs: number,
): Array<DbReadySpan> {
  const spans: Array<DbReadySpan> = [];
  const dir = NodePath.dirname(tracePath);
  const base = NodePath.basename(tracePath);
  let names: Array<string> = [];
  try {
    names = NodeFS.readdirSync(dir).filter((name) => name === base || name.startsWith(`${base}.`));
  } catch {
    return spans;
  }
  for (const name of names) {
    const file = NodePath.join(dir, name);
    // Open first and stat the descriptor: the identity and size then belong to the bytes read, even if rotation renames or
    // replaces the name in between. A name that vanished since readdir is skipped (the next scan sees its new name).
    let fd: number;
    try {
      fd = NodeFS.openSync(file, "r");
    } catch {
      continue;
    }
    let buffer: Buffer;
    let from: number;
    let key: string;
    try {
      const stat = NodeFS.fstatSync(fd);
      key = `${stat.dev}:${stat.ino}`;
      from = offsets.get(key) ?? 0;
      if (stat.size < from) from = 0;
      if (stat.size <= from) continue;
      buffer = Buffer.alloc(stat.size - from);
      const read = NodeFS.readSync(fd, buffer, 0, buffer.length, from);
      buffer = buffer.subarray(0, read);
    } finally {
      NodeFS.closeSync(fd);
    }
    const lastNewline = buffer.lastIndexOf(0x0a);
    if (lastNewline < 0) continue;
    offsets.set(key, from + lastNewline + 1);
    spans.push(...dbReadySpans(buffer.subarray(0, lastNewline + 1).toString("utf8"), notBeforeMs));
  }
  return spans;
}

/**
 * Database readiness without readiness spans (trace timing off, or a trace level above Info drops them): the first
 * authenticated shell read that answers 200 has been served from the migrated database.
 */
async function waitDatabaseReadyByHttp(
  origin: string,
  token: string,
  child: NodeChildProcess.ChildProcess,
  timeoutS: number,
): Promise<{
  readonly problem?: string | undefined;
  readonly readyAtMs?: number | undefined;
  readonly waitedMs: number;
}> {
  const started = Date.now();
  const deadline = started + timeoutS * 1000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null || child.signalCode !== null) {
      return { problem: "server_exited", waitedMs: Date.now() - started };
    }
    try {
      // Never past the deadline: the request gets only the time that is left.
      const left = Math.min(30_000, deadline - Date.now());
      if (left <= 0) break;
      const response = await fetch(
        `${origin}/api/orchestration/shell?omitIdleSubagents=1&omitOlderSettled=1`,
        { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(left) },
      );
      await response.arrayBuffer();
      if (response.ok && Date.now() <= deadline) {
        return { readyAtMs: Date.now(), waitedMs: Date.now() - started };
      }
    } catch {
      // Not ready yet.
    }
    await sleep(250);
  }
  return { problem: "db_ready_timeout", waitedMs: Date.now() - started };
}

/** Waits until the server's trace shows its database set up and migrated; reads only the bytes added since the last poll. */
async function waitDatabaseReady(
  tracePath: string,
  child: NodeChildProcess.ChildProcess,
  timeoutS: number,
  notBeforeMs: number,
): Promise<{
  readonly problem?: string | undefined;
  readonly span?: DbReadySpan | undefined;
  readonly waitedMs: number;
}> {
  const started = Date.now();
  const deadline = started + timeoutS * 1000;
  const offsets = new Map<string, number>();
  const spans: Array<DbReadySpan> = [];
  while (Date.now() < deadline) {
    if (child.exitCode !== null || child.signalCode !== null) {
      return { problem: "server_exited", waitedMs: Date.now() - started };
    }
    spans.push(...scanTraceForReadiness(tracePath, offsets, notBeforeMs));
    const decision = dbReadiness(spans, Date.now());
    if (decision.state === "ready") return { span: decision.span, waitedMs: Date.now() - started };
    if (decision.state === "failed")
      return { problem: "db_setup_failed", span: decision.span, waitedMs: Date.now() - started };
    await sleep(250);
  }
  return { problem: "db_ready_timeout", waitedMs: Date.now() - started };
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
  /** SIGTERM grace before SIGKILL when the server is stopped (--stop-timeout). */
  readonly stopTimeoutS: number;
  readonly keepRuns: boolean;
  /** The preload run.ts adds to NODE_OPTIONS (absolute path); null with --preload none. */
  readonly preload: string | null;
  readonly seedCompactedAt: boolean;
  /** --now, normalised; null when not given. */
  readonly now: string | null;
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
  const compactionMarker = options.seedCompactedAt ? seedCompactedAt(baseDir, Date.now()) : null;
  const problems: Array<string> = [];
  const receiver = options.otlpIntervalMs > 0 ? await startOtlpReceiver() : undefined;
  const port = await freePort();
  const origin = `http://127.0.0.1:${port}`;
  const bootstrap = NodeCrypto.randomUUID();
  const env: NodeJS.ProcessEnv = {
    ...cleanEnv(),
    ...target.env,
    ...target.nodeFlags?.env,
    ...(receiver === undefined
      ? {}
      : {
          T3CODE_OTLP_METRICS_URL: receiver.url,
          T3CODE_OTLP_PROTOCOL: "http/json",
          T3CODE_OTLP_EXPORT_INTERVAL_MS: String(options.otlpIntervalMs),
        }),
  };
  const serverLog = NodeFS.openSync(NodePath.join(runDir, "server.log"), "w");
  // A dist or source ref runs under this node; a SEA ref is the executable itself, which reads NODE_OPTIONS or, when
  // built to take them on its command line (exec-argv.json "cli"), gets them as a leading --node-options=….
  const launcher =
    target.entryKind === "sea"
      ? seaCommand(target, env.NODE_OPTIONS)
      : [process.execPath, target.entry];
  const [command, ...args] = pinned(options.serverCpus, [
    ...launcher,
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
  const spawnedAtMs = Date.now();
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
  const tracePath =
    target.env.T3CODE_TRACE_FILE === undefined
      ? NodePath.join(baseDir, "userdata", "logs", "server.trace.ndjson")
      : NodePath.resolve(runDir, target.env.T3CODE_TRACE_FILE);
  const httpProblem = await waitReady(origin, server, options.startupTimeoutS);
  const httpReadyMs = Date.now();
  // What is left of the startup budget (0 when spent: the waits then time out at once, never past the deadline).
  const remainingS = () => Math.max(0, options.startupTimeoutS - (Date.now() - spawnedAtMs) / 1000);
  // Load starts only once the database is set up and migrated, on every arm alike (the port can answer earlier): from the
  // server's own Sqlite.setup span when the trace carries timed Info spans, else from the first authenticated shell read.
  const spansCarryReadiness = traceRequired(target.env);
  let dbReady:
    | {
        readonly problem?: string | undefined;
        readonly at?: number | undefined;
        readonly via: string;
        readonly waitedMs: number;
      }
    | undefined;
  let token: string | undefined;
  if (httpProblem === undefined && spansCarryReadiness) {
    const ready = await waitDatabaseReady(tracePath, server, remainingS(), spawnedAtMs);
    dbReady = {
      problem: ready.problem,
      at: ready.span?.endMs,
      via: ready.span?.name ?? "trace",
      waitedMs: ready.waitedMs,
    };
    if (ready.problem === undefined) token = await exchangeBootstrap(origin, bootstrap);
  } else if (httpProblem === undefined) {
    token = await exchangeBootstrap(origin, bootstrap);
    if (token !== undefined) {
      const ready = await waitDatabaseReadyByHttp(origin, token, server, remainingS());
      dbReady = {
        problem: ready.problem,
        at: ready.readyAtMs,
        via: "http shell read",
        waitedMs: ready.waitedMs,
      };
    }
  }
  const startProblem = httpProblem ?? dbReady?.problem;
  // The bench preload writes heap-config.json in the server process before the server code runs.
  const nodeFlagsCheck =
    startProblem === undefined
      ? checkNodeFlags(target.nodeFlags, readJson(NodePath.join(runDir, "heap-config.json")))
      : null;
  let window: WindowBounds | null = null;
  let driver: DriverSummary | null = null;
  // The driver's exit and its result file, for driver_failure when it fails.
  let driverEnd: {
    readonly exit: { readonly code: number | null; readonly signal: string | null };
    readonly resultFile: "present" | "missing" | "unreadable";
    readonly timedOut: boolean;
  } | null = null;
  let collector: Collector | undefined;
  if (startProblem !== undefined) {
    problems.push(startProblem);
  } else if (nodeFlagsCheck?.problem) {
    // The server does not run with the flags its build asks for: nothing it measures would describe that build.
    problems.push(nodeFlagsCheck.problem);
  } else if (token === undefined) {
    problems.push("server_auth_failed");
  } else {
    collector = new Collector(server.pid!);
    collector.start();
    const driverOut = NodePath.join(runDir, "driver");
    const driverLogPath = NodePath.join(runDir, "driver.log");
    const driverLog = NodeFS.openSync(driverLogPath, "w");
    // The preload's window histogram opens and closes on the driver's own phase lines.
    const stopWindowPhases =
      options.preload === null ? () => {} : followWindowPhases(driverLogPath, runDir);
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
    stopWindowPhases();
    // The preload notices window-end on its next 1 Hz tick; the server must still be up then.
    if (options.preload !== null) await awaitLoopWindow(runDir, 3000);
    NodeFS.closeSync(driverLog);
    collector.stop();
    if (collector.gone && !problems.includes("server_exited")) problems.push("pid_gone");
    const resultPath = NodePath.join(driverOut, "driver.json");
    try {
      driver = readDriverResult(JSON.parse(NodeFS.readFileSync(resultPath, "utf8")));
    } catch {
      driver = null;
    }
    driverEnd = {
      exit: { code: child.exitCode, signal: child.signalCode },
      resultFile:
        driver !== null ? "present" : NodeFS.existsSync(resultPath) ? "unreadable" : "missing",
      timedOut: outcome === "timeout",
    };
    problems.push(
      ...driverProblems(driver, NodeFS.existsSync(NodePath.join(driverOut, "driver.failed"))),
    );
    window = driver?.window ?? {
      startMs: driverStart + options.warmupS * 1000,
      endMs: driverStart + (options.warmupS + options.windowS) * 1000,
    };
  }
  const serverExit = { code: server.exitCode, signal: server.signalCode };
  // How the server stopped: a forced (SIGKILL) stop may lose the shutdown spans and logs (server.stop, required).
  const stop = await stopGroup(server, options.stopTimeoutS * 1000);
  live.delete(server);
  logOpen = false;
  NodeFS.closeSync(serverLog);
  await receiver?.close();

  const proc =
    collector === undefined || window === null
      ? null
      : summariseWindow(server.pid!, collector.snapshots, window);
  problems.push(...(proc?.problems ?? []));
  const trace = window === null ? null : await readTraceWindow(tracePath, window);
  problems.push(...(trace?.problems ?? []));
  const spans = trace?.valid ? summariseSpans(trace.spans) : null;
  const otlp =
    receiver === undefined || window === null ? null : summariseGauges(receiver.points, window);
  if (otlp !== null && otlp.samples_in_window === 0) problems.push("otlp_no_samples");
  // Whether the preload took effect in this server (a run that started it); its metrics are n/a when not.
  const preload =
    startProblem === undefined ? preloadOutcome(runDir, options.preload !== null) : null;
  if (preload?.requested === true && !preload.loaded) problems.push("preload_not_loaded");
  const loop = preload?.loaded === true && window !== null ? loopMetrics(runDir) : null;

  const metrics = JSON.parse(
    JSON.stringify({
      cpu: proc?.cpu ?? null,
      io: proc?.io ?? null,
      mem: proc?.mem ?? null,
      otlp,
      loop,
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
  // Which problems a saturated server explains, from the server's own ELU over the window.
  const elu = serverElu(metrics);
  const classified = classifyProblems({
    problems: [...new Set(problems)],
    elu: elu.elu,
    traceRequired: traceRequired(target.env),
  });
  const valid = classified.valid;
  const record = {
    schema: "t3-server-bench/run/1",
    label: target.label,
    attempt,
    valid,
    // Each problem with its kind: "saturation" (explained by a saturated server, the attempt stays valid) or "defect".
    problems: classified.problems,
    saturated: classified.saturated,
    saturation: { elu: elu.elu, elu_source: elu.source, threshold: SATURATION_ELU },
    started_at: startedAt,
    now: options.now,
    preload,
    compaction_marker: compactionMarker,
    resource_monitor: target.resourceMonitor ?? null,
    window:
      window === null
        ? null
        : {
            start: new Date(window.startMs).toISOString(),
            end: new Date(window.endMs).toISOString(),
          },
    server: {
      pid: server.pid ?? null,
      exit_before_stop: serverExit,
      stop,
      node_flags: nodeFlagsCheck,
      // When the port answered and when the database was ready (load starts after both).
      readiness: {
        http_ready_at: httpProblem === undefined ? new Date(httpReadyMs).toISOString() : null,
        db_ready_via: dbReady?.via ?? null,
        db_ready_at: dbReady?.at === undefined ? null : new Date(dbReady.at).toISOString(),
        db_ready_after_http_ms:
          dbReady?.at === undefined ? null : Math.round(dbReady.at - httpReadyMs),
        waited_for_db_ms: dbReady ? dbReady.waitedMs : null,
      },
      v8_log: serverV8LogEntry(readText(NodePath.join(runDir, "server.log"))),
    },
    // Sessions that produced provider output in the window, of all sessions, and whether some went unserved (a
    // saturated server: the attempt stays valid, flagged sessions_starved). Nulls when the window was not measured.
    ...saturation(driver),
    // Why the driver failed (driver_failed only), readable without the run's directory.
    driver_failure:
      problems.includes("driver_failed") && driverEnd !== null
        ? driverFailure(driver, driverEnd.exit, driverEnd.resultFile, driverEnd.timedOut)
        : null,
    driver:
      driver === null
        ? null
        : {
            ok: driver.ok,
            error: driver.error,
            failure_code: driver.failure_code,
            knobs: driver.knobs,
            workload: driver.workload,
          },
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
    `${target.label} attempt ${attempt}: ${valid ? (record.saturated ? "valid, saturated" : "valid") : "INVALID"}${record.problems.length > 0 ? ` (${record.problems.map((problem) => `${problem.code}: ${problem.kind}`).join(", ")})` : ""}`,
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
      base: { type: "string", default: DEFAULT_BASE },
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
      "turn-gap": { type: "string" },
      settings: { type: "string" },
      "seed-compacted-at": { type: "boolean", default: false },
      preload: { type: "string" },
      now: { type: "string" },
      archive: { type: "string", multiple: true },
      "server-env": { type: "string", multiple: true },
      "ref-env": { type: "string", multiple: true },
      "server-cpus": { type: "string" },
      "driver-cpus": { type: "string" },
      "otlp-interval-ms": { type: "string", default: "1000" },
      out: { type: "string" },
      work: { type: "string" },
      "build-cmd": { type: "string", default: "vp i && vp run --filter t3 build" },
      "startup-timeout": { type: "string", default: "300s" },
      "stop-timeout": { type: "string", default: "120s" },
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
  const stopTimeoutS = parseDuration(values["stop-timeout"], "--stop-timeout");
  if (stopTimeoutS <= 0) throw new Error("--stop-timeout must be positive");
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
  if (values.sessions !== undefined) parseCount(values.sessions, "--sessions", 1);
  // 0 subscribers / 0 pollers is the one-session profile; the driver takes both.
  for (const flag of ["subscribers", "pollers"] as const) {
    if (values[flag] !== undefined) parseCount(values[flag], `--${flag}`, 0);
  }
  if (values["turn-gap"] !== undefined && !DRIVER_DURATION.test(values["turn-gap"].trim())) {
    throw new Error("--turn-gap must be a duration such as 5s, 1500ms or 1m");
  }
  const now = values.now === undefined ? null : parseNow(values.now);
  const preload = resolvePreload(values.preload, harnessDir);
  const archives = parseArchiveSpecs(values.archive ?? [], refs);
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

  const serverEnv: Record<string, string> = {};
  for (const assignment of values["server-env"] ?? []) {
    const [name, value] = parseAssignment(assignment, "--server-env");
    serverEnv[name] = value;
  }
  const harness = await harnessIdentity(repoRoot);
  // An installed harness has no git to resolve --base: it is recorded as given (its SHA only when it is one).
  const baseSha =
    harness.source === "git"
      ? resolveCommit(repoRoot, values.base)
      : HEX40.test(values.base)
        ? values.base
        : null;
  const harnessSha = harness.sha;
  const harnessFilesSha = await harnessFilesSha256();
  const preloadSha =
    preload === null
      ? null
      : await filesHash(
          [preload, NodePath.join(NodePath.dirname(preload), "bench-preload.mjs")].filter(isFile),
        );
  const fixture = await fixtureInfo(values.fixture, workDir, values.settings);
  const prepared: Array<Omit<Target, "env">> = [];
  for (const [index, spec] of refs.entries()) {
    prepared.push(await prepareTarget(spec, workDir, values["build-cmd"], archives.get(index)));
  }
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
  const targets: Array<Target> = [];
  for (const target of prepared) {
    const composed = composeServerEnv({
      defaults: BENCH_DEFAULT_ENV,
      serverEnv,
      refEnv: refEnv.get(target.label),
      archive: target.archive,
      preload,
    });
    checkResourceMonitor(composed.resourceMonitor);
    const withEnv: Target = {
      ...target,
      env: composed.env,
      resourceMonitor: composed.resourceMonitor,
    };
    if (target.archive !== null) {
      console.error(
        `${target.label}: ${target.archive.layout} archive, t3 sha256 ${target.archive.binary_sha256}; resource monitor ${
          composed.resourceMonitor === null
            ? "none"
            : `${composed.resourceMonitor.path} (${composed.resourceMonitor.source})`
        }`,
      );
    }
    const nodeFlags = await probeNodeFlags(withEnv);
    console.error(
      `${target.label}: ${
        nodeFlags.supported
          ? `${NODE_FLAGS_COMMAND} ${JSON.stringify(nodeFlags.report)}; launcher adds ${addedNodeOptions(nodeFlags) ?? "nothing"}`
          : `no ${NODE_FLAGS_COMMAND} (${nodeFlags.unsupportedReason}); launched as before`
      }`,
    );
    targets.push({ ...withEnv, nodeFlags });
  }

  const driverPath =
    values.driver === undefined
      ? NodeFS.existsSync(NodePath.join(harnessDir, "driver.ts"))
        ? NodePath.join(harnessDir, "driver.ts")
        : NodePath.join(harnessDir, "driver-stub.ts")
      : values.driver === "stub"
        ? NodePath.join(harnessDir, "driver-stub.ts")
        : NodePath.resolve(values.driver);
  const driverArgs = [
    ...(["sessions", "subscribers", "pollers", "turn-gap"] as const).flatMap((flag) =>
      values[flag] === undefined ? [] : [`--${flag}`, values[flag]!.trim()],
    ),
    ...(values["driver-arg"] ?? []),
  ];
  const driverSha = await driverHash(driverPath);
  const fakesSha = await fakesHash(harnessDir);
  const launchersSha = await launchersHash(fixture, harnessDir);
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
    stopTimeoutS,
    keepRuns: values["keep-runs"],
    preload,
    seedCompactedAt: values["seed-compacted-at"],
    now,
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
        server_entry: target.entryKind,
        server_sha: target.sha,
        server_ref: target.ref,
        server_dirty: target.dirty,
        server_tree: target.tree,
        server_version: target.version,
        server_archive_sha256: target.archive?.archive_sha256 ?? null,
        server_binary_sha256: target.archive?.binary_sha256 ?? null,
        server_layout: target.archive?.layout ?? null,
        server_archive_verified: target.archiveVerified ?? false,
        server_release:
          target.archive?.release === undefined || target.archive.release === null
            ? null
            : { ...target.archive.release },
        server_resource_monitor:
          target.resourceMonitor === undefined || target.resourceMonitor === null
            ? null
            : { ...target.resourceMonitor, found: isFile(target.resourceMonitor.path) },
        server_exec_argv: target.archive?.exec_argv ?? null,
        server_node_flags:
          target.nodeFlags === undefined
            ? null
            : {
                supported: target.nodeFlags.supported,
                applied: target.nodeFlags.report?.applied ?? null,
                max_semi_space_mb: target.nodeFlags.expectedSemiSpaceMb,
                kept_reason:
                  target.nodeFlags.report?.keptReason ?? target.nodeFlags.unsupportedReason,
                added: addedNodeOptions(target.nodeFlags),
              },
        base_sha: baseSha,
        base_ref: values.base,
        harness_version: harness.version,
        harness_sha: harnessSha,
        harness_source_sha: harness.source_sha,
        harness_files_sha256: harnessFilesSha,
        fixture: {
          kind: fixture.kind,
          sha256: fixture.sha256,
          bytes: fixture.bytes,
          settings_sha256: fixture.settings?.sha256 ?? null,
        },
        now,
        preload: preload === null ? null : { file: NodePath.basename(preload), sha256: preloadSha },
      },
      // True only when the preload loaded in every valid run (its event-loop metrics are n/a otherwise).
      preload:
        preload !== null &&
        valid.length > 0 &&
        valid.every((record) => record.preload?.loaded === true),
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
          launchers_sha256: launchersSha,
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
          stop_timeout_s: stopTimeoutS,
          seed_compacted_at: values["seed-compacted-at"],
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

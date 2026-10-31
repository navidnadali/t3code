// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalTimers:off preferSchemaOverJson:off - standalone bench sampler outside the Effect runtime; it owns its 1 Hz timer.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeReadline from "node:readline";
import * as NodeUtil from "node:util";
import * as NodeZlib from "node:zlib";

export const HELP = `Usage:
  node tools/server-bench/collect.ts --pid <pid> --trace <server.trace.ndjson> --duration <s> --out <file>
                                     [--otlp-port <port>] [--interval-ms 1000]

Samples one running T3 Code server at 1 Hz and writes one JSON summary of the window
[start, start + duration]. run.ts uses the same functions for every bench run.

Metric definitions (the same as tools/prod-sample.sh, so bench and production compare):
  CPU     utime/stime deltas of /proc/<pid>/task/<tid>/stat over the window, % of one core.
  I/O     syscr/syscw/rchar/wchar/read_bytes deltas of /proc/<pid>/task/<tid>/io, per second.
          "process" is /proc/<pid>/io, which also folds in the I/O of exited threads and reaped
          children (git, claude); "threads_sum" is the server's own live threads only. Per-thread
          deltas are summed over the 1 s intervals, so a thread that starts mid-window counts in
          full; the last partial second of a thread that exits mid-window is lost.
  Memory  peak VmRSS of the 1 s samples; VmHWM at the end.
  Gauges  t3_server_* gauges posted by the server's OTLP exporter to a local receiver
          (T3CODE_OTLP_METRICS_URL=<receiver>/v1/metrics, T3CODE_OTLP_PROTOCOL=http/json). The server
          samples them from 60 s after start every 30 s and on each client connect
          (EventLoopMonitor.ts); each sample covers the time since the previous one. ELU and CPU
          are the means weighted by that time over the samples whose interval lies in the window,
          event-loop delay their maximum; RSS and heap peaks use every sample seen in the window.
          ELU therefore needs warm-up + window of 90 s or more.
  Spans   server.trace.ndjson plus its rotated files (.1 newest .. .N), de-duplicated by inode and
          re-read when a rotation happens during the read; spans whose end falls in the window,
          p50/p95/max (nearest rank) by name, and for "sqlite"/"sql.transaction" by name and
          db.connection with db.lock_wait_ms. Only names, durations, exit tags and the db.*
          attributes are kept. A trace that does not reach back to the window start
          (trace_coverage) or whose newest span ends 30 s or more before the window end
          (trace_stale) yields no span statistics (n/a).

Thread labels (every Node worker thread has comm "WorkerThread", so the SQLite workers are told
apart by I/O pattern and start order, judged from each thread's lifetime counters):
  main           tid == pid.
  checkpoint     the WorkerThread whose lifetime writes look like a WAL checkpoint (wchar >= 1 MiB
                 and 0.5 <= wchar/rchar <= 2: it copies WAL frames into the database); if none has
                 checkpointed yet, the earliest-started WorkerThread that is not reading like a
                 reader (the checkpoint worker starts with the writer connection, the reader workers
                 only on their first read).
  sqlite-reader  every other WorkerThread that writes at most max(64 KiB, 1 % of its reads).
  other          everything else (V8 platform, libuv pool, inspector, threads of native modules).
Each thread's label and its basis ("io-pattern", "start-order", "default") are in the output.

macOS has no /proc: process user/sys CPU and RSS come from ps(1); per-thread CPU and all I/O
rows are null (rendered n/a).`;

export type ThreadRole = "main" | "sqlite-reader" | "checkpoint" | "other";
type RoleBasis = "pid" | "io-pattern" | "start-order" | "default" | "comm";

export interface IoCounters {
  readonly rchar: number;
  readonly wchar: number;
  readonly syscr: number;
  readonly syscw: number;
  readonly read_bytes: number;
  readonly write_bytes: number;
}

export interface ThreadSample {
  readonly tid: number;
  readonly comm: string;
  /** Start time in clock ticks since boot; with the tid it identifies a thread across samples. */
  readonly starttime: number;
  readonly userS: number;
  readonly sysS: number;
  readonly io: IoCounters | null;
}

export interface Snapshot {
  /** performance.now() in ms; rates use it. */
  readonly mono: number;
  /** Epoch ms; window bounds and trace times use it. */
  readonly wall: number;
  readonly source: "proc" | "ps";
  readonly userS: number;
  readonly sysS: number;
  readonly io: IoCounters | null;
  readonly threads: ReadonlyMap<string, ThreadSample> | null;
  readonly rssKb: number | null;
  readonly hwmKb: number | null;
}

export interface WindowBounds {
  readonly startMs: number;
  readonly endMs: number;
}

export interface CpuStats {
  readonly user_pct: number;
  readonly sys_pct: number;
  readonly busy_pct: number;
}

export interface IoRates {
  readonly syscr_per_s: number;
  readonly syscw_per_s: number;
  readonly rchar_mb_s: number;
  readonly wchar_mb_s: number;
  readonly read_bytes_mb_s: number;
}

export interface ThreadRow {
  readonly tid: number;
  readonly comm: string;
  readonly role: ThreadRole;
  readonly basis: RoleBasis;
  readonly starttime: number;
  readonly user_pct: number;
  readonly sys_pct: number;
  readonly busy_pct: number;
  readonly syscr_per_s: number | null;
  readonly syscw_per_s: number | null;
  readonly rchar_mb_s: number | null;
  readonly wchar_mb_s: number | null;
  readonly lifetime_rchar_mb: number | null;
  readonly lifetime_wchar_mb: number | null;
}

export interface ProcWindowSummary {
  readonly source: "proc" | "ps" | null;
  readonly samples: number;
  readonly elapsed_s: number | null;
  readonly problems: ReadonlyArray<string>;
  readonly cpu: {
    readonly main: CpuStats | null;
    readonly process: CpuStats | null;
    readonly roles: Partial<Record<ThreadRole, CpuStats & { readonly threads: number }>> | null;
    readonly main_busy_1s: {
      readonly p50_pct: number;
      readonly p95_pct: number;
      readonly max_pct: number;
    } | null;
  };
  readonly io: {
    readonly main: IoRates | null;
    readonly threads_sum: IoRates | null;
    readonly process: IoRates | null;
    readonly roles: Partial<Record<ThreadRole, IoRates>> | null;
  };
  readonly mem: { readonly rss_peak_mib: number | null; readonly hwm_mib: number | null };
  readonly threads: ReadonlyArray<ThreadRow> | null;
}

const MIB = 1024 * 1024;

export function round3(value: number): number {
  return Math.abs(value) < 100 ? Math.round(value * 1000) / 1000 : Math.round(value * 10) / 10;
}

const roundOrNull = (value: number | null | undefined) =>
  value === null || value === undefined || !Number.isFinite(value) ? null : round3(value);

/** Nearest-rank percentile of an ascending list (prod-sample.sh pct_nr). */
export function percentile(sorted: ReadonlyArray<number>, p: number): number | null {
  if (sorted.length === 0) return null;
  return sorted[Math.max(1, Math.ceil((p / 100) * sorted.length)) - 1] ?? null;
}

const readOptional = (path: string): string | undefined => {
  try {
    return NodeFS.readFileSync(path, "utf8");
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT" || code === "ESRCH" || code === "EACCES" || code === "EPERM") {
      return undefined;
    }
    throw error;
  }
};

export function parseProcStat(text: string, clkTck: number) {
  const close = text.lastIndexOf(")");
  const fields = text
    .slice(close + 2)
    .trim()
    .split(/\s+/);
  const number = (index: number) => {
    const value = Number(fields[index]);
    if (!Number.isFinite(value)) throw new Error(`malformed /proc stat line: ${text.slice(0, 80)}`);
    return value;
  };
  return {
    comm: text.slice(text.indexOf("(") + 1, close),
    state: fields[0] ?? "?",
    userS: number(11) / clkTck,
    sysS: number(12) / clkTck,
    starttime: number(19),
  };
}

export function parseProcIo(text: string): IoCounters | null {
  const values = new Map<string, number>();
  for (const line of text.split("\n")) {
    const [key, value] = line.split(":");
    if (key !== undefined && value !== undefined && /^\s*\d+\s*$/.test(value)) {
      values.set(key.trim(), Number(value));
    }
  }
  if (!values.has("rchar") || !values.has("syscr")) return null;
  const get = (key: string) => values.get(key) ?? 0;
  return {
    rchar: get("rchar"),
    wchar: get("wchar"),
    syscr: get("syscr"),
    syscw: get("syscw"),
    read_bytes: get("read_bytes"),
    write_bytes: get("write_bytes"),
  };
}

const parseStatusKb = (text: string, key: string) => {
  const match = new RegExp(`^${key}:\\s+(\\d+)\\s+kB`, "m").exec(text);
  return match?.[1] === undefined ? null : Number(match[1]);
};

/** `ps` CPU time: [[dd-]hh:]mm:ss[.ss] to seconds. */
export function parsePsTime(text: string): number | null {
  const match = /^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+(?:\.\d+)?)$/.exec(text.trim());
  if (match === null) return null;
  const [, days = "0", hours = "0", minutes = "0", seconds = "0"] = match;
  return Number(days) * 86_400 + Number(hours) * 3600 + Number(minutes) * 60 + Number(seconds);
}

export function clockTicksPerSecond(): number {
  const result = NodeChildProcess.spawnSync("getconf", ["CLK_TCK"], { encoding: "utf8" });
  const value = Number(result.stdout?.trim());
  return result.status === 0 && Number.isInteger(value) && value > 0 ? value : 100;
}

export function procAvailable(): boolean {
  return NodeFS.existsSync("/proc/self/stat");
}

/** One /proc reading of the process and each of its threads; undefined once the process is gone. */
export function readProcSnapshot(pid: number, clkTck: number): Snapshot | undefined {
  const mono = NodePerfHooks.performance.now();
  const wall = Date.now();
  const statText = readOptional(`/proc/${pid}/stat`);
  if (statText === undefined) return undefined;
  const stat = parseProcStat(statText, clkTck);
  const ioText = readOptional(`/proc/${pid}/io`);
  const threads = new Map<string, ThreadSample>();
  let tids: Array<string>;
  try {
    tids = NodeFS.readdirSync(`/proc/${pid}/task`);
  } catch {
    return undefined;
  }
  for (const tid of tids) {
    const threadStat = readOptional(`/proc/${pid}/task/${tid}/stat`);
    if (threadStat === undefined) continue;
    const parsed = parseProcStat(threadStat, clkTck);
    const threadIo = readOptional(`/proc/${pid}/task/${tid}/io`);
    threads.set(`${tid}:${parsed.starttime}`, {
      tid: Number(tid),
      comm: parsed.comm,
      starttime: parsed.starttime,
      userS: parsed.userS,
      sysS: parsed.sysS,
      io: threadIo === undefined ? null : parseProcIo(threadIo),
    });
  }
  const status = readOptional(`/proc/${pid}/status`) ?? "";
  return {
    mono: (mono + NodePerfHooks.performance.now()) / 2,
    wall: (wall + Date.now()) / 2,
    source: "proc",
    userS: stat.userS,
    sysS: stat.sysS,
    io: ioText === undefined ? null : parseProcIo(ioText),
    threads,
    rssKb: parseStatusKb(status, "VmRSS"),
    hwmKb: parseStatusKb(status, "VmHWM"),
  };
}

/** macOS fallback: process CPU and RSS from ps(1); no threads, no I/O. */
export function readPsSnapshot(pid: number): Snapshot | undefined {
  const mono = NodePerfHooks.performance.now();
  const wall = Date.now();
  const result = NodeChildProcess.spawnSync("ps", ["-o", "rss=,utime=,time=", "-p", String(pid)], {
    encoding: "utf8",
  });
  const [rss, user, total] = (result.stdout ?? "").trim().split(/\s+/);
  const userS = user === undefined ? null : parsePsTime(user);
  const totalS = total === undefined ? null : parsePsTime(total);
  if (result.status !== 0 || userS === null || totalS === null) return undefined;
  return {
    mono,
    wall,
    source: "ps",
    userS,
    sysS: Math.max(0, totalS - userS),
    io: null,
    threads: null,
    rssKb: Number.isFinite(Number(rss)) ? Number(rss) : null,
    hwmKb: null,
  };
}

/** Samples a pid on a fixed 1 s cadence (missed ticks are skipped, never bunched). */
export class Collector {
  readonly pid: number;
  readonly intervalMs: number;
  readonly snapshots: Array<Snapshot> = [];
  #read: () => Snapshot | undefined;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #startedAt = 0;
  #gone = false;

  constructor(pid: number, intervalMs = 1000) {
    this.pid = pid;
    this.intervalMs = intervalMs;
    if (procAvailable()) {
      const clkTck = clockTicksPerSecond();
      this.#read = () => readProcSnapshot(pid, clkTck);
    } else {
      this.#read = () => readPsSnapshot(pid);
    }
  }

  get gone(): boolean {
    return this.#gone;
  }

  start(): void {
    this.#startedAt = NodePerfHooks.performance.now();
    this.#sample();
    this.#schedule();
  }

  stop(): ReadonlyArray<Snapshot> {
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    if (!this.#gone) this.#sample();
    return this.snapshots;
  }

  #sample(): void {
    const snapshot = this.#read();
    if (snapshot === undefined) {
      this.#gone = true;
      return;
    }
    this.snapshots.push(snapshot);
  }

  #schedule(): void {
    if (this.#gone) return;
    const elapsed = NodePerfHooks.performance.now() - this.#startedAt;
    const next = (Math.floor(elapsed / this.intervalMs) + 1) * this.intervalMs;
    this.#timer = setTimeout(() => {
      this.#sample();
      this.#schedule();
    }, next - elapsed);
  }
}

const isWorkerThread = (comm: string) => comm === "WorkerThread";

/** Labels threads by the rules in HELP; `threads` holds each thread's latest sample. */
export function classifyThreads(
  pid: number,
  threads: ReadonlyArray<ThreadSample>,
): Map<string, { readonly role: ThreadRole; readonly basis: RoleBasis }> {
  const labels = new Map<string, { role: ThreadRole; basis: RoleBasis }>();
  const keyOf = (thread: ThreadSample) => `${thread.tid}:${thread.starttime}`;
  const workers = threads
    .filter((thread) => thread.tid !== pid && isWorkerThread(thread.comm))
    .toSorted((a, b) => a.starttime - b.starttime || a.tid - b.tid);
  const reads = (thread: ThreadSample) => thread.io?.rchar ?? 0;
  const writes = (thread: ThreadSample) => thread.io?.wchar ?? 0;
  const checkpointLike = (thread: ThreadSample) =>
    writes(thread) >= MIB &&
    writes(thread) >= 0.5 * reads(thread) &&
    writes(thread) <= 2 * reads(thread);
  const readerLike = (thread: ThreadSample) =>
    reads(thread) >= MIB && writes(thread) <= Math.max(64 * 1024, 0.01 * reads(thread));
  const byIo = workers.filter(checkpointLike).toSorted((a, b) => writes(b) - writes(a))[0];
  const checkpoint = byIo ?? workers.find((thread) => !readerLike(thread));
  for (const thread of threads) {
    const key = keyOf(thread);
    if (thread.tid === pid) {
      labels.set(key, { role: "main", basis: "pid" });
    } else if (!isWorkerThread(thread.comm)) {
      labels.set(key, { role: "other", basis: "comm" });
    } else if (thread === checkpoint) {
      labels.set(key, {
        role: "checkpoint",
        basis: byIo === undefined ? "start-order" : "io-pattern",
      });
    } else if (writes(thread) <= Math.max(64 * 1024, 0.01 * reads(thread))) {
      labels.set(key, {
        role: "sqlite-reader",
        basis: readerLike(thread) ? "io-pattern" : "default",
      });
    } else {
      labels.set(key, { role: "other", basis: "io-pattern" });
    }
  }
  return labels;
}

/** The snapshots inside the window, allowing half an interval of slack at each end. */
export function selectWindow(
  snapshots: ReadonlyArray<Snapshot>,
  bounds: WindowBounds,
  intervalMs = 1000,
): Array<Snapshot> {
  const slack = intervalMs / 2;
  return snapshots.filter(
    (snapshot) => snapshot.wall >= bounds.startMs - slack && snapshot.wall <= bounds.endMs + slack,
  );
}

const cpuStats = (userS: number, sysS: number, elapsedS: number): CpuStats => ({
  user_pct: round3((userS / elapsedS) * 100),
  sys_pct: round3((sysS / elapsedS) * 100),
  busy_pct: round3(((userS + sysS) / elapsedS) * 100),
});

interface IoDelta {
  syscr: number;
  syscw: number;
  rchar: number;
  wchar: number;
  read_bytes: number;
}

const emptyIo = (): IoDelta => ({ syscr: 0, syscw: 0, rchar: 0, wchar: 0, read_bytes: 0 });

const io0: IoCounters = { rchar: 0, wchar: 0, syscr: 0, syscw: 0, read_bytes: 0, write_bytes: 0 };

const addIo = (into: IoDelta, a: IoCounters, b: IoCounters) => {
  into.syscr += b.syscr - a.syscr;
  into.syscw += b.syscw - a.syscw;
  into.rchar += b.rchar - a.rchar;
  into.wchar += b.wchar - a.wchar;
  into.read_bytes += b.read_bytes - a.read_bytes;
};

const ioRates = (delta: IoDelta, elapsedS: number): IoRates => ({
  syscr_per_s: round3(delta.syscr / elapsedS),
  syscw_per_s: round3(delta.syscw / elapsedS),
  rchar_mb_s: round3(delta.rchar / elapsedS / 1e6),
  wchar_mb_s: round3(delta.wchar / elapsedS / 1e6),
  read_bytes_mb_s: round3(delta.read_bytes / elapsedS / 1e6),
});

const emptyProcSummary = (
  source: ProcWindowSummary["source"],
  samples: number,
  problem: string,
): ProcWindowSummary => ({
  source,
  samples,
  elapsed_s: null,
  problems: [problem],
  cpu: { main: null, process: null, roles: null, main_busy_1s: null },
  io: { main: null, threads_sum: null, process: null, roles: null },
  mem: { rss_peak_mib: null, hwm_mib: null },
  threads: null,
});

/** CPU, I/O and memory of the window from the 1 s snapshots. */
export function summariseWindow(
  pid: number,
  snapshots: ReadonlyArray<Snapshot>,
  bounds: WindowBounds,
  intervalMs = 1000,
): ProcWindowSummary {
  const window = selectWindow(snapshots, bounds, intervalMs);
  const first = window[0];
  const last = window.at(-1);
  if (first === undefined || last === undefined || window.length < 2) {
    return emptyProcSummary(snapshots[0]?.source ?? null, window.length, "window_too_short");
  }
  const elapsedS = (last.mono - first.mono) / 1000;
  if (elapsedS <= 0) return emptyProcSummary(first.source, window.length, "window_too_short");
  const rssValues = window.flatMap((snapshot) => (snapshot.rssKb === null ? [] : [snapshot.rssKb]));
  const mem = {
    rss_peak_mib: rssValues.length === 0 ? null : round3(Math.max(...rssValues) / 1024),
    hwm_mib: last.hwmKb === null ? null : round3(last.hwmKb / 1024),
  };
  const processCpu = cpuStats(last.userS - first.userS, last.sysS - first.sysS, elapsedS);
  const processIo = (() => {
    if (first.io === null || last.io === null) return null;
    const delta = emptyIo();
    addIo(delta, first.io, last.io);
    return ioRates(delta, elapsedS);
  })();
  if (window.some((snapshot) => snapshot.threads === null)) {
    return {
      source: first.source,
      samples: window.length,
      elapsed_s: round3(elapsedS),
      problems: ["per_thread_unavailable"],
      cpu: { main: null, process: processCpu, roles: null, main_busy_1s: null },
      io: { main: null, threads_sum: null, process: processIo, roles: null },
      mem,
      threads: null,
    };
  }

  const latest = new Map<string, ThreadSample>();
  for (const snapshot of window) {
    for (const [key, thread] of snapshot.threads ?? []) latest.set(key, thread);
  }
  const labels = classifyThreads(pid, [...latest.values()]);
  const perThread = new Map<string, { userS: number; sysS: number; io: IoDelta | null }>();
  const mainSeries: Array<number> = [];
  for (let index = 1; index < window.length; index++) {
    const a = window[index - 1]!;
    const b = window[index]!;
    const intervalS = (b.mono - a.mono) / 1000;
    for (const [key, after] of b.threads ?? []) {
      // A thread missing from the earlier sample started after it: everything it did counts.
      const before = a.threads?.get(key) ?? {
        ...after,
        userS: 0,
        sysS: 0,
        io: after.io === null ? null : io0,
      };
      const total = perThread.get(key) ?? { userS: 0, sysS: 0, io: emptyIo() };
      total.userS += after.userS - before.userS;
      total.sysS += after.sysS - before.sysS;
      if (before.io !== null && after.io !== null && total.io !== null) {
        addIo(total.io, before.io, after.io);
      } else {
        total.io = null;
      }
      perThread.set(key, total);
      if (after.tid === pid && intervalS > 0) {
        mainSeries.push(
          ((after.userS - before.userS + after.sysS - before.sysS) / intervalS) * 100,
        );
      }
    }
  }

  const roleCpu = new Map<ThreadRole, { userS: number; sysS: number; threads: number }>();
  const roleIo = new Map<ThreadRole, IoDelta | null>();
  let threadsIo: IoDelta | null = emptyIo();
  const rows: Array<ThreadRow> = [];
  for (const [key, total] of perThread) {
    const thread = latest.get(key)!;
    const label = labels.get(key) ?? { role: "other" as const, basis: "default" as const };
    const cpu = roleCpu.get(label.role) ?? { userS: 0, sysS: 0, threads: 0 };
    cpu.userS += total.userS;
    cpu.sysS += total.sysS;
    cpu.threads += 1;
    roleCpu.set(label.role, cpu);
    const previousIo = roleIo.has(label.role) ? roleIo.get(label.role)! : emptyIo();
    if (previousIo === null || total.io === null) {
      roleIo.set(label.role, null);
    } else {
      for (const field of ["syscr", "syscw", "rchar", "wchar", "read_bytes"] as const) {
        previousIo[field] += total.io[field];
      }
      roleIo.set(label.role, previousIo);
    }
    if (threadsIo !== null && total.io !== null) {
      for (const field of ["syscr", "syscw", "rchar", "wchar", "read_bytes"] as const) {
        threadsIo[field] += total.io[field];
      }
    } else {
      threadsIo = null;
    }
    const rates = total.io === null ? null : ioRates(total.io, elapsedS);
    rows.push({
      tid: thread.tid,
      comm: thread.comm,
      role: label.role,
      basis: label.basis,
      starttime: thread.starttime,
      ...cpuStats(total.userS, total.sysS, elapsedS),
      syscr_per_s: rates?.syscr_per_s ?? null,
      syscw_per_s: rates?.syscw_per_s ?? null,
      rchar_mb_s: rates?.rchar_mb_s ?? null,
      wchar_mb_s: rates?.wchar_mb_s ?? null,
      lifetime_rchar_mb: thread.io === null ? null : round3(thread.io.rchar / 1e6),
      lifetime_wchar_mb: thread.io === null ? null : round3(thread.io.wchar / 1e6),
    });
  }
  const roles: Partial<Record<ThreadRole, CpuStats & { threads: number }>> = {};
  for (const [role, cpu] of roleCpu) {
    roles[role] = { ...cpuStats(cpu.userS, cpu.sysS, elapsedS), threads: cpu.threads };
  }
  const ioRoles: Partial<Record<ThreadRole, IoRates>> = {};
  let ioRolesComplete = true;
  for (const [role, io] of roleIo) {
    if (io === null) ioRolesComplete = false;
    else ioRoles[role] = ioRates(io, elapsedS);
  }
  const mainKey = [...perThread.keys()].find((key) => latest.get(key)?.tid === pid);
  const mainTotal = mainKey === undefined ? undefined : perThread.get(mainKey);
  const series = mainSeries.toSorted((a, b) => a - b);
  return {
    source: first.source,
    samples: window.length,
    elapsed_s: round3(elapsedS),
    problems: mainTotal === undefined ? ["main_thread_missing"] : [],
    cpu: {
      main: mainTotal === undefined ? null : cpuStats(mainTotal.userS, mainTotal.sysS, elapsedS),
      process: processCpu,
      roles,
      main_busy_1s:
        series.length === 0
          ? null
          : {
              p50_pct: round3(percentile(series, 50)!),
              p95_pct: round3(percentile(series, 95)!),
              max_pct: round3(series.at(-1)!),
            },
    },
    io: {
      main: mainTotal?.io ? ioRates(mainTotal.io, elapsedS) : null,
      threads_sum: threadsIo === null ? null : ioRates(threadsIo, elapsedS),
      process: processIo,
      roles: ioRolesComplete ? ioRoles : null,
    },
    mem,
    threads: rows.toSorted((a, b) => b.busy_pct - a.busy_pct),
  };
}

export interface SpanRecord {
  readonly name: string;
  readonly durationMs: number;
  readonly endMs: number;
  readonly ok: boolean;
  readonly connection?: string;
  readonly lockWaitMs?: number;
  readonly lockOwner?: string;
}

const SQL_SPANS = new Set(["sqlite", "sql.transaction"]);

/** One trace line to the fields the bench keeps; undefined for a line that is not a span. */
export function parseSpanLine(line: string): SpanRecord | undefined {
  let record: unknown;
  try {
    record = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (typeof record !== "object" || record === null) return undefined;
  const span = record as {
    name?: unknown;
    durationMs?: unknown;
    endTimeUnixNano?: unknown;
    exit?: { _tag?: unknown } | null;
    attributes?: Record<string, unknown> | null;
  };
  const endNs = Number(span.endTimeUnixNano);
  if (!Number.isFinite(endNs) || typeof span.durationMs !== "number") return undefined;
  const name = typeof span.name === "string" && span.name.length > 0 ? span.name : "?";
  const exitTag = span.exit?._tag;
  const base = {
    name,
    durationMs: span.durationMs,
    endMs: endNs / 1e6,
    ok: exitTag === undefined || exitTag === "Success",
  };
  if (!SQL_SPANS.has(name)) return base;
  const attributes = span.attributes ?? {};
  const wait = Number(attributes["db.lock_wait_ms"]);
  const owner = attributes["db.lock_owner_at_start.name"];
  return {
    ...base,
    connection: String(attributes["db.connection"] ?? "none"),
    ...(attributes["db.lock_wait_ms"] !== undefined &&
    attributes["db.lock_wait_ms"] !== null &&
    Number.isFinite(wait)
      ? { lockWaitMs: wait }
      : {}),
    ...(typeof owner === "string" ? { lockOwner: owner } : {}),
  };
}

export interface TraceWindowRead {
  readonly valid: boolean;
  readonly problems: ReadonlyArray<string>;
  readonly attempts: number;
  readonly files: ReadonlyArray<{ readonly name: string; readonly records: number }>;
  readonly bad_lines: number;
  readonly newest_after_window_end_s: number | null;
  readonly spans: ReadonlyArray<SpanRecord>;
}

const fileIdentity = (path: string) => {
  try {
    const stat = NodeFS.statSync(path);
    return `${stat.dev}:${stat.ino}`;
  } catch {
    return undefined;
  }
};

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Spans ending inside the window from the trace and its rotated files (.1 newest .. .N),
 * read newest to oldest until a file starts before the window; re-read when the live file
 * rotates during the read. Like prod-sample.sh, a trace whose newest span ends more than 30 s
 * before the window end is stale (it stopped mid-window) and yields no spans.
 */
export async function readTraceWindow(
  tracePath: string,
  bounds: WindowBounds,
  options: {
    readonly attempts?: number;
    readonly maxRotated?: number;
    readonly staleMs?: number;
  } = {},
): Promise<TraceWindowRead> {
  const attempts = options.attempts ?? 3;
  const staleMs = options.staleMs ?? 30_000;
  const maxRotated = options.maxRotated ?? 1000;
  let result: TraceWindowRead | undefined;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    if (attempt > 1) await sleep(200);
    const before = fileIdentity(tracePath);
    const seen = new Set<string>();
    const spans: Array<SpanRecord> = [];
    const files: Array<{ name: string; records: number }> = [];
    let covered = false;
    let newest = Number.NEGATIVE_INFINITY;
    let badLines = 0;
    for (let index = 0; index <= maxRotated && !covered; index++) {
      const name = index === 0 ? tracePath : `${tracePath}.${index}`;
      let fd: number;
      try {
        fd = NodeFS.openSync(name, "r");
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        if (index === 0) continue;
        break;
      }
      const stat = NodeFS.fstatSync(fd);
      const identity = `${stat.dev}:${stat.ino}`;
      if (seen.has(identity)) {
        NodeFS.closeSync(fd);
        continue;
      }
      seen.add(identity);
      let records = 0;
      let oldest = Number.POSITIVE_INFINITY;
      const lines = NodeReadline.createInterface({
        input: NodeFS.createReadStream("", { fd, autoClose: true }),
        crlfDelay: Number.POSITIVE_INFINITY,
      });
      for await (const line of lines) {
        if (line.length === 0) continue;
        const span = parseSpanLine(line);
        if (span === undefined) {
          badLines++;
          continue;
        }
        records++;
        oldest = Math.min(oldest, span.endMs);
        newest = Math.max(newest, span.endMs);
        if (span.endMs >= bounds.startMs && span.endMs <= bounds.endMs) spans.push(span);
      }
      files.push({ name: NodePath.basename(name), records });
      if (oldest <= bounds.startMs) covered = true;
    }
    const rotated = before === undefined || fileIdentity(tracePath) !== before;
    const stale = !(newest >= bounds.endMs - staleMs);
    const problems =
      files.length === 0
        ? ["trace_missing"]
        : rotated
          ? ["trace_rotated_during_read"]
          : !covered
            ? ["trace_coverage"]
            : stale
              ? ["trace_stale"]
              : [];
    result = {
      valid: problems.length === 0,
      problems,
      attempts: attempt,
      files,
      bad_lines: badLines,
      newest_after_window_end_s: Number.isFinite(newest)
        ? round3((newest - bounds.endMs) / 1000)
        : null,
      spans: problems.length === 0 ? spans : [],
    };
    // A coverage gap can be a rename chain in flight, so it is read again like a rotation.
    if (result.valid || problems[0] === "trace_missing" || problems[0] === "trace_stale") break;
  }
  return result!;
}

export interface SpanStats {
  readonly count: number;
  readonly errors: number;
  readonly total_ms: number;
  readonly avg_ms: number;
  readonly p50_ms: number;
  readonly p95_ms: number;
  readonly max_ms: number;
}

export interface SqlStats extends SpanStats {
  readonly lock_wait_n: number;
  readonly lock_wait_avg_ms: number | null;
  readonly lock_wait_p50_ms: number | null;
  readonly lock_wait_p95_ms: number | null;
  readonly lock_wait_max_ms: number | null;
}

export interface SpanSummary {
  readonly spans: number;
  readonly errors: number;
  readonly by_name: Readonly<Record<string, SpanStats>>;
  /** Keyed `<span name>|<db.connection>`, as in prod-sample.sh. */
  readonly sql: Readonly<Record<string, SqlStats>>;
  /** Who held the connection when a waiting SQL span started, by total wait. */
  readonly lock_owners: ReadonlyArray<{
    readonly connection: string;
    readonly owner: string;
    readonly count: number;
    readonly total_wait_ms: number;
    readonly max_wait_ms: number;
  }>;
}

const spanStats = (durations: Array<number>, errors: number): SpanStats => {
  const sorted = durations.toSorted((a, b) => a - b);
  const total = sorted.reduce((sum, value) => sum + value, 0);
  return {
    count: sorted.length,
    errors,
    total_ms: round3(total),
    avg_ms: round3(total / sorted.length),
    p50_ms: round3(percentile(sorted, 50)!),
    p95_ms: round3(percentile(sorted, 95)!),
    max_ms: round3(sorted.at(-1)!),
  };
};

export function summariseSpans(spans: ReadonlyArray<SpanRecord>, ownerLimit = 20): SpanSummary {
  const byName = new Map<string, { durations: Array<number>; errors: number }>();
  const sql = new Map<string, { durations: Array<number>; errors: number; waits: Array<number> }>();
  const owners = new Map<
    string,
    { connection: string; owner: string; count: number; total: number; max: number }
  >();
  let errors = 0;
  for (const span of spans) {
    const entry = byName.get(span.name) ?? { durations: [], errors: 0 };
    entry.durations.push(span.durationMs);
    if (!span.ok) {
      entry.errors++;
      errors++;
    }
    byName.set(span.name, entry);
    if (span.connection === undefined) continue;
    const key = `${span.name}|${span.connection}`;
    const sqlEntry = sql.get(key) ?? { durations: [], errors: 0, waits: [] };
    sqlEntry.durations.push(span.durationMs);
    if (!span.ok) sqlEntry.errors++;
    if (span.lockWaitMs !== undefined) sqlEntry.waits.push(span.lockWaitMs);
    sql.set(key, sqlEntry);
    if (span.lockOwner !== undefined && span.lockWaitMs !== undefined && span.lockWaitMs > 0) {
      const ownerKey = `${span.connection}|${span.lockOwner}`;
      const owner = owners.get(ownerKey) ?? {
        connection: span.connection,
        owner: span.lockOwner,
        count: 0,
        total: 0,
        max: 0,
      };
      owner.count++;
      owner.total += span.lockWaitMs;
      owner.max = Math.max(owner.max, span.lockWaitMs);
      owners.set(ownerKey, owner);
    }
  }
  const byNameOut: Record<string, SpanStats> = {};
  for (const [name, entry] of [...byName].toSorted(([a], [b]) => a.localeCompare(b))) {
    byNameOut[name] = spanStats(entry.durations, entry.errors);
  }
  const sqlOut: Record<string, SqlStats> = {};
  for (const [key, entry] of [...sql].toSorted(([a], [b]) => a.localeCompare(b))) {
    const waits = entry.waits.toSorted((a, b) => a - b);
    const waitTotal = waits.reduce((sum, value) => sum + value, 0);
    sqlOut[key] = {
      ...spanStats(entry.durations, entry.errors),
      lock_wait_n: waits.length,
      lock_wait_avg_ms: waits.length === 0 ? null : round3(waitTotal / waits.length),
      lock_wait_p50_ms: roundOrNull(percentile(waits, 50)),
      lock_wait_p95_ms: roundOrNull(percentile(waits, 95)),
      lock_wait_max_ms: roundOrNull(waits.at(-1)),
    };
  }
  return {
    spans: spans.length,
    errors,
    by_name: byNameOut,
    sql: sqlOut,
    lock_owners: [...owners.values()]
      .toSorted((a, b) => b.total - a.total)
      .slice(0, ownerLimit)
      .map((owner) => ({
        connection: owner.connection,
        owner: owner.owner,
        count: owner.count,
        total_wait_ms: round3(owner.total),
        max_wait_ms: round3(owner.max),
      })),
  };
}

export interface GaugePoint {
  readonly name: string;
  readonly value: number;
  readonly timeNs: number;
  readonly receivedMs: number;
}

/** t3_server_* gauge data points of one OTLP/HTTP JSON metrics export. */
export function parseOtlpGauges(body: unknown, receivedMs: number): Array<GaugePoint> {
  const points: Array<GaugePoint> = [];
  const list = (value: unknown): Array<unknown> => (Array.isArray(value) ? value : []);
  for (const resource of list((body as { resourceMetrics?: unknown } | null)?.resourceMetrics)) {
    for (const scope of list((resource as { scopeMetrics?: unknown } | null)?.scopeMetrics)) {
      for (const metric of list((scope as { metrics?: unknown } | null)?.metrics)) {
        const { name, gauge } = (metric ?? {}) as {
          name?: unknown;
          gauge?: { dataPoints?: unknown };
        };
        if (typeof name !== "string" || !name.startsWith("t3_server_") || gauge === undefined) {
          continue;
        }
        for (const point of list(gauge.dataPoints)) {
          const { asDouble, asInt, timeUnixNano } = (point ?? {}) as Record<string, unknown>;
          const value = Number(asDouble ?? asInt);
          const timeNs = Number(timeUnixNano);
          if (Number.isFinite(value) && Number.isFinite(timeNs)) {
            points.push({ name, value, timeNs, receivedMs });
          }
        }
      }
    }
  }
  return points;
}

export interface GaugeSummary {
  readonly exports: number;
  readonly samples: number;
  readonly samples_in_window: number;
  readonly intervals_in_window: number;
  readonly elu_mean: number | null;
  readonly elu_max: number | null;
  readonly event_loop_delay_max_ms: number | null;
  readonly cpu_utilization_mean: number | null;
  readonly rss_peak_mib: number | null;
  readonly heap_used_peak_mib: number | null;
}

const GAUGES = [
  "t3_server_event_loop_utilization",
  "t3_server_event_loop_delay_max",
  "t3_server_cpu_utilization",
  "t3_server_resident_bytes",
  "t3_server_heap_used_bytes",
] as const;

/**
 * Every export repeats the latest values, so a new server sample is an export whose gauges differ
 * from the previous one, timed by when it was first received. The server samples every 30 s and
 * also when a client connects, so a sample covers the time since the one before it: ELU and CPU are
 * means weighted by that time, over the samples whose whole interval lies in the window.
 */
export function summariseGauges(
  points: ReadonlyArray<GaugePoint>,
  bounds: WindowBounds,
  options: { readonly slackMs?: number } = {},
): GaugeSummary {
  const slackMs = options.slackMs ?? 2000;
  const exports = new Map<number, { receivedMs: number; values: Map<string, number> }>();
  for (const point of points) {
    const entry = exports.get(point.timeNs) ?? { receivedMs: point.receivedMs, values: new Map() };
    entry.receivedMs = Math.min(entry.receivedMs, point.receivedMs);
    entry.values.set(point.name, point.value);
    exports.set(point.timeNs, entry);
  }
  const samples: Array<{ seenMs: number; values: Map<string, number> }> = [];
  let previous: string | undefined;
  for (const [, entry] of [...exports].toSorted(([a], [b]) => a - b)) {
    const tuple = GAUGES.map((name) => entry.values.get(name) ?? null);
    if (tuple.every((value) => value === null || value === 0)) continue;
    const key = JSON.stringify(tuple);
    if (key === previous) continue;
    previous = key;
    samples.push({ seenMs: entry.receivedMs, values: entry.values });
  }
  const endMs = bounds.endMs + slackMs;
  const intervals = samples.flatMap((sample, index) => {
    const before = samples[index - 1];
    return before !== undefined && before.seenMs >= bounds.startMs && sample.seenMs <= endMs
      ? [{ weightMs: sample.seenMs - before.seenMs, values: sample.values }]
      : [];
  });
  const instants = samples.filter(
    (sample) => sample.seenMs >= bounds.startMs && sample.seenMs <= endMs,
  );
  const weightedMean = (name: string) => {
    let weight = 0;
    let total = 0;
    for (const interval of intervals) {
      const value = interval.values.get(name);
      if (value === undefined) continue;
      weight += interval.weightMs;
      total += value * interval.weightMs;
    }
    return weight > 0 ? round3(total / weight) : null;
  };
  const max = (from: ReadonlyArray<{ values: Map<string, number> }>, name: string, scale = 1) => {
    const values = from.flatMap((entry) => {
      const value = entry.values.get(name);
      return value === undefined ? [] : [value];
    });
    return values.length === 0 ? null : round3(Math.max(...values) / scale);
  };
  return {
    exports: exports.size,
    samples: samples.length,
    samples_in_window: instants.length,
    intervals_in_window: intervals.length,
    elu_mean: weightedMean("t3_server_event_loop_utilization"),
    elu_max: max(intervals, "t3_server_event_loop_utilization"),
    event_loop_delay_max_ms: max(intervals, "t3_server_event_loop_delay_max"),
    cpu_utilization_mean: weightedMean("t3_server_cpu_utilization"),
    rss_peak_mib: max(instants, "t3_server_resident_bytes", MIB),
    heap_used_peak_mib: max(instants, "t3_server_heap_used_bytes", MIB),
  };
}

export interface OtlpReceiver {
  readonly url: string;
  readonly points: Array<GaugePoint>;
  readonly stats: { requests: number; rejected: number };
  close(): Promise<void>;
}

/** A local OTLP/HTTP JSON metrics endpoint at <url> (POST /v1/metrics). */
export async function startOtlpReceiver(port = 0): Promise<OtlpReceiver> {
  const points: Array<GaugePoint> = [];
  const stats = { requests: 0, rejected: 0 };
  const server = NodeHttp.createServer((request, response) => {
    const chunks: Array<Buffer> = [];
    let size = 0;
    request.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size <= 32 * MIB) chunks.push(chunk);
    });
    request.on("end", () => {
      stats.requests++;
      const reject = (status: number) => {
        stats.rejected++;
        response.writeHead(status).end();
      };
      if (request.method !== "POST" || request.url?.split("?")[0] !== "/v1/metrics") {
        return reject(404);
      }
      if (size > 32 * MIB) return reject(413);
      try {
        const raw = Buffer.concat(chunks);
        const body =
          request.headers["content-encoding"] === "gzip" ? NodeZlib.gunzipSync(raw) : raw;
        points.push(...parseOtlpGauges(JSON.parse(body.toString("utf8")), Date.now()));
      } catch {
        return reject(400);
      }
      response.writeHead(200, { "content-type": "application/json" }).end("{}");
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("receiver has no port");
  return {
    url: `http://127.0.0.1:${address.port}/v1/metrics`,
    points,
    stats,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

const SECRET_NAME = /TOKEN|SECRET|KEY|PASS|AUTH|CRED|COOKIE|SESSION|PRIVATE|URL|URI|DSN|HEADER/i;
const SAFE_VALUE = /^[A-Za-z0-9._:,+-]{0,40}$/;

const fingerprint = (value: string) =>
  `<redacted sha256:${NodeCrypto.createHash("sha256").update(value).digest("hex").slice(0, 12)}>`;

/**
 * Knob values from an environment, redacted as prod-sample.sh does, except that a value
 * redacted only for its shape (a path, a long list) keeps a short fingerprint so two
 * different settings never read the same; secret-named knobs stay plain "<redacted>".
 */
export function knobsFromEnv(
  env: Readonly<Record<string, string | undefined>>,
  prefixes: ReadonlyArray<string> = ["T3CODE_", "T3_", "BENCH_"],
): Record<string, string> {
  const knobs: Record<string, string> = {};
  for (const [name, value] of Object.entries(env)) {
    if (value === undefined) continue;
    if (prefixes.some((prefix) => name.startsWith(prefix))) {
      knobs[name] = SECRET_NAME.test(name)
        ? "<redacted>"
        : SAFE_VALUE.test(value)
          ? value
          : fingerprint(value);
    } else if (name === "NODE_OPTIONS") {
      const tokens = value.split(/\s+/).filter((token) => token.length > 0);
      const kept = tokens.filter((token) =>
        /^--(max-old-space-size|max-semi-space-size|max-old-generation-size|stack-size)=\d+$/.test(
          token,
        ),
      );
      knobs[name] =
        kept.join(" ") +
        (tokens.length > kept.length ? ` (+${tokens.length - kept.length} other flags)` : "");
    } else if (name === "UV_THREADPOOL_SIZE" && /^\d+$/.test(value)) {
      knobs[name] = value;
    }
  }
  return Object.fromEntries(Object.entries(knobs).toSorted(([a], [b]) => a.localeCompare(b)));
}

/** The settings that decide which spans exist; `compare` refuses results that differ here. */
export function traceSampling(env: Readonly<Record<string, string | undefined>>) {
  return {
    trace_min_level: env.T3CODE_TRACE_MIN_LEVEL ?? "Info",
    trace_sql_min_duration_ms: env.T3CODE_TRACE_SQL_MIN_DURATION_MS ?? "0",
    trace_timing_enabled: env.T3CODE_TRACE_TIMING_ENABLED ?? "true",
  };
}

const readEnviron = (pid: number): Record<string, string> | undefined => {
  try {
    const raw = NodeFS.readFileSync(`/proc/${pid}/environ`);
    return Object.fromEntries(
      raw
        .toString("utf8")
        .split("\0")
        .filter((entry) => entry.includes("="))
        .map((entry) => [entry.slice(0, entry.indexOf("=")), entry.slice(entry.indexOf("=") + 1)]),
    );
  } catch {
    return undefined;
  }
};

const parseSeconds = (value: string, flag: string) => {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m)?$/.exec(value.trim());
  if (match === null) throw new Error(`${flag} must be a duration such as 300, 300s or 5m`);
  const amount = Number(match[1]);
  return match[2] === "ms" ? amount / 1000 : match[2] === "m" ? amount * 60 : amount;
};

async function main(argv: ReadonlyArray<string>) {
  const { values } = NodeUtil.parseArgs({
    args: [...argv],
    options: {
      pid: { type: "string" },
      trace: { type: "string" },
      duration: { type: "string" },
      out: { type: "string" },
      "otlp-port": { type: "string" },
      "interval-ms": { type: "string", default: "1000" },
      help: { type: "boolean", short: "h" },
    },
  });
  if (values.help) {
    console.log(HELP);
    return 0;
  }
  const pid = Number(values.pid);
  const intervalMs = Number(values["interval-ms"]);
  if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error("--pid must be a process id");
  if (values.trace === undefined || values.out === undefined || values.duration === undefined) {
    throw new Error("--trace, --duration and --out are required (see --help)");
  }
  if (!Number.isInteger(intervalMs) || intervalMs < 100) {
    throw new Error("--interval-ms must be an integer >= 100");
  }
  const durationS = parseSeconds(values.duration, "--duration");
  if (durationS < 2) throw new Error("--duration must be at least 2 s");
  const receiver =
    values["otlp-port"] === undefined
      ? undefined
      : await startOtlpReceiver(Number(values["otlp-port"]));
  if (receiver !== undefined) console.error(`OTLP receiver: ${receiver.url}`);
  const collector = new Collector(pid, intervalMs);
  const startMs = Date.now();
  collector.start();
  await sleep(durationS * 1000);
  const snapshots = collector.stop();
  const bounds = { startMs, endMs: Date.now() };
  // Spans are flushed in batches (T3CODE_TRACE_BATCH_WINDOW_MS, 1 s by default).
  await sleep(3000);
  const trace = await readTraceWindow(values.trace, bounds);
  await receiver?.close();
  const environ = procAvailable() ? readEnviron(pid) : undefined;
  const output = {
    schema: "t3-server-bench/collect/1",
    pid,
    window: {
      start: new Date(bounds.startMs).toISOString(),
      end: new Date(bounds.endMs).toISOString(),
    },
    pid_gone: collector.gone,
    knobs: environ === undefined ? null : knobsFromEnv(environ),
    sampling: environ === undefined ? null : traceSampling(environ),
    proc: summariseWindow(pid, snapshots, bounds, intervalMs),
    otlp:
      receiver === undefined
        ? null
        : { ...summariseGauges(receiver.points, bounds), receiver: receiver.stats },
    trace: {
      valid: trace.valid,
      problems: trace.problems,
      files: trace.files,
      bad_lines: trace.bad_lines,
      ...(trace.valid ? summariseSpans(trace.spans) : {}),
    },
  };
  NodeFS.mkdirSync(NodePath.dirname(NodePath.resolve(values.out)), { recursive: true });
  NodeFS.writeFileSync(values.out, `${JSON.stringify(output, null, 2)}\n`);
  console.error(`wrote ${values.out}`);
  return 0;
}

if (import.meta.main) {
  main(process.argv.slice(2)).then(
    (code) => process.exit(code),
    (error: unknown) => {
      console.error(`collect: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(2);
    },
  );
}

import * as NodeUtil from "node:util";

export const POLL_CALLS = [
  "mcp.t3_project_list",
  "mcp.t3_thread_read",
  "http.threadBounded",
] as const;
export type PollCall = (typeof POLL_CALLS)[number];

const POLL_MIX_KEYS: Record<string, PollCall> = {
  project_list: "mcp.t3_project_list",
  thread_read: "mcp.t3_thread_read",
  http_bounded: "http.threadBounded",
};

/**
 * MCP tool calls per minute per live Claude session in production: ws4-navid's trace showed a
 * median of 12.5 `McpHttpServer.authenticateRequest` spans per 120 s with 166 live sessions
 * (P1-009 first samples, 2026-10-31), about 0.04.
 */
export const PRODUCTION_SESSION_MCP_RATE = 0.04;

export interface DriverOptions {
  readonly url: string;
  readonly out: string;
  readonly tokenFile: string | null;
  readonly mode: "fresh" | "history";
  readonly sessions: number;
  readonly subscribers: number;
  readonly pollers: number;
  readonly projects: number;
  readonly warmupMs: number;
  readonly windowMs: number;
  readonly seed: string;
  readonly provider: string;
  readonly model: string;
  readonly runtimeMode: string;
  readonly pollRate: number;
  readonly pollMix: ReadonlyArray<readonly [PollCall, number]>;
  readonly sessionMcpRatePerMinute: number;
  readonly sessionMcpCredentials: string | null;
  readonly switchIntervalMs: number;
  readonly turnGapMs: number;
  readonly messageChars: number;
  readonly readPool: number;
  readonly distributionTolerance: number;
  readonly workspaceDir: string | null;
  readonly callTimeoutMs: number;
  readonly graceMs: number;
  readonly sessionsPerSocket: number;
  readonly maxDriverElu: number;
  readonly maxDriverDelayMs: number;
  readonly stallTimeoutMs: number;
  readonly setupConcurrency: number;
  readonly maxErrorRate: number;
  readonly taskset: string | null;
  readonly serverPid: number | null;
  readonly stopSessions: boolean;
}

export class UsageError extends Error {
  override readonly name = "UsageError";
  /** `--help`: print the usage and exit cleanly. */
  readonly help: boolean;

  constructor(message: string, options: { readonly help?: boolean } = {}) {
    super(message);
    this.help = options.help === true;
  }
}

/** `300s`, `5m`, `1500ms`, `1h`; a bare number is seconds. */
export function parseDuration(text: string, flag: string): number {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)?$/u.exec(text.trim());
  if (match === null)
    throw new UsageError(`--${flag} must be a duration such as 60s, 5m or 1500ms`);
  const value = Number(match[1]);
  const unit = match[2] ?? "s";
  return value * (unit === "ms" ? 1 : unit === "s" ? 1000 : unit === "m" ? 60_000 : 3_600_000);
}

function parseCount(text: string, flag: string, minimum: number): number {
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new UsageError(`--${flag} must be an integer of at least ${minimum}`);
  }
  return value;
}

function parseFraction(text: string, flag: string): number {
  const value = Number(text);
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new UsageError(`--${flag} must be between 0 and 1`);
  }
  return value;
}

function parseNonNegative(text: string, flag: string): number {
  const value = Number(text);
  if (!Number.isFinite(value) || value < 0) throw new UsageError(`--${flag} must be 0 or more`);
  return value;
}

function parsePositive(text: string, flag: string): number {
  const value = Number(text);
  if (!Number.isFinite(value) || value <= 0) throw new UsageError(`--${flag} must be positive`);
  return value;
}

/** `project_list=1,thread_read=2,http_bounded=1`; omitted calls are not made. */
export function parsePollMix(text: string): ReadonlyArray<readonly [PollCall, number]> {
  const entries = text.split(",").map((part) => {
    const [key, weight, ...rest] = part.split("=");
    const call = key === undefined ? undefined : POLL_MIX_KEYS[key.trim()];
    const value = Number(weight);
    if (call === undefined || rest.length > 0 || !Number.isFinite(value) || value < 0) {
      throw new UsageError(
        `--poll-mix takes ${Object.keys(POLL_MIX_KEYS).join("|")}=<weight> pairs, got "${part}"`,
      );
    }
    return [call, value] as const;
  });
  if (new Set(entries.map(([call]) => call)).size !== entries.length) {
    throw new UsageError("--poll-mix names a call twice");
  }
  if (!entries.some(([, weight]) => weight > 0)) {
    throw new UsageError("--poll-mix needs at least one positive weight");
  }
  return entries;
}

export const USAGE = `Usage: node tools/server-bench/driver.ts --url <origin> --out <dir> [options]

Drives load against a running scratch T3 server and writes <out>/driver.json.
The bearer token comes from T3_BENCH_TOKEN or --token-file; it needs the scopes
orchestration:read, orchestration:operate and access:write, e.g.
  t3 auth session issue --base-dir <scratch> --token-only \\
    --scope orchestration:read --scope orchestration:operate --scope access:write

  --mode history|fresh      history (default): sessions continue, and readers read, seeded
                            threads picked to match the fixture's turn-item distribution;
                            fresh: new projects and threads
  --sessions N              streaming sessions (default 80)
  --subscribers N           WebSocket clients: shell stream + one thread stream (default 80)
  --pollers N               MCP/HTTP pollers (default 20)
  --projects N              fresh mode: projects the threads spread over (default 8)
  --warmup DUR              unmeasured warm-up (default 60s)
  --window DUR              measured window (default 300s)
  --seed TEXT               drives every workload choice (default 1)
  --provider ID             provider instance sessions run on (default claudeAgent)
  --model SLUG              model for new threads and switched ones (default claude-sonnet-4-6)
  --runtime-mode MODE       runtime mode sessions run in (default full-access)
  --poll-rate N             calls per second per poller (default 1)
  --poll-mix SPEC           project_list=W,thread_read=W,http_bounded=W (default 1 each)
  --session-mcp-rate N      MCP tool calls per minute per session, as an agent makes them
                            (default ${PRODUCTION_SESSION_MCP_RATE}, production's rate; 0 turns it off). The calls
                            follow --poll-mix's MCP weights
  --session-mcp-credentials DIR
                            use the credentials providers write here (one JSON file per session:
                            {"url": …, "authorization": …}); the driver sets the provider
                            instance's BENCH_MCP_CREDENTIALS_DIR to DIR (the token then also
                            needs settings:write and providers:manage). Without it each session
                            signs in as its own MCP client
  --switch-interval DUR     subscribers move to another thread this often; 0 holds (default 30s)
  --turn-gap DUR            pause between a session's turns (default 1s)
  --message-chars N         size of each user message (default 400)
  --read-pool N             history mode: threads pollers read (default 200)
  --distribution-tolerance F
                            history mode: fail unless the exercised threads' p50 and p95 turn
                            items are within this fraction of the fixture's (default 0.2)
  --workspace-dir DIR       git workspaces the sessions run in (default <out>/workspaces)
  --call-timeout DUR        per call (default 60s)
  --grace DUR               calls started in the window may finish this long after it; later
                            ones are reported as unfinished (default 60s)
  --sessions-per-socket N   sessions sharing one WebSocket for their commands and own thread
                            streams (default 10)
  --max-driver-elu F        fail the run when the driver's event loop was busier (default 0.7)
  --max-driver-delay DUR    fail the run when the driver's event-loop delay p99 was longer
                            (default 50ms): its own lag would show up as server latency
  --stall-timeout DUR       fail the run when the server leaves a ping unanswered this long
                            (default 30s)
  --setup-concurrency N     parallel setup requests (default 8)
  --max-error-rate F        fail the run when any call type errs more often (default 0.01)
  --taskset CPUS            Linux: pin the driver to these CPUs (taskset -c list)
  --server-pid PID          watch this process; its exit fails the run at once
  --no-stop-sessions        leave runs going when the driver ends
`;

const parseFlags = (argv: ReadonlyArray<string>) =>
  NodeUtil.parseArgs({
    args: [...argv],
    allowPositionals: true,
    allowNegative: true,
    options: {
      url: { type: "string" },
      out: { type: "string" },
      "token-file": { type: "string" },
      mode: { type: "string", default: "history" },
      sessions: { type: "string", default: "80" },
      subscribers: { type: "string" },
      pollers: { type: "string", default: "20" },
      projects: { type: "string", default: "8" },
      warmup: { type: "string", default: "60s" },
      window: { type: "string", default: "300s" },
      seed: { type: "string", default: "1" },
      provider: { type: "string", default: "claudeAgent" },
      model: { type: "string", default: "claude-sonnet-4-6" },
      "runtime-mode": { type: "string", default: "full-access" },
      "poll-rate": { type: "string", default: "1" },
      "poll-mix": { type: "string", default: "project_list=1,thread_read=1,http_bounded=1" },
      "session-mcp-rate": { type: "string", default: String(PRODUCTION_SESSION_MCP_RATE) },
      "session-mcp-credentials": { type: "string" },
      "switch-interval": { type: "string", default: "30s" },
      "turn-gap": { type: "string", default: "1s" },
      "message-chars": { type: "string", default: "400" },
      "read-pool": { type: "string", default: "200" },
      "distribution-tolerance": { type: "string", default: "0.2" },
      "workspace-dir": { type: "string" },
      "call-timeout": { type: "string", default: "60s" },
      grace: { type: "string", default: "60s" },
      "sessions-per-socket": { type: "string", default: "10" },
      "max-driver-elu": { type: "string", default: "0.7" },
      "max-driver-delay": { type: "string", default: "50ms" },
      "stall-timeout": { type: "string", default: "30s" },
      "setup-concurrency": { type: "string", default: "8" },
      "max-error-rate": { type: "string", default: "0.01" },
      taskset: { type: "string" },
      "server-pid": { type: "string" },
      "stop-sessions": { type: "boolean", default: true },
      help: { type: "boolean", default: false },
    },
  });

export function parseDriverOptions(argv: ReadonlyArray<string>): DriverOptions {
  let parsed: ReturnType<typeof parseFlags>;
  try {
    parsed = parseFlags(argv);
  } catch (error) {
    throw new UsageError(error instanceof Error ? error.message : String(error));
  }
  const { values, positionals } = parsed;
  if (values.help) throw new UsageError("", { help: true });
  if (positionals.length > 0) throw new UsageError(`Unexpected argument: ${positionals[0]}`);
  if (values.url === undefined) throw new UsageError("--url is required");
  if (values.out === undefined) throw new UsageError("--out is required");
  let origin: string;
  try {
    const url = new URL(values.url);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("protocol");
    origin = url.origin;
  } catch {
    throw new UsageError("--url must be an http(s) origin such as http://127.0.0.1:3773");
  }
  if (values.mode !== "fresh" && values.mode !== "history") {
    throw new UsageError("--mode must be fresh or history");
  }
  const sessions = parseCount(values.sessions, "sessions", 0);
  const maxErrorRate = parseFraction(values["max-error-rate"], "max-error-rate");
  const windowMs = parseDuration(values.window, "window");
  if (windowMs <= 0) throw new UsageError("--window must be positive");
  if (values.taskset !== undefined && !/^[\d,-]+$/u.test(values.taskset)) {
    throw new UsageError("--taskset takes a CPU list such as 8-15 or 0,2,4");
  }
  const options: DriverOptions = {
    url: origin,
    out: values.out,
    tokenFile: values["token-file"] ?? null,
    mode: values.mode,
    sessions,
    subscribers: parseCount(values.subscribers ?? String(sessions), "subscribers", 0),
    pollers: parseCount(values.pollers, "pollers", 0),
    projects: parseCount(values.projects, "projects", 1),
    warmupMs: parseDuration(values.warmup, "warmup"),
    windowMs,
    seed: values.seed,
    provider: values.provider,
    model: values.model,
    runtimeMode: values["runtime-mode"],
    pollRate: parsePositive(values["poll-rate"], "poll-rate"),
    pollMix: parsePollMix(values["poll-mix"]),
    sessionMcpRatePerMinute: parseNonNegative(values["session-mcp-rate"], "session-mcp-rate"),
    sessionMcpCredentials: values["session-mcp-credentials"] ?? null,
    switchIntervalMs: parseDuration(values["switch-interval"], "switch-interval"),
    turnGapMs: parseDuration(values["turn-gap"], "turn-gap"),
    messageChars: parseCount(values["message-chars"], "message-chars", 1),
    readPool: parseCount(values["read-pool"], "read-pool", 1),
    distributionTolerance: parseFraction(
      values["distribution-tolerance"],
      "distribution-tolerance",
    ),
    workspaceDir: values["workspace-dir"] ?? null,
    callTimeoutMs: parseDuration(values["call-timeout"], "call-timeout"),
    graceMs: parseDuration(values.grace, "grace"),
    sessionsPerSocket: parseCount(values["sessions-per-socket"], "sessions-per-socket", 1),
    maxDriverElu: parseFraction(values["max-driver-elu"], "max-driver-elu"),
    maxDriverDelayMs: parseDuration(values["max-driver-delay"], "max-driver-delay"),
    stallTimeoutMs: parseDuration(values["stall-timeout"], "stall-timeout"),
    setupConcurrency: parseCount(values["setup-concurrency"], "setup-concurrency", 1),
    maxErrorRate,
    taskset: values.taskset ?? null,
    serverPid:
      values["server-pid"] === undefined ? null : parseCount(values["server-pid"], "server-pid", 1),
    stopSessions: values["stop-sessions"],
  };
  if (
    options.mode === "fresh" &&
    options.sessions === 0 &&
    options.subscribers + options.pollers > 0
  ) {
    throw new UsageError(
      "fresh mode reads the threads its sessions create: --sessions must be > 0",
    );
  }
  return options;
}

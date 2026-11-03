// @effect-diagnostics nodeBuiltinImport:off -- Runs first thing in bin.ts, before the CLI's Effect runtime loads.
import * as NodeFS from "node:fs";
import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import * as NodeSea from "node:sea";
import * as NodeV8 from "node:v8";

import { SERVICE_LAUNCHER_CONTEXT_ENV } from "./cloud/serviceProtocol.ts";

// V8 reads --max-semi-space-size only at startup, so the cap travels in NODE_OPTIONS: a parent that
// starts the server puts it there, and a server started without it relaunches itself once (execve,
// same pid). The server's children get the NODE_OPTIONS the server itself was given.
export const DEFAULT_MAX_SEMI_SPACE_MB = 64;
/** Escape hatch: 0/off disables the default; a positive integer sets the cap in MiB. */
export const MAX_SEMI_SPACE_ENV = "T3CODE_V8_MAX_SEMI_SPACE_MB";
/** The NODE_OPTIONS the server's children must inherit (the value before the flag was added). */
export const INHERITED_NODE_OPTIONS_ENV = "T3CODE_INHERITED_NODE_OPTIONS";

const FLAG = "--max-semi-space-size";
/** The escape hatch accepts caps up to this (MiB); V8 would take far more, which no server needs. */
const MAX_SEMI_SPACE_MB_LIMIT = 1024;
const BOOTSTRAP_FD_ENV = "T3CODE_BOOTSTRAP_FD";
/** Set by the relaunch to the pid it relaunches; execve keeps the pid, so only that image matches it. */
const RELAUNCHED_PID_ENV = "T3CODE_YOUNG_GENERATION_RELAUNCHED_PID";
const DISABLED_VALUES = new Set(["0", "off", "false", "no"]);
export const SERVER_COMMANDS: ReadonlySet<string> = new Set(["serve", "start", "__node-flags"]);
/** Every top-level command that never starts a server; `youngGeneration.test.ts` checks it against the CLI's exported commands. */
export const NON_SERVER_COMMANDS: ReadonlySet<string> = new Set([
  "acp-mcp-bridge",
  "acp-mcp-call",
  "__document-text",
  "__managed-chatgpt-token",
  "__process-spawner",
  "__claude-history",
  "__service-launcher",
  "__service-preflight",
  "__ssh-helper",
  "app",
  "auth",
  "browser",
  "connect",
  "doctor",
  "environment",
  "help",
  "pair",
  "policy",
  "project",
  "service",
  "status",
  "theme",
  "thread",
  "trace",
  "triage",
  "up",
  "update",
  "uninstall",
]);
/** Root flags that print and exit; every other root flag (`--port`, `--no-browser`, `--base-dir`…) runs the server. */
const NON_SERVER_ROOT_FLAGS = new Set([
  "--help",
  "-h",
  "--version",
  "-v",
  "-V",
  "--completions",
  "--wizard",
]);

export type Environment = Readonly<Record<string, string | undefined>>;

/** Splits NODE_OPTIONS as Node does: spaces separate, double quotes group, a backslash inside quotes escapes. */
export const tokenizeNodeOptions = (nodeOptions: string | undefined): ReadonlyArray<string> => {
  if (nodeOptions === undefined) return [];
  const tokens: Array<string> = [];
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
};

/** Node accepts `_` for `-` in option names and a single leading dash; compare on the `--dashed` form. */
const optionName = (arg: string): string => {
  const end = arg.indexOf("=");
  const name = (end === -1 ? arg : arg.slice(0, end)).replaceAll("_", "-");
  return name.startsWith("-") && !name.startsWith("--") ? `-${name}` : name;
};

/** The flag's last value in args (V8 takes the last; `""` for a bare flag without a value); undefined when absent. */
const flagIn = (args: ReadonlyArray<string>): string | undefined => {
  let value: string | undefined;
  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    if (optionName(arg) !== FLAG) continue;
    const end = arg.indexOf("=");
    value = end === -1 ? (args[index + 1] ?? "") : arg.slice(end + 1);
  }
  return value;
};

interface ExplicitFlag {
  readonly present: boolean;
  /** The numeric value, or null when absent or not a number. */
  readonly mb: number | null;
}

const explicitMaxSemiSpace = (env: Environment, execArgv: ReadonlyArray<string>): ExplicitFlag => {
  const value = flagIn(execArgv) ?? flagIn(tokenizeNodeOptions(env.NODE_OPTIONS));
  if (value === undefined) return { present: false, mb: null };
  const parsed = Number(value);
  return { present: true, mb: value !== "" && Number.isFinite(parsed) ? parsed : null };
};

/** The cap to apply, or null when the environment disables it or a flag already sets one. */
export const resolveMaxSemiSpaceMb = (
  env: Environment,
  execArgv: ReadonlyArray<string>,
): number | null => {
  const setting = env[MAX_SEMI_SPACE_ENV]?.trim().toLowerCase() ?? "";
  if (DISABLED_VALUES.has(setting)) return null;
  if (explicitMaxSemiSpace(env, execArgv).present) return null;
  const requested = Number(setting);
  return setting !== "" &&
    Number.isInteger(requested) &&
    requested > 0 &&
    requested <= MAX_SEMI_SPACE_MB_LIMIT
    ? requested
    : DEFAULT_MAX_SEMI_SPACE_MB;
};

export const nodeOptionsWithMaxSemiSpace = (
  nodeOptions: string | undefined,
  mb: number,
): string => {
  const current = nodeOptions?.trim() ?? "";
  return current === "" ? `${FLAG}=${mb}` : `${current} ${FLAG}=${mb}`;
};

/**
 * NODE_OPTIONS as a `ps -E -o command=` line reports it: the command line, then `NAME=value`
 * entries separated by spaces; a value runs until the next `NAME=` token. Undefined when absent.
 */
export const nodeOptionsFromProcessListing = (line: string): string | undefined => {
  const tokens = line.trim().split(" ");
  const start = tokens.findIndex((token) => token.startsWith("NODE_OPTIONS="));
  if (start === -1) return undefined;
  const value = [tokens[start]!.slice("NODE_OPTIONS=".length)];
  for (const token of tokens.slice(start + 1)) {
    if (/^[A-Za-z_][A-Za-z0-9_]*=/.test(token)) break;
    value.push(token);
  }
  return value.join(" ").trim();
};

type StartupNodeOptions =
  | { readonly known: true; readonly value: string | undefined }
  | { readonly known: false };

/**
 * NODE_OPTIONS as Node started with, from the operating system's record of this process — a preload
 * may have changed `process.env` since. Linux keeps it in /proc; macOS answers through `ps -E`.
 */
const startupNodeOptions = (platform: string, pid: number): StartupNodeOptions => {
  try {
    if (platform === "linux") {
      const entry = NodeFS.readFileSync("/proc/self/environ", "latin1")
        .split("\0")
        .find((candidate) => candidate.startsWith("NODE_OPTIONS="));
      return { known: true, value: entry?.slice("NODE_OPTIONS=".length) };
    }
    if (platform === "darwin") {
      const listing = NodeChildProcess.spawnSync(
        "/bin/ps",
        ["-E", "-ww", "-o", "command=", "-p", String(pid)],
        { encoding: "utf8", timeout: 2000 },
      );
      if (listing.status !== 0) return { known: false };
      return { known: true, value: nodeOptionsFromProcessListing(listing.stdout) };
    }
  } catch {
    return { known: false };
  }
  return { known: false };
};

const PRELOAD_FLAGS = new Set(["--require", "-r", "--import", "--loader", "--experimental-loader"]);
/** Whether a preload is visible on the command line or in NODE_OPTIONS as it reads now. */
const preloadVisible = (env: Environment, execArgv: ReadonlyArray<string>): boolean =>
  [...execArgv, ...tokenizeNodeOptions(env.NODE_OPTIONS)].some(
    (token) =>
      PRELOAD_FLAGS.has(token) || [...PRELOAD_FLAGS].some((flag) => token.startsWith(`${flag}=`)),
  );

/**
 * Whether a CommonJS module already ran before this entry: a `--require` preload that removed
 * itself from NODE_OPTIONS still sits in the module cache. (An `--import` preload leaves no trace.)
 */
const preloadRan = (): boolean => {
  try {
    return Object.keys(NodeModule.createRequire(process.execPath).cache).length > 0;
  } catch {
    return false;
  }
};

/** `value` without the `--max-semi-space-size=<mb>` token (the last one), a preload's own additions kept. */
const withoutCapToken = (value: string | undefined, mb: number): string | undefined => {
  if (value === undefined) return undefined;
  const tokens = [...tokenizeNodeOptions(value)];
  const index = tokens.lastIndexOf(`${FLAG}=${mb}`);
  if (index !== -1) tokens.splice(index, 1);
  const rest = tokens.join(" ");
  return rest === "" ? undefined : rest;
};

const withFlag = (env: Environment, mb: number): Environment => ({
  ...env,
  NODE_OPTIONS: nodeOptionsWithMaxSemiSpace(env.NODE_OPTIONS, mb),
  [INHERITED_NODE_OPTIONS_ENV]: env.NODE_OPTIONS ?? "",
});

/** The environment for a server another process starts; unchanged when disabled or already explicit. */
export const serverChildEnvironment = (env: Environment): Environment => {
  const mb = resolveMaxSemiSpaceMb(env, []);
  return mb === null ? env : withFlag(env, mb);
};

const isDirectorySync = (path: string): boolean => {
  try {
    return NodeFS.statSync(path).isDirectory();
  } catch {
    return false;
  }
};

/**
 * Whether argv runs the server in this process: `t3`, `t3 --port 3773`, `t3 ./dir`, `t3 existing-dir`,
 * `serve`, `start`. Help and version flags, and other subcommands, do not.
 */
export const isServerCommand = (
  argv: ReadonlyArray<string>,
  isDirectory: (path: string) => boolean = isDirectorySync,
): boolean => {
  // Global flags may precede the command (`t3 --log-level warn doctor`); classify what follows them.
  let index = 2;
  while (argv[index] === "--log-level" || argv[index]?.startsWith("--log-level=") === true) {
    index += argv[index] === "--log-level" ? 2 : 1;
  }
  const command = argv[index];
  if (command === undefined) return true;
  if (SERVER_COMMANDS.has(command)) return true;
  if (NON_SERVER_COMMANDS.has(command)) return false;
  if (command.startsWith("-")) return !NON_SERVER_ROOT_FLAGS.has(command);
  if (
    command === "." ||
    command === ".." ||
    command === "~" ||
    /[/\\]/.test(command) ||
    /^[a-z]:/i.test(command)
  ) {
    return true;
  }
  return isDirectory(command);
};

const INSPECTOR_FLAGS = [
  "--inspect",
  "--inspect-brk",
  "--inspect-port",
  "--inspect-wait",
  "--debug-port",
];

/** Whether Node was started with an inspector flag on the command line or in NODE_OPTIONS. */
const inspectorRequested = (env: Environment, execArgv: ReadonlyArray<string>): boolean =>
  [...execArgv, ...tokenizeNodeOptions(env.NODE_OPTIONS)].some((arg) =>
    INSPECTOR_FLAGS.includes(optionName(arg)),
  );

const bootstrapFdOf = (argv: ReadonlyArray<string>, env: Environment): string | undefined => {
  for (let index = 2; index < argv.length; index++) {
    const arg = argv[index]!;
    if (arg === "--bootstrap-fd") return argv[index + 1] ?? "";
    if (arg.startsWith("--bootstrap-fd=")) return arg.slice("--bootstrap-fd=".length);
  }
  const fromEnv = env[BOOTSTRAP_FD_ENV]?.trim();
  return fromEnv === undefined || fromEnv === "" ? undefined : fromEnv;
};

export type RelaunchPlan =
  | {
      readonly kind: "relaunch";
      readonly file: string;
      readonly args: ReadonlyArray<string>;
      readonly env: Environment;
    }
  | {
      readonly kind: "keep";
      readonly reason: string;
      /** The server was started over IPC by a service launcher that supplied no flag (an RC2 launcher). */
      readonly launcherPredatesDefault?: true;
    };

export const planYoungGenerationRelaunch = (input: {
  readonly argv: ReadonlyArray<string>;
  readonly execArgv: ReadonlyArray<string>;
  readonly env: Environment;
  readonly execPath: string;
  readonly platform: string;
  readonly hasIpcChannel: boolean;
  readonly isSea: boolean;
  readonly canExecve: boolean;
  readonly inspectorActive?: boolean;
  readonly isDirectory?: (path: string) => boolean;
  readonly pid?: number;
  readonly relaunchedPid?: string;
  /** A preload already ran in this process, whatever NODE_OPTIONS says now. */
  readonly preloadRan?: boolean;
}): RelaunchPlan => {
  // At most one relaunch per process, whatever a preload did to NODE_OPTIONS in the new image.
  if (input.pid !== undefined && input.relaunchedPid === String(input.pid)) {
    return { kind: "keep", reason: "relaunched once already" };
  }
  // The marker counts only with the flag behind it; a stray one must not hide a missing cap.
  if (
    input.env[INHERITED_NODE_OPTIONS_ENV] !== undefined &&
    explicitMaxSemiSpace(input.env, input.execArgv).present
  ) {
    return { kind: "keep", reason: "the young-generation cap is already in effect" };
  }
  if (input.platform === "win32" || !input.canExecve) {
    return { kind: "keep", reason: "process.execve is unavailable on this platform" };
  }
  // execve keeps only stdio: an IPC channel or a bootstrap descriptor would be lost.
  if (input.hasIpcChannel) {
    // A remote update replaces the binary, not the running launcher; one that predates the default
    // supplies no flag, and a server started over IPC cannot relaunch itself. Say so, visibly.
    if (
      input.env[SERVICE_LAUNCHER_CONTEXT_ENV] !== undefined &&
      resolveMaxSemiSpaceMb(input.env, input.execArgv) !== null &&
      !explicitMaxSemiSpace(input.env, input.execArgv).present
    ) {
      return {
        kind: "keep",
        reason:
          "started over IPC by a service launcher that predates the young-generation default; the cap applies once the service restarts",
        launcherPredatesDefault: true,
      };
    }
    return { kind: "keep", reason: "started with an IPC channel" };
  }
  if (bootstrapFdOf(input.argv, input.env) !== undefined) {
    return { kind: "keep", reason: "started with a bootstrap descriptor" };
  }
  // A second image would open a second inspector session and drop the attached debugger.
  if (input.inspectorActive === true || inspectorRequested(input.env, input.execArgv)) {
    return { kind: "keep", reason: "the inspector is active" };
  }
  // A preload runs before this code and may have rewritten NODE_OPTIONS: what it reads now is not
  // what Node started with, so neither an explicit cap nor the user's options can be trusted.
  if (input.preloadRan === true || preloadVisible(input.env, input.execArgv)) {
    return { kind: "keep", reason: "started with a preload" };
  }
  const mb = resolveMaxSemiSpaceMb(input.env, input.execArgv);
  if (mb === null) return { kind: "keep", reason: "disabled or set explicitly" };
  if (!isServerCommand(input.argv, input.isDirectory)) {
    return { kind: "keep", reason: "not a server command" };
  }
  return {
    kind: "relaunch",
    file: input.execPath,
    // A single executable takes only user arguments; Node takes its flags, the script and the arguments.
    args: input.isSea
      ? [input.execPath, ...input.argv.slice(2)]
      : [input.execPath, ...input.execArgv, ...input.argv.slice(1)],
    env: {
      ...withFlag(input.env, mb),
      ...(input.pid === undefined ? {} : { [RELAUNCHED_PID_ENV]: String(input.pid) }),
    },
  };
};

export interface YoungGenerationOutcome {
  /** True when this process runs with the cap from NODE_OPTIONS (relaunched, or started so by a parent). */
  readonly applied: boolean;
  readonly nodeOptionsAtStart: string | undefined;
  readonly keptReason?: string;
  /** Started over IPC by a service launcher that supplied no flag; the cap waits for a service restart. */
  readonly launcherPredatesDefault?: true;
}

let lastOutcome: YoungGenerationOutcome | undefined;

/**
 * Applies the default on the way in: a process whose NODE_OPTIONS already carry the cap restores
 * the inherited NODE_OPTIONS for its children; any other server process relaunches itself with
 * the cap. Returns only when the process goes on without relaunching.
 */
export const applyYoungGenerationDefault = (): YoungGenerationOutcome =>
  (lastOutcome = applyYoungGenerationDefaultUnsafe());

/** Whether an inspector session is open; a Node built without one has no module to ask. */
const inspectorActive = (): boolean => {
  if (process.features.inspector !== true) return false;
  try {
    const inspector = NodeModule.createRequire(process.execPath)("node:inspector") as {
      readonly url: () => string | undefined;
    };
    return inspector.url() !== undefined;
  } catch {
    return false;
  }
};

const applyYoungGenerationDefaultUnsafe = (): YoungGenerationOutcome => {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Runs before the Effect runtime exists.
  const platform = process.platform;
  // What Node started with, not what a preload left behind: an explicit cap or a preload in the
  // startup NODE_OPTIONS decides, a marker is checked against it, and a relaunch carries it on.
  const startup = startupNodeOptions(platform, process.pid);
  const nodeOptionsAtStart = startup.known ? startup.value : process.env.NODE_OPTIONS;
  const inherited = process.env[INHERITED_NODE_OPTIONS_ENV];
  const relaunchedPid = process.env[RELAUNCHED_PID_ENV];
  delete process.env[RELAUNCHED_PID_ENV];
  if (inherited !== undefined) {
    delete process.env[INHERITED_NODE_OPTIONS_ENV];
    const mb = explicitMaxSemiSpace({ NODE_OPTIONS: nodeOptionsAtStart }, process.execArgv).mb;
    // Honour the marker only when the process started with exactly the marker plus the cap — what
    // the relaunch or the launcher wrote; a stray or forged marker is ignored. Children then get
    // NODE_OPTIONS as it reads now minus that cap, so a preload's own additions stay.
    if (mb !== null && nodeOptionsAtStart === nodeOptionsWithMaxSemiSpace(inherited, mb)) {
      const forChildren = withoutCapToken(process.env.NODE_OPTIONS, mb);
      if (forChildren === undefined) delete process.env.NODE_OPTIONS;
      else process.env.NODE_OPTIONS = forChildren;
      return { applied: true, nodeOptionsAtStart };
    }
  }
  if (!startup.known) {
    return { applied: false, nodeOptionsAtStart, keptReason: "startup options unavailable" };
  }
  const { NODE_OPTIONS: _current, ...rest } = process.env;
  const startupEnv: Environment =
    startup.value === undefined ? rest : { ...rest, NODE_OPTIONS: startup.value };
  const plan = planYoungGenerationRelaunch({
    argv: process.argv,
    execArgv: process.execArgv,
    env: startupEnv,
    execPath: process.execPath,
    platform,
    hasIpcChannel: process.channel !== undefined,
    isSea: NodeSea.isSea(),
    canExecve: typeof process.execve === "function",
    inspectorActive: inspectorActive(),
    pid: process.pid,
    ...(relaunchedPid === undefined ? {} : { relaunchedPid }),
    preloadRan: preloadRan(),
  });
  if (plan.kind === "keep") {
    return plan.launcherPredatesDefault === true
      ? {
          applied: false,
          nodeOptionsAtStart,
          keptReason: plan.reason,
          launcherPredatesDefault: true,
        }
      : { applied: false, nodeOptionsAtStart, keptReason: plan.reason };
  }
  const execve = process.execve;
  if (execve === undefined) {
    return { applied: false, nodeOptionsAtStart, keptReason: "process.execve is unavailable" };
  }
  try {
    execve(plan.file, [...plan.args], { ...plan.env });
  } catch (error) {
    process.stderr.write(
      `t3: running without ${FLAG}: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return { applied: false, nodeOptionsAtStart, keptReason: "relaunch failed" };
  }
  return { applied: false, nodeOptionsAtStart, keptReason: "relaunch returned" };
};

export interface NodeRuntimeFlags {
  /** Started over IPC by a service launcher that supplied no flag; the cap waits for a service restart. */
  readonly launcherPredatesDefault?: true;
  readonly execArgv: ReadonlyArray<string>;
  /** NODE_OPTIONS as V8 saw it at startup (the server's children see `nodeOptionsForChildren`). */
  readonly nodeOptions: string | null;
  readonly nodeOptionsForChildren: string | null;
  readonly heapSizeLimitMiB: number;
  /** The young-generation cap in effect: the flag's value, or Node's default when none was given. */
  readonly maxSemiSpaceMiB: number | "node default";
  readonly youngGenerationDefault: boolean;
}

export const describeNodeRuntime = (input: {
  readonly execArgv: ReadonlyArray<string>;
  readonly nodeOptionsAtStart: string | undefined;
  readonly nodeOptionsForChildren: string | undefined;
  readonly heapSizeLimitBytes: number;
  readonly applied: boolean;
}): NodeRuntimeFlags => ({
  execArgv: input.execArgv,
  nodeOptions: input.nodeOptionsAtStart ?? null,
  nodeOptionsForChildren: input.nodeOptionsForChildren ?? null,
  heapSizeLimitMiB: Math.round(input.heapSizeLimitBytes / 1_048_576),
  maxSemiSpaceMiB:
    explicitMaxSemiSpace({ NODE_OPTIONS: input.nodeOptionsAtStart }, input.execArgv).mb ??
    "node default",
  youngGenerationDefault: input.applied,
});

/** What this process runs with, for the startup log: parsed sizes only, never NODE_OPTIONS verbatim. */
export const nodeRuntimeFlags = (): Omit<
  NodeRuntimeFlags,
  "nodeOptions" | "nodeOptionsForChildren"
> => {
  const {
    nodeOptions: _nodeOptions,
    nodeOptionsForChildren: _forChildren,
    ...sizes
  } = describeNodeRuntime({
    execArgv: process.execArgv,
    nodeOptionsAtStart: lastOutcome?.nodeOptionsAtStart ?? process.env.NODE_OPTIONS,
    nodeOptionsForChildren: process.env.NODE_OPTIONS,
    heapSizeLimitBytes: NodeV8.getHeapStatistics().heap_size_limit,
    applied: lastOutcome?.applied ?? false,
  });
  return {
    ...sizes,
    ...(lastOutcome?.launcherPredatesDefault === undefined
      ? {}
      : { launcherPredatesDefault: true }),
  };
};

const isOpenDescriptor = (fd: string): boolean => {
  const parsed = Number(fd);
  if (!Number.isInteger(parsed) || parsed < 0) return false;
  try {
    NodeFS.fstatSync(parsed);
    return true;
  } catch {
    return false;
  }
};

/** `t3 __node-flags`: the young-generation settings this process runs with, as JSON on stdout. */
export const printNodeFlags = (outcome: YoungGenerationOutcome): void => {
  const bootstrapFd = bootstrapFdOf(process.argv, process.env);
  const report = {
    applied: outcome.applied,
    maxSemiSpaceMb: explicitMaxSemiSpace(
      { NODE_OPTIONS: outcome.nodeOptionsAtStart },
      process.execArgv,
    ).mb,
    nodeOptionsAtStart: outcome.nodeOptionsAtStart ?? null,
    nodeOptionsForChildren: process.env.NODE_OPTIONS ?? null,
    execArgv: process.execArgv,
    ipc: process.channel !== undefined,
    sea: NodeSea.isSea(),
    pid: process.pid,
    ...(outcome.keptReason === undefined ? {} : { keptReason: outcome.keptReason }),
    ...(outcome.launcherPredatesDefault === undefined ? {} : { launcherPredatesDefault: true }),
    ...(bootstrapFd === undefined ? {} : { bootstrapFdOpen: isOpenDescriptor(bootstrapFd) }),
  };
  process.stdout.write(`${JSON.stringify(report)}\n`);
  if (process.channel !== undefined) process.disconnect();
};

// @effect-diagnostics nodeBuiltinImport:off -- Runs first thing in bin.ts, before the CLI's Effect runtime loads.
import * as NodeFS from "node:fs";
import * as NodeInspector from "node:inspector";
import * as NodeSea from "node:sea";
import * as NodeV8 from "node:v8";

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
const DISABLED_VALUES = new Set(["0", "off", "false", "no"]);
const SERVER_COMMANDS = new Set(["serve", "start", "__node-flags"]);
/** Subcommands and fast paths that never run the server here: no file-system check for them. */
const NON_SERVER_COMMANDS = new Set([
  "acp-mcp-bridge",
  "acp-mcp-call",
  "__document-text",
  "__managed-chatgpt-token",
  "__process-spawner",
  "__claude-history",
  "__service-launcher",
  "auth",
  "connect",
  "doctor",
  "project",
  "service",
  "thread",
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
  const command = argv[2];
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
  | { readonly kind: "keep"; readonly reason: string };

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
}): RelaunchPlan => {
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
  if (input.hasIpcChannel) return { kind: "keep", reason: "started with an IPC channel" };
  if (bootstrapFdOf(input.argv, input.env) !== undefined) {
    return { kind: "keep", reason: "started with a bootstrap descriptor" };
  }
  // A second image would open a second inspector session and drop the attached debugger.
  if (input.inspectorActive === true || inspectorRequested(input.env, input.execArgv)) {
    return { kind: "keep", reason: "the inspector is active" };
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
    env: withFlag(input.env, mb),
  };
};

export interface YoungGenerationOutcome {
  /** True when this process runs with the cap from NODE_OPTIONS (relaunched, or started so by a parent). */
  readonly applied: boolean;
  readonly nodeOptionsAtStart: string | undefined;
  readonly keptReason?: string;
}

let lastOutcome: YoungGenerationOutcome | undefined;

/**
 * Applies the default on the way in: a process whose NODE_OPTIONS already carry the cap restores
 * the inherited NODE_OPTIONS for its children; any other server process relaunches itself with
 * the cap. Returns only when the process goes on without relaunching.
 */
export const applyYoungGenerationDefault = (): YoungGenerationOutcome =>
  (lastOutcome = applyYoungGenerationDefaultUnsafe());

const applyYoungGenerationDefaultUnsafe = (): YoungGenerationOutcome => {
  const nodeOptionsAtStart = process.env.NODE_OPTIONS;
  const inherited = process.env[INHERITED_NODE_OPTIONS_ENV];
  if (inherited !== undefined) {
    delete process.env[INHERITED_NODE_OPTIONS_ENV];
    if (explicitMaxSemiSpace({ NODE_OPTIONS: nodeOptionsAtStart }, process.execArgv).present) {
      if (inherited === "") delete process.env.NODE_OPTIONS;
      else process.env.NODE_OPTIONS = inherited;
      return { applied: true, nodeOptionsAtStart };
    }
  }
  const plan = planYoungGenerationRelaunch({
    argv: process.argv,
    execArgv: process.execArgv,
    env: process.env,
    execPath: process.execPath,
    // oxlint-disable-next-line t3code/no-global-process-runtime -- Runs before the Effect runtime exists.
    platform: process.platform,
    hasIpcChannel: process.channel !== undefined,
    isSea: NodeSea.isSea(),
    canExecve: typeof process.execve === "function",
    inspectorActive: NodeInspector.url() !== undefined,
  });
  if (plan.kind === "keep") return { applied: false, nodeOptionsAtStart, keptReason: plan.reason };
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
  return sizes;
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
    ...(bootstrapFd === undefined ? {} : { bootstrapFdOpen: isOpenDescriptor(bootstrapFd) }),
  };
  process.stdout.write(`${JSON.stringify(report)}\n`);
  if (process.channel !== undefined) process.disconnect();
};

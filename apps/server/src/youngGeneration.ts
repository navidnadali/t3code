// @effect-diagnostics nodeBuiltinImport:off -- Runs first thing in bin.ts, before the CLI's Effect runtime loads.
import * as NodeFS from "node:fs";
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

/** One argument of NODE_OPTIONS: its text as Node reads it, and where it sits in the string, quotes included. */
interface NodeOptionSpan {
  readonly start: number;
  readonly end: number;
  readonly text: string;
}

/** Splits NODE_OPTIONS as Node does: spaces separate, double quotes group, a backslash inside quotes escapes. */
const nodeOptionSpans = (nodeOptions: string): ReadonlyArray<NodeOptionSpan> => {
  const spans: Array<NodeOptionSpan> = [];
  let text = "";
  let start = -1;
  let quoted = false;
  const close = (end: number): void => {
    if (start !== -1) spans.push({ start, end, text });
    text = "";
    start = -1;
  };
  for (let index = 0; index < nodeOptions.length; index++) {
    let char = nodeOptions[index]!;
    if (char === "\\" && quoted && index + 1 < nodeOptions.length) {
      if (start === -1) start = index;
      char = nodeOptions[++index]!;
    } else if (char === " " && !quoted) {
      close(index);
      continue;
    } else if (char === '"') {
      if (start === -1) start = index;
      quoted = !quoted;
      continue;
    }
    if (start === -1) start = index;
    text += char;
  }
  close(nodeOptions.length);
  return spans;
};

export const tokenizeNodeOptions = (nodeOptions: string | undefined): ReadonlyArray<string> =>
  nodeOptions === undefined ? [] : nodeOptionSpans(nodeOptions).map((span) => span.text);

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

export type StartupNodeOptions =
  | { readonly known: true; readonly value: string | undefined }
  | { readonly known: false };

/** NODE_OPTIONS out of a /proc environ block: UTF-8 like `process.env`, entries separated by NUL. */
export const nodeOptionsFromEnviron = (block: Uint8Array): string | undefined =>
  new TextDecoder("utf-8")
    .decode(block)
    .split("\0")
    .find((entry) => entry.startsWith("NODE_OPTIONS="))
    ?.slice("NODE_OPTIONS=".length);

/**
 * NODE_OPTIONS as Node started with, from the operating system's record of this process — a preload
 * may have changed `process.env` since. Only Linux keeps an exact record (/proc, 13 µs to read);
 * elsewhere the relaunch rules decide without one.
 */
const startupNodeOptions = (platform: string): StartupNodeOptions => {
  if (platform !== "linux") return { known: false };
  try {
    return {
      known: true,
      value: nodeOptionsFromEnviron(NodeFS.readFileSync("/proc/self/environ")),
    };
  } catch {
    return { known: false };
  }
};

/** The preload options on their `--dashed` form (`optionName`), `--r` standing for Node's `-r`. */
const PRELOAD_OPTIONS = new Set([
  "--require",
  "--r",
  "--import",
  "--loader",
  "--experimental-loader",
]);
/** Whether a preload is visible on the command line or in NODE_OPTIONS as it reads now, in any spelling. */
const preloadVisible = (env: Environment, execArgv: ReadonlyArray<string>): boolean =>
  [...execArgv, ...tokenizeNodeOptions(env.NODE_OPTIONS)].some((token) =>
    PRELOAD_OPTIONS.has(optionName(token)),
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

/**
 * `value` without its last `--max-semi-space-size=<mb>` argument — an argument as Node reads it, so
 * the flag's text inside a quoted value is never touched — cut out of the string so the rest keeps
 * its quoting exactly (a preload's own additions included); undefined when nothing remains.
 */
const withoutCapArg = (value: string | undefined, mb: number): string | undefined => {
  if (value === undefined) return undefined;
  const spans = nodeOptionSpans(value);
  let cap: NodeOptionSpan | undefined;
  for (const span of spans) if (span.text === `${FLAG}=${mb}`) cap = span;
  const rest =
    cap === undefined
      ? value
      : [value.slice(0, cap.start).trimEnd(), value.slice(cap.end).trimStart()]
          .filter((part) => part !== "")
          .join(" ");
  const trimmed = rest.trim();
  return trimmed === "" ? undefined : trimmed;
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

/** A process as the decision sees it: `process` read once, plus the operating system's startup record. */
export interface YoungGenerationProcess {
  readonly argv: ReadonlyArray<string>;
  readonly execArgv: ReadonlyArray<string>;
  /** The environment as it reads now, marker and pid guard included. */
  readonly env: Environment;
  readonly execPath: string;
  readonly platform: string;
  readonly pid: number;
  readonly hasIpcChannel: boolean;
  readonly isSea: boolean;
  readonly canExecve: boolean;
  readonly inspectorActive: boolean;
  readonly preloadRan: boolean;
  readonly startup: StartupNodeOptions;
  readonly isDirectory?: (path: string) => boolean;
}

export type YoungGenerationDecision =
  | {
      readonly kind: "applied";
      readonly outcome: YoungGenerationOutcome;
      /** NODE_OPTIONS for the server's children: the value in effect minus the cap added for the server. */
      readonly nodeOptionsForChildren: string | undefined;
    }
  | { readonly kind: "keep"; readonly outcome: YoungGenerationOutcome }
  | {
      readonly kind: "relaunch";
      readonly plan: Extract<RelaunchPlan, { readonly kind: "relaunch" }>;
      /** The outcome should the relaunch return instead of replacing the image. */
      readonly outcome: YoungGenerationOutcome;
    };

/**
 * The whole decision, free of side effects: honour a marker, keep when a known startup record
 * differs from the value in effect (a preload rewrote it), else the relaunch rules.
 */
export const decideYoungGeneration = (view: YoungGenerationProcess): YoungGenerationDecision => {
  const {
    [INHERITED_NODE_OPTIONS_ENV]: inherited,
    [RELAUNCHED_PID_ENV]: relaunchedPid,
    ...env
  } = view.env;
  const current = env.NODE_OPTIONS;
  const { startup } = view;
  // What V8 started with, for the outcome: the record when known, else the value in effect.
  const nodeOptionsAtStart = startup.known ? startup.value : current;
  if (inherited !== undefined) {
    const mb = explicitMaxSemiSpace({ NODE_OPTIONS: nodeOptionsAtStart }, view.execArgv).mb;
    const written = mb === null ? undefined : nodeOptionsWithMaxSemiSpace(inherited, mb);
    // Honour the marker only for a process that started with exactly the marker plus the cap — what
    // the relaunch or the launcher wrote, seen in the startup record or still leading the value in
    // effect; a stray or forged marker is ignored. Children then get the value in effect minus that
    // one argument, cut out of the string, so a preload's additions and the user's quoting survive.
    const wrote =
      written !== undefined &&
      ((startup.known && startup.value === written) ||
        current === written ||
        current?.startsWith(`${written} `) === true);
    if (mb !== null && wrote) {
      return {
        kind: "applied",
        outcome: { applied: true, nodeOptionsAtStart },
        nodeOptionsForChildren: withoutCapArg(current, mb),
      };
    }
  }
  const keep = (
    keptReason: string,
    extra: { readonly launcherPredatesDefault?: true } = {},
  ): YoungGenerationDecision => ({
    kind: "keep",
    outcome: { applied: false, nodeOptionsAtStart, keptReason, ...extra },
  });
  // A record that differs from the value in effect means a preload rewrote NODE_OPTIONS: what it
  // left cannot be carried into a relaunch. Without a record, the rules below decide.
  if (startup.known && startup.value !== current) return keep("NODE_OPTIONS changed since startup");
  const plan = planYoungGenerationRelaunch({
    argv: view.argv,
    execArgv: view.execArgv,
    env,
    execPath: view.execPath,
    platform: view.platform,
    hasIpcChannel: view.hasIpcChannel,
    isSea: view.isSea,
    canExecve: view.canExecve,
    inspectorActive: view.inspectorActive,
    pid: view.pid,
    ...(relaunchedPid === undefined ? {} : { relaunchedPid }),
    preloadRan: view.preloadRan,
    ...(view.isDirectory === undefined ? {} : { isDirectory: view.isDirectory }),
  });
  if (plan.kind === "keep") {
    return keep(
      plan.reason,
      plan.launcherPredatesDefault === undefined ? {} : { launcherPredatesDefault: true },
    );
  }
  return {
    kind: "relaunch",
    plan,
    outcome: { applied: false, nodeOptionsAtStart, keptReason: "relaunch returned" },
  };
};

const applyYoungGenerationDefaultUnsafe = (): YoungGenerationOutcome => {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- Runs before the Effect runtime exists.
  const platform = process.platform;
  const view: YoungGenerationProcess = {
    argv: process.argv,
    execArgv: process.execArgv,
    env: { ...process.env },
    execPath: process.execPath,
    platform,
    pid: process.pid,
    hasIpcChannel: process.channel !== undefined,
    isSea: NodeSea.isSea(),
    canExecve: typeof process.execve === "function",
    inspectorActive: inspectorActive(),
    preloadRan: preloadRan(),
    // Only a server command asks: helpers run per tool call never relaunch.
    startup: isServerCommand(process.argv) ? startupNodeOptions(platform) : { known: false },
  };
  // Neither travels to children or into a second image on its own.
  delete process.env[INHERITED_NODE_OPTIONS_ENV];
  delete process.env[RELAUNCHED_PID_ENV];
  const decision = decideYoungGeneration(view);
  if (decision.kind === "applied") {
    if (decision.nodeOptionsForChildren === undefined) delete process.env.NODE_OPTIONS;
    else process.env.NODE_OPTIONS = decision.nodeOptionsForChildren;
    return decision.outcome;
  }
  if (decision.kind === "keep") return decision.outcome;
  const execve = process.execve;
  if (execve === undefined) {
    return { ...decision.outcome, keptReason: "process.execve is unavailable" };
  }
  try {
    execve(decision.plan.file, [...decision.plan.args], { ...decision.plan.env });
  } catch (error) {
    process.stderr.write(
      `t3: running without ${FLAG}: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return { ...decision.outcome, keptReason: "relaunch failed" };
  }
  return decision.outcome;
};

export interface NodeRuntimeFlags {
  /** Started over IPC by a service launcher that supplied no flag; the cap waits for a service restart. */
  readonly launcherPredatesDefault?: true;
  /** The process wanted the cap (neither disabled nor set explicitly) and runs without it; `keptReason` says why. */
  readonly youngGenerationLost?: true;
  readonly keptReason?: string;
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
  /** The escape hatch's value, to tell a cap nobody wanted from one that was lost. */
  readonly maxSemiSpaceSetting?: string | undefined;
  readonly keptReason?: string | undefined;
}): NodeRuntimeFlags => {
  const wanted =
    resolveMaxSemiSpaceMb(
      { NODE_OPTIONS: input.nodeOptionsAtStart, [MAX_SEMI_SPACE_ENV]: input.maxSemiSpaceSetting },
      input.execArgv,
    ) !== null;
  return {
    execArgv: input.execArgv,
    nodeOptions: input.nodeOptionsAtStart ?? null,
    nodeOptionsForChildren: input.nodeOptionsForChildren ?? null,
    heapSizeLimitMiB: Math.round(input.heapSizeLimitBytes / 1_048_576),
    maxSemiSpaceMiB:
      explicitMaxSemiSpace({ NODE_OPTIONS: input.nodeOptionsAtStart }, input.execArgv).mb ??
      "node default",
    youngGenerationDefault: input.applied,
    ...(input.applied || !wanted ? {} : { youngGenerationLost: true }),
    ...(input.keptReason === undefined ? {} : { keptReason: input.keptReason }),
  };
};

/** The startup log's sizes for an outcome and environment: parsed sizes only, never NODE_OPTIONS verbatim. */
export const runtimeFlagsOf = (
  outcome: YoungGenerationOutcome | undefined,
  env: Environment,
): Omit<NodeRuntimeFlags, "nodeOptions" | "nodeOptionsForChildren"> => {
  const {
    nodeOptions: _nodeOptions,
    nodeOptionsForChildren: _forChildren,
    ...sizes
  } = describeNodeRuntime({
    execArgv: process.execArgv,
    // A known-absent startup value stays absent: the environment now may hold what a preload added.
    nodeOptionsAtStart: outcome === undefined ? env.NODE_OPTIONS : outcome.nodeOptionsAtStart,
    nodeOptionsForChildren: env.NODE_OPTIONS,
    heapSizeLimitBytes: NodeV8.getHeapStatistics().heap_size_limit,
    applied: outcome?.applied ?? false,
    maxSemiSpaceSetting: env[MAX_SEMI_SPACE_ENV],
    keptReason: outcome?.keptReason,
  });
  return {
    ...sizes,
    ...(outcome?.launcherPredatesDefault === undefined ? {} : { launcherPredatesDefault: true }),
  };
};

/** What this process runs with, for the startup log: parsed sizes only, never NODE_OPTIONS verbatim. */
export const nodeRuntimeFlags = (): Omit<
  NodeRuntimeFlags,
  "nodeOptions" | "nodeOptionsForChildren"
> => runtimeFlagsOf(lastOutcome, process.env);

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

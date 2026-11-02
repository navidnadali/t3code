// @effect-diagnostics nodeBuiltinImport:off -- Runs first thing in bin.ts, before the CLI's Effect runtime loads.
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
const DISABLED_VALUES = new Set(["0", "off", "false", "no"]);
const SERVER_COMMANDS = new Set(["serve", "start", "__node-flags"]);

export type Environment = Readonly<Record<string, string | undefined>>;

const words = (nodeOptions: string | undefined): ReadonlyArray<string> =>
  nodeOptions === undefined
    ? []
    : nodeOptions
        .trim()
        .split(/\s+/)
        .filter((word) => word !== "");

const flagIn = (args: ReadonlyArray<string>): string | undefined => {
  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    if (arg.startsWith(`${FLAG}=`)) return arg.slice(FLAG.length + 1);
    if (arg === FLAG) return args[index + 1] ?? "";
  }
  return undefined;
};

/** The cap (MiB) an explicit `--max-semi-space-size` in NODE_OPTIONS or execArgv sets, if any. */
const explicitMaxSemiSpaceMb = (
  env: Environment,
  execArgv: ReadonlyArray<string>,
): number | null => {
  const value = flagIn(execArgv) ?? flagIn(words(env.NODE_OPTIONS));
  if (value === undefined) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

/** The cap to apply, or null when the environment disables it or a flag already sets one. */
export const resolveMaxSemiSpaceMb = (
  env: Environment,
  execArgv: ReadonlyArray<string>,
): number | null => {
  const setting = env[MAX_SEMI_SPACE_ENV]?.trim().toLowerCase() ?? "";
  if (DISABLED_VALUES.has(setting)) return null;
  if (explicitMaxSemiSpaceMb(env, execArgv) !== null) return null;
  const requested = Number(setting);
  return setting !== "" && Number.isInteger(requested) && requested > 0
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

/** Whether argv names a command that runs the server in this process (`t3`, `t3 ./dir`, `serve`, `start`). */
export const isServerCommand = (argv: ReadonlyArray<string>): boolean => {
  const command = argv[2];
  if (command === undefined) return true;
  if (SERVER_COMMANDS.has(command)) return true;
  if (command.startsWith("-")) return false;
  return (
    command === "." ||
    command === ".." ||
    command === "~" ||
    /[/\\]/.test(command) ||
    /^[a-z]:/i.test(command)
  );
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
}): RelaunchPlan => {
  if (input.env[INHERITED_NODE_OPTIONS_ENV] !== undefined) {
    return { kind: "keep", reason: "the young-generation cap is already in effect" };
  }
  if (!isServerCommand(input.argv)) return { kind: "keep", reason: "not a server command" };
  if (input.platform === "win32" || !input.canExecve) {
    return { kind: "keep", reason: "process.execve is unavailable on this platform" };
  }
  // execve keeps only stdio: an IPC channel or a bootstrap descriptor would be lost.
  if (input.hasIpcChannel) return { kind: "keep", reason: "started with an IPC channel" };
  if (input.argv.some((arg) => arg === "--bootstrap-fd" || arg.startsWith("--bootstrap-fd="))) {
    return { kind: "keep", reason: "started with a bootstrap descriptor" };
  }
  const mb = resolveMaxSemiSpaceMb(input.env, input.execArgv);
  if (mb === null) return { kind: "keep", reason: "disabled or set explicitly" };
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

/**
 * Applies the default on the way in: a process whose NODE_OPTIONS already carry the cap restores
 * the inherited NODE_OPTIONS for its children; any other server process relaunches itself with
 * the cap. Returns only when the process goes on without relaunching.
 */
let lastOutcome: YoungGenerationOutcome | undefined;

export const applyYoungGenerationDefault = (): YoungGenerationOutcome =>
  (lastOutcome = applyYoungGenerationDefaultUnsafe());

const applyYoungGenerationDefaultUnsafe = (): YoungGenerationOutcome => {
  const nodeOptionsAtStart = process.env.NODE_OPTIONS;
  const inherited = process.env[INHERITED_NODE_OPTIONS_ENV];
  if (inherited !== undefined) {
    if (inherited === "") delete process.env.NODE_OPTIONS;
    else process.env.NODE_OPTIONS = inherited;
    delete process.env[INHERITED_NODE_OPTIONS_ENV];
    return { applied: true, nodeOptionsAtStart };
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
    explicitMaxSemiSpaceMb({ NODE_OPTIONS: input.nodeOptionsAtStart }, input.execArgv) ??
    "node default",
  youngGenerationDefault: input.applied,
});

/** What this process runs with, for the startup log; the outcome comes from bin.ts's call. */
export const nodeRuntimeFlags = (): NodeRuntimeFlags =>
  describeNodeRuntime({
    execArgv: process.execArgv,
    nodeOptionsAtStart: lastOutcome?.nodeOptionsAtStart ?? process.env.NODE_OPTIONS,
    nodeOptionsForChildren: process.env.NODE_OPTIONS,
    heapSizeLimitBytes: NodeV8.getHeapStatistics().heap_size_limit,
    applied: lastOutcome?.applied ?? false,
  });

/** `t3 __node-flags`: the young-generation settings this process runs with, as JSON on stdout. */
export const printNodeFlags = (outcome: YoungGenerationOutcome): void => {
  const report = {
    applied: outcome.applied,
    maxSemiSpaceMb: explicitMaxSemiSpaceMb(
      { NODE_OPTIONS: outcome.nodeOptionsAtStart },
      process.execArgv,
    ),
    nodeOptionsAtStart: outcome.nodeOptionsAtStart ?? null,
    nodeOptionsForChildren: process.env.NODE_OPTIONS ?? null,
    execArgv: process.execArgv,
    ipc: process.channel !== undefined,
    sea: NodeSea.isSea(),
    pid: process.pid,
    ...(outcome.keptReason === undefined ? {} : { keptReason: outcome.keptReason }),
  };
  process.stdout.write(`${JSON.stringify(report)}\n`);
  if (process.channel !== undefined) process.disconnect();
};

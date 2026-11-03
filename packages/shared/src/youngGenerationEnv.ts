/**
 * The young-generation cap's environment contract, shared by the server, its service launcher and
 * the desktop app: the flag, the escape hatch, the marker that restores children's NODE_OPTIONS,
 * and the NODE_OPTIONS tokeniser Node's own rules define. No runtime dependencies.
 */

export const DEFAULT_MAX_SEMI_SPACE_MB = 64;

/** Escape hatch: 0/off disables the default; a positive integer sets the cap in MiB. */
export const MAX_SEMI_SPACE_ENV = "T3CODE_V8_MAX_SEMI_SPACE_MB";

/** The NODE_OPTIONS the server's children must inherit (the value before the flag was added). */
export const INHERITED_NODE_OPTIONS_ENV = "T3CODE_INHERITED_NODE_OPTIONS";

export const MAX_SEMI_SPACE_FLAG = "--max-semi-space-size";

/** The escape hatch accepts caps up to this (MiB); V8 would take far more, which no server needs. */
const MAX_SEMI_SPACE_MB_LIMIT = 1024;

const DISABLED_VALUES = new Set(["0", "off", "false", "no"]);

export type Environment = Readonly<Record<string, string | undefined>>;

/** One argument of NODE_OPTIONS: its text as Node reads it, and where it sits in the string, quotes included. */
export interface NodeOptionSpan {
  readonly start: number;
  readonly end: number;
  readonly text: string;
}

/** Splits NODE_OPTIONS as Node does: spaces separate, double quotes group, a backslash inside quotes escapes. */
export const nodeOptionSpans = (nodeOptions: string): ReadonlyArray<NodeOptionSpan> => {
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
export const optionName = (arg: string): string => {
  const end = arg.indexOf("=");
  const name = (end === -1 ? arg : arg.slice(0, end)).replaceAll("_", "-");
  return name.startsWith("-") && !name.startsWith("--") ? `-${name}` : name;
};

/** The flag's last value in args (V8 takes the last; `""` for a bare flag without a value); undefined when absent. */
export const flagIn = (args: ReadonlyArray<string>): string | undefined => {
  let value: string | undefined;
  for (let index = 0; index < args.length; index++) {
    const arg = args[index]!;
    if (optionName(arg) !== MAX_SEMI_SPACE_FLAG) continue;
    const end = arg.indexOf("=");
    value = end === -1 ? (args[index + 1] ?? "") : arg.slice(end + 1);
  }
  return value;
};

export interface ExplicitFlag {
  readonly present: boolean;
  /** The numeric value, or null when absent or not a number. */
  readonly mb: number | null;
}

export const explicitMaxSemiSpace = (
  env: Environment,
  execArgv: ReadonlyArray<string>,
): ExplicitFlag => {
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
  return current === ""
    ? `${MAX_SEMI_SPACE_FLAG}=${mb}`
    : `${current} ${MAX_SEMI_SPACE_FLAG}=${mb}`;
};

/** The cap the environment asks for: 0 when disabled, the knob's value, else the default. */
export const capSetting = (env: Environment): number => {
  const setting = env[MAX_SEMI_SPACE_ENV]?.trim().toLowerCase() ?? "";
  if (DISABLED_VALUES.has(setting)) return 0;
  const requested = Number(setting);
  return setting !== "" &&
    Number.isInteger(requested) &&
    requested > 0 &&
    requested <= MAX_SEMI_SPACE_MB_LIMIT
    ? requested
    : DEFAULT_MAX_SEMI_SPACE_MB;
};

export const withFlag = (env: Environment, mb: number): Environment => ({
  ...env,
  NODE_OPTIONS: nodeOptionsWithMaxSemiSpace(env.NODE_OPTIONS, mb),
  [INHERITED_NODE_OPTIONS_ENV]: env.NODE_OPTIONS ?? "",
});

/** The environment for a server another process starts; unchanged when disabled or already explicit. */
export const serverChildEnvironment = (env: Environment): Environment => {
  const mb = resolveMaxSemiSpaceMb(env, []);
  return mb === null ? env : withFlag(env, mb);
};

/**
 * The single executable's baked Node options: the young-generation default, so a bare start runs
 * with it and never relaunches. The server reads the same value back from `execArgv`.
 */
export const SEA_EXEC_ARGV: ReadonlyArray<string> = [
  `${MAX_SEMI_SPACE_FLAG}=${DEFAULT_MAX_SEMI_SPACE_MB}`,
];

/**
 * Node applies NODE_OPTIONS before the baked execArgv, so a baked cap could never be changed from
 * the environment; "cli" lets `--node-options` extend and override it. It also makes Node ignore
 * NODE_OPTIONS, which the server then carries onto its own command line.
 */
export const SEA_EXEC_ARGV_EXTENSION = "cli";

/** Beside the executable in a runtime archive: tells a launcher to pass Node options on the command line. */
export const RUNTIME_EXEC_ARGV_FILE = "exec-argv.json";

/** How a single executable is built for a platform ("win" or "win32" for Windows). */
export interface SeaBuildOptions {
  readonly execArgv: ReadonlyArray<string>;
  readonly execArgvExtension: "cli" | "env" | "none";
}

/**
 * Windows has no `execve` to carry NODE_OPTIONS onto the command line, so a baked cap there could
 * never be overridden and NODE_OPTIONS would be lost: nothing is baked and the executable reads
 * NODE_OPTIONS itself, as before (the service launcher passes the cap). Everywhere else the default
 * is baked and options are taken on the command line.
 */
export const seaBuildOptions = (platform: string): SeaBuildOptions =>
  platform === "win" || platform === "win32"
    ? { execArgv: [], execArgvExtension: "env" }
    : { execArgv: SEA_EXEC_ARGV, execArgvExtension: SEA_EXEC_ARGV_EXTENSION };

/**
 * The cap as a node flag for a process started with the environment's NODE_OPTIONS in force:
 * nothing when NODE_OPTIONS sets one or the hatch disables it, else the knob's value or the default.
 */
export const nodeCapFlag = (env: Environment): string | undefined => {
  if (flagIn(tokenizeNodeOptions(env.NODE_OPTIONS)) !== undefined) return undefined;
  const wanted = capSetting(env);
  return wanted === 0 ? undefined : `${MAX_SEMI_SPACE_FLAG}=${wanted}`;
};

/** One argument as NODE_OPTIONS text: double quotes around spaces and quotes, which Node unquotes. */
export const quoteNodeOption = (token: string): string =>
  /[\s"]/.test(token) ? `"${token.replaceAll(/(["\\])/g, "\\$1")}"` : token;

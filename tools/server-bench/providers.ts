#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off globalConsole:off preferSchemaOverJson:off - Standalone bench tooling outside the Effect runtime.
/**
 * Bench provider instances and the session mix. Scratch settings register one replaying instance
 * per provider kind, with the fakes' load knobs in its environment (`providers.ts settings`). A
 * load driver adds `PROVIDER_LOAD_FLAGS` and starts session i on `planSessions(...)[i]`.
 */
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as NodeUtil from "node:util";

export const PROVIDER_KINDS = ["claude", "codex", "acp"] as const;
export type ProviderKind = (typeof PROVIDER_KINDS)[number];

export interface BenchProvider {
  readonly kind: ProviderKind;
  readonly instanceId: string;
  readonly driver: string;
  readonly model: string;
  /** The fake's launcher, relative to tools/server-bench. */
  readonly launcher: string;
  /** The fake's transcript list variable; the pacing knobs are shared. */
  readonly transcriptVar: string;
}

export const BENCH_PROVIDERS: Readonly<Record<ProviderKind, BenchProvider>> = {
  claude: {
    kind: "claude",
    instanceId: "claudeBench",
    driver: "claudeAgent",
    model: "claude-sonnet-4-6",
    launcher: "fake-claude/claude",
    transcriptVar: "BENCH_TRANSCRIPT",
  },
  codex: {
    kind: "codex",
    instanceId: "codexBench",
    driver: "codex",
    model: "gpt-6-luna",
    launcher: "fake-codex/codex",
    transcriptVar: "BENCH_CODEX_TRANSCRIPT",
  },
  acp: {
    kind: "acp",
    instanceId: "acpBench",
    driver: "acpRegistry",
    model: "bench-replay",
    launcher: "fake-acp/acp-agent",
    transcriptVar: "BENCH_ACP_TRANSCRIPT",
  },
};

// Built-in instances a fresh settings file would probe; disabled so no run reaches a real provider.
const BUILT_IN_DRIVERS = [
  "codex",
  "claudeAgent",
  "cursor",
  "grok",
  "fx",
  "opencode",
  "antigravity",
  "pi",
  "muse",
] as const;

export class ProviderLoadError extends Error {
  override readonly name = "ProviderLoadError";
}

/** Load-driver flags, in `node:util` parseArgs form. `--sessions`/`--provider` stay the driver's. */
export const PROVIDER_LOAD_FLAGS = {
  "codex-sessions": { type: "string", default: "0" },
  "acp-sessions": { type: "string", default: "0" },
  "codex-provider": { type: "string", default: BENCH_PROVIDERS.codex.instanceId },
  "codex-model": { type: "string", default: BENCH_PROVIDERS.codex.model },
  "acp-provider": { type: "string", default: BENCH_PROVIDERS.acp.instanceId },
  "acp-model": { type: "string", default: BENCH_PROVIDERS.acp.model },
} as const;

export const PROVIDER_LOAD_USAGE = `  --codex-sessions N        sessions on the replaying Codex instance, on top of --sessions (default 0)
  --acp-sessions N          sessions on the replaying ACP instance, on top of --sessions (default 0)
  --codex-provider ID       --codex-model SLUG   (default ${BENCH_PROVIDERS.codex.instanceId}, ${BENCH_PROVIDERS.codex.model})
  --acp-provider ID         --acp-model SLUG     (default ${BENCH_PROVIDERS.acp.instanceId}, ${BENCH_PROVIDERS.acp.model})
  Turn and delta rates are the fakes' BENCH_* knobs in the scratch settings (providers.ts settings).`;

export interface SessionGroup {
  readonly kind: ProviderKind;
  readonly instanceId: string;
  readonly model: string;
  readonly count: number;
}

export interface PlannedSession {
  readonly index: number;
  readonly kind: ProviderKind;
  readonly instanceId: string;
  readonly model: string;
}

function parseCount(text: string, flag: string): number {
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new ProviderLoadError(`--${flag} must be an integer of at least 0`);
  }
  return value;
}

/**
 * The session groups of one run: the driver's own `--sessions` on `--provider` (a Claude
 * instance unless it names the Codex or ACP one), then the Codex and ACP sessions.
 */
export function parseProviderLoad(
  values: { readonly [K in keyof typeof PROVIDER_LOAD_FLAGS]?: string | undefined },
  primary: { readonly sessions: number; readonly provider: string; readonly model: string },
): ReadonlyArray<SessionGroup> {
  const flag = (name: keyof typeof PROVIDER_LOAD_FLAGS) =>
    values[name] ?? PROVIDER_LOAD_FLAGS[name].default;
  const codex = {
    kind: "codex",
    instanceId: flag("codex-provider"),
    model: flag("codex-model"),
    count: parseCount(flag("codex-sessions"), "codex-sessions"),
  } as const;
  const acp = {
    kind: "acp",
    instanceId: flag("acp-provider"),
    model: flag("acp-model"),
    count: parseCount(flag("acp-sessions"), "acp-sessions"),
  } as const;
  const primaryKind =
    primary.provider === codex.instanceId
      ? "codex"
      : primary.provider === acp.instanceId
        ? "acp"
        : "claude";
  return [
    {
      kind: primaryKind,
      instanceId: primary.provider,
      model: primary.model,
      count: primary.sessions,
    },
    codex,
    acp,
  ];
}

/**
 * One entry per session, interleaved so every prefix of the list holds each group in proportion:
 * sessions started in order ramp every provider up together.
 */
export function planSessions(groups: ReadonlyArray<SessionGroup>): ReadonlyArray<PlannedSession> {
  const slots = groups.flatMap((group, order) =>
    Array.from({ length: group.count }, (_, nth) => ({
      group,
      order,
      position: (nth + 0.5) / group.count,
    })),
  );
  slots.sort((left, right) => left.position - right.position || left.order - right.order);
  return slots.map(({ group }, index) => ({
    index,
    kind: group.kind,
    instanceId: group.instanceId,
    model: group.model,
  }));
}

/**
 * History mode: session i continues seeded thread i on the instance and model the plan gives it,
 * whatever that thread last ran on; such a thread switches with the session's first turn.
 * `switches` counts, per instance, the threads it takes over from another instance, and
 * `modelSwitches` the threads already on it that move to the planned model.
 */
export function historyAssignments<
  T extends { readonly modelSelection: { readonly instanceId: string; readonly model: string } },
>(
  plan: ReadonlyArray<PlannedSession>,
  threads: ReadonlyArray<T>,
): {
  readonly assignments: ReadonlyArray<{
    readonly session: PlannedSession;
    readonly thread: T;
    /** The first turn sends the planned instance and model. */
    readonly switchProvider: boolean;
  }>;
  readonly switches: Readonly<Record<string, number>>;
  readonly modelSwitches: Readonly<Record<string, number>>;
} {
  if (threads.length < plan.length) {
    throw new ProviderLoadError(
      `${plan.length} sessions need as many threads, not ${threads.length}`,
    );
  }
  const switches: Record<string, number> = {};
  const modelSwitches: Record<string, number> = {};
  const count = (counts: Record<string, number>, instanceId: string) => {
    counts[instanceId] = (counts[instanceId] ?? 0) + 1;
  };
  const assignments = plan.map((session, index) => {
    const thread = threads[index]!;
    const otherInstance = thread.modelSelection.instanceId !== session.instanceId;
    const otherModel = !otherInstance && thread.modelSelection.model !== session.model;
    if (otherInstance) count(switches, session.instanceId);
    if (otherModel) count(modelSwitches, session.instanceId);
    return { session, thread, switchProvider: otherInstance || otherModel };
  });
  return { assignments, switches, modelSwitches };
}

export const FAKE_STATE_DIR = "fake-state";

/**
 * A fresh, owner-only directory per planned instance under `<out>/fake-state`: a run's sessions
 * claim their ordinals from 0 whatever settings it restores. A non-empty one is refused, since
 * reusing it would shift the mix.
 */
export function prepareFakeState(
  out: string,
  instanceIds: ReadonlyArray<string>,
): Record<string, string> {
  const root = NodePath.resolve(out, FAKE_STATE_DIR);
  if (NodeFS.existsSync(root) && NodeFS.readdirSync(root).length > 0) {
    throw new ProviderLoadError(`${root} is not empty: give every run its own --out`);
  }
  const create = (dir: string) => {
    NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
    NodeFS.chmodSync(dir, 0o700);
    return dir;
  };
  create(root);
  return Object.fromEntries(
    instanceIds.map((id) => [id, create(NodePath.join(root, id.replace(/[^\w.-]/gu, "_")))]),
  );
}

type InstanceEnvironment = ReadonlyArray<{
  readonly name: string;
  readonly value: string;
  readonly sensitive?: boolean;
}>;

/** `environment` with `values` set: entries of the same names replaced, the rest kept in order. */
export function withEnvironment(
  environment: InstanceEnvironment,
  values: Readonly<Record<string, string>>,
): InstanceEnvironment {
  return [
    ...environment.filter((entry) => !(entry.name in values)),
    ...Object.entries(values).map(([name, value]) => ({ name, value, sensitive: false })),
  ];
}

/** Sessions per provider instance, the way a run's result names them. */
export function sessionsByInstance(
  plan: ReadonlyArray<PlannedSession>,
): Readonly<Record<string, number>> {
  const counts: Record<string, number> = {};
  for (const session of plan) counts[session.instanceId] = (counts[session.instanceId] ?? 0) + 1;
  return counts;
}

export interface BenchKnobs {
  readonly seed?: string | undefined;
  readonly speed?: string | undefined;
  readonly deltasPerSec?: string | undefined;
  readonly turnLoops?: string | undefined;
  readonly toolPayloadBytes?: string | undefined;
  readonly maxGapMs?: string | undefined;
}

export interface BenchInstanceOptions {
  /** Directory holding the fake launchers (tools/server-bench). */
  readonly benchDir: string;
  /** The node binary the launchers run (BENCH_NODE). */
  readonly node: string;
  /** Each fake appends a JSON line per played turn here (BENCH_STATS_DIR). */
  readonly statsDir: string;
  readonly kinds: ReadonlyArray<ProviderKind>;
  readonly knobs?: BenchKnobs;
  readonly transcripts?: Partial<Record<ProviderKind, string>>;
  /** Extra environment per kind, such as `{ codex: { BENCH_FAULT: "exit-mid-turn" } }`. */
  readonly env?: Partial<Record<ProviderKind, Readonly<Record<string, string>>>>;
  readonly launchers?: Partial<Record<ProviderKind, string>>;
  /** CODEX_HOME and the Claude home of the instances (empty scratch directories). */
  readonly codexHome?: string;
  readonly claudeHome?: string;
}

type EnvironmentEntry = {
  readonly name: string;
  readonly value: string;
  readonly sensitive: false;
};

function environment(entries: Readonly<Record<string, string | undefined>>) {
  return Object.entries(entries).flatMap(([name, value]): ReadonlyArray<EnvironmentEntry> =>
    value === undefined || value === "" ? [] : [{ name, value, sensitive: false }],
  );
}

/** The `providerInstances` entries that register the replaying fakes. */
export function benchProviderInstances(
  options: BenchInstanceOptions,
): Readonly<Record<string, unknown>> {
  const instances: Record<string, unknown> = {};
  for (const kind of options.kinds) {
    const provider = BENCH_PROVIDERS[kind];
    const launcher =
      options.launchers?.[kind] ?? NodePath.join(options.benchDir, provider.launcher);
    const config =
      kind === "acp"
        ? { source: "local", commandPath: launcher, commandArgs: [] }
        : kind === "codex"
          ? { binaryPath: launcher, ...(options.codexHome ? { homePath: options.codexHome } : {}) }
          : {
              binaryPath: launcher,
              ...(options.claudeHome ? { homePath: options.claudeHome } : {}),
            };
    instances[provider.instanceId] = {
      driver: provider.driver,
      displayName: `${kind === "acp" ? "ACP" : kind === "codex" ? "Codex" : "Claude"} (bench)`,
      config,
      environment: environment({
        BENCH_NODE: options.node,
        BENCH_STATS_DIR: options.statsDir,
        BENCH_SEED: options.knobs?.seed,
        BENCH_SPEED: options.knobs?.speed,
        BENCH_DELTAS_PER_SEC: options.knobs?.deltasPerSec,
        BENCH_TURN_LOOPS: options.knobs?.turnLoops,
        BENCH_TOOL_PAYLOAD_BYTES: options.knobs?.toolPayloadBytes,
        BENCH_MAX_GAP_MS: options.knobs?.maxGapMs,
        [provider.transcriptVar]: options.transcripts?.[kind],
        ...options.env?.[kind],
      }),
    };
  }
  return instances;
}

/** Settings with the bench instances added; built-in instances disabled unless kept. */
export function withBenchInstances(
  settings: Readonly<Record<string, unknown>>,
  options: BenchInstanceOptions & { readonly keepBuiltIns?: boolean },
): Record<string, unknown> {
  const current = (settings.providerInstances ?? {}) as Readonly<Record<string, unknown>>;
  const disabled = options.keepBuiltIns
    ? {}
    : Object.fromEntries(
        BUILT_IN_DRIVERS.map((driver) => [
          driver,
          { ...(current[driver] as object | undefined), driver, enabled: false },
        ]),
      );
  return {
    ...settings,
    providerInstances: { ...current, ...disabled, ...benchProviderInstances(options) },
    ...(options.kinds.includes("claude")
      ? {
          textGenerationModelSelection: {
            instanceId: BENCH_PROVIDERS.claude.instanceId,
            model: BENCH_PROVIDERS.claude.model,
          },
        }
      : {}),
  };
}

/** The BENCH_* environment of every bench instance in a settings file, for a run's result. */
export function readBenchKnobs(
  settingsPath: string,
): Readonly<Record<string, Record<string, string>>> {
  const settings = JSON.parse(NodeFS.readFileSync(settingsPath, "utf8")) as {
    readonly providerInstances?: Readonly<
      Record<string, { readonly environment?: ReadonlyArray<{ name: string; value: string }> }>
    >;
  };
  const knobs: Record<string, Record<string, string>> = {};
  for (const [instanceId, instance] of Object.entries(settings.providerInstances ?? {})) {
    const entries = (instance.environment ?? []).filter(
      (entry) => entry.name.startsWith("BENCH_") && entry.name !== "BENCH_NODE",
    );
    if (entries.length > 0) {
      knobs[instanceId] = Object.fromEntries(entries.map((entry) => [entry.name, entry.value]));
    }
  }
  return knobs;
}

interface FakeModule {
  readonly configFrom?: (env: Readonly<Record<string, string>>) => unknown;
  readonly loadTranscripts?: (config: unknown) => Promise<unknown>;
  readonly readConfig?: (env: Readonly<Record<string, string>>) => unknown;
  readonly loadMix?: (config: unknown) => Promise<unknown>;
}

/**
 * Reads a bench instance's knobs and loads its transcript mix the way its fake will at start, so
 * a bad value fails here instead of in every session. A Claude launcher without the fake beside it
 * is not checked.
 */
export async function validateBenchInstance(
  kind: ProviderKind,
  launcher: string,
  environment: ReadonlyArray<{ readonly name: string; readonly value: string }>,
): Promise<void> {
  const file = NodePath.join(
    NodePath.dirname(launcher),
    `${NodePath.basename(BENCH_PROVIDERS[kind].launcher)}.mjs`,
  );
  if (!NodeFS.existsSync(file)) return;
  const fake = (await import(NodeURL.pathToFileURL(file).href)) as FakeModule;
  const env = Object.fromEntries(environment.map((entry) => [entry.name, entry.value]));
  try {
    if (fake.configFrom !== undefined && fake.loadTranscripts !== undefined) {
      await fake.loadTranscripts(fake.configFrom(env));
    } else if (fake.readConfig !== undefined && fake.loadMix !== undefined) {
      await fake.loadMix(fake.readConfig(env));
    }
  } catch (error) {
    throw new ProviderLoadError(
      `${BENCH_PROVIDERS[kind].instanceId}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

const SETTINGS_USAGE = `Usage: node tools/server-bench/providers.ts settings --out <settings.json> --stats-dir <dir> [options]

Adds the replaying bench provider instances to a scratch server's settings.json (created when
missing, other keys kept) and disables the built-in instances. Write it before the server
starts: a running Codex app-server is shared by its instance's sessions and keeps the
environment it started with. It sets no BENCH_STATE_DIR: the load driver gives each run's fakes a
fresh one, so restoring these settings for every run replays the same plan.

  --kinds LIST              claude,codex,acp (default codex,acp; claude needs fake-claude/claude)
  --seed N                  BENCH_SEED for every fake: where the transcript mix's cycle starts
  --speed X                 BENCH_SPEED for every fake (time scale; 0 never waits)
  --deltas-per-sec N        BENCH_DELTAS_PER_SEC for every fake
  --turn-loops N            BENCH_TURN_LOOPS for every fake
  --tool-payload-bytes N    BENCH_TOOL_PAYLOAD_BYTES for every fake: the large-tool-payload size
  --max-gap-ms N            BENCH_MAX_GAP_MS for the Codex and ACP fakes: caps recorded waits
  --claude-transcripts L    BENCH_TRANSCRIPT; --codex-transcripts L, --acp-transcripts L likewise
  --env KIND:NAME=VALUE     extra environment for one kind, e.g. codex:BENCH_FAULT=exit-mid-turn
  --node PATH               BENCH_NODE (default: this node)
  --codex-home DIR          CODEX_HOME of the Codex instance (created)
  --claude-home DIR         home of the Claude instance (created)
  --claude-launcher PATH    the fake Claude launcher, when it lives elsewhere
  --keep-built-ins          leave the built-in provider instances as they are
`;

async function settingsCommand(argv: ReadonlyArray<string>): Promise<void> {
  const { values } = NodeUtil.parseArgs({
    args: [...argv],
    options: {
      out: { type: "string" },
      "stats-dir": { type: "string" },
      kinds: { type: "string", default: "codex,acp" },
      seed: { type: "string" },
      speed: { type: "string" },
      "deltas-per-sec": { type: "string" },
      "turn-loops": { type: "string" },
      "tool-payload-bytes": { type: "string" },
      "max-gap-ms": { type: "string" },
      "claude-transcripts": { type: "string" },
      "codex-transcripts": { type: "string" },
      "acp-transcripts": { type: "string" },
      env: { type: "string", multiple: true, default: [] },
      node: { type: "string", default: process.execPath },
      "codex-home": { type: "string" },
      "claude-home": { type: "string" },
      "claude-launcher": { type: "string" },
      "keep-built-ins": { type: "boolean", default: false },
      help: { type: "boolean", default: false },
    },
  });
  if (values.help) {
    process.stdout.write(SETTINGS_USAGE);
    return;
  }
  if (values.out === undefined || values["stats-dir"] === undefined) {
    throw new ProviderLoadError("--out and --stats-dir are required");
  }
  const kinds = values.kinds.split(",").map((kind) => kind.trim());
  for (const kind of kinds) {
    if (!(PROVIDER_KINDS as ReadonlyArray<string>).includes(kind)) {
      throw new ProviderLoadError(`--kinds takes ${PROVIDER_KINDS.join(",")}, not ${kind}`);
    }
  }
  const env: Partial<Record<ProviderKind, Record<string, string>>> = {};
  for (const assignment of values.env) {
    const match = /^(claude|codex|acp):([A-Za-z_][A-Za-z0-9_]*)=(.*)$/s.exec(assignment);
    if (match === null)
      throw new ProviderLoadError(`--env takes KIND:NAME=VALUE, not ${assignment}`);
    const kind = match[1] as ProviderKind;
    env[kind] = { ...env[kind], [match[2]!]: match[3]! };
  }
  const benchDir = import.meta.dirname;
  const launchers: Partial<Record<ProviderKind, string>> = {};
  if (values["claude-launcher"] !== undefined) launchers.claude = values["claude-launcher"];
  for (const kind of kinds as ReadonlyArray<ProviderKind>) {
    const launcher = launchers[kind] ?? NodePath.join(benchDir, BENCH_PROVIDERS[kind].launcher);
    if (!NodeFS.existsSync(launcher)) {
      throw new ProviderLoadError(`No ${kind} fake at ${launcher}`);
    }
  }
  const current = NodeFS.existsSync(values.out)
    ? (JSON.parse(NodeFS.readFileSync(values.out, "utf8")) as Record<string, unknown>)
    : {};
  const settings = withBenchInstances(current, {
    benchDir,
    node: values.node,
    statsDir: NodePath.resolve(values["stats-dir"]),
    kinds: kinds as ReadonlyArray<ProviderKind>,
    knobs: {
      seed: values.seed,
      speed: values.speed,
      deltasPerSec: values["deltas-per-sec"],
      turnLoops: values["turn-loops"],
      toolPayloadBytes: values["tool-payload-bytes"],
      maxGapMs: values["max-gap-ms"],
    },
    transcripts: {
      ...(values["claude-transcripts"] ? { claude: values["claude-transcripts"] } : {}),
      ...(values["codex-transcripts"] ? { codex: values["codex-transcripts"] } : {}),
      ...(values["acp-transcripts"] ? { acp: values["acp-transcripts"] } : {}),
    },
    env,
    launchers,
    ...(values["codex-home"] ? { codexHome: NodePath.resolve(values["codex-home"]) } : {}),
    ...(values["claude-home"] ? { claudeHome: NodePath.resolve(values["claude-home"]) } : {}),
    keepBuiltIns: values["keep-built-ins"],
  });
  const instances = settings.providerInstances as Readonly<
    Record<string, { readonly environment: ReadonlyArray<{ name: string; value: string }> }>
  >;
  for (const kind of kinds as ReadonlyArray<ProviderKind>) {
    const launcher = launchers[kind] ?? NodePath.join(benchDir, BENCH_PROVIDERS[kind].launcher);
    await validateBenchInstance(
      kind,
      launcher,
      instances[BENCH_PROVIDERS[kind].instanceId]?.environment ?? [],
    );
  }
  for (const dir of [values["stats-dir"], values["codex-home"], values["claude-home"]]) {
    if (dir !== undefined) NodeFS.mkdirSync(dir, { recursive: true });
  }
  NodeFS.mkdirSync(NodePath.dirname(values.out), { recursive: true });
  NodeFS.writeFileSync(values.out, `${JSON.stringify(settings, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify(readBenchKnobs(values.out))}\n`);
}

if (import.meta.main) {
  const [command, ...rest] = process.argv.slice(2);
  (command === "settings"
    ? settingsCommand(rest)
    : Promise.reject(new ProviderLoadError(SETTINGS_USAGE))
  ).catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(2);
  });
}

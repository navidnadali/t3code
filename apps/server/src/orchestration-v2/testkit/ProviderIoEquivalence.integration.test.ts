import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  ClaudeSettings,
  type OrchestrationV2StoredEvent,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { AcpLineOffloads } from "@t3tools/provider-acp/server/AcpSessionRuntime";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { materializeReplayTranscriptWorkspace } from "@t3tools/provider-testing/replayTranscript";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../config.ts";
import * as EventNdjsonLogger from "../../provider/EventNdjsonLogger.ts";
import * as PortScanner from "../../preview/PortScanner.ts";
import {
  serializedRecordProblem,
  serializeEventNdjsonRecord,
} from "../../provider/EventNdjsonRecord.ts";
import * as ProviderIoOffloads from "../../provider/io/ProviderIoOffloads.ts";
import * as ProviderIoWorkerPool from "../../provider/io/ProviderIoWorkerPool.ts";
import { AcpRegistryOrchestratorReplayHarness } from "../Adapters/AcpRegistryAdapterV2.testkit.ts";
import * as ClaudeAdapterV2 from "../Adapters/ClaudeAdapterV2.ts";
import { CodexOrchestratorReplayHarness } from "../Adapters/CodexAdapterV2.testkit.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "./fixtures/index.ts";
import { ReplayStdoutChunking, seededStdoutChunking } from "./ReplayStdoutChunking.ts";
import { runReplayFixture, type ReplayFixtureCase } from "./ProviderReplayFixtures.ts";
import {
  makeReplayServerConfig,
  type OrchestratorV2ProviderReplayHarness,
} from "./ProviderReplayHarness.ts";

type ProviderIoServices =
  | ProviderEventLoggers.ProviderEventLoggers
  | ProviderIoWorkerPool.ProviderIoWorkerPool
  | ProviderIoOffloads.CodexLineOffloads
  | AcpLineOffloads
  | ReplayStdoutChunking;

interface Capture {
  /** Per provider session, in open order. */
  readonly adapterEvents: ReadonlyArray<ReadonlyArray<unknown>>;
  /** Per file, stream, direction and provider session (see `byLogWriter`), in order. */
  readonly logLines: Readonly<Record<string, ReadonlyArray<unknown>>>;
  /** Per entity (see `byEntity`), in order. */
  readonly sinkWrites: Readonly<Record<string, ReadonlyArray<unknown>>>;
  /** The type of each thread's and run's first EventSink write, across writers. */
  readonly firstWrites: Readonly<Record<string, string>>;
}

interface PoolUse {
  readonly sessions: number;
  /** Lines the workers decoded (not handed back to main). */
  readonly decoded: number;
}

interface CaptureRun {
  readonly capture: Capture;
  /** Per stage name. */
  readonly pool: Readonly<Record<string, PoolUse>>;
}

interface WorkerMode {
  readonly name: string;
  readonly workers: number;
  /** T3CODE_PROVIDER_IO_{CODEX,ACP}_OFFLOAD_MIN_CHARS; unset keeps the defaults. */
  readonly offloadMinChars?: number;
}

interface AdapterRow {
  readonly driver: string;
  readonly path: string;
  /**
   * `pending` until the provider's output runs on the workers: the row still compares each mode
   * with 0 workers. `required` also fails unless the workers took the provider's sessions, and,
   * with an offload threshold of 0, its lines.
   */
  readonly offload: "pending" | "required";
  /** The pool stage that takes this provider's output. */
  readonly stage: string;
  readonly fixtures?: ReadonlySet<string>;
  /** Compared with 0 workers. */
  readonly modes: ReadonlyArray<WorkerMode>;
  readonly capture: (input: ReplayFixtureCase, mode: WorkerMode) => Effect.Effect<CaptureRun>;
}

const LOG_LINE = /^\[[^\]]*\] ([A-Z]+): (.*)$/u;
const ISO_TIMESTAMP = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z/gu;
const UUID_V4 = /[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gu;
const ANY_UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/giu;
const API_ID = /\b(?:msg|toolu|srvtoolu|req)_[A-Za-z0-9]{8,}/gu;
const TASK_ID = /\\?"(?:task_id|taskId|agent_id|agentId)\\?":\\?"([^"\\]+)\\?"/gu;
const DURATION_MS = /(\\?"duration_ms\\?":)\d+/gu;

// Writers interleave and race even in-thread (an interrupt's settle effect or the provider's own
// events close a run), so writes are compared per entity, in order, without the command-derived
// event id. Attribution is kept except on the settle effect's provider-thread write, the only
// write that race attributes to the run's node and run.
const SETTLE_EFFECT = /:background-work-settled$/u;

const byEntity = (storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>) => {
  const entities = new Map<string, Array<unknown>>();
  for (const { commandId, event } of storedEvents) {
    const { id: _id, ...write } = event;
    const settled =
      commandId !== null &&
      SETTLE_EFFECT.test(commandId) &&
      event.type === "provider-thread.updated";
    const {
      nodeId: _nodeId,
      runId: _runId,
      ...unattributed
    } = write as typeof write & {
      readonly nodeId?: unknown;
      readonly runId?: unknown;
    };
    const entityId = (event.payload as { readonly id?: unknown } | undefined)?.id;
    const entity =
      typeof entityId === "string" ? `${event.type.split(".")[0]} ${entityId}` : event.type;
    entities.set(entity, [...(entities.get(entity) ?? []), settled ? unattributed : write]);
  }
  return Object.fromEntries([...entities].toSorted(([a], [b]) => (a < b ? -1 : 1)));
};

interface LogLine {
  readonly file: string;
  readonly stream: string | undefined;
  readonly record: unknown;
}

// Each session's incoming protocol records come from its stdout reader and the outgoing ones from
// its request senders, so the order is compared per file, stream, direction and session.
// With `perRequest`, request records are also split per request: the real log store can append
// different requests' records out of send order under contention (P1-069 follow-up pending).
const byLogWriter = (logLines: ReadonlyArray<LogLine>, perRequest: boolean) => {
  const writers = new Map<string, Array<unknown>>();
  for (const { file, stream, record } of logLines) {
    const { event, providerSessionId } = record as {
      readonly event?: {
        readonly direction?: unknown;
        readonly kind?: unknown;
        readonly payload?: { readonly method?: unknown; readonly request?: unknown };
      };
      readonly providerSessionId?: unknown;
    };
    const direction = typeof event?.direction === "string" ? event.direction : "-";
    const session = typeof providerSessionId === "string" ? providerSessionId : "-";
    const request =
      perRequest && event?.kind === "request"
        ? ` ${String(event.payload?.method)} ${JSON.stringify(event.payload?.request)}`
        : "";
    const writer = `${file} ${String(stream)} ${direction} ${session}${request}`;
    writers.set(writer, [...(writers.get(writer) ?? []), record]);
  }
  return Object.fromEntries([...writers].toSorted(([a], [b]) => (a < b ? -1 : 1)));
};

// Writers race, but each thread and run starts with the same write whatever the interleaving.
const firstWrites = (storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>) => {
  const first = new Map<string, string>();
  for (const { event } of storedEvents) {
    const { threadId, runId } = event as { readonly threadId?: unknown; readonly runId?: unknown };
    for (const key of [`thread ${String(threadId)}`, `run ${String(runId)}`]) {
      if (!key.endsWith(" undefined") && !first.has(key)) first.set(key, event.type);
    }
  }
  return Object.fromEntries([...first].toSorted(([a], [b]) => (a < b ? -1 : 1)));
};

// Timestamps, per-run temporary directory names and random ids are the only run-specific values.
// Temporary directories keep their prefix, and they and random ids (v4 UUIDs; with `freshIds` also
// the ids a fake CLI mints) are renamed by first appearance across the channels, so where each one
// recurs is still compared.
const normalize = (
  raw: {
    readonly adapterEvents: ReadonlyArray<ReadonlyArray<unknown>>;
    readonly logLines: ReadonlyArray<LogLine>;
    readonly sinkWrites: ReadonlyArray<OrchestrationV2StoredEvent>;
  },
  tmpRoot: string,
  options: { readonly freshIds: boolean; readonly perRequest: boolean },
): Capture => {
  const { freshIds } = options;
  // Either separator (Windows), repeated where a path sits inside an escaped JSON string.
  const separator = String.raw`[\\/]+`;
  const tmpDirectory = new RegExp(
    `(?:${separator}private)?${tmpRoot
      .split(/[\\/]+/u)
      .map((segment) => segment.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&"))
      .join(separator)}${separator}([^\\\\/\\s"']+)`,
    "gu",
  );
  const tmpNames = new Map<string, string>();
  const tmpName = (_match: string, directory: string) => {
    // mkdtemp appends six random characters to the prefix.
    const known = tmpNames.get(directory) ?? `<tmp ${directory.slice(0, -6)}#${tmpNames.size + 1}>`;
    tmpNames.set(directory, known);
    return known;
  };
  const scrub = (value: unknown) => {
    const text = JSON.stringify(value, (_key, field: unknown) =>
      typeof field === "string" ? field.replace(tmpDirectory, tmpName) : field,
    ).replace(ISO_TIMESTAMP, "<timestamp>");
    return freshIds ? text.replace(DURATION_MS, (_match, key: string) => `${key}0`) : text;
  };
  const texts = [
    scrub(raw.adapterEvents),
    scrub(byLogWriter(raw.logLines, options.perRequest)),
    scrub(byEntity(raw.sinkWrites)),
    scrub(firstWrites(raw.sinkWrites)),
  ];
  const ids = new Map<string, string>();
  const name = (id: string) => {
    if (!ids.has(id)) ids.set(id, `<id ${ids.size + 1}>`);
  };
  for (const text of texts) {
    if (freshIds) for (const match of text.matchAll(TASK_ID)) name(match[1]!);
    for (const match of text.matchAll(freshIds ? ANY_UUID : UUID_V4)) name(match[0]);
    if (freshIds) for (const match of text.matchAll(API_ID)) name(match[0]);
  }
  const renames = [...ids].toSorted(([a], [b]) => b.length - a.length);
  const [adapterEvents, logLines, sinkWrites, firstWritten] = texts.map((text) =>
    JSON.parse(renames.reduce((renamed, [id, alias]) => renamed.replaceAll(id, alias), text)),
  );
  return { adapterEvents, logLines, sinkWrites, firstWrites: firstWritten };
};

const recordAdapterEvents = (
  registry: ProviderAdapterRegistry.ProviderAdapterRegistryV2["Service"],
  adapterEvents: Array<Array<unknown>>,
) =>
  ProviderAdapterRegistry.ProviderAdapterRegistryV2.of({
    ...registry,
    get: (instanceId) =>
      registry.get(instanceId).pipe(
        Effect.map((adapter) => ({
          ...adapter,
          openSession: (input) =>
            adapter.openSession(input).pipe(
              Effect.map((runtime) => {
                const events: Array<unknown> = [];
                adapterEvents.push(events);
                return {
                  ...runtime,
                  events: runtime.events.pipe(
                    Stream.tap((event) =>
                      Effect.sync(() => {
                        events.push(event);
                      }),
                    ),
                  ),
                };
              }),
            ),
        })),
      ),
  });

const fixtureSeed = (input: ReplayFixtureCase) => {
  let hash = 0x2029;
  for (const character of `${input.driver.driver} ${input.fixtureName}`) {
    hash = Math.imul(hash ^ character.charCodeAt(0), 0x01000193);
  }
  return hash;
};

// Replays a fixture with the pool at `workers`, the native protocol log written by the real log
// store, and the adapter's events recorded where the session manager reads them.
const adapterRow = <
  Transcript extends ProviderReplayTranscript,
  Error,
  WorkspaceError = never,
>(row: {
  readonly driver: string;
  readonly path: string;
  readonly offload: AdapterRow["offload"];
  readonly stage: string;
  readonly fixtures?: ReadonlySet<string>;
  readonly modes: AdapterRow["modes"];
  readonly freshIds?: boolean;
  /** Writes the native log through the real store instead of the synchronous capture. */
  readonly realLogStore?: true;
  /** Off where the provider is not replayed from the fixture's recording, so its checks do not apply. */
  readonly checkOutput?: boolean;
  readonly harness: (
    services: Layer.Layer<ProviderIoServices>,
    input: ReplayFixtureCase,
  ) => Effect.Effect<
    OrchestratorV2ProviderReplayHarness<Transcript, Error>,
    never,
    FileSystem.FileSystem | Path.Path | Scope.Scope
  >;
  readonly materializeWorkspace?: (
    transcript: ProviderReplayTranscript,
    workspace: string,
  ) => Effect.Effect<ProviderReplayTranscript, WorkspaceError>;
}): AdapterRow => ({
  driver: row.driver,
  path: row.path,
  offload: row.offload,
  stage: row.stage,
  ...(row.fixtures === undefined ? {} : { fixtures: row.fixtures }),
  modes: row.modes,
  capture: (input, mode) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const logDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-provider-io-equivalence-",
      });
      // Otherwise the native log as the store accepts it, appended synchronously and so in call
      // order: the real store can reorder records of different requests under contention.
      const store =
        row.realLogStore === true
          ? yield* EventNdjsonLogger.makeEventNdjsonLogStore(path.join(logDir, "events.log"), {
              batchWindowMs: 0,
            })
          : undefined;
      const logLines: Array<LogLine> = [];
      const captured: ProviderEventLoggers.EventNdjsonLogger = {
        filePath: path.join(logDir, "events.log"),
        write: (event, threadId) =>
          Effect.sync(() => {
            const record = serializeEventNdjsonRecord("native", event);
            if (record._tag === "Skipped") return;
            logLines.push({
              file: String(threadId),
              stream: "native",
              record:
                record._tag === "Serialized"
                  ? JSON.parse(record.payload)
                  : { failed: record.error.message },
            });
          }),
        writeSerialized: (record, threadId) =>
          Effect.sync(() => {
            const problem = serializedRecordProblem("native", record);
            logLines.push({
              file: String(threadId),
              stream: record.stream,
              record: problem === undefined ? JSON.parse(record.payload) : { refused: problem },
            });
          }),
        close: () => Effect.void,
      };
      const native = store === undefined ? captured : store.logger("native");
      const pool = yield* ProviderIoWorkerPool.make({ workers: mode.workers });
      const use: Record<string, { sessions: number; decoded: number }> = {};
      const layerPool = Layer.succeed(
        ProviderIoWorkerPool.ProviderIoWorkerPool,
        ProviderIoWorkerPool.ProviderIoWorkerPool.of({
          open: (openInput) =>
            pool.open(openInput).pipe(
              Effect.map(
                Option.map((session) => {
                  const stage = (use[openInput.stage.name] ??= { sessions: 0, decoded: 0 });
                  stage.sessions += 1;
                  // Only lines the worker decoded count: it may hand a line back to main.
                  return {
                    ...session,
                    call: (request) =>
                      session.call(request).pipe(
                        Effect.tap((reply) =>
                          Effect.sync(() => {
                            if ((reply as { readonly _tag?: unknown })._tag === "Decoded") {
                              stage.decoded += 1;
                            }
                          }),
                        ),
                      ),
                  };
                }),
              ),
            ),
        }),
      );
      const offloadMinChars = mode.offloadMinChars;
      const layerConfig = Layer.effect(
        ServerConfig.ServerConfig,
        makeReplayServerConfig(input.fixtureName).pipe(
          // The knobs as the server reads them: offloads open only with workers configured.
          Effect.map((config) => ({
            ...config,
            providerIoWorkers: mode.workers,
            ...(offloadMinChars === undefined
              ? {}
              : {
                  providerIoCodexOffloadMinChars: offloadMinChars,
                  providerIoAcpOffloadMinChars: offloadMinChars,
                }),
          })),
          Effect.orDie,
        ),
      ).pipe(Layer.provide(NodeServices.layer));
      // As in the server: sessions find the pool and the line offloads in their context.
      const services = yield* Layer.build(
        Layer.mergeAll(
          Layer.succeed(ProviderEventLoggers.ProviderEventLoggers, {
            native,
            canonical: undefined,
          }),
          layerPool,
          ProviderIoOffloads.layer.pipe(Layer.provide(Layer.merge(layerPool, layerConfig))),
          // The same seed in every mode, so each mode reads the same chunks.
          Layer.succeed(ReplayStdoutChunking, seededStdoutChunking(fixtureSeed(input))),
        ),
      );
      const harness = yield* row.harness(Layer.succeedContext(services), input);
      const adapterEvents: Array<Array<unknown>> = [];
      const result = yield* runReplayFixture({
        ...input,
        ...(row.checkOutput === false
          ? { driver: { ...input.driver, assertOutput: () => undefined } }
          : {}),
        harness: {
          ...harness,
          makeProviderAdapterRegistryLayer: (transcript, options) =>
            Layer.effect(
              ProviderAdapterRegistry.ProviderAdapterRegistryV2,
              ProviderAdapterRegistry.ProviderAdapterRegistryV2.pipe(
                Effect.map((registry) => recordAdapterEvents(registry, adapterEvents)),
              ),
            ).pipe(Layer.provide(harness.makeProviderAdapterRegistryLayer(transcript, options))),
        },
        ...(row.materializeWorkspace === undefined
          ? {}
          : { materializeWorkspace: row.materializeWorkspace }),
      }).pipe(Effect.provide(services));
      if (store !== undefined) {
        yield* store.close();
        for (const file of (yield* fileSystem.readDirectory(logDir)).toSorted()) {
          const contents = yield* fileSystem.readFileString(path.join(logDir, file));
          for (const line of contents.split("\n")) {
            if (line.length === 0) continue;
            const [, stream, payload] = LOG_LINE.exec(line) ?? [];
            assert.isDefined(payload, `log line format: ${line}`);
            logLines.push({ file, stream, record: JSON.parse(payload) });
          }
        }
      }
      const capture = normalize(
        { adapterEvents, logLines, sinkWrites: result.storedEvents },
        path.dirname(logDir),
        { freshIds: row.freshIds === true, perRequest: row.realLogStore === true },
      );
      return { capture, pool: use };
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer), Effect.orDie),
});

const replayHarness =
  <Transcript extends ProviderReplayTranscript, Error>(
    harness: OrchestratorV2ProviderReplayHarness<Transcript, Error>,
  ) =>
  (services: Layer.Layer<ProviderIoServices>) =>
    Effect.succeed({
      ...harness,
      makeProviderAdapterRegistryLayer: (transcript: Transcript, options) =>
        harness.makeProviderAdapterRegistryLayer(transcript, options).pipe(Layer.provide(services)),
    } satisfies OrchestratorV2ProviderReplayHarness<Transcript, Error>);

const decodeClaudeSettings = Schema.decodeEffect(ClaudeSettings);
const FAKE_CLAUDE_DIR = new URL("../../../../../tools/server-bench/fake-claude/", import.meta.url);

// The real Claude adapter and Agent SDK `query()` read a CLI's stream-json: P1-003's fake CLI
// playing the fixture's recorded turns with fresh ids.
const claudeWireHarness = (services: Layer.Layer<ProviderIoServices>, input: ReplayFixtureCase) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stateDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-fake-claude-state-" });
    const homeDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-fake-claude-home-" });
    const transcriptPath = yield* path.fromFileUrl(input.driver.transcriptFile).pipe(Effect.orDie);
    // The `claude` launcher is a POSIX shell script; on Windows the SDK runs the `.mjs` entry with node.
    const launcher = (yield* HostProcess.Platform) === "win32" ? "claude.mjs" : "claude";
    const binaryPath = yield* path
      .fromFileUrl(new URL(launcher, FAKE_CLAUDE_DIR))
      .pipe(Effect.orDie);
    const layerRunner = ClaudeAdapterV2.layerQueryRunner.pipe(
      Layer.provide(Layer.mergeAll(PortScanner.layerTest, services, NodeServices.layer)),
    );
    const layerRegistry = ProviderAdapterRegistry.layerFromAdaptersEffect(
      ClaudeAdapterV2.createClaudeAdapterV2(
        {
          instanceId: ClaudeAdapterV2.CLAUDE_DEFAULT_INSTANCE_ID,
          displayName: undefined,
          enabled: true,
          environment: [
            { name: "BENCH_TRANSCRIPT", value: transcriptPath, sensitive: false },
            { name: "BENCH_SPEED", value: "0", sensitive: false },
            { name: "BENCH_STATE_DIR", value: stateDir, sensitive: false },
            { name: "BENCH_NODE", value: process.execPath, sensitive: false },
          ],
          config: yield* decodeClaudeSettings({ binaryPath, homePath: homeDir }),
        },
        {},
      ).pipe(Effect.map((adapter) => [adapter])),
    ).pipe(
      Layer.provide(
        Layer.mergeAll(
          layerRunner,
          services,
          IdAllocator.layer,
          NodeServices.layer,
          Layer.effect(
            ServerConfig.ServerConfig,
            makeReplayServerConfig(input.fixtureName).pipe(Effect.orDie),
          ).pipe(Layer.provide(NodeServices.layer)),
        ),
      ),
    );
    return {
      driver: ClaudeAdapterV2.CLAUDE_PROVIDER,
      decodeTranscript: Effect.succeed,
      makeProviderAdapterRegistryLayer: () => layerRegistry.pipe(Layer.orDie),
    } satisfies OrchestratorV2ProviderReplayHarness<ProviderReplayTranscript, never>;
  }).pipe(Effect.orDie);

// The fake CLI does not play wake turns, permission prompts or a turn cut off mid-tool, so the
// fixtures that need them hang or diverge between runs (tools/server-bench/fake-claude/README.md).
const CLAUDE_WIRE_FIXTURES = new Set([
  "claude_idle_resume",
  "claude_local_bash_task",
  "claude_mcp_tool_presentation",
  "claude_nested_subagent_model",
  "claude_result_is_error",
  "command_output_streaming",
  "message_steering",
  "multi_turn",
  "multi_turn_restart",
  "queued_turn",
  "simple",
  "subagent",
  "thread_rollback",
  "tool_call_read_only",
  "tool_call_workspace_never",
  "web_search",
]);

// The replay fixtures' lines are mostly short, so one mode sends every line to the workers.
const LINE_OFFLOAD_MODES: ReadonlyArray<WorkerMode> = [
  { name: "2 workers", workers: 2 },
  { name: "2 workers, every line offloaded", workers: 2, offloadMinChars: 0 },
];

// The fixtures that stalled when a slow log append let an interrupt overtake the prompt.
const ACP_REAL_LOG_STORE_FIXTURES = new Set([
  "message_steering",
  "queued_turn",
  "simple",
  "turn_interrupt",
]);

const ROWS: ReadonlyArray<AdapterRow> = [
  adapterRow({
    driver: "codex",
    path: "replay",
    // P1-025 moves long Codex stdout lines onto the workers.
    offload: "required",
    stage: "codex-app-server",
    modes: LINE_OFFLOAD_MODES,
    harness: replayHarness(CodexOrchestratorReplayHarness),
    materializeWorkspace: (transcript, workspace) =>
      Effect.succeed(materializeReplayTranscriptWorkspace(transcript, workspace)),
  }),
  adapterRow({
    driver: "acpRegistry",
    path: "replay",
    // P1-025 moves long ACP stdout lines onto the workers.
    offload: "required",
    stage: "acp",
    modes: LINE_OFFLOAD_MODES,
    harness: replayHarness(AcpRegistryOrchestratorReplayHarness),
  }),
  adapterRow({
    driver: "acpRegistry",
    path: "replay, real log store",
    // End to end with the store the server uses: ACP logs a request before sending it (P1-069).
    offload: "required",
    stage: "acp",
    modes: LINE_OFFLOAD_MODES,
    fixtures: ACP_REAL_LOG_STORE_FIXTURES,
    realLogStore: true,
    harness: replayHarness(AcpRegistryOrchestratorReplayHarness),
  }),
  adapterRow({
    driver: "claudeAgent",
    path: "wire (fake CLI)",
    // P1-027 runs the Agent SDK on the workers; it names the stage it registers.
    offload: "pending",
    stage: "claude-agent-sdk",
    modes: [{ name: "2 workers", workers: 2 }],
    fixtures: CLAUDE_WIRE_FIXTURES,
    freshIds: true,
    checkOutput: false,
    harness: claudeWireHarness,
  }),
];

const CASES = ROWS.flatMap((row) =>
  ORCHESTRATOR_REPLAY_FIXTURES.flatMap((fixture) =>
    fixture.providers
      .filter((provider) => provider.driver === row.driver)
      .filter(() => row.fixtures === undefined || row.fixtures.has(fixture.name))
      .map((provider) => ({
        name: `${row.driver} ${row.path} ${fixture.name}`,
        row,
        input: { fixtureName: fixture.name, buildInput: fixture.buildInput, driver: provider },
      })),
  ),
);

describe("provider I/O at 0 and 2 workers", () => {
  // `%s` keeps the whole fixture name in the title, so `-t` can select it.
  it.effect.each(CASES.map((testCase) => [testCase.name, testCase] as const))(
    "%s: same adapter events, EventSink writes and protocol-log lines",
    ([, { row, input }]) =>
      Effect.gen(function* () {
        const inThread = yield* row.capture(input, { name: "0 workers", workers: 0 });
        assert.deepEqual(inThread.pool, {});
        assert.isNotEmpty(inThread.capture.adapterEvents.flat());
        assert.isNotEmpty(Object.keys(inThread.capture.logLines));
        assert.isNotEmpty(Object.keys(inThread.capture.sinkWrites));
        for (const mode of row.modes) {
          const onWorkers = yield* row.capture(input, mode);
          if (row.offload === "required") {
            const use = onWorkers.pool[row.stage];
            assert.isAbove(use?.sessions ?? 0, 0, `${mode.name}: workers took the sessions`);
            if (mode.offloadMinChars === 0) {
              assert.isAbove(use?.decoded ?? 0, 0, `${mode.name}: workers decoded the lines`);
            }
          }
          for (const channel of [
            "adapterEvents",
            "logLines",
            "sinkWrites",
            "firstWrites",
          ] as const) {
            assert.deepStrictEqual(
              onWorkers.capture[channel],
              inThread.capture[channel],
              `${mode.name}: ${channel}`,
            );
          }
        }
      }),
  );

  it("tells EventSink writes apart by their run and node, except a settle effect's provider-thread write", () => {
    const write = (
      commandId: string | null,
      type: string,
      attribution: { readonly runId?: string; readonly nodeId?: string },
    ) =>
      ({
        sequence: 1,
        commandId,
        event: {
          id: `event:${commandId}`,
          type,
          threadId: "t",
          payload: { id: "e" },
          ...attribution,
        },
      }) as unknown as OrchestrationV2StoredEvent;
    const sinkWrites = (writes: ReadonlyArray<OrchestrationV2StoredEvent>) =>
      normalize({ adapterEvents: [], logLines: [], sinkWrites: writes }, "/no-tmp-root", {
        freshIds: false,
        perRequest: false,
      }).sinkWrites;
    const ordinary = sinkWrites([write("c", "run.updated", { runId: "r1", nodeId: "n1" })]);
    assert.notDeepEqual(
      sinkWrites([write("c", "run.updated", { runId: "r2", nodeId: "n1" })]),
      ordinary,
    );
    assert.notDeepEqual(
      sinkWrites([write("c", "run.updated", { runId: "r1", nodeId: "n2" })]),
      ordinary,
    );
    assert.notDeepEqual(
      sinkWrites([write("c:background-work-settled", "run.updated", { runId: "r1" })]),
      sinkWrites([write(null, "run.updated", {})]),
    );
    assert.deepStrictEqual(
      sinkWrites([
        write("c:background-work-settled", "provider-thread.updated", {
          runId: "r1",
          nodeId: "n1",
        }),
      ]),
      sinkWrites([write(null, "provider-thread.updated", {})]),
    );
  });

  it("names only fixtures that have a Claude recording in the Claude wire row", () => {
    const claude = new Set(
      ORCHESTRATOR_REPLAY_FIXTURES.filter((fixture) =>
        fixture.providers.some((provider) => provider.driver === "claudeAgent"),
      ).map((fixture) => fixture.name),
    );
    assert.deepEqual(
      [...CLAUDE_WIRE_FIXTURES].filter((name) => !claude.has(name)),
      [],
    );
  });
});

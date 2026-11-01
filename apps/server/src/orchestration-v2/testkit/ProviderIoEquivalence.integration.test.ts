import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  ClaudeSettings,
  type OrchestrationV2StoredEvent,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
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
import * as PortScanner from "../../preview/PortScanner.ts";
import * as EventNdjsonLogger from "../../provider/EventNdjsonLogger.ts";
import * as ProviderIoWorkerPool from "../../provider/io/ProviderIoWorkerPool.ts";
import { AcpRegistryOrchestratorReplayHarness } from "../Adapters/AcpRegistryAdapterV2.testkit.ts";
import * as ClaudeAdapterV2 from "../Adapters/ClaudeAdapterV2.ts";
import { CodexOrchestratorReplayHarness } from "../Adapters/CodexAdapterV2.testkit.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "./fixtures/index.ts";
import { runReplayFixture, type ReplayFixtureCase } from "./ProviderReplayFixtures.ts";
import {
  makeReplayServerConfig,
  type OrchestratorV2ProviderReplayHarness,
} from "./ProviderReplayHarness.ts";

type ProviderIoServices =
  | ProviderEventLoggers.ProviderEventLoggers
  | ProviderIoWorkerPool.ProviderIoWorkerPool;

interface Capture {
  /** Per provider session, in open order. */
  readonly adapterEvents: ReadonlyArray<ReadonlyArray<unknown>>;
  readonly logLines: ReadonlyArray<unknown>;
  /** Per writer (see `bySource`), in order. */
  readonly sinkWrites: Readonly<Record<string, ReadonlyArray<unknown>>>;
}

interface CaptureRun {
  readonly capture: Capture;
  readonly poolSessions: number;
}

interface AdapterRow {
  readonly driver: string;
  readonly path: string;
  /**
   * `pending` until the provider's output runs on the workers: the row still compares 0 and 2
   * workers. `required` also fails unless 2 workers took the provider's sessions.
   */
  readonly offload: "pending" | "required";
  readonly fixtures?: ReadonlySet<string>;
  readonly capture: (input: ReplayFixtureCase, workers: number) => Effect.Effect<CaptureRun>;
}

const ISO_TIMESTAMP = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z/gu;
const LOG_LINE = /^\[[^\]]*\] ([A-Z]+): (.*)$/u;
const UUID_V4 = /[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gu;
const ANY_UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/giu;
const API_ID = /\b(?:msg|toolu|srvtoolu|req)_[A-Za-z0-9]{8,}/gu;
const TASK_ID = /\\?"(?:task_id|taskId|agent_id|agentId)\\?":\\?"([^"\\]+)\\?"/gu;
const DURATION_MS = /(\\?"duration_ms\\?":)\d+/gu;

// Independent writers interleave differently between runs even in-thread, so EventSink writes are
// compared per writer: each command; the session manager's own session-status writes; and the run
// events it hands to run execution (ProviderSessionManager event pump).
const bySource = (storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>) => {
  const sources = new Map<string, Array<unknown>>();
  for (const { sequence: _sequence, ...write } of storedEvents) {
    const source =
      write.commandId ??
      (write.event.type.startsWith("provider-session.") ? "provider sessions" : "provider runs");
    sources.set(source, [...(sources.get(source) ?? []), write]);
  }
  return Object.fromEntries([...sources].toSorted(([a], [b]) => (a < b ? -1 : 1)));
};

// Timestamps, per-run temporary directory names and random ids are the only run-specific values.
// Random ids (v4 UUIDs; with `freshIds` also the ids a fake CLI mints) are renamed by first
// appearance across the channels, so where each id recurs is still compared.
const normalize = (
  raw: {
    readonly adapterEvents: ReadonlyArray<ReadonlyArray<unknown>>;
    readonly logLines: ReadonlyArray<unknown>;
    readonly sinkWrites: ReadonlyArray<OrchestrationV2StoredEvent>;
  },
  tmpRoot: string,
  freshIds: boolean,
): Capture => {
  const tmpDirectory = new RegExp(
    `(?:/private)?${tmpRoot.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&")}/[^/\\\\\\s"']+`,
    "gu",
  );
  const scrub = (value: unknown) => {
    const text = JSON.stringify(value)
      .replace(tmpDirectory, "<tmp>")
      .replace(ISO_TIMESTAMP, "<timestamp>");
    return freshIds ? text.replace(DURATION_MS, (_match, key: string) => `${key}0`) : text;
  };
  const texts = [scrub(raw.adapterEvents), scrub(raw.logLines), scrub(bySource(raw.sinkWrites))];
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
  const [adapterEvents, logLines, sinkWrites] = texts.map((text) =>
    JSON.parse(renames.reduce((renamed, [id, alias]) => renamed.replaceAll(id, alias), text)),
  );
  return { adapterEvents, logLines, sinkWrites };
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
  readonly fixtures?: ReadonlySet<string>;
  readonly freshIds?: boolean;
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
  ...(row.fixtures === undefined ? {} : { fixtures: row.fixtures }),
  capture: (input, workers) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const logDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-provider-io-equivalence-",
      });
      const store = yield* EventNdjsonLogger.makeEventNdjsonLogStore(
        path.join(logDir, "events.log"),
        { batchWindowMs: 0 },
      );
      const pool = yield* ProviderIoWorkerPool.make({ workers });
      let poolSessions = 0;
      const services = Layer.mergeAll(
        Layer.succeed(ProviderEventLoggers.ProviderEventLoggers, {
          native: store.logger("native"),
          canonical: undefined,
        }),
        Layer.succeed(
          ProviderIoWorkerPool.ProviderIoWorkerPool,
          ProviderIoWorkerPool.ProviderIoWorkerPool.of({
            open: (openInput) =>
              pool.open(openInput).pipe(
                Effect.tap((session) =>
                  Effect.sync(() => {
                    if (Option.isSome(session)) poolSessions += 1;
                  }),
                ),
              ),
          }),
        ),
      );
      const harness = yield* row.harness(services, input);
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
      });
      yield* store.close();
      const logLines: Array<unknown> = [];
      for (const file of (yield* fileSystem.readDirectory(logDir)).toSorted()) {
        const contents = yield* fileSystem.readFileString(path.join(logDir, file));
        for (const line of contents.split("\n")) {
          if (line.length === 0) continue;
          const [, stream, payload] = LOG_LINE.exec(line) ?? [];
          assert.isDefined(payload, `log line format: ${line}`);
          logLines.push({ file, stream, record: JSON.parse(payload) });
        }
      }
      const capture = normalize(
        { adapterEvents, logLines, sinkWrites: result.storedEvents },
        path.dirname(logDir),
        row.freshIds === true,
      );
      return { capture, poolSessions };
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
const FAKE_CLAUDE = new URL(
  "../../../../../tools/server-bench/fake-claude/claude",
  import.meta.url,
);

// The real Claude adapter and Agent SDK `query()` read a CLI's stream-json: P1-003's fake CLI
// playing the fixture's recorded turns with fresh ids.
const claudeWireHarness = (services: Layer.Layer<ProviderIoServices>, input: ReplayFixtureCase) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stateDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-fake-claude-state-" });
    const homeDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-fake-claude-home-" });
    const transcriptPath = yield* path.fromFileUrl(input.driver.transcriptFile).pipe(Effect.orDie);
    const binaryPath = yield* path.fromFileUrl(FAKE_CLAUDE).pipe(Effect.orDie);
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

const ROWS: ReadonlyArray<AdapterRow> = [
  adapterRow({
    driver: "codex",
    path: "replay",
    // P1-025 moves Codex stdout handling onto the workers.
    offload: "pending",
    harness: replayHarness(CodexOrchestratorReplayHarness),
    materializeWorkspace: (transcript, workspace) =>
      Effect.succeed(materializeReplayTranscriptWorkspace(transcript, workspace)),
  }),
  adapterRow({
    driver: "acpRegistry",
    path: "replay",
    // P1-025 moves ACP stdout handling onto the workers.
    offload: "pending",
    harness: replayHarness(AcpRegistryOrchestratorReplayHarness),
  }),
  adapterRow({
    driver: "claudeAgent",
    path: "wire (fake CLI)",
    // P1-027 runs the Agent SDK on the workers.
    offload: "pending",
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
  it.effect.each(CASES)(
    "$name: same adapter events, EventSink writes and protocol-log lines",
    ({ row, input }) =>
      Effect.gen(function* () {
        const inThread = yield* row.capture(input, 0);
        const onWorkers = yield* row.capture(input, 2);
        assert.equal(inThread.poolSessions, 0);
        if (row.offload === "required") {
          assert.isAbove(onWorkers.poolSessions, 0, "2 workers took the provider's sessions");
        }
        assert.isNotEmpty(inThread.capture.adapterEvents.flat());
        assert.isNotEmpty(inThread.capture.logLines);
        assert.isNotEmpty(Object.keys(inThread.capture.sinkWrites));
        assert.deepStrictEqual(onWorkers.capture.adapterEvents, inThread.capture.adapterEvents);
        assert.deepStrictEqual(onWorkers.capture.logLines, inThread.capture.logLines);
        assert.deepStrictEqual(onWorkers.capture.sinkWrites, inThread.capture.sinkWrites);
      }),
  );

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

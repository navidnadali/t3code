import { assert, describe, it } from "@effect/vitest";
import type { OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as ProviderIoWorkerPool from "../../provider/io/ProviderIoWorkerPool.ts";
import {
  type ClaudeWireReplayRecording,
  makeClaudeWireReplayHarness,
} from "../Adapters/ClaudeAgentSdkWireReplay.testkit.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "./fixtures/index.ts";
import { runReplayFixture } from "./ProviderReplayFixtures.ts";

// The CLI asks these for a permission, which the worker relays to main.
const PERMISSION_FIXTURES = new Set([
  "tool_call_denied_write",
  "tool_call_read_only_on_request",
  "tool_call_restricted_granular",
]);

// Timing races outside the SDK, which the worker's extra hop tips the other way (adapter events still
// match exactly): two orchestration writers that both write without a command interleave their
// stored events differently, and a frame the CLI sends after the turn's result races teardown.
const INTERLEAVED_WRITERS = new Set(["claude_background_subagent_lifecycle"]);
const TRAILING_FRAME_RACE = new Set(["claude_compact_after_peer_turn"]);

const CLAUDE_FIXTURES = ORCHESTRATOR_REPLAY_FIXTURES.flatMap((fixture) =>
  fixture.providers
    .filter((provider) => provider.driver === "claudeAgent")
    .map((provider) => [fixture.name, fixture, provider] as const),
);

type Fixture = (typeof CLAUDE_FIXTURES)[number];

// Each run has its own temp directories and fresh prompt ids, and the scenario's debounce moves the
// test clock by however many late frames it saw; none of that is what the provider did.
const TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/;
const RUN_DIRECTORY =
  /[^\s"']*\/t3-(?:orchestrator-v2|orchestration-v2-replay)-[\w-]*?-[A-Za-z0-9]{6}(?=[/\s"']|$)/g;
const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g;
const comparable = (
  value: unknown,
  options: { readonly numberUuids: boolean } = { numberUuids: true },
) => {
  const uuids = new Map<string, string>();
  const normalize = (entry: unknown): unknown => {
    if (typeof entry === "string") {
      if (TIMESTAMP.test(entry)) return "<time>";
      return entry.replace(RUN_DIRECTORY, "<run-dir>").replace(UUID, (uuid) => {
        if (!options.numberUuids) return "<uuid>";
        let stable = uuids.get(uuid);
        if (stable === undefined) {
          stable = `<uuid-${uuids.size}>`;
          uuids.set(uuid, stable);
        }
        return stable;
      });
    }
    if (Array.isArray(entry)) return entry.map(normalize);
    if (typeof entry === "object" && entry !== null) {
      return Object.fromEntries(
        Object.entries(entry).map(([key, field]) => [normalize(key), normalize(field)]),
      );
    }
    return entry;
  };
  return normalize(JSON.parse(JSON.stringify(value)));
};

// Writers that run independently (provider ingestion, each command) interleave by timing in-thread
// too, so each one's own sequence is what must match; their ids are random, so only their shape is.
const byWriter = (
  stored: ReadonlyArray<OrchestrationV2StoredEvent>,
  options: { readonly ordered: boolean },
) => {
  const writers = new Map<string, Array<unknown>>();
  for (const { commandId, event } of stored) {
    const writer = `${commandId ?? "provider"}|${event.threadId}`;
    writers.set(writer, [...(writers.get(writer) ?? []), event]);
  }
  const grouped = comparable(Object.fromEntries(writers), { numberUuids: false }) as Record<
    string,
    unknown
  >;
  return Object.keys(grouped)
    .toSorted()
    .map((writer) => {
      const events = grouped[writer] as ReadonlyArray<unknown>;
      return [
        writer,
        options.ordered ? events : events.map((event) => JSON.stringify(event)).toSorted(),
      ];
    });
};

// What T3 sent and what the CLI sent each keep their order; when a control call's completion is
// logged relative to frames the CLI sent around its answer is up to the scheduler.
const byDirection = (logs: ReadonlyArray<unknown>) => {
  const normalized = comparable(logs) as ReadonlyArray<{
    readonly event: { readonly direction: string };
  }>;
  return {
    incoming: normalized.filter((line) => line.event.direction === "incoming"),
    outgoing: normalized.filter((line) => line.event.direction === "outgoing"),
  };
};

const runAtWireLevel = ([name, fixture, provider]: Fixture, workers: 0 | 2) =>
  Effect.gen(function* () {
    const recording: ClaudeWireReplayRecording = { adapterEvents: [], protocolLogs: [] };
    const opens: Array<boolean> = [];
    let pool: ProviderIoWorkerPool.ProviderIoWorkerPool["Service"] | undefined;
    if (workers > 0) {
      const workerPool = yield* ProviderIoWorkerPool.make({ workers });
      pool = {
        open: (input) =>
          workerPool
            .open(input)
            .pipe(Effect.tap((opened) => Effect.sync(() => opens.push(Option.isSome(opened))))),
      };
    }
    const result = yield* runReplayFixture({
      fixtureName: name,
      buildInput: fixture.buildInput,
      driver: provider,
      harness: makeClaudeWireReplayHarness({ pool, recording }),
    });
    return { result, recording, opens };
  }).pipe(Effect.scoped);

describe("Claude replay fixtures at the SDK wire level, in-thread and on provider I/O workers", () => {
  it.effect.each(CLAUDE_FIXTURES)("replays %s identically with the SDK on 2 workers", (fixture) =>
    Effect.gen(function* () {
      const [name] = fixture;
      const inThread = yield* runAtWireLevel(fixture, 0);
      const inWorker = yield* runAtWireLevel(fixture, 2);

      assert.isAbove(inWorker.opens.length, 0);
      assert.isTrue(inWorker.opens.every((opened) => opened));
      assert.isAbove(inThread.recording.adapterEvents.length, 0);
      if (PERMISSION_FIXTURES.has(name)) {
        assert.isTrue(
          inThread.recording.adapterEvents.some(
            (event) => event.type === "runtime_request.updated",
          ),
        );
      }
      assert.deepStrictEqual(
        comparable(inWorker.recording.adapterEvents),
        comparable(inThread.recording.adapterEvents),
      );
      const ordered = !INTERLEAVED_WRITERS.has(name);
      assert.deepStrictEqual(
        byWriter(inWorker.result.storedEvents, { ordered }),
        byWriter(inThread.result.storedEvents, { ordered }),
      );
      if (!TRAILING_FRAME_RACE.has(name)) {
        assert.deepStrictEqual(
          byDirection(inWorker.recording.protocolLogs),
          byDirection(inThread.recording.protocolLogs),
        );
      }
    }),
  );
});

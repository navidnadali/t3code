import { assert, describe, it } from "@effect/vitest";
import type { OrchestrationV2DomainEvent, OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as ProviderIoWorkerPool from "../../provider/io/ProviderIoWorkerPool.ts";
import {
  type ClaudeWireReplayRecording,
  makeClaudeWireReplayHarness,
} from "../Adapters/ClaudeAgentSdkWireReplay.testkit.ts";
import { ORCHESTRATOR_REPLAY_FIXTURES } from "./fixtures/index.ts";
import { runReplayFixture } from "./ProviderReplayFixtures.ts";

// The CLI asks these for a permission, which a worker refuses until P1-028 relays it to main.
const PERMISSION_FIXTURES = new Set([
  "tool_call_denied_write",
  "tool_call_read_only_on_request",
  "tool_call_restricted_granular",
]);

// Races outside the SDK that the worker's extra hop tips the other way (adapter events match): two
// commandless writers interleave differently, and a frame sent after the result reaches the adapter
// a round trip later, after teardown stopped the reader.
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

// Each writer's own sequence must match: independent writers interleave by timing in-thread too.
// Known in-thread race: an interrupt's effect and ingestion both record a stopped run, so those
// records are compared per thread whoever wrote them, and an update repeating the last state is
// dropped (the thread's unchanged state may be written once more).
const INTERRUPTION_RECORDS = new Set([
  "run-attempt.updated",
  "run.updated",
  "node.updated",
  "turn-item.updated",
]);
const recordsInterruption = (commandId: string | null, event: OrchestrationV2DomainEvent) =>
  (commandId === null || commandId.includes(":provider-turn.interrupt:")) &&
  INTERRUPTION_RECORDS.has(event.type) &&
  (event.payload as { readonly status?: unknown }).status === "interrupted";

const byWriter = (
  stored: ReadonlyArray<OrchestrationV2StoredEvent>,
  options: { readonly ordered: boolean },
) => {
  const writers = new Map<string, Array<unknown>>();
  const add = (writer: string, event: unknown) =>
    writers.set(writer, [...(writers.get(writer) ?? []), event]);
  for (const { commandId, event } of stored) {
    if (recordsInterruption(commandId, event)) {
      add(`interruption|${event.threadId}`, { ...event, id: "<recorded by either writer>" });
    } else {
      add(`${commandId ?? "provider"}|${event.threadId}`, event);
    }
  }
  const grouped = comparable(Object.fromEntries(writers), { numberUuids: false }) as Record<
    string,
    ReadonlyArray<{ readonly type: string; readonly payload: unknown }>
  >;
  const state = (event: { readonly type: string; readonly payload: unknown }) =>
    JSON.stringify([event.type, event.payload]);
  // Turn-item ordinals come from one counter in arrival order across writers: compare their rank.
  const ranked = (written: ReadonlyArray<{ readonly type: string; readonly payload: unknown }>) => {
    const ordinalOf = (event: { readonly type: string; readonly payload: unknown }) =>
      event.type === "turn-item.updated"
        ? (event.payload as { readonly ordinal?: unknown }).ordinal
        : undefined;
    const ordinals = [
      ...new Set(written.map(ordinalOf).filter((ordinal) => typeof ordinal === "number")),
    ].toSorted((left, right) => left - right);
    return written.map((event) => {
      const ordinal = ordinalOf(event);
      if (typeof ordinal !== "number") return event;
      const payload = {
        ...(event.payload as object),
        ordinal: `rank ${ordinals.indexOf(ordinal)}`,
      };
      return { ...event, payload };
    });
  };
  return Object.keys(grouped)
    .toSorted()
    .map((writer) => {
      const written = ranked(grouped[writer]!);
      const changes = written
        .filter((event, index) => index === 0 || state(event) !== state(written[index - 1]!))
        .map((event) => JSON.stringify(event));
      return [
        writer,
        options.ordered && !writer.startsWith("interruption|") ? changes : changes.toSorted(),
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
  it.effect.each(CLAUDE_FIXTURES.filter(([name]) => !PERMISSION_FIXTURES.has(name)))(
    "replays %s identically with the SDK on 2 workers",
    (fixture) =>
      Effect.gen(function* () {
        const [name] = fixture;
        const inThread = yield* runAtWireLevel(fixture, 0);
        const inWorker = yield* runAtWireLevel(fixture, 2);

        assert.isAbove(inWorker.opens.length, 0);
        assert.isTrue(inWorker.opens.every((opened) => opened));
        assert.isAbove(inThread.recording.adapterEvents.length, 0);
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

  it.effect.each(CLAUDE_FIXTURES.filter(([name]) => PERMISSION_FIXTURES.has(name)))(
    "replays %s in-thread at the wire level (its permission waits for P1-028 on a worker)",
    (fixture) =>
      Effect.gen(function* () {
        const inThread = yield* runAtWireLevel(fixture, 0);
        assert.isAbove(inThread.recording.adapterEvents.length, 0);
      }),
  );
});

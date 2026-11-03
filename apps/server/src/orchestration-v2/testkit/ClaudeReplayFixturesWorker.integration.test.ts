import { assert, describe, it } from "@effect/vitest";
import type {
  OrchestrationV2StoredEvent,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as ProviderIoWorkerPool from "../../provider/io/ProviderIoWorkerPool.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import { TURN_ITEM_ORDINALS_PER_RUN } from "../TurnItemPositionStore.ts";
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

// A race outside the SDK that the worker's extra hop tips the other way (adapter events match): a
// frame sent after the result reaches the adapter a round trip later, after teardown stopped the
// reader.
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

// Left open by the product, so compared per entity only: Stop's settle (when the interrupt call
// returns) and the stopped turn's own end (its run's consumer or finalization, when the CLI exits)
// both record the stopped run and settle its provider thread; the second finds it done.
const STOP_COMMAND = ":provider-turn.interrupt:";
const INTERRUPTION_RECORDS = new Set([
  "run-attempt.updated",
  "run.updated",
  "node.updated",
  "turn-item.updated",
]);

type StoredRecord = {
  readonly commandId: string | null;
  readonly event: {
    readonly type: string;
    readonly threadId: string;
    readonly runId?: string;
    readonly payload: Record<string, unknown>;
  };
};

// Turn-item ordinals come from one counter per run band, allocated in commit order across writers
// (TurnItemPositionStore): compare their rank within a writer.
const ranked = (records: ReadonlyArray<StoredRecord["event"]>) => {
  const ordinalOf = (event: StoredRecord["event"]) =>
    event.type === "turn-item.updated" ? event.payload.ordinal : undefined;
  const ordinals = [
    ...new Set(records.map(ordinalOf).filter((ordinal) => typeof ordinal === "number")),
  ].toSorted((left, right) => left - right);
  return records.map((event) => {
    const ordinal = ordinalOf(event);
    if (typeof ordinal !== "number") return event;
    return {
      ...event,
      payload: { ...event.payload, ordinal: `rank ${ordinals.indexOf(ordinal)}` },
    };
  });
};

// The UUIDs a store's records carry, event ids left out (each event's id is new and random).
const recordsOf = (stored: ReadonlyArray<OrchestrationV2StoredEvent>) =>
  stored.map(({ commandId, event }) => {
    const { id: _id, ...rest } = event;
    return JSON.parse(JSON.stringify({ commandId, event: rest })) as StoredRecord;
  });
const uuidsOf = (value: unknown) => new Set(JSON.stringify(value).match(UUID) ?? []);

// Timestamps and run directories normalized. A UUID both runs carry stays as it is, so entities and
// references stay distinct whatever order they arrive in. One only this run made is named by its least
// place in content (type, thread, run, entity, field; per-run UUIDs masked) and an index, which orders
// UUIDs sharing that place by the distinct records carrying each (masked, ordinals out), then arrival.
const normalizer = (records: ReadonlyArray<StoredRecord>, shared: ReadonlySet<string>) => {
  const mask = (text: string) => text.replace(UUID, (uuid) => (shared.has(uuid) ? uuid : "<uuid>"));
  // What a record states, whoever wrote it (a thread update by its payload), as the entity view has it;
  // per-run UUIDs are masked by the caller, each UUID's own told apart from the others.
  const content = ({ event }: StoredRecord) =>
    JSON.stringify(
      event.type === "provider-thread.updated"
        ? { type: event.type, threadId: event.threadId, payload: event.payload }
        : event,
      (key, value: unknown) =>
        key === "ordinal"
          ? undefined
          : typeof value === "string" && TIMESTAMP.test(value)
            ? "<time>"
            : value,
    ).replace(RUN_DIRECTORY, "<run-dir>");
  const places = new Map<string, string>();
  const histories = new Map<string, Array<string>>();
  const arrival: Array<string> = [];
  for (const record of records) {
    const { type, threadId, runId, payload } = record.event;
    const carrier = mask([type, threadId, runId ?? "", String(payload.id)].join(" | "));
    const recordContent = content(record);
    const carried = new Set<string>();
    const visit = (entry: unknown, field: string) => {
      if (typeof entry === "string") {
        for (const uuid of entry.match(UUID) ?? []) {
          if (shared.has(uuid)) continue;
          const place = `${carrier} | ${field}`;
          const known = places.get(uuid);
          if (known === undefined) arrival.push(uuid);
          if (known === undefined || place < known) places.set(uuid, place);
          carried.add(uuid);
        }
      } else if (Array.isArray(entry)) {
        entry.forEach((item, index) => visit(item, `${field}[${index}]`));
      } else if (typeof entry === "object" && entry !== null) {
        for (const [key, value] of Object.entries(entry)) {
          visit(key, `${field}{}`);
          visit(value, `${field}.${key}`);
        }
      }
    };
    visit(record, "");
    for (const uuid of carried) {
      const own = recordContent.replace(UUID, (other) =>
        shared.has(other) ? other : other === uuid ? "<self>" : "<uuid>",
      );
      histories.set(uuid, [...(histories.get(uuid) ?? []), own]);
    }
  }
  const history = new Map(
    // Distinct records: a repeated write, which the comparison skips, does not change the order.
    [...histories].map(([uuid, contents]) => [
      uuid,
      JSON.stringify([...new Set(contents)].toSorted()),
    ]),
  );
  const tied = new Map<string, Array<string>>();
  for (const uuid of arrival) {
    const place = places.get(uuid)!;
    tied.set(place, [...(tied.get(place) ?? []), uuid]);
  }
  // Every name ends in its index, so a field key that reads like one ("x #0") cannot take it.
  const named = new Map(
    [...tied].flatMap(([place, uuids]) =>
      uuids
        .toSorted((left, right) => {
          const [a, b] = [history.get(left)!, history.get(right)!];
          return a < b ? -1 : a > b ? 1 : 0;
        })
        .map((uuid, index) => [uuid, `<uuid ${place} #${index}>`] as const),
    ),
  );
  const normalize = (entry: unknown): unknown => {
    if (typeof entry === "string") {
      if (TIMESTAMP.test(entry)) return "<time>";
      return entry
        .replace(RUN_DIRECTORY, "<run-dir>")
        .replace(UUID, (uuid) => named.get(uuid) ?? uuid);
    }
    if (Array.isArray(entry)) return entry.map(normalize);
    if (typeof entry === "object" && entry !== null) {
      return Object.fromEntries(
        Object.entries(entry).map(([key, field]) => [normalize(key), normalize(field)]),
      );
    }
    return entry;
  };
  return (value: unknown) => normalize(JSON.parse(JSON.stringify(value)));
};

// Stored events compared by each thread's final projection, per entity (its states in order,
// whoever wrote them), per ordinal band (items in ordinal order) and per writer (each command's, and
// each run's commandless records, in order), skipping what no commit's end state shows: a write
// repeating its entity's record, or, in Stop's settle commit, one that commit overwrites.
const comparableStore = (
  stored: ReadonlyArray<OrchestrationV2StoredEvent>,
  shared: ReadonlySet<string>,
) => {
  const raw = recordsOf(stored);
  const normalize = normalizer(raw, shared);
  const records = raw.map(normalize) as ReadonlyArray<StoredRecord>;
  const entityOf = ({ event }: StoredRecord) =>
    `${event.threadId}|${event.type}|${String(event.payload.id)}`;

  // A thread update comes through its run (consumer, finalization, Stop; each its own envelope) or
  // the roster, so only its payload and per-thread order count, in every fixture: a state change may
  // land up to that thread's next state, past another run's records (41 in subagent_lifecycle), and
  // a client may show the thread's earlier state for those commits.
  const recorded = ({ event }: StoredRecord) =>
    event.type === "provider-thread.updated"
      ? { type: event.type, threadId: event.threadId, payload: event.payload }
      : event;

  // Whether a write changes the thread's projection is the reducer's call (ProjectionStore): a
  // rewrite with a side effect elsewhere, such as the app thread's active provider thread, counts.
  const projections = new Map<string, OrchestrationV2ThreadProjection>();
  const projected = (projection: OrchestrationV2ThreadProjection | undefined) =>
    JSON.stringify(
      projection === undefined
        ? null
        : { ...projection, updatedAt: null, thread: { ...projection.thread, updatedAt: null } },
      (_key, value: unknown) =>
        typeof value === "string" && TIMESTAMP.test(value) ? "<time>" : value,
    );
  // The projection after a write, or undefined when the reducer cannot take it (then it counts).
  const next = (
    projection: OrchestrationV2ThreadProjection | undefined,
    { event }: OrchestrationV2StoredEvent,
  ) => {
    try {
      return event.type === "thread.created"
        ? ProjectionStore.emptyProjection(event)
        : projection === undefined
          ? undefined
          : ProjectionStore.applyToProjection(projection, event);
    } catch {
      return undefined;
    }
  };
  // Each commit (a command's records, or one commandless record) skips a write that repeats its
  // entity's record, or in Stop's settle one the commit overwrites, only if the commit then ends
  // with the projection it ends with in full. Readers acting on each event see other commands'.
  const lastStates = new Map<string, string>();
  const writes: Array<StoredRecord> = [];
  for (let start = 0; start < records.length;) {
    const commandId = records[start]!.commandId;
    let end = start;
    while (commandId !== null && records[end + 1]?.commandId === commandId) end++;
    const commitEnd = (threadId: string, skipped: ReadonlySet<number>) => {
      let projection = projections.get(threadId);
      for (let at = start; at <= end; at++) {
        if (skipped.has(at) || stored[at]!.event.threadId !== threadId) continue;
        projection = next(projection, stored[at]!);
        if (projection === undefined) return undefined;
      }
      return projected(projection);
    };
    const settles = commandId?.includes(STOP_COMMAND) === true;
    const full = new Map<string, string | undefined>();
    const skipped = new Set<number>();
    for (let at = start; at <= end; at++) {
      const record = records[at]!;
      const entity = entityOf(record);
      const state = JSON.stringify(recorded(record));
      const overwritten =
        settles && records.slice(at + 1, end + 1).some((later) => entityOf(later) === entity);
      if (overwritten || lastStates.get(entity) === state) {
        const threadId = stored[at]!.event.threadId;
        if (!full.has(threadId)) full.set(threadId, commitEnd(threadId, new Set()));
        const ends = full.get(threadId);
        if (ends !== undefined && ends === commitEnd(threadId, new Set([...skipped, at]))) {
          skipped.add(at);
          continue;
        }
      }
      lastStates.set(entity, state);
      writes.push(record);
    }
    for (let at = start; at <= end; at++) {
      const threadId = stored[at]!.event.threadId;
      const projection = next(projections.get(threadId), stored[at]!);
      if (projection !== undefined) projections.set(threadId, projection);
    }
    start = end + 1;
  }

  const entities = new Map<string, Array<unknown>>();
  for (const record of writes) {
    const state = recorded(record) as { readonly type: string; readonly payload: object };
    const { ordinal: _ordinal, ...payload } = state.payload as Record<string, unknown>;
    const entity = entityOf(record);
    entities.set(entity, [
      ...(entities.get(entity) ?? []),
      state.type === "turn-item.updated" ? { ...state, payload } : state,
    ]);
  }

  // A band (a run's items in its thread; a subagent thread's, all runs, in band 0) keeps the order
  // of items its runs' own writers add, ties included: readers order by ordinal. A command's item
  // (prompt, Stop request) against them takes its place in commit order: compared within its command.
  const bandItems = new Map<string, Map<string, number>>();
  for (const { commandId, event } of writes) {
    const ordinal = event.payload.ordinal;
    const ownWriter = commandId === null || commandId.includes(STOP_COMMAND);
    if (event.type !== "turn-item.updated" || !ownWriter || typeof ordinal !== "number") continue;
    const band = `${event.threadId}|band ${Math.floor(ordinal / TURN_ITEM_ORDINALS_PER_RUN)}`;
    bandItems.set(band, (bandItems.get(band) ?? new Map()).set(String(event.payload.id), ordinal));
  }

  // Each run consumes the session's events itself, and a settled run's consumer lives on while it
  // owns background work, so two runs' records interleave by scheduling. Runless (session) records
  // and thread updates are compared per entity only; a run's order across threads is not compared.
  const writers = new Map<string, Array<StoredRecord["event"]>>();
  for (const { commandId, event } of writes) {
    if (commandId?.includes(STOP_COMMAND) === true) continue;
    if (event.type === "provider-thread.updated") continue;
    if (
      commandId === null &&
      (event.runId === undefined ||
        (INTERRUPTION_RECORDS.has(event.type) && event.payload.status === "interrupted"))
    ) {
      continue;
    }
    const writer = `${commandId ?? `provider ${event.runId}`}|${event.threadId}`;
    writers.set(writer, [...(writers.get(writer) ?? []), event]);
  }
  const byKey = <Value>(entries: Iterable<readonly [string, Value]>) =>
    [...entries].toSorted(([left], [right]) => left.localeCompare(right));
  // Each thread's projection after every write: collections in id order (they keep arrival order)
  // and turn items without ordinals or positions (the band and writer views compare their order).
  const byId = (value: unknown): unknown => {
    if (Array.isArray(value)) {
      const items = value.map(byId);
      const idOf = (entry: unknown) =>
        typeof entry !== "object" || entry === null
          ? undefined
          : ((entry as { id?: unknown }).id ?? (entry as { sourceItemId?: unknown }).sourceItemId);
      return items.every((entry) => idOf(entry) !== undefined)
        ? items.toSorted((left, right) => String(idOf(left)).localeCompare(String(idOf(right))))
        : items;
    }
    if (typeof value !== "object" || value === null) return value;
    return Object.fromEntries(Object.entries(value).map(([key, field]) => [key, byId(field)]));
  };
  const settled = [...projections].map(([thread, projection]) => {
    const unordered = (items: ReadonlyArray<object>): ReadonlyArray<object> =>
      items.map((entry) => {
        const {
          ordinal: _ordinal,
          position: _position,
          ...rest
        } = entry as Record<string, unknown>;
        const item = rest.item;
        return typeof item === "object" && item !== null
          ? { ...rest, item: unordered([item])[0] }
          : rest;
      });
    const final = {
      ...projection,
      turnItems: unordered(projection.turnItems),
      visibleTurnItems: unordered(projection.visibleTurnItems),
    };
    return [normalize(thread) as string, byId(normalize(final))] as const;
  });
  return {
    projections: byKey(settled),
    entities: byKey(entities),
    bandItems: byKey(bandItems).map(([band, items]) => [
      band,
      [...new Set(items.values())]
        .toSorted((left, right) => left - right)
        .map((ordinal) =>
          [...items]
            .filter(([, itemOrdinal]) => itemOrdinal === ordinal)
            .map(([item]) => item)
            .toSorted(),
        ),
    ]),
    writers: byKey(writers).map(([writer, events]) => [
      writer,
      ranked(events).map((event) => JSON.stringify(event)),
    ]),
  };
};

// Two runs' stores, compared with the UUIDs both carry kept as they are.
const comparableStores = (
  left: ReadonlyArray<OrchestrationV2StoredEvent>,
  right: ReadonlyArray<OrchestrationV2StoredEvent>,
) => {
  const theirs = uuidsOf(recordsOf(right));
  const shared = new Set([...uuidsOf(recordsOf(left))].filter((uuid) => theirs.has(uuid)));
  return [comparableStore(left, shared), comparableStore(right, shared)] as const;
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

describe("Claude replay comparison", () => {
  const comparePair = comparableStores;
  const threadId = "thread:compare";
  const stop = "effect:command:interrupt-1:provider-turn.interrupt:turn-1:background-work-settled";
  const run1 = { runId: "run:compare:1", nodeId: "node:compare:run-1" };
  const run2 = { runId: "run:compare:2", nodeId: "node:compare:run-2" };
  const run3 = { runId: "run:compare:3", nodeId: "node:compare:run-3" };
  type Envelope = { readonly runId?: string; readonly nodeId?: string };
  const record = (
    type: string,
    id: string,
    status: string,
    envelope: Envelope = {},
    payload: Record<string, unknown> = {},
  ) => ({ type, threadId, ...envelope, payload: { id, status, ...payload } });
  const task = { taskId: "bt1", kind: "command", description: "Background sleep task" };
  const providerThread = (
    status: string,
    tasks: number,
    envelope: Envelope = {},
    id = "provider-thread:compare",
  ) =>
    record("provider-thread.updated", id, status, envelope, {
      appThreadId: threadId,
      pendingBackgroundTasks: tasks === 0 ? [] : [task],
      updatedAt: "2026-11-03T00:00:00.000Z",
    });
  const turn = (status: string, envelope: Envelope) =>
    record("provider-turn.updated", `provider-turn:${envelope.runId}`, status, envelope);
  const item = (id: string, status: string, envelope: Envelope, ordinal: number) =>
    record("turn-item.updated", id, status, envelope, { ordinal });
  const runRecord = (status: string, run: typeof run1) =>
    record("run.updated", run.runId, status, run, {
      providerThreadId: "provider-thread:compare",
      rootNodeId: run.nodeId,
    });
  const started = [null, runRecord("running", run1)] as const;
  const created = [
    "command:thread-create",
    { type: "thread.created", threadId, payload: { id: threadId, activeProviderThreadId: null } },
  ] as const;
  const store = (
    writes: ReadonlyArray<readonly [string | null, Record<string, unknown>]>,
  ): ReadonlyArray<OrchestrationV2StoredEvent> =>
    [created, ...writes].map(([commandId, event], index) => {
      const uuid = `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`;
      const writer = commandId === null ? "provider-session:s" : `command:${commandId}`;
      const id = `event:${threadId}:${writer}:${uuid}`;
      return { sequence: index + 1, commandId, event: { ...event, id } } as never;
    });

  // claude_background_task_interrupt: Stop's settle (when the interrupt call returns) and the
  // provider's turn end (when the CLI exits) both settle the thread; the second finds it done.
  it("compares a stopped turn's provider-thread settlement whichever writer records it", () => {
    assert.deepStrictEqual(
      ...comparePair(
        store([
          started,
          [null, providerThread("active", 1)],
          [stop, providerThread("idle", 1, run1)],
          // Stop's settle writes the stopped run between the thread's two states, in one commit.
          [stop, runRecord("interrupted", run1)],
          [stop, providerThread("idle", 0, run1)],
          [null, turn("interrupted", run1)],
        ]),
        store([
          started,
          [null, providerThread("active", 1)],
          [null, turn("interrupted", run1)],
          [null, providerThread("idle", 0)],
          [null, runRecord("interrupted", run1)],
          [null, providerThread("idle", 0)],
        ]),
      ),
    );
  });

  // The same race over the stopped run's own records: Stop's settle records them, or the run's
  // finalization does and also rewrites the thread's unchanged state (45 events against 46).
  it("compares a stopped run's records whichever writer records them", () => {
    const interrupted = (writer: string | null) =>
      [
        [writer, record("run-attempt.updated", "attempt:compare:1", "interrupted", run1)],
        [writer, record("node.updated", run1.nodeId, "interrupted", run1)],
        [writer, item("turn-item:compare:result", "interrupted", run1, 8)],
        [writer, runRecord("interrupted", run1)],
      ] as const;
    assert.deepStrictEqual(
      ...comparePair(
        store([
          started,
          [null, turn("interrupted", run1)],
          [null, providerThread("idle", 0)],
          ...interrupted(stop),
        ]),
        store([
          started,
          [null, turn("interrupted", run1)],
          [null, providerThread("idle", 0)],
          ...interrupted(null),
          [null, providerThread("idle", 0, run1)],
        ]),
      ),
    );
  });

  // A thread update reaches the store through its run's consumer (stamped with the run) or the
  // session's roster (not); whichever writes it first, the other finds it done.
  it("compares a thread update whether its run's consumer or the session's roster records it", () => {
    assert.deepStrictEqual(
      ...comparePair(
        store([
          started,
          [null, providerThread("active", 1)],
          [null, providerThread("idle", 0, run1)],
        ]),
        store([started, [null, providerThread("active", 1)], [null, providerThread("idle", 0)]]),
      ),
    );
  });

  it("still tells apart a stopped run's records written differently", () => {
    const stopped = (result: number) =>
      store([
        started,
        [null, item("turn-item:compare:tool", "running", run1, 7)],
        [stop, item("turn-item:compare:tool", "interrupted", run1, 7)],
        [stop, item("turn-item:compare:result", "interrupted", run1, result)],
        [stop, providerThread("idle", 0, run1)],
      ]);
    // The interrupt result placed before the tool it stopped.
    assert.notDeepEqual(...comparePair(stopped(6), stopped(8)));
    // The interrupt result sharing the tool's ordinal.
    assert.notDeepEqual(...comparePair(stopped(7), stopped(8)));
  });

  it("still tells apart a child thread's items from two runs in the other order", () => {
    // A subagent's thread holds items from two runs on one ordinal counter, read in ordinal order.
    const child = "thread:compare-child";
    const childItem = (id: string, run: typeof run1, ordinal: number) =>
      [null, { ...item(id, "completed", run, ordinal), threadId: child }] as const;
    const childCreated = [
      null,
      {
        type: "thread.created",
        threadId: child,
        payload: { id: child, activeProviderThreadId: null },
      },
    ] as const;
    const runs = [
      [null, runRecord("completed", run1)],
      [null, runRecord("running", run2)],
    ] as const;
    assert.notDeepEqual(
      ...comparePair(
        store([
          ...runs,
          childCreated,
          childItem("turn-item:child:answer", run1, 3),
          childItem("turn-item:child:resumed", run2, 4),
        ]),
        store([
          ...runs,
          childCreated,
          childItem("turn-item:child:answer", run1, 4),
          childItem("turn-item:child:resumed", run2, 3),
        ]),
      ),
    );
  });

  it("still tells apart a write its command overwrites, which readers of each event saw", () => {
    const command = "command:fixture:compare:settle";
    assert.notDeepEqual(
      ...comparePair(
        store([
          started,
          [command, providerThread("active", 0)],
          [command, providerThread("idle", 0)],
        ]),
        store([started, [command, providerThread("idle", 0)]]),
      ),
    );
  });

  it("still tells apart a reference to another UUID-identified item", () => {
    const a = "turn-item:provider:claudeAgent:native-item:11111111-1111-4111-8111-111111111111";
    const b = "turn-item:provider:claudeAgent:native-item:22222222-2222-4222-8222-222222222222";
    const pointing = (parent: string) =>
      [
        null,
        record("turn-item.updated", "turn-item:compare:tool", "completed", run1, {
          ordinal: 1000004,
          parentItemId: parent,
        }),
      ] as const;
    const items = [
      [null, item(a, "completed", run1, 1000002)],
      [null, item(b, "completed", run1, 1000003)],
    ] as const;
    assert.notDeepEqual(
      ...comparePair(
        store([started, ...items, pointing(a)]),
        store([started, ...items, pointing(b)]),
      ),
    );
  });

  it("still tells apart a rewrite that changes the projection", () => {
    const other = "provider-thread:compare-other";
    const twoThreads = [
      [null, providerThread("active", 0)],
      [null, providerThread("active", 0, {}, other)],
    ] as const;
    // Writing the first thread again makes it the app thread's active provider thread once more.
    assert.notDeepEqual(
      ...comparePair(
        store([...twoThreads, [null, providerThread("active", 0)]]),
        store(twoThreads),
      ),
    );
    // The same writes in another order leave another active provider thread.
    assert.notDeepEqual(...comparePair(store([...twoThreads].toReversed()), store(twoThreads)));
    // A write its own command replaces, whose effect on the projection outlives the replacement.
    const queue = "command:fixture:compare:queue";
    const placeholder = record(
      "provider-thread.updated",
      "provider-thread:compare",
      "not_loaded",
      {},
      {
        appThreadId: threadId,
        firstRunOrdinal: null,
        nativeThreadRef: null,
        providerSessionId: null,
      },
    );
    assert.notDeepEqual(
      ...comparePair(
        store([
          [queue, providerThread("active", 0)],
          [queue, placeholder],
        ]),
        store([[queue, placeholder]]),
      ),
    );
    // Its commit left a state a later command's write then hides from the final projection.
    const later = [
      "command:fixture:compare:switch",
      providerThread("active", 0, {}, other),
    ] as const;
    assert.notDeepEqual(
      ...comparePair(
        store([[queue, providerThread("active", 0)], [queue, placeholder], later]),
        store([[queue, placeholder], later]),
      ),
    );
    // One write overwritten later in its commit, one repeating the entity: together they decide
    // which thread the commit leaves active.
    const both = [
      [null, providerThread("active", 0)],
      [null, providerThread("active", 0, {}, other)],
    ] as const;
    const changedLater = [
      "command:fixture:compare:switch",
      providerThread("active", 1, {}, other),
    ] as const;
    assert.notDeepEqual(
      ...comparePair(
        store([
          ...both,
          [queue, providerThread("active", 0)],
          [queue, providerThread("active", 0)],
          [queue, placeholder],
          changedLater,
        ]),
        store([...both, [queue, placeholder], changedLater]),
      ),
    );
    // Two such writes, each of which alone the other's effect would cover.
    const twice = [
      [queue, providerThread("active", 0)],
      [queue, providerThread("active", 1)],
    ] as const;
    assert.notDeepEqual(
      ...comparePair(
        store([...twice, [queue, placeholder], later]),
        store([[queue, placeholder], later]),
      ),
    );
  });

  // claude_background_subagent_lifecycle and claude_background_wake_before_queued_prompt: a settled
  // run's consumer, still owning a background subagent, and the next run's consumer ingest their
  // own events concurrently.
  const subagentCancelled = [
    [null, record("node.updated", "node:compare:task", "cancelled", run2)],
    [null, record("subagent.updated", "subagent:compare", "cancelled", run2)],
  ] as const;
  const subagentItemCancelled = [
    null,
    item("turn-item:compare:subagent", "cancelled", run2, 3),
  ] as const;
  const nextTurn = [
    [null, providerThread("active", 0)],
    [null, turn("running", run3)],
    [null, item("turn-item:compare:thinking", "completed", run3, 4)],
  ] as const;
  const subagentFirst = store([...subagentCancelled, subagentItemCancelled, ...nextTurn]);

  it("compares two writers' UUID-identified entities whichever lands first", () => {
    const node = (id: string, run: typeof run1) =>
      [
        null,
        record("node.updated", `node:provider:claudeAgent:native-item:${id}`, "running", run),
      ] as const;
    const first = node("11111111-1111-4111-8111-111111111111", run1);
    const second = node("22222222-2222-4222-8222-222222222222", run2);
    const runs = [started, [null, runRecord("running", run2)]] as const;
    assert.deepStrictEqual(
      ...comparePair(store([...runs, first, second]), store([...runs, second, first])),
    );
  });

  // A provider continuation's message id is new in every replay; two provider threads'
  // continuations may commit in either order.
  it("compares two continuations' new message ids whichever commits first", () => {
    const continuation = (providerThread: string, message: string) =>
      [
        `provider-continuation:message:thread:${providerThread}:${message}`,
        record("message.updated", `message:thread:${providerThread}:${message}`, "sent"),
      ] as const;
    const [a, b] = ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"];
    const messageId = (digit: number) => `${String(digit).repeat(8)}-0000-4000-8000-000000000000`;
    assert.deepStrictEqual(
      ...comparePair(
        store([started, continuation(a, messageId(1)), continuation(b, messageId(2))]),
        store([started, continuation(b, messageId(3)), continuation(a, messageId(4))]),
      ),
    );
  });

  // Records whose UUIDs only one store made (a server-made id): a tolerated repeat earlier in the
  // run's writer, or two run consumers in either order, one referring to the other's new node.
  const fresh = (digit: number) => `${String(digit).repeat(8)}-aaaa-4aaa-8aaa-aaaaaaaaaaaa`;
  const nativeNode = (uuid: string, run: typeof run1, payload: Record<string, unknown> = {}) =>
    [
      null,
      record(
        "node.updated",
        `node:provider:claudeAgent:native-item:${uuid}`,
        "running",
        run,
        payload,
      ),
    ] as const;
  const bothRuns = [
    [null, runRecord("running", run1)],
    [null, runRecord("running", run2)],
    [null, providerThread("active", 0)],
  ] as const;
  const childOf = (uuid: string) =>
    [
      null,
      record("node.updated", "node:compare:child", "running", run2, {
        parentNodeId: `node:provider:claudeAgent:native-item:${uuid}`,
      }),
    ] as const;

  it.each([
    ["only each store made", fresh(1), fresh(2)],
    ["both stores carry", fresh(1), fresh(1)],
  ])(
    "compares a run's new node after a tolerated repeat in one store, with a UUID %s",
    (_, a, b) => {
      assert.deepStrictEqual(
        ...comparePair(
          store([...bothRuns, [null, providerThread("active", 0, run2)], nativeNode(a, run2)]),
          store([...bothRuns, nativeNode(b, run2)]),
        ),
      );
    },
  );

  it.each([
    ["only each store made", fresh(1), fresh(2)],
    ["both stores carry", fresh(1), fresh(1)],
  ])(
    "compares two run consumers in either order, one referring to the other's node, with a UUID %s",
    (_, a, b) => {
      assert.deepStrictEqual(
        ...comparePair(
          store([...bothRuns, nativeNode(a, run1), childOf(a)]),
          store([...bothRuns, childOf(b), nativeNode(b, run1)]),
        ),
      );
    },
  );

  // UUIDs only one store made that share their least place: numbered by content, not arrival.
  const session = (uuid: string, status: string) =>
    [null, record("provider-session.updated", `provider-session:new:${uuid}`, status)] as const;
  it.each([
    ["only each store made", [fresh(1), fresh(2)], [fresh(4), fresh(3)]],
    ["both stores carry", [fresh(1), fresh(2)], [fresh(2), fresh(1)]],
  ])("compares a runless writer's two new entities in either order, with UUIDs %s", (_, a, b) => {
    assert.deepStrictEqual(
      ...comparePair(
        store([...bothRuns, session(a[0]!, "ready"), session(a[1]!, "running")]),
        store([...bothRuns, session(b[0]!, "running"), session(b[1]!, "ready")]),
      ),
    );
  });

  // Alike when they first appear, told apart by what follows.
  it.each([
    ["only each store made", [fresh(1), fresh(2)], [fresh(4), fresh(3)]],
    ["both stores carry", [fresh(1), fresh(2)], [fresh(1), fresh(2)]],
  ])(
    "compares a runless writer's two new entities in either order, alike until one is updated, with UUIDs %s",
    (_, a, b) => {
      assert.deepStrictEqual(
        ...comparePair(
          store([
            ...bothRuns,
            session(a[0]!, "ready"),
            session(a[1]!, "ready"),
            session(a[0]!, "running"),
          ]),
          store([
            ...bothRuns,
            session(b[1]!, "ready"),
            session(b[0]!, "ready"),
            session(b[0]!, "running"),
          ]),
        ),
      );
    },
  );

  // The repeat is skipped (it changes no projection), by the same writer or by Stop's settle.
  it.each(
    (["only each store made", "both stores carry"] as const).flatMap((uuids) =>
      ([null, stop] as const).map(
        (writer) =>
          [
            `${uuids}${writer === null ? "" : ", the repeat by Stop"}`,
            [fresh(1), fresh(2)],
            uuids === "both stores carry" ? [fresh(1), fresh(2)] : [fresh(3), fresh(4)],
            writer,
          ] as const,
      ),
    ),
  )(
    "compares a runless writer's two new entities, alike until one is updated, one written twice, with UUIDs %s",
    (_, a, b, writer) => {
      assert.deepStrictEqual(
        ...comparePair(
          store([
            ...bothRuns,
            session(a[0]!, "ready"),
            session(a[1]!, "ready"),
            session(a[0]!, "running"),
          ]),
          store([
            ...bothRuns,
            session(b[0]!, "ready"),
            session(b[1]!, "ready"),
            [writer, session(b[1]!, "ready")[1]],
            session(b[0]!, "running"),
          ]),
        ),
      );
    },
  );

  // Records alike once masked, but for which UUID each one refers to.
  it.each([
    ["only each store made", [fresh(1), fresh(2)], [fresh(4), fresh(3)]],
    ["both stores carry", [fresh(1), fresh(2)], [fresh(1), fresh(2)]],
  ])(
    "compares a runless writer's two new entities in either order, one naming the other, with UUIDs %s",
    (_, a, b) => {
      const failed = (uuid: string, cause: string) =>
        [
          null,
          record(
            "provider-session.updated",
            `provider-session:new:${uuid}`,
            "error",
            {},
            {
              lastError: `upstream ${cause}`,
            },
          ),
        ] as const;
      assert.deepStrictEqual(
        ...comparePair(
          store([...bothRuns, failed(a[0]!, a[0]!), failed(a[1]!, a[0]!)]),
          store([...bothRuns, failed(b[1]!, b[0]!), failed(b[0]!, b[0]!)]),
        ),
      );
    },
  );

  it("still tells apart a run completing its first or its second new node", () => {
    const done = (uuid: string) =>
      [
        null,
        record("node.updated", `node:provider:claudeAgent:native-item:${uuid}`, "completed", run1),
      ] as const;
    assert.notDeepEqual(
      ...comparePair(
        store([
          ...bothRuns,
          nativeNode(fresh(1), run1),
          nativeNode(fresh(2), run1),
          done(fresh(1)),
        ]),
        store([
          ...bothRuns,
          nativeNode(fresh(3), run1),
          nativeNode(fresh(4), run1),
          done(fresh(4)),
        ]),
      ),
    );
  });

  it("still tells apart a node re-parented back to its first new parent, or not", () => {
    const child = (uuid: string) =>
      [
        null,
        record("node.updated", "node:compare:child", "running", run1, {
          parentNodeId: `node:new:${uuid}`,
        }),
      ] as const;
    assert.notDeepEqual(
      ...comparePair(
        store([...bothRuns, child(fresh(1)), child(fresh(2)), child(fresh(1))]),
        store([...bothRuns, child(fresh(3)), child(fresh(4)), child(fresh(4))]),
      ),
    );
  });

  // A field key that reads like a numbered name ("x #0") must not take another UUID's name.
  it.each(["x #0", "y"])(
    "still tells apart a reference to a third new node or back to the first, under the key %s",
    (key) => {
      const refs = (value: Record<string, string>) =>
        [
          null,
          record("node.updated", "node:compare:refs", "running", run1, { refs: value }),
        ] as const;
      assert.notDeepEqual(
        ...comparePair(
          store([...bothRuns, refs({ x: fresh(1) }), refs({ x: fresh(2), [key]: fresh(3) })]),
          store([...bothRuns, refs({ x: fresh(4) }), refs({ x: fresh(5), [key]: fresh(4) })]),
        ),
      );
    },
  );

  it("still tells apart a run completing the other run's new node instead of its own", () => {
    const done = (uuid: string) =>
      [
        null,
        record("node.updated", `node:provider:claudeAgent:native-item:${uuid}`, "completed", run1),
      ] as const;
    assert.notDeepEqual(
      ...comparePair(
        store([
          ...bothRuns,
          nativeNode(fresh(1), run1),
          nativeNode(fresh(2), run2),
          done(fresh(1)),
        ]),
        store([
          ...bothRuns,
          nativeNode(fresh(3), run1),
          nativeNode(fresh(4), run2),
          done(fresh(4)),
        ]),
      ),
    );
  });

  it("compares two run consumers' records whichever lands first", () => {
    assert.deepStrictEqual(
      ...comparePair(
        subagentFirst,
        store([...subagentCancelled, ...nextTurn, subagentItemCancelled]),
      ),
    );
  });

  it("still tells apart what a writer or an entity records differently", () => {
    const variants = [
      // A record attributed to another run.
      [
        ...subagentCancelled,
        [null, item("turn-item:compare:subagent", "cancelled", run3, 3)],
        ...nextTurn,
      ],
      // One run consumer's records in another order.
      [...subagentCancelled, subagentItemCancelled, nextTurn[0], nextTurn[2], nextTurn[1]],
      // An entity's states in another order.
      [...subagentCancelled, subagentItemCancelled, [null, providerThread("idle", 0)], ...nextTurn],
      // A state that is missing.
      [...subagentCancelled, ...nextTurn],
    ] as const;
    for (const variant of variants) {
      assert.notDeepEqual(...comparePair(store(variant), subagentFirst));
    }
  });
});

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
      assert.deepStrictEqual(
        ...comparableStores(inWorker.result.storedEvents, inThread.result.storedEvents),
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

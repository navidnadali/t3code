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

// The CLI asks these for a permission, which the worker relays to main.
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
// records are compared per thread whoever wrote them; a commandless provider-thread update that
// repeats the previous state (the losing writer's) is dropped.
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
      const repeatsThread = (event: (typeof written)[number], index: number) =>
        writer.startsWith("provider|") &&
        event.type === "provider-thread.updated" &&
        index > 0 &&
        state(event) === state(written[index - 1]!);
      const changes = written
        .filter((event, index) => !repeatsThread(event, index))
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

describe("Claude replay comparison", () => {
  const comparePair = (
    left: ReadonlyArray<OrchestrationV2StoredEvent>,
    right: ReadonlyArray<OrchestrationV2StoredEvent>,
  ) => [byWriter(left, { ordered: true }), byWriter(right, { ordered: true })] as const;
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

// The commit lane's cost per provider snapshot is set by its round trips to the writer worker, each of which
// waits for a saturated event loop. These rows count the writer messages each streaming snapshot costs, alone and in a
// shared batch, at a file database with the writer worker, and check that every write still lands.
import type * as NodeWorkerThreads from "node:worker_threads";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as ConfigProvider from "effect/ConfigProvider";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { CurrentWaitObserver } from "@t3tools/shared/WaitObserver";
import { expect, vi } from "vite-plus/test";

import { recordingWaitObserver } from "../observability/WaitObserver.testkit.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const writerMessages = vi.hoisted((): Array<string> => []);
// Every statement the writer runs, in order: a message's carried statements, then its own.
const writerExecuted = vi.hoisted((): Array<string> => []);

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    readonly #writer: boolean;
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      super(...args);
      const data = args[1]?.workerData as { readonly readOnly?: unknown } | undefined;
      this.#writer = data !== undefined && "readOnly" in data && data.readOnly !== true;
    }
    override postMessage(...args: Parameters<NodeWorkerThreads.Worker["postMessage"]>) {
      const [message] = args;
      if (this.#writer && typeof message === "object" && message !== null && "sql" in message) {
        writerMessages.push(String(message.sql));
        const carried = (message as { readonly prelude?: ReadonlyArray<{ readonly sql: string }> })
          .prelude;
        for (const statement of carried ?? []) writerExecuted.push(statement.sql);
        writerExecuted.push(String(message.sql));
      }
      return super.postMessage(...args);
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

const providerInstanceId = ProviderInstanceId.make("claudeAgent");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "claude-sonnet-4-6",
} satisfies ModelSelection;

const makeThread = (threadId: ThreadId, now: DateTime.Utc): OrchestrationV2AppThread => ({
  createdBy: "user",
  creationSource: "web",
  id: threadId,
  projectId: ProjectId.make("project:commit-lane"),
  title: "Commit lane",
  providerInstanceId,
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
});

type SnapshotKind = "turn-item" | "node" | "message";

/** One provider snapshot of a streaming item; repeated calls update the same row with new text. */
const snapshot = (
  kind: SnapshotKind,
  threadId: ThreadId,
  at: DateTime.Utc,
  step: number,
): OrchestrationV2DomainEvent => {
  const id = EventId.make(`event:${threadId}:${kind}:${step}`);
  const runId = RunId.make(`run:thread:${threadId}:ordinal:1`);
  const nodeId = NodeId.make(`node:${threadId}:assistant`);
  const messageId = MessageId.make(`message:${threadId}:assistant`);
  switch (kind) {
    case "turn-item":
      return {
        id,
        type: "turn-item.updated",
        threadId,
        occurredAt: at,
        payload: {
          id: TurnItemId.make(`turn-item:${threadId}:assistant`),
          threadId,
          runId,
          nodeId,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 1,
          status: "running",
          title: null,
          startedAt: at,
          completedAt: null,
          updatedAt: at,
          type: "assistant_message",
          messageId,
          text: `text ${step}`,
          streaming: true,
        },
      };
    case "node":
      return {
        id,
        type: "node.updated",
        threadId,
        occurredAt: at,
        payload: {
          id: nodeId,
          threadId,
          runId,
          parentNodeId: null,
          rootNodeId: nodeId,
          kind: "assistant_message",
          status: "running",
          countsForRun: false,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: at,
          completedAt: null,
        },
      };
    case "message":
      return {
        id,
        type: "message.updated",
        threadId,
        occurredAt: at,
        payload: {
          createdBy: "agent",
          creationSource: "provider",
          id: messageId,
          threadId,
          runId,
          nodeId,
          role: "assistant",
          text: `text ${step}`,
          attachments: [],
          streaming: true,
          createdAt: at,
          updatedAt: at,
        },
      };
  }
};

const KINDS: ReadonlyArray<SnapshotKind> = ["turn-item", "node", "message"];
const threadOf = (index: number) => ThreadId.make(`thread:commit-lane:${index}`);

/** Creates the threads and each one's streaming rows, so measured snapshots update rows that exist. */
const seed = Effect.fnUntraced(function* (threads: number) {
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  for (let index = 0; index < threads; index++) {
    const threadId = threadOf(index);
    yield* sink.write({
      events: [
        {
          id: EventId.make(`event:${threadId}:created`),
          type: "thread.created",
          threadId,
          providerInstanceId,
          occurredAt: now,
          payload: makeThread(threadId, now),
        },
        ...KINDS.map((kind) => snapshot(kind, threadId, now, 0)),
      ],
    });
  }
});

type Snapshot = {
  readonly type: string;
  readonly event: OrchestrationV2DomainEvent;
  /** A second write of the same job, after the first. */
  readonly second?: OrchestrationV2DomainEvent;
};

/**
 * A consumer per thread, as the provider ingestor runs one per live run; snapshots share commits.
 * `committed` takes one entry per event once it is published, which follows its batch's COMMIT.
 */
const startConsumer = Effect.fnUntraced(function* (threadId: ThreadId) {
  const sink = yield* EventSink.EventSinkV2;
  const queue = yield* Queue.unbounded<Snapshot, Cause.Done>();
  const committed = yield* Queue.unbounded<void>();
  const afterSequence = yield* sink.latestSequence({ threadId });
  yield* sink.stream({ threadId, afterSequence }).pipe(
    Stream.runForEach(() => Queue.offer(committed, undefined)),
    Effect.forkScoped,
  );
  const fiber = yield* sink
    .consumeProviderEvents(
      Stream.fromQueue(queue),
      ({ event, second }) =>
        sink
          .write({ events: [event] })
          .pipe(
            Effect.andThen(second === undefined ? Effect.void : sink.write({ events: [second] })),
          ),
      Effect.succeed(false),
      () => Effect.succeed(true),
    )
    .pipe(Effect.forkScoped);
  return { queue, committed, fiber };
});

/** A consumer whose snapshot job writes nothing and holds the lane until its gate opens. */
const startHolder = Effect.fnUntraced(function* () {
  const sink = yield* EventSink.EventSinkV2;
  const queue = yield* Queue.unbounded<Snapshot, Cause.Done>();
  const gates = yield* Queue.unbounded<{
    readonly entered: Deferred.Deferred<void>;
    readonly gate: Deferred.Deferred<void>;
  }>();
  const fiber = yield* sink
    .consumeProviderEvents(
      Stream.fromQueue(queue),
      () =>
        Queue.take(gates).pipe(
          Effect.flatMap(({ entered, gate }) =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(gate))),
          ),
        ),
      Effect.succeed(false),
      () => Effect.succeed(true),
    )
    .pipe(Effect.forkScoped);
  /** Queues a job that holds the lane, and returns once the lane is running it. */
  const hold = Effect.fnUntraced(function* (event: OrchestrationV2DomainEvent) {
    const entered = yield* Deferred.make<void>();
    const gate = yield* Deferred.make<void>();
    yield* Queue.offer(gates, { entered, gate });
    yield* Queue.offer(queue, { type: event.type, event });
    yield* Deferred.await(entered);
    return gate;
  });
  return { queue, fiber, hold };
});

const layer = (filename: string) =>
  EventSink.layer.pipe(
    Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
    Layer.provideMerge(SqlitePersistence.layerFromPath(filename)),
  );

// The commit lane's own depth gauge (jobs queued or running), to know when a batch is fully queued.
const waits = recordingWaitObserver();
const awaitLaneDepth = (jobs: number) =>
  Effect.promise(() =>
    expect
      .poll(() => waits.readers.get("ProviderCommitBatch.depth")?.() ?? 0, { timeout: 30_000 })
      .toBeGreaterThanOrEqual(jobs),
  );

const withDatabase = <A, E>(
  body: Effect.Effect<
    A,
    E,
    EventSink.EventSinkV2 | ProjectionStore.ProjectionStoreV2 | SqlClient.SqlClient | Scope.Scope
  >,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const filename = path.join(yield* fs.makeTempDirectoryScoped(), "statev2.sqlite");
    return yield* body.pipe(Effect.provide(layer(filename)));
  }).pipe(
    Effect.scoped,
    Effect.provideService(CurrentWaitObserver, waits.observer),
    Effect.provide(NodeServices.layer),
  );

/** Each row's stored state after the measured snapshots: the last text and the thread touched to the last event. */
const assertLanded = Effect.fnUntraced(function* (threadId: ThreadId, step: number) {
  const sql = yield* SqlClient.SqlClient;
  const [item] = yield* sql<{ readonly text: string }>`
    SELECT json_extract(payload_json, '$.text') AS text FROM orchestration_v2_projection_turn_items
    WHERE turn_item_id = ${`turn-item:${threadId}:assistant`}`;
  const [message] = yield* sql<{ readonly text: string }>`
    SELECT json_extract(payload_json, '$.text') AS text FROM orchestration_v2_projection_messages
    WHERE message_id = ${`message:${threadId}:assistant`}`;
  const [events] = yield* sql<{ readonly count: number }>`
    SELECT COUNT(*) AS count FROM orchestration_events WHERE stream_id = ${threadId}`;
  const [head] = yield* sql<{ readonly last_sequence: number; readonly max_sequence: number }>`
    SELECT last_sequence, (SELECT MAX(sequence) FROM orchestration_events) AS max_sequence
    FROM orchestration_v2_projection_metadata WHERE projection_name = 'thread-projections'`;
  assert.equal(item?.text, `text ${step}`);
  assert.equal(message?.text, `text ${step}`);
  assert.equal(events?.count, 1 + KINDS.length * (step + 1));
  assert.equal(head?.last_sequence, head?.max_sequence);
});

it.effect(
  "a lone streaming snapshot costs its transaction ends, one read, its event insert and one flush",
  () =>
    withDatabase(
      Effect.gen(function* () {
        yield* seed(1);
        const threadId = threadOf(0);
        const consumer = yield* startConsumer(threadId);
        const perKind: Record<string, number> = {};
        for (const kind of KINDS) {
          writerMessages.length = 0;
          const at = yield* DateTime.now;
          yield* Queue.offer(consumer.queue, {
            type: kind,
            event: snapshot(kind, threadId, at, 1),
          });
          yield* Queue.take(consumer.committed);
          perKind[kind] = writerMessages.length;
        }
        yield* Queue.end(consumer.queue);
        yield* Fiber.join(consumer.fiber);
        yield* assertLanded(threadId, 1);
        // BEGIN, the item's position and run ordinal in one read, the event insert, one flush of the projection rows,
        // the thread touch and the projection head, and COMMIT. perf.12 sends 8, 6 and 6.
        assert.deepEqual(perKind, { "turn-item": 5, node: 4, message: 4 });
      }),
    ),
);

it.effect(
  "a streaming snapshot in a shared batch costs its savepoint, one read, its event insert and one flush",
  () =>
    withDatabase(
      Effect.gen(function* () {
        const threads = 6;
        yield* seed(threads);
        // The holder's job keeps the lane busy until every measured job is queued behind it.
        const holder = yield* startHolder();
        const consumers = yield* Effect.forEach(Array.from({ length: threads - 1 }), (_, index) =>
          startConsumer(threadOf(index + 1)),
        );
        const perKind: Record<string, number> = {};
        for (const kind of KINDS) {
          const now = yield* DateTime.now;
          const gate = yield* holder.hold(
            snapshot("node", threadOf(0), now, KINDS.indexOf(kind) + 1),
          );
          for (const [index, consumer] of consumers.entries()) {
            const event = snapshot(kind, threadOf(index + 1), now, 1);
            yield* Queue.offer(consumer.queue, { type: kind, event });
          }
          yield* awaitLaneDepth(1 + consumers.length);
          writerMessages.length = 0;
          yield* Deferred.succeed(gate, undefined);
          for (const consumer of consumers) yield* Queue.take(consumer.committed);
          // Every measured job went through one shared batch; its BEGIN and COMMIT are shared.
          assert.equal(writerMessages.filter((sql) => /^\s*BEGIN\b/i.test(sql)).length, 1);
          const own = writerMessages.filter((sql) => !/^\s*(BEGIN|COMMIT)\b/i.test(sql));
          perKind[kind] = own.length / consumers.length;
        }
        yield* Effect.forEach([holder, ...consumers], (consumer) => Queue.end(consumer.queue));
        yield* Effect.forEach([holder, ...consumers], (consumer) => Fiber.join(consumer.fiber));
        for (let index = 1; index < threads; index++) yield* assertLanded(threadOf(index), 1);
        // Each job: its SAVEPOINT, the item's position and run ordinal in one read, the event insert and one
        // flush of its projection writes. perf.12 sends 7, 5 and 5.
        assert.deepEqual(perKind, { "turn-item": 4, node: 3, message: 3 });
      }),
    ),
);

const savepoints = () => writerExecuted.filter((sql) => /^\s*SAVEPOINT\b/i.test(sql)).length;

it.effect(
  "a shared batch opens no savepoint for one consumer's snapshots, and one per run of a consumer's snapshots for several",
  () =>
    withDatabase(
      Effect.gen(function* () {
        yield* seed(3);
        const holder = yield* startHolder();
        const consumers = yield* Effect.forEach([1, 2], (index) => startConsumer(threadOf(index)));
        // Queues each snapshot behind the held lane in turn, so the batch keeps exactly this order.
        const batch = Effect.fnUntraced(function* (owners: ReadonlyArray<number>, step: number) {
          const now = yield* DateTime.now;
          const gate = yield* holder.hold(snapshot("node", threadOf(0), now, step));
          for (const [position, owner] of owners.entries()) {
            const event = snapshot("turn-item", threadOf(owner + 1), now, step * 10 + position);
            yield* Queue.offer(consumers[owner]!.queue, { type: event.type, event });
            yield* awaitLaneDepth(2 + position);
          }
          writerExecuted.length = 0;
          yield* Deferred.succeed(gate, undefined);
          for (const owner of owners) yield* Queue.take(consumers[owner]!.committed);
          assert.equal(writerExecuted.filter((sql) => /^\s*BEGIN\b/i.test(sql)).length, 1);
          return savepoints();
        });
        const single = yield* batch([0, 0, 0, 0], 1);
        const interleaved = yield* batch([0, 0, 1, 1, 0], 2);
        // One savepoint per job would be 4 and 5.
        assert.deepEqual({ single, interleaved }, { single: 0, interleaved: 3 });
        const sql = yield* SqlClient.SqlClient;
        for (const [owner, last] of [
          [0, 24],
          [1, 23],
        ] as const) {
          const [item] = yield* sql<{ readonly text: string }>`
            SELECT json_extract(payload_json, '$.text') AS text FROM orchestration_v2_projection_turn_items
            WHERE turn_item_id = ${`turn-item:${threadOf(owner + 1)}:assistant`}`;
          assert.equal(item?.text, `text ${last}`);
        }
        yield* Effect.forEach([holder, ...consumers], (consumer) => Queue.end(consumer.queue));
      }),
    ),
);

const shape = (sql: string) => {
  const text = sql.replace(/\s+/g, " ").trim();
  const write = /^(?:INSERT INTO|UPDATE)\s+(\w+)/i.exec(text);
  if (write !== null) return `${text.split(" ")[0]!.toUpperCase()} ${write[1]}`;
  if (/^(SAVEPOINT|BEGIN|COMMIT)\b/i.test(text)) return text.split(" ")[0]!.toUpperCase();
  return text === "SELECT 1" ? "flush" : `SELECT ${/\bFROM\s+(\w+)/i.exec(text)?.[1] ?? "?"}`;
};

it.effect(
  "keeps the order of a snapshot's writes, with the deferred ones after the reads that precede them",
  () =>
    withDatabase(
      Effect.gen(function* () {
        yield* seed(3);
        const holder = yield* startHolder();
        const consumers = yield* Effect.forEach([1, 2], (index) => startConsumer(threadOf(index)));
        const now = yield* DateTime.now;
        const gate = yield* holder.hold(snapshot("node", threadOf(0), now, 1));
        for (const [index, consumer] of consumers.entries()) {
          const event = snapshot("turn-item", threadOf(index + 1), now, 1);
          yield* Queue.offer(consumer.queue, { type: event.type, event });
        }
        yield* awaitLaneDepth(1 + consumers.length);
        writerExecuted.length = 0;
        yield* Deferred.succeed(gate, undefined);
        for (const consumer of consumers) yield* Queue.take(consumer.committed);
        const job = [
          "SAVEPOINT",
          "SELECT orchestration_v2_turn_item_positions",
          "INSERT orchestration_events",
          "INSERT orchestration_v2_projection_turn_items",
          "UPDATE orchestration_v2_projection_threads",
          "INSERT orchestration_v2_projection_metadata",
          "flush",
        ];
        // The batch: the holder's empty transaction ends, then one BEGIN, each job in turn, one COMMIT.
        assert.deepEqual(writerExecuted.map(shape), ["COMMIT", "BEGIN", ...job, ...job, "COMMIT"]);
        yield* Effect.forEach([holder, ...consumers], (consumer) => Queue.end(consumer.queue));
      }),
    ),
);

/** A projection write that fails for a marked message, as a constraint or I/O failure would. */
const injectProjectionFailure = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const when of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER inject_failure_${when.toLowerCase()} BEFORE ${when}
      ON orchestration_v2_projection_messages
      WHEN json_extract(NEW.payload_json, '$.text') = 'fail'
      BEGIN SELECT RAISE(ABORT, 'injected projection failure'); END`);
  }
});

const storedState = Effect.fnUntraced(function* (threadId: ThreadId) {
  const sql = yield* SqlClient.SqlClient;
  const [row] = yield* sql<{
    readonly events: number;
    readonly text: string;
    readonly updated_at: string;
  }>`
    SELECT
      (SELECT COUNT(*) FROM orchestration_events WHERE stream_id = ${threadId}) AS events,
      (SELECT json_extract(payload_json, '$.text') FROM orchestration_v2_projection_messages
        WHERE message_id = ${`message:${threadId}:assistant`}) AS text,
      (SELECT updated_at FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}) AS updated_at
  `;
  return row!;
});

const failing = (threadId: ThreadId, at: DateTime.Utc): OrchestrationV2DomainEvent => {
  const event = snapshot("message", threadId, at, 1);
  return event.type === "message.updated"
    ? { ...event, payload: { ...event.payload, text: "fail" } }
    : event;
};

it.effect(
  "a failing projection write fails only its own job in a shared batch, leaving none of it",
  () =>
    withDatabase(
      Effect.gen(function* () {
        yield* seed(4);
        yield* injectProjectionFailure;
        const before = yield* storedState(threadOf(2));
        const holder = yield* startHolder();
        const consumers = yield* Effect.forEach([1, 2, 3], (index) =>
          startConsumer(threadOf(index)),
        );
        const at = DateTime.add(yield* DateTime.now, { seconds: 5 });
        const gate = yield* holder.hold(snapshot("node", threadOf(0), at, 1));
        for (const [index, consumer] of consumers.entries()) {
          const threadId = threadOf(index + 1);
          const event = index === 1 ? failing(threadId, at) : snapshot("message", threadId, at, 1);
          yield* Queue.offer(consumer.queue, { type: event.type, event });
        }
        yield* awaitLaneDepth(1 + consumers.length);
        yield* Deferred.succeed(gate, undefined);
        yield* Queue.take(consumers[0]!.committed);
        yield* Queue.take(consumers[2]!.committed);
        const exit = yield* Fiber.await(consumers[1]!.fiber);
        assert(Exit.isFailure(exit));
        assert.include(Cause.pretty(exit.cause), "injected projection failure");
        // The failed job's event, projection row and thread touch rolled back with its savepoint.
        assert.deepEqual(yield* storedState(threadOf(2)), before);
        for (const index of [1, 3]) {
          const state = yield* storedState(threadOf(index));
          assert.equal(state.text, "text 1");
          assert.equal(state.events, 1 + KINDS.length + 1);
        }
        yield* Effect.forEach([holder, consumers[0]!, consumers[2]!], (consumer) =>
          Queue.end(consumer.queue),
        );
      }),
    ),
);

it.effect("a failing projection write fails a lone job's transaction, leaving none of it", () =>
  withDatabase(
    Effect.gen(function* () {
      yield* seed(1);
      yield* injectProjectionFailure;
      const before = yield* storedState(threadOf(0));
      const consumer = yield* startConsumer(threadOf(0));
      const event = failing(threadOf(0), DateTime.add(yield* DateTime.now, { seconds: 5 }));
      yield* Queue.offer(consumer.queue, { type: event.type, event });
      const exit = yield* Fiber.await(consumer.fiber);
      assert(Exit.isFailure(exit));
      assert.include(Cause.pretty(exit.cause), "injected projection failure");
      assert.deepEqual(yield* storedState(threadOf(0)), before);
      // The writer takes the next transaction.
      const sink = yield* EventSink.EventSinkV2;
      const next = snapshot("message", threadOf(0), yield* DateTime.now, 2);
      yield* sink.write({ events: [next] });
      assert.equal((yield* storedState(threadOf(0))).text, "text 2");
    }),
  ),
);

it.effect(
  "a failing projection write fails the write even when a terminal run's lineage is read after it",
  () =>
    withDatabase(
      Effect.gen(function* () {
        yield* seed(1);
        yield* injectProjectionFailure;
        const threadId = threadOf(0);
        const sink = yield* EventSink.EventSinkV2;
        const now = DateTime.add(yield* DateTime.now, { seconds: 5 });
        const before = yield* storedState(threadId);
        // A finished run makes the write read its thread's lineage, a read that tolerates its own failure.
        const finished: OrchestrationV2DomainEvent = {
          id: EventId.make(`event:${threadId}:run-finished`),
          type: "run.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: RunId.make(`run:thread:${threadId}:ordinal:1`),
            threadId,
            ordinal: 1,
            providerInstanceId,
            modelSelection,
            providerThreadId: null,
            userMessageId: MessageId.make(`message:${threadId}:user`),
            rootNodeId: null,
            activeAttemptId: null,
            status: "completed",
            queuePosition: null,
            requestedAt: now,
            startedAt: now,
            completedAt: now,
            checkpointId: null,
            contextHandoffId: null,
          },
        };
        const exit = yield* sink
          .write({ events: [failing(threadId, now), finished] })
          .pipe(Effect.exit);
        assert(Exit.isFailure(exit));
        assert.include(Cause.pretty(exit.cause), "injected projection failure");
        assert.deepEqual(yield* storedState(threadId), before);
        const sql = yield* SqlClient.SqlClient;
        const runs =
          yield* sql`SELECT 1 FROM orchestration_v2_projection_runs WHERE thread_id = ${threadId}`;
        assert.lengthOf(runs, 0);
      }),
    ),
);

// A failed job in a shared batch: the batch runs one consumer's jobs with no savepoint, rolls back on a
// failure and applies the jobs before it again from what they wrote. These rows hold it to a savepoint
// per job (the reference) for every fault position.

interface PlannedJob {
  readonly owner: number;
  readonly fault: boolean;
  /** Two writes, the second the faulty one when the job fails: the first queued its publication. */
  readonly twoWrites?: boolean;
}
/** Batches, each in the order its jobs reach the lane. */
type Plan = ReadonlyArray<ReadonlyArray<PlannedJob>>;

const injectedFailure = (threadId: ThreadId, at: DateTime.Utc, step: number) => {
  const event = snapshot("message", threadId, at, step);
  return event.type === "message.updated"
    ? { ...event, payload: { ...event.payload, text: "fail" } }
    : event;
};

/**
 * Every row of every table and the AUTOINCREMENT counters, each table's rows sorted, as text; the
 * migration ledgers, stamped with the wall clock when the database was created, are left out.
 */
const databaseImage = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql<{ readonly name: string }>`
    SELECT name FROM sqlite_schema
    WHERE type = 'table' AND (name NOT LIKE 'sqlite_%' OR name = 'sqlite_sequence')
      AND name NOT IN ('effect_sql_migrations', 't3_perf_fork_migrations') ORDER BY name`;
  const image: Record<string, ReadonlyArray<string>> = {};
  for (const { name } of tables) {
    const rows = yield* sql.unsafe<Record<string, unknown>>(`SELECT * FROM "${name}"`);
    image[name] = rows
      .map((row) =>
        JSON.stringify(row, (_key, value: unknown) =>
          value instanceof Uint8Array ? Buffer.from(value).toString("hex") : value,
        ),
      )
      .toSorted();
  }
  return image;
});

/**
 * Runs `plan` against a fresh database: each batch is queued, in its order, behind a held lane, then
 * released. Returns the database image, the order a subscriber of every thread saw, and each
 * consumer's outcome. A consumer whose job failed gets no later jobs.
 */
const runPlan = (plan: Plan, options: { readonly perJobSavepoints: boolean }) =>
  withDatabase(
    Effect.gen(function* () {
      const owners = Math.max(...plan.flat().map((job) => job.owner)) + 1;
      yield* seed(owners + 1);
      yield* injectProjectionFailure;
      const sink = yield* EventSink.EventSinkV2;
      const published = yield* Queue.unbounded<string>();
      yield* sink.stream({ afterSequence: yield* sink.latestSequence() }).pipe(
        Stream.runForEach((stored) =>
          Queue.offer(published, `${stored.sequence} ${stored.event.id}`),
        ),
        Effect.forkScoped,
      );
      let committedJobs = 0;
      const holder = yield* startHolder();
      const consumers = yield* Effect.forEach(Array.from({ length: owners }), (_, owner) =>
        startConsumer(threadOf(owner + 1)),
      );
      const outcomes: Record<number, string> = {};
      let step = 0;
      for (const [index, batch] of plan.entries()) {
        const at = DateTime.add(DateTime.makeUnsafe(0), { seconds: index + 1 });
        const gate = yield* holder.hold(snapshot("node", threadOf(0), at, 100 + index));
        let queued = 0;
        const expected = new Map<number, { committed: number; failed: boolean }>();
        for (const job of batch) {
          // A consumer that failed in an earlier batch has stopped; within its batch, a failed
          // consumer's later jobs are still queued, and cancelled.
          if (outcomes[job.owner] !== undefined) continue;
          const state = expected.get(job.owner) ?? { committed: 0, failed: false };
          expected.set(job.owner, state);
          step += 1;
          const threadId = threadOf(job.owner + 1);
          const kind = KINDS[step % KINDS.length]!;
          const first = job.twoWrites === true || !job.fault;
          const event = first
            ? snapshot(kind, threadId, at, step)
            : injectedFailure(threadId, at, step);
          step += 1;
          const second =
            job.twoWrites !== true
              ? undefined
              : job.fault
                ? injectedFailure(threadId, at, step)
                : snapshot(KINDS[step % KINDS.length]!, threadId, at, step);
          yield* Queue.offer(consumers[job.owner]!.queue, {
            type: event.type,
            event,
            ...(second === undefined ? {} : { second }),
          });
          queued += 1;
          yield* awaitLaneDepth(1 + queued);
          if (job.fault) state.failed = true;
          else if (!state.failed) state.committed += job.twoWrites === true ? 2 : 1;
        }
        yield* Deferred.succeed(gate, undefined);
        for (const [owner, state] of expected) {
          const consumer = consumers[owner]!;
          for (let taken = 0; taken < state.committed; taken++)
            yield* Queue.take(consumer.committed);
          committedJobs += state.committed;
          if (!state.failed) continue;
          const exit = yield* Fiber.await(consumer.fiber);
          outcomes[owner] = Exit.isFailure(exit)
            ? `failed: ${Cause.pretty(exit.cause).includes("injected projection failure")}`
            : "succeeded";
        }
      }
      for (const [owner, consumer] of consumers.entries()) {
        if (outcomes[owner] !== undefined) continue;
        yield* Queue.end(consumer.queue);
        outcomes[owner] = Exit.isSuccess(yield* Fiber.await(consumer.fiber))
          ? "succeeded"
          : "failed";
      }
      yield* Queue.end(holder.queue);
      yield* Fiber.join(holder.fiber);
      const seen: Array<string> = [];
      for (let taken = 0; taken < committedJobs; taken++) seen.push(yield* Queue.take(published));
      assert.equal(yield* Queue.size(published), 0);
      return { image: yield* databaseImage, seen, outcomes };
    }),
  ).pipe(
    Effect.provideService(
      ConfigProvider.ConfigProvider,
      ConfigProvider.fromUnknown({
        T3CODE_PROVIDER_COMMIT_JOB_SAVEPOINTS: options.perJobSavepoints,
      }),
    ),
  );

/** A seeded run of batches: 2-4 consumers, 2-8 jobs a batch, about one job in six failing. */
const randomPlan = (seed: number): Plan => {
  let state = seed >>> 0;
  const next = () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
  const owners = 2 + Math.floor(next() * 3);
  return Array.from({ length: 3 }, () =>
    Array.from({ length: 2 + Math.floor(next() * 7) }, () => ({
      owner: Math.floor(next() * owners),
      fault: next() < 1 / 6,
      twoWrites: next() < 1 / 4,
    })),
  );
};

const job = (owner: number, fault = false, twoWrites = false): PlannedJob => ({
  owner,
  fault,
  twoWrites,
});
const PLANS: ReadonlyArray<readonly [string, Plan]> = [
  ["one consumer, its first job fails", [[job(0, true), job(0), job(0)]]],
  ["one consumer, a middle job fails", [[job(0), job(0), job(0, true), job(0)]]],
  ["one consumer, its last job fails", [[job(0), job(0), job(0), job(0, true)]]],
  [
    "a failing job whose first write had queued its publication",
    [
      [job(0, false, true), job(0, true, true), job(0)],
      [job(1), job(1, true, true), job(0)],
    ],
  ],
  [
    "two consumers, a failure in each run",
    [[job(0), job(0, true), job(1), job(1), job(1, true), job(0)]],
  ],
  [
    "interleaved consumers, back-to-back batches",
    [
      [job(0), job(1), job(0, true)],
      [job(1), job(2)],
    ],
  ],
  ...[1, 2, 3, 4, 5, 6].map((seed) => [`seed ${seed}`, randomPlan(seed)] as const),
];

it.effect.each(PLANS)(
  "a failed job leaves the batch as a savepoint per job would: %s",
  ([_name, plan]) =>
    Effect.gen(function* () {
      const reference = yield* runPlan(plan, { perJobSavepoints: true });
      const recorded = yield* runPlan(plan, { perJobSavepoints: false });
      assert.deepEqual(recorded.outcomes, reference.outcomes);
      assert.deepEqual(recorded.seen, reference.seen);
      assert.deepEqual(recorded.image, reference.image);
    }),
);

it.effect(
  "the jobs applied again after a failure leave the rows a batch without the failure leaves",
  () =>
    Effect.gen(function* () {
      const failed = yield* runPlan([[job(0), job(0), job(0, true), job(0)]], {
        perJobSavepoints: false,
      });
      const clean = yield* runPlan([[job(0), job(0)]], { perJobSavepoints: false });
      assert.deepEqual(failed.image, clean.image);
      assert.deepEqual(failed.seen, clean.seen);
    }),
);

/**
 * What runs in a restart's gap, set by a row. The batch worker reads it through RestartGapProbe,
 * provided outside the database layer so that the worker the layer forks has it.
 */
const gap: { probe: Effect.Effect<void> } = { probe: Effect.void };
const withGapProbe = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.provideService(
      NodeSqliteClient.RestartGapProbe,
      Effect.suspend(() => gap.probe),
    ),
  );

/** One consumer's batch of snapshots behind a held lane, the third of them failing. */
const failInSingleConsumerBatch = Effect.fnUntraced(function* () {
  yield* seed(2);
  yield* injectProjectionFailure;
  const before = yield* storedState(threadOf(1));
  const holder = yield* startHolder();
  const consumer = yield* startConsumer(threadOf(1));
  const at = DateTime.add(DateTime.makeUnsafe(0), { seconds: 1 });
  const gate = yield* holder.hold(snapshot("node", threadOf(0), at, 100));
  const events = [
    snapshot("turn-item", threadOf(1), at, 1),
    snapshot("node", threadOf(1), at, 2),
    injectedFailure(threadOf(1), at, 3),
  ];
  for (const [index, event] of events.entries()) {
    yield* Queue.offer(consumer.queue, { type: event.type, event });
    yield* awaitLaneDepth(2 + index);
  }
  writerExecuted.length = 0;
  yield* Deferred.succeed(gate, undefined);
  return { holder, consumer, before };
});

it.effect(
  "no other write reaches the writer between a failed batch's rollback and its re-commit",
  () =>
    withGapProbe(
      withDatabase(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE competing_write (note TEXT NOT NULL)`;
          // A writer of this process outside the batch, released at the instant after the rollback.
          const release = yield* Deferred.make<void>();
          const asking = yield* Deferred.make<void>();
          let landed = false;
          const competing = yield* Deferred.await(release).pipe(
            Effect.andThen(Deferred.succeed(asking, undefined)),
            Effect.andThen(sql`INSERT INTO competing_write (note) VALUES ('competing')`),
            Effect.andThen(
              Effect.sync(() => {
                landed = true;
              }),
            ),
            Effect.forkScoped,
          );
          let inGap: { readonly landed: boolean; readonly sent: boolean } | undefined;
          gap.probe = Effect.gen(function* () {
            yield* Deferred.succeed(release, undefined);
            yield* Deferred.await(asking);
            // Enough turns for the write to reach the writer, were the writer free: it posts at once.
            for (let turn = 0; turn < 100; turn++) yield* Effect.yieldNow;
            inGap = {
              landed,
              sent: writerExecuted.some((text) => text.includes("competing_write")),
            };
          });
          const { holder, consumer } = yield* failInSingleConsumerBatch();
          yield* Queue.take(consumer.committed);
          yield* Queue.take(consumer.committed);
          assert(Exit.isFailure(yield* Fiber.await(consumer.fiber)));
          yield* Fiber.join(competing);
          gap.probe = Effect.void;
          assert.deepEqual(inGap, { landed: false, sent: false });
          const shapes = writerExecuted.map((text) => text.replace(/\s+/g, " ").trim());
          const rollback = shapes.indexOf("ROLLBACK");
          const competingAt = shapes.findIndex((text) =>
            text.startsWith("INSERT INTO competing_write"),
          );
          const commitAt = shapes.findIndex((text, index) => index > rollback && text === "COMMIT");
          assert.isAbove(rollback, -1);
          assert.equal(shapes[rollback + 1], "BEGIN IMMEDIATE");
          assert.isAbove(competingAt, commitAt);
          yield* Queue.end(holder.queue);
        }),
      ),
    ),
);

it.effect("a re-apply that fails fails the whole batch, as a failed commit does", () =>
  withGapProbe(
    withDatabase(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        gap.probe = sql
          .unsafe(
            `CREATE TRIGGER inject_reapply_failure BEFORE INSERT ON orchestration_events
             BEGIN SELECT RAISE(ABORT, 'injected re-apply failure'); END`,
          )
          .pipe(Effect.orDie, Effect.asVoid);
        const { holder, consumer, before } = yield* failInSingleConsumerBatch();
        const exit = yield* Fiber.await(consumer.fiber);
        gap.probe = Effect.void;
        assert(Exit.isFailure(exit));
        assert.include(Cause.pretty(exit.cause), "injected re-apply failure");
        // None of the batch committed, and nothing of it was published.
        assert.deepEqual(yield* storedState(threadOf(1)), before);
        assert.equal(yield* Queue.size(consumer.committed), 0);
        // The writer takes the next transaction.
        yield* sql`DROP TRIGGER inject_reapply_failure`;
        const sink = yield* EventSink.EventSinkV2;
        yield* sink.write({ events: [snapshot("message", threadOf(1), yield* DateTime.now, 9)] });
        assert.equal((yield* storedState(threadOf(1))).text, "text 9");
        yield* Queue.end(holder.queue);
      }),
    ),
  ),
);

// @effect-diagnostics nodeBuiltinImport:off - inspects the pass's scratch file with Node built-ins.
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

// Removal of one path can be made to fail (as EBUSY would on Windows while the file is open), and
// the filesystem's free space can be faked.
const rmSyncInterceptor = vi.hoisted(() => ({
  failPath: null as string | null,
  calls: [] as Array<string>,
  freeBytes: null as number | (() => number) | null,
}));

// The scratch store's worker thread can be made impossible to start, or to fail its next upsert.
const workerInterceptor = vi.hoisted(() => ({ failConstruction: false, failNextUpsert: false }));

vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  return {
    ...actual,
    rmSync: (...args: Parameters<typeof actual.rmSync>) => {
      const target = String(args[0]);
      rmSyncInterceptor.calls.push(target);
      if (target === rmSyncInterceptor.failPath) {
        const error = new Error("resource busy or locked");
        Object.assign(error, { code: "EBUSY" });
        throw error;
      }
      return actual.rmSync(...args);
    },
    statfsSync: (...args: Parameters<typeof actual.statfsSync>) => {
      const stats = actual.statfsSync(args[0]);
      const free = rmSyncInterceptor.freeBytes;
      if (free === null) return stats;
      const bavail = typeof free === "function" ? free() : free;
      return { ...stats, bsize: 1, bavail, bfree: bavail };
    },
  };
});

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:worker_threads")>();
  class Worker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      if (workerInterceptor.failConstruction) {
        const error = new Error("Worker initialization failure");
        Object.assign(error, { code: "ERR_WORKER_INIT_FAILED" });
        throw error;
      }
      super(...args);
    }
    override postMessage(value: unknown, ...rest: ReadonlyArray<unknown>) {
      const message = value as { readonly id?: number; readonly type?: string } | null;
      if (workerInterceptor.failNextUpsert && message?.type === "upsert") {
        // The worker answers an upsert only when it fails; that answer comes back once the next
        // message (the lookup queued behind it) has been posted, as it would from a real worker.
        workerInterceptor.failNextUpsert = false;
        this.failedUpsertId = message.id;
        return;
      }
      super.postMessage(value, ...(rest as [never]));
      if (this.failedUpsertId !== undefined) {
        const id = this.failedUpsertId;
        this.failedUpsertId = undefined;
        this.emit("message", { id, error: "injected upsert failure" });
      }
    }
    private failedUpsertId: number | undefined;
  }
  return { ...actual, Worker };
});

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventStore from "./EventStore.ts";
import * as CompactionScratch from "./CompactionScratch.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const database = SqlitePersistence.layerMemory;
const stores = Layer.mergeAll(
  database,
  EventStore.layer.pipe(Layer.provideMerge(database)),
  ProjectionStore.layer.pipe(Layer.provideMerge(database)),
);
const layerTest = Layer.mergeAll(stores, ProjectionMaintenance.layer.pipe(Layer.provide(stores)));
const isMaintenanceError = Schema.is(ProjectionMaintenance.ProjectionMaintenanceError);

interface SeedEvent {
  readonly kind: string;
  readonly stream: string;
  readonly version: 1 | 2;
  readonly type: string;
  readonly payload: Record<string, unknown>;
  readonly commandId?: string;
}

const item = (id: string, type: string, revision: number) => ({
  id,
  type,
  status: "running",
  title: id,
  output: `${id}:${revision}`,
});

/** One thread's events; `n` makes every entity distinct per thread. */
function threadEvents(n: number): ReadonlyArray<SeedEvent> {
  const stream = `thread-${n}`;
  const v2 = (type: string, payload: Record<string, unknown>, commandId?: string): SeedEvent => ({
    kind: "thread",
    stream,
    version: 2,
    type,
    payload,
    ...(commandId === undefined ? {} : { commandId }),
  });
  const link = { host: "github.com", repository: `org/repo-${n}`, number: n };
  return [
    v2("thread.created", { id: stream }),
    v2("thread.visited", { id: stream }),
    v2("message.updated", { id: `m1-${n}`, revision: 0 }),
    v2("thread.pull-request-link-updated", { ...link, state: "open" }),
    v2("turn-item.updated", item(`a-${n}`, "command_execution", 0)),
    v2("thread.archived", { id: stream }),
    v2("message.updated", { id: `m1-${n}`, revision: 1 }),
    v2("node.updated", { id: `node-${n}`, revision: 0 }),
    v2("turn-item.updated", item(`a-${n}`, "command_execution", 1)),
    v2("turn-item.updated", item(`sub-${n}`, "subagent", 0)),
    v2("thread.marked-unread", { id: stream }),
    v2("thread.tags-updated", { id: stream, tags: ["a"] }),
    v2("thread.tags-updated", { id: stream, tags: ["a", "b"] }),
    v2("turn-item.updated", item(`b-${n}`, "reasoning", 0)),
    v2("message.updated", { id: `m2-${n}`, revision: 0 }),
    v2("turn-item.updated", item(`c-${n}`, "command_execution", 0), `command-${n}`),
    v2("thread.pull-request-link-updated", { ...link, state: "merged" }),
    v2("node.updated", { id: `node-${n}`, revision: 1 }),
    v2("turn-item.updated", item(`a-${n}`, "command_execution", 2)),
    v2("message.updated", { id: `m1-${n}`, revision: 2 }),
    v2("turn-item.updated", item(`sub-${n}`, "subagent", 1)),
    v2("thread.pinned", { id: stream }),
    v2("turn-item.updated", item(`b-${n}`, "reasoning", 1)),
    v2("thread.visited", { id: stream }),
    v2("turn-item.updated", item(`c-${n}`, "command_execution", 1), `command-${n}`),
    v2("message.updated", { id: `m1-${n}`, revision: 3 }),
    v2("thread.pull-request-link-updated", { ...link, state: "closed" }),
    v2("node.updated", { id: `node-${n}`, revision: 2 }),
    v2("turn-item.updated", item(`a-${n}`, "command_execution", 3)),
    v2("turn-item.updated", item(`a-${n}`, "command_execution", 4)),
    // Past one page (50 rows) of the stream, so carry-over across pages within a stream is exercised.
    ...Array.from({ length: 60 }, (_, revision) =>
      v2("message.updated", { id: `m1-${n}`, revision: 4 + revision }),
    ),
  ];
}

/** Distinct keyed entities per thread above: m1, m2, node, a, b and the pull-request link (subagent and commanded items are not keyed). */
const THREADS = 4;

/** Rows interleaved across threads, so every global page mixes streams; plus legacy v1 threads and a project stream. */
const seedStore = Effect.fnUntraced(function* () {
  const sql = yield* SqlClient.SqlClient;
  const perThread = Array.from({ length: THREADS }, (_, index) => threadEvents(index + 1));
  const interleaved: Array<SeedEvent> = [];
  for (let position = 0; position < perThread[0]!.length; position++) {
    for (const events of perThread) interleaved.push(events[position]!);
  }
  // A thread whose transcript was imported (every v1 event obsolete) and one that was not (kept).
  for (let index = 0; index < 5; index++) {
    interleaved.push(
      {
        kind: "thread",
        stream: "imported",
        version: 1,
        type: "thread.message",
        payload: { index },
      },
      { kind: "thread", stream: "pending", version: 1, type: "thread.message", payload: { index } },
    );
  }
  interleaved.push(
    {
      kind: "project",
      stream: "project-1",
      version: 2,
      type: "project.updated",
      payload: { id: "project-1" },
    },
    {
      kind: "project",
      stream: "project-1",
      version: 2,
      type: "project.updated",
      payload: { id: "project-1" },
    },
  );
  const versions = new Map<string, number>();
  for (const [index, event] of interleaved.entries()) {
    const key = `${event.kind}\u0000${event.stream}`;
    const version = (versions.get(key) ?? 0) + 1;
    versions.set(key, version);
    yield* sql`INSERT INTO orchestration_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, command_id,
      actor_kind, payload_json, metadata_json, application_event_version
    ) VALUES (
      ${`event-${index}`}, ${event.kind}, ${event.stream}, ${version}, ${event.type}, '2026-11-02',
      ${event.commandId ?? null}, 'server', ${JSON.stringify(event.payload)}, '{}', ${event.version}
    )`;
  }
  yield* sql`INSERT INTO orchestration_v2_legacy_imports (
    thread_id, source_updated_at, shell_imported_at, transcript_imported_at, imported_message_count
  ) VALUES ('imported', '2026-11-02', '2026-11-02', '2026-11-02', 5), ('pending', '2026-11-02', '2026-11-02', NULL, 0)`;
  yield* sql`INSERT INTO orchestration_command_receipts (
    command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status, command_type
  ) VALUES
    ('legacy-imported', 'thread', 'imported', '2026-11-02', 0, 'accepted', 'legacy'),
    ('legacy-pending', 'thread', 'pending', '2026-11-02', 0, 'accepted', 'legacy'),
    ('command-1', 'thread', 'thread-1', '2026-11-02', 0, 'accepted', 'thread')`;
});

interface ScanRow {
  readonly sequence: number;
  readonly application_event_version: number;
  readonly aggregate_kind: string;
  readonly stream_id: string;
  readonly event_type: string;
  readonly entity_id: string | null;
  readonly imported_legacy_thread: number;
}

/** The whole store as the pass classifies rows, newest first. */
const scanRows = Effect.fnUntraced(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<ScanRow>`
    SELECT
      event.sequence,
      event.application_event_version,
      event.aggregate_kind,
      event.stream_id,
      event.event_type,
      CASE
        WHEN event.application_event_version = 2
          AND event.event_type IN ('message.updated', 'node.updated')
        THEN json_extract(event.payload_json, '$.id')
        WHEN event.application_event_version = 2
          AND event.event_type = 'turn-item.updated'
          AND event.command_id IS NULL
          AND json_extract(event.payload_json, '$.type') <> 'subagent'
        THEN json_extract(event.payload_json, '$.id')
        WHEN event.application_event_version = 2
          AND event.event_type = 'thread.pull-request-link-updated'
        THEN json_array(json_extract(event.payload_json, '$.host'),
          json_extract(event.payload_json, '$.repository'),
          json_extract(event.payload_json, '$.number'))
        ELSE NULL
      END AS entity_id,
      CASE
        WHEN event.application_event_version = 1 AND event.aggregate_kind = 'thread'
        THEN EXISTS (
          SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import
          WHERE legacy_import.thread_id = event.stream_id
            AND legacy_import.transcript_imported_at IS NOT NULL
        )
        ELSE 0
      END AS imported_legacy_thread
    FROM orchestration_events AS event
    ORDER BY event.sequence DESC
  `;
});

const SUPERSEDABLE = new Set([
  "thread.archived",
  "thread.unarchived",
  "thread.settled",
  "thread.unsettled",
  "thread.snoozed",
  "thread.unsnoozed",
  "thread.pinned",
  "thread.auto-settle-set",
  "thread.unpinned",
  "thread.pin-reordered",
  "thread.active-reordered",
  "thread.metadata-updated",
  "thread.pull-request-synced",
  "thread.runtime-mode-updated",
  "thread.interaction-mode-updated",
  "thread.model-selection-updated",
  "thread.provider-switched",
  "thread.guest-share-updated",
]);
const READ_STATE = new Set(["thread.visited", "thread.marked-unread"]);
const TAG_THREAD_EVENT_TYPE = "thread.tags-updated";

/** The global pass as it was: one scan of the whole store, newest first, with store-wide tracking. */
function referenceCompaction(rows: ReadonlyArray<ScanRow>): {
  readonly obsolete: ReadonlySet<number>;
  readonly trackedKeys: number;
} {
  const obsolete = new Set<number>();
  const retainedThreadIds = new Set<string>();
  const retainedReadStateThreadIds = new Set<string>();
  const retainedTagThreadIds = new Set<string>();
  const oldestCandidateByEntityKey = new Map<string, number | null>();
  for (const row of rows) {
    if (row.imported_legacy_thread === 1) {
      obsolete.add(row.sequence);
    } else if (row.application_event_version === 2) {
      if (row.aggregate_kind === "thread" && SUPERSEDABLE.has(row.event_type)) {
        if (retainedThreadIds.has(row.stream_id)) obsolete.add(row.sequence);
        else retainedThreadIds.add(row.stream_id);
      } else if (row.aggregate_kind === "thread" && READ_STATE.has(row.event_type)) {
        if (retainedThreadIds.has(row.stream_id) || retainedReadStateThreadIds.has(row.stream_id)) {
          obsolete.add(row.sequence);
        } else retainedReadStateThreadIds.add(row.stream_id);
      } else if (row.aggregate_kind === "thread" && row.event_type === TAG_THREAD_EVENT_TYPE) {
        if (retainedThreadIds.has(row.stream_id) || retainedTagThreadIds.has(row.stream_id)) {
          obsolete.add(row.sequence);
        } else retainedTagThreadIds.add(row.stream_id);
      } else if (
        row.event_type === "thread.pull-request-link-updated" &&
        retainedThreadIds.has(row.stream_id)
      ) {
        obsolete.add(row.sequence);
      } else if (
        row.event_type === "thread.pull-request-link-updated" ||
        row.event_type === "message.updated" ||
        row.event_type === "node.updated" ||
        (row.event_type === "turn-item.updated" && row.entity_id !== null)
      ) {
        const key = JSON.stringify([row.event_type, row.stream_id, row.entity_id]);
        const candidate = oldestCandidateByEntityKey.get(key);
        if (candidate === undefined) oldestCandidateByEntityKey.set(key, null);
        else {
          if (candidate !== null) obsolete.add(candidate);
          oldestCandidateByEntityKey.set(key, row.sequence);
        }
      }
    }
  }
  return { obsolete, trackedKeys: oldestCandidateByEntityKey.size };
}

it.effect("deletes exactly what the store-wide pass deleted, and keeps the rest in order", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const before = yield* scanRows();
    const expected = referenceCompaction(before).obsolete;
    // Every rule has at least one deletion in this store.
    const byType = new Map<string, number>();
    for (const row of before) {
      if (expected.has(row.sequence)) {
        byType.set(row.event_type, (byType.get(row.event_type) ?? 0) + 1);
      }
    }
    for (const type of [
      "thread.message",
      "thread.archived",
      "thread.visited",
      "thread.tags-updated",
      "thread.pull-request-link-updated",
      "message.updated",
      "node.updated",
      "turn-item.updated",
    ]) {
      assert.isAbove(byType.get(type) ?? 0, 0, type);
    }

    const summary = yield* maintenance.compactEventStore();

    const after = yield* sql<{ readonly sequence: number }>`
      SELECT sequence FROM orchestration_events ORDER BY sequence
    `;
    const remaining = new Set(after.map((row) => row.sequence));
    const deleted = before
      .map((row) => row.sequence)
      .filter((sequence) => !remaining.has(sequence));
    assert.deepEqual(new Set(deleted), expected);
    assert.equal(summary.deletedEventCount, expected.size);
    assert.deepEqual(
      after.map((row) => row.sequence),
      before
        .map((row) => row.sequence)
        .filter((sequence) => !expected.has(sequence))
        .sort((left, right) => left - right),
    );
    // Subagent and commanded items, the project stream and the pending import are untouched.
    for (const row of before) {
      const untouched =
        row.aggregate_kind === "project" ||
        row.stream_id === "pending" ||
        (row.event_type === "turn-item.updated" && row.entity_id === null);
      if (untouched)
        assert.isTrue(remaining.has(row.sequence), `${row.event_type} ${row.sequence}`);
    }
    const receipts = yield* sql<{ readonly command_id: string }>`
      SELECT command_id FROM orchestration_command_receipts ORDER BY command_id
    `;
    assert.deepEqual(
      receipts.map((row) => row.command_id),
      ["command-1", "legacy-pending"],
    );
    assert.equal(summary.deletedReceiptCount, 1);
  }).pipe(Effect.provide(layerTest)),
);

const scratchDir = Effect.acquireRelease(
  Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "compaction-scratch-"))),
  (dir) => Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true })),
);

it.effect(
  "keeps its working set in the scratch file, not in memory, and removes it at the end",
  () =>
    Effect.gen(function* () {
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      yield* seedStore();
      const reference = referenceCompaction(yield* scanRows());
      const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
      let pages = 0;
      let scratchSeen = 0;
      const betweenPages = Effect.sync(() => {
        pages += 1;
        // The scratch database exists for the pass's duration only.
        if (NodeFS.existsSync(scratchPath)) scratchSeen += 1;
      });
      const summary = yield* maintenance.compactEventStore(betweenPages, { scratchPath });
      assert.isAbove(pages, 1);
      assert.equal(scratchSeen, pages);
      assert.equal(summary.trackedKeys, reference.trackedKeys);
      assert.equal(summary.deletedEventCount, reference.obsolete.size);
      for (const file of [scratchPath, `${scratchPath}-wal`, `${scratchPath}-shm`]) {
        assert.isFalse(NodeFS.existsSync(file), file);
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("a scratch file left behind by a crash is replaced, never read", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const before = yield* scanRows();
    const reference = referenceCompaction(before);
    const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
    // A previous pass's state with decisions the current store knows nothing about.
    const stale = new NodeSqlite.DatabaseSync(scratchPath);
    stale.exec(
      "CREATE TABLE candidates (entity_key TEXT PRIMARY KEY, oldest INTEGER) WITHOUT ROWID",
    );
    const insert = stale.prepare(
      "INSERT OR REPLACE INTO candidates (entity_key, oldest) VALUES (?, ?)",
    );
    for (const row of before) {
      if (row.entity_id !== null) {
        insert.run(JSON.stringify([row.event_type, row.stream_id, row.entity_id]), row.sequence);
      }
    }
    stale.close();

    const summary = yield* maintenance.compactEventStore(Effect.void, { scratchPath });
    const after = yield* sql<{ readonly sequence: number }>`
      SELECT sequence FROM orchestration_events ORDER BY sequence
    `;
    const remaining = new Set(after.map((row) => row.sequence));
    assert.deepEqual(
      new Set(before.map((row) => row.sequence).filter((sequence) => !remaining.has(sequence))),
      reference.obsolete,
    );
    assert.equal(summary.deletedEventCount, reference.obsolete.size);
    assert.isFalse(NodeFS.existsSync(scratchPath));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("a scratch file that cannot be removed is logged, and does not fail the pass", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const before = yield* scanRows();
    const reference = referenceCompaction(before);
    const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
    const logs: Array<unknown> = [];
    // Removing the database at the end fails; the pass opened it fine (the first removal, before open).
    const betweenPages = Effect.sync(() => {
      rmSyncInterceptor.failPath = scratchPath;
    });
    yield* Effect.addFinalizer(() => Effect.sync(() => void (rmSyncInterceptor.failPath = null)));
    rmSyncInterceptor.calls.length = 0;
    const summary = yield* maintenance.compactEventStore(betweenPages, { scratchPath }).pipe(
      Effect.provide(
        Logger.layer([Logger.make(({ logLevel, message }) => logs.push([logLevel, message]))], {
          mergeWithExisting: false,
        }),
      ),
      Effect.provideService(References.MinimumLogLevel, "Debug"),
    );
    assert.equal(summary.deletedEventCount, reference.obsolete.size);
    const after = yield* sql<{ readonly sequence: number }>`
      SELECT sequence FROM orchestration_events ORDER BY sequence
    `;
    const remaining = new Set(after.map((row) => row.sequence));
    assert.deepEqual(
      new Set(before.map((row) => row.sequence).filter((sequence) => !remaining.has(sequence))),
      reference.obsolete,
    );
    // The failure is a warning, and the remaining removals still ran.
    assert.isTrue(
      logs.some(
        (entry) =>
          Array.isArray(entry) &&
          entry[0] === "Warn" &&
          String(entry[1]).includes("Compaction scratch store cleanup failed"),
      ),
      JSON.stringify(logs),
    );
    const failedAt = rmSyncInterceptor.calls.lastIndexOf(scratchPath);
    assert.isAbove(failedAt, 0);
    assert.deepEqual(rmSyncInterceptor.calls.slice(failedAt + 1), [
      `${scratchPath}-journal`,
      `${scratchPath}-wal`,
      `${scratchPath}-shm`,
    ]);
    assert.isTrue(NodeFS.existsSync(scratchPath));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("a failure while preparing the scratch store fails the pass and leaves no files", () =>
  Effect.gen(function* () {
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    // The store cannot be opened: its directory does not exist.
    const scratchPath = NodePath.join(yield* scratchDir, "missing", "compaction-scratch.sqlite");
    // A typed failure (never a defect), naming the scratch store underneath the pass's own error.
    const error = yield* maintenance
      .compactEventStore(Effect.void, { scratchPath })
      .pipe(Effect.flip);
    const inner = error.cause;
    assert.isTrue(isMaintenanceError(inner) && inner.operation === "compaction scratch store");
    for (const file of [scratchPath, `${scratchPath}-wal`, `${scratchPath}-shm`]) {
      assert.isFalse(NodeFS.existsSync(file), file);
    }
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

const capturedLogs = (logs: Array<unknown>) =>
  Logger.layer([Logger.make(({ logLevel, message }) => logs.push([logLevel, message]))], {
    mergeWithExisting: false,
  });

/** The fields logged with the first warning whose text includes `text`. */
const warning = (
  logs: ReadonlyArray<unknown>,
  text: string,
): Record<string, unknown> | undefined => {
  for (const entry of logs) {
    if (!Array.isArray(entry) || entry[0] !== "Warn" || !Array.isArray(entry[1])) continue;
    const [message, fields] = entry[1] as ReadonlyArray<unknown>;
    if (String(message).includes(text) && typeof fields === "object" && fields !== null) {
      return fields as Record<string, unknown>;
    }
  }
  return undefined;
};

it.effect("skips the pass with a warning when the disk cannot hold the scratch store", () =>
  Effect.gen(function* () {
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const before = yield* scanRows();
    const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
    const logs: Array<unknown> = [];
    rmSyncInterceptor.freeBytes = 1;
    yield* Effect.addFinalizer(() => Effect.sync(() => void (rmSyncInterceptor.freeBytes = null)));
    const summary = yield* maintenance
      .compactEventStore(Effect.void, { scratchPath })
      .pipe(
        Effect.provide(capturedLogs(logs)),
        Effect.provideService(References.MinimumLogLevel, "Debug"),
      );
    assert.equal(summary.deletedEventCount, 0);
    assert.equal(summary.trackedKeys, 0);
    // The result says so, for `t3 db compact` and the doctor: where, how much is needed, how much there is.
    assert.deepEqual(summary.skipped, {
      scratchPath,
      requiredBytes: Math.max(
        CompactionScratch.requiredFreeBytes(before.length),
        CompactionScratch.SCRATCH_RUNNING_FREE_SPACE_FLOOR,
      ),
      freeBytes: 1,
      stage: "before",
    });
    const after = yield* scanRows();
    assert.equal(after.length, before.length);
    const fields = warning(logs, "Compaction skipped");
    assert.isDefined(fields, JSON.stringify(logs));
    assert.equal(fields?.sequenceBound, before.length);
    assert.notProperty(fields ?? {}, "events");
    assert.isFalse(NodeFS.existsSync(scratchPath));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("removes an empty directory at the scratch path and compacts", () =>
  Effect.gen(function* () {
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const reference = referenceCompaction(yield* scanRows());
    const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
    NodeFS.mkdirSync(scratchPath);
    const summary = yield* maintenance.compactEventStore(Effect.void, { scratchPath });
    assert.equal(summary.deletedEventCount, reference.obsolete.size);
    assert.isFalse(NodeFS.existsSync(scratchPath));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("names a directory that is in the way of the scratch file, and deletes nothing", () =>
  Effect.gen(function* () {
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const before = yield* scanRows();
    const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
    NodeFS.mkdirSync(scratchPath);
    NodeFS.writeFileSync(NodePath.join(scratchPath, "keep"), "");
    const error = yield* maintenance
      .compactEventStore(Effect.void, { scratchPath })
      .pipe(Effect.flip);
    const inner = error.cause;
    assert.isTrue(isMaintenanceError(inner) && inner.operation === "compaction scratch store");
    const message = String(isMaintenanceError(inner) ? inner.cause : "");
    assert.include(message, "directory is in the way of the compaction scratch file");
    assert.include(message, scratchPath);
    assert.equal((yield* scanRows()).length, before.length);
    assert.isTrue(NodeFS.existsSync(NodePath.join(scratchPath, "keep")));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("a scratch store whose worker cannot start fails the pass with a typed error", () =>
  Effect.gen(function* () {
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const before = yield* scanRows();
    const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
    workerInterceptor.failConstruction = true;
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => void (workerInterceptor.failConstruction = false)),
    );
    const error = yield* maintenance
      .compactEventStore(Effect.void, { scratchPath })
      .pipe(Effect.flip);
    const inner = error.cause;
    assert.isTrue(isMaintenanceError(inner) && inner.operation === "compaction scratch store");
    assert.equal((yield* scanRows()).length, before.length);
    assert.isFalse(NodeFS.existsSync(scratchPath));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("a stale scratch file is removed before the free space it occupies is judged", () =>
  Effect.gen(function* () {
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const before = yield* scanRows();
    const reference = referenceCompaction(before);
    const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
    // A crash left a scratch file: while it is there the disk is short of the margin, without it
    // there is plenty — what matters is that it goes before the space is judged.
    NodeFS.writeFileSync(scratchPath, "stale");
    rmSyncInterceptor.calls.length = 0;
    rmSyncInterceptor.freeBytes = () =>
      rmSyncInterceptor.calls.includes(scratchPath)
        ? 1 << 30
        : CompactionScratch.requiredFreeBytes(before.length) - 1;
    yield* Effect.addFinalizer(() => Effect.sync(() => void (rmSyncInterceptor.freeBytes = null)));
    const summary = yield* maintenance.compactEventStore(Effect.void, { scratchPath });
    assert.equal(summary.deletedEventCount, reference.obsolete.size);
    assert.isFalse(NodeFS.existsSync(scratchPath));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect(
  "stops a running pass when the disk fills up, keeping what it deleted and saying why",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      // An older bulk of distinct entities (nothing to delete) below the seeded store: the pass deletes
      // from the newest pages first, then runs into the full disk on its way down.
      for (let index = 0; index < 1000; index++) {
        yield* sql`INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`bulk-${index}`}, 'thread', 'thread-bulk', ${index + 1}, 'message.updated', '2026-11-02',
          'server', ${JSON.stringify({ id: `bulk-${index}` })}, '{}', 2
        )`;
      }
      yield* seedStore();
      const before = yield* scanRows();
      const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
      // Plenty of room when the pass starts; 32 MiB left after its first page, below the running floor.
      let pages = 0;
      rmSyncInterceptor.freeBytes = () => (pages === 0 ? 1 << 30 : 32 * 1024 * 1024);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => void (rmSyncInterceptor.freeBytes = null)),
      );
      const betweenPages = Effect.sync(() => void (pages += 1));
      const logs: Array<unknown> = [];
      const summary = yield* maintenance
        .compactEventStore(betweenPages, { scratchPath })
        .pipe(
          Effect.provide(capturedLogs(logs)),
          Effect.provideService(References.MinimumLogLevel, "Debug"),
        );
      assert.equal(summary.skipped?.stage, "during");
      // The server log says what happened: stopped early, not skipped; and it names the sequence bound.
      const fields = warning(logs, "Compaction stopped early");
      assert.isDefined(fields, JSON.stringify(logs));
      assert.equal(fields?.sequenceBound, before.length);
      assert.isUndefined(warning(logs, "Compaction skipped"));
      assert.equal(summary.skipped?.scratchPath, scratchPath);
      assert.equal(summary.skipped?.freeBytes, 32 * 1024 * 1024);
      // What stopped the pass is the running floor, not the size estimate — the report must say so.
      assert.equal(
        summary.skipped?.requiredBytes,
        CompactionScratch.SCRATCH_RUNNING_FREE_SPACE_FLOOR,
      );
      const after = yield* sql<{ readonly sequence: number }>`
      SELECT sequence FROM orchestration_events ORDER BY sequence
    `;
      // What the first page deleted stays deleted; the rest of the store is untouched.
      assert.equal(before.length - after.length, summary.deletedEventCount);
      assert.equal(summary.deletedEventCount, referenceCompaction(before).obsolete.size);
      assert.isAbove(summary.deletedEventCount, 0);
      assert.equal(summary.deletedReceiptCount, 0);
      for (const file of [
        scratchPath,
        `${scratchPath}-journal`,
        `${scratchPath}-wal`,
        `${scratchPath}-shm`,
      ]) {
        assert.isFalse(NodeFS.existsSync(file), file);
      }
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("removes a rollback journal left beside the scratch file", () =>
  Effect.gen(function* () {
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const reference = referenceCompaction(yield* scanRows());
    const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
    NodeFS.writeFileSync(`${scratchPath}-journal`, "stale");
    const summary = yield* maintenance.compactEventStore(Effect.void, { scratchPath });
    assert.equal(summary.deletedEventCount, reference.obsolete.size);
    assert.isFalse(NodeFS.existsSync(`${scratchPath}-journal`));
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it("needs at most 256 bytes per event, and a megabyte however small the store", () => {
  assert.equal(CompactionScratch.requiredFreeBytes(0), 1024 * 1024);
  assert.equal(CompactionScratch.requiredFreeBytes(1000), 1024 * 1024);
  assert.equal(CompactionScratch.requiredFreeBytes(1_000_000), 256_000_000);
});

it.effect(
  "a disk below the running floor is reported as skipped before the pass, not stopped early",
  () =>
    Effect.gen(function* () {
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      yield* seedStore();
      const before = yield* scanRows();
      const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
      // Above the size estimate, below the floor a running pass insists on: it must not start at all.
      rmSyncInterceptor.freeBytes = 32 * 1024 * 1024;
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => void (rmSyncInterceptor.freeBytes = null)),
      );
      const summary = yield* maintenance.compactEventStore(Effect.void, { scratchPath });
      assert.deepEqual(summary.skipped, {
        scratchPath,
        requiredBytes: CompactionScratch.SCRATCH_RUNNING_FREE_SPACE_FLOOR,
        freeBytes: 32 * 1024 * 1024,
        stage: "before",
      });
      assert.equal(summary.deletedEventCount, 0);
      assert.equal((yield* scanRows()).length, before.length);
      assert.isFalse(NodeFS.existsSync(scratchPath));
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect(
  "a lookup queued behind a failing upsert fails instead of answering from stale state",
  () =>
    Effect.gen(function* () {
      const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
      const scratch = yield* CompactionScratch.open(scratchPath);
      workerInterceptor.failNextUpsert = true;
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => void (workerInterceptor.failNextUpsert = false)),
      );
      // The upsert is only posted; its failure comes back once the lookup behind it is on its way.
      yield* scratch.upsert([["key", null]]);
      const error = yield* scratch.lookup(["key"]).pipe(Effect.flip);
      assert.include(error.message, "injected upsert failure");
    }).pipe(Effect.scoped),
);

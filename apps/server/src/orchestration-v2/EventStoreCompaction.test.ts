// @effect-diagnostics nodeBuiltinImport:off - inspects the pass's scratch file with Node built-ins.
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventStore from "./EventStore.ts";
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

/** The global pass as it was: one scan of the whole store, newest first, with store-wide tracking. */
function referenceCompaction(rows: ReadonlyArray<ScanRow>): {
  readonly obsolete: ReadonlySet<number>;
  readonly trackedKeys: number;
} {
  const obsolete = new Set<number>();
  const retainedThreadIds = new Set<string>();
  const retainedReadStateThreadIds = new Set<string>();
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

/** Statements the pass runs on the main thread, by `sourceSQL`; the scratch store's are among them. */
const mainThreadStatements = Effect.acquireRelease(
  Effect.sync(() => {
    const reads: Array<string> = [];
    const writes: Array<string> = [];
    const all = NodeSqlite.StatementSync.prototype.all;
    const run = NodeSqlite.StatementSync.prototype.run;
    const readSpy = vi
      .spyOn(NodeSqlite.StatementSync.prototype, "all")
      .mockImplementation(function (this: NodeSqlite.StatementSync, ...params) {
        reads.push(this.sourceSQL);
        return all.apply(this, params);
      });
    const writeSpy = vi
      .spyOn(NodeSqlite.StatementSync.prototype, "run")
      .mockImplementation(function (this: NodeSqlite.StatementSync, ...params) {
        writes.push(this.sourceSQL);
        return run.apply(this, params);
      });
    return { reads, writes, readSpy, writeSpy };
  }),
  ({ readSpy, writeSpy }) =>
    Effect.sync(() => {
      readSpy.mockRestore();
      writeSpy.mockRestore();
    }),
);

it.effect(
  "keeps its working set in the scratch file, not in memory, and removes it at the end",
  () =>
    Effect.gen(function* () {
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      yield* seedStore();
      const reference = referenceCompaction(yield* scanRows());
      const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
      const statements = yield* mainThreadStatements;
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
      // Every page of events consults the scratch store and writes its decisions back to it.
      const lookups = statements.reads.filter((query) => /FROM candidates\b/.test(query));
      const upserts = statements.writes.filter((query) => /INSERT INTO candidates\b/.test(query));
      assert.isAtLeast(lookups.length, pages);
      assert.isAtLeast(upserts.length, 1);
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
    let replaced = false;
    // While the pass holds the file open, a directory takes its path: unlinking it at the end fails.
    const betweenPages = Effect.sync(() => {
      if (replaced) return;
      replaced = true;
      NodeFS.rmSync(scratchPath);
      NodeFS.mkdirSync(scratchPath);
    });
    const summary = yield* maintenance.compactEventStore(betweenPages, { scratchPath });
    assert.equal(summary.deletedEventCount, reference.obsolete.size);
    const after = yield* sql<{ readonly sequence: number }>`
      SELECT sequence FROM orchestration_events ORDER BY sequence
    `;
    const remaining = new Set(after.map((row) => row.sequence));
    assert.deepEqual(
      new Set(before.map((row) => row.sequence).filter((sequence) => !remaining.has(sequence))),
      reference.obsolete,
    );
    assert.isTrue(NodeFS.statSync(scratchPath).isDirectory());
    for (const file of [`${scratchPath}-wal`, `${scratchPath}-shm`]) {
      assert.isFalse(NodeFS.existsSync(file), file);
    }
  }).pipe(Effect.scoped, Effect.provide(layerTest)),
);

it.effect("a failure while preparing the scratch store fails the pass and leaves no files", () =>
  Effect.gen(function* () {
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    yield* seedStore();
    const scratchPath = NodePath.join(yield* scratchDir, "compaction-scratch.sqlite");
    yield* Effect.acquireRelease(
      Effect.sync(() =>
        vi.spyOn(NodeSqlite.DatabaseSync.prototype, "exec").mockImplementationOnce(() => {
          throw new Error("scratch store setup failed");
        }),
      ),
      (spy) => Effect.sync(() => spy.mockRestore()),
    );
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

// The offline filtered copy. It prunes the event log and compacts the file in
// one pass: a new file holding every table, and of the events only what the compaction keeps, checked
// against the source before the swap; the original stays beside it until restored or removed.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- a checksum of the database file's bytes.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the checksum reads the file synchronously.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Statement from "effect/sql/Statement";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";
import * as TestClock from "effect/testing/TestClock";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import * as FilteredCopy from "./EventLogFilteredCopy.ts";
import {
  EVENT_LOG_PRUNE_CURSOR,
  EVENT_LOG_PRUNE_MARK,
  EVENT_LOG_PRUNED_THROUGH,
  EVENT_LOG_REFERENCE_SCAN,
  EVENT_LOG_REFERENCE_SCAN_FROM,
} from "./OrchestrationEventStore.ts";
import * as Sqlite from "./Sqlite.ts";
import * as ServerOwnership from "../serverOwnership.ts";

const DAY = 86_400_000;
const NOW = Date.UTC(2026, 10, 18, 9);

const sha256 = (file: string) =>
  NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(file)).digest("hex");

type Row = {
  readonly type: string;
  readonly stream: string;
  readonly payload: object;
  readonly v1?: true;
};
const node = (stream: string, revision: number): Row => ({
  type: "node.updated",
  stream,
  payload: { id: `node:${stream}`, status: "running", revision },
});

// Every class the rule knows, below and above the boundary.
const fixture = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  let id = 0;
  const insert = (rows: ReadonlyArray<Row>) =>
    Effect.forEach(rows, (row) =>
      sql<{ readonly sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`event:copy:${++id}`}, 'thread', ${row.stream},
          COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events
            WHERE aggregate_kind = 'thread' AND stream_id = ${row.stream}), 0),
          ${row.type}, '2026-11-01T00:00:00.000Z', 'server', ${JSON.stringify(row.payload)}, '{}',
          ${row.v1 === true ? 1 : 2}
        ) RETURNING sequence
      `.pipe(Effect.map((rows) => rows[0]!.sequence)),
    );
  const prompt = "Check the backlog. ".repeat(10);
  const kept = yield* insert([
    { type: "thread.created", stream: "thread:a", payload: { id: "thread:a" } },
    { type: "run.created", stream: "thread:a", payload: { id: "run:a" } },
    { type: "checkpoint-scope.created", stream: "thread:a", payload: { id: "scope:a" } },
    { type: "run.updated", stream: "thread:a", payload: { id: "run:a", status: "running" } },
    { type: "run.updated", stream: "thread:a", payload: { id: "run:a", status: "interrupted" } },
    {
      type: "turn-item.updated",
      stream: "thread:a",
      payload: { id: "item:stop", type: "run_interrupt_request", providerTurnId: "turn:a" },
    },
    { type: "subagent.updated", stream: "thread:a", payload: { id: "sub:a", prompt } },
    { type: "message.updated", stream: "thread:legacy", payload: { id: "m" }, v1: true },
  ]);
  const prunable = yield* insert([
    node("thread:a", 0),
    { type: "run.updated", stream: "thread:a", payload: { id: "run:a", status: "completed" } },
    node("thread:idle", 0),
  ]);
  const [idleHead] = yield* insert([node("thread:idle", 1)]);
  const anchors = yield* insert([
    {
      type: "turn-item.updated",
      stream: "thread:sub",
      payload: { id: "item:used", type: "subagent", prompt },
    },
    {
      type: "turn-item.updated",
      stream: "thread:sub",
      payload: { id: "item:unused", type: "subagent", prompt: "Unused. " },
    },
  ]);
  const prunableAfter = yield* insert([node("thread:sub", 0)]);
  // The boundary, marked two days ago; everything after it is newer than the retained day.
  const [boundary] = yield* sql<{ readonly sequence: number }>`
    SELECT MAX(sequence) AS sequence FROM orchestration_events
  `;
  const above = yield* insert([
    {
      type: "turn-item.updated",
      stream: "thread:sub",
      payload: { id: "item:used", type: "subagent", prompt: "", promptSequence: anchors[0] },
    },
    node("thread:a", 1),
    node("thread:sub", 1),
  ]);
  // A sequence used then deleted: the copy must not hand it out again.
  const [gone] = yield* insert([node("thread:gone", 0)]);
  yield* sql`DELETE FROM orchestration_events WHERE sequence = ${gone!}`;
  for (const [name, sequence, at] of [
    ["thread-projections", gone!, NOW],
    [EVENT_LOG_PRUNE_MARK, boundary!.sequence, NOW - 2 * DAY],
  ] as const) {
    yield* sql`
      INSERT INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
      VALUES (${name}, 2, ${sequence}, ${DateTime.formatIso(DateTime.makeUnsafe(at))})
      ON CONFLICT(projection_name) DO UPDATE SET
        last_sequence = excluded.last_sequence, updated_at = excluded.updated_at
    `;
  }
  // Another rowid table with a gap: rowids survive the copy.
  yield* sql`
    INSERT INTO orchestration_command_receipts (rowid, command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status)
    VALUES (2, 'command:a', 'thread', 'thread:a', '2026-11-01', 1, 'accepted'),
           (7, 'command:b', 'thread', 'thread:a', '2026-11-01', 2, 'accepted')
  `;
  return {
    kept,
    prunable: [...prunable, ...prunableAfter],
    idleHead: idleHead!,
    anchors,
    above,
    boundary: boundary!.sequence,
    gone: gone!,
  };
});

// A database built by this build's own migrations, then closed: what the window copies.
const withDatabase = <A, E>(
  use: (paths: {
    readonly db: string;
    readonly built: Effect.Success<typeof fixture>;
  }) => Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-filtered-copy-" });
    const db = path.join(directory, "statev2.sqlite");
    const built = yield* fixture.pipe(Effect.provide(Sqlite.layerFromPath(db)), Effect.scoped);
    return yield* use({ db, built });
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

const readFrom = <A>(
  db: string,
  query: (sql: SqlClient.SqlClient) => Effect.Effect<A, SqlError.SqlError>,
) =>
  Effect.flatMap(SqlClient.SqlClient, query).pipe(
    Effect.provide(NodeSqliteClient.layer({ filename: db, readonly: true })),
    Effect.scoped,
    Effect.orDie,
  );

const sequences = (db: string) =>
  readFrom(
    db,
    (sql) =>
      sql<{
        readonly sequence: number;
      }>`SELECT sequence FROM orchestration_events ORDER BY sequence`,
  ).pipe(Effect.map((rows) => rows.map((row) => row.sequence)));

it.effect("the filtered copy keeps every event the compaction keeps, and every other row", () =>
  withDatabase(({ db, built }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const result = yield* FilteredCopy.compactByFilteredCopy(db, { swap: false });
      const copy = db + FilteredCopy.COPY_SUFFIX;
      assert.equal(result.boundary, built.boundary);
      assert.isTrue(result.verified);

      const left = new Set(yield* sequences(copy));
      // Everything after the boundary; kept types and the idle stream's head; every
      // potential anchor at or below the boundary (the online pass resolves them later).
      for (const sequence of [...built.above, ...built.kept, built.idleHead, ...built.anchors]) {
        assert.isTrue(left.has(sequence), `kept ${sequence}`);
      }
      for (const sequence of built.prunable)
        assert.isFalse(left.has(sequence), `pruned ${sequence}`);

      // The borrower still restores its prompt from the anchor it points at.
      const restored = yield* readFrom(
        copy,
        (sql) =>
          sql<{ readonly prompt: string | null }>`
          SELECT json_extract(anchor.payload_json, '$.prompt') AS prompt
          FROM orchestration_events AS borrower
          JOIN orchestration_events AS anchor
            ON anchor.sequence = json_extract(borrower.payload_json, '$.promptSequence')
          WHERE borrower.sequence = ${built.above[0]!}
        `,
      );
      assert.equal(restored[0]?.prompt, "Check the backlog. ".repeat(10));

      // The copy publishes its boundary and the online pass's progress starts there, with the
      // anchors at or below it held for the online pass to resolve.
      const metadata = yield* readFrom(
        copy,
        (sql) =>
          sql<{ readonly projection_name: string; readonly last_sequence: number }>`
          SELECT projection_name, last_sequence FROM orchestration_v2_projection_metadata
        `,
      );
      const meta = new Map(metadata.map((row) => [row.projection_name, row.last_sequence]));
      assert.equal(meta.get(EVENT_LOG_PRUNED_THROUGH), built.boundary);
      assert.equal(meta.get(EVENT_LOG_PRUNE_CURSOR), built.boundary);
      assert.equal(meta.get(EVENT_LOG_REFERENCE_SCAN), built.boundary);
      assert.equal(meta.get(EVENT_LOG_REFERENCE_SCAN_FROM), built.boundary);
      // The mark was due, so the copy is the pass that set the next one.
      assert.equal(meta.get(EVENT_LOG_PRUNE_MARK), Math.max(...built.above));
      const held = yield* readFrom(
        copy,
        (sql) =>
          sql<{ readonly sequence: number }>`
          SELECT sequence FROM orchestration_events_prune_state WHERE held = 1 ORDER BY sequence
        `,
      );
      assert.deepEqual(
        held.map((row) => row.sequence),
        [built.idleHead, ...built.anchors],
      );

      // AUTOINCREMENT does not hand out the deleted sequence again; rowids elsewhere survive.
      const [sequence] = yield* readFrom(
        copy,
        (sql) =>
          sql<{
            readonly seq: number;
          }>`SELECT seq FROM sqlite_sequence WHERE name = 'orchestration_events'`,
      );
      assert.equal(sequence?.seq, built.gone);
      const receipts = yield* readFrom(
        copy,
        (sql) =>
          sql<{
            readonly rowid: number;
          }>`SELECT rowid FROM orchestration_command_receipts ORDER BY rowid`,
      );
      assert.deepEqual(
        receipts.map((row) => row.rowid),
        [2, 7],
      );

      // The same schema objects, and the original untouched.
      const schema = (file: string) =>
        readFrom(
          file,
          (sql) =>
            sql<{ readonly type: string; readonly name: string }>`
            SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name
          `,
        );
      assert.deepEqual(yield* schema(copy), yield* schema(db));
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.PRE_SUFFIX));
    }),
  ),
);

it.effect("the filtered copy never prunes past the projections", () =>
  withDatabase(({ db, built }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      // The projections lag behind the mark, so the boundary is where they stand.
      const lagging = built.prunable[0]!;
      yield* Effect.flatMap(
        SqlClient.SqlClient,
        (sql) =>
          sql`UPDATE orchestration_v2_projection_metadata SET last_sequence = ${lagging} WHERE projection_name = 'thread-projections'`,
      ).pipe(Effect.provide(NodeSqliteClient.layer({ filename: db })), Effect.scoped, Effect.orDie);
      const result = yield* FilteredCopy.compactByFilteredCopy(db, { swap: false });
      assert.equal(result.boundary, lagging);
      const left = new Set(yield* sequences(db + FilteredCopy.COPY_SUFFIX));
      for (const sequence of built.prunable.filter((sequence) => sequence > lagging)) {
        assert.isTrue(left.has(sequence), `kept ${sequence} above the projections`);
      }
    }),
  ),
);

it.effect(
  "verification catches a copy that dropped what the rule keeps, and nothing is swapped",
  () =>
    withDatabase(({ db }) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const before = sha256(db);
        // A deliberately broken filter: it drops the potential prompt anchors.
        const error = yield* FilteredCopy.compactByFilteredCopy(db, {
          swap: true,
          brokenFilterForTest: "NOT (anchor)",
        }).pipe(Effect.flip);
        assert.equal(error._tag, "FilteredCopyError");
        assert.equal(error.step, "verify");
        assert.include(error.reason, "anchor");
        assert.equal(sha256(db), before);
        assert.isFalse(NodeFS.existsSync(db + FilteredCopy.PRE_SUFFIX));
      }),
    ),
);

it.effect("a copy cut off midway leaves the source untouched, and a new run starts over", () =>
  withDatabase(({ db, built }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const before = sha256(db);
      const reached = yield* Deferred.make<void>();
      const never = yield* Deferred.make<void>();
      const holdEvents: Statement.Transformer = (statement) => {
        const [query] = statement.compile();
        return query.includes("INSERT INTO main.orchestration_events")
          ? Deferred.succeed(reached, undefined).pipe(
              Effect.andThen(Deferred.await(never)),
              Effect.as(statement),
            )
          : Effect.succeed(statement);
      };
      const cut = yield* FilteredCopy.compactByFilteredCopy(db, { swap: true }).pipe(
        Effect.provideService(Statement.CurrentTransformer, holdEvents),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(reached);
      yield* Fiber.interrupt(cut);
      assert.equal(sha256(db), before);
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.PRE_SUFFIX));
      // The partial copy's space is given back.
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.COPY_SUFFIX));
      // The next run replaces the partial copy.
      const result = yield* FilteredCopy.compactByFilteredCopy(db, { swap: false });
      assert.isTrue(result.verified);
      assert.equal(result.boundary, built.boundary);
    }),
  ),
);

it.effect("the swap keeps the original beside the copy, and one call restores it", () =>
  withDatabase(({ db, built }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const before = sha256(db);
      yield* FilteredCopy.compactByFilteredCopy(db, { swap: true });
      assert.equal(sha256(db + FilteredCopy.PRE_SUFFIX), before);
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.COPY_SUFFIX));
      for (const sequence of built.prunable) assert.notInclude(yield* sequences(db), sequence);

      yield* FilteredCopy.restoreBeforeFilteredCopy(db);
      assert.equal(sha256(db), before);
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.PRE_SUFFIX));
      assert.isTrue(NodeFS.existsSync(db + FilteredCopy.REJECTED_SUFFIX));
    }),
  ),
);

it.effect("the filtered copy refuses a database whose last writer did not close it cleanly", () =>
  withDatabase(({ db }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      // A write-ahead log with frames in it: a server still running, or one killed.
      NodeFS.writeFileSync(db + "-wal", Buffer.alloc(4096, 1));
      const error = yield* FilteredCopy.compactByFilteredCopy(db, { swap: true }).pipe(Effect.flip);
      assert.equal(error.step, "precondition");
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.COPY_SUFFIX));
    }),
  ),
);

it.effect("the filtered copy refuses a database another process has open", () =>
  withDatabase(({ db }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const before = sha256(db);
      // A server that has the file open, idle: its log is empty, but it may write at any moment.
      const error = yield* Effect.flatMap(SqlClient.SqlClient, (sql) =>
        sql`SELECT 1`.pipe(
          Effect.orDie,
          Effect.andThen(FilteredCopy.compactByFilteredCopy(db, { swap: true })),
          Effect.flip,
        ),
      ).pipe(Effect.provide(NodeSqliteClient.layer({ filename: db })), Effect.orDie);
      assert.equal(error.step, "precondition");
      assert.include(error.reason, "another process");
      assert.equal(sha256(db), before);
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.COPY_SUFFIX));
    }),
  ),
);

it.effect("a database written while it was copied is not swapped", () =>
  withDatabase(({ db }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      // A server starts, changes a row the copy already holds, checkpoints and stops, all before
      // the swap: the copy is stale, and verification's row counts cannot see it.
      let written = false;
      const writeMeanwhile: Statement.Transformer = (statement) => {
        const [query] = statement.compile();
        if (written || !query.includes("quick_check")) return Effect.succeed(statement);
        written = true;
        return Effect.flatMap(SqlClient.SqlClient, (sql) =>
          sql`UPDATE orchestration_command_receipts SET status = 'rejected' WHERE rowid = 2`.pipe(
            Effect.andThen(sql`PRAGMA wal_checkpoint(TRUNCATE)`),
          ),
        ).pipe(
          Effect.provide(NodeSqliteClient.layer({ filename: db })),
          Effect.orDie,
          Effect.as(statement),
        );
      };
      const error = yield* FilteredCopy.compactByFilteredCopy(db, { swap: true }).pipe(
        Effect.provideService(Statement.CurrentTransformer, writeMeanwhile),
        Effect.flip,
      );
      assert.isTrue(written);
      assert.equal(error.step, "swap");
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.PRE_SUFFIX));
      const [receipt] = yield* readFrom(
        db,
        (sql) =>
          sql<{
            readonly status: string;
          }>`SELECT status FROM orchestration_command_receipts WHERE rowid = 2`,
      );
      assert.equal(receipt?.status, "rejected");
    }),
  ),
);

it.effect("the filtered copy refuses a database with no boundary due", () =>
  withDatabase(({ db }) =>
    Effect.gen(function* () {
      // The mark is an hour old and nothing was pruned before: there is nothing the rule allows to prune.
      yield* TestClock.setTime(NOW - 2 * DAY + 3_600_000);
      const error = yield* FilteredCopy.compactByFilteredCopy(db, { swap: true }).pipe(Effect.flip);
      assert.equal(error.step, "precondition");
      assert.include(error.reason, "no boundary is due");
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.COPY_SUFFIX));
    }),
  ),
);

// A server cannot start while the copy runs, through the swap.
const serverStatePath = (db: string) => db.replace(/[^/]+$/, "server-runtime.json");

it.effect("the filtered copy refuses while a server owns the database's home", () =>
  withDatabase(({ db }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const before = sha256(db);
      const error = yield* ServerOwnership.holdServerOwnershipLock(serverStatePath(db)).pipe(
        Effect.orDie,
        Effect.andThen(FilteredCopy.compactByFilteredCopy(db, { swap: true })),
        Effect.flip,
        Effect.scoped,
      );
      assert.equal(error.step, "precondition");
      assert.include(error.reason, "server owns");
      assert.equal(sha256(db), before);
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.COPY_SUFFIX));
    }),
  ),
);

it.effect("a server cannot start while the filtered copy runs", () =>
  withDatabase(({ db }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const starts: Array<string> = [];
      const tryStart: Statement.Transformer = (statement) => {
        const [query] = statement.compile();
        if (!query.includes("quick_check")) return Effect.succeed(statement);
        return ServerOwnership.holdServerOwnershipLock(serverStatePath(db)).pipe(
          Effect.scoped,
          Effect.match({ onFailure: (error) => error._tag, onSuccess: () => "started" }),
          Effect.map((outcome) => {
            starts.push(outcome);
            return statement;
          }),
        );
      };
      yield* FilteredCopy.compactByFilteredCopy(db, { swap: true }).pipe(
        Effect.provideService(Statement.CurrentTransformer, tryStart),
      );
      assert.deepEqual(starts, ["ServerAlreadyRunningError"]);
    }),
  ),
);

// A restore never deletes a copy an earlier restore set aside.
it.effect("a second restore keeps the copy the first restore set aside", () =>
  withDatabase(({ db }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      yield* FilteredCopy.compactByFilteredCopy(db, { swap: true });
      yield* FilteredCopy.restoreBeforeFilteredCopy(db);
      const rejected = sha256(db + FilteredCopy.REJECTED_SUFFIX);
      yield* FilteredCopy.compactByFilteredCopy(db, { swap: true });
      const error = yield* FilteredCopy.restoreBeforeFilteredCopy(db).pipe(Effect.flip);
      assert.equal(error.step, "restore");
      assert.include(error.reason, FilteredCopy.REJECTED_SUFFIX);
      assert.equal(sha256(db + FilteredCopy.REJECTED_SUFFIX), rejected);
      assert.isTrue(NodeFS.existsSync(db + FilteredCopy.PRE_SUFFIX));
    }),
  ),
);

// A restore checks its files under the ownership lock, so a second restore that
// started meanwhile cannot move the restored database aside.
it.effect("two restores at once leave the original in place and the copy set aside", () =>
  withDatabase(({ db }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const before = sha256(db);
      yield* FilteredCopy.compactByFilteredCopy(db, { swap: true });
      const copy = sha256(db);
      const fs = yield* FileSystem.FileSystem;
      const paused = yield* Deferred.make<void>();
      const go = yield* Deferred.make<void>();
      // The second restore stops at its last file check until the first has run.
      const pausing = FileSystem.FileSystem.of({
        ...fs,
        exists: (file) =>
          file === db + FilteredCopy.REJECTED_SUFFIX + "-shm"
            ? Deferred.succeed(paused, undefined).pipe(
                Effect.andThen(Deferred.await(go)),
                Effect.andThen(fs.exists(file)),
              )
            : fs.exists(file),
      });
      const second = yield* FilteredCopy.restoreBeforeFilteredCopy(db).pipe(
        Effect.provideService(FileSystem.FileSystem, pausing),
        Effect.exit,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(paused);
      const first = yield* FilteredCopy.restoreBeforeFilteredCopy(db).pipe(Effect.exit);
      yield* Deferred.succeed(go, undefined);
      const secondExit = yield* Fiber.join(second);
      assert.equal([first, secondExit].filter((exit) => exit._tag === "Success").length, 1);
      assert.equal(sha256(db), before);
      assert.equal(sha256(db + FilteredCopy.REJECTED_SUFFIX), copy);
    }),
  ),
);

// Windows cannot rename a file a SQLite handle holds open, so there the swap and
// the restore close their exclusive connection first; elsewhere they rename under it.
const openAtRename = (db: string, fs: FileSystem.FileSystem, seen: Array<boolean>) =>
  FileSystem.FileSystem.of({
    ...fs,
    rename: (from, to) =>
      Effect.sync(() => {
        if (from !== db) return;
        // An exclusive transaction from a second connection fails while another handle holds it.
        const probe = new NodeSqlite.DatabaseSync(db);
        try {
          probe.exec("PRAGMA busy_timeout = 0; BEGIN EXCLUSIVE; COMMIT;");
          seen.push(false);
        } catch {
          seen.push(true);
        } finally {
          probe.close();
        }
      }).pipe(Effect.andThen(fs.rename(from, to))),
  });

it.effect.each([
  { platform: "win32" as const, open: false },
  { platform: "linux" as const, open: true },
])(
  "the swap and the restore rename with the handle open: $open on $platform",
  ({ platform, open }) =>
    withDatabase(({ db }) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(NOW);
        const before = sha256(db);
        const fs = yield* FileSystem.FileSystem;
        const seen: Array<boolean> = [];
        yield* Effect.gen(function* () {
          yield* FilteredCopy.compactByFilteredCopy(db, { swap: true });
          yield* FilteredCopy.restoreBeforeFilteredCopy(db);
        }).pipe(
          Effect.provideService(FileSystem.FileSystem, openAtRename(db, fs, seen)),
          Effect.provideService(HostProcess.Platform, platform),
        );
        assert.deepEqual(seen, [open, open]);
        assert.equal(sha256(db), before);
      }),
    ),
);

// On Windows a writer that needs no ownership lock (a CLI command once
// migrations are current) can commit between the last check and the rename. The swap reads the moved
// original again and puts it back.
it.effect("a commit just before the Windows swap's rename leaves the original in place", () =>
  withDatabase(({ db }) =>
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW);
      const fs = yield* FileSystem.FileSystem;
      let committed = false;
      const commitFirst = FileSystem.FileSystem.of({
        ...fs,
        rename: (from, to) =>
          Effect.sync(() => {
            if (from !== db || committed) return;
            committed = true;
            const writer = new NodeSqlite.DatabaseSync(db);
            try {
              writer.exec(
                "UPDATE orchestration_command_receipts SET status = 'rejected' WHERE rowid = 2",
              );
            } finally {
              writer.close();
            }
          }).pipe(Effect.andThen(fs.rename(from, to))),
      });
      const error = yield* FilteredCopy.compactByFilteredCopy(db, { swap: true }).pipe(
        Effect.provideService(FileSystem.FileSystem, commitFirst),
        Effect.provideService(HostProcess.Platform, "win32"),
        Effect.flip,
      );
      assert.isTrue(committed);
      assert.equal(error.step, "swap");
      assert.isFalse(NodeFS.existsSync(db + FilteredCopy.PRE_SUFFIX));
      const [receipt] = yield* readFrom(
        db,
        (sql) =>
          sql<{
            readonly status: string;
          }>`SELECT status FROM orchestration_command_receipts WHERE rowid = 2`,
      );
      assert.equal(receipt?.status, "rejected");
    }),
  ),
);

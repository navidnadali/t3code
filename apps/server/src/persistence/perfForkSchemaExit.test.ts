// @effect-diagnostics nodeBuiltinImport:off - runs the exit script as an operator would, on the file.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { migrationManifest, runMigrations, type RunMigrationsOptions } from "./Migrations.ts";
import { PERF_FORK_MIGRATIONS_TABLE, runPerfForkMigrations } from "./PerfForkMigrations.ts";
import { THREAD_ITEM_TOTALS } from "./PerfForkMigrations/003_ThreadItemTotals.ts";
import { normalizeSql, PERF_FORK_ORDINAL_INDEX } from "./testkit/PerfForkOrdinalIndex.ts";
import { PERF_FORK_RUN_ID_TURN_ITEM_COUNTS } from "./testkit/PerfForkRunIdTurnItemCounts.ts";
import * as EventLogPruneState from "./PerfForkMigrations/EventLogPruneState.ts";

const EXIT_SCRIPT = new URL("../../../../scripts/perf-fork-schema-exit.sql", import.meta.url);
const EXIT_SCRIPT_BEFORE_085 = new URL(
  "../../../../scripts/perf-fork-schema-exit-before-085.sql",
  import.meta.url,
);
// Upstream's last migration when the fork's first builds recorded the fork's at 77 and 78.
const EARLY_FORK_UPSTREAM_HEAD = 76;
// Upstream's last migration in the last fork build before upstream's migration 85.
const BEFORE_085_UPSTREAM_HEAD = 80;
const COUNTS_TABLE = "orchestration_v2_projection_turn_item_counts";

const migrate = (filename: string, options?: RunMigrationsOptions) =>
  runMigrations(options).pipe(
    Effect.map((ran) => ran.map(([id]) => id)),
    Effect.provide(NodeSqliteClient.layer({ filename })),
  );

const withDatabase = <A>(filename: string, use: (db: NodeSqlite.DatabaseSync) => A) =>
  Effect.acquireUseRelease(
    Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
    (db) => Effect.sync(() => use(db)),
    (db) => Effect.sync(() => db.close()),
  );

const tempDatabase = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  return path.join(
    yield* fs.makeTempDirectoryScoped({ prefix: "t3-fork-exit-" }),
    "statev2.sqlite",
  );
});

// Every row of every table, each table's rows sorted, so two reads compare whatever their order.
const contents = (db: NodeSqlite.DatabaseSync) =>
  Object.fromEntries(
    db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
      )
      .all()
      .map(({ name }) => [
        String(name),
        db
          .prepare(`SELECT * FROM "${String(name)}"`)
          .all()
          .map((row) => JSON.stringify(row))
          .toSorted(),
      ]),
  );

const schema = (db: NodeSqlite.DatabaseSync) =>
  db.prepare("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name").all();

const objectsNamed = (db: NodeSqlite.DatabaseSync, where: string) =>
  db
    .prepare(`SELECT type, name FROM sqlite_master WHERE ${where} ORDER BY name`)
    .all()
    .map(({ type, name }) => `${String(type)}:${String(name)}`);

// The fork's own objects.
const forkObjects = (db: NodeSqlite.DatabaseSync) =>
  objectsNamed(db, `name = '${PERF_FORK_MIGRATIONS_TABLE}'`);

// Every index over `$.messageId`, on whichever table, its statement normalised.
const messageOrdinalIndexes = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare(
      "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'index' AND instr(sql, '$.messageId') > 0 ORDER BY name",
    )
    .all()
    .map(({ name, tbl_name, sql }) => ({
      name: String(name),
      tbl_name: String(tbl_name),
      sql: normalizeSql(String(sql)),
    }));

// The message-ordinal index as upstream's chain alone creates it, through its migration 94.
const upstream094Index = Effect.gen(function* () {
  const filename = yield* tempDatabase;
  yield* migrate(filename, { toMigrationInclusive: 94 });
  return yield* withDatabase(filename, messageOrdinalIndexes);
});

// The turn-item counts and their triggers: upstream's since its migration 85, the fork's before it.
const countObjects = (db: NodeSqlite.DatabaseSync) =>
  objectsNamed(db, `name LIKE 'orchestration_v2_turn_item_counts_%' OR name = '${COUNTS_TABLE}'`);

const COUNT_OBJECTS = [
  `table:${COUNTS_TABLE}`,
  "trigger:orchestration_v2_turn_item_counts_delete",
  "trigger:orchestration_v2_turn_item_counts_insert",
  "trigger:orchestration_v2_turn_item_counts_update",
];

const countColumns = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare(`SELECT name FROM pragma_table_info('${COUNTS_TABLE}')`)
    .all()
    .map(({ name }) => String(name));

const insertTurnItem = (
  db: NodeSqlite.DatabaseSync,
  id: string,
  threadId: string,
  runId: string | null,
  payloadJson = '{"messageId":"m1"}',
) =>
  db
    .prepare(
      `INSERT INTO orchestration_v2_projection_turn_items (
         turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
       ) VALUES (?, ?, ?, 1, 'assistant_message', 'completed', '2026-11-01T00:00:00.000Z', ?)`,
    )
    .run(id, threadId, runId, payloadJson);

// Items, a move to another run and a delete, so the counts take every trigger.
const writeTurnItems = (db: NodeSqlite.DatabaseSync) => {
  insertTurnItem(db, "a1", "thread-a", "run-1");
  insertTurnItem(db, "a2", "thread-a", "run-1");
  insertTurnItem(db, "a3", "thread-a", null);
  insertTurnItem(db, "b1", "thread-b", "run-2", "{not json");
  db.prepare(
    "UPDATE orchestration_v2_projection_turn_items SET run_id = 'run-3' WHERE turn_item_id = 'a2'",
  ).run();
  db.prepare("DELETE FROM orchestration_v2_projection_turn_items WHERE turn_item_id = 'a1'").run();
};

const ledgerIds = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare("SELECT migration_id FROM effect_sql_migrations ORDER BY migration_id")
    .all()
    .map(({ migration_id }) => Number(migration_id));

const forkLedger = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare(`SELECT migration_id, name FROM ${PERF_FORK_MIGRATIONS_TABLE} ORDER BY migration_id`)
    .all()
    .map(({ migration_id, name }) => [Number(migration_id), String(name)]);

const FORK_CHAIN = [
  [1, "ProjectionTurnItemCounts"],
  [2, "TurnItemMessageOrdinalIndex"],
  [3, "ThreadItemTotals"],
  [4, EventLogPruneState.NAME],
];

const countsAsStored = (db: NodeSqlite.DatabaseSync, key = "run_key") =>
  db
    .prepare(
      `SELECT thread_id, ${key} AS run_key, item_count FROM ${COUNTS_TABLE}
       ORDER BY thread_id, run_key`,
    )
    .all();

const countsFromItems = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare(
      `SELECT thread_id, COALESCE(run_id, '') AS run_key, COUNT(*) AS item_count
       FROM orchestration_v2_projection_turn_items
       GROUP BY thread_id, COALESCE(run_id, '') ORDER BY thread_id, run_key`,
    )
    .all();

// A database as the last fork build before upstream's migration 85 left it: the fork chain
// recorded, its counts keyed by run_id, its message-ordinal index.
const forkBefore085 = (filename: string) =>
  Effect.gen(function* () {
    yield* migrate(filename, { toMigrationInclusive: BEFORE_085_UPSTREAM_HEAD });
    yield* withDatabase(filename, (db) => {
      for (const statement of PERF_FORK_RUN_ID_TURN_ITEM_COUNTS) db.exec(statement);
      db.exec(PERF_FORK_ORDINAL_INDEX);
    });
    yield* runPerfForkMigrations().pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
  });

it.effect(
  "leaves the fork's schema so upstream's migrations start on it with every row and upstream's index kept",
  () =>
    Effect.gen(function* () {
      const filename = yield* tempDatabase;
      const exitScript = NodeFS.readFileSync(EXIT_SCRIPT, "utf8");
      const upstreamIndex = yield* upstream094Index;
      assert.lengthOf(upstreamIndex, 1);

      assert.deepEqual(
        yield* migrate(filename),
        migrationManifest.map(([id]) => id),
      );
      const expected = yield* withDatabase(filename, (db) => {
        assert.deepEqual(forkLedger(db), FORK_CHAIN);
        assert.deepEqual(messageOrdinalIndexes(db), upstreamIndex);
        writeTurnItems(db);
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
        const {
          [PERF_FORK_MIGRATIONS_TABLE]: _forkLedger,
          [THREAD_ITEM_TOTALS]: _threadItemTotals,
          ...rest
        } = contents(db);
        return rest;
      });

      yield* withDatabase(filename, (db) => {
        db.exec(exitScript);
        // Running it again changes nothing.
        db.exec(exitScript);
        assert.deepEqual(forkObjects(db), []);
        // Upstream's migrations 85 and 94 own the counts and the index: they stay.
        assert.deepEqual(countObjects(db), COUNT_OBJECTS);
        assert.deepEqual(messageOrdinalIndexes(db), upstreamIndex);
        assert.deepEqual(contents(db), expected);
      });

      // Upstream's chain alone, as an upstream build runs it: 94 is recorded and runs no more.
      assert.deepEqual(yield* migrate(filename, { toPerfForkMigrationInclusive: 0 }), []);
      yield* withDatabase(filename, (db) => {
        assert.deepEqual(contents(db), expected);
        assert.deepEqual(messageOrdinalIndexes(db), upstreamIndex);
        insertTurnItem(db, "c1", "thread-c", "run-4");
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
      });

      assert.deepEqual(yield* migrate(filename), []);
      yield* withDatabase(filename, (db) => {
        assert.deepEqual(forkLedger(db), FORK_CHAIN);
        assert.deepEqual(forkObjects(db), [`table:${PERF_FORK_MIGRATIONS_TABLE}`]);
        assert.deepEqual(messageOrdinalIndexes(db), upstreamIndex);
        assert.deepEqual(countObjects(db), COUNT_OBJECTS);
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
        insertTurnItem(db, "c2", "thread-c", "run-4");
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
      });
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "leaves a fork database from before upstream's migration 85 so upstream's migrations start on it and keep its index",
  () =>
    Effect.gen(function* () {
      const filename = yield* tempDatabase;
      const exitScript = NodeFS.readFileSync(EXIT_SCRIPT_BEFORE_085, "utf8");
      const upstreamIds = migrationManifest.map(([id]) => id);
      const upstreamIndex = yield* upstream094Index;

      yield* forkBefore085(filename);
      const expected = yield* withDatabase(filename, (db) => {
        assert.deepEqual(forkLedger(db), FORK_CHAIN);
        writeTurnItems(db);
        assert.deepEqual(countsAsStored(db, "run_id"), countsFromItems(db));
        const {
          [COUNTS_TABLE]: _counts,
          [PERF_FORK_MIGRATIONS_TABLE]: _forkLedger,
          [THREAD_ITEM_TOTALS]: _threadItemTotals,
          ...rest
        } = contents(db);
        return rest;
      });

      yield* withDatabase(filename, (db) => {
        db.exec(exitScript);
        // Running it again changes nothing.
        db.exec(exitScript);
        assert.deepEqual(forkObjects(db), []);
        assert.deepEqual(countObjects(db), []);
        // The fork's index is upstream's 94's, which takes it as it is.
        assert.deepEqual(messageOrdinalIndexes(db), upstreamIndex);
        assert.deepEqual(contents(db), expected);
      });

      // Upstream's chain alone, as an upstream build runs it: its migration 85 makes the counts.
      assert.deepEqual(
        yield* migrate(filename, { toPerfForkMigrationInclusive: 0 }),
        upstreamIds.filter((id) => id > BEFORE_085_UPSTREAM_HEAD),
      );
      yield* withDatabase(filename, (db) => {
        assert.deepEqual(ledgerIds(db), upstreamIds);
        assert.deepEqual(forkObjects(db), []);
        assert.deepEqual(messageOrdinalIndexes(db), upstreamIndex);
        assert.deepEqual(countObjects(db), COUNT_OBJECTS);
        assert.deepEqual(countColumns(db), ["thread_id", "run_key", "item_count"]);
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
      });

      assert.deepEqual(yield* migrate(filename), []);
      yield* withDatabase(filename, (db) => {
        assert.deepEqual(forkLedger(db), FORK_CHAIN);
        assert.deepEqual(forkObjects(db), [`table:${PERF_FORK_MIGRATIONS_TABLE}`]);
        assert.deepEqual(messageOrdinalIndexes(db), upstreamIndex);
        insertTurnItem(db, "c1", "thread-c", "run-4");
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
      });
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("leaves the first fork builds' schema so upstream's migrations start on it", () =>
  Effect.gen(function* () {
    const filename = yield* tempDatabase;
    const exitScript = NodeFS.readFileSync(EXIT_SCRIPT_BEFORE_085, "utf8");
    const upstreamIds = migrationManifest.map(([id]) => id);
    const upstreamIndex = yield* upstream094Index;

    yield* migrate(filename, { toMigrationInclusive: EARLY_FORK_UPSTREAM_HEAD });
    yield* withDatabase(filename, (db) => {
      for (const statement of PERF_FORK_RUN_ID_TURN_ITEM_COUNTS) db.exec(statement);
      db.exec(PERF_FORK_ORDINAL_INDEX);
      db.exec(`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (77, 'ProjectionTurnItemCounts'), (78, 'TurnItemMessageOrdinalIndex');
      `);
      db.exec(exitScript);
      assert.deepEqual(forkObjects(db), []);
      assert.deepEqual(countObjects(db), []);
      assert.deepEqual(messageOrdinalIndexes(db), upstreamIndex);
      assert.deepEqual(
        ledgerIds(db),
        upstreamIds.filter((id) => id <= EARLY_FORK_UPSTREAM_HEAD),
      );
    });

    yield* migrate(filename, { toPerfForkMigrationInclusive: 0 });
    yield* withDatabase(filename, (db) => {
      assert.deepEqual(ledgerIds(db), upstreamIds);
      assert.deepEqual(forkObjects(db), []);
      assert.deepEqual(messageOrdinalIndexes(db), upstreamIndex);
      assert.deepEqual(countObjects(db), COUNT_OBJECTS);
    });
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("each exit script refuses the other's database and changes nothing", () =>
  Effect.gen(function* () {
    const before085 = yield* tempDatabase;
    yield* forkBefore085(before085);
    const after085 = yield* tempDatabase;
    yield* migrate(after085);

    for (const [filename, script, refusal] of [
      [
        before085,
        EXIT_SCRIPT,
        "migration 85 has not run: use perf-fork-schema-exit-before-085.sql",
      ],
      [after085, EXIT_SCRIPT_BEFORE_085, "migration 85 ran: use perf-fork-schema-exit.sql"],
    ] as const) {
      const exitScript = NodeFS.readFileSync(script, "utf8");
      const read = (db: NodeSqlite.DatabaseSync) => ({
        schema: schema(db),
        contents: contents(db),
      });
      const before = yield* withDatabase(filename, (db) => {
        writeTurnItems(db);
        return read(db);
      });
      yield* withDatabase(filename, (db) => assert.throws(() => db.exec(exitScript), refusal));
      yield* withDatabase(filename, (db) => assert.deepEqual(read(db), before));
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

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

const EXIT_SCRIPT = new URL("../../../../scripts/perf-fork-schema-exit.sql", import.meta.url);
// Upstream's last migration when release candidate 1 recorded the fork's at 77 and 78.
const RC1_UPSTREAM_HEAD = 76;
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

const forkObjects = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare(
      `SELECT type, name FROM sqlite_master
       WHERE name LIKE 'orchestration_v2_turn_item_counts_%'
          OR name IN (
            '${COUNTS_TABLE}', 'orchestration_v2_projection_turn_items_message_ordinal_idx',
            '${PERF_FORK_MIGRATIONS_TABLE}'
          )
       ORDER BY name`,
    )
    .all()
    .map(({ type, name }) => `${String(type)}:${String(name)}`);

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
];

const countsAsStored = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare(`SELECT thread_id, run_id, item_count FROM ${COUNTS_TABLE} ORDER BY thread_id, run_id`)
    .all();

const countsFromItems = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare(
      `SELECT thread_id, COALESCE(run_id, '') AS run_id, COUNT(*) AS item_count
       FROM orchestration_v2_projection_turn_items
       GROUP BY thread_id, COALESCE(run_id, '') ORDER BY thread_id, run_id`,
    )
    .all();

it.effect(
  "leaves the fork's schema so upstream's migrations start on it with every row kept, and the fork re-applies it",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const filename = path.join(
        yield* fs.makeTempDirectoryScoped({ prefix: "t3-fork-exit-" }),
        "statev2.sqlite",
      );
      const exitScript = NodeFS.readFileSync(EXIT_SCRIPT, "utf8");

      assert.deepEqual(
        yield* migrate(filename),
        migrationManifest.map(([id]) => id),
      );
      const expected = yield* withDatabase(filename, (db) => {
        assert.deepEqual(forkLedger(db), FORK_CHAIN);
        insertTurnItem(db, "a1", "thread-a", "run-1");
        insertTurnItem(db, "a2", "thread-a", "run-1");
        insertTurnItem(db, "a3", "thread-a", null);
        insertTurnItem(db, "b1", "thread-b", "run-2", "{not json");
        db.prepare(
          "UPDATE orchestration_v2_projection_turn_items SET run_id = 'run-3' WHERE turn_item_id = 'a2'",
        ).run();
        db.prepare(
          "DELETE FROM orchestration_v2_projection_turn_items WHERE turn_item_id = 'a1'",
        ).run();
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
        const {
          [COUNTS_TABLE]: _counts,
          [PERF_FORK_MIGRATIONS_TABLE]: _forkLedger,
          ...rest
        } = contents(db);
        return rest;
      });

      yield* withDatabase(filename, (db) => {
        db.exec(exitScript);
        // Running it again changes nothing.
        db.exec(exitScript);
        assert.deepEqual(forkObjects(db), []);
        assert.deepEqual(contents(db), expected);
      });

      // Upstream's chain alone, as an upstream build runs it.
      assert.deepEqual(yield* migrate(filename, { toPerfForkMigrationInclusive: 0 }), []);
      yield* withDatabase(filename, (db) => {
        assert.deepEqual(contents(db), expected);
        insertTurnItem(db, "c1", "thread-c", "run-4");
      });

      assert.deepEqual(yield* migrate(filename), []);
      yield* withDatabase(filename, (db) => {
        assert.deepEqual(forkLedger(db), FORK_CHAIN);
        assert.deepEqual(forkObjects(db), [
          `table:${COUNTS_TABLE}`,
          "index:orchestration_v2_projection_turn_items_message_ordinal_idx",
          "trigger:orchestration_v2_turn_item_counts_delete",
          "trigger:orchestration_v2_turn_item_counts_insert",
          "trigger:orchestration_v2_turn_item_counts_update",
          `table:${PERF_FORK_MIGRATIONS_TABLE}`,
        ]);
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
        insertTurnItem(db, "c2", "thread-c", "run-4");
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
      });
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("leaves release candidate 1's schema so upstream's migrations start on it", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const filename = path.join(
      yield* fs.makeTempDirectoryScoped({ prefix: "t3-fork-exit-" }),
      "statev2.sqlite",
    );
    const exitScript = NodeFS.readFileSync(EXIT_SCRIPT, "utf8");
    const upstreamIds = migrationManifest.map(([id]) => id);

    yield* migrate(filename, { toMigrationInclusive: RC1_UPSTREAM_HEAD });
    yield* runPerfForkMigrations().pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    yield* withDatabase(filename, (db) => {
      db.exec(`
        DROP TABLE ${PERF_FORK_MIGRATIONS_TABLE};
        DELETE FROM effect_sql_migrations WHERE migration_id > ${RC1_UPSTREAM_HEAD};
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (77, 'ProjectionTurnItemCounts'), (78, 'TurnItemMessageOrdinalIndex');
      `);
      db.exec(exitScript);
      assert.deepEqual(forkObjects(db), []);
      assert.deepEqual(
        ledgerIds(db),
        upstreamIds.filter((id) => id <= RC1_UPSTREAM_HEAD),
      );
    });

    yield* migrate(filename, { toPerfForkMigrationInclusive: 0 });
    yield* withDatabase(filename, (db) => {
      assert.deepEqual(ledgerIds(db), upstreamIds);
      assert.deepEqual(forkObjects(db), []);
    });
  }).pipe(Effect.provide(NodeServices.layer)),
);

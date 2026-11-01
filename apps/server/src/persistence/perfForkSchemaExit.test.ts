// @effect-diagnostics nodeBuiltinImport:off - runs the exit script as an operator would, on the file.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { runMigrations } from "./Migrations.ts";

const EXIT_SCRIPT = new URL("../../../../scripts/perf-fork-schema-exit.sql", import.meta.url);
const UPSTREAM_HEAD = 76;
const COUNTS_TABLE = "orchestration_v2_projection_turn_item_counts";

const migrate = (filename: string, toMigrationInclusive?: number) =>
  runMigrations({ toMigrationInclusive }).pipe(
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
          OR name IN ('${COUNTS_TABLE}', 'orchestration_v2_projection_turn_items_message_ordinal_idx')
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

      assert.deepEqual((yield* migrate(filename)).slice(-2), [77, 78]);
      const expected = yield* withDatabase(filename, (db) => {
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
        const { [COUNTS_TABLE]: _counts, ...rest } = contents(db);
        return {
          ...rest,
          effect_sql_migrations: rest.effect_sql_migrations!.filter(
            (row) => (JSON.parse(row) as { migration_id: number }).migration_id <= UPSTREAM_HEAD,
          ),
        };
      });

      yield* withDatabase(filename, (db) => {
        db.exec(exitScript);
        // Running it again changes nothing.
        db.exec(exitScript);
        assert.deepEqual(forkObjects(db), []);
        assert.deepEqual(contents(db), expected);
      });

      assert.deepEqual(yield* migrate(filename, UPSTREAM_HEAD), []);
      yield* withDatabase(filename, (db) => {
        assert.deepEqual(contents(db), expected);
        insertTurnItem(db, "c1", "thread-c", "run-4");
      });

      assert.deepEqual(yield* migrate(filename), [77, 78]);
      yield* withDatabase(filename, (db) => {
        assert.deepEqual(forkObjects(db), [
          `table:${COUNTS_TABLE}`,
          "index:orchestration_v2_projection_turn_items_message_ordinal_idx",
          "trigger:orchestration_v2_turn_item_counts_delete",
          "trigger:orchestration_v2_turn_item_counts_insert",
          "trigger:orchestration_v2_turn_item_counts_update",
        ]);
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
        insertTurnItem(db, "c2", "thread-c", "run-4");
        assert.deepEqual(countsAsStored(db), countsFromItems(db));
      });
    }).pipe(Effect.provide(NodeServices.layer)),
);

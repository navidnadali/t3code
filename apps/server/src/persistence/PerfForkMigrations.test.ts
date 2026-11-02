import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Logger from "effect/Logger";
import * as References from "effect/References";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "./Migrations.ts";

const FORK_TABLE = "t3_perf_fork_migrations";
const FORK_CHAIN: Array<[number, string]> = [
  [1, "ProjectionTurnItemCounts"],
  [2, "TurnItemMessageOrdinalIndex"],
];
// Release candidate 1 recorded the fork's chain in upstream's ledger, under upstream's next ids.
const RC1_IDS: Record<number, number> = { 1: 77, 2: 78 };
const RC1_RAN_AT = "2026-11-01 00:00:00";
const upstreamChain = migrationManifest.filter(
  ([, name]) => !FORK_CHAIN.some(([, forkName]) => forkName === name),
);
const UPSTREAM_HEAD = Math.max(...upstreamChain.map(([id]) => id));
// Upstream's last migration when release candidate 1 recorded the fork's at 77 and 78; any later
// upstream migration runs on a release candidate 1 database.
const RC1_UPSTREAM_HEAD = 76;
const upstreamSinceRc1 = upstreamChain.filter(([id]) => id > RC1_UPSTREAM_HEAD);

const ledger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
  `;
  return rows.map((row) => [row.migration_id, row.name] as const);
});

const forkLedger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const table = yield* sql`
    SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ${FORK_TABLE}
  `;
  if (table.length === 0) return [];
  return yield* sql<{
    readonly migration_id: number;
    readonly name: string;
    readonly created_at: string;
  }>`SELECT migration_id, name, created_at FROM ${sql(FORK_TABLE)} ORDER BY migration_id`;
});

const schema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name`;
});

const counts = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`
    SELECT thread_id, run_id, item_count FROM orchestration_v2_projection_turn_item_counts
    ORDER BY thread_id, run_id
  `;
});

const countedFromItems = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`
    SELECT thread_id, COALESCE(run_id, '') AS run_id, COUNT(*) AS item_count
    FROM orchestration_v2_projection_turn_items
    GROUP BY thread_id, COALESCE(run_id, '') ORDER BY thread_id, run_id
  `;
});

const insertTurnItems = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [id, threadId, runId] of [
    ["a1", "thread-a", "run-1"],
    ["a2", "thread-a", "run-1"],
    ["a3", "thread-a", null],
    ["b1", "thread-b", "run-2"],
  ] as const) {
    yield* sql`
      INSERT INTO orchestration_v2_projection_turn_items (
        turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
      ) VALUES (
        ${id}, ${threadId}, ${runId}, 1, 'assistant_message', 'completed',
        '2026-11-01T00:00:00.000Z', '{"messageId":"m1"}'
      )
    `;
  }
  // Migration 1's recount would remove this row: it stays only if 1 did not run again.
  yield* sql`
    INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, run_id, item_count)
    VALUES ('thread-stray', '', 7)
  `;
});

// From either layout to release candidate 1's: the given fork migrations recorded in upstream's
// ledger, and no fork table.
const toRc1 = (forkIds: ReadonlyArray<number>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DROP TABLE IF EXISTS ${sql(FORK_TABLE)}`;
    yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id > ${RC1_UPSTREAM_HEAD}`;
    for (const [id, name] of FORK_CHAIN) {
      if (!forkIds.includes(id)) continue;
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name, created_at)
        VALUES (${RC1_IDS[id]!}, ${name}, ${RC1_RAN_AT})
      `;
    }
  });

const TURN_ITEMS = "orchestration_v2_projection_turn_items";
// The fork's objects on the turn-item table, which an upstream rebuild of that table drops.
const FORK_OBJECTS_ON_TURN_ITEMS = [
  "orchestration_v2_projection_turn_items_message_ordinal_idx",
  "orchestration_v2_turn_item_counts_delete",
  "orchestration_v2_turn_item_counts_insert",
  "orchestration_v2_turn_item_counts_update",
];

const forkObjectsOnTurnItems = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly name: string }>`
    SELECT name FROM sqlite_master WHERE name IN ${sql.in(FORK_OBJECTS_ON_TURN_ITEMS)} ORDER BY name
  `;
  return rows.map((row) => row.name);
});

// An upstream migration that rebuilds the turn-item table (create, copy, drop, rename) and then
// recreates upstream's own indexes and triggers on it; the fork's go with the old table.
const rebuildTurnItems = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [table] = yield* sql<{ readonly sql: string }>`
    SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ${TURN_ITEMS}
  `;
  const upstreamDependents = yield* sql<{ readonly sql: string }>`
    SELECT sql FROM sqlite_master
    WHERE tbl_name = ${TURN_ITEMS} AND type IN ('index', 'trigger') AND sql IS NOT NULL
      AND name NOT IN ${sql.in(FORK_OBJECTS_ON_TURN_ITEMS)}
  `;
  yield* sql.unsafe(table!.sql.replace(TURN_ITEMS, `${TURN_ITEMS}_rebuilt`));
  yield* sql.unsafe(`INSERT INTO ${TURN_ITEMS}_rebuilt SELECT * FROM ${TURN_ITEMS}`);
  yield* sql.unsafe(`DROP TABLE ${TURN_ITEMS}`);
  yield* sql.unsafe(`ALTER TABLE ${TURN_ITEMS}_rebuilt RENAME TO ${TURN_ITEMS}`);
  for (const dependent of upstreamDependents) yield* sql.unsafe(dependent.sql);
});

const insertMoreTurnItems = (prefix: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const [index, runId] of ["run-1", "run-9", null].entries()) {
      yield* sql`
        INSERT INTO orchestration_v2_projection_turn_items (
          turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
        ) VALUES (
          ${`${prefix}-${index}`}, 'thread-a', ${runId}, ${index + 10}, 'user_message', 'completed',
          '2026-11-02T00:00:00.000Z', '{"messageId":"m2"}'
        )
      `;
    }
  });

const usesTheOrdinalIndex = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const plan = yield* sql<{ readonly detail: string }>`
    EXPLAIN QUERY PLAN
    SELECT json_extract(payload_json, '$.messageId') AS message_id, MIN(ordinal) AS ordinal
    FROM orchestration_v2_projection_turn_items
    WHERE thread_id = 'thread-a' AND type IN ('user_message', 'assistant_message')
      AND json_valid(payload_json)
    GROUP BY json_extract(payload_json, '$.messageId')
  `;
  assert.include(
    plan[0]?.detail,
    "USING INDEX orchestration_v2_projection_turn_items_message_ordinal_idx",
  );
});

const captureWarnings = (warnings: Array<unknown>) =>
  Logger.layer([
    Logger.make(({ logLevel, message, fiber }) => {
      if (logLevel === "Warn") {
        warnings.push([message, fiber.getRef(References.CurrentLogAnnotations)]);
      }
    }),
  ]);

const freshSchema = Effect.gen(function* () {
  yield* runMigrations();
  return yield* schema;
}).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));

describe("performance fork migrations", () => {
  it.effect(
    "record in their own table, so upstream's ledger ends at upstream's last migration",
    () =>
      Effect.gen(function* () {
        yield* runMigrations({ toMigrationInclusive: UPSTREAM_HEAD - 1 });
        assert.deepStrictEqual(yield* forkLedger, []);

        yield* runMigrations();
        assert.deepStrictEqual(yield* ledger, upstreamChain);
        assert.deepStrictEqual(
          (yield* forkLedger).map((row) => [row.migration_id, row.name]),
          FORK_CHAIN,
        );
        assert.deepStrictEqual(yield* runMigrations(), []);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect.each([
    { recorded: [1, 2], moved: [1, 2], dropIndex: false },
    // Builds with P1-014 but before P1-068.
    { recorded: [1], moved: [1], dropIndex: true },
    // Row 77 deleted by hand: recording 2 alone would make the migrator skip 1.
    { recorded: [2], moved: [], dropIndex: false },
  ])(
    "moves release candidate 1's rows $recorded to the fork's table without running them again",
    ({ recorded, moved, dropIndex }) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const expectedSchema = yield* freshSchema;
        yield* runMigrations();
        yield* toRc1(recorded);
        if (dropIndex) {
          yield* sql`DROP INDEX orchestration_v2_projection_turn_items_message_ordinal_idx`;
        }
        yield* insertTurnItems;
        const countsBefore = yield* counts;

        assert.deepStrictEqual(yield* runMigrations(), upstreamSinceRc1);
        assert.deepStrictEqual(yield* ledger, upstreamChain);
        const forkRows = yield* forkLedger;
        assert.deepStrictEqual(
          forkRows.map((row) => [row.migration_id, row.name]),
          FORK_CHAIN,
        );
        assert.deepStrictEqual(
          forkRows.filter((row) => row.created_at === RC1_RAN_AT).map((row) => row.migration_id),
          moved,
        );
        assert.deepStrictEqual(
          yield* counts,
          moved.includes(1) ? countsBefore : yield* countedFromItems,
        );
        assert.deepStrictEqual(yield* schema, expectedSchema);
        assert.deepStrictEqual(yield* runMigrations(), []);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("keeps the fork's own rows when release candidate 1 ran again on the database", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* toRc1([1, 2]);
      yield* sql`
        CREATE TABLE ${sql(FORK_TABLE)} (
          migration_id integer PRIMARY KEY NOT NULL,
          created_at datetime NOT NULL DEFAULT current_timestamp,
          name VARCHAR(255) NOT NULL
        )
      `;
      for (const [id, name] of FORK_CHAIN) {
        yield* sql`
          INSERT INTO ${sql(FORK_TABLE)} (migration_id, name, created_at)
          VALUES (${id}, ${name}, '2026-11-02 00:00:00')
        `;
      }

      assert.deepStrictEqual(yield* runMigrations(), upstreamSinceRc1);
      assert.deepStrictEqual(yield* ledger, upstreamChain);
      assert.deepStrictEqual(
        yield* forkLedger,
        FORK_CHAIN.map(([id, name]) => ({
          migration_id: id,
          name,
          created_at: "2026-11-02 00:00:00",
        })),
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("let upstream's next migration run on a fork database", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const forkRan = yield* forkLedger;
      const next = UPSTREAM_HEAD + 1;

      // Upstream's next build runs its chain, one id past this one's, on upstream's ledger.
      const ran = yield* Migrator.make({})({
        loader: Migrator.fromRecord({
          [`${next}_NextUpstreamMigration`]: Effect.asVoid(
            sql`CREATE TABLE next_upstream_migration (id INTEGER)`,
          ),
        }),
      });
      assert.deepStrictEqual(ran, [[next, "NextUpstreamMigration"]]);
      assert.lengthOf(
        yield* sql`SELECT 1 FROM sqlite_master WHERE name = 'next_upstream_migration'`,
        1,
      );

      // This build still starts on it, keeps upstream's row and has nothing of its own to run.
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* ledger, [...upstreamChain, [next, "NextUpstreamMigration"]]);
      assert.deepStrictEqual(yield* forkLedger, forkRan);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect(
    "re-applies a fork migration whose objects an upstream rebuild of their table dropped",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        const forkRan = yield* forkLedger;
        yield* insertTurnItems;
        const next = UPSTREAM_HEAD + 1;
        assert.deepStrictEqual(
          yield* Migrator.make({})({
            loader: Migrator.fromRecord({ [`${next}_RebuildTurnItems`]: rebuildTurnItems }),
          }),
          [[next, "RebuildTurnItems"]],
        );
        assert.deepStrictEqual(yield* forkObjectsOnTurnItems, []);
        // Writes while the triggers are gone leave the counts behind.
        yield* insertMoreTurnItems("while-missing");
        assert.notDeepEqual(yield* counts, yield* countedFromItems);

        const warnings: Array<unknown> = [];
        assert.deepStrictEqual(
          yield* runMigrations().pipe(Effect.provide(captureWarnings(warnings))),
          [],
        );
        assert.deepStrictEqual(yield* forkObjectsOnTurnItems, FORK_OBJECTS_ON_TURN_ITEMS);
        assert.deepStrictEqual(yield* forkLedger, forkRan);
        assert.deepStrictEqual(yield* counts, yield* countedFromItems);
        for (const missing of FORK_OBJECTS_ON_TURN_ITEMS) {
          assert.include(JSON.stringify(warnings), missing);
        }
        yield* insertMoreTurnItems("after");
        yield* sql`DELETE FROM orchestration_v2_projection_turn_items WHERE turn_item_id = 'a1'`;
        assert.deepStrictEqual(yield* counts, yield* countedFromItems);
        yield* usesTheOrdinalIndex;

        // With every object in place, a start runs neither body again.
        yield* sql`
          INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, run_id, item_count)
          VALUES ('thread-stray', '', 7)
        `;
        const countsWithStray = yield* counts;
        yield* runMigrations();
        assert.deepStrictEqual(yield* counts, countsWithStray);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("warns about fork migrations that a newer fork build recorded", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`
        INSERT INTO ${sql(FORK_TABLE)} (migration_id, name)
        VALUES (${FORK_CHAIN.length + 1}, 'FromNewerFork')
      `;
      const warnings: Array<unknown> = [];

      assert.deepStrictEqual(
        yield* runMigrations().pipe(Effect.provide(captureWarnings(warnings))),
        [],
      );
      assert.include(
        JSON.stringify(warnings),
        `${FORK_TABLE}:${FORK_CHAIN.length + 1}_FromNewerFork`,
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

// @effect-diagnostics nodeBuiltinImport:off - a second connection plays another server starting.
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  DATABASE_MIGRATIONS_FINISHED_MESSAGE,
  DATABASE_MIGRATIONS_STARTED_MESSAGE,
  isRunningDatabaseMigrations,
} from "@t3tools/shared/databaseMigrationLog";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as References from "effect/References";
import * as Tracer from "effect/Tracer";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import { hasPendingMigrations, migrationManifest, runMigrations } from "./Migrations.ts";
import TurnItemCounts from "./Migrations/085_TurnItemCounts.ts";
import {
  replacePerfForkTurnItemCounts,
  runPerfForkMigrations,
  UPSTREAM_TURN_ITEM_COUNTS_MIGRATION_ID,
} from "./PerfForkMigrations.ts";
import TurnItemMessageOrdinalIndex from "./PerfForkMigrations/002_TurnItemMessageOrdinalIndex.ts";
import { PERF_FORK_RUN_ID_TURN_ITEM_COUNTS } from "./testkit/PerfForkRunIdTurnItemCounts.ts";

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
// Upstream's last migration in the last fork build before upstream's migration 85.
const BEFORE_085_UPSTREAM_HEAD = 80;

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
    SELECT thread_id, run_key, item_count FROM orchestration_v2_projection_turn_item_counts
    ORDER BY thread_id, run_key
  `;
});

const forkCounts = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`
    SELECT thread_id, run_id, item_count FROM orchestration_v2_projection_turn_item_counts
    ORDER BY thread_id, run_id
  `;
});

const countedFromItems = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`
    SELECT thread_id, COALESCE(run_id, '') AS run_key, COUNT(*) AS item_count
    FROM orchestration_v2_projection_turn_items
    GROUP BY thread_id, COALESCE(run_id, '') ORDER BY thread_id, run_key
  `;
});

// A count row no turn item backs: it stays only while nothing recounts the table.
const insertStrayCount = (key: "run_id" | "run_key") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, ${sql(key)}, item_count)
      VALUES ('thread-stray', '', 7)
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
});

// A database as release candidate 1 left it: upstream's chain through 76, the fork's objects (its
// counts keyed by run_id, its index) and the given fork migrations recorded in upstream's ledger.
const atRc1 = (forkIds: ReadonlyArray<number>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: RC1_UPSTREAM_HEAD });
    for (const statement of PERF_FORK_RUN_ID_TURN_ITEM_COUNTS) yield* sql.unsafe(statement);
    yield* TurnItemMessageOrdinalIndex;
    for (const [id, name] of FORK_CHAIN) {
      if (!forkIds.includes(id)) continue;
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name, created_at)
        VALUES (${RC1_IDS[id]!}, ${name}, ${RC1_RAN_AT})
      `;
    }
  });

// A database as the last fork build before upstream's migration 85 left it: the fork chain
// recorded, its counts keyed by run_id.
const beforeUpstream085 = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations({ toMigrationInclusive: BEFORE_085_UPSTREAM_HEAD });
  for (const statement of PERF_FORK_RUN_ID_TURN_ITEM_COUNTS) yield* sql.unsafe(statement);
  yield* runPerfForkMigrations();
});

const TURN_ITEMS = "orchestration_v2_projection_turn_items";
// The fork's objects on the turn-item table, which an upstream rebuild of that table drops. The
// count triggers on it are upstream's (migration 85): such a rebuild recreates them with its own.
const ORDINAL_INDEX = "orchestration_v2_projection_turn_items_message_ordinal_idx";
const FORK_OBJECTS_ON_TURN_ITEMS = [ORDINAL_INDEX];

const forkObjectsOnTurnItems = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly name: string }>`
    SELECT name FROM sqlite_master
    WHERE name IN ${sql.in(FORK_OBJECTS_ON_TURN_ITEMS)} AND tbl_name = ${TURN_ITEMS}
    ORDER BY name
  `;
  return rows.map((row) => row.name);
});

// An upstream rebuild that keeps the old table aside: SQLite moves every trigger and index onto
// it, under the same names, and upstream then moves its own back to the new table.
const rebuildTurnItemsKeepingTheOld = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [table] = yield* sql<{ readonly sql: string }>`
    SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ${TURN_ITEMS}
  `;
  const upstreamDependents = yield* sql<{
    readonly type: string;
    readonly name: string;
    readonly sql: string;
  }>`
    SELECT type, name, sql FROM sqlite_master
    WHERE tbl_name = ${TURN_ITEMS} AND type IN ('index', 'trigger') AND sql IS NOT NULL
      AND name NOT IN ${sql.in(FORK_OBJECTS_ON_TURN_ITEMS)}
  `;
  yield* sql.unsafe(table!.sql.replace(TURN_ITEMS, `${TURN_ITEMS}_next`));
  yield* sql.unsafe(`INSERT INTO ${TURN_ITEMS}_next SELECT * FROM ${TURN_ITEMS}`);
  yield* sql.unsafe(`ALTER TABLE ${TURN_ITEMS} RENAME TO ${TURN_ITEMS}_old`);
  yield* sql.unsafe(`ALTER TABLE ${TURN_ITEMS}_next RENAME TO ${TURN_ITEMS}`);
  for (const dependent of upstreamDependents) {
    yield* sql.unsafe(`DROP ${dependent.type.toUpperCase()} main."${dependent.name}"`);
    yield* sql.unsafe(dependent.sql);
  }
});

// An upstream migration that rebuilds the turn-item table (create, copy, drop, rename) and then
// recreates upstream's own indexes and triggers on it, the count triggers among them; the fork's
// index goes with the old table.
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

// Upstream's migration 94 (a62bc8156), which merged the fork's migration 2 word for word.
const UPSTREAM_094_SQL = `
    CREATE INDEX IF NOT EXISTS orchestration_v2_projection_turn_items_message_ordinal_idx
    ON orchestration_v2_projection_turn_items(
      thread_id, json_extract(payload_json, '$.messageId'), ordinal
    )
    WHERE type IN ('user_message', 'assistant_message') AND json_valid(payload_json)
  `;

const captureWarnings = (warnings: Array<unknown>) =>
  Logger.layer([
    Logger.make(({ logLevel, message, fiber }) => {
      if (logLevel === "Warn") {
        warnings.push([message, fiber.getRef(References.CurrentLogAnnotations)]);
      }
    }),
  ]);

const restoreWarnings = (warnings: ReadonlyArray<unknown>) =>
  warnings
    .map((warning) => JSON.stringify(warning))
    .filter((text) => text.includes("restoring them"));

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
    // Builds with the turn item counts but before the message-ordinal index.
    { recorded: [1], moved: [1], dropIndex: true },
    // Row 77 deleted by hand: recording 2 alone would make the migrator skip 1.
    { recorded: [2], moved: [], dropIndex: false },
  ])(
    "moves release candidate 1's rows $recorded to the fork's table without running them again",
    ({ recorded, moved, dropIndex }) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const expectedSchema = yield* freshSchema;
        yield* atRc1(recorded);
        if (dropIndex) yield* sql.unsafe(`DROP INDEX ${ORDINAL_INDEX}`);
        yield* insertTurnItems;
        yield* insertStrayCount("run_id");

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
        // Upstream's migration 85 replaced the fork's counts with its own, counted from the items.
        assert.deepStrictEqual(yield* counts, yield* countedFromItems);
        assert.deepStrictEqual(yield* schema, expectedSchema);
        assert.deepStrictEqual(yield* runMigrations(), []);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("keeps the fork's own rows when release candidate 1 ran again on the database", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* atRc1([1, 2]);
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

  it.effect.each([
    { from: "a fresh database", setup: Effect.void, upgrade: upstreamChain },
    {
      from: "0.1.7-perf.6's (upstream 80, fork 1 and 2)",
      setup: beforeUpstream085,
      upgrade: upstreamChain.filter(([id]) => id > BEFORE_085_UPSTREAM_HEAD),
    },
  ])(
    "let upstream's migration 94, the fork's message-ordinal index, run as a no-op on $from",
    ({ setup, upgrade }) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* setup;
        assert.deepStrictEqual(yield* runMigrations(), upgrade);
        const forkRan = yield* forkLedger;
        assert.deepStrictEqual(
          forkRan.map((row) => [row.migration_id, row.name]),
          FORK_CHAIN,
        );
        yield* insertTurnItems;
        yield* insertMoreTurnItems("before-094");
        const schemaBefore = yield* schema;

        assert.deepStrictEqual(
          yield* Migrator.make({})({
            loader: Migrator.fromRecord({
              "94_TurnItemMessageOrdinalIndex": Effect.asVoid(sql.unsafe(UPSTREAM_094_SQL)),
            }),
          }),
          [[94, "TurnItemMessageOrdinalIndex"]],
        );
        assert.deepStrictEqual(yield* schema, schemaBefore);
        // SQLite keeps the statement without IF NOT EXISTS: the fork's index is 094's own.
        assert.deepStrictEqual(
          yield* sql`SELECT type, tbl_name, sql FROM sqlite_master WHERE name = ${ORDINAL_INDEX}`,
          [
            {
              type: "index",
              tbl_name: TURN_ITEMS,
              sql: UPSTREAM_094_SQL.trimStart().replace("IF NOT EXISTS ", ""),
            },
          ],
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT name FROM sqlite_master
            WHERE type = 'index' AND tbl_name = ${TURN_ITEMS} AND instr(sql, '$.messageId') > 0
          `,
          [{ name: ORDINAL_INDEX }],
        );
        yield* usesTheOrdinalIndex;

        assert.isFalse(yield* hasPendingMigrations());
        assert.deepStrictEqual(yield* runMigrations(), []);
        assert.deepStrictEqual(yield* ledger, [
          ...upstreamChain,
          [94, "TurnItemMessageOrdinalIndex"],
        ]);
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
        yield* insertMoreTurnItems("while-missing");

        const warnings: Array<unknown> = [];
        assert.deepStrictEqual(
          yield* runMigrations().pipe(Effect.provide(captureWarnings(warnings))),
          [],
        );
        assert.deepStrictEqual(yield* forkObjectsOnTurnItems, FORK_OBJECTS_ON_TURN_ITEMS);
        assert.deepStrictEqual(yield* forkLedger, forkRan);
        const [restored, ...more] = restoreWarnings(warnings);
        assert.deepStrictEqual(more, []);
        assert.include(restored, "t3_perf_fork_migrations:2_TurnItemMessageOrdinalIndex");
        for (const missing of FORK_OBJECTS_ON_TURN_ITEMS) {
          assert.include(restored, missing);
        }
        yield* insertMoreTurnItems("after");
        yield* sql`DELETE FROM orchestration_v2_projection_turn_items WHERE turn_item_id = 'a1'`;
        assert.deepStrictEqual(yield* counts, yield* countedFromItems);
        yield* usesTheOrdinalIndex;

        // With every object in place, a start runs no body again and leaves upstream's counts be.
        yield* insertStrayCount("run_key");
        const countsWithStray = yield* counts;
        const quiet: Array<unknown> = [];
        yield* runMigrations().pipe(Effect.provide(captureWarnings(quiet)));
        assert.deepStrictEqual(restoreWarnings(quiet), []);
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

  it.effect("restores missing objects once, and warns once, when starts run concurrently", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* insertTurnItems;
      yield* sql.unsafe(`DROP INDEX ${ORDINAL_INDEX}`);
      yield* insertMoreTurnItems("while-missing");
      const warnings: Array<unknown> = [];

      yield* Effect.all([runMigrations(), runMigrations(), runMigrations()], {
        concurrency: "unbounded",
      }).pipe(Effect.provide(captureWarnings(warnings)));
      const restoring = (migration: string) =>
        warnings.filter((warning) => {
          const text = JSON.stringify(warning);
          return text.includes("restoring them") && text.includes(migration);
        });
      assert.lengthOf(restoring("t3_perf_fork_migrations:1_ProjectionTurnItemCounts"), 0);
      assert.lengthOf(restoring("t3_perf_fork_migrations:2_TurnItemMessageOrdinalIndex"), 1);
      assert.deepStrictEqual(yield* forkObjectsOnTurnItems, FORK_OBJECTS_ON_TURN_ITEMS);
      assert.deepStrictEqual(yield* counts, yield* countedFromItems);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("warns only when this start restores the objects, not when another just did", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const filename = path.join(
        yield* fs.makeTempDirectoryScoped({ prefix: "t3-fork-heal-" }),
        "statev2.sqlite",
      );
      const atFile = NodeSqliteClient.layer({ filename });
      const restore = yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        const created = yield* sql<{ readonly sql: string }>`
          SELECT sql FROM sqlite_master WHERE name IN ${sql.in(FORK_OBJECTS_ON_TURN_ITEMS)}
        `;
        yield* sql.unsafe(`DROP INDEX ${ORDINAL_INDEX}`);
        yield* insertMoreTurnItems("while-missing");
        return created.map((row) => row.sql);
      }).pipe(Effect.provide(atFile));

      // Another start restores the objects right after this start's first check finds them
      // missing.
      let restoredElsewhere = false;
      const anotherStart = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          if (options.name !== "inspectPerfForkObjects") return span;
          const end = span.end.bind(span);
          span.end = (endTime, exit) => {
            end(endTime, exit);
            if (restoredElsewhere) return;
            restoredElsewhere = true;
            const db = new NodeSqlite.DatabaseSync(filename);
            try {
              db.exec("BEGIN IMMEDIATE");
              for (const statement of restore) db.exec(statement);
              db.exec("COMMIT");
            } finally {
              db.close();
            }
          };
          return span;
        },
      });
      const warnings: Array<unknown> = [];

      yield* Effect.gen(function* () {
        yield* runMigrations().pipe(
          Effect.withTracer(anotherStart),
          Effect.provide(captureWarnings(warnings)),
        );
        assert.isTrue(restoredElsewhere);
        assert.notInclude(JSON.stringify(warnings), "restoring them");
        assert.deepStrictEqual(yield* forkObjectsOnTurnItems, FORK_OBJECTS_ON_TURN_ITEMS);
      }).pipe(Effect.provide(atFile));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("names the fork migration and its missing objects when running it again fails", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql.unsafe(`DROP INDEX ${ORDINAL_INDEX}`);
      // A table under the index's name stops its CREATE INDEX IF NOT EXISTS.
      yield* sql.unsafe(`CREATE TABLE ${ORDINAL_INDEX} (id INTEGER)`);

      const exit = yield* Effect.exit(runMigrations());
      assert.isTrue(Exit.isFailure(exit));
      assert.include(String(exit), "MigrationError");
      assert.include(String(exit), "t3_perf_fork_migrations:2_TurnItemMessageOrdinalIndex");
      assert.include(String(exit), ORDINAL_INDEX);
      assert.deepStrictEqual(yield* forkObjectsOnTurnItems, []);

      // A later start tries again.
      yield* sql.unsafe(`DROP TABLE ${ORDINAL_INDEX}`);
      yield* runMigrations();
      assert.deepStrictEqual(yield* forkObjectsOnTurnItems, FORK_OBJECTS_ON_TURN_ITEMS);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect(
    "moves the fork's objects back from a turn-item table an upstream rebuild kept aside",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        yield* insertTurnItems;
        const next = UPSTREAM_HEAD + 1;
        yield* Migrator.make({})({
          loader: Migrator.fromRecord({
            [`${next}_RebuildTurnItemsKeepingTheOld`]: rebuildTurnItemsKeepingTheOld,
          }),
        });
        assert.deepStrictEqual(yield* forkObjectsOnTurnItems, []);
        yield* insertMoreTurnItems("while-aside");

        yield* runMigrations();
        assert.deepStrictEqual(yield* forkObjectsOnTurnItems, FORK_OBJECTS_ON_TURN_ITEMS);
        assert.deepStrictEqual(
          yield* sql`SELECT name FROM sqlite_master WHERE tbl_name = ${`${TURN_ITEMS}_old`}
          AND name IN ${sql.in(FORK_OBJECTS_ON_TURN_ITEMS)}`,
          [],
        );
        yield* insertMoreTurnItems("after");
        assert.deepStrictEqual(yield* counts, yield* countedFromItems);
        yield* usesTheOrdinalIndex;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

const dropOrdinalIndex = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`DROP INDEX ${ORDINAL_INDEX}`);
});

// Upstream's chain current, and the fork's chain left with work of each kind it does.
const upstreamCurrentWithForkWork = [
  { work: "no fork ledger yet", setup: runMigrations({ toPerfForkMigrationInclusive: 0 }) },
  { work: "a fork migration to run", setup: runMigrations({ toPerfForkMigrationInclusive: 1 }) },
  { work: "a fork index to restore", setup: Effect.andThen(runMigrations(), dropOrdinalIndex) },
];

describe("pending migration work", () => {
  it.effect.each(upstreamCurrentWithForkWork)(
    "counts the fork's chain when upstream's is current: $work",
    ({ setup }) =>
      Effect.gen(function* () {
        yield* setup;
        assert.isTrue(yield* hasPendingMigrations());
        yield* runMigrations();
        assert.isFalse(yield* hasPendingMigrations());
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("includes a release candidate 1 row the fork's chain would move", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      // Hand-edited: upstream's own row at 78 under the name release candidate 1 recorded there.
      yield* sql`
        UPDATE effect_sql_migrations SET name = ${FORK_CHAIN[1]![1]} WHERE migration_id = ${RC1_IDS[2]!}
      `;
      assert.isTrue(yield* hasPendingMigrations());
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

// The start's output as the desktop reads it, while the fork's chain runs and once it is done.
const migrationLines = <A, E>(start: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  Effect.gen(function* () {
    const lines: Array<string> = [];
    const whileForkChainRuns: Array<boolean> = [];
    const tracer = Tracer.make({
      span(options) {
        if (options.name === "runPerfForkMigrations") {
          whileForkChainRuns.push(isRunningDatabaseMigrations(lines.join("\n")));
        }
        return new Tracer.NativeSpan(options);
      },
    });
    yield* start.pipe(
      Effect.withTracer(tracer),
      Effect.provide(
        Logger.layer([
          Logger.make(({ message }) => {
            lines.push(Array.isArray(message) ? message.join(" ") : String(message));
          }),
        ]),
      ),
    );
    const count = (marker: string) => lines.filter((line) => line.includes(marker)).length;
    return {
      whileForkChainRuns,
      afterwards: isRunningDatabaseMigrations(lines.join("\n")),
      started: count(DATABASE_MIGRATIONS_STARTED_MESSAGE),
      finished: count(DATABASE_MIGRATIONS_FINISHED_MESSAGE),
    };
  });

describe("migration lifecycle lines", () => {
  it.effect.each([
    {
      work: "upstream's last migration and the whole fork chain",
      setup: runMigrations({ toMigrationInclusive: UPSTREAM_HEAD - 1 }),
    },
    { work: "upstream's migration 85 over the fork's counts", setup: beforeUpstream085 },
    ...upstreamCurrentWithForkWork,
  ])("keep the desktop waiting until the fork's chain is done: $work", ({ setup }) =>
    Effect.gen(function* () {
      yield* setup;

      assert.deepStrictEqual(yield* migrationLines(runMigrations()), {
        whileForkChainRuns: [true],
        afterwards: false,
        started: 1,
        finished: 1,
      });
      assert.deepStrictEqual(yield* migrationLines(runMigrations()), {
        whileForkChainRuns: [false],
        afterwards: false,
        started: 0,
        finished: 0,
      });
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

describe("replacing the fork's turn-item counts with upstream's migration 85", () => {
  const countMigration = <E>(migration: Effect.Effect<void, E, SqlClient.SqlClient>) =>
    Migrator.make({})({
      loader: Migrator.fromRecord({
        [`${UPSTREAM_TURN_ITEM_COUNTS_MIGRATION_ID}_TurnItemCounts`]: migration,
      }),
    });

  it.effect("rolls the drop back when the migration it wraps fails", () =>
    Effect.gen(function* () {
      yield* beforeUpstream085;
      yield* insertTurnItems;
      yield* insertStrayCount("run_id");
      const schemaBefore = yield* schema;
      const countsBefore = yield* forkCounts;

      const exit = yield* Effect.exit(
        replacePerfForkTurnItemCounts(
          countMigration(
            TurnItemCounts.pipe(Effect.andThen(Effect.fail("after creating the counts"))),
          ),
        ),
      );
      assert.isTrue(Exit.isFailure(exit));
      assert.include(String(exit), `${UPSTREAM_TURN_ITEM_COUNTS_MIGRATION_ID}_TurnItemCounts`);
      // The fork's table, its triggers and its counts, stray row included, as they were.
      assert.deepStrictEqual(yield* schema, schemaBefore);
      assert.deepStrictEqual(yield* forkCounts, countsBefore);
      assert.deepStrictEqual(
        (yield* ledger).at(-1),
        upstreamChain.find(([id]) => id === BEFORE_085_UPSTREAM_HEAD),
      );

      assert.deepStrictEqual(
        yield* runMigrations(),
        upstreamChain.filter(([id]) => id > BEFORE_085_UPSTREAM_HEAD),
      );
      assert.deepStrictEqual(yield* counts, yield* countedFromItems);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("leaves upstream's counts when another start already ran migration 85", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      yield* insertTurnItems;
      yield* insertStrayCount("run_key");
      const schemaBefore = yield* schema;
      const countsBefore = yield* counts;

      assert.deepStrictEqual(
        yield* replacePerfForkTurnItemCounts(countMigration(TurnItemCounts)),
        [],
      );
      assert.deepStrictEqual(yield* schema, schemaBefore);
      assert.deepStrictEqual(yield* counts, countsBefore);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

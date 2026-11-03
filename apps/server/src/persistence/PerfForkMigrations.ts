import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import Migration0001 from "./PerfForkMigrations/001_ProjectionTurnItemCounts.ts";
import Migration0002 from "./PerfForkMigrations/002_TurnItemMessageOrdinalIndex.ts";

/**
 * The performance fork's schema changes run as a chain of their own, recorded in this table, so
 * upstream's ledger keeps only upstream's ids and every upstream migration still runs. Add fork
 * migrations here, never to `Migrations.ts`.
 */
export const PERF_FORK_MIGRATIONS_TABLE = "t3_perf_fork_migrations";

const TURN_ITEMS = "orchestration_v2_projection_turn_items";
const TURN_ITEM_COUNTS = "orchestration_v2_projection_turn_item_counts";

type PerfForkObject = readonly [type: "table" | "trigger" | "index", name: string, table: string];

// [id, name, migration, the objects it leaves in place]
const perfForkMigrationEntries = [
  [
    1,
    "ProjectionTurnItemCounts",
    Migration0001,
    [
      ["table", TURN_ITEM_COUNTS, TURN_ITEM_COUNTS],
      ["trigger", "orchestration_v2_turn_item_counts_insert", TURN_ITEMS],
      ["trigger", "orchestration_v2_turn_item_counts_update", TURN_ITEMS],
      ["trigger", "orchestration_v2_turn_item_counts_delete", TURN_ITEMS],
    ],
  ],
  [
    2,
    "TurnItemMessageOrdinalIndex",
    Migration0002,
    [["index", "orchestration_v2_projection_turn_items_message_ordinal_idx", TURN_ITEMS]],
  ],
] as const;

export const PERF_FORK_MIGRATION_HEAD = Math.max(...perfForkMigrationEntries.map(([id]) => id));

/** How logs, reports and the service preflight name a migration of this chain. */
export const perfForkMigrationLabel = ([id, name]: readonly [number, string]) =>
  `${PERF_FORK_MIGRATIONS_TABLE}:${id}_${name}`;

// Release candidate 1 recorded the chain in upstream's ledger: [ledger id, name, chain id].
const RC1_LEDGER_ROWS = [
  [77, "ProjectionTurnItemCounts", 1],
  [78, "TurnItemMessageOrdinalIndex", 2],
] as const;

export const isRc1PerfForkMigration = (row: { readonly id: number; readonly name: string }) =>
  RC1_LEDGER_ROWS.some(([id, name]) => row.id === id && row.name === name);

// Effect's own DDL for a migrations table, so a moved database's schema matches a fresh one.
const createLedger = (sql: SqlClient.SqlClient) =>
  sql`CREATE TABLE IF NOT EXISTS ${sql(PERF_FORK_MIGRATIONS_TABLE)} (
  migration_id integer PRIMARY KEY NOT NULL,
  created_at datetime NOT NULL DEFAULT current_timestamp,
  name VARCHAR(255) NOT NULL
)`;

/**
 * Moves release candidate 1's rows from upstream's ledger into this chain's, keeping when they
 * ran, so those migrations do not run again. A row recorded without every earlier migration of
 * the chain is dropped instead: the migrator would skip those; the bodies are safe to run again.
 */
export const moveRc1PerfForkMigrations = Effect.fn("moveRc1PerfForkMigrations")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      const upstreamLedger = yield* sql`
        SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'
      `;
      if (upstreamLedger.length === 0) return;
      const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
        SELECT migration_id, name FROM effect_sql_migrations
        WHERE migration_id IN ${sql.in(RC1_LEDGER_ROWS.map(([id]) => id))}
      `;
      const found = RC1_LEDGER_ROWS.filter(([id, name]) =>
        rows.some((row) => row.migration_id === id && row.name === name),
      );
      if (found.length === 0) return;
      yield* createLedger(sql);
      const recorded = new Set(
        (yield* sql<{ readonly migration_id: number }>`
          SELECT migration_id FROM ${sql(PERF_FORK_MIGRATIONS_TABLE)}
        `).map((row) => row.migration_id),
      );
      for (const [ledgerId, name, id] of found) {
        if (perfForkMigrationEntries.every(([earlier]) => earlier >= id || recorded.has(earlier))) {
          yield* sql`
            INSERT OR IGNORE INTO ${sql(PERF_FORK_MIGRATIONS_TABLE)} (migration_id, name, created_at)
            SELECT ${id}, name, created_at FROM effect_sql_migrations
            WHERE migration_id = ${ledgerId} AND name = ${name}
          `;
          recorded.add(id);
        }
        yield* sql`
          DELETE FROM effect_sql_migrations WHERE migration_id = ${ledgerId} AND name = ${name}
        `;
      }
    }),
  );
});

// Objects not in place on their table, and those of them that sit on another table: a rebuild
// that keeps the old table aside takes the triggers and indexes with it, under the same names.
const inspectObjects = Effect.fn("inspectPerfForkObjects")(function* (
  objects: ReadonlyArray<PerfForkObject>,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly type: string;
    readonly name: string;
    readonly tbl_name: string;
  }>`
    SELECT type, name, tbl_name FROM sqlite_master WHERE name IN ${sql.in(objects.map(([, name]) => name))}
  `;
  const missing = objects.filter(
    ([type, name, table]) =>
      !rows.some((row) => row.type === type && row.name === name && row.tbl_name === table),
  );
  const misplaced = missing.filter(
    ([type, name]) =>
      type !== "table" && rows.some((row) => row.type === type && row.name === name),
  );
  return { missing, misplaced };
});

const run = Migrator.make({});

export const runPerfForkMigrations = Effect.fn("runPerfForkMigrations")(function* (
  throughId?: number,
) {
  const entries = perfForkMigrationEntries.filter(
    ([id]) => throughId === undefined || id <= throughId,
  );
  if (entries.length === 0) return [];
  const executed = yield* run({
    loader: Migrator.fromRecord(
      Object.fromEntries(entries.map(([id, name, migration]) => [`${id}_${name}`, migration])),
    ),
    table: PERF_FORK_MIGRATIONS_TABLE,
  });
  if (executed.length > 0) {
    yield* Effect.log("Performance fork migrations ran successfully").pipe(
      Effect.annotateLogs({ migrations: executed.map(perfForkMigrationLabel) }),
    );
  }

  const sql = yield* SqlClient.SqlClient;
  const recorded = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM ${sql(PERF_FORK_MIGRATIONS_TABLE)}
  `;
  // An upstream migration that rebuilds a table drops the fork's triggers and indexes on it (or
  // moves them to the old table it keeps), and the migrator never runs a recorded migration
  // again: run one whose objects are not in place again.
  for (const [id, name, migration, objects] of entries) {
    if (!recorded.some((row) => row.migration_id === id && row.name === name)) continue;
    const found = yield* inspectObjects(objects);
    if (found.missing.length === 0) continue;
    const label = perfForkMigrationLabel([id, name]);
    const missing = found.missing.map(([, object]) => object);
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          // Under the write lock: another start may have restored them since the check above.
          const now = yield* inspectObjects(objects);
          if (now.missing.length === 0) return;
          yield* Effect.logWarning(
            "A performance fork migration's objects are missing; restoring them.",
          ).pipe(
            Effect.annotateLogs({
              migration: label,
              missing: now.missing.map(([, object]) => object),
            }),
          );
          for (const [type, object] of now.misplaced) {
            yield* sql.unsafe(`DROP ${type === "trigger" ? "TRIGGER" : "INDEX"} main."${object}"`);
          }
          yield* migration;
        }),
      )
      .pipe(
        Effect.mapError(
          (cause) =>
            new Migrator.MigrationError({
              kind: "Failed",
              message: `Running ${label} again failed; missing: ${missing.join(", ")}`,
              cause,
            }),
        ),
      );
  }

  const names = new Map<number, string>(perfForkMigrationEntries.map(([id, name]) => [id, name]));
  const divergent = recorded.flatMap((row) => {
    const expected = names.get(row.migration_id);
    const label = perfForkMigrationLabel([row.migration_id, row.name]);
    if (expected === undefined) return [`${label} (unknown to this build)`];
    return expected === row.name ? [] : [`${label} (this build: ${expected})`];
  });
  if (divergent.length > 0) {
    yield* Effect.logWarning(
      "Performance fork migration history diverges from this build; recorded migration ids are skipped, not reconciled by name.",
    ).pipe(Effect.annotateLogs({ divergent }));
  }
  return executed;
});

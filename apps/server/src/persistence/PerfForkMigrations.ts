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

const perfForkMigrationEntries = [
  [1, "ProjectionTurnItemCounts", Migration0001],
  [2, "TurnItemMessageOrdinalIndex", Migration0002],
] as const;

export const PERF_FORK_MIGRATION_HEAD = Math.max(...perfForkMigrationEntries.map(([id]) => id));

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
      for (const [ledgerId, , id] of found) {
        if (perfForkMigrationEntries.every(([earlier]) => earlier >= id || recorded.has(earlier))) {
          yield* sql`
            INSERT OR IGNORE INTO ${sql(PERF_FORK_MIGRATIONS_TABLE)} (migration_id, name, created_at)
            SELECT ${id}, name, created_at FROM effect_sql_migrations WHERE migration_id = ${ledgerId}
          `;
          recorded.add(id);
        }
        yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id = ${ledgerId}`;
      }
    }),
  );
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
      Effect.annotateLogs({ migrations: executed.map(([id, name]) => `${id}_${name}`) }),
    );
  }
  return executed;
});

import { DATABASE_MIGRATION_PROGRESS_MESSAGE } from "@t3tools/shared/databaseMigrationLog";
import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import Migration0001 from "./PerfForkMigrations/001_ProjectionTurnItemCounts.ts";
import Migration0002 from "./PerfForkMigrations/002_TurnItemMessageOrdinalIndex.ts";
import Migration0003, {
  THREAD_ITEM_TOTALS,
  THREAD_ITEM_TOTALS_TRIGGERS,
} from "./PerfForkMigrations/003_ThreadItemTotals.ts";
import * as EventLogPruneState from "./PerfForkMigrations/EventLogPruneState.ts";

/**
 * The performance fork's schema changes run as a chain of their own, recorded in this table, so
 * upstream's ledger keeps only upstream's ids and every upstream migration still runs. Add fork
 * migrations here, never to `Migrations.ts`.
 */
export const PERF_FORK_MIGRATIONS_TABLE = "t3_perf_fork_migrations";

const TURN_ITEM_COUNTS = "orchestration_v2_projection_turn_item_counts";

type PerfForkObject = readonly [type: "table" | "trigger" | "index", name: string, table: string];

const TURN_ITEM_COUNT_TRIGGERS = [
  "orchestration_v2_turn_item_counts_insert",
  "orchestration_v2_turn_item_counts_update",
  "orchestration_v2_turn_item_counts_delete",
] as const;

// [id, name, migration, the objects it leaves in place]
const perfForkMigrationEntries = [
  // Upstream's migration 85 owns the turn-item counts now; see replacePerfForkTurnItemCounts.
  [1, "ProjectionTurnItemCounts", Migration0001, []],
  // Upstream's migration 94 owns the message-ordinal index now.
  [2, "TurnItemMessageOrdinalIndex", Migration0002, []],
  // The shell's item count starts from these totals instead of summing a thread's runs.
  [
    3,
    "ThreadItemTotals",
    Migration0003,
    [
      ["table", THREAD_ITEM_TOTALS, THREAD_ITEM_TOTALS],
      ...THREAD_ITEM_TOTALS_TRIGGERS.map(([name, table]) => ["trigger", name, table] as const),
    ],
  ],
  // The event-log compaction's state. Its id is set here only; a renumber is this line.
  // 4, after the shell's ThreadItemTotals (3) in ship order.
  [
    4,
    EventLogPruneState.NAME,
    EventLogPruneState.default,
    [["table", EventLogPruneState.TABLE, EventLogPruneState.TABLE]],
  ],
] as const satisfies ReadonlyArray<
  readonly [
    number,
    string,
    Effect.Effect<void, unknown, SqlClient.SqlClient>,
    ReadonlyArray<PerfForkObject>,
  ]
>;

export const PERF_FORK_MIGRATION_IDS: ReadonlyArray<number> = perfForkMigrationEntries.map(
  ([id]) => id,
);

/** The chain this build ships, [id, name] in order: what a fresh database runs. */
export const PERF_FORK_MIGRATIONS: ReadonlyArray<readonly [number, string]> =
  perfForkMigrationEntries.map(([id, name]) => [id, name] as const);

export const PERF_FORK_MIGRATION_HEAD = Math.max(...PERF_FORK_MIGRATION_IDS);

/** How logs, reports and the service preflight name a migration of this chain. */
export const perfForkMigrationLabel = ([id, name]: readonly [number, string]) =>
  `${PERF_FORK_MIGRATIONS_TABLE}:${id}_${name}`;

/**
 * The migrator runs only the ids above the highest one a database recorded, so the chain must run
 * 1, 2, ..., N in order: an id out of order is a migration some database never runs.
 */
export const perfForkChainProblems = (ids: ReadonlyArray<number>): ReadonlyArray<string> =>
  ids.flatMap((id, index) =>
    id === index + 1 ? [] : [`fork migration ${id} is listed where ${index + 1} belongs`],
  );

/**
 * What a database's fork ledger holds that this build's chain cannot run safely: an id the build
 * does not know (a newer or another branch's build recorded it), a different migration under an id
 * it knows, or an id missing below the highest recorded one, which the migrator would never run.
 */
export const perfForkLedgerProblems = (
  recorded: ReadonlyArray<{ readonly id: number; readonly name: string }>,
  chain: ReadonlyArray<
    readonly [number, string, ...ReadonlyArray<unknown>]
  > = perfForkMigrationEntries,
): ReadonlyArray<string> => {
  const names = new Map<number, string>(chain.map(([id, name]) => [id, name]));
  const ids = new Set(recorded.map(({ id }) => id));
  const highest = Math.max(0, ...ids);
  const problems: Array<string> = [];
  for (const { id, name } of recorded) {
    const expected = names.get(id);
    if (expected === undefined) {
      problems.push(`${perfForkMigrationLabel([id, name])} (unknown to this build)`);
    } else if (expected !== name) {
      problems.push(`${perfForkMigrationLabel([id, name])} (this build: ${expected})`);
    }
  }
  for (let id = 1; id < highest; id++) {
    if (ids.has(id)) continue;
    problems.push(
      `${perfForkMigrationLabel([id, names.get(id) ?? "unknown"])} (never recorded, below ${highest})`,
    );
  }
  return problems;
};

// The fork's first builds recorded the chain in upstream's ledger: [ledger id, name, chain id].
const LEGACY_LEDGER_ROWS = [
  [77, "ProjectionTurnItemCounts", 1],
  [78, "TurnItemMessageOrdinalIndex", 2],
] as const;

export const isLegacyPerfForkMigration = (row: { readonly id: number; readonly name: string }) =>
  LEGACY_LEDGER_ROWS.some(([id, name]) => row.id === id && row.name === name);

// Untraced helpers below run inside their caller's span.
const legacyLedgerRows = Effect.fnUntraced(function* () {
  const sql = yield* SqlClient.SqlClient;
  const upstreamLedger = yield* sql`
    SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'
  `;
  if (upstreamLedger.length === 0) return [];
  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM effect_sql_migrations
    WHERE migration_id IN ${sql.in(LEGACY_LEDGER_ROWS.map(([id]) => id))}
  `;
  return LEGACY_LEDGER_ROWS.filter(([id, name]) =>
    rows.some((row) => row.migration_id === id && row.name === name),
  );
});

// Effect's own DDL for a migrations table, so a moved database's schema matches a fresh one.
const createLedger = (sql: SqlClient.SqlClient) =>
  sql`CREATE TABLE IF NOT EXISTS ${sql(PERF_FORK_MIGRATIONS_TABLE)} (
  migration_id integer PRIMARY KEY NOT NULL,
  created_at datetime NOT NULL DEFAULT current_timestamp,
  name VARCHAR(255) NOT NULL
)`;

const ledgerRefusal = (problems: ReadonlyArray<string>) =>
  new Migrator.MigrationError({
    kind: "Failed",
    message: `This database's performance fork migration history cannot be run by this build: ${problems.join(", ")}. Start the build that recorded it, or apply that build's documented down step first.`,
  });

const recordedForkLedger = Effect.fnUntraced(function* () {
  const sql = yield* SqlClient.SqlClient;
  const ledger = yield* sql`
    SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ${PERF_FORK_MIGRATIONS_TABLE}
  `;
  if (ledger.length === 0) return [];
  return (yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM ${sql(PERF_FORK_MIGRATIONS_TABLE)}
  `).map((row) => ({ id: row.migration_id, name: row.name }));
});

/**
 * Refuses a fork ledger this build cannot run before a start writes anything: the first fork
 * builds' move, upstream's chain and the fork's. The first fork builds' rows count as the fork ids the
 * move would record (it records one only after every earlier id).
 */
export const assertPerfForkLedgerRunnable = Effect.fn("assertPerfForkLedgerRunnable")(function* () {
  const recorded = yield* recordedForkLedger();
  const ids = new Set(recorded.map(({ id }) => id));
  const moving: Array<{ readonly id: number; readonly name: string }> = [];
  for (const [, name, id] of yield* legacyLedgerRows()) {
    if (ids.has(id)) continue;
    if (perfForkMigrationEntries.every(([earlier]) => earlier >= id || ids.has(earlier))) {
      moving.push({ id, name });
      ids.add(id);
    }
  }
  const problems = perfForkLedgerProblems([...recorded, ...moving]);
  if (problems.length > 0) return yield* ledgerRefusal(problems);
});

/**
 * Moves the first fork builds' rows from upstream's ledger into this chain's, keeping when they
 * ran, so those migrations do not run again. A row recorded without every earlier migration of
 * the chain is dropped instead: the migrator would skip those; the bodies are safe to run again.
 */
export const moveLegacyPerfForkMigrations = Effect.fn("moveLegacyPerfForkMigrations")(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Checked before the write lock too: a current database's start must not take it.
  if ((yield* legacyLedgerRows()).length === 0) return;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      const found = yield* legacyLedgerRows();
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

/** Upstream's migration that creates the turn-item count table and triggers fork migration 1 made. */
export const UPSTREAM_TURN_ITEM_COUNTS_MIGRATION_ID = 85;

/**
 * Runs upstream's count migration in one transaction with dropping the count table and triggers
 * fork migration 1 left (keyed by run_id): it creates them under the same names without IF NOT
 * EXISTS and backfills its own. Checked under the write lock, so another start's table stays.
 */
export const replacePerfForkTurnItemCounts = Effect.fn("replacePerfForkTurnItemCounts")(function* <
  A,
  E,
  R,
>(upstreamMigration: Effect.Effect<A, E, R>) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const columns = yield* sql<{ readonly name: string }>`
        SELECT name FROM pragma_table_info(${TURN_ITEM_COUNTS})
      `;
      if (columns.some((column) => column.name === "run_id")) {
        for (const trigger of TURN_ITEM_COUNT_TRIGGERS) {
          yield* sql.unsafe(`DROP TRIGGER IF EXISTS main."${trigger}"`);
        }
        yield* sql.unsafe(`DROP TABLE main."${TURN_ITEM_COUNTS}"`);
        yield* Effect.log(
          "Replacing the performance fork's turn-item counts with upstream's migration",
        ).pipe(
          Effect.annotateLogs({
            migration: perfForkMigrationLabel([1, "ProjectionTurnItemCounts"]),
          }),
        );
      }
      return yield* upstreamMigration;
    }),
  );
});

// Objects not in place on their table, and those of them that sit on another table: a rebuild
// that keeps the old table aside takes the triggers and indexes with it, under the same names.
const findObjects = Effect.fnUntraced(function* (objects: ReadonlyArray<PerfForkObject>) {
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

const inspectObjects = Effect.fn("inspectPerfForkObjects")(function* (
  objects: ReadonlyArray<PerfForkObject>,
) {
  return yield* findObjects(objects);
});

const chainEntries = (throughId: number | undefined) =>
  perfForkMigrationEntries.filter(([id]) => throughId === undefined || id <= throughId);

const isRecorded = (
  recorded: ReadonlyArray<{ readonly migration_id: number; readonly name: string }>,
  id: number,
  name: string,
) => recorded.some((row) => row.migration_id === id && row.name === name);

// The recorded rows, and the migrations above the highest recorded id, which the migrator runs.
const unrecordedEntries = Effect.fnUntraced(function* (throughId: number | undefined) {
  const sql = yield* SqlClient.SqlClient;
  const ledger = yield* sql`
    SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ${PERF_FORK_MIGRATIONS_TABLE}
  `;
  const recorded =
    ledger.length === 0
      ? []
      : yield* sql<{ readonly migration_id: number; readonly name: string }>`
          SELECT migration_id, name FROM ${sql(PERF_FORK_MIGRATIONS_TABLE)}
        `;
  const latest = Math.max(0, ...recorded.map((row) => row.migration_id));
  return { recorded, unrecorded: chainEntries(throughId).filter(([id]) => id > latest) };
});

/**
 * Whether a start's fork chain writes anything: the first fork builds' rows to move, migrations to
 * run, or a recorded migration's objects to restore. A few small reads, cheap on every start.
 */
export const hasPendingPerfForkMigrations = Effect.fn("hasPendingPerfForkMigrations")(function* (
  throughId?: number,
) {
  if ((yield* legacyLedgerRows()).length > 0) return true;
  const { recorded, unrecorded } = yield* unrecordedEntries(throughId);
  if (unrecorded.length > 0) return true;
  for (const [id, name, , objects] of chainEntries(throughId)) {
    if (objects.length === 0 || !isRecorded(recorded, id, name)) continue;
    if ((yield* findObjects(objects)).missing.length > 0) return true;
  }
  return false;
});

const run = Migrator.make({});

export const runPerfForkMigrations = Effect.fn("runPerfForkMigrations")(function* (
  throughId?: number,
) {
  const entries = chainEntries(throughId);
  if (entries.length === 0) return [];
  const { recorded: ledgerRows, unrecorded } = yield* unrecordedEntries(throughId);
  // Refuse before anything runs: the migrator would skip a gap for good, and a build must not run
  // on a fork migration it does not know (a newer build, or another branch under the same id).
  const problems = perfForkLedgerProblems(
    ledgerRows.map((row) => ({ id: row.migration_id, name: row.name })),
  );
  if (problems.length > 0) return yield* ledgerRefusal(problems);
  if (unrecorded.length > 0) {
    yield* Effect.log(DATABASE_MIGRATION_PROGRESS_MESSAGE).pipe(
      Effect.annotateLogs({
        migration: unrecorded.map(([id, name]) => perfForkMigrationLabel([id, name])).join(", "),
      }),
    );
  }
  // The migrator takes the write lock even when it has nothing to run.
  const executed =
    unrecorded.length === 0
      ? []
      : yield* run({
          loader: Migrator.fromRecord(
            Object.fromEntries(
              entries.map(([id, name, migration]) => [`${id}_${name}`, migration]),
            ),
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
    if (objects.length === 0) continue;
    if (!isRecorded(recorded, id, name)) continue;
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
          yield* Effect.log(DATABASE_MIGRATION_PROGRESS_MESSAGE).pipe(
            Effect.annotateLogs({ migration: label }),
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

  return executed;
});

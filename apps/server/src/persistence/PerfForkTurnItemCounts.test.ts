import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "./Migrations.ts";

const FORK_TABLE = "t3_perf_fork_migrations";
const FORK_CHAIN: Array<[number, string]> = [
  [1, "ProjectionTurnItemCounts"],
  [2, "TurnItemMessageOrdinalIndex"],
  [3, "ThreadItemTotals"],
  [4, "EventLogPruneState"],
];
const PERF5_RAN_AT = "2026-11-04 00:00:00";
const UPSTREAM_COUNTS_ID = 85;
const COUNTS = "orchestration_v2_projection_turn_item_counts";
const COUNT_TRIGGERS = [
  "orchestration_v2_turn_item_counts_delete",
  "orchestration_v2_turn_item_counts_insert",
  "orchestration_v2_turn_item_counts_update",
];
const ORDINAL_INDEX = "orchestration_v2_projection_turn_items_message_ordinal_idx";

// Release 0.1.6-perf.5's fork chain, verbatim: migration 1 (counts keyed by run_id) and 2.
const PERF5_FORK_SCHEMA = [
  `CREATE TABLE IF NOT EXISTS orchestration_v2_projection_turn_item_counts (
    thread_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    PRIMARY KEY (thread_id, run_id)
  ) WITHOUT ROWID`,
  `CREATE TRIGGER IF NOT EXISTS orchestration_v2_turn_item_counts_insert
  AFTER INSERT ON orchestration_v2_projection_turn_items
  BEGIN
    INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, run_id, item_count)
    VALUES (new.thread_id, COALESCE(new.run_id, ''), 1)
    ON CONFLICT(thread_id, run_id) DO UPDATE SET item_count = item_count + 1;
  END`,
  `CREATE TRIGGER IF NOT EXISTS orchestration_v2_turn_item_counts_update
  AFTER UPDATE OF thread_id, run_id ON orchestration_v2_projection_turn_items
  WHEN old.thread_id IS NOT new.thread_id OR old.run_id IS NOT new.run_id
  BEGIN
    UPDATE orchestration_v2_projection_turn_item_counts SET item_count = item_count - 1
    WHERE thread_id = old.thread_id AND run_id = COALESCE(old.run_id, '');
    DELETE FROM orchestration_v2_projection_turn_item_counts
    WHERE thread_id = old.thread_id AND run_id = COALESCE(old.run_id, '') AND item_count = 0;
    INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, run_id, item_count)
    VALUES (new.thread_id, COALESCE(new.run_id, ''), 1)
    ON CONFLICT(thread_id, run_id) DO UPDATE SET item_count = item_count + 1;
  END`,
  `CREATE TRIGGER IF NOT EXISTS orchestration_v2_turn_item_counts_delete
  AFTER DELETE ON orchestration_v2_projection_turn_items
  BEGIN
    UPDATE orchestration_v2_projection_turn_item_counts SET item_count = item_count - 1
    WHERE thread_id = old.thread_id AND run_id = COALESCE(old.run_id, '');
    DELETE FROM orchestration_v2_projection_turn_item_counts
    WHERE thread_id = old.thread_id AND run_id = COALESCE(old.run_id, '') AND item_count = 0;
  END`,
  `CREATE INDEX IF NOT EXISTS orchestration_v2_projection_turn_items_message_ordinal_idx
  ON orchestration_v2_projection_turn_items(
    thread_id, json_extract(payload_json, '$.messageId'), ordinal
  )
  WHERE type IN ('user_message', 'assistant_message') AND json_valid(payload_json)`,
];

const ledger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
  `;
  return rows.map((row) => [row.migration_id, row.name] as const);
});

const forkLedger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    readonly migration_id: number;
    readonly name: string;
    readonly created_at: string;
  }>`
    SELECT migration_id, name, created_at FROM ${sql(FORK_TABLE)} ORDER BY migration_id
  `;
});

const countColumns = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly name: string }>`SELECT name FROM pragma_table_info(${COUNTS})`;
  return rows.map((row) => row.name);
});

const counts = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`
    SELECT thread_id, run_key, item_count FROM orchestration_v2_projection_turn_item_counts
    ORDER BY thread_id, run_key
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

// Each count object and the message-ordinal index exactly once, on the table it belongs to.
const assertCountObjectsOnce = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly type: string;
    readonly name: string;
    readonly tbl_name: string;
  }>`
    SELECT type, name, tbl_name FROM sqlite_master
    WHERE name LIKE '%turn_item_counts%' OR name = ${ORDINAL_INDEX}
    ORDER BY type, name
  `;
  assert.deepStrictEqual(
    rows.map((row) => [row.type, row.name, row.tbl_name]),
    [
      ["index", ORDINAL_INDEX, "orchestration_v2_projection_turn_items"],
      ["table", COUNTS, COUNTS],
      ...COUNT_TRIGGERS.map((name) => ["trigger", name, "orchestration_v2_projection_turn_items"]),
    ],
  );
  assert.deepStrictEqual(yield* countColumns, ["thread_id", "run_key", "item_count"]);
});

const insertItems = (
  prefix: string,
  items: ReadonlyArray<readonly [string, string | null, string]>,
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const [index, [threadId, runId, type]] of items.entries()) {
      yield* sql`
        INSERT INTO orchestration_v2_projection_turn_items (
          turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
        ) VALUES (
          ${`${prefix}-${index}`}, ${threadId}, ${runId}, ${index + 1}, ${type}, 'completed',
          '2026-11-04T00:00:00.000Z', ${JSON.stringify({ messageId: `${prefix}-m${index % 3}` })}
        )
      `;
    }
  });

const PERF5_ITEMS = [
  ["thread-a", "run-1", "user_message"],
  ["thread-a", "run-1", "assistant_message"],
  ["thread-a", "run-1", "command_execution"],
  ["thread-a", "run-2", "assistant_message"],
  ["thread-a", null, "user_message"],
  ["thread-b", "run-3", "user_message"],
  ["thread-b", "run-3", "reasoning"],
] as const;

// Keeps the counts in step: new items, an item moved to another run and to no run, a delete.
const assertTriggersKeepCountsInStep = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* insertItems("after", [
    ["thread-a", "run-1", "assistant_message"],
    ["thread-c", "run-9", "user_message"],
    ["thread-c", null, "assistant_message"],
  ]);
  yield* sql`UPDATE orchestration_v2_projection_turn_items SET run_id = 'run-2' WHERE turn_item_id = 'perf5-0'`;
  yield* sql`UPDATE orchestration_v2_projection_turn_items SET run_id = NULL WHERE turn_item_id = 'after-0'`;
  yield* sql`DELETE FROM orchestration_v2_projection_turn_items WHERE turn_item_id = 'perf5-6'`;
  assert.deepStrictEqual(yield* counts, yield* countedFromItems);
});

// A database as release 0.1.6-perf.5 leaves it: upstream's chain through `upstreamHead` (78 by
// that build, 80 when a newer upstream build also ran), the fork chain recorded, counts filled.
const atPerf5 = (upstreamHead: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: upstreamHead });
    for (const statement of PERF5_FORK_SCHEMA) yield* sql.unsafe(statement);
    yield* sql.unsafe(`CREATE TABLE ${FORK_TABLE} (
  migration_id integer PRIMARY KEY NOT NULL,
  created_at datetime NOT NULL DEFAULT current_timestamp,
  name VARCHAR(255) NOT NULL
)`);
    for (const [id, name] of FORK_CHAIN) {
      yield* sql`
        INSERT INTO ${sql(FORK_TABLE)} (migration_id, name, created_at)
        VALUES (${id}, ${name}, ${PERF5_RAN_AT})
      `;
    }
    yield* insertItems("perf5", PERF5_ITEMS);
  });

const upstreamChain = migrationManifest.map(([id, name]) => [id, name] as const);
const memory = NodeSqliteClient.layer({ filename: ":memory:" });

describe("turn-item counts across the upgrade to upstream's migration 85", () => {
  it.effect.each([78, 80])(
    "a database at release 0.1.6-perf.5's state (upstream %i) upgrades: upstream's counts, once each, in step",
    (upstreamHead) =>
      Effect.gen(function* () {
        yield* atPerf5(upstreamHead);
        const ran = yield* runMigrations();
        assert.deepStrictEqual(
          ran.map(([id]) => id),
          upstreamChain.filter(([id]) => id > upstreamHead).map(([id]) => id),
        );
        assert.deepStrictEqual(yield* ledger, upstreamChain);
        assert.deepStrictEqual(
          (yield* forkLedger).map((row) => [row.migration_id, row.name, row.created_at]),
          FORK_CHAIN.map(([id, name]) => [id, name, PERF5_RAN_AT]),
        );
        yield* assertCountObjectsOnce;
        assert.deepStrictEqual(yield* counts, yield* countedFromItems);
        yield* assertTriggersKeepCountsInStep;
        assert.deepStrictEqual(yield* runMigrations(), []);
        yield* assertCountObjectsOnce;
      }).pipe(Effect.provide(memory)),
  );

  it.effect("a fresh database gets upstream's counts and message-ordinal index, once each", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      assert.deepStrictEqual(yield* ledger, upstreamChain);
      assert.deepStrictEqual(
        (yield* forkLedger).map((row) => [row.migration_id, row.name]),
        FORK_CHAIN,
      );
      yield* assertCountObjectsOnce;
      yield* insertItems("perf5", PERF5_ITEMS);
      yield* assertTriggersKeepCountsInStep;
    }).pipe(Effect.provide(memory)),
  );

  it.effect(
    "a database that already ran upstream's migration 85 keeps its counts as they are",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        // An upstream build's database: upstream's chain, no fork chain yet.
        yield* runMigrations({ toPerfForkMigrationInclusive: 0 });
        yield* insertItems("perf5", PERF5_ITEMS);
        // Neither dropped nor recounted: this row stays only if nothing touches the table.
        yield* sql`
        INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, run_key, item_count)
        VALUES ('thread-stray', '', 7)
      `;
        const before = yield* counts;

        assert.deepStrictEqual(yield* runMigrations(), []);
        assert.deepStrictEqual(
          (yield* forkLedger).map((row) => [row.migration_id, row.name]),
          FORK_CHAIN,
        );
        assert.deepStrictEqual(yield* counts, before);
        yield* assertCountObjectsOnce;
      }).pipe(Effect.provide(memory)),
  );

  it.effect("an upgrade that fails after migration 85 completes when started again", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* atPerf5(80);
      // A hand-applied index under migration 86's name stops that migration (no IF NOT EXISTS).
      yield* sql`
        CREATE INDEX orchestration_v2_projection_turn_items_queued_input_idx
        ON orchestration_v2_projection_turn_items(thread_id)
      `;
      const failed = yield* Effect.exit(runMigrations());
      assert.isTrue(Exit.isFailure(failed));
      assert.deepStrictEqual(
        (yield* ledger).at(-1),
        upstreamChain.find(([id]) => id === UPSTREAM_COUNTS_ID),
      );
      yield* assertCountObjectsOnce;
      assert.deepStrictEqual(yield* counts, yield* countedFromItems);

      yield* sql`DROP INDEX orchestration_v2_projection_turn_items_queued_input_idx`;
      yield* runMigrations();
      assert.deepStrictEqual(yield* ledger, upstreamChain);
      yield* assertCountObjectsOnce;
      yield* assertTriggersKeepCountsInStep;
    }).pipe(Effect.provide(memory)),
  );
});

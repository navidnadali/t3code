import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

/** The performance fork's per-thread item total, beside upstream's per-run counts (migration 85). */
export const THREAD_ITEM_TOTALS = "t3_perf_thread_item_totals";
export const THREAD_ITEM_TOTALS_TRIGGERS = [
  ["t3_perf_thread_item_totals_item_insert", "orchestration_v2_projection_turn_items"],
  ["t3_perf_thread_item_totals_item_update", "orchestration_v2_projection_turn_items"],
  ["t3_perf_thread_item_totals_item_delete", "orchestration_v2_projection_turn_items"],
  ["t3_perf_thread_item_totals_run_insert", "orchestration_v2_projection_runs"],
  ["t3_perf_thread_item_totals_run_delete", "orchestration_v2_projection_runs"],
  ["t3_perf_thread_item_totals_run_update", "orchestration_v2_projection_runs"],
] as const;

/**
 * Each thread's turn items that have no run or whose run exists: the items the shell's count
 * starts from before it leaves out rolled-back runs and held queued input. Upstream's count table
 * and triggers stay as they are, so a build without this table reads them unchanged; these
 * triggers keep the totals while any build writes. Idempotent: running it again rebuilds the totals.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS t3_perf_thread_item_totals (
      thread_id TEXT PRIMARY KEY NOT NULL,
      item_count INTEGER NOT NULL
    ) WITHOUT ROWID
  `;
  // An item counts while it has no run (run_key '' upstream) or its run exists, wherever that run's
  // row sits.
  const counted = (item: "new" | "old") =>
    sql.literal(
      `(COALESCE(${item}.run_id, '') = '' OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id = ${item}.run_id))`,
    );
  const add = (thread: string, delta: string) =>
    sql.literal(`INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
      VALUES (${thread}, ${delta})
      ON CONFLICT (thread_id) DO UPDATE SET item_count = item_count + excluded.item_count;`);
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS t3_perf_thread_item_totals_item_insert
    AFTER INSERT ON orchestration_v2_projection_turn_items
    WHEN ${counted("new")}
    BEGIN
      ${add("new.thread_id", "1")}
    END
  `;
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS t3_perf_thread_item_totals_item_delete
    AFTER DELETE ON orchestration_v2_projection_turn_items
    WHEN ${counted("old")}
    BEGIN
      ${add("old.thread_id", "-1")}
    END
  `;
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS t3_perf_thread_item_totals_item_update
    AFTER UPDATE OF thread_id, run_id ON orchestration_v2_projection_turn_items
    WHEN old.thread_id IS NOT new.thread_id OR old.run_id IS NOT new.run_id
    BEGIN
      INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
      SELECT old.thread_id, -1 WHERE ${counted("old")}
      ON CONFLICT (thread_id) DO UPDATE SET item_count = item_count + excluded.item_count;
      INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
      SELECT new.thread_id, 1 WHERE ${counted("new")}
      ON CONFLICT (thread_id) DO UPDATE SET item_count = item_count + excluded.item_count;
    END
  `;
  // A run's row arriving after its items, or leaving before them, moves them in or out.
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS t3_perf_thread_item_totals_run_insert
    AFTER INSERT ON orchestration_v2_projection_runs
    WHEN new.run_id <> ''
    BEGIN
      INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
      SELECT thread_id, COUNT(*) FROM orchestration_v2_projection_turn_items
      WHERE run_id = new.run_id GROUP BY thread_id
      ON CONFLICT (thread_id) DO UPDATE SET item_count = item_count + excluded.item_count;
    END
  `;
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS t3_perf_thread_item_totals_run_delete
    AFTER DELETE ON orchestration_v2_projection_runs
    WHEN old.run_id <> ''
    BEGIN
      INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
      SELECT thread_id, -COUNT(*) FROM orchestration_v2_projection_turn_items
      WHERE run_id = old.run_id GROUP BY thread_id
      ON CONFLICT (thread_id) DO UPDATE SET item_count = item_count + excluded.item_count;
    END
  `;
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS t3_perf_thread_item_totals_run_update
    AFTER UPDATE OF run_id ON orchestration_v2_projection_runs
    WHEN old.run_id IS NOT new.run_id
    BEGIN
      INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
      SELECT thread_id, -COUNT(*) FROM orchestration_v2_projection_turn_items
      WHERE old.run_id <> '' AND run_id = old.run_id GROUP BY thread_id
      ON CONFLICT (thread_id) DO UPDATE SET item_count = item_count + excluded.item_count;
      INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
      SELECT thread_id, COUNT(*) FROM orchestration_v2_projection_turn_items
      WHERE new.run_id <> '' AND run_id = new.run_id GROUP BY thread_id
      ON CONFLICT (thread_id) DO UPDATE SET item_count = item_count + excluded.item_count;
    END
  `;
  yield* rebuildThreadItemTotals;
});

/**
 * The totals recomputed: from upstream's per-run counts (migration 85) when the database has them,
 * else from the items themselves (a database the fork's chain reaches before 85 runs).
 */
export const rebuildThreadItemTotals = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DELETE FROM t3_perf_thread_item_totals`;
  const upstreamCounts = yield* sql<{ readonly name: string }>`
    SELECT name FROM pragma_table_info('orchestration_v2_projection_turn_item_counts')
    WHERE name = 'run_key'
  `;
  yield* upstreamCounts.length > 0
    ? sql`
        INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
        SELECT counts.thread_id, SUM(counts.item_count)
        FROM orchestration_v2_projection_turn_item_counts AS counts
        WHERE counts.run_key = ''
          OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id = counts.run_key)
        GROUP BY counts.thread_id
      `
    : sql`
        INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
        SELECT item.thread_id, COUNT(*)
        FROM orchestration_v2_projection_turn_items AS item
        WHERE COALESCE(item.run_id, '') = ''
          OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id = item.run_id)
        GROUP BY item.thread_id
      `;
});

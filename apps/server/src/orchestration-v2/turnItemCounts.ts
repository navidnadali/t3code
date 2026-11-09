import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";

/** Refills the maintained turn-item counts from the items: fork migration 1's backfill and doctor's repair. */
export const recountTurnItems = Effect.fn("TurnItemCounts.recount")(function* (
  sql: SqlClient.SqlClient,
) {
  // One transaction: an interrupt between clear and refill would leave every shell at zero items.
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`DELETE FROM orchestration_v2_projection_turn_item_counts`;
      // Group in the (thread_id, run_id) index's order, then fold NULL runs into ''.
      yield* sql`
        INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, run_id, item_count)
        SELECT thread_id, COALESCE(run_id, ''), SUM(item_count)
        FROM (
          SELECT thread_id, run_id, COUNT(*) AS item_count
          FROM orchestration_v2_projection_turn_items
          GROUP BY thread_id, run_id
        )
        GROUP BY thread_id, COALESCE(run_id, '')
      `;
    }),
  );
});

export const driftedTurnItemCountThreads = Effect.fn("TurnItemCounts.driftedThreads")(function* (
  sql: SqlClient.SqlClient,
) {
  const exists =
    yield* sql`SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'orchestration_v2_projection_turn_item_counts'`;
  if (exists.length === 0) return [];
  return yield* sql<{ readonly thread_id: string }>`
    WITH stored AS (SELECT thread_id, run_id, item_count FROM orchestration_v2_projection_turn_item_counts),
    counted AS (SELECT thread_id, COALESCE(run_id, '') AS run_id, SUM(item_count) AS item_count FROM (
      SELECT thread_id, run_id, COUNT(*) AS item_count FROM orchestration_v2_projection_turn_items GROUP BY thread_id, run_id
    ) GROUP BY thread_id, COALESCE(run_id, ''))
    SELECT DISTINCT thread_id FROM (
      SELECT * FROM (SELECT * FROM stored EXCEPT SELECT * FROM counted)
      UNION ALL SELECT * FROM (SELECT * FROM counted EXCEPT SELECT * FROM stored)
    ) ORDER BY thread_id
  `;
});

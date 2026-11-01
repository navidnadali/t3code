import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { recountTurnItems } from "../../orchestration-v2/turnItemCounts.ts";

/**
 * Turn items per thread and run, so a shell reads one row per run instead of counting every item.
 * Triggers keep it exact on every write; INSERT OR REPLACE would skip the delete trigger (recursive
 * triggers are off), so turn items take upserts. Run-less items count under '' (RunIds are
 * non-empty). Re-running recounts from the items.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // The triggers below write here: drop them before this table, or every turn-item insert fails.
  yield* sql`
    CREATE TABLE IF NOT EXISTS orchestration_v2_projection_turn_item_counts (
      thread_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      item_count INTEGER NOT NULL,
      PRIMARY KEY (thread_id, run_id)
    ) WITHOUT ROWID
  `;
  // A migration that rebuilds the turn-item table (create, copy, drop, rename) drops these
  // triggers with it; this migration must run after such a rebuild, or the rebuild recreate them.
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS orchestration_v2_turn_item_counts_insert
    AFTER INSERT ON orchestration_v2_projection_turn_items
    BEGIN
      INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, run_id, item_count)
      VALUES (new.thread_id, COALESCE(new.run_id, ''), 1)
      ON CONFLICT(thread_id, run_id) DO UPDATE SET item_count = item_count + 1;
    END
  `;
  // Every projection upsert sets thread_id and run_id; only a real move changes the counts.
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS orchestration_v2_turn_item_counts_update
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
    END
  `;
  yield* sql`
    CREATE TRIGGER IF NOT EXISTS orchestration_v2_turn_item_counts_delete
    AFTER DELETE ON orchestration_v2_projection_turn_items
    BEGIN
      UPDATE orchestration_v2_projection_turn_item_counts SET item_count = item_count - 1
      WHERE thread_id = old.thread_id AND run_id = COALESCE(old.run_id, '');
      DELETE FROM orchestration_v2_projection_turn_item_counts
      WHERE thread_id = old.thread_id AND run_id = COALESCE(old.run_id, '') AND item_count = 0;
    END
  `;

  yield* recountTurnItems(sql);
});

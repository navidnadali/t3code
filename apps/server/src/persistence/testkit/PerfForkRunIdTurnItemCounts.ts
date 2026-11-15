/**
 * Fork migration 1's turn-item counts as every fork build before upstream's migration 85 left them,
 * verbatim: keyed by run_id, under the names 85 uses.
 */
export const PERF_FORK_RUN_ID_TURN_ITEM_COUNTS = [
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
];

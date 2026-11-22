-- Rebuilds upstream's per-thread item totals (migration 102, ThreadItemCounts) from its per-run
-- counts, as 102's backfill does, when thread-item-counts-check.sql reports drift. It holds the
-- database's write lock while it rebuilds; a running server's writes wait for it.
-- Replaces perf-fork-003-repair.sql since fork migration 5 retired fork migration 3's own totals.
BEGIN IMMEDIATE;
DELETE FROM orchestration_v2_projection_thread_item_counts;
INSERT INTO orchestration_v2_projection_thread_item_counts (thread_id, item_count)
SELECT counts.thread_id, SUM(counts.item_count)
FROM orchestration_v2_projection_turn_item_counts AS counts
WHERE counts.run_key = ''
  OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id = counts.run_key)
GROUP BY counts.thread_id;
COMMIT;

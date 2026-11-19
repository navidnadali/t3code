-- Rebuilds fork migration 3's per-thread item totals (ThreadItemTotals) from upstream's per-run
-- counts, as the migration's backfill does, when perf-fork-003-check.sql reports drift. It holds
-- the database's write lock while it rebuilds; a running server's writes wait for it.
BEGIN IMMEDIATE;
DELETE FROM t3_perf_thread_item_totals;
INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
SELECT counts.thread_id, SUM(counts.item_count)
FROM orchestration_v2_projection_turn_item_counts AS counts
WHERE counts.run_key = ''
  OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id = counts.run_key)
GROUP BY counts.thread_id;
COMMIT;

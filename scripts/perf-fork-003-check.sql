-- Checks fork migration 3's per-thread item totals (ThreadItemTotals) against upstream's per-run
-- counts, reading only, and prints counts alone: no thread ids. drifted_threads above 0: run
-- perf-fork-003-repair.sql. cross_thread_items, for information: items whose run belongs to another
-- thread, which the shell counts as their own thread's timeline shows them (perf.12 did not).
WITH expected AS (
  SELECT counts.thread_id, SUM(counts.item_count) AS item_count
  FROM orchestration_v2_projection_turn_item_counts AS counts
  WHERE counts.run_key = ''
    OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id = counts.run_key)
  GROUP BY counts.thread_id
),
drift AS (
  SELECT totals.item_count - COALESCE(expected.item_count, 0) AS delta
  FROM t3_perf_thread_item_totals AS totals
  LEFT JOIN expected ON expected.thread_id = totals.thread_id
  UNION ALL
  SELECT expected.item_count
  FROM expected
  WHERE NOT EXISTS (
    SELECT 1 FROM t3_perf_thread_item_totals AS totals WHERE totals.thread_id = expected.thread_id
  )
)
SELECT
  (SELECT COUNT(*) FROM t3_perf_thread_item_totals) AS threads,
  (SELECT COUNT(*) FROM drift WHERE delta <> 0) AS drifted_threads,
  (SELECT COALESCE(SUM(ABS(delta)), 0) FROM drift) AS drifted_items,
  (
    SELECT COALESCE(SUM(counts.item_count), 0)
    FROM orchestration_v2_projection_turn_item_counts AS counts
    JOIN orchestration_v2_projection_runs AS r ON r.run_id = counts.run_key
    WHERE counts.run_key <> '' AND r.thread_id <> counts.thread_id
  ) AS cross_thread_items;

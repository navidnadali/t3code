-- Checks upstream's per-thread item totals (migration 102, ThreadItemCounts) against its per-run
-- counts (migration 85), reading only, and prints counts alone: no thread ids. drifted_threads above
-- 0: run thread-item-counts-repair.sql. cross_thread_items, for information: items whose run belongs
-- to another thread, which the shell counts as their own thread's timeline shows them.
-- Replaces perf-fork-003-check.sql since fork migration 5 retired fork migration 3's own totals.
WITH expected AS (
  SELECT counts.thread_id, SUM(counts.item_count) AS item_count
  FROM orchestration_v2_projection_turn_item_counts AS counts
  WHERE counts.run_key = ''
    OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id = counts.run_key)
  GROUP BY counts.thread_id
),
drift AS (
  SELECT totals.item_count - COALESCE(expected.item_count, 0) AS delta
  FROM orchestration_v2_projection_thread_item_counts AS totals
  LEFT JOIN expected ON expected.thread_id = totals.thread_id
  UNION ALL
  SELECT expected.item_count
  FROM expected
  WHERE NOT EXISTS (
    SELECT 1 FROM orchestration_v2_projection_thread_item_counts AS totals
    WHERE totals.thread_id = expected.thread_id
  )
)
SELECT
  (SELECT COUNT(*) FROM orchestration_v2_projection_thread_item_counts) AS threads,
  (SELECT COUNT(*) FROM drift WHERE delta <> 0) AS drifted_threads,
  (SELECT COALESCE(SUM(ABS(delta)), 0) FROM drift) AS drifted_items,
  (
    SELECT COALESCE(SUM(counts.item_count), 0)
    FROM orchestration_v2_projection_turn_item_counts AS counts
    JOIN orchestration_v2_projection_runs AS r ON r.run_id = counts.run_key
    WHERE counts.run_key <> '' AND r.thread_id <> counts.thread_id
  ) AS cross_thread_items;

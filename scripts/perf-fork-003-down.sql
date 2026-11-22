-- Takes fork migration 3 (ThreadItemTotals) back out, so a build whose fork chain ends at 2
-- (0.1.9-perf.12 and earlier) passes its service preflight on this database: its per-thread item
-- totals table and triggers, and its row in the fork's migrations table. Upstream's counts stay.
-- Run perf-fork-005-down.sql and perf-fork-004-down.sql first. Stop the server first.
BEGIN IMMEDIATE;
DROP TRIGGER IF EXISTS t3_perf_thread_item_totals_item_insert;
DROP TRIGGER IF EXISTS t3_perf_thread_item_totals_item_update;
DROP TRIGGER IF EXISTS t3_perf_thread_item_totals_item_delete;
DROP TRIGGER IF EXISTS t3_perf_thread_item_totals_run_insert;
DROP TRIGGER IF EXISTS t3_perf_thread_item_totals_run_delete;
DROP TRIGGER IF EXISTS t3_perf_thread_item_totals_run_update;
DROP TABLE IF EXISTS t3_perf_thread_item_totals;
DELETE FROM t3_perf_fork_migrations WHERE migration_id = 3 AND name = 'ThreadItemTotals';
COMMIT;

-- Leaves the performance fork's schema for an upstream build, on a database that has not run
-- upstream's migration 85, keeping every other row: the fork's turn-item counts (keyed by run_id,
-- under the names 85 creates) and their triggers, its migrations table, and release candidate 1's
-- records of its migrations in upstream's table (77 and 78). The message-ordinal index stays:
-- upstream's migration 94 creates the same index and takes this one as it is. Stop the server
-- first. See docs/operations/perf-fork-schema-exit.md.
-- Order matters: the triggers write the counts table, so a turn-item write fails once the table
-- is gone and they are not.
BEGIN IMMEDIATE;
CREATE TEMP TABLE perf_fork_schema_exit_check (
  ok INTEGER CONSTRAINT "migration 85 ran: use perf-fork-schema-exit.sql" CHECK (ok)
);
INSERT INTO perf_fork_schema_exit_check
SELECT NOT EXISTS (
  SELECT 1 FROM pragma_table_info('orchestration_v2_projection_turn_item_counts')
  WHERE name = 'run_key'
);
DROP TABLE temp.perf_fork_schema_exit_check;
DROP TRIGGER IF EXISTS orchestration_v2_turn_item_counts_insert;
DROP TRIGGER IF EXISTS orchestration_v2_turn_item_counts_update;
DROP TRIGGER IF EXISTS orchestration_v2_turn_item_counts_delete;
DROP TABLE IF EXISTS orchestration_v2_projection_turn_item_counts;
DROP TABLE IF EXISTS t3_perf_fork_migrations;
DELETE FROM effect_sql_migrations
WHERE (migration_id = 77 AND name = 'ProjectionTurnItemCounts')
   OR (migration_id = 78 AND name = 'TurnItemMessageOrdinalIndex');
COMMIT;

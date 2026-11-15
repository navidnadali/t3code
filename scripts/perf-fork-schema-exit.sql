-- Leaves the performance fork's schema for an upstream build, keeping every other row: its index,
-- its migrations table, and release candidate 1's records of its migrations in upstream's table
-- (77 and 78). The turn-item counts and their triggers are upstream's (migration 85) and stay; a
-- database that has not run 85 needs perf-fork-schema-exit-before-085.sql instead. Stop the server
-- first. See docs/operations/perf-fork-schema-exit.md.
BEGIN IMMEDIATE;
CREATE TEMP TABLE perf_fork_schema_exit_check (
  ok INTEGER CONSTRAINT "migration 85 has not run: use perf-fork-schema-exit-before-085.sql" CHECK (ok)
);
INSERT INTO perf_fork_schema_exit_check
SELECT NOT EXISTS (
  SELECT 1 FROM pragma_table_info('orchestration_v2_projection_turn_item_counts')
  WHERE name = 'run_id'
);
DROP TABLE temp.perf_fork_schema_exit_check;
DROP INDEX IF EXISTS orchestration_v2_projection_turn_items_message_ordinal_idx;
DROP TABLE IF EXISTS t3_perf_fork_migrations;
DELETE FROM effect_sql_migrations
WHERE (migration_id = 77 AND name = 'ProjectionTurnItemCounts')
   OR (migration_id = 78 AND name = 'TurnItemMessageOrdinalIndex');
COMMIT;

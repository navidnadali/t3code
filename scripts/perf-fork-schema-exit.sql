-- Leaves the performance fork's schema for an upstream build, keeping every other row: its objects,
-- its migrations table, and release candidate 1's records of its migrations in upstream's table
-- (77 and 78). Stop the server first. See docs/operations/perf-fork-schema-exit.md.
-- Order matters: the triggers write the counts table, so a turn-item write fails once the table
-- is gone and they are not.
BEGIN IMMEDIATE;
DROP TRIGGER IF EXISTS orchestration_v2_turn_item_counts_insert;
DROP TRIGGER IF EXISTS orchestration_v2_turn_item_counts_update;
DROP TRIGGER IF EXISTS orchestration_v2_turn_item_counts_delete;
DROP TABLE IF EXISTS orchestration_v2_projection_turn_item_counts;
DROP INDEX IF EXISTS orchestration_v2_projection_turn_items_message_ordinal_idx;
DROP TABLE IF EXISTS t3_perf_fork_migrations;
DELETE FROM effect_sql_migrations
WHERE (migration_id = 77 AND name = 'ProjectionTurnItemCounts')
   OR (migration_id = 78 AND name = 'TurnItemMessageOrdinalIndex');
COMMIT;

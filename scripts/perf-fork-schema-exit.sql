-- Leaves the performance fork's schema (migrations 77 and 78) for an upstream build, keeping every
-- other row. Stop the server first. See docs/operations/perf-fork-schema-exit.md.
-- Order matters: the triggers write the counts table, so a turn-item write fails once the table
-- is gone and they are not.
BEGIN IMMEDIATE;
DROP TRIGGER IF EXISTS orchestration_v2_turn_item_counts_insert;
DROP TRIGGER IF EXISTS orchestration_v2_turn_item_counts_update;
DROP TRIGGER IF EXISTS orchestration_v2_turn_item_counts_delete;
DROP TABLE IF EXISTS orchestration_v2_projection_turn_item_counts;
DROP INDEX IF EXISTS orchestration_v2_projection_turn_items_message_ordinal_idx;
DELETE FROM effect_sql_migrations
WHERE (migration_id = 77 AND name = 'ProjectionTurnItemCounts')
   OR (migration_id = 78 AND name = 'TurnItemMessageOrdinalIndex');
COMMIT;

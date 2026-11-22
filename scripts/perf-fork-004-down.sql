-- Takes fork migration 4 (EventLogPruneState) back out, as
-- apps/server/src/persistence/PerfForkMigrations/EventLogPruneState.ts `down` does: its held-rows
-- table, its row in the fork's migrations table, and its progress metadata (cursor and reference
-- scan). The prune boundary (mark, pruned-through) stays: older builds read it with the same
-- meaning. A rollback to 0.1.9-perf.12 runs perf-fork-005-down.sql first (a database a build with
-- fork migration 5 started on), then this, then perf-fork-003-down.sql, so the fork ledger is 1..2
-- again. Stop the server first.
BEGIN IMMEDIATE;
DROP TABLE IF EXISTS orchestration_events_prune_state;
DELETE FROM t3_perf_fork_migrations WHERE migration_id = 4 AND name = 'EventLogPruneState';
DELETE FROM orchestration_v2_projection_metadata
WHERE projection_name IN (
  'event-log-prune-cursor:v1',
  'event-log-reference-scan:v1',
  'event-log-reference-scan-from:v1'
);
COMMIT;

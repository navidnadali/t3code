-- Takes upstream's migrations 106 to 110 back out, so a build whose upstream chain ends at 105
-- (0.1.11-perf.16 and its fleet builds) passes its service preflight on this database: that
-- preflight refuses any recorded migration above its own head. For a build at 96, run
-- upstream-097-105-down.sql and perf-fork-005-down.sql after it. Stop the server first.
--
-- Primary rollback is restoring the snapshot taken before the upgrade. This is the path for when a
-- restore would lose more: it deletes what 106 to 110 hold, agent feedback (108), issue-tracker
-- accounts beyond each tracker's first and the bindings to them (109), and workflow runs (110),
-- which builds at 105 have no use for, and leaves every other row as it is. The tables a workflow
-- engine journals runs in, which it creates itself, stay; no older build reads them.
BEGIN IMMEDIATE;
-- 110 Workflows
DROP TABLE IF EXISTS workflow_run_references;
DROP TABLE IF EXISTS workflow_store;
DROP TABLE IF EXISTS workflow_hooks;
DROP TABLE IF EXISTS workflow_triggers;
DROP TABLE IF EXISTS workflow_signals;
DROP TABLE IF EXISTS workflow_marks;
DROP TABLE IF EXISTS workflow_mail;
DROP TABLE IF EXISTS workflow_waits;
DROP TABLE IF EXISTS workflow_steps;
DROP TABLE IF EXISTS workflow_runs;
-- 109 IssueTrackerAccounts: a binding to an added account would read as the first account's.
-- A second run stops at the column drop, rolled back.
DELETE FROM issue_tracker_bindings WHERE account_id IS NOT NULL;
ALTER TABLE issue_tracker_bindings DROP COLUMN account_id;
DROP TABLE IF EXISTS issue_tracker_accounts;
-- 108 AgentFeedback
DROP TABLE IF EXISTS agent_feedback;
-- 107 DeferredSlashCommandIndex: migration 100's index again, on 0.1.11-perf.16's condition, which
-- its turn-start read spells the same (the trim set is JavaScript's whitespace, written out).
DROP INDEX IF EXISTS orchestration_v2_projection_messages_deferred_command_idx;
CREATE INDEX orchestration_v2_projection_messages_deferred_command_idx
ON orchestration_v2_projection_messages(thread_id, created_at, message_id)
WHERE role = 'user' AND (
  lower(trim(json_extract(payload_json, '$.text'), ' 	
                 　﻿')) = '/compact'
  OR trim(json_extract(payload_json, '$.text'), ' 	
                 　﻿') GLOB '/goal*'
);
-- 106 WorktreePathKeyIndex
DROP INDEX IF EXISTS orchestration_v2_projection_threads_worktree_path_key_idx;
DELETE FROM effect_sql_migrations
WHERE (migration_id = 106 AND name = 'WorktreePathKeyIndex')
   OR (migration_id = 107 AND name = 'DeferredSlashCommandIndex')
   OR (migration_id = 108 AND name = 'AgentFeedback')
   OR (migration_id = 109 AND name = 'IssueTrackerAccounts')
   OR (migration_id = 110 AND name = 'Workflows');
COMMIT;

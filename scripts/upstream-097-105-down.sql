-- Takes upstream's migrations 97 to 105 back out, so a build whose upstream chain ends at 96
-- (0.1.9-perf.13 to 0.1.9-perf.15 and their fleet builds) passes its service preflight on this
-- database: that preflight refuses any recorded migration above its own head. Run it with
-- perf-fork-005-down.sql, which gives back fork migration 3's totals. Stop the server first.
--
-- Primary rollback is restoring the snapshot taken before the upgrade. This is the path for when a
-- restore would lose more: it deletes what 97 to 105 hold, issue-tracker bindings (97, 103) and
-- task folders (105), which builds at 96 have no use for, and leaves every other row as it is.
BEGIN IMMEDIATE;
-- 105 TaskFolders
DROP INDEX IF EXISTS idx_task_folders_real_root;
DROP TABLE IF EXISTS task_folders;
-- 104 ProjectionProjectsTrackedFolders: the column it added. A second run stops here, rolled back.
ALTER TABLE projection_projects DROP COLUMN tracked_folders_json;
-- 102 ThreadItemCounts: the triggers first, as they write the table.
DROP TRIGGER IF EXISTS orchestration_v2_thread_item_counts_count_insert;
DROP TRIGGER IF EXISTS orchestration_v2_thread_item_counts_count_update;
DROP TRIGGER IF EXISTS orchestration_v2_thread_item_counts_count_delete;
DROP TRIGGER IF EXISTS orchestration_v2_thread_item_counts_run_insert;
DROP TRIGGER IF EXISTS orchestration_v2_thread_item_counts_run_delete;
DROP TABLE IF EXISTS orchestration_v2_projection_thread_item_counts;
DROP INDEX IF EXISTS orchestration_v2_projection_runs_hidden_items_idx;
-- 101 DelegatedTaskReadIndexes
DROP INDEX IF EXISTS orchestration_v2_projection_runs_delegated_task_idx;
DROP INDEX IF EXISTS orchestration_v2_projection_messages_delegated_task_idx;
-- 100 TurnStartReadIndexes
DROP INDEX IF EXISTS orchestration_v2_projection_messages_deferred_command_idx;
DROP INDEX IF EXISTS orchestration_v2_projection_subagents_routable_idx;
DROP INDEX IF EXISTS orchestration_v2_projection_context_transfers_fork_idx;
-- 099 BackgroundRecoveryIndexes
DROP INDEX IF EXISTS orchestration_v2_projection_turn_items_usage_limit_idx;
-- 098 ActiveShellIndexIndexes
DROP INDEX IF EXISTS orchestration_v2_projection_threads_active_shell_idx;
DROP INDEX IF EXISTS orchestration_v2_projection_provider_threads_pending_tasks_idx;
-- 097 IssueTrackerBindings (and 103's column on it)
DROP TABLE IF EXISTS issue_tracker_bindings;
DELETE FROM effect_sql_migrations
WHERE (migration_id = 97 AND name = 'IssueTrackerBindings')
   OR (migration_id = 98 AND name = 'ActiveShellIndexIndexes')
   OR (migration_id = 99 AND name = 'BackgroundRecoveryIndexes')
   OR (migration_id = 100 AND name = 'TurnStartReadIndexes')
   OR (migration_id = 101 AND name = 'DelegatedTaskReadIndexes')
   OR (migration_id = 102 AND name = 'ThreadItemCounts')
   OR (migration_id = 103 AND name = 'IssueTrackerAdditionalScopes')
   OR (migration_id = 104 AND name = 'ProjectionProjectsTrackedFolders')
   OR (migration_id = 105 AND name = 'TaskFolders');
COMMIT;

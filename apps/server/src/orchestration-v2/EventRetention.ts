/**
 * The event-log compaction's retention rule as SQL over a row aliased `event` of `table` (the
 * online pass reads `orchestration_events`, the offline copy `src.orchestration_events`). One rule
 * for both, so the copy keeps exactly what the online pass would.
 */
export const eventRetentionSql = (table: string) => ({
  // Rows kept whatever their age. Their payload never changes, so once classified they are never read again.
  keptForGood: `CASE
    WHEN event.event_type = 'checkpoint-scope.created' THEN 1
    -- Accepted delegation retries recover their task and child run from these events.
    WHEN event.event_type IN ('subagent.updated', 'run.created') THEN 1
    -- Stop/interrupt retries recover the interrupted run from either result form.
    WHEN event.event_type = 'run.updated'
      AND json_extract(event.payload_json, '$.status') = 'interrupted' THEN 1
    WHEN event.event_type = 'turn-item.updated'
      AND json_extract(event.payload_json, '$.type') = 'run_interrupt_request'
      AND json_extract(event.payload_json, '$.providerTurnId') IS NOT NULL THEN 1
    WHEN event.application_event_version = 1 THEN 1
    -- The legacy importer treats a thread without thread.created as not yet imported.
    WHEN event.event_type = 'thread.created' THEN 1
    -- Codex usage history reads the selection a run attempt started with.
    WHEN event.event_type = 'run.updated'
      AND json_extract(event.payload_json, '$.status') = 'running' THEN 1
    ELSE 0
  END`,
  // Kept while it lasts: the newest row keeps its stream's high water, stream_version and snapshot.
  isStreamHead: `event.sequence = (
    SELECT MAX(newer.sequence) FROM ${table} AS newer
    WHERE newer.aggregate_kind = event.aggregate_kind AND newer.stream_id = event.stream_id
  )`,
  // Kept while borrowed: a row storing a subagent prompt later rows may borrow through promptSequence.
  isPromptAnchor: `(
    event.event_type IN ('subagent.updated', 'turn-item.updated')
    AND json_type(event.payload_json, '$.promptSequence') IS NULL
    AND json_type(event.payload_json, '$.prompt') = 'text'
  )`,
  borrowedPrompt: `CASE
    WHEN event.event_type IN ('subagent.updated', 'turn-item.updated')
      AND json_type(event.payload_json, '$.promptSequence') = 'integer'
    THEN json_extract(event.payload_json, '$.promptSequence')
  END`,
});

export const ORDINAL_INDEX = "orchestration_v2_projection_turn_items_message_ordinal_idx";

/**
 * Fork migration 2's message-ordinal index as every fork build up to 0.1.9-perf.7 created it,
 * verbatim. Upstream's migration 94 creates the same index under the same name.
 */
export const PERF_FORK_ORDINAL_INDEX = `
    CREATE INDEX IF NOT EXISTS orchestration_v2_projection_turn_items_message_ordinal_idx
    ON orchestration_v2_projection_turn_items(
      thread_id, json_extract(payload_json, '$.messageId'), ordinal
    )
    WHERE type IN ('user_message', 'assistant_message') AND json_valid(payload_json)
  `;

/** A schema statement with its whitespace collapsed, so two databases' copies compare. */
export const normalizeSql = (sql: string) => sql.replace(/\s+/g, " ").trim();

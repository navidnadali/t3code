// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - streams rows through node:sqlite.
import * as NodeSqlite from "node:sqlite";

import {
  OrchestrationV2AppThreadJson,
  OrchestrationV2CheckpointJson,
  OrchestrationV2CheckpointScopeJson,
  OrchestrationV2ContextHandoffJson,
  OrchestrationV2ContextTransferJson,
  OrchestrationV2ConversationMessageJson,
  OrchestrationV2DomainEventJson,
  OrchestrationV2ExecutionNodeJson,
  OrchestrationV2PlanArtifact,
  OrchestrationV2ProviderSessionJson,
  OrchestrationV2ProviderThreadJson,
  OrchestrationV2ProviderTurnJson,
  OrchestrationV2RunAttemptJson,
  OrchestrationV2RunJson,
  OrchestrationV2RuntimeRequestJson,
  OrchestrationV2SubagentJson,
  OrchestrationV2TurnItemJson,
} from "@t3tools/contracts";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

const fromJson = <S extends Schema.Top & { readonly DecodingServices: never }>(schema: S) =>
  Schema.decodeUnknownExit(Schema.fromJsonString(schema));

/** Each v2 projection table and the schema the projection store decodes its payload with. */
const PROJECTIONS: ReadonlyArray<
  readonly [string, (json: unknown) => Exit.Exit<unknown, unknown>]
> = [
  ["orchestration_v2_projection_threads", fromJson(OrchestrationV2AppThreadJson)],
  ["orchestration_v2_projection_runs", fromJson(OrchestrationV2RunJson)],
  ["orchestration_v2_projection_run_attempts", fromJson(OrchestrationV2RunAttemptJson)],
  ["orchestration_v2_projection_nodes", fromJson(OrchestrationV2ExecutionNodeJson)],
  ["orchestration_v2_projection_subagents", fromJson(OrchestrationV2SubagentJson)],
  ["orchestration_v2_projection_provider_sessions", fromJson(OrchestrationV2ProviderSessionJson)],
  ["orchestration_v2_projection_provider_threads", fromJson(OrchestrationV2ProviderThreadJson)],
  ["orchestration_v2_projection_provider_turns", fromJson(OrchestrationV2ProviderTurnJson)],
  ["orchestration_v2_projection_runtime_requests", fromJson(OrchestrationV2RuntimeRequestJson)],
  ["orchestration_v2_projection_messages", fromJson(OrchestrationV2ConversationMessageJson)],
  ["orchestration_v2_projection_plans", fromJson(OrchestrationV2PlanArtifact)],
  ["orchestration_v2_projection_turn_items", fromJson(OrchestrationV2TurnItemJson)],
  ["orchestration_v2_projection_checkpoint_scopes", fromJson(OrchestrationV2CheckpointScopeJson)],
  ["orchestration_v2_projection_checkpoints", fromJson(OrchestrationV2CheckpointJson)],
  ["orchestration_v2_projection_context_handoffs", fromJson(OrchestrationV2ContextHandoffJson)],
  ["orchestration_v2_projection_context_transfers", fromJson(OrchestrationV2ContextTransferJson)],
];

const decodeEvent = Schema.decodeUnknownExit(OrchestrationV2DomainEventJson);

// The event store's own read: a compacted prompt is restored from the event it points at.
const EVENTS = `
  SELECT event.sequence, event.event_id, event.stream_id, event.event_type, event.occurred_at,
    event.metadata_json,
    CASE WHEN event.event_type IN ('subagent.updated', 'turn-item.updated')
        AND json_type(event.payload_json, '$.promptSequence') = 'integer'
      THEN json_set(json_remove(event.payload_json, '$.promptSequence'), '$.prompt',
        (SELECT json_extract(anchor.payload_json, '$.prompt') FROM orchestration_events AS anchor
         WHERE anchor.sequence = json_extract(event.payload_json, '$.promptSequence')))
      ELSE event.payload_json END AS payload_json
  FROM orchestration_events AS event
  WHERE event.application_event_version = 2 AND event.aggregate_kind = 'thread'`;

const parse = (text: unknown): unknown => {
  try {
    return typeof text === "string" ? (JSON.parse(text) as unknown) : null;
  } catch {
    return Symbol.for("unparseable");
  }
};

interface EventRow {
  readonly sequence: number;
  readonly event_id: string;
  readonly stream_id: string;
  readonly event_type: string;
  readonly occurred_at: string;
  readonly metadata_json: string;
  readonly payload_json: string;
}

/**
 * Rows the server cannot decode, as `table:rowid` (events as `orchestration_events:sequence`):
 * every v2 projection payload and v2 thread event, one row at a time so memory stays flat on a
 * thread with millions of items. Compared before and after the content pass.
 */
export const undecodableRows = (path: string): Set<string> => {
  const failed = new Set<string>();
  const db = new NodeSqlite.DatabaseSync(path, { readOnly: true });
  try {
    const tables = new Set(
      (
        db.prepare(`SELECT name FROM sqlite_schema WHERE type = 'table'`).all() as Array<{
          name: string;
        }>
      ).map((row) => row.name),
    );
    for (const [table, decode] of PROJECTIONS) {
      if (!tables.has(table)) continue;
      const rows = db.prepare(`SELECT rowid AS id, payload_json FROM "${table}"`);
      rows.setReadBigInts(true);
      for (const row of rows.iterate() as Iterable<{ id: bigint; payload_json: unknown }>) {
        if (Exit.isFailure(decode(row.payload_json))) failed.add(`${table}:${row.id}`);
      }
    }
    if (tables.has("orchestration_events")) {
      for (const row of db.prepare(EVENTS).iterate() as Iterable<EventRow>) {
        const metadata = parse(row.metadata_json);
        const values =
          typeof metadata === "object" && metadata !== null
            ? (metadata as Record<string, unknown>)
            : {};
        const pick = (key: string) => (values[key] === undefined ? {} : { [key]: values[key] });
        const decoded = decodeEvent({
          id: row.event_id,
          threadId: row.stream_id,
          type: row.event_type,
          occurredAt: row.occurred_at,
          payload: parse(row.payload_json),
          ...pick("runId"),
          ...pick("nodeId"),
          ...pick("driver"),
          ...pick("providerInstanceId"),
          ...pick("rawEventId"),
        });
        if (Exit.isFailure(decoded)) failed.add(`orchestration_events:${row.sequence}`);
      }
    }
  } finally {
    db.close();
  }
  return failed;
};

// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - copies, edits and scans SQLite files through node:sqlite.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import { type ContentMode, contentFindings, emptyCounts, rewriteContent } from "./content.ts";
import { undecodableRows } from "./decode.ts";
import { extendedFindings } from "./extended.ts";
import { createExclusive, fillExclusive, privateScratchDirectory, withSidecars } from "./guards.ts";
import { CREDENTIAL_COLUMN, scanDecoded, scanFileBytes } from "./secrets.ts";
import {
  compactEvents,
  database,
  failed,
  isRunnable,
  legacyImport,
  migrate,
  neutraliseThreads,
  rebuildSearch,
  recoveryThreadIds,
  restartRecovery,
  runnableWork,
  safeDestination,
  withEventSink,
  withLegacyImporter,
  withMaintenance,
  withProjections,
  withRecovery,
} from "./server.ts";

/** Deleted outright: credentials, sessions, pairing, links to other machines, and queued work. */
export const CLEARED_TABLES: ReadonlyArray<string> = [
  // The `migrate-dev-db` list (apps/server/scripts/migrate-dev-db.ts).
  "scheduled_tasks",
  "orchestration_v2_effect_outbox",
  "orchestration_v2_thread_launch_workflows",
  "orchestration_command_receipts",
  "provider_session_runtime",
  "auth_sessions",
  "auth_pairing_links",
  "peer_environment_links",
  // Webhook requests keep their headers and bodies; they have no foreign key to their task.
  "scheduled_task_webhook_deliveries",
  "scheduled_task_webhook_relay_deliveries",
  // Pull request state read from the host, keyed by host and repository (v1 tables).
  "projection_thread_pull_requests",
  "pull_request_files_viewed",
  "pull_request_review_guides",
];

/** The server's own bookkeeping, kept whole: migration names and projection positions. */
const STRUCTURAL_TABLES: ReadonlySet<string> = new Set([
  "effect_sql_migrations",
  "orchestration_v2_projection_metadata",
]);

const FTS_SHADOW = /^orchestration_v2_message_search_(?:data|idx|content|docsize|config)$/;

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const literal = (text: string) => `'${text.replaceAll("'", "''")}'`;

/** Every table that holds rows of its own: no SQLite internals, no search index (rebuilt). */
const tableNames = (db: NodeSqlite.DatabaseSync) =>
  (
    db
      .prepare(
        `SELECT name, sql FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'`,
      )
      .all() as Array<{ name: string; sql: string }>
  ).filter((row) => !/^CREATE VIRTUAL TABLE/i.test(row.sql) && !FTS_SHADOW.test(row.name));

const columnsOf = (db: NodeSqlite.DatabaseSync, table: string) =>
  db.prepare(`SELECT name, type FROM pragma_table_info(?)`).all(table) as Array<{
    name: string;
    type: string;
  }>;

const count = (db: NodeSqlite.DatabaseSync, sql: string, ...params: Array<string>) =>
  Number((db.prepare(sql).get(...params) as { n: number }).n);

const bump = (counts: Record<string, number>, key: string) => {
  counts[key] = (counts[key] ?? 0) + 1;
};

/**
 * P1-002's extended check (`fixture/extended.ts`) on every value of every table, the search
 * index's shadow tables included: `table:scope|kind` counts. A gate beside the self-check.
 */
export const extendedTables = (db: NodeSqlite.DatabaseSync): Record<string, number> => {
  const rows: Record<string, number> = {};
  const tables = db
    .prepare(
      `SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
         AND sql NOT LIKE 'CREATE VIRTUAL TABLE%'`,
    )
    .all() as Array<{ name: string }>;
  for (const { name } of tables) {
    const columns = (
      db.prepare(`SELECT name FROM pragma_table_xinfo(?) WHERE hidden = 0`).all(name) as Array<{
        name: string;
      }>
    ).map((column) => column.name);
    if (columns.length === 0) continue;
    const statement = db.prepare(`SELECT ${columns.map(quote).join(", ")} FROM ${quote(name)}`);
    statement.setReturnArrays(true);
    // FTS shadow tables hold integers past 2^53; only text and BLOBs are read.
    statement.setReadBigInts(true);
    const report = (row: string) => bump(rows, `${name}:${row}`);
    for (const row of statement.iterate() as Iterable<Array<unknown>>) {
      for (const value of row) {
        if (typeof value === "string") extendedFindings(value, report);
        else if (value instanceof Uint8Array && value.length > 0) {
          extendedFindings(Buffer.from(value).toString("utf8"), report);
        }
      }
    }
  }
  return rows;
};

export interface TableScan {
  /** Pattern findings by `table:kind`, on decoded content (nested JSON unwrapped). */
  readonly hits: Record<string, number>;
  /** Blank mode: values by `table.column` that are neither allowlisted nor filler, or a BLOB with data. */
  readonly notBlanked: Record<string, number>;
  readonly credentialColumns: Record<string, number>;
}

/**
 * Every value of every table. Blank mode checks both layers: each string is allowlisted and
 * unflagged, or filler; keys are identifiers; BLOBs are zero. Redact mode runs the pattern layer.
 */
export const scanTables = (db: NodeSqlite.DatabaseSync, mode: ContentMode): TableScan => {
  const hits: Record<string, number> = {};
  const notBlanked: Record<string, number> = {};
  const credentialColumns: Record<string, number> = {};
  for (const { name } of tableNames(db)) {
    const columns = columnsOf(db, name).map((column) => column.name);
    for (const column of columns) {
      if (!CREDENTIAL_COLUMN.test(column)) continue;
      const filled = count(
        db,
        `SELECT COUNT(*) AS n FROM ${quote(name)} WHERE ${quote(column)} IS NOT NULL`,
      );
      if (filled > 0) credentialColumns[`${name}.${column}`] = filled;
    }
    const structural = STRUCTURAL_TABLES.has(name);
    const statement = db.prepare(`SELECT ${columns.map(quote).join(", ")} FROM ${quote(name)}`);
    statement.setReturnArrays(true);
    // FTS shadow tables hold integers past 2^53; only text and BLOBs are read.
    statement.setReadBigInts(true);
    for (const row of statement.iterate() as Iterable<Array<unknown>>) {
      row.forEach((value, index) => {
        const column = columns[index] as string;
        if (typeof value === "string") {
          const report = (kind: string) =>
            kind === "not-blanked"
              ? bump(notBlanked, `${name}.${column}`)
              : bump(hits, `${name}:${kind}`);
          if (structural) scanDecoded(value, { entropy: false }).forEach(report);
          else contentFindings(value, column, mode, report);
        } else if (
          value instanceof Uint8Array &&
          mode === "blank" &&
          value.some((byte) => byte !== 0)
        ) {
          bump(notBlanked, `${name}.${column}`);
        }
      });
    }
  }
  return { hits, notBlanked, credentialColumns };
};

/** Row counts of every table, for the report's source-to-fixture comparison. */
const countRows = (path: string) => {
  const db = new NodeSqlite.DatabaseSync(path, { readOnly: true });
  try {
    return Object.fromEntries(
      tableNames(db).map(({ name }) => [
        name,
        count(db, `SELECT COUNT(*) AS n FROM ${quote(name)}`),
      ]),
    );
  } finally {
    db.close();
  }
};

const removeFiles = (path: string) => {
  for (const suffix of ["", "-wal", "-shm", "-journal"])
    NodeFS.rmSync(`${path}${suffix}`, { force: true });
};

const openWritable = (path: string) => {
  const db = new NodeSqlite.DatabaseSync(path, { enableForeignKeyConstraints: false });
  // A scratch copy: nothing to roll back to, and a failure deletes it.
  db.exec("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF");
  return db;
};

/** The cleared tables plus every table whose rows reference one, however far down the chain. */
export const clearedClosure = (db: NodeSqlite.DatabaseSync) => {
  const names = new Set(tableNames(db).map((row) => row.name));
  const references = db
    .prepare(
      `SELECT m.name, f."table" AS parent FROM sqlite_schema m, pragma_foreign_key_list(m.name) f
       WHERE m.type = 'table'`,
    )
    .all() as Array<{ name: string; parent: string }>;
  const cleared = new Set(CLEARED_TABLES.filter((table) => names.has(table)));
  for (let grew = true; grew;) {
    grew = false;
    for (const { name, parent } of references) {
      if (cleared.has(parent) && !cleared.has(name)) {
        cleared.add(name);
        grew = true;
      }
    }
  }
  return [...cleared];
};

const clearAndNeutralise = (db: NodeSqlite.DatabaseSync) => {
  const cleared: Record<string, number> = {};
  for (const table of clearedClosure(db)) {
    const result = db.prepare(`DELETE FROM ${quote(table)}`).run();
    cleared[table] = Number(result.changes);
  }
  // Auto-pull fetches each project's remote on start.
  const autoPull = Number(
    db.prepare(`UPDATE projection_projects SET auto_pull = 0 WHERE auto_pull <> 0`).run().changes,
  );
  // v1 sessions are history (the v2 recovery owns live work); a live status would read as running.
  const tables = new Set(tableNames(db).map((row) => row.name));
  const v1Sessions = tables.has("projection_thread_sessions")
    ? Number(
        db
          .prepare(
            `UPDATE projection_thread_sessions SET status = 'stopped'
             WHERE status NOT IN ('stopped', 'error')`,
          )
          .run().changes,
      )
    : 0;
  return { cleared, autoPull, v1Sessions };
};

/** What a settled app-owned subagent leaves (Orchestrator: a result and a consumed transfer). */
export const SETTLED_RESULT =
  "[bench fixture] Result withheld: the task was settled when the fixture was sanitised.";

const SUBAGENT_RESULT_CHILDREN = `
  SELECT child.thread_id FROM orchestration_v2_projection_threads AS child
  WHERE child.deleted_at IS NULL AND json_valid(child.payload_json)
    AND json_extract(child.payload_json, '$.lineage.relationshipToParent') = 'subagent'
    AND json_extract(child.payload_json, '$.lineage.parentThreadId') IS NOT NULL
    AND json_extract(child.payload_json, '$.forkedFrom.type') = 'node'
    AND (EXISTS (
      SELECT 1 FROM orchestration_v2_projection_runs AS requested
      JOIN orchestration_v2_projection_subagents AS task
        ON task.subagent_id = json_extract(requested.payload_json, '$.delegatedTaskId')
      WHERE requested.thread_id = child.thread_id
        AND requested.status IN ('completed', 'interrupted', 'failed', 'cancelled', 'rolled_back')
        AND task.child_thread_id = child.thread_id
        AND json_extract(task.payload_json, '$.origin') = 'app_owned'
        AND json_extract(task.payload_json, '$.result') IS NULL
    ) OR (
      (SELECT status FROM orchestration_v2_projection_runs
       WHERE thread_id = child.thread_id
         AND NOT (status = 'queued' AND json_extract(payload_json, '$.queueHeld') IS 1)
       ORDER BY ordinal DESC LIMIT 1
      ) IN ('completed', 'interrupted', 'failed', 'cancelled', 'rolled_back')
      AND NOT EXISTS (
        SELECT 1 FROM orchestration_v2_projection_context_transfers
        WHERE source_thread_id = child.thread_id
          AND target_thread_id = json_extract(child.payload_json, '$.lineage.parentThreadId')
          AND type = 'subagent_result')))`;

/**
 * What the restart recovery leaves open on purpose (requests answerable by a message, questions
 * it converted to them, app-owned delegations), closed as P1-002's sanitiser closes them: nothing
 * outside a held queued run stays pending, running or waiting.
 */
const settleLeftovers = (db: NodeSqlite.DatabaseSync, now: string) => {
  const changes = (sql: string, ...params: Array<string>) =>
    Number(db.prepare(sql).run(...params).changes);
  const queued = `SELECT run_id FROM orchestration_v2_projection_runs WHERE status = 'queued'`;
  return {
    requests: changes(
      `UPDATE orchestration_v2_projection_runtime_requests SET status = 'expired', resolved_at = ?,
         payload_json = json_set(payload_json, '$.status', 'expired', '$.resolvedAt', ?,
           '$.responseCapability', json_object('type', 'not_resumable',
             'reason', 'The server restarted before this runtime request was resolved.'))
       WHERE status = 'pending'`,
      now,
      now,
    ),
    nodes: changes(
      `UPDATE orchestration_v2_projection_nodes SET status = 'cancelled', completed_at = ?,
         payload_json = json_set(payload_json, '$.status', 'cancelled', '$.completedAt', ?)
       WHERE status IN ('pending', 'running', 'waiting')
         AND (run_id IS NULL OR run_id NOT IN (${queued}))`,
      now,
      now,
    ),
    providerTurns: changes(
      `UPDATE orchestration_v2_projection_provider_turns SET status = 'cancelled', completed_at = ?,
         payload_json = json_set(payload_json, '$.status', 'cancelled', '$.completedAt', ?)
       WHERE status IN ('pending', 'running')`,
      now,
      now,
    ),
    turnItems: changes(
      `UPDATE orchestration_v2_projection_turn_items SET status = 'cancelled', updated_at = ?,
         payload_json = json_set(payload_json, '$.status', 'cancelled', '$.completedAt', ?,
           '$.updatedAt', ?)
       WHERE status NOT IN ('completed', 'interrupted', 'failed', 'cancelled')
         AND (run_id IS NULL OR run_id NOT IN (${queued}))`,
      now,
      now,
      now,
    ),
    secretRequests: changes(
      `UPDATE orchestration_v2_projection_turn_items
       SET payload_json = json_set(payload_json, '$.secretStatus', 'cancelled')
       WHERE type = 'secret_request' AND json_extract(payload_json, '$.secretStatus') = 'pending'`,
    ),
    subagentItems: changes(
      `UPDATE orchestration_v2_projection_turn_items
       SET payload_json = json_set(payload_json, '$.result', ?)
       WHERE type = 'subagent' AND status = 'cancelled' AND json_type(payload_json, '$.result') IS 'null'`,
      SETTLED_RESULT,
    ),
    providerThreads: changes(
      `UPDATE orchestration_v2_projection_provider_threads
       SET status = CASE WHEN status = 'active' THEN 'idle' ELSE status END, updated_at = ?,
         payload_json = json_set(payload_json, '$.status',
           CASE WHEN status = 'active' THEN 'idle' ELSE status END,
           '$.pendingBackgroundTasks', json('[]'), '$.updatedAt', ?)
       WHERE status = 'active' OR (json_valid(payload_json)
         AND json_array_length(payload_json, '$.pendingBackgroundTasks') > 0)`,
      now,
      now,
    ),
    providerSessions: changes(
      `UPDATE orchestration_v2_projection_provider_sessions SET status = 'stopped', updated_at = ?,
         payload_json = json_set(payload_json, '$.status', 'stopped', '$.updatedAt', ?,
           '$.lastError', json('null'))
       WHERE status NOT IN ('stopped', 'error')`,
      now,
      now,
    ),
    subagents: changes(
      `UPDATE orchestration_v2_projection_subagents SET status = 'cancelled', completed_at = ?,
         updated_at = ?,
         payload_json = json_set(payload_json, '$.status', 'cancelled', '$.completedAt', ?,
           '$.updatedAt', ?)
       WHERE status IN ('pending', 'running', 'waiting')`,
      now,
      now,
      now,
      now,
    ),
  };
};

/**
 * What a boot would still deliver, settled as the server leaves it once done (as P1-002's
 * sanitiser does): delegated and subagent deliveries closed, streaming messages final, finished
 * subagent children given a result and a consumed transfer, failed v1 transcripts marked imported.
 */
const settleDeliveries = (db: NodeSqlite.DatabaseSync, now: string) => {
  const changes = (sql: string, ...params: Array<string | null>) =>
    Number(db.prepare(sql).run(...params).changes);
  const tables = new Set(tableNames(db).map((row) => row.name));
  const streaming = changes(
    `UPDATE orchestration_v2_projection_messages SET streaming = 0, updated_at = ?,
       payload_json = json_set(payload_json, '$.streaming', json('false'), '$.updatedAt', ?)
     WHERE streaming = 1`,
    now,
    now,
  );
  const transcriptsUnimportable = tables.has("orchestration_v2_legacy_imports")
    ? changes(
        `UPDATE orchestration_v2_legacy_imports
         SET transcript_imported_at = COALESCE(shell_imported_at, ?)
         WHERE transcript_imported_at IS NULL`,
        now,
      )
    : 0;
  const delegated = changes(
    `UPDATE orchestration_v2_projection_runs
     SET payload_json = json_set(payload_json, '$.delegatedCompletion.delivery', json('null'))
     WHERE json_valid(payload_json)
       AND json_type(payload_json, '$.delegatedCompletion.delivery') = 'object'`,
  );
  const deliveries = changes(
    `UPDATE orchestration_v2_projection_subagents
     SET payload_json = json_set(payload_json, '$.completionDelivery.state', 'disposed')
     WHERE json_extract(payload_json, '$.completionDelivery.state') = 'pending'`,
  );
  let results = 0;
  let closed = 0;
  let transfers = 0;
  let unsettled = 0;
  for (let round = 0; round < 3; round++) {
    const children = (
      db.prepare(SUBAGENT_RESULT_CHILDREN).all() as Array<{ thread_id: string }>
    ).map((row) => row.thread_id);
    if (children.length === 0) break;
    for (const child of children) {
      const thread = db
        .prepare(
          `SELECT json_extract(payload_json, '$.lineage.parentThreadId') AS parent
           FROM orchestration_v2_projection_threads WHERE thread_id = ?`,
        )
        .get(child) as { parent: string };
      const run = db
        .prepare(
          `SELECT run_id, provider, status,
             COALESCE(provider_instance_id, json_extract(payload_json, '$.providerInstanceId')) AS instance
           FROM orchestration_v2_projection_runs
           WHERE thread_id = ? AND NOT (status = 'queued' AND json_extract(payload_json, '$.queueHeld') IS 1)
           ORDER BY ordinal DESC LIMIT 1`,
        )
        .get(child) as
        | { run_id: string; provider: string; status: string; instance: string | null }
        | undefined;
      results += changes(
        `UPDATE orchestration_v2_projection_subagents
         SET payload_json = json_set(payload_json, '$.result', ?)
         WHERE child_thread_id = ? AND json_extract(payload_json, '$.origin') = 'app_owned'
           AND json_extract(payload_json, '$.result') IS NULL`,
        SETTLED_RESULT,
        child,
      );
      // The task, its node and the parent's mirrored item end as the child's run did: the restart
      // recovery leaves app-owned delegations open for the child to close.
      if (run !== undefined) {
        const ended =
          run.status === "completed" || run.status === "failed" ? run.status : "cancelled";
        const tasks = `SELECT subagent_id FROM orchestration_v2_projection_subagents
          WHERE child_thread_id = ? AND json_extract(payload_json, '$.origin') = 'app_owned'`;
        closed += changes(
          `UPDATE orchestration_v2_projection_subagents SET status = ?, updated_at = ?,
             completed_at = COALESCE(completed_at, ?),
             payload_json = json_set(payload_json, '$.status', ?, '$.updatedAt', ?,
               '$.completedAt', COALESCE(json_extract(payload_json, '$.completedAt'), ?))
           WHERE subagent_id IN (${tasks}) AND status IN ('idle', 'pending', 'running', 'waiting')`,
          ended,
          now,
          now,
          ended,
          now,
          now,
          child,
        );
        closed += changes(
          `UPDATE orchestration_v2_projection_nodes SET status = ?, completed_at = COALESCE(completed_at, ?),
             payload_json = json_set(payload_json, '$.status', ?,
               '$.completedAt', COALESCE(json_extract(payload_json, '$.completedAt'), ?))
           WHERE node_id IN (${tasks}) AND status IN ('idle', 'pending', 'running', 'waiting')`,
          ended,
          now,
          ended,
          now,
          child,
        );
        closed += changes(
          `UPDATE orchestration_v2_projection_turn_items SET status = ?, updated_at = ?,
             payload_json = json_set(payload_json, '$.status', ?, '$.updatedAt', ?,
               '$.completedAt', COALESCE(json_extract(payload_json, '$.completedAt'), ?),
               '$.result', COALESCE(json_extract(payload_json, '$.result'), ?))
           WHERE type = 'subagent' AND json_extract(payload_json, '$.subagentId') IN (${tasks})
             AND status NOT IN ('completed', 'interrupted', 'failed', 'cancelled')`,
          ended,
          now,
          ended,
          now,
          now,
          SETTLED_RESULT,
          child,
        );
      }
      const delivered = db
        .prepare(
          `SELECT 1 FROM orchestration_v2_projection_context_transfers
           WHERE source_thread_id = ? AND target_thread_id = ? AND type = 'subagent_result'`,
        )
        .get(child, thread.parent);
      if (delivered !== undefined || run === undefined) continue;
      const task = db
        .prepare(
          `SELECT run_id FROM orchestration_v2_projection_subagents
           WHERE child_thread_id = ? AND thread_id = ? AND json_extract(payload_json, '$.origin') = 'app_owned'
           ORDER BY updated_at DESC LIMIT 1`,
        )
        .get(child, thread.parent) as { run_id: string | null } | undefined;
      const parentRunId = task?.run_id ?? null;
      const parent = (db
        .prepare(
          `SELECT provider, COALESCE(provider_instance_id, json_extract(payload_json, '$.providerInstanceId')) AS instance
           FROM orchestration_v2_projection_runs WHERE run_id = ?`,
        )
        .get(parentRunId) ??
        db
          .prepare(
            `SELECT default_provider AS provider,
               COALESCE(provider_instance_id, json_extract(payload_json, '$.providerInstanceId')) AS instance
             FROM orchestration_v2_projection_threads WHERE thread_id = ?`,
          )
          .get(thread.parent)) as { provider: string; instance: string | null } | undefined;
      if (parent?.instance == null || run.instance === null) {
        unsettled++;
        continue;
      }
      const id = `context-transfer:fixture-settled:${child}`;
      const payload = {
        id,
        type: "subagent_result",
        sourceThreadId: child,
        targetThreadId: thread.parent,
        sourcePoint: { threadId: child, runId: run.run_id },
        basePoint: null,
        sourceProviderInstanceId: run.instance,
        targetProviderInstanceId: parent.instance,
        targetRunId: parentRunId,
        status: "consumed",
        resolution: null,
        createdBy: "system",
        error: null,
        createdAt: now,
        updatedAt: now,
        consumedAt: now,
      };
      transfers += changes(
        `INSERT INTO orchestration_v2_projection_context_transfers (context_transfer_id,
           source_thread_id, target_thread_id, target_run_id, type, status, source_provider,
           target_provider, updated_at, payload_json, source_provider_instance_id,
           target_provider_instance_id)
         VALUES (?, ?, ?, ?, 'subagent_result', 'consumed', ?, ?, ?, ?, ?, ?)`,
        id,
        child,
        thread.parent,
        parentRunId,
        run.provider,
        parent.provider,
        now,
        JSON.stringify(payload),
        run.instance,
        parent.instance,
      );
    }
  }
  const leftover = settleLeftovers(db, now);
  return {
    delegated,
    deliveries,
    streaming,
    results,
    closed,
    transfers,
    unsettled,
    transcriptsUnimportable,
    leftover,
  };
};

/**
 * Rewrites every string and BLOB of every table but the structural ones: blank mode leaves only
 * the allowlist and filler, redact mode masks what the pattern layer finds. Triggers (the message
 * search index) are dropped for the pass and restored; the index is rebuilt afterwards.
 */
const rewriteTables = (db: NodeSqlite.DatabaseSync, mode: ContentMode) => {
  const counts = emptyCounts();
  const byTable: Record<string, number> = {};
  db.function(
    "fixture_content",
    { deterministic: true, useBigIntArguments: true },
    (value, column) => {
      if (typeof value === "string") return rewriteContent(value, String(column), mode, counts);
      if (value instanceof Uint8Array && mode === "blank") {
        if (value.some((byte) => byte !== 0)) counts.changed++;
        return new Uint8Array(value.length);
      }
      return value;
    },
  );
  const triggers = db
    .prepare(`SELECT name, sql FROM sqlite_schema WHERE type = 'trigger' AND sql IS NOT NULL`)
    .all() as Array<{ name: string; sql: string }>;
  // No journal to roll back with on failure: the caller deletes the scratch copy.
  db.exec("BEGIN");
  for (const trigger of triggers) db.exec(`DROP TRIGGER ${quote(trigger.name)}`);
  for (const { name } of tableNames(db)) {
    if (STRUCTURAL_TABLES.has(name)) continue;
    const columns = columnsOf(db, name).map((column) => column.name);
    const before = counts.changed;
    db.prepare(
      `UPDATE ${quote(name)} SET ${columns
        .map((column) => `${quote(column)} = fixture_content(${quote(column)}, ${literal(column)})`)
        .join(", ")}`,
    ).run();
    if (counts.changed > before) byTable[name] = counts.changed - before;
  }
  for (const trigger of triggers) db.exec(trigger.sql);
  db.exec("COMMIT");
  return { ...counts, byTable };
};

/** True when VACUUM INTO kept every message rowid, which the contentless search index keys on. */
const sameMessageRowids = (left: string, right: string) => {
  const query = `SELECT rowid, message_id FROM orchestration_v2_projection_messages ORDER BY rowid`;
  const a = new NodeSqlite.DatabaseSync(left, { readOnly: true });
  const b = new NodeSqlite.DatabaseSync(right, { readOnly: true });
  try {
    const exists = a.prepare(
      `SELECT 1 FROM sqlite_schema WHERE name = 'orchestration_v2_projection_messages'`,
    );
    if (exists.get() === undefined) return true;
    const rightRows = b.prepare(query).iterate() as Iterator<{ rowid: number; message_id: string }>;
    for (const row of a.prepare(query).iterate() as Iterable<{
      rowid: number;
      message_id: string;
    }>) {
      const other = rightRows.next();
      if (
        other.done ||
        other.value.rowid !== row.rowid ||
        other.value.message_id !== row.message_id
      )
        return false;
    }
    return rightRows.next().done === true;
  } finally {
    a.close();
    b.close();
  }
};

/**
 * VACUUM INTO `destination`, created empty, private and exclusively first (SQLite fills an empty
 * file): no free pages, so nothing deleted or blanked survives on disk.
 */
export const compactInto = Effect.fn("fixture.compactInto")(function* (
  source: string,
  destination: string,
) {
  createExclusive(destination);
  const db = new NodeSqlite.DatabaseSync(source);
  try {
    db.prepare(`VACUUM INTO ?`).run(destination);
  } finally {
    db.close();
  }
  if (!sameMessageRowids(source, destination)) {
    yield* rebuildSearch.pipe(Effect.provide(database(destination)));
    const compacted = new NodeSqlite.DatabaseSync(destination);
    try {
      compacted.exec(`PRAGMA incremental_vacuum`);
    } finally {
      compacted.close();
    }
  }
});

export interface FixtureCheck {
  readonly mode: ContentMode;
  readonly secretHits: Record<string, number>;
  /** P1-002's extended check: `table:scope|kind` counts, all of which must be zero. */
  readonly extendedHits: Record<string, number>;
  readonly notBlanked: Record<string, number>;
  readonly credentialColumns: Record<string, number>;
  readonly rawHits: Record<string, number>;
  readonly runnable: Record<string, number>;
}

/** Both layers on every table, the token patterns on the raw file, and the work a boot would run. */
export const checkFixture = Effect.fn("fixture.check")(function* (path: string, mode: ContentMode) {
  const db = new NodeSqlite.DatabaseSync(path, { readOnly: true });
  let tables: TableScan;
  let extendedHits: Record<string, number>;
  try {
    tables = scanTables(db, mode);
    extendedHits = extendedTables(db);
  } finally {
    db.close();
  }
  const runnable = yield* runnableWork.pipe(Effect.provide(withProjections(path)));
  return {
    mode,
    secretHits: tables.hits,
    extendedHits,
    notBlanked: tables.notBlanked,
    credentialColumns: tables.credentialColumns,
    rawHits: Object.fromEntries(scanFileBytes(path)),
    runnable: { ...runnable },
  } satisfies FixtureCheck;
});

export const isClean = (check: FixtureCheck) =>
  Object.keys(check.secretHits).length === 0 &&
  Object.keys(check.extendedHits).length === 0 &&
  Object.keys(check.notBlanked).length === 0 &&
  Object.keys(check.credentialColumns).length === 0 &&
  Object.keys(check.rawHits).length === 0 &&
  !isRunnable(check.runnable as never);

/**
 * Files the command created, removed if it fails or is interrupted. SIGINT and SIGTERM clean up
 * and exit (130, 143); SIGKILL cannot, and leaves the scratch directory beside the destination.
 */
export const ownedFiles = () => {
  const files: Array<string> = [];
  const directories: Array<string> = [];
  const cleanup = () => {
    for (const file of files.splice(0)) removeFiles(file);
    for (const directory of directories.splice(0))
      NodeFS.rmSync(directory, { recursive: true, force: true });
  };
  const onSignal = (signal: NodeJS.Signals) => {
    cleanup();
    process.exit(signal === "SIGINT" ? 130 : 143);
  };
  return {
    file: (path: string) => files.push(path),
    directory: (path: string) => directories.push(path),
    cleanup,
    /** Keeps the outputs: only the scratch directory goes from here on. */
    keep: () => files.splice(0),
    watch: () => {
      process.on("SIGINT", onSignal);
      process.on("SIGTERM", onSignal);
    },
    unwatch: () => {
      process.off("SIGINT", onSignal);
      process.off("SIGTERM", onSignal);
    },
  };
};

export const runSanitise = Effect.fn("fixture.sanitise")(function* (input: {
  readonly source: string;
  readonly destination: string;
  readonly report?: string | undefined;
  /** Pattern redaction only, content kept: fails open by nature, never for a shared fixture. */
  readonly keepContent?: boolean | undefined;
}) {
  const mode: ContentMode = input.keepContent === true ? "redact" : "blank";
  const destination = yield* safeDestination({
    destination: input.destination,
    source: input.source,
    ...(input.report === undefined ? {} : { distinctFrom: [input.report] }),
  });
  const report =
    input.report === undefined
      ? undefined
      : yield* safeDestination({
          destination: input.report,
          source: input.source,
          distinctFrom: withSidecars(input.destination),
        });
  if (!NodeFS.statSync(input.source, { throwIfNoEntry: false })?.isFile()) {
    return yield* failed(`Source database ${input.source} does not exist.`);
  }
  const owned = ownedFiles();
  return yield* Effect.gen(function* () {
    owned.watch();
    if (report !== undefined) {
      createExclusive(report);
      owned.file(report);
    }
    // Private: the copy holds everything until the content pass, so it sits in a 0700 directory.
    const directory = privateScratchDirectory(destination, ".fixture-sanitise-");
    owned.directory(directory);
    const scratch = NodePath.join(directory, "scratch.sqlite");
    yield* Console.log(`Copying ${input.source} (read-only, VACUUM INTO) into ${directory}...`);
    createExclusive(scratch);
    const source = new NodeSqlite.DatabaseSync(input.source, { readOnly: true });
    try {
      source.prepare(`VACUUM INTO ?`).run(scratch);
    } finally {
      source.close();
    }

    yield* Console.log("Running the server's migrations...");
    const executed = yield* migrate.pipe(Effect.provide(database(scratch)));
    const sourceCounts = countRows(scratch);
    const live = yield* recoveryThreadIds.pipe(Effect.provide(withProjections(scratch)));

    yield* Console.log("Clearing credentials, links and queued work...");
    const db = openWritable(scratch);
    let neutralised: ReturnType<typeof clearAndNeutralise>;
    try {
      db.exec("BEGIN");
      neutralised = clearAndNeutralise(db);
      db.exec("COMMIT");
    } finally {
      db.close();
    }
    yield* Console.log("Running the server's start-up work: v1 import, restart recovery...");
    const legacy = yield* legacyImport.pipe(Effect.provide(withLegacyImporter(scratch)));
    const restart = yield* restartRecovery.pipe(Effect.provide(withRecovery(scratch)));
    const settling = openWritable(scratch);
    let deliveries: ReturnType<typeof settleDeliveries>;
    try {
      settling.exec("BEGIN");
      deliveries = settleDeliveries(settling, DateTime.formatIso(yield* DateTime.now));
      settling.exec("COMMIT");
    } finally {
      settling.close();
    }
    const neutralisedThreads = yield* neutraliseThreads.pipe(
      Effect.provide(withEventSink(scratch)),
    );
    yield* Console.log("Compacting the event store as a first boot would...");
    const compaction = yield* compactEvents.pipe(Effect.provide(withMaintenance(scratch)));
    const startup = yield* runnableWork.pipe(Effect.provide(withProjections(scratch)));
    if (isRunnable(startup)) {
      return yield* failed(
        `Start-up work left after neutralising, nothing written: ${JSON.stringify(
          Object.fromEntries(Object.entries(startup).filter(([, n]) => n > 0)),
        )}`,
      );
    }
    const readBefore = undecodableRows(scratch);

    yield* Console.log(
      mode === "blank"
        ? "Blanking content: filler for every string outside the allowlist..."
        : "Redacting credential patterns (--keep-content: content kept)...",
    );
    const content = yield* Effect.try({
      try: () => {
        const rewriting = openWritable(scratch);
        try {
          return rewriteTables(rewriting, mode);
        } finally {
          rewriting.close();
        }
      },
      catch: (cause) => failed("The content pass failed; nothing written", cause),
    });
    yield* rebuildSearch.pipe(Effect.provide(database(scratch)));
    const readAfter = undecodableRows(scratch);
    const broken: Record<string, number> = {};
    for (const row of readAfter) {
      if (!readBefore.has(row)) bump(broken, row.slice(0, row.lastIndexOf(":")));
    }
    if (Object.keys(broken).length > 0) {
      return yield* failed(
        `The content pass made rows undecodable, nothing written: ${JSON.stringify(broken)}`,
      );
    }

    yield* Console.log(`Compacting into ${destination}...`);
    owned.file(destination);
    yield* compactInto(scratch, destination);

    yield* Console.log("Checking the result...");
    const check = yield* checkFixture(destination, mode);
    if (!isClean(check)) {
      return yield* failed(`Self-check failed, nothing written: ${JSON.stringify(check)}`);
    }
    const fixtureCounts = countRows(destination);
    const result = {
      destination,
      mode,
      sizeBytes: NodeFS.statSync(destination).size,
      migrations: executed.map(([id, name]) => `${id}_${name}`),
      threadsWithStartupWork: live.size,
      clearedRows: neutralised.cleared,
      autoPullDisabled: neutralised.autoPull,
      v1SessionsStopped: neutralised.v1Sessions,
      legacyImport: legacy,
      restartRecovery: restart,
      deliveriesSettled: deliveries,
      ...neutralisedThreads,
      compaction,
      rows: Object.fromEntries(
        Object.entries(sourceCounts).map(([table, rows]) => [
          table,
          { source: rows, fixture: fixtureCounts[table] ?? 0 },
        ]),
      ),
      content,
      undecodableRows: { before: readBefore.size, after: readAfter.size },
      check,
    };
    if (report !== undefined) fillExclusive(report, `${JSON.stringify(result, null, 2)}\n`);
    owned.keep();
    return result;
  }).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        owned.cleanup();
        owned.unwatch();
      }),
    ),
  );
});

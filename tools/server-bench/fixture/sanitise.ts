// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - copies, edits and scans SQLite files through node:sqlite.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as Console from "effect/Console";
import * as Effect from "effect/Effect";

import { CREDENTIAL_COLUMN, MAYBE_SECRET, redact, scanFileBytes, secretHits } from "./secrets.ts";
import {
  WATCHED_THREADS,
  database,
  disableAutoSettle,
  failed,
  isRunnable,
  migrate,
  rebuildSearch,
  recoveryThreadIds,
  runnableWork,
  safeDestination,
  withEventSink,
  withProjections,
} from "./server.ts";
import { withSidecars } from "./guards.ts";

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
];

const PROVIDER_SESSIONS = "orchestration_v2_projection_provider_sessions";
const BINDINGS = "orchestration_v2_projection_provider_session_bindings";
const FTS_SHADOW = /^orchestration_v2_message_search_(?:data|idx|content|docsize|config)$/;

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

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

/** TEXT affinity or no declared type: where free text, and so a pasted credential, can sit. */
const textColumns = (db: NodeSqlite.DatabaseSync, table: string) =>
  columnsOf(db, table)
    .filter(({ type }) => type === "" || /CHAR|CLOB|TEXT/i.test(type))
    .map(({ name }) => name);

const count = (db: NodeSqlite.DatabaseSync, sql: string, ...params: Array<string>) =>
  Number((db.prepare(sql).get(...params) as { n: number }).n);

export interface TextScan {
  readonly hits: Record<string, number>;
  readonly credentialColumns: Record<string, number>;
}

/** Every text column of every table, by the precise patterns, plus credential-named columns. */
export const scanTables = (db: NodeSqlite.DatabaseSync): TextScan => {
  const hits: Record<string, number> = {};
  const credentialColumns: Record<string, number> = {};
  for (const { name } of tableNames(db)) {
    for (const column of columnsOf(db, name)) {
      if (CREDENTIAL_COLUMN.test(column.name)) {
        const filled = count(
          db,
          `SELECT COUNT(*) AS n FROM ${quote(name)} WHERE ${quote(column.name)} IS NOT NULL`,
        );
        if (filled > 0) credentialColumns[`${name}.${column.name}`] = filled;
      }
    }
    const columns = textColumns(db, name);
    if (columns.length === 0) continue;
    const statement = db.prepare(`SELECT ${columns.map(quote).join(", ")} FROM ${quote(name)}`);
    statement.setReturnArrays(true);
    for (const row of statement.iterate() as Iterable<Array<unknown>>) {
      for (const value of row) {
        if (typeof value !== "string" || !MAYBE_SECRET.test(value)) continue;
        for (const hit of secretHits(value))
          hits[`${name}:${hit}`] = (hits[`${name}:${hit}`] ?? 0) + 1;
      }
    }
  }
  return { hits, credentialColumns };
};

const removeFiles = (path: string) => {
  for (const suffix of ["", "-wal", "-shm", "-journal"])
    NodeFS.rmSync(`${path}${suffix}`, { force: true });
};

const openWritable = (path: string) => {
  const db = new NodeSqlite.DatabaseSync(path, { enableForeignKeyConstraints: false });
  db.function("fixture_maybe_secret", { deterministic: true }, (value) =>
    typeof value === "string" && MAYBE_SECRET.test(value) ? 1 : 0,
  );
  db.function("fixture_redact", { deterministic: true }, (value) =>
    typeof value === "string" ? redact(value) : value,
  );
  return db;
};

/** Drops whole thread families that hold live work, as `migrate-dev-db` excludes them. */
const dropLiveFamilies = (db: NodeSqlite.DatabaseSync, live: ReadonlySet<string>) => {
  db.exec(`
    CREATE TEMP TABLE live_threads (thread_id TEXT PRIMARY KEY);
    CREATE TEMP TABLE dropped_threads (thread_id TEXT PRIMARY KEY);
  `);
  const insert = db.prepare(`INSERT OR IGNORE INTO live_threads (thread_id) VALUES (?)`);
  for (const id of live) insert.run(id);
  // Usage-limit recovery also depends on settings: a failed latest run counts as live.
  db.exec(`
    INSERT OR IGNORE INTO live_threads (thread_id)
      SELECT r.thread_id FROM orchestration_v2_projection_runs r
      WHERE r.status = 'failed' AND r.ordinal = (
        SELECT MAX(latest.ordinal) FROM orchestration_v2_projection_runs latest
        WHERE latest.thread_id = r.thread_id)
      UNION
      SELECT thread_id FROM orchestration_v2_projection_threads
      WHERE json_extract(payload_json, '$.limitRecovery') IS NOT NULL
      UNION ${WATCHED_THREADS};
    CREATE TEMP TABLE thread_families AS
      SELECT thread_id, COALESCE(json_extract(payload_json, '$.lineage.rootThreadId'), thread_id) AS root_id
      FROM orchestration_v2_projection_threads;
    INSERT OR IGNORE INTO dropped_threads (thread_id)
      SELECT thread_id FROM thread_families WHERE root_id IN (
        SELECT family.root_id FROM thread_families family
        JOIN live_threads live ON live.thread_id = family.thread_id);
    INSERT OR IGNORE INTO dropped_threads (thread_id) SELECT thread_id FROM live_threads;
  `);
  const dropped = count(db, `SELECT COUNT(*) AS n FROM temp.dropped_threads`);
  const removed: Record<string, number> = {};
  const remove = (table: string, where: string) => {
    const result = db.prepare(`DELETE FROM ${quote(table)} WHERE ${where}`).run();
    if (Number(result.changes) > 0) removed[table] = (removed[table] ?? 0) + Number(result.changes);
  };
  db.exec(`
    CREATE TEMP TABLE dropped_sessions AS
      SELECT provider_session_id FROM ${BINDINGS} WHERE thread_id IN (SELECT thread_id FROM temp.dropped_threads)
      UNION SELECT provider_session_id FROM ${PROVIDER_SESSIONS}
        WHERE thread_id IN (SELECT thread_id FROM temp.dropped_threads)
      EXCEPT SELECT provider_session_id FROM ${BINDINGS}
        WHERE thread_id NOT IN (SELECT thread_id FROM temp.dropped_threads);
  `);
  for (const { name } of tableNames(db)) {
    if (name === PROVIDER_SESSIONS) continue;
    const columns = columnsOf(db, name).map((column) => column.name);
    if (columns.includes("thread_id")) {
      remove(name, `thread_id IN (SELECT thread_id FROM temp.dropped_threads)`);
    }
  }
  remove(
    PROVIDER_SESSIONS,
    `provider_session_id IN (SELECT provider_session_id FROM temp.dropped_sessions)`,
  );
  remove(
    "orchestration_v2_projection_context_transfers",
    `source_thread_id IN (SELECT thread_id FROM temp.dropped_threads)
      OR target_thread_id IN (SELECT thread_id FROM temp.dropped_threads)`,
  );
  remove(
    "orchestration_v2_projection_subagents",
    `child_thread_id IN (SELECT thread_id FROM temp.dropped_threads)`,
  );
  remove(
    "orchestration_events",
    `aggregate_kind = 'thread' AND stream_id IN (SELECT thread_id FROM temp.dropped_threads)`,
  );
  // The projection still covers every remaining event; keep `t3 doctor` verification valid.
  db.exec(`
    UPDATE orchestration_v2_projection_metadata
    SET last_sequence = (
      SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events
      WHERE application_event_version = 2 AND aggregate_kind = 'thread')
    WHERE last_sequence > (
      SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events
      WHERE application_event_version = 2 AND aggregate_kind = 'thread');
  `);
  return { dropped, removed };
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
  return { cleared, autoPull };
};

const redactTables = (db: NodeSqlite.DatabaseSync) => {
  const redacted: Record<string, number> = {};
  for (const { name } of tableNames(db)) {
    const columns = textColumns(db, name);
    if (columns.length === 0) continue;
    const set = columns
      .map(
        (column) =>
          `${quote(column)} = CASE WHEN fixture_maybe_secret(${quote(column)}) THEN fixture_redact(${quote(column)}) ELSE ${quote(column)} END`,
      )
      .join(", ");
    const where = columns.map((column) => `fixture_maybe_secret(${quote(column)})`).join(" OR ");
    const result = db.prepare(`UPDATE ${quote(name)} SET ${set} WHERE ${where}`).run();
    if (Number(result.changes) > 0) redacted[name] = Number(result.changes);
  }
  return redacted;
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

/** VACUUM INTO `destination`: no free pages, so nothing deleted or redacted survives on disk. */
export const compactInto = Effect.fn("fixture.compactInto")(function* (
  source: string,
  destination: string,
) {
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
  readonly secretHits: Record<string, number>;
  readonly credentialColumns: Record<string, number>;
  readonly rawHits: Record<string, number>;
  readonly runnable: Record<string, number>;
}

export const checkFixture = Effect.fn("fixture.check")(function* (path: string) {
  const db = new NodeSqlite.DatabaseSync(path, { readOnly: true });
  let tables: TextScan;
  try {
    tables = scanTables(db);
  } finally {
    db.close();
  }
  const runnable = yield* runnableWork.pipe(Effect.provide(withProjections(path)));
  return {
    secretHits: tables.hits,
    credentialColumns: tables.credentialColumns,
    rawHits: Object.fromEntries(scanFileBytes(path)),
    runnable: { ...runnable },
  } satisfies FixtureCheck;
});

export const isClean = (check: FixtureCheck) =>
  Object.keys(check.secretHits).length === 0 &&
  Object.keys(check.credentialColumns).length === 0 &&
  Object.keys(check.rawHits).length === 0 &&
  !isRunnable(check.runnable as never);

export const runSanitise = Effect.fn("fixture.sanitise")(function* (input: {
  readonly source: string;
  readonly destination: string;
  readonly report?: string | undefined;
}) {
  const scratch = `${input.destination}.sanitise-tmp`;
  const destination = yield* safeDestination({
    destination: input.destination,
    source: input.source,
    scratch: withSidecars(scratch),
    ...(input.report === undefined ? {} : { distinctFrom: [input.report] }),
  });
  if (input.report !== undefined) {
    yield* safeDestination({
      destination: input.report,
      source: input.source,
      distinctFrom: [...withSidecars(input.destination), ...withSidecars(scratch)],
    });
  }
  if (!NodeFS.statSync(input.source, { throwIfNoEntry: false })?.isFile()) {
    return yield* failed(`Source database ${input.source} does not exist.`);
  }
  return yield* Effect.gen(function* () {
    yield* Console.log(`Copying ${input.source} (read-only, VACUUM INTO)...`);
    const source = new NodeSqlite.DatabaseSync(input.source, { readOnly: true });
    try {
      source.prepare(`VACUUM INTO ?`).run(scratch);
    } finally {
      source.close();
    }

    yield* Console.log("Running the server's migrations and finding live threads...");
    const executed = yield* migrate.pipe(Effect.provide(database(scratch)));
    const live = yield* recoveryThreadIds.pipe(Effect.provide(withProjections(scratch)));

    yield* Console.log(
      "Dropping live thread families, clearing credentials and queued work, redacting...",
    );
    const db = openWritable(scratch);
    let pruned: ReturnType<typeof dropLiveFamilies>;
    let neutralised: ReturnType<typeof clearAndNeutralise>;
    try {
      db.exec("BEGIN");
      pruned = dropLiveFamilies(db, live);
      neutralised = clearAndNeutralise(db);
      db.exec("COMMIT");
    } finally {
      db.close();
    }
    const autoSettleDisabled = yield* disableAutoSettle.pipe(
      Effect.provide(withEventSink(scratch)),
    );
    const redacting = openWritable(scratch);
    let redacted: Record<string, number>;
    try {
      redacting.exec("BEGIN");
      redacted = redactTables(redacting);
      redacting.exec("COMMIT");
    } finally {
      redacting.close();
    }
    yield* rebuildSearch.pipe(Effect.provide(database(scratch)));

    yield* Console.log(`Compacting into ${destination}...`);
    yield* compactInto(scratch, destination);
    NodeFS.chmodSync(destination, 0o600);

    yield* Console.log("Checking the result...");
    const check = yield* checkFixture(destination);
    if (!isClean(check)) {
      removeFiles(destination);
      return yield* failed(`Self-check failed, nothing written: ${JSON.stringify(check)}`);
    }
    return {
      destination,
      sizeBytes: NodeFS.statSync(destination).size,
      migrations: executed.map(([id, name]) => `${id}_${name}`),
      liveThreads: live.size,
      droppedThreads: pruned.dropped,
      removedRows: pruned.removed,
      clearedRows: neutralised.cleared,
      autoPullDisabled: neutralised.autoPull,
      autoSettleDisabled,
      redactedRows: redacted,
      check,
    };
  }).pipe(
    Effect.onError(() => Effect.sync(() => removeFiles(destination))),
    Effect.ensuring(Effect.sync(() => removeFiles(scratch))),
  );
});

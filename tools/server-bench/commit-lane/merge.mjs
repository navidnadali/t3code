// The migration bench's merge.mjs, lint-only edits.
// Merges synth shards into one database: shard 0's schema (auto_vacuum NONE), events renumbered (promptSequence
// moved with them), messages and turn items through the destination's triggers, last_sequence set to the merged head.
// usage: node merge.mjs --dest <db> [--free-pages N] [--prune-mark-at ISO] [--pad-events-bytes N | --pad-events-bytes-file F] <shard>...
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

const args = process.argv.slice(2);
const opt = (name, fallback) => {
  const at = args.indexOf(name);
  if (at < 0) return fallback;
  const value = args[at + 1];
  args.splice(at, 2);
  return value;
};
const has = (name) => {
  const at = args.indexOf(name);
  if (at < 0) return false;
  args.splice(at, 1);
  return true;
};
const dest = opt("--dest");
const freePages = Number(opt("--free-pages", "0"));
const pruneMarkAt = opt("--prune-mark-at", "");
const deleteShards = has("--delete-shards");
const cacheMiB = Number(opt("--cache-mib", "4096"));
// Synth event payloads run below a busy server's per-row size; pad them (mean N chars, uniform N/2..3N/2) in a
// field no migration reads. N comes from --pad-events-bytes, else from the file --pad-events-bytes-file names
// (so a running pipeline can change it between merges), else 0.
const padFile = opt("--pad-events-bytes-file", "");
const padBytes = opt("--pad-events-bytes", "");
const padEvents = Number(
  padBytes !== ""
    ? padBytes
    : padFile !== "" && NodeFS.existsSync(padFile)
      ? NodeFS.readFileSync(padFile, "utf8").trim()
      : "0",
);
const shards = args;
if (!dest || NodeFS.existsSync(dest) || shards.length === 0)
  throw new Error("usage: --dest <new db> <shard>...");
const t0 = Date.now();
const log = (message) =>
  console.log(`${new Date().toISOString()} +${((Date.now() - t0) / 1000).toFixed(0)}s ${message}`);

const db = new NodeSqlite.DatabaseSync(dest);
db.exec(`PRAGMA page_size = 4096; PRAGMA auto_vacuum = NONE; PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;
  PRAGMA cache_size = -${cacheMiB * 1024}; PRAGMA temp_store = MEMORY; PRAGMA foreign_keys = OFF;`);

const source = new NodeSqlite.DatabaseSync(shards[0], { readOnly: true });
const schema = source
  .prepare(
    `SELECT type, name, tbl_name AS tbl, sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid`,
  )
  .all();
source.close();
const virtual = schema
  .filter((row) => row.type === "table" && /^CREATE VIRTUAL TABLE/i.test(row.sql))
  .map((row) => row.name);
const isShadow = (name) => virtual.some((v) => name.startsWith(`${v}_`));
const tables = schema.filter((row) => row.type === "table" && !isShadow(row.name));
for (const row of tables) db.exec(row.sql);
for (const row of schema.filter((row) => row.type === "index")) db.exec(row.sql);
for (const row of schema.filter((row) => row.type === "view")) db.exec(row.sql);
for (const row of schema.filter((row) => row.type === "trigger")) db.exec(row.sql);
log(`pad events: ${padEvents}`);
log(
  `schema: ${tables.length} tables (${virtual.length} virtual), ${schema.filter((r) => r.type === "index").length} indexes, ${schema.filter((r) => r.type === "trigger").length} triggers`,
);

const derived = new Set(["orchestration_v2_projection_turn_item_counts", ...virtual]);
const fromShardZero = new Set([
  "effect_sql_migrations",
  "t3_perf_fork_migrations",
  "orchestration_v2_projection_metadata",
]);
// The search trigger reads the message's thread, so threads go first.
const first = ["orchestration_v2_projection_threads"];
const copyOrder = [
  ...first,
  ...tables.map((row) => row.name).filter((name) => !first.includes(name)),
].filter((name) => !derived.has(name));
const columns = (table) =>
  db
    .prepare(`SELECT name FROM pragma_table_info('${table}')`)
    .all()
    .map((row) => `"${row.name}"`);

for (const [index, shard] of shards.entries()) {
  db.exec(`ATTACH DATABASE 'file:${shard}?mode=ro' AS s`);
  const offset = Number(
    db.prepare(`SELECT COALESCE(MAX(sequence), 0) AS m FROM main.orchestration_events`).get().m,
  );
  db.exec("BEGIN");
  const counts = {};
  for (const table of copyOrder) {
    if (fromShardZero.has(table) && index > 0) continue;
    const exists = db
      .prepare(`SELECT 1 FROM s.sqlite_master WHERE type = 'table' AND name = ?`)
      .get(table);
    if (!exists) continue;
    const cols = columns(table);
    let select = cols.join(", ");
    let order = "";
    if (table === "orchestration_events") {
      select = cols
        .map((col) => {
          if (col === '"sequence"') return `sequence + ${offset}`;
          if (col === '"payload_json"') {
            const moved = `CASE WHEN event_type IN ('subagent.updated', 'turn-item.updated') AND json_type(payload_json, '$.promptSequence') = 'integer'
              THEN json_set(payload_json, '$.promptSequence', json_extract(payload_json, '$.promptSequence') + ${offset}) ELSE payload_json END`;
            return padEvents > 0
              ? `json_set(${moved}, '$.migbenchPad', hex(zeroblob(${Math.floor(padEvents / 4)} + abs(random()) % ${Math.max(1, Math.floor(padEvents / 2))})))`
              : moved;
          }
          return col;
        })
        .join(", ");
      order = " ORDER BY sequence";
    } else if (table.endsWith("command_receipts")) {
      select = cols
        .map((col) => (col === '"result_sequence"' ? `result_sequence + ${offset}` : col))
        .join(", ");
    }
    const result = db
      .prepare(
        `INSERT INTO main."${table}" (${cols.join(", ")}) SELECT ${select} FROM s."${table}"${order}`,
      )
      .run();
    if (result.changes > 0) counts[table] = Number(result.changes);
  }
  db.exec("COMMIT");
  db.exec("DETACH DATABASE s");
  log(`shard ${index} (${shard}): offset ${offset}, ${JSON.stringify(counts)}`);
  if (deleteShards)
    for (const suffix of ["", "-wal", "-shm"]) NodeFS.rmSync(`${shard}${suffix}`, { force: true });
}

const head = Number(db.prepare(`SELECT MAX(sequence) AS m FROM orchestration_events`).get().m);
db.prepare(
  `UPDATE orchestration_v2_projection_metadata SET last_sequence = ? WHERE projection_name = 'thread-projections'`,
).run(head);
db.prepare(
  `INSERT OR REPLACE INTO sqlite_sequence (name, seq) VALUES ('orchestration_events', ?)`,
).run(head);
if (pruneMarkAt !== "") {
  db.prepare(`INSERT OR REPLACE INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
    SELECT 'event-log-prune-mark', schema_version, ?, ? FROM orchestration_v2_projection_metadata WHERE projection_name = 'thread-projections'`).run(
    head,
    pruneMarkAt,
  );
}
log(`head sequence ${head}; prune mark ${pruneMarkAt || "none"}`);
if (freePages > 0) {
  db.exec(`CREATE TABLE migbench_pad (b BLOB)`);
  const insert = db.prepare(
    `INSERT INTO migbench_pad (b) SELECT zeroblob(4000) FROM (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?) SELECT i FROM n)`,
  );
  for (let left = freePages; left > 0; left -= 100_000) insert.run(Math.min(100_000, left));
  db.exec(`DROP TABLE migbench_pad`);
  log(`free pages: ${db.prepare("PRAGMA freelist_count").get().freelist_count}`);
}
db.exec("PRAGMA journal_mode = WAL");
const pages = db.prepare("PRAGMA page_count").get().page_count;
const free = db.prepare("PRAGMA freelist_count").get().freelist_count;
db.close();
log(
  `MERGE-OK ${dest}: ${pages} pages (${((pages * 4096) / 2 ** 30).toFixed(2)} GiB), freelist ${free}`,
);

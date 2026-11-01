// @effect-diagnostics nodeBuiltinImport:off - streams rows through node:sqlite.
import type * as NodeSqlite from "node:sqlite";

import { type ContentCounts, type ContentMode, emptyCounts, rewriteContent } from "./content.ts";

/** At most this many rows, and this many bytes plus one row, are held per batch. */
export interface BatchLimits {
  readonly rows: number;
  readonly bytes: number;
}

export const BATCH_LIMITS: BatchLimits = { rows: 2_000, bytes: 64 * 1024 * 1024 };

/** One committed batch: its rows and bytes, and the table's progress. */
export interface BlankStep {
  readonly table: string;
  readonly rows: number;
  readonly bytes: number;
  readonly done: number;
  readonly total: number;
  readonly tableBytes: number;
}

export interface BlankResult extends ContentCounts {
  readonly byTable: Record<string, number>;
}

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const literal = (text: string) => `'${text.replaceAll("'", "''")}'`;

const sizeOf = (value: unknown) =>
  typeof value === "string"
    ? Buffer.byteLength(value, "utf8")
    : value instanceof Uint8Array
      ? value.length
      : 8;

const withoutRowid = (db: NodeSqlite.DatabaseSync, table: string) =>
  /\bWITHOUT\s+ROWID\b/i.test(
    (db.prepare(`SELECT sql FROM sqlite_schema WHERE name = ?`).get(table) as { sql: string }).sql,
  );

const SMALLEST_ROWID = -(2n ** 63n);

/**
 * Rewrites every string and BLOB of `tables` (blank: allowlist and filler; redact: patterns only),
 * streaming rows in rowid order in batches bounded by `limits`, each committed before its step is
 * yielded. Not one UPDATE with a function: node:sqlite keeps every value it sees until the
 * statement ends. WITHOUT ROWID tables (pull request tables, emptied before) take one statement.
 */
export const blankTables = function* (
  db: NodeSqlite.DatabaseSync,
  tables: ReadonlyArray<string>,
  mode: ContentMode,
  limits: BatchLimits = BATCH_LIMITS,
): Generator<BlankStep, BlankResult> {
  const counts = emptyCounts();
  const byTable: Record<string, number> = {};
  const rewrite = (value: unknown, column: string): unknown => {
    if (typeof value === "string") return rewriteContent(value, column, mode, counts);
    if (value instanceof Uint8Array && mode === "blank" && value.some((byte) => byte !== 0)) {
      counts.changed++;
      return new Uint8Array(value.length);
    }
    return value;
  };
  db.function(
    "fixture_content",
    { deterministic: true, useBigIntArguments: true },
    (value, column) => rewrite(value, String(column)) as NodeSqlite.SQLOutputValue,
  );
  const triggers = db
    .prepare(`SELECT name, sql FROM sqlite_schema WHERE type = 'trigger' AND sql IS NOT NULL`)
    .all() as Array<{ name: string; sql: string }>;
  db.exec("BEGIN");
  for (const trigger of triggers) db.exec(`DROP TRIGGER ${quote(trigger.name)}`);
  db.exec("COMMIT");
  for (const table of tables) {
    const columns = (
      db.prepare(`SELECT name FROM pragma_table_info(?)`).all(table) as Array<{ name: string }>
    ).map((column) => column.name);
    if (columns.length === 0) continue;
    const before = counts.changed;
    const total = Number(
      (db.prepare(`SELECT COUNT(*) AS n FROM ${quote(table)}`).get() as { n: number }).n,
    );
    if (withoutRowid(db, table)) {
      db.exec("BEGIN");
      db.prepare(
        `UPDATE ${quote(table)} SET ${columns
          .map(
            (column) => `${quote(column)} = fixture_content(${quote(column)}, ${literal(column)})`,
          )
          .join(", ")}`,
      ).run();
      db.exec("COMMIT");
      yield { table, rows: total, bytes: 0, done: total, total, tableBytes: 0 };
    } else {
      const select = db.prepare(
        `SELECT rowid, ${columns.map(quote).join(", ")} FROM ${quote(table)}
         WHERE rowid > ? ORDER BY rowid LIMIT ?`,
      );
      select.setReturnArrays(true);
      select.setReadBigInts(true);
      const update = db.prepare(
        `UPDATE ${quote(table)} SET ${columns.map((column) => `${quote(column)} = ?`).join(", ")}
         WHERE rowid = ?`,
      );
      let after = SMALLEST_ROWID;
      let done = 0;
      let tableBytes = 0;
      for (;;) {
        const batch: Array<Array<unknown> | undefined> = [];
        let bytes = 0;
        for (const row of select.iterate(after, limits.rows) as Iterable<Array<unknown>>) {
          batch.push(row);
          for (let index = 1; index < row.length; index++) bytes += sizeOf(row[index]);
          if (bytes >= limits.bytes) break;
        }
        if (batch.length === 0) break;
        db.exec("BEGIN");
        for (let index = 0; index < batch.length; index++) {
          const row = batch[index] as Array<unknown>;
          batch[index] = undefined;
          const [rowid, ...values] = row;
          let changed = false;
          const next = values.map((value, column) => {
            const rewritten = rewrite(value, columns[column] as string);
            if (rewritten !== value) changed = true;
            return rewritten;
          });
          if (changed) update.run(...(next as Array<NodeSqlite.SQLInputValue>), rowid as bigint);
          after = rowid as bigint;
        }
        db.exec("COMMIT");
        done += batch.length;
        tableBytes += bytes;
        yield { table, rows: batch.length, bytes, done, total, tableBytes };
      }
    }
    if (counts.changed > before) byTable[table] = counts.changed - before;
  }
  db.exec("BEGIN");
  for (const trigger of triggers) db.exec(trigger.sql);
  db.exec("COMMIT");
  return { ...counts, byTable };
};

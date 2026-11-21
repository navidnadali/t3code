import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { eventRetentionSql } from "../orchestration-v2/EventRetention.ts";
import { rebuildMessageSearch } from "../orchestration-v2/messageSearchIndex.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ServerOwnership from "../serverOwnership.ts";
import { lockExclusively } from "./DatabaseMaintenance.ts";
import {
  EVENT_LOG_PRUNE_CURSOR,
  EVENT_LOG_PRUNE_MARK,
  EVENT_LOG_PRUNED_THROUGH,
  EVENT_LOG_REFERENCE_SCAN,
  EVENT_LOG_REFERENCE_SCAN_FROM,
} from "./OrchestrationEventStore.ts";
import { TABLE as PRUNE_STATE } from "./PerfForkMigrations/EventLogPruneState.ts";

/** The copy is built beside the database; after the swap the original keeps the second name. */
export const COPY_SUFFIX = ".filtered-copy";
export const PRE_SUFFIX = ".pre-filtered-copy";
/** Where a restore moves the copy it replaces, so a restore deletes nothing. */
export const REJECTED_SUFFIX = ".filtered-copy-rejected";

const RETAINED_EVENT_HISTORY = Duration.days(1);
const EVENTS = "orchestration_events";
const METADATA = "orchestration_v2_projection_metadata";
// The copy writes these metadata rows; every other row is copied as it is.
const PROGRESS = "event-log-%";
// Random inserts into the event id index stay in memory: about 1 GiB for a large event log.
const COPY_CACHE_KIB = 2 * 1024 * 1024;
const KEPT_CLASSES = ["above", "kept", "anchor", "head"] as const;
type KeptClass = (typeof KEPT_CLASSES)[number];
type ClassCounts = Record<
  KeptClass | "pruned",
  { readonly rows: number; readonly sequenceSum: number }
>;

export class FilteredCopyError extends Schema.TaggedError<FilteredCopyError>()(
  "FilteredCopyError",
  { step: Schema.String, reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {
  override get message() {
    return `Filtered copy, ${this.step}: ${this.reason}`;
  }
}

export interface FilteredCopyResult {
  /** Events at or below it that the rule does not keep are absent from the copy. */
  readonly boundary: number;
  readonly mark: number;
  readonly verified: true;
  readonly swapped: boolean;
  readonly integrity: "quick" | "full";
  /** The source's events by class; the copy holds all but `pruned`. */
  readonly classes: ClassCounts;
  readonly phases: {
    readonly copyMs: number;
    readonly indexMs: number;
    readonly searchMs: number;
    readonly verifyMs: number;
  };
  readonly bytes: {
    readonly source: number;
    readonly copy: number;
    /** Linux only: what this process read from and wrote to storage meanwhile. */
    readonly read?: number;
    readonly written?: number;
  };
}

// The source is attached read-only, so no statement can write to it.
const readOnlyUri = (file: string) =>
  `file:${file.replaceAll("%", "%25").replaceAll("?", "%3f").replaceAll("#", "%23")}?mode=ro`;

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

// The rule's classes over `${schema}.orchestration_events AS event`, in the order they are tested.
const classClauses = (schema: "main" | "src", boundary: number) => {
  const rule = eventRetentionSql(`${schema}.${EVENTS}`);
  const clauses: Record<KeptClass, string> = {
    above: `event.sequence > ${boundary}`,
    kept: `(${rule.keptForGood}) = 1`,
    anchor: rule.isPromptAnchor,
    head: rule.isStreamHead,
  };
  return {
    clauses,
    classify: `CASE ${KEPT_CLASSES.map((name) => `WHEN ${clauses[name]} THEN '${name}'`).join(" ")} ELSE 'pruned' END`,
  };
};

// One pass in rowid order: the subquery runs as a co-routine (LIMIT -1 keeps it from being flattened,
// which would evaluate the CASE once per aggregate), so nothing is sorted or written to temp files.
const countClasses = (sql: SqlClient.SqlClient, schema: "main" | "src", boundary: number) => {
  const names = [...KEPT_CLASSES, "pruned" as const];
  return Effect.map(
    sql.unsafe<Record<string, number | null>>(
      `SELECT ${names
        .map(
          (name) =>
            `count(*) FILTER (WHERE class = '${name}') AS "${name}_rows", ` +
            `sum(sequence) FILTER (WHERE class = '${name}') AS "${name}_sum"`,
        )
        .join(", ")}
       FROM (
         SELECT event.sequence AS sequence, ${classClauses(schema, boundary).classify} AS class
         FROM ${schema}.${EVENTS} AS event LIMIT -1
       )`,
    ),
    ([row]) =>
      Object.fromEntries(
        names.map((name) => [
          name,
          { rows: row?.[`${name}_rows`] ?? 0, sequenceSum: row?.[`${name}_sum`] ?? 0 },
        ]),
      ) as ClassCounts,
  );
};

type SchemaObject = {
  readonly type: string;
  readonly name: string;
  readonly tbl_name: string;
  readonly sql: string;
};

const isVirtual = (object: { readonly type: string; readonly sql: string }) =>
  object.type === "table" && /^\s*CREATE\s+VIRTUAL\s+TABLE/i.test(object.sql);

const failAt = (step: string, reason: string, cause?: unknown) =>
  Effect.fail(new FilteredCopyError({ step, reason, ...(cause === undefined ? {} : { cause }) }));

const walBytes = (fs: FileSystem.FileSystem, databasePath: string) =>
  fs.stat(databasePath + "-wal").pipe(
    Effect.map((info) => Number(info.size)),
    Effect.orElseSucceed(() => 0),
  );

// The server's ownership lock on the database's home, held until the caller's scope closes: a
// server, or a CLI that migrates, cannot start meanwhile.
const holdServerHome = (databasePath: string, step: string) =>
  Effect.flatMap(Path.Path, (path) =>
    ServerOwnership.holdServerOwnershipLock(
      path.join(path.dirname(databasePath), "server-runtime.json"),
    ),
  ).pipe(
    Effect.catchTags({
      ServerAlreadyRunningError: () =>
        failAt(
          step,
          "a T3 Code server owns this database's home, or another copy or restore runs: stop it first",
        ),
      ServerOwnershipError: (cause) => failAt(step, cause.message, cause),
    }),
  );

// Runs `use` while this process holds SQLite's exclusive lock on the file, so no other connection
// has it open. A log with frames is refused before opening: opening would recover it into the file.
const whileExclusive = Effect.fn("EventLogFilteredCopy.whileExclusive")(
  function* (
    fs: FileSystem.FileSystem,
    databasePath: string,
    step: string,
    use: Effect.Effect<void, FilteredCopyError>,
  ) {
    if ((yield* walBytes(fs, databasePath)) > 0) {
      return yield* failAt(
        step,
        "the database's write-ahead log holds frames: stop the server cleanly first",
      );
    }
    yield* lockExclusively(databasePath).pipe(
      Effect.catchTags({
        DatabaseInUseError: () =>
          failAt(step, "another process has the database open: stop the T3 Code server first"),
      }),
    );
    return yield* use;
  },
  (effect, _fs, databasePath, step) =>
    effect.pipe(
      Effect.provide(NodeSqliteClient.layer({ filename: databasePath })),
      Effect.catchTags({ SqlError: (cause) => failAt(step, cause.message, cause) }),
    ),
);

// Renames the database aside once `check` passes, under the exclusive handle except on Windows,
// which cannot rename an open file; there a lock-free CLI writer could commit in the gap. So the moved
// file and the log left behind are checked again, and a changed one is renamed back.
const moveAside = Effect.fn("EventLogFilteredCopy.moveAside")(function* (
  fs: FileSystem.FileSystem,
  databasePath: string,
  target: string,
  step: string,
  check: (file: string) => Effect.Effect<void, FilteredCopyError>,
) {
  const toError = (cause: { readonly message: string }) =>
    new FilteredCopyError({ step, reason: cause.message, cause });
  const rename = fs.rename(databasePath, target).pipe(Effect.mapError(toError));
  if ((yield* HostProcess.Platform) === "win32") {
    yield* whileExclusive(fs, databasePath, step, check(databasePath));
    yield* rename;
  } else {
    yield* whileExclusive(fs, databasePath, step, check(databasePath).pipe(Effect.andThen(rename)));
  }
  const unchanged = Effect.gen(function* () {
    if ((yield* walBytes(fs, databasePath)) > 0) {
      return yield* failAt(step, "a commit reached the write-ahead log during the rename");
    }
    yield* check(target);
  });
  yield* unchanged.pipe(
    Effect.catchTags({
      FilteredCopyError: (error) =>
        fs
          .rename(target, databasePath)
          .pipe(
            Effect.mapError(toError),
            Effect.andThen(
              failAt(step, `${error.reason}; the rename was undone and nothing was swapped`),
            ),
          ),
    }),
  );
});

// The copy is written with synchronous=OFF, so it is flushed before it replaces the original, and the
// directory after each swap or restore, so a power loss leaves one whole database under each name.
// Windows cannot open a directory to flush it, and flushes a file only through a handle it may
// write: a file opens r+ (never truncated), a directory read-only.
const flushToDisk = Effect.fn("EventLogFilteredCopy.flushToDisk")(function* (
  fs: FileSystem.FileSystem,
  file: string,
  step: string,
  options?: { readonly directory?: boolean },
) {
  if (options?.directory === true && (yield* HostProcess.Platform) === "win32") return;
  yield* fs.open(file, { flag: options?.directory === true ? "r" : "r+" }).pipe(
    Effect.flatMap((handle) => handle.sync),
    Effect.scoped,
    Effect.mapError((cause) => new FilteredCopyError({ step, reason: cause.message, cause })),
  );
});

/**
 * Copies `databasePath` (its server stopped) to `<databasePath>.filtered-copy` with every table, keeping of
 * the events exactly what the online compaction keeps through the boundary due now; checks the copy
 * against the source; with `swap`, renames the original to `.pre-filtered-copy` and the copy into its place.
 * The source is only read, so a failure or interruption at any step leaves it as it was.
 */
export const compactByFilteredCopy = Effect.fn("EventLogFilteredCopy.compact")(function* (
  databasePath: string,
  options: {
    readonly swap: boolean;
    /** `quick` (default) checks every page; `full` also checks each index against its table. */
    readonly integrity?: "quick" | "full";
    /** @internal Narrows the filter by a predicate over the class names, to prove the check catches it. */
    readonly brokenFilterForTest?: string;
  },
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const copyPath = databasePath + COPY_SUFFIX;
  const prePath = databasePath + PRE_SUFFIX;
  const integrity = options.integrity ?? "quick";
  const fail = failAt;
  const exists = (file: string) => fs.exists(file).pipe(Effect.orElseSucceed(() => false));
  const size = (file: string) =>
    fs.stat(file).pipe(
      Effect.map((info) => Number(info.size)),
      Effect.orElseSucceed(() => 0),
    );
  const fingerprintOf = (file: string) =>
    fs.stat(file).pipe(
      Effect.map((info) =>
        [
          String(info.size),
          String(Option.getOrUndefined(info.mtime)?.getTime()),
          String(Option.getOrUndefined(info.ino)),
        ].join(":"),
      ),
      Effect.orElseSucceed(() => "missing"),
    );
  const processIo = fs.readFileString("/proc/self/io").pipe(
    Effect.map((text) => {
      const field = (name: string) => Number(new RegExp(`^${name}: (\\d+)$`, "m").exec(text)?.[1]);
      return { read: field("read_bytes"), written: field("write_bytes") };
    }),
    Effect.option,
  );
  const removeFiles = (file: string, suffixes: ReadonlyArray<string>) =>
    Effect.forEach(suffixes, (suffix) => fs.remove(file + suffix, { force: true }), {
      discard: true,
    });

  // Held to the end: no server, other copy or restore starts while the copy is read, checked and
  // swapped in, and every file check below sees the others' renames.
  yield* holdServerHome(databasePath, "precondition");
  if (!(yield* exists(databasePath))) {
    return yield* fail("precondition", `${databasePath} does not exist`);
  }
  yield* whileExclusive(fs, databasePath, "precondition", Effect.void);
  if (options.swap && (yield* exists(prePath))) {
    return yield* fail(
      "precondition",
      `${path.basename(prePath)} from an earlier swap is still there: remove it once the database is verified, or restore it`,
    );
  }
  const before = yield* fingerprintOf(databasePath);
  yield* removeFiles(copyPath, ["", "-wal", "-shm", "-journal"]).pipe(Effect.orDie);
  const ioBefore = yield* processIo;
  const now = yield* DateTime.now;

  const built = yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    // A new file: no rollback journal (a failed copy is thrown away), the server's page size.
    yield* sql.unsafe("PRAGMA page_size = 4096");
    yield* sql.unsafe("PRAGMA auto_vacuum = INCREMENTAL");
    yield* sql.unsafe("PRAGMA journal_mode = OFF");
    yield* sql.unsafe("PRAGMA synchronous = OFF");
    yield* sql.unsafe(`PRAGMA cache_size = -${COPY_CACHE_KIB}`);
    yield* sql.unsafe("ATTACH DATABASE ? AS src", [readOnlyUri(databasePath)]);

    const state = yield* sql<{ readonly name: string }>`
      SELECT name FROM src.sqlite_master WHERE type = 'table' AND name = ${PRUNE_STATE}
    `;
    if (state.length === 0) {
      return yield* fail(
        "precondition",
        "start this release on the database once first: it creates the compaction's state table",
      );
    }

    // The boundary the online pass would publish now: the mark once a day old, capped by the projections.
    const metadata = new Map(
      (yield* sql<{
        readonly projection_name: string;
        readonly last_sequence: number;
        readonly updated_at: string;
      }>`
        SELECT projection_name, last_sequence, updated_at FROM src.orchestration_v2_projection_metadata
      `).map((row) => [row.projection_name, row]),
    );
    const [headRow] = yield* sql<{ readonly sequence: number | null }>`
      SELECT MAX(sequence) AS sequence FROM src.orchestration_events
    `;
    const head = headRow?.sequence ?? 0;
    const projected = metadata.get("thread-projections")?.last_sequence ?? 0;
    const mark = metadata.get(EVENT_LOG_PRUNE_MARK);
    const prunedThrough = metadata.get(EVENT_LOG_PRUNED_THROUGH)?.last_sequence ?? 0;
    const markIsDue =
      mark !== undefined &&
      DateTime.toEpochMillis(now) - Date.parse(mark.updated_at) >=
        Duration.toMillis(RETAINED_EVENT_HISTORY);
    const boundary = markIsDue
      ? Math.max(Math.min(mark.last_sequence, projected), prunedThrough)
      : prunedThrough;
    // `now` was read before the head, and nothing writes meanwhile: (head, now) is a valid mark.
    const nextMark = mark === undefined || markIsDue ? head : mark.last_sequence;
    if (boundary === 0) {
      return yield* fail(
        "precondition",
        "no boundary is due yet, so nothing can be pruned: run this build for a day first",
      );
    }

    const objects = yield* sql<SchemaObject>`
      SELECT type, name, tbl_name, sql FROM src.sqlite_master
      WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid
    `;
    const virtual = objects.filter(isVirtual);
    // A virtual table creates its own shadow tables.
    const isShadow = (name: string) => virtual.some((table) => name.startsWith(`${table.name}_`));
    const tables = objects.filter(
      (object) => object.type === "table" && !isVirtual(object) && !isShadow(object.name),
    );

    const copyStarted = yield* Clock.currentTimeMillis;
    for (const object of [...tables, ...virtual]) yield* sql.unsafe(object.sql);
    const { clauses } = classClauses("src", boundary);
    const keep = KEPT_CLASSES.map((name) => `(${clauses[name]})`).join(" OR ");
    const filter =
      options.brokenFilterForTest === undefined
        ? keep
        : `(${keep}) AND (${options.brokenFilterForTest.replace(
            /\b(above|kept|anchor|head)\b/g,
            (name) => `(${clauses[name as KeptClass]})`,
          )})`;
    for (const table of tables) {
      // The online pass's progress is rebuilt below for the copy's boundary.
      if (table.name === PRUNE_STATE) continue;
      const columns = yield* sql<{
        readonly name: string;
        readonly type: string;
        readonly pk: number;
        readonly hidden: number;
      }>`SELECT name, type, pk, hidden FROM pragma_table_xinfo(${table.name}, 'src')`;
      const keys = columns.filter((column) => column.pk > 0);
      const withoutRowid = /\bWITHOUT\s+ROWID\b/i.test(table.sql);
      // Rowids are kept explicitly where no INTEGER PRIMARY KEY holds them: the search index and
      // receipts' scans refer to them. Generated columns are computed again.
      const rowidIsAColumn =
        withoutRowid || (keys.length === 1 && keys[0]!.type.toUpperCase() === "INTEGER");
      const names = [
        ...(rowidIsAColumn ? [] : ["rowid"]),
        ...columns.filter((column) => column.hidden === 0).map((column) => quote(column.name)),
      ].join(", ");
      yield* sql.unsafe(
        table.name === EVENTS
          ? `INSERT INTO main.${EVENTS} (${names}) SELECT ${names} FROM src.${EVENTS} AS event WHERE ${filter}`
          : `INSERT INTO main.${quote(table.name)} (${names}) SELECT ${names} FROM src.${quote(table.name)}`,
      );
    }
    // AUTOINCREMENT never hands out a sequence the source used, deleted rows' included.
    yield* sql.unsafe("DELETE FROM main.sqlite_sequence");
    yield* sql.unsafe(
      "INSERT INTO main.sqlite_sequence (name, seq) SELECT name, seq FROM src.sqlite_sequence",
    );
    const [userVersion] = yield* sql<{ readonly user_version: number }>`PRAGMA src.user_version`;
    yield* sql.unsafe(`PRAGMA main.user_version = ${Number(userVersion?.user_version ?? 0)}`);

    // The online pass continues from the boundary: every row at or below it is classified, its
    // heads and prompt anchors are held for the pass to resolve, and the reference scan starts there.
    const copied = eventRetentionSql(`main.${EVENTS}`);
    yield* sql.unsafe(
      `INSERT INTO main.${PRUNE_STATE} (sequence, held, prompt_sequence)
       SELECT event.sequence, 1, ${copied.borrowedPrompt}
       FROM main.${EVENTS} AS event
       WHERE event.sequence <= ${boundary} AND (${copied.keptForGood}) = 0`,
    );
    const writeMetadata = (name: string, sequence: number) => sql`
      INSERT INTO main.orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
      VALUES (${name}, ${ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION}, ${sequence}, ${DateTime.formatIso(now)})
      ON CONFLICT(projection_name) DO UPDATE SET
        last_sequence = excluded.last_sequence, updated_at = excluded.updated_at
    `;
    if (boundary > prunedThrough) yield* writeMetadata(EVENT_LOG_PRUNED_THROUGH, boundary);
    if (mark === undefined || markIsDue) yield* writeMetadata(EVENT_LOG_PRUNE_MARK, nextMark);
    for (const name of [
      EVENT_LOG_PRUNE_CURSOR,
      EVENT_LOG_REFERENCE_SCAN,
      EVENT_LOG_REFERENCE_SCAN_FROM,
    ]) {
      yield* writeMetadata(name, boundary);
    }
    const copyMs = (yield* Clock.currentTimeMillis) - copyStarted;

    // Indexes after the rows: one sorted build each instead of an insert per row.
    const indexStarted = yield* Clock.currentTimeMillis;
    for (const object of objects) {
      if (object.type === "table" || isShadow(object.tbl_name)) continue;
      yield* sql.unsafe(object.sql);
    }
    const stats = yield* sql`SELECT 1 FROM src.sqlite_master WHERE name = 'sqlite_stat1'`;
    if (stats.length > 0) {
      yield* sql.unsafe("ANALYZE main.sqlite_schema");
      yield* sql.unsafe("DELETE FROM main.sqlite_stat1");
      yield* sql.unsafe("INSERT INTO main.sqlite_stat1 SELECT * FROM src.sqlite_stat1");
    }
    const indexMs = (yield* Clock.currentTimeMillis) - indexStarted;
    yield* sql.unsafe("DETACH DATABASE src");
    const searchStarted = yield* Clock.currentTimeMillis;
    yield* rebuildMessageSearch(sql);
    const searchMs = (yield* Clock.currentTimeMillis) - searchStarted;
    yield* sql.unsafe("PRAGMA journal_mode = WAL");
    return { boundary, mark: nextMark, copyMs, indexMs, searchMs };
  }).pipe(
    Effect.provide(NodeSqliteClient.layer({ filename: copyPath })),
    Effect.catchTags({ SqlError: (cause) => fail("copy", cause.message, cause) }),
    // A partial copy can be as large as the result; a failed or interrupted copy gives its space back.
    Effect.onError(() =>
      removeFiles(copyPath, ["", "-wal", "-shm", "-journal"]).pipe(Effect.ignore),
    ),
  );

  // The copy against the source, both read only, before anything moves.
  const verifyStarted = yield* Clock.currentTimeMillis;
  const verified = yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe("ATTACH DATABASE ? AS src", [readOnlyUri(databasePath)]);
    const problems: Array<string> = [];
    const checks = yield* sql.unsafe<Record<string, unknown>>(
      integrity === "full" ? "PRAGMA main.integrity_check" : "PRAGMA main.quick_check",
    );
    const verdict = checks.map((row) => String(Object.values(row)[0])).join("; ");
    if (verdict !== "ok") problems.push(`${integrity}_check: ${verdict.slice(0, 500)}`);

    const source = yield* countClasses(sql, "src", built.boundary);
    const copy = yield* countClasses(sql, "main", built.boundary);
    for (const name of KEPT_CLASSES) {
      if (
        source[name].rows !== copy[name].rows ||
        source[name].sequenceSum !== copy[name].sequenceSum
      ) {
        problems.push(
          `${name} events: source ${source[name].rows} rows, copy ${copy[name].rows} rows`,
        );
      }
    }
    if (copy.pruned.rows !== 0) problems.push(`the copy kept ${copy.pruned.rows} pruned events`);

    // Every other table row for row; the metadata the copy writes is checked below.
    const tables = yield* sql<{
      readonly type: string;
      readonly name: string;
      readonly sql: string;
    }>`
      SELECT type, name, sql FROM src.sqlite_master
      WHERE type = 'table' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
    `;
    const virtual = tables.filter(isVirtual).map((table) => table.name);
    for (const { name } of tables) {
      if (name === EVENTS || name === PRUNE_STATE || virtual.includes(name)) continue;
      if (virtual.some((table) => name.startsWith(`${table}_`))) continue;
      // Counted in place: a count over a subquery would copy each table into temp files.
      const where = name === METADATA ? ` WHERE projection_name NOT LIKE '${PROGRESS}'` : "";
      const [counts] = yield* sql.unsafe<{
        readonly source: number;
        readonly copy: number;
        readonly missing: number;
      }>(
        `SELECT (SELECT count(*) FROM src.${quote(name)}${where}) AS source,
                (SELECT count(*) FROM main.${quote(name)}${where}) AS copy,
                ${
                  name === METADATA
                    ? `(SELECT count(*) FROM (SELECT * FROM src.${METADATA}${where} EXCEPT SELECT * FROM main.${METADATA}${where}))`
                    : "0"
                } AS missing`,
      );
      if (counts?.source !== counts?.copy || counts?.missing !== 0) {
        problems.push(`${name}: source ${counts?.source} rows, copy ${counts?.copy} rows`);
      }
    }
    const [sequence] = yield* sql<{ readonly source: number | null; readonly copy: number | null }>`
      SELECT (SELECT seq FROM src.sqlite_sequence WHERE name = ${EVENTS}) AS source,
             (SELECT seq FROM main.sqlite_sequence WHERE name = ${EVENTS}) AS copy
    `;
    if (sequence?.source !== sequence?.copy) {
      problems.push(`sqlite_sequence: source ${sequence?.source}, copy ${sequence?.copy}`);
    }

    // The boundary rule: published as the boundary, and no projection behind it.
    const metadata = yield* sql<{
      readonly projection_name: string;
      readonly last_sequence: number;
    }>`SELECT projection_name, last_sequence FROM main.orchestration_v2_projection_metadata`;
    const progress = new Map(metadata.map((row) => [row.projection_name, row.last_sequence]));
    for (const name of [
      EVENT_LOG_PRUNED_THROUGH,
      EVENT_LOG_PRUNE_CURSOR,
      EVENT_LOG_REFERENCE_SCAN,
      EVENT_LOG_REFERENCE_SCAN_FROM,
    ]) {
      if ((progress.get(name) ?? 0) !== built.boundary) {
        problems.push(`${name} ${progress.get(name)} is not the boundary ${built.boundary}`);
      }
    }
    for (const row of metadata) {
      if (row.projection_name.startsWith("event-log-")) continue;
      if (row.last_sequence < built.boundary) {
        problems.push(`${row.projection_name} at ${row.last_sequence} is behind the boundary`);
      }
    }
    const [held] = yield* sql.unsafe<{ readonly rows: number; readonly sequence_sum: number }>(
      `SELECT count(*) AS rows, COALESCE(sum(sequence), 0) AS sequence_sum
       FROM main.${PRUNE_STATE} WHERE held = 1`,
    );
    if (
      held?.rows !== copy.head.rows + copy.anchor.rows ||
      held?.sequence_sum !== copy.head.sequenceSum + copy.anchor.sequenceSum
    ) {
      problems.push(
        `held rows (${held?.rows}) are not the heads and anchors at or below the boundary`,
      );
    }
    yield* sql.unsafe("DETACH DATABASE src");
    return { problems, classes: source };
  }).pipe(
    Effect.provide(NodeSqliteClient.layer({ filename: copyPath, readonly: true })),
    Effect.catchTags({ SqlError: (cause) => fail("verify", cause.message, cause) }),
  );
  const verifyMs = (yield* Clock.currentTimeMillis) - verifyStarted;
  if (verified.problems.length > 0) return yield* fail("verify", verified.problems.join("; "));

  // The swap, only for the file the copy was read from: the original keeps its bytes under a second name.
  if (options.swap) {
    yield* flushToDisk(fs, copyPath, "swap");
    // The original moves aside while no other connection can have it open (moveAside), so no commit
    // lands in a log the swap leaves behind. Closing that connection removes the original's empty log.
    yield* moveAside(fs, databasePath, prePath, "swap", (file) =>
      Effect.flatMap(fingerprintOf(file), (now) =>
        now === before
          ? Effect.void
          : fail("swap", "the database changed after it was copied: run the copy again"),
      ),
    );
    yield* Effect.gen(function* () {
      yield* removeFiles(databasePath, ["-wal", "-shm"]);
      yield* removeFiles(copyPath, ["-wal", "-shm"]);
      yield* fs.rename(copyPath, databasePath);
    }).pipe(
      Effect.mapError(
        (cause) =>
          new FilteredCopyError({
            step: "swap",
            reason: `${cause.message}; the original is ${path.basename(prePath)}: restore it`,
            cause,
          }),
      ),
    );
    yield* flushToDisk(fs, path.dirname(databasePath), "swap", { directory: true });
  }
  const ioAfter = yield* processIo;
  const io =
    Option.isSome(ioBefore) && Option.isSome(ioAfter)
      ? {
          read: ioAfter.value.read - ioBefore.value.read,
          written: ioAfter.value.written - ioBefore.value.written,
        }
      : {};
  return {
    boundary: built.boundary,
    mark: built.mark,
    verified: true,
    swapped: options.swap,
    integrity,
    classes: verified.classes,
    phases: { copyMs: built.copyMs, indexMs: built.indexMs, searchMs: built.searchMs, verifyMs },
    bytes: {
      source: yield* size(options.swap ? prePath : databasePath),
      copy: yield* size(options.swap ? databasePath : copyPath),
      ...io,
    },
  } satisfies FilteredCopyResult;
}, Effect.scoped);

/**
 * Puts the original back after a swap, or after a swap cut between its two renames. The copy it
 * replaces is kept as `.filtered-copy-rejected`; a restore deletes nothing and refuses while one is there.
 */
export const restoreBeforeFilteredCopy = Effect.fn("EventLogFilteredCopy.restore")(function* (
  databasePath: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const prePath = databasePath + PRE_SUFFIX;
  const rejectedPath = databasePath + REJECTED_SUFFIX;
  const exists = (file: string) => fs.exists(file).pipe(Effect.orElseSucceed(() => false));
  const toError = (cause: { readonly message: string }) =>
    new FilteredCopyError({ step: "restore", reason: cause.message, cause });
  // Every check and rename below runs under the lock, so a concurrent restore sees their result.
  yield* holdServerHome(databasePath, "restore");
  if (!(yield* exists(prePath))) {
    return yield* failAt("restore", `there is no ${path.basename(prePath)} to restore`);
  }
  for (const suffix of ["", "-wal", "-shm"]) {
    if (yield* exists(rejectedPath + suffix)) {
      return yield* failAt(
        "restore",
        `${path.basename(rejectedPath + suffix)} from an earlier restore is still there: move it away first`,
      );
    }
  }
  if (yield* exists(databasePath)) {
    yield* moveAside(fs, databasePath, rejectedPath, "restore", () => Effect.void);
  } else if ((yield* walBytes(fs, databasePath)) > 0) {
    return yield* failAt("restore", "a write-ahead log with frames has no database beside it");
  }
  yield* Effect.gen(function* () {
    yield* fs.remove(databasePath + "-wal", { force: true });
    yield* fs.remove(databasePath + "-shm", { force: true });
    yield* fs.rename(prePath, databasePath);
  }).pipe(Effect.mapError(toError));
  yield* flushToDisk(fs, path.dirname(databasePath), "restore", { directory: true });
}, Effect.scoped);

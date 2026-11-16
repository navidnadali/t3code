// @effect-diagnostics nodeBuiltinImport:off - measures a database through node:sqlite, read-only.
import type * as NodeSqlite from "node:sqlite";

/** `[q, value]` pairs, q ascending in [0, 1]; q = 1 is the maximum. */
export type Quantiles = ReadonlyArray<readonly [number, number]>;

export interface SizeMix {
  readonly rows: number;
  readonly meanBytes: number;
}

export interface TableShape {
  readonly rows: number;
  /** Pages of the table and its indexes times the page size (`dbstat`). */
  readonly bytes?: number;
  readonly payload?: {
    readonly column: string;
    readonly mean: number;
    readonly quantiles: Quantiles;
  };
}

export interface Shape {
  readonly format: typeof SHAPE_FORMAT;
  readonly version: 1;
  readonly source?: { readonly label?: string; readonly capturedAt?: string };
  readonly pageSize: number;
  readonly pageCount: number;
  readonly tables: Readonly<Record<string, TableShape>>;
  readonly stat1?: ReadonlyArray<{
    readonly tbl: string;
    readonly idx: string | null;
    readonly stat: string;
  }>;
  readonly distributions?: {
    readonly threadsPerProject?: Quantiles;
    readonly turnItemsPerThread?: Quantiles;
  };
  /** Threads by relationship; `v1Import` counts the roots imported from v1, held as run-less history. */
  readonly lineage?: {
    readonly root: number;
    readonly fork: number;
    readonly subagent: number;
    readonly v1Import?: number;
    /** Threads with turn items but no run: v1 imports and children fed by a parent's stream. */
    readonly runless?: number;
  };
  readonly mix?: {
    readonly turnItemTypes?: Readonly<Record<string, SizeMix>>;
    readonly eventTypes?: Readonly<Record<string, SizeMix>>;
    /** `orchestration_events` by `v<application_event_version>/<aggregate_kind>`. */
    readonly applicationEvents?: Readonly<Record<string, SizeMix>>;
  };
}

export const SHAPE_FORMAT = "t3-server-bench-shape";
export const QUANTILE_GRID = [
  0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.999, 1,
] as const;

export class ShapeError extends Error {}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const isCount = (value: unknown): value is number =>
  typeof value === "number" && Number.isInteger(value) && value >= 0;

const checkQuantiles = (value: unknown, where: string): Quantiles => {
  if (!Array.isArray(value) || value.length === 0) {
    throw new ShapeError(`${where}: expected a non-empty array of [q, value] pairs`);
  }
  let previous: readonly [number, number] | undefined;
  for (const pair of value) {
    if (
      !Array.isArray(pair) ||
      pair.length !== 2 ||
      typeof pair[0] !== "number" ||
      typeof pair[1] !== "number" ||
      pair[0] < 0 ||
      pair[0] > 1 ||
      pair[1] < 0
    ) {
      throw new ShapeError(`${where}: each entry must be [q in 0..1, value >= 0]`);
    }
    if (previous !== undefined && (pair[0] <= previous[0] || pair[1] < previous[1])) {
      throw new ShapeError(`${where}: q and value must both ascend`);
    }
    previous = [pair[0], pair[1]];
  }
  if (previous?.[0] !== 1)
    throw new ShapeError(`${where}: the last entry must be q = 1 (the maximum)`);
  return value as Quantiles;
};

const checkMix = (value: unknown, where: string) => {
  if (value === undefined) return;
  if (!isRecord(value)) throw new ShapeError(`${where}: expected an object`);
  for (const [key, entry] of Object.entries(value)) {
    if (!isRecord(entry) || !isCount(entry.rows) || typeof entry.meanBytes !== "number") {
      throw new ShapeError(`${where}.${key}: expected { rows, meanBytes }`);
    }
  }
};

/** Validates a parsed shape file; throws ShapeError naming the first bad field. */
export const parseShape = (value: unknown): Shape => {
  if (!isRecord(value)) throw new ShapeError("shape: expected a JSON object");
  if (value.format !== SHAPE_FORMAT || value.version !== 1) {
    throw new ShapeError(`shape: expected format "${SHAPE_FORMAT}" version 1`);
  }
  if (!isCount(value.pageSize) || value.pageSize === 0 || !isCount(value.pageCount)) {
    throw new ShapeError("shape: pageSize and pageCount must be positive integers");
  }
  if (!isRecord(value.tables)) throw new ShapeError("shape.tables: expected an object");
  for (const [name, table] of Object.entries(value.tables)) {
    if (!isRecord(table) || !isCount(table.rows)) {
      throw new ShapeError(`shape.tables.${name}.rows: expected a non-negative integer`);
    }
    if (table.bytes !== undefined && !isCount(table.bytes)) {
      throw new ShapeError(`shape.tables.${name}.bytes: expected a non-negative integer`);
    }
    if (table.payload !== undefined) {
      const payload = table.payload;
      if (
        !isRecord(payload) ||
        typeof payload.column !== "string" ||
        typeof payload.mean !== "number"
      ) {
        throw new ShapeError(`shape.tables.${name}.payload: expected { column, mean, quantiles }`);
      }
      checkQuantiles(payload.quantiles, `shape.tables.${name}.payload.quantiles`);
    }
  }
  const distributions = value.distributions;
  if (distributions !== undefined) {
    if (!isRecord(distributions)) throw new ShapeError("shape.distributions: expected an object");
    for (const [name, quantiles] of Object.entries(distributions)) {
      if (quantiles !== undefined) checkQuantiles(quantiles, `shape.distributions.${name}`);
    }
  }
  const lineage = value.lineage;
  if (
    lineage !== undefined &&
    (!isRecord(lineage) ||
      !isCount(lineage.root) ||
      !isCount(lineage.fork) ||
      !isCount(lineage.subagent) ||
      (lineage.v1Import !== undefined && !isCount(lineage.v1Import)) ||
      (lineage.runless !== undefined && !isCount(lineage.runless)))
  ) {
    throw new ShapeError(
      "shape.lineage: expected { root, fork, subagent, v1Import?, runless? } counts",
    );
  }
  if (value.mix !== undefined) {
    if (!isRecord(value.mix)) throw new ShapeError("shape.mix: expected an object");
    checkMix(value.mix.turnItemTypes, "shape.mix.turnItemTypes");
    checkMix(value.mix.eventTypes, "shape.mix.eventTypes");
    checkMix(value.mix.applicationEvents, "shape.mix.applicationEvents");
  }
  return value as unknown as Shape;
};

export const shapeRows = (shape: Shape, table: string): number => shape.tables[table]?.rows ?? 0;

/** Inverse CDF, linear between the recorded points. */
export const quantile = (quantiles: Quantiles, q: number): number => {
  const first = quantiles[0] as readonly [number, number];
  if (q <= first[0]) return first[1];
  for (let index = 1; index < quantiles.length; index++) {
    const [q1, v1] = quantiles[index] as readonly [number, number];
    if (q <= q1) {
      const [q0, v0] = quantiles[index - 1] as readonly [number, number];
      return q1 === q0 ? v1 : v0 + ((v1 - v0) * (q - q0)) / (q1 - q0);
    }
  }
  return (quantiles.at(-1) as readonly [number, number])[1];
};

/** Exact quantiles at QUANTILE_GRID of an ascending list. */
export const summarize = (ascending: ArrayLike<number>): Quantiles => {
  if (ascending.length === 0) return [[1, 0]];
  return QUANTILE_GRID.map((q) => {
    const index = Math.min(ascending.length - 1, Math.max(0, Math.ceil(q * ascending.length) - 1));
    return [q, ascending[index] as number] as const;
  });
};

/** An SQL identifier, quoted: table names come from the database, not from this code. */
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

const tableNames = (db: NodeSqlite.DatabaseSync) =>
  (
    db
      .prepare(
        `SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name`,
      )
      .all() as Array<{ name: string }>
  ).map((row) => row.name);

/** Ascending values of the query's `value` column, without one object per row held at once. */
const columnValues = (db: NodeSqlite.DatabaseSync, sql: string): Float64Array => {
  let values = new Float64Array(1024);
  let length = 0;
  for (const row of db.prepare(sql).iterate() as Iterable<{ value: number }>) {
    if (length === values.length) {
      const grown = new Float64Array(values.length * 2);
      grown.set(values);
      values = grown;
    }
    values[length++] = Number(row.value);
  }
  return values.slice(0, length).sort();
};

const sizeMix = (db: NodeSqlite.DatabaseSync, sql: string): Record<string, SizeMix> =>
  Object.fromEntries(
    (db.prepare(sql).all() as Array<{ key: string; rows: number; mean: number }>).map((row) => [
      row.key,
      { rows: Number(row.rows), meanBytes: Math.round(Number(row.mean) * 10) / 10 },
    ]),
  );

const PAYLOAD_COLUMNS = ["payload_json", "text", "body", "guide_json", "headers_json"];

/** Counts and size distributions only: a shape holds no row content. */
export const measureShape = (
  db: NodeSqlite.DatabaseSync,
  source: { readonly label?: string; readonly capturedAt?: string } = {},
): Shape => {
  const pragma = (name: string) =>
    Number((db.prepare(`PRAGMA ${name}`).get() as Record<string, number>)[name]);
  const owner = new Map(
    (
      db
        .prepare(`SELECT name, tbl_name FROM sqlite_schema WHERE type IN ('table', 'index')`)
        .all() as Array<{
        name: string;
        tbl_name: string;
      }>
    ).map((row) => [row.name, row.tbl_name]),
  );
  const bytesByTable = new Map<string, number>();
  for (const row of db
    .prepare(`SELECT name, SUM(pgsize) AS bytes FROM dbstat GROUP BY name`)
    .all() as Array<{
    name: string;
    bytes: number;
  }>) {
    const table = owner.get(row.name) ?? row.name;
    bytesByTable.set(table, (bytesByTable.get(table) ?? 0) + Number(row.bytes));
  }
  const virtualTables = new Set(
    (
      db
        .prepare(`SELECT name FROM sqlite_schema WHERE sql LIKE 'CREATE VIRTUAL TABLE%'`)
        .all() as Array<{
        name: string;
      }>
    ).map((row) => row.name),
  );
  const tables: Record<string, TableShape> = {};
  for (const name of tableNames(db)) {
    const rows = Number(
      (db.prepare(`SELECT COUNT(*) AS n FROM ${quote(name)}`).get() as { n: number }).n,
    );
    const columns = (
      db.prepare(`SELECT name FROM pragma_table_info(?)`).all(name) as Array<{ name: string }>
    ).map((column) => column.name);
    const column = virtualTables.has(name)
      ? undefined
      : PAYLOAD_COLUMNS.find((candidate) => columns.includes(candidate));
    const lengths =
      column === undefined || rows === 0
        ? new Float64Array(0)
        : columnValues(db, `SELECT length(${quote(column)}) AS value FROM ${quote(name)}`);
    tables[name] = {
      rows,
      bytes: bytesByTable.get(name) ?? 0,
      ...(column === undefined || rows === 0
        ? {}
        : {
            payload: {
              column,
              mean:
                Math.round((lengths.reduce((sum, value) => sum + value, 0) / lengths.length) * 10) /
                10,
              quantiles: summarize(lengths),
            },
          }),
    };
  }
  const has = (table: string) => tables[table] !== undefined;
  const stat1 =
    db.prepare(`SELECT 1 FROM sqlite_schema WHERE name = 'sqlite_stat1'`).get() === undefined
      ? []
      : (db.prepare(`SELECT tbl, idx, stat FROM sqlite_stat1 ORDER BY tbl, idx`).all() as Array<{
          tbl: string;
          idx: string | null;
          stat: string;
        }>);
  const threadsTable = "orchestration_v2_projection_threads";
  const distributions = has(threadsTable)
    ? {
        threadsPerProject: summarize(
          columnValues(
            db,
            `SELECT COUNT(t.thread_id) AS value FROM projection_projects p
             LEFT JOIN ${threadsTable} t ON t.project_id = p.project_id GROUP BY p.project_id`,
          ),
        ),
        turnItemsPerThread: summarize(
          columnValues(
            db,
            `SELECT COUNT(i.turn_item_id) AS value FROM ${threadsTable} t
             LEFT JOIN orchestration_v2_projection_turn_items i ON i.thread_id = t.thread_id
             GROUP BY t.thread_id`,
          ),
        ),
      }
    : undefined;
  const lineageRows = has(threadsTable)
    ? (db
        .prepare(
          `SELECT COALESCE(json_extract(payload_json, '$.lineage.relationshipToParent'), 'root') AS kind,
             COUNT(*) AS n FROM ${threadsTable} GROUP BY 1`,
        )
        .all() as Array<{ kind: string; n: number }>)
    : [];
  const lineageCount = (kind: string) =>
    Number(lineageRows.find((row) => row.kind === kind)?.n ?? 0);
  const v1Import = has(threadsTable)
    ? Number(
        (
          db
            .prepare(
              `SELECT COUNT(*) AS n FROM ${threadsTable}
               WHERE json_extract(payload_json, '$.historyOrigin') = 'v1_import'`,
            )
            .get() as { n: number }
        ).n,
      )
    : 0;
  const runless = has(threadsTable)
    ? Number(
        (
          db
            .prepare(
              `SELECT COUNT(*) AS n FROM ${threadsTable} t
               WHERE EXISTS (SELECT 1 FROM orchestration_v2_projection_turn_items i WHERE i.thread_id = t.thread_id)
                 AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_runs r WHERE r.thread_id = t.thread_id)`,
            )
            .get() as { n: number }
        ).n,
      )
    : 0;
  return {
    format: SHAPE_FORMAT,
    version: 1,
    source,
    pageSize: pragma("page_size"),
    pageCount: pragma("page_count"),
    tables,
    stat1,
    ...(distributions === undefined ? {} : { distributions }),
    ...(has(threadsTable)
      ? {
          lineage: {
            root: lineageCount("root"),
            fork: lineageCount("fork"),
            subagent: lineageCount("subagent"),
            v1Import,
            runless,
          },
        }
      : {}),
    mix: {
      ...(has("orchestration_v2_projection_turn_items")
        ? {
            turnItemTypes: sizeMix(
              db,
              `SELECT type AS key, COUNT(*) AS rows, AVG(length(payload_json)) AS mean
               FROM orchestration_v2_projection_turn_items GROUP BY type ORDER BY type`,
            ),
          }
        : {}),
      ...(has("orchestration_events")
        ? {
            eventTypes: sizeMix(
              db,
              `SELECT event_type AS key, COUNT(*) AS rows, AVG(length(payload_json)) AS mean
               FROM orchestration_events WHERE application_event_version = 2 AND aggregate_kind = 'thread'
               GROUP BY event_type ORDER BY event_type`,
            ),
            applicationEvents: sizeMix(
              db,
              `SELECT 'v' || application_event_version || '/' || aggregate_kind AS key, COUNT(*) AS rows,
                 AVG(length(payload_json)) AS mean FROM orchestration_events GROUP BY 1 ORDER BY 1`,
            ),
          }
        : {}),
    },
  };
};

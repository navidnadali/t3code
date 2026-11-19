/**
 * Read-bound rows at a long thread's scale. A test captures the statements an
 * operation issues on a small thread, copies the database, gives the thread a long history on the
 * copy, and runs the statements again: a bounded read returns the same rows and takes about the
 * same SQLite VM steps, which are deterministic, whatever the history.
 */
import type * as NodeSqlite from "node:sqlite";

import type { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Statement from "effect/sql/Statement";
import type * as Tracer from "effect/Tracer";

declare module "node:sqlite" {
  interface StatementSync {
    // Node 26 has this; @types/node 24 does not declare it yet.
    stat(counter: "vmStep"): number;
  }
}

/** A long thread: 14k runs and attempts, 13k provider turns. */
export const HUGE_THREAD = {
  runs: Number(process.env.T3_HUGE_THREAD_RUNS ?? 14_000),
  providerTurns: Number(process.env.T3_HUGE_THREAD_PROVIDER_TURNS ?? 13_000),
} as const;

/**
 * Copies a thread's projection rows `copies` times (provider turns `turnCopies` times) under new
 * ids and lower ordinals: older completed runs with their attempts, nodes, provider turns, messages
 * and items on the same provider threads. Secret and link cards are not copied.
 */
export const addThreadHistory = (
  copy: NodeSqlite.DatabaseSync,
  threadId: ThreadId,
  copies: number,
  turnCopies: number,
) => {
  const tables: ReadonlyArray<{
    readonly table: string;
    readonly key: string;
    readonly renamed: ReadonlyArray<string>;
    readonly where?: string;
    readonly copies?: number;
  }> = [
    { table: "runs", key: "run_id", renamed: ["root_node_id", "user_message_id"] },
    { table: "run_attempts", key: "attempt_id", renamed: ["run_id"] },
    { table: "nodes", key: "node_id", renamed: ["run_id", "parent_node_id", "provider_turn_id"] },
    {
      table: "provider_turns",
      key: "provider_turn_id",
      renamed: ["run_attempt_id", "node_id"],
      copies: turnCopies,
    },
    { table: "messages", key: "message_id", renamed: ["run_id", "node_id"] },
    {
      table: "turn_items",
      key: "turn_item_id",
      renamed: ["run_id", "node_id", "provider_turn_id"],
      where: "AND type NOT IN ('secret_request', 'link_request')",
    },
  ];
  for (const { table, key, renamed, where, copies: n = copies } of tables) {
    const name = `orchestration_v2_projection_${table}`;
    const columns = copy
      .prepare(`SELECT name FROM pragma_table_info('${name}')`)
      .all()
      .map((column) => String(column.name));
    // Each copy takes the ordinals below the last one's, so copies keep (thread or provider
    // thread, ordinal) unique and the thread's own rows stay its latest.
    const span = columns.includes("ordinal")
      ? Number(
          copy
            .prepare(
              `SELECT COALESCE(MAX(ordinal) - MIN(ordinal) + 1, 1) AS span FROM ${name} WHERE thread_id = ?`,
            )
            .get(threadId)?.span ?? 1,
        )
      : 0;
    // History is settled: its rows are completed, in their columns and payloads.
    const settled = columns.includes("status");
    const values = columns.map((column) =>
      column === key || renamed.includes(column)
        ? `CASE WHEN ${column} IS NULL THEN NULL ELSE ${column} || ':history:' || history.n END`
        : column === "ordinal"
          ? `ordinal - history.n * ${span}`
          : settled && column === "status"
            ? "'completed'"
            : settled && column === "payload_json"
              ? "json_set(payload_json, '$.status', 'completed')"
              : column,
    );
    copy.exec(`WITH RECURSIVE history(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM history WHERE n < ${n})
      INSERT INTO ${name} (${columns.join(", ")})
      SELECT ${values.join(", ")} FROM ${name}, history
      WHERE thread_id = '${threadId}' ${where ?? ""}`);
  }
};

/** Collects the statements issued under any of `spans`, compiled with their parameters. */
export const captureStatements = (spans: ReadonlyArray<string>) => {
  const statements: Array<readonly [string, ReadonlyArray<unknown>]> = [];
  const record = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.provideService(
      effect,
      Statement.CurrentTransformer,
      (statement, _make, _fiber, span) => {
        for (
          let parent = Option.some<Tracer.AnySpan>(span);
          Option.isSome(parent) && parent.value._tag === "Span";
          parent = parent.value.parent
        ) {
          if (spans.includes(parent.value.name)) {
            statements.push(statement.compile());
            break;
          }
        }
        return Effect.succeed(statement);
      },
    );
  return { statements, record };
};

/** Rows each projection table returns and the VM steps of the reads, run again on `copy`. */
export const replayReads = (
  copy: NodeSqlite.DatabaseSync,
  statements: ReadonlyArray<readonly [string, ReadonlyArray<unknown>]>,
) => {
  const rows: Record<string, number> = {};
  let steps = 0;
  for (const [query, params] of statements) {
    if (!/^\s*(SELECT|WITH)\b/.test(query)) continue;
    const statement = copy.prepare(query);
    const read = statement.all(...(params as ReadonlyArray<NodeSqlite.SQLInputValue>)).length;
    steps += statement.stat("vmStep");
    const table = /FROM orchestration_v2_projection_([a-z_]+)/.exec(query)?.[1] ?? "other";
    rows[table] = (rows[table] ?? 0) + read;
  }
  return { rows, steps };
};

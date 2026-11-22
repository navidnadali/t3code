// @effect-diagnostics nodeBuiltinImport:off - runs the down and check scripts as an operator would, on the file.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import { readShellItemCounts } from "../orchestration-v2/ProjectionStore.ts";
import { recordedMigrations } from "./databaseMigrationHead.ts";
import { migrationManifest, runMigrations } from "./Migrations.ts";
import {
  PERF_FORK_MIGRATIONS_TABLE,
  perfForkLedgerProblems,
  runPerfForkMigrations,
} from "./PerfForkMigrations.ts";
import * as EventLogPruneState from "./PerfForkMigrations/EventLogPruneState.ts";
import { PERF_FORK_THREAD_ITEM_TOTALS } from "./testkit/PerfForkThreadItemTotals.ts";

// Read when a row runs, so a missing script fails that row alone.
const script = (name: string) =>
  NodeFS.readFileSync(new URL(`../../../../scripts/${name}`, import.meta.url), "utf8");

// The upstream head and fork chain of 0.1.9-perf.13 to 0.1.9-perf.15 and their fleet builds.
const PERF_14_UPSTREAM_HEAD = 96;
const PERF_14_FORK_CHAIN = [
  [1, "ProjectionTurnItemCounts"],
  [2, "TurnItemMessageOrdinalIndex"],
  [3, "ThreadItemTotals"],
  [4, EventLogPruneState.NAME],
] as const;
const FORK_CHAIN = [...PERF_14_FORK_CHAIN, [5, "RetireThreadItemTotals"]];
const UPSTREAM_HEAD = Math.max(...migrationManifest.map(([id]) => id));

const THREADS = ["thread-0", "thread-1", "thread-2", "thread-3"];
const RUNS = Array.from({ length: 12 }, (_, index) => `run-${index}`);
const STATUSES = ["queued", "running", "completed", "failed", "cancelled", "rolled_back"];
const runThread = (runId: string) => THREADS[Number(runId.slice(4)) % THREADS.length]!;

// Seeded writes as the projector makes them: runs in every status, some inserted after their items or
// deleted before them; items with no run, with a run, moved, some into another thread's run.
const writeHistory = (seed: number, writes: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    let state = seed;
    const random = () => {
      state = (state * 1_103_515_245 + 12_345) % 2_147_483_648;
      return state / 2_147_483_648;
    };
    const pick = <A>(values: ReadonlyArray<A>) => values[Math.floor(random() * values.length)]!;
    for (let step = 0; step < writes; step++) {
      const roll = random();
      if (roll < 0.25) {
        const runId = pick(RUNS);
        yield* sql`
          INSERT INTO orchestration_v2_projection_runs (
            run_id, thread_id, ordinal, provider, provider_thread_id, status, requested_at,
            completed_at, payload_json
          )
          VALUES (
            ${runId}, ${runThread(runId)}, ${step}, 'codex', NULL, ${pick(STATUSES)},
            '2026-11-22T00:00:00.000Z', NULL, '{}'
          )
          ON CONFLICT (run_id) DO UPDATE SET status = excluded.status
        `;
      } else if (roll < 0.32) {
        yield* sql`DELETE FROM orchestration_v2_projection_runs WHERE run_id = ${pick(RUNS)}`;
      } else if (roll < 0.9) {
        const runRoll = random();
        const runId = runRoll < 0.2 ? null : pick(RUNS);
        const held = random() < 0.3;
        yield* sql`
          INSERT INTO orchestration_v2_projection_turn_items (
            turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
          )
          VALUES (
            ${`item-${Math.floor(random() * 40)}`},
            ${runId === null || random() < 0.1 ? pick(THREADS) : runThread(runId)}, ${runId}, 1,
            ${held ? "user_message" : "assistant_message"}, 'completed',
            '2026-11-22T00:00:00.000Z', ${held ? '{"inputIntent":"queued_turn"}' : "{}"}
          )
          ON CONFLICT (turn_item_id) DO UPDATE SET
            thread_id = excluded.thread_id, run_id = excluded.run_id, type = excluded.type,
            payload_json = excluded.payload_json
        `;
      } else {
        yield* sql`
          DELETE FROM orchestration_v2_projection_turn_items
          WHERE turn_item_id = ${`item-${Math.floor(random() * 40)}`}
        `;
      }
    }
  });

// A database as 0.1.9-perf.14 leaves it: upstream through 96, fork 1-4 recorded, fork 3's totals
// table and triggers in place (its body as those builds ran it), and a write history on top.
const atPerf14 = Effect.gen(function* () {
  yield* runMigrations({ toMigrationInclusive: PERF_14_UPSTREAM_HEAD });
  yield* runPerfForkMigrations(4);
  yield* PERF_FORK_THREAD_ITEM_TOTALS;
  yield* writeHistory(20_261_122, 600);
});

const rows = <A extends object>(values: ReadonlyArray<A>) => values.map((row) => ({ ...row }));

const forkTotals = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return rows(
    yield* sql<{ readonly thread_id: string; readonly item_count: number }>`
      SELECT thread_id, item_count FROM t3_perf_thread_item_totals WHERE item_count <> 0
      ORDER BY thread_id
    `,
  );
});

const upstreamTotals = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return rows(
    yield* sql<{ readonly thread_id: string; readonly item_count: number }>`
      SELECT thread_id, item_count FROM orchestration_v2_projection_thread_item_counts
      WHERE item_count <> 0 ORDER BY thread_id
    `,
  );
});

const forkTotalsObjects = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<{ readonly name: string }>`
    SELECT name FROM sqlite_master WHERE name LIKE 't3_perf_thread_item_totals%' ORDER BY name
  `).map((row) => row.name);
});

const ledgers = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const upstream = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
  `;
  const fork = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM t3_perf_fork_migrations ORDER BY migration_id
  `;
  return {
    upstreamHead: Math.max(...upstream.map((row) => row.migration_id)),
    upstreamCount: upstream.length,
    fork: fork.map((row) => [row.migration_id, row.name]),
  };
});

const check = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [row] = yield* sql.unsafe<Record<string, number>>(script("thread-item-counts-check.sql"));
  return { ...row };
});

describe("fork migration 5 retires fork migration 3's totals for upstream's migration 102", () => {
  it.effect(
    "upgrades a perf.14 database: upstream through 105, fork 1-5, fork 3's objects gone, upstream's totals exact",
    () =>
      Effect.gen(function* () {
        yield* atPerf14;
        assert.lengthOf(yield* forkTotalsObjects, 7);
        const before = yield* forkTotals;
        assert.isAbove(before.length, 2);

        yield* runMigrations();
        assert.deepEqual(yield* ledgers, {
          upstreamHead: UPSTREAM_HEAD,
          upstreamCount: migrationManifest.length,
          fork: FORK_CHAIN,
        });
        assert.deepEqual(yield* forkTotalsObjects, []);
        // 102 backfills from 085's per-run counts: the totals fork 3 kept, thread for thread.
        assert.deepEqual(yield* upstreamTotals, before);
        const checked = yield* check;
        assert.strictEqual(checked.drifted_threads, 0);
        assert.strictEqual(checked.drifted_items, 0);
        // Writes after the upgrade keep upstream's totals exact, and a second start runs nothing.
        yield* writeHistory(7_919, 300);
        assert.strictEqual((yield* check).drifted_threads, 0);
        assert.deepEqual(yield* runMigrations(), []);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("never creates fork 3's totals on a fresh database", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      assert.deepEqual(yield* forkTotalsObjects, []);
      assert.deepEqual((yield* ledgers).fork, FORK_CHAIN);
      yield* writeHistory(31_337, 400);
      assert.strictEqual((yield* check).drifted_threads, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("reads the shell's counts from upstream's totals through its hidden-runs index", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const tables = (yield* sql<{ readonly sql: string }>`
        SELECT sql FROM sqlite_master WHERE name = 'orchestration_v2_projection_thread_item_counts'
      `).map((row) => row.sql);
      assert.lengthOf(tables, 1);
      yield* writeHistory(4_242, 200);
      const statements: Array<ReturnType<Statement.Statement<unknown>["compile"]>> = [];
      const observe: Statement.Transformer = (statement) => {
        statements.push(statement.compile());
        return Effect.succeed(statement);
      };
      yield* readShellItemCounts(
        sql,
        THREADS.map((thread) => ThreadId.make(thread)),
      ).pipe(Effect.provideService(Statement.CurrentTransformer, observe));
      const [query, params] = statements[0]!;
      const plan = (yield* sql.unsafe<{ detail: string }>(
        `EXPLAIN QUERY PLAN ${query}`,
        params,
      )).map((row) => row.detail);
      // The thread's total by key (upstream's table, aliased totals), then only its hidden runs.
      assert.include(plan, "SEARCH totals USING PRIMARY KEY (thread_id=?)");
      assert.isTrue(
        plan.some((detail) => detail.includes("orchestration_v2_projection_runs_hidden_items_idx")),
        JSON.stringify(plan),
      );
      assert.isFalse(
        plan.some((detail) => detail.includes("t3_perf_thread_item_totals")),
        JSON.stringify(plan),
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

it.layer(NodeServices.layer)(
  "the down path to a build at upstream 96 with fork chain 1-4",
  (it) => {
    // The rollback that does not restore a snapshot: upstream's 97-105 and fork 5 taken out by the
    // operator's scripts, so perf.14's preflight passes; perf.14's start then runs fork 3's body again
    // (recorded, objects missing), and the totals come back exact. This build's next start upgrades it
    // again to the same state.
    it.effect(
      "lets perf.14's checks pass, fork 3 come back exact, and this build upgrade it again",
      () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const databasePath = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: "t3-fork-005-down-" }),
            "statev2.sqlite",
          );
          const onFile = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
            effect.pipe(Effect.provide(NodeSqliteClient.layer({ filename: databasePath })));
          const perf14 = yield* onFile(
            Effect.gen(function* () {
              yield* atPerf14;
              return yield* forkTotals;
            }),
          );
          const upgraded = yield* onFile(
            Effect.gen(function* () {
              yield* runMigrations();
              return yield* upstreamTotals;
            }),
          );
          assert.deepEqual(upgraded, perf14);

          {
            using db = new NodeSqlite.DatabaseSync(databasePath);
            db.exec(script("upstream-097-105-down.sql"));
            db.exec(script("perf-fork-005-down.sql"));
          }
          // perf.14's preflight: no upstream migration above its head, and its fork chain runs the ledger.
          const upstream = recordedMigrations(databasePath) ?? [];
          assert.deepEqual(
            upstream.filter(({ id }) => id > PERF_14_UPSTREAM_HEAD),
            [],
          );
          assert.lengthOf(upstream, PERF_14_UPSTREAM_HEAD);
          const fork = recordedMigrations(databasePath, PERF_FORK_MIGRATIONS_TABLE) ?? [];
          assert.deepEqual(perfForkLedgerProblems(fork, PERF_14_FORK_CHAIN), []);
          assert.deepEqual(
            fork.map(({ id }) => id),
            [1, 2, 3, 4],
          );
          // perf.14's start: fork 3 recorded with its objects missing runs its body again.
          const restored = yield* onFile(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              for (const table of [
                "orchestration_v2_projection_thread_item_counts",
                "task_folders",
                "issue_tracker_bindings",
              ]) {
                assert.deepEqual(
                  yield* sql`SELECT name FROM sqlite_master WHERE name = ${table}`,
                  [],
                );
              }
              assert.deepEqual(
                yield* sql`
              SELECT name FROM pragma_table_info('projection_projects') WHERE name = 'tracked_folders_json'
            `,
                [],
              );
              yield* PERF_FORK_THREAD_ITEM_TOTALS;
              return yield* forkTotals;
            }),
          );
          assert.deepEqual(restored, perf14);

          // This build again: upstream's 97-105 and fork 5 run once more, to the upgraded state.
          yield* onFile(
            Effect.gen(function* () {
              yield* runMigrations();
              assert.deepEqual((yield* ledgers).fork, FORK_CHAIN);
              assert.deepEqual(yield* forkTotalsObjects, []);
              assert.deepEqual(yield* upstreamTotals, perf14);
              assert.strictEqual((yield* check).drifted_threads, 0);
            }),
          );
        }),
    );
  },
);

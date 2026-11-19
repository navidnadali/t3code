// @effect-diagnostics nodeBuiltinImport:off - runs the check and repair scripts as an operator would.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import { readShellItemCounts } from "../orchestration-v2/ProjectionStore.ts";
import { runMigrations } from "./Migrations.ts";
import {
  rebuildThreadItemTotals,
  THREAD_ITEM_TOTALS,
} from "./PerfForkMigrations/003_ThreadItemTotals.ts";

const script = (name: string) =>
  NodeFS.readFileSync(new URL(`../../../../scripts/${name}`, import.meta.url), "utf8");
const CHECK_SCRIPT = script("perf-fork-003-check.sql");
const REPAIR_SCRIPT = script("perf-fork-003-repair.sql");

type CountRow = {
  readonly thread_id: string;
  readonly item_count: number;
  readonly runless_item_count: number;
};

type CheckRow = {
  readonly threads: number;
  readonly drifted_threads: number;
  readonly drifted_items: number;
  readonly cross_thread_items: number;
};

// The shell's item count as 0.1.9-perf.12 read it: a sum over every run of the thread.
const perf12ShellItemCounts = (sql: SqlClient.SqlClient, threadIds: ReadonlyArray<string>) =>
  sql<CountRow>`
    SELECT counts.thread_id,
      SUM(CASE WHEN counts.run_key = '' OR r.status <> 'rolled_back'
        THEN counts.item_count - CASE
          WHEN r.status IN ('queued', 'cancelled') THEN (
            SELECT COUNT(*)
            FROM orchestration_v2_projection_turn_items held
            INDEXED BY orchestration_v2_projection_turn_items_queued_input_idx
            WHERE held.run_id = counts.run_key
              AND held.type = 'user_message'
              AND json_valid(held.payload_json)
              AND json_extract(held.payload_json, '$.inputIntent') = 'queued_turn'
          )
          ELSE 0
        END
        ELSE 0 END) AS item_count,
      SUM(CASE WHEN counts.run_key = '' THEN counts.item_count ELSE 0 END)
        AS runless_item_count
    FROM orchestration_v2_projection_turn_item_counts counts
    LEFT JOIN orchestration_v2_projection_runs r
      INDEXED BY orchestration_v2_projection_runs_run_status_idx
      ON r.run_id = counts.run_key
    WHERE counts.thread_id IN (SELECT value FROM json_each(${JSON.stringify(threadIds)}))
    GROUP BY counts.thread_id
  `;

// A missing row reads as zero items, so rows of zeros compare equal to no row.
const byThread = (rows: ReadonlyArray<CountRow>) =>
  Object.fromEntries(
    rows
      .filter((row) => row.item_count !== 0 || row.runless_item_count !== 0)
      .map((row) => [row.thread_id, [row.item_count, row.runless_item_count]]),
  );

const THREADS = ["thread-0", "thread-1", "thread-2"];
const RUNS = Array.from({ length: 9 }, (_, index) => `run-${index}`);
const ITEMS = Array.from({ length: 16 }, (_, index) => `item-${index}`);
const STATUSES = ["queued", "running", "completed", "failed", "cancelled", "rolled_back"];
// A run's own thread; its items mostly sit there, and some in another thread.
const runThread = (runId: string) => THREADS[Number(runId.slice(4)) % THREADS.length]!;

const upsertRun = (sql: SqlClient.SqlClient, runId: string, ordinal: number, status: string) => sql`
  INSERT INTO orchestration_v2_projection_runs (
    run_id, thread_id, ordinal, provider, provider_thread_id, status, requested_at, completed_at,
    payload_json
  )
  VALUES (
    ${runId}, ${runThread(runId)}, ${ordinal}, 'codex', NULL, ${status},
    '2026-11-18T00:00:00.000Z', NULL, '{}'
  )
  ON CONFLICT (run_id) DO UPDATE SET status = excluded.status
`;

const upsertItem = (
  sql: SqlClient.SqlClient,
  item: {
    readonly id: string;
    readonly threadId: string;
    readonly runId: string | null;
    readonly type: string;
    readonly payloadJson: string;
  },
) => sql`
  INSERT INTO orchestration_v2_projection_turn_items (
    turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
  )
  VALUES (
    ${item.id}, ${item.threadId}, ${item.runId}, 1, ${item.type}, 'completed',
    '2026-11-18T00:00:00.000Z', ${item.payloadJson}
  )
  ON CONFLICT (turn_item_id) DO UPDATE SET
    thread_id = excluded.thread_id,
    run_id = excluded.run_id,
    type = excluded.type,
    payload_json = excluded.payload_json
`;

// What each thread's timeline shows of its items (createOrchestrationV2TurnItemVisibility, from the
// thread's projection: its items and its own runs), as upstream's shell count takes it: an item with
// a run counts once that run exists; the thread's own rolled-back runs hide their items, and its own
// queued or cancelled runs their queued input.
const timelineCounts = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const runs = new Map(
      (yield* sql<{ readonly run_id: string; readonly thread_id: string; readonly status: string }>`
        SELECT run_id, thread_id, status FROM orchestration_v2_projection_runs
      `).map((run) => [run.run_id, run]),
    );
    const items = yield* sql<{
      readonly thread_id: string;
      readonly run_id: string | null;
      readonly type: string;
      readonly payload_json: string;
    }>`SELECT thread_id, run_id, type, payload_json FROM orchestration_v2_projection_turn_items`;
    const counts: Array<CountRow> = [];
    for (const threadId of THREADS) {
      let itemCount = 0;
      let runless = 0;
      for (const item of items) {
        if (item.thread_id !== threadId) continue;
        if (item.run_id === null || item.run_id === "") {
          runless++;
          itemCount++;
          continue;
        }
        const run = runs.get(item.run_id);
        if (run === undefined) continue;
        const own = run.thread_id === threadId;
        if (own && run.status === "rolled_back") continue;
        const held =
          item.type === "user_message" && item.payload_json.includes('"inputIntent":"queued_turn"');
        if (own && (run.status === "queued" || run.status === "cancelled") && held) continue;
        itemCount++;
      }
      counts.push({ thread_id: threadId, item_count: itemCount, runless_item_count: runless });
    }
    return counts;
  });

const check = (sql: SqlClient.SqlClient) =>
  sql.unsafe<CheckRow>(CHECK_SCRIPT).pipe(Effect.map((rows) => rows[0]!));

const totals = (sql: SqlClient.SqlClient) =>
  sql<{ readonly thread_id: string; readonly item_count: number }>`
    SELECT thread_id, item_count FROM t3_perf_thread_item_totals WHERE item_count <> 0
    ORDER BY thread_id
  `;

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("fork migration 3's totals", (it) => {
  // Seeded write histories (runs queued, cancelled, rolled back, inserted after or deleted before
  // their items; items moved, some into another thread's run; rolled-back transactions). After each
  // write the count equals each timeline's and the totals a rebuild; while every item sits in its
  // run's thread, it also equals perf.12's.
  it.effect("keeps the shell's item count equal to each timeline's through random writes", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      let seed = 20_261_118;
      const random = () => {
        seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648;
        return seed / 2_147_483_648;
      };
      const pick = <A>(values: ReadonlyArray<A>) => values[Math.floor(random() * values.length)]!;
      let ordinal = 0;
      const write = Effect.gen(function* () {
        const roll = random();
        if (roll < 0.25) {
          yield* upsertRun(sql, pick(RUNS), ++ordinal, pick(STATUSES));
        } else if (roll < 0.33) {
          yield* sql`DELETE FROM orchestration_v2_projection_runs WHERE run_id = ${pick(RUNS)}`;
        } else if (roll < 0.36) {
          // Renaming a run to an unused id of the same thread.
          const from = pick(RUNS);
          const to = RUNS.find(
            (runId) => runId !== from && runThread(runId) === runThread(from) && random() < 0.5,
          );
          if (to !== undefined) {
            yield* sql`
              UPDATE OR IGNORE orchestration_v2_projection_runs SET run_id = ${to}
              WHERE run_id = ${from}
            `;
          }
        } else if (roll < 0.88) {
          const runRoll = random();
          const runId = runRoll < 0.2 ? null : runRoll < 0.25 ? "" : pick(RUNS);
          const typeRoll = random();
          yield* upsertItem(sql, {
            id: pick(ITEMS),
            threadId:
              runId === null || runId === "" || random() < 0.1 ? pick(THREADS) : runThread(runId),
            runId,
            type: typeRoll < 0.45 ? "user_message" : "assistant_message",
            payloadJson:
              typeRoll < 0.3
                ? '{"inputIntent":"queued_turn"}'
                : typeRoll < 0.45
                  ? '{"inputIntent":"turn"}'
                  : "{}",
          });
        } else {
          yield* sql`
            DELETE FROM orchestration_v2_projection_turn_items WHERE turn_item_id = ${pick(ITEMS)}
          `;
        }
      });
      // Writes after which some item sat in another thread's run, and after which none did.
      const steps = { crossThread: 0, perf12: 0 };
      const assertConsistent = Effect.gen(function* () {
        const checked = yield* check(sql);
        assert.strictEqual(checked.drifted_threads, 0);
        const shell = byThread(
          yield* readShellItemCounts(
            sql,
            THREADS.map((thread) => ThreadId.make(thread)),
          ),
        );
        assert.deepEqual(shell, byThread(yield* timelineCounts(sql)));
        if (checked.cross_thread_items === 0) {
          assert.deepEqual(shell, byThread(yield* perf12ShellItemCounts(sql, THREADS)));
          steps.perf12++;
        } else {
          steps.crossThread++;
        }
      });

      let rolledBack = 0;
      for (let history = 0; history < 150; history++) {
        for (let step = 0; step < 40; step++) {
          if (random() < 0.08) {
            const writes = 1 + Math.floor(random() * 4);
            const exit = yield* Effect.exit(
              sql.withTransaction(
                Effect.gen(function* () {
                  for (let index = 0; index < writes; index++) yield* write;
                  return yield* Effect.fail("rolled back" as const);
                }),
              ),
            );
            assert.isTrue(Exit.isFailure(exit));
            rolledBack++;
          } else {
            yield* write;
          }
          yield* assertConsistent;
        }
        yield* sql`DELETE FROM orchestration_v2_projection_turn_items`;
        yield* sql`DELETE FROM orchestration_v2_projection_runs`;
        assert.deepEqual(yield* totals(sql), []);
      }
      assert.isAbove(rolledBack, 100);
      assert.isAbove(steps.crossThread, 300);
      assert.isAbove(steps.perf12, 3_000);
    }),
  );

  // Items in another thread's run show in their own thread's timeline whatever that run's status,
  // and a run's held input in another thread is not its own thread's to hide. perf.12 hid the first
  // and subtracted the second from both threads, down to negative counts; the check counts them.
  it.effect("counts items in another thread's run as each timeline shows them", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* upsertRun(sql, "run-0", 1, "rolled_back");
      yield* upsertRun(sql, "run-3", 2, "cancelled");
      for (const [id, threadId, runId, held] of [
        ["x-1", "thread-1", "run-0", false],
        ["x-2", "thread-1", "run-0", false],
        ["h-1", "thread-1", "run-3", true],
        ["h-2", "thread-0", "run-3", true],
      ] as const) {
        yield* upsertItem(sql, {
          id,
          threadId,
          runId,
          type: held ? "user_message" : "assistant_message",
          payloadJson: held ? '{"inputIntent":"queued_turn"}' : "{}",
        });
      }
      const { threads: _, ...checked } = yield* check(sql);
      assert.deepEqual(checked, { drifted_threads: 0, drifted_items: 0, cross_thread_items: 3 });
      const both = ["thread-0", "thread-1"];
      assert.deepEqual(byThread(yield* perf12ShellItemCounts(sql, both)), {
        "thread-0": [-1, 0],
        "thread-1": [-1, 0],
      });
      const shell = byThread(
        yield* readShellItemCounts(
          sql,
          both.map((id) => ThreadId.make(id)),
        ),
      );
      assert.deepEqual(shell, { "thread-1": [3, 0] });
      assert.deepEqual(shell, byThread(yield* timelineCounts(sql)));
      yield* sql`DELETE FROM orchestration_v2_projection_turn_items`;
      yield* sql`DELETE FROM orchestration_v2_projection_runs`;
    }),
  );
});

it.layer(NodeServices.layer)("fork migration 3's check and repair scripts", (it) => {
  it.effect("reports drifted totals by count alone, and the repair rebuilds them", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const filename = path.join(
        yield* fs.makeTempDirectoryScoped({ prefix: "t3-fork-003-" }),
        "statev2.sqlite",
      );
      const rebuilt = yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        yield* upsertRun(sql, "run-0", 1, "completed");
        yield* upsertRun(sql, "run-1", 2, "cancelled");
        for (const [id, runId] of [
          ["a", "run-0"],
          ["b", "run-0"],
          ["c", null],
          ["d", "run-1"],
          ["e", "run-8"],
        ] as const) {
          yield* upsertItem(sql, {
            id,
            threadId: runId === null ? "thread-2" : runThread(runId),
            runId,
            type: "assistant_message",
            payloadJson: "{}",
          });
        }
        yield* rebuildThreadItemTotals;
        return yield* totals(sql);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));

      using db = new NodeSqlite.DatabaseSync(filename);
      const checkRow = () => ({ ...db.prepare(CHECK_SCRIPT).get() });
      assert.deepEqual(checkRow(), {
        threads: 3,
        drifted_threads: 0,
        drifted_items: 0,
        cross_thread_items: 0,
      });
      db.exec(`
        UPDATE ${THREAD_ITEM_TOTALS} SET item_count = item_count + 5 WHERE thread_id = 'thread-0';
        DELETE FROM ${THREAD_ITEM_TOTALS} WHERE thread_id = 'thread-1';
        INSERT INTO ${THREAD_ITEM_TOTALS} (thread_id, item_count) VALUES ('thread-gone', 3);
      `);
      assert.deepEqual(checkRow(), {
        threads: 3,
        drifted_threads: 3,
        drifted_items: 9,
        cross_thread_items: 0,
      });
      db.exec(REPAIR_SCRIPT);
      assert.deepEqual(checkRow(), {
        threads: 3,
        drifted_threads: 0,
        drifted_items: 0,
        cross_thread_items: 0,
      });
      assert.deepEqual(
        db
          .prepare(
            `SELECT thread_id, item_count FROM ${THREAD_ITEM_TOTALS} WHERE item_count <> 0 ORDER BY thread_id`,
          )
          .all()
          .map((row) => ({ ...row })),
        rebuilt.map((row) => ({ ...row })),
      );
    }),
  );
});

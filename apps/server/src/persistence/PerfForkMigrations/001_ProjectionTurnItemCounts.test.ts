import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import ProjectionTurnItemCounts from "./001_ProjectionTurnItemCounts.ts";

it.effect(
  "backfills turn-item counts per thread and run, re-runs cleanly, then follows writes",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toPerfForkMigrationInclusive: 0 });
      // The projection's own upsert: every update rewrites thread_id and run_id.
      const upsert = (id: string, threadId: string, runId: string | null) => sql`
      INSERT INTO orchestration_v2_projection_turn_items (
        turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
      )
      VALUES (
        ${id}, ${threadId}, ${runId}, 1, 'assistant_message', 'completed',
        '2026-11-01T00:00:00.000Z', '{}'
      )
      ON CONFLICT(turn_item_id) DO UPDATE SET
        thread_id = excluded.thread_id,
        run_id = excluded.run_id,
        updated_at = excluded.updated_at
    `;
      const stored = () => sql<{
        readonly thread_id: string;
        readonly run_id: string;
        readonly item_count: number;
      }>`
      SELECT thread_id, run_id, item_count
      FROM orchestration_v2_projection_turn_item_counts
      ORDER BY thread_id, run_id
    `;
      const counted = () => sql`
      SELECT thread_id, COALESCE(run_id, '') AS run_id, COUNT(*) AS item_count
      FROM orchestration_v2_projection_turn_items
      GROUP BY thread_id, run_id
      ORDER BY thread_id, run_id
    `;
      for (const [id, threadId, runId] of [
        ["a1", "thread-a", "run-1"],
        ["a2", "thread-a", "run-1"],
        ["a3", "thread-a", "run-1"],
        ["a4", "thread-a", "run-2"],
        ["a5", "thread-a", null],
        ["a6", "thread-a", null],
        ["b1", "thread-b", "run-3"],
        ["c1", "thread-c", null],
      ] as const) {
        yield* upsert(id, threadId, runId);
      }

      yield* runMigrations();
      const backfilled = yield* stored();
      assert.deepEqual(
        backfilled.map((row) => [row.thread_id, row.run_id, row.item_count]),
        [
          ["thread-a", "", 2],
          ["thread-a", "run-1", 3],
          ["thread-a", "run-2", 1],
          ["thread-b", "run-3", 1],
          ["thread-c", "", 1],
        ],
      );
      yield* ProjectionTurnItemCounts;
      assert.deepEqual(yield* stored(), backfilled);

      yield* upsert("a1", "thread-a", "run-2");
      yield* upsert("a2", "thread-a", "run-1");
      yield* upsert("a5", "thread-a", "run-1");
      yield* upsert("b1", "thread-c", "run-3");
      yield* upsert("b2", "thread-b", null);
      yield* sql`DELETE FROM orchestration_v2_projection_turn_items WHERE turn_item_id = 'c1'`;
      assert.deepEqual(yield* stored(), yield* counted());
      assert.deepEqual(
        (yield* stored()).map((row) => [row.thread_id, row.run_id, row.item_count]),
        [
          ["thread-a", "", 1],
          ["thread-a", "run-1", 3],
          ["thread-a", "run-2", 2],
          ["thread-b", "", 1],
          ["thread-c", "run-3", 1],
        ],
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import TurnItemMessageOrdinalIndex from "./078_TurnItemMessageOrdinalIndex.ts";

const ordinalsOf = (threadId: string, from: string) => `
  SELECT json_extract(payload_json, '$.messageId') AS message_id, MIN(ordinal) AS ordinal
  FROM ${from}
  WHERE thread_id = '${threadId}' AND type IN ('user_message', 'assistant_message')
  GROUP BY json_extract(payload_json, '$.messageId')
  ORDER BY message_id
`;

it.effect(
  "adds the message-ordinal index once, re-runs cleanly and keeps the grouped ordinals",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 77 });
      for (const [id, threadId, ordinal, type, messageId] of [
        ["a1", "thread-a", 1, "user_message", "m1"],
        ["a2", "thread-a", 3, "assistant_message", "m2"],
        ["a3", "thread-a", 2, "assistant_message", "m2"],
        ["a4", "thread-a", 4, "dynamic_tool", "m3"],
        ["a5", "thread-a", 5, "user_message", "m3"],
        ["b1", "thread-b", 1, "assistant_message", "m1"],
      ] as const) {
        yield* sql`
        INSERT INTO orchestration_v2_projection_turn_items (
          turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
        )
        VALUES (
          ${id}, ${threadId}, NULL, ${ordinal}, ${type}, 'completed',
          '2026-11-01T00:00:00.000Z', ${JSON.stringify({ messageId })}
        )
      `;
      }
      const expected = [
        { message_id: "m1", ordinal: 1 },
        { message_id: "m2", ordinal: 2 },
        { message_id: "m3", ordinal: 5 },
      ];
      const unindexed = "orchestration_v2_projection_turn_items NOT INDEXED";
      assert.deepEqual(yield* sql.unsafe(ordinalsOf("thread-a", unindexed)), expected);

      assert.deepEqual(
        (yield* runMigrations()).map(([id]) => id),
        [78],
      );
      const index = () => sql<{ readonly sql: string }>`
      SELECT sql FROM sqlite_master
      WHERE type = 'index' AND name = 'orchestration_v2_projection_turn_items_message_ordinal_idx'
    `;
      const created = yield* index();
      assert.lengthOf(created, 1);
      yield* TurnItemMessageOrdinalIndex;
      assert.deepEqual(yield* index(), created);
      assert.deepEqual(yield* runMigrations(), []);

      const indexed = "orchestration_v2_projection_turn_items";
      assert.include(
        (yield* sql.unsafe<{ readonly detail: string }>(
          `EXPLAIN QUERY PLAN ${ordinalsOf("thread-a", indexed)}`,
        ))[0]?.detail,
        "USING INDEX orchestration_v2_projection_turn_items_message_ordinal_idx",
      );
      assert.deepEqual(yield* sql.unsafe(ordinalsOf("thread-a", indexed)), expected);
      assert.deepEqual(yield* sql.unsafe(ordinalsOf("thread-b", indexed)), [
        { message_id: "m1", ordinal: 1 },
      ]);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

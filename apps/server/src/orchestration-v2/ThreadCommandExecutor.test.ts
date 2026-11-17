import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import { CurrentWaitObserver } from "@t3tools/shared/WaitObserver";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import { recordingWaitObserver } from "../observability/WaitObserver.testkit.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

it.effect("ranks thread lock waits by thread and sizes ranked threads by decade", () => {
  const { observer, reports, sizers } = recordingWaitObserver();
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1234)
      INSERT INTO orchestration_v2_projection_runs
        (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
      SELECT 'run-' || i, 'thread-huge', i, 'codex', 'completed', '2026-11-17T00:00:00Z', '{}'
      FROM n`;
    yield* sql`
      WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 56)
      INSERT INTO orchestration_v2_projection_subagents
        (subagent_id, thread_id, parent_node_id, provider, origin, status, updated_at, payload_json)
      SELECT 'subagent-' || i, 'thread-huge', 'node', 'codex', 'provider', 'completed',
        '2026-11-17T00:00:00Z', '{}'
      FROM n`;
    yield* Layer.build(ThreadCommandExecutor.layerThreadSizes);
    const executor = yield* ThreadCommandExecutor.make;
    yield* executor.withLock(ThreadId.make("thread-huge"), Effect.void);
    // SQLite reports its own connection waits too.
    const lockWaits = reports("wait").filter(([site]) => !String(site).startsWith("sqlite."));
    assert.deepStrictEqual(lockWaits, [
      [ThreadCommandExecutor.THREAD_LOCK_SITE, 0, undefined, "thread-huge", undefined],
    ]);

    for (const site of [
      ThreadCommandExecutor.THREAD_LOCK_SITE,
      ThreadCommandExecutor.MESSAGE_INTAKE_SITE,
    ]) {
      const sizes = yield* sizers.get(site)!(["thread-huge", "thread-new"]);
      assert.deepStrictEqual(sizes.get("thread-huge"), { runs: 1_000, subagents: 10 });
      assert.deepStrictEqual(sizes.get("thread-new"), { runs: 0, subagents: 0 });
    }
  }).pipe(
    Effect.provide(SqlitePersistence.layerMemory),
    Effect.provideService(CurrentWaitObserver, observer),
  );
});

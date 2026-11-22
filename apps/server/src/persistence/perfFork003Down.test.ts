// @effect-diagnostics nodeBuiltinImport:off - runs the down script as an operator would, on the file.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { vi } from "vite-plus/test";

import { runServicePreflight } from "../cloud/servicePreflight.ts";
import { SERVICE_LAUNCHER_PROTOCOL } from "../cloud/serviceProtocol.ts";
import { recordedMigrations } from "./databaseMigrationHead.ts";
import { runMigrations } from "./Migrations.ts";
import { PERF_FORK_MIGRATIONS_TABLE } from "./PerfForkMigrations.ts";

// The service preflight of a build with the ledger guard whose fork chain ends at 2. Migrations
// run this build's chain: they call the module's own function, not this export.
vi.mock("./PerfForkMigrations.ts", async (importOriginal) => {
  const original = await importOriginal<typeof import("./PerfForkMigrations.ts")>();
  return {
    ...original,
    perfForkLedgerProblems: (
      recorded: ReadonlyArray<{ readonly id: number; readonly name: string }>,
    ) =>
      original.perfForkLedgerProblems(recorded, [
        [1, "ProjectionTurnItemCounts"],
        [2, "TurnItemMessageOrdinalIndex"],
      ]),
  };
});

// 0.1.9-perf.12's own preflight: it refuses a fork id above its chain's head, 2.
const perf12ForkRefusal = (databasePath: string) =>
  (recordedMigrations(databasePath, PERF_FORK_MIGRATIONS_TABLE) ?? []).filter(({ id }) => id > 2);

const DOWN_005_SCRIPT = NodeFS.readFileSync(
  new URL("../../../../scripts/perf-fork-005-down.sql", import.meta.url),
  "utf8",
);
const DOWN_004_SCRIPT = NodeFS.readFileSync(
  new URL("../../../../scripts/perf-fork-004-down.sql", import.meta.url),
  "utf8",
);
const DOWN_SCRIPT = NodeFS.readFileSync(
  new URL("../../../../scripts/perf-fork-003-down.sql", import.meta.url),
  "utf8",
);
const CHECK_SCRIPT = NodeFS.readFileSync(
  new URL("../../../../scripts/thread-item-counts-check.sql", import.meta.url),
  "utf8",
);

const insertItem = (db: NodeSqlite.DatabaseSync, id: string, threadId: string, runId: string) =>
  db
    .prepare(
      `INSERT INTO orchestration_v2_projection_turn_items (
         turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
       ) VALUES (?, ?, ?, 1, 'assistant_message', 'completed', '2026-11-18T00:00:00.000Z', '{}')`,
    )
    .run(id, threadId, runId);

const insertRun = (db: NodeSqlite.DatabaseSync, runId: string, threadId: string, ordinal: number) =>
  db
    .prepare(
      `INSERT INTO orchestration_v2_projection_runs (
         run_id, thread_id, ordinal, provider, provider_thread_id, status, requested_at,
         completed_at, payload_json
       ) VALUES (?, ?, ?, 'codex', NULL, 'completed', '2026-11-18T00:00:00.000Z', NULL, '{}')`,
    )
    .run(runId, threadId, ordinal);

const totalsObjects = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare(
      "SELECT type, name FROM sqlite_master WHERE name LIKE 't3_perf_thread_item_totals%' ORDER BY name",
    )
    .all()
    .map(({ type, name }) => `${String(type)}:${String(name)}`);

const forkLedger = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare(`SELECT migration_id, name FROM ${PERF_FORK_MIGRATIONS_TABLE} ORDER BY migration_id`)
    .all()
    .map(({ migration_id, name }) => [Number(migration_id), String(name)]);

it.layer(NodeServices.layer)("fork migration 3's down step", (it) => {
  // perf.12's preflight, and the guard's for a chain ending at 2, refuse a database with fork
  // migration 3. The down steps (5, 4, then 3) take them out and leave the ledger 1..2, so both pass
  // and perf.12 writes on upstream's counts alone; this build's next start records 3 to 5 again
  // (3 creates nothing since upstream's 102 keeps the totals), and upstream's totals match those writes.
  it.effect(
    "lets perf.12's preflight pass, and this build's next start reads upstream's totals",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const databasePath = path.join(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-fork-003-down-" }),
          "statev2.sqlite",
        );
        const migrate = runMigrations().pipe(
          Effect.provide(NodeSqliteClient.layer({ filename: databasePath })),
        );
        const preflight = () =>
          runServicePreflight({
            databasePath,
            launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
            version: "0.1.9-perf.12",
          });
        yield* migrate;
        {
          using db = new NodeSqlite.DatabaseSync(databasePath);
          insertRun(db, "run-1", "thread-1", 1);
          insertItem(db, "a", "thread-1", "run-1");
          assert.deepEqual(totalsObjects(db), []);
        }
        // This build ships fork migrations 3 to 5: perf.12 refuses them until the rollback takes them
        // out, newest first (005, 004, then 003).
        assert.deepEqual(perf12ForkRefusal(databasePath), [
          { id: 3, name: "ThreadItemTotals" },
          { id: 4, name: "EventLogPruneState" },
          { id: 5, name: "RetireThreadItemTotals" },
        ]);
        const blocked = preflight();
        assert.strictEqual(blocked.status, "blocked");
        assert.include(
          blocked.status === "blocked" ? blocked.reason : "",
          `${PERF_FORK_MIGRATIONS_TABLE}:3_ThreadItemTotals (unknown to this build)`,
        );

        {
          using db = new NodeSqlite.DatabaseSync(databasePath);
          db.exec(DOWN_005_SCRIPT);
          db.exec(DOWN_004_SCRIPT);
          db.exec(DOWN_SCRIPT);
          assert.deepEqual(totalsObjects(db), []);
          assert.deepEqual(forkLedger(db), [
            [1, "ProjectionTurnItemCounts"],
            [2, "TurnItemMessageOrdinalIndex"],
          ]);
        }
        assert.deepEqual(perf12ForkRefusal(databasePath), []);
        assert.deepEqual(preflight(), {
          status: "ready",
          version: "0.1.9-perf.12",
          launcherProtocol: SERVICE_LAUNCHER_PROTOCOL,
        });

        // perf.12's writes: no totals triggers, upstream's counts only.
        {
          using db = new NodeSqlite.DatabaseSync(databasePath);
          insertItem(db, "b", "thread-1", "run-1");
          insertItem(db, "c", "thread-2", "run-2");
          insertRun(db, "run-2", "thread-2", 1);
          db.prepare(
            "DELETE FROM orchestration_v2_projection_turn_items WHERE turn_item_id = 'a'",
          ).run();
        }

        yield* migrate;
        using db = new NodeSqlite.DatabaseSync(databasePath);
        assert.deepEqual(totalsObjects(db), []);
        assert.deepEqual(forkLedger(db).slice(-3), [
          [3, "ThreadItemTotals"],
          [4, "EventLogPruneState"],
          [5, "RetireThreadItemTotals"],
        ]);
        assert.deepEqual(
          { ...db.prepare(CHECK_SCRIPT).get() },
          { threads: 2, drifted_threads: 0, drifted_items: 0, cross_thread_items: 0 },
        );
        assert.deepEqual(
          db
            .prepare(
              "SELECT thread_id, item_count FROM orchestration_v2_projection_thread_item_counts ORDER BY thread_id",
            )
            .all()
            .map((row) => ({ ...row })),
          [
            { thread_id: "thread-1", item_count: 1 },
            { thread_id: "thread-2", item_count: 1 },
          ],
        );
      }),
  );
});

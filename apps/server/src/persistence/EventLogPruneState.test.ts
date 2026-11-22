// Rolling back to perf.12 after the compaction's fork migration ran. perf.12's start
// and service preflight refuse a fork ledger id they do not know. This build ships fork migrations 3
// (ThreadItemTotals), 4 (this one) and 5 (RetireThreadItemTotals): the rollback takes them out newest
// first, so after 005's down script this down step must leave the chain 1..3 (a prefix, which the
// ledger guard accepts), and after 003's down script perf.12's chain exactly. The operator's form,
// scripts/perf-fork-004-down.sql, must leave the same.
// @effect-diagnostics nodeBuiltinImport:off - runs the down scripts as an operator would, on the file.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import { recordedMigrations } from "./databaseMigrationHead.ts";
import {
  EVENT_LOG_PRUNE_CURSOR,
  EVENT_LOG_PRUNE_MARK,
  EVENT_LOG_PRUNED_THROUGH,
  EVENT_LOG_REFERENCE_SCAN,
  EVENT_LOG_REFERENCE_SCAN_FROM,
} from "./OrchestrationEventStore.ts";
import { PERF_FORK_MIGRATIONS_TABLE, perfForkLedgerProblems } from "./PerfForkMigrations.ts";
import * as EventLogPruneState from "./PerfForkMigrations/EventLogPruneState.ts";
import * as Sqlite from "./Sqlite.ts";

// perf.12's fork chain, the one a rollback ends at.
const PERF_12_CHAIN = [
  [1, "ProjectionTurnItemCounts"],
  [2, "TurnItemMessageOrdinalIndex"],
] as const;
// This build's chain without fork migrations 5 and 4: what this down step leaves after 005's.
const BEFORE_004_CHAIN = [...PERF_12_CHAIN, [3, "ThreadItemTotals"]] as const;
const script = (name: string) =>
  NodeFS.readFileSync(new URL(`../../../../scripts/${name}`, import.meta.url), "utf8");

it.effect("the compaction state's down step leaves a ledger and schema perf.12 starts on", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-prune-down-" });
    const dbPath = path.join(directory, "state.sqlite");
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const objects = () =>
        sql<{ readonly name: string }>`
          SELECT name FROM sqlite_master WHERE name = ${EventLogPruneState.TABLE}
        `;
      assert.lengthOf(yield* objects(), 1);
      // The state a pass leaves: its progress, held rows, and the boundary every build reads.
      yield* sql.unsafe(`INSERT INTO ${EventLogPruneState.TABLE} (sequence, held) VALUES (7, 1)`);
      for (const name of [
        EVENT_LOG_PRUNE_CURSOR,
        EVENT_LOG_REFERENCE_SCAN,
        EVENT_LOG_REFERENCE_SCAN_FROM,
        EVENT_LOG_PRUNE_MARK,
        EVENT_LOG_PRUNED_THROUGH,
      ]) {
        yield* sql`
          INSERT INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
          VALUES (${name}, 2, 7, '2026-11-18T00:00:00.000Z')
        `;
      }

      yield* sql.unsafe(
        `DELETE FROM ${PERF_FORK_MIGRATIONS_TABLE} WHERE migration_id = 5 AND name = 'RetireThreadItemTotals'`,
      );
      yield* EventLogPruneState.down;

      assert.lengthOf(yield* objects(), 0);
      const left = yield* sql<{ readonly projection_name: string }>`
        SELECT projection_name FROM orchestration_v2_projection_metadata
        WHERE projection_name LIKE 'event-log-%' ORDER BY projection_name
      `;
      // The boundary stays: perf.12 reads it with the same meaning. The progress goes with the
      // held rows, so a later roll-forward classifies again instead of losing them.
      assert.deepEqual(
        left.map((row) => row.projection_name),
        [EVENT_LOG_PRUNE_MARK, EVENT_LOG_PRUNED_THROUGH].toSorted(),
      );
    }).pipe(Effect.provide(Sqlite.layerFromPath(dbPath)));

    const fork = recordedMigrations(dbPath, PERF_FORK_MIGRATIONS_TABLE);
    assert.isNotNull(fork);
    assert.deepEqual(perfForkLedgerProblems(fork!, BEFORE_004_CHAIN), []);
    assert.deepEqual(
      fork!.map(({ id }) => id),
      BEFORE_004_CHAIN.map(([id]) => id),
    );
    // Then 003's down script: perf.12's chain exactly.
    {
      using db = new NodeSqlite.DatabaseSync(dbPath);
      db.exec(script("perf-fork-003-down.sql"));
    }
    const perf12 = recordedMigrations(dbPath, PERF_FORK_MIGRATIONS_TABLE);
    assert.deepEqual(perfForkLedgerProblems(perf12!, PERF_12_CHAIN), []);
    assert.deepEqual(
      perf12!.map(({ id }) => id),
      PERF_12_CHAIN.map(([id]) => id),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("the operator's 004 down script leaves exactly what the down step leaves", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-prune-down-sql-" });
    const prepared = (name: string) =>
      Effect.gen(function* () {
        const dbPath = path.join(directory, name);
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql.unsafe(
            `INSERT INTO ${EventLogPruneState.TABLE} (sequence, held) VALUES (7, 1)`,
          );
          for (const projection of [
            EVENT_LOG_PRUNE_CURSOR,
            EVENT_LOG_REFERENCE_SCAN,
            EVENT_LOG_REFERENCE_SCAN_FROM,
            EVENT_LOG_PRUNE_MARK,
            EVENT_LOG_PRUNED_THROUGH,
          ]) {
            yield* sql`
              INSERT INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
              VALUES (${projection}, 2, 7, '2026-11-18T00:00:00.000Z')
            `;
          }
        }).pipe(Effect.provide(Sqlite.layerFromPath(dbPath)));
        return dbPath;
      });
    // 005's down first, as the operator runs it: in the same session as the down step, since
    // opening the database runs this build's migrations, 5 included.
    const viaEffect = yield* prepared("effect.sqlite");
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe(
        `DELETE FROM ${PERF_FORK_MIGRATIONS_TABLE} WHERE migration_id = 5 AND name = 'RetireThreadItemTotals'`,
      );
      yield* EventLogPruneState.down;
    }).pipe(Effect.provide(Sqlite.layerFromPath(viaEffect)));
    const viaScript = yield* prepared("script.sqlite");
    {
      using db = new NodeSqlite.DatabaseSync(viaScript);
      db.exec(script("perf-fork-005-down.sql"));
    }
    {
      using db = new NodeSqlite.DatabaseSync(viaScript);
      db.exec(script("perf-fork-004-down.sql"));
    }
    const state = (dbPath: string) => {
      using db = new NodeSqlite.DatabaseSync(dbPath);
      return {
        objects: db
          .prepare("SELECT type, name FROM sqlite_master ORDER BY type, name")
          .all()
          .map((row) => ({ ...row })),
        ledger: db
          .prepare(
            `SELECT migration_id, name FROM ${PERF_FORK_MIGRATIONS_TABLE} ORDER BY migration_id`,
          )
          .all()
          .map((row) => ({ ...row })),
        metadata: db
          .prepare(
            "SELECT projection_name FROM orchestration_v2_projection_metadata WHERE projection_name LIKE 'event-log-%' ORDER BY projection_name",
          )
          .all()
          .map((row) => ({ ...row })),
      };
    };
    const effectState = state(viaEffect);
    assert.deepEqual(state(viaScript), effectState);
    assert.deepEqual(
      effectState.ledger.map((row) => row.migration_id),
      BEFORE_004_CHAIN.map(([id]) => id),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

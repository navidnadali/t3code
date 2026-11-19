import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import {
  EVENT_LOG_PRUNE_CURSOR,
  EVENT_LOG_REFERENCE_SCAN,
  EVENT_LOG_REFERENCE_SCAN_FROM,
} from "../OrchestrationEventStore.ts";

/** The event-log compaction's own rows: its fork migration is named, not numbered, in this file. */
export const NAME = "EventLogPruneState";
export const TABLE = "orchestration_events_prune_state";

/**
 * Event rows the compaction keeps for a reason that can lapse (a stream's newest row, a prompt
 * anchor), and the largest scanned sequence referencing each anchor. The compaction's progress
 * rows describe this table's contents, so a table created anew (first run, or after it was lost)
 * clears them in the same transaction: classification and the reference scan then start over.
 */
const ensure = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const present = yield* sql`
    SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ${TABLE}
  `.pipe(NodeSqliteClient.readOnly);
  if (present.length > 0) return;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql.unsafe(`
        CREATE TABLE IF NOT EXISTS ${TABLE} (
          sequence INTEGER PRIMARY KEY,
          held INTEGER NOT NULL DEFAULT 0,
          prompt_sequence INTEGER,
          max_reference INTEGER
        )
      `);
      yield* sql`
        DELETE FROM orchestration_v2_projection_metadata
        WHERE projection_name IN ${sql.in([
          EVENT_LOG_PRUNE_CURSOR,
          EVENT_LOG_REFERENCE_SCAN,
          EVENT_LOG_REFERENCE_SCAN_FROM,
        ])}
      `;
    }),
  );
});

// Each pass calls it too (ProjectionMaintenance), so doctor's offline compaction finds the table.
export default ensure;

/**
 * Rolls the migration back for a build without it, offline: the table, its ledger row and the
 * progress naming its rows go; the boundary rows stay, read the same by every build.
 */
export const down = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql.unsafe(`DROP TABLE IF EXISTS ${TABLE}`);
      yield* sql`DELETE FROM t3_perf_fork_migrations WHERE name = ${NAME}`;
      yield* sql`
        DELETE FROM orchestration_v2_projection_metadata
        WHERE projection_name IN ${sql.in([
          EVENT_LOG_PRUNE_CURSOR,
          EVENT_LOG_REFERENCE_SCAN,
          EVENT_LOG_REFERENCE_SCAN_FROM,
        ])}
      `;
    }),
  );
});

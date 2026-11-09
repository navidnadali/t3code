import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { driftedTurnItemCountThreads } from "./turnItemCounts.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

export interface ProjectionIntegrityVerification {
  readonly valid: boolean;
  readonly schemaVersion: number;
  readonly expectedSequence: number;
  readonly projectionSequence: number;
  readonly unreadableThreadIds: ReadonlyArray<ThreadId>;
  readonly missingThreadIds: ReadonlyArray<ThreadId>;
  readonly turnItemCountDriftThreadIds: ReadonlyArray<ThreadId>;
  readonly unexpectedThreadIds: ReadonlyArray<ThreadId>;
}

type ProjectionMetadataRow = { readonly schema_version: number; readonly last_sequence: number };

/** Checks persisted coverage and readability without rebuilding or requiring pruned event bodies. */
export const verifyProjectionIntegrity = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventStore = yield* EventStore.EventStoreV2;
  const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
  const expectedThreadRows = yield* sql<{ readonly thread_id: string }>`
        SELECT DISTINCT stream_id AS thread_id
        FROM orchestration_events INDEXED BY orchestration_events_v2_created_threads_idx
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND event_type = 'thread.created'
        ORDER BY stream_id ASC
      `;
  const projectionRows = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id
        FROM orchestration_v2_projection_threads
        ORDER BY thread_id ASC
      `;
  const actualIds = projectionRows.map((row) => ThreadId.make(row.thread_id));
  const expectedIds = expectedThreadRows.map((row) => ThreadId.make(row.thread_id));
  const actualSet = new Set(actualIds);
  const expectedSet = new Set(expectedIds);
  const missingThreadIds = expectedIds.filter((threadId) => !actualSet.has(threadId));
  const unexpectedThreadIds = actualIds.filter((threadId) => !expectedSet.has(threadId));
  const turnItemCountDriftThreadIds = (yield* driftedTurnItemCountThreads(sql)).map((row) =>
    ThreadId.make(row.thread_id),
  );
  const unreadableThreadIds = yield* projectionStore.getUnreadableThreadIds();
  const metadata = yield* sql<ProjectionMetadataRow>`
        SELECT schema_version, last_sequence
        FROM orchestration_v2_projection_metadata
        WHERE projection_name = 'thread-projections'
        LIMIT 1
      `;
  const expectedSequence = yield* eventStore.latestSequence();
  const schemaVersion = metadata[0]?.schema_version ?? 0;
  const projectionSequence = metadata[0]?.last_sequence ?? 0;
  return {
    valid:
      (schemaVersion === ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION ||
        expectedSequence === 0) &&
      projectionSequence === expectedSequence &&
      missingThreadIds.length === 0 &&
      unexpectedThreadIds.length === 0 &&
      turnItemCountDriftThreadIds.length === 0 &&
      unreadableThreadIds.length === 0,
    schemaVersion,
    expectedSequence,
    projectionSequence,
    turnItemCountDriftThreadIds,
    unreadableThreadIds,
    missingThreadIds,
    unexpectedThreadIds,
  } satisfies ProjectionIntegrityVerification;
});

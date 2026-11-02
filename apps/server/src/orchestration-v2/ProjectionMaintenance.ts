// @effect-diagnostics nodeBuiltinImport:off - the compaction scratch store is a native SQLite handle; its file is removed with it.
import { ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

export interface ProjectionVerificationV2 {
  readonly valid: boolean;
  readonly schemaVersion: number;
  readonly expectedSequence: number;
  readonly projectionSequence: number;
  readonly unreadableThreadIds: ReadonlyArray<ThreadId>;
  readonly missingThreadIds: ReadonlyArray<ThreadId>;
  readonly unexpectedThreadIds: ReadonlyArray<ThreadId>;
  readonly turnItemCountDriftThreadIds: ReadonlyArray<ThreadId>;
}

export class ProjectionMaintenanceError extends Schema.TaggedError<ProjectionMaintenanceError>()(
  "ProjectionMaintenanceError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface CompactEventStoreOptions {
  /**
   * File for the pass's working set (every keyed entity's oldest candidate so far), replaced at
   * every pass start and removed at its end; in memory when absent. Nothing in it outlives a pass.
   */
  readonly scratchPath?: string | undefined;
}

export interface ProjectionMaintenanceV2Shape {
  readonly verify: Effect.Effect<ProjectionVerificationV2, ProjectionMaintenanceError>;
  readonly rebuild: Effect.Effect<ProjectionVerificationV2, ProjectionMaintenanceError>;
  readonly compactEventStore: (
    betweenPages?: Effect.Effect<void>,
    options?: CompactEventStoreOptions,
  ) => Effect.Effect<
    {
      readonly deletedEventCount: number;
      readonly deletedReceiptCount: number;
      readonly reclaimableBytes: number;
      /** Keyed entities the pass saw; their state was on disk, not in the JS heap. */
      readonly trackedKeys: number;
    },
    ProjectionMaintenanceError
  >;
}

export class ProjectionMaintenanceV2 extends Context.Service<
  ProjectionMaintenanceV2,
  ProjectionMaintenanceV2Shape
>()("t3/orchestration-v2/ProjectionMaintenance/ProjectionMaintenanceV2") {}

type ProjectionMetadataRow = {
  readonly schema_version: number;
  readonly last_sequence: number;
};

const encodeEntityKey = Schema.encodeSync(
  Schema.fromJsonString(Schema.Tuple([Schema.String, Schema.String, Schema.NullOr(Schema.String)])),
);

// The scratch store keeps the pass's working set out of the heap: SQLite caches this much of it,
// the rest lives in the file (measured on a 2 GB store: 32 MiB costs ≈ 0.2 ms per page over 8 MiB's 1.4 ms).
const SCRATCH_CACHE_KIB = 32768;
const SCRATCH_PAGE_SIZE = 8192;

const removeScratchFiles = (scratchPath: string | undefined) => {
  if (scratchPath === undefined) return;
  for (const file of [scratchPath, `${scratchPath}-wal`, `${scratchPath}-shm`]) {
    NodeFS.rmSync(file, { force: true });
  }
};

type CompactionScratch = {
  readonly lookup: (keys: ReadonlyArray<string>) => Map<string, number | null>;
  readonly upsert: (entries: ReadonlyArray<readonly [string, number | null]>) => void;
};

const scratchError = (cause: unknown) =>
  new ProjectionMaintenanceError({ operation: "compaction scratch store", cause });

/** Opens the pass's working-set store; whatever a previous pass left there is replaced, never read. */
const openScratch = (scratchPath: string | undefined) =>
  Effect.acquireRelease(
    Effect.try({
      try: (): CompactionScratch & { readonly db: NodeSqlite.DatabaseSync } => {
        removeScratchFiles(scratchPath);
        const db = new NodeSqlite.DatabaseSync(scratchPath ?? ":memory:");
        try {
          // Scratch data, one transaction for the whole pass and never committed: no fsync, and
          // pages beyond the cache spill to the WAL once rather than at every page's commit.
          db.exec(`
            PRAGMA page_size = ${SCRATCH_PAGE_SIZE};
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = OFF;
            PRAGMA cache_size = -${SCRATCH_CACHE_KIB};
            CREATE TABLE candidates (entity_key TEXT PRIMARY KEY, oldest INTEGER) WITHOUT ROWID;
            BEGIN;
          `);
          const select = db.prepare(`
            SELECT entity_key, oldest FROM candidates
            WHERE entity_key IN (SELECT value FROM json_each(?))
          `);
          const upsert = db.prepare(`
            INSERT INTO candidates (entity_key, oldest)
            SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]') FROM json_each(?) WHERE true
            ON CONFLICT (entity_key) DO UPDATE SET oldest = excluded.oldest
          `);
          return {
            db,
            lookup: (keys) => {
              const found = new Map<string, number | null>();
              if (keys.length === 0) return found;
              for (const row of select.all(JSON.stringify(keys))) {
                found.set(String(row.entity_key), row.oldest === null ? null : Number(row.oldest));
              }
              return found;
            },
            upsert: (entries) => {
              if (entries.length > 0) upsert.run(JSON.stringify(entries));
            },
          };
        } catch (cause) {
          db.close();
          throw cause;
        }
      },
      catch: scratchError,
    }),
    ({ db }) =>
      Effect.sync(() => {
        db.close();
        removeScratchFiles(scratchPath);
      }),
  );

export const layer: Layer.Layer<
  ProjectionMaintenanceV2,
  never,
  EventStore.EventStoreV2 | ProjectionStore.ProjectionStoreV2 | SqlClient.SqlClient
> = Layer.effect(
  ProjectionMaintenanceV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const eventStore = yield* EventStore.EventStoreV2;
    const projectionStore = yield* ProjectionStore.ProjectionStoreV2;

    // Fork migration 1 adds the maintained counts; maintenance also runs on older schemas.
    const hasTurnItemCounts = sql`
      SELECT 1 FROM sqlite_schema
      WHERE type = 'table' AND name = 'orchestration_v2_projection_turn_item_counts'
    `.pipe(Effect.map((rows) => rows.length > 0));
    // Threads whose maintained counts differ from a COUNT(*) of their items, either way.
    const selectTurnItemCountDrift = sql<{ readonly thread_id: string }>`
      WITH stored AS (
        SELECT thread_id, run_id, item_count
        FROM orchestration_v2_projection_turn_item_counts
      ),
      counted AS (
        SELECT thread_id, COALESCE(run_id, '') AS run_id, SUM(item_count) AS item_count
        FROM (
          SELECT thread_id, run_id, COUNT(*) AS item_count
          FROM orchestration_v2_projection_turn_items
          GROUP BY thread_id, run_id
        )
        GROUP BY thread_id, COALESCE(run_id, '')
      )
      SELECT DISTINCT thread_id
      FROM (
        SELECT * FROM (SELECT * FROM stored EXCEPT SELECT * FROM counted)
        UNION ALL
        SELECT * FROM (SELECT * FROM counted EXCEPT SELECT * FROM stored)
      )
      ORDER BY thread_id ASC
    `;

    const verify = Effect.gen(function* () {
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
      const unreadableThreadIds = yield* projectionStore.getUnreadableThreadIds();
      const turnItemCountDriftThreadIds = (yield* hasTurnItemCounts)
        ? (yield* selectTurnItemCountDrift).map((row) => ThreadId.make(row.thread_id))
        : [];
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
          schemaVersion === ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION &&
          projectionSequence === expectedSequence &&
          missingThreadIds.length === 0 &&
          unexpectedThreadIds.length === 0 &&
          unreadableThreadIds.length === 0 &&
          turnItemCountDriftThreadIds.length === 0,
        schemaVersion,
        expectedSequence,
        projectionSequence,
        unreadableThreadIds,
        missingThreadIds,
        unexpectedThreadIds,
        turnItemCountDriftThreadIds,
      } satisfies ProjectionVerificationV2;
    }).pipe(sql.withTransaction);

    const rebuild = Effect.gen(function* () {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const throughSequence = yield* eventStore.latestSequence();
          yield* sql`DELETE FROM orchestration_v2_projection_context_transfers`;
          yield* sql`DELETE FROM orchestration_v2_projection_context_handoffs`;
          yield* sql`DELETE FROM orchestration_v2_projection_checkpoints`;
          yield* sql`DELETE FROM orchestration_v2_projection_checkpoint_scopes`;
          // Replay recounts through the turn-item triggers, from zero.
          if (yield* hasTurnItemCounts) {
            yield* sql`DELETE FROM orchestration_v2_projection_turn_item_counts`;
          }
          yield* sql`DELETE FROM orchestration_v2_projection_turn_items`;
          yield* sql`DELETE FROM orchestration_v2_projection_plans`;
          yield* sql`DELETE FROM orchestration_v2_projection_messages`;
          yield* sql`DELETE FROM orchestration_v2_projection_runtime_requests`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_turns`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_threads`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings`;
          yield* sql`DELETE FROM orchestration_v2_projection_provider_sessions`;
          yield* sql`DELETE FROM orchestration_v2_projection_subagents`;
          yield* sql`DELETE FROM orchestration_v2_projection_nodes`;
          yield* sql`DELETE FROM orchestration_v2_projection_run_attempts`;
          yield* sql`DELETE FROM orchestration_v2_projection_runs`;
          yield* sql`DELETE FROM orchestration_v2_projection_threads`;
          yield* sql`DELETE FROM orchestration_v2_turn_item_positions`;

          const pageSize = 500;
          let lastSequence = 0;
          while (true) {
            const page = yield* eventStore
              .read({ afterSequence: lastSequence, throughSequence, limit: pageSize })
              .pipe(Stream.runCollect);
            for (const stored of page) {
              yield* projectionStore.apply(stored.event);
              if (stored.event.type === "turn-item.updated") {
                yield* sql`
                INSERT INTO orchestration_v2_turn_item_positions (
                  thread_id,
                  turn_item_id,
                  ordinal
                )
                VALUES (
                  ${stored.event.threadId},
                  ${stored.event.payload.id},
                  ${stored.event.payload.ordinal}
                )
                ON CONFLICT(thread_id, turn_item_id) DO UPDATE SET
                  ordinal = excluded.ordinal
                `;
              }
              lastSequence = stored.sequence;
            }
            if (page.length < pageSize) break;
            yield* Effect.yieldNow;
          }
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`
            INSERT INTO orchestration_v2_projection_metadata (
              projection_name,
              schema_version,
              last_sequence,
              updated_at
            )
            VALUES (
              'thread-projections',
              ${ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
              ${lastSequence},
              ${now}
            )
            ON CONFLICT(projection_name) DO UPDATE SET
              schema_version = excluded.schema_version,
              last_sequence = excluded.last_sequence,
              updated_at = excluded.updated_at
          `;
        }),
      );
      return yield* verify;
    });

    const mapError =
      (operation: string) =>
      <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.mapError((cause) => new ProjectionMaintenanceError({ operation, cause })),
        );

    // Keep thread.created as the verification and replay anchor; clients recognize deletion by thread.deleted event type.
    const SUPERSEDABLE_THREAD_EVENT_TYPES = [
      "thread.archived",
      "thread.unarchived",
      "thread.settled",
      "thread.unsettled",
      "thread.snoozed",
      "thread.unsnoozed",
      "thread.pinned",
      "thread.auto-settle-set",
      "thread.unpinned",
      "thread.pin-reordered",
      "thread.active-reordered",
      "thread.metadata-updated",
      "thread.pull-request-synced",
      "thread.runtime-mode-updated",
      "thread.interaction-mode-updated",
      "thread.model-selection-updated",
      "thread.provider-switched",
      "thread.guest-share-updated",
    ];
    // Read-state events preserve updatedAt and supersede only each other. Activity events supersede both groups.
    const READ_STATE_THREAD_EVENT_TYPES = ["thread.visited", "thread.marked-unread"];
    // Older clients skip tag events, so these supersede only each other.
    const TAG_THREAD_EVENT_TYPE = "thread.tags-updated";

    const supersedableThreadEventTypes = new Set(SUPERSEDABLE_THREAD_EVENT_TYPES);
    const readStateThreadEventTypes = new Set(READ_STATE_THREAD_EVENT_TYPES);
    // Subagent snapshots stay intact because later rows can reference an earlier prompt.
    // Commanded turn items stay for command-result replay.
    // Small pages bound work on the reader worker and synchronous fallback.
    const COMPACTION_PAGE_SIZE = 50;

    // Keep first/latest keyed events for replay order; page every event to bound sparse scans.
    const compactEventStore = Effect.fn("ProjectionMaintenance.compactEventStore")(
      function* (
        betweenPages: Effect.Effect<void> = Effect.void,
        options?: CompactEventStoreOptions,
      ) {
        const bounds = yield* sql<{
          readonly event_sequence: number;
          readonly receipt_row_id: number;
        }>`
        SELECT
          COALESCE((SELECT MAX(sequence) FROM orchestration_events), 0) AS event_sequence,
          COALESCE((SELECT MAX(rowid) FROM orchestration_command_receipts), 0) AS receipt_row_id
      `.pipe(NodeSqliteClient.readOnly);
        let throughSequence = bounds[0]?.event_sequence ?? 0;
        let throughReceiptRowId = bounds[0]?.receipt_row_id ?? 0;
        // One entry per thread; the per-entity working set is on disk (`openScratch`).
        const retainedThreadIds = new Set<string>();
        const retainedReadStateThreadIds = new Set<string>();
        const retainedTagThreadIds = new Set<string>();
        const scratch = yield* openScratch(options?.scratchPath);
        let deletedEventCount = 0;
        let deletedReceiptCount = 0;
        let trackedKeys = 0;

        while (throughSequence > 0) {
          const rows = yield* sql<{
            readonly sequence: number;
            readonly application_event_version: number;
            readonly aggregate_kind: string;
            readonly stream_id: string;
            readonly event_type: string;
            readonly entity_id: string | null;
            readonly imported_legacy_thread: number;
          }>`
          SELECT
            event.sequence,
            event.application_event_version,
            event.aggregate_kind,
            event.stream_id,
            event.event_type,
            CASE
              WHEN event.application_event_version = 2
                AND event.event_type IN ('message.updated', 'node.updated')
              THEN json_extract(event.payload_json, '$.id')
              WHEN event.application_event_version = 2
                AND event.event_type = 'turn-item.updated'
                AND event.command_id IS NULL
                AND json_extract(event.payload_json, '$.type') <> 'subagent'
              THEN json_extract(event.payload_json, '$.id')
              WHEN event.application_event_version = 2
                AND event.event_type = 'thread.pull-request-link-updated'
              THEN json_array(json_extract(event.payload_json, '$.host'),
                json_extract(event.payload_json, '$.repository'),
                json_extract(event.payload_json, '$.number'))
              ELSE NULL
            END AS entity_id,
            CASE
              WHEN event.application_event_version = 1 AND event.aggregate_kind = 'thread'
              THEN EXISTS (
                SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import
                WHERE legacy_import.thread_id = event.stream_id
                  AND legacy_import.transcript_imported_at IS NOT NULL
              )
              ELSE 0
            END AS imported_legacy_thread
          FROM orchestration_events AS event
          WHERE event.sequence <= ${throughSequence}
          ORDER BY event.sequence DESC
          LIMIT ${COMPACTION_PAGE_SIZE + 1}
        `.pipe(NodeSqliteClient.readOnly);
          const page = rows.slice(0, COMPACTION_PAGE_SIZE);
          const obsolete: number[] = [];
          // Retain each key's oldest candidate, deleting it only when an older event appears. The
          // page's keys are read from the scratch store at once and its decisions written back at once.
          const keyOf = (row: (typeof page)[number]) =>
            row.application_event_version === 2 &&
            (row.event_type === "thread.pull-request-link-updated" ||
              row.event_type === "message.updated" ||
              row.event_type === "node.updated" ||
              (row.event_type === "turn-item.updated" && row.entity_id !== null))
              ? encodeEntityKey([row.event_type, row.stream_id, row.entity_id])
              : undefined;
          const keys = page.map(keyOf);
          const oldestCandidateByEntityKey = yield* Effect.try({
            try: () => scratch.lookup(keys.filter((key) => key !== undefined)),
            catch: scratchError,
          });
          const changes = new Map<string, number | null>();
          for (const [index, row] of page.entries()) {
            if (row.imported_legacy_thread === 1) {
              obsolete.push(row.sequence);
            } else if (row.application_event_version === 2) {
              if (
                row.aggregate_kind === "thread" &&
                supersedableThreadEventTypes.has(row.event_type)
              ) {
                if (retainedThreadIds.has(row.stream_id)) obsolete.push(row.sequence);
                else retainedThreadIds.add(row.stream_id);
              } else if (
                row.aggregate_kind === "thread" &&
                readStateThreadEventTypes.has(row.event_type)
              ) {
                if (
                  retainedThreadIds.has(row.stream_id) ||
                  retainedReadStateThreadIds.has(row.stream_id)
                ) {
                  obsolete.push(row.sequence);
                } else retainedReadStateThreadIds.add(row.stream_id);
              } else if (
                row.aggregate_kind === "thread" &&
                row.event_type === TAG_THREAD_EVENT_TYPE
              ) {
                if (
                  retainedThreadIds.has(row.stream_id) ||
                  retainedTagThreadIds.has(row.stream_id)
                ) {
                  obsolete.push(row.sequence);
                } else retainedTagThreadIds.add(row.stream_id);
              } else if (
                row.event_type === "thread.pull-request-link-updated" &&
                retainedThreadIds.has(row.stream_id)
              ) {
                // A newer whole-thread payload already carries this link's state.
                obsolete.push(row.sequence);
              } else if (
                row.event_type === "thread.pull-request-link-updated" ||
                row.event_type === "message.updated" ||
                row.event_type === "node.updated" ||
                (row.event_type === "turn-item.updated" && row.entity_id !== null)
              ) {
                const key = keys[index]!;
                const candidate = oldestCandidateByEntityKey.get(key);
                let next: number | null;
                if (candidate === undefined) {
                  next = null;
                  trackedKeys += 1;
                } else {
                  if (candidate !== null) obsolete.push(candidate);
                  next = row.sequence;
                }
                oldestCandidateByEntityKey.set(key, next);
                changes.set(key, next);
              }
            }
          }
          yield* Effect.try({ try: () => scratch.upsert([...changes]), catch: scratchError });
          // Never hold a transaction or the reader lock across a yield or a rest.
          yield* Effect.yieldNow;
          if (obsolete.length > 0) {
            yield* sql`DELETE FROM orchestration_events WHERE sequence IN ${sql.in(obsolete)}`;
            deletedEventCount += obsolete.length;
            yield* Effect.yieldNow;
          }
          throughSequence =
            rows.length <= COMPACTION_PAGE_SIZE ? 0 : (page.at(-1)?.sequence ?? 1) - 1;
          if (throughSequence > 0 || throughReceiptRowId > 0) yield* betweenPages;
        }

        while (throughReceiptRowId > 0) {
          const rows = yield* sql<{
            readonly row_id: number;
            readonly command_id: string;
            readonly imported_legacy_thread: number;
          }>`
          SELECT
            receipt.rowid AS row_id,
            receipt.command_id,
            CASE
              WHEN receipt.command_type = 'legacy' AND receipt.aggregate_kind = 'thread'
              THEN EXISTS (
                SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import
                WHERE legacy_import.thread_id = receipt.aggregate_id
                  AND legacy_import.transcript_imported_at IS NOT NULL
              )
              ELSE 0
            END AS imported_legacy_thread
          FROM orchestration_command_receipts AS receipt
          WHERE receipt.rowid <= ${throughReceiptRowId}
          ORDER BY receipt.rowid DESC
          LIMIT ${COMPACTION_PAGE_SIZE + 1}
        `.pipe(NodeSqliteClient.readOnly);
          const page = rows.slice(0, COMPACTION_PAGE_SIZE);
          const obsolete = page
            .filter((row) => row.imported_legacy_thread === 1)
            .map((row) => row.command_id);
          yield* Effect.yieldNow;
          if (obsolete.length > 0) {
            yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id IN ${sql.in(obsolete)}`;
            deletedReceiptCount += obsolete.length;
            yield* Effect.yieldNow;
          }
          throughReceiptRowId = (page.at(-1)?.row_id ?? 1) - 1;
          if (rows.length <= COMPACTION_PAGE_SIZE) break;
          yield* betweenPages;
        }

        const freelistRows = yield* sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`;
        const pageSizeRows = yield* sql<{ readonly page_size: number }>`PRAGMA page_size`;
        const reclaimableBytes =
          (freelistRows[0]?.freelist_count ?? 0) * (pageSizeRows[0]?.page_size ?? 0);

        return { deletedEventCount, deletedReceiptCount, reclaimableBytes, trackedKeys };
      },
      Effect.scoped,
      mapError("compact event store"),
    );

    return ProjectionMaintenanceV2.of({
      verify: mapError("verify")(verify),
      rebuild: mapError("rebuild")(rebuild),
      compactEventStore,
    });
  }),
);

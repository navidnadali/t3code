import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Statement from "effect/sql/Statement";

import {
  EVENT_LOG_PRUNE_CURSOR,
  EVENT_LOG_PRUNE_MARK,
  EVENT_LOG_PRUNED_THROUGH,
  EVENT_LOG_REFERENCE_SCAN,
  EVENT_LOG_REFERENCE_SCAN_FROM,
} from "../persistence/OrchestrationEventStore.ts";
import ensurePruneState, {
  TABLE as PRUNE_STATE,
} from "../persistence/PerfForkMigrations/EventLogPruneState.ts";
import { eventRetentionSql } from "./EventRetention.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

export class ProjectionMaintenanceError extends Schema.TaggedError<ProjectionMaintenanceError>()(
  "ProjectionMaintenanceError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface ProjectionMaintenanceV2Shape {
  /**
   * Deletes events the projections already cover once they are older than the retained history.
   * `betweenPages` paces the pass; `beforeWrite` runs before each write that deletes or records
   * progress, and may wait (for disk headroom, say). Progress is kept in the database, so a pass cut
   * short resumes where it stopped.
   */
  readonly pruneEventStore: (
    betweenPages?: Effect.Effect<void>,
    beforeWrite?: Effect.Effect<void>,
  ) => Effect.Effect<
    {
      readonly deletedEventCount: number;
      readonly deletedReceiptCount: number;
      readonly reclaimableBytes: number;
    },
    ProjectionMaintenanceError
  >;
}

export class ProjectionMaintenanceV2 extends Context.Service<
  ProjectionMaintenanceV2,
  ProjectionMaintenanceV2Shape
>()("t3/orchestration-v2/ProjectionMaintenance/ProjectionMaintenanceV2") {}

// Reconnect replay and command-id retries read this tail; older cursors resync from a snapshot.
const RETAINED_EVENT_HISTORY = Duration.days(1);
// Small pages bound work on the reader worker and synchronous fallback.
const PAGE_SIZE = 50;
const REFERENCE_SCAN_WINDOW = 1000;
// Progress is written at least this often, so a restart repeats at most this much reading.
const FLUSH_ROWS = 500;
const FLUSH_INTERVAL = Duration.seconds(30);
// A long pass checks the mark again this often, so a new boundary still comes a day after its mark.
const BOUNDARY_REFRESH = Duration.minutes(10);
// One delete holds the writer about this long: the most a foreground write waits behind it.
const DELETE_HOLD_TARGET_MS = 10;
const DELETE_BATCH_MIN = 5;
const DELETE_BATCH_MAX = 500;
// The writer reads rows committed after the reference scan; past this many, the scan catches up first.
const UNSCANNED_LIMIT = 2_000;

type MetadataRow = {
  readonly projection_name: string;
  readonly last_sequence: number;
  readonly updated_at: string;
};

type Boundary = {
  /** Events at or below it may be pruned (published as pruned-through). */
  readonly through: number;
  /** The newest mark: the next boundary once it is a day old. */
  readonly mark: number;
  readonly head: number;
  readonly receiptRowId: number;
};

export const layer: Layer.Layer<ProjectionMaintenanceV2, never, SqlClient.SqlClient> = Layer.effect(
  ProjectionMaintenanceV2,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    const writeMetadata = (name: string, sequence: number, at: DateTime.Utc) => sql`
      INSERT INTO orchestration_v2_projection_metadata (
        projection_name, schema_version, last_sequence, updated_at
      )
      VALUES (
        ${name},
        ${ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION},
        ${sequence},
        ${DateTime.formatIso(at)}
      )
      ON CONFLICT(projection_name) DO UPDATE SET
        last_sequence = excluded.last_sequence,
        updated_at = excluded.updated_at
    `;

    const readMetadata = (names: ReadonlyArray<string>) =>
      sql<MetadataRow>`
        SELECT projection_name, last_sequence, updated_at
        FROM orchestration_v2_projection_metadata
        WHERE projection_name IN ${sql.in(names)}
      `.pipe(
        NodeSqliteClient.readOnly,
        NodeSqliteClient.historyRead,
        Effect.map((rows) => new Map(rows.map((row) => [row.projection_name, row]))),
      );

    // A mark (H, T) says every event at or below H was committed by T: T is read before H, and a
    // write in flight takes sequences above H. Pruning through a mark only once it is a day old keeps
    // the retained day without commit times. The mark is written as the due pass starts, so a pass a
    // restart cuts short still lets the next boundary come a day later.
    const refreshBoundary = Effect.fnUntraced(function* (beforeWrite: Effect.Effect<void>) {
      const now = yield* DateTime.now;
      const metadata = yield* readMetadata([
        "thread-projections",
        EVENT_LOG_PRUNE_MARK,
        EVENT_LOG_PRUNED_THROUGH,
      ]);
      const bounds = yield* sql<{ readonly sequence: number; readonly receipt_row_id: number }>`
        SELECT
          COALESCE((SELECT MAX(sequence) FROM orchestration_events), 0) AS sequence,
          COALESCE((SELECT MAX(rowid) FROM orchestration_command_receipts), 0) AS receipt_row_id
      `.pipe(NodeSqliteClient.readOnly, NodeSqliteClient.historyRead);
      const head = bounds[0]?.sequence ?? 0;
      const projected = metadata.get("thread-projections")?.last_sequence ?? 0;
      const mark = metadata.get(EVENT_LOG_PRUNE_MARK);
      let through = metadata.get(EVENT_LOG_PRUNED_THROUGH)?.last_sequence ?? 0;
      const markIsDue =
        mark !== undefined &&
        DateTime.toEpochMillis(now) - Date.parse(mark.updated_at) >=
          Duration.toMillis(RETAINED_EVENT_HISTORY);
      if (mark !== undefined && markIsDue) {
        const next = Math.max(Math.min(mark.last_sequence, projected), through);
        if (next > through) {
          // Published before any delete at or below it: a resuming client takes a snapshot.
          yield* beforeWrite;
          yield* writeMetadata(EVENT_LOG_PRUNED_THROUGH, next, now);
          through = next;
        }
      }
      let markSequence = mark?.last_sequence ?? 0;
      if (mark === undefined || markIsDue) {
        yield* beforeWrite;
        yield* writeMetadata(EVENT_LOG_PRUNE_MARK, head, now);
        markSequence = head;
      }
      return {
        through,
        mark: markSequence,
        head,
        receiptRowId: bounds[0]?.receipt_row_id ?? 0,
      } satisfies Boundary;
    });

    const retention = eventRetentionSql("orchestration_events");
    const keptForGood = sql.literal(retention.keptForGood);
    const isStreamHead = sql.literal(retention.isStreamHead);
    const isPromptAnchor = sql.literal(retention.isPromptAnchor);
    const borrowedPrompt = sql.literal(retention.borrowedPrompt);

    type Classified = {
      readonly sequence: number;
      readonly kept: number;
      readonly head: number;
      readonly anchor: number;
      readonly prompt_sequence: number | null;
    };
    const classified = (where: Statement.Fragment, limit: number) =>
      sql<Classified>`
        SELECT
          event.sequence,
          ${keptForGood} AS kept,
          CASE WHEN ${keptForGood} = 1 THEN 0 ELSE ${isStreamHead} END AS head,
          CASE WHEN ${keptForGood} = 1 THEN 0 WHEN ${isPromptAnchor} THEN 1 ELSE 0 END AS anchor,
          ${borrowedPrompt} AS prompt_sequence
        FROM orchestration_events AS event
        WHERE ${where}
        ORDER BY event.sequence ASC
        LIMIT ${limit}
      `.pipe(NodeSqliteClient.readOnly, NodeSqliteClient.historyRead);

    const latestSequence = sql<{ readonly sequence: number }>`
      SELECT COALESCE(MAX(sequence), 0) AS sequence FROM orchestration_events
    `.pipe(
      NodeSqliteClient.readOnly,
      Effect.map((rows) => rows[0]?.sequence ?? 0),
    );

    const recordHeld = (rows: ReadonlyArray<readonly [number, number | null]>) =>
      sql.unsafe(
        `INSERT INTO ${PRUNE_STATE} (sequence, held, prompt_sequence)
         SELECT json_extract(value, '$[0]'), 1, json_extract(value, '$[1]') FROM json_each(?) WHERE true
         ON CONFLICT(sequence) DO UPDATE SET held = 1, prompt_sequence = excluded.prompt_sequence`,
        [JSON.stringify(rows)],
      );

    const recordReferences = (references: ReadonlyMap<number, number>) =>
      sql.unsafe(
        `INSERT INTO ${PRUNE_STATE} (sequence, held, max_reference)
         SELECT json_extract(value, '$[0]'), 0, json_extract(value, '$[1]') FROM json_each(?) WHERE true
         ON CONFLICT(sequence) DO UPDATE SET
           max_reference = MAX(COALESCE(max_reference, 0), excluded.max_reference)`,
        [JSON.stringify([...references])],
      );

    // Imported legacy threads are served from v2 rows; their v1 events and receipts have no reader.
    const pruneImportedLegacyHistory = Effect.fnUntraced(function* <E>(
      throughSequence: number,
      throughReceiptRowId: number,
      betweenPages: Effect.Effect<void, E>,
      beforeWrite: Effect.Effect<void>,
    ) {
      let deletedEventCount = 0;
      let deletedReceiptCount = 0;
      for (let cursor = 0; ;) {
        const rows = yield* sql<{ readonly sequence: number; readonly imported: number }>`
          SELECT
            event.sequence,
            event.aggregate_kind = 'thread' AND EXISTS (
              SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import
              WHERE legacy_import.thread_id = event.stream_id
                AND legacy_import.transcript_imported_at IS NOT NULL
            ) AS imported
          FROM orchestration_events AS event
            INDEXED BY idx_orchestration_events_application_sequence
          WHERE event.application_event_version = 1
            AND event.sequence > ${cursor}
            AND event.sequence <= ${throughSequence}
          ORDER BY event.sequence ASC
          LIMIT ${PAGE_SIZE}
        `.pipe(NodeSqliteClient.readOnly, NodeSqliteClient.historyRead);
        const obsolete = rows.filter((row) => row.imported === 1).map((row) => row.sequence);
        yield* Effect.yieldNow;
        if (obsolete.length > 0) {
          yield* beforeWrite;
          yield* sql`DELETE FROM orchestration_events WHERE sequence IN ${sql.in(obsolete)}`;
          deletedEventCount += obsolete.length;
          yield* Effect.yieldNow;
        }
        yield* betweenPages;
        if (rows.length < PAGE_SIZE) break;
        cursor = rows.at(-1)?.sequence ?? cursor;
      }
      for (let cursor = 0; ;) {
        const rows = yield* sql<{
          readonly row_id: number;
          readonly command_id: string;
          readonly imported: number;
        }>`
          SELECT
            receipt.rowid AS row_id,
            receipt.command_id,
            receipt.command_type = 'legacy' AND receipt.aggregate_kind = 'thread' AND EXISTS (
              SELECT 1 FROM orchestration_v2_legacy_imports AS legacy_import
              WHERE legacy_import.thread_id = receipt.aggregate_id
                AND legacy_import.transcript_imported_at IS NOT NULL
            ) AS imported
          FROM orchestration_command_receipts AS receipt
          WHERE receipt.rowid > ${cursor} AND receipt.rowid <= ${throughReceiptRowId}
          ORDER BY receipt.rowid ASC
          LIMIT ${PAGE_SIZE}
        `.pipe(NodeSqliteClient.readOnly, NodeSqliteClient.historyRead);
        const obsolete = rows.filter((row) => row.imported === 1).map((row) => row.command_id);
        yield* Effect.yieldNow;
        if (obsolete.length > 0) {
          yield* beforeWrite;
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id IN ${sql.in(obsolete)}`;
          deletedReceiptCount += obsolete.length;
          yield* Effect.yieldNow;
        }
        yield* betweenPages;
        if (rows.length < PAGE_SIZE) break;
        cursor = rows.at(-1)?.row_id ?? cursor;
      }
      return { deletedEventCount, deletedReceiptCount };
    });

    // A pass: classify (cursor, boundary] once, deleting what nothing keeps and holding heads and
    // prompt anchors; scan forward for prompt references, each row once; check held rows again;
    // prune imported legacy history. Progress is stored, so a restart repeats one flush of reading.
    const pruneEventStore = Effect.fn("ProjectionMaintenance.pruneEventStore")(
      function* (
        betweenPages: Effect.Effect<void> = Effect.void,
        beforeWrite: Effect.Effect<void> = Effect.void,
      ) {
        // Doctor's offline compaction may meet a database no server of this build has migrated.
        yield* beforeWrite;
        yield* ensurePruneState.pipe(Effect.provideService(SqlClient.SqlClient, sql));
        let boundary = yield* refreshBoundary(beforeWrite);
        let boundaryAt = yield* Clock.currentTimeMillis;
        // A long pass checks the mark again between pages, so the next boundary is never late.
        const paced = Effect.gen(function* () {
          yield* betweenPages;
          const now = yield* Clock.currentTimeMillis;
          if (now - boundaryAt < Duration.toMillis(BOUNDARY_REFRESH)) return;
          boundary = yield* refreshBoundary(beforeWrite);
          boundaryAt = now;
        });
        const progress = yield* readMetadata([
          EVENT_LOG_PRUNE_CURSOR,
          EVENT_LOG_REFERENCE_SCAN,
          EVENT_LOG_REFERENCE_SCAN_FROM,
        ]);
        let deletedEventCount = 0;

        // Deletes in batches sized to the hold target. Elapsed time includes any wait for the
        // writer, so the per-row cost is a decaying minimum: waits only ever lengthen a sample.
        let batch = PAGE_SIZE;
        let costPerRowMs: number | undefined;
        const deleteEvents = Effect.fnUntraced(function* (sequences: ReadonlyArray<number>) {
          for (let start = 0; start < sequences.length;) {
            const chunk = sequences.slice(start, start + batch);
            start += chunk.length;
            yield* beforeWrite;
            const started = yield* Clock.currentTimeMillis;
            const deleted = yield* sql<{ readonly sequence: number }>`
              DELETE FROM orchestration_events WHERE sequence IN ${sql.in(chunk)} RETURNING sequence
            `;
            const elapsed = (yield* Clock.currentTimeMillis) - started;
            deletedEventCount += deleted.length;
            const perRow = elapsed / chunk.length;
            costPerRowMs =
              costPerRowMs === undefined ? perRow : Math.min(costPerRowMs * 1.05, perRow);
            batch =
              costPerRowMs <= 0
                ? DELETE_BATCH_MAX
                : Math.min(
                    DELETE_BATCH_MAX,
                    Math.max(DELETE_BATCH_MIN, Math.round(DELETE_HOLD_TARGET_MS / costPerRowMs)),
                  );
            yield* paced;
          }
        });

        let cursor = Math.min(
          progress.get(EVENT_LOG_PRUNE_CURSOR)?.last_sequence ?? 0,
          boundary.through,
        );
        let scan = progress.get(EVENT_LOG_REFERENCE_SCAN)?.last_sequence;
        let scanFrom = progress.get(EVENT_LOG_REFERENCE_SCAN_FROM)?.last_sequence;

        // The reference scan reads forward from where it stopped, recording the largest referencing
        // sequence per anchor; references first, then its position, so the position never runs ahead.
        const scanTo = Effect.fnUntraced(function* (target: number) {
          const references = new Map<number, number>();
          let scanned = scan!;
          let since = yield* Clock.currentTimeMillis;
          const save = Effect.gen(function* () {
            if (scanned === scan) return;
            yield* beforeWrite;
            if (references.size > 0) yield* recordReferences(references);
            references.clear();
            yield* writeMetadata(EVENT_LOG_REFERENCE_SCAN, scanned, yield* DateTime.now);
            scan = scanned;
            since = yield* Clock.currentTimeMillis;
          });
          while (scanned < target) {
            const windowEnd = Math.min(scanned + REFERENCE_SCAN_WINDOW, target);
            const rows = yield* sql<{ readonly anchor: number; readonly reference: number }>`
              SELECT
                json_extract(payload_json, '$.promptSequence') AS anchor,
                MAX(sequence) AS reference
              FROM orchestration_events
              WHERE sequence > ${scanned}
                AND sequence <= ${windowEnd}
                AND event_type IN ('subagent.updated', 'turn-item.updated')
                AND json_type(payload_json, '$.promptSequence') = 'integer'
              GROUP BY anchor
            `.pipe(NodeSqliteClient.readOnly, NodeSqliteClient.historyRead);
            for (const row of rows) {
              references.set(row.anchor, Math.max(references.get(row.anchor) ?? 0, row.reference));
            }
            scanned = windowEnd;
            yield* paced;
            const now = yield* Clock.currentTimeMillis;
            if (
              scanned - scan! >= FLUSH_ROWS * 10 ||
              now - since >= Duration.toMillis(FLUSH_INTERVAL)
            ) {
              yield* save;
            }
          }
          yield* save;
        });

        let legacy:
          | { readonly deletedEventCount: number; readonly deletedReceiptCount: number }
          | undefined;
        // Until a boundary that came due meanwhile is classified too.
        while (true) {
          // 1. Classify and delete. A flush writes deletes, then held rows, then the cursor, so the
          // stored cursor never passes work that is not stored.
          let position = cursor;
          let deletable: Array<number> = [];
          let held: Array<readonly [number, number | null]> = [];
          let lastFlush = yield* Clock.currentTimeMillis;
          const flush = Effect.gen(function* () {
            yield* deleteEvents(deletable);
            deletable = [];
            if (held.length > 0) {
              yield* beforeWrite;
              yield* recordHeld(held);
              held = [];
            }
            if (position > cursor) {
              yield* beforeWrite;
              yield* writeMetadata(EVENT_LOG_PRUNE_CURSOR, position, yield* DateTime.now);
              cursor = position;
            }
            lastFlush = yield* Clock.currentTimeMillis;
          });
          while (position < boundary.through) {
            const rows = yield* classified(
              sql`event.sequence > ${position} AND event.sequence <= ${boundary.through}`,
              PAGE_SIZE,
            );
            for (const row of rows) {
              if (row.kept === 1) continue;
              if (row.head === 1 || row.anchor === 1)
                held.push([row.sequence, row.prompt_sequence]);
              else deletable.push(row.sequence);
            }
            position = rows.length < PAGE_SIZE ? boundary.through : rows.at(-1)!.sequence;
            yield* paced;
            if (deletable.length >= batch) {
              const ready = deletable.splice(0, deletable.length - (deletable.length % batch));
              yield* deleteEvents(ready);
            }
            const now = yield* Clock.currentTimeMillis;
            if (
              position >= boundary.through ||
              position - cursor >= FLUSH_ROWS ||
              now - lastFlush >= Duration.toMillis(FLUSH_INTERVAL)
            ) {
              yield* flush;
            }
          }
          yield* flush;

          // Everything below is relative to this boundary, even if a later one comes due meanwhile:
          // rows above it are not classified yet, so their references still count.
          const through = cursor;

          // 2. The reference scan. Rows at or below the boundary are candidates, not referrers, so a
          // scan behind it jumps to it; first use starts at the newest mark, the next boundary.
          if (scan === undefined || scanFrom === undefined || scan < through) {
            scan = scanFrom = Math.max(through, scan === undefined ? boundary.mark : 0);
            yield* beforeWrite;
            yield* writeMetadata(EVENT_LOG_REFERENCE_SCAN_FROM, scanFrom, yield* DateTime.now);
            yield* writeMetadata(EVENT_LOG_REFERENCE_SCAN, scan, yield* DateTime.now);
          }
          yield* scanTo(yield* latestSequence);

          // 3. Held rows. A head stays while it is its stream's newest row; an anchor stays while a
          // remaining row references it: above the boundary via max_reference (gap-free from a scan
          // start at or below it, read in the delete's transaction), below it a held row's, and rows
          // committed after the scan are read in that transaction too.
          if (scanFrom <= through) {
            const heldRows = yield* sql<{
              readonly sequence: number;
              readonly prompt_sequence: number | null;
            }>`
              SELECT sequence, prompt_sequence FROM ${sql(PRUNE_STATE)}
              WHERE held = 1 ORDER BY sequence ASC
            `.pipe(NodeSqliteClient.readOnly);
            const borrowedByHeld = new Set(
              heldRows.flatMap((row) =>
                row.prompt_sequence === null ? [] : [row.prompt_sequence],
              ),
            );
            for (let start = 0; start < heldRows.length; start += PAGE_SIZE) {
              const page = heldRows.slice(start, start + PAGE_SIZE);
              const current = yield* classified(
                sql`event.sequence IN ${sql.in(page.map((row) => row.sequence))}`,
                PAGE_SIZE,
              );
              yield* paced;
              const byId = new Map(current.map((row) => [row.sequence, row]));
              // Deleted by another build, or kept for good under this rule: no longer held.
              const gone = page
                .filter((row) => !byId.has(row.sequence) || byId.get(row.sequence)?.kept === 1)
                .map((row) => row.sequence);
              const lapsed = page.filter((row) => {
                const now = byId.get(row.sequence);
                if (now === undefined || now.kept === 1 || now.head === 1) return false;
                return now.anchor === 0 || !borrowedByHeld.has(row.sequence);
              });
              if (lapsed.length === 0 && gone.length === 0) continue;
              // The writer reads the rows committed after the scan, so their number is checked
              // again under the writer; past the limit it lets go, the scan catches up, and retries.
              let outcome: { readonly deleted: number } | "catch-up" = "catch-up";
              while (outcome === "catch-up") {
                if ((yield* latestSequence) - scan! > UNSCANNED_LIMIT) {
                  yield* scanTo(yield* latestSequence);
                }
                yield* beforeWrite;
                outcome = yield* sql.withTransaction(
                  Effect.gen(function* () {
                    const tail = yield* sql<{ readonly sequence: number | null }>`
                    SELECT MAX(sequence) AS sequence FROM orchestration_events
                  `;
                    if ((tail[0]?.sequence ?? 0) - scan! > UNSCANNED_LIMIT)
                      return "catch-up" as const;
                    const anchors = lapsed
                      .filter((row) => byId.get(row.sequence)?.anchor === 1)
                      .map((row) => row.sequence);
                    // Referenced from above the boundary (the scan, through any catch-up) or by a row
                    // committed after the scan: no insert interleaves inside this transaction.
                    const referenced = new Set<number>();
                    if (anchors.length > 0) {
                      const scanned = yield* sql<{ readonly sequence: number }>`
                      SELECT sequence FROM ${sql(PRUNE_STATE)}
                      WHERE sequence IN ${sql.in(anchors)} AND COALESCE(max_reference, 0) > ${through}
                    `;
                      const appended = yield* sql<{ readonly anchor: number }>`
                      SELECT DISTINCT json_extract(payload_json, '$.promptSequence') AS anchor
                      FROM orchestration_events
                      WHERE sequence > ${scan!}
                        AND event_type IN ('subagent.updated', 'turn-item.updated')
                        AND json_type(payload_json, '$.promptSequence') = 'integer'
                        AND json_extract(payload_json, '$.promptSequence') IN ${sql.in(anchors)}
                    `;
                      for (const row of scanned) referenced.add(row.sequence);
                      for (const row of appended) referenced.add(row.anchor);
                    }
                    const obsolete = lapsed
                      .map((row) => row.sequence)
                      .filter((sequence) => !referenced.has(sequence));
                    const deleted =
                      obsolete.length === 0
                        ? []
                        : yield* sql<{ readonly sequence: number }>`
                          DELETE FROM orchestration_events WHERE sequence IN ${sql.in(obsolete)}
                          RETURNING sequence
                        `;
                    const settled = [...obsolete, ...gone];
                    if (settled.length > 0) {
                      yield* sql`DELETE FROM ${sql(PRUNE_STATE)} WHERE sequence IN ${sql.in(settled)}`;
                    }
                    return { deleted: deleted.length };
                  }),
                );
              }
              deletedEventCount += outcome.deleted;
              yield* paced;
            }
            // Reference counts for anchors that are classified and not held (kept for good, or gone),
            // found on a reader and deleted a bounded batch at a time, each batch after the last.
            for (let after = 0; ;) {
              const records = yield* sql<{ readonly sequence: number }>`
                SELECT sequence FROM ${sql(PRUNE_STATE)}
                WHERE sequence > ${after} AND sequence <= ${through} AND held = 0
                ORDER BY sequence ASC LIMIT ${DELETE_BATCH_MAX}
              `.pipe(NodeSqliteClient.readOnly);
              if (records.length === 0) break;
              after = records.at(-1)!.sequence;
              yield* beforeWrite;
              yield* sql`
                DELETE FROM ${sql(PRUNE_STATE)}
                WHERE held = 0 AND sequence IN ${sql.in(records.map((row) => row.sequence))}
              `;
              yield* paced;
              if (records.length < DELETE_BATCH_MAX) break;
            }
          }
          if (cursor < boundary.through) continue;
          // 4. Legacy history, once; a boundary that comes due meanwhile is classified after it.
          if (legacy === undefined) {
            legacy = yield* pruneImportedLegacyHistory(
              boundary.head,
              boundary.receiptRowId,
              paced,
              beforeWrite,
            );
            deletedEventCount += legacy.deletedEventCount;
          }
          if (cursor >= boundary.through) break;
        }

        const freelistRows = yield* sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`;
        const pageSizeRows = yield* sql<{ readonly page_size: number }>`PRAGMA page_size`;
        const reclaimableBytes =
          (freelistRows[0]?.freelist_count ?? 0) * (pageSizeRows[0]?.page_size ?? 0);

        return {
          deletedEventCount,
          deletedReceiptCount: legacy?.deletedReceiptCount ?? 0,
          reclaimableBytes,
        };
      },
      Effect.mapError(
        (cause) => new ProjectionMaintenanceError({ operation: "prune event store", cause }),
      ),
    );

    return ProjectionMaintenanceV2.of({ pruneEventStore });
  }),
);

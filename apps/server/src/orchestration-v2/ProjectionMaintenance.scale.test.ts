// A daily compaction pass costs its day's rows, not the whole event log. A pass that re-read every
// row of a large event log (one 4 KiB page each) could not finish between restarts.
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- the process's own I/O counters.
import * as NodeFS from "node:fs";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";

// T3_BENCH_COMPACTION_SCALE multiplies the rows (default 1, ~40 k rows, ~150 MB); run it larger by hand.
const SCALE = Math.max(1, Number(process.env.T3_BENCH_COMPACTION_SCALE ?? "1"));
const DAY_ROWS = 2_000 * SCALE;
// About a tenth of each day's rows are kept for good (kept types), so earlier days pile up below the boundary.
const EARLIER_KEPT_ROWS = 16 * DAY_ROWS;
// A mean event payload of about 3 KB: about one row per 4 KiB page.
const PAYLOAD_BYTES = 3_000;
const PAGE_BYTES = 4_096;

// Rows generated in SQLite itself: building 40 k+ rows through statements would dominate the test.
// `kind` picks the type mix: kept rows of earlier days, or one day's mix (1 kept in 8).
const insertRows = (tag: string, count: number, kind: "kept" | "day") =>
  Effect.flatMap(SqlClient.SqlClient, (sql) =>
    sql.unsafe(
      `
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        actor_kind, payload_json, metadata_json, application_event_version
      )
      WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
      SELECT
        'event:' || ? || ':' || i, 'thread', 'thread:' || (i % 50),
        (SELECT COALESCE(MAX(stream_version), 0) FROM orchestration_events) + i,
        CASE
          WHEN ? = 'kept' THEN
            CASE i % 4 WHEN 0 THEN 'run.created' WHEN 1 THEN 'checkpoint-scope.created' ELSE 'run.updated' END
          ELSE
            CASE i % 8 WHEN 0 THEN 'run.updated' WHEN 1 THEN 'turn-item.updated' WHEN 2 THEN 'provider-turn.updated'
              ELSE 'node.updated' END
        END,
        '2026-11-01T00:00:00.000Z', 'server',
        json_object(
          'id', 'item:' || i,
          'status', CASE WHEN ? = 'kept' AND i % 4 = 3 THEN 'interrupted' WHEN i % 8 = 0 OR ? = 'kept' THEN 'running' ELSE 'completed' END,
          'type', 'command_execution',
          'text', printf('%.*c', ?, 'x')
        ),
        '{}', 2
      FROM n
      `,
      [count, tag, kind, kind, kind, PAYLOAD_BYTES],
    ),
  );

const projectThroughHead = Effect.flatMap(
  SqlClient.SqlClient,
  (sql) => sql`
    INSERT INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
    VALUES ('thread-projections', 2, (SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events), '2026-11-01T00:00:00.000Z')
    ON CONFLICT(projection_name) DO UPDATE SET last_sequence = excluded.last_sequence
  `,
);

const pass = ProjectionMaintenance.ProjectionMaintenanceV2.pipe(
  Effect.flatMap((maintenance) => maintenance.pruneEventStore()),
  Effect.provide(ProjectionMaintenance.layer),
);

// Steady state on day 2: earlier days' kept rows, then day 1 (due now) and day 2 (newer than the
// boundary). The measured pass is the day-2 one, whose boundary is the end of day 1.
const steadyState = Effect.gen(function* () {
  yield* insertRows("earlier", EARLIER_KEPT_ROWS, "kept");
  yield* projectThroughHead;
  yield* pass;
  yield* insertRows("day1", DAY_ROWS, "day");
  yield* projectThroughHead;
  yield* TestClock.adjust("1 day");
  yield* pass;
  const sql = yield* SqlClient.SqlClient;
  const earlier = yield* sql<{ readonly sequence: number }>`
    SELECT MAX(sequence) AS sequence FROM orchestration_events WHERE event_id LIKE 'event:earlier:%'
  `;
  yield* insertRows("day2", DAY_ROWS, "day");
  yield* projectThroughHead;
  yield* TestClock.adjust("1 day");
  return { earlierThrough: earlier[0]!.sequence };
});

const withStore = <A, E>(use: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-compaction-scale-" });
    return yield* use.pipe(
      Effect.provide(SqlitePersistence.layerFromPath(path.join(directory, "state.sqlite"))),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

it.effect("a daily pass does not read again the kept rows of earlier days", () =>
  withStore(
    Effect.gen(function* () {
      const { earlierThrough } = yield* steadyState;
      // Tripwire: earlier days' kept rows can no longer be parsed. They were classified when their
      // own day was pruned; a pass that reads them again fails.
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
          UPDATE orchestration_events SET payload_json = '{'
          WHERE event_type = 'run.updated' AND sequence <= ${earlierThrough}
        `;
      const result = yield* pass;
      // Day 1's prunable rows (7 in 8) are gone.
      assert.isAtLeast(result.deletedEventCount, Math.floor((DAY_ROWS * 7) / 8));
    }),
  ),
);

const procIo = "/proc/self/io";
const readChars = () => {
  const line = NodeFS.readFileSync(procIo, "utf8")
    .split("\n")
    .find((entry) => entry.startsWith("rchar:"));
  return Number(line?.split(":")[1]);
};

// rchar counts every read() of the database and WAL, cached or not: a bound in pages, on any disk.
// A window row is read twice (classified, scanned for prompt references); a deleted one again by the
// writer with its index leaves and WAL frames. 8 pages a row covers that, whatever earlier days hold.
const PAGES_PER_WINDOW_ROW = 8;
// Schema, metadata and b-tree interior pages read once per pass, independent of the rows.
const FIXED_BYTES = 8 * 1024 * 1024;

// Linux only: the process's read counters come from procfs.
it.effect.skipIf(!NodeFS.existsSync(procIo))(
  "a daily pass reads in proportion to its window of rows, not to the whole event log",
  () =>
    withStore(
      Effect.gen(function* () {
        yield* steadyState;
        const before = readChars();
        yield* pass;
        const read = readChars() - before;
        const windowRows = 2 * DAY_ROWS;
        const bound = PAGES_PER_WINDOW_ROW * windowRows * PAGE_BYTES + FIXED_BYTES;
        const measured = {
          scale: SCALE,
          windowRows,
          earlierKeptRows: EARLIER_KEPT_ROWS,
          readBytes: read,
          pagesPerWindowRow: Number((read / PAGE_BYTES / windowRows).toFixed(2)),
          bound,
        };
        // Printed for the record; the bound below is the verdict.
        yield* Effect.logWarning("compaction window read", measured);
        assert.isAtMost(read, bound, JSON.stringify(measured));
      }),
    ),
);

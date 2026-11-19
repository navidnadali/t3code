// The event-log compaction's boundary rule and restarts. A deploy that cuts the pass after it
// published a boundary and before it wrote its next mark must not freeze the boundary for days.
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { runPerfForkMigrations } from "../persistence/PerfForkMigrations.ts";
import { TABLE as PRUNE_STATE } from "../persistence/PerfForkMigrations/EventLogPruneState.ts";
import {
  EVENT_LOG_PRUNE_MARK,
  EVENT_LOG_PRUNED_THROUGH,
} from "../persistence/OrchestrationEventStore.ts";
import * as ProjectionMaintenance from "./ProjectionMaintenance.ts";

type Row = {
  readonly type: string;
  readonly stream: string;
  readonly payload: Record<string, unknown>;
};

let eventIds = 0;

// Direct rows: the compaction reads only these columns, and building them through the sink would
// make the fixtures slow without testing anything more.
const insertEvents = (rows: ReadonlyArray<Row>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const sequences: Array<number> = [];
    for (const row of rows) {
      eventIds += 1;
      const inserted = yield* sql<{ readonly sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`event:resume:${eventIds}`}, 'thread', ${row.stream},
          COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events
            WHERE aggregate_kind = 'thread' AND stream_id = ${row.stream}), 0),
          ${row.type}, '2026-11-01T00:00:00.000Z', 'server', ${JSON.stringify(row.payload)}, '{}', 2
        ) RETURNING sequence
      `;
      sequences.push(inserted[0]!.sequence);
    }
    return sequences;
  });

// The projections are current: the boundary's other limit is not under test here.
const projectThroughHead = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
    VALUES ('thread-projections', 2, (SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events), '2026-11-01T00:00:00.000Z')
    ON CONFLICT(projection_name) DO UPDATE SET last_sequence = excluded.last_sequence
  `;
});

const metadata = (name: string) =>
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql<{ readonly last_sequence: number }>`
      SELECT last_sequence FROM orchestration_v2_projection_metadata WHERE projection_name = ${name}
    `,
  ).pipe(Effect.map((rows) => rows[0]?.last_sequence));

const remaining = Effect.flatMap(
  SqlClient.SqlClient,
  (sql) =>
    sql<{ readonly sequence: number }>`SELECT sequence FROM orchestration_events ORDER BY sequence`,
).pipe(Effect.map((rows) => rows.map((row) => row.sequence)));

const head = Effect.map(remaining, (sequences) => sequences.at(-1) ?? 0);

// A fresh service each time, as after a restart: only the database carries state between passes.
const pass = ProjectionMaintenance.ProjectionMaintenanceV2.pipe(
  Effect.flatMap((maintenance) => maintenance.pruneEventStore()),
  Effect.provide(ProjectionMaintenance.layer),
);

const isEventDelete = (query: string) => /^\s*DELETE FROM orchestration_events\b/i.test(query);

// Holds the first statement that deletes events, then interrupts the pass there: a restart.
const passCutAtFirstDelete = Effect.gen(function* () {
  const reached = yield* Deferred.make<void>();
  const never = yield* Deferred.make<void>();
  const hold: Statement.Transformer = (statement) => {
    const [query] = statement.compile();
    return isEventDelete(query)
      ? Deferred.succeed(reached, undefined).pipe(
          Effect.andThen(Deferred.await(never)),
          Effect.as(statement),
        )
      : Effect.succeed(statement);
  };
  const fiber = yield* pass.pipe(
    Effect.provideService(Statement.CurrentTransformer, hold),
    Effect.forkChild({ startImmediately: true }),
  );
  yield* Deferred.await(reached);
  yield* Fiber.interrupt(fiber);
});

const node = (stream: string, revision: number): Row => ({
  type: "node.updated",
  stream,
  payload: { id: `node:${stream}`, status: "running", revision, text: "x".repeat(200) },
});

it.effect("a due pass cut by a restart still lets the boundary advance a day later", () =>
  Effect.gen(function* () {
    // Day 0: a run's history and an idle stream; the first pass only marks the head.
    const kept = yield* insertEvents([
      { type: "run.created", stream: "thread:a", payload: { id: "run:a" } },
      { type: "checkpoint-scope.created", stream: "thread:a", payload: { id: "scope:a" } },
      { type: "run.updated", stream: "thread:a", payload: { id: "run:a", status: "running" } },
      { type: "run.updated", stream: "thread:a", payload: { id: "run:a", status: "interrupted" } },
      {
        type: "turn-item.updated",
        stream: "thread:a",
        payload: { id: "item:stop", type: "run_interrupt_request", providerTurnId: "turn:a" },
      },
    ]);
    const prunableDay0 = yield* insertEvents([
      node("thread:a", 0),
      node("thread:a", 1),
      { type: "run.updated", stream: "thread:a", payload: { id: "run:a", status: "completed" } },
      node("thread:idle", 0),
    ]);
    const [idleHead] = yield* insertEvents([node("thread:idle", 1)]);
    const [anchor] = yield* insertEvents([
      {
        type: "turn-item.updated",
        stream: "thread:sub",
        payload: { id: "item:sub", type: "subagent", prompt: "Review the diff. ".repeat(20) },
      },
    ]);
    yield* projectThroughHead;
    yield* pass;
    const day0Head = yield* head;
    assert.equal(yield* metadata(EVENT_LOG_PRUNE_MARK), day0Head);

    // Day 1: more history, then the due pass publishes the boundary and is cut by a restart.
    const prunableDay1 = yield* insertEvents([
      node("thread:a", 2),
      node("thread:a", 3),
      {
        type: "turn-item.updated",
        stream: "thread:sub",
        payload: { id: "item:sub", type: "subagent", prompt: "", promptSequence: anchor },
      },
    ]);
    yield* projectThroughHead;
    const day1Head = yield* head;
    yield* TestClock.adjust("1 day");
    yield* passCutAtFirstDelete;
    assert.equal(yield* metadata(EVENT_LOG_PRUNED_THROUGH), day0Head);

    // After the cut pass: a row still borrowing the day-0 prompt, and the thread's new head.
    const day2 = yield* insertEvents([
      {
        type: "turn-item.updated",
        stream: "thread:sub",
        payload: { id: "item:sub", type: "subagent", prompt: "", promptSequence: anchor },
      },
      node("thread:a", 4),
    ]);
    yield* projectThroughHead;

    // Day 2: the boundary is the head the cut pass started with, not the day-0 one.
    yield* TestClock.adjust("1 day");
    const statements: Array<{ readonly query: string; readonly params: ReadonlyArray<unknown> }> =
      [];
    const record: Statement.Transformer = (statement) => {
      const [query, params] = statement.compile();
      statements.push({ query, params });
      return Effect.succeed(statement);
    };
    // Written just before this pass: younger than the retained day, so never pruned.
    const recent = yield* insertEvents([node("thread:a", 5)]);
    yield* projectThroughHead;
    yield* pass.pipe(Effect.provideService(Statement.CurrentTransformer, record));

    assert.equal(yield* metadata(EVENT_LOG_PRUNED_THROUGH), day1Head);
    const left = new Set(yield* remaining);
    for (const sequence of [...prunableDay0, ...prunableDay1]) {
      assert.isFalse(
        left.has(sequence),
        `prunable row ${sequence} at or below the boundary is gone`,
      );
    }
    for (const sequence of [...kept, idleHead!, anchor!, ...day2, ...recent]) {
      assert.isTrue(left.has(sequence), `row ${sequence} is kept`);
    }
    // The boundary is published before anything at or below it is deleted.
    const published = statements.findIndex(
      ({ query, params }) =>
        /orchestration_v2_projection_metadata/.test(query) &&
        params.includes(EVENT_LOG_PRUNED_THROUGH) &&
        params.includes(day1Head),
    );
    const firstDelete = statements.findIndex(({ query }) => isEventDelete(query));
    assert.isAtLeast(published, 0);
    assert.isAbove(firstDelete, published);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// A wide window of prunable rows with kept rows spread through it, on a few streams.
const windowRows = (count: number): ReadonlyArray<Row> =>
  Array.from({ length: count }, (_, index): Row => {
    const stream = `thread:${index % 7}`;
    return index % 4 === 0
      ? { type: "run.updated", stream, payload: { id: `run:${index}`, status: "running" } }
      : node(stream, index);
  });

// The first pass marks the window; a day later the next one prunes through it.
const storeDueForTheWindow = (count: number) =>
  Effect.gen(function* () {
    const sequences = yield* insertEvents(windowRows(count));
    yield* projectThroughHead;
    yield* pass;
    // Each stream's newest row comes after the window, so no window row is a stream head.
    yield* insertEvents(Array.from({ length: 7 }, (_, index) => node(`thread:${index}`, count)));
    yield* projectThroughHead;
    yield* TestClock.adjust("1 day");
    return sequences;
  });

it.effect("a restart mid-pass resumes where the pass stopped instead of starting over", () =>
  Effect.gen(function* () {
    const window = 4_000;
    const sequences = yield* storeDueForTheWindow(window);
    const sql = yield* SqlClient.SqlClient;

    // Cut the pass once its deletes have passed 60% of the window.
    const deep = sequences[Math.floor(window * 0.6)]!;
    const reached = yield* Deferred.make<void>();
    const never = yield* Deferred.make<void>();
    const holdDeep: Statement.Transformer = (statement) => {
      const [query, params] = statement.compile();
      const lowest = Math.min(
        ...params.filter((value): value is number => typeof value === "number"),
      );
      return isEventDelete(query) && lowest > deep
        ? Deferred.succeed(reached, undefined).pipe(
            Effect.andThen(Deferred.await(never)),
            Effect.as(statement),
          )
        : Effect.succeed(statement);
    };
    const cut = yield* pass.pipe(
      Effect.provideService(Statement.CurrentTransformer, holdDeep),
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(reached);
    yield* Fiber.interrupt(cut);

    // Tripwire: the kept rows in the first 30% of the window can no longer be parsed. A pass that
    // classified them before the restart must not read them again.
    const tripwire = sequences[Math.floor(window * 0.3)]!;
    yield* sql`
      UPDATE orchestration_events SET payload_json = '{'
      WHERE event_type = 'run.updated' AND sequence <= ${tripwire}
    `;
    yield* pass;

    // The same rows are left as after one uninterrupted pass: every node row, no run.updated(running) row.
    const left = new Set(yield* remaining);
    windowRows(window).forEach((row, index) => {
      assert.equal(
        left.has(sequences[index]!),
        row.type === "run.updated",
        `${row.type} at window index ${index}`,
      );
    });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("an uninterrupted pass leaves the rows the resumed one does", () =>
  Effect.gen(function* () {
    const window = 4_000;
    const sequences = yield* storeDueForTheWindow(window);
    yield* pass;
    const left = new Set(yield* remaining);
    windowRows(window).forEach((row, index) => {
      assert.equal(left.has(sequences[index]!), row.type === "run.updated");
    });
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// Held rows are checked again once the boundary passes the reference scan's start: two due days on.
const twoDueDays = Effect.gen(function* () {
  yield* projectThroughHead;
  yield* TestClock.adjust("1 day");
  yield* pass;
  yield* insertEvents([node("thread:later", 0)]);
  yield* projectThroughHead;
  yield* TestClock.adjust("1 day");
});

it.effect("a held stream head is pruned once its stream moves on", () =>
  Effect.gen(function* () {
    const [, idleHead] = yield* insertEvents([node("thread:idle", 0), node("thread:idle", 1)]);
    yield* projectThroughHead;
    yield* pass;
    yield* TestClock.adjust("1 day");
    yield* pass;
    assert.include(yield* remaining, idleHead!);
    // The stream moves on: its old head is an ordinary row now.
    yield* insertEvents([node("thread:idle", 2)]);
    yield* twoDueDays;
    yield* pass;
    assert.notInclude(yield* remaining, idleHead!);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// With more rows than the writer reads in its transaction, the scan catches up first.
it.effect.each([
  { case: "a few rows", filler: 0 },
  { case: "enough rows that the scan catches up first", filler: 2_100 },
])("an anchor whose borrower commits after the reference scan is kept ($case)", ({ filler }) =>
  Effect.gen(function* () {
    const prompt = "Audit the migration. ".repeat(20);
    const [anchor] = yield* insertEvents([
      {
        type: "turn-item.updated",
        stream: "thread:race",
        payload: { id: "item:race", type: "subagent", prompt },
      },
    ]);
    // Its only borrower so far is pruned with it, so the next due pass finds it unreferenced.
    yield* insertEvents([
      {
        type: "turn-item.updated",
        stream: "thread:race",
        payload: { id: "item:race", type: "subagent", prompt: "", promptSequence: anchor },
      },
      node("thread:race", 0),
    ]);
    yield* projectThroughHead;
    yield* pass;
    yield* TestClock.adjust("1 day");

    // Hold the check of held rows; meanwhile the insert path borrows the anchor's prompt again.
    const reached = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let held = false;
    const holdHeldCheck: Statement.Transformer = (statement) => {
      const [query] = statement.compile();
      if (!held && query.includes("AS kept") && query.includes("event.sequence IN")) {
        held = true;
        return Deferred.succeed(reached, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(statement),
        );
      }
      return Effect.succeed(statement);
    };
    const running = yield* pass.pipe(
      Effect.provideService(Statement.CurrentTransformer, holdHeldCheck),
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(reached);
    const [borrower] = yield* insertEvents([
      {
        type: "turn-item.updated",
        stream: "thread:race",
        payload: { id: "item:race", type: "subagent", prompt: "", promptSequence: anchor },
      },
    ]);
    yield* insertEvents(Array.from({ length: filler }, (_, index) => node("thread:filler", index)));
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(running);

    const sql = yield* SqlClient.SqlClient;
    const restored = yield* sql<{ readonly prompt: string | null }>`
      SELECT json_extract(anchor.payload_json, '$.prompt') AS prompt
      FROM orchestration_events AS borrower
      LEFT JOIN orchestration_events AS anchor
        ON anchor.sequence = json_extract(borrower.payload_json, '$.promptSequence')
      WHERE borrower.sequence = ${borrower!}
    `;
    assert.deepEqual(restored, [{ prompt }]);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// A pass that runs past the next mark's day prunes through it before it finishes.
it.effect("a boundary that comes due while a pass runs is pruned by that pass", () =>
  Effect.gen(function* () {
    yield* insertEvents([node("thread:a", 0), node("thread:a", 1)]);
    yield* projectThroughHead;
    yield* pass;
    const day1 = yield* insertEvents([node("thread:a", 2), node("thread:a", 3)]);
    yield* insertEvents([node("thread:a", 4)]);
    yield* projectThroughHead;
    const day1Head = yield* head;
    yield* TestClock.adjust("1 day");

    // Hold the reference scan's first read; meanwhile the mark this pass wrote comes due.
    const reached = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let held = false;
    const holdScan: Statement.Transformer = (statement) => {
      const [query] = statement.compile();
      if (!held && query.includes("AS reference")) {
        held = true;
        return Deferred.succeed(reached, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(statement),
        );
      }
      return Effect.succeed(statement);
    };
    const running = yield* pass.pipe(
      Effect.provideService(Statement.CurrentTransformer, holdScan),
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(reached);
    yield* TestClock.adjust("1 day");
    yield* TestClock.adjust("11 minutes");
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(running);

    assert.equal(yield* metadata(EVENT_LOG_PRUNED_THROUGH), day1Head);
    const left = yield* remaining;
    for (const sequence of day1) assert.notInclude(left, sequence);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// An anchor held below the boundary whose only borrower sits above it, with the pass's progress
// stored: the borrower keeps it.
const heldAnchorWithBorrower = Effect.gen(function* () {
  const prompt = "Summarise the release. ".repeat(20);
  const [anchor] = yield* insertEvents([
    {
      type: "turn-item.updated",
      stream: "thread:held",
      payload: { id: "item:held", type: "subagent", prompt },
    },
    node("thread:held", 0),
  ]);
  yield* projectThroughHead;
  yield* pass;
  const [borrower] = yield* insertEvents([
    {
      type: "turn-item.updated",
      stream: "thread:held",
      payload: { id: "item:held", type: "subagent", prompt: "", promptSequence: anchor },
    },
    node("thread:held", 1),
  ]);
  yield* projectThroughHead;
  yield* TestClock.adjust("1 day");
  yield* pass;
  return { anchor: anchor!, borrower: borrower!, prompt };
});

const restoredPrompt = (borrower: number) =>
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql<{ readonly prompt: string | null }>`
      SELECT json_extract(anchor.payload_json, '$.prompt') AS prompt
      FROM orchestration_events AS borrower
      LEFT JOIN orchestration_events AS anchor
        ON anchor.sequence = json_extract(borrower.payload_json, '$.promptSequence')
      WHERE borrower.sequence = ${borrower}
    `,
  ).pipe(Effect.map((rows) => rows[0]?.prompt ?? null));

// The progress rows describe the state table's contents, so a table
// created anew (the fork migration restoring a lost table, or doctor on an unmigrated database)
// must restart classification instead of trusting progress whose recorded references are gone.
it.effect.each([
  { case: "the fork migration restores it", restore: runPerfForkMigrations().pipe(Effect.asVoid) },
  { case: "the pass meets the database without it", restore: Effect.void },
])("a lost state table restarts classification ($case)", ({ restore }) =>
  Effect.gen(function* () {
    const prompt = "Summarise the release. ".repeat(20);
    yield* insertEvents([node("thread:start", 0)]);
    yield* projectThroughHead;
    yield* pass;
    const [anchor] = yield* insertEvents([
      {
        type: "turn-item.updated",
        stream: "thread:held",
        payload: { id: "item:held", type: "subagent", prompt },
      },
      node("thread:held", 0),
    ]);
    yield* projectThroughHead;
    yield* TestClock.adjust("1 day");
    yield* pass;
    // The borrower comes after this boundary's mark; the reference scan records it.
    const [borrower] = yield* insertEvents([
      {
        type: "turn-item.updated",
        stream: "thread:held",
        payload: { id: "item:held", type: "subagent", prompt: "", promptSequence: anchor },
      },
      node("thread:held", 1),
    ]);
    yield* projectThroughHead;
    yield* pass;

    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe(`DROP TABLE ${PRUNE_STATE}`);
    yield* restore;
    // The anchor's boundary comes due; its borrower, above it, must keep it.
    yield* TestClock.adjust("1 day");
    yield* pass;
    assert.include(yield* remaining, borrower!);
    assert.equal(yield* restoredPrompt(borrower!), prompt);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// Rows committed while the pass waits for headroom or the writer are counted
// again under the writer, so the writer never reads more than the limit of unscanned rows.
it.effect("the writer reads at most the limit of rows committed after the reference scan", () =>
  Effect.gen(function* () {
    const { borrower, prompt } = yield* heldAnchorWithBorrower;
    yield* insertEvents([node("thread:held", 2)]);
    yield* projectThroughHead;
    yield* TestClock.adjust("1 day");
    // Arm on the check of held rows; the next write guard then lets 2,100 rows in.
    let armed = false;
    let fired = false;
    const arm: Statement.Transformer = (statement) => {
      const [query] = statement.compile();
      if (query.includes("AS kept") && query.includes("event.sequence IN")) armed = true;
      return Effect.succeed(statement);
    };
    const tails: Array<number> = [];
    const watchTail: Statement.Transformer = (statement, compiler, fiber, span) => {
      const [query, params] = statement.compile();
      if (
        query.includes("SELECT DISTINCT json_extract(payload_json, '$.promptSequence') AS anchor")
      ) {
        tails.push(params.find((value): value is number => typeof value === "number") ?? 0);
      }
      return arm(statement, compiler, fiber, span);
    };
    const sql = yield* SqlClient.SqlClient;
    const flood = Effect.suspend(() => {
      if (!armed || fired) return Effect.void;
      fired = true;
      return insertEvents(
        Array.from({ length: 2_100 }, (_, index) => node("thread:flood", index)),
      ).pipe(Effect.asVoid, Effect.orDie, Effect.provideService(SqlClient.SqlClient, sql));
    });
    yield* ProjectionMaintenance.ProjectionMaintenanceV2.pipe(
      Effect.flatMap((maintenance) => maintenance.pruneEventStore(Effect.void, flood)),
      Effect.provide(ProjectionMaintenance.layer),
      Effect.provideService(Statement.CurrentTransformer, watchTail),
    );
    assert.isTrue(fired);
    const latest = yield* head;
    for (const scanned of tails) assert.isAtMost(latest - scanned, 2_000);
    if ((yield* remaining).includes(borrower)) {
      assert.equal(yield* restoredPrompt(borrower), prompt);
    }
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// A boundary that comes due during legacy pruning is classified by the same pass.
it.effect("a boundary that comes due during legacy pruning is pruned by that pass", () =>
  Effect.gen(function* () {
    yield* insertEvents([node("thread:a", 0), node("thread:a", 1)]);
    yield* projectThroughHead;
    yield* pass;
    const day1 = yield* insertEvents([node("thread:a", 2), node("thread:a", 3)]);
    yield* insertEvents([node("thread:a", 4)]);
    yield* projectThroughHead;
    const day1Head = yield* head;
    yield* TestClock.adjust("1 day");
    const reached = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let held = false;
    const holdLegacy: Statement.Transformer = (statement) => {
      const [query] = statement.compile();
      if (!held && query.includes("INDEXED BY idx_orchestration_events_application_sequence")) {
        held = true;
        return Deferred.succeed(reached, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(statement),
        );
      }
      return Effect.succeed(statement);
    };
    const running = yield* pass.pipe(
      Effect.provideService(Statement.CurrentTransformer, holdLegacy),
      Effect.forkChild({ startImmediately: true }),
    );
    yield* Deferred.await(reached);
    yield* TestClock.adjust("1 day");
    yield* TestClock.adjust("11 minutes");
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(running);
    assert.equal(yield* metadata(EVENT_LOG_PRUNED_THROUGH), day1Head);
    const left = yield* remaining;
    for (const sequence of day1) assert.notInclude(left, sequence);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// Reference records for anchors already classified are cleared in bounded
// batches, so the cleanup never holds the writer for all of them at once.
it.effect("the reference records of classified anchors are cleared in bounded batches", () =>
  Effect.gen(function* () {
    const prompt = "Plan the rollout. ".repeat(10);
    // Kept-for-good anchors, classified by the first due pass; their borrowers come after it.
    const anchors = yield* insertEvents(
      Array.from({ length: 600 }, (_, index) => ({
        type: "subagent.updated",
        stream: `thread:anchor:${index}`,
        payload: { id: `sub:${index}`, prompt },
      })),
    );
    yield* projectThroughHead;
    yield* pass;
    yield* TestClock.adjust("1 day");
    yield* pass;
    yield* insertEvents(
      anchors.map((anchor, index) => ({
        type: "subagent.updated",
        stream: `thread:anchor:${index}`,
        payload: { id: `sub:${index}`, prompt: "", promptSequence: anchor },
      })),
    );
    yield* projectThroughHead;
    yield* TestClock.adjust("1 day");
    const deletes: Array<readonly [string, number]> = [];
    const record: Statement.Transformer = (statement) => {
      const [query, params] = statement.compile();
      if (
        query.includes(`DELETE FROM ${PRUNE_STATE}`) ||
        query.includes(`DELETE FROM "${PRUNE_STATE}"`)
      ) {
        deletes.push([query, params.length]);
      }
      return Effect.succeed(statement);
    };
    yield* pass.pipe(Effect.provideService(Statement.CurrentTransformer, record));
    const sql = yield* SqlClient.SqlClient;
    const [left] = yield* sql<{ readonly records: number }>`
      SELECT count(*) AS records FROM ${sql(PRUNE_STATE)} WHERE held = 0
    `;
    assert.equal(left?.records, 0);
    assert.isAbove(deletes.length, 1);
    for (const [query, parameters] of deletes) {
      assert.match(query, /\bIN\s*\(/, "each delete names its rows");
      assert.isAtMost(parameters, 500);
    }
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

// Each cleanup batch starts after the previous one, so held rows before the
// reference records are read once, not once per batch.
it.effect("each batch of the reference-record cleanup starts after the previous batch", () =>
  Effect.gen(function* () {
    const prompt = "Plan the rollout. ".repeat(10);
    // Idle streams' heads: held, and first in the state table.
    yield* insertEvents(
      Array.from({ length: 1_000 }, (_, index) => node(`thread:idle:${index}`, 0)),
    );
    const anchors = yield* insertEvents(
      Array.from({ length: 600 }, (_, index) => ({
        type: "subagent.updated",
        stream: `thread:anchor:${index}`,
        payload: { id: `sub:${index}`, prompt },
      })),
    );
    yield* projectThroughHead;
    yield* pass;
    yield* TestClock.adjust("1 day");
    yield* pass;
    yield* insertEvents(
      anchors.map((anchor, index) => ({
        type: "subagent.updated",
        stream: `thread:anchor:${index}`,
        payload: { id: `sub:${index}`, prompt: "", promptSequence: anchor },
      })),
    );
    yield* projectThroughHead;
    yield* TestClock.adjust("1 day");
    const batches: Array<ReadonlyArray<unknown>> = [];
    const record: Statement.Transformer = (statement) => {
      const [query, params] = statement.compile();
      if (/^\s*SELECT sequence FROM/.test(query) && query.includes("held = 0"))
        batches.push(params);
      return Effect.succeed(statement);
    };
    yield* pass.pipe(Effect.provideService(Statement.CurrentTransformer, record));
    const sql = yield* SqlClient.SqlClient;
    const [left] = yield* sql<{ readonly records: number }>`
      SELECT count(*) AS records FROM ${sql(PRUNE_STATE)} WHERE held = 0
    `;
    assert.equal(left?.records, 0);
    // 600 records: a batch of 500, then one starting after the 500th record.
    assert.isAtLeast(batches.length, 2);
    assert.isTrue(batches[1]?.includes(anchors[499]), `second batch: ${String(batches[1])}`);
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

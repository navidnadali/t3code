// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - reads the shape file and measures the result through node:sqlite.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ApplicationProjectCreatedPayload,
  EventId,
  OrchestrationV2DomainEventJson,
  ProjectId,
} from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { ClaudeProviderCapabilitiesV2 } from "../../../apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as EventSink from "../../../apps/server/src/orchestration-v2/EventSink.ts";
import * as EventStore from "../../../apps/server/src/orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../../apps/server/src/orchestration-v2/ProjectStore.ts";
import * as Sqlite from "../../../apps/server/src/persistence/Sqlite.ts";
import { Builder, type LegacyRows } from "./entities.ts";
import { type SynthPlan, planSynth } from "./plan.ts";
import { checkFixture, compactInto, isClean, ownedFiles } from "./sanitise.ts";
import { createExclusive, fillExclusive, privateScratchDirectory, withSidecars } from "./guards.ts";
import { failed, safeDestination } from "./server.ts";
import { type Shape, measureShape, parseShape, shapeRows } from "./shape.ts";

const decodeEvents = Schema.decodeUnknownSync(Schema.Array(OrchestrationV2DomainEventJson));
const decodeProjectPayload = Schema.decodeUnknownSync(ApplicationProjectCreatedPayload);
const BATCH_EVENTS = 2_000;

export interface SynthInput {
  readonly shapePath: string;
  readonly scale: number;
  readonly seed: number;
  readonly destination: string;
  readonly providerInstanceId: string;
  readonly driver: string;
  readonly model: string;
  readonly workspaceRoot: string;
  readonly now: string | undefined;
  readonly report?: string | undefined;
}

export interface TableComparison {
  readonly table: string;
  readonly target: number;
  readonly planned: number | null;
  readonly actual: number;
  readonly deviation: number | null;
  readonly targetBytes: number | null;
  readonly actualBytes: number;
}

// Kept by the server itself (migrations, search index, positions, P1-014's turn item counts).
const DERIVED_TABLES =
  /^(?:effect_sql_migrations|sqlite_.*|orchestration_v2_message_search(?:_.*)?|orchestration_v2_projection_metadata|orchestration_v2_projection_turn_item_counts)$/;

/** Rows and bytes per table of a built database against the shape scaled, and against the plan. */
export const compare = (shape: Shape, plan: SynthPlan, actual: Shape): Array<TableComparison> => {
  const names = new Set([...Object.keys(shape.tables), ...Object.keys(actual.tables)]);
  return [...names]
    .filter((table) => !DERIVED_TABLES.test(table))
    .sort()
    .map((table) => {
      const target = Math.round(shapeRows(shape, table) * plan.scale);
      const rows = actual.tables[table]?.rows ?? 0;
      const sourceBytes = shape.tables[table]?.bytes;
      return {
        table,
        target,
        planned: plan.expected[table] ?? null,
        actual: rows,
        deviation: target === 0 ? (rows === 0 ? 0 : null) : (rows - target) / target,
        targetBytes: sourceBytes === undefined ? null : Math.round(sourceBytes * plan.scale),
        actualBytes: actual.tables[table]?.bytes ?? 0,
      };
    });
};

/** What the v1 importer leaves behind: its bookkeeping (both phases done) and the v1 rows it read. */
const writeLegacy = Effect.fn("fixture.synth.writeLegacy")(function* (rows: LegacyRows) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
        INSERT INTO orchestration_v2_legacy_imports (
          thread_id, source_updated_at, shell_imported_at, transcript_imported_at, imported_message_count
        ) VALUES (${rows.threadId}, ${rows.importedAt}, ${rows.importedAt}, ${rows.importedAt}, ${rows.importedMessages})
      `;
      yield* sql`
        INSERT INTO projection_threads (thread_id, project_id, title, created_at, updated_at)
        VALUES (${rows.threadId}, ${rows.projectId}, ${rows.title}, ${rows.createdAt}, ${rows.importedAt})
      `;
      for (const message of rows.messages) {
        yield* sql`
          INSERT INTO projection_thread_messages (
            message_id, thread_id, turn_id, role, text, is_streaming, created_at, updated_at
          ) VALUES (${message.id}, ${rows.threadId}, NULL, ${message.role}, ${message.text}, 0, ${message.at}, ${message.at})
        `;
      }
    }),
  );
});

const writeDatabase = Effect.fn("fixture.synth.write")(function* (
  plan: SynthPlan,
  builder: Builder,
) {
  const sql = yield* SqlClient.SqlClient;
  const eventStore = yield* EventStore.EventStoreV2;
  const projectStore = yield* ProjectStore.ProjectStoreV2;
  const sink = yield* EventSink.EventSinkV2;

  yield* sql.withTransaction(
    Effect.forEach(builder.projectEvents(), (project) =>
      eventStore
        .appendProjectEvent({
          eventId: EventId.make(project.eventId),
          aggregateKind: "project",
          aggregateId: ProjectId.make(project.projectId),
          occurredAt: project.occurredAt,
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "project.created",
          payload: decodeProjectPayload(project.payload),
        })
        .pipe(Effect.flatMap((event) => projectStore.apply(event))),
    ),
  );

  // Parents first: a fork names its source run, a child the node that spawned it.
  const order = [
    ...plan.threads.filter((thread) => thread.parent === null && thread.fork === null),
    ...plan.threads.filter((thread) => thread.fork !== null),
    ...plan.threads.filter((thread) => thread.parent !== null),
  ];
  let written = 0;
  let nextReport = 0;
  for (const thread of order) {
    const built = builder.threadEvents(thread);
    const events = decodeEvents(built.events);
    for (let start = 0; start < events.length; start += BATCH_EVENTS) {
      yield* sink.write({ events: events.slice(start, start + BATCH_EVENTS) });
    }
    if (built.legacy !== null) yield* writeLegacy(built.legacy);
    written += events.length;
    if (written >= nextReport) {
      yield* Console.log(`  ${written} events written`);
      nextReport = written + 250_000;
    }
  }
  return written;
});

/**
 * The two columns the server stamps with the wall clock (when each migration ran, when the
 * projection last moved) set to the synth's own `now`, so the same shape and seed give the same
 * bytes. Kept in the formats the server writes.
 */
const pinWallClock = (path: string, now: number) => {
  const iso = DateTime.formatIso(DateTime.makeUnsafe(now));
  const db = new NodeSqlite.DatabaseSync(path);
  try {
    db.prepare(`UPDATE effect_sql_migrations SET created_at = ?`).run(
      iso.slice(0, 19).replace("T", " "),
    );
    db.prepare(`UPDATE orchestration_v2_projection_metadata SET updated_at = ?`).run(iso);
  } finally {
    db.close();
  }
};

export const runSynth = Effect.fn("fixture.synth")(function* (input: SynthInput) {
  const destination = yield* safeDestination({
    destination: input.destination,
    ...(input.report === undefined ? {} : { distinctFrom: [input.report] }),
  });
  const report =
    input.report === undefined
      ? undefined
      : yield* safeDestination({
          destination: input.report,
          source: input.shapePath,
          distinctFrom: withSidecars(input.destination),
        });
  const shape = yield* Effect.try({
    try: () => parseShape(JSON.parse(NodeFS.readFileSync(input.shapePath, "utf8"))),
    catch: (cause) => failed(`--like ${input.shapePath}`, cause),
  });
  if (!(input.scale > 0) || !Number.isFinite(input.scale)) {
    return yield* failed(`--scale must be a positive number, got ${input.scale}`);
  }
  const plan = planSynth(shape, input.scale, input.seed);
  const now = Date.parse(input.now ?? shape.source?.capturedAt ?? "2026-10-31T00:00:00.000Z");
  if (Number.isNaN(now)) return yield* failed(`--now is not a date: ${input.now}`);
  const builder = new Builder(plan, {
    providerInstanceId: input.providerInstanceId,
    driver: input.driver,
    model: input.model,
    workspaceRoot: input.workspaceRoot,
    now,
    capabilities: ClaudeProviderCapabilitiesV2,
  });

  const owned = ownedFiles();
  return yield* Effect.gen(function* () {
    owned.watch();
    if (report !== undefined) {
      createExclusive(report);
      owned.file(report);
    }
    const directory = privateScratchDirectory(destination, ".fixture-synth-");
    owned.directory(directory);
    const scratch = NodePath.join(directory, "scratch.sqlite");
    yield* Console.log(
      `Planned ${plan.projects} projects, ${plan.threads.length} threads, ${plan.totals.turnItems} turn items, ${plan.expected.orchestration_events} events.`,
    );
    // The server's own setup: migrations, pragmas and page layout of a fresh database.
    const events = yield* writeDatabase(plan, builder).pipe(
      Effect.provide(
        EventSink.layer.pipe(
          Layer.provideMerge(
            Layer.mergeAll(EventStore.layer, ProjectionStore.layer, ProjectStore.layer),
          ),
          Layer.provideMerge(Sqlite.layerFromPath(scratch)),
          Layer.provide(NodeServices.layer),
        ),
      ),
    );
    yield* Console.log(`Wrote ${events} events; compacting into ${destination}...`);
    pinWallClock(scratch, now);
    owned.file(destination);
    yield* compactInto(scratch, destination);

    const db = new NodeSqlite.DatabaseSync(destination, { readOnly: true });
    let actual: Shape;
    try {
      actual = measureShape(db);
    } finally {
      db.close();
    }
    const tables = compare(shape, plan, actual);
    const mismatched = tables.filter((row) => row.planned !== null && row.planned !== row.actual);
    // Generated text is not filler: the pattern layer checks it, as for a --keep-content copy.
    const check = yield* checkFixture(destination, "redact");
    if (mismatched.length > 0 || !isClean(check)) {
      return yield* failed(
        `Self-check failed: ${JSON.stringify({ notAsPlanned: mismatched, check }, null, 2)}`,
      );
    }
    const targetBytes = Math.round(shape.pageSize * shape.pageCount * plan.scale);
    const sizeBytes = actual.pageSize * actual.pageCount;
    const result = {
      destination,
      scale: plan.scale,
      seed: plan.seed,
      sizeBytes,
      targetBytes,
      sizeDeviation: targetBytes === 0 ? null : (sizeBytes - targetBytes) / targetBytes,
      substituted: plan.substituted,
      tables,
      shape: actual,
    };
    if (report !== undefined) fillExclusive(report, `${JSON.stringify(result, null, 2)}\n`);
    owned.keep();
    return result;
  }).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        owned.cleanup();
        owned.unwatch();
      }),
    ),
  );
});

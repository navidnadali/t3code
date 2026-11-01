import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { EventId, type OrchestrationV2DomainEvent, ThreadId } from "@t3tools/contracts";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";

import * as EffectOutbox from "../../../apps/server/src/orchestration-v2/EffectOutbox.ts";
import * as EventSink from "../../../apps/server/src/orchestration-v2/EventSink.ts";
import * as EventStore from "../../../apps/server/src/orchestration-v2/EventStore.ts";
import * as ProjectionMaintenance from "../../../apps/server/src/orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "../../../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as ProviderRuntimeRecovery from "../../../apps/server/src/orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as LegacyV1ThreadImporter from "../../../apps/server/src/orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import { rebuildMessageSearch } from "../../../apps/server/src/orchestration-v2/messageSearchIndex.ts";
import { runMigrations } from "../../../apps/server/src/persistence/Migrations.ts";
import * as ServerSettings from "../../../apps/server/src/serverSettings.ts";
import { assertSafeDestination } from "./guards.ts";

/** A destination the tool will not write: inside a T3 home, existing, or the source (exit 2). */
export class FixtureRefusedError extends Schema.TaggedError<FixtureRefusedError>()(
  "FixtureRefusedError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

export class FixtureFailedError extends Schema.TaggedError<FixtureFailedError>()(
  "FixtureFailedError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

const messageOf = (cause: unknown) => (cause instanceof Error ? cause.message : String(cause));

/** The path guard, as a typed failure. */
export const safeDestination = (input: Parameters<typeof assertSafeDestination>[0]) =>
  Effect.try({
    try: () => assertSafeDestination(input),
    catch: (cause) => new FixtureRefusedError({ detail: messageOf(cause) }),
  });

export const failed = (detail: string, cause?: unknown) =>
  new FixtureFailedError({
    detail: cause === undefined ? detail : `${detail}: ${messageOf(cause)}`,
  });

// `queued-runs` also lists held queues, which `canStartQueuedRun` then refuses; the startable
// ones are counted on their own (`queuedRunsStartable`).
const RECOVERY_KINDS: ReadonlyArray<ProjectionStore.ProjectionRecoveryKind> = [
  "runtime",
  "subagent-results",
  "delegated-completions",
];

/** A plain connection: no server pragmas, so `sqlite_stat1` and the journal mode stay as they are. */
export const database = (filename: string) => NodeSqliteClient.layer({ filename });

export const withProjections = (filename: string) =>
  ProjectionStore.layer.pipe(Layer.provideMerge(database(filename)));

/** The server's write path on a plain connection: events, projections, positions, metadata. */
export const withEventSink = (filename: string) =>
  EventSink.layer.pipe(
    Layer.provideMerge(Layer.mergeAll(EventStore.layer, ProjectionStore.layer)),
    Layer.provideMerge(database(filename)),
  );

/** Threads whose pull request watch the server resumes on boot (it reads the host, may wake the agent). */
export const WATCHED_THREADS = `
  SELECT DISTINCT thread.thread_id FROM orchestration_v2_projection_threads AS thread,
    json_each(thread.payload_json, '$.pullRequests') AS link
  WHERE json_valid(thread.payload_json)
    AND json_type(link.value, '$.watch') IS NOT NULL
    AND json_type(link.value, '$.watch') <> 'null'`;

/** Threads whose pull request links or branch the PR sync and branch discovery read from the host. */
export const PULL_REQUEST_THREADS = `
  SELECT thread_id FROM orchestration_v2_projection_threads
  WHERE deleted_at IS NULL AND json_valid(payload_json) AND (
    COALESCE(json_array_length(payload_json, '$.pullRequests'), 0) > 0
    OR json_type(payload_json, '$.linkedPullRequest') NOT IN ('null')
    OR json_type(payload_json, '$.branchPullRequest') NOT IN ('null')
    OR json_type(payload_json, '$.branch') NOT IN ('null'))`;

/** Threads a timer would act on: a usage-limit recovery or a snooze to wake from. */
const TIMED_THREADS = `
  SELECT thread_id FROM orchestration_v2_projection_threads
  WHERE json_valid(payload_json) AND (
    json_type(payload_json, '$.limitRecovery') NOT IN ('null')
    OR json_type(payload_json, '$.snoozedUntil') NOT IN ('null'))`;

/**
 * Per thread a boot would act on, one `thread.metadata-updated` (`updatedAt` kept): auto-settle
 * off, pull request links and branch cleared, limit-recovery and snooze timers removed. Run it
 * after the restart recovery, which turns threads with cancelled runs into settle candidates.
 */
export const neutraliseThreads = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const rows = (query: string) =>
    sql
      .unsafe<{ thread_id: string }>(query)
      .unprepared.pipe(Effect.map((found) => new Set(found.map((row) => row.thread_id))));
  const settling = new Set<string>(
    (yield* projections.getSettlementCandidates()).map((thread) => thread.id),
  );
  const linked = yield* rows(PULL_REQUEST_THREADS);
  const timed = yield* rows(TIMED_THREADS);
  const events: Array<OrchestrationV2DomainEvent> = [];
  for (const id of new Set([...settling, ...linked, ...timed])) {
    const thread = yield* projections.getThread(ThreadId.make(id)).pipe(Effect.option);
    if (Option.isNone(thread)) continue;
    events.push({
      id: EventId.make(`event:thread:${encodeURIComponent(id)}:fixture-sanitise:neutralise`),
      threadId: thread.value.id,
      type: "thread.metadata-updated",
      occurredAt: now,
      payload: {
        ...thread.value,
        ...(settling.has(id) ? { autoSettleDisabledAt: now } : {}),
        ...(linked.has(id)
          ? { pullRequests: [], linkedPullRequest: null, branchPullRequest: null, branch: null }
          : {}),
        ...(timed.has(id) ? { limitRecovery: null, snoozedUntil: null, snoozedAt: null } : {}),
      },
    });
  }
  for (let start = 0; start < events.length; start += 500) {
    yield* sink.write({ events: events.slice(start, start + 500) });
  }
  return {
    autoSettleDisabled: settling.size,
    pullRequestInputsCleared: linked.size,
    timersCleared: timed.size,
  };
});

/**
 * The server's own restart recovery (`ProviderRuntimeRecoveryService.reconcile("startup")`), as
 * a boot runs it: active runs, attempts, nodes, provider turns and items cancelled, pending
 * runtime requests closed, provider threads idle, sessions stopped, queues held, unsettled
 * effects retired. Restart continuation is off, so nothing is scheduled to run.
 */
export const restartRecovery = Effect.gen(function* () {
  const recovery = yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService;
  return yield* recovery.reconcile("startup");
});

export const withRecovery = (filename: string) =>
  ProviderRuntimeRecovery.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        EffectOutbox.layer,
        IdAllocator.layer,
        ServerSettings.layerTest({ continueThreadsAfterServerUpdate: false }),
      ),
    ),
    Layer.provideMerge(withEventSink(filename)),
  );

/** The server's v1 importer, as a boot runs it: v1 shells become v2 threads, transcripts copied. */
export const legacyImport = Effect.gen(function* () {
  const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
  const shells = yield* importer.reconcileShells;
  const transcripts = yield* importer.importPendingTranscripts;
  return {
    shellThreads: shells.importedThreadCount,
    transcriptThreads: transcripts.importedThreadCount,
    messages: shells.importedMessageCount + transcripts.importedMessageCount,
  };
});

export const withLegacyImporter = (filename: string) =>
  LegacyV1ThreadImporter.layer.pipe(Layer.provideMerge(withEventSink(filename)));

/** The server's own migrations, as `migrate-dev-db` runs them on a copy. */
export const migrate = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe("PRAGMA foreign_keys = ON").unprepared;
  return yield* runMigrations();
});

/** Threads the server would resume, recover or deliver to on start, by its own queries. */
export const recoveryThreadIds = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const ids = new Set<string>();
  for (const kind of RECOVERY_KINDS) {
    for (const id of yield* projections.getRecoveryThreadIds(kind)) ids.add(id);
  }
  const far = DateTime.makeUnsafe("9999-01-01T00:00:00.000Z");
  for (const candidate of yield* projections.getLimitRecoveryCandidates({
    now: far,
    autoResume: true,
    snooze: true,
  })) {
    ids.add(candidate.id);
  }
  return ids;
});

export const rebuildSearch = Effect.gen(function* () {
  yield* rebuildMessageSearch(yield* SqlClient.SqlClient);
});

/** v1 threads the legacy importer would turn into v2 threads on start (no v2 `thread.created`). */
export const LEGACY_SHELL_IMPORTS = `
  SELECT thread.thread_id FROM projection_threads AS thread
  WHERE NOT EXISTS (
    SELECT 1 FROM orchestration_events AS event
    WHERE event.application_event_version = 2 AND event.aggregate_kind = 'thread'
      AND event.stream_id = thread.thread_id AND event.event_type = 'thread.created')`;

/** Imported v1 threads whose transcript the importer would still copy on start. */
export const LEGACY_TRANSCRIPT_IMPORTS = `
  SELECT thread_id FROM orchestration_v2_legacy_imports WHERE transcript_imported_at IS NULL`;

export interface RunnableWork {
  readonly recoveryThreads: number;
  readonly [table: string]: number;
}

// Rows here would make a server act on start: run, deliver, fetch, import, or reach another
// machine. Each names the table it needs, so an older schema without it counts nothing.
const RUNNABLE_QUERIES: ReadonlyArray<readonly [name: string, table: string, query: string]> = [
  ...[
    "scheduled_tasks",
    "scheduled_task_webhook_pending",
    "orchestration_v2_effect_outbox",
    "orchestration_v2_thread_launch_workflows",
    "peer_environment_links",
    "provider_session_runtime",
    "auth_sessions",
    "auth_pairing_links",
  ].map((table) => [table, table, `SELECT COUNT(*) AS n FROM ${table}`] as const),
  [
    "projection_projects.auto_pull",
    "projection_projects",
    "SELECT COUNT(*) AS n FROM projection_projects WHERE auto_pull <> 0",
  ],
  [
    "orchestration_v2_projection_threads.pullRequestWatch",
    "orchestration_v2_projection_threads",
    `SELECT COUNT(*) AS n FROM (${WATCHED_THREADS})`,
  ],
  [
    "orchestration_v2_projection_threads.pullRequestOrBranch",
    "orchestration_v2_projection_threads",
    `SELECT COUNT(*) AS n FROM (${PULL_REQUEST_THREADS})`,
  ],
  [
    "orchestration_v2_projection_threads.limitRecovery",
    "orchestration_v2_projection_threads",
    `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads
     WHERE json_extract(payload_json, '$.limitRecovery') IS NOT NULL`,
  ],
  [
    "queuedRunsStartable",
    "orchestration_v2_projection_runs",
    `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads AS thread
     WHERE thread.archived_at IS NULL AND thread.deleted_at IS NULL
       AND EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE thread_id = thread.thread_id
         AND status = 'queued' AND json_extract(payload_json, '$.queueHeld') IS NOT 1)
       AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE thread_id = thread.thread_id
         AND status IN ('preparing', 'starting', 'running', 'waiting'))`,
  ],
  [
    "orchestration_v2_projection_threads.snoozedUntil",
    "orchestration_v2_projection_threads",
    `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads
     WHERE json_extract(payload_json, '$.snoozedUntil') IS NOT NULL`,
  ],
  [
    "orchestration_v2_projection_subagents.deliveryPending",
    "orchestration_v2_projection_subagents",
    `SELECT COUNT(*) AS n FROM orchestration_v2_projection_subagents
     WHERE json_extract(payload_json, '$.completionDelivery.state') = 'pending'`,
  ],
  [
    "orchestration_v2_projection_messages.streaming",
    "orchestration_v2_projection_messages",
    "SELECT COUNT(*) AS n FROM orchestration_v2_projection_messages WHERE streaming = 1",
  ],
  [
    "projection_thread_sessions.live",
    "projection_thread_sessions",
    "SELECT COUNT(*) AS n FROM projection_thread_sessions WHERE status NOT IN ('stopped', 'error')",
  ],
  [
    "legacyShellImports",
    "projection_threads",
    `SELECT COUNT(*) AS n FROM (${LEGACY_SHELL_IMPORTS})`,
  ],
  [
    "legacyTranscriptImports",
    "orchestration_v2_legacy_imports",
    `SELECT COUNT(*) AS n FROM (${LEGACY_TRANSCRIPT_IMPORTS})`,
  ],
];

/** Everything that would run on start; all zero for a fixture that is safe to boot. */
export const runnableWork = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = new Set(
    (yield* sql<{ name: string }>`SELECT name FROM sqlite_schema WHERE type = 'table'`).map(
      (row) => row.name,
    ),
  );
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const counts: Record<string, number> = {
    recoveryThreads: (yield* recoveryThreadIds).size,
    settlementCandidates: (yield* projections.getSettlementCandidates()).length,
  };
  for (const [name, table, query] of RUNNABLE_QUERIES) {
    if (!tables.has(table)) continue;
    const rows = yield* sql.unsafe<{ n: number }>(query).unprepared;
    counts[name] = Number(rows[0]?.n ?? 0);
  }
  return counts as RunnableWork;
});

export const isRunnable = (work: RunnableWork) => Object.values(work).some((count) => count > 0);

/**
 * The server's event-store compaction, as a first boot runs it: superseded snapshot events and
 * the v1 events of imported threads go now, so booting the fixture deletes nothing.
 */
export const compactEvents = Effect.gen(function* () {
  const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
  return yield* maintenance.compactEventStore();
});

export const withMaintenance = (filename: string) =>
  ProjectionMaintenance.layer.pipe(Layer.provideMerge(withEventSink(filename)));

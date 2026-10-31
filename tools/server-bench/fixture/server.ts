import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { EventId, type OrchestrationV2DomainEvent, ThreadId } from "@t3tools/contracts";

import * as EventSink from "../../../apps/server/src/orchestration-v2/EventSink.ts";
import * as EventStore from "../../../apps/server/src/orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../../apps/server/src/orchestration-v2/ProjectionStore.ts";
import { rebuildMessageSearch } from "../../../apps/server/src/orchestration-v2/messageSearchIndex.ts";
import { runMigrations } from "../../../apps/server/src/persistence/Migrations.ts";
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

const RECOVERY_KINDS: ReadonlyArray<ProjectionStore.ProjectionRecoveryKind> = [
  "queued-runs",
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

/**
 * One `thread.metadata-updated` per thread a boot would act on, through the server's sink
 * (`updatedAt` kept): automatic settlement off on every thread the settle sweep could pick (no
 * settle event, no settle hook), and pull request links and branch cleared (no host or git reads).
 */
export const neutraliseThreads = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* EventSink.EventSinkV2;
  const now = yield* DateTime.now;
  const settling = new Set<string>(
    (yield* projections.getSettlementCandidates()).map((thread) => thread.id),
  );
  const linked = new Set(
    (yield* sql.unsafe<{ thread_id: string }>(PULL_REQUEST_THREADS).unprepared).map(
      (row) => row.thread_id,
    ),
  );
  const events: Array<OrchestrationV2DomainEvent> = [];
  for (const id of new Set([...settling, ...linked])) {
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
      },
    });
  }
  for (let start = 0; start < events.length; start += 500) {
    yield* sink.write({ events: events.slice(start, start + 500) });
  }
  return { autoSettleDisabled: settling.size, pullRequestInputsCleared: linked.size };
});

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

export interface RunnableWork {
  readonly recoveryThreads: number;
  readonly [table: string]: number;
}

// Rows here would make a server act on start: run, deliver, fetch, or reach another machine.
const RUNNABLE_QUERIES: ReadonlyArray<readonly [string, string]> = [
  ["scheduled_tasks", "SELECT COUNT(*) AS n FROM scheduled_tasks"],
  ["scheduled_task_webhook_pending", "SELECT COUNT(*) AS n FROM scheduled_task_webhook_pending"],
  ["orchestration_v2_effect_outbox", "SELECT COUNT(*) AS n FROM orchestration_v2_effect_outbox"],
  [
    "orchestration_v2_thread_launch_workflows",
    "SELECT COUNT(*) AS n FROM orchestration_v2_thread_launch_workflows",
  ],
  ["peer_environment_links", "SELECT COUNT(*) AS n FROM peer_environment_links"],
  ["provider_session_runtime", "SELECT COUNT(*) AS n FROM provider_session_runtime"],
  ["auth_sessions", "SELECT COUNT(*) AS n FROM auth_sessions"],
  ["auth_pairing_links", "SELECT COUNT(*) AS n FROM auth_pairing_links"],
  [
    "projection_projects.auto_pull",
    "SELECT COUNT(*) AS n FROM projection_projects WHERE auto_pull <> 0",
  ],
  [
    "orchestration_v2_projection_threads.pullRequestWatch",
    `SELECT COUNT(*) AS n FROM (${WATCHED_THREADS})`,
  ],
  [
    "orchestration_v2_projection_threads.pullRequestOrBranch",
    `SELECT COUNT(*) AS n FROM (${PULL_REQUEST_THREADS})`,
  ],
  [
    "orchestration_v2_projection_threads.limitRecovery",
    `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads
     WHERE json_extract(payload_json, '$.limitRecovery') IS NOT NULL`,
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
  for (const [name, query] of RUNNABLE_QUERIES) {
    if (!tables.has(name.split(".")[0] as string)) continue;
    const rows = yield* sql.unsafe<{ n: number }>(query).unprepared;
    counts[name] = Number(rows[0]?.n ?? 0);
  }
  return counts as RunnableWork;
});

export const isRunnable = (work: RunnableWork) => Object.values(work).some((count) => count > 0);

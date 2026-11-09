import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqlite from "node:sqlite";

import {
  ThreadId,
  ProjectId,
  ProviderInstanceId,
  EventId,
  MessageId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import * as Sqlite from "./Sqlite.ts";

const seedThread = (dbPath: string) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const now = yield* DateTime.now;
    const threadId = ThreadId.make("thread-1");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const messageId = MessageId.make("message-1");
    yield* sink.write({
      events: [
        {
          id: EventId.make("thread-created"),
          type: "thread.created",
          threadId,
          occurredAt: now,
          payload: {
            id: threadId,
            projectId: ProjectId.make("project-1"),
            title: "Doctor",
            providerInstanceId,
            modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            deletedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            createdBy: "user",
            creationSource: "web",
          },
        },
        {
          id: EventId.make("message-created"),
          type: "message.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: messageId,
            threadId,
            runId: null,
            nodeId: null,
            role: "assistant",
            text: "needle",
            attachments: [],
            streaming: false,
            createdAt: now,
            updatedAt: now,
            createdBy: "agent",
            creationSource: "provider",
          },
        },
        {
          id: EventId.make("item-created"),
          type: "turn-item.updated",
          threadId,
          occurredAt: now,
          payload: {
            id: TurnItemId.make("item-1"),
            threadId,
            runId: null,
            nodeId: null,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            status: "completed",
            title: null,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            type: "assistant_message",
            messageId,
            text: "needle",
            streaming: false,
          },
        },
      ],
    });
  }).pipe(
    Effect.provide(
      EventSink.layer.pipe(
        Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
        Layer.provide(Sqlite.layerFromPath(dbPath)),
      ),
    ),
  );

it.layer(DatabaseMaintenance.layer.pipe(Layer.provideMerge(NodeServices.layer)))(
  "DatabaseMaintenance",
  (it) => {
    it.effect("reports an unreadable database as an inspection failure", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
        const dbPath = path.join(
          yield* fs.makeTempDirectoryScoped({ prefix: "t3-inspect-" }),
          "statev2.sqlite",
        );
        yield* fs.writeFileString(dbPath, "not a database");
        const failure = yield* Effect.flip(maintenance.inspect(dbPath));
        assert.equal(failure._tag, "DatabaseInspectError");
      }).pipe(Effect.scoped),
    );

    it.effect("reports and deletes only the database from before 0.1.0", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
        const userdata = yield* fs.makeTempDirectoryScoped({ prefix: "t3-legacy-" });
        const dbPath = path.join(userdata, "statev2.sqlite");
        const legacy = path.join(userdata, "state.sqlite");
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`CREATE TABLE retained_data (value TEXT)`;
          yield* sql`INSERT INTO retained_data VALUES ('keep')`;
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
        assert.isNull((yield* maintenance.inspect(dbPath)).legacyDatabaseBytes);

        const openLegacy = NodeSqliteClient.layer({ filename: legacy });
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA journal_mode = WAL`;
          yield* sql`CREATE TABLE v1_data (value TEXT)`;
          yield* sql`INSERT INTO v1_data VALUES ('rollback')`;
        }).pipe(Effect.provide(openLegacy));
        assert.isAbove((yield* maintenance.inspect(dbPath)).legacyDatabaseBytes ?? 0, 0);

        const refused = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`SELECT value FROM v1_data`;
          const failure = yield* Effect.flip(maintenance.removeLegacyDatabase(dbPath));
          assert.deepEqual(yield* sql`SELECT value FROM v1_data`, [{ value: "rollback" }]);
          return failure;
        }).pipe(Effect.provide(openLegacy));
        assert.equal(refused._tag, "DatabaseInUseError");
        assert.isTrue(yield* fs.exists(legacy));

        yield* Effect.flatMap(DatabaseMaintenance.DatabaseMaintenance, (service) =>
          service.removeLegacyDatabase(dbPath),
        ).pipe(
          Effect.provide(Layer.fresh(DatabaseMaintenance.layer)),
          Effect.provideService(HostProcess.Platform, "win32"),
          Effect.provideService(FileSystem.FileSystem, {
            ...fs,
            remove: (target, options) =>
              Effect.gen(function* () {
                if (target === legacy) {
                  yield* Effect.gen(function* () {
                    const sql = yield* SqlClient.SqlClient;
                    yield* sql`PRAGMA busy_timeout = 0`;
                    yield* sql`BEGIN EXCLUSIVE`;
                    yield* sql`COMMIT`;
                  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: legacy })));
                  // Create sidecars after the probe closes so only Doctor can remove them.
                  yield* fs.writeFileString(`${legacy}-wal`, "leftover WAL");
                  yield* fs.writeFileString(`${legacy}-shm`, "leftover SHM");
                }
                yield* fs.remove(target, options);
              }),
          }),
        );
        assert.isFalse(yield* fs.exists(legacy));
        assert.isFalse(yield* fs.exists(`${legacy}-wal`));
        assert.isFalse(yield* fs.exists(`${legacy}-shm`));
        assert.isTrue(yield* fs.exists(dbPath));
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(yield* sql`SELECT value FROM retained_data`, [{ value: "keep" }]);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })));
        assert.isNull((yield* maintenance.inspect(dbPath)).legacyDatabaseBytes);
      }).pipe(Effect.scoped),
    );

    it.effect("reports the compaction's outcome when the VACUUM after it fails", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-compact-full-" });
        const dbPath = path.join(baseDir, "userdata", "statev2.sqlite");
        yield* Effect.void.pipe(Effect.provide(Sqlite.layerFromPath(dbPath)));
        // VACUUM fails as it does on a full disk; everything before it ran.
        const run = NodeSqlite.StatementSync.prototype.run;
        const all = NodeSqlite.StatementSync.prototype.all;
        const full = (sql: string) => {
          if (!/\bVACUUM\b/.test(sql)) return undefined;
          const error = new Error("database or disk is full");
          Object.assign(error, { code: "SQLITE_FULL" });
          return error;
        };
        yield* Effect.acquireRelease(
          Effect.sync(() => [
            vi.spyOn(NodeSqlite.StatementSync.prototype, "run").mockImplementation(function (
              this: NodeSqlite.StatementSync,
              ...params
            ) {
              const error = full(this.sourceSQL);
              if (error !== undefined) throw error;
              return run.apply(this, params);
            }),
            vi.spyOn(NodeSqlite.StatementSync.prototype, "all").mockImplementation(function (
              this: NodeSqlite.StatementSync,
              ...params
            ) {
              const error = full(this.sourceSQL);
              if (error !== undefined) throw error;
              return all.apply(this, params);
            }),
          ]),
          (spies) => Effect.sync(() => spies.forEach((spy) => spy.mockRestore())),
        );
        const error = yield* maintenance.compact(dbPath).pipe(Effect.flip);
        assert.equal(error._tag, "DatabaseCompactError");
        const compaction = error._tag === "DatabaseCompactError" ? error.compaction : undefined;
        assert.deepEqual(compaction, {
          deletedEventCount: 0,
          deletedReceiptCount: 0,
          repairedItemCountThreads: 0,
        });
        // The VACUUM's own error is kept underneath, however many layers wrap it.
        const messages: Array<string> = [];
        for (
          let cause: unknown = error._tag === "DatabaseCompactError" ? error.cause : undefined;
          cause !== undefined && cause !== null;
          cause = (cause as { cause?: unknown }).cause
        ) {
          messages.push(String((cause as { message?: unknown }).message ?? cause));
        }
        assert.isTrue(
          messages.some((message) => message.includes("disk is full")),
          messages.join(" | "),
        );
      }).pipe(Effect.scoped),
    );

    it.effect.each(["missing", "unreadable"] as const)(
      "refuses to prune a database with a %s thread projection",
      (damage) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
          const dbPath = path.join(
            yield* fs.makeTempDirectoryScoped({ prefix: "t3-damaged-projection-" }),
            "statev2.sqlite",
          );
          yield* seedThread(dbPath);
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            if (damage === "missing")
              yield* sql`DELETE FROM orchestration_v2_projection_threads WHERE thread_id = 'thread-1'`;
            else
              yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{}' WHERE thread_id = 'thread-1'`;
            yield* sql`INSERT INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
            VALUES ('event-log-prune-mark', 2, 3, '1960-01-01T00:00:00.000Z')`;
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
          const failed = yield* maintenance.compact(dbPath).pipe(Effect.flip);
          assert.equal(failed._tag, "DatabaseCompactError");
          assert.equal(
            failed._tag === "DatabaseCompactError" && (failed.cause as { _tag?: string })._tag,
            "DatabaseProjectionsInvalidError",
          );
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            assert.deepEqual(
              yield* sql`SELECT event_id FROM orchestration_events ORDER BY sequence`,
              [
                { event_id: "thread-created" },
                { event_id: "message-created" },
                { event_id: "item-created" },
              ],
            );
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })));
        }).pipe(Effect.scoped),
    );

    it.effect("compacts an existing database offline only once its server has stopped", () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const maintenance = yield* DatabaseMaintenance.DatabaseMaintenance;
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-compact-" });
        const dbPath = path.join(baseDir, "userdata", "statev2.sqlite");
        // Opening the database migrates it.
        yield* seedThread(dbPath);
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA auto_vacuum = NONE`;
          yield* sql`VACUUM`;
          yield* sql`CREATE TABLE retained_data (value TEXT)`;
          yield* sql`INSERT INTO retained_data VALUES ('keep')`;
          yield* sql`CREATE TABLE discarded_data (value BLOB)`;
          yield* sql`INSERT INTO discarded_data VALUES (zeroblob(4194304))`;
          yield* sql`DROP TABLE discarded_data`;

          // An index that drifted from its rows, as a renumbering VACUUM would leave it.
          yield* sql`
            INSERT INTO orchestration_v2_message_search (orchestration_v2_message_search)
            VALUES ('delete-all')
          `;

          // Item counts that drifted from their items: one too high, one with no items at all.
          yield* sql`
            UPDATE orchestration_v2_projection_turn_item_counts SET item_count = 3
            WHERE thread_id = 'thread-1'
          `;
          yield* sql`
            INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, run_id, item_count)
            VALUES ('thread-2', 'run-2', 4)
          `;
          yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
          yield* sql`INSERT INTO orchestration_v2_legacy_imports (
            thread_id, source_updated_at, shell_imported_at, transcript_imported_at, imported_message_count
          ) VALUES ('imported', '2026-10-17', '2026-10-17', '2026-10-17', 0)`;
          yield* sql`WITH RECURSIVE history(n) AS (
            SELECT 1 UNION ALL SELECT n + 1 FROM history WHERE n < 51
          ) INSERT INTO orchestration_command_receipts (
            command_id, aggregate_kind, aggregate_id, accepted_at, result_sequence, status, command_type
          ) SELECT 'legacy-' || n, 'thread', 'imported', '2026-10-17', 0, 'accepted', 'legacy' FROM history`;
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));

        const sizeBefore = (yield* fs.stat(dbPath)).size;
        const refused = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`SELECT value FROM retained_data`;
          return yield* maintenance.compact(dbPath).pipe(Effect.flip);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
        assert.equal(refused._tag, "DatabaseInUseError");
        assert.equal((yield* fs.stat(dbPath)).size, sizeBefore);

        const clock = yield* Clock.Clock;
        const pauses: number[] = [];
        let activeNanos = 0n;
        const result = yield* maintenance.compact(dbPath).pipe(
          Effect.provideService(Clock.Clock, {
            ...clock,
            // Exceed the background budget whenever the compactor checks elapsed time.
            monotonicTimeNanos: Effect.sync(() => (activeNanos += 30_000_000n)),
            sleep: (duration) =>
              Effect.sync(() => {
                pauses.push(Duration.toMillis(duration));
              }),
          }),
        );
        assert.deepEqual(pauses, [], "offline doctor must not throttle compaction");
        assert.equal(result.deletedReceiptCount, 51);
        assert.equal(result.repairedItemCountThreads, 2);
        assert.isBelow(result.afterBytes, result.beforeBytes - 3 * 1024 * 1024);
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepEqual(yield* sql`PRAGMA auto_vacuum`, [{ auto_vacuum: 2 }]);
          assert.deepEqual(yield* sql`SELECT value FROM retained_data`, [{ value: "keep" }]);
          assert.deepEqual(
            yield* sql`
              SELECT thread_id, run_id, item_count FROM orchestration_v2_projection_turn_item_counts
            `,
            [{ thread_id: "thread-1", run_id: "", item_count: 1 }],
          );
          assert.deepEqual(
            yield* sql`
              SELECT messages.message_id AS "messageId"
              FROM orchestration_v2_message_search AS search
              INNER JOIN orchestration_v2_projection_messages AS messages
                ON messages.rowid = search.rowid
              WHERE orchestration_v2_message_search MATCH '"needle"'
            `,
            [{ messageId: "message-1" }],
          );
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath, readonly: true })));
      }).pipe(Effect.scoped),
    );
  },
);

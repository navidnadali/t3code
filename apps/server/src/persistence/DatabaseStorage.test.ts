import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import * as DatabaseStorage from "./DatabaseStorage.ts";
import * as Sqlite from "./Sqlite.ts";

const freePages = Effect.map(
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`,
  ),
  (rows) => rows[0]?.freelist_count ?? 0,
);

const storageLayer = <E>(
  config: Layer.Layer<ServerConfig.ServerConfig, E, FileSystem.FileSystem | Path.Path>,
) =>
  DatabaseStorage.layer.pipe(
    Layer.provide(ProjectionMaintenance.layer),
    Layer.provideMerge(Sqlite.layerMemory),
    Layer.provideMerge(config),
    Layer.provide(NodeServices.layer),
  );

const testConfig = ServerConfig.layerTest(process.cwd(), { prefix: "database-storage-" });

it.effect("a new database returns freed pages online without losing data", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const storage = yield* DatabaseStorage.DatabaseStorage;
    assert.deepEqual(yield* sql`PRAGMA auto_vacuum`, [{ auto_vacuum: 2 }]);
    yield* sql`CREATE TABLE retained (value TEXT)`;
    yield* sql`INSERT INTO retained VALUES ('keep')`;
    yield* sql`CREATE TABLE discarded (value BLOB)`;
    yield* sql`INSERT INTO discarded VALUES (zeroblob(6291456))`;
    yield* sql`DROP TABLE discarded`;
    const before = yield* freePages;
    assert.isAbove(before, 1024);
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span(options) {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    yield* storage.reclaim.pipe(Effect.withTracer(tracer), Effect.withTracerEnabled(true));
    assert.equal(before - (yield* freePages), 1024);
    assert.lengthOf(spans, 1);
    assert.deepEqual(
      spans.map((span) => span.name),
      ["DatabaseStorage.reclaim"],
    );
    assert.equal(spans[0]?.attributes.get("db.reclaimed_pages"), 1024);
    yield* storage.reclaim;
    assert.equal(yield* freePages, 0);
    assert.deepEqual(yield* sql`SELECT value FROM retained`, [{ value: "keep" }]);
  }).pipe(Effect.provide(storageLayer(testConfig))),
);

const compactionLayer = (
  pruneEventStore: ProjectionMaintenance.ProjectionMaintenanceV2["Service"]["pruneEventStore"],
) =>
  DatabaseStorage.layer.pipe(
    Layer.provide(
      Layer.succeed(ProjectionMaintenance.ProjectionMaintenanceV2, {
        pruneEventStore,
      }),
    ),
    Layer.provide(Sqlite.layerMemory),
  );

const minute = 60_000;
const startupTime = Date.UTC(2026, 9, 19);
const emptyCompaction = {
  deletedEventCount: 0,
  deletedReceiptCount: 0,
  reclaimableBytes: 0,
  trackedKeys: 0,
};

it.effect("remembers completion only after the paced pass finishes", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(startupTime);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* ServerConfig.ServerConfig;
    const timestampPath = path.join(path.dirname(config.dbPath), "compacted-at");
    const betweenPagesReached = yield* Deferred.make<void>();
    const completed = yield* Deferred.make<void>();
    const timestampWritten = yield* Deferred.make<void>();
    const compact = (betweenPages: Effect.Effect<void> = Effect.void) =>
      Effect.gen(function* () {
        yield* Effect.sleep(30);
        yield* Deferred.succeed(betweenPagesReached, undefined);
        yield* betweenPages;
        yield* Deferred.succeed(completed, undefined);
        return emptyCompaction;
      });
    yield* Effect.gen(function* () {
      yield* TestClock.adjust(minute);
      yield* TestClock.adjust(30);
      yield* Deferred.await(betweenPagesReached);
      assert.isFalse(yield* Deferred.isDone(completed));
      assert.isFalse(yield* fs.exists(timestampPath));
      yield* TestClock.adjust(269);
      assert.isFalse(yield* Deferred.isDone(completed));
      assert.isFalse(yield* fs.exists(timestampPath));
      yield* TestClock.adjust(1);
      yield* Deferred.await(timestampWritten);
      assert.isTrue(yield* Deferred.isDone(completed));
      assert.equal(Number(yield* fs.readFileString(timestampPath)), startupTime + minute + 300);
    }).pipe(
      Effect.provide(compactionLayer(compact)),
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        writeFileString: (file, content, options) =>
          fs
            .writeFileString(file, content, options)
            .pipe(Effect.ensuring(Deferred.succeed(timestampWritten, undefined))),
      }),
    );
  }).pipe(Effect.provide(testConfig.pipe(Layer.provideMerge(NodeServices.layer)))),
);

// NONE is a database from before automatic shrinking, which the repair converts.
const reclaimLayer = (
  restart: Effect.Effect<void> | null,
  autoVacuum: "NONE" | "FULL" | "INCREMENTAL" = "NONE",
) =>
  DatabaseStorage.layer.pipe(
    Layer.provide(ProjectionMaintenance.layer),
    Layer.provide(Layer.succeed(DatabaseStorage.ServerRestart, restart)),
    Layer.provide(
      Layer.effectDiscard(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql.unsafe(`PRAGMA auto_vacuum = ${autoVacuum}`);
          yield* sql`VACUUM`;
        }),
      ).pipe(Layer.provideMerge(Sqlite.layerMemory)),
    ),
  );

it.effect(
  "a reclaim from Settings saves its request, then restarts the server after replying",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const config = yield* ServerConfig.ServerConfig;
      const files = DatabaseMaintenance.reclaimFiles(path, config.dbPath);
      const request = Effect.flatMap(
        DatabaseStorage.DatabaseStorage,
        (storage) => storage.requestReclaim,
      );
      const restarted = yield* Deferred.make<void>();
      const restart = Deferred.succeed(restarted, undefined).pipe(Effect.asVoid);

      // Nothing starts a server run by hand again, so it keeps pointing at the command.
      const unsupervised = yield* request.pipe(Effect.flip, Effect.provide(reclaimLayer(null)));
      assert.equal(unsupervised._tag, "DatabaseReclaimError");
      // A database that already shrinks on its own (or a stale dialog after another client's
      // repair) has nothing to reclaim, so it must not restart.
      for (const autoVacuum of ["FULL", "INCREMENTAL"] as const) {
        const refused = yield* request.pipe(
          Effect.flip,
          Effect.provide(reclaimLayer(restart, autoVacuum)),
        );
        assert.equal(refused._tag, "DatabaseReclaimError", autoVacuum);
      }
      assert.isFalse(yield* fs.exists(files.request));

      yield* fs.writeFileString(files.failure, "an earlier failure");
      yield* Effect.gen(function* () {
        const storage = yield* DatabaseStorage.DatabaseStorage;
        const shownFailure = storage.status.pipe(
          Stream.filter((status) => status !== null),
          Stream.runHead,
          Effect.map((status) => Option.getOrThrow(status)?.reclaimFailure),
        );
        assert.equal(yield* shownFailure, "an earlier failure");
        yield* storage.requestReclaim;
        // The retry clears it at once, so Settings shows the restart, not the old failure.
        assert.isUndefined(yield* shownFailure);
      }).pipe(Effect.provide(reclaimLayer(restart)));
      assert.isTrue(yield* fs.exists(files.request));
      assert.isFalse(yield* fs.exists(files.failure));
      assert.isFalse(yield* Deferred.isDone(restarted), "the reply must go out before the restart");
      yield* TestClock.adjust(500);
      yield* Deferred.await(restarted);
    }).pipe(Effect.provide(testConfig.pipe(Layer.provideMerge(NodeServices.layer)))),
);

// The commit lane can keep the single writer close to fully busy, so the compaction pass
// shares it at background priority: a foreground write already waiting goes first.
it.effect("the compaction pass writes after foreground writes already waiting for the writer", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(startupTime);
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE writes (name TEXT NOT NULL)`;
    const passWriting = yield* Deferred.make<void>();
    const passWrote = yield* Deferred.make<void>();
    const compact = () =>
      Deferred.succeed(passWriting, undefined).pipe(
        Effect.andThen(sql`INSERT INTO writes VALUES ('compaction')`),
        Effect.andThen(Deferred.succeed(passWrote, undefined)),
        Effect.as(emptyCompaction),
        Effect.orDie,
      );
    // A foreground transaction holds the writer while the pass, then a foreground write, queue for it.
    const held = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const holder = yield* sql
      .withTransaction(
        sql`INSERT INTO writes VALUES ('holder')`.pipe(
          Effect.andThen(Deferred.succeed(held, undefined)),
          Effect.andThen(Deferred.await(release)),
        ),
      )
      .pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(held);
    yield* Effect.gen(function* () {
      yield* TestClock.adjust(minute);
      yield* Deferred.await(passWriting);
      // Lets the pass's write reach the writer and queue there first.
      for (let turn = 0; turn < 20; turn++) yield* Effect.yieldNow;
      const foreground = yield* sql`INSERT INTO writes VALUES ('foreground')`.pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holder);
      yield* Fiber.join(foreground);
      yield* Deferred.await(passWrote);
      const rows = yield* sql<{ readonly name: string }>`SELECT name FROM writes ORDER BY rowid`;
      assert.deepEqual(
        rows.map((row) => row.name),
        ["holder", "foreground", "compaction"],
      );
    }).pipe(
      Effect.provide(
        DatabaseStorage.layer.pipe(
          Layer.provide(
            Layer.succeed(ProjectionMaintenance.ProjectionMaintenanceV2, {
              pruneEventStore: compact,
            }),
          ),
        ),
      ),
    );
  }).pipe(
    Effect.provide(
      Layer.mergeAll(Sqlite.layerMemory, testConfig.pipe(Layer.provideMerge(NodeServices.layer))),
    ),
  ),
);

// A pass's deletes rewrite index pages that file system snapshots can pin, and a host can have
// little room left. A pass pauses while the database's file system, or the host's
// headroom signal, is below its floor, and resumes from its progress once there is room again.
const dueStore = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (let index = 0; index < 120; index++) {
    yield* sql`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        actor_kind, payload_json, metadata_json, application_event_version
      ) VALUES (
        ${`event:guard:${index}`}, 'thread', 'thread:guard', ${index}, 'node.updated',
        '2026-10-01T00:00:00.000Z', 'server', '{"status":"running"}', '{}', 2
      )
    `;
  }
  // Marked two days ago through the head: the first pass is due and prunes all but the head.
  const markedAt = DateTime.formatIso(DateTime.makeUnsafe(startupTime - 2 * 86_400_000));
  for (const name of ["thread-projections", "event-log-prune-mark"]) {
    yield* sql`
      INSERT INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
      VALUES (${name}, 2, (SELECT MAX(sequence) FROM orchestration_events), ${markedAt})
      ON CONFLICT(projection_name) DO UPDATE SET
        last_sequence = excluded.last_sequence, updated_at = excluded.updated_at
    `;
  }
});

// The boundary the pass published, and whether the due mark is still the one from two days ago.
const boundaryRows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly projection_name: string; readonly updated_at: string }>`
    SELECT projection_name, updated_at FROM orchestration_v2_projection_metadata
    WHERE projection_name IN ('event-log-prune-mark', 'event-log-pruned-through')
  `;
  const markedAt = DateTime.formatIso(DateTime.makeUnsafe(startupTime - 2 * 86_400_000));
  return {
    prunedThrough: rows.find((row) => row.projection_name === "event-log-pruned-through")
      ?.updated_at,
    markedTwoDaysAgo:
      rows.find((row) => row.projection_name === "event-log-prune-mark")?.updated_at === markedAt,
  };
}).pipe(Effect.orDie);

const eventCount = Effect.flatMap(
  SqlClient.SqlClient,
  (sql) => sql<{ readonly count: number }>`SELECT COUNT(*) AS count FROM orchestration_events`,
).pipe(Effect.map((rows) => rows[0]?.count ?? 0));

// Runs the storage service's own pass on the due store; settles on whichever comes first: the pass
// completing (its completion file) or the pass saying it paused.
type InStore = Effect.Effect<
  void,
  never,
  ServerConfig.ServerConfig | FileSystem.FileSystem | Path.Path
>;

const guardedPass = (
  env: Record<string, string>,
  options: {
    readonly before?: InStore;
    readonly afterPause?: InStore;
    readonly atFirst?: Effect.Effect<unknown, never, SqlClient.SqlClient>;
  } = {},
) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(startupTime);
    yield* dueStore;
    if (options.before !== undefined) yield* options.before;
    const fs = yield* FileSystem.FileSystem;
    const completed = yield* Deferred.make<void>();
    const paused = yield* Deferred.make<string>();
    const capture = Logger.make(({ message }) => {
      const text = Array.isArray(message) ? message.join(" ") : String(message);
      if (/compaction paused/i.test(text)) Deferred.doneUnsafe(paused, Effect.succeed(text));
    });
    return yield* Effect.gen(function* () {
      yield* TestClock.adjust(minute);
      const first = yield* Effect.raceFirst(
        Deferred.await(paused).pipe(Effect.as("paused" as const)),
        Deferred.await(completed).pipe(Effect.as("completed" as const)),
      );
      const leftAtFirst = yield* eventCount;
      const atFirst = options.atFirst === undefined ? undefined : yield* options.atFirst;
      if (first === "paused" && options.afterPause !== undefined) {
        yield* options.afterPause;
        yield* TestClock.adjust("5 minutes");
        yield* Deferred.await(completed);
      }
      return { first, leftAtFirst, atFirst, leftAtEnd: yield* eventCount };
    }).pipe(
      Effect.provide(
        DatabaseStorage.layer.pipe(
          Layer.provide(ProjectionMaintenance.layer),
          Layer.provide(ConfigProvider.layer(ConfigProvider.fromEnvRecord(env))),
          Layer.provide(Logger.layer([capture], { mergeWithExisting: true })),
        ),
      ),
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        writeFileString: (file, content, options) =>
          fs
            .writeFileString(file, content, options)
            .pipe(
              Effect.ensuring(
                file.endsWith("compacted-at")
                  ? Deferred.succeed(completed, undefined)
                  : Effect.void,
              ),
            ),
      }),
    );
  }).pipe(
    Effect.provide(
      Layer.mergeAll(Sqlite.layerMemory, testConfig.pipe(Layer.provideMerge(NodeServices.layer))),
    ),
  );

it.effect("a compaction pass pauses while the database's file system is below its floor", () =>
  Effect.gen(function* () {
    // A floor no disk meets: the pass must not delete, nor even publish its boundary.
    const result = yield* guardedPass(
      { T3CODE_EVENT_PRUNE_MIN_FREE_MB: String(2 ** 40) },
      { atFirst: boundaryRows },
    );
    assert.equal(result.first, "paused");
    assert.equal(result.leftAtFirst, 120);
    assert.deepEqual(result.atFirst, { prunedThrough: undefined, markedTwoDaysAgo: true });
  }),
);

// A signal the server cannot read is not an absent one.
it.effect("a compaction pass treats an unreadable host headroom signal as no room", () =>
  Effect.gen(function* () {
    const result = yield* guardedPass(
      {},
      {
        before: Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const config = yield* ServerConfig.ServerConfig;
          // A directory where the file should be: reading it fails with an error other than NotFound.
          yield* fs.makeDirectory(path.join(path.dirname(config.dbPath), "storage-headroom.json"));
        }).pipe(Effect.orDie),
      },
    );
    assert.equal(result.first, "paused");
    assert.equal(result.leftAtFirst, 120);
  }),
);

const headroomSignal = (availableBytes: number, updatedAt: number) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* ServerConfig.ServerConfig;
    yield* fs.writeFileString(
      path.join(path.dirname(config.dbPath), "storage-headroom.json"),
      JSON.stringify({
        availableBytes,
        updatedAt: DateTime.formatIso(DateTime.makeUnsafe(updatedAt)),
      }),
    );
  }).pipe(Effect.orDie);

const GiB = 2 ** 30;

it.effect("a compaction pass pauses on a low host headroom signal and resumes once it rises", () =>
  Effect.gen(function* () {
    const result = yield* guardedPass(
      {},
      {
        before: headroomSignal(10 * GiB, startupTime),
        afterPause: headroomSignal(500 * GiB, startupTime + minute),
      },
    );
    assert.equal(result.first, "paused");
    assert.equal(result.leftAtFirst, 120);
    // Resumed from its progress: everything but the stream's head is gone.
    assert.equal(result.leftAtEnd, 1);
  }),
);

it.effect("a compaction pass treats a stale host headroom signal as no room", () =>
  Effect.gen(function* () {
    // Plenty of room, but written three hours ago: the host stopped updating it.
    const result = yield* guardedPass(
      {},
      { before: headroomSignal(500 * GiB, startupTime - 3 * 3_600_000) },
    );
    assert.equal(result.first, "paused");
    assert.equal(result.leftAtFirst, 120);
  }),
);

// The signal's contract: bytes as an integer, a UTC timestamp
// with Z, not more than 10 min ahead; anything else is no room.
it.effect.each([
  { case: "dated an hour ahead", signal: { availableBytes: 500 * GiB, ahead: 3_600_000 } },
  { case: "a fractional byte count", signal: { availableBytes: 500 * GiB + 0.5, ahead: 0 } },
  { case: "a local time without Z", signal: { availableBytes: 500 * GiB, ahead: 0, local: true } },
])("a compaction pass treats a host headroom signal $case as no room", ({ signal }) =>
  Effect.gen(function* () {
    const result = yield* guardedPass(
      {},
      {
        before: Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const config = yield* ServerConfig.ServerConfig;
          const at = DateTime.formatIso(DateTime.makeUnsafe(startupTime + signal.ahead));
          yield* fs.writeFileString(
            path.join(path.dirname(config.dbPath), "storage-headroom.json"),
            JSON.stringify({
              availableBytes: signal.availableBytes,
              updatedAt: "local" in signal ? at.replace("Z", "") : at,
            }),
          );
        }).pipe(Effect.orDie),
      },
    );
    assert.equal(result.first, "paused");
    assert.equal(result.leftAtFirst, 120);
  }),
);

it.effect("a compaction pass without a host headroom signal runs as before", () =>
  Effect.gen(function* () {
    const result = yield* guardedPass({});
    assert.equal(result.first, "completed");
    assert.equal(result.leftAtFirst, 1);
  }),
);

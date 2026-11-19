import { DatabaseReclaimError, type DatabaseStorageStatus } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as SqlClient from "effect/sql/SqlClient";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect has no free-space query.
import * as NodeFSP from "node:fs/promises";

import { resolveHostCliCommand } from "../cli/invocation.ts";
import * as ServerConfig from "../config.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import { forkParked } from "../serverActivation.ts";

// Bound each reclaim pass to one minute of churn.
const RECLAIM_PAGES_PER_PASS = 1024;
const COMPACTION_INTERVAL = Duration.hours(6);

// A compaction pass rewrites index pages and WAL frames that file system snapshots can keep,
// so it writes only while the database's file system and, where the host publishes one, the host's
// headroom are above their floors. Otherwise it waits and checks again, its progress kept.
const MIN_FREE_MB = Config.Int("T3CODE_EVENT_PRUNE_MIN_FREE_MB").pipe(Config.withDefault(2048));
const MIN_HOST_HEADROOM_MB = Config.Int("T3CODE_EVENT_PRUNE_MIN_HOST_HEADROOM_MB").pipe(
  Config.withDefault(65_536),
);
// The host's signal: JSON { availableBytes, updatedAt } it rewrites at least hourly. Absent: no
// host limit. Unreadable or older than HEADROOM_SIGNAL_MAX_AGE: no room, since the host stopped.
const HEADROOM_FILE = Config.String("T3CODE_EVENT_PRUNE_HEADROOM_FILE").pipe(Config.option);
const HEADROOM_SIGNAL_MAX_AGE = Duration.hours(2);
// Clocks differ a little; a value further ahead than this is wrong, not fresh.
const HEADROOM_SIGNAL_MAX_AHEAD = Duration.minutes(10);
const UTC_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$/;
const HEADROOM_RECHECK = Duration.minutes(5);
// A passing check covers the writes of this long; deletes come in batches of milliseconds.
const HEADROOM_CHECK_CACHE = Duration.seconds(30);
const HeadroomSignal = Schema.fromJsonString(
  Schema.Struct({ availableBytes: Schema.Number, updatedAt: Schema.String }),
);
const decodeHeadroomSignal = Schema.decodeUnknownEffect(HeadroomSignal);

export class DatabaseStorage extends Context.Service<
  DatabaseStorage,
  {
    /** Return free pages to the filesystem on databases with incremental auto_vacuum. */
    readonly reclaim: Effect.Effect<void>;
    /** Current file accounting, re-published after each maintenance pass. Null until first read. */
    readonly status: Stream.Stream<DatabaseStorageStatus | null>;
    /** Checks free space, then stops the server; its shutdown runs the offline repair. */
    readonly requestReclaim: Effect.Effect<void, DatabaseReclaimError>;
  }
>()("t3/persistence/DatabaseStorage") {}

/** Stops this server so its desktop app or background service starts it again; null when run by hand. */
export const ServerRestart = Context.Reference<Effect.Effect<void> | null>(
  "t3/persistence/DatabaseStorage/ServerRestart",
  { defaultValue: () => null },
);

const formatGiB = (bytes: number) => `${(bytes / 1024 ** 3).toFixed(1)} GiB`;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const statusRef = yield* SubscriptionRef.make<DatabaseStorageStatus | null>(null);
  const restart = yield* ServerRestart;
  const reclaimFiles = DatabaseMaintenance.reclaimFiles(path, config.dbPath);
  let reclaimFailure = Option.getOrUndefined(
    yield* fs.readFileString(reclaimFiles.failure).pipe(Effect.option),
  );

  // The notice shows host-shell quoting verbatim; desktop needs its launcher because t3 is absent from PATH.
  const { command: compactCommand } = yield* resolveHostCliCommand("doctor", [
    "--base-dir",
    config.baseDir,
  ]);
  // doctor --base-dir targets userdata, not implicit dev homes; suppress status to avoid touching the live install.
  const doctorTargetsThisDatabase =
    path.resolve(config.dbPath) ===
    path.resolve(config.baseDir, "userdata", path.basename(config.dbPath));
  // Only VACUUM changes these, and doctor refuses to VACUUM while a server has the database open.
  let layout:
    | {
        readonly pageSize: number;
        readonly autoVacuum: number;
        readonly incrementalVacuum: boolean;
      }
    | undefined;
  const readLayout = Effect.suspend(() =>
    layout !== undefined
      ? Effect.succeed(layout)
      : Effect.gen(function* () {
          const size = yield* sql<{ readonly page_size: number }>`PRAGMA page_size`;
          const mode = yield* sql<{ readonly auto_vacuum: number }>`PRAGMA auto_vacuum`;
          const autoVacuum = mode[0]?.auto_vacuum ?? 0;
          layout = {
            pageSize: size[0]?.page_size ?? 0,
            autoVacuum,
            incrementalVacuum: autoVacuum === 2,
          };
          return layout;
        }),
  );
  const readPages = Effect.gen(function* () {
    const { pageSize, incrementalVacuum } = yield* readLayout;
    const pages = yield* sql<{ readonly page_count: number }>`PRAGMA page_count`;
    const free = yield* sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`;
    return {
      databaseBytes: (pages[0]?.page_count ?? 0) * pageSize,
      reclaimableBytes: (free[0]?.freelist_count ?? 0) * pageSize,
      incrementalVacuum,
    };
  });
  const publishStatus = Effect.gen(function* () {
    if (!doctorTargetsThisDatabase) return;
    const { databaseBytes, reclaimableBytes, incrementalVacuum } = yield* readPages;
    const next: DatabaseStorageStatus = {
      databaseBytes,
      reclaimableBytes,
      incrementalVacuumEnabled: incrementalVacuum,
      compactCommand,
      canReclaim: restart !== null,
      ...(reclaimFailure === undefined ? {} : { reclaimFailure }),
    };
    // Every set reaches each open Storage page; most passes change nothing.
    if (!Equal.equals(yield* SubscriptionRef.get(statusRef), next)) {
      yield* SubscriptionRef.set(statusRef, next);
    }
  }).pipe(
    Effect.catch((cause) => Effect.logWarning("Database storage inspection failed", { cause })),
  );

  const reclaim = Effect.suspend(() => {
    let page = 0;
    return Effect.gen(function* () {
      if (!(yield* readLayout).incrementalVacuum) return;
      // The driver steps once per statement; incremental_vacuum frees one page per step. Yield between pages.
      for (; page < RECLAIM_PAGES_PER_PASS; page++) {
        const free = yield* sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`;
        if ((free[0]?.freelist_count ?? 0) === 0) break;
        yield* sql`PRAGMA incremental_vacuum(1)`;
        yield* Effect.yieldNow;
      }
    }).pipe(
      Effect.ensuring(Effect.suspend(() => Effect.annotateCurrentSpan("db.reclaimed_pages", page))),
      Effect.catch((cause) => Effect.logWarning("Database space reclamation failed", { cause })),
      // The pass owns the span; tracing each page's two PRAGMAs floods the server trace.
      Effect.withTracerEnabled(false),
      Effect.withSpan("DatabaseStorage.reclaim"),
    );
  });

  const minFreeBytes = (yield* MIN_FREE_MB.pipe(Effect.orElseSucceed(() => 2048))) * 2 ** 20;
  const minHostHeadroomBytes =
    (yield* MIN_HOST_HEADROOM_MB.pipe(Effect.orElseSucceed(() => 65_536))) * 2 ** 20;
  const headroomFile = Option.getOrElse(
    yield* HEADROOM_FILE.pipe(Effect.orElseSucceed(() => Option.none<string>())),
    () => path.join(path.dirname(config.dbPath), "storage-headroom.json"),
  );
  const formatMiB = (bytes: number) => `${Math.floor(bytes / 2 ** 20)} MiB`;
  // Why the pass may not write now, or undefined when it may.
  const headroomShortfall = Effect.gen(function* () {
    const disk = yield* Effect.tryPromise({
      try: () => NodeFSP.statfs(path.dirname(config.dbPath)),
      // An unknown free space does not stop the pass; the host signal still can.
      catch: () => undefined,
    }).pipe(Effect.option);
    if (Option.isSome(disk) && disk.value.bavail * disk.value.bsize < minFreeBytes) {
      return `the database's file system has ${formatMiB(disk.value.bavail * disk.value.bsize)} free, below ${formatMiB(minFreeBytes)}`;
    }
    // Only a missing file means no host limit; one the server cannot read is no room.
    const text = yield* fs.readFileString(headroomFile).pipe(
      Effect.asSome,
      Effect.catchTags({
        PlatformError: (error) =>
          Effect.succeed(
            error.reason._tag === "NotFound" ? Option.none<string>() : Option.some(""),
          ),
      }),
    );
    if (Option.isNone(text)) return undefined;
    const signal = yield* decodeHeadroomSignal(text.value).pipe(Effect.option);
    if (
      Option.isNone(signal) ||
      !Number.isSafeInteger(signal.value.availableBytes) ||
      signal.value.availableBytes < 0 ||
      !UTC_TIMESTAMP.test(signal.value.updatedAt)
    ) {
      return `the host headroom signal ${headroomFile} is unreadable`;
    }
    const age = DateTime.toEpochMillis(yield* DateTime.now) - Date.parse(signal.value.updatedAt);
    if (
      !(age <= Duration.toMillis(HEADROOM_SIGNAL_MAX_AGE)) ||
      age < -Duration.toMillis(HEADROOM_SIGNAL_MAX_AHEAD)
    ) {
      return `the host headroom signal ${headroomFile} is stale`;
    }
    if (signal.value.availableBytes < minHostHeadroomBytes) {
      return `the host reports ${formatMiB(signal.value.availableBytes)} of headroom, below ${formatMiB(minHostHeadroomBytes)}`;
    }
    return undefined;
  });
  let headroomCheckedUntil = 0;
  // Returns whether it waited.
  const awaitHeadroom = Effect.gen(function* () {
    if ((yield* Clock.currentTimeMillis) < headroomCheckedUntil) return false;
    let paused = false;
    while (true) {
      const shortfall = yield* headroomShortfall;
      if (shortfall === undefined) break;
      if (!paused) {
        yield* Effect.logWarning("Event-log compaction paused: no room to write", {
          reason: shortfall,
        });
        paused = true;
      }
      yield* Effect.sleep(HEADROOM_RECHECK);
    }
    if (paused) yield* Effect.logInfo("Event-log compaction resumed");
    headroomCheckedUntil =
      (yield* Clock.currentTimeMillis) + Duration.toMillis(HEADROOM_CHECK_CACHE);
    return paused;
  });

  // Even a pass that deletes nothing scans the event store. Persist completion
  // so restarting does not trigger another scan before the six-hour interval.
  const compactedAtPath = path.join(path.dirname(config.dbPath), "compacted-at");
  const compact = Effect.gen(function* () {
    let sliceStarted = yield* Clock.monotonicTimeNanos;
    const betweenPages = Effect.gen(function* () {
      const elapsed = (yield* Clock.monotonicTimeNanos) - sliceStarted;
      if (elapsed < 25_000_000n) return;
      // Keep background maintenance at 10% duty, including pages that exceed the budget.
      yield* Effect.sleep((Number(elapsed) / 1_000_000) * 9);
      sliceStarted = yield* Clock.monotonicTimeNanos;
    });
    // A wait for headroom is not work: the duty slice starts again after it.
    const beforeWrite = Effect.gen(function* () {
      if (yield* awaitHeadroom) sliceStarted = yield* Clock.monotonicTimeNanos;
    });
    // Background priority: the commit lane and every other writer already waiting go first.
    const result = yield* maintenance
      .pruneEventStore(betweenPages, beforeWrite)
      .pipe(NodeSqliteClient.background);
    const completedAt = yield* Clock.currentTimeMillis;
    yield* fs.writeFileString(compactedAtPath, String(completedAt)).pipe(Effect.ignore);
    if (result.deletedEventCount > 0 || result.deletedReceiptCount > 0) {
      yield* Effect.logInfo("Compacted orchestration history", result);
    }
  }).pipe(
    Effect.catch((cause) =>
      Effect.logWarning("Orchestration history compaction failed", { cause }),
    ),
  );
  const lastCompactedAt = yield* fs.readFileString(compactedAtPath).pipe(
    Effect.map(Number),
    Effect.orElseSucceed(() => Number.NaN),
  );
  const intervalMs = Duration.toMillis(COMPACTION_INTERVAL);
  const untilDueMs = Number.isFinite(lastCompactedAt)
    ? lastCompactedAt + intervalMs - (yield* Clock.currentTimeMillis)
    : 0;
  const firstCompactionDelay = Duration.millis(
    Math.min(intervalMs, Math.max(Duration.toMillis(Duration.minutes(1)), untilDueMs)),
  );

  yield* forkParked(
    Effect.sleep(firstCompactionDelay).pipe(
      Effect.andThen(
        compact.pipe(
          Effect.andThen(publishStatus),
          Effect.repeat(Schedule.spaced(COMPACTION_INTERVAL)),
        ),
      ),
    ),
  );
  yield* forkParked(
    publishStatus.pipe(
      Effect.andThen(Effect.sleep("1 minute")),
      Effect.andThen(
        reclaim.pipe(Effect.andThen(publishStatus), Effect.repeat(Schedule.spaced("1 minute"))),
      ),
    ),
  );

  const requestReclaim = Effect.gen(function* () {
    if (restart === null || !doctorTargetsThisDatabase) {
      return yield* new DatabaseReclaimError({
        reason: `Stop T3 Code on this machine, then run: ${compactCommand}`,
      });
    }
    // Only databases without automatic shrinking need it; a stale dialog can ask after another client's repair.
    if ((yield* readLayout).autoVacuum !== 0) {
      return yield* new DatabaseReclaimError({
        reason: "This database already shrinks on its own. Nothing to reclaim.",
      });
    }
    const { databaseBytes, reclaimableBytes } = yield* readPages;
    const shortfall = yield* DatabaseMaintenance.reclaimSpaceShortfall(
      config.dbPath,
      databaseBytes - reclaimableBytes,
    ).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
    );
    if (shortfall !== null) {
      return yield* new DatabaseReclaimError({
        reason: `Reclaiming needs ${formatGiB(shortfall.neededBytes)} of free disk space on this machine. ${formatGiB(shortfall.freeBytes)} is free.`,
      });
    }
    yield* fs.remove(reclaimFiles.failure, { force: true });
    reclaimFailure = undefined;
    yield* publishStatus;
    const requestedAt = yield* Clock.currentTimeMillis;
    // One step: a request interrupted between them would repair on some unrelated later exit.
    yield* Effect.uninterruptible(
      fs.writeFileString(reclaimFiles.request, String(requestedAt)).pipe(
        // Reply before stopping: the restart closes this connection.
        Effect.andThen(restart.pipe(Effect.delay("500 millis"), Effect.forkDetach)),
      ),
    );
    yield* Effect.logInfo("Restarting to reclaim database space");
  }).pipe(
    Effect.mapError((cause) =>
      cause._tag === "DatabaseReclaimError"
        ? cause
        : new DatabaseReclaimError({ reason: "Could not prepare the repair. Try again." }),
    ),
    Effect.withSpan("DatabaseStorage.requestReclaim"),
  );

  return DatabaseStorage.of({
    reclaim,
    status: SubscriptionRef.changes(statusRef),
    requestReclaim,
  });
});

export const layer = Layer.effect(DatabaseStorage, make);

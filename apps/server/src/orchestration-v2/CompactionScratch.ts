// @effect-diagnostics nodeBuiltinImport:off - the scratch store is a worker thread owning a native SQLite handle; its files are created and removed here.
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeWorkerThreads from "node:worker_threads";

/**
 * The compaction pass's working set — every keyed entity's oldest candidate so far — kept in a
 * scratch SQLite database owned by a worker thread, so neither the heap nor the main thread carries
 * it: the store is file-backed with a fixed cache, and its statements (and the disk waits they can
 * run into beside a multi-gigabyte store) never run on the event loop. Nothing in it outlives a pass.
 */
interface CompactionScratch {
  readonly lookup: (
    keys: ReadonlyArray<string>,
  ) => Effect.Effect<Map<string, number | null>, CompactionScratchError>;
  /** Queued behind the previous messages; a failure surfaces on the next lookup. */
  readonly upsert: (
    entries: ReadonlyArray<readonly [key: string, oldest: number | null]>,
  ) => Effect.Effect<void, CompactionScratchError>;
}

export class CompactionScratchError extends Schema.TaggedError<CompactionScratchError>()(
  "CompactionScratchError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

/** What a key costs on disk, measured at ≈ 207 bytes (8 KiB pages filled by random inserts), rounded up. */
const SCRATCH_BYTES_PER_KEY = 256;
/** Free space a pass insists on: twice its worst-case scratch size. */
const SCRATCH_FREE_SPACE_FACTOR = 2;
// SQLite keeps this much of the working set in memory; the rest lives in the file.
const SCRATCH_CACHE_KIB = 32768;
const SCRATCH_PAGE_SIZE = 8192;

/** The scratch database and the sidecars SQLite may create beside it. */
const scratchFiles = (scratchPath: string): ReadonlyArray<string> => [
  scratchPath,
  `${scratchPath}-wal`,
  `${scratchPath}-shm`,
];

/** The free space the pass needs for `keys` entities, in bytes. */
export const requiredFreeBytes = (keys: number) =>
  keys * SCRATCH_BYTES_PER_KEY * SCRATCH_FREE_SPACE_FACTOR;

/** Whether the scratch file's filesystem can take a pass over `keys` entities; unknown counts as yes. */
export const hasRoomFor = (scratchPath: string, keys: number) =>
  Effect.try(() => {
    const stats = NodeFS.statfsSync(NodePath.dirname(scratchPath));
    return stats.bavail * stats.bsize >= requiredFreeBytes(keys);
  }).pipe(
    Effect.catch((cause) =>
      Effect.logWarning("Could not read free space for the compaction scratch store", {
        scratchPath,
        cause,
      }).pipe(Effect.as(true)),
    ),
  );

// The worker's source is inline so it also runs from bundles and single-executable builds.
const scratchWorkerSource = `
const { parentPort, workerData } = require("node:worker_threads");
const { DatabaseSync } = require("node:sqlite");
const describe = (error) => String(error && error.message ? error.message : error);
let db;
let select;
let upsert;
try {
  db = new DatabaseSync(workerData.filename);
  // Scratch data in one transaction that is never committed: no journal, no fsync; pages beyond the
  // cache spill to the file once. A crash leaves a file the next pass replaces.
  db.exec(\`
    PRAGMA page_size = \${workerData.pageSize};
    PRAGMA journal_mode = OFF;
    PRAGMA synchronous = OFF;
    PRAGMA cache_size = -\${workerData.cacheKib};
    CREATE TABLE candidates (entity_key TEXT PRIMARY KEY, oldest INTEGER) WITHOUT ROWID;
    BEGIN;
  \`);
  select = db.prepare(
    "SELECT entity_key, oldest FROM candidates WHERE entity_key IN (SELECT value FROM json_each(?))",
  );
  upsert = db.prepare(
    "INSERT INTO candidates (entity_key, oldest) " +
      "SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]') FROM json_each(?) WHERE true " +
      "ON CONFLICT (entity_key) DO UPDATE SET oldest = excluded.oldest",
  );
  parentPort.postMessage({ type: "ready" });
} catch (error) {
  if (db !== undefined) {
    try { db.close(); } catch {}
  }
  parentPort.postMessage({ type: "failed", message: describe(error) });
}
parentPort.on("message", (request) => {
  try {
    if (request.type === "lookup") {
      parentPort.postMessage({ id: request.id, rows: select.all(request.keys) });
    } else if (request.type === "upsert") {
      // Answered only when it fails: the next lookup is queued behind it anyway.
      upsert.run(request.entries);
    } else if (request.type === "close") {
      db.close();
      parentPort.postMessage({ id: request.id, rows: [] });
    }
  } catch (error) {
    parentPort.postMessage({ id: request.id, error: describe(error) });
  }
});
`;

type ScratchReply =
  | { readonly type: "ready" }
  | { readonly type: "failed"; readonly message: string }
  | { readonly id: number; readonly rows: ReadonlyArray<Record<string, unknown>> }
  | { readonly id: number; readonly error: string };

type ScratchRequest =
  | { readonly type: "lookup"; readonly keys: string }
  | { readonly type: "close" };

// The worker takes its arguments as JSON text, which SQLite's json_each reads directly.
const encodeKeys = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
const encodeEntries = Schema.encodeSync(
  Schema.fromJsonString(Schema.Array(Schema.Tuple([Schema.String, Schema.NullOr(Schema.Number)]))),
);

const failure = (message: string, cause?: unknown) =>
  new CompactionScratchError(cause === undefined ? { message } : { message, cause });

const isScratchError = Schema.is(CompactionScratchError);

/** Makes room for the scratch file: whatever a previous pass left is replaced, never read. */
const clearScratchPath = (scratchPath: string) => {
  const existing = NodeFS.statSync(scratchPath, { throwIfNoEntry: false });
  if (existing?.isDirectory()) {
    try {
      NodeFS.rmdirSync(scratchPath);
    } catch (cause) {
      throw failure(
        `A directory is in the way of the compaction scratch file: ${scratchPath}`,
        cause,
      );
    }
  }
  for (const file of scratchFiles(scratchPath)) NodeFS.rmSync(file, { force: true });
};

/**
 * Removes what a previous pass left at `scratchPath` (an empty directory included; a non-empty one
 * is reported), so that a stale scratch file never counts against the free space the pass needs.
 */
export const prepare = (scratchPath: string) =>
  Effect.try({
    try: () => clearScratchPath(scratchPath),
    catch: (cause) =>
      isScratchError(cause)
        ? cause
        : failure(`Could not prepare the compaction scratch file: ${scratchPath}`, cause),
  });

type Started = {
  readonly worker: NodeWorkerThreads.Worker;
  /** Settles with the startup failure, or with nothing; it never rejects, so nothing is ever unhandled. */
  readonly started: Promise<CompactionScratchError | undefined>;
  readonly pending: Map<number, (reply: ScratchReply) => void>;
  readonly stopped: () => string | undefined;
  readonly failedUpsert: () => string | undefined;
};

/** Starts the worker with its listeners and its startup promise in one synchronous step. */
const startWorker = (scratchPath: string | undefined): Started => {
  const worker = new NodeWorkerThreads.Worker(scratchWorkerSource, {
    eval: true,
    workerData: {
      filename: scratchPath ?? ":memory:",
      cacheKib: SCRATCH_CACHE_KIB,
      pageSize: SCRATCH_PAGE_SIZE,
    },
  });
  const pending = new Map<number, (reply: ScratchReply) => void>();
  let stopped: string | undefined;
  let failedUpsert: string | undefined;
  let announce: (reply: ScratchReply) => void = () => {};
  const started = new Promise<CompactionScratchError | undefined>((resolve) => {
    announce = (reply) => {
      announce = () => {};
      if ("type" in reply && reply.type === "ready") resolve(undefined);
      else resolve(failure("type" in reply ? reply.message : "lost the scratch store"));
    };
  });
  const stop = (message: string) => {
    stopped = message;
    announce({ type: "failed", message });
    for (const [id, resume] of pending) resume({ id, error: message });
    pending.clear();
  };
  worker.on("message", (reply: ScratchReply) => {
    if ("type" in reply) {
      announce(reply);
      return;
    }
    const resume = pending.get(reply.id);
    if (resume === undefined) {
      if ("error" in reply) failedUpsert ??= reply.error;
      return;
    }
    pending.delete(reply.id);
    resume(reply);
  });
  worker.on("error", (cause) => stop(`Compaction scratch store failed: ${cause.message}`));
  worker.on("exit", (code) => stop(`Compaction scratch store exited with code ${code}`));
  return {
    worker,
    started,
    pending,
    stopped: () => stopped,
    failedUpsert: () => failedUpsert,
  };
};

/** Tear-down steps, each attempted whatever the previous one did; the scratch store is disposable. */
const cleanupSteps = (worker: NodeWorkerThreads.Worker, scratchPath: string | undefined) => [
  ["terminate", () => worker.terminate().then(() => undefined)] as const,
  ...(scratchPath === undefined ? [] : scratchFiles(scratchPath)).map(
    (file) => [`remove ${file}`, () => NodeFS.rmSync(file, { force: true })] as const,
  ),
];

/**
 * Opens the pass's working-set store at `scratchPath` (in memory when absent) for the scope's
 * lifetime: the worker is stopped and the files removed on every exit — best effort, logged, never
 * raised, so a cleanup failure cannot fail the pass that already ran.
 */
export const open: (
  scratchPath: string | undefined,
) => Effect.Effect<CompactionScratch, CompactionScratchError, Scope.Scope> = Effect.fn(
  "CompactionScratch.open",
)(function* (scratchPath: string | undefined) {
  if (scratchPath !== undefined) yield* prepare(scratchPath);
  const { worker, started, pending, stopped, failedUpsert } = yield* Effect.acquireRelease(
    Effect.try({
      try: () => startWorker(scratchPath),
      catch: (cause) => failure("Could not start the compaction scratch store", cause),
    }),
    ({ worker }) =>
      Effect.forEach(
        cleanupSteps(worker, scratchPath),
        ([what, step]) =>
          Effect.tryPromise({
            try: async () => {
              await step();
            },
            catch: (cause) => failure(`Compaction scratch store cleanup failed: ${what}`, cause),
          }).pipe(
            Effect.catch((error) =>
              Effect.logWarning("Compaction scratch store cleanup failed", { cause: error }),
            ),
          ),
        { discard: true },
      ),
  );
  const startupFailure = yield* Effect.promise(() => started);
  if (startupFailure !== undefined) return yield* startupFailure;

  let nextRequestId = 0;
  const broken = () => {
    const message = stopped() ?? failedUpsert();
    return message === undefined ? undefined : failure(message);
  };
  const request = (input: ScratchRequest) =>
    Effect.callback<ReadonlyArray<Record<string, unknown>>, CompactionScratchError>((resume) => {
      const error = broken();
      if (error !== undefined) {
        resume(Effect.fail(error));
        return;
      }
      const id = nextRequestId++;
      pending.set(id, (reply) => {
        if ("rows" in reply) resume(Effect.succeed(reply.rows));
        else
          resume(Effect.fail(failure("error" in reply ? reply.error : "lost the scratch store")));
      });
      // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
      worker.postMessage({ id, ...input });
      return Effect.sync(() => void pending.delete(id));
    });

  return {
    lookup: (keys) =>
      keys.length === 0
        ? Effect.succeed(new Map<string, number | null>())
        : Effect.map(request({ type: "lookup", keys: encodeKeys(keys) }), (rows) => {
            const found = new Map<string, number | null>();
            for (const row of rows) {
              found.set(String(row.entity_key), row.oldest === null ? null : Number(row.oldest));
            }
            return found;
          }),
    upsert: (entries) =>
      Effect.suspend(() => {
        const error = broken();
        if (error !== undefined) return Effect.fail(error);
        if (entries.length > 0) {
          const message = { id: nextRequestId++, type: "upsert", entries: encodeEntries(entries) };
          // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
          worker.postMessage(message);
        }
        return Effect.void;
      }),
  } satisfies CompactionScratch;
});

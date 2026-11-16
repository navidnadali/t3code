import * as NodeSqlite from "node:sqlite";
import * as NodeUtil from "node:util";
import * as NodeWorkerThreads from "node:worker_threads";

import * as Cache from "effect/Cache";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import { identity } from "effect/Function";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import * as Scheduler from "effect/Scheduler";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Context from "effect/Context";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as Reactivity from "effect/reactivity/Reactivity";
import * as Client from "effect/sql/SqlClient";
import type { Borrower, Connection } from "effect/sql/SqlConnection";
import { SqlError, classifySqliteError } from "effect/sql/SqlError";
import * as Statement from "effect/sql/Statement";

const ATTR_DB_SYSTEM_NAME = "db.system.name";

export interface SqliteClientConfig {
  readonly filename: string;
  readonly readonly?: boolean | undefined;
  readonly spanAttributes?: Record<string, unknown> | undefined;
  /** Ignored for in-memory databases, which cannot share a second connection. */
  readonly readerWorker?: boolean | undefined;
  /** Most reader workers to run at once (1 to 32), each started on demand; defaults to 4. */
  readonly readerPoolSize?: number | undefined;
  /** Runs the writer and WAL checkpoints off the event loop. Ignored for in-memory databases. */
  readonly writerWorker?: boolean | undefined;
  /** Writer page cache in MiB; unset or 0 keeps SQLite's default. */
  readonly cacheSizeMb?: number | undefined;
  /** Page cache in MiB for each reader worker; unset or 0 keeps SQLite's default. */
  readonly readerCacheSizeMb?: number | undefined;
  /** Experimental: memory map in MiB for the writer and reader workers; unset or 0 keeps it off. */
  readonly mmapSizeMb?: number | undefined;
}

// A negative cache_size is a 32-bit KiB count; larger values silently become 0.
export const SQLITE_MAX_CACHE_SIZE_MB = 2 ** 21;
// Node's bundled SQLite is built with SQLITE_MAX_MMAP_SIZE=0x7fff0000 and silently clamps larger maps.
export const SQLITE_MAX_MMAP_SIZE_MB = 2047;

const mebibytes = (
  options: SqliteClientConfig,
  name: "cacheSizeMb" | "readerCacheSizeMb" | "mmapSizeMb",
  maximum: number,
) => {
  const value = options[name] ?? 0;
  if (!Number.isSafeInteger(value) || value < 0 || value > maximum) {
    throw new RangeError(`SQLite ${name} must be an integer from 0 to ${maximum}, got ${value}`);
  }
  return value;
};

const tuningPragmas = (cacheSizeMb: number, mmapSizeMb: number) =>
  [
    ...(cacheSizeMb > 0 ? [`PRAGMA cache_size = ${-cacheSizeMb * 1024};`] : []),
    ...(mmapSizeMb > 0 ? [`PRAGMA mmap_size = ${mmapSizeMb * 1024 * 1024};`] : []),
  ].join(" ");

// SqlClient masks transaction acquisition, so readOnly carries the caller's interruption policy.
const ReadOnly = Context.Reference<typeof Effect.interruptible | undefined>(
  "@t3tools/shared/nodeSqliteClient/ReadOnly",
  { defaultValue: () => undefined },
);

/** Wrap the whole transaction; existing write transactions retain the writer, and writes on the reader fail as readonly. */
export const readOnly = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.uninterruptibleMask((restore) =>
    restore(Effect.provideService(effect, ReadOnly, restore)),
  );

/**
 * Permits for reads of a thread's whole history, one fewer than the reader workers (at least one).
 * Such a read holds one from before its transaction until decoded, so this bounds the history row
 * sets held at once.
 */
export const ReaderPermits = Context.Reference<Semaphore.Semaphore | undefined>(
  "@t3tools/shared/nodeSqliteClient/ReaderPermits",
  { defaultValue: () => undefined },
);

const readerPermitCount = (readers: number) => Math.max(1, readers - 1);

const OnReaderWorker = Context.Reference<boolean>(
  "@t3tools/shared/nodeSqliteClient/OnReaderWorker",
  { defaultValue: () => false },
);

/**
 * Concurrency for the statements of one read transaction: together on a reader worker, where they
 * share a round trip; in turn on the writer, where forking them gains nothing and lets the reading
 * client take the writer again before reads waiting for it.
 */
export const readConcurrency: Effect.Effect<"unbounded" | undefined> = Effect.gen(function* () {
  return (yield* OnReaderWorker) ? "unbounded" : undefined;
});

/**
 * An idle reader always takes a queued read. When classes compete, a class passed over once per
 * reader goes first (most passed over first), then `small` (default) or `page` reads holding no
 * reader, then small, page, history; history reads holding all readers but two (min 1) go last.
 */
export type ReadClass = "small" | "page" | "history";

const ReadClassRef = Context.Reference<ReadClass>("@t3tools/shared/nodeSqliteClient/ReadClass", {
  defaultValue: () => "small",
});

/** Marks a read of one page of a thread's history, bounded by the page. */
export const pageRead = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.provideService(effect, ReadClassRef, "page");

/** Marks a read of a thread's whole history. */
export const historyRead = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.provideService(effect, ReadClassRef, "history");

/** historyRead for a stream. */
export const historyReadStream = <A, E, R>(
  stream: Stream.Stream<A, E, R>,
): Stream.Stream<A, E, R> => Stream.provideService(stream, ReadClassRef, "history");

const historyReaderCount = (readers: number) => Math.max(1, readers - 2);

const READ_CLASSES: ReadonlyArray<ReadClass> = ["small", "page", "history"];

interface SlotWaiter {
  readonly readClass: ReadClass;
  woken: boolean;
  readonly wake: () => void;
}

// Reader slots handed out by class. A woken waiter takes the slot handed to it when it runs, so one
// interrupted in between takes nothing and passes the slot on; reads never pass queued ones.
class ReaderSlots {
  free: number;
  woken = 0;
  readonly held: Record<ReadClass, number> = { small: 0, page: 0, history: 0 };
  readonly wokenFor: Record<ReadClass, number> = { small: 0, page: 0, history: 0 };
  readonly passedOver: Record<ReadClass, number> = { small: 0, page: 0, history: 0 };
  readonly historyCap: number;
  readonly agingLimit: number;
  readonly waiters: Record<ReadClass, Set<SlotWaiter>> = {
    small: new Set(),
    page: new Set(),
    history: new Set(),
  };

  constructor(readers: number) {
    this.free = readers;
    this.historyCap = historyReaderCount(readers);
    this.agingLimit = Math.max(1, readers);
  }

  private queued(readClass: ReadClass): number {
    return this.waiters[readClass].size - this.wokenFor[readClass];
  }

  // Compared in turn, lowest first: history reads at their limit go last; a class passed over once
  // per reader goes before the rest, the most passed over first; a small or page class holding no
  // reader next; then class order.
  private rank(readClass: ReadClass, order: number): ReadonlyArray<number> {
    const holding = this.held[readClass] + this.wokenFor[readClass];
    const overLimit = readClass === "history" && holding >= this.historyCap;
    const passedOver = this.passedOver[readClass];
    const aged = passedOver >= this.agingLimit;
    const served = readClass === "history" || holding > 0;
    return [overLimit ? 1 : 0, aged ? -passedOver : 0, served ? 1 : 0, order];
  }

  private firstQueued(readClass: ReadClass): SlotWaiter | undefined {
    for (const waiter of this.waiters[readClass]) if (!waiter.woken) return waiter;
    return undefined;
  }

  private next(): ReadClass | undefined {
    let next: ReadClass | undefined;
    let nextRank: ReadonlyArray<number> = [];
    for (const [order, readClass] of READ_CLASSES.entries()) {
      if (this.queued(readClass) === 0) continue;
      const rank = this.rank(readClass, order);
      const first = rank.findIndex((value, index) => value !== nextRank[index]);
      if (next === undefined || rank[first]! < nextRank[first]!)
        [next, nextRank] = [readClass, rank];
    }
    return next;
  }

  take(readClass: ReadClass): Effect.Effect<void> {
    return Effect.suspend(() => {
      if (this.free - this.woken <= 0 || READ_CLASSES.some((other) => this.queued(other) > 0))
        return this.wait(readClass);
      this.free--;
      this.held[readClass]++;
      return Effect.void;
    });
  }

  private wait(readClass: ReadClass): Effect.Effect<void> {
    return Effect.callback<void>((resume) => {
      const waiter: SlotWaiter = {
        readClass,
        woken: false,
        wake: () => {
          waiter.woken = true;
          resume(Effect.suspend(() => (this.claim(waiter) ? Effect.void : this.take(readClass))));
        },
      };
      this.waiters[readClass].add(waiter);
      return Effect.withFiber((fiber) => {
        // Interrupted after being woken, it gives the slot handed to it to the next waiter.
        const wokenSlot = waiter.woken;
        this.forget(waiter);
        if (wokenSlot) this.wakeWaiters(fiber);
        return Effect.void;
      });
    });
  }

  // A woken waiter takes the slot handed to it, whatever has queued since.
  private claim(waiter: SlotWaiter): boolean {
    if (!this.forget(waiter)) return false;
    this.free--;
    this.held[waiter.readClass]++;
    return true;
  }

  private forget(waiter: SlotWaiter): boolean {
    const queue = this.waiters[waiter.readClass];
    if (!queue.delete(waiter)) return false;
    if (queue.size === 0) this.passedOver[waiter.readClass] = 0;
    if (waiter.woken) {
      this.woken--;
      this.wokenFor[waiter.readClass]--;
    }
    return true;
  }

  release(readClass: ReadClass): Effect.Effect<void> {
    return Effect.withFiber((fiber) => {
      this.free++;
      this.held[readClass]--;
      this.wakeWaiters(fiber);
      return Effect.void;
    });
  }

  private wakeWaiters(fiber: Fiber.Fiber<unknown, unknown>) {
    if (READ_CLASSES.every((readClass) => this.queued(readClass) === 0)) return;
    fiber.currentDispatcher.scheduleTask(() => {
      while (this.free - this.woken > 0) {
        const readClass = this.next();
        if (readClass === undefined) return;
        const waiter = this.firstQueued(readClass)!;
        for (const other of READ_CLASSES)
          if (other !== readClass && this.queued(other) > 0) this.passedOver[other]++;
        this.passedOver[readClass] = 0;
        this.woken++;
        this.wokenFor[readClass]++;
        waiter.wake();
      }
    }, 0);
  }
}

interface Acquired {
  readonly connection: "writer" | "reader";
  readonly lockWaitMs: number;
}

interface ConnectionOwner {
  readonly fiberId: number;
  readonly name: string;
  readonly traceId?: string;
  readonly spanId?: string;
}

const connectionOwner = (fiber: Fiber.Fiber<unknown, unknown>): ConnectionOwner => {
  const current = Context.getOrUndefined(fiber.context, Tracer.ParentSpan);
  if (current?._tag !== "Span") return { fiberId: fiber.id, name: "untraced" };
  let span: Tracer.Span = current;
  while (span.name === "sqlite" || span.name === "sql.transaction") {
    const parent: Tracer.AnySpan | undefined = Option.getOrUndefined(span.parent);
    if (parent?._tag !== "Span") break;
    span = parent;
  }
  return { fiberId: fiber.id, name: span.name, traceId: span.traceId, spanId: span.spanId };
};

const annotateAcquired = (
  fiber: Fiber.Fiber<unknown, unknown>,
  acquired: Acquired | undefined,
  ownerAtWaitStart?: ConnectionOwner,
) => {
  const span = Context.getOrUndefined(fiber.context, Tracer.ParentSpan);
  if (span?._tag !== "Span" || acquired === undefined) return;
  span.attribute("db.connection", acquired.connection);
  span.attribute("db.lock_wait_ms", acquired.lockWaitMs);
  if (ownerAtWaitStart !== undefined) {
    span.attribute("db.lock_owner_at_start.fiber_id", ownerAtWaitStart.fiberId);
    span.attribute("db.lock_owner_at_start.name", ownerAtWaitStart.name);
    if (ownerAtWaitStart.traceId !== undefined) {
      span.attribute("db.lock_owner_at_start.trace_id", ownerAtWaitStart.traceId);
    }
    if (ownerAtWaitStart.spanId !== undefined) {
      span.attribute("db.lock_owner_at_start.span_id", ownerAtWaitStart.spanId);
    }
  }
};

const TransactionAcquire = Context.Reference<
  | {
      acquired?: Acquired;
      ownerAtWaitStart: ConnectionOwner | undefined;
      restore: typeof Effect.interruptible | undefined;
    }
  | undefined
>("@t3tools/shared/nodeSqliteClient/TransactionAcquire", { defaultValue: () => undefined });

export class UnsupportedNodeSqliteVersionError extends Schema.TaggedError<UnsupportedNodeSqliteVersionError>()(
  "UnsupportedNodeSqliteVersionError",
  {
    nodeVersion: Schema.String,
    requirement: Schema.String,
  },
) {
  override get message(): string {
    return `Node.js ${this.nodeVersion} is missing required node:sqlite APIs. Upgrade to ${this.requirement}.`;
  }
}

export class UnsupportedNodeSqliteOperationError extends Schema.TaggedError<UnsupportedNodeSqliteOperationError>()(
  "UnsupportedNodeSqliteOperationError",
  {},
) {
  override get message(): string {
    return "Node SQLite does not support executeStream.";
  }
}

const checkNodeSqliteCompat = Effect.fn("checkNodeSqliteCompat")(function () {
  const parts = process.versions.node.split(".").map(Number);
  const major = parts[0] ?? 0;
  const minor = parts[1] ?? 0;
  const supported = (major === 22 && minor >= 16) || (major === 23 && minor >= 11) || major >= 24;

  if (!supported) {
    return Effect.die(
      new UnsupportedNodeSqliteVersionError({
        nodeVersion: process.versions.node,
        requirement: "Node.js >=22.16, >=23.11, or >=24",
      }),
    );
  }
  return Effect.void;
});

// node:sqlite supplies errcode; classifySqliteError expects errno.
const classifyError = (cause: unknown, message: string, operation: string) => {
  if (
    Predicate.hasProperty(cause, "errcode") &&
    typeof cause.errcode === "number" &&
    !Predicate.hasProperty(cause, "errno")
  ) {
    Object.assign(cause, { errno: cause.errcode });
  }
  return classifySqliteError(cause, { message, operation });
};

// SQLite retains text/blob bindings until rebind or finalization; node:sqlite cannot clear them.
const LARGE_BINDING_SIZE = 64 * 1024;

const isBlob = (value: unknown): value is ArrayBufferView | ArrayBufferLike =>
  ArrayBuffer.isView(value) || NodeUtil.types.isAnyArrayBuffer(value);

const bindingSize = (value: unknown): number => {
  if (typeof value === "string") {
    return value.length;
  }
  return isBlob(value) ? value.byteLength : 0;
};

const hasLargeBinding = (params: ReadonlyArray<unknown>): boolean => {
  try {
    let size = 0;
    for (const param of params) {
      if (
        (typeof param === "object" || typeof param === "function") &&
        param !== null &&
        !isBlob(param)
      ) {
        // Named parameters may be callable objects; read data properties only to avoid invoking getters twice.
        for (const property of Object.values(Object.getOwnPropertyDescriptors(param))) {
          if (!property.enumerable) continue;
          if (!("value" in property)) return true;
          size += bindingSize(property.value);
        }
      } else {
        size += bindingSize(param);
      }
    }
    return size > LARGE_BINDING_SIZE;
  } catch {
    return true;
  }
};

// A reader refuses a database outside WAL mode with this code; retrying cannot change that.
const NOT_WAL = "ERR_SQLITE_NOT_WAL";

// Inline worker source also runs in bundles and single-executable builds without module files.
const workerSource = `
const { parentPort, workerData } = require("node:worker_threads");
const { DatabaseSync } = require("node:sqlite");
const describe = (error) => ({
  message: String(error && error.message ? error.message : error),
  code: error && error.code,
  errcode: error && error.errcode,
  errstr: error && error.errstr,
});
let db;
try {
  // A read-only open cannot be undone by SQL, unlike PRAGMA query_only.
  const next = new DatabaseSync(workerData.filename, {
    readOnly: workerData.readOnly,
    allowExtension: workerData.allowExtension,
  });
  next.exec("PRAGMA busy_timeout = 5000;");
  if (workerData.pragmas) next.exec(workerData.pragmas);
  // Reads the header, so a file that is not a database fails here rather than on first use.
  const mode = String(next.prepare("PRAGMA journal_mode").get().journal_mode).toLowerCase();
  // Without WAL a reader's shared lock blocks the writer's commits, so refuse.
  if (workerData.requireWal && mode !== "wal") {
    throw Object.assign(new Error("database uses journal_mode " + mode + ", not wal"), { code: ${JSON.stringify(NOT_WAL)} });
  }
  db = next;
  parentPort.postMessage({ type: "ready" });
} catch (error) {
  parentPort.postMessage({ type: "unavailable", ...describe(error) });
}
const statements = new Map();
const prepare = (sql, cached) => {
  if (!cached) return db.prepare(sql);
  let statement = statements.get(sql);
  if (statement === undefined) {
    statement = db.prepare(sql);
    if (statements.size >= workerData.prepareCacheSize) statements.delete(statements.keys().next().value);
  } else {
    statements.delete(sql);
  }
  statements.set(sql, statement);
  return statement;
};
parentPort.on("message", (request) => {
  if (request.type === "close") {
    // Closing lets SQLite checkpoint and remove the WAL, which terminating would skip.
    try {
      if (db !== undefined) db.close();
    } catch {}
    parentPort.close();
    return;
  }
  const { id, sql, params, cached, values, raw, safeIntegers, received } = request;
  let operation = "open";
  let iterator;
  try {
    if (db === undefined) throw new Error("database connection is unavailable");
    operation = "prepare";
    const statement = prepare(sql, cached);
    operation = "execute";
    statement.setReadBigInts(safeIntegers);
    if (statement.columns().length > 0) {
      statement.setReturnArrays(values);
      let rows = [];
      let bytes = 0;
      let pendingBatch = false;
      iterator = statement.iterate(...params);
      for (const row of iterator) {
        let rowBytes = 0;
        for (const key in row) {
          const value = row[key];
          rowBytes += typeof value === "string" ? value.length * 2
            : ArrayBuffer.isView(value) ? value.byteLength : 8;
        }
        if (rows.length > 0 && (rows.length >= 16384 || bytes + rowBytes > 8 * 1024 * 1024)) {
          // Build the next batch during receipt, but allow only one unacknowledged message.
          if (pendingBatch) Atomics.wait(received, 0, 0);
          Atomics.store(received, 0, 0);
          parentPort.postMessage({ id, rows, more: true });
          rows = [];
          bytes = 0;
          pendingBatch = true;
        }
        rows.push(row);
        bytes += rowBytes;
      }
      if (pendingBatch) Atomics.wait(received, 0, 0);
      parentPort.postMessage({ id, rows });
    } else {
      const result = statement.run(...params);
      parentPort.postMessage({ id, rows: raw ? result : [] });
    }
  } catch (error) {
    // A step that throws leaves its statement active; an active write blocks every later COMMIT.
    try {
      iterator?.return();
    } catch {}
    // A failed iterator can report its previous SQLite error when the statement is reused.
    statements.delete(sql);
    parentPort.postMessage({ id, error: { operation, ...describe(error) } });
  }
});
`;

interface WorkerRequest {
  readonly sql: string;
  readonly params: ReadonlyArray<unknown>;
  readonly cached: boolean;
  readonly values: boolean;
  readonly raw: boolean;
}

interface WorkerFailure {
  readonly message: string;
  readonly code?: unknown;
  readonly errcode?: unknown;
  readonly errstr?: unknown;
}

type WorkerStartup =
  | { readonly type: "ready" }
  | ({ readonly type: "unavailable" } & WorkerFailure);

type WorkerReply =
  | { readonly id: number; readonly rows: ReadonlyArray<any>; readonly more?: boolean }
  | {
      readonly id: number;
      readonly error: WorkerFailure & { readonly operation: "open" | "prepare" | "execute" };
    };

const workerFailureMessages = {
  open: "Failed to open database",
  prepare: "Failed to prepare statement",
  execute: "Failed to execute statement",
} as const;

const failed = (cause: unknown, message: string, operation: string) =>
  new SqlError({ reason: classifyError(cause, message, operation) });

const failedWith = (
  { message, code, errcode, errstr }: WorkerFailure,
  operation: keyof typeof workerFailureMessages,
) =>
  failed(
    Object.assign(new Error(message), { code, errcode, errstr }),
    workerFailureMessages[operation],
    operation,
  );

// Read-transaction cleanup succeeds after worker exit because it can no longer change anything.
const endsReadTransaction = (sql: string) => /^\s*ROLLBACK\b|^\s*COMMIT\b/i.test(sql);

// WAL readers run concurrently; one worker serialized every read behind the slowest transaction.
const DEFAULT_READER_POOL_SIZE = 4;
// Each reader is a worker thread with its own connection and page cache.
export const SQLITE_MAX_READER_POOL_SIZE = 32;
const READER_STARTUP_TIMEOUT = Duration.seconds(10);
const READER_RETRY_DELAY = Duration.seconds(1);
const READER_RETRY_MAX_DELAY = Duration.minutes(1);

const readerPoolAvailable = Metric.gauge("t3_sqlite_reader_pool_available", {
  description: "1 while SQLite reads can use reader workers, 0 while they use the writer.",
});

// Bind transactions to one worker; fail after its exit instead of resuming outside the transaction on a replacement.
const makeWorkerConnection = Effect.fnUntraced(function* (options: {
  readonly filename: string;
  readonly role: "reader" | "writer";
  readonly readOnly: boolean;
  readonly pragmas: string;
  readonly onReply?: () => void;
}) {
  const label = options.role === "reader" ? "Database reader" : "Database writer";
  const readOnly = options.readOnly;
  let current:
    | {
        readonly worker: NodeWorkerThreads.Worker;
        readonly connection: Connection;
        readonly started: Promise<WorkerStartup>;
      }
    | undefined;
  let nextRequestId = 0;
  // A replacement writer would lose the pragmas callers set on it, such as foreign_keys.
  let writerStopped: string | undefined;

  const start = () => {
    const worker = new NodeWorkerThreads.Worker(workerSource, {
      eval: true,
      workerData: {
        filename: options.filename,
        readOnly,
        requireWal: options.role === "reader",
        allowExtension: false,
        prepareCacheSize: 200,
        ...(options.pragmas === "" ? {} : { pragmas: options.pragmas }),
      },
    });
    let announce: (startup: WorkerStartup) => void = () => {};
    const started = new Promise<WorkerStartup>((resolve) => {
      announce = resolve;
    });
    let stoppedMessage: string | undefined;
    const pending = new Map<
      number,
      {
        readonly endsTransaction: boolean;
        readonly received: Int32Array;
        readonly rows: Array<unknown>;
        readonly resume: (reply: WorkerReply) => void;
      }
    >();
    // A message the worker cannot receive, such as one with an uncloneable binding, fails its
    // own statement.
    const send = (
      message: WorkerRequest & {
        readonly id: number;
        readonly safeIntegers: boolean;
        readonly received: Int32Array;
      },
    ) => {
      try {
        // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
        worker.postMessage(message);
      } catch (cause) {
        const request = pending.get(message.id);
        if (request === undefined) return;
        pending.delete(message.id);
        if (pending.size === 0) worker.unref();
        request.resume({
          id: message.id,
          error: {
            operation: "execute",
            message: `Statement could not be sent to the ${options.role}: ${
              cause instanceof Error ? cause.message : String(cause)
            }`,
          },
        });
      }
    };
    const stop = (message: string) => {
      stoppedMessage = message;
      if (options.role === "writer") writerStopped = message;
      announce({ type: "unavailable", message });
      if (current?.worker === worker) current = undefined;
      for (const [id, request] of pending) {
        request.resume(
          request.endsTransaction
            ? { id, rows: [] }
            : { id, error: { operation: "execute", message } },
        );
      }
      pending.clear();
    };
    worker.on("message", (reply: WorkerReply | WorkerStartup) => {
      if ("type" in reply) {
        announce(reply);
        if (pending.size === 0) worker.unref();
        return;
      }
      const request = pending.get(reply.id);
      if (request === undefined) return;
      if ("rows" in reply && reply.more) {
        for (const row of reply.rows) request.rows.push(row);
        Atomics.store(request.received, 0, 1);
        Atomics.notify(request.received, 0);
        return;
      }
      pending.delete(reply.id);
      if (pending.size === 0) worker.unref();
      options.onReply?.();
      if ("rows" in reply && request.rows.length > 0) {
        for (const row of reply.rows) request.rows.push(row);
        request.resume({ id: reply.id, rows: request.rows });
      } else {
        request.resume(reply);
      }
    });
    worker.on("error", (cause) => stop(`${label} failed: ${cause.message}`));
    worker.on("exit", (code) => stop(`${label} exited with code ${code}`));

    const request = (input: WorkerRequest) =>
      Effect.withFiber<ReadonlyArray<any>, SqlError>((fiber) => {
        // A COMMIT that never reached a writer must fail; only its ROLLBACK is moot.
        const endsTransaction = readOnly
          ? endsReadTransaction(input.sql)
          : /^\s*ROLLBACK\b/i.test(input.sql);
        const safeIntegers = Boolean(Context.get(fiber.context, Client.SafeIntegers));
        const reply = Effect.callback<ReadonlyArray<any>, SqlError>((resume) => {
          // The fiber can yield between building this effect and running it, so the worker's
          // state is read here, in the same synchronous step as the post: a worker that stopped
          // in that gap must fail the statement rather than never answer it.
          if (stoppedMessage !== undefined) {
            resume(
              endsTransaction
                ? Effect.succeed([])
                : Effect.fail(failedWith({ message: stoppedMessage }, "execute")),
            );
            return;
          }
          const id = nextRequestId++;
          const received = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
          pending.set(id, {
            endsTransaction,
            received,
            rows: [],
            resume: (reply) =>
              resume(
                "rows" in reply
                  ? Effect.succeed(reply.rows)
                  : Effect.fail(failedWith(reply.error, reply.error.operation)),
              ),
          });
          if (pending.size === 1) worker.ref();
          const cached = input.cached && !hasLargeBinding(input.params);
          send({ id, safeIntegers, ...input, cached, received });
        });
        // A posted write runs to the end, as it did on the event loop; the caller and its lock
        // must not move on while the worker is still applying it. Reads stay cancellable.
        return options.role === "writer" ? Effect.uninterruptible(reply) : reply;
      });

    const connection = identity<Connection>({
      execute(sql, params, rowTransform) {
        const effect = request({ sql, params, cached: true, values: false, raw: false });
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeRaw(sql, params) {
        return request({ sql, params, cached: true, values: false, raw: true });
      },
      executeValues(sql, params) {
        return request({ sql, params, cached: true, values: true, raw: false });
      },
      executeValuesUnprepared(sql, params) {
        return request({ sql, params: params ?? [], cached: false, values: true, raw: false });
      },
      executeUnprepared(sql, params, rowTransform) {
        // Read snapshots need deferred BEGIN; readonly connections reject IMMEDIATE and EXCLUSIVE.
        const statement = readOnly && /^\s*BEGIN\b/i.test(sql) ? "BEGIN" : sql;
        const effect = request({
          sql: statement,
          params: params ?? [],
          cached: false,
          values: false,
          raw: false,
        });
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeStream(_sql, _params) {
        return Stream.die(new UnsupportedNodeSqliteOperationError());
      },
    });
    current = { worker, connection, started };
    return current;
  };

  yield* Effect.addFinalizer(() =>
    Effect.promise(async () => {
      const open = current;
      if (open === undefined) return;
      if (options.role === "writer") {
        await new Promise<void>((resolve) => {
          open.worker.once("exit", () => resolve());
          open.worker.ref();
          // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
          open.worker.postMessage({ type: "close" });
        });
      }
      await open.worker.terminate();
    }),
  );

  const retire = (worker: NodeWorkerThreads.Worker) => {
    if (current?.worker === worker) current = undefined;
    void worker.terminate();
  };

  return Effect.suspend((): Effect.Effect<Connection, SqlError> => {
    if (current === undefined && writerStopped !== undefined) {
      return Effect.fail(failedWith({ message: writerStopped }, "execute"));
    }
    let entry: NonNullable<typeof current>;
    try {
      entry = current ?? start();
    } catch (cause) {
      return Effect.fail(failed(cause, workerFailureMessages.open, "open"));
    }
    const started = Effect.promise(() => entry.started);
    return Effect.flatMap(
      Effect.withFiber((fiber) => {
        const restore = Context.get(fiber.context, ReadOnly) ?? identity;
        // Readers fall back to the writer; the writer has no fallback, so a slow disk only delays it.
        return restore(
          options.role === "reader"
            ? Effect.timeoutOption(started, READER_STARTUP_TIMEOUT)
            : Effect.asSome(started),
        );
      }),
      (startup): Effect.Effect<Connection, SqlError> => {
        const outcome = Option.getOrUndefined(startup);
        if (outcome?.type === "ready") {
          // A late ready event must not hand out a worker retired by another acquire timeout.
          return current === entry
            ? Effect.succeed(entry.connection)
            : Effect.fail(failedWith({ message: `${label} stopped while starting` }, "open"));
        }
        retire(entry.worker);
        return Effect.fail(
          failedWith(
            outcome ?? {
              message: `${label} did not open the database within ${Duration.toMillis(READER_STARTUP_TIMEOUT)} ms`,
            },
            "open",
          ),
        );
      },
    );
  });
});

const startFailure = (error: SqlError) => {
  const cause = error.reason.cause;
  return {
    reason: cause instanceof Error ? cause.message : error.message,
    permanent: Predicate.hasProperty(cause, "code") && cause.code === NOT_WAL,
  };
};

// A reader that cannot start turns off the whole pool, so routing never picks a dead slot, until
// one background retry starts it again.
const makeReader = Effect.fnUntraced(function* (
  filename: string,
  pool: { unavailable: boolean },
  pragmas: string,
) {
  const acquire = yield* makeWorkerConnection({
    filename,
    role: "reader",
    readOnly: true,
    pragmas,
  });
  const scope = yield* Effect.scope;
  const context = yield* Effect.context<never>();

  const recover = (delay: Duration.Duration): Effect.Effect<void> =>
    Effect.sleep(delay).pipe(
      Effect.andThen(acquire),
      Effect.matchEffect({
        onSuccess: () =>
          Effect.suspend(() => {
            pool.unavailable = false;
            return Metric.update(readerPoolAvailable, 1).pipe(
              Effect.andThen(
                Effect.logInfo(
                  "SQLite reader worker available again; reads use the reader workers",
                ),
              ),
            );
          }),
        onFailure: (error) => {
          const { reason, permanent } = startFailure(error);
          return permanent
            ? Effect.logInfo(
                "SQLite reader worker retries stopped permanently; reads use the writer",
                { reason },
              )
            : recover(Duration.min(Duration.times(delay, 2), READER_RETRY_MAX_DELAY));
        },
      }),
    );

  return Effect.suspend((): Effect.Effect<Connection | undefined> =>
    pool.unavailable
      ? Effect.undefined
      : acquire.pipe(
          Effect.map((connection) => (pool.unavailable ? undefined : connection)),
          Effect.catch((error) => {
            if (pool.unavailable) return Effect.undefined;
            pool.unavailable = true;
            const { reason, permanent } = startFailure(error);
            // Pool state belongs to the client: never log, meter or retry with the failed read's services.
            return Effect.logWarning("SQLite reader worker unavailable; reads use the writer", {
              reason,
            }).pipe(
              Effect.andThen(Metric.update(readerPoolAvailable, 0)),
              Effect.andThen(
                permanent ? Effect.void : Effect.forkIn(recover(READER_RETRY_DELAY), scope),
              ),
              Effect.updateContext((_: Context.Context<never>) => context),
              Effect.as(undefined),
            );
          }),
        ),
  );
});

// A checkpoint copies WAL pages into the database and syncs it. On the writer's thread every
// write would wait for that sync, so a second connection checkpoints beside the writes.
const CHECKPOINT_FRAMES = 1000;
const CHECKPOINT_IDLE_MS = 100;
// If checkpoints fall behind constant writes, the writer checkpoints itself, delaying writes only.
const WRITER_BACKSTOP_FRAMES = 10_000;

const checkpointWorkerSource = `
const { parentPort, workerData } = require("node:worker_threads");
const { DatabaseSync } = require("node:sqlite");
let db;
let frames;
let passive;
let idle;
const check = (force) => {
  if (db === undefined) {
    db = new DatabaseSync(workerData.filename);
    db.exec("PRAGMA busy_timeout = 5000;");
    // macOS requires F_FULLFSYNC to flush the drive cache.
    db.exec("PRAGMA checkpoint_fullfsync = ON;");
    frames = db.prepare("PRAGMA wal_checkpoint(NOOP)");
    passive = db.prepare("PRAGMA wal_checkpoint(PASSIVE)");
  }
  const before = frames.get();
  let state = before;
  if (state.log - state.checkpointed >= workerData.frames || (force && state.log > state.checkpointed)) {
    passive.get();
    state = frames.get();
  }
  // The WAL restarts only once every frame is checkpointed, so catch up when writes pause.
  // A reader pinning the WAL stops progress; the next write retries instead of a timer loop.
  const progressed = !force || state.checkpointed > before.checkpointed;
  if (state.log > state.checkpointed && progressed) {
    idle = setTimeout(() => run(true), workerData.idleMs);
  }
  parentPort.postMessage({ type: "checked", log: state.log, checkpointed: state.checkpointed });
};
const run = (force) => {
  clearTimeout(idle);
  try {
    check(force);
  } catch (error) {
    parentPort.postMessage({ type: "failed", message: String(error && error.message ? error.message : error) });
  }
};
parentPort.on("message", () => run(false));
`;

const sqliteVersion = () => {
  const db = new NodeSqlite.DatabaseSync(":memory:");
  try {
    return String(db.prepare("SELECT sqlite_version() AS version").get()?.version);
  } finally {
    db.close();
  }
};

// Returns the writer's per-statement notification, or undefined when the writer checkpoints alone.
const startCheckpointWorker = Effect.fnUntraced(function* (filename: string) {
  const warn = (reason: string) =>
    Effect.logWarning("SQLite checkpoint worker unavailable; the writer checkpoints its own WAL", {
      reason,
    });
  const version = sqliteVersion();
  const [major = 0, minor = 0] = version.split(".").map(Number);
  // Older SQLite treats NOOP as PASSIVE, so counting frames would checkpoint on every write.
  if (major < 3 || (major === 3 && minor < 51)) {
    yield* warn(`SQLite ${version} has no NOOP checkpoint`);
    return undefined;
  }
  const started = Result.try(
    () =>
      new NodeWorkerThreads.Worker(checkpointWorkerSource, {
        eval: true,
        workerData: { filename, frames: CHECKPOINT_FRAMES, idleMs: CHECKPOINT_IDLE_MS },
      }),
  );
  if (Result.isFailure(started)) {
    yield* warn(String(started.failure));
    return undefined;
  }
  const worker = started.success;
  worker.unref();
  const runFork = Effect.runForkWith(yield* Effect.context<never>());
  let stopped = false;
  const stop = (reason: string) => {
    if (stopped) return;
    stopped = true;
    void worker.terminate();
    runFork(warn(reason));
  };
  worker.on("message", (message: { readonly type?: unknown; readonly message?: unknown }) => {
    if (message?.type === "failed") stop(String(message.message));
  });
  worker.on("error", (cause) => stop(cause.message));
  worker.on("exit", (code) => stop(`exited with code ${code}`));
  yield* Effect.addFinalizer(() =>
    Effect.promise(() => {
      stopped = true;
      return worker.terminate();
    }),
  );
  let scheduled = false;
  return () => {
    if (stopped || scheduled) return;
    scheduled = true;
    // One check per event-loop turn is enough, however many statements finished in it.
    setImmediate(() => {
      scheduled = false;
      // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
      if (!stopped) worker.postMessage(0);
    });
  };
});

const make = Effect.fn("makeWithDatabase")(function* (
  options: SqliteClientConfig,
): Effect.fn.Return<
  { readonly client: Client.SqlClient; readonly readerPermits: Semaphore.Semaphore },
  SqlError,
  Scope.Scope | Reactivity.Reactivity
> {
  yield* checkNodeSqliteCompat();

  const compiler = Statement.makeCompilerSqlite();
  const isFileDatabase = options.filename !== ":memory:" && options.filename !== "";
  const mmapSizeMb = mebibytes(options, "mmapSizeMb", SQLITE_MAX_MMAP_SIZE_MB);
  const writerPragmas = tuningPragmas(
    mebibytes(options, "cacheSizeMb", SQLITE_MAX_CACHE_SIZE_MB),
    mmapSizeMb,
  );
  const readerPragmas = tuningPragmas(
    mebibytes(options, "readerCacheSizeMb", SQLITE_MAX_CACHE_SIZE_MB),
    mmapSizeMb,
  );
  const readerPoolSize = options.readerPoolSize ?? DEFAULT_READER_POOL_SIZE;
  if (
    !Number.isSafeInteger(readerPoolSize) ||
    readerPoolSize < 1 ||
    readerPoolSize > SQLITE_MAX_READER_POOL_SIZE
  ) {
    throw new RangeError(
      `SQLite readerPoolSize must be an integer from 1 to ${SQLITE_MAX_READER_POOL_SIZE}, got ${readerPoolSize}`,
    );
  }

  const makeConnection = Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const db = yield* Effect.try({
      try: () =>
        new NodeSqlite.DatabaseSync(options.filename, {
          readOnly: options.readonly ?? false,
          allowExtension: false,
        }),
      catch: (cause) =>
        new SqlError({
          reason: classifyError(cause, "Failed to open database", "open"),
        }),
    });
    yield* Scope.addFinalizer(
      scope,
      Effect.try({
        try: () => db.close(),
        catch: (cause) =>
          new SqlError({
            reason: classifyError(cause, "Failed to close database", "close"),
          }),
      }).pipe(Effect.orDie),
    );
    if (writerPragmas !== "") {
      // A page cache size reads the schema, so it waits for another process's lock as the open
      // does; the wait set before (none, just after opening) is restored for the caller.
      yield* Effect.try({
        try: () => {
          const previous = Number(db.prepare("PRAGMA busy_timeout").get()?.timeout ?? 0);
          db.exec("PRAGMA busy_timeout = 5000;");
          try {
            db.exec(writerPragmas);
          } finally {
            db.exec(`PRAGMA busy_timeout = ${previous};`);
          }
        },
        catch: (cause) =>
          new SqlError({
            reason: classifyError(cause, "Failed to configure database", "configure"),
          }),
      });
    }
    const statementReaderCache = new WeakMap<NodeSqlite.StatementSync, boolean>();
    const hasRows = (statement: NodeSqlite.StatementSync): boolean => {
      const cached = statementReaderCache.get(statement);
      if (cached !== undefined) {
        return cached;
      }
      const value = statement.columns().length > 0;
      statementReaderCache.set(statement, value);
      return value;
    };

    const prepare = (sql: string) =>
      Effect.try({
        try: () => db.prepare(sql),
        catch: (cause) =>
          new SqlError({
            reason: classifyError(cause, "Failed to prepare statement", "prepare"),
          }),
      });

    const prepareCache = yield* Cache.makeWith(prepare, {
      capacity: 200,
      // A transient prepare failure must not outlive the lock or missing schema.
      timeToLive: (exit) => (Exit.isSuccess(exit) ? Duration.minutes(10) : Duration.zero),
    });

    const statementFor = (sql: string, params: ReadonlyArray<unknown>) =>
      hasLargeBinding(params) ? prepare(sql) : Cache.get(prepareCache, sql);

    const runStatement = (
      statement: NodeSqlite.StatementSync,
      params: ReadonlyArray<unknown>,
      mode: "rows" | "raw" | "values" = "rows",
    ) =>
      Effect.withFiber<ReadonlyArray<any>, SqlError>((fiber) => {
        let arrays = false;
        let result: Exit.Exit<ReadonlyArray<unknown>, SqlError>;
        try {
          statement.setReadBigInts(Boolean(Context.get(fiber.context, Client.SafeIntegers)));
          if (hasRows(statement)) {
            arrays = mode === "values";
            if (arrays) statement.setReturnArrays(true);
            result = Exit.succeed(statement.all(...(params as any)));
          } else {
            const value = statement.run(...(params as any));
            result = Exit.succeed(
              mode === "raw" ? (value as unknown as ReadonlyArray<unknown>) : [],
            );
          }
        } catch (cause) {
          result = Exit.fail(
            new SqlError({
              reason: classifyError(cause, "Failed to execute statement", "execute"),
            }),
          );
        }
        if (arrays) {
          try {
            statement.setReturnArrays(false);
          } catch (cause) {
            return Exit.die(
              new SqlError({
                reason: classifyError(
                  cause,
                  "Failed to reset statement result mode",
                  "resetResultMode",
                ),
              }),
            );
          }
        }
        return result;
      });

    const run = (sql: string, params: ReadonlyArray<unknown>, raw = false) =>
      Effect.flatMap(statementFor(sql, params), (statement) =>
        runStatement(statement, params, raw ? "raw" : "rows"),
      );
    const runValues = (sql: string, params: ReadonlyArray<unknown>) =>
      Effect.flatMap(statementFor(sql, params), (statement) =>
        runStatement(statement, params, "values"),
      );

    return identity<Connection>({
      execute(sql, params, rowTransform) {
        return rowTransform ? Effect.map(run(sql, params), rowTransform) : run(sql, params);
      },
      executeRaw(sql, params) {
        return run(sql, params, true);
      },
      executeValues(sql, params) {
        return runValues(sql, params);
      },
      executeValuesUnprepared(sql, params) {
        return Effect.flatMap(prepare(sql), (statement) =>
          runStatement(statement, params ?? [], "values"),
        );
      },
      executeUnprepared(sql, params, rowTransform) {
        const effect = prepare(sql).pipe(
          Effect.flatMap((statement) => runStatement(statement, params ?? [])),
        );
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeStream(_sql, _params) {
        return Stream.die(new UnsupportedNodeSqliteOperationError());
      },
    });
  });

  const semaphore = yield* Semaphore.make(1);
  const writerWorker = options.writerWorker === true && isFileDatabase;
  let checkpointAfterWrite: (() => void) | undefined;
  const writerConnection: Effect.Effect<Connection, SqlError> = writerWorker
    ? yield* makeWorkerConnection({
        filename: options.filename,
        role: "writer",
        readOnly: options.readonly ?? false,
        pragmas: writerPragmas,
        onReply: () => checkpointAfterWrite?.(),
      })
    : Effect.succeed(yield* makeConnection);
  // Opening now fails the layer for a file that is not a database, not its first query.
  const opened = yield* writerConnection;
  // Started after the writer, so it stops first and the writer's close can checkpoint.
  if (writerWorker && options.readonly !== true) {
    checkpointAfterWrite = yield* startCheckpointWorker(options.filename);
    if (checkpointAfterWrite !== undefined) {
      yield* opened.executeUnprepared(
        `PRAGMA wal_autocheckpoint = ${WRITER_BACKSTOP_FRAMES}`,
        [],
        undefined,
      );
    }
  }
  const readerPool = { unavailable: false };
  const readers =
    options.readerWorker && isFileDatabase
      ? yield* Effect.forEach(Array.from({ length: readerPoolSize }), () =>
          Effect.map(
            Effect.all([
              makeReader(options.filename, readerPool, readerPragmas),
              Semaphore.make(1),
            ]),
            ([acquire, lock]) => ({
              acquire,
              lock,
              claimed: false,
            }),
          ),
        )
      : [];
  if (readers.length > 0) yield* Metric.update(readerPoolAvailable, 1);
  const slots = new ReaderSlots(readers.length);

  interface Target {
    readonly name: Acquired["connection"];
    readonly lock: Semaphore.Semaphore;
    readonly connection: Connection;
  }
  const writer = Effect.map(writerConnection, (connection): Target => ({
    name: "writer",
    lock: semaphore,
    connection,
  }));
  let activeWriter: ConnectionOwner | undefined;
  // Callers must run route uninterruptibly and pass `claimed` a finalizer that runs `release` once.
  // Waits use the caller's `restore`, not readOnly's, which may have been captured outside a mask.
  const route = (
    claimed: (release: Effect.Effect<void>) => Effect.Effect<void>,
    restore: typeof Effect.interruptible,
  ) =>
    Effect.withFiber((fiber): Effect.Effect<Target, SqlError> => {
      if (readers.length === 0 || readerPool.unavailable || !Context.get(fiber.context, ReadOnly)) {
        return writer;
      }
      const readClass = Context.get(fiber.context, ReadClassRef);
      const claim = Effect.andThen(
        restore(slots.take(readClass)),
        Effect.suspend(() => {
          // Holding a permit leaves a slot idle; the lowest wins, so idle slots never start a worker.
          const reader = readers.find((slot) => !slot.claimed)!;
          reader.claimed = true;
          // A writer fallback frees the slot at once; the finalizer's later run then does nothing.
          let held = true;
          const release = Effect.suspend(() => {
            if (!held) return Effect.void;
            held = false;
            reader.claimed = false;
            return slots.release(readClass);
          });
          return Effect.as(claimed(release), { reader, release });
        }),
      );
      return Effect.flatMap(
        // A scheduler yield after taking the permit must not precede its release finalizer.
        Effect.provideService(claim, Scheduler.PreventSchedulerYield, true),
        ({ reader, release }) =>
          Effect.flatMap(
            Effect.provideService(reader.acquire, ReadOnly, restore),
            (readerConnection) =>
              readerConnection === undefined
                ? Effect.andThen(release, writer)
                : Effect.succeed<Target>({
                    name: "reader",
                    lock: reader.lock,
                    connection: readerConnection,
                  }),
          ),
      );
    });

  const acquired = (
    target: { readonly name: Acquired["connection"] },
    clock: Clock.Clock,
    start: bigint,
  ): Acquired => ({
    connection: target.name,
    lockWaitMs: Number((clock.monotonicTimeNanosUnsafe() - start) / 1000n) / 1000,
  });

  const borrower: Borrower = (use) =>
    Effect.uninterruptibleMask((restore) => {
      let release: Effect.Effect<void> = Effect.void;
      const claimed = (next: Effect.Effect<void>) =>
        Effect.sync(() => {
          release = next;
        });
      // Started before routing, so lock wait includes waiting for reader admission.
      return Effect.withFiber((fiber) => {
        const start = fiber.getRef(Clock.Clock).monotonicTimeNanosUnsafe();
        return Effect.flatMap(route(claimed, restore), (target) =>
          restore(borrowTarget(target, use, start)),
        );
      }).pipe(Effect.ensuring(Effect.suspend(() => release)));
    });
  const borrowTarget = <A, E, R>(
    target: Target,
    use: (connection: Connection) => Effect.Effect<A, E, R>,
    start: bigint,
  ): Effect.Effect<A, E, R> =>
    Effect.withFiber((fiber) => {
      const clock = fiber.getRef(Clock.Clock);
      const ownerAtWaitStart = target.name === "writer" ? activeWriter : undefined;
      const owner = target.name === "writer" ? connectionOwner(fiber) : undefined;
      const useConnection = Effect.suspend(() => {
        if (target.name === "writer") activeWriter = owner;
        if (
          fiber.getRef(References.TracerEnabled) &&
          fiber.getRef(Statement.SpanPropagationEnabled)
        ) {
          annotateAcquired(fiber, acquired(target, clock, start), ownerAtWaitStart);
        }
        return use(target.connection);
      });
      const useOwnedConnection =
        target.name === "writer"
          ? Effect.onExit(useConnection, () =>
              Effect.sync(() => {
                if (activeWriter === owner) activeWriter = undefined;
              }),
            )
          : useConnection;
      return target.lock.withPermits(1)(useOwnedConnection);
    });
  const claimedUntilScopeCloses = (release: Effect.Effect<void>) =>
    Effect.withFiber((fiber) =>
      Scope.addFinalizer(Context.getUnsafe(fiber.context, Scope.Scope), release),
    );
  // Started before routing, so lock wait includes waiting for reader admission.
  const transactionAcquirer = Effect.uninterruptibleMask((maskRestore) =>
    Effect.withFiber((fiber) => {
      const start = fiber.getRef(Clock.Clock).monotonicTimeNanosUnsafe();
      const pending = fiber.getRef(TransactionAcquire);
      const restore = pending?.restore ?? maskRestore;
      return Effect.flatMap(route(claimedUntilScopeCloses, restore), (target) => {
        const scope = Context.getUnsafe(fiber.context, Scope.Scope);
        const clock = fiber.getRef(Clock.Clock);
        const ownerAtWaitStart = target.name === "writer" ? activeWriter : undefined;
        const owner = target.name === "writer" ? connectionOwner(fiber) : undefined;
        const acquire = Effect.tap(restore(target.lock.take(1)), () => {
          if (target.name === "writer") activeWriter = owner;
          if (pending !== undefined) {
            pending.acquired = acquired(target, clock, start);
            pending.ownerAtWaitStart = ownerAtWaitStart;
          }
          return Scope.addFinalizer(
            scope,
            Effect.sync(() => {
              if (target.name === "writer" && activeWriter === owner) activeWriter = undefined;
            }).pipe(Effect.andThen(target.lock.release(1))),
          );
        });
        return Effect.as(
          // A scheduler yield after taking the permit must not precede its release finalizer.
          Effect.provideService(acquire, Scheduler.PreventSchedulerYield, true),
          target.connection,
        );
      });
    }),
  );

  const client = yield* Client.make({
    driver: "node:sqlite",
    acquirer: transactionAcquirer,
    borrower,
    compiler,
    transactionAcquirer,
    // BEGIN IMMEDIATE avoids read-then-write SQLITE_BUSY_SNAPSHOT, which busy_timeout cannot resolve.
    beginTransaction: options.readonly === true ? "BEGIN" : "BEGIN IMMEDIATE",
    spanAttributes: [
      ...(options.spanAttributes ? Object.entries(options.spanAttributes) : []),
      [ATTR_DB_SYSTEM_NAME, "sqlite"],
    ],
  });
  const withTransaction = client.withTransaction;
  const readerPermits = yield* Semaphore.make(readerPermitCount(readers.length));
  const transactional = Object.assign(client, {
    withTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      Effect.uninterruptibleMask((restore) => {
        // SqlClient masks setup; acquisition must preserve the caller's interruptibility.
        const pending: {
          acquired?: Acquired;
          ownerAtWaitStart: ConnectionOwner | undefined;
          restore: typeof Effect.interruptible | undefined;
        } = {
          ownerAtWaitStart: undefined,
          restore,
        };
        const body = Effect.withFiber<A, E, R>((fiber) => {
          annotateAcquired(fiber, pending.acquired, pending.ownerAtWaitStart);
          pending.restore = undefined;
          // Savepoints acquire nothing and keep their transaction's connection.
          return pending.acquired === undefined
            ? effect
            : Effect.provideService(
                effect,
                OnReaderWorker,
                pending.acquired.connection === "reader",
              );
        });
        return restore(
          withTransaction(body).pipe(Effect.provideService(TransactionAcquire, pending)),
        );
      }),
  });
  return { client: transactional, readerPermits };
});

export const layer = (config: SqliteClientConfig): Layer.Layer<Client.SqlClient, SqlError> =>
  Layer.effectContext(
    Effect.map(make(config), ({ client, readerPermits }) =>
      Context.make(Client.SqlClient, client).pipe(Context.add(ReaderPermits, readerPermits)),
    ),
  ).pipe(Layer.provide(Reactivity.layer));

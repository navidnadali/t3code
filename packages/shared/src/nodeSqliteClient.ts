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

import { CurrentWaitObserver } from "./WaitObserver.ts";

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

const DeferredWrite = Context.Reference<boolean>("@t3tools/shared/nodeSqliteClient/DeferredWrite", {
  defaultValue: () => false,
});
/**
 * A withDeferredWrites scope: a carried write of its that fails is kept here until the scope fails, and
 * the scope's end waits for the replies of statements still carrying its writes.
 */
interface DeferredScope {
  failure?: SqlError;
  carriers: number;
  drained: Array<() => void>;
}
const DeferredWriteScope = Context.Reference<DeferredScope | undefined>(
  "@t3tools/shared/nodeSqliteClient/DeferredWriteScope",
  { defaultValue: () => undefined },
);
const FlushDeferredWrites = Context.Reference<boolean>(
  "@t3tools/shared/nodeSqliteClient/FlushDeferredWrites",
  { defaultValue: () => false },
);

/** A statement the writer ran, kept so that its effect can be applied again without the code that issued it. */
export interface RecordedStatement {
  readonly sql: string;
  readonly params: ReadonlyArray<unknown>;
}
const StatementRecorder = Context.Reference<Array<RecordedStatement> | undefined>(
  "@t3tools/shared/nodeSqliteClient/StatementRecorder",
  { defaultValue: () => undefined },
);
// A plain SELECT changes nothing (the client registers no SQL function with side effects).
const isPlainRead = (sql: string) => /^\s*SELECT\b/i.test(sql);

/**
 * Runs `effect`, keeping in `into`, in order, each statement it runs on the writer that may change the
 * database (all but plain SELECTs, its savepoints included), once it succeeded or was queued as a
 * deferred write. Applied again on the state `effect` started from, they reproduce what it wrote.
 */
export const recordStatements =
  (into: Array<RecordedStatement>) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.provideService(effect, StatementRecorder, into);

// A binding as it was when its statement ran: a caller may reuse a buffer after its write.
const keptBinding = (value: unknown): unknown =>
  ArrayBuffer.isView(value)
    ? new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength))
    : value instanceof ArrayBuffer
      ? value.slice(0)
      : value;

const recordingCache = new WeakMap<Connection, Connection>();
/** The writer connection, keeping what a recordStatements scope runs on it. */
const recordingConnection = (connection: Connection): Connection => {
  const cached = recordingCache.get(connection);
  if (cached !== undefined) return cached;
  const kept = <A>(
    sql: string,
    params: ReadonlyArray<unknown> | undefined,
    effect: Effect.Effect<A, SqlError>,
  ): Effect.Effect<A, SqlError> =>
    Effect.withFiber((fiber) => {
      const into = Context.get(fiber.context, StatementRecorder);
      if (into === undefined || isPlainRead(sql)) return effect;
      const kept = { sql, params: (params ?? []).map(keptBinding) };
      return Effect.tap(effect, () =>
        Effect.sync(() => {
          into.push(kept);
        }),
      );
    });
  const recording = identity<Connection>({
    execute: (sql, params, rowTransform) =>
      kept(sql, params, connection.execute(sql, params, rowTransform)),
    executeRaw: (sql, params) => kept(sql, params, connection.executeRaw(sql, params)),
    executeValues: (sql, params) => kept(sql, params, connection.executeValues(sql, params)),
    executeValuesUnprepared: (sql, params) =>
      kept(sql, params, connection.executeValuesUnprepared(sql, params)),
    executeUnprepared: (sql, params, rowTransform) =>
      kept(sql, params, connection.executeUnprepared(sql, params, rowTransform)),
    executeStream: (sql, params, rowTransform) =>
      connection.executeStream(sql, params, rowTransform),
  });
  recordingCache.set(connection, recording);
  return recording;
};

const transactionLevel = (sql: Client.SqlClient, operation: string) =>
  Effect.flatMap(Effect.serviceOption(sql.transactionService), (transaction) =>
    Option.isNone(transaction)
      ? Effect.die(new Error(`${operation} runs only inside a transaction`))
      : Effect.map(Client.activeTransactionConnection(transaction.value), (connection) => ({
          connection,
          depth: transaction.value[1],
        })),
  );

/**
 * Runs recorded statements again, in order, on the connection of the transaction the caller holds. Nothing
 * records them again.
 */
export const applyStatements = (
  sql: Client.SqlClient,
  statements: ReadonlyArray<RecordedStatement>,
) =>
  Effect.flatMap(transactionLevel(sql, "applyStatements"), ({ connection }) =>
    Effect.forEach(
      statements,
      (statement) =>
        Effect.provideService(
          connection.executeRaw(statement.sql, statement.params),
          StatementRecorder,
          undefined,
        ),
      { discard: true },
    ),
  );

/** Runs between a restart's ROLLBACK and its BEGIN, when a test injects a competing write there. */
export const RestartGapProbe = Context.Reference<Effect.Effect<void> | undefined>(
  "@t3tools/shared/nodeSqliteClient/RestartGapProbe",
  { defaultValue: () => undefined },
);

/**
 * Rolls back the caller's transaction at its top level and begins a new one on the same connection, which
 * the caller keeps throughout: no other statement of this process reaches the writer in between. If it
 * cannot begin again, a deferred BEGIN leaves the caller's own ROLLBACK something to end.
 */
export const restartTransaction = (sql: Client.SqlClient) =>
  Effect.flatMap(transactionLevel(sql, "restartTransaction"), ({ connection, depth }) => {
    if (depth !== 0)
      return Effect.die(new Error("restartTransaction runs only at a transaction's top level"));
    const unrecorded = <A>(effect: Effect.Effect<A, SqlError>) =>
      Effect.provideService(effect, StatementRecorder, undefined);
    return Effect.withFiber((fiber) =>
      unrecorded(
        connection.executeUnprepared("ROLLBACK", [], undefined).pipe(
          Effect.andThen(Context.get(fiber.context, RestartGapProbe) ?? Effect.void),
          Effect.andThen(connection.executeUnprepared("BEGIN IMMEDIATE", [], undefined)),
          Effect.tapError(() =>
            Effect.ignore(connection.executeUnprepared("BEGIN", [], undefined)),
          ),
          Effect.asVoid,
        ),
      ),
    );
  });

/** Marks a write whose result the caller ignores; inside withDeferredWrites it may share a later round trip. */
export const deferredWrite = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.provideService(effect, DeferredWrite, true);

/**
 * In a writer-worker transaction, the deferred writes `effect` makes at this transaction level go to the
 * worker with its next statement that awaits a reply, in order, and the rest before `effect` completes,
 * so their failures fail `effect`. In transactions it opens, and elsewhere, they run at once.
 */
export const withDeferredWrites =
  (sql: Client.SqlClient) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | SqlError, R> =>
    Effect.flatMap(Effect.serviceOption(sql.transactionService), (transaction) =>
      Option.isNone(transaction)
        ? // Without withTransaction's connection nothing could flush, so nothing waits.
          effect
        : Effect.provideService(
            effect.pipe(
              Effect.tap(() =>
                // On the transaction's own connection, below statement tracing; a no-op when nothing waits.
                Effect.flatMap(
                  Client.activeTransactionConnection(transaction.value),
                  (connection) =>
                    Effect.provideService(
                      connection.executeUnprepared("SELECT 1", [], undefined),
                      FlushDeferredWrites,
                      true,
                    ),
                ),
              ),
            ),
            // A token per run: a failure stays with the run that hit it.
            DeferredWriteScope,
            { carriers: 0, drained: [] },
          ),
    );

/**
 * For the writes that end a transaction's body: in a writer-worker transaction, the deferred writes
 * `effect` makes wait for the transaction's next statement that awaits a reply, its COMMIT, which
 * carries them and fails with their failure (the worker then rolls the transaction back). Nothing
 * flushes them before: a COMMIT that follows at once costs no round trip of its own for them.
 */
export const withWritesCarriedByCommit =
  (sql: Client.SqlClient) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    Effect.flatMap(Effect.serviceOption(sql.transactionService), (transaction) =>
      Option.isNone(transaction)
        ? effect
        : Effect.provideService(effect, DeferredWriteScope, { carriers: 0, drained: [] }),
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

type WriterPriority = "foreground" | "background";

const WriterPriorityRef = Context.Reference<WriterPriority>(
  "@t3tools/shared/nodeSqliteClient/WriterPriority",
  { defaultValue: () => "foreground" },
);

/**
 * Runs `effect`'s writer acquisitions at background priority: each goes after every foreground one
 * waiting, and a release with no foreground waiter hands the writer to the oldest background one. A
 * foreground acquisition then waits for at most the one background hold in flight, kept short.
 */
export const background = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.provideService(effect, WriterPriorityRef, "background");

/** The subset of a semaphore a connection target uses: one permit at a time. */
interface ConnectionLock {
  readonly take: (permits: 1) => Effect.Effect<unknown>;
  readonly release: (permits: 1) => Effect.Effect<unknown>;
  readonly withPermits: (
    permits: 1,
  ) => <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
}

interface BackgroundWaiter {
  granted: boolean;
  wake: (() => void) | undefined;
}

// How long a background waiter waits before it looks again for an idle writer itself: a
// foreground holder that took the writer before any background waiter queued releases it without
// the hand-off below.
const BACKGROUND_RECHECK_MS = 20;

// Background acquisitions of the writer. Foreground ones use the writer's semaphore exactly as
// they did before, so their order and scheduling are unchanged; while a background waiter queues,
// foreground releases end with a hand-off that gives the writer to the oldest background waiter
// when no foreground acquisition is waiting for it.
class BackgroundWriter {
  private readonly waiters = new Set<BackgroundWaiter>();
  private readonly semaphore: Semaphore.Semaphore;
  /** Gives the writer to the oldest background waiter if it is free and no foreground one waits. */
  readonly handOff: Effect.Effect<void>;
  private readonly release: Effect.Effect<void>;
  /** Foreground locks used while a background waiter queues: the same semaphore, then the hand-off. */
  readonly foregroundHandingOff: ConnectionLock;
  readonly background: ConnectionLock;

  constructor(semaphore: Semaphore.Semaphore) {
    this.semaphore = semaphore;
    // Uninterruptible: the take and the grant are one step, or an interrupt between them leaks it.
    this.handOff = Effect.suspend(() => {
      const next = this.waiters.values().next().value;
      if (next === undefined || this.foregroundWaiting()) return Effect.void;
      return Effect.flatMap(semaphore.takeIfAvailable(1), (taken) =>
        Effect.withFiber((fiber) => {
          if (!taken) return Effect.void;
          // A foreground acquisition that queued meanwhile goes first.
          if (this.foregroundWaiting() || !this.waiters.delete(next)) return semaphore.release(1);
          next.granted = true;
          fiber.currentDispatcher.scheduleTask(() => next.wake?.(), 0);
          return Effect.void;
        }),
      );
    }).pipe(Effect.uninterruptible);
    this.release = Effect.andThen(semaphore.release(1), this.handOff);
    this.foregroundHandingOff = {
      take: () => semaphore.take(1),
      release: () => this.release,
      withPermits: () => (effect) =>
        Effect.ensuring(semaphore.withPermits(1)(effect), this.handOff),
    };
    this.background = {
      // The caller restores interruption around this and runs it without scheduler yields.
      take: () => this.acquire(() => Effect.void, identity),
      release: () => this.release,
      withPermits: () => (effect) =>
        Effect.uninterruptibleMask((restore) =>
          this.acquire(() => Effect.ensuring(restore(effect), this.release), restore),
        ),
    };
  }

  get waiting(): boolean {
    return this.waiters.size > 0;
  }

  // Effect's semaphore keeps its queued acquisitions in `waiters` (effect 4.1.3); unknown, it
  // reads as one waiting so background work never goes ahead of a foreground acquisition.
  private foregroundWaiting(): boolean {
    const waiters = (this.semaphore as unknown as { readonly waiters?: { readonly size: number } })
      .waiters;
    return waiters === undefined || waiters.size > 0;
  }

  // Queues, then tries the hand-off itself (an idle writer), and again every BACKGROUND_RECHECK;
  // only the wait is interruptible, and a waiter handed the writer but interrupted passes it on.
  private acquire<A, E, R>(
    onAcquire: () => Effect.Effect<A, E, R>,
    restore: <X, Y, Z>(effect: Effect.Effect<X, Y, Z>) => Effect.Effect<X, Y, Z>,
  ): Effect.Effect<A, E, R> {
    return Effect.suspend(() => {
      const waiter: BackgroundWaiter = { granted: false, wake: undefined };
      this.waiters.add(waiter);
      const granted = Effect.callback<boolean>((resume) => {
        if (waiter.granted) return resume(Effect.succeed(true));
        waiter.wake = () => resume(Effect.succeed(true));
      });
      // A wall-clock timer: the holder's release is real, whatever clock the caller runs on.
      const recheck = Effect.callback<boolean>((resume) => {
        // @effect-diagnostics-next-line globalTimersInEffect:off -- a real release is wall-clock.
        const timer = setTimeout(() => resume(Effect.succeed(false)), BACKGROUND_RECHECK_MS);
        return Effect.sync(() => clearTimeout(timer));
      });
      const waitOnce = Effect.andThen(
        this.handOff,
        Effect.suspend(() =>
          waiter.granted ? Effect.succeed(true) : Effect.raceFirst(granted, recheck),
        ),
      );
      const wait: Effect.Effect<void> = Effect.flatMap(waitOnce, (done) =>
        done || waiter.granted ? Effect.void : wait,
      );
      return restore(wait).pipe(
        Effect.onInterrupt(() =>
          Effect.suspend(() => {
            if (!waiter.granted) {
              this.waiters.delete(waiter);
              return Effect.void;
            }
            return this.release;
          }),
        ),
        Effect.andThen(Effect.suspend(onAcquire)),
      );
    });
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
  const { id, sql, params, cached, values, raw, safeIntegers, received, prelude } = request;
  let operation = "open";
  let iterator;
  try {
    if (db === undefined) throw new Error("database connection is unavailable");
    if (prelude !== undefined) {
      operation = "execute";
      // Deferred writes run first, in order; the statement that carried them reports their failure.
      for (const [index, deferred] of prelude.entries()) {
        try {
          const statement = prepare(deferred.sql, deferred.cached);
          statement.setReadBigInts(deferred.safeIntegers === true);
          statement.run(...deferred.params);
        } catch (error) {
          statements.delete(deferred.sql);
          // A COMMIT that cannot run must not leave its transaction open.
          if (/^\\s*(?:COMMIT|END)\\b/i.test(sql)) {
            try {
              db.exec("ROLLBACK");
            } catch {}
          }
          parentPort.postMessage({ id, error: { operation, ...describe(error) }, carried: index });
          return;
        }
      }
    }
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
      /** The index of the carried (deferred) statement that failed; the request itself did not run. */
      readonly carried?: number;
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

/** Succeeds once no statement still carries the scope's writes; fails if any of them failed. */
const carriersSettled = (scope: DeferredScope) =>
  Effect.callback<ReadonlyArray<any>, SqlError>((resume) => {
    // Checked where the wait registers, in one step: the fiber can yield before this runs,
    // and a last reply landing in that gap would find no one waiting.
    const settle = () =>
      resume(scope.failure === undefined ? Effect.succeed([]) : Effect.fail(scope.failure));
    if (scope.carriers === 0) settle();
    else scope.drained.push(settle);
  });

/**
 * A waiting write's bindings as of now: buffers are copied, since the caller may reuse them before the
 * write is sent. Undefined when a value is not one SQLite binds, so the write is sent at once instead.
 */
const waitingBindings = (params: ReadonlyArray<unknown>): ReadonlyArray<unknown> | undefined => {
  const copy: Array<unknown> = [];
  for (const value of params) {
    if (
      value === null ||
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "bigint"
    )
      copy.push(value);
    else if (ArrayBuffer.isView(value))
      copy.push(
        new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength)),
      );
    else return undefined;
  }
  return copy;
};

// Read-transaction cleanup succeeds after worker exit because it can no longer change anything.
const endsReadTransaction = (sql: string) => /^\s*ROLLBACK\b|^\s*COMMIT\b/i.test(sql);

// WAL readers run concurrently; one worker serialized every read behind the slowest transaction.
const DEFAULT_READER_POOL_SIZE = 4;
// Each reader is a worker thread with its own connection and page cache.
export const SQLITE_MAX_READER_POOL_SIZE = 32;
const READER_STARTUP_TIMEOUT = Duration.seconds(10);
const READER_RETRY_DELAY = Duration.seconds(1);
const READER_RETRY_MAX_DELAY = Duration.minutes(1);

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
    // Writer only: inside a transaction this connection began, deferred writes wait here for the next
    // statement that awaits a reply, which carries them to the worker.
    let inTransaction = false;
    let deferred: Array<{
      readonly sql: string;
      readonly params: ReadonlyArray<unknown>;
      readonly cached: boolean;
      readonly safeIntegers: boolean;
      readonly scope: DeferredScope | undefined;
    }> = [];
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
      carried: typeof deferred = [],
    ) => {
      const payload =
        carried.length === 0
          ? message
          : { ...message, prelude: carried.map(({ scope: _scope, ...statement }) => statement) };
      try {
        // oxlint-disable-next-line unicorn/require-post-message-target-origin -- Node workers do not accept a target origin.
        worker.postMessage(payload);
      } catch (cause) {
        // Statements it carried were never sent: back in the queue, for the next statement or the rollback.
        if (carried.length > 0) deferred = [...carried, ...deferred];
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
        let begins = false;
        const rollsBack = options.role === "writer" && /^\s*ROLLBACK\b/i.test(input.sql);
        let flushed: DeferredScope | undefined;
        if (options.role !== "writer" && Context.get(fiber.context, FlushDeferredWrites)) {
          return Effect.succeed([]);
        }
        if (options.role === "writer") {
          // A scope whose carried write failed fails on its next statement and at its end, even if the
          // statement that carried the write had its error handled; rollbacks always go through.
          const scope = Context.get(fiber.context, DeferredWriteScope);
          if (scope?.failure !== undefined && !/^\s*ROLLBACK\b/i.test(input.sql)) {
            return Effect.fail(scope.failure);
          }
          if (Context.get(fiber.context, FlushDeferredWrites)) {
            // Statements may still carry this scope's writes: their replies decide, after the
            // flush has carried whatever still waits here.
            if (scope === undefined) return Effect.succeed([]);
            if (deferred.length === 0) return carriersSettled(scope);
            flushed = scope;
          }
          // Only values SQLite binds wait: anything else posts with the statement and fails on its own.
          const bindings =
            inTransaction &&
            stoppedMessage === undefined &&
            scope !== undefined &&
            !input.raw &&
            Context.get(fiber.context, DeferredWrite)
              ? waitingBindings(input.params)
              : undefined;
          if (bindings !== undefined) {
            deferred.push({
              sql: input.sql,
              params: bindings,
              cached: input.cached && !hasLargeBinding(input.params),
              safeIntegers,
              scope,
            });
            return Effect.succeed([]);
          }
          if (/^\s*(?:COMMIT|END)\b|^\s*ROLLBACK\s*(?:TRANSACTION\s*)?;?\s*$/i.test(input.sql)) {
            inTransaction = false;
          }
          begins = /^\s*BEGIN\b/i.test(input.sql);
        }
        const reply = Effect.callback<ReadonlyArray<any>, SqlError>((resume) => {
          // A rollback discards what waits (a SAVEPOINT carries what waited before it); if it fails,
          // it undid nothing, so those writes' scopes wait for its reply and fail with its error.
          // Taken in the step that counts those scopes: a scope never finds its writes in neither.
          const discarded = rollsBack ? deferred : [];
          if (rollsBack) deferred = [];
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
          const prelude = deferred;
          if (prelude.length > 0) deferred = [];
          // Each scope whose writes this statement carries or discards waits for its reply before it can end.
          const carrying = new Set<DeferredScope>();
          for (const statement of [...prelude, ...discarded])
            if (statement.scope !== undefined) carrying.add(statement.scope);
          for (const carried of carrying) carried.carriers += 1;
          pending.set(id, {
            endsTransaction,
            received,
            rows: [],
            resume: (reply) => {
              if (begins && "rows" in reply) inTransaction = true;
              // The failed carried write and the ones after it never ran, and a failed rollback's
              // discarded writes never will: their scopes fail.
              if ("error" in reply) {
                const failure = failedWith(reply.error, reply.error.operation);
                const lost = [
                  ...(reply.carried === undefined ? [] : prelude.slice(reply.carried)),
                  ...discarded,
                ];
                for (const statement of lost) {
                  if (statement.scope !== undefined) statement.scope.failure ??= failure;
                }
              }
              for (const carried of carrying) {
                carried.carriers -= 1;
                if (carried.carriers === 0) for (const done of carried.drained.splice(0)) done();
              }
              resume(
                "rows" in reply
                  ? Effect.succeed(reply.rows)
                  : Effect.fail(failedWith(reply.error, reply.error.operation)),
              );
            },
          });
          if (pending.size === 1) worker.ref();
          const cached = input.cached && !hasLargeBinding(input.params);
          send({ id, safeIntegers, ...input, cached, received }, prelude);
        });
        // A posted write runs to the end, as it did on the event loop; the caller and its lock
        // must not move on while the worker is still applying it. Reads stay cancellable.
        const posted = options.role === "writer" ? Effect.uninterruptible(reply) : reply;
        return flushed === undefined ? posted : Effect.andThen(posted, carriersSettled(flushed));
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
            return Effect.logInfo(
              "SQLite reader worker available again; reads use the reader workers",
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
            // Pool state belongs to the client: never log or retry with the failed read's services.
            return Effect.logWarning("SQLite reader worker unavailable; reads use the writer", {
              reason,
            }).pipe(
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
        // Nothing waits on this connection, so a deferring scope's flush has nothing to send.
        const effect = Effect.withFiber<ReadonlyArray<any>, SqlError>((fiber) =>
          Context.get(fiber.context, FlushDeferredWrites)
            ? Effect.succeed([])
            : prepare(sql).pipe(
                Effect.flatMap((statement) => runStatement(statement, params ?? [])),
              ),
        );
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeStream(_sql, _params) {
        return Stream.die(new UnsupportedNodeSqliteOperationError());
      },
    });
  });

  const semaphore = yield* Semaphore.make(1);
  const backgroundWriter = new BackgroundWriter(semaphore);
  const writerWorker = options.writerWorker === true && isFileDatabase;
  let checkpointAfterWrite: (() => void) | undefined;
  const writerConnection: Effect.Effect<Connection, SqlError> = Effect.map(
    writerWorker
      ? yield* makeWorkerConnection({
          filename: options.filename,
          role: "writer",
          readOnly: options.readonly ?? false,
          pragmas: writerPragmas,
          onReply: () => checkpointAfterWrite?.(),
        })
      : Effect.succeed(yield* makeConnection),
    recordingConnection,
  );
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
  const slots = new ReaderSlots(readers.length);

  interface Target {
    readonly name: Acquired["connection"];
    readonly lock: ConnectionLock;
    readonly background: boolean;
    readonly connection: Connection;
  }
  // Foreground acquisitions take the semaphore itself unless a background waiter queues.
  const writerLock = (priority: WriterPriority): ConnectionLock =>
    priority === "background"
      ? backgroundWriter.background
      : backgroundWriter.waiting
        ? backgroundWriter.foregroundHandingOff
        : semaphore;
  const writer = (priority: WriterPriority) =>
    Effect.map(writerConnection, (connection): Target => ({
      name: "writer",
      lock: writerLock(priority),
      background: priority === "background",
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
      const priority = Context.get(fiber.context, WriterPriorityRef);
      if (readers.length === 0 || readerPool.unavailable || !Context.get(fiber.context, ReadOnly)) {
        return writer(priority);
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
                ? Effect.andThen(release, writer(priority))
                : Effect.succeed<Target>({
                    name: "reader",
                    lock: reader.lock,
                    background: false,
                    connection: readerConnection,
                  }),
          ),
      );
    });

  // The wait observer hears of every acquisition, traced or not.
  const observeWait = (
    fiber: Fiber.Fiber<unknown, unknown>,
    target: Target,
    clock: Clock.Clock,
    start: bigint,
    ownerAtWaitStart: ConnectionOwner | undefined,
  ) =>
    fiber.getRef(CurrentWaitObserver)?.wait(
      // Background waits are long by design; kept apart, they leave the writer's waits as they were.
      target.name === "reader"
        ? "sqlite.reader"
        : target.background
          ? "sqlite.writer.background"
          : "sqlite.writer",
      Number(clock.monotonicTimeNanosUnsafe() - start),
      ownerAtWaitStart?.name,
    );

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
        observeWait(fiber, target, clock, start, ownerAtWaitStart);
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
          observeWait(fiber, target, clock, start, ownerAtWaitStart);
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
          // A deferring scope belongs to its own level: a transaction opened inside it starts without one.
          const own = Effect.provideService(effect, DeferredWriteScope, undefined);
          // Savepoints acquire nothing and keep their transaction's connection.
          return pending.acquired === undefined
            ? own
            : Effect.provideService(own, OnReaderWorker, pending.acquired.connection === "reader");
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

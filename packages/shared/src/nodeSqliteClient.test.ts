import * as NodeSqlite from "node:sqlite";
import type * as NodeWorkerThreads from "node:worker_threads";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Logger from "effect/Logger";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Scheduler from "effect/Scheduler";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as Stream from "effect/Stream";
import type * as Tracer from "effect/Tracer";
import { vi } from "vite-plus/test";

import * as SqliteClient from "./nodeSqliteClient.ts";

const startedWorkers = vi.hoisted((): Array<NodeWorkerThreads.Worker> => []);
const workerExits = vi.hoisted(() => new WeakMap<NodeWorkerThreads.Worker, Promise<void>>());
const workerParams = vi.hoisted(() => new WeakMap<NodeWorkerThreads.Worker, Array<unknown>>());
const workerMessages = vi.hoisted(
  () => new WeakMap<NodeWorkerThreads.Worker, Array<Record<string, unknown>>>(),
);
const workerReplies = vi.hoisted(
  () => new WeakMap<NodeWorkerThreads.Worker, Array<Record<string, unknown>>>(),
);
// Holds every COMMIT sent to a worker until released, and answers the next one with an error.
const commitHold = vi.hoisted(() => ({
  on: false,
  held: [] as Array<() => void>,
  failNext: false,
}));
// Holds the reply to any request whose SQL mentions "hold-reply" until released, and tells a waiter.
const replyHold = vi.hoisted(() => ({
  ids: new Set<number>(),
  held: [] as Array<() => void>,
  waiters: [] as Array<() => void>,
}));
const silentWorkers = vi.hoisted(() => ({ on: false, readyOnTerminate: false }));
const failingWorkers = vi.hoisted(() => ({ message: undefined as string | undefined }));

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      super(
        ...((silentWorkers.on
          ? ["setInterval(() => {}, 1 << 30);", { eval: true }]
          : failingWorkers.message !== undefined
            ? [`throw new Error(${JSON.stringify(failingWorkers.message)});`, { eval: true }]
            : args) as ConstructorParameters<typeof actual.Worker>),
      );
      startedWorkers.push(this);
      workerExits.set(this, new Promise((resolve) => this.once("exit", () => resolve())));
      workerParams.set(this, []);
      workerMessages.set(this, []);
      workerReplies.set(this, []);
      this.on("message", (reply) => {
        if (typeof reply === "object" && reply !== null) workerReplies.get(this)!.push(reply);
      });
    }
    override postMessage(...args: Parameters<NodeWorkerThreads.Worker["postMessage"]>) {
      const [request] = args;
      if (typeof request === "object" && request !== null) {
        workerMessages.get(this)!.push(request as Record<string, unknown>);
        for (const part of "batch" in request ? (request.batch as Array<any>) : [request]) {
          if ("params" in part)
            workerParams.get(this)!.push(...(part.params as ReadonlyArray<unknown>));
        }
        if ("sql" in request && String(request.sql).includes("hold-reply"))
          replyHold.ids.add(request.id as number);
        if ("sql" in request && /^\s*COMMIT\b/i.test(String(request.sql)) && commitHold.on) {
          if (commitHold.failNext) {
            commitHold.failNext = false;
            this.emit("message", {
              id: request.id,
              error: { operation: "execute", message: "commit answered with an error" },
            });
          }
          commitHold.held.push(() => super.postMessage(...args));
          return;
        }
      }
      super.postMessage(...args);
    }
    override emit(event: string | symbol, ...args: Array<any>): boolean {
      const reply = args[0];
      if (
        event === "message" &&
        typeof reply === "object" &&
        reply !== null &&
        replyHold.ids.has(reply.id)
      ) {
        replyHold.ids.delete(reply.id);
        replyHold.held.push(() => super.emit(event, ...args));
        for (const waiter of replyHold.waiters.splice(0)) waiter();
        return true;
      }
      return super.emit(event, ...args);
    }
    override terminate() {
      // A "ready" already queued when termination starts reaches the parent before "exit".
      if (silentWorkers.readyOnTerminate) this.emit("message", { type: "ready" });
      return super.terminate();
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

const withReaderClient = <A, E>(
  use: (sql: SqlClient.SqlClient) => Effect.Effect<A, E, SqlClient.SqlClient>,
  config: Pick<
    SqliteClient.SqliteClientConfig,
    "readerPoolSize" | "cacheSizeMb" | "readerCacheSizeMb" | "mmapSizeMb"
  > = {},
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-reader-" });
    const filename = path.join(directory, "state.sqlite");
    return yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA journal_mode = WAL`;
      yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
      yield* sql`INSERT INTO entries VALUES ('first')`;
      return yield* use(sql);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, readerWorker: true, ...config })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

const countEntries = (sql: SqlClient.SqlClient) =>
  Effect.map(
    sql<{ count: number }>`SELECT COUNT(*) AS count FROM entries`,
    (rows) => rows[0]?.count,
  );

const layer = it.layer(SqliteClient.layer({ filename: ":memory:" }));

layer("NodeSqliteClient", (it) => {
  it.effect("records the writer connection owner on a waiting query", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holder = yield* sql
        .withTransaction(
          sql`SELECT 1`.pipe(
            Effect.andThen(Deferred.succeed(held, undefined)),
            Effect.andThen(Deferred.await(release)),
          ),
        )
        .pipe(Effect.withSpan("test.writerHolder"), Effect.forkChild);
      yield* Deferred.await(held);

      const waitingSpan = yield* Deferred.make<Tracer.Span>();
      const observeWaiter: Statement.Transformer = (statement, _sql, _fiber, span) =>
        Deferred.succeed(waitingSpan, span).pipe(Effect.as(statement));
      const waiter = yield* sql`SELECT 1`.pipe(
        Effect.provideService(Statement.CurrentTransformer, observeWaiter),
        Effect.provideService(Statement.SpanPropagationEnabled, true),
        (effect) => Effect.forkChild(effect, { startImmediately: true }),
      );
      const span = yield* Deferred.await(waitingSpan);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holder);
      yield* Fiber.join(waiter);

      assert.equal(span.attributes.get("db.lock_owner_at_start.name"), "test.writerHolder");
      assert.typeOf(span.attributes.get("db.lock_owner_at_start.fiber_id"), "number");
      assert.typeOf(span.attributes.get("db.lock_owner_at_start.trace_id"), "string");
      assert.typeOf(span.attributes.get("db.lock_owner_at_start.span_id"), "string");
    }),
  );

  it.effect("retries preparing a query after the missing schema becomes available", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const select = sql<{ name: string }>`SELECT name FROM created_after_prepare_failure`;
      const error = yield* select.pipe(Effect.flip);
      assert.equal(error._tag, "SqlError");
      assert.equal(error.reason.operation, "prepare");

      yield* sql`CREATE TABLE created_after_prepare_failure(name TEXT NOT NULL)`;
      yield* sql`INSERT INTO created_after_prepare_failure VALUES ('recovered')`;
      assert.deepEqual(yield* select, [{ name: "recovered" }]);
      assert.deepEqual(yield* select.values, [["recovered"]]);
    }),
  );

  it.effect("classifies constraint failures by their SQLite result code", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE constrained(name TEXT NOT NULL UNIQUE)`;
      yield* sql`INSERT INTO constrained VALUES ('taken')`;

      const duplicate = yield* sql`INSERT INTO constrained VALUES ('taken')`.pipe(Effect.flip);
      assert(duplicate.reason._tag === "UniqueViolation");
      assert.equal(duplicate.reason.constraint, "constrained.name");

      const missing = yield* sql`INSERT INTO constrained VALUES (NULL)`.pipe(Effect.flip);
      assert.equal(missing.reason._tag, "ConstraintError");
    }),
  );
});

const largeText = "x".repeat(64 * 1024 + 1);

it.effect("reads integers on the values path as the caller asked, cached or not", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const value of ["small", largeText]) {
      const values = sql`SELECT 1 AS id, length(${value}) AS size`.values;
      assert.deepEqual(yield* values.pipe(Effect.provideService(SqlClient.SafeIntegers, true)), [
        [1n, BigInt(value.length)],
      ]);
      assert.deepEqual(yield* values, [[1, value.length]]);
    }
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

const makeTempDatabase = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-transaction-" });
  const filename = path.join(directory, "state.sqlite");
  const other = yield* Effect.acquireRelease(
    Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
    (database) => Effect.sync(() => database.close()),
  );
  yield* Effect.sync(() => {
    other.exec(`
      PRAGMA journal_mode = WAL;
      CREATE TABLE counters(id INTEGER PRIMARY KEY, value INTEGER NOT NULL);
      INSERT INTO counters VALUES (1, 0);
    `);
  });
  return { filename, other };
});

it.effect("keeps another connection from committing between a transaction's read and write", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const [row] = yield* sql<{ readonly value: number }>`
            SELECT value FROM counters WHERE id = 1
          `;
          // Deferred BEGIN permits a competing commit, causing SQLITE_BUSY_SNAPSHOT that no timeout can resolve.
          yield* Effect.sync(() =>
            assert.throws(
              () => other.exec("UPDATE counters SET value = value + 1 WHERE id = 1"),
              /database is locked/,
            ),
          );
          yield* sql`UPDATE counters SET value = ${(row?.value ?? 0) + 10} WHERE id = 1`;
        }),
      );
      yield* Effect.sync(() => other.exec("UPDATE counters SET value = value + 1 WHERE id = 1"));
      assert.deepEqual(yield* sql`SELECT value FROM counters WHERE id = 1`.values, [[11]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename })));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("waits for another process's lock while the database opens", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-open-" });
    const filename = path.join(directory, "state.sqlite");
    const seed = new NodeSqlite.DatabaseSync(filename);
    seed.exec("PRAGMA journal_mode = WAL; CREATE TABLE entries(name TEXT NOT NULL)");
    seed.close();
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const holder = yield* spawner.spawn(
      ChildProcess.make(process.execPath, [
        "-e",
        `const db = new (require("node:sqlite").DatabaseSync)(process.argv[1]);
        db.exec("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE");
        console.log("held");
        setTimeout(() => db.close(), 300);`,
        filename,
      ]),
    );
    yield* Stream.runHead(holder.stdout);
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.deepEqual(yield* sql`SELECT COUNT(*) FROM entries`.values, [[0]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, checkpointWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

// A page cache size makes the writer read the schema, which needs the lock the open waits for.
it.effect.each([
  { checkpoint: "its checkpoint worker", checkpointWorker: true, unsupported: false },
  { checkpoint: "no checkpoint worker", checkpointWorker: false, unsupported: false },
  {
    checkpoint: "a Node SQLite without the worker's APIs",
    checkpointWorker: true,
    unsupported: true,
  },
])(
  "waits for another process's lock while the database opens with a page cache size and $checkpoint",
  ({ checkpointWorker, unsupported }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-open-" });
      const filename = path.join(directory, "state.sqlite");
      const seed = new NodeSqlite.DatabaseSync(filename);
      seed.exec("PRAGMA journal_mode = WAL; CREATE TABLE entries(name TEXT NOT NULL)");
      seed.close();
      if (unsupported) {
        const prototype = NodeSqlite.DatabaseSync.prototype;
        const descriptor = Object.getOwnPropertyDescriptor(prototype, "setAuthorizer")!;
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            Reflect.deleteProperty(prototype, "setAuthorizer");
          }),
          () => Effect.sync(() => Object.defineProperty(prototype, "setAuthorizer", descriptor)),
        );
      }
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const holder = yield* spawner.spawn(
        ChildProcess.make(process.execPath, [
          "-e",
          `const db = new (require("node:sqlite").DatabaseSync)(process.argv[1]);
          db.exec("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE");
          console.log("held");
          setTimeout(() => db.close(), 300);`,
          filename,
        ]),
      );
      yield* Stream.runHead(holder.stdout);
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepEqual(yield* sql`SELECT COUNT(*) FROM entries`.values, [[0]]);
        assert.deepEqual(yield* sql`PRAGMA cache_size`.values, [[-1024]]);
        // The wait covers the tuning only; the caller's wait is what it was before.
        assert.deepEqual(yield* sql`PRAGMA busy_timeout`.values, [
          [checkpointWorker && !unsupported ? 5000 : 0],
        ]);
      }).pipe(Effect.provide(SqliteClient.layer({ filename, checkpointWorker, cacheSizeMb: 1 })));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("reports a transaction blocked by another writer as a lock timeout", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA busy_timeout = 0`;
      const read = sql.withTransaction(sql`SELECT value FROM counters WHERE id = 1`.values);

      yield* Effect.sync(() => other.exec("BEGIN IMMEDIATE"));
      const error = yield* read.pipe(Effect.flip);
      assert.equal(error.reason._tag, "LockTimeoutError");

      yield* Effect.sync(() => other.exec("COMMIT"));
      assert.deepEqual(yield* read, [[0]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename })));
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("lets the writer commit while a read-only transaction is open", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const snapshotTaken = yield* Deferred.make<void>();
      const writer = yield* Effect.forkChild(
        Deferred.await(snapshotTaken).pipe(
          Effect.andThen(sql`INSERT INTO entries VALUES ('second')`),
        ),
      );
      const counts = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const before = yield* countEntries(sql);
            yield* Deferred.succeed(snapshotTaken, undefined);
            // Holding the writer lock here would deadlock this join.
            yield* Fiber.join(writer);
            const after = yield* countEntries(sql);
            return { before, after };
          }),
        )
        .pipe(SqliteClient.readOnly);

      assert.deepEqual(counts, { before: 1, after: 1 });
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 2);
    }),
  ),
);

it.effect("serves another read while a read-only transaction is open", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const opened = yield* Deferred.make<void>();
      const other = yield* Effect.forkChild(
        Deferred.await(opened).pipe(Effect.andThen(countEntries(sql).pipe(SqliteClient.readOnly))),
      );
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* countEntries(sql);
            yield* Deferred.succeed(opened, undefined);
            // One shared reader would make this join wait on the transaction forever.
            assert.equal(yield* Fiber.join(other), 1);
          }),
        )
        .pipe(SqliteClient.readOnly);
    }),
  ),
);

// oxlint-disable-next-line t3code/no-real-time-wait-in-tests -- a zero-delay macrotask boundary that lets the worker's queued messages land, not a wait for time
const eventLoopTurn = Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));
// The failure an effect ends with (a success fails the test), typed as its error.
const failureOf = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.map(Effect.exit(effect), (exit) => {
    if (Exit.isSuccess(exit)) return assert.fail("expected a failure");
    return Cause.squash(exit.cause) as E;
  });

const holdReader = (sql: SqlClient.SqlClient, name: string) =>
  Effect.gen(function* () {
    const held = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const fiber = yield* sql
      .withTransaction(
        sql`SELECT ${name} AS name`.pipe(
          Effect.andThen(Deferred.succeed(held, undefined)),
          Effect.andThen(Deferred.await(release)),
        ),
      )
      .pipe(SqliteClient.readOnly, Effect.forkChild);
    yield* Deferred.await(held);
    return Deferred.succeed(release, undefined).pipe(Effect.andThen(Fiber.join(fiber)));
  });

const workerThatRan = (since: number, name: string) =>
  startedWorkers.slice(since).findIndex((worker) => workerParams.get(worker)?.includes(name));

it.effect("runs a waiting read on the first reader that frees up", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const startedBefore = startedWorkers.length;
      const [first, second, ...rest] = yield* Effect.forEach(["a", "b", "c", "d"], (name) =>
        holdReader(sql, name),
      );
      const waiting = yield* sql`SELECT ${"waiting"} AS name`.pipe(
        SqliteClient.readOnly,
        (effect) => Effect.forkChild(effect, { startImmediately: true }),
      );
      yield* second!;
      // Wake-ups run on setImmediate, so one turn lets the waiting read claim the freed slot first.
      yield* eventLoopTurn;
      yield* first!;
      yield* Effect.all(rest);
      assert.deepEqual(yield* Fiber.join(waiting), [{ name: "waiting" }]);

      assert.equal(startedWorkers.length - startedBefore, 4);
      assert.equal(workerThatRan(startedBefore, "waiting"), workerThatRan(startedBefore, "b"));
    }),
  ),
);

it.effect.each([
  { setting: "1", size: 1, workers: 1 },
  { setting: "2", size: 2, workers: 2 },
  { setting: "32", size: 32, workers: 32 },
  { setting: "unset", size: undefined, workers: 4 },
])("caps reader workers at $workers when the pool size is $setting", ({ size, workers }) =>
  withReaderClient(
    (sql) =>
      Effect.gen(function* () {
        const startedBefore = startedWorkers.length;
        const releases = yield* Effect.forEach(
          Array.from({ length: workers }, (_, index) => `held-${index}`),
          (name) => holdReader(sql, name),
        );
        const extra = yield* sql
          .withTransaction(sql`SELECT ${"extra"} AS name`)
          .pipe(SqliteClient.readOnly, (effect) =>
            Effect.forkChild(effect, { startImmediately: true }),
          );
        yield* releases.at(-1)!;
        yield* eventLoopTurn;
        yield* Effect.all(releases.slice(0, -1));
        assert.deepEqual(yield* Fiber.join(extra), [{ name: "extra" }]);

        assert.equal(startedWorkers.length - startedBefore, workers);
        assert.equal(workerThatRan(startedBefore, "extra"), workers - 1);
      }),
    { readerPoolSize: size },
  ),
);

// A permit is free when all of them can be taken at once and no more.
const assertPermits = (expected: number) =>
  Effect.gen(function* () {
    const permits = yield* SqliteClient.ReaderPermits;
    assert.isDefined(permits);
    const all = yield* permits!.withPermitsIfAvailable(expected)(Effect.void);
    const more = yield* permits!.withPermitsIfAvailable(expected + 1)(Effect.void);
    assert.deepEqual([Option.isSome(all), Option.isSome(more)], [true, false]);
  });

// A large read that runs as far as it can at once, then pauses in its transaction until released.
// A read of the given class that runs as far as it can at once, then pauses in its transaction
// until released; `started` records the order in which reads reached their statement.
const holdClassReader = (
  sql: SqlClient.SqlClient,
  name: string,
  started: Array<string>,
  mark: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R> = (effect) => effect,
) =>
  Effect.gen(function* () {
    const running = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const fiber = yield* sql
      .withTransaction(
        sql`SELECT ${name} AS name`.pipe(
          Effect.andThen(Effect.sync(() => started.push(name))),
          Effect.andThen(Deferred.succeed(running, undefined)),
          Effect.andThen(Deferred.await(release)),
        ),
      )
      .pipe(mark, SqliteClient.readOnly, Effect.forkChild({ startImmediately: true }));
    return {
      running: Deferred.await(running),
      release: Deferred.succeed(release, undefined).pipe(Effect.andThen(Fiber.join(fiber))),
    };
  });

// No reader idles while a read waits: with nothing else queued, one class may use every reader.
it.effect.each([
  { name: "page", mark: SqliteClient.pageRead },
  { name: "history", mark: SqliteClient.historyRead },
])("lets $name reads use every idle reader while nothing else waits", ({ name, mark }) =>
  withReaderClient(
    (sql) =>
      Effect.gen(function* () {
        const started: Array<string> = [];
        const reads = yield* Effect.forEach([1, 2, 3, 4], (index) =>
          holdClassReader(sql, `${name}-${index}`, started, mark),
        );
        yield* Effect.forEach(reads, (read) => read.running);
        assert.lengthOf(started, 4);
        yield* Effect.forEach(reads, (read) => read.release);
      }),
    { readerPoolSize: 4 },
  ),
);

it.effect("serves waiting reads by class: small, then page, then history", () =>
  withReaderClient(
    (sql) =>
      Effect.gen(function* () {
        const started: Array<string> = [];
        // One reader, held; the waiters arrive in the reverse of the order they must run in.
        const holder = yield* holdClassReader(sql, "holder", started);
        yield* holder.running;
        const waiters = yield* Effect.forEach(
          [
            ["history", SqliteClient.historyRead],
            ["page", SqliteClient.pageRead],
            ["small", <A, E, R>(effect: Effect.Effect<A, E, R>) => effect],
          ] as const,
          ([name, mark]) => holdClassReader(sql, name, started, mark),
        );
        yield* holder.release;
        for (const waiter of waiters.toReversed()) {
          yield* waiter.running;
          yield* waiter.release;
        }
        assert.deepEqual(started, ["holder", "small", "page", "history"]);
      }),
    { readerPoolSize: 1 },
  ),
);

it.effect("lets a page read pass history reads still waiting for a reader", () =>
  withReaderClient(
    (sql) =>
      Effect.gen(function* () {
        const started: Array<string> = [];
        // Two readers, both held by small reads; two history reads and a page read wait.
        const holders = yield* Effect.forEach(["holder-1", "holder-2"], (name) =>
          holdClassReader(sql, name, started),
        );
        yield* Effect.forEach(holders, (holder) => holder.running);
        const histories = yield* Effect.forEach(["history-1", "history-2"], (name) =>
          holdClassReader(sql, name, started, SqliteClient.historyRead),
        );
        const page = yield* holdClassReader(sql, "page", started, SqliteClient.pageRead);
        // One reader frees: the page read gets it, not the history read queued before it.
        yield* holders[0]!.release;
        yield* page.running;
        // The holders start in either order; the page read must be third, before any history read.
        assert.deepEqual(started.slice(0, 2).toSorted(), ["holder-1", "holder-2"]);
        assert.deepEqual(started.slice(2), ["page"]);
        // Each history read takes a reader as one frees; the first is running before the second
        // reader frees, since a COMMIT is no longer awaited and the two would otherwise race.
        yield* page.release;
        yield* histories[0]!.running;
        yield* holders[1]!.release;
        yield* histories[1]!.running;
        for (const history of histories) yield* history.release;
        assert.deepEqual(started.slice(3), ["history-1", "history-2"]);
      }),
    { readerPoolSize: 2 },
  ),
);

it.effect("drops a history read's place in the queue when it is interrupted waiting", () =>
  withReaderClient(
    (sql) =>
      Effect.gen(function* () {
        const started: Array<string> = [];
        const holder = yield* holdClassReader(sql, "holder", started);
        yield* holder.running;
        const waiting = yield* sql
          .withTransaction(sql`SELECT ${"waiting"} AS name`)
          .pipe(
            SqliteClient.historyRead,
            SqliteClient.readOnly,
            Effect.forkChild({ startImmediately: true }),
          );
        yield* Fiber.interrupt(waiting);
        yield* holder.release;
        const next = yield* holdClassReader(sql, "next", started, SqliteClient.historyRead);
        yield* next.release;
        assert.deepEqual(started, ["holder", "next"]);
      }),
    { readerPoolSize: 1 },
  ),
);

// A read of the given class that records when it reaches its statement, then ends.
const classRead = (
  sql: SqlClient.SqlClient,
  name: string,
  started: Array<string>,
  mark: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R> = (effect) => effect,
) =>
  sql
    .withTransaction(
      sql`SELECT ${name} AS name`.pipe(Effect.andThen(Effect.sync(() => started.push(name)))),
    )
    .pipe(mark, SqliteClient.readOnly, Effect.forkChild({ startImmediately: true }));

it.effect("gives a page read holding no reader the next one before small reads", () =>
  withReaderClient(
    (sql) =>
      Effect.gen(function* () {
        const started: Array<string> = [];
        const holders = yield* Effect.forEach(["holder-1", "holder-2"], (name) =>
          holdClassReader(sql, name, started),
        );
        yield* Effect.forEach(holders, (holder) => holder.running);
        // A small read queues first, then a page read; small reads already hold readers.
        const small = yield* classRead(sql, "small", started);
        const page = yield* classRead(sql, "page", started, SqliteClient.pageRead);
        yield* holders[0]!.release;
        yield* Fiber.join(page);
        yield* holders[1]!.release;
        yield* Fiber.join(small);
        assert.deepEqual(started.slice(2), ["page", "small"]);
      }),
    { readerPoolSize: 2 },
  ),
);

// Passed over once for each reader in the pool, a waiting class goes next: no class starves.
it.effect.each([
  {
    name: "a page read queued behind a stream of small reads",
    stream: "small",
    streamMark: <A, E, R>(effect: Effect.Effect<A, E, R>) => effect,
    waiting: "page",
    waitingMark: SqliteClient.pageRead,
  },
  {
    name: "a history read queued behind a stream of page reads",
    stream: "page",
    streamMark: SqliteClient.pageRead,
    waiting: "history",
    waitingMark: SqliteClient.historyRead,
  },
])("serves $name within the pool size", ({ stream, streamMark, waiting, waitingMark }) =>
  withReaderClient(
    (sql) =>
      Effect.gen(function* () {
        const started: Array<string> = [];
        const holder = yield* holdClassReader(sql, "holder", started);
        yield* holder.running;
        // Twenty reads of one class queue, then the other read: one of the twenty always waits.
        const streamed = yield* Effect.forEach(
          Array.from({ length: 20 }, (_, index) => `${stream}-${index}`),
          (name) => classRead(sql, name, started, streamMark),
        );
        const read = yield* classRead(sql, waiting, started, waitingMark);
        yield* holder.release;
        yield* Effect.forEach([...streamed, read], Fiber.join);
        // One reader: passed over once, by the stream's first read, the other read goes next.
        assert.strictEqual(started.indexOf(waiting), 2);
      }),
    { readerPoolSize: 1 },
  ),
);

// Aging scales with the pool: a waiting class goes next once passed over once per reader.
it.effect.each([{ readers: 1 }, { readers: 2 }, { readers: 4 }, { readers: 8 }])(
  "serves a history read behind a stream of page reads after one per reader ($readers readers)",
  ({ readers }) =>
    withReaderClient(
      (sql) =>
        Effect.gen(function* () {
          const started: Array<string> = [];
          const holders = yield* Effect.forEach(
            Array.from({ length: readers }, (_, index) => `holder-${index}`),
            (name) => holdClassReader(sql, name, started, SqliteClient.pageRead),
          );
          yield* Effect.forEach(holders, (holder) => holder.running);
          const pages = yield* Effect.forEach(
            Array.from({ length: 3 * readers }, (_, index) => `page-${index}`),
            (name) => classRead(sql, name, started, SqliteClient.pageRead),
          );
          const history = yield* classRead(sql, "history", started, SqliteClient.historyRead);
          // One reader frees and cycles through the queue while the other holders keep theirs.
          yield* holders[0]!.release;
          yield* Fiber.join(history);
          assert.strictEqual(started.indexOf("history"), 2 * readers);
          yield* Effect.forEach(holders.slice(1), (holder) => holder.release);
          yield* Effect.forEach(pages, Fiber.join);
        }),
      { readerPoolSize: readers },
    ),
);

it.effect(
  "serves a history read queued behind streams of small and page reads within one round",
  () =>
    withReaderClient(
      (sql) =>
        Effect.gen(function* () {
          const started: Array<string> = [];
          const holder = yield* holdClassReader(sql, "holder", started);
          yield* holder.running;
          const queue = [
            ...Array.from({ length: 20 }, (_, index) => ({
              name: `small-${index}`,
              mark: <A, E, R>(effect: Effect.Effect<A, E, R>) => effect,
            })),
            ...Array.from({ length: 20 }, (_, index) => ({
              name: `page-${index}`,
              mark: SqliteClient.pageRead,
            })),
            { name: "history", mark: SqliteClient.historyRead },
          ];
          const queued = yield* Effect.forEach(queue, ({ name, mark }) =>
            classRead(sql, name, started, mark),
          );
          yield* holder.release;
          yield* Effect.forEach(queued, Fiber.join);
          // Small and page reads each pass the history read over once; then it has waited longest.
          assert.deepEqual(started.slice(0, 4), ["holder", "small-0", "page-0", "history"]);
        }),
      { readerPoolSize: 1 },
    ),
);

it.effect(
  "serves history reads past their limit after other queued reads, however long they wait",
  () =>
    withReaderClient(
      (sql) =>
        Effect.gen(function* () {
          const started: Array<string> = [];
          // Four readers: two history reads (their limit) and two small reads hold them.
          const histories = yield* Effect.forEach(["history-1", "history-2"], (name) =>
            holdClassReader(sql, name, started, SqliteClient.historyRead),
          );
          let smalls = yield* Effect.forEach(["small-1", "small-2"], (name) =>
            holdClassReader(sql, name, started),
          );
          yield* Effect.forEach([...histories, ...smalls], (read) => read.running);
          const waiting = yield* holdClassReader(
            sql,
            "history-3",
            started,
            SqliteClient.historyRead,
          );
          // Four freed readers go to small reads, each passing over the waiting history read.
          for (const round of [1, 2, 3, 4]) {
            const next = yield* holdClassReader(sql, `small-${round + 2}`, started);
            yield* smalls[0]!.release;
            yield* next.running;
            smalls = [...smalls.slice(1), next];
          }
          const page = yield* holdClassReader(sql, "page", started, SqliteClient.pageRead);
          yield* smalls[0]!.release;
          yield* page.running;
          assert.notInclude(started, "history-3");
          yield* histories[0]!.release;
          yield* waiting.running;
          yield* Effect.all([
            waiting.release,
            histories[1]!.release,
            smalls[1]!.release,
            page.release,
          ]);
        }),
      { readerPoolSize: 4 },
    ),
);

// One reader is always left for reads that take no permit, unless there is only one.
it.effect.each([
  { setting: "1", size: 1, permits: 1 },
  { setting: "3", size: 3, permits: 2 },
  { setting: "unset", size: undefined, permits: 3 },
])(
  "gives one reader permit fewer than the readers when the pool size is $setting",
  ({ size, permits }) => withReaderClient(() => assertPermits(permits), { readerPoolSize: size }),
);

it.effect("gives one reader permit without reader workers", () =>
  assertPermits(1).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.effect.each([{ size: 0 }, { size: 33 }, { size: 1.5 }, { size: Number.NaN }])(
  "refuses a reader pool size of $size before opening the database",
  ({ size }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-pool-size-" });
      const filename = path.join(directory, "state.sqlite");
      const exit = yield* Effect.exit(
        Effect.provide(
          Effect.void,
          SqliteClient.layer({ filename, readerWorker: true, readerPoolSize: size }),
        ),
      );
      assert(Exit.isFailure(exit));
      assert.include(
        String(Cause.squash(exit.cause)),
        `SQLite readerPoolSize must be an integer from 1 to 32, got ${size}`,
      );
      assert.isFalse(yield* fs.exists(filename));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

// regression: #19226 read-only transactions run on the separate reader connection
it.effect("rejects a write in a read-only transaction", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const error = yield* sql
        .withTransaction(sql`INSERT INTO entries VALUES ('blocked')`)
        .pipe(SqliteClient.readOnly, Effect.flip);

      assert.equal(error._tag, "SqlError");
      assert.include(String(error.reason.cause), "readonly database");
      assert.equal(yield* countEntries(sql), 1);
    }),
  ),
);

it.effect("keeps the reader read-only even if a statement turns query_only off", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const error = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`PRAGMA query_only = OFF`;
            yield* sql`INSERT INTO entries VALUES ('blocked')`;
          }),
        )
        .pipe(SqliteClient.readOnly, Effect.flip);

      assert.equal(error._tag, "SqlError");
      assert.include(String(error.reason.cause), "readonly database");
      assert.equal(yield* countEntries(sql), 1);
    }),
  ),
);

it.effect("keeps read-only work inside a write transaction on the writer", () =>
  withReaderClient((sql) =>
    sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO entries VALUES ('uncommitted')`;
        const count = yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly);
        assert.equal(count, 2);
      }),
    ),
  ),
);

it.effect("fails a read-only transaction whose reader stops, then starts a new reader", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const startedBefore = startedWorkers.length;
      const error = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* countEntries(sql);
            yield* Effect.promise(() => startedWorkers.at(-1)!.terminate());
            return yield* countEntries(sql);
          }),
        )
        .pipe(SqliteClient.readOnly, Effect.flip);

      assert.equal(error._tag, "SqlError");
      assert.include(String(error.reason.cause), "Database reader exited");
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
      assert.equal(startedWorkers.length, startedBefore + 2);
    }),
  ),
);

const pageCacheAndMap = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const [cache] = yield* sql<{ readonly cache_size: number }>`PRAGMA cache_size`;
    const [mmap] = yield* sql<{ readonly mmap_size: number }>`PRAGMA mmap_size`;
    return { cacheSize: cache?.cache_size, mmapSize: mmap?.mmap_size };
  });

it.effect.each([
  {
    tuning: "configured",
    config: { cacheSizeMb: 64, readerCacheSizeMb: 8, mmapSizeMb: 16 },
    writer: { cacheSize: -64 * 1024, mmapSize: 16 * 1024 * 1024 },
    reader: { cacheSize: -8 * 1024, mmapSize: 16 * 1024 * 1024 },
  },
  {
    tuning: "default",
    config: {},
    writer: { cacheSize: -2000, mmapSize: 0 },
    reader: { cacheSize: -2000, mmapSize: 0 },
  },
])(
  "gives the writer and every reader, restarted ones too, the $tuning page cache and memory map",
  ({ config, writer, reader }) =>
    withReaderClient(
      (sql) =>
        Effect.gen(function* () {
          assert.deepEqual(yield* pageCacheAndMap(sql), writer);
          const startedBefore = startedWorkers.length;
          // Each open read transaction holds its reader, so four of them start the whole pool.
          const release = yield* Deferred.make<void>();
          const transactions = yield* Effect.forEach([0, 1, 2, 3], () =>
            Effect.gen(function* () {
              const opened = yield* Deferred.make<void>();
              const fiber = yield* sql
                .withTransaction(
                  pageCacheAndMap(sql).pipe(
                    Effect.tap(() => Deferred.succeed(opened, undefined)),
                    Effect.tap(() => Deferred.await(release)),
                  ),
                )
                .pipe(SqliteClient.readOnly, Effect.forkChild);
              return { opened, fiber };
            }),
          );
          yield* Effect.forEach(transactions, ({ opened }) => Deferred.await(opened));
          yield* Deferred.succeed(release, undefined);
          assert.deepEqual(yield* Fiber.joinAll(transactions.map(({ fiber }) => fiber)), [
            reader,
            reader,
            reader,
            reader,
          ]);
          const pool = startedWorkers.slice(startedBefore);
          assert.equal(pool.length, 4);

          for (const worker of pool) yield* Effect.promise(() => worker.terminate());
          assert.deepEqual(
            yield* sql.withTransaction(pageCacheAndMap(sql)).pipe(SqliteClient.readOnly),
            reader,
          );
          assert.equal(startedWorkers.length, startedBefore + 5);
        }),
      config,
    ),
);

it.effect.each([
  { setting: "a negative page cache", config: { cacheSizeMb: -1 }, name: "cacheSizeMb" },
  {
    setting: "a fractional reader page cache",
    config: { readerCacheSizeMb: 1.5 },
    name: "readerCacheSizeMb",
  },
  {
    setting: "a page cache past SQLite's 32-bit KiB count",
    config: { cacheSizeMb: SqliteClient.SQLITE_MAX_CACHE_SIZE_MB + 1 },
    name: "cacheSizeMb",
  },
  {
    setting: "a memory map SQLite would clamp",
    config: { mmapSizeMb: SqliteClient.SQLITE_MAX_MMAP_SIZE_MB + 1 },
    name: "mmapSizeMb",
  },
])("refuses $setting before opening the database", ({ config, name }) =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(
      Effect.provide(Effect.void, SqliteClient.layer({ filename: ":memory:", ...config })),
    );
    assert(Exit.isFailure(exit));
    assert.include(String(Cause.squash(exit.cause)), `SQLite ${name} must be an integer from 0 to`);
  }),
);

const withFileClient = <A, E>(
  setup: (
    sql: SqlClient.SqlClient,
    filename: string,
  ) => Effect.Effect<A, E, SqlClient.SqlClient | FileSystem.FileSystem>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-reader-off-" });
    const filename = path.join(directory, "state.sqlite");
    return yield* Effect.gen(function* () {
      return yield* setup(yield* SqlClient.SqlClient, filename);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, readerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

it.effect("uses the writer when the reader cannot open the database", () =>
  withFileClient((sql, filename) =>
    Effect.gen(function* () {
      yield* sql`PRAGMA journal_mode = WAL`;
      yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
      yield* sql`INSERT INTO entries VALUES ('kept')`;
      yield* (yield* FileSystem.FileSystem).remove(filename);
      const startedBefore = startedWorkers.length;

      assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
      assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);

      // A read beside an open read transaction must not start a worker on the next slot.
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holder = yield* sql
        .withTransaction(
          countEntries(sql).pipe(
            Effect.andThen(Deferred.succeed(held, undefined)),
            Effect.andThen(Deferred.await(release)),
          ),
        )
        .pipe(SqliteClient.readOnly, Effect.forkChild);
      yield* Deferred.await(held);
      const other = yield* countEntries(sql).pipe(SqliteClient.readOnly, (effect) =>
        Effect.forkChild(effect, { startImmediately: true }),
      );
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(holder);
      assert.equal(yield* Fiber.join(other), 1);
      assert.equal(startedWorkers.length, startedBefore + 1);
    }),
  ),
);

const withLogLines = <A, E, R>(
  use: (lines: Queue.Queue<ReadonlyArray<unknown>>) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const lines = yield* Queue.unbounded<ReadonlyArray<unknown>>();
    const logger = Logger.make(({ logLevel, message }) => {
      Queue.offerUnsafe(lines, [logLevel, ...(message as ReadonlyArray<unknown>)]);
    });
    return yield* use(lines).pipe(
      Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
    );
  });

const readConnection = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const spans: Array<Tracer.Span> = [];
    const observe: Statement.Transformer = (statement, _sql, _fiber, span) =>
      Effect.sync(() => {
        spans.push(span);
        return statement;
      });
    const count = yield* countEntries(sql).pipe(
      SqliteClient.readOnly,
      Effect.provideService(Statement.CurrentTransformer, observe),
      Effect.provideService(Statement.SpanPropagationEnabled, true),
    );
    return { count, connection: spans.map((span) => span.attributes.get("db.connection")) };
  });

const withWorkers = <A, E, R>(
  mode: { readonly failWith?: string; readonly silent?: boolean },
  effect: Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      failingWorkers.message = mode.failWith;
      silentWorkers.on = mode.silent ?? false;
    }),
    () => effect,
    () =>
      Effect.sync(() => {
        failingWorkers.message = undefined;
        silentWorkers.on = false;
      }),
  );

const unavailableWarning = "SQLite reader worker unavailable; reads use the main connection";

const notWal = "database uses journal_mode delete, not wal";

const readerPoolAvailable = Effect.map(Metric.snapshot, (snapshots) =>
  snapshots.flatMap((snapshot) =>
    snapshot.id === "t3_sqlite_reader_pool_available" && snapshot.type === "Gauge"
      ? [snapshot.state.value]
      : [],
  ),
);

it.effect.each([
  {
    failure: "a crash",
    workers: { failWith: "reader start failed" },
    startup: Duration.zero,
    reason: "Database reader failed: reader start failed",
  },
  {
    failure: "a startup timeout",
    workers: { silent: true },
    startup: Duration.seconds(10),
    reason: "worker did not open the database within 10000 ms",
  },
])(
  "reads on the writer after $failure until a retry starts the reader again",
  ({ workers, startup, reason }) =>
    withLogLines((lines) =>
      withReaderClient((sql) =>
        Effect.gen(function* () {
          // The failed read's own metric attributes must not label the pool's gauge.
          const failed = yield* withWorkers(
            workers,
            Effect.gen(function* () {
              const read = yield* Effect.forkChild(readConnection(sql), {
                startImmediately: true,
              });
              yield* TestClock.adjust(startup);
              return yield* Fiber.join(read);
            }).pipe(Effect.provideService(Metric.CurrentMetricAttributes, { read: "failed" })),
          );
          assert.deepEqual(failed, { count: 1, connection: ["writer"] });
          const startedBefore = startedWorkers.length;
          assert.deepEqual(yield* readConnection(sql), { count: 1, connection: ["writer"] });
          assert.equal(startedWorkers.length, startedBefore);
          assert.deepEqual(yield* Queue.take(lines), ["Warn", unavailableWarning, { reason }]);
          assert.deepEqual(yield* readerPoolAvailable, [0]);

          yield* TestClock.adjust("1 second");
          assert.equal(startedWorkers.length, startedBefore + 1);
          assert.deepEqual(yield* Queue.take(lines), [
            "Info",
            "SQLite reader worker available again; reads use the reader workers",
          ]);
          assert.deepEqual(yield* readerPoolAvailable, [1]);
          assert.deepEqual(yield* readConnection(sql), { count: 1, connection: ["reader"] });
          assert.equal(Queue.sizeUnsafe(lines), 0);
        }),
      ),
    ).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
);

it.effect("backs off a reader that never starts without logging each retry", () =>
  withLogLines((lines) =>
    withReaderClient((sql) =>
      withWorkers(
        { silent: true },
        Effect.gen(function* () {
          const read = yield* Effect.forkChild(readConnection(sql), { startImmediately: true });
          yield* TestClock.adjust("10 seconds");
          assert.deepEqual(yield* Fiber.join(read), { count: 1, connection: ["writer"] });

          const startedBefore = startedWorkers.length;
          // Each retry waits 10 s for startup; the gaps between them double from 1 s up to a minute.
          yield* TestClock.adjust("10 minutes");
          assert.equal(startedWorkers.length - startedBefore, 12);
          assert.equal(Queue.sizeUnsafe(lines), 1);
        }),
      ),
    ),
  ),
);

it.effect.each([
  {
    when: "the database is not in WAL mode",
    journalMode: "DELETE",
    workers: {},
    starts: 1,
    logged: [["Warn", unavailableWarning, { reason: notWal }]],
  },
  {
    when: "a retry finds the database out of WAL mode",
    journalMode: "WAL",
    workers: { failWith: "reader start failed" },
    starts: 2,
    logged: [
      ["Warn", unavailableWarning, { reason: "Database reader failed: reader start failed" }],
      [
        "Info",
        "SQLite reader worker retries stopped permanently; reads use the main connection",
        { reason: notWal },
      ],
    ],
  },
])(
  "keeps readers off without further retries when $when",
  ({ journalMode, workers, starts, logged }) =>
    withLogLines((lines) =>
      withFileClient((sql) =>
        Effect.gen(function* () {
          yield* sql.unsafe(`PRAGMA journal_mode = ${journalMode}`);
          yield* sql`CREATE TABLE entries(name TEXT NOT NULL)`;
          yield* sql`INSERT INTO entries VALUES ('kept')`;
          const startedBefore = startedWorkers.length;

          const failed = yield* withWorkers(workers, readConnection(sql));
          assert.deepEqual(failed, { count: 1, connection: ["writer"] });
          yield* sql`PRAGMA journal_mode = DELETE`;
          yield* TestClock.adjust("1 second");
          yield* Effect.promise(() => workerExits.get(startedWorkers.at(-1)!)!);
          yield* TestClock.adjust("1 hour");
          assert.deepEqual(yield* readConnection(sql), { count: 1, connection: ["writer"] });
          assert.equal(startedWorkers.length, startedBefore + starts);
          assert.deepEqual(yield* Queue.clear(lines), logged);
          assert.deepEqual(yield* readerPoolAvailable, [0]);
        }),
      ),
    ).pipe(Effect.provideService(Metric.MetricRegistry, new Map())),
);

it.effect("finishes masked database cleanup after its caller is interrupted", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE cleanup_writes(value INTEGER PRIMARY KEY)`;
    const entered = yield* Deferred.make<void>();
    const work = yield* Effect.forkChild(
      Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() =>
          Effect.gen(function* () {
            yield* sql`INSERT INTO cleanup_writes VALUES (1)`;
            yield* sql.withTransaction(sql`INSERT INTO cleanup_writes VALUES (2)`);
          }),
        ),
      ),
      { startImmediately: true },
    );
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(work);
    assert.deepEqual(yield* sql`SELECT value FROM cleanup_writes ORDER BY value`.values, [
      [1],
      [2],
    ]);
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.effect.each([
  { reservation: false, masked: false },
  { reservation: false, masked: true },
  { reservation: true, masked: false },
  { reservation: true, masked: true },
])(
  "preserves caller cancellation while waiting (reservation=$reservation, masked=$masked)",
  ({ reservation, masked }) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE entries(value INTEGER)`;
      const held = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holder = yield* Effect.forkChild(
        sql.withTransaction(
          Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
        ),
        { startImmediately: true },
      );
      yield* Deferred.await(held);
      const entered = yield* Deferred.make<void>();
      const body = sql`INSERT INTO entries VALUES (1)`.pipe(
        Effect.andThen(Deferred.succeed(entered, undefined)),
      );
      const acquire = reservation
        ? Effect.scoped(
            Effect.gen(function* () {
              const connection = yield* sql.reserve;
              yield* connection.executeUnprepared("INSERT INTO entries VALUES (1)", [], undefined);
              yield* Deferred.succeed(entered, undefined);
            }),
          )
        : sql.withTransaction(body);
      const waiter = yield* Effect.forkChild(masked ? Effect.uninterruptible(acquire) : acquire, {
        startImmediately: true,
      });
      const cancelled = yield* Effect.forkChild(Fiber.interrupt(waiter), {
        startImmediately: true,
      });
      yield* Effect.gen(function* () {
        yield* eventLoopTurn;
        assert.equal(cancelled.pollUnsafe() !== undefined, !masked);
        assert.isFalse(yield* Deferred.isDone(entered));
      }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
      yield* Fiber.join(holder);
      yield* Fiber.join(cancelled);
      assert.equal(yield* Deferred.isDone(entered), masked);
      yield* sql.withTransaction(sql`INSERT INTO entries VALUES (2)`);
      assert.deepEqual(
        yield* sql`SELECT value FROM entries ORDER BY value`.values,
        masked ? [[1], [2]] : [[2]],
      );
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" }))),
);

it.effect.each([
  { acquisition: "statement", masked: false },
  { acquisition: "statement", masked: true },
  { acquisition: "transaction", masked: false },
  { acquisition: "transaction", masked: true },
  { acquisition: "reservation", masked: false },
  { acquisition: "reservation", masked: true },
] as const)(
  "preserves caller cancellation while waiting for a reader ($acquisition, masked=$masked)",
  ({ acquisition, masked }) =>
    withReaderClient(
      (sql) =>
        Effect.gen(function* () {
          const release = yield* holdReader(sql, "held");
          const entered = yield* Deferred.make<void>();
          const acquire =
            acquisition === "statement"
              ? Effect.asVoid(countEntries(sql))
              : acquisition === "transaction"
                ? Effect.asVoid(sql.withTransaction(countEntries(sql)))
                : Effect.scoped(
                    Effect.flatMap(sql.reserve, (connection) =>
                      Effect.asVoid(connection.executeUnprepared("SELECT 1", [], undefined)),
                    ),
                  );
          const read = acquire.pipe(
            Effect.andThen(Deferred.succeed(entered, undefined)),
            SqliteClient.readOnly,
          );
          const waiter = yield* Effect.forkChild(masked ? Effect.uninterruptible(read) : read, {
            startImmediately: true,
          });
          const cancelled = yield* Effect.forkChild(Fiber.interrupt(waiter), {
            startImmediately: true,
          });
          yield* Effect.gen(function* () {
            yield* eventLoopTurn;
            assert.equal(cancelled.pollUnsafe() !== undefined, !masked);
            assert.isFalse(yield* Deferred.isDone(entered));
          }).pipe(Effect.ensuring(Effect.orDie(release)));
          yield* Fiber.join(cancelled);
          assert.equal(yield* Deferred.isDone(entered), masked);
          // The only slot is free again.
          assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
        }),
      { readerPoolSize: 1 },
    ),
);

// Messages a worker received, as "begin+" for a folded BEGIN, the statement's first word, or "batch(n)".
const messageShapes = (worker: NodeWorkerThreads.Worker) =>
  workerMessages.get(worker)!.map((message) => {
    const batch = Array.isArray(message.batch)
      ? (message.batch as Array<Record<string, unknown>>)
      : undefined;
    const begin = (batch?.[0] ?? message).begin === true ? "begin+" : "";
    if (batch) return `${begin}batch(${batch.length})`;
    return `${begin}${String(message.sql).trim().split(/\s+/)[0]}`;
  });

it.effect(
  "sends a read transaction's BEGIN with its first statement and nothing for an empty one",
  () =>
    withReaderClient((sql) =>
      Effect.gen(function* () {
        const before = startedWorkers.length;
        yield* sql.withTransaction(Effect.void).pipe(SqliteClient.readOnly);
        const counts = yield* sql
          .withTransaction(Effect.all([countEntries(sql), countEntries(sql)]))
          .pipe(SqliteClient.readOnly);
        assert.deepEqual(counts, [1, 1]);
        // The COMMIT leaves for the worker on the next turn, after the transaction already ended.
        yield* eventLoopTurn;
        const worker = startedWorkers[before]!;
        assert.deepEqual(messageShapes(worker), ["begin+SELECT", "SELECT", "COMMIT"]);
        // A read after the transaction starts no transaction of its own.
        assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
        yield* eventLoopTurn;
        assert.deepEqual(messageShapes(worker).slice(3), ["SELECT"]);
      }),
    ),
);

it.effect("finishes a read transaction without waiting for COMMIT and ignores its error", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const before = startedWorkers.length;
      commitHold.on = true;
      commitHold.failNext = true;
      try {
        const count = yield* sql
          .withTransaction(countEntries(sql))
          .pipe(SqliteClient.readOnly, Effect.exit);
        // Completed before the COMMIT even left for the worker; held back there, its error reply
        // is ignored.
        assert.deepEqual(count, Exit.succeed(1));
        // V2: the COMMIT leaves at once, still unanswered when the transaction has ended.
        assert.lengthOf(commitHold.held, 1);
        assert.deepEqual(messageShapes(startedWorkers[before]!), ["begin+SELECT", "COMMIT"]);
      } finally {
        commitHold.on = false;
        for (const release of commitHold.held.splice(0)) release();
      }
      // The worker is in sync again once the COMMIT reaches it.
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
    }),
  ),
);

it.effect(
  "answers a duplicate BEGIN and a COMMIT with no transaction with SQLite's error, bookkeeping intact",
  () =>
    withReaderClient((sql) =>
      Effect.gen(function* () {
        const before = startedWorkers.length;
        const duplicate = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              assert.equal(yield* countEntries(sql), 1);
              return yield* sql.unsafe("BEGIN").pipe(Effect.flip);
            }),
          )
          .pipe(SqliteClient.readOnly);
        assert.include(String(duplicate.reason.cause), "within a transaction");
        // The reader is still usable: the next read transaction starts and ends as always.
        assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
        yield* eventLoopTurn;
        assert.deepEqual(messageShapes(startedWorkers[before]!), [
          "begin+SELECT",
          "BEGIN",
          "COMMIT",
          "begin+SELECT",
          "COMMIT",
        ]);
        // A COMMIT with no transaction to end is answered, not dropped.
        const stray = yield* sql.unsafe("COMMIT").pipe(SqliteClient.readOnly, Effect.flip);
        assert.include(String(stray.reason.cause), "no transaction is active");
      }),
    ),
);

it.effect(
  "keeps a read transaction through a nested rollback and answers a missing savepoint with SQLite's error",
  () =>
    withReaderClient((sql) =>
      Effect.gen(function* () {
        const before = startedWorkers.length;
        const outer = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              assert.equal(yield* countEntries(sql), 1);
              // A failed nested transaction rolls back to its savepoint; the outer one goes on.
              const nested = yield* sql.withTransaction(Effect.fail("nested")).pipe(Effect.exit);
              assert.isTrue(Exit.isFailure(nested));
              const duplicate = yield* sql.unsafe("BEGIN").pipe(Effect.flip);
              assert.include(String(duplicate.reason.cause), "within a transaction");
              for (const form of [
                "ROLLBACK TO SAVEPOINT missing",
                "ROLLBACK TRANSACTION TO SAVEPOINT missing",
                "ROLLBACK/*to the savepoint*/TO SAVEPOINT missing",
                "-- a comment first\nROLLBACK TO missing",
              ]) {
                const missing = yield* sql.unsafe(form).pipe(Effect.flip);
                assert.include(String(missing.reason.cause), "no such savepoint", form);
              }
              return yield* countEntries(sql);
            }),
          )
          .pipe(SqliteClient.readOnly);
        assert.equal(outer, 1);
        // The reader is still usable, and only the two transactions carried a BEGIN.
        assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
        yield* eventLoopTurn;
        const shapes = messageShapes(startedWorkers[before]!);
        assert.deepEqual(
          shapes.filter((shape) => shape.startsWith("begin+")),
          ["begin+SELECT", "begin+SELECT"],
          shapes.join(" "),
        );
        assert.include(shapes, "ROLLBACK", shapes.join(" "));
      }),
    ),
);

it.effect("forwards statements that merely start with a transaction keyword to SQLite", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      // SQLite's whitespace is narrower than JavaScript's: a no-break space or a vertical tab is
      // part of the token, not a separator.
      for (const form of [
        "BEGIN2",
        "COMMIT2",
        "BEGIN 1",
        "ROLLBACK x",
        "END 2",
        "BEGIN\u00a0",
        "BEGIN\v",
      ]) {
        const error = yield* sql.unsafe(form).pipe(SqliteClient.readOnly, Effect.flip);
        assert.match(
          String(error.reason.cause),
          /syntax error|unrecognized token/,
          JSON.stringify(form),
        );
      }
      // Inside a transaction too, and the reader stays usable afterwards.
      const inside = yield* sql
        .withTransaction(sql.unsafe("BEGIN2").pipe(Effect.flip))
        .pipe(SqliteClient.readOnly);
      assert.include(String(inside.reason.cause), "syntax error");
      assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
    }),
  ),
);

it.effect("leaves a transaction control with bindings or a raw result to SQLite", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const before = startedWorkers.length;
      // A BEGIN with a binding is SQLite's error, not a folded BEGIN.
      const bound = yield* sql.unsafe("BEGIN", [1]).pipe(SqliteClient.readOnly, Effect.flip);
      assert.include(String(bound.reason.cause), "column index out of range");
      assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
      // Raw controls keep the run() result, as before.
      const raw = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const begun = yield* failureOf(sql.unsafe("BEGIN").raw);
            assert.include(String(begun.reason.cause), "within a transaction");
            assert.equal(yield* countEntries(sql), 1);
            return yield* failureOf(sql.unsafe("RELEASE SAVEPOINT missing").raw);
          }),
        )
        .pipe(SqliteClient.readOnly);
      assert.include(String(raw.reason.cause), "no such savepoint");
      const committed = yield* Effect.gen(function* () {
        yield* sql.unsafe("BEGIN").raw;
        assert.equal(yield* countEntries(sql), 1);
        return yield* sql.unsafe("COMMIT").raw;
      }).pipe(SqliteClient.readOnly);
      assert.deepEqual(committed, { changes: 0, lastInsertRowid: 0 });
      assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
      yield* eventLoopTurn;
      // The raw BEGIN inside the transaction was its first statement, so it carried the folded one.
      const shapes = messageShapes(startedWorkers[before]!);
      assert.deepEqual(
        shapes.filter((shape) => shape.startsWith("begin+")),
        ["begin+SELECT", "begin+BEGIN", "begin+SELECT"],
        shapes.join(" "),
      );
    }),
  ),
);

it.effect("keeps its transaction bookkeeping in step with controls the worker ran itself", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      // A transaction opened raw survives a managed one that cannot start inside it.
      const manual = yield* Effect.gen(function* () {
        yield* sql.unsafe("BEGIN").raw;
        assert.equal(yield* countEntries(sql), 1);
        const managed = yield* sql.withTransaction(countEntries(sql)).pipe(Effect.flip);
        assert.include(String(managed.reason.cause), "within a transaction");
        assert.equal(yield* countEntries(sql), 1);
        return yield* sql.unsafe("COMMIT").raw;
      }).pipe(SqliteClient.readOnly);
      assert.deepEqual(manual, { changes: 0, lastInsertRowid: 0 });
      // A raw COMMIT inside a managed transaction leaves its COMMIT with nothing to end.
      const mixed = yield* failureOf(
        sql
          .withTransaction(
            Effect.gen(function* () {
              assert.equal(yield* countEntries(sql), 1);
              return yield* sql.unsafe("COMMIT").raw;
            }),
          )
          .pipe(SqliteClient.readOnly),
      );
      assert.include(String(mixed.reason.cause), "no transaction is active");
      // A top-level savepoint is a transaction too: the managed one fails, the savepoint survives.
      const savepoint = yield* Effect.gen(function* () {
        yield* sql.unsafe("SAVEPOINT outer");
        const managed = yield* sql.withTransaction(countEntries(sql)).pipe(Effect.flip);
        assert.include(String(managed.reason.cause), "within a transaction");
        return yield* sql.unsafe("RELEASE SAVEPOINT outer");
      }).pipe(SqliteClient.readOnly);
      assert.deepEqual(savepoint, []);
      // And the reader is as usable as ever.
      assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
    }),
  ),
);

it.effect("ignores a reply that arrives after a newer transaction transition", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
      // A standalone read whose fiber gives up on it while the worker's reply is held back.
      const abandoned = yield* Effect.forkChild(
        sql`SELECT COUNT(*) AS count /* hold-reply */ FROM entries`.pipe(SqliteClient.readOnly),
        { startImmediately: true },
      );
      yield* Effect.promise(() => new Promise<void>((resolve) => replyHold.waiters.push(resolve)));
      yield* Fiber.interrupt(abandoned);
      const count = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            // This read carries the folded BEGIN; the stale reply lands after it.
            assert.equal(yield* countEntries(sql), 1);
            for (const release of replyHold.held.splice(0)) release();
            const duplicate = yield* sql.unsafe("BEGIN").pipe(Effect.flip);
            assert.include(String(duplicate.reason.cause), "within a transaction");
            return yield* countEntries(sql);
          }),
        )
        .pipe(SqliteClient.readOnly);
      assert.equal(count, 1);
      assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
    }),
  ),
);

it.effect(
  "forwards a BEGIN while an earlier statement on the same reader is still outstanding",
  () =>
    withReaderClient((sql) =>
      Effect.gen(function* () {
        const [savepoint, begin] = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              // Ends the managed transaction before it began, leaving the reserved reader idle.
              yield* sql.unsafe("ROLLBACK");
              return yield* Effect.all(
                [
                  sql.unsafe("SAVEPOINT outer").pipe(Effect.exit),
                  sql.unsafe("BEGIN").pipe(Effect.exit),
                ],
                { concurrency: "unbounded" },
              );
            }),
          )
          .pipe(SqliteClient.readOnly);
        assert.isTrue(Exit.isSuccess(savepoint));
        // The BEGIN reached SQLite behind the savepoint, which had opened a transaction.
        assert.isTrue(Exit.isFailure(begin), "BEGIN behind a savepoint");
        // The managed COMMIT ended the savepoint's transaction: the reader is free again.
        assert.equal(yield* sql.withTransaction(countEntries(sql)).pipe(SqliteClient.readOnly), 1);
      }),
    ),
);

it.effect("sends statements issued together as one batch and answers each on its own", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const before = startedWorkers.length;
      const results = yield* sql
        .withTransaction(
          Effect.all(
            [
              countEntries(sql),
              sql`SELECT name FROM missing_table`,
              sql<{ name: string }>`SELECT name FROM entries`,
            ],
            { concurrency: "unbounded", mode: "result" },
          ),
        )
        .pipe(SqliteClient.readOnly);
      assert.deepEqual(results[0], Result.succeed(1));
      assert.isTrue(Result.isFailure(results[1]));
      assert.deepEqual(results[2], Result.succeed([{ name: "first" }]));
      yield* eventLoopTurn;
      // V2: pipelined, one message per statement, the BEGIN with the first.
      assert.deepEqual(messageShapes(startedWorkers[before]!), [
        "begin+SELECT",
        "SELECT",
        "SELECT",
        "COMMIT",
      ]);
    }),
  ),
);

// A yield budget of three makes a fiber yield after every step (two cannot progress at all: the
// resumption and the yield check use the budget up), so whatever the transaction's fiber does after
// k of its own yields lands at every point of that statement's path, including between the request
// effect being built and its callback running.
const oneStepTurns = Effect.provideService(Scheduler.MaxOpsBeforeYield, 3);

it.effect(
  "keeps a read transaction's BEGIN on the first statement that leaves, whichever fiber yields",
  () =>
    withReaderClient((sql) =>
      Effect.gen(function* () {
        const before = startedWorkers.length;
        let seen = 0;
        for (let yields = 0; yields < 32; yields++) {
          yield* sql
            .withTransaction(
              Effect.gen(function* () {
                const first = yield* Effect.forkChild(countEntries(sql).pipe(oneStepTurns), {
                  startImmediately: true,
                });
                for (let i = 0; i < yields; i++) yield* Effect.yieldNow;
                const second = yield* Effect.forkChild(countEntries(sql), {
                  startImmediately: true,
                });
                assert.deepEqual([yield* Fiber.join(second), yield* Fiber.join(first)], [1, 1]);
              }),
            )
            .pipe(SqliteClient.readOnly);
          yield* eventLoopTurn;
          const shapes = messageShapes(startedWorkers[before]!).slice(seen);
          seen += shapes.length;
          assert.deepEqual(shapes, ["begin+SELECT", "SELECT", "COMMIT"], `after ${yields} yields`);
        }
      }),
    ),
);

it.effect("never lets an interrupted statement take a read transaction's BEGIN with it", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const before = startedWorkers.length;
      let seen = 0;
      for (let yields = 0; yields < 32; yields++) {
        const count = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const first = yield* Effect.forkChild(countEntries(sql).pipe(oneStepTurns), {
                startImmediately: true,
              });
              for (let i = 0; i < yields; i++) yield* Effect.yieldNow;
              yield* Fiber.interrupt(first);
              return yield* countEntries(sql);
            }),
          )
          .pipe(SqliteClient.readOnly);
        assert.equal(count, 1);
        yield* eventLoopTurn;
        const shapes = messageShapes(startedWorkers[before]!).slice(seen);
        seen += shapes.length;
        // The interrupted statement left before the interruption or not at all; either way the
        // BEGIN is on the first message.
        assert.deepInclude(
          [
            ["begin+SELECT", "SELECT", "COMMIT"],
            ["begin+SELECT", "COMMIT"],
          ],
          shapes,
          `after ${yields} yields: ${shapes.join(" ")}`,
        );
      }
    }),
  ),
);

it.effect("fails a statement whose worker stopped before it left instead of waiting forever", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      for (let yields = 0; yields < 64; yields++) {
        // Warm a reader, then stop it after the statement's fiber has taken k steps towards it.
        assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
        const worker = startedWorkers.at(-1)!;
        const first = yield* Effect.forkChild(
          countEntries(sql).pipe(SqliteClient.readOnly, oneStepTurns),
          { startImmediately: true },
        );
        for (let i = 0; i < yields; i++) yield* Effect.yieldNow;
        // The stop lands at this very step of the statement's path (a termination's exit would
        // arrive turns later, and a second exit would rescue a statement the first one lost).
        worker.emit("exit", 1);
        // Failed with the stopped reader or answered by a fresh one; a statement posted to nobody
        // would never settle here.
        const exit = yield* Fiber.join(first).pipe(Effect.exit);
        yield* Effect.promise(() => worker.terminate());
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause) as { reason: { cause: unknown } };
          assert.include(String(error.reason.cause), "Database reader exited");
        } else {
          assert.equal(exit.value, 1);
        }
      }
    }),
  ),
);

it.effect("answers reader statements with column names and row arrays", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const before = startedWorkers.length;
      const rows = yield* sql<{
        name: string;
        one: number;
      }>`SELECT name, 1 AS one FROM entries`.pipe(SqliteClient.readOnly);
      assert.deepEqual(rows, [{ name: "first", one: 1 }]);
      const values = yield* sql`SELECT name, 1 AS one FROM entries`.values.pipe(
        SqliteClient.readOnly,
      );
      assert.deepEqual(values, [["first", 1]]);
      const replies = workerReplies
        .get(startedWorkers[before]!)!
        .filter((reply) => "rows" in reply);
      assert.lengthOf(replies, 2);
      for (const reply of replies) {
        assert.deepEqual(reply.columns, ["name", "one"]);
        assert.deepEqual(reply.rows, [["first", 1]]);
      }
    }),
  ),
);

it.effect("fails a reader statement with an uncloneable binding and still answers the others", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const [bad, good] = yield* sql
        .withTransaction(
          Effect.all(
            [
              sql.unsafe("SELECT ? AS value", [() => 1]).pipe(Effect.exit),
              countEntries(sql).pipe(Effect.exit),
            ],
            { concurrency: "unbounded" },
          ),
        )
        .pipe(SqliteClient.readOnly);
      assert.isTrue(Exit.isFailure(bad!));
      assert.deepEqual(good, Exit.succeed(1));
      // The worker is still in step afterwards.
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
    }),
  ),
);

it.effect("names the columns of a cached reader statement after a schema change", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const before = yield* sql`SELECT * FROM entries`.pipe(SqliteClient.readOnly);
      assert.deepEqual(before, [{ name: "first" }]);
      yield* sql`ALTER TABLE entries ADD COLUMN extra TEXT`;
      const after = yield* sql`SELECT * FROM entries`.pipe(SqliteClient.readOnly);
      assert.deepEqual(after, [{ name: "first", extra: null }]);
    }),
  ),
);

it.effect("keeps a reader column named __proto__", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const rows = yield* sql`SELECT 7 AS [__proto__], 9 AS ordinary`.pipe(SqliteClient.readOnly);
      assert.deepEqual(Object.entries(rows[0]!), [
        ["__proto__", 7],
        ["ordinary", 9],
      ]);
    }),
  ),
);

it.effect("keeps a read transaction when its first statement cannot be sent", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const snapshotTaken = yield* Deferred.make<void>();
      const writer = yield* Effect.forkChild(
        Deferred.await(snapshotTaken).pipe(
          Effect.andThen(sql`INSERT INTO entries VALUES ('second')`),
        ),
      );
      const counts = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const unsent = yield* sql.unsafe("SELECT ? AS value", [() => 1]).pipe(Effect.exit);
            assert.isTrue(Exit.isFailure(unsent));
            const before = yield* countEntries(sql);
            yield* Deferred.succeed(snapshotTaken, undefined);
            yield* Fiber.join(writer);
            const after = yield* countEntries(sql);
            return { before, after };
          }),
        )
        .pipe(SqliteClient.readOnly);
      // Both reads ran inside the transaction's snapshot, which the lost BEGIN would not give.
      assert.deepEqual(counts, { before: 1, after: 1 });
      assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 2);
    }),
  ),
);

it.effect(
  "keeps a read transaction for the batch siblings of a statement that cannot be sent",
  () =>
    withReaderClient((sql) =>
      Effect.gen(function* () {
        const snapshotTaken = yield* Deferred.make<void>();
        const writer = yield* Effect.forkChild(
          Deferred.await(snapshotTaken).pipe(
            Effect.andThen(sql`INSERT INTO entries VALUES ('second')`),
          ),
        );
        const counts = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const [unsent, before] = yield* Effect.all(
                [sql.unsafe("SELECT ? AS value", [() => 1]).pipe(Effect.exit), countEntries(sql)],
                { concurrency: "unbounded" },
              );
              assert.isTrue(Exit.isFailure(unsent));
              yield* Deferred.succeed(snapshotTaken, undefined);
              yield* Fiber.join(writer);
              const after = yield* countEntries(sql);
              return { before, after };
            }),
          )
          .pipe(SqliteClient.readOnly);
        // The sibling took the transaction's snapshot, so the later read sees the same rows.
        assert.deepEqual(counts, { before: 1, after: 1 });
        assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 2);
      }),
    ),
);

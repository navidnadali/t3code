import * as NodeSqlite from "node:sqlite";
import type * as NodeWorkerThreads from "node:worker_threads";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Scheduler from "effect/Scheduler";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import * as Statement from "effect/sql/Statement";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import type * as Tracer from "effect/Tracer";
import { vi } from "vite-plus/test";

import * as SqliteClient from "./nodeSqliteClient.ts";

const startedWorkers = vi.hoisted((): Array<NodeWorkerThreads.Worker> => []);
const workerExits = vi.hoisted(() => new WeakMap<NodeWorkerThreads.Worker, Promise<void>>());
const workerParams = vi.hoisted(() => new WeakMap<NodeWorkerThreads.Worker, Array<unknown>>());
const workerMessages = vi.hoisted(
  () => new WeakMap<NodeWorkerThreads.Worker, Array<Record<string, unknown>>>(),
);
// Whether each worker was last referenced or unreferenced; Node's Worker has no hasRef().
const workerRefs = vi.hoisted(() => new WeakMap<NodeWorkerThreads.Worker, boolean>());
// Workers that behave as exited: a post to one is dropped without an error, and reported here.
const stoppedWorkers = vi.hoisted(() => new WeakMap<NodeWorkerThreads.Worker, () => void>());
const silentWorkers = vi.hoisted(() => ({ on: false, readyOnTerminate: false }));
const failingWorkers = vi.hoisted(() => ({ message: undefined as string | undefined }));
const checkpointWorkers = vi.hoisted((): Array<NodeWorkerThreads.Worker> => []);

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
      if (args[1]?.workerData?.frames !== undefined) checkpointWorkers.push(this);
    }
    override postMessage(...args: Parameters<NodeWorkerThreads.Worker["postMessage"]>) {
      const [request] = args;
      if (typeof request === "object" && request !== null) {
        workerMessages.get(this)!.push(request as Record<string, unknown>);
        if ("params" in request)
          workerParams.get(this)!.push(...(request.params as ReadonlyArray<unknown>));
      }
      const dropped = stoppedWorkers.get(this);
      if (dropped !== undefined) return dropped();
      super.postMessage(...args);
    }
    override ref() {
      workerRefs.set(this, true);
      super.ref();
    }
    override unref() {
      workerRefs.set(this, false);
      super.unref();
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
    "readerPoolSize" | "cacheSizeMb" | "readerCacheSizeMb" | "mmapSizeMb" | "writerWorker"
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

it.effect.each([false, true])("batches worker results without losing rows (reader=%s)", (reader) =>
  Effect.gen(function* () {
    const { filename } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const route = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        reader ? effect.pipe(SqliteClient.readOnly) : effect;
      yield* route(sql`SELECT 1`);
      const worker = startedWorkers
        .filter((candidate) => !checkpointWorkers.includes(candidate))
        .at(-1)!;
      const batches: Array<ReadonlyArray<unknown>> = [];
      const record = (reply: { rows?: ReadonlyArray<unknown> }) => {
        if (Array.isArray(reply.rows) && reply.rows.length > 0) batches.push(reply.rows);
      };
      worker.prependListener("message", record);
      yield* Effect.gen(function* () {
        const text = "x".repeat(16 * 1024);
        const query = sql<{ n: number; text: string }>`
          WITH RECURSIVE sequence(n) AS (
            SELECT 1 UNION ALL SELECT n + 1 FROM sequence WHERE n < 300
          ) SELECT n, ${text} AS text FROM sequence
        `;
        const rows = yield* reader ? route(query.values) : query;
        assert.equal(rows.length, 300);
        for (const [index, row] of rows.entries()) {
          assert.deepEqual(row, reader ? [index + 1, text] : { n: index + 1, text });
        }
        assert.isAbove(batches.length, 1);
        for (const batch of batches) {
          assert.isAtMost(batch.length, 16_384);
          assert.isAtMost(batch.length * text.length * 2, 8 * 1024 * 1024);
        }
        batches.length = 0;
        const narrow = sql<{ n: number }>`
          WITH RECURSIVE sequence(n) AS (
            SELECT 1 UNION ALL SELECT n + 1 FROM sequence WHERE n < 10000
          ) SELECT n FROM sequence
        `;
        const narrowRows = yield* reader ? route(narrow.values) : narrow;
        assert.equal(narrowRows.length, 10_000);
        assert.deepEqual(narrowRows.at(-1), reader ? [10_000] : { n: 10_000 });
        assert.equal(batches.length, 1);
      }).pipe(Effect.ensuring(Effect.sync(() => worker.off("message", record))));
    }).pipe(
      Effect.provide(SqliteClient.layer({ filename, writerWorker: true, readerWorker: true })),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("reuses a worker query after an iterator constraint failure", () =>
  Effect.gen(function* () {
    const { filename } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TABLE unique_entries(name TEXT UNIQUE)`;
      const insert = (name: string) =>
        sql<{ name: string }>`INSERT INTO unique_entries VALUES (${name}) RETURNING name`;
      yield* insert("first");
      const error = yield* insert("first").pipe(Effect.flip);
      assert.equal(error.reason._tag, "UniqueViolation");
      assert.deepEqual(yield* insert("second"), [{ name: "second" }]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("commits on the worker writer after a returning write timed out on a lock", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA busy_timeout = 0`;
      yield* Effect.sync(() => other.exec("BEGIN IMMEDIATE"));
      const error = yield* sql`UPDATE counters SET value = 1 WHERE id = 1 RETURNING value`.pipe(
        Effect.flip,
      );
      assert.equal(error.reason._tag, "LockTimeoutError");
      yield* Effect.sync(() => other.exec("COMMIT"));

      yield* sql.withTransaction(sql`UPDATE counters SET value = 2 WHERE id = 1`);
      assert.deepEqual(yield* sql`SELECT value FROM counters WHERE id = 1`.values, [[2]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

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
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

// A page cache size makes the writer read the schema, which needs the lock the open waits for.
it.effect.each([
  { writer: "the writer worker", writerWorker: true },
  { writer: "the main-thread writer", writerWorker: false },
])(
  "waits for another process's lock while the database opens with a page cache size and $writer",
  ({ writerWorker }) =>
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
        assert.deepEqual(yield* sql`PRAGMA cache_size`.values, [[-1024]]);
        // The wait covers the tuning only; the caller's wait is what it was before.
        assert.deepEqual(yield* sql`PRAGMA busy_timeout`.values, [[writerWorker ? 5000 : 0]]);
      }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker, cacheSizeMb: 1 })));
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

it.effect("lets the event loop run while a write waits for another connection's lock", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA busy_timeout = 5000`;
      yield* Effect.sync(() => other.exec("BEGIN IMMEDIATE"));
      const write = yield* Effect.forkChild(
        sql`UPDATE counters SET value = value + 1 WHERE id = 1`,
        { startImmediately: true },
      );
      // A writer on the event loop sleeps in SQLite's busy handler here, so this commit
      // cannot run until that write has already timed out.
      yield* eventLoopTurn;
      yield* Effect.sync(() => other.exec("UPDATE counters SET value = 10 WHERE id = 1; COMMIT"));
      yield* Fiber.join(write);
      assert.deepEqual(yield* sql`SELECT value FROM counters WHERE id = 1`.values, [[11]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("finishes a write already sent to the writer before its caller is interrupted", () =>
  Effect.gen(function* () {
    const { filename, other } = yield* makeTempDatabase;
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA busy_timeout = 5000`;
      yield* Effect.sync(() => other.exec("BEGIN IMMEDIATE"));
      const write = yield* Effect.forkChild(
        sql`UPDATE counters SET value = value + 1 WHERE id = 1`,
        { startImmediately: true },
      );
      yield* eventLoopTurn;
      const interrupted = yield* Effect.forkChild(Fiber.interrupt(write), {
        startImmediately: true,
      });
      yield* eventLoopTurn;
      // The write is waiting in the worker for the lock, so the interrupt has to wait too.
      assert.isUndefined(interrupted.pollUnsafe());
      yield* Effect.sync(() => other.exec("COMMIT"));
      yield* Fiber.join(interrupted);
      assert.deepEqual(yield* sql`SELECT value FROM counters WHERE id = 1`.values, [[1]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
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

it.effect("gives a queued read the first reader that frees up", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const holders = yield* Effect.forEach([0, 1, 2, 3], () =>
        Effect.gen(function* () {
          const held = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const fiber = yield* sql
            .withTransaction(
              countEntries(sql).pipe(
                Effect.andThen(Deferred.succeed(held, undefined)),
                Effect.andThen(Deferred.await(release)),
              ),
            )
            .pipe(SqliteClient.readOnly, Effect.forkChild);
          yield* Deferred.await(held);
          return { fiber, release };
        }),
      );
      const cancelled = yield* Effect.uninterruptibleMask((restore) =>
        SqliteClient.readOnly(restore(sql.withTransaction(countEntries(sql)))),
      ).pipe((effect) => Effect.forkChild(effect, { startImmediately: true }));
      yield* Fiber.interrupt(cancelled);
      const cancelledRead = yield* Effect.uninterruptibleMask((restore) =>
        SqliteClient.readOnly(restore(countEntries(sql))),
      ).pipe((effect) => Effect.forkChild(effect, { startImmediately: true }));
      yield* Fiber.interrupt(cancelledRead);
      const queuedSpan = yield* Deferred.make<Tracer.Span>();
      const observeQueued: Statement.Transformer = (statement, _sql, _fiber, span) =>
        Deferred.succeed(queuedSpan, span).pipe(Effect.as(statement));
      const queued = yield* countEntries(sql).pipe(
        Effect.provideService(Statement.CurrentTransformer, observeQueued),
        Effect.provideService(Statement.SpanPropagationEnabled, true),
        SqliteClient.readOnly,
        (effect) => Effect.forkChild(effect, { startImmediately: true }),
      );
      // Masked read-only work keeps its cancellation pending and still runs once a reader frees up.
      let maskedRan = 0;
      const maskedRuns = yield* Effect.forEach(
        [
          Effect.scoped(
            Effect.flatMap(sql.reserve, (connection) =>
              connection.executeUnprepared("SELECT 1", [], undefined),
            ),
          ).pipe(Effect.asVoid),
          sql.withTransaction(countEntries(sql)).pipe(Effect.asVoid),
        ],
        (effect) =>
          effect.pipe(
            Effect.andThen(Effect.sync(() => maskedRan++)),
            Effect.uninterruptible,
            SqliteClient.readOnly,
            (masked) => Effect.forkChild(masked, { startImmediately: true }),
          ),
      );
      const interrupting = yield* Effect.forEach(maskedRuns, (fiber) =>
        Effect.forkChild(Fiber.interrupt(fiber), { startImmediately: true }),
      );
      yield* TestClock.adjust("1 second");
      yield* Deferred.succeed(holders[1]!.release, undefined);
      yield* Fiber.join(holders[1]!.fiber);
      // Pinned to the first reader it saw, this read would wait for that reader's transaction.
      assert.equal(yield* Fiber.join(queued), 1);
      assert.equal((yield* Deferred.await(queuedSpan)).attributes.get("db.lock_wait_ms"), 1000);
      for (const { fiber, release } of [holders[0]!, holders[2]!, holders[3]!]) {
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(fiber);
      }
      yield* Effect.forEach(interrupting, Fiber.join);
      assert.equal(maskedRan, 2);
    }),
  ),
);

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
        yield* page.release;
        // Each history read takes a reader as one frees.
        yield* histories[0]!.running;
        yield* holders[1]!.release;
        for (const history of histories) {
          yield* history.running;
          yield* history.release;
        }
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

const configuredTuning = {
  config: { cacheSizeMb: 64, readerCacheSizeMb: 8, mmapSizeMb: 16 },
  writer: { cacheSize: -64 * 1024, mmapSize: 16 * 1024 * 1024 },
  reader: { cacheSize: -8 * 1024, mmapSize: 16 * 1024 * 1024 },
};
const defaultTuning = {
  config: {},
  writer: { cacheSize: -2000, mmapSize: 0 },
  reader: { cacheSize: -2000, mmapSize: 0 },
};

it.effect.each([
  { tuning: "configured", writerWorker: true, ...configuredTuning },
  { tuning: "configured", writerWorker: false, ...configuredTuning },
  { tuning: "default", writerWorker: true, ...defaultTuning },
  { tuning: "default", writerWorker: false, ...defaultTuning },
])(
  "gives the writer and every reader, restarted ones too, the $tuning page cache and memory map (writer worker: $writerWorker)",
  ({ config, writerWorker, writer, reader }) =>
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
      { ...config, writerWorker },
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

const unavailableWarning = "SQLite reader worker unavailable; reads use the writer";

const notWal = "database uses journal_mode delete, not wal";

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
    reason: "Database reader did not open the database within 10000 ms",
  },
])(
  "reads on the writer after $failure until a retry starts the reader again",
  ({ workers, startup, reason }) =>
    withLogLines((lines) =>
      withReaderClient((sql) =>
        Effect.gen(function* () {
          const failed = yield* withWorkers(
            workers,
            Effect.gen(function* () {
              const read = yield* Effect.forkChild(readConnection(sql), {
                startImmediately: true,
              });
              yield* TestClock.adjust(startup);
              return yield* Fiber.join(read);
            }),
          );
          assert.deepEqual(failed, { count: 1, connection: ["writer"] });
          const startedBefore = startedWorkers.length;
          assert.deepEqual(yield* readConnection(sql), { count: 1, connection: ["writer"] });
          assert.equal(startedWorkers.length, startedBefore);
          assert.deepEqual(yield* Queue.take(lines), ["Warn", unavailableWarning, { reason }]);

          yield* TestClock.adjust("1 second");
          assert.equal(startedWorkers.length, startedBefore + 1);
          assert.deepEqual(yield* Queue.take(lines), [
            "Info",
            "SQLite reader worker available again; reads use the reader workers",
          ]);
          assert.deepEqual(yield* readConnection(sql), { count: 1, connection: ["reader"] });
          assert.equal(Queue.sizeUnsafe(lines), 0);
        }),
      ),
    ),
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
  { phase: "the retry waits", beforeClose: Duration.zero, retries: 0 },
  { phase: "a retried reader starts", beforeClose: Duration.seconds(1), retries: 1 },
])("stops retrying the reader when the client closes while $phase", ({ beforeClose, retries }) =>
  withWorkers(
    { silent: true },
    Effect.gen(function* () {
      const startedAtClose = yield* withReaderClient((sql) =>
        Effect.gen(function* () {
          const read = yield* Effect.forkChild(readConnection(sql), { startImmediately: true });
          yield* TestClock.adjust("10 seconds");
          assert.deepEqual(yield* Fiber.join(read), { count: 1, connection: ["writer"] });

          const startedBefore = startedWorkers.length;
          yield* TestClock.adjust(beforeClose);
          assert.equal(startedWorkers.length - startedBefore, retries);
          return startedWorkers.length;
        }),
      );
      yield* TestClock.adjust("10 minutes");
      assert.equal(startedWorkers.length, startedAtClose);
    }),
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
        "SQLite reader worker retries stopped permanently; reads use the writer",
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
        }),
      ),
    ),
);

it.effect("fails with a typed SqlError when the database file is not SQLite", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-notadb-" });
    const filename = path.join(directory, "state.sqlite");
    yield* fs.writeFileString(filename, "not a db\n");

    const exit = yield* Layer.build(SqliteClient.layer({ filename, writerWorker: true })).pipe(
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) assert.isTrue(SqlError.isSqlError(Cause.squash(exit.cause)));
    if (Exit.isFailure(exit)) assert.isFalse(Cause.hasDies(exit.cause));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
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

// oxlint-disable-next-line t3code/no-real-time-wait-in-tests -- a zero-delay macrotask boundary that lets the worker's queued messages land, not a wait for time
const eventLoopTurn = Effect.promise(() => new Promise<void>((resolve) => setImmediate(resolve)));

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

it.effect("keeps writes moving while an outside reader pins the WAL, then trims the WAL", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-pinned-wal-" });
    const filename = path.join(directory, "state.sqlite");
    // Stands in for `sqlite3 statev2.sqlite` or a backup script reading the database.
    const outside = yield* Effect.acquireRelease(
      Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
      (db) =>
        Effect.sync(() => {
          if (db.isTransaction) db.exec("COMMIT");
          db.close();
        }),
    );
    outside.exec("PRAGMA journal_mode = WAL; CREATE TABLE blobs(data BLOB NOT NULL)");
    const walSize = fs.stat(`${filename}-wal`).pipe(
      Effect.map((info) => Number(info.size)),
      Effect.orDie,
    );
    const limit = 8 * 1024 * 1024;
    const payload = new Uint8Array(1024 * 1024).fill(7);
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql.unsafe(`PRAGMA journal_size_limit = ${limit}`);
      yield* sql`INSERT INTO blobs VALUES (${payload})`;
      outside.exec("BEGIN");
      outside.prepare("SELECT COUNT(*) FROM blobs").get();
      for (let write = 0; write < 24; write++) yield* sql`INSERT INTO blobs VALUES (${payload})`;
      assert.isAbove(yield* walSize, limit);
      outside.exec("COMMIT");
      const checkpointer = checkpointWorkers.at(-1)!;
      const caughtUp = Effect.callback<void>((resume) => {
        const onMessage = (message: { type?: unknown; log?: unknown; checkpointed?: unknown }) => {
          if (
            message?.type === "checked" &&
            typeof message.log === "number" &&
            message.log > 0 &&
            message.log === message.checkpointed
          ) {
            resume(Effect.void);
          }
        };
        checkpointer.on("message", onMessage);
        checkpointer.ref();
        return Effect.sync(() => {
          checkpointer.off("message", onMessage);
          checkpointer.unref();
        });
      });
      // The worker checkpoints the released frames; the next write restarts the WAL at the limit.
      yield* Effect.all([caughtUp, sql`DELETE FROM blobs WHERE rowid = 1`], { concurrency: 2 });
      yield* sql`DELETE FROM blobs WHERE rowid = 2`;
      assert.isAtMost(yield* walSize, limit);
      assert.deepEqual(yield* sql`SELECT COUNT(*) FROM blobs`.values, [[23]]);
    }).pipe(Effect.provide(SqliteClient.layer({ filename, writerWorker: true })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

// The first word of each statement a worker received, in order.
const statementsSent = (worker: NodeWorkerThreads.Worker) =>
  workerMessages.get(worker)!.map((message) => String(message.sql).trim().split(/\s+/)[0]);

it.effect("posts statements issued together at once, in order, and answers each on its own", () =>
  withReaderClient((sql) =>
    Effect.gen(function* () {
      const before = startedWorkers.length;
      const [concurrency, results] = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const concurrency = yield* SqliteClient.readConcurrency;
            const results = yield* Effect.all(
              [
                countEntries(sql),
                sql`SELECT name FROM missing_table`,
                sql<{ name: string }>`SELECT name FROM entries`,
              ],
              { concurrency, mode: "result" },
            );
            return [concurrency, results] as const;
          }),
        )
        .pipe(SqliteClient.readOnly);
      assert.equal(concurrency, "unbounded");
      assert.deepEqual(results[0], Result.succeed(1));
      assert.isTrue(Result.isFailure(results[1]));
      assert.deepEqual(results[2], Result.succeed([{ name: "first" }]));
      // One message per statement, in order.
      assert.deepEqual(statementsSent(startedWorkers[before]!), [
        "BEGIN",
        "SELECT",
        "SELECT",
        "SELECT",
        "COMMIT",
      ]);
      // On the writer the statements run in turn.
      assert.isUndefined(yield* sql.withTransaction(SqliteClient.readConcurrency));
    }),
  ),
);

// Yields at every step and calls `stop` as the n-th yield is scheduled, so a sweep of n lands the
// stop between every two steps of a statement's path.
const stoppingAtYield = (n: number, stop: () => void): Scheduler.Scheduler => {
  const scheduler = new Scheduler.MixedScheduler();
  let yields = 0;
  return {
    executionMode: scheduler.executionMode,
    shouldYield: () => true,
    makeDispatcher: () => {
      const dispatcher = scheduler.makeDispatcher();
      return {
        scheduleTask: (task, priority) => {
          if (++yields === n) stop();
          dispatcher.scheduleTask(task, priority);
        },
        flush: () => dispatcher.flush(),
      };
    },
  };
};

it.effect("fails a statement whose worker stopped before it left instead of waiting forever", () =>
  withLogLines((lines) =>
    withReaderClient((sql) =>
      Effect.gen(function* () {
        let posted = false;
        for (let n = 1; n <= 2000; n++) {
          assert.equal(yield* countEntries(sql).pipe(SqliteClient.readOnly), 1);
          const worker = startedWorkers.at(-1)!;
          const sent = workerMessages.get(worker)!.length;
          const dropped = yield* Deferred.make<void>();
          let stopped = false;
          const stop = () => {
            stopped = true;
            posted = workerMessages.get(worker)!.length > sent;
            // From now on the worker drops what is posted to it, as an exited one does.
            stoppedWorkers.set(worker, () => Deferred.doneUnsafe(dropped, Exit.void));
            worker.emit("exit", 1);
          };
          const first = yield* Effect.forkChild(
            countEntries(sql).pipe(
              SqliteClient.readOnly,
              Effect.provideService(Scheduler.Scheduler, stoppingAtYield(n, stop)),
            ),
            { startImmediately: true },
          );
          const outcome = yield* Effect.raceFirst(
            Effect.exit(Fiber.join(first)),
            Effect.as(Deferred.await(dropped), "dropped" as const),
          );
          if (outcome === "dropped") {
            assert.fail(`a statement was posted to a stopped reader at yield ${n}`);
          }
          // Failed with the stopped reader, or answered by a fresh reader or the writer.
          if (Exit.isFailure(outcome)) {
            const error = Cause.squash(outcome.cause);
            assert(SqlError.isSqlError(error));
            assert.include(String(error.reason.cause), "Database reader exited");
          } else {
            assert.equal(outcome.value, 1);
          }
          assert.isTrue(stopped, `the statement finished before its fiber's yield ${n}`);
          yield* Effect.promise(() => worker.terminate());
          // A stop while the reader was starting turns the pool off until a retry starts it again.
          if ((yield* Queue.clear(lines)).length > 0) {
            yield* TestClock.adjust("1 second");
            assert.equal((yield* Queue.take(lines))[0], "Info");
          }
          if (posted) break;
        }
        assert.isTrue(posted);
      }),
    ),
  ),
);

it.effect.each([
  { role: "reader", writerWorker: false },
  { role: "writer", writerWorker: true },
] as const)(
  "fails a $role statement with an uncloneable binding on its own and lets go of the worker",
  ({ role, writerWorker }) =>
    withReaderClient(
      (sql) =>
        Effect.gen(function* () {
          const on = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
            role === "reader" ? SqliteClient.readOnly(effect) : effect;
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
            .pipe(on);
          assert(Exit.isFailure(bad));
          assert.isFalse(Cause.hasDies(bad.cause));
          assert.isTrue(SqlError.isSqlError(Cause.squash(bad.cause)));
          assert.deepEqual(good, Exit.succeed(1));
          // The worker is still in step afterwards.
          assert.equal(yield* countEntries(sql).pipe(on), 1);
          const worker = startedWorkers.findLast((candidate) =>
            workerMessages.get(candidate)?.some((message) => message.sql === "SELECT ? AS value"),
          )!;
          assert.isFalse(workerRefs.get(worker));
        }),
      { writerWorker },
    ),
);

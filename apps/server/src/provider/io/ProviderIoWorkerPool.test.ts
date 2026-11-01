import type * as NodeWorkerThreads from "node:worker_threads";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Scheduler from "effect/Scheduler";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { AcpTransportError } from "effect-acp/errors";
import { CodexAppServerTransportError } from "effect-codex-app-server/errors";
import { vi } from "vite-plus/test";

import * as ProviderIoWorkerPool from "./ProviderIoWorkerPool.ts";
import { echoStage, linesStage } from "./testFixtures/providerIoTestStages.ts";

type WorkerMode = "fixture" | "production" | "silent" | "throws" | "inert";

const workers = vi.hoisted(() => ({
  mode: "fixture" as WorkerMode,
  started: new Array<NodeWorkerThreads.Worker>(),
  exits: new WeakMap<NodeWorkerThreads.Worker, Promise<number>>(),
  opened: new Array<number>(),
}));

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      const replacements: Record<WorkerMode, ConstructorParameters<typeof actual.Worker>> = {
        fixture: [new URL("./testFixtures/providerIoTestWorker.ts", import.meta.url)],
        production: args,
        silent: ["setInterval(() => {}, 1 << 30);", { eval: true }],
        throws: ['throw new Error("worker start failed with SECRET-TOKEN-42");', { eval: true }],
        // Ready, then never answers: the test injects the worker's replies itself.
        inert: [
          'require("node:worker_threads").parentPort.postMessage({ type: "ready" }); setInterval(() => {}, 1 << 30);',
          { eval: true },
        ],
      };
      super(...replacements[workers.mode]);
      workers.started.push(this);
      workers.exits.set(this, new Promise((resolve) => this.once("exit", resolve)));
    }
    override postMessage(...args: Parameters<NodeWorkerThreads.Worker["postMessage"]>) {
      const [message] = args as [{ readonly type?: unknown; readonly session?: unknown }];
      if (message.type === "open" && typeof message.session === "number") {
        workers.opened.push(message.session);
      }
      super.postMessage(...args);
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

const withPool = <A, E, R>(
  options: { readonly workers: number; readonly mode?: WorkerMode },
  use: (
    pool: ProviderIoWorkerPool.ProviderIoWorkerPool["Service"],
    started: () => number,
  ) => Effect.Effect<A, E, R | Scope.Scope>,
) =>
  Effect.gen(function* () {
    workers.mode = options.mode ?? "fixture";
    const startedBefore = workers.started.length;
    const pool = yield* ProviderIoWorkerPool.make({ workers: options.workers });
    return yield* use(pool, () => workers.started.length - startedBefore);
  }).pipe(Effect.scoped);

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

const bytes = (text: string) => new TextEncoder().encode(text);

const toCodexError = (cause: unknown) =>
  new CodexAppServerTransportError({ operation: "read-input-stream", cause });

const openEcho = (
  pool: ProviderIoWorkerPool.ProviderIoWorkerPool["Service"],
  tag: string,
  options: { readonly highWaterMark?: number; readonly kill?: Effect.Effect<void> } = {},
) =>
  pool
    .open({
      stage: echoStage,
      options: { tag },
      onFailure: toCodexError,
      kill: options.kill ?? Effect.void,
      ...(options.highWaterMark === undefined ? {} : { highWaterMark: options.highWaterMark }),
    })
    .pipe(Effect.map(Option.getOrThrow));

const openEchoOption = (pool: ProviderIoWorkerPool.ProviderIoWorkerPool["Service"]) =>
  pool.open({
    stage: echoStage,
    options: { tag: "a" },
    onFailure: toCodexError,
    kill: Effect.void,
  });

const spawnIdleChild = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  return yield* spawner.spawn(
    ChildProcess.make(process.execPath, ["-e", "setInterval(() => {}, 1 << 30)"], {
      stdout: "ignore",
      stderr: "ignore",
    }),
  );
});

const unavailableWarning =
  "Provider I/O workers unavailable; provider output is handled on the main thread";

it.effect("starts no worker and leaves every stage in-thread when the pool size is 0", () =>
  withPool({ workers: 0 }, (pool, started) =>
    Effect.gen(function* () {
      assert.isTrue(Option.isNone(yield* openEchoOption(pool)));
      assert.equal(started(), 0);
    }),
  ),
);

it.effect("starts its workers with the pool and stops them when the pool closes", () =>
  Effect.gen(function* () {
    const poolWorkers = yield* withPool({ workers: 2 }, (_pool, started) =>
      Effect.sync(() => workers.started.slice(-started())),
    );
    assert.equal(poolWorkers.length, 2);
    // Closing the pool awaits each worker's termination.
    for (const worker of poolWorkers) {
      yield* Effect.promise(() => workers.exits.get(worker)!);
    }
  }),
);

it.effect("completes the ready handshake with the production worker", () =>
  withPool({ workers: 1, mode: "production" }, (pool) =>
    Effect.gen(function* () {
      const session = yield* openEcho(pool, "a");
      // The production worker has no test stages, so it rejects the session after the handshake.
      const error = yield* Effect.flip(Stream.runCollect(session.output));
      assert.instanceOf(error, CodexAppServerTransportError);
      assert.equal((error.cause as ProviderIoWorkerPool.ProviderIoStageError).stage, "test-echo");
    }),
  ),
);

it.effect("delivers each session's outputs in order while sessions run concurrently", () =>
  withPool({ workers: 2 }, (pool, started) =>
    Effect.gen(function* () {
      const lineCount = 40;
      const results = yield* Effect.forEach(
        ["s0", "s1", "s2", "s3"],
        (name) =>
          Effect.gen(function* () {
            const session = Option.getOrThrow(
              yield* pool.open({
                stage: linesStage,
                options: undefined,
                onFailure: toCodexError,
                kill: Effect.void,
                highWaterMark: 16,
              }),
            );
            const produce = Effect.forEach(
              Array.from({ length: lineCount }, (_, index) => `${name}:${index}\n`),
              // Split every line across two chunks so lines also cross chunk boundaries.
              (line) =>
                session
                  .write(bytes(line.slice(0, 3)))
                  .pipe(Effect.andThen(session.write(bytes(line.slice(3))))),
              { discard: true },
            ).pipe(Effect.andThen(session.end));
            const [, lines] = yield* Effect.all([produce, Stream.runCollect(session.output)], {
              concurrency: 2,
            });
            return { name, lines };
          }),
        { concurrency: "unbounded" },
      );
      for (const { name, lines } of results) {
        assert.deepEqual(
          lines,
          Array.from({ length: lineCount }, (_, index) => `${name}:${index}`),
        );
      }
      assert.equal(started(), 2);
    }),
  ),
);

it.effect(
  "pauses a producer at the session's credit window and resumes it as outputs are taken",
  () =>
    withPool({ workers: 1 }, (pool) =>
      Effect.gen(function* () {
        const session = yield* openEcho(pool, "t", { highWaterMark: 8 });
        let admitted = 0;
        const thirdAdmitted = yield* Deferred.make<void>();
        const producer = yield* Effect.forkChild(
          Effect.gen(function* () {
            for (const text of ["aaaa", "bbbb", "cccc", "dddd"]) {
              yield* session.write(bytes(text));
              admitted += 1;
              if (admitted === 3) yield* Deferred.succeed(thirdAdmitted, undefined);
            }
            yield* session.end;
          }),
          { startImmediately: true },
        );
        // Two 4-byte chunks fill the 8-byte window; nothing was consumed, so the third waits.
        assert.equal(admitted, 2);
        assert.isUndefined(producer.pollUnsafe());

        const pull = yield* Stream.toPull(session.output);
        const first = yield* pull;
        assert.equal(first[0], "t:aaaa");
        yield* Deferred.await(thirdAdmitted);
        assert.isAtLeast(admitted, 3);

        const rest = yield* Stream.runCollect(Stream.fromPull(Effect.succeed(pull)));
        yield* Fiber.join(producer);
        assert.equal(admitted, 4);
        assert.deepEqual([...first, ...rest], ["t:aaaa", "t:bbbb", "t:cccc", "t:dddd", "t:end"]);
      }),
    ),
);

it.effect(
  "fails a dead worker's sessions with their transport errors, kills their children, and restarts on the next open",
  () =>
    withLogLines((lines) =>
      withPool({ workers: 1 }, (pool, started) =>
        Effect.gen(function* () {
          const codexChild = yield* spawnIdleChild;
          const acpChild = yield* spawnIdleChild;
          const codex = yield* openEcho(pool, "codex", {
            kill: Effect.ignore(codexChild.kill()),
          });
          const acp = Option.getOrThrow(
            yield* pool.open({
              stage: echoStage,
              options: { tag: "acp" },
              onFailure: (cause) =>
                new AcpTransportError({ operation: "read-input-stream", cause }),
              kill: Effect.ignore(acpChild.kill()),
            }),
          );

          yield* codex.write(bytes("exit"));
          const codexError = yield* Effect.flip(Stream.runDrain(codex.output));
          const acpError = yield* Effect.flip(Stream.runDrain(acp.output));
          assert.instanceOf(codexError, CodexAppServerTransportError);
          assert.instanceOf(codexError.cause, ProviderIoWorkerPool.ProviderIoWorkerError);
          assert.instanceOf(acpError, AcpTransportError);
          assert.instanceOf(acpError.cause, ProviderIoWorkerPool.ProviderIoWorkerError);
          // The idle children never exit on their own.
          for (const child of [codexChild, acpChild]) {
            yield* Effect.exit(child.exitCode);
            assert.isFalse(yield* child.isRunning);
          }
          assert.instanceOf(yield* Effect.flip(acp.write(bytes("late"))), AcpTransportError);
          assert.instanceOf(yield* Effect.flip(acp.call("late")), AcpTransportError);
          assert.deepEqual(yield* Queue.take(lines), [
            "Warn",
            "Provider I/O worker stopped",
            { worker: 0, sessions: 2, reason: "exit", exitCode: 3 },
          ]);
          assert.equal(started(), 1);

          const replacement = yield* openEcho(pool, "next");
          assert.equal(started(), 2);
          yield* replacement.write(bytes("hello"));
          yield* replacement.end;
          assert.deepEqual(yield* Stream.runCollect(replacement.output), [
            "next:hello",
            "next:end",
          ]);
        }),
      ),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("runs a session's calls on its worker after the chunks sent before them", () =>
  withPool({ workers: 1 }, (pool) =>
    Effect.gen(function* () {
      const session = yield* openEcho(pool, "t");
      yield* session.write(bytes("a"));
      yield* session.write(bytes("b"));
      assert.equal(yield* session.call("count"), "t:count:2");
      const failed = yield* Effect.flip(session.call("throw"));
      assert.instanceOf(failed, CodexAppServerTransportError);
      assert.instanceOf(failed.cause, ProviderIoWorkerPool.ProviderIoStageError);

      yield* session.write(bytes("c"));
      yield* session.end;
      assert.deepEqual(yield* Stream.runCollect(session.output), ["t:a", "t:b", "t:c", "t:end"]);
      // The worker has dropped the session after its end; a late call fails instead of waiting.
      assert.instanceOf(yield* Effect.flip(session.call("late")), CodexAppServerTransportError);
    }),
  ),
);

it.effect("fails only the session whose stage throws, after the outputs it emitted first", () =>
  withPool({ workers: 1 }, (pool) =>
    Effect.gen(function* () {
      let kills = 0;
      const failing = yield* openEcho(pool, "bad", {
        kill: Effect.sync(() => {
          kills += 1;
        }),
      });
      const healthy = yield* openEcho(pool, "good");
      yield* failing.write(bytes("one"));
      yield* failing.write(bytes("throw"));
      const outputs: Array<string> = [];
      const error = yield* Stream.runForEach(failing.output, (output) =>
        Effect.sync(() => outputs.push(output)),
      ).pipe(Effect.flip);
      assert.deepEqual(outputs, ["bad:one", "bad:before-throw"]);
      assert.instanceOf(error, CodexAppServerTransportError);
      assert.instanceOf(error.cause, ProviderIoWorkerPool.ProviderIoStageError);
      assert.equal((error.cause as ProviderIoWorkerPool.ProviderIoStageError).stage, "test-echo");
      assert.equal(kills, 0);

      yield* healthy.write(bytes("still"));
      yield* healthy.end;
      assert.deepEqual(yield* Stream.runCollect(healthy.output), ["good:still", "good:end"]);
    }),
  ),
);

it.effect.each([
  {
    failure: "never becomes ready",
    mode: "silent" as const,
    startup: Duration.seconds(10),
    annotations: { reason: "not-ready" },
  },
  {
    failure: "fails while starting",
    mode: "throws" as const,
    startup: Duration.zero,
    annotations: { reason: "error", errorTag: "Error" },
  },
])("falls back to in-thread for good when the worker $failure", ({ mode, startup, annotations }) =>
  withLogLines((lines) =>
    withPool({ workers: 1, mode }, (pool, started) =>
      Effect.gen(function* () {
        const opened = yield* Effect.forkChild(Effect.scoped(openEchoOption(pool)), {
          startImmediately: true,
        });
        yield* TestClock.adjust(startup);
        assert.isTrue(Option.isNone(yield* Fiber.join(opened)));
        const warning = yield* Queue.take(lines);
        assert.deepEqual(warning, ["Warn", unavailableWarning, annotations]);
        // Log annotations never carry a worker's error text, which may hold provider data.
        assert.notInclude(JSON.stringify(warning), "SECRET");

        assert.isTrue(Option.isNone(yield* openEchoOption(pool)));
        assert.equal(started(), 1);
        assert.equal(Queue.sizeUnsafe(lines), 0);
      }),
    ),
  ),
);

it.effect("rejects and logs worker messages of an unknown or malformed shape and keeps going", () =>
  withLogLines((lines) =>
    withPool({ workers: 1 }, (pool) =>
      Effect.gen(function* () {
        const session = yield* openEcho(pool, "t");
        yield* session.write(bytes("unknown"));
        yield* session.write(bytes("malformed"));
        yield* session.end;
        assert.deepEqual(yield* Stream.runCollect(session.output), [
          "t:unknown",
          "t:malformed",
          "t:end",
        ]);
        assert.deepEqual(yield* Queue.take(lines), [
          "Warn",
          "Provider I/O worker sent an unknown message",
          { type: "bogus" },
        ]);
        assert.deepEqual(yield* Queue.take(lines), [
          "Warn",
          "Provider I/O worker sent an unknown message",
          { type: "output" },
        ]);
      }),
    ),
  ),
);

// Parks the forked fiber after `ops` operations, so the caller acts at that exact point; a budget
// under 3 never lets a resumed fiber make progress.
const forkParkedAfter = <A, E, R>(ops: number, effect: Effect.Effect<A, E, R>) =>
  Effect.forkChild(Effect.provideService(effect, Scheduler.MaxOpsBeforeYield, ops), {
    startImmediately: true,
  });

const OP_BOUNDARIES = 40;

it.effect("admits a waiting write whenever its credit returns during admission", () =>
  withPool({ workers: 1, mode: "inert" }, (pool) =>
    Effect.gen(function* () {
      const worker = workers.started.at(-1)!;
      for (let ops = 3; ops <= OP_BOUNDARIES; ops++) {
        yield* Effect.gen(function* () {
          const session = yield* openEcho(pool, "t", { highWaterMark: 4 });
          yield* session.write(bytes("full"));
          const write = yield* forkParkedAfter(ops, session.write(bytes("next")));
          worker.emit("message", {
            type: "output",
            session: workers.opened.at(-1),
            outputs: [],
            bytes: 4,
          });
          yield* Fiber.join(write);
        }).pipe(Effect.scoped);
      }
    }),
  ),
);

it.effect("starts one replacement for a stopped worker however many sessions open at once", () =>
  withPool({ workers: 1 }, (pool, started) =>
    Effect.gen(function* () {
      // Stopping a worker before its handshake would turn the pool off instead.
      yield* openEcho(pool, "ready").pipe(Effect.scoped);
      for (let ops = 3; ops <= OP_BOUNDARIES; ops++) {
        const current = workers.started.at(-1)!;
        yield* Effect.promise(() => current.terminate());
        yield* Effect.promise(() => workers.exits.get(current)!);
        const startedBefore = started();
        yield* Effect.gen(function* () {
          const first = yield* forkParkedAfter(ops, openEchoOption(pool));
          const second = yield* Effect.forkChild(openEchoOption(pool), { startImmediately: true });
          assert.isTrue(Option.isSome(yield* Fiber.join(first)));
          assert.isTrue(Option.isSome(yield* Fiber.join(second)));
        }).pipe(Effect.scoped);
        assert.equal(started() - startedBefore, 1);
      }
    }),
  ),
);

it.effect("never hands out a session on a worker that stopped while it was opening", () =>
  withPool({ workers: 1 }, (pool) =>
    Effect.gen(function* () {
      yield* openEcho(pool, "ready").pipe(Effect.scoped);
      for (let ops = 3; ops <= OP_BOUNDARIES; ops++) {
        const current = workers.started.at(-1)!;
        yield* Effect.gen(function* () {
          const opening = yield* forkParkedAfter(ops, openEchoOption(pool));
          current.emit("exit", 1);
          const session = yield* Fiber.join(opening);
          if (Option.isNone(session)) return;
          // A session that registered in time fails with the worker; a later one is on a replacement.
          yield* Effect.exit(session.value.end);
          yield* Effect.exit(Stream.runDrain(session.value.output));
        }).pipe(Effect.scoped);
        yield* Effect.promise(() => current.terminate());
      }
      assert.isTrue(Option.isSome(yield* openEchoOption(pool)));
    }),
  ),
);

it.effect("returns a silent chunk's credit at once, even when it shares a batch with outputs", () =>
  withPool({ workers: 1 }, (pool) =>
    Effect.gen(function* () {
      const session = Option.getOrThrow(
        yield* pool.open({
          stage: linesStage,
          options: undefined,
          onFailure: toCodexError,
          kill: Effect.void,
          highWaterMark: 8,
        }),
      );
      yield* session.write(bytes("ab\n"));
      yield* session.write(bytes("cdefg"));
      // The window is full and no output was taken; only the silent chunk's credit can admit this.
      yield* session.write(bytes("h\n"));
      yield* session.end;
      assert.deepEqual(yield* Stream.runCollect(session.output), ["ab", "cdefgh"]);
    }),
  ),
);

it.effect("copies a Buffer view for the worker and leaves the caller's buffer intact", () =>
  withPool({ workers: 1 }, (pool) =>
    Effect.gen(function* () {
      const session = Option.getOrThrow(
        yield* pool.open({
          stage: linesStage,
          options: undefined,
          onFailure: toCodexError,
          kill: Effect.void,
          highWaterMark: 4,
        }),
      );
      const pooled = Buffer.from("e\n");
      const backing = Buffer.alloc(64);
      backing.write("f\n", 8);
      const view = backing.subarray(8, 10);

      yield* session.write(bytes("abcd"));
      // The window is full, so the worker's credit for the silent chunk admits this write.
      yield* session.write(pooled);
      yield* session.write(view);
      yield* session.end;
      assert.deepEqual(yield* Stream.runCollect(session.output), ["abcde", "f"]);
      assert.equal(pooled.toString(), "e\n");
      assert.equal(backing.byteLength, 64);
      assert.equal(view.toString(), "f\n");
    }),
  ),
);

it.effect("rejects a credit that is not a byte count without touching the window", () =>
  withLogLines((lines) =>
    withPool({ workers: 1, mode: "inert" }, (pool) =>
      Effect.gen(function* () {
        const session = yield* openEcho(pool, "t", { highWaterMark: 4 });
        workers.started.at(-1)!.emit("message", {
          type: "output",
          session: workers.opened.at(-1),
          outputs: [],
          bytes: Number.NaN,
        });
        assert.deepEqual(yield* Queue.take(lines), [
          "Warn",
          "Provider I/O worker sent an unknown message",
          { type: "output" },
        ]);
        yield* session.write(bytes("ok"));
      }),
    ),
  ),
);

it.effect("admits an end queued behind a write whose fiber was interrupted", () =>
  withPool({ workers: 1, mode: "inert" }, (pool) =>
    Effect.gen(function* () {
      const session = yield* openEcho(pool, "t", { highWaterMark: 4 });
      yield* session.write(bytes("full"));
      const write = yield* Effect.forkChild(session.write(bytes("next")), {
        startImmediately: true,
      });
      const end = yield* Effect.forkChild(session.end, { startImmediately: true });
      yield* Fiber.interrupt(write);
      yield* Fiber.join(end);
    }),
  ),
);

it.effect("runs a call only after the writes waiting for credit before it", () =>
  withPool({ workers: 1 }, (pool) =>
    Effect.gen(function* () {
      const session = yield* openEcho(pool, "t", { highWaterMark: 4 });
      yield* session.write(bytes("full"));
      const write = yield* Effect.forkChild(session.write(bytes("next")), {
        startImmediately: true,
      });
      const call = yield* Effect.forkChild(session.call("count"), { startImmediately: true });
      const pull = yield* Stream.toPull(session.output);
      assert.equal((yield* pull)[0], "t:full");
      yield* Fiber.join(write);
      // The worker saw both chunks before the call.
      assert.equal(yield* Fiber.join(call), "t:count:2");
    }),
  ),
);

it.effect("fails an open or a call whose data cannot be cloned, leaving nothing behind", () =>
  withPool({ workers: 1 }, (pool) =>
    Effect.gen(function* () {
      let kills = 0;
      const notCloneable = Object.assign({ tag: "bad" }, { callback: () => undefined });
      const openError = yield* Effect.flip(
        pool.open({
          stage: echoStage,
          options: notCloneable,
          onFailure: toCodexError,
          kill: Effect.sync(() => {
            kills += 1;
          }),
        }),
      );
      assert.instanceOf(openError, CodexAppServerTransportError);
      assert.instanceOf(openError.cause, ProviderIoWorkerPool.ProviderIoStageError);

      const session = yield* openEcho(pool, "t");
      const callError = yield* Effect.flip(session.call((() => "x") as unknown as string));
      assert.instanceOf(callError, CodexAppServerTransportError);
      assert.equal(yield* session.call("count"), "t:count:0");

      // A crash fails the live session; the failed open left no session whose child it kills.
      yield* session.write(bytes("exit"));
      yield* Effect.flip(Stream.runDrain(session.output));
      assert.equal(kills, 0);
    }),
  ),
);

it.effect("caps the credit a worker returns at what was sent", () =>
  withPool({ workers: 1, mode: "inert" }, (pool) =>
    Effect.gen(function* () {
      const session = yield* openEcho(pool, "t", { highWaterMark: 4 });
      workers.started.at(-1)!.emit("message", {
        type: "output",
        session: workers.opened.at(-1),
        outputs: [],
        bytes: 1_000_000,
      });
      yield* session.write(bytes("full"));
      const write = yield* Effect.forkChild(session.write(bytes("next")), {
        startImmediately: true,
      });
      assert.isUndefined(write.pollUnsafe());
      yield* Fiber.interrupt(write);
    }),
  ),
);

it.effect("logs a worker that dies while it has no sessions", () =>
  withLogLines((lines) =>
    withPool({ workers: 1 }, (pool) =>
      Effect.gen(function* () {
        yield* openEcho(pool, "ready").pipe(Effect.scoped);
        const worker = workers.started.at(-1)!;
        yield* Effect.promise(() => worker.terminate());
        const [level, message, annotations] = yield* Queue.take(lines);
        assert.deepEqual([level, message], ["Warn", "Provider I/O worker stopped"]);
        assert.deepInclude(annotations, { worker: 0, sessions: 0, reason: "exit" });
      }),
    ),
  ),
);

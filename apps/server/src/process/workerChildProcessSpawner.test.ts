import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import { ChildProcessSpawner } from "effect/process/ChildProcessSpawner";

import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkerChildProcessSpawner from "./workerChildProcessSpawner.ts";

const node = (script: string, options: ChildProcess.CommandOptions = {}) =>
  ChildProcess.make(process.execPath, ["-e", script], options);

const summarizeError = (error: PlatformError.PlatformError) => {
  const { reason } = error;
  const cause = reason instanceof PlatformError.SystemError ? reason.cause : undefined;
  return {
    message: error.message,
    reason: reason._tag,
    module: reason.module,
    method: reason.method,
    ...(reason instanceof PlatformError.SystemError
      ? { pathOrDescriptor: reason.pathOrDescriptor, syscall: reason.syscall }
      : {}),
    causeCode: (cause as NodeJS.ErrnoException | undefined)?.code,
    causeMessage: (cause as Error | undefined)?.message,
  };
};

const collect = (spawner: ChildProcessSpawner["Service"], command: ChildProcess.Command) =>
  Effect.gen(function* () {
    const handle = yield* spawner.spawn(command);
    const [stdout, stderr, exit] = yield* Effect.all(
      [
        Stream.mkString(Stream.decodeText(handle.stdout)),
        Stream.mkString(Stream.decodeText(handle.stderr)),
        Effect.exit(handle.exitCode),
      ],
      { concurrency: "unbounded" },
    );
    return {
      stdout,
      stderr,
      exit: Exit.isSuccess(exit)
        ? { code: exit.value }
        : { error: Exit.isFailure(exit) ? String(exit.cause) : undefined },
      running: yield* handle.isRunning,
    };
  }).pipe(Effect.scoped);

const spawnError = (spawner: ChildProcessSpawner["Service"], command: ChildProcess.Command) =>
  spawner.spawn(command).pipe(Effect.scoped, Effect.flip, Effect.map(summarizeError));

// Wraps the Node spawner and counts the commands that reach it.
const recordingSpawner = Effect.gen(function* () {
  const nodeSpawner = yield* ChildProcessSpawner;
  const calls: Array<ChildProcess.Command> = [];
  const spawner = ChildProcessSpawner.of({
    ...nodeSpawner,
    spawn: (command) => {
      calls.push(command);
      return nodeSpawner.spawn(command);
    },
  });
  return { calls, spawner };
});

describe("WorkerChildProcessSpawner", () => {
  it.live("matches the Node spawner's output, stdin, exit codes and signal exits", () =>
    Effect.gen(function* () {
      const nodeSpawner = yield* ChildProcessSpawner;
      // A small window makes the 3 MiB output pause and resume the worker's reads.
      const workerSpawner = yield* WorkerChildProcessSpawner.make(nodeSpawner, {
        windowBytes: 64 * 1024,
      });
      const commands = [
        node("process.stdout.write('out'); process.stderr.write('err'); process.exitCode = 3"),
        node("process.stdin.pipe(process.stdout)", {
          stdin: Stream.encodeText(Stream.make("from ", "stdin")),
        }),
        node("process.kill(process.pid, 'SIGTERM')"),
        node(`process.stdout.write(Buffer.alloc(${3 * 1024 * 1024}, 97))`),
      ];
      for (const command of commands) {
        const expected = yield* collect(nodeSpawner, command);
        expect(yield* collect(workerSpawner, command)).toEqual(expected);
      }
      expect((yield* collect(workerSpawner, commands[3]!)).stdout).toHaveLength(3 * 1024 * 1024);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("fails like the Node spawner for a missing executable or working directory", () =>
    Effect.gen(function* () {
      const nodeSpawner = yield* ChildProcessSpawner;
      const workerSpawner = yield* WorkerChildProcessSpawner.make(nodeSpawner);
      const missingExecutable = ChildProcess.make("t3-missing-executable-p1060", ["--version"]);
      const missingCwd = node("1", { cwd: "/nonexistent/t3-missing-cwd-p1060" });

      const expected = yield* spawnError(nodeSpawner, missingExecutable);
      expect(expected).toMatchObject({ reason: "NotFound", method: "spawn", causeCode: "ENOENT" });
      expect(yield* spawnError(workerSpawner, missingExecutable)).toEqual(expected);
      expect(yield* spawnError(workerSpawner, missingCwd)).toEqual(
        yield* spawnError(nodeSpawner, missingCwd),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("kills a running child, escalating to SIGKILL after forceKillAfter", () =>
    Effect.gen(function* () {
      const nodeSpawner = yield* ChildProcessSpawner;
      const workerSpawner = yield* WorkerChildProcessSpawner.make(nodeSpawner);
      const killed = (spawner: ChildProcessSpawner["Service"], ignoreSigterm: boolean) =>
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(
            node(
              `${ignoreSigterm ? "process.on('SIGTERM', () => {});" : ""} process.stdout.write('ready'); setInterval(() => {}, 1000)`,
            ),
          );
          yield* Stream.runHead(handle.stdout);
          yield* handle.kill({ forceKillAfter: "100 millis" });
          const exit = yield* Effect.flip(handle.exitCode);
          return { exit: summarizeError(exit), running: yield* handle.isRunning };
        }).pipe(Effect.scoped);

      for (const ignoreSigterm of [false, true]) {
        const result = yield* killed(workerSpawner, ignoreSigterm);
        expect(result).toEqual(yield* killed(nodeSpawner, ignoreSigterm));
        expect(result.exit.causeMessage).toContain(ignoreSigterm ? "SIGKILL" : "SIGTERM");
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("runs other command shapes on the fallback spawner", () =>
    Effect.gen(function* () {
      const { calls, spawner } = yield* recordingSpawner;
      const workerSpawner = yield* WorkerChildProcessSpawner.make(spawner);

      yield* collect(workerSpawner, node("process.stdout.write('worker')"));
      expect(calls).toHaveLength(0);

      const ignoredStdin = node("process.stdout.write('fallback')", { stdin: "ignore" });
      expect((yield* collect(workerSpawner, ignoredStdin)).stdout).toBe("fallback");
      expect(calls).toEqual([ignoredStdin]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("fails the spawn in flight and falls back once the worker stops", () =>
    Effect.gen(function* () {
      const { calls, spawner } = yield* recordingSpawner;
      const workerSpawner = yield* WorkerChildProcessSpawner.make(spawner, {
        workerSource: `require("node:worker_threads").parentPort.on("message", () => { throw new Error("worker crashed"); });`,
      });
      const command = node("process.stdout.write('after')");

      expect(yield* spawnError(workerSpawner, command)).toMatchObject({
        reason: "Unknown",
        method: "spawn",
      });
      expect(calls).toHaveLength(0);
      expect((yield* collect(workerSpawner, command)).stdout).toBe("after");
      expect(calls).toEqual([command]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    `${WorkerChildProcessSpawner.VCS_SPAWN_WORKER_ENV} moves VCS commands off the main spawner, results and errors unchanged`,
    () =>
      Effect.gen(function* () {
        const outcome = (knob: string | undefined) =>
          Effect.gen(function* () {
            const { calls, spawner } = yield* recordingSpawner;
            const layer = VcsProcess.layer.pipe(
              Layer.provide(Layer.succeed(ChildProcessSpawner, spawner)),
              Layer.provide(
                ConfigProvider.layer(
                  ConfigProvider.fromUnknown(
                    knob === undefined
                      ? {}
                      : { [WorkerChildProcessSpawner.VCS_SPAWN_WORKER_ENV]: knob },
                  ),
                ),
              ),
            );
            return yield* Effect.gen(function* () {
              const vcs = yield* VcsProcess.VcsProcess;
              const version = yield* vcs.run({
                operation: "test.worker-spawner",
                command: "git",
                args: ["--version"],
                cwd: process.cwd(),
              });
              const missing = yield* Effect.flip(
                vcs.run({
                  operation: "test.worker-spawner",
                  command: "t3-missing-executable-p1060",
                  args: [],
                  cwd: process.cwd(),
                }),
              );
              return {
                version: {
                  exitCode: version.exitCode,
                  stdout: version.stdout,
                  stderr: version.stderr,
                },
                missing: { tag: missing._tag, message: missing.message },
                mainSpawns: calls.length,
              };
            }).pipe(Effect.provide(layer));
          });

        const unset = yield* outcome(undefined);
        expect(unset.mainSpawns).toBe(2);
        expect(unset.missing.tag).toBe("VcsProcessSpawnError");
        expect(yield* outcome("false")).toEqual(unset);
        expect(yield* outcome("true")).toEqual({ ...unset, mainSpawns: 0 });
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});

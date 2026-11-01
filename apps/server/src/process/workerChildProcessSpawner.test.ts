import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import { ChildProcessSpawner } from "effect/process/ChildProcessSpawner";
import { vi } from "vite-plus/test";

import * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkerChildProcessSpawner from "./workerChildProcessSpawner.ts";

// Windows keeps the main-thread spawner, so the worker cases run elsewhere.
const workerPlatform = HostProcess.Platform.defaultValue() !== "win32";

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
  it.live.skipIf(!workerPlatform)(
    "matches the Node spawner's output, stdin, exit codes and signal exits",
    () =>
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

  it.live.skipIf(!workerPlatform)(
    "fails like the Node spawner for a missing executable or working directory",
    () =>
      Effect.gen(function* () {
        const nodeSpawner = yield* ChildProcessSpawner;
        const workerSpawner = yield* WorkerChildProcessSpawner.make(nodeSpawner);
        const missingExecutable = ChildProcess.make("t3-missing-executable-p1060", ["--version"]);
        const missingCwd = node("1", { cwd: "/nonexistent/t3-missing-cwd-p1060" });

        const expected = yield* spawnError(nodeSpawner, missingExecutable);
        expect(expected).toMatchObject({
          reason: "NotFound",
          method: "spawn",
          causeCode: "ENOENT",
        });
        expect(yield* spawnError(workerSpawner, missingExecutable)).toEqual(expected);
        expect(yield* spawnError(workerSpawner, missingCwd)).toEqual(
          yield* spawnError(nodeSpawner, missingCwd),
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "kills a running child, escalating to SIGKILL after forceKillAfter",
    () =>
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

      const piped = node("process.stdout.write('worker')");
      yield* collect(workerSpawner, piped);
      const onMain = workerPlatform ? [] : [piped];
      expect(calls).toEqual(onMain);

      const ignoredStdin = node("process.stdout.write('fallback')", { stdin: "ignore" });
      expect((yield* collect(workerSpawner, ignoredStdin)).stdout).toBe("fallback");
      onMain.push(ignoredStdin);
      expect(calls).toEqual(onMain);

      const windowsSpawner = yield* WorkerChildProcessSpawner.make(spawner).pipe(
        Effect.provideService(HostProcess.Platform, "win32"),
      );
      const onWindows = node("process.stdout.write('windows')");
      expect((yield* collect(windowsSpawner, onWindows)).stdout).toBe("windows");
      onMain.push(onWindows);
      expect(calls).toEqual(onMain);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "kills the children of a worker that stops, without waiting on them",
    () =>
      Effect.gen(function* () {
        const nodeSpawner = yield* ChildProcessSpawner;
        // The worker throws on the first stdin write, after the child is running.
        const workerSource = WorkerChildProcessSpawner.spawnWorkerSource.replace(
          'case "stdin":',
          'case "stdin": throw new Error("worker crashed");',
        );
        expect(workerSource).not.toBe(WorkerChildProcessSpawner.spawnWorkerSource);
        for (const detached of [true, false]) {
          const workerSpawner = yield* WorkerChildProcessSpawner.make(nodeSpawner, {
            workerSource,
          });
          const kill = vi.spyOn(process, "kill");
          yield* Effect.gen(function* () {
            const handle = yield* workerSpawner.spawn(
              node("process.stdin.resume(); setInterval(() => {}, 1000)", { detached }),
            );
            yield* Stream.run(Stream.make(new Uint8Array([1])), handle.stdin).pipe(Effect.ignore);
            const error = yield* Effect.flip(handle.exitCode);
            expect(summarizeError(error).causeMessage).toContain("SIGKILL");
            expect(yield* handle.isRunning).toBe(false);
            expect(kill).toHaveBeenCalledWith(detached ? -handle.pid : handle.pid, "SIGKILL");
            // Only the stopped worker could reap the child, and Linux signals a zombie's group as
            // if it were alive: simulate that, and expect no wait on it.
            const realKill = process.kill.bind(process);
            kill.mockImplementation((pid, signal) =>
              pid === -handle.pid && (signal === 0 || signal === "SIGTERM")
                ? true
                : realKill(pid, signal),
            );
            yield* handle.kill();
            expect(kill).not.toHaveBeenCalledWith(-handle.pid, 0);
          }).pipe(Effect.scoped, Effect.ensuring(Effect.sync(() => kill.mockRestore())));
        }
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("runs the command on the fallback spawner when the worker cannot start", () =>
    Effect.gen(function* () {
      const { calls, spawner } = yield* recordingSpawner;
      // Node rejects a non-string eval source synchronously, like any failed construction.
      const workerSpawner = yield* WorkerChildProcessSpawner.make(spawner, {
        workerSource: 0 as unknown as string,
      });
      const command = node("process.stdout.write('fallback')");
      expect((yield* collect(workerSpawner, command)).stdout).toBe("fallback");
      expect(calls).toEqual([command]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "fails the spawn in flight and falls back once the worker stops",
    () =>
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

  it.live.skipIf(!workerPlatform)(
    "settles stdin writes and kills that the worker answers after the child closed",
    () =>
      Effect.gen(function* () {
        const nodeSpawner = yield* ChildProcessSpawner;
        // The worker reports exit and close before it answers, as when the child exits meanwhile.
        const workerSource = WorkerChildProcessSpawner.spawnWorkerSource.replace(
          "const post = (message, transfer) => parentPort.postMessage(message, transfer);",
          `const post = (message, transfer) => {
            if (message.t === "stdin-done" || message.t === "kill-result") {
              parentPort.postMessage({ t: "exit", id: message.id, code: 0, signal: null });
              parentPort.postMessage({ t: "close", id: message.id });
            }
            parentPort.postMessage(message, transfer);
          };`,
        );
        expect(workerSource).not.toBe(WorkerChildProcessSpawner.spawnWorkerSource);
        const script = "process.stdin.resume(); setInterval(() => {}, 1000)";

        const writer = yield* WorkerChildProcessSpawner.make(nodeSpawner, { workerSource });
        yield* Effect.gen(function* () {
          const handle = yield* writer.spawn(node(script));
          // Ending stdin after the close fails as Node's does; the write itself settles.
          yield* Effect.exit(Stream.run(Stream.make(new Uint8Array([1])), handle.stdin));
          expect(yield* handle.exitCode).toBe(0);
        }).pipe(Effect.scoped);

        const killer = yield* WorkerChildProcessSpawner.make(nodeSpawner, { workerSource });
        yield* Effect.gen(function* () {
          // Without its own process group, the kill goes through the worker.
          const handle = yield* killer.spawn(node(script, { detached: false }));
          yield* handle.kill({ killSignal: "SIGKILL" });
          expect(yield* handle.exitCode).toBe(0);
        }).pipe(Effect.scoped);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    `${WorkerChildProcessSpawner.VCS_SPAWN_WORKER_ENV} moves VCS commands off the main spawner, results and errors unchanged`,
    () =>
      Effect.gen(function* () {
        const outcome = (knob: string | undefined) =>
          Effect.gen(function* () {
            const { calls, spawner } = yield* recordingSpawner;
            // As in server.ts, a shared ProcessRunner is provided outside VcsProcess too.
            const layer = Layer.mergeAll(VcsProcess.layer, GitVcsDriver.layer).pipe(
              Layer.provide(ProcessRunner.layer),
              Layer.provide(
                ServerConfig.layerTest(process.cwd(), { prefix: "t3-worker-spawner-test-" }),
              ),
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
              const git = yield* GitVcsDriver.GitVcsDriver;
              const gitVersion = yield* git.execute({
                operation: "test.worker-spawner",
                cwd: process.cwd(),
                args: ["--version"],
              });
              const missingCwd = yield* Effect.flip(
                git.execute({
                  operation: "test.worker-spawner",
                  cwd: "/nonexistent/t3-missing-cwd-p1060",
                  args: ["--version"],
                }),
              );
              return {
                version: {
                  exitCode: version.exitCode,
                  stdout: version.stdout,
                  stderr: version.stderr,
                },
                missing: { tag: missing._tag, message: missing.message },
                gitVersion: {
                  exitCode: gitVersion.exitCode,
                  stdout: gitVersion.stdout,
                  stderr: gitVersion.stderr,
                },
                missingCwd: {
                  tag: missingCwd._tag,
                  detail: missingCwd.detail,
                  cause: summarizeError(missingCwd.cause as PlatformError.PlatformError),
                },
                mainSpawns: calls.length,
              };
            }).pipe(Effect.provide(layer));
          });

        const unset = yield* outcome(undefined);
        expect(unset.mainSpawns).toBe(4);
        expect(unset.missing.tag).toBe("VcsProcessSpawnError");
        expect(unset.missingCwd).toMatchObject({
          tag: "GitCommandError",
          cause: { reason: "NotFound" },
        });
        expect(yield* outcome("false")).toEqual(unset);
        expect(yield* outcome("true")).toEqual({ ...unset, mainSpawns: workerPlatform ? 0 : 4 });
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});

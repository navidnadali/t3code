// @effect-diagnostics nodeBuiltinImport:off - Children prove they are gone by closing a socket; a server process is killed outright.
import * as NodeChildProcess from "node:child_process";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
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

const bytes = (stream: Stream.Stream<Uint8Array, PlatformError.PlatformError>) =>
  Effect.map(Stream.runCollect(stream), (chunks) => Buffer.concat(chunks));

const collect = (spawner: ChildProcessSpawner["Service"], command: ChildProcess.Command) =>
  Effect.gen(function* () {
    const handle = yield* spawner.spawn(command);
    const [stdout, stderr, exit] = yield* Effect.all(
      [bytes(handle.stdout), bytes(handle.stderr), Effect.exit(handle.exitCode)],
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

interface RemoteOptions {
  readonly source?: string;
  readonly windowBytes?: number;
}

const remotes = [
  {
    name: "worker thread",
    make: (fallback: ChildProcessSpawner["Service"], options: RemoteOptions = {}) =>
      WorkerChildProcessSpawner.make(fallback, {
        ...(options.source === undefined ? {} : { workerSource: options.source }),
        ...(options.windowBytes === undefined ? {} : { windowBytes: options.windowBytes }),
      }),
  },
  {
    name: "spawner process",
    make: (fallback: ChildProcessSpawner["Service"], options: RemoteOptions = {}) =>
      WorkerChildProcessSpawner.makeProcessSpawner(fallback, options),
  },
] as const;

// A child connects to this server and holds the connection until it dies.
const childWatch = Effect.gen(function* () {
  const connections = yield* Queue.unbounded<NodeNet.Socket>();
  const sockets = new Set<NodeNet.Socket>();
  const server = yield* Effect.acquireRelease(
    Effect.callback<NodeNet.Server>((resume) => {
      const server = NodeNet.createServer((socket) => {
        sockets.add(socket);
        socket.on("error", () => {});
        socket.resume();
        Queue.offerUnsafe(connections, socket);
      });
      server.listen(0, "127.0.0.1", () => resume(Effect.succeed(server)));
    }),
    (server) =>
      Effect.callback<void>((resume) => {
        server.close(() => resume(Effect.void));
        for (const socket of sockets) socket.destroy();
      }),
  );
  const { port } = server.address() as NodeNet.AddressInfo;
  return {
    connectScript: (then = "") =>
      `require("node:net").connect(${port}, "127.0.0.1").on("connect", () => { process.stdout.write(String(process.ppid)); ${then} }); setInterval(() => {}, 1000)`,
    connected: Queue.take(connections),
    closed: (socket: NodeNet.Socket) =>
      Effect.callback<void>((resume) => {
        if (socket.closed) return resume(Effect.void);
        socket.once("close", () => resume(Effect.void));
      }),
  };
});

const capturedLogs = () => {
  const logs: Array<{ readonly level: string; readonly message: unknown }> = [];
  const layer = Logger.layer(
    [Logger.make(({ logLevel, message }) => logs.push({ level: logLevel, message }))],
    { mergeWithExisting: false },
  );
  return { logs, layer };
};

describe("WorkerChildProcessSpawner", () => {
  for (const remote of remotes) {
    it.live.skipIf(!workerPlatform)(
      `${remote.name}: matches the Node spawner's output bytes, stdin, cwd, env, exit codes and signal exits`,
      () =>
        Effect.gen(function* () {
          const nodeSpawner = yield* ChildProcessSpawner;
          // A small window makes the 3 MiB output pause and resume the remote reads.
          const remoteSpawner = yield* remote.make(nodeSpawner, { windowBytes: 64 * 1024 });
          // Set after the remote side started: Node reads process.env at each spawn.
          yield* Effect.acquireRelease(
            Effect.sync(() => {
              process.env.T3_P1070_LATE = "late";
            }),
            () =>
              Effect.sync(() => {
                delete process.env.T3_P1070_LATE;
              }),
          );
          const printEnv =
            "process.stdout.write(JSON.stringify([process.env.T3_P1070_SET, process.env.T3_P1070_LATE, typeof process.env.PATH]))";
          const commands = [
            node("process.stdout.write('out'); process.stderr.write('err'); process.exitCode = 3"),
            node(
              "process.stdout.write(Buffer.from([0xff, 0x00, 0xfe, 0x80])); process.stderr.write(Buffer.from([0xc3, 0x28]))",
            ),
            node("process.stdin.pipe(process.stdout)", {
              stdin: Stream.encodeText(Stream.make("from ", "stdin")),
            }),
            node("process.stdout.write(process.cwd())", { cwd: NodeOS.tmpdir() }),
            node(printEnv),
            node(printEnv, { env: { T3_P1070_SET: "set" }, extendEnv: true }),
            node(printEnv, { env: { T3_P1070_SET: "set" } }),
            node("process.kill(process.pid, 'SIGTERM')"),
            node("process.kill(process.pid, 'SIGKILL')"),
            node(`process.stdout.write(Buffer.alloc(${3 * 1024 * 1024}, 97))`),
            ChildProcess.make("git", ["--version"]),
            ChildProcess.make("git", ["rev-parse", "--show-toplevel"], { cwd: process.cwd() }),
            ChildProcess.make("git", ["cat-file", "-p", "HEAD"], { cwd: process.cwd() }),
            ChildProcess.make("git", ["rev-parse", "--verify", "refs/heads/t3-missing-p1070"], {
              cwd: process.cwd(),
            }),
            ChildProcess.make("git", ["hash-object", "--stdin"], {
              stdin: Stream.encodeText(Stream.make("p1-070\n")),
            }),
          ];
          for (const command of commands) {
            const expected = yield* collect(nodeSpawner, command);
            expect(yield* collect(remoteSpawner, command)).toEqual(expected);
          }
          expect((yield* collect(remoteSpawner, commands[4]!)).stdout.toString()).toBe(
            JSON.stringify([undefined, "late", "string"]),
          );
          expect((yield* collect(remoteSpawner, commands[9]!)).stdout).toHaveLength(
            3 * 1024 * 1024,
          );
        }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );

    it.live.skipIf(!workerPlatform)(
      `${remote.name}: fails like the Node spawner for a missing executable or working directory`,
      () =>
        Effect.gen(function* () {
          const nodeSpawner = yield* ChildProcessSpawner;
          const remoteSpawner = yield* remote.make(nodeSpawner);
          const missingExecutable = ChildProcess.make("t3-missing-executable-p1060", ["--version"]);
          const missingCwd = node("1", { cwd: "/nonexistent/t3-missing-cwd-p1060" });

          const expected = yield* spawnError(nodeSpawner, missingExecutable);
          expect(expected).toMatchObject({
            reason: "NotFound",
            method: "spawn",
            causeCode: "ENOENT",
          });
          expect(yield* spawnError(remoteSpawner, missingExecutable)).toEqual(expected);
          expect(yield* spawnError(remoteSpawner, missingCwd)).toEqual(
            yield* spawnError(nodeSpawner, missingCwd),
          );
        }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );

    it.live.skipIf(!workerPlatform)(
      `${remote.name}: kills a running child, escalating to SIGKILL after forceKillAfter`,
      () =>
        Effect.gen(function* () {
          const nodeSpawner = yield* ChildProcessSpawner;
          const remoteSpawner = yield* remote.make(nodeSpawner);
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
            const result = yield* killed(remoteSpawner, ignoreSigterm);
            expect(result).toEqual(yield* killed(nodeSpawner, ignoreSigterm));
            expect(result.exit.causeMessage).toContain(ignoreSigterm ? "SIGKILL" : "SIGTERM");
          }
        }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    );
  }

  it.live("runs other command shapes on the fallback spawner", () =>
    Effect.gen(function* () {
      const { calls, spawner } = yield* recordingSpawner;
      const workerSpawner = yield* WorkerChildProcessSpawner.make(spawner);

      const piped = node("process.stdout.write('worker')");
      yield* collect(workerSpawner, piped);
      const onMain = workerPlatform ? [] : [piped];
      expect(calls).toEqual(onMain);

      const ignoredStdin = node("process.stdout.write('fallback')", { stdin: "ignore" });
      expect((yield* collect(workerSpawner, ignoredStdin)).stdout.toString()).toBe("fallback");
      onMain.push(ignoredStdin);
      expect(calls).toEqual(onMain);

      const windowsSpawner = yield* WorkerChildProcessSpawner.make(spawner).pipe(
        Effect.provideService(HostProcess.Platform, "win32"),
      );
      const onWindows = node("process.stdout.write('windows')");
      expect((yield* collect(windowsSpawner, onWindows)).stdout.toString()).toBe("windows");
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

  it.live.skipIf(!workerPlatform)(
    "kills the process group of an exited child whose descendant outlives the worker",
    () =>
      Effect.gen(function* () {
        const nodeSpawner = yield* ChildProcessSpawner;
        const workerSource = WorkerChildProcessSpawner.spawnWorkerSource.replace(
          'case "stdin":',
          'case "stdin": throw new Error("worker crashed");',
        );
        const workerSpawner = yield* WorkerChildProcessSpawner.make(nodeSpawner, { workerSource });
        // The leader exits at once; its SIGTERM-ignoring descendant keeps stdout open.
        const leader = node(
          `const child = require("node:child_process").spawn(process.execPath, ["-e", "process.on('SIGTERM', () => {}); setInterval(() => {}, 1000)"], { stdio: ["ignore", "inherit", "ignore"] }); process.stdout.write(String(child.pid)); process.exit(0)`,
        );
        let descendant: number | undefined;
        const kill = vi.spyOn(process, "kill");
        yield* Effect.gen(function* () {
          const handle = yield* workerSpawner.spawn(leader);
          const first = yield* Stream.runHead(Stream.decodeText(handle.stdout));
          descendant = Number(first._tag === "Some" ? first.value : Number.NaN);
          expect(yield* handle.exitCode).toBe(0);
          yield* Stream.run(Stream.make(new Uint8Array([1])), handle.stdin).pipe(Effect.ignore);
          expect(kill).toHaveBeenCalledWith(-handle.pid, "SIGKILL");
        }).pipe(
          Effect.scoped,
          Effect.ensuring(
            Effect.sync(() => {
              kill.mockRestore();
              if (descendant !== undefined && Number.isInteger(descendant)) {
                try {
                  process.kill(descendant, "SIGKILL");
                } catch {
                  // Already gone.
                }
              }
            }),
          ),
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "never signals the server's own process group for a child the worker reports as pid 0",
    () =>
      Effect.gen(function* () {
        const nodeSpawner = yield* ChildProcessSpawner;
        // The worker reports pid 0, then throws on the first stdin write.
        const workerSource = WorkerChildProcessSpawner.spawnWorkerSource
          .replace("pid: child.pid", "pid: 0")
          .replace('case "stdin":', 'case "stdin": throw new Error("worker crashed");');
        expect(workerSource).toContain("pid: 0");
        const realKill = process.kill.bind(process);
        // kill(0) would signal this test's own group: refuse it as a missing group would.
        const kill = vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
          if (Math.abs(pid) <= 1) throw Object.assign(new Error("kill ESRCH"), { code: "ESRCH" });
          return realKill(pid, signal);
        });
        let stranded: number | undefined;
        const ownGroupCalls = yield* Effect.gen(function* () {
          const workerSpawner = yield* WorkerChildProcessSpawner.make(nodeSpawner, {
            workerSource,
          });
          // A failed exit signals the group from its exit listener and its release.
          yield* Effect.gen(function* () {
            const failed = yield* workerSpawner.spawn(node("process.exit(3)"));
            expect(yield* failed.exitCode).toBe(3);
          }).pipe(Effect.scoped);
          // A kill falls back to the worker's own kill of the child.
          yield* Effect.gen(function* () {
            const running = yield* workerSpawner.spawn(node("setInterval(() => {}, 1000)"));
            yield* running.kill();
            expect(yield* running.isRunning).toBe(false);
          }).pipe(Effect.scoped);
          // A stopped worker kills its children's groups.
          yield* Effect.gen(function* () {
            const child = yield* workerSpawner.spawn(
              node(
                "process.stdout.write(String(process.pid)); process.stdin.on('end', () => process.exit(0)); process.stdin.resume()",
              ),
            );
            const first = yield* Stream.runHead(Stream.decodeText(child.stdout));
            stranded = Number(first._tag === "Some" ? first.value : Number.NaN);
            yield* Stream.run(Stream.make(new Uint8Array([1])), child.stdin).pipe(Effect.ignore);
            yield* Effect.flip(child.exitCode);
          }).pipe(Effect.scoped);
          return kill.mock.calls.filter(([pid]) => Math.abs(pid) <= 1);
        }).pipe(
          Effect.scoped,
          Effect.ensuring(
            Effect.sync(() => {
              kill.mockRestore();
              if (stranded !== undefined && Number.isInteger(stranded) && stranded > 1) {
                try {
                  process.kill(stranded, "SIGKILL");
                } catch {
                  // Already gone.
                }
              }
            }),
          ),
        );
        expect(ownGroupCalls).toEqual([]);
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
      expect((yield* collect(workerSpawner, command)).stdout.toString()).toBe("fallback");
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
        expect((yield* collect(workerSpawner, command)).stdout.toString()).toBe("after");
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

  it.live.skipIf(!workerPlatform)(
    "kills the running children of a spawner process that stops and restarts it for the next command",
    () => {
      const { logs, layer } = capturedLogs();
      return Effect.gen(function* () {
        const { calls, spawner } = yield* recordingSpawner;
        const watch = yield* childWatch;
        // The spawner exits on the first stdin write, after the child is running.
        const source = WorkerChildProcessSpawner.spawnerProcessSource.replace(
          'case "stdin":',
          'case "stdin": process.exit(70);',
        );
        expect(source).not.toBe(WorkerChildProcessSpawner.spawnerProcessSource);
        const remoteSpawner = yield* WorkerChildProcessSpawner.makeProcessSpawner(spawner, {
          source,
        });
        const run = Effect.gen(function* () {
          const handle = yield* remoteSpawner.spawn(
            node(watch.connectScript("process.stdin.resume();")),
          );
          const socket = yield* watch.connected;
          const spawnerPid = (yield* bytes(Stream.take(handle.stdout, 1))).toString();
          yield* Stream.run(Stream.make(new Uint8Array([1])), handle.stdin).pipe(Effect.ignore);
          const error = yield* Effect.flip(handle.exitCode);
          expect(summarizeError(error).causeMessage).toContain("SIGKILL");
          expect(yield* handle.isRunning).toBe(false);
          yield* watch.closed(socket);
          return spawnerPid;
        }).pipe(Effect.scoped);

        const first = yield* run;
        // The next command restarts the spawner; it may stop again once it has started one.
        const second = yield* run;
        expect(second).not.toBe(first);
        const after = node("process.stdout.write(String(process.ppid))");
        const third = (yield* collect(remoteSpawner, after)).stdout.toString();
        expect([first, second]).not.toContain(third);
        expect(calls).toEqual([]);
        expect(logs.map(({ level }) => level)).toEqual(["Warn", "Warn"]);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, layer)));
    },
  );

  it.live.skipIf(!workerPlatform)(
    "fails commands, never on the server's spawner, once a restarted spawner process stops before starting one",
    () => {
      const { logs, layer } = capturedLogs();
      return Effect.gen(function* () {
        const { calls, spawner } = yield* recordingSpawner;
        const source = WorkerChildProcessSpawner.spawnerProcessSource.replace(
          'case "spawn":',
          'case "spawn": if (message.args.includes("stop-spawner")) process.exit(70);',
        );
        expect(source).not.toBe(WorkerChildProcessSpawner.spawnerProcessSource);
        const remoteSpawner = yield* WorkerChildProcessSpawner.makeProcessSpawner(spawner, {
          source,
        });
        const stopping = ChildProcess.make(process.execPath, ["-e", "1", "stop-spawner"]);
        const healthy = node("process.stdout.write('ok')");

        for (let attempt = 0; attempt < 2; attempt++) {
          expect(yield* spawnError(remoteSpawner, stopping)).toMatchObject({
            reason: "Unknown",
            method: "spawn",
            causeMessage: "The process spawner stopped",
          });
        }
        expect(yield* spawnError(remoteSpawner, healthy)).toMatchObject({
          reason: "Unknown",
          method: "spawn",
          causeMessage: "The process spawner stopped",
        });
        expect(calls).toEqual([]);
        expect(logs.map(({ level }) => level)).toEqual(["Warn", "Error"]);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, layer)));
    },
  );

  it.live.skipIf(!workerPlatform)(
    "closing the spawner process kills the children it still runs and ends the spawner",
    () =>
      Effect.gen(function* () {
        const nodeSpawner = yield* ChildProcessSpawner;
        const watch = yield* childWatch;
        const { socket, spawnerPid } = yield* Effect.gen(function* () {
          const remoteSpawner = yield* WorkerChildProcessSpawner.makeProcessSpawner(nodeSpawner);
          const handle = yield* remoteSpawner.spawn(node(watch.connectScript()));
          // Unreferenced, so its own release leaves it running.
          yield* Effect.asVoid(handle.unref);
          const socket = yield* watch.connected;
          const spawnerPid = Number((yield* bytes(Stream.take(handle.stdout, 1))).toString());
          return { socket, spawnerPid };
        }).pipe(Effect.scoped);
        yield* watch.closed(socket);
        // Node reaps the spawner before reporting its exit, which closing waited for.
        expect(() => process.kill(spawnerPid, 0)).toThrow(
          expect.objectContaining({ code: "ESRCH" }),
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "closing the spawner process lets it kill a child whose start the server has not heard of yet",
    () =>
      Effect.gen(function* () {
        const nodeSpawner = yield* ChildProcessSpawner;
        const watch = yield* childWatch;
        // The spawner never reports this child's start, so only the spawner knows its PID.
        const source = WorkerChildProcessSpawner.spawnerProcessSource.replace(
          'child.on("spawn", () => post({ t: "spawn", id, pid: child.pid }));',
          'child.on("spawn", () => message.args.includes("unreported") || post({ t: "spawn", id, pid: child.pid }));',
        );
        expect(source).not.toBe(WorkerChildProcessSpawner.spawnerProcessSource);
        const outer = yield* Effect.scope;
        const socket = yield* Effect.gen(function* () {
          const remoteSpawner = yield* WorkerChildProcessSpawner.makeProcessSpawner(nodeSpawner, {
            source,
          });
          // Outlives the spawner's scope: closing it fails this pending spawn instead.
          const pending = yield* Effect.forkIn(
            remoteSpawner.spawn(
              ChildProcess.make(process.execPath, ["-e", watch.connectScript(), "unreported"]),
            ),
            outer,
          );
          return { connected: yield* watch.connected, pending };
        }).pipe(Effect.scoped);
        yield* watch.closed(socket.connected);
        expect(yield* Effect.flip(Fiber.join(socket.pending))).toMatchObject({
          message: expect.stringContaining("spawn"),
        });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "kills a child of a spawner process that dies before acknowledging its start",
    () =>
      Effect.gen(function* () {
        const nodeSpawner = yield* ChildProcessSpawner;
        const watch = yield* childWatch;
        const source = WorkerChildProcessSpawner.spawnerProcessSource.replace(
          'child.on("spawn", () => post({ t: "spawn", id, pid: child.pid }));',
          'child.on("spawn", () => message.args.includes("unacknowledged") || post({ t: "spawn", id, pid: child.pid }));',
        );
        expect(source).not.toBe(WorkerChildProcessSpawner.spawnerProcessSource);
        const remoteSpawner = yield* WorkerChildProcessSpawner.makeProcessSpawner(nodeSpawner, {
          source,
        });
        // Once connected, the child kills the spawner, whose acknowledgement never came.
        const pending = yield* Effect.forkScoped(
          remoteSpawner
            .spawn(
              ChildProcess.make(process.execPath, [
                "-e",
                watch.connectScript('process.kill(process.ppid, "SIGKILL");'),
                "unacknowledged",
              ]),
            )
            .pipe(Effect.scoped),
        );
        yield* watch.closed(yield* watch.connected);
        expect(summarizeError(yield* Effect.flip(Fiber.join(pending)))).toMatchObject({
          method: "spawn",
          causeMessage: "The process spawner stopped",
        });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "a spawner process that fails kills the children it started before it exits",
    () =>
      Effect.gen(function* () {
        const nodeSpawner = yield* ChildProcessSpawner;
        const watch = yield* childWatch;
        // Only the spawner knows this child; the spawner then fails on the child's first output.
        const source = WorkerChildProcessSpawner.spawnerProcessSource
          .replace(
            "if (child.pid !== undefined) started(id, child.pid);",
            'if (child.pid !== undefined && !message.args.includes("unreported")) started(id, child.pid);',
          )
          .replace(
            'child.on("spawn", () => post({ t: "spawn", id, pid: child.pid }));',
            'child.on("spawn", () => message.args.includes("unreported") || post({ t: "spawn", id, pid: child.pid }));',
          )
          .replace(
            'stream.on("data", (chunk) => {',
            'stream.on("data", (chunk) => { if (message.args.includes("unreported")) throw new Error("spawner bug");',
          );
        expect(source.match(/unreported/g)).toHaveLength(3);
        const remoteSpawner = yield* WorkerChildProcessSpawner.makeProcessSpawner(nodeSpawner, {
          source,
        });
        const pending = yield* Effect.forkScoped(
          remoteSpawner
            .spawn(ChildProcess.make(process.execPath, ["-e", watch.connectScript(), "unreported"]))
            .pipe(Effect.scoped),
        );
        yield* watch.closed(yield* watch.connected);
        expect(summarizeError(yield* Effect.flip(Fiber.join(pending)))).toMatchObject({
          method: "spawn",
          causeMessage: "The process spawner stopped",
        });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "the spawner process kills its children and exits when the server dies",
    () =>
      Effect.gen(function* () {
        const watch = yield* childWatch;
        const launch = WorkerChildProcessSpawner.spawnerProcessLaunch({
          executable: process.execPath,
          isExecutable: false,
          environment: process.env,
          windowBytes: 64 * 1024,
        });
        const children = [true, false].map((detached) => ({
          command: process.execPath,
          args: ["-e", watch.connectScript()],
          options: { env: process.env, stdio: ["pipe", "pipe", "pipe"], detached },
        }));
        // Stands in for the server: starts the spawner as the server does, then is killed.
        const server = `
          const launch = JSON.parse(process.argv[1]);
          const spawner = require("node:child_process").spawn(launch.command, launch.args, launch.options);
          launch.children.forEach((child, index) => spawner.send({ t: "spawn", id: index + 1, ...child }));`;
        const host = NodeChildProcess.spawn(
          process.execPath,
          ["-e", server, JSON.stringify({ ...launch, children })],
          { stdio: ["ignore", "ignore", "pipe"] },
        );
        // The spawner shares the server's stderr: its end means both have exited.
        const stderrEnded = yield* Deferred.make<string>();
        let stderr = "";
        host.stderr.on("data", (chunk: Buffer) => (stderr += chunk.toString()));
        host.stderr.on("end", () => Deferred.doneUnsafe(stderrEnded, Exit.succeed(stderr)));
        const sockets = [yield* watch.connected, yield* watch.connected];
        host.kill("SIGKILL");
        yield* Effect.forEach(sockets, watch.closed);
        expect(yield* Deferred.await(stderrEnded)).toBe("");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "single executables start the spawner process through the CLI entry",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const nodeSpawner = yield* ChildProcessSpawner;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-process-spawner-" });
        // Runs the CLI entry the way a single executable runs itself.
        const executable = path.join(directory, "t3");
        const entry = path.join(import.meta.dirname, "..", "bin.ts");
        yield* fs.writeFileString(
          executable,
          `#!/bin/sh\nexec ${JSON.stringify(process.execPath)} ${JSON.stringify(entry)} "$@"\n`,
        );
        yield* fs.chmod(executable, 0o755);
        const remoteSpawner = yield* WorkerChildProcessSpawner.makeProcessSpawner(nodeSpawner).pipe(
          Effect.provideService(HostProcess.ExecutablePath, executable),
          Effect.provideService(HostProcess.IsExecutable, true),
        );
        const command = ChildProcess.make("git", ["--version"]);
        expect(yield* collect(remoteSpawner, command)).toEqual(
          yield* collect(nodeSpawner, command),
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(!workerPlatform)(
    "VCS layers provided per WebSocket connection reuse the server's spawner process",
    () =>
      Effect.gen(function* () {
        const spawnerPid = Effect.gen(function* () {
          const vcs = yield* VcsProcess.VcsProcess;
          const output = yield* vcs.run({
            operation: "test.process-spawner",
            command: process.execPath,
            args: ["-e", "process.stdout.write(String(process.ppid))"],
            cwd: process.cwd(),
          });
          return Number(output.stdout);
        });
        const vcsLayers = Layer.mergeAll(VcsProcess.layer, GitVcsDriver.layer);
        const server = vcsLayers.pipe(
          Layer.provide(ProcessRunner.layer),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-process-spawner-test-" }),
          ),
          Layer.provideMerge(
            ConfigProvider.layer(
              ConfigProvider.fromUnknown({
                [WorkerChildProcessSpawner.PROCESS_SPAWNER_ENV]: "child",
              }),
            ),
          ),
        );
        yield* Effect.gen(function* () {
          const serverSpawner = yield* spawnerPid;
          // As ws.ts provides each connection's layers inside the server's context.
          const connectionSpawner = yield* spawnerPid.pipe(Effect.provide(vcsLayers));
          expect(connectionSpawner).toBe(serverSpawner);
          const spawners = NodeChildProcess.execFileSync("ps", [
            "-A",
            "-ww",
            "-o",
            "ppid=,pid=,command=",
          ])
            .toString()
            .split("\n")
            .map((line) => line.trim().split(/\s+/))
            .filter(
              ([ppid, , ...command]) =>
                Number(ppid) === process.pid && command.join(" ").includes("t3-process-spawner"),
            )
            .map(([, pid]) => Number(pid));
          expect(spawners).toEqual([serverSpawner]);
        }).pipe(Effect.provide(server));
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live(
    `${WorkerChildProcessSpawner.VCS_SPAWN_WORKER_ENV} and ${WorkerChildProcessSpawner.PROCESS_SPAWNER_ENV}=child move VCS commands off the main spawner, results and errors unchanged`,
    () =>
      Effect.gen(function* () {
        const outcome = (knobs: Record<string, string>) =>
          Effect.gen(function* () {
            const { calls, spawner } = yield* recordingSpawner;
            // As in server.ts, a shared ProcessRunner is provided outside VcsProcess too.
            const layer = Layer.mergeAll(VcsProcess.layer, GitVcsDriver.layer).pipe(
              Layer.provide(ProcessRunner.layer),
              Layer.provide(
                ServerConfig.layerTest(process.cwd(), { prefix: "t3-worker-spawner-test-" }),
              ),
              Layer.provide(Layer.succeed(ChildProcessSpawner, spawner)),
              Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(knobs))),
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

        const { VCS_SPAWN_WORKER_ENV: worker, PROCESS_SPAWNER_ENV: spawner } =
          WorkerChildProcessSpawner;
        const unset = yield* outcome({});
        expect(unset.mainSpawns).toBe(4);
        expect(unset.missing.tag).toBe("VcsProcessSpawnError");
        expect(unset.missingCwd).toMatchObject({
          tag: "GitCommandError",
          cause: { reason: "NotFound" },
        });
        expect(yield* outcome({ [worker]: "false", [spawner]: "default" })).toEqual(unset);
        const offMain = { ...unset, mainSpawns: workerPlatform ? 0 : 4 };
        expect(yield* outcome({ [worker]: "true" })).toEqual(offMain);
        expect(yield* outcome({ [spawner]: "child" })).toEqual(offMain);
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});

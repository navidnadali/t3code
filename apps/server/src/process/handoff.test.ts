// @effect-diagnostics nodeBuiltinImport:off - Fixtures connect raw sockets and kill processes by PID.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";

import * as ServerConfig from "../config.ts";
import { spawnerProcessIds } from "./spawnerProcesses.ts";
import { spawnerProcessSource } from "./spawnerSources.ts";
import * as WorkerChildProcessSpawner from "./workerChildProcessSpawner.ts";

// Hand-off identifies a provider by /proc start time, so it runs on Linux only.
const linux = HostProcess.Platform.defaultValue() === "linux";

const launcherLayer = (knobs: Record<string, string> = {}) =>
  WorkerChildProcessSpawner.layerProviderLauncher.pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-handoff-test-" })),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(knobs))),
    Layer.provideMerge(NodeServices.layer),
  );

const node = (script: string, options: ChildProcess.CommandOptions = {}) =>
  ChildProcess.make(process.execPath, ["-e", script], options);

const text = (stream: Stream.Stream<Uint8Array, PlatformError.PlatformError>) =>
  Stream.runCollect(stream).pipe(Effect.map((chunks) => Buffer.concat(chunks).toString()));

// Logs as they come; `waitFor` resolves on the first matching one, past or future.
const capturedLogs = () => {
  const logs: Array<{ readonly level: string; readonly text: string }> = [];
  const waiters = new Set<{ match: (text: string) => boolean; done: () => void }>();
  const layer = Logger.layer(
    [
      Logger.make(({ logLevel, message }) => {
        const text = (Array.isArray(message) ? message : [message]).map(String).join(" ");
        logs.push({ level: logLevel, text });
        for (const waiter of waiters) if (waiter.match(text)) waiter.done();
      }),
    ],
    { mergeWithExisting: false },
  );
  const waitFor = (match: (text: string) => boolean) =>
    Effect.callback<void>((resume) => {
      if (logs.some(({ text }) => match(text))) return resume(Effect.void);
      const waiter = { match, done: () => resume(Effect.void) };
      waiters.add(waiter);
      return Effect.sync(() => waiters.delete(waiter));
    });
  return { logs, layer, waitFor };
};

// A TCP listener a child connects to: its PID arrives, and the socket closes when it dies.
const childWatch = Effect.gen(function* () {
  const connections: Array<(socket: NodeNet.Socket) => void> = [];
  const pending: Array<NodeNet.Socket> = [];
  const server = yield* Effect.acquireRelease(
    Effect.callback<NodeNet.Server>((resume) => {
      const server = NodeNet.createServer((socket) => {
        socket.on("error", () => {});
        const next = connections.shift();
        if (next === undefined) pending.push(socket);
        else next(socket);
      });
      server.listen(0, "127.0.0.1", () => resume(Effect.succeed(server)));
    }),
    (server) => Effect.sync(() => server.close()),
  );
  const { port } = server.address() as NodeNet.AddressInfo;
  return {
    script: `require("node:net").connect(${port}, "127.0.0.1").on("connect", function () { this.write(String(process.pid)); }); setInterval(() => {}, 1000);`,
    connected: Effect.callback<NodeNet.Socket>((resume) => {
      const socket = pending.shift();
      if (socket !== undefined) return resume(Effect.succeed(socket));
      connections.push((socket) => resume(Effect.succeed(socket)));
    }),
    pidOf: (socket: NodeNet.Socket) =>
      Effect.callback<number>((resume) => {
        socket.once("data", (data) => resume(Effect.succeed(Number(String(data)))));
      }),
    closed: (socket: NodeNet.Socket) =>
      Effect.callback<void>((resume) => {
        if (socket.closed) return resume(Effect.void);
        socket.once("close", () => resume(Effect.void));
      }),
  };
});

describe("provider process hand-off", () => {
  it.live.skipIf(!linux)(
    "hands off 64 concurrent processes without losing a line of their output",
    () =>
      Effect.gen(function* () {
        const { spawner } = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
        const lines = 400;
        const outputs = yield* Effect.forEach(
          Array.from({ length: 64 }, (_, index) => index),
          (index) =>
            spawner.string(
              node(
                `process.stdout.write(process.ppid + "\\n"); for (let n = 0; n < ${lines}; n++) process.stdout.write("${index} " + n + "\\n");`,
              ),
            ),
          { concurrency: "unbounded" },
        );
        for (const [index, output] of outputs.entries()) {
          const [parent, ...rest] = output.split("\n").filter((line) => line.length > 0);
          expect(spawnerProcessIds.has(Number(parent))).toBe(true);
          expect(rest).toEqual(Array.from({ length: lines }, (_, n) => `${index} ${n}`));
        }
      }).pipe(Effect.provide(launcherLayer())),
  );

  it.live.skipIf(!linux)(
    "gives a handed-off process end of input, both directions in volume, and its stderr",
    () =>
      Effect.gen(function* () {
        const { spawner } = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
        const payload = Buffer.alloc(16 * 1024 * 1024, 120);
        const echoed = yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* spawner.spawn(
              ChildProcess.make("sh", ["-c", "cat; echo done >&2"], {
                stdin: Stream.make(new Uint8Array(payload)),
              }),
            );
            const [stdout, stderr, code] = yield* Effect.all(
              [
                Stream.runCollect(handle.stdout).pipe(
                  Effect.map((chunks) => Buffer.concat(chunks)),
                ),
                text(handle.stderr),
                handle.exitCode,
              ],
              { concurrency: "unbounded" },
            );
            return { stdout, stderr, code };
          }),
        );
        expect(echoed.stdout.equals(payload)).toBe(true);
        expect({ stderr: echoed.stderr, code: echoed.code }).toEqual({ stderr: "done\n", code: 0 });
        // What a provider sees on its stdio: no terminal, sockets rather than pipes.
        const seen = yield* spawner.string(
          node(
            `const fs = require("node:fs"); const tty = require("node:tty"); let n = 0; process.stdin.on("data", (d) => (n += d.length)); process.stdin.on("end", () => process.stdout.write(JSON.stringify({ n, tty: [0, 1].map((fd) => tty.isatty(fd)), socket: [0, 1].map((fd) => fs.fstatSync(fd).isSocket()) })));`,
            { stdin: Stream.make(new TextEncoder().encode("abc")) },
          ),
        );
        expect(JSON.parse(seen)).toEqual({ n: 3, tty: [false, false], socket: [true, true] });
      }).pipe(Effect.provide(launcherLayer())),
  );

  it.live.skipIf(!linux)(
    "keeps a provider running when the spawner dies, and reports its exit once it ends",
    () => {
      const { layer: logLayer, waitFor } = capturedLogs();
      return Effect.gen(function* () {
        const { spawner } = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
        const watch = yield* childWatch;
        const handle = yield* spawner.spawn(node(watch.script));
        const socket = yield* watch.connected;
        const pid = yield* watch.pidOf(socket);
        for (const spawnerPid of spawnerProcessIds) process.kill(spawnerPid, "SIGKILL");
        yield* waitFor((text) => text.includes("keep running without the process spawner"));
        // Its spawner is gone, it is not: still running, never reported exited.
        expect(yield* handle.isRunning).toBe(true);
        expect(socket.closed).toBe(false);
        process.kill(pid, "SIGKILL");
        yield* watch.closed(socket);
        const exit = yield* Effect.exit(handle.exitCode);
        expect(Exit.isFailure(exit)).toBe(true);
        expect(yield* handle.isRunning).toBe(false);
        yield* waitFor((text) => text.includes("outlived the process spawner exited"));
      }).pipe(Effect.scoped, Effect.provide(launcherLayer().pipe(Layer.provideMerge(logLayer))));
    },
  );

  it.effect.skipIf(!linux)("tells a reused PID from the process it named", () =>
    Effect.sync(() => {
      const stat = require("node:fs").readFileSync(`/proc/${process.pid}/stat`, "utf8") as string;
      const startTime = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19]!;
      expect(WorkerChildProcessSpawner.isSameProcess(process.pid, startTime)).toBe(true);
      // Same PID, another start time: a different process, so the one it named is gone.
      expect(WorkerChildProcessSpawner.isSameProcess(process.pid, `${startTime}1`)).toBe(false);
      expect(WorkerChildProcessSpawner.isSameProcess(2 ** 30, startTime)).toBe(false);
    }),
  );

  it.live.skipIf(!linux)(
    "starts provider processes on Node's spawner when hand-off is off or fails, with a warning",
    () => {
      const { layer: logLayer, logs } = capturedLogs();
      const parentOf = Effect.gen(function* () {
        const { spawner } = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
        return Number(yield* spawner.string(node("process.stdout.write(String(process.ppid))")));
      });
      return Effect.gen(function* () {
        const direct = yield* parentOf.pipe(
          Effect.provide(
            launcherLayer({ [WorkerChildProcessSpawner.PROVIDER_SPAWNER_ENV]: "direct" }),
          ),
        );
        expect(direct).toBe(process.pid);
        // Its socket directory gone: this hand-off fails, the start does not.
        const fallback = yield* Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const { stateDir } = yield* ServerConfig.ServerConfig;
          yield* fs.remove(NodePath.join(stateDir, "spawner"), { recursive: true });
          return yield* parentOf;
        }).pipe(Effect.provide(launcherLayer()));
        expect(fallback).toBe(process.pid);
        expect(logs.some(({ text }) => text.includes("hand-off failed"))).toBe(true);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, logLayer)));
    },
  );

  it.live.skipIf(!linux)("gives an SDK a Node-style process with its stdio, exit and close", () =>
    Effect.gen(function* () {
      const launcher = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
      const child = launcher.spawn(
        "/bin/sh",
        ["-c", 'echo ready; read -r line; echo "got $line"; exit 3'],
        { env: process.env, stdio: ["pipe", "pipe", "pipe"], held: [2] },
      );
      const output = yield* Effect.callback<{ stdout: string; code: number | null }>((resume) => {
        let stdout = "";
        child.stdout!.on("data", (chunk: Buffer) => {
          stdout += chunk.toString();
          if (stdout === "ready\n") child.stdin!.end("hello\n");
        });
        child.once("close", (code: number | null) => resume(Effect.succeed({ stdout, code })));
      });
      expect(output).toEqual({ stdout: "ready\ngot hello\n", code: 3 });
      // A held descriptor stays the server's: open, unread, not offered as a stream.
      expect({ exitCode: child.exitCode, stderr: child.stderr }).toEqual({
        exitCode: 3,
        stderr: null,
      });
      expect(spawnerProcessIds.size).toBeGreaterThan(0);
    }).pipe(Effect.provide(launcherLayer())),
  );

  it.live.skipIf(!linux)(
    "refuses a connection without the server's header: it never becomes a provider's stdio",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-handoff-" });
        const spawner = NodeChildProcess.spawn(
          process.execPath,
          ["-e", spawnerProcessSource, "65536"],
          {
            stdio: ["ignore", "pipe", "inherit", "ipc"],
            serialization: "advanced",
            detached: true,
          },
        );
        spawner.stdout!.resume();
        yield* Effect.addFinalizer(() => Effect.sync(() => spawner.disconnect()));
        const received: Array<Record<string, unknown>> = [];
        const waiters: Array<{ t: string; done: () => void }> = [];
        spawner.on("message", (message: Record<string, unknown>) => {
          received.push(message);
          const matching = waiters.filter((waiter) => waiter.t === message.t);
          waiters.splice(0, waiters.length, ...waiters.filter((waiter) => waiter.t !== message.t));
          for (const waiter of matching) waiter.done();
        });
        const next = (t: string) =>
          Effect.callback<void>((resume) => {
            if (received.some((message) => message.t === t)) return resume(Effect.void);
            waiters.push({ t, done: () => resume(Effect.void) });
          });
        yield* next("ready");
        const path = NodePath.join(directory, "provider");
        const nonce = NodeCrypto.randomBytes(16);
        spawner.send({
          t: "spawn",
          id: 1,
          command: process.execPath,
          args: ["-e", "process.stdin.pipe(process.stdout)"],
          options: { env: process.env, stdio: ["ignore", "ignore", "ignore"], detached: true },
          handoff: { path, nonce: new Uint8Array(nonce), fds: [0, 1] },
        });
        yield* next("handoff");
        const connect = (header: Buffer | undefined) =>
          Effect.callback<NodeNet.Socket>((resume) => {
            const socket = NodeNet.connect({ path }, () => {
              if (header !== undefined) socket.write(header);
              resume(Effect.succeed(socket));
            });
            socket.on("error", () => {});
          });
        const closed = (socket: NodeNet.Socket) =>
          Effect.callback<void>((resume) => {
            if (socket.closed) return resume(Effect.void);
            socket.once("close", () => resume(Effect.void));
          });
        // Another nonce, then nothing at all: one is refused at once, the other when the start completes.
        const forged = yield* connect(
          Buffer.concat([NodeCrypto.randomBytes(16), Buffer.from([0])]),
        );
        yield* next("handoff-refused");
        yield* closed(forged);
        const silent = yield* connect(undefined);
        const stdin = yield* connect(Buffer.concat([nonce, Buffer.from([0])]));
        const stdout = yield* connect(Buffer.concat([nonce, Buffer.from([1])]));
        yield* next("spawn");
        yield* closed(silent);
        const echoed = yield* Effect.callback<string>((resume) => {
          let output = "";
          stdout.on("data", (chunk: Buffer) => (output += chunk.toString()));
          stdout.on("end", () => resume(Effect.succeed(output)));
          stdin.end("only the server's own connections reach the provider");
        });
        expect(echoed).toBe("only the server's own connections reach the provider");
        // The listener is gone once the start completed.
        const late = yield* Effect.callback<string>((resume) => {
          const socket = NodeNet.connect({ path });
          socket.on("connect", () => resume(Effect.succeed("connected")));
          socket.on("error", (error: NodeJS.ErrnoException) =>
            resume(Effect.succeed(String(error.code))),
          );
        });
        expect(late).toBe("ENOENT");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

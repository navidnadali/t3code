// @effect-diagnostics nodeBuiltinImport:off - Fixtures connect raw sockets and kill processes by PID.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ServerConfig from "../config.ts";
import { answerDeadlines } from "./spawnerDeadlines.ts";
import { isPidReused, isSameProcess } from "./processIdentity.ts";
import { adoptedProcessIds, spawnerProcessIds } from "./spawnerProcesses.ts";
import { execAfterGoSource, spawnerProcessSource } from "./spawnerSources.ts";
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

// The spawner process alone, driven over its channel as the server drives it.
const rawSpawner = Effect.gen(function* () {
  const spawner = NodeChildProcess.spawn(process.execPath, ["-e", spawnerProcessSource, "65536"], {
    stdio: ["ignore", "pipe", "inherit", "ipc"],
    serialization: "advanced",
    detached: true,
  });
  spawner.stdout!.resume();
  const received: Array<{ readonly message: Record<string, unknown>; readonly handle: unknown }> =
    [];
  const waiters = new Set<() => void>();
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      spawner.disconnect();
      for (const { handle } of received) if (handle instanceof NodeNet.Socket) handle.destroy();
    }),
  );
  spawner.on("message", (message: Record<string, unknown>, handle: unknown) => {
    received.push({ message, handle });
    for (const wake of waiters) wake();
  });
  const until = <A>(pick: () => A | undefined) =>
    Effect.callback<A>((resume) => {
      const check = () => {
        const found = pick();
        if (found === undefined) return;
        waiters.delete(check);
        resume(Effect.succeed(found));
      };
      waiters.add(check);
      check();
      return Effect.sync(() => waiters.delete(check));
    });
  const next = (t: string, id?: number) =>
    until(
      () =>
        received.find(({ message }) => message.t === t && (id === undefined || message.id === id))
          ?.message,
    );
  // The stdio ends the spawner passed for a start, by descriptor, once there are `count`.
  const ends = (id: number, count: number) =>
    until(() => {
      const passed = received.filter(
        ({ message, handle }) =>
          message.t === "handoff-fd" && message.id === id && handle instanceof NodeNet.Socket,
      );
      if (passed.length < count) return undefined;
      return new Map(
        passed.map(({ message, handle }) => [message.fd as number, handle as NodeNet.Socket]),
      );
    });
  yield* next("ready");
  return { spawner, next, ends };
});

// A launcher over a spawner that refuses every hand-off: each start falls back to Node's spawner.
const refusingLauncher = Effect.gen(function* () {
  const node = yield* ChildProcessSpawner.ChildProcessSpawner;
  const source = spawnerProcessSource.replace(
    "const startHandoff = (message) => {",
    'const startHandoff = (message) => {\n  if (message) return post({ t: "handoff-failed", id: message.id, error: { name: "Error", message: "no hand-off here" } });',
  );
  expect(source).not.toBe(spawnerProcessSource);
  const { host } = yield* WorkerChildProcessSpawner.makeProcessSpawnerWithHost(node, { source });
  return yield* WorkerChildProcessSpawner.makeProviderLauncher(node, host!);
});

// A process of the server's own user that races every socket appearing under the state directory:
// it moves the real one aside, listens in its place, and relays whatever passes, counting each.
const sameUserAttacker = `
const fs = require("node:fs"), net = require("node:net"), path = require("node:path");
const dir = process.argv[1];
const seen = new Set();
const watcher = fs.watch(dir, { recursive: true }, (event, name) => {
  if (!name || path.basename(name).startsWith("x-") || seen.has(name)) return;
  const target = path.join(dir, name);
  let info; try { info = fs.lstatSync(target); } catch { return; }
  if (!info.isSocket()) return;
  seen.add(name);
  const real = path.join(path.dirname(target), "x-" + path.basename(target));
  try { fs.renameSync(target, real); } catch { return; }
  const server = net.createServer((client) => {
    client.on("error", () => {});
    process.stdout.write("captured\\n");
    const upstream = net.connect(real);
    upstream.on("error", () => {});
    client.pipe(upstream); upstream.pipe(client);
  });
  server.on("error", () => {});
  server.listen(target);
});
watcher.on("error", () => {});
process.stdout.write("ready\\n");
setInterval(() => {}, 1000);
`;

// The host alone over a transport the test plays: which start deadlines are armed as reports arrive.
describe("hand-off start deadlines", () => {
  it.each([
    { order: ["handoff-queued", "started"] },
    { order: ["started", "handoff-queued"] },
  ] as const)("keeps an active start's deadline when its reports arrive as $order", ({ order }) => {
    const armed = new Set<object>();
    let events: { readonly onMessage: (message: unknown) => void } | undefined;
    const host = WorkerChildProcessSpawner.makeRemoteHost({
      open: (opened) => {
        events = opened;
        return { post: () => {}, ref: () => {}, unref: () => {}, terminate: async () => {} };
      },
      windowBytes: 65_536,
      stoppedMessage: "stopped",
      closedMessage: "closed",
      restarts: 1,
      startDeadline: () => {
        const deadline = {};
        armed.add(deadline);
        return () => armed.delete(deadline);
      },
      handoffShim: () => ({ command: "shim", args: [] }),
    });
    const child = host.spawnHandoff(
      "provider",
      [],
      { stdio: ["ignore", "ignore", "ignore"] },
      { fds: [0, 1, 2] },
      { onSpawn: () => {}, onError: () => {}, onUnavailable: () => {} },
    );
    // Its PID report travels on the spawner's stdout, its queue report on the channel: either may
    // come first. Once the spawner has started it, the start must be answered in time.
    for (const t of order) {
      events!.onMessage(
        t === "started" ? { t, id: child.id, pid: 4242, startTime: "1" } : { t, id: child.id },
      );
    }
    expect(armed.size).toBe(1);
  });
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
    "gives a provider the same stdio through hand-off as through Node's spawner",
    () => {
      // Each descriptor's type, terminal and open flags (blocking mode included), its other open
      // descriptors, signal mask and dispositions, limits, then stdin's EOF.
      const probe = ChildProcess.make(
        "/bin/sh",
        [
          "-c",
          'for fd in 0 1 2; do echo "$fd $(stat -L -c %F /proc/$$/fd/$fd) $([ -t $fd ] && echo tty || echo notty) $(grep ^flags /proc/$$/fdinfo/$fd)"; done; ls /proc/$$/fd | tr "\\n" " "; echo; grep -E "^Sig(Blk|Ign)" /proc/$$/status; cat /proc/$$/limits; wc -c; echo stderr >&2',
        ],
        { stdin: Stream.make(new TextEncoder().encode("abc")) },
      );
      const seen = (knob: string) =>
        Effect.gen(function* () {
          const { spawner } = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
          return yield* Effect.scoped(
            Effect.gen(function* () {
              const handle = yield* spawner.spawn(probe);
              const [stdout, stderr, exitCode] = yield* Effect.all(
                [text(handle.stdout), text(handle.stderr), handle.exitCode],
                { concurrency: "unbounded" },
              );
              return { stdout, stderr, exitCode };
            }),
          );
        }).pipe(Effect.provide(launcherLayer({ T3CODE_PROVIDER_SPAWNER: knob })));
      return Effect.gen(function* () {
        const direct = yield* seen("direct");
        expect(direct.stdout).toMatch(/^0 socket notty flags:\s+0?2\n/);
        expect(yield* seen("handoff")).toEqual(direct);
      });
    },
  );

  // Exactly its environment, names a shell would drop or values it would reset included.
  it.live.skipIf(!linux)("gives a provider exactly its environment through hand-off", () => {
    const env = {
      PATH: process.env.PATH ?? "/usr/bin:/bin",
      "my-var": "1",
      "A.B": "2",
      IFS: "x",
      OPTIND: "3",
      PPID: "123",
      _: "/not/the/command",
      PWD: "/not/the/cwd",
      NODE_OPTIONS: "",
      MULTI_LINE: "one\ntwo\n",
    };
    const seen = (knob: string) =>
      Effect.gen(function* () {
        const { spawner } = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
        return JSON.parse(
          yield* spawner.string(
            ChildProcess.make(
              process.execPath,
              ["-e", "process.stdout.write(JSON.stringify(process.env))"],
              { env, extendEnv: false, cwd: NodeOS.tmpdir() },
            ),
          ),
        ) as Record<string, string>;
      }).pipe(Effect.provide(launcherLayer({ T3CODE_PROVIDER_SPAWNER: knob })));
    return Effect.gen(function* () {
      const direct = yield* seen("direct");
      expect(direct).toEqual(env);
      expect(yield* seen("handoff")).toEqual(direct);
    });
  });

  // A secret in a provider's environment is readable by its own user only (/proc/<pid>/environ),
  // never by everyone (/proc/<pid>/cmdline), at any point of a hand-off.
  it.live.skipIf(!linux)("never puts a provider's environment on any process's argv", () =>
    Effect.gen(function* () {
      const secret = `t3-secret-${NodeCrypto.randomUUID()}`;
      // Reads every process's argv until stdin closes, then says whether the secret was there.
      const watcher = NodeChildProcess.spawn(
        process.execPath,
        [
          "-e",
          `const fs = require("node:fs"); let seen = 0, scans = 0, open = true;
           process.stdin.on("end", () => (open = false)).resume();
           const scan = () => {
             for (const pid of fs.readdirSync("/proc")) {
               if (!/^\\d+$/.test(pid)) continue;
               try { if (fs.readFileSync("/proc/" + pid + "/cmdline", "latin1").includes(process.env.T3_WATCH_FOR)) seen += 1; } catch {}
             }
             scans += 1;
             if (open) setImmediate(scan); else process.stdout.write(JSON.stringify({ seen, scans }));
           };
           scan();`,
        ],
        // In its environment, not on its argv: it looks for the secret in every argv.
        { stdio: ["pipe", "pipe", "ignore"], env: { ...process.env, T3_WATCH_FOR: secret } },
      );
      yield* Effect.addFinalizer(() => Effect.sync(() => watcher.kill("SIGKILL")));
      const launcher = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
      const env = { ...process.env, T3_TEST_SECRET: secret };
      const script =
        "process.stdout.write(process.env.T3_TEST_SECRET === undefined ? 'missing' : 'present')";
      const outputs: Array<string> = [];
      for (let index = 0; index < 6; index++) {
        outputs.push(
          yield* launcher.spawner.string(
            ChildProcess.make(process.execPath, ["-e", script], { env, extendEnv: false }),
          ),
        );
        const nodeStyle = launcher.spawn(process.execPath, ["-e", script], {
          env,
          stdio: ["pipe", "pipe", "pipe"],
        });
        outputs.push(
          yield* Effect.callback<string>((resume) => {
            let output = "";
            nodeStyle.stdout!.on("data", (chunk: Buffer) => (output += chunk.toString()));
            nodeStyle.stderr!.resume();
            nodeStyle.once("close", () => resume(Effect.succeed(output)));
          }),
        );
      }
      const watched = yield* Effect.callback<{ seen: number; scans: number }>((resume) => {
        let output = "";
        watcher.stdout!.on("data", (chunk: Buffer) => (output += chunk.toString()));
        watcher.stdout!.on("end", () => resume(Effect.succeed(JSON.parse(output))));
        watcher.stdin!.end();
      });
      expect(outputs).toEqual(Array.from({ length: 12 }, () => "present"));
      expect(watched.seen).toBe(0);
      expect(watched.scans).toBeGreaterThan(0);
    }).pipe(Effect.scoped, Effect.provide(launcherLayer())),
  );

  // The provider's NODE_OPTIONS (a --require, V8 flags) applies to the provider, never to the shim
  // that becomes it.
  it.live.skipIf(!linux)("applies a provider's NODE_OPTIONS to the provider alone", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-handoff-require-" });
      const log = NodePath.join(directory, "required.log");
      const marker = NodePath.join(directory, "marker.cjs");
      yield* fs.writeFileString(
        marker,
        `require("node:fs").appendFileSync(${JSON.stringify(log)}, process.argv.slice(1).join(" ") + "\\n");`,
      );
      const { spawner } = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
      const output = yield* spawner.string(
        ChildProcess.make(
          process.execPath,
          ["-e", "process.stdout.write('provider')", "the-provider"],
          {
            env: { ...process.env, NODE_OPTIONS: `--require ${marker}` },
            extendEnv: false,
          },
        ),
      );
      expect(output).toBe("provider");
      expect(yield* fs.readFileString(log)).toBe("the-provider\n");
    }).pipe(Effect.scoped, Effect.provide(launcherLayer())),
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
        // Its spawner is gone, it is not: still running, never reported exited, still the server's.
        expect(yield* handle.isRunning).toBe(true);
        expect(adoptedProcessIds.has(pid)).toBe(true);
        expect(socket.closed).toBe(false);
        process.kill(pid, "SIGKILL");
        yield* watch.closed(socket);
        // Its status is unknown, not a signal: it was not this server's child when it ended.
        const failure = yield* Effect.flip(handle.exitCode);
        const reason = (failure.cause as Error | undefined)?.message;
        expect(reason).toContain("exited; its exit status is unknown");
        expect(reason).not.toContain("signal");
        expect(yield* handle.isRunning).toBe(false);
        expect(adoptedProcessIds.has(pid)).toBe(false);
        yield* waitFor((text) => text.includes("outlived the process spawner exited"));
      }).pipe(Effect.scoped, Effect.provide(launcherLayer().pipe(Layer.provideMerge(logLayer))));
    },
  );

  it.effect.skipIf(!linux)("tells a reused PID from the process it named", () =>
    Effect.sync(() => {
      const stat = require("node:fs").readFileSync(`/proc/${process.pid}/stat`, "utf8") as string;
      const startTime = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19]!;
      expect(isSameProcess(process.pid, startTime)).toBe(true);
      // Same PID, another start time: a different process, so the one it named is gone.
      expect(isSameProcess(process.pid, `${startTime}1`)).toBe(false);
      expect(isSameProcess(2 ** 30, startTime)).toBe(false);
      // Reused only when the PID names another process; one that names none is not.
      expect(isPidReused(process.pid, startTime)).toBe(false);
      expect(isPidReused(process.pid, `${startTime}1`)).toBe(true);
      expect(isPidReused(2 ** 30, startTime)).toBe(false);
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
        // A spawner that cannot hand off: each start fails over to Node's spawner, both kinds.
        const launcher = yield* refusingLauncher;
        const fallback = Number(
          yield* launcher.spawner.string(
            ChildProcess.make(process.execPath, [
              "-e",
              "process.stdout.write(String(process.ppid))",
            ]),
          ),
        );
        const nodeStyle = launcher.spawn(
          process.execPath,
          ["-e", "process.stdout.write(String(process.ppid))"],
          { env: process.env, stdio: ["pipe", "pipe", "pipe"] },
        );
        const nodeStyleParent = yield* Effect.callback<number>((resume) => {
          let output = "";
          nodeStyle.stdout!.on("data", (chunk: Buffer) => (output += chunk.toString()));
          nodeStyle.stderr!.resume();
          nodeStyle.once("close", () => resume(Effect.succeed(Number(output))));
        });
        expect([fallback, nodeStyleParent]).toEqual([process.pid, process.pid]);
        expect(logs.filter(({ text }) => text.includes("hand-off failed")).length).toBe(2);
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, logLayer)));
    },
  );

  // As Node's ChildProcess: output nobody reads is drained at exit, so close always follows.
  it.live.skipIf(!linux)("closes a Node-style process whose output nobody reads", () =>
    Effect.gen(function* () {
      const launcher = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
      const child = launcher.spawn(
        process.execPath,
        ["-e", "process.stdout.write('x'.repeat(1000)); process.stderr.write('unread')"],
        { env: process.env, stdio: ["pipe", "pipe", "pipe"] },
      );
      const closed = yield* Effect.callback<string>((resume) => {
        child.once("close", (code: number | null) => resume(Effect.succeed(`close ${code}`)));
      }).pipe(
        Effect.timeoutOrElse({ duration: "3 seconds", orElse: () => Effect.succeed("no close") }),
      );
      expect(closed).toBe("close 0");
    }).pipe(Effect.provide(launcherLayer())),
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

  // A Unix socket path holds 107 bytes on Linux; a state directory can be longer.
  it.live.skipIf(!linux)("hands off whatever the length of the server's state directory", () => {
    const { logs, layer: logLayer } = capturedLogs();
    return Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const base = yield* fs.makeTempDirectoryScoped({ prefix: "t3-handoff-long-" });
      const baseDir = NodePath.join(base, "a-state-directory-".repeat(6));
      const handedOff = yield* Effect.gen(function* () {
        const { stateDir } = yield* ServerConfig.ServerConfig;
        expect(
          Buffer.byteLength(NodePath.join(stateDir, "spawner", "x".repeat(16))),
        ).toBeGreaterThan(108);
        const { spawner } = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
        const ppid = Number(
          yield* spawner.string(node("process.stdout.write(String(process.ppid))")),
        );
        // While the spawner runs: the server forgets its PID once it exits.
        return spawnerProcessIds.has(ppid);
      }).pipe(
        Effect.provide(
          WorkerChildProcessSpawner.layerProviderLauncher.pipe(
            Layer.provideMerge(ServerConfig.layerTest(process.cwd(), baseDir)),
            Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
            Layer.provideMerge(logLayer),
          ),
        ),
      );
      expect({
        handedOff,
        warnings: logs.filter(({ text }) => text.includes("hand-off failed")).length,
      }).toEqual({ handedOff: true, warnings: 0 });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));
  });

  // A hand-off start the spawner reported (its early PID line) but never answered, and then the
  // spawner died: the server kills that start's group only while the PID still names it.
  it.live.skipIf(!linux)(
    "never kills the group of a process that took the PID of an unanswered hand-off start",
    () =>
      Effect.gen(function* () {
        const node = yield* ChildProcessSpawner.ChildProcessSpawner;
        // Leads its own group, as a process that took the started child's PID would.
        const stranger = NodeChildProcess.spawn("sleep", ["30"], {
          detached: true,
          stdio: "ignore",
        });
        yield* Effect.promise(() => new Promise((resolve) => stranger.once("spawn", resolve)));
        yield* Effect.addFinalizer(() => Effect.sync(() => stranger.kill("SIGKILL")));
        // The spawner reports the stranger's PID for this start, then dies before answering it.
        const reported = "if (child.pid !== undefined) started(id, child.pid";
        const answered = 'child.on("spawn", () => {\n    if (cancelled) return;';
        const source = WorkerChildProcessSpawner.spawnerProcessSource
          .replace(
            `const startTime = child.pid === undefined ? undefined : processStartTime(child.pid);\n  ${reported}`,
            `const startTime = child.pid === undefined ? undefined : processStartTime(child.pid);\n  if (child.pid !== undefined) started(id, Number(message.args.at(-1))`,
          )
          .replace(
            answered,
            // Its PID line has long reached the server when it dies, before passing any descriptor.
            'child.on("spawn", () => {\n    setTimeout(() => process.kill(process.pid, "SIGKILL"), 500);\n    return;',
          );
        expect(source.match(/message\.args\.at\(-1\)|process\.kill\(process\.pid/g)).toHaveLength(
          2,
        );
        const { host } = yield* WorkerChildProcessSpawner.makeProcessSpawnerWithHost(node, {
          source,
        });
        const outcome = yield* Effect.callback<string>((resume) => {
          host!.spawnHandoff(
            process.execPath,
            ["-e", "", String(stranger.pid)],
            { env: process.env, stdio: ["ignore", "ignore", "ignore"], detached: true },
            { fds: [0, 1, 2] },
            {
              onSpawn: () => resume(Effect.succeed("spawned")),
              onError: (error) => resume(Effect.succeed(`error: ${error.message}`)),
              onUnavailable: () => resume(Effect.succeed("unavailable")),
            },
          );
        });
        // A signal sent would end it within the second.
        const ended = yield* Effect.callback<boolean>((resume) => {
          stranger.once("exit", () => resume(Effect.succeed(true)));
        }).pipe(
          Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.succeed(false) }),
        );
        expect({ outcome, ended }).toEqual({ outcome: "unavailable", ended: false });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // No path a process of the same user could reach carries a hand-off: such a process could
  // otherwise read or write another session's provider.
  it.live.skipIf(!linux)("hands off nothing a process of the server's own user can intercept", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const { stateDir } = yield* ServerConfig.ServerConfig;
      const { spawner } = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
      // Where hand-off sockets lived; watched whether or not anything uses it now.
      yield* fs.makeDirectory(NodePath.join(stateDir, "spawner"), { recursive: true });
      const attacker = NodeChildProcess.spawn(
        process.execPath,
        ["-e", sameUserAttacker, stateDir],
        {
          stdio: ["ignore", "pipe", "ignore"],
        },
      );
      yield* Effect.addFinalizer(() => Effect.sync(() => attacker.kill("SIGKILL")));
      let captured = 0;
      yield* Effect.callback<void>((resume) => {
        attacker.stdout!.setEncoding("utf8").on("data", (chunk: string) => {
          if (chunk.includes("ready")) resume(Effect.void);
          captured += chunk.split("captured").length - 1;
        });
      });
      const echoes: Array<string> = [];
      for (let index = 0; index < 12; index++) {
        echoes.push(
          yield* spawner.string(
            node("process.stdin.pipe(process.stdout)", {
              stdin: Stream.make(new TextEncoder().encode(`secret-${index}`)),
            }),
          ),
        );
      }
      expect({ captured, echoes }).toEqual({
        captured: 0,
        echoes: Array.from({ length: 12 }, (_, index) => `secret-${index}`),
      });
    }).pipe(Effect.scoped, Effect.provide(launcherLayer())),
  );

  it.live.skipIf(!linux)(
    "closes the server's ends of a handed-off process's stdio once it has exited",
    () => {
      const openDescriptors = () =>
        require("node:fs").readdirSync("/proc/self/fd").length as number;
      return Effect.gen(function* () {
        const launcher = yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
        // The spawner process is up before counting.
        yield* launcher.spawner.exitCode(node(""));
        const before = openDescriptors();
        for (let index = 0; index < 8; index++) {
          // Exit only, its output never read; output read, stdin never written.
          yield* launcher.spawner.exitCode(node("process.stdout.write('unread')"));
          yield* launcher.spawner.string(node("process.stdout.write('read')"));
          const child = launcher.spawn(process.execPath, ["-e", "process.stdout.write('x')"], {
            env: process.env,
            stdio: ["pipe", "pipe", "pipe"],
          });
          child.stdout!.resume();
          child.stderr!.resume();
          yield* Effect.callback<void>((resume) => {
            child.once("close", () => resume(Effect.void));
          });
        }
        expect(openDescriptors()).toBe(before);
      }).pipe(Effect.provide(launcherLayer()));
    },
  );

  it.live.skipIf(!linux)(
    "passes a provider's closed stdin to the SDK as an error when it starts on Node's spawner",
    () =>
      Effect.gen(function* () {
        // This start falls back to Node's spawner.
        const launcher = yield* refusingLauncher;
        const child = launcher.spawn("/bin/sh", ["-c", "exec 0<&-; sleep 5"], {
          env: process.env,
          stdio: ["pipe", "pipe", "pipe"],
        });
        yield* Effect.addFinalizer(() => Effect.sync(() => child.kill("SIGKILL")));
        // More than a pipe holds: the write is still pending when the shell closes its stdin.
        const error = yield* Effect.callback<string>((resume) => {
          child.stdin!.on("error", (cause: NodeJS.ErrnoException) =>
            resume(Effect.succeed(cause.code ?? cause.message)),
          );
          child.stdin!.write(Buffer.alloc(4 * 1024 * 1024));
        });
        expect(error).toBe("EPIPE");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // The Linux desktop runs the server with Electron's executable, Node only with
  // ELECTRON_RUN_AS_NODE=1: the shim must start as Node too, or no provider runs.
  it.live.skipIf(!linux)("hands off from a server running on Electron's executable", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const node = yield* ChildProcessSpawner.ChildProcessSpawner;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-handoff-electron-" });
      const electron = NodePath.join(directory, "electron");
      yield* fs.writeFileString(
        electron,
        `#!/bin/sh\n[ "$ELECTRON_RUN_AS_NODE" = 1 ] || { echo "the Electron app would start" >&2; exit 9; }\nexec ${process.execPath} "$@"\n`,
      );
      yield* fs.chmod(electron, 0o755);
      const handedOff = yield* Effect.gen(function* () {
        const { host } = yield* WorkerChildProcessSpawner.makeProcessSpawnerWithHost(node);
        const launcher = yield* WorkerChildProcessSpawner.makeProviderLauncher(node, host!);
        const parent = yield* launcher.spawner.string(
          ChildProcess.make(process.execPath, ["-e", "process.stdout.write(String(process.ppid))"]),
        );
        return spawnerProcessIds.has(Number(parent));
      }).pipe(Effect.provideService(HostProcess.ExecutablePath, electron));
      expect(handedOff).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // A burst of starts (a restart resuming every session) costs a bounded number of waiting shims,
  // and a start waiting its turn is not a start the spawner failed to answer.
  it.live.skipIf(!linux)(
    "hands off a burst with a bounded number of shims at once, all in time",
    () => {
      const { logs, layer: logLayer } = capturedLogs();
      return Effect.gen(function* () {
        const node = yield* ChildProcessSpawner.ChildProcessSpawner;
        // Counts the shims alive at once (the shim's script is on its argv; the spawner's own script
        // holds it too, and names itself) until stdin closes.
        const watcher = NodeChildProcess.spawn(
          process.execPath,
          [
            "-e",
            `const fs = require("node:fs"); let peak = 0, open = true;
           process.stdin.on("end", () => (open = false)).resume();
           const scan = () => {
             let alive = 0;
             for (const pid of fs.readdirSync("/proc")) {
               if (!/^\\d+$/.test(pid) || pid === String(process.pid)) continue;
               try {
                 const cmdline = fs.readFileSync("/proc/" + pid + "/cmdline", "latin1");
                 if (cmdline.includes("process.execve") && !cmdline.includes("t3-process-spawner")) alive += 1;
               } catch {}
             }
             peak = Math.max(peak, alive);
             if (open) setImmediate(scan); else process.stdout.write(String(peak));
           };
           scan();`,
          ],
          { stdio: ["pipe", "pipe", "ignore"] },
        );
        yield* Effect.addFinalizer(() => Effect.sync(() => watcher.kill("SIGKILL")));
        const { host } = yield* WorkerChildProcessSpawner.makeProcessSpawnerWithHost(node, {
          handoffLimit: 4,
          // Far longer than one start takes, far shorter than the whole burst.
          timers: answerDeadlines(2_000),
        });
        const launcher = yield* WorkerChildProcessSpawner.makeProviderLauncher(node, host!);
        const parents = yield* Effect.forEach(
          Array.from({ length: 160 }, (_, index) => index),
          () =>
            launcher.spawner.string(
              ChildProcess.make(process.execPath, [
                "-e",
                "setTimeout(() => process.stdout.write(String(process.ppid)), 200)",
              ]),
            ),
          { concurrency: "unbounded" },
        );
        const peak = yield* Effect.callback<number>((resume) => {
          let output = "";
          watcher.stdout!.on("data", (chunk: Buffer) => (output += chunk.toString()));
          watcher.stdout!.on("end", () => resume(Effect.succeed(Number(output))));
          watcher.stdin!.end();
        });
        expect({
          handedOff: parents.filter((parent) => Number(parent) !== process.pid).length,
          failed: logs.filter(({ text }) => text.includes("hand-off failed")).length,
          // A slot frees as its shim closes descriptor 3, a moment before it becomes the command.
          withinBound: peak <= 4 + 2,
          peak,
        }).toEqual({ handedOff: 160, failed: 0, withinBound: true, peak });
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(NodeServices.layer, logLayer)));
    },
  );

  // The spawner passes each stdio end over its channel and keeps none; the command runs only once
  // the server says it holds them all, and a start cancelled before that never runs.
  it.live.skipIf(!linux)(
    "passes a hand-off's stdio over its channel and runs it only when told",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-handoff-" });
        const { spawner, next, ends } = yield* rawSpawner;
        const start = (id: number, marker: string) =>
          spawner.send({
            t: "spawn",
            id,
            command: process.execPath,
            args: [
              "-e",
              `require("node:fs").writeFileSync(${JSON.stringify(marker)}, "ran"); process.stdin.pipe(process.stdout);`,
            ],
            options: { env: process.env, stdio: ["ignore", "ignore", "ignore"], detached: true },
            handoff: {
              fds: [0, 1, 2],
              shim: { command: process.execPath, args: ["-e", execAfterGoSource] },
            },
          });
        const ranMarker = NodePath.join(directory, "ran");
        start(1, ranMarker);
        const held = yield* ends(1, 3);
        expect([...held.keys()].toSorted()).toEqual([0, 1, 2]);
        expect(yield* fs.exists(ranMarker)).toBe(false);
        spawner.send({ t: "handoff-go", id: 1 });
        yield* next("spawn");
        const echoed = yield* Effect.callback<string>((resume) => {
          let output = "";
          held.get(1)!.on("data", (chunk: Buffer) => (output += chunk.toString()));
          held.get(1)!.on("end", () => resume(Effect.succeed(output)));
          held.get(2)!.resume();
          held.get(0)!.end("only the server holds this provider's stdio");
        });
        expect(echoed).toBe("only the server holds this provider's stdio");
        // Once it has exited, its start is over in the spawner too: it says so, and forgets it.
        yield* next("exit", 1);
        const closed = yield* next("close", 1).pipe(
          Effect.as(true),
          Effect.timeoutOrElse({ duration: "2 seconds", orElse: () => Effect.succeed(false) }),
        );
        expect(closed).toBe(true);
        // Cancelled once its ends were passed, before the go: it ends without running.
        const cancelledMarker = NodePath.join(directory, "cancelled");
        start(2, cancelledMarker);
        for (const end of (yield* ends(2, 3)).values()) end.destroy();
        spawner.send({ t: "handoff-cancel", id: 2 });
        const exit = yield* next("exit", 2);
        yield* next("close", 2);
        expect({ code: exit.code, ran: yield* fs.exists(cancelledMarker) }).toEqual({
          code: 125,
          ran: false,
        });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

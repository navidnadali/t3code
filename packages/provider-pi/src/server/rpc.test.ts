// @effect-diagnostics nodeBuiltinImport:off - A real process group stands for one that took a PID.
import * as NodeChildProcess from "node:child_process";

import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as PlatformError from "effect/PlatformError";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { makePiRpcConnection } from "./rpc.ts";

// Its exit is known but carries no code: killed by a signal, or (a provider handed off by the
// server's spawner process, whose spawner then died) found by watching its PID.
it.live.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "signals no process group at teardown once pi has exited, whatever its exit status",
  () =>
    Effect.gen(function* () {
      // Leads its own group, as a process that took pi's PID might.
      const stranger = NodeChildProcess.spawn("sleep", ["30"], { detached: true, stdio: "ignore" });
      yield* Effect.promise(() => new Promise((resolve) => stranger.once("spawn", resolve)));
      yield* Effect.addFinalizer(() => Effect.sync(() => stranger.kill("SIGKILL")));
      const spawner = ChildProcessSpawner.make(() =>
        Effect.succeed(
          ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(stranger.pid!),
            exitCode: Effect.fail(
              PlatformError.systemError({
                _tag: "Unknown",
                module: "ChildProcess",
                method: "exitCode",
                description: "Process interrupted due to receipt of signal: 'SIGKILL'",
              }),
            ),
            isRunning: Effect.succeed(false),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.drain,
            stdout: Stream.empty,
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          }),
        ),
      );
      const connection = yield* makePiRpcConnection({
        command: "pi",
        args: ["--mode", "rpc"],
        cwd: undefined,
        env: {},
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
      yield* Effect.exit(connection.exited);
      yield* connection.terminate;
      assert.deepStrictEqual(
        { exitCode: stranger.exitCode, signalCode: stranger.signalCode },
        { exitCode: null, signalCode: null },
      );
    }).pipe(Effect.scoped),
);

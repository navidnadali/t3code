// @effect-diagnostics nodeBuiltinImport:off - A real process group stands for one that took a PID.
import * as NodeChildProcess from "node:child_process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import type * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as OpenCodeRuntime from "./OpenCodeRuntime.ts";
import * as OpenCodeServerLedger from "./OpenCodeServerLedger.ts";

// Leads its own group, as a process that took an exited OpenCode process's PID might. A handed-off
// OpenCode's handle signals its group only while that PID is still OpenCode's.
const strangerAndSpawner = (output: Stream.Stream<Uint8Array>, exitCode: Effect.Effect<number>) =>
  Effect.gen(function* () {
    const stranger = NodeChildProcess.spawn("sleep", ["30"], { detached: true, stdio: "ignore" });
    yield* Effect.promise(() => new Promise((resolve) => stranger.once("spawn", resolve)));
    yield* Effect.addFinalizer(() => Effect.sync(() => stranger.kill("SIGKILL")));
    const kills: Array<string | undefined> = [];
    const spawner = ChildProcessSpawner.make(() =>
      Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(stranger.pid!),
          exitCode: Effect.map(exitCode, ChildProcessSpawner.ExitCode),
          isRunning: Effect.succeed(false),
          kill: (options?: ChildProcess.KillOptions) =>
            Effect.sync(() => void kills.push(options?.killSignal)),
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: output,
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      ),
    );
    // A signal sent to the stranger's group would end it within the second.
    const strangerEnded = Effect.callback<boolean>((resume) => {
      if (stranger.exitCode !== null || stranger.signalCode !== null) {
        return resume(Effect.succeed(true));
      }
      stranger.once("exit", () => resume(Effect.succeed(true)));
    }).pipe(Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.succeed(false) }));
    const runtime = OpenCodeRuntime.layer.pipe(
      Layer.provide(OpenCodeServerLedger.layerTest),
      Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)),
      Layer.provideMerge(NodeServices.layer),
    );
    return { kills, strangerEnded, runtime };
  });

describe.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "OpenCode runtime teardown",
  () => {
    it.live("cleans up after a command through its handle, not by signalling its PID's group", () =>
      Effect.gen(function* () {
        const { kills, strangerEnded, runtime } = yield* strangerAndSpawner(
          Stream.empty,
          Effect.succeed(0),
        );
        yield* Effect.gen(function* () {
          const opencode = yield* OpenCodeRuntime.OpenCodeRuntime;
          yield* opencode.runOpenCodeCommand({ binaryPath: "opencode", args: ["--version"] });
        }).pipe(Effect.provide(runtime));
        assert.deepStrictEqual(
          { kills, strangerEnded: yield* strangerEnded },
          { kills: ["SIGKILL"], strangerEnded: false },
        );
      }).pipe(Effect.scoped),
    );

    it.live("stops its server through its handle, not by signalling its PID's group", () =>
      Effect.gen(function* () {
        const { kills, strangerEnded, runtime } = yield* strangerAndSpawner(
          Stream.encodeText(Stream.make("opencode server listening on http://127.0.0.1:4096\n")),
          Effect.never,
        );
        yield* Effect.gen(function* () {
          const opencode = yield* OpenCodeRuntime.OpenCodeRuntime;
          const scope = yield* Scope.make();
          yield* opencode
            .startOpenCodeServerProcess({
              binaryPath: "opencode",
              directory: "/",
              port: 4096,
              verify: (url) => Effect.succeed(url),
            })
            .pipe(Scope.provide(scope));
          yield* Scope.close(scope, Exit.void);
        }).pipe(Effect.provide(runtime));
        assert.deepStrictEqual(
          { kills, strangerEnded: yield* strangerEnded },
          { kills: ["SIGTERM", "SIGKILL"], strangerEnded: false },
        );
      }).pipe(Effect.scoped),
    );
  },
);

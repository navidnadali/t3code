import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as OpenCodeRuntime from "@t3tools/provider-opencode/server/OpenCodeRuntime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ServerConfig from "../config.ts";
import * as WorkerChildProcessSpawner from "../process/workerChildProcessSpawner.ts";
import { layerOpenCodeRuntime } from "./openCodeRuntimeLayer.ts";

// Records each command and answers it as `opencode --version` would.
const recordingSpawner = (spawned: Array<string>) =>
  ChildProcessSpawner.make((command) => {
    const { command: executable, args } = command as unknown as {
      readonly command: string;
      readonly args: ReadonlyArray<string>;
    };
    spawned.push([executable, ...args].join(" "));
    return Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.encodeText(Stream.make("1.2.3\n")),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    );
  });

it.effect(
  "starts OpenCode's commands from the server's provider launcher when there is one",
  () => {
    const launched: Array<string> = [];
    const fromServer: Array<string> = [];
    return Effect.gen(function* () {
      const runtime = yield* OpenCodeRuntime.OpenCodeRuntime;
      yield* runtime.runOpenCodeCommand({ binaryPath: "opencode", args: ["--version"] });
      // The ledger's own `ps` (macOS) goes the same way.
      expect(fromServer).toEqual([]);
      expect(launched).toContain("opencode --version");
    }).pipe(
      Effect.provide(
        layerOpenCodeRuntime.pipe(
          Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-opencode-launcher-" })),
          Layer.provide(
            Layer.succeed(
              WorkerChildProcessSpawner.ProviderProcessLauncher,
              WorkerChildProcessSpawner.ProviderProcessLauncher.of({
                spawn: () => {
                  throw new Error("OpenCode is driven through Effect");
                },
                spawner: recordingSpawner(launched),
              }),
            ),
          ),
          Layer.provide(
            Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, recordingSpawner(fromServer)),
          ),
          Layer.provide(NodeServices.layer),
        ),
      ),
      Effect.scoped,
    );
  },
);

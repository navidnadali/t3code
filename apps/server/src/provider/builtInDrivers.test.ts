import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, expect, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import { PROVIDER_IO_STAGE_PROVIDERS } from "@t3tools/provider-core/server/ProviderIoStageTiming";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import { GrokDriver } from "@t3tools/provider-grok/server";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as WorkerChildProcessSpawner from "../process/workerChildProcessSpawner.ts";
import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";

it("times every built-in driver's provider I/O under its own name", () => {
  for (const { driverKind } of BUILT_IN_DRIVERS) {
    assert.isTrue(PROVIDER_IO_STAGE_PROVIDERS.has(driverKind), driverKind);
  }
});

const layerTest = TestProviderHost.layer({ runBackgroundWork: false }).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(IdAllocator.layer),
  Layer.provideMerge(McpProviderSessions.layer),
  Layer.provideMerge(ProviderLatestVersions.layer),
  Layer.provideMerge(
    Layer.succeed(
      ProviderEventLoggers.ProviderEventLoggers,
      ProviderEventLoggers.NoOpProviderEventLoggers,
    ),
  ),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("The fixture makes no HTTP request")),
    ),
  ),
);

// A stub `brew` answering `--prefix` with the fixture's own prefix.
const stubBrew = (prefix: string, spawned: Array<string>) =>
  ChildProcessSpawner.make((command) => {
    const { args } = command as unknown as { readonly args: ReadonlyArray<string> };
    spawned.push(args.join(" "));
    return Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.encodeText(Stream.make(`${prefix}\n`)),
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    );
  });

// The stub `brew` cannot run as an executable on Windows.
it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "starts an ACP driver's processes from the server's provider launcher when there is one",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const prefix = yield* fs.realPath(
        yield* fs.makeTempDirectoryScoped({ prefix: "t3-launcher-grok-" }),
      );
      const binaryPath = path.join(prefix, "Caskroom", "grok-build", "1.2.3,456", "grok");
      for (const executable of [path.join(prefix, "bin", "brew"), binaryPath]) {
        yield* fs.makeDirectory(path.dirname(executable), { recursive: true });
        yield* fs.writeFileString(executable, "#!/bin/sh\n");
        yield* fs.chmod(executable, 0o755);
      }
      const launched: Array<string> = [];
      const fromServer: Array<string> = [];
      // The list's entry for Grok, as the registry creates it.
      const grok = BUILT_IN_DRIVERS.find(
        ({ driverKind }) => driverKind === GrokDriver.driverKind,
      ) as unknown as typeof GrokDriver;
      const instance = yield* grok
        .create({
          instanceId: ProviderInstanceId.make("grok-launcher"),
          displayName: "Grok test",
          enabled: false,
          environment: [],
          config: { ...grok.defaultConfig(), binaryPath },
        })
        .pipe(
          Effect.provideService(
            WorkerChildProcessSpawner.ProviderProcessLauncher,
            WorkerChildProcessSpawner.ProviderProcessLauncher.of({
              spawn: () => {
                throw new Error("ACP agents are driven through Effect");
              },
              spawner: stubBrew(prefix, launched),
            }),
          ),
          Effect.provideService(
            ChildProcessSpawner.ChildProcessSpawner,
            stubBrew(prefix, fromServer),
          ),
        );
      yield* instance.snapshot.resolveMaintenance();
      expect({ launched, fromServer }).toEqual({ launched: ["--prefix"], fromServer: [] });
    }).pipe(Effect.provide(layerTest), Effect.scoped),
);

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
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
import * as HttpClientResponse from "effect/http/HttpClientResponse";
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
      // Latest-version lookups find nothing.
      HttpClient.make((request) =>
        Effect.succeed(HttpClientResponse.fromWeb(request, new Response(null, { status: 404 }))),
      ),
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

// Which spawners a driver's checks and maintenance lookups reach (a Homebrew-installed binary's
// lookup runs `brew --prefix`; a status check runs the binary).
const spawnersUsed = (driverKind: string, binaryName: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const prefix = yield* fs.realPath(
      yield* fs.makeTempDirectoryScoped({ prefix: `t3-launcher-${driverKind}-` }),
    );
    const binaryPath = path.join(prefix, "Caskroom", binaryName, "1.2.3", binaryName);
    for (const executable of [path.join(prefix, "bin", "brew"), binaryPath]) {
      yield* fs.makeDirectory(path.dirname(executable), { recursive: true });
      yield* fs.writeFileString(executable, "#!/bin/sh\n");
      yield* fs.chmod(executable, 0o755);
    }
    const spawned = { launcher: [], commands: [], ambient: [] } satisfies Record<
      string,
      Array<string>
    >;
    // The list's entry, as the registry creates it; each of these drivers takes this environment.
    const driver = BUILT_IN_DRIVERS.find(
      (entry) => entry.driverKind === driverKind,
    ) as unknown as typeof GrokDriver;
    const instance = yield* driver
      .create({
        instanceId: ProviderInstanceId.make(`${driverKind}-launcher`),
        displayName: "Launcher test",
        enabled: true,
        environment: [],
        config: { ...driver.defaultConfig(), binaryPath },
      })
      .pipe(
        Effect.provideService(
          WorkerChildProcessSpawner.ProviderProcessLauncher,
          WorkerChildProcessSpawner.ProviderProcessLauncher.of({
            spawn: () => {
              throw new Error("These drivers start processes through Effect");
            },
            spawner: stubBrew(prefix, spawned.launcher),
          }),
        ),
        Effect.provideService(
          WorkerChildProcessSpawner.CommandSpawner,
          stubBrew(prefix, spawned.commands),
        ),
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          stubBrew(prefix, spawned.ambient),
        ),
      );
    yield* instance.snapshot.resolveMaintenance();
    yield* instance.snapshot.refresh;
    return {
      launcher: spawned.launcher.length > 0,
      commands: spawned.commands.length > 0,
      ambient: spawned.ambient.length > 0,
    };
  }).pipe(Effect.provide(layerTest), Effect.scoped);

// The stub `brew` cannot run as an executable on Windows.
describe.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "built-in drivers' processes",
  () => {
    it.effect("start from the server's provider launcher for a provider verified on hand-off", () =>
      Effect.gen(function* () {
        expect(yield* spawnersUsed("pi", "pi")).toEqual({
          launcher: true,
          commands: false,
          ambient: false,
        });
      }),
    );

    it.effect("stay on Node's spawner for Grok, which exits when its parent process dies", () =>
      Effect.gen(function* () {
        expect(yield* spawnersUsed("grok", "grok")).toEqual({
          launcher: false,
          commands: false,
          ambient: true,
        });
      }),
    );

    it.effect(
      "run Muse's checks on the server's command spawner (its SDK starts its sessions)",
      () =>
        Effect.gen(function* () {
          expect(yield* spawnersUsed("muse", "muse")).toEqual({
            launcher: false,
            commands: true,
            ambient: false,
          });
        }),
    );
  },
);

import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as HostProcess from "@t3tools/shared/HostProcess";

import { injectLinuxArm64Sea } from "./linux-arm64-sea.ts";

/** Beside `t3` in the archive; the server looks for the same name (its test pins them together). */
export const PROCESS_SPAWNER_EXECUTABLE = "t3-process-spawner";

class ProcessSpawnerSeaError extends Schema.TaggedError<ProcessSpawnerSeaError>()(
  "ProcessSpawnerSeaError",
  { message: Schema.String },
) {}

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const run = Effect.fn("ProcessSpawnerSea.run")(function* (
  command: string,
  args: Array<string>,
  verbose: boolean,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner.spawn(
    ChildProcess.make(command, args, { stdout: verbose ? "inherit" : "ignore", stderr: "inherit" }),
  );
  const exitCode = yield* child.exitCode;
  if (exitCode !== 0) {
    return yield* new ProcessSpawnerSeaError({
      message: `${command} exited with code ${exitCode}.`,
    });
  }
}, Effect.scoped);

/**
 * Builds the spawner process as its own small single executable: started as `t3 __process-spawner`,
 * a single executable parses the whole server bundle first (about 4x the memory).
 */
export const buildProcessSpawnerSea = Effect.fn("ProcessSpawnerSea.build")(function* (input: {
  /** The spawner process's script, `spawnerProcessSource` of apps/server. */
  readonly source: string;
  readonly outputPath: string;
  readonly nodeVersion: string;
  readonly verbose: boolean;
}) {
  if (process.versions.node !== input.nodeVersion) {
    return yield* new ProcessSpawnerSeaError({
      message: `The spawner executable must be built with Node ${input.nodeVersion}; running ${process.versions.node}.`,
    });
  }
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcess.Platform;
  const temp = yield* fs.makeTempDirectoryScoped({ prefix: "t3-process-spawner-sea-" });
  const mainPath = path.join(temp, "process-spawner.cjs");
  yield* fs.writeFileString(mainPath, input.source);
  // TEMPORARY one-time Node fix. Remove when fixed: https://github.com/nodejs/node/issues/67048
  if (platform === "linux" && (yield* HostProcess.Architecture) === "arm64") {
    return yield* injectLinuxArm64Sea({
      bundlePath: mainPath,
      outputPath: input.outputPath,
      nodeVersion: input.nodeVersion,
      verbose: input.verbose,
      mainFormat: "commonjs",
    }).pipe(Effect.mapError((error) => new ProcessSpawnerSeaError({ message: error.message })));
  }
  const configPath = path.join(temp, "sea.json");
  yield* fs.writeFileString(
    configPath,
    yield* encodeJson({
      main: mainPath,
      output: input.outputPath,
      mainFormat: "commonjs",
      disableExperimentalSEAWarning: true,
      useCodeCache: false,
      // The server's NODE_OPTIONS, such as its heap ceiling, are not the spawner's.
      execArgvExtension: "none",
    }),
  );
  yield* run(process.execPath, ["--build-sea", configPath], input.verbose);
  // Apple Silicon runs only signed binaries; the archive signs it again with the release identity.
  if (platform === "darwin") {
    yield* run("codesign", ["--force", "--sign", "-", input.outputPath], input.verbose);
  }
}, Effect.scoped);

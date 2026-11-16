// @effect-diagnostics nodeBuiltinImport:off - Starts the built spawner over Node IPC, as the server does.
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  buildProcessSpawnerSea,
  PROCESS_SPAWNER_EXECUTABLE,
} from "../../../scripts/lib/process-spawner-sea.ts";
import * as SpawnerSources from "../src/process/spawnerSources.ts";
import serverPackage from "../package.json" with { type: "json" };

const serverDir = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");

// The first message a spawner sends over its channel: what the server checks before using it.
const handshakeOf = (executable: string, args: ReadonlyArray<string> = []) =>
  Effect.callback<unknown>((resume) => {
    const spawner = NodeChildProcess.spawn(executable, [...args, String(64 * 1024)], {
      stdio: ["ignore", "ignore", "inherit", "ipc"],
      serialization: "advanced",
      detached: true,
    });
    spawner.once("message", (message) => {
      spawner.once("exit", () => resume(Effect.succeed(message)));
      spawner.disconnect();
    });
  });

interface SpawnerReply {
  readonly t: string;
  readonly s?: number;
  readonly chunk?: Uint8Array;
}

// Starts a spawner as the server does, runs one command on it, and reads the spawner's RSS then.
const runOnSpawner = (command: string, args: ReadonlyArray<string>) =>
  Effect.callback<{ readonly stdout: string; readonly rssKiB: number }>((resume) => {
    const spawner = NodeChildProcess.spawn(command, args, {
      stdio: ["ignore", "pipe", "inherit", "ipc"],
      serialization: "advanced",
      detached: true,
    });
    spawner.stdout?.resume();
    let stdout = "";
    spawner.on("message", (message: SpawnerReply) => {
      if (message.t === "data" && message.chunk !== undefined) {
        stdout += Buffer.from(message.chunk).toString();
        spawner.send({ t: "ack", id: 1, s: message.s, bytes: message.chunk.byteLength });
      }
      if (message.t === "close") {
        const rssKiB = Number(
          NodeChildProcess.execFileSync("ps", ["-o", "rss=", "-p", String(spawner.pid)]),
        );
        spawner.once("exit", () => resume(Effect.succeed({ stdout, rssKiB })));
        spawner.disconnect();
      }
    });
    spawner.send({
      t: "spawn",
      id: 1,
      command: process.execPath,
      args: ["-e", "process.stdout.write('ran')"],
      options: { env: process.env, stdio: ["pipe", "pipe", "pipe"], detached: true },
    });
  });

describe("process spawner single executable", () => {
  it.live.skipIf(HostProcess.Platform.defaultValue() === "win32")(
    "runs commands with about the memory of the node -e spawner",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-spawner-sea-test-" });
        // The server finds the executable the build ships by this name.
        expect(PROCESS_SPAWNER_EXECUTABLE).toBe(SpawnerSources.PROCESS_SPAWNER_EXECUTABLE);
        const executable = path.join(directory, PROCESS_SPAWNER_EXECUTABLE);
        yield* buildProcessSpawnerSea({
          source: SpawnerSources.spawnerProcessSource,
          outputPath: executable,
          nodeVersion: process.versions.node,
          verbose: false,
        });
        const windowBytes = String(64 * 1024);
        const built = yield* runOnSpawner(executable, [windowBytes]);
        const node = yield* runOnSpawner(process.execPath, [
          "-e",
          SpawnerSources.spawnerProcessSource,
          windowBytes,
        ]);
        expect(built.stdout).toBe("ran");
        expect(node.stdout).toBe("ran");
        // The server's own entry, t3 __process-spawner, measured about 4x the node -e spawner.
        expect(built.rssKiB).toBeLessThan(node.rssKiB * 1.5);
        // As a handed-off command's shim: told on descriptor 3 what to become, it becomes that.
        const became = yield* Effect.callback<string>((resume) => {
          const shim = NodeChildProcess.spawn(executable, [SpawnerSources.EXEC_AFTER_GO], {
            stdio: ["ignore", "pipe", "inherit", "pipe"],
            env: {},
          });
          let output = "";
          shim.stdout!.on("data", (chunk: Buffer) => (output += chunk.toString()));
          shim.once("close", () => resume(Effect.succeed(output)));
          (shim.stdio[3] as NodeJS.WritableStream).end(
            JSON.stringify({
              file: process.execPath,
              argv: [process.execPath, "-e", "process.stdout.write(process.env.T3_EXACT)"],
              env: { T3_EXACT: "its own environment" },
            }),
          );
        });
        expect(became).toBe("its own environment");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // A release check, run on request as youngGenerationSea.test.ts is (T3_SEA_TEST=1). A packager
  // stamps the server with APP_VERSION; the spawner beside it must announce that build too, or
  // the server refuses it and runs its spawner from the whole bundle, without provider hand-off.
  it.live.skipIf(
    HostProcess.Platform.defaultValue() === "win32" || process.env.T3_SEA_TEST !== "1",
  )("ships a spawner whose handshake the stamped server accepts", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-spawner-version-sea-" });
      const stamped = `${serverPackage.version}-perf.7`;
      const build = NodeChildProcess.spawnSync(
        process.execPath,
        ["scripts/cli.ts", "build-exe", "--out-dir", dir],
        {
          cwd: serverDir,
          env: { ...process.env, NODE_OPTIONS: undefined, APP_VERSION: stamped },
          encoding: "utf8",
          timeout: 600_000,
        },
      );
      expect(build.status, build.stderr).toBe(0);
      // `t3 __process-spawner` runs the spawner from the stamped bundle: what its server expects.
      const expected = yield* handshakeOf(path.join(dir, "t3"), ["__process-spawner"]);
      expect(expected).toEqual({
        t: "ready",
        protocol: SpawnerSources.spawnerHandshake.protocol,
        build: stamped,
      });
      expect(yield* handshakeOf(path.join(dir, PROCESS_SPAWNER_EXECUTABLE))).toEqual(expected);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

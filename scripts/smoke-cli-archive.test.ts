import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { PROCESS_SPAWNER_EXECUTABLE } from "./lib/process-spawner-sea.ts";

const collect = <E>(stream: Stream.Stream<Uint8Array, E>) =>
  stream.pipe(
    Stream.decodeText(),
    Stream.runFold(
      () => "",
      (acc, chunk) => acc + chunk,
    ),
  );

// Smokes an archive for `key` whose t3 only stands in, without t3-process-spawner, and with
// build-cli-archive's record of whether it staged one when given: past the layout, the first check
// of t3 itself fails.
const smokeArchiveWithoutSpawner = Effect.fn("test.smokeArchiveWithoutSpawner")(function* (
  key: string,
  manifest?: { readonly processSpawner: boolean } | string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-cli-smoke-test-" });
  const root = `t3-9.9.9-${key}`;
  const content = path.join(dir, "stage", root);
  yield* fs.makeDirectory(path.join(content, "client"), { recursive: true });
  yield* fs.writeFileString(path.join(content, "client/index.html"), "<!doctype html>\n");
  yield* fs.writeFileString(path.join(content, "t3"), "#!/bin/sh\nexit 1\n");
  yield* fs.chmod(path.join(content, "t3"), 0o755);
  if (manifest !== undefined) {
    yield* fs.writeFileString(
      path.join(content, "t3-archive.json"),
      typeof manifest === "string" ? manifest : JSON.stringify(manifest),
    );
  }
  if ((yield* HostProcess.Platform) === "linux") {
    yield* fs.makeDirectory(path.join(content, "lib"));
    yield* fs.writeFileString(path.join(content, "lib/libatomic.so.1"), "");
    yield* fs.writeFileString(path.join(content, "lib/libatomic.so.1.LICENSE"), "");
  }
  const archive = path.join(dir, `${root}.tar.gz`);
  const tar = yield* spawner
    .spawn(ChildProcess.make("tar", ["-czf", archive, "-C", path.join(dir, "stage"), root]))
    .pipe(Effect.flatMap((child) => child.exitCode));
  assert.equal(Number(tar), 0);

  const smoke = yield* spawner.spawn(
    ChildProcess.make(
      process.execPath,
      ["scripts/smoke-cli-archive.ts", "--archive", archive, "--expect-version", "9.9.9"],
      { cwd: path.dirname(import.meta.dirname) },
    ),
  );
  const [stdout, stderr, exitCode] = yield* Effect.all(
    [collect(smoke.stdout), collect(smoke.stderr), smoke.exitCode.pipe(Effect.map(Number))],
    { concurrency: "unbounded" },
  );
  return { output: stdout + stderr, exitCode };
}, Effect.scoped);

it.layer(NodeServices.layer)("smoke-cli-archive", (it) => {
  // build-exe and build-cli-archive put t3-process-spawner in native, non-Windows archives only,
  // and the archive records whether it has one; so a cross-built archive checked on its own target
  // does not need one, and a native one cannot lose it unnoticed.
  const posix = HostProcess.Platform.defaultValue() !== "win32";
  const keys = Effect.gen(function* () {
    const platform = yield* HostProcess.Platform;
    const arch = yield* HostProcess.Architecture;
    return [`${platform}-${arch}`, `${platform}-${arch === "x64" ? "arm64" : "x64"}`];
  });

  it.effect.runIf(posix)("requires t3-process-spawner in an archive without a record", () =>
    Effect.gen(function* () {
      for (const key of [...(yield* keys), "unnamed"]) {
        const smoke = yield* smokeArchiveWithoutSpawner(key);

        assert.notEqual(smoke.exitCode, 0, key);
        assert.include(smoke.output, `missing ${PROCESS_SPAWNER_EXECUTABLE}`, key);
      }
    }),
  );

  it.effect.runIf(posix)("requires t3-process-spawner where the archive records one", () =>
    Effect.gen(function* () {
      for (const key of yield* keys) {
        const smoke = yield* smokeArchiveWithoutSpawner(key, { processSpawner: true });

        assert.notEqual(smoke.exitCode, 0, key);
        assert.include(smoke.output, `missing ${PROCESS_SPAWNER_EXECUTABLE}`, key);
      }
    }),
  );

  it.effect.runIf(posix)(
    "does not require t3-process-spawner where the archive records none, as a cross-built one on its target",
    () =>
      Effect.gen(function* () {
        for (const key of yield* keys) {
          const smoke = yield* smokeArchiveWithoutSpawner(key, { processSpawner: false });

          assert.notEqual(smoke.exitCode, 0, key);
          assert.include(smoke.output, "CLI archive smoke test failed while", key);
          assert.notInclude(smoke.output, PROCESS_SPAWNER_EXECUTABLE, key);
        }
      }),
  );

  it.effect.runIf(posix)("fails an archive whose record does not decode", () =>
    Effect.gen(function* () {
      const [native] = yield* keys;
      const smoke = yield* smokeArchiveWithoutSpawner(native!, '{"processSpawner":"yes"}');

      assert.notEqual(smoke.exitCode, 0);
      assert.include(smoke.output, "CLI archive smoke test failed while reading t3-archive.json");
    }),
  );
});

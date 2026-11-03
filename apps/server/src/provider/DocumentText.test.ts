import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as TestClock from "effect/testing/TestClock";

import { makeProcessProbe } from "../testUtils/processProbe.ts";
import {
  commandsLeaveTheServer,
  withServerCommandSpawner,
} from "../testUtils/serverCommandSpawner.ts";
import * as DocumentText from "./DocumentText.ts";

describe("DocumentText", () => {
  it.effect.skipIf(!commandsLeaveTheServer)(
    "extracts a document's text from the server's command spawner",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-document-text-" });
        const notebook = path.join(directory, "notes.ipynb");
        yield* fs.writeFileString(
          notebook,
          JSON.stringify({
            cells: [{ cell_type: "markdown", source: ["North calibration: 946"] }],
          }),
        );
        const { result, ambientCommands, serverCommands } = yield* withServerCommandSpawner(
          Effect.gen(function* () {
            const documents = yield* DocumentText.DocumentText;
            return yield* documents.ensure(notebook);
          }).pipe(Effect.provide(DocumentText.layer)),
        );
        expect({
          sidecar: result,
          text: yield* fs.readFileString(`${notebook}.txt`),
          ambientCommands,
          serverCommands: serverCommands.length,
        }).toEqual({
          sidecar: `${notebook}.txt`,
          text: expect.stringContaining("North calibration: 946"),
          ambientCommands: [],
          serverCommands: 1,
        });
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect.skipIf(!commandsLeaveTheServer)(
    "kills an extractor that ignores SIGTERM once it times out",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const probe = yield* makeProcessProbe;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-document-text-" });
        const extractor = path.join(directory, "extractor");
        const stuck = `process.on("SIGTERM", () => {}); require("node:net").createConnection(${probe.port}, "127.0.0.1", function () { this.write(process.pid + "\\n"); }); setInterval(() => {}, 1000);`;
        yield* fs.writeFileString(
          extractor,
          `#!/bin/sh\nexec ${JSON.stringify(process.execPath)} -e ${JSON.stringify(stuck)}\n`,
        );
        yield* fs.chmod(extractor, 0o755);
        const notebook = path.join(directory, "notes.ipynb");
        yield* fs.writeFileString(notebook, JSON.stringify({ cells: [] }));
        // The spawner stays up throughout: only the timeout may end the extractor.
        const { result } = yield* withServerCommandSpawner(
          Effect.gen(function* () {
            const documents = yield* DocumentText.DocumentText;
            const extraction = yield* documents.ensure(notebook).pipe(Effect.forkChild);
            yield* probe.started;
            yield* TestClock.adjust("15 seconds");
            yield* probe.exited;
            return yield* Fiber.join(extraction);
          }).pipe(Effect.provide(DocumentText.layer)),
        ).pipe(
          Effect.provideService(HostProcess.ExecutablePath, extractor),
          Effect.provideService(HostProcess.IsExecutable, true),
        );
        expect(result).toBeUndefined();
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  // The fixture extractor is a shell script.
  describe.skipIf(!commandsLeaveTheServer)("output limit", () => {
    it.effect.each(["stdout", "stderr"] as const)(
      "keeps no text when the extractor writes past the output limit on %s",
      (stream) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-document-text-" });
          const extractor = path.join(directory, "extractor");
          const write = `process.${stream}.write("x".repeat(5 * 1024 * 1024))`;
          yield* fs.writeFileString(
            extractor,
            `#!/bin/sh\nexec ${JSON.stringify(process.execPath)} -e ${JSON.stringify(write)}\n`,
          );
          yield* fs.chmod(extractor, 0o755);
          const notebook = path.join(directory, "notes.ipynb");
          yield* fs.writeFileString(notebook, JSON.stringify({ cells: [] }));
          const sidecar = yield* Effect.gen(function* () {
            const documents = yield* DocumentText.DocumentText;
            return yield* documents.ensure(notebook);
          }).pipe(
            Effect.provide(DocumentText.layer),
            Effect.provideService(HostProcess.ExecutablePath, extractor),
            Effect.provideService(HostProcess.IsExecutable, true),
          );
          expect({ sidecar, written: yield* fs.exists(`${notebook}.txt`) }).toEqual({
            sidecar: undefined,
            written: false,
          });
        }).pipe(Effect.provide(NodeServices.layer)),
    );
  });
});

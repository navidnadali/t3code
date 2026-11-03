import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

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
});

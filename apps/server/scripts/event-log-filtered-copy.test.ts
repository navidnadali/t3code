import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import * as Sqlite from "../src/persistence/Sqlite.ts";
import { runFilteredCopyStep } from "./event-log-filtered-copy.ts";

// A stopped database built by this build's migrations, with no event history to prune yet.
const withStoppedDatabase = <A, E>(
  use: (db: string) => Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-filtered-copy-cli-" });
    const db = path.join(directory, "statev2.sqlite");
    yield* Effect.void.pipe(Effect.provide(Sqlite.layerFromPath(db)), Effect.scoped);
    return yield* use(db);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

it.effect("the operator script runs the filtered copy and the restore on a stopped database", () =>
  withStoppedDatabase((db) =>
    Effect.gen(function* () {
      // The copy's own checks run: a database with no pruning due is refused, not copied.
      const copy = yield* runFilteredCopyStep({ step: "copy", databasePath: db }).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(copy));
      assert.match(String(Exit.isFailure(copy) ? copy.cause : ""), /boundary/);
      const restore = yield* runFilteredCopyStep({ step: "restore", databasePath: db }).pipe(
        Effect.exit,
      );
      assert.isTrue(Exit.isFailure(restore));
      assert.match(String(Exit.isFailure(restore) ? restore.cause : ""), /to restore/);
    }),
  ),
);

it.effect("the operator script refuses a relative database path", () =>
  Effect.gen(function* () {
    const exit = yield* runFilteredCopyStep({
      step: "copy",
      databasePath: "statev2.sqlite",
    }).pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(exit));
  }).pipe(Effect.provide(NodeServices.layer)),
);

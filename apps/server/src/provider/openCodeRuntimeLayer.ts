import * as OpenCodeRuntime from "@t3tools/provider-opencode/server/OpenCodeRuntime";
import * as OpenCodeServerLedger from "@t3tools/provider-opencode/server/OpenCodeServerLedger";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as ServerConfig from "../config.ts";
import * as WorkerChildProcessSpawner from "../process/workerChildProcessSpawner.ts";

/**
 * The server's OpenCode runtime: its `opencode serve` servers and commands, and their ledger,
 * started from the server's provider launcher when there is one.
 */
export const layerOpenCodeRuntime = OpenCodeRuntime.layer.pipe(
  Layer.provide(
    Layer.unwrap(
      Effect.gen(function* () {
        const config = yield* ServerConfig.ServerConfig;
        return OpenCodeServerLedger.layer({ stateDir: config.stateDir });
      }),
    ),
  ),
  Layer.provide(WorkerChildProcessSpawner.layerProviderSpawner),
);

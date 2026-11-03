import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChildProcessSpawner } from "effect/process";

import * as WorkerChildProcessSpawner from "../process/workerChildProcessSpawner.ts";

/** Windows keeps Node's spawner, so commands reach the ambient spawner there. */
export const commandsLeaveTheServer = HostProcess.Platform.defaultValue() !== "win32";

/**
 * Runs `effect` with the server's CommandSpawner (the spawner process, knobs unset) and records the
 * commands that reach the ambient spawner instead.
 */
export const withServerCommandSpawner = Effect.fn("testUtils.withServerCommandSpawner")(function* <
  A,
  E,
  R,
>(effect: Effect.Effect<A, E, R>) {
  const ambient = yield* ChildProcessSpawner.ChildProcessSpawner;
  const ambientCommands: Array<string> = [];
  const recording = ChildProcessSpawner.ChildProcessSpawner.of({
    ...ambient,
    spawn: Effect.fn("testUtils.ambientSpawn")(function* (command) {
      ambientCommands.push(command._tag === "StandardCommand" ? command.command : command._tag);
      return yield* ambient.spawn(command);
    }),
  });
  const result = yield* effect.pipe(
    Effect.provide(
      WorkerChildProcessSpawner.layerCommandSpawner.pipe(
        Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
      ),
    ),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, recording),
  );
  return { result, ambientCommands };
});

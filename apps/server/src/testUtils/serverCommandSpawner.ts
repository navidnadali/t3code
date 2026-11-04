import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as WorkerChildProcessSpawner from "../process/workerChildProcessSpawner.ts";

/** Windows keeps Node's spawner, so commands reach the ambient spawner there. */
export const commandsLeaveTheServer = HostProcess.Platform.defaultValue() !== "win32";

const recording = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  commands: Array<string>,
) =>
  ChildProcessSpawner.make(
    Effect.fn("testUtils.recordedSpawn")(function* (command: ChildProcess.Command) {
      commands.push(command._tag === "StandardCommand" ? command.command : command._tag);
      return yield* spawner.spawn(command);
    }),
  );

/**
 * Runs `effect` with the server's CommandSpawner (the spawner process, knobs unset) and records the
 * commands that reach it and those that reach the ambient spawner instead.
 */
export const withServerCommandSpawner = Effect.fn("testUtils.withServerCommandSpawner")(function* <
  A,
  E,
  R,
>(effect: Effect.Effect<A, E, R>) {
  const ambient = yield* ChildProcessSpawner.ChildProcessSpawner;
  const ambientCommands: Array<string> = [];
  const serverCommands: Array<string> = [];
  const ambientRecording = recording(ambient, ambientCommands);
  return yield* Effect.gen(function* () {
    const built = yield* Layer.build(
      WorkerChildProcessSpawner.layerCommandSpawner.pipe(
        Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
      ),
    );
    const server = Context.get(built, WorkerChildProcessSpawner.CommandSpawner);
    const result = yield* effect.pipe(
      Effect.provideService(
        WorkerChildProcessSpawner.CommandSpawner,
        recording(server, serverCommands),
      ),
    );
    return { result, ambientCommands, serverCommands };
  }).pipe(
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, ambientRecording),
    Effect.scoped,
  );
});

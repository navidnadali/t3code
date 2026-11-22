import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as WorkerChildProcessSpawner from "../process/workerChildProcessSpawner.ts";
import * as Settings from "../serverSettings.ts";
import { commandsLeaveTheServer } from "../testUtils/serverCommandSpawner.ts";
import * as KeepAwake from "./KeepAwake.ts";

describe("KeepAwake.holdWhileWanted", () => {
  it.effect("keeps one hold across a gap shorter than the grace and releases after it", () =>
    Effect.gen(function* () {
      const wanted = yield* Queue.unbounded<boolean>();
      const transitions = yield* Queue.unbounded<"held" | "released">();
      const hold = Effect.acquireRelease(Queue.offer(transitions, "held"), () =>
        Queue.offer(transitions, "released"),
      ).pipe(Effect.andThen(Effect.never), Effect.scoped);
      yield* KeepAwake.holdWhileWanted(Stream.fromQueue(wanted), hold, "60 seconds").pipe(
        Effect.forkScoped,
      );

      yield* Queue.offer(wanted, true);
      expect(yield* Queue.take(transitions)).toBe("held");

      yield* Queue.offer(wanted, false);
      yield* TestClock.adjust("30 seconds");
      yield* Queue.offer(wanted, true);
      yield* TestClock.adjust("120 seconds");
      expect(yield* Queue.size(transitions)).toBe(0);

      yield* Queue.offer(wanted, false);
      yield* TestClock.adjust("120 seconds");
      expect(yield* Queue.take(transitions)).toBe("released");
      expect(yield* Queue.size(transitions)).toBe(0);
    }),
  );
});

const neverExiting = ChildProcessSpawner.makeHandle({
  pid: ChildProcessSpawner.ProcessId(1),
  exitCode: Effect.never,
  isRunning: Effect.succeed(true),
  kill: () => Effect.void,
  unref: Effect.succeed(Effect.void),
  stdin: Sink.drain,
  stdout: Stream.empty,
  stderr: Stream.empty,
  all: Stream.empty,
  getInputFd: () => Sink.drain,
  getOutputFd: () => Stream.empty,
});

const layerActiveRun = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE orchestration_v2_projection_runs (status TEXT NOT NULL)`;
    yield* sql`INSERT INTO orchestration_v2_projection_runs (status) VALUES ('running')`;
  }),
).pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })));

const layerKeepAwake = KeepAwake.layer.pipe(
  Layer.provide(layerActiveRun),
  Layer.provide(
    Layer.mock(OrchestrationEventStore.OrchestrationEventStore)({
      latestApplicationSequence: Effect.succeed(0),
      streamApplicationEvents: () => Stream.never,
    }),
  ),
  Layer.provide(Settings.ServerSettingsService.layerTest()),
);

describe("KeepAwake.layer", () => {
  // The inhibitor starts while runs start, so it must not fork the server.
  it.effect("starts the sleep inhibitor from the server's command spawner", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<"ambient" | "server">();
      const holding = (route: "ambient" | "server") =>
        ChildProcessSpawner.make(() =>
          Deferred.succeed(started, route).pipe(Effect.as(neverExiting)),
        );
      yield* Layer.build(
        layerKeepAwake.pipe(
          Layer.provide(Layer.succeed(WorkerChildProcessSpawner.CommandSpawner, holding("server"))),
          Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, holding("ambient"))),
        ),
      );
      expect(yield* Deferred.await(started)).toBe("server");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // The spawner process starts only commands whose stdio are all piped; others fall back.
  it.effect.skipIf(!commandsLeaveTheServer)(
    "starts the sleep inhibitor in the spawner process, not by its fallback",
    () =>
      Effect.gen(function* () {
        const route = yield* Deferred.make<"ambient" | "spawner process">();
        const ambient = ChildProcessSpawner.make(() =>
          Deferred.succeed(route, "ambient").pipe(Effect.as(neverExiting)),
        );
        const built = yield* Layer.build(
          WorkerChildProcessSpawner.layerCommandSpawner.pipe(
            Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
          ),
        ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, ambient));
        const server = Context.get(built, WorkerChildProcessSpawner.CommandSpawner);
        // Settles after any fallback, so a command that fell back has already said so.
        const spawnerProcess = ChildProcessSpawner.make((command) =>
          server.spawn(command).pipe(Effect.ensuring(Deferred.succeed(route, "spawner process"))),
        );
        yield* Layer.build(
          layerKeepAwake.pipe(
            Layer.provide(Layer.succeed(WorkerChildProcessSpawner.CommandSpawner, spawnerProcess)),
            Layer.provide(Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, ambient)),
          ),
        );
        expect(yield* Deferred.await(route)).toBe("spawner process");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

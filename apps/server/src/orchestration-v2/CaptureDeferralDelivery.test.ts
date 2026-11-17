import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  MessageId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";

const layerTest = Layer.fresh(
  EffectOutbox.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
);

const captureHold = Duration.seconds(7);

const capture = (id: string, threadId: ThreadId) => ({
  id,
  commandId: CommandId.make(`command:${id}`),
  threadId,
  request: {
    type: "checkpoint.capture" as const,
    runId: RunId.make(`run:${id}`),
    scopeId: CheckpointScopeId.make(`scope:${id}`),
  },
});

it.effect(
  "delivers a steer into a top-level thread without waiting out its capture's deferral",
  () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const sql = yield* SqlClient.SqlClient;
      const orchestrator = ThreadId.make("thread:deferral-delivery:orchestrator");
      const childOf = (index: number) => ThreadId.make(`thread:deferral-delivery:child-${index}`);
      const now = DateTime.formatIso(yield* DateTime.now);
      const insertThread = (threadId: ThreadId, subagent: boolean) =>
        sql`INSERT INTO orchestration_v2_projection_threads (
              thread_id, project_id, title, default_provider, provider_instance_id, runtime_mode,
              interaction_mode, created_at, updated_at, payload_json
            ) VALUES (${threadId}, 'project', 'title', 'codex', 'codex', 'full-access', 'default',
              ${now}, ${now}, ${JSON.stringify({
                lineage: subagent
                  ? { parentThreadId: "thread:parent", relationshipToParent: "subagent" }
                  : { parentThreadId: null, relationshipToParent: null },
              })})`;
      yield* insertThread(orchestrator, false);
      for (let index = 0; index < 30; index++) yield* insertThread(childOf(index), true);

      const steerClaimedAt = yield* Deferred.make<number>();
      const layerExecutor = Layer.succeed(
        EffectWorker.OrchestrationEffectExecutorV2,
        EffectWorker.OrchestrationEffectExecutorV2.of({
          execute: (effect) =>
            effect.request.type === "provider-turn.steer"
              ? Clock.currentTimeMillis.pipe(
                  Effect.flatMap((at) => Deferred.succeed(steerClaimedAt, at)),
                  Effect.asVoid,
                )
              : Effect.sleep(captureHold),
        }),
      );
      const layerWorker = EffectWorker.layerWithOptions({ workerId: "deferral-delivery" }).pipe(
        Layer.provide(
          Layer.merge(Layer.succeed(EffectOutbox.EffectOutboxV2, outbox), layerExecutor),
        ),
      );
      const enqueue = (effects: Parameters<typeof outbox.enqueue>[0]) =>
        outbox.enqueue(effects).pipe(Effect.andThen(outbox.notifyAvailable(effects.length)));

      yield* Effect.gen(function* () {
        // The loop stays saturated, so two samples close the gate to two gated effects.
        yield* EffectWorker.runDaemonWithOptions({
          concurrency: 4,
          admission: { delayAboveMs: 40 },
          pressure: Effect.succeed(Effect.succeed(60)),
        }).pipe(Effect.forkScoped);
        // Delegated children's captures keep both gated slots busy and arrive faster than
        // they drain, as on a busy orchestration host.
        let child = 0;
        const childCapture = () => {
          const index = child++;
          return capture(`effect:deferral-delivery:child-${index}`, childOf(index));
        };
        yield* enqueue([childCapture(), childCapture(), childCapture(), childCapture()]);
        yield* Effect.replicateEffect(TestClock.adjust("1 second"), 3, { discard: true });
        // The orchestrator's turn ends: its capture is queued while it idles in `waiting`.
        yield* enqueue([capture("effect:deferral-delivery:orchestrator", orchestrator)]);
        for (let second = 3; second < 33; second++) {
          if (second % 3 === 0) yield* enqueue([childCapture()]);
          yield* TestClock.adjust("1 second");
        }

        // An automation is steered into the orchestrator's live turn: the steer is held behind
        // the orchestrator's own capture (lane rule), which base claimed long ago.
        const steerEnqueuedAt = yield* Clock.currentTimeMillis;
        yield* enqueue([
          {
            id: "effect:deferral-delivery:steer",
            commandId: CommandId.make("command:deferral-delivery:steer"),
            threadId: orchestrator,
            request: {
              type: "provider-turn.steer" as const,
              providerSessionId: ProviderSessionId.make("session:deferral-delivery"),
              providerThreadId: ProviderThreadId.make("provider-thread:deferral-delivery"),
              providerTurnId: ProviderTurnId.make("turn:deferral-delivery"),
              messageId: MessageId.make("message:deferral-delivery:automation"),
            },
          },
        ]);
        yield* Effect.replicateEffect(TestClock.adjust("1 second"), 60, { discard: true });

        assert.isTrue(yield* Deferred.isDone(steerClaimedAt), "the steer never ran");
        const waitedMs = (yield* Deferred.await(steerClaimedAt)) - steerEnqueuedAt;
        assert.isAtMost(
          waitedMs,
          1_000,
          `the steer waited ${waitedMs / 1000} s for the orchestrator's deferred capture`,
        );
      }).pipe(Effect.provide(layerWorker), Effect.scoped);
    }).pipe(Effect.provide(layerTest)),
);

import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EffectAdmission from "./EffectAdmission.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";

const layerTest = Layer.fresh(
  EffectOutbox.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
);

const turnStart = (id: string, thread: string) => ({
  id,
  commandId: CommandId.make(`command:${id}`),
  threadId: ThreadId.make(thread),
  request: { type: "provider-turn.start" as const, runId: RunId.make(`run:${id}`) },
});
const interrupt = (id: string, thread: string) => ({
  id,
  commandId: CommandId.make(`command:${id}`),
  threadId: ThreadId.make(thread),
  request: {
    type: "provider-turn.interrupt" as const,
    providerSessionId: ProviderSessionId.make("ps:probe"),
    providerThreadId: ProviderThreadId.make("pt:probe"),
    providerTurnId: ProviderTurnId.make("turn:probe"),
  },
});

const idOf = (claim: Option.Option<EffectOutbox.OrchestrationEffectV2>) =>
  Option.match(claim, { onNone: () => null, onSome: (effect) => effect.id });

it.layer(layerTest)("claim-time exclusion on the real outbox", (it) => {
  it.effect("wakes every worker of the largest pool with one call", () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const workers = EffectWorker.EFFECT_WORKER_MAX_CONCURRENCY;
      const woken = yield* Ref.make(0);
      // The whole pool waits for work; a reopened gate then wakes it with one notification.
      const takers = yield* Effect.forkChild(
        Effect.all(
          Array.from({ length: workers }, () =>
            outbox.awaitAvailable.pipe(Effect.andThen(Ref.update(woken, (count) => count + 1))),
          ),
          { concurrency: "unbounded", discard: true },
        ),
      );
      yield* Effect.replicateEffect(Effect.yieldNow, 4, { discard: true });
      yield* outbox.notifyAvailable(workers);
      yield* Effect.replicateEffect(Effect.yieldNow, 8, { discard: true });
      assert.equal(yield* Ref.get(woken), workers);
      yield* Fiber.join(takers);
    }),
  );

  it.effect("skips gated effects, keeps thread order, and leaves held rows claimable", () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const gated = EffectAdmission.GATED_EFFECT_TYPES;
      yield* outbox.enqueue([
        turnStart("effect:a-start", "thread:a"),
        interrupt("effect:a-interrupt", "thread:a"),
        interrupt("effect:b-interrupt", "thread:b"),
        turnStart("effect:c-start", "thread:c"),
      ]);

      // The newer interrupt of thread A stays behind A's held turn start; B's interrupt is free.
      assert.equal(
        idOf(yield* outbox.claimNext({ workerId: "w1", excludeEffectTypes: gated })),
        "effect:b-interrupt",
      );
      assert.isNull(idOf(yield* outbox.claimNext({ workerId: "w2", excludeEffectTypes: gated })));
      assert.isTrue(Option.isNone(yield* outbox.nextClaimableAtExcluding(gated)));
      assert.isTrue(Option.isSome(yield* outbox.nextClaimableAt));

      // Cancelling a held start (interrupt before the turn started) settles it without a claim.
      const cancelled = yield* outbox.cancelUnsettled({
        threadId: ThreadId.make("thread:c"),
        effectTypes: ["provider-turn.start", "provider-turn.restart"],
        reason: "probe",
      });
      assert.deepEqual(cancelled, ["effect:c-start"]);

      // A slot holder takes A's start once, then A's interrupt only after the start settles.
      assert.equal(idOf(yield* outbox.claimNext({ workerId: "w3" })), "effect:a-start");
      assert.isNull(idOf(yield* outbox.claimNext({ workerId: "w4" })));
      assert.isTrue(yield* outbox.succeed({ effectId: "effect:a-start", workerId: "w3" }));
      assert.equal(
        idOf(yield* outbox.claimNext({ workerId: "w4", excludeEffectTypes: gated })),
        "effect:a-interrupt",
      );
      assert.isNull(idOf(yield* outbox.claimNext({ workerId: "w5" })));
    }),
  );
});

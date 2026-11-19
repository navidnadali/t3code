/**
 * A user's turn start on a root thread must not wait behind every earlier gated effect of every
 * thread, as it did behind a deep backlog of pending turn starts and captures.
 * These rows replay such a queue shape on the real outbox: four gated slots, TestClock holds.
 */
import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  ProviderSessionId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as TestClock from "effect/testing/TestClock";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EffectAdmission from "./EffectAdmission.ts";
import * as EffectOutbox from "./EffectOutbox.ts";

const layerTest = () =>
  Layer.fresh(EffectOutbox.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)));

const SLOTS = 4;
// ~45 s per turn start and ~6 s per capture: about five starts a minute on four slots.
const HOLD_MS = { "provider-turn.start": 45_000, "checkpoint.capture": 6_000 } as const;

let sequence = 0;
const nextId = (name: string) => `effect:${String(sequence++).padStart(8, "0")}:${name}`;
const capture = (thread: string): EffectOutbox.PendingOrchestrationEffectV2 => ({
  id: nextId(`${thread}:capture`),
  commandId: CommandId.make(`command:${thread}:capture`),
  threadId: ThreadId.make(thread),
  request: {
    type: "checkpoint.capture",
    runId: RunId.make(`run:${thread}:previous`),
    scopeId: CheckpointScopeId.make(`scope:${thread}`),
  },
});
const turnStart = (thread: string, user = false): EffectOutbox.PendingOrchestrationEffectV2 => ({
  id: nextId(`${thread}:start`),
  commandId: CommandId.make(`command:${thread}:start`),
  threadId: ThreadId.make(thread),
  // What the orchestrator marks for a person's message on a root thread.
  request: {
    type: "provider-turn.start",
    runId: RunId.make(`run:${thread}`),
    ...(user ? { claimPriority: "user" } : {}),
  } as EffectOutbox.OrchestrationEffectRequestV2,
});

/** `depth` pending gated effects across threads, turn starts and captures at 200:160. */
const backlog = (depth: number, prefix: string) => {
  const effects: Array<EffectOutbox.PendingOrchestrationEffectV2> = [];
  for (let thread = 0; effects.length < depth; thread += 1) {
    const name = `thread:${prefix}:${thread}`;
    if (Math.floor((thread * 160) / 200) !== Math.floor(((thread + 1) * 160) / 200))
      effects.push(capture(name));
    if (effects.length < depth) effects.push(turnStart(name));
  }
  return effects;
};

interface Claimed {
  readonly at: number;
  readonly effect: EffectOutbox.OrchestrationEffectV2;
}

/**
 * Four gated slots on the outbox: each claim holds its slot for its type's hold, then succeeds.
 * `arrivals` enqueue at their times. Runs until `until` returns true or `limitMs` passes.
 */
const runSlots = Effect.fnUntraced(function* (input: {
  readonly arrivals: ReadonlyArray<{
    readonly at: number;
    readonly effects: ReadonlyArray<EffectOutbox.PendingOrchestrationEffectV2>;
  }>;
  readonly limitMs: number;
  readonly until?: (claims: ReadonlyArray<Claimed>) => boolean;
}) {
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const slots: Array<{ freeAt: number; effectId: string | null }> = Array.from(
    { length: SLOTS },
    () => ({ freeAt: 0, effectId: null }),
  );
  const pending = [...input.arrivals].sort((left, right) => left.at - right.at);
  const claims: Array<Claimed> = [];
  let now = 0;
  let inFlightMax = 0;
  while (now <= input.limitMs && !(input.until?.(claims) ?? false)) {
    while (pending.length > 0 && pending[0]!.at <= now)
      yield* outbox.enqueue(pending.shift()!.effects);
    for (const [index, slot] of slots.entries()) {
      if (slot.freeAt > now) continue;
      if (slot.effectId !== null) {
        assert.isTrue(
          yield* outbox.succeed({ effectId: slot.effectId, workerId: `slot:${index}` }),
        );
        slot.effectId = null;
      }
      const claimed = yield* outbox.claimNext({ workerId: `slot:${index}` });
      if (Option.isNone(claimed)) continue;
      const hold = HOLD_MS[claimed.value.request.type as keyof typeof HOLD_MS];
      slot.effectId = claimed.value.id;
      slot.freeAt = now + hold;
      claims.push({ at: now, effect: claimed.value });
    }
    const inFlight = slots.filter((slot) => slot.effectId !== null).length;
    inFlightMax = Math.max(inFlightMax, inFlight);
    // Work-conserving: a slot is left idle only when nothing is claimable.
    if (inFlight < SLOTS) {
      const nextAt = yield* outbox.nextClaimableAt();
      const clock = DateTime.toEpochMillis(yield* DateTime.now);
      assert.isFalse(
        Option.isSome(nextAt) && DateTime.toEpochMillis(nextAt.value) <= clock,
        `a slot idles at ${now} ms while work is claimable`,
      );
    }
    // A full gate leaves gated work in the outbox, whatever its priority.
    if (inFlight === SLOTS)
      assert.isTrue(
        Option.isNone(
          yield* outbox.claimNext({
            workerId: "probe",
            excludeEffectTypes: EffectAdmission.GATED_EFFECT_TYPES,
          }),
        ),
      );
    const next = Math.min(
      ...slots.map((slot) => (slot.effectId === null ? Number.POSITIVE_INFINITY : slot.freeAt)),
      pending[0]?.at ?? Number.POSITIVE_INFINITY,
    );
    if (!Number.isFinite(next)) break;
    yield* TestClock.adjust(Duration.millis(next - now));
    now = next;
  }
  assert.isAtMost(inFlightMax, SLOTS);
  return claims;
});

/** Slot time the claims occupy within the first `windowMs`. */
const slotTime = (claims: ReadonlyArray<Claimed>, windowMs: number) =>
  claims.reduce(
    (total, { at, effect }) =>
      total +
      Math.max(
        0,
        Math.min(at + HOLD_MS[effect.request.type as keyof typeof HOLD_MS], windowMs) - at,
      ),
    0,
  );

/** Each thread's claims come in the order its effects were enqueued. */
const assertThreadOrder = (claims: ReadonlyArray<Claimed>) => {
  const byThread = new Map<string, Array<string>>();
  for (const { effect } of claims)
    byThread.set(effect.threadId, [...(byThread.get(effect.threadId) ?? []), effect.id]);
  for (const ids of byThread.values()) assert.deepEqual(ids, ids.toSorted());
};

// A person's message on root thread U, with U's capture of its previous run ahead of the start,
// arrives just after the slots fill: U's capture takes the next slot to free and its start the slot
// the capture frees (one slot turnover), however deep the backlog behind which it arrives.
it.effect.each([10, 200, 1_000])(
  "claims a user's capture and turn start within one slot turnover behind %i gated effects",
  (depth) =>
    Effect.gen(function* () {
      const userAt = 1;
      const userCapture = capture("thread:user");
      const userStart = turnStart("thread:user", true);
      const claims = yield* runSlots({
        arrivals: [
          { at: 0, effects: backlog(depth, `backlog-${depth}`) },
          { at: userAt, effects: [userCapture, userStart] },
        ],
        limitMs: 2 * 60 * 60_000,
        until: (claimed) => claimed.some(({ effect }) => effect.id === userStart.id),
      });
      const nextFree = Math.min(
        ...claims
          .filter(({ at }) => at < userAt)
          .map(({ at, effect }) => at + HOLD_MS[effect.request.type as keyof typeof HOLD_MS]),
      );
      const claimedAt = (id: string) =>
        claims.find(({ effect }) => effect.id === id)?.at ?? Number.POSITIVE_INFINITY;
      assert.isAtMost(claimedAt(userCapture.id), nextFree);
      assert.isAtMost(claimedAt(userStart.id), nextFree + HOLD_MS["checkpoint.capture"]);
      assertThreadOrder(claims);
    }).pipe(Effect.provide(layerTest())),
);

// User starts at 1/min for an hour on top of a backlog: prioritising reorders claims but loses no
// capacity, so the slots are as busy as under FIFO (the same starts unmarked), within 1% of slot
// time, and runSlots checks no slot idles while work is claimable. At depth 1,000 the backlog
// outlasts the hour; claim counts differ there (longer starts go first) and are not the bound.
it.effect.each([200, 1_000])(
  "keeps every slot as busy as FIFO while users jump a %i-deep queue",
  (depth) =>
    Effect.gen(function* () {
      const windowMs = 60 * 60_000;
      const run = (marked: boolean) =>
        runSlots({
          arrivals: [
            { at: 0, effects: backlog(depth, `share-${depth}-${marked}`) },
            ...Array.from({ length: 60 }, (_, minute) => ({
              at: minute * 60_000,
              effects: [turnStart(`thread:share-${depth}-${marked}:user:${minute}`, marked)],
            })),
          ],
          limitMs: windowMs,
        }).pipe(Effect.provide(layerTest()));
      const fifo = slotTime(yield* run(false), windowMs);
      const priority = yield* run(true);
      assert.isAtMost(Math.abs(slotTime(priority, windowMs) - fifo), 0.01 * fifo);
      assertThreadOrder(priority);
    }),
);

// A flood of user starts (10/min, twice what four slots start) on top of automation. Once aged
// (available 5 min), automation gets at least one claim per priority unit (every 2nd claim: these
// units are single starts), so its extra wait over FIFO (the same starts unmarked) is at most its
// own FIFO wait plus 5 min, and the turnover until a slot frees after it ages, however many wait.
it.effect.each([12, 30, 120])(
  "keeps %i automation starts moving under a flood of user turn starts",
  (automation) =>
    Effect.gen(function* () {
      const agedMs = Duration.toMillis(Duration.minutes(5));
      const run = (marked: boolean) => {
        const arrivals = [
          {
            at: 0,
            effects: Array.from({ length: SLOTS }, (_, index) =>
              turnStart(`thread:flood-${automation}-${marked}:user:first-${index}`, marked),
            ),
          },
          {
            at: 1,
            effects: Array.from({ length: automation }, (_, index) =>
              turnStart(`thread:flood-${automation}-${marked}:automation:${index}`),
            ),
          },
          ...Array.from({ length: 180 }, (_, index) => ({
            at: 2 + index * 6_000,
            effects: [turnStart(`thread:flood-${automation}-${marked}:user:${index}`, marked)],
          })),
        ];
        return runSlots({ arrivals, limitMs: 3 * 60 * 60_000 }).pipe(
          Effect.map((claims) => ({ arrivals, claims })),
          Effect.provide(layerTest()),
        );
      };
      const isAutomation = (id: string) => id.includes(":automation:");
      const waits = ({
        arrivals,
        claims,
      }: Effect.Success<ReturnType<typeof run>>): ReadonlyMap<string, number> => {
        const enqueuedAt = new Map(
          arrivals.flatMap(({ at, effects }) => effects.map((effect) => [effect.threadId, at])),
        );
        return new Map(
          claims
            .filter(({ effect }) => isAutomation(effect.threadId))
            .map(({ at, effect }) => [effect.threadId, at - enqueuedAt.get(effect.threadId)!]),
        );
      };
      const fifoWaits = waits(yield* run(false));
      const priority = yield* run(true);
      const priorityWaits = waits(priority);
      assert.equal(priorityWaits.size, automation);
      for (const [thread, wait] of priorityWaits) {
        const fifoWait = fifoWaits.get(thread.replace("-true:", "-false:"))!;
        assert.isAtMost(wait, 2 * fifoWait + agedMs + HOLD_MS["provider-turn.start"], thread);
      }
      // While aged automation waits, no two claims in a row are priority claims.
      const enqueuedAt = 1;
      let previousWasPriority = false;
      let claimedAutomation = 0;
      for (const { at, effect } of priority.claims) {
        const aged = at - enqueuedAt >= agedMs && claimedAutomation < automation;
        const isPriority = !isAutomation(effect.threadId);
        if (aged) assert.isFalse(previousWasPriority && isPriority, `two priority claims at ${at}`);
        previousWasPriority = aged && isPriority;
        if (!isPriority) claimedAutomation += 1;
      }
    }),
);

// The same flood with each user message a capture and its marked start: a two-claim unit. Once
// aged, automation receives at least every 2nd claim; a unit's claims stay together and are repaid
// after it. So while aged automation waits, priority claims never lead its claims by more than one
// unit, and its extra wait over FIFO stays within its own FIFO wait plus 5 min and a turnover.
it.effect.each([12, 30, 120])(
  "keeps %i automation starts moving under a flood of user capture-and-start units",
  (automation) =>
    Effect.gen(function* () {
      const agedMs = Duration.toMillis(Duration.minutes(5));
      const unitClaims = 2;
      const run = (marked: boolean) => {
        const name = (index: number | string) =>
          `thread:units-${automation}-${marked}:user:${index}`;
        const arrivals = [
          {
            at: 0,
            effects: Array.from({ length: SLOTS }, (_, index) =>
              turnStart(name(`first-${index}`), marked),
            ),
          },
          {
            at: 1,
            effects: Array.from({ length: automation }, (_, index) =>
              turnStart(`thread:units-${automation}-${marked}:automation:${index}`),
            ),
          },
          ...Array.from({ length: 180 }, (_, index) => ({
            at: 2 + index * 6_000,
            effects: [capture(name(index)), turnStart(name(index), marked)],
          })),
        ];
        return runSlots({ arrivals, limitMs: 3 * 60 * 60_000 }).pipe(
          Effect.map((claims) => ({ arrivals, claims })),
          Effect.provide(layerTest()),
        );
      };
      const isAutomation = (id: string) => id.includes(":automation:");
      const automationWaits = ({ claims }: Effect.Success<ReturnType<typeof run>>) =>
        new Map(
          claims
            .filter(({ effect }) => isAutomation(effect.threadId))
            .map(({ at, effect }) => [effect.threadId.replace(/-(true|false):/, ":"), at - 1]),
        );
      const fifoWaits = automationWaits(yield* run(false));
      const priority = yield* run(true);
      const priorityWaits = automationWaits(priority);
      assert.equal(priorityWaits.size, automation);
      for (const [thread, wait] of priorityWaits) {
        const fifoWait = fifoWaits.get(thread)!;
        assert.isAtMost(wait, 2 * fifoWait + agedMs + HOLD_MS["provider-turn.start"], thread);
      }
      // While aged automation waits, priority claims lead automation's by at most one unit.
      let lead = 0;
      let claimedAutomation = 0;
      for (const { at, effect } of priority.claims) {
        const aged = at - 1 >= agedMs && claimedAutomation < automation;
        if (isAutomation(effect.threadId)) {
          claimedAutomation += 1;
          lead = Math.max(0, lead - 1);
        } else if (aged) {
          lead += 1;
          assert.isAtMost(lead, unitClaims, `priority claims lead at ${at}`);
        }
      }
    }),
);

// The claim guarantee whatever the holds: automation of captures (6 s) then starts (45 s) under a
// flood of user units still gets every 2nd claim once aged. The time bound needs the claims ahead
// of an effect to hold as long as the priority claims they alternate with; a short-
// capture prefix does not, which slot-time repayment would cover.
it.effect("keeps the claim alternation under mixed automation holds", () =>
  Effect.gen(function* () {
    const agedMs = Duration.toMillis(Duration.minutes(5));
    const name = (index: number | string) => `thread:mixed:user:${index}`;
    const automation = Array.from({ length: 30 }, (_, index) => `thread:mixed:automation:${index}`);
    const claims = yield* runSlots({
      arrivals: [
        {
          at: 0,
          effects: Array.from({ length: SLOTS }, (_, index) =>
            turnStart(name(`first-${index}`), true),
          ),
        },
        {
          at: 1,
          effects: [
            ...automation.map((thread) => capture(thread)),
            ...automation.map((thread) => turnStart(thread)),
          ],
        },
        ...Array.from({ length: 180 }, (_, index) => ({
          at: 2 + index * 6_000,
          effects: [capture(name(index)), turnStart(name(index), true)],
        })),
      ],
      limitMs: 3 * 60 * 60_000,
    }).pipe(Effect.provide(layerTest()));
    const isAutomation = (thread: string) => thread.includes(":automation:");
    let lead = 0;
    let claimedAutomation = 0;
    for (const { at, effect } of claims) {
      const aged = at - 1 >= agedMs && claimedAutomation < automation.length * 2;
      if (isAutomation(effect.threadId)) {
        claimedAutomation += 1;
        lead = Math.max(0, lead - 1);
      } else if (aged) {
        lead += 1;
        assert.isAtMost(lead, 2, `priority claims lead at ${at}`);
      }
    }
    assert.equal(claimedAutomation, automation.length * 2);
    assertThreadOrder(claims);
  }),
);

// Workers claim concurrently: each claim still sees the kind of the one before it, so with aged
// automation waiting, three concurrent claims cannot all be priority claims.
it.effect("alternates with aged automation across concurrent claims", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue([turnStart("thread:concurrent:a1"), turnStart("thread:concurrent:a2")]);
    yield* TestClock.adjust(Duration.minutes(6));
    yield* outbox.enqueue(
      [1, 2, 3].map((index) => turnStart(`thread:concurrent:user:${index}`, true)),
    );
    // Each statement yields first, as the production writer's round trip does, so the claims'
    // fibers interleave between building a claim and running it.
    const claims = yield* Effect.forEach(
      [1, 2, 3],
      (index) => outbox.claimNext({ workerId: `concurrent:${index}` }),
      { concurrency: "unbounded" },
    ).pipe(
      Effect.provideService(Statement.CurrentTransformer, (statement) =>
        Effect.replicateEffect(Effect.yieldNow, 3, { discard: true }).pipe(Effect.as(statement)),
      ),
    );
    const threads = claims.map((claim) => Option.getOrUndefined(claim)?.threadId ?? "none");
    assert.lengthOf(
      threads.filter((thread) => thread.includes(":user:")),
      2,
    );
  }).pipe(Effect.provide(layerTest())),
);

// A marked start that fails and is retried stays tracked: within its unit it is claimed again, and
// each claim, the retry too, owes aged automation one, so retries cannot keep it waiting.
it.effect("keeps tracking a retried user turn start", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue([turnStart("thread:retry:a1"), turnStart("thread:retry:a2")]);
    yield* TestClock.adjust(Duration.minutes(6));
    const user = turnStart("thread:retry:user", true);
    yield* outbox.enqueue([user]);
    const claim = () =>
      outbox
        .claimNext({ workerId: "retry" })
        .pipe(Effect.map((claimed) => Option.getOrUndefined(claimed)?.threadId ?? "none"));
    const order = [yield* claim()];
    assert.isTrue(
      yield* outbox.retry({ effectId: user.id, workerId: "retry", error: "probe", delayMs: 0 }),
    );
    order.push(yield* claim(), yield* claim());
    yield* outbox.enqueue([turnStart("thread:retry:user-2", true)]);
    order.push(yield* claim(), yield* claim());
    // The retried start is a second priority claim: aged automation is owed two before user-2.
    assert.deepEqual(order, [
      "thread:retry:user",
      "thread:retry:user",
      "thread:retry:a1",
      "thread:retry:a2",
      "thread:retry:user-2",
    ]);
  }).pipe(Effect.provide(layerTest())),
);

// A user message's capture and turn start are one priority unit: with automation aged past the
// starvation age, the start still takes the slot its capture frees (no extra slot turnover),
// whenever in a minute the message arrives.
it.effect("claims a user's start right after its capture behind an aged backlog", () =>
  Effect.gen(function* () {
    for (let second = 0; second < 60; second += 1) {
      const userAt = 6 * 60_000 + second * 1_000 + 1;
      const userCapture = capture(`thread:aged-${second}:user`);
      const userStart = turnStart(`thread:aged-${second}:user`, true);
      const claims = yield* runSlots({
        arrivals: [
          { at: 0, effects: backlog(200, `aged-${second}`) },
          { at: userAt, effects: [userCapture, userStart] },
        ],
        limitMs: userAt + 10 * 60_000,
        until: (claimed) => claimed.some(({ effect }) => effect.id === userStart.id),
      }).pipe(Effect.provide(layerTest()));
      const nextFree = Math.min(
        ...claims
          .filter(({ at }) => at < userAt)
          .map(({ at, effect }) => at + HOLD_MS[effect.request.type as keyof typeof HOLD_MS])
          .filter((freeAt) => freeAt >= userAt),
      );
      const claimedAt = (id: string) =>
        claims.find(({ effect }) => effect.id === id)?.at ?? Number.POSITIVE_INFINITY;
      assert.isAtMost(claimedAt(userCapture.id), nextFree, `arrival +${second}s`);
      assert.isAtMost(
        claimedAt(userStart.id),
        claimedAt(userCapture.id) + HOLD_MS["checkpoint.capture"],
        `arrival +${second}s`,
      );
      assertThreadOrder(claims);
    }
  }),
);

// Under a flood of user messages (capture and start each) with automation aged past 10 min,
// automation gets at least one claim between any two priority units.
it.effect("gives aged automation a claim between any two user units under a flood", () =>
  Effect.gen(function* () {
    const claims = yield* runSlots({
      arrivals: [
        {
          at: 0,
          effects: Array.from({ length: 30 }, (_, index) => turnStart(`thread:units:a${index}`)),
        },
        ...Array.from({ length: 120 }, (_, index) => ({
          at: 1 + index * 6_000,
          effects: [
            capture(`thread:units:user:${index}`),
            turnStart(`thread:units:user:${index}`, true),
          ],
        })),
      ],
      limitMs: 25 * 60_000,
    });
    // A unit starts with the first claim on a user thread; between two unit starts made while
    // automation older than 10 min waits, an automation claim must come.
    const automationClaimedAt = claims
      .filter(({ effect }) => !effect.threadId.includes(":user:"))
      .map(({ at }) => at);
    const lastAutomation = Math.max(...automationClaimedAt);
    const started = new Set<string>();
    let automationSinceUnit = true;
    for (const { at, effect } of claims) {
      if (!effect.threadId.includes(":user:")) {
        automationSinceUnit = true;
        continue;
      }
      if (started.has(effect.threadId)) continue;
      started.add(effect.threadId);
      if (at > 10 * 60_000 && at < lastAutomation) assert.isTrue(automationSinceUnit, String(at));
      automationSinceUnit = false;
    }
    assertThreadOrder(claims);
  }).pipe(Effect.provide(layerTest())),
);

// A turn start retried inside its unit is still that unit: claimed again ahead of aged automation.
it.effect("keeps a retried user start in its unit", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue([turnStart("thread:unit-retry:a1"), turnStart("thread:unit-retry:a2")]);
    yield* TestClock.adjust(Duration.minutes(6));
    const userCapture = capture("thread:unit-retry:user");
    const userStart = turnStart("thread:unit-retry:user", true);
    yield* outbox.enqueue([userCapture, userStart]);
    const claim = () =>
      outbox
        .claimNext({ workerId: "unit-retry" })
        .pipe(Effect.map((claimed) => Option.getOrUndefined(claimed)?.id ?? "none"));
    const order = [yield* claim()];
    assert.isTrue(yield* outbox.succeed({ effectId: userCapture.id, workerId: "unit-retry" }));
    order.push(yield* claim());
    assert.isTrue(
      yield* outbox.retry({
        effectId: userStart.id,
        workerId: "unit-retry",
        error: "probe",
        delayMs: 0,
      }),
    );
    order.push(yield* claim(), yield* claim());
    assert.deepEqual(order, [userCapture.id, userStart.id, userStart.id, order[3]!]);
    assert.include(order[3]!, "thread:unit-retry:a");
  }).pipe(Effect.provide(layerTest())),
);

// What is left ahead of a unit's start on its thread (a detach after the capture) is the unit's
// too: claimed ahead of aged automation, so the start is not left a slot turnover behind. The
// unit's claim reads only the unit threads' pending effects, whatever backlog waits.
it.effect("claims what is left ahead of a unit's start before aged automation", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const sql = yield* SqlClient.SqlClient;
    yield* outbox.enqueue([turnStart("thread:unit-ahead:a1"), turnStart("thread:unit-ahead:a2")]);
    yield* TestClock.adjust(Duration.minutes(6));
    const userCapture = capture("thread:unit-ahead:user");
    const userDetach: EffectOutbox.PendingOrchestrationEffectV2 = {
      id: nextId("thread:unit-ahead:user:detach"),
      commandId: CommandId.make("command:unit-ahead:detach"),
      threadId: ThreadId.make("thread:unit-ahead:user"),
      request: {
        type: "provider-session.detach",
        providerSessionId: ProviderSessionId.make("session:unit-ahead"),
      },
    };
    const userStart = turnStart("thread:unit-ahead:user", true);
    yield* outbox.enqueue([userCapture, userDetach, userStart]);
    const statements: Array<readonly [string, ReadonlyArray<unknown>]> = [];
    const claim = () =>
      outbox.claimNext({ workerId: "unit-ahead" }).pipe(
        Effect.map((claimed) => Option.getOrUndefined(claimed)?.id ?? "none"),
        Effect.provideService(Statement.CurrentTransformer, (statement) => {
          statements.push(statement.compile());
          return Effect.succeed(statement);
        }),
      );
    const order = [yield* claim()];
    assert.isTrue(yield* outbox.succeed({ effectId: userCapture.id, workerId: "unit-ahead" }));
    statements.length = 0;
    order.push(yield* claim());
    const unitStatements = [...statements];
    assert.isTrue(yield* outbox.succeed({ effectId: userDetach.id, workerId: "unit-ahead" }));
    order.push(yield* claim(), yield* claim());
    assert.deepEqual(order, [userCapture.id, userDetach.id, userStart.id, order[3]!]);
    assert.include(order[3]!, "thread:unit-ahead:a");
    // The unit's read and claim: by the units' threads and by id, never in claim order.
    for (const [query, params] of unitStatements.slice(0, 2)) {
      const plan = (yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${query}`,
        params,
      )).map((row) => row.detail);
      assert.isFalse(
        plan.some(
          (detail) =>
            detail.includes("orchestration_v2_effect_outbox_claim_idx") ||
            /^SCAN (candidate|ahead|unit_start)\b/.test(detail),
        ),
        plan.join("\n"),
      );
    }
  }).pipe(Effect.provide(layerTest())),
);

// Two users' messages overlap with no aged automation waiting: nothing is owed, so the second
// user's capture does not wait for the first user's start, and each start follows its capture.
it.effect(
  "opens a second user's unit while the first's start waits, when nothing aged is owed",
  () =>
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      yield* outbox.enqueue([1, 2, 3].map((index) => turnStart(`thread:overlap:a${index}`)));
      const captureA = capture("thread:overlap:user-a");
      const startA = turnStart("thread:overlap:user-a", true);
      yield* outbox.enqueue([captureA, startA]);
      const claim = () =>
        outbox
          .claimNext({ workerId: "overlap" })
          .pipe(Effect.map((claimed) => Option.getOrUndefined(claimed)?.id ?? "none"));
      const order = [yield* claim()];
      const captureB = capture("thread:overlap:user-b");
      const startB = turnStart("thread:overlap:user-b", true);
      yield* outbox.enqueue([captureB, startB]);
      order.push(yield* claim());
      assert.isTrue(yield* outbox.succeed({ effectId: captureA.id, workerId: "overlap" }));
      order.push(yield* claim());
      assert.isTrue(yield* outbox.succeed({ effectId: captureB.id, workerId: "overlap" }));
      order.push(yield* claim());
      assert.deepEqual(order, [captureA.id, captureB.id, startA.id, startB.id]);
    }).pipe(Effect.provide(layerTest())),
);

// Units' delayed retries that come due together are not claimed back to back while aged
// automation waits: each owes it a claim first.
it.effect("repays aged automation between units' retries that come due together", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue(
      Array.from({ length: 12 }, (_, index) => turnStart(`thread:retries:a${index}`)),
    );
    yield* TestClock.adjust(Duration.minutes(6));
    const users = [1, 2, 3, 4].map((index) => turnStart(`thread:retries:user-${index}`, true));
    yield* outbox.enqueue(users);
    const claim = () =>
      outbox
        .claimNext({ workerId: "retries" })
        .pipe(Effect.map((claimed) => Option.getOrUndefined(claimed)?.threadId ?? "none"));
    // Each start is claimed, then aged automation is repaid, then the start is retried later.
    for (const user of users) {
      assert.equal(yield* claim(), user.threadId);
      assert.include(yield* claim(), "thread:retries:a");
      assert.isTrue(
        yield* outbox.retry({
          effectId: user.id,
          workerId: "retries",
          error: "probe",
          delayMs: 30_000,
        }),
      );
    }
    yield* TestClock.adjust(Duration.seconds(30));
    const order: Array<string> = [];
    for (let index = 0; index < 8; index += 1) order.push(yield* claim());
    const kinds = order.map((thread) => (thread.includes(":user-") ? "user" : "automation"));
    for (let index = 1; index < kinds.length; index += 1)
      assert.isFalse(kinds[index] === "user" && kinds[index - 1] === "user", kinds.join(","));
  }).pipe(Effect.provide(layerTest())),
);

// The last aged effect is claimed while user A's capture runs: nothing aged waits any more, so
// user B's unit opens at once instead of waiting behind younger automation for A's start.
it.effect("opens a second user's unit once the last aged effect is claimed", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue([turnStart("thread:last-aged:a1")]);
    yield* TestClock.adjust(Duration.minutes(6));
    yield* outbox.enqueue([1, 2, 3].map((index) => turnStart(`thread:last-aged:young-${index}`)));
    const captureA = capture("thread:last-aged:user-a");
    yield* outbox.enqueue([captureA, turnStart("thread:last-aged:user-a", true)]);
    const claim = () =>
      outbox
        .claimNext({ workerId: "last-aged" })
        .pipe(Effect.map((claimed) => Option.getOrUndefined(claimed)?.id ?? "none"));
    assert.equal(yield* claim(), captureA.id);
    assert.include(yield* claim(), "thread:last-aged:a1");
    const captureB = capture("thread:last-aged:user-b");
    yield* outbox.enqueue([captureB, turnStart("thread:last-aged:user-b", true)]);
    assert.equal(yield* claim(), captureB.id);
  }).pipe(Effect.provide(layerTest())),
);

// The aged effect that held users back is cancelled between the check and the FIFO claim: the claim
// still takes the waiting user's capture rather than coming back empty.
it.effect("claims a waiting user's capture when the aged work that held it back is gone", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const sql = yield* SqlClient.SqlClient;
    const a1 = turnStart("thread:held-race:a1");
    const a2 = turnStart("thread:held-race:a2");
    yield* outbox.enqueue([a1, a2]);
    yield* TestClock.adjust(Duration.minutes(6));
    const captureA = capture("thread:held-race:user-a");
    yield* outbox.enqueue([captureA, turnStart("thread:held-race:user-a", true)]);
    const claim = () =>
      outbox
        .claimNext({ workerId: "held-race" })
        .pipe(Effect.map((claimed) => Option.getOrUndefined(claimed)?.id ?? "none"));
    assert.equal(yield* claim(), captureA.id);
    assert.include(yield* claim(), "thread:held-race:a1");
    const captureB = capture("thread:held-race:user-b");
    yield* outbox.enqueue([captureB, turnStart("thread:held-race:user-b", true)]);
    // The check reads aged work; the next statement (the claim) runs after a2 is cancelled.
    let checked = false;
    const cancelAfterCheck: Statement.Transformer = (statement) => {
      const [query] = statement.compile();
      const claimAfterCheck = checked && query.includes("UPDATE orchestration_v2_effect_outbox");
      // One shot: the cancel's own UPDATE passes through here too.
      if (claimAfterCheck) checked = false;
      if (query.includes("SELECT EXISTS (")) checked = true;
      return claimAfterCheck
        ? Effect.succeed(statement).pipe(
            Effect.tap(() =>
              sql`UPDATE orchestration_v2_effect_outbox SET status = 'cancelled' WHERE effect_id = ${a2.id}`.pipe(
                Effect.orDie,
              ),
            ),
          )
        : Effect.succeed(statement);
    };
    assert.equal(
      yield* claim().pipe(Effect.provideService(Statement.CurrentTransformer, cancelAfterCheck)),
      captureB.id,
    );
  }).pipe(Effect.provide(layerTest())),
);

// Older users' effects become claimable while user A's unit runs and aged automation waits: plain
// FIFO does not take them past the alternation, as if they were not priority work.
it.effect("keeps older users' effects out of plain FIFO while a unit holds others back", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const users = [1, 2, 3].map((index) => `thread:fifo-bypass:user-b${index}`);
    const cleanups = users.map((thread) => ({
      id: nextId(`${thread}:cleanup`),
      commandId: CommandId.make(`command:${thread}:cleanup`),
      threadId: ThreadId.make(thread),
      request: { type: "terminal.cleanup" as const },
    }));
    yield* outbox.enqueue(
      users.flatMap((thread, index) => [
        cleanups[index]!,
        capture(thread),
        turnStart(thread, true),
      ]),
    );
    for (const _ of users)
      yield* outbox.claimNext({
        workerId: "fifo-bypass",
        excludeEffectTypes: EffectAdmission.GATED_EFFECT_TYPES,
      });
    yield* outbox.enqueue([1, 2, 3, 4].map((index) => turnStart(`thread:fifo-bypass:a${index}`)));
    yield* TestClock.adjust(Duration.minutes(6));
    const captureA = capture("thread:fifo-bypass:user-a");
    yield* outbox.enqueue([captureA, turnStart("thread:fifo-bypass:user-a", true)]);
    const claim = () =>
      outbox
        .claimNext({ workerId: "fifo-bypass" })
        .pipe(Effect.map((claimed) => Option.getOrUndefined(claimed)?.threadId ?? "none"));
    assert.equal(yield* claim(), "thread:fifo-bypass:user-a");
    assert.include(yield* claim(), "thread:fifo-bypass:a");
    for (const cleanup of cleanups)
      assert.isTrue(yield* outbox.succeed({ effectId: cleanup.id, workerId: "fifo-bypass" }));
    const kinds: Array<string> = [];
    for (let index = 0; index < 4; index += 1)
      kinds.push((yield* claim()).includes(":user-") ? "user" : "automation");
    for (let index = 1; index < kinds.length; index += 1)
      assert.isFalse(kinds[index] === "user" && kinds[index - 1] === "user", kinds.join(","));
  }).pipe(Effect.provide(layerTest())),
);

// A unit whose start does not follow its capture within the unit window is released: its start
// then waits its turn behind aged automation like any new unit.
it.effect("releases a unit whose start does not follow its capture in time", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue([turnStart("thread:unit-expiry:a1"), turnStart("thread:unit-expiry:a2")]);
    yield* TestClock.adjust(Duration.minutes(6));
    const userCapture = capture("thread:unit-expiry:user");
    const userStart = turnStart("thread:unit-expiry:user", true);
    yield* outbox.enqueue([userCapture, userStart]);
    const claim = () =>
      outbox
        .claimNext({ workerId: "unit-expiry" })
        .pipe(Effect.map((claimed) => Option.getOrUndefined(claimed)?.id ?? "none"));
    const order = [yield* claim()];
    // The capture hangs past the unit window before it settles.
    yield* TestClock.adjust(Duration.minutes(3));
    assert.isTrue(yield* outbox.succeed({ effectId: userCapture.id, workerId: "unit-expiry" }));
    order.push(yield* claim(), yield* claim());
    assert.equal(order[0], userCapture.id);
    assert.include(order[1]!, "thread:unit-expiry:a");
    assert.equal(order[2], userStart.id);
  }).pipe(Effect.provide(layerTest())),
);

// While the gate is full, workers claim only ungated effects, such as the detach ahead of each of a
// flood of user starts. Those claims run the users' ungated work but open no unit, so once a slot
// frees, aged automation still gets one claim in two against the users' starts.
it.effect("opens no unit from a gate-excluded claim, so a flood cannot pass owed aged work", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue([turnStart("thread:excluded:a1"), turnStart("thread:excluded:a2")]);
    yield* TestClock.adjust(Duration.minutes(11));
    const users = Array.from({ length: 6 }, (_, index) => `thread:excluded:user:${index}`);
    yield* outbox.enqueue(
      users.flatMap((thread) => [
        {
          id: nextId(`${thread}:detach`),
          commandId: CommandId.make(`command:${thread}:detach`),
          threadId: ThreadId.make(thread),
          request: {
            type: "provider-session.detach",
            providerSessionId: ProviderSessionId.make(`session:${thread}`),
          },
        } satisfies EffectOutbox.PendingOrchestrationEffectV2,
        turnStart(thread, true),
      ]),
    );
    for (const _ of users) {
      const claimed = yield* outbox.claimNext({
        workerId: "excluded",
        excludeEffectTypes: EffectAdmission.GATED_EFFECT_TYPES,
      });
      const detach = Option.getOrUndefined(claimed);
      assert.equal(detach?.request.type, "provider-session.detach");
      assert.isTrue(yield* outbox.succeed({ effectId: detach!.id, workerId: "excluded" }));
    }
    const order: Array<string> = [];
    for (let index = 0; index < 6; index += 1) {
      const claimed = yield* outbox.claimNext({ workerId: "excluded" });
      order.push(Option.getOrUndefined(claimed)?.threadId ?? "none");
    }
    // No two user starts in a row while automation waits.
    assert.deepEqual(
      order.map((thread) => (thread.includes(":user:") ? "user" : "automation")),
      ["user", "automation", "user", "automation", "user", "user"],
    );
  }).pipe(Effect.provide(layerTest())),
);

// A worker that leaves gated work in the outbox (a full gate) claims a cleanup between two gated
// claims: that does not settle what a unit owes aged automation, so it still goes next.
it.effect("keeps aged automation owed across a gate-excluded claim", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    yield* outbox.enqueue([turnStart("thread:gated:automation")]);
    yield* TestClock.adjust(Duration.minutes(6));
    yield* outbox.enqueue([
      turnStart("thread:gated:user-1", true),
      {
        id: nextId("thread:gated:cleanup"),
        commandId: CommandId.make("command:gated:cleanup"),
        threadId: ThreadId.make("thread:gated:cleanup"),
        request: { type: "terminal.cleanup" },
      },
      turnStart("thread:gated:user-2", true),
    ]);
    const claim = (excludeEffectTypes?: ReadonlyArray<string>) =>
      outbox
        .claimNext({ workerId: "gated", ...(excludeEffectTypes ? { excludeEffectTypes } : {}) })
        .pipe(Effect.map((claimed) => Option.getOrUndefined(claimed)?.threadId ?? "none"));
    assert.deepEqual(
      [yield* claim(), yield* claim(EffectAdmission.GATED_EFFECT_TYPES), yield* claim()],
      ["thread:gated:user-1", "thread:gated:cleanup", "thread:gated:automation"],
    );
  }).pipe(Effect.provide(layerTest())),
);

// The claim is the worker's once its UPDATE commits: a failed read of which unit it belongs to
// still hands the effect over, and owes aged work instead of leaving a running row nobody runs.
it.effect("hands over a claimed effect when its unit cannot be read", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const userCapture = capture("thread:unit-read:user");
    yield* outbox.enqueue([userCapture, turnStart("thread:unit-read:user", true)]);
    const claimed = yield* outbox.claimNext({ workerId: "unit-read" }).pipe(
      Effect.provideService(Statement.CurrentTransformer, (statement) => {
        const [query] = statement.compile();
        return query.includes("ORDER BY rowid ASC")
          ? (Effect.fail(
              new EffectOutbox.EffectOutboxError({ operation: "injected unit read" }),
            ) as never)
          : Effect.succeed(statement);
      }),
    );
    assert.equal(Option.getOrUndefined(claimed)?.id, userCapture.id);
    assert.equal(Option.getOrUndefined(yield* outbox.get(userCapture.id))?.leaseOwner, "unit-read");
  }).pipe(Effect.provide(layerTest())),
);

// A build that predates the field reads a marked turn start as an ordinary one, and this build
// reads one without it: neither fails the row as undecodable.
it.effect("decodes a turn start with or without its claim priority", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    const sql = yield* SqlClient.SqlClient;
    for (const [index, payload] of [
      { type: "provider-turn.start", runId: "run:compat:0", claimPriority: "user" },
      { type: "provider-turn.start", runId: "run:compat:1" },
    ].entries()) {
      yield* sql`INSERT INTO orchestration_v2_effect_outbox (
          effect_id, command_id, thread_id, effect_type, payload_json, status, attempt_count,
          available_at, lease_owner, created_at, updated_at, completed_at, last_error
        ) VALUES (
          ${`effect:compat:${index}`}, ${`command:compat:${index}`}, ${`thread:compat:${index}`},
          'provider-turn.start', ${JSON.stringify(payload)}, 'pending', 0,
          '1970-01-01T00:00:00.000Z', NULL, '1970-01-01T00:00:00.000Z',
          '1970-01-01T00:00:00.000Z', NULL, NULL
        )`;
      const claimed = yield* outbox.claimNext({ workerId: `compat:${index}` });
      assert.equal(Option.getOrUndefined(claimed)?.request.type, "provider-turn.start");
    }
  }).pipe(Effect.provide(layerTest())),
);

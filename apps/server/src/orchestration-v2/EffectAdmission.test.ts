import { assert, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";
import { CurrentWaitObserver } from "@t3tools/shared/WaitObserver";

import { recordingWaitObserver } from "../observability/WaitObserver.testkit.ts";
import * as EffectAdmission from "./EffectAdmission.ts";

interface Harness {
  readonly admission: EffectAdmission.EffectAdmissionShape;
  readonly pressure: Ref.Ref<number>;
  readonly restored: Ref.Ref<number>;
  readonly freed: Ref.Ref<number>;
  /** Times the pressure source was acquired and released. */
  readonly acquired: Ref.Ref<number>;
  readonly released: Ref.Ref<number>;
}

const makeHarness = (workers: number, closeAbove = 40) =>
  Effect.gen(function* () {
    const pressure = yield* Ref.make(5);
    const restored = yield* Ref.make(0);
    const freed = yield* Ref.make(0);
    const acquired = yield* Ref.make(0);
    const released = yield* Ref.make(0);
    const admission = yield* EffectAdmission.make({
      workers,
      closeAbove,
      samplesToSwitch: 2,
      sampleInterval: Duration.seconds(1),
      pressure: Effect.acquireRelease(
        Ref.update(acquired, (count) => count + 1).pipe(Effect.as(Ref.get(pressure))),
        () => Ref.update(released, (count) => count + 1),
      ),
      onAllowanceRestored: Ref.update(restored, (count) => count + 1),
      onSlotFreed: Ref.update(freed, (count) => count + 1),
    });
    return { admission, pressure, restored, freed, acquired, released } satisfies Harness;
  });

/** Takes a slot, or none, and returns what gives it back. */
const reserve = (admission: EffectAdmission.EffectAdmissionShape) =>
  admission.reserve.pipe(Effect.map(Option.getOrUndefined));

const sampling = (admission: EffectAdmission.EffectAdmissionShape) =>
  admission.state.pipe(Effect.map((state) => state.sampling));

it.effect("hands out one slot per worker and excludes nothing while a slot is free", () =>
  Effect.gen(function* () {
    const { admission } = yield* makeHarness(2);

    assert.deepEqual(yield* admission.excludedTypes, []);
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    assert.isDefined(first);
    assert.isDefined(second);
    assert.isUndefined(yield* reserve(admission));
    assert.deepEqual(yield* admission.excludedTypes, EffectAdmission.GATED_EFFECT_TYPES);
    assert.equal((yield* admission.state).inFlight, 2);

    yield* first!.release;
    assert.deepEqual(yield* admission.excludedTypes, []);
    assert.isDefined(yield* reserve(admission));
    assert.isUndefined(yield* reserve(admission));
    yield* second!.release;
    assert.equal((yield* admission.state).inFlight, 1);
  }),
);

it.effect(
  "runs the pressure source only from the allowance's worth of gated effects in flight",
  () =>
    Effect.gen(function* () {
      const { admission, acquired, released } = yield* makeHarness(4);
      // Allowance while saturated is 2: one gated effect cannot be held, so nothing samples.
      const first = yield* reserve(admission);
      assert.equal(yield* Ref.get(acquired), 0);
      assert.isFalse(yield* sampling(admission));
      yield* TestClock.adjust("5 seconds");
      assert.equal(yield* Ref.get(acquired), 0);

      // The second gated effect could be held, so the source starts; it stops once one finishes.
      const second = yield* reserve(admission);
      assert.equal(yield* Ref.get(acquired), 1);
      assert.isTrue(yield* sampling(admission));
      yield* second!.release;
      assert.isFalse(yield* sampling(admission));
      assert.equal(yield* Ref.get(released), 1);
      yield* first!.release;
      yield* TestClock.adjust("5 seconds");
      assert.equal(yield* Ref.get(acquired), 1);
    }),
);

it.effect(
  "halves the allowance after two saturated samples and restores it after two calm ones",
  () =>
    Effect.gen(function* () {
      const { admission, pressure, restored } = yield* makeHarness(4);
      yield* Ref.set(pressure, 60);
      const first = yield* reserve(admission);
      const second = yield* reserve(admission);
      assert.isDefined(first);
      assert.isDefined(second);

      // One high sample switches nothing; the second closes the gate at the saturated allowance.
      yield* TestClock.adjust("1 second");
      assert.equal((yield* admission.state).saturated, false);
      yield* TestClock.adjust("1 second");
      assert.deepEqual(yield* admission.state, {
        allowed: 2,
        inFlight: 2,
        saturated: true,
        sampling: true,
      });
      assert.isUndefined(yield* reserve(admission));
      assert.deepEqual(yield* admission.excludedTypes, EffectAdmission.GATED_EFFECT_TYPES);

      // Between the marks nothing changes; one calm sample is not enough either.
      yield* Ref.set(pressure, 30);
      yield* TestClock.adjust("2 seconds");
      assert.equal((yield* admission.state).saturated, true);
      yield* Ref.set(pressure, 10);
      yield* TestClock.adjust("1 second");
      assert.equal((yield* admission.state).saturated, true);
      assert.equal(yield* Ref.get(restored), 0);

      // The second calm sample restores the allowance and wakes the workers once.
      yield* TestClock.adjust("1 second");
      assert.deepEqual(yield* admission.state, {
        allowed: 4,
        inFlight: 2,
        saturated: false,
        sampling: true,
      });
      assert.equal(yield* Ref.get(restored), 1);
      assert.deepEqual(yield* admission.excludedTypes, []);
      assert.isDefined(yield* reserve(admission));
      assert.isDefined(yield* reserve(admission));
      assert.isUndefined(yield* reserve(admission));
    }),
);

it.effect("counts the time its gate stood closed and saturated for the wait observer", () => {
  const { observer, readers } = recordingWaitObserver();
  return Effect.gen(function* () {
    const { admission, pressure } = yield* makeHarness(4);
    const closedMs = readers.get("EffectAdmission.closedMs")!;
    const saturatedMs = readers.get("EffectAdmission.saturatedMs")!;
    yield* Ref.set(pressure, 60);
    const slots = [];
    for (let index = 0; index < 4; index++) slots.push(yield* reserve(admission));
    assert.deepEqual([closedMs(), saturatedMs()], [0, 0]);

    // Full since the fourth slot; saturated from the second high sample.
    yield* TestClock.adjust("3 seconds");
    assert.deepEqual([closedMs(), saturatedMs()], [3_000, 1_000]);
    // Two slots still fill the saturated allowance; the third freed reopens the gate.
    yield* slots[0]!.release;
    yield* slots[1]!.release;
    assert.equal(closedMs(), 3_000);
    yield* slots[2]!.release;
    yield* TestClock.adjust("2 seconds");
    assert.deepEqual([closedMs(), saturatedMs()], [3_000, 3_000]);
    yield* slots[3]!.release;
  }).pipe(Effect.provideService(CurrentWaitObserver, observer));
});

it.effect("reopens only below three quarters of the closing mark, not at or above it", () =>
  Effect.gen(function* () {
    const { admission, pressure } = yield* makeHarness(2);
    yield* Ref.set(pressure, 60);
    const first = yield* reserve(admission);
    yield* TestClock.adjust("2 seconds");
    assert.equal((yield* admission.state).saturated, true);

    // Exactly the reopen mark and anything between the marks keep the gate closed.
    for (const held of [30, 32, 35, 39]) {
      yield* Ref.set(pressure, held);
      yield* TestClock.adjust("2 seconds");
      assert.equal((yield* admission.state).saturated, true, `pressure ${held}`);
    }
    // Just under three quarters reopens, even though it is well above half the mark.
    yield* Ref.set(pressure, 29);
    yield* TestClock.adjust("2 seconds");
    assert.equal((yield* admission.state).saturated, false);
    yield* first!.release;
  }),
);

it.effect(
  "keeps running effects when the allowance shrinks and excludes new ones until they end",
  () =>
    Effect.gen(function* () {
      const { admission, pressure, freed } = yield* makeHarness(2);
      const first = yield* reserve(admission);
      const second = yield* reserve(admission);
      assert.isDefined(first);
      assert.isDefined(second);

      yield* Ref.set(pressure, 60);
      yield* TestClock.adjust("2 seconds");
      assert.deepEqual(yield* admission.state, {
        allowed: 1,
        inFlight: 2,
        saturated: true,
        sampling: true,
      });
      assert.deepEqual(yield* admission.excludedTypes, EffectAdmission.GATED_EFFECT_TYPES);

      // One slot given back still leaves two in flight above the allowance of one: no wake-up yet.
      yield* first!.release;
      assert.isUndefined(yield* reserve(admission));
      assert.equal(yield* Ref.get(freed), 0);
      // The next one frees a slot below the allowance, which wakes one worker.
      yield* second!.release;
      assert.equal(yield* Ref.get(freed), 1);
      assert.deepEqual(yield* admission.excludedTypes, []);
      assert.isDefined(yield* reserve(admission));
      assert.isUndefined(yield* reserve(admission));
    }),
);

it.effect("wakes one worker when a slot frees while gated effects were being skipped", () =>
  Effect.gen(function* () {
    const { admission, freed } = yield* makeHarness(2);
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    assert.isUndefined(yield* reserve(admission));
    yield* first!.release;
    assert.equal(yield* Ref.get(freed), 1);
    // A release while a slot was already free wakes nobody: nothing was being skipped.
    yield* second!.release;
    assert.equal(yield* Ref.get(freed), 1);
  }),
);

it.effect("a slot given back unused frees nothing and wakes nobody", () =>
  Effect.gen(function* () {
    const { admission, freed } = yield* makeHarness(1);
    const slot = yield* reserve(admission);
    assert.isUndefined(yield* reserve(admission));
    // The claim found nothing: the slot was never used, so no worker was skipped for it.
    yield* slot!.discard;
    assert.equal(yield* Ref.get(freed), 0);
    assert.equal((yield* admission.state).inFlight, 0);
    // A gated effect that ran does free the slot, and the one worker's own wake-up is bounded.
    const used = yield* reserve(admission);
    yield* used!.release;
    assert.equal(yield* Ref.get(freed), 1);
    yield* used!.discard;
    assert.equal(yield* Ref.get(freed), 1);
  }),
);

it.effect("stops counting a slot held longer than a turn start may take", () =>
  Effect.gen(function* () {
    const { admission, freed } = yield* makeHarness(2);
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    assert.isDefined(first);
    assert.isDefined(second);
    assert.isUndefined(yield* reserve(admission));

    // Two hung starts must not hold every slot for good: after the bound a new start may run.
    yield* TestClock.adjust(EffectAdmission.SLOT_MAX_HOLD);
    assert.deepEqual(yield* admission.excludedTypes, []);
    assert.equal(yield* Ref.get(freed), 1);
    const third = yield* reserve(admission);
    assert.isDefined(third);
    assert.equal((yield* admission.state).inFlight, 1);

    // A late release of an expired slot changes nothing; the live one still counts.
    yield* first!.release;
    yield* second!.release;
    assert.equal((yield* admission.state).inFlight, 1);
    yield* third!.release;
    assert.equal((yield* admission.state).inFlight, 0);
  }),
);

it.effect("stops sampling when expired slots leave the open gate nothing to act on", () =>
  Effect.gen(function* () {
    const { admission, acquired, released } = yield* makeHarness(4);
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    assert.isTrue(yield* sampling(admission));
    // Calm throughout: both starts hang past the bound, so nothing is in flight and the source,
    // which runs only while the gate can act, must stop with them.
    yield* TestClock.adjust(EffectAdmission.SLOT_MAX_HOLD);
    assert.deepEqual(yield* admission.state, {
      allowed: 4,
      inFlight: 0,
      saturated: false,
      sampling: false,
    });
    assert.equal(yield* Ref.get(released), 1);
    // The late releases of the expired slots change nothing.
    yield* first!.release;
    yield* second!.release;
    assert.equal(yield* Ref.get(acquired), 1);
    assert.isFalse(yield* sampling(admission));
  }),
);

it.effect("keeps counting a slot through the longest ordinary turn start", () =>
  Effect.gen(function* () {
    const { admission } = yield* makeHarness(2);
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    // Turn starts run a minute or two, as a rule: their slow tail must not expire the slot, or
    // the gate would drift back towards the worker count under exactly the load it is for.
    yield* TestClock.adjust("5 minutes");
    assert.deepEqual(yield* admission.excludedTypes, EffectAdmission.GATED_EFFECT_TYPES);
    assert.equal((yield* admission.state).inFlight, 2);
    yield* first!.release;
    yield* second!.release;
  }),
);

it.effect("arms one sampler when reserves race while the source is being acquired", () =>
  Effect.gen(function* () {
    const acquired = yield* Ref.make(0);
    const acquiring = yield* Deferred.make<void>();
    const admission = yield* EffectAdmission.make({
      workers: 4,
      closeAbove: 40,
      samplesToSwitch: 2,
      sampleInterval: Duration.seconds(1),
      pressure: Effect.acquireRelease(
        Deferred.await(acquiring).pipe(
          Effect.andThen(Ref.update(acquired, (count) => count + 1)),
          Effect.as(Effect.succeed(5)),
        ),
        () => Effect.void,
      ),
      onAllowanceRestored: Effect.void,
      onSlotFreed: Effect.void,
    });
    const first = yield* reserve(admission);
    // The second reserve arms the sampler and waits for the source; the third must not arm another.
    const second = yield* Effect.forkChild(admission.reserve);
    yield* Effect.yieldNow;
    const third = yield* reserve(admission);
    assert.isTrue(yield* sampling(admission));
    yield* Deferred.succeed(acquiring, undefined);
    const secondSlot = Option.getOrUndefined(yield* Fiber.join(second));
    assert.equal(yield* Ref.get(acquired), 1);
    assert.isTrue(yield* sampling(admission));
    yield* first!.release;
    yield* secondSlot!.release;
    yield* third!.release;
    assert.isFalse(yield* sampling(admission));
  }),
);

it.effect("lets a stop requested during the reopening sample wait for its wake-up", () =>
  Effect.gen(function* () {
    const pressure = yield* Ref.make(60);
    const restored = yield* Ref.make(0);
    const released = yield* Ref.make(0);
    const unblock = yield* Deferred.make<void>();
    const admission = yield* EffectAdmission.make({
      workers: 4,
      closeAbove: 40,
      samplesToSwitch: 2,
      sampleInterval: Duration.seconds(1),
      pressure: Effect.acquireRelease(Effect.succeed(Ref.get(pressure)), () =>
        Ref.update(released, (count) => count + 1),
      ),
      // The wake-up takes a while: the window in which a worker may ask the sampler to stop.
      onAllowanceRestored: Deferred.await(unblock).pipe(
        Effect.andThen(Ref.update(restored, (count) => count + 1)),
      ),
      onSlotFreed: Effect.void,
    });
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    yield* TestClock.adjust("2 seconds");
    assert.isTrue((yield* admission.state).saturated);
    yield* first!.release;
    yield* second!.release;
    yield* Ref.set(pressure, 5);
    yield* TestClock.adjust("2 seconds");
    // The gate has reopened and the sampler sits in the wake-up; nothing is in flight, so a
    // worker consulting the gate now asks the sampler to stop. The wake-up must still arrive.
    assert.isFalse((yield* admission.state).saturated);
    const stopping = yield* Effect.forkChild(admission.excludedTypes);
    yield* Effect.yieldNow;
    yield* Deferred.succeed(unblock, undefined);
    yield* Fiber.join(stopping);
    assert.equal(yield* Ref.get(restored), 1);
    assert.deepEqual(yield* admission.state, {
      allowed: 4,
      inFlight: 0,
      saturated: false,
      sampling: false,
    });
    assert.equal(yield* Ref.get(released), 1);
  }),
);

it.effect("starts its sample counts afresh after it stops", () =>
  Effect.gen(function* () {
    const { admission, pressure } = yield* makeHarness(4);
    yield* Ref.set(pressure, 60);
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    // One saturated sample, then nothing in flight: the sampler stops.
    yield* TestClock.adjust("1 second");
    yield* first!.release;
    yield* second!.release;
    assert.isFalse(yield* sampling(admission));
    // One saturated sample after re-arming is not two in a row.
    const third = yield* reserve(admission);
    const fourth = yield* reserve(admission);
    yield* TestClock.adjust("1 second");
    assert.deepEqual(yield* admission.state, {
      allowed: 4,
      inFlight: 2,
      saturated: false,
      sampling: true,
    });
    yield* TestClock.adjust("1 second");
    assert.isTrue((yield* admission.state).saturated);
    yield* third!.release;
    yield* fourth!.release;
  }),
);

/** Replays a 1 Hz loop-delay p99 shape through an armed gate: the state after each sample. */
const replay = (series: ReadonlyArray<number>) =>
  Effect.gen(function* () {
    const { admission, pressure } = yield* makeHarness(4);
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    const states = yield* Effect.forEach(series, (value) =>
      Ref.set(pressure, value).pipe(
        Effect.andThen(TestClock.adjust("1 second")),
        Effect.andThen(admission.state),
        Effect.map((state) => state.saturated),
      ),
    );
    yield* first!.release;
    yield* second!.release;
    return states;
  });

it.effect(
  "reopens within two calm samples after a spike at light load; stays closed when saturated",
  () =>
    Effect.gen(function* () {
      // At light load a short spike closes the gate, then the p99 settles under the reopen mark
      // but above half the mark; the gate must reopen within two samples, not stay closed.
      assert.deepEqual(yield* replay([64, 66, 22, 26, 21, 27, 23, 26]), [
        false,
        true,
        true,
        false,
        false,
        false,
        false,
        false,
      ]);
      // A saturated loop hovers around the mark: closed, and no flapping.
      assert.deepEqual(yield* replay([48, 46, 36, 43, 34, 47, 41, 37]), [
        false,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
      ]);
    }),
);

it.effect("recovers from saturation at a small mark too", () =>
  Effect.gen(function* () {
    const { admission, pressure, restored } = yield* makeHarness(4, 5);
    // Pressure 5 is at the mark; the gate closes and reopens once the loop is idle.
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    yield* TestClock.adjust("2 seconds");
    assert.equal((yield* admission.state).saturated, true);
    yield* Ref.set(pressure, 0);
    yield* TestClock.adjust("2 seconds");
    assert.deepEqual(yield* admission.state, {
      allowed: 4,
      inFlight: 2,
      saturated: false,
      sampling: true,
    });
    assert.equal(yield* Ref.get(restored), 1);
    yield* first!.release;
    yield* second!.release;
  }),
);

it.effect("sizes the saturated allowance, the reopen mark and the pressure reading", () =>
  Effect.sync(() => {
    assert.equal(EffectAdmission.saturatedAllowance(1), 1);
    assert.equal(EffectAdmission.saturatedAllowance(2), 1);
    assert.equal(EffectAdmission.saturatedAllowance(4), 2);
    assert.equal(EffectAdmission.saturatedAllowance(5), 3);
    assert.equal(EffectAdmission.REOPEN_FRACTION, 0.75);
    assert.equal(EffectAdmission.openMarkFor(40), 30);
    assert.equal(EffectAdmission.openMarkFor(5), 3.75);
    assert.isTrue(EffectAdmission.isGated("provider-turn.start"));
    assert.isTrue(EffectAdmission.isGated("checkpoint.capture"));
    assert.isFalse(EffectAdmission.isGated("provider-turn.interrupt"));
    assert.isFalse(EffectAdmission.isGated("runtime-request.respond"));
    // The histogram drops the first gap after a reset: a block spanning most of the interval
    // leaves it few samples and reading calm, so then the sampler's own lateness stands in.
    assert.equal(EffectAdmission.MIN_HISTOGRAM_SAMPLES, 10);
    assert.equal(EffectAdmission.latencyPressure(0, 200, 3), 200);
    assert.equal(EffectAdmission.latencyPressure(0, 200, 100), 0);
    assert.equal(EffectAdmission.latencyPressure(45, 3, 100), 45);
    assert.equal(EffectAdmission.latencyPressure(45, 60, 9), 60);
    assert.equal(EffectAdmission.latencyPressure(Number.NaN, -2, 0), 0);
    assert.equal(EffectAdmission.samplerLateness(1_250, 0, 1_000), 250);
    assert.equal(EffectAdmission.samplerLateness(990, 0, 1_000), -10);
    const reading = { p99Ns: 5e6, nowMs: 1_250, previousMs: 0, intervalMs: 1_000 };
    assert.equal(EffectAdmission.histogramReading({ ...reading, samples: 100 }), 5);
    assert.equal(EffectAdmission.histogramReading({ ...reading, samples: 3 }), 250);
    assert.equal(
      EffectAdmission.histogramReading({ ...reading, p99Ns: Number.NaN, samples: 0 }),
      250,
    );
    assert.equal(Duration.toMillis(EffectAdmission.SLOT_MAX_HOLD), 600_000);
  }),
);

// A gated effect waiting on the commit lane does no main-thread work, so its slot can
// run another gated effect meanwhile; it takes its slot back when its write lands.
it.effect("lends a waiting effect's slot to another gated effect", () =>
  Effect.gen(function* () {
    const { admission, freed } = yield* makeHarness(2);
    const waiting = yield* reserve(admission);
    const working = yield* reserve(admission);
    assert.isUndefined(yield* reserve(admission));

    // While it waits on the lane, its slot runs another effect; the skipped workers are woken.
    yield* waiting!.suspend;
    assert.equal(yield* Ref.get(freed), 1);
    assert.equal((yield* admission.state).inFlight, 1);
    const borrower = yield* reserve(admission);
    assert.isDefined(borrower);
    assert.deepEqual(yield* admission.excludedTypes, EffectAdmission.GATED_EFFECT_TYPES);

    // Its write lands: it takes a slot back at once, three over two until two are given back.
    yield* waiting!.resume;
    assert.equal((yield* admission.state).inFlight, 3);
    yield* working!.release;
    assert.isUndefined(yield* reserve(admission));
    // Released once, in whichever state: no slot is freed twice.
    yield* waiting!.release;
    yield* waiting!.release;
    assert.equal((yield* admission.state).inFlight, 1);
    assert.isDefined(yield* reserve(admission));
    yield* borrower!.release;
  }),
);

it.effect("gives back nothing an effect does not hold when it ends lent out", () =>
  Effect.gen(function* () {
    const { admission, freed } = yield* makeHarness(1);
    const lent = yield* reserve(admission);
    yield* lent!.suspend;
    assert.equal(yield* Ref.get(freed), 1);
    const other = yield* reserve(admission);
    // Its effect ends while lent out: release, then a late resume or suspend, change nothing.
    yield* lent!.release;
    yield* lent!.resume;
    yield* lent!.suspend;
    assert.equal((yield* admission.state).inFlight, 1);
    assert.equal(yield* Ref.get(freed), 1);
    yield* other!.release;
    assert.equal((yield* admission.state).inFlight, 0);
  }),
);

it.effect(
  "takes a returning effect's slot back at once, so a lock it holds cannot stall the gate",
  () =>
    Effect.gen(function* () {
      // At one slot: A lends its slot for a write made under a lock; B takes the slot and waits for
      // that lock. A's write lands: if A had to wait for B's slot, neither could go on.
      const { admission } = yield* makeHarness(1);
      const a = yield* reserve(admission);
      yield* a!.suspend;
      const b = yield* reserve(admission);
      assert.isDefined(b);
      const resumed = yield* a!.resume.pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.isDefined(resumed.pollUnsafe(), "the returning effect waited for a slot");
      // Over the allowance until one gives its slot back: no new gated claim meanwhile.
      assert.isUndefined(yield* reserve(admission));
      assert.deepEqual(yield* admission.excludedTypes, EffectAdmission.GATED_EFFECT_TYPES);
      yield* a!.release;
      assert.isUndefined(yield* reserve(admission));
      yield* b!.release;
      assert.isDefined(yield* reserve(admission));
    }),
);

it.effect(
  "lends no more slots than its allowance, so gated work in flight stays within twice it",
  () =>
    Effect.gen(function* () {
      // Bound: in flight = slots held + slots lent; a claim is admitted only below the allowance and a
      // slot is lent only while fewer than the saturated allowance are out (one here).
      const { admission } = yield* makeHarness(1);
      const first = yield* reserve(admission);
      yield* first!.suspend;
      const second = yield* reserve(admission);
      assert.isDefined(second);
      // One slot is already out: the second effect keeps its slot while it waits on the lane.
      yield* second!.suspend;
      assert.isUndefined(yield* reserve(admission), "a third gated effect was admitted");
      assert.equal((yield* admission.state).inFlight, 1);
      yield* first!.resume;
      yield* second!.resume;
      assert.equal((yield* admission.state).inFlight, 2);
      yield* first!.release;
      yield* second!.release;
      assert.equal((yield* admission.state).inFlight, 0);
    }),
);

it.effect(
  "bounds lent slots by the saturated allowance, whatever the allowance is when they return",
  () =>
    Effect.gen(function* () {
      // Four workers: four slots open, two when saturated. Lending stops at two, so a drop to the
      // saturated allowance cannot leave more out than it allows: in flight <= 4 + 2.
      const { admission, pressure } = yield* makeHarness(4);
      const first = yield* Effect.forEach([1, 2, 3, 4], () => reserve(admission));
      yield* Effect.forEach(first, (slot) => slot!.suspend);
      assert.equal((yield* admission.state).inFlight, 2, "only two of four slots were lent");
      const more = yield* Effect.forEach([1, 2], () => reserve(admission));
      assert.isTrue(more.every((slot) => slot !== undefined));
      assert.isUndefined(yield* reserve(admission));
      yield* Ref.set(pressure, 60);
      yield* TestClock.adjust("3 seconds");
      assert.equal((yield* admission.state).allowed, 2);
      yield* Effect.forEach(first, (slot) => slot!.resume);
      assert.equal((yield* admission.state).inFlight, 6);
      assert.isUndefined(yield* reserve(admission));
      yield* Effect.forEach([...first, ...more], (slot) => slot!.release);
      assert.equal((yield* admission.state).inFlight, 0);
    }),
);

it.effect("lends nothing for a slot that already expired", () =>
  Effect.gen(function* () {
    const { admission } = yield* makeHarness(1);
    const hung = yield* reserve(admission);
    yield* TestClock.adjust(EffectAdmission.SLOT_MAX_HOLD);
    const next = yield* reserve(admission);
    assert.isDefined(next);
    // The hung effect's slot no longer counts: lending it would free nothing and its return would
    // add a slot nobody gave back.
    yield* hung!.suspend;
    yield* hung!.resume;
    assert.equal((yield* admission.state).inFlight, 1);
    assert.isUndefined(yield* reserve(admission));
    yield* hung!.release;
    yield* next!.release;
    assert.equal((yield* admission.state).inFlight, 0);
  }),
);

it.effect("counts a returning effect as lent until it holds a slot again", () =>
  Effect.gen(function* () {
    // At one slot: A lends its slot and B takes it. A's return is held at its clock read, where it
    // can yield; until it holds a slot it still counts as lent, so B keeps its slot when it waits on
    // the lane and no third effect is admitted.
    const { admission } = yield* makeHarness(1);
    const a = yield* reserve(admission);
    yield* a!.suspend;
    const b = yield* reserve(admission);
    const clock = yield* Clock.clockWith(Effect.succeed);
    const reading = yield* Deferred.make<void>();
    const proceed = yield* Deferred.make<void>();
    const heldAtRead: Clock.Clock = {
      currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe(),
      currentTimeMillis: Deferred.succeed(reading, undefined).pipe(
        Effect.andThen(Deferred.await(proceed)),
        Effect.andThen(clock.currentTimeMillis),
      ),
      currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
      currentTimeNanos: clock.currentTimeNanos,
      monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
      monotonicTimeNanos: clock.monotonicTimeNanos,
      sleep: (duration) => clock.sleep(duration),
    };
    const returning = yield* a!.resume.pipe(
      Effect.provideService(Clock.Clock, heldAtRead),
      Effect.forkChild,
    );
    yield* Deferred.await(reading);
    yield* b!.suspend;
    const c = yield* reserve(admission);
    yield* Deferred.succeed(proceed, undefined);
    yield* Fiber.join(returning);
    assert.isUndefined(c, "a third gated effect was admitted while the first one returned");
    assert.equal((yield* admission.state).inFlight, 2);
    yield* Effect.forEach([a, b, c], (slot) => slot?.release ?? Effect.void);
    assert.equal((yield* admission.state).inFlight, 0);
  }),
);

import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

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

    yield* first!;
    assert.deepEqual(yield* admission.excludedTypes, []);
    assert.isDefined(yield* reserve(admission));
    assert.isUndefined(yield* reserve(admission));
    yield* second!;
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
      yield* second!;
      assert.isFalse(yield* sampling(admission));
      assert.equal(yield* Ref.get(released), 1);
      yield* first!;
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

it.effect("reopens only below half the closing mark, not at or above it", () =>
  Effect.gen(function* () {
    const { admission, pressure } = yield* makeHarness(2);
    yield* Ref.set(pressure, 60);
    const first = yield* reserve(admission);
    yield* TestClock.adjust("2 seconds");
    assert.equal((yield* admission.state).saturated, true);

    // Exactly the reopen mark and anything between the marks keep the gate closed.
    for (const held of [20, 22, 24, 39]) {
      yield* Ref.set(pressure, held);
      yield* TestClock.adjust("2 seconds");
      assert.equal((yield* admission.state).saturated, true, `pressure ${held}`);
    }
    // Just under half reopens, even though it is well above a third of the mark.
    yield* Ref.set(pressure, 19);
    yield* TestClock.adjust("2 seconds");
    assert.equal((yield* admission.state).saturated, false);
    yield* first!;
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
      yield* first!;
      assert.isUndefined(yield* reserve(admission));
      assert.equal(yield* Ref.get(freed), 0);
      // The next one frees a slot below the allowance, which wakes one worker.
      yield* second!;
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
    yield* first!;
    assert.equal(yield* Ref.get(freed), 1);
    // A release while a slot was already free wakes nobody: nothing was being skipped.
    yield* second!;
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
    yield* first!;
    yield* second!;
    assert.equal((yield* admission.state).inFlight, 1);
    yield* third!;
    assert.equal((yield* admission.state).inFlight, 0);
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
    yield* first!;
    yield* second!;
  }),
);

it.effect("sizes the saturated allowance, the reopen mark and the pressure reading", () =>
  Effect.sync(() => {
    assert.equal(EffectAdmission.saturatedAllowance(1), 1);
    assert.equal(EffectAdmission.saturatedAllowance(2), 1);
    assert.equal(EffectAdmission.saturatedAllowance(4), 2);
    assert.equal(EffectAdmission.saturatedAllowance(5), 3);
    assert.equal(EffectAdmission.openMarkFor(40), 20);
    assert.equal(EffectAdmission.openMarkFor(5), 2.5);
    assert.isTrue(EffectAdmission.isGated("provider-turn.start"));
    assert.isTrue(EffectAdmission.isGated("checkpoint.capture"));
    assert.isFalse(EffectAdmission.isGated("provider-turn.interrupt"));
    assert.isFalse(EffectAdmission.isGated("runtime-request.respond"));
    // The histogram drops the first gap after a reset: a block spanning the whole interval reads
    // as calm there, so the sampler's own lateness stands in.
    assert.equal(EffectAdmission.latencyPressure(0, 200), 200);
    assert.equal(EffectAdmission.latencyPressure(45, 3), 45);
    assert.equal(EffectAdmission.latencyPressure(Number.NaN, -2), 0);
    assert.equal(Duration.toMillis(EffectAdmission.SLOT_MAX_HOLD), 120_000);
  }),
);

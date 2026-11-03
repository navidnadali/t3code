import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

import * as EffectAdmission from "./EffectAdmission.ts";

const makeAdmission = (
  workers: number,
  pressure: Ref.Ref<number>,
  restored: Ref.Ref<number>,
  closeAbove = 40,
) =>
  EffectAdmission.make({
    workers,
    closeAbove,
    samplesToSwitch: 2,
    sampleInterval: Duration.seconds(1),
    pressure: Ref.get(pressure),
    onAllowanceRestored: Ref.update(restored, (count) => count + 1),
  });

/** Takes a slot, or none, and returns what gives it back. */
const reserve = (admission: EffectAdmission.EffectAdmissionShape) =>
  admission.reserve.pipe(Effect.map(Option.getOrUndefined));

it.effect("hands out one slot per worker and excludes nothing while a slot is free", () =>
  Effect.gen(function* () {
    const pressure = yield* Ref.make(5);
    const restored = yield* Ref.make(0);
    const admission = yield* makeAdmission(2, pressure, restored);

    assert.deepEqual(yield* admission.excludedTypes, []);
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    assert.isDefined(first);
    assert.isDefined(second);
    assert.isUndefined(yield* reserve(admission));
    assert.deepEqual(yield* admission.excludedTypes, EffectAdmission.GATED_EFFECT_TYPES);
    assert.deepEqual(yield* admission.state, { allowed: 2, inFlight: 2, saturated: false });

    yield* first!;
    assert.deepEqual(yield* admission.excludedTypes, []);
    assert.isDefined(yield* reserve(admission));
    assert.isUndefined(yield* reserve(admission));
    yield* second!;
    assert.deepEqual(yield* admission.state, { allowed: 2, inFlight: 1, saturated: false });
  }),
);

it.effect(
  "halves the allowance after two saturated samples and restores it after two calm ones",
  () =>
    Effect.gen(function* () {
      const pressure = yield* Ref.make(60);
      const restored = yield* Ref.make(0);
      const admission = yield* makeAdmission(4, pressure, restored);

      // One high sample switches nothing.
      yield* TestClock.adjust("1 second");
      assert.deepEqual(yield* admission.state, { allowed: 4, inFlight: 0, saturated: false });
      yield* TestClock.adjust("1 second");
      assert.deepEqual(yield* admission.state, { allowed: 2, inFlight: 0, saturated: true });

      const first = yield* reserve(admission);
      const second = yield* reserve(admission);
      assert.isDefined(first);
      assert.isDefined(second);
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
      assert.deepEqual(yield* admission.state, { allowed: 4, inFlight: 2, saturated: false });
      assert.equal(yield* Ref.get(restored), 1);
      assert.deepEqual(yield* admission.excludedTypes, []);
      assert.isDefined(yield* reserve(admission));
      assert.isDefined(yield* reserve(admission));
      assert.isUndefined(yield* reserve(admission));
    }),
);

it.effect(
  "keeps running effects when the allowance shrinks and excludes new ones until they end",
  () =>
    Effect.gen(function* () {
      const pressure = yield* Ref.make(5);
      const restored = yield* Ref.make(0);
      const admission = yield* makeAdmission(2, pressure, restored);
      const first = yield* reserve(admission);
      const second = yield* reserve(admission);
      assert.isDefined(first);
      assert.isDefined(second);

      yield* Ref.set(pressure, 60);
      yield* TestClock.adjust("2 seconds");
      assert.deepEqual(yield* admission.state, { allowed: 1, inFlight: 2, saturated: true });
      assert.deepEqual(yield* admission.excludedTypes, EffectAdmission.GATED_EFFECT_TYPES);

      // One slot given back still leaves two in flight above the allowance of one.
      yield* first!;
      assert.isUndefined(yield* reserve(admission));
      yield* second!;
      assert.deepEqual(yield* admission.excludedTypes, []);
      assert.isDefined(yield* reserve(admission));
      assert.isUndefined(yield* reserve(admission));
    }),
);

it.effect("recovers from saturation at a small mark too", () =>
  Effect.gen(function* () {
    const pressure = yield* Ref.make(5);
    const restored = yield* Ref.make(0);
    const admission = yield* makeAdmission(4, pressure, restored, 5);

    // Pressure 5 is at the mark; the gate closes and reopens once the loop is idle.
    yield* TestClock.adjust("2 seconds");
    assert.equal((yield* admission.state).saturated, true);
    yield* Ref.set(pressure, 0);
    yield* TestClock.adjust("2 seconds");
    assert.deepEqual(yield* admission.state, { allowed: 4, inFlight: 0, saturated: false });
    assert.equal(yield* Ref.get(restored), 1);
  }),
);

it.effect("sizes the saturated allowance and the reopen mark", () =>
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
  }),
);

it.effect("stops counting a slot held longer than a turn start may take", () =>
  Effect.gen(function* () {
    const pressure = yield* Ref.make(5);
    const restored = yield* Ref.make(0);
    const admission = yield* makeAdmission(2, pressure, restored);
    const first = yield* reserve(admission);
    const second = yield* reserve(admission);
    assert.isDefined(first);
    assert.isDefined(second);
    assert.isUndefined(yield* reserve(admission));

    // Two hung starts must not hold every slot for good: after the bound a new start may run.
    yield* TestClock.adjust("120 seconds");
    assert.deepEqual(yield* admission.excludedTypes, []);
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

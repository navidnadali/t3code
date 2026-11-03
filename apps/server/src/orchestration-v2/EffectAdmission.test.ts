import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

import * as EffectAdmission from "./EffectAdmission.ts";

const makeAdmission = (workers: number, utilization: Ref.Ref<number>) =>
  EffectAdmission.make({
    workers,
    utilizationHighMark: 0.95,
    utilizationLowMark: 0.85,
    samplesToSwitch: 2,
    sampleInterval: Duration.seconds(1),
    utilization: Ref.get(utilization),
  });

/** Admits a gated effect in a forked fiber that holds its slot until `release` is done. */
const hold = (
  admission: EffectAdmission.EffectAdmissionShape,
  effectType: string,
  started: Ref.Ref<number>,
  release: Deferred.Deferred<void>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const receipt = yield* admission.admit(effectType);
      yield* Ref.update(started, (count) => count + 1);
      yield* Deferred.await(release);
      return receipt;
    }),
  ).pipe(Effect.forkChild);

it.effect("admits gated effects up to the worker count and the next one when a slot frees", () =>
  Effect.gen(function* () {
    const utilization = yield* Ref.make(0.2);
    const admission = yield* makeAdmission(2, utilization);
    const started = yield* Ref.make(0);
    const first = yield* Deferred.make<void>();
    const second = yield* Deferred.make<void>();
    const third = yield* Deferred.make<void>();

    const one = yield* hold(admission, "provider-turn.start", started, first);
    const two = yield* hold(admission, "checkpoint.capture", started, second);
    const three = yield* hold(admission, "provider-turn.start", started, third);
    yield* Effect.yieldNow;

    assert.equal(yield* Ref.get(started), 2);
    assert.deepEqual(yield* admission.state, {
      allowed: 2,
      inFlight: 2,
      waiting: 1,
      saturated: false,
    });

    yield* Deferred.succeed(first, undefined);
    assert.equal(yield* Fiber.join(one).pipe(Effect.map((receipt) => receipt.gated)), true);
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(started), 3);
    assert.deepEqual(yield* admission.state, {
      allowed: 2,
      inFlight: 2,
      waiting: 0,
      saturated: false,
    });

    yield* Deferred.succeed(second, undefined);
    yield* Deferred.succeed(third, undefined);
    yield* Fiber.join(two);
    yield* Fiber.join(three);
    assert.deepEqual(yield* admission.state, {
      allowed: 2,
      inFlight: 0,
      waiting: 0,
      saturated: false,
    });
  }),
);

it.effect("never holds an interrupt, a runtime answer or a cleanup, even while full", () =>
  Effect.gen(function* () {
    const utilization = yield* Ref.make(0.2);
    const admission = yield* makeAdmission(1, utilization);
    const started = yield* Ref.make(0);
    const release = yield* Deferred.make<void>();
    const holder = yield* hold(admission, "provider-turn.start", started, release);
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(started), 1);

    for (const effectType of [
      "provider-turn.interrupt",
      "runtime-request.respond",
      "provider-session.detach",
      "terminal.cleanup",
    ]) {
      const receipt = yield* Effect.scoped(admission.admit(effectType));
      assert.deepEqual(receipt, { gated: false, waitedMs: 0 });
    }
    assert.equal((yield* admission.state).inFlight, 1);

    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(holder);
  }),
);

it.effect(
  "halves the allowance after two saturated samples and restores it after two calm ones",
  () =>
    Effect.gen(function* () {
      const utilization = yield* Ref.make(0.99);
      const admission = yield* makeAdmission(4, utilization);

      // One high sample switches nothing.
      yield* TestClock.adjust("1 second");
      assert.deepEqual(yield* admission.state, {
        allowed: 4,
        inFlight: 0,
        waiting: 0,
        saturated: false,
      });
      yield* TestClock.adjust("1 second");
      assert.deepEqual(yield* admission.state, {
        allowed: 2,
        inFlight: 0,
        waiting: 0,
        saturated: true,
      });

      const started = yield* Ref.make(0);
      // Forked from the test fiber itself: a fork inside forEach dies with that iteration.
      const releases = [
        yield* Deferred.make<void>(),
        yield* Deferred.make<void>(),
        yield* Deferred.make<void>(),
      ];
      const fibers = [
        yield* hold(admission, "provider-turn.start", started, releases[0]!),
        yield* hold(admission, "provider-turn.start", started, releases[1]!),
        yield* hold(admission, "provider-turn.start", started, releases[2]!),
      ];
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(started), 2);
      assert.equal((yield* admission.state).waiting, 1);

      // Between the marks nothing changes; one calm sample is not enough either.
      yield* Ref.set(utilization, 0.9);
      yield* TestClock.adjust("2 seconds");
      assert.equal((yield* admission.state).saturated, true);
      yield* Ref.set(utilization, 0.5);
      yield* TestClock.adjust("1 second");
      assert.equal((yield* admission.state).saturated, true);
      assert.equal(yield* Ref.get(started), 2);

      // The second calm sample restores the allowance and admits the waiter.
      yield* TestClock.adjust("1 second");
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(started), 3);
      assert.deepEqual(yield* admission.state, {
        allowed: 4,
        inFlight: 3,
        waiting: 0,
        saturated: false,
      });

      yield* Effect.forEach(releases, (release) => Deferred.succeed(release, undefined));
      const receipts = yield* Effect.forEach(fibers, Fiber.join);
      assert.deepEqual(
        receipts.map((receipt) => receipt.gated),
        [true, true, true],
      );
      // The waiter was held through the four seconds of samples before the allowance returned.
      assert.equal(receipts[2]?.waitedMs, 4000);
    }),
);

it.effect("lets a running effect finish when the allowance shrinks and holds only new ones", () =>
  Effect.gen(function* () {
    const utilization = yield* Ref.make(0.2);
    const admission = yield* makeAdmission(2, utilization);
    const started = yield* Ref.make(0);
    const first = yield* Deferred.make<void>();
    const second = yield* Deferred.make<void>();
    const third = yield* Deferred.make<void>();
    const one = yield* hold(admission, "provider-turn.start", started, first);
    const two = yield* hold(admission, "provider-turn.start", started, second);
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(started), 2);

    yield* Ref.set(utilization, 0.99);
    yield* TestClock.adjust("2 seconds");
    assert.deepEqual(yield* admission.state, {
      allowed: 1,
      inFlight: 2,
      waiting: 0,
      saturated: true,
    });

    const three = yield* hold(admission, "provider-turn.start", started, third);
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(started), 2);
    // One slot freed still leaves two in flight above the allowance of one.
    yield* Deferred.succeed(first, undefined);
    yield* Fiber.join(one);
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(started), 2);
    yield* Deferred.succeed(second, undefined);
    yield* Fiber.join(two);
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(started), 3);

    yield* Deferred.succeed(third, undefined);
    yield* Fiber.join(three);
    assert.equal((yield* admission.state).inFlight, 0);
  }),
);

it.effect("drops an interrupted waiter and hands the slot to the next one", () =>
  Effect.gen(function* () {
    const utilization = yield* Ref.make(0.2);
    const admission = yield* makeAdmission(1, utilization);
    const started = yield* Ref.make(0);
    const first = yield* Deferred.make<void>();
    const second = yield* Deferred.make<void>();
    const third = yield* Deferred.make<void>();
    const one = yield* hold(admission, "provider-turn.start", started, first);
    const two = yield* hold(admission, "provider-turn.start", started, second);
    const three = yield* hold(admission, "provider-turn.start", started, third);
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(started), 1);
    assert.equal((yield* admission.state).waiting, 2);

    yield* Fiber.interrupt(two);
    assert.equal((yield* admission.state).waiting, 1);

    yield* Deferred.succeed(first, undefined);
    yield* Fiber.join(one);
    yield* Effect.yieldNow;
    assert.equal(yield* Ref.get(started), 2);
    assert.deepEqual(yield* admission.state, {
      allowed: 1,
      inFlight: 1,
      waiting: 0,
      saturated: false,
    });

    yield* Deferred.succeed(third, undefined);
    yield* Fiber.join(three);
    assert.equal((yield* admission.state).inFlight, 0);
  }),
);

it.effect("saturatedAllowance keeps at least one worker and half of the rest", () =>
  Effect.sync(() => {
    assert.equal(EffectAdmission.saturatedAllowance(1), 1);
    assert.equal(EffectAdmission.saturatedAllowance(2), 1);
    assert.equal(EffectAdmission.saturatedAllowance(4), 2);
    assert.equal(EffectAdmission.saturatedAllowance(5), 3);
  }),
);

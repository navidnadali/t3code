import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";

import type { OrchestrationEffectRequestV2 } from "./EffectOutbox.ts";

/** Effects that start turn work on the main thread; the rest answer users or free resources. */
export const GATED_EFFECT_TYPES: ReadonlyArray<OrchestrationEffectRequestV2["type"]> = [
  "provider-turn.start",
  "provider-runtime.continue",
  "checkpoint.capture",
  "thread-title.generate",
];

export const isGated = (effectType: string): boolean =>
  (GATED_EFFECT_TYPES as ReadonlyArray<string>).includes(effectType);

/** A slot counts for this long at most, longer than any turn start, so hung starts free the allowance. */
export const SLOT_MAX_HOLD = Duration.minutes(2);

export interface AdmissionState {
  readonly allowed: number;
  readonly inFlight: number;
  readonly saturated: boolean;
  /** Whether the pressure source is running: only while the gate can act. */
  readonly sampling: boolean;
}

export interface EffectAdmissionShape {
  /** Effect types a claim must skip right now: the gated ones while the allowance is used up. */
  readonly excludedTypes: Effect.Effect<ReadonlyArray<string>>;
  /** A slot for one gated effect if one is free; the effect it carries gives the slot back. */
  readonly reserve: Effect.Effect<Option.Option<Effect.Effect<void>>>;
  readonly state: Effect.Effect<AdmissionState>;
}

const ungated: EffectAdmissionShape = {
  excludedTypes: Effect.succeed([]),
  reserve: Effect.succeedSome(Effect.void),
  state: Effect.succeed({ allowed: Infinity, inFlight: 0, saturated: false, sampling: false }),
};

/** Admission the effect worker consults before a claim; the default admits everything. */
export const EffectAdmission = Context.Reference<EffectAdmissionShape>(
  "t3/orchestration-v2/EffectAdmission",
  { defaultValue: () => ungated },
);

export interface EffectAdmissionOptions {
  /** Gated effects in flight at most while the main thread has headroom. */
  readonly workers: number;
  /** Pressure at or above which the main thread counts as saturated; it reopens below half of it. */
  readonly closeAbove: number;
  /** Consecutive samples on one side of a mark before the state switches. */
  readonly samplesToSwitch: number;
  readonly sampleInterval: Duration.Duration;
  /** Acquires the pressure source for as long as the gate can act and yields one sample per call. */
  readonly pressure: Effect.Effect<Effect.Effect<number>, never, Scope.Scope>;
  /** Runs when the full allowance returns, so idle workers claim the effects they skipped. */
  readonly onAllowanceRestored: Effect.Effect<void>;
  /** Runs when a slot frees while gated effects were being skipped, so one idle worker claims. */
  readonly onSlotFreed: Effect.Effect<void>;
}

/** Gated effects allowed while saturated: half the workers, at least one. */
export const saturatedAllowance = (workers: number): number => Math.max(1, Math.ceil(workers / 2));

/** Headroom counts as back at half the closing pressure, so every mark can reopen. */
export const openMarkFor = (closeAbove: number): number => closeAbove / 2;

/**
 * The loop's delay over the last interval: the histogram's p99, or the sampler's own lateness
 * when it is larger — the histogram drops the first gap after a reset, so a block that spans a
 * whole interval would otherwise read as calm.
 */
export const latencyPressure = (p99Ms: number, latenessMs: number): number =>
  Math.max(Number.isFinite(p99Ms) ? p99Ms : 0, Number.isFinite(latenessMs) ? latenessMs : 0, 0);

interface Slot {
  readonly takenAtMs: number;
}

/**
 * Gated effects run `workers` at a time; after `samplesToSwitch` samples at or above `closeAbove`
 * only `saturatedAllowance` do, until as many fall below half of it. The pressure source runs only
 * while the gate can act, so an idle server pays nothing; a slot counts for `SLOT_MAX_HOLD` at most.
 */
export const make = Effect.fn("EffectAdmission.make")(function* (options: EffectAdmissionOptions) {
  const workers = Math.max(1, Math.floor(options.workers));
  const whenSaturated = saturatedAllowance(workers);
  const openBelow = openMarkFor(options.closeAbove);
  const holdMs = Duration.toMillis(SLOT_MAX_HOLD);
  const layerScope = yield* Effect.scope;
  let allowed = workers;
  let saturated = false;
  let highRun = 0;
  let lowRun = 0;
  let slots: Array<Slot> = [];
  let sampler: { readonly fiber: Fiber.Fiber<void>; readonly scope: Scope.Closeable } | undefined;
  let armed = false;
  const isArmed = () => armed;

  const prune = (nowMs: number) => {
    const before = slots.length;
    slots = slots.filter((slot) => nowMs - slot.takenAtMs < holdMs);
    return before >= allowed && slots.length < allowed;
  };
  const full = () => slots.length >= allowed;
  const canAct = () => saturated || slots.length >= whenSaturated;

  const disarm = Effect.suspend(() => {
    const running = sampler;
    sampler = undefined;
    armed = false;
    highRun = 0;
    lowRun = 0;
    return running === undefined
      ? Effect.void
      : Fiber.interrupt(running.fiber).pipe(
          Effect.andThen(Scope.close(running.scope, Exit.void)),
          Effect.asVoid,
        );
  });

  // Untraced: one sample per second while armed, inside the daemon's own scope.
  const sample = Effect.fnUntraced(function* (next: Effect.Effect<number>) {
    {
      const nowMs = yield* Clock.currentTimeMillis;
      if (prune(nowMs)) yield* options.onSlotFreed;
      const pressure = yield* next;
      if (pressure >= options.closeAbove) {
        highRun += 1;
        lowRun = 0;
      } else if (pressure < openBelow) {
        lowRun += 1;
        highRun = 0;
      } else {
        highRun = 0;
        lowRun = 0;
      }
      if (!saturated && highRun >= options.samplesToSwitch) {
        saturated = true;
        allowed = whenSaturated;
      } else if (saturated && lowRun >= options.samplesToSwitch) {
        saturated = false;
        allowed = workers;
      } else {
        return;
      }
      yield* Effect.logInfo("Orchestration effect admission changed", {
        saturated,
        allowed,
        pressure,
      });
      if (!saturated) {
        yield* options.onAllowanceRestored;
        if (!canAct()) armed = false;
      }
    }
  });

  const arm = Effect.gen(function* () {
    if (sampler !== undefined) return;
    armed = true;
    const scope = yield* Scope.make();
    const next = yield* options.pressure.pipe(Effect.provideService(Scope.Scope, scope));
    const loop = Effect.gen(function* () {
      while (isArmed()) {
        yield* Effect.sleep(options.sampleInterval);
        if (!isArmed()) break;
        yield* sample(next);
      }
    }).pipe(
      Effect.ensuring(
        Effect.suspend(() => {
          if (sampler?.scope === scope) sampler = undefined;
          return Scope.close(scope, Exit.void);
        }),
      ),
    );
    const fiber = yield* Effect.forkIn(loop, layerScope);
    sampler = { fiber, scope };
  });

  const release = Effect.fnUntraced(function* (slot: Slot) {
    const index = slots.indexOf(slot);
    if (index === -1) return;
    const wasFull = full();
    slots.splice(index, 1);
    if (wasFull && !full()) yield* options.onSlotFreed;
    if (!canAct() && sampler !== undefined) yield* disarm;
  });

  const reserve: EffectAdmissionShape["reserve"] = Effect.gen(function* () {
    const nowMs = yield* Clock.currentTimeMillis;
    if (prune(nowMs)) yield* options.onSlotFreed;
    if (full()) return Option.none();
    const slot: Slot = { takenAtMs: nowMs };
    slots.push(slot);
    if (canAct() && sampler === undefined) yield* arm;
    return Option.some(release(slot));
  });

  return {
    excludedTypes: Effect.gen(function* () {
      const nowMs = yield* Clock.currentTimeMillis;
      if (prune(nowMs)) yield* options.onSlotFreed;
      return full() ? GATED_EFFECT_TYPES : [];
    }),
    reserve,
    state: Effect.sync(() => ({
      allowed,
      inFlight: slots.length,
      saturated,
      sampling: sampler !== undefined,
    })),
  } satisfies EffectAdmissionShape;
});

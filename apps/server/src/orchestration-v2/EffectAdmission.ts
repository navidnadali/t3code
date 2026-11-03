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

/**
 * A slot counts for this long at most. A safety valve against hung starts, not a tuning value:
 * production turn starts run about 92 s on average, so the bound sits well past their slow tail —
 * a slot that expired would let the gate drift back towards the worker count.
 */
export const SLOT_MAX_HOLD = Duration.minutes(10);

export interface AdmissionState {
  readonly allowed: number;
  readonly inFlight: number;
  readonly saturated: boolean;
  /** Whether the pressure source is running: only while the gate can act. */
  readonly sampling: boolean;
}

export interface AdmissionSlot {
  /**
   * Gives the slot back after its claim took work — a gated effect that ran, or an ungated one
   * that needs no slot; the last slot freed wakes a worker that skipped gated effects meanwhile.
   */
  readonly release: Effect.Effect<void>;
  /** Gives the slot back unused, because the claim found nothing: no worker is woken for it. */
  readonly discard: Effect.Effect<void>;
}

export interface EffectAdmissionShape {
  /** Effect types a claim must skip right now: the gated ones while the allowance is used up. */
  readonly excludedTypes: Effect.Effect<ReadonlyArray<string>>;
  /** A slot for one gated effect if one is free. */
  readonly reserve: Effect.Effect<Option.Option<AdmissionSlot>>;
  readonly state: Effect.Effect<AdmissionState>;
}

const ungated: EffectAdmissionShape = {
  excludedTypes: Effect.succeed([]),
  reserve: Effect.succeedSome({ release: Effect.void, discard: Effect.void }),
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
  /** Pressure at or above which the main thread counts as saturated; it reopens below three quarters of it. */
  readonly closeAbove: number;
  /** Consecutive samples on one side of a mark before the state switches. */
  readonly samplesToSwitch: number;
  readonly sampleInterval: Duration.Duration;
  /** Acquires the pressure source for as long as the gate can act and yields one sample per call. */
  readonly pressure: Effect.Effect<Effect.Effect<number>, never, Scope.Scope>;
  /** Runs when the full allowance returns, so idle workers claim the effects they skipped. */
  readonly onAllowanceRestored: Effect.Effect<void>;
  /** Runs when a finished or expired slot frees the used-up allowance, so one idle worker claims. */
  readonly onSlotFreed: Effect.Effect<void>;
}

/** Gated effects allowed while saturated: half the workers, at least one. */
export const saturatedAllowance = (workers: number): number => Math.max(1, Math.ceil(workers / 2));

/**
 * Headroom counts as back below three quarters of the closing pressure. Replayed on the recorded
 * 1 Hz loop-delay series: at light load a spike decays below that within a few seconds, while a
 * saturated loop hovers around the mark and stays above it — half the mark held a spike-closed gate
 * for a minute at 250 sessions, and higher fractions, a bounded spell or the p50 flap at 80.
 */
export const REOPEN_FRACTION = 0.75;

export const openMarkFor = (closeAbove: number): number => closeAbove * REOPEN_FRACTION;

/**
 * A second at 10 ms resolution records about a hundred histogram samples; fewer than this many
 * means a block spanned most of its ticks, and the histogram, which drops the first gap after a
 * reset, would read the second as calm.
 */
export const MIN_HISTOGRAM_SAMPLES = 10;

/** The sampler's own lateness: how far past its interval the sample came. */
export const samplerLateness = (nowMs: number, previousMs: number, intervalMs: number): number =>
  nowMs - previousMs - intervalMs;

/**
 * The loop's delay over the last interval: the histogram's p99, or, when the histogram holds
 * fewer than `MIN_HISTOGRAM_SAMPLES`, the sampler's own lateness if that is larger.
 */
export const latencyPressure = (p99Ms: number, latenessMs: number, samples: number): number => {
  const p99 = Number.isFinite(p99Ms) ? Math.max(p99Ms, 0) : 0;
  if (samples >= MIN_HISTOGRAM_SAMPLES) return p99;
  return Math.max(p99, Number.isFinite(latenessMs) ? latenessMs : 0);
};

/** One reading of the event loop delay source, from its histogram and the sampler's clock. */
export const histogramReading = (reading: {
  readonly p99Ns: number;
  readonly samples: number;
  readonly nowMs: number;
  readonly previousMs: number;
  readonly intervalMs: number;
}): number =>
  latencyPressure(
    reading.p99Ns / 1e6,
    samplerLateness(reading.nowMs, reading.previousMs, reading.intervalMs),
    reading.samples,
  );

interface Slot {
  readonly takenAtMs: number;
}

interface Sampler {
  readonly scope: Scope.Closeable;
  /** Cleared when this sampler is stopped, so a successor's run does not keep it sampling. */
  active: boolean;
  fiber?: Fiber.Fiber<void>;
}

/**
 * Gated effects run `workers` at a time; after `samplesToSwitch` samples at or above `closeAbove`
 * only `saturatedAllowance` do, until as many fall below three quarters of it. The pressure source runs only
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
  let sampler: Sampler | undefined;

  const prune = (nowMs: number) => {
    const before = slots.length;
    slots = slots.filter((slot) => nowMs - slot.takenAtMs < holdMs);
    return before >= allowed && slots.length < allowed;
  };
  const full = () => slots.length >= allowed;
  const canAct = () => saturated || slots.length >= whenSaturated;

  // Stops `current` from inside its own run: the loop ends and its finalizer closes the scope.
  const stop = (current: Sampler) => {
    current.active = false;
    if (sampler === current) sampler = undefined;
    highRun = 0;
    lowRun = 0;
  };

  const disarm = Effect.suspend(() => {
    const running = sampler;
    if (running === undefined) return Effect.void;
    stop(running);
    return (running.fiber === undefined ? Effect.void : Fiber.interrupt(running.fiber)).pipe(
      Effect.andThen(Scope.close(running.scope, Exit.void)),
      Effect.asVoid,
    );
  });

  // Untraced: one sample per second while armed, inside the daemon's own scope.
  const sample = Effect.fnUntraced(function* (current: Sampler, next: Effect.Effect<number>) {
    {
      const nowMs = yield* Clock.currentTimeMillis;
      if (prune(nowMs)) yield* options.onSlotFreed;
      if (!canAct()) return stop(current);
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
        if (!canAct()) stop(current);
      }
    }
  });

  const arm = Effect.gen(function* () {
    if (sampler !== undefined) return;
    // Claimed before the first yield: a settle that runs meanwhile must not arm a second one.
    const scope = Scope.makeUnsafe();
    const current: Sampler = { scope, active: true };
    sampler = current;
    const next = yield* options.pressure.pipe(Effect.provideService(Scope.Scope, scope));
    const isActive = () => current.active;
    // A sample runs to its end: a stop requested meanwhile lands after its wake-ups went out.
    const loop = Effect.gen(function* () {
      while (isActive()) {
        yield* Effect.sleep(options.sampleInterval);
        if (!isActive()) break;
        yield* Effect.uninterruptible(sample(current, next));
      }
    }).pipe(
      Effect.ensuring(
        Effect.suspend(() => {
          if (sampler === current) sampler = undefined;
          return Scope.close(scope, Exit.void);
        }),
      ),
    );
    current.fiber = yield* Effect.forkIn(loop, layerScope);
  });

  // The sampler runs exactly while the gate can act; called after every change to the slots.
  const settle = Effect.suspend(() =>
    canAct() ? (sampler === undefined ? arm : Effect.void) : disarm,
  );

  const giveBack = Effect.fnUntraced(function* (slot: Slot, ran: boolean) {
    const index = slots.indexOf(slot);
    if (index === -1) return;
    const wasFull = full();
    slots.splice(index, 1);
    if (ran && wasFull && !full()) yield* options.onSlotFreed;
    yield* settle;
  });

  const reserve: EffectAdmissionShape["reserve"] = Effect.gen(function* () {
    const nowMs = yield* Clock.currentTimeMillis;
    if (prune(nowMs)) yield* options.onSlotFreed;
    if (full()) {
      yield* settle;
      return Option.none();
    }
    const slot: Slot = { takenAtMs: nowMs };
    slots.push(slot);
    yield* settle;
    return Option.some({ release: giveBack(slot, true), discard: giveBack(slot, false) });
  });

  return {
    excludedTypes: Effect.gen(function* () {
      const nowMs = yield* Clock.currentTimeMillis;
      if (prune(nowMs)) yield* options.onSlotFreed;
      yield* settle;
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

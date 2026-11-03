import * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

/** Effects that start turn work on the main thread; the rest answer users or free resources. */
export const GATED_EFFECT_TYPES: ReadonlyArray<string> = [
  "provider-turn.start",
  "provider-runtime.continue",
  "checkpoint.capture",
  "thread-title.generate",
];

export const isGated = (effectType: string): boolean => GATED_EFFECT_TYPES.includes(effectType);

export interface AdmissionState {
  readonly allowed: number;
  readonly inFlight: number;
  readonly saturated: boolean;
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
  state: Effect.succeed({ allowed: Infinity, inFlight: 0, saturated: false }),
};

/** Admission the effect worker consults before a claim; the default admits everything. */
export const EffectAdmission = Context.Reference<EffectAdmissionShape>(
  "t3/orchestration-v2/EffectAdmission",
  { defaultValue: () => ungated },
);

export interface EffectAdmissionOptions {
  /** Gated effects in flight at most while the main thread has headroom. */
  readonly workers: number;
  /** Event loop utilization at or above which the main thread counts as saturated. */
  readonly utilizationHighMark: number;
  /** Consecutive samples on one side of a mark before the state switches. */
  readonly samplesToSwitch: number;
  readonly sampleInterval: Duration.Duration;
  /** Event loop utilization of the last interval, 0 to 1. */
  readonly utilization: Effect.Effect<number>;
  /** Runs when the full allowance returns, so idle workers claim the effects they skipped. */
  readonly onAllowanceRestored: Effect.Effect<void>;
}

/** Gated effects allowed while saturated: half the workers, at least one. */
export const saturatedAllowance = (workers: number): number => Math.max(1, Math.ceil(workers / 2));

/** Headroom returns a tenth below the high mark, or halfway to zero for marks under 0.2. */
export const utilizationLowMark = (highMark: number): number =>
  highMark - Math.min(0.1, highMark / 2);

/**
 * Admission keyed to the main thread's headroom. Gated effects run `workers` at a time; after
 * `samplesToSwitch` samples at or above the high mark only `saturatedAllowance` of them do, until as
 * many samples fall below the low mark. Effects already running are never stopped.
 */
export const make = Effect.fn("EffectAdmission.make")(function* (options: EffectAdmissionOptions) {
  const workers = Math.max(1, Math.floor(options.workers));
  const whenSaturated = saturatedAllowance(workers);
  const lowMark = utilizationLowMark(options.utilizationHighMark);
  let allowed = workers;
  let inFlight = 0;
  let saturated = false;
  let highRun = 0;
  let lowRun = 0;

  const release = Effect.sync(() => {
    inFlight -= 1;
  });

  const sample = Effect.gen(function* () {
    const utilization = yield* options.utilization;
    if (utilization >= options.utilizationHighMark) {
      highRun += 1;
      lowRun = 0;
    } else if (utilization < lowMark) {
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
      utilization,
    });
    if (!saturated) yield* options.onAllowanceRestored;
  });
  yield* Effect.sleep(options.sampleInterval).pipe(
    Effect.andThen(sample),
    Effect.forever,
    Effect.forkScoped,
  );

  return {
    excludedTypes: Effect.sync(() => (inFlight < allowed ? [] : GATED_EFFECT_TYPES)),
    reserve: Effect.sync(() => {
      if (inFlight >= allowed) return Option.none();
      inFlight += 1;
      return Option.some(release);
    }),
    state: Effect.sync(() => ({ allowed, inFlight, saturated })),
  } satisfies EffectAdmissionShape;
});

import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type * as Scope from "effect/Scope";

/** Effects that start turn work on the main thread; the rest answer users or free resources. */
const GATED_EFFECT_TYPES: ReadonlySet<string> = new Set([
  "provider-turn.start",
  "provider-runtime.continue",
  "checkpoint.capture",
  "thread-title.generate",
]);

export interface AdmissionReceipt {
  readonly gated: boolean;
  readonly waitedMs: number;
}

export interface AdmissionState {
  readonly allowed: number;
  readonly inFlight: number;
  readonly waiting: number;
  readonly saturated: boolean;
}

export interface EffectAdmissionShape {
  /** Holds an admission slot for a gated effect type until the scope closes; others pass at once. */
  readonly admit: (effectType: string) => Effect.Effect<AdmissionReceipt, never, Scope.Scope>;
  readonly state: Effect.Effect<AdmissionState>;
}

const ungated: EffectAdmissionShape = {
  admit: () => Effect.succeed({ gated: false, waitedMs: 0 }),
  state: Effect.succeed({ allowed: Infinity, inFlight: 0, waiting: 0, saturated: false }),
};

/** Admission the effect worker consults; the default admits everything. */
export const EffectAdmission = Context.Reference<EffectAdmissionShape>(
  "t3/orchestration-v2/EffectAdmission",
  { defaultValue: () => ungated },
);

export interface EffectAdmissionOptions {
  /** Gated effects in flight at most while the main thread has headroom. */
  readonly workers: number;
  /** Event loop utilization at or above which the main thread counts as saturated. */
  readonly utilizationHighMark: number;
  /** Utilization below which it counts as having headroom again. */
  readonly utilizationLowMark: number;
  /** Consecutive samples on one side of a mark before the state switches. */
  readonly samplesToSwitch: number;
  readonly sampleInterval: Duration.Duration;
  /** Event loop utilization of the last interval, 0 to 1. */
  readonly utilization: Effect.Effect<number>;
}

/** Gated effects allowed while saturated: half the workers, at least one. */
export const saturatedAllowance = (workers: number): number => Math.max(1, Math.ceil(workers / 2));

/**
 * Admission keyed to the main thread's headroom. Gated effects run `workers` at a time; after
 * `samplesToSwitch` samples at or above the high mark only `saturatedAllowance` of them do, until as
 * many samples fall below the low mark. Effects already running are never stopped.
 */
export const make = Effect.fn("EffectAdmission.make")(function* (options: EffectAdmissionOptions) {
  const workers = Math.max(1, Math.floor(options.workers));
  const whenSaturated = saturatedAllowance(workers);
  let allowed = workers;
  let inFlight = 0;
  let saturated = false;
  let highRun = 0;
  let lowRun = 0;
  const waiters: Array<Deferred.Deferred<void>> = [];

  // A woken waiter owns its slot from here; its await only learns of it.
  const wake = () => {
    while (inFlight < allowed && waiters.length > 0) {
      const next = waiters.shift()!;
      inFlight += 1;
      Deferred.doneUnsafe(next, Exit.void);
    }
  };
  const release = Effect.sync(() => {
    inFlight -= 1;
    wake();
  });
  // The wait is interruptible; owning a slot and registering its release are not.
  const acquire: Effect.Effect<void, never, Scope.Scope> = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      if (inFlight < allowed) {
        inFlight += 1;
      } else {
        const waiter = Deferred.makeUnsafe<void>();
        waiters.push(waiter);
        yield* restore(Deferred.await(waiter)).pipe(
          Effect.onInterrupt(() =>
            Effect.sync(() => {
              const index = waiters.indexOf(waiter);
              // Already woken: the slot was handed over and must go back.
              if (index === -1) {
                inFlight -= 1;
                wake();
              } else {
                waiters.splice(index, 1);
              }
            }),
          ),
        );
      }
      yield* Effect.addFinalizer(() => release);
    }),
  );

  const sample = Effect.gen(function* () {
    const utilization = yield* options.utilization;
    if (utilization >= options.utilizationHighMark) {
      highRun += 1;
      lowRun = 0;
    } else if (utilization < options.utilizationLowMark) {
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
      wake();
    } else {
      return;
    }
    yield* Effect.logInfo("Orchestration effect admission changed", {
      saturated,
      allowed,
      utilization,
    });
  });
  yield* Effect.sleep(options.sampleInterval).pipe(
    Effect.andThen(sample),
    Effect.forever,
    Effect.forkScoped,
  );

  const admit: EffectAdmissionShape["admit"] = (effectType) =>
    GATED_EFFECT_TYPES.has(effectType)
      ? Effect.gen(function* () {
          const queuedAtMs = yield* Clock.currentTimeMillis;
          yield* acquire;
          const admittedAtMs = yield* Clock.currentTimeMillis;
          return { gated: true, waitedMs: admittedAtMs - queuedAtMs };
        })
      : Effect.succeed({ gated: false, waitedMs: 0 });

  return {
    admit,
    state: Effect.sync(() => ({ allowed, inFlight, waiting: waiters.length, saturated })),
  };
});

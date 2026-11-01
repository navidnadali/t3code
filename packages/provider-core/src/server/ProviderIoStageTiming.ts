import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export type ProviderIoStage =
  | "framing"
  | "json"
  | "decode"
  | "logShape"
  | "logSerialize"
  | "sdkMessage"
  | "adapter"
  | "ingest"
  | "offload";

export interface ProviderIoStageTimer {
  readonly now: () => number;
  readonly record: (stage: ProviderIoStage, elapsedMs: number, bytes: number) => void;
}

export interface ProviderIoStageSummary {
  readonly provider: string;
  readonly stage: ProviderIoStage;
  readonly count: number;
  readonly totalMs: number;
  readonly maxMs: number;
  readonly bytes: number;
}

export interface ProviderIoStageRecorder {
  readonly now: () => number;
  readonly forProvider: (provider: string) => ProviderIoStageTimer;
  /** Returns the totals recorded since the previous drain and resets them. */
  readonly drain: () => ReadonlyArray<ProviderIoStageSummary>;
}

/** Timed under their own name: the built-in drivers and the ACP default; any other is `other`. */
export const PROVIDER_IO_STAGE_PROVIDERS: ReadonlySet<string> = new Set([
  "codex",
  "claudeAgent",
  "cursor",
  "grok",
  "fx",
  "opencode",
  "antigravity",
  "pi",
  "muse",
  "acpRegistry",
  "acp",
]);

interface StageTotals {
  count: number;
  totalMs: number;
  maxMs: number;
  bytes: number;
}

export function makeProviderIoStageRecorder(now: () => number): ProviderIoStageRecorder {
  const providers = new Map<
    string,
    { readonly timer: ProviderIoStageTimer; readonly stages: Map<ProviderIoStage, StageTotals> }
  >();
  return {
    now,
    forProvider: (name) => {
      const provider = PROVIDER_IO_STAGE_PROVIDERS.has(name) ? name : "other";
      const existing = providers.get(provider);
      if (existing !== undefined) return existing.timer;
      const stages = new Map<ProviderIoStage, StageTotals>();
      const timer: ProviderIoStageTimer = {
        now,
        record: (stage, elapsedMs, bytes) => {
          let totals = stages.get(stage);
          if (totals === undefined) {
            totals = { count: 0, totalMs: 0, maxMs: 0, bytes: 0 };
            stages.set(stage, totals);
          }
          totals.count += 1;
          totals.totalMs += elapsedMs;
          totals.maxMs = Math.max(totals.maxMs, elapsedMs);
          totals.bytes += bytes;
        },
      };
      providers.set(provider, { timer, stages });
      return timer;
    },
    drain: () => {
      const summaries: Array<ProviderIoStageSummary> = [];
      for (const [provider, { stages }] of providers) {
        for (const [stage, totals] of stages) {
          if (totals.count === 0) continue;
          summaries.push({ provider, stage, ...totals });
          totals.count = 0;
          totals.totalMs = 0;
          totals.maxMs = 0;
          totals.bytes = 0;
        }
      }
      return summaries;
    },
  };
}

export function providerIoStageRecorderFromEnv(
  env: Readonly<Record<string, string | undefined>>,
): ProviderIoStageRecorder | undefined {
  return env.T3CODE_PROVIDER_IO_STAGE_TIMING === "1"
    ? makeProviderIoStageRecorder(() => performance.now())
    : undefined;
}

/** Bench-only main-thread timing of provider output handling; undefined (off) unless the env knob is 1. */
export const ProviderIoStageTiming = Context.Reference<ProviderIoStageRecorder | undefined>(
  "@t3tools/provider-core/server/ProviderIoStageTiming",
  { defaultValue: () => providerIoStageRecorderFromEnv(process.env) },
);

/** The driver ACP transport timing is recorded under; the ACP adapter provides its own. */
export const ProviderIoStageDriver = Context.Reference<string>(
  "@t3tools/provider-core/server/ProviderIoStageDriver",
  { defaultValue: () => "acp" },
);

/**
 * Builds the effect where it is called, as when timing is off, and records building plus running
 * it: wall time, which is main-thread busy time only while the effect does not suspend.
 */
export const timeProviderIoStage = <A, E, R>(
  timer: ProviderIoStageTimer | undefined,
  stage: ProviderIoStage,
  bytes: number,
  makeEffect: () => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => {
  if (timer === undefined) return makeEffect();
  const buildStartedAt = timer.now();
  const effect = makeEffect();
  const buildMs = timer.now() - buildStartedAt;
  return Effect.suspend(() => {
    const startedAt = timer.now();
    return Effect.onExit(effect, () =>
      Effect.sync(() => timer.record(stage, buildMs + timer.now() - startedAt, bytes)),
    );
  });
};

/** Times the synchronous shaping a protocol logger does for provider output (incoming events). */
export const timeProviderIoLogShape = <
  Event extends { readonly direction: "incoming" | "outgoing" },
  Write,
>(
  timer: ProviderIoStageTimer | undefined,
  logger: (event: Event) => Write,
): ((event: Event) => Write) =>
  timer === undefined
    ? logger
    : (event) => {
        if (event.direction !== "incoming") return logger(event);
        const startedAt = timer.now();
        const write = logger(event);
        timer.record("logShape", timer.now() - startedAt, 0);
        return write;
      };

/** Counts one message and the bytes read since the previous count; the time is left to a profile. */
export const makeProviderIoReadCounter = (
  timer: ProviderIoStageTimer,
  stage: ProviderIoStage,
  bytesRead: () => number,
) => {
  let counted = 0;
  return () => {
    const total = bytesRead();
    timer.record(stage, 0, total - counted);
    counted = total;
  };
};

const SUMMARY_INTERVAL = "10 seconds";

const roundMs = (value: number) => Math.round(value * 1000) / 1000;

const reportSummaries = (recorder: ProviderIoStageRecorder) =>
  Effect.suspend(() =>
    Effect.forEach(
      recorder.drain(),
      (summary) =>
        Effect.void.pipe(
          Effect.withSpan("provider.io.stage", {
            root: true,
            attributes: {
              provider: summary.provider,
              stage: summary.stage,
              count: summary.count,
              totalMs: roundMs(summary.totalMs),
              maxMs: roundMs(summary.maxMs),
              bytes: summary.bytes,
            },
          }),
        ),
      { discard: true },
    ),
  );

/** One span per provider and stage per interval, never per frame. */
export const layerReporter = Layer.effectDiscard(
  Effect.gen(function* () {
    const recorder = yield* ProviderIoStageTiming;
    if (recorder === undefined) return;
    yield* Effect.addFinalizer(() => reportSummaries(recorder));
    yield* Effect.sleep(SUMMARY_INTERVAL).pipe(
      Effect.andThen(reportSummaries(recorder)),
      Effect.forever,
      Effect.forkScoped,
    );
  }),
);

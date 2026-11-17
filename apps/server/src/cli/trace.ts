import { PositiveInt } from "@t3tools/contracts";
import { SQL_LOCK_WAIT_FLOOR_MS } from "@t3tools/shared/observability";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Command, Flag } from "effect/cli";

import * as ServerConfig from "../config.ts";
import { streamTraceFileLines, toRotatedTracePaths } from "../diagnostics/TraceDiagnostics.ts";
import { resolveBaseDir } from "../os-jank.ts";
import { baseDirFlag, DurationFromString, traceFileConfig, traceMaxFilesConfig } from "./config.ts";

const decodeTraceSpanLine = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      name: Schema.String,
      durationMs: Schema.Finite,
      endTimeUnixNano: Schema.FiniteFromString,
      exit: Schema.optional(Schema.Struct({ _tag: Schema.String })),
      // OTLP errors use code 2; interrupts carry effect.fiber.interrupted, or code 1 with Interrupted before Effect 4.0.2.
      status: Schema.optional(
        Schema.Struct({
          code: Schema.optional(Schema.String),
          message: Schema.optional(Schema.String),
        }),
      ),
      attributes: Schema.optional(
        Schema.Struct({
          "effect.fiber.interrupted": Schema.optional(Schema.Unknown),
          "db.lock_wait_ms": Schema.optional(Schema.Unknown),
        }),
      ),
    }),
  ),
);

const decodeSpanSummaryLine = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      type: Schema.Literal("span-summary"),
      name: Schema.String,
      thresholdMs: Schema.Finite,
      sampleEvery: Schema.optional(Schema.Int),
      sampleBelowMs: Schema.optional(Schema.Finite),
      count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
      maxDurationMs: Schema.Finite,
      startTimeUnixNano: Schema.FiniteFromString,
      endTimeUnixNano: Schema.FiniteFromString,
    }),
  ),
);

/** A percentile that falls among summarised spans is known only to be under their threshold. */
/** `null` when the window wrote no span to rank, as when sampling left all of them out. */
type TraceLatency = number | { readonly underMs: number } | null;

export function makeTraceSpanSummary(sinceMs = -Infinity) {
  const byName = new Map<
    string,
    {
      /** Each written span's duration, and whether it could have been sampled out. */
      written: Array<{ readonly ms: number; readonly plain: boolean }>;
      interrupted: number;
      failures: number;
      summarised: number;
      summarisedMaxMs: number;
      summarisedUnderMs: number;
      /** Left out by sampling, like the written spans: counted, not ranked. */
      sampledOut: number;
      /** Statements at least this slow were always written, so they stand for no sampled one. */
      sampleBelowMs: number;
    }
  >();
  let spanCount = 0;
  let skippedLineCount = 0;
  let firstEndMs = Infinity;
  let lastEndMs = -Infinity;

  const statsInWindow = (
    name: string,
    endTimeUnixNano: number,
    startTimeUnixNano = endTimeUnixNano,
  ) => {
    const endMs = endTimeUnixNano / 1_000_000;
    // The report prints end times as dates, so skip ones outside the Date range.
    if (Option.isNone(DateTime.make(endMs))) {
      skippedLineCount += 1;
      return undefined;
    }
    if (endMs < sinceMs) return undefined;

    // A summary's spans ended from its start on; its whole count is kept, so is its whole interval.
    const startMs = Math.min(startTimeUnixNano / 1_000_000, endMs);
    firstEndMs = Math.min(firstEndMs, Option.isSome(DateTime.make(startMs)) ? startMs : endMs);
    lastEndMs = Math.max(lastEndMs, endMs);
    const stats = byName.get(name) ?? {
      written: [],
      interrupted: 0,
      failures: 0,
      summarised: 0,
      summarisedMaxMs: -Infinity,
      summarisedUnderMs: 0,
      sampledOut: 0,
      sampleBelowMs: Infinity,
    };
    byName.set(name, stats);
    return stats;
  };

  const addLine = (line: string) => {
    if (line.trim().length === 0) return;
    const span = Option.getOrUndefined(decodeTraceSpanLine(line));
    if (span === undefined) {
      const summary = Option.getOrUndefined(decodeSpanSummaryLine(line));
      if (summary === undefined) {
        skippedLineCount += 1;
        return;
      }
      const stats = statsInWindow(summary.name, summary.endTimeUnixNano, summary.startTimeUnixNano);
      if (stats === undefined) return;
      spanCount += summary.count;
      stats.summarisedMaxMs = Math.max(stats.summarisedMaxMs, summary.maxDurationMs);
      if (summary.sampleEvery !== undefined) {
        stats.sampledOut += summary.count;
        // Older records do not say: then every plain written statement stands for them.
        if (summary.sampleBelowMs !== undefined) {
          stats.sampleBelowMs = Math.min(stats.sampleBelowMs, summary.sampleBelowMs);
        }
        return;
      }
      stats.summarised += summary.count;
      stats.summarisedUnderMs = Math.max(stats.summarisedUnderMs, summary.thresholdMs);
      return;
    }
    const stats = statsInWindow(span.name, span.endTimeUnixNano);
    if (stats === undefined) return;

    spanCount += 1;
    const lockWaitMs = span.attributes?.["db.lock_wait_ms"];
    const failed = span.exit?._tag === "Failure" || span.status?.code === "2";
    stats.written.push({
      ms: span.durationMs,
      // As the tracer samples: succeeded and waited for no connection.
      plain:
        !failed &&
        span.exit?._tag !== "Interrupted" &&
        (lockWaitMs === undefined ||
          (typeof lockWaitMs === "number" && lockWaitMs < SQL_LOCK_WAIT_FLOOR_MS)),
    });
    if (
      span.exit?._tag === "Interrupted" ||
      span.attributes?.["effect.fiber.interrupted"] === true ||
      span.status?.message === "Interrupted"
    ) {
      stats.interrupted += 1;
    }
    if (failed) stats.failures += 1;
  };

  const finish = () => {
    const minutes = (lastEndMs - firstEndMs) / 60_000;
    const spans = [...byName]
      .map(([name, stats]) => {
        const { written, interrupted, failures, summarised, sampledOut, sampleBelowMs } = stats;
        const sorted = written.toSorted((left, right) => left.ms - right.ms);
        const count = sorted.length + summarised + sampledOut;
        // A written statement that could have been sampled out stands for its share of the
        // sampled-out ones; one always written (slow, failed or lock-waited) stands for itself.
        const eligible = (entry: { readonly ms: number; readonly plain: boolean }) =>
          entry.plain && entry.ms < sampleBelowMs;
        const eligibleCount = sampledOut === 0 ? 0 : sorted.filter(eligible).length;
        const sampledWeight =
          eligibleCount === 0 ? 1 : (eligibleCount + sampledOut) / eligibleCount;
        // Summarised spans sort before every written span at or above their threshold.
        const percentile = (p: number): TraceLatency => {
          if (sorted.length === 0 && summarised === 0) return null;
          const beyond = Math.ceil(p * count) - summarised;
          if (beyond <= 0) return { underMs: stats.summarisedUnderMs };
          if (sorted.length === 0) return null;
          // With no written statement to stand for the sampled-out ones, they fill the ranks below
          // the cutoff in unknown order; only the always-written statements above it are known.
          if (sampledOut > 0 && eligibleCount === 0) {
            if (!Number.isFinite(sampleBelowMs)) return null;
            const below = sorted.filter((entry) => entry.ms < sampleBelowMs).length;
            const above = beyond - below - sampledOut;
            if (above <= 0) return null;
            return sorted[below + above - 1]!.ms;
          }
          let ranked = 0;
          let value = sorted.at(-1)!.ms;
          for (const entry of sorted) {
            ranked += sampledOut > 0 && eligible(entry) ? sampledWeight : 1;
            if (ranked >= beyond - 1e-9) {
              value = entry.ms;
              break;
            }
          }
          return summarised > 0 && value < stats.summarisedUnderMs
            ? { underMs: stats.summarisedUnderMs }
            : value;
        };
        return {
          name,
          count,
          perMinute: minutes > 0 ? count / minutes : undefined,
          p50Ms: percentile(0.5),
          p90Ms: percentile(0.9),
          maxMs: Math.max(sorted.at(-1)?.ms ?? -Infinity, stats.summarisedMaxMs),
          interrupted,
          failures,
        };
      })
      .toSorted((left, right) => right.count - left.count || left.name.localeCompare(right.name));

    return { spanCount, skippedLineCount, firstEndMs, lastEndMs, minutes, spans };
  };

  return { addLine, finish };
}

const formatMs = (ms: number) =>
  Math.round(ms) < 1_000 ? `${Math.round(ms)}ms` : `${Math.round(ms / 100) / 10}s`;

const formatLatency = (latency: TraceLatency) =>
  latency === null
    ? "-"
    : typeof latency === "number"
      ? formatMs(latency)
      : `<${latency.underMs}ms`;

export function formatTraceSummary(
  summary: ReturnType<ReturnType<typeof makeTraceSpanSummary>["finish"]>,
  limit: number,
) {
  const header = ["span", "count", "/min", "p50", "p90", "max", "interrupted", "failed"];
  const rows = summary.spans
    .slice(0, limit)
    .map((span) => [
      span.name,
      String(span.count),
      span.perMinute === undefined
        ? "-"
        : span.perMinute < 0.1
          ? "<0.1"
          : span.perMinute.toFixed(1),
      formatLatency(span.p50Ms),
      formatLatency(span.p90Ms),
      formatMs(span.maxMs),
      String(span.interrupted),
      String(span.failures),
    ]);
  const table = [header, ...rows];
  const widths = header.map((_, column) => Math.max(...table.map((row) => row[column]!.length)));
  const formatIso = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
  return [
    `${summary.spanCount} spans ended from ${formatIso(summary.firstEndMs)} to ${formatIso(summary.lastEndMs)} (${summary.minutes.toFixed(1).replace(/\.0+$/, "")} min).`,
    ...(summary.skippedLineCount > 0
      ? [`Skipped ${summary.skippedLineCount} lines that are not spans.`]
      : []),
    "",
    ...table.map((row) =>
      row
        .map((cell, column) =>
          column === 0 ? cell.padEnd(widths[column]!) : cell.padStart(widths[column]!),
        )
        .join("  "),
    ),
    ...(summary.spans.length > limit
      ? ["", `${summary.spans.length - limit} more span names. Use --limit to show more.`]
      : []),
  ].join("\n");
}

const traceSummaryCommand = Command.make("summary", {
  baseDir: baseDirFlag,
  since: Flag.String("since").pipe(
    Flag.withSchema(DurationFromString),
    Flag.withDescription("Only count spans that ended in this window, for example 30m or 2h."),
    Flag.optional,
  ),
  limit: Flag.Int("limit").pipe(
    Flag.withSchema(PositiveInt),
    Flag.withDescription("Number of span names to show, busiest first."),
    Flag.withDefault(25),
  ),
}).pipe(
  Command.withDescription("Summarize the local server trace file: counts, rates, and latency."),
  Command.withHandler(
    Effect.fn("cli.trace.summary")(function* (flags) {
      const fs = yield* FileSystem.FileSystem;
      const envHome = yield* Config.String("T3CODE_HOME").pipe(Config.option);
      const baseDir = yield* resolveBaseDir(
        Option.getOrUndefined(Option.orElse(flags.baseDir, () => envHome)),
      );
      const traceFilePath =
        (yield* traceFileConfig) ??
        (yield* ServerConfig.deriveServerPaths(baseDir, undefined)).serverTracePath;
      const sinceMs = Option.isSome(flags.since)
        ? (yield* Clock.currentTimeMillis) - Duration.toMillis(flags.since.value)
        : undefined;
      const summarizer = makeTraceSpanSummary(sinceMs);
      yield* Effect.forEach(
        toRotatedTracePaths(traceFilePath, yield* traceMaxFilesConfig),
        (path) => streamTraceFileLines(fs, path, summarizer.addLine),
        { discard: true },
      );
      const summary = summarizer.finish();

      yield* Console.log(
        summary.spanCount === 0
          ? `No spans found in ${traceFilePath} or its rotated files${sinceMs === undefined ? "" : " in that window"}.${summary.skippedLineCount > 0 ? ` Skipped ${summary.skippedLineCount} lines that are not spans.` : ""}`
          : formatTraceSummary(summary, flags.limit),
      );
    }),
  ),
);

export const traceCommand = Command.make("trace").pipe(
  Command.withDescription("Inspect the local server trace file."),
  Command.withSubcommands([traceSummaryCommand]),
);

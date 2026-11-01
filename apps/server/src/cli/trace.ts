import { PositiveInt } from "@t3tools/contracts";
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
        Schema.Struct({ "effect.fiber.interrupted": Schema.optional(Schema.Unknown) }),
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
      count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
      maxDurationMs: Schema.Finite,
      startTimeUnixNano: Schema.FiniteFromString,
      endTimeUnixNano: Schema.FiniteFromString,
    }),
  ),
);

/** A percentile that falls among summarised spans is known only to be under their threshold. */
type TraceLatency = number | { readonly underMs: number };

export function makeTraceSpanSummary(sinceMs = -Infinity) {
  const byName = new Map<
    string,
    {
      durations: number[];
      interrupted: number;
      failures: number;
      summarised: number;
      summarisedMaxMs: number;
      summarisedUnderMs: number;
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
      durations: [],
      interrupted: 0,
      failures: 0,
      summarised: 0,
      summarisedMaxMs: -Infinity,
      summarisedUnderMs: 0,
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
      stats.summarised += summary.count;
      stats.summarisedMaxMs = Math.max(stats.summarisedMaxMs, summary.maxDurationMs);
      stats.summarisedUnderMs = Math.max(stats.summarisedUnderMs, summary.thresholdMs);
      return;
    }
    const stats = statsInWindow(span.name, span.endTimeUnixNano);
    if (stats === undefined) return;

    spanCount += 1;
    stats.durations.push(span.durationMs);
    if (
      span.exit?._tag === "Interrupted" ||
      span.attributes?.["effect.fiber.interrupted"] === true ||
      span.status?.message === "Interrupted"
    ) {
      stats.interrupted += 1;
    }
    if (span.exit?._tag === "Failure" || span.status?.code === "2") stats.failures += 1;
  };

  const finish = () => {
    const minutes = (lastEndMs - firstEndMs) / 60_000;
    const spans = [...byName]
      .map(([name, stats]) => {
        const { durations, interrupted, failures, summarised } = stats;
        const sorted = durations.toSorted((left, right) => left - right);
        const count = sorted.length + summarised;
        // Summarised spans sort before every written span at or above their threshold.
        const percentile = (p: number): TraceLatency => {
          const written = sorted[Math.ceil(p * count) - 1 - summarised];
          return summarised > 0 && (written === undefined || written < stats.summarisedUnderMs)
            ? { underMs: stats.summarisedUnderMs }
            : written!;
        };
        return {
          name,
          count,
          perMinute: minutes > 0 ? count / minutes : undefined,
          p50Ms: percentile(0.5),
          p90Ms: percentile(0.9),
          maxMs: Math.max(sorted.at(-1) ?? -Infinity, stats.summarisedMaxMs),
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
  ms < 1_000 ? `${Math.round(ms)}ms` : `${Math.round(ms / 100) / 10}s`;

const formatLatency = (latency: TraceLatency) =>
  typeof latency === "number" ? formatMs(latency) : `<${latency.underMs}ms`;

function formatTraceSummary(
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

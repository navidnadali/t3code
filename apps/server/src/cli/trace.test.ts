import { assert, it } from "@effect/vitest";

import { makeTraceSpanSummary } from "./trace.ts";

const MINUTE_MS = 60_000;

function span(name: string, durationMs: number, endMs: number, exitTag = "Success") {
  return JSON.stringify({
    type: "effect-span",
    name,
    traceId: "trace",
    spanId: "span",
    durationMs,
    endTimeUnixNano: String(BigInt(endMs) * 1_000_000n),
    exit: { _tag: exitTag, cause: "cause" },
  });
}

function browserSpan(
  name: string,
  status: { code: string; message?: string },
  attributes: Record<string, unknown> = {},
) {
  return JSON.stringify({
    type: "otlp-span",
    name,
    durationMs: 1,
    endTimeUnixNano: "1000000",
    status,
    attributes,
  });
}

it("drops spans that ended before the window and counts unreadable lines", () => {
  const summarizer = makeTraceSpanSummary(MINUTE_MS);
  [
    span("old", 1, 0),
    "",
    "{not json",
    JSON.stringify({ name: "no-duration" }),
    JSON.stringify({ name: "far-future", durationMs: 1, endTimeUnixNano: "9".repeat(22) }),
    span("recent", 4, 5 * MINUTE_MS),
  ].forEach(summarizer.addLine);
  const summary = summarizer.finish();

  assert.strictEqual(summary.skippedLineCount, 3);
  assert.deepStrictEqual(
    summary.spans.map((entry) => [entry.name, entry.count, entry.perMinute]),
    [["recent", 1, undefined]],
  );
});

function sqlSummary(count: number, startMs: number, endMs: number) {
  return JSON.stringify({
    type: "span-summary",
    name: "sqlite",
    thresholdMs: 2,
    count,
    totalDurationMs: count * 0.5,
    maxDurationMs: 1.5,
    startTimeUnixNano: String(BigInt(startMs) * 1_000_000n),
    endTimeUnixNano: String(BigInt(endMs) * 1_000_000n),
  });
}

it("counts summarised SQL spans over their window and a percentile among them as under it", () => {
  const rates = [
    [sqlSummary(600, 0, MINUTE_MS)],
    [sqlSummary(600, 0, MINUTE_MS), sqlSummary(600, MINUTE_MS, 2 * MINUTE_MS)],
  ].map((lines) => {
    const summarizer = makeTraceSpanSummary();
    lines.forEach(summarizer.addLine);
    return summarizer.finish().spans.map((entry) => entry.perMinute);
  });
  assert.deepStrictEqual(rates, [[600], [600]]);

  const summarizer = makeTraceSpanSummary();
  [
    sqlSummary(8, MINUTE_MS, 2 * MINUTE_MS),
    span("sqlite", 5, 2 * MINUTE_MS),
    span("sqlite", 10, 3 * MINUTE_MS),
  ].forEach(summarizer.addLine);
  const summary = summarizer.finish();

  assert.strictEqual(summary.skippedLineCount, 0);
  assert.deepStrictEqual(
    summary.spans.map((entry) => [
      entry.count,
      entry.perMinute,
      entry.p50Ms,
      entry.p90Ms,
      entry.maxMs,
    ]),
    [[10, 5, { underMs: 2 }, 5, 10]],
  );
});

it("reads failures and interrupts of browser spans from their OTLP status", () => {
  const summarizer = makeTraceSpanSummary();
  [
    browserSpan("render", { code: "2", message: "boom" }),
    browserSpan("render", { code: "0" }, { "effect.fiber.interrupted": true }),
    browserSpan("render", { code: "1", message: "Interrupted" }),
    browserSpan("render", { code: "1" }),
  ].forEach(summarizer.addLine);

  const [render] = summarizer.finish().spans;
  assert.deepStrictEqual([render?.count, render?.interrupted, render?.failures], [4, 2, 1]);
});

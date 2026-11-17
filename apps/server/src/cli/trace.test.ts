import { assert, it } from "@effect/vitest";

import { formatTraceSummary, makeTraceSpanSummary } from "./trace.ts";

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
  const rates = (
    [
      [-Infinity, [sqlSummary(600, 0, MINUTE_MS)]],
      [-Infinity, [sqlSummary(600, 0, MINUTE_MS), sqlSummary(600, MINUTE_MS, 2 * MINUTE_MS)]],
      [MINUTE_MS - 1_000, [sqlSummary(600, 0, MINUTE_MS)]],
    ] as const
  ).map(([sinceMs, lines]) => {
    const summarizer = makeTraceSpanSummary(sinceMs);
    lines.forEach(summarizer.addLine);
    return summarizer.finish().spans.map((entry) => entry.perMinute);
  });
  assert.deepStrictEqual(rates, [[600], [600], [600]]);

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

it("counts sampled-out SQL statements but takes percentiles from the written sample", () => {
  const summarizer = makeTraceSpanSummary();
  [
    JSON.stringify({
      ...JSON.parse(sqlSummary(8, MINUTE_MS, 2 * MINUTE_MS)),
      thresholdMs: 0,
      sampleEvery: 5,
    }),
    span("sqlite", 5, 2 * MINUTE_MS),
    span("sqlite", 10, 3 * MINUTE_MS),
  ].forEach(summarizer.addLine);
  assert.deepStrictEqual(
    summarizer.finish().spans.map((entry) => [entry.count, entry.p50Ms, entry.p90Ms, entry.maxMs]),
    [[10, 5, 10, 10]],
  );
});

it("weights the written sample when the threshold filter and sampling are both on", () => {
  const summarizer = makeTraceSpanSummary();
  [
    JSON.stringify({ ...JSON.parse(sqlSummary(400, MINUTE_MS, 2 * MINUTE_MS)), thresholdMs: 10 }),
    JSON.stringify({
      ...JSON.parse(sqlSummary(594, MINUTE_MS, 2 * MINUTE_MS)),
      thresholdMs: 10,
      sampleEvery: 100,
      maxDurationMs: 100,
    }),
    ...Array.from({ length: 6 }, () => span("sqlite", 100, 2 * MINUTE_MS)),
  ].forEach(summarizer.addLine);
  // 400 fast statements under 10 ms, then 600 at 100 ms that six written ones stand for.
  assert.deepStrictEqual(
    summarizer.finish().spans.map((entry) => [entry.count, entry.p50Ms, entry.p90Ms]),
    [[1_000, 100, 100]],
  );
});

it("ranks the weighted sample before rounding to a written statement", () => {
  const summarizer = makeTraceSpanSummary();
  [
    JSON.stringify({
      ...JSON.parse(sqlSummary(1, MINUTE_MS, 2 * MINUTE_MS)),
      thresholdMs: 0,
      sampleEvery: 2,
      maxDurationMs: 50,
    }),
    span("sqlite", 1, 2 * MINUTE_MS),
    span("sqlite", 100, 2 * MINUTE_MS),
    span("sqlite", 1_000, 2 * MINUTE_MS),
  ].forEach(summarizer.addLine);
  // Each written statement stands for 4/3: the second of four is the 100 ms one.
  assert.deepStrictEqual(
    summarizer.finish().spans.map((entry) => [entry.count, entry.p50Ms]),
    [[4, 100]],
  );
});

it("prints no percentile past the threshold summaries when nothing above them was written", () => {
  const summarizer = makeTraceSpanSummary();
  [
    JSON.stringify({ ...JSON.parse(sqlSummary(40, MINUTE_MS, 2 * MINUTE_MS)), thresholdMs: 10 }),
    JSON.stringify({
      ...JSON.parse(sqlSummary(60, MINUTE_MS, 2 * MINUTE_MS)),
      thresholdMs: 10,
      sampleEvery: 100,
    }),
  ].forEach(summarizer.addLine);
  // The 40 fast ones rank first; the 60 sampled-out ones above them have no written stand-in.
  assert.deepStrictEqual(
    summarizer.finish().spans.map((entry) => [entry.p50Ms, entry.p90Ms]),
    [[null, null]],
  );
});

it("weights only the sampled statements, never the slow ones always written", () => {
  const summarizer = makeTraceSpanSummary();
  [
    JSON.stringify({
      ...JSON.parse(sqlSummary(99, MINUTE_MS, 2 * MINUTE_MS)),
      thresholdMs: 0,
      sampleEvery: 100,
      sampleBelowMs: 1_000,
      maxDurationMs: 1,
    }),
    span("sqlite", 1, 2 * MINUTE_MS),
    ...Array.from({ length: 10 }, () => span("sqlite", 10_000, 2 * MINUTE_MS)),
  ].forEach(summarizer.addLine);
  // 100 statements at 1 ms, one written for them all, then ten at 10 s.
  assert.deepStrictEqual(
    summarizer.finish().spans.map((entry) => [entry.count, entry.p50Ms, entry.p90Ms]),
    [[110, 1, 1]],
  );
});

it("ranks always-written slow statements above sampled-out ones that lost their stand-in", () => {
  const summarizer = makeTraceSpanSummary();
  [
    JSON.stringify({
      ...JSON.parse(sqlSummary(1, MINUTE_MS, 2 * MINUTE_MS)),
      thresholdMs: 0,
      sampleEvery: 100,
      sampleBelowMs: 1_000,
      maxDurationMs: 1,
    }),
    ...Array.from({ length: 9 }, () => span("sqlite", 10_000, 2 * MINUTE_MS)),
  ].forEach(summarizer.addLine);
  // The window lost the written statement that stood for the sampled-out one, which is under
  // the cutoff: every rank above it is a 10 s statement; its own rank is unknown.
  assert.deepStrictEqual(
    summarizer.finish().spans.map((entry) => [entry.count, entry.p50Ms, entry.p90Ms]),
    [[10, 10_000, 10_000]],
  );
});

it("prints no percentile for a window of sampled-out statements alone", () => {
  const summarizer = makeTraceSpanSummary();
  summarizer.addLine(
    JSON.stringify({
      ...JSON.parse(sqlSummary(100, MINUTE_MS, 2 * MINUTE_MS)),
      thresholdMs: 0,
      sampleEvery: 5,
      maxDurationMs: 25,
    }),
  );
  const summary = summarizer.finish();
  assert.deepStrictEqual(
    summary.spans.map((entry) => [entry.count, entry.p50Ms, entry.p90Ms, entry.maxMs]),
    [[100, null, null, 25]],
  );
  const row = formatTraceSummary(summary, 25)
    .split("\n")
    .find((line) => line.startsWith("sqlite"));
  assert.deepStrictEqual(row?.trim().split(/\s+/).slice(3, 6), ["-", "-", "25ms"]);
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

it("prints durations that round up to a second in seconds", () => {
  const summarizer = makeTraceSpanSummary();
  [span("rpc", 999.4, MINUTE_MS), span("rpc", 999.6, 2 * MINUTE_MS)].forEach(summarizer.addLine);

  const row = formatTraceSummary(summarizer.finish(), 25)
    .split("\n")
    .find((line) => line.startsWith("rpc"));
  assert.deepStrictEqual(row?.trim().split(/\s+/).slice(3, 6), ["999ms", "1s", "1s"]);
});

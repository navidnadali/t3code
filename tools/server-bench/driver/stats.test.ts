import { describe, expect, it } from "vite-plus/test";

import { bucketUpperBound, Recorder } from "./stats.ts";

/** Calls that start and finish in the window, with the given latencies. */
const record = (recorder: Recorder, type: string, latencies: ReadonlyArray<number>) => {
  for (const value of latencies) recorder.succeed(recorder.begin(type), value);
};

describe("Recorder", () => {
  it("attributes calls by start time: warm-up calls never count, window calls finish in grace", () => {
    const recorder = new Recorder();
    const warmUp = recorder.begin("session.turn");
    recorder.openWindow();
    const longTurn = recorder.begin("session.turn");
    const failedTurn = recorder.begin("session.turn");
    const stuckTurn = recorder.begin("session.turn");
    recorder.succeed(warmUp, 999);
    recorder.closeWindow();
    const afterWindow = recorder.begin("session.turn");
    recorder.succeed(afterWindow, 1);
    // Grace period: the window's calls may still end.
    recorder.succeed(longTurn, 42_000);
    recorder.fail(failedTurn, "run failed");
    expect(recorder.inFlight).toBe(1);
    recorder.seal();
    recorder.succeed(stuckTurn, 90_000);

    expect(recorder.summarize(10).calls["session.turn"]).toMatchObject({
      count: 1,
      errors: 1,
      unfinished: 1,
      perSecond: 0.3,
      p50: 42_000,
      max: 42_000,
    });
  });

  it("counts events and samples only while the window is open", () => {
    const recorder = new Recorder();
    recorder.count("events.thread", 50);
    recorder.sample("push.threadEvent", 999);
    recorder.openWindow();
    recorder.count("events.thread", 20, 2000);
    for (const value of [4, 1, 3, 2, 10]) recorder.sample("push.threadEvent", value);
    recorder.closeWindow();
    recorder.count("events.thread", 50);
    recorder.sample("push.threadEvent", 999);

    const { calls, counters } = recorder.summarize(10);
    expect(calls["push.threadEvent"]).toMatchObject({
      count: 5,
      errors: 0,
      p50: 3,
      p95: 10,
      p99: 10,
      max: 10,
      mean: 4,
    });
    expect(counters["events.thread"]).toEqual({ count: 20, perSecond: 2, bytes: 2000 });
  });

  it("forgets discarded calls and counts errors per call type", () => {
    const recorder = new Recorder();
    recorder.openWindow();
    record(recorder, "session.dispatch", [5, 5, 5, 5, 5, 5, 5, 5, 5]);
    recorder.fail(recorder.begin("session.dispatch"), "busy");
    recorder.fail(recorder.begin("session.dispatch"), "busy");
    recorder.discard(recorder.begin("session.dispatch"));
    record(recorder, "poll.http.threadBounded", [5]);
    expect(recorder.inFlight).toBe(0);

    expect(recorder.worstErrorRate()).toEqual({ type: "session.dispatch", rate: 2 / 11 });
    expect(recorder.summarize(1).calls["session.dispatch"]).toMatchObject({
      count: 9,
      errors: 2,
      unfinished: 0,
      perSecond: 11,
      errorSamples: ["busy"],
    });
  });

  it("buckets latencies on a shared log scale so runs compare bucket by bucket", () => {
    const recorder = new Recorder();
    recorder.openWindow();
    record(recorder, "push.threadEvent", [1, 1, 1.1, 100]);
    // Bounds are 2^(k/4) ms, rounded to the microsecond.
    expect(recorder.summarize(1).calls["push.threadEvent"]?.histogram).toEqual([
      [1, 2],
      [1.189, 1],
      [107.635, 1],
    ]);
    expect(bucketUpperBound(1.1)).toBeGreaterThanOrEqual(1.1);
    expect(bucketUpperBound(1.1)).toBeLessThan(1.1 * 2 ** 0.25);
  });

  it("reports no percentiles for a call type that only failed", () => {
    const recorder = new Recorder();
    recorder.openWindow();
    recorder.error("session.turn", "run failed");
    expect(recorder.summarize(1).calls["session.turn"]).toMatchObject({
      count: 0,
      errors: 1,
      p50: null,
      max: null,
    });
  });
});

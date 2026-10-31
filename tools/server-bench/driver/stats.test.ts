import { describe, expect, it } from "vite-plus/test";

import { bucketUpperBound, Recorder } from "./stats.ts";

describe("Recorder", () => {
  it("keeps only what happens while the window is open", () => {
    const recorder = new Recorder();
    recorder.latency("poll.mcp.t3_thread_read", 999);
    recorder.error("poll.mcp.t3_thread_read", "warm-up failure");
    recorder.count("events.thread", 50);
    recorder.setOpen(true);
    for (const value of [4, 1, 3, 2, 10]) recorder.latency("poll.mcp.t3_thread_read", value);
    recorder.count("events.thread", 20, 2000);
    recorder.setOpen(false);
    recorder.latency("poll.mcp.t3_thread_read", 999);

    const { calls, counters } = recorder.summarize(10);
    expect(calls["poll.mcp.t3_thread_read"]).toMatchObject({
      count: 5,
      errors: 0,
      perSecond: 0.5,
      p50: 3,
      p95: 10,
      p99: 10,
      max: 10,
      mean: 4,
    });
    expect(counters["events.thread"]).toEqual({ count: 20, perSecond: 2, bytes: 2000 });
  });

  it("counts errors per call type and reports the worst error rate", () => {
    const recorder = new Recorder();
    recorder.setOpen(true);
    for (let index = 0; index < 9; index++) recorder.latency("session.dispatch", 5);
    recorder.error("session.dispatch", "busy");
    recorder.error("session.dispatch", "busy");
    recorder.latency("poll.http.threadBounded", 5);

    expect(recorder.worstErrorRate()).toEqual({ type: "session.dispatch", rate: 2 / 11 });
    const { calls } = recorder.summarize(1);
    expect(calls["session.dispatch"]).toMatchObject({
      count: 9,
      errors: 2,
      perSecond: 11,
      errorSamples: ["busy"],
    });
  });

  it("buckets latencies on a shared log scale so runs compare bucket by bucket", () => {
    const recorder = new Recorder();
    recorder.setOpen(true);
    for (const value of [1, 1, 1.1, 100]) recorder.latency("push.threadEvent", value);
    const { calls } = recorder.summarize(1);
    // Bounds are 2^(k/4) ms, rounded to the microsecond.
    expect(calls["push.threadEvent"]?.histogram).toEqual([
      [1, 2],
      [1.189, 1],
      [107.635, 1],
    ]);
    expect(bucketUpperBound(1.1)).toBeGreaterThanOrEqual(1.1);
    expect(bucketUpperBound(1.1)).toBeLessThan(1.1 * 2 ** 0.25);
  });

  it("reports no percentiles for a call type that only failed", () => {
    const recorder = new Recorder();
    recorder.setOpen(true);
    recorder.error("session.turn", "run failed");
    expect(recorder.summarize(1).calls["session.turn"]).toMatchObject({
      count: 0,
      errors: 1,
      p50: null,
      max: null,
    });
  });
});

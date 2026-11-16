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

describe("Recorder: calls a session owns", () => {
  it("summarises session calls over the sessions kept, and every call without an owner", () => {
    const recorder = new Recorder();
    recorder.openWindow();
    // 250 sessions: 87 finish a turn in the window (session i in 1000 + i ms), 163 never get output.
    const turns = Array.from({ length: 250 }, (_, index) =>
      recorder.begin("session.turn", `session-${index}`),
    );
    for (let index = 0; index < 87; index++) {
      recorder.succeed(turns[index]!, 1000 + index);
      recorder.sample("session.firstItem", 100 + index, `session-${index}`);
    }
    recorder.error("session.watch", "stream ended", { owner: "session-200" });
    record(recorder, "poll.http.threadBounded", [10, 20, 30]);
    recorder.closeWindow();
    recorder.seal();
    const served = new Set(Array.from({ length: 87 }, (_, index) => `session-${index}`));
    const { calls } = recorder.summarize(120, (owner) => served.has(owner));
    expect(calls["session.turn"]).toMatchObject({
      count: 87,
      errors: 0,
      unfinished: 0,
      p50: 1043,
      max: 1086,
    });
    expect(calls["session.firstItem"]).toMatchObject({ count: 87, p50: 143 });
    expect(calls["session.watch"]).toMatchObject({ count: 0, errors: 0 });
    expect(calls["poll.http.threadBounded"]).toMatchObject({ count: 3, p50: 20 });
    // Without a filter every session counts: the 163 turns still running are unfinished.
    expect(recorder.summarize(120).calls["session.turn"]).toMatchObject({
      count: 87,
      unfinished: 163,
    });
  });
});

describe("Recorder.summarizeOwned (one provider's sessions)", () => {
  it("summarises only the calls of the owners kept, and no call without an owner", () => {
    const recorder = new Recorder();
    recorder.openWindow();
    recorder.succeed(recorder.begin("session.turn", "a"), 100);
    recorder.succeed(recorder.begin("session.turn", "b"), 900);
    recorder.sample("session.firstItem", 10, "b");
    record(recorder, "poll.http.threadBounded", [5]);
    recorder.closeWindow();
    recorder.seal();
    const calls = recorder.summarizeOwned(120, (owner) => owner === "b").calls;
    expect(Object.keys(calls).toSorted()).toEqual(["session.firstItem", "session.turn"]);
    expect(calls["session.turn"]).toMatchObject({ count: 1, p50: 900 });
    expect(calls["session.firstItem"]).toMatchObject({ count: 1, p50: 10 });
  });
});

describe("Recorder: timeouts apart from other errors", () => {
  it("counts each call type's timeouts, and the window's errors that were not timeouts", () => {
    const recorder = new Recorder();
    recorder.openWindow();
    recorder.fail(recorder.begin("session.dispatch"), "dispatch: timed out", { timeout: true });
    recorder.fail(recorder.begin("session.dispatch"), "dispatch: timed out", { timeout: true });
    recorder.error("poll.mcp.t3_thread_read", "not sent: 8 calls in flight", { timeout: true });
    expect(recorder.nonTimeoutErrors()).toBe(0);
    recorder.fail(recorder.begin("session.dispatch"), "answered 503: busy");
    expect(recorder.nonTimeoutErrors()).toBe(1);
    recorder.closeWindow();
    recorder.seal();
    expect(recorder.summarize(120).calls["session.dispatch"]).toMatchObject({
      errors: 3,
      timeouts: 2,
    });
    expect(recorder.summarize(120).calls["poll.mcp.t3_thread_read"]).toMatchObject({
      errors: 1,
      timeouts: 1,
    });
  });
});

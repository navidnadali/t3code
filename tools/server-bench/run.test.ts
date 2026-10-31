import { describe, expect, it } from "vite-plus/test";

import { benchEnvHash, readDriverResult } from "./run.ts";

describe("readDriverResult", () => {
  it("reads the P1-004 driver.json: window, per-call latencies and event rates", () => {
    const summary = readDriverResult({
      schemaVersion: 1,
      status: "ok",
      knobs: { sessions: 80, seed: "1" },
      timeline: { warmup: "2026-10-31T21:00:00.000Z", window: "2026-10-31T21:01:00.000Z" },
      windowSeconds: 300.004,
      calls: {
        "mcp.t3_project_list": {
          count: 290,
          errors: 1,
          perSecond: 0.97,
          p50: 950,
          p95: 9800,
          p99: 12_000,
          max: 15_000,
        },
      },
      counters: {
        "events.thread": { count: 123_750, perSecond: 412.5, bytes: 1 },
        "events.shell": { count: 28_800, perSecond: 96, bytes: 1 },
      },
    });

    expect(summary).toEqual({
      ok: true,
      error: null,
      window: {
        startMs: Date.parse("2026-10-31T21:01:00.000Z"),
        endMs: Date.parse("2026-10-31T21:06:00.004Z"),
      },
      thread_events_per_s: 412.5,
      shell_events_per_s: 96,
      calls: {
        "mcp.t3_project_list": {
          count: 290,
          errors: 1,
          per_s: 0.97,
          p50_ms: 950,
          p95_ms: 9800,
          p99_ms: 12_000,
          max_ms: 15_000,
        },
      },
      knobs: { sessions: 80, seed: "1" },
    });
  });

  it("treats a failed run as failed and keeps absent numbers null", () => {
    const summary = readDriverResult({
      schemaVersion: 1,
      status: "failed",
      failure: { phase: "window", reason: "the server exited" },
      timeline: { window: "2026-10-31T21:01:00.000Z" },
    });

    expect(summary.ok).toBe(false);
    expect(summary.error).toBe("the server exited");
    expect(summary.window).toBeNull();
    expect(summary.thread_events_per_s).toBeNull();
    expect(summary.calls).toBeNull();
  });
});

describe("benchEnvHash", () => {
  it("tells raw load settings apart and ignores where things live", () => {
    const base = { BENCH_SPEED: "2", BENCH_TRANSCRIPT: "/srv/a.ndjson", T3CODE_LOG_LEVEL: "Info" };

    expect(benchEnvHash(base)).toBe(
      benchEnvHash({ ...base, BENCH_STATS_DIR: "/tmp/run-2", BENCH_NODE: "/usr/bin/node" }),
    );
    expect(benchEnvHash(base)).not.toBe(
      benchEnvHash({ ...base, BENCH_TRANSCRIPT: "/srv/b.ndjson" }),
    );
    expect(benchEnvHash(base)).toBe(benchEnvHash({ ...base, T3CODE_LOG_LEVEL: "Debug" }));
  });
});

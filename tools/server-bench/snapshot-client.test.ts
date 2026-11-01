import { describe, expect, it } from "vite-plus/test";

import {
  MAX_TIMER_MS,
  makeTimeoutDraw,
  parseOptions,
  type RequestRecord,
  summarize,
} from "./snapshot-client.ts";

const required = ["--url=http://h", "--thread=t"];

describe("parseOptions", () => {
  it("defaults to the bot's cadence and timeout with the body left unread", () => {
    expect(parseOptions(["--url", "http://127.0.0.1:3773/", "--thread", "t-1"])).toEqual({
      url: "http://127.0.0.1:3773",
      threadId: "t-1",
      intervalMs: 300_000,
      timeoutMs: 23_000,
      timeoutMaxMs: 23_000,
      seed: "1",
      read: "none",
      acceptEncoding: "gzip, deflate",
      count: null,
      protocol: null,
      out: null,
      tokenFile: null,
    });
  });

  it("reads durations in ms, s and m, and timeout ranges", () => {
    const options = parseOptions([
      ...required,
      "--interval=2m",
      "--timeout=500ms",
      "--read=full",
      "--count=3",
      "--seed=ab",
    ]);
    expect([
      options.intervalMs,
      options.timeoutMs,
      options.read,
      options.count,
      options.seed,
    ]).toEqual([120_000, 500, "full", 3, "ab"]);
    expect(parseOptions([...required, "--timeout=2s-9s"])).toMatchObject({
      timeoutMs: 2_000,
      timeoutMaxMs: 9_000,
    });
  });

  it("rejects malformed values and timings that would flood the server or overflow a timer", () => {
    expect(() => parseOptions(["--url=http://h"])).toThrow(/--thread are required/);
    expect(() => parseOptions([...required, "--read=some"])).toThrow(/--read/);
    expect(() => parseOptions([...required, "--count=0"])).toThrow(/positive/);
    expect(() => parseOptions([...required, "--timeout=soon"])).toThrow(/duration/);
    expect(() => parseOptions([...required, "--timeout=9s-2s"])).toThrow(/low to high/);
    expect(() => parseOptions([...required, "--timeout=2s-9s-20s"])).toThrow(/one duration/);
    expect(() => parseOptions([...required, `--interval=${"9".repeat(400)}`])).toThrow(/duration/);
    for (const interval of ["0", "-5s", "4999ms", "40000m"]) {
      expect(() => parseOptions([...required, `--interval=${interval}`])).toThrow(/--interval/);
    }
    for (const timeout of ["0", "99ms", "2s-40000m", `${MAX_TIMER_MS + 1}ms`]) {
      expect(() => parseOptions([...required, `--timeout=${timeout}`])).toThrow(/--timeout/);
    }
    expect(
      parseOptions([...required, "--interval=5s", `--timeout=${MAX_TIMER_MS}ms`]),
    ).toMatchObject({ intervalMs: 5_000, timeoutMs: MAX_TIMER_MS });
  });
});

describe("makeTimeoutDraw", () => {
  it("repeats the same abort times for the same seed, inside the range", () => {
    const draws = (seed: string) => {
      const next = makeTimeoutDraw(
        parseOptions([...required, "--timeout=2s-9s", `--seed=${seed}`]),
      );
      return Array.from({ length: 50 }, next);
    };
    const first = draws("pair-1");
    expect(draws("pair-1")).toEqual(first);
    expect(draws("pair-2")).not.toEqual(first);
    expect(first.every((value) => value >= 2_000 && value <= 9_000)).toBe(true);
    expect(new Set(first).size).toBeGreaterThan(40);
    const fixed = makeTimeoutDraw(parseOptions([...required, "--timeout=23s"]));
    expect([fixed(), fixed()]).toEqual([23_000, 23_000]);
  });
});

describe("summarize", () => {
  it("counts outcomes and the realised abort times", () => {
    const record = (outcome: RequestRecord["outcome"], timeoutMs: number): RequestRecord => ({
      at: "2026-11-01T00:00:00.000Z",
      seq: 1,
      seed: "s",
      outcome,
      status: null,
      contentEncoding: null,
      contentLength: null,
      wireBytes: 0,
      timeoutMs,
      headersMs: null,
      endMs: timeoutMs,
    });
    expect(
      summarize("s", [
        record("aborted_before_headers", 3_000),
        record("aborted_during_body", 8_000),
        record("aborted_before_headers", 2_000),
      ]),
    ).toEqual({
      summary: {
        seed: "s",
        requests: 3,
        outcomes: { aborted_before_headers: 2, aborted_during_body: 1 },
        timeoutMs: { min: 2_000, median: 3_000, max: 8_000 },
      },
    });
    expect(summarize("s", []).summary.timeoutMs).toBeNull();
  });
});

import { describe, expect, it } from "vite-plus/test";

import { parseDriverOptions, parseDuration, parsePollMix, UsageError } from "./options.ts";

const required = ["--url", "http://127.0.0.1:3773/ignored", "--out", "/tmp/bench"];

describe("parseDriverOptions", () => {
  it("defaults to the G2 history workload", () => {
    const options = parseDriverOptions(required);
    expect(options).toMatchObject({
      url: "http://127.0.0.1:3773",
      mode: "history",
      sessions: 80,
      subscribers: 80,
      pollers: 20,
      warmupMs: 60_000,
      windowMs: 300_000,
      stopSessions: true,
    });
  });

  it("reads every knob from the command line", () => {
    const options = parseDriverOptions([
      ...required,
      "--mode",
      "fresh",
      "--sessions",
      "4",
      "--warmup",
      "1500ms",
      "--window",
      "2m",
      "--poll-mix",
      "thread_read=3,http_bounded=1",
      "--no-stop-sessions",
      "--grace",
      "90s",
      "--sessions-per-socket",
      "8",
      "--max-driver-delay",
      "20ms",
    ]);
    expect(options).toMatchObject({
      mode: "fresh",
      sessions: 4,
      subscribers: 4,
      warmupMs: 1500,
      windowMs: 120_000,
      pollMix: [
        ["mcp.t3_thread_read", 3],
        ["http.threadBounded", 1],
      ],
      stopSessions: false,
      graceMs: 90_000,
      sessionsPerSocket: 8,
      maxDriverDelayMs: 20,
      maxDriverElu: 0.7,
    });
  });

  it.each([
    [["--out", "/tmp/bench"], "--url is required"],
    [[...required, "--mode", "replay"], "--mode must be fresh or history"],
    [[...required, "--sessions", "1.5"], "--sessions must be an integer"],
    [[...required, "--window", "0s"], "--window must be positive"],
    [[...required, "--bogus"], "Unknown option '--bogus'"],
    [[...required, "--taskset", "all"], "--taskset takes a CPU list"],
    [[...required, "--max-driver-elu", "2"], "--max-driver-elu must be between 0 and 1"],
    [[...required, "--sessions-per-socket", "0"], "--sessions-per-socket must be an integer"],
    [[...required, "--mode", "fresh", "--sessions", "0"], "--sessions must be > 0"],
  ])("rejects %j", (argv, message) => {
    expect(() => parseDriverOptions(argv)).toThrow(UsageError);
    expect(() => parseDriverOptions(argv)).toThrow(message);
  });
});

describe("parseDuration", () => {
  it("reads units and treats a bare number as seconds", () => {
    expect(parseDuration("300", "window")).toBe(300_000);
    expect(parseDuration("5m", "window")).toBe(300_000);
    expect(parseDuration("250ms", "window")).toBe(250);
    expect(() => parseDuration("5 minutes", "window")).toThrow(UsageError);
  });
});

describe("parsePollMix", () => {
  it("rejects unknown calls, repeats and an all-zero mix", () => {
    expect(() => parsePollMix("thread_list=1")).toThrow(UsageError);
    expect(() => parsePollMix("thread_read=1,thread_read=2")).toThrow("names a call twice");
    expect(() => parsePollMix("thread_read=0")).toThrow("at least one positive weight");
  });
});

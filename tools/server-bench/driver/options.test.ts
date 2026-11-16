// @effect-diagnostics nodeBuiltinImport:off - Resolves paths as the standalone driver does.
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import {
  DEFAULT_SESSION_MCP_RATE,
  missingScopes,
  parseDriverOptions,
  parseDuration,
  parsePollMix,
  USAGE,
  UsageError,
} from "./options.ts";
import { compareDistributions, summarizeDistribution } from "./sampling.ts";

const required = ["--url", "http://127.0.0.1:3773/ignored", "--out", "/tmp/bench"];

describe("the session MCP rate (a chosen bench parameter)", () => {
  it("defaults to one MCP call per 20 minutes per live session, and the help claims no deployment", () => {
    expect(DEFAULT_SESSION_MCP_RATE).toBe(0.05);
    expect(parseDriverOptions(required).sessionMcpRatePerMinute).toBe(0.05);
    expect(USAGE).toMatch(/--session-mcp-rate N[\s\S]*\(default 0\.05;/);
  });
});

describe("parseDriverOptions", () => {
  it("defaults to a history workload of 25 sessions, 25 subscribers and 10 pollers", () => {
    const options = parseDriverOptions(required);
    expect(options).toMatchObject({
      url: "http://127.0.0.1:3773",
      mode: "history",
      sessions: 25,
      subscribers: 25,
      pollers: 10,
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

  it("plans Codex and ACP sessions next to --sessions on the bench instances", () => {
    const options = parseDriverOptions([
      ...required,
      "--sessions",
      "2",
      "--codex-sessions",
      "3",
      "--acp-sessions",
      "1",
    ]);

    expect(options).toMatchObject({
      provider: "claudeBench",
      sessions: 2,
      codexSessions: 3,
      acpSessions: 1,
      subscribers: 6,
    });
    expect(options.sessionPlan.map((session) => [session.instanceId, session.model])).toEqual([
      ["codexBench", "gpt-6-luna"],
      ["claudeBench", "claude-sonnet-4-6"],
      ["codexBench", "gpt-6-luna"],
      ["acpBench", "bench-replay"],
      ["claudeBench", "claude-sonnet-4-6"],
      ["codexBench", "gpt-6-luna"],
    ]);
    expect(
      parseDriverOptions([...required, "--mode", "fresh", "--sessions", "0", "--acp-sessions", "2"])
        .sessionPlan.length,
    ).toBe(2);
    expect(parseDriverOptions([...required, "--mode", "mixed", "--sessions", "3"]).mode).toBe(
      "mixed",
    );
  });

  // The fakes run in each session's workspace, so a relative directory must not reach them.
  it("makes --session-mcp-credentials absolute, for the driver and the fakes alike", () => {
    const credentials = (directory: string) =>
      parseDriverOptions([...required, "--session-mcp-credentials", directory])
        .sessionMcpCredentials;

    expect(parseDriverOptions(required).sessionMcpCredentials).toBeNull();
    expect(credentials("./credentials")).toBe(NodePath.resolve("credentials"));
    expect(credentials("/run/credentials/")).toBe("/run/credentials");
  });

  it.each([
    [[...required, "--session-mcp-credentials", ""], "--session-mcp-credentials needs a directory"],
    [[...required, "--codex-sessions=-1"], "--codex-sessions must be an integer"],
    [["--out", "/tmp/bench"], "--url is required"],
    [[...required, "--mode", "replay"], "--mode must be fresh, history or mixed"],
    [[...required, "--sessions", "1.5"], "--sessions must be an integer"],
    [[...required, "--window", "0s"], "--window must be positive"],
    [[...required, "--bogus"], "Unknown option '--bogus'"],
    [[...required, "--taskset", "all"], "--taskset takes a CPU list"],
    [[...required, "--max-driver-elu", "2"], "--max-driver-elu must be between 0 and 1"],
    [[...required, "--sessions-per-socket", "0"], "--sessions-per-socket must be an integer"],
    [[...required, "--mode", "fresh", "--sessions", "0"], "start at least one session"],
  ])("rejects %j", (argv, message) => {
    expect(() => parseDriverOptions(argv)).toThrow(UsageError);
    expect(() => parseDriverOptions(argv)).toThrow(message);
  });
});

describe("--distribution-tolerance", () => {
  it("accepts 0 to 5: a fixture's idle roots can sit well above its turn-item p50", () => {
    expect(parseDriverOptions(required).distributionTolerance).toBe(0.2);
    for (const [text, value] of [
      ["0", 0],
      ["0.2", 0.2],
      ["1", 1],
      ["2.5", 2.5],
      ["5", 5],
    ] as const) {
      expect(
        parseDriverOptions([...required, "--distribution-tolerance", text]).distributionTolerance,
      ).toBe(value);
    }
  });

  it.each(["5.01", "-0.1", "-1", "abc", "", "Infinity"])(
    "refuses %j with a named error",
    (text) => {
      // `=` form: node's parseArgs takes a separate "-1" for another option.
      const argv = [...required, `--distribution-tolerance=${text}`];
      expect(() => parseDriverOptions(argv)).toThrow(UsageError);
      expect(() => parseDriverOptions(argv)).toThrow(
        "--distribution-tolerance must be between 0 and 5",
      );
    },
  );

  it("is the tolerance the distribution check applies", () => {
    // The public fixture's case: idle roots at 3.125 times the fixture's p50 turn items.
    const population = summarizeDistribution([16, 16, 16, 100]);
    const sample = summarizeDistribution([50, 50, 50, 120]);
    const check = (argv: ReadonlyArray<string>) =>
      compareDistributions(population, sample, parseDriverOptions(argv).distributionTolerance);
    expect(check(required)).toMatchObject({ p50Ratio: 3.125, withinTolerance: false });
    expect(check([...required, "--distribution-tolerance", "2.2"])).toMatchObject({
      p50Ratio: 3.125,
      tolerance: 2.2,
      withinTolerance: true,
    });
  });

  it("leaves the other fraction flags at most 1", () => {
    expect(() => parseDriverOptions([...required, "--max-driver-elu", "1.5"])).toThrow(
      "--max-driver-elu must be between 0 and 1",
    );
  });
});

describe("--fresh-subscriber-threads", () => {
  it("defaults to none and takes a count in fresh mode only", () => {
    expect(parseDriverOptions(required).freshSubscriberThreads).toBe(0);
    expect(
      parseDriverOptions([...required, "--mode", "fresh", "--fresh-subscriber-threads", "20"])
        .freshSubscriberThreads,
    ).toBe(20);
    expect(() =>
      parseDriverOptions([...required, "--mode", "mixed", "--fresh-subscriber-threads", "20"]),
    ).toThrow(UsageError);
    expect(() =>
      parseDriverOptions([...required, "--mode", "fresh", "--fresh-subscriber-threads", "-1"]),
    ).toThrow(UsageError);
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

describe("missingScopes", () => {
  const base = ["orchestration:read", "orchestration:operate", "access:write"];

  it("asks for settings scopes whenever the driver updates a bench instance, credentials or not", () => {
    expect(missingScopes(base, { updatesInstances: true })).toEqual([
      "settings:write",
      "providers:manage",
    ]);
    expect(missingScopes(base, { updatesInstances: false })).toEqual([]);
    expect(missingScopes(["orchestration:read"], { updatesInstances: false })).toEqual([
      "orchestration:operate",
      "access:write",
    ]);
  });
});

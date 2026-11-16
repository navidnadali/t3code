import { describe, expect, it } from "vite-plus/test";

import {
  composeResult,
  judgeWindow,
  windowFindings,
  windowVerdict,
  type WindowEvidence,
} from "./verdict.ts";

const healthy: WindowEvidence = {
  worstErrorRate: { type: "poll.mcp.t3_thread_read", rate: 0.005 },
  maxErrorRate: 0.01,
  nonTimeoutErrors: 0,
  sessions: 25,
  sessionsWithOutput: 25,
  distribution: { p50Ratio: 1.05, p95Ratio: 0.9, tolerance: 0.2, withinTolerance: true },
  sessionMcpRequired: 25,
  sessionMcpCallers: 25,
  driver: {
    eventLoopUtilization: 0.2,
    eventLoopDelayP99Ms: 12,
    maxEventLoopUtilization: 0.7,
    maxEventLoopDelayP99Ms: 50,
  },
};

describe("judgeWindow", () => {
  it("publishes a window that streamed, rarely failed and matched the fixture", () => {
    expect(judgeWindow(healthy)).toBeNull();
    expect(judgeWindow({ ...healthy, distribution: null })).toBeNull();
  });

  it("rejects a window measured by a saturated driver", () => {
    expect(
      judgeWindow({ ...healthy, driver: { ...healthy.driver, eventLoopUtilization: 0.75 } }),
    ).toBe(
      "the driver itself was saturated (event loop 75% busy, delay p99 12 ms; limits 70% and 50 ms), so its latencies are not the server's",
    );
    expect(
      judgeWindow({ ...healthy, driver: { ...healthy.driver, eventLoopDelayP99Ms: 80 } }),
    ).toContain("the driver itself was saturated");
  });

  it("rejects a window with too many failed calls", () => {
    expect(
      judgeWindow({
        ...healthy,
        nonTimeoutErrors: 2,
        worstErrorRate: { type: "session.dispatch", rate: 0.25 },
      }),
    ).toBe(
      "session.dispatch failed 25.00% of calls (limit 1%); 2 error(s) in the window were not timeouts",
    );
  });

  it("rejects a responsive server whose provider produced no output", () => {
    expect(judgeWindow({ ...healthy, sessionsWithOutput: 0 })).toBe(
      "only 0 of 25 sessions produced provider output during the window",
    );
    expect(judgeWindow({ ...healthy, sessionsWithOutput: 24 })).toContain("only 24 of 25");
  });

  it("rejects a window in which some sessions could not call their MCP tools", () => {
    expect(judgeWindow({ ...healthy, sessionMcpCallers: 12 })).toBe(
      "only 12 of 25 sessions had an MCP client when the window opened",
    );
    expect(judgeWindow({ ...healthy, sessionMcpRequired: 0, sessionMcpCallers: 0 })).toBeNull();
  });

  it("rejects exercised threads that do not look like the fixture", () => {
    expect(
      judgeWindow({
        ...healthy,
        distribution: { p50Ratio: 0.001, p95Ratio: 0.001, tolerance: 0.2, withinTolerance: false },
      }),
    ).toContain("do not match the fixture's turn-item distribution");
  });
});

describe("windowVerdict (a stable code per failure)", () => {
  it("tells a server that served only some sessions apart from a broken measurement", () => {
    expect(windowVerdict(healthy)).toBeNull();
    expect(windowVerdict({ ...healthy, sessions: 250, sessionsWithOutput: 87 })).toEqual({
      code: "sessions_starved",
      reason: "only 87 of 250 sessions produced provider output during the window",
      sessionsServed: 87,
      sessionsTotal: 250,
    });
    expect(
      windowVerdict({ ...healthy, driver: { ...healthy.driver, eventLoopUtilization: 0.75 } })
        ?.code,
    ).toBe("driver_saturated");
    expect(
      windowVerdict({
        ...healthy,
        nonTimeoutErrors: 1,
        worstErrorRate: { type: "session.dispatch", rate: 0.25 },
      })?.code,
    ).toBe("error_rate");
    expect(windowVerdict({ ...healthy, sessionMcpCallers: 3 })?.code).toBe("session_mcp_missing");
    expect(
      windowVerdict({
        ...healthy,
        distribution: { p50Ratio: 2, p95Ratio: 1, tolerance: 0.2, withinTolerance: false },
      })?.code,
    ).toBe("distribution_mismatch");
    // Starved sessions are the last check: a window that also failed a hard check reports that failure instead.
    const starved = { ...healthy, sessions: 250, sessionsWithOutput: 87 };
    expect(windowVerdict({ ...starved, sessionMcpCallers: 3 })?.code).toBe("session_mcp_missing");
    expect(
      windowVerdict({
        ...starved,
        distribution: { p50Ratio: 2, p95Ratio: 1, tolerance: 0.2, withinTolerance: false },
      })?.code,
    ).toBe("distribution_mismatch");
    // A saturated driver outranks starved sessions: its own latencies are not the server's.
    expect(
      windowVerdict({
        ...healthy,
        sessions: 250,
        sessionsWithOutput: 87,
        driver: { ...healthy.driver, eventLoopUtilization: 0.75 },
      })?.code,
    ).toBe("driver_saturated");
  });
});

describe("composeResult (what driver.json carries)", () => {
  const base = { schemaVersion: 1, knobs: { sessions: 250 } };
  const measured = { windowSeconds: 120, calls: { "session.turn": { count: 87 } } };

  it("keeps a failed window's measurements beside its failure", () => {
    const failure = {
      phase: "window",
      code: "sessions_starved",
      reason: "only 87 of 250 sessions produced provider output during the window",
      sessionsServed: 87,
      sessionsTotal: 250,
    };
    expect(composeResult(base, measured, failure)).toEqual({
      ...base,
      status: "failed",
      ...measured,
      failure,
    });
  });

  it("carries only the failure when nothing was measured, and the measurements when it ran clean", () => {
    const setup = { phase: "setup", reason: "provider instance missing" };
    expect(composeResult(base, null, setup)).toEqual({ ...base, status: "failed", failure: setup });
    expect(composeResult(base, measured, null)).toEqual({ ...base, status: "ok", ...measured });
  });
});

describe("windowFindings (every reason a window falls short, for run.ts to classify)", () => {
  const overLimit = { ...healthy, worstErrorRate: { type: "session.dispatch", rate: 0.3 } };

  it("names an error rate made of timeouts alone call_timeouts, and error_rate once any error is not a timeout", () => {
    expect(windowFindings(overLimit).map((finding) => finding.code)).toEqual(["call_timeouts"]);
    expect(
      windowFindings({ ...overLimit, nonTimeoutErrors: 1 }).map((finding) => finding.code),
    ).toEqual(["error_rate"]);
    expect(windowFindings(healthy)).toEqual([]);
  });

  it("lists every finding, not only the first", () => {
    const findings = windowFindings({
      ...overLimit,
      sessions: 50,
      sessionsWithOutput: 31,
      sessionMcpCallers: 40,
      sessionMcpRequired: 50,
    });
    expect(findings.map((finding) => finding.code)).toEqual([
      "call_timeouts",
      "session_mcp_missing",
      "sessions_starved",
    ]);
    expect(findings.at(-1)).toMatchObject({ sessionsServed: 31, sessionsTotal: 50 });
  });
});

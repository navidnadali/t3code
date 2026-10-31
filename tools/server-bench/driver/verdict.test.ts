import { describe, expect, it } from "vite-plus/test";

import { judgeWindow, type WindowEvidence } from "./verdict.ts";

const healthy: WindowEvidence = {
  worstErrorRate: { type: "poll.mcp.t3_thread_read", rate: 0.005 },
  maxErrorRate: 0.01,
  sessions: 80,
  sessionsWithOutput: 80,
  distribution: { p50Ratio: 1.05, p95Ratio: 0.9, tolerance: 0.2, withinTolerance: true },
  sessionMcpRequired: 80,
  sessionMcpCallers: 80,
};

describe("judgeWindow", () => {
  it("publishes a window that streamed, rarely failed and matched the fixture", () => {
    expect(judgeWindow(healthy)).toBeNull();
    expect(judgeWindow({ ...healthy, distribution: null })).toBeNull();
  });

  it("rejects a window with too many failed calls", () => {
    expect(
      judgeWindow({ ...healthy, worstErrorRate: { type: "session.dispatch", rate: 0.25 } }),
    ).toBe("session.dispatch failed 25.00% of calls (limit 1%)");
  });

  it("rejects a responsive server whose provider produced no output", () => {
    expect(judgeWindow({ ...healthy, sessionsWithOutput: 0 })).toBe(
      "only 0 of 80 sessions produced provider output during the window",
    );
    expect(judgeWindow({ ...healthy, sessionsWithOutput: 79 })).toContain("only 79 of 80");
  });

  it("rejects a window in which some sessions could not call their MCP tools", () => {
    expect(judgeWindow({ ...healthy, sessionMcpCallers: 12 })).toBe(
      "only 12 of 80 sessions had an MCP client when the window opened",
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

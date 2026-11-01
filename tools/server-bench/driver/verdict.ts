import type { DistributionMatch } from "./sampling.ts";

export interface WindowEvidence {
  readonly worstErrorRate: { readonly type: string; readonly rate: number } | null;
  readonly maxErrorRate: number;
  readonly sessions: number;
  /** Sessions whose running turn gained turn items during the window. */
  readonly sessionsWithOutput: number;
  /** History mode only: the exercised threads against the fixture. */
  readonly distribution: DistributionMatch | null;
  /** Sessions that must call their MCP tools (0 when that load is off). */
  readonly sessionMcpRequired: number;
  /** Sessions with a signed-in MCP caller when the window opened. */
  readonly sessionMcpCallers: number;
  /** The driver's own load over the window, with the limits it must stay under. */
  readonly driver: {
    readonly eventLoopUtilization: number;
    readonly eventLoopDelayP99Ms: number;
    readonly maxEventLoopUtilization: number;
    readonly maxEventLoopDelayP99Ms: number;
  };
}

/**
 * Why a complete window still cannot be published, or null when it can: a saturated driver, too
 * many failed calls, sessions that did not stream or call MCP, or threads unlike the fixture.
 */
export function judgeWindow(evidence: WindowEvidence): string | null {
  const driver = evidence.driver;
  if (
    driver.eventLoopUtilization > driver.maxEventLoopUtilization ||
    driver.eventLoopDelayP99Ms > driver.maxEventLoopDelayP99Ms
  ) {
    return `the driver itself was saturated (event loop ${Math.round(driver.eventLoopUtilization * 100)}% busy, delay p99 ${driver.eventLoopDelayP99Ms} ms; limits ${driver.maxEventLoopUtilization * 100}% and ${driver.maxEventLoopDelayP99Ms} ms), so its latencies are not the server's`;
  }
  const worst = evidence.worstErrorRate;
  if (worst !== null && worst.rate > evidence.maxErrorRate) {
    return `${worst.type} failed ${(worst.rate * 100).toFixed(2)}% of calls (limit ${evidence.maxErrorRate * 100}%)`;
  }
  if (evidence.sessionsWithOutput < evidence.sessions) {
    return `only ${evidence.sessionsWithOutput} of ${evidence.sessions} sessions produced provider output during the window`;
  }
  if (evidence.sessionMcpCallers < evidence.sessionMcpRequired) {
    return `only ${evidence.sessionMcpCallers} of ${evidence.sessionMcpRequired} sessions had an MCP client when the window opened`;
  }
  const distribution = evidence.distribution;
  if (distribution !== null && !distribution.withinTolerance) {
    return `the exercised threads do not match the fixture's turn-item distribution (p50 ratio ${distribution.p50Ratio}, p95 ratio ${distribution.p95Ratio}, tolerance ${distribution.tolerance})`;
  }
  return null;
}

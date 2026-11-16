import type { DistributionMatch } from "./sampling.ts";

export interface WindowEvidence {
  readonly worstErrorRate: { readonly type: string; readonly rate: number } | null;
  readonly maxErrorRate: number;
  /** Errors in the window that were not timeouts (an answer such as a 5xx, a decode error, a closed stream). */
  readonly nonTimeoutErrors: number;
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
  return windowVerdict(evidence)?.reason ?? null;
}

/** Why a window failed, as a stable code: "sessions_starved" (the server served only some sessions) is the one a
 * saturated server produces; the others mean the measurement itself is not usable. */
export type WindowFailureCode =
  | "driver_saturated"
  | "error_rate"
  | "call_timeouts"
  | "sessions_starved"
  | "session_mcp_missing"
  | "distribution_mismatch";

export interface WindowVerdict {
  readonly code: WindowFailureCode;
  readonly reason: string;
  /** sessions_starved: the sessions that produced output in the window, and all of them. */
  readonly sessionsServed?: number;
  readonly sessionsTotal?: number;
}

/**
 * The window's verdict with a stable code, checked in this order: a saturated driver (its latencies are not the
 * server's), too many failed calls, sessions without an MCP client, threads unlike the fixture, and last sessions
 * that produced no output (sessions_starved: the server served only some of them, a saturated server's window,
 * which the bench keeps as valid and flags). Null when every session was served and nothing failed.
 */
export function windowVerdict(evidence: WindowEvidence): WindowVerdict | null {
  return windowFindings(evidence)[0] ?? null;
}

/**
 * Every reason the window falls short, in the verdict's order (run.ts classifies each as saturation or defect): a
 * saturated driver; an error rate over the limit (call_timeouts when every error was a timeout, error_rate when any
 * was not); sessions without an MCP client; exercised threads unlike the fixture; and sessions that produced no
 * output. Empty when the window is clean.
 */
export function windowFindings(evidence: WindowEvidence): Array<WindowVerdict> {
  const findings: Array<WindowVerdict> = [];
  const driver = evidence.driver;
  if (
    driver.eventLoopUtilization > driver.maxEventLoopUtilization ||
    driver.eventLoopDelayP99Ms > driver.maxEventLoopDelayP99Ms
  ) {
    findings.push({
      code: "driver_saturated",
      reason: `the driver itself was saturated (event loop ${Math.round(driver.eventLoopUtilization * 100)}% busy, delay p99 ${driver.eventLoopDelayP99Ms} ms; limits ${driver.maxEventLoopUtilization * 100}% and ${driver.maxEventLoopDelayP99Ms} ms), so its latencies are not the server's`,
    });
  }
  const worst = evidence.worstErrorRate;
  if (worst !== null && worst.rate > evidence.maxErrorRate) {
    const rate = `${worst.type} failed ${(worst.rate * 100).toFixed(2)}% of calls (limit ${evidence.maxErrorRate * 100}%)`;
    findings.push(
      evidence.nonTimeoutErrors === 0
        ? { code: "call_timeouts", reason: `${rate}, every error a timeout` }
        : {
            code: "error_rate",
            reason: `${rate}; ${evidence.nonTimeoutErrors} error(s) in the window were not timeouts`,
          },
    );
  }
  if (evidence.sessionMcpCallers < evidence.sessionMcpRequired) {
    findings.push({
      code: "session_mcp_missing",
      reason: `only ${evidence.sessionMcpCallers} of ${evidence.sessionMcpRequired} sessions had an MCP client when the window opened`,
    });
  }
  const distribution = evidence.distribution;
  if (distribution !== null && !distribution.withinTolerance) {
    const test = distribution.test;
    findings.push({
      code: "distribution_mismatch",
      reason:
        test === undefined
          ? `the exercised threads do not match the fixture's turn-item distribution (p50 ratio ${distribution.p50Ratio}, p95 ratio ${distribution.p95Ratio}, tolerance ${distribution.tolerance})`
          : `the exercised threads are not a plausible sample of the fixture: ${test.below_p50} of ${test.n} below its p50 (accepted ${test.band_below[0]}..${test.band_below[1]}, p ${test.p_value_below}), ${test.above_p95} above its p95 (accepted ${test.band_above[0]}..${test.band_above[1]}, p ${test.p_value_above}), alpha ${test.alpha}; p50 ratio ${distribution.p50Ratio}, p95 ratio ${distribution.p95Ratio}`,
    });
  }
  if (evidence.sessionsWithOutput < evidence.sessions) {
    findings.push({
      code: "sessions_starved",
      reason: `only ${evidence.sessionsWithOutput} of ${evidence.sessions} sessions produced provider output during the window`,
      sessionsServed: evidence.sessionsWithOutput,
      sessionsTotal: evidence.sessions,
    });
  }
  return findings;
}

/**
 * driver.json's body: the measured window when it ran clean; only the failure when the run broke before or during the
 * window; and both when a complete window was judged unpublishable, so its statistics (for instance over the sessions
 * a saturated server did serve) stay readable beside the failure. The status stays "failed" then.
 */
export function composeResult<B extends Record<string, unknown>>(
  base: B,
  measured: Record<string, unknown> | null,
  failure: Readonly<Record<string, unknown>> | null,
): Record<string, unknown> & { readonly status: "ok" | "failed" } {
  if (failure === null) return { ...base, status: "ok", ...measured };
  if (failure.phase === "window" && measured !== null) {
    return { ...base, status: "failed", ...measured, failure };
  }
  return { ...base, status: "failed", failure };
}

import type { DistributionMatch } from "./sampling.ts";

export interface WindowEvidence {
  readonly worstErrorRate: { readonly type: string; readonly rate: number } | null;
  readonly maxErrorRate: number;
  readonly sessions: number;
  /** Sessions whose running turn gained turn items during the window. */
  readonly sessionsWithOutput: number;
  /** History mode only: the exercised threads against the fixture. */
  readonly distribution: DistributionMatch | null;
}

/**
 * Why a complete window still cannot be published, or null when it can: too many failed calls,
 * sessions that did not stream, or threads that do not look like the fixture.
 */
export function judgeWindow(evidence: WindowEvidence): string | null {
  const worst = evidence.worstErrorRate;
  if (worst !== null && worst.rate > evidence.maxErrorRate) {
    return `${worst.type} failed ${(worst.rate * 100).toFixed(2)}% of calls (limit ${evidence.maxErrorRate * 100}%)`;
  }
  if (evidence.sessionsWithOutput < evidence.sessions) {
    return `only ${evidence.sessionsWithOutput} of ${evidence.sessions} sessions produced provider output during the window`;
  }
  const distribution = evidence.distribution;
  if (distribution !== null && !distribution.withinTolerance) {
    return `the exercised threads do not match the fixture's turn-item distribution (p50 ratio ${distribution.p50Ratio}, p95 ratio ${distribution.p95Ratio}, tolerance ${distribution.tolerance})`;
  }
  return null;
}

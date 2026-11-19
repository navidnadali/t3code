/**
 * A long run history on one provider thread, tens of thousands of runs: a turn start plans over
 * every run, attempt and provider turn of it. Ids have the product's full length and shared
 * prefixes. Seeded, so a failing count replays.
 */
import {
  MessageId,
  NodeId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

/** Rows run at 2,000 runs and, with T3_HUGE_HISTORY_SCALE=1, at 25,000. */
export const HUGE_HISTORY_RUNS = 25_000;

const THREAD = "3f2b7c1e-8d4a-4e6b-9c5f-1a2b3c4d5e6f";

export interface HugeHistory {
  readonly providerThreadId: ProviderThreadId;
  readonly runs: ReadonlyArray<OrchestrationV2Run>;
  readonly attempts: ReadonlyArray<{ readonly id: RunAttemptId; readonly runId: RunId }>;
  readonly providerTurns: ReadonlyArray<OrchestrationV2ProviderTurn>;
  /** The run a turn start is starting now, after all of them. */
  readonly current: OrchestrationV2Run;
  /** User messages that are compactions or `/goal` commands. */
  readonly compactionMessageIds: ReadonlySet<string>;
}

// The LCG modulo 2^31, in exact 32-bit integer arithmetic: a float product of the state and the
// multiplier passes 2^53 and drops low bits, which skews pairs of draws.
export const seededRandom = (seed: number) => {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1_103_515_245) + 12_345) & 0x7f_ff_ff_ff;
    return state / 2_147_483_648;
  };
};

const runId = (ordinal: number) => RunId.make(`run:thread:${THREAD}:ordinal:${ordinal}`);
const attemptId = (ordinal: number, attempt: number) =>
  RunAttemptId.make(
    `run-attempt:run:run%3Athread%3A${THREAD}%3Aordinal%3A${ordinal}:attempt:${attempt}`,
  );

/**
 * The mix: 85% completed, 8% cancelled, 4% interrupted, 3% failed; 5% retried; 3% restart
 * continuations in chains of 1-4 behind a cut run, which holds restart work two times in three
 * (about 2% of runs); a few non-completing runs' turns still running; a compaction every 2,000 runs.
 */
export const hugeHistory = (count: number, seed: number): HugeHistory => {
  const random = seededRandom(seed);
  const providerThreadId = ProviderThreadId.make(
    `provider-thread:provider:claudeAgent:native-thread:pending%3Arun%3Athread%3A${THREAD}%3Aordinal%3A1`,
  );
  const startedAt = Date.UTC(2026, 9, 1);
  const runs: Array<OrchestrationV2Run> = [];
  const attempts: Array<{ id: RunAttemptId; runId: RunId }> = [];
  const providerTurns: Array<OrchestrationV2ProviderTurn> = [];
  const compactionMessageIds = new Set<string>();
  let chainLeft = 0;
  for (let ordinal = 1; ordinal <= count; ordinal += 1) {
    // A chain of 1-4 continuations starts on about 1.2% of runs: this run is cut and continued.
    const cut = chainLeft === 0 && ordinal < count && random() < 0.012;
    const continues = chainLeft > 0;
    if (continues) chainLeft -= 1;
    if (cut) chainLeft = 1 + Math.floor(random() * 4);
    // A continuation that is cut again is continued in turn, until the chain's last one.
    const cutAgain = continues && chainLeft > 0;
    const draw = random();
    const status =
      cut || cutAgain
        ? "cancelled"
        : draw < 0.85
          ? "completed"
          : draw < 0.93
            ? "cancelled"
            : draw < 0.97
              ? "interrupted"
              : "failed";
    const attemptCount = random() < 0.05 ? 2 + Math.floor(random() * 2) : 1;
    const userMessageId = MessageId.make(`message:${THREAD}:${ordinal}`);
    if (ordinal % 2_000 === 0) compactionMessageIds.add(userMessageId);
    runs.push({
      id: runId(ordinal),
      ordinal,
      providerThreadId,
      userMessageId,
      activeAttemptId: attemptId(ordinal, attemptCount),
      status,
      startedAt: DateTime.makeUnsafe(startedAt + ordinal * 60_000),
      completedAt: DateTime.makeUnsafe(startedAt + ordinal * 60_000 + 30_000),
      ...(continues ? { restartContinuationOfRunId: runId(ordinal - 1) } : {}),
      ...((cut || cutAgain) && random() < 0.65
        ? {
            restartCancelledBackgroundWork: [
              { kind: "subagent" as const, label: `Background agent ${ordinal % 7}` },
            ],
          }
        : {}),
    } as unknown as OrchestrationV2Run);
    for (let attempt = 1; attempt <= attemptCount; attempt += 1) {
      const id = attemptId(ordinal, attempt);
      attempts.push({ id, runId: runId(ordinal) });
      const last = attempt === attemptCount;
      providerTurns.push({
        id: ProviderTurnId.make(`provider-turn:provider:claudeAgent:native-turn:turn%3A${id}`),
        providerThreadId,
        nodeId: NodeId.make(`node:run:${runId(ordinal)}:root`),
        runAttemptId: id,
        nativeTurnRef: null,
        ordinal: providerTurns.length + 1,
        status: !last
          ? "interrupted"
          : status === "completed"
            ? "completed"
            : random() < 0.02
              ? "running"
              : status,
        startedAt: DateTime.makeUnsafe(startedAt + ordinal * 60_000),
        completedAt: DateTime.makeUnsafe(startedAt + ordinal * 60_000 + 30_000),
      } as OrchestrationV2ProviderTurn);
    }
  }
  const current = {
    id: runId(count + 1),
    ordinal: count + 1,
    providerThreadId,
    userMessageId: MessageId.make(`message:${THREAD}:${count + 1}`),
    activeAttemptId: attemptId(count + 1, 1),
    status: "starting",
    startedAt: null,
    completedAt: null,
  } as OrchestrationV2Run;
  return { providerThreadId, runs, attempts, providerTurns, current, compactionMessageIds };
};

/**
 * The same records behind proxies that count every property read, so a row can bound how often a
 * planner touches each record without a clock.
 */
export const countReads = <A extends object>(
  records: ReadonlyArray<A>,
  counter: { reads: number },
) =>
  records.map(
    (record) =>
      new Proxy(record, {
        get(target, key, receiver) {
          counter.reads += 1;
          return Reflect.get(target, key, receiver);
        },
      }),
  );

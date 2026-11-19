import { assert, it } from "@effect/vitest";

import { abandonedProviderTurns } from "./ProviderTurnStartService.ts";
import { countReads, HUGE_HISTORY_RUNS, hugeHistory } from "./testkit/HugeHistory.ts";

// A turn start on a thread with tens of thousands of runs must not block the event loop finding
// abandoned provider turns by scanning every attempt for each run that ended without completing.
// It reads each run, attempt and provider turn a bounded number of times instead.
const ABANDONED_TURN_READS_PER_RECORD = 4;

const abandonedTurnReadsRow = (count: number) => () => {
  const history = hugeHistory(count, 27_027);
  const counter = { reads: 0 };
  const runs = countReads(history.runs, counter);
  const attempts = countReads(history.attempts, counter);
  const providerTurns = countReads(history.providerTurns, counter);
  const abandoned = abandonedProviderTurns(
    { runs, attempts, providerTurns },
    history.current,
    history.providerThreadId,
  );
  const records = runs.length + attempts.length + providerTurns.length;
  assert.isAbove(abandoned.length, 0);
  assert.isAtMost(
    counter.reads,
    ABANDONED_TURN_READS_PER_RECORD * records,
    `${counter.reads} reads of ${records} records`,
  );
};

it(
  "finds a turn start's abandoned provider turns reading each record a bounded number of times",
  abandonedTurnReadsRow(2_000),
);

it.skipIf(process.env.T3_HUGE_HISTORY_SCALE !== "1")(
  "finds abandoned provider turns in bounded reads per record at 25,000 runs",
  abandonedTurnReadsRow(HUGE_HISTORY_RUNS),
);

// Most turn starts find no running provider turn on their thread. Then none can be abandoned, and a
// start reads only each provider turn's thread and status, not the thread's runs and attempts.
const QUIET_THREAD_READS_PER_TURN = 2;

it("finds no abandoned provider turn on a thread with none running from its turns alone", () => {
  const history = hugeHistory(2_000, 27_027);
  const counter = { reads: 0 };
  const runs = countReads(history.runs, counter);
  const attempts = countReads(history.attempts, counter);
  const providerTurns = countReads(
    history.providerTurns.map((turn) =>
      turn.status === "running" ? { ...turn, status: "interrupted" as const } : turn,
    ),
    counter,
  );
  assert.deepEqual(
    abandonedProviderTurns(
      { runs, attempts, providerTurns },
      history.current,
      history.providerThreadId,
    ),
    [],
  );
  assert.isAtMost(
    counter.reads,
    QUIET_THREAD_READS_PER_TURN * providerTurns.length,
    `${counter.reads} reads of ${providerTurns.length} provider turns`,
  );
});

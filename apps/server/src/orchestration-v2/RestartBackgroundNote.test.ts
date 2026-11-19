import { assert, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  type OrchestrationV2Run,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import {
  cancelledRosterTaskWork,
  cancelledTurnItemWork,
  delegatedTaskRuns,
  isAwaitedWork,
  isRestartNoteContinuation,
  latestRanFirst,
  pendingRestartCancelledBackgroundWork,
  restartContinuationNote,
  restartNoteCompactionCandidates,
  mergeRestartCancelledBackgroundWork,
  planRestartNote,
} from "./RestartBackgroundNote.ts";
import { countReads, HUGE_HISTORY_RUNS, hugeHistory } from "./testkit/HugeHistory.ts";

const claudeThread = ProviderThreadId.make("provider-thread:claude");
const codexThread = ProviderThreadId.make("provider-thread:codex");
const lost = [{ kind: "subagent" as const, label: "Background subagent test" }];

function run(
  ordinal: number,
  providerThreadId: ProviderThreadId,
  extra: Partial<OrchestrationV2Run> = {},
): OrchestrationV2Run {
  return {
    id: RunId.make(`run:${ordinal}`),
    ordinal,
    providerThreadId,
    userMessageId: MessageId.make(`message:${ordinal}`),
    activeAttemptId: RunAttemptId.make(`attempt:${ordinal}`),
    status: "completed",
    ...extra,
  } as OrchestrationV2Run;
}

const turnFor = (source: OrchestrationV2Run) => ({
  runAttemptId: source.activeAttemptId,
  providerThreadId: source.providerThreadId!,
  status: "completed" as const,
});

it("keeps the note for the provider thread that lost the work across a provider switch", () => {
  const root = run(1, claudeThread, { restartCancelledBackgroundWork: lost });
  const onCodex = run(2, codexThread);
  const backOnClaude = run(3, claudeThread);
  const pending = (target: OrchestrationV2Run, runs: ReadonlyArray<OrchestrationV2Run>) =>
    pendingRestartCancelledBackgroundWork({
      runs,
      providerTurns: runs.filter((source) => source.id !== target.id).map(turnFor),
      compactionMessageIds: new Set(),
      run: target,
      attempts: [{ id: target.activeAttemptId!, runId: target.id }],
    });

  assert.deepEqual(pending(onCodex, [root, onCodex]), []);
  assert.deepEqual(pending(backOnClaude, [root, onCodex, backOnClaude]), lost);
  const later = run(4, claudeThread);
  assert.deepEqual(pending(later, [root, onCodex, backOnClaude, later]), []);
});

it("delivers a resumed queued run's note even after a higher-ordinal run", () => {
  const ranFirst = run(3, claudeThread, {
    completedAt: DateTime.makeUnsafe("2026-10-03T10:00:00.000Z"),
  });
  const resumed = run(2, claudeThread, {
    completedAt: DateTime.makeUnsafe("2026-10-03T10:05:00.000Z"),
    restartCancelledBackgroundWork: lost,
  });
  const next = run(4, claudeThread);
  const runs = [resumed, ranFirst, next];
  assert.deepEqual(
    pendingRestartCancelledBackgroundWork({
      runs,
      providerTurns: [resumed, ranFirst].map(turnFor),
      compactionMessageIds: new Set(),
      run: next,
      attempts: [{ id: next.activeAttemptId!, runId: next.id }],
    }),
    lost,
  );
});

// Turn start reads the messages restartNoteCompactionCandidates names, up to the first that is not a
// compaction, besides its own and any a handoff names. On any history that gives the same note as
// every message's compaction.
it("finds the same restart note from its candidates' compactions as from every message's", () => {
  let seed = 20_261_117;
  const random = () => {
    seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648;
    return seed / 2_147_483_648;
  };
  const pick = <A>(values: ReadonlyArray<A>) => values[Math.floor(random() * values.length)]!;
  const statuses = [
    "completed",
    "completed",
    "completed",
    "failed",
    "interrupted",
    "cancelled",
    "rolled_back",
    "waiting",
  ] as const;
  let notes = 0;
  for (let trial = 0; trial < 4_000; trial += 1) {
    const runs: Array<OrchestrationV2Run> = [];
    const count = 2 + Math.floor(random() * 10);
    for (let ordinal = 1; ordinal <= count; ordinal += 1) {
      const earlier = runs.length === 0 || random() > 0.2 ? undefined : pick(runs).id;
      runs.push(
        run(ordinal, random() < 0.75 ? claudeThread : codexThread, {
          status: ordinal === count ? "starting" : pick(statuses),
          completedAt:
            ordinal === count || random() < 0.15
              ? null
              : DateTime.makeUnsafe(Date.UTC(2026, 10, 17, 10, Math.floor(random() * 30))),
          ...(random() < 0.1 ? { activeAttemptId: null } : {}),
          ...(random() < 0.35
            ? { restartCancelledBackgroundWork: [{ kind: "subagent", label: `lost ${ordinal}` }] }
            : {}),
          ...(earlier === undefined ? {} : { restartContinuationOfRunId: earlier }),
        }),
      );
    }
    const target = runs.at(-1)!;
    const attempts = runs.flatMap((source) => [
      ...(source.activeAttemptId === null
        ? []
        : [{ id: source.activeAttemptId, runId: source.id }]),
      ...(random() < 0.2
        ? [{ id: RunAttemptId.make(`attempt:${source.ordinal}:earlier`), runId: source.id }]
        : []),
    ]);
    const providerTurns = attempts
      .filter(() => random() < 0.7)
      .map((attempt) => ({
        runAttemptId: attempt.id,
        providerThreadId: runs.find((source) => source.id === attempt.runId)!.providerThreadId!,
        status: pick(["completed", "completed", "completed", "cancelled", "running"] as const),
      }));
    const compactions = new Set(
      runs.filter(() => random() < 0.3).map((source) => source.userMessageId),
    );
    const input = { runs, providerTurns, run: target, attempts };
    const every = pendingRestartCancelledBackgroundWork({
      ...input,
      compactionMessageIds: compactions,
    });
    // Its own message and handoff targets' are read by key; some of those are compactions.
    const read = new Set(
      runs
        .filter((source) => source.id === target.id || random() < 0.2)
        .map((source) => source.userMessageId)
        .filter((id) => compactions.has(id)),
    );
    for (const candidate of restartNoteCompactionCandidates(input)) {
      if (!compactions.has(candidate.userMessageId)) break;
      read.add(candidate.userMessageId);
    }
    assert.deepEqual(
      pendingRestartCancelledBackgroundWork({ ...input, compactionMessageIds: read }),
      every,
    );
    if (every.length > 0) notes += 1;
  }
  assert.isAbove(notes, 200);
});

// regression: #19750 wake threads whose subagents' background work a restart cut
it("names a lost subagent by the id the agent resumes it with", () => {
  const ownShell = cancelledRosterTaskWork({
    kind: "command",
    taskId: "shell-1",
    description: "sleep 150",
    parentTaskId: "agent-1",
  });
  assert.deepEqual(ownShell, {
    kind: "subagent",
    label: "sleep 150 (subagent agent-1)",
    id: "agent-1",
  });
  assert.isTrue(isAwaitedWork(ownShell));
  const long = cancelledRosterTaskWork({
    kind: "command",
    taskId: "shell-1",
    description: "d".repeat(400),
    parentTaskId: "agent-1",
  }).label;
  assert.lengthOf(long, 160);
  assert.isTrue(long.endsWith("(subagent agent-1)"));
  const longId = cancelledRosterTaskWork({
    kind: "command",
    taskId: "shell-1",
    parentTaskId: "a".repeat(400),
  }).label;
  assert.lengthOf(longId, 160);
  assert.isTrue(
    cancelledRosterTaskWork({
      kind: "command",
      taskId: "shell-1",
      description: `${"d".repeat(158)}🚀 deploy`,
    }).label.isWellFormed(),
  );
  assert.isFalse(isAwaitedWork(cancelledRosterTaskWork({ kind: "command", taskId: "shell-2" })));
  assert.isTrue(
    isAwaitedWork(cancelledRosterTaskWork({ kind: "command", taskId: "tests", awaited: true })),
  );
  assert.isFalse(isAwaitedWork(cancelledRosterTaskWork({ kind: "monitor", taskId: "watchdog" })));
  assert.isTrue(
    isAwaitedWork(cancelledRosterTaskWork({ kind: "monitor", taskId: "ci", awaited: true })),
  );
  const codexCommand = { id: "item-2", type: "command_execution", input: "sleep 180" };
  assert.isFalse(isAwaitedWork(cancelledTurnItemWork(codexCommand as never)!));
  assert.isTrue(
    isAwaitedWork(cancelledTurnItemWork({ ...codexCommand, completionWake: "always" } as never)!),
  );
  assert.equal(
    cancelledTurnItemWork({
      id: "item-1",
      type: "subagent",
      origin: "provider_native",
      title: "Review",
      nativeItemRef: { driver: "claudeAgent", nativeId: "agent-1", strength: "strong" },
    } as never)?.label,
    "Review (id agent-1)",
  );
});

it("keeps separate cancelled tasks that share a kind and label", () => {
  const first = { kind: "shell" as const, label: "sleep 20", id: "item-1" };
  const second = { kind: "shell" as const, label: "sleep 20", id: "item-2" };
  const merged = mergeRestartCancelledBackgroundWork([first], [second, first]);
  assert.deepEqual(merged, [first, second]);
  const legacy = { kind: "shell" as const, label: "sleep 20" };
  assert.lengthOf(mergeRestartCancelledBackgroundWork([legacy], [legacy]), 1);
});

it("does not repeat the note when a steer restarts the run on a new attempt", () => {
  const root = run(1, claudeThread, { restartCancelledBackgroundWork: lost });
  const steered = run(2, claudeThread, {
    activeAttemptId: RunAttemptId.make("attempt:2b"),
  });
  const firstAttempt = RunAttemptId.make("attempt:2a");
  const pending = (runAttemptIds: ReadonlyArray<RunAttemptId>, delivered: boolean) =>
    pendingRestartCancelledBackgroundWork({
      runs: [root, steered],
      providerTurns: [
        turnFor(root),
        ...(delivered
          ? [
              {
                runAttemptId: firstAttempt,
                providerThreadId: claudeThread,
                status: "completed" as const,
              },
            ]
          : []),
      ],
      compactionMessageIds: new Set(),
      run: steered,
      attempts: runAttemptIds.map((id) => ({ id, runId: steered.id })),
    });

  assert.deepEqual(pending([firstAttempt, RunAttemptId.make("attempt:2b")], true), []);
  assert.deepEqual(pending([firstAttempt, RunAttemptId.make("attempt:2b")], false), lost);
});

it("counts a prompted mid-turn or chained continuation as delivering the note", () => {
  const cut = run(1, claudeThread, { status: "cancelled", restartCancelledBackgroundWork: lost });
  const cutTurn = { ...turnFor(cut), status: "cancelled" as const };
  const continuation = run(2, claudeThread, { restartContinuationOfRunId: cut.id });
  assert.isTrue(isRestartNoteContinuation(continuation, [cut, continuation], [cutTurn], []));
  assert.isFalse(
    isRestartNoteContinuation(
      continuation,
      [{ ...cut, restartCancelledBackgroundWork: [] }, continuation],
      [cutTurn],
      [],
    ),
  );
  const later = run(3, claudeThread);
  const pending = (delivered: boolean) =>
    pendingRestartCancelledBackgroundWork({
      runs: [cut, continuation, later],
      providerTurns: [cutTurn, ...(delivered ? [turnFor(continuation)] : [])],
      compactionMessageIds: new Set(),
      run: later,
      attempts: [{ id: later.activeAttemptId!, runId: later.id }],
    });
  assert.deepEqual(pending(true), []);
  assert.deepEqual(pending(false), lost);

  const unstarted = { ...continuation, status: "cancelled" as const };
  const chained = run(3, claudeThread, { restartContinuationOfRunId: unstarted.id });
  assert.deepEqual(restartContinuationNote(unstarted, [cut, unstarted], [cutTurn], []), {
    work: lost,
    settled: false,
  });
  assert.isTrue(isRestartNoteContinuation(chained, [cut, unstarted, chained], [cutTurn], []));
  for (const status of ["running", "cancelled"] as const) {
    const turns = [cutTurn, { ...turnFor(unstarted), status }];
    assert.deepEqual(restartContinuationNote(unstarted, [cut, unstarted], turns, []), {
      work: lost,
      settled: false,
    });
    assert.deepEqual(
      pendingRestartCancelledBackgroundWork({
        runs: [cut, unstarted, later],
        providerTurns: turns,
        compactionMessageIds: new Set(),
        run: later,
        attempts: [{ id: later.activeAttemptId!, runId: later.id }],
      }),
      lost,
    );
  }

  const steered = { ...unstarted, activeAttemptId: RunAttemptId.make("attempt:replacement") };
  const attempts = [{ id: unstarted.activeAttemptId!, runId: steered.id }];
  const turns = [
    cutTurn,
    turnFor(unstarted),
    { ...turnFor(steered), status: "cancelled" as const },
  ];
  assert.deepEqual(restartContinuationNote(steered, [cut, steered], turns, attempts).work, []);
  assert.isFalse(isRestartNoteContinuation(chained, [cut, steered, chained], turns, attempts));
  assert.deepEqual(
    pendingRestartCancelledBackgroundWork({
      runs: [cut, steered, later],
      providerTurns: turns,
      compactionMessageIds: new Set(),
      run: later,
      attempts,
    }),
    [],
  );
});

it("counts a merged completion wake for the task of any parent run it carries", () => {
  const taskId = NodeId.make("node:follow-up");
  const owned = run(1, claudeThread, { delegatedTaskId: taskId });
  // The wake's first part is a run this read leaves out of the task (its own chain is elsewhere).
  const elsewhere = run(2, claudeThread);
  const wake = run(3, claudeThread);
  const later = run(4, claudeThread);
  const part = (parentRunId: RunId, task: string) => ({
    parentRunId,
    generation: 1,
    taskIds: [NodeId.make(task)],
  });
  const runs = [owned, elsewhere, wake, later];
  const messages = [
    {
      runId: wake.id,
      delegatedCompletion: {
        ...part(elsewhere.id, "node:a"),
        taskIds: [NodeId.make("node:a"), NodeId.make("node:b")],
        merged: [part(owned.id, "node:b")],
      },
    },
    // The next generation's wake, for results the merged wake's turn delegated.
    { runId: later.id, delegatedCompletion: part(wake.id, "node:c") },
  ];

  assert.deepEqual(
    delegatedTaskRuns({ runs, messages }, taskId).map((source) => source.id),
    [owned.id, wake.id, later.id],
  );
});

// A turn start on a thread with tens of thousands of runs must not block the event loop here. Its
// restart-note planning reads each run, attempt and provider turn a bounded number of times, and
// works out whether a run prompted the thread only for the latest-ran runs it takes: about 4 reads
// per record, against about 7 when it does so for every run.
const RESTART_NOTE_READS_PER_RECORD = 5.5;
// It lists the thread's runs in ran order once, then reads only the runs it takes: about 5 reads
// per run, against 9 when it lists them twice and 12 when it works out every run's verdict.
const RESTART_NOTE_READS_PER_RUN = 6.5;

const planTurnStartRestartNote = (
  history: ReturnType<typeof hugeHistory>,
  counter: { reads: number },
  runCounter = { reads: 0 },
) => {
  const runs = countReads(countReads(history.runs, counter), runCounter);
  const attempts = countReads(history.attempts, counter);
  const providerTurns = countReads(history.providerTurns, counter);
  const [current] = countReads([history.current], counter);
  // As ProviderTurnStartService.start makes them, reading candidates' messages up to the first that
  // is not a compaction.
  const plan = planRestartNote({ runs, providerTurns, run: current!, attempts });
  plan.isContinuation(current!);
  const compactions = new Set<string>();
  let candidates = 0;
  for (const candidate of plan.compactionCandidates()) {
    candidates += 1;
    if (!history.compactionMessageIds.has(candidate.userMessageId)) break;
    compactions.add(candidate.userMessageId);
  }
  plan.pending(compactions);
  return { records: runs.length + attempts.length + providerTurns.length, candidates };
};

const restartNoteReadsRow = (count: number) => () => {
  const history = hugeHistory(count, 27_027);
  const counter = { reads: 0 };
  const runReads = { reads: 0 };
  const { records, candidates } = planTurnStartRestartNote(history, counter, runReads);
  assert.isAbove(candidates, 0);
  assert.isAtMost(
    counter.reads,
    RESTART_NOTE_READS_PER_RECORD * records,
    `${counter.reads} reads of ${records} records`,
  );
  assert.isAtMost(
    runReads.reads,
    RESTART_NOTE_READS_PER_RUN * history.runs.length,
    `${runReads.reads} reads of ${history.runs.length} runs`,
  );
};

it(
  "plans a turn start's restart note reading each run, attempt and turn a bounded number of times",
  restartNoteReadsRow(2_000),
);

it.skipIf(process.env.T3_HUGE_HISTORY_SCALE !== "1")(
  "plans a turn start's restart note in bounded reads per record at 25,000 runs",
  restartNoteReadsRow(HUGE_HISTORY_RUNS),
);

// Adversarial: many continuations of one long chain of cut runs. Each continuation's check walks
// the chain; walking it once for all of them keeps the reads linear in the chain's length (about
// 10 per record here, against hundreds when each walks it).
const CHAIN_FAN_IN_READS_PER_RECORD = 16;

// A cut chain of `chain` cancelled continuations, each holding `work(index)`, then `fanIn`
// completed runs continuing its last one. With `loop`, its first run continues its last.
const withCutChain = (
  chain: number,
  fanIn: number,
  work: (
    index: number,
  ) => ReadonlyArray<NonNullable<OrchestrationV2Run["restartCancelledBackgroundWork"]>[number]>,
  loop = false,
) => {
  const history = hugeHistory(200, 27_028);
  const base = history.runs.length;
  const runs = [...history.runs];
  const attempts = [...history.attempts];
  const providerTurns = [...history.providerTurns];
  const template = history.runs.at(-1)!;
  const turn = history.providerTurns.at(-1)!;
  const add = (
    index: number,
    extra: Partial<OrchestrationV2Run>,
    status: "completed" | "cancelled",
  ) => {
    const ordinal = base + index;
    const id = RunId.make(`${template.id}:extra:${index}`);
    const attemptId = RunAttemptId.make(`${template.activeAttemptId}:extra:${index}`);
    runs.push({
      ...template,
      id,
      ordinal,
      activeAttemptId: attemptId,
      userMessageId: MessageId.make(`${template.userMessageId}:extra:${index}`),
      completedAt: DateTime.makeUnsafe(Date.UTC(2026, 10, 1) + ordinal * 60_000),
      status,
      ...extra,
    } as OrchestrationV2Run);
    attempts.push({ id: attemptId, runId: id });
    providerTurns.push({
      ...turn,
      runAttemptId: attemptId,
      ordinal: providerTurns.length + 1,
      status,
    });
    return id;
  };
  let previous = add(1, { restartCancelledBackgroundWork: work(1) }, "cancelled");
  for (let index = 2; index <= chain; index += 1) {
    previous = add(
      index,
      { restartContinuationOfRunId: previous, restartCancelledBackgroundWork: work(index) },
      "cancelled",
    );
  }
  if (loop) runs[base] = { ...runs[base]!, restartContinuationOfRunId: previous };
  for (let index = 1; index <= fanIn; index += 1) {
    add(chain + index, { restartContinuationOfRunId: previous }, "completed");
  }
  const current = {
    ...history.current,
    ordinal: base + chain + fanIn + 1,
  } as OrchestrationV2Run;
  return { ...history, runs, attempts, providerTurns, current };
};

it("plans a restart note over many continuations of one long cut chain in bounded reads", () => {
  const counter = { reads: 0 };
  const { records } = planTurnStartRestartNote(
    withCutChain(400, 400, () => lost),
    counter,
  );
  assert.isAtMost(
    counter.reads,
    CHAIN_FAN_IN_READS_PER_RECORD * records,
    `${counter.reads} reads of ${records} records`,
  );
});

// A chain that loops back on itself: every run on the loop passes the whole loop, wherever its walk
// starts, so one walk answers for them all. Each continuation's message here is a compaction, so the
// start takes every one as a candidate and asks whether it continues a note.
it("plans a restart note over many continuations of one cut chain that loops in bounded reads", () => {
  const history = withCutChain(400, 400, () => lost, true);
  const compactionMessageIds = new Set([
    ...history.compactionMessageIds,
    ...history.runs.slice(-400).map((run) => run.userMessageId as string),
  ]);
  const counter = { reads: 0 };
  const { records, candidates } = planTurnStartRestartNote(
    { ...history, compactionMessageIds },
    counter,
  );
  assert.isAbove(candidates, 400);
  assert.isAtMost(
    counter.reads,
    CHAIN_FAN_IN_READS_PER_RECORD * records,
    `${counter.reads} reads of ${records} records`,
  );
});

// A continuation check needs only whether a chain carries work, not the chain's merged work: a cut
// chain whose runs each hold distinct work reads each entry a bounded number of times, not once
// per run after it.
const CHAIN_WORK_READS_PER_ENTRY = 4;

it("plans a restart note over a long cut chain of distinct work in reads linear in its work", () => {
  const workReads = { reads: 0 };
  let entries = 0;
  const history = withCutChain(1_600, 1, (index) => {
    entries += 1;
    return countReads(
      [{ kind: "subagent" as const, label: `Background agent ${index}` }],
      workReads,
    );
  });
  planTurnStartRestartNote(history, { reads: 0 });
  assert.isAtMost(
    workReads.reads,
    CHAIN_WORK_READS_PER_ENTRY * entries,
    `${workReads.reads} reads of ${entries} work entries`,
  );
});

// Compaction candidates come latest-ran first. A thread's runs are in ran order already, as a
// rule, so ordering them reads each run's cached key a few times, not once per heap level.
const ORDER_KEY_READS_PER_RUN = 4;

it("orders a thread's runs latest-ran first in key reads linear in its runs", () => {
  const keyReads = { reads: 0 };
  const counted = (values: ReadonlyArray<number>) =>
    new Proxy(values, {
      get(target, key, receiver) {
        if (typeof key === "string" && /^\d+$/.test(key)) keyReads.reads += 1;
        return Reflect.get(target, key, receiver);
      },
    });
  // Completed runs, then the last few still running (no completion time).
  const ends = counted(
    Array.from({ length: HUGE_HISTORY_RUNS }, (_, position) =>
      position < HUGE_HISTORY_RUNS - 3
        ? Date.UTC(2026, 9, 1) + position * 60_000
        : Number.POSITIVE_INFINITY,
    ),
  );
  const ordinals = counted(
    Array.from({ length: HUGE_HISTORY_RUNS }, (_, position) => position + 1),
  );
  const order = latestRanFirst(ends, ordinals);
  assert.deepEqual(order.slice(0, 4), [
    HUGE_HISTORY_RUNS - 1,
    HUGE_HISTORY_RUNS - 2,
    HUGE_HISTORY_RUNS - 3,
    HUGE_HISTORY_RUNS - 4,
  ]);
  assert.strictEqual(order.at(-1), 0);
  assert.isAtMost(
    keyReads.reads,
    ORDER_KEY_READS_PER_RUN * HUGE_HISTORY_RUNS,
    `${keyReads.reads} key reads of ${HUGE_HISTORY_RUNS} runs`,
  );
});

// A thread none of whose runs prompted it: a caller that takes every candidate asks about each run
// once and is given none.
const UNPROMPTED_DRAIN_READS_PER_RECORD = 6;

it("takes every compaction candidate of a thread no run prompted in bounded reads", () => {
  const history = hugeHistory(2_000, 27_027);
  const counter = { reads: 0 };
  const runs = countReads(
    history.runs.map((run) => ({ ...run, status: "interrupted" as const })),
    counter,
  );
  const attempts = countReads(history.attempts, counter);
  const providerTurns = countReads(
    history.providerTurns.map((turn) => ({ ...turn, status: "interrupted" as const })),
    counter,
  );
  const plan = planRestartNote({ runs, providerTurns, run: history.current, attempts });
  assert.deepEqual([...plan.compactionCandidates()], []);
  const records = runs.length + attempts.length + providerTurns.length;
  assert.isAtMost(
    counter.reads,
    UNPROMPTED_DRAIN_READS_PER_RECORD * records,
    `${counter.reads} reads of ${records} records`,
  );
});

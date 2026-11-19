import { assert, it } from "@effect/vitest";
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

import { abandonedProviderTurns } from "./ProviderTurnStartService.ts";
import {
  isRestartNoteContinuation,
  planRestartNote,
  restartContinuationNote,
  restartNoteIndex,
  turnStartIndex,
} from "./RestartBackgroundNote.ts";
import { seededRandom } from "./testkit/HugeHistory.ts";
import {
  referenceAbandonedProviderTurns,
  referenceIsRestartNoteContinuation,
  referencePendingRestartCancelledBackgroundWork,
  referenceRestartContinuationNote,
  referenceRestartNoteCompactionCandidates,
} from "./testkit/TurnStartPlanningReference.ts";

const threads = [
  ProviderThreadId.make("provider-thread:a"),
  ProviderThreadId.make("provider-thread:b"),
] as const;
const statuses = [
  "completed",
  "completed",
  "completed",
  "cancelled",
  "interrupted",
  "failed",
  "rolled_back",
  "waiting",
  "running",
  "starting",
] as const;
const turnStatuses = [
  "completed",
  "completed",
  "completed",
  "cancelled",
  "running",
  "interrupted",
  "failed",
] as const;
const labels = ["agent a", "agent b", "shell", "monitor"] as const;

// A small projection exercising what the planning reads: runs on two provider threads (and none),
// every status, missing or duplicate attempts, turns on the other thread, continuation chains that
// end, point nowhere, repeat or loop, duplicate run ids, shared and repeated work entries, and
// completion times that tie.
const randomProjection = (random: () => number) => {
  const pick = <A>(values: ReadonlyArray<A>) => values[Math.floor(random() * values.length)]!;
  const count = 2 + Math.floor(random() * 30);
  const runs: Array<OrchestrationV2Run> = [];
  for (let ordinal = 1; ordinal <= count; ordinal += 1) {
    const reused = runs.length > 0 && random() < 0.03;
    const id = reused ? pick(runs).id : RunId.make(`run:${ordinal}`);
    const draw = random();
    const continuation =
      draw < 0.2 && runs.length > 0
        ? pick(runs).id
        : draw < 0.24
          ? RunId.make(`run:${1 + Math.floor(random() * (count + 2))}`)
          : draw < 0.27
            ? RunId.make("run:missing")
            : draw < 0.29
              ? id
              : undefined;
    const work =
      random() < 0.35
        ? Array.from({ length: 1 + Math.floor(random() * 3) }, () => ({
            kind: pick(["subagent", "shell", "task"] as const),
            label: pick(labels),
            ...(random() < 0.3 ? { id: `task:${pick(labels)}` } : {}),
          }))
        : undefined;
    runs.push({
      id,
      ordinal,
      providerThreadId: random() < 0.1 ? null : random() < 0.65 ? threads[0] : threads[1],
      userMessageId: MessageId.make(`message:${ordinal}`),
      activeAttemptId: random() < 0.1 ? null : RunAttemptId.make(`attempt:${ordinal}:active`),
      status: pick(statuses),
      completedAt:
        random() < 0.15
          ? null
          : DateTime.makeUnsafe(Date.UTC(2026, 10, 17, 10, Math.floor(random() * 12))),
      ...(continuation === undefined ? {} : { restartContinuationOfRunId: continuation }),
      ...(work === undefined ? {} : { restartCancelledBackgroundWork: work }),
    } as unknown as OrchestrationV2Run);
  }
  const attempts = runs.flatMap((run) => [
    ...(run.activeAttemptId === null ? [] : [{ id: run.activeAttemptId, runId: run.id }]),
    ...Array.from({ length: random() < 0.25 ? 1 + Math.floor(random() * 2) : 0 }, (_, index) => ({
      id: RunAttemptId.make(`attempt:${run.ordinal}:earlier:${index}`),
      runId: run.id,
    })),
    ...(random() < 0.03
      ? [
          {
            id: RunAttemptId.make(`attempt:${run.ordinal}:orphan`),
            runId: RunId.make("run:missing"),
          },
        ]
      : []),
  ]);
  // Most attempts have one provider turn; some none, and some a second (a continued turn).
  const providerTurns: Array<OrchestrationV2ProviderTurn> = attempts.flatMap((attempt, index) =>
    Array.from(
      { length: random() < 0.25 ? 0 : random() < 0.15 ? 2 : 1 },
      (_, turn) =>
        ({
          id: ProviderTurnId.make(`turn:${index}:${turn}`),
          providerThreadId:
            random() < 0.1
              ? pick(threads)
              : (runs.find((run) => run.id === attempt.runId)?.providerThreadId ?? threads[0]),
          nodeId: NodeId.make("node:root"),
          runAttemptId: random() < 0.04 ? null : attempt.id,
          nativeTurnRef: null,
          ordinal: index * 2 + turn + 1,
          status: pick(turnStatuses),
          startedAt: null,
          completedAt: null,
        }) as OrchestrationV2ProviderTurn,
    ),
  );
  // About one projection in eight closes a loop of continuations among runs that never completed,
  // and most of those lead a tail of 1-3 such runs into it.
  const loops = random() < 0.125 && runs.length >= 3;
  let tail = 0;
  if (loops) {
    const loop = runs.slice(-1 - Math.floor(random() * Math.min(4, runs.length - 1)));
    const before = runs.slice(0, runs.length - loop.length);
    tail = Math.min(before.length, Math.floor(random() * 4));
    const cut = [...before.slice(before.length - tail), ...loop].map((run) => runs.indexOf(run));
    cut.forEach((position, index) => {
      const next =
        index < tail
          ? runs[cut[index + 1]!]!
          : runs[cut[tail + ((index - tail + 1) % loop.length)]!]!;
      runs[position] = {
        ...runs[position]!,
        status: "cancelled",
        restartContinuationOfRunId: next.id,
      } as OrchestrationV2Run;
    });
    const cutAttempts = new Set(
      attempts
        .filter((attempt) => cut.some((position) => runs[position]!.id === attempt.runId))
        .map((attempt) => attempt.id),
    );
    for (const turn of providerTurns.splice(0)) {
      if (!cutAttempts.has(turn.runAttemptId!)) providerTurns.push(turn);
    }
  }
  const compactions = new Set(
    runs.filter(() => random() < 0.25).map((run) => run.userMessageId as string),
  );
  return { runs, attempts, providerTurns, compactions, loops, tail };
};

// The linear planning gives exactly what the quadratic one gave, on every projection.
it("plans a turn start exactly as the quadratic planning did on seeded random projections", () => {
  const random = seededRandom(27_027);
  const covered = {
    notes: 0,
    walks: 0,
    loops: 0,
    tails: 0,
    candidates: 0,
    pending: 0,
    abandoned: 0,
    detached: 0,
  };
  for (let trial = 0; trial < 3_000; trial += 1) {
    const { runs, attempts, providerTurns, compactions, loops, tail } = randomProjection(random);
    if (loops) covered.loops += 1;
    if (tail > 1) covered.tails += 1;
    for (const source of runs) {
      const note = restartContinuationNote(source, runs, providerTurns, attempts);
      assert.deepEqual(
        note,
        referenceRestartContinuationNote(source, runs, providerTurns, attempts),
      );
      if (note.work.length > (source.restartCancelledBackgroundWork?.length ?? 0))
        covered.walks += 1;
      if (note.work.length > 0) covered.notes += 1;
    }
    // One index answers every run's note in any order, as its remembered notes must not leak
    // between walks that loop or start from a copy.
    const index = restartNoteIndex(runs, providerTurns, attempts);
    const order = runs.map((run) => [random(), run] as const).toSorted((a, b) => a[0] - b[0]);
    for (const [, source] of order) {
      const copy = { ...source, status: "cancelled" } as OrchestrationV2Run;
      for (const asked of random() < 0.5 ? [source, copy] : [copy, source]) {
        assert.deepEqual(
          index.noteFrom(asked),
          referenceRestartContinuationNote(asked, runs, providerTurns, attempts),
        );
      }
    }
    // A run object that is not the projection's run of its id walks on its own.
    const copy = {
      ...runs[Math.floor(random() * runs.length)]!,
      status: "cancelled",
    } as OrchestrationV2Run;
    if (copy.restartContinuationOfRunId !== undefined) covered.detached += 1;
    assert.deepEqual(
      restartContinuationNote(copy, runs, providerTurns, attempts),
      referenceRestartContinuationNote(copy, runs, providerTurns, attempts),
    );
    // The run starting: the projection's newest, or one it does not hold yet.
    const current =
      random() < 0.5
        ? runs.at(-1)!
        : ({
            ...runs.at(-1)!,
            id: RunId.make("run:starting"),
            ordinal: runs.length + 1,
            completedAt: null,
            ...(random() < 0.2 ? { restartContinuationOfRunId: runs[0]!.id } : {}),
          } as unknown as OrchestrationV2Run);
    const input = { runs, providerTurns, run: current, attempts };
    // The turn start shares one index between its restart note and its abandoned turns.
    const shared = turnStartIndex(runs, providerTurns, attempts);
    const plan = planRestartNote(input, shared);
    const asked = [...runs, current, ...runs]
      .map((run) => [random(), run] as const)
      .toSorted((a, b) => a[0] - b[0])
      .map(([, run]) => run);
    for (const run of asked) {
      const expected = referenceIsRestartNoteContinuation(run, runs, providerTurns, attempts);
      assert.equal(plan.isContinuation(run), expected);
      assert.equal(isRestartNoteContinuation(run, runs, providerTurns, attempts), expected);
    }
    const candidates = [...plan.compactionCandidates()];
    assert.deepEqual(
      candidates.map((run) => run.ordinal),
      referenceRestartNoteCompactionCandidates(input).map((run) => run.ordinal),
    );
    if (candidates.length > 0) covered.candidates += 1;
    for (const compactionMessageIds of [
      new Set<string>(),
      compactions,
      new Set(runs.map((run) => run.userMessageId as string)),
    ]) {
      const pending = plan.pending(compactionMessageIds);
      assert.deepEqual(
        pending,
        referencePendingRestartCancelledBackgroundWork({ ...input, compactionMessageIds }),
      );
      if (pending.length > 0) covered.pending += 1;
    }
    for (const providerThreadId of threads) {
      for (const run of [current, runs[Math.floor(random() * runs.length)]!]) {
        const abandoned = abandonedProviderTurns(
          { runs, attempts, providerTurns },
          run,
          providerThreadId,
        );
        const expected = referenceAbandonedProviderTurns(
          { runs, attempts, providerTurns },
          run,
          providerThreadId,
        );
        assert.deepEqual(abandoned, expected);
        assert.deepEqual(
          abandonedProviderTurns({ runs, attempts, providerTurns }, run, providerThreadId, shared),
          expected,
        );
        if (abandoned.length > 0) covered.abandoned += 1;
      }
    }
  }
  // Every path ran on many projections.
  for (const [path, count] of Object.entries(covered)) assert.isAbove(count, 100, path);
});

// @effect-diagnostics nodeBuiltinImport:off - builds a small fixture through the CLI and reads it back.
// Run outcomes: a shape's `runOutcomes` gives its runs a fixed rotation of statuses and restart chains, so a fixture
// can hold a long history of runs that did not all complete, as a long-lived thread has. The long-thread fixture is
// one project and one thread whose runs follow it: every 8th run did not complete, every 16th is the interrupted
// run a restart continues (the next run names it), every 32nd interrupted run carries background work the restart
// cancelled. The last run always completes, so the thread is idle.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { describe, expect, it, onTestFinished } from "vite-plus/test";

import { planSynth, runOutcome } from "./fixture/plan.ts";
import { ShapeError, parseShape } from "./fixture/shape.ts";

const ROOT = NodePath.resolve(import.meta.dirname, "..", "..");
const FIXTURE = NodePath.join(import.meta.dirname, "fixture.ts");
const LONG_THREAD = NodePath.join(import.meta.dirname, "fixture", "long-thread-1.shape.json");
const longThread = parseShape(JSON.parse(NodeFS.readFileSync(LONG_THREAD, "utf8")));
const EXAMPLE = NodePath.join(import.meta.dirname, "fixture", "example-shape.json");
const example = parseShape(JSON.parse(NodeFS.readFileSync(EXAMPLE, "utf8")));
// 64 of the shape's 20,000 runs: the rotation repeats every 32 runs.
const SMALL = 64 / 20_000;

const scratch = (name: string) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), `fixture-${name}-`));
  onTestFinished(() => NodeFS.rmSync(directory, { recursive: true, force: true }));
  return directory;
};

describe("runOutcomes in a shape", () => {
  it("are three whole periods of 2 or more, or the shape is refused", () => {
    const bad = (runOutcomes: unknown) => () => parseShape({ ...longThread, runOutcomes });
    expect(bad({ incompleteEvery: 8, continuationEvery: 16 })).toThrow(ShapeError);
    expect(bad({ incompleteEvery: 1, continuationEvery: 16, restartWorkEvery: 32 })).toThrow(
      ShapeError,
    );
    expect(bad({ incompleteEvery: 8, continuationEvery: 12, restartWorkEvery: 32 })).toThrow(
      ShapeError,
    );
    expect(bad({ incompleteEvery: 8, continuationEvery: 16, restartWorkEvery: 24 })).toThrow(
      ShapeError,
    );
    expect(bad("often")).toThrow(ShapeError);
    expect(parseShape({ ...longThread }).runOutcomes).toEqual({
      incompleteEvery: 8,
      continuationEvery: 16,
      restartWorkEvery: 32,
    });
  });
});

describe("the rotation", () => {
  const periods = { incompleteEvery: 8, continuationEvery: 16, restartWorkEvery: 32 };
  it("gives each run ordinal its status, the run it continues and its restart work", () => {
    const of = (ordinal: number) => runOutcome(periods, ordinal, 64);
    expect(of(1)).toEqual({ status: "completed", continuationOf: null, restartWork: false });
    expect(of(8)).toEqual({ status: "cancelled", continuationOf: null, restartWork: false });
    expect(of(16)).toEqual({ status: "interrupted", continuationOf: null, restartWork: false });
    expect(of(17)).toEqual({ status: "completed", continuationOf: 16, restartWork: false });
    expect(of(24)).toEqual({ status: "failed", continuationOf: null, restartWork: false });
    expect(of(32)).toEqual({ status: "interrupted", continuationOf: null, restartWork: true });
    expect(of(33)).toEqual({ status: "completed", continuationOf: 32, restartWork: false });
    expect(of(40)).toEqual({ status: "cancelled", continuationOf: null, restartWork: false });
    // The last run completes whatever its place in the rotation: the thread is idle.
    expect(of(64)).toEqual({ status: "completed", continuationOf: null, restartWork: false });
  });

  it("over 20,000 runs: one in 8 not completed, one in 16 a continuation, one in 32 with restart work", () => {
    const all = Array.from({ length: 20_000 }, (_, index) =>
      runOutcome(periods, index + 1, 20_000),
    );
    const count = (test: (o: (typeof all)[number]) => boolean) => all.filter(test).length;
    expect({
      notCompleted: count((o) => o.status !== "completed"),
      interrupted: count((o) => o.status === "interrupted"),
      cancelled: count((o) => o.status === "cancelled"),
      failed: count((o) => o.status === "failed"),
      continuations: count((o) => o.continuationOf !== null),
      restartWork: count((o) => o.restartWork),
    }).toEqual({
      notCompleted: 2_499,
      interrupted: 1_249,
      cancelled: 625,
      failed: 625,
      continuations: 1_249,
      restartWork: 624,
    });
  });
});

describe("planSynth with runOutcomes", () => {
  it("plans one project and one thread whose runs carry the rotation, and leaves other shapes as they were", () => {
    const plan = planSynth(longThread, SMALL, 20261017);
    expect(plan.projects).toBe(1);
    expect(plan.threads).toHaveLength(1);
    const runs = plan.threads[0]?.runs ?? [];
    expect(runs).toHaveLength(64);
    expect(new Set(runs.map((run) => run.items.join(",")))).toEqual(
      new Set(["user_message,assistant_message"]),
    );
    expect(runs.map((run) => run.outcome?.status)).toEqual(
      Array.from(
        { length: 64 },
        (_, index) => runOutcome(longThread.runOutcomes!, index + 1, 64).status,
      ),
    );
    // A shape without runOutcomes plans no outcomes at all (its plan, and so its fixture, is unchanged).
    expect(
      planSynth(example, 0.2, 7)
        .threads.flatMap((thread) => thread.runs)
        .some((run) => "outcome" in run),
    ).toBe(false);
  });
});

describe("a fixture built with runOutcomes", () => {
  it("projects each run with its status, the run it continues and the background work its restart cancelled", () => {
    const directory = scratch("outcomes");
    const database = NodePath.join(directory, "long-thread.sqlite");
    const result = NodeChildProcess.spawnSync(
      process.execPath,
      [
        FIXTURE,
        "synth",
        "--like",
        LONG_THREAD,
        "--scale",
        String(SMALL),
        "--seed",
        "20261017",
        database,
      ],
      { cwd: ROOT, env: process.env, encoding: "utf8" },
    );
    expect(result.stderr).toBe("");
    expect(result.status).toBe(0);
    const db = new NodeSqlite.DatabaseSync(database, { readOnly: true });
    try {
      const rows = db
        .prepare(
          `SELECT run.ordinal AS ordinal, run.status AS status,
             (SELECT continued.ordinal FROM orchestration_v2_projection_runs AS continued
              WHERE continued.run_id = json_extract(run.payload_json, '$.restartContinuationOfRunId')) AS continuationOf,
             json_array_length(json_extract(run.payload_json, '$.restartCancelledBackgroundWork')) AS restartWork,
             (SELECT attempt.status FROM orchestration_v2_projection_run_attempts AS attempt
              WHERE attempt.run_id = run.run_id) AS attemptStatus
           FROM orchestration_v2_projection_runs AS run ORDER BY run.ordinal`,
        )
        .all() as Array<{
        ordinal: number;
        status: string;
        continuationOf: number | null;
        restartWork: number | null;
        attemptStatus: string;
      }>;
      expect(rows).toHaveLength(64);
      for (const row of rows) {
        const want = runOutcome(longThread.runOutcomes!, row.ordinal, 64);
        expect({
          ordinal: row.ordinal,
          status: row.status,
          attempt: row.attemptStatus,
          continuationOf: row.continuationOf,
          restartWork: (row.restartWork ?? 0) > 0,
        }).toEqual({
          ordinal: row.ordinal,
          status: want.status,
          attempt: want.status,
          continuationOf: want.continuationOf,
          restartWork: want.restartWork,
        });
      }
      const thread = db
        .prepare("SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads")
        .get() as { n: number };
      expect(thread.n).toBe(1);
    } finally {
      db.close();
    }
  }, 600_000);
});

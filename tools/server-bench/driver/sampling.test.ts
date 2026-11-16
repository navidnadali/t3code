import { describe, expect, it } from "vite-plus/test";

import {
  compareDistributions,
  createRng,
  percentile,
  pickMatching,
  plannedMismatch,
  quantileTest,
  sampleValues,
  summarizeDistribution,
  windowMatch,
} from "./sampling.ts";

const draws = (seed: string, count: number) => {
  const rng = createRng(seed);
  return Array.from({ length: count }, () => rng.next());
};

/** A long-tailed history: most threads short, a few very long. */
const skewedPopulation = (count: number) => {
  const rng = createRng("population");
  return Array.from({ length: count }, (_, index) => ({
    id: `thread-${index}`,
    value: Math.round(1 + 5000 * rng.next() ** 6),
  }));
};

describe("createRng", () => {
  it("repeats its sequence for a seed and differs across seeds", () => {
    expect(draws("7", 5)).toEqual(draws("7", 5));
    expect(draws("7", 5)).not.toEqual(draws("8", 5));
    expect(draws("7", 1000).every((value) => value >= 0 && value < 1)).toBe(true);
  });

  it("forks streams that do not depend on the parent's draws", () => {
    const untouched = createRng("seed").fork("pollers");
    const used = createRng("seed");
    used.next();
    used.next();
    const forked = used.fork("pollers");
    expect([forked.next(), forked.next()]).toEqual([untouched.next(), untouched.next()]);
  });
});

describe("percentile", () => {
  it("uses the nearest rank", () => {
    const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    expect(percentile(sorted, 0.5)).toBe(5);
    expect(percentile(sorted, 0.95)).toBe(10);
    expect(percentile(sorted, 0.01)).toBe(1);
    expect(percentile([], 0.5)).toBeNaN();
  });
});

describe("pickMatching", () => {
  it("draws distinct threads whose turn-item distribution matches the population's", () => {
    const population = skewedPopulation(5000);
    const target = population.map((thread) => thread.value);
    const picked = pickMatching(createRng("sample"), population, target, 200);
    expect(new Set(picked.map((thread) => thread.id)).size).toBe(200);
    const want = summarizeDistribution(target);
    const got = summarizeDistribution(picked.map((thread) => thread.value));
    expect(Math.abs((got.p50 as number) / (want.p50 as number) - 1)).toBeLessThanOrEqual(0.2);
    expect(Math.abs((got.p95 as number) / (want.p95 as number) - 1)).toBeLessThanOrEqual(0.2);
  });

  it("matches the population even when only some threads qualify", () => {
    const population = skewedPopulation(5000);
    const target = population.map((thread) => thread.value);
    // Every third thread qualifies (for example, idle threads without a worktree).
    const candidates = population.filter((_, index) => index % 3 === 0);
    const picked = pickMatching(createRng("sessions"), candidates, target, 80);
    const want = summarizeDistribution(target);
    const got = summarizeDistribution(picked.map((thread) => thread.value));
    expect(Math.abs((got.p50 as number) / (want.p50 as number) - 1)).toBeLessThanOrEqual(0.2);
    expect(Math.abs((got.p95 as number) / (want.p95 as number) - 1)).toBeLessThanOrEqual(0.2);
  });

  it("depends on the seed only, not on the candidates' order", () => {
    const population = skewedPopulation(300);
    const target = population.map((thread) => thread.value);
    const forward = pickMatching(createRng("same"), population, target, 30);
    const backward = pickMatching(createRng("same"), population.toReversed(), target, 30);
    expect(backward.map((thread) => thread.id)).toEqual(forward.map((thread) => thread.id));
  });

  it("takes every candidate when asked for all of them", () => {
    const population = [
      { id: "a", value: 5 },
      { id: "b", value: 5 },
      { id: "c", value: 5 },
    ];
    const picked = pickMatching(createRng("all"), population, [5, 5, 5], 3);
    expect(picked.map((thread) => thread.id).toSorted()).toEqual(["a", "b", "c"]);
  });

  it("refuses to pick more threads than qualify", () => {
    expect(() => pickMatching(createRng("x"), [{ id: "a", value: 1 }], [1], 2)).toThrow(
      "only 1 qualify",
    );
  });
});

describe("compareDistributions", () => {
  it("fails a sample that cannot reach the population's long tail", () => {
    const population = summarizeDistribution([1, 1000, 1000]);
    expect(compareDistributions(population, summarizeDistribution([1]), 0.2)).toEqual({
      p50Ratio: 0.001,
      p95Ratio: 0.001,
      tolerance: 0.2,
      withinTolerance: false,
    });
    expect(
      compareDistributions(population, summarizeDistribution([1, 900, 1100]), 0.2).withinTolerance,
    ).toBe(true);
  });
});

describe("sampleValues (the distribution check's sample)", () => {
  it("concatenates the groups: a thread in two groups counts twice, a thread without a count not at all", () => {
    const counts = new Map([
      ["a", 1],
      ["b", 5],
      ["c", 9],
    ]);
    expect(sampleValues(counts, [["a", "b"], new Set(["a", "c", "new"])])).toEqual([1, 5, 1, 9]);
    expect(sampleValues(counts, [])).toEqual([]);
  });

  // A large fixture's shape: 3000 threads whose turn items rise geometrically from 1 to about 1000 (p50 32), so its
  // low region is dense with equal counts; two of every three threads are idle roots a session can continue.
  const threads = Array.from({ length: 3000 }, (_, index) => ({
    id: `t${String(index).padStart(4, "0")}`,
    value: Math.max(1, Math.round(32 * 2 ** ((index - 1500) / 300))),
    idle: index % 3 !== 0,
  }));
  const target = threads.map((thread) => thread.value);
  const reference = summarizeDistribution(target);
  const counts = new Map(threads.map((thread) => [thread.id, thread.value]));
  const plan = (sessions: number) => ({
    sessions: pickMatching(
      createRng("1").fork("sessions"),
      threads.filter((thread) => thread.idle),
      target,
      sessions,
    ).map((thread) => thread.id),
    readPool: pickMatching(createRng("1").fork("reads"), threads, target, 200).map(
      (thread) => thread.id,
    ),
  });

  it("matches the 250-session plan that a de-duplicated union failed (default tolerance 0.2)", () => {
    const { sessions, readPool } = plan(250);
    const match = (ids: ReadonlyArray<ReadonlyArray<string>>) =>
      compareDistributions(reference, summarizeDistribution(sampleValues(counts, ids)), 0.2);
    // Each planned set follows the fixture on its own...
    expect(match([sessions]).withinTolerance).toBe(true);
    expect(match([readPool]).withinTolerance).toBe(true);
    // ...but both pick the same low-count threads, so their union without the duplicates sits high.
    const union = [...new Set([...sessions, ...readPool])].map((id) => counts.get(id)!);
    expect(union.length).toBeLessThan(450);
    expect(compareDistributions(reference, summarizeDistribution(union), 0.2)).toMatchObject({
      p50Ratio: 1.469,
      withinTolerance: false,
    });
    expect(match([sessions, readPool])).toMatchObject({ p50Ratio: 1, withinTolerance: true });
  });

  it.each([100, 50, 1])("matches the %i-session plan too", (size) => {
    const { sessions, readPool } = plan(size);
    const values = sampleValues(counts, [sessions, readPool]);
    expect(values).toHaveLength(size + 200);
    expect(
      compareDistributions(reference, summarizeDistribution(values), 0.2).withinTolerance,
    ).toBe(true);
  });
});

describe("quantileTest (after the window: an exact, size-aware binomial test)", () => {
  // The large fixture's shape: 3000 threads rising geometrically from 1 to about 1000 items (p50 32).
  const population = Array.from({ length: 3000 }, (_, index) =>
    Math.max(1, Math.round(32 * 2 ** ((index - 1500) / 300))),
  );
  const sorted = population.toSorted((left, right) => left - right);
  /** n threads from the given quantile range of the fixture, evenly spread. */
  const from = (n: number, low: number, high: number) =>
    Array.from({ length: n }, (_, index) => {
      const q = low + ((high - low) * (index + 0.5)) / n;
      return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
    });

  it("passes a mixed sample whose median a few high draws lifted, which the fixed 20 % ratio failed", () => {
    // 50 session threads like the fixture, 27 polled threads that happened to sit high, 6 subscribed small ones.
    const exercised = [...from(50, 0, 1), ...from(27, 0.4, 1), ...from(6, 0, 0.5)];
    const ratio = compareDistributions(
      summarizeDistribution(population),
      summarizeDistribution(exercised),
      0.2,
    );
    expect(ratio).toMatchObject({ p50Ratio: 1.594, withinTolerance: false });
    const test = quantileTest(population, exercised);
    expect(test.below_p50).toBe(35);
    expect(test).toMatchObject({ n: 83, alpha: 0.001, passed: true });
    expect(test.below_p50).toBeGreaterThanOrEqual(test.band_below[0]);
    expect(test.below_p50).toBeLessThanOrEqual(test.band_below[1]);
  });

  it("fails a genuinely skewed set: 80 % above the fixture's p50", () => {
    const exercised = [...from(17, 0, 0.5), ...from(66, 0.5, 1)];
    const test = quantileTest(population, exercised);
    expect(test.n).toBe(83);
    expect(test.passed).toBe(false);
    expect(test.p_value_below).toBeLessThan(0.001);
  });

  it("fails too many threads above the fixture's p95", () => {
    const exercised = [...from(63, 0, 0.95), ...from(20, 0.96, 1)];
    const test = quantileTest(population, exercised);
    expect(test.above_p95).toBe(20);
    expect(test.passed).toBe(false);
  });

  it("does not fail a small set spuriously: 5 threads all above the median", () => {
    const test = quantileTest(population, from(5, 0.55, 0.9));
    expect(test).toMatchObject({ n: 5, below_p50: 0, passed: true });
    expect(test.band_below).toEqual([0, 5]);
  });

  it("uses the fixture's own share below its p50 and above its p95 (ties at the median)", () => {
    const tied = [...Array.from({ length: 600 }, () => 32), ...population.slice(0, 2400)];
    const test = quantileTest(tied, tied.slice(0, 100));
    expect(test.expected_below).toBeLessThan(0.5);
    expect(test.expected_above).toBeLessThanOrEqual(0.05);
  });
});

describe("plannedMismatch (setup: the planned sets together, and each set of 20 or more alone)", () => {
  const population = Array.from({ length: 3000 }, (_, index) =>
    Math.max(1, Math.round(32 * 2 ** ((index - 1500) / 300))),
  );
  const reference = summarizeDistribution(population);
  const sorted = population.toSorted((left, right) => left - right);
  const from = (n: number, low: number, high: number) =>
    Array.from({ length: n }, (_, index) => {
      const q = low + ((high - low) * (index + 0.5)) / n;
      return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
    });

  it("fails fast on sessions that alone are unlike the fixture, though the read pool hides them together", () => {
    const sets = [
      { name: "sessions", values: from(24, 0.35, 1) },
      { name: "read pool", values: from(400, 0, 1) },
    ];
    expect(
      compareDistributions(reference, summarizeDistribution(sets.flatMap((set) => set.values)), 0.2)
        .withinTolerance,
    ).toBe(true);
    expect(plannedMismatch(reference, sets, 0.2)).toMatchObject({
      name: "sessions",
      match: { withinTolerance: false },
    });
  });

  it("leaves a set under 20 threads to the combined check, and reports the combined set when it fails", () => {
    expect(
      plannedMismatch(
        reference,
        [
          { name: "sessions", values: from(10, 0.8, 1) },
          { name: "read pool", values: from(200, 0, 1) },
        ],
        0.2,
      ),
    ).toBeNull();
    expect(
      plannedMismatch(
        reference,
        [
          { name: "sessions", values: from(10, 0.8, 1) },
          { name: "read pool", values: from(200, 0.6, 1) },
        ],
        0.2,
      ),
    ).toMatchObject({ name: "planned threads" });
  });
});

describe("windowMatch (the post-window match driver.json records)", () => {
  it("says the binomial test decided it, beside the ratios kept for diagnosis", () => {
    const fixture = Array.from({ length: 200 }, (_, index) => index + 1);
    const exercised = [...fixture.slice(90, 110), 199, 200];
    const match = windowMatch(fixture, exercised, 0.2);
    expect(match.decidedBy).toBe("binomial");
    expect(match.withinTolerance).toBe(match.test?.passed);
    expect(match.tolerance).toBe(0.2);
    expect(Object.keys(match)).toEqual([
      "p50Ratio",
      "p95Ratio",
      "tolerance",
      "withinTolerance",
      "decidedBy",
      "test",
    ]);
  });
});

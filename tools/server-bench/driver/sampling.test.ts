import { describe, expect, it } from "vite-plus/test";

import { createRng, percentile, pickMatching, summarizeDistribution } from "./sampling.ts";

const draws = (seed: string, count: number) => {
  const rng = createRng(seed);
  return Array.from({ length: count }, () => rng.next());
};

/** A long-tailed history: most threads short, a few very long, like a real database. */
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

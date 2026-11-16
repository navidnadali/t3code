export interface Rng {
  /** Uniform in [0, 1). */
  readonly next: () => number;
  /** An independent stream, so adding a consumer never shifts another one's draws. */
  readonly fork: (label: string) => Rng;
}

function hashString(text: string): number {
  let hash = 2166136261;
  for (let index = 0; index < text.length; index++) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

export function createRng(seed: string): Rng {
  let state = hashString(seed);
  return {
    next: () => {
      state = (state + 0x6d2b79f5) >>> 0;
      let value = state;
      value = Math.imul(value ^ (value >>> 15), value | 1);
      value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
      return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
    },
    fork: (label) => createRng(`${seed}/${label}`),
  };
}

export function pickIndex(rng: Rng, length: number): number {
  return Math.min(length - 1, Math.floor(rng.next() * length));
}

export function weightedChoice<T>(rng: Rng, entries: ReadonlyArray<readonly [T, number]>): T {
  const total = entries.reduce((sum, [, weight]) => sum + weight, 0);
  let point = rng.next() * total;
  for (const [value, weight] of entries) {
    point -= weight;
    if (point < 0) return value;
  }
  const last = entries.at(-1);
  if (last === undefined) throw new Error("weightedChoice needs at least one entry");
  return last[0];
}

export function shuffled<T>(rng: Rng, values: ReadonlyArray<T>): Array<T> {
  const result = [...values];
  for (let index = result.length - 1; index > 0; index--) {
    const other = pickIndex(rng, index + 1);
    [result[index], result[other]] = [result[other] as T, result[index] as T];
  }
  return result;
}

/** Nearest-rank percentile of ascending values; NaN when empty. */
export function percentile(sorted: ArrayLike<number>, fraction: number): number {
  if (sorted.length === 0) return Number.NaN;
  const rank = Math.ceil(fraction * sorted.length);
  return sorted[Math.min(sorted.length - 1, Math.max(0, rank - 1))] as number;
}

export interface DistributionSummary {
  readonly count: number;
  readonly p50: number | null;
  readonly p95: number | null;
  readonly max: number | null;
  readonly mean: number | null;
}

export function summarizeDistribution(values: ReadonlyArray<number>): DistributionSummary {
  if (values.length === 0) return { count: 0, p50: null, p95: null, max: null, mean: null };
  const sorted = values.toSorted((left, right) => left - right);
  return {
    count: sorted.length,
    p50: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    max: sorted.at(-1) ?? null,
    mean: sorted.reduce((sum, value) => sum + value, 0) / sorted.length,
  };
}

export interface Weighted {
  readonly id: string;
  readonly value: number;
}

/**
 * Picks `size` distinct candidates whose values follow `target`'s distribution: stratum i takes
 * the candidate nearest to target's quantile at (i + u) / size. Candidates may be a subset of the
 * target population; ties break by id so the pick depends on the seed only.
 */
export function pickMatching<T extends Weighted>(
  rng: Rng,
  candidates: ReadonlyArray<T>,
  target: ReadonlyArray<number>,
  size: number,
): Array<T> {
  if (size > candidates.length) {
    throw new Error(`Asked for ${size} threads but only ${candidates.length} qualify.`);
  }
  if (size === 0) return [];
  const sortedTarget = target.toSorted((left, right) => left - right);
  const sorted = candidates.toSorted(
    (left, right) => left.value - right.value || (left.id < right.id ? -1 : 1),
  );
  const used = new Uint8Array(sorted.length);
  const picked: Array<T> = [];
  for (let stratum = 0; stratum < size; stratum++) {
    const wanted = percentile(sortedTarget, (stratum + rng.next()) / size);
    let low = 0;
    let high = sorted.length;
    while (low < high) {
      const middle = (low + high) >> 1;
      if ((sorted[middle] as T).value < wanted) low = middle + 1;
      else high = middle;
    }
    let below = low - 1;
    let above = low;
    while (below >= 0 && used[below] === 1) below--;
    while (above < sorted.length && used[above] === 1) above++;
    const index =
      below < 0
        ? above
        : above >= sorted.length
          ? below
          : wanted - (sorted[below] as T).value <= (sorted[above] as T).value - wanted
            ? below
            : above;
    used[index] = 1;
    picked.push(sorted[index] as T);
  }
  return picked;
}

export interface DistributionMatch {
  readonly p50Ratio: number | null;
  readonly p95Ratio: number | null;
  readonly tolerance: number;
  readonly withinTolerance: boolean;
  /** After the window, the binomial test that decides withinTolerance (the ratios are then for diagnosis). */
  readonly test?: QuantileTest;
  /** What decided withinTolerance, when it is not the ratio tolerance: the binomial test after the window. */
  readonly decidedBy?: "binomial";
}

/**
 * The values of every group's threads, concatenated. Each group (the session threads, the read pool, ...) is its own
 * sample matched to the population, and matched samples share the population's dense values; de-duplicating across
 * groups would drop exactly those and push the combined p50 up. A thread in two groups counts twice; ids without a
 * count (a mixed-mode session's new thread) are left out.
 */
export function sampleValues(
  counts: ReadonlyMap<string, number>,
  groups: ReadonlyArray<Iterable<string>>,
): Array<number> {
  const values: Array<number> = [];
  for (const group of groups) {
    for (const id of group) {
      const value = counts.get(id);
      if (value !== undefined) values.push(value);
    }
  }
  return values;
}

/** Whether `sample`'s p50 and p95 sit within `tolerance` (a fraction) of `population`'s. */
export function compareDistributions(
  population: DistributionSummary,
  sample: DistributionSummary,
  tolerance: number,
): DistributionMatch {
  const ratio = (value: number | null, base: number | null) =>
    value === null || base === null || base === 0 ? null : Math.round((value / base) * 1000) / 1000;
  const p50Ratio = ratio(sample.p50, population.p50);
  const p95Ratio = ratio(sample.p95, population.p95);
  const within = (value: number | null) => value !== null && Math.abs(value - 1) <= tolerance;
  return { p50Ratio, p95Ratio, tolerance, withinTolerance: within(p50Ratio) && within(p95Ratio) };
}

export interface QuantileTest {
  readonly n: number;
  readonly below_p50: number;
  readonly expected_below: number;
  readonly band_below: readonly [number, number];
  readonly p_value_below: number;
  readonly above_p95: number;
  readonly expected_above: number;
  readonly band_above: readonly [number, number];
  readonly p_value_above: number;
  readonly alpha: number;
  readonly passed: boolean;
}

/** P(X = k) for X ~ Binomial(n, p), k = 0..n (exact, through log factorials). */
function binomialPmf(n: number, p: number): Float64Array {
  const pmf = new Float64Array(n + 1);
  if (p <= 0) {
    pmf[0] = 1;
    return pmf;
  }
  if (p >= 1) {
    pmf[n] = 1;
    return pmf;
  }
  const logFactorial = new Float64Array(n + 1);
  for (let index = 2; index <= n; index++) {
    logFactorial[index] = logFactorial[index - 1]! + Math.log(index);
  }
  for (let k = 0; k <= n; k++) {
    pmf[k] = Math.exp(
      logFactorial[n]! -
        logFactorial[k]! -
        logFactorial[n - k]! +
        k * Math.log(p) +
        (n - k) * Math.log(1 - p),
    );
  }
  return pmf;
}

/** Two-sided exact binomial test of `count` of n against share p: its p-value and the counts it accepts at `alpha`. */
function binomialTest(
  count: number,
  n: number,
  p: number,
  alpha: number,
): { readonly pValue: number; readonly band: readonly [number, number] } {
  const pmf = binomialPmf(n, p);
  const low = new Float64Array(n + 1);
  const high = new Float64Array(n + 2);
  for (let k = 0; k <= n; k++) low[k] = (k > 0 ? low[k - 1]! : 0) + pmf[k]!;
  for (let k = n; k >= 0; k--) high[k] = high[k + 1]! + pmf[k]!;
  // Doubling the smaller tail.
  const pValue = (k: number) => Math.min(1, 2 * Math.min(low[k]!, high[k]!));
  let first = 0;
  while (first < n && pValue(first) <= alpha) first++;
  let last = n;
  while (last > first && pValue(last) <= alpha) last--;
  return { pValue: pValue(count), band: [first, last] };
}

/**
 * After the window: are the exercised threads a plausible sample of the fixture? The number below the fixture's p50
 * and the number above its p95 are each tested against Binomial(n, share), where share is the fixture's own share
 * below its p50 / above its p95 (0.5 and 0.05 without ties), two-sided at `alpha`. Unlike a fixed tolerance on the
 * median, this allows for small samples, so a few threads drawn high by chance do not fail the window.
 */
export function quantileTest(
  population: ReadonlyArray<number>,
  sample: ReadonlyArray<number>,
  alpha = 0.001,
): QuantileTest {
  const sorted = population.toSorted((left, right) => left - right);
  const n = sample.length;
  if (sorted.length === 0 || n === 0) {
    return {
      n,
      below_p50: 0,
      expected_below: 0.5,
      band_below: [0, n],
      p_value_below: 1,
      above_p95: 0,
      expected_above: 0.05,
      band_above: [0, n],
      p_value_above: 1,
      alpha,
      passed: true,
    };
  }
  const p50 = percentile(sorted, 0.5);
  const p95 = percentile(sorted, 0.95);
  const share = (count: number) => Math.round((count / sorted.length) * 1e6) / 1e6;
  const expectedBelow = share(sorted.filter((value) => value < p50).length);
  const expectedAbove = share(sorted.filter((value) => value > p95).length);
  const below = sample.filter((value) => value < p50).length;
  const above = sample.filter((value) => value > p95).length;
  const belowTest = binomialTest(below, n, expectedBelow, alpha);
  const aboveTest = binomialTest(above, n, expectedAbove, alpha);
  const round = (value: number) => Number(value.toPrecision(4));
  return {
    n,
    below_p50: below,
    expected_below: expectedBelow,
    band_below: belowTest.band,
    p_value_below: round(belowTest.pValue),
    above_p95: above,
    expected_above: expectedAbove,
    band_above: aboveTest.band,
    p_value_above: round(aboveTest.pValue),
    alpha,
    passed: belowTest.pValue > alpha && aboveTest.pValue > alpha,
  };
}

/** After the window: the ratios for diagnosis, decided by the binomial test of the exercised threads. */
export function windowMatch(
  population: ReadonlyArray<number>,
  sample: ReadonlyArray<number>,
  tolerance: number,
): DistributionMatch {
  const test = quantileTest(population, sample);
  return {
    ...compareDistributions(
      summarizeDistribution(population),
      summarizeDistribution(sample),
      tolerance,
    ),
    withinTolerance: test.passed,
    decidedBy: "binomial",
    test,
  };
}

/**
 * At setup: the planned sets (session threads, read pool) together must match the fixture within `tolerance`, and
 * each set of `minAlone` threads or more must match alone too, so skewed sessions fail before the window instead of
 * after it. Smaller sets are left to the combined check (one thread is a random quantile). Null when all match.
 */
export function plannedMismatch(
  reference: DistributionSummary,
  sets: ReadonlyArray<{ readonly name: string; readonly values: ReadonlyArray<number> }>,
  tolerance: number,
  minAlone = 20,
): { readonly name: string; readonly match: DistributionMatch } | null {
  const together = compareDistributions(
    reference,
    summarizeDistribution(sets.flatMap((set) => set.values)),
    tolerance,
  );
  if (!together.withinTolerance) return { name: "planned threads", match: together };
  for (const set of sets) {
    if (set.values.length < minAlone) continue;
    const alone = compareDistributions(reference, summarizeDistribution(set.values), tolerance);
    if (!alone.withinTolerance) return { name: set.name, match: alone };
  }
  return null;
}

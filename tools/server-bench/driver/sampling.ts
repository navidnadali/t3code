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

import type { KeySizer, WaitObserver } from "@t3tools/shared/WaitObserver";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";

import {
  addToTally,
  makeTally,
  resetTally,
  roundMs,
  type Tally,
  tallyPercentiles,
} from "./SpanMinutes.ts";

// Sites and holder names come from code; the caps bound a line should one ever carry data.
export const MAX_WAIT_SITES = 200;
export const OTHER_SITE = "(other)";
const MAX_HOLDER_NAMES = 200;
const MAX_NAME_LENGTH = 200;
const HOLDERS_WRITTEN = 5;
const OTHER_HOLDER = "(other)";
/** Keys ranked per site and per order: by wait, by hold. */
const TOP_KEYS = 5;
const MAX_KEYS_PER_SITE = 10_000;
/** Sites that rank keys, by wait or by a keyed gauge; past it, keys go unranked. */
export const MAX_RANKED_SITES = 32;
/** Registered gauges and counters; each ranked keyed gauge adds its sum besides. */
export const MAX_GAUGES = 200;
// A size read waits behind the server's own reads; a minute line never waits long for it.
const SIZER_TIMEOUT = Duration.seconds(2);

const QUANTILES = [0.5, 0.9, 0.99];
const DEPTH_QUANTILES = [0.5, 0.9];

/** Waits at one site in a minute, in ms; `holders` and `depth` only where the site reports them. */
export interface WaitStat {
  readonly n: number;
  readonly totalMs: number;
  readonly maxMs: number;
  readonly p50Ms: number;
  readonly p90Ms: number;
  readonly p99Ms: number;
  /** The top holders by the wait they caused: `[waits, totalMs]`. */
  readonly holders?: Record<string, readonly [n: number, totalMs: number]>;
  /** The top waiting operations, likewise. */
  readonly waiters?: Record<string, readonly [n: number, totalMs: number]>;
  /** How the waits ended, likewise. */
  readonly outcomes?: Record<string, readonly [n: number, totalMs: number]>;
  /** p50, p90 and max of the queue lengths arrivals found. */
  readonly depth?: readonly [p50: number, p90: number, max: number];
}

/** One key's minute at a site, ranked without its identity; size classes come from the sizer. */
export type KeyRank = Readonly<Record<string, number>>;

/** Keyed waits rank by wait and by hold; keyed gauges by their value. */
export interface KeyRanks {
  readonly byWait?: ReadonlyArray<KeyRank>;
  readonly byHold?: ReadonlyArray<KeyRank>;
  readonly byDepth?: ReadonlyArray<KeyRank>;
}

export interface WaitSections {
  readonly waits: Record<string, WaitStat>;
  readonly gauges: Record<string, number>;
  readonly threads: Record<string, KeyRanks>;
}

type Labels = Map<string, { n: number; totalNs: number }>;

interface Site {
  readonly waits: Tally;
  readonly holders: Labels;
  readonly waiters: Labels;
  readonly outcomes: Labels;
  readonly depth: Tally;
}

interface KeyTally {
  n: number;
  waitNs: number;
  holdNs: number;
}

interface Probe {
  readonly kind: "gauge" | "counter";
  readonly read: () => number;
  last: number;
}

const clip = (name: string) =>
  name.length > MAX_NAME_LENGTH ? name.slice(0, MAX_NAME_LENGTH) : name;

const top = <A>(entries: Iterable<A>, by: (entry: A) => number, count: number): Array<A> =>
  Array.from(entries)
    .filter((entry) => by(entry) > 0)
    .toSorted((left, right) => by(right) - by(left))
    .slice(0, count);

/**
 * Aggregates the waits reported through `observer` and drains them once a minute. Keys stay only
 * for the minute; the line ranks them without their identity.
 */
export const makeWaitAggregate = () => {
  const sites = new Map<string, Site>();
  const keyed = new Map<string, Map<unknown, KeyTally>>();
  const probes = new Map<string, Probe>();
  const keyedProbes = new Map<
    string,
    Set<{ readonly key: unknown; readonly read: () => number }>
  >();
  const sizers = new Map<string, KeySizer>();

  const addSite = (name: string): Site => {
    const clipped = clip(name);
    const existing = sites.get(clipped);
    if (existing !== undefined) return existing;
    const target = sites.size < MAX_WAIT_SITES ? clipped : OTHER_SITE;
    const site = sites.get(target) ?? {
      waits: makeTally(),
      holders: new Map(),
      waiters: new Map(),
      outcomes: new Map(),
      depth: makeTally(),
    };
    sites.set(target, site);
    return site;
  };
  // Admitted for good: the real ones are few and steady.
  const rankedSites = new Set<string>();
  const admitRanked = (site: string): string | undefined => {
    const name = clip(site);
    if (rankedSites.has(name)) return name;
    if (rankedSites.size >= MAX_RANKED_SITES) return undefined;
    rankedSites.add(name);
    return name;
  };
  const keyTally = (rawSite: string, key: unknown): KeyTally | undefined => {
    const site = admitRanked(rawSite);
    if (site === undefined) return undefined;
    let keys = keyed.get(site);
    if (keys === undefined) {
      keys = new Map();
      keyed.set(site, keys);
    }
    const existing = keys.get(key);
    if (existing !== undefined) return existing;
    if (keys.size >= MAX_KEYS_PER_SITE) return undefined;
    const created = { n: 0, waitNs: 0, holdNs: 0 };
    keys.set(key, created);
    return created;
  };

  const addLabel = (labels: Labels, label: string, ns: number) => {
    let counted = labels.get(label);
    if (counted === undefined) {
      const name = labels.size < MAX_HOLDER_NAMES ? clip(label) : OTHER_HOLDER;
      counted = labels.get(name) ?? { n: 0, totalNs: 0 };
      labels.set(name, counted);
    }
    counted.n++;
    counted.totalNs += ns;
  };

  const observer: WaitObserver = {
    wait(site, waitNs, holder, key, waiter, outcome) {
      const tally = sites.get(site) ?? addSite(site);
      const ns = Math.max(0, waitNs);
      addToTally(tally.waits, ns);
      if (holder !== undefined) addLabel(tally.holders, holder, ns);
      if (waiter !== undefined) addLabel(tally.waiters, waiter, ns);
      if (outcome !== undefined) addLabel(tally.outcomes, outcome, ns);
      if (key !== undefined) {
        const entry = keyTally(site, key);
        if (entry !== undefined) {
          entry.n++;
          entry.waitNs += ns;
        }
      }
    },
    hold(site, key, holdNs) {
      const entry = keyTally(site, key);
      if (entry !== undefined) entry.holdNs += Math.max(0, holdNs);
    },
    depth(site, depth) {
      addToTally((sites.get(site) ?? addSite(site)).depth, Math.max(0, Math.round(depth)));
    },
    register(rawName, kind, read) {
      const name = clip(rawName);
      if (!probes.has(name) && probes.size >= MAX_GAUGES) return () => {};
      const probe: Probe = { kind, read, last: kind === "counter" ? read() : 0 };
      probes.set(name, probe);
      return () => {
        if (probes.get(name) === probe) probes.delete(name);
      };
    },
    registerKeyed(rawSite, key, read) {
      const site = admitRanked(rawSite);
      if (site === undefined) return () => {};
      let registered = keyedProbes.get(site);
      if (registered === undefined) {
        registered = new Set();
        keyedProbes.set(site, registered);
      }
      if (registered.size >= MAX_KEYS_PER_SITE) return () => {};
      const probe = { key, read };
      registered.add(probe);
      return () => {
        registered.delete(probe);
        if (registered.size === 0 && keyedProbes.get(site) === registered) keyedProbes.delete(site);
      };
    },
    sizeKeys(rawSite, sizer) {
      const site = clip(rawSite);
      sizers.set(site, sizer);
      return () => {
        if (sizers.get(site) === sizer) sizers.delete(site);
      };
    },
  };

  // A name past the cap counts under `(other)`, as do the labels past the top few.
  const topLabels = (labels: Labels) => {
    if (labels.size === 0) return undefined;
    const folded = labels.get(OTHER_HOLDER) ?? { n: 0, totalNs: 0 };
    const named = Array.from(labels)
      .filter(([label]) => label !== OTHER_HOLDER)
      .toSorted(([, left], [, right]) => right.totalNs - left.totalNs);
    const other = named
      .slice(HOLDERS_WRITTEN)
      .reduce(
        (sum, [, counted]) => ({ n: sum.n + counted.n, totalNs: sum.totalNs + counted.totalNs }),
        folded,
      );
    return Object.fromEntries(
      [
        ...named.slice(0, HOLDERS_WRITTEN),
        ...(other.n > 0 ? [[OTHER_HOLDER, other] as const] : []),
      ].map(
        ([label, counted]) =>
          [label, [counted.n, roundMs(counted.totalNs / 1e6)] as const] as const,
      ),
    );
  };

  const drainWaits = () => {
    const waits: Record<string, WaitStat> = {};
    for (const [name, site] of sites) {
      if (site.waits.count === 0 && site.depth.count === 0) {
        sites.delete(name);
        continue;
      }
      const [p50 = 0, p90 = 0, p99 = 0] = tallyPercentiles(site.waits, QUANTILES).map((ns) =>
        roundMs(ns / 1e6),
      );
      const holders = topLabels(site.holders);
      const waiters = topLabels(site.waiters);
      const outcomes = topLabels(site.outcomes);
      const [depthP50 = 0, depthP90 = 0] = tallyPercentiles(site.depth, DEPTH_QUANTILES);
      waits[name] = {
        n: site.waits.count,
        totalMs: roundMs(site.waits.totalNs / 1e6),
        maxMs: roundMs(site.waits.maxNs / 1e6),
        p50Ms: p50,
        p90Ms: p90,
        p99Ms: p99,
        ...(holders === undefined ? {} : { holders }),
        ...(waiters === undefined ? {} : { waiters }),
        ...(outcomes === undefined ? {} : { outcomes }),
        ...(site.depth.count === 0
          ? {}
          : { depth: [Math.round(depthP50), Math.round(depthP90), site.depth.maxNs] as const }),
      };
      resetTally(site.waits);
      resetTally(site.depth);
      site.holders.clear();
      site.waiters.clear();
      site.outcomes.clear();
    }
    return waits;
  };

  const drainGauges = () => {
    const gauges: Record<string, number> = {};
    for (const [name, probe] of probes) {
      const value = probe.read();
      gauges[name] = roundMs(probe.kind === "counter" ? value - probe.last : value);
      probe.last = value;
    }
    return gauges;
  };

  const sizesOf = Effect.fnUntraced(function* (site: string, keys: ReadonlyArray<unknown>) {
    const unsized: ReadonlyMap<unknown, Readonly<Record<string, number>>> = new Map();
    const sizer = sizers.get(site);
    if (sizer === undefined || keys.length === 0) return unsized;
    return yield* sizer(Array.from(new Set(keys))).pipe(
      Effect.timeoutOrElse({ duration: SIZER_TIMEOUT, orElse: () => Effect.succeed(unsized) }),
      Effect.catchCause(() => Effect.succeed(unsized)),
    );
  });

  // Untraced: it runs in the minute flush, which writes no spans.
  const rankKeys = Effect.fnUntraced(function* (site: string, keys: Map<unknown, KeyTally>) {
    const byWait = top(keys, ([, entry]) => entry.waitNs, TOP_KEYS);
    const byHold = top(keys, ([, entry]) => entry.holdNs, TOP_KEYS);
    const sizes = yield* sizesOf(
      site,
      [...byWait, ...byHold].map(([key]) => key),
    );
    const rank = ([key, entry]: readonly [unknown, KeyTally]): KeyRank => ({
      waitMs: roundMs(entry.waitNs / 1e6),
      holdMs: roundMs(entry.holdNs / 1e6),
      n: entry.n,
      ...sizes.get(key),
    });
    return [site, { byWait: byWait.map(rank), byHold: byHold.map(rank) }] as const;
  });

  const rankGauges = Effect.fnUntraced(function* (
    site: string,
    registered: ReadonlySet<{ readonly key: unknown; readonly read: () => number }>,
    gauges: Record<string, number>,
  ) {
    const values = new Map<unknown, number>();
    for (const { key, read } of registered) values.set(key, (values.get(key) ?? 0) + read());
    gauges[site] = roundMs(Array.from(values.values()).reduce((sum, value) => sum + value, 0));
    const byDepth = top(values, ([, value]) => value, TOP_KEYS);
    const sizes = yield* sizesOf(
      site,
      byDepth.map(([key]) => key),
    );
    return [
      site,
      { byDepth: byDepth.map(([key, depth]) => ({ depth: roundMs(depth), ...sizes.get(key) })) },
    ] as const;
  });

  const drain: Effect.Effect<WaitSections> = Effect.suspend(() => {
    const waits = drainWaits();
    const gauges = drainGauges();
    const minuteKeys = Array.from(keyed);
    keyed.clear();
    return Effect.all(
      [
        Effect.forEach(minuteKeys, ([site, keys]) => rankKeys(site, keys), {
          concurrency: "unbounded",
        }),
        Effect.forEach(keyedProbes, ([site, registered]) => rankGauges(site, registered, gauges), {
          concurrency: "unbounded",
        }),
      ],
      { concurrency: "unbounded" },
    ).pipe(
      Effect.map(([keyRanks, gaugeRanks]) => {
        const threads: Record<string, KeyRanks> = {};
        for (const [site, ranks] of [...keyRanks, ...gaugeRanks]) {
          threads[site] = { ...threads[site], ...ranks };
        }
        return { waits, gauges, threads };
      }),
    );
  });

  return { observer, drain };
};

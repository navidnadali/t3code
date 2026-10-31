import { type Quantiles, type Shape, quantile, shapeRows } from "./shape.ts";

export const ITEM_KINDS = [
  "user_message",
  "assistant_message",
  "reasoning",
  "command_execution",
  "file_change",
  "dynamic_tool",
  "todo_list",
  "web_search",
  "file_search",
  "checkpoint",
  "subagent",
  "approval_request",
  "notification",
  "system_notice",
] as const;
export type ItemKind = (typeof ITEM_KINDS)[number];

const isItemKind = (kind: string): kind is ItemKind =>
  (ITEM_KINDS as ReadonlyArray<string>).includes(kind);

// Payload bytes per kind when the shape has no turn item mix.
const DEFAULT_ITEM_BYTES: Record<ItemKind, number> = {
  user_message: 800,
  assistant_message: 1500,
  reasoning: 1200,
  command_execution: 2500,
  file_change: 3000,
  dynamic_tool: 2000,
  todo_list: 900,
  web_search: 1500,
  file_search: 1200,
  checkpoint: 700,
  subagent: 2500,
  approval_request: 1500,
  notification: 900,
  system_notice: 500,
};
const DEFAULT_ITEM_MIX: Partial<Record<ItemKind, number>> = {
  assistant_message: 25,
  reasoning: 15,
  command_execution: 30,
  file_change: 10,
  dynamic_tool: 10,
  todo_list: 4,
  checkpoint: 6,
};
// Heavy-tailed fallbacks, in multiples of the mean.
const DEFAULT_PER_THREAD: Quantiles = [
  [0, 0],
  [0.1, 0.05],
  [0.5, 0.35],
  [0.9, 2],
  [0.99, 8],
  [1, 30],
];
const DEFAULT_PER_PROJECT: Quantiles = [
  [0, 0.05],
  [0.5, 0.5],
  [0.9, 2.5],
  [1, 12],
];
// Updates per entity for event types the store never compacts (provider turns see dozens).
const MAX_UPDATES = 1_000;
// Kinds that need a run's node, scope or turn; children are never spawned from them.
const RUN_ONLY_KINDS: ReadonlySet<ItemKind> = new Set([
  "checkpoint",
  "todo_list",
  "subagent",
  "approval_request",
]);
const UNPARENTED_KINDS: ReadonlySet<ItemKind> = new Set([...RUN_ONLY_KINDS, "user_message"]);

/** Fixed-seed generator (mulberry32): the same seed builds the same database. */
export const makeRandom = (seed: number) => {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
};
export type Random = ReturnType<typeof makeRandom>;

export const shuffle = <A>(random: Random, values: Array<A>): Array<A> => {
  for (let index = values.length - 1; index > 0; index--) {
    const other = Math.floor(random() * (index + 1));
    [values[index], values[other]] = [values[other] as A, values[index] as A];
  }
  return values;
};

/** Splits `total` in proportion to `weights` (largest remainder), so the parts sum to it exactly. */
export const apportion = (total: number, weights: ReadonlyArray<number>): Array<number> => {
  if (weights.length === 0 || total <= 0) return weights.map(() => 0);
  const sum = weights.reduce((acc, weight) => acc + weight, 0);
  if (sum <= 0)
    return apportion(
      total,
      weights.map(() => 1),
    );
  const exact = weights.map((weight) => (total * weight) / sum);
  const parts = exact.map(Math.floor);
  let left = total - parts.reduce((acc, part) => acc + part, 0);
  const order = exact.map((value, index) => ({ index, rest: value - Math.floor(value) }));
  order.sort((a, b) => b.rest - a.rest || a.index - b.index);
  for (const { index } of order) {
    if (left <= 0) break;
    parts[index] = (parts[index] as number) + 1;
    left--;
  }
  return parts;
};

/** Deals `total` extra events over `slots` entities in order: entity j gets the j-th share. */
export class Spreader {
  private next = 0;
  readonly total: number;
  readonly slots: number;

  constructor(total: number, slots: number) {
    this.total = total;
    this.slots = slots;
  }

  take(): number {
    if (this.slots === 0) return 0;
    const index = this.next++;
    return (
      Math.floor(((index + 1) * this.total) / this.slots) -
      Math.floor((index * this.total) / this.slots)
    );
  }
}

export interface RunPlan {
  readonly items: ReadonlyArray<ItemKind>;
  readonly extraAttempts: number;
  readonly childNodes: number;
  /** Checkpoint rows in the thread's root scope; one backs each `checkpoint` item. */
  readonly checkpoints: number;
}

export interface ThreadPlan {
  readonly index: number;
  readonly project: number;
  /** Set on a subagent child: the parent thread and the position of its `subagent` item. */
  readonly parent: { readonly thread: number; readonly run: number; readonly item: number } | null;
  /** Set on a fork: the root it forked from and the run (index) it forked at. */
  readonly fork: { readonly thread: number; readonly run: number } | null;
  /** Turns; on a run-less thread they are history groups with no run, node or provider turn. */
  readonly runs: ReadonlyArray<RunPlan>;
  readonly runless: boolean;
  /** Imported from v1: run-less, with the importer's ids and v1 rows kept beside it. */
  readonly imported: boolean;
  readonly scopes: number;
  readonly providerSessions: number;
  readonly extraProviderThreads: number;
  /** v1 `projection_thread_messages` rows kept for an imported thread. */
  readonly legacyMessages: number;
  /** A child's consumed `subagent_result` transfer; one with runs always has it, or a boot delivers it. */
  readonly resultTransfer: boolean;
}

/** Events beyond one per entity, as totals dealt with a Spreader in write order. */
export interface EventPlan {
  readonly threadFinal: boolean;
  readonly runUpdates: number;
  readonly attemptUpdates: number;
  readonly providerTurnUpdates: number;
  readonly providerThreadUpdates: number;
  readonly nodeLatest: number;
  readonly messageLatest: number;
  readonly turnItemLatest: number;
  readonly subagentUpdates: number;
  readonly providerSessionUpdates: number;
  readonly runtimeRequestUpdates: number;
  /** Share of a turn item's or message's text its first (in-progress) event carries. */
  readonly turnItemFirstShare: number;
  readonly messageFirstShare: number;
}

export interface EntityTotals {
  readonly runs: number;
  readonly attempts: number;
  readonly nodes: number;
  readonly providerThreads: number;
  readonly messages: number;
  readonly turnItems: number;
  readonly subagents: number;
  readonly providerSessions: number;
  readonly runtimeRequests: number;
}

export interface SynthPlan {
  readonly scale: number;
  readonly seed: number;
  readonly projects: number;
  readonly threads: ReadonlyArray<ThreadPlan>;
  readonly events: EventPlan;
  readonly totals: EntityTotals;
  /** Mean payload bytes per item kind, and the turn item size spread in multiples of its mean. */
  readonly itemBytes: Readonly<Record<ItemKind, number>>;
  readonly itemSpread: Quantiles | null;
  /** Mean checkpoint payload bytes, which sets its file list. */
  readonly checkpointBytes: number;
  /** Text length spread of v1 message rows, or null when the shape has none. */
  readonly legacyText: Quantiles | null;
  /** Shape item types this generator writes as `system_notice`, with their scaled counts. */
  readonly substituted: Readonly<Record<string, number>>;
  readonly expected: Readonly<Record<string, number>>;
}

const scaled = (value: number, scale: number) => Math.round(value * scale);
const sum = (values: ArrayLike<number>) =>
  Array.from(values).reduce((acc, value) => acc + value, 0);

/** Draws `count` values at evenly spaced quantiles, then shuffles them: low variance, same distribution. */
const stratified = (random: Random, quantiles: Quantiles, count: number, factor = 1) =>
  shuffle(
    random,
    Array.from(
      { length: count },
      (_, index) => quantile(quantiles, (index + 0.5) / count) * factor,
    ),
  );

/** Rounds `values` to integers summing to `total`, each at least `floor(index)`. */
const fitTotal = (
  values: ReadonlyArray<number>,
  total: number,
  floor: (index: number) => number,
): Array<number> => {
  const floors = values.map((_, index) => floor(index));
  const extra = apportion(
    Math.max(0, total - sum(floors)),
    values.map((value, index) => Math.max(0, value - (floors[index] as number))),
  );
  return floors.map((value, index) => value + (extra[index] as number));
};

/** Runs per thread: one at least for a thread with items, never more than its items. */
const fitRuns = (items: ReadonlyArray<number>, total: number) => {
  const runs = items.map((count) => (count === 0 ? 0 : 1));
  const capacity = items.map((count, index) => count - (runs[index] as number));
  const share = apportion(Math.min(Math.max(0, total - sum(runs)), sum(capacity)), capacity);
  return runs.map(
    (value, index) => value + Math.min(share[index] as number, capacity[index] as number),
  );
};

/** Solves events = finals + firsts for the bytes a first event carries, as a share of a final one. */
const firstShare = (
  events: { readonly rows: number; readonly meanBytes: number } | undefined,
  entityMean: number | undefined,
  entities: number,
) => {
  if (events === undefined || entityMean === undefined || entityMean <= 0) return 0.5;
  const firsts = events.rows - entities;
  if (firsts <= 0) return 0.5;
  const share = (events.rows * events.meanBytes - entities * entityMean) / (firsts * entityMean);
  return Math.min(1, Math.max(0, share));
};

const pickWeighted = (random: Random, cumulative: Float64Array, total: number) => {
  const needle = random() * total;
  let low = 0;
  let high = cumulative.length - 1;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((cumulative[middle] as number) > needle) high = middle;
    else low = middle + 1;
  }
  return low;
};

export const planSynth = (shape: Shape, scale: number, seed: number): SynthPlan => {
  if (!(scale > 0) || !Number.isFinite(scale))
    throw new RangeError("scale must be a positive number");
  const random = makeRandom(seed);
  const target = (table: string) => scaled(shapeRows(shape, table), scale);

  const projects = Math.max(1, target("projection_projects"));
  const threadCount = Math.max(1, target("orchestration_v2_projection_threads"));
  const totalItems = target("orchestration_v2_projection_turn_items");

  // Thread lengths keep the source distribution; the scale changes how many threads there are.
  const perThread = shape.distributions?.turnItemsPerThread;
  const drawn = perThread
    ? stratified(random, perThread, threadCount)
    : stratified(random, DEFAULT_PER_THREAD, threadCount, totalItems / threadCount);
  const itemsPerThread = fitTotal(drawn, totalItems, (index) =>
    (drawn[index] as number) >= 0.5 ? 1 : 0,
  );

  // Roles: v1 imports and subagent children, then run-less children (their items ride the parent's stream).
  const lineage = shape.lineage;
  const lineageTotal = lineage ? lineage.root + lineage.fork + lineage.subagent : 0;
  const ofThreads = (count: number | undefined) =>
    lineageTotal > 0 ? Math.round((threadCount * (count ?? 0)) / lineageTotal) : 0;
  const order = shuffle(
    random,
    itemsPerThread.flatMap((items, index) => (items > 0 ? [index] : [])),
  );
  const imported = new Uint8Array(threadCount);
  const isChild = new Uint8Array(threadCount);
  const runless = new Uint8Array(threadCount);
  const importedCount = Math.min(order.length, ofThreads(lineage?.v1Import));
  for (const index of order.slice(0, importedCount)) imported[index] = runless[index] = 1;
  const childCount = Math.min(order.length - importedCount - 1, ofThreads(lineage?.subagent));
  const children = order.slice(importedCount, importedCount + Math.max(0, childCount));
  for (const index of children) isChild[index] = 1;
  const runlessChildren = Math.max(
    0,
    ofThreads(lineage?.runless ?? lineage?.v1Import) - importedCount,
  );
  for (const index of children.slice(0, runlessChildren)) runless[index] = 1;
  const forkStart = importedCount + Math.max(0, childCount);
  const forks = order.slice(
    forkStart,
    forkStart + Math.max(0, Math.min(order.length - forkStart - 1, ofThreads(lineage?.fork))),
  );
  const isFork = new Uint8Array(threadCount);
  for (const index of forks) isFork[index] = 1;

  const nativeItems = itemsPerThread.map((items, index) => (runless[index] === 1 ? 0 : items));
  const runsPerThread = fitRuns(nativeItems, target("orchestration_v2_projection_runs"));
  const turnRatio =
    sum(nativeItems) === 0 ? 0.1 : Math.min(1, sum(runsPerThread) / sum(nativeItems));
  const groupsPerThread = itemsPerThread.map((items, index) =>
    runless[index] === 1
      ? Math.min(items, Math.max(1, Math.round(items * turnRatio)))
      : (runsPerThread[index] as number),
  );

  const projectWeights = shape.distributions?.threadsPerProject
    ? stratified(random, shape.distributions.threadsPerProject, projects)
    : stratified(random, DEFAULT_PER_PROJECT, projects, threadCount / projects);
  const threadsPerProject = apportion(
    threadCount,
    projectWeights.map((weight) => Math.max(weight, 0.01)),
  );
  const projectOf: Array<number> = shuffle(
    random,
    threadsPerProject.flatMap((count, project) => Array.from({ length: count }, () => project)),
  );

  // Item kinds: every turn opens with a user message; the rest are dealt from the shape's mix.
  const mix = shape.mix?.turnItemTypes;
  const substituted: Record<string, number> = {};
  const kindWeights = new Map<ItemKind, number>();
  const kindBytes = new Map<ItemKind, { rows: number; bytes: number }>();
  if (mix !== undefined && Object.keys(mix).length > 0) {
    for (const [type, { rows, meanBytes }] of Object.entries(mix)) {
      const kind = isItemKind(type) ? type : "system_notice";
      if (kind !== type) substituted[type] = scaled(rows, scale);
      const entry = kindBytes.get(kind) ?? { rows: 0, bytes: 0 };
      kindBytes.set(kind, { rows: entry.rows + rows, bytes: entry.bytes + rows * meanBytes });
      kindWeights.set(kind, (kindWeights.get(kind) ?? 0) + rows * scale);
    }
  } else {
    for (const [kind, weight] of Object.entries(DEFAULT_ITEM_MIX)) {
      kindWeights.set(kind as ItemKind, weight);
    }
  }
  const itemBytes = Object.fromEntries(
    ITEM_KINDS.map((kind) => {
      const entry = kindBytes.get(kind);
      return [kind, entry && entry.rows > 0 ? entry.bytes / entry.rows : DEFAULT_ITEM_BYTES[kind]];
    }),
  ) as Record<ItemKind, number>;
  const itemPayload = shape.tables.orchestration_v2_projection_turn_items?.payload;
  const itemSpread =
    itemPayload && itemPayload.mean > 0
      ? itemPayload.quantiles.map(([q, value]) => [q, value / itemPayload.mean] as const)
      : null;

  const groups = sum(groupsPerThread);
  kindWeights.set("user_message", Math.max(0, (kindWeights.get("user_message") ?? 0) - groups));
  const otherKinds = [...kindWeights.keys()];
  const otherCounts = apportion(
    Math.max(0, totalItems - groups),
    otherKinds.map((kind) => kindWeights.get(kind) ?? 0),
  );
  // Kinds that need a node, scope or turn go to native runs; run-less history gets the rest.
  const nativeSlots = sum(nativeItems) - sum(runsPerThread);
  const runOnly: Array<ItemKind> = [];
  const free: Array<ItemKind> = [];
  otherKinds.forEach((kind, index) => {
    const list = RUN_ONLY_KINDS.has(kind) ? runOnly : free;
    for (let n = 0; n < (otherCounts[index] as number); n++) list.push(kind);
  });
  shuffle(random, runOnly);
  for (const kind of runOnly.splice(nativeSlots)) {
    substituted[kind] = (substituted[kind] ?? 0) + 1;
    free.push("system_notice");
  }
  shuffle(random, free);
  const decks = {
    native: shuffle(random, [
      ...runOnly,
      ...free.splice(0, Math.max(0, nativeSlots - runOnly.length)),
    ]),
    runless: free,
  };
  const dealt = { native: 0, runless: 0 };
  const runItems: Array<Array<Array<ItemKind>>> = groupsPerThread.map((groupCount, thread) => {
    const deck = runless[thread] === 1 ? "runless" : "native";
    return apportion(
      (itemsPerThread[thread] as number) - groupCount,
      Array.from({ length: groupCount }, () => 1),
    ).map((size) => {
      const kinds = decks[deck].slice(dealt[deck], (dealt[deck] += size));
      const assistant = kinds.indexOf("assistant_message");
      if (assistant >= 0) kinds.push(...kinds.splice(assistant, 1));
      return ["user_message", ...kinds];
    });
  });

  // Each child is spawned by a `subagent` item of a native root, bigger threads more often; the
  // mix's own subagent items spawn first, other items convert only past them.
  const parents: Array<ThreadPlan["parent"]> = Array.from({ length: threadCount }, () => null);
  const spawners: Array<{ thread: number; run: number; item: number }> = [];
  runItems.forEach((runs, thread) => {
    if (isChild[thread] === 1 || runless[thread] === 1) return;
    runs.forEach((items, run) =>
      items.forEach((kind, item) => {
        if (kind === "subagent") spawners.push({ thread, run, item });
      }),
    );
  });
  shuffle(random, spawners);
  const cumulative = new Float64Array(threadCount);
  let weight = 0;
  for (let thread = 0; thread < threadCount; thread++) {
    if (isChild[thread] === 0 && runless[thread] === 0) {
      weight += (nativeItems[thread] as number) - (runsPerThread[thread] as number);
    }
    cumulative[thread] = weight;
  }
  const convert = (): { thread: number; run: number; item: number } | undefined => {
    if (weight <= 0) return undefined;
    for (let attempt = 0; attempt < 256; attempt++) {
      const thread = pickWeighted(random, cumulative, weight);
      const runs = runItems[thread] as Array<Array<ItemKind>>;
      const run = Math.floor(random() * runs.length);
      const items = runs[run] as Array<ItemKind>;
      const item = Math.floor(random() * items.length);
      if (UNPARENTED_KINDS.has(items[item] as ItemKind)) continue;
      items[item] = "subagent";
      return { thread, run, item };
    }
    return undefined;
  };
  for (const child of children) {
    const slot = spawners.pop() ?? convert();
    if (slot === undefined) isChild[child] = 0;
    else parents[child] = slot;
  }
  // A fork branches from a run of a native root, so its inherited history is a real run.
  const roots = runItems.flatMap((runs, thread) =>
    isChild[thread] === 0 && isFork[thread] === 0 && runless[thread] === 0 && runs.length > 0
      ? [thread]
      : [],
  );
  const forkOf: Array<ThreadPlan["fork"]> = Array.from({ length: threadCount }, () => null);
  for (const fork of forks) {
    if (roots.length === 0) continue;
    const thread = roots[Math.floor(random() * roots.length)] as number;
    forkOf[fork] = {
      thread,
      run: Math.floor(random() * (runItems[thread] as Array<unknown>).length),
    };
  }
  // Forks and children work in their parent's project.
  const projectFor = (index: number): number => {
    const fork = forkOf[index];
    const parent = parents[index];
    return fork
      ? projectFor(fork.thread)
      : parent
        ? projectFor(parent.thread)
        : (projectOf[index] as number);
  };

  const allRuns = runItems.flat();
  const isNative = runItems.flatMap((runs, thread) => runs.map(() => runless[thread] === 0));
  const count = (kind: ItemKind) =>
    allRuns.reduce((acc, items) => acc + items.filter((item) => item === kind).length, 0);
  const nativeRuns = sum(runsPerThread);
  const ones = isNative.map((native) => (native ? 1 : 0));
  const extraAttempts = apportion(
    Math.max(0, target("orchestration_v2_projection_run_attempts") - nativeRuns),
    ones,
  );
  const childNodes = apportion(
    Math.max(0, target("orchestration_v2_projection_nodes") - nativeRuns),
    allRuns.map((items, index) => (isNative[index] ? items.length - 1 : 0)),
  );

  // Checkpoints share one root scope per thread; standalone ones join threads that already have one.
  const checkpointItems = allRuns.map(
    (items) => items.filter((item) => item === "checkpoint").length,
  );
  let runCursor = 0;
  const threadOfRun = runItems.flatMap((runs, thread) => runs.map(() => thread));
  const hasCheckpoints = new Uint8Array(threadCount);
  checkpointItems.forEach((value, index) => {
    if (value > 0) hasCheckpoints[threadOfRun[index] as number] = 1;
  });
  const standalone = apportion(
    Math.max(0, target("orchestration_v2_projection_checkpoints") - sum(checkpointItems)),
    allRuns.map((_, index) =>
      isNative[index] &&
      (hasCheckpoints[threadOfRun[index] as number] === 1 || sum(hasCheckpoints) === 0)
        ? 1
        : 0,
    ),
  );
  const checkpoints = checkpointItems.map((value, index) => value + (standalone[index] as number));
  const rootScopes = new Uint8Array(threadCount);
  checkpoints.forEach((value, index) => {
    if (value > 0) rootScopes[threadOfRun[index] as number] = 1;
  });
  const extraScopes = apportion(
    Math.max(0, target("orchestration_v2_projection_checkpoint_scopes") - sum(rootScopes)),
    Array.from(rootScopes),
  );

  const withRuns = runsPerThread.map((runs) => (runs > 0 ? 1 : 0));
  const providerSessions = apportion(
    target("orchestration_v2_projection_provider_sessions"),
    withRuns,
  );
  const extraProviderThreads = apportion(
    Math.max(0, target("orchestration_v2_projection_provider_threads") - sum(withRuns)),
    runsPerThread,
  );
  const legacyMessages = apportion(
    target("projection_thread_messages"),
    itemsPerThread.map((items, index) => (imported[index] === 1 ? items : 0)),
  );

  const spawned = children.filter((child) => parents[child] !== null);
  const forked = forkOf.filter((fork) => fork !== null).length;
  const transferTarget = Math.max(
    spawned.filter((child) => runless[child] === 0).length,
    Math.min(spawned.length, target("orchestration_v2_projection_context_transfers") - forked),
  );
  const withTransfer = new Uint8Array(threadCount);
  for (const child of [
    ...spawned.filter((c) => runless[c] === 0),
    ...spawned.filter((c) => runless[c] === 1),
  ].slice(0, transferTarget)) {
    withTransfer[child] = 1;
  }

  const threads: Array<ThreadPlan> = runItems.map((runs, index) => {
    const parent = parents[index] ?? null;
    return {
      index,
      project: projectFor(index),
      parent,
      fork: forkOf[index] ?? null,
      runless: runless[index] === 1,
      imported: imported[index] === 1,
      scopes: (rootScopes[index] as number) + (extraScopes[index] as number),
      providerSessions: providerSessions[index] as number,
      extraProviderThreads: extraProviderThreads[index] as number,
      legacyMessages: legacyMessages[index] as number,
      resultTransfer: withTransfer[index] === 1,
      runs: runs.map((items) => {
        const at = runCursor++;
        return {
          items,
          extraAttempts: extraAttempts[at] as number,
          childNodes: childNodes[at] as number,
          checkpoints: checkpoints[at] as number,
        };
      }),
    };
  });

  const totals: EntityTotals = {
    runs: nativeRuns,
    attempts: nativeRuns + sum(extraAttempts),
    nodes: nativeRuns + sum(childNodes),
    providerThreads: sum(withRuns) + sum(extraProviderThreads),
    messages: count("user_message") + count("assistant_message"),
    turnItems: totalItems,
    subagents: count("subagent"),
    providerSessions: sum(providerSessions),
    runtimeRequests: count("approval_request"),
  };
  const checkpointRows = sum(checkpoints);
  const scopes = sum(rootScopes) + sum(extraScopes);
  const plans = count("todo_list");
  const transfers = sum(withTransfer) + forked;

  // Extra events where the shape records more than one per entity; first/latest pairs stay at two.
  const eventMix = shape.mix?.eventTypes ?? {};
  const eventTarget = (type: string) => scaled(eventMix[type]?.rows ?? 0, scale);
  const extra = (type: string, base: number, cap: number) =>
    Math.max(0, Math.min(eventTarget(type) - base, base * cap));
  const finalTypes = Object.keys(eventMix).filter(
    (type) => type.startsWith("thread.") && type !== "thread.created",
  );
  const events: EventPlan = {
    threadFinal:
      Object.keys(eventMix).length === 0 || finalTypes.some((type) => eventTarget(type) > 0),
    runUpdates: Math.min(eventTarget("run.updated"), totals.runs * MAX_UPDATES),
    attemptUpdates: Math.min(eventTarget("run-attempt.updated"), totals.attempts * MAX_UPDATES),
    providerTurnUpdates: extra("provider-turn.updated", totals.attempts, MAX_UPDATES),
    providerThreadUpdates: extra("provider-thread.updated", totals.providerThreads, MAX_UPDATES),
    nodeLatest: extra("node.updated", totals.nodes, 1),
    messageLatest: extra("message.updated", totals.messages, 1),
    turnItemLatest: extra("turn-item.updated", totalItems, 1),
    subagentUpdates: extra("subagent.updated", totals.subagents, MAX_UPDATES),
    providerSessionUpdates: Math.min(
      eventTarget("provider-session.updated"),
      totals.providerSessions * MAX_UPDATES,
    ),
    runtimeRequestUpdates: extra("runtime-request.updated", totals.runtimeRequests, MAX_UPDATES),
    turnItemFirstShare: firstShare(
      eventMix["turn-item.updated"],
      itemPayload?.mean,
      shapeRows(shape, "orchestration_v2_projection_turn_items"),
    ),
    messageFirstShare: firstShare(
      eventMix["message.updated"],
      shape.tables.orchestration_v2_projection_messages?.payload?.mean,
      shapeRows(shape, "orchestration_v2_projection_messages"),
    ),
  };
  const threadEvents =
    threadCount * (events.threadFinal ? 2 : 1) +
    totals.runs +
    events.runUpdates +
    totals.attempts * 2 +
    events.attemptUpdates +
    events.providerTurnUpdates +
    events.subagentUpdates +
    totals.providerSessions +
    events.providerSessionUpdates +
    totals.runtimeRequests +
    events.runtimeRequestUpdates +
    totals.nodes +
    events.nodeLatest +
    totals.providerThreads +
    events.providerThreadUpdates +
    totals.messages +
    events.messageLatest +
    totalItems +
    events.turnItemLatest +
    plans +
    scopes +
    checkpointRows +
    totals.subagents +
    transfers;
  const searchable = threads
    .filter((thread) => thread.parent === null)
    .reduce(
      (acc, thread) =>
        acc +
        sum(
          thread.runs.map(
            (run) =>
              run.items.filter((item) => item === "user_message" || item === "assistant_message")
                .length,
          ),
        ),
      0,
    );
  const legacyThreads = sum(imported);
  const legacyText = shape.tables.projection_thread_messages?.payload?.quantiles ?? null;
  const checkpointPayload = shape.tables.orchestration_v2_projection_checkpoints?.payload?.mean;

  return {
    scale,
    seed,
    projects,
    threads,
    events,
    totals,
    itemBytes,
    itemSpread,
    checkpointBytes: checkpointPayload ?? itemBytes.checkpoint,
    legacyText: legacyThreads > 0 ? legacyText : null,
    substituted,
    expected: {
      projection_projects: projects,
      orchestration_v2_projection_threads: threadCount,
      orchestration_v2_projection_runs: totals.runs,
      orchestration_v2_projection_run_attempts: totals.attempts,
      orchestration_v2_projection_nodes: totals.nodes,
      orchestration_v2_projection_provider_turns: totals.attempts,
      orchestration_v2_projection_provider_threads: totals.providerThreads,
      orchestration_v2_projection_provider_sessions: totals.providerSessions,
      orchestration_v2_projection_provider_session_bindings: totals.providerSessions,
      orchestration_v2_projection_messages: totals.messages,
      orchestration_v2_projection_turn_items: totalItems,
      orchestration_v2_turn_item_positions: totalItems,
      orchestration_v2_projection_plans: plans,
      orchestration_v2_projection_checkpoint_scopes: scopes,
      orchestration_v2_projection_checkpoints: checkpointRows,
      orchestration_v2_projection_subagents: totals.subagents,
      orchestration_v2_projection_runtime_requests: totals.runtimeRequests,
      orchestration_v2_projection_context_transfers: transfers,
      orchestration_v2_projection_metadata: 1,
      orchestration_v2_legacy_imports: legacyThreads,
      orchestration_v2_message_search_docsize: searchable,
      projection_threads: legacyThreads,
      projection_thread_messages: legacyThreads > 0 ? sum(legacyMessages) : 0,
      orchestration_events: projects + threadEvents,
    },
  };
};

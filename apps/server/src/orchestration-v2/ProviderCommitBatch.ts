import { CurrentWaitObserver, operationName } from "@t3tools/shared/WaitObserver";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as IngestLag from "./IngestLag.ts";

const MAX_EVENTS = 64;
const MAX_BYTES = 1024 * 1024;
// While another write waits, a shared transaction stops taking jobs after this long.
const MAX_HOLD_NANOS = 100_000_000n;
// A stream waiting this long ranks like a main thread, so subagents cannot starve.
const STARVATION_MILLIS = 30_000;
// Bounds snapshots held across stream queues; a full backlog makes streams wait to read more.
const MAX_QUEUED = 4 * MAX_EVENTS;
const encodeQueue = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

export const enabled = Config.Boolean("T3CODE_PROVIDER_COMMIT_BATCHING").pipe(
  Config.withDefault(true),
  Effect.orDie,
);

/** Every job of a shared batch under a savepoint of its own, as before batches recorded their jobs. */
export const perJobSavepoints = Config.Boolean("T3CODE_PROVIDER_COMMIT_JOB_SAVEPOINTS").pipe(
  Config.withDefault(false),
  Effect.orDie,
);

type Snapshot =
  | {
      readonly type: "node.updated";
      readonly node: { readonly id: string; readonly threadId: string };
    }
  | {
      readonly type: "message.updated";
      readonly message: { readonly id: string; readonly threadId: string };
    }
  | {
      readonly type: "turn_item.updated";
      readonly turnItem: { readonly id: string; readonly threadId: string };
    }
  | {
      readonly type: "plan.updated";
      readonly plan: { readonly id: string; readonly threadId: string };
    };
/** Provider snapshots, which a consumer batches unless it says otherwise. */
export const isSnapshot = (event: { readonly type: string }): event is Snapshot =>
  event.type === "node.updated" ||
  event.type === "message.updated" ||
  event.type === "turn_item.updated" ||
  event.type === "plan.updated";
// Undefined for an item that names no entity, such as a consumer's own item under a snapshot type.
const snapshotEntity = (event: Snapshot) => {
  const entity: { readonly id?: unknown; readonly threadId?: unknown } | undefined =
    event.type === "node.updated"
      ? event.node
      : event.type === "message.updated"
        ? event.message
        : event.type === "turn_item.updated"
          ? event.turnItem
          : event.plan;
  return typeof entity?.id === "string" && typeof entity.threadId === "string"
    ? { key: `${event.type}:${entity.id}`, threadId: entity.threadId }
    : undefined;
};

/** Who a provider stream writes for; ranks its snapshots against other streams'. */
export interface StreamPriority {
  readonly threadId: string;
  /** A subagent or delegated task, which yields to main threads. */
  readonly delegated: boolean;
}

export class CommitBatchError extends Schema.TaggedError<CommitBatchError>()("CommitBatchError", {
  cause: Schema.Defect(),
}) {}
export const isCommitBatchError = Schema.is(CommitBatchError);

const WAIT_SITE = "ProviderCommitBatch.queue";
const BATCH_SITE = "ProviderCommitBatch.batch";

interface StreamState {
  readonly owner: { active: boolean };
  readonly priority: StreamPriority | undefined;
  /** Queued snapshots not yet taken into a batch, in stream order. */
  readonly jobs: Array<Job>;
  unsettled: number;
  servedAt: number;
  /** A control, failure or EOF of this stream waits for its queued snapshots. */
  finishing: boolean;
}
interface Job {
  readonly stream: StreamState;
  /** Still in its stream's queue, holding one of the backlog's `MAX_QUEUED` slots. */
  queued: boolean;
  readonly key: string;
  /** The thread its writes are for; undefined when unknown, and then every barrier waits for it. */
  readonly threadId: string | undefined;
  readonly queuedAt: number;
  readonly queuedNs: bigint;
  /** The caller's operation, and the batch flushing when this one queued; only while observed. */
  readonly operation: string | undefined;
  readonly queuedBehind: string | undefined;
  readonly bytes: number;
  readonly execute: Effect.Effect<void, CommitBatchError>;
  readonly settle: (
    exit: Exit.Exit<void, CommitBatchError>,
    preserveFailure: boolean,
  ) => Effect.Effect<void>;
  /** Settles a job a later snapshot of the same entity replaced, with that snapshot's outcome. */
  readonly skip: (exit: Exit.Exit<void, CommitBatchError>) => Effect.Effect<void>;
  readonly cancel: Effect.Effect<void>;
  readonly settled: Effect.Effect<void>;
}
interface Unit {
  readonly job: Job;
  readonly superseded: ReadonlyArray<Job>;
}
interface Batch {
  readonly units: ReadonlyArray<Unit>;
  readonly reason: "ready" | "limit";
  readonly dequeued: number;
}

/**
 * Snapshots carry an entity's full state, so an earlier one is dropped when a later one of the
 * same entity follows it in this stream with no first appearance of another entity in between.
 * Kept jobs stay in stream order, and entities first appear in the same order as before.
 */
function coalesce(jobs: ReadonlyArray<Job>): Array<Unit> {
  const firstSeen = new Map<string, number>();
  jobs.forEach((job, index) => {
    if (!firstSeen.has(job.key)) firstSeen.set(job.key, index);
  });
  const survivor: Array<number> = Array.from({ length: jobs.length }, (_, index) => index);
  const nextOfKey = new Map<string, number>();
  for (let index = jobs.length - 1; index >= 0; index--) {
    const job = jobs[index]!;
    const next = nextOfKey.get(job.key);
    nextOfKey.set(job.key, index);
    if (next === undefined) continue;
    let replaced = true;
    for (let between = index + 1; replaced && between < next; between++) {
      if (firstSeen.get(jobs[between]!.key)! > index) replaced = false;
    }
    if (replaced) survivor[index] = survivor[next]!;
  }
  const superseded = new Map<number, Array<Job>>();
  jobs.forEach((job, index) => {
    const kept = survivor[index]!;
    if (kept !== index) superseded.set(kept, [...(superseded.get(kept) ?? []), job]);
  });
  return jobs.flatMap((job, index) =>
    survivor[index] === index ? [{ job, superseded: superseded.get(index) ?? [] }] : [],
  );
}

/**
 * What a caller does while its write waits on the lane: an admitted effect lends its slot out
 * (`suspend`) and takes it back (`resume`, which never waits). Only on the `owner` fiber: fibers it
 * forks (a run's ingestion) inherit the context but not the slot.
 */
export const CurrentLaneWait = Context.Reference<
  | {
      readonly owner: number;
      readonly suspend: Effect.Effect<void>;
      readonly resume: Effect.Effect<void>;
    }
  | undefined
>("t3/ProviderCommitBatch/CurrentLaneWait", { defaultValue: () => undefined });

const CurrentWorker = Context.Reference<number | undefined>(
  "t3/ProviderCommitBatch/CurrentWorker",
  {
    defaultValue: () => undefined,
  },
);

/** What a snapshot job wrote, kept while its batch is open. */
export interface Recording {
  /** Runs the job's body, keeping each statement it writes. */
  readonly around: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  /** Applies the kept statements again, under a savepoint of their own, without running the body. */
  readonly reapply: Effect.Effect<void, CommitBatchError>;
  /** Drops what the job queued for after the commit (publications, counts): a failed job's. */
  readonly discard: Effect.Effect<void>;
}

/**
 * How a shared batch keeps one job's failure from its other jobs while running each job's body once:
 * one consumer's run of jobs shares a savepoint (none for a single-consumer batch); on a failure, the
 * run rolls back and the jobs before the failed one are applied again from what they wrote.
 */
export interface Isolation {
  /** Runs one consumer's run of jobs, in a batch of several consumers, under a savepoint. */
  readonly segment: <A, E>(work: Effect.Effect<A, E>) => Effect.Effect<A, E | CommitBatchError>;
  /** Starts a job's recording: marks what it queues for after the commit, and keeps what it writes. */
  readonly record: Effect.Effect<Recording>;
  /** Rolls the whole batch transaction back on its own connection, which it keeps. */
  readonly restart: Effect.Effect<void, CommitBatchError>;
  /** Every job of a multi-job batch under a savepoint of its own: the reference the batch must equal. */
  readonly perJobSavepoints?: boolean;
}

/** No transaction to isolate in: jobs run as they are, and a failure keeps nothing to undo. */
export const noIsolation: Isolation = {
  segment: (work) => work,
  record: Effect.succeed({
    around: (effect) => effect,
    reapply: Effect.void,
    discard: Effect.void,
  }),
  restart: Effect.void,
};

/** A run of one consumer's jobs rolled back by its failed job, with the jobs before it to apply again. */
class SegmentFailed {
  readonly applied: ReadonlyArray<Recording>;
  constructor(applied: ReadonlyArray<Recording>) {
    this.applied = applied;
  }
}

export const make = Effect.fn("ProviderCommitBatch.make")(function* (
  commit: (work: Effect.Effect<void, CommitBatchError>) => Effect.Effect<void, CommitBatchError>,
  enabled: boolean,
  isolation: Isolation = noIsolation,
  /** Whether a client has the thread open; its streams rank first. */
  isViewed: (threadId: string) => boolean = () => false,
  /**
   * Whether a later snapshot of an entity replaces an earlier one still queued, whose `process`
   * then never runs. Off unless asked: a run's ingestion folds its backlog in
   * its own mailbox first and must see every item it hands over processed.
   */
  coalesceSnapshots = false,
) {
  const wake = yield* Queue.sliding<void>(1);
  const backlog = yield* Semaphore.make(MAX_QUEUED);
  const streams = new Set<StreamState>();
  const pending = new Set<Job>();
  let closed = false;
  let servings = 0;
  // Writes in `barrier` that have not finished; they wait on the batch or the writer it holds.
  let waitingWriters = 0;
  // Batched items that name no snapshot entity each keep a key of their own: none replaces another.
  let unkeyed = 0;
  const observer = yield* CurrentWaitObserver;
  // The operation of the batch the worker is flushing, named to the writes that wait behind it.
  let flushing: string | undefined;
  if (enabled && observer !== undefined) {
    const unregister = observer.register(
      "ProviderCommitBatch.depth",
      "gauge",
      () => pending.size + waitingWriters,
    );
    yield* Effect.addFinalizer(() => Effect.sync(unregister));
  }
  const shutdown = Effect.gen(function* () {
    closed = true;
    yield* Queue.shutdown(wake);
    for (const job of pending) yield* job.cancel;
    yield* backlog.releaseAll;
  });
  yield* Effect.addFinalizer(() => shutdown);
  const entityOf = (event: { readonly type: string }) =>
    (isSnapshot(event) ? snapshotEntity(event) : undefined) ?? {
      key: `${event.type}#${++unkeyed}`,
      threadId: undefined,
    };
  // These helpers run once per raw event; the shared transaction owns the tracing span.
  const enqueue = Effect.fnUntraced(function* <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    stream: StreamState,
    entity: { readonly key: string; readonly threadId: string | undefined },
    bytes: number,
    skipped: A,
  ) {
    const caller = yield* Effect.context<R>();
    const reply = yield* Deferred.make<A, E | CommitBatchError>();
    let ownFailure: Cause.Cause<E> | undefined;
    let result: A;
    const release = () => {
      if (pending.delete(job)) stream.unsettled--;
    };
    observer?.depth(WAIT_SITE, pending.size);
    const job: Job = {
      stream,
      queued: false,
      key: entity.key,
      threadId: entity.threadId,
      queuedAt: yield* Clock.currentTimeMillis,
      queuedNs: observer === undefined ? 0n : yield* Clock.monotonicTimeNanos,
      operation: observer === undefined ? undefined : (operationName(caller) ?? "(untraced)"),
      queuedBehind: flushing,
      bytes,
      execute: effect.pipe(
        Effect.tapCause((cause) =>
          Effect.sync(() => {
            ownFailure = cause;
          }),
        ),
        Effect.mapError((cause) => new CommitBatchError({ cause })),
        Effect.provideContext(caller),
        Effect.tap((value) =>
          Effect.sync(() => {
            result = value;
          }),
        ),
        Effect.asVoid,
      ),
      settle: Effect.fnUntraced(function* (
        exit: Exit.Exit<void, CommitBatchError>,
        preserveFailure: boolean,
      ) {
        release();
        if (Exit.isFailure(exit)) {
          stream.owner.active = false;
          yield* Deferred.failCause(
            reply,
            preserveFailure && ownFailure !== undefined ? ownFailure : exit.cause,
          );
        } else yield* Deferred.succeed(reply, result);
      }),
      skip: (exit) =>
        Exit.isFailure(exit)
          ? job.settle(exit, false)
          : Effect.suspend(() => {
              release();
              return Deferred.succeed(reply, skipped);
            }),
      cancel: Effect.suspend(() => {
        release();
        const dequeued = job.queued;
        job.queued = false;
        return Deferred.interrupt(reply).pipe(
          Effect.andThen(dequeued ? backlog.release(1) : Effect.void),
        );
      }),
      settled: Deferred.await(reply).pipe(Effect.exit, Effect.asVoid),
    };
    yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (closed) return yield* Deferred.interrupt(reply);
        yield* restore(backlog.take(1));
        if (closed) {
          yield* backlog.release(1);
          return yield* Deferred.interrupt(reply);
        }
        job.queued = true;
        pending.add(job);
        stream.unsettled++;
        stream.jobs.push(job);
        yield* Queue.offer(wake, undefined);
      }),
    );
    return Deferred.await(reply);
  });
  const rank = (stream: StreamState, now: number) => {
    const tier =
      stream.priority === undefined
        ? 1
        : isViewed(stream.priority.threadId)
          ? 0
          : stream.priority.delegated
            ? 2
            : 1;
    const starved = now - stream.jobs[0]!.queuedAt >= STARVATION_MILLIS;
    return [starved ? Math.min(tier, 1) : tier, stream.finishing ? 0 : 1, stream.servedAt];
  };
  // Viewed streams first, then main threads, then subagents; within a tier, streams finishing a
  // control first, then the stream served longest ago.
  const nextBatch = (now: number): Batch | undefined => {
    const ready = Array.from(streams, (stream) =>
      stream.jobs.length === 0 ? [] : [{ stream, rank: rank(stream, now) }],
    )
      .flat()
      .sort((left, right) => {
        for (let index = 0; index < left.rank.length; index++) {
          const order = left.rank[index]! - right.rank[index]!;
          if (order !== 0) return order;
        }
        return 0;
      });
    if (ready.length === 0) return undefined;
    const units: Array<Unit> = [];
    let bytes = 0;
    let dequeued = 0;
    let reason: Batch["reason"] = "ready";
    for (const { stream } of ready) {
      let take = 0;
      while (take < stream.jobs.length && take < MAX_EVENTS - units.length) {
        const next = stream.jobs[take]!.bytes;
        if (units.length + take > 0 && bytes + next > MAX_BYTES) break;
        bytes += next;
        take++;
      }
      if (take < stream.jobs.length) reason = "limit";
      if (take === 0) break;
      const taken = stream.jobs.splice(0, take);
      for (const job of taken) job.queued = false;
      dequeued += take;
      units.push(
        ...(coalesceSnapshots ? coalesce(taken) : taken.map((job) => ({ job, superseded: [] }))),
      );
      stream.servedAt = ++servings;
      if (units.length >= MAX_EVENTS || bytes >= MAX_BYTES) break;
    }
    return { units, reason, dequeued };
  };
  const cancelUnit = Effect.fnUntraced(function* ({ job, superseded }: Unit) {
    yield* job.cancel;
    for (const replaced of superseded) yield* replaced.cancel;
  });
  // Commits a prefix of `units` in one transaction and returns how many it took.
  const commitPrefix = (units: ReadonlyArray<Unit>, reason: Batch["reason"]) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const outcomes = new Map<Job, Exit.Exit<void, CommitBatchError>>();
        let taken = 0;
        let startedAt = 0n;
        let held = false;
        // Whether the transaction stops taking units: it has held the writer long enough while
        // another write waits. Checked before each unit but the first.
        const holdEnds = Effect.gen(function* () {
          if (held) return true;
          held =
            taken > 0 &&
            waitingWriters > 0 &&
            (yield* Clock.monotonicTimeNanos) - startedAt >= MAX_HOLD_NANOS;
          return held;
        });
        // A savepoint per unit, as the batch must behave; a lone unit fails its transaction instead.
        const perJob = Effect.gen(function* () {
          for (const unit of units) {
            if (yield* holdEnds) return;
            taken++;
            const { job } = unit;
            if (!job.stream.owner.active) {
              yield* cancelUnit(unit);
              continue;
            }
            const isolated = units.length > 1;
            const recording = isolated ? yield* isolation.record : undefined;
            const outcome = yield* (isolated ? isolation.segment(job.execute) : job.execute).pipe(
              Effect.exit,
            );
            outcomes.set(job, outcome);
            if (Exit.isFailure(outcome)) {
              if (recording !== undefined) yield* recording.discard;
              job.stream.owner.active = false;
              if (units.length === 1) return yield* Effect.failCause(outcome.cause);
            }
          }
        });
        // One stream's consecutive units, recorded; stops at a failure, after which the stream's
        // later units cancel.
        const runSegment = Effect.fnUntraced(function* (segment: ReadonlyArray<Unit>) {
          const applied: Array<Recording> = [];
          let failed: Recording | undefined;
          for (const unit of segment) {
            if (yield* holdEnds) break;
            taken++;
            const { job } = unit;
            if (!job.stream.owner.active) {
              yield* cancelUnit(unit);
              continue;
            }
            const recording = yield* isolation.record;
            const outcome = yield* recording.around(job.execute).pipe(Effect.exit);
            outcomes.set(job, outcome);
            if (Exit.isSuccess(outcome)) applied.push(recording);
            else {
              job.stream.owner.active = false;
              failed = recording;
            }
          }
          return { applied, failed };
        });
        const optimistic = Effect.gen(function* () {
          const segments: Array<Array<Unit>> = [];
          for (const unit of units) {
            const last = segments.at(-1);
            if (last !== undefined && last[0]!.job.stream === unit.job.stream) last.push(unit);
            else segments.push([unit]);
          }
          if (segments.length === 1) {
            const { applied, failed } = yield* runSegment(segments[0]!);
            if (failed === undefined) return;
            yield* failed.discard;
            yield* isolation.restart;
            for (const recording of applied) yield* recording.reapply;
            return;
          }
          for (const segment of segments) {
            if (yield* holdEnds) return;
            const rolledBack = yield* isolation
              .segment(
                Effect.flatMap(runSegment(segment), ({ applied, failed }) =>
                  failed === undefined
                    ? Effect.undefined
                    : Effect.andThen(failed.discard, Effect.fail(new SegmentFailed(applied))),
                ),
              )
              .pipe(
                Effect.catchIf(
                  (error): error is SegmentFailed => error instanceof SegmentFailed,
                  (error) => Effect.succeed(error),
                ),
              );
            if (rolledBack !== undefined)
              for (const recording of rolledBack.applied) yield* recording.reapply;
          }
        });
        const work = Effect.gen(function* () {
          // A retried transaction runs this again from the first unit.
          outcomes.clear();
          taken = 0;
          held = false;
          startedAt = yield* Clock.monotonicTimeNanos;
          yield* units.length === 1 || isolation.perJobSavepoints === true ? perJob : optimistic;
        });
        const exit = yield* restore(
          commit(work).pipe(Effect.provideService(CurrentWorker, yield* Effect.fiberId)),
        ).pipe(Effect.exit);
        const committed = taken === 0 ? units : units.slice(0, taken);
        const jobs = committed.flatMap((unit) => [unit.job, ...unit.superseded]);
        // Events per commit.
        observer?.depth(BATCH_SITE, jobs.length);
        yield* Effect.annotateCurrentSpan({
          "orchestration_v2.work_item_count": committed.length,
          "orchestration_v2.coalesced_work_item_count": jobs.length - committed.length,
          "orchestration_v2.deferred_work_item_count": units.length - committed.length,
          "orchestration_v2.consumer_count": new Set(jobs.map((job) => job.stream)).size,
          "orchestration_v2.raw_json_bytes": jobs.reduce((bytes, job) => bytes + job.bytes, 0),
          "orchestration_v2.batch_flush_reason": reason,
          "orchestration_v2.backlog_batching": true,
          "orchestration_v2.batch_wait_ms":
            (yield* Clock.currentTimeMillis) - Math.min(...jobs.map((job) => job.queuedAt)),
        });
        for (const { job, superseded } of committed) {
          const outcome = outcomes.get(job) ?? Exit.void;
          const final = Exit.isSuccess(exit) ? outcome : exit;
          yield* job.settle(
            final,
            Exit.isSuccess(exit) ||
              (Exit.isFailure(exit) &&
                Exit.isFailure(outcome) &&
                Cause.squash(exit.cause) === Cause.squash(outcome.cause)),
          );
          for (const replaced of superseded) yield* replaced.skip(final);
        }
        return committed.length;
      }),
    ).pipe(Effect.withSpan("orchestrationV2.ProviderCommitBatch.flush"));
  if (enabled) {
    yield* Effect.forever(
      Effect.gen(function* () {
        const batch = nextBatch(yield* Clock.currentTimeMillis);
        if (batch === undefined) return yield* Queue.take(wake);
        yield* backlog.release(batch.dequeued);
        if (observer !== undefined) {
          const flushNs = yield* Clock.monotonicTimeNanos;
          for (const { job, superseded } of batch.units) {
            for (const queued of [job, ...superseded]) {
              const waitNs = Number(flushNs - queued.queuedNs);
              observer.wait(WAIT_SITE, waitNs, queued.queuedBehind);
              observer.wait(`${WAIT_SITE}.snapshot`, waitNs, undefined);
            }
          }
          flushing = batch.units[0]?.job.operation;
        }
        let units = batch.units;
        while (units.length > 0) units = units.slice(yield* commitPrefix(units, batch.reason));
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            flushing = undefined;
          }),
        ),
      ),
    ).pipe(Effect.ensuring(shutdown), Effect.forkScoped);
  }
  /**
   * Runs `effect` after queued snapshots for `threadIds` commit; omitted, after every queued
   * snapshot. Other threads' snapshots never delay it.
   */
  const barrier = Effect.fnUntraced(function* <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    threadIds?: ReadonlyArray<string>,
  ) {
    if (closed) return yield* Effect.interrupt;
    if (!enabled || (yield* CurrentWorker) === (yield* Effect.fiberId)) return yield* effect;
    const awaited = Array.from(pending).filter(
      (job) =>
        threadIds === undefined || job.threadId === undefined || threadIds.includes(job.threadId),
    );
    // A write counts as waiting until it ends, also one with no snapshot to wait for: it may wait
    // on the writer the batch holds, so the batch stops taking jobs after its hold limit.
    const counted = <B, E2, R2>(write: Effect.Effect<B, E2, R2>) =>
      Effect.uninterruptibleMask((restore) => {
        waitingWriters++;
        return restore(write).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              waitingWriters--;
            }),
          ),
        );
      });
    observer?.depth(WAIT_SITE, awaited.length);
    if (awaited.length === 0) {
      observer?.wait(WAIT_SITE, 0, undefined);
      observer?.wait(`${WAIT_SITE}.barrier`, 0, undefined);
      return yield* counted(effect);
    }
    const holder = flushing;
    const waitStartNs = observer === undefined ? 0n : yield* Clock.monotonicTimeNanos;
    const wait = Effect.gen(function* () {
      for (const job of awaited) yield* job.settled;
      if (observer !== undefined) {
        const waitNs = Number((yield* Clock.monotonicTimeNanos) - waitStartNs);
        observer.wait(WAIT_SITE, waitNs, holder);
        observer.wait(`${WAIT_SITE}.barrier`, waitNs, undefined);
      }
    });
    const lent = yield* CurrentLaneWait;
    // The slot is out while the write waits for the snapshots before it, and back before the write
    // runs; taking it back never waits, so this runs on every exit.
    const waited =
      lent?.owner === (yield* Effect.fiberId)
        ? Effect.acquireUseRelease(
            lent.suspend,
            () => wait,
            () => lent.resume,
          )
        : wait;
    return yield* counted(
      Effect.gen(function* () {
        yield* waited;
        if (closed) return yield* Effect.interrupt;
        return yield* effect;
      }),
    );
  });
  const consume = Effect.fnUntraced(function* <A extends { readonly type: string }, E, R, E2, R2>(
    source: Stream.Stream<A, E, R>,
    process: (event: A) => Effect.Effect<void, E2, R2>,
    after: Effect.Effect<boolean, E2, R2>,
    canBatch: (event: A) => Effect.Effect<boolean, never, R2> = (event) =>
      Effect.succeed(isSnapshot(event)),
    priority?: StreamPriority,
  ) {
    if (!enabled)
      return yield* source.pipe(
        Stream.mapEffect((event) => {
          const ingest = { persisted: false };
          return process(event).pipe(
            Effect.provideService(IngestLag.CurrentIngest, ingest),
            Effect.tap(() =>
              ingest.persisted ? IngestLag.reportCommitted(observer, event) : Effect.void,
            ),
            Effect.andThen(after),
          );
        }),
        Stream.takeUntil(Boolean),
        Stream.runDrain,
      );
    const stream: StreamState = {
      owner: { active: true },
      priority,
      jobs: [],
      unsettled: 0,
      servedAt: 0,
      finishing: false,
    };
    streams.add(stream);
    const signals = source.pipe(
      Stream.map((event) => ({ _tag: "Event" as const, event })),
      Stream.catchCause((cause) => Stream.succeed({ _tag: "Failure" as const, cause })),
      Stream.concat(Stream.succeed({ _tag: "End" as const })),
    );
    let resume: Effect.Effect<void> = Effect.void;
    // Replies run in order, so EOF, failures and controls follow this stream's earlier snapshots
    // without waiting for other streams' work.
    yield* signals.pipe(
      Stream.mapEffect((signal) =>
        Effect.gen(function* () {
          yield* resume;
          const processed = yield* Deferred.make<void>();
          const ingest = { persisted: false };
          const event = signal._tag === "Event" ? signal.event : undefined;
          let reply: Effect.Effect<boolean, E | E2 | CommitBatchError, R2>;
          let check = false;
          if (event !== undefined && (yield* canBatch(event))) {
            const bytes = Buffer.byteLength(yield* encodeQueue(event));
            reply = yield* enqueue(
              process(event).pipe(
                Effect.provideService(IngestLag.CurrentIngest, ingest),
                Effect.as(false),
              ),
              stream,
              entityOf(event),
              bytes,
              false,
            );
          } else {
            if (stream.unsettled > 0) stream.finishing = true;
            if (signal._tag === "End") reply = Effect.succeed(true);
            else if (signal._tag === "Failure") reply = Effect.failCause(signal.cause);
            else {
              const control = signal.event;
              reply = Effect.sync(() => {
                stream.finishing = false;
              }).pipe(
                Effect.andThen(
                  process(control).pipe(Effect.provideService(IngestLag.CurrentIngest, ingest)),
                ),
                Effect.as(false),
              );
              check = true;
              resume = Deferred.await(processed);
            }
          }
          return { reply, processed, check, event, ingest };
        }),
      ),
      Stream.buffer({ capacity: MAX_EVENTS }),
      Stream.mapEffect(({ reply, processed, check, event, ingest }) =>
        reply.pipe(
          // The reply settles once the event's writes commit.
          Effect.tap(() =>
            event === undefined || !ingest.persisted
              ? Effect.void
              : IngestLag.reportCommitted(observer, event),
          ),
          Effect.flatMap((end) =>
            end ? Effect.succeed(true) : check ? after : Effect.succeed(false),
          ),
          Effect.tap((stop) =>
            Effect.sync(() => {
              if (stop) stream.owner.active = false;
            }),
          ),
          Effect.ensuring(Deferred.succeed(processed, undefined)),
        ),
      ),
      Stream.takeUntil(Boolean),
      Stream.runDrain,
      Effect.ensuring(
        Effect.gen(function* () {
          stream.owner.active = false;
          streams.delete(stream);
          for (const job of stream.jobs.splice(0)) yield* job.cancel;
        }),
      ),
    );
  });
  return { barrier, consume };
});

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
import * as Stream from "effect/Stream";

import * as IngestLag from "./IngestLag.ts";

const MAX_EVENTS = 64;
const MAX_BYTES = 1024 * 1024;
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

/** Provider snapshots, which a consumer batches unless it says otherwise. */
export const isSnapshot = (event: { readonly type: string }) =>
  event.type === "node.updated" ||
  event.type === "message.updated" ||
  event.type === "turn_item.updated" ||
  event.type === "plan.updated";

export class CommitBatchError extends Schema.TaggedError<CommitBatchError>()("CommitBatchError", {
  cause: Schema.Defect(),
}) {}
export const isCommitBatchError = Schema.is(CommitBatchError);

const WAIT_SITE = "ProviderCommitBatch.queue";
const BATCH_SITE = "ProviderCommitBatch.batch";

/** A provider snapshot a consumer batched, or a write's barrier: a command's or a control's. */
type JobKind = "snapshot" | "barrier";

interface Job {
  readonly owner: { active: boolean };
  readonly kind: JobKind;
  readonly queuedAt: number;
  readonly queuedNs: bigint;
  /** The caller's operation, and the job flushing when this one queued; only while observed. */
  readonly operation: string | undefined;
  readonly queuedBehind: string | undefined;
  readonly snapshot: boolean;
  readonly bytes: number;
  readonly execute: Effect.Effect<void, CommitBatchError>;
  readonly settle: (
    exit: Exit.Exit<void, CommitBatchError>,
    preserveFailure: boolean,
  ) => Effect.Effect<void>;
  readonly cancel: Effect.Effect<void>;
}
interface Batch {
  readonly events: ReadonlyArray<Job>;
  readonly bytes: number;
  readonly reason: "ready" | "limit" | "control";
}

function collect(queue: Queue.Queue<Job>) {
  return Stream.fromEffectRepeat(Queue.takeBetween(queue, 1, MAX_EVENTS)).pipe(
    Stream.flatMap((ready) => {
      const batches: Array<Batch> = [];
      let events: Array<Job> = [];
      let bytes = 0;
      const flush = (reason: Batch["reason"]) => {
        if (events.length > 0) batches.push({ events, bytes, reason });
        events = [];
        bytes = 0;
      };
      for (const job of ready) {
        if (!job.snapshot) {
          flush("control");
          batches.push({ events: [job], bytes: 0, reason: "control" });
        } else {
          if (events.length > 0 && bytes + job.bytes > MAX_BYTES) flush("limit");
          events.push(job);
          bytes += job.bytes;
          if (events.length >= MAX_EVENTS || bytes >= MAX_BYTES) flush("limit");
        }
      }
      flush("ready");
      return Stream.fromArray(batches);
    }),
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
) {
  const queue = yield* Queue.bounded<Job>(MAX_EVENTS);
  const pending = new Set<Job>();
  const observer = yield* CurrentWaitObserver;
  // The operation of the batch the worker is flushing, named to the jobs that queue behind it.
  let flushing: string | undefined;
  if (enabled && observer !== undefined) {
    const unregister = observer.register("ProviderCommitBatch.depth", "gauge", () => pending.size);
    yield* Effect.addFinalizer(() => Effect.sync(unregister));
  }
  let closed = false;
  const shutdown = Effect.gen(function* () {
    closed = true;
    yield* Queue.shutdown(queue);
    for (const job of pending) yield* job.cancel;
  });
  yield* Effect.addFinalizer(() => shutdown);
  // These helpers run once per raw event; the shared transaction owns the tracing span.
  const enqueue = Effect.fnUntraced(function* <A, E, R>(
    effect: Effect.Effect<A, E, R>,
    token: { active: boolean },
    kind: JobKind,
    bytes: number,
  ) {
    const snapshot = kind === "snapshot";
    const caller = yield* Effect.context<R>();
    const reply = yield* Deferred.make<A, E | CommitBatchError>();
    let ownFailure: Cause.Cause<E> | undefined;
    let result: A;
    observer?.depth(WAIT_SITE, pending.size);
    const job: Job = {
      owner: token,
      kind,
      queuedAt: yield* Clock.currentTimeMillis,
      queuedNs: observer === undefined ? 0n : yield* Clock.monotonicTimeNanos,
      operation: observer === undefined ? undefined : (operationName(caller) ?? "(untraced)"),
      queuedBehind: flushing,
      snapshot,
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
        pending.delete(job);
        if (Exit.isFailure(exit)) {
          token.active = false;
          yield* Deferred.failCause(
            reply,
            preserveFailure && ownFailure !== undefined ? ownFailure : exit.cause,
          );
        } else yield* Deferred.succeed(reply, result);
      }),
      cancel: Effect.gen(function* () {
        pending.delete(job);
        yield* Deferred.interrupt(reply);
      }),
    };
    yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        pending.add(job);
        const accepted = yield* restore(Queue.offer(queue, job)).pipe(
          Effect.onError(() => job.cancel),
        );
        if (!accepted) yield* job.cancel;
      }),
    );
    return Deferred.await(reply);
  });
  // A savepoint per job, as the batch must behave; a lone job fails its transaction instead.
  const perJob = (
    jobs: ReadonlyArray<Job>,
    outcomes: Map<Job, Exit.Exit<void, CommitBatchError>>,
  ) =>
    Effect.forEach(
      jobs,
      (job) =>
        Effect.gen(function* () {
          if (!job.owner.active) return yield* job.cancel;
          const isolated = jobs.length > 1 && job.bytes > 0;
          const recording = isolated ? yield* isolation.record : undefined;
          const outcome = yield* (isolated ? isolation.segment(job.execute) : job.execute).pipe(
            Effect.exit,
          );
          outcomes.set(job, outcome);
          if (Exit.isFailure(outcome)) {
            if (recording !== undefined) yield* recording.discard;
            job.owner.active = false;
            if (jobs.length === 1) return yield* Effect.failCause(outcome.cause);
          }
        }),
      { discard: true },
    );
  // One consumer's consecutive jobs, recorded; stops at a failure, after which its later jobs cancel.
  const runSegment = Effect.fnUntraced(function* (
    segment: ReadonlyArray<Job>,
    outcomes: Map<Job, Exit.Exit<void, CommitBatchError>>,
  ) {
    const applied: Array<Recording> = [];
    let failed: Recording | undefined;
    for (const job of segment) {
      if (!job.owner.active) {
        yield* job.cancel;
        continue;
      }
      const recording = yield* isolation.record;
      const outcome = yield* recording.around(job.execute).pipe(Effect.exit);
      outcomes.set(job, outcome);
      if (Exit.isSuccess(outcome)) applied.push(recording);
      else {
        job.owner.active = false;
        failed = recording;
      }
    }
    return { applied, failed };
  });
  const optimistic = Effect.fnUntraced(function* (
    jobs: ReadonlyArray<Job>,
    outcomes: Map<Job, Exit.Exit<void, CommitBatchError>>,
  ) {
    const segments: Array<Array<Job>> = [];
    for (const job of jobs) {
      const last = segments.at(-1);
      if (last !== undefined && last[0]!.owner === job.owner) last.push(job);
      else segments.push([job]);
    }
    if (segments.length === 1) {
      const { applied, failed } = yield* runSegment(segments[0]!, outcomes);
      if (failed === undefined) return;
      yield* failed.discard;
      yield* isolation.restart;
      for (const recording of applied) yield* recording.reapply;
      return;
    }
    for (const segment of segments) {
      // A consumer's end writes nothing, so it needs no savepoint.
      if (segment.every((job) => job.bytes === 0)) {
        yield* runSegment(segment, outcomes);
        continue;
      }
      const rolledBack = yield* isolation
        .segment(
          Effect.flatMap(runSegment(segment, outcomes), ({ applied, failed }) =>
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
  if (enabled) {
    yield* collect(queue).pipe(
      Stream.runForEach((batch) =>
        Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const jobs = batch.events.filter((job) => job.owner.active);
            for (const job of batch.events) if (!job.owner.active) yield* job.cancel;
            if (jobs.length === 0) return;
            if (observer !== undefined) {
              const flushNs = yield* Clock.monotonicTimeNanos;
              for (const job of jobs) {
                const waitNs = Number(flushNs - job.queuedNs);
                observer.wait(WAIT_SITE, waitNs, job.queuedBehind);
                observer.wait(`${WAIT_SITE}.${job.kind}`, waitNs, undefined);
              }
              observer.depth(BATCH_SITE, jobs.length);
              flushing = jobs[0]!.operation;
            }
            yield* Effect.annotateCurrentSpan({
              "orchestration_v2.work_item_count": jobs.length,
              "orchestration_v2.consumer_count": new Set(jobs.map((job) => job.owner)).size,
              "orchestration_v2.raw_json_bytes": batch.bytes,
              "orchestration_v2.batch_flush_reason": batch.reason,
              "orchestration_v2.backlog_batching": true,
              "orchestration_v2.batch_wait_ms":
                (yield* Clock.currentTimeMillis) - Math.min(...jobs.map((job) => job.queuedAt)),
            });
            const outcomes = new Map<Job, Exit.Exit<void, CommitBatchError>>();
            const work =
              jobs.length === 1 || isolation.perJobSavepoints === true
                ? perJob(jobs, outcomes)
                : optimistic(jobs, outcomes);
            const exit = yield* restore(
              (jobs.every((job) => job.bytes === 0) ? work : commit(work)).pipe(
                Effect.provideService(CurrentWorker, yield* Effect.fiberId),
              ),
            ).pipe(Effect.exit);
            for (const job of jobs) {
              const outcome = outcomes.get(job) ?? Exit.void;
              yield* job.settle(
                Exit.isSuccess(exit) ? outcome : exit,
                Exit.isSuccess(exit) ||
                  (Exit.isFailure(exit) &&
                    Exit.isFailure(outcome) &&
                    Cause.squash(exit.cause) === Cause.squash(outcome.cause)),
              );
            }
          }),
        ).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              flushing = undefined;
            }),
          ),
          Effect.withSpan("orchestrationV2.ProviderCommitBatch.flush"),
        ),
      ),
      Effect.ensuring(shutdown),
      Effect.forkScoped,
    );
  }
  const barrier = Effect.fnUntraced(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
    if (closed) return yield* Effect.interrupt;
    if (!enabled || (yield* CurrentWorker) === (yield* Effect.fiberId)) return yield* effect;
    if (pending.size === 0) {
      observer?.depth(WAIT_SITE, 0);
      observer?.wait(WAIT_SITE, 0, undefined);
      observer?.wait(`${WAIT_SITE}.barrier`, 0, undefined);
      return yield* effect;
    }
    const token = { active: true };
    const queued = enqueue(Effect.exit(effect), token, "barrier", 0).pipe(
      Effect.flatten,
      Effect.orDie,
      Effect.flatten,
      Effect.ensuring(
        Effect.sync(() => {
          token.active = false;
        }),
      ),
    );
    const lent = yield* CurrentLaneWait;
    // The slot is out from before the write may wait for room in the queue until it settles; taking
    // it back never waits, so this runs on every exit.
    return yield* lent?.owner === (yield* Effect.fiberId)
      ? Effect.acquireUseRelease(
          lent.suspend,
          () => queued,
          () => lent.resume,
        )
      : queued;
  });
  const consume = Effect.fnUntraced(function* <A extends { readonly type: string }, E, R, E2, R2>(
    source: Stream.Stream<A, E, R>,
    process: (event: A) => Effect.Effect<void, E2, R2>,
    after: Effect.Effect<boolean, E2, R2>,
    canBatch: (event: A) => Effect.Effect<boolean, never, R2> = (event) =>
      Effect.succeed(isSnapshot(event)),
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
    const token = { active: true };
    const signals = source.pipe(
      Stream.map((event) => ({ _tag: "Event" as const, event })),
      Stream.catchCause((cause) => Stream.succeed({ _tag: "Failure" as const, cause })),
      Stream.concat(Stream.succeed({ _tag: "End" as const })),
    );
    let resume: Effect.Effect<void> = Effect.void;
    yield* signals.pipe(
      Stream.mapEffect((signal) =>
        Effect.gen(function* () {
          yield* resume;
          const processed = yield* Deferred.make<void>();
          const event = signal._tag === "Event" ? signal.event : undefined;
          // EOF only acknowledges prior work; it can share a commit with other consumers.
          const snapshot =
            signal._tag === "End" || (event !== undefined && (yield* canBatch(event)));
          const bytes =
            snapshot && event !== undefined ? Buffer.byteLength(yield* encodeQueue(event)) : 0;
          const ingest = { persisted: false };
          const work: Effect.Effect<boolean, E | E2, R2> =
            signal._tag === "Failure"
              ? Effect.failCause(signal.cause)
              : event === undefined
                ? Effect.succeed(true)
                : process(event).pipe(
                    Effect.provideService(IngestLag.CurrentIngest, ingest),
                    Effect.as(false),
                  );
          // A control runs after the replies before it settle, so only its own writes queue.
          const reply = snapshot ? yield* enqueue(work, token, "snapshot", bytes) : work;
          if (!snapshot) resume = Deferred.await(processed);
          return { reply, processed, check: !snapshot, event, ingest };
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
              if (stop) token.active = false;
            }),
          ),
          Effect.ensuring(Deferred.succeed(processed, undefined)),
        ),
      ),
      Stream.takeUntil(Boolean),
      Stream.runDrain,
      Effect.ensuring(
        Effect.sync(() => {
          token.active = false;
        }),
      ),
    );
  });
  return { barrier, consume };
});

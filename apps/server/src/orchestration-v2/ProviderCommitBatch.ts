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

const CurrentWorker = Context.Reference<number | undefined>(
  "t3/ProviderCommitBatch/CurrentWorker",
  {
    defaultValue: () => undefined,
  },
);

export const make = Effect.fn("ProviderCommitBatch.make")(function* (
  commit: (work: Effect.Effect<void, CommitBatchError>) => Effect.Effect<void, CommitBatchError>,
  enabled: boolean,
  isolate: (
    work: Effect.Effect<void, CommitBatchError>,
  ) => Effect.Effect<void, CommitBatchError> = (work) => work,
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
            const work = Effect.forEach(
              jobs,
              (job) =>
                Effect.gen(function* () {
                  if (!job.owner.active) return yield* job.cancel;
                  const outcome = yield* (
                    jobs.length > 1 && job.bytes > 0 ? isolate(job.execute) : job.execute
                  ).pipe(Effect.exit);
                  outcomes.set(job, outcome);
                  if (Exit.isFailure(outcome)) {
                    job.owner.active = false;
                    if (jobs.length === 1) return yield* Effect.failCause(outcome.cause);
                  }
                }),
              { discard: true },
            );
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
    return yield* enqueue(Effect.exit(effect), token, "barrier", 0).pipe(
      Effect.flatten,
      Effect.orDie,
      Effect.flatten,
      Effect.ensuring(
        Effect.sync(() => {
          token.active = false;
        }),
      ),
    );
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

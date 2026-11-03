import type {
  ApplicationStoredEvent,
  OrchestrationProjectShell,
  OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import type * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectStore from "./ProjectStore.ts";
import {
  makeLiveStreamBudget,
  type LiveStreamBufferError,
  type RetainedLiveItem,
} from "./LiveStreamBudget.ts";
import {
  loadShellSnapshotParts,
  makeShellDeltaAdmission,
  makeThreadShellVersions,
  projectShellDeltas,
  shellDeltaKey,
  shouldPublishShellApplicationEvent,
  toShellApplicationEvent,
  type ShellApplicationEvent,
  type ShellDelta,
  type VersionedShellDelta,
} from "./ShellStream.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

export class ShellDeltaHubError extends Schema.TaggedError<ShellDeltaHubError>()(
  "ShellDeltaHubError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

/** Every active shell as of `snapshotSequence`, before a subscriber's own trims. */
export interface ShellResyncBase {
  readonly schemaVersion: number;
  readonly snapshotSequence: number;
  readonly threads: ReadonlyArray<OrchestrationV2ThreadShell>;
  readonly projects: ReadonlyArray<OrchestrationProjectShell>;
}

/**
 * The subscriber fell more than its buffer behind: the deltas it held were dropped and `base`,
 * read after that, replaces them. The stream continues above `base.snapshotSequence`.
 */
export interface ShellResync {
  readonly kind: "resync";
  readonly base: ShellResyncBase;
}

export type ShellSubscriptionItem = ShellDelta | ShellResync;

export interface ShellDeltaSubscription {
  /**
   * The deltas buffered since the subscription was taken, without those at or below
   * `afterSequence` (the snapshot or replay the stream continues covers them). Take it once.
   */
  readonly stream: (
    afterSequence: number,
  ) => Stream.Stream<ShellSubscriptionItem, ShellDeltaHubError | LiveStreamBufferError>;
}

export interface ShellDeltaHubShape {
  /** For tests and diagnostics: the subscribers registered, and whether a base is retained. */
  readonly inspect: () => { readonly subscribers: number; readonly baseRetained: boolean };
  /**
   * Take the subscription before reading the snapshot it continues: every later committed event
   * reaches it as a projected delta, the latest per aggregate, under the live-stream budget. A
   * buffer that would overflow is emptied instead, and the stream's next pull yields a `resync`
   * whose base is shared by every subscriber emptied before that read; a hub failure ends it.
   */
  readonly subscribe: (limits?: {
    readonly maxItems?: number;
    readonly maxSerializedBytes?: number;
  }) => Effect.Effect<ShellDeltaSubscription, never, Scope.Scope>;
}

export class ShellDeltaHub extends Context.Service<ShellDeltaHub, ShellDeltaHubShape>()(
  "t3/orchestration-v2/ShellDeltaHub",
) {}

/** Matches the per-subscriber `groupedWithin(512, 50 ms)` the shell stream used before. */
export const SHELL_DELTA_BATCH_WINDOW = Duration.millis(50);

interface HubInstance {
  readonly output: PubSub.PubSub<ReadonlyArray<VersionedShellDelta>>;
  readonly failed: Deferred.Deferred<never, ShellDeltaHubError>;
  readonly scope: Scope.Closeable;
  subscribers: number;
  failure: ShellDeltaHubError | undefined;
}

const aggregateKey = (stored: ShellApplicationEvent) =>
  "aggregateKind" in stored ? `project:${stored.aggregateId}` : `thread:${stored.event.threadId}`;

const make = Effect.gen(function* () {
  const applicationEvents = yield* OrchestrationEventStore.OrchestrationEventStore;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const projectService = yield* ProjectService.ProjectService;
  const projectStore = yield* ProjectStore.ProjectStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const layerScope = yield* Effect.scope;
  const lifecycle = yield* Semaphore.make(1);
  let current: HubInstance | undefined;

  // A reset drops deltas at or below the highest sequence the hub has published. A base read
  // started after that publish covers them: the events were committed before it, and whatever
  // is committed after reaches the emptied buffer. So one read serves every subscriber a batch
  // resets, however the resets and their pulls interleave, instead of a snapshot per subscriber.
  let publishedSequence = 0;
  let latestBase:
    | {
        readonly coversThrough: number;
        readonly result: Deferred.Deferred<ShellResyncBase, ShellDeltaHubError>;
      }
    | undefined;
  // Per subscriber: the highest sequence its consumer has processed, and its reset.
  interface SubscriberState {
    seen: number;
    resetThrough: number | undefined;
  }
  const subscribers = new Set<SubscriberState>();
  // The last base is released once its wave is served: no subscriber is in the reset state, and
  // every subscriber has processed the batch the read covers, so none can still reset from it.
  const releaseBaseIfServed = () => {
    const base = latestBase;
    if (base === undefined) return;
    for (const state of subscribers) {
      if (state.resetThrough !== undefined || state.seen < base.coversThrough) return;
    }
    latestBase = undefined;
  };
  const resyncBase = (through: number) =>
    // Publishing a read to later resets and starting it is one uninterruptible step: a
    // subscriber cancelled between the two would leave a read nobody runs, and every reset
    // reusing it waiting forever. Only the wait for the result can be interrupted.
    Effect.uninterruptibleMask((restore) =>
      Effect.suspend(() => {
        if (latestBase !== undefined && latestBase.coversThrough >= through) {
          const read = latestBase;
          return restore(Deferred.await(read.result)).pipe(Effect.map((base) => ({ read, base })));
        }
        const read = {
          coversThrough: publishedSequence,
          result: Deferred.makeUnsafe<ShellResyncBase, ShellDeltaHubError>(),
        };
        latestBase = read;
        // One read transaction, as the initial snapshot: the sequence, the thread shells and the
        // project rows agree, so every delta above the sequence is newer than the base's rows.
        return loadShellSnapshotParts({
          sql,
          readThreads: threadManagement.readShellSnapshot({ location: "active" }),
          listProjects: projectStore.listShells(),
          latestSequence: applicationEvents.latestApplicationSequence,
        }).pipe(
          Effect.map(({ projects, threads, snapshotSequence }): ShellResyncBase => ({
            schemaVersion: threads.schemaVersion,
            snapshotSequence,
            threads: threads.threads,
            projects,
          })),
          Effect.mapError(
            (cause) =>
              new ShellDeltaHubError({ message: "The shell snapshot for a resync failed", cause }),
          ),
          Effect.exit,
          Effect.flatMap((exit) =>
            Effect.suspend(() => {
              // A failed read is not kept for its wave: the next pull reads again.
              if (Exit.isFailure(exit) && latestBase === read) latestBase = undefined;
              return Deferred.done(read.result, exit);
            }),
          ),
          // In the hub's scope: a subscriber interrupted mid-read must not fail the others' wait.
          Effect.forkIn(layerScope),
          Effect.andThen(restore(Deferred.await(read.result))),
          Effect.map((base) => ({ read, base })),
        );
      }),
    );

  // One committed-events consumer for every shell subscriber. Events coalesce per aggregate
  // while a batch is read, so memory is bounded by the aggregates that changed, not by events.
  const run = Effect.fnUntraced(function* (input: {
    readonly committed: PubSub.Subscription<ApplicationStoredEvent>;
    readonly output: PubSub.PubSub<ReadonlyArray<VersionedShellDelta>>;
  }) {
    const subagentThreadIds = new Set<string>(
      yield* threadManagement.listActiveSubagentThreadIds(),
    );
    const pending = new Map<string, ShellApplicationEvent>();
    const ready = yield* Queue.bounded<void>(1);
    const versions = makeThreadShellVersions();
    const drain = Effect.gen(function* () {
      while (true) {
        const events = yield* PubSub.takeAll(input.committed);
        const wasEmpty = pending.size === 0;
        for (const stored of events) {
          if (!shouldPublishShellApplicationEvent(stored, subagentThreadIds)) continue;
          const key = aggregateKey(stored);
          const previous = pending.get(key);
          if (previous === undefined || previous.sequence < stored.sequence) {
            pending.set(key, toShellApplicationEvent(stored));
          }
        }
        // Only the empty-to-nonempty transition wakes the projection; one token is ever queued.
        if (wasEmpty && pending.size > 0) yield* Queue.offer(ready, undefined);
      }
    });
    const project = Effect.fn("ShellDeltaHub.projectShellItems")(function* () {
      const batch = Array.from(pending.values());
      pending.clear();
      const items = yield* projectShellDeltas(batch, {
        threadShell: threadManagement.getThreadShellForEvent,
        projectShell: projectService.getShell,
      });
      const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
      for (const item of items) publishedSequence = Math.max(publishedSequence, item.sequence);
      yield* PubSub.publish(
        input.output,
        items.map((item) => ({
          sequence: item.sequence,
          item,
          version: versions.stamp(item, now),
        })),
      );
    });
    const projectBatches = Effect.gen(function* () {
      while (true) {
        yield* Queue.take(ready);
        yield* Effect.sleep(SHELL_DELTA_BATCH_WINDOW);
        yield* project();
      }
    });
    // Either side failing fails the hub, so no subscriber waits on a dead consumer.
    yield* Effect.all([drain, projectBatches], { concurrency: "unbounded", discard: true });
  });

  const start = Effect.gen(function* () {
    const scope = yield* Scope.fork(layerScope);
    const output = yield* PubSub.unbounded<ReadonlyArray<VersionedShellDelta>>();
    const failed = yield* Deferred.make<never, ShellDeltaHubError>();
    const committed = yield* applicationEvents.subscribeCommitted.pipe(Scope.provide(scope));
    const instance: HubInstance = { output, failed, scope, subscribers: 0, failure: undefined };
    yield* run({ committed, output }).pipe(
      Effect.catchCause((cause) =>
        Effect.suspend(() => {
          // Stopping with the last subscriber interrupts the fibers; only a failure is reported.
          if (Cause.hasInterruptsOnly(cause)) return Effect.void;
          instance.failure = new ShellDeltaHubError({
            message: "The shell delta hub stopped",
            cause,
          });
          return Deferred.fail(failed, instance.failure);
        }),
      ),
      Scope.provide(scope),
      Effect.forkIn(scope),
    );
    return instance;
  });

  const stop = (instance: HubInstance) =>
    Effect.suspend(() => {
      if (current === instance) current = undefined;
      latestBase = undefined;
      return Scope.close(instance.scope, Exit.void);
    });

  const subscribe: ShellDeltaHubShape["subscribe"] = Effect.fnUntraced(function* (limits) {
    // The subscription has its own scope: it closes with the subscriber's, or earlier when the
    // buffer overflows or the hub fails, and only then does this subscriber stop counting. Taking
    // the instance and registering that release is one uninterruptible step, so a subscriber
    // cancelled while the hub starts can neither orphan the hub nor leave a count behind.
    const subscriptionScope = yield* Scope.fork(yield* Effect.scope);
    const instance = yield* Effect.acquireRelease(
      lifecycle.withPermit(
        Effect.gen(function* () {
          if (current !== undefined && current.failure !== undefined) yield* stop(current);
          const instance = current ?? (yield* start);
          current = instance;
          instance.subscribers += 1;
          return instance;
        }),
      ),
      (instance) =>
        lifecycle.withPermit(
          Effect.suspend(() => {
            instance.subscribers -= 1;
            return instance.subscribers === 0 && current === instance
              ? stop(instance)
              : Effect.void;
          }),
        ),
    ).pipe(Scope.provide(subscriptionScope));
    const subscription = yield* PubSub.subscribe(instance.output).pipe(
      Scope.provide(subscriptionScope),
    );
    // Batches published before this subscription cannot reach it: it has them "processed". The
    // entry and its removal are one step, so a setup cancelled after it leaves nothing behind.
    const state = yield* Effect.acquireRelease(
      Effect.sync(() => {
        const state: SubscriberState = { seen: publishedSequence, resetThrough: undefined };
        subscribers.add(state);
        return state;
      }),
      (state) =>
        Effect.sync(() => {
          subscribers.delete(state);
          releaseBaseIfServed();
        }),
    ).pipe(Scope.provide(subscriptionScope));
    // One budget for all this subscriber retains: the latest delta per aggregate, held until the
    // stream is taken (its snapshot read runs meanwhile) and buffered after. When the stream
    // starts, held deltas the snapshot covers or an unchanged check skips are released.
    const budget = yield* makeLiveStreamBudget(limits);
    const admission = makeShellDeltaAdmission();
    const pending = new Map<string, RetainedLiveItem<VersionedShellDelta>>();
    const ready = yield* Queue.bounded<
      void,
      ShellDeltaHubError | LiveStreamBufferError | Cause.Done
    >(1);
    let afterSequence: number | undefined;
    let highestOffered = 0;
    // `state.resetThrough` is set by a reset: the highest sequence the dropped deltas can have,
    // cleared by a resync whose base covers it (or by a snapshot that does, when the stream opens).
    const setReset = (through: number | undefined) => {
      state.resetThrough = through;
      if (through === undefined) releaseBaseIfServed();
    };
    let closed = false;
    const close = (error?: LiveStreamBufferError) =>
      Effect.suspend(() => {
        if (closed) return Effect.void;
        closed = true;
        subscribers.delete(state);
        releaseBaseIfServed();
        budget.release(pending.values());
        pending.clear();
        return (error ? Queue.fail(ready, error) : Effect.void).pipe(
          Effect.andThen(Queue.shutdown(ready)),
        );
      });
    yield* Effect.addFinalizer(() => close());
    yield* budget.failed.pipe(
      Effect.catchTags({ LiveStreamBufferError: close }),
      Effect.forkScoped,
    );
    // Each delta is one synchronous step (budget swap, map update, wake-up), so it cannot
    // interleave with a drain. A swap the budget refuses empties the buffer for a resync instead
    // of failing it: the snapshot read after this reset replaces what was dropped.
    const offer = (delta: VersionedShellDelta, now: number): Effect.Effect<void> => {
      if (closed) return Effect.void;
      if (afterSequence !== undefined) {
        if (delta.sequence <= afterSequence) return Effect.void;
        // While the buffer is reset, nothing is unchanged: the client will show the base, which
        // may hold a state the hub never projected, and a shell equal to one sent before is news.
        if (state.resetThrough === undefined && !admission.admit(delta, now)) return Effect.void;
      }
      const key = shellDeltaKey(delta.item);
      const previous = pending.get(key);
      if (previous !== undefined && previous.value.sequence >= delta.sequence) return Effect.void;
      const wasEmpty = pending.size === 0;
      highestOffered = Math.max(highestOffered, delta.sequence);
      let swapped = budget.swapUnsafe(previous, delta);
      let report: Effect.Effect<void> = Effect.void;
      if (Effect.isEffect(swapped)) {
        // Entering the reset state is logged once: while the delivered chunk alone fills the
        // budget, every further delta takes this path until the client pulls.
        if (state.resetThrough === undefined) {
          report = Effect.logWarning("orchestration shell live buffer reset for a resync", {
            pendingDeltas: pending.size,
            afterSequence,
          });
        }
        budget.release(pending.values());
        pending.clear();
        setReset(highestOffered);
        // What the client already holds stays charged until it acknowledges; this delta waits
        // for the resync if even that leaves no room.
        swapped = budget.swapUnsafe(undefined, delta);
      }
      if (!Effect.isEffect(swapped)) pending.set(key, swapped);
      // A started stream needs a wakeup on the empty-to-nonempty transition and on a reset.
      if (afterSequence !== undefined && (wasEmpty || state.resetThrough !== undefined)) {
        Queue.offerUnsafe(ready, undefined);
      }
      return report;
    };
    // A batch is processed whole; with it processed, this subscriber cannot reset from the base
    // read after it any more.
    const processed = (batch: ReadonlyArray<VersionedShellDelta>) => {
      if (closed) return;
      const seenBefore = state.seen;
      for (const delta of batch) state.seen = Math.max(state.seen, delta.sequence);
      const base = latestBase;
      if (
        base !== undefined &&
        seenBefore < base.coversThrough &&
        state.seen >= base.coversThrough
      ) {
        releaseBaseIfServed();
      }
    };
    yield* Stream.fromSubscription(subscription).pipe(
      Stream.merge(Stream.fromEffect(Deferred.await(instance.failed))),
      Stream.ensuring(Scope.close(subscriptionScope, Exit.void)),
      Stream.runForEach((batch) =>
        Effect.clockWith((clock) => {
          const now = clock.currentTimeMillisUnsafe();
          return Effect.forEach(batch, (delta) => offer(delta, now), { discard: true }).pipe(
            Effect.andThen(Effect.sync(() => processed(batch))),
          );
        }),
      ),
      Effect.raceFirst(budget.failed),
      Effect.exit,
      Effect.flatMap((exit) =>
        Exit.isFailure(exit) ? Queue.failCause(ready, exit.cause) : Queue.end(ready),
      ),
      Effect.forkScoped({ startImmediately: true }),
    );
    // Continues from `sequence`: what it covers is released, and `keep` decides the rest. Deltas
    // held before the stream had a position were never admitted, so opening admits them once;
    // after a reset, what the buffer holds was admitted when offered and stays.
    const continueFrom = (sequence: number, keep: (delta: VersionedShellDelta) => boolean) => {
      afterSequence = sequence;
      for (const [key, item] of pending) {
        if (item.value.sequence <= sequence || !keep(item.value)) {
          budget.release([item]);
          pending.delete(key);
        }
      }
      if (pending.size > 0) Queue.offerUnsafe(ready, undefined);
    };
    const open = (sequence: number) =>
      Effect.clockWith((clock) =>
        Effect.sync(() => {
          if (closed) return;
          const now = clock.currentTimeMillisUnsafe();
          continueFrom(sequence, (delta) => admission.admit(delta, now));
          // A reset the snapshot covers (everything dropped is at or below its sequence) needs
          // no resync; a later one does, and the stream starts with it.
          if (state.resetThrough !== undefined && state.resetThrough <= sequence) {
            setReset(undefined);
          }
          if (state.resetThrough !== undefined) Queue.offerUnsafe(ready, undefined);
        }),
      );
    type Delivered = RetainedLiveItem<VersionedShellDelta | ShellResync>;
    const drain: Effect.Effect<
      Arr.NonEmptyArray<Delivered>,
      ShellDeltaHubError | LiveStreamBufferError | Cause.Done
    > = Queue.take(ready).pipe(
      Effect.flatMap(() =>
        Effect.suspend(() => {
          if (state.resetThrough !== undefined) {
            return resyncBase(state.resetThrough).pipe(
              Effect.flatMap(({ read, base }) =>
                Effect.suspend(() => {
                  // A started stream resets only on deltas past its position, and the base is
                  // read after they were committed, so it is never behind the position. A store
                  // that breaks this must not roll the subscriber back: its stream ends instead.
                  if (afterSequence !== undefined && base.snapshotSequence < afterSequence) {
                    return Effect.fail(
                      new ShellDeltaHubError({
                        message: "The shell resync base is behind the subscriber",
                        cause: { snapshotSequence: base.snapshotSequence, afterSequence },
                      }),
                    );
                  }
                  // A reset the read does not cover keeps the flag: the next pull resyncs again.
                  if (
                    state.resetThrough !== undefined &&
                    state.resetThrough <= read.coversThrough
                  ) {
                    setReset(undefined);
                  }
                  // The client shows the base now, not the shells sent before: a delta equal to
                  // one of those is still news to it (the base may hold a state never projected).
                  admission.reset();
                  continueFrom(base.snapshotSequence, () => true);
                  const resync: ShellResync = { kind: "resync", base };
                  return Effect.succeed([
                    { value: resync, serializedBytes: 0 },
                  ] as Arr.NonEmptyArray<Delivered>);
                }),
              ),
            );
          }
          // A close can empty the map before the token is taken; the next take then ends or fails.
          if (pending.size === 0) return drain;
          const items = Array.from(pending.values()).sort(
            (left, right) => left.value.sequence - right.value.sequence,
          );
          pending.clear();
          // Moving to delivery preserves the budget charge until ACK.
          return Effect.succeed(items as Arr.NonEmptyArray<Delivered>);
        }),
      ),
    );
    const deliver = budget
      .deliver(Stream.fromPull(Effect.succeed(drain)))
      .pipe(Stream.map((value): ShellSubscriptionItem => ("item" in value ? value.item : value)));
    return {
      stream: (sequence) => Stream.unwrap(open(sequence).pipe(Effect.as(deliver))),
    };
  });

  const inspect: ShellDeltaHubShape["inspect"] = () => ({
    subscribers: subscribers.size,
    baseRetained: latestBase !== undefined,
  });

  return ShellDeltaHub.of({ inspect, subscribe });
});

export const layer: Layer.Layer<
  ShellDeltaHub,
  never,
  | OrchestrationEventStore.OrchestrationEventStore
  | ProjectService.ProjectService
  | ProjectStore.ProjectStoreV2
  | SqlClient.SqlClient
  | ThreadManagementService.ThreadManagementService
> = Layer.effect(ShellDeltaHub, make);

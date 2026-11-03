import type {
  ApplicationStoredEvent,
  OrchestrationProjectShell,
  ProjectId,
} from "@t3tools/contracts";
import type * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as ProjectEnrichmentService from "../project/ProjectEnrichmentService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import {
  makeLiveStreamBudget,
  type LiveStreamBufferError,
  type RetainedLiveItem,
} from "./LiveStreamBudget.ts";
import {
  makeShellDeltaAdmission,
  makeThreadShellVersions,
  projectShellDeltas,
  shellDeltaKey,
  shellDeltaAggregate,
  shouldPublishShellApplicationEvent,
  toShellApplicationEvent,
  type ShellApplicationEvent,
  type ShellDelta,
  type VersionedShellDelta,
} from "./ShellStream.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

export class ShellDeltaHubError extends Schema.TaggedError<ShellDeltaHubError>()(
  "ShellDeltaHubError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface ShellDeltaSubscription {
  /**
   * The deltas buffered since the subscription was taken, without those at or below
   * `afterSequence` (the snapshot or replay the stream continues covers them). Take it once.
   */
  readonly stream: (
    afterSequence: number,
    /** The read ordinal a resume replay sent per aggregate (`shellDeltaAggregate`); deltas read earlier are dropped. */
    replayReads?: ReadonlyMap<string, number>,
  ) => Stream.Stream<ShellDelta, ShellDeltaHubError | LiveStreamBufferError>;
}

export interface SharedProjectShellRead {
  readonly project: Option.Option<OrchestrationProjectShell>;
  /** Creation order of the shared read; reads of one project run in this order. */
  readonly readOrdinal: number;
}

export interface ShellDeltaHubShape {
  /**
   * A project's shell as of event `sequence` or later, like `getThreadShellForEvent`: shared with
   * callers the project's latest read covers, one read per project at a time, with its ordinal.
   * The store row is what is shared; the repository identity is added per caller from the
   * enrichment cache, because it changes without a project event.
   */
  readonly readProjectShellForEvent: (input: {
    readonly projectId: ProjectId;
    readonly sequence: number;
  }) => Effect.Effect<SharedProjectShellRead, ProjectService.ProjectOperationError>;
  /**
   * Take the subscription before reading the snapshot it continues: every committed event the
   * hub sees from then on reaches this subscriber as a projected item. Deltas are buffered from
   * now on, the latest per aggregate, under the live-stream budget; on overflow or a hub failure
   * the subscription ends on its own, before the subscriber's scope closes.
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

interface SharedProjectRead {
  sequence: number;
  started: boolean;
  /** Infinity while the read is in flight; a settled read expires after the shared-read TTL. */
  expiresAt: number;
  readonly readOrdinal: number;
  readonly result: Deferred.Deferred<
    Option.Option<OrchestrationProjectShell>,
    ProjectService.ProjectOperationError
  >;
}

interface PendingShellEvent {
  readonly stored: ShellApplicationEvent;
  /** Every coalesced event is a transcript update, which focused watchers get elsewhere. */
  readonly transcriptOnly: boolean;
}

const isTranscriptEvent = (stored: ApplicationStoredEvent) =>
  !("aggregateKind" in stored) &&
  (stored.event.type === "node.updated" ||
    stored.event.type === "turn-item.updated" ||
    stored.event.type === "provider-turn.updated" ||
    stored.event.type === "message.updated");

const make = Effect.gen(function* () {
  const applicationEvents = yield* OrchestrationEventStore.OrchestrationEventStore;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const projectEnrichment = yield* ProjectEnrichmentService.ProjectEnrichmentService;
  const layerScope = yield* Effect.scope;
  const lifecycle = yield* Semaphore.make(1);
  let current: HubInstance | undefined;

  // Project shells read like thread shells do (P1-021): a caller whose sequence the project's
  // latest read covers shares it, a newer sequence starts a read after it. One entry per project.
  const projectReads = new Map<ProjectId, SharedProjectRead>();
  let nextProjectReadOrdinal = 0;
  const enrich = (
    row: Option.Option<OrchestrationProjectShell>,
  ): Effect.Effect<Option.Option<OrchestrationProjectShell>> =>
    Option.isNone(row)
      ? Effect.succeedNone
      : Effect.map(projectEnrichment.getAvailable(row.value.workspaceRoot), (enrichment) =>
          Option.some({ ...row.value, repositoryIdentity: enrichment.repositoryIdentity }),
        );
  const withOrdinal = (read: SharedProjectRead) =>
    Deferred.await(read.result).pipe(
      Effect.flatMap(enrich),
      Effect.map((project) => ({ project, readOrdinal: read.readOrdinal })),
    );
  // Settled reads move to the end when they settle, so they expire in map order; a pending read
  // stays, so the project's next read still waits for it. Same limits as the thread memo.
  const trimProjectReads = (now: number) => {
    for (const [projectId, read] of projectReads) {
      if (read.expiresAt === Number.POSITIVE_INFINITY) continue;
      if (
        projectReads.size <= ThreadManagementService.SHARED_SHELL_READ_CAPACITY &&
        now < read.expiresAt
      ) {
        return;
      }
      projectReads.delete(projectId);
    }
  };
  const readProjectShellForEvent: ShellDeltaHubShape["readProjectShellForEvent"] = (input) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.clockWith((clock) => {
        const now = clock.currentTimeMillisUnsafe();
        const current = projectReads.get(input.projectId);
        if (
          current !== undefined &&
          now < current.expiresAt &&
          (current.sequence >= input.sequence || !current.started)
        ) {
          current.sequence = Math.max(current.sequence, input.sequence);
          return restore(withOrdinal(current));
        }
        const read: SharedProjectRead = {
          sequence: Math.max(input.sequence, current?.sequence ?? 0),
          started: false,
          expiresAt: Number.POSITIVE_INFINITY,
          readOrdinal: ++nextProjectReadOrdinal,
          result: Deferred.makeUnsafe(),
        };
        projectReads.delete(input.projectId);
        projectReads.set(input.projectId, read);
        trimProjectReads(now);
        const previous =
          current !== undefined && current.expiresAt === Number.POSITIVE_INFINITY
            ? Effect.exit(Deferred.await(current.result))
            : Effect.void;
        return previous.pipe(
          Effect.andThen(
            Effect.sync(() => {
              read.started = true;
            }),
          ),
          Effect.andThen(
            projects.getShell(input.projectId).pipe(
              Effect.mapError(
                (cause) =>
                  new ProjectService.ProjectOperationError({
                    operation: "read-project",
                    projectId: input.projectId,
                    cause,
                  }),
              ),
            ),
          ),
          Effect.forkIn(layerScope),
          Effect.flatMap((fiber) => {
            // A failed read is forgotten, so a retry at the same sequence reads again.
            fiber.addObserver((exit) => {
              const isCurrent = projectReads.get(input.projectId) === read;
              if (isCurrent) projectReads.delete(input.projectId);
              if (Exit.isSuccess(exit)) {
                const settledAt = clock.currentTimeMillisUnsafe();
                read.expiresAt = settledAt + ThreadManagementService.SHARED_SHELL_READ_TTL_MS;
                if (isCurrent) projectReads.set(input.projectId, read);
                trimProjectReads(settledAt);
              }
              Deferred.doneUnsafe(read.result, exit);
            });
            return restore(withOrdinal(read));
          }),
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
    const pending = new Map<string, PendingShellEvent>();
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
          const transcriptOnly = isTranscriptEvent(stored) && (previous?.transcriptOnly ?? true);
          if (previous === undefined || previous.stored.sequence < stored.sequence) {
            pending.set(key, { stored: toShellApplicationEvent(stored), transcriptOnly });
          } else {
            pending.set(key, { stored: previous.stored, transcriptOnly });
          }
        }
        // Only the empty-to-nonempty transition wakes the projection; one token is ever queued.
        if (wasEmpty && pending.size > 0) yield* Queue.offer(ready, undefined);
      }
    });
    const project = Effect.fn("ShellDeltaHub.projectShellItems")(function* () {
      const batch = Array.from(pending.values());
      pending.clear();
      const transcriptOnly = new Set(
        batch.flatMap((entry) =>
          entry.transcriptOnly && !("aggregateKind" in entry.stored)
            ? [entry.stored.event.threadId]
            : [],
        ),
      );
      const projected = yield* projectShellDeltas(
        batch.map((entry) => entry.stored),
        {
          threadShell: threadManagement.getThreadShellReadForEvent,
          projectShell: readProjectShellForEvent,
        },
      );
      const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
      const deltas: Array<VersionedShellDelta> = [];
      for (const { item, readOrdinal } of projected) {
        // A subagent's transcript updates reach its focused watchers through the thread stream;
        // the shell, read because the subagent was unknown at hub start, shows the lineage now.
        if (
          item.kind === "thread.updated" &&
          transcriptOnly.has(item.thread.id) &&
          item.thread.lineage.relationshipToParent === "subagent"
        ) {
          subagentThreadIds.add(item.thread.id);
          continue;
        }
        deltas.push({
          sequence: item.sequence,
          item,
          version: versions.stamp(item, now),
          readOrdinal,
        });
      }
      if (deltas.length > 0) yield* PubSub.publish(input.output, deltas);
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
          return Effect.logWarning("shell delta hub stopped; every shell stream ends", {
            cause,
          }).pipe(Effect.andThen(Deferred.fail(failed, instance.failure)));
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
    const mutex = yield* Semaphore.make(1);
    let afterSequence: number | undefined;
    let replayReads: ReadonlyMap<string, number> | undefined;
    let closed = false;
    // A delta read before the replay's read of the same aggregate carries older-or-equal content.
    const behindReplay = (delta: VersionedShellDelta) => {
      const replayRead = replayReads?.get(shellDeltaAggregate(delta.item));
      return replayRead !== undefined && delta.readOrdinal <= replayRead;
    };
    const close = (error?: LiveStreamBufferError) =>
      mutex.withPermit(
        Effect.gen(function* () {
          if (closed) return;
          closed = true;
          budget.release(pending.values());
          pending.clear();
          if (error) yield* Queue.fail(ready, error);
          yield* Queue.shutdown(ready);
        }),
      );
    yield* Effect.addFinalizer(() => close());
    yield* budget.failed.pipe(
      Effect.catchTags({ LiveStreamBufferError: close }),
      Effect.forkScoped,
    );
    const retain = Effect.fnUntraced(function* (delta: VersionedShellDelta) {
      const key = shellDeltaKey(delta.item);
      const previous = pending.get(key);
      if (previous !== undefined && previous.value.sequence >= delta.sequence) return;
      const wasEmpty = pending.size === 0;
      const [item] = yield* budget.replace(previous === undefined ? [] : [previous], [delta]);
      pending.set(key, item!);
      // Only the empty-to-nonempty transition of a started stream needs a wakeup.
      if (wasEmpty && afterSequence !== undefined) yield* Queue.offer(ready, undefined);
    });
    yield* Stream.fromSubscription(subscription).pipe(
      Stream.flatMap(Stream.fromIterable),
      Stream.merge(Stream.fromEffect(Deferred.await(instance.failed))),
      Stream.ensuring(Scope.close(subscriptionScope, Exit.void)),
      Stream.runForEach((delta) =>
        mutex.withPermit(
          Effect.gen(function* () {
            yield* budget.check;
            if (closed) return;
            if (afterSequence !== undefined) {
              if (delta.sequence <= afterSequence || behindReplay(delta)) return;
              if (!admission.admit(delta, yield* Clock.currentTimeMillis)) return;
            }
            yield* retain(delta);
          }).pipe(Effect.uninterruptible),
        ),
      ),
      Effect.raceFirst(budget.failed),
      Effect.exit,
      Effect.flatMap((exit) =>
        Exit.isFailure(exit) ? Queue.failCause(ready, exit.cause) : Queue.end(ready),
      ),
      Effect.forkScoped({ startImmediately: true }),
    );
    const open = (sequence: number, reads?: ReadonlyMap<string, number>) =>
      mutex.withPermit(
        Effect.gen(function* () {
          if (closed) return;
          afterSequence = sequence;
          replayReads = reads;
          const now = yield* Clock.currentTimeMillis;
          for (const [key, item] of pending) {
            if (
              item.value.sequence <= sequence ||
              behindReplay(item.value) ||
              !admission.admit(item.value, now)
            ) {
              budget.release([item]);
              pending.delete(key);
            }
          }
          if (pending.size > 0) yield* Queue.offer(ready, undefined);
        }),
      );
    const deliver = budget.deliver(
      Stream.fromPull(
        Effect.succeed(
          Queue.take(ready).pipe(
            Effect.andThen(
              mutex.withPermit(
                Effect.sync(() => {
                  const items = Array.from(pending.values()).sort(
                    (left, right) => left.value.sequence - right.value.sequence,
                  );
                  pending.clear();
                  // Moving to delivery preserves the budget charge until ACK.
                  return items as Arr.NonEmptyArray<RetainedLiveItem<VersionedShellDelta>>;
                }),
              ),
            ),
          ),
        ),
      ),
    );
    return {
      stream: (sequence, reads) =>
        Stream.unwrap(
          open(sequence, reads).pipe(Effect.as(deliver.pipe(Stream.map((delta) => delta.item)))),
        ),
    };
  });

  return ShellDeltaHub.of({ readProjectShellForEvent, subscribe });
});

export const layer: Layer.Layer<
  ShellDeltaHub,
  never,
  | OrchestrationEventStore.OrchestrationEventStore
  | ProjectEnrichmentService.ProjectEnrichmentService
  | ProjectStore.ProjectStoreV2
  | ThreadManagementService.ThreadManagementService
> = Layer.effect(ShellDeltaHub, make);

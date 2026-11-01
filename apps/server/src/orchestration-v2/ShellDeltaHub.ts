import type { ApplicationStoredEvent, ThreadId } from "@t3tools/contracts";
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
import * as Stream from "effect/Stream";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as ProjectService from "../project/ProjectService.ts";
import {
  makeThreadShellVersions,
  projectShellDeltas,
  shouldPublishShellApplicationEvent,
  skipUnchangedShellDeltas,
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

export interface ShellDeltaSubscription {
  /**
   * The deltas published after the subscription was taken, without those at or below
   * `afterSequence` (the snapshot or replay the stream continues covers them).
   */
  readonly stream: (afterSequence: number) => Stream.Stream<ShellDelta, ShellDeltaHubError>;
}

export interface ShellDeltaHubShape {
  /**
   * Take the subscription before reading the snapshot it continues: every committed event the
   * hub sees from then on reaches this subscriber as a projected item.
   */
  readonly subscribe: Effect.Effect<ShellDeltaSubscription, never, Scope.Scope>;
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
  const layerScope = yield* Effect.scope;
  const lifecycle = yield* Semaphore.make(1);
  let current: HubInstance | undefined;

  // One committed-events consumer for every shell subscriber. Events coalesce per aggregate
  // while a batch is read, so memory is bounded by the aggregates that changed, not by events.
  const run = Effect.fn("ShellDeltaHub.run")(function* (input: {
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
      yield* PubSub.publish(
        input.output,
        items.map((item) => ({ item, version: versions.stamp(item, now) })),
      );
    });
    yield* drain.pipe(Effect.forkScoped);
    while (true) {
      yield* Queue.take(ready);
      yield* Effect.sleep(SHELL_DELTA_BATCH_WINDOW);
      yield* project();
    }
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
      return Scope.close(instance.scope, Exit.void);
    });

  const subscribe: ShellDeltaHubShape["subscribe"] = Effect.gen(function* () {
    const { instance, subscription } = yield* lifecycle.withPermit(
      Effect.gen(function* () {
        if (current !== undefined && current.failure !== undefined) yield* stop(current);
        const instance = current ?? (yield* start);
        current = instance;
        instance.subscribers += 1;
        return { instance, subscription: yield* PubSub.subscribe(instance.output) };
      }),
    );
    yield* Effect.addFinalizer(() =>
      lifecycle.withPermit(
        Effect.suspend(() => {
          instance.subscribers -= 1;
          return instance.subscribers === 0 && current === instance
            ? stop(instance)
            : Effect.void;
        }),
      ),
    );
    return {
      stream: (afterSequence) =>
        Stream.fromSubscription(subscription).pipe(
          Stream.map((batch) => batch.filter((delta) => delta.item.sequence > afterSequence)),
          skipUnchangedShellDeltas,
          Stream.merge(Stream.fromEffect(Deferred.await(instance.failed))),
        ),
    };
  });

  return ShellDeltaHub.of({ subscribe });
});

export const layer: Layer.Layer<
  ShellDeltaHub,
  never,
  | OrchestrationEventStore.OrchestrationEventStore
  | ProjectService.ProjectService
  | ThreadManagementService.ThreadManagementService
> = Layer.effect(ShellDeltaHub, make);

import { it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { describe, expect } from "vite-plus/test";

import {
  bufferLatestLiveStream,
  LiveStreamBufferError,
  makeLiveStreamBudget,
  replayAndBufferProjectedLiveEvents,
  resyncOnLiveOverflow,
  type RetainedLiveItem,
} from "./LiveStreamBudget.ts";

describe("LiveStreamBudget", () => {
  it.effect("closes the source without releasing a batch still waiting for an ACK", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const budget = yield* makeLiveStreamBudget({ maxItems: 3 });
        const queue = yield* Queue.unbounded<RetainedLiveItem<{ text: string }>>();
        const sourceClosed = yield* Deferred.make<void>();
        const first = yield* budget.retain({ text: "first" });
        const second = yield* budget.retain({ text: "second" });
        const third = yield* budget.retain({ text: "third" });
        yield* Queue.offerAll(queue, [first, second, third]);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const pull = yield* Stream.toPull(
              budget.deliver(
                Stream.fromQueue(queue).pipe(
                  Stream.rechunk(1),
                  Stream.ensuring(Deferred.succeed(sourceClosed, undefined)),
                ),
              ),
            );
            expect(yield* pull).toEqual([{ text: "first" }]);
            expect(yield* Queue.size(queue)).toBe(0);
            expect((yield* budget.usage).retainedItems).toBe(3);
            const overflow = yield* budget.retain({ text: "fourth" }).pipe(Effect.result);
            expect(overflow._tag).toBe("Failure");
            yield* Deferred.await(sourceClosed);
            yield* budget.closed;
            expect((yield* budget.usage).retainedItems).toBe(1);
          }),
        );
        expect(yield* budget.usage).toEqual({ retainedItems: 0, retainedSerializedBytes: 0 });
      }),
    ),
  );
});

type Event = { readonly sequence: number; readonly text: string; readonly threadId?: string };

describe("bufferLatestLiveStream", () => {
  it.effect.each(["items", "bytes"] as const)(
    "counts an unacknowledged state against the %s limit when its replacement arrives",
    (limit) =>
      Effect.scoped(
        Effect.gen(function* () {
          const releaseUpdate = yield* Deferred.make<void>();
          const closed = yield* Deferred.make<void>();
          const first = { sequence: 1, text: "é".repeat(20) };
          const next = { ...first, sequence: 2 };
          const pull = yield* Stream.toPull(
            bufferLatestLiveStream(
              Stream.concat(
                Stream.succeed(first),
                Stream.fromEffect(Deferred.await(releaseUpdate)).pipe(Stream.map(() => next)),
              ).pipe(Stream.ensuring(Deferred.succeed(closed, undefined))),
              () => "same-thread",
              limit === "items"
                ? { maxItems: 1 }
                : { maxSerializedBytes: Buffer.byteLength(JSON.stringify(first)) },
            ),
          );
          expect(yield* pull).toEqual([first]);
          yield* Deferred.succeed(releaseUpdate, undefined);
          yield* Deferred.await(closed);
          const result = yield* pull.pipe(Effect.result);
          expect(result._tag).toBe("Failure");
          if (result._tag === "Failure") expect(result.failure._tag).toBe("LiveStreamBufferError");
        }),
      ),
  );
});

describe("replayAndBufferProjectedLiveEvents", () => {
  it.effect.each(["high-water", "replay"] as const)(
    "unsubscribes and cancels a blocked %s read on live overflow",
    (phase) =>
      Effect.scoped(
        Effect.gen(function* () {
          const pubsub = yield* PubSub.unbounded<Event>();
          const subscribed = yield* Deferred.make<PubSub.Subscription<Event>>();
          const readStarted = yield* Deferred.make<void>();
          const readClosed = yield* Deferred.make<void>();
          const blockedRead = Deferred.succeed(readStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(readClosed, undefined)),
          );
          const pull = yield* Stream.toPull(
            replayAndBufferProjectedLiveEvents(
              {
                project: (event: Event) => event,
                subscribe: PubSub.subscribe(pubsub).pipe(
                  Effect.tap((subscription) => Deferred.succeed(subscribed, subscription)),
                ),
                latestSequence:
                  phase === "high-water" ? blockedRead.pipe(Effect.as(0)) : Effect.succeed(0),
                replay: () => (phase === "replay" ? Stream.fromEffect(blockedRead) : Stream.empty),
              },
              { maxItems: 2 },
            ),
          );
          const reader = yield* pull.pipe(Effect.result, Effect.forkScoped);
          yield* Deferred.await(readStarted);
          yield* PubSub.publishAll(pubsub, [
            { sequence: 1, text: "one" },
            { sequence: 2, text: "two" },
            { sequence: 3, text: "overflow" },
          ]);
          yield* (yield* Deferred.await(subscribed)).shutdownHook.await;
          yield* Deferred.await(readClosed);
          const result = yield* Fiber.join(reader);
          expect(result._tag).toBe("Failure");
          if (result._tag === "Failure") expect(result.failure._tag).toBe("LiveStreamBufferError");
          expect(yield* PubSub.size(pubsub)).toBe(0);
          yield* PubSub.publish(pubsub, { sequence: 4, text: "after overflow" });
          expect(yield* PubSub.size(pubsub)).toBe(0);
        }),
      ),
  );

  it.effect("wakes a keyed consumer for every burst and delivers each key's newest event", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const input = yield* Queue.unbounded<Event, Cause.Done>();
        const pull = yield* Stream.toPull(
          bufferLatestLiveStream(Stream.fromQueue(input), (event) => event.threadId ?? "", {
            maxItems: 8,
          }),
        );
        // The first pull waits for the first burst.
        const waiting = yield* pull.pipe(Effect.forkScoped);
        // Bursts of varying shape, each pulled to its end before the next: a lost wake-up would
        // leave a pull waiting forever, a lost event would leave a key behind.
        let sequence = 0;
        let lastSeen = 0;
        for (let round = 1; round <= 60; round++) {
          const burst = Array.from({ length: 1 + (round % 7) }, (_, index) => ({
            sequence: ++sequence,
            text: `round ${round}`,
            threadId: `thread-${(round + index) % 4}`,
          }));
          yield* Queue.offerAll(input, burst);
          const newest = new Map(burst.map((event) => [event.threadId, event.sequence]));
          const seen = new Map<string | undefined, number>();
          while (seen.size < newest.size || [...newest].some(([key, s]) => seen.get(key)! < s)) {
            for (const event of yield* round === 1 && seen.size === 0
              ? Fiber.join(waiting)
              : pull) {
              expect(event.sequence).toBeGreaterThan(lastSeen);
              lastSeen = event.sequence;
              seen.set(event.threadId, event.sequence);
            }
          }
          expect(seen).toEqual(newest);
        }
      }),
    ),
  );

  it.effect("lets a fast reader replay a database page larger than both buffer limits", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const pubsub = yield* PubSub.unbounded<Event>();
        const events = Array.from({ length: 20 }, (_, index) => ({
          sequence: index + 1,
          text: "historical event",
        }));
        const replayed = yield* replayAndBufferProjectedLiveEvents(
          {
            project: (event: Event) => event,
            subscribe: PubSub.subscribe(pubsub),
            latestSequence: Effect.succeed(20),
            replay: () => Stream.fromIterable(events),
          },
          { maxItems: 2, maxSerializedBytes: 100 },
        ).pipe(Stream.take(20), Stream.runCollect);
        expect(replayed).toEqual(events);
        expect(yield* PubSub.size(pubsub)).toBe(0);
      }),
    ),
  );

  it.effect(
    "filters unrelated events before buffering and deduplicates the replay/live overlap",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const pubsub = yield* PubSub.unbounded<Event>();
          const highWaterStarted = yield* Deferred.make<void>();
          const highWater = yield* Deferred.make<number>();
          const first = { sequence: 1, text: "first", threadId: "selected" };
          const unrelatedReplay = { sequence: 0, text: "unrelated replay", threadId: "other" };
          const overlap = { sequence: 2, text: "overlap", threadId: "selected" };
          const last = { sequence: 10, text: "last", threadId: "selected" };
          const reader = yield* replayAndBufferProjectedLiveEvents(
            {
              project: (event: Event) => event,
              subscribe: PubSub.subscribe(pubsub),
              latestSequence: Deferred.succeed(highWaterStarted, undefined).pipe(
                Effect.andThen(Deferred.await(highWater)),
              ),
              replay: () => Stream.make(unrelatedReplay, first, overlap),
              filter: (event) => event.threadId === "selected",
            },
            { maxItems: 4 },
          ).pipe(Stream.take(3), Stream.runCollect, Effect.forkScoped);
          yield* Deferred.await(highWaterStarted);
          yield* PubSub.publishAll(pubsub, [
            overlap,
            ...Array.from({ length: 6 }, (_, index) => ({
              sequence: index + 3,
              text: "unrelated",
              threadId: "other",
            })),
            last,
          ]);
          yield* Deferred.succeed(highWater, 2);
          expect(yield* Fiber.join(reader)).toEqual([first, overlap, last]);
          expect(yield* PubSub.size(pubsub)).toBe(0);
        }),
      ),
  );
});

describe("resyncOnLiveOverflow", () => {
  it.effect("follows each overflow with the next segment, backing off within a streak", () =>
    Effect.gen(function* () {
      const overflow = new LiveStreamBufferError({ message: "full" });
      const collected: Array<string> = [];
      const built: Array<Deferred.Deferred<void>> = [];
      let builds = 0;
      // Every segment emits its label; the first three then overflow, the fourth ends normally.
      const segment = (label: string, overflows: boolean) =>
        Stream.make(label).pipe(
          overflows ? Stream.concat(Stream.fail(overflow)) : Stream.concat(Stream.empty),
        );
      const next = Effect.sync(() => {
        builds += 1;
        Deferred.doneUnsafe(built[builds - 1]!, Effect.void);
        return segment(`resync-${builds}`, builds < 3);
      });
      for (let index = 0; index < 3; index++) built.push(yield* Deferred.make<void>());
      const done = yield* resyncOnLiveOverflow(segment("first", true), () => next).pipe(
        Stream.runForEach((label) => Effect.sync(() => void collected.push(label))),
        Effect.forkScoped,
      );
      // The first resync of a streak is immediate.
      yield* Deferred.await(built[0]!);
      expect(collected).toEqual(["first"]);
      // The second waits 250 ms, the third 500 ms.
      yield* TestClock.adjust("249 millis");
      expect(builds).toBe(1);
      yield* TestClock.adjust("1 millis");
      yield* Deferred.await(built[1]!);
      yield* TestClock.adjust("499 millis");
      expect(builds).toBe(2);
      yield* TestClock.adjust("1 millis");
      yield* Deferred.await(built[2]!);
      // A segment that ends without overflowing ends the stream.
      yield* Fiber.join(done);
      expect(collected).toEqual(["first", "resync-1", "resync-2", "resync-3"]);
      expect(builds).toBe(3);
    }).pipe(Effect.scoped),
  );

  it.effect("passes other failures through", () =>
    Effect.gen(function* () {
      const result = yield* resyncOnLiveOverflow(Stream.fail("boom" as const), () =>
        Effect.succeed(Stream.empty),
      ).pipe(Stream.runDrain, Effect.result);
      expect(result._tag).toBe("Failure");
    }),
  );
});

import { CurrentWaitObserver } from "@t3tools/shared/WaitObserver";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";

interface Queued<A> {
  readonly item: A;
  /** Set only while a wait observer is present. */
  readonly queuedNs: bigint;
  readonly holder: "(key busy)" | "(pool full)" | undefined;
}

/**
 * Handles items in arrival order per key, with at most `concurrency` keys at once. A key gives up
 * its worker after each item when others wait, so a burst or a slow item cannot hold them up.
 * With a `name`, the wait observer hears of each item's wait and the pending keys.
 */
export const makeKeyedReactor = Effect.fn("KeyedReactor.make")(function* <
  K,
  A,
  E = never,
>(options: {
  readonly concurrency: number;
  readonly handle: (item: A) => Effect.Effect<void, E>;
  readonly name?: string;
}) {
  // A key is in `pending` while it has an item waiting or running. Each such key is either held
  // by exactly one worker or listed once in `ready`.
  const pending = new Map<K, Array<Queued<A>>>();
  const ready: Array<K> = [];
  const workers = yield* FiberSet.make<void>();
  let running = 0;
  let idle: Array<Deferred.Deferred<void>> = [];
  const site = options.name;
  const observer = site === undefined ? undefined : yield* CurrentWaitObserver;
  const clock = yield* Clock.clockWith(Effect.succeed);
  if (observer !== undefined && site !== undefined) {
    const unregister = observer.register(`${site}.pendingKeys`, "gauge", () => pending.size);
    yield* Effect.addFinalizer(() => Effect.sync(unregister));
  }
  const work = (key: K): Effect.Effect<void> =>
    Effect.suspend(() => {
      const items = pending.get(key)!;
      const queued = items.shift()!;
      if (observer !== undefined && site !== undefined) {
        observer.wait(
          site,
          Number(clock.monotonicTimeNanosUnsafe() - queued.queuedNs),
          queued.holder,
        );
      }
      return options.handle(queued.item).pipe(
        Effect.catchCause((cause) => Effect.logWarning("A keyed reaction failed", { cause })),
        Effect.andThen(
          Effect.suspend(() => {
            if (items.length > 0) ready.push(key);
            else pending.delete(key);
            const next = ready.shift();
            if (next !== undefined) return work(next);
            running -= 1;
            if (pending.size > 0) return Effect.void;
            const waiters = idle;
            idle = [];
            return Effect.forEach(waiters, (waiter) => Deferred.succeed(waiter, undefined));
          }),
        ),
      );
    });
  return {
    offer: (key: K, item: A) =>
      Effect.suspend(() => {
        const items = pending.get(key);
        const full = running >= options.concurrency;
        const queuedNs = observer === undefined ? 0n : clock.monotonicTimeNanosUnsafe();
        if (observer !== undefined && site !== undefined) observer.depth(site, pending.size);
        if (items !== undefined) {
          items.push({ item, queuedNs, holder: "(key busy)" });
          return Effect.void;
        }
        pending.set(key, [{ item, queuedNs, holder: full ? "(pool full)" : undefined }]);
        if (full) {
          ready.push(key);
          return Effect.void;
        }
        running += 1;
        return FiberSet.run(workers, work(key)).pipe(Effect.asVoid);
      }),
    pendingKeys: Effect.sync(() => pending.size),
    /** Waits until every offered item has been handled. */
    whenIdle: Effect.suspend(() => {
      if (pending.size === 0) return Effect.void;
      const waiter = Deferred.makeUnsafe<void>();
      idle.push(waiter);
      return Deferred.await(waiter);
    }),
  };
});

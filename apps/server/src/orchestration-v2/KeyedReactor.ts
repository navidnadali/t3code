import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";

/**
 * Handles items in arrival order per key, with at most `concurrency` keys at once. A key gives up
 * its worker after each item when others wait, so a burst or a slow item cannot hold them up.
 */
export const makeKeyedReactor = Effect.fn("KeyedReactor.make")(function* <
  K,
  A,
  E = never,
>(options: { readonly concurrency: number; readonly handle: (item: A) => Effect.Effect<void, E> }) {
  // A key is in `pending` while it has an item waiting or running. Each such key is either held
  // by exactly one worker or listed once in `ready`.
  const pending = new Map<K, Array<A>>();
  const ready: Array<K> = [];
  const workers = yield* FiberSet.make<void>();
  let running = 0;
  let idle: Array<Deferred.Deferred<void>> = [];
  const work = (key: K): Effect.Effect<void> =>
    Effect.suspend(() => {
      const items = pending.get(key)!;
      return options.handle(items.shift()!).pipe(
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
        if (items !== undefined) {
          items.push(item);
          return Effect.void;
        }
        pending.set(key, [item]);
        if (running >= options.concurrency) {
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

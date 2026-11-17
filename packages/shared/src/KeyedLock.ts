import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FiberSet from "effect/FiberSet";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import { CurrentWaitObserver, operationName } from "./WaitObserver.ts";

export interface KeyedLock<Key> {
  readonly withLock: <A, E, R>(key: Key, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  readonly activeKeys: Effect.Effect<ReadonlyArray<Key>>;
}

export interface KeyedLockOptions {
  /** Names the lock to the wait observer, which then hears of every acquisition. */
  readonly site?: string;
  /** Also passes the key, so the observer can rank keys by wait and hold. */
  readonly rankKeys?: boolean;
}

interface LockEntry {
  readonly semaphore: Semaphore.Semaphore;
  readonly users: number;
  /** The operation holding the key, while one does; kept across the entry's copies. */
  readonly holder: { name: string | undefined; owner: object | undefined };
}

/**
 * Not reentrant: taking a key while already holding it deadlocks.
 */
export const make = Effect.fn("KeyedLock.make")(function* <Key>(
  options: KeyedLockOptions = {},
): Effect.fn.Return<KeyedLock<Key>> {
  const locks = yield* Ref.make<ReadonlyMap<Key, LockEntry>>(new Map());
  const observer = options.site === undefined ? undefined : yield* CurrentWaitObserver;

  const acquire = (key: Key) =>
    Effect.flatMap(Semaphore.make(1), (candidate) =>
      Ref.modify(locks, (current) => {
        const existing = current.get(key);
        const entry: LockEntry = {
          semaphore: existing?.semaphore ?? candidate,
          users: (existing?.users ?? 0) + 1,
          holder: existing?.holder ?? { name: undefined, owner: undefined },
        };
        const next = new Map(current);
        next.set(key, entry);
        return [entry, next] as const;
      }),
    );

  const release = (key: Key) =>
    Ref.update(locks, (current) => {
      const existing = current.get(key);
      if (existing === undefined) return current;
      const next = new Map(current);
      if (existing.users === 1) {
        next.delete(key);
      } else {
        next.set(key, { ...existing, users: existing.users - 1 });
      }
      return next;
    });

  const site = options.site;
  const clock = yield* Clock.clockWith(Effect.succeed);
  return {
    withLock: (key, effect) => {
      if (observer === undefined || site === undefined) {
        return Effect.acquireUseRelease(
          acquire(key),
          (entry) => entry.semaphore.withPermit(effect),
          () => release(key),
        );
      }
      const ranked = options.rankKeys === true ? key : undefined;
      return Effect.suspend(() => {
        // This run's acquisition, once it holds the key; the release reads it. Per run, as the
        // returned effect may run more than once.
        const mine = { acquiredNs: -1n };
        return Effect.acquireUseRelease(
          acquire(key),
          (entry) => {
            const waitStart = clock.monotonicTimeNanosUnsafe();
            const holder = entry.holder.name;
            return entry.semaphore.withPermit(
              Effect.withFiber((fiber) => {
                mine.acquiredNs = clock.monotonicTimeNanosUnsafe();
                const operation = operationName(fiber.context) ?? "(untraced)";
                // Waiters are named only behind a holder: an uncontended one waited for nothing.
                observer.wait(
                  site,
                  Number(mine.acquiredNs - waitStart),
                  holder,
                  ranked,
                  holder === undefined ? undefined : operation,
                );
                entry.holder.name = operation;
                entry.holder.owner = mine;
                return effect;
              }),
            );
          },
          (entry) => {
            if (mine.acquiredNs >= 0n) {
              // A waiter that took the key since keeps its own name.
              if (entry.holder.owner === mine) {
                entry.holder.name = undefined;
                entry.holder.owner = undefined;
              }
              if (ranked !== undefined) {
                observer.hold(
                  site,
                  ranked,
                  Number(clock.monotonicTimeNanosUnsafe() - mine.acquiredNs),
                );
              }
            }
            return release(key);
          },
        );
      });
    },
    activeKeys: Effect.map(Ref.get(locks), (current) => Array.from(current.keys())),
  } satisfies KeyedLock<Key>;
});
export interface KeyedRunner<Key> {
  /** Forks `effect` to run once every effect passed earlier for `key` has finished. */
  readonly run: <R>(
    key: Key,
    effect: Effect.Effect<void, never, R>,
  ) => Effect.Effect<void, never, R>;
}

/**
 * For a stream consumer whose handler waits on a per-key lock: a busy key delays only its own
 * later effects. The forked fiber starts immediately, so it queues on the key's FIFO semaphore
 * before `run` returns; that keeps each key's effects in `run` order.
 */
export const makeRunner = Effect.fn("KeyedLock.makeRunner")(function* <Key>(): Effect.fn.Return<
  KeyedRunner<Key>,
  never,
  Scope.Scope
> {
  const lock = yield* make<Key>();
  const fibers = yield* FiberSet.make<void, never>();
  return {
    run: (key, effect) =>
      FiberSet.run(fibers, lock.withLock(key, effect), { startImmediately: true }).pipe(
        Effect.asVoid,
      ),
  } satisfies KeyedRunner<Key>;
});

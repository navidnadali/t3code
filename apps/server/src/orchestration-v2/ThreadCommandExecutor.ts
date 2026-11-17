import type { ThreadId } from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { CurrentWaitObserver, type KeySizer } from "@t3tools/shared/WaitObserver";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";

import * as IngestLag from "./IngestLag.ts";

/** The per-thread command lock and the message intake lock, as the wait observer names them. */
export const THREAD_LOCK_SITE = "ThreadCommandExecutor.lock";
export const MESSAGE_INTAKE_SITE = "Orchestrator.messageIntake";

export class ThreadCommandExecutor extends Context.Service<
  ThreadCommandExecutor,
  KeyedLock.KeyedLock<ThreadId> & {
    /**
     * One thread deletion plans and commits at a time: each decides what its deleted fork
     * sources keep for the forks still alive. Take it before any thread lock.
     */
    readonly withDeletionLock: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  }
>()("t3/orchestration-v2/ThreadCommandExecutor") {}

export const make = Effect.gen(function* () {
  const deletion = yield* Semaphore.make(1);
  return ThreadCommandExecutor.of({
    ...(yield* KeyedLock.make<ThreadId>({ site: THREAD_LOCK_SITE, rankKeys: true })),
    withDeletionLock: Effect.fn("ThreadCommandExecutor.withDeletionLock")(function* <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ) {
      return yield* deletion.withPermit(effect);
    }),
  });
});

export const layer = Layer.effect(ThreadCommandExecutor, make);

/** The decade a count falls in, by its floor: 0, 1, 10, 100 and so on. */
const sizeClass = (count: number): number => (count < 1 ? 0 : 10 ** Math.floor(Math.log10(count)));

// A few threads rank each minute and their sizes change slowly: a class ten minutes old will do.
const SIZE_MAX_AGE_MS = 10 * 60_000;
const SIZES_CACHED = 256;

/**
 * Sizes the threads that rank at the thread locks or by ingest backlog by their runs and subagent
 * rows, each a decade class, so a minute line tells a huge thread from a small one without naming
 * either.
 */
export const layerThreadSizes = Layer.effectDiscard(
  Effect.gen(function* () {
    const observer = yield* CurrentWaitObserver;
    if (observer === undefined) return;
    const sql = yield* SqlClient.SqlClient;
    const cache = new Map<
      unknown,
      { readonly atMs: number; readonly size: Record<string, number> }
    >();
    // Untraced: it runs in the span minute flush. Both counts read a covering thread_id index.
    const count = Effect.fnUntraced(function* (threadId: string) {
      const [runs] = yield* sql<{ readonly n: number }>`
        SELECT count(*) AS n FROM orchestration_v2_projection_runs WHERE thread_id = ${threadId}`;
      const [subagents] = yield* sql<{ readonly n: number }>`
        SELECT count(*) AS n FROM orchestration_v2_projection_subagents WHERE thread_id = ${threadId}`;
      return { runs: sizeClass(runs?.n ?? 0), subagents: sizeClass(subagents?.n ?? 0) };
    });
    const sizer: KeySizer = Effect.fnUntraced(function* (keys) {
      const nowMs = yield* Clock.currentTimeMillis;
      const sizes = new Map<unknown, Readonly<Record<string, number>>>();
      for (const key of keys) {
        if (typeof key !== "string") continue;
        const cached = cache.get(key);
        if (cached !== undefined && nowMs - cached.atMs < SIZE_MAX_AGE_MS) {
          sizes.set(key, cached.size);
          continue;
        }
        const size = yield* NodeSqliteClient.readOnly(count(key)).pipe(Effect.option);
        if (size._tag === "None") continue;
        cache.delete(key);
        if (cache.size >= SIZES_CACHED) cache.delete(cache.keys().next().value);
        cache.set(key, { atMs: nowMs, size: size.value });
        sizes.set(key, size.value);
      }
      return sizes;
    });
    for (const site of [THREAD_LOCK_SITE, MESSAGE_INTAKE_SITE, IngestLag.BACKLOG_SITE]) {
      const unregister = observer.sizeKeys(site, sizer);
      yield* Effect.addFinalizer(() => Effect.sync(unregister));
    }
  }),
);

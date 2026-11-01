import * as NodeWorkerThreads from "node:worker_threads";

import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberSet from "effect/FiberSet";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../config.ts";
import { embeddedProviderIoWorkerSource } from "./embeddedProviderIoWorker.ts";
import type {
  ProviderIoMainMessage,
  ProviderIoStage,
  ProviderIoWorkerMessage,
} from "./providerIoProtocol.ts";

export class ProviderIoWorkerError extends Schema.TaggedError<ProviderIoWorkerError>()(
  "ProviderIoWorkerError",
  { cause: Schema.Defect() },
) {
  override get message() {
    return "Provider I/O worker stopped.";
  }
}

export class ProviderIoStageError extends Schema.TaggedError<ProviderIoStageError>()(
  "ProviderIoStageError",
  { stage: Schema.String, cause: Schema.Defect() },
) {
  override get message() {
    return `Provider I/O stage ${this.stage} failed.`;
  }
}

export interface ProviderIoSession<Output, E> {
  /** Suspends while the session's unconsumed input is at its high-water mark. */
  readonly write: (chunk: Uint8Array) => Effect.Effect<void, E>;
  readonly end: Effect.Effect<void, E>;
  readonly output: Stream.Stream<Output, E>;
}

export interface ProviderIoOpenInput<Options, Output, E> {
  readonly stage: ProviderIoStage<Options, Output>;
  readonly options: Options;
  readonly onFailure: (error: ProviderIoWorkerError | ProviderIoStageError) => E;
  /** Stops the session's child process when its worker dies. */
  readonly kill: Effect.Effect<void>;
  readonly highWaterMark?: number;
}

export class ProviderIoWorkerPool extends Context.Service<
  ProviderIoWorkerPool,
  {
    /** None when the pool is off or unavailable; the caller then runs the stage in-thread. */
    readonly open: <Options, Output, E>(
      input: ProviderIoOpenInput<Options, Output, E>,
    ) => Effect.Effect<Option.Option<ProviderIoSession<Output, E>>, never, Scope.Scope>;
  }
>()("t3/provider/io/ProviderIoWorkerPool") {}

export const PROVIDER_IO_MAX_WORKERS = 32;
const HANDSHAKE_TIMEOUT = Duration.seconds(10);
const DEFAULT_HIGH_WATER_MARK = 1024 * 1024;

interface SessionHandle {
  readonly deliver: (outputs: ReadonlyArray<unknown>, bytes: number) => void;
  readonly finish: () => void;
  readonly failStage: (cause: unknown) => void;
  readonly failWorker: (error: ProviderIoWorkerError, kill: boolean) => void;
}

interface Slot {
  readonly index: number;
  readonly worker: NodeWorkerThreads.Worker;
  readonly handshake: Deferred.Deferred<void, ProviderIoWorkerError>;
  readonly sessions: Map<number, SessionHandle>;
  load: number;
  stopped: boolean;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

// Checked by hand: a schema decode per message would cost main the work the pool moves off it.
const parseWorkerMessage = (message: unknown): ProviderIoWorkerMessage | undefined => {
  if (!isRecord(message)) return undefined;
  const hasSession = typeof message.session === "number";
  switch (message.type) {
    case "ready":
      return { type: "ready" };
    case "output":
      return hasSession && Array.isArray(message.outputs) && typeof message.bytes === "number"
        ? (message as ProviderIoWorkerMessage)
        : undefined;
    case "end":
    case "fail":
      return hasSession ? (message as ProviderIoWorkerMessage) : undefined;
    default:
      return undefined;
  }
};

const describeMessageType = (message: unknown) =>
  isRecord(message) && typeof message.type === "string"
    ? message.type.slice(0, 64)
    : typeof message;

const reasonOf = (cause: unknown) => (cause instanceof Error ? cause.message : String(cause));

const spawnWorker = () =>
  embeddedProviderIoWorkerSource === undefined
    ? new NodeWorkerThreads.Worker(new URL("./providerIoWorker.ts", import.meta.url), {
        name: "t3-provider-io",
      })
    : new NodeWorkerThreads.Worker(embeddedProviderIoWorkerSource, {
        eval: true,
        name: "t3-provider-io",
      });

export const make = Effect.fn("ProviderIoWorkerPool.make")(function* (options: {
  readonly workers: number;
}) {
  const scope = yield* Effect.scope;
  const runFork = yield* FiberSet.makeRuntime<never>();
  const slots = Arr.makeBy(options.workers, (): Slot | undefined => undefined);
  let unavailable = false;
  let closing = false;
  let nextSessionId = 0;

  const turnOff = (error: ProviderIoWorkerError) =>
    Effect.suspend(() => {
      if (unavailable || closing) return Effect.void;
      unavailable = true;
      return Effect.logWarning(
        "Provider I/O workers unavailable; provider output is handled on the main thread",
        { reason: reasonOf(error.cause) },
      );
    });

  const stop = (slot: Slot, error: ProviderIoWorkerError) => {
    if (slot.stopped) return;
    slot.stopped = true;
    if (slots[slot.index] === slot) slots[slot.index] = undefined;
    Deferred.doneUnsafe(slot.handshake, Exit.fail(error));
    const sessions = [...slot.sessions.values()];
    slot.sessions.clear();
    for (const session of sessions) session.failWorker(error, !closing);
    if (sessions.length > 0 && !closing) {
      runFork(
        Effect.logWarning("Provider I/O worker stopped; its sessions failed", {
          sessions: sessions.length,
          reason: reasonOf(error.cause),
        }),
      );
    }
  };

  const receive = (slot: Slot, raw: unknown) => {
    const message = parseWorkerMessage(raw);
    if (message === undefined) {
      runFork(
        Effect.logWarning("Provider I/O worker sent an unknown message", {
          type: describeMessageType(raw),
        }),
      );
      return;
    }
    if (message.type === "ready") {
      Deferred.doneUnsafe(slot.handshake, Exit.void);
      return;
    }
    const session = slot.sessions.get(message.session);
    if (session === undefined) return;
    switch (message.type) {
      case "output":
        session.deliver(message.outputs, message.bytes);
        return;
      case "end":
        session.finish();
        return;
      case "fail":
        session.failStage(message.error);
        return;
    }
  };

  const start = (index: number) =>
    Effect.suspend(() => {
      let worker: NodeWorkerThreads.Worker;
      try {
        worker = spawnWorker();
      } catch (cause) {
        return turnOff(new ProviderIoWorkerError({ cause })).pipe(Effect.as(undefined));
      }
      worker.unref();
      const slot: Slot = {
        index,
        worker,
        handshake: Deferred.makeUnsafe(),
        sessions: new Map(),
        load: 0,
        stopped: false,
      };
      slots[index] = slot;
      worker.on("message", (message: unknown) => receive(slot, message));
      worker.on("error", (cause) => stop(slot, new ProviderIoWorkerError({ cause })));
      worker.on("exit", (code) =>
        stop(
          slot,
          new ProviderIoWorkerError({
            cause: new Error(`Provider I/O worker exited with code ${code}.`),
          }),
        ),
      );
      worker.on("messageerror", (cause) => {
        stop(slot, new ProviderIoWorkerError({ cause }));
        void worker.terminate();
      });
      return Deferred.await(slot.handshake).pipe(
        Effect.timeoutOrElse({
          duration: HANDSHAKE_TIMEOUT,
          orElse: () =>
            Effect.fail(
              new ProviderIoWorkerError({
                cause: new Error(
                  `Provider I/O worker was not ready within ${Duration.toSeconds(HANDSHAKE_TIMEOUT)} s.`,
                ),
              }),
            ),
        }),
        Effect.matchEffect({
          onSuccess: () => Effect.logDebug("Provider I/O worker ready", { worker: index }),
          onFailure: (error) =>
            Effect.sync(() => {
              stop(slot, error);
              void worker.terminate();
            }).pipe(Effect.andThen(turnOff(error))),
        }),
        Effect.forkIn(scope, { startImmediately: true }),
        Effect.as(slot),
      );
    });

  // A stopped slot counts as idle, so it restarts only when a session needs it.
  const pick = Effect.suspend(() => {
    let chosen = 0;
    for (let index = 1; index < slots.length; index++) {
      const candidate = slots[index];
      const best = slots[chosen];
      const load = candidate?.load ?? 0;
      const bestLoad = best?.load ?? 0;
      if (load < bestLoad || (load === bestLoad && candidate !== undefined && best === undefined)) {
        chosen = index;
      }
    }
    const slot = slots[chosen];
    return slot === undefined ? start(chosen) : Effect.succeed(slot);
  });

  // Untraced: the open span already covers it.
  const makeSession = Effect.fnUntraced(function* <Options, Output, E>(
    slot: Slot,
    input: ProviderIoOpenInput<Options, Output, E>,
  ) {
    const id = nextSessionId++;
    const highWaterMark = input.highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
    const queue = yield* Queue.unbounded<
      { readonly outputs: Arr.NonEmptyReadonlyArray<Output>; readonly bytes: number },
      E | Cause.Done
    >();
    const waiters: Array<{
      readonly bytes: number;
      readonly admit: () => void;
      readonly fail: (effect: Effect.Effect<void, E>) => void;
    }> = [];
    let outstanding = 0;
    let failure: { readonly error: E } | undefined;
    let ended = false;
    let finished = false;
    let closed = false;

    const post = (message: ProviderIoMainMessage, transfer?: ReadonlyArray<ArrayBuffer>) => {
      if (slot.stopped) return;
      slot.worker.postMessage(message, transfer);
    };
    // A copy owns its buffer: transferring the caller's view could detach a pooled Buffer.
    const send = (chunk: Uint8Array) => {
      outstanding += chunk.byteLength;
      const copy = new Uint8Array(chunk);
      post({ type: "bytes", session: id, chunk: copy }, [copy.buffer]);
    };
    const admit = () => {
      while (waiters.length > 0) {
        const next = waiters[0]!;
        if (next.bytes > 0 && outstanding >= highWaterMark) return;
        waiters.shift();
        next.admit();
      }
    };
    const release = (bytes: number) => {
      outstanding -= bytes;
      admit();
    };
    const enqueue = (bytes: number, admitNow: () => void) =>
      Effect.callback<void, E>((resume) => {
        const waiter = {
          bytes,
          admit: () => {
            admitNow();
            resume(Effect.void);
          },
          fail: resume,
        };
        waiters.push(waiter);
        return Effect.sync(() => {
          const index = waiters.indexOf(waiter);
          if (index !== -1) waiters.splice(index, 1);
        });
      });
    const fail = (error: ProviderIoWorkerError | ProviderIoStageError, kill: boolean) => {
      if (failure !== undefined || finished || closed) return;
      const mapped = input.onFailure(error);
      failure = { error: mapped };
      Queue.failCauseUnsafe(queue, Cause.fail(mapped));
      for (const waiter of waiters.splice(0)) waiter.fail(Effect.fail(mapped));
      if (kill) runFork(input.kill);
    };

    slot.sessions.set(id, {
      deliver: (outputs, bytes) => {
        if (failure !== undefined || closed) return;
        if (Arr.isReadonlyArrayNonEmpty(outputs)) {
          Queue.offerUnsafe(queue, {
            outputs: outputs as Arr.NonEmptyReadonlyArray<Output>,
            bytes,
          });
        } else {
          release(bytes);
        }
      },
      finish: () => {
        finished = true;
        Queue.endUnsafe(queue);
      },
      failStage: (cause) =>
        fail(new ProviderIoStageError({ stage: input.stage.name, cause }), false),
      failWorker: fail,
    });
    if (slot.sessions.size === 1) slot.worker.ref();
    post({ type: "open", session: id, stage: input.stage.name, options: input.options });

    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        closed = true;
        for (const waiter of waiters.splice(0)) waiter.fail(Effect.interrupt);
        Queue.shutdownUnsafe(queue);
        if (!slot.sessions.delete(id)) return;
        post({ type: "close", session: id });
        if (slot.sessions.size === 0) slot.worker.unref();
      }),
    );

    const session: ProviderIoSession<Output, E> = {
      write: (chunk) =>
        Effect.suspend(() => {
          if (failure !== undefined) return Effect.fail(failure.error);
          if (closed) return Effect.interrupt;
          if (ended) return Effect.die(new Error("Provider I/O session input already ended."));
          if (chunk.byteLength === 0) return Effect.void;
          if (waiters.length === 0 && outstanding < highWaterMark) {
            send(chunk);
            return Effect.void;
          }
          return enqueue(chunk.byteLength, () => send(chunk));
        }),
      end: Effect.suspend(() => {
        if (failure !== undefined) return Effect.fail(failure.error);
        if (closed) return Effect.interrupt;
        if (ended) return Effect.void;
        ended = true;
        const sendEnd = () => post({ type: "eof", session: id });
        if (waiters.length === 0) {
          sendEnd();
          return Effect.void;
        }
        return enqueue(0, sendEnd);
      }),
      output: Stream.fromQueue(queue).pipe(
        Stream.mapArray((batches) => {
          let bytes = 0;
          for (const batch of batches) bytes += batch.bytes;
          release(bytes);
          return Arr.flatMap(batches, (batch) => batch.outputs);
        }),
      ),
    };
    return session;
  }, Effect.uninterruptible);

  const open = Effect.fn("ProviderIoWorkerPool.open")(function* <Options, Output, E>(
    input: ProviderIoOpenInput<Options, Output, E>,
  ) {
    if (options.workers === 0 || unavailable || closing) return Option.none();
    const slot = yield* Effect.acquireRelease(
      pick.pipe(
        Effect.map((picked) => {
          if (picked !== undefined) picked.load += 1;
          return picked;
        }),
      ),
      (picked) =>
        Effect.sync(() => {
          if (picked !== undefined) picked.load -= 1;
        }),
    );
    if (slot === undefined) return Option.none();
    const ready = yield* Deferred.await(slot.handshake).pipe(
      Effect.match({ onFailure: () => false, onSuccess: () => !slot.stopped }),
    );
    if (!ready) return Option.none();
    return Option.some(yield* makeSession(slot, input));
  });

  yield* Effect.addFinalizer(() =>
    Effect.promise(() => {
      closing = true;
      return Promise.all(slots.map((slot) => slot?.worker.terminate()));
    }),
  );

  for (let index = 0; index < options.workers; index++) {
    if (unavailable) break;
    yield* start(index);
  }

  return ProviderIoWorkerPool.of({ open });
});

export const layer = Layer.effect(
  ProviderIoWorkerPool,
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    return yield* make({ workers: config.providerIoWorkers ?? 0 });
  }),
);

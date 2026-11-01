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
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { errorTag } from "@t3tools/shared/observability";

import * as ServerConfig from "../../config.ts";
import { loadEmbeddedProviderIoWorkerSource } from "./embeddedProviderIoWorker.ts";
import type {
  ProviderIoMainMessage,
  ProviderIoStage,
  ProviderIoWorkerMessage,
} from "./providerIoProtocol.ts";

export class ProviderIoWorkerError extends Schema.TaggedError<ProviderIoWorkerError>()(
  "ProviderIoWorkerError",
  {
    reason: Schema.Literals(["spawn-failed", "not-ready", "error", "exit", "message-error"]),
    exitCode: Schema.optionalKey(Schema.Int),
    cause: Schema.optionalKey(Schema.Defect()),
  },
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

export interface ProviderIoSession<Output, E, Request = never, Reply = never> {
  /** Suspends while the session's unconsumed input is at its high-water mark. */
  readonly write: (chunk: Uint8Array) => Effect.Effect<void, E>;
  readonly end: Effect.Effect<void, E>;
  readonly output: Stream.Stream<Output, E>;
  readonly call: (request: Request) => Effect.Effect<Reply, E>;
  /** A call whose reply nobody waits for; ordered like `call`. False when the session can take none. */
  readonly notify: (request: Request) => boolean;
}

export interface ProviderIoOpenInput<
  Options,
  Output,
  E,
  Request = never,
  Reply = never,
  Event = never,
> {
  readonly stage: ProviderIoStage<Options, Output, Request, Reply, Event>;
  readonly options: Options;
  readonly onFailure: (error: ProviderIoWorkerError | ProviderIoStageError) => E;
  /** Stops the session's child process when its worker dies. */
  readonly kill: Effect.Effect<void>;
  /** The child can outlive the session's output (a stage that runs a library), so kill it even then. */
  readonly killAfterEnd?: boolean;
  /** Receives the stage's events on main as they arrive, ahead of queued outputs. */
  readonly onEvent?: (event: Event) => void;
  readonly highWaterMark?: number;
}

export class ProviderIoWorkerPool extends Context.Service<
  ProviderIoWorkerPool,
  {
    /**
     * None when the pool is off or unavailable; the caller then runs the stage in-thread. An
     * open that fails or is interrupted before it returns keeps nothing.
     */
    readonly open: <Options, Output, E, Request = never, Reply = never, Event = never>(
      input: ProviderIoOpenInput<Options, Output, E, Request, Reply, Event>,
    ) => Effect.Effect<Option.Option<ProviderIoSession<Output, E, Request, Reply>>, E, Scope.Scope>;
  }
>()("t3/provider/io/ProviderIoWorkerPool") {}

export const PROVIDER_IO_MAX_WORKERS = 32;
const HANDSHAKE_TIMEOUT = Duration.seconds(10);
const DEFAULT_HIGH_WATER_MARK = 1024 * 1024;

interface SessionHandle {
  readonly deliver: (outputs: ReadonlyArray<unknown>, bytes: number) => void;
  readonly finish: (failure?: { readonly error: unknown }) => void;
  readonly event: (event: unknown) => void;
  readonly reply: (call: number, ok: boolean, value: unknown) => void;
  readonly failStage: (cause: unknown) => void;
  readonly failWorker: (error: ProviderIoWorkerError, kill: boolean) => void;
}

interface Slot {
  readonly index: number;
  readonly worker: NodeWorkerThreads.Worker;
  readonly handshake: Deferred.Deferred<void, ProviderIoWorkerError>;
  readonly sessions: Map<number, SessionHandle>;
  load: number;
  ready: boolean;
  stopped: boolean;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isCount = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;

// Checked by hand: a schema decode per message would cost main the work the pool moves off it.
const parseWorkerMessage = (message: unknown): ProviderIoWorkerMessage | undefined => {
  if (!isRecord(message)) return undefined;
  if (message.type === "ready") return { type: "ready" };
  if (!isCount(message.session)) return undefined;
  switch (message.type) {
    case "output":
      return Array.isArray(message.outputs) && isCount(message.bytes)
        ? (message as ProviderIoWorkerMessage)
        : undefined;
    case "reply":
      return isCount(message.call) && typeof message.ok === "boolean"
        ? (message as ProviderIoWorkerMessage)
        : undefined;
    case "end":
    case "fail":
    case "event":
      return message as ProviderIoWorkerMessage;
    default:
      return undefined;
  }
};

const describeMessageType = (message: unknown) =>
  isRecord(message) && typeof message.type === "string"
    ? message.type.slice(0, 64)
    : typeof message;

// A worker's error text can carry provider data, so logs name only its class and a fixed reason.
const describeStop = (error: ProviderIoWorkerError) => ({
  reason: error.reason,
  ...(error.exitCode === undefined ? {} : { exitCode: error.exitCode }),
  ...(error.cause === undefined ? {} : { errorTag: errorTag(error.cause) }),
});

const spawnWorker = () => {
  const source = loadEmbeddedProviderIoWorkerSource();
  return source === undefined
    ? new NodeWorkerThreads.Worker(new URL("./providerIoWorker.ts", import.meta.url), {
        name: "t3-provider-io",
      })
    : new NodeWorkerThreads.Worker(source, { eval: true, name: "t3-provider-io" });
};

// Cloning a view copies its whole backing buffer, so only a chunk that owns its buffer is cloned;
// any other is copied once and the copy transferred.
const ownsBuffer = (chunk: Uint8Array) =>
  chunk.byteOffset === 0 &&
  chunk.buffer instanceof ArrayBuffer &&
  chunk.byteLength === chunk.buffer.byteLength;

export const make = Effect.fn("ProviderIoWorkerPool.make")(function* (options: {
  readonly workers: number;
}) {
  const scope = yield* Effect.scope;
  const runFork = yield* FiberSet.makeRuntime<never>();
  const slots = Arr.makeBy(options.workers, (): Slot | undefined => undefined);
  let unavailable = false;
  let closing = false;
  let nextSessionId = 0;

  // Called synchronously, so the pool is off before anything else can run; the log follows.
  const turnOff = (error: ProviderIoWorkerError) => {
    if (unavailable || closing) return Effect.void;
    unavailable = true;
    return Effect.logWarning(
      "Provider I/O workers unavailable; provider output is handled on the main thread",
      describeStop(error),
    );
  };

  const stop = (slot: Slot, error: ProviderIoWorkerError) => {
    if (slot.stopped) return;
    slot.stopped = true;
    if (slots[slot.index] === slot) slots[slot.index] = undefined;
    Deferred.doneUnsafe(slot.handshake, Exit.fail(error));
    const sessions = [...slot.sessions.values()];
    slot.sessions.clear();
    for (const session of sessions) session.failWorker(error, !closing);
    // A worker that never became ready is reported by its handshake as the pool turning off.
    if (slot.ready && !closing) {
      runFork(
        Effect.logWarning("Provider I/O worker stopped", {
          worker: slot.index,
          sessions: sessions.length,
          ...describeStop(error),
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
      slot.ready = true;
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
        session.finish(message.failed === true ? { error: message.error } : undefined);
        return;
      case "event":
        session.event(message.event);
        return;
      case "fail":
        session.failStage(message.error);
        return;
      case "reply":
        session.reply(message.call, message.ok, message.value);
        return;
    }
  };

  // Synchronous, so concurrent opens see the slot as taken before either can yield.
  const spawnSlot = (index: number): Slot => {
    const worker = spawnWorker();
    worker.unref();
    const slot: Slot = {
      index,
      worker,
      handshake: Deferred.makeUnsafe(),
      sessions: new Map(),
      load: 0,
      ready: false,
      stopped: false,
    };
    slots[index] = slot;
    worker.on("message", (message: unknown) => receive(slot, message));
    worker.on("error", (cause) =>
      stop(slot, new ProviderIoWorkerError({ reason: "error", cause })),
    );
    worker.on("exit", (exitCode) =>
      stop(slot, new ProviderIoWorkerError({ reason: "exit", exitCode })),
    );
    worker.on("messageerror", (cause) => {
      stop(slot, new ProviderIoWorkerError({ reason: "message-error", cause }));
      void worker.terminate();
    });
    return slot;
  };

  const watchHandshake = (slot: Slot) =>
    Deferred.await(slot.handshake).pipe(
      Effect.timeoutOrElse({
        duration: HANDSHAKE_TIMEOUT,
        orElse: () => Effect.fail(new ProviderIoWorkerError({ reason: "not-ready" })),
      }),
      Effect.matchEffect({
        onSuccess: () => Effect.logDebug("Provider I/O worker ready", { worker: slot.index }),
        onFailure: (error) =>
          Effect.suspend(() => {
            stop(slot, error);
            void slot.worker.terminate();
            return turnOff(error);
          }),
      }),
      Effect.forkIn(scope, { startImmediately: true }),
    );

  // Spawns at once (callers rely on that) and returns the effect that watches the handshake.
  const startSlot = (index: number, load: number): Effect.Effect<Option.Option<Slot>> => {
    let slot: Slot;
    try {
      slot = spawnSlot(index);
    } catch (cause) {
      return turnOff(new ProviderIoWorkerError({ reason: "spawn-failed", cause })).pipe(
        Effect.as(Option.none()),
      );
    }
    slot.load = load;
    return watchHandshake(slot).pipe(Effect.as(Option.some(slot)));
  };

  // Picks the least-loaded slot and counts the caller on it in one synchronous step; a stopped
  // slot counts as idle, so it restarts only when a session needs it.
  const reserve = Effect.suspend((): Effect.Effect<Option.Option<Slot>> => {
    if (unavailable || closing) return Effect.succeedNone;
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
    const existing = slots[chosen];
    if (existing === undefined) return startSlot(chosen, 1);
    existing.load += 1;
    return Effect.succeedSome(existing);
  });

  // Untraced: the open span already covers it.
  const makeSession = Effect.fnUntraced(function* <Options, Output, E, Request, Reply, Event>(
    slot: Slot,
    input: ProviderIoOpenInput<Options, Output, E, Request, Reply, Event>,
  ) {
    const queue = yield* Queue.unbounded<
      { readonly outputs: Arr.NonEmptyReadonlyArray<Output>; readonly bytes: number },
      E | Cause.Done
    >();
    // From this check to the open message nothing yields, so a stop cannot slip in between.
    if (slot.stopped || unavailable || closing) return Option.none();
    const id = nextSessionId++;
    const highWaterMark = Math.max(1, input.highWaterMark ?? DEFAULT_HIGH_WATER_MARK);
    // Writes, ends and calls leave in arrival order; only writes need credit.
    const waiters: Array<{
      readonly bytes: number;
      readonly admit: () => void;
      readonly fail: (effect: Effect.Effect<never, E>) => void;
    }> = [];
    const calls = new Map<number, (effect: Effect.Effect<Reply, E>) => void>();
    let nextCallId = 0;
    let outstanding = 0;
    let failure: { readonly error: E } | undefined;
    // Once accepted, the end of input is sent even if its caller stops waiting for it.
    let ending:
      | { sent: boolean; readonly waiting: Set<(effect: Effect.Effect<void, E>) => void> }
      | undefined;
    let finished = false;
    let closed = false;

    const post = (message: ProviderIoMainMessage, transfer?: ReadonlyArray<ArrayBuffer>) => {
      if (!slot.stopped) slot.worker.postMessage(message, transfer);
    };
    // Options and call requests come from the caller and may not survive a structured clone.
    const postCloned = (message: ProviderIoMainMessage): E | undefined => {
      try {
        post(message);
        return undefined;
      } catch (cause) {
        return input.onFailure(new ProviderIoStageError({ stage: input.stage.name, cause }));
      }
    };
    const send = (chunk: Uint8Array) => {
      outstanding += chunk.byteLength;
      if (ownsBuffer(chunk)) {
        post({ type: "bytes", session: id, chunk });
        return;
      }
      // Not `slice`: on a Buffer that is a view sharing the caller's memory.
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
    // A worker never returns more than was sent; a bogus count must not open the window for good.
    const release = (bytes: number) => {
      outstanding = Math.max(0, outstanding - bytes);
      admit();
    };
    // Admits at once or queues, in one step, so returning credit cannot miss the waiter; the
    // returned cancel also lets whatever queued behind it go.
    const schedule = (waiter: (typeof waiters)[number]) => {
      if (waiters.length === 0 && (waiter.bytes === 0 || outstanding < highWaterMark)) {
        waiter.admit();
        return () => {};
      }
      waiters.push(waiter);
      return () => {
        const index = waiters.indexOf(waiter);
        if (index === -1) return;
        waiters.splice(index, 1);
        admit();
      };
    };
    const whenAdmitted = (bytes: number, onAdmit: () => void) =>
      Effect.callback<void, E>((resume) => {
        if (failure !== undefined) {
          resume(Effect.fail(failure.error));
          return;
        }
        if (closed) {
          resume(Effect.interrupt);
          return;
        }
        const cancel = schedule({
          bytes,
          admit: () => {
            onAdmit();
            resume(Effect.void);
          },
          fail: resume,
        });
        return Effect.sync(cancel);
      });
    // A session whose output already ended keeps its outputs and its child; only calls fail.
    const fail = (error: ProviderIoWorkerError | ProviderIoStageError, kill: boolean) => {
      if (failure !== undefined || closed) return;
      const mapped = input.onFailure(error);
      failure = { error: mapped };
      for (const resume of calls.values()) resume(Effect.fail(mapped));
      calls.clear();
      for (const waiter of waiters.splice(0)) waiter.fail(Effect.fail(mapped));
      if (finished) {
        if (kill && input.killAfterEnd === true) runFork(input.kill);
        return;
      }
      Queue.failCauseUnsafe(queue, Cause.fail(mapped));
      if (kill) runFork(input.kill);
    };

    const opened = postCloned({
      type: "open",
      session: id,
      stage: input.stage.name,
      options: input.options,
    });
    if (opened !== undefined) return yield* Effect.fail(opened);
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
      // A failed end fails only the output; calls stay open.
      finish: (end) => {
        if (failure !== undefined || closed || finished) return;
        finished = true;
        if (end === undefined) {
          Queue.endUnsafe(queue);
          return;
        }
        Queue.failCauseUnsafe(
          queue,
          Cause.fail(
            input.onFailure(
              new ProviderIoStageError({ stage: input.stage.name, cause: end.error }),
            ),
          ),
        );
      },
      event: (event) => {
        if (failure !== undefined || closed) return;
        input.onEvent?.(event as Event);
      },
      reply: (call, ok, value) => {
        const resume = calls.get(call);
        if (resume === undefined) return;
        calls.delete(call);
        resume(
          ok
            ? Effect.succeed(value as Reply)
            : Effect.fail(
                input.onFailure(
                  new ProviderIoStageError({ stage: input.stage.name, cause: value }),
                ),
              ),
        );
      },
      failStage: (cause) =>
        fail(new ProviderIoStageError({ stage: input.stage.name, cause }), false),
      failWorker: fail,
    });
    if (slot.sessions.size === 1) slot.worker.ref();

    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        closed = true;
        for (const waiter of waiters.splice(0)) waiter.fail(Effect.interrupt);
        for (const resume of calls.values()) resume(Effect.interrupt);
        calls.clear();
        Queue.shutdownUnsafe(queue);
        if (!slot.sessions.delete(id)) return;
        post({ type: "close", session: id });
        if (slot.sessions.size === 0) slot.worker.unref();
      }),
    );

    const session: ProviderIoSession<Output, E, Request, Reply> = {
      write: (chunk) =>
        Effect.suspend(() => {
          if (ending !== undefined) {
            return Effect.die(new Error("Provider I/O session input already ended."));
          }
          if (chunk.byteLength === 0) return Effect.void;
          return whenAdmitted(chunk.byteLength, () => send(chunk));
        }),
      end: Effect.callback<void, E>((resume) => {
        if (failure !== undefined) {
          resume(Effect.fail(failure.error));
          return;
        }
        if (closed) {
          resume(Effect.interrupt);
          return;
        }
        if (ending === undefined) {
          const state = {
            sent: false,
            waiting: new Set<(effect: Effect.Effect<void, E>) => void>(),
          };
          ending = state;
          const settle = (effect: Effect.Effect<void, E>) => {
            for (const waiter of state.waiting) waiter(effect);
            state.waiting.clear();
          };
          schedule({
            bytes: 0,
            admit: () => {
              post({ type: "eof", session: id });
              state.sent = true;
              settle(Effect.void);
            },
            fail: settle,
          });
        }
        if (ending.sent) {
          resume(Effect.void);
          return;
        }
        const waiting = ending.waiting;
        waiting.add(resume);
        return Effect.sync(() => {
          waiting.delete(resume);
        });
      }),
      output: Stream.fromQueue(queue).pipe(
        Stream.mapArray((batches) => {
          let bytes = 0;
          for (const batch of batches) bytes += batch.bytes;
          release(bytes);
          return Arr.flatMap(batches, (batch) => batch.outputs);
        }),
      ),
      // Queued behind waiting writes, so the stage answers on the state they leave.
      call: (request) =>
        Effect.callback<Reply, E>((resume) => {
          if (failure !== undefined) {
            resume(Effect.fail(failure.error));
            return;
          }
          if (closed) {
            resume(Effect.interrupt);
            return;
          }
          const call = nextCallId++;
          const cancel = schedule({
            bytes: 0,
            admit: () => {
              calls.set(call, resume);
              const failed = postCloned({ type: "call", session: id, call, request });
              if (failed === undefined) return;
              calls.delete(call);
              resume(Effect.fail(failed));
            },
            fail: resume,
          });
          return Effect.sync(() => {
            cancel();
            calls.delete(call);
          });
        }),
      notify: (request) => {
        if (failure !== undefined || closed || slot.stopped) return false;
        post({ type: "notify", session: id, request });
        return true;
      },
    };
    return Option.some(session);
  }, Effect.uninterruptible);

  // Runs in the scope `open` forks; returning a session is what keeps that scope.
  const openInScope = Effect.fn("ProviderIoWorkerPool.open")(function* <
    Options,
    Output,
    E,
    Request,
    Reply,
    Event,
  >(input: ProviderIoOpenInput<Options, Output, E, Request, Reply, Event>) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const reserved = yield* reserve;
        if (Option.isNone(reserved)) return Option.none();
        const slot = reserved.value;
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            slot.load -= 1;
          }),
        );
        const ready = yield* restore(Deferred.await(slot.handshake)).pipe(
          Effect.match({ onFailure: () => false, onSuccess: () => true }),
        );
        if (!ready) return Option.none();
        return yield* makeSession(slot, input);
      }),
    );
  });

  // An open that fails, finds no worker or is interrupted before it returns releases its
  // reservation and session at once; a returned session belongs to the caller's scope.
  const open = <Options, Output, E, Request = never, Reply = never, Event = never>(
    input: ProviderIoOpenInput<Options, Output, E, Request, Reply, Event>,
  ): Effect.Effect<Option.Option<ProviderIoSession<Output, E, Request, Reply>>, E, Scope.Scope> =>
    options.workers === 0
      ? Effect.succeedNone
      : Effect.flatMap(Effect.scope, (parent) =>
          Effect.flatMap(Scope.fork(parent), (scope) =>
            openInScope(input).pipe(
              Scope.provide(scope),
              Effect.onExit((exit) =>
                Exit.isSuccess(exit) && Option.isSome(exit.value)
                  ? Effect.void
                  : Scope.close(scope, exit),
              ),
            ),
          ),
        );

  yield* Effect.addFinalizer(() =>
    Effect.promise(() => {
      closing = true;
      return Promise.all(slots.map((slot) => slot?.worker.terminate()));
    }),
  );

  for (let index = 0; index < options.workers; index++) {
    if (unavailable) break;
    yield* startSlot(index, 0);
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

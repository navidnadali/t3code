import type {
  McpServerConfig,
  Options as ClaudeQueryOptions,
  Query as ClaudeQuery,
  SDKMessage,
  SDKUserMessage,
  SpawnedProcess as ClaudeSpawnedProcess,
  SpawnOptions as ClaudeSpawnOptions,
} from "@anthropic-ai/claude-agent-sdk";
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import {
  type ClaudeAgentSdkCallbackCall,
  type ClaudeAgentSdkStageOptions,
  type ClaudeAgentSdkWorkerCallback,
  type ClaudeAgentSdkWorkerEvent,
  type ClaudeAgentSdkWorkerHooks,
  type ClaudeAgentSdkWorkerRequest,
  claudeAgentSdkStage,
  toClaudeAgentSdkProcessError,
} from "../../provider/io/claudeAgentSdkStage.ts";
import type {
  ProviderIoSession,
  ProviderIoStageError,
  ProviderIoWorkerError,
  ProviderIoWorkerPool,
} from "../../provider/io/ProviderIoWorkerPool.ts";

/** The pool Claude queries run on; undefined (the default) keeps them on the main thread. */
export const ClaudeAgentSdkWorkerPool = Context.Reference<
  ProviderIoWorkerPool["Service"] | undefined
>("t3/orchestration-v2/Adapters/ClaudeAgentSdkWorkerPool", { defaultValue: () => undefined });

/** What the query runner calls on a query, whether the SDK runs here or in a worker. */
export type ClaudeQueryControl = Pick<
  ClaudeQuery,
  | "setMcpServers"
  | "setModel"
  | "applyFlagSettings"
  | "setPermissionMode"
  | "stopTask"
  | "backgroundTasks"
  | "interrupt"
  | "close"
  | typeof Symbol.asyncDispose
>;

export interface StartedClaudeQuery<E> {
  readonly query: ClaudeQueryControl;
  readonly messages: Stream.Stream<SDKMessage, E>;
}

/** The callbacks main runs for the worker's SDK. */
export type ClaudeAgentSdkMainCallbacks = Pick<
  ClaudeQueryOptions,
  ClaudeAgentSdkWorkerCallback | "hooks"
>;

/**
 * The worker's share of the query options, or undefined when the query must stay in-thread: an
 * abort controller, options that are not plain data (a session store, an in-process MCP server,
 * a hook that is not a function), or no CLI path (the worker cannot find the SDK's bundled CLI
 * the way main does). Main's callbacks and hooks stay on main; the worker relays their calls.
 */
export function claudeAgentSdkStageOptions(
  options: ClaudeQueryOptions,
  mcpServers: Record<string, McpServerConfig> | undefined,
): ClaudeAgentSdkStageOptions | undefined {
  const {
    abortController,
    canUseTool,
    hooks,
    mcpServers: _mcpServers,
    onElicitation,
    onUserDialog,
    spawnClaudeCodeProcess: _spawnClaudeCodeProcess,
    stderr: _stderr,
    ...data
  } = options;
  if (abortController !== undefined || data.pathToClaudeCodeExecutable === undefined) {
    return undefined;
  }
  if (Object.values(mcpServers ?? {}).some((server) => server.type === "sdk")) return undefined;
  if (!isPlainData(data) || !isPlainData(mcpServers)) return undefined;
  const hookShape = hooks === undefined ? undefined : hooksShape(hooks);
  if (hookShape === null) return undefined;
  const callbacks: Array<ClaudeAgentSdkWorkerCallback> = [];
  if (canUseTool !== undefined) callbacks.push("canUseTool");
  if (onElicitation !== undefined) callbacks.push("onElicitation");
  if (onUserDialog !== undefined) callbacks.push("onUserDialog");
  return {
    options: data,
    callbacks,
    ...(hookShape === undefined ? {} : { hooks: hookShape }),
  };
}

// Null when a matcher is not plain data besides its hooks, or a hook is not a function.
const hooksShape = (
  hooks: NonNullable<ClaudeQueryOptions["hooks"]>,
): ClaudeAgentSdkWorkerHooks | null => {
  const shape: Record<string, ClaudeAgentSdkWorkerHooks[keyof ClaudeAgentSdkWorkerHooks]> = {};
  for (const [event, matchers] of Object.entries(hooks)) {
    if (!Array.isArray(matchers)) return null;
    const matcherShapes = [];
    for (const matcher of matchers as ReadonlyArray<unknown>) {
      if (typeof matcher !== "object" || matcher === null) return null;
      const { hooks: callbacks, ...rest } = matcher as { readonly hooks?: unknown };
      if (!Array.isArray(callbacks) || !isPlainData(rest)) return null;
      if (!callbacks.every((callback) => typeof callback === "function")) return null;
      matcherShapes.push({ ...rest, hooks: callbacks.length });
    }
    shape[event] = matcherShapes;
  }
  return shape;
};

const callbackOf = (
  callbacks: ClaudeAgentSdkMainCallbacks,
  call: ClaudeAgentSdkCallbackCall,
): ((...args: ReadonlyArray<never>) => unknown) | undefined => {
  if (call.name !== "hook") return callbacks[call.name];
  const [event, matcher, index] = call.hook;
  return callbacks.hooks?.[event]?.[matcher]?.hooks[index];
};

// What the SDK sends the CLI for a callback that throws.
const sdkErrorText = (error: unknown) => {
  try {
    return error instanceof Error ? String(error.message) : String(error);
  } catch {
    return Object.prototype.toString.call(error);
  }
};

// What structured cloning carries over unchanged: no functions, class instances or cycles.
const isPlainData = (value: unknown, ancestors = new Set<object>()): boolean => {
  if (typeof value === "function" || typeof value === "symbol") return false;
  if (typeof value !== "object" || value === null) return true;
  if (ancestors.has(value)) return false;
  const prototype: unknown = Object.getPrototypeOf(value);
  if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null) return false;
  ancestors.add(value);
  const plain = Object.values(value).every((entry) => isPlainData(entry, ancestors));
  ancestors.delete(value);
  return plain;
};

type WorkerSessionError = ProviderIoWorkerError | ProviderIoStageError;

// A stage error carries what the SDK threw; a dead worker is its own cause.
const workerCause = (error: WorkerSessionError): unknown =>
  error._tag === "ProviderIoStageError" ? error.cause : error;

// Read from the CLI ahead of the worker, before the pipe is paused.
const STDOUT_BUFFER_BYTES = 1024 * 1024;

/**
 * Runs `query()` on a provider I/O worker, or returns none when the pool is off. Main keeps the
 * CLI: it spawns it with `spawn` (the in-thread spawn hook), feeds its stdout to the worker in
 * order with its exit and errors, and writes, ends and kills it as the worker's SDK asks.
 */
export const startClaudeQueryInWorker = Effect.fnUntraced(function* <E>(input: {
  readonly pool: ProviderIoWorkerPool["Service"];
  readonly stageOptions: ClaudeAgentSdkStageOptions;
  readonly callbacks: ClaudeAgentSdkMainCallbacks;
  readonly prompt: AsyncIterable<SDKUserMessage>;
  readonly spawn: (options: ClaudeSpawnOptions) => ClaudeSpawnedProcess;
  readonly toError: (cause: unknown, method: string) => E;
}) {
  const context = yield* Effect.context<never>();
  const runFork = Effect.runForkWith(context);
  const runPromise = Effect.runPromiseWith(context);
  const scope = yield* Scope.make();
  // @effect-diagnostics-next-line abortControllerInEffect:off - Node aborts the CLI it spawned with this signal.
  const abort = new AbortController();
  // Main's side of each callback the worker's SDK is waiting on, by the worker's id.
  const callbacks = new Map<number, AbortController>();
  // Callbacks waiting for the consumer to finish the messages the SDK read before raising them.
  const due: Array<{ readonly after: number; readonly start: () => void }> = [];
  let handed = 0;
  let consuming = false;
  let consumerIdle = false;
  const frames: Array<Uint8Array | ClaudeAgentSdkWorkerRequest> = [];
  let session:
    | ProviderIoSession<SDKMessage, WorkerSessionError, ClaudeAgentSdkWorkerRequest, unknown>
    | undefined;
  let child: ClaudeSpawnedProcess | undefined;
  let childDone = false;
  let outputDone = false;
  let released = false;
  // Like the SDK's cleanup: no callback starts after it, and the ones running are aborted.
  let cleanedUp = false;
  let promptIterator: AsyncIterator<SDKUserMessage> | undefined;
  let bufferedBytes = 0;
  let paused = false;
  let pumping = false;

  const cleanUp = () => {
    cleanedUp = true;
    due.length = 0;
    const running = [...callbacks.values()];
    callbacks.clear();
    for (const controller of running) controller.abort();
  };

  // The worker's SDK may still kill the CLI after its output ends, so the session lasts until both.
  const release = () => {
    if (released || !outputDone || (child !== undefined && !childDone)) return;
    released = true;
    cleanUp();
    void promptIterator?.return?.();
    runFork(Scope.close(scope, Exit.void));
  };

  const pump: Effect.Effect<void> = Effect.gen(function* () {
    const target = session;
    if (target === undefined) return;
    while (true) {
      const frame = frames.shift();
      if (frame === undefined) return;
      if (!(frame instanceof Uint8Array)) {
        target.notify(frame);
        continue;
      }
      bufferedBytes -= frame.byteLength;
      if (paused && bufferedBytes < STDOUT_BUFFER_BYTES) {
        paused = false;
        child?.stdout.resume();
      }
      yield* target.write(frame);
    }
  }).pipe(
    // A failed session drops what is left; its output reports the failure.
    Effect.catchCause(() =>
      Effect.sync(() => {
        frames.length = 0;
        bufferedBytes = 0;
      }),
    ),
    Effect.ensuring(Effect.sync(() => (pumping = false))),
  );
  const kick = () => {
    if (pumping || released || frames.length === 0) return;
    pumping = true;
    runFork(pump.pipe(Effect.ensuring(Effect.sync(kick))));
  };
  const enqueue = (frame: Uint8Array | ClaudeAgentSdkWorkerRequest) => {
    frames.push(frame);
    kick();
  };

  const spawnChild = (event: Extract<ClaudeAgentSdkWorkerEvent, { readonly type: "spawn" }>) => {
    if (child !== undefined || released) return;
    let spawned: ClaudeSpawnedProcess;
    try {
      // The SDK passes cwd even when it is undefined.
      spawned = input.spawn({
        command: event.command,
        args: [...event.args],
        cwd: event.cwd,
        env: { ...event.env },
        signal: abort.signal,
      } as ClaudeSpawnOptions);
    } catch (error) {
      enqueue({ type: "error", error: toClaudeAgentSdkProcessError(error) });
      return;
    }
    child = spawned;
    spawned.stdout.on("data", (chunk: Uint8Array) => {
      bufferedBytes += chunk.byteLength;
      if (!paused && bufferedBytes >= STDOUT_BUFFER_BYTES) {
        paused = true;
        spawned.stdout.pause();
      }
      enqueue(chunk);
    });
    spawned.stdout.once("end", () => enqueue({ type: "stdoutEnd" }));
    spawned.stdout.on("error", (error) =>
      enqueue({ type: "stdoutError", error: toClaudeAgentSdkProcessError(error) }),
    );
    spawned.stdin.on("error", (error) =>
      session?.notify({ type: "stdinError", error: toClaudeAgentSdkProcessError(error) }),
    );
    spawned.once("exit", (code, signal) => {
      enqueue({ type: "exit", code, signal });
      childDone = true;
      release();
    });
    spawned.on("error", (error) => {
      enqueue({ type: "error", error: toClaudeAgentSdkProcessError(error) });
      // A spawn that fails emits only "error".
      if ("pid" in spawned && spawned.pid === undefined) {
        childDone = true;
        release();
      }
    });
  };

  const answer = (
    reply: Extract<ClaudeAgentSdkWorkerRequest, { readonly type: "callbackResult" }>,
  ) => {
    try {
      session?.notify(reply);
    } catch {
      // The SDK writes the answer as JSON, which drops what cloning refuses (such as functions).
      let result: unknown;
      try {
        result = JSON.parse(JSON.stringify(reply.result));
      } catch (error) {
        session?.notify({ type: "callbackError", id: reply.id, text: sdkErrorText(error) });
        return;
      }
      session?.notify({ ...reply, result });
    }
  };

  // A cancelled callback still answers, as in-thread.
  const runCallback = (id: number, call: ClaudeAgentSdkCallbackCall) => {
    const controller = callbacks.get(id);
    if (controller === undefined) return;
    const options = { ...call.args.at(-1), signal: controller.signal };
    const args: Array<unknown> = [...call.args.slice(0, -1), options];
    let result: unknown;
    try {
      const callback = callbackOf(input.callbacks, call);
      if (callback === undefined) throw new Error(`${call.name} callback is not provided.`);
      result = callback(...(args as Array<never>));
    } catch (error) {
      result = Promise.reject(error);
    }
    void Promise.resolve(result).then(
      (value) => {
        if (callbacks.delete(id)) answer({ type: "callbackResult", id, result: value });
      },
      (error: unknown) => {
        if (callbacks.delete(id)) {
          session?.notify({ type: "callbackError", id, text: sdkErrorText(error) });
        }
      },
    );
  };

  // Before the consumer's first pull nothing orders a callback, as in-thread.
  const startDue = () => {
    for (let next = due[0]; next !== undefined; next = due[0]) {
      if (consuming && next.after > handed) return;
      due.shift();
      next.start();
    }
  };

  const onEvent = (event: ClaudeAgentSdkWorkerEvent) => {
    switch (event.type) {
      case "spawn":
        spawnChild(event);
        return;
      case "stdin":
        child?.stdin.write(event.data);
        return;
      case "stdinEnd":
        child?.stdin.end();
        return;
      case "kill":
        child?.kill(event.signal);
        return;
      case "abort":
        abort.abort();
        return;
      case "callback":
        if (cleanedUp) return;
        callbacks.set(event.id, new AbortController());
        due.push({ after: event.after, start: () => runCallback(event.id, event.call) });
        if (consumerIdle || !consuming) startDue();
        return;
      case "callbackAbort":
        callbacks.get(event.id)?.abort();
        return;
    }
  };

  const opened = yield* input.pool
    .open({
      stage: claudeAgentSdkStage,
      options: input.stageOptions,
      onFailure: (error: WorkerSessionError) => error,
      kill: Effect.sync(() => {
        cleanUp();
        if (child !== undefined && !childDone) child.kill("SIGKILL");
      }),
      killAfterEnd: true,
      onEvent,
    })
    .pipe(Effect.provideService(Scope.Scope, scope));
  if (Option.isNone(opened)) {
    yield* Scope.close(scope, Exit.void);
    return Option.none<StartedClaudeQuery<E>>();
  }
  const workerSession = opened.value;
  session = workerSession;

  const finishOutput = Effect.sync(() => {
    outputDone = true;
    release();
  });
  yield* workerSession.call({ type: "start" }).pipe(
    Effect.mapError((error) => input.toError(workerCause(error), "query")),
    Effect.onError(() => finishOutput),
  );

  const prompts = input.prompt[Symbol.asyncIterator]();
  promptIterator = prompts;
  void (async () => {
    try {
      while (true) {
        const next = await prompts.next();
        if (next.done === true) break;
        if (!released) workerSession.notify({ type: "prompt", message: next.value });
      }
    } finally {
      if (!released) workerSession.notify({ type: "promptEnd" });
    }
  })();

  // A reply resumes its caller at once, but outputs wake the consumer through the scheduler; yielding
  // lets the consumer take the frames that came before the reply first, as it would in-thread.
  const request = <A>(message: ClaudeAgentSdkWorkerRequest): Promise<A> =>
    released
      ? Promise.reject(new Error("The Claude query has ended."))
      : runPromise(workerSession.call(message).pipe(Effect.onExit(() => Effect.yieldNow))).then(
          (value) => value as A,
          (error: WorkerSessionError) => Promise.reject(workerCause(error)),
        );
  // Like the SDK's: disposing and returning never fail.
  const settle = (message: ClaudeAgentSdkWorkerRequest) =>
    released
      ? Effect.void
      : workerSession.call(message).pipe(Effect.ignore, Effect.ensuring(finishOutput));

  const query: ClaudeQueryControl = {
    setMcpServers: (servers) => request({ type: "setMcpServers", servers }),
    setModel: (model) => request({ type: "setModel", model }),
    applyFlagSettings: (settings) => request({ type: "applyFlagSettings", settings }),
    setPermissionMode: (mode) => request({ type: "setPermissionMode", mode }),
    stopTask: (taskId) => request({ type: "stopTask", taskId }),
    backgroundTasks: (toolUseId) => request({ type: "backgroundTasks", toolUseId }),
    interrupt: () => request({ type: "interrupt" }),
    // In-thread, frames the SDK has not read when it closes never reach a waiting reader.
    close: () => {
      if (released) return;
      cleanUp();
      workerSession.cutOutput();
      workerSession.notify({ type: "close" });
    },
    [Symbol.asyncDispose]: () => {
      cleanUp();
      return runPromise(settle({ type: "asyncDispose" }));
    },
  };

  // In-thread, the consumer races the SDK through the messages read before a callback. Here the
  // callback starts once the consumer has handled them all (as in-thread when the request comes in
  // its own read): at once when it waits for output, else at its next pull, with output split there.
  const ordered = Stream.transformPull(workerSession.output, (pull) =>
    Effect.sync(() => {
      let chunk: ReadonlyArray<SDKMessage> = [];
      let offset = 0;
      const waitForOutput = Effect.suspend(() => {
        consumerIdle = true;
        return pull;
      }).pipe(Effect.ensuring(Effect.sync(() => (consumerIdle = false))));
      return Effect.gen(function* () {
        consuming = true;
        while (true) {
          startDue();
          if (offset === chunk.length) {
            chunk = yield* waitForOutput;
            offset = 0;
            continue;
          }
          const next = due[0];
          const end =
            next === undefined
              ? chunk.length
              : Math.min(chunk.length, offset + next.after - handed);
          const taken = offset === 0 && end === chunk.length ? chunk : chunk.slice(offset, end);
          offset = end;
          handed += taken.length;
          if (Arr.isReadonlyArrayNonEmpty(taken)) return taken;
        }
      });
    }),
  );

  // Ending the in-thread stream returns the SDK's iterator, which closes the query: idle at once,
  // in the background while a read is pending.
  const messages = ordered.pipe(
    Stream.mapError((error) => input.toError(workerCause(error), "fromAsyncIterable")),
    Stream.onExit((exit) => {
      cleanUp();
      if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)) {
        if (!released) workerSession.notify({ type: "return" });
        return finishOutput;
      }
      return settle({ type: "return" });
    }),
  );

  return Option.some<StartedClaudeQuery<E>>({ query, messages });
});

import type {
  HookCallback,
  HookCallbackMatcher,
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
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
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
  claudeAgentSdkStageOnMain,
  toClaudeAgentSdkProcessError,
} from "../../provider/io/claudeAgentSdkProtocol.ts";
import * as ServerConfig from "../../config.ts";
import {
  type ProviderIoSession,
  type ProviderIoStageError,
  type ProviderIoWorkerError,
  ProviderIoWorkerPool,
} from "../../provider/io/ProviderIoWorkerPool.ts";

/** The pool Claude queries run on; undefined (the default) keeps them on the main thread. */
export const ClaudeAgentSdkWorkerPool = Context.Reference<
  ProviderIoWorkerPool["Service"] | undefined
>("t3/orchestration-v2/Adapters/ClaudeAgentSdkWorkerPool", { defaultValue: () => undefined });

/** The server's pool for Claude queries, when T3CODE_PROVIDER_IO_CLAUDE_SDK opts in. */
export const layerClaudeAgentSdkWorkerPool = Layer.effect(
  ClaudeAgentSdkWorkerPool,
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    if (config.providerIoClaudeSdk !== true) return undefined;
    return yield* ProviderIoWorkerPool;
  }),
);

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

export interface ClaudeAgentSdkWorkerSetup {
  readonly stageOptions: ClaudeAgentSdkStageOptions;
  /** Main's callbacks and hook functions as they are now, as the SDK captures them at initialize. */
  readonly callbacks: ClaudeAgentSdkMainCallbacks;
}

/**
 * The worker's share of the query options and main's callbacks, or undefined to stay in-thread: an
 * abort controller, options that are not plain data (a session store, an in-process MCP server, a
 * hook that is not a function), or no CLI path (the worker cannot find the SDK's CLI as main does).
 */
export function claudeAgentSdkWorkerSetup(
  options: ClaudeQueryOptions,
  mcpServers: Record<string, McpServerConfig> | undefined,
): ClaudeAgentSdkWorkerSetup | undefined {
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
  const read = hooks === undefined ? undefined : readHooks(hooks);
  if (read === null) return undefined;
  const names: Array<ClaudeAgentSdkWorkerCallback> = [];
  if (canUseTool !== undefined) names.push("canUseTool");
  if (onElicitation !== undefined) names.push("onElicitation");
  if (onUserDialog !== undefined) names.push("onUserDialog");
  return {
    stageOptions: {
      options: data,
      callbacks: names,
      ...(read === undefined ? {} : { hooks: read.shape }),
    },
    callbacks: {
      ...(canUseTool === undefined ? {} : { canUseTool }),
      ...(onElicitation === undefined ? {} : { onElicitation }),
      ...(onUserDialog === undefined ? {} : { onUserDialog }),
      ...(read === undefined ? {} : { hooks: read.functions }),
    },
  };
}

// The fields the SDK reads from each matcher, read once as it does (getters and all): their shape for
// the worker and a copy of the hook functions for main. Null when a matcher's hooks are not functions
// or its matcher or timeout is not plain data.
const readHooks = (
  hooks: NonNullable<ClaudeQueryOptions["hooks"]>,
): {
  readonly shape: ClaudeAgentSdkWorkerHooks;
  readonly functions: NonNullable<ClaudeQueryOptions["hooks"]>;
} | null => {
  const shape: Record<string, ClaudeAgentSdkWorkerHooks[keyof ClaudeAgentSdkWorkerHooks]> = {};
  const functions: Record<string, Array<HookCallbackMatcher>> = {};
  for (const [event, matchers] of Object.entries(hooks)) {
    if (!Array.isArray(matchers)) return null;
    const matcherShapes = [];
    const matcherFunctions: Array<HookCallbackMatcher> = [];
    for (const matcher of matchers as ReadonlyArray<unknown>) {
      if (typeof matcher !== "object" || matcher === null) return null;
      const { hooks: callbacks, matcher: pattern, timeout } = matcher as Record<string, unknown>;
      if (!Array.isArray(callbacks) || !isPlainData(pattern) || !isPlainData(timeout)) return null;
      if (!callbacks.every((callback) => typeof callback === "function")) return null;
      matcherShapes.push({
        ...(pattern === undefined ? {} : { matcher: pattern as string }),
        ...(timeout === undefined ? {} : { timeout: timeout as number }),
        hooks: callbacks.length,
      });
      matcherFunctions.push({ hooks: [...(callbacks as Array<HookCallback>)] });
    }
    shape[event] = matcherShapes;
    functions[event] = matcherFunctions;
  }
  return { shape, functions };
};

const callbackOf = (
  callbacks: ClaudeAgentSdkMainCallbacks,
  call: ClaudeAgentSdkCallbackCall,
): ((...args: ReadonlyArray<never>) => unknown) | undefined => {
  if (call.name !== "hook") return callbacks[call.name];
  const [event, matcher, index] = call.hook;
  return callbacks.hooks?.[event]?.[matcher]?.hooks[index];
};

// The SDK's response field for the answer, as it writes it to the CLI (canUseTool's spread with its
// tool use id), or undefined for a null answer, which it does not answer. Cloning the value instead
// would keep what JSON converts (toJSON, a Buffer) and fail on what JSON reads (a Proxy).
const answerJson = (call: ClaudeAgentSdkCallbackCall, value: unknown): string | undefined => {
  if (value === null && call.name !== "hook") return undefined;
  const response =
    call.name === "canUseTool"
      ? { ...(value as object), toolUseID: call.args[2].toolUseID }
      : value;
  return JSON.stringify({ response });
};

// The SDK's error field for a callback that throws, as it writes it (an Error's message whatever its
// type, else the error as a string), or undefined when it cannot and sends nothing. In-thread, a
// rejection whose string form throws is also an unhandled rejection in the SDK; here it is not.
const errorJson = (error: unknown): string | undefined => {
  try {
    return JSON.stringify({ error: error instanceof Error ? error.message : String(error) });
  } catch {
    return undefined;
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
// The SDK's close: end the CLI's input, SIGTERM after this grace, SIGKILL after the second wait.
const CLOSE_GRACE = Duration.seconds(2);
const CLOSE_KILL_AFTER = Duration.seconds(5);
// The SDK's own error for a call once its transport has closed.
const QUERY_ENDED = "ProcessTransport is not ready for writing";

/**
 * Runs `query()` on a provider I/O worker (none when the pool is off); main spawns and owns the CLI.
 * Called uninterruptibly like in-thread `query()`, so it can wait out the pool's 10 s handshake.
 */
// Untraced: the runner's open span already covers it.
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

  // What the SDK's close would do when its session can no longer run it.
  const stopChild = () => {
    const target = child;
    if (target === undefined || childDone) return;
    target.stdin.end();
    runFork(
      Effect.gen(function* () {
        yield* Effect.sleep(CLOSE_GRACE);
        if (childDone) return;
        target.kill("SIGTERM");
        yield* Effect.sleep(CLOSE_KILL_AFTER);
        if (!childDone) target.kill("SIGKILL");
      }),
    );
  };
  const isClose = (frame: Uint8Array | ClaudeAgentSdkWorkerRequest) =>
    !(frame instanceof Uint8Array) && frame.type === "close";

  const pump: Effect.Effect<void> = Effect.gen(function* () {
    const target = session;
    if (target === undefined) return;
    while (true) {
      const frame = frames.shift();
      if (frame === undefined) return;
      if (!(frame instanceof Uint8Array)) {
        if (!target.notify(frame) && isClose(frame)) stopChild();
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
        const closing = frames.some(isClose);
        frames.length = 0;
        bufferedBytes = 0;
        if (closing) stopChild();
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

  // A cancelled callback still answers, as in-thread. Nothing here may throw into the pool's handler.
  const runCallback = (id: number, call: ClaudeAgentSdkCallbackCall) => {
    const controller = callbacks.get(id);
    if (controller === undefined) return;
    let result: unknown;
    try {
      // The SDK puts the signal first.
      const options = { signal: controller.signal, ...call.args.at(-1) };
      const args: Array<unknown> = [...call.args.slice(0, -1), options];
      const callback = callbackOf(input.callbacks, call);
      if (callback === undefined) throw new Error(`${call.name} callback is not provided.`);
      result = callback(...(args as Array<never>));
    } catch (error) {
      result = Promise.reject(error);
    }
    const fail = (error: unknown) =>
      session?.notify({ type: "callbackError", id, json: errorJson(error) });
    void Promise.resolve(result).then(
      (value) => {
        if (!callbacks.delete(id)) return;
        let json: string | undefined;
        try {
          json = answerJson(call, value);
        } catch (error) {
          fail(error);
          return;
        }
        session?.notify({ type: "callbackResult", id, json });
      },
      (error: unknown) => {
        if (callbacks.delete(id)) fail(error);
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
        if (cleanedUp || callbacks.has(event.id)) return;
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
      stage: claudeAgentSdkStageOnMain,
      options: input.stageOptions,
      onFailure: (error: WorkerSessionError) => error,
      kill: Effect.sync(() => {
        cleanUp();
        if (child !== undefined && !childDone) child.kill("SIGKILL");
      }),
      killAfterEnd: true,
      onEvent,
    })
    .pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.mapError((error) => input.toError(workerCause(error), "query")),
      Effect.onError(() => Scope.close(scope, Exit.void)),
    );
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
    } catch {
      // The prompt queue ends by interruption only; either way the worker's prompts end here.
    }
    if (!released) workerSession.notify({ type: "promptEnd" });
  })();

  // A reply resumes its caller at once, but outputs wake the consumer through the scheduler; yielding
  // lets the consumer take the frames that came before the reply first, as it would in-thread.
  const request = <A>(message: ClaudeAgentSdkWorkerRequest): Promise<A> =>
    released
      ? Promise.reject(new Error(QUERY_ENDED))
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
    // After the stdout main has read, so the worker's SDK closes where in-thread it would have. A
    // control request the SDK reads in the same tick as an in-thread close may still start its
    // callback there; here a callback raised after close never starts.
    close: () => {
      if (released) return;
      cleanUp();
      enqueue({ type: "close" });
    },
    [Symbol.asyncDispose]: () => {
      cleanUp();
      return runPromise(settle({ type: "asyncDispose" }));
    },
  };

  // A callback starts once the consumer has handled the messages read before it (the in-thread order
  // when the request comes in its own read), output split there. Invariant: the message consumer must
  // never wait on a callback's result, or this deadlocks. A cancel that comes before the start leaves
  // the callback starting with an aborted signal, where in-thread it was running.
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

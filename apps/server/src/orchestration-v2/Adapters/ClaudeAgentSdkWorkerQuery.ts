import type {
  HookCallback,
  McpServerConfig,
  Options as ClaudeQueryOptions,
  Query as ClaudeQuery,
  SDKMessage,
  SDKUserMessage,
  SpawnedProcess as ClaudeSpawnedProcess,
  SpawnOptions as ClaudeSpawnOptions,
} from "@anthropic-ai/claude-agent-sdk";
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
  type ClaudeAgentSdkStageOptions,
  type ClaudeAgentSdkWorkerCallback,
  type ClaudeAgentSdkWorkerEvent,
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

/**
 * The worker's share of the query options, or undefined when the query must stay in-thread: an
 * abort controller, hooks other than T3's SubagentStart context, options that are not plain data
 * (a session store, an in-process MCP server), or no CLI path (the worker cannot find the SDK's
 * bundled CLI the way main does).
 */
export function claudeAgentSdkStageOptions(
  options: ClaudeQueryOptions,
  mcpServers: Record<string, McpServerConfig> | undefined,
  subagentStart: { readonly hook: HookCallback; readonly context: string },
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
  let subagentStartContext: string | undefined;
  if (hooks !== undefined) {
    const [matcher, ...otherMatchers] = hooks.SubagentStart ?? [];
    if (
      Object.keys(hooks).length !== 1 ||
      matcher === undefined ||
      otherMatchers.length > 0 ||
      matcher.matcher !== undefined ||
      matcher.timeout !== undefined ||
      matcher.hooks.length !== 1 ||
      matcher.hooks[0] !== subagentStart.hook
    ) {
      return undefined;
    }
    subagentStartContext = subagentStart.context;
  }
  const refuse: Array<ClaudeAgentSdkWorkerCallback> = [];
  if (canUseTool !== undefined) refuse.push("canUseTool");
  if (onElicitation !== undefined) refuse.push("onElicitation");
  if (onUserDialog !== undefined) refuse.push("onUserDialog");
  return {
    options: data,
    refuse,
    ...(subagentStartContext === undefined ? {} : { subagentStartContext }),
  };
}

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
  const refused = new Set<ClaudeAgentSdkWorkerCallback>();
  const frames: Array<Uint8Array | ClaudeAgentSdkWorkerRequest> = [];
  let session:
    | ProviderIoSession<SDKMessage, WorkerSessionError, ClaudeAgentSdkWorkerRequest, unknown>
    | undefined;
  let child: ClaudeSpawnedProcess | undefined;
  let childDone = false;
  let outputDone = false;
  let released = false;
  let promptIterator: AsyncIterator<SDKUserMessage> | undefined;
  let bufferedBytes = 0;
  let paused = false;
  let pumping = false;

  // The worker's SDK may still kill the CLI after its output ends, so the session lasts until both.
  const release = () => {
    if (released || !outputDone || (child !== undefined && !childDone)) return;
    released = true;
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
      case "refused":
        if (refused.has(event.callback)) return;
        refused.add(event.callback);
        runFork(
          Effect.logWarning("Claude provider I/O worker refused a callback", {
            callback: event.callback,
          }),
        );
        return;
    }
  };

  const opened = yield* input.pool
    .open({
      stage: claudeAgentSdkStageOnMain,
      options: input.stageOptions,
      onFailure: (error: WorkerSessionError) => error,
      kill: Effect.sync(() => {
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
    // After the stdout main has read, so the worker's SDK closes where in-thread it would have.
    close: () => {
      if (!released) enqueue({ type: "close" });
    },
    [Symbol.asyncDispose]: () => runPromise(settle({ type: "asyncDispose" })),
  };

  // Ending the in-thread stream returns the SDK's iterator, which closes the query: idle at once,
  // in the background while a read is pending.
  const messages = workerSession.output.pipe(
    Stream.mapError((error) => input.toError(workerCause(error), "fromAsyncIterable")),
    Stream.onExit((exit) => {
      if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)) {
        if (!released) workerSession.notify({ type: "return" });
        return finishOutput;
      }
      return settle({ type: "return" });
    }),
  );

  return Option.some<StartedClaudeQuery<E>>({ query, messages });
});

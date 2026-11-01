// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalDateInEffect:off globalTimers:off -- Mirrors NodeChildProcessSpawner: Node streams, and its exit poll on host timers.
import * as PlatformNodeSink from "@effect/platform-node-shared/NodeSink";
import * as PlatformNodeStream from "@effect/platform-node-shared/NodeStream";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Config from "effect/Config";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Predicate from "effect/Predicate";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import {
  ChildProcessSpawner,
  ExitCode,
  make as makeSpawner,
  makeHandle,
  ProcessId,
} from "effect/process/ChildProcessSpawner";
import * as NodeWorkerThreads from "node:worker_threads";
import * as NodeStream from "node:stream";

export const VCS_SPAWN_WORKER_ENV = "T3CODE_VCS_SPAWN_WORKER";

const STREAM_WINDOW_BYTES = 1024 * 1024;
const processGroupGraceMillis = 1_000;
const processGroupPollIntervalMillis = 10;

// Inline worker source also runs in bundles and single-executable builds without module files.
export const spawnWorkerSource = `
"use strict";
const { parentPort, workerData } = require("node:worker_threads");
const { spawn } = require("node:child_process");
const windowBytes = workerData.windowBytes;
const streamNames = ["stdout", "stderr"];
const children = new Map();
const post = (message, transfer) => parentPort.postMessage(message, transfer);
const errorFields = (error) => {
  if (!(error instanceof Error)) return { name: "Error", message: String(error) };
  const fields = { name: error.name, message: error.message, stack: error.stack };
  for (const key of ["code", "errno", "syscall", "path", "spawnargs"]) {
    if (error[key] !== undefined) fields[key] = error[key];
  }
  return fields;
};
const start = (message) => {
  const id = message.id;
  let child;
  try {
    child = spawn(message.command, message.args, message.options);
  } catch (error) {
    post({ t: "throw", id, error: errorFields(error) });
    return;
  }
  const entry = { child, inflight: [0, 0], paused: [false, false] };
  children.set(id, entry);
  child.on("spawn", () => post({ t: "spawn", id, pid: child.pid }));
  child.on("error", (error) => post({ t: "error", id, error: errorFields(error) }));
  child.on("exit", (code, signal) => post({ t: "exit", id, code, signal }));
  child.on("close", () => {
    children.delete(id);
    post({ t: "close", id });
  });
  child.stdin.on("error", (error) => post({ t: "stdin-error", id, error: errorFields(error) }));
  streamNames.forEach((name, s) => {
    const stream = child[name];
    stream.on("data", (chunk) => {
      const data = new Uint8Array(chunk.byteLength);
      data.set(chunk);
      entry.inflight[s] += data.byteLength;
      post({ t: "data", id, s, chunk: data }, [data.buffer]);
      if (entry.inflight[s] >= windowBytes && !entry.paused[s]) {
        entry.paused[s] = true;
        stream.pause();
      }
    });
    stream.on("end", () => post({ t: "end", id, s }));
    stream.on("error", (error) => post({ t: "stream-error", id, s, error: errorFields(error) }));
  });
};
const destroyedStdin = { name: "Error", message: "Cannot call write after a stream was destroyed", code: "ERR_STREAM_DESTROYED" };
parentPort.on("message", (message) => {
  const entry = children.get(message.id);
  switch (message.t) {
    case "spawn":
      start(message);
      return;
    case "stdin":
      if (entry === undefined) return post({ t: "stdin-done", id: message.id, seq: message.seq, error: destroyedStdin });
      entry.child.stdin.write(message.chunk, (error) =>
        post({ t: "stdin-done", id: message.id, seq: message.seq, error: error ? errorFields(error) : undefined }),
      );
      return;
    case "stdin-end":
      if (entry === undefined) return post({ t: "stdin-done", id: message.id, seq: message.seq, error: destroyedStdin });
      entry.child.stdin.end((error) =>
        post({ t: "stdin-done", id: message.id, seq: message.seq, error: error ? errorFields(error) : undefined }),
      );
      return;
    case "stdin-destroy":
      if (entry !== undefined) entry.child.stdin.destroy();
      return;
    case "kill": {
      if (entry === undefined) return post({ t: "kill-result", id: message.id, seq: message.seq, killed: false });
      let killed;
      try {
        killed = entry.child.kill(message.signal);
      } catch (error) {
        return post({ t: "kill-result", id: message.id, seq: message.seq, killed: false, error: errorFields(error) });
      }
      return post({ t: "kill-result", id: message.id, seq: message.seq, killed });
    }
    case "ack":
      if (entry === undefined) return;
      entry.inflight[message.s] -= message.bytes;
      if (entry.paused[message.s] && entry.inflight[message.s] < windowBytes) {
        entry.paused[message.s] = false;
        entry.child[streamNames[message.s]].resume();
      }
      return;
  }
});
`;

type ExitCodeWithSignal = readonly [code: number | null, signal: NodeJS.Signals | null];

interface SerializedError {
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
  readonly code?: unknown;
  readonly errno?: unknown;
  readonly syscall?: unknown;
  readonly path?: unknown;
  readonly spawnargs?: unknown;
}

type WorkerMessage =
  | { readonly t: "spawn"; readonly id: number; readonly pid: number }
  | {
      readonly t: "error" | "throw" | "stdin-error";
      readonly id: number;
      readonly error: SerializedError;
    }
  | {
      readonly t: "exit";
      readonly id: number;
      readonly code: number | null;
      readonly signal: NodeJS.Signals | null;
    }
  | { readonly t: "close"; readonly id: number }
  | { readonly t: "data"; readonly id: number; readonly s: 0 | 1; readonly chunk: Uint8Array }
  | { readonly t: "end"; readonly id: number; readonly s: 0 | 1 }
  | {
      readonly t: "stream-error";
      readonly id: number;
      readonly s: 0 | 1;
      readonly error: SerializedError;
    }
  | {
      readonly t: "stdin-done";
      readonly id: number;
      readonly seq: number;
      readonly error?: SerializedError;
    }
  | {
      readonly t: "kill-result";
      readonly id: number;
      readonly seq: number;
      readonly killed: boolean;
      readonly error?: SerializedError;
    };

const reviveError = (fields: SerializedError): Error => {
  const error =
    fields.name === "TypeError"
      ? new TypeError(fields.message)
      : fields.name === "RangeError"
        ? new RangeError(fields.message)
        : new Error(fields.message);
  if (error.name !== fields.name) error.name = fields.name;
  if (fields.stack !== undefined) error.stack = fields.stack;
  for (const key of ["code", "errno", "syscall", "path", "spawnargs"] as const) {
    if (fields[key] !== undefined) Object.assign(error, { [key]: fields[key] });
  }
  return error;
};

type KillOutcome =
  | { readonly _tag: "Sent"; readonly killed: boolean }
  | { readonly _tag: "Threw"; readonly error: Error };

interface RemoteChild {
  readonly id: number;
  pid: number | undefined;
  readonly exitSignal: Deferred.Deferred<ExitCodeWithSignal>;
  readonly exitListeners: Set<(code: number | null, signal: NodeJS.Signals | null) => void>;
  readonly stdout: NodeStream.PassThrough;
  readonly stderr: NodeStream.PassThrough;
  readonly stdin: NodeStream.Writable;
  readonly ended: [boolean, boolean];
  referenced: boolean;
  closed: boolean;
  killedOnStop: boolean;
}

interface SpawnCallbacks {
  readonly onSpawn: () => void;
  readonly onError: (error: Error) => void;
  readonly onThrow: (error: Error) => void;
}

interface Entry {
  readonly child: RemoteChild;
  readonly callbacks: SpawnCallbacks;
  spawned: boolean;
  holdsLoop: boolean;
  readonly unacked: [number, number];
  readonly stdinCallbacks: Map<number, (error?: Error | null) => void>;
  readonly killCallbacks: Map<number, (outcome: KillOutcome) => void>;
}

const WORKER_STOPPED_MESSAGE = "The child process spawner worker stopped";
const DESTROYED_STDIN: SerializedError = {
  name: "Error",
  message: "Cannot call write after a stream was destroyed",
  code: "ERR_STREAM_DESTROYED",
};

const makeWorkerHost = (options: { readonly source: string; readonly windowBytes: number }) => {
  let worker: NodeWorkerThreads.Worker | undefined;
  let failure: unknown;
  let failed = false;
  let closing = false;
  let nextId = 1;
  let nextSeq = 1;
  let holding = 0;
  const entries = new Map<number, Entry>();

  const post = (message: unknown, transfer?: ReadonlyArray<ArrayBuffer>) => {
    worker?.postMessage(message, transfer as Array<ArrayBuffer> | undefined);
  };

  // A child keeps the event loop alive as Node's handles do: until close, or until its
  // output ends once unreferenced.
  const updateHold = (entry: Entry) => {
    const { child } = entry;
    const holds = !child.closed && (child.referenced || !(child.ended[0] && child.ended[1]));
    if (holds === entry.holdsLoop) return;
    entry.holdsLoop = holds;
    holding += holds ? 1 : -1;
    if (holding === 0) worker?.unref();
    else if (holding === 1 && holds) worker?.ref();
  };

  const completeExit = (entry: Entry, code: number | null, signal: NodeJS.Signals | null) => {
    if (!Deferred.doneUnsafe(entry.child.exitSignal, Exit.succeed([code, signal] as const))) return;
    for (const listener of entry.child.exitListeners) listener(code, signal);
  };

  // A closed child stays known until the worker has answered its stdin writes and kills.
  const forgetIfSettled = (entry: Entry) => {
    if (entry.child.closed && entry.stdinCallbacks.size === 0 && entry.killCallbacks.size === 0) {
      entries.delete(entry.child.id);
    }
  };

  const remove = (entry: Entry) => {
    entry.child.closed = true;
    updateHold(entry);
    forgetIfSettled(entry);
  };

  const stop = (cause: unknown) => {
    if (failed) return;
    failed = true;
    failure = cause;
    const error = new Error(WORKER_STOPPED_MESSAGE, { cause });
    for (const entry of entries.values()) {
      const { child } = entry;
      if (!entry.spawned) {
        entry.callbacks.onError(error);
      } else if (!Deferred.isDoneUnsafe(child.exitSignal) && child.pid !== undefined) {
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch {
          // A child spawned with detached: false leads no process group.
          try {
            process.kill(child.pid, "SIGKILL");
          } catch {
            // Already gone.
          }
        }
        // Node cannot reap another thread's children: the zombie stays until the server exits.
        child.killedOnStop = true;
        completeExit(entry, null, "SIGKILL");
      }
      for (const [index, stream] of [child.stdout, child.stderr].entries()) {
        if (!child.ended[index]) stream.destroy(error);
      }
      child.stdin.destroy(error);
      const stdinCallbacks = [...entry.stdinCallbacks.values()];
      const killCallbacks = [...entry.killCallbacks.values()];
      entry.stdinCallbacks.clear();
      entry.killCallbacks.clear();
      for (const callback of stdinCallbacks) callback(error);
      for (const callback of killCallbacks) callback({ _tag: "Sent", killed: false });
      remove(entry);
    }
    void worker?.terminate();
    worker?.unref();
  };

  const onMessage = (message: WorkerMessage) => {
    const entry = entries.get(message.id);
    if (entry === undefined) return;
    const { child } = entry;
    switch (message.t) {
      case "spawn":
        entry.spawned = true;
        child.pid = message.pid;
        entry.callbacks.onSpawn();
        return;
      case "error":
        if (!entry.spawned) entry.callbacks.onError(reviveError(message.error));
        return;
      case "throw":
        entry.callbacks.onThrow(reviveError(message.error));
        remove(entry);
        return;
      case "exit":
        completeExit(entry, message.code, message.signal);
        return;
      case "close":
        remove(entry);
        return;
      case "data": {
        const stream = message.s === 0 ? child.stdout : child.stderr;
        if (stream.destroyed) return;
        const bytes = message.chunk.byteLength;
        stream.write(message.chunk, () => {
          entry.unacked[message.s] += bytes;
          if (entry.unacked[message.s] >= options.windowBytes / 4) {
            post({ t: "ack", id: child.id, s: message.s, bytes: entry.unacked[message.s] });
            entry.unacked[message.s] = 0;
          }
        });
        return;
      }
      case "end":
        child.ended[message.s] = true;
        (message.s === 0 ? child.stdout : child.stderr).end();
        updateHold(entry);
        return;
      case "stream-error":
        (message.s === 0 ? child.stdout : child.stderr).destroy(reviveError(message.error));
        return;
      case "stdin-error":
        child.stdin.destroy(reviveError(message.error));
        return;
      case "stdin-done": {
        const callback = entry.stdinCallbacks.get(message.seq);
        entry.stdinCallbacks.delete(message.seq);
        forgetIfSettled(entry);
        callback?.(message.error === undefined ? null : reviveError(message.error));
        return;
      }
      case "kill-result": {
        const callback = entry.killCallbacks.get(message.seq);
        entry.killCallbacks.delete(message.seq);
        forgetIfSettled(entry);
        callback?.(
          message.error === undefined
            ? { _tag: "Sent", killed: message.killed }
            : { _tag: "Threw", error: reviveError(message.error) },
        );
        return;
      }
    }
  };

  const ensureWorker = () => {
    if (worker !== undefined || failed) return;
    try {
      const started = new NodeWorkerThreads.Worker(options.source, {
        eval: true,
        name: "t3-child-process-spawner",
        workerData: { windowBytes: options.windowBytes },
        stdout: false,
        stderr: false,
      });
      started.on("message", onMessage);
      started.on("messageerror", stop);
      started.on("error", stop);
      started.on("exit", (code) => stop(new Error(`spawner worker exited with code ${code}`)));
      started.unref();
      worker = started;
    } catch (error) {
      stop(error);
    }
  };

  const spawn = (
    command: string,
    args: ReadonlyArray<string>,
    spawnOptions: Record<string, unknown>,
    callbacks: SpawnCallbacks,
  ): RemoteChild => {
    ensureWorker();
    const id = nextId++;
    const stdinCallbacks = new Map<number, (error?: Error | null) => void>();
    const sendStdin = (
      message: Record<string, unknown>,
      callback: (error?: Error | null) => void,
    ) => {
      if (failed) return callback(new Error(WORKER_STOPPED_MESSAGE, { cause: failure }));
      if (child.closed) return callback(reviveError(DESTROYED_STDIN));
      const seq = nextSeq++;
      stdinCallbacks.set(seq, callback);
      if (message.chunk instanceof Uint8Array) {
        post({ ...message, t: "stdin", id, seq }, [message.chunk.buffer as ArrayBuffer]);
      } else {
        post({ ...message, id, seq });
      }
    };
    const child: RemoteChild = {
      id,
      pid: undefined,
      exitSignal: Deferred.makeUnsafe<ExitCodeWithSignal>(),
      exitListeners: new Set(),
      stdout: new NodeStream.PassThrough(),
      stderr: new NodeStream.PassThrough(),
      stdin: new NodeStream.Writable({
        write(chunk: Buffer, _encoding, callback) {
          const copy = new Uint8Array(chunk.byteLength);
          copy.set(chunk);
          sendStdin({ chunk: copy }, callback);
        },
        final(callback) {
          sendStdin({ t: "stdin-end" }, callback);
        },
        destroy(error, callback) {
          if (!failed) post({ t: "stdin-destroy", id });
          callback(error);
        },
      }),
      ended: [false, false],
      referenced: true,
      closed: false,
      killedOnStop: false,
    };
    const entry: Entry = {
      child,
      callbacks,
      spawned: false,
      holdsLoop: false,
      unacked: [0, 0],
      stdinCallbacks,
      killCallbacks: new Map(),
    };
    // Errors this host raises on the streams must not crash the process before a consumer listens.
    for (const stream of [child.stdout, child.stderr, child.stdin]) stream.on("error", () => {});
    if (failed) {
      callbacks.onError(new Error(WORKER_STOPPED_MESSAGE, { cause: failure }));
      return child;
    }
    entries.set(id, entry);
    updateHold(entry);
    post({ t: "spawn", id, command, args, options: spawnOptions });
    return child;
  };

  // Node's ChildProcess.kill returns false once the child has exited.
  const kill = (
    child: RemoteChild,
    signal: NodeJS.Signals,
    callback: (outcome: KillOutcome) => void,
  ) => {
    const entry = entries.get(child.id);
    if (entry === undefined || failed || Deferred.isDoneUnsafe(child.exitSignal)) {
      return callback({ _tag: "Sent", killed: false });
    }
    const seq = nextSeq++;
    entry.killCallbacks.set(seq, callback);
    post({ t: "kill", id: child.id, seq, signal });
  };

  const setReferenced = (child: RemoteChild, referenced: boolean) => {
    child.referenced = referenced;
    const entry = entries.get(child.id);
    if (entry !== undefined) updateHold(entry);
  };

  return {
    /** Starts the worker on first use; false once it has failed or closed. */
    start: () => {
      ensureWorker();
      return !failed;
    },
    get closing() {
      return closing;
    },
    spawn,
    kill,
    setReferenced,
    terminate: () =>
      Effect.promise(async () => {
        const current = worker;
        closing = true;
        stop(new Error("spawner worker closed"));
        if (current !== undefined) await current.terminate();
      }),
  };
};

// Mirrors @effect/platform-node-shared's internal handleErrnoException, which the package does not export.
const toPlatformError = (
  method: string,
  error: NodeJS.ErrnoException,
  command: ChildProcess.StandardCommand,
): PlatformError.PlatformError => {
  const reason: PlatformError.SystemErrorTag =
    error.code === "ENOENT"
      ? "NotFound"
      : error.code === "EACCES"
        ? "PermissionDenied"
        : error.code === "EEXIST"
          ? "AlreadyExists"
          : error.code === "EISDIR" || error.code === "ENOTDIR" || error.code === "ELOOP"
            ? "BadResource"
            : error.code === "EBUSY"
              ? "Busy"
              : "Unknown";
  return PlatformError.systemError({
    _tag: reason,
    module: "ChildProcess",
    method,
    pathOrDescriptor: `${command.command} ${command.args.join(" ")}`,
    syscall: error.syscall,
    cause: error,
  });
};

const toError = (error: unknown): Error =>
  error instanceof globalThis.Error ? error : new globalThis.Error(String(error));

const handleInputErrors = (writable: NodeStream.Writable): void => {
  const onError = () => {};
  writable.on("error", onError);
  writable.once("close", () => writable.off("error", onError));
};

const outputStream = (option: ChildProcess.CommandOptions["stdout"]) =>
  Predicate.isObject(option) && !Sink.isSink(option) && "stream" in option ? option.stream : option;

/** The command shapes the worker runs: one process, piped stdio, no extra descriptors. */
const runsOnWorker = (command: ChildProcess.Command): command is ChildProcess.StandardCommand => {
  if (command._tag !== "StandardCommand") return false;
  const { options } = command;
  if (options.additionalFds !== undefined && Object.keys(options.additionalFds).length > 0) {
    return false;
  }
  const stdin =
    Predicate.isObject(options.stdin) &&
    !Stream.isStream(options.stdin) &&
    "stream" in options.stdin
      ? options.stdin.stream
      : options.stdin;
  if (stdin !== undefined && stdin !== "pipe" && !Stream.isStream(stdin)) return false;
  return [outputStream(options.stdout), outputStream(options.stderr)].every(
    (stream) => stream === undefined || stream === "pipe",
  );
};

/**
 * Starts child processes on a worker thread: Node's async `spawn` still forks and execs on the
 * calling thread, which costs the server's main thread up to a millisecond per Git command.
 * Handles keep `NodeChildProcessSpawner`'s semantics; other command shapes use `fallback`.
 */
export const make = Effect.fn("WorkerChildProcessSpawner.make")(function* (
  fallback: ChildProcessSpawner["Service"],
  options: { readonly workerSource?: string; readonly windowBytes?: number } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcess.Platform;
  const host = makeWorkerHost({
    source: options.workerSource ?? spawnWorkerSource,
    windowBytes: options.windowBytes ?? STREAM_WINDOW_BYTES,
  });
  yield* Effect.addFinalizer(() => host.terminate());
  let announcedFallback = false;

  const resolveWorkingDirectory = Effect.fnUntraced(function* (
    commandOptions: ChildProcess.CommandOptions,
  ) {
    if (Predicate.isUndefined(commandOptions.cwd)) return undefined;
    yield* fs.access(commandOptions.cwd);
    return path.resolve(commandOptions.cwd);
  });

  const resolveStdinOption = (
    commandOptions: ChildProcess.CommandOptions,
  ): ChildProcess.StdinConfig => {
    const defaultConfig: ChildProcess.StdinConfig = {
      stream: "pipe",
      encoding: "utf-8",
      endOnDone: true,
    };
    if (Predicate.isUndefined(commandOptions.stdin)) return defaultConfig;
    if (typeof commandOptions.stdin === "string" || Stream.isStream(commandOptions.stdin)) {
      return { ...defaultConfig, stream: commandOptions.stdin };
    }
    return {
      stream: commandOptions.stdin.stream,
      encoding: commandOptions.stdin.encoding ?? defaultConfig.encoding,
      endOnDone: commandOptions.stdin.endOnDone ?? defaultConfig.endOnDone,
    };
  };

  const spawnRemote = (
    command: ChildProcess.StandardCommand,
    spawnOptions: Record<string, unknown>,
  ) =>
    Effect.callback<RemoteChild, PlatformError.PlatformError>((resume) => {
      const child = host.spawn(command.command, command.args, spawnOptions, {
        onSpawn: () => resume(Effect.succeed(child)),
        onError: (error) => resume(Effect.fail(toPlatformError("spawn", error, command))),
        onThrow: (error) => resume(Effect.die(error)),
      });
      return Effect.sync(() => host.kill(child, "SIGTERM", () => {}));
    });

  const isProcessAlive = (child: RemoteChild): boolean => {
    if (child.killedOnStop) return false;
    if (!Deferred.isDoneUnsafe(child.exitSignal)) return true;
    try {
      process.kill(-child.pid!, 0);
      return true;
    } catch {
      return false;
    }
  };

  const killProcessGroup = (
    command: ChildProcess.StandardCommand,
    child: RemoteChild,
    signal: NodeJS.Signals,
  ) =>
    Effect.try({
      try: () => {
        process.kill(-child.pid!, signal);
      },
      catch: (error) => toPlatformError("kill", toError(error), command),
    });

  const killProcess = (
    command: ChildProcess.StandardCommand,
    child: RemoteChild,
    signal: NodeJS.Signals,
  ) =>
    Effect.callback<void, PlatformError.PlatformError>((resume) => {
      host.kill(child, signal, (outcome) => {
        if (outcome._tag === "Threw") return resume(Effect.die(outcome.error));
        resume(
          outcome.killed
            ? Effect.void
            : Effect.fail(
                toPlatformError(
                  "kill",
                  new globalThis.Error("Failed to kill child process"),
                  command,
                ),
              ),
        );
      });
    });

  const awaitProcessExit = (child: RemoteChild, timeoutMillis: number): Effect.Effect<void> =>
    Effect.callback<void>((resume) => {
      const deadline = Date.now() + timeoutMillis;
      let timer: NodeJS.Timeout | undefined;
      const stop = () => {
        clearTimeout(timer);
        child.exitListeners.delete(poll);
      };
      const poll = () => {
        clearTimeout(timer);
        if (Date.now() >= deadline || !isProcessAlive(child)) {
          stop();
          resume(Effect.void);
          return;
        }
        timer = setTimeout(poll, processGroupPollIntervalMillis);
      };
      child.exitListeners.add(poll);
      poll();
      return Effect.sync(stop);
    });

  const terminateProcessGroup = Effect.fnUntraced(function* (
    command: ChildProcess.StandardCommand,
    child: RemoteChild,
    killOptions: ChildProcess.KillOptions | undefined,
  ) {
    const signalGroup = (signal: NodeJS.Signals) =>
      killProcessGroup(command, child, signal).pipe(
        Effect.catch(() => killProcess(command, child, signal)),
      );
    yield* signalGroup(killOptions?.killSignal ?? "SIGTERM");
    if (Predicate.isUndefined(killOptions?.forceKillAfter)) {
      yield* awaitProcessExit(child, processGroupGraceMillis);
    } else {
      yield* awaitProcessExit(child, Duration.toMillis(killOptions.forceKillAfter));
      if (isProcessAlive(child)) {
        yield* signalGroup("SIGKILL");
        yield* awaitProcessExit(child, processGroupGraceMillis);
      }
    }
    yield* Deferred.await(child.exitSignal);
  });

  const spawnOnWorker = Effect.fnUntraced(function* (command: ChildProcess.StandardCommand) {
    const stdinConfig = resolveStdinOption(command.options);
    let isReferenced = true;
    const cwd = yield* resolveWorkingDirectory(command.options);
    // Node reads process.env at spawn time when env is unset; the worker's copy can be stale.
    const env = command.options.extendEnv
      ? { ...globalThis.process.env, ...command.options.env }
      : (command.options.env ?? { ...globalThis.process.env });
    const detached = command.options.detached ?? platform !== "win32";
    const spawnOptions = {
      cwd,
      env,
      stdio: ["pipe", "pipe", "pipe"],
      detached,
      shell: command.options.shell,
      windowsHide: command.options.windowsHide ?? !detached,
    };

    const child = yield* Effect.acquireRelease(
      spawnRemote(command, spawnOptions),
      Effect.fnUntraced(function* (child) {
        const exited = yield* Deferred.isDone(child.exitSignal);
        if (exited) {
          const [code] = yield* Deferred.await(child.exitSignal);
          if (code !== 0 && Predicate.isNotNull(code)) {
            yield* Effect.ignore(terminateProcessGroup(command, child, command.options));
          } else if (isReferenced && platform !== "win32" && command.options.detached !== false) {
            yield* Effect.ignore(terminateProcessGroup(command, child, command.options));
          }
          return;
        }
        if (!isReferenced) return;
        yield* Effect.ignore(terminateProcessGroup(command, child, command.options));
      }),
    );

    child.exitListeners.add((code) => {
      if (code !== 0 && Predicate.isNotNull(code)) {
        try {
          process.kill(-child.pid!, command.options.killSignal ?? "SIGTERM");
        } catch {
          // Best-effort cleanup, as NodeChildProcessSpawner does.
        }
      }
    });
    const reref = Effect.sync(() => {
      if (!isReferenced) {
        host.setReferenced(child, true);
        isReferenced = true;
      }
    });
    const unref = Effect.sync(() => {
      if (isReferenced) {
        host.setReferenced(child, false);
        isReferenced = false;
      }
      return reref;
    });

    handleInputErrors(child.stdin);
    const stdinSink: Sink.Sink<void, unknown, never, PlatformError.PlatformError> =
      PlatformNodeSink.fromWritable({
        evaluate: () => child.stdin,
        onError: (error) => toPlatformError("fromWritable(stdin)", toError(error), command),
        endOnDone: stdinConfig.endOnDone,
        encoding: stdinConfig.encoding,
      });
    if (Stream.isStream(stdinConfig.stream)) {
      yield* Effect.forkScoped(Stream.run(stdinConfig.stream, stdinSink));
    }
    const stdout = PlatformNodeStream.fromReadable<Uint8Array, PlatformError.PlatformError>({
      evaluate: () => child.stdout,
      onError: (error) => toPlatformError("fromReadable(stdout)", toError(error), command),
    });
    const stderr = PlatformNodeStream.fromReadable<Uint8Array, PlatformError.PlatformError>({
      evaluate: () => child.stderr,
      onError: (error) => toPlatformError("fromReadable(stderr)", toError(error), command),
    });
    const exitCode = Effect.flatMap(Deferred.await(child.exitSignal), ([code, signal]) => {
      if (Predicate.isNotNull(code)) return Effect.succeed(ExitCode(code));
      const error = new globalThis.Error(
        `Process interrupted due to receipt of signal: '${signal}'`,
      );
      return Effect.fail(toPlatformError("exitCode", error, command));
    });

    return makeHandle({
      pid: ProcessId(child.pid!),
      exitCode,
      isRunning: Effect.map(Deferred.isDone(child.exitSignal), (done) => !done),
      kill: (killOptions) => terminateProcessGroup(command, child, killOptions),
      stdin: stdinSink,
      stdout,
      stderr,
      all: Stream.merge(stdout, stderr),
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
      unref,
    });
  });

  return makeSpawner((command) =>
    Effect.gen(function* () {
      if (platform === "win32" || !runsOnWorker(command)) {
        return yield* fallback.spawn(command);
      }
      if (!host.start()) {
        if (!announcedFallback && !host.closing) {
          announcedFallback = true;
          yield* Effect.logWarning(
            "Child process spawner worker unavailable; spawning on the main thread",
          );
        }
        return yield* fallback.spawn(command);
      }
      return yield* spawnOnWorker(command);
    }),
  );
});

/**
 * Replaces the ambient ChildProcessSpawner with the worker-backed one when
 * T3CODE_VCS_SPAWN_WORKER is true; unset or false keeps it. The worker spawns real processes,
 * so an injected (fake) spawner then only sees the command shapes the worker does not run.
 */
export const layerFromConfig = Layer.effect(
  ChildProcessSpawner,
  Effect.gen(function* () {
    const fallback = yield* ChildProcessSpawner;
    const platform = yield* HostProcess.Platform;
    const enabled = yield* Config.Boolean(VCS_SPAWN_WORKER_ENV).pipe(
      Config.withDefault(false),
      Effect.orDie,
    );
    if (!enabled || platform === "win32") return fallback;
    return yield* make(fallback);
  }),
);

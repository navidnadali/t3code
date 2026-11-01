// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalDateInEffect:off globalTimers:off -- Mirrors NodeChildProcessSpawner: Node streams, and its exit poll on host timers.
import * as PlatformNodeSink from "@effect/platform-node-shared/NodeSink";
import * as PlatformNodeStream from "@effect/platform-node-shared/NodeStream";
import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";
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
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import {
  type ChildProcessHandle,
  ChildProcessSpawner,
  ExitCode,
  make as makeSpawner,
  makeHandle,
  ProcessId,
} from "effect/process/ChildProcessSpawner";
import * as NodeChildProcess from "node:child_process";
import * as NodeWorkerThreads from "node:worker_threads";
import * as NodeStream from "node:stream";

import { spawnerProcessSource, spawnWorkerSource } from "./spawnerSources.ts";

export { spawnerProcessSource, spawnWorkerSource };

export const VCS_SPAWN_WORKER_ENV = "T3CODE_VCS_SPAWN_WORKER";
export const PROCESS_SPAWNER_ENV = "T3CODE_PROCESS_SPAWNER";

const STREAM_WINDOW_BYTES = 1024 * 1024;
const processGroupGraceMillis = 1_000;
const processGroupPollIntervalMillis = 10;
const spawnerCloseGraceMillis = 1_000;

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
  readonly detached: boolean;
  referenced: boolean;
  closed: boolean;
  killedOnStop: boolean;
  stopError: Error | undefined;
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

/** Where spawn requests run: a worker thread or the spawner process. */
interface Transport {
  readonly post: (message: unknown, transfer?: ReadonlyArray<ArrayBuffer>) => void;
  readonly ref: () => void;
  readonly unref: () => void;
  readonly terminate: () => Promise<void>;
}

interface TransportEvents {
  readonly onMessage: (message: WorkerMessage) => void;
  readonly onStop: (cause: unknown) => void;
}

const WORKER_STOPPED_MESSAGE = "The child process spawner worker stopped";
const SPAWNER_STOPPED_MESSAGE = "The process spawner stopped";
const DESTROYED_STDIN: SerializedError = {
  name: "Error",
  message: "Cannot call write after a stream was destroyed",
  code: "ERR_STREAM_DESTROYED",
};

const makeRemoteHost = (options: {
  readonly open: (events: TransportEvents) => Transport;
  readonly windowBytes: number;
  readonly stoppedMessage: string;
  readonly closedMessage: string;
  /** Stops in a row, with no command started in between, after which the host stays down. */
  readonly restarts: number;
  readonly onStop?: (cause: unknown, restarting: boolean) => void;
}) => {
  let transport: Transport | undefined;
  let generation: object | undefined;
  let failure: unknown;
  let down = false;
  let closing = false;
  let stopsInARow = 0;
  let nextId = 1;
  let nextSeq = 1;
  let holding = 0;
  const entries = new Map<number, Entry>();

  const post = (message: unknown, transfer?: ReadonlyArray<ArrayBuffer>) => {
    transport?.post(message, transfer);
  };

  // A child keeps the event loop alive as Node's handles do: until close, or until its
  // output ends once unreferenced.
  const updateHold = (entry: Entry) => {
    const { child } = entry;
    const holds = !child.closed && (child.referenced || !(child.ended[0] && child.ended[1]));
    if (holds === entry.holdsLoop) return;
    entry.holdsLoop = holds;
    holding += holds ? 1 : -1;
    if (holding === 0) transport?.unref();
    else if (holding === 1 && holds) transport?.ref();
  };

  const completeExit = (entry: Entry, code: number | null, signal: NodeJS.Signals | null) => {
    if (!Deferred.doneUnsafe(entry.child.exitSignal, Exit.succeed([code, signal] as const))) return;
    for (const listener of entry.child.exitListeners) listener(code, signal);
  };

  // A closed child stays known until the transport has answered its stdin writes and kills.
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
    const stopped = transport;
    // Callers resume inside the callbacks below and may start commands on a restarted transport.
    const stoppedEntries = [...entries.values()];
    transport = undefined;
    generation = undefined;
    failure = cause;
    stopsInARow += 1;
    const restarting = !closing && stopsInARow <= options.restarts;
    if (!restarting) down = true;
    if (!closing) options.onStop?.(cause, restarting);
    const error = new Error(options.stoppedMessage, { cause });
    for (const { child } of stoppedEntries) child.stopError = error;
    for (const entry of stoppedEntries) {
      const { child } = entry;
      if (!entry.spawned) {
        entry.callbacks.onError(error);
      } else if (child.pid !== undefined) {
        const running = !Deferred.isDoneUnsafe(child.exitSignal);
        // Descendants can outlive an exited leader in its group; a child spawned with
        // detached: false leads no group, and once exited its PID may belong to another process.
        if (child.detached || running) {
          try {
            if (child.detached) signalProcessGroup(child.pid, "SIGKILL");
            // As signalProcessGroup checks: kill(0) would signal this server's own group.
            else if (child.pid > 1) process.kill(child.pid, "SIGKILL");
          } catch {
            // Already gone.
          }
        }
        // Node cannot reap another thread's children: a stopped worker's stay zombies until the
        // server exits (a stopped spawner process's pass to init).
        child.killedOnStop = true;
        if (running) completeExit(entry, null, "SIGKILL");
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
    if (stopped !== undefined) {
      void stopped.terminate();
      stopped.unref();
    }
  };

  const onMessage = (message: WorkerMessage) => {
    const entry = entries.get(message.id);
    if (entry === undefined) return;
    const { child } = entry;
    switch (message.t) {
      case "spawn":
        stopsInARow = 0;
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

  // Events of a transport that already stopped must not touch its successor.
  const ensureTransport = () => {
    if (transport !== undefined || down) return;
    const current = {};
    generation = current;
    try {
      const opened = options.open({
        onMessage: (message) => {
          if (generation === current) onMessage(message);
        },
        onStop: (cause) => {
          if (generation === current) stop(cause);
        },
      });
      if (generation === current) transport = opened;
      else void opened.terminate();
    } catch (error) {
      if (generation === current) stop(error);
    }
  };

  const spawn = (
    command: string,
    args: ReadonlyArray<string>,
    spawnOptions: Record<string, unknown>,
    callbacks: SpawnCallbacks,
  ): RemoteChild => {
    ensureTransport();
    const id = nextId++;
    const stdinCallbacks = new Map<number, (error?: Error | null) => void>();
    const sendStdin = (
      message: Record<string, unknown>,
      callback: (error?: Error | null) => void,
    ) => {
      if (child.stopError !== undefined) return callback(child.stopError);
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
          if (child.stopError === undefined) post({ t: "stdin-destroy", id });
          callback(error);
        },
      }),
      ended: [false, false],
      detached: spawnOptions.detached === true,
      referenced: true,
      closed: false,
      killedOnStop: false,
      stopError: undefined,
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
    if (transport === undefined) {
      child.stopError = new Error(options.stoppedMessage, { cause: failure });
      callbacks.onError(child.stopError);
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
    if (
      entry === undefined ||
      child.stopError !== undefined ||
      Deferred.isDoneUnsafe(child.exitSignal)
    ) {
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
    /** Starts the transport if needed; false once it is down for good or closed. */
    start: () => {
      ensureTransport();
      return transport !== undefined;
    },
    get closing() {
      return closing;
    },
    get failure() {
      return failure;
    },
    spawn,
    kill,
    setReferenced,
    terminate: () =>
      Effect.promise(async () => {
        const current = transport;
        closing = true;
        down = true;
        if (current !== undefined) stop(new Error(options.closedMessage));
        await current?.terminate();
      }),
  };
};

type RemoteHost = ReturnType<typeof makeRemoteHost>;

const openWorker =
  (source: string, windowBytes: number) =>
  (events: TransportEvents): Transport => {
    const worker = new NodeWorkerThreads.Worker(source, {
      eval: true,
      name: "t3-child-process-spawner",
      workerData: { windowBytes },
      stdout: false,
      stderr: false,
    });
    worker.on("message", events.onMessage);
    worker.on("messageerror", events.onStop);
    worker.on("error", events.onStop);
    worker.on("exit", (code) =>
      events.onStop(new Error(`spawner worker exited with code ${code}`)),
    );
    worker.unref();
    return {
      post: (message, transfer) =>
        worker.postMessage(message, transfer as Array<ArrayBuffer> | undefined),
      ref: () => worker.ref(),
      unref: () => worker.unref(),
      terminate: async () => {
        await worker.terminate();
      },
    };
  };

/** How the server starts its spawner process; single executables dispatch `__process-spawner`. */
export const spawnerProcessLaunch = (input: {
  readonly executable: string;
  readonly isExecutable: boolean;
  readonly environment: NodeJS.ProcessEnv;
  readonly windowBytes: number;
  readonly source?: string;
}) => {
  // Children get the server's environment with each request; the server's heap options are not the spawner's.
  const { NODE_OPTIONS: _nodeOptions, ...environment } = input.environment;
  return {
    command: input.executable,
    args: input.isExecutable
      ? ["__process-spawner", String(input.windowBytes)]
      : ["-e", input.source ?? spawnerProcessSource, String(input.windowBytes)],
    options: {
      stdio: ["ignore", "ignore", "inherit", "ipc"],
      serialization: "advanced",
      // Its own process group: signals for the server's group, such as Ctrl-C, pass it by.
      detached: true,
      env: { ...environment, ELECTRON_RUN_AS_NODE: "1" },
      windowsHide: true,
    } satisfies NodeChildProcess.SpawnOptions,
  };
};

const openSpawnerProcess =
  (launch: ReturnType<typeof spawnerProcessLaunch>) =>
  (events: TransportEvents): Transport => {
    const spawner = NodeChildProcess.spawn(launch.command, launch.args, launch.options);
    // A spawner that failed to start reports only an error, with a negative exitCode.
    const hasExited = () => spawner.exitCode !== null || spawner.signalCode !== null;
    const exited = new Promise<void>((resolve) => {
      const settle = () => {
        if (hasExited()) resolve();
      };
      spawner.on("exit", settle);
      spawner.on("error", settle);
    });
    spawner.on("message", events.onMessage);
    spawner.on("error", events.onStop);
    spawner.on("exit", (code, signal) =>
      events.onStop(new Error(`The process spawner exited with ${signal ?? `code ${code}`}`)),
    );
    // Without its channel the spawner can no longer answer; it exits on disconnect itself.
    spawner.on("disconnect", () => {
      if (!hasExited()) spawner.kill("SIGKILL");
    });
    spawner.unref();
    spawner.channel?.unref();
    let terminated: Promise<void> | undefined;
    return {
      post: (message) => {
        if (spawner.connected) spawner.send(message as NodeChildProcess.Serializable);
      },
      ref: () => {
        spawner.ref();
        spawner.channel?.ref();
      },
      unref: () => {
        spawner.unref();
        spawner.channel?.unref();
      },
      terminate: () =>
        (terminated ??= new Promise<void>((resolve) => {
          if (hasExited()) return resolve();
          const timer = setTimeout(() => spawner.kill("SIGKILL"), spawnerCloseGraceMillis);
          void exited.then(() => {
            clearTimeout(timer);
            resolve();
          });
          if (spawner.connected) spawner.disconnect();
          else spawner.kill("SIGKILL");
        })),
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

/** Handles keep `NodeChildProcessSpawner`'s semantics; other command shapes use `fallback`. */
const makeRemoteSpawner = Effect.fnUntraced(function* (
  fallback: ChildProcessSpawner["Service"],
  host: RemoteHost,
  unavailable: (
    command: ChildProcess.StandardCommand,
  ) => Effect.Effect<ChildProcessHandle, PlatformError.PlatformError, Scope.Scope>,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcess.Platform;

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
      signalProcessGroup(child.pid!, 0);
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
        signalProcessGroup(child.pid!, signal);
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
          signalProcessGroup(child.pid!, command.options.killSignal ?? "SIGTERM");
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
      if (!host.start()) return yield* unavailable(command);
      return yield* spawnOnWorker(command);
    }),
  );
});

/**
 * Starts child processes on a worker thread: Node's async `spawn` still forks and execs on the
 * calling thread, which costs the server's main thread up to a millisecond per Git command.
 */
export const make = Effect.fn("WorkerChildProcessSpawner.make")(function* (
  fallback: ChildProcessSpawner["Service"],
  options: { readonly workerSource?: string; readonly windowBytes?: number } = {},
) {
  const windowBytes = options.windowBytes ?? STREAM_WINDOW_BYTES;
  const host = makeRemoteHost({
    open: openWorker(options.workerSource ?? spawnWorkerSource, windowBytes),
    windowBytes,
    stoppedMessage: WORKER_STOPPED_MESSAGE,
    closedMessage: "spawner worker closed",
    restarts: 0,
  });
  yield* Effect.addFinalizer(() => host.terminate());
  let announcedFallback = false;
  return yield* makeRemoteSpawner(fallback, host, (command) =>
    Effect.gen(function* () {
      if (!announcedFallback && !host.closing) {
        announcedFallback = true;
        yield* Effect.logWarning(
          "Child process spawner worker unavailable; spawning on the main thread",
        );
      }
      return yield* fallback.spawn(command);
    }),
  );
});

/**
 * Starts child processes from a small spawner process, so the server itself never forks: on
 * Linux, Node forks and copies the page tables of the whole server for every command. A spawner
 * that stops is restarted once; if it stops again before starting a command, commands fail.
 */
export const makeProcessSpawner = Effect.fn("ProcessSpawner.make")(function* (
  fallback: ChildProcessSpawner["Service"],
  options: { readonly source?: string; readonly windowBytes?: number } = {},
) {
  const windowBytes = options.windowBytes ?? STREAM_WINDOW_BYTES;
  const launch = spawnerProcessLaunch({
    executable: yield* HostProcess.ExecutablePath,
    isExecutable: yield* HostProcess.IsExecutable,
    environment: yield* HostProcess.Environment,
    windowBytes,
    ...(options.source === undefined ? {} : { source: options.source }),
  });
  const runLog = Effect.runSyncExitWith(yield* Effect.context<never>());
  const host = makeRemoteHost({
    open: openSpawnerProcess(launch),
    windowBytes,
    stoppedMessage: SPAWNER_STOPPED_MESSAGE,
    closedMessage: "process spawner closed",
    restarts: 1,
    onStop: (cause, restarting) => {
      runLog(
        restarting
          ? Effect.logWarning("Process spawner stopped; the next command restarts it", cause)
          : Effect.logError(
              "Process spawner stopped again without starting a command; commands fail until the server restarts",
              cause,
            ),
      );
    },
  });
  yield* Effect.addFinalizer(() => host.terminate());
  // Started while the server is small: starting it is the one fork it costs.
  host.start();
  return yield* makeRemoteSpawner(fallback, host, (command) =>
    Effect.fail(
      toPlatformError(
        "spawn",
        new globalThis.Error(SPAWNER_STOPPED_MESSAGE, { cause: host.failure }),
        command,
      ),
    ),
  );
});

/**
 * Replaces the ambient ChildProcessSpawner: T3CODE_PROCESS_SPAWNER=child starts commands from a
 * spawner process, else T3CODE_VCS_SPAWN_WORKER=true from a worker thread; unset keeps it. Both
 * spawn real processes, so an injected (fake) spawner then only sees the command shapes they do not run.
 */
export const layerFromConfig = Layer.effect(
  ChildProcessSpawner,
  Effect.gen(function* () {
    const fallback = yield* ChildProcessSpawner;
    const platform = yield* HostProcess.Platform;
    const spawner = yield* Config.Literals(["default", "child"], PROCESS_SPAWNER_ENV).pipe(
      Config.withDefault("default"),
      Effect.orDie,
    );
    const worker = yield* Config.Boolean(VCS_SPAWN_WORKER_ENV).pipe(
      Config.withDefault(false),
      Effect.orDie,
    );
    if (platform === "win32") return fallback;
    if (spawner === "child") return yield* makeProcessSpawner(fallback);
    if (!worker) return fallback;
    return yield* make(fallback);
  }),
);

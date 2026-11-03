// @effect-diagnostics nodeBuiltinImport:off globalDate:off globalDateInEffect:off globalTimers:off -- Mirrors NodeChildProcessSpawner: Node streams, and its exit poll on host timers.
import * as PlatformNodeSink from "@effect/platform-node-shared/NodeSink";
import * as PlatformNodeStream from "@effect/platform-node-shared/NodeStream";
import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
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
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeWorkerThreads from "node:worker_threads";
import * as NodeStream from "node:stream";

import * as ProcessRunner from "../processRunner.ts";
import * as ServerConfig from "../config.ts";
import { HandoffProcess, type ProviderSpawnOptions } from "./handoffProcess.ts";
import { answerDeadlines, type SpawnerTimers } from "./spawnerDeadlines.ts";
import { registerSpawnerProcess } from "./spawnerProcesses.ts";
import {
  PROCESS_SPAWNER_EXECUTABLE,
  spawnerHandshake,
  spawnerProcessSource,
  spawnWorkerSource,
} from "./spawnerSources.ts";

export { PROCESS_SPAWNER_EXECUTABLE, spawnerProcessSource, spawnWorkerSource };
export type { SpawnerTimers };

export const VCS_SPAWN_WORKER_ENV = "T3CODE_VCS_SPAWN_WORKER";
export const PROCESS_SPAWNER_ENV = "T3CODE_PROCESS_SPAWNER";

// Unread output per stream before the child blocks: a per-stream bound, not tuned to load. Memory
// grows only with concurrent commands, which callers cap (VcsProcess and GitVcsDriver permits).
const STREAM_WINDOW_BYTES = 1024 * 1024;
// As NodeChildProcessSpawner: the wait after a group signal, polled.
const processGroupGraceMillis = 1_000;
const processGroupPollIntervalMillis = 10;
// Time the spawner gets at close to kill its children before it is killed.
const spawnerCloseGraceMillis = 1_000;
// A spawner answers its start-up and each start within milliseconds; whatever the load, a spawner
// silent for this long is broken, and the server stops it.
const spawnerAnswerMillis = 10_000;
// Each restart forks the server; a spawner that keeps stopping is a fault, whatever the load. Down,
// it is retried after 15 s, doubling while starts keep failing, up to 5 minutes.
export const spawnerRestartLimit = {
  restarts: 3,
  windowMillis: 10 * 60_000,
  coolDownMillis: 15_000,
  maxCoolDownMillis: 5 * 60_000,
};

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
  | {
      readonly t: "spawn" | "started";
      readonly id: number;
      readonly pid: number;
      readonly startTime?: string;
    }
  | { readonly t: "handoff" | "handoff-refused"; readonly id: number }
  | { readonly t: "handoff-failed"; readonly id: number; readonly error: SerializedError }
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

const isSerializedError = (error: unknown) =>
  Predicate.isObject(error) &&
  typeof (error as { name?: unknown }).name === "string" &&
  typeof (error as { message?: unknown }).message === "string";

const isStreamIndex = (s: unknown) => s === 0 || s === 1;

// kill(0) and kill(-1) reach this server's group or every process the user owns.
const isChildPid = (pid: unknown) => Number.isSafeInteger(pid) && (pid as number) > 1;

/** A message the host can act on; anything else means the transport is broken. */
const isWorkerMessage = (value: unknown): value is WorkerMessage => {
  if (!Predicate.isObject(value)) return false;
  const message = value as Record<string, unknown>;
  if (!Number.isSafeInteger(message.id)) return false;
  switch (message.t) {
    case "spawn":
    case "started":
      return (
        isChildPid(message.pid) &&
        (message.startTime === undefined || typeof message.startTime === "string")
      );
    case "handoff":
    case "handoff-refused":
      return true;
    case "handoff-failed":
      return isSerializedError(message.error);
    case "data":
      return isStreamIndex(message.s) && message.chunk instanceof Uint8Array;
    case "end":
      return isStreamIndex(message.s);
    case "stream-error":
      return isStreamIndex(message.s) && isSerializedError(message.error);
    case "error":
    case "throw":
    case "stdin-error":
      return isSerializedError(message.error);
    case "exit":
      return (
        (message.code === null || Number.isInteger(message.code)) &&
        (message.signal === null || typeof message.signal === "string")
      );
    case "close":
      return true;
    case "stdin-done":
      return (
        Number.isSafeInteger(message.seq) &&
        (message.error === undefined || isSerializedError(message.error))
      );
    case "kill-result":
      return (
        Number.isSafeInteger(message.seq) &&
        typeof message.killed === "boolean" &&
        (message.error === undefined || isSerializedError(message.error))
      );
    default:
      return false;
  }
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
  /** Its caller stopped waiting for the start: the child is killed as soon as its PID is known. */
  abandoned: boolean;
  cancelStartDeadline: (() => void) | undefined;
  holdsLoop: boolean;
  readonly unacked: [number, number];
  readonly stdinCallbacks: Map<number, (error?: Error | null) => void>;
  readonly killCallbacks: Map<number, (outcome: KillOutcome) => void>;
}

/** A child started by hand-off: the spawner forked it, the server holds its stdio sockets. */
export interface HandoffChild {
  readonly id: number;
  pid: number | undefined;
  /** /proc start time, read by the spawner before it could reap the child: with the PID, its identity. */
  startTime: string | undefined;
  /** The server's end of each handed-off descriptor; output sockets stay paused until read. */
  readonly sockets: Map<number, NodeNet.Socket>;
  readonly exitSignal: Deferred.Deferred<ExitCodeWithSignal>;
  readonly exitListeners: Set<(code: number | null, signal: NodeJS.Signals | null) => void>;
  /** Its spawner stopped while it ran: its exit is found by watching its PID and start time. */
  orphaned: boolean;
  referenced: boolean;
}

export interface HandoffCallbacks {
  readonly onSpawn: () => void;
  /** The command failed to start, as Node's spawner reports it (ENOENT and the like). */
  readonly onError: (error: Error) => void;
  /** Hand-off failed before the command started: nothing ran, start it another way. */
  readonly onUnavailable: (error: Error) => void;
}

interface HandoffEntry {
  readonly child: HandoffChild;
  readonly callbacks: HandoffCallbacks;
  readonly socketPath: string;
  readonly nonce: Uint8Array;
  readonly fds: ReadonlyArray<number>;
  spawned: boolean;
  settled: boolean;
  abandoned: boolean;
  closed: boolean;
  holdsLoop: boolean;
  cancelStartDeadline: (() => void) | undefined;
  readonly killCallbacks: Map<number, (outcome: KillOutcome) => void>;
}

// Unix socket paths are limited to 104 bytes on macOS and 108 on Linux, NUL included.
const MAX_SOCKET_PATH_BYTES = 100;
const ORPHAN_POLL_MILLIS = 1_000;

/**
 * Whether the process with this PID is still the one the spawner started: same /proc start time,
 * not a zombie. A PID that names another process (reused) or none means the child exited.
 */
export const isSameProcess = (pid: number, startTime: string): boolean => {
  let stat: string;
  try {
    stat = NodeFS.readFileSync(`/proc/${pid}/stat`, "utf8");
  } catch {
    return false;
  }
  const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
  return fields[0] !== "Z" && fields[0] !== "X" && fields[19] === startTime;
};

/** Where spawn requests run: a worker thread or the spawner process. */
interface Transport {
  readonly post: (message: unknown, transfer?: ReadonlyArray<ArrayBuffer>) => void;
  readonly ref: () => void;
  readonly unref: () => void;
  readonly terminate: () => Promise<void>;
}

interface TransportEvents {
  readonly onMessage: (message: unknown) => void;
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
  /**
   * Stops within a rolling window after which the host is down, started commands or not. With it,
   * a host that is down retries after a cool-down that doubles while failures continue.
   */
  readonly restartWindow?: {
    readonly restarts: number;
    readonly windowMillis: number;
    readonly coolDownMillis: number;
    readonly maxCoolDownMillis: number;
    readonly nowMillis: () => number;
  };
  readonly onStop?: (cause: unknown, restarting: boolean, retryInMillis?: number) => void;
  readonly onRetry?: () => void;
  /** Arms the deadline for the transport to answer a start; without it, a start may wait for ever. */
  readonly startDeadline?: (expire: () => void) => () => void;
  readonly startDeadlineMessage?: string;
  /** A connection to a hand-off socket did not present this server's header and was closed. */
  readonly onHandoffRefused?: () => void;
  /** Children started by hand-off kept running when the spawner stopped; then each exit. */
  readonly onHandoffOrphans?: (count: number) => void;
  readonly onHandoffOrphanExit?: () => void;
}) => {
  let transport: Transport | undefined;
  let generation: object | undefined;
  let failure: unknown;
  let down = false;
  let retryAt: number | undefined;
  let coolDown = 0;
  let closing = false;
  let stopsInARow = 0;
  const recentStops: Array<number> = [];
  let nextId = 1;
  let nextSeq = 1;
  let holding = 0;
  const entries = new Map<number, Entry>();
  const handoffs = new Map<number, HandoffEntry>();
  const orphans = new Set<HandoffChild>();
  let orphanTimer: ReturnType<typeof setInterval> | undefined;

  const post = (message: unknown, transfer?: ReadonlyArray<ArrayBuffer>) => {
    transport?.post(message, transfer);
  };

  // A child keeps the event loop alive as Node's handles do: until close, or until its
  // output ends once unreferenced; and while a stdin write or kill awaits its answer.
  const updateHold = (entry: Entry) => {
    const { child } = entry;
    const holds =
      entry.stdinCallbacks.size > 0 ||
      entry.killCallbacks.size > 0 ||
      (!child.closed && (child.referenced || !(child.ended[0] && child.ended[1])));
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
    updateHold(entry);
    if (entry.child.closed && entry.stdinCallbacks.size === 0 && entry.killCallbacks.size === 0) {
      entries.delete(entry.child.id);
    }
  };

  const remove = (entry: Entry) => {
    entry.child.closed = true;
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
    const window = options.restartWindow;
    const now = window?.nowMillis() ?? 0;
    if (window !== undefined) {
      while (recentStops.length > 0 && recentStops[0]! <= now - window.windowMillis) {
        recentStops.shift();
      }
      // A spawner that stayed up for a whole window starts its cool-downs over.
      if (recentStops.length === 0) coolDown = 0;
      recentStops.push(now);
    }
    const restarting =
      !closing &&
      stopsInARow <= options.restarts &&
      (window === undefined || recentStops.length <= window.restarts);
    if (!restarting) {
      down = true;
      if (window !== undefined && !closing) {
        coolDown =
          coolDown === 0 ? window.coolDownMillis : Math.min(coolDown * 2, window.maxCoolDownMillis);
        retryAt = now + coolDown;
      }
    }
    if (!closing) options.onStop?.(cause, restarting, retryAt === undefined ? undefined : coolDown);
    const error = new Error(options.stoppedMessage, { cause });
    for (const entry of stoppedEntries) {
      entry.child.stopError = error;
      entry.cancelStartDeadline?.();
    }
    for (const entry of stoppedEntries) {
      const { child } = entry;
      // A child whose start the transport reported but never acknowledged is killed too.
      if (child.pid !== undefined) {
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
      if (!entry.spawned) entry.callbacks.onError(error);
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
    // Hand-off: a running child's stdio are the server's, so it keeps running; one not yet
    // running never ran (the caller starts it another way), unless its start was reported.
    const stoppedHandoffs = [...handoffs.values()];
    handoffs.clear();
    const running: Array<HandoffChild> = [];
    for (const entry of stoppedHandoffs) {
      const { child } = entry;
      entry.cancelStartDeadline?.();
      if (entry.holdsLoop) {
        entry.holdsLoop = false;
        holding -= 1;
      }
      const killCallbacks = [...entry.killCallbacks.values()];
      entry.killCallbacks.clear();
      if (!entry.settled) {
        entry.settled = true;
        if (child.pid !== undefined) killHandoffGroup(child);
        destroySockets(child);
        entry.callbacks.onUnavailable(error);
      } else if (entry.spawned && !Deferred.isDoneUnsafe(child.exitSignal)) {
        running.push(child);
      }
      for (const callback of killCallbacks) callback(killOrphan(child, "SIGTERM", false));
    }
    adoptOrphans(running);
    if (stopped !== undefined) {
      void stopped.terminate();
      stopped.unref();
    }
  };

  const killAbandoned = (entry: Entry) => {
    const { child } = entry;
    if (child.pid === undefined || child.killedOnStop) return;
    child.killedOnStop = true;
    try {
      if (child.detached) signalProcessGroup(child.pid, "SIGKILL");
      else if (child.pid > 1) process.kill(child.pid, "SIGKILL");
    } catch {
      // Already gone.
    }
  };

  const updateHandoffHold = (entry: HandoffEntry) => {
    const { child } = entry;
    const holds =
      entry.killCallbacks.size > 0 ||
      !entry.settled ||
      (entry.spawned && child.referenced && !Deferred.isDoneUnsafe(child.exitSignal));
    if (holds === entry.holdsLoop) return;
    entry.holdsLoop = holds;
    holding += holds ? 1 : -1;
    if (holding === 0) transport?.unref();
    else if (holding === 1 && holds) transport?.ref();
  };

  const completeHandoffExit = (
    child: HandoffChild,
    code: number | null,
    signal: NodeJS.Signals | null,
  ) => {
    if (!Deferred.doneUnsafe(child.exitSignal, Exit.succeed([code, signal] as const))) return;
    for (const listener of child.exitListeners) listener(code, signal);
  };

  const forgetHandoff = (entry: HandoffEntry) => {
    updateHandoffHold(entry);
    if (entry.settled && entry.closed && entry.killCallbacks.size === 0) {
      handoffs.delete(entry.child.id);
    }
  };

  const destroySockets = (child: HandoffChild) => {
    for (const socket of child.sockets.values()) socket.destroy();
  };

  // Hand-off itself failed before the command ran: tell the spawner and the caller.
  const handoffUnavailable = (entry: HandoffEntry, error: Error) => {
    if (entry.settled) return;
    entry.settled = true;
    entry.closed = true;
    entry.cancelStartDeadline?.();
    post({ t: "handoff-cancel", id: entry.child.id });
    destroySockets(entry.child);
    entry.callbacks.onUnavailable(error);
    forgetHandoff(entry);
  };

  const killHandoffGroup = (child: HandoffChild) => {
    if (child.pid === undefined) return;
    try {
      signalProcessGroup(child.pid, "SIGKILL");
    } catch {
      // Already gone.
    }
  };

  const checkOrphans = () => {
    for (const child of orphans) {
      const alive =
        child.pid !== undefined &&
        child.startTime !== undefined &&
        isSameProcess(child.pid, child.startTime);
      if (alive) continue;
      orphans.delete(child);
      completeHandoffExit(child, null, null);
      options.onHandoffOrphanExit?.();
    }
    if (orphans.size === 0 && orphanTimer !== undefined) {
      clearInterval(orphanTimer);
      orphanTimer = undefined;
    }
  };

  const adoptOrphans = (children: ReadonlyArray<HandoffChild>) => {
    if (children.length === 0) return;
    for (const child of children) {
      child.orphaned = true;
      orphans.add(child);
    }
    options.onHandoffOrphans?.(children.length);
    if (orphanTimer === undefined) {
      orphanTimer = setInterval(checkOrphans, ORPHAN_POLL_MILLIS);
      orphanTimer.unref();
    }
    checkOrphans();
  };

  const connectHandoff = (entry: HandoffEntry) => {
    const { child } = entry;
    for (const fd of entry.fds) {
      const socket = NodeNet.connect({ path: entry.socketPath });
      child.sockets.set(fd, socket);
      // Output waits for its reader; stdin and held descriptors are never read.
      socket.pause();
      if (!child.referenced) socket.unref();
      socket.once("connect", () => {
        const header = new Uint8Array(entry.nonce.byteLength + 1);
        header.set(entry.nonce);
        header[entry.nonce.byteLength] = fd;
        socket.write(header);
      });
      socket.on("error", (error) => {
        if (!entry.spawned) handoffUnavailable(entry, error);
      });
    }
  };

  const onHandoffMessage = (entry: HandoffEntry, message: WorkerMessage) => {
    const { child } = entry;
    switch (message.t) {
      case "handoff":
        if (!entry.settled) connectHandoff(entry);
        return;
      case "handoff-refused":
        options.onHandoffRefused?.();
        return;
      case "handoff-failed":
        handoffUnavailable(entry, reviveError(message.error));
        return;
      case "started":
        child.pid ??= message.pid;
        if (entry.abandoned) killHandoffGroup(child);
        return;
      case "spawn":
        stopsInARow = 0;
        entry.cancelStartDeadline?.();
        child.pid = message.pid;
        child.startTime = message.startTime;
        if (entry.abandoned) {
          killHandoffGroup(child);
          destroySockets(child);
          return;
        }
        entry.spawned = true;
        entry.settled = true;
        entry.callbacks.onSpawn();
        updateHandoffHold(entry);
        return;
      case "error":
        entry.cancelStartDeadline?.();
        if (!entry.spawned && !entry.settled) {
          entry.settled = true;
          destroySockets(child);
          entry.callbacks.onError(reviveError(message.error));
        }
        forgetHandoff(entry);
        return;
      case "throw":
        entry.cancelStartDeadline?.();
        if (!entry.settled) {
          entry.settled = true;
          destroySockets(child);
          entry.callbacks.onError(reviveError(message.error));
        }
        entry.closed = true;
        forgetHandoff(entry);
        return;
      case "exit":
        completeHandoffExit(child, message.code, message.signal);
        updateHandoffHold(entry);
        return;
      case "close":
        entry.closed = true;
        forgetHandoff(entry);
        return;
      case "kill-result": {
        const callback = entry.killCallbacks.get(message.seq);
        entry.killCallbacks.delete(message.seq);
        forgetHandoff(entry);
        callback?.(
          message.error === undefined
            ? { _tag: "Sent", killed: message.killed }
            : { _tag: "Threw", error: reviveError(message.error) },
        );
        return;
      }
      default:
        // Hand-off output never passes through the spawner.
        return;
    }
  };

  const onMessage = (message: WorkerMessage) => {
    const handoff = handoffs.get(message.id);
    if (handoff !== undefined) return onHandoffMessage(handoff, message);
    const entry = entries.get(message.id);
    if (entry === undefined) return;
    const { child } = entry;
    switch (message.t) {
      case "started":
        child.pid ??= message.pid;
        if (entry.abandoned) killAbandoned(entry);
        return;
      case "spawn":
        stopsInARow = 0;
        entry.cancelStartDeadline?.();
        child.pid = message.pid;
        if (entry.abandoned) return killAbandoned(entry);
        entry.spawned = true;
        entry.callbacks.onSpawn();
        return;
      case "error":
        entry.cancelStartDeadline?.();
        if (!entry.spawned) entry.callbacks.onError(reviveError(message.error));
        return;
      case "throw":
        entry.cancelStartDeadline?.();
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
    if (transport !== undefined || closing) return;
    if (down) {
      const window = options.restartWindow;
      if (retryAt === undefined || window === undefined || window.nowMillis() < retryAt) return;
      down = false;
      retryAt = undefined;
      options.onRetry?.();
    }
    const current = {};
    generation = current;
    try {
      const opened = options.open({
        onMessage: (message) => {
          if (generation !== current) return;
          if (isWorkerMessage(message)) onMessage(message);
          else stop(new Error("The spawner sent a malformed message"));
        },
        onStop: (cause) => {
          if (generation === current) stop(cause);
        },
      });
      if (generation !== current) {
        void opened.terminate();
        return;
      }
      transport = opened;
      // Stopped children still holding the loop are released only after their callbacks ran.
      if (holding > 0) opened.ref();
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
      updateHold(entry);
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
      abandoned: false,
      cancelStartDeadline: undefined,
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
    // An unanswered start means the transport is not working: stop it, failing what waits on it.
    entry.cancelStartDeadline = options.startDeadline?.(() => {
      entry.cancelStartDeadline = undefined;
      if (entries.get(id) === entry && child.stopError === undefined && !entry.spawned) {
        stop(new Error(options.startDeadlineMessage ?? "The transport did not answer a start"));
      }
    });
    return child;
  };

  /**
   * Starts a command whose stdio `request.fds` are sockets the server holds: the spawner forks it
   * and reads none of its output. `spawnOptions.stdio` gives the other descriptors.
   */
  const spawnHandoff = (
    command: string,
    args: ReadonlyArray<string>,
    spawnOptions: Record<string, unknown>,
    request: { readonly dir: string; readonly fds: ReadonlyArray<number> },
    callbacks: HandoffCallbacks,
  ): HandoffChild => {
    ensureTransport();
    const id = nextId++;
    const child: HandoffChild = {
      id,
      pid: undefined,
      startTime: undefined,
      sockets: new Map(),
      exitSignal: Deferred.makeUnsafe<ExitCodeWithSignal>(),
      exitListeners: new Set(),
      orphaned: false,
      referenced: true,
    };
    const socketPath = NodePath.join(request.dir, NodeCrypto.randomBytes(12).toString("base64url"));
    if (transport === undefined) {
      callbacks.onUnavailable(new Error(options.stoppedMessage, { cause: failure }));
      return child;
    }
    if (Buffer.byteLength(socketPath) > MAX_SOCKET_PATH_BYTES) {
      callbacks.onUnavailable(new Error(`The hand-off socket path is too long: ${socketPath}`));
      return child;
    }
    const entry: HandoffEntry = {
      child,
      callbacks,
      socketPath,
      nonce: NodeCrypto.randomBytes(16),
      fds: request.fds,
      spawned: false,
      settled: false,
      abandoned: false,
      closed: false,
      holdsLoop: false,
      cancelStartDeadline: undefined,
      killCallbacks: new Map(),
    };
    handoffs.set(id, entry);
    updateHandoffHold(entry);
    post({
      t: "spawn",
      id,
      command,
      args,
      options: spawnOptions,
      handoff: { path: socketPath, nonce: entry.nonce, fds: request.fds },
    });
    entry.cancelStartDeadline = options.startDeadline?.(() => {
      entry.cancelStartDeadline = undefined;
      if (handoffs.get(id) === entry && !entry.settled) {
        stop(new Error(options.startDeadlineMessage ?? "The transport did not answer a start"));
      }
    });
    return child;
  };

  // A hand-off child without its spawner: signal it only while it is still the same process.
  const killOrphan = (child: HandoffChild, signal: NodeJS.Signals, send: boolean): KillOutcome => {
    if (
      !send ||
      child.pid === undefined ||
      child.startTime === undefined ||
      Deferred.isDoneUnsafe(child.exitSignal) ||
      !isSameProcess(child.pid, child.startTime)
    ) {
      return { _tag: "Sent", killed: false };
    }
    try {
      process.kill(child.pid, signal);
      return { _tag: "Sent", killed: true };
    } catch (error) {
      return { _tag: "Threw", error: error as Error };
    }
  };

  const killHandoff = (
    child: HandoffChild,
    signal: NodeJS.Signals,
    callback: (outcome: KillOutcome) => void,
  ) => {
    if (Deferred.isDoneUnsafe(child.exitSignal)) return callback({ _tag: "Sent", killed: false });
    if (child.orphaned) return callback(killOrphan(child, signal, true));
    const entry = handoffs.get(child.id);
    if (entry === undefined || !entry.spawned) return callback({ _tag: "Sent", killed: false });
    const seq = nextSeq++;
    entry.killCallbacks.set(seq, callback);
    updateHandoffHold(entry);
    post({ t: "kill", id: child.id, seq, signal });
  };

  const abandonHandoff = (child: HandoffChild) => {
    const entry = handoffs.get(child.id);
    if (entry === undefined || entry.settled) return;
    entry.abandoned = true;
    entry.settled = true;
    entry.cancelStartDeadline?.();
    post({ t: "handoff-cancel", id: child.id });
    killHandoffGroup(child);
    destroySockets(child);
    forgetHandoff(entry);
  };

  const setHandoffReferenced = (child: HandoffChild, referenced: boolean) => {
    child.referenced = referenced;
    for (const socket of child.sockets.values()) {
      if (referenced) socket.ref();
      else socket.unref();
    }
    const entry = handoffs.get(child.id);
    if (entry !== undefined) updateHandoffHold(entry);
  };

  // Its caller was interrupted before the start was answered.
  const abandon = (child: RemoteChild) => {
    const entry = entries.get(child.id);
    if (entry === undefined || entry.spawned) return;
    entry.abandoned = true;
    killAbandoned(entry);
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
    updateHold(entry);
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
    abandon,
    kill,
    setReferenced,
    spawnHandoff,
    abandonHandoff,
    killHandoff,
    setHandoffReferenced,
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

/** How the server starts its spawner process: single executables run the spawner executable beside them, or else `t3 __process-spawner`. */
export const spawnerProcessLaunch = (input: {
  readonly executable: string;
  readonly isExecutable: boolean;
  /** The spawner executable beside a single executable, when its build shipped one. */
  readonly spawnerExecutable?: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly windowBytes: number;
  readonly source?: string;
}) => {
  // Children get the server's environment with each request; the server's heap options are not the spawner's.
  const { NODE_OPTIONS: _nodeOptions, ...environment } = input.environment;
  const windowBytes = String(input.windowBytes);
  return {
    command: input.spawnerExecutable ?? input.executable,
    args:
      input.spawnerExecutable !== undefined
        ? [windowBytes]
        : input.isExecutable
          ? ["__process-spawner", windowBytes]
          : ["-e", input.source ?? spawnerProcessSource, windowBytes],
    options: {
      // Its stdout reports each child's PID as soon as it starts.
      stdio: ["ignore", "pipe", "inherit", "ipc"],
      serialization: "advanced",
      // Its own process group: signals for the server's group, such as Ctrl-C, pass it by.
      detached: true,
      env: { ...environment, ELECTRON_RUN_AS_NODE: "1" },
      windowsHide: true,
    } satisfies NodeChildProcess.SpawnOptions,
  };
};

const findSpawnerExecutable = Effect.fnUntraced(function* (executable: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const candidate = path.join(path.dirname(executable), PROCESS_SPAWNER_EXECUTABLE);
  const info = yield* fs.stat(candidate).pipe(Effect.option);
  return Option.isSome(info) && info.value.type === "File" ? candidate : undefined;
});

interface SpawnerHandshake {
  readonly expected: { readonly protocol: string; readonly build: string };
  /** Arms the deadline for the spawner's first message. */
  readonly deadline: (expire: () => void) => () => void;
  readonly onReady: () => void;
}

const openSpawnerProcess =
  (launch: ReturnType<typeof spawnerProcessLaunch>, handshake: SpawnerHandshake) =>
  (events: TransportEvents): Transport => {
    const spawner = NodeChildProcess.spawn(launch.command, launch.args, launch.options);
    const forgetSpawner =
      spawner.pid === undefined ? () => {} : registerSpawnerProcess(spawner.pid);
    // A spawner that failed to start reports only an error, with a negative exitCode.
    const hasExited = () => spawner.exitCode !== null || spawner.signalCode !== null;
    const exited = new Promise<void>((resolve) => {
      const settle = () => {
        if (hasExited()) resolve();
      };
      spawner.on("exit", settle);
      spawner.on("error", settle);
    });
    let closing = false;
    let exitCause: Error | undefined;
    let channelOpen = true;
    let startsOpen = true;
    // Stops once every reply and start report the spawner wrote before it died has been read.
    const stopWhenDrained = () => {
      if (exitCause !== undefined && !channelOpen && !startsOpen) events.onStop(stopCause());
    };
    // A spawner that ends itself kills its own group, itself included: its reason says why.
    const stopCause = () =>
      refused ??
      (exitReason === undefined
        ? exitCause
        : new Error(`The process spawner exited: ${exitReason}`));
    // Until it has said who it is, anything from the spawner other than that ends it.
    let answered = false;
    let refused: Error | undefined;
    const outbox: Array<unknown> = [];
    const refuse = (reason: string) => {
      if (answered || refused !== undefined) return;
      refused = new Error(`The process spawner ${reason}`);
      cancelHandshake();
      if (!hasExited()) spawner.kill("SIGKILL");
    };
    const cancelHandshake = handshake.deadline(() =>
      refuse(`did not answer within ${spawnerAnswerMillis / 1_000} s of starting`),
    );
    let partial = "";
    let exitReason: string | undefined;
    // Each child's PID as it starts, and the spawner's reason when it ends itself.
    const starts = spawner.stdout as NodeNet.Socket;
    starts.setEncoding("utf8");
    // Read apart from its replies, these may arrive before the handshake is: they wait for it.
    const early: Array<string> = [];
    const readLine = (line: string) => {
      if (!answered) return void early.push(line);
      if (line.startsWith("exit ")) {
        exitReason = line.slice("exit ".length);
        return;
      }
      const [id, pid] = line.split(" ").map(Number);
      events.onMessage({ t: "started", id, pid });
    };
    starts.on("data", (chunk: string) => {
      const lines = (partial + chunk).split("\n");
      partial = lines.pop() ?? "";
      for (const line of lines) readLine(line);
    });
    starts.on("close", () => {
      startsOpen = false;
      stopWhenDrained();
    });
    starts.unref();
    spawner.on("message", (message: unknown) => {
      if (answered) return events.onMessage(message);
      const { expected } = handshake;
      const ready = message as { t?: unknown; protocol?: unknown; build?: unknown } | null;
      if (ready?.t !== "ready") return refuse("answered with something other than its handshake");
      if (ready.protocol !== expected.protocol || ready.build !== expected.build) {
        return refuse(
          `speaks protocol ${String(ready.protocol)} of build ${String(ready.build)}, not ${expected.protocol} of ${expected.build}`,
        );
      }
      answered = true;
      cancelHandshake();
      handshake.onReady();
      for (const line of early.splice(0)) readLine(line);
      for (const queued of outbox.splice(0)) {
        if (spawner.connected) spawner.send(queued as NodeChildProcess.Serializable);
      }
    });
    spawner.on("error", (error) => {
      if (hasExited()) cancelHandshake();
      if (hasExited()) events.onStop(error);
      else if (!closing) spawner.kill("SIGKILL");
    });
    spawner.on("exit", (code, signal) => {
      cancelHandshake();
      forgetSpawner();
      // Launched detached, it led its own group; while members remain, no process can take its id.
      try {
        if (launch.options.detached) signalProcessGroup(spawner.pid!, "SIGKILL");
      } catch {
        // No member left.
      }
      exitCause = new Error(`The process spawner exited with ${signal ?? `code ${code}`}`);
      stopWhenDrained();
      // A descendant still holding one of its pipes must not delay the stop for long.
      setTimeout(() => events.onStop(stopCause()), spawnerCloseGraceMillis).unref();
    });
    // A spawner that closed its channel can no longer answer; one this host disconnects kills
    // its children first, including those whose start it has not reported yet.
    spawner.on("disconnect", () => {
      channelOpen = false;
      if (!closing && !hasExited()) spawner.kill("SIGKILL");
      stopWhenDrained();
    });
    spawner.unref();
    spawner.channel?.unref();
    let terminated: Promise<void> | undefined;
    return {
      // Nothing goes to a spawner before it has said who it is: one that is refused then has
      // started nothing the server would not know about.
      post: (message) => {
        if (!answered) return void outbox.push(message);
        if (spawner.connected) spawner.send(message as NodeChildProcess.Serializable);
      },
      // Held while a command runs, until its spawner's replies and start reports are drained.
      ref: () => {
        spawner.ref();
        spawner.channel?.ref();
        starts.ref();
      },
      unref: () => {
        spawner.unref();
        spawner.channel?.unref();
        starts.unref();
      },
      terminate: () =>
        (terminated ??= new Promise<void>((resolve) => {
          if (hasExited()) return resolve();
          const timer = setTimeout(() => spawner.kill("SIGKILL"), spawnerCloseGraceMillis);
          void exited.then(() => {
            clearTimeout(timer);
            resolve();
          });
          closing = true;
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

/** What a handle needs of a started child, wherever its stdio are. */
interface ChildView {
  readonly pid: number | undefined;
  readonly exitSignal: Deferred.Deferred<ExitCodeWithSignal>;
  readonly exitListeners: Set<(code: number | null, signal: NodeJS.Signals | null) => void>;
  readonly stdin: NodeStream.Writable;
  readonly stdout: NodeStream.Readable;
  readonly stderr: NodeStream.Readable;
  /** Killed when its transport stopped; a hand-off child never is. */
  readonly killedOnStop: boolean;
}

/** How children start and are signalled: relayed by a worker or spawner, or handed off. */
interface ChildBackend<C extends ChildView, E> {
  readonly start: (
    command: ChildProcess.StandardCommand,
    spawnOptions: Record<string, unknown>,
  ) => Effect.Effect<C, PlatformError.PlatformError | E>;
  readonly kill: (
    child: C,
    signal: NodeJS.Signals,
    callback: (outcome: KillOutcome) => void,
  ) => void;
  readonly setReferenced: (child: C, referenced: boolean) => void;
}

/** Handles for started children, whichever backend started them: the same kill and exit semantics. */
const makeChildHandles = Effect.fnUntraced(function* () {
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

  const isProcessAlive = (child: ChildView): boolean => {
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
    child: ChildView,
    signal: NodeJS.Signals,
  ) =>
    Effect.try({
      try: () => {
        signalProcessGroup(child.pid!, signal);
      },
      catch: (error) => toPlatformError("kill", toError(error), command),
    });

  const killProcess = <C extends ChildView>(
    command: ChildProcess.StandardCommand,
    backend: ChildBackend<C, unknown>,
    child: C,
    signal: NodeJS.Signals,
  ) =>
    Effect.callback<void, PlatformError.PlatformError>((resume) => {
      backend.kill(child, signal, (outcome) => {
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

  const awaitProcessExit = (child: ChildView, timeoutMillis: number): Effect.Effect<void> =>
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

  const terminateProcessGroup = Effect.fnUntraced(function* <C extends ChildView>(
    command: ChildProcess.StandardCommand,
    backend: ChildBackend<C, unknown>,
    child: C,
    killOptions: ChildProcess.KillOptions | undefined,
  ) {
    const signalGroup = (signal: NodeJS.Signals) =>
      killProcessGroup(command, child, signal).pipe(
        Effect.catch(() => killProcess(command, backend, child, signal)),
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

  const spawnWith = Effect.fnUntraced(function* <C extends ChildView, E>(
    command: ChildProcess.StandardCommand,
    backend: ChildBackend<C, E>,
  ) {
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
      // Interruptible, so a command's own timeout ends the wait for a start that is not answered.
      backend.start(command, spawnOptions).pipe(Effect.interruptible),
      Effect.fnUntraced(function* (child) {
        const exited = yield* Deferred.isDone(child.exitSignal);
        if (exited) {
          const [code] = yield* Deferred.await(child.exitSignal);
          if (code !== 0 && Predicate.isNotNull(code)) {
            yield* Effect.ignore(terminateProcessGroup(command, backend, child, command.options));
          } else if (isReferenced && platform !== "win32" && command.options.detached !== false) {
            yield* Effect.ignore(terminateProcessGroup(command, backend, child, command.options));
          }
          return;
        }
        if (!isReferenced) return;
        yield* Effect.ignore(terminateProcessGroup(command, backend, child, command.options));
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
        backend.setReferenced(child, true);
        isReferenced = true;
      }
    });
    const unref = Effect.sync(() => {
      if (isReferenced) {
        backend.setReferenced(child, false);
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
      kill: (killOptions) => terminateProcessGroup(command, backend, child, killOptions),
      stdin: stdinSink,
      stdout,
      stderr,
      all: Stream.merge(stdout, stderr),
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
      unref,
    });
  });

  return { platform, spawnWith };
});

/** Handles keep `NodeChildProcessSpawner`'s semantics; other command shapes use `fallback`. */
const makeRemoteSpawner = Effect.fnUntraced(function* (
  fallback: ChildProcessSpawner["Service"],
  host: RemoteHost,
  unavailable: (
    command: ChildProcess.StandardCommand,
  ) => Effect.Effect<ChildProcessHandle, PlatformError.PlatformError, Scope.Scope>,
) {
  const { platform, spawnWith } = yield* makeChildHandles();
  const relayed: ChildBackend<RemoteChild, never> = {
    start: (command, spawnOptions) =>
      Effect.callback<RemoteChild, PlatformError.PlatformError>((resume) => {
        const child = host.spawn(command.command, command.args, spawnOptions, {
          onSpawn: () => resume(Effect.succeed(child)),
          onError: (error) => resume(Effect.fail(toPlatformError("spawn", error, command))),
          onThrow: (error) => resume(Effect.die(error)),
        });
        return Effect.sync(() => host.abandon(child));
      }),
    kill: host.kill,
    setReferenced: host.setReferenced,
  };

  return makeSpawner((command) =>
    Effect.gen(function* () {
      if (platform === "win32" || !runsOnWorker(command)) {
        return yield* fallback.spawn(command);
      }
      if (!host.start()) return yield* unavailable(command);
      return yield* spawnWith(command, relayed);
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
 * Starts the commands given to it from a small spawner process, so the server does not fork for
 * them (on Linux each fork copies the whole server's page tables). A stopped spawner is restarted;
 * past `spawnerRestartLimit` it is down and commands fail until its cool-down has passed.
 */

interface ProcessSpawnerOptions {
  readonly source?: string;
  readonly windowBytes?: number;
  readonly timers?: SpawnerTimers;
}

export const makeProcessSpawner = Effect.fn("ProcessSpawner.make")(function* (
  fallback: ChildProcessSpawner["Service"],
  options: ProcessSpawnerOptions = {},
) {
  return (yield* makeProcessSpawnerWithHost(fallback, options)).spawner;
});

const makeProcessSpawnerWithHost = Effect.fnUntraced(function* (
  fallback: ChildProcessSpawner["Service"],
  options: ProcessSpawnerOptions = {},
) {
  const windowBytes = options.windowBytes ?? STREAM_WINDOW_BYTES;
  const executable = yield* HostProcess.ExecutablePath;
  const isExecutable = yield* HostProcess.IsExecutable;
  // A single executable would parse the whole server bundle to run the spawner (~4x the memory).
  const spawnerExecutable = isExecutable ? yield* findSpawnerExecutable(executable) : undefined;
  if (isExecutable && spawnerExecutable === undefined) {
    yield* Effect.logWarning(
      `No ${PROCESS_SPAWNER_EXECUTABLE} beside ${executable}; the process spawner runs the whole server bundle`,
    );
  }
  const environment = yield* HostProcess.Environment;
  const launchWith = (sibling: string | undefined) =>
    spawnerProcessLaunch({
      executable,
      isExecutable,
      ...(sibling === undefined ? {} : { spawnerExecutable: sibling }),
      environment,
      windowBytes,
      ...(options.source === undefined ? {} : { source: options.source }),
    });
  const builtIn = launchWith(undefined);
  let launch = spawnerExecutable === undefined ? builtIn : launchWith(spawnerExecutable);
  const runLog = Effect.runSyncExitWith(yield* Effect.context<never>());
  const clock = yield* Clock.Clock;
  const { schedule } = options.timers ?? answerDeadlines(spawnerAnswerMillis);
  let answered = false;
  const host = makeRemoteHost({
    open: (events) => {
      answered = false;
      return openSpawnerProcess(launch, {
        expected: spawnerHandshake,
        deadline: (expire) => schedule("handshake", expire),
        onReady: () => {
          answered = true;
        },
      })(events);
    },
    startDeadline: (expire) => schedule("start", expire),
    startDeadlineMessage: `The process spawner did not answer a start within ${spawnerAnswerMillis / 1_000} s`,
    windowBytes,
    stoppedMessage: SPAWNER_STOPPED_MESSAGE,
    closedMessage: "process spawner closed",
    restarts: 1,
    restartWindow: {
      ...spawnerRestartLimit,
      nowMillis: () => Number(clock.monotonicTimeNanosUnsafe() / 1_000_000n),
    },
    onStop: (cause, restarting, retryInMillis) => {
      // A spawner executable that never got as far as its handshake is not used again.
      if (launch !== builtIn && !answered) {
        launch = builtIn;
        runLog(
          Effect.logWarning(
            `${PROCESS_SPAWNER_EXECUTABLE} beside ${executable} did not start; the process spawner falls back to the CLI entry, which runs the whole server bundle`,
            cause,
          ),
        );
      }
      runLog(
        restarting
          ? Effect.logWarning("Process spawner stopped; the next command restarts it", cause)
          : Effect.logError(
              `Process spawner stopped again before starting a command, or more than ${spawnerRestartLimit.restarts} times in ${spawnerRestartLimit.windowMillis / 60_000} minutes; commands fail for ${Math.round((retryInMillis ?? 0) / 1_000)} s, then it is started again`,
              cause,
            ),
      );
    },
    onRetry: () => {
      runLog(Effect.logWarning("Process spawner starting again after its cool-down"));
    },
    onHandoffRefused: () => {
      runLog(
        Effect.logWarning(
          "A connection to a provider hand-off socket did not present this server's header; it was closed",
        ),
      );
    },
    onHandoffOrphans: (count) => {
      runLog(
        Effect.logWarning(
          `${count} provider process(es) keep running without the process spawner; their exits are found by watching their PIDs, with no exit status`,
        ),
      );
    },
    onHandoffOrphanExit: () => {
      runLog(Effect.logInfo("A provider process that outlived the process spawner exited"));
    },
  });
  yield* Effect.addFinalizer(() => host.terminate());
  // Started while the server is small: starting it is the one fork it costs.
  host.start();
  const spawner = yield* makeRemoteSpawner(fallback, host, (command) =>
    Effect.fail(
      toPlatformError(
        "spawn",
        new globalThis.Error(SPAWNER_STOPPED_MESSAGE, { cause: host.failure }),
        command,
      ),
    ),
  );
  return { spawner, host };
});

const processSpawnerChoices = new Map<string, "node" | "worker" | "child">([
  ["", "node"],
  ["default", "node"],
  ["0", "node"],
  ["false", "node"],
  ["off", "node"],
  ["no", "node"],
  ["worker", "worker"],
  ["child", "child"],
  ["1", "child"],
  ["true", "child"],
  ["on", "child"],
  ["yes", "child"],
]);

/**
 * Replaces the ambient ChildProcessSpawner. Unset (or child, 1, true) starts commands from a spawner
 * process; worker (or, while it is unset, T3CODE_VCS_SPAWN_WORKER=true) from a worker thread; default,
 * 0, false or off keep Node's spawner on the event loop, as Windows always does, and so does an
 * unknown value, which is logged. Off-main spawners run real processes even under a fake spawner.
 */
/** Node's spawner and, when the knobs chose the spawner process, its host (for hand-off). */
class SpawnerHost extends Context.Service<
  SpawnerHost,
  { readonly node: ChildProcessSpawner["Service"]; readonly host: RemoteHost | undefined }
>()("t3/process/workerChildProcessSpawner/SpawnerHost") {}

export const layerFromConfig = Layer.effectContext(
  Effect.gen(function* () {
    const fallback = yield* ChildProcessSpawner;
    const platform = yield* HostProcess.Platform;
    const value = yield* Config.String(PROCESS_SPAWNER_ENV).pipe(Config.option, Effect.orDie);
    const worker = yield* Config.Boolean(VCS_SPAWN_WORKER_ENV).pipe(
      Config.withDefault(false),
      Effect.orDie,
    );
    let spawner = Option.match(value, {
      onNone: () => (worker ? "worker" : "child"),
      onSome: (set) => processSpawnerChoices.get(set.trim().toLowerCase()),
    });
    if (spawner === undefined) {
      yield* Effect.logError(
        `${PROCESS_SPAWNER_ENV}="${Option.getOrElse(value, () => "")}" is not child, worker or default (on/off); keeping Node's spawner on the event loop`,
      );
      spawner = "node";
    }
    const withHost = (service: ChildProcessSpawner["Service"], host?: RemoteHost) =>
      Context.make(ChildProcessSpawner, service).pipe(
        Context.add(SpawnerHost, { node: fallback, host }),
      );
    if (platform === "win32" || spawner === "node") return withHost(fallback);
    if (spawner === "child") {
      const started = yield* makeProcessSpawnerWithHost(fallback);
      return withHost(started.spawner, started.host);
    }
    return withHost(yield* make(fallback));
  }),
);

/**
 * The server's ProcessRunner, on the spawner the knobs choose. Layers are memoized by reference, so
 * every `ProcessRunner.layer` inside a layer this is provided to is this same runner.
 */
export const layerProcessRunner = ProcessRunner.layer.pipe(Layer.provide(layerFromConfig));

/** The server's spawner for commands that run to completion (checks, text generation). */
export class CommandSpawner extends Context.Service<
  CommandSpawner,
  ChildProcessSpawner["Service"]
>()("t3/process/workerChildProcessSpawner/CommandSpawner") {}

/** The spawner for a command that runs to completion: the server's, else the ambient one. */
export const commandSpawner = Effect.gen(function* () {
  const server = yield* Effect.serviceOption(CommandSpawner);
  return Option.isSome(server) ? server.value : yield* ChildProcessSpawner;
});

/** The server's CommandSpawner: the same spawner as `layerProcessRunner`'s (memoized). */
export const layerCommandSpawner = Layer.effect(
  CommandSpawner,
  Effect.service(ChildProcessSpawner),
).pipe(Layer.provide(layerFromConfig));

export const PROVIDER_SPAWNER_ENV = "T3CODE_PROVIDER_SPAWNER";

class HandoffUnavailableError extends Data.TaggedError("HandoffUnavailableError")<{
  readonly cause: Error;
}> {}

interface HandoffView extends ChildView {
  readonly child: HandoffChild;
}

const handoffView = (child: HandoffChild): HandoffView => {
  const output = (fd: number) => child.sockets.get(fd) ?? NodeStream.Readable.from([]);
  return {
    child,
    get pid() {
      return child.pid;
    },
    exitSignal: child.exitSignal,
    exitListeners: child.exitListeners,
    stdin: child.sockets.get(0) ?? new NodeStream.PassThrough(),
    stdout: output(1),
    stderr: output(2),
    killedOnStop: false,
  };
};

/**
 * Starts long-lived processes by hand-off: the spawner process forks them with Unix sockets the
 * server holds as their stdio, so their I/O never passes through the spawner. A command it
 * cannot hand off runs on Node's spawner, as does one whose hand-off fails, with `warn`.
 */
const makeHandoffSpawner = Effect.fnUntraced(function* (
  node: ChildProcessSpawner["Service"],
  host: RemoteHost,
  dir: string,
  warn: (reason: Error) => Effect.Effect<void>,
) {
  const { platform, spawnWith } = yield* makeChildHandles();
  const handedOff: ChildBackend<HandoffView, HandoffUnavailableError> = {
    start: (command, spawnOptions) =>
      Effect.callback<HandoffView, PlatformError.PlatformError | HandoffUnavailableError>(
        (resume) => {
          const child = host.spawnHandoff(
            command.command,
            command.args,
            { ...spawnOptions, stdio: ["ignore", "ignore", "ignore"] },
            { dir, fds: [0, 1, 2] },
            {
              onSpawn: () => resume(Effect.succeed(handoffView(child))),
              onError: (error) => resume(Effect.fail(toPlatformError("spawn", error, command))),
              onUnavailable: (cause) => resume(Effect.fail(new HandoffUnavailableError({ cause }))),
            },
          );
          return Effect.sync(() => host.abandonHandoff(child));
        },
      ),
    kill: (view, signal, callback) => host.killHandoff(view.child, signal, callback),
    setReferenced: (view, referenced) => host.setHandoffReferenced(view.child, referenced),
  };
  return makeSpawner((command) =>
    Effect.gen(function* () {
      // Its own group: a spawner that stops ends its own group, never a running provider's.
      if (platform !== "linux" || !runsOnWorker(command) || command.options.detached === false) {
        return yield* node.spawn(command);
      }
      return yield* spawnWith(command, handedOff).pipe(
        Effect.catchTags({
          HandoffUnavailableError: (error) =>
            warn(error.cause).pipe(Effect.andThen(node.spawn(command))),
        }),
      );
    }),
  );
});

/** How the server starts long-lived provider processes (Claude CLI, Codex app-server, ACP agents). */
export class ProviderProcessLauncher extends Context.Service<
  ProviderProcessLauncher,
  {
    /** A Node-style child process, for an SDK that drives one (the Claude Agent SDK's spawn hook). */
    readonly spawn: (
      command: string,
      args: ReadonlyArray<string>,
      options: ProviderSpawnOptions,
    ) => NodeChildProcess.ChildProcess | HandoffProcess;
    /** For Effect consumers. */
    readonly spawner: ChildProcessSpawner["Service"];
  }
>()("t3/process/workerChildProcessSpawner/ProviderProcessLauncher") {}

const providerSpawnerChoices = new Map<string, "handoff" | "direct">([
  ["handoff", "handoff"],
  ["on", "handoff"],
  ["1", "handoff"],
  ["true", "handoff"],
  ["direct", "direct"],
  ["default", "direct"],
  ["off", "direct"],
  ["0", "direct"],
  ["false", "direct"],
]);

const directLauncher = (node: ChildProcessSpawner["Service"]) =>
  ProviderProcessLauncher.of({
    spawn: (command, args, options) =>
      NodeChildProcess.spawn(command, [...args], {
        cwd: options.cwd,
        env: options.env,
        signal: options.signal,
        stdio: [...options.stdio],
        detached: true,
      }),
    spawner: node,
  });

/**
 * Provider processes start by hand-off from the spawner process (Linux, when the process
 * spawner is on) unless `T3CODE_PROVIDER_SPAWNER` says direct: Node's spawner on the event loop,
 * as before. Hand-off sockets live in a directory only the server's user can enter.
 */
export const layerProviderLauncher = Layer.effect(
  ProviderProcessLauncher,
  Effect.gen(function* () {
    const { node, host } = yield* SpawnerHost;
    const platform = yield* HostProcess.Platform;
    const value = yield* Config.String(PROVIDER_SPAWNER_ENV).pipe(Config.option, Effect.orDie);
    const choice = Option.match(value, {
      onNone: () => "handoff" as const,
      onSome: (set) => providerSpawnerChoices.get(set.trim().toLowerCase()),
    });
    if (choice === undefined) {
      yield* Effect.logError(
        `${PROVIDER_SPAWNER_ENV}="${Option.getOrElse(value, () => "")}" is not handoff or direct; provider processes start on Node's spawner`,
      );
    }
    if (choice !== "handoff" || host === undefined || platform !== "linux") {
      return directLauncher(node);
    }
    const { stateDir } = yield* ServerConfig.ServerConfig;
    const dir = NodePath.join(stateDir, "spawner");
    const prepared = yield* Effect.try(() => {
      NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
      NodeFS.chmodSync(dir, 0o700);
      const info = NodeFS.statSync(dir);
      if (!info.isDirectory() || info.uid !== globalThis.process.getuid?.()) {
        throw new Error(`${dir} is not a directory this user owns`);
      }
    }).pipe(Effect.option);
    if (Option.isNone(prepared)) {
      yield* Effect.logWarning(
        `Provider processes start on Node's spawner: the hand-off directory ${dir} is unusable`,
      );
      return directLauncher(node);
    }
    const runLog = Effect.runSyncExitWith(yield* Effect.context<never>());
    const warn = (reason: Error) =>
      Effect.logWarning(
        "Provider process hand-off failed; this one starts on Node's spawner",
        reason,
      );
    const spawner = yield* makeHandoffSpawner(node, host, dir, warn);
    return ProviderProcessLauncher.of({
      spawn: (command, args, options) =>
        new HandoffProcess(host, dir, command, args, options, (reason) => runLog(warn(reason))),
      spawner,
    });
  }),
).pipe(Layer.provide(layerFromConfig));

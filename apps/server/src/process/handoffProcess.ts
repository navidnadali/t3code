// @effect-diagnostics nodeBuiltinImport:off - A Node-style child process for SDKs that drive one directly.
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import type * as NodeNet from "node:net";
import * as NodeStream from "node:stream";

import { isPidReused } from "./processIdentity.ts";
import type { HandoffCallbacks, HandoffChild } from "./workerChildProcessSpawner.ts";

export interface ProviderSpawnOptions {
  readonly cwd?: string | undefined;
  readonly env: NodeJS.ProcessEnv;
  readonly signal?: AbortSignal | undefined;
  /**
   * stdin, stdout and stderr; piped ones become the server's sockets. Only these three: the child
   * gets them in blocking mode, as Node's own pipes, which a descriptor past 2 would not be.
   */
  readonly stdio: readonly ["pipe" | "ignore", "pipe" | "ignore", "pipe" | "ignore"];
  /** Piped descriptors the server only holds open (a watch pipe): never read, not in `stdio`. */
  readonly held?: ReadonlyArray<0 | 1 | 2>;
}

/** What a hand-off process needs of the spawner host. */
export interface HandoffHost {
  readonly spawnHandoff: (
    command: string,
    args: ReadonlyArray<string>,
    spawnOptions: Record<string, unknown>,
    request: { readonly fds: ReadonlyArray<number> },
    callbacks: HandoffCallbacks,
  ) => HandoffChild;
  readonly abandonHandoff: (child: HandoffChild) => void;
  readonly killHandoff: (
    child: HandoffChild,
    signal: NodeJS.Signals,
    callback: (outcome: unknown) => void,
  ) => void;
  readonly setHandoffReferenced: (child: HandoffChild, referenced: boolean) => void;
}

// Counts what the provider wrote, as a socket's bytesRead does (provider I/O stage timing reads it).
class OutputStream extends NodeStream.PassThrough {
  bytesRead = 0;
  override _transform(
    chunk: Buffer,
    encoding: BufferEncoding,
    callback: NodeStream.TransformCallback,
  ): void {
    this.bytesRead += chunk.byteLength;
    super._transform(chunk, encoding, callback);
  }
}

const abortError = () =>
  Object.assign(new Error("The operation was aborted"), { name: "AbortError", code: "ABORT_ERR" });

/**
 * A process as Node's spawner returns one, started by the spawner process with its piped
 * descriptors handed to the server. Its stdio exist at once and join the descriptors when the
 * start is answered; if hand-off is unavailable, it starts on Node's spawner instead.
 */
export class HandoffProcess extends NodeEvents.EventEmitter {
  readonly stdio: Array<NodeStream.PassThrough | null>;
  pid: number | undefined = undefined;
  exitCode: number | null = null;
  signalCode: NodeJS.Signals | null = null;
  killed = false;
  #child: HandoffChild | undefined;
  #direct: NodeChildProcess.ChildProcess | undefined;
  #pendingKill: NodeJS.Signals | undefined;
  #exited = false;
  #closed = false;
  #openOutputs = 0;
  readonly #held: Array<NodeNet.Socket> = [];
  #host: HandoffHost | undefined;

  constructor(
    host: HandoffHost,
    command: string,
    args: ReadonlyArray<string>,
    options: ProviderSpawnOptions,
    fallback: (reason: Error) => void,
  ) {
    super();
    const held = new Set<number>(options.held ?? []);
    this.stdio = options.stdio.map((mode, fd) =>
      mode !== "pipe" || held.has(fd)
        ? null
        : fd === 0
          ? new NodeStream.PassThrough()
          : new OutputStream(),
    );
    for (const [fd, stream] of this.stdio.entries()) {
      if (stream === null || fd === 0) continue;
      this.#openOutputs += 1;
      stream.once("end", () => {
        this.#openOutputs -= 1;
        this.#maybeClose();
      });
    }
    const fds = options.stdio.flatMap((mode, fd) => (mode === "pipe" ? [fd] : []));
    const placeholders = options.stdio.map(() => "ignore");
    const startDirect = (reason: Error) => {
      fallback(reason);
      this.#startDirect(command, args, options);
    };
    if (options.signal?.aborted) {
      queueMicrotask(() => this.#cancel());
      return;
    }
    options.signal?.addEventListener("abort", () => this.#abort(), { once: true });
    this.#host = host;
    this.#child = host.spawnHandoff(
      command,
      args,
      { cwd: options.cwd, env: options.env, stdio: placeholders, detached: true },
      { fds },
      {
        onSpawn: () => this.#joinHandedOff(host),
        onError: (error) => this.#fail(error),
        onUnavailable: (error) => startDirect(error),
      },
    );
  }

  /** The server's ends of held descriptors: they close when the server exits, however it exits. */
  get held(): ReadonlyArray<NodeNet.Socket> {
    return this.#held;
  }

  /**
   * Whether a signal to its PID's group still reaches only its own processes: false once that PID
   * names another process, as it can by the time an exit learned by watching is reported.
   */
  groupIsOwn(): boolean {
    const child = this.#child;
    if (this.#direct !== undefined || child?.pid === undefined || child.startTime === undefined) {
      return true;
    }
    return !isPidReused(child.pid, child.startTime);
  }

  get stdin(): NodeStream.PassThrough | null {
    return this.stdio[0] ?? null;
  }

  get stdout(): NodeStream.PassThrough | null {
    return this.stdio[1] ?? null;
  }

  get stderr(): NodeStream.PassThrough | null {
    return this.stdio[2] ?? null;
  }

  #joinHandedOff(host: HandoffHost) {
    const child = this.#child!;
    this.pid = child.pid;
    for (const [fd, socket] of child.sockets) {
      const stream = this.stdio[fd];
      if (stream === undefined || stream === null) {
        // Held for the child (a watch descriptor): never read or written here.
        socket.unref();
        socket.on("error", () => {});
        this.#held.push(socket);
      } else if (fd === 0) {
        socket.on("error", (error) => stream.destroy(error));
        socket.once("close", () => stream.destroy());
        stream.pipe(socket);
      } else {
        socket.on("error", (error) => stream.destroy(error));
        socket.pipe(stream);
        socket.resume();
      }
    }
    child.exitListeners.add((code, signal) => this.#exit(code, signal));
    this.emit("spawn");
    if (this.#pendingKill !== undefined) host.killHandoff(child, this.#pendingKill, () => {});
  }

  #startDirect(command: string, args: ReadonlyArray<string>, options: ProviderSpawnOptions) {
    let child: NodeChildProcess.ChildProcess;
    try {
      child = NodeChildProcess.spawn(command, [...args], {
        cwd: options.cwd,
        env: options.env,
        stdio: [...options.stdio],
        detached: true,
      });
    } catch (error) {
      this.#fail(error as Error);
      return;
    }
    this.#direct = child;
    this.pid = child.pid;
    // Its pipes' failures and closing reach the streams the SDK holds, as with a handed-off one.
    for (const [fd, stream] of this.stdio.entries()) {
      const pipe = child.stdio[fd];
      if (stream === null || pipe === null || pipe === undefined) continue;
      pipe.on("error", (error) => stream.destroy(error));
      if (fd === 0) {
        pipe.once("close", () => stream.destroy());
        stream.pipe(pipe as NodeStream.Writable);
      } else {
        (pipe as NodeStream.Readable).pipe(stream);
      }
    }
    child.on("spawn", () => this.emit("spawn"));
    child.on("error", (error) => this.#fail(error));
    child.on("exit", (code, signal) => this.#exit(code, signal));
    if (this.#pendingKill !== undefined) child.kill(this.#pendingKill);
  }

  #fail(error: Error) {
    this.emit("error", error);
    if (this.pid === undefined && !this.#exited) {
      // As Node's: a process that never started closes with no exit, its exit code the negative
      // errno of why it could not start (-2 for ENOENT, -13 for EACCES).
      this.#exited = true;
      const errno = (error as NodeJS.ErrnoException).errno;
      if (typeof errno === "number" && errno < 0) this.exitCode = errno;
      for (const stream of this.stdio) stream?.destroy();
      this.#openOutputs = 0;
      this.#maybeClose();
    }
  }

  #exit(code: number | null, signal: NodeJS.Signals | null) {
    if (this.#exited) return;
    this.#exited = true;
    this.exitCode = code;
    this.signalCode = signal;
    // As Node's: stdin closes when the process exits.
    this.stdin?.destroy();
    this.emit("exit", code, signal);
    // As Node's flushStdio: output nobody reads drains, so close follows.
    process.nextTick(() => {
      for (const stream of [this.stdout, this.stderr]) {
        if (stream?.readable && stream.listenerCount("readable") === 0) stream.resume();
      }
    });
    this.#maybeClose();
  }

  #maybeClose() {
    if (this.#closed || !this.#exited || this.#openOutputs > 0) return;
    this.#closed = true;
    // Its output has ended: the server's ends of its descriptors close with it.
    for (const socket of this.#child?.sockets.values() ?? []) socket.destroy();
    this.emit("close", this.exitCode, this.signalCode);
  }

  #abort() {
    if (this.#exited) return;
    // Not started yet (waiting its turn in the spawner, say).
    if (this.#direct === undefined && this.pid === undefined) return this.#cancel();
    this.kill("SIGTERM");
    this.emit("error", abortError());
  }

  /**
   * Aborted before it started: as the Effect path's cancel, its start is abandoned (and leaves the
   * spawner's queue), so it never runs. Its events are those of Node's, whose process the abort
   * kills at once: the abort error, then exit and close by SIGTERM.
   */
  #cancel() {
    if (this.#exited) return;
    this.killed = true;
    if (this.#child !== undefined) this.#host?.abandonHandoff(this.#child);
    this.emit("error", abortError());
    for (const stream of [this.stdout, this.stderr]) stream?.end();
    process.nextTick(() => this.#exit(null, "SIGTERM"));
  }

  kill(signal: NodeJS.Signals = "SIGTERM"): boolean {
    if (this.#exited) return false;
    this.killed = true;
    if (this.#direct !== undefined) return this.#direct.kill(signal);
    const child = this.#child;
    if (child === undefined || child.pid === undefined || this.pid === undefined) {
      this.#pendingKill = signal;
      return true;
    }
    this.#host?.killHandoff(child, signal, () => {});
    return true;
  }

  ref(): void {
    if (this.#direct !== undefined) this.#direct.ref();
    else if (this.#child !== undefined) this.#host?.setHandoffReferenced(this.#child, true);
  }

  unref(): void {
    if (this.#direct !== undefined) this.#direct.unref();
    else if (this.#child !== undefined) this.#host?.setHandoffReferenced(this.#child, false);
  }
}

// @effect-diagnostics nodeBuiltinImport:off - A Node-style child process for SDKs that drive one directly.
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import type * as NodeNet from "node:net";
import * as NodeStream from "node:stream";

import type { HandoffCallbacks, HandoffChild } from "./workerChildProcessSpawner.ts";

export interface ProviderSpawnOptions {
  readonly cwd?: string | undefined;
  readonly env: NodeJS.ProcessEnv;
  readonly signal?: AbortSignal | undefined;
  /** Piped descriptors become the server's sockets; the others are ignored. */
  readonly stdio: ReadonlyArray<"pipe" | "ignore">;
}

/** What a hand-off process needs of the spawner host. */
export interface HandoffHost {
  readonly spawnHandoff: (
    command: string,
    args: ReadonlyArray<string>,
    spawnOptions: Record<string, unknown>,
    request: { readonly dir: string; readonly fds: ReadonlyArray<number> },
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
    dir: string,
    command: string,
    args: ReadonlyArray<string>,
    options: ProviderSpawnOptions,
    fallback: (reason: Error) => void,
  ) {
    super();
    this.stdio = options.stdio.map((mode, fd) =>
      mode === "pipe" && fd <= 2 ? new NodeStream.PassThrough() : null,
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
      queueMicrotask(() => this.#fail(abortError()));
      return;
    }
    options.signal?.addEventListener("abort", () => this.#abort(), { once: true });
    this.#host = host;
    this.#child = host.spawnHandoff(
      command,
      args,
      { cwd: options.cwd, env: options.env, stdio: placeholders, detached: true },
      { dir, fds },
      {
        onSpawn: () => this.#joinHandedOff(host),
        onError: (error) => this.#fail(error),
        onUnavailable: (error) => startDirect(error),
      },
    );
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
    for (const [fd, stream] of this.stdio.entries()) {
      const pipe = child.stdio[fd];
      if (stream === null || pipe === null || pipe === undefined) continue;
      if (fd === 0) stream.pipe(pipe as NodeStream.Writable);
      else (pipe as NodeStream.Readable).pipe(stream);
    }
    child.on("spawn", () => this.emit("spawn"));
    child.on("error", (error) => this.#fail(error));
    child.on("exit", (code, signal) => this.#exit(code, signal));
    if (this.#pendingKill !== undefined) child.kill(this.#pendingKill);
  }

  #fail(error: Error) {
    this.emit("error", error);
    if (this.pid === undefined && !this.#exited) {
      // As Node's: a process that never started closes with no exit.
      this.#exited = true;
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
    this.emit("exit", code, signal);
    this.#maybeClose();
  }

  #maybeClose() {
    if (this.#closed || !this.#exited || this.#openOutputs > 0) return;
    this.#closed = true;
    for (const socket of this.#held) socket.destroy();
    this.emit("close", this.exitCode, this.signalCode);
  }

  #abort() {
    if (this.#exited) return;
    this.kill("SIGTERM");
    this.emit("error", abortError());
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

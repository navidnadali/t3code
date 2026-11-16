// @effect-diagnostics nodeBuiltinImport:off - An in-process stand-in for the CLI process the SDK drives.
import type { SpawnedProcess, SpawnOptions } from "@anthropic-ai/claude-agent-sdk";
import * as NodeEvents from "node:events";
import * as NodeReadline from "node:readline";
import * as NodeStream from "node:stream";

export type FakeClaudeCliFrame = Record<string, unknown> & { readonly type: string };

export type FakeClaudeCliAnswer = { readonly response: unknown } | { readonly error: string };

export interface FakeClaudeCliOptions {
  /** Answers the SDK's control requests; undefined falls back to the default answer. */
  readonly control?: (
    request: Record<string, unknown> & { readonly subtype: string },
    cli: FakeClaudeCli,
  ) => FakeClaudeCliAnswer | Promise<FakeClaudeCliAnswer> | undefined;
  readonly onUser?: (message: FakeClaudeCliFrame, cli: FakeClaudeCli) => void;
  /** The real CLI exits when its input ends and when it is killed; these turn that off. */
  readonly exitOnStdinEnd?: boolean;
  readonly exitOnKill?: boolean;
  /** Delivers stdout in a later macrotask, as a pipe does, instead of within the current one. */
  readonly pipeLatency?: boolean;
  /** With `pipeLatency`, starts a new read for each control request, after the pending one. */
  readonly requestsInOwnRead?: boolean;
}

const defaultAnswer = (request: Record<string, unknown> & { readonly subtype: string }) => {
  switch (request.subtype) {
    case "initialize":
      return {
        response: {
          commands: [],
          agents: [],
          models: [],
          output_style: "default",
          available_output_styles: ["default"],
          account: { email: "fake@example.invalid", subscriptionType: "max", tokenSource: "oauth" },
          pid: 4242,
        },
      };
    case "mcp_set_servers":
      return {
        response: {
          added: Object.keys((request.servers as Record<string, unknown> | undefined) ?? {}),
          removed: [],
          errors: {},
        },
      };
    case "background_tasks":
      return { response: { backgrounded: true } };
    default:
      return { response: {} };
  }
};

/** Speaks the CLI's stream-json protocol to the SDK over in-memory pipes. */
export class FakeClaudeCli extends NodeEvents.EventEmitter implements SpawnedProcess {
  readonly stdin = new NodeStream.PassThrough();
  readonly stdout = new NodeStream.PassThrough();
  killed = false;
  exitCode: number | null = null;
  signalCode: NodeJS.Signals | null = null;
  readonly frames: Array<FakeClaudeCliFrame> = [];
  readonly kills: Array<NodeJS.Signals> = [];
  spawnOptions: Omit<SpawnOptions, "signal"> | undefined;
  readonly stdinEnded: Promise<void>;
  readonly #options: FakeClaudeCliOptions;
  readonly #waiters = new Set<{
    readonly matches: (frame: FakeClaudeCliFrame) => boolean;
    readonly resolve: (frame: FakeClaudeCliFrame) => void;
  }>();
  readonly #pending = new Map<string, (answer: Record<string, unknown>) => void>();
  readonly #killWaiters = new Map<NodeJS.Signals, Array<() => void>>();
  #nextRequest = 0;
  // Reads not yet delivered, each sent in its own macrotask.
  readonly #unsent: Array<Array<string>> = [];

  constructor(options: FakeClaudeCliOptions = {}) {
    super();
    this.#options = options;
    this.stdinEnded = new Promise((resolve) => this.stdin.once("end", () => resolve()));
    NodeReadline.createInterface({ input: this.stdin, crlfDelay: Infinity }).on("line", (line) =>
      this.#receive(line),
    );
    this.stdin.once("end", () => {
      if (options.exitOnStdinEnd !== false) this.exit(0, null);
    });
  }

  /** The `spawnClaudeCodeProcess` hook that hands this CLI to the SDK. */
  readonly spawn = (options: SpawnOptions): SpawnedProcess => {
    const { signal, ...rest } = options;
    this.spawnOptions = rest;
    signal.addEventListener("abort", () => this.kill("SIGTERM"), { once: true });
    return this;
  };

  write(message: unknown, options: { readonly ownRead?: boolean } = {}) {
    const line = `${JSON.stringify(message)}\n`;
    if (this.#options.pipeLatency !== true) {
      this.stdout.write(line);
      return;
    }
    const pending = this.#unsent.at(-1);
    if (pending !== undefined && options.ownRead !== true) {
      pending.push(line);
      return;
    }
    this.#unsent.push([line]);
    setImmediate(() => this.#send());
  }

  #send() {
    const lines = this.#unsent.shift() ?? [];
    if (this.exitCode === null && this.signalCode === null) this.stdout.write(lines.join(""));
  }

  /** Sends a control request to the SDK and resolves with its control response. */
  request(request: Record<string, unknown>): Promise<Record<string, unknown>> {
    const requestId = `cli-${this.#nextRequest++}`;
    return new Promise((resolve) => {
      this.#pending.set(requestId, resolve);
      this.write(
        { type: "control_request", request_id: requestId, request },
        { ownRead: this.#options.requestsInOwnRead === true },
      );
    });
  }

  waitFor(matches: (frame: FakeClaudeCliFrame) => boolean): Promise<FakeClaudeCliFrame> {
    const seen = this.frames.find(matches);
    if (seen !== undefined) return Promise.resolve(seen);
    return new Promise((resolve) => this.#waiters.add({ matches, resolve }));
  }

  waitForControl(subtype: string) {
    return this.waitFor(
      (frame) =>
        frame.type === "control_request" &&
        (frame.request as { readonly subtype?: unknown } | undefined)?.subtype === subtype,
    );
  }

  waitForKill(signal: NodeJS.Signals): Promise<void> {
    if (this.kills.includes(signal)) return Promise.resolve();
    return new Promise((resolve) =>
      this.#killWaiters.set(signal, [...(this.#killWaiters.get(signal) ?? []), resolve]),
    );
  }

  kill(signal: NodeJS.Signals) {
    if (this.exitCode !== null || this.signalCode !== null) return false;
    this.killed = true;
    this.kills.push(signal);
    for (const resolve of this.#killWaiters.get(signal) ?? []) resolve();
    this.#killWaiters.delete(signal);
    if (this.#options.exitOnKill !== false) process.nextTick(() => this.exit(null, signal));
    return true;
  }

  exit(code: number | null, signal: NodeJS.Signals | null) {
    if (this.exitCode !== null || this.signalCode !== null) return;
    while (this.#unsent.length > 0) this.#send();
    this.exitCode = code;
    this.signalCode = signal;
    this.stdout.end();
    this.emit("exit", code, signal);
  }

  #receive(line: string) {
    if (line.trim() === "") return;
    const frame = JSON.parse(line) as FakeClaudeCliFrame;
    this.frames.push(frame);
    for (const waiter of this.#waiters) {
      if (!waiter.matches(frame)) continue;
      this.#waiters.delete(waiter);
      waiter.resolve(frame);
    }
    if (frame.type === "control_response") {
      const response = frame.response as Record<string, unknown>;
      const resolve = this.#pending.get(String(response.request_id));
      this.#pending.delete(String(response.request_id));
      resolve?.(response);
      return;
    }
    if (frame.type === "control_request") {
      const request = frame.request as Record<string, unknown> & { readonly subtype: string };
      void Promise.resolve(this.#options.control?.(request, this) ?? defaultAnswer(request)).then(
        (answer) =>
          this.write({
            type: "control_response",
            response:
              "error" in answer
                ? { subtype: "error", request_id: frame.request_id, error: answer.error }
                : { subtype: "success", request_id: frame.request_id, response: answer.response },
          }),
      );
      return;
    }
    if (frame.type === "user") this.#options.onUser?.(frame, this);
  }
}

/** Frames with each run's random request ids replaced by their order, for comparing runs. */
export function normalizedFrames(frames: ReadonlyArray<FakeClaudeCliFrame>) {
  const ids = new Map<string, string>();
  const idOf = (id: unknown) => {
    const key = String(id);
    let stable = ids.get(key);
    if (stable === undefined) {
      stable = `request-${ids.size}`;
      ids.set(key, stable);
    }
    return stable;
  };
  return frames.map((frame) => {
    if (frame.type === "control_request") return { ...frame, request_id: idOf(frame.request_id) };
    if (frame.type === "control_response") {
      const response = frame.response as Record<string, unknown>;
      return { ...frame, response: { ...response, request_id: idOf(response.request_id) } };
    }
    return frame;
  });
}

// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off preferSchemaOverJson:off cryptoRandomUUID:off - Wall-clock load generation outside Effect.
import * as NodeCrypto from "node:crypto";

import type { DriverOptions, PollCall } from "./options.ts";
import {
  CallError,
  FatalError,
  httpJson,
  httpRequest,
  type HttpTarget,
  McpSession,
  RpcSocket,
} from "./protocol.ts";
import { pickIndex, type Rng, weightedChoice } from "./sampling.ts";
import type { CallHandle, Recorder } from "./stats.ts";

/** The subset of `OrchestrationV2ThreadShell` the driver reads. */
export interface ThreadShell {
  readonly id: string;
  readonly projectId: string;
  readonly modelSelection: { readonly instanceId: string; readonly model: string };
  readonly runtimeMode: string;
  readonly interactionMode: string;
  readonly worktreePath: string | null;
  readonly lineage: {
    readonly relationshipToParent: "fork" | "subagent" | null;
    readonly sideChat?: boolean;
  };
  readonly latestRunId: string | null;
  readonly activeRunId: string | null;
  readonly status: string;
  readonly lastError?: string | null;
  readonly pendingRuntimeRequest: unknown;
  readonly itemCount: number;
  readonly archivedAt: string | null;
}

/** Turn items a provider writes; the user's message, checkpoints and run bookkeeping are not. */
const PROVIDER_ITEM_TYPES = new Set([
  "assistant_message",
  "reasoning",
  "command_execution",
  "dynamic_tool",
  "file_change",
  "file_search",
  "web_search",
  "todo_list",
  "proposed_plan",
  "subagent",
]);

interface DomainEvent {
  readonly type?: string;
  readonly occurredAt?: string;
  readonly payload?: { readonly type?: string; readonly role?: string };
}

/** A thread event carrying what the provider wrote: one of its turn items, or assistant text. */
export function isProviderOutputEvent(event: DomainEvent | undefined): boolean {
  if (event?.type === "turn-item.updated")
    return PROVIDER_ITEM_TYPES.has(event.payload?.type ?? "");
  return event?.type === "message.updated" && event.payload?.role === "assistant";
}

export interface ShellSnapshot {
  readonly snapshotSequence: number;
  readonly threads: ReadonlyArray<ThreadShell>;
  readonly archivedThreads?: ReadonlyArray<ThreadShell>;
  readonly projects: ReadonlyArray<{ readonly id: string; readonly workspaceRoot: string }>;
}

interface StreamItem {
  readonly kind?: string;
  readonly sequence?: number;
  readonly snapshotSequence?: number;
  readonly thread?: ThreadShell;
  readonly event?: DomainEvent;
  readonly snapshot?: ShellSnapshot;
}

export interface RunContext {
  readonly options: DriverOptions;
  readonly http: HttpTarget;
  readonly recorder: Recorder;
  readonly openSocket: () => Promise<RpcSocket>;
  /** Ends the run as failed; later calls are ignored. */
  readonly abort: (error: FatalError) => void;
  readonly isStopping: () => boolean;
}

export const now = () => performance.now();

export const newId = () => NodeCrypto.randomUUID();

/**
 * Times one call into `type`, attributed to the window it starts in. A fatal error aborts the
 * run; `ignore` marks failures that are not the server's (they are dropped); any other failure
 * is counted and yields undefined, or is rethrown when the caller cannot go on without it.
 */
export async function timed<T>(
  context: RunContext,
  type: string,
  call: () => Promise<T>,
  options: { readonly required?: boolean; readonly ignore?: (error: unknown) => boolean } = {},
): Promise<T | undefined> {
  const handle = context.recorder.begin(type);
  const started = now();
  try {
    const value = await call();
    context.recorder.succeed(handle, now() - started);
    return value;
  } catch (error) {
    if (error instanceof FatalError) {
      context.recorder.discard(handle);
      context.abort(error);
    } else if (context.isStopping() || options.ignore?.(error) === true) {
      context.recorder.discard(handle);
    } else {
      context.recorder.fail(handle, error instanceof Error ? error.message : String(error));
    }
    if (options.required === true) throw error;
    return undefined;
  }
}

function withTimeout<T>(promise: Promise<T>, milliseconds: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new CallError(`${what}: timed out`)), milliseconds);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

const TERMINAL_RUN_STATUSES = new Set([
  "completed",
  "interrupted",
  "failed",
  "cancelled",
  "rolled_back",
]);

const WORDS = [
  "measure",
  "the",
  "server",
  "under",
  "steady",
  "load",
  "and",
  "report",
  "what",
  "changed",
  "since",
  "last",
  "turn",
  "keep",
  "going",
];

export function messageText(rng: Rng, chars: number, label: string): string {
  let text = `${label}:`;
  while (text.length < chars) text += ` ${WORDS[pickIndex(rng, WORDS.length)]}`;
  return text.slice(0, chars);
}

/**
 * Follows every thread's shell on one socket. Sessions learn here that their turn ended, the way
 * a client's sidebar does, without opening a second stream per thread.
 */
export class ShellWatcher {
  private readonly threads = new Map<string, ThreadShell>();
  private readonly listeners = new Map<string, (shell: ThreadShell) => void>();

  seed(snapshot: ShellSnapshot): void {
    for (const thread of snapshot.threads) this.threads.set(thread.id, thread);
  }

  get(threadId: string): ThreadShell | undefined {
    return this.threads.get(threadId);
  }

  listen(threadId: string, listener: (shell: ThreadShell) => void): void {
    this.listeners.set(threadId, listener);
  }

  follow(context: RunContext, socket: RpcSocket, afterSequence: number): void {
    socket.stream(
      "orchestration.subscribeShell",
      { afterSequence, requestCompletionMarker: true },
      {
        onValues: (values) => {
          for (const value of values as ReadonlyArray<StreamItem>) {
            const threads =
              value.kind === "snapshot"
                ? (value.snapshot?.threads ?? [])
                : value.kind === "thread.updated" && value.thread !== undefined
                  ? [value.thread]
                  : [];
            for (const thread of threads) {
              this.threads.set(thread.id, thread);
              this.listeners.get(thread.id)?.(thread);
            }
          }
        },
        onEnd: (error) =>
          context.abort(
            new FatalError(`The sessions' shell stream ended: ${error?.message ?? "completed"}`),
          ),
      },
    );
  }
}

/** One agent session: sends a turn, waits for its run to settle, pauses, sends the next. */
export class Session {
  private turn = 0;
  private waiting: {
    readonly previousRunId: string | null;
    readonly startedAt: number;
    readonly handle: CallHandle;
  } | null = null;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private pendingModelSelection: { instanceId: string; model: string } | undefined;
  private outputInWindow = false;
  private stopWatching: (() => void) | undefined;
  private watchTimer: ReturnType<typeof setTimeout> | undefined;

  readonly index: number;
  readonly threadId: string;
  readonly selection: { readonly instanceId: string; readonly model: string };
  private readonly context: RunContext;
  private readonly socket: RpcSocket;
  private readonly watcher: ShellWatcher;
  private readonly rng: Rng;

  constructor(input: {
    readonly index: number;
    readonly threadId: string;
    readonly context: RunContext;
    readonly socket: RpcSocket;
    readonly watcher: ShellWatcher;
    readonly rng: Rng;
    /** The provider instance and model this session's turns run on. */
    readonly selection: { readonly instanceId: string; readonly model: string };
    /** History mode: the seeded thread ran elsewhere, so the first turn switches it. */
    readonly switchProvider: boolean;
  }) {
    this.index = input.index;
    this.threadId = input.threadId;
    this.context = input.context;
    this.socket = input.socket;
    this.watcher = input.watcher;
    this.rng = input.rng;
    this.selection = input.selection;
    const { watcher, threadId } = input;
    if (input.switchProvider) this.pendingModelSelection = { ...input.selection };
    watcher.listen(threadId, (shell) => this.onShell(shell));
  }

  /** Fresh mode: the launch is the first turn. */
  async launch(projectId: string, title: string): Promise<void> {
    const { options } = this.context;
    this.waiting = {
      previousRunId: null,
      startedAt: now(),
      handle: this.context.recorder.begin("session.turn"),
    };
    this.turn = 1;
    await timed(
      this.context,
      "session.launchThread",
      () =>
        this.socket.call(
          "orchestration.launchThread",
          {
            commandId: newId(),
            threadId: this.threadId,
            projectId,
            title,
            generateTitle: false,
            modelSelection: { ...this.selection },
            runtimeMode: options.runtimeMode,
            interactionMode: "default",
            workspaceStrategy: { type: "root" },
            initialMessage: { text: this.nextText(), attachments: [] },
          },
          options.callTimeoutMs,
        ),
      { required: true },
    );
  }

  /** History mode: the first turn continues the seeded thread. */
  start(): void {
    void this.dispatch();
  }

  private nextText(): string {
    return messageText(
      this.rng,
      this.context.options.messageChars,
      `Bench turn ${this.turn} of session ${this.index}`,
    );
  }

  private async dispatch(): Promise<void> {
    if (this.context.isStopping()) return;
    const { options } = this.context;
    this.turn++;
    const turn = {
      previousRunId: this.watcher.get(this.threadId)?.latestRunId ?? null,
      startedAt: now(),
      handle: this.context.recorder.begin("session.turn"),
    };
    this.waiting = turn;
    const messageId = newId();
    const sent = await timed(this.context, "session.dispatch", () =>
      this.socket.call(
        "orchestration.dispatchCommand",
        {
          type: "message.dispatch",
          commandId: newId(),
          threadId: this.threadId,
          messageId,
          text: this.nextText(),
          attachments: [],
          createdBy: "user",
          creationSource: "web",
          ...(this.pendingModelSelection === undefined
            ? {}
            : { modelSelection: this.pendingModelSelection }),
          dispatchMode: { type: "start_immediately" },
        },
        options.callTimeoutMs,
      ),
    );
    // A late failure (a timeout, say) can arrive after the shell already settled this turn.
    if (sent === undefined && this.waiting === turn) {
      this.context.recorder.discard(turn.handle);
      this.waiting = null;
      this.schedule(Math.max(options.turnGapMs, 1000));
      return;
    }
    this.pendingModelSelection = undefined;
  }

  private onShell(shell: ThreadShell): void {
    const waiting = this.waiting;
    if (waiting === null) return;
    if (shell.latestRunId === waiting.previousRunId || !TERMINAL_RUN_STATUSES.has(shell.status)) {
      return;
    }
    this.waiting = null;
    const { recorder } = this.context;
    if (shell.status === "completed") {
      recorder.succeed(waiting.handle, now() - waiting.startedAt);
    } else {
      recorder.fail(waiting.handle, `run ${shell.status}: ${shell.lastError ?? "no error text"}`);
    }
    this.schedule(this.context.options.turnGapMs);
  }

  private schedule(delayMs: number): void {
    if (this.context.isStopping()) return;
    this.timer = setTimeout(() => void this.dispatch(), delayMs);
  }

  get inFlight(): boolean {
    return this.waiting !== null;
  }

  /**
   * Follows the session's own thread, as the user who started it does. Provider output counts
   * only when it reaches this stream while the window is open, so nothing written before or
   * after the window can vouch for it.
   */
  watch(): void {
    const follow = (afterSequence: number | undefined) => {
      let last = afterSequence;
      this.stopWatching = this.socket.stream(
        "orchestration.subscribeThread",
        {
          threadId: this.threadId,
          ...(last === undefined ? {} : { afterSequence: last }),
          requestCompletionMarker: true,
          acceptBoundedSnapshot: true,
          acceptDeferredWork: true,
        },
        {
          onValues: (values) => {
            for (const value of values as ReadonlyArray<StreamItem>) {
              if (value.kind === "snapshot") last = value.snapshotSequence ?? last;
              if (value.kind !== "event") continue;
              last = value.sequence ?? last;
              if (this.context.recorder.isOpen && isProviderOutputEvent(value.event)) {
                this.outputInWindow = true;
                this.context.recorder.count("sessions.providerEvents");
              }
            }
          },
          onEnd: (error) => {
            if (this.context.isStopping()) return;
            this.context.recorder.error("session.watch", error?.message ?? "stream ended");
            this.watchTimer = setTimeout(() => follow(last), 250);
          },
        },
      );
    };
    follow(undefined);
  }

  windowOpened(): void {
    this.outputInWindow = false;
  }

  get producedOutput(): boolean {
    return this.outputInWindow;
  }

  /** Stops sending turns; with `stopThread`, also stops whatever the server is running there. */
  async stop(stopThread: boolean): Promise<void> {
    clearTimeout(this.timer);
    clearTimeout(this.watchTimer);
    this.stopWatching?.();
    if (!stopThread) return;
    await this.socket
      .call(
        "orchestration.dispatchCommand",
        { type: "thread.stop", commandId: newId(), threadId: this.threadId, reason: "bench end" },
        5_000,
      )
      .catch(() => undefined);
  }
}

interface BoundedSnapshot {
  readonly snapshotSequence: number;
}

/**
 * One client window: the shell stream for its sidebar plus one open thread, loaded the way the
 * web client loads it (HTTP snapshot, then the socket from that sequence).
 */
export class Subscriber {
  private socket: RpcSocket | undefined;
  private stopThread: (() => void) | undefined;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private generation = 0;
  private current: string | undefined;
  private syncHandle: CallHandle = null;
  /** Between asking for a thread and its stream synchronizing. */
  private loading = false;
  private switchWhenSynchronized = false;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private syncTimer: ReturnType<typeof setTimeout> | undefined;
  readonly touched = new Set<string>();

  readonly index: number;
  private readonly context: RunContext;
  private readonly pool: ReadonlyArray<string>;
  private readonly rng: Rng;

  constructor(index: number, context: RunContext, pool: ReadonlyArray<string>, rng: Rng) {
    this.index = index;
    this.context = context;
    this.pool = pool;
    this.rng = rng;
  }

  async open(): Promise<void> {
    const socket = await this.context.openSocket();
    this.socket = socket;
    await this.openShell(socket);
    await this.show(this.pool[this.index % this.pool.length] as string);
  }

  /** Moves to another thread at a fixed interval, starting at a seeded phase. */
  startSwitching(): void {
    const interval = this.context.options.switchIntervalMs;
    if (interval <= 0 || this.pool.length < 2) return;
    const tick = () => {
      if (this.context.isStopping()) return;
      this.requestSwitch();
      this.timer = setTimeout(tick, interval);
    };
    this.timer = setTimeout(tick, this.rng.next() * interval);
  }

  private async openShell(socket: RpcSocket): Promise<void> {
    const { recorder } = this.context;
    const snapshot = (await timed(
      this.context,
      "subscribe.httpShell",
      () =>
        httpJson<ShellSnapshot>(
          this.context.http,
          "/api/orchestration/shell?omitIdleSubagents=1&omitOlderSettled=1",
        ),
      { required: true },
    )) as { readonly value: ShellSnapshot; readonly bytes: number };
    recorder.count("bytes.httpShell", 1, snapshot.bytes);
    const synced = Promise.withResolvers<void>();
    const handle = recorder.begin("subscribe.shellSync");
    const started = now();
    let synchronized = false;
    socket.stream(
      "orchestration.subscribeShell",
      {
        afterSequence: snapshot.value.snapshotSequence,
        requestCompletionMarker: true,
        omitIdleSubagents: true,
        omitOlderSettled: true,
      },
      {
        onValues: (values, bytes) => {
          recorder.count("events.shell", values.length, bytes);
          if (
            !synchronized &&
            (values as ReadonlyArray<StreamItem>).some((v) => v.kind === "synchronized")
          ) {
            synchronized = true;
            recorder.succeed(handle, now() - started);
            synced.resolve();
          }
        },
        onEnd: (error) => {
          if (this.context.isStopping()) return;
          this.context.abort(
            new FatalError(
              `Subscriber ${this.index} shell stream ended: ${error?.message ?? "completed"}`,
            ),
          );
        },
      },
    );
    await withTimeout(synced.promise, this.context.options.callTimeoutMs, "shell subscription");
  }

  async show(threadId: string): Promise<void> {
    const socket = this.socket;
    if (socket === undefined || this.context.isStopping()) return;
    const generation = ++this.generation;
    this.stopThread?.();
    this.stopThread = undefined;
    clearTimeout(this.syncTimer);
    this.context.recorder.fail(this.syncHandle, "left before the thread synchronized");
    this.loading = true;
    this.current = threadId;
    const { recorder, http } = this.context;
    if (recorder.isOpen) this.touched.add(threadId);
    const bounded = await timed(this.context, "subscribe.httpThreadBounded", () =>
      httpJson<BoundedSnapshot>(
        http,
        `/api/orchestration/threads/${encodeURIComponent(threadId)}/bounded?compactTurnItems=1&dedupeBodies=1&compactIds=1&deferWork=1`,
      ),
    );
    if (generation !== this.generation || this.context.isStopping()) return;
    if (bounded !== undefined) recorder.count("bytes.httpThreadBounded", 1, bounded.bytes);
    const handle = recorder.begin("subscribe.threadSync");
    this.syncHandle = handle;
    const started = now();
    let synchronized = false;
    this.stopThread = socket.stream(
      "orchestration.subscribeThread",
      {
        threadId,
        ...(bounded === undefined ? {} : { afterSequence: bounded.value.snapshotSequence }),
        requestCompletionMarker: true,
        acceptBoundedSnapshot: true,
        acceptDeferredWork: true,
        acceptCompactTurnItems: true,
        acceptCompactIds: true,
        acceptDedupedBodies: true,
      },
      {
        onValues: (values, bytes) => {
          const received = Date.now();
          let events = 0;
          for (const value of values as ReadonlyArray<StreamItem>) {
            if (value.kind === "event") {
              events++;
              const occurredAt = Date.parse(value.event?.occurredAt ?? "");
              if (Number.isFinite(occurredAt)) {
                recorder.sample("push.threadEvent", Math.max(0, received - occurredAt));
              }
            } else if (value.kind === "snapshot") {
              recorder.count("events.threadSnapshot", 1);
            } else if (value.kind === "synchronized" && !synchronized) {
              synchronized = true;
              clearTimeout(this.syncTimer);
              recorder.succeed(handle, now() - started);
              this.loading = false;
              if (this.switchWhenSynchronized) {
                this.switchWhenSynchronized = false;
                void this.show(this.nextThread());
              }
            }
          }
          recorder.count("events.thread", events, bytes);
        },
        onEnd: (error) => {
          if (generation !== this.generation || this.context.isStopping()) return;
          const reason = error?.message ?? "stream ended";
          if (synchronized) recorder.error("subscribe.threadStream", reason);
          else recorder.fail(handle, reason);
          clearTimeout(this.syncTimer);
          this.retryTimer = setTimeout(() => void this.show(this.nextThread()), 250);
        },
      },
    );
    // A load that never synchronizes must not hold the client (and its switches) forever.
    this.syncTimer = setTimeout(() => {
      if (generation !== this.generation || synchronized || this.context.isStopping()) return;
      recorder.fail(handle, "did not synchronize within the call timeout");
      this.syncHandle = null;
      this.switchWhenSynchronized = false;
      void this.show(this.nextThread());
    }, this.context.options.callTimeoutMs);
  }

  /** Moves to another thread; a load still in progress is measured to its end first. */
  requestSwitch(): void {
    if (this.loading) this.switchWhenSynchronized = true;
    else void this.show(this.nextThread());
  }

  private nextThread(): string {
    return this.pool[pickIndex(this.rng, this.pool.length)] as string;
  }

  /** Counts the thread already open when the window begins as shown in it. */
  windowOpened(): void {
    if (this.current !== undefined) this.touched.add(this.current);
  }

  stop(): void {
    clearTimeout(this.timer);
    clearTimeout(this.retryTimer);
    clearTimeout(this.syncTimer);
    this.stopThread?.();
    this.socket?.close();
  }
}

const MAX_POLLS_IN_FLIGHT = 32;

/**
 * A client calling the server at a fixed rate: a poller, or a session's agent using its MCP
 * tools. Calls start on schedule even while earlier ones are still out, so a slow server shows up
 * as latency, not as fewer calls; a call that cannot start because too many are out counts as a
 * failed call. Call types are recorded as `<prefix>.<call>`.
 */
export class Poller {
  private timer: ReturnType<typeof setTimeout> | undefined;
  private inFlight = 0;
  readonly touched = new Set<string>();

  readonly index: number;
  private readonly context: RunContext;
  private readonly mcp: McpSession | null;
  private readonly pool: ReadonlyArray<string>;
  private readonly rng: Rng;
  private readonly prefix: string;
  private readonly ratePerSecond: number;
  private readonly mix: ReadonlyArray<readonly [PollCall, number]>;
  private readonly endsOnUnauthorized: boolean;
  private stoppedForGood = false;

  constructor(input: {
    readonly index: number;
    readonly context: RunContext;
    readonly mcp: McpSession | null;
    readonly pool: ReadonlyArray<string>;
    readonly rng: Rng;
    readonly prefix: string;
    readonly ratePerSecond: number;
    readonly mix: ReadonlyArray<readonly [PollCall, number]>;
    /** A session's own credential: a 401 means its session ended, so the caller stops quietly. */
    readonly endsOnUnauthorized?: boolean;
  }) {
    this.index = input.index;
    this.context = input.context;
    this.mcp = input.mcp;
    this.pool = input.pool;
    this.rng = input.rng;
    this.prefix = input.prefix;
    this.ratePerSecond = input.ratePerSecond;
    this.mix = input.mix;
    this.endsOnUnauthorized = input.endsOnUnauthorized === true;
  }

  /** The caller's session ended (its credential was refused); it makes no more calls. */
  get ended(): boolean {
    return this.stoppedForGood;
  }

  start(): void {
    const interval = 1000 / this.ratePerSecond;
    const startedAt = now() + this.rng.next() * interval;
    let tick = 0;
    const next = () => {
      if (this.context.isStopping() || this.stoppedForGood) return;
      this.fire();
      tick++;
      this.timer = setTimeout(next, Math.max(0, startedAt + tick * interval - now()));
    };
    this.timer = setTimeout(next, startedAt - now());
  }

  private fire(): void {
    const call: PollCall = weightedChoice(this.rng, this.mix);
    const threadId = this.pool[pickIndex(this.rng, this.pool.length)] as string;
    if (this.inFlight >= MAX_POLLS_IN_FLIGHT) {
      this.context.recorder.error(
        `${this.prefix}.${call}`,
        `not sent: ${MAX_POLLS_IN_FLIGHT} calls in flight`,
      );
      return;
    }
    this.inFlight++;
    void this.run(call, threadId).finally(() => {
      this.inFlight--;
    });
  }

  private async run(call: PollCall, threadId: string): Promise<void> {
    const { recorder, http } = this.context;
    const type = `${this.prefix}.${call}`;
    if (call !== "mcp.t3_project_list" && recorder.isOpen) this.touched.add(threadId);
    const bytes = await timed(
      this.context,
      type,
      async () => {
        if (call === "http.threadBounded") {
          const result = await httpRequest(
            http,
            `/api/orchestration/threads/${encodeURIComponent(threadId)}/bounded?compactTurnItems=1&dedupeBodies=1&compactIds=1&deferWork=1`,
          );
          if (result.status !== 200) {
            throw new CallError(
              `bounded read answered ${result.status}: ${result.text.slice(0, 200)}`,
            );
          }
          return result.bytes;
        }
        const mcp = this.mcp;
        if (mcp === null) throw new CallError("no MCP session");
        return call === "mcp.t3_project_list"
          ? mcp.callTool("t3_project_list", {})
          : mcp.callTool("t3_thread_read", { threadId });
      },
      {
        ignore: (error) => {
          const ended =
            this.endsOnUnauthorized && error instanceof CallError && error.status === 401;
          if (ended) {
            this.stoppedForGood = true;
            clearTimeout(this.timer);
          }
          return ended;
        },
      },
    );
    if (bytes !== undefined) recorder.count(`bytes.${type}`, 1, bytes);
  }

  async stop(closeSession: boolean): Promise<void> {
    clearTimeout(this.timer);
    if (closeSession) await this.mcp?.close();
  }
}

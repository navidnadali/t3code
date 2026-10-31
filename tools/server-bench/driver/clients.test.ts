import { describe, expect, it, vi } from "vite-plus/test";

import {
  isProviderOutputEvent,
  Session,
  ShellWatcher,
  Subscriber,
  timed,
  type RunContext,
  type ThreadShell,
} from "./clients.ts";
import { parseDriverOptions } from "./options.ts";
import { CallError, type RpcSocket } from "./protocol.ts";
import { createRng } from "./sampling.ts";
import { Recorder } from "./stats.ts";

const shell = (overrides: Partial<ThreadShell>): ThreadShell => ({
  id: "thread-1",
  projectId: "project-1",
  modelSelection: { instanceId: "claudeBench", model: "claude-sonnet-4-6" },
  runtimeMode: "full-access",
  interactionMode: "default",
  worktreePath: null,
  lineage: { relationshipToParent: null },
  latestRunId: "run-1",
  activeRunId: null,
  status: "completed",
  pendingRuntimeRequest: null,
  itemCount: 10,
  archivedAt: null,
  ...overrides,
});

/** Shell and thread streams fed by the test, and a socket that accepts every command. */
const harness = (call: () => Promise<unknown> = async () => ({ sequence: 1 })) => {
  const streams = new Map<string, (values: ReadonlyArray<unknown>) => void>();
  const opened: Array<string> = [];
  const socket = {
    stream: (
      tag: string,
      _payload: unknown,
      handlers: { onValues: (values: ReadonlyArray<unknown>) => void },
    ) => {
      opened.push(tag);
      streams.set(tag, (values) => handlers.onValues(values));
      return () => undefined;
    },
    call,
    close: () => undefined,
  } as unknown as RpcSocket;
  const recorder = new Recorder();
  const context: RunContext = {
    options: parseDriverOptions(["--url", "http://127.0.0.1:1", "--out", "/tmp/unused"]),
    http: { origin: "http://127.0.0.1:1", token: "", timeoutMs: 1, protocolVersion: 2 },
    recorder,
    openSocket: async () => socket,
    abort: () => undefined,
    isStopping: () => false,
  };
  const watcher = new ShellWatcher();
  watcher.seed({ snapshotSequence: 0, threads: [shell({})], projects: [] });
  watcher.follow(context, socket, 0);
  const session = new Session({
    index: 0,
    threadId: "thread-1",
    context,
    socket,
    watcher,
    rng: createRng("test"),
    switchProvider: false,
  });
  const update = (thread: ThreadShell) =>
    streams.get("orchestration.subscribeShell")?.([
      { kind: "thread.updated", sequence: 1, thread },
    ]);
  const emit = (type: string, payload: Record<string, unknown>) =>
    streams.get("orchestration.subscribeThread")?.([
      { kind: "event", sequence: 2, event: { type, payload, occurredAt: "2026-10-31T12:00:00Z" } },
    ]);
  return { session, recorder, update, emit, context, streams, opened };
};

describe("Session", () => {
  it("settles a turn from a coalesced shell update and only from a new run", async () => {
    const { session, recorder, update } = harness();
    recorder.openWindow();
    session.start();
    // The run the thread already had does not end the new turn.
    update(shell({ latestRunId: "run-1", status: "completed", itemCount: 11 }));
    expect(session.inFlight).toBe(true);
    // Start, output and completion can arrive as one update.
    update(shell({ latestRunId: "run-2", status: "completed", itemCount: 19 }));
    expect(session.inFlight).toBe(false);
    expect(recorder.summarize(1).calls["session.turn"]?.count).toBe(1);
    await session.stop(false);
  });

  it("does not count a turn that started before the window", async () => {
    const { session, recorder, update } = harness();
    session.start();
    recorder.openWindow();
    update(shell({ latestRunId: "run-2", status: "completed", itemCount: 19 }));
    expect(session.inFlight).toBe(false);
    expect(recorder.summarize(1).calls["session.turn"]).toBeUndefined();
    await session.stop(false);
  });

  it("survives a dispatch that fails after the shell already settled its turn", async () => {
    const reply = Promise.withResolvers<unknown>();
    const { session, recorder, update } = harness(() => reply.promise);
    recorder.openWindow();
    session.start();
    update(shell({ latestRunId: "run-2", status: "completed", itemCount: 19 }));
    reply.reject(new CallError("orchestration.dispatchCommand: timed out"));
    await reply.promise.catch(() => undefined);
    await Promise.resolve();
    expect(recorder.summarize(1).calls).toMatchObject({
      "session.turn": { count: 1, errors: 0 },
      "session.dispatch": { count: 0, errors: 1 },
    });
    await session.stop(false);
  });

  it("counts a failed run as a failed turn", async () => {
    const { session, recorder, update } = harness();
    recorder.openWindow();
    session.start();
    update(shell({ latestRunId: "run-2", status: "failed", lastError: "provider exited" }));
    expect(recorder.summarize(1).calls["session.turn"]).toMatchObject({
      count: 0,
      errors: 1,
      errorSamples: ["run failed: provider exited"],
    });
    await session.stop(false);
  });
});

describe("Session output check", () => {
  it("counts provider output that streams while the window is open", async () => {
    const { session, recorder, emit } = harness();
    session.watch();
    recorder.openWindow();
    session.windowOpened();
    emit("turn-item.updated", { type: "assistant_message", status: "running" });
    expect(session.producedOutput).toBe(true);
    expect(recorder.summarize(1).counters["sessions.providerEvents"]?.count).toBe(1);
    await session.stop(false);
  });

  it("does not count output written before the window or first arriving after it", async () => {
    const { session, recorder, emit } = harness();
    session.watch();
    emit("turn-item.updated", { type: "assistant_message", status: "completed" });
    recorder.openWindow();
    session.windowOpened();
    emit("turn-item.updated", { type: "user_message", status: "completed" });
    emit("turn-item.updated", { type: "checkpoint", status: "completed" });
    emit("run.updated", { status: "completed" });
    recorder.closeWindow();
    emit("turn-item.updated", { type: "assistant_message", status: "running" });
    expect(session.producedOutput).toBe(false);
    await session.stop(false);
  });
});

describe("isProviderOutputEvent", () => {
  it.each([
    [{ type: "turn-item.updated", payload: { type: "reasoning" } }, true],
    [{ type: "turn-item.updated", payload: { type: "command_execution" } }, true],
    [{ type: "message.updated", payload: { role: "assistant" } }, true],
    [{ type: "message.updated", payload: { role: "user" } }, false],
    [{ type: "turn-item.updated", payload: { type: "user_message" } }, false],
    [{ type: "turn-item.updated", payload: { type: "run_interrupt_result" } }, false],
    [{ type: "run.updated", payload: {} }, false],
  ])("%j is provider output: %s", (event, expected) => {
    expect(isProviderOutputEvent(event)).toBe(expected);
  });
});

describe("timed", () => {
  it("drops failures the caller marks as not the server's, such as an ended session's 401", async () => {
    const { recorder, context } = harness();
    recorder.openWindow();
    const unauthorized = new CallError("MCP answered 401", { status: 401 });
    const ended = (error: unknown) => error instanceof CallError && error.status === 401;
    await timed(context, "session.mcp.t3_thread_read", () => Promise.reject(unauthorized), {
      ignore: ended,
    });
    await timed(
      context,
      "session.mcp.t3_thread_read",
      () => Promise.reject(new CallError("MCP answered 500", { status: 500 })),
      { ignore: ended },
    );
    expect(recorder.summarize(1).calls["session.mcp.t3_thread_read"]).toMatchObject({
      count: 0,
      errors: 1,
      errorSamples: ["MCP answered 500"],
    });
    expect(recorder.inFlight).toBe(0);
  });
});

describe("Subscriber", () => {
  it("lets a thread load finish before switching, and counts one cut short as failed", async () => {
    using _fetch = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(
        async () =>
          new Response(JSON.stringify({ snapshotSequence: 1, threads: [], projects: [] })),
      );
    const { recorder, context, streams, opened } = harness();
    const subscriber = new Subscriber(0, context, ["thread-1", "thread-2"], createRng("test"));
    const opening = subscriber.open();
    // The harness's own shell watcher holds the first shell stream; the subscriber opens the second.
    await vi.waitFor(() =>
      expect(opened.filter((tag) => tag === "orchestration.subscribeShell")).toHaveLength(2),
    );
    streams.get("orchestration.subscribeShell")?.([{ kind: "synchronized" }]);
    await opening;
    recorder.openWindow();
    const threadStreams = () => opened.filter((tag) => tag === "orchestration.subscribeThread");
    expect(threadStreams()).toHaveLength(1);

    // Still loading: the switch waits for the load to synchronize, then happens.
    subscriber.requestSwitch();
    expect(threadStreams()).toHaveLength(1);
    streams.get("orchestration.subscribeThread")?.([{ kind: "synchronized" }]);
    await vi.waitFor(() => expect(threadStreams()).toHaveLength(2));

    // Leaving a thread before it synchronizes is a failed load, not a vanished one.
    await subscriber.show("thread-1");
    streams.get("orchestration.subscribeThread")?.([{ kind: "synchronized" }]);
    expect(recorder.summarize(1).calls["subscribe.threadSync"]).toMatchObject({
      count: 1,
      errors: 1,
      errorSamples: ["left before the thread synchronized"],
    });
    subscriber.stop();
  });
});

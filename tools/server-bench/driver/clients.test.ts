import { describe, expect, it } from "vite-plus/test";

import {
  providerOutputDuring,
  Session,
  ShellWatcher,
  type RunContext,
  type ThreadShell,
} from "./clients.ts";
import { parseDriverOptions } from "./options.ts";
import type { RpcSocket } from "./protocol.ts";
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

/** A shell stream fed by the test and a socket that accepts every command. */
const harness = () => {
  let push: (values: ReadonlyArray<unknown>) => void = () => undefined;
  const socket = {
    stream: (_tag: string, _payload: unknown, handlers: { onValues: typeof push }) => {
      push = (values) => handlers.onValues(values);
      return () => undefined;
    },
    call: async () => ({ sequence: 1 }),
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
  const update = (thread: ThreadShell) => push([{ kind: "thread.updated", sequence: 1, thread }]);
  return { session, recorder, update };
};

describe("Session", () => {
  it("settles a turn from a coalesced shell update and only from a new run", async () => {
    const { session, recorder, update } = harness();
    recorder.setOpen(true);
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

  it("counts a failed run as a failed turn", async () => {
    const { session, recorder, update } = harness();
    recorder.setOpen(true);
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

describe("providerOutputDuring", () => {
  const windowStart = Date.parse("2026-10-31T12:00:00.000Z");
  const windowEnd = Date.parse("2026-10-31T12:05:00.000Z");
  const during = (items: Parameters<typeof providerOutputDuring>[0]) =>
    providerOutputDuring(items, windowStart, windowEnd);

  it("sees an item that started before the window and was still streaming after it", () => {
    expect(
      during([
        {
          type: "assistant_message",
          status: "running",
          startedAt: "2026-10-31T11:59:00.000Z",
          completedAt: null,
          updatedAt: "2026-10-31T12:05:02.000Z",
        },
      ]),
    ).toBe(true);
  });

  it("sees output that completed together with the run inside the window", () => {
    expect(
      during([
        {
          type: "command_execution",
          status: "completed",
          startedAt: "2026-10-31T12:04:59.000Z",
          completedAt: "2026-10-31T12:04:59.500Z",
          updatedAt: "2026-10-31T12:04:59.500Z",
        },
      ]),
    ).toBe(true);
  });

  it("does not count output that first arrived after the window closed", () => {
    expect(
      during([
        {
          type: "assistant_message",
          status: "running",
          startedAt: "2026-10-31T12:05:01.000Z",
          completedAt: null,
          updatedAt: "2026-10-31T12:05:01.500Z",
        },
      ]),
    ).toBe(false);
  });

  it("ignores the user's messages, checkpoints, run bookkeeping and older output", () => {
    const inWindow = {
      status: "completed",
      startedAt: "2026-10-31T12:01:00.000Z",
      completedAt: "2026-10-31T12:01:00.000Z",
      updatedAt: "2026-10-31T12:01:00.000Z",
    };
    expect(
      during([
        {
          type: "assistant_message",
          status: "completed",
          startedAt: "2026-10-31T11:58:00.000Z",
          completedAt: "2026-10-31T11:59:59.999Z",
          updatedAt: "2026-10-31T11:59:59.999Z",
        },
        { type: "user_message", ...inWindow },
        { type: "checkpoint", ...inWindow },
        { type: "run_interrupt_request", ...inWindow },
        { type: "run_interrupt_result", ...inWindow },
        { type: "reasoning" },
      ]),
    ).toBe(false);
  });
});

import { describe, expect, it } from "vite-plus/test";

import { Session, ShellWatcher, type RunContext, type ThreadShell } from "./clients.ts";
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
  it("does not count a turn that completes without provider output as streaming", async () => {
    const { session, recorder, update } = harness();
    recorder.setOpen(true);
    session.windowOpened();
    session.start();
    // The user's message and the run's end add items, but none while the provider runs.
    update(
      shell({ latestRunId: "run-2", activeRunId: "run-2", status: "starting", itemCount: 11 }),
    );
    update(shell({ latestRunId: "run-2", activeRunId: "run-2", status: "running", itemCount: 11 }));
    update(shell({ latestRunId: "run-2", status: "completed", itemCount: 12 }));
    expect(session.producedOutput).toBe(false);
    expect(recorder.summarize(1).calls["session.turn"]?.count).toBe(1);
    await session.stop(false);
  });

  it("counts turn items added while the provider runs as streaming", async () => {
    const { session, recorder, update } = harness();
    recorder.setOpen(true);
    session.windowOpened();
    const running = { latestRunId: "run-2", activeRunId: "run-2", status: "running" };
    update(shell({ ...running, itemCount: 11 }));
    update(shell({ ...running, itemCount: 14 }));
    expect(session.producedOutput).toBe(true);
    expect(recorder.summarize(1).counters["sessions.providerItems"]?.count).toBe(3);
    await session.stop(false);
  });
});

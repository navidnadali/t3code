import type { OrchestrationV2StoredEvent } from "@t3tools/contracts";
import { assert, it } from "vite-plus/test";

import { collapseSupersededItemStates } from "./SupersededItemStates.ts";

const stored = (
  type: string,
  payload: Readonly<Record<string, unknown>>,
  runId: string | undefined = "run:a",
  threadId = "thread",
) =>
  ({
    sequence: 0,
    commandId: null,
    event: { id: `${type}:${String(payload.id)}`, type, threadId, runId, payload },
  }) as unknown as OrchestrationV2StoredEvent;
const labels = (events: ReadonlyArray<OrchestrationV2StoredEvent>) =>
  events.map(({ event }) => {
    const payload = event.payload as { id: string; status?: string; streaming?: boolean };
    return `${payload.id}:${payload.status ?? (payload.streaming ? "streaming" : "final")}`;
  });

it("collapses the states a later state of the same item supersedes before the run's next control", () => {
  const tool = (status: string) =>
    stored("turn-item.updated", { id: "tool", type: "dynamic_tool", status });
  const usage = (n: number) =>
    stored("provider-turn.updated", { id: "turn", status: "running", tokens: n });
  const node = (status: string) => stored("node.updated", { id: "node", status });
  const text = (streaming: boolean) => stored("message.updated", { id: "text", streaming });
  const control = stored("run.updated", { id: "run:a", status: "running" });
  const otherRun = stored("run.updated", { id: "run:b", status: "running" }, "run:b", "thread:b");
  assert.deepEqual(
    labels(
      collapseSupersededItemStates([
        tool("running"),
        usage(1),
        text(true),
        otherRun,
        tool("completed"),
        usage(2),
        node("running"),
        control,
        node("running"),
        node("completed"),
        node("running"),
        text(false),
        stored("provider-turn.updated", { id: "turn", status: "completed" }),
      ]),
    ),
    [
      // In the first segment the tool's result and the newer usage report take their first state's
      // place (another thread's run's control is no barrier); the text's step stays: its final comes
      // after this run's control.
      "tool:completed",
      "turn:running",
      "text:streaming",
      "run:b:running",
      "node:running",
      "run:a:running",
      // A final ends its item's state; a later state of the item starts a new one.
      "node:completed",
      "node:running",
      "text:final",
      "turn:completed",
    ],
  );
});

it("keeps a settled run's states in order but for streamed text steps", () => {
  const settled = stored("run.updated", { id: "run:a", status: "completed" });
  const tool = (id: string, status: string) =>
    stored("turn-item.updated", { id, type: "dynamic_tool", status });
  const step = (text: string) =>
    stored("turn-item.updated", { id: "text", type: "assistant_message", streaming: true, text });
  assert.deepEqual(
    labels(
      collapseSupersededItemStates([
        settled,
        tool("a", "running"),
        step("one"),
        step("one two"),
        tool("b", "completed"),
        tool("a", "completed"),
      ]),
    ),
    ["run:a:completed", "a:running", "text:streaming", "b:completed", "a:completed"],
  );
});

it("ends what waits on a thread at a control of another run on that thread", () => {
  // An inherited background item keeps its earlier run's id; the consuming run's control still
  // bounds it.
  const inherited = (status: string) =>
    stored("turn-item.updated", { id: "bash", type: "command_execution", status }, "run:earlier");
  const control = stored(
    "provider-thread.updated",
    { id: "provider-thread", status: "active" },
    "run:now",
  );
  assert.deepEqual(
    labels(collapseSupersededItemStates([inherited("running"), control, inherited("completed")])),
    ["bash:running", "provider-thread:active", "bash:completed"],
  );
});

it("replaces an inherited item's states while a newer run on its thread consumes them", () => {
  // An interrupted run's background item stays open; the thread's next run ingests it before its
  // own root terminal, so its states coalesce as that run's do.
  const inherited = (status: string) =>
    stored("turn-item.updated", { id: "bash", type: "command_execution", status }, "run:earlier");
  assert.deepEqual(
    labels(
      collapseSupersededItemStates([
        stored("run.updated", { id: "run:earlier", status: "interrupted" }, "run:earlier"),
        stored("run.updated", { id: "run:now", status: "running" }, "run:now"),
        inherited("running"),
        inherited("completed"),
      ]),
    ),
    ["run:earlier:interrupted", "run:now:running", "bash:completed"],
  );
});

it.each(["completed", "waiting"])(
  "keeps a %s run's late states in order after its thread's next run starts",
  (status) => {
    // Only an interrupted, failed or cancelled run's background items pass to the next run
    // (selectInheritedBackgroundTurnItems); this run's own ingestion keeps them, past its terminal.
    const late = [
      stored("turn-item.updated", { id: "bash", type: "command_execution", status: "running" }),
      stored("message.updated", { id: "text", streaming: false }),
      stored("turn-item.updated", { id: "bash", type: "command_execution", status: "completed" }),
    ];
    const runs = [
      stored("run.updated", { id: "run:a", status }),
      stored("run.updated", { id: "run:b", status: "running" }, "run:b"),
    ];
    const kept = collapseSupersededItemStates([...runs, ...late]);
    assert.deepEqual(labels(kept), [
      `run:a:${status}`,
      "run:b:running",
      "bash:running",
      "text:final",
      "bash:completed",
    ]);
    // A regression that writes the command's end ahead of the late final must not compare equal.
    assert.notDeepEqual(
      labels(kept),
      labels(collapseSupersededItemStates([...runs, late[2]!, late[1]!])),
    );
  },
);

it("ends an inherited item's wait at a control of its consuming run on another thread", () => {
  const inherited = (status: string) =>
    stored("turn-item.updated", { id: "bash", type: "command_execution", status }, "run:earlier");
  assert.deepEqual(
    labels(
      collapseSupersededItemStates([
        stored("run.updated", { id: "run:earlier", status: "interrupted" }, "run:earlier"),
        stored("run.updated", { id: "run:now", status: "running" }, "run:now"),
        inherited("running"),
        stored(
          "provider-thread.updated",
          { id: "child", status: "active" },
          "run:now",
          "thread:child",
        ),
        inherited("completed"),
      ]),
    ),
    [
      "run:earlier:interrupted",
      "run:now:running",
      "bash:running",
      "child:active",
      "bash:completed",
    ],
  );
});

it("ends what waits for the consuming run at an inherited item it keeps in order after it settled", () => {
  // The older run was interrupted; its successor settled too and keeps every state but streamed
  // text in order: the inherited command is a barrier for the successor's text steps.
  const text = (value: string) =>
    stored(
      "turn-item.updated",
      { id: "text", type: "assistant_message", streaming: true, value },
      "run:now",
      "thread:child",
    );
  assert.deepEqual(
    labels(
      collapseSupersededItemStates([
        stored("run.updated", { id: "run:earlier", status: "interrupted" }, "run:earlier"),
        stored("run.updated", { id: "run:now", status: "running" }, "run:now"),
        stored("run.updated", { id: "run:now", status: "waiting" }, "run:now"),
        text("one"),
        stored(
          "turn-item.updated",
          { id: "bash", type: "command_execution", status: "running" },
          "run:earlier",
        ),
        text("one two"),
      ]),
    ),
    [
      "run:earlier:interrupted",
      "run:now:running",
      "run:now:waiting",
      "text:streaming",
      "bash:running",
      "text:streaming",
    ],
  );
});

it("keeps a tool's states apart across a steer's user message", () => {
  const tool = (status: string) =>
    stored("turn-item.updated", { id: "tool", type: "dynamic_tool", status });
  assert.deepEqual(
    labels(
      collapseSupersededItemStates([
        tool("running"),
        stored("turn-item.updated", { id: "steer", type: "user_message", status: "completed" }),
        tool("completed"),
      ]),
    ),
    ["tool:running", "steer:completed", "tool:completed"],
  );
});

it("keeps a replaced usage report's token usage, as the store does", () => {
  const report = (usage?: number) =>
    stored("provider-turn.updated", {
      id: "turn",
      status: "running",
      ...(usage === undefined ? {} : { tokenUsage: { usedTokens: usage } }),
    });
  const [collapsed] = collapseSupersededItemStates([report(42), report()]);
  assert.deepEqual((collapsed!.event.payload as { tokenUsage?: unknown }).tokenUsage, {
    usedTokens: 42,
  });
});

it("keeps an item's states apart across a control of its run that stored nothing", () => {
  // tool running -> background_work.updated (stores nothing) -> tool completed: the mailbox keeps
  // both states, so the helper must too, given the run's controls the stored writes cannot show.
  const tool = (status: string, id: string) => {
    const write = stored("turn-item.updated", { id: "tool", type: "command_execution", status });
    return { ...write, event: { ...write.event, id } } as OrchestrationV2StoredEvent;
  };
  const controls = [
    {
      afterEventId: "write:1",
      runId: "run:a",
      threadId: "thread",
      type: "background_work.updated",
    },
  ];
  const kept = collapseSupersededItemStates(
    [tool("running", "write:1"), tool("completed", "write:2")],
    controls,
  );
  assert.deepEqual(labels(kept), ["tool:running", "tool:completed"]);
  // A mailbox that coalesced across that control stores only the completed state: it must not
  // compare equal.
  assert.notDeepEqual(
    labels(kept),
    labels(collapseSupersededItemStates([tool("completed", "write:2")], [])),
  );
  // Another run's control does not end this run's wait.
  assert.deepEqual(
    labels(
      collapseSupersededItemStates(
        [tool("running", "write:1"), tool("completed", "write:2")],
        [{ ...controls[0]!, runId: "run:b" }],
      ),
    ),
    ["tool:completed"],
  );
});

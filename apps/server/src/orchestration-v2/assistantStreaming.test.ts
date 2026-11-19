import { MessageId, ProviderDriverKind, ThreadId } from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import { assert, describe, expect, it, vi } from "vite-plus/test";
import {
  makeAssistantStreamingFilter,
  makeIngestionMailbox,
  splitBufferedAssistantText,
} from "./assistantStreaming.ts";

describe("V2 assistant streaming", () => {
  it("keeps code fences intact", () => {
    expect(splitBufferedAssistantText("Intro\n\n```ts\nx()\n\n")).toEqual({
      ready: "Intro\n\n",
      rest: "```ts\nx()\n\n",
    });
    expect(splitBufferedAssistantText("```ts\nx()\n```\nrest")).toEqual({
      ready: "```ts\nx()\n```\n",
      rest: "rest",
    });
    expect(splitBufferedAssistantText("```a``` is inline.\n\n```\ncode\n\nmore\n")).toEqual({
      ready: "```a``` is inline.\n\n",
      rest: "```\ncode\n\nmore\n",
    });
  });
});

it("holds a streamed section heading until its content has a boundary", () => {
  expect(splitBufferedAssistantText("Intro\n\n## Results\n\n")).toEqual({
    ready: "Intro\n\n",
    rest: "## Results\n\n",
  });
  expect(splitBufferedAssistantText("**Results**\n\nBody\n\nNext")).toEqual({
    ready: "**Results**\n\nBody\n\n",
    rest: "Next",
  });
});

type Step =
  // A snapshot at `at` ms, and the text it delivers at once (null when held back).
  | { readonly at: number; readonly text: string; readonly shows: string | null }
  | { readonly at: number; readonly final: string }
  | { readonly id: string; readonly at: number; readonly text: string; readonly shows: string }
  // Held snapshots due by `idle` ms, delivered whole unless a newer snapshot of theirs still waits.
  | {
      readonly idle: number;
      readonly waiting?: ReadonlyArray<string>;
      readonly shows: ReadonlyArray<string>;
    }
  | { readonly dueAt: number | null };

const createdAt = DateTime.makeUnsafe(0);
const snapshot = (id: string, text: string, streaming: boolean) =>
  ({
    type: "message.updated",
    driver: ProviderDriverKind.make("codex"),
    message: {
      createdBy: "agent",
      creationSource: "provider",
      id: MessageId.make(id),
      threadId: ThreadId.make("thread"),
      runId: null,
      nodeId: null,
      role: "assistant",
      text,
      attachments: [],
      streaming,
      createdAt,
      updatedAt: createdAt,
    },
  }) satisfies ProviderAdapter.ProviderAdapterV2Event;
const shown = (event: ProviderAdapter.ProviderAdapterV2Event | null) =>
  event?.type === "message.updated" ? event.message.text : null;

it.each<{ readonly name: string; readonly mode?: "turn"; readonly steps: ReadonlyArray<Step> }>([
  {
    name: "an unfinished code fence",
    steps: [
      { at: 0, text: "A\n\n```ts\nx()", shows: "A\n\n" },
      { dueAt: 800 },
      { idle: 799, shows: [] },
      { idle: 800, shows: ["A\n\n```ts\nx()"] },
      { dueAt: null },
      { at: 1_300, text: "A\n\n```ts\nx()\n```\n\nB", shows: "A\n\n```ts\nx()\n```\n\n" },
    ],
  },
  {
    name: "a list's partial last item",
    steps: [
      { at: 0, text: "Steps:\n\n- one\n- tw", shows: "Steps:\n\n- one\n" },
      { idle: 800, shows: ["Steps:\n\n- one\n- tw"] },
      { at: 1_300, text: "Steps:\n\n- one\n- two\n- th", shows: "Steps:\n\n- one\n- two\n" },
    ],
  },
  {
    name: "an incomplete table row",
    steps: [
      { at: 0, text: "Intro.\n\n| a | b |\n|---|---|\n| 1 |", shows: "Intro.\n\n" },
      { idle: 800, shows: ["Intro.\n\n| a | b |\n|---|---|\n| 1 |"] },
      {
        at: 1_300,
        text: "Intro.\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\nAfter",
        shows: "Intro.\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n",
      },
    ],
  },
  {
    name: "more of the same paragraph after an idle delivery",
    steps: [
      { at: 0, text: "A\n\nB", shows: "A\n\n" },
      { idle: 800, shows: ["A\n\nB"] },
      { at: 900, text: "A\n\nB more", shows: null },
      { idle: 1_699, shows: [] },
      { idle: 1_700, shows: ["A\n\nB more"] },
      { idle: 5_000, shows: [] },
      // Its only boundary is behind what the idle delivery showed.
      { at: 5_100, text: "A\n\nB more and", shows: null },
      { at: 5_200, text: "A\n\nB more and.\n\nC", shows: "A\n\nB more and.\n\n" },
      { at: 5_300, final: "A\n\nB more and.\n\nC." },
      { idle: 10_000, shows: [] },
    ],
  },
  {
    name: "completion at the idle deadline",
    steps: [
      { at: 0, text: "A\n\nB", shows: "A\n\n" },
      { at: 800, final: "A\n\nB." },
      { idle: 800, shows: [] },
    ],
  },
  {
    name: "completion right after an idle delivery at the same instant",
    steps: [
      { at: 0, text: "A\n\nB", shows: "A\n\n" },
      { idle: 800, shows: ["A\n\nB"] },
      { at: 800, final: "A\n\nB." },
      { idle: 5_000, shows: [] },
    ],
  },
  {
    name: "several fast paragraphs inside the throttle",
    steps: [
      { at: 0, text: "A\n\nB", shows: "A\n\n" },
      { at: 100, text: "A\n\nB\n\nC", shows: null },
      { at: 200, text: "A\n\nB\n\nC\n\nD", shows: null },
      { at: 300, text: "A\n\nB\n\nC\n\nD\n\nE", shows: null },
      { idle: 1_099, shows: [] },
      { idle: 1_100, shows: ["A\n\nB\n\nC\n\nD\n\nE"] },
    ],
  },
  {
    name: "two messages due at different times",
    steps: [
      { id: "a", at: 0, text: "A\n\nx", shows: "A\n\n" },
      { id: "b", at: 300, text: "B\n\ny", shows: "B\n\n" },
      { dueAt: 800 },
      { idle: 800, shows: ["A\n\nx"] },
      { dueAt: 1_100 },
      { idle: 1_100, shows: ["B\n\ny"] },
    ],
  },
  {
    name: "text whose newer snapshot still waits for ingestion",
    steps: [
      { at: 0, text: "A\n\nB", shows: "A\n\n" },
      { idle: 800, waiting: ["m"], shows: [] },
      { dueAt: null },
      { at: 900, text: "A\n\nB more.\n\nC", shows: "A\n\nB more.\n\n" },
    ],
  },
  {
    name: "empty text",
    steps: [{ at: 0, text: "", shows: null }, { dueAt: null }],
  },
  {
    name: "whitespace-only text",
    steps: [
      { at: 0, text: " \n", shows: null },
      { idle: 800, shows: [" \n"] },
      { idle: 5_000, shows: [] },
    ],
  },
  {
    // Rewrites replace delivered text: shorter at once, same length after the throttle.
    name: "provider rewrites and truncation",
    steps: [
      { at: 0, text: "Original long paragraph.\n\n", shows: "Original long paragraph.\n\n" },
      { at: 1_000, text: "New.\n\n", shows: "New.\n\n" },
      { at: 1_100, text: "Old.\n\n", shows: null },
      { idle: 1_900, shows: ["Old.\n\n"] },
      { at: 2_500, text: "Old", shows: null },
      { idle: 3_300, shows: ["Old"] },
      { at: 4_000, text: "", shows: null },
      { idle: 4_800, shows: [""] },
    ],
  },
  {
    name: "a rewrite that drops an open code fence",
    steps: [
      { at: 0, text: "Intro\n\n```ts\nx()\n", shows: "Intro\n\n" },
      { at: 400, text: "Retry\n\n- a\n- b", shows: "Retry\n\n- a\n" },
    ],
  },
  {
    name: "turn mode",
    mode: "turn",
    steps: [{ at: 0, text: "A\n\nB", shows: null }, { dueAt: null }],
  },
])("paragraph streaming holds and idle-delivers $name", ({ mode, steps }) => {
  const filter = makeAssistantStreamingFilter(mode ?? "paragraph", 800);
  for (const step of steps) {
    if ("dueAt" in step) assert.equal(filter.nextIdleFlushAt(), step.dueAt);
    else if ("idle" in step)
      assert.deepEqual(
        filter
          .takeIdleFlushes(step.idle, (key) =>
            (step.waiting ?? []).some((id) => key === `message.updated:${id}`),
          )
          .map(shown),
        [...step.shows],
      );
    else if ("final" in step)
      assert.equal(shown(filter.filter(snapshot("m", step.final, false), step.at)), step.final);
    else
      assert.equal(
        shown(filter.filter(snapshot("id" in step ? step.id : "m", step.text, true), step.at)),
        step.shows,
      );
  }
});

it("scans each streamed line once while a long code fence is still open", () => {
  const filter = makeAssistantStreamingFilter("paragraph", 800);
  const lineCount = 400;
  let text = "Intro\n\n```ts\n";
  const indexOf = vi.spyOn(String.prototype, "indexOf");
  try {
    for (let line = 0; line < lineCount; line++) {
      text += `const value${line} = ${line};\n`;
      filter.filter(snapshot("m", text, true), line * 400);
    }
    const newlineSearches = indexOf.mock.calls.filter(([search]) => search === "\n").length;
    expect(newlineSearches).toBeLessThan(lineCount * 4);
  } finally {
    indexOf.mockRestore();
  }
  text += "```\n";
  assert.equal(shown(filter.filter(snapshot("m", text, true), lineCount * 400)), text);
});

it("counts what the ingestion mailbox holds until it is ingested, a coalesced snapshot once", () => {
  const mailbox = makeIngestionMailbox();
  const first = snapshot("message:a", "one", true);
  expect(mailbox.offer(first)).toBeUndefined();
  // The newer step of the same text takes the older one's place; it is not pending twice, and
  // the offer hands back the step it replaced, which will never be taken.
  const newer = snapshot("message:a", "one two", true);
  expect(mailbox.offer(newer)).toBe(first);
  const done = snapshot("message:b", "done", false);
  expect(mailbox.offer(done)).toBeUndefined();
  expect(mailbox.pending()).toBe(2);
  expect(mailbox.takeAll()).toEqual([newer, done]);
  expect(mailbox.takeAll()).toEqual([]);
  // Taken but not ingested yet: still the run's backlog.
  expect(mailbox.pending()).toBe(2);
  mailbox.settle(newer);
  expect(mailbox.pending()).toBe(1);
  mailbox.settle(done);
  expect(mailbox.pending()).toBe(0);
});

// Any item's newer snapshot replaces its waiting one, so a backlog writes each item once.
const runId = "run" as never;
const providerTurn = (id: string, status: "running" | "completed", usedTokens: number) =>
  ({
    type: "provider_turn.updated",
    driver: ProviderDriverKind.make("claudeAgent"),
    providerTurn: {
      id,
      providerThreadId: "provider-thread",
      nodeId: "node:root",
      runAttemptId: "attempt",
      status,
      tokenUsage: { usedTokens },
    },
  }) as unknown as ProviderAdapter.ProviderAdapterV2Event;
const toolNode = (id: string, status: "running" | "completed") =>
  ({
    type: "node.updated",
    driver: ProviderDriverKind.make("claudeAgent"),
    node: { id, threadId: "thread", runId, kind: "tool_call", status, providerThreadId: null },
  }) as unknown as ProviderAdapter.ProviderAdapterV2Event;
const toolItem = (id: string, status: "running" | "completed", title = "Tool") =>
  ({
    type: "turn_item.updated",
    driver: ProviderDriverKind.make("claudeAgent"),
    turnItem: { id, threadId: "thread", runId, type: "dynamic_tool", status, title },
  }) as unknown as ProviderAdapter.ProviderAdapterV2Event;
const controls: ReadonlyArray<readonly [string, ProviderAdapter.ProviderAdapterV2Event]> = [
  [
    "a provider thread update",
    {
      type: "provider_thread.updated",
      driver: ProviderDriverKind.make("claudeAgent"),
      providerThread: { id: "provider-thread", status: "active" },
    } as unknown as ProviderAdapter.ProviderAdapterV2Event,
  ],
  ["a provider turn's end", providerTurn("turn", "completed", 9)],
  [
    "a turn terminal",
    {
      type: "turn.terminal",
      driver: ProviderDriverKind.make("claudeAgent"),
      providerThreadId: "provider-thread",
      providerTurnId: "turn",
      status: "completed",
    } as unknown as ProviderAdapter.ProviderAdapterV2Event,
  ],
  [
    "a plan update",
    {
      type: "plan.updated",
      driver: ProviderDriverKind.make("claudeAgent"),
      plan: { id: "plan", threadId: "thread", runId, kind: "todo_list", steps: [] },
    } as unknown as ProviderAdapter.ProviderAdapterV2Event,
  ],
  [
    "a subagent update",
    {
      type: "subagent.updated",
      driver: ProviderDriverKind.make("claudeAgent"),
      subagent: { id: "subagent", threadId: "thread", runId, status: "running" },
    } as unknown as ProviderAdapter.ProviderAdapterV2Event,
  ],
];

it("replaces any item's waiting snapshot with its newer one in place, so a backlog writes each item once", () => {
  const mailbox = makeIngestionMailbox();
  const usage = providerTurn("turn", "running", 1);
  const node = toolNode("node:tool", "running");
  const item = toolItem("item:tool", "running");
  const text = snapshot("message:a", "Before the tool.\n\n", true);
  const nodeDone = toolNode("node:tool", "completed");
  const itemDone = toolItem("item:tool", "completed");
  const usageLater = providerTurn("turn", "running", 2);
  const textDone = snapshot("message:a", "Before the tool.\n\nAfter it.", false);
  for (const event of [usage, node, item, text]) expect(mailbox.offer(event)).toBeUndefined();
  // Each newer snapshot hands back the one it replaced, which is never taken.
  expect(mailbox.offer(nodeDone)).toBe(node);
  expect(mailbox.offer(itemDone)).toBe(item);
  expect(mailbox.offer(usageLater)).toBe(usage);
  expect(mailbox.offer(textDone)).toBe(text);
  // Every item keeps the place its first snapshot took, with its newest state.
  expect(mailbox.takeAll()).toEqual([usageLater, nodeDone, itemDone, textDone]);
  expect(mailbox.pending()).toBe(4);
  for (const event of [usageLater, nodeDone, itemDone, textDone]) mailbox.settle(event);
  expect(mailbox.pending()).toBe(0);
});

it("never drops an item's final: a later snapshot of the item queues after it", () => {
  const mailbox = makeIngestionMailbox();
  const running = toolItem("item:tool", "running");
  const done = toolItem("item:tool", "completed");
  const after = toolItem("item:tool", "completed", "Tool, renamed");
  const textDone = snapshot("message:a", "Done.", false);
  const textAgain = snapshot("message:a", "Done, again.", true);
  expect(mailbox.offer(running)).toBeUndefined();
  expect(mailbox.offer(done)).toBe(running);
  expect(mailbox.offer(after)).toBeUndefined();
  expect(mailbox.offer(textDone)).toBeUndefined();
  expect(mailbox.offer(textAgain)).toBeUndefined();
  expect(mailbox.takeAll()).toEqual([done, after, textDone, textAgain]);
});

it.each(controls)("moves no snapshot across %s, which is never replaced", (_name, control) => {
  const mailbox = makeIngestionMailbox();
  const running = toolItem("item:tool", "running");
  const done = toolItem("item:tool", "completed");
  const usage = providerTurn("turn", "running", 1);
  const usageLater = providerTurn("turn", "running", 2);
  const text = snapshot("message:a", "Step.\n\n", true);
  const textLater = snapshot("message:a", "Step.\n\nNext.\n\n", true);
  for (const event of [running, usage, text, control, done, usageLater, textLater, control])
    expect(mailbox.offer(event)).toBeUndefined();
  expect(mailbox.takeAll()).toEqual([
    running,
    usage,
    text,
    control,
    done,
    usageLater,
    textLater,
    control,
  ]);
});

it("keeps every event but streamed text steps in arrival order once the root terminal is queued", () => {
  // From the root terminal on, ingestion checks whether to stop after each event; a state moved
  // ahead could end it before the events it overtook. Streamed text steps still coalesce, as before.
  const mailbox = makeIngestionMailbox();
  const running = toolItem("item:tool", "running");
  const later = toolItem("item:tool", "running", "Tool, later");
  const other = toolItem("item:other", "completed");
  const done = toolItem("item:tool", "completed");
  const step = snapshot("message:a", "Step.\n\n", true);
  const nextStep = snapshot("message:a", "Step.\n\nNext.\n\n", true);
  mailbox.rootTerminalQueued();
  for (const event of [running, later, other, step]) expect(mailbox.offer(event)).toBeUndefined();
  expect(mailbox.offer(nextStep)).toBe(step);
  expect(mailbox.offer(done)).toBeUndefined();
  expect(mailbox.takeAll()).toEqual([running, later, other, nextStep, done]);
});

it("keeps a replaced usage report's token usage when the newer report carries none", () => {
  // The projection keeps a provider turn's last reported usage when a later report omits it, so
  // the report that takes another's place must carry that usage forward.
  const mailbox = makeIngestionMailbox();
  const withUsage = providerTurn("turn", "running", 42);
  const bare = {
    ...providerTurn("turn", "running", 0),
    providerTurn: { id: "turn", providerThreadId: "provider-thread", status: "running" },
  } as unknown as ProviderAdapter.ProviderAdapterV2Event;
  expect(mailbox.offer(withUsage)).toBeUndefined();
  expect(mailbox.offer(bare)).toBe(withUsage);
  const [taken] = mailbox.takeAll();
  expect(
    taken?.type === "provider_turn.updated" ? taken.providerTurn.tokenUsage : undefined,
  ).toEqual({
    usedTokens: 42,
  });
  expect(taken?.type === "provider_turn.updated" ? taken.providerTurn.status : undefined).toBe(
    "running",
  );
});

// Ingesting an event is not a pure database write: the streaming filter and the mailbox keep what
// they have seen. A commit that fails must therefore never be retried by running ingestion again.
it("keeps ingestion state, so replay must not re-run ingestion: a second pass holds the snapshot and a second settle drops a newer one's backlog", () => {
  const filter = makeAssistantStreamingFilter("paragraph", 800);
  const streamed = snapshot("message:a", "First paragraph.\n\nSecond", true);
  expect(filter.filter(streamed, 10_000)).not.toBeNull();
  // Run again, the same snapshot at the same time is held back, so it would not be stored.
  expect(filter.filter(streamed, 10_000)).toBeNull();

  const mailbox = makeIngestionMailbox();
  const older = snapshot("message:a", "A", true);
  const newer = snapshot("message:a", "A B", true);
  mailbox.offer(older);
  const taken = mailbox.takeAll();
  mailbox.offer(newer);
  const key = "message.updated:message:a";
  for (const event of taken) mailbox.settle(event);
  expect(mailbox.isWaiting(key)).toBe(true);
  // Settled again, the newer snapshot still queued no longer counts as waiting.
  for (const event of taken) mailbox.settle(event);
  expect(mailbox.isWaiting(key)).toBe(false);
});

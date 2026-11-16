import { assert, it as effectIt } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import { describe, expect, it } from "vite-plus/test";

import {
  MAX_VISIBLE_STARTS_AHEAD_OF_WAKE,
  nextQueuedRun,
  queuedRunsInDeliveryOrder,
} from "./QueuedRunOrder.ts";

const minute = (value: number) => DateTime.makeUnsafe(value * 60_000);

const completion = (id: string) => ({
  id: `message:${id}`,
  delegatedCompletion: { generation: 1, parentRunId: `run:parent-${id}`, taskIds: ["task:child"] },
});
const notice = (id: string) => ({
  id: `message:${id}`,
  notification: { source: { kind: "background_task" }, outcome: "completed", summary: id },
});
const visible = (id: string) => ({ id: `message:${id}` });

const queued = (
  id: string,
  input: {
    readonly ordinal: number;
    readonly queuePosition?: number;
    readonly requestedAt?: number;
    readonly queueHeld?: true;
  },
) => ({
  id: `run:${id}`,
  ordinal: input.ordinal,
  ...(input.queuePosition === undefined ? {} : { queuePosition: input.queuePosition }),
  ...(input.queueHeld === undefined ? {} : { queueHeld: input.queueHeld }),
  status: "queued",
  userMessageId: `message:${id}`,
  requestedAt: minute(input.requestedAt ?? 0),
  startedAt: null,
});

const started = (id: string, ordinal: number, startedAt: number) => ({
  id: `run:${id}`,
  ordinal,
  status: "completed",
  userMessageId: `message:${id}`,
  requestedAt: minute(startedAt),
  startedAt: minute(startedAt),
});

const order = (projection: { readonly runs: unknown; readonly messages: unknown }) =>
  queuedRunsInDeliveryOrder(projection as never).map((run) => run.id);

describe("queued run delivery order", () => {
  it("does not let automatic completions overtake already queued messages", () => {
    const projection = {
      messages: [
        { id: "message:visible-first" },
        {
          id: "message:automatic",
          delegatedCompletion: {
            generation: 1,
            parentRunId: "run:parent",
            taskIds: ["task:child"],
          },
        },
        { id: "message:visible-second" },
      ],
      runs: [
        {
          id: "run:visible-first",
          ordinal: 2,
          queuePosition: 1,
          status: "queued",
          userMessageId: "message:visible-first",
        },
        {
          id: "run:automatic",
          ordinal: 4,
          queuePosition: 3,
          status: "queued",
          userMessageId: "message:automatic",
        },
        {
          id: "run:visible-second",
          ordinal: 3,
          queuePosition: 2,
          status: "queued",
          userMessageId: "message:visible-second",
        },
      ],
    } as never;

    expect(queuedRunsInDeliveryOrder(projection).map((run) => run.id)).toEqual([
      "run:visible-first",
      "run:visible-second",
      "run:automatic",
    ]);
  });

  it("starts visible messages ahead of earlier wakes, each class in queue order", () => {
    expect(
      order({
        messages: [
          completion("completion"),
          completion("follow-up-completion"),
          notice("notice"),
          { id: "message:automation", scheduledTaskId: "scheduled-task:hourly" },
          { id: "message:cross-thread", senderThreadId: "thread:sender" },
          visible("user"),
        ],
        runs: [
          queued("completion", { ordinal: 2, queuePosition: 2 }),
          queued("notice", { ordinal: 3, queuePosition: 3 }),
          queued("automation", { ordinal: 4, queuePosition: 5 }),
          // The user moved the cross-thread message ahead of the automation.
          queued("cross-thread", { ordinal: 5, queuePosition: 4 }),
          queued("user", { ordinal: 6 }),
          queued("follow-up-completion", { ordinal: 7, queuePosition: 1 }),
        ],
      }),
    ).toEqual([
      "run:cross-thread",
      "run:automation",
      "run:user",
      "run:follow-up-completion",
      "run:completion",
      "run:notice",
    ]);
  });

  it.each([
    {
      name: "after enough visible starts passed it, the oldest wake goes next",
      history: [1, 2, 3, 4].map((index) => started(`visible-${index}`, index, 10 + index)),
      expected: ["run:wake", "run:next", "run:later-wake"],
    },
    {
      name: "with one start fewer, visible input still goes first",
      history: [1, 2, 3].map((index) => started(`visible-${index}`, index, 10 + index)),
      expected: ["run:next", "run:wake", "run:later-wake"],
    },
    {
      name: "a wake that started among them resets the count",
      history: [
        started("visible-1", 1, 11),
        started("earlier-wake", 2, 12),
        started("visible-3", 3, 13),
        started("visible-4", 4, 14),
        started("visible-5", 5, 15),
      ],
      expected: ["run:next", "run:wake", "run:later-wake"],
    },
    {
      name: "starts from before the wake was queued do not count",
      history: [1, 2, 3, 4].map((index) => started(`visible-${index}`, index, 6 + index)),
      expected: ["run:next", "run:wake", "run:later-wake"],
    },
    {
      name: "a start whose message was not read does not count",
      history: [
        started("visible-1", 1, 11),
        started("unread", 2, 12),
        started("visible-3", 3, 13),
        started("visible-4", 4, 14),
      ],
      expected: ["run:next", "run:wake", "run:later-wake"],
    },
  ])("$name", ({ history, expected }) => {
    expect(
      order({
        messages: [
          notice("wake"),
          completion("later-wake"),
          completion("earlier-wake"),
          visible("next"),
          ...history
            .filter((run) => run.id !== "run:unread" && run.id !== "run:earlier-wake")
            .map((run) => ({ id: run.userMessageId })),
        ],
        runs: [
          ...history,
          queued("wake", { ordinal: 20, requestedAt: 10 }),
          queued("later-wake", { ordinal: 21, requestedAt: 10 }),
          queued("next", { ordinal: 22, requestedAt: 16 }),
        ],
      }),
    ).toEqual(expected);
  });

  it("keeps a message moved to the front ahead of a wake that is due", () => {
    expect(
      order({
        messages: [
          notice("wake"),
          visible("first"),
          visible("second"),
          ...[1, 2, 3, 4].map((index) => visible(`visible-${index}`)),
        ],
        runs: [
          ...[1, 2, 3, 4].map((index) => started(`visible-${index}`, index, 10 + index)),
          queued("wake", { ordinal: 20, queuePosition: 2, requestedAt: 10 }),
          queued("first", { ordinal: 22, queuePosition: 1, requestedAt: 16 }),
          queued("second", { ordinal: 23, queuePosition: 3, requestedAt: 16 }),
        ],
      }),
    ).toEqual(["run:first", "run:wake", "run:second"]);
  });

  it("starts the first unheld run, judging wakes among unheld runs", () => {
    const history = [1, 2, 3, 4].map((index) => started(`visible-${index}`, index, 10 + index));
    const messages = [
      notice("held-wake"),
      notice("wake"),
      visible("held"),
      visible("next"),
      ...history.map((run) => ({ id: run.userMessageId })),
    ];
    const next = (runs: ReadonlyArray<unknown>) => nextQueuedRun({ runs, messages } as never)?.id;
    expect(
      next([
        ...history,
        queued("held", { ordinal: 20, requestedAt: 9, queueHeld: true }),
        queued("wake", { ordinal: 21, requestedAt: 10 }),
      ]),
    ).toBe("run:wake");
    expect(
      next([
        ...history,
        queued("held-wake", { ordinal: 20, requestedAt: 9, queueHeld: true }),
        queued("next", { ordinal: 21, requestedAt: 16 }),
      ]),
    ).toBe("run:next");
    expect(
      next([
        ...history,
        queued("held-wake", { ordinal: 20, requestedAt: 12, queueHeld: true }),
        queued("wake", { ordinal: 21, requestedAt: 10 }),
        queued("next", { ordinal: 22, requestedAt: 16 }),
      ]),
    ).toBe("run:wake");
  });

  effectIt.effect("bounds the wait of both classes while both keep arriving", () =>
    Effect.gen(function* () {
      const runs: Array<Record<string, unknown>> = [];
      const messages: Array<Record<string, unknown>> = [];
      let ordinal = 0;
      const enqueue = Effect.fnUntraced(function* (id: string, hidden: boolean) {
        ordinal += 1;
        messages.push(hidden ? completion(id) : visible(id));
        runs.push({
          id: `run:${id}`,
          ordinal,
          queuePosition: ordinal,
          status: "queued",
          userMessageId: `message:${id}`,
          requestedAt: yield* DateTime.now,
          startedAt: null,
        });
      });
      for (const index of [1, 2, 3]) yield* enqueue(`wake-${index}`, true);
      const starts: Array<string> = [];
      for (let step = 1; step <= 15; step += 1) {
        // A busy orchestrator: a run takes about three minutes, and both kinds keep coming.
        yield* TestClock.adjust("3 minutes");
        yield* enqueue(`visible-${step}`, false);
        yield* enqueue(`wake-${step + 3}`, true);
        const next = nextQueuedRun({ runs, messages } as never);
        assert.isDefined(next);
        const now = yield* DateTime.now;
        const index = runs.findIndex((run) => run.id === next.id);
        runs[index] = { ...runs[index], status: "completed", startedAt: now, completedAt: now };
        starts.push(String(next.id).replace("run:", ""));
      }
      const wakeTurn = MAX_VISIBLE_STARTS_AHEAD_OF_WAKE + 1;
      assert.deepEqual(
        starts,
        Array.from({ length: 15 }, (_, index) =>
          (index + 1) % wakeTurn === 0
            ? `wake-${(index + 1) / wakeTurn}`
            : `visible-${index + 1 - Math.floor((index + 1) / wakeTurn)}`,
        ),
      );
    }),
  );
});

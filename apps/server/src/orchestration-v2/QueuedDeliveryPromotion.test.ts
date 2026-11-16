import { assert, it } from "@effect/vitest";
import {
  ChatAttachmentId,
  ComposerContextId,
  EnvironmentId,
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ScheduledTaskId,
  ThreadId,
  TurnItemId,
  type ComposerContextRecord,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2Run,
  type OrchestrationV2TurnCapabilities,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";

import {
  decideQueuedDelivery,
  decideQueuedDeliveryPromotion,
  QUEUED_DELIVERY_MERGE_MAX_BYTES,
  QUEUED_DELIVERY_MERGE_MAX_ITEMS,
  type QueuedDeliveryPromotion,
} from "./QueuedDeliveryPromotion.ts";

const threadId = ThreadId.make("thread:promotion");
const selection = { instanceId: ProviderInstanceId.make("claudeAgent"), model: "claude-opus" };
const providerThreadId = ProviderThreadId.make("provider-thread:promotion");
const claudeTurns: OrchestrationV2TurnCapabilities = {
  exposesNativeTurnId: true,
  emitsTurnStarted: true,
  emitsTurnCompleted: true,
  supportsInterrupt: true,
  supportsActiveSteering: true,
  activeSteeringInterruptsTools: true,
  supportsSteeringByInterruptRestart: false,
  supportsQueuedMessages: true,
  terminalStatusQuality: "strong",
};

type Origin =
  | "automation"
  | "cross-thread"
  | "linked"
  | "user"
  | "completion"
  | "notification"
  | "follow-up";

const run = (
  name: string,
  ordinal: number,
  requestedAt: DateTime.Utc,
  fields: Partial<OrchestrationV2Run> = {},
): OrchestrationV2Run => ({
  id: RunId.make(`run:${name}`),
  threadId,
  ordinal,
  providerInstanceId: selection.instanceId,
  modelSelection: selection,
  providerThreadId,
  userMessageId: MessageId.make(`message:${name}`),
  rootNodeId: NodeId.make(`node:${name}`),
  activeAttemptId: RunAttemptId.make(`attempt:${name}`),
  status: "queued",
  queuePosition: ordinal,
  requestedAt,
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
  ...fields,
});

const message = (
  name: string,
  origin: Origin,
  at: DateTime.Utc,
  fields: Partial<OrchestrationV2ConversationMessage> = {},
): OrchestrationV2ConversationMessage => ({
  id: MessageId.make(`message:${name}`),
  threadId,
  runId: RunId.make(`run:${name}`),
  nodeId: null,
  role: "user",
  text: `${name} text`,
  attachments: [],
  streaming: false,
  createdAt: at,
  updatedAt: at,
  createdBy: origin === "user" || origin === "follow-up" ? "user" : "agent",
  creationSource: origin === "user" || origin === "follow-up" ? "web" : "server",
  ...(origin === "automation" ? { scheduledTaskId: ScheduledTaskId.make("task:check") } : {}),
  ...(origin === "cross-thread" ? { senderThreadId: ThreadId.make("thread:sender") } : {}),
  ...(origin === "linked"
    ? {
        linkedSender: {
          environmentId: EnvironmentId.make("environment:remote"),
          threadId: ThreadId.make("thread:remote"),
          title: "Remote sender",
        },
      }
    : {}),
  ...(origin === "completion"
    ? {
        delegatedCompletion: {
          parentRunId: RunId.make("run:parent"),
          generation: 1,
          taskIds: [NodeId.make("task:child")],
        },
      }
    : {}),
  ...(origin === "notification"
    ? {
        notification: {
          source: { kind: "background_task" },
          outcome: "updated",
          summary: "Background activity updated",
        },
      }
    : {}),
  ...fields,
});

const turn = (
  target: OrchestrationV2Run,
  status: OrchestrationV2ProviderTurn["status"],
  startedAt: DateTime.Utc | null,
): OrchestrationV2ProviderTurn => ({
  id: ProviderTurnId.make(`provider-turn:${target.id}`),
  providerThreadId,
  nodeId: target.rootNodeId!,
  runAttemptId: target.activeAttemptId,
  nativeTurnRef: null,
  ordinal: 1,
  status,
  startedAt,
  completedAt: null,
});

const stopRequest = (target: OrchestrationV2Run, at: DateTime.Utc): OrchestrationV2TurnItem => ({
  id: TurnItemId.make("item:stop"),
  threadId,
  runId: target.id,
  nodeId: target.rootNodeId!,
  providerThreadId,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 1,
  status: "completed",
  title: null,
  startedAt: at,
  completedAt: at,
  updatedAt: at,
  type: "run_interrupt_request",
  message: "Stop",
  holdQueue: true,
});

const compaction = (target: OrchestrationV2Run, at: DateTime.Utc): OrchestrationV2TurnItem => ({
  id: TurnItemId.make("item:compaction"),
  threadId,
  runId: target.id,
  nodeId: target.rootNodeId!,
  providerThreadId,
  providerTurnId: null,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 2,
  status: "running",
  title: null,
  startedAt: at,
  completedAt: null,
  updatedAt: at,
  type: "compaction",
  driver: null,
});

const steerItem = (
  target: OrchestrationV2Run,
  providerTurnId: ProviderTurnId,
  at: DateTime.Utc,
  automatic: boolean,
): OrchestrationV2TurnItem => ({
  id: TurnItemId.make(`item:steer:${providerTurnId}`),
  threadId,
  runId: target.id,
  nodeId: target.rootNodeId!,
  providerThreadId,
  providerTurnId,
  nativeItemRef: null,
  parentItemId: null,
  ordinal: 3,
  status: "completed",
  title: null,
  startedAt: at,
  completedAt: at,
  updatedAt: at,
  type: "user_message",
  createdBy: "user",
  creationSource: "web",
  messageId: MessageId.make("message:earlier-steer"),
  inputIntent: "promoted_queued_to_steer",
  ...(automatic ? { automaticDelivery: true as const } : {}),
  text: "earlier steer",
  attachments: [],
});

interface Scenario {
  readonly active?: OrchestrationV2Run["status"] | "none";
  readonly turnStatus?: OrchestrationV2ProviderTurn["status"] | "none";
  readonly turnStartedAt?: DateTime.Utc;
  readonly activeText?: string;
  readonly queued: ReadonlyArray<{
    readonly name: string;
    readonly origin: Origin;
    readonly run?: Partial<OrchestrationV2Run>;
    readonly message?: Partial<OrchestrationV2ConversationMessage>;
  }>;
  readonly stopReached?: boolean;
  readonly compacting?: boolean;
  /** A steer that already went into the live turn, or into an earlier turn of the run. */
  readonly steered?: {
    readonly at: DateTime.Utc;
    readonly turn: "live" | "earlier";
    /** Automatic Send now steered it, not the user or an agent. */
    readonly automatic?: boolean;
  };
  readonly capabilities?: Partial<OrchestrationV2TurnCapabilities>;
  readonly pendingRequest?: boolean;
  readonly otherActive?: boolean;
}

// Every queued run is requested at the scenario's start; the test clock then moves on.
const decide = (scenario: Scenario, start: DateTime.Utc) =>
  Effect.gen(function* () {
    const target = run("active", 1, start, {
      status: scenario.active === "none" ? "completed" : (scenario.active ?? "running"),
      queuePosition: null,
      startedAt: start,
    });
    const queuedRuns = scenario.queued.map((item, index) =>
      run(item.name, index + 2, start, {
        ...(item.origin === "follow-up" ? { steerFollowUpOfRunId: RunId.make("run:old") } : {}),
        ...item.run,
      }),
    );
    const messages = [
      message("active", "user", start, { text: scenario.activeText ?? "Keep working." }),
      ...scenario.queued.map((item) => message(item.name, item.origin, start, item.message)),
    ];
    return decideQueuedDeliveryPromotion({
      now: yield* DateTime.now,
      runs: [
        ...(scenario.otherActive === true
          ? [run("other", 0, start, { status: "starting", queuePosition: null })]
          : []),
        target,
        ...queuedRuns,
      ],
      runtimeRequests: scenario.pendingRequest === true ? [{ status: "pending" }] : [],
      messages,
      providerTurns:
        scenario.turnStatus === "none"
          ? []
          : [turn(target, scenario.turnStatus ?? "running", scenario.turnStartedAt ?? null)],
      targetTurnItems: [
        ...(scenario.stopReached === true ? [stopRequest(target, start)] : []),
        ...(scenario.compacting === true ? [compaction(target, start)] : []),
        ...(scenario.steered === undefined
          ? []
          : [
              steerItem(
                target,
                scenario.steered.turn === "live"
                  ? turn(target, "running", null).id
                  : ProviderTurnId.make("provider-turn:earlier"),
                scenario.steered.at,
                scenario.steered.automatic ?? true,
              ),
            ]),
      ],
      capabilities: { ...claudeTurns, ...scenario.capabilities },
      modelSelectionOf: (queued) => queued.modelSelection,
    });
  });

const retryAt = (scenario: Scenario, start: DateTime.Utc) =>
  Effect.gen(function* () {
    const target = run("active", 1, start, {
      status: "running",
      queuePosition: null,
      startedAt: start,
    });
    return decideQueuedDelivery({
      now: yield* DateTime.now,
      runs: [
        target,
        ...scenario.queued.map((item, index) => run(item.name, index + 2, start, item.run)),
      ],
      messages: scenario.queued.map((item) => message(item.name, item.origin, start)),
      providerTurns: [turn(target, "running", null)],
      runtimeRequests: [],
      targetTurnItems:
        scenario.steered === undefined
          ? []
          : [
              steerItem(
                target,
                turn(target, "running", null).id,
                scenario.steered.at,
                scenario.steered.automatic ?? true,
              ),
            ],
      capabilities: claudeTurns,
      modelSelectionOf: (queued) => queued.modelSelection,
    }).retryAt;
  });

const promotedRun = (decision: QueuedDeliveryPromotion | null) => decision?.queuedRunId ?? null;
const steeredRuns = (decision: QueuedDeliveryPromotion | null) =>
  decision === null ? [] : [decision.queuedRunId, ...(decision.mergedQueuedRunIds ?? [])];

it.effect.each(["automation", "cross-thread", "linked", "user"] as const)(
  "promotes a queued %s message into the live turn once 60 s have passed",
  (origin) =>
    Effect.gen(function* () {
      const start = yield* DateTime.now;
      const scenario: Scenario = { queued: [{ name: "incoming", origin }] };
      yield* TestClock.adjust("59 seconds");
      assert.isNull(yield* decide(scenario, start));
      yield* TestClock.adjust("1 second");
      const decision = yield* decide(scenario, start);
      assert.deepEqual(decision, {
        queuedRunId: RunId.make("run:incoming"),
        targetRunId: RunId.make("run:active"),
        commandId: "command:auto-send-now:run:incoming:run:active:provider-turn:run:active",
      } as never);
    }),
);

it.effect("never promotes a hidden wake or an agent's message to its own thread", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("1 hour");
    for (const origin of ["completion", "notification"] as const) {
      assert.isNull(yield* decide({ queued: [{ name: origin, origin }] }, start), origin);
    }
    assert.isNull(
      yield* decide(
        {
          queued: [
            {
              name: "self",
              origin: "cross-thread",
              message: { senderThreadId: threadId, creationSource: "mcp" },
            },
          ],
        },
        start,
      ),
    );
  }),
);

it.effect("steers a scheduled prompt at once where steering leaves running tools alone", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    const queued: Scenario["queued"] = [{ name: "automation", origin: "automation" }];
    yield* TestClock.adjust("1 second");
    assert.isNull(yield* decide({ queued }, start));
    assert.equal(
      promotedRun(
        yield* decide({ capabilities: { activeSteeringInterruptsTools: false }, queued }, start),
      ),
      "run:automation",
    );
    assert.isNull(
      yield* decide(
        {
          capabilities: { activeSteeringInterruptsTools: false },
          queued: [{ name: "cross-thread", origin: "cross-thread" }],
        },
        start,
      ),
    );
  }),
);

it.effect("skips a pile of hidden wakes queued ahead of a visible message", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("60 seconds");
    const decision = yield* decide(
      {
        queued: [
          ...Array.from({ length: 20 }, (_, index) => ({
            name: `wake-${index}`,
            origin: index % 2 === 0 ? ("completion" as const) : ("notification" as const),
          })),
          { name: "user", origin: "user" },
          { name: "automation", origin: "automation" },
          { name: "cross-thread", origin: "cross-thread" },
        ],
      },
      start,
    );
    assert.equal(promotedRun(decision), "run:user");
  }),
);

it.effect.each([
  {
    name: "a message the thread queued for itself",
    origin: "cross-thread",
    message: { senderThreadId: threadId },
  },
  { name: "a fresh-session message", run: { freshContext: true } },
  { name: "a held queue", run: { queueHeld: true } },
  { name: "/goal", message: { text: "/goal ship it" } },
  { name: "/compact", message: { text: "/compact" } },
  { name: "/logout", message: { text: " /LOGOUT " } },
  {
    name: "attachments the provider cannot steer",
    message: {
      attachments: [
        {
          type: "image",
          id: ChatAttachmentId.make("attachment-image"),
          name: "screen.png",
          mimeType: "image/png",
          sizeBytes: 1,
        },
      ],
    },
    capabilities: { activeSteeringAcceptsAttachments: false },
  },
  {
    name: "another model selection",
    run: { modelSelection: { ...selection, model: "claude-haiku" } },
  },
] satisfies ReadonlyArray<{
  readonly name: string;
  readonly origin?: Origin;
  readonly run?: Partial<OrchestrationV2Run>;
  readonly message?: Partial<OrchestrationV2ConversationMessage>;
  readonly capabilities?: Partial<OrchestrationV2TurnCapabilities>;
}>)("does not promote $name", (item) =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("10 minutes");
    const decision = yield* decide(
      {
        queued: [
          {
            name: "excluded",
            origin: item.origin ?? "automation",
            ...(item.run === undefined ? {} : { run: item.run }),
            ...(item.message === undefined ? {} : { message: item.message }),
          },
        ],
        ...(item.capabilities === undefined ? {} : { capabilities: item.capabilities }),
      },
      start,
    );
    assert.isNull(decision);
  }),
);

// A gap waits for the turn: its start triggers the next decision, made on what is queued then.
it.effect.each([
  { active: "running", turnStatus: "running", steers: true },
  { active: "waiting", turnStatus: "running", steers: true },
  { active: "preparing", turnStatus: "none", steers: false },
  { active: "starting", turnStatus: "pending", steers: false },
  { active: "running", turnStatus: "completed", steers: false },
  // A waiting run whose turn is over ends its wait for the message instead.
  { active: "waiting", turnStatus: "completed", steers: true },
  { active: "waiting", turnStatus: "pending", steers: false },
  { active: "waiting", turnStatus: "none", steers: false },
  { active: "none", turnStatus: "none", steers: false },
] satisfies ReadonlyArray<{
  readonly active: Scenario["active"];
  readonly turnStatus: Scenario["turnStatus"];
  readonly steers: boolean;
}>)("a $active run with a $turnStatus turn steers: $steers", ({ active, turnStatus, steers }) =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("60 seconds");
    const decision = yield* decide(
      { active, turnStatus, queued: [{ name: "automation", origin: "automation" }] },
      start,
    );
    assert.equal(decision !== null, steers);
  }),
);

it.effect.each([
  { name: "Stop reached the run", scenario: { stopReached: true } },
  { name: "the run compacts", scenario: { activeText: "/compact" } },
  { name: "the run signs out", scenario: { activeText: "/logout" } },
  { name: "the turn compacts on its own", scenario: { compacting: true } },
  {
    name: "the provider cannot steer",
    scenario: { capabilities: { supportsActiveSteering: false } },
  },
] satisfies ReadonlyArray<{ readonly name: string; readonly scenario: Partial<Scenario> }>)(
  "waits while $name",
  ({ scenario }) =>
    Effect.gen(function* () {
      const start = yield* DateTime.now;
      yield* TestClock.adjust("10 minutes");
      assert.isNull(
        yield* decide(
          { ...scenario, queued: [{ name: "automation", origin: "automation" }] },
          start,
        ),
      );
    }),
);

it.effect.each(["user", "automation", "cross-thread"] as const)(
  "ends the wait of a run whose turn is over for a %s message after 60 s, steering or not",
  (origin) =>
    Effect.gen(function* () {
      const start = yield* DateTime.now;
      const scenario: Scenario = {
        active: "waiting",
        turnStatus: "completed",
        queued: [
          {
            name: "incoming",
            origin,
            message: {
              attachments: [
                {
                  type: "image",
                  id: ChatAttachmentId.make("attachment-waiting"),
                  name: "screen.png",
                  mimeType: "image/png",
                  sizeBytes: 1,
                },
              ],
            },
          },
        ],
        capabilities: { supportsActiveSteering: false, activeSteeringAcceptsAttachments: false },
      };
      yield* TestClock.adjust("59 seconds");
      assert.isNull(yield* decide(scenario, start));
      yield* TestClock.adjust("1 second");
      assert.deepEqual(yield* decide(scenario, start), {
        queuedRunId: RunId.make("run:incoming"),
        targetRunId: RunId.make("run:active"),
        commandId: "command:auto-send-now:run:incoming:run:active:wait",
      } as never);
      assert.isNull(yield* decide({ ...scenario, stopReached: true }, start));
      assert.isNull(yield* decide({ ...scenario, pendingRequest: true }, start));
      assert.isNull(yield* decide({ ...scenario, otherActive: true }, start));
    }),
);

// A finished compaction or sign-out waits for its checkpoint like any other turn.
it.effect.each(["/compact", "/logout"])(
  "ends the wait of a %s run whose turn is over after 60 s",
  (activeText) =>
    Effect.gen(function* () {
      const start = yield* DateTime.now;
      const scenario: Scenario = {
        active: "waiting",
        turnStatus: "completed",
        activeText,
        queued: [{ name: "incoming", origin: "user" }],
      };
      yield* TestClock.adjust("59 seconds");
      assert.isNull(yield* decide(scenario, start));
      yield* TestClock.adjust("1 second");
      assert.equal(promotedRun(yield* decide(scenario, start)), RunId.make("run:incoming"));
    }),
);

// Ending a wait starts the message as a run of its own, which any visible message may be.
it.effect.each([
  { name: "a fresh-session message", run: { freshContext: true }, message: {} },
  { name: "a goal command", run: {}, message: { text: "/goal ship it" } },
  { name: "a compaction", run: {}, message: { text: "/compact" } },
  {
    name: "a steer deferred to the waiting run",
    run: { steerWhenReadyRunId: RunId.make("run:active") },
    message: {},
  },
] satisfies ReadonlyArray<{
  readonly name: string;
  readonly run: Partial<OrchestrationV2Run>;
  readonly message: Partial<OrchestrationV2ConversationMessage>;
}>)("ends the wait of a run whose turn is over for $name after 60 s", ({ run, message }) =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    const scenario: Scenario = {
      active: "waiting",
      turnStatus: "completed",
      queued: [{ name: "incoming", origin: "user", run, message }],
    };
    yield* TestClock.adjust("59 seconds");
    assert.isNull(yield* decide(scenario, start));
    yield* TestClock.adjust("1 second");
    assert.equal(promotedRun(yield* decide(scenario, start)), RunId.make("run:incoming"));
  }),
);

it.effect("steers during a running compaction where the provider keeps it intact", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("60 seconds");
    const decision = yield* decide(
      {
        compacting: true,
        capabilities: { activeSteeringDuringCompaction: true },
        queued: [{ name: "automation", origin: "automation" }],
      },
      start,
    );
    assert.equal(promotedRun(decision), "run:automation");
  }),
);

it.effect("retries a steer that missed its turn at once, and leaves one waiting on this run", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("1 second");
    assert.equal(
      promotedRun(
        yield* decide(
          {
            turnStartedAt: start,
            queued: [
              {
                name: "deferred-here",
                origin: "automation",
                run: { steerWhenReadyRunId: RunId.make("run:active") },
              },
              { name: "follow-up", origin: "follow-up" },
            ],
          },
          start,
        ),
      ),
      "run:follow-up",
    );
    assert.equal(
      promotedRun(
        yield* decide(
          {
            queued: [
              {
                name: "deferred-before",
                origin: "user",
                run: { steerWhenReadyRunId: RunId.make("run:ended") },
              },
            ],
          },
          start,
        ),
      ),
      "run:deferred-before",
    );
    yield* TestClock.adjust("1 hour");
    assert.isNull(
      yield* decide(
        {
          queued: [
            {
              name: "deferred-here",
              origin: "automation",
              run: { steerWhenReadyRunId: RunId.make("run:active") },
            },
          ],
        },
        start,
      ),
    );
  }),
);

it.effect(
  "merges every due message into one steer: the user's own first, then automations, then other threads', each in queue order",
  () =>
    Effect.gen(function* () {
      const start = yield* DateTime.now;
      yield* TestClock.adjust("2 minutes");
      const decision = yield* decide(
        {
          queued: [
            { name: "automation-late", origin: "automation", run: { queuePosition: 9 } },
            { name: "cross-thread", origin: "cross-thread", run: { queuePosition: 3 } },
            { name: "user-late", origin: "user", run: { queuePosition: 12 } },
            { name: "linked", origin: "linked", run: { queuePosition: 5 } },
            { name: "automation-early", origin: "automation", run: { queuePosition: 4 } },
            {
              name: "user-over-mcp",
              origin: "user",
              run: { queuePosition: 1 },
              message: { creationSource: "mcp" },
            },
            { name: "user-early", origin: "user", run: { queuePosition: 2 } },
            { name: "wake", origin: "completion", run: { queuePosition: 6 } },
          ],
        },
        start,
      );
      assert.deepEqual(steeredRuns(decision), [
        "run:user-early",
        "run:user-late",
        "run:automation-early",
        "run:automation-late",
        "run:cross-thread",
        "run:linked",
        "run:user-over-mcp",
      ] as never);
      assert.match(
        decision!.commandId,
        /^command:auto-send-now:run:user-early:run:active:provider-turn:run:active:merged:7:[0-9a-f]{8}$/,
      );
    }),
);

it.effect("caps a merged steer by the text the provider gets, context expanded", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("2 minutes");
    const annotated = (name: string): Scenario["queued"][number] => {
      const record: ComposerContextRecord = {
        version: 1,
        contextId: ComposerContextId.make(`ctx_${name}`),
        kind: "preview-annotation",
        label: "Checkout",
        annotationId: `ann_${name}`,
        pageUrl: "http://localhost:3000/checkout",
        pageTitle: "Checkout",
        comment: "c".repeat(Math.floor(QUEUED_DELIVERY_MERGE_MAX_BYTES * 0.6)),
        targetSummary: "1 selected element",
        styleChanges: [],
        elements: [],
      };
      return {
        name,
        origin: "cross-thread",
        message: {
          text: `[Checkout](t3-context://v1/preview-annotation/ctx_${name})`,
          context: { version: 1, records: [record] },
        },
      };
    };
    const decision = yield* decide(
      { queued: [annotated("first"), annotated("second"), { name: "short", origin: "user" }] },
      start,
    );
    assert.deepEqual(steeredRuns(decision), ["run:short", "run:first"] as never);
  }),
);

it.effect("steers a slash command alone, so it still starts its text", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("2 minutes");
    const command = yield* decide(
      {
        queued: [
          { name: "review", origin: "user", message: { text: "/review inspect auth" } },
          { name: "plain", origin: "cross-thread" },
        ],
      },
      start,
    );
    assert.deepEqual(steeredRuns(command), ["run:review"] as never);
    const behind = yield* decide(
      {
        queued: [
          { name: "plain", origin: "user" },
          { name: "review", origin: "automation", message: { text: "  /review inspect auth" } },
          { name: "later", origin: "cross-thread" },
        ],
      },
      start,
    );
    assert.deepEqual(steeredRuns(behind), ["run:plain"] as never);
  }),
);

it.effect("caps a merged steer by messages and text, oldest first; a long message goes alone", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("2 minutes");
    const many = yield* decide(
      {
        queued: Array.from({ length: QUEUED_DELIVERY_MERGE_MAX_ITEMS + 5 }, (_, index) => ({
          name: `peer-${index}`,
          origin: "cross-thread" as const,
        })),
      },
      start,
    );
    assert.deepEqual(
      steeredRuns(many),
      Array.from(
        { length: QUEUED_DELIVERY_MERGE_MAX_ITEMS },
        (_, index) => `run:peer-${index}`,
      ) as never,
    );
    const third = "x".repeat(Math.floor(QUEUED_DELIVERY_MERGE_MAX_BYTES / 3));
    const long = yield* decide(
      {
        queued: [
          { name: "first", origin: "cross-thread", message: { text: third } },
          { name: "second", origin: "cross-thread", message: { text: third } },
          { name: "third", origin: "cross-thread", message: { text: `${third}xxx` } },
          { name: "short", origin: "cross-thread" },
        ],
      },
      start,
    );
    assert.deepEqual(steeredRuns(long), ["run:first", "run:second"] as never);
    const alone = yield* decide(
      {
        queued: [
          {
            name: "huge",
            origin: "user",
            message: { text: "y".repeat(QUEUED_DELIVERY_MERGE_MAX_BYTES + 1) },
          },
          { name: "short", origin: "cross-thread" },
        ],
      },
      start,
    );
    assert.deepEqual(steeredRuns(alone), ["run:huge"] as never);
    assert.equal(
      alone?.commandId,
      "command:auto-send-now:run:huge:run:active:provider-turn:run:active",
    );
  }),
);

it.effect("is held back only by automatic steers, never by one a user or another thread sent", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("2 minutes");
    const queued: Scenario["queued"] = [{ name: "automation", origin: "automation" }];
    for (const ago of ["1 second", "30 seconds"] as const) {
      const steeredAt = DateTime.subtractDuration(
        yield* DateTime.now,
        Duration.fromInputUnsafe(ago),
      );
      assert.equal(
        promotedRun(
          yield* decide(
            { steered: { at: steeredAt, turn: "live", automatic: false }, queued },
            start,
          ),
        ),
        "run:automation",
        ago,
      );
      assert.isNull(
        yield* decide({ steered: { at: steeredAt, turn: "live", automatic: true }, queued }, start),
        ago,
      );
    }
  }),
);

it.effect("carries the messages that come due before the next automatic steer could go", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("2 minutes");
    const now = yield* DateTime.now;
    const decision = yield* decide(
      {
        queued: [
          { name: "due", origin: "cross-thread" },
          {
            name: "rider",
            origin: "automation",
            run: { requestedAt: DateTime.subtractDuration(now, Duration.seconds(1)) },
          },
          { name: "fresh", origin: "user", run: { requestedAt: now } },
        ],
      },
      start,
    );
    assert.deepEqual(steeredRuns(decision), ["run:fresh", "run:rider", "run:due"] as never);
  }),
);

it.effect("never lets a message still in its grace displace a due one", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("2 minutes");
    const fresh = { requestedAt: DateTime.subtractDuration(yield* DateTime.now, "1 second") };
    // A slash command steers alone, so as the first rider it would take the whole steer.
    const command = yield* decide(
      {
        queued: [
          { name: "overdue", origin: "cross-thread" },
          { name: "review", origin: "user", run: fresh, message: { text: "/review auth" } },
        ],
      },
      start,
    );
    assert.deepEqual(steeredRuns(command), ["run:overdue"] as never);
    const crowd = yield* decide(
      {
        queued: [
          ...Array.from({ length: 20 }, (_, index) => ({
            name: `due-${index}`,
            origin: "cross-thread" as const,
          })),
          ...Array.from({ length: 10 }, (_, index) => ({
            name: `rider-${index}`,
            origin: "user" as const,
            run: fresh,
          })),
        ],
      },
      start,
    );
    assert.includeMembers(
      [...steeredRuns(crowd)] as Array<string>,
      Array.from({ length: 20 }, (_, index) => `run:due-${index}`),
    );
    assert.lengthOf(steeredRuns(crowd), QUEUED_DELIVERY_MERGE_MAX_ITEMS);
    const third = "x".repeat(Math.floor(QUEUED_DELIVERY_MERGE_MAX_BYTES / 3));
    const heavy = yield* decide(
      {
        queued: [
          { name: "due-a", origin: "cross-thread", message: { text: third } },
          { name: "due-b", origin: "cross-thread", message: { text: third } },
          { name: "rider", origin: "user", run: fresh, message: { text: third } },
        ],
      },
      start,
    );
    assert.deepEqual(steeredRuns(heavy), ["run:due-a", "run:due-b"] as never);
  }),
);

it.effect("always carries the oldest due message, whatever outranks it", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("2 minutes");
    // More of the user's own messages than one steer takes, all queued after the oldest.
    const flood = yield* decide(
      {
        queued: [
          { name: "oldest", origin: "cross-thread" },
          ...Array.from({ length: QUEUED_DELIVERY_MERGE_MAX_ITEMS + 5 }, (_, index) => ({
            name: `user-${index}`,
            origin: "user" as const,
          })),
        ],
      },
      start,
    );
    assert.include([...steeredRuns(flood)] as Array<string>, "run:oldest");
    assert.lengthOf(steeredRuns(flood), QUEUED_DELIVERY_MERGE_MAX_ITEMS);
    // A due slash command of a higher class goes alone, but only once it is the oldest.
    const command = yield* decide(
      {
        queued: [
          { name: "oldest", origin: "cross-thread" },
          { name: "review", origin: "user", message: { text: "/review auth" } },
        ],
      },
      start,
    );
    assert.deepEqual(steeredRuns(command), ["run:oldest"] as never);
    const alone = yield* decide(
      {
        queued: [
          { name: "review", origin: "automation", message: { text: "/review auth" } },
          { name: "user", origin: "user" },
        ],
      },
      start,
    );
    assert.deepEqual(steeredRuns(alone), ["run:review"] as never);
  }),
);

it.effect("names when the first queued message comes due while none is", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("2 minutes");
    const queuedAt = DateTime.subtractDuration(yield* DateTime.now, Duration.seconds(30));
    const scenario: Scenario = {
      queued: [
        {
          name: "later",
          origin: "cross-thread",
          run: { requestedAt: DateTime.addDuration(queuedAt, Duration.seconds(10)) },
        },
        { name: "user", origin: "user", run: { requestedAt: queuedAt } },
      ],
    };
    assert.isNull(yield* decide(scenario, start));
    assert.deepEqual(
      yield* retryAt(scenario, start),
      DateTime.addDuration(queuedAt, Duration.seconds(60)),
    );
  }),
);

it.effect("names the moment the spacing lets a waiting due message in", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    yield* TestClock.adjust("2 minutes");
    const steeredAt = yield* DateTime.now;
    const queued: Scenario["queued"] = [{ name: "user", origin: "user" }];
    yield* TestClock.adjust("25 seconds");
    assert.deepEqual(
      yield* retryAt({ steered: { at: steeredAt, turn: "live" }, queued }, start),
      DateTime.addDuration(steeredAt, Duration.seconds(60)),
    );
    assert.isNull(
      yield* retryAt({ steered: { at: steeredAt, turn: "live", automatic: false }, queued }, start),
    );
    assert.isNull(yield* retryAt({ steered: { at: steeredAt, turn: "live" }, queued: [] }, start));
  }),
);

it.effect("steers a message deferred to this run once its turn ran a whole tick without it", () =>
  Effect.gen(function* () {
    const start = yield* DateTime.now;
    const scenario: Scenario = {
      turnStartedAt: start,
      queued: [
        {
          name: "deferred-here",
          origin: "user",
          run: { steerWhenReadyRunId: RunId.make("run:active") },
        },
      ],
    };
    yield* TestClock.adjust("4 seconds");
    assert.isNull(yield* decide(scenario, start));
    yield* TestClock.adjust("1 second");
    assert.equal(promotedRun(yield* decide(scenario, start)), "run:deferred-here");
    assert.isNull(yield* decide({ ...scenario, turnStatus: "pending" }, start));
  }),
);

it.effect(
  "retries a missed steer into another run, or into a turn of its run that started since",
  () =>
    Effect.gen(function* () {
      const start = yield* DateTime.now;
      const missedAt = DateTime.addDuration(start, Duration.seconds(10));
      const queued: Scenario["queued"] = [
        {
          name: "follow-up",
          origin: "follow-up",
          run: { requestedAt: missedAt, steerFollowUpOfRunId: RunId.make("run:active") },
        },
      ];
      yield* TestClock.adjust("20 seconds");
      assert.isNull(yield* decide({ turnStartedAt: start, queued }, start));
      assert.isNull(yield* decide({ queued }, start));
      assert.equal(
        promotedRun(
          yield* decide(
            { turnStartedAt: DateTime.addDuration(start, Duration.seconds(15)), queued },
            start,
          ),
        ),
        "run:follow-up",
      );
      assert.isNull(yield* decide({ turnStatus: "completed", queued }, start));
      assert.equal(
        promotedRun(
          yield* decide(
            {
              turnStartedAt: start,
              queued: [{ name: "follow-up", origin: "follow-up", run: { requestedAt: missedAt } }],
            },
            start,
          ),
        ),
        "run:follow-up",
      );
    }),
);

it.effect(
  "steers into one turn at most once a minute automatically; another turn takes one at once",
  () =>
    Effect.gen(function* () {
      const start = yield* DateTime.now;
      yield* TestClock.adjust("2 minutes");
      const steeredAt = yield* DateTime.now;
      const queued: Scenario["queued"] = [
        { name: "automation", origin: "automation" },
        { name: "follow-up", origin: "follow-up" },
      ];
      yield* TestClock.adjust("59 seconds");
      assert.isNull(yield* decide({ steered: { at: steeredAt, turn: "live" }, queued }, start));
      assert.deepEqual(
        steeredRuns(yield* decide({ steered: { at: steeredAt, turn: "earlier" }, queued }, start)),
        ["run:follow-up", "run:automation"] as never,
      );
      yield* TestClock.adjust("1 second");
      assert.deepEqual(
        steeredRuns(yield* decide({ steered: { at: steeredAt, turn: "live" }, queued }, start)),
        ["run:follow-up", "run:automation"] as never,
      );
    }),
);

import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ThreadProjection,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  waitEndableRun,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { ClaudeProviderCapabilitiesV2 } from "./Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import { CursorProviderCapabilitiesV2 } from "@t3tools/provider-cursor/testing";
import { makeGrokAcpAdapterFlavor } from "@t3tools/provider-grok/testing";
import * as CommandPolicy from "./CommandPolicy.ts";

const commandId = CommandId.make("command-policy-test");
const threadId = ThreadId.make("command-policy-thread");
const activeRunId = RunId.make("command-policy-active-run");
const activeAttemptId = RunAttemptId.make("command-policy-active-attempt");
const userMessageId = MessageId.make("command-policy-user-message");

const baseCapabilities: OrchestrationV2ProviderCapabilities = CodexProviderCapabilitiesV2;

function capabilities(
  override: (current: OrchestrationV2ProviderCapabilities) => OrchestrationV2ProviderCapabilities,
): OrchestrationV2ProviderCapabilities {
  return override(baseCapabilities);
}

function dispatchProjection(
  sessionCapabilities?: OrchestrationV2ProviderCapabilities,
): OrchestrationV2ThreadProjection {
  const providerThreadId = ProviderThreadId.make("command-policy-provider-thread");
  const providerSessionId = ProviderSessionId.make("command-policy-provider-session");
  return {
    runs:
      sessionCapabilities === undefined
        ? []
        : [
            {
              id: activeRunId,
              status: "running",
              providerThreadId,
              activeAttemptId,
              userMessageId,
            },
          ],
    messages: [{ id: userMessageId, text: "first", attachments: [] }],
    providerThreads:
      sessionCapabilities === undefined ? [] : [{ id: providerThreadId, providerSessionId }],
    providerSessions:
      sessionCapabilities === undefined
        ? []
        : [{ id: providerSessionId, capabilities: sessionCapabilities }],
    providerTurns:
      sessionCapabilities === undefined
        ? []
        : [{ runAttemptId: activeAttemptId, status: "running" }],
  } as unknown as OrchestrationV2ThreadProjection;
}

it("resolves automatic message delivery from authoritative provider capabilities", () => {
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "start_immediately" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(baseCapabilities),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: true,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      dispatchProjection(
        capabilities((current) => ({
          ...current,
          turns: {
            ...current.turns,
            supportsActiveSteering: false,
            supportsQueuedMessages: false,
            supportsSteeringByInterruptRestart: true,
          },
        })),
      ),
      { type: "start_immediately" },
      "auto",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
});

it.each(["preparing", "starting"] as const)(
  "steers an automatic message into the next turn while the handoff run is %s",
  (status) => {
    const projection = dispatchProjection(baseCapabilities);
    assert.deepEqual(
      CommandPolicy.resolveMessageDispatchIntent(
        { ...projection, runs: projection.runs.map((run) => ({ ...run, status })) },
        { type: "start_immediately" },
        "auto",
      ),
      { type: "steer_active", targetRunId: activeRunId },
    );
  },
);

it.each([
  ["has not started", []],
  ["is pending", [{ runAttemptId: activeAttemptId, status: "pending" }]],
  ["has completed", [{ runAttemptId: activeAttemptId, status: "completed" }]],
  ["has failed", [{ runAttemptId: activeAttemptId, status: "failed" }]],
  ["was interrupted", [{ runAttemptId: activeAttemptId, status: "interrupted" }]],
  ["was cancelled", [{ runAttemptId: activeAttemptId, status: "cancelled" }]],
  [
    "belongs to a previous attempt",
    [{ runAttemptId: RunAttemptId.make("previous-attempt"), status: "running" }],
  ],
])("resolves delivery when the active run's provider turn %s", (label, providerTurns) => {
  const projection = {
    ...dispatchProjection(baseCapabilities),
    providerTurns,
  } as unknown as OrchestrationV2ThreadProjection;
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(projection, { type: "start_immediately" }, "auto"),
    // An ended turn means its run is ending, so the next run takes the message.
    label === "has not started" || label === "is pending" || label.includes("previous attempt")
      ? { type: "steer_active", targetRunId: activeRunId }
      : { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(projection, { type: "start_immediately" }, "steer"),
    { type: "steer_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      projection,
      { type: "start_immediately" },
      "restart",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
});

it("steers automatic delivery into a turn gap only where it would steer a live turn", () => {
  const gap = (
    sessionCapabilities: OrchestrationV2ProviderCapabilities | undefined,
    status: "starting" | "waiting" = "starting",
    text = "first",
  ) => {
    const projection = dispatchProjection(baseCapabilities);
    return {
      ...projection,
      runs: projection.runs.map((run) => ({ ...run, status })),
      messages: [{ id: userMessageId, text, attachments: [] }],
      providerSessions: projection.providerSessions.flatMap((session) =>
        sessionCapabilities === undefined
          ? []
          : [{ ...session, capabilities: sessionCapabilities }],
      ),
      providerTurns: [],
    } as unknown as OrchestrationV2ThreadProjection;
  };
  const restartOnly = capabilities((current) => ({
    ...current,
    turns: {
      ...current.turns,
      supportsActiveSteering: false,
      supportsInterrupt: true,
      supportsSteeringByInterruptRestart: true,
      supportsQueuedMessages: true,
    },
  }));
  const queueOnly = capabilities((current) => ({
    ...current,
    turns: {
      ...current.turns,
      supportsActiveSteering: false,
      supportsSteeringByInterruptRestart: false,
      supportsQueuedMessages: true,
    },
  }));
  const resolve = (projection: OrchestrationV2ThreadProjection, preferRestartSteering = false) =>
    CommandPolicy.resolveMessageDispatchIntent(
      projection,
      { type: "start_immediately" },
      "auto",
      preferRestartSteering,
    );
  const steer = { type: "steer_active" as const, targetRunId: activeRunId };
  const queue = { type: "queue_after_active" as const };
  // Before its session opens the run has no capabilities; dispatch checks the provider's own.
  assert.deepEqual(resolve(gap(undefined)), steer);
  assert.deepEqual(resolve(gap(restartOnly)), queue);
  assert.deepEqual(resolve(gap(restartOnly), true), steer);
  assert.deepEqual(resolve(gap(queueOnly), true), queue);
  assert.deepEqual(resolve(gap(baseCapabilities, "waiting")), queue);
  assert.deepEqual(resolve(gap(baseCapabilities, "starting", "/compact")), queue);
});

// regression: #15892 a scheduled prompt cancels the tools running in its bound thread
it("steers a scheduled prompt into a live turn only where its running tools keep running", () => {
  const interruptsTools = capabilities((current) => ({
    ...current,
    turns: { ...current.turns, supportsActiveSteering: true, activeSteeringInterruptsTools: true },
  }));
  const restartOnly = capabilities((current) => ({
    ...current,
    turns: {
      ...current.turns,
      supportsActiveSteering: false,
      supportsInterrupt: true,
      supportsSteeringByInterruptRestart: true,
      supportsQueuedMessages: true,
    },
  }));
  const resolve = (projection: OrchestrationV2ThreadProjection, scheduledPrompt: boolean) =>
    CommandPolicy.resolveMessageDispatchIntent(
      projection,
      { type: "start_immediately" },
      "auto",
      true,
      scheduledPrompt,
    );
  const steer = { type: "steer_active" as const, targetRunId: activeRunId };
  const queue = { type: "queue_after_active" as const };
  assert.deepEqual(resolve(dispatchProjection(baseCapabilities), true), steer);
  assert.deepEqual(resolve(dispatchProjection(interruptsTools), true), queue);
  assert.deepEqual(resolve(dispatchProjection(restartOnly), true), queue);
  assert.deepEqual(resolve(dispatchProjection(interruptsTools), false), steer);
  // The run's next turn has no tools running yet, so a gap still takes it as a deferred steer.
  const gap = {
    ...dispatchProjection(interruptsTools),
    providerTurns: [],
  } as unknown as OrchestrationV2ThreadProjection;
  assert.deepEqual(resolve(gap, true), steer);
});

// regression: #26695 an interrupt-restart steer cancels Copilot's background agents
it("queues a steer for a provider that cannot steer either way", () => {
  const projection = dispatchProjection(
    capabilities((current) => ({
      ...current,
      turns: {
        ...current.turns,
        supportsActiveSteering: false,
        supportsSteeringByInterruptRestart: false,
        supportsQueuedMessages: true,
      },
    })),
  );
  for (const deliveryIntent of ["auto", "steer"] as const) {
    assert.deepEqual(
      CommandPolicy.resolveMessageDispatchIntent(
        projection,
        { type: "start_immediately" },
        deliveryIntent,
      ),
      { type: "queue_after_active" },
    );
  }
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(projection, {
      type: "steer_active",
      targetRunId: activeRunId,
    }),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      projection,
      { type: "start_immediately" },
      "restart",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(projection, {
      type: "steer_active",
      targetRunId: RunId.make("command-policy-missing-run"),
    }),
    { type: "steer_active", targetRunId: RunId.make("command-policy-missing-run") },
  );
});

// regression: #27014 a Grok follow-up cancels the foreground tool the user wanted to finish
it("does not steer a Grok follow-up by interrupting its running turn", () => {
  const grok = makeGrokAcpAdapterFlavor(
    { makeRuntime: () => Effect.never } as unknown as Parameters<
      typeof makeGrokAcpAdapterFlavor
    >[0],
    "/home/ada",
  ).capabilities;
  const projection = dispatchProjection(grok);
  for (const deliveryIntent of ["auto", "steer"] as const) {
    const mode = CommandPolicy.resolveMessageDispatchIntent(
      projection,
      { type: "start_immediately" },
      deliveryIntent,
    );
    assert.isFalse(
      mode.type === "steer_active" && !grok.turns.supportsActiveSteering,
      `${deliveryIntent} follow-up resolved to ${mode.type} by interrupt-restart`,
    );
  }
});

// regression: #37326 a steered Grok follow-up waits for the turn to end instead of interjecting
it("steers a Grok follow-up into its running turn without restarting it", () => {
  const grok = makeGrokAcpAdapterFlavor(
    { makeRuntime: () => Effect.never } as unknown as Parameters<
      typeof makeGrokAcpAdapterFlavor
    >[0],
    "/home/ada",
  ).capabilities;
  const mode = CommandPolicy.resolveMessageDispatchIntent(
    dispatchProjection(grok),
    { type: "start_immediately" },
    "steer",
  );
  assert.strictEqual(mode.type, "steer_active");
  assert.isTrue(grok.turns.supportsActiveSteering);
});

// regression: #32606 a slash command steered into a running turn replaced the task
it.each([
  ["/compact", { type: "queue_after_active" }],
  ["/context", { type: "queue_after_active" }],
  [" /context all\n", { type: "queue_after_active" }],
  ["/plugin:review-pr 12", { type: "queue_after_active" }],
  ["/home/me/x.ts fails", { type: "steer_active", targetRunId: activeRunId }],
  ["/ context", { type: "steer_active", targetRunId: activeRunId }],
  ["$skill /context", { type: "steer_active", targetRunId: activeRunId }],
] as const)("steers %j only when it is not a slash command", (text, expected) => {
  const projection = dispatchProjection(baseCapabilities);
  for (const deliveryIntent of ["auto", "steer"] as const) {
    assert.deepEqual(
      CommandPolicy.resolveMessageDispatch(projection, {
        dispatchMode: { type: "start_immediately" },
        deliveryIntent,
        text,
        attachments: [],
      }),
      expected,
    );
  }
});

it.each(["/compact", " /LOGOUT "])("queues a steer sent while %s runs", (text) => {
  const projection = dispatchProjection(baseCapabilities);
  const maintenance = {
    ...projection,
    messages: [{ id: userMessageId, text, attachments: [] }],
  } as unknown as OrchestrationV2ThreadProjection;
  for (const deliveryIntent of ["auto", "steer"] as const) {
    assert.deepEqual(
      CommandPolicy.resolveMessageDispatchIntent(
        maintenance,
        { type: "start_immediately" },
        deliveryIntent,
      ),
      { type: "queue_after_active" },
    );
  }
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(maintenance, {
      type: "steer_active",
      targetRunId: activeRunId,
    }),
    { type: "queue_after_active" },
  );
  assert.deepEqual(
    CommandPolicy.resolveMessageDispatchIntent(
      maintenance,
      { type: "start_immediately" },
      "restart",
    ),
    { type: "restart_active", targetRunId: activeRunId },
  );
});

const waitingRootNodeId = "command-policy-root-node";

function waitingProjection(input: {
  readonly runStatus?: "waiting" | "running";
  readonly turnStatus?: "pending" | "running" | "completed" | "interrupted" | null;
  readonly pendingRequest?: boolean;
  readonly otherActive?: boolean;
}): OrchestrationV2ThreadProjection {
  const turnStatus = input.turnStatus === undefined ? "completed" : input.turnStatus;
  return {
    runs: [
      {
        id: activeRunId,
        status: input.runStatus ?? "waiting",
        rootNodeId: waitingRootNodeId,
        activeAttemptId,
        userMessageId,
      },
      ...(input.otherActive === true
        ? [{ id: RunId.make("command-policy-other-run"), status: "starting" }]
        : []),
    ],
    providerTurns:
      turnStatus === null
        ? []
        : [
            {
              nodeId: waitingRootNodeId,
              runAttemptId: activeAttemptId,
              ordinal: 1,
              status: "running",
            },
            {
              nodeId: waitingRootNodeId,
              runAttemptId: activeAttemptId,
              ordinal: 2,
              status: turnStatus,
            },
          ],
    runtimeRequests:
      input.pendingRequest === true ? [{ status: "pending" }] : [{ status: "resolved" }],
  } as unknown as OrchestrationV2ThreadProjection;
}

it.each([
  { when: "its turn is over", input: {}, ends: true },
  { when: "its turn was interrupted", input: { turnStatus: "interrupted" }, ends: true },
  { when: "its turn is pending", input: { turnStatus: "pending" }, ends: false },
  { when: "its turn still runs", input: { turnStatus: "running" }, ends: false },
  { when: "it has no turn", input: { turnStatus: null }, ends: false },
  { when: "a request waits on the user", input: { pendingRequest: true }, ends: false },
  { when: "it is running", input: { runStatus: "running" }, ends: false },
  { when: "another run is active", input: { otherActive: true }, ends: false },
] as const)("lets visible input end a waiting run's wait when $when: $ends", ({ input, ends }) => {
  assert.equal(waitEndableRun(waitingProjection(input))?.id, ends ? activeRunId : undefined);
});

const layer = it.layer(CommandPolicy.layer);

layer("CommandPolicyV2", (it) => {
  it.effect("steers a live Cursor turn in place", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;
      const result = yield* policy.decideSteeringExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("cursor"),
        capabilities: CursorProviderCapabilitiesV2,
      });
      assert.equal(result, "active_steering");
    }),
  );

  it.effect("falls back to portable context when Cursor cannot fork natively", () =>
    Effect.gen(function* () {
      const policy = yield* CommandPolicy.CommandPolicyV2;

      const result = yield* policy.decideForkExecution({
        commandId,
        threadId,
        providerInstanceId: ProviderInstanceId.make("cursor"),
        capabilities: CursorProviderCapabilitiesV2,
        sameProvider: true,
        hasStrongNativeSource: true,
        hasNativeTurnBoundary: false,
        sourceRunStatus: "completed",
        fromSpecificTurn: true,
        sourceBusy: false,
      });

      assert.equal(result, "portable_context");
    }),
  );

  it.effect.each([
    {
      provider: "codex",
      capabilities: CodexProviderCapabilitiesV2,
      hasNativeTurnBoundary: true,
      busyStrategy: "native_fork",
    },
    {
      provider: "codex without busy forks",
      capabilities: {
        ...CodexProviderCapabilitiesV2,
        threads: { ...CodexProviderCapabilitiesV2.threads, canForkWhileSourceRunning: undefined },
      },
      hasNativeTurnBoundary: true,
      busyStrategy: "portable_context",
    },
    {
      provider: "codex",
      capabilities: CodexProviderCapabilitiesV2,
      hasNativeTurnBoundary: false,
      busyStrategy: "portable_context",
    },
    {
      provider: "claudeAgent",
      capabilities: ClaudeProviderCapabilitiesV2,
      hasNativeTurnBoundary: false,
      busyStrategy: "native_fork",
    },
  ] satisfies ReadonlyArray<{
    provider: string;
    capabilities: OrchestrationV2ProviderCapabilities;
    hasNativeTurnBoundary: boolean;
    busyStrategy: CommandPolicy.ForkExecutionPolicyV2;
  }>)(
    "forks a busy $provider source with hasNativeTurnBoundary=$hasNativeTurnBoundary as $busyStrategy",
    ({ capabilities, hasNativeTurnBoundary, busyStrategy }) =>
      Effect.gen(function* () {
        const policy = yield* CommandPolicy.CommandPolicyV2;
        const decide = (sourceBusy: boolean) =>
          policy.decideForkExecution({
            commandId,
            threadId,
            providerInstanceId: ProviderInstanceId.make("custom-instance"),
            capabilities,
            sameProvider: true,
            hasStrongNativeSource: true,
            hasNativeTurnBoundary,
            sourceRunStatus: "completed",
            fromSpecificTurn: true,
            sourceBusy,
          });

        assert.equal(yield* decide(false), "native_fork");
        assert.equal(yield* decide(true), busyStrategy);
      }),
  );
});

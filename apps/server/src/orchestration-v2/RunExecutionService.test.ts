import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  ChatAttachmentId,
  ChatFileAttachment,
  CommandId,
  MessageId,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2ContextHandoff,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2PendingBackgroundTask,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ServerSettingsError,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { CodexAppServerRequestError } from "effect-codex-app-server/errors";
import { SqlError, classifySqliteError } from "effect/sql/SqlError";

import { toPersistenceSqlError } from "../persistence/Errors.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as CheckpointService from "./CheckpointService.ts";
import type { PendingOrchestrationEffectV2 } from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderCommitBatch from "./ProviderCommitBatch.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as ProviderUsageAnalytics from "../telemetry/ProviderUsageAnalytics.ts";
import { CurrentWaitObserver } from "@t3tools/shared/WaitObserver";
import * as McpAppModelContext from "../mcpApps/McpAppModelContext.ts";
import * as PreviewAgentActivity from "../preview/PreviewAgentActivity.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as WorkspaceEntries from "../workspace/WorkspaceEntries.ts";
import { recordingWaitObserver } from "../observability/WaitObserver.testkit.ts";
import * as IngestLag from "./IngestLag.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";

const mockEventSink = (implementation: Partial<EventSink.EventSinkV2["Service"]>) =>
  Layer.mock(EventSink.EventSinkV2)({
    consumeProviderEvents: (source, process, after) =>
      source.pipe(
        Stream.mapEffect((event) => process(event).pipe(Effect.andThen(after))),
        Stream.takeUntil(Boolean),
        Stream.runDrain,
      ),
    // The newer run that claimed a provider thread has attached.
    writeProviderThreadRosterIfUnattached: () =>
      Effect.succeed({ unattached: false, storedEvents: [] }),
    ...implementation,
  });

const driver = ProviderDriverKind.make("codex");
const rootAppThread = (id: ThreadId) =>
  ({
    id,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
  }) as OrchestrationV2AppThread;

const layerTurnItemPositionsTest = Layer.mock(TurnItemPositionStore.TurnItemPositionStoreV2)({
  allocate: () => Effect.succeed(1),
});

const layerRunExecutionTest = RunExecutionService.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      McpAppModelContext.layerEmpty,
      PreviewAgentActivity.layer,
      Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
      layerTurnItemPositionsTest,
      mockEventSink({}),
      IdAllocator.layer,
      ThreadCommandExecutor.layer,
      Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
        ingestNormalized: () => Effect.succeed([]),
      }),
      ServerSettings.layerTest(),
    ),
  ),
);

it.effect("routes shared-runtime events only to their owning root run", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const first: RunExecutionService.ProviderEventRouteIdentity = {
      threadId: ThreadId.make("thread:shared-runtime:first"),
      runId: RunId.make("run:shared-runtime:first"),
      attemptId: RunAttemptId.make("attempt:shared-runtime:first"),
      providerThreadId: ProviderThreadId.make("provider-thread:shared-runtime:first"),
    };
    const second: RunExecutionService.ProviderEventRouteIdentity = {
      threadId: ThreadId.make("thread:shared-runtime:second"),
      runId: RunId.make("run:shared-runtime:second"),
      attemptId: RunAttemptId.make("attempt:shared-runtime:second"),
      providerThreadId: ProviderThreadId.make("provider-thread:shared-runtime:second"),
    };
    const firstTurnId = ProviderTurnId.make("provider-turn:shared-runtime:first");
    const turnEvent: ProviderAdapter.ProviderAdapterV2Event = {
      type: "provider_turn.updated",
      driver,
      threadId: first.threadId,
      providerTurn: {
        id: firstTurnId,
        providerThreadId: first.providerThreadId,
        nodeId: NodeId.make("node:shared-runtime:first"),
        runAttemptId: first.attemptId,
        nativeTurnRef: null,
        ordinal: 1,
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    };
    const messageEvent: ProviderAdapter.ProviderAdapterV2Event = {
      type: "message.updated",
      driver,
      message: {
        createdBy: "agent",
        creationSource: "provider",
        id: MessageId.make("message:shared-runtime:first"),
        threadId: first.threadId,
        runId: first.runId,
        nodeId: NodeId.make("node:shared-runtime:first"),
        role: "assistant",
        text: "first only",
        attachments: [],
        streaming: false,
        createdAt: now,
        updatedAt: now,
      },
    };
    const terminalEvent: ProviderAdapter.ProviderAdapterV2Event = {
      type: "turn.terminal",
      driver,
      providerThreadId: first.providerThreadId,
      providerTurnId: firstTurnId,
      runOrdinal: 1,
      status: "completed",
      failure: null,
      threadDisposition: "reusable",
    };

    const firstInitial = RunExecutionService.makeProviderEventRoutingState({
      identity: first,
      providerTurnId: null,
    });
    const secondInitial = RunExecutionService.makeProviderEventRoutingState({
      identity: second,
      providerTurnId: null,
    });
    const [firstTurnAccepted, firstAfterTurn] = RunExecutionService.routeProviderEvent(
      turnEvent,
      first,
      firstInitial,
    );
    const [secondTurnAccepted, secondAfterTurn] = RunExecutionService.routeProviderEvent(
      turnEvent,
      second,
      secondInitial,
    );

    assert.isTrue(firstTurnAccepted);
    assert.isFalse(secondTurnAccepted);
    assert.isTrue(RunExecutionService.routeProviderEvent(messageEvent, first, firstAfterTurn)[0]);
    assert.isFalse(
      RunExecutionService.routeProviderEvent(messageEvent, second, secondAfterTurn)[0],
    );
    assert.isTrue(RunExecutionService.routeProviderEvent(terminalEvent, first, firstAfterTurn)[0]);
    assert.isFalse(
      RunExecutionService.routeProviderEvent(terminalEvent, second, secondAfterTurn)[0],
    );
  }),
);

it("drops only another run's root work on threads the run does not own", () => {
  const identity: RunExecutionService.ProviderEventRouteIdentity = {
    threadId: ThreadId.make("thread:drop:self"),
    runId: RunId.make("run:drop:self"),
    attemptId: RunAttemptId.make("attempt:drop:self"),
    providerThreadId: ProviderThreadId.make("provider-thread:drop:self"),
  };
  const other = {
    threadId: ThreadId.make("thread:drop:other"),
    runId: RunId.make("run:drop:other"),
    attemptId: RunAttemptId.make("attempt:drop:other"),
    providerThreadId: ProviderThreadId.make("provider-thread:drop:other"),
  };
  const ownedChild = ThreadId.make("thread:drop:owned-child");
  const ownedChildProviderThread = ProviderThreadId.make("provider-thread:drop:owned-child");
  const state = {
    ...RunExecutionService.makeProviderEventRoutingState({ identity, providerTurnId: null }),
    ownedThreadIds: new Set([identity.threadId, ownedChild]),
    ownedProviderThreadIds: new Set([identity.providerThreadId, ownedChildProviderThread]),
  };
  const event = (value: unknown) => value as ProviderAdapter.ProviderAdapterV2Event;
  const item = (threadId: ThreadId, runId: RunId | null) =>
    event({
      type: "turn_item.updated",
      driver,
      turnItem: { id: TurnItemId.make("turn-item:drop"), threadId, runId, type: "dynamic_tool" },
    });
  const turn = (input: {
    readonly threadId?: ThreadId;
    readonly attemptId: RunAttemptId | null;
    readonly providerThreadId: ProviderThreadId;
  }) =>
    event({
      type: "provider_turn.updated",
      driver,
      ...(input.threadId === undefined ? {} : { threadId: input.threadId }),
      providerTurn: {
        id: ProviderTurnId.make("provider-turn:drop"),
        providerThreadId: input.providerThreadId,
        runAttemptId: input.attemptId,
        status: "running",
      },
    });
  const cases: ReadonlyArray<readonly [string, ProviderAdapter.ProviderAdapterV2Event, boolean]> = [
    ["another run's item", item(other.threadId, other.runId), true],
    [
      "another run's node",
      event({
        type: "node.updated",
        driver,
        node: { threadId: other.threadId, runId: other.runId },
      }),
      true,
    ],
    [
      "another run's message",
      event({
        type: "message.updated",
        driver,
        message: { threadId: other.threadId, runId: other.runId },
      }),
      true,
    ],
    [
      "another run's plan",
      event({
        type: "plan.updated",
        driver,
        plan: { threadId: other.threadId, runId: other.runId },
      }),
      true,
    ],
    ["another run's usage report", turn({ ...other, attemptId: other.attemptId }), true],
    // Routing can still own these, so they stay.
    ["another run's item on this run's thread", item(identity.threadId, other.runId), false],
    ["another run's item on an owned child", item(ownedChild, other.runId), false],
    ["a runless child item", item(other.threadId, null), false],
    [
      "a usage report without its thread",
      turn({ attemptId: other.attemptId, providerThreadId: other.providerThreadId }),
      false,
    ],
    [
      "a usage report on an owned provider thread",
      turn({
        threadId: other.threadId,
        attemptId: other.attemptId,
        providerThreadId: ownedChildProviderThread,
      }),
      false,
    ],
    [
      "a child turn without an attempt",
      turn({ threadId: other.threadId, attemptId: null, providerThreadId: other.providerThreadId }),
      false,
    ],
    // Controls always stay: rosters, steers, children and terminals.
    [
      "another run's roster",
      event({
        type: "provider_thread.updated",
        driver,
        providerThread: { id: other.providerThreadId, appThreadId: other.threadId },
      }),
      false,
    ],
    ["another run's steer", event({ type: "steer.updated", driver, runId: other.runId }), false],
    [
      "another run's subagent",
      event({
        type: "subagent.updated",
        driver,
        subagent: { threadId: other.threadId, runId: other.runId, childThreadId: null },
      }),
      false,
    ],
    [
      "another run's terminal",
      event({
        type: "turn.terminal",
        driver,
        providerThreadId: other.providerThreadId,
        providerTurnId: ProviderTurnId.make("provider-turn:drop"),
      }),
      false,
    ],
    ["this run's item", item(identity.threadId, identity.runId), false],
  ];
  for (const [name, candidate, dropped] of cases) {
    assert.strictEqual(
      RunExecutionService.isAnotherRunsProviderEvent(candidate, identity, state),
      dropped,
      name,
    );
    // A dropped event is one routing rejects.
    if (dropped)
      assert.isFalse(RunExecutionService.routeProviderEvent(candidate, identity, state)[0], name);
  }

  // Drops wait while an event that may give the run a thread or provider thread is queued.
  const node = (threadId: ThreadId, runId: RunId | null, providerThreadId: ProviderThreadId) =>
    event({ type: "node.updated", driver, node: { threadId, runId, providerThreadId } });
  const extending: ReadonlyArray<
    readonly [string, ProviderAdapter.ProviderAdapterV2Event, boolean]
  > = [
    ["a child's creation", event({ type: "app_thread.created", driver }), true],
    ["a subagent", event({ type: "subagent.updated", driver }), true],
    ["a roster", event({ type: "provider_thread.updated", driver }), true],
    [
      "an owned child's node on a new provider thread",
      node(ownedChild, null, ProviderThreadId.make("provider-thread:drop:new")),
      true,
    ],
    [
      "an owned child's node on its known provider thread",
      node(ownedChild, null, ownedChildProviderThread),
      false,
    ],
    [
      "this run's root node",
      node(identity.threadId, identity.runId, identity.providerThreadId),
      false,
    ],
    ["another run's node", node(other.threadId, other.runId, other.providerThreadId), false],
    ["an item", item(ownedChild, null), false],
  ];
  for (const [name, candidate, extendsOwnership] of extending)
    assert.strictEqual(
      RunExecutionService.mayExtendRunOwnership(candidate, identity, state),
      extendsOwnership,
      name,
    );
});

it("routes only enrolled workflow members after the root turn ends", () => {
  const threadId = ThreadId.make("thread:late-child");
  const rootProviderTurnId = ProviderTurnId.make("provider-turn:late-child");
  const identity: RunExecutionService.ProviderEventRouteIdentity = {
    threadId,
    runId: RunId.make("run:late-child"),
    attemptId: RunAttemptId.make("attempt:late-child"),
    providerThreadId: ProviderThreadId.make("provider-thread:late-child"),
  };
  const childCreated = (
    childThreadId: ThreadId,
    parentThreadId = threadId,
  ): ProviderAdapter.ProviderAdapterV2Event =>
    ({
      type: "app_thread.created",
      driver,
      appThread: {
        id: childThreadId,
        lineage: {
          parentThreadId,
          relationshipToParent: "subagent",
          rootThreadId: threadId,
        },
      },
    }) as ProviderAdapter.ProviderAdapterV2Event;
  const earlyChild = ThreadId.make("thread:late-child:early");
  const lateChild = ThreadId.make("thread:late-child:late");

  const initial = RunExecutionService.makeProviderEventRoutingState({
    identity,
    providerTurnId: rootProviderTurnId,
  });
  const [earlyAccepted, live] = RunExecutionService.routeProviderEvent(
    childCreated(earlyChild),
    identity,
    initial,
  );
  assert.isTrue(earlyAccepted);
  const existingDescendant = ThreadId.make("thread:late-child:existing-descendant");
  const [descendantAccepted, withDescendant] = RunExecutionService.routeProviderEvent(
    childCreated(existingDescendant, earlyChild),
    identity,
    live,
  );
  assert.isTrue(descendantAccepted);
  const [terminalAccepted, ended] = RunExecutionService.routeProviderEvent(
    {
      type: "turn.terminal",
      driver,
      providerThreadId: identity.providerThreadId,
      providerTurnId: rootProviderTurnId,
      runOrdinal: 1,
      status: "completed",
      failure: null,
      threadDisposition: "reusable",
    },
    identity,
    withDescendant,
  );
  assert.isTrue(terminalAccepted);
  assert.isTrue(ended.ownedThreadIds.has(earlyChild));
  const [lateAccepted, afterLate] = RunExecutionService.routeProviderEvent(
    childCreated(lateChild),
    identity,
    ended,
  );
  assert.isFalse(lateAccepted);
  assert.isFalse(afterLate.ownedThreadIds.has(lateChild));

  const memberId = ThreadId.make("thread:late-child:workflow-member");
  assert.isFalse(
    RunExecutionService.routeProviderEvent(childCreated(memberId, earlyChild), identity, ended)[0],
  );
  const coordinator: OrchestrationV2Subagent = {
    id: NodeId.make("node:late-child:workflow"),
    threadId,
    runId: identity.runId,
    parentNodeId: NodeId.make("node:late-child"),
    origin: "provider_native",
    createdBy: "agent",
    driver,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerThreadId: null,
    childThreadId: earlyChild,
    nativeTaskRef: null,
    prompt: "Review the code",
    title: "Review",
    model: null,
    status: "running",
    result: null,
    startedAt: null,
    completedAt: null,
    updatedAt: DateTime.makeUnsafe(0),
    workflow: {
      phases: [],
      agents: [{ index: 0, label: "Reviewer", state: "running", childThreadId: memberId }],
    },
  };
  const [foreignAccepted, afterForeign] = RunExecutionService.routeProviderEvent(
    {
      type: "subagent.updated",
      driver,
      subagent: {
        ...coordinator,
        threadId: earlyChild,
        childThreadId: existingDescendant,
        runId: RunId.make("run:late-child:new"),
      },
    },
    identity,
    ended,
  );
  assert.isTrue(foreignAccepted);
  assert.isFalse(
    RunExecutionService.routeProviderEvent(
      childCreated(memberId, existingDescendant),
      identity,
      afterForeign,
    )[0],
  );
  const [workflowAccepted, afterWorkflow] = RunExecutionService.routeProviderEvent(
    { type: "subagent.updated", driver, subagent: coordinator },
    identity,
    ended,
  );
  assert.isTrue(workflowAccepted);
  assert.isFalse(
    RunExecutionService.routeProviderEvent(childCreated(memberId), identity, afterWorkflow)[0],
  );
  const [memberAccepted, afterMember] = RunExecutionService.routeProviderEvent(
    childCreated(memberId, earlyChild),
    identity,
    afterWorkflow,
  );
  assert.isTrue(memberAccepted);
  assert.isTrue(afterMember.ownedThreadIds.has(memberId));
  const [unrelatedAccepted] = RunExecutionService.routeProviderEvent(
    childCreated(ThreadId.make("thread:late-child:unrelated-member"), lateChild),
    identity,
    ended,
  );
  assert.isFalse(unrelatedAccepted);
  const [unrelatedDescendantAccepted, afterUnrelatedDescendant] =
    RunExecutionService.routeProviderEvent(
      childCreated(ThreadId.make("thread:late-child:unrelated-descendant"), earlyChild),
      identity,
      afterWorkflow,
    );
  assert.isFalse(unrelatedDescendantAccepted);
  assert.isFalse(
    afterUnrelatedDescendant.ownedThreadIds.has(
      ThreadId.make("thread:late-child:unrelated-descendant"),
    ),
  );
});

it("moves a child thread to the run that shows its subagent now", () => {
  const threadId = ThreadId.make("thread:handoff");
  const rootProviderTurnId = ProviderTurnId.make("provider-turn:handoff");
  const identity: RunExecutionService.ProviderEventRouteIdentity = {
    threadId,
    runId: RunId.make("run:handoff:1"),
    attemptId: RunAttemptId.make("attempt:handoff:1"),
    providerThreadId: ProviderThreadId.make("provider-thread:handoff"),
  };
  const resumedChild = ThreadId.make("thread:handoff:resumed");
  const siblingChild = ThreadId.make("thread:handoff:sibling");
  const providerThreadOf = (childThreadId: ThreadId) =>
    ProviderThreadId.make(`provider-thread:${childThreadId}`);
  const subagent = (childThreadId: ThreadId, runId: RunId): OrchestrationV2Subagent => ({
    id: NodeId.make(`node:${childThreadId}`),
    threadId,
    runId,
    parentNodeId: NodeId.make("node:handoff"),
    origin: "provider_native",
    createdBy: "agent",
    driver,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerThreadId: providerThreadOf(childThreadId),
    childThreadId,
    nativeTaskRef: null,
    prompt: "Work",
    title: "Work",
    model: null,
    status: "running",
    result: null,
    startedAt: null,
    completedAt: null,
    updatedAt: DateTime.makeUnsafe(0),
  });
  const childMessage = (childThreadId: ThreadId) =>
    ({
      type: "message.updated",
      driver,
      message: {
        id: MessageId.make(`message:${childThreadId}`),
        threadId: childThreadId,
        runId: null,
      },
    }) as ProviderAdapter.ProviderAdapterV2Event;
  const childTurn = (childThreadId: ThreadId, ordinal: number) =>
    ({
      type: "provider_turn.updated",
      driver,
      providerTurn: {
        id: ProviderTurnId.make(`provider-turn:${childThreadId}:${ordinal}`),
        providerThreadId: providerThreadOf(childThreadId),
        runAttemptId: null,
        status: "running",
      },
    }) as ProviderAdapter.ProviderAdapterV2Event;
  const ended = RunExecutionService.routeProviderEvent(
    {
      type: "turn.terminal",
      driver,
      providerThreadId: identity.providerThreadId,
      providerTurnId: rootProviderTurnId,
      runOrdinal: 1,
      status: "completed",
      failure: null,
      threadDisposition: "reusable",
    },
    identity,
    RunExecutionService.makeProviderEventRoutingState({
      identity,
      providerTurnId: rootProviderTurnId,
      relatedThreadIds: [resumedChild, siblingChild],
      relatedProviderThreadIds: [providerThreadOf(resumedChild), providerThreadOf(siblingChild)],
    }),
  )[1];
  const [firstTurnAccepted, withChildTurn] = RunExecutionService.routeProviderEvent(
    childTurn(resumedChild, 1),
    identity,
    ended,
  );
  assert.isTrue(firstTurnAccepted);
  const [movedAccepted, afterMove] = RunExecutionService.routeProviderEvent(
    {
      type: "subagent.updated",
      driver,
      subagent: subagent(resumedChild, RunId.make("run:handoff:2")),
    },
    identity,
    withChildTurn,
  );
  assert.isFalse(movedAccepted);
  assert.isFalse(
    RunExecutionService.routeProviderEvent(childMessage(resumedChild), identity, afterMove)[0],
  );
  assert.isTrue(
    RunExecutionService.routeProviderEvent(childMessage(siblingChild), identity, afterMove)[0],
  );
  assert.isFalse(
    RunExecutionService.routeProviderEvent(childTurn(resumedChild, 1), identity, afterMove)[0],
  );
  assert.isFalse(
    RunExecutionService.routeProviderEvent(childTurn(resumedChild, 2), identity, afterMove)[0],
  );
  assert.isTrue(
    RunExecutionService.routeProviderEvent(childTurn(siblingChild, 1), identity, afterMove)[0],
  );
  assert.isTrue(
    RunExecutionService.routeProviderEvent(
      { type: "subagent.updated", driver, subagent: subagent(siblingChild, identity.runId) },
      identity,
      afterMove,
    )[0],
  );
  const resuming: RunExecutionService.ProviderEventRouteIdentity = {
    ...identity,
    runId: RunId.make("run:handoff:2"),
    attemptId: RunAttemptId.make("attempt:handoff:2"),
  };
  const resumingState = RunExecutionService.makeProviderEventRoutingState({
    identity: resuming,
    providerTurnId: ProviderTurnId.make("provider-turn:handoff:2"),
  });
  assert.isFalse(
    RunExecutionService.routeProviderEvent(childMessage(resumedChild), resuming, resumingState)[0],
  );
  const [moveAccepted, afterTake] = RunExecutionService.routeProviderEvent(
    { type: "subagent.updated", driver, subagent: subagent(resumedChild, resuming.runId) },
    resuming,
    resumingState,
  );
  assert.isTrue(moveAccepted);
  assert.isTrue(
    RunExecutionService.routeProviderEvent(childMessage(resumedChild), resuming, afterTake)[0],
  );
});

it("does not route a superseded attempt through a reused provider thread", () => {
  const threadId = ThreadId.make("thread:shared-runtime:restart");
  const providerThreadId = ProviderThreadId.make("provider-thread:shared-runtime:restart");
  const oldAttempt: RunExecutionService.ProviderEventRouteIdentity = {
    threadId,
    runId: RunId.make("run:shared-runtime:restart"),
    attemptId: RunAttemptId.make("attempt:shared-runtime:restart:old"),
    providerThreadId,
  };
  const newAttempt: RunExecutionService.ProviderEventRouteIdentity = {
    ...oldAttempt,
    attemptId: RunAttemptId.make("attempt:shared-runtime:restart:new"),
  };
  const oldTurnEvent: ProviderAdapter.ProviderAdapterV2Event = {
    type: "provider_turn.updated",
    driver,
    threadId,
    providerTurn: {
      id: ProviderTurnId.make("provider-turn:shared-runtime:restart:old"),
      providerThreadId,
      nodeId: NodeId.make("node:shared-runtime:restart:old"),
      runAttemptId: oldAttempt.attemptId,
      nativeTurnRef: null,
      ordinal: 1,
      status: "interrupted",
      startedAt: null,
      completedAt: null,
    },
  };

  const newState = RunExecutionService.makeProviderEventRoutingState({
    identity: newAttempt,
    providerTurnId: null,
  });
  assert.isFalse(RunExecutionService.routeProviderEvent(oldTurnEvent, newAttempt, newState)[0]);
});

it("routes only exact same-thread background items inherited from settled runs", () => {
  const threadId = ThreadId.make("thread:inherited-background-routing");
  const otherThreadId = ThreadId.make("thread:inherited-background-routing:other");
  const priorRunId = RunId.make("run:inherited-background-routing:prior");
  const currentRunId = RunId.make("run:inherited-background-routing:current");
  const itemId = TurnItemId.make("turn-item:inherited-background-routing");
  const identity: RunExecutionService.ProviderEventRouteIdentity = {
    threadId,
    runId: currentRunId,
    attemptId: RunAttemptId.make("attempt:inherited-background-routing:current"),
    providerThreadId: ProviderThreadId.make("provider-thread:inherited-background-routing:current"),
  };
  const initial = RunExecutionService.makeProviderEventRoutingState({
    identity,
    inheritedBackgroundTurnItems: [{ id: itemId, runId: priorRunId }],
    providerTurnId: null,
  });
  const inheritedRunning = {
    type: "turn_item.updated",
    driver,
    turnItem: {
      id: itemId,
      threadId,
      runId: priorRunId,
      providerTurnId: null,
      ordinal: 1,
      type: "subagent",
      status: "running",
    },
  } as Extract<ProviderAdapter.ProviderAdapterV2Event, { type: "turn_item.updated" }>;
  const unrelatedRunItem = {
    ...inheritedRunning,
    turnItem: {
      ...inheritedRunning.turnItem,
      runId: RunId.make("run:inherited-background-routing:unrelated"),
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
  const unlistedPriorItem = {
    ...inheritedRunning,
    turnItem: {
      ...inheritedRunning.turnItem,
      id: TurnItemId.make("turn-item:inherited-background-routing:unrelated"),
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
  const unrelatedThreadItem = {
    ...inheritedRunning,
    turnItem: { ...inheritedRunning.turnItem, threadId: otherThreadId },
  } as ProviderAdapter.ProviderAdapterV2Event;
  const ordinaryItem = {
    ...inheritedRunning,
    turnItem: { ...inheritedRunning.turnItem, type: "reasoning" as const },
  } as ProviderAdapter.ProviderAdapterV2Event;
  const inheritedTerminal = {
    ...inheritedRunning,
    turnItem: { ...inheritedRunning.turnItem, status: "completed" as const },
  } as ProviderAdapter.ProviderAdapterV2Event;

  const [runningAccepted, afterRunning] = RunExecutionService.routeProviderEvent(
    inheritedRunning,
    identity,
    initial,
  );
  assert.isTrue(runningAccepted);
  assert.isFalse(
    RunExecutionService.routeProviderEvent(unrelatedRunItem, identity, afterRunning)[0],
  );
  assert.isFalse(
    RunExecutionService.routeProviderEvent(unlistedPriorItem, identity, afterRunning)[0],
  );
  assert.isFalse(
    RunExecutionService.routeProviderEvent(unrelatedThreadItem, identity, afterRunning)[0],
  );
  assert.isFalse(RunExecutionService.routeProviderEvent(ordinaryItem, identity, afterRunning)[0]);

  const [terminalAccepted, afterTerminal] = RunExecutionService.routeProviderEvent(
    inheritedTerminal,
    identity,
    afterRunning,
  );
  assert.isTrue(terminalAccepted);
  assert.isFalse(
    RunExecutionService.routeProviderEvent(inheritedRunning, identity, afterTerminal)[0],
    "a nonterminal replay must not resurrect an inherited terminal",
  );
});

it("routes an earlier run's plan rows to this thread's active root turn only", () => {
  const threadId = ThreadId.make("thread:earlier-plan-routing");
  const priorRunId = RunId.make("run:earlier-plan-routing:prior");
  const rootTurnId = ProviderTurnId.make("provider-turn:earlier-plan-routing:current");
  const identity: RunExecutionService.ProviderEventRouteIdentity = {
    threadId,
    runId: RunId.make("run:earlier-plan-routing:current"),
    attemptId: RunAttemptId.make("attempt:earlier-plan-routing:current"),
    providerThreadId: ProviderThreadId.make("provider-thread:earlier-plan-routing"),
  };
  const active = RunExecutionService.makeProviderEventRoutingState({
    identity,
    providerTurnId: rootTurnId,
  });
  const planRow = {
    type: "turn_item.updated",
    driver,
    turnItem: {
      id: TurnItemId.make("turn-item:earlier-plan-routing"),
      threadId,
      runId: priorRunId,
      type: "todo_list",
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
  const plan = {
    type: "plan.updated",
    driver,
    plan: { threadId, runId: priorRunId },
  } as ProviderAdapter.ProviderAdapterV2Event;
  const otherThreadPlan = {
    type: "plan.updated",
    driver,
    plan: { threadId: ThreadId.make("thread:earlier-plan-routing:other"), runId: priorRunId },
  } as ProviderAdapter.ProviderAdapterV2Event;
  const ordinaryRow = {
    type: "turn_item.updated",
    driver,
    turnItem: {
      id: TurnItemId.make("turn-item:earlier-plan-routing:tool"),
      threadId,
      runId: priorRunId,
      type: "reasoning",
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
  const [, ended] = RunExecutionService.routeProviderEvent(
    {
      type: "turn.terminal",
      driver,
      providerThreadId: identity.providerThreadId,
      providerTurnId: rootTurnId,
      runOrdinal: 2,
      status: "completed",
      failure: null,
      threadDisposition: "reusable",
    },
    identity,
    active,
  );

  assert.isTrue(RunExecutionService.routeProviderEvent(planRow, identity, active)[0]);
  assert.isTrue(RunExecutionService.routeProviderEvent(plan, identity, active)[0]);
  assert.isFalse(RunExecutionService.routeProviderEvent(otherThreadPlan, identity, active)[0]);
  assert.isFalse(RunExecutionService.routeProviderEvent(ordinaryRow, identity, active)[0]);
  assert.isFalse(RunExecutionService.routeProviderEvent(planRow, identity, ended)[0]);
  assert.isFalse(RunExecutionService.routeProviderEvent(plan, identity, ended)[0]);
});

it("selects only live background items from non-completed settled prior runs", () => {
  const threadId = ThreadId.make("thread:inherited-background-selection");
  const otherThreadId = ThreadId.make("thread:inherited-background-selection:other");
  const currentProviderThreadId = ProviderThreadId.make(
    "provider-thread:inherited-background-selection:current",
  );
  const foreignProviderThreadId = ProviderThreadId.make(
    "provider-thread:inherited-background-selection:foreign-session",
  );
  const interruptedRunId = RunId.make("run:inherited-background-selection:interrupted");
  const failedRunId = RunId.make("run:inherited-background-selection:failed");
  const cancelledRunId = RunId.make("run:inherited-background-selection:cancelled");
  const completedRunId = RunId.make("run:inherited-background-selection:completed");
  const rolledBackRunId = RunId.make("run:inherited-background-selection:rolled-back");
  const currentRunId = RunId.make("run:inherited-background-selection:current");
  const inheritedItemId = TurnItemId.make("turn-item:inherited-background-selection:live");
  const failedItemId = TurnItemId.make("turn-item:inherited-background-selection:failed");
  const cancelledItemId = TurnItemId.make("turn-item:inherited-background-selection:cancelled");
  const makeRun = (
    id: RunId,
    ordinal: number,
    status: OrchestrationV2Run["status"],
    runThreadId = threadId,
  ) =>
    ({
      id,
      threadId: runThreadId,
      ordinal,
      status,
    }) as OrchestrationV2Run;
  const makeItem = (
    id: TurnItemId,
    runId: RunId,
    status: OrchestrationV2TurnItem["status"],
    type: OrchestrationV2TurnItem["type"] = "subagent",
    itemThreadId = threadId,
    providerThreadId = currentProviderThreadId,
  ) =>
    ({
      id,
      threadId: itemThreadId,
      runId,
      providerThreadId,
      type,
      status,
    }) as OrchestrationV2TurnItem;

  const selected = RunExecutionService.selectInheritedBackgroundTurnItems({
    threadId,
    currentProviderThreadId,
    currentRunOrdinal: 6,
    runs: [
      makeRun(interruptedRunId, 1, "interrupted"),
      makeRun(failedRunId, 2, "failed"),
      makeRun(cancelledRunId, 3, "cancelled"),
      makeRun(completedRunId, 4, "completed"),
      makeRun(rolledBackRunId, 5, "rolled_back"),
      makeRun(currentRunId, 6, "running"),
      makeRun(
        RunId.make("run:inherited-background-selection:other"),
        1,
        "interrupted",
        otherThreadId,
      ),
    ],
    turnItems: [
      makeItem(inheritedItemId, interruptedRunId, "running"),
      makeItem(failedItemId, failedRunId, "running"),
      makeItem(cancelledItemId, cancelledRunId, "running"),
      makeItem(
        TurnItemId.make("turn-item:inherited-background-selection:completed-run"),
        completedRunId,
        "running",
      ),
      makeItem(
        TurnItemId.make("turn-item:inherited-background-selection:terminal"),
        interruptedRunId,
        "completed",
      ),
      makeItem(
        TurnItemId.make("turn-item:inherited-background-selection:ordinary"),
        interruptedRunId,
        "running",
        "reasoning",
      ),
      makeItem(
        TurnItemId.make("turn-item:inherited-background-selection:rolled-back"),
        rolledBackRunId,
        "running",
      ),
      makeItem(
        TurnItemId.make("turn-item:inherited-background-selection:other-thread"),
        interruptedRunId,
        "running",
        "subagent",
        otherThreadId,
      ),
      makeItem(
        TurnItemId.make("turn-item:inherited-background-selection:foreign-provider"),
        interruptedRunId,
        "running",
        "subagent",
        threadId,
        foreignProviderThreadId,
      ),
    ],
  });

  assert.deepEqual(selected, [
    { id: inheritedItemId, runId: interruptedRunId },
    { id: failedItemId, runId: failedRunId },
    { id: cancelledItemId, runId: cancelledRunId },
  ]);
});

it("does not carry interrupted or still-running child ownership into later attempts", () => {
  assert.isFalse(RunExecutionService.canRouteRelatedSubagent("interrupted"));
  assert.isFalse(RunExecutionService.canRouteRelatedSubagent("failed"));
  assert.isFalse(RunExecutionService.canRouteRelatedSubagent("cancelled"));
  assert.isTrue(RunExecutionService.canRouteRelatedSubagent("completed"));
  assert.isFalse(RunExecutionService.canRouteRelatedSubagent("running"));

  const threadId = ThreadId.make("thread:related-child:next-attempt");
  const childThreadId = ThreadId.make("thread:related-child:interrupted");
  const identity: RunExecutionService.ProviderEventRouteIdentity = {
    threadId,
    runId: RunId.make("run:related-child:next-attempt"),
    attemptId: RunAttemptId.make("attempt:related-child:next-attempt"),
    providerThreadId: ProviderThreadId.make("provider-thread:related-child:next-attempt"),
  };
  const state = RunExecutionService.makeProviderEventRoutingState({
    identity,
    providerTurnId: null,
    relatedThreadIds: RunExecutionService.canRouteRelatedSubagent("interrupted")
      ? [childThreadId]
      : [],
  });
  const childNodeId = NodeId.make("node:related-child:interrupted");
  const lateChildNode = {
    type: "node.updated",
    driver,
    node: {
      id: childNodeId,
      threadId: childThreadId,
      runId: null,
      parentNodeId: null,
      rootNodeId: childNodeId,
      kind: "root_turn",
      status: "completed",
      countsForRun: false,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: null,
      completedAt: null,
    },
  } satisfies ProviderAdapter.ProviderAdapterV2Event;

  assert.isFalse(RunExecutionService.routeProviderEvent(lateChildNode, identity, state)[0]);
});

it.effect("rechecks run ownership immediately before calling the provider", () =>
  Effect.gen(function* () {
    const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
    const guardCalls = yield* Ref.make(0);
    const providerStarts = yield* Ref.make(0);
    const threadId = ThreadId.make("thread:run-execution-start-guard");
    const runId = RunId.make("run:run-execution-start-guard");
    const attemptId = RunAttemptId.make("attempt:run-execution-start-guard");
    const providerThreadId = ProviderThreadId.make("provider-thread:run-execution-start-guard");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const providerSessionId = ProviderSessionId.make("session:run-execution-start-guard");
    const rootNodeId = NodeId.make("node:run-execution-start-guard");
    const run = {
      id: runId,
      threadId,
      ordinal: 1,
      providerInstanceId,
    } as OrchestrationV2Run;
    const rootNode = { id: rootNodeId } as OrchestrationV2ExecutionNode;
    const providerThread = {
      id: providerThreadId,
      driver,
    } as OrchestrationV2ProviderThread;
    const attempt = {
      id: attemptId,
      providerTurnId: null,
    } as OrchestrationV2RunAttempt;
    const session = {
      events: Stream.never,
      startTurn: () => Ref.update(providerStarts, (count) => count + 1),
    } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;

    yield* runExecution.startRootRun({
      commandId: CommandId.make("command:run-execution-start-guard"),
      appThread: rootAppThread(threadId),
      providerSessionId,
      session,
      run,
      rootNode,
      checkpointScope: {
        id: CheckpointScopeId.make("checkpoint-scope:run-execution-start-guard"),
      } as OrchestrationV2CheckpointScope,
      providerThread,
      attempt,
      attemptId,
      providerTurnOrdinal: 1,
      shouldStartProviderTurn: () =>
        Ref.modify(guardCalls, (calls) => [calls === 0, calls + 1] as const),
      message: {
        messageId: MessageId.make("message:run-execution-start-guard"),
        text: "Do not start after ownership changes.",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
      },
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
      runtimePolicy: {
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: process.cwd(),
        approvalPolicy: "never",
        sandboxPolicy: {
          type: "readOnly",
          access: { type: "fullAccess" },
          networkAccess: false,
        },
      },
    });

    assert.equal(yield* Ref.get(guardCalls), 2);
    assert.equal(yield* Ref.get(providerStarts), 0);
  }).pipe(Effect.provide(layerRunExecutionTest)),
);

it.effect("passes the thread's MCP app context to the provider under a safe key", () =>
  Effect.gen(function* () {
    const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
    const started = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2TurnInput>();
    const threadId = ThreadId.make("thread:run-execution-app-context");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const attemptId = RunAttemptId.make("attempt:run-execution-app-context");
    const session = {
      events: Stream.never,
      startTurn: (input: ProviderAdapter.ProviderAdapterV2TurnInput) =>
        Deferred.succeed(started, input),
    } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime;

    yield* runExecution.startRootRun({
      commandId: CommandId.make("command:run-execution-app-context"),
      appThread: rootAppThread(threadId),
      providerSessionId: ProviderSessionId.make("session:run-execution-app-context"),
      session,
      run: {
        id: RunId.make("run:run-execution-app-context"),
        threadId,
        ordinal: 1,
        providerInstanceId,
      } as OrchestrationV2Run,
      rootNode: {
        id: NodeId.make("node:run-execution-app-context"),
      } as OrchestrationV2ExecutionNode,
      checkpointScope: {
        id: CheckpointScopeId.make("checkpoint-scope:run-execution-app-context"),
      } as OrchestrationV2CheckpointScope,
      providerThread: {
        id: ProviderThreadId.make("provider-thread:run-execution-app-context"),
        driver,
      } as OrchestrationV2ProviderThread,
      attempt: { id: attemptId, providerTurnId: null } as OrchestrationV2RunAttempt,
      attemptId,
      providerTurnOrdinal: 1,
      message: {
        messageId: MessageId.make("message:run-execution-app-context"),
        text: "What is on my list?",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
      },
      modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
      runtimePolicy: { runtimeMode: "full-access", interactionMode: "default", cwd: process.cwd() },
    });

    const input = yield* Deferred.await(started);
    assert.deepEqual(input.appContext, [
      { key: "mcp_app_turn-item_provider_codex_native-item_call-1", text: "2 overdue" },
    ]);
  }).pipe(
    Effect.provide(
      RunExecutionService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            PreviewAgentActivity.layer,
            Layer.mock(McpAppModelContext.McpAppModelContext)({
              forThread: () =>
                Effect.succeed([
                  {
                    itemId: "turn-item:provider:codex:native-item:call-1",
                    server: "my tools>",
                    tool: "list",
                    text: "2 overdue",
                  },
                ]),
            }),
            Layer.mock(CheckpointService.CheckpointServiceV2)({
              captureBaseline: () => Effect.void,
            }),
            layerTurnItemPositionsTest,
            mockEventSink({}),
            IdAllocator.layer,
            ThreadCommandExecutor.layer,
            Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
              ingestNormalized: () => Effect.succeed([]),
            }),
            ServerSettings.layerTest(),
          ),
        ),
      ),
    ),
  ),
);

it.effect("fails the run when its ownership check cannot be read before calling the provider", () =>
  Effect.gen(function* () {
    const guardCalls = yield* Ref.make(0);
    const providerStarts = yield* Ref.make(0);
    const writes = yield* Ref.make<ReadonlyArray<OrchestrationV2DomainEvent>>([]);
    const threadId = ThreadId.make("thread:run-execution-start-guard-read");
    const runId = RunId.make("run:run-execution-start-guard-read");
    const attemptId = RunAttemptId.make("attempt:run-execution-start-guard-read");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const layerTest = RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          McpAppModelContext.layerEmpty,
          PreviewAgentActivity.layer,
          Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
          layerTurnItemPositionsTest,
          mockEventSink({
            writeIfRunCurrent: (input) =>
              Ref.update(writes, (current) => [...current, ...input.events]).pipe(
                Effect.as({ committed: true, storedEvents: [] }),
              ),
          }),
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
            ingestNormalized: () => Effect.succeed([]),
          }),
          ServerSettings.layerTest(),
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      yield* runExecution.startRootRun({
        commandId: CommandId.make("command:run-execution-start-guard-read"),
        appThread: rootAppThread(threadId),
        providerSessionId: ProviderSessionId.make("session:run-execution-start-guard-read"),
        session: {
          events: Stream.never,
          startTurn: () => Ref.update(providerStarts, (count) => count + 1),
        } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
        run: { id: runId, threadId, ordinal: 1, providerInstanceId } as OrchestrationV2Run,
        rootNode: {
          id: NodeId.make("node:run-execution-start-guard-read"),
        } as OrchestrationV2ExecutionNode,
        checkpointScope: {
          id: CheckpointScopeId.make("checkpoint-scope:run-execution-start-guard-read"),
        } as OrchestrationV2CheckpointScope,
        providerThread: {
          id: ProviderThreadId.make("provider-thread:run-execution-start-guard-read"),
          driver,
        } as OrchestrationV2ProviderThread,
        attempt: { id: attemptId, providerTurnId: null } as OrchestrationV2RunAttempt,
        attemptId,
        providerTurnOrdinal: 1,
        shouldStartProviderTurn: () =>
          Ref.getAndUpdate(guardCalls, (calls) => calls + 1).pipe(
            Effect.flatMap((calls) =>
              calls === 0
                ? Effect.succeed(true)
                : Effect.fail(
                    new ProjectionStore.ProjectionStoreReadError({
                      threadId,
                      cause: "database unavailable",
                    }),
                  ),
            ),
          ),
        shouldFinalizeRun: () =>
          Effect.fail(
            new ProjectionStore.ProjectionStoreReadError({
              threadId,
              cause: "database unavailable",
            }),
          ),
        message: {
          messageId: MessageId.make("message:run-execution-start-guard-read"),
          text: "Start while the store is down.",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
        modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: process.cwd(),
          approvalPolicy: "never",
          sandboxPolicy: {
            type: "readOnly",
            access: { type: "fullAccess" },
            networkAccess: false,
          },
        },
      });
    }).pipe(Effect.provide(layerTest));

    assert.equal(yield* Ref.get(guardCalls), 2);
    assert.equal(yield* Ref.get(providerStarts), 0);
    const runUpdate = (yield* Ref.get(writes)).find((event) => event.type === "run.updated");
    assert.equal(
      runUpdate?.type === "run.updated" ? runUpdate.payload.status : undefined,
      "failed",
    );
  }),
);

it.effect(
  "dispatches only attachment-free compact commands through the native compaction path",
  () =>
    Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      const calls: Array<string> = [];
      const attachment = ChatFileAttachment.make({
        type: "file",
        id: ChatAttachmentId.make("compact-file-12345678-1234-1234-1234-123456789abc"),
        name: "notes.txt",
        mimeType: "text/plain",
        sizeBytes: 1,
      });
      const cases = [
        { text: " /compact ", attachments: [], expected: "compact" },
        { text: "/compact", attachments: [attachment], expected: "prompt" },
        { text: "Continue the work", attachments: [], expected: "prompt" },
      ];
      for (const [index, testCase] of cases.entries()) {
        const threadId = ThreadId.make(`thread:compact-routing:${index}`);
        const attemptId = RunAttemptId.make(`attempt:compact-routing:${index}`);
        yield* runExecution.startRootRun({
          commandId: CommandId.make(`command:compact-routing:${index}`),
          appThread: rootAppThread(threadId),
          providerSessionId: ProviderSessionId.make(`session:compact-routing:${index}`),
          session: {
            events: Stream.never,
            startTurn: () =>
              Effect.sync(() => {
                calls.push("prompt");
              }),
            compactThread: () =>
              Effect.sync(() => {
                calls.push("compact");
              }),
          } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
          run: {
            id: RunId.make(`run:compact-routing:${index}`),
            threadId,
            ordinal: 1,
            providerInstanceId: ProviderInstanceId.make("codex"),
          } as OrchestrationV2Run,
          rootNode: {
            id: NodeId.make(`node:compact-routing:${index}`),
          } as OrchestrationV2ExecutionNode,
          checkpointScope: {
            id: CheckpointScopeId.make(`checkpoint:compact-routing:${index}`),
          } as OrchestrationV2CheckpointScope,
          providerThread: {
            id: ProviderThreadId.make(`provider-thread:compact-routing:${index}`),
            driver,
          } as OrchestrationV2ProviderThread,
          attempt: { id: attemptId, providerTurnId: null } as OrchestrationV2RunAttempt,
          attemptId,
          providerTurnOrdinal: 1,
          message: {
            messageId: MessageId.make(`message:compact-routing:${index}`),
            text: testCase.text,
            attachments: testCase.attachments,
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: process.cwd(),
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "readOnly",
              access: { type: "fullAccess" },
              networkAccess: false,
            },
          },
        });
      }
      assert.deepEqual(
        calls,
        cases.map((testCase) => testCase.expected),
      );
    }).pipe(Effect.provide(layerRunExecutionTest)),
);

it.effect("starts the provider when checkpoint baseline capture fails", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:run-execution-baseline-failure");
    const runId = RunId.make("run:run-execution-baseline-failure");
    const attemptId = RunAttemptId.make("attempt:run-execution-baseline-failure");
    const providerThreadId = ProviderThreadId.make(
      "provider-thread:run-execution-baseline-failure",
    );
    const providerInstanceId = ProviderInstanceId.make("codex");
    const providerSessionId = ProviderSessionId.make("session:run-execution-baseline-failure");
    const rootNodeId = NodeId.make("node:run-execution-baseline-failure");
    const checkpointScope = {
      id: CheckpointScopeId.make("checkpoint-scope:run-execution-baseline-failure"),
    } as OrchestrationV2CheckpointScope;
    const providerStarts = yield* Ref.make(0);
    const writes = yield* Ref.make<
      ReadonlyArray<{
        events: ReadonlyArray<OrchestrationV2DomainEvent>;
        effects: ReadonlyArray<PendingOrchestrationEffectV2>;
      }>
    >([]);
    const layerTest = RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          McpAppModelContext.layerEmpty,
          PreviewAgentActivity.layer,
          Layer.mock(CheckpointService.CheckpointServiceV2)({
            captureBaseline: () =>
              Effect.fail(
                new CheckpointService.CheckpointBaselineCaptureError({
                  scopeId: checkpointScope.id,
                  ordinalWithinScope: 0,
                  cause: new Error("VCS process timed out"),
                }),
              ),
          }),
          layerTurnItemPositionsTest,
          mockEventSink({
            writeWithEffects: (input) =>
              Ref.update(writes, (current) => [
                ...current,
                { events: input.events, effects: input.effects },
              ]).pipe(Effect.as([])),
          }),
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
            ingestNormalized: () => Effect.succeed([]),
          }),
          ServerSettings.layerTest(),
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      yield* runExecution.startRootRun({
        commandId: CommandId.make("command:run-execution-baseline-failure"),
        appThread: rootAppThread(threadId),
        providerSessionId,
        session: {
          events: Stream.never,
          startTurn: () => Ref.update(providerStarts, (count) => count + 1),
        } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
        run: {
          id: runId,
          threadId,
          ordinal: 1,
          providerInstanceId,
        } as OrchestrationV2Run,
        rootNode: { id: rootNodeId } as OrchestrationV2ExecutionNode,
        checkpointScope,
        providerThread: {
          id: providerThreadId,
          driver,
        } as OrchestrationV2ProviderThread,
        attempt: {
          id: attemptId,
          providerTurnId: null,
        } as OrchestrationV2RunAttempt,
        attemptId,
        providerTurnOrdinal: 1,
        message: {
          messageId: MessageId.make("message:run-execution-baseline-failure"),
          text: "Start the provider without a baseline.",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
        modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: process.cwd(),
          approvalPolicy: "never",
          sandboxPolicy: {
            type: "readOnly",
            access: { type: "fullAccess" },
            networkAccess: false,
          },
        },
      });
    }).pipe(Effect.provide(layerTest));

    assert.equal(yield* Ref.get(providerStarts), 1);

    const events = (yield* Ref.get(writes)).flatMap((write) => [...write.events]);
    const failedEvent = events.find(
      (event) =>
        (event.type === "run.updated" ||
          event.type === "run-attempt.updated" ||
          event.type === "node.updated") &&
        event.payload.status === "failed",
    );
    assert.isUndefined(failedEvent);
    const errorItem = events.find(
      (event) => event.type === "turn-item.updated" && event.payload.type === "error",
    );
    assert.isUndefined(errorItem);
  }),
);

it.effect("prompts the provider only after a forked baseline capture finishes", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:run-execution-forked-baseline");
    const runId = RunId.make("run:run-execution-forked-baseline");
    const attemptId = RunAttemptId.make("attempt:run-execution-forked-baseline");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const checkpointScope = {
      id: CheckpointScopeId.make("checkpoint-scope:run-execution-forked-baseline"),
    } as OrchestrationV2CheckpointScope;
    const run = { id: runId, threadId, ordinal: 1, providerInstanceId } as OrchestrationV2Run;
    const order = yield* Ref.make<ReadonlyArray<string>>([]);
    const layerTest = RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          McpAppModelContext.layerEmpty,
          PreviewAgentActivity.layer,
          Layer.mock(CheckpointService.CheckpointServiceV2)({
            captureBaseline: () =>
              Effect.sleep("1 second").pipe(
                Effect.andThen(Ref.update(order, (current) => [...current, "baseline"])),
              ),
          }),
          layerTurnItemPositionsTest,
          mockEventSink({}),
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
            ingestNormalized: () => Effect.succeed([]),
          }),
          ServerSettings.layerTest(),
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      const baseline = yield* runExecution.forkBaseline({
        appThread: rootAppThread(threadId),
        run,
        checkpointScope,
      });
      const started = yield* runExecution
        .startRootRun({
          commandId: CommandId.make("command:run-execution-forked-baseline"),
          appThread: rootAppThread(threadId),
          providerSessionId: ProviderSessionId.make("session:run-execution-forked-baseline"),
          session: {
            events: Stream.never,
            startTurn: () => Ref.update(order, (current) => [...current, "prompt"]),
          } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
          run,
          rootNode: {
            id: NodeId.make("node:run-execution-forked-baseline"),
          } as OrchestrationV2ExecutionNode,
          checkpointScope,
          providerThread: {
            id: ProviderThreadId.make("provider-thread:run-execution-forked-baseline"),
            driver,
          } as OrchestrationV2ProviderThread,
          attempt: { id: attemptId, providerTurnId: null } as OrchestrationV2RunAttempt,
          attemptId,
          baseline,
          providerTurnOrdinal: 1,
          message: {
            messageId: MessageId.make("message:run-execution-forked-baseline"),
            text: "Wait for the baseline.",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: process.cwd(),
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "readOnly",
              access: { type: "fullAccess" },
              networkAccess: false,
            },
          },
        })
        .pipe(Effect.forkChild);
      yield* TestClock.adjust("1 second");
      yield* Fiber.join(started);
    }).pipe(Effect.provide(layerTest));

    assert.deepEqual(yield* Ref.get(order), ["baseline", "prompt"]);
  }),
);

// A run's baseline is taken where its checkpoint will be: its scope's real path, fixed when the run
// was scoped there, not a spelling (a symlink) that may point elsewhere by its turn start.
it.effect.each([
  { name: "a scope with its real path", realCwd: "/repo-real", expected: "/repo-real" },
  { name: "a scope created before it kept one", realCwd: undefined, expected: "/repo-link" },
])("takes a run's baseline at its workspace's real path: $name", ({ realCwd, expected }) =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:run-execution-baseline-workspace");
    const checkpointScope = {
      id: CheckpointScopeId.make("checkpoint-scope:run-execution-baseline-workspace"),
      cwd: "/repo-first-spelling",
      ...(realCwd === undefined ? {} : { realCwd }),
    } as OrchestrationV2CheckpointScope;
    const run = {
      id: RunId.make("run:run-execution-baseline-workspace"),
      threadId,
      ordinal: 2,
      providerInstanceId: ProviderInstanceId.make("codex"),
      workspaceCwd: "/repo-link",
    } as OrchestrationV2Run;
    const workspaces = yield* Ref.make<ReadonlyArray<string>>([]);
    const layerTest = RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          McpAppModelContext.layerEmpty,
          PreviewAgentActivity.layer,
          Layer.mock(CheckpointService.CheckpointServiceV2)({
            captureBaseline: ({ scope }) =>
              Ref.update(workspaces, (current) => [...current, scope.cwd]),
          }),
          layerTurnItemPositionsTest,
          mockEventSink({}),
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
            ingestNormalized: () => Effect.succeed([]),
          }),
          ServerSettings.layerTest(),
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      yield* yield* runExecution.forkBaseline({
        appThread: rootAppThread(threadId),
        run,
        checkpointScope,
      });
    }).pipe(Effect.provide(layerTest));
    assert.deepEqual(yield* Ref.get(workspaces), [expected]);
  }),
);

it.effect.each(["failure", "interruption", "stale-attempt", "start-guard"] as const)(
  "handles %s before the provider turn starts",
  (scenario) =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread:run-execution-settings-failure");
      const runId = RunId.make("run:run-execution-settings-failure");
      const attemptId = RunAttemptId.make("attempt:run-execution-settings-failure");
      const providerInstanceId = ProviderInstanceId.make("codex");
      const providerSessionId = ProviderSessionId.make("session:run-execution-settings-failure");
      const providerThreadId = ProviderThreadId.make(
        "provider-thread:run-execution-settings-failure",
      );
      const rootNodeId = NodeId.make("node:run-execution-settings-failure");
      const checkpointScope = {
        id: CheckpointScopeId.make("checkpoint-scope:run-execution-settings-failure"),
      } as OrchestrationV2CheckpointScope;
      const providerStarts = yield* Ref.make(0);
      const refreshes = yield* Ref.make(0);
      const guardedWrites = yield* Ref.make(0);
      const writes = yield* Ref.make<ReadonlyArray<ReadonlyArray<OrchestrationV2DomainEvent>>>([]);
      const layerTest = RunExecutionService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            McpAppModelContext.layerEmpty,
            PreviewAgentActivity.layer,
            Layer.mock(CheckpointService.CheckpointServiceV2)({
              captureBaseline: () =>
                scenario === "start-guard" ? Effect.void : Effect.die("not reached"),
            }),
            layerTurnItemPositionsTest,
            mockEventSink({
              writeIfRunCurrent: (input) =>
                Effect.gen(function* () {
                  assert.equal(input.threadId, threadId);
                  assert.equal(input.runId, runId);
                  assert.equal(input.activeAttemptId, attemptId);
                  assert.equal(input.expectedStatus, "running");
                  yield* Ref.update(guardedWrites, (count) => count + 1);
                  if (scenario === "stale-attempt") {
                    return { committed: false, storedEvents: [] };
                  }
                  yield* Ref.update(writes, (current) => [...current, input.events]);
                  return { committed: true, storedEvents: [] };
                }),
            }),
            IdAllocator.layer,
            ThreadCommandExecutor.layer,
            Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
              ingestNormalized: () => Effect.succeed([]),
            }),
            scenario === "start-guard"
              ? ServerSettings.layerTest()
              : Layer.mock(ServerSettings.ServerSettingsService)({
                  getSettings:
                    scenario === "interruption"
                      ? Effect.interrupt
                      : Effect.fail(
                          new ServerSettingsError({
                            settingsPath: "<test>",
                            operation: "read-file",
                            cause: new Error("settings read failed"),
                          }),
                        ),
                }),
            Layer.succeed(RunFinalizationService.RunFinalizationObserver, {
              refresh: () => Effect.void,
              refreshRepositories: () => Effect.void,
              refreshAfterTurn: () => Ref.update(refreshes, (count) => count + 1),
            }),
          ),
        ),
      );

      const result = yield* Effect.gen(function* () {
        const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
        yield* runExecution.startRootRun({
          commandId: CommandId.make("command:run-execution-settings-failure"),
          appThread: rootAppThread(threadId),
          providerSessionId,
          session: {
            events: Stream.never,
            startTurn: () => Ref.update(providerStarts, (count) => count + 1),
          } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
          run: {
            id: runId,
            threadId,
            ordinal: 1,
            providerInstanceId,
            status: "running",
          } as OrchestrationV2Run,
          rootNode: { id: rootNodeId, status: "running" } as OrchestrationV2ExecutionNode,
          checkpointScope,
          providerThread: {
            id: providerThreadId,
            driver,
          } as OrchestrationV2ProviderThread,
          attempt: {
            id: attemptId,
            providerTurnId: null,
            status: "running",
          } as OrchestrationV2RunAttempt,
          attemptId,
          providerTurnOrdinal: 1,
          ...(scenario === "start-guard"
            ? { shouldStartProviderTurn: () => Effect.succeed(false) }
            : {}),
          message: {
            messageId: MessageId.make("message:run-execution-settings-failure"),
            text: "Start after settings fail.",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: process.cwd(),
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "readOnly",
              access: { type: "fullAccess" },
              networkAccess: false,
            },
          },
        });
      }).pipe(Effect.provide(layerTest), Effect.exit);

      assert.equal(yield* Ref.get(providerStarts), 0);
      const events = (yield* Ref.get(writes)).flat();
      if (scenario === "interruption") {
        assert.isTrue(Exit.isFailure(result));
        if (Exit.isFailure(result)) assert.isTrue(Cause.hasInterruptsOnly(result.cause));
        assert.equal(yield* Ref.get(guardedWrites), 0);
        assert.equal(yield* Ref.get(refreshes), 0);
        assert.isEmpty(events);
        return;
      }
      assert.isTrue(Exit.isSuccess(result));
      if (scenario === "start-guard") {
        assert.equal(yield* Ref.get(guardedWrites), 0);
        assert.equal(yield* Ref.get(refreshes), 0);
        assert.isEmpty(events);
        return;
      }
      assert.equal(yield* Ref.get(guardedWrites), 1);
      if (scenario === "stale-attempt") {
        assert.equal(yield* Ref.get(refreshes), 0);
        assert.isEmpty(events);
        return;
      }
      assert.equal(yield* Ref.get(refreshes), 1);
      assert.deepEqual(
        events
          .filter(
            (event) =>
              event.type === "run.updated" ||
              event.type === "run-attempt.updated" ||
              event.type === "node.updated",
          )
          .map((event) => event.payload.status),
        ["failed", "failed", "failed"],
      );
      const errorItem = events.find(
        (event) => event.type === "turn-item.updated" && event.payload.type === "error",
      );
      assert.isDefined(errorItem);
      if (errorItem?.type === "turn-item.updated" && errorItem.payload.type === "error") {
        assert.equal(errorItem.payload.failure.message, "Run preparation failed.");
        // The provider never had the prompt, which decides whether a wake delivered its results.
        assert.equal(errorItem.payload.failure.code, "run_preparation_failed");
      }
    }),
);

it.effect("ingests new member threads of an owned child after the root turn terminalizes", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:run-execution-late-child");
    const childThreadId = ThreadId.make("thread:run-execution-late-child:child");
    const memberThreadId = ThreadId.make("thread:run-execution-late-child:member");
    const runId = RunId.make("run:run-execution-late-child");
    const attemptId = RunAttemptId.make("attempt:run-execution-late-child");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const providerSessionId = ProviderSessionId.make("session:run-execution-late-child");
    const providerThreadId = ProviderThreadId.make("provider-thread:run-execution-late-child");
    const childProviderThreadId = ProviderThreadId.make(
      "provider-thread:run-execution-late-child:child",
    );
    const rootProviderTurnId = ProviderTurnId.make("provider-turn:run-execution-late-child");
    const childProviderTurnId = ProviderTurnId.make("provider-turn:run-execution-late-child:child");
    const rootNodeId = NodeId.make("node:run-execution-late-child");
    const childNodeId = NodeId.make("node:run-execution-late-child:child");
    const subagentNodeId = NodeId.make("node:run-execution-late-child:subagent");
    const childMessageIngested = yield* Deferred.make<void>();
    const order = yield* Ref.make<ReadonlyArray<string>>([]);
    const origins = yield* Ref.make<ReadonlyArray<readonly [string, unknown, unknown]>>([]);
    const requests = yield* Ref.make<ReadonlyArray<string>>([]);
    const layerTest = RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(ProviderUsageAnalytics.ProviderTurnAnalytics, {
            runStarted: (input) =>
              Ref.update(requests, (current) => [...current, `started:${input.runId}`]),
            turnCompleted: () => Effect.void,
            runSettled: (input) =>
              Ref.update(requests, (current) => [
                ...current,
                `settled:${input.runId}:${input.status}:children-running=${input.childrenRunning}`,
              ]),
          }),
          McpAppModelContext.layerEmpty,
          PreviewAgentActivity.layer,
          Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
          layerTurnItemPositionsTest,
          mockEventSink({
            write: () => Effect.succeed([]),
            writeWithEffects: (input) =>
              Effect.gen(function* () {
                if (
                  input.events.some(
                    (event) => event.type === "run.updated" && event.runId === runId,
                  )
                ) {
                  yield* Ref.update(order, (current) => [...current, "root-finalized"]);
                }
                return [];
              }),
            writeIfRunCurrent: () => Effect.succeed({ committed: true, storedEvents: [] }),
          }),
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
            ingestNormalized: (input) =>
              Effect.gen(function* () {
                const eventType = input.event.type;
                yield* Ref.update(origins, (current) => [
                  ...current,
                  [
                    eventType,
                    input.analyticsContext?.origin,
                    input.analyticsContext?.segment,
                  ] as const,
                ]);
                if (
                  input.event.type === "app_thread.created" &&
                  input.event.appThread.id === memberThreadId
                ) {
                  yield* Ref.update(order, (current) => [...current, "member-thread"]);
                }
                if (
                  input.event.type === "message.updated" &&
                  input.event.message.threadId === memberThreadId
                ) {
                  yield* Ref.update(order, (current) => [...current, "child-message"]);
                  yield* Deferred.succeed(childMessageIngested, undefined).pipe(Effect.ignore);
                }
                return [];
              }),
          }),
          ServerSettings.layerTest(),
        ),
      ),
    );
    const events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event> = [
      {
        type: "app_thread.created",
        driver,
        appThread: {
          id: childThreadId,
          lineage: {
            parentThreadId: threadId,
            relationshipToParent: "subagent",
            rootThreadId: threadId,
          },
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
      {
        type: "provider_thread.updated",
        driver,
        providerThread: {
          id: childProviderThreadId,
          appThreadId: childThreadId,
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
      {
        type: "subagent.updated",
        driver,
        subagent: {
          ...makeRunOwnedSubagentFixture({
            ids: backgroundScenarioIds("run-execution-late-child"),
            providerInstanceId,
            childThreadId,
            driver,
            status: "running",
          }),
          workflow: {
            phases: [],
            agents: [
              { index: 0, label: "Reviewer", state: "running", childThreadId: memberThreadId },
            ],
          },
        },
      },
      {
        type: "provider_turn.updated",
        driver,
        threadId: childThreadId,
        providerTurn: {
          id: childProviderTurnId,
          providerThreadId: childProviderThreadId,
          nodeId: childNodeId,
          runAttemptId: null,
          status: "running",
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
      {
        type: "turn.terminal",
        driver,
        providerThreadId,
        providerTurnId: rootProviderTurnId,
        runOrdinal: 1,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      },
      {
        type: "app_thread.created",
        driver,
        appThread: {
          id: memberThreadId,
          lineage: {
            parentThreadId: childThreadId,
            relationshipToParent: "subagent",
            rootThreadId: threadId,
          },
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
      {
        type: "message.updated",
        driver,
        message: {
          id: MessageId.make("message:run-execution-late-child:child"),
          threadId: memberThreadId,
          runId: null,
          nodeId: childNodeId,
          role: "assistant",
          text: "Hello.",
          streaming: false,
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
      {
        type: "provider_turn.updated",
        driver,
        threadId: childThreadId,
        providerTurn: {
          id: childProviderTurnId,
          providerThreadId: childProviderThreadId,
          nodeId: childNodeId,
          runAttemptId: null,
          status: "completed",
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
      {
        type: "subagent.updated",
        driver,
        subagent: {
          id: subagentNodeId,
          threadId,
          runId,
          status: "completed",
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
    ];

    yield* Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      yield* runExecution.startRootRun({
        commandId: CommandId.make("command:run-execution-late-child"),
        appThread: rootAppThread(threadId),
        providerSessionId,
        session: {
          events: Stream.fromIterable(events),
          startTurn: () => Effect.void,
        } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
        run: {
          id: runId,
          threadId,
          ordinal: 1,
          providerInstanceId,
        } as OrchestrationV2Run,
        rootNode: { id: rootNodeId } as OrchestrationV2ExecutionNode,
        checkpointScope: {
          id: CheckpointScopeId.make("checkpoint-scope:run-execution-late-child"),
        } as OrchestrationV2CheckpointScope,
        providerThread: {
          id: providerThreadId,
          driver,
        } as OrchestrationV2ProviderThread,
        attempt: {
          id: attemptId,
          providerTurnId: rootProviderTurnId,
          reason: "initial",
        } as OrchestrationV2RunAttempt,
        attemptId,
        providerTurnOrdinal: 1,
        message: {
          messageId: MessageId.make("message:run-execution-late-child:user"),
          text: "Spawn a child and finish.",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
        modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: process.cwd(),
          approvalPolicy: "never",
          sandboxPolicy: {
            type: "readOnly",
            access: { type: "fullAccess" },
            networkAccess: false,
          },
        },
      });
    }).pipe(Effect.provide(layerTest));

    const observed = yield* Deferred.await(childMessageIngested).pipe(
      Effect.timeoutOption("2 seconds"),
    );
    assert.isTrue(Option.isSome(observed), "child message was not ingested after root terminal");
    assert.deepEqual(yield* Ref.get(order), ["root-finalized", "member-thread", "child-message"]);
    // The run's message started the root; the parent agent started the child's turn.
    const recorded = yield* Ref.get(origins);
    assert.deepEqual(recorded[0]?.slice(0, 2), [
      "app_thread.created",
      { createdBy: "user", creationSource: "web", scheduled: false, subagent: false },
    ]);
    assert.deepEqual(
      recorded.find(([type]) => type === "provider_turn.updated"),
      [
        "provider_turn.updated",
        { createdBy: "agent", creationSource: "provider", scheduled: false, subagent: true },
        "native_child",
      ],
    );
    // The child outlives the root, so the request summary says its later turns are missing.
    assert.deepEqual(yield* Ref.get(requests), [
      `started:${runId}`,
      `settled:${runId}:completed:children-running=true`,
    ]);
  }),
);

it.effect("ends a nested native subagent and its thread when the root run is interrupted", () =>
  Effect.gen(function* () {
    const key = "run-execution-nested-native";
    const threadId = ThreadId.make(`thread:${key}`);
    const childThreadId = ThreadId.make(`thread:${key}:child`);
    const grandchildThreadId = ThreadId.make(`thread:${key}:grandchild`);
    const runId = RunId.make(`run:${key}`);
    const attemptId = RunAttemptId.make(`attempt:${key}`);
    const providerInstanceId = ProviderInstanceId.make("codex");
    const providerThreadId = ProviderThreadId.make(`provider-thread:${key}`);
    const rootProviderTurnId = ProviderTurnId.make(`provider-turn:${key}`);
    const nestedId = NodeId.make(`node:${key}:nested`);
    const grandchildRootId = NodeId.make(`node:${key}:grandchild-root`);
    const finalized = yield* Deferred.make<ReadonlyArray<OrchestrationV2DomainEvent>>();
    const layerTest = RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          McpAppModelContext.layerEmpty,
          PreviewAgentActivity.layer,
          Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
          layerTurnItemPositionsTest,
          mockEventSink({
            write: () => Effect.succeed([]),
            writeWithEffects: (input) =>
              Effect.as(
                input.events.some((event) => event.type === "run.updated")
                  ? Deferred.succeed(finalized, input.events)
                  : Effect.void,
                [],
              ),
          }),
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
            ingestNormalized: () => Effect.succeed([]),
          }),
          ServerSettings.layerTest(),
        ),
      ),
    );
    const subagentThread = (id: ThreadId, parentThreadId: ThreadId) =>
      ({
        type: "app_thread.created",
        driver,
        appThread: {
          id,
          lineage: { parentThreadId, relationshipToParent: "subagent", rootThreadId: threadId },
        },
      }) as ProviderAdapter.ProviderAdapterV2Event;
    const events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event> = [
      subagentThread(childThreadId, threadId),
      subagentThread(grandchildThreadId, childThreadId),
      {
        type: "subagent.updated",
        driver,
        subagent: {
          id: nestedId,
          threadId: childThreadId,
          runId: null,
          origin: "provider_native",
          childThreadId: grandchildThreadId,
          status: "running",
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
      {
        type: "node.updated",
        driver,
        node: {
          id: grandchildRootId,
          threadId: grandchildThreadId,
          runId: null,
          kind: "root_turn",
          status: "running",
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
      {
        type: "turn.terminal",
        driver,
        providerThreadId,
        providerTurnId: rootProviderTurnId,
        runOrdinal: 1,
        status: "interrupted",
        failure: null,
        threadDisposition: "reusable",
      },
    ];

    yield* Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      yield* runExecution.startRootRun({
        commandId: CommandId.make(`command:${key}`),
        appThread: rootAppThread(threadId),
        providerSessionId: ProviderSessionId.make(`session:${key}`),
        session: {
          events: Stream.fromIterable(events),
          startTurn: () => Effect.void,
        } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
        run: { id: runId, threadId, ordinal: 1, providerInstanceId } as OrchestrationV2Run,
        rootNode: { id: NodeId.make(`node:${key}`) } as OrchestrationV2ExecutionNode,
        checkpointScope: {
          id: CheckpointScopeId.make(`checkpoint-scope:${key}`),
        } as OrchestrationV2CheckpointScope,
        providerThread: { id: providerThreadId, driver } as OrchestrationV2ProviderThread,
        attempt: { id: attemptId, providerTurnId: rootProviderTurnId } as OrchestrationV2RunAttempt,
        attemptId,
        providerTurnOrdinal: 1,
        message: {
          messageId: MessageId.make(`message:${key}:user`),
          text: "Spawn a nested subagent.",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
        modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: process.cwd(),
          approvalPolicy: "never",
          sandboxPolicy: { type: "readOnly", access: { type: "fullAccess" }, networkAccess: false },
        },
      });
    }).pipe(Effect.provide(layerTest));

    const ended = (yield* Deferred.await(finalized)).flatMap((event) =>
      event.type === "subagent.updated" || event.type === "node.updated"
        ? [[event.type, event.payload.id, event.payload.status]]
        : [],
    );
    assert.deepInclude(ended, ["subagent.updated", nestedId, "interrupted"]);
    assert.deepInclude(ended, ["node.updated", grandchildRootId, "interrupted"]);
  }),
);

it.effect("drains queued child output after the root loses provider-thread ownership", () =>
  Effect.gen(function* () {
    const childEvents: Array<string> = [];
    yield* runBackgroundItemScenario(
      "queued-child-output",
      (ids) => {
        const childProviderThreadId = ProviderThreadId.make(
          "provider-thread:queued-child-output:child",
        );
        const snapshot = (child: boolean, pendingBackgroundWork: boolean) => ({
          type: "provider_thread.updated" as const,
          driver,
          providerThread: {
            id: child ? childProviderThreadId : ids.providerThreadId,
            driver,
            providerInstanceId: ProviderInstanceId.make("codex"),
            providerSessionId: null,
            appThreadId: child ? ids.childThreadId : ids.threadId,
            ownerNodeId: null,
            nativeThreadRef: null,
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            pendingBackgroundTasks: [],
            createdAt: DateTime.makeUnsafe(0),
            updatedAt: DateTime.makeUnsafe(0),
          } satisfies OrchestrationV2ProviderThread,
          pendingBackgroundWork,
        });
        const childTurn = (status: "running" | "completed") => ({
          type: "provider_turn.updated" as const,
          driver,
          threadId: ids.childThreadId,
          providerTurn: {
            id: ProviderTurnId.make("provider-turn:queued-child-output:child"),
            providerThreadId: childProviderThreadId,
            runAttemptId: null,
            ordinal: 2,
            status,
          } as OrchestrationV2ProviderTurn,
        });
        const message = (text: string) =>
          ({
            type: "message.updated",
            driver,
            message: { threadId: ids.childThreadId, runId: null, text },
          }) as ProviderAdapter.ProviderAdapterV2Event;
        return [
          childThreadCreatedEvent(ids),
          snapshot(false, true),
          snapshot(true, true),
          subagentEvent(ids, "running"),
          rootTerminalEvent(ids, "completed"),
          snapshot(false, true),
          subagentEvent(ids, "completed"),
          childTurn("running"),
          message("GRANDCHILD_OK"),
          childTurn("completed"),
          snapshot(true, false),
          snapshot(false, false),
          message("after-drain"),
        ];
      },
      {
        keepEventStreamOpen: true,
        hasPendingBackgroundWorkForThread: () => Effect.succeed(false),
        providerThreadOwnerLost: true,
        onIngest: (event) =>
          Effect.sync(() => {
            if (event.type === "provider_turn.updated") {
              childEvents.push(event.providerTurn.status);
            }
            if (event.type === "message.updated") childEvents.push(event.message.text);
          }),
      },
    );
    assert.deepEqual(childEvents, ["running", "GRANDCHILD_OK", "completed"]);
  }),
);

// A newer run claims the provider thread when it is queued and ingests only from its turn start.
it.effect.each([
  {
    claimant: "has not attached, until its work settles",
    unattached: true,
    ingested: [true, true, false],
  },
  { claimant: "has attached, no further", unattached: false, ingested: [true, true] },
] as const)(
  "after its terminal, a run follows root snapshots while the run that claimed its thread $claimant",
  ({ unattached, ingested }) =>
    Effect.gen(function* () {
      const snapshots: Array<boolean | undefined> = [];
      yield* runBackgroundItemScenario(
        `claimant-${unattached ? "unattached" : "attached"}`,
        (ids) => {
          const snapshot = (pendingBackgroundWork: boolean) => ({
            type: "provider_thread.updated" as const,
            driver,
            providerThread: {
              id: ids.providerThreadId,
              driver,
              providerInstanceId: ProviderInstanceId.make("codex"),
              providerSessionId: null,
              appThreadId: ids.threadId,
              ownerNodeId: null,
              nativeThreadRef: null,
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: 1,
              lastRunOrdinal: 1,
              handoffIds: [],
              forkedFrom: null,
              pendingBackgroundTasks: [],
              createdAt: DateTime.makeUnsafe(0),
              updatedAt: DateTime.makeUnsafe(0),
            } satisfies OrchestrationV2ProviderThread,
            pendingBackgroundWork,
          });
          return [
            snapshot(true),
            rootTerminalEvent(ids, "completed"),
            snapshot(true),
            snapshot(false),
            snapshot(true),
          ];
        },
        {
          keepEventStreamOpen: true,
          providerThreadOwnerLost: true,
          claimantUnattached: unattached,
          onIngest: (event) =>
            Effect.sync(() => {
              if (event.type === "provider_thread.updated") {
                snapshots.push(event.pendingBackgroundWork);
              }
            }),
        },
      );
      assert.deepEqual(snapshots, [...ingested]);
    }),
);

it.effect("reports the ingest lag of roster corrections written for an unattached claimant", () => {
  const { observer, reports } = recordingWaitObserver();
  return Effect.gen(function* () {
    yield* runBackgroundItemScenario(
      "claimant-roster-lag",
      (ids) => {
        const snapshot = (pendingBackgroundWork: boolean) => ({
          type: "provider_thread.updated" as const,
          driver,
          providerThread: {
            id: ids.providerThreadId,
            driver,
            providerInstanceId: ProviderInstanceId.make("codex"),
            providerSessionId: null,
            appThreadId: ids.threadId,
            ownerNodeId: null,
            nativeThreadRef: null,
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            pendingBackgroundTasks: [],
            createdAt: DateTime.makeUnsafe(0),
            updatedAt: DateTime.makeUnsafe(0),
          } satisfies OrchestrationV2ProviderThread,
          pendingBackgroundWork,
        });
        // After the terminal the run's own writes refuse; the roster correction writes instead.
        const corrections = [snapshot(true), snapshot(false)];
        for (const event of corrections) IngestLag.markArrival(event, 0n);
        return [snapshot(true), rootTerminalEvent(ids, "completed"), ...corrections];
      },
      {
        batched: true,
        keepEventStreamOpen: true,
        providerThreadOwnerLost: true,
        claimantUnattached: true,
        rosterCorrected: true,
      },
    );
    assert.deepEqual(
      reports("wait")
        .map(([site]) => String(site))
        .filter((site) => site.startsWith("ProviderIngest.lag.")),
      ["ProviderIngest.lag.lifecycle", "ProviderIngest.lag.lifecycle"],
    );
  }).pipe(Effect.provideService(CurrentWaitObserver, observer));
});

it.effect("keeps ingesting until queued background work clears after the probe goes idle", () =>
  Effect.gen(function* () {
    // The adapter's probe runs ahead of its event queue, so only the queued update may release.
    // A child transferred to another run takes its pending work with it.
    const pending = yield* Ref.make(true);
    const consumed = yield* Deferred.make<void>();
    const result = yield* Effect.raceFirst(
      runBackgroundItemScenario(
        "queued-background-work",
        (ids) => {
          const childProviderThreadId = ProviderThreadId.make(`${ids.providerThreadId}:child`);
          const childProviderTurnId = ProviderTurnId.make(`${ids.rootProviderTurnId}:child`);
          const running = subagentEvent(ids, "running");
          if (running.type !== "subagent.updated") throw new Error("Missing subagent");
          const linked = {
            ...running,
            subagent: {
              ...running.subagent,
              origin: "provider_native" as const,
              childThreadId: ids.childThreadId,
              providerThreadId: childProviderThreadId,
            },
          };
          const backgroundWork = (providerTurnId: ProviderTurnId, pending: boolean) =>
            ({
              type: "background_work.updated",
              driver,
              providerTurnId,
              pending,
            }) satisfies ProviderAdapter.ProviderAdapterV2Event;
          return [
            childThreadCreatedEvent(ids),
            linked,
            {
              type: "provider_turn.updated",
              driver,
              threadId: ids.childThreadId,
              providerTurn: {
                id: childProviderTurnId,
                providerThreadId: childProviderThreadId,
                runAttemptId: null,
                status: "completed",
              },
            } as ProviderAdapter.ProviderAdapterV2Event,
            backgroundWork(childProviderTurnId, true),
            backgroundWork(ids.rootProviderTurnId, true),
            rootTerminalEvent(ids, "completed"),
            { ...linked, subagent: { ...linked.subagent, runId: RunId.make("run:replacement") } },
            backgroundTurnItemEvent(ids, "dynamic_tool", "completed", 1),
            backgroundWork(ids.rootProviderTurnId, false),
          ];
        },
        {
          keepEventStreamOpen: true,
          hasPendingBackgroundWorkForThread: () => Ref.get(pending),
          onFinalized: () => Ref.set(pending, false),
          onEventsConsumed: Deferred.succeed(consumed, undefined).pipe(Effect.asVoid),
        },
      ).pipe(Effect.asSome),
      Deferred.await(consumed).pipe(Effect.as(Option.none())),
    );
    assert.deepEqual(
      result,
      Option.some(["subagent:running", "root-finalized", "turn_item:completed"]),
    );
  }),
);

it.effect("reports a written message's and its run's terminal's ingest lag", () => {
  const { observer, reports } = recordingWaitObserver();
  return Effect.gen(function* () {
    yield* runBackgroundItemScenario(
      "ingest-lag",
      (ids) => {
        const events = [
          streamedMessageEvent({
            id: "message:ingest-lag",
            threadId: ids.threadId,
            runId: ids.runId,
            role: "assistant",
            text: "Done.",
            streaming: false,
          }),
          rootTerminalEvent(ids, "completed"),
        ];
        // As the session manager stamps each event it takes from the adapter.
        for (const event of events) IngestLag.markArrival(event, 0n);
        return events;
      },
      {
        batched: true,
        // The ingestor marks what it writes; a message write stands in for it here.
        onIngest: (event) =>
          event.type === "message.updated" ? IngestLag.markPersisted : Effect.void,
      },
    );
    // A terminal writes its run's final events itself, not through the ingestor.
    assert.sameMembers(
      reports("wait")
        .map(([site]) => String(site))
        .filter((site) => site.startsWith("ProviderIngest.lag.")),
      ["ProviderIngest.lag.delta", "ProviderIngest.lag.lifecycle"],
    );
  }).pipe(Effect.provideService(CurrentWaitObserver, observer));
});

it.effect("keeps ingesting while the last stored root snapshot reports pending work", () =>
  Effect.gen(function* () {
    // The live probe is already idle, as when it reads adapter state ahead of the queued events.
    const consumed = yield* Deferred.make<void>();
    const result = yield* Effect.raceFirst(
      runBackgroundItemScenario(
        "root-snapshot-pending-work",
        (ids) => {
          const snapshot = (pendingBackgroundWork: boolean) =>
            ({
              type: "provider_thread.updated",
              driver,
              providerThread: { id: ids.providerThreadId, appThreadId: ids.threadId },
              pendingBackgroundWork,
            }) as ProviderAdapter.ProviderAdapterV2Event;
          return [
            snapshot(true),
            rootTerminalEvent(ids, "completed"),
            backgroundTurnItemEvent(ids, "dynamic_tool", "completed", 1),
            snapshot(false),
          ];
        },
        {
          keepEventStreamOpen: true,
          hasPendingBackgroundWorkForThread: () => Effect.succeed(false),
          onEventsConsumed: Deferred.succeed(consumed, undefined).pipe(Effect.asVoid),
        },
      ).pipe(Effect.asSome),
      Deferred.await(consumed).pipe(Effect.as(Option.none())),
    );
    assert.deepEqual(result, Option.some(["root-finalized", "turn_item:completed"]));
  }),
);

it.effect("keeps ingesting a child thread's late background item completion", () =>
  Effect.gen(function* () {
    const observed = yield* runBackgroundItemScenario("bg-child-item", (ids) => [
      childThreadCreatedEvent(ids),
      subagentEvent(ids, "running"),
      childBackgroundTurnItemEvent(ids, "running", 1),
      rootTerminalEvent(ids, "completed"),
      subagentEvent(ids, "completed"),
      childBackgroundTurnItemEvent(ids, "completed", 2),
    ]);
    assert.deepEqual(observed, [
      "subagent:running",
      "turn_item:running",
      "root-finalized",
      "subagent:completed",
      "turn_item:completed",
    ]);
  }),
);

it.effect.each([null, "run:nested-sdk"] as const)(
  "releases a settled run when its live child transfers to another run (%s)",
  (nestedRunId) =>
    Effect.gen(function* () {
      const consumed = yield* Deferred.make<void>();
      const result = yield* Effect.raceFirst(
        runBackgroundItemScenario(
          "child-transfer-release",
          (ids) => {
            const running = subagentEvent(ids, "running");
            if (running.type !== "subagent.updated") throw new Error("Missing subagent");
            const linked = {
              ...running,
              subagent: {
                ...running.subagent,
                origin: "provider_native" as const,
                childThreadId: ids.childThreadId,
                providerThreadId: null,
              },
            };
            return [
              childThreadCreatedEvent(ids),
              linked,
              {
                ...linked,
                subagent: {
                  ...linked.subagent,
                  id: NodeId.make("node:child-transfer-release:nested"),
                  threadId: ids.childThreadId,
                  runId: nestedRunId === null ? null : RunId.make(nestedRunId),
                  childThreadId: null,
                },
              },
              childBackgroundTurnItemEvent(ids, "running", 1),
              rootTerminalEvent(ids, "completed"),
              { ...linked, subagent: { ...linked.subagent, runId: RunId.make("run:replacement") } },
            ];
          },
          {
            keepEventStreamOpen: true,
            onEventsConsumed: Deferred.succeed(consumed, undefined).pipe(Effect.asVoid),
          },
        ).pipe(Effect.asSome),
        Deferred.await(consumed).pipe(Effect.as(Option.none())),
      );
      assert.isTrue(
        Option.isSome(result),
        "the transferred child must release its prior subscription",
      );
      if (Option.isSome(result))
        assert.deepEqual(result.value, [
          "subagent:running",
          "subagent:running",
          "turn_item:running",
          "root-finalized",
        ]);
    }),
);

it.effect.each(["none", "sibling", "grandchild"] as const)(
  "drops a transferred child's pending snapshot but keeps other owned pending work (%s)",
  (otherPending) =>
    Effect.gen(function* () {
      const consumed = yield* Deferred.make<void>();
      const result = yield* Effect.raceFirst(
        runBackgroundItemScenario(
          "child-transfer-pending",
          (ids) => {
            const childProviderThreadId = ProviderThreadId.make(
              "provider-thread:child-transfer-pending:child",
            );
            const otherThreadId = ThreadId.make("thread:child-transfer-pending:other");
            const snapshot = (
              providerThreadId: ProviderThreadId,
              appThreadId: ThreadId,
              pendingBackgroundWork: boolean,
            ) => ({
              type: "provider_thread.updated" as const,
              driver,
              providerThread: {
                id: providerThreadId,
                appThreadId,
                status: "idle",
              } as OrchestrationV2ProviderThread,
              pendingBackgroundWork,
            });
            const running = subagentEvent(ids, "running");
            if (running.type !== "subagent.updated") throw new Error("Missing subagent");
            const linked = {
              ...running,
              subagent: {
                ...running.subagent,
                origin: "provider_native" as const,
                childThreadId: ids.childThreadId,
                providerThreadId: childProviderThreadId,
              },
            };
            // A grandchild stays with this run when its parent moves; a sibling was never the child's.
            const other =
              otherPending === "none"
                ? []
                : [
                    {
                      ...childThreadCreatedEvent(ids),
                      appThread: {
                        id: otherThreadId,
                        lineage: {
                          parentThreadId:
                            otherPending === "grandchild" ? ids.childThreadId : ids.threadId,
                          relationshipToParent: "subagent",
                          rootThreadId: ids.threadId,
                        },
                      },
                    } as ProviderAdapter.ProviderAdapterV2Event,
                    snapshot(
                      ProviderThreadId.make("provider-thread:child-transfer-pending:other"),
                      otherThreadId,
                      true,
                    ),
                  ];
            return [
              childThreadCreatedEvent(ids),
              linked,
              snapshot(childProviderThreadId, ids.childThreadId, true),
              ...other,
              rootTerminalEvent(ids, "completed"),
              { ...linked, subagent: { ...linked.subagent, runId: RunId.make("run:replacement") } },
              snapshot(childProviderThreadId, ids.childThreadId, false),
            ];
          },
          {
            keepEventStreamOpen: true,
            hasPendingBackgroundWorkForThread: () => Effect.succeed(false),
            onEventsConsumed: Deferred.succeed(consumed, undefined).pipe(Effect.asVoid),
          },
        ).pipe(Effect.asSome),
        Deferred.await(consumed).pipe(Effect.as(Option.none())),
      );
      assert.strictEqual(
        Option.isSome(result),
        otherPending === "none",
        "only pending work this run still owns may keep the prior subscription open",
      );
    }),
);

// The handover event is not this run's, yet it is what settles the run's agent turns.
it.effect(
  "ends preview activity when a transferred child settles a run kept open by background work",
  () =>
    Effect.gen(function* () {
      const active = yield* Deferred.make<boolean>();
      yield* runBackgroundItemScenario(
        "child-transfer-preview",
        (ids) => {
          const running = subagentEvent(ids, "running");
          if (running.type !== "subagent.updated") throw new Error("Missing subagent");
          const linked = {
            ...running,
            subagent: {
              ...running.subagent,
              origin: "provider_native" as const,
              childThreadId: ids.childThreadId,
              providerThreadId: null,
            },
          };
          return [
            childThreadCreatedEvent(ids),
            linked,
            backgroundTurnItemEvent(ids, "command_execution", "running", 1),
            rootTerminalEvent(ids, "completed"),
            { ...linked, subagent: { ...linked.subagent, runId: RunId.make("run:replacement") } },
          ];
        },
        {
          keepEventStreamOpen: true,
          afterEvents: (activity) =>
            activity
              .active(backgroundScenarioIds("child-transfer-preview").threadId)
              .pipe(Effect.flatMap((value) => Deferred.succeed(active, value))),
        },
      ).pipe(Effect.forkScoped);
      assert.isFalse(yield* Deferred.await(active));
    }),
);

it.effect("keeps ingesting a failed root's background item when its work continues", () =>
  Effect.gen(function* () {
    const observed = yield* runBackgroundItemScenario("bg-failed-continues", (ids) => [
      backgroundTurnItemEvent(ids, "command_execution", "running", 1),
      { ...rootTerminalEvent(ids, "failed"), childTurnsContinue: true },
      backgroundTurnItemEvent(ids, "command_execution", "completed", 2),
    ]);
    assert.deepEqual(observed, ["turn_item:running", "root-finalized", "turn_item:completed"]);
  }),
);

it.effect.each(["interrupted", "failed"] as const)(
  "does not pin ingestion on background items when the root turn is %s",
  (status) =>
    Effect.gen(function* () {
      const observed = yield* runBackgroundItemScenario(`bg-${status}`, (ids) => [
        backgroundTurnItemEvent(ids, "command_execution", "running", 1),
        rootTerminalEvent(ids, status),
        backgroundTurnItemEvent(ids, "command_execution", "completed", 2),
      ]);
      assert.deepEqual(observed, ["turn_item:running", "root-finalized"]);
    }),
);

it.effect("seeds inherited background items before their next update", () =>
  Effect.gen(function* () {
    const key = "inherited-background-seeded";
    const priorRunId = RunId.make(`run:${key}:prior`);
    const observed = yield* runBackgroundItemScenario(
      key,
      (ids) => [
        rootTerminalEvent(ids, "completed"),
        backgroundTurnItemEventForRun(ids, priorRunId, "subagent", "completed", 1),
      ],
      {
        loadInheritedBackgroundTurnItems: () =>
          Effect.succeed([{ id: TurnItemId.make(`turn-item:${key}`), runId: priorRunId }]),
      },
    );

    assert.deepEqual(observed, ["root-finalized", "turn_item:completed"]);
  }),
);

it.effect("refreshes inherited background items after event subscription", () =>
  Effect.gen(function* () {
    const key = "inherited-background-subscription-refresh";
    const ids = backgroundScenarioIds(key);
    const priorRunId = RunId.make(`run:${key}:prior`);
    const itemStatus = yield* Ref.make<OrchestrationV2TurnItem["status"]>("running");
    const loadInheritedBackgroundTurnItems = () =>
      Ref.get(itemStatus).pipe(
        Effect.map((status) =>
          RunExecutionService.selectInheritedBackgroundTurnItems({
            threadId: ids.threadId,
            currentProviderThreadId: ids.providerThreadId,
            currentRunOrdinal: 2,
            runs: [
              {
                id: priorRunId,
                threadId: ids.threadId,
                ordinal: 1,
                status: "interrupted",
              } as OrchestrationV2Run,
            ],
            turnItems: [
              {
                id: ids.itemId,
                threadId: ids.threadId,
                runId: priorRunId,
                providerThreadId: ids.providerThreadId,
                type: "subagent",
                status,
              } as OrchestrationV2TurnItem,
            ],
          }),
        ),
      );

    const observed = yield* runBackgroundItemScenario(
      key,
      (scenarioIds) => [rootTerminalEvent(scenarioIds, "completed")],
      {
        keepEventStreamOpen: true,
        loadInheritedBackgroundTurnItems,
        onSubscribe: Ref.set(itemStatus, "completed"),
      },
    );

    assert.deepEqual(yield* loadInheritedBackgroundTurnItems(), []);
    assert.deepEqual(observed, ["root-finalized"]);
  }),
);

it.effect(
  "cascade-terminalizes run-owned subagent rows on interrupt before root finalization",
  () =>
    Effect.gen(function* () {
      const ids = backgroundScenarioIds("subagent-interrupt-cascade");
      const childThreadId = ids.childThreadId;
      const unrelatedChildThreadId = ThreadId.make(
        "thread:subagent-interrupt-cascade:unrelated-child",
      );
      const providerInstanceId = ProviderInstanceId.make("codex");
      const written = yield* Ref.make<ReadonlyArray<OrchestrationV2DomainEvent>>([]);
      const ingested = yield* Ref.make<ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>>([]);
      const ingestionDone = yield* Deferred.make<void>();
      const layerTest = RunExecutionService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            McpAppModelContext.layerEmpty,
            PreviewAgentActivity.layer,
            Layer.mock(CheckpointService.CheckpointServiceV2)({
              captureBaseline: () => Effect.void,
            }),
            layerTurnItemPositionsTest,
            mockEventSink({
              write: () => Effect.succeed([]),
              writeWithEffects: (input) =>
                Effect.gen(function* () {
                  yield* Ref.update(written, (current) => [...current, ...input.events]);
                  return [];
                }),
              writeIfRunCurrent: () => Effect.succeed({ committed: true, storedEvents: [] }),
            }),
            IdAllocator.layer,
            ThreadCommandExecutor.layer,
            Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
              ingestNormalized: (input) =>
                Ref.update(ingested, (current) => [...current, input.event]).pipe(Effect.as([])),
            }),
            ServerSettings.layerTest(),
          ),
        ),
      );

      const runningSubagent = makeRunOwnedSubagentFixture({
        ids,
        providerInstanceId,
        childThreadId,
        driver,
        status: "running",
      });
      const runningTurnItem = makeRunOwnedSubagentTurnItemFixture({
        ids,
        providerInstanceId,
        childThreadId,
        driver,
        status: "running",
      });
      const runningNode = makeRunOwnedSubagentNodeFixture({
        ids,
        status: "running",
      });
      const runningChildNode = makeRunOwnedSubagentChildNodeFixture({
        ids,
        status: "running",
      });
      const memberThreadId = ThreadId.make("thread:subagent-interrupt-cascade:member");
      const runningMember = {
        ...runningSubagent,
        id: NodeId.make("node:subagent-interrupt-cascade:member"),
        threadId: childThreadId,
        runId: null,
        parentNodeId: runningChildNode.id,
        childThreadId: memberThreadId,
      };
      const foreignSubagent = {
        ...runningMember,
        id: NodeId.make("node:subagent-interrupt-cascade:foreign-subagent"),
        runId: RunId.make("run:subagent-interrupt-cascade:foreign"),
        childThreadId: unrelatedChildThreadId,
      };
      const runningMemberRoot = {
        ...runningChildNode,
        id: NodeId.make("node:subagent-interrupt-cascade:member-root"),
        threadId: memberThreadId,
        rootNodeId: NodeId.make("node:subagent-interrupt-cascade:member-root"),
      };
      const foreignChildNode = {
        ...runningChildNode,
        id: NodeId.make("node:subagent-interrupt-cascade:foreign-child"),
        runId: foreignSubagent.runId,
      };
      const runningChildTurnItem = makeLinkedChildTurnItemFixture({
        ids,
        driver,
        type: "command_execution",
      });
      const foreignChildTurnItem = {
        ...runningChildTurnItem,
        id: TurnItemId.make("turn-item:subagent-interrupt-cascade:foreign-child"),
        runId: foreignSubagent.runId,
      };
      const suppressedChildAssistantTurnItem = makeLinkedChildTurnItemFixture({
        ids: {
          ...ids,
          childItemId: TurnItemId.make("turn-item:subagent-interrupt-cascade:suppressed-assistant"),
        },
        driver,
        type: "assistant_message",
      });
      const unrelatedChildNode = {
        ...runningChildNode,
        id: NodeId.make("node:subagent-interrupt-cascade:unrelated-child"),
        threadId: unrelatedChildThreadId,
        runId: ids.runId,
        rootNodeId: NodeId.make("node:subagent-interrupt-cascade:unrelated-child"),
      };
      const unrelatedChildTurnItem = {
        ...runningChildTurnItem,
        id: TurnItemId.make("turn-item:subagent-interrupt-cascade:unrelated-child"),
        threadId: unrelatedChildThreadId,
        nodeId: unrelatedChildNode.id,
      };

      yield* Effect.gen(function* () {
        const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
        yield* runExecution.startRootRun({
          commandId: CommandId.make("command:subagent-interrupt-cascade"),
          appThread: rootAppThread(ids.threadId),
          providerSessionId: ProviderSessionId.make("session:subagent-interrupt-cascade"),
          session: {
            events: Stream.empty,
            subscribeEvents: Effect.succeed({
              events: Stream.fromIterable([
                childThreadCreatedEvent(ids),
                {
                  type: "subagent.updated",
                  driver,
                  subagent: runningSubagent,
                },
                {
                  type: "node.updated",
                  driver,
                  node: runningNode,
                },
                {
                  type: "node.updated",
                  driver,
                  node: runningChildNode,
                },
                {
                  type: "node.updated",
                  driver,
                  node: unrelatedChildNode,
                },
                {
                  type: "turn_item.updated",
                  driver,
                  turnItem: runningTurnItem,
                },
                {
                  type: "turn_item.updated",
                  driver,
                  turnItem: runningChildTurnItem,
                },
                {
                  type: "turn_item.updated",
                  driver,
                  turnItem: suppressedChildAssistantTurnItem,
                },
                {
                  type: "turn_item.updated",
                  driver,
                  turnItem: unrelatedChildTurnItem,
                },
                { type: "subagent.updated", driver, subagent: runningMember },
                { type: "subagent.updated", driver, subagent: foreignSubagent },
                { type: "node.updated", driver, node: runningMemberRoot },
                { type: "node.updated", driver, node: foreignChildNode },
                { type: "turn_item.updated", driver, turnItem: foreignChildTurnItem },
                rootTerminalEvent(ids, "interrupted"),
                {
                  type: "subagent.updated",
                  driver,
                  subagent: {
                    ...runningSubagent,
                    status: "completed" as const,
                    result: "should-not-apply",
                    completedAt: runningSubagent.updatedAt,
                  },
                },
                {
                  type: "turn_item.updated",
                  driver,
                  turnItem: {
                    ...runningTurnItem,
                    status: "completed" as const,
                    result: "should-not-apply",
                    completedAt: runningTurnItem.updatedAt,
                    updatedAt: runningTurnItem.updatedAt,
                  },
                },
                {
                  type: "turn_item.updated",
                  driver,
                  turnItem: {
                    ...runningChildTurnItem,
                    status: "completed" as const,
                    completedAt: runningChildTurnItem.updatedAt,
                    updatedAt: runningChildTurnItem.updatedAt,
                  },
                },
                {
                  type: "node.updated",
                  driver,
                  node: {
                    ...runningChildNode,
                    status: "completed" as const,
                    completedAt: runningChildNode.startedAt,
                  },
                },
              ] satisfies ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>),
              close: Deferred.succeed(ingestionDone, undefined),
            }),
            startTurn: () => Effect.void,
          } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
          run: {
            id: ids.runId,
            threadId: ids.threadId,
            ordinal: 1,
            providerInstanceId,
          } as OrchestrationV2Run,
          rootNode: { id: ids.rootNodeId } as OrchestrationV2ExecutionNode,
          checkpointScope: {
            id: CheckpointScopeId.make("checkpoint-scope:subagent-interrupt-cascade"),
          } as OrchestrationV2CheckpointScope,
          providerThread: {
            id: ids.providerThreadId,
            driver,
          } as OrchestrationV2ProviderThread,
          attempt: {
            id: ids.attemptId,
            providerTurnId: ids.rootProviderTurnId,
          } as OrchestrationV2RunAttempt,
          attemptId: ids.attemptId,
          relatedThreadIds: [unrelatedChildThreadId, memberThreadId],
          providerTurnOrdinal: 1,
          message: {
            messageId: MessageId.make("message:subagent-interrupt-cascade:user"),
            text: "Spawn a subagent then stop.",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
          runtimePolicy: {
            runtimeMode: "full-access",
            interactionMode: "default",
            cwd: process.cwd(),
            approvalPolicy: "never",
            sandboxPolicy: {
              type: "readOnly",
              access: { type: "fullAccess" },
              networkAccess: false,
            },
          },
        });
      }).pipe(Effect.provide(layerTest));

      const closed = yield* Deferred.await(ingestionDone).pipe(Effect.timeoutOption("2 seconds"));
      assert.isTrue(Option.isSome(closed), "event ingestion fiber did not finish");

      const events = yield* Ref.get(written);
      const subagentEvents = events.flatMap((event) =>
        event.type === "subagent.updated" ? [event] : [],
      );
      const turnItemEvents = events.flatMap((event) => {
        if (event.type !== "turn-item.updated" || event.payload.type !== "subagent") {
          return [];
        }
        return [
          {
            ...event,
            payload: event.payload,
          },
        ];
      });
      const nodeEvents = events.flatMap((event) =>
        event.type === "node.updated" &&
        event.payload.status === "interrupted" &&
        (event.payload.id === runningNode.id || event.payload.id === runningChildNode.id)
          ? [event]
          : [],
      );
      const runUpdatedIndex = events.findIndex((event) => event.type === "run.updated");
      assert.isAtLeast(runUpdatedIndex, 0, "root run.updated must be written");

      assert.lengthOf(subagentEvents, 2);
      const terminalSubagent = subagentEvents.find(
        (event) => event.payload.id === runningSubagent.id,
      );
      const terminalMember = subagentEvents.find((event) => event.payload.id === runningMember.id);
      assert.isDefined(terminalMember);
      assert.equal(terminalMember.payload.status, "interrupted");
      assert.equal(terminalMember.payload.runId, null);
      assert.equal(terminalMember.payload.childThreadId, memberThreadId);
      assert.isTrue(
        events.some(
          (event) =>
            event.type === "node.updated" &&
            event.payload.id === runningMemberRoot.id &&
            event.payload.status === "interrupted",
        ),
      );
      assert.isFalse(
        events.some(
          (event) => event.type === "subagent.updated" && event.payload.id === foreignSubagent.id,
        ),
      );
      assert.isFalse(
        events.some(
          (event) => event.type === "node.updated" && event.payload.id === foreignChildNode.id,
        ),
      );
      assert.isFalse(
        events.some(
          (event) =>
            event.type === "turn-item.updated" && event.payload.id === foreignChildTurnItem.id,
        ),
      );
      assert.isDefined(terminalSubagent);
      assert.equal(terminalSubagent.payload.status, "interrupted");
      assert.equal(terminalSubagent.payload.childThreadId, childThreadId);
      assert.equal(terminalSubagent.payload.result, null);
      assert.isNotNull(terminalSubagent.payload.completedAt);

      assert.lengthOf(turnItemEvents, 1);
      const terminalTurnItem = turnItemEvents[0];
      assert.isDefined(terminalTurnItem);
      assert.equal(terminalTurnItem.payload.status, "interrupted");
      assert.equal(terminalTurnItem.payload.childThreadId, childThreadId);
      assert.equal(terminalTurnItem.payload.result, null);

      const terminalChildTurnItems = events.flatMap((event) =>
        event.type === "turn-item.updated" &&
        event.payload.id === runningChildTurnItem.id &&
        event.payload.status === "interrupted"
          ? [event]
          : [],
      );
      assert.lengthOf(terminalChildTurnItems, 1);
      const terminalChildTurnItem = terminalChildTurnItems[0];
      assert.isDefined(terminalChildTurnItem);
      assert.equal(terminalChildTurnItem.threadId, childThreadId);
      assert.equal(terminalChildTurnItem.payload.threadId, childThreadId);
      assert.equal(terminalChildTurnItem.payload.runId, null);
      assert.equal(terminalChildTurnItem.payload.type, "command_execution");
      assert.isFalse(
        events.some(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.id === unrelatedChildTurnItem.id &&
            event.payload.status === "interrupted",
        ),
        "related but unlinked child turn items must not cascade",
      );
      assert.isFalse(
        events.some(
          (event) =>
            event.type === "turn-item.updated" &&
            event.payload.id === suppressedChildAssistantTurnItem.id,
        ),
        "suppressed streaming child items must not be created by the cascade",
      );

      assert.lengthOf(nodeEvents, 2);
      assert.isTrue(
        nodeEvents.some(
          (event) => event.payload.id === runningNode.id && event.payload.kind === "subagent",
        ),
      );
      assert.isFalse(
        events.some(
          (event) =>
            event.type === "node.updated" &&
            event.payload.id === unrelatedChildNode.id &&
            event.payload.status === "interrupted",
        ),
        "owned child threads without a live run-owned subagent link must not cascade",
      );
      assert.isTrue(
        nodeEvents.some(
          (event) =>
            event.payload.id === runningChildNode.id &&
            event.runId === (runningChildNode.runId ?? ids.runId) &&
            event.payload.threadId === childThreadId &&
            event.payload.kind === "root_turn",
        ),
      );

      const cascadeIndexes = events.flatMap((event, index) => {
        if (event.type === "subagent.updated" && event.payload.status === "interrupted") {
          return [index];
        }
        if (
          event.type === "turn-item.updated" &&
          event.payload.status === "interrupted" &&
          (event.payload.type === "subagent" || event.payload.id === runningChildTurnItem.id)
        ) {
          return [index];
        }
        if (event.type === "node.updated" && event.payload.status === "interrupted") {
          return event.payload.id === runningNode.id || event.payload.id === runningChildNode.id
            ? [index]
            : [];
        }
        return [];
      });
      assert.isTrue(
        cascadeIndexes.every((index) => index < runUpdatedIndex),
        "subagent cascade must precede run.updated",
      );
      assert.isFalse(
        events.some((event) => {
          if (event.type === "subagent.updated") {
            return event.payload.status === "completed";
          }
          if (event.type === "turn-item.updated" && event.payload.type === "subagent") {
            return event.payload.status === "completed";
          }
          return false;
        }),
        "late provider completion must not reopen cascaded subagent rows",
      );
      assert.isFalse(
        (yield* Ref.get(ingested)).some(
          (event) =>
            (event.type === "subagent.updated" && event.subagent.status === "completed") ||
            (event.type === "turn_item.updated" && event.turnItem.status === "completed") ||
            (event.type === "node.updated" && event.node.status === "completed"),
        ),
        "late provider completion must not be ingested after interrupt",
      );
    }),
);

it.effect("settles native subagents when a workspace handoff completes the root run", () =>
  Effect.gen(function* () {
    const ids = backgroundScenarioIds("subagent-workspace-handoff");
    const providerInstanceId = ProviderInstanceId.make("codex");
    const written = yield* Ref.make<ReadonlyArray<OrchestrationV2DomainEvent>>([]);
    const ingestionDone = yield* Deferred.make<void>();
    const layerTest = RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          McpAppModelContext.layerEmpty,
          PreviewAgentActivity.layer,
          Layer.mock(CheckpointService.CheckpointServiceV2)({
            captureBaseline: () => Effect.void,
          }),
          layerTurnItemPositionsTest,
          mockEventSink({
            write: () => Effect.succeed([]),
            writeWithEffects: (input) =>
              Ref.update(written, (current) => [...current, ...input.events]).pipe(Effect.as([])),
            writeIfRunCurrent: (input) =>
              Ref.update(written, (current) => [...current, ...input.events]).pipe(
                Effect.as({ committed: true, storedEvents: [] }),
              ),
          }),
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
            ingestNormalized: () => Effect.succeed([]),
          }),
          ServerSettings.layerTest(),
        ),
      ),
    );
    const fixture = {
      ids,
      providerInstanceId,
      childThreadId: ids.childThreadId,
      driver,
      status: "running",
    } as const;
    const runningSubagent = makeRunOwnedSubagentFixture(fixture);
    const runningTurnItem = makeRunOwnedSubagentTurnItemFixture(fixture);
    const runningNode = makeRunOwnedSubagentNodeFixture({ ids, status: "running" });
    const runningChildNode = makeRunOwnedSubagentChildNodeFixture({ ids, status: "running" });

    yield* Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      yield* runExecution.startRootRun({
        commandId: CommandId.make("command:subagent-workspace-handoff"),
        appThread: rootAppThread(ids.threadId),
        providerSessionId: ProviderSessionId.make("session:subagent-workspace-handoff"),
        session: {
          events: Stream.empty,
          subscribeEvents: Effect.succeed({
            // The provider event stream stays open: only settled children may end ingestion.
            events: Stream.concat(
              Stream.fromIterable([
                childThreadCreatedEvent(ids),
                { type: "subagent.updated", driver, subagent: runningSubagent },
                { type: "node.updated", driver, node: runningNode },
                { type: "node.updated", driver, node: runningChildNode },
                { type: "turn_item.updated", driver, turnItem: runningTurnItem },
                rootTerminalEvent(ids, "interrupted"),
              ] satisfies ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>),
              Stream.never,
            ),
            close: Deferred.succeed(ingestionDone, undefined),
          }),
          startTurn: () => Effect.void,
        } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
        run: {
          id: ids.runId,
          threadId: ids.threadId,
          ordinal: 1,
          providerInstanceId,
        } as OrchestrationV2Run,
        rootNode: { id: ids.rootNodeId } as OrchestrationV2ExecutionNode,
        checkpointScope: {
          id: CheckpointScopeId.make("checkpoint-scope:subagent-workspace-handoff"),
        } as OrchestrationV2CheckpointScope,
        providerThread: { id: ids.providerThreadId, driver } as OrchestrationV2ProviderThread,
        attempt: {
          id: ids.attemptId,
          providerTurnId: ids.rootProviderTurnId,
        } as OrchestrationV2RunAttempt,
        attemptId: ids.attemptId,
        providerTurnOrdinal: 1,
        shouldFinalizeRun: () => Effect.succeed(true),
        hasWorkspaceMoved: () => Effect.succeed(true),
        message: {
          messageId: MessageId.make("message:subagent-workspace-handoff:user"),
          text: "Spawn a subagent then move to a worktree.",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
        modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: process.cwd(),
          approvalPolicy: "never",
          sandboxPolicy: {
            type: "readOnly",
            access: { type: "fullAccess" },
            networkAccess: false,
          },
        },
      });
    }).pipe(Effect.provide(layerTest));

    const closed = yield* Deferred.await(ingestionDone).pipe(Effect.timeoutOption("2 seconds"));
    assert.isTrue(Option.isSome(closed), "settled children must end event ingestion");

    const events = yield* Ref.get(written);
    const statusOf = (type: OrchestrationV2DomainEvent["type"], id: string) =>
      events.flatMap((event) => {
        if (event.type !== type) return [];
        const payload = (
          event as { readonly payload: { readonly id: string; readonly status: string } }
        ).payload;
        return payload.id === id ? [payload.status] : [];
      });
    assert.deepEqual(statusOf("subagent.updated", runningSubagent.id), ["interrupted"]);
    assert.deepEqual(statusOf("turn-item.updated", runningTurnItem.id), ["interrupted"]);
    assert.deepEqual(statusOf("node.updated", runningNode.id), ["interrupted"]);
    assert.deepEqual(statusOf("node.updated", runningChildNode.id), ["interrupted"]);
    assert.deepEqual(statusOf("run.updated", ids.runId), ["waiting"]);
    assert.deepEqual(statusOf("run-attempt.updated", ids.attemptId), ["completed"]);
    assert.isFalse(
      events.some(
        (event) =>
          event.type === "turn-item.updated" && event.payload.type === "run_interrupt_result",
      ),
    );
  }),
);

it.effect("omits interrupt results and subagent cascade for a superseded attempt", () =>
  Effect.gen(function* () {
    const { written, observed } = yield* captureRootRunTermination({
      key: "steer-supersede",
      shouldFinalizeRun: () => Effect.succeed(false),
      seedOpenSubagent: true,
    });
    assert.deepEqual(
      written.map((item) => item.type),
      [],
    );
    assert.deepEqual(observed, []);
  }),
);

it.effect("emits run_interrupt_result when superseded attempt still has a hard-stop request", () =>
  Effect.gen(function* () {
    const { written, observed } = yield* captureRootRunTermination({
      key: "stop-then-steer-supersede",
      shouldFinalizeRun: () => Effect.succeed(false),
      hasUnpairedRunInterruptRequest: () => Effect.succeed(true),
    });
    assert.deepEqual(
      written.map((item) => item.type),
      ["run_interrupt_result"],
    );
    assert.deepEqual(observed, ["pull-requests-refreshed"]);
    const ids = backgroundScenarioIds("stop-then-steer-supersede");
    const expectedRequestId = yield* Effect.gen(function* () {
      const idAllocator = yield* IdAllocator.IdAllocatorV2;
      return idAllocator.derive.runSignalTurnItem({
        runId: ids.runId,
        signal: "interrupt-request",
      });
    }).pipe(Effect.provide(IdAllocator.layer));
    assert.equal(written[0]?.parentItemId, expectedRequestId);
  }),
);

it.effect("keeps a Stop that commits while a workspace handoff completes the run", () =>
  Effect.gen(function* () {
    const stopCommitted = yield* Ref.make(false);
    const { written, finalEvents, committedEffects } = yield* captureRootRunTermination({
      key: "stop-races-workspace-handoff",
      shouldFinalizeRun: () => Effect.succeed(true),
      // Stop commits after the handoff is detected and before the terminal write.
      hasWorkspaceMoved: () => Ref.set(stopCommitted, true).pipe(Effect.as(true)),
      // The sink refuses a guarded write once the Stop request is stored.
      beforeTerminalWrite: (request) =>
        Ref.get(stopCommitted).pipe(
          Effect.map((stopped) => !(stopped && request.unlessInterruptRequested !== undefined)),
        ),
    });
    const statuses = (type: OrchestrationV2DomainEvent["type"]) =>
      finalEvents.flatMap((event) =>
        event.type === type ? [(event.payload as { readonly status: string }).status] : [],
      );
    assert.deepEqual(statuses("run.updated"), ["interrupted"]);
    assert.deepEqual(statuses("run-attempt.updated"), ["interrupted"]);
    assert.deepEqual(
      written.map((item) => item.type),
      ["run_interrupt_result"],
    );
    assert.deepEqual(
      committedEffects.map((effect) => effect.request.type),
      ["checkpoint.capture"],
    );
  }),
);

it.effect("does not overwrite Stop when ownership changes after the finalization read", () =>
  Effect.gen(function* () {
    const { written, observed, submittedEffects, committedEffects } =
      yield* captureRootRunTermination({
        key: "stop-wins-finalization-gap",
        shouldFinalizeRun: () => Effect.succeed(true),
        rejectTerminalWrite: true,
      });
    assert.deepEqual(written, []);
    assert.deepEqual(observed, []);
    assert.deepEqual(
      submittedEffects.map((effect) => effect.request.type),
      ["checkpoint.capture"],
    );
    assert.deepEqual(committedEffects, []);
  }),
);

it.effect.each([
  { providerReason: undefined, requestReason: undefined, expected: "Stopped by the provider" },
  {
    providerReason: "preCompact hook (project): Save a checkpoint.",
    requestReason: undefined,
    expected: "preCompact hook (project): Save a checkpoint.",
  },
  {
    providerReason: "preCompact hook (project): Save a checkpoint.",
    requestReason: "Stopped by the user",
    expected: "Stopped by the user",
  },
])(
  "emits run_interrupt_result with $expected when finalizing the active attempt",
  ({ providerReason, requestReason, expected }) =>
    Effect.gen(function* () {
      const { written, observed, committedEffects } = yield* captureRootRunTermination({
        key: "hard-stop",
        shouldFinalizeRun: () => Effect.succeed(true),
        loadRunInterruptReason: () => Effect.succeed(requestReason),
        events: (ids) =>
          Stream.make({ ...rootTerminalEvent(ids, "interrupted"), reason: providerReason }),
      });
      assert.deepEqual(
        written.map((item) => item.type),
        ["run_interrupt_result"],
      );
      assert.deepEqual(observed, ["run:interrupted", "pull-requests-refreshed"]);
      assert.equal(
        written[0]?.type === "run_interrupt_result" ? written[0].message : undefined,
        expected,
      );
      assert.deepEqual(
        committedEffects.map((effect) => effect.request.type),
        ["checkpoint.capture"],
      );
    }),
);

it.effect("keeps a provider's stop reason on a completed run", () =>
  Effect.gen(function* () {
    const reason = "sessionStart hook (user): Plugin guard blocked this session.";
    const { written } = yield* captureRootRunTermination({
      key: "completed-stop-reason",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: (ids) => Stream.make({ ...rootTerminalEvent(ids, "completed"), reason }),
    });
    assert.deepEqual(
      written.map((item) => (item.type === "system_notice" ? item.message : item.type)),
      [reason],
    );
  }),
);

it.effect("records a finished run as failed when its ownership check cannot be read", () =>
  Effect.gen(function* () {
    const { observed } = yield* captureRootRunTermination({
      key: "finalize-guard-read-failure",
      shouldFinalizeRun: () =>
        Effect.fail(
          new ProjectionStore.ProjectionStoreReadError({
            threadId: ThreadId.make("thread:finalize-guard-read-failure"),
            cause: "database unavailable",
          }),
        ),
      events: (ids) => Stream.make(rootTerminalEvent(ids, "completed")),
    });
    assert.include(observed, "run:failed");
  }),
);

it.effect("does not refresh pull requests for auxiliary or stale provider terminals", () =>
  Effect.gen(function* () {
    const { observed } = yield* captureRootRunTermination({
      key: "pull-request-refresh:auxiliary",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: (ids) =>
        Stream.fromIterable([
          rootTerminalEvent(
            { ...ids, rootProviderTurnId: ProviderTurnId.make("turn:child") },
            "completed",
          ),
          rootTerminalEvent(
            { ...ids, rootProviderTurnId: ProviderTurnId.make("turn:previous") },
            "interrupted",
          ),
          rootTerminalEvent(ids, "cancelled"),
        ]),
    });
    assert.deepEqual(observed, ["run:cancelled", "pull-requests-refreshed"]);
  }),
);

it.effect("refreshes pull requests after a provider stream exits with an error", () =>
  Effect.gen(function* () {
    const { written, observed } = yield* captureRootRunTermination({
      key: "pull-request-refresh:stream-error",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: () =>
        Stream.fail(
          new ProviderAdapter.ProviderAdapterEventStreamError({
            driver,
            providerSessionId: ProviderSessionId.make("session:exited"),
            cause: "provider process exited",
          }),
        ),
    });
    assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
    assert.deepEqual(
      written.map((item) => item.type),
      ["error"],
    );
    const error = written.find((item) => item.type === "error");
    assert.include(error?.failure.message ?? "", "provider event stream closed unexpectedly");
  }),
);

it.effect("records a provider's input rejection at turn start in the terminal error", () =>
  Effect.gen(function* () {
    const rejection = "goal objective must be at most 4000 characters";
    const { written, observed } = yield* captureRootRunTermination({
      key: "start-failure:input-rejected",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: () => Stream.never,
      afterStart: (closed) => Deferred.succeed(closed, undefined),
      startTurn: (turnInput) =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterTurnStartError({
            driver,
            threadId: turnInput.threadId,
            providerThreadId: turnInput.providerThread.id,
            runId: turnInput.runId,
            cause: new CodexAppServerRequestError({
              code: -32600,
              errorMessage: rejection,
              method: "thread/goal/set",
            }),
          }),
        ),
    });
    assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
    const error = written.find((item) => item.type === "error");
    assert.include(error?.failure.message ?? "", rejection);
  }),
);

it.effect("marks a failed provider start call, so a wake it carried is sent again", () =>
  Effect.gen(function* () {
    const { written } = yield* captureRootRunTermination({
      key: "start-failure:call-failed",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: () => Stream.never,
      afterStart: (closed) => Deferred.succeed(closed, undefined),
      startTurn: (turnInput) =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterTurnStartError({
            driver,
            threadId: turnInput.threadId,
            providerThreadId: turnInput.providerThread.id,
            runId: turnInput.runId,
            cause: "The provider query closed before the prompt was queued.",
          }),
        ),
    });
    const error = written.find((item) => item.type === "error");
    assert.equal(error?.failure.class, "provider_error");
    assert.equal(error?.failure.code, "run_start_failed");
  }),
);

it.effect(
  "marks a turn that failed before its start call returned, so its wake is sent again",
  () =>
    Effect.gen(function* () {
      const ingested: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
      const terminalWritten = yield* Deferred.make<void>();
      let terminalWrites = 0;
      yield* captureRootRunTermination({
        key: "start-failure:terminal-first",
        shouldFinalizeRun: () => Effect.succeed(true),
        events: (ids) => Stream.make(rootTerminalEvent(ids, "failed")),
        ingestNormalized: (input) =>
          Effect.sync(() => {
            ingested.push(input.event);
            return [];
          }),
        // The provider reports the turn failed while its start call is pending, as OpenCode can;
        // the start call's own failure write then finds the run ended.
        beforeTerminalWrite: () =>
          Effect.sync(() => (terminalWrites += 1) === 1).pipe(
            Effect.tap(() => Deferred.succeed(terminalWritten, undefined)),
          ),
        startTurn: (turnInput) =>
          Deferred.await(terminalWritten).pipe(
            Effect.andThen(
              Effect.fail(
                new ProviderAdapter.ProviderAdapterTurnStartError({
                  driver,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause: "The prompt request failed.",
                }),
              ),
            ),
          ),
      });
      const terminal = ingested.find((event) => event.type === "turn.terminal");
      assert.equal(
        terminal?.type === "turn.terminal" && terminal.status === "failed"
          ? terminal.failure.code
          : undefined,
        "run_start_failed",
      );
    }),
);

it.effect("marks a run whose event stream failed before its start call returned", () =>
  Effect.gen(function* () {
    const failureWritten = yield* Deferred.make<void>();
    const committedFailures: Array<OrchestrationV2ProviderFailure> = [];
    yield* captureRootRunTermination({
      key: "start-failure:stream-first",
      shouldFinalizeRun: () => Effect.succeed(true),
      // The provider's stream fails while its start call is pending; the start call's own
      // failure write then finds the run ended.
      events: () =>
        Stream.fail(
          new ProviderAdapter.ProviderAdapterEventStreamError({
            driver,
            providerSessionId: ProviderSessionId.make("session:stream-first"),
            cause: "provider process exited",
          }),
        ),
      beforeTerminalWrite: (request) =>
        Effect.sync(() => {
          const first = committedFailures.length === 0;
          for (const event of request.events) {
            if (event.type === "turn-item.updated" && event.payload.type === "error" && first)
              committedFailures.push(event.payload.failure);
          }
          return first;
        }).pipe(Effect.tap(() => Deferred.succeed(failureWritten, undefined))),
      startTurn: (turnInput) =>
        Deferred.await(failureWritten).pipe(
          Effect.andThen(
            Effect.fail(
              new ProviderAdapter.ProviderAdapterTurnStartError({
                driver,
                threadId: turnInput.threadId,
                providerThreadId: turnInput.providerThread.id,
                runId: turnInput.runId,
                cause: "The prompt request failed.",
              }),
            ),
          ),
        ),
    });
    assert.lengthOf(committedFailures, 1);
    assert.equal(committedFailures[0]?.code, "run_start_failed");
  }),
);

it.effect("tells the user why /compact failed on a provider without compaction", () =>
  Effect.gen(function* () {
    const { written } = yield* captureRootRunTermination({
      key: "start-failure:compact-unsupported",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: () => Stream.never,
      afterStart: (closed) => Deferred.succeed(closed, undefined),
      messageText: "/compact",
    });
    const error = written.find((item) => item.type === "error");
    assert.include(error?.failure.message ?? "", "does not support /compact");
  }),
);

it.effect.each(["failure", "defect", "interrupt-failure", "ownership-lost"] as const)(
  "stops the live provider and background work before settling an ingestion %s",
  (failureMode) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const stopped = yield* Deferred.make<void>();
      const backgroundWorkRunning = yield* Ref.make(true);
      const backgroundWorkAtFinalization = yield* Ref.make<boolean | null>(null);
      const now = yield* DateTime.now;
      const interrupts = yield* Ref.make<
        ReadonlyArray<ProviderAdapter.ProviderAdapterV2InterruptInput>
      >([]);
      const ids = backgroundScenarioIds(`ingestion:${failureMode}`);
      const { written, observed, finalEvents } = yield* captureRootRunTermination({
        key: `ingestion:${failureMode}`,
        initialProviderTurnId: null,
        rejectTerminalWrite: failureMode === "ownership-lost",
        shouldFinalizeRun: () => Effect.succeed(true),
        startTurn: () =>
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(stopped))),
        events: () =>
          Stream.unwrap(
            Deferred.await(started).pipe(
              Effect.as(
                Stream.make({
                  type: "provider_turn.updated" as const,
                  driver,
                  providerTurn: {
                    id: ids.rootProviderTurnId,
                    providerThreadId: ids.providerThreadId,
                    nodeId: ids.rootNodeId,
                    runAttemptId: ids.attemptId,
                    nativeTurnRef: null,
                    ordinal: 1,
                    status: "running" as const,
                    startedAt: now,
                    completedAt: null,
                  },
                }),
              ),
            ),
          ),
        ingestNormalized: () => {
          const error = new ProviderEventIngestor.ProviderEventPublishError({
            providerSessionId: ProviderSessionId.make(`session:${failureMode}`),
            eventCount: 1,
            cause: toPersistenceSqlError("OrchestrationEventStore.appendAgentEvents:insert")(
              new SqlError({
                reason: classifySqliteError(
                  Object.assign(new Error("unable to open database file"), { errcode: 14 }),
                ),
              }),
            ),
          });
          return failureMode === "defect" ? Effect.die(error) : Effect.fail(error);
        },
        interruptTurn: (input) =>
          Ref.update(interrupts, (current) => [...current, input]).pipe(
            Effect.andThen(
              input.requestRuntimeRestart === true && failureMode !== "interrupt-failure"
                ? Ref.set(backgroundWorkRunning, false)
                : Effect.void,
            ),
            Effect.andThen(Deferred.succeed(stopped, undefined)),
            Effect.andThen(
              failureMode === "interrupt-failure"
                ? Effect.fail(
                    new ProviderAdapter.ProviderAdapterInterruptError({
                      driver,
                      providerThreadId: input.providerThread.id,
                      providerTurnId: input.providerTurnId,
                      cause: "provider already exited",
                    }),
                  )
                : Effect.void,
            ),
          ),
        beforeTerminalWrite: () =>
          Effect.gen(function* () {
            const calls = yield* Ref.get(interrupts);
            assert.equal(calls.length, 1);
            assert.equal(calls[0]?.providerTurnId, ids.rootProviderTurnId);
            assert.equal(calls[0]?.providerThread.id, ids.providerThreadId);
            yield* Ref.set(backgroundWorkAtFinalization, yield* Ref.get(backgroundWorkRunning));
            return true;
          }),
      });
      if (failureMode === "ownership-lost") {
        assert.deepEqual(finalEvents, []);
        assert.deepEqual(observed, []);
      } else {
        assert.equal(
          yield* Ref.get(backgroundWorkAtFinalization),
          failureMode === "interrupt-failure",
        );
        assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
        const error = written.find((item) => item.type === "error");
        assert.include(error?.failure.message ?? "", "(SQLITE_CANTOPEN)");
        const repaired = finalEvents.find((event) => event.type === "provider-turn.updated");
        if (failureMode === "interrupt-failure") {
          assert.isUndefined(repaired);
        } else {
          assert.equal(repaired?.payload.id, ids.rootProviderTurnId);
          assert.equal(repaired?.payload.status, "interrupted");
          assert.isNotNull(repaired?.payload.completedAt);
        }
      }
    }),
);

it.effect.each([
  { status: "completed", failure: "later-stream", interrupt: "succeeds" },
  { status: "failed", failure: "later-stream", interrupt: "succeeds" },
  { status: "completed", failure: "terminal-ingest", interrupt: "succeeds" },
  { status: "completed", failure: "terminal-ingest", interrupt: "fails" },
] as const)(
  "stores a $status root provider turn snapshot after a $failure failure only if the interrupt succeeds (it $interrupt)",
  ({ status, failure, interrupt }) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const terminalAt = DateTime.subtract(now, { seconds: 1 });
      const key = `terminal-snapshot:${status}:${failure}:${interrupt}`;
      const durableTurns = yield* Ref.make<
        ReadonlyMap<ProviderTurnId, OrchestrationV2ProviderTurn>
      >(new Map());
      const persist = (turn: OrchestrationV2ProviderTurn) =>
        Ref.update(durableTurns, (current) => new Map(current).set(turn.id, turn));
      const interrupts = yield* Ref.make<
        ReadonlyArray<ProviderAdapter.ProviderAdapterV2InterruptInput>
      >([]);
      const ids = backgroundScenarioIds(key);
      const rootTurn = (
        turnStatus: OrchestrationV2ProviderTurn["status"],
        completedAt: DateTime.Utc | null,
      ) => ({
        type: "provider_turn.updated" as const,
        driver,
        providerTurn: {
          id: ids.rootProviderTurnId,
          providerThreadId: ids.providerThreadId,
          nodeId: ids.rootNodeId,
          runAttemptId: ids.attemptId,
          nativeTurnRef: null,
          ordinal: 1,
          status: turnStatus,
          startedAt: terminalAt,
          completedAt,
        },
      });
      const { observed, finalEvents } = yield* captureRootRunTermination({
        key,
        shouldFinalizeRun: () => Effect.succeed(true),
        events: () =>
          Stream.make(rootTurn("running", null), rootTurn(status, terminalAt)).pipe(
            Stream.concat(
              failure === "later-stream"
                ? Stream.fail(
                    new ProviderAdapter.ProviderAdapterEventStreamError({
                      driver,
                      providerSessionId: ProviderSessionId.make(`session:${key}`),
                      cause: "provider process exited",
                    }),
                  )
                : Stream.empty,
            ),
          ),
        ingestNormalized: ({ event }) =>
          event.type !== "provider_turn.updated"
            ? Effect.succeed([])
            : failure === "terminal-ingest" && event.providerTurn.status !== "running"
              ? Effect.fail(
                  new ProviderEventIngestor.ProviderEventPublishError({
                    providerSessionId: ProviderSessionId.make(`session:${key}`),
                    eventCount: 1,
                    cause: new Error("SQLITE_FULL: database or disk is full"),
                  }),
                )
              : persist(event.providerTurn).pipe(Effect.as([])),
        interruptTurn: (input) =>
          Ref.update(interrupts, (current) => [...current, input]).pipe(
            Effect.andThen(
              interrupt === "fails"
                ? Effect.fail(
                    new ProviderAdapter.ProviderAdapterInterruptError({
                      driver,
                      providerThreadId: input.providerThread.id,
                      providerTurnId: input.providerTurnId,
                      cause: "provider already exited",
                    }),
                  )
                : Effect.void,
            ),
          ),
        beforeTerminalWrite: (request) =>
          Effect.forEach(request.events, (event) =>
            event.type === "provider-turn.updated" ? persist(event.payload) : Effect.void,
          ).pipe(Effect.as(true)),
      });
      const calls = yield* Ref.get(interrupts);
      assert.equal(calls.length, 1);
      assert.isTrue(calls[0]?.requestRuntimeRestart);
      assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
      assert.isTrue(finalEvents.some((event) => event.type === "run.updated"));
      const durable = (yield* Ref.get(durableTurns)).get(ids.rootProviderTurnId);
      // A failed interrupt keeps the stored turn running for the next start to retry.
      assert.equal(durable?.status, interrupt === "fails" ? "running" : status);
      assert.deepEqual(durable?.completedAt, interrupt === "fails" ? null : terminalAt);
    }),
);

it.effect("does not interrupt an ended provider when its terminal write fails", () =>
  Effect.gen(function* () {
    const interrupts = yield* Ref.make(0);
    const writes = yield* Ref.make(0);
    const { observed } = yield* captureRootRunTermination({
      key: "terminal-write-failure",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: (ids) => Stream.make(rootTerminalEvent(ids, "completed")),
      interruptTurn: () => Ref.update(interrupts, (count) => count + 1),
      beforeTerminalWrite: () =>
        Effect.gen(function* () {
          if ((yield* Ref.updateAndGet(writes, (count) => count + 1)) === 1) {
            return yield* new EventSink.EventSinkWriteError({ eventCount: 1 });
          }
          return true;
        }),
    });
    assert.equal(yield* Ref.get(interrupts), 0);
    assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
  }),
);

it.effect("ends a running root command when the provider stream fails mid-turn", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const { written, observed } = yield* captureRootRunTermination({
      key: "stream-error-running-command",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: (ids) =>
        Stream.make(
          {
            type: "provider_turn.updated" as const,
            driver,
            providerTurn: {
              id: ids.rootProviderTurnId,
              providerThreadId: ids.providerThreadId,
              nodeId: ids.rootNodeId,
              runAttemptId: ids.attemptId,
              nativeTurnRef: null,
              ordinal: 1,
              status: "running" as const,
              startedAt: now,
              completedAt: null,
            },
          },
          backgroundTurnItemEvent(ids, "command_execution", "running", 1),
        ).pipe(
          Stream.concat(
            Stream.fail(
              new ProviderAdapter.ProviderAdapterEventStreamError({
                driver,
                providerSessionId: ProviderSessionId.make("session:stream-error-running-command"),
                cause: "provider process exited",
              }),
            ),
          ),
        ),
      // The dead process cannot take the interrupt either.
      interruptTurn: (input) =>
        Effect.fail(
          new ProviderAdapter.ProviderAdapterInterruptError({
            driver,
            providerThreadId: input.providerThread.id,
            providerTurnId: input.providerTurnId,
            cause: "provider already exited",
          }),
        ),
    });
    assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
    const ids = backgroundScenarioIds("stream-error-running-command");
    const endedCommand = written.find(
      (item) => item.id === ids.itemId && item.status !== "running",
    );
    assert.isDefined(endedCommand);
  }),
);

it.effect("fails the run when a released session's interrupt exits interrupted", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const key = "released-session-interrupted-interrupt";
    const { observed, finalEvents } = yield* captureRootRunTermination({
      key,
      shouldFinalizeRun: () => Effect.succeed(true),
      events: (ids) =>
        Stream.make({
          type: "provider_turn.updated" as const,
          driver,
          providerTurn: {
            id: ids.rootProviderTurnId,
            providerThreadId: ids.providerThreadId,
            nodeId: ids.rootNodeId,
            runAttemptId: ids.attemptId,
            nativeTurnRef: null,
            ordinal: 1,
            status: "running" as const,
            startedAt: now,
            completedAt: null,
          },
        }).pipe(
          Stream.concat(
            Stream.fail(
              new ProviderAdapter.ProviderAdapterEventStreamError({
                driver,
                providerSessionId: ProviderSessionId.make(`session:${key}`),
                raisedByT3: true,
                cause: "Provider sign-in changed.",
              }),
            ),
          ),
        ),
      // An ACP session whose scope is already closing interrupts the join.
      interruptTurn: () => Effect.interrupt,
    });
    assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
    assert.isTrue(finalEvents.some((event) => event.type === "run.updated"));
  }),
);

it.effect("keeps a running root command open when a write fails under a live provider", () =>
  Effect.gen(function* () {
    const key = "write-error-running-command";
    const ids = backgroundScenarioIds(key);
    const secondItemId = TurnItemId.make(`${ids.itemId}:second`);
    const { written, observed } = yield* captureRootRunTermination({
      key,
      shouldFinalizeRun: () => Effect.succeed(true),
      events: () =>
        Stream.make(
          backgroundTurnItemEvent(ids, "command_execution", "running", 1),
          backgroundTurnItemEvent(ids, "command_execution", "running", 2, secondItemId),
        ),
      ingestNormalized: ({ event }) =>
        event.type === "turn_item.updated" && event.turnItem.id === secondItemId
          ? Effect.fail(
              new ProviderEventIngestor.ProviderEventPublishError({
                providerSessionId: ProviderSessionId.make(`session:${key}`),
                eventCount: 1,
                cause: new Error("SQLITE_FULL: database or disk is full"),
              }),
            )
          : Effect.succeed([]),
    });
    assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
    assert.isUndefined(written.find((item) => item.id === ids.itemId));
  }),
);

it.effect.each([
  { ended: true },
  // T3 ending the stream proves no dead runtime; Stop settles the work it releases.
  { ended: false, raisedByT3: true },
  // A Stop that already settled the subagent keeps its result.
  { ended: false, stopped: true },
] as const)(
  "ends a subagent that outlived its root when the provider stream fails (%o)",
  ({ ended, raisedByT3, stopped }) =>
    Effect.gen(function* () {
      const providerInstanceId = ProviderInstanceId.make("codex");
      const { finalEvents } = yield* captureRootRunTermination({
        key: "stream-error-outlived-subagent",
        shouldFinalizeRun: () => Effect.succeed(true),
        events: (ids) =>
          Stream.make(
            {
              type: "subagent.updated" as const,
              driver,
              subagent: makeRunOwnedSubagentFixture({
                ids,
                providerInstanceId,
                childThreadId: ids.childThreadId,
                driver,
                status: "running",
              }),
            },
            {
              type: "turn_item.updated" as const,
              driver,
              turnItem: makeRunOwnedSubagentTurnItemFixture({
                ids,
                providerInstanceId,
                childThreadId: ids.childThreadId,
                driver,
                status: "running",
              }),
            },
            rootTerminalEvent(ids, "completed"),
          ).pipe(
            Stream.concat(
              Stream.fail(
                new ProviderAdapter.ProviderAdapterEventStreamError({
                  driver,
                  providerSessionId: ProviderSessionId.make(
                    "session:stream-error-outlived-subagent",
                  ),
                  ...(raisedByT3 === undefined ? {} : { raisedByT3 }),
                  cause: "provider process exited",
                }),
              ),
            ),
          ),
        ...(stopped === true
          ? {
              settleStored: (work) => ({
                ...work,
                subagents: work.subagents.map((row) => ({ ...row, status: "interrupted" })),
                turnItems: work.turnItems.map((row) => ({ ...row, status: "interrupted" })),
              }),
            }
          : {}),
      });
      assert.deepEqual(
        finalEvents.flatMap((event) =>
          event.type === "run.updated" ||
          event.type === "subagent.updated" ||
          event.type === "turn-item.updated"
            ? [`${event.type}:${event.payload.status}`]
            : [],
        ),
        ended
          ? ["run.updated:waiting", "subagent.updated:failed", "turn-item.updated:failed"]
          : ["run.updated:waiting"],
      );
    }),
);

it.effect("restores dropped inline handoffs when ingestion fails before the terminal", () =>
  Effect.gen(function* () {
    const key = "dropped-turn-ingest-error";
    const ids = backgroundScenarioIds(key);
    const handoff = { id: `handoff:${key}` } as unknown as OrchestrationV2ContextHandoff;
    const { finalEvents } = yield* captureRootRunTermination({
      key,
      shouldFinalizeRun: () => Effect.succeed(true),
      loadDroppedInlineHandoffs: () => Effect.succeed([handoff]),
      events: () =>
        Stream.make({
          type: "provider_turn.updated" as const,
          driver,
          threadId: ids.threadId,
          providerTurn: {
            id: ids.rootProviderTurnId,
            providerThreadId: ids.providerThreadId,
            runAttemptId: ids.attemptId,
            ordinal: 1,
            status: "failed",
            promptDropped: true,
          } as OrchestrationV2ProviderTurn,
        }),
      ingestNormalized: ({ event }) =>
        event.type === "provider_turn.updated"
          ? Effect.fail(
              new ProviderEventIngestor.ProviderEventPublishError({
                providerSessionId: ProviderSessionId.make(`session:${key}`),
                eventCount: 1,
                cause: new Error("SQLITE_FULL: database or disk is full"),
              }),
            )
          : Effect.succeed([]),
    });
    assert.deepEqual(
      finalEvents.flatMap((event) =>
        event.type === "context-handoff.updated" ? [event.payload.id] : [],
      ),
      [handoff.id],
    );
  }),
);

it.effect("does not interrupt a provider when ingestion fails before the root turn is known", () =>
  Effect.gen(function* () {
    const interrupts = yield* Ref.make(0);
    const { observed } = yield* captureRootRunTermination({
      key: "unknown-turn-stream-error",
      initialProviderTurnId: null,
      shouldFinalizeRun: () => Effect.succeed(true),
      events: () =>
        Stream.fail(
          new ProviderAdapter.ProviderAdapterEventStreamError({
            driver,
            providerSessionId: ProviderSessionId.make("session:unknown-turn"),
            cause: "provider process exited",
          }),
        ),
      interruptTurn: () => Ref.update(interrupts, (count) => count + 1),
    });
    assert.equal(yield* Ref.get(interrupts), 0);
    assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
  }),
);

it.effect("interrupts a provider turn that starts after Stop settled its attempt", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const key = "turn-after-stop";
    const ids = backgroundScenarioIds(key);
    const stopped = yield* Ref.make(false);
    const accepted = yield* Deferred.make<void>();
    const interrupted = yield* Deferred.make<ProviderAdapter.ProviderAdapterV2InterruptInput>();
    yield* captureRootRunTermination({
      key,
      initialProviderTurnId: null,
      shouldFinalizeRun: () => Ref.get(stopped).pipe(Effect.map((value) => !value)),
      shouldStartProviderTurn: () => Ref.get(stopped).pipe(Effect.map((value) => !value)),
      // Stop commits while the provider accepts the prompt.
      startTurn: () =>
        Ref.set(stopped, true).pipe(Effect.andThen(Deferred.succeed(accepted, undefined))),
      events: () =>
        Stream.unwrap(
          Deferred.await(accepted).pipe(
            Effect.as(
              Stream.make({
                type: "provider_turn.updated" as const,
                driver,
                providerTurn: {
                  id: ids.rootProviderTurnId,
                  providerThreadId: ids.providerThreadId,
                  nodeId: ids.rootNodeId,
                  runAttemptId: ids.attemptId,
                  nativeTurnRef: null,
                  ordinal: 1,
                  status: "running" as const,
                  startedAt: now,
                  completedAt: null,
                },
              }).pipe(
                Stream.concat(
                  Stream.fromEffect(
                    Deferred.await(interrupted).pipe(
                      Effect.as(rootTerminalEvent(ids, "interrupted")),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      interruptTurn: (input) => Deferred.succeed(interrupted, input).pipe(Effect.asVoid),
    });
    const call = yield* Deferred.await(interrupted);
    assert.equal(call.providerTurnId, ids.rootProviderTurnId);
    assert.equal(call.providerThread.id, ids.providerThreadId);
  }),
);

// The server browser keeps an agent's tab focused for exactly this span.
it.effect.each(["completed", "failed"] as const)(
  "reports the thread executing until its %s run's event consumer ends",
  (outcome) =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const { previewActivityEnded } = yield* captureRootRunTermination({
        key: `preview-activity:${outcome}`,
        shouldFinalizeRun: () => Effect.succeed(true),
        events: (ids) =>
          Stream.fromEffect(Deferred.await(gate)).pipe(
            Stream.flatMap(() =>
              outcome === "completed"
                ? Stream.make(rootTerminalEvent(ids, "completed"))
                : Stream.fail(
                    new ProviderAdapter.ProviderAdapterEventStreamError({
                      driver,
                      providerSessionId: ProviderSessionId.make("session:exited"),
                      cause: "provider process exited",
                    }),
                  ),
            ),
          ),
        afterStart: () =>
          Effect.gen(function* () {
            const activity = yield* PreviewAgentActivity.PreviewAgentActivity;
            assert.isTrue(
              yield* activity.active(ThreadId.make(`thread:preview-activity:${outcome}`)),
            );
            yield* Deferred.succeed(gate, undefined);
          }),
      });
      assert.isTrue(previewActivityEnded);
    }),
);

// Background commands keep the consumer alive for the whole session; only the agent's own
// turn and the children it drives should hold the browser tab focused.
it.effect.each([
  { name: "ends once the root turn finishes beside background work", openSubagent: false },
  { name: "stays while a run-owned subagent continues", openSubagent: true },
])("preview activity $name", ({ openSubagent }) =>
  Effect.gen(function* () {
    const key = `preview-activity:background:${openSubagent}`;
    const consumerWaiting = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const providerInstanceId = ProviderInstanceId.make("codex");
    yield* captureRootRunTermination({
      key,
      shouldFinalizeRun: () => Effect.succeed(true),
      hasPendingBackgroundWorkForThread: () => Effect.succeed(true),
      events: (ids) =>
        Stream.concat(
          Stream.fromIterable([
            ...(openSubagent
              ? [
                  {
                    type: "subagent.updated",
                    driver,
                    subagent: makeRunOwnedSubagentFixture({
                      ids,
                      providerInstanceId,
                      childThreadId: ids.childThreadId,
                      driver,
                      status: "running",
                    }),
                  } as const,
                ]
              : []),
            rootTerminalEvent(ids, "completed"),
          ] satisfies ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>),
          Stream.fromEffect(
            Deferred.succeed(consumerWaiting, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
            ),
          ).pipe(Stream.drain),
        ),
      afterStart: (closed) =>
        Effect.gen(function* () {
          yield* Deferred.await(consumerWaiting);
          assert.isFalse(yield* Deferred.isDone(closed));
          const activity = yield* PreviewAgentActivity.PreviewAgentActivity;
          assert.equal(yield* activity.active(ThreadId.make(`thread:${key}`)), openSubagent);
          yield* Deferred.succeed(release, undefined);
        }),
    });
  }),
);

// EventSink owns commit batching; this layer must preserve event order and terminal delivery.
// A backlog is ingested once per item state: an item's newer snapshot takes its waiting one's place,
// so the command's two running reports never reach the commit path.
it.effect.each([
  { count: 21, text: 0 },
  { count: 41, text: 10_000 },
])(
  "ingests a provider backlog once and in order, each item at its newest state ($count events, $text bytes each)",
  ({ count, text }) =>
    Effect.gen(function* () {
      const key = `backlog-${count}-${text}`;
      const ids = backgroundScenarioIds(key);
      const providerEvents = yield* Queue.unbounded<
        ProviderAdapter.ProviderAdapterV2Event,
        Cause.Done
      >();
      // A background command starts, updates and ends among them: only the changes it makes to
      // the run's tracking are written on their own.
      const command = TurnItemId.make(`turn-item:${key}:command`);
      const commandUpdates = new Map([
        [5, backgroundTurnItemEvent(ids, "command_execution", "running", 5, command)],
        [10, backgroundTurnItemEvent(ids, "command_execution", "running", 5, command)],
        [15, backgroundTurnItemEvent(ids, "command_execution", "completed", 5, command)],
      ]);
      const items = Array.from(
        { length: count },
        (_, index): ProviderAdapter.ProviderAdapterV2Event =>
          commandUpdates.get(index) ??
          ({
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: TurnItemId.make(`turn-item:${key}:${index}`),
              threadId: ids.threadId,
              runId: ids.runId,
              providerTurnId: ids.rootProviderTurnId,
              ordinal: index + 1,
              type: "assistant_message",
              status: "completed",
              streaming: false,
              text: "x".repeat(text),
            },
          } as ProviderAdapter.ProviderAdapterV2Event),
      );
      const terminal = rootTerminalEvent(ids, "completed");
      const writes: Array<ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>> = [];
      const writing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      // The first write holds the writer until everything else has queued behind it.
      const write = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
        Effect.gen(function* () {
          writes.push(events);
          if (writes.length === 1) {
            yield* Deferred.succeed(writing, undefined);
            yield* Deferred.await(release);
          }
        });
      yield* Queue.offer(providerEvents, items[0]!);
      yield* captureRootRunTermination({
        key,
        shouldFinalizeRun: () => Effect.succeed(true),
        events: () => Stream.fromQueue(providerEvents),
        ingestNormalized: (input) => write([input.event]).pipe(Effect.as([])),
        afterStart: () =>
          Deferred.await(writing).pipe(
            Effect.andThen(Queue.offerAll(providerEvents, [...items.slice(1), terminal])),
            Effect.andThen(Deferred.succeed(release, undefined)),
          ),
      });
      // The command takes its first report's place with its last state; everything else, the
      // terminal last, once and in the provider's order.
      assert.deepEqual(writes.flat(), [
        ...items.slice(0, 5),
        items[15],
        ...items.slice(6, 10),
        ...items.slice(11, 15),
        ...items.slice(16),
        terminal,
      ]);
    }),
);

it.effect("closes a child whose completion failed to write", () =>
  Effect.gen(function* () {
    const key = "child-close-failed";
    const ids = backgroundScenarioIds(key);
    const node = (status: "running" | "completed") =>
      ({
        type: "node.updated",
        driver,
        node: { ...makeRunOwnedSubagentNodeFixture({ ids, status: "running" }), status },
      }) as const;
    // The child's running state is stored before its completion arrives (a completion that arrives
    // with it replaces it; see below).
    const runningStored = yield* Deferred.make<void>();
    const { finalEvents } = yield* captureRootRunTermination({
      key,
      shouldFinalizeRun: () => Effect.succeed(true),
      events: () =>
        Stream.make(node("running")).pipe(
          Stream.concat(
            Stream.fromEffect(Deferred.await(runningStored).pipe(Effect.as(node("completed")))),
          ),
        ),
      ingestNormalized: (input) =>
        input.event.type === "node.updated" && input.event.node.status === "completed"
          ? Effect.fail(
              new ProviderEventIngestor.ProviderEventPublishError({
                providerSessionId: ProviderSessionId.make(`session:${key}`),
                eventCount: 1,
              }),
            )
          : Deferred.succeed(runningStored, undefined).pipe(Effect.as([])),
    });
    const closed = finalEvents.find(
      (event) => event.type === "node.updated" && event.payload.id === ids.subagentNodeId,
    );
    assert.isDefined(closed);
    assert.notEqual(closed?.type === "node.updated" ? closed.payload.status : undefined, "running");
  }),
);

it.effect(
  "stores nothing open for a child whose completion arrived with its start and failed to write",
  () =>
    Effect.gen(function* () {
      const key = "child-close-failed-coalesced";
      const ids = backgroundScenarioIds(key);
      const node = (status: "running" | "completed") =>
        ({
          type: "node.updated",
          driver,
          node: { ...makeRunOwnedSubagentNodeFixture({ ids, status: "running" }), status },
        }) as const;
      const stored: Array<string> = [];
      const { finalEvents } = yield* captureRootRunTermination({
        key,
        shouldFinalizeRun: () => Effect.succeed(true),
        events: () => Stream.make(node("running"), node("completed")),
        ingestNormalized: (input) =>
          input.event.type === "node.updated" && input.event.node.status === "completed"
            ? Effect.fail(
                new ProviderEventIngestor.ProviderEventPublishError({
                  providerSessionId: ProviderSessionId.make(`session:${key}`),
                  eventCount: 1,
                }),
              )
            : Effect.sync(() => {
                if (input.event.type === "node.updated") stored.push(input.event.node.status);
              }).pipe(Effect.as([])),
      });
      // The completion took the start's place and failed: the child was never stored, so nothing is
      // left running and there is nothing to close.
      assert.deepEqual(stored, []);
      assert.isUndefined(
        finalEvents.find(
          (event) =>
            event.type === "node.updated" &&
            event.payload.id === ids.subagentNodeId &&
            event.payload.status === "running",
        ),
      );
    }),
);

it.effect("fails the run when its event stream ends without a root terminal", () =>
  Effect.gen(function* () {
    const { observed, finalEvents } = yield* captureRootRunTermination({
      key: "stream-end-without-terminal",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: () => Stream.empty,
    });
    assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
    assert.equal(
      finalEvents.find((event) => event.type === "provider-thread.updated")?.payload.status,
      "idle",
    );
  }),
);

it.effect("leaves the run to restart recovery when shutdown interrupts its event stream", () =>
  Effect.gen(function* () {
    const { written, observed } = yield* captureRootRunTermination({
      key: "stream-end:shutdown",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: () => Stream.failCause(Cause.interrupt()),
    });
    assert.deepEqual(written, []);
    assert.deepEqual(observed, []);
  }),
);

it.effect("does not start a turn whose run settled during the app context read", () =>
  Effect.gen(function* () {
    const contextReadStarted = yield* Deferred.make<void>();
    const runFailed = yield* Deferred.make<void>();
    const current = yield* Ref.make(true);
    const providerCalls = yield* Ref.make(0);
    const { observed } = yield* captureRootRunTermination({
      key: "stream-end:context-read",
      shouldStartProviderTurn: () => Ref.get(current),
      shouldFinalizeRun: () => Ref.get(current),
      events: () => Stream.unwrap(Deferred.await(contextReadStarted).pipe(Effect.as(Stream.empty))),
      appContext: Deferred.succeed(contextReadStarted, undefined).pipe(
        Effect.andThen(Deferred.await(runFailed)),
        Effect.as([]),
      ),
      refreshAfterTurn: Ref.set(current, false).pipe(
        Effect.andThen(Deferred.succeed(runFailed, undefined)),
      ),
      startTurn: () => Ref.update(providerCalls, (count) => count + 1),
    });
    assert.deepEqual(observed, ["run:failed", "pull-requests-refreshed"]);
    assert.equal(yield* Ref.get(providerCalls), 0);
  }),
);

it.effect("keeps completed runs completed when pull request refresh fails", () =>
  Effect.gen(function* () {
    const { observed } = yield* captureRootRunTermination({
      key: "pull-request-refresh:refresh-failure",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: (ids) => Stream.make(rootTerminalEvent(ids, "completed")),
      refreshAfterTurn: Effect.die("refresh failed"),
    });
    assert.deepEqual(observed, ["run:waiting", "pull-requests-refreshed"]);
  }),
);

it.effect("completes a run while its after-turn pull request refresh waits on the forge", () =>
  Effect.gen(function* () {
    const refreshStarted = yield* Deferred.make<void>();
    const capture = yield* captureRootRunTermination({
      key: "pull-request-refresh:forge-waits",
      shouldFinalizeRun: () => Effect.succeed(true),
      events: (ids) => Stream.make(rootTerminalEvent(ids, "completed")),
      // The server's observer, whose pull request refresh never gets its forge answer.
      observer: RunFinalizationService.layerObserver.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(WorkspaceEntries.WorkspaceEntries)({}),
            Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({}),
            Layer.mock(ProjectionStore.ProjectionStoreV2)({}),
            Layer.mock(ProjectService.ProjectService)({}),
            Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({}),
            Layer.mock(PullRequestService.PullRequestService)({
              refreshAfterTurn: () =>
                Deferred.succeed(refreshStarted, undefined).pipe(Effect.andThen(Effect.never)),
            }),
          ),
        ),
      ),
      // The run's ingestion ends, and its refresh was requested and is still waiting.
      afterStart: (closed) =>
        Deferred.await(closed).pipe(Effect.andThen(Deferred.await(refreshStarted))),
    }).pipe(Effect.timeoutOption("1 minute"), Effect.forkChild({ startImmediately: true }));
    yield* TestClock.adjust("1 minute");
    const result = yield* Fiber.join(capture);
    assert.isTrue(
      Option.isSome(result),
      "the run's finalization waited for the after-turn pull request refresh",
    );
    if (Option.isSome(result)) assert.deepEqual(result.value.observed, ["run:waiting"]);
  }),
);

it.effect.each(["completed", "failed"] as const)(
  "persists a background-task roster clear that arrives after a %s turn",
  (status) =>
    Effect.gen(function* () {
      const pending = yield* Ref.make(true);
      const ingested = yield* Ref.make<Array<ProviderAdapter.ProviderAdapterV2Event>>([]);
      yield* captureRootRunTermination({
        key: `roster-clear-${status}`,
        shouldFinalizeRun: () => Effect.succeed(true),
        hasPendingBackgroundWorkForThread: () => Ref.get(pending),
        events: (ids) => {
          const runningBuild: OrchestrationV2PendingBackgroundTask = {
            taskId: "running-build",
            kind: "command",
          };
          const roster = (hasTask: boolean): ProviderAdapter.ProviderAdapterV2Event => {
            const pendingBackgroundTasks: ReadonlyArray<OrchestrationV2PendingBackgroundTask> =
              hasTask ? [runningBuild] : [];
            return {
              type: "provider_thread.updated",
              driver,
              providerThread: {
                id: ids.providerThreadId,
                driver,
                appThreadId: ids.threadId,
                pendingBackgroundTasks,
              } as OrchestrationV2ProviderThread,
            };
          };
          return Stream.fromIterable([roster(true), rootTerminalEvent(ids, status), roster(false)]);
        },
        // The adapter clears its live roster before ingestion reaches the queued clear.
        beforeTerminalWrite: () => Ref.set(pending, false).pipe(Effect.as(true)),
        ingestNormalized: (input) =>
          Ref.update(ingested, (all) => [...all, input.event]).pipe(
            Effect.as(
              input.event.type === "provider_thread.updated"
                ? [{} as OrchestrationV2StoredEvent]
                : [],
            ),
          ),
      });
      const rosters = (yield* Ref.get(ingested)).flatMap((event) =>
        event.type === "provider_thread.updated"
          ? [event.providerThread.pendingBackgroundTasks]
          : [],
      );
      assert.deepEqual(rosters.at(-1), [], "an ended task must leave the persisted roster");
    }),
);

function captureRootRunTermination(input: {
  readonly key: string;
  readonly shouldFinalizeRun: () => Effect.Effect<boolean, ProjectionStore.ProjectionStoreV2Error>;
  readonly rejectTerminalWrite?: boolean;
  readonly hasUnpairedRunInterruptRequest?: () => Effect.Effect<boolean, never>;
  readonly hasWorkspaceMoved?: () => Effect.Effect<boolean, never>;
  readonly loadRunInterruptReason?: () => Effect.Effect<string | undefined, never>;
  readonly loadDroppedInlineHandoffs?: RunExecutionService.RunExecutionServiceV2StartRootRunInput["loadDroppedInlineHandoffs"];
  readonly hasPendingBackgroundWorkForThread?: ProviderAdapter.ProviderAdapterV2SessionRuntime["hasPendingBackgroundWorkForThread"];
  readonly seedOpenSubagent?: boolean;
  readonly events?: (
    ids: BackgroundScenarioIds,
  ) => Stream.Stream<
    ProviderAdapter.ProviderAdapterV2Event,
    ProviderAdapter.ProviderAdapterV2Error
  >;
  readonly startTurn?: ProviderAdapter.ProviderAdapterV2SessionRuntime["startTurn"];
  readonly interruptTurn?: ProviderAdapter.ProviderAdapterV2SessionRuntime["interruptTurn"];
  readonly messageText?: string;
  readonly initialProviderTurnId?: ProviderTurnId | null;
  readonly onStartFailure?: (
    error: RunExecutionService.RunExecutionServiceV2Error,
  ) => Effect.Effect<void>;
  readonly refreshAfterTurn?: Effect.Effect<void>;
  // Replaces the recording observer, e.g. with the server's own (a reference: no requirement).
  readonly observer?: Layer.Layer<never>;
  readonly onSubscriptionClose?: Effect.Effect<void>;
  readonly shouldStartProviderTurn?: RunExecutionService.RunExecutionServiceV2StartRootRunInput["shouldStartProviderTurn"];
  readonly appContext?: ReturnType<McpAppModelContext.McpAppModelContext["Service"]["forThread"]>;
  readonly ingestNormalized?: ProviderEventIngestor.ProviderEventIngestorV2["Service"]["ingestNormalized"];
  readonly beforeTerminalWrite?: (
    request: Parameters<EventSink.EventSinkV2["Service"]["writeIfRunCurrent"]>[0],
  ) => Effect.Effect<boolean, EventSink.EventSinkV2Error>;
  readonly afterStart?: (
    closed: Deferred.Deferred<void>,
  ) => Effect.Effect<void, never, PreviewAgentActivity.PreviewAgentActivity>;
  /** Changes the stored rows after the provider reported them, as a concurrent Stop does. */
  readonly settleStored?: (
    work: RunExecutionService.UnsettledWork,
  ) => RunExecutionService.UnsettledWork;
}) {
  return Effect.gen(function* () {
    const ids = backgroundScenarioIds(input.key);
    // The rows the provider's events stored, which the run reads back when its runtime is lost.
    const stored = {
      subagents: new Map<string, OrchestrationV2Subagent>(),
      nodes: new Map<string, OrchestrationV2ExecutionNode>(),
      turnItems: new Map<string, OrchestrationV2TurnItem>(),
    };
    const store = (event: ProviderAdapter.ProviderAdapterV2Event) =>
      Effect.sync(() => {
        if (event.type === "subagent.updated")
          stored.subagents.set(event.subagent.id, event.subagent);
        if (event.type === "node.updated") stored.nodes.set(event.node.id, event.node);
        if (event.type === "turn_item.updated")
          stored.turnItems.set(event.turnItem.id, event.turnItem);
      });
    const loadUnsettledWork = (threadId: ThreadId) =>
      Effect.sync(() => {
        const work = {
          subagents: [...stored.subagents.values()].filter((row) => row.threadId === threadId),
          nodes: [...stored.nodes.values()].filter((row) => row.threadId === threadId),
          turnItems: [...stored.turnItems.values()].filter((row) => row.threadId === threadId),
        };
        return input.settleStored?.(work) ?? work;
      });
    const providerInstanceId = ProviderInstanceId.make("codex");
    const runningSubagent = makeRunOwnedSubagentFixture({
      ids,
      providerInstanceId,
      childThreadId: ids.childThreadId,
      driver,
      status: "running",
    });
    const writtenItems = yield* Ref.make<ReadonlyArray<OrchestrationV2TurnItem>>([]);
    const finalEvents = yield* Ref.make<ReadonlyArray<OrchestrationV2DomainEvent>>([]);
    const observed = yield* Ref.make<ReadonlyArray<string>>([]);
    const submittedEffects = yield* Ref.make<ReadonlyArray<PendingOrchestrationEffectV2>>([]);
    const committedEffects = yield* Ref.make<ReadonlyArray<PendingOrchestrationEffectV2>>([]);
    const ingestionDone = yield* Deferred.make<void>();
    const captureTurnItem = (payload: OrchestrationV2TurnItem) =>
      Ref.update(writtenItems, (current) => [...current, payload]);
    const captureFinalEvents = (events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
      Effect.gen(function* () {
        yield* Ref.update(finalEvents, (current) => [...current, ...events]);
        for (const event of events) {
          if (event.type === "turn-item.updated") {
            yield* captureTurnItem(event.payload);
          }
          if (event.type === "run.updated") {
            yield* Ref.update(observed, (current) => [...current, `run:${event.payload.status}`]);
          }
        }
      });
    const layerTest = RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.mock(McpAppModelContext.McpAppModelContext)({
            forThread: () => input.appContext ?? Effect.succeed([]),
          }),
          Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
          layerTurnItemPositionsTest,
          mockEventSink({
            write: (payload) =>
              Effect.gen(function* () {
                for (const event of payload.events) {
                  if (event.type === "turn-item.updated") {
                    yield* captureTurnItem(event.payload);
                  }
                }
                return [];
              }),
            writeWithEffects: (payload) =>
              Effect.gen(function* () {
                yield* Ref.update(submittedEffects, (current) => [...current, ...payload.effects]);
                yield* Ref.update(committedEffects, (current) => [...current, ...payload.effects]);
                yield* captureFinalEvents(payload.events);
                return [];
              }),
            writeIfRunCurrent: (payload) =>
              Effect.gen(function* () {
                const effects = payload.effects ?? [];
                yield* Ref.update(submittedEffects, (current) => [...current, ...effects]);
                if (
                  input.rejectTerminalWrite === true ||
                  (input.beforeTerminalWrite !== undefined &&
                    !(yield* input.beforeTerminalWrite(payload)))
                ) {
                  return { committed: false, storedEvents: [] };
                }
                yield* Ref.update(committedEffects, (current) => [...current, ...effects]);
                yield* captureFinalEvents(payload.events);
                return { committed: true, storedEvents: [] };
              }),
            // Committed only against the row that was read, as the real conditional writes are.
            writeIfSubagentUnchanged: (payload) =>
              stored.subagents.get(payload.expected.id) === payload.expected
                ? captureFinalEvents(payload.events).pipe(
                    Effect.as({ committed: true, storedEvents: [] }),
                  )
                : Effect.succeed({ committed: false, storedEvents: [] }),
            writeIfTurnItemUnchanged: (payload) =>
              stored.turnItems.get(payload.expected.id) === payload.expected
                ? captureFinalEvents(payload.events).pipe(
                    Effect.as({ committed: true, storedEvents: [] }),
                  )
                : Effect.succeed({ committed: false, storedEvents: [] }),
          }),
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
            ingestNormalized: (request) =>
              store(request.event).pipe(
                Effect.andThen(input.ingestNormalized?.(request) ?? Effect.succeed([])),
              ),
          }),
          ServerSettings.layerTest(),
          input.observer ??
            Layer.succeed(RunFinalizationService.RunFinalizationObserver, {
              refresh: () => Effect.void,
              refreshRepositories: () => Effect.void,
              refreshAfterTurn: () =>
                Ref.update(observed, (current) => [...current, "pull-requests-refreshed"]).pipe(
                  Effect.andThen(input.refreshAfterTurn ?? Effect.void),
                ),
            }),
        ),
      ),
      Layer.provideMerge(PreviewAgentActivity.layer),
    );

    const previewActivityEnded = yield* Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      const start = runExecution.startRootRun({
        commandId: CommandId.make(`command:${input.key}`),
        appThread: rootAppThread(ids.threadId),
        providerSessionId: ProviderSessionId.make(`session:${input.key}`),
        session: {
          driver,
          events: Stream.empty,
          subscribeEvents: Effect.succeed({
            events:
              input.events?.(ids) ??
              Stream.fromIterable([
                ...(input.seedOpenSubagent
                  ? [
                      { type: "subagent.updated", driver, subagent: runningSubagent } as const,
                      {
                        type: "node.updated",
                        driver,
                        node: makeRunOwnedSubagentNodeFixture({ ids, status: "running" }),
                      } as const,
                      {
                        type: "turn_item.updated",
                        driver,
                        turnItem: makeRunOwnedSubagentTurnItemFixture({
                          ids,
                          providerInstanceId,
                          childThreadId: ids.childThreadId,
                          driver,
                          status: "running",
                        }),
                      } as const,
                    ]
                  : []),
                rootTerminalEvent(ids, "interrupted"),
              ] satisfies ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>),
            close: Deferred.succeed(ingestionDone, undefined).pipe(
              Effect.andThen(input.onSubscriptionClose ?? Effect.void),
            ),
          }),
          startTurn: input.startTurn ?? (() => Effect.void),
          interruptTurn: input.interruptTurn ?? (() => Effect.void),
          hasPendingBackgroundWorkForThread: input.hasPendingBackgroundWorkForThread,
        } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
        run: {
          id: ids.runId,
          threadId: ids.threadId,
          ordinal: 1,
          providerInstanceId,
        } as OrchestrationV2Run,
        rootNode: {
          id: ids.rootNodeId,
          providerTurnId: ids.rootProviderTurnId,
        } as OrchestrationV2ExecutionNode,
        checkpointScope: {
          id: CheckpointScopeId.make(`checkpoint-scope:${input.key}`),
        } as OrchestrationV2CheckpointScope,
        providerThread: {
          id: ids.providerThreadId,
          driver,
        } as OrchestrationV2ProviderThread,
        attempt: {
          id: ids.attemptId,
          providerTurnId:
            input.initialProviderTurnId === undefined
              ? ids.rootProviderTurnId
              : input.initialProviderTurnId,
        } as OrchestrationV2RunAttempt,
        attemptId: ids.attemptId,
        providerTurnOrdinal: 1,
        shouldFinalizeRun: input.shouldFinalizeRun,
        loadUnsettledWork,
        ...(input.loadRunInterruptReason === undefined
          ? {}
          : { loadRunInterruptReason: input.loadRunInterruptReason }),
        ...(input.shouldStartProviderTurn === undefined
          ? {}
          : { shouldStartProviderTurn: input.shouldStartProviderTurn }),
        ...(input.hasUnpairedRunInterruptRequest === undefined
          ? {}
          : {
              hasUnpairedRunInterruptRequest: input.hasUnpairedRunInterruptRequest,
            }),
        ...(input.hasWorkspaceMoved === undefined
          ? {}
          : { hasWorkspaceMoved: input.hasWorkspaceMoved }),
        ...(input.loadDroppedInlineHandoffs === undefined
          ? {}
          : { loadDroppedInlineHandoffs: input.loadDroppedInlineHandoffs }),
        message: {
          messageId: MessageId.make(`message:${input.key}`),
          text: input.messageText ?? "interrupt projection",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
        modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: process.cwd(),
          approvalPolicy: "never",
          sandboxPolicy: {
            type: "readOnly",
            access: { type: "fullAccess" },
            networkAccess: false,
          },
        },
      });
      yield* start.pipe(
        Effect.catch((error) => input.onStartFailure?.(error) ?? Effect.fail(error)),
      );
      yield* input.afterStart?.(ingestionDone) ?? Effect.void;
      yield* Deferred.await(ingestionDone);
      const activity = yield* PreviewAgentActivity.PreviewAgentActivity;
      return !(yield* activity.active(ids.threadId));
    }).pipe(Effect.provide(layerTest));

    return {
      previewActivityEnded,
      written: yield* Ref.get(writtenItems),
      finalEvents: yield* Ref.get(finalEvents),
      observed: yield* Ref.get(observed),
      submittedEffects: yield* Ref.get(submittedEffects),
      committedEffects: yield* Ref.get(committedEffects),
    };
  });
}

interface BackgroundScenarioIds {
  readonly threadId: ThreadId;
  readonly childThreadId: ThreadId;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly providerThreadId: ProviderThreadId;
  readonly rootProviderTurnId: ProviderTurnId;
  readonly rootNodeId: NodeId;
  readonly itemId: TurnItemId;
  readonly childItemId: TurnItemId;
  readonly subagentNodeId: NodeId;
}

function backgroundScenarioIds(key: string): BackgroundScenarioIds {
  return {
    threadId: ThreadId.make(`thread:${key}`),
    childThreadId: ThreadId.make(`thread:${key}:child`),
    runId: RunId.make(`run:${key}`),
    attemptId: RunAttemptId.make(`attempt:${key}`),
    providerThreadId: ProviderThreadId.make(`provider-thread:${key}`),
    rootProviderTurnId: ProviderTurnId.make(`provider-turn:${key}`),
    rootNodeId: NodeId.make(`node:${key}`),
    itemId: TurnItemId.make(`turn-item:${key}`),
    childItemId: TurnItemId.make(`turn-item:${key}:child`),
    subagentNodeId: NodeId.make(`node:${key}:subagent`),
  };
}

function childThreadCreatedEvent(
  ids: BackgroundScenarioIds,
): ProviderAdapter.ProviderAdapterV2Event {
  return {
    type: "app_thread.created",
    driver,
    appThread: {
      id: ids.childThreadId,
      lineage: {
        parentThreadId: ids.threadId,
        relationshipToParent: "subagent",
        rootThreadId: ids.threadId,
      },
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
}

function childBackgroundTurnItemEvent(
  ids: BackgroundScenarioIds,
  status: "running" | "completed" | "idle",
  ordinal: number,
): ProviderAdapter.ProviderAdapterV2Event {
  return {
    type: "turn_item.updated",
    driver,
    turnItem: {
      id: ids.childItemId,
      threadId: ids.childThreadId,
      runId: null,
      providerTurnId: null,
      ordinal,
      type: "command_execution",
      status,
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
}

function backgroundTurnItemEvent(
  ids: BackgroundScenarioIds,
  type: "command_execution" | "dynamic_tool" | "subagent",
  status: "running" | "completed" | "idle",
  ordinal: number,
  itemId?: TurnItemId,
): ProviderAdapter.ProviderAdapterV2Event {
  return {
    type: "turn_item.updated",
    driver,
    turnItem: {
      id: itemId ?? ids.itemId,
      threadId: ids.threadId,
      runId: ids.runId,
      providerTurnId: ids.rootProviderTurnId,
      ordinal,
      type,
      status,
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
}

function backgroundTurnItemEventForRun(
  ids: BackgroundScenarioIds,
  runId: RunId,
  type: "command_execution" | "dynamic_tool" | "subagent",
  status: "running" | "completed" | "idle",
  ordinal: number,
): ProviderAdapter.ProviderAdapterV2Event {
  const event = backgroundTurnItemEvent(ids, type, status, ordinal);
  if (event.type !== "turn_item.updated") {
    return event;
  }
  return { ...event, turnItem: { ...event.turnItem, runId } };
}

function subagentEvent(
  ids: BackgroundScenarioIds,
  status: "running" | "completed" | "idle",
): ProviderAdapter.ProviderAdapterV2Event {
  return {
    type: "subagent.updated",
    driver,
    subagent: {
      id: ids.subagentNodeId,
      threadId: ids.threadId,
      runId: ids.runId,
      status,
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
}

function makeRunOwnedSubagentFixture(input: {
  readonly ids: BackgroundScenarioIds;
  readonly providerInstanceId: ProviderInstanceId;
  readonly childThreadId: ThreadId;
  readonly driver: typeof driver;
  readonly status: "running" | "interrupted";
}): OrchestrationV2Subagent {
  const now = DateTime.makeUnsafe("2026-07-21T12:00:00.000Z");
  return {
    id: input.ids.subagentNodeId,
    threadId: input.ids.threadId,
    runId: input.ids.runId,
    parentNodeId: input.ids.rootNodeId,
    origin: "provider_native",
    createdBy: "agent",
    driver: input.driver,
    providerInstanceId: input.providerInstanceId,
    providerThreadId: null,
    childThreadId: input.childThreadId,
    nativeTaskRef: {
      driver: input.driver,
      nativeId: `task:${input.ids.subagentNodeId}`,
      strength: "strong",
    },
    prompt: "hold",
    title: "Live-test subagent hold",
    model: null,
    status: input.status,
    result: null,
    startedAt: now,
    completedAt: null,
    updatedAt: now,
  };
}

function makeRunOwnedSubagentTurnItemFixture(input: {
  readonly ids: BackgroundScenarioIds;
  readonly providerInstanceId: ProviderInstanceId;
  readonly childThreadId: ThreadId;
  readonly driver: typeof driver;
  readonly status: "running" | "interrupted";
}): Extract<OrchestrationV2TurnItem, { type: "subagent" }> {
  const now = DateTime.makeUnsafe("2026-07-21T12:00:00.000Z");
  return {
    id: input.ids.itemId,
    threadId: input.ids.threadId,
    runId: input.ids.runId,
    nodeId: input.ids.subagentNodeId,
    providerThreadId: input.ids.providerThreadId,
    providerTurnId: input.ids.rootProviderTurnId,
    nativeItemRef: {
      driver: input.driver,
      nativeId: `task:${input.ids.subagentNodeId}`,
      strength: "strong",
    },
    parentItemId: null,
    ordinal: 3,
    status: input.status,
    title: "Live-test subagent hold",
    startedAt: now,
    completedAt: null,
    updatedAt: now,
    type: "subagent",
    subagentId: input.ids.subagentNodeId,
    origin: "provider_native",
    driver: input.driver,
    providerInstanceId: input.providerInstanceId,
    childThreadId: input.childThreadId,
    prompt: "hold",
    result: null,
  };
}

function makeLinkedChildTurnItemFixture(input: {
  readonly ids: BackgroundScenarioIds;
  readonly driver: typeof driver;
  readonly type: "assistant_message" | "command_execution" | "reasoning";
}): OrchestrationV2TurnItem {
  const now = DateTime.makeUnsafe("2026-07-21T12:00:00.000Z");
  const base = {
    id: input.ids.childItemId,
    threadId: input.ids.childThreadId,
    runId: null,
    nodeId: NodeId.make(`${input.ids.subagentNodeId}:child-root`),
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 1,
    status: "running" as const,
    title: "Working",
    startedAt: now,
    completedAt: null,
    updatedAt: now,
  };
  if (input.type === "assistant_message") {
    return {
      ...base,
      type: "assistant_message",
      messageId: MessageId.make(`${input.ids.childItemId}:message`),
      text: "partial response",
      streaming: true,
    };
  }
  if (input.type === "reasoning") {
    return {
      ...base,
      type: "reasoning",
      text: "partial progress",
      streaming: true,
    };
  }
  return {
    ...base,
    type: "command_execution",
    input: "sleep 300",
  };
}

function makeRunOwnedSubagentNodeFixture(input: {
  readonly ids: BackgroundScenarioIds;
  readonly status: "running" | "interrupted";
}): OrchestrationV2ExecutionNode {
  const now = DateTime.makeUnsafe("2026-07-21T12:00:00.000Z");
  return {
    id: input.ids.subagentNodeId,
    threadId: input.ids.threadId,
    runId: input.ids.runId,
    parentNodeId: input.ids.rootNodeId,
    rootNodeId: input.ids.rootNodeId,
    kind: "subagent",
    status: input.status,
    countsForRun: false,
    providerThreadId: input.ids.providerThreadId,
    providerTurnId: input.ids.rootProviderTurnId,
    nativeItemRef: {
      driver,
      nativeId: `task:${input.ids.subagentNodeId}`,
      strength: "strong",
    },
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: null,
  };
}

function makeRunOwnedSubagentChildNodeFixture(input: {
  readonly ids: BackgroundScenarioIds;
  readonly status: "running" | "interrupted";
}): OrchestrationV2ExecutionNode {
  const now = DateTime.makeUnsafe("2026-07-21T12:00:00.000Z");
  return {
    id: NodeId.make(`${input.ids.subagentNodeId}:child-root`),
    threadId: input.ids.childThreadId,
    runId: null,
    parentNodeId: null,
    rootNodeId: NodeId.make(`${input.ids.subagentNodeId}:child-root`),
    kind: "root_turn",
    status: input.status,
    countsForRun: false,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: now,
    completedAt: null,
  };
}

function rootTerminalEvent(
  ids: BackgroundScenarioIds,
  status: "completed" | "interrupted" | "cancelled" | "failed",
): ProviderAdapter.ProviderAdapterV2Event {
  const common = {
    type: "turn.terminal" as const,
    driver,
    providerThreadId: ids.providerThreadId,
    providerTurnId: ids.rootProviderTurnId,
    runOrdinal: 1,
    threadDisposition: "reusable" as const,
  };
  return status === "failed"
    ? {
        ...common,
        status,
        failureItemOrdinal: 101,
        failure: {
          class: "provider_error",
          message: "Provider failed",
          code: null,
          retryable: null,
        },
      }
    : { ...common, status, failure: null };
}

function runBackgroundItemScenario(
  key: string,
  makeEvents: (
    ids: BackgroundScenarioIds,
  ) =>
    | ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>
    | Stream.Stream<ProviderAdapter.ProviderAdapterV2Event>,
  options?: {
    readonly onIngest?: (
      event: ProviderAdapter.ProviderAdapterV2Event,
      write: { readonly inSharedCommit: boolean },
    ) => Effect.Effect<void>;
    // Consumes through the real commit coordinator, as the event sink does.
    readonly batched?: boolean;
    readonly keepEventStreamOpen?: boolean;
    readonly hasPendingBackgroundWorkForThread?: (
      providerThread: OrchestrationV2ProviderThread,
    ) => Effect.Effect<boolean>;
    readonly loadInheritedBackgroundTurnItems?: () => Effect.Effect<
      ReadonlyArray<{ readonly id: TurnItemId; readonly runId: RunId }>
    >;
    readonly onSubscribe?: Effect.Effect<void>;
    readonly onEventsConsumed?: Effect.Effect<void>;
    // Owner-gated root snapshots store nothing, as after a newer run claims the thread.
    readonly providerThreadOwnerLost?: boolean;
    // That newer run has not attached yet, so this run's root snapshots still set the roster.
    readonly claimantUnattached?: boolean;
    // The unattached claimant's stored roster differs, so the correction writes it.
    readonly rosterCorrected?: boolean;
    readonly afterEvents?: (
      activity: PreviewAgentActivity.PreviewAgentActivity["Service"],
    ) => Effect.Effect<void>;
    readonly onFinalized?: (
      events: ReadonlyArray<OrchestrationV2DomainEvent>,
    ) => Effect.Effect<void>;
    readonly shouldStartProviderTurn?: RunExecutionService.RunExecutionServiceV2StartRootRunInput["shouldStartProviderTurn"];
    // A commit coordinator shared with other runs, instead of the run's own.
    readonly coordinator?: Effect.Success<ReturnType<typeof ProviderCommitBatch.make>>;
    // Other runs on the shared writer, for the run's whole life (needs `batched`).
    readonly otherWriters?: (
      coordinator: Effect.Success<ReturnType<typeof ProviderCommitBatch.make>>,
    ) => Effect.Effect<void>;
    // Sees each held text's display-position reservation.
    readonly onPositionReserved?: (turnItemId: TurnItemId) => Effect.Effect<void>;
    // Sees the size of each chunk ingestion hands to the commit coordinator (needs `batched`).
    readonly onIngestionChunk?: (size: number) => Effect.Effect<void>;
  },
) {
  return Effect.gen(function* () {
    const ids = backgroundScenarioIds(key);
    const providerInstanceId = ProviderInstanceId.make("codex");
    const observed = yield* Ref.make<ReadonlyArray<string>>([]);
    const ingestionDone = yield* Deferred.make<void>();
    const previewActivity = yield* PreviewAgentActivity.PreviewAgentActivity.pipe(
      Effect.provide(PreviewAgentActivity.layer),
    );
    let inSharedCommit = false;
    const coordinator =
      options?.coordinator ??
      (options?.batched === true
        ? yield* ProviderCommitBatch.make(
            (work) =>
              Effect.suspend(() => {
                inSharedCommit = true;
                return work;
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    inSharedCommit = false;
                  }),
                ),
              ),
            true,
          )
        : undefined);
    if (coordinator !== undefined && options?.otherWriters !== undefined)
      yield* options.otherWriters(coordinator).pipe(Effect.forkChild({ startImmediately: true }));
    const layerTest = RunExecutionService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          McpAppModelContext.layerEmpty,
          Layer.succeed(PreviewAgentActivity.PreviewAgentActivity, previewActivity),
          Layer.mock(CheckpointService.CheckpointServiceV2)({ captureBaseline: () => Effect.void }),
          options?.onPositionReserved === undefined
            ? layerTurnItemPositionsTest
            : Layer.mock(TurnItemPositionStore.TurnItemPositionStoreV2)({
                allocate: ({ turnItemId }) =>
                  options.onPositionReserved!(turnItemId).pipe(Effect.as(1)),
              }),
          mockEventSink({
            ...(coordinator === undefined
              ? {}
              : {
                  consumeProviderEvents: (source, process, after, canBatch) =>
                    coordinator.consume(
                      options?.onIngestionChunk === undefined
                        ? source
                        : source.pipe(
                            Stream.mapArrayEffect((chunk) =>
                              options.onIngestionChunk!(chunk.length).pipe(Effect.as(chunk)),
                            ),
                          ),
                      process,
                      after,
                      canBatch,
                    ),
                }),
            write: () => Effect.succeed([]),
            writeWithEffects: (input) =>
              Effect.gen(function* () {
                if (
                  input.events.some(
                    (event) => event.type === "run.updated" && event.runId === ids.runId,
                  )
                ) {
                  yield* Ref.update(observed, (current) => [...current, "root-finalized"]);
                  yield* options?.onFinalized?.(input.events) ?? Effect.void;
                }
                return [];
              }),
            writeIfRunCurrent: () => Effect.succeed({ committed: true, storedEvents: [] }),
            ...(options?.claimantUnattached === true
              ? {
                  writeProviderThreadRosterIfUnattached: () =>
                    Effect.succeed({
                      unattached: true,
                      storedEvents:
                        options.rosterCorrected === true ? [{} as OrchestrationV2StoredEvent] : [],
                    }),
                }
              : {}),
          }),
          IdAllocator.layer,
          ThreadCommandExecutor.layer,
          Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
            ingestNormalized: (input) => {
              const write = Effect.gen(function* () {
                const event = input.event;
                yield* options?.onIngest?.(event, { inSharedCommit }) ?? Effect.void;
                if (event.type === "turn_item.updated") {
                  yield* Ref.update(observed, (current) => [
                    ...current,
                    `turn_item:${event.turnItem.status}`,
                  ]);
                }
                if (event.type === "subagent.updated") {
                  yield* Ref.update(observed, (current) => [
                    ...current,
                    `subagent:${event.subagent.status}`,
                  ]);
                }
                // Only the count of stored events matters to the run.
                return event.type === "provider_thread.updated" &&
                  !(
                    options?.providerThreadOwnerLost === true &&
                    input.writeIfProviderThreadOwner !== undefined
                  )
                  ? [{} as OrchestrationV2StoredEvent]
                  : [];
              });
              // A write outside a batch waits for queued snapshots, as the event sink's does.
              return coordinator === undefined ? write : coordinator.barrier(write);
            },
          }),
          ServerSettings.layerTest(),
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
      yield* runExecution.startRootRun({
        commandId: CommandId.make(`command:${key}`),
        appThread: rootAppThread(ids.threadId),
        providerSessionId: ProviderSessionId.make(`session:${key}`),
        session: {
          events: Stream.empty,
          ...(options?.hasPendingBackgroundWorkForThread === undefined
            ? {}
            : { hasPendingBackgroundWorkForThread: options.hasPendingBackgroundWorkForThread }),
          subscribeEvents: Effect.gen(function* () {
            yield* options?.onSubscribe ?? Effect.void;
            const source = makeEvents(ids);
            const events = (Stream.isStream(source) ? source : Stream.fromIterable(source)).pipe(
              Stream.concat(
                Stream.fromEffect(
                  Effect.andThen(
                    options?.onEventsConsumed ?? Effect.void,
                    options?.afterEvents?.(previewActivity) ?? Effect.void,
                  ),
                ).pipe(Stream.drain),
              ),
            );
            return {
              events:
                options?.keepEventStreamOpen === true
                  ? events.pipe(Stream.concat(Stream.never))
                  : events,
              close: Deferred.succeed(ingestionDone, undefined),
            };
          }),
          startTurn: () => Effect.void,
        } as unknown as ProviderAdapter.ProviderAdapterV2SessionRuntime,
        run: {
          id: ids.runId,
          threadId: ids.threadId,
          ordinal: 1,
          providerInstanceId,
        } as OrchestrationV2Run,
        rootNode: { id: ids.rootNodeId } as OrchestrationV2ExecutionNode,
        checkpointScope: {
          id: CheckpointScopeId.make(`checkpoint-scope:${key}`),
        } as OrchestrationV2CheckpointScope,
        providerThread: {
          id: ids.providerThreadId,
          driver,
        } as OrchestrationV2ProviderThread,
        attempt: {
          id: ids.attemptId,
          providerTurnId: ids.rootProviderTurnId,
        } as OrchestrationV2RunAttempt,
        attemptId: ids.attemptId,
        ...(options?.shouldStartProviderTurn === undefined
          ? {}
          : { shouldStartProviderTurn: options.shouldStartProviderTurn }),
        ...(options?.loadInheritedBackgroundTurnItems === undefined
          ? {}
          : {
              loadInheritedBackgroundTurnItems: options.loadInheritedBackgroundTurnItems,
            }),
        providerTurnOrdinal: 1,
        message: {
          messageId: MessageId.make(`message:${key}:user`),
          text: "Start a background item and finish.",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
        modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
        runtimePolicy: {
          runtimeMode: "full-access",
          interactionMode: "default",
          cwd: process.cwd(),
          approvalPolicy: "never",
          sandboxPolicy: {
            type: "readOnly",
            access: { type: "fullAccess" },
            networkAccess: false,
          },
        },
      });
    }).pipe(Effect.provide(layerTest));

    // No timeout: tests advance the TestClock, which would fire it early.
    yield* Deferred.await(ingestionDone);

    return yield* Ref.get(observed);
  });
}

it.effect("keeps a failed root's still-running native children live while the run settles", () =>
  Effect.gen(function* () {
    const finalized = yield* Ref.make<ReadonlyArray<OrchestrationV2DomainEvent>>([]);
    const childTurn = (
      ids: BackgroundScenarioIds,
      status: "running" | "completed",
    ): ProviderAdapter.ProviderAdapterV2Event =>
      ({
        type: "provider_turn.updated",
        driver,
        threadId: ids.childThreadId,
        providerTurn: {
          id: ProviderTurnId.make(`${ids.rootProviderTurnId}:child`),
          providerThreadId: ProviderThreadId.make(`${ids.providerThreadId}:child`),
          nodeId: NodeId.make(`${ids.subagentNodeId}:child-root`),
          runAttemptId: null,
          status,
        },
      }) as ProviderAdapter.ProviderAdapterV2Event;
    const observed = yield* runBackgroundItemScenario(
      "failed-root-live-child",
      (ids) => [
        childThreadCreatedEvent(ids),
        subagentEvent(ids, "running"),
        childTurn(ids, "running"),
        { ...rootTerminalEvent(ids, "failed"), childTurnsContinue: true },
        subagentEvent(ids, "completed"),
        childTurn(ids, "completed"),
      ],
      {
        keepEventStreamOpen: true,
        onFinalized: (events) => Ref.set(finalized, events),
      },
    );

    assert.deepEqual(observed, ["subagent:running", "root-finalized", "subagent:completed"]);
    const written = yield* Ref.get(finalized);
    assert.deepEqual(
      written.flatMap((event) => (event.type === "run.updated" ? [event.payload.status] : [])),
      ["failed"],
    );
    assert.isFalse(
      written.some((event) => event.type === "subagent.updated"),
      "a child that outlives the failed root must not be settled with it",
    );
  }),
);

it.effect.each([
  { name: "on time", wakesEarly: false },
  // Timers run on the monotonic clock, so one can fire while the wall clock still reads a
  // millisecond before its deadline.
  { name: "before the wall clock reaches the deadline", wakesEarly: true },
])("delivers held text while an open-prompt provider idles, waking $name", ({ wakesEarly }) =>
  Effect.gen(function* () {
    const key = `idle-paragraph-${wakesEarly}`;
    const ids = backgroundScenarioIds(key);
    const now = yield* DateTime.now;
    const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const delivered = yield* Ref.make<ReadonlyArray<string>>([]);
    const paragraphDelivered = yield* Deferred.make<void>();
    const nextParagraphDelivered = yield* Deferred.make<void>();
    const clock = yield* Clock.Clock;
    const assistantText = (text: string, streaming: boolean) =>
      [
        {
          type: "message.updated",
          driver,
          message: {
            createdBy: "agent",
            creationSource: "provider",
            id: MessageId.make(`message:${key}`),
            threadId: ids.threadId,
            runId: ids.runId,
            nodeId: ids.rootNodeId,
            role: "assistant",
            text,
            attachments: [],
            streaming,
            createdAt: now,
            updatedAt: now,
          },
        },
        {
          type: "turn_item.updated",
          driver,
          turnItem: {
            ...makeLinkedChildTurnItemFixture({ ids, driver, type: "assistant_message" }),
            id: ids.itemId,
            threadId: ids.threadId,
            runId: ids.runId,
            nodeId: ids.rootNodeId,
            providerTurnId: ids.rootProviderTurnId,
            text,
            streaming,
          },
        },
      ] as ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>;

    const blockerStarted = yield* Deferred.make<void>();
    const releaseBlocker = yield* Deferred.make<void>();
    const blocker = streamedMessageEvent({
      id: `message:${key}:blocker`,
      threadId: ids.threadId,
      runId: ids.runId,
      role: "assistant",
      text: "Other",
      streaming: false,
    });
    const scenario = yield* runBackgroundItemScenario(key, () => Stream.fromQueue(providerEvents), {
      batched: true,
      onIngest: (event, write) =>
        event.type === "message.updated" && event.message.text === "Other"
          ? Deferred.succeed(blockerStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseBlocker)),
            )
          : event.type === "message.updated" || event.type === "turn_item.updated"
            ? Effect.gen(function* () {
                const message = event.type === "message.updated" ? event.message : event.turnItem;
                const text = "text" in message ? message.text : "";
                const streaming = "streaming" in message && message.streaming;
                yield* Ref.update(delivered, (current) => [
                  ...current,
                  `${event.type}:${streaming ? "streaming" : "final"}:${JSON.stringify(text)}${
                    write.inSharedCommit ? " in commit" : ""
                  }`,
                ]);
                if (event.type === "turn_item.updated") {
                  yield* Deferred.succeed(paragraphDelivered, undefined);
                  if (text === "First.\n\nWaiting on the build.\n\n")
                    yield* Deferred.succeed(nextParagraphDelivered, undefined);
                }
              })
            : Effect.void,
    }).pipe(
      Effect.provideService(Clock.Clock, {
        ...clock,
        sleep: (duration) =>
          clock.sleep(wakesEarly ? Duration.subtract(duration, Duration.millis(1)) : duration),
      }),
      Effect.forkChild,
    );
    // Waking a sleep runs ingestion until it suspends, so an idle write has landed by now.
    const lastShown = Ref.get(delivered).pipe(Effect.map((shown) => shown.at(-1)));

    yield* Queue.offerAll(providerEvents, assistantText("First.\n\nWaiting on the build", true));
    yield* Deferred.await(paragraphDelivered);
    // The provider keeps its prompt open while a backgrounded command runs.
    yield* TestClock.adjust("2 seconds");
    assert.strictEqual(
      yield* lastShown,
      'turn_item.updated:streaming:"First.\\n\\nWaiting on the build"',
    );
    // The next paragraph's tail still flushes on a later pause.
    yield* Queue.offerAll(
      providerEvents,
      assistantText("First.\n\nWaiting on the build.\n\nRunning", true),
    );
    yield* Deferred.await(nextParagraphDelivered);
    yield* TestClock.adjust("1 second");
    assert.strictEqual(
      yield* lastShown,
      'turn_item.updated:streaming:"First.\\n\\nWaiting on the build.\\n\\nRunning"',
    );
    yield* Queue.offerAll(providerEvents, [
      ...assistantText("First.\n\nWaiting on the build.\n\nRunning tests", true),
      blocker,
      ...assistantText("First.\n\nWaiting on the build.\n\nRunning tests passed.", false),
    ]);
    yield* Deferred.await(blockerStarted);
    // The appended text comes due while its final snapshots wait behind the blocked write.
    yield* TestClock.adjust("1 second");
    yield* Deferred.succeed(releaseBlocker, undefined);
    yield* Queue.offer(providerEvents, rootTerminalEvent(ids, "completed"));
    yield* Fiber.join(scenario);

    // Snapshots share the batch commit; the idle write commits alone, before it counts as shown.
    // The final snapshots win over the stale append that came due behind them.
    assert.deepEqual(yield* Ref.get(delivered), [
      `message.updated:streaming:"First.\\n\\n" in commit`,
      `turn_item.updated:streaming:"First.\\n\\n" in commit`,
      'message.updated:streaming:"First.\\n\\nWaiting on the build"',
      'turn_item.updated:streaming:"First.\\n\\nWaiting on the build"',
      `message.updated:streaming:"First.\\n\\nWaiting on the build.\\n\\n" in commit`,
      `turn_item.updated:streaming:"First.\\n\\nWaiting on the build.\\n\\n" in commit`,
      'message.updated:streaming:"First.\\n\\nWaiting on the build.\\n\\nRunning"',
      'turn_item.updated:streaming:"First.\\n\\nWaiting on the build.\\n\\nRunning"',
      `message.updated:final:"First.\\n\\nWaiting on the build.\\n\\nRunning tests passed." in commit`,
      `turn_item.updated:final:"First.\\n\\nWaiting on the build.\\n\\nRunning tests passed." in commit`,
    ]);
  }),
);

it.effect("commits a running turn's usage reports with the snapshots around them", () =>
  Effect.gen(function* () {
    const ingested: Array<string> = [];
    const rootTurn = (
      ids: BackgroundScenarioIds,
      status: "running" | "completed",
    ): ProviderAdapter.ProviderAdapterV2Event =>
      ({
        type: "provider_turn.updated",
        driver,
        providerTurn: {
          id: ids.rootProviderTurnId,
          providerThreadId: ids.providerThreadId,
          nodeId: ids.rootNodeId,
          runAttemptId: ids.attemptId,
          status,
        },
      }) as ProviderAdapter.ProviderAdapterV2Event;
    // A control between the reports keeps each its own state (in one backlog the newest report
    // takes the others' place; see below).
    const control = (ids: BackgroundScenarioIds) =>
      ({
        type: "provider_thread.updated",
        driver,
        providerThread: { id: ids.providerThreadId, driver, status: "active" },
      }) as ProviderAdapter.ProviderAdapterV2Event;
    yield* runBackgroundItemScenario(
      "usage-reports-batch",
      (ids) => [
        rootTurn(ids, "running"),
        backgroundTurnItemEvent(ids, "dynamic_tool", "completed", 1),
        control(ids),
        rootTurn(ids, "running"),
        backgroundTurnItemEvent(ids, "dynamic_tool", "completed", 2),
        control(ids),
        rootTurn(ids, "running"),
        rootTurn(ids, "completed"),
        rootTerminalEvent(ids, "completed"),
      ],
      {
        batched: true,
        shouldStartProviderTurn: () => Effect.succeed(true),
        onIngest: (event, write) =>
          Effect.sync(() => {
            if (event.type === "provider_turn.updated")
              ingested.push(
                `${event.providerTurn.status}${write.inSharedCommit ? " in commit" : ""}`,
              );
          }),
      },
    );
    // A Claude turn reports usage about twice per API request; each report ran alone and held
    // the run's later items behind a full pass of the shared writer. The first report rechecks
    // ownership under the thread lock, and a settled turn's report ends the turn, so both stay alone.
    assert.deepEqual(ingested, ["running", "running in commit", "running in commit", "completed"]);
  }),
);

it.effect(
  "ingests one backlog's running usage reports as the newest, in the first one's place",
  () =>
    Effect.gen(function* () {
      const ingested: Array<string> = [];
      const rootTurn = (
        ids: BackgroundScenarioIds,
        status: "running" | "completed",
        usedTokens?: number,
      ): ProviderAdapter.ProviderAdapterV2Event =>
        ({
          type: "provider_turn.updated",
          driver,
          providerTurn: {
            id: ids.rootProviderTurnId,
            providerThreadId: ids.providerThreadId,
            nodeId: ids.rootNodeId,
            runAttemptId: ids.attemptId,
            status,
            ...(usedTokens === undefined ? {} : { tokenUsage: { usedTokens } }),
          },
        }) as ProviderAdapter.ProviderAdapterV2Event;
      yield* runBackgroundItemScenario(
        "usage-reports-coalesced",
        (ids) => [
          rootTurn(ids, "running", 1),
          backgroundTurnItemEvent(ids, "dynamic_tool", "completed", 1),
          rootTurn(ids, "running", 2),
          backgroundTurnItemEvent(ids, "dynamic_tool", "completed", 2),
          rootTurn(ids, "running", 3),
          rootTurn(ids, "completed"),
          rootTerminalEvent(ids, "completed"),
        ],
        {
          batched: true,
          shouldStartProviderTurn: () => Effect.succeed(true),
          onIngest: (event, write) =>
            Effect.sync(() => {
              if (event.type === "provider_turn.updated") {
                const usage = (event.providerTurn as { tokenUsage?: { usedTokens: number } })
                  .tokenUsage;
                ingested.push(
                  `${event.providerTurn.status}${usage === undefined ? "" : `:${usage.usedTokens}`}${write.inSharedCommit ? " in commit" : ""}`,
                );
              }
            }),
        },
      );
      // The newest report is the root turn's first ingested one, so it still rechecks ownership
      // alone; the turn's end stays a control.
      assert.deepEqual(ingested, ["running:3", "completed"]);
    }),
);

it.effect.each([0, 1, 3, 10])(
  "ends a run a few writer passes after its turn's events arrive, with %s API requests",
  (requests) =>
    Effect.gen(function* () {
      // Eight other runs each hold the shared writer for 120 ms per write, so a pass is about a second,
      // as on a saturated server. The turn arrives in one burst, as a replayed Claude wake does.
      let otherWrites = 0;
      let writesAtBurst = 0;
      const saturated = yield* Deferred.make<void>();
      const writesUntilFinalized = yield* Deferred.make<number>();
      const write = Effect.sleep("120 millis").pipe(
        Effect.andThen(
          Effect.sync(() => {
            otherWrites++;
          }),
        ),
      );
      const rootTurn = (
        ids: BackgroundScenarioIds,
        status: "running" | "completed",
      ): ProviderAdapter.ProviderAdapterV2Event =>
        ({
          type: "provider_turn.updated",
          driver,
          providerTurn: {
            id: ids.rootProviderTurnId,
            providerThreadId: ids.providerThreadId,
            nodeId: ids.rootNodeId,
            runAttemptId: ids.attemptId,
            status,
          },
        }) as ProviderAdapter.ProviderAdapterV2Event;
      yield* runBackgroundItemScenario(
        `run-end-passes-${requests}`,
        (ids) =>
          Stream.fromEffect(
            Deferred.await(saturated).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  writesAtBurst = otherWrites;
                }),
              ),
            ),
          ).pipe(
            Stream.drain,
            Stream.concat(
              Stream.fromIterable([
                rootTurn(ids, "running"),
                // Claude reports usage at each API request's start and end, around its items.
                ...Array.from({ length: requests }, (_, request) => [
                  rootTurn(ids, "running"),
                  backgroundTurnItemEvent(ids, "dynamic_tool", "running", request * 10 + 1),
                  rootTurn(ids, "running"),
                  backgroundTurnItemEvent(ids, "dynamic_tool", "completed", request * 10 + 2),
                ]).flat(),
                rootTurn(ids, "completed"),
                {
                  type: "provider_thread.updated",
                  driver,
                  providerThread: { id: ids.providerThreadId, driver, status: "active" },
                } as ProviderAdapter.ProviderAdapterV2Event,
                rootTerminalEvent(ids, "completed"),
              ]),
            ),
          ),
        {
          batched: true,
          shouldStartProviderTurn: () => Effect.succeed(true),
          otherWriters: (coordinator) =>
            Effect.gen(function* () {
              // Writers queue only behind work in flight, so one snapshot holds the writer first.
              const holding = yield* Deferred.make<void>();
              yield* coordinator
                .consume(
                  Stream.make(
                    backgroundTurnItemEvent(
                      backgroundScenarioIds("other"),
                      "dynamic_tool",
                      "running",
                      1,
                    ),
                  ),
                  () => Deferred.succeed(holding, undefined).pipe(Effect.andThen(write)),
                  Effect.succeed(false),
                )
                .pipe(Effect.forkChild);
              yield* Deferred.await(holding);
              for (let writer = 0; writer < 8; writer++)
                yield* coordinator.barrier(write).pipe(Effect.forever, Effect.forkChild);
              yield* Deferred.succeed(saturated, undefined);
              return yield* Effect.never;
            }),
          onFinalized: () =>
            Deferred.succeed(writesUntilFinalized, otherWrites - writesAtBurst).pipe(Effect.asVoid),
        },
      ).pipe(Effect.forkChild);
      yield* Deferred.await(saturated);
      yield* TestClock.adjust("2 minutes");
      // Each item outside a batch queued twice, and each usage report held the run's later items
      // behind a pass, so the writes ahead of the finalize grew with the requests. Now they stay
      // flat, whatever the turn's length.
      assert.isAtMost(yield* Deferred.await(writesUntilFinalized), 55);
    }),
);

it.effect("keeps every run on a shared provider session up with its own turn", () =>
  Effect.gen(function* () {
    // Codex runs every thread of an instance on one provider session, so each run's consumer
    // receives every thread's events and drops the others'. Eight runs share it while eight other
    // writers keep the shared writer busy (120 ms a write, a pass of about a second). Every 10 s each
    // thread reports usage, writes two items and reports usage again.
    const runs = 8;
    const coordinator = yield* ProviderCommitBatch.make((work) => work, true);
    const write = Effect.sleep("120 millis");
    const holding = yield* Deferred.make<void>();
    yield* coordinator
      .consume(
        Stream.make(
          backgroundTurnItemEvent(backgroundScenarioIds("other"), "dynamic_tool", "running", 1),
        ),
        () => Deferred.succeed(holding, undefined).pipe(Effect.andThen(write)),
        Effect.succeed(false),
      )
      .pipe(Effect.forkChild);
    yield* Deferred.await(holding);
    for (let writer = 0; writer < 8; writer++)
      yield* coordinator.barrier(write).pipe(Effect.forever, Effect.forkChild);
    const ids = Array.from({ length: runs }, (_, run) =>
      backgroundScenarioIds(`shared-session-${run}`),
    );
    const rootTurn = (
      own: BackgroundScenarioIds,
      status: "running" | "completed",
    ): ProviderAdapter.ProviderAdapterV2Event =>
      ({
        type: "provider_turn.updated",
        driver,
        providerTurn: {
          id: own.rootProviderTurnId,
          providerThreadId: own.providerThreadId,
          nodeId: own.rootNodeId,
          runAttemptId: own.attemptId,
          status,
        },
      }) as ProviderAdapter.ProviderAdapterV2Event;
    const subscriptions = yield* Effect.forEach(ids, () =>
      Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>(),
    );
    // The last round of its own items each run has ingested.
    const ingestedRound = Array.from({ length: runs }, () => 0);
    for (let run = 0; run < runs; run++) {
      yield* Queue.offer(subscriptions[run]!, rootTurn(ids[run]!, "running"));
      yield* runBackgroundItemScenario(
        `shared-session-${run}`,
        () => Stream.fromQueue(subscriptions[run]!),
        {
          coordinator,
          shouldStartProviderTurn: () => Effect.succeed(true),
          onIngest: (event) =>
            Effect.sync(() => {
              if (event.type === "turn_item.updated")
                ingestedRound[run] = Math.max(
                  ingestedRound[run]!,
                  Math.floor(event.turnItem.ordinal / 10),
                );
            }),
        },
      ).pipe(Effect.forkChild);
    }
    let round = 0;
    yield* Effect.gen(function* () {
      for (;;) {
        yield* Effect.sleep("10 seconds");
        round++;
        for (const own of ids)
          for (const event of [
            rootTurn(own, "running"),
            backgroundTurnItemEvent(own, "dynamic_tool", "running", round * 10 + 1),
            backgroundTurnItemEvent(own, "dynamic_tool", "completed", round * 10 + 2),
            rootTurn(own, "running"),
          ])
            for (const subscription of subscriptions) yield* Queue.offer(subscription, event);
      }
    }).pipe(Effect.forkChild);
    yield* TestClock.adjust("5 minutes");
    // Before this, all nine fell behind together, 14 rounds (140 s) after 5 minutes and growing:
    // each other thread's usage report cost every run a writer pass that wrote nothing. Now one.
    assert.deepEqual(
      ingestedRound.map((ingested) => round - ingested <= 2),
      ingestedRound.map(() => true),
    );
  }),
);

it.effect.each([{ anotherRun: false }, { anotherRun: true }])(
  "ingests a backlog's newest streamed text once, keeping finals and every other event in order (another run streaming: $anotherRun)",
  ({ anotherRun }) =>
    Effect.gen(function* () {
      const key = "streamed-backlog";
      const ids = backgroundScenarioIds(key);
      const now = yield* DateTime.now;
      const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const ingested = yield* Ref.make<ReadonlyArray<string>>([]);
      const firstDelivered = yield* Deferred.make<void>();
      const blockerStarted = yield* Deferred.make<void>();
      const releaseBlocker = yield* Deferred.make<void>();
      const assistantText = (text: string, streaming: boolean) =>
        [
          {
            type: "message.updated",
            driver,
            message: {
              createdBy: "agent",
              creationSource: "provider",
              id: MessageId.make(`message:${key}`),
              threadId: ids.threadId,
              runId: ids.runId,
              nodeId: ids.rootNodeId,
              role: "assistant",
              text,
              attachments: [],
              streaming,
              createdAt: now,
              updatedAt: now,
            },
          },
          {
            type: "turn_item.updated",
            driver,
            turnItem: {
              ...makeLinkedChildTurnItemFixture({ ids, driver, type: "assistant_message" }),
              id: ids.itemId,
              threadId: ids.threadId,
              runId: ids.runId,
              nodeId: ids.rootNodeId,
              providerTurnId: ids.rootProviderTurnId,
              text,
              streaming,
            },
          },
        ] as ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>;
      const rootTurn = (status: "running" | "completed") =>
        ({
          type: "provider_turn.updated",
          driver,
          providerTurn: {
            id: ids.rootProviderTurnId,
            providerThreadId: ids.providerThreadId,
            nodeId: ids.rootNodeId,
            runAttemptId: ids.attemptId,
            status,
          },
        }) as ProviderAdapter.ProviderAdapterV2Event;
      const toolItemId = TurnItemId.make(`turn-item:${key}:tool`);
      // On a shared session another run's text arrives between every event of this run's.
      const other = backgroundScenarioIds(`${key}:another-run`);
      const withAnotherRun = (events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) =>
        anotherRun
          ? events.flatMap((event) => [
              {
                type: "node.updated",
                driver,
                node: {
                  id: other.rootNodeId,
                  threadId: other.threadId,
                  runId: other.runId,
                  kind: "assistant_message",
                  status: "running",
                  providerThreadId: other.providerThreadId,
                },
              } as ProviderAdapter.ProviderAdapterV2Event,
              event,
            ])
          : events;
      const scenario = yield* runBackgroundItemScenario(
        key,
        () => Stream.fromQueue(providerEvents),
        {
          batched: true,
          onIngest: (event, write) =>
            Effect.gen(function* () {
              const commit = write.inSharedCommit ? " in commit" : "";
              if (event.type === "provider_thread.updated") {
                yield* Ref.update(ingested, (current) => [...current, `provider_thread${commit}`]);
                yield* Deferred.succeed(blockerStarted, undefined);
                return yield* Deferred.await(releaseBlocker);
              }
              if (
                event.type === "message.updated" ||
                (event.type === "turn_item.updated" && event.turnItem.type === "assistant_message")
              ) {
                const message = event.type === "message.updated" ? event.message : event.turnItem;
                const text = "text" in message ? message.text : "";
                const streaming = "streaming" in message && message.streaming;
                yield* Ref.update(ingested, (current) => [
                  ...current,
                  `${event.type}:${streaming ? "streaming" : "final"}:${JSON.stringify(text)}${commit}`,
                ]);
                if (event.type === "turn_item.updated")
                  yield* Deferred.succeed(firstDelivered, undefined);
                return;
              }
              yield* Ref.update(ingested, (current) => [
                ...current,
                `${
                  event.type === "turn_item.updated"
                    ? `tool:${event.turnItem.status}`
                    : event.type === "provider_turn.updated"
                      ? `provider_turn:${event.providerTurn.status}`
                      : event.type
                }${commit}`,
              ]);
            }),
        },
      ).pipe(Effect.forkChild);

      yield* Queue.offerAll(providerEvents, assistantText("First.\n\n", true));
      yield* Deferred.await(firstDelivered);
      yield* Queue.offer(providerEvents, {
        type: "provider_thread.updated",
        driver,
        providerThread: { id: ids.providerThreadId, driver, status: "active" },
      } as ProviderAdapter.ProviderAdapterV2Event);
      yield* Deferred.await(blockerStarted);
      // While a write holds ingestion, the provider streams on: two steps of text, a usage report
      // and a tool call, two more steps, the tool's result, then the final text and the turn's end.
      yield* Queue.offerAll(
        providerEvents,
        withAnotherRun([
          ...assistantText("First.\n\nSecond.\n\n", true),
          ...assistantText("First.\n\nSecond.\n\nThird.\n\n", true),
          rootTurn("running"),
          backgroundTurnItemEvent(ids, "dynamic_tool", "running", 2, toolItemId),
          ...assistantText("First.\n\nSecond.\n\nThird.\n\nFourth.\n\n", true),
          ...assistantText("First.\n\nSecond.\n\nThird.\n\nFourth.\n\nFifth.\n\n", true),
          backgroundTurnItemEvent(ids, "dynamic_tool", "completed", 2, toolItemId),
          ...assistantText("First.\n\nSecond.\n\nThird.\n\nFourth.\n\nFifth.", false),
          rootTurn("completed"),
          rootTerminalEvent(ids, "completed"),
        ]),
      );
      // The write holds ingestion for a second, so the next text is shown as soon as it resumes.
      yield* TestClock.adjust("1 second");
      yield* Deferred.succeed(releaseBlocker, undefined);
      yield* Fiber.join(scenario);

      // A snapshot a newer one of its item superseded before a control (here the provider turn's
      // end) arrived is never ingested (any item, not only streamed text, and an item's final
      // replaces its steps); each item keeps its first arrival's place, and every control is
      // ingested once, in order, after everything that arrived before it.
      assert.deepEqual(yield* Ref.get(ingested), [
        'message.updated:streaming:"First.\\n\\n" in commit',
        'turn_item.updated:streaming:"First.\\n\\n" in commit',
        "provider_thread",
        'message.updated:final:"First.\\n\\nSecond.\\n\\nThird.\\n\\nFourth.\\n\\nFifth." in commit',
        'turn_item.updated:final:"First.\\n\\nSecond.\\n\\nThird.\\n\\nFourth.\\n\\nFifth." in commit',
        "provider_turn:running in commit",
        "tool:completed in commit",
        "provider_turn:completed",
        "turn.terminal",
      ]);
    }),
);

it.effect("hands a backlog to the commit path in one chunk, not one per event", () =>
  Effect.gen(function* () {
    const key = "backlog-handover";
    const ids = backgroundScenarioIds(key);
    const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const chunks = yield* Ref.make<ReadonlyArray<number>>([]);
    const blockerStarted = yield* Deferred.make<void>();
    const releaseBlocker = yield* Deferred.make<void>();
    const scenario = yield* runBackgroundItemScenario(key, () => Stream.fromQueue(providerEvents), {
      batched: true,
      onIngest: (event) =>
        event.type === "provider_thread.updated"
          ? Deferred.succeed(blockerStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseBlocker)),
            )
          : Effect.void,
      onIngestionChunk: (size) => Ref.update(chunks, (current) => [...current, size]),
    }).pipe(Effect.forkChild);

    yield* Queue.offer(providerEvents, {
      type: "provider_thread.updated",
      driver,
      providerThread: { id: ids.providerThreadId, driver, status: "active" },
    } as ProviderAdapter.ProviderAdapterV2Event);
    yield* Deferred.await(blockerStarted);
    // Twenty tool calls, none superseding another, and the turn's end arrive behind the write.
    const backlog = Array.from({ length: 20 }, (_, index) =>
      backgroundTurnItemEvent(
        ids,
        "dynamic_tool",
        "completed",
        index + 1,
        TurnItemId.make(`turn-item:${key}:${index}`),
      ),
    );
    yield* Queue.offerAll(providerEvents, [...backlog, rootTerminalEvent(ids, "completed")]);
    yield* Deferred.succeed(releaseBlocker, undefined);
    yield* Fiber.join(scenario);

    // Each chunk costs a pass through the merge with idle-flush ticks and through the commit
    // path's stages, so a backlog handed over an event at a time costs that many passes.
    assert.deepEqual(yield* Ref.get(chunks), [1, backlog.length + 1]);
  }),
);

it.effect(
  "takes what arrived during a write as one batch, so a later text step replaces an earlier one",
  () =>
    Effect.gen(function* () {
      const key = "busy-batch";
      const ids = backgroundScenarioIds(key);
      const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const ingested = yield* Ref.make<ReadonlyArray<string>>([]);
      const blockerStarted = yield* Deferred.make<void>();
      const releaseBlocker = yield* Deferred.make<void>();
      const text = (value: string, streaming: boolean) =>
        ({
          type: "turn_item.updated",
          driver,
          turnItem: {
            ...makeLinkedChildTurnItemFixture({ ids, driver, type: "assistant_message" }),
            id: ids.itemId,
            threadId: ids.threadId,
            runId: ids.runId,
            nodeId: ids.rootNodeId,
            providerTurnId: ids.rootProviderTurnId,
            text: value,
            streaming,
            status: streaming ? "running" : "completed",
          },
        }) as ProviderAdapter.ProviderAdapterV2Event;
      const scenario = yield* runBackgroundItemScenario(
        key,
        () => Stream.fromQueue(providerEvents),
        {
          batched: true,
          onIngest: (event) => {
            if (event.type === "provider_thread.updated")
              return Deferred.succeed(blockerStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseBlocker)),
              );
            if (event.type !== "turn_item.updated") return Effect.void;
            const item = event.turnItem;
            if (item.type !== "assistant_message") return Effect.void;
            return Ref.update(ingested, (current) => [
              ...current,
              `${item.streaming ? "streaming" : "final"}:${JSON.stringify(item.text)}`,
            ]);
          },
        },
      ).pipe(Effect.forkChild);

      yield* Queue.offer(providerEvents, {
        type: "provider_thread.updated",
        driver,
        providerThread: { id: ids.providerThreadId, driver, status: "active" },
      } as ProviderAdapter.ProviderAdapterV2Event);
      yield* Deferred.await(blockerStarted);
      // While the write holds ingestion, the text streams one step, then another.
      yield* Queue.offer(providerEvents, text("One.\n\n", true));
      yield* TestClock.adjust(0);
      yield* Queue.offerAll(providerEvents, [
        text("One.\n\nTwo.\n\n", true),
        text("One.\n\nTwo.\n\nThree.", false),
        rootTerminalEvent(ids, "completed"),
      ]);
      yield* TestClock.adjust("1 second");
      yield* Deferred.succeed(releaseBlocker, undefined);
      yield* Fiber.join(scenario);

      // Taking the first step while the write still ran would commit it on its own. The final,
      // arriving in the same batch, replaces the steps too.
      assert.deepEqual(yield* Ref.get(ingested), ['final:"One.\\n\\nTwo.\\n\\nThree."']);
    }),
);

it.effect("keeps interleaved streamed texts in the order they first appeared", () =>
  Effect.gen(function* () {
    const key = "streamed-backlog-order";
    const ids = backgroundScenarioIds(key);
    const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    // A text takes its position at its first reservation (held) or write (shown).
    const positions = yield* Ref.make<ReadonlyArray<string>>([]);
    const place = (turnItemId: string) =>
      Ref.update(positions, (current) =>
        current.includes(turnItemId) ? current : [...current, turnItemId],
      );
    const blockerStarted = yield* Deferred.make<void>();
    const releaseBlocker = yield* Deferred.make<void>();
    const first = TurnItemId.make(`turn-item:${key}:first`);
    const second = TurnItemId.make(`turn-item:${key}:second`);
    const text = (id: TurnItemId, value: string, streaming: boolean) =>
      ({
        type: "turn_item.updated",
        driver,
        turnItem: {
          ...makeLinkedChildTurnItemFixture({ ids, driver, type: "assistant_message" }),
          id,
          threadId: ids.threadId,
          runId: ids.runId,
          nodeId: ids.rootNodeId,
          providerTurnId: ids.rootProviderTurnId,
          text: value,
          streaming,
          status: streaming ? "running" : "completed",
        },
      }) as ProviderAdapter.ProviderAdapterV2Event;
    const scenario = yield* runBackgroundItemScenario(key, () => Stream.fromQueue(providerEvents), {
      batched: true,
      onIngest: (event) =>
        event.type === "provider_thread.updated"
          ? Deferred.succeed(blockerStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseBlocker)),
            )
          : event.type === "turn_item.updated"
            ? place(event.turnItem.id)
            : Effect.void,
      onPositionReserved: place,
    }).pipe(Effect.forkChild);

    yield* Queue.offer(providerEvents, {
      type: "provider_thread.updated",
      driver,
      providerThread: { id: ids.providerThreadId, driver, status: "active" },
    } as ProviderAdapter.ProviderAdapterV2Event);
    yield* Deferred.await(blockerStarted);
    // Behind the write, the first text streams, a second text starts, then the first streams on.
    yield* Queue.offerAll(providerEvents, [
      text(first, "First text", true),
      text(second, "Second text", true),
      text(first, "First text, more", true),
      text(first, "First text, more.", false),
      text(second, "Second text.", false),
      rootTerminalEvent(ids, "completed"),
    ]);
    yield* Deferred.succeed(releaseBlocker, undefined);
    yield* Fiber.join(scenario);

    // Coalescing the first text's steps must not move it behind the second.
    assert.deepEqual(yield* Ref.get(positions), [first, second]);
  }),
);

// A backlog's snapshots of one run, as on a busy server, where a run's events wait seconds for
// the shared commit lane. Each provider event carries its index in `sentIndex`, so what the run
// ingests can be checked against what the provider sent.
type IndexedEvent = ProviderAdapter.ProviderAdapterV2Event & { readonly sentIndex: number };

/** A snapshot's item, or null for a control (anything that is not one item's whole state). */
function coalescedItemKey(event: ProviderAdapter.ProviderAdapterV2Event): string | null {
  switch (event.type) {
    case "provider_turn.updated":
      return ["completed", "interrupted", "failed", "cancelled"].includes(event.providerTurn.status)
        ? null
        : `provider_turn:${event.providerTurn.id}`;
    case "node.updated":
      return `node:${event.node.id}`;
    case "turn_item.updated":
      return `turn_item:${event.turnItem.id}`;
    case "message.updated":
      return `message:${event.message.id}`;
    default:
      return null;
  }
}

function isFinalSnapshot(event: ProviderAdapter.ProviderAdapterV2Event): boolean {
  if (event.type === "message.updated") return !event.message.streaming;
  if (event.type === "turn_item.updated")
    return "streaming" in event.turnItem && event.turnItem.streaming === true
      ? false
      : !["pending", "running", "waiting", "idle"].includes(event.turnItem.status);
  if (event.type === "node.updated")
    return !["pending", "running", "waiting", "idle"].includes(event.node.status);
  return false;
}

/**
 * A coalescer ahead of the commit lane keeps: each item's newest state once between controls (a final
 * starts a new state), per-item order, first-arrival order, controls in place, every final, and a
 * crash at any cut losing no item that arrived before one it kept.
 */
function assertCoalescingInvariants(
  sent: ReadonlyArray<IndexedEvent>,
  ingested: ReadonlyArray<number>,
  complete = true,
) {
  // Every invariant is checked, so a broken coalescer shows each one it breaks.
  const broken: Array<string> = [];
  const check = (invariant: string, holds: boolean, detail: string) => {
    if (!holds) broken.push(`${invariant}: ${detail}`);
  };
  // The state each snapshot belongs to: (segment between controls, item, finals seen before).
  const state = new Map<number, string>();
  const stateFirst = new Map<string, number>();
  const stateLast = new Map<string, number>();
  const finalsSeen = new Map<string, number>();
  let segment = 0;
  const controls: Array<number> = [];
  for (const event of sent) {
    const key = coalescedItemKey(event);
    if (key === null) {
      controls.push(event.sentIndex);
      segment++;
      continue;
    }
    const id = `${segment}|${key}|${finalsSeen.get(key) ?? 0}`;
    state.set(event.sentIndex, id);
    if (!stateFirst.has(id)) stateFirst.set(id, event.sentIndex);
    stateLast.set(id, event.sentIndex);
    if (isFinalSnapshot(event)) finalsSeen.set(key, (finalsSeen.get(key) ?? 0) + 1);
  }
  const keyOf = new Map(sent.map((event) => [event.sentIndex, coalescedItemKey(event)]));
  const slotOf = (index: number) => (state.has(index) ? stateFirst.get(state.get(index)!)! : index);
  // Newest state, once.
  const ingestedStates = ingested.flatMap((index) => {
    const id = state.get(index);
    return id === undefined ? [] : [id];
  });
  const twice = ingestedStates.filter((id, at) => ingestedStates.indexOf(id) !== at);
  check("newest state once", twice.length === 0, `ingested twice: ${twice.join(", ")}`);
  const stale = ingested.filter(
    (index) => state.has(index) && stateLast.get(state.get(index)!) !== index,
  );
  check("newest state once", stale.length === 0, `not the newest: #${stale.join(", #")}`);
  // Per-item order.
  const lastOfKey = new Map<string, number>();
  for (const index of ingested) {
    const key = keyOf.get(index);
    if (key === null || key === undefined) continue;
    check("per-item order", index > (lastOfKey.get(key) ?? -1), `${key} went back to #${index}`);
    lastOfKey.set(key, Math.max(index, lastOfKey.get(key) ?? -1));
  }
  // First arrival.
  const firsts = ingested.map(slotOf);
  check(
    "first arrival",
    firsts.every((first, at) => at === 0 || first > firsts[at - 1]!),
    `slots in ingest order ${JSON.stringify(firsts)}`,
  );
  // Controls and terminals.
  for (const control of controls) {
    if (!ingested.includes(control)) {
      check("controls", !complete, `control #${control} never ingested`);
      continue;
    }
    check(
      "controls",
      ingested.filter((index) => index === control).length === 1,
      `control #${control} ingested more than once`,
    );
    const at = ingested.indexOf(control);
    const early = ingested
      .slice(0, at)
      .filter((index) => slotOf(index) > control || index > control);
    check(
      "controls",
      early.length === 0,
      `#${early.join(", #")} arrived after #${control} but went before it`,
    );
    const late = ingested.slice(at + 1).filter((index) => index < control);
    check(
      "controls",
      late.length === 0,
      `#${late.join(", #")} arrived before #${control} but went after it`,
    );
  }
  // Finals, once everything sent was ingested.
  if (complete)
    for (const event of sent)
      if (coalescedItemKey(event) !== null && isFinalSnapshot(event))
        check("finals", ingested.includes(event.sentIndex), `final #${event.sentIndex} dropped`);
  // Crash: every cut of what was ingested holds each item that arrived first.
  for (let cut = 1; cut <= ingested.length; cut++) {
    const prefix = new Set(
      ingested.slice(0, cut).map((index) => state.get(index) ?? `control:${index}`),
    );
    const reach = Math.max(...firsts.slice(0, cut));
    const lost = [...stateFirst].filter(([id, first]) => first <= reach && !prefix.has(id));
    check(
      "crash cuts",
      lost.length === 0,
      `a crash after ${cut} write(s) loses ${lost.map(([id]) => id).join(", ")}, which arrived first`,
    );
  }
  assert.deepEqual([...new Set(broken)], [], "coalescing invariants");
}

function coalescedBacklog(ids: BackgroundScenarioIds): ReadonlyArray<IndexedEvent> {
  const usage = (usedTokens: number) =>
    ({
      type: "provider_turn.updated",
      driver,
      providerTurn: {
        id: ids.rootProviderTurnId,
        providerThreadId: ids.providerThreadId,
        nodeId: ids.rootNodeId,
        runAttemptId: ids.attemptId,
        status: "running",
        tokenUsage: { usedTokens },
      },
    }) as ProviderAdapter.ProviderAdapterV2Event;
  const node = (status: "running" | "completed") =>
    ({
      type: "node.updated",
      driver,
      node: {
        id: NodeId.make(`${ids.rootNodeId}:tool-a`),
        threadId: ids.threadId,
        runId: ids.runId,
        parentNodeId: ids.rootNodeId,
        rootNodeId: ids.rootNodeId,
        kind: "tool_call",
        status,
        providerThreadId: null,
      },
    }) as ProviderAdapter.ProviderAdapterV2Event;
  const tool = (name: string, status: "running" | "completed", title?: string) => {
    const event = backgroundTurnItemEvent(
      ids,
      "dynamic_tool",
      status,
      2,
      TurnItemId.make(`${ids.itemId}:${name}`),
    );
    return title === undefined
      ? event
      : ({
          ...event,
          turnItem: { ...(event as { turnItem: object }).turnItem, title },
        } as ProviderAdapter.ProviderAdapterV2Event);
  };
  const text = (value: string, streaming: boolean) =>
    ({
      type: "turn_item.updated",
      driver,
      turnItem: {
        ...makeLinkedChildTurnItemFixture({ ids, driver, type: "assistant_message" }),
        id: TurnItemId.make(`${ids.itemId}:text`),
        threadId: ids.threadId,
        runId: ids.runId,
        nodeId: ids.rootNodeId,
        providerTurnId: ids.rootProviderTurnId,
        text: value,
        streaming,
        status: streaming ? "running" : "completed",
      },
    }) as ProviderAdapter.ProviderAdapterV2Event;
  const control = {
    type: "provider_thread.updated",
    driver,
    providerThread: { id: ids.providerThreadId, driver, status: "active" },
  } as ProviderAdapter.ProviderAdapterV2Event;
  return [
    // The held write.
    control,
    // What the provider sent meanwhile: a usage report, a tool call, a text step and a second tool
    // call, the first tool's result, another usage report; a control; the second tool's result, the
    // text's final, usage, a third tool that runs, finishes and is renamed after it finished; the
    // provider turn's end and the run's terminal.
    usage(1),
    node("running"),
    tool("a", "running"),
    text("Before the tools.\n\n", true),
    tool("c", "running"),
    node("completed"),
    tool("a", "completed"),
    usage(2),
    control,
    tool("c", "completed"),
    text("Before the tools.\n\nAfter them.", false),
    usage(3),
    tool("d", "running"),
    tool("d", "completed"),
    tool("d", "completed", "Tool d, renamed"),
    {
      type: "provider_turn.updated",
      driver,
      providerTurn: {
        id: ids.rootProviderTurnId,
        providerThreadId: ids.providerThreadId,
        nodeId: ids.rootNodeId,
        runAttemptId: ids.attemptId,
        status: "completed",
      },
    } as ProviderAdapter.ProviderAdapterV2Event,
    rootTerminalEvent(ids, "completed"),
  ].map((event, sentIndex) => ({ ...event, sentIndex }) as IndexedEvent);
}

/** Runs the backlog behind a held control write; returns the indexes the run ingested, in order. */
function runCoalescedBacklog(key: string, failAtWrite?: number) {
  return Effect.gen(function* () {
    const ids = backgroundScenarioIds(key);
    const sent = coalescedBacklog(ids);
    const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const ingested = yield* Ref.make<ReadonlyArray<number>>([]);
    const lane = yield* Ref.make(0);
    const blockerStarted = yield* Deferred.make<void>();
    const releaseBlocker = yield* Deferred.make<void>();
    const finalized = yield* Deferred.make<void>();
    let writes = 0;
    const scenario = yield* runBackgroundItemScenario(key, () => Stream.fromQueue(providerEvents), {
      batched: true,
      onIngest: (event, write) =>
        Effect.gen(function* () {
          const index = (event as Partial<IndexedEvent>).sentIndex!;
          if (++writes === failAtWrite) return yield* Effect.die("injected write failure");
          if (index === 0) {
            yield* Deferred.succeed(blockerStarted, undefined);
            yield* Deferred.await(releaseBlocker);
          }
          if (write.inSharedCommit) yield* Ref.update(lane, (count) => count + 1);
          yield* Ref.update(ingested, (current) => [...current, index]);
        }),
      // The terminal is ingested where it finalizes the run (once, whatever it writes).
      onFinalized: () =>
        Ref.update(ingested, (current) =>
          current.includes(sent.at(-1)!.sentIndex) ? current : [...current, sent.at(-1)!.sentIndex],
        ).pipe(Effect.andThen(Deferred.succeed(finalized, undefined))),
    }).pipe(Effect.exit, Effect.forkChild);
    yield* Queue.offer(providerEvents, sent[0]!);
    yield* Deferred.await(blockerStarted);
    yield* Queue.offerAll(providerEvents, sent.slice(1));
    yield* Deferred.succeed(releaseBlocker, undefined);
    // The run settles at its terminal, after everything before it. A broken coalescer can leave
    // the run waiting for a state it dropped: what it ingested by then is still checked.
    yield* Effect.raceFirst(Fiber.join(scenario).pipe(Effect.asVoid), Deferred.await(finalized));
    yield* Fiber.interrupt(scenario);
    return { sent, ingested: yield* Ref.get(ingested), laneWrites: yield* Ref.get(lane) };
  });
}

it.effect("ingests each item of a backlog once, newest state at its first arrival", () =>
  Effect.gen(function* () {
    const { sent, ingested, laneWrites } = yield* runCoalescedBacklog("coalesced-backlog");
    assertCoalescingInvariants(sent, ingested);
    // The backlog's 14 snapshots are 10 states: the tools' running reports, the earlier usage
    // reports and nothing else are superseded before the lane. Without coalescing all 14 reach it.
    assert.equal(laneWrites, 10);
    assert.deepEqual(ingested, [0, 8, 6, 7, 4, 5, 9, 10, 11, 12, 14, 15, 16, 17]);
  }),
);

it.effect(
  "ingests a late final queued before a background item's result after the root terminal",
  () =>
    Effect.gen(function* () {
      // After the root terminal every event is checked for the end of ingestion; a background
      // command keeps it open. A result moved ahead of an event that arrived before it would end
      // ingestion first and drop that event.
      const key = "coalesced-post-terminal";
      const ids = backgroundScenarioIds(key);
      const command = TurnItemId.make(`${ids.itemId}:command`);
      const late = TurnItemId.make(`${ids.itemId}:late`);
      const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const ingested = yield* Ref.make<ReadonlyArray<string>>([]);
      const blockerStarted = yield* Deferred.make<void>();
      const releaseBlocker = yield* Deferred.make<void>();
      let blocked = false;
      const scenario = yield* runBackgroundItemScenario(
        key,
        () => Stream.fromQueue(providerEvents),
        {
          batched: true,
          onIngest: (event) =>
            Effect.gen(function* () {
              if (event.type === "provider_thread.updated" && !blocked) {
                blocked = true;
                yield* Deferred.succeed(blockerStarted, undefined);
                yield* Deferred.await(releaseBlocker);
              }
              if (event.type === "turn_item.updated")
                yield* Ref.update(ingested, (current) => [
                  ...current,
                  `${event.turnItem.id === command ? "command" : "late"}:${event.turnItem.status}`,
                ]);
            }),
        },
      ).pipe(Effect.forkChild);
      yield* Queue.offerAll(providerEvents, [
        backgroundTurnItemEvent(ids, "command_execution", "running", 2, command),
        rootTerminalEvent(ids, "completed"),
        {
          type: "provider_thread.updated",
          driver,
          providerThread: { id: ids.providerThreadId, driver, status: "active" },
        } as ProviderAdapter.ProviderAdapterV2Event,
      ]);
      yield* Deferred.await(blockerStarted);
      yield* Queue.offerAll(providerEvents, [
        backgroundTurnItemEvent(ids, "command_execution", "running", 2, command),
        backgroundTurnItemEvent(ids, "dynamic_tool", "completed", 3, late),
        backgroundTurnItemEvent(ids, "command_execution", "completed", 2, command),
      ]);
      yield* Deferred.succeed(releaseBlocker, undefined);
      yield* Fiber.join(scenario);
      assert.deepEqual(yield* Ref.get(ingested), [
        "command:running",
        "command:running",
        "late:completed",
        "command:completed",
      ]);
    }),
);

it.effect("persists a prefix of the coalesced backlog when a write fails mid-batch", () =>
  Effect.gen(function* () {
    // The third write (item a's result, which took its running report's place) fails: its run
    // keeps the two writes before it, and nothing after it.
    const { sent, ingested } = yield* runCoalescedBacklog("coalesced-backlog-crash", 4);
    assert.deepEqual(ingested.slice(0, 3), [0, 8, 6]);
    // Nothing of the backlog is written after the failure; only the run's end (its terminal's
    // index, recorded when the run is finalized) may follow.
    assert.deepEqual(
      ingested.slice(3).filter((index) => index !== sent.at(-1)!.sentIndex),
      [],
    );
    assertCoalescingInvariants(sent, ingested.slice(0, 3), false);
  }),
);

it.effect("ends finished Codex seats' work on time on a session with 90 bound threads", () =>
  Effect.gen(function* () {
    // Only run consumers subscribe and Codex reports only turns it tracks, so a bound thread without
    // a run costs nothing. A finished run keeps its consumer while work it started runs on (a
    // seat's follower), and that consumer gets every event of the session. 90 bound threads: 2 live
    // runs, 28 recently finished seats, 60 threads with no consumer.
    const live = 2;
    const seats = 28;
    const coordinator = yield* ProviderCommitBatch.make((work) => work, true);
    const write = Effect.sleep("100 millis");
    const holding = yield* Deferred.make<void>();
    yield* coordinator
      .consume(
        Stream.make(
          backgroundTurnItemEvent(backgroundScenarioIds("other"), "dynamic_tool", "running", 1),
        ),
        () => Deferred.succeed(holding, undefined).pipe(Effect.andThen(write)),
        Effect.succeed(false),
      )
      .pipe(Effect.forkChild);
    yield* Deferred.await(holding);
    for (let writer = 0; writer < 8; writer++)
      yield* coordinator.barrier(write).pipe(Effect.forever, Effect.forkChild);
    const ids = Array.from({ length: live + seats }, (_, run) =>
      backgroundScenarioIds(`finished-seat-${run}`),
    );
    const rootTurn = (
      own: BackgroundScenarioIds,
      status: "running" | "completed",
    ): ProviderAdapter.ProviderAdapterV2Event =>
      ({
        type: "provider_turn.updated",
        driver,
        providerTurn: {
          id: own.rootProviderTurnId,
          providerThreadId: own.providerThreadId,
          nodeId: own.rootNodeId,
          runAttemptId: own.attemptId,
          status,
        },
      }) as ProviderAdapter.ProviderAdapterV2Event;
    // One 50 ms flush of streamed Codex text: the message's node and its streaming turn item.
    const textFlush = (
      own: BackgroundScenarioIds,
      round: number,
      flush: number,
    ): ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event> => {
      const nodeId = NodeId.make(`${own.rootNodeId}:text:${round}`);
      const itemId = TurnItemId.make(`${own.itemId}:text:${round}`);
      return [
        {
          type: "node.updated",
          driver,
          node: {
            id: nodeId,
            threadId: own.threadId,
            runId: own.runId,
            kind: "assistant_message",
            status: "running",
            providerThreadId: own.providerThreadId,
          },
        } as ProviderAdapter.ProviderAdapterV2Event,
        {
          type: "turn_item.updated",
          driver,
          turnItem: {
            id: itemId,
            threadId: own.threadId,
            runId: own.runId,
            nodeId,
            providerTurnId: own.rootProviderTurnId,
            ordinal: round * 100 + 50,
            type: "assistant_message",
            status: "running",
            messageId: MessageId.make(`${itemId}:message`),
            text: "word ".repeat(flush + 1),
            streaming: true,
          },
        } as ProviderAdapter.ProviderAdapterV2Event,
      ];
    };
    const follower = (own: BackgroundScenarioIds, status: "running" | "completed") =>
      backgroundTurnItemEvent(
        own,
        "dynamic_tool",
        status,
        status === "running" ? 1 : 2,
        TurnItemId.make(`${own.itemId}:follower`),
      );
    const subscriptions = yield* Effect.forEach(ids, () =>
      Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>(),
    );
    // Live runs: the last round of their own items ingested. Seats: whether the follower's
    // completion was ingested.
    const ingestedRound = ids.map(() => 0);
    const followerEnded = ids.map(() => false);
    for (let run = 0; run < ids.length; run++) {
      const own = ids[run]!;
      for (const event of run < live
        ? [rootTurn(own, "running")]
        : [
            rootTurn(own, "running"),
            follower(own, "running"),
            rootTurn(own, "completed"),
            rootTerminalEvent(own, "completed"),
          ])
        yield* Queue.offer(subscriptions[run]!, event);
      yield* runBackgroundItemScenario(
        `finished-seat-${run}`,
        () => Stream.fromQueue(subscriptions[run]!),
        {
          coordinator,
          shouldStartProviderTurn: () => Effect.succeed(true),
          onIngest: (event) =>
            Effect.sync(() => {
              if (event.type !== "turn_item.updated" || event.turnItem.threadId !== own.threadId)
                return;
              if (run >= live) followerEnded[run] = event.turnItem.status === "completed";
              else
                ingestedRound[run] = Math.max(
                  ingestedRound[run]!,
                  Math.floor(event.turnItem.ordinal / 100),
                );
            }),
        },
      ).pipe(Effect.forkChild);
    }
    let round = 0;
    const offerAll = (event: ProviderAdapter.ProviderAdapterV2Event) =>
      Effect.forEach(subscriptions, (subscription) => Queue.offer(subscription, event), {
        discard: true,
      });
    yield* Effect.gen(function* () {
      for (;;) {
        yield* Effect.sleep("10 seconds");
        round++;
        // A live run reports usage, streams a second of text (Codex sends a node and a turn item
        // every 50 ms) and runs a tool. A finished seat sends nothing until its follower ends.
        for (const own of ids.slice(0, live))
          for (const event of [
            rootTurn(own, "running"),
            ...Array.from({ length: 20 }, (_, flush) => textFlush(own, round, flush)).flat(),
            backgroundTurnItemEvent(own, "dynamic_tool", "running", round * 100 + 1),
            backgroundTurnItemEvent(own, "dynamic_tool", "completed", round * 100 + 2),
          ])
            yield* offerAll(event);
      }
    }).pipe(Effect.forkChild);
    yield* TestClock.adjust("5 minutes");
    for (const own of ids.slice(live)) yield* offerAll(follower(own, "completed"));
    yield* TestClock.adjust("20 seconds");
    // Before this, the live runs kept up but no seat had ingested its follower's end: past its root
    // terminal a consumer ran every event of the session alone and was minutes behind.
    assert.deepEqual(
      ids.map((_, run) => (run < live ? round - ingestedRound[run]! <= 2 : followerEnded[run])),
      ids.map(() => true),
      `live runs' rounds behind: ${ingestedRound
        .slice(0, live)
        .map((ingested) => round - ingested)
        .join(
          ", ",
        )}; seats whose follower end was ingested: ${followerEnded.slice(live).filter(Boolean).length} of ${seats}`,
    );
  }),
);

it.effect.each([
  // A few lagging consumers, each behind throughout, run every usage report on the session alone:
  // a writer pass of seconds.
  { live: 6, writeMs: 400 },
  // Every bound thread with events in a busy window, on a 1 s writer pass.
  { live: 16, writeMs: 100 },
])(
  "keeps $live live runs on a shared provider session up with a busy session's event mix ($writeMs ms writes)",
  ({ live, writeMs }) =>
    Effect.gen(function* () {
      // A busy Codex session's mix: per usage report a few turn items and node snapshots. Every
      // 10 s each live run reports usage once, writes four turn items (two tool calls) and four
      // node snapshots. The other bound threads, up to 90, have no
      // consumer and send nothing.
      const coordinator = yield* ProviderCommitBatch.make((work) => work, true);
      const write = Effect.sleep(Duration.millis(writeMs));
      const holding = yield* Deferred.make<void>();
      yield* coordinator
        .consume(
          Stream.make(
            backgroundTurnItemEvent(backgroundScenarioIds("other"), "dynamic_tool", "running", 1),
          ),
          () => Deferred.succeed(holding, undefined).pipe(Effect.andThen(write)),
          Effect.succeed(false),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(holding);
      for (let writer = 0; writer < 10; writer++)
        yield* coordinator.barrier(write).pipe(Effect.forever, Effect.forkChild);
      const ids = Array.from({ length: live }, (_, run) =>
        backgroundScenarioIds(`busy-mix-${live}-${run}`),
      );
      const rootTurn = (own: BackgroundScenarioIds): ProviderAdapter.ProviderAdapterV2Event =>
        ({
          type: "provider_turn.updated",
          driver,
          providerTurn: {
            id: own.rootProviderTurnId,
            providerThreadId: own.providerThreadId,
            nodeId: own.rootNodeId,
            runAttemptId: own.attemptId,
            status: "running",
          },
        }) as ProviderAdapter.ProviderAdapterV2Event;
      const rootNode = (own: BackgroundScenarioIds): ProviderAdapter.ProviderAdapterV2Event =>
        ({
          type: "node.updated",
          driver,
          node: {
            id: own.rootNodeId,
            threadId: own.threadId,
            runId: own.runId,
            kind: "root_turn",
            status: "running",
            providerThreadId: own.providerThreadId,
          },
        }) as ProviderAdapter.ProviderAdapterV2Event;
      const subscriptions = yield* Effect.forEach(ids, () =>
        Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>(),
      );
      // The last round of its own items each run has ingested.
      const ingestedRound = ids.map(() => 0);
      for (let run = 0; run < live; run++) {
        const own = ids[run]!;
        yield* Queue.offer(subscriptions[run]!, rootTurn(own));
        yield* runBackgroundItemScenario(
          `busy-mix-${live}-${run}`,
          () => Stream.fromQueue(subscriptions[run]!),
          {
            coordinator,
            shouldStartProviderTurn: () => Effect.succeed(true),
            onIngest: (event) =>
              Effect.sync(() => {
                if (event.type === "turn_item.updated" && event.turnItem.threadId === own.threadId)
                  ingestedRound[run] = Math.max(
                    ingestedRound[run]!,
                    Math.floor(event.turnItem.ordinal / 100),
                  );
              }),
          },
        ).pipe(Effect.forkChild);
      }
      let round = 0;
      yield* Effect.gen(function* () {
        for (;;) {
          yield* Effect.sleep("10 seconds");
          round++;
          for (const own of ids) {
            const events = [
              rootTurn(own),
              ...Array.from({ length: 3 }, () => rootNode(own)),
              ...[0, 1, 2].flatMap((tool) => {
                const itemId = TurnItemId.make(`${own.itemId}:${tool}`);
                return [
                  backgroundTurnItemEvent(
                    own,
                    "dynamic_tool",
                    "running",
                    round * 100 + 2 * tool + 1,
                    itemId,
                  ),
                  backgroundTurnItemEvent(
                    own,
                    "dynamic_tool",
                    "completed",
                    round * 100 + 2 * tool + 2,
                    itemId,
                  ),
                ];
              }),
            ];
            for (const event of events)
              for (const subscription of subscriptions) yield* Queue.offer(subscription, event);
          }
        }
      }).pipe(Effect.forkChild);
      yield* TestClock.adjust("5 minutes");
      // Before these fixes the runs fell 20 and 12-13 rounds behind; with the first two 10-11 and
      // each run still queued every other run's snapshots, jobs that grow as the runs squared.
      const behind = ingestedRound.map((ingested) => round - ingested);
      assert.deepEqual(
        behind.map((rounds) => rounds <= 2),
        behind.map(() => true),
        `rounds behind: ${behind.join(", ")}`,
      );
    }),
);

it.effect("keeps another run's event on a child the run gains in the same backlog", () => {
  const recording = recordingWaitObserver();
  // The run outlives the service's scope in this harness, so keep the counter's reader.
  let dropped: (() => number) | undefined;
  const observer: typeof recording.observer = {
    ...recording.observer,
    register: (name, kind, read) => {
      if (name === IngestLag.FANOUT_DROPPED_SITE) dropped = read;
      return recording.observer.register(name, kind, read);
    },
  };
  return Effect.gen(function* () {
    // Routing gains the child only when it takes the child's creation, so another run's event
    // queued behind it must not be dropped as unowned. Later, that run's own work is dropped.
    const key = "gained-child";
    const other = backgroundScenarioIds(`${key}:other`);
    const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const ingested: Array<string> = [];
    const childIngested = yield* Deferred.make<void>();
    const ids = backgroundScenarioIds(key);
    const scenario = yield* runBackgroundItemScenario(key, () => Stream.fromQueue(providerEvents), {
      batched: true,
      onIngest: (event) =>
        event.type === "turn_item.updated"
          ? Effect.sync(() =>
              ingested.push(`${event.turnItem.threadId}:${event.turnItem.runId}`),
            ).pipe(
              Effect.andThen(
                event.turnItem.threadId === ids.childThreadId
                  ? Deferred.succeed(childIngested, undefined)
                  : Effect.void,
              ),
            )
          : Effect.void,
    }).pipe(Effect.forkChild);
    yield* Queue.offerAll(providerEvents, [
      childThreadCreatedEvent(ids),
      {
        type: "turn_item.updated",
        driver,
        turnItem: {
          ...makeLinkedChildTurnItemFixture({ ids, driver, type: "command_execution" }),
          runId: other.runId,
          status: "completed",
        },
      } as ProviderAdapter.ProviderAdapterV2Event,
    ]);
    yield* Deferred.await(childIngested);
    const othersWork = [
      backgroundTurnItemEvent(other, "dynamic_tool", "running", 1),
      backgroundTurnItemEvent(other, "dynamic_tool", "completed", 2),
    ];
    // As the session manager stamps each event it takes from the adapter.
    for (const event of othersWork) IngestLag.markArrival(event, 0n);
    yield* Queue.offerAll(providerEvents, [...othersWork, rootTerminalEvent(ids, "completed")]);
    yield* Fiber.join(scenario);

    assert.deepEqual(ingested, [`${ids.childThreadId}:${other.runId}`]);
    // Dropped events are counted, and are neither backlog nor lag.
    assert.strictEqual(dropped?.(), 2);
    assert.isFalse(
      recording.reports("wait").some(([site]) => String(site).startsWith("ProviderIngest.lag.")),
      "a dropped event reports no ingest lag",
    );
  }).pipe(Effect.provideService(CurrentWaitObserver, observer));
});

it.effect("drops another run's work again after a child's coalesced snapshots", () => {
  const recording = recordingWaitObserver();
  let dropped: (() => number) | undefined;
  const observer: typeof recording.observer = {
    ...recording.observer,
    register: (name, kind, read) => {
      if (name === IngestLag.FANOUT_DROPPED_SITE) dropped = read;
      return recording.observer.register(name, kind, read);
    },
  };
  return Effect.gen(function* () {
    // A child's streamed text on a new provider thread holds drops until routing takes it. The
    // mailbox keeps only its newest step, and the steps it replaced must release the hold too.
    const key = "coalesced-child";
    const ids = backgroundScenarioIds(key);
    const other = backgroundScenarioIds(`${key}:other`);
    const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const blockerStarted = yield* Deferred.make<void>();
    const releaseBlocker = yield* Deferred.make<void>();
    const markerIngested = yield* Deferred.make<void>();
    const scenario = yield* runBackgroundItemScenario(key, () => Stream.fromQueue(providerEvents), {
      batched: true,
      onIngest: (event) =>
        event.type === "provider_thread.updated"
          ? Deferred.succeed(blockerStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseBlocker)),
            )
          : event.type === "turn_item.updated" && event.turnItem.threadId === ids.threadId
            ? Deferred.succeed(markerIngested, undefined)
            : Effect.void,
    }).pipe(Effect.forkChild);
    const childText = () =>
      ({
        type: "node.updated",
        driver,
        node: {
          id: NodeId.make(`node:${key}:child-text`),
          threadId: ids.childThreadId,
          runId: null,
          kind: "assistant_message",
          status: "running",
          providerThreadId: ProviderThreadId.make(`provider-thread:${key}:child`),
        },
      }) as ProviderAdapter.ProviderAdapterV2Event;
    yield* Queue.offerAll(providerEvents, [
      childThreadCreatedEvent(ids),
      {
        type: "provider_thread.updated",
        driver,
        providerThread: { id: ids.providerThreadId, driver, status: "active" },
      } as ProviderAdapter.ProviderAdapterV2Event,
    ]);
    yield* Deferred.await(blockerStarted);
    // Behind the write, the child streams three steps, which the mailbox takes in one pass, then
    // this run's tool reports; once it is ingested, routing has taken the child's text.
    yield* Queue.offerAll(providerEvents, [
      childText(),
      childText(),
      childText(),
      backgroundTurnItemEvent(ids, "dynamic_tool", "completed", 1),
    ]);
    yield* Deferred.succeed(releaseBlocker, undefined);
    yield* Deferred.await(markerIngested);
    yield* Queue.offerAll(providerEvents, [
      backgroundTurnItemEvent(other, "dynamic_tool", "running", 1),
      backgroundTurnItemEvent(other, "dynamic_tool", "completed", 2),
      rootTerminalEvent(ids, "completed"),
    ]);
    yield* Fiber.join(scenario);

    assert.strictEqual(dropped?.(), 2);
  }).pipe(Effect.provideService(CurrentWaitObserver, observer));
});

it.effect("ends a finished run's ingestion when its work ends while only another run streams", () =>
  Effect.gen(function* () {
    // A run past its root terminal stays for background work it can only probe. Another run's
    // events are dropped, yet each pass that drops some still asks whether ingestion can stop.
    const key = "probe-stop";
    const other = backgroundScenarioIds(`${key}:other`);
    const providerEvents = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const pending = yield* Ref.make(true);
    const finalized = yield* Ref.make(false);
    const probedAfterTerminal = yield* Deferred.make<void>();
    const scenario = yield* runBackgroundItemScenario(key, () => Stream.fromQueue(providerEvents), {
      batched: true,
      hasPendingBackgroundWorkForThread: () =>
        Effect.gen(function* () {
          if (yield* Ref.get(finalized)) yield* Deferred.succeed(probedAfterTerminal, undefined);
          return yield* Ref.get(pending);
        }),
      onFinalized: () => Ref.set(finalized, true),
    }).pipe(Effect.forkChild);
    const ids = backgroundScenarioIds(key);
    yield* Queue.offer(providerEvents, rootTerminalEvent(ids, "completed"));
    yield* Deferred.await(probedAfterTerminal);
    yield* TestClock.adjust("1 second");
    assert.isUndefined(scenario.pollUnsafe(), "pending work holds ingestion open");
    // The work ends without an event of this run's.
    yield* Ref.set(pending, false);
    yield* Queue.offer(
      providerEvents,
      backgroundTurnItemEvent(other, "dynamic_tool", "running", 1),
    );
    yield* Fiber.join(scenario);
  }),
);

const heldChildText = "partial response";

function childAssistantSnapshot(
  ids: BackgroundScenarioIds,
): ProviderAdapter.ProviderAdapterV2Event {
  return {
    type: "turn_item.updated",
    driver,
    turnItem: {
      ...makeLinkedChildTurnItemFixture({ ids, driver, type: "assistant_message" }),
      text: heldChildText,
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
}

function linkedSubagentEvent(
  ids: BackgroundScenarioIds,
  status: "running" | "completed",
): ProviderAdapter.ProviderAdapterV2Event {
  return {
    type: "subagent.updated",
    driver,
    subagent: {
      ...makeRunOwnedSubagentFixture({
        ids,
        providerInstanceId: ProviderInstanceId.make("codex"),
        childThreadId: ids.childThreadId,
        driver,
        status: "running",
      }),
      status,
    },
  };
}

const isHeldChildSnapshot = (event: ProviderAdapter.ProviderAdapterV2Event) =>
  event.type === "turn_item.updated" &&
  "text" in event.turnItem &&
  event.turnItem.text === heldChildText;

// Feeds a held child snapshot, then waits until ingestion has read past it.
const holdChildSnapshot = Effect.fn(function* (
  ids: BackgroundScenarioIds,
  events: Queue.Enqueue<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>,
  subagentIngested: Deferred.Deferred<void>,
) {
  yield* Queue.offerAll(events, [
    childThreadCreatedEvent(ids),
    childAssistantSnapshot(ids),
    linkedSubagentEvent(ids, "running"),
  ]);
  yield* Deferred.await(subagentIngested);
});

it.effect("settles a child message only an idle flush wrote when the root is interrupted", () =>
  Effect.gen(function* () {
    const ids = backgroundScenarioIds("idle-child-interrupt");
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>();
    const subagentIngested = yield* Deferred.make<void>();
    const flushed = yield* Deferred.make<void>();
    const finalized = yield* Ref.make<ReadonlyArray<OrchestrationV2DomainEvent>>([]);
    const scenario = yield* runBackgroundItemScenario(
      "idle-child-interrupt",
      () => Stream.fromQueue(events),
      {
        onIngest: (event) =>
          event.type === "subagent.updated"
            ? Deferred.succeed(subagentIngested, undefined)
            : isHeldChildSnapshot(event)
              ? Deferred.succeed(flushed, undefined)
              : Effect.void,
        onFinalized: (written) => Ref.set(finalized, written),
      },
    ).pipe(Effect.forkChild);

    yield* holdChildSnapshot(ids, events, subagentIngested);
    yield* TestClock.adjust("1 second");
    yield* Deferred.await(flushed);
    yield* Queue.offer(events, rootTerminalEvent(ids, "interrupted"));
    yield* Fiber.join(scenario);

    const settled = (yield* Ref.get(finalized)).flatMap((event) =>
      event.type === "turn-item.updated" && event.payload.id === ids.childItemId
        ? [event.payload]
        : [],
    );
    assert.equal(settled.length, 1);
    assert.equal(settled[0]?.status, "interrupted");
    assert.equal("text" in settled[0]! ? settled[0].text : null, heldChildText);
    assert.isFalse("streaming" in settled[0]! && settled[0].streaming);
  }),
);

it.effect.each([
  { name: "completed", terminalStatus: "completed" as const },
  { name: "failed with its children continuing", terminalStatus: "failed" as const },
])("idle-flushes a continuing child's text after the root $name", ({ terminalStatus }) =>
  Effect.gen(function* () {
    const key = `idle-child-after-${terminalStatus}`;
    const ids = backgroundScenarioIds(key);
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>();
    const subagentIngested = yield* Deferred.make<void>();
    const rootFinalized = yield* Deferred.make<void>();
    const flushed = yield* Deferred.make<void>();
    const rootWrites = yield* Ref.make<ReadonlyArray<string>>([]);
    const scenario = yield* runBackgroundItemScenario(key, () => Stream.fromQueue(events), {
      onIngest: (event) =>
        event.type === "subagent.updated"
          ? Deferred.succeed(subagentIngested, undefined)
          : isHeldChildSnapshot(event)
            ? Deferred.succeed(flushed, undefined)
            : event.type === "message.updated" && event.message.threadId === ids.threadId
              ? Ref.update(rootWrites, (current) => [...current, event.message.text])
              : Effect.void,
      onFinalized: () => Deferred.succeed(rootFinalized, undefined),
    }).pipe(Effect.forkChild);

    yield* holdChildSnapshot(ids, events, subagentIngested);
    // The root ends mid-paragraph: its tail is held, and the run settles without it.
    yield* Queue.offer(
      events,
      streamedMessageEvent({
        id: `message:${key}:root`,
        threadId: ids.threadId,
        runId: ids.runId,
        role: "assistant",
        text: "Root.\n\nhalf",
        streaming: true,
      }),
    );
    const terminal = rootTerminalEvent(ids, terminalStatus);
    yield* Queue.offer(
      events,
      terminalStatus === "failed"
        ? ({ ...terminal, childTurnsContinue: true } as ProviderAdapter.ProviderAdapterV2Event)
        : terminal,
    );
    yield* Deferred.await(rootFinalized);
    yield* TestClock.adjust("1 second");
    yield* Deferred.await(flushed);
    yield* Queue.offer(events, linkedSubagentEvent(ids, "completed"));
    const observed = yield* Fiber.join(scenario);

    assert.deepEqual(observed.slice(-3), [
      "root-finalized",
      "turn_item:running",
      "subagent:completed",
    ]);
    // A streaming write after the run settled would reopen the root's message.
    assert.deepEqual(yield* Ref.get(rootWrites), ["Root.\n\n"]);
  }),
);

function streamedMessageEvent(input: {
  readonly id: string;
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly role: "assistant" | "user";
  readonly text: string;
  readonly streaming: boolean;
}): ProviderAdapter.ProviderAdapterV2Event {
  const now = DateTime.makeUnsafe("2026-07-21T12:00:00.000Z");
  return {
    type: "message.updated",
    driver,
    message: {
      createdBy: input.role === "user" ? "user" : "agent",
      creationSource: "provider",
      id: MessageId.make(input.id),
      threadId: input.threadId,
      runId: input.runId,
      nodeId: null,
      role: input.role,
      text: input.text,
      attachments: [],
      streaming: input.streaming,
      createdAt: now,
      updatedAt: now,
    },
  } as ProviderAdapter.ProviderAdapterV2Event;
}

it.effect("drops a child's held text once the child is handed to another run", () =>
  Effect.gen(function* () {
    const key = "idle-child-handoff";
    const ids = backgroundScenarioIds(key);
    const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event, Cause.Done>();
    const subagentIngested = yield* Deferred.make<void>();
    const handoffRead = yield* Deferred.make<void>();
    const rootFlushed = yield* Deferred.make<void>();
    const staleWrites = yield* Ref.make<ReadonlyArray<string>>([]);
    const scenario = yield* runBackgroundItemScenario(key, () => Stream.fromQueue(events), {
      onIngest: (event) => {
        if (event.type === "subagent.updated") return Deferred.succeed(subagentIngested, undefined);
        if (event.type !== "message.updated" && event.type !== "turn_item.updated")
          return Effect.void;
        const message = event.type === "message.updated" ? event.message : event.turnItem;
        if (message.threadId === ids.childThreadId)
          return Ref.update(staleWrites, (current) => [...current, event.type]);
        if (event.type === "message.updated" && event.message.role === "user")
          return Deferred.succeed(handoffRead, undefined);
        return Deferred.succeed(rootFlushed, undefined);
      },
    }).pipe(Effect.forkChild);

    yield* holdChildSnapshot(ids, events, subagentIngested);
    const handoff = linkedSubagentEvent(ids, "running");
    yield* Queue.offerAll(events, [
      streamedMessageEvent({
        id: `message:${key}:child`,
        threadId: ids.childThreadId,
        runId: null,
        role: "assistant",
        text: heldChildText,
        streaming: true,
      }),
      handoff.type === "subagent.updated"
        ? { ...handoff, subagent: { ...handoff.subagent, runId: RunId.make(`run:${key}:next`) } }
        : handoff,
      // Due in the same flush as the child's text, after it.
      streamedMessageEvent({
        id: `message:${key}:root`,
        threadId: ids.threadId,
        runId: ids.runId,
        role: "assistant",
        text: "Root partial",
        streaming: true,
      }),
      streamedMessageEvent({
        id: `message:${key}:marker`,
        threadId: ids.threadId,
        runId: ids.runId,
        role: "user",
        text: "marker",
        streaming: false,
      }),
    ]);
    yield* Deferred.await(handoffRead);
    yield* TestClock.adjust("1 second");
    yield* Deferred.await(rootFlushed);
    yield* Queue.end(events);
    yield* Fiber.join(scenario);

    assert.deepEqual(yield* Ref.get(staleWrites), []);
  }),
);

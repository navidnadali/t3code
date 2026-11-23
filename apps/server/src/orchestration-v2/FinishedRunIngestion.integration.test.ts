import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  type RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import { CurrentWaitObserver, type WaitObserver } from "@t3tools/shared/WaitObserver";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { ClaudeProviderCapabilitiesV2 } from "./Adapters/ClaudeAdapterV2.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as IngestLag from "./IngestLag.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

const driver = ProviderDriverKind.make("claudeAgent");
const instanceId = ProviderInstanceId.make("claudeAgent");
const modelSelection = { instanceId, model: "claude-test" };
const INGESTION_FAILED = "orchestration V2 provider event ingestion failed";

interface Turn {
  readonly turn: ProviderAdapter.ProviderAdapterV2TurnInput;
  readonly events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>;
}

/**
 * Each run's event subscription, by thread in the order the thread's runs subscribed. A run
 * reports its backlog under its thread until its subscription closes.
 */
const makeSubscriptionLog = () => {
  const byThread = new Map<string, Array<Deferred.Deferred<void>>>();
  const observer: WaitObserver = {
    wait: () => {},
    hold: () => {},
    depth: () => {},
    register: () => () => {},
    sizeKeys: () => () => {},
    registerKeyed: (site, key) => {
      if (site !== IngestLag.BACKLOG_SITE) return () => {};
      const closed = Deferred.makeUnsafe<void>();
      byThread.set(String(key), [...(byThread.get(String(key)) ?? []), closed]);
      return () => {
        Deferred.doneUnsafe(closed, Exit.void);
      };
    },
  };
  return {
    observer,
    subscription: (threadId: ThreadId, index: number) => {
      const closed = byThread.get(threadId)?.[index];
      assert.isDefined(closed, `run subscription ${index} on ${threadId}`);
      return closed!;
    },
  };
};

const providerTurnId = (turn: Turn) => ProviderTurnId.make(`provider-turn:${turn.turn.attemptId}`);

/**
 * A provider that opens one events queue per session and starts each turn as running; a turn then
 * reports only what the test offers. Claude's capabilities give each thread its own process,
 * Codex's put every thread on one.
 */
const makeScriptedProvider = (
  cwd: string,
  capabilities: typeof ClaudeProviderCapabilitiesV2 | typeof CodexProviderCapabilitiesV2,
) => {
  const turns: Array<Turn> = [];
  const started = new Map<string, Deferred.Deferred<Turn>>();
  const startedOn = (threadId: ThreadId) => {
    const existing = started.get(threadId);
    if (existing !== undefined) return existing;
    const deferred = Deferred.makeUnsafe<Turn>();
    started.set(threadId, deferred);
    return deferred;
  };
  const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(capabilities),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (input) =>
      Effect.gen(function* () {
        const now = yield* DateTime.now;
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        return {
          instanceId,
          driver,
          providerSessionId: input.providerSessionId,
          providerSession: {
            id: input.providerSessionId,
            driver,
            providerInstanceId: instanceId,
            status: "ready",
            cwd,
            model: modelSelection.model,
            capabilities,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(events),
          ensureThread: ({ threadId }) =>
            Effect.succeed({
              id: ProviderThreadId.make(`provider-thread:${threadId}`),
              driver,
              providerInstanceId: instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: threadId,
              ownerNodeId: null,
              nativeThreadRef: { driver, nativeId: `native:${threadId}`, strength: "strong" },
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            }),
          resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
          startTurn: (turn) =>
            Effect.gen(function* () {
              const entry = { turn, events };
              turns.push(entry);
              yield* Queue.offer(events, providerTurnEvent(entry, "running", yield* DateTime.now));
              const first = startedOn(turn.threadId);
              if (!(yield* Deferred.isDone(first))) yield* Deferred.succeed(first, entry);
            }),
          steerTurn: () => Effect.die("unused"),
          interruptTurn: ({ providerTurnId: interrupted }) =>
            Effect.gen(function* () {
              const entry = turns.find((candidate) => providerTurnId(candidate) === interrupted);
              if (entry === undefined) return;
              yield* Queue.offer(
                entry.events,
                providerTurnEvent(entry, "interrupted", yield* DateTime.now),
              );
              yield* Queue.offer(entry.events, terminalEvent(entry, "interrupted"));
            }),
          respondToRuntimeRequest: () => Effect.die("unused"),
          readThreadSnapshot: () => Effect.die("unused"),
          rollbackThread: () => Effect.die("unused"),
          forkThread: () => Effect.die("unused"),
        };
      }),
  };
  /** The first turn this provider started on the thread. */
  const firstTurn = (threadId: ThreadId) => Deferred.await(startedOn(threadId));
  return { adapter, turns, firstTurn };
};

/** Starts a parent's first run, and from it a delegated task whose child runs on its own. */
const startDelegatingParent = Effect.fnUntraced(function* (input: {
  readonly name: string;
  readonly cwd: string;
  readonly turns: ReadonlyArray<Turn>;
  readonly delegate: boolean;
}) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const parentThreadId = ThreadId.make(`thread:${input.name}-parent`);
  const parentRunning = yield* turnRunning();
  yield* orchestrator.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`${input.name}-create-parent`),
    threadId: parentThreadId,
    projectId: ProjectId.make(`project:${input.name}`),
    title: "Parent",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: input.cwd,
    createdBy: "user",
    creationSource: "web",
  });
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make(`${input.name}-start-parent`),
    threadId: parentThreadId,
    messageId: MessageId.make(`message:${input.name}-start-parent`),
    text: "Delegate the deploy",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  yield* worker.drain();
  yield* Fiber.join(parentRunning);
  const parent = input.turns.find((entry) => entry.turn.threadId === parentThreadId)!;
  if (!input.delegate) return { parentThreadId, parent, child: undefined };
  const parentRun = (yield* orchestrator.getThreadProjection(parentThreadId)).runs[0]!;
  const childRunning = yield* turnRunning();
  yield* orchestrator.dispatch({
    type: "delegated_task.request",
    commandId: CommandId.make(`${input.name}-delegate`),
    parentThreadId,
    parentRunId: parentRun.id,
    parentNodeId: parentRun.rootNodeId!,
    task: "Deploy and report back",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    completionWake: "always",
    createdBy: "agent",
    creationSource: "mcp",
  });
  yield* worker.drain();
  yield* Fiber.join(childRunning);
  const child = input.turns.find((entry) => entry.turn.threadId !== parentThreadId)!;
  return { parentThreadId, parent, child };
});

/**
 * A Claude parent on a provider with one process per thread, as Claude runs in production,
 * whose first run is running and, unless `delegate` is false, has delegated a task.
 */
const claudeParent = <A, E, R>(
  name: string,
  options: { readonly delegate: boolean },
  body: (input: {
    readonly turns: ReadonlyArray<Turn>;
    readonly parentThreadId: ThreadId;
    readonly parent: Turn;
    readonly child: Turn | undefined;
    readonly subscription: (threadId: ThreadId, index: number) => Deferred.Deferred<void>;
    readonly warnings: ReadonlyArray<string>;
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace(name);
    const provider = makeScriptedProvider(cwd, ClaudeProviderCapabilitiesV2);
    const warnings: Array<string> = [];
    const subscriptions = makeSubscriptionLog();
    const logger = Logger.make(({ logLevel, message }) => {
      if (logLevel === "Warn") warnings.push(JSON.stringify(message));
    });
    return yield* Effect.gen(function* () {
      const started = yield* startDelegatingParent({
        name,
        cwd,
        turns: provider.turns,
        delegate: options.delegate,
      });
      return yield* body({
        ...started,
        turns: provider.turns,
        subscription: subscriptions.subscription,
        warnings,
      });
    }).pipe(
      Effect.provide(
        Layer.merge(
          ProviderReplayHarness.layerWithRegistry(
            { name },
            ProviderAdapterRegistry.layerSingle(provider.adapter),
            { runEffectWorker: false },
          ),
          Logger.layer([logger], { mergeWithExisting: false }),
        ),
      ),
      Effect.provideService(CurrentWaitObserver, subscriptions.observer),
    );
  });

// Read the cursor before forking: events offered right after a watch can commit before a
// live-tail subscriber has read its starting sequence.
const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const afterSequence = yield* eventSink.latestSequence();
    return yield* eventSink.stream({ afterSequence }).pipe(
      Stream.map((stored) => stored.event),
      Stream.filter(predicate),
      Stream.take(1),
      Stream.runDrain,
      Effect.forkScoped,
    );
  });

const turnRunning = () =>
  watch((event) => event.type === "provider-turn.updated" && event.payload.status === "running");

/** The run left its turn: ended, failed, or waiting for its checkpoint. */
const runEnded = (runId: RunId) =>
  watch(
    (event) =>
      event.type === "run.updated" &&
      event.payload.id === runId &&
      event.payload.status !== "queued" &&
      event.payload.status !== "preparing" &&
      event.payload.status !== "starting" &&
      event.payload.status !== "running",
  );

/** The parent's delegated task settled. */
const taskSettled = (parentThreadId: ThreadId) =>
  watch(
    (event) =>
      event.type === "subagent.updated" &&
      event.threadId === parentThreadId &&
      event.payload.origin === "app_owned" &&
      ["completed", "failed", "cancelled", "interrupted"].includes(event.payload.status),
  );

const itemStored = (itemId: TurnItemId) =>
  watch((event) => event.type === "turn-item.updated" && event.payload.id === itemId);

function providerTurnEvent(
  entry: Turn,
  status: "running" | "completed" | "failed" | "interrupted",
  at: DateTime.Utc,
): ProviderAdapter.ProviderAdapterV2Event {
  return {
    type: "provider_turn.updated",
    driver,
    providerTurn: {
      id: providerTurnId(entry),
      providerThreadId: entry.turn.providerThread.id,
      nodeId: entry.turn.rootNodeId,
      runAttemptId: entry.turn.attemptId,
      nativeTurnRef: null,
      ordinal: entry.turn.providerTurnOrdinal,
      status,
      startedAt: at,
      completedAt: status === "running" ? null : at,
    },
  };
}

function terminalEvent(
  entry: Turn,
  status: "completed" | "failed" | "interrupted",
): ProviderAdapter.ProviderAdapterV2Event {
  const ended = {
    type: "turn.terminal",
    driver,
    providerThreadId: entry.turn.providerThread.id,
    providerTurnId: providerTurnId(entry),
    runOrdinal: entry.turn.runOrdinal,
    threadDisposition: "reusable",
  } as const;
  return status === "failed"
    ? {
        ...ended,
        status,
        failure: makeProviderFailure({ message: "Provider overloaded", class: "provider_error" }),
        failureItemOrdinal: 100,
      }
    : { ...ended, status, failure: null };
}

const endTurn = Effect.fnUntraced(function* (entry: Turn, reply: string) {
  const now = yield* DateTime.now;
  const messageId = MessageId.make(`message:reply:${entry.turn.runId}`);
  const at = { threadId: entry.turn.threadId, runId: entry.turn.runId };
  for (const event of [
    {
      type: "message.updated",
      driver,
      message: {
        ...at,
        id: messageId,
        nodeId: entry.turn.rootNodeId,
        role: "assistant",
        text: reply,
        attachments: [],
        streaming: false,
        createdBy: "agent",
        creationSource: "provider",
        createdAt: now,
        updatedAt: now,
      },
    },
    {
      type: "turn_item.updated",
      driver,
      turnItem: {
        ...at,
        id: TurnItemId.make(`turn-item:reply:${entry.turn.runId}`),
        nodeId: entry.turn.rootNodeId,
        providerThreadId: entry.turn.providerThread.id,
        providerTurnId: providerTurnId(entry),
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 50,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "assistant_message",
        messageId,
        text: reply,
        streaming: false,
      },
    },
    providerTurnEvent(entry, "completed", now),
    terminalEvent(entry, "completed"),
  ] satisfies ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>) {
    yield* Queue.offer(entry.events, event);
  }
});

/** A command the provider runs in the background of a turn, as Claude reports its tool calls. */
const reportCommand = Effect.fnUntraced(function* (
  entry: Turn,
  id: TurnItemId,
  status: "running" | "completed",
  ordinal = 10,
) {
  const now = yield* DateTime.now;
  yield* Queue.offer(entry.events, {
    type: "turn_item.updated",
    driver,
    turnItem: {
      id,
      threadId: entry.turn.threadId,
      runId: entry.turn.runId,
      nodeId: entry.turn.rootNodeId,
      providerThreadId: entry.turn.providerThread.id,
      providerTurnId: providerTurnId(entry),
      nativeItemRef: { driver, nativeId: id, strength: "strong" },
      parentItemId: null,
      ordinal,
      status,
      title: "Watch the deploy log",
      startedAt: now,
      completedAt: status === "running" ? null : now,
      updatedAt: now,
      type: "command_execution",
      input: "tail -f deploy.log",
      ...(status === "running" ? {} : { output: "done" }),
    },
  });
});

/**
 * Whether the finished run still ingests its session's events: a late item of the run reaches
 * the store only through the run's own subscription.
 */
const finishedRunIngestion = Effect.fnUntraced(function* (
  entry: Turn,
  subscriptionClosed: Deferred.Deferred<void>,
  probe: string,
) {
  const probeId = TurnItemId.make(`turn-item:probe:${probe}`);
  const probeStored = yield* itemStored(probeId);
  yield* reportCommand(entry, probeId, "completed", 90);
  return yield* Effect.raceFirst(
    Deferred.await(subscriptionClosed).pipe(Effect.as("closed" as const)),
    Fiber.join(probeStored).pipe(Effect.as("still ingesting" as const)),
  );
});

/** The parent's first run fails with its delegated task still running, as a provider error does. */
const failParentRun = Effect.fnUntraced(function* (parent: Turn) {
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const ended = yield* runEnded(parent.turn.runId);
  yield* Queue.offer(parent.events, providerTurnEvent(parent, "failed", yield* DateTime.now));
  yield* Queue.offer(parent.events, terminalEvent(parent, "failed"));
  yield* Fiber.join(ended);
  yield* worker.drain();
});

const completeParentRun = Effect.fnUntraced(function* (parent: Turn) {
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const ended = yield* runEnded(parent.turn.runId);
  yield* endTurn(parent, "Delegated.");
  yield* Fiber.join(ended);
  yield* worker.drain();
});

/** A person writes to the parent again; the run starts on the parent's own process. */
const sendToParent = Effect.fnUntraced(function* (
  turns: ReadonlyArray<Turn>,
  parentThreadId: ThreadId,
  key: string,
) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
  const running = yield* turnRunning();
  yield* orchestrator.dispatch({
    type: "message.dispatch",
    commandId: CommandId.make(`${key}-message`),
    threadId: parentThreadId,
    messageId: MessageId.make(`message:${key}`),
    text: "How is the deploy going?",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "web",
  });
  yield* worker.drain();
  yield* Fiber.join(running);
  const next = turns.at(-1)!;
  assert.equal(next.turn.threadId, parentThreadId);
  return next;
});

const runStatus = Effect.fnUntraced(function* (threadId: ThreadId, runId: RunId) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  return (yield* orchestrator.getThreadProjection(threadId)).runs.find((run) => run.id === runId)
    ?.status;
});

const stopTaskFromList = Effect.fnUntraced(function* (parentThreadId: ThreadId, child: Turn) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const task = (yield* orchestrator.getThreadProjection(parentThreadId)).subagents.find(
    (candidate) => candidate.origin === "app_owned",
  )!;
  // What the client sends when a person stops a delegated task from the parent's list.
  yield* orchestrator.dispatch({
    type: "subagent.stop",
    commandId: CommandId.make("stop-task-from-list"),
    threadId: parentThreadId,
    runId: task.runId!,
    subagentId: task.id,
  });
  yield* orchestrator.dispatch({
    type: "thread.stop",
    commandId: CommandId.make("stop-task-child"),
    threadId: child.turn.threadId,
  });
});

const childFinishes = Effect.fnUntraced(function* (child: Turn) {
  const ended = yield* runEnded(child.turn.runId);
  yield* endTurn(child, "Deployed.");
  yield* Fiber.join(ended);
});

// Each run started while an earlier failed run's delegated task ran inherited the task as
// background work. Only a provider event ends inherited work, and a delegated task's child, not
// the provider, settles it: the finished run ingested every event on its process until release.
it.effect.each([
  { name: "the child finishes it", settle: "child" },
  { name: "it is stopped from the parent's background-task list", settle: "stopped" },
] as const)(
  "a finished run closes its subscription although an earlier failed run's delegated task was running ($name)",
  ({ settle }) =>
    claudeParent(
      `inherited-delegation-${settle}`,
      { delegate: true },
      Effect.fnUntraced(function* ({ turns, parentThreadId, parent, child, subscription }) {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        yield* failParentRun(parent);
        const next = yield* sendToParent(turns, parentThreadId, `${settle}-next`);
        const nextEnded = yield* runEnded(next.turn.runId);
        yield* endTurn(next, "Still deploying.");
        yield* Fiber.join(nextEnded);
        yield* worker.drain();

        const settled = yield* taskSettled(parentThreadId);
        yield* settle === "child"
          ? childFinishes(child!)
          : stopTaskFromList(parentThreadId, child!);
        yield* worker.drain();
        yield* Fiber.join(settled);
        yield* worker.drain();

        assert.equal(yield* runStatus(parentThreadId, next.turn.runId), "completed");
        assert.equal(
          yield* finishedRunIngestion(next, subscription(parentThreadId, 1), settle),
          "closed",
        );
      }),
    ),
);

// Restart recovery cancels the runs a restart cut and leaves their delegated tasks running, since
// each child continues on its own, so each parent's restart continuation inherited its cut run's
// task. On Codex every thread shares one process, and each leak took every thread's events.
it.effect(
  "a restart continuation closes its subscription although the cut run's delegated task was running",
  () =>
    Effect.gen(function* () {
      const name = "restart-inherited-delegation";
      const cwd = yield* checkpointWorkspace(name);
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const tempDir = yield* Effect.acquireRelease(
        fs.makeTempDirectory({ prefix: "t3-finished-run-ingestion-" }),
        (directory) => fs.remove(directory, { recursive: true, force: true }).pipe(Effect.orDie),
      );
      const database = (file: string) =>
        SqlitePersistence.layerFromPath(path.join(tempDir, file)).pipe(
          Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        );
      const crashPath = path.join(tempDir, "crash.sqlite");
      const state = database("state.sqlite");

      // The parent's run and its delegated child are running when the server stops.
      const before = makeScriptedProvider(cwd, CodexProviderCapabilitiesV2);
      const parentThreadId = yield* Effect.scoped(
        Effect.gen(function* () {
          const started = yield* startDelegatingParent({
            name,
            cwd,
            turns: before.turns,
            delegate: true,
          });
          const sql = yield* SqlClient.SqlClient;
          yield* sql`VACUUM INTO ${crashPath}`;
          return started.parentThreadId;
        }).pipe(
          Effect.provide(
            Layer.merge(
              ProviderReplayHarness.layerWithRegistry(
                { name },
                ProviderAdapterRegistry.layerSingle(before.adapter),
                { runEffectWorker: false, databaseLayer: state },
              ),
              state,
            ),
          ),
        ),
      );

      const after = makeScriptedProvider(cwd, CodexProviderCapabilitiesV2);
      const subscriptions = makeSubscriptionLog();
      yield* Effect.scoped(
        Effect.gen(function* () {
          const orchestrator = yield* Orchestrator.OrchestratorV2;
          const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
          const continuation = yield* after.firstTurn(parentThreadId);
          const cut = (yield* orchestrator.getThreadProjection(parentThreadId)).runs.find(
            (run) => run.id === continuation.turn.runId,
          )?.restartContinuationOfRunId;
          assert.isDefined(cut);
          assert.equal(yield* runStatus(parentThreadId, cut!), "cancelled");
          const childThreadId = before.turns.find(
            (entry) => entry.turn.threadId !== parentThreadId,
          )!.turn.threadId;
          const childContinuation = yield* after.firstTurn(childThreadId);

          const continuationEnded = yield* runEnded(continuation.turn.runId);
          yield* endTurn(continuation, "Resumed; the deploy is still running.");
          yield* Fiber.join(continuationEnded);
          const settled = yield* taskSettled(parentThreadId);
          yield* childFinishes(childContinuation);
          yield* worker.drain();
          yield* Fiber.join(settled);
          yield* worker.drain();

          assert.equal(yield* runStatus(parentThreadId, continuation.turn.runId), "completed");
          assert.equal(
            yield* finishedRunIngestion(
              continuation,
              subscriptions.subscription(parentThreadId, 0),
              "restart",
            ),
            "closed",
          );
        }).pipe(
          Effect.provide(
            ProviderReplayHarness.layerWithRegistry(
              { name },
              ProviderAdapterRegistry.layerSingle(after.adapter),
              {
                databaseLayer: database("crash.sqlite"),
                recoverOnStartup: true,
                continueThreadsAfterServerUpdate: true,
              },
            ),
          ),
          Effect.provideService(CurrentWaitObserver, subscriptions.observer),
        ),
      );
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

// Guard: work the provider still runs holds the finished run's subscription until the provider
// reports it ended, whether the run started it or inherited it from a failed run.
it.effect.each([
  { name: "its own", inherited: false },
  { name: "an earlier failed run's", inherited: true },
] as const)(
  "a finished run keeps ingesting while $name background command runs, and stops when it ends",
  ({ inherited }) =>
    claudeParent(
      inherited ? "inherited-command" : "own-command",
      { delegate: false },
      Effect.fnUntraced(function* ({ turns, parentThreadId, parent, subscription }) {
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const commandId = TurnItemId.make("turn-item:deploy-log");
        if (inherited) {
          yield* reportCommand(parent, commandId, "running");
          yield* failParentRun(parent);
        } else {
          yield* completeParentRun(parent);
        }
        const next = yield* sendToParent(turns, parentThreadId, "command-next");
        const nextEnded = yield* runEnded(next.turn.runId);
        if (!inherited) yield* reportCommand(next, commandId, "running");
        yield* endTurn(next, "Watching the deploy log.");
        yield* Fiber.join(nextEnded);
        yield* worker.drain();

        const nextSubscription = subscription(parentThreadId, 1);
        assert.equal(
          yield* finishedRunIngestion(next, nextSubscription, "command-running"),
          "still ingesting",
        );
        yield* reportCommand(inherited ? parent : next, commandId, "completed");
        yield* Deferred.await(nextSubscription);
      }),
    ),
);

// When T3 itself ends a finished run's stream, as an idle release of its process does, the run
// has nothing left to write, yet each one logged an ingestion failure.
it.effect("a finished run whose session T3 releases logs no ingestion failure", () =>
  claudeParent(
    "released-finished-run",
    { delegate: false },
    Effect.fnUntraced(function* ({ turns, parentThreadId, parent, subscription, warnings }) {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const sessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      yield* completeParentRun(parent);
      const next = yield* sendToParent(turns, parentThreadId, "released-next");
      const nextEnded = yield* runEnded(next.turn.runId);
      yield* reportCommand(next, TurnItemId.make("turn-item:dev-server"), "running");
      yield* endTurn(next, "Left the dev server running.");
      yield* Fiber.join(nextEnded);
      yield* worker.drain();

      const session = (yield* orchestrator.getThreadProjection(parentThreadId))
        .providerSessions[0]!;
      yield* sessions.release({ providerSessionId: session.id, reason: "idle_timeout" });
      yield* Deferred.await(subscription(parentThreadId, 1));

      assert.equal(yield* runStatus(parentThreadId, next.turn.runId), "completed");
      assert.deepEqual(
        warnings.filter((warning) => warning.includes(INGESTION_FAILED)),
        [],
      );
    }),
  ),
);

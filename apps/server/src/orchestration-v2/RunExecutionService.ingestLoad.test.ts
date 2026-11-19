import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  CommandId,
  MessageId,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2StoredEvent,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as McpAppModelContext from "../mcpApps/McpAppModelContext.ts";
import * as PreviewAgentActivity from "../preview/PreviewAgentActivity.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as EventSink from "./EventSink.ts";
import * as ProviderCommitBatch from "./ProviderCommitBatch.ts";
import * as ProviderEventIngestor from "./ProviderEventIngestor.ts";
import * as RunExecutionService from "./RunExecutionService.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import * as TurnItemPositionStore from "./TurnItemPositionStore.ts";

// Eight Claude threads, four of them writing long answers, share one SQLite writer. Each statement
// costs what an event-loop-saturated server pays for one, so the writer stays busy.
const THREADS = 8;
const HOT_THREADS = 4;
const SECONDS = 90;
const WARMUP_SECONDS = 30;
const STATEMENT_MS = 7;
const SAVEPOINT_MS = 5;
const WRITE_STATEMENTS = 7;
const EVENT_MS = 0.4;
const OTHER_WRITES_PER_MINUTE = 320;
// The adapter coalesces streamed text every 50 ms.
const STEP_MS = 50;

const driver = ProviderDriverKind.make("claudeAgent");
const providerInstanceId = ProviderInstanceId.make("claude");
const InSharedCommit = Context.Reference<boolean>("test/InSharedCommit", {
  defaultValue: () => false,
});

function seeded(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

interface Ids {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly attemptId: RunAttemptId;
  readonly providerThreadId: ProviderThreadId;
  readonly rootProviderTurnId: ProviderTurnId;
  readonly rootNodeId: NodeId;
}

const at = (ms: number) => DateTime.makeUnsafe(ms);
const event = (value: unknown) => value as ProviderAdapter.ProviderAdapterV2Event;

/** A Claude turn on the provider's clock: requests of thinking, streamed text and a tool call. */
function claudeTurn(ids: Ids, hot: boolean, random: () => number, start: number) {
  const uniform = (lo: number, hi: number) => lo + (hi - lo) * random();
  const events: Array<{
    readonly at: number;
    readonly event: ProviderAdapter.ProviderAdapterV2Event;
  }> = [];
  let now = start;
  const push = (value: unknown) => events.push({ at: now, event: event(value) });
  const providerTurn = (status: "running" | "completed") => ({
    type: "provider_turn.updated",
    driver,
    threadId: ids.threadId,
    providerTurn: {
      id: ids.rootProviderTurnId,
      providerThreadId: ids.providerThreadId,
      nodeId: ids.rootNodeId,
      runAttemptId: ids.attemptId,
      nativeTurnRef: null,
      ordinal: 1,
      status,
      startedAt: at(start),
      completedAt: status === "running" ? null : at(now),
    },
  });
  const item = (value: Record<string, unknown>) => ({
    type: "turn_item.updated",
    driver,
    turnItem: {
      threadId: ids.threadId,
      runId: ids.runId,
      nodeId: ids.rootNodeId,
      providerThreadId: ids.providerThreadId,
      providerTurnId: ids.rootProviderTurnId,
      nativeItemRef: null,
      parentItemId: null,
      updatedAt: at(now),
      ...value,
    },
  });
  push(providerTurn("running"));
  const requests = 2 + Math.floor(random() * (hot ? 4 : 7));
  let ordinal = 100;
  for (let request = 0; request < requests; request++) {
    if (request > 0) push(providerTurn("running"));
    const thinkingSteps = Math.floor(((hot ? uniform(2, 8) : uniform(0, 2)) * 1000) / STEP_MS);
    const thinkingId = TurnItemId.make(`reasoning:${ids.runId}:${request}`);
    const thinkingOrdinal = ++ordinal;
    const thinkingStart = now;
    for (let step = 0, text = ""; step <= thinkingSteps; step++) {
      const final = step === thinkingSteps;
      if (!final) text += "thinking about it. ";
      push(
        item({
          id: thinkingId,
          ordinal: thinkingOrdinal,
          type: "reasoning",
          title: "Thinking",
          text,
          streaming: !final,
          status: final ? "completed" : "running",
          startedAt: at(thinkingStart),
          completedAt: final ? at(now) : null,
        }),
      );
      if (!final) now += STEP_MS;
    }
    const nativeId = `${ids.runId}:text:${request}`;
    const nodeId = NodeId.make(`node:${nativeId}`);
    const messageId = MessageId.make(`message:${nativeId}`);
    const textOrdinal = ++ordinal;
    const textStart = now;
    const textSeconds = hot ? uniform(10, 40) : uniform(0.3, 2);
    const textSteps = Math.max(1, Math.floor((textSeconds * 1000) / STEP_MS));
    for (let step = 0, text = ""; step <= textSteps; step++) {
      const final = step === textSteps;
      if (!final) text += step % 25 === 24 ? "word word.\n\n" : "word word ";
      const status = final ? "completed" : "running";
      push({
        type: "node.updated",
        driver,
        node: {
          id: nodeId,
          threadId: ids.threadId,
          runId: ids.runId,
          parentNodeId: ids.rootNodeId,
          rootNodeId: ids.rootNodeId,
          kind: "assistant_message",
          status,
          countsForRun: false,
          providerThreadId: ids.providerThreadId,
          providerTurnId: ids.rootProviderTurnId,
          nativeItemRef: null,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: at(textStart),
          completedAt: final ? at(now) : null,
        },
      });
      push({
        type: "message.updated",
        driver,
        message: {
          createdBy: "agent",
          creationSource: "provider",
          id: messageId,
          threadId: ids.threadId,
          runId: ids.runId,
          nodeId,
          role: "assistant",
          text,
          attachments: [],
          streaming: !final,
          createdAt: at(textStart),
          updatedAt: at(now),
        },
      });
      push(
        item({
          id: TurnItemId.make(`turn-item:${nativeId}`),
          nodeId,
          ordinal: textOrdinal,
          type: "assistant_message",
          messageId,
          text,
          streaming: !final,
          status,
          startedAt: at(textStart),
          completedAt: final ? at(now) : null,
        }),
      );
      if (!final) now += STEP_MS;
    }
    push(providerTurn("running"));
    if (request === requests - 1) break;
    const toolId = TurnItemId.make(`tool:${ids.runId}:${request}`);
    const toolOrdinal = ++ordinal;
    const toolStart = now;
    const tool = (status: "running" | "completed") =>
      item({
        id: toolId,
        ordinal: toolOrdinal,
        type: "dynamic_tool",
        title: "Tool",
        status,
        startedAt: at(toolStart),
        completedAt: status === "running" ? null : at(now),
      });
    push(tool("running"));
    now += (hot ? uniform(1, 6) : uniform(5, 40)) * 1000;
    push(tool("completed"));
  }
  push(providerTurn("completed"));
  push({
    type: "provider_thread.updated",
    driver,
    providerThread: {
      id: ids.providerThreadId,
      driver,
      providerInstanceId,
      appThreadId: ids.threadId,
      status: "running",
    },
  });
  push({
    type: "turn.terminal",
    driver,
    providerThreadId: ids.providerThreadId,
    providerTurnId: ids.rootProviderTurnId,
    runOrdinal: 1,
    threadDisposition: "reusable",
    status: "completed",
    failure: null,
  });
  return events;
}

const percentile = (values: ReadonlyArray<number>, q: number) =>
  values.toSorted((a, b) => a - b)[Math.min(values.length - 1, Math.floor(q * values.length))]!;

it.effect(
  "keeps a long streamed answer's commits within seconds of the provider while the shared writer is busy",
  () =>
    Effect.gen(function* () {
      const writer = Semaphore.makeUnsafe(1);
      const start = yield* Clock.currentTimeMillis;
      const windowStart = start + WARMUP_SECONDS * 1000;
      // Adapter stamp to commit, in seconds, of the hot threads' assistant messages.
      const hotLags: Array<number> = [];
      let pendingInCommit: Array<number> = [];
      let busyMs = 0;
      const hold = (ms: number) =>
        Effect.gen(function* () {
          const from = yield* Clock.currentTimeMillis;
          yield* Effect.sleep(ms);
          busyMs += Math.max(0, (yield* Clock.currentTimeMillis) - Math.max(from, windowStart));
        });
      const recordCommit = (stamps: ReadonlyArray<number>) =>
        Effect.map(Clock.currentTimeMillis, (committedAt) => {
          if (committedAt < windowStart) return;
          for (const stamp of stamps) hotLags.push((committedAt - stamp) / 1000);
        });
      const coordinator = yield* ProviderCommitBatch.make(
        (work) =>
          writer.withPermits(1)(
            Effect.gen(function* () {
              yield* hold(STATEMENT_MS);
              yield* Effect.provideService(work, InSharedCommit, true);
              yield* hold(STATEMENT_MS);
              const committed = pendingInCommit;
              pendingInCommit = [];
              yield* recordCommit(committed);
            }),
          ),
        true,
        // The model's lane costs were taken with a savepoint per job.
        {
          ...ProviderCommitBatch.noIsolation,
          segment: (work) => hold(SAVEPOINT_MS).pipe(Effect.andThen(work)),
          perJobSavepoints: true,
        },
      );
      const write = (hotStamp: number | undefined) =>
        Effect.gen(function* () {
          if (yield* InSharedCommit) {
            yield* hold(WRITE_STATEMENTS * STATEMENT_MS);
            if (hotStamp !== undefined) pendingInCommit.push(hotStamp);
            return;
          }
          yield* writer.withPermits(1)(
            hold((WRITE_STATEMENTS + 2) * STATEMENT_MS).pipe(
              Effect.andThen(recordCommit(hotStamp === undefined ? [] : [hotStamp])),
            ),
          );
        });
      const isHot = (threadId: string) => Number(threadId.slice("thread:".length)) < HOT_THREADS;
      const layer = RunExecutionService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            McpAppModelContext.layerEmpty,
            PreviewAgentActivity.layer,
            Layer.mock(CheckpointService.CheckpointServiceV2)({
              captureBaseline: () => Effect.void,
            }),
            Layer.mock(TurnItemPositionStore.TurnItemPositionStoreV2)({
              allocate: () => Effect.succeed(1),
            }),
            Layer.mock(EventSink.EventSinkV2)({
              consumeProviderEvents: (source, process, after, canBatch) =>
                coordinator.consume(
                  source,
                  (provided) =>
                    Effect.gen(function* () {
                      if (yield* InSharedCommit) yield* hold(EVENT_MS);
                      else yield* writer.withPermits(1)(hold(EVENT_MS));
                      return yield* process(provided);
                    }),
                  after,
                  canBatch,
                ),
              write: () => coordinator.barrier(write(undefined)).pipe(Effect.as([])),
              writeWithEffects: () => coordinator.barrier(write(undefined)).pipe(Effect.as([])),
              writeIfRunCurrent: () =>
                coordinator
                  .barrier(write(undefined))
                  .pipe(Effect.as({ committed: true, storedEvents: [] })),
            }),
            IdAllocator.layer,
            ThreadCommandExecutor.layer,
            Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
              ingestNormalized: (input) =>
                coordinator
                  .barrier(
                    write(
                      input.event.type === "message.updated" && isHot(input.threadId)
                        ? DateTime.toEpochMillis(input.event.message.updatedAt)
                        : undefined,
                    ),
                  )
                  .pipe(
                    Effect.as(
                      input.event.type === "provider_thread.updated"
                        ? [{} as OrchestrationV2StoredEvent]
                        : [],
                    ),
                  ),
            }),
            ServerSettings.layerTest(),
          ),
        ),
      );
      yield* Effect.gen(function* () {
        const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
        const end = start + SECONDS * 1000;
        // A thread's next turn starts a few seconds after T3 has settled its last one.
        const runTurns = (
          thread: number,
          random: () => number,
          run: number,
        ): Effect.Effect<void, RunExecutionService.RunExecutionServiceV2Error> =>
          Effect.gen(function* () {
            if ((yield* Clock.currentTimeMillis) >= end) return;
            {
              const key = `${thread}:${run}`;
              const ids: Ids = {
                threadId: ThreadId.make(`thread:${thread}`),
                runId: RunId.make(`run:${key}`),
                attemptId: RunAttemptId.make(`attempt:${key}`),
                providerThreadId: ProviderThreadId.make(`provider-thread:${thread}`),
                rootProviderTurnId: ProviderTurnId.make(`provider-turn:${key}`),
                rootNodeId: NodeId.make(`node:${key}`),
              };
              const turn = claudeTurn(
                ids,
                thread < HOT_THREADS,
                random,
                yield* Clock.currentTimeMillis,
              );
              const source = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
              const closed = yield* Deferred.make<void>();
              // The provider runs on its own clock, whatever T3 has ingested.
              yield* Effect.gen(function* () {
                for (const next of turn) {
                  const now = yield* Clock.currentTimeMillis;
                  if (next.at > now) yield* Effect.sleep(next.at - now);
                  yield* Queue.offer(source, next.event);
                }
              }).pipe(Effect.forkChild);
              yield* runExecution.startRootRun({
                commandId: CommandId.make(`command:${key}`),
                appThread: {
                  id: ids.threadId,
                  lineage: {
                    parentThreadId: null,
                    relationshipToParent: null,
                    rootThreadId: ids.threadId,
                  },
                } as OrchestrationV2AppThread,
                providerSessionId: ProviderSessionId.make(`session:${thread}`),
                session: {
                  events: Stream.empty,
                  subscribeEvents: Effect.succeed({
                    events: Stream.fromQueue(source),
                    close: Deferred.succeed(closed, undefined),
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
                providerTurnOrdinal: 1,
                message: {
                  messageId: MessageId.make(`message:${key}:user`),
                  text: "Continue.",
                  attachments: [],
                  createdBy: "user",
                  creationSource: "web",
                },
                modelSelection: { instanceId: providerInstanceId, model: "claude" },
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
              yield* Deferred.await(closed);
            }
            yield* Effect.sleep(Math.floor(2000 + random() * 8000));
            yield* runTurns(thread, random, run + 1);
          });
        const runThread = (thread: number) =>
          Effect.suspend(() => {
            const random = seeded(1000 + thread);
            return Effect.sleep(Math.floor(random() * 20_000)).pipe(
              Effect.andThen(runTurns(thread, random, 0)),
            );
          });
        const threads = yield* Effect.forEach(
          Array.from({ length: THREADS }, (_, thread) => thread),
          (thread) => runThread(thread).pipe(Effect.forkChild),
        );
        // Commands, run lifecycle and effects of other threads queue on the same writer.
        const random = seeded(7919);
        yield* Effect.gen(function* () {
          for (;;) {
            yield* Effect.sleep(
              Math.max(1, Math.floor((-Math.log(1 - random()) * 60_000) / OTHER_WRITES_PER_MINUTE)),
            );
            yield* coordinator.barrier(write(undefined)).pipe(Effect.forkChild);
          }
        }).pipe(Effect.forkChild);
        yield* TestClock.adjust(`${SECONDS} seconds`);
        yield* Fiber.interruptAll(threads);
      }).pipe(Effect.provide(layer), Effect.scoped);

      const windowMs = (yield* Clock.currentTimeMillis) - windowStart;
      assert.isAtLeast(busyMs / windowMs, 0.95, "the shared writer is saturated");
      assert.isAbove(hotLags.length, 0);
      // At one commit a delta, a saturated writer committed assistant text minutes after the adapter
      // stamped it: a backlog was ingested a 50 ms step at a time, each one paying a writer pass.
      assert.isAtMost(percentile(hotLags, 0.9), 5, "p90 adapter-to-commit lag of the long answers");
    }),
);

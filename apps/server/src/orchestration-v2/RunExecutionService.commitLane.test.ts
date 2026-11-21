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

// `commit-lane-saturation`, write volume: a busy server's lane. 60 runs write snapshots in an
// average minute, about 31 lane jobs per run-minute (p50 22, p90 64), about 1,870 jobs/min. A batch
// costs about 25 ms plus 21 ms per job; barrier-only writes come at about 330/min and 50 ms each.
// The lane is close to fully busy.
const RUNS = 60;
const JOBS_PER_RUN_MINUTE_P50 = 22;
// Log-normal spread: p90 = p50 * e^(1.2816 sigma) = 64, mean 31.1.
const JOBS_PER_RUN_MINUTE_SIGMA = 0.833;
const JOBS_PER_RUN_MINUTE_MAX = 170;
const JOBS_PER_RUN_MINUTE_MEAN = 31.1;
const BATCH_MS = 25;
const SAVEPOINT_MS = 4;
const WRITE_MS = 17;
const JOB_MS = SAVEPOINT_MS + WRITE_MS;
const BARRIERS_PER_MINUTE = 330;
const BARRIER_MS = 50;
// The share of lane jobs that a later snapshot of the same item supersedes in the same lane batch
// with no control between: about a third. It varies by provider and by kind: running usage reports
// most often, then tool nodes and command items, then reasoning; streamed assistant text seldom.
const SUPERSEDE_SHARE = Number(process.env.T3_LANE_SUPERSEDE_SHARE ?? 0.34);
// Usage reports per run come seconds apart, so none supersedes another inside a backlog here.
const USAGE_SHARE = 0.06;
// Of the items, tool calls (node + turn item per state) and streamed texts (message + turn item).
const TEXT_ITEM_SHARE = 0.17;
// A run's handovers to the lane: the per-run gap between consecutive lane batches carrying its
// writes (p50 about 9 s), which is how long its events wait together.
const HANDOVER_MS = 9_000;
// An item's superseded states come this close before its next one.
const STATE_GAP_MS: readonly [number, number] = [50, 250];
const WARMUP_SECONDS = 60;
const WINDOW_SECONDS = 120;

const driver = ProviderDriverKind.make("claudeAgent");
const providerInstanceId = ProviderInstanceId.make("claude");
const InSharedCommit = Context.Reference<boolean>("test/LaneInSharedCommit", {
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

interface Scheduled {
  readonly at: number;
  readonly event: ProviderAdapter.ProviderAdapterV2Event;
  /** A later snapshot of the same item follows before any control. */
  readonly superseded: boolean;
}

const at = (ms: number) => DateTime.makeUnsafe(ms);
const event = (value: unknown) => value as ProviderAdapter.ProviderAdapterV2Event;

/** Each run's lane jobs per minute: a log-normal per-run spread, scaled to its mean. */
function runRates(random: () => number) {
  const normal = () => Math.sqrt(-2 * Math.log(1 - random())) * Math.cos(2 * Math.PI * random());
  const drawn = Array.from({ length: RUNS }, () =>
    Math.min(
      JOBS_PER_RUN_MINUTE_MAX,
      JOBS_PER_RUN_MINUTE_P50 * Math.exp(JOBS_PER_RUN_MINUTE_SIGMA * normal()),
    ),
  );
  const scale = (JOBS_PER_RUN_MINUTE_MEAN * RUNS) / drawn.reduce((sum, rate) => sum + rate, 0);
  return drawn.map((rate) => rate * scale);
}

/**
 * One run's provider stream for `durationMs`: items arrive as a Poisson process; each is a tool
 * call or a streamed text whose states (all but the last superseded) come a few hundred ms apart,
 * plus periodic usage reports.
 */
function runSchedule(
  ids: Ids,
  random: () => number,
  jobsPerMinute: number,
  start: number,
  durationMs: number,
) {
  // Superseded states per item, so that SUPERSEDE_SHARE of all jobs are superseded.
  const itemShare = SUPERSEDE_SHARE / (1 - USAGE_SHARE);
  const extraStates = itemShare / (1 - itemShare);
  const itemsPerMinute = (jobsPerMinute * (1 - USAGE_SHARE)) / (2 * (1 + extraStates));
  const usagePerMinute = jobsPerMinute * USAGE_SHARE;
  const schedule: Array<Scheduled> = [];
  const push = (ms: number, value: unknown, superseded = false) =>
    schedule.push({ at: ms, event: event(value), superseded });
  const usage = (ms: number, usedTokens: number) =>
    push(ms, {
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
        status: "running",
        startedAt: at(start),
        completedAt: null,
        tokenUsage: { usedTokens },
      },
    });
  usage(start, 1);
  let tokens = 1;
  for (
    let next = start + (-Math.log(1 - random()) * 60_000) / usagePerMinute;
    next < start + durationMs;
    next += 60_000 / usagePerMinute
  )
    usage(next, ++tokens);
  let ordinal = 100;
  for (
    let next = start + (-Math.log(1 - random()) * 60_000) / itemsPerMinute, item = 0;
    next < start + durationMs;
    next += (-Math.log(1 - random()) * 60_000) / itemsPerMinute, item++
  ) {
    const text = random() < TEXT_ITEM_SHARE;
    const floor = Math.floor(extraStates);
    const superseded = text
      ? Number(random() < Math.min(1, extraStates))
      : floor + Number(random() < extraStates - floor);
    const itemOrdinal = ++ordinal;
    const nativeId = `${ids.runId}:${item}`;
    const nodeId = NodeId.make(`node:${nativeId}`);
    const turnItemId = TurnItemId.make(`turn-item:${nativeId}`);
    const messageId = MessageId.make(`message:${nativeId}`);
    let now = next;
    for (let state = 0; state <= superseded; state++) {
      const final = state === superseded;
      const status = final ? "completed" : "running";
      if (text) {
        const value = `${"Paragraph. ".repeat(state + 1)}${final ? "Done." : "\n\n"}`;
        push(
          now,
          {
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
              text: value,
              attachments: [],
              streaming: !final,
              createdAt: at(next),
              updatedAt: at(now),
            },
          },
          !final,
        );
        push(
          now,
          {
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: turnItemId,
              threadId: ids.threadId,
              runId: ids.runId,
              nodeId,
              providerThreadId: ids.providerThreadId,
              providerTurnId: ids.rootProviderTurnId,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: itemOrdinal,
              type: "assistant_message",
              messageId,
              text: value,
              streaming: !final,
              status,
              startedAt: at(next),
              completedAt: final ? at(now) : null,
              updatedAt: at(now),
            },
          },
          !final,
        );
      } else {
        push(
          now,
          {
            type: "node.updated",
            driver,
            node: {
              id: nodeId,
              threadId: ids.threadId,
              runId: ids.runId,
              parentNodeId: ids.rootNodeId,
              rootNodeId: ids.rootNodeId,
              kind: "tool_call",
              status,
              countsForRun: false,
              providerThreadId: null,
              providerTurnId: ids.rootProviderTurnId,
              nativeItemRef: null,
              runtimeRequestId: null,
              checkpointScopeId: null,
              startedAt: at(next),
              completedAt: final ? at(now) : null,
            },
          },
          !final,
        );
        push(
          now,
          {
            type: "turn_item.updated",
            driver,
            turnItem: {
              id: turnItemId,
              threadId: ids.threadId,
              runId: ids.runId,
              nodeId,
              providerThreadId: ids.providerThreadId,
              providerTurnId: ids.rootProviderTurnId,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: itemOrdinal,
              type: "dynamic_tool",
              title: `Tool ${state}`,
              status,
              startedAt: at(next),
              completedAt: final ? at(now) : null,
              updatedAt: at(now),
            },
          },
          !final,
        );
      }
      now += STATE_GAP_MS[0] + random() * (STATE_GAP_MS[1] - STATE_GAP_MS[0]);
    }
  }
  return schedule.toSorted((left, right) => left.at - right.at);
}

/** What the run receives at each handover: everything sent since the last one. */
function handoversOf(schedule: ReadonlyArray<Scheduled>, first: number, everyMs: number) {
  const handovers: Array<{ at: number; events: Array<ProviderAdapter.ProviderAdapterV2Event> }> =
    [];
  for (const next of schedule) {
    const at = everyMs > 0 ? first + Math.ceil((next.at - first) / everyMs) * everyMs : next.at;
    const last = handovers.at(-1);
    if (last !== undefined && last.at === at) last.events.push(next.event);
    else handovers.push({ at, events: [next.event] });
  }
  return handovers;
}

const percentile = (values: ReadonlyArray<number>, q: number) =>
  values.toSorted((a, b) => a - b)[Math.min(values.length - 1, Math.floor(q * values.length))]!;

it.effect.each([
  { mode: "handovers", handoverMs: HANDOVER_MS },
  { mode: "as sent", handoverMs: 0 },
])(
  "keeps the commit lane to the item states a busy server's mix does not supersede ($mode)",
  ({ handoverMs }) =>
    Effect.gen(function* () {
      const writer = Semaphore.makeUnsafe(1);
      const start = yield* Clock.currentTimeMillis;
      const windowStart = start + WARMUP_SECONDS * 1000;
      const windowEnd = windowStart + WINDOW_SECONDS * 1000;
      const inWindow = (ms: number) => ms >= windowStart && ms < windowEnd;
      let busyMs = 0;
      let laneWrites = 0;
      let batches = 0;
      let barrierWrites = 0;
      // Provider stamp to commit, in seconds, of the items' final states (reported, not gated).
      const finalLags: Array<number> = [];
      // Items whose final state the lane has written, and states written after an item's final.
      const finalized = new Set<string>();
      let statesAfterFinal = 0;
      // The most final states sent and not yet committed at any commit in the window.
      const sentFinalStamps: Array<number> = [];
      let committedFinals = 0;
      let maxFinalBacklog = 0;
      let pendingFinals: Array<number> = [];
      const hold = (ms: number) =>
        Effect.gen(function* () {
          const from = yield* Clock.currentTimeMillis;
          yield* Effect.sleep(ms);
          const to = yield* Clock.currentTimeMillis;
          busyMs += Math.max(0, Math.min(to, windowEnd) - Math.max(from, windowStart));
        });
      const commitBatch = yield* ProviderCommitBatch.make(
        (work) =>
          writer.withPermits(1)(
            Effect.gen(function* () {
              yield* hold(BATCH_MS / 2);
              yield* Effect.provideService(work, InSharedCommit, true);
              yield* hold(BATCH_MS / 2);
              const committedAt = yield* Clock.currentTimeMillis;
              committedFinals += pendingFinals.length;
              if (inWindow(committedAt)) {
                batches++;
                for (const stamp of pendingFinals) finalLags.push((committedAt - stamp) / 1000);
                const sent = sentFinalStamps.filter((stamp) => stamp <= committedAt).length;
                maxFinalBacklog = Math.max(maxFinalBacklog, sent - committedFinals);
              }
              pendingFinals = [];
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
      const snapshotWrite = (
        finalStamp: number | undefined,
        intermediate = false,
        itemKey: string | undefined = undefined,
      ) =>
        Effect.gen(function* () {
          if (itemKey !== undefined) {
            // An item's state written after its final state would undo it.
            if (finalized.has(itemKey)) statesAfterFinal++;
            if (finalStamp !== undefined) finalized.add(itemKey);
          }
          if (yield* InSharedCommit) {
            yield* hold(WRITE_MS);
            if (inWindow(yield* Clock.currentTimeMillis)) {
              laneWrites++;
              if (intermediate) laneIntermediates++;
            }
            if (finalStamp !== undefined) pendingFinals.push(finalStamp);
            return;
          }
          yield* writer.withPermits(1)(hold(JOB_MS + BATCH_MS));
        });
      const barrierWrite = writer.withPermits(1)(
        Effect.gen(function* () {
          yield* hold(BARRIER_MS);
          if (inWindow(yield* Clock.currentTimeMillis)) barrierWrites++;
        }),
      );
      let laneIntermediates = 0;
      const isIntermediate = (provided: ProviderAdapter.ProviderAdapterV2Event) =>
        (provided.type === "turn_item.updated" && provided.turnItem.status === "running") ||
        (provided.type === "node.updated" && provided.node.status === "running") ||
        (provided.type === "message.updated" && provided.message.streaming);
      const itemKeyOf = (provided: ProviderAdapter.ProviderAdapterV2Event) =>
        provided.type === "turn_item.updated" ? provided.turnItem.id : undefined;
      const finalStampOf = (provided: ProviderAdapter.ProviderAdapterV2Event) =>
        provided.type === "turn_item.updated" && provided.turnItem.status === "completed"
          ? DateTime.toEpochMillis(provided.turnItem.updatedAt)
          : undefined;
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
                commitBatch.consume(source, process, after, canBatch),
              write: () => commitBatch.barrier(barrierWrite).pipe(Effect.as([])),
              writeWithEffects: () => commitBatch.barrier(barrierWrite).pipe(Effect.as([])),
              writeIfRunCurrent: () =>
                commitBatch
                  .barrier(barrierWrite)
                  .pipe(Effect.as({ committed: true, storedEvents: [] })),
            }),
            IdAllocator.layer,
            ThreadCommandExecutor.layer,
            Layer.mock(ProviderEventIngestor.ProviderEventIngestorV2)({
              ingestNormalized: (input) =>
                commitBatch
                  .barrier(
                    snapshotWrite(
                      finalStampOf(input.event),
                      isIntermediate(input.event),
                      itemKeyOf(input.event),
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
      // What today's lane would write in the window: every state sent, as none of these kinds is
      // coalesced on 5960bd711f; and how many of them a later state of their item supersedes.
      let offeredJobs = 0;
      let offeredSuperseded = 0;
      yield* Effect.gen(function* () {
        const runExecution = yield* RunExecutionService.RunExecutionServiceV2;
        const durationMs = (WARMUP_SECONDS + WINDOW_SECONDS) * 1000 + 5_000;
        const rates = runRates(seeded(2099));
        const runs = yield* Effect.forEach(
          Array.from({ length: RUNS }, (_, run) => run),
          (run) =>
            Effect.gen(function* () {
              const random = seeded(2100 + run);
              const ids: Ids = {
                threadId: ThreadId.make(`thread:${run}`),
                runId: RunId.make(`run:${run}`),
                attemptId: RunAttemptId.make(`attempt:${run}`),
                providerThreadId: ProviderThreadId.make(`provider-thread:${run}`),
                rootProviderTurnId: ProviderTurnId.make(`provider-turn:${run}`),
                rootNodeId: NodeId.make(`node:${run}`),
              };
              const schedule = runSchedule(ids, random, rates[run]!, start, durationMs);
              for (const next of schedule) {
                if (inWindow(next.at)) {
                  offeredJobs++;
                  if (next.superseded) offeredSuperseded++;
                }
                const stamp = finalStampOf(next.event);
                if (stamp !== undefined) sentFinalStamps.push(stamp);
              }
              const source = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
              // The provider runs on its own clock. A run's events reach the lane in handovers
              // ~9 s apart (p50); each handover here is what the run sent since the last.
              // As sent (0), the provider offers each event when it sends it and the run's
              // handovers follow the lane's own pace.
              const handovers = handoversOf(schedule, start + random() * handoverMs, handoverMs);
              yield* Effect.gen(function* () {
                for (const handover of handovers) {
                  const now = yield* Clock.currentTimeMillis;
                  if (handover.at > now) yield* Effect.sleep(handover.at - now);
                  yield* Queue.offerAll(source, handover.events);
                }
              }).pipe(Effect.forkChild);
              return yield* runExecution
                .startRootRun({
                  commandId: CommandId.make(`command:${run}`),
                  appThread: {
                    id: ids.threadId,
                    lineage: {
                      parentThreadId: null,
                      relationshipToParent: null,
                      rootThreadId: ids.threadId,
                    },
                  } as OrchestrationV2AppThread,
                  providerSessionId: ProviderSessionId.make(`session:${run}`),
                  session: {
                    events: Stream.empty,
                    subscribeEvents: Effect.succeed({
                      events: Stream.fromQueue(source),
                      close: Effect.void,
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
                    id: CheckpointScopeId.make(`checkpoint-scope:${run}`),
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
                    messageId: MessageId.make(`message:${run}:user`),
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
                })
                .pipe(Effect.forkChild);
            }),
        );
        // Commands, run lifecycle and effects of other threads queue on the same lane.
        const random = seeded(7919);
        yield* Effect.gen(function* () {
          for (;;) {
            yield* Effect.sleep(
              Math.max(1, Math.floor((-Math.log(1 - random()) * 60_000) / BARRIERS_PER_MINUTE)),
            );
            yield* commitBatch.barrier(barrierWrite).pipe(Effect.forkChild);
          }
        }).pipe(Effect.forkChild);
        yield* TestClock.adjust(`${WARMUP_SECONDS + WINDOW_SECONDS} seconds`);
        yield* Fiber.interruptAll(runs);
      }).pipe(Effect.provide(layer), Effect.scoped);

      const minutes = WINDOW_SECONDS / 60;
      const busy = busyMs / (WINDOW_SECONDS * 1000);
      const allowedWrites = (offeredJobs - offeredSuperseded) / minutes;
      // The lane time today's offered jobs need, less the superseded ones, plus barriers and batches.
      const predictedBusy =
        ((offeredJobs - offeredSuperseded) * JOB_MS +
          barrierWrites * BARRIER_MS +
          batches * BATCH_MS) /
        (WINDOW_SECONDS * 1000);
      const summary = {
        offeredJobsPerMinute: Math.round(offeredJobs / minutes),
        supersededShare: Number((offeredSuperseded / offeredJobs).toFixed(3)),
        laneWritesPerMinute: Math.round(laneWrites / minutes),
        laneIntermediatesPerMinute: Math.round(laneIntermediates / minutes),
        batchesPerMinute: Math.round(batches / minutes),
        barrierWritesPerMinute: Math.round(barrierWrites / minutes),
        laneBusy: Number(busy.toFixed(3)),
        predictedBusy: Number(predictedBusy.toFixed(3)),
        finalCommitLagP50S: finalLags.length === 0 ? null : percentile(finalLags, 0.5),
        finalCommitLagP99S: finalLags.length === 0 ? null : percentile(finalLags, 0.99),
        maxFinalBacklog,
        statesAfterFinal,
        finalsSentInWindow: sentFinalStamps.filter(inWindow).length,
        finalsSentByTail: sentFinalStamps.filter((stamp) => stamp <= windowEnd - 15_000).length,
        committedFinals,
      };
      // Times (busy share, commit lag) are reported, not gated: the rows gate on counts.
      process.stderr.write(`commit-lane row: ${JSON.stringify(summary)}\n`);
      // The model offers a busy server's load: ~1,900 jobs/min from 60 runs; and it ran.
      assert.isAbove(laneWrites, 0);
      assert.isAbove(batches, 0);
      assert.isAtLeast(offeredJobs / minutes, 1_600);
      assert.isAtMost(offeredJobs / minutes, 2_300);
      // Every item's final state is written, and never followed by an older state of the item.
      assert.isAtLeast(committedFinals, summary.finalsSentByTail, JSON.stringify(summary));
      assert.equal(statesAfterFinal, 0, JSON.stringify(summary));
      if (handoverMs > 0) {
        // At those handovers, each item state a later one supersedes in the same handover never
        // reaches the lane (10% headroom for states straddling two).
        assert.isAtMost(laneWrites / minutes, allowedWrites * 1.1, JSON.stringify(summary));
      } else {
        // As sent, a shorter backlog coalesces less, but the final states waiting for the lane
        // stay within what the providers send in 5 s; an uncoalesced backlog holds about 9 s.
        const finalsPerSecond = summary.finalsSentInWindow / WINDOW_SECONDS;
        assert.isAtMost(maxFinalBacklog, finalsPerSecond * 5, JSON.stringify(summary));
        assert.isBelow(laneWrites, offeredJobs, JSON.stringify(summary));
      }
    }),
);

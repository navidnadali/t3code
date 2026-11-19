/**
 * Differential row: for each coalesced kind, random partial reports with controls between
 * go once through the mailbox and once in arrival order into two real stores; every projection row
 * must match in every column, so a store that merges a field a later report omits fails here.
 */
import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { makeIngestionMailbox } from "./assistantStreaming.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const SEED = Number(process.env.COALESCING_DIFF_SEED ?? 21_021);
const TRIALS_PER_KIND = 60;
const driver = ProviderDriverKind.make("claudeAgent");
const providerInstanceId = ProviderInstanceId.make("claude");
const providerThreadId = ProviderThreadId.make("provider-thread:diff");

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

const layerStore = () => {
  const database = SqlitePersistence.layerMemory;
  const stores = Layer.mergeAll(
    database,
    EventStore.layer.pipe(Layer.provideMerge(database)),
    ProjectionStore.layer.pipe(Layer.provideMerge(database)),
  );
  return Layer.fresh(Layer.mergeAll(stores, EventSink.layer.pipe(Layer.provide(stores))));
};

const TABLES = [
  ["orchestration_v2_projection_threads", "thread_id"],
  ["orchestration_v2_projection_provider_turns", "thread_id"],
  ["orchestration_v2_projection_nodes", "thread_id"],
  ["orchestration_v2_projection_messages", "thread_id"],
  ["orchestration_v2_projection_turn_items", "thread_id"],
  ["orchestration_v2_projection_turn_item_counts", "thread_id"],
  ["orchestration_v2_turn_item_positions", "thread_id"],
] as const;

type Kind = "provider_turn" | "node" | "message" | "turn_item";

/** One trial's provider events: two items of `kind`, partial reports, controls between. */
function trialEvents(kind: Kind, thread: ThreadId, random: () => number, now: DateTime.Utc) {
  const pick = <A>(values: ReadonlyArray<A>) => values[Math.floor(random() * values.length)]!;
  const maybe = (field: string, value: unknown) => (random() < 0.5 ? { [field]: value } : {});
  const events: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
  const steps = 2 + Math.floor(random() * 7);
  const settled = new Set<number>();
  for (let step = 0; step < steps; step++) {
    if (random() < 0.2) {
      events.push({
        type: "background_work.updated",
        driver,
        providerTurnId: ProviderTurnId.make(`turn:${thread}:background`),
        pending: false,
      } as unknown as ProviderAdapter.ProviderAdapterV2Event);
      continue;
    }
    const item = Math.floor(random() * 2);
    const final = !settled.has(item) && random() < 0.25;
    if (final) settled.add(item);
    const usage = (used: number) => ({
      usedTokens: used,
      updatedAt: DateTime.formatIso(now),
      ...maybe("maxTokens", pick([null, 200_000])),
      ...maybe("inputTokens", used),
      ...maybe("outputTokens", Math.floor(used / 10)),
    });
    switch (kind) {
      case "provider_turn":
        events.push({
          type: "provider_turn.updated",
          driver,
          threadId: thread,
          providerTurn: {
            id: ProviderTurnId.make(`turn:${thread}:${item}`),
            providerThreadId: ProviderThreadId.make(`provider-thread:${thread}`),
            nodeId: NodeId.make(`node:${thread}:root`),
            runAttemptId: null,
            nativeTurnRef: null,
            ordinal: 1 + item,
            status: "running",
            startedAt: now,
            completedAt: null,
            ...maybe("tokenUsage", usage(1_000 + step)),
            ...maybe("turnTokenUsage", {
              usageScope: "main_agent",
              usageStatus: "complete",
              hasSubagents: false,
              inputTokens: step,
              outputTokens: step,
            }),
            ...maybe("model", pick(["claude-a", "claude-b"])),
          },
        } as unknown as ProviderAdapter.ProviderAdapterV2Event);
        break;
      case "node":
        events.push({
          type: "node.updated",
          driver,
          node: {
            id: NodeId.make(`node:${thread}:${item}`),
            threadId: thread,
            runId: null,
            parentNodeId: null,
            rootNodeId: NodeId.make(`node:${thread}:${item}`),
            kind: "tool_call",
            status: settled.has(item)
              ? pick(["completed", "failed"])
              : pick(["pending", "running"]),
            countsForRun: random() < 0.5,
            providerThreadId: random() < 0.5 ? providerThreadId : null,
            providerTurnId: null,
            nativeItemRef: null,
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt: random() < 0.5 ? now : null,
            completedAt: settled.has(item) ? now : null,
          },
        } as unknown as ProviderAdapter.ProviderAdapterV2Event);
        break;
      case "message":
        events.push({
          type: "message.updated",
          driver,
          message: {
            createdBy: "agent",
            creationSource: "provider",
            id: MessageId.make(`message:${thread}:${item}`),
            threadId: thread,
            runId: null,
            nodeId: null,
            role: "assistant",
            text: `step ${step} `.repeat(1 + Math.floor(random() * 3)),
            attachments: [],
            streaming: !settled.has(item),
            createdAt: now,
            updatedAt: now,
            ...maybe("workflowScript", random() < 0.5),
          },
        } as unknown as ProviderAdapter.ProviderAdapterV2Event);
        break;
      case "turn_item": {
        const command = item === 0;
        events.push({
          type: "turn_item.updated",
          driver,
          turnItem: {
            id: TurnItemId.make(`item:${thread}:${item}`),
            threadId: thread,
            runId: null,
            nodeId: null,
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1 + item,
            status: settled.has(item) ? "completed" : "running",
            title: pick(["Tool", "Tool, renamed"]),
            startedAt: now,
            completedAt: settled.has(item) ? now : null,
            updatedAt: now,
            ...(command
              ? {
                  type: "command_execution",
                  input: "ls",
                  ...maybe("output", `out ${step}`),
                  ...maybe("exitCode", step % 3),
                  ...maybe("outputIndicatesFailure", random() < 0.5),
                }
              : {
                  type: "dynamic_tool",
                  toolName: "mcp__tool",
                  input: { step },
                  ...maybe("output", { step }),
                  ...maybe("viewedImagePath", "/tmp/image.png"),
                }),
          },
        } as unknown as ProviderAdapter.ProviderAdapterV2Event);
        break;
      }
    }
  }
  return events;
}

/** The domain event the ingestor stores for a coalesced kind; null for a control (not stored here). */
function toDomain(
  event: ProviderAdapter.ProviderAdapterV2Event,
  thread: ThreadId,
  id: string,
  now: DateTime.Utc,
): OrchestrationV2DomainEvent | null {
  const base = {
    id: EventId.make(id),
    threadId: thread,
    driver,
    providerInstanceId,
    occurredAt: now,
  };
  switch (event.type) {
    case "provider_turn.updated":
      return {
        ...base,
        type: "provider-turn.updated",
        payload: event.providerTurn,
      } as OrchestrationV2DomainEvent;
    case "node.updated":
      return { ...base, type: "node.updated", payload: event.node } as OrchestrationV2DomainEvent;
    case "message.updated":
      return {
        ...base,
        type: "message.updated",
        payload: event.message,
      } as OrchestrationV2DomainEvent;
    case "turn_item.updated":
      return {
        ...base,
        type: "turn-item.updated",
        payload: event.turnItem,
      } as OrchestrationV2DomainEvent;
    default:
      return null;
  }
}

const thread = (id: ThreadId, now: DateTime.Utc): OrchestrationV2AppThread =>
  ({
    createdBy: "user",
    creationSource: "web",
    id,
    projectId: ProjectId.make(`project:${id}`),
    title: `Thread ${id}`,
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "claude" } satisfies ModelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  }) as unknown as OrchestrationV2AppThread;

it.effect.each<Kind>(["provider_turn", "node", "message", "turn_item"])(
  "stores the same rows coalesced as in arrival order (%s)",
  (kind) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const coalescedStore = yield* Layer.build(layerStore());
      const arrivalStore = yield* Layer.build(layerStore());
      const write = (events: ReadonlyArray<OrchestrationV2DomainEvent>) =>
        Effect.forEach(events, (event) =>
          EventSink.EventSinkV2.pipe(Effect.flatMap((sink) => sink.write({ events: [event] }))),
        );
      const rows = (threadId: ThreadId) =>
        Effect.forEach(TABLES, ([table, column]) =>
          SqlClient.SqlClient.pipe(
            Effect.flatMap((sql) =>
              sql.unsafe(`SELECT * FROM ${table} WHERE ${column} = ? ORDER BY 1, 2`, [threadId]),
            ),
            Effect.map((found) => [table, found] as const),
          ),
        );
      const seed = SEED + ["provider_turn", "node", "message", "turn_item"].indexOf(kind) * 1_000;
      const random = seeded(seed);
      let replaced = 0;
      for (let trial = 0; trial < TRIALS_PER_KIND; trial++) {
        const threadId = ThreadId.make(`thread:diff:${kind}:${trial}`);
        const sent = trialEvents(kind, threadId, random, now);
        const mailbox = makeIngestionMailbox();
        for (const event of sent) mailbox.offer(event);
        const coalesced = mailbox.takeAll();
        replaced += sent.length - coalesced.length;
        const created = toDomainCreated(threadId, now);
        const domain = (
          events: ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>,
          side: string,
        ) =>
          events.flatMap((event, index) => {
            const stored = toDomain(event, threadId, `event:${side}:${threadId}:${index}`, now);
            return stored === null ? [] : [stored];
          });
        yield* write([created("coalesced"), ...domain(coalesced, "coalesced")]).pipe(
          Effect.provide(coalescedStore),
        );
        yield* write([created("arrival"), ...domain(sent, "arrival")]).pipe(
          Effect.provide(arrivalStore),
        );
        const coalescedRows = yield* rows(threadId).pipe(Effect.provide(coalescedStore));
        const arrivalRows = yield* rows(threadId).pipe(Effect.provide(arrivalStore));
        assert.deepEqual(
          coalescedRows,
          arrivalRows,
          `${kind}: seed ${seed}, trial ${trial} (rerun with COALESCING_DIFF_SEED=${SEED}): ${JSON.stringify(sent.map((event) => event.type))}`,
        );
      }
      // The trials did coalesce: the row compares stores that differ in what they were sent.
      assert.isAbove(replaced, TRIALS_PER_KIND, `${kind}: too few reports coalesced`);
    }).pipe(Effect.scoped),
);

const toDomainCreated = (threadId: ThreadId, now: DateTime.Utc) => (side: string) =>
  ({
    id: EventId.make(`event:${side}:${threadId}:created`),
    type: "thread.created",
    threadId,
    providerInstanceId,
    occurredAt: now,
    payload: thread(threadId, now),
  }) as OrchestrationV2DomainEvent;

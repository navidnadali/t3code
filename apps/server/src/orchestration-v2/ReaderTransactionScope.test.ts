import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CheckpointScopeId,
  ContextTransferId,
  EventId,
  MessageId,
  type ModelSelection,
  NodeId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import * as Tracer from "effect/Tracer";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const providerInstanceId = ProviderInstanceId.make("codex");
const providerDriver = ProviderDriverKind.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} satisfies ModelSelection;
const at = (second: number) => DateTime.makeUnsafe(Date.UTC(2026, 0, 1, 0, 0, second));

const layerFile = (filename: string, readerPoolSize?: number) =>
  EventSink.layer.pipe(
    Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
    Layer.provideMerge(
      SqlitePersistence.layerFromPath(
        filename,
        readerPoolSize === undefined ? undefined : { readerPoolSize },
      ),
    ),
  );

const withFileStore = <A, E>(
  prefix: string,
  body: Effect.Effect<
    A,
    E,
    | EventSink.EventSinkV2
    | ProjectionStore.ProjectionStoreV2
    | EventStore.EventStoreV2
    | SqlClient.SqlClient
  >,
  readerPoolSize?: number,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix });
    return yield* body.pipe(
      Effect.provide(layerFile(path.join(directory, "statev2.sqlite"), readerPoolSize)),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

function makeThread(
  threadId: ThreadId,
  forkedFrom: OrchestrationV2AppThread["forkedFrom"],
): OrchestrationV2AppThread {
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make("project:reader-scope"),
    title: `Thread ${threadId}`,
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom,
    createdAt: at(0),
    updatedAt: at(0),
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

const event = (
  id: string,
  rest: Omit<OrchestrationV2DomainEvent, "id" | "occurredAt">,
): OrchestrationV2DomainEvent =>
  ({ id: EventId.make(id), occurredAt: at(1), ...rest }) as OrchestrationV2DomainEvent;

// One run with a root node, its checkpoint scope, a user and an assistant message and tool items.
function runEvents(input: {
  readonly threadId: ThreadId;
  readonly name: string;
  readonly ordinal: number;
  readonly toolItems: number;
}): Array<OrchestrationV2DomainEvent> {
  const { threadId, name } = input;
  const runId = RunId.make(`run:${name}`);
  const rootNodeId = NodeId.make(`node:${name}:root`);
  const scopeId = CheckpointScopeId.make(`scope:${name}`);
  const userMessageId = MessageId.make(`message:${name}:user`);
  const assistantMessageId = MessageId.make(`message:${name}:assistant`);
  const itemBase = (index: number) => ({
    id: TurnItemId.make(`item:${name}:${index}`),
    threadId,
    runId,
    nodeId: rootNodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: index,
    status: "completed" as const,
    title: null,
    startedAt: at(2),
    completedAt: at(2),
    updatedAt: at(2),
  });
  return [
    event(`event:${name}:run`, {
      type: "run.updated",
      threadId,
      runId,
      payload: {
        id: runId,
        threadId,
        ordinal: input.ordinal,
        providerInstanceId,
        modelSelection,
        providerThreadId: null,
        userMessageId,
        rootNodeId,
        activeAttemptId: null,
        status: "completed",
        requestedAt: at(1),
        startedAt: at(1),
        completedAt: at(3),
        checkpointId: null,
        contextHandoffId: null,
      },
    } as never),
    event(`event:${name}:root`, {
      type: "node.updated",
      threadId,
      payload: {
        id: rootNodeId,
        threadId,
        runId,
        parentNodeId: null,
        rootNodeId,
        kind: "root_turn",
        status: "completed",
        countsForRun: true,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: scopeId,
        startedAt: at(1),
        completedAt: at(3),
      },
    } as never),
    event(`event:${name}:scope`, {
      type: "checkpoint-scope.created",
      threadId,
      payload: {
        id: scopeId,
        threadId,
        runId,
        nodeId: rootNodeId,
        parentScopeId: null,
        providerThreadId: null,
        kind: "root_run",
        ordinalWithinParent: input.ordinal,
        advancesAppRunCount: true,
        cwd: "/repo",
        createdAt: at(1),
      },
    } as never),
    // The assistant reply is stored first, so only the turn-item order puts the user message first.
    ...[
      { id: assistantMessageId, role: "assistant" as const, createdAt: at(1) },
      { id: userMessageId, role: "user" as const, createdAt: at(2) },
    ].map((message) =>
      event(`event:${name}:message:${message.role}`, {
        type: "message.updated",
        threadId,
        payload: {
          createdBy: message.role === "user" ? "user" : "agent",
          creationSource: message.role === "user" ? "web" : "provider",
          id: message.id,
          threadId,
          runId,
          nodeId: rootNodeId,
          role: message.role,
          text: `${name} ${message.role}`,
          attachments: [],
          streaming: false,
          createdAt: message.createdAt,
          updatedAt: message.createdAt,
        },
      } as never),
    ),
    event(`event:${name}:item:user`, {
      type: "turn-item.updated",
      threadId,
      payload: {
        ...itemBase(1),
        type: "user_message",
        createdBy: "user",
        creationSource: "web",
        messageId: userMessageId,
        inputIntent: "turn_start",
        text: `${name} user`,
        attachments: [],
      },
    } as never),
    event(`event:${name}:item:assistant`, {
      type: "turn-item.updated",
      threadId,
      payload: {
        ...itemBase(2),
        type: "assistant_message",
        messageId: assistantMessageId,
        text: `${name} assistant`,
        streaming: false,
      },
    } as never),
    ...Array.from({ length: input.toolItems }, (_, offset) =>
      event(`event:${name}:item:tool:${offset}`, {
        type: "turn-item.updated",
        threadId,
        payload: {
          ...itemBase(3 + offset),
          type: "dynamic_tool",
          toolName: `tool-${offset}`,
          input: { offset },
          output: { offset, text: "x".repeat(2_048) },
        },
      } as never),
    ),
  ];
}

interface Fixture {
  readonly source: ThreadId;
  readonly fork: ThreadId;
  readonly forkRunId: RunId;
  readonly child: ThreadId;
}

// A run fork of a source thread, with a subagent, its spawn transfer and a provider thread.
const writeFixture = (name: string, toolItems: number) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const source = ThreadId.make(`thread:${name}:source`);
    const fork = ThreadId.make(`thread:${name}:fork`);
    const child = ThreadId.make(`thread:${name}:child`);
    const subagentId = NodeId.make(`node:${name}:subagent`);
    const forkRunId = RunId.make(`run:${name}:fork`);
    yield* sink.write({
      events: [
        event(`event:${name}:source`, {
          type: "thread.created",
          threadId: source,
          providerInstanceId,
          payload: makeThread(source, null),
        } as never),
        ...runEvents({ threadId: source, name: `${name}:source`, ordinal: 1, toolItems }),
        event(`event:${name}:fork`, {
          type: "thread.created",
          threadId: fork,
          providerInstanceId,
          payload: makeThread(fork, {
            type: "run",
            threadId: source,
            runId: RunId.make(`run:${name}:source`),
          }),
        } as never),
        ...runEvents({ threadId: fork, name: `${name}:fork`, ordinal: 1, toolItems }),
        event(`event:${name}:subagent`, {
          type: "subagent.updated",
          threadId: fork,
          nodeId: subagentId,
          providerInstanceId,
          payload: {
            id: subagentId,
            threadId: fork,
            runId: forkRunId,
            parentNodeId: NodeId.make(`node:${name}:fork:root`),
            origin: "app_owned",
            createdBy: "agent",
            driver: providerDriver,
            providerInstanceId,
            providerThreadId: ProviderThreadId.make(`provider-thread:${name}:child`),
            childThreadId: child,
            nativeTaskRef: null,
            prompt: "Inspect",
            title: "Child",
            model: modelSelection.model,
            status: "completed",
            result: "done",
            startedAt: at(2),
            completedAt: at(3),
            updatedAt: at(3),
          },
        } as never),
        event(`event:${name}:transfer`, {
          type: "context-transfer.created",
          threadId: fork,
          providerInstanceId,
          payload: {
            id: ContextTransferId.make(`transfer:${name}:spawn`),
            type: "subagent_spawn",
            sourceThreadId: fork,
            targetThreadId: child,
            sourcePoint: { threadId: fork },
            basePoint: null,
            sourceProviderInstanceId: providerInstanceId,
            targetProviderInstanceId: providerInstanceId,
            targetRunId: null,
            status: "consumed",
            resolution: null,
            createdBy: "agent",
            error: null,
            createdAt: at(2),
            updatedAt: at(2),
            consumedAt: at(2),
          },
        } as never),
        event(`event:${name}:provider-thread`, {
          type: "provider-thread.updated",
          threadId: fork,
          driver: providerDriver,
          providerInstanceId,
          payload: {
            id: ProviderThreadId.make(`provider-thread:${name}:fork`),
            driver: providerDriver,
            providerInstanceId,
            providerSessionId: null,
            appThreadId: fork,
            ownerNodeId: null,
            nativeThreadRef: null,
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: 1,
            lastRunOrdinal: 1,
            handoffIds: [],
            forkedFrom: null,
            createdAt: at(1),
            updatedAt: at(1),
          },
        } as never),
      ],
    });
    return { source, fork, forkRunId, child } satisfies Fixture;
  });

it.effect("keeps the results of the reads that hold reader transactions", () =>
  withFileStore(
    "reader-scope-results-",
    Effect.gen(function* () {
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const fixture = yield* writeFixture("results", 400);

      const projection = yield* store.getThreadProjection(fixture.fork);
      assert.lengthOf(projection.turnItems, 402);
      assert.deepEqual(
        projection.messages.map((message) => message.id),
        [
          MessageId.make("message:results:fork:user"),
          MessageId.make("message:results:fork:assistant"),
        ],
      );
      assert.deepEqual(
        projection.subagents.map((subagent) => subagent.childThreadId),
        [fixture.child],
      );
      assert.lengthOf(projection.contextTransfers, 1);
      assert.lengthOf(projection.checkpointScopes, 1);
      // The fork shows the source run's items before its own, behind a fork marker.
      assert.isAbove(projection.visibleTurnItems.length, projection.turnItems.length);
      assert.deepEqual(
        projection.visibleTurnItems.map((row) => row.position),
        projection.visibleTurnItems.map((_, index) => index),
      );
      assert.strictEqual(
        projection.visibleTurnItems.at(-1)?.item.id,
        TurnItemId.make("item:results:fork:402"),
      );

      const snapshot = yield* store.getThreadSnapshot(fixture.fork);
      assert.deepStrictEqual(snapshot.projection, projection);
      assert.isAbove(snapshot.snapshotSequence, 0);

      // Without turn items, only the message ordinal read puts the user message first.
      const records = yield* store.getThreadRecords(fixture.fork, [
        "messages",
        "subagents",
        "contextTransfers",
      ]);
      assert.deepEqual(
        records.messages.map((message) => message.id),
        [
          MessageId.make("message:results:fork:user"),
          MessageId.make("message:results:fork:assistant"),
        ],
      );
      assert.deepStrictEqual(records.subagents, projection.subagents);
      assert.deepStrictEqual(records.contextTransfers, projection.contextTransfers);
      assert.deepStrictEqual(records.thread, projection.thread);

      const turnStart = yield* store.getTurnStartContext(fixture.fork, fixture.forkRunId);
      assert.isTrue(turnStart.hasConversation);
      assert.deepEqual(
        turnStart.nodes.map((node) => node.id),
        [NodeId.make("node:results:fork:root")],
      );
      assert.deepStrictEqual(turnStart.checkpointScopes, projection.checkpointScopes);
      assert.lengthOf(turnStart.turnItems, 402);
      assert.deepEqual(
        turnStart.messages.map((message) => message.id),
        [MessageId.make("message:results:fork:user")],
      );
      assert.deepStrictEqual(turnStart.subagents, projection.subagents);
      assert.deepStrictEqual(turnStart.contextTransfers, projection.contextTransfers);

      const shell = yield* store.getThreadShell(fixture.fork);
      assert.strictEqual(shell?.id, fixture.fork);

      const candidates = yield* store.getSettlementCandidates(fixture.fork);
      assert.deepEqual(
        candidates.map((candidate) => candidate.id),
        [fixture.fork],
      );
    }),
  ),
);

const spanLineage = (span: Tracer.Span): ReadonlyArray<Tracer.Span> => {
  const lineage = [span];
  let parent = Option.getOrUndefined(span.parent);
  while (parent?._tag === "Span") {
    lineage.push(parent);
    parent = Option.getOrUndefined(parent.parent);
  }
  return lineage;
};

// Runs a read with statement spans on and returns its statements grouped by transaction.
const traced = <A, E, R>(read: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const statements: Array<Tracer.Span> = [];
    const exit = yield* read.pipe(
      Effect.provideService(Statement.CurrentTransformer, (statement, _sql, _fiber, span) =>
        Effect.sync(() => {
          statements.push(span);
          return statement;
        }),
      ),
      Effect.provideService(Statement.SpanPropagationEnabled, true),
      Effect.exit,
    );
    const transactions = [
      ...new Set(
        statements.map((span) =>
          spanLineage(span).find((candidate) => candidate.name === "sql.transaction"),
        ),
      ),
    ];
    return { exit, statements, transactions };
  });

type Store = ProjectionStore.ProjectionStoreV2["Service"];

interface ReadCase {
  readonly name: string;
  readonly read: (
    store: Store,
    fixture: Fixture,
  ) => Effect.Effect<unknown, ProjectionStore.ProjectionStoreV2Error>;
  /** Statements in the one reader transaction, whatever the size of the thread. */
  readonly statements: number;
  /** A row the read decodes, made undecodable. */
  readonly corrupt: (sql: SqlClient.SqlClient, fixture: Fixture) => Statement.Statement<unknown>;
  readonly error: string;
}

const corruptRows = {
  forkToolItem: (sql: SqlClient.SqlClient, fixture: Fixture) =>
    sql`UPDATE orchestration_v2_projection_turn_items SET payload_json = '{}'
      WHERE thread_id = ${fixture.fork} AND type = 'dynamic_tool'`,
  subagent: (sql: SqlClient.SqlClient, fixture: Fixture) =>
    sql`UPDATE orchestration_v2_projection_subagents SET payload_json = '{}'
      WHERE thread_id = ${fixture.fork}`,
  sourceThread: (sql: SqlClient.SqlClient, fixture: Fixture) =>
    sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{}'
      WHERE thread_id = ${fixture.source}`,
  forkThread: (sql: SqlClient.SqlClient, fixture: Fixture) =>
    sql`UPDATE orchestration_v2_projection_threads SET payload_json = '{}'
      WHERE thread_id = ${fixture.fork}`,
};

const readCases: ReadonlyArray<ReadCase> = [
  {
    name: "thread projection",
    read: (store, fixture) => store.getThreadProjection(fixture.fork),
    statements: 32,
    corrupt: corruptRows.forkToolItem,
    error: "ProjectionStoreReadError",
  },
  {
    name: "thread snapshot",
    read: (store, fixture) => store.getThreadSnapshot(fixture.fork),
    statements: 33,
    corrupt: corruptRows.forkToolItem,
    error: "ProjectionStoreReadError",
  },
  {
    name: "thread records",
    read: (store, fixture) =>
      store.getThreadRecords(fixture.fork, [
        "messages",
        "subagents",
        "contextTransfers",
        "turnItems",
      ]),
    statements: 6,
    corrupt: corruptRows.subagent,
    error: "ProjectionStoreReadError",
  },
  {
    name: "turn start context",
    read: (store, fixture) => store.getTurnStartContext(fixture.fork, fixture.forkRunId),
    statements: 14,
    corrupt: corruptRows.subagent,
    error: "ProjectionStoreReadError",
  },
  {
    name: "thread shell",
    read: (store, fixture) => store.getThreadShell(fixture.fork),
    statements: 7,
    corrupt: corruptRows.sourceThread,
    error: "ProjectionStoreReadError",
  },
  {
    name: "settlement candidates",
    read: (store, fixture) => store.getSettlementCandidates(fixture.fork),
    statements: 3,
    corrupt: corruptRows.forkThread,
    error: "ProjectionStoreSetupError",
  },
];

it.effect.each(readCases)(
  "runs the $name in one reader transaction of a fixed number of statements",
  (readCase) =>
    withFileStore(
      "reader-scope-bound-",
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const small = yield* writeFixture("small", 0);
        const large = yield* writeFixture("large", 400);
        for (const fixture of [small, large]) {
          const { exit, statements, transactions } = yield* traced(readCase.read(store, fixture));
          assert.isTrue(Exit.isSuccess(exit));
          assert.strictEqual(statements.length, readCase.statements);
          assert.lengthOf(transactions, 1);
          assert.strictEqual(transactions[0]?.attributes.get("db.connection"), "reader");
        }
      }),
    ),
);

it.effect.each(readCases)(
  "ends the $name reader transaction before decoding its rows",
  (readCase) =>
    withFileStore(
      "reader-scope-decode-",
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const fixture = yield* writeFixture("decode", 2);
        yield* readCase.corrupt(sql, fixture);
        const { exit, statements, transactions } = yield* traced(readCase.read(store, fixture));
        // The read still fails on the bad row, but only after its transaction committed.
        assert.lengthOf(transactions, 1);
        const status = transactions[0]?.status;
        assert.strictEqual(status?._tag, "Ended");
        assert.isTrue(status?._tag === "Ended" && Exit.isSuccess(status.exit));
        assert.strictEqual(Option.getOrUndefined(Exit.findErrorOption(exit))?._tag, readCase.error);
        assert.strictEqual(statements.length, readCase.statements);
      }),
    ),
);

const toolItem = (input: {
  readonly id: string;
  readonly threadId: ThreadId;
  readonly runId: RunId | null;
  readonly ordinal: number;
  readonly status: "running" | "completed";
}): OrchestrationV2DomainEvent =>
  event(`event:${input.id}`, {
    type: "turn-item.updated",
    threadId: input.threadId,
    payload: {
      id: TurnItemId.make(input.id),
      threadId: input.threadId,
      runId: input.runId,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: input.ordinal,
      status: input.status,
      title: null,
      startedAt: at(4),
      completedAt: input.status === "completed" ? at(4) : null,
      updatedAt: at(4),
      type: "dynamic_tool",
      toolName: "late",
      input: {},
      output: input.status === "completed" ? {} : null,
    },
  } as never);

interface ConsistencyCase {
  readonly name: string;
  readonly read: (
    store: Store,
    fixture: Fixture,
  ) => Effect.Effect<unknown, ProjectionStore.ProjectionStoreV2Error>;
  /** The statement to pause before; the transaction has already read rows by then. */
  readonly pauseBefore: (
    query: string,
    params: ReadonlyArray<unknown>,
    fixture: Fixture,
  ) => boolean;
  /** Changes rows that only the statements after the pause read. */
  readonly write: (fixture: Fixture) => ReadonlyArray<OrchestrationV2DomainEvent>;
}

const consistencyCases: ReadonlyArray<ConsistencyCase> = [
  {
    name: "thread projection",
    read: (store, fixture) => store.getThreadProjection(fixture.fork),
    pauseBefore: (query, params, fixture) =>
      query.includes("FROM orchestration_v2_projection_threads") && params.includes(fixture.source),
    write: (fixture) => [
      toolItem({
        id: "item:snapshot:source:late",
        threadId: fixture.source,
        runId: RunId.make("run:snapshot:source"),
        ordinal: 1_000,
        status: "completed",
      }),
    ],
  },
  {
    name: "thread snapshot",
    read: (store, fixture) => store.getThreadSnapshot(fixture.fork),
    pauseBefore: (query) => query.includes("MAX(sequence)"),
    write: (fixture) => [
      toolItem({
        id: "item:snapshot:fork:late",
        threadId: fixture.fork,
        runId: fixture.forkRunId,
        ordinal: 1_000,
        status: "completed",
      }),
    ],
  },
  {
    name: "thread records",
    read: (store, fixture) =>
      store.getThreadRecords(fixture.fork, ["messages", "subagents", "contextTransfers"]),
    pauseBefore: (query) => query.includes("FROM orchestration_v2_projection_context_transfers"),
    write: (fixture) => [
      event("event:snapshot:fork:late-transfer", {
        type: "context-transfer.created",
        threadId: fixture.fork,
        providerInstanceId,
        payload: {
          id: ContextTransferId.make("transfer:snapshot:late"),
          type: "subagent_spawn",
          sourceThreadId: fixture.fork,
          targetThreadId: fixture.child,
          sourcePoint: { threadId: fixture.fork },
          basePoint: null,
          sourceProviderInstanceId: providerInstanceId,
          targetProviderInstanceId: providerInstanceId,
          targetRunId: null,
          status: "consumed",
          resolution: null,
          createdBy: "agent",
          error: null,
          createdAt: at(4),
          updatedAt: at(4),
          consumedAt: at(4),
        },
      } as never),
    ],
  },
  {
    name: "turn start context",
    read: (store, fixture) => store.getTurnStartContext(fixture.fork, fixture.forkRunId),
    pauseBefore: (query) => query.includes("GLOB '/goal*'"),
    write: (fixture) => [
      event("event:snapshot:fork:compact", {
        type: "message.updated",
        threadId: fixture.fork,
        payload: {
          createdBy: "user",
          creationSource: "web",
          id: MessageId.make("message:snapshot:fork:compact"),
          threadId: fixture.fork,
          runId: null,
          nodeId: null,
          role: "user",
          text: "/compact",
          attachments: [],
          streaming: false,
          createdAt: at(4),
          updatedAt: at(4),
        },
      } as never),
    ],
  },
  {
    name: "thread shell",
    read: (store, fixture) => store.getThreadShell(fixture.fork),
    pauseBefore: (query) => query.includes("orchestration_v2_projection_turn_item_counts"),
    write: (fixture) => [
      toolItem({
        id: "item:snapshot:fork:counted",
        threadId: fixture.fork,
        runId: fixture.forkRunId,
        ordinal: 1_000,
        status: "completed",
      }),
    ],
  },
  {
    name: "settlement candidates",
    read: (store, fixture) => store.getSettlementCandidates(fixture.fork),
    pauseBefore: (query) => query.includes("owning_provider_instance_id"),
    write: (fixture) => [
      toolItem({
        id: "item:snapshot:fork:background",
        threadId: fixture.fork,
        runId: null,
        ordinal: 1_000,
        status: "running",
      }),
    ],
  },
];

it.effect.each(consistencyCases)(
  "keeps a write that commits between the $name statements out of that read",
  (consistencyCase) =>
    withFileStore(
      "reader-scope-snapshot-",
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const fixture = yield* writeFixture("snapshot", 2);
        const before = yield* consistencyCase.read(store, fixture);
        const paused = yield* Deferred.make<void>();
        const resume = yield* Deferred.make<void>();
        const pause: Statement.Transformer = (statement) =>
          Effect.gen(function* () {
            const [query, params] = statement.compile();
            if (
              consistencyCase.pauseBefore(query, params, fixture) &&
              (yield* Deferred.succeed(paused, undefined))
            ) {
              yield* Deferred.await(resume);
            }
            return statement;
          });
        const reading = yield* consistencyCase
          .read(store, fixture)
          .pipe(Effect.provideService(Statement.CurrentTransformer, pause), Effect.forkChild);
        yield* Deferred.await(paused);
        yield* sink.write({ events: consistencyCase.write(fixture) });
        yield* Deferred.succeed(resume, undefined);
        const during = yield* Fiber.join(reading);
        const after = yield* consistencyCase.read(store, fixture);
        assert.notDeepEqual(after, before);
        assert.deepStrictEqual(during, before);
      }),
    ),
);

const isReadError = Schema.is(ProjectionStore.ProjectionStoreReadError);

// The run's root node is decoded early for its checkpoint scope, yet a bad run row still wins.
it.effect("reports the turn start context's first undecodable row in its old order", () =>
  withFileStore(
    "reader-scope-order-",
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const fixture = yield* writeFixture("order", 0);
      yield* sql`UPDATE orchestration_v2_projection_runs
        SET payload_json = ${JSON.stringify({ rootNodeId: "node:order:fork:root" })}
        WHERE run_id = ${fixture.forkRunId}`;
      yield* sql`UPDATE orchestration_v2_projection_nodes SET payload_json = 'not json'
        WHERE node_id = 'node:order:fork:root'`;
      const error = yield* Effect.flip(store.getTurnStartContext(fixture.fork, fixture.forkRunId));
      let cause: unknown = error;
      while (isReadError(cause)) cause = cause.cause;
      assert.strictEqual(
        (cause as { readonly message?: string }).message,
        'Missing key\n  at ["id"]',
      );
    }),
  ),
);

const isDecodeSpan = (span: Tracer.AnySpan) =>
  span._tag === "Span" && span.name.startsWith("ProjectionStore.decode");

// A read holds rows from its first statement until its outermost decode span ends.
it.effect.each(readCases)(
  "lets no more $name reads hold rows at once than the pool has readers",
  (readCase) =>
    withFileStore(
      "reader-scope-permits-",
      Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        const fixture = yield* writeFixture("permits", 1_500);
        let holding = 0;
        let mostHolding = 0;
        const tracer = Tracer.make({
          span(options) {
            const span = Tracer.nativeTracer.span(options);
            const parent = Option.getOrUndefined(options.parent);
            if (isDecodeSpan(span) && (parent === undefined || !isDecodeSpan(parent))) {
              const end = span.end.bind(span);
              span.end = (endTime, exit) => {
                holding--;
                end(endTime, exit);
              };
            }
            return span;
          },
        });
        const read = Effect.suspend(() => {
          let started = false;
          return readCase.read(store, fixture).pipe(
            Effect.provideService(Statement.CurrentTransformer, (statement) =>
              Effect.sync(() => {
                if (!started) {
                  started = true;
                  holding++;
                  mostHolding = Math.max(mostHolding, holding);
                }
                return statement;
              }),
            ),
          );
        });
        const results = yield* Effect.all(
          Array.from({ length: 6 }, () => read),
          {
            concurrency: "unbounded",
          },
        ).pipe(Effect.provideService(Tracer.Tracer, tracer));
        assert.lengthOf(results, 6);
        assert.strictEqual(holding, 0);
        assert.isAtMost(mostHolding, 2);
      }),
      2,
    ),
);

it.effect("reads inside a write transaction without waiting for a reader permit", () =>
  withFileStore(
    "reader-scope-permit-tx-",
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const fixture = yield* writeFixture("permit-tx", 2);
      const holding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const holder = yield* store.getThreadRecords(fixture.fork, ["runs"]).pipe(
        Effect.provideService(Statement.CurrentTransformer, (statement) =>
          Deferred.succeed(holding, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as(statement),
          ),
        ),
        Effect.forkChild,
      );
      yield* Deferred.await(holding);
      // The only permit is taken; this read has the writer and must not wait for it.
      const inside = yield* sql.withTransaction(store.getThreadRecords(fixture.fork, ["runs"]));
      yield* Deferred.succeed(release, undefined);
      assert.deepStrictEqual(yield* Fiber.join(holder), inside);
    }),
    1,
  ),
);

import type { SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ClaudeSettings,
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ScheduledTaskUpsertInput,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as ScheduledTaskService from "../scheduledTasks/ScheduledTaskService.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ClaudeAdapterV2 from "./Adapters/ClaudeAdapterV2.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ThreadLaunchService from "./ThreadLaunchService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";

// The repro bench's probes on a busy Claude orchestrator, on the test clock: every probe class
// reaches the live turn within the 60 s grace and one 5 s tick.
const DELIVERY_BOUND_MS = 65_000;
// While due messages fill every steer, one still in its grace cannot ride along; it goes in the
// first steer after it comes due: the grace, the spacing and one tick.
const SATURATED_DELIVERY_BOUND_MS = 125_000;
const sessionId = "busy-orchestrator-session";
const settings = Schema.decodeSync(ClaudeSettings)({});
const decodeScheduledTask = Schema.decodeEffect(ScheduledTaskUpsertInput);
const modelSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-sonnet-4-6",
};
// The bench's tasks name another instance than the busy run's; a bound run follows the thread.
const taskModelSelection = {
  instanceId: ProviderInstanceId.make("claudeAgent"),
  model: "claude-haiku-4-5",
};

const batch = {
  type: "assistant",
  uuid: "batch",
  session_id: sessionId,
  parent_tool_use_id: null,
  message: {
    id: "tool-batch",
    role: "assistant",
    type: "message",
    model: modelSelection.model,
    content: [
      { type: "tool_use", id: "build", name: "Bash", input: { command: "make" } },
      { type: "tool_use", id: "watch", name: "Bash", input: { command: "watch" } },
    ],
    stop_reason: "tool_use",
    stop_sequence: null,
    usage: { input_tokens: 1, output_tokens: 1 },
  },
} as unknown as SDKMessage;

const scheduledTaskLayer = ScheduledTaskService.layer.pipe(
  Layer.provide(ThreadManagementService.layer),
  Layer.provide(ServerSettings.layerTest()),
  Layer.provide(
    Layer.mock(LegacyV1ThreadImporter.LegacyV1ThreadImporter)({
      ensureTranscript: () => Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 }),
    }),
  ),
  Layer.provide(Layer.mock(ThreadLaunchService.ThreadLaunchService)({})),
  Layer.provide(ManagedPolicy.layerTest()),
  Layer.provide(
    Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
      namedProjectsRoot: "/projects",
      isScratchProject: () => Effect.succeed(false),
    }),
  ),
  Layer.provide(Layer.mock(SecretRequests.SecretRequests)({})),
  Layer.provide(CommandReceiptStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
  Layer.provide(Layer.mergeAll(NodeCrypto.layer, Scheduler.layer, SqlitePersistence.layerMemory)),
);

type Source = "user" | "automation" | "cross-thread";

interface Probe {
  readonly source: Source;
  readonly text: string;
  readonly queuedAt: number;
}

/**
 * A Claude orchestrator in a long live turn whose tools keep running, probed as the bench does:
 * `probeAt(second)` names the probes to send at that second. Returns each probe's delay
 * until a steer carried it (null: never), and every steer's text.
 */
const busyOrchestrator = (input: {
  readonly name: string;
  readonly backlog?: number;
  readonly probeAt: (second: number) => ReadonlyArray<Source>;
  readonly probeForSeconds: number;
  readonly settleForSeconds: number;
  /** How far the test clock moves between probes; 5 s unless set. */
  readonly stepSeconds?: number;
}) =>
  Effect.gen(function* () {
    const cwd = yield* checkpointWorkspace(input.name);
    const sdkMessages = yield* Queue.unbounded<SDKMessage>();
    const offers: Array<{ readonly message: SDKUserMessage; readonly at: number }> = [];
    const adapter = yield* ClaudeAdapterV2.makeClaudeAdapterV2({
      instanceId: modelSelection.instanceId,
      settings,
      environment: {},
      attachmentsDir: cwd,
      fileSystem: yield* FileSystem.FileSystem,
      path: yield* Path.Path,
      random: yield* SecureRandom.SecureRandom,
      idAllocator: yield* IdAllocator.IdAllocatorV2,
      queryRunner: {
        allocateSessionId: Effect.succeed(sessionId),
        open: () =>
          Effect.succeed({
            messages: Stream.fromQueue(sdkMessages),
            offer: (message) =>
              Clock.currentTimeMillis.pipe(
                Effect.map((at) => {
                  offers.push({ message, at });
                }),
              ),
            setModel: () => Effect.void,
            applyFlagSettings: () => Effect.void,
            setPermissionMode: () => Effect.void,
            renameSession: () => Effect.void,
            backgroundTasks: () => Effect.succeed(false),
            mcpServerStatus: Effect.succeed([]),
            readMcpResource: () => Effect.die("unexpected MCP resource read"),
            callMcpTool: () => Effect.die("unexpected MCP tool call"),
            listPermissionRules: Effect.die("unexpected permission rules read"),
            stopTask: () => Effect.void,
            interrupt: Effect.die("automatic delivery must never interrupt"),
            close: Effect.void,
          }),
        forkSession: () => Effect.die("unused"),
        subagentLaunchToolUseId: () => Effect.succeed(null),
        subagentModel: () => Effect.succeed(null),
        assertComplete: Effect.void,
      },
    });
    return yield* Effect.gen(function* () {
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
      const tasks = yield* ScheduledTaskService.ScheduledTaskService;
      const threadId = ThreadId.make(`thread:${input.name}`);
      const projectId = ProjectId.make(`project:${input.name}`);
      const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
        orchestrator.streamDomainEvents.pipe(
          Stream.filter(predicate),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped,
        );
      yield* orchestrator.dispatch({
        type: "thread.create",
        commandId: CommandId.make("create"),
        threadId,
        projectId,
        title: "Busy orchestrator",
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: cwd,
        createdBy: "user",
        creationSource: "web",
      });
      const running = yield* watch(
        (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
      );
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("long-run"),
        threadId,
        messageId: MessageId.make("long-run"),
        text: "Coordinate the migration.",
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "web",
      });
      yield* worker.drain();
      yield* Fiber.join(running);
      const toolsRunning = yield* watch(
        (event) =>
          event.type === "turn-item.updated" && event.payload.nativeItemRef?.nativeId === "watch",
      );
      yield* Queue.offer(sdkMessages, batch);
      yield* Fiber.join(toolsRunning);

      for (let index = 0; index < (input.backlog ?? 0); index++) {
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`backlog-${index}`),
          threadId,
          messageId: MessageId.make(`backlog-${index}`),
          text: `Backlog agent message #${index}.`,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          ...(index % 2 === 0
            ? { createdBy: "user" as const, creationSource: "mcp" as const }
            : {
                createdBy: "agent" as const,
                creationSource: "mcp" as const,
                senderThreadId: ThreadId.make(`thread:worker-${index % 7}`),
              }),
        });
      }

      const probes: Array<Probe> = [];
      const sendProbe = Effect.fn("sendProbe")(function* (source: Source, index: number) {
        const queuedAt = yield* Clock.currentTimeMillis;
        const text = `Probe ${index} from ${source}`;
        probes.push({ source, text, queuedAt });
        if (source === "automation") {
          const { task } = yield* tasks.upsert(
            yield* decodeScheduledTask({
              title: `Automation ${index}`,
              prompt: text,
              enabled: true,
              schedule: { type: "interval", everyMs: 86_400_000 },
              projectId,
              threadId,
              threadMode: "continue",
              workspaceStrategy: { type: "root" },
              modelSelection: taskModelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
            }),
          );
          const ran = yield* tasks.runNow({ id: task.id });
          assert.equal(ran.task.lastRunStatus, "succeeded");
          return;
        }
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`probe-${index}`),
          threadId,
          messageId: MessageId.make(`probe-${index}`),
          text,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          ...(source === "user"
            ? { createdBy: "user" as const, creationSource: "web" as const }
            : {
                createdBy: "agent" as const,
                creationSource: "mcp" as const,
                senderThreadId: ThreadId.make(`thread:sender-${index % 4}`),
                deliveryIntent: "auto" as const,
              }),
        });
      });
      const startedAt = yield* Clock.currentTimeMillis;
      let index = 0;
      const step = input.stepSeconds ?? 5;
      for (
        let second = 0;
        second < input.probeForSeconds + input.settleForSeconds;
        second += step
      ) {
        for (const source of second < input.probeForSeconds ? input.probeAt(second) : []) {
          yield* sendProbe(source, index++);
        }
        yield* TestClock.adjust(Duration.seconds(step));
        yield* worker.drain();
      }
      const steers = offers.filter((offer) => offer.message.priority === "now");
      const steerTexts = steers.map((offer) => JSON.stringify(offer.message.message));
      const delays = probes.map((probe) => {
        const steer = steers.find((offer) =>
          JSON.stringify(offer.message.message).includes(probe.text),
        );
        return { ...probe, delayMs: steer === undefined ? null : steer.at - probe.queuedAt };
      });
      const projection = yield* orchestrator.getThreadProjection(threadId);
      const automaticSteerTimes = [
        ...new Set(
          projection.turnItems.flatMap((item) =>
            item.type === "user_message" &&
            item.automaticDelivery === true &&
            item.startedAt !== null
              ? [DateTime.toEpochMillis(item.startedAt)]
              : [],
          ),
        ),
      ].toSorted((left, right) => left - right);
      return {
        startedAt,
        delays,
        automaticSteerGapsMs: automaticSteerTimes
          .slice(1)
          .map((at, i) => at - automaticSteerTimes[i]!),
        steerTexts,
        stillQueued: projection.runs.filter((run) => run.status === "queued").length,
        longRunStatus: projection.runs.find((run) => run.ordinal === 1)?.status,
      };
    }).pipe(
      Effect.provide(
        scheduledTaskLayer.pipe(
          Layer.provideMerge(
            ProviderReplayHarness.layerWithRegistry(
              { name: input.name },
              ProviderAdapterRegistry.layerSingle(adapter),
              { runEffectWorker: false },
            ),
          ),
        ),
      ),
    );
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        IdAllocator.layer,
        McpProviderSessions.layer,
        NodeServices.layer,
        NodeCrypto.layer,
      ),
    ),
  );

const late = (
  delays: ReadonlyArray<Probe & { readonly delayMs: number | null }>,
  boundMs = DELIVERY_BOUND_MS,
) =>
  delays
    .filter((probe) => probe.delayMs === null || probe.delayMs > boundMs)
    .map(
      (probe) =>
        `${probe.text}: ${probe.delayMs === null ? "never delivered" : `${probe.delayMs / 1000} s`}`,
    );

it.effect(
  "delivers user messages and automations to a busy Claude turn within 65 s while cross-thread steers land every 20 s",
  () =>
    Effect.gen(function* () {
      // A cross-thread steer every 20 s; a user message every 40 s and an automation between them.
      const result = yield* busyOrchestrator({
        name: "busy-orchestrator-steers",
        probeAt: (second) =>
          second % 20 !== 0
            ? []
            : [
                "cross-thread",
                ...(second % 40 === 0 ? ["user" as const] : ["automation" as const]),
              ],
        probeForSeconds: 360,
        settleForSeconds: 180,
      });
      assert.deepEqual(late(result.delays), []);
      // Automatic steers stay a minute apart however many messages are due.
      assert.isAtLeast(result.automaticSteerGapsMs.length, 5);
      assert.isTrue(result.automaticSteerGapsMs.every((gap) => gap >= 60_000));
      assert.equal(result.stillQueued, 0);
      assert.equal(result.longRunStatus, "running");
    }),
);

it.effect(
  "delivers user messages, automations and cross-thread messages within 125 s past 200 queued agent messages",
  () =>
    Effect.gen(function* () {
      // The bench's O2 rotation: user, cross-thread, automation, one every 20 s.
      const result = yield* busyOrchestrator({
        name: "busy-orchestrator-backlog",
        backlog: 200,
        probeAt: (second) =>
          second % 20 === 0
            ? [(["user", "cross-thread", "automation"] as const)[(second / 20) % 3]!]
            : [],
        probeForSeconds: 360,
        settleForSeconds: 720,
      });
      assert.deepEqual(late(result.delays, SATURATED_DELIVERY_BOUND_MS), []);
      assert.isAtLeast(result.automaticSteerGapsMs.length, 8);
      assert.isTrue(result.automaticSteerGapsMs.every((gap) => gap >= 60_000));
      const backlogSteers = Array.from(
        { length: 200 },
        (_, index) =>
          result.steerTexts.filter((text) => text.includes(`Backlog agent message #${index}.`))
            .length,
      );
      assert.deepEqual(
        backlogSteers.flatMap((count, index) => (count === 1 ? [] : [`${index}: ${count}`])),
        [],
      );
      assert.equal(result.stillQueued, 0);
      assert.equal(result.longRunStatus, "running");
    }),
);

it.effect("carries automations that come due just after an automatic steer in that steer", () =>
  Effect.gen(function* () {
    // The user's message is due at 60 s; automations queued at 1 s and 59 s come due after it.
    const result = yield* busyOrchestrator({
      name: "busy-orchestrator-rider",
      probeAt: (second) =>
        second === 0 ? ["user"] : second === 1 || second === 59 ? ["automation"] : [],
      probeForSeconds: 60,
      settleForSeconds: 120,
      stepSeconds: 1,
    });
    assert.deepEqual(late(result.delays), []);
    const deliveredAt = result.delays.map((probe) => probe.queuedAt + probe.delayMs!);
    assert.deepEqual(deliveredAt, [deliveredAt[0], deliveredAt[0], deliveredAt[0]]);
    assert.lengthOf(result.automaticSteerGapsMs, 0);
  }),
);

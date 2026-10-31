// @effect-diagnostics globalDate:off - timestamps come from the plan's fixed clock, not the wall clock.
import {
  type ProviderDriverKind,
  type ProviderInstanceId,
  MessageId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import { derive } from "@t3tools/provider-core/server/IdAllocator";

import {
  type ItemKind,
  type Random,
  type RunPlan,
  Spreader,
  type SynthPlan,
  type ThreadPlan,
  makeRandom,
} from "./plan.ts";
import { quantile } from "./shape.ts";

export interface BuildOptions {
  readonly providerInstanceId: string;
  readonly driver: string;
  readonly model: string;
  readonly workspaceRoot: string;
  /** Epoch ms of the newest synthetic activity; history spreads over the year before it. */
  readonly now: number;
  readonly capabilities: unknown;
}

export interface ProjectEventInput {
  readonly projectId: string;
  readonly eventId: string;
  readonly occurredAt: string;
  readonly payload: Record<string, unknown>;
}

/** A v1-imported thread's importer bookkeeping and the v1 rows it was imported from. */
export interface LegacyRows {
  readonly threadId: string;
  readonly projectId: string;
  readonly title: string;
  readonly createdAt: string;
  readonly importedAt: string;
  readonly importedMessages: number;
  readonly messages: ReadonlyArray<{
    readonly id: string;
    readonly role: string;
    readonly text: string;
    readonly at: string;
  }>;
}

/** Spawn details a parent records for its child, read when the child is written. */
interface SpawnLink {
  readonly parentThreadId: string;
  readonly rootThreadId: string;
  readonly parentRunId: string;
  readonly subagentNodeId: string;
}

type Json = Record<string, unknown>;
type Emit = (type: string, payload: Json, at: number, scope?: Json, byCommand?: boolean) => void;
interface Clock {
  now: number;
  itemOrdinal: number;
  turnOrdinal: number;
  lastRunId: string | null;
}
/** The thread's checkpoint scopes, made on its first run, and where its checkpoint chain stands. */
interface Scopes {
  ids: Array<string>;
  ordinal: number;
  previous: string | null;
}

const WORDS = (
  "the a an of to in and or for with on at by from as is are was be this that it its we you they " +
  "build test check update file module function value result error server client thread project " +
  "request response stream event queue worker index table query row column page cache read write " +
  "commit branch merge review change patch diff line output input command shell script config option " +
  "flag default return type schema field record batch window latency budget metric trace span log " +
  "start stop run wait retry timeout signal process memory disk network socket message session " +
  "provider model agent tool call plan step task note summary detail reason status state order"
).split(" ");
const BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const BULK_FIELDS = ["output", "diffStr", "text", "result", "prompt", "message", "detail"];
// Payload bytes of a native item before its text, with the server's composite ids.
const NATIVE_ITEM_BASE = 1_000;

/** A fixed word corpus: synthetic text carries no data and matches no secret pattern. */
const makeCorpus = (random: Random, length: number) => {
  const parts: Array<string> = [];
  let size = 0;
  while (size < length) {
    let word = WORDS[Math.floor(random() * WORDS.length)] as string;
    if (random() < 0.08) word = `${word}.`;
    parts.push(word);
    size += word.length + 1;
  }
  return parts.join(" ");
};

const hex = (value: number, width: number) => value.toString(16).padStart(width, "0").slice(-width);

/** IdAllocator's joinId, for the allocated (random) id shapes that `derive` does not cover. */
const joinId = (prefix: string, ...parts: ReadonlyArray<string | number>) =>
  [prefix, ...parts.map((part) => encodeURIComponent(String(part)))].join(":");

const childNodeKind = (kind: ItemKind | undefined) =>
  kind === "assistant_message" ||
  kind === "reasoning" ||
  kind === "todo_list" ||
  kind === "subagent" ||
  kind === "approval_request"
    ? kind
    : "tool_call";

export class Builder {
  private readonly plan: SynthPlan;
  private readonly options: BuildOptions;
  private readonly driver: ProviderDriverKind;
  private readonly instance: ProviderInstanceId;
  private readonly random: Random;
  private readonly corpus: string;
  private counter = 0;
  private readonly spreaders: Record<
    | "runUpdates"
    | "attemptUpdates"
    | "providerTurnUpdates"
    | "providerThreadUpdates"
    | "nodeLatest"
    | "messageLatest"
    | "turnItemLatest"
    | "subagentUpdates"
    | "providerSessionUpdates"
    | "runtimeRequestUpdates",
    Spreader
  >;
  private readonly childBySlot = new Map<string, number>();
  private readonly spawns = new Map<number, SpawnLink>();
  /** Lineage root of each thread already built; parents are always built first. */
  private readonly roots = new Map<number, string>();
  readonly threadIds: ReadonlyArray<string>;
  readonly projectIds: ReadonlyArray<string>;

  constructor(plan: SynthPlan, options: BuildOptions) {
    this.plan = plan;
    this.options = options;
    this.driver = options.driver as ProviderDriverKind;
    this.instance = options.providerInstanceId as ProviderInstanceId;
    this.random = makeRandom(plan.seed ^ 0x5eed);
    this.corpus = makeCorpus(this.random, 1 << 20);
    const { events, totals } = plan;
    this.spreaders = {
      runUpdates: new Spreader(events.runUpdates, totals.runs),
      attemptUpdates: new Spreader(events.attemptUpdates, totals.attempts),
      providerTurnUpdates: new Spreader(events.providerTurnUpdates, totals.attempts),
      providerThreadUpdates: new Spreader(events.providerThreadUpdates, totals.providerThreads),
      nodeLatest: new Spreader(events.nodeLatest, totals.nodes),
      messageLatest: new Spreader(events.messageLatest, totals.messages),
      turnItemLatest: new Spreader(events.turnItemLatest, totals.turnItems),
      subagentUpdates: new Spreader(events.subagentUpdates, totals.subagents),
      providerSessionUpdates: new Spreader(events.providerSessionUpdates, totals.providerSessions),
      runtimeRequestUpdates: new Spreader(events.runtimeRequestUpdates, totals.runtimeRequests),
    };
    this.projectIds = Array.from({ length: plan.projects }, () => this.uuid());
    this.threadIds = plan.threads.map((thread) =>
      thread.parent !== null
        ? derive.threadFromProviderThread({ driver: this.driver, nativeThreadId: this.uuid() })
        : thread.runless
          ? `import:${options.driver}:${this.uuid()}`
          : this.uuid(),
    );
    for (const thread of plan.threads) {
      if (thread.parent !== null) {
        const { thread: parent, run, item } = thread.parent;
        this.childBySlot.set(`${parent}:${run}:${item}`, thread.index);
      }
    }
  }

  /** UUID-shaped and unique: the last group is a running counter. */
  private uuid(): string {
    const word = () => Math.floor(this.random() * 0x100000000);
    return `${hex(word(), 8)}-${hex(word(), 4)}-4${hex(word(), 3)}-${hex(0x8000 | (word() & 0x3fff), 4)}-${hex(this.counter++, 12)}`;
  }

  /** A Claude-style native id (`toolu_01…`, `msg_01…`), unique by its counter tail. */
  private native(prefix: string): string {
    let id = `${prefix}_01`;
    for (let index = 0; index < 14; index++) id += BASE62[Math.floor(this.random() * 62)];
    let tail = this.counter++;
    for (let index = 0; index < 8; index++) {
      id += BASE62[tail % 62];
      tail = Math.floor(tail / 62);
    }
    return id;
  }

  private text(length: number): string {
    if (length <= 0) return "";
    let out = "";
    while (out.length < length) {
      const start = Math.floor(this.random() * (this.corpus.length - 1));
      out += this.corpus.slice(start, start + (length - out.length));
    }
    return out.trimEnd().padEnd(length, ".");
  }

  private iso(ms: number) {
    return new Date(ms).toISOString();
  }

  private spreadFactor() {
    const spread = this.plan.itemSpread;
    return spread === null ? 1 : quantile(spread, this.random());
  }

  private itemTarget(kind: ItemKind) {
    return Math.max(1, Math.round(this.plan.itemBytes[kind] * this.spreadFactor()));
  }

  /** Grows `field` so the JSON payload reaches about `bytes`. */
  private sizeTo(payload: Json, field: string, bytes: number): Json {
    const base = JSON.stringify(payload).length + field.length + 4;
    payload[field] = this.text(Math.max(1, bytes - base));
    return payload;
  }

  /** The in-progress snapshot a first event carries: same entity, its text cut to the plan's share. */
  private firstOf(payload: Json, share: number): Json {
    const first: Json = { ...payload };
    if ("status" in first) {
      first.status = "running";
      first.completedAt = null;
    }
    for (const field of BULK_FIELDS) {
      const value = first[field];
      if (typeof value === "string")
        first[field] = value.slice(0, Math.floor(value.length * share));
    }
    if ("streaming" in first) first.streaming = true;
    return first;
  }

  projectEvents(): Array<ProjectEventInput> {
    const start = this.options.now - 400 * 86_400_000;
    return this.projectIds.map((projectId, index) => {
      const at = this.iso(start + index * 60_000);
      return {
        projectId,
        eventId: joinId("event", "project", projectId, this.uuid()),
        occurredAt: at,
        payload: {
          projectId,
          title: `Synthetic project ${index + 1}`,
          workspaceRoot: `${this.options.workspaceRoot}/project-${index + 1}`,
          defaultModelSelection: null,
          scripts: [],
          createdAt: at,
          updatedAt: at,
        },
      };
    });
  }

  /** Encoded domain events for one thread, in commit order, and its v1 rows when it was imported. */
  threadEvents(thread: ThreadPlan): {
    readonly events: Array<Json>;
    readonly legacy: LegacyRows | null;
  } {
    const { options, driver, instance } = this;
    const events: Array<Json> = [];
    const threadId = this.threadIds[thread.index] as string;
    const cwd = `${options.workspaceRoot}/project-${thread.project + 1}`;
    const sessionKey = joinId(
      "provider-session",
      "provider-instance",
      instance,
      "thread",
      threadId,
      this.uuid(),
    );
    const commandKey = `${this.uuid()}:initial-message`;
    const emit: Emit = (type, payload, at, scope = {}, byCommand = false) => {
      const id = byCommand
        ? joinId("event", "thread", threadId, "command", commandKey, this.uuid())
        : joinId("event", "thread", threadId, "provider-session", sessionKey, this.uuid());
      events.push({ id, threadId, type, payload, occurredAt: this.iso(at), ...scope });
    };
    const spawn = thread.parent === null ? undefined : this.spawns.get(thread.index);
    const fork =
      thread.fork === null
        ? undefined
        : {
            threadId: this.threadIds[thread.fork.thread] as string,
            rootThreadId: this.roots.get(thread.fork.thread) as string,
            runId: derive.run({
              threadId: ThreadId.make(this.threadIds[thread.fork.thread] as string),
              ordinal: thread.fork.run + 1,
            }),
          };
    this.roots.set(thread.index, spawn?.rootThreadId ?? fork?.rootThreadId ?? threadId);
    const span = thread.runs.reduce((acc, run) => acc + 720_000 + 5_000 * run.items.length, 0);
    const createdAt = options.now - span - Math.floor(this.random() * 360 * 86_400_000);
    const providerThreadId =
      thread.runless || thread.runs.length === 0
        ? null
        : derive.providerThread({
            driver,
            nativeThreadId: `pending:${derive.run({ threadId: ThreadId.make(threadId), ordinal: 1 })}`,
          });
    const threadPayload: Json = {
      createdBy: spawn ? "agent" : "user",
      creationSource: spawn ? "provider" : "web",
      id: threadId,
      projectId: this.projectIds[thread.project],
      title: spawn
        ? `Synthetic subagent ${thread.index + 1}`
        : `Synthetic thread ${thread.index + 1}`,
      providerInstanceId: instance,
      modelSelection: { instanceId: instance, model: options.model },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThreadId,
      ...(thread.runless ? { historyOrigin: "v1_import" } : {}),
      lineage: spawn
        ? {
            parentThreadId: spawn.parentThreadId,
            relationshipToParent: "subagent",
            rootThreadId: spawn.rootThreadId,
          }
        : fork
          ? {
              parentThreadId: fork.threadId,
              relationshipToParent: "fork",
              rootThreadId: fork.rootThreadId,
            }
          : { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: spawn
        ? { type: "node", nodeId: spawn.subagentNodeId }
        : fork
          ? { type: "run", threadId: fork.threadId, runId: fork.runId }
          : null,
      createdAt: this.iso(createdAt),
      updatedAt: this.iso(createdAt),
      archivedAt: null,
      // Present as the importer and settle write them, so the boot's v1 repair finds nothing to add.
      linkedPullRequest: null,
      pullRequests: [],
      branchPullRequest: null,
      pinnedAt: null,
      pinOrderKey: null,
      activeOrderKey: null,
      snoozedUntil: null,
      snoozedAt: null,
      unsettledAt: null,
      // Settled as the server's sweep leaves an idle thread, so a boot settles nothing.
      settledOverride: this.plan.events.threadFinal ? null : "settled",
      settledAt: this.plan.events.threadFinal ? null : this.iso(createdAt),
      lastVisitedAt: null,
      deletedAt: null,
    };
    emit("thread.created", threadPayload, createdAt, {}, true);

    const providerThread = (
      id: string,
      status: string,
      first: number | null,
      last: number | null,
    ): Json => ({
      id,
      driver,
      providerInstanceId: instance,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: { driver, nativeId: this.uuid(), strength: "strong" },
      nativeConversationHeadRef: null,
      status,
      firstRunOrdinal: first,
      lastRunOrdinal: last,
      handoffIds: [],
      forkedFrom: null,
      createdAt: this.iso(createdAt),
      updatedAt: this.iso(createdAt),
    });
    const providerThreads: Array<Json> = [];
    if (providerThreadId !== null) {
      providerThreads.push(providerThread(providerThreadId, "idle", 1, thread.runs.length));
      for (let index = 0; index < thread.extraProviderThreads; index++) {
        const id = derive.providerThread({ driver, nativeThreadId: this.uuid() });
        providerThreads.push(providerThread(id, "closed", null, null));
      }
    }
    for (const payload of providerThreads) emit("provider-thread.updated", payload, createdAt);
    for (let index = 0; index < thread.providerSessions; index++) {
      const session: Json = {
        id: joinId(
          "provider-session",
          "provider-instance",
          instance,
          "thread",
          threadId,
          this.uuid(),
        ),
        driver,
        providerInstanceId: instance,
        status: "stopped",
        cwd,
        model: options.model,
        capabilities: options.capabilities,
        createdAt: this.iso(createdAt),
        updatedAt: this.iso(createdAt),
        lastError: null,
      };
      const scope = { driver, providerInstanceId: instance };
      emit("provider-session.attached", session, createdAt, scope);
      for (let update = this.spreaders.providerSessionUpdates.take(); update > 0; update--) {
        emit("provider-session.updated", session, createdAt, scope);
      }
    }

    const clock: Clock = { now: createdAt, itemOrdinal: 0, turnOrdinal: 0, lastRunId: null };
    const legacy = { message: 0, item: 0 };
    const scopes: Scopes = { ids: [], ordinal: 0, previous: null };
    thread.runs.forEach((run, runIndex) => {
      if (thread.runless) this.runlessTurn(thread, run, threadId, clock, legacy, emit);
      else
        this.nativeRun(thread, run, runIndex, {
          threadId,
          providerThreadId,
          cwd,
          clock,
          scopes,
          emit,
        });
    });

    for (const payload of providerThreads) {
      for (let update = this.spreaders.providerThreadUpdates.take(); update > 0; update--) {
        emit("provider-thread.updated", { ...payload, updatedAt: this.iso(clock.now) }, clock.now);
      }
    }
    if (spawn !== undefined && thread.resultTransfer) {
      emit(
        "context-transfer.created",
        {
          id: joinId(
            "context-transfer",
            "type",
            "subagent_result",
            "source-thread",
            threadId,
            "target-thread",
            spawn.parentThreadId,
            this.uuid(),
          ),
          type: "subagent_result",
          sourceThreadId: threadId,
          targetThreadId: spawn.parentThreadId,
          sourcePoint:
            clock.lastRunId === null ? { threadId } : { threadId, runId: clock.lastRunId },
          basePoint: null,
          sourceProviderInstanceId: instance,
          targetProviderInstanceId: instance,
          targetRunId: spawn.parentRunId,
          status: "consumed",
          resolution: null,
          createdBy: "agent",
          error: null,
          createdAt: this.iso(clock.now),
          updatedAt: this.iso(clock.now),
          consumedAt: this.iso(clock.now),
        },
        clock.now,
      );
    }
    if (fork !== undefined) {
      const firstRun =
        thread.runs.length > 0 && !thread.runless
          ? derive.run({ threadId: ThreadId.make(threadId), ordinal: 1 })
          : null;
      const transfer: Json = {
        id: joinId(
          "context-transfer",
          "type",
          "fork",
          "source-thread",
          fork.threadId,
          "target-thread",
          threadId,
          this.uuid(),
        ),
        type: "fork",
        sourceThreadId: fork.threadId,
        targetThreadId: threadId,
        sourcePoint: { threadId: fork.threadId, runId: fork.runId },
        basePoint: null,
        sourceProviderInstanceId: instance,
        targetProviderInstanceId: instance,
        targetRunId: firstRun,
        status: "consumed",
        resolution: null,
        createdBy: "user",
        error: null,
        createdAt: this.iso(createdAt),
        updatedAt: this.iso(createdAt),
        consumedAt: this.iso(createdAt),
      };
      emit("context-transfer.created", transfer, createdAt);
    }
    if (this.plan.events.threadFinal) {
      const settled = clock.now + 60_000;
      const payload: Json = {
        ...threadPayload,
        updatedAt: this.iso(settled),
        settledOverride: "settled",
        settledAt: this.iso(settled),
        lastVisitedAt: this.iso(settled),
      };
      emit("thread.settled", payload, settled, {}, true);
    }
    if (!thread.imported) return { events, legacy: null };
    const spread = this.plan.legacyText;
    const roles = ["user", "assistant", "reasoning", "assistant"];
    const messages = Array.from({ length: thread.legacyMessages }, (_, index) => ({
      id: this.uuid(),
      role: roles[index % roles.length] as string,
      text: this.text(
        Math.max(1, Math.round(spread === null ? 600 : quantile(spread, this.random()))),
      ),
      at: this.iso(createdAt + index * 30_000),
    }));
    return {
      events,
      legacy: {
        threadId,
        projectId: threadPayload.projectId as string,
        title: threadPayload.title as string,
        createdAt: this.iso(createdAt),
        importedAt: this.iso(clock.now),
        importedMessages: legacy.message,
        messages,
      },
    };
  }

  private emitMessage(payload: Json, at: number, emit: Emit, scope: Json) {
    if (this.spreaders.messageLatest.take() > 0) {
      emit("message.updated", this.firstOf(payload, this.plan.events.messageFirstShare), at, scope);
    }
    emit("message.updated", payload, at, scope, payload.role === "user");
  }

  private emitItem(item: Json, at: number, emit: Emit, scope: Json) {
    if (this.spreaders.turnItemLatest.take() > 0) {
      emit("turn-item.updated", this.firstOf(item, this.plan.events.turnItemFirstShare), at, scope);
    }
    emit("turn-item.updated", item, at + 2_000, scope);
  }

  /** History with no run, node or provider turn: v1 imports (importer ids) or a parent-fed child. */
  private runlessTurn(
    thread: ThreadPlan,
    run: RunPlan,
    threadId: string,
    clock: Clock,
    legacy: { message: number; item: number },
    emit: Emit,
  ) {
    const source = threadId.slice(threadId.indexOf(":") + 1);
    const at = (clock.now += 60_000 + Math.floor(this.random() * 600_000));
    run.items.forEach((kind, index) => {
      const itemAt = at + 5_000 * index;
      const nativeId = kind === "assistant_message" ? this.native("msg") : this.native("toolu");
      const base: Json = {
        id: thread.imported
          ? `migration:v1:turn-item:${source}:${String(++legacy.item).padStart(6, "0")}`
          : derive.turnItemFromProviderItem({ driver: this.driver, nativeItemId: nativeId }),
        threadId,
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: clock.itemOrdinal++,
        status: "completed",
        title: null,
        startedAt: this.iso(itemAt),
        completedAt: this.iso(itemAt + 2_000),
        updatedAt: this.iso(itemAt + 2_000),
        type: kind,
      };
      if (kind !== "user_message" && kind !== "assistant_message") {
        this.emitItem(this.plainItem(kind, base), itemAt, emit, {});
        return;
      }
      const user = kind === "user_message";
      const messageId = thread.imported
        ? `${source}:${String(++legacy.message).padStart(6, "0")}`
        : user
          ? this.uuid()
          : derive.messageFromProviderItem({ driver: this.driver, nativeItemId: nativeId });
      const item = this.sizeTo(
        user
          ? {
              ...base,
              createdBy: "user",
              creationSource: "web",
              messageId,
              inputIntent: "turn_start",
              attachments: [],
            }
          : { ...base, messageId, streaming: false },
        "text",
        this.itemTarget(kind),
      );
      const message: Json = {
        createdBy: user ? "user" : "agent",
        creationSource: user ? "web" : "provider",
        id: messageId,
        threadId,
        runId: null,
        nodeId: null,
        role: user ? "user" : "assistant",
        text: item.text,
        attachments: [],
        streaming: false,
        createdAt: this.iso(itemAt),
        updatedAt: this.iso(itemAt),
      };
      this.emitMessage(message, itemAt, emit, {});
      this.emitItem(item, itemAt, emit, {});
    });
    clock.now = at + 5_000 * (run.items.length + 1);
  }

  private nativeRun(
    thread: ThreadPlan,
    run: RunPlan,
    runIndex: number,
    context: {
      readonly threadId: string;
      readonly providerThreadId: string | null;
      readonly cwd: string;
      readonly clock: Clock;
      readonly scopes: Scopes;
      readonly emit: Emit;
    },
  ) {
    const { options, driver, instance } = this;
    const { threadId, providerThreadId, cwd, clock, scopes, emit } = context;
    const runId = derive.run({ threadId: ThreadId.make(threadId), ordinal: runIndex + 1 });
    clock.lastRunId = runId;
    const scoped = { runId };
    const requestedAt = (clock.now += 60_000 + Math.floor(this.random() * 600_000));
    const completedAt = requestedAt + 5_000 * (run.items.length + 1);
    const userMessageId = this.uuid();
    const rootNodeId = derive.rootNode({ runId });
    const attemptIds = Array.from({ length: run.extraAttempts + 1 }, (_, index) =>
      derive.runAttempt({ runId, attemptOrdinal: index + 1 }),
    );
    const providerTurnIds = attemptIds.map((attemptId) =>
      derive.providerTurn({ driver, nativeTurnId: `turn:${attemptId}` }),
    );
    const finalTurn = providerTurnIds.at(-1) as string;
    const nativeIds = run.items.map((kind) =>
      kind === "assistant_message"
        ? this.native("msg")
        : kind === "reasoning"
          ? `${this.native("msg")}:thinking:0`
          : this.native("toolu"),
    );
    const node = (
      id: string,
      kind: string,
      parent: string | null,
      nativeId: string | null,
    ): Json => ({
      id,
      threadId,
      runId,
      parentNodeId: parent,
      rootNodeId,
      kind,
      status: "completed",
      countsForRun: parent === null,
      providerThreadId,
      providerTurnId: finalTurn,
      nativeItemRef: nativeId === null ? null : { driver, nativeId, strength: "strong" },
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: this.iso(requestedAt),
      completedAt: this.iso(completedAt),
    });
    const emitNode = (payload: Json) => {
      if (this.spreaders.nodeLatest.take() > 0) {
        emit(
          "node.updated",
          { ...payload, status: "running", completedAt: null },
          requestedAt,
          scoped,
        );
      }
      emit("node.updated", payload, completedAt, scoped);
    };
    const message = (
      id: string,
      role: "user" | "assistant",
      text: unknown,
      at: number,
      nodeId: string | null,
    ): Json => ({
      createdBy: role === "user" ? "user" : "agent",
      creationSource: role === "user" ? "web" : "provider",
      id,
      threadId,
      runId,
      nodeId,
      role,
      text,
      attachments: [],
      streaming: false,
      createdAt: this.iso(at),
      updatedAt: this.iso(at),
    });

    const userText = this.text(Math.max(1, this.itemTarget("user_message") - NATIVE_ITEM_BASE));
    this.emitMessage(
      message(userMessageId, "user", userText, requestedAt, null),
      requestedAt,
      emit,
      scoped,
    );
    const runPayload: Json = {
      id: runId,
      threadId,
      ordinal: runIndex + 1,
      providerInstanceId: instance,
      modelSelection: { instanceId: instance, model: options.model },
      providerThreadId,
      userMessageId,
      rootNodeId,
      activeAttemptId: attemptIds.at(-1),
      status: "completed",
      requestedAt: this.iso(requestedAt),
      startedAt: this.iso(requestedAt + 1_000),
      completedAt: this.iso(completedAt),
      checkpointId: null,
      contextHandoffId: null,
    };
    emit("run.created", runPayload, requestedAt, scoped, true);
    for (let update = this.spreaders.runUpdates.take(); update > 0; update--) {
      emit("run.updated", runPayload, completedAt, scoped);
    }
    attemptIds.forEach((attemptId, index) => {
      const last = index === attemptIds.length - 1;
      const attempt: Json = {
        id: attemptId,
        runId,
        attemptOrdinal: index + 1,
        rootNodeId,
        providerInstanceId: instance,
        providerThreadId,
        providerTurnId: providerTurnIds[index],
        reason: index === 0 ? "initial" : "retry",
        status: last ? "completed" : "superseded",
        startedAt: this.iso(requestedAt + 1_000),
        completedAt: this.iso(completedAt),
      };
      emit("run-attempt.created", attempt, requestedAt, scoped);
      for (let update = this.spreaders.attemptUpdates.take(); update > 0; update--) {
        emit("run-attempt.updated", attempt, completedAt, scoped);
      }
      const turn: Json = {
        id: providerTurnIds[index],
        providerThreadId,
        nodeId: rootNodeId,
        runAttemptId: attemptId,
        nativeTurnRef: { driver, nativeId: this.uuid(), strength: "weak" },
        ordinal: ++clock.turnOrdinal,
        status: last ? "completed" : "interrupted",
        startedAt: this.iso(requestedAt + 1_000),
        completedAt: this.iso(completedAt),
      };
      emit("provider-turn.updated", turn, requestedAt, scoped);
      for (let update = this.spreaders.providerTurnUpdates.take(); update > 0; update--) {
        emit("provider-turn.updated", turn, completedAt, scoped);
      }
    });
    emitNode(node(rootNodeId, "root_turn", null, null));
    // Item i (after the user message) runs on child node (i - 1) mod count, as tool calls do.
    const tools = Math.max(1, run.items.length - 1);
    const childNodeIds = Array.from({ length: run.childNodes }, (_, index) => {
      const nativeId =
        index < run.items.length - 1 ? (nativeIds[1 + index] as string) : this.native("toolu");
      const id = derive.nodeFromProviderItem({ driver, nativeItemId: nativeId });
      emitNode(node(id, childNodeKind(run.items[1 + (index % tools)]), rootNodeId, nativeId));
      return id;
    });

    if (runIndex === 0) {
      for (let index = 0; index < thread.scopes; index++) {
        const scopeId = joinId(
          "checkpoint-scope",
          "thread",
          threadId,
          "name",
          index === 0 ? "root" : `tool-${index}`,
        );
        scopes.ids.push(scopeId);
        const scope: Json = {
          id: scopeId,
          threadId,
          runId,
          nodeId: rootNodeId,
          parentScopeId: index === 0 ? null : scopes.ids[0],
          providerThreadId,
          kind: index === 0 ? "root_run" : "tool",
          ordinalWithinParent: index,
          advancesAppRunCount: index === 0,
          cwd,
          createdAt: this.iso(requestedAt),
        };
        emit("checkpoint-scope.created", scope, requestedAt, scoped);
      }
    }
    const rootScope = scopes.ids[0] as string;
    const checkpoint = (at: number) => {
      const id = joinId("checkpoint", "scope", rootScope, "name", scopes.ordinal);
      const files = Array.from(
        {
          length: Math.max(
            1,
            Math.round((this.plan.checkpointBytes * this.spreadFactor() - 900) / 90),
          ),
        },
        (_, file) => ({
          path: `src/module-${file}-${Math.floor(this.random() * 400)}.ts`,
          kind: "modified",
          additions: 12,
          deletions: 3,
        }),
      );
      const captured: Json = {
        id,
        threadId,
        scopeId: rootScope,
        runId,
        nodeId: rootNodeId,
        parentCheckpointId: scopes.previous,
        ordinalWithinScope: scopes.ordinal++,
        appRunOrdinal: runIndex + 1,
        ref: `refs/t3/checkpoints/${encodeURIComponent(id)}`,
        status: "ready",
        files,
        capturedAt: this.iso(at),
      };
      emit("checkpoint.captured", captured, at, scoped);
      scopes.previous = id;
      return { id, files };
    };

    run.items.forEach((kind, index) => {
      const at = requestedAt + 5_000 * (index + 1);
      const itemNode =
        index === 0 || run.childNodes === 0
          ? rootNodeId
          : (childNodeIds[(index - 1) % run.childNodes] as string);
      const nativeId = nativeIds[index] as string;
      const steer = kind === "user_message" && index > 0 ? this.uuid() : null;
      const messageId = kind === "user_message" ? (steer ?? userMessageId) : null;
      const base: Json = {
        id:
          messageId === null
            ? derive.turnItemFromProviderItem({ driver, nativeItemId: nativeId })
            : derive.userTurnItem({ messageId: MessageId.make(messageId) }),
        threadId,
        runId,
        nodeId: itemNode,
        providerThreadId,
        providerTurnId: finalTurn,
        nativeItemRef: index === 0 ? null : { driver, nativeId, strength: "strong" },
        parentItemId: null,
        ordinal: clock.itemOrdinal++,
        status: "completed",
        title: null,
        startedAt: this.iso(at),
        completedAt: this.iso(at + 2_000),
        updatedAt: this.iso(at + 2_000),
        type: kind,
      };
      let item: Json;
      switch (kind) {
        case "user_message": {
          const text =
            steer === null
              ? userText
              : this.text(Math.max(1, this.itemTarget(kind) - NATIVE_ITEM_BASE));
          if (steer !== null)
            this.emitMessage(message(steer, "user", text, at, null), at, emit, scoped);
          const intent = steer === null ? "turn_start" : "steer";
          item = {
            ...base,
            createdBy: "user",
            creationSource: "web",
            messageId,
            inputIntent: intent,
            text,
            attachments: [],
          };
          break;
        }
        case "assistant_message": {
          const assistantId = derive.messageFromProviderItem({ driver, nativeItemId: nativeId });
          item = this.sizeTo(
            { ...base, messageId: assistantId, streaming: false },
            "text",
            this.itemTarget(kind),
          );
          this.emitMessage(
            message(assistantId, "assistant", item.text, at, itemNode),
            at,
            emit,
            scoped,
          );
          break;
        }
        case "todo_list": {
          const planId = joinId(
            "plan",
            "thread",
            threadId,
            "provider",
            driver,
            "run",
            runId,
            this.uuid(),
          );
          const bytes = this.itemTarget(kind);
          const steps = Array.from({ length: 4 }, (_, step) => ({
            id: `step-${step + 1}`,
            text: this.text(Math.max(8, Math.floor((bytes - NATIVE_ITEM_BASE) / 4))),
            status: "completed",
          }));
          const artifact: Json = {
            id: planId,
            threadId,
            runId,
            nodeId: itemNode,
            status: "completed",
            kind: "todo_list",
            steps,
          };
          emit("plan.updated", artifact, at, scoped);
          item = { ...base, planId, steps };
          break;
        }
        case "checkpoint": {
          const { id, files } = checkpoint(at);
          item = { ...base, checkpointId: id, scopeId: rootScope, files };
          break;
        }
        case "approval_request": {
          const requestId = RuntimeRequestId.make(
            joinId("runtime-request", "provider", driver, "native-request", nativeId, this.uuid()),
          );
          const request: Json = {
            id: requestId,
            nodeId: itemNode,
            providerTurnId: finalTurn,
            nativeRequestRef: null,
            kind: "command",
            status: "resolved",
            responseCapability: { type: "not_resumable", reason: "synthetic history" },
            createdAt: this.iso(at),
            resolvedAt: this.iso(at + 2_000),
          };
          emit("runtime-request.updated", request, at, scoped);
          for (let update = this.spreaders.runtimeRequestUpdates.take(); update > 0; update--) {
            emit("runtime-request.updated", request, at, scoped);
          }
          const approval = {
            ...base,
            id: derive.approvalTurnItem({ requestId }),
            requestId,
            requestKind: "command",
          };
          item = this.sizeTo(approval, "prompt", this.itemTarget(kind));
          break;
        }
        case "subagent": {
          const subagentId = derive.nodeFromProviderItem({ driver, nativeItemId: nativeId });
          const child = this.childBySlot.get(`${thread.index}:${runIndex}:${index}`);
          const childThreadId = child === undefined ? null : (this.threadIds[child] as string);
          if (child !== undefined) {
            this.spawns.set(child, {
              parentThreadId: threadId,
              rootThreadId: this.roots.get(thread.index) as string,
              parentRunId: runId,
              subagentNodeId: subagentId,
            });
          }
          const half = Math.max(1, Math.floor((this.itemTarget(kind) - 2 * NATIVE_ITEM_BASE) / 2));
          const prompt = this.text(half);
          const result = this.text(half);
          const subagent: Json = {
            id: subagentId,
            threadId,
            runId,
            parentNodeId: rootNodeId,
            origin: "provider_native",
            createdBy: "agent",
            driver,
            providerInstanceId: instance,
            providerThreadId: null,
            childThreadId,
            nativeTaskRef: { driver, nativeId, strength: "strong" },
            prompt,
            title: "Synthetic subagent",
            model: options.model,
            status: "completed",
            result,
            startedAt: base.startedAt,
            completedAt: base.completedAt,
            updatedAt: base.updatedAt,
          };
          emit("subagent.updated", subagent, at, scoped);
          for (let update = this.spreaders.subagentUpdates.take(); update > 0; update--) {
            emit("subagent.updated", subagent, at, scoped);
          }
          item = {
            ...base,
            subagentId,
            origin: "provider_native",
            driver,
            providerInstanceId: instance,
            childThreadId,
            prompt,
            result,
          };
          break;
        }
        default:
          item = this.plainItem(kind, base);
      }
      this.emitItem(item, at, emit, scoped);
    });
    const standalone = run.checkpoints - run.items.filter((kind) => kind === "checkpoint").length;
    for (let index = 0; index < standalone; index++) checkpoint(completedAt);
    clock.now = completedAt;
  }

  /** Item kinds that stand alone: no message, plan, checkpoint or request behind them. */
  private plainItem(kind: ItemKind, base: Json): Json {
    const bytes = this.itemTarget(kind);
    switch (kind) {
      case "reasoning":
        return this.sizeTo({ ...base, streaming: false }, "text", bytes);
      case "command_execution":
        return this.sizeTo(
          { ...base, input: "vp test run src/module.test.ts", exitCode: 0 },
          "output",
          bytes,
        );
      case "file_change": {
        const fileName = `src/module-${Math.floor(this.random() * 400)}.ts`;
        return this.sizeTo({ ...base, fileName, additions: 12, deletions: 3 }, "diffStr", bytes);
      }
      case "dynamic_tool": {
        const tool = {
          ...base,
          toolName: "mcp__t3__thread_read",
          input: { threadId: base.threadId },
        };
        return this.sizeTo(tool, "output", bytes);
      }
      case "web_search": {
        const snippet = this.text(Math.max(1, bytes - NATIVE_ITEM_BASE));
        const results = [
          { title: "Synthetic result", url: "https://example.invalid/result", snippet },
        ];
        return { ...base, patterns: ["synthetic search"], results };
      }
      case "file_search": {
        const preview = this.text(Math.max(1, bytes - NATIVE_ITEM_BASE));
        return {
          ...base,
          pattern: "synthetic",
          results: [{ fileName: "src/module.ts", line: 1, preview }],
        };
      }
      case "notification": {
        const notice = {
          ...base,
          source: { kind: "system" },
          outcome: "completed",
          summary: "Synthetic notice",
        };
        return this.sizeTo(notice, "detail", bytes);
      }
      default:
        return this.sizeTo({ ...base, type: "system_notice" }, "message", bytes);
    }
  }
}

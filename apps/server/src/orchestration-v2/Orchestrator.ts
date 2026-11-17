import type {
  OrchestrationV2ThreadLinkMetadata,
  GuestShareId,
  OrchestrationV2SearchThreadInput,
  OrchestrationV2SearchThreadResult,
  OrchestrationV2ThreadHistoryPage,
  ProjectId,
  ProviderTurnId,
} from "@t3tools/contracts";
import {
  latestExecutedRun,
  latestRootProviderFailure,
  orchestrationV2WorktreePending,
  runRanAfter,
  usageLimitBlockedRun,
} from "@t3tools/shared/orchestrationV2ThreadError";
import { canonicalizeGitCafePullRequestUrl } from "@t3tools/source-control-definitions/registry";
import { threadPullRequestsOf } from "../sourceControl/threadPullRequests.ts";
import {
  normalizeThreadPullRequestKey,
  visibleThreadPullRequests,
  threadPullRequestKeysEqual,
  legacyThreadPullRequestKey,
} from "../sourceControl/threadPullRequests.ts";
import {
  ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
  type ChatAttachment,
  CommandId,
  type GuestMessageId,
  isCompactionCommandMessage,
  isDelegatedAppThread,
  isOrchestrationV2WorkActive,
  isProviderNativeSubagentThread,
  MessageId,
  type NodeId,
  type ModelSelection,
  OrchestrationV2Command,
  type OrchestrationV2InternalCommand,
  type OrchestrationV2LimitRecoveryUpdate,
  type OrchestrationV2Notification,
  type OrchestrationV2ServerCommand,
  type ThreadPullRequestLink,
  type ThreadPullRequestWatch,
  type OrchestrationV2AppThread,
  type OrchestrationV2ContextHandoff,
  type OrchestrationV2ContextSourcePoint,
  type OrchestrationV2ContextTransfer,
  type OrchestrationV2ContextTransferResolution,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DelegatedCompletionCohort,
  type OrchestrationV2DelegatedCompletionDelivery,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RemoteTaskChild,
  type OrchestrationV2ModelSwitch,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ThreadShellSnapshot,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2Subagent,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
  latestProviderTurnForAttempt,
  parseGoalCommand,
  orchestrationV2RunWorkStartedAt,
  ProviderInstanceId,
  ProviderInteractionMode,
  isRuntimeModeAbove,
  resolveProviderInstanceEnabled,
  resolveProjectProviderInstanceEnabled,
  type ProviderSessionId,
  RunId,
  RuntimeMode,
  ThreadLinkedPullRequest,
  ThreadId,
  TurnItemId,
  TrimmedNonEmptyString,
  managedRuntimeModeCeilingMessage,
  waitEndableRun,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import { modelSelectionsEqual } from "@t3tools/shared/model";
import { latestShownRun, modelSwitchSince } from "./modelSwitch.ts";
import { isPlaceholderThreadTitle } from "./placeholderThreadTitle.ts";
import { combinePullRequestWatchWakes, type PullRequestWatchWake } from "./pullRequestWatch.ts";
import { recordThreadOpen } from "@t3tools/shared/paletteSearch";
import {
  backgroundWorkHoldsCompletion,
  derivePendingBackgroundWork,
  pendingBackgroundTurnItems,
} from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberSet from "effect/FiberSet";
import * as FiberMap from "effect/FiberMap";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as ProjectStore from "./ProjectStore.ts";
import {
  checkpointRestorePlace,
  isCheckpointRestoreIsolated,
  SHARED_WORKSPACE_RESTORE_MESSAGE,
} from "./CheckpointRestoreSafety.ts";
import { CheckpointServiceV2 } from "./CheckpointService.ts";
import {
  automaticDeliverySteers,
  CommandPolicyV2,
  followUpOwnerRun,
  goalCommandStopsActiveRun,
  isActiveRun,
  isNativeMaintenanceCommand,
  resolveMessageDispatchIntent,
} from "./CommandPolicy.ts";
import { CommandReceiptStoreV2, type CommandReceiptV2 } from "./CommandReceiptStore.ts";
import {
  ContextHandoffServiceV2,
  readForkHistoryItems,
  readInheritedHandoffItems,
  visibleConversationItems,
  visibleDeltaRunOrdinals,
} from "./ContextHandoffService.ts";
import { notificationTurnItem } from "@t3tools/provider-core/server/notification";
import { makeKeyedReactor } from "./KeyedReactor.ts";
import { makeRunEndReactions, type RunEndReaction } from "./RunEndReaction.ts";
import {
  cancelledBeforeStart,
  continuationFailedBeforePrompt,
  continuationStartRetryDelayMs,
  delegatedTaskAwaitsWork,
  delegatedTaskChildRun,
  delegatedTaskReadCovers,
  delegatedTaskResultRun,
  delegatedTaskRuns,
  isRestartContinuationSource,
  restartContinuationSource,
  restartNoteOnly,
  runOutcome,
} from "./RestartBackgroundNote.ts";
import {
  isBufferedProviderNotification,
  isUndeliveredMailboxSteer,
} from "./NotificationMailbox.ts";
import { EventSinkV2 } from "./EventSink.ts";
import {
  decideGuestMessageSend,
  guestShareAfterCancelledSend,
  releasedGuestAttachmentIds,
  withoutGuestAttribution,
  decideGuestShareCommand,
  type GuestShareCommand,
} from "./GuestShareDecider.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import {
  orchestrationRunReactorEventsTotal,
  withCommandMetrics,
} from "../observability/Metrics.ts";
import type { OrchestrationEffectRequestV2, PendingOrchestrationEffectV2 } from "./EffectOutbox.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import {
  DispatchModeLimit,
  exceededDispatchModeLimit,
  narrowToCeiling,
  type RuntimeModeCeiling,
  runtimeModeCeilingRefusal,
  threadsStartingWork,
} from "./DispatchModeLimit.ts";
import {
  applyToProjection,
  emptyProjection,
  threadShellFromProjection,
  ProjectionStoreV2,
  type ProjectionRecordField,
  type ProjectionTimelinePage,
  type ProjectionTimelinePageOptions,
  type ProjectionRecordFilter,
  type ProjectionRecords,
  type ProjectionCheckpointContext,
  type OlderSettledThreadsListing,
  type ActiveShellIndexEntry,
  type ActiveThreadListEntry,
  type OlderSettledThreadsQuery,
  type ShellSnapshotOptions,
  type RunsLatestFirstCursor,
  type ProjectionQueuedDeliveryCandidate,
  type DelegatedTaskRecords,
} from "./ProjectionStore.ts";
import { ProviderAdapterRegistryV2 } from "./ProviderAdapterRegistry.ts";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import * as RunExecutionService from "./RunExecutionService.ts";
import { RUN_PREPARATION_FAILED_CODE, RUN_START_FAILED_CODE } from "./RunFailureCodes.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { ProviderSwitchServiceV2 } from "./ProviderSwitchService.ts";
import {
  decideQueuedDelivery,
  QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX,
  QUEUED_DELIVERY_PROMOTION_GRACE,
  QUEUED_DELIVERY_PROMOTION_SPACING,
  QUEUED_DELIVERY_PROMOTION_TICK,
  queuedDeliveryTarget,
  startsWithSlashCommand,
} from "./QueuedDeliveryPromotion.ts";
import { isHiddenWake, nextQueuedRun, queuedRunsInDeliveryOrder } from "./QueuedRunOrder.ts";
import {
  type CompletionWakeOwnership,
  completionWakeOwnership,
  completionWakeParts,
  withCompletionWakePart,
  withoutCompletionWakePart,
} from "./CompletionWakeParts.ts";
import { isRuntimePolicyAboveCeilingError, RuntimePolicyV2 } from "./RuntimePolicy.ts";
import {
  endOrphanedNativeSubagent,
  endRunlessRootTurns,
  makeSubagentChildThread,
  subagentResultForRun,
  delegatedTaskProgress,
  subagentThreadTitle,
} from "@t3tools/provider-core/server/subagentProjection";
import {
  forkableSourceRunStatusError,
  isForkableSourceRunStatus,
  ThreadForkServiceV2,
} from "./ThreadForkService.ts";
import { planThreadDeletion } from "./ThreadDeletion.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { forkParked } from "../serverActivation.ts";
import * as ServerSettings from "../serverSettings.ts";

const isMessageQuestionAnswer = Schema.is(
  Schema.Union([TrimmedNonEmptyString, Schema.NonEmptyArray(TrimmedNonEmptyString)]),
);

export class OrchestratorDispatchError extends Schema.TaggedError<OrchestratorDispatchError>()(
  "OrchestratorDispatchError",
  {
    commandId: CommandId,
    commandType: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to dispatch orchestration command ${this.commandType} (${this.commandId}).`;
  }
}

export class OrchestratorThreadAlreadyExistsError extends Schema.TaggedError<OrchestratorThreadAlreadyExistsError>()(
  "OrchestratorThreadAlreadyExistsError",
  { commandId: CommandId, threadId: ThreadId },
) {
  override get message(): string {
    return `Thread ${this.threadId} already exists. Use a new thread id.`;
  }
}

export class OrchestratorCommandRejectedError extends Schema.TaggedError<OrchestratorCommandRejectedError>()(
  "OrchestratorCommandRejectedError",
  { commandId: CommandId, commandType: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {
  override get message(): string {
    return `Orchestration command ${this.commandType} (${this.commandId}) was rejected before commit.`;
  }
}

export class OrchestratorProjectionError extends Schema.TaggedError<OrchestratorProjectionError>()(
  "OrchestratorProjectionError",
  {
    threadId: ThreadId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Failed to load orchestration projection for thread ${this.threadId}.`;
  }
}

const isOrchestratorProjectionError = Schema.is(OrchestratorProjectionError);

export class OrchestratorDomainEventStreamError extends Schema.TaggedError<OrchestratorDomainEventStreamError>()(
  "OrchestratorDomainEventStreamError",
  {
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return "Failed while streaming orchestration domain events.";
  }
}

export class OrchestratorProviderAdapterError extends Schema.TaggedError<OrchestratorProviderAdapterError>()(
  "OrchestratorProviderAdapterError",
  {
    commandId: CommandId,
    providerInstanceId: ProviderInstanceId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `Provider adapter failed while dispatching orchestration command ${this.commandId}.`;
  }
}

export class OrchestratorSubagentThreadReadOnlyError extends Schema.TaggedError<OrchestratorSubagentThreadReadOnlyError>()(
  "OrchestratorSubagentThreadReadOnlyError",
  { commandId: CommandId, threadId: ThreadId },
) {
  override get message(): string {
    return "This subagent is run by its provider and cannot take messages. Message the parent thread instead.";
  }
}

export class OrchestratorThreadAboveModeLimitError extends Schema.TaggedError<OrchestratorThreadAboveModeLimitError>()(
  "OrchestratorThreadAboveModeLimitError",
  {
    commandId: CommandId,
    threadId: ThreadId,
    mode: Schema.Literals(["runtime", "interaction"]),
    runtimeMode: RuntimeMode,
    interactionMode: ProviderInteractionMode,
  },
) {
  override get message(): string {
    return `Thread ${this.threadId} now runs in ${this.runtimeMode}/${this.interactionMode} mode, above what this caller may change.`;
  }
}

export class OrchestratorProviderSelectionBusyError extends Schema.TaggedError<OrchestratorProviderSelectionBusyError>()(
  "OrchestratorProviderSelectionBusyError",
  { commandId: CommandId, threadId: ThreadId },
) {
  override get message(): string {
    return `Thread ${this.threadId} has an active turn. Wait for it to finish, then retry the provider selection change.`;
  }
}

const isOrchestratorDispatchError = Schema.is(OrchestratorDispatchError);

/** The ceiling refusal under however many dispatch errors wrapped it on the way out. */
function runtimePolicyCeilingCause(error: unknown) {
  let current = error;
  while (isOrchestratorDispatchError(current)) current = current.cause;
  return isRuntimePolicyAboveCeilingError(current) ? current : undefined;
}

export class OrchestratorRuntimeModeAboveCeilingError extends Schema.TaggedError<OrchestratorRuntimeModeAboveCeilingError>()(
  "OrchestratorRuntimeModeAboveCeilingError",
  {
    commandId: CommandId,
    threadId: ThreadId,
    refused: Schema.Literals(["mode", "turn"]),
    runtimeMode: RuntimeMode,
    maxRuntimeMode: RuntimeMode,
    organization: Schema.String,
  },
) {
  override get message(): string {
    const limit = managedRuntimeModeCeilingMessage(this.organization, this.maxRuntimeMode);
    return this.refused === "turn" ? `${limit} Switch this thread's mode to continue.` : limit;
  }
}

export class OrchestratorCommandPreviouslyRejectedError extends Schema.TaggedError<OrchestratorCommandPreviouslyRejectedError>()(
  "OrchestratorCommandPreviouslyRejectedError",
  {
    commandId: CommandId,
    commandType: Schema.String,
    detail: Schema.String,
  },
) {
  override get message(): string {
    return `Command ${this.commandId} was previously rejected: ${this.detail}`;
  }
}

export class OrchestratorCommandIdConflictError extends Schema.TaggedError<OrchestratorCommandIdConflictError>()(
  "OrchestratorCommandIdConflictError",
  {
    commandId: CommandId,
    commandType: Schema.String,
    receiptThreadId: ThreadId,
    commandThreadId: ThreadId,
  },
) {
  override get message(): string {
    return `Command ${this.commandId} was already handled for thread ${this.receiptThreadId} and cannot be replayed for ${this.commandThreadId}.`;
  }
}

export const OrchestratorV2Error = Schema.Union([
  OrchestratorDispatchError,
  OrchestratorThreadAlreadyExistsError,
  OrchestratorCommandRejectedError,
  OrchestratorProjectionError,
  OrchestratorDomainEventStreamError,
  OrchestratorProviderAdapterError,
  OrchestratorCommandPreviouslyRejectedError,
  OrchestratorCommandIdConflictError,
  OrchestratorSubagentThreadReadOnlyError,
  OrchestratorThreadAboveModeLimitError,
  OrchestratorProviderSelectionBusyError,
  OrchestratorRuntimeModeAboveCeilingError,
]);
export type OrchestratorV2Error = typeof OrchestratorV2Error.Type;

export interface OrchestratorV2DispatchResult {
  readonly sequence: number;
  readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
  /** The command id was already accepted; analytics skip it. */
  readonly replayed?: true;
}

export interface OrchestratorV2Shape {
  readonly resumeQueuedRuns: Effect.Effect<number, OrchestratorV2Error>;
  readonly recoverDelegatedTasks: Effect.Effect<void>;
  /** At startup, after recovery: queues pull request news a stop kept from its provider again. */
  readonly recoverPullRequestWakes: Effect.Effect<void>;
  readonly recoverDelegatedTask: (
    threadId: ThreadId,
    options?: { readonly sourceRunId?: RunId; readonly completedRunId?: RunId },
  ) => Effect.Effect<void, OrchestratorProjectionError>;
  /** Commits cancellation with the child's Stop after descendants accept stopping. */
  readonly cancelDelegatedTask: <E, R>(input: {
    readonly parentThreadId: ThreadId;
    readonly taskId: NodeId;
    readonly command: Extract<OrchestrationV2ServerCommand, { readonly type: "thread.stop" }>;
    readonly stopDescendants: (
      dispatch: OrchestratorV2Shape["dispatch"],
    ) => Effect.Effect<void, E, R>;
  }) => Effect.Effect<void, E | OrchestratorV2Error, R>;
  readonly delegatedTaskResultPending: (
    childThreadId: ThreadId,
    taskId?: NodeId,
  ) => Effect.Effect<boolean, OrchestratorProjectionError>;
  readonly restartContinuationPending: (run: OrchestrationV2Run) => Effect.Effect<boolean>;
  /** Declined continuations emit no thread event; this still wakes waiters. */
  readonly awaitRestartContinuation: (run: OrchestrationV2Run) => Effect.Effect<void>;
  readonly dispatch: (
    command: OrchestrationV2ServerCommand,
  ) => Effect.Effect<OrchestratorV2DispatchResult, OrchestratorV2Error>;
  readonly autoArmLimitRecovery: (input: {
    readonly commandId: CommandId;
    readonly threadId: ThreadId;
    readonly limitRecovery: OrchestrationV2LimitRecoveryUpdate;
  }) => Effect.Effect<void, OrchestratorV2Error>;
  readonly searchThreadStream: (
    input: OrchestrationV2SearchThreadInput,
  ) => Stream.Stream<OrchestrationV2SearchThreadResult, OrchestratorV2Error>;
  readonly searchThread: (
    input: OrchestrationV2SearchThreadInput,
  ) => Effect.Effect<OrchestrationV2SearchThreadResult, OrchestratorV2Error>;
  readonly getThreadHistoryPage: (
    threadId: ThreadId,
    cursor: string,
    throughEntryId?: string,
    conversationOnly?: boolean,
    workMode?: "deferred" | "work",
  ) => Effect.Effect<OrchestrationV2ThreadHistoryPage, OrchestratorV2Error>;
  readonly getTimelinePage: (
    threadId: ThreadId,
    options: ProjectionTimelinePageOptions,
  ) => Effect.Effect<ProjectionTimelinePage, OrchestratorProjectionError>;
  readonly getMessageCount: (threadId: ThreadId) => Effect.Effect<number, OrchestratorV2Error>;
  readonly getMessagesById: (
    threadId: ThreadId,
    messageIds: ReadonlyArray<MessageId>,
  ) => Effect.Effect<
    ReadonlyMap<MessageId, OrchestrationV2ConversationMessage>,
    OrchestratorV2Error
  >;
  readonly getTranscriptByteFloor: (
    threadId: ThreadId,
    limit: number,
  ) => Effect.Effect<number, OrchestratorV2Error>;
  readonly getTurnItem: (input: {
    readonly threadId: ThreadId;
    readonly itemId: TurnItemId;
  }) => Effect.Effect<OrchestrationV2TurnItem | null, OrchestratorV2Error>;
  readonly getThreadRecords: <K extends ProjectionRecordField>(
    threadId: ThreadId,
    fields: ReadonlyArray<K>,
    filter?: ProjectionRecordFilter,
  ) => Effect.Effect<ProjectionRecords<K>, OrchestratorV2Error>;
  readonly listAppOwnedChildThreadIds: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<ThreadId>, OrchestratorV2Error>;
  readonly getThreadProjection: (
    threadId: ThreadId,
  ) => Effect.Effect<OrchestrationV2ThreadProjection, OrchestratorV2Error>;
  readonly getCheckpointContext: (
    threadId: ThreadId,
  ) => Effect.Effect<ProjectionCheckpointContext, OrchestratorV2Error>;
  readonly getThreadSnapshot: (threadId: ThreadId) => Effect.Effect<
    {
      readonly schemaVersion: number;
      readonly snapshotSequence: number;
      readonly projection: OrchestrationV2ThreadProjection;
    },
    OrchestratorV2Error
  >;
  readonly getThreadSnapshotWindow: (
    threadId: ThreadId,
    options: Parameters<ProjectionStoreV2["Service"]["getThreadSnapshotWindow"]>[1],
  ) => Effect.Effect<
    {
      readonly schemaVersion: number;
      readonly snapshotSequence: number;
      readonly projection: OrchestrationV2ThreadProjection;
    },
    OrchestratorV2Error
  >;
  readonly getPullRequestSnapshot: (
    options: Parameters<ProjectionStoreV2["Service"]["getPullRequestSnapshot"]>[0],
  ) => Effect.Effect<
    Effect.Success<ReturnType<ProjectionStoreV2["Service"]["getPullRequestSnapshot"]>>,
    OrchestratorV2Error
  >;
  readonly getAgentAwarenessSnapshot: (
    threadId?: ThreadId,
  ) => Effect.Effect<
    Effect.Success<ReturnType<ProjectionStoreV2["Service"]["getAgentAwarenessSnapshot"]>>,
    OrchestratorV2Error
  >;
  readonly getShellSnapshot: (
    options?: ShellSnapshotOptions,
  ) => Effect.Effect<OrchestrationV2ThreadShellSnapshot, OrchestratorV2Error>;
  readonly listActiveSubagentThreadIds: () => Effect.Effect<
    ReadonlyArray<ThreadId>,
    OrchestratorV2Error
  >;
  readonly countThreads: () => Effect.Effect<number, OrchestratorV2Error>;
  readonly listActiveShellIndex: () => Effect.Effect<
    ReadonlyArray<ActiveShellIndexEntry>,
    OrchestratorV2Error
  >;
  readonly listActiveThreads: (
    projectId?: ProjectId,
  ) => Effect.Effect<ReadonlyArray<ActiveThreadListEntry>, OrchestratorV2Error>;
  readonly listOlderSettledThreads: (
    query: OlderSettledThreadsQuery,
  ) => Effect.Effect<OlderSettledThreadsListing, OrchestratorV2Error>;
  readonly readShellSnapshot: (
    options?: ShellSnapshotOptions,
  ) => Effect.Effect<
    Effect.Effect<OrchestrationV2ThreadShellSnapshot, OrchestratorV2Error>,
    OrchestratorV2Error
  >;
  readonly getThreadShell: (
    threadId: ThreadId,
  ) => Effect.Effect<OrchestrationV2ThreadShell | null, OrchestratorV2Error>;
  readonly findGuestShareThreadId: (
    shareId: GuestShareId,
  ) => Effect.Effect<ThreadId | null, OrchestratorV2Error>;
  /** Titles of the given threads that exist, archived ones included. */
  readonly getThreadLinkMetadata: (
    threadIds: ReadonlyArray<ThreadId>,
  ) => Effect.Effect<Record<ThreadId, OrchestrationV2ThreadLinkMetadata>, OrchestratorV2Error>;
  readonly getThreadTitles: (
    threadIds: ReadonlyArray<ThreadId>,
  ) => Effect.Effect<Record<ThreadId, string>, OrchestratorV2Error>;
  readonly getThreadEventSequence: (
    threadId: ThreadId,
  ) => Effect.Effect<number, OrchestratorV2Error>;
  readonly streamStoredEvents: Stream.Stream<OrchestrationV2StoredEvent, OrchestratorV2Error>;
  /** Events arrive projected for the wire and shared by every subscriber; never mutate them. */
  readonly streamStoredEventsFrom: (input?: {
    readonly threadId?: ThreadId;
    readonly afterSequence?: number;
    readonly eventType?: OrchestrationV2DomainEvent["type"];
  }) => Stream.Stream<OrchestrationV2StoredEvent, OrchestratorV2Error>;
  readonly streamDomainEvents: Stream.Stream<OrchestrationV2DomainEvent, OrchestratorV2Error>;
}

export class OrchestratorV2 extends Context.Service<OrchestratorV2, OrchestratorV2Shape>()(
  "t3/orchestration-v2/Orchestrator/OrchestratorV2",
) {}

/** Run ordinals can skip values: a queued run moves past newer runs when it starts. */
function nextRunOrdinal(projection: Pick<OrchestrationV2ThreadProjection, "runs">): number {
  return projection.runs.reduce((max, run) => Math.max(max, run.ordinal), 0) + 1;
}

/**
 * Stamp at wake start; queued prompts lack start times and delegated results jump the queue.
 * Auto-drained agent messages continue active work; explicit queue starts reset it.
 */
function wakeWorkStartedAt(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "subagents">,
  trigger: Pick<
    OrchestrationV2ConversationMessage,
    "createdBy" | "creationSource" | "senderThreadId"
  > & {
    readonly notification?: unknown;
    readonly delegatedCompletion?: unknown;
    readonly restartContinuationOfRunId?: RunId | undefined;
  },
  delivery: "immediate" | "automatic-queue" | "explicit-queue" = "immediate",
): Pick<OrchestrationV2Run, "workStartedAt"> {
  if (
    trigger.notification === undefined &&
    trigger.delegatedCompletion === undefined &&
    trigger.restartContinuationOfRunId === undefined &&
    !(
      trigger.createdBy === "agent" &&
      trigger.creationSource === "mcp" &&
      trigger.senderThreadId !== undefined &&
      (delivery === "automatic-queue" ||
        (delivery === "immediate" &&
          projection.subagents.some(
            (subagent) =>
              subagent.origin === "app_owned" && subagent.childThreadId === trigger.senderThreadId,
          )))
    )
  ) {
    return {};
  }
  const previous = projection.runs
    .flatMap((run) => (run.startedAt === null ? [] : [{ run, startedAt: run.startedAt }]))
    .toSorted(
      (left, right) =>
        DateTime.Order(right.startedAt, left.startedAt) || right.run.ordinal - left.run.ordinal,
    )[0]?.run;
  return previous === undefined ? {} : { workStartedAt: orchestrationV2RunWorkStartedAt(previous) };
}

const threadPullRequestLinksEqual = Schema.toEquivalence(Schema.NullOr(ThreadLinkedPullRequest));

/** Unfinished runs (including queued) or open requests block moving a thread to another project. */
export function threadHasUnfinishedWork(records: {
  readonly runs: ReadonlyArray<{ readonly status: string }>;
  readonly runtimeRequests: ReadonlyArray<{ readonly status: string }>;
}): boolean {
  return (
    records.runs.some(
      (run) =>
        run.status !== "completed" &&
        run.status !== "failed" &&
        run.status !== "cancelled" &&
        run.status !== "interrupted" &&
        run.status !== "rolled_back",
    ) || records.runtimeRequests.some((request) => request.status === "pending")
  );
}

/** A T3-delegated child's parent, whose lock a follow-up into the child takes first. */
export function appOwnedSubagentParentOf(
  thread: Pick<OrchestrationV2AppThread, "lineage" | "forkedFrom">,
): ThreadId | undefined {
  return thread.lineage.relationshipToParent === "subagent" &&
    thread.lineage.parentThreadId !== null &&
    thread.forkedFrom?.type === "node"
    ? thread.lineage.parentThreadId
    : undefined;
}

function commandThreadId(command: OrchestrationV2ServerCommand): ThreadId {
  switch (command.type) {
    case "thread.create":
    case "thread.archive":
    case "thread.unarchive":
    case "thread.delete":
    case "thread.guest-share.open":
    case "thread.guest-share.redeem":
    case "thread.guest-share.close":
    case "thread.guest-message.submit":
    case "thread.guest-message.dismiss":
    case "thread.settle":
    case "thread.auto-settle":
    case "thread.unsettle":
    case "thread.snooze":
    case "thread.unsnooze":
    case "thread.auto-settle.set":
    case "thread.pin":
    case "thread.unpin":
    case "thread.pin.reorder":
    case "thread.active.reorder":
    case "thread.tag.set":
    case "thread.visit":
    case "thread.mark-unread":
    case "thread.metadata.update":
    case "thread.pull-request.link":
    case "thread.pull-request.unlink":
    case "thread.pull-request-link.sync":
    case "thread.pull-request.watch":
    case "thread.pull-request-watch.sync":
    case "thread.pull-request-watch.checks-reopen":
    case "thread.pull-request.sync":
    case "thread.title.regeneration.complete":
    case "thread.project.adopt-folder":
    case "thread.runtime-mode.set":
    case "thread.interaction-mode.set":
    case "thread.model-selection.set":
    case "provider-session.detach":
    case "message.dispatch":
    case "notification.delivery.accept":
    case "prepared-run.release":
    case "prepared-run.progress":
    case "prepared-run.fail":
    case "prepared-run.retry":
    case "subagent.stop":
    case "run.interrupt":
    case "queued-message.promote-to-steer":
    case "queued-run.start":
    case "queue.resume":
    case "queued-run.reorder":
    case "queued-run.cancel":
    case "queued-run.edit":
    case "runtime-request.respond":
    case "runtime-request.delivery.fail":
    case "thread.user-input.dismiss":
    case "checkpoint.rollback":
    case "checkpoint.rollback.fail":
    case "thread.background-work.settle":
    case "thread.interrupt.unresponsive":
    case "thread.stop":
    case "provider.switch":
      return command.threadId;
    case "delegated_task.request":
    case "delegated_task.remote.request":
    case "delegated_task.remote.complete":
    case "delegated_task.remote.stop":
    case "delegated_task.wake-policy":
    case "delegated_task.completion-delivery.acknowledge":
    case "delegated_task.completion-delivery.dispose":
    case "thread.created.record":
      return command.parentThreadId;
    case "secret_request.record":
    case "link_request.record":
      return command.threadId;
    case "thread.fork":
    case "thread.merge_back":
      return command.targetThreadId;
  }
}

// Validate before resolving the request: adapters may otherwise coerce unusable answers.
const decodeUserInputAnswers = Schema.decodeUnknownEffect(
  Schema.Record(
    Schema.String,
    Schema.Union([
      Schema.String,
      Schema.Array(Schema.String),
      Schema.Struct({ answers: Schema.Array(Schema.String) }),
    ]),
  ),
);

function pendingThreadTitleGenerationEffect(
  commandId: CommandId,
  threadId: ThreadId,
  kind:
    | { readonly type: "initial"; readonly messageId: MessageId }
    | { readonly type: "regenerate" },
): PendingOrchestrationEffectV2 {
  return {
    id: `effect:${commandId}:thread-title.generate`,
    commandId,
    threadId,
    request: { type: "thread-title.generate", kind },
  };
}

// Clients seed a new thread with its first message, and empty threads start as "New thread" or
// an agent's placeholder. Any other title on a thread without messages was chosen on purpose.
// Forks and side chats are titled from their source history instead.
function hasTitleSetBeforeFirstMessage(
  thread: OrchestrationV2AppThread,
  titleSeed: string | undefined,
): boolean {
  const title = thread.title.trim();
  return (
    thread.forkedFrom === null &&
    thread.lineage.sideChat !== true &&
    title.length > 0 &&
    title !== "New thread" &&
    !isPlaceholderThreadTitle(title) &&
    title !== titleSeed?.trim()
  );
}

function delegatedTasksStopEffect(command: {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly reason?: string | undefined;
}): PendingOrchestrationEffectV2 {
  return {
    id: `effect:${command.commandId}:delegated-tasks.stop`,
    commandId: command.commandId,
    threadId: command.threadId,
    request: {
      type: "delegated-tasks.stop",
      ...(command.reason === undefined ? {} : { reason: command.reason }),
    },
  };
}

const WORKSPACE_PREPARATION_INPUT = "Preparing workspace";
const RUNTIME_REQUEST_DELIVERY_FAILED_MESSAGE =
  "T3 Code could not confirm that your answer reached the agent. If the run stays stuck, stop it and try again.";

function withoutPreparationResult(
  item: Extract<OrchestrationV2TurnItem, { readonly type: "command_execution" }>,
) {
  const { output: _output, exitCode: _exitCode, outputIndicatesFailure: _failure, ...rest } = item;
  return rest;
}

/** Stop ended this task, so its result is not delivered to the parent agent. */
function cancelledAppOwnedTask(
  task: OrchestrationV2Subagent,
  now: DateTime.Utc,
): OrchestrationV2Subagent {
  return {
    ...task,
    status: "cancelled",
    completedAt: now,
    updatedAt: now,
    completionDelivery: { state: "disposed", observedByRunId: null },
  };
}

function askedForLinkedMachine(
  card: Extract<OrchestrationV2TurnItem, { readonly type: "link_request" }>,
  linked: Extract<OrchestrationV2InternalCommand, { readonly type: "link_request.record" }>,
): boolean {
  if (card.requestedAccess !== linked.requestedAccess) return false;
  if (card.environmentId !== undefined) return card.environmentId === linked.linkedEnvironmentId;
  if (card.url !== undefined) return linked.url !== undefined && card.url === linked.url;
  const normalize = (text: string | undefined) => text?.trim().toLowerCase() ?? "";
  const hint = normalize(card.hint);
  return hint !== "" && hint === normalize(linked.hint) && hint === normalize(linked.linkedLabel);
}

// A workspace change detaches the session, which can settle the calling run at once. Queuing its
// follow-up in a later command would let a delegated task report its result before that exists.
function metadataUpdateContinuation(
  command: Extract<OrchestrationV2Command, { readonly type: "thread.metadata.update" }>,
): Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }> | undefined {
  return command.continuation === undefined
    ? undefined
    : {
        type: "message.dispatch",
        commandId: command.commandId,
        threadId: command.threadId,
        messageId: command.continuation.messageId,
        text: command.continuation.text,
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        creationSource: "mcp",
      };
}

// A later command for the first message would leave the new thread committed with no run,
// and every client would show it idle until that command landed.
function threadCreateInitialMessage(
  command: Extract<OrchestrationV2Command, { readonly type: "thread.create" }>,
): Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }> | undefined {
  if (command.initialMessage === undefined) return undefined;
  const { workspaceStrategy, ...message } = command.initialMessage;
  return {
    ...message,
    type: "message.dispatch",
    commandId: command.commandId,
    threadId: command.threadId,
    modelSelection: command.modelSelection,
    dispatchMode:
      workspaceStrategy === undefined
        ? { type: "start_immediately" }
        : { type: "defer_start", workspaceStrategy },
    createdBy: command.createdBy,
    creationSource: command.creationSource,
  };
}

// Provider wakes can start follow-up work; monitor updates only report progress.
// Dispatch takes the parent's lock for follow-ups because intake writes the parent.
function mayStartSubagentFollowup(
  command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>,
): boolean {
  if (command.scheduledTaskId !== undefined || command.restartContinuationOfRunId !== undefined)
    return false;
  if (command.notification !== undefined)
    return (
      command.notification.source.kind !== "monitor" || command.notification.outcome !== "updated"
    );
  return (
    command.createdBy === "user" ||
    command.creationSource === "mcp" ||
    command.usageLimitContinuationOfRunId !== undefined
  );
}

/** Waiting runs only drain post-terminal output; their agent turn is over and still needs a wake. */
function isLiveRun(run: OrchestrationV2Run): boolean {
  return run.status === "preparing" || run.status === "starting" || run.status === "running";
}

function hasLiveRun(projection: Pick<OrchestrationV2ThreadProjection, "runs">): boolean {
  return projection.runs.some(isLiveRun);
}

function withPullRequestWatch(
  link: ThreadPullRequestLink,
  watch: ThreadPullRequestWatch | undefined,
): ThreadPullRequestLink {
  const { watch: previous, endedWatch, ...rest } = link;
  if (watch !== undefined) return { ...rest, watch };
  const ended = previous ?? endedWatch;
  return ended === undefined ? rest : { ...rest, endedWatch: ended };
}

function syncedPullRequestWatch(
  existing: ThreadPullRequestWatch | undefined,
  command: {
    readonly watch: ThreadPullRequestWatch | null;
    readonly checksPendingBefore?: boolean | undefined;
  },
): ThreadPullRequestWatch | undefined {
  if (command.watch === null) return undefined;
  // A push cleared the answer while this read was in flight, so its answer is the old head's.
  if (command.checksPendingBefore === undefined || existing?.checksPending !== undefined) {
    return command.watch;
  }
  return withoutChecksAnswer(command.watch);
}

const withoutChecksAnswer = ({
  checksPending: _answer,
  ...watch
}: ThreadPullRequestWatch): ThreadPullRequestWatch => watch;

function legacyPullRequestLink(
  thread: OrchestrationV2AppThread,
  linked: ThreadLinkedPullRequest,
  now: DateTime.Utc,
): ThreadPullRequestLink {
  const key = legacyThreadPullRequestKey(linked);
  const existing = threadPullRequestsOf(thread).find((link) =>
    threadPullRequestKeysEqual(link, key),
  );
  return withPullRequestWatch(
    {
      ...key,
      url: canonicalizeGitCafePullRequestUrl(linked.url),
      source: "manual",
      linkedAt: DateTime.formatIso(now),
      snapshot: null,
      stack: null,
      ...(existing?.repositoryWebUrl !== undefined && {
        repositoryWebUrl: existing.repositoryWebUrl,
      }),
      ...(existing?.endedWatch && { endedWatch: existing.endedWatch }),
    },
    existing?.watch,
  );
}

const DELEGATED_RESULT_INLINE_BYTES = 16_000;

function delegatedCompletionWakeDetail(
  taskIds: ReadonlyArray<string>,
  tasks: ReadonlyArray<OrchestrationV2Subagent>,
): string {
  let remainingBytes = DELEGATED_RESULT_INLINE_BYTES;
  return taskIds
    .map((taskId) => {
      const task = tasks.find((candidate) => candidate.id === taskId);
      const label = `Delegated task ${taskId}${task?.title == null ? "" : ` (${task.title})`}${task === undefined ? " reached a terminal state" : ` [${task.status}]`}`;
      if (task?.result != null) {
        const detail = `${label}:\n${task.result}`;
        const bytes = Buffer.byteLength(detail);
        if (bytes <= remainingBytes) {
          remainingBytes -= bytes;
          return detail;
        }
      }
      return `${label}. Use task_status with taskId ${taskId} to read the result.`;
    })
    .join("\n\n");
}

function isTerminalDelegatedTaskStatus(status: OrchestrationV2Subagent["status"]): boolean {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "interrupted"
  );
}

function delegatedTaskTerminalStatus(
  status: OrchestrationV2Run["status"],
): OrchestrationV2Subagent["status"] | null {
  switch (status) {
    case "completed":
    case "failed":
    case "cancelled":
    case "interrupted":
      return status;
    case "rolled_back":
      return "cancelled";
    case "preparing":
    case "queued":
    case "starting":
    case "running":
    case "waiting":
      return null;
  }
}

/** The interrupted or cancelled result run whose open questions finalizing a child dismisses. */
function delegatedTaskStoppedResultRun(
  progress: ReturnType<typeof delegatedTaskProgress>,
  childTasks: ReadonlyArray<OrchestrationV2Subagent>,
  originalTaskId: NodeId,
): OrchestrationV2Run | undefined {
  const { resultRun } = progress;
  if (resultRun === undefined) return undefined;
  // Only the run a cancelled task settled from ends with it. A task row read before its
  // finalize settled it from this result run has no such run yet.
  const resultTaskId = resultRun.delegatedTaskId ?? originalTaskId;
  const resultTask = childTasks.find((task) => task.id === resultTaskId);
  const taskCancelled =
    resultTask?.cancelledRunId !== undefined && resultTask.cancelledRunId === resultRun.id;
  if (taskCancelled ? progress.state === "working" : progress.state !== "result_available")
    return undefined;
  const status = delegatedTaskTerminalStatus(runOutcome(resultRun));
  return status === "interrupted" || status === "cancelled" || taskCancelled
    ? resultRun
    : undefined;
}

/** Failed worktree setup keeps the queue held so queued messages cannot start in the project checkout. */
function worktreeSetupPending(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "runs">,
): boolean {
  return (
    projection.thread.worktreePath === null &&
    projection.runs.some(
      (run) =>
        (run.status === "failed" || run.status === "interrupted") &&
        run.workspacePreparation?.type === "worktree",
    )
  );
}

// Delegated completions, monitor notifications and unpinned scheduled deliveries never choose a
// model, so a queued one runs on the thread's current pick rather than the one frozen at queue time.
function followsCurrentConversation(
  message:
    | Pick<
        OrchestrationV2ConversationMessage,
        | "createdBy"
        | "creationSource"
        | "notification"
        | "delegatedCompletion"
        | "followsThreadSelection"
      >
    | undefined,
): boolean {
  return (
    message?.delegatedCompletion !== undefined ||
    message?.followsThreadSelection === true ||
    (message?.notification?.source.kind === "monitor" && !isBufferedProviderNotification(message))
  );
}

type MergedSteerMessage = Pick<
  OrchestrationV2ConversationMessage,
  | "text"
  | "attachments"
  | "context"
  | "createdBy"
  | "creationSource"
  | "scheduledTaskId"
  | "senderThreadId"
  | "linkedSender"
  | "idleCompaction"
> & { readonly messageId: MessageId };

function queuedRunModelSelection(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "messages">,
  run: OrchestrationV2Run,
): ModelSelection {
  const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
  return followsCurrentConversation(message)
    ? projection.thread.modelSelection
    : run.modelSelection;
}

/**
 * The pick a message that names none inherits: the last one queued ahead of it. Wakes never pick
 * for a visible message; a due wake starts first by fairness, which must not decide its model.
 */
function queueTailModelSelection(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "messages">,
  incoming: Pick<OrchestrationV2ConversationMessage, "delegatedCompletion" | "notification">,
): ModelSelection {
  const messages = new Map(projection.messages.map((message) => [message.id, message]));
  const ahead = queuedRunsInDeliveryOrder(projection).filter((run) => {
    const message = messages.get(run.userMessageId);
    return isHiddenWake(incoming) || message === undefined || !isHiddenWake(message);
  });
  return (
    ahead.findLast((run) => !followsCurrentConversation(messages.get(run.userMessageId)))
      ?.modelSelection ?? projection.thread.modelSelection
  );
}

/**
 * The provider's latest run stopped on its usage limit and nothing ran after it.
 * With `now` (automatic starts), a run on another provider since does not lift
 * a reported reset still in the future; an unknown reset is lifted, as transient 429s are.
 */
function usageLimitHoldsQueue(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "turnItems" | "providerSessions">,
  providerInstanceId: ModelSelection["instanceId"],
  now?: DateTime.Utc,
): boolean {
  let latestSession: OrchestrationV2ThreadProjection["providerSessions"][number] | undefined;
  for (const session of projection.providerSessions) {
    if (
      session.providerInstanceId === providerInstanceId &&
      (latestSession === undefined ||
        DateTime.isGreaterThan(session.updatedAt, latestSession.updatedAt))
    ) {
      latestSession = session;
    }
  }
  const limited = usageLimitBlockedRun(
    projection.runs.filter((run) => run.providerInstanceId === providerInstanceId),
    projection.turnItems,
    latestSession?.lastError ?? null,
  );
  if (limited === null) return false;
  if (latestExecutedRun(projection.runs)?.id === limited.id) return true;
  if (now === undefined) return false;
  const resetAt = latestRootProviderFailure(limited, projection.turnItems)?.resetAt;
  return resetAt != null && Date.parse(resetAt) > DateTime.toEpochMillis(now);
}

function pullRequestSnapshotEndsWatch(
  snapshot: ThreadPullRequestLink["snapshot"],
  watchStartedAt: ThreadPullRequestWatch["startedAt"],
): boolean {
  return (
    snapshot?.state === "merged" ||
    // A restarted watch can see a closed snapshot from before the PR reopened.
    (snapshot?.state === "closed" &&
      DateTime.isGreaterThanOrEqualTo(
        DateTime.makeUnsafe(snapshot.syncedAt),
        DateTime.makeUnsafe(watchStartedAt),
      ))
  );
}

type MonitorPullRequestStamp = NonNullable<
  Extract<OrchestrationV2Notification["source"], { readonly kind: "monitor" }>["pullRequest"]
>;

function monitorPullRequestStamps(
  source: OrchestrationV2Notification["source"] | undefined,
): ReadonlyArray<MonitorPullRequestStamp> {
  if (source?.kind !== "monitor" || source.pullRequest === undefined) return [];
  return [source.pullRequest, ...(source.otherPullRequests ?? [])];
}

function isStalePullRequestWatchStamp(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "messages">,
  stamp: MonitorPullRequestStamp,
): boolean {
  const link = threadPullRequestsOf(projection.thread).find(
    (candidate) =>
      candidate.source !== "stack-dismissed" && threadPullRequestKeysEqual(candidate, stamp),
  );
  if (link === undefined) return true;
  if (stamp.watchEnded === true) {
    // The final notice outlives its watch, but a replacement watch supersedes it.
    const latestWatch = link.watch ?? link.endedWatch;
    return latestWatch?.startedAt !== stamp.watchStartedAt;
  }
  if (link.watch === undefined) {
    // A watch the server ended with a final notice still owes the wakes it queued before it.
    const endedWithNotice = projection.messages.some((message) =>
      monitorPullRequestStamps(message.notification?.source).some(
        (ending) =>
          ending.watchEnded === true &&
          ending.watchStartedAt === stamp.watchStartedAt &&
          threadPullRequestKeysEqual(ending, stamp),
      ),
    );
    return (
      !endedWithNotice ||
      link.endedWatch?.startedAt !== stamp.watchStartedAt ||
      pullRequestSnapshotEndsWatch(link.snapshot, stamp.watchStartedAt)
    );
  }
  return (
    link.watch.startedAt !== stamp.watchStartedAt ||
    (stamp.headSha !== null && link.watch.headSha !== stamp.headSha) ||
    pullRequestSnapshotEndsWatch(link.snapshot, link.watch.startedAt)
  );
}

function isStalePullRequestWatchWake(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "messages">,
  run: OrchestrationV2Run,
): boolean {
  const stamps = monitorPullRequestStamps(
    projection.messages.find((message) => message.id === run.userMessageId)?.notification?.source,
  );
  return (
    stamps.length > 0 && stamps.every((stamp) => isStalePullRequestWatchStamp(projection, stamp))
  );
}

/** A live wake's restart settles its cohort, which needs the parent runs a bounded read left out. */
function liveWakeParentUnread(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "messages">,
): boolean {
  return projection.runs.some((run) => {
    const ownership = isLiveRun(run)
      ? projection.messages.find((message) => message.id === run.userMessageId)?.delegatedCompletion
      : undefined;
    return (
      ownership !== undefined &&
      completionWakeParts(ownership).some(
        (part) => !projection.runs.some((candidate) => candidate.id === part.parentRunId),
      )
    );
  });
}

/** Whether the wake's staleness looks for a final notice among all of the thread's messages. */
function pullRequestWakeReadsNotices(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "messages">,
  run: OrchestrationV2Run,
): boolean {
  const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
  return monitorPullRequestStamps(message?.notification?.source).some((stamp) => {
    const link = threadPullRequestsOf(projection.thread).find(
      (candidate) =>
        candidate.source !== "stack-dismissed" && threadPullRequestKeysEqual(candidate, stamp),
    );
    return link !== undefined && stamp.watchEnded !== true && link.watch === undefined;
  });
}

/**
 * Whether starting `run` decides on history a queue-start read leaves out: a v1 import's items,
 * a fresh session or a provider or model change (sessions to release, the switch plan), or a
 * handoff of the runs since another provider last ran.
 */
function queueStartReadsHistory(
  projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "providerThreads" | "runs" | "turnItems"
  >,
  run: OrchestrationV2Run,
  message: Pick<OrchestrationV2ConversationMessage, "notification" | "delegatedCompletion">,
): boolean {
  const activeProviderThread = dispatchProviderThread(
    projection,
    message,
    run.freshContext === true,
  );
  const latestHandoffRun = projection.runs.findLast(isHandoffSourceRun);
  return (
    projection.thread.historyOrigin === "v1_import" ||
    run.freshContext === true ||
    !modelSelectionsEqual(projection.thread.modelSelection, run.modelSelection) ||
    (activeProviderThread !== undefined &&
      activeProviderThread.providerInstanceId !== run.providerInstanceId) ||
    (latestHandoffRun !== undefined &&
      latestHandoffRun.providerInstanceId !== run.providerInstanceId)
  );
}

/**
 * A queued wake covering several pull requests, rebuilt from the ones still current, or
 * undefined when it needs no change.
 */
function currentPullRequestWatchWake(
  projection: Pick<OrchestrationV2ThreadProjection, "thread" | "messages">,
  run: OrchestrationV2Run,
): OrchestrationV2ConversationMessage | undefined {
  const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
  const stamps = monitorPullRequestStamps(message?.notification?.source);
  if (message === undefined || stamps.length < 2) return undefined;
  const current = stamps.filter((stamp) => !isStalePullRequestWatchStamp(projection, stamp));
  const [first, ...rest] = current;
  if (first === undefined || rest.length === stamps.length - 1) return undefined;
  const sections = current.flatMap(({ section }) => (section === undefined ? [] : [section]));
  if (sections.length !== current.length) return undefined;
  const wake = combinePullRequestWatchWakes(
    sections.map(({ text, outcome, summary }) => ({
      text,
      notification: { source: { kind: "monitor" }, outcome, summary },
    })) as [PullRequestWatchWake, ...Array<PullRequestWatchWake>],
  );
  return {
    ...message,
    text: wake.text,
    notification: {
      ...wake.notification,
      source: {
        kind: "monitor",
        pullRequest: rest.length === 0 ? withoutSection(first) : first,
        ...(rest.length === 0 ? {} : { otherPullRequests: rest }),
      },
    },
  };
}

function withoutSection({ section: _, ...stamp }: MonitorPullRequestStamp) {
  return stamp;
}

type WakeClass = "completion" | "pull-request";

// Merging takes other parent runs' results into a wake only while it carries at most this
// many; one parent run's own results stay together, as before. A merged prompt inlines 16 kB
// and names the rest (~200 B each), so 25 bounds the turn and the absorbing write, and drains
// any backlog 25 results a wake.
const MAX_MERGED_WAKE_ITEMS = 25;

// A startup sends again pull request news whose wake failed (the server stopped before it reacted)
// or was cut by a restart at most this long before it.
const PULL_REQUEST_WAKE_RETRY_WINDOW = Duration.hours(4);
// Restarts in a row that may cut the same news before its prompt went out: news whose start keeps
// crashing the server would otherwise be sent forever.
const MAX_PULL_REQUEST_WAKE_RESTARTS = 3;
const NOTICE_RETRY_MESSAGE_PREFIX = "message:notice-retry:";
const NOTICE_RETRY_COMMAND_PREFIX = "command:notice-retry:";

/** A thread that takes no pull request watch news: archived, settled, or a provider's subagent. */
function refusesPullRequestNews(
  thread: Pick<
    OrchestrationV2AppThread,
    "archivedAt" | "settledOverride" | "settledAt" | "lineage" | "creationSource"
  >,
): boolean {
  return (
    thread.archivedAt !== null ||
    thread.settledOverride === "settled" ||
    thread.settledAt !== null ||
    isProviderNativeSubagentThread(thread)
  );
}

/** The results or pull requests a wake carries. */
function mergedWakeItems(
  message: Pick<OrchestrationV2ConversationMessage, "notification" | "delegatedCompletion">,
): number {
  return (
    message.delegatedCompletion?.taskIds.length ??
    monitorPullRequestStamps(message.notification?.source).length
  );
}

/** Hidden wakes one run can carry together: delegated results, and pull request watch news. */
function mergeableWakeClass(
  message: Pick<
    OrchestrationV2ConversationMessage,
    "createdBy" | "creationSource" | "notification" | "delegatedCompletion"
  >,
): WakeClass | undefined {
  if (message.createdBy !== "agent" || message.creationSource !== "server") return undefined;
  if (message.delegatedCompletion !== undefined) return "completion";
  return monitorPullRequestStamps(message.notification?.source).length > 0
    ? "pull-request"
    : undefined;
}

/** Queued wakes of a class another can join: unstarted, not deferred steers, held as asked. */
function joinableQueuedWakes(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "messages">,
  wakeClass: WakeClass,
  held: boolean,
): ReadonlyArray<{
  readonly run: OrchestrationV2Run;
  readonly message: OrchestrationV2ConversationMessage;
}> {
  return queuedRunsInDeliveryOrder(projection).flatMap((run) => {
    if (run.steerWhenReadyRunId !== undefined || (run.queueHeld === true) !== held) return [];
    const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
    return message !== undefined &&
      message.runId === run.id &&
      mergeableWakeClass(message) === wakeClass
      ? [{ run, message }]
      : [];
  });
}

/** A queued pull request wake that also tells `incoming`'s news, each pull request in its section. */
function mergedPullRequestWatchWake(
  target: OrchestrationV2ConversationMessage,
  incoming: Pick<OrchestrationV2ConversationMessage, "text" | "notification">,
): OrchestrationV2ConversationMessage | undefined {
  const stamps = [target, incoming].flatMap((wake) => {
    const own = monitorPullRequestStamps(wake.notification?.source);
    const [only] = own;
    return own.length === 1 && only !== undefined && wake.notification !== undefined
      ? [
          {
            ...only,
            section: only.section ?? {
              text: wake.text,
              outcome: wake.notification.outcome,
              summary: wake.notification.summary,
            },
          },
        ]
      : own;
  });
  const sections = stamps.flatMap(({ section }) => (section === undefined ? [] : [section]));
  const [first, ...rest] = stamps;
  if (first === undefined || sections.length !== stamps.length) return undefined;
  const wake = combinePullRequestWatchWakes(
    sections.map(({ text, outcome, summary }) => ({
      text,
      notification: { source: { kind: "monitor" }, outcome, summary },
    })) as [PullRequestWatchWake, ...Array<PullRequestWatchWake>],
  );
  return {
    ...target,
    text: wake.text,
    notification: {
      ...wake.notification,
      source: { kind: "monitor", pullRequest: first, otherPullRequests: rest },
    },
  };
}

function queuedRunCancellationEvents(
  projection: Pick<OrchestrationV2ThreadProjection, "nodes" | "attempts">,
  run: OrchestrationV2Run,
  now: DateTime.Utc,
): ReadonlyArray<Omit<OrchestrationV2DomainEvent, "id">> {
  const rootNode = projection.nodes.find((node) => node.id === run.rootNodeId);
  const attempt = projection.attempts.find((candidate) => candidate.id === run.activeAttemptId);
  const events: Array<Omit<OrchestrationV2DomainEvent, "id">> = [
    {
      type: "run.updated",
      threadId: run.threadId,
      runId: run.id,
      ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: { ...run, status: "cancelled", queuePosition: null, completedAt: now },
    },
  ];
  if (attempt !== undefined && rootNode !== undefined) {
    events.push({
      type: "run-attempt.updated",
      threadId: run.threadId,
      runId: run.id,
      nodeId: rootNode.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: { ...attempt, status: "cancelled", completedAt: now },
    });
  }
  if (rootNode !== undefined) {
    events.push({
      type: "node.updated",
      threadId: run.threadId,
      runId: run.id,
      nodeId: rootNode.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: { ...rootNode, status: "cancelled", completedAt: now },
    });
  }
  return events;
}

function latestStableRun(
  projection: Pick<OrchestrationV2ThreadProjection, "runs">,
): OrchestrationV2Run | null {
  return (
    projection.runs
      .filter((run) => run.status === "completed" && run.checkpointId !== null)
      .toSorted((left, right) => right.ordinal - left.ordinal)[0] ?? null
  );
}

function runForSourcePoint(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "checkpoints">,
  sourcePoint: Extract<
    OrchestrationV2Command,
    { readonly type: "thread.fork" | "thread.merge_back" }
  >["sourcePoint"],
): OrchestrationV2Run | null {
  switch (sourcePoint.type) {
    case "latest_stable":
      return latestStableRun(projection);
    case "run":
      return projection.runs.find((run) => run.id === sourcePoint.runId) ?? null;
    case "checkpoint": {
      const checkpoint = projection.checkpoints.find(
        (candidate) => candidate.id === sourcePoint.checkpointId,
      );
      return checkpoint?.runId === null || checkpoint === undefined
        ? null
        : (projection.runs.find((run) => run.id === checkpoint.runId) ?? null);
    }
  }
}

function providerThreadForRun(
  projection: Pick<OrchestrationV2ThreadProjection, "providerThreads">,
  run: OrchestrationV2Run,
): OrchestrationV2ProviderThread | undefined {
  return run.providerThreadId === null
    ? undefined
    : projection.providerThreads.find((candidate) => candidate.id === run.providerThreadId);
}

function providerTurnForRun(
  projection: Pick<OrchestrationV2ThreadProjection, "providerTurns" | "attempts">,
  run: OrchestrationV2Run,
): OrchestrationV2ProviderTurn | undefined {
  if (run.activeAttemptId === null) {
    return undefined;
  }

  return (
    latestProviderTurnForAttempt(projection.providerTurns, run.activeAttemptId) ??
    projection.providerTurns.find((turn) => {
      const attempt = projection.attempts.find((candidate) => candidate.id === run.activeAttemptId);
      return attempt?.providerTurnId === turn.id;
    })
  );
}

function contextSourcePointForRun(
  projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "providerThreads" | "providerTurns" | "attempts"
  >,
  run: OrchestrationV2Run,
): OrchestrationV2ContextSourcePoint {
  const providerThread = providerThreadForRun(projection, run);
  const providerTurn = providerTurnForRun(projection, run);
  return {
    threadId: projection.thread.id,
    runId: run.id,
    ...(run.checkpointId === null ? {} : { checkpointId: run.checkpointId }),
    ...(providerThread?.nativeThreadRef === null || providerThread?.nativeThreadRef === undefined
      ? {}
      : { providerThreadRef: providerThread.nativeThreadRef }),
    ...(providerTurn?.nativeTurnRef === null || providerTurn?.nativeTurnRef === undefined
      ? {}
      : { providerTurnRef: providerTurn.nativeTurnRef }),
  };
}

function pendingForkTransferForThread(
  projection: Pick<OrchestrationV2ThreadProjection, "contextTransfers" | "thread">,
): OrchestrationV2ContextTransfer | undefined {
  return projection.contextTransfers.find(
    (transfer) =>
      transfer.type === "fork" &&
      transfer.targetThreadId === projection.thread.id &&
      transfer.status === "pending",
  );
}

function pendingMergeBackTransfersForThread(
  projection: Pick<OrchestrationV2ThreadProjection, "contextTransfers" | "thread">,
): ReadonlyArray<OrchestrationV2ContextTransfer> {
  return projection.contextTransfers.filter(
    (transfer) =>
      transfer.type === "merge_back" &&
      transfer.targetThreadId === projection.thread.id &&
      transfer.status === "pending",
  );
}

function latestContextTransfer(
  transfers: ReadonlyArray<OrchestrationV2ContextTransfer>,
): OrchestrationV2ContextTransfer | undefined {
  return transfers.reduce<OrchestrationV2ContextTransfer | undefined>((latest, transfer) => {
    if (latest === undefined) {
      return transfer;
    }
    return DateTime.toEpochMillis(transfer.updatedAt) >= DateTime.toEpochMillis(latest.updatedAt)
      ? transfer
      : latest;
  }, undefined);
}

export function shouldPrepareLegacyImportHandoff(input: {
  readonly hasNativeHistory?: boolean;
  readonly hasCompletedRun: boolean;
  readonly historyOrigin: OrchestrationV2AppThread["historyOrigin"];
  readonly legacyImportItemCount: number;
}): boolean {
  return (
    input.historyOrigin === "v1_import" &&
    !input.hasNativeHistory &&
    !input.hasCompletedRun &&
    input.legacyImportItemCount > 0
  );
}

function appendContextHandoffId(
  handoffIds: OrchestrationV2ProviderThread["handoffIds"],
  handoffId: OrchestrationV2ContextHandoff["id"] | null,
): OrchestrationV2ProviderThread["handoffIds"] {
  return handoffId === null ? handoffIds : Array.from(new Set([...handoffIds, handoffId]));
}

/** A fresh-session schedule run starts a side conversation; the thread's own one continues past it. */
function isScheduledFreshRun(
  run: Pick<OrchestrationV2Run, "freshContext" | "userMessageId">,
  turnItems: OrchestrationV2ThreadProjection["turnItems"],
): boolean {
  // Turn items, unlike messages, are not narrowed to the runs since the latest compaction.
  return (
    run.freshContext === true &&
    turnItems.some(
      (item) =>
        item.type === "user_message" &&
        item.messageId === run.userMessageId &&
        item.scheduledTaskId !== undefined,
    )
  );
}

/** Ordinal of the latest started fresh-context run. Runs before it never reach a provider again. */
function contextBoundaryOrdinal(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "turnItems">,
): number {
  let boundary = 0;
  for (const run of projection.runs) {
    if (
      run.freshContext === true &&
      run.status !== "queued" &&
      !cancelledBeforeStart(run) &&
      run.ordinal > boundary &&
      !isScheduledFreshRun(run, projection.turnItems)
    ) {
      boundary = run.ordinal;
    }
  }
  return boundary;
}

/** The provider thread a message continues: the active one, unless a fresh-session schedule run started it. */
function conversationProviderThread(
  projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "providerThreads" | "runs" | "turnItems"
  >,
): OrchestrationV2ProviderThread | undefined {
  const startedByScheduledFreshRun = (providerThread: OrchestrationV2ProviderThread) =>
    projection.runs.some(
      (run) =>
        run.ordinal === providerThread.firstRunOrdinal &&
        isScheduledFreshRun(run, projection.turnItems),
    );
  const active = projection.providerThreads.find(
    (candidate) => candidate.id === projection.thread.activeProviderThreadId,
  );
  if (active === undefined || !startedByScheduledFreshRun(active)) return active;
  const boundary = contextBoundaryOrdinal(projection);
  return (
    projection.providerThreads
      .filter(
        (candidate) =>
          candidate.appThreadId === projection.thread.id &&
          candidate.ownerNodeId === null &&
          candidate.lastRunOrdinal !== null &&
          !providerThreadPredatesBoundary(candidate, boundary) &&
          !startedByScheduledFreshRun(candidate),
      )
      .toSorted((left, right) => (right.lastRunOrdinal ?? 0) - (left.lastRunOrdinal ?? 0))[0] ??
    active
  );
}

/** Wakes and fresh starts act on the active session; a message continues the conversation. */
function dispatchProviderThread(
  projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "providerThreads" | "runs" | "turnItems"
  >,
  message: Pick<OrchestrationV2ConversationMessage, "notification" | "delegatedCompletion">,
  freshContext: boolean,
): OrchestrationV2ProviderThread | undefined {
  return freshContext ||
    message.notification !== undefined ||
    message.delegatedCompletion !== undefined
    ? projection.providerThreads.find(
        (candidate) => candidate.id === projection.thread.activeProviderThreadId,
      )
    : conversationProviderThread(projection);
}

/** Threads with no started run (imported, or reserved by another queued run) never cross it. */
function providerThreadPredatesBoundary(
  providerThread: Pick<OrchestrationV2ProviderThread, "firstRunOrdinal">,
  boundary: number,
): boolean {
  return (
    boundary > 0 &&
    (providerThread.firstRunOrdinal === null || providerThread.firstRunOrdinal < boundary)
  );
}

/** A provider that hosts one native thread per session keeps the old conversation alive otherwise. */
function sessionsReleasedByFreshContext(
  projection: Pick<OrchestrationV2ThreadProjection, "providerSessions">,
  activeProviderThread: OrchestrationV2ProviderThread | undefined,
): ReadonlyArray<OrchestrationV2ProviderSession> {
  const sessionId = activeProviderThread?.providerSessionId ?? null;
  return projection.providerSessions.filter(
    (session) =>
      session.id === sessionId &&
      session.status !== "stopped" &&
      session.status !== "error" &&
      !session.capabilities.sessions.supportsMultipleProviderThreadsPerSession,
  );
}

const FRESH_CONTEXT_DETACH_REASON = "Started a fresh session.";
/** How often idle threads are checked for a queue whose run-end start never came. */
const IDLE_QUEUE_CHECK_INTERVAL = Duration.seconds(30);
// A thread still blocked after a check is checked again after 1, 2, 4 … up to 32 intervals.
const IDLE_QUEUE_CHECK_MAX_BACKOFF = 32;

// Failures in a row past this many no longer lengthen wakeRetryAt's backoff.
const WAKE_RETRY_FAILURES_COUNTED = Math.log2(IDLE_QUEUE_CHECK_MAX_BACKOFF) + 1;

// Wakes stop being sent again after this many failed runs in a row (about an hour of
// wakeRetryAt's backoff): their results settle with a notice instead.
const MAX_WAKE_START_FAILURES = 8;

/** The thread's latest executed runs that failed, counted back to the last that did not. */
function failedRunsInARow(runs: ReadonlyArray<OrchestrationV2Run>): number {
  let failures = 0;
  for (const run of runs
    .filter(
      (run) => run.status !== "queued" && !(run.status === "cancelled" && run.startedAt === null),
    )
    .toSorted(
      (left, right) => Number(runRanAfter(right, left)) - Number(runRanAfter(left, right)),
    )) {
    if (run.status !== "failed") break;
    failures += 1;
  }
  return failures;
}

/**
 * When hidden wakes behind failed runs may start again: one idle-check interval after the last
 * failure, doubling with each failure in a row up to IDLE_QUEUE_CHECK_MAX_BACKOFF intervals.
 */
function wakeRetryAt(runs: ReadonlyArray<OrchestrationV2Run>, failed: OrchestrationV2Run) {
  const failures = failedRunsInARow(runs);
  const intervals = Math.min(2 ** Math.max(failures - 1, 0), IDLE_QUEUE_CHECK_MAX_BACKOFF);
  return DateTime.addDuration(
    failed.completedAt ?? failed.requestedAt,
    Duration.times(IDLE_QUEUE_CHECK_INTERVAL, intervals),
  );
}
/** Threads (or delegation families) whose run-end and steer reactions may run at once. */
export const REACTION_CONCURRENCY = 16;
const FAMILY_KEY_CACHE_SIZE = 10_000;
// Runs whose last status the run-end reactor remembers to tell a rewrite from a real end.
const RUN_END_REACTION_MEMORY = 50_000;
// A delegated child's latest runs, by ordinal and by end, that finalizing it reads first. A
// decision on an older run reads four times as many, up to the whole child.
const DELEGATED_TASK_RUN_WINDOW = 16;

function uniqueSessions(
  sessions: ReadonlyArray<OrchestrationV2ProviderSession>,
): ReadonlyArray<OrchestrationV2ProviderSession> {
  return Array.from(new Map(sessions.map((session) => [session.id, session])).values());
}

function rootProviderThreadsForProvider(
  projection: Pick<
    OrchestrationV2ThreadProjection,
    "providerThreads" | "thread" | "runs" | "turnItems"
  >,
  providerInstanceId: ModelSelection["instanceId"],
): ReadonlyArray<OrchestrationV2ProviderThread> {
  const boundary = contextBoundaryOrdinal(projection);
  return projection.providerThreads
    .filter(
      (providerThread) =>
        providerThread.providerInstanceId === providerInstanceId &&
        providerThread.appThreadId === projection.thread.id &&
        providerThread.ownerNodeId === null &&
        !providerThreadPredatesBoundary(providerThread, boundary),
    )
    .toSorted(
      (left, right) =>
        (right.lastRunOrdinal ?? 0) - (left.lastRunOrdinal ?? 0) ||
        DateTime.toEpochMillis(right.updatedAt) - DateTime.toEpochMillis(left.updatedAt),
    );
}

function isHandoffSourceRun(run: OrchestrationV2Run): boolean {
  return (
    run.status === "completed" ||
    run.status === "failed" ||
    run.status === "interrupted" ||
    // A queued request cancelled before it started never reached any provider.
    (run.status === "cancelled" && run.startedAt !== null)
  );
}

/**
 * What a message dispatch reads past its projection: from the projection itself when that is the
 * whole command projection, from the store when the dispatch read left the settled history out.
 */
interface MessageDispatchHistory {
  /** Every run of the thread, for a provider handoff. */
  readonly runs: (
    projection: Pick<OrchestrationV2ThreadProjection, "runs">,
  ) => Effect.Effect<ReadonlyArray<OrchestrationV2Run>, OrchestratorProjectionError>;
  /** `modelSwitchSince` over the whole thread. */
  readonly modelSwitch: (
    projection: Pick<OrchestrationV2ThreadProjection, "runs" | "turnItems">,
    to: ModelSelection,
  ) => Effect.Effect<ReturnType<typeof modelSwitchSince>, OrchestratorProjectionError>;
}

const projectedHistory: MessageDispatchHistory = {
  runs: (projection) => Effect.succeed(projection.runs),
  modelSwitch: (projection, to) => Effect.succeed(modelSwitchSince(projection, to)),
};

function lastDeliveredRunForProviderThread(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "providerTurns">,
  providerThreadId: OrchestrationV2ProviderThread["id"],
): OrchestrationV2Run | undefined {
  return projection.runs.findLast(
    (run) =>
      isHandoffSourceRun(run) &&
      run.providerThreadId === providerThreadId &&
      (run.status === "completed" ||
        projection.providerTurns.some((turn) => turn.runAttemptId === run.activeAttemptId)),
  );
}

type DyingNativeSubagent = {
  readonly projection: Pick<OrchestrationV2ThreadProjection, "nodes" | "subagents" | "turnItems">;
  readonly subagent: Pick<
    Extract<OrchestrationV2TurnItem, { readonly type: "subagent" }>,
    "subagentId" | "origin" | "childThreadId" | "providerInstanceId"
  >;
};

const nativeSubagentRef = (subagent: OrchestrationV2Subagent): DyingNativeSubagent["subagent"] => ({
  subagentId: subagent.id,
  origin: subagent.origin,
  childThreadId: subagent.childThreadId,
  providerInstanceId: subagent.providerInstanceId,
});

const makeOrchestrator = Effect.fn("orchestrationV2.Orchestrator.layer")(function* () {
  const checkpointService = yield* CheckpointServiceV2;
  const commandPolicy = yield* CommandPolicyV2;
  const contextHandoffService = yield* ContextHandoffServiceV2;
  const eventSink = yield* EventSinkV2;
  const commandReceipts = yield* CommandReceiptStoreV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const projectionStore = yield* ProjectionStoreV2;
  const effectOutbox = yield* EffectOutbox.EffectOutboxV2;
  // The thread's scopes decide which root scope the run's workspace uses: callers pass those of the
  // command's projection (stored, this command's pending events applied; none for a thread created
  // in it), so the choice reads nothing in the serialized section. The run records its own cwd.
  const runWorkspaces = new WeakMap<OrchestrationV2CheckpointScope, string>();
  const prepareRootRunScope = (
    input: Parameters<typeof checkpointService.prepareRootRunScope>[0],
  ) =>
    checkpointService
      .prepareRootRunScope(input)
      .pipe(Effect.tap((scope) => Effect.sync(() => runWorkspaces.set(scope, input.cwd))));
  // The cwd a run was scoped with: what it works in, whatever spelling its scope keeps.
  const runWorkspaceOf = (scope: OrchestrationV2CheckpointScope) =>
    runWorkspaces.get(scope) ?? scope.cwd;
  const nextTurnItemOrdinal = (
    projection: Pick<OrchestrationV2ThreadProjection, "thread"> &
      Partial<Pick<OrchestrationV2ThreadProjection, "turnItems">>,
  ) =>
    projectionStore.getNextTurnItemOrdinal(projection.thread.id).pipe(
      Effect.mapError(
        (cause) => new OrchestratorProjectionError({ threadId: projection.thread.id, cause }),
      ),
      Effect.map((next) =>
        (projection.turnItems ?? []).reduce(
          (ordinal, item) => Math.max(ordinal, item.ordinal + 1),
          next,
        ),
      ),
    );

  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const providerAdapters = yield* ProviderAdapterRegistryV2;
  const continuationRequests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
  const providerSessions = yield* ProviderSessionManagerV2;
  const providerSwitchService = yield* ProviderSwitchServiceV2;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const runtimePolicy = yield* RuntimePolicyV2;
  const threadForkService = yield* ThreadForkServiceV2;
  const threadDispatch = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const messageIntake = yield* KeyedLock.make<ThreadId>();
  // Read once: the policy is fixed for the server's life, and dispatch takes it as an input.
  const { managedPolicy } = serverSettings;
  const runtimeModeCeiling: RuntimeModeCeiling | undefined =
    managedPolicy?.limits?.maxRuntimeMode === undefined
      ? undefined
      : {
          organization: managedPolicy.organization,
          maxRuntimeMode: managedPolicy.limits.maxRuntimeMode,
        };

  /** The driver when this provider runs `/goal` natively, else undefined. */
  const goalDriverFor = Effect.fn("Orchestrator.goalDriverFor")(function* (
    command: Pick<OrchestrationV2ServerCommand, "commandId" | "type">,
    providerInstanceId: ProviderInstanceId,
  ) {
    const adapter = yield* providerAdapters.get(providerInstanceId).pipe(mapDispatchError(command));
    const { driver } = adapter;
    if (driver === "opencode" || driver === "antigravity") {
      const capabilities = yield* adapter.getCapabilities().pipe(mapDispatchError(command));
      return capabilities.turns.supportsNativeGoals === true ? driver : undefined;
    }
    return driver === "codex" || driver === "claudeAgent" ? driver : undefined;
  });

  /** Unsupported goal controls can become new objectives in native providers. */
  const ensureGoalCommandSupported = Effect.fn("Orchestrator.ensureGoalCommandSupported")(
    function* (
      command: Pick<OrchestrationV2ServerCommand, "commandId" | "type">,
      message: { readonly text: string; readonly attachments: ReadonlyArray<ChatAttachment> },
      providerInstanceId: ProviderInstanceId,
    ) {
      const goalCommand = parseGoalCommand(message);
      if (goalCommand === null || goalCommand.type === "set") return;
      const driver = yield* goalDriverFor(command, providerInstanceId);
      if (driver === "antigravity") {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause:
            "Antigravity accepts /goal followed by an objective. Interrupt the running turn, then ask Antigravity to stop the current goal.",
        });
      }
      if (goalCommand.type !== "pause" && goalCommand.type !== "resume") return;
      if (driver !== "claudeAgent") return;
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "Claude goals cannot be paused or resumed. Send /goal clear to remove the goal.",
      });
    },
  );

  const mapDispatchError =
    (command: Pick<OrchestrationV2ServerCommand, "commandId" | "type">) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, OrchestratorDispatchError, R> =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorDispatchError({
              commandId: command.commandId,
              commandType: command.type,
              cause,
            }),
        ),
      );

  const mapDelegatedCompletionError = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError(
        (cause) =>
          new OrchestratorDispatchError({
            commandId: CommandId.make("command:system:delegated-completion-delivery"),
            commandType: "delegated_task.completion-delivery",
            cause,
          }),
      ),
    );

  const providerSessionIdFor = (input: {
    readonly adapter: ProviderAdapter.ProviderAdapterV2["Service"];
    readonly providerInstanceId: ProviderInstanceId;
    readonly threadId: ThreadId;
  }) =>
    input.adapter.getCapabilities().pipe(
      Effect.flatMap((capabilities) =>
        capabilities.sessions.supportsMultipleProviderThreadsPerSession
          ? Effect.succeed(
              idAllocator.derive.providerSession({
                providerInstanceId: input.providerInstanceId,
              }),
            )
          : idAllocator.allocate.providerSession({
              providerInstanceId: input.providerInstanceId,
              threadId: input.threadId,
            }),
      ),
    );

  const makeEvent =
    /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
    Effect.fnUntraced(function* <Event extends OrchestrationV2DomainEvent>(
      command: OrchestrationV2ServerCommand,
      event: Omit<Event, "id">,
    ) {
      const eventId = yield* mapDispatchError(command)(
        idAllocator.allocate.event({
          threadId: event.threadId,
          commandId: command.commandId,
        }),
      );
      return {
        ...event,
        id: eventId,
      } as Event;
    });

  const emit =
    (events: Ref.Ref<Array<OrchestrationV2DomainEvent>>, command: OrchestrationV2ServerCommand) =>
    <Event extends OrchestrationV2DomainEvent>(event: Omit<Event, "id">) =>
      Effect.gen(function* () {
        const withId = yield* makeEvent(command, event);
        yield* Ref.update(events, (existing) => [...existing, withId]);
        return withId;
      });

  // Detaching kills background work running inside the provider process.
  // Probe even with an empty roster: Claude's native subagents never reach
  // pendingBackgroundTasks. Sessions shared across threads probe per thread.
  const sessionHasBackgroundWork = Effect.fn("Orchestrator.sessionHasBackgroundWork")(function* (
    detaching: OrchestrationV2ThreadProjection["providerSessions"][number],
    providerThreads: OrchestrationV2ThreadProjection["providerThreads"],
  ) {
    const session = yield* providerSessions
      .get(detaching.id)
      .pipe(Effect.orElseSucceed(() => Option.none()));
    if (Option.isNone(session)) return false;
    const runtime = session.value;
    const probes =
      !detaching.capabilities.sessions.supportsMultipleProviderThreadsPerSession &&
      runtime.hasPendingBackgroundWork !== undefined
        ? [runtime.hasPendingBackgroundWork]
        : providerThreads.flatMap((providerThread) =>
            providerThread.providerSessionId === detaching.id &&
            runtime.hasPendingBackgroundWorkForThread !== undefined
              ? [runtime.hasPendingBackgroundWorkForThread(providerThread)]
              : [],
          );
    for (const probe of probes) {
      // A failed probe cannot confirm that the work ended.
      if (yield* probe.pipe(Effect.catchCause(() => Effect.succeed(true)))) return true;
    }
    return false;
  });

  // Runs in every command transaction; avoid reading thousands of finished leaf nodes.
  const readCommandProjection = (
    threadId: ThreadId,
    recent?: ProjectionRecordFilter["sinceLastCompaction"],
  ) =>
    projectionStore
      .getThreadRecords(
        threadId,
        [
          "runs",
          "attempts",
          "nodes",
          "subagents",
          "providerSessions",
          "providerThreads",
          "providerTurns",
          "runtimeRequests",
          "messages",
          "turnItems",
          "checkpointScopes",
          "contextTransfers",
        ],
        {
          turnItemTypes: ["user_message", "error"],
          messageRoles: ["user"],
          omitSettledLeafNodes: true,
          // Commands here read only undelivered or active tasks; a dispatcher thread keeps
          // thousands of delivered ones, each carrying its full prompt.
          omitSettledSubagents: true,
          // No command reads a settled task's node or its child's provider thread.
          omitSettledSubagentRecords: true,
          // Commands act on pending forks and merge-backs only; consumed ones stay unread.
          contextTransferStatuses: ["pending"],
          ...(recent === undefined ? {} : { sinceLastCompaction: recent }),
        },
      )
      .pipe(
        Effect.map((records): OrchestrationV2ThreadProjection => ({
          ...records,
          checkpoints: [],
          plans: [],
          contextHandoffs: [],
          visibleTurnItems: [],
          updatedAt: records.thread.updatedAt,
        })),
        Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
      );

  // The rows starting or promoting a queued run decides on; decisions that need history read the
  // whole command projection instead (see queueStartReadsHistory).
  const readQueuedRunProjection = (threadId: ThreadId, runIds: ReadonlyArray<RunId> = []) =>
    projectionStore
      .getQueuedRunRecords(threadId, {
        messageIds: [],
        runIds,
        latestExecutedRuns: WAKE_RETRY_FAILURES_COUNTED,
      })
      .pipe(
        Effect.map((records): OrchestrationV2ThreadProjection => ({
          ...records,
          checkpoints: [],
          plans: [],
          contextHandoffs: [],
          visibleTurnItems: [],
          updatedAt: records.thread.updatedAt,
        })),
        Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
      );

  // Command reads omit settled tasks, but a completion notice counts its whole cohort; a
  // merged wake names the tasks of many parent runs, so it reads just those.
  const notificationTasks = (
    threadId: ThreadId,
    message: Pick<OrchestrationV2ConversationMessage, "delegatedCompletion">,
    loaded: ReadonlyArray<OrchestrationV2Subagent>,
  ) =>
    message.delegatedCompletion === undefined
      ? Effect.succeed(loaded)
      : projectionStore
          .getThreadRecords(
            threadId,
            ["subagents"],
            message.delegatedCompletion.merged === undefined
              ? { subagentRunIds: [message.delegatedCompletion.parentRunId] }
              : { subagentIds: message.delegatedCompletion.taskIds },
          )
          .pipe(
            Effect.map((records) => records.subagents),
            Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
          );

  const readHandoffItems = (threadId: ThreadId, runIds?: ReadonlyArray<RunId | null>) =>
    projectionStore
      .getThreadRecords(
        threadId,
        ["turnItems"],
        runIds === undefined ? undefined : { turnItemRunIds: runIds },
      )
      .pipe(
        Effect.map((records) => records.turnItems),
        Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
      );

  const readInheritedItems = (thread: OrchestrationV2AppThread) =>
    readInheritedHandoffItems(projectionStore, thread).pipe(
      Effect.mapError((cause) => new OrchestratorProjectionError({ threadId: thread.id, cause })),
    );

  /** Includes tombstones, which still reserve their id. */
  const findThread = (threadId: ThreadId) =>
    projectionStore.getThread(threadId).pipe(
      Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
      Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
    );

  /** Side chats live inside their parent, so deleting the parent deletes them too. */
  const liveSideChatIds = Effect.fn("Orchestrator.liveSideChatIds")(
    function* (threadId: ThreadId) {
      const { contextTransfers } = yield* projectionStore
        .getThreadRecords(threadId, ["contextTransfers"])
        .pipe(
          Effect.catchTags({
            ProjectionStoreThreadNotFoundError: () => Effect.succeed({ contextTransfers: [] }),
          }),
        );
      const sideChatIds: Array<ThreadId> = [];
      for (const transfer of contextTransfers) {
        if (transfer.type !== "fork" || transfer.sourceThreadId !== threadId) continue;
        const child = yield* projectionStore
          .getThread(transfer.targetThreadId)
          .pipe(
            Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
          );
        if (child?.lineage.sideChat === true && child.deletedAt === null) {
          sideChatIds.push(child.id);
        }
      }
      return sideChatIds;
    },
    (effect, threadId) =>
      effect.pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause }))),
  );

  const getProjectionWithPendingEvents =
    /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
    Effect.fnUntraced(function* (
      threadId: ThreadId,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      recent?: ProjectionRecordFilter["sinceLastCompaction"],
    ) {
      const pending = (yield* Ref.get(events)).filter((event) => event.threadId === threadId);
      const stored = yield* Effect.option(readCommandProjection(threadId, recent));
      let projection: OrchestrationV2ThreadProjection;
      if (Option.isSome(stored)) {
        projection = stored.value;
      } else {
        const created = pending.find(
          (
            event,
          ): event is Extract<OrchestrationV2DomainEvent, { readonly type: "thread.created" }> =>
            event.type === "thread.created",
        );
        if (created === undefined) {
          return yield* new OrchestratorProjectionError({ threadId });
        }
        projection = emptyProjection(created);
      }

      for (const event of pending) {
        if (event.type === "thread.created" && projection.thread.id === event.payload.id) {
          projection = { ...projection, thread: event.payload, updatedAt: event.occurredAt };
          continue;
        }
        projection = applyToProjection(projection, event);
      }
      return projection;
    });

  const storedHistory = (threadId: ThreadId): MessageDispatchHistory => ({
    runs: () =>
      projectionStore.getThreadRecords(threadId, ["runs"]).pipe(
        Effect.map((records) => records.runs),
        Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
      ),
    // From the latest run back to the first one the timeline shows, not every run and message.
    modelSwitch: Effect.fnUntraced(
      function* (_projection: unknown, to: ModelSelection) {
        const runs: Array<OrchestrationV2Run> = [];
        const turnItems: Array<OrchestrationV2TurnItem> = [];
        let before: RunsLatestFirstCursor | undefined;
        let limit = 8;
        do {
          const page = yield* projectionStore.getRunsLatestFirst(threadId, {
            ...(before === undefined ? {} : { before }),
            limit,
          });
          runs.push(...page.runs);
          turnItems.push(...page.turnItems);
          before = page.next;
          limit = Math.min(limit * 2, 256);
        } while (before !== undefined && latestShownRun({ runs, turnItems }) === undefined);
        return modelSwitchSince({ runs, turnItems }, to);
      },
      Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
    ),
  });

  // A dispatch reads the rows it decides on, not the thread's settled history. Continuations,
  // goals, idle compactions, fresh sends, pending forks and merge-backs, v1 imports, and a
  // dispatch after its command already wrote this thread read the whole command projection, as
  // before; a provider switch rereads it in dispatchMessage.
  const readMessageDispatchProjection = Effect.fnUntraced(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    // A compacted thread reads every run and turn item, but attempts, nodes, messages and provider
    // turns only for its recent runs and the runs this command names. An idle compaction reads
    // in full: its busy check looks for a delegated result's message under any old run.
    const whole = getProjectionWithPendingEvents(
      command.threadId,
      events,
      command.idleCompaction === undefined
        ? {
            keepRunIds: [
              command.manualContinuationOfRunId,
              command.restartContinuationOfRunId,
              command.usageLimitContinuationOfRunId,
              command.steerFollowUpOfRunId,
              ...(command.dispatchMode.type === "steer_active" ||
              command.dispatchMode.type === "restart_active"
                ? [command.dispatchMode.targetRunId]
                : []),
            ].flatMap((runId) => (runId === undefined ? [] : [runId])),
            keepMessageId: command.messageId,
          }
        : undefined,
    ).pipe(Effect.map((projection) => ({ projection, history: projectedHistory })));
    if (
      command.manualContinuationOfRunId !== undefined ||
      command.usageLimitContinuationOfRunId !== undefined ||
      command.restartContinuationOfRunId !== undefined ||
      command.idleCompaction !== undefined ||
      command.freshContext === true ||
      parseGoalCommand(command) !== null ||
      (yield* Ref.get(events)).some((event) => event.threadId === command.threadId)
    ) {
      return yield* whole;
    }
    const stored = yield* Effect.option(
      projectionStore.getMessageDispatchRecords(command.threadId, {
        messageIds: [command.messageId],
        runIds: [
          ...(command.steerFollowUpOfRunId === undefined ? [] : [command.steerFollowUpOfRunId]),
          ...(command.delegatedCompletion === undefined
            ? []
            : [command.delegatedCompletion.parentRunId]),
          ...(command.dispatchMode.type === "steer_active" ||
          command.dispatchMode.type === "restart_active"
            ? [command.dispatchMode.targetRunId]
            : []),
        ],
      }),
    );
    if (Option.isNone(stored)) {
      return yield* new OrchestratorProjectionError({ threadId: command.threadId });
    }
    const projection: OrchestrationV2ThreadProjection = {
      ...stored.value,
      checkpoints: [],
      plans: [],
      contextHandoffs: [],
      visibleTurnItems: [],
      updatedAt: stored.value.thread.updatedAt,
    };
    // A restart settles the completion wake it replaces, which needs the wake's parent run.
    const mayRestart =
      command.dispatchMode.type === "steer_active" ||
      command.dispatchMode.type === "restart_active" ||
      command.deliveryIntent !== undefined;
    if (
      projection.thread.historyOrigin === "v1_import" ||
      pendingForkTransferForThread(projection) !== undefined ||
      pendingMergeBackTransfersForThread(projection).length > 0 ||
      (mayRestart && liveWakeParentUnread(projection))
    ) {
      return yield* whole;
    }
    return { projection, history: storedHistory(command.threadId) };
  });

  const makeSystemEvent =
    /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
    Effect.fnUntraced(function* <Event extends OrchestrationV2DomainEvent>(
      event: Omit<Event, "id">,
    ) {
      const eventId = yield* idAllocator.allocate.event({
        threadId: event.threadId,
      });
      return {
        ...event,
        id: eventId,
      } as Event;
    });

  const writeSystemEvents =
    /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
    Effect.fnUntraced(function* (
      events: ReadonlyArray<Omit<OrchestrationV2DomainEvent, "id">>,
      effects: ReadonlyArray<PendingOrchestrationEffectV2> = [],
      options: { readonly updateDelegatedCompletionOnly?: boolean } = {},
    ) {
      const withIds = yield* Effect.forEach(events, (event) => makeSystemEvent(event));
      yield* eventSink.writeWithEffects({ events: withIds, effects, ...options });
    });

  const completionDeliveryRun = (
    projection: Pick<OrchestrationV2ThreadProjection, "messages" | "runs">,
    delivery: OrchestrationV2DelegatedCompletionDelivery | null | undefined,
  ) => {
    if (delivery == null) return undefined;
    const message = projection.messages.find((candidate) => candidate.id === delivery.messageId);
    return projection.runs.find((candidate) => candidate.id === message?.runId);
  };

  const completionDeliveryMessage = (
    projection: Pick<OrchestrationV2ThreadProjection, "messages">,
    delivery: OrchestrationV2DelegatedCompletionDelivery | null | undefined,
  ) =>
    delivery === null || delivery === undefined
      ? undefined
      : projection.messages.find((candidate) => candidate.id === delivery.messageId);

  /** The wake carrying `ownership`, its text naming every task; tasks not loaded are read by id. */
  const completionWakeMessage = Effect.fnUntraced(function* (
    message: OrchestrationV2ConversationMessage,
    ownership: CompletionWakeOwnership,
    loaded: ReadonlyArray<OrchestrationV2Subagent>,
    now: DateTime.Utc,
  ) {
    const missing = ownership.taskIds.filter(
      (taskId) => !loaded.some((task) => task.id === taskId),
    );
    const tasks =
      missing.length === 0
        ? loaded
        : [
            ...loaded,
            ...(yield* projectionStore.getThreadRecords(message.threadId, ["subagents"], {
              subagentIds: missing,
            })).subagents,
          ];
    return {
      ...message,
      text: delegatedCompletionWakeDetail(ownership.taskIds, tasks),
      delegatedCompletion: ownership,
      updatedAt: now,
    } satisfies OrchestrationV2ConversationMessage;
  });

  const wakeMessageUpdated = (
    run: OrchestrationV2Run,
    message: OrchestrationV2ConversationMessage,
    occurredAt: DateTime.Utc,
  ) =>
    ({
      type: "message.updated",
      threadId: run.threadId,
      runId: run.id,
      ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
      providerInstanceId: run.providerInstanceId,
      occurredAt,
      payload: message,
    }) as const;

  const cohortRepointed = (
    parentRun: OrchestrationV2Run,
    messageId: MessageId,
    occurredAt: DateTime.Utc,
  ) =>
    ({
      type: "run.updated",
      threadId: parentRun.threadId,
      runId: parentRun.id,
      ...(parentRun.rootNodeId === null ? {} : { nodeId: parentRun.rootNodeId }),
      providerInstanceId: parentRun.providerInstanceId,
      occurredAt,
      payload: {
        ...parentRun,
        delegatedCompletion: {
          ...parentRun.delegatedCompletion!,
          delivery: { ...parentRun.delegatedCompletion!.delivery!, messageId },
        },
      },
    }) as const;

  /**
   * The cohorts that still own a completion wake, each with the tasks its delivery holds. A
   * cohort moved to another wake or settled since is not this wake's to deliver.
   */
  const currentCompletionWakeParts = (
    runs: ReadonlyArray<OrchestrationV2Run>,
    message: OrchestrationV2ConversationMessage,
  ) =>
    message.delegatedCompletion === undefined
      ? []
      : completionWakeParts(message.delegatedCompletion).flatMap((part) => {
          const parentRun = runs.find((run) => run.id === part.parentRunId);
          const delivery = parentRun?.delegatedCompletion?.delivery;
          return parentRun?.delegatedCompletion?.disposition === "open" &&
            delivery?.messageId === message.id &&
            delivery.generation === part.generation
            ? [{ parentRun, part: { ...part, taskIds: delivery.taskIds } }]
            : [];
        });

  const isUnobservedAcknowledgement = (
    command: OrchestrationV2ServerCommand,
  ): command is Extract<
    OrchestrationV2ServerCommand,
    { readonly type: "delegated_task.completion-delivery.acknowledge" }
  > & { readonly observedByRunId: null } =>
    command.type === "delegated_task.completion-delivery.acknowledge" &&
    command.observedByRunId === null;

  /** Parent run and wake message events that commit a planDelegatedCompletionDelivery result. */
  const delegatedCompletionPlanEvents = (
    threadId: ThreadId,
    plan: {
      readonly parentRun: OrchestrationV2Run | undefined;
      readonly message: OrchestrationV2ThreadProjection["messages"][number] | undefined;
    },
    threadProviderInstanceId: ProviderInstanceId,
    occurredAt: DateTime.Utc,
  ): ReadonlyArray<Omit<OrchestrationV2DomainEvent, "id">> => [
    ...(plan.parentRun === undefined
      ? []
      : [
          {
            type: "run.updated" as const,
            threadId,
            runId: plan.parentRun.id,
            ...(plan.parentRun.rootNodeId === null ? {} : { nodeId: plan.parentRun.rootNodeId }),
            providerInstanceId: plan.parentRun.providerInstanceId,
            occurredAt,
            payload: plan.parentRun,
          },
        ]),
    ...(plan.message === undefined
      ? []
      : [
          {
            type: "message.updated" as const,
            threadId,
            ...(plan.message.runId === null ? {} : { runId: plan.message.runId }),
            ...(plan.message.nodeId === null ? {} : { nodeId: plan.message.nodeId }),
            providerInstanceId: plan.parentRun?.providerInstanceId ?? threadProviderInstanceId,
            occurredAt,
            payload: plan.message,
          },
        ]),
  ];

  /** The parent records `planDelegatedCompletionDelivery` reads for one task: its run, and the queued wake's message, run and tasks. */
  const readDelegatedCompletionPlanRecords =
    /* The delegated task operation that calls this owns the span. */
    Effect.fnUntraced(function* (parentThreadId: ThreadId, task: OrchestrationV2Subagent) {
      const {
        thread,
        runs: [parentRun],
      } = yield* projectionStore.getThreadRecords(parentThreadId, ["runs"], {
        runIds: task.runId === null ? [] : [task.runId],
      });
      const delivery = parentRun?.delegatedCompletion?.delivery;
      if (delivery == null) return { thread, parentRun, runs: [], messages: [], subagents: [] };
      const { messages } = yield* projectionStore.getThreadRecords(parentThreadId, ["messages"], {
        messageIds: [delivery.messageId],
        messageRoles: ["user"],
      });
      const deliveryRunId = messages[0]?.runId;
      const { runs, subagents } = yield* projectionStore.getThreadRecords(
        parentThreadId,
        ["runs", "subagents"],
        {
          runIds: deliveryRunId == null ? [] : [deliveryRunId],
          subagentIds: [...delivery.taskIds, task.id],
        },
      );
      return { thread, parentRun, runs, messages, subagents };
    });

  const offerDelegatedCompletionDelivery =
    /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
    Effect.fnUntraced(function* (threadId: ThreadId, parentRunId: RunId) {
      const controls = yield* projectionStore.getThreadRecords(threadId, ["runs"], {
        runIds: [parentRunId],
      });
      const parentRun = controls.runs[0];
      const cohort = parentRun?.delegatedCompletion;
      const delivery = cohort?.delivery;
      if (
        parentRun === undefined ||
        cohort?.disposition !== "open" ||
        delivery == null ||
        delivery.taskIds.length === 0 ||
        controls.thread.archivedAt !== null ||
        controls.thread.deletedAt !== null
      )
        return;
      const messages = yield* projectionStore.getThreadRecords(threadId, ["messages"], {
        messageIds: [delivery.messageId],
      });
      const message = messages.messages[0];
      if (message !== undefined) {
        if (message.delegatedCompletion === undefined || message.runId === null) return;
        const runs = yield* projectionStore.getThreadRecords(threadId, ["runs"], {
          runIds: [message.runId],
        });
        const run = runs.runs[0];
        const turns = yield* projectionStore.getThreadRecords(threadId, ["providerTurns"], {
          providerTurnAttemptIds: run?.activeAttemptId == null ? [] : [run.activeAttemptId],
        });
        if (!isUndeliveredMailboxSteer({ ...messages, ...runs, ...turns }, delivery.messageId))
          return;
      }
      const projection = yield* projectionStore.getThreadRecords(threadId, ["providerThreads"], {
        providerThreadIds: parentRun.providerThreadId === null ? [] : [parentRun.providerThreadId],
      });
      const providerThread =
        parentRun.providerThreadId === null
          ? undefined
          : projection.providerThreads.find(
              (candidate) => candidate.id === parentRun.providerThreadId,
            );
      if (providerThread === undefined) {
        return;
      }
      yield* continuationRequests.offer({
        threadId,
        providerThreadId: providerThread.id,
        driver: providerThread.driver,
        detail: null,
        delivery: "message_text",
        delegatedCompletion: {
          parentRunId,
          generation: delivery.generation,
          messageId: delivery.messageId,
        },
      });
    });

  const offerDelegatedCompletionDeliveries =
    /* This helper runs for individual orchestration events; the enclosing operation owns the span. */
    Effect.fnUntraced(function* (threadId: ThreadId) {
      const projection = yield* projectionStore.getThreadRecords(threadId, ["runs"]);
      for (const run of projection.runs) {
        if (
          run.delegatedCompletion?.delivery !== undefined &&
          run.delegatedCompletion.delivery !== null
        ) {
          yield* offerDelegatedCompletionDelivery(threadId, run.id);
        }
      }
    });

  /** Marks a wake the provider received delivered and reserves the next one for pending siblings. */
  const acceptDelegatedCompletionDelivery = Effect.fn(
    "Orchestrator.acceptDelegatedCompletionDelivery",
  )(function* (input: {
    readonly command: OrchestrationV2ServerCommand;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly projection: Pick<
      OrchestrationV2ThreadProjection,
      "thread" | "runs" | "messages" | "subagents"
    >;
    readonly messageId: OrchestrationV2ConversationMessage["id"];
    readonly now: DateTime.Utc;
  }) {
    const { projection, now } = input;
    const message = projection.messages.find((row) => row.id === input.messageId);
    const ownership = message?.delegatedCompletion;
    if (ownership === undefined) return undefined;
    const threadId = projection.thread.id;
    const emitEvent = emit(input.events, input.command);
    let acceptedParentRunId: RunId | undefined;
    for (const part of completionWakeParts(ownership)) {
      const parentRun = projection.runs.find((run) => run.id === part.parentRunId);
      const cohort = parentRun?.delegatedCompletion;
      const delivery = cohort?.delivery;
      if (
        parentRun === undefined ||
        cohort === undefined ||
        cohort.disposition !== "open" ||
        delivery == null ||
        delivery.messageId !== input.messageId ||
        delivery.generation !== part.generation
      ) {
        continue;
      }
      const parentIsLive = hasLiveRun({ runs: [parentRun] });
      const pendingTaskIds =
        projection.thread.archivedAt === null && projection.thread.deletedAt === null
          ? projection.subagents
              .filter(
                (task) =>
                  task.origin === "app_owned" &&
                  task.runId === parentRun.id &&
                  task.completionDelivery?.state === "pending" &&
                  isTerminalDelegatedTaskStatus(task.status) &&
                  (!parentIsLive || task.completionWake === "always"),
              )
              .map((task) => task.id)
          : [];
      const nextDelivery =
        pendingTaskIds.length === 0
          ? null
          : {
              generation: cohort.nextGeneration,
              messageId: yield* mapDelegatedCompletionError(
                idAllocator.allocate.message({
                  threadId,
                  ordinal:
                    (yield* mapDelegatedCompletionError(
                      projectionStore.getMessageCount(threadId),
                    )) + 1,
                }),
              ),
              taskIds: pendingTaskIds,
            };
      const acceptedIds = new Set(delivery.taskIds);
      const pendingIds = new Set(pendingTaskIds);
      for (const task of projection.subagents) {
        const state = pendingIds.has(task.id)
          ? "claimed"
          : acceptedIds.has(task.id) && task.completionDelivery?.state === "claimed"
            ? "delivered"
            : undefined;
        if (state === undefined) continue;
        yield* emitEvent({
          type: "subagent.updated",
          threadId,
          runId: parentRun.id,
          nodeId: task.id,
          driver: task.driver,
          providerInstanceId: task.providerInstanceId,
          occurredAt: now,
          payload: {
            ...task,
            completionDelivery: { state, observedByRunId: null },
            updatedAt: now,
          },
        });
      }
      yield* emitEvent({
        type: "run.updated",
        threadId,
        runId: parentRun.id,
        providerInstanceId: parentRun.providerInstanceId,
        occurredAt: now,
        payload: {
          ...parentRun,
          delegatedCompletion: {
            ...cohort,
            nextGeneration: cohort.nextGeneration + (nextDelivery === null ? 0 : 1),
            delivery: nextDelivery,
          },
        },
      });
      acceptedParentRunId ??= parentRun.id;
    }
    return acceptedParentRunId;
  });

  const emitQueuedRunCancellation = Effect.fn("Orchestrator.emitQueuedRunCancellation")(
    function* (input: {
      readonly command: OrchestrationV2ServerCommand;
      readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
      readonly projection: Pick<OrchestrationV2ThreadProjection, "nodes" | "attempts">;
      readonly run: OrchestrationV2Run;
      readonly now: DateTime.Utc;
    }) {
      const emitEvent = emit(input.events, input.command);
      for (const event of queuedRunCancellationEvents(input.projection, input.run, input.now)) {
        yield* emitEvent(event);
      }
    },
  );

  const failQueuedRunStart = Effect.fn("Orchestrator.failQueuedRunStart")(function* (
    threadId: ThreadId,
    cause: unknown,
    queuedRunId?: RunId,
  ) {
    const projection = yield* projectionStore.getThreadRecords(
      threadId,
      ["runs", "nodes", "attempts", "providerThreads", "turnItems", "messages"],
      { turnItemTypes: ["user_message"], messageRoles: ["user"] },
    );
    const queuedRun =
      queuedRunId === undefined
        ? nextQueuedRun(projection)
        : projection.runs.find((run) => run.id === queuedRunId && run.status === "queued");
    if (queuedRun === undefined) return;
    // A steer that fell back to the queue keeps its user item, hidden while the
    // run is queued. Republish it before the failure so clients whose bounded
    // snapshot omitted it can show the prompt beside the error.
    const retainedUserItems = projection.turnItems.filter(
      (item) => item.type === "user_message" && item.runId === queuedRun.id,
    );
    const now = yield* DateTime.now;
    const rootNode = projection.nodes.find((node) => node.id === queuedRun.rootNodeId);
    const attempt = projection.attempts.find((entry) => entry.id === queuedRun.activeAttemptId);
    const providerThread = projection.providerThreads.find(
      (entry) => entry.id === queuedRun.providerThreadId,
    );
    const handoffUnsupported =
      typeof cause === "object" &&
      cause !== null &&
      "_tag" in cause &&
      cause._tag === "CommandPolicyCapabilityUnsupportedError";
    const failureCause =
      typeof cause === "object" &&
      cause !== null &&
      "_tag" in cause &&
      cause._tag === "OrchestratorDispatchError" &&
      "cause" in cause
        ? cause.cause
        : cause;
    const failure = makeProviderFailure({
      cause: failureCause,
      code: handoffUnsupported ? "context_handoff_unsupported" : "queued_start_failed",
      class: handoffUnsupported ? "validation_error" : "unknown",
    });
    yield* writeSystemEvents([
      ...retainedUserItems.map((item) => ({
        type: "turn-item.updated" as const,
        threadId,
        runId: queuedRun.id,
        ...(rootNode === undefined ? {} : { nodeId: rootNode.id }),
        providerInstanceId: queuedRun.providerInstanceId,
        occurredAt: now,
        payload: { ...item, status: "failed" as const, completedAt: now, updatedAt: now },
      })),
      ...(attempt !== undefined && rootNode !== undefined
        ? [
            {
              type: "run-attempt.updated" as const,
              threadId,
              runId: queuedRun.id,
              nodeId: rootNode.id,
              providerInstanceId: queuedRun.providerInstanceId,
              occurredAt: now,
              payload: { ...attempt, status: "failed" as const, completedAt: now },
            },
          ]
        : []),
      ...(rootNode === undefined
        ? []
        : [
            {
              type: "node.updated" as const,
              threadId,
              runId: queuedRun.id,
              nodeId: rootNode.id,
              providerInstanceId: queuedRun.providerInstanceId,
              occurredAt: now,
              payload: { ...rootNode, status: "failed" as const, completedAt: now },
            },
          ]),
      ...(rootNode === undefined || providerThread === undefined
        ? []
        : [
            {
              type: "turn-item.updated" as const,
              threadId,
              runId: queuedRun.id,
              nodeId: rootNode.id,
              providerInstanceId: queuedRun.providerInstanceId,
              occurredAt: now,
              payload: {
                id: idAllocator.derive.turnItemFromProviderItem({
                  driver: providerThread.driver,
                  nativeItemId: `queued-start-failure:${queuedRun.id}`,
                }),
                threadId,
                runId: queuedRun.id,
                nodeId: rootNode.id,
                providerThreadId: providerThread.id,
                providerTurnId: null,
                nativeItemRef: null,
                parentItemId: null,
                ordinal: yield* nextTurnItemOrdinal(projection),
                status: "failed" as const,
                title: "Queued provider could not start",
                startedAt: now,
                completedAt: now,
                updatedAt: now,
                type: "error" as const,
                failure,
              },
            },
          ]),
      {
        type: "run.updated",
        threadId,
        runId: queuedRun.id,
        ...(rootNode === undefined ? {} : { nodeId: rootNode.id }),
        providerInstanceId: queuedRun.providerInstanceId,
        occurredAt: now,
        payload: { ...queuedRun, status: "failed", queuePosition: null, completedAt: now },
      },
    ]);
  });

  const isStaleQuestionWake = Effect.fn("Orchestrator.isStaleQuestionWake")(function* (
    projection: Pick<OrchestrationV2ThreadProjection, "thread" | "subagents">,
    messageId: MessageId,
    notification: OrchestrationV2ThreadProjection["messages"][number]["notification"],
  ) {
    if (notification?.source.kind !== "delegated_task" || notification.outcome !== "updated")
      return false;
    const source = notification.source;
    const threadId = projection.thread.id;
    const task =
      projection.subagents.find((candidate) => source.taskIds.includes(candidate.id)) ??
      // Command projections omit settled tasks whose result was already handed over.
      (yield* projectionStore
        .getThreadRecords(threadId, ["subagents"], { subagentIds: source.taskIds })
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause }))))
        .subagents[0];
    // An unknown task gives no evidence the question is obsolete.
    if (task === undefined) return false;
    const childThreadId = task.childThreadId;
    if (
      childThreadId === null ||
      ["completed", "failed", "cancelled", "interrupted"].includes(task.status)
    )
      return true;
    const child = yield* projectionStore
      .getThreadRecords(childThreadId, ["runtimeRequests"], { runtimeRequestStatuses: ["pending"] })
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: childThreadId, cause }),
        ),
      );
    return !child.runtimeRequests.some(
      (request) =>
        request.kind === "user_input" &&
        request.status === "pending" &&
        messageId === `message:delegated-task-question:${threadId}:${request.id}`,
    );
  });

  const cancelStaleQuestionWakes = Effect.fn("Orchestrator.cancelStaleQuestionWakes")(function* (
    projection: OrchestrationV2ThreadProjection,
  ) {
    const events = yield* Ref.make<Array<OrchestrationV2DomainEvent>>([]);
    for (const run of projection.runs) {
      if (run.status !== "queued") continue;
      const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
      if (
        message === undefined ||
        !(yield* isStaleQuestionWake(projection, message.id, message.notification).pipe(
          // A child thread read must not fail the parent's unrelated next queued run.
          Effect.catchTags({
            OrchestratorProjectionError: (cause) =>
              Effect.logWarning("Failed to check a queued task question wake", {
                runId: run.id,
                cause,
              }).pipe(Effect.as(false)),
          }),
        ))
      )
        continue;
      yield* emitQueuedRunCancellation({
        command: {
          type: "queued-run.cancel",
          commandId: CommandId.make(`command:system:stale-question:${run.id}`),
          threadId: projection.thread.id,
          runId: run.id,
        },
        events,
        projection,
        run,
        now: yield* DateTime.now,
      });
    }
    const cancellations = yield* Ref.get(events);
    if (cancellations.length === 0) return false;
    yield* writeSystemEvents(cancellations);
    return true;
  });

  // The wake about to start takes in the thread's other unheld queued wakes of its class (queued
  // before merging, or unable to join) and cancels them, in one write. No run is active here, so
  // every parent run it re-points has ended.
  const absorbQueuedWakes = Effect.fn("Orchestrator.absorbQueuedWakes")(function* (
    projection: OrchestrationV2ThreadProjection,
    run: OrchestrationV2Run,
  ) {
    const message = projection.messages.find((row) => row.id === run.userMessageId);
    const wakeClass =
      message?.runId === run.id && run.steerWhenReadyRunId === undefined
        ? mergeableWakeClass(message)
        : undefined;
    if (message === undefined || wakeClass === undefined) return false;
    const others = joinableQueuedWakes(projection, wakeClass, false).filter(
      (other) => other.run.id !== run.id,
    );
    const now = yield* DateTime.now;
    const events: Array<Omit<OrchestrationV2DomainEvent, "id">> = [];
    // Oldest first, until the next wake would take it past MAX_MERGED_WAKE_ITEMS.
    const absorbed: Array<(typeof others)[number]> = [];
    if (wakeClass === "pull-request") {
      let merged = message;
      for (const other of others) {
        if (mergedWakeItems(merged) + mergedWakeItems(other.message) > MAX_MERGED_WAKE_ITEMS) break;
        const next = mergedPullRequestWatchWake(merged, other.message);
        if (next === undefined) continue;
        merged = next;
        absorbed.push(other);
      }
      if (absorbed.length === 0) return false;
      events.push(wakeMessageUpdated(run, { ...merged, updatedAt: now }, now));
    } else {
      const own = currentCompletionWakeParts(projection.runs, message);
      if (own.length === 0 || isDelegatedAppThread(projection.thread)) return false;
      const taken = [...own];
      const carried = () => taken.reduce((count, { part }) => count + part.taskIds.length, 0);
      for (const other of others) {
        const parts = currentCompletionWakeParts(projection.runs, other.message);
        const items = parts.reduce((count, { part }) => count + part.taskIds.length, 0);
        if (carried() + items > MAX_MERGED_WAKE_ITEMS) break;
        taken.push(...parts);
        absorbed.push(other);
      }
      if (absorbed.length === 0) return false;
      const ownership = completionWakeOwnership(taken.map(({ part }) => part))!;
      for (const { parentRun } of taken.slice(own.length)) {
        events.push(cohortRepointed(parentRun, message.id, now));
      }
      events.push(
        wakeMessageUpdated(
          run,
          yield* completionWakeMessage(message, ownership, projection.subagents, now).pipe(
            Effect.mapError(
              (cause) => new OrchestratorProjectionError({ threadId: run.threadId, cause }),
            ),
          ),
          now,
        ),
      );
    }
    for (const other of absorbed) {
      events.push(...queuedRunCancellationEvents(projection, other.run, now));
    }
    yield* writeSystemEvents(events);
    return true;
  });

  const startNextQueuedRun = Effect.fn("Orchestrator.startNextQueuedRun")(
    function* (
      threadId: ThreadId,
      options?: {
        readonly failedRunId?: RunId;
        readonly endedRunId?: RunId;
        readonly queuedRunId?: RunId;
        /** Treat the latest executed run as the one that just ended, as its run-end handler would. */
        readonly latestRunEnded?: boolean;
        readonly userStarted?: boolean;
      },
    ) {
      if (!(yield* projectionStore.canStartQueuedRun(threadId))) return;
      // Bounded until a decision needs the history that read leaves out; then whole, as before.
      let whole = false;
      const read = Effect.suspend(() =>
        whole ? readCommandProjection(threadId) : readQueuedRunProjection(threadId),
      );
      const readWhole = Effect.suspend(() => {
        whole = true;
        return readCommandProjection(threadId);
      });
      let projection = yield* read;
      if (
        projection.thread.archivedAt !== null ||
        projection.thread.deletedAt !== null ||
        projection.runs.some(isActiveRun)
      ) {
        return;
      }

      if (yield* cancelStaleQuestionWakes(projection)) {
        projection = yield* read;
      }
      let storedQueuedRun =
        options?.queuedRunId === undefined
          ? nextQueuedRun(projection)
          : projection.runs.find(
              (run) => run.id === options.queuedRunId && run.status === "queued",
            );
      while (storedQueuedRun !== undefined) {
        if (!whole && pullRequestWakeReadsNotices(projection, storedQueuedRun)) {
          const runId = storedQueuedRun.id;
          projection = yield* readWhole;
          storedQueuedRun = projection.runs.find(
            (run) => run.id === runId && run.status === "queued",
          );
          continue;
        }
        if (!isStalePullRequestWatchWake(projection, storedQueuedRun)) break;
        yield* writeSystemEvents(
          queuedRunCancellationEvents(projection, storedQueuedRun, yield* DateTime.now),
        );
        projection = yield* read;
        storedQueuedRun = nextQueuedRun(projection);
      }
      if (
        storedQueuedRun !== undefined &&
        (yield* absorbQueuedWakes(projection, storedQueuedRun))
      ) {
        const runId = storedQueuedRun.id;
        projection = yield* read;
        storedQueuedRun = projection.runs.find(
          (run) => run.id === runId && run.status === "queued",
        );
      }
      if (
        storedQueuedRun !== undefined &&
        !whole &&
        pullRequestWakeReadsNotices(projection, storedQueuedRun)
      ) {
        const runId = storedQueuedRun.id;
        projection = yield* readWhole;
        storedQueuedRun = projection.runs.find(
          (run) => run.id === runId && run.status === "queued",
        );
      }
      // A wake that outlived some of its pull requests tells only the current ones.
      const currentWake =
        storedQueuedRun === undefined
          ? undefined
          : currentPullRequestWatchWake(projection, storedQueuedRun);
      if (storedQueuedRun !== undefined && currentWake !== undefined) {
        const now = yield* DateTime.now;
        yield* writeSystemEvents([
          {
            type: "message.updated",
            threadId,
            runId: storedQueuedRun.id,
            ...(storedQueuedRun.rootNodeId === null ? {} : { nodeId: storedQueuedRun.rootNodeId }),
            providerInstanceId: storedQueuedRun.providerInstanceId,
            occurredAt: now,
            payload: { ...currentWake, updatedAt: now },
          },
        ]);
        const runId = storedQueuedRun.id;
        projection = yield* read;
        storedQueuedRun = projection.runs.find((run) => run.id === runId);
      }
      if (storedQueuedRun === undefined) {
        return;
      }
      const queuedMessage = projection.messages.find(
        (candidate) => candidate.id === storedQueuedRun.userMessageId,
      );
      const modelSelection = queuedRunModelSelection(projection, storedQueuedRun);
      const queuedRun = {
        ...storedQueuedRun,
        modelSelection,
        providerInstanceId: modelSelection.instanceId,
      };
      // A limited provider would fail and consume every waiting message.
      if (
        usageLimitHoldsQueue(
          projection,
          queuedRun.providerInstanceId,
          options?.userStarted === true ? undefined : yield* DateTime.now,
        )
      ) {
        return;
      }
      // Waits without a hold: lowering the thread's mode starts the queue again.
      if (isRuntimeModeAbove(projection.thread.runtimeMode, runtimeModeCeiling?.maxRuntimeMode)) {
        return;
      }
      // Only hidden wakes wait behind a failed run: nobody would resume them, so they retry on
      // their own after a backoff instead of being held.
      const failedRun = latestExecutedRun(projection.runs);
      const retriesWakes =
        failedRun?.status === "failed" &&
        options?.userStarted !== true &&
        projection.runs.every(
          (run) =>
            run.status !== "queued" ||
            run.queueHeld === true ||
            projection.messages.some(
              (message) => message.id === run.userMessageId && isHiddenWake(message),
            ),
        );
      if (
        retriesWakes &&
        DateTime.isLessThan(yield* DateTime.now, wakeRetryAt(projection.runs, failedRun))
      ) {
        return;
      }
      // Failed worktree creation would run queued work in the project checkout; wait for the user.
      const endedRunId = options?.latestRunEnded === true ? failedRun?.id : options?.endedRunId;
      const failedRunId =
        options?.latestRunEnded === true && failedRun?.status === "failed"
          ? failedRun.id
          : options?.failedRunId;
      const failureClass =
        failedRun && failedRunId !== undefined && failedRun.id === failedRunId
          ? latestRootProviderFailure(failedRun, projection.turnItems)?.class
          : undefined;
      const worktreeMissing =
        endedRunId !== undefined &&
        failedRun?.id === endedRunId &&
        failedRun?.workspacePreparation?.type === "worktree" &&
        projection.thread.worktreePath === null;
      const settings = yield* serverSettings.getSettings.pipe(Effect.orDie);
      const projectOverride =
        settings.projectSettingsOverrides[projection.thread.projectId]?.providerInstances?.[
          queuedRun.providerInstanceId
        ]?.enabled;
      const machineInstance = Object.hasOwn(
        settings.providerInstances,
        queuedRun.providerInstanceId,
      )
        ? settings.providerInstances[queuedRun.providerInstanceId]
        : undefined;
      const instanceDisabledForProject =
        projectOverride !== undefined
          ? !projectOverride
          : machineInstance !== undefined && !resolveProviderInstanceEnabled(machineInstance);
      if (
        worktreeMissing ||
        instanceDisabledForProject ||
        (failureClass !== undefined &&
          failureClass !== "validation_error" &&
          failedRun?.providerInstanceId === queuedRun.providerInstanceId &&
          !retriesWakes)
      ) {
        const now = yield* DateTime.now;
        yield* writeSystemEvents(
          projection.runs
            .filter((run) => run.status === "queued")
            .map((run) => ({
              type: "run.updated" as const,
              threadId,
              runId: run.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
              payload: { ...run, queueHeld: true },
            })),
        );
        return;
      }
      // A restart continuation resumes the cut turn, so queued work waits until it has started
      // or been declined. With none pending, no run awaits one.
      if (options?.userStarted !== true) {
        if (!whole && (yield* projectionStore.hasPendingRestartContinuation(threadId))) {
          projection = yield* readWhole;
        }
        const continuationSource = whole ? restartContinuationSource(projection.runs) : undefined;
        if (
          continuationSource !== undefined &&
          (yield* awaitsRestartContinuation(continuationSource))
        ) {
          return;
        }
      }
      if (
        !whole &&
        queuedMessage !== undefined &&
        queueStartReadsHistory(projection, queuedRun, queuedMessage)
      ) {
        projection = yield* readWhole;
      }
      // Move queued ordinals after runs that started while they waited to preserve transcript and checkpoint order.
      // Started runs have prompts; cancelled queued messages do not.
      const startedAhead = yield* projectionStore.hasRunPromptAfter(threadId, queuedRun.ordinal);
      const ordinal = startedAhead ? nextRunOrdinal(projection) : queuedRun.ordinal;
      const commandId = CommandId.make(`command:system:start-queued:${queuedRun.id}`);
      const rootNodeId = queuedRun.rootNodeId;
      const attemptId = queuedRun.activeAttemptId;
      const providerThreadId = queuedRun.providerThreadId;
      if (rootNodeId === null || attemptId === null || providerThreadId === null) {
        return yield* new OrchestratorDispatchError({
          commandId,
          commandType: "message.dispatch",
          cause: `Queued run ${queuedRun.id} is missing execution identity.`,
        });
      }

      const rootNode = projection.nodes.find((candidate) => candidate.id === rootNodeId);
      const attempt = projection.attempts.find((candidate) => candidate.id === attemptId);
      const legacyQueuedTurnItem = projection.turnItems.find(
        (
          candidate,
        ): candidate is Extract<OrchestrationV2TurnItem, { readonly type: "user_message" }> =>
          candidate.type === "user_message" &&
          candidate.runId === queuedRun.id &&
          candidate.messageId === queuedRun.userMessageId,
      );
      const storedProviderThread = projection.providerThreads.find(
        (candidate) => candidate.id === providerThreadId,
      );
      const storedCheckpointScope = projection.checkpointScopes.find(
        (scope) => scope.id === rootNode?.checkpointScopeId,
      );
      if (
        rootNode === undefined ||
        attempt === undefined ||
        queuedMessage === undefined ||
        storedProviderThread === undefined ||
        (rootNode.checkpointScopeId !== null && storedCheckpointScope === undefined)
      ) {
        return yield* new OrchestratorDispatchError({
          commandId,
          commandType: "message.dispatch",
          cause: `Queued run ${queuedRun.id} is missing projection state.`,
        });
      }
      // A scheduled delivery starts on the thread's current pick, so /goal must suit that provider.
      yield* ensureGoalCommandSupported(
        { commandId, type: "message.dispatch" },
        queuedMessage,
        queuedRun.providerInstanceId,
      );

      const now = yield* DateTime.now;
      const targetAdapter = yield* providerAdapters.get(queuedRun.providerInstanceId).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorDispatchError({
              commandId,
              commandType: "message.dispatch",
              cause,
            }),
        ),
      );
      const followsCurrent = followsCurrentConversation(queuedMessage);
      const freshContext = queuedRun.freshContext === true;
      const activeProviderThread = dispatchProviderThread(projection, queuedMessage, freshContext);
      const contextBoundary = freshContext ? ordinal : contextBoundaryOrdinal(projection);
      // A run queued before a fresh run started is bound to the conversation that run left behind.
      const followsLatestContext =
        !freshContext &&
        (followsCurrent || providerThreadPredatesBoundary(storedProviderThread, contextBoundary));
      const targetProviderThread = followsLatestContext
        ? activeProviderThread?.providerInstanceId === queuedRun.providerInstanceId &&
          !providerThreadPredatesBoundary(activeProviderThread, contextBoundary)
          ? activeProviderThread
          : rootProviderThreadsForProvider(projection, queuedRun.providerInstanceId)[0]
        : freshContext &&
            // The reserved row belongs to the instance the run queued on, which may have changed.
            (storedProviderThread.providerInstanceId !== queuedRun.providerInstanceId ||
              storedProviderThread.firstRunOrdinal !== null ||
              storedProviderThread.nativeThreadRef !== null)
          ? undefined
          : storedProviderThread;
      const queuedProviderThread: OrchestrationV2ProviderThread = targetProviderThread ?? {
        id: idAllocator.derive.providerThread({
          driver: targetAdapter.driver,
          // The queue reserved `pending:<run>` for this run; another run may have taken it.
          nativeThreadId: freshContext
            ? `pending:${queuedRun.id}:fresh`
            : `pending:${queuedRun.id}`,
        }),
        driver: targetAdapter.driver,
        providerInstanceId: queuedRun.providerInstanceId,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const selectionChanged = !modelSelectionsEqual(
        projection.thread.modelSelection,
        queuedRun.modelSelection,
      );
      // The picker may already have moved ahead of the native conversation.
      const switchFromActiveProvider =
        followsCurrent &&
        activeProviderThread !== undefined &&
        activeProviderThread.providerInstanceId !== queuedRun.providerInstanceId;
      const switchPlan =
        selectionChanged || switchFromActiveProvider
          ? yield* providerSwitchService
              .plan({
                projection: switchFromActiveProvider
                  ? {
                      ...projection,
                      thread: {
                        ...projection.thread,
                        modelSelection: {
                          ...projection.thread.modelSelection,
                          instanceId: activeProviderThread.providerInstanceId,
                        },
                      },
                    }
                  : projection,
                targetModelSelection: queuedRun.modelSelection,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestratorDispatchError({
                      commandId,
                      commandType: "message.dispatch",
                      cause,
                    }),
                ),
              )
          : null;
      const releasableSessions = uniqueSessions([
        ...projection.providerSessions.filter(
          (session) =>
            switchPlan?.releaseProviderSessionIds.includes(session.id) &&
            session.status !== "stopped" &&
            session.status !== "error",
        ),
        ...(freshContext ? sessionsReleasedByFreshContext(projection, activeProviderThread) : []),
      ]);
      const busySessionIds = new Set<ProviderSessionId>();
      for (const session of releasableSessions) {
        if (yield* sessionHasBackgroundWork(session, projection.providerThreads)) {
          busySessionIds.add(session.id);
        }
      }
      // Nobody waits on a queued start to refuse it. A handoff runs in its own session, so a
      // busy one stays up; a restart resumes the busy session's native thread, so wait instead.
      // A fresh run resumes nothing, so it never waits.
      if (
        busySessionIds.size > 0 &&
        !freshContext &&
        switchPlan?.transition.type === "restart_and_resume"
      ) {
        yield* Effect.logInfo(
          "Holding the queue: the replaced provider session still runs background work",
          { threadId, runId: queuedRun.id, providerSessionIds: Array.from(busySessionIds) },
        );
        yield* writeSystemEvents(
          projection.runs
            .filter((run) => run.status === "queued")
            .map((run) => ({
              type: "run.updated" as const,
              threadId,
              runId: run.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
              payload: { ...run, queueHeld: true },
            })),
        );
        return;
      }
      const sessionsToDetach = releasableSessions.filter(
        (session) => !busySessionIds.has(session.id),
      );
      const canResumeAcrossInstances =
        !freshContext &&
        switchPlan?.instanceChanged === true &&
        switchPlan.transition.type === "restart_and_resume" &&
        activeProviderThread !== undefined &&
        activeProviderThread.nativeThreadRef !== null;
      const deliveryProviderThread = canResumeAcrossInstances
        ? {
            ...queuedProviderThread,
            nativeThreadRef: activeProviderThread.nativeThreadRef,
            nativeConversationHeadRef: activeProviderThread.nativeConversationHeadRef,
            nativeMetadata: activeProviderThread.nativeMetadata,
          }
        : queuedProviderThread;
      const targetCapabilities = yield* targetAdapter.getCapabilities().pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorDispatchError({
              commandId,
              commandType: "message.dispatch",
              cause,
            }),
        ),
      );
      const latestCompletedRun = projection.runs.findLast((run) => run.status === "completed");
      const latestHandoffRun = projection.runs.findLast(isHandoffSourceRun);
      const targetLastCompletedRun = lastDeliveredRunForProviderThread(
        projection,
        queuedProviderThread.id,
      );
      const coveredRuns =
        freshContext ||
        canResumeAcrossInstances ||
        latestHandoffRun === undefined ||
        latestHandoffRun.providerInstanceId === queuedRun.providerInstanceId
          ? []
          : projection.runs.filter(
              (run) =>
                isHandoffSourceRun(run) &&
                run.ordinal >= contextBoundary &&
                run.ordinal > (targetLastCompletedRun?.ordinal ?? 0) &&
                run.ordinal <= latestHandoffRun.ordinal,
            );
      const needsFullContext = deliveryProviderThread.nativeThreadRef === null;
      const legacyImportItems =
        projection.thread.historyOrigin === "v1_import" && contextBoundary === 0
          ? yield* readHandoffItems(threadId, [null])
          : [];
      const handoffStrategy = needsFullContext
        ? ("full_thread_summary" as const)
        : ("delta_since_target_last_seen" as const);
      const transferId =
        coveredRuns.length === 0
          ? null
          : yield* idAllocator.allocate
              .contextTransfer({
                sourceThreadId: threadId,
                targetThreadId: threadId,
                type: "provider_handoff",
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestratorDispatchError({
                      commandId,
                      commandType: "message.dispatch",
                      cause,
                    }),
                ),
              );
      if (transferId !== null) {
        yield* commandPolicy.ensureContextHandoff({
          commandId,
          threadId,
          providerInstanceId: queuedRun.providerInstanceId,
          capabilities: targetCapabilities,
          strategy: needsFullContext ? "full_thread_summary" : "delta_context",
        });
      }
      const handoff =
        transferId === null || latestHandoffRun === undefined
          ? null
          : yield* contextHandoffService
              .prepareProviderHandoff({
                threadId,
                targetRunId: queuedRun.id,
                transferId,
                fromProviderThreadIds: Array.from(
                  new Set(
                    coveredRuns.flatMap((run) =>
                      run.providerThreadId === null ? [] : [run.providerThreadId],
                    ),
                  ),
                ),
                toProviderThreadId: queuedProviderThread.id,
                fromProviderInstanceId: latestHandoffRun.providerInstanceId,
                toProviderInstanceId: queuedRun.providerInstanceId,
                coveredRunOrdinals: {
                  from: coveredRuns[0]!.ordinal,
                  to: coveredRuns.at(-1)!.ordinal,
                },
                runs: projection.runs,
                strategy: handoffStrategy,
                items: [
                  ...(needsFullContext && contextBoundary === 0
                    ? yield* readInheritedItems(projection.thread)
                    : []),
                  ...(needsFullContext && latestCompletedRun !== undefined
                    ? legacyImportItems
                    : []),
                  ...(yield* readHandoffItems(
                    threadId,
                    coveredRuns.map((run) => run.id),
                  )),
                ],
                createdAt: now,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestratorDispatchError({
                      commandId,
                      commandType: "message.dispatch",
                      cause,
                    }),
                ),
              );
      const legacyImportRecoveryHandoff =
        latestCompletedRun === undefined && needsFullContext && legacyImportItems.length > 0
          ? yield* contextHandoffService
              .prepareLegacyImport({
                threadId,
                targetRunId: queuedRun.id,
                toProviderThreadId: queuedProviderThread.id,
                toProviderInstanceId: queuedRun.providerInstanceId,
                items: legacyImportItems,
                createdAt: now,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestratorDispatchError({
                      commandId,
                      commandType: "message.dispatch",
                      cause,
                    }),
                ),
              )
          : null;
      const activeHandoff = handoff ?? legacyImportRecoveryHandoff;
      // A queued run starts in the thread's workspace now; if the thread moved since the run was
      // queued, it takes that workspace's scope. A policy the turn start refuses keeps the stored one.
      const resolvedForStart = yield* runtimePolicy
        .resolve({ thread: projection.thread, modelSelection: queuedRun.modelSelection })
        .pipe(Effect.result);
      const workspaceNow =
        resolvedForStart._tag === "Success"
          ? (resolvedForStart.success.cwd ?? projection.thread.worktreePath ?? process.cwd())
          : undefined;
      // A stored scope created at this cwd as its real path is that workspace without a lookup.
      const checkpointScope =
        storedCheckpointScope !== undefined &&
        (workspaceNow === undefined ||
          (workspaceNow === storedCheckpointScope.cwd &&
            workspaceNow === storedCheckpointScope.realCwd))
          ? storedCheckpointScope
          : yield* Effect.gen(function* () {
              if (resolvedForStart._tag === "Failure") return yield* resolvedForStart.failure;
              const prepared = yield* prepareRootRunScope({
                threadScopes: projection.checkpointScopes,
                threadId,
                runId: queuedRun.id,
                rootNodeId: rootNode.id,
                providerThreadId: queuedProviderThread.id,
                cwd: workspaceNow!,
                createdAt: now,
              });
              // Another spelling of the same workspace keeps the scope as stored.
              return prepared.id === storedCheckpointScope?.id ? storedCheckpointScope : prepared;
            }).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorDispatchError({
                    commandId,
                    commandType: "message.dispatch",
                    cause,
                  }),
              ),
            );
      const providerSessionId =
        (!canResumeAcrossInstances &&
        queuedProviderThread.providerSessionId !== null &&
        !switchPlan?.releaseProviderSessionIds.includes(queuedProviderThread.providerSessionId)
          ? queuedProviderThread.providerSessionId
          : null) ??
        (yield* providerAdapters.get(queuedRun.providerInstanceId).pipe(
          Effect.flatMap((adapter) =>
            providerSessionIdFor({
              adapter,
              providerInstanceId: queuedRun.providerInstanceId,
              threadId,
            }),
          ),
          Effect.mapError(
            (cause) =>
              new OrchestratorDispatchError({
                commandId,
                commandType: "message.dispatch",
                cause,
              }),
          ),
        ));
      const providerThread: OrchestrationV2ProviderThread = {
        ...deliveryProviderThread,
        providerSessionId,
        status: "not_loaded",
        firstRunOrdinal: queuedProviderThread.firstRunOrdinal ?? ordinal,
        lastRunOrdinal: ordinal,
        handoffIds: appendContextHandoffId(
          appendContextHandoffId(queuedProviderThread.handoffIds, handoff?.id ?? null),
          legacyImportRecoveryHandoff?.id ?? null,
        ),
        updatedAt: now,
      };
      // Resume can clear the hold before the stopped run ends; its Stop marker still makes the drain explicit.
      const queueDelivery =
        endedRunId === undefined ||
        queuedRun.queueHeld === true ||
        (yield* projectionStore
          .getThreadRecords(threadId, ["turnItems"], {
            turnItemTypes: ["run_interrupt_request"],
            turnItemRunIds: [endedRunId],
          })
          .pipe(
            Effect.map(({ turnItems }) => turnItems.length > 0),
            Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
          ))
          ? "explicit-queue"
          : "automatic-queue";
      const startingRun: OrchestrationV2Run = {
        ...queuedRun,
        providerThreadId: queuedProviderThread.id,
        ordinal,
        status: "starting",
        // Where it starts now; a policy that did not resolve keeps where it was queued.
        workspaceCwd: workspaceNow ?? queuedRun.workspaceCwd ?? runWorkspaceOf(checkpointScope),
        queuePosition: null,
        startedAt: null,
        contextHandoffId: activeHandoff?.id ?? null,
        ...wakeWorkStartedAt(
          projection,
          {
            createdBy: queuedMessage.createdBy,
            creationSource: queuedMessage.creationSource,
            senderThreadId: queuedMessage.senderThreadId,
            notification: queuedMessage.notification,
            delegatedCompletion: queuedMessage.delegatedCompletion,
            restartContinuationOfRunId: queuedRun.restartContinuationOfRunId,
          },
          queueDelivery,
        ),
      };
      const userTurnItem: OrchestrationV2TurnItem = {
        ...(legacyQueuedTurnItem ?? {
          id: idAllocator.derive.userTurnItem({ messageId: queuedMessage.id }),
          threadId,
          runId: queuedRun.id,
          nodeId: rootNodeId,
          providerThreadId: queuedProviderThread.id,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: ordinal * 100,
          status: "completed",
          title: null,
          type: "user_message",
          messageId: queuedMessage.id,
          text: queuedMessage.text,
          attachments: queuedMessage.attachments,
          ...(queuedMessage.context ? { context: queuedMessage.context } : {}),
          createdBy: queuedMessage.createdBy,
          creationSource: queuedMessage.creationSource,
          ...(queuedMessage.scheduledTaskId === undefined
            ? {}
            : { scheduledTaskId: queuedMessage.scheduledTaskId }),
          ...(queuedMessage.senderThreadId === undefined
            ? {}
            : { senderThreadId: queuedMessage.senderThreadId }),
          ...(queuedMessage.linkedSender === undefined
            ? {}
            : { linkedSender: queuedMessage.linkedSender }),
          ...(queuedMessage.idleCompaction === undefined
            ? {}
            : { idleCompaction: queuedMessage.idleCompaction }),
        }),
        ...(freshContext ? { freshContext: true } : {}),
        providerThreadId: queuedProviderThread.id,
        inputIntent: "queued_turn",
        status: "completed",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        ...(activeHandoff !== null
          ? {}
          : whole
            ? modelSwitchSince(projection, queuedRun.modelSelection)
            : yield* storedHistory(threadId).modelSwitch(projection, queuedRun.modelSelection)),
      };
      const handoffTurnItem: OrchestrationV2TurnItem | null =
        activeHandoff === null
          ? null
          : {
              id: idAllocator.derive.runSignalTurnItem({
                runId: queuedRun.id,
                signal: `context-handoff:${activeHandoff.id}`,
              }),
              threadId,
              runId: queuedRun.id,
              nodeId: rootNodeId,
              providerThreadId: queuedProviderThread.id,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: ordinal * 100 - 1,
              status: "completed",
              title: handoff === null ? "Imported context" : "Provider handoff",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              type: "handoff",
              contextHandoffId: activeHandoff.id,
              fromProviderThreadIds: activeHandoff.fromProviderThreadIds,
              toProviderThreadId: activeHandoff.toProviderThreadId,
              fromProviderInstanceIds: Array.from(
                new Set(coveredRuns.map((run) => run.providerInstanceId)),
              ),
              toProviderInstanceId: queuedRun.providerInstanceId,
              fromModelSelections: Array.from(
                new Map(
                  coveredRuns.map((run) => [
                    `${run.modelSelection.instanceId}\0${run.modelSelection.model}`,
                    run.modelSelection,
                  ]),
                ).values(),
              ),
              toModel: queuedRun.modelSelection.model,
              strategy: activeHandoff.strategy,
              summary: activeHandoff.summaryText,
            };
      const executionIdentityEvents: ReadonlyArray<Omit<OrchestrationV2DomainEvent, "id">> =
        storedCheckpointScope === undefined ||
        checkpointScope.id !== storedCheckpointScope.id ||
        storedProviderThread.id !== queuedProviderThread.id ||
        attempt.providerInstanceId !== queuedRun.providerInstanceId
          ? [
              {
                type: "checkpoint-scope.created",
                threadId,
                runId: queuedRun.id,
                nodeId: rootNodeId,
                providerInstanceId: queuedRun.providerInstanceId,
                occurredAt: now,
                payload: { ...checkpointScope, providerThreadId: queuedProviderThread.id },
              },
              {
                type: "node.updated",
                threadId,
                runId: queuedRun.id,
                nodeId: rootNodeId,
                providerInstanceId: queuedRun.providerInstanceId,
                occurredAt: now,
                payload: {
                  ...rootNode,
                  providerThreadId: queuedProviderThread.id,
                  checkpointScopeId: checkpointScope.id,
                },
              },
              {
                type: "run-attempt.updated",
                threadId,
                runId: queuedRun.id,
                nodeId: rootNodeId,
                providerInstanceId: queuedRun.providerInstanceId,
                occurredAt: now,
                payload: {
                  ...attempt,
                  providerInstanceId: queuedRun.providerInstanceId,
                  providerThreadId: queuedProviderThread.id,
                },
              },
            ]
          : [];
      const detachReason = freshContext
        ? FRESH_CONTEXT_DETACH_REASON
        : "Provider or model selection changed.";
      yield* writeSystemEvents(
        [
          ...(selectionChanged
            ? [
                {
                  type:
                    queuedRun.providerInstanceId === projection.thread.providerInstanceId
                      ? ("thread.model-selection-updated" as const)
                      : ("thread.provider-switched" as const),
                  threadId,
                  providerInstanceId: queuedRun.providerInstanceId,
                  occurredAt: now,
                  payload: {
                    ...projection.thread,
                    providerInstanceId: queuedRun.providerInstanceId,
                    modelSelection: queuedRun.modelSelection,
                    updatedAt: now,
                  },
                },
              ]
            : []),
          ...(handoff === null || transferId === null || latestHandoffRun === undefined
            ? []
            : [
                {
                  type: "context-transfer.created" as const,
                  threadId,
                  runId: queuedRun.id,
                  providerInstanceId: queuedRun.providerInstanceId,
                  occurredAt: now,
                  payload: {
                    id: transferId,
                    type: "provider_handoff" as const,
                    sourceThreadId: threadId,
                    targetThreadId: threadId,
                    sourcePoint: contextSourcePointForRun(projection, latestHandoffRun),
                    basePoint:
                      needsFullContext || targetLastCompletedRun === undefined
                        ? null
                        : contextSourcePointForRun(projection, targetLastCompletedRun),
                    sourceProviderInstanceId: latestHandoffRun.providerInstanceId,
                    targetProviderInstanceId: queuedRun.providerInstanceId,
                    targetRunId: queuedRun.id,
                    status: "consumed" as const,
                    resolution: {
                      strategy: needsFullContext
                        ? ("portable_context" as const)
                        : ("delta_context" as const),
                      contextHandoffId: handoff.id,
                    },
                    createdBy: queuedMessage.createdBy,
                    error: null,
                    createdAt: now,
                    updatedAt: now,
                    consumedAt: now,
                  },
                },
                {
                  type: "context-handoff.updated" as const,
                  threadId,
                  runId: queuedRun.id,
                  providerInstanceId: queuedRun.providerInstanceId,
                  occurredAt: now,
                  payload: handoff,
                },
              ]),
          ...(legacyImportRecoveryHandoff === null
            ? []
            : [
                {
                  type: "context-handoff.updated" as const,
                  threadId,
                  runId: queuedRun.id,
                  providerInstanceId: queuedRun.providerInstanceId,
                  occurredAt: now,
                  payload: legacyImportRecoveryHandoff,
                },
              ]),
          ...(handoffTurnItem === null
            ? []
            : [
                {
                  type: "turn-item.updated" as const,
                  threadId,
                  runId: queuedRun.id,
                  nodeId: rootNodeId,
                  providerInstanceId: queuedRun.providerInstanceId,
                  occurredAt: now,
                  payload: handoffTurnItem,
                },
              ]),
          ...sessionsToDetach.map((session) => ({
            type: "provider-session.detached" as const,
            threadId,
            driver: session.driver,
            providerInstanceId: session.providerInstanceId,
            occurredAt: now,
            payload: {
              providerSessionId: session.id,
              detachedAt: now,
              reason: detachReason,
            },
          })),
          ...executionIdentityEvents,
          {
            type: "provider-thread.updated",
            threadId,
            providerInstanceId: queuedRun.providerInstanceId,
            occurredAt: now,
            payload: providerThread,
          },
          {
            type: "turn-item.updated",
            threadId,
            runId: queuedRun.id,
            nodeId: rootNodeId,
            providerInstanceId: queuedRun.providerInstanceId,
            occurredAt: now,
            payload: notificationTurnItem(
              userTurnItem,
              queuedMessage,
              yield* notificationTasks(threadId, queuedMessage, projection.subagents),
            ),
          },
          {
            type: "run.updated",
            threadId,
            runId: queuedRun.id,
            nodeId: rootNodeId,
            providerInstanceId: queuedRun.providerInstanceId,
            occurredAt: now,
            payload: startingRun,
          },
        ],
        [
          ...sessionsToDetach.map((session) => ({
            id: `effect:${commandId}:provider-session.detach:${session.id}`,
            commandId,
            threadId,
            request: {
              type: "provider-session.detach" as const,
              providerSessionId: session.id,
              detail: detachReason,
            },
          })),
          {
            // A restart can return this run to the queue after its previous start effect was cancelled.
            id: `effect:${commandId}:provider-turn.start:${queuedRun.id}:${yield* idAllocator.allocate.event({ threadId, commandId })}`,
            commandId,
            threadId,
            request: { type: "provider-turn.start", runId: queuedRun.id },
          },
        ],
      );
    },
    (effect, threadId, options?) =>
      effect.pipe(
        Effect.catch((cause) => failQueuedRunStart(threadId, cause, options?.queuedRunId)),
      ),
  );

  // Under load a resumed provider can time out before taking the prompt; the restart still owes the turn.
  const retryContinuationStart = Effect.fn("Orchestrator.retryContinuationStart")(function* (
    threadId: ThreadId,
    runId: RunId,
  ) {
    const projection = yield* projectionStore.getThreadRecords(
      threadId,
      ["runs", "providerTurns", "turnItems"],
      { turnItemTypes: ["error"], turnItemRunIds: [runId] },
    );
    const run = projection.runs.find((candidate) => candidate.id === runId);
    if (run === undefined) return;
    const delayMs = continuationStartRetryDelayMs(run, projection);
    if (delayMs === undefined) return;
    const commandId = CommandId.make(`command:restart-continuation-retry:${run.id}`);
    // A start retry is not a crash: carry the crash budget forward unchanged.
    const previous =
      run.restartContinuationOfRunId === undefined
        ? undefined
        : Option.getOrUndefined(
            yield* effectOutbox.get(
              `effect:restart-continuation:${run.restartContinuationOfRunId}`,
            ),
          )?.request;
    const processLossCount =
      previous?.type === "provider-runtime.continue" ? previous.processLossCount : undefined;
    yield* writeSystemEvents(
      [],
      [
        {
          id: `effect:restart-continuation:${run.id}`,
          commandId,
          threadId,
          request: {
            type: "provider-runtime.continue",
            sourceRunId: run.id,
            ...(processLossCount === undefined ? {} : { processLossCount }),
          },
          availableAt: DateTime.add(yield* DateTime.now, { milliseconds: delayMs }),
        },
      ],
    );
  });

  // A hold waits for a person to review what they queued. Nobody reviews messages from agents,
  // the server or scheduled tasks, so a later completed run releases those unless it was stopped.
  const releaseUnattendedHolds = Effect.fn("Orchestrator.releaseUnattendedHolds")(function* (
    threadId: ThreadId,
    completedRunId: RunId,
  ) {
    const held = yield* projectionStore.getHeldQueuedRuns(threadId);
    if (held.length === 0) return;
    const projection = yield* projectionStore.getThreadRecords(
      threadId,
      ["runs", "messages", "turnItems"],
      {
        messageIds: held.map((run) => run.userMessageId),
        turnItemTypes: ["run_interrupt_request"],
        turnItemRunIds: [completedRunId],
      },
    );
    // Only the newest run completing shows the thread moved past the hold; a late or
    // bookkeeping update to an older run, overtaken by a failure or Stop, does not.
    if (
      latestExecutedRun(projection.runs)?.id !== completedRunId ||
      projection.turnItems.length > 0 ||
      worktreeSetupPending(projection)
    )
      return;
    const { messages } = projection;
    const unattended = new Set(
      messages
        .filter((message) => message.createdBy !== "user" || message.scheduledTaskId !== undefined)
        .map((message) => message.id),
    );
    const now = yield* DateTime.now;
    const released = held.filter((run) => unattended.has(run.userMessageId));
    if (released.length === 0) return;
    yield* writeSystemEvents(
      released.map((run) => ({
        type: "run.updated" as const,
        threadId,
        runId: run.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: { ...run, queueHeld: false },
      })),
    );
  });

  const resumeQueuedRuns = Effect.gen(function* () {
    const threadIds = yield* projectionStore.getRecoveryThreadIds("queued-runs");
    let resumed = 0;
    for (const threadId of threadIds) {
      const resumedThread = yield* Effect.gen(function* () {
        if (!(yield* projectionStore.canStartQueuedRun(threadId))) return false;
        yield* threadDispatch.withLock(threadId, startNextQueuedRun(threadId));
        return true;
      }).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Failed to resume queued V2 run after recovery", {
            threadId,
            cause,
          }).pipe(Effect.as(false)),
        ),
      );
      if (resumedThread) {
        resumed += 1;
      }
    }
    return resumed;
  }).pipe(
    Effect.mapError(
      (cause) =>
        new OrchestratorDispatchError({
          commandId: CommandId.make("command:system:resume-queued-runs"),
          commandType: "message.dispatch",
          cause,
        }),
    ),
  );

  const dispatchDelegatedTaskCompletionDeliveryResolution = Effect.fn(
    "Orchestrator.dispatchDelegatedTaskCompletionDeliveryResolution",
  )(function* (
    command: Extract<
      OrchestrationV2Command,
      {
        readonly type:
          | "delegated_task.completion-delivery.acknowledge"
          | "delegated_task.completion-delivery.dispose";
      }
    >,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const projection = yield* loadProjectionForCommand(command, ["subagents"], {
      subagentIds: [command.taskId],
    });
    const task = projection.subagents.find(
      (candidate) => candidate.id === command.taskId && candidate.origin === "app_owned",
    );
    if (task === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Delegated task ${command.taskId} is not an app-owned task of thread ${command.parentThreadId}.`,
      });
    }
    const state =
      command.type === "delegated_task.completion-delivery.acknowledge"
        ? "acknowledged"
        : "disposed";
    const now = yield* DateTime.now;
    const emitEvent = emit(events, command);
    const {
      runs: [parentRun],
    } = yield* loadProjectionForCommand(command, ["runs"], {
      runIds: task.runId === null ? [] : [task.runId],
    });
    // A late wait can arrive after finalization held the result for its still-live spawning run.
    if (isUnobservedAcknowledgement(command)) {
      if (
        !isTerminalDelegatedTaskStatus(task.status) ||
        task.completionDelivery?.state !== "pending"
      )
        return;
      if (parentRun !== undefined && isLiveRun(parentRun)) return;
      const records = yield* loadProjectionForCommand(
        command,
        ["runs", "subagents", "messages", "providerTurns"],
        { messageRoles: ["user"] },
      );
      const plan = yield* planDelegatedCompletionDelivery({
        parentProjection: records,
        parentRun,
        task,
        updatedTask: task,
        now,
      }).pipe(mapDispatchError(command));
      if (plan.task.completionDelivery?.state === "pending") return;
      yield* emitEvent({
        type: "subagent.updated",
        threadId: command.parentThreadId,
        ...(task.runId === null ? {} : { runId: task.runId }),
        nodeId: task.id,
        driver: task.driver,
        providerInstanceId: task.providerInstanceId,
        occurredAt: now,
        payload: plan.task,
      });
      for (const event of delegatedCompletionPlanEvents(
        command.parentThreadId,
        plan,
        projection.thread.providerInstanceId,
        now,
      )) {
        yield* emitEvent(event);
      }
      return plan.parentRun?.id;
    }
    // task_status and t3_thread_read use distinct command IDs, so two
    // valid observations can race after their read preflight. Re-emit the
    // existing task row so the second dispatch is a successful idempotent
    // no-op rather than "already acknowledged/disposed" or empty-events.
    if (
      task.completionDelivery?.state === state ||
      (command.type === "delegated_task.completion-delivery.acknowledge" &&
        task.completionDelivery?.state === "disposed")
    ) {
      yield* emitEvent({
        type: "subagent.updated",
        threadId: command.parentThreadId,
        ...(task.runId === null ? {} : { runId: task.runId }),
        nodeId: task.id,
        driver: task.driver,
        providerInstanceId: task.providerInstanceId,
        occurredAt: now,
        payload: task,
      });
      return;
    }
    const updatedTask: OrchestrationV2Subagent = {
      ...task,
      completionDelivery: {
        state,
        observedByRunId:
          command.type === "delegated_task.completion-delivery.acknowledge"
            ? command.observedByRunId
            : null,
      },
      updatedAt: now,
    };
    yield* emitEvent({
      type: "subagent.updated",
      threadId: command.parentThreadId,
      ...(task.runId === null ? {} : { runId: task.runId }),
      nodeId: task.id,
      driver: task.driver,
      providerInstanceId: task.providerInstanceId,
      occurredAt: now,
      payload: updatedTask,
    });

    const cohort = parentRun?.delegatedCompletion;
    const delivery = cohort?.delivery;
    if (
      parentRun === undefined ||
      cohort === undefined ||
      delivery === null ||
      delivery === undefined
    ) {
      return;
    }
    if (!delivery.taskIds.includes(task.id)) {
      return;
    }

    const { messages } = yield* loadProjectionForCommand(command, ["messages"], {
      messageRoles: ["user"],
      messageIds: [delivery.messageId],
    });
    const deliveryRunId = completionDeliveryMessage({ messages }, delivery)?.runId ?? null;
    const { runs } = yield* loadProjectionForCommand(command, ["runs"], {
      runIds: deliveryRunId === null ? [] : [deliveryRunId],
    });
    const deliveryProjection = { messages, runs };
    const remainingTaskIds = delivery.taskIds.filter((taskId) => taskId !== task.id);
    const deliveryRun = completionDeliveryRun(deliveryProjection, delivery);
    const clearDelivery =
      remainingTaskIds.length === 0 &&
      (deliveryRun === undefined || deliveryRun.status === "queued");
    const updatedCohort: OrchestrationV2DelegatedCompletionCohort = {
      ...cohort,
      delivery: clearDelivery
        ? null
        : {
            ...delivery,
            taskIds: remainingTaskIds,
          },
    };
    yield* emitEvent({
      type: "run.updated",
      threadId: command.parentThreadId,
      runId: parentRun.id,
      ...(parentRun.rootNodeId === null ? {} : { nodeId: parentRun.rootNodeId }),
      providerInstanceId: parentRun.providerInstanceId,
      occurredAt: now,
      payload: {
        ...parentRun,
        delegatedCompletion: updatedCohort,
      },
    });

    const message = completionDeliveryMessage(deliveryProjection, delivery);
    // A merged wake keeps the other parent runs' results; it ends only when it carries none.
    const ownership =
      message?.delegatedCompletion === undefined
        ? undefined
        : remainingTaskIds.length === 0
          ? withoutCompletionWakePart(message.delegatedCompletion, parentRun.id)
          : withCompletionWakePart(message.delegatedCompletion, {
              parentRunId: parentRun.id,
              generation: delivery.generation,
              taskIds: remainingTaskIds,
            });
    if (deliveryRun?.status === "queued") {
      if (ownership === undefined) {
        const rootNodeFilter = {
          nodeIds: deliveryRun.rootNodeId === null ? [] : [deliveryRun.rootNodeId],
        };
        yield* emitQueuedRunCancellation({
          command,
          events,
          // Attempts have no id filter; a wake that never started has no attempt to cancel.
          projection:
            deliveryRun.activeAttemptId === null
              ? {
                  attempts: [],
                  ...(yield* loadProjectionForCommand(command, ["nodes"], rootNodeFilter)),
                }
              : yield* loadProjectionForCommand(command, ["nodes", "attempts"], rootNodeFilter),
          run: deliveryRun,
          now,
        });
        return parentRun.id;
      }
      if (message !== undefined) {
        const { subagents: remainingTasks } = yield* loadProjectionForCommand(
          command,
          ["subagents"],
          { subagentIds: ownership.taskIds },
        );
        yield* emitEvent(
          wakeMessageUpdated(
            deliveryRun,
            yield* completionWakeMessage(message, ownership, remainingTasks, now).pipe(
              mapDispatchError(command),
            ),
            now,
          ),
        );
      }
    }
    return parentRun.id;
  });

  const disposeDelegatedCompletionCohort = Effect.fn(
    "Orchestrator.disposeDelegatedCompletionCohort",
  )(function* (input: {
    readonly command: OrchestrationV2ServerCommand;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly projection: Pick<
      OrchestrationV2ThreadProjection,
      "runs" | "subagents" | "messages" | "nodes" | "attempts"
    >;
    readonly parentRunId: RunId;
    readonly disposition: "stopped" | "disposed";
    readonly now: DateTime.Utc;
    readonly cancelQueuedDelivery?: boolean;
    // An interrupted or cancelled wake disposes only the results it carried. The cohort stays
    // open, and its delivery stays the wake's, so the wake's terminal reserves any sibling's result.
    readonly interruptedWakeTaskIds?: ReadonlyArray<NodeId>;
  }) {
    const parentRun = input.projection.runs.find((candidate) => candidate.id === input.parentRunId);
    if (parentRun === undefined) {
      return;
    }
    const cohort = parentRun.delegatedCompletion;
    const tasks = input.projection.subagents.filter(
      (candidate) => candidate.origin === "app_owned" && candidate.runId === input.parentRunId,
    );
    if (cohort === undefined && tasks.length === 0) {
      return;
    }
    const emitEvent = emit(input.events, input.command);
    const wakeTaskIds = input.interruptedWakeTaskIds;
    const keptTaskIds = new Set(
      wakeTaskIds !== undefined && cohort?.disposition === "open"
        ? tasks
            .filter(
              (task) =>
                !wakeTaskIds.includes(task.id) &&
                task.completionDelivery?.state !== "acknowledged" &&
                task.completionDelivery?.state !== "delivered" &&
                task.completionDelivery?.state !== "disposed",
            )
            .map((task) => task.id)
        : [],
    );
    const nextCohort =
      keptTaskIds.size > 0
        ? cohort!
        : ({
            disposition: cohort?.disposition === "disposed" ? "disposed" : input.disposition,
            nextGeneration: cohort?.nextGeneration ?? 1,
            delivery: null,
          } as const);
    yield* emitEvent({
      type: "run.updated",
      threadId: parentRun.threadId,
      runId: parentRun.id,
      ...(parentRun.rootNodeId === null ? {} : { nodeId: parentRun.rootNodeId }),
      providerInstanceId: parentRun.providerInstanceId,
      occurredAt: input.now,
      payload: {
        ...parentRun,
        delegatedCompletion: nextCohort,
      },
    });
    for (const task of tasks) {
      if (
        keptTaskIds.has(task.id) ||
        task.completionDelivery?.state === "acknowledged" ||
        task.completionDelivery?.state === "delivered" ||
        task.completionDelivery?.state === "disposed"
      ) {
        continue;
      }
      yield* emitEvent({
        type: "subagent.updated",
        threadId: parentRun.threadId,
        ...(task.runId === null ? {} : { runId: task.runId }),
        nodeId: task.id,
        driver: task.driver,
        providerInstanceId: task.providerInstanceId,
        occurredAt: input.now,
        payload: {
          ...task,
          completionDelivery: {
            state: "disposed",
            observedByRunId: null,
          },
          updatedAt: input.now,
        },
      });
    }
    const deliveryRun = completionDeliveryRun(input.projection, cohort?.delivery ?? null);
    const deliveryMessage = completionDeliveryMessage(input.projection, cohort?.delivery ?? null);
    // A merged wake still carries the other parent runs' results, so it only loses this one's.
    const remaining =
      deliveryMessage?.delegatedCompletion === undefined
        ? undefined
        : withoutCompletionWakePart(deliveryMessage.delegatedCompletion, parentRun.id);
    if (
      keptTaskIds.size === 0 &&
      input.cancelQueuedDelivery !== false &&
      deliveryRun?.status === "queued" &&
      deliveryMessage !== undefined &&
      remaining !== undefined
    ) {
      yield* emitEvent(
        wakeMessageUpdated(
          deliveryRun,
          yield* completionWakeMessage(
            deliveryMessage,
            remaining,
            input.projection.subagents,
            input.now,
          ).pipe(mapDispatchError(input.command)),
          input.now,
        ),
      );
    } else if (
      keptTaskIds.size === 0 &&
      input.cancelQueuedDelivery !== false &&
      deliveryRun?.status === "queued"
    ) {
      yield* emitQueuedRunCancellation({
        command: input.command,
        events: input.events,
        projection: input.projection,
        run: deliveryRun,
        now: input.now,
      });
    }
  });

  const disposeAllDelegatedCompletionCohorts = (input: {
    readonly command: OrchestrationV2Command;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly projection: Pick<
      OrchestrationV2ThreadProjection,
      "runs" | "subagents" | "messages" | "nodes" | "attempts"
    >;
    readonly now: DateTime.Utc;
    readonly cancelQueuedDelivery?: boolean;
  }) =>
    Effect.forEach(
      Array.from(
        new Set([
          ...input.projection.runs
            .filter((run) => run.delegatedCompletion !== undefined)
            .map((run) => run.id),
          ...input.projection.subagents
            .filter((task) => task.origin === "app_owned" && task.runId !== null)
            .map((task) => task.runId!),
        ]),
      ),
      (parentRunId) =>
        disposeDelegatedCompletionCohort({
          ...input,
          parentRunId,
          disposition: "disposed",
        }),
      { concurrency: 1, discard: true },
    );

  const dispatchThreadCreate = Effect.fn("orchestrationV2.dispatch.threadCreate")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "thread.create" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    yield* Effect.annotateCurrentSpan({
      "orchestration_v2.command_id": command.commandId,
      "orchestration_v2.command_type": command.type,
      "orchestration_v2.thread_id": command.threadId,
      "orchestration_v2.driver": command.modelSelection.instanceId,
    });

    // Tombstones reserve their ids too; recreating one would overwrite its history.
    const existing = yield* projectionStore.getThread(command.threadId).pipe(
      Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
      Effect.mapError(
        (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
      ),
    );
    if (existing !== null) {
      return yield* new OrchestratorThreadAlreadyExistsError({
        commandId: command.commandId,
        threadId: command.threadId,
      });
    }

    const now = yield* DateTime.now;
    const emitEvent = emit(events, command);
    const thread: OrchestrationV2AppThread = {
      createdBy: command.createdBy,
      creationSource: command.creationSource,
      id: command.threadId,
      projectId: command.projectId,
      title: command.title,
      providerInstanceId: command.modelSelection.instanceId,
      modelSelection: command.modelSelection,
      runtimeMode: command.runtimeMode,
      interactionMode: command.interactionMode,
      branch: command.branch,
      worktreePath: command.worktreePath,
      activeProviderThreadId: null,
      pullRequests: [],
      ...(command.linkOrigin === undefined ? {} : { linkOrigin: command.linkOrigin }),
      // Only stamped work names a remote parent, so a client cannot claim one.
      ...(command.linkOrigin === undefined || command.delegatedFrom === undefined
        ? {}
        : { delegatedFrom: command.delegatedFrom }),
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: command.threadId,
      },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      snoozedUntil: null,
      snoozedAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    };

    yield* emitEvent({
      type: "thread.created",
      threadId: command.threadId,
      providerInstanceId: command.modelSelection.instanceId,
      occurredAt: now,
      payload: thread,
    });
    if (command.importedNativeThread !== undefined) {
      yield* emitEvent({
        type: "provider-thread.updated",
        threadId: command.threadId,
        driver: command.importedNativeThread.ref.driver,
        providerInstanceId: command.modelSelection.instanceId,
        occurredAt: now,
        payload: {
          id: idAllocator.derive.providerThread({
            driver: command.importedNativeThread.ref.driver,
            providerInstanceId: command.modelSelection.instanceId,
            nativeThreadId: command.importedNativeThread.ref.nativeId,
          }),
          driver: command.importedNativeThread.ref.driver,
          providerInstanceId: command.modelSelection.instanceId,
          providerSessionId: null,
          appThreadId: command.threadId,
          ownerNodeId: null,
          nativeThreadRef: command.importedNativeThread.ref,
          nativeConversationHeadRef: null,
          status: "not_loaded",
          firstRunOrdinal: null,
          lastRunOrdinal: null,
          handoffIds: [],
          forkedFrom: null,
          contextUsage: null,
          nativeMetadata: command.importedNativeThread.metadata ?? null,
          createdAt: now,
          updatedAt: now,
        },
      });
    }
  });

  const dispatchThreadVisit = Effect.fn("orchestrationV2.dispatch.threadVisit")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "thread.visit" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const thread = yield* projectionStore
      .getThread(command.threadId)
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
        ),
      );
    if (thread.deletedAt !== null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is deleted.`,
      });
    }
    const visitedAt = DateTime.make(command.visitedAt);
    if (Option.isNone(visitedAt)) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} visit time ${command.visitedAt} is not a valid timestamp.`,
      });
    }
    const movesForward =
      thread.lastVisitedAt === null ||
      DateTime.toEpochMillis(visitedAt.value) > DateTime.toEpochMillis(thread.lastVisitedAt);
    const occurredAt = yield* DateTime.now;
    // Opens are timed by the server clock so devices with skewed clocks agree.
    const opens = command.opened
      ? recordThreadOpen(
          thread.opens ?? null,
          DateTime.toEpochMillis(occurredAt),
          command.searchQuery,
        )
      : null;
    if (!movesForward && opens === null) return;
    // Viewing a thread changes read state only. Loading its transcript, bumping
    // updatedAt, or copying the thread into the event makes a read receipt scale with it.
    yield* emit(
      events,
      command,
    )({
      type: "thread.visit-recorded",
      threadId: command.threadId,
      providerInstanceId: thread.providerInstanceId,
      occurredAt,
      payload: {
        lastVisitedAt: movesForward ? visitedAt.value : thread.lastVisitedAt,
        ...(opens ? { opens } : {}),
      },
    });
  });

  // Recheck under the lock after reading the host. Record the watch first so wake events carry it.
  const dispatchPullRequestWatchSync = Effect.fn("orchestrationV2.dispatch.pullRequestWatchSync")(
    function* (
      command: Extract<
        OrchestrationV2ServerCommand,
        { readonly type: "thread.pull-request-watch.sync" }
      >,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    ) {
      const thread = yield* projectionStore
        .getThread(command.threadId)
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
          ),
        );
      const { others = [], wake, ...primary } = command;
      const entries = [
        { ...primary, includesRemarks: wake?.includesRemarks },
        ...others.map((other) => ({
          type: primary.type,
          commandId: primary.commandId,
          threadId: primary.threadId,
          ...other,
        })),
      ].map((entry) => {
        const key = normalizeThreadPullRequestKey(entry);
        const link = threadPullRequestsOf(thread).find(
          (candidate) =>
            candidate.source !== "stack-dismissed" && threadPullRequestKeysEqual(candidate, key),
        );
        return { entry, key, link };
      });
      const inactive = refusesPullRequestNews(thread);
      // An idle thread dispatches immediately, bypassing queued-wake validation.
      const outdated = entries.map(
        ({ entry, link }) =>
          wake?.notification.source.kind === "monitor" &&
          entry.watch !== null &&
          pullRequestSnapshotEndsWatch(link?.snapshot ?? null, entry.startedAt),
      );
      if (
        entries.some(({ entry, link }) => link?.watch?.startedAt !== entry.startedAt) ||
        (wake && inactive) ||
        // One wake cannot drop the news of just some of its pull requests; the sender splits it.
        (entries.length > 1 && outdated.some(Boolean))
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "The pull request watch ended or its thread settled while it was read.",
        });
      }
      for (const { entry } of entries) {
        const { includesRemarks: _, ...mutation } = entry;
        yield* dispatchThreadMutation(mutation, events, effects);
      }
      if (wake === undefined || outdated[0]) return;
      const { sections, ...wakeMessage } = wake;
      const stamps = entries.map(({ entry, key }, index) => ({
        ...key,
        watchStartedAt: entry.startedAt,
        headSha: entry.includesRemarks === true ? null : (entry.watch?.headSha ?? null),
        ...(entry.watch === null ? { watchEnded: true } : {}),
        ...(entries.length > 1 && sections?.[index] !== undefined
          ? { section: sections[index] }
          : {}),
      }));
      yield* dispatchMessage(
        {
          type: "message.dispatch",
          commandId: command.commandId,
          threadId: command.threadId,
          messageId: wakeMessage.messageId,
          text: wakeMessage.text,
          notification: {
            ...wakeMessage.notification,
            source:
              wake.notification.source.kind === "monitor"
                ? {
                    kind: "monitor",
                    pullRequest: stamps[0]!,
                    ...(stamps.length > 1 ? { otherPullRequests: stamps.slice(1) } : {}),
                  }
                : wake.notification.source,
          },
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        },
        events,
        effects,
      );
    },
  );

  // Deleted after the share change commits: the files of waiting messages it dismissed or dropped.
  const releaseGuestAttachments = (
    command: { readonly commandId: CommandId; readonly threadId: ThreadId },
    before: OrchestrationV2AppThread["guestShare"],
    after: OrchestrationV2AppThread["guestShare"],
    effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
  ) => {
    const attachmentIds = releasedGuestAttachmentIds(before ?? null, after ?? null);
    return attachmentIds.length === 0
      ? Effect.void
      : Ref.update(effects, (existing) => [
          ...existing,
          {
            id: `effect:${command.commandId}:guest-attachment.cleanup`,
            commandId: command.commandId,
            threadId: command.threadId,
            request: { type: "attachment.cleanup", attachmentIds: [...attachmentIds] },
          } satisfies PendingOrchestrationEffectV2,
        ]);
  };

  const dispatchGuestShare = Effect.fn("orchestrationV2.dispatch.guestShare")(function* (
    command: GuestShareCommand,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
  ) {
    const projection = yield* projectionStore
      .getThreadRecords(command.threadId, [])
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
        ),
      );
    const thread = projection.thread;
    const now = yield* DateTime.now;
    const decided =
      thread.deletedAt !== null
        ? Result.fail(`Thread ${command.threadId} is deleted.`)
        : decideGuestShareCommand({
            thread,
            command,
            now,
          });
    if (Result.isFailure(decided)) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: decided.failure,
      });
    }
    if (decided.success === thread) return;
    yield* emit(
      events,
      command,
    )({
      type: "thread.guest-share-updated",
      threadId: command.threadId,
      providerInstanceId: thread.providerInstanceId,
      occurredAt: now,
      payload: decided.success,
    });
    yield* releaseGuestAttachments(command, thread.guestShare, decided.success.guestShare, effects);
  });

  // Emitted before the message so the send and the status change commit together, and so
  // `dispatchMessage`, which reads pending events, keeps the updated share.
  const emitGuestMessageSent = Effect.fn("orchestrationV2.dispatch.guestMessageSent")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>,
    guestMessageId: GuestMessageId,
    thread: OrchestrationV2AppThread,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const now = yield* DateTime.now;
    const decided = decideGuestMessageSend({ thread, guestMessageId, now });
    if (Result.isFailure(decided)) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: decided.failure,
      });
    }
    yield* emit(
      events,
      command,
    )({
      type: "thread.guest-share-updated",
      threadId: command.threadId,
      providerInstanceId: thread.providerInstanceId,
      occurredAt: now,
      payload: decided.success.thread,
    });
    return decided.success;
  });

  const dispatchThreadMutation = Effect.fn("orchestrationV2.dispatch.threadMutation")(function* (
    command: Extract<
      OrchestrationV2ServerCommand,
      {
        readonly type:
          | "thread.archive"
          | "thread.unarchive"
          | "thread.settle"
          | "thread.unsettle"
          | "thread.snooze"
          | "thread.unsnooze"
          | "thread.auto-settle.set"
          | "thread.pin"
          | "thread.unpin"
          | "thread.pin.reorder"
          | "thread.tag.set"
          | "thread.active.reorder"
          | "thread.mark-unread"
          | "thread.metadata.update"
          | "thread.pull-request.link"
          | "thread.pull-request.unlink"
          | "thread.pull-request-link.sync"
          | "thread.pull-request.watch"
          | "thread.pull-request-watch.sync"
          | "thread.pull-request-watch.checks-reopen"
          | "thread.pull-request.sync"
          | "thread.title.regeneration.complete"
          | "thread.runtime-mode.set"
          | "thread.interaction-mode.set"
          | "thread.model-selection.set"
          | "provider.switch";
      }
    >,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
  ) {
    const thread = yield* projectionStore.getThread(command.threadId).pipe(
      Effect.mapError(
        (cause) =>
          new OrchestratorProjectionError({
            threadId: command.threadId,
            cause,
          }),
      ),
    );
    if (thread.deletedAt !== null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is deleted.`,
      });
    }
    if (
      command.type === "thread.pull-request.watch" &&
      command.watching &&
      isProviderNativeSubagentThread(thread)
    ) {
      return yield* new OrchestratorSubagentThreadReadOnlyError({
        commandId: command.commandId,
        threadId: command.threadId,
      });
    }
    if (
      command.type === "thread.pull-request.watch" &&
      command.watching &&
      (thread.settledOverride === "settled" ||
        thread.settledAt !== null ||
        thread.archivedAt !== null)
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is settled or archived and cannot watch pull requests.`,
      });
    }
    // Child tasks report to their parent, which owns PR monitoring; child watches would cause unrelated wakes.
    if (
      command.type === "thread.pull-request.watch" &&
      command.watching &&
      thread.lineage.relationshipToParent === "subagent"
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is a subagent; its parent thread watches pull requests.`,
      });
    }
    // Agent watches use the latest executed run to catch calls racing Stop; users can still watch.
    if (
      command.type === "thread.pull-request.watch" &&
      command.watching &&
      command.link?.source === "agent"
    ) {
      const { runs } = yield* projectionStore
        .getThreadRecords(command.threadId, ["runs"])
        .pipe(mapDispatchError(command));
      const latest = latestExecutedRun(runs);
      if (latest !== null && (yield* stopReachedRun(command, command.threadId, latest.id))) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} was stopped.`,
        });
      }
    }
    if (
      command.type === "thread.metadata.update" &&
      command.expectedWorktreePath !== undefined &&
      command.expectedWorktreePath !== thread.worktreePath
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} worktree changed before the metadata update could be applied.`,
      });
    }
    if (
      command.type === "thread.metadata.update" &&
      command.expectedBranch !== undefined &&
      command.expectedBranch !== thread.branch
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} branch changed before the metadata update could be applied.`,
      });
    }
    if (command.type === "thread.metadata.update" && command.expectedEmpty === true) {
      const records = yield* projectionStore
        .getThreadRecords(command.threadId, ["runs"])
        .pipe(mapDispatchError(command));
      const messageCount = yield* projectionStore
        .getMessageCount(command.threadId)
        .pipe(mapDispatchError(command));
      if (messageCount > 0 || records.runs.length > 0)
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} is no longer empty.`,
        });
    }
    if (command.type === "thread.archive" && thread.archivedAt !== null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is already archived.`,
      });
    }
    if (command.type === "thread.archive") {
      const records = yield* projectionStore
        .getThreadRecords(command.threadId, ["subagents"])
        .pipe(mapDispatchError(command));
      if (
        records.subagents.some(
          (task) =>
            task.origin === "app_owned" &&
            task.childThreadId !== null &&
            isOrchestrationV2WorkActive(task.status),
        )
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} has running delegated tasks. Wait for them or cancel them before archiving.`,
        });
      }
    }
    if (command.type === "thread.unarchive" && thread.archivedAt === null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is not archived.`,
      });
    }
    if (
      (command.type === "thread.settle" ||
        command.type === "thread.unsettle" ||
        command.type === "thread.snooze" ||
        command.type === "thread.unsnooze" ||
        command.type === "thread.auto-settle.set" ||
        command.type === "thread.pin" ||
        command.type === "thread.unpin" ||
        command.type === "thread.pin.reorder" ||
        command.type === "thread.active.reorder" ||
        command.type === "thread.pull-request.sync") &&
      thread.archivedAt !== null
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is archived.`,
      });
    }
    // Reject reorder after unpin so a raced request cannot restore a cleared pin.
    if (command.type === "thread.pin.reorder" && thread.pinnedAt == null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is not pinned and cannot be reordered.`,
      });
    }
    if (
      command.type === "thread.active.reorder" &&
      (thread.pinnedAt != null || thread.settledOverride === "settled")
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is not active and cannot be reordered.`,
      });
    }
    if (command.type === "thread.pull-request.sync") {
      const project = yield* projects.get(command.projectId).pipe(mapDispatchError(command));
      const currentSequence = yield* eventSink.latestSequence({ threadId: command.threadId }).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorProjectionError({
              threadId: command.threadId,
              cause,
            }),
        ),
      );
      if (
        Option.isNone(project) ||
        project.value.deletedAt !== null ||
        project.value.workspaceRoot !== command.expected.workspaceRoot ||
        // A replacement rewrites pullRequests, which the expected fields do not capture.
        (command.linkedPullRequest !== undefined && currentSequence > command.snapshotSequence) ||
        thread.projectId !== command.projectId ||
        thread.branch !== command.expected.branch ||
        thread.worktreePath !== command.expected.worktreePath ||
        !threadPullRequestLinksEqual(
          thread.linkedPullRequest ?? null,
          command.expected.linkedPullRequest,
        ) ||
        !threadPullRequestLinksEqual(
          thread.branchPullRequest ?? null,
          command.expected.branchPullRequest,
        )
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} changed before pull request discovery.`,
        });
      }
    }
    if (command.type === "thread.settle") {
      const projection = yield* loadProjectionForCommand(command, ["runs", "runtimeRequests"]);
      if (
        command.expectedLatestRunId !== undefined &&
        projection.runs.reduce<OrchestrationV2Run | undefined>(
          (latest, run) => (latest === undefined || run.ordinal > latest.ordinal ? run : latest),
          undefined,
        )?.id !== command.expectedLatestRunId
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "The thread received another run before settlement.",
        });
      }
      const queuedMessages = yield* projectionStore
        .getMessagesById(
          command.threadId,
          projection.runs.filter((run) => run.status === "queued").map((run) => run.userMessageId),
        )
        .pipe(mapDispatchError(command));
      const automaticMessageIds = new Set(
        [...queuedMessages.values()]
          .filter(
            (message) =>
              message.role === "user" &&
              (message.createdBy !== "user" ||
                message.scheduledTaskId !== undefined ||
                message.notification !== undefined ||
                message.delegatedCompletion !== undefined),
          )
          .map((message) => message.id),
      );
      const automaticQueuedRuns = projection.runs.filter(
        (run) => run.status === "queued" && automaticMessageIds.has(run.userMessageId),
      );
      const runningRunExists = projection.runs.some((run) =>
        ["preparing", "starting", "running", "waiting"].includes(run.status),
      );
      const queuedMessageExists = projection.runs.some(
        (run) => run.status === "queued" && !automaticQueuedRuns.includes(run),
      );
      const pendingRequests = projection.runtimeRequests.filter(
        (request) => request.status === "pending",
      );
      const blockingRequestExists = pendingRequests.some(
        (request) => request.kind !== "user_input" || request.responseCapability.type !== "message",
      );
      const blocker = blockingRequestExists
        ? "is waiting on an approval or question. Answer it before settling."
        : runningRunExists
          ? "is still running. Stop it before settling."
          : queuedMessageExists
            ? "has a queued message. Send it or remove it from the queue before settling."
            : null;
      if (blocker !== null) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} ${blocker}`,
        });
      }

      for (const request of pendingRequests) {
        yield* dispatchRuntimeRequestRespond(
          {
            type: "runtime-request.respond",
            commandId: command.commandId,
            threadId: command.threadId,
            requestId: request.id,
            decision: "cancel",
          },
          events,
          effects,
        );
      }
      for (const run of automaticQueuedRuns) {
        yield* dispatchQueuedRunCancel(
          {
            type: "queued-run.cancel",
            commandId: command.commandId,
            threadId: command.threadId,
            runId: run.id,
          },
          events,
          "cohort",
        );
      }
    }

    const needsProviderState =
      command.type === "thread.runtime-mode.set" ||
      command.type === "thread.model-selection.set" ||
      command.type === "provider.switch" ||
      command.type === "thread.archive" ||
      command.type === "thread.settle" ||
      (command.type === "thread.metadata.update" &&
        command.worktreePath !== undefined &&
        command.worktreePath !== thread.worktreePath);
    const providerContext = needsProviderState
      ? yield* projectionStore
          .getThreadProviderContext(
            command.threadId,
            command.type === "thread.model-selection.set" || command.type === "provider.switch"
              ? command.modelSelection.instanceId
              : undefined,
          )
          .pipe(mapDispatchError(command))
      : null;
    const providerSwitchPlan =
      command.type === "thread.model-selection.set" || command.type === "provider.switch"
        ? yield* Effect.gen(function* () {
            yield* providerAdapters.get(command.modelSelection.instanceId).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorProviderAdapterError({
                    commandId: command.commandId,
                    providerInstanceId: command.modelSelection.instanceId,
                    cause,
                  }),
              ),
            );
            const plan = yield* providerSwitchService
              .plan({
                projection: providerContext!,
                targetModelSelection: command.modelSelection,
              })
              .pipe(mapDispatchError(command));
            if (plan.instanceChanged || plan.releaseProviderSessionIds.length > 0) {
              const projection = yield* loadProjectionForCommand(command, ["runs"], {
                turnItemTypes: [],
              });
              if (projection.runs.some(isActiveRun)) {
                return yield* new OrchestratorProviderSelectionBusyError({
                  commandId: command.commandId,
                  threadId: command.threadId,
                });
              }
            }
            return plan;
          })
        : null;

    // Archive and settle stop idle background work; serialized commands prevent starts racing detach.
    // Settlement rejects active or blocked runs.
    const detachSessionIds = new Set(
      command.type === "thread.archive" || command.type === "thread.settle"
        ? (providerContext?.providerSessions ?? []).map((session) => session.id)
        : command.type === "thread.metadata.update" &&
            command.worktreePath !== undefined &&
            command.worktreePath !== thread.worktreePath
          ? (providerContext?.providerSessions ?? []).map((session) => session.id)
          : command.type === "thread.runtime-mode.set"
            ? (providerContext?.providerSessions ?? [])
                .filter(
                  (session) => !session.capabilities.sessions.supportsRuntimeModeSwitchInSession,
                )
                .map((session) => session.id)
            : (providerSwitchPlan?.releaseProviderSessionIds ?? []),
    );
    const liveSessions = (providerContext?.providerSessions ?? []).filter(
      (session) =>
        detachSessionIds.has(session.id) &&
        session.status !== "stopped" &&
        session.status !== "error",
    );
    if (
      command.type !== "thread.archive" &&
      command.type !== "thread.settle" &&
      liveSessions.length > 0
    ) {
      // The provider context holds only the active and target provider threads; a session kept
      // busy by an earlier queued hand-off owns neither, so probe every thread of this one.
      const { providerThreads } = yield* projectionStore
        .getThreadRecords(command.threadId, ["providerThreads"], {
          omitSettledSubagentRecords: true,
        })
        .pipe(mapDispatchError(command));
      for (const session of liveSessions) {
        if (yield* sessionHasBackgroundWork(session, providerThreads)) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause:
              "The current session is still running background agents or commands, and this change would end them. Wait for them to finish, or press Stop, then try again.",
          });
        }
      }
    }

    const now = yield* DateTime.now;
    if (command.type === "thread.metadata.update" && command.limitRecovery != null) {
      const projection = yield* loadProjectionForCommand(
        command,
        ["runs", "runtimeRequests", "turnItems"],
        { turnItemTypes: ["error"] },
      );
      const run = usageLimitBlockedRun(projection.runs, projection.turnItems, null);
      const failure = latestRootProviderFailure(run, projection.turnItems);
      const resetMs = Date.parse(command.limitRecovery.resetAt);
      if (command.limitRecovery.snooze === true && resetMs <= DateTime.toEpochMillis(now)) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "The reset time has passed. Retry the thread manually.",
        });
      }
      if (thread.settledOverride === "settled") {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "The thread is settled. Un-settle it to resume at the reset.",
        });
      }
      if (
        !Number.isFinite(resetMs) ||
        thread.archivedAt !== null ||
        run?.id !== command.limitRecovery.runId ||
        failure?.class !== "usage_limit" ||
        failure.resetAt !== command.limitRecovery.resetAt ||
        resetMs <= DateTime.toEpochMillis(run.completedAt ?? run.requestedAt) ||
        projection.runtimeRequests.some((request) => request.status === "pending")
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "The provider limit changed before recovery could be configured.",
        });
      }
    }
    let restoredUnsettledAt: DateTime.Utc | null | undefined;
    if (command.type === "thread.unsettle" && command.restoreUnsettledAt !== undefined) {
      const parsed =
        command.restoreUnsettledAt === null
          ? Option.some(null)
          : DateTime.make(command.restoreUnsettledAt);
      if (Option.isNone(parsed)) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} unsettle anchor ${command.restoreUnsettledAt} is not a valid time.`,
        });
      }
      restoredUnsettledAt = parsed.value;
    }
    let snoozedUntil: DateTime.Utc | null = null;
    if (command.type === "thread.snooze") {
      const projection = yield* loadProjectionForCommand(command, ["runs", "runtimeRequests"], {
        turnItemTypes: [],
      });
      const parsedSnoozedUntil = DateTime.make(command.snoozedUntil);
      if (
        Option.isNone(parsedSnoozedUntil) ||
        DateTime.toEpochMillis(parsedSnoozedUntil.value) <= DateTime.toEpochMillis(now)
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} snooze wake time ${command.snoozedUntil} is not in the future.`,
        });
      }
      if (
        projection.runtimeRequests.some(
          (request) => request.status === "pending" && request.kind !== "user_input",
        )
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} has a pending approval and cannot be snoozed.`,
        });
      }
      const queuedRuns = projection.runs.filter((run) => run.status === "queued");
      const queuedMessages =
        queuedRuns.length === 0
          ? []
          : (yield* loadProjectionForCommand(command, ["messages"], {
              messageIds: queuedRuns.map((run) => run.userMessageId),
            })).messages;
      const automaticMessageIds = new Set(
        queuedMessages
          .filter(
            (message) =>
              message.notification !== undefined || message.delegatedCompletion !== undefined,
          )
          .map((message) => message.id),
      );
      const userRuns = queuedRuns.filter((run) => !automaticMessageIds.has(run.userMessageId));
      // User work waiting behind a usage limit cannot start until the reset or the user acts.
      const latest = latestExecutedRun(projection.runs);
      const limitRecords =
        userRuns.length === 0 || latest?.status !== "failed"
          ? null
          : yield* loadProjectionForCommand(command, ["turnItems", "providerSessions"], {
              turnItemTypes: ["error"],
              turnItemRunIds: [latest.id],
            });
      const heldBehindLimit =
        limitRecords !== null &&
        userRuns.every((run) =>
          usageLimitHoldsQueue({ ...limitRecords, runs: projection.runs }, run.providerInstanceId),
        );
      if (userRuns.length > 0 && !heldBehindLimit) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} has a queued run and cannot be snoozed.`,
        });
      }
      snoozedUntil = parsedSnoozedUntil.value;
    }
    let markUnreadVisitedAt: DateTime.Utc | null = null;
    if (command.type === "thread.mark-unread") {
      const shell = yield* projectionStore
        .getThreadShell(command.threadId)
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
          ),
        );
      const latestRunCompletedAt = shell?.latestRunCompletedAt ?? null;
      if (latestRunCompletedAt === null) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} has no completed run to mark unread.`,
        });
      }
      markUnreadVisitedAt = DateTime.subtract(latestRunCompletedAt, { milliseconds: 1 });
    }
    const updatedThread: OrchestrationV2AppThread = (() => {
      switch (command.type) {
        case "thread.archive":
          return {
            ...thread,
            archivedAt: now,
            titleRegeneration: null,
            guestShare: null,
            pullRequests: thread.pullRequests?.map((link) => withPullRequestWatch(link, undefined)),
            updatedAt: now,
          };
        case "thread.unarchive":
          return { ...thread, archivedAt: null, updatedAt: now };
        case "thread.settle": {
          const wasPinned = thread.pinnedAt != null;
          const alreadySettled =
            thread.settledOverride === "settled" && thread.settledAt !== null && !wasPinned;
          return {
            ...thread,
            settledOverride: "settled",
            settledAt: alreadySettled ? thread.settledAt : (command.settledAt ?? now),
            // Settled threads refuse recovery, so an armed resume would stay hidden until un-settled.
            limitRecovery: thread.limitRecovery
              ? { ...thread.limitRecovery, autoResume: false, snooze: false }
              : thread.limitRecovery,
            pullRequests: thread.pullRequests?.map((link) => withPullRequestWatch(link, undefined)),
            unsettledAt: null,
            pinnedAt: null,
            pinOrderKey: null,
            activeOrderKey: null,
            snoozedUntil: null,
            snoozedAt: null,
            updatedAt:
              alreadySettled && thread.snoozedUntil == null && thread.snoozedAt == null
                ? thread.updatedAt
                : now,
          };
        }
        case "thread.unsettle": {
          const alreadyPinnedActive = thread.settledOverride === "active";
          return {
            ...thread,
            settledOverride: "active",
            settledAt: null,
            unsettledAt: alreadyPinnedActive
              ? (thread.unsettledAt ?? null)
              : restoredUnsettledAt === undefined
                ? now
                : restoredUnsettledAt,
            updatedAt: alreadyPinnedActive ? thread.updatedAt : now,
          };
        }
        case "thread.snooze": {
          // Re-snoozing acknowledges the latest wake even at the same time; commandId deduplicates retries.
          return {
            ...thread,
            snoozedUntil,
            limitRecovery: thread.limitRecovery ? { ...thread.limitRecovery, snooze: false } : null,
            snoozedAt: now,
            updatedAt: now,
          };
        }
        case "thread.unsnooze": {
          const alreadyAwake = thread.snoozedUntil == null;
          return {
            ...thread,
            snoozedUntil: null,
            snoozedAt: null,
            lastSnoozeWakeAt: alreadyAwake ? thread.lastSnoozeWakeAt : now,
            updatedAt: alreadyAwake ? thread.updatedAt : now,
          };
        }
        case "thread.auto-settle.set": {
          const unchanged = command.enabled === (thread.autoSettleDisabledAt == null);
          return {
            ...thread,
            autoSettleDisabledAt: command.enabled ? null : (thread.autoSettleDisabledAt ?? now),
            updatedAt: unchanged ? thread.updatedAt : now,
          };
        }
        case "thread.pin": {
          const alreadyPinned = thread.pinnedAt != null;
          const promotes = thread.settledOverride === "settled" || thread.snoozedUntil != null;
          return {
            ...thread,
            pinnedAt: alreadyPinned ? thread.pinnedAt : now,
            ...(alreadyPinned || command.orderKey === undefined
              ? {}
              : { pinOrderKey: command.orderKey }),
            settledOverride:
              thread.settledOverride === "settled" ? "active" : thread.settledOverride,
            settledAt: thread.settledOverride === "settled" ? null : thread.settledAt,
            ...(thread.settledOverride === "settled" ? { unsettledAt: now } : {}),
            snoozedUntil: null,
            snoozedAt: null,
            lastSnoozeWakeAt: thread.snoozedUntil == null ? thread.lastSnoozeWakeAt : now,
            updatedAt: alreadyPinned && !promotes ? thread.updatedAt : now,
          };
        }
        case "thread.unpin": {
          const alreadyUnpinned = thread.pinnedAt == null;
          return {
            ...thread,
            pinnedAt: null,
            pinOrderKey: null,
            updatedAt: alreadyUnpinned ? thread.updatedAt : now,
          };
        }
        case "thread.pin.reorder": {
          const keyUnchanged = thread.pinOrderKey === command.orderKey;
          return {
            ...thread,
            pinOrderKey: command.orderKey,
            updatedAt: keyUnchanged ? thread.updatedAt : now,
          };
        }
        case "thread.active.reorder": {
          return {
            ...thread,
            activeOrderKey: command.orderKey,
            updatedAt: thread.updatedAt,
          };
        }
        case "thread.tag.set": {
          const current = thread.tags ?? [];
          return {
            ...thread,
            tags: command.tagged
              ? current.includes(command.tag)
                ? current
                : [...current, command.tag]
              : current.filter((tag) => tag !== command.tag),
          };
        }
        case "thread.mark-unread": {
          // Retire the wake before rewinding the visit, preserving its settlement timer.
          const snoozeExpired =
            thread.snoozedUntil != null &&
            DateTime.toEpochMillis(thread.snoozedUntil) <= DateTime.toEpochMillis(now);
          return {
            ...thread,
            lastVisitedAt: markUnreadVisitedAt,
            ...(snoozeExpired
              ? { snoozedUntil: null, snoozedAt: null, lastSnoozeWakeAt: thread.snoozedUntil }
              : {}),
          };
        }
        case "thread.metadata.update": {
          const previousRecovery =
            thread.limitRecovery?.runId === command.limitRecovery?.runId &&
            thread.limitRecovery?.resetAt === command.limitRecovery?.resetAt
              ? thread.limitRecovery
              : null;
          const limitRecovery =
            command.limitRecovery === undefined
              ? thread.limitRecovery
              : command.limitRecovery === null
                ? null
                : {
                    ...command.limitRecovery,
                    autoResume:
                      command.limitRecovery.autoResume ?? previousRecovery?.autoResume ?? false,
                    snooze: command.limitRecovery.snooze ?? previousRecovery?.snooze ?? false,
                    requestId: command.commandId,
                  };
          return {
            ...thread,
            ...(command.title === undefined ? {} : { title: command.title }),
            ...(command.limitRecovery === undefined ? {} : { limitRecovery }),
            ...(command.limitRecovery !== undefined &&
            limitRecovery?.snooze === true &&
            Date.parse(limitRecovery.resetAt) > DateTime.toEpochMillis(now)
              ? {
                  snoozedUntil: DateTime.makeUnsafe(limitRecovery.resetAt),
                  // Recovery updates acknowledge the same stopped run; their timestamps must not trigger fresh failure wakes.
                  snoozedAt: now,
                }
              : command.limitRecovery !== undefined &&
                  thread.limitRecovery?.snooze &&
                  thread.snoozedUntil != null &&
                  DateTime.toEpochMillis(thread.snoozedUntil) ===
                    Date.parse(thread.limitRecovery.resetAt)
                ? { snoozedUntil: null, snoozedAt: null, lastSnoozeWakeAt: now }
                : {}),
            ...(command.branch === undefined ? {} : { branch: command.branch }),
            ...(command.worktreePath === undefined ? {} : { worktreePath: command.worktreePath }),
            ...(command.linkedPullRequest === undefined
              ? {}
              : {
                  linkedPullRequest: command.linkedPullRequest,
                  pullRequests: [
                    ...threadPullRequestsOf(thread).filter(
                      (link) =>
                        !(
                          thread.linkedPullRequest &&
                          threadPullRequestKeysEqual(
                            link,
                            legacyThreadPullRequestKey(thread.linkedPullRequest),
                          )
                        ) &&
                        !(
                          command.linkedPullRequest &&
                          threadPullRequestKeysEqual(
                            link,
                            legacyThreadPullRequestKey(command.linkedPullRequest),
                          )
                        ),
                    ),
                    ...(command.linkedPullRequest
                      ? [legacyPullRequestLink(thread, command.linkedPullRequest, now)]
                      : []),
                  ],
                }),
            ...(command.regenerateTitle === true
              ? { titleRegeneration: { requestId: command.commandId, startedAt: now } }
              : command.regenerateTitle === false || command.title !== undefined
                ? { titleRegeneration: null }
                : {}),
            updatedAt: now,
          };
        }
        case "thread.pull-request.link":
        case "thread.pull-request.unlink":
        case "thread.pull-request-link.sync": {
          const key = normalizeThreadPullRequestKey(command);
          const links = threadPullRequestsOf(thread);
          const existing = links.find((link) => threadPullRequestKeysEqual(link, key));
          let pullRequests = links;
          if (command.type === "thread.pull-request.link") {
            const undismisses =
              existing?.source === "stack-dismissed" &&
              command.source !== "stack" &&
              command.source !== "stack-dismissed";
            if (existing && !undismisses) return thread;
            const link = existing
              ? {
                  ...existing,
                  source: command.source,
                  url: canonicalizeGitCafePullRequestUrl(command.url),
                  ...(command.repositoryWebUrl !== undefined && {
                    repositoryWebUrl: command.repositoryWebUrl,
                  }),
                }
              : {
                  ...key,
                  url: canonicalizeGitCafePullRequestUrl(command.url),
                  ...(command.repositoryWebUrl !== undefined && {
                    repositoryWebUrl: command.repositoryWebUrl,
                  }),
                  source: command.source,
                  linkedAt: DateTime.formatIso(now),
                  snapshot: null,
                  stack: null,
                };
            const branch = thread.branchPullRequest;
            const branchKey = branch ? legacyThreadPullRequestKey(branch) : null;
            pullRequests = [
              ...(command.source === "manual" &&
              branch &&
              branchKey &&
              visibleThreadPullRequests(links).length === 0 &&
              !threadPullRequestKeysEqual(branchKey, key) &&
              !links.some((entry) => threadPullRequestKeysEqual(entry, branchKey))
                ? [
                    {
                      ...branchKey,
                      url: canonicalizeGitCafePullRequestUrl(branch.url),
                      source: "manual" as const,
                      linkedAt: DateTime.formatIso(now),
                      snapshot: null,
                      stack: null,
                    },
                  ]
                : []),
              ...links.filter((entry) => !threadPullRequestKeysEqual(entry, key)),
              link,
            ];
          } else if (command.type === "thread.pull-request.unlink") {
            if (!existing) return thread;
            const belongsToStack =
              existing.source === "stack" ||
              existing.stack !== null ||
              links.some(
                (link) =>
                  link.host.toLowerCase() === key.host &&
                  link.repository.toLowerCase() === key.repository &&
                  link.stack?.layers.some((layer) => layer.number === key.number),
              );
            pullRequests = belongsToStack
              ? links.map((link) =>
                  link === existing
                    ? {
                        ...withPullRequestWatch(link, undefined),
                        source: "stack-dismissed" as const,
                      }
                    : link,
                )
              : links.filter((link) => link !== existing);
          } else {
            if (!existing) return thread;
            pullRequests = links.map((link) =>
              link === existing
                ? { ...link, snapshot: command.snapshot, stack: command.stack }
                : link,
            );
          }
          return {
            ...thread,
            pullRequests,
            linkedPullRequest:
              thread.linkedPullRequest &&
              pullRequests.some(
                (link) =>
                  link.source !== "stack-dismissed" &&
                  threadPullRequestKeysEqual(
                    link,
                    legacyThreadPullRequestKey(thread.linkedPullRequest!),
                  ),
              )
                ? thread.linkedPullRequest
                : null,
            updatedAt: command.type === "thread.pull-request-link.sync" ? thread.updatedAt : now,
          };
        }
        case "thread.pull-request-watch.checks-reopen": {
          const linked = threadPullRequestsOf(thread);
          if (!linked.some((link) => link.watch?.checksPending !== undefined)) return thread;
          return {
            ...thread,
            pullRequests: linked.map((link) =>
              link.watch?.checksPending === undefined
                ? link
                : { ...link, watch: withoutChecksAnswer(link.watch) },
            ),
          };
        }
        case "thread.pull-request.watch":
        case "thread.pull-request-watch.sync": {
          const key = normalizeThreadPullRequestKey(command);
          const startedAt = DateTime.formatIso(now);
          const linked = threadPullRequestsOf(thread);
          const visible = (link: ThreadPullRequestLink) =>
            link.source !== "stack-dismissed" && threadPullRequestKeysEqual(link, key);
          const dismissed = linked.find((link) => threadPullRequestKeysEqual(link, key));
          const links =
            command.type === "thread.pull-request.watch" &&
            command.watching &&
            command.link !== undefined &&
            !linked.some(visible)
              ? [
                  ...linked.filter((link) => !threadPullRequestKeysEqual(link, key)),
                  {
                    ...key,
                    url: canonicalizeGitCafePullRequestUrl(command.link.url),
                    ...((command.link.repositoryWebUrl ?? dismissed?.repositoryWebUrl) !==
                      undefined && {
                      repositoryWebUrl:
                        command.link.repositoryWebUrl ?? dismissed?.repositoryWebUrl,
                    }),
                    source: command.link.source,
                    linkedAt: startedAt,
                    snapshot: null,
                    stack: null,
                    ...(dismissed?.endedWatch && { endedWatch: dismissed.endedWatch }),
                  },
                ]
              : linked;
          const existing = links.find(visible);
          if (existing === undefined) return thread;
          const watch =
            command.type === "thread.pull-request-watch.sync"
              ? existing.watch?.startedAt === command.startedAt
                ? syncedPullRequestWatch(existing.watch, command)
                : existing.watch
              : !command.watching
                ? undefined
                : (existing.watch ?? {
                    startedAt,
                    headSha: null,
                    failedChecks: [],
                    passed: false,
                    passedChecks: [],
                    conflicting: false,
                    // Watching again must not repeat check or conflict news the last watch told.
                    ...(existing.endedWatch && {
                      headSha: existing.endedWatch.headSha,
                      failedChecks: existing.endedWatch.failedChecks,
                      passed: existing.endedWatch.passed,
                      passedChecks: existing.endedWatch.passedChecks,
                      conflicting: existing.endedWatch.conflicting,
                      rebaseRequired: existing.endedWatch.rebaseRequired,
                    }),
                    awaitingBaseline: existing.endedWatch
                      ? existing.endedWatch.awaitingBaseline
                      : "all",
                    ...(existing.endedWatch?.owedChecks !== undefined && {
                      owedChecks: existing.endedWatch.owedChecks,
                    }),
                    ...(existing.endedWatch?.awaitingApproval !== undefined && {
                      awaitingApproval: existing.endedWatch.awaitingApproval,
                    }),
                    ...(existing.endedWatch?.approvalAnnounced !== undefined && {
                      approvalAnnounced: existing.endedWatch.approvalAnnounced,
                    }),
                    // A finished-checks answer may be an older head's; a fresh read decides.
                    ...(existing.endedWatch?.checksPending === true && { checksPending: true }),
                    ...(existing.endedWatch?.checksAwaitedSince !== undefined && {
                      checksAwaitedSince: existing.endedWatch.checksAwaitedSince,
                    }),
                    remarksThrough: startedAt,
                    remarkIds: [],
                    wakes: 0,
                  });
          if (watch === existing.watch && links === linked) return thread;
          const final =
            command.type === "thread.pull-request-watch.sync" && watch === undefined
              ? command.endedWatch
              : undefined;
          return {
            ...thread,
            pullRequests: links.map((link) =>
              link === existing
                ? withPullRequestWatch(final ? { ...link, watch: final } : link, watch)
                : link,
            ),
            updatedAt: command.type === "thread.pull-request.watch" ? now : thread.updatedAt,
          };
        }
        case "thread.pull-request.sync":
          return {
            ...thread,
            branchPullRequest: command.branchPullRequest,
            ...(command.linkedPullRequest === undefined
              ? {}
              : {
                  linkedPullRequest: command.linkedPullRequest,
                  pullRequests: [
                    ...threadPullRequestsOf(thread).filter(
                      (link) =>
                        !(
                          thread.linkedPullRequest &&
                          threadPullRequestKeysEqual(
                            link,
                            legacyThreadPullRequestKey(thread.linkedPullRequest),
                          )
                        ) &&
                        !(
                          command.linkedPullRequest &&
                          threadPullRequestKeysEqual(
                            link,
                            legacyThreadPullRequestKey(command.linkedPullRequest),
                          )
                        ),
                    ),
                    ...(command.linkedPullRequest
                      ? [legacyPullRequestLink(thread, command.linkedPullRequest, now)]
                      : []),
                  ],
                }),
            updatedAt: thread.updatedAt,
          };
        case "thread.title.regeneration.complete":
          return thread.titleRegeneration?.requestId === command.requestId
            ? {
                ...thread,
                ...(command.title === undefined ? {} : { title: command.title }),
                ...(command.outcome === undefined
                  ? {}
                  : {
                      lastTitleRegeneration: {
                        ...command.outcome,
                        requestId: command.requestId,
                        completedAt: now,
                      },
                    }),
                titleRegeneration: null,
                updatedAt: now,
              }
            : thread;
        case "thread.runtime-mode.set":
          return { ...thread, runtimeMode: command.runtimeMode, updatedAt: now };
        case "thread.interaction-mode.set":
          return { ...thread, interactionMode: command.interactionMode, updatedAt: now };
        case "thread.model-selection.set":
        case "provider.switch":
          return {
            ...thread,
            providerInstanceId: command.modelSelection.instanceId,
            modelSelection: command.modelSelection,
            updatedAt: now,
          };
      }
    })();
    if (command.type === "thread.pull-request-watch.checks-reopen") {
      if (updatedThread === thread) return;
      yield* emit(
        events,
        command,
      )({
        type: "thread.pull-request-synced",
        threadId: command.threadId,
        providerInstanceId: updatedThread.providerInstanceId,
        occurredAt: now,
        payload: updatedThread,
      });
      return;
    }
    if (
      command.type === "thread.pull-request-link.sync" ||
      command.type === "thread.pull-request-watch.sync"
    ) {
      if (updatedThread === thread) return;
      const link = threadPullRequestsOf(updatedThread).find((candidate) =>
        threadPullRequestKeysEqual(candidate, normalizeThreadPullRequestKey(command)),
      );
      if (link === undefined) return;
      yield* emit(
        events,
        command,
      )({
        type: "thread.pull-request-link-updated",
        threadId: command.threadId,
        providerInstanceId: updatedThread.providerInstanceId,
        occurredAt: now,
        payload: link,
      });
      return;
    }
    const eventType = (() => {
      switch (command.type) {
        case "thread.archive":
          return "thread.archived" as const;
        case "thread.unarchive":
          return "thread.unarchived" as const;
        case "thread.settle":
          return "thread.settled" as const;
        case "thread.unsettle":
          return "thread.unsettled" as const;
        case "thread.snooze":
          return "thread.snoozed" as const;
        case "thread.unsnooze":
          return "thread.unsnoozed" as const;
        case "thread.auto-settle.set":
          return "thread.auto-settle-set" as const;
        case "thread.pin":
          return "thread.pinned" as const;
        case "thread.unpin":
          return "thread.unpinned" as const;
        case "thread.pin.reorder":
          return "thread.pin-reordered" as const;
        case "thread.active.reorder":
          return "thread.active-reordered" as const;
        case "thread.tag.set":
          return "thread.tags-updated" as const;
        case "thread.mark-unread":
          return "thread.marked-unread" as const;
        case "thread.metadata.update":
        case "thread.title.regeneration.complete":
          return "thread.metadata-updated" as const;
        case "thread.pull-request.link":
        case "thread.pull-request.unlink":
        case "thread.pull-request.watch":
        case "thread.pull-request.sync":
          return "thread.pull-request-synced" as const;
        case "thread.runtime-mode.set":
          return "thread.runtime-mode-updated" as const;
        case "thread.interaction-mode.set":
          return "thread.interaction-mode-updated" as const;
        case "thread.model-selection.set":
          return thread.providerInstanceId === command.modelSelection.instanceId
            ? ("thread.model-selection-updated" as const)
            : ("thread.provider-switched" as const);
        case "provider.switch":
          return "thread.provider-switched" as const;
      }
    })();
    yield* emit(
      events,
      command,
    )({
      type: eventType,
      threadId: command.threadId,
      providerInstanceId: updatedThread.providerInstanceId,
      occurredAt: now,
      payload: updatedThread,
    });
    // Archiving ends the guest share, so its waiting files go too.
    yield* releaseGuestAttachments(command, thread.guestShare, updatedThread.guestShare, effects);

    if (
      command.type === "thread.model-selection.set" &&
      providerSwitchPlan !== null &&
      !providerSwitchPlan.instanceChanged &&
      providerSwitchPlan.releaseProviderSessionIds.length === 0
    ) {
      const { providerThread, providerTurn } = yield* projectionStore
        .getRunningTurnContext(command.threadId)
        .pipe(mapDispatchError(command));
      const providerSessionId = providerThread?.providerSessionId;
      if (providerThread !== undefined && providerTurn !== undefined && providerSessionId) {
        const session = yield* providerSessions.get(providerSessionId).pipe(
          Effect.mapError(
            (cause) =>
              new OrchestratorDispatchError({
                commandId: command.commandId,
                commandType: command.type,
                cause,
              }),
          ),
        );
        if (Option.isSome(session) && session.value.applyModelSelection !== undefined) {
          yield* Ref.update(effects, (existing) => [
            ...existing,
            {
              id: `effect:${command.commandId}:provider-session.apply-model-selection`,
              commandId: command.commandId,
              threadId: command.threadId,
              request: {
                type: "provider-session.apply-model-selection",
                providerSessionId,
                providerThreadId: providerThread.id,
                modelSelection: command.modelSelection,
              },
            } satisfies PendingOrchestrationEffectV2,
          ]);
        }
      }
    }

    // Queued even while no session is live: it runs after any in-flight turn start publishes one.
    if (
      (command.type === "thread.metadata.update" ||
        command.type === "thread.title.regeneration.complete") &&
      updatedThread.title !== thread.title
    ) {
      yield* Ref.update(effects, (existing) => [
        ...existing,
        {
          id: `effect:${command.commandId}:provider-session.apply-thread-title`,
          commandId: command.commandId,
          threadId: command.threadId,
          request: { type: "provider-session.apply-thread-title", title: updatedThread.title },
        } satisfies PendingOrchestrationEffectV2,
      ]);
    }

    if (command.type === "thread.metadata.update" && command.regenerateTitle === true) {
      yield* Ref.update(effects, (existing) => [
        ...existing,
        pendingThreadTitleGenerationEffect(command.commandId, command.threadId, {
          type: "regenerate",
        }),
      ]);
    }

    if (command.type === "thread.archive") {
      const projection = yield* loadProjectionForCommand(
        command,
        ["runs", "attempts", "nodes", "subagents", "messages"],
        { turnItemTypes: [], messageRoles: ["user"] },
      );
      const emitEvent = emit(events, command);
      const activeRunIds = new Set(
        projection.runs
          .filter((run) => run.status === "queued" || run.status === "preparing")
          .map((run) => run.id),
      );
      const preparingRuns = projection.runs.filter((run) => run.status === "preparing");
      const preparationItems =
        preparingRuns.length === 0
          ? []
          : (yield* loadProjectionForCommand(command, ["turnItems"], {
              turnItemTypes: ["command_execution"],
              turnItemRunIds: preparingRuns.map((run) => run.id),
            })).turnItems;
      for (const run of projection.runs.filter((candidate) => activeRunIds.has(candidate.id))) {
        yield* emitEvent({
          type: "run.updated",
          threadId: command.threadId,
          runId: run.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...run, status: "cancelled", queuePosition: null, completedAt: now },
        });
      }
      for (const run of preparingRuns) {
        const preparationItem = preparationItems.find(
          (item) =>
            item.runId === run.id &&
            item.type === "command_execution" &&
            item.input === WORKSPACE_PREPARATION_INPUT &&
            item.status === "running",
        );
        if (preparationItem === undefined) continue;
        yield* emitEvent({
          type: "turn-item.updated",
          threadId: command.threadId,
          runId: run.id,
          nodeId: preparationItem.nodeId ?? undefined,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: {
            ...preparationItem,
            status: "cancelled",
            title: "Workspace preparation cancelled",
            completedAt: now,
            updatedAt: now,
          },
        });
      }
      for (const attempt of projection.attempts.filter(
        (candidate) =>
          activeRunIds.has(candidate.runId) &&
          (candidate.status === "pending" || candidate.status === "running"),
      )) {
        const run = projection.runs.find((candidate) => candidate.id === attempt.runId)!;
        yield* emitEvent({
          type: "run-attempt.updated",
          threadId: command.threadId,
          runId: attempt.runId,
          nodeId: attempt.rootNodeId,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...attempt, status: "cancelled", completedAt: now },
        });
      }
      for (const node of projection.nodes.filter(
        (candidate) =>
          candidate.runId !== null &&
          activeRunIds.has(candidate.runId) &&
          ["pending", "running", "waiting"].includes(candidate.status),
      )) {
        const run = projection.runs.find((candidate) => candidate.id === node.runId)!;
        yield* emitEvent({
          type: "node.updated",
          threadId: command.threadId,
          runId: run.id,
          nodeId: node.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...node, status: "cancelled", completedAt: now },
        });
      }
      yield* disposeAllDelegatedCompletionCohorts({
        command,
        events,
        projection: yield* getProjectionWithPendingEvents(command.threadId, events),
        now,
        cancelQueuedDelivery: false,
      });
      yield* closePendingLinkRequests({ command, events, threadId: command.threadId, now });
    }

    if (liveSessions.length > 0) {
      yield* Effect.forEach(
        liveSessions,
        (session) =>
          Effect.gen(function* () {
            yield* emit(
              events,
              command,
            )({
              type: "provider-session.detached",
              threadId: command.threadId,
              driver: session.driver,
              providerInstanceId: session.providerInstanceId,
              occurredAt: now,
              payload: {
                providerSessionId: session.id,
                detachedAt: now,
                reason:
                  command.type === "thread.archive"
                    ? "Thread archived."
                    : command.type === "thread.settle"
                      ? "Thread settled."
                      : command.type === "thread.metadata.update"
                        ? "Workspace changed."
                        : command.type === "thread.runtime-mode.set"
                          ? "Runtime mode changed."
                          : "Provider or model selection changed.",
              },
            });
            const pendingEffect = {
              id: `effect:${command.commandId}:provider-session.detach:${session.id}`,
              commandId: command.commandId,
              threadId: command.threadId,
              request: {
                type: "provider-session.detach",
                providerSessionId: session.id,
                detail:
                  command.type === "thread.archive"
                    ? "Thread archived."
                    : command.type === "thread.settle"
                      ? "Thread settled."
                      : command.type === "thread.metadata.update"
                        ? "Workspace changed."
                        : command.type === "thread.runtime-mode.set"
                          ? "Runtime mode changed."
                          : "Provider or model selection changed.",
                // Terminal detaches revoke MCP credentials; reattachments need other detach reasons to preserve them.
                ...(command.type === "thread.archive" ? { revokeMcpCredential: true } : {}),
              },
            } satisfies PendingOrchestrationEffectV2;
            yield* Ref.update(effects, (existing) => [...existing, pendingEffect]);
          }),
        { concurrency: 1, discard: true },
      );
    }

    if (command.type === "thread.archive") {
      yield* Ref.update(effects, (existing) => [
        ...existing,
        {
          id: `effect:${command.commandId}:terminal.cleanup`,
          commandId: command.commandId,
          threadId: command.threadId,
          request: { type: "terminal.cleanup" },
        } satisfies PendingOrchestrationEffectV2,
      ]);
    }
  });

  const dispatchProviderSessionDetach = Effect.fn("orchestrationV2.dispatch.providerSessionDetach")(
    function* (
      command: Extract<OrchestrationV2Command, { readonly type: "provider-session.detach" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    ) {
      const projection = yield* projectionStore
        .getThreadRecords(command.threadId, ["providerSessions"])
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
          ),
        );
      const session = projection.providerSessions.find(
        (candidate) => candidate.id === command.providerSessionId,
      );
      if (session === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Provider session ${command.providerSessionId} does not belong to thread ${command.threadId}.`,
        });
      }
      const now = yield* DateTime.now;
      yield* emit(
        events,
        command,
      )({
        type: "provider-session.detached",
        threadId: command.threadId,
        driver: session.driver,
        providerInstanceId: session.providerInstanceId,
        occurredAt: now,
        payload: {
          providerSessionId: session.id,
          detachedAt: now,
          ...(command.reason === undefined ? {} : { reason: command.reason }),
        },
      });
      const pendingEffect = {
        id: `effect:${command.commandId}:provider-session.detach:${command.providerSessionId}`,
        commandId: command.commandId,
        threadId: command.threadId,
        request: {
          type: "provider-session.detach",
          providerSessionId: command.providerSessionId,
          ...(command.reason === undefined ? {} : { detail: command.reason }),
        },
      } satisfies PendingOrchestrationEffectV2;
      yield* Ref.update(effects, (existing) => [...existing, pendingEffect]);
    },
  );

  const dispatchThreadProjectAdoptFolder = Effect.fn(
    "orchestrationV2.dispatch.threadProjectAdoptFolder",
  )(function* (
    command: Extract<
      OrchestrationV2ServerCommand,
      { readonly type: "thread.project.adopt-folder" }
    >,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const reject = (cause: string) =>
      new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} ${cause}`,
      });
    const projection = yield* projectionStore
      .getThreadRecords(command.threadId, ["runs", "runtimeRequests"], { turnItemTypes: [] })
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
        ),
      );
    const thread = projection.thread;
    if (thread.deletedAt !== null) return yield* reject("is deleted.");
    if (threadHasUnfinishedWork(projection)) {
      return yield* reject("still has work in progress. Finish or stop it first.");
    }
    const current = yield* projects.get(thread.projectId).pipe(mapDispatchError(command));
    const target = yield* projects.get(command.projectId).pipe(mapDispatchError(command));
    if (Option.isNone(target) || target.value.deletedAt !== null) {
      return yield* reject(`cannot move to missing project ${command.projectId}.`);
    }
    // The thread always lands in the project root, so the move must not change its cwd.
    const cwd = thread.worktreePath ?? Option.getOrUndefined(current)?.workspaceRoot;
    if (cwd !== target.value.workspaceRoot) {
      return yield* reject(`does not work in ${target.value.workspaceRoot}.`);
    }
    const now = yield* DateTime.now;
    yield* emit(
      events,
      command,
    )({
      type: "thread.metadata-updated",
      threadId: command.threadId,
      providerInstanceId: thread.providerInstanceId,
      occurredAt: now,
      payload: { ...thread, projectId: command.projectId, worktreePath: null, updatedAt: now },
    });
  });

  const refuseAboveDispatchModeLimit = Effect.fn("orchestrationV2.dispatch.refuseAboveModeLimit")(
    function* (
      command: OrchestrationV2ServerCommand,
      threadId: ThreadId,
      modes: {
        readonly runtimeMode: RuntimeMode;
        readonly interactionMode: ProviderInteractionMode;
      },
    ) {
      const limit = narrowToCeiling(yield* DispatchModeLimit, runtimeModeCeiling);
      const exceeded = limit === undefined ? undefined : exceededDispatchModeLimit(limit, modes);
      if (limit === undefined || exceeded === undefined) return;
      const refusal = {
        threadId,
        mode: exceeded,
        runtimeMode: modes.runtimeMode,
        interactionMode: modes.interactionMode,
      };
      if (limit.refused !== undefined) yield* Ref.set(limit.refused, refusal);
      return yield* new OrchestratorThreadAboveModeLimitError({
        commandId: command.commandId,
        ...refusal,
      });
    },
  );

  const refuseAboveRuntimeModeCeiling = Effect.fn(
    "orchestrationV2.dispatch.refuseAboveRuntimeModeCeiling",
  )(function* (
    command: OrchestrationV2ServerCommand,
    events: ReadonlyArray<OrchestrationV2DomainEvent>,
  ) {
    if (runtimeModeCeiling === undefined) return;
    const runtimeModes = new Map<ThreadId, RuntimeMode>();
    for (const threadId of threadsStartingWork(events)) {
      const thread = yield* projectionStore.getThread(threadId).pipe(
        Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
        Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
      );
      if (thread !== null) runtimeModes.set(threadId, thread.runtimeMode);
    }
    const refusal = runtimeModeCeilingRefusal(
      runtimeModeCeiling.maxRuntimeMode,
      events,
      runtimeModes,
    );
    if (refusal === undefined) return;
    return yield* new OrchestratorRuntimeModeAboveCeilingError({
      commandId: command.commandId,
      ...refusal,
      ...runtimeModeCeiling,
    });
  });

  const dispatchThreadFork = Effect.fn("orchestrationV2.dispatch.threadFork")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "thread.fork" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    yield* Effect.annotateCurrentSpan({
      "orchestration_v2.command_id": command.commandId,
      "orchestration_v2.command_type": command.type,
      "orchestration_v2.source_thread_id": command.sourceThreadId,
      "orchestration_v2.target_thread_id": command.targetThreadId,
      "orchestration_v2.source_point_type": command.sourcePoint.type,
    });

    const sourceProjection = yield* projectionStore
      .getThreadRecords(command.sourceThreadId, [
        "runs",
        ...(command.sourcePoint.type === "checkpoint" ? (["checkpoints"] as const) : []),
        "providerThreads",
        "providerTurns",
        "attempts",
      ])
      .pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorProjectionError({
              threadId: command.sourceThreadId,
              cause,
            }),
        ),
      );
    // Read under the source's lock; validate the modes copied here, not an earlier read.
    yield* refuseAboveDispatchModeLimit(command, command.sourceThreadId, sourceProjection.thread);
    if (sourceProjection.thread.deletedAt !== null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.sourceThreadId} was deleted.`,
      });
    }
    // The fork would copy no worktree path and run in the project checkout.
    if (
      command.sideChat === true &&
      orchestrationV2WorktreePending(sourceProjection.thread.worktreePath, sourceProjection.runs)
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.sourceThreadId} has no worktree yet. Start the side chat once its worktree is set up.`,
      });
    }
    if ((yield* findThread(command.targetThreadId)) !== null) {
      return yield* new OrchestratorThreadAlreadyExistsError({
        commandId: command.commandId,
        threadId: command.targetThreadId,
      });
    }

    const sourceRun = runForSourcePoint(sourceProjection, command.sourcePoint);

    // A side chat may open during the parent's first turn; it starts from a digest instead.
    if (
      sourceRun === null &&
      !(command.sideChat === true && command.sourcePoint.type === "latest_stable")
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `No stable source run was found for fork source ${command.sourcePoint.type}.`,
      });
    }
    if (sourceRun !== null && !isForkableSourceRunStatus(sourceRun.status)) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: forkableSourceRunStatusError(sourceRun),
      });
    }
    const sourceProviderThread =
      sourceRun === null ? undefined : providerThreadForRun(sourceProjection, sourceRun);
    const now = command.createdAt ?? (yield* DateTime.now);
    const emitEvent = emit(events, command);
    const transferId = yield* mapDispatchError(command)(
      idAllocator.allocate.contextTransfer({
        sourceThreadId: sourceProjection.thread.id,
        targetThreadId: command.targetThreadId,
        type: "fork",
      }),
    );
    const { targetThread, transfer } = yield* threadForkService
      .plan({
        sourceProjection,
        sourceRun,
        sourceProviderThread,
        sourceTokenUsage:
          sourceRun === null
            ? undefined
            : providerTurnForRun(sourceProjection, sourceRun)?.tokenUsage,
        canonicalSourcePoint:
          sourceRun === null
            ? { threadId: sourceProjection.thread.id }
            : contextSourcePointForRun(sourceProjection, sourceRun),
        transferId,
        targetThreadId: command.targetThreadId,
        ...(command.title === undefined ? {} : { title: command.title }),
        ...(command.sideChat ? { sideChat: true } : {}),
        createdBy: command.createdBy,
        creationSource: command.creationSource,
        createdAt: now,
      })
      .pipe(mapDispatchError(command));

    yield* emitEvent({
      type: "thread.created",
      threadId: command.targetThreadId,
      providerInstanceId: targetThread.providerInstanceId,
      occurredAt: now,
      payload: targetThread,
    });
    yield* emitEvent({
      type: "context-transfer.created",
      threadId: command.targetThreadId,
      providerInstanceId: transfer.sourceProviderInstanceId ?? targetThread.providerInstanceId,
      occurredAt: now,
      payload: transfer,
    });
  });

  const dispatchThreadMergeBack = Effect.fn("orchestrationV2.dispatch.threadMergeBack")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "thread.merge_back" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    yield* Effect.annotateCurrentSpan({
      "orchestration_v2.command_id": command.commandId,
      "orchestration_v2.command_type": command.type,
      "orchestration_v2.source_thread_id": command.sourceThreadId,
      "orchestration_v2.target_thread_id": command.targetThreadId,
      "orchestration_v2.source_point_type": command.sourcePoint.type,
    });

    const sourceProjection = yield* projectionStore
      .getThreadRecords(command.sourceThreadId, [
        "runs",
        "checkpoints",
        "providerThreads",
        "providerTurns",
        "attempts",
        "contextTransfers",
      ])
      .pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorProjectionError({
              threadId: command.sourceThreadId,
              cause,
            }),
        ),
      );
    // The source is unlocked; validate the modes copied here, not an earlier read.
    yield* refuseAboveDispatchModeLimit(command, command.sourceThreadId, sourceProjection.thread);
    const targetProjection = yield* projectionStore
      .getThreadRecords(command.targetThreadId, ["runs", "contextTransfers"])
      .pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorProjectionError({
              threadId: command.targetThreadId,
              cause,
            }),
        ),
      );

    if (
      sourceProjection.thread.lineage.relationshipToParent !== "fork" ||
      sourceProjection.thread.lineage.parentThreadId !== command.targetThreadId
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.sourceThreadId} is not a fork of ${command.targetThreadId}.`,
      });
    }
    // Queued runs cannot consume merge-back context. Refuse under the target's
    // command lock before a pending transfer can block its ordinary messages.
    if (targetProjection.runs.some((run) => isActiveRun(run) || run.status === "queued")) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause:
          "Finish or stop the target thread's active work and clear its queue before merging back.",
      });
    }
    const otherPendingMergeBack = targetProjection.contextTransfers.some(
      (transfer) =>
        transfer.type === "merge_back" &&
        transfer.status === "pending" &&
        transfer.targetThreadId === command.targetThreadId &&
        transfer.sourceThreadId !== command.sourceThreadId,
    );
    if (otherPendingMergeBack) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.targetThreadId} already has pending context from another side chat or fork. Send a message there first.`,
      });
    }

    const sourceRun = runForSourcePoint(sourceProjection, command.sourcePoint);
    if (sourceRun === null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `No stable source run was found for merge-back source ${command.sourcePoint.type}.`,
      });
    }
    if (sourceRun.status !== "completed" && sourceRun.status !== "waiting") {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Merge-back source run ${sourceRun.id} is ${sourceRun.status}; only provider-finished runs are supported.`,
      });
    }

    const forkTransfer = sourceProjection.contextTransfers.findLast(
      (transfer) =>
        transfer.type === "fork" &&
        transfer.sourceThreadId === command.targetThreadId &&
        transfer.targetThreadId === command.sourceThreadId,
    );
    if (forkTransfer === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `No fork transfer exists between ${command.targetThreadId} and ${command.sourceThreadId}.`,
      });
    }

    const sourceProviderThread = providerThreadForRun(sourceProjection, sourceRun);
    const now = command.createdAt ?? (yield* DateTime.now);
    const emitEvent = emit(events, command);
    const transferId = yield* mapDispatchError(command)(
      idAllocator.allocate.contextTransfer({
        sourceThreadId: command.sourceThreadId,
        targetThreadId: command.targetThreadId,
        type: "merge_back",
      }),
    );
    const pendingMergeBackTransfersForPair = targetProjection.contextTransfers.filter(
      (transfer) =>
        transfer.type === "merge_back" &&
        transfer.status === "pending" &&
        transfer.sourceThreadId === command.sourceThreadId &&
        transfer.targetThreadId === command.targetThreadId,
    );
    const transfer: OrchestrationV2ContextTransfer = {
      id: transferId,
      type: "merge_back",
      sourceThreadId: command.sourceThreadId,
      targetThreadId: command.targetThreadId,
      sourcePoint: contextSourcePointForRun(sourceProjection, sourceRun),
      basePoint: forkTransfer.sourcePoint,
      sourceProviderInstanceId: sourceRun.providerInstanceId,
      targetProviderInstanceId: targetProjection.thread.modelSelection.instanceId,
      targetRunId: null,
      status: "pending",
      resolution: null,
      createdBy: command.createdBy,
      error:
        sourceProviderThread === undefined ? "Source merge-back run has no provider thread." : null,
      createdAt: now,
      updatedAt: now,
      consumedAt: null,
    };

    for (const pendingTransfer of pendingMergeBackTransfersForPair) {
      yield* emitEvent({
        type: "context-transfer.updated",
        threadId: command.targetThreadId,
        providerInstanceId: sourceRun.providerInstanceId,
        occurredAt: now,
        payload: {
          ...pendingTransfer,
          status: "superseded",
          error: `Superseded by merge-back transfer ${transferId}.`,
          updatedAt: now,
        },
      });
    }
    yield* emitEvent({
      type: "context-transfer.created",
      threadId: command.targetThreadId,
      providerInstanceId: sourceRun.providerInstanceId,
      occurredAt: now,
      payload: transfer,
    });
  });

  const ensureSteeringMessage = Effect.fn("Orchestrator.ensureSteeringMessage")(function* (
    command: Extract<
      OrchestrationV2Command,
      { readonly type: "message.dispatch" | "queued-message.promote-to-steer" }
    >,
    message: Pick<OrchestrationV2ConversationMessage, "text" | "attachments" | "context">,
  ) {
    if (isNativeMaintenanceCommand(message)) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause:
          message.text.trim().toLowerCase() === "/compact"
            ? "Context compaction must run as a separate turn. Queue it or wait for the active turn to finish."
            : "Signing out must run as a separate turn. Queue it or wait for the active turn to finish.",
      });
    }
    if (parseGoalCommand(message) !== null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause:
          "Goal commands must run as a separate turn. Queue it or wait for the active turn to finish.",
      });
    }
  });

  const dispatchSteerIntoRun = Effect.fn("Orchestrator.dispatchSteerIntoRun")(function* (input: {
    readonly command: Extract<
      OrchestrationV2Command,
      { readonly type: "message.dispatch" | "queued-message.promote-to-steer" }
    >;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>;
    readonly projection: Pick<
      OrchestrationV2ThreadProjection,
      | "runs"
      | "messages"
      | "providerSessions"
      | "providerThreads"
      | "providerTurns"
      | "subagents"
      | "attempts"
      | "nodes"
      | "thread"
      | "turnItems"
      | "checkpointScopes"
    >;
    readonly modelSelection: ModelSelection;
    readonly targetRunId: OrchestrationV2Run["id"];
    readonly messageId: OrchestrationV2ConversationMessage["id"];
    readonly text: string;
    readonly attachments: ReadonlyArray<ChatAttachment>;
    readonly context?: import("@t3tools/contracts").OrchestrationMessageContext | undefined;
    readonly createdBy: OrchestrationV2ConversationMessage["createdBy"];
    readonly creationSource: OrchestrationV2ConversationMessage["creationSource"];
    readonly scheduledTaskId?: OrchestrationV2ConversationMessage["scheduledTaskId"];
    readonly senderThreadId?: OrchestrationV2ConversationMessage["senderThreadId"];
    readonly linkedSender?: OrchestrationV2ConversationMessage["linkedSender"];
    readonly idleCompaction?: OrchestrationV2ConversationMessage["idleCompaction"];
    readonly delegatedCompletion?: OrchestrationV2ConversationMessage["delegatedCompletion"];
    readonly notification?: OrchestrationV2ConversationMessage["notification"];
    readonly forceRestart: boolean;
    readonly history?: MessageDispatchHistory;
    /** Automatic Send now: its turn items mark the steer, which paces the next one. */
    readonly automaticDelivery?: boolean;
    /** Further queued messages that join this steer, in order. */
    readonly mergedMessages?: ReadonlyArray<MergedSteerMessage>;
  }) {
    const mergedMessages = input.mergedMessages ?? [];
    const queueIfSessionReleased =
      input.command.type === "message.dispatch" && input.command.deliveryIntent === "auto";
    const history = input.history ?? projectedHistory;
    const targetRun = input.projection.runs.find((candidate) => candidate.id === input.targetRunId);
    if (targetRun === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: `Target run ${input.targetRunId} was not found.`,
      });
    }
    for (const message of [input, ...mergedMessages]) {
      yield* ensureSteeringMessage(input.command, message);
    }
    const targetMessage = input.projection.messages.find(
      (message) => message.id === targetRun.userMessageId,
    );
    if (targetMessage !== undefined && isNativeMaintenanceCommand(targetMessage)) {
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause:
          targetMessage.text.trim().toLowerCase() === "/compact"
            ? "Wait for context compaction to finish before steering the thread."
            : "Wait for sign-out to finish before steering the thread.",
      });
    }
    const rootNodeId = targetRun.rootNodeId;
    if (rootNodeId === null) {
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: `Target run ${targetRun.id} has no root node.`,
      });
    }
    if (targetRun.status !== "running" && targetRun.status !== "waiting") {
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: `Target run ${targetRun.id} is ${targetRun.status} and cannot be steered.`,
      });
    }
    // The run reads as running until its provider stops, but a steer accepted
    // after Stop never reaches the agent.
    if (yield* stopReachedRun(input.command, input.command.threadId, targetRun.id)) {
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: `Target run ${targetRun.id} is stopping and cannot be steered.`,
      });
    }
    const providerThread = input.projection.providerThreads.find(
      (candidate) => candidate.id === targetRun.providerThreadId,
    );
    if (providerThread === undefined || providerThread.providerSessionId === null) {
      if (queueIfSessionReleased) return "queue" as const;
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: `Provider thread ${targetRun.providerThreadId} has no active provider session for steering.`,
      });
    }
    const providerSessionId = providerThread.providerSessionId;
    const providerTurn = input.projection.providerTurns.find(
      (candidate) =>
        candidate.runAttemptId === targetRun.activeAttemptId && candidate.status === "running",
    );
    if (providerTurn === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: `No running provider turn found for active run ${targetRun.id}.`,
      });
    }
    const sessionOption = yield* providerSessions.get(providerSessionId).pipe(
      Effect.mapError(
        (cause) =>
          new OrchestratorDispatchError({
            commandId: input.command.commandId,
            commandType: input.command.type,
            cause,
          }),
      ),
    );
    if (Option.isNone(sessionOption)) {
      if (queueIfSessionReleased) return "queue" as const;
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: `Provider session ${providerThread.providerSessionId} is not active.`,
      });
    }

    const session = sessionOption.value;
    const now = yield* DateTime.now;
    const emitEvent = emit(input.events, input.command);
    const selectionChanged = !modelSelectionsEqual(targetRun.modelSelection, input.modelSelection);
    const providerInstanceChanged =
      targetRun.providerInstanceId !== input.modelSelection.instanceId;
    const selectionTransition =
      selectionChanged && !providerInstanceChanged
        ? yield* providerAdapters.get(targetRun.providerInstanceId).pipe(
            Effect.flatMap((adapter) =>
              adapter.planSelectionTransition({
                current: targetRun.modelSelection,
                target: input.modelSelection,
                sessionCapabilities: session.providerSession.capabilities,
              }),
            ),
            Effect.mapError(
              (cause) =>
                new OrchestratorProviderAdapterError({
                  commandId: input.command.commandId,
                  providerInstanceId: targetRun.providerInstanceId,
                  cause,
                }),
            ),
          )
        : null;
    if (selectionTransition?.type === "reject") {
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: selectionTransition.reason,
      });
    }
    const appendSteeringMessage = Effect.fn("Orchestrator.appendSteeringMessage")(
      function* (messageInput: {
        readonly runId: OrchestrationV2Run["id"];
        readonly nodeId: OrchestrationV2ExecutionNode["id"];
        readonly providerTurnId: typeof providerTurn.id | null;
        readonly providerThreadId: OrchestrationV2ProviderThread["id"];
        readonly providerInstanceId: ProviderInstanceId;
        readonly modelSwitch?: OrchestrationV2ModelSwitch | undefined;
        readonly merged?: { readonly message: MergedSteerMessage; readonly ordinal: number };
      }) {
        const fields: Omit<MergedSteerMessage, "messageId"> & {
          readonly messageId: OrchestrationV2ConversationMessage["id"];
          readonly delegatedCompletion?: OrchestrationV2ConversationMessage["delegatedCompletion"];
          readonly notification?: OrchestrationV2ConversationMessage["notification"];
        } = messageInput.merged?.message ?? input;
        const message: OrchestrationV2ConversationMessage = {
          createdBy: fields.createdBy,
          creationSource: fields.creationSource,
          ...(fields.delegatedCompletion === undefined
            ? {}
            : { delegatedCompletion: fields.delegatedCompletion }),
          ...(fields.notification === undefined ? {} : { notification: fields.notification }),
          ...(fields.scheduledTaskId === undefined
            ? {}
            : { scheduledTaskId: fields.scheduledTaskId }),
          ...(fields.senderThreadId === undefined ? {} : { senderThreadId: fields.senderThreadId }),
          ...(fields.linkedSender === undefined ? {} : { linkedSender: fields.linkedSender }),
          ...(fields.idleCompaction === undefined ? {} : { idleCompaction: fields.idleCompaction }),
          id: fields.messageId,
          threadId: input.command.threadId,
          runId: messageInput.runId,
          nodeId: messageInput.nodeId,
          role: "user",
          text: fields.text,
          attachments: fields.attachments,
          ...(fields.context ? { context: fields.context } : {}),
          streaming: false,
          createdAt: now,
          updatedAt: now,
        };
        const turnItem: OrchestrationV2TurnItem = {
          createdBy: fields.createdBy,
          creationSource: fields.creationSource,
          ...(fields.scheduledTaskId === undefined
            ? {}
            : { scheduledTaskId: fields.scheduledTaskId }),
          ...(fields.senderThreadId === undefined ? {} : { senderThreadId: fields.senderThreadId }),
          ...(fields.linkedSender === undefined ? {} : { linkedSender: fields.linkedSender }),
          ...(fields.idleCompaction === undefined ? {} : { idleCompaction: fields.idleCompaction }),
          id: idAllocator.derive.userTurnItem({ messageId: fields.messageId }),
          threadId: input.command.threadId,
          runId: messageInput.runId,
          nodeId: messageInput.nodeId,
          providerThreadId: messageInput.providerThreadId,
          providerTurnId: messageInput.providerTurnId,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: messageInput.merged?.ordinal ?? (yield* nextTurnItemOrdinal(input.projection)),
          status: "completed",
          title: null,
          startedAt: now,
          completedAt: now,
          updatedAt: now,
          type: "user_message",
          messageId: fields.messageId,
          inputIntent:
            input.command.type === "queued-message.promote-to-steer"
              ? "promoted_queued_to_steer"
              : "steer",
          ...(input.automaticDelivery === true ? { automaticDelivery: true as const } : {}),
          text: fields.text,
          attachments: fields.attachments,
          ...(fields.context ? { context: fields.context } : {}),
          ...(messageInput.modelSwitch === undefined
            ? {}
            : { modelSwitch: messageInput.modelSwitch }),
        };
        yield* emitEvent({
          type: "message.updated",
          threadId: input.command.threadId,
          runId: messageInput.runId,
          nodeId: messageInput.nodeId,
          providerInstanceId: messageInput.providerInstanceId,
          occurredAt: now,
          payload: message,
        });
        yield* emitEvent({
          type: "turn-item.updated",
          threadId: input.command.threadId,
          runId: messageInput.runId,
          nodeId: messageInput.nodeId,
          providerInstanceId: messageInput.providerInstanceId,
          occurredAt: now,
          payload: notificationTurnItem(
            turnItem,
            message,
            yield* notificationTasks(input.command.threadId, message, input.projection.subagents),
          ),
        });
      },
    );

    // A selection the provider applies on its next turn joins the running
    // turn as a steer and waits for the next turn. Restarting the turn for it
    // would end the turn's native subagents, which a steer must never do.
    const selectionMustApplyNow =
      selectionChanged &&
      (providerInstanceChanged || selectionTransition?.type !== "apply_on_next_turn");
    const attachmentsNeedRestart =
      [input, ...mergedMessages].some((message) => message.attachments.length > 0) &&
      session.providerSession.capabilities.turns.activeSteeringAcceptsAttachments === false;
    const steeringPolicy = yield* mapDispatchError(input.command)(
      commandPolicy.decideSteeringExecution({
        commandId: input.command.commandId,
        threadId: input.command.threadId,
        providerInstanceId: targetRun.providerInstanceId,
        capabilities: session.providerSession.capabilities,
        forceRestart: input.forceRestart || selectionMustApplyNow || attachmentsNeedRestart,
      }),
    );

    if (steeringPolicy === "active_steering") {
      const instanceChanged =
        input.projection.thread.providerInstanceId !== input.modelSelection.instanceId;
      // Only a user's steer carries the picker's selection; the adapter applies it to the live turn.
      const steerCarriesSelection =
        input.delegatedCompletion === undefined && input.notification === undefined;
      if (
        steerCarriesSelection &&
        (instanceChanged ||
          !modelSelectionsEqual(input.projection.thread.modelSelection, input.modelSelection))
      ) {
        yield* emitEvent({
          type: instanceChanged ? "thread.provider-switched" : "thread.model-selection-updated",
          threadId: input.command.threadId,
          providerInstanceId: input.modelSelection.instanceId,
          occurredAt: now,
          payload: {
            ...input.projection.thread,
            providerInstanceId: input.modelSelection.instanceId,
            modelSelection: input.modelSelection,
            updatedAt: now,
          },
        });
      }
      const steeringMessage = {
        runId: targetRun.id,
        nodeId: rootNodeId,
        providerTurnId: providerTurn.id,
        providerThreadId: providerThread.id,
        providerInstanceId: targetRun.providerInstanceId,
      };
      yield* appendSteeringMessage(steeringMessage);
      if (mergedMessages.length > 0) {
        const firstOrdinal = (yield* nextTurnItemOrdinal(input.projection)) + 1;
        for (const [index, message] of mergedMessages.entries()) {
          yield* appendSteeringMessage({
            ...steeringMessage,
            merged: { message, ordinal: firstOrdinal + index },
          });
        }
      }
      yield* Ref.update(input.effects, (existing) => [
        ...existing,
        {
          id: `effect:${input.command.commandId}:provider-turn.steer:${providerTurn.id}`,
          commandId: input.command.commandId,
          threadId: input.command.threadId,
          request: {
            type: "provider-turn.steer",
            durableDelivery: true,
            acceptDelegatedCompletion: input.delegatedCompletion !== undefined,
            ...(queueIfSessionReleased ? { deliveryIntent: "auto" as const } : {}),
            providerSessionId,
            providerThreadId: providerThread.id,
            providerTurnId: providerTurn.id,
            messageId: input.messageId,
            ...(mergedMessages.length === 0
              ? {}
              : { mergedMessageIds: mergedMessages.map((message) => message.messageId) }),
            ...(steerCarriesSelection ? { modelSelection: input.modelSelection } : {}),
            steerFollowUpAfterRunOrdinal: Math.max(
              0,
              ...input.projection.runs.map((run) => run.ordinal),
            ),
          },
        } satisfies PendingOrchestrationEffectV2,
      ]);
      return;
    }
    if (mergedMessages.length > 0) {
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: "Queued messages are merged only into a steer the provider takes without a restart.",
      });
    }

    const currentAttempt = input.projection.attempts.find(
      (candidate) => candidate.id === targetRun.activeAttemptId,
    );
    const currentRootNode = input.projection.nodes.find((candidate) => candidate.id === rootNodeId);
    const attemptOrdinal =
      Math.max(
        0,
        ...input.projection.attempts
          .filter((candidate) => candidate.runId === targetRun.id)
          .map((candidate) => candidate.attemptOrdinal),
      ) + 1;
    const nextAttemptId = idAllocator.derive.runAttempt({
      runId: targetRun.id,
      attemptOrdinal,
    });
    const nextRootNodeId = idAllocator.derive.rootNodeAttempt({
      runId: targetRun.id,
      attemptOrdinal,
    });
    let restartProviderThread = providerThread;
    let restartSessionTransition:
      | {
          readonly type: "replace";
          readonly replacementProviderSessionId: ProviderSessionId;
        }
      | { readonly type: "detach" }
      | null = null;
    let restartHandoff: OrchestrationV2ContextHandoff | null = null;
    let restartTransfer: OrchestrationV2ContextTransfer | null = null;
    const canResumeAcrossInstances =
      providerInstanceChanged &&
      providerThread.nativeThreadRef !== null &&
      (yield* providerSwitchService
        .plan({
          projection: {
            ...input.projection,
            thread: { ...input.projection.thread, modelSelection: targetRun.modelSelection },
          },
          targetModelSelection: input.modelSelection,
        })
        .pipe(mapDispatchError(input.command))).transition.type === "restart_and_resume";
    const requiresProviderThreadHandoff =
      (providerInstanceChanged && !canResumeAcrossInstances) ||
      selectionTransition?.type === "create_with_handoff";
    const requiresProviderSessionRestart =
      canResumeAcrossInstances || selectionTransition?.type === "restart_session";
    if (requiresProviderThreadHandoff) {
      const targetAdapter = yield* providerAdapters.get(input.modelSelection.instanceId).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorProviderAdapterError({
              commandId: input.command.commandId,
              providerInstanceId: input.modelSelection.instanceId,
              cause,
            }),
        ),
      );
      const targetCapabilities = yield* targetAdapter.getCapabilities().pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorProviderAdapterError({
              commandId: input.command.commandId,
              providerInstanceId: input.modelSelection.instanceId,
              cause,
            }),
        ),
      );
      yield* mapDispatchError(input.command)(
        commandPolicy.ensureContextHandoff({
          commandId: input.command.commandId,
          threadId: input.command.threadId,
          providerInstanceId: input.modelSelection.instanceId,
          capabilities: targetCapabilities,
          strategy: "full_thread_summary",
        }),
      );
      const existingTargetProviderThread = rootProviderThreadsForProvider(
        input.projection,
        input.modelSelection.instanceId,
      ).find((candidate) => candidate.id !== providerThread.id);
      const targetProviderSessionId =
        existingTargetProviderThread?.providerSessionId ??
        (yield* mapDispatchError(input.command)(
          providerSessionIdFor({
            adapter: targetAdapter,
            providerInstanceId: input.modelSelection.instanceId,
            threadId: input.command.threadId,
          }),
        ));
      const targetProviderThreadBase: OrchestrationV2ProviderThread =
        existingTargetProviderThread === undefined
          ? {
              id: idAllocator.derive.providerThread({
                driver: targetAdapter.driver,
                nativeThreadId: `pending:${targetRun.id}:attempt:${attemptOrdinal}`,
              }),
              driver: targetAdapter.driver,
              providerInstanceId: input.modelSelection.instanceId,
              providerSessionId: targetProviderSessionId,
              appThreadId: input.command.threadId,
              ownerNodeId: null,
              nativeThreadRef: null,
              nativeConversationHeadRef: null,
              status: "not_loaded",
              firstRunOrdinal: targetRun.ordinal,
              lastRunOrdinal: targetRun.ordinal,
              handoffIds: [],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            }
          : {
              ...existingTargetProviderThread,
              providerSessionId: targetProviderSessionId,
              lastRunOrdinal: targetRun.ordinal,
              updatedAt: now,
            };
      const transferId = yield* mapDispatchError(input.command)(
        idAllocator.allocate.contextTransfer({
          sourceThreadId: input.command.threadId,
          targetThreadId: input.command.threadId,
          type: "provider_handoff",
        }),
      );
      const contextBoundary = contextBoundaryOrdinal(input.projection);
      restartHandoff = yield* contextHandoffService
        .prepareProviderHandoff({
          threadId: input.command.threadId,
          targetRunId: targetRun.id,
          transferId,
          fromProviderThreadIds: [providerThread.id],
          toProviderThreadId: targetProviderThreadBase.id,
          fromProviderInstanceId: targetRun.providerInstanceId,
          toProviderInstanceId: input.modelSelection.instanceId,
          coveredRunOrdinals: { from: Math.max(1, contextBoundary), to: targetRun.ordinal },
          strategy: "full_thread_summary",
          items: yield* readHandoffItems(
            input.command.threadId,
            contextBoundary === 0
              ? undefined
              : (yield* history.runs(input.projection))
                  .filter((run) => run.ordinal >= contextBoundary)
                  .map((run) => run.id),
          ),
          runs: yield* history.runs(input.projection),
          createdAt: now,
        })
        .pipe(mapDispatchError(input.command));
      restartProviderThread = {
        ...targetProviderThreadBase,
        handoffIds: Array.from(
          new Set([...targetProviderThreadBase.handoffIds, restartHandoff.id]),
        ),
      };
      restartTransfer = {
        id: transferId,
        type: "provider_handoff",
        sourceThreadId: input.command.threadId,
        targetThreadId: input.command.threadId,
        sourcePoint: contextSourcePointForRun(input.projection, targetRun),
        basePoint: null,
        sourceProviderInstanceId: targetRun.providerInstanceId,
        targetProviderInstanceId: input.modelSelection.instanceId,
        targetRunId: targetRun.id,
        status: "consumed",
        resolution: {
          strategy: "portable_context",
          contextHandoffId: restartHandoff.id,
        },
        createdBy: input.createdBy,
        error: null,
        createdAt: now,
        updatedAt: now,
        consumedAt: now,
      };
      restartSessionTransition = { type: "detach" };
    } else if (requiresProviderSessionRestart) {
      const nextProviderSessionId = yield* mapDispatchError(input.command)(
        idAllocator.allocate.providerSession({
          providerInstanceId: input.modelSelection.instanceId,
          threadId: input.command.threadId,
        }),
      );
      restartProviderThread = {
        ...providerThread,
        providerInstanceId: input.modelSelection.instanceId,
        providerSessionId: nextProviderSessionId,
        status: "not_loaded",
        updatedAt: now,
      };
      restartSessionTransition = {
        type: "replace",
        replacementProviderSessionId: nextProviderSessionId,
      };
    }
    const resolvedRuntimePolicy = yield* runtimePolicy
      .resolve({ thread: input.projection.thread, modelSelection: input.modelSelection })
      .pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorDispatchError({
              commandId: input.command.commandId,
              commandType: input.command.type,
              cause,
            }),
        ),
      );
    const checkpointScope = yield* prepareRootRunScope({
      threadScopes: input.projection.checkpointScopes,
      threadId: input.command.threadId,
      runId: targetRun.id,
      rootNodeId: nextRootNodeId,
      providerThreadId: restartProviderThread.id,
      cwd:
        resolvedRuntimePolicy.cwd ??
        input.projection.thread.worktreePath ??
        session.providerSession.cwd,
      createdAt: now,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new OrchestratorDispatchError({
            commandId: input.command.commandId,
            commandType: input.command.type,
            cause,
          }),
      ),
    );
    const restartedRun: OrchestrationV2Run = {
      ...targetRun,
      providerInstanceId: input.modelSelection.instanceId,
      modelSelection: input.modelSelection,
      providerThreadId: restartProviderThread.id,
      rootNodeId: nextRootNodeId,
      activeAttemptId: nextAttemptId,
      userMessageId: input.messageId,
      status: "starting",
      contextHandoffId: restartHandoff?.id ?? targetRun.contextHandoffId,
      workspaceCwd: runWorkspaceOf(checkpointScope),
    };
    const nextAttempt: OrchestrationV2RunAttempt = {
      id: nextAttemptId,
      runId: targetRun.id,
      attemptOrdinal,
      rootNodeId: nextRootNodeId,
      providerInstanceId: input.modelSelection.instanceId,
      providerThreadId: restartProviderThread.id,
      providerTurnId: null,
      reason: "steering_restart",
      status: "pending",
      startedAt: null,
      completedAt: null,
    };
    const nextRootNode: OrchestrationV2ExecutionNode = {
      id: nextRootNodeId,
      threadId: input.command.threadId,
      runId: targetRun.id,
      parentNodeId: null,
      rootNodeId: nextRootNodeId,
      kind: "root_turn",
      status: "pending",
      countsForRun: true,
      providerThreadId: restartProviderThread.id,
      providerTurnId: null,
      nativeItemRef: null,
      runtimeRequestId: null,
      checkpointScopeId: checkpointScope.id,
      startedAt: null,
      completedAt: null,
    };
    if (currentAttempt !== undefined) {
      yield* emitEvent({
        type: "run-attempt.updated",
        threadId: input.command.threadId,
        runId: targetRun.id,
        nodeId: rootNodeId,
        providerInstanceId: targetRun.providerInstanceId,
        occurredAt: now,
        payload: { ...currentAttempt, status: "superseded", completedAt: now },
      });
    }
    if (currentRootNode !== undefined) {
      yield* emitEvent({
        type: "node.updated",
        threadId: input.command.threadId,
        runId: targetRun.id,
        nodeId: rootNodeId,
        providerInstanceId: targetRun.providerInstanceId,
        occurredAt: now,
        payload: { ...currentRootNode, status: "interrupted", completedAt: now },
      });
    }
    if (selectionChanged) {
      yield* emitEvent({
        type: providerInstanceChanged
          ? "thread.provider-switched"
          : "thread.model-selection-updated",
        threadId: input.command.threadId,
        providerInstanceId: input.modelSelection.instanceId,
        occurredAt: now,
        payload: {
          ...input.projection.thread,
          providerInstanceId: input.modelSelection.instanceId,
          modelSelection: input.modelSelection,
          updatedAt: now,
        },
      });
    }
    if (requiresProviderThreadHandoff || requiresProviderSessionRestart) {
      yield* emitEvent({
        type: "provider-thread.updated",
        threadId: input.command.threadId,
        driver: restartProviderThread.driver,
        providerInstanceId: input.modelSelection.instanceId,
        occurredAt: now,
        payload: restartProviderThread,
      });
    }
    if (restartHandoff !== null) {
      yield* emitEvent({
        type: "context-handoff.updated",
        threadId: input.command.threadId,
        runId: targetRun.id,
        providerInstanceId: input.modelSelection.instanceId,
        occurredAt: now,
        payload: restartHandoff,
      });
    }
    if (restartTransfer !== null) {
      yield* emitEvent({
        type: "context-transfer.created",
        threadId: input.command.threadId,
        runId: targetRun.id,
        providerInstanceId: input.modelSelection.instanceId,
        occurredAt: now,
        payload: restartTransfer,
      });
    }
    yield* emitEvent({
      type: "run.updated",
      threadId: input.command.threadId,
      runId: targetRun.id,
      nodeId: nextRootNodeId,
      providerInstanceId: input.modelSelection.instanceId,
      occurredAt: now,
      payload: restartedRun,
    });
    yield* emitEvent({
      type: "run-attempt.created",
      threadId: input.command.threadId,
      runId: targetRun.id,
      nodeId: nextRootNodeId,
      providerInstanceId: input.modelSelection.instanceId,
      occurredAt: now,
      payload: nextAttempt,
    });
    yield* emitEvent({
      type: "node.updated",
      threadId: input.command.threadId,
      runId: targetRun.id,
      nodeId: nextRootNodeId,
      providerInstanceId: input.modelSelection.instanceId,
      occurredAt: now,
      payload: nextRootNode,
    });
    yield* emitEvent({
      type: "checkpoint-scope.created",
      threadId: input.command.threadId,
      runId: targetRun.id,
      nodeId: nextRootNodeId,
      providerInstanceId: input.modelSelection.instanceId,
      occurredAt: now,
      payload: checkpointScope,
    });
    // The restart replaces the run's input, so the wake it was running can no longer be
    // settled at its terminal. Its provider turn was running, so the wake already landed.
    yield* acceptDelegatedCompletionDelivery({
      command: input.command,
      events: input.events,
      projection: input.projection,
      messageId: targetRun.userMessageId,
      now,
    });
    yield* appendSteeringMessage({
      runId: targetRun.id,
      nodeId: nextRootNodeId,
      providerTurnId: null,
      providerThreadId: restartProviderThread.id,
      providerInstanceId: input.modelSelection.instanceId,
      // Measured on the pre-restart projection, so the rewritten run is not its own baseline.
      ...(restartHandoff === null
        ? yield* history.modelSwitch(input.projection, input.modelSelection)
        : {}),
    });
    const interruptedAttemptId = targetRun.activeAttemptId;
    if (interruptedAttemptId === null) {
      return yield* new OrchestratorDispatchError({
        commandId: input.command.commandId,
        commandType: input.command.type,
        cause: `Active run ${targetRun.id} has no attempt to interrupt.`,
      });
    }
    yield* Ref.update(input.effects, (existing) => [
      ...existing,
      {
        id: `effect:${input.command.commandId}:provider-turn.restart:${providerTurn.id}`,
        commandId: input.command.commandId,
        threadId: input.command.threadId,
        request: {
          type: "provider-turn.restart",
          providerSessionId,
          providerThreadId: providerThread.id,
          providerTurnId: providerTurn.id,
          interruptedAttemptId,
          runId: targetRun.id,
          ...(restartSessionTransition === null
            ? {}
            : { sessionTransition: restartSessionTransition }),
        },
      } satisfies PendingOrchestrationEffectV2,
    ]);
  });

  const releaseQueueHold = Effect.fn("Orchestrator.releaseQueueHold")(function* (
    command: OrchestrationV2ServerCommand,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    thread: OrchestrationV2AppThread,
    now: DateTime.Utc,
  ) {
    return yield* emit(
      events,
      command,
    )({
      type: "thread.metadata-updated",
      threadId: thread.id,
      occurredAt: now,
      payload: {
        ...thread,
        queueReleaseGeneration: (thread.queueReleaseGeneration ?? 0) + 1,
      },
    });
  });

  // Providers own native subagent conversations; messages cannot target them, and adapters ask questions on the parent.
  const refuseNativeSubagentMessage = Effect.fnUntraced(function* (
    command: Pick<OrchestrationV2Command, "commandId"> & { readonly threadId: ThreadId },
  ) {
    const thread = yield* projectionStore
      .getThread(command.threadId)
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
        ),
      );
    if (isProviderNativeSubagentThread(thread)) {
      return yield* new OrchestratorSubagentThreadReadOnlyError({
        commandId: command.commandId,
        threadId: command.threadId,
      });
    }
    return thread;
  });

  // A new completion or pull request wake that would queue joins the thread's first queued
  // wake of its class instead, so a busy thread keeps one wake per class however many arrive.
  const joinQueuedWake = Effect.fn("Orchestrator.joinQueuedWake")(function* (input: {
    readonly command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly projection: OrchestrationV2ThreadProjection;
    readonly message: Pick<
      OrchestrationV2ConversationMessage,
      "createdBy" | "creationSource" | "text" | "notification" | "delegatedCompletion"
    >;
    readonly queueHeld: boolean;
    readonly delegatedCompletionOnly: Ref.Ref<RunId | undefined> | undefined;
  }) {
    const { command, projection, message } = input;
    const wakeClass = mergeableWakeClass(message);
    if (wakeClass === undefined || projection.messages.some((row) => row.id === command.messageId))
      return false;
    const emitEvent = emit(input.events, command);
    const now = yield* DateTime.now;
    // The newest one, so wakes keep their order; a full one leaves the new wake its own run.
    const target = joinableQueuedWakes(projection, wakeClass, input.queueHeld).at(-1);
    if (
      target === undefined ||
      mergedWakeItems(target.message) + mergedWakeItems(message) > MAX_MERGED_WAKE_ITEMS
    )
      return false;
    if (wakeClass === "pull-request") {
      const merged = mergedPullRequestWatchWake(target.message, message);
      if (merged === undefined) return false;
      yield* emitEvent(wakeMessageUpdated(target.run, { ...merged, updatedAt: now }, now));
      return true;
    }
    const parentRun = projection.runs.find(
      (run) => run.id === message.delegatedCompletion?.parentRunId,
    );
    const delivery = parentRun?.delegatedCompletion?.delivery;
    // A delegated thread's follow-up task owns the wakes of its own runs; merging would blur that.
    if (
      parentRun === undefined ||
      delivery == null ||
      input.delegatedCompletionOnly === undefined ||
      isDelegatedAppThread(projection.thread)
    )
      return false;
    const ownership = target.message.delegatedCompletion!;
    const { runs: owners } = yield* projectionStore
      .getThreadRecords(command.threadId, ["runs"], {
        runIds: completionWakeParts(ownership).map((part) => part.parentRunId),
      })
      .pipe(mapDispatchError(command));
    if (
      !currentCompletionWakeParts(owners, target.message).some(
        ({ part }) => part.parentRunId === ownership.parentRunId,
      )
    )
      return false;
    const joined = withCompletionWakePart(ownership, {
      parentRunId: parentRun.id,
      generation: delivery.generation,
      taskIds: delivery.taskIds,
    });
    const updated = yield* completionWakeMessage(
      target.message,
      joined,
      projection.subagents,
      now,
    ).pipe(mapDispatchError(command));
    yield* emitEvent(cohortRepointed(parentRun, target.message.id, now));
    yield* emitEvent(wakeMessageUpdated(target.run, updated, now));
    // The parent run may still be live; its lifecycle is not this command's to write.
    yield* Ref.set(input.delegatedCompletionOnly, parentRun.id);
    return true;
  });

  const dispatchMessage = Effect.fn("Orchestrator.dispatchMessage")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    delegatedCompletionOnly?: Ref.Ref<RunId | undefined>,
  ) {
    const dispatchRead = yield* readMessageDispatchProjection(command, events);
    let projection = dispatchRead.projection;
    if (command.steerFollowUpOfRunId !== undefined) {
      const source = projection.runs.find((run) => run.id === command.steerFollowUpOfRunId);
      // Only a declined steer's follow-up carries an ordinal; the fork's tracked delivery also names
      // the steered run on follow-ups that start at once or must survive Stop.
      const declined = command.steerFollowUpAfterRunOrdinal !== undefined;
      if (
        source === undefined ||
        (declined && command.dispatchMode.type !== "queue_after_active") ||
        !projection.messages.some(
          (message) => message.id === command.messageId && message.runId === source.id,
        ) ||
        projection.thread.archivedAt !== null ||
        projection.thread.deletedAt !== null ||
        (declined && (yield* stopReachedRun(command, command.threadId, source.id)))
      ) {
        yield* emit(
          events,
          command,
        )({
          type: "thread.metadata-updated",
          threadId: command.threadId,
          occurredAt: yield* DateTime.now,
          payload: projection.thread,
        });
        return;
      }
    }
    if (yield* isStaleQuestionWake(projection, command.messageId, command.notification)) {
      yield* emit(
        events,
        command,
      )({
        type: "thread.metadata-updated",
        threadId: command.threadId,
        occurredAt: yield* DateTime.now,
        payload: projection.thread,
      });
      return;
    }
    const activeProviderInstanceId = projection.providerThreads.find(
      (candidate) => candidate.id === projection.thread.activeProviderThreadId,
    )?.providerInstanceId;
    // Another provider is handed what ran since it last saw the thread, which can be older than
    // the compaction, so a provider switch reads the whole history.
    if (
      activeProviderInstanceId !== undefined &&
      activeProviderInstanceId !==
        (command.modelSelection ?? projection.thread.modelSelection).instanceId
    ) {
      projection = yield* getProjectionWithPendingEvents(command.threadId, events);
    }
    if (command.manualContinuationOfRunId !== undefined) {
      const source = projection.runs.find((run) => run.id === command.manualContinuationOfRunId);
      const limited = latestRootProviderFailure(source ?? null, projection.turnItems);
      if (
        command.dispatchMode.type !== "start_immediately" ||
        source === undefined ||
        (source.status !== "interrupted" &&
          !(source.status === "failed" && limited?.class === "usage_limit")) ||
        latestExecutedRun(projection.runs)?.id !== source.id ||
        projection.thread.archivedAt !== null ||
        projection.thread.deletedAt !== null ||
        projection.runtimeRequests.some((request) => request.status === "pending")
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "This thread can no longer be resumed from that run.",
        });
      }
    }
    if (command.usageLimitContinuationOfRunId !== undefined) {
      const run = usageLimitBlockedRun(projection.runs, projection.turnItems, null);
      const failure = latestRootProviderFailure(run, projection.turnItems);
      const recovery = projection.thread.limitRecovery;
      const now = yield* DateTime.now;
      if (
        run?.id !== command.usageLimitContinuationOfRunId ||
        failure?.class !== "usage_limit" ||
        threadShellFromProjection(projection).lastErrorClass !== "usage_limit" ||
        !recovery?.autoResume ||
        recovery.requestId !== command.usageLimitRecoveryRequestId ||
        recovery.runId !== run.id ||
        recovery.resetAt !== failure.resetAt ||
        Date.parse(recovery.resetAt) > DateTime.toEpochMillis(now) ||
        projection.thread.archivedAt !== null ||
        projection.thread.deletedAt !== null ||
        projection.thread.settledOverride === "settled" ||
        projection.thread.providerInstanceId !== run.providerInstanceId ||
        projection.runtimeRequests.some((request) => request.status === "pending") ||
        (projection.thread.snoozedUntil != null &&
          DateTime.toEpochMillis(projection.thread.snoozedUntil) > DateTime.toEpochMillis(now))
      ) {
        yield* emit(
          events,
          command,
        )({
          type: "thread.metadata-updated",
          threadId: command.threadId,
          occurredAt: now,
          payload: {
            ...projection.thread,
            ...(recovery?.autoResume &&
            recovery.requestId === command.usageLimitRecoveryRequestId &&
            recovery.runId === command.usageLimitContinuationOfRunId &&
            projection.thread.snoozedUntil != null &&
            DateTime.toEpochMillis(projection.thread.snoozedUntil) > DateTime.toEpochMillis(now)
              ? { limitRecovery: { ...recovery, requestId: command.commandId } }
              : {}),
          },
        });
        return;
      }
    }

    if (command.restartContinuationOfRunId !== undefined) {
      const source = projection.runs.find((run) => run.id === command.restartContinuationOfRunId);
      if (
        !source ||
        !(
          isRestartContinuationSource(source, projection.providerTurns, projection.thread) ||
          continuationFailedBeforePrompt(source, projection.providerTurns)
        ) ||
        projection.thread.archivedAt !== null ||
        projection.thread.deletedAt !== null ||
        projection.thread.providerInstanceId !== source.providerInstanceId ||
        projection.runs.some(
          (run) =>
            run.id !== source.id &&
            run.status !== "queued" &&
            !cancelledBeforeStart(run) &&
            runRanAfter(run, source),
        ) ||
        (yield* stopReachedRun(command, command.threadId, source.id))
      ) {
        // Preserve the current row so stale automatic deliveries receive an
        // accepted receipt without changing work or repeatedly retrying.
        yield* emit(
          events,
          command,
        )({
          type: "thread.metadata-updated",
          threadId: command.threadId,
          occurredAt: yield* DateTime.now,
          payload: projection.thread,
        });
        return;
      }
    }

    // The service checked idleness before this command; a run or a delegated result that landed
    // since makes the thread busy, and that turn refreshes the cache anyway.
    if (
      command.idleCompaction === "beforeCacheExpires" &&
      projection.runs.some(
        (run) =>
          run.status === "queued" ||
          isActiveRun(run) ||
          (run.delegatedCompletion?.disposition === "open" &&
            run.delegatedCompletion.delivery !== null &&
            !projection.messages.some(
              (message) => message.id === run.delegatedCompletion?.delivery?.messageId,
            )),
      )
    ) {
      yield* emit(
        events,
        command,
      )({
        type: "thread.metadata-updated",
        threadId: command.threadId,
        occurredAt: yield* DateTime.now,
        payload: projection.thread,
      });
      return;
    }

    if (projection.thread.lastVisitedAt === null && !isNativeMaintenanceCommand(command)) {
      const now = yield* DateTime.now;
      // Null keeps imported history read. The first fresh dispatch establishes
      // a read watermark so its completion can be unread on every client.
      // Offset it so a completion in the same millisecond is still unread.
      const event = yield* emit(
        events,
        command,
      )({
        type: "thread.metadata-updated",
        threadId: command.threadId,
        providerInstanceId: projection.thread.providerInstanceId,
        occurredAt: now,
        payload: {
          ...projection.thread,
          lastVisitedAt: DateTime.subtract(now, { milliseconds: 1 }),
        },
      });
      projection = applyToProjection(projection, event);
    }

    // A run the thread queues for itself continues the task of the run that sent it.
    const continuedTaskId = projection.runs.find((run) =>
      command.restartContinuationOfRunId === undefined
        ? command.senderThreadId === command.threadId && isActiveRun(run)
        : run.id === command.restartContinuationOfRunId,
    )?.delegatedTaskId;

    if (projection.thread.settledOverride !== null) {
      const now = yield* DateTime.now;
      const thread: OrchestrationV2AppThread = {
        ...projection.thread,
        settledOverride: null,
        settledAt: null,
        unsettledAt:
          projection.thread.settledOverride === "active"
            ? (projection.thread.unsettledAt ?? null)
            : now,
        updatedAt: now,
      };
      const event = yield* emit(
        events,
        command,
      )({
        type: "thread.unsettled",
        threadId: command.threadId,
        providerInstanceId: thread.providerInstanceId,
        occurredAt: now,
        payload: thread,
      });
      projection = applyToProjection(projection, event);
    }
    // Background wakes and delegated completions continue work the user already snoozed, so
    // only a message someone actually sent ends the snooze.
    const automaticWake =
      command.notification !== undefined || command.delegatedCompletion !== undefined;
    if (projection.thread.snoozedUntil != null && !automaticWake) {
      const now = yield* DateTime.now;
      const thread: OrchestrationV2AppThread = {
        ...projection.thread,
        snoozedUntil: null,
        snoozedAt: null,
        updatedAt: now,
      };
      const event = yield* emit(
        events,
        command,
      )({
        type: "thread.unsnoozed",
        threadId: command.threadId,
        providerInstanceId: thread.providerInstanceId,
        occurredAt: now,
        payload: thread,
      });
      projection = applyToProjection(projection, event);
    }
    const userMessages = projection.messages.filter((message) => message.role === "user");
    // The recent runs of a compacted thread can hold only a `/compact` while an older prompt
    // exists. Every prompt's turn item is loaded; without one, every user message is checked.
    const onlyMaintenanceHistory = [
      ...userMessages,
      ...projection.turnItems.flatMap((item) => (item.type === "user_message" ? [item] : [])),
    ].every(isNativeMaintenanceCommand)
      ? projectionStore
          .getThreadRecords(command.threadId, ["messages"], { messageRoles: ["user"] })
          .pipe(
            // A thread this command creates has no stored messages yet.
            Effect.catchTags({
              ProjectionStoreThreadNotFoundError: () => Effect.succeed({ messages: [] }),
            }),
            Effect.map(({ messages }) => [...messages, ...userMessages]),
            Effect.map((all) => all.length > 0 && all.every(isNativeMaintenanceCommand)),
            mapDispatchError(command),
          )
      : Effect.succeed(false);
    // A thread named before its first message (for example by an MCP launch that tracks an issue)
    // keeps that name instead of a title generated from a terse prompt such as "work on this".
    const seedsUntitledThread =
      command.titleSeed !== undefined &&
      (yield* projectionStore.getMessageCount(command.threadId).pipe(mapDispatchError(command))) ===
        0 &&
      !hasTitleSetBeforeFirstMessage(projection.thread, command.titleSeed);
    if (
      !isNativeMaintenanceCommand(command) &&
      (seedsUntitledThread || (command.createdBy === "user" && (yield* onlyMaintenanceHistory)))
    ) {
      const now = yield* DateTime.now;
      const thread: OrchestrationV2AppThread = {
        ...projection.thread,
        title: command.titleSeed ?? projection.thread.title,
        titleRegeneration: { requestId: command.commandId, startedAt: now },
        updatedAt: now,
      };
      const event = yield* emit(
        events,
        command,
      )({
        type: "thread.metadata-updated",
        threadId: command.threadId,
        providerInstanceId: thread.providerInstanceId,
        occurredAt: now,
        payload: thread,
      });
      yield* Ref.update(effects, (existing) => [
        ...existing,
        pendingThreadTitleGenerationEffect(command.commandId, command.threadId, {
          type: "initial",
          messageId: command.messageId,
        }),
      ]);
      projection = applyToProjection(projection, event);
    }
    let modelSelection = command.modelSelection ?? projection.thread.modelSelection;
    let dispatchMode = resolveMessageDispatchIntent(
      projection,
      command.dispatchMode,
      command.deliveryIntent,
      command.creationSource === "mcp",
      command.scheduledTaskId !== undefined,
    );
    if (dispatchMode.type === "steer_active" || dispatchMode.type === "restart_active") {
      const targetRunId = dispatchMode.targetRunId;
      const target = projection.runs.find((run) => run.id === targetRunId);
      const turn = latestProviderTurnForAttempt(
        projection.providerTurns.filter((candidate) => candidate.nodeId === target?.rootNodeId),
        target?.activeAttemptId,
      );
      // The client may still show a running turn while its completion is being
      // projected. Preserve the submission as a new turn when steering is too late.
      if (
        target !== undefined &&
        (target.status === "completed" ||
          (dispatchMode.type === "restart_active" && !isActiveRun(target)) ||
          ((target.status === "running" || target.status === "waiting") &&
            turn !== undefined &&
            (turn.status === "completed" ||
              (dispatchMode.type === "restart_active" &&
                turn.status !== "pending" &&
                turn.status !== "running"))))
      ) {
        dispatchMode = { type: "start_immediately" };
      }
    }
    // Stop leaves the run reading as running until its provider stops. A plain
    // send in that window queues behind the stopped queue instead of steering
    // text the agent never receives, and like any user send it releases the
    // hold; an explicit steer still fails below.
    if (
      command.deliveryIntent === "auto" &&
      (dispatchMode.type === "steer_active" || dispatchMode.type === "restart_active") &&
      (yield* stopReachedRun(command, command.threadId, dispatchMode.targetRunId))
    ) {
      dispatchMode = { type: "queue_after_active" };
    }
    // An explicit restart that Stop reached has only its start left, so it
    // queues behind the stopping run like any other start.
    if (
      dispatchMode.type === "restart_active" &&
      (yield* stopReachedRun(command, command.threadId, dispatchMode.targetRunId))
    ) {
      dispatchMode = { type: "start_immediately" };
    }
    const freshContext = command.freshContext === true;
    if (
      (freshContext ||
        (command.deliveryIntent === "auto" && isNativeMaintenanceCommand(command))) &&
      (dispatchMode.type === "steer_active" || dispatchMode.type === "restart_active")
    ) {
      dispatchMode = { type: "queue_after_active" };
    }
    // Native goal commands must not steer an existing turn.
    const parsedGoalCommand = parseGoalCommand(command);
    const goalCommand =
      parsedGoalCommand === null ||
      (yield* goalDriverFor(command, modelSelection.instanceId)) === undefined
        ? null
        : parsedGoalCommand;
    if (
      parsedGoalCommand !== null &&
      goalCommand === null &&
      command.deliveryIntent === "auto" &&
      (dispatchMode.type === "steer_active" || dispatchMode.type === "restart_active")
    ) {
      dispatchMode = { type: "queue_after_active" };
    }
    let queuedRunStop:
      | { readonly runId: RunId; readonly reason: string; readonly skipsHeldQueue?: true }
      | undefined;
    if (goalCommand !== null) {
      // A goal command never joins a running turn. One that starts or ends goal
      // work stops the run and goes first in the queue; the rest wait. A send
      // that asked to be queued (no delivery intent) only queues, and a run
      // that is only finishing or running background work is never stopped.
      const resolvedMode = dispatchMode;
      const target =
        resolvedMode.type === "steer_active" || resolvedMode.type === "restart_active"
          ? projection.runs.find((run) => run.id === resolvedMode.targetRunId)
          : resolvedMode.type === "queue_after_active" && command.deliveryIntent !== undefined
            ? projection.runs.findLast(isLiveRun)
            : undefined;
      if (target !== undefined) {
        const goal = projection.providerThreads.find(
          (candidate) => candidate.id === target.providerThreadId,
        )?.goal;
        const targetMessage = projection.messages.find(
          (message) => message.id === target.userMessageId,
        );
        if (
          isLiveRun(target) &&
          goalCommandStopsActiveRun(goalCommand, goal?.status ?? null) &&
          (targetMessage === undefined || !isNativeMaintenanceCommand(targetMessage)) &&
          !(yield* stopReachedRun(command, command.threadId, target.id))
        ) {
          queuedRunStop = { runId: target.id, reason: "Stopped to apply /goal" };
        }
        dispatchMode = { type: "queue_after_active" };
      }
    }
    if (
      command.notification !== undefined &&
      (command.createdBy !== "agent" ||
        (command.creationSource !== "server" && command.creationSource !== "provider") ||
        (dispatchMode.type !== "queue_after_active" &&
          !(
            command.restartContinuationOfRunId !== undefined &&
            dispatchMode.type === "start_immediately"
          )))
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "Notifications must be server- or provider-created queued messages.",
      });
    }
    let delegatedCompletion: OrchestrationV2ConversationMessage["delegatedCompletion"] | undefined;
    if (command.delegatedCompletion !== undefined) {
      const requestedCompletion = command.delegatedCompletion;
      if (
        command.createdBy !== "agent" ||
        command.creationSource !== "server" ||
        dispatchMode.type !== "queue_after_active"
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "Delegated completion delivery must be a server-created queued message.",
        });
      }
      const parentRun = projection.runs.find(
        (candidate) => candidate.id === requestedCompletion.parentRunId,
      );
      const delivery = parentRun?.delegatedCompletion?.delivery;
      const existingMessage = projection.messages.find(
        (candidate) => candidate.id === command.messageId,
      );
      const followUpPrefix = "command:steer-follow-up:";
      const durableFollowUp =
        command.steerFollowUpOfRunId !== undefined &&
        existingMessage?.runId === command.steerFollowUpOfRunId &&
        command.commandId.startsWith(followUpPrefix) &&
        Option.exists(
          yield* effectOutbox
            .get(command.commandId.slice(followUpPrefix.length))
            .pipe(mapDispatchError(command)),
          (effect) =>
            effect.threadId === command.threadId &&
            effect.status === "running" &&
            effect.request.type === "provider-turn.steer" &&
            effect.request.messageId === command.messageId &&
            effect.request.deliveryPhase === "follow_up",
        );
      if (
        parentRun?.delegatedCompletion?.disposition !== "open" ||
        delivery === null ||
        delivery === undefined ||
        delivery.generation !== requestedCompletion.generation ||
        delivery.messageId !== command.messageId ||
        (existingMessage !== undefined &&
          !durableFollowUp &&
          !isUndeliveredMailboxSteer(projection, command.messageId))
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "Delegated completion delivery is no longer dispatchable.",
        });
      }
      delegatedCompletion = {
        parentRunId: requestedCompletion.parentRunId,
        generation: delivery.generation,
        taskIds: delivery.taskIds,
      };
    }
    // A provider that can stop a turn but not restart it with new input runs a
    // restart as the next turn: first in the queue, with the running turn stopped.
    if (
      dispatchMode.type === "restart_active" &&
      !isNativeMaintenanceCommand(command) &&
      parsedGoalCommand === null
    ) {
      const targetRunId = dispatchMode.targetRunId;
      const target = projection.runs.find((run) => run.id === targetRunId);
      const providerSessionId = projection.providerThreads.find(
        (candidate) => candidate.id === target?.providerThreadId,
      )?.providerSessionId;
      const turns = projection.providerSessions.find((session) => session.id === providerSessionId)
        ?.capabilities.turns;
      const targetMessage = projection.messages.find(
        (message) => message.id === target?.userMessageId,
      );
      if (
        target?.status === "running" &&
        turns !== undefined &&
        turns.supportsInterrupt &&
        turns.supportsQueuedMessages &&
        !turns.supportsSteeringByInterruptRestart &&
        (targetMessage === undefined || !isNativeMaintenanceCommand(targetMessage)) &&
        !(yield* stopReachedRun(command, command.threadId, target.id))
      ) {
        // The restart replaces the stopped turn, so an older held queue does not hold it.
        queuedRunStop = {
          runId: target.id,
          reason: "Stopped to send a new message",
          skipsHeldQueue: true,
        };
        dispatchMode = { type: "queue_after_active" };
      }
    }
    // Steer mailbox deliveries, delegated task questions, and command notices
    // into an active goal under the thread lock: queued behind a long turn or a
    // goal run, they are never seen. Never interrupt/restart a turn for one.
    const noticeKind = command.notification?.source.kind;
    const commandNotice = noticeKind === "command" && command.creationSource === "server";
    if (
      !freshContext &&
      command.steerFollowUpOfRunId === undefined &&
      (delegatedCompletion !== undefined
        ? delegatedCompletion.taskIds.every(
            (id) =>
              projection.subagents.find((task) => task.id === id)?.completionWake === "always",
          )
        : noticeKind === "delegated_task" || commandNotice)
    ) {
      const active = projection.runs.find((run) => run.status === "running");
      const providerThread = projection.providerThreads.find(
        (row) => row.id === active?.providerThreadId,
      );
      const activeTurn = projection.providerTurns.find(
        (turn) => turn.runAttemptId === active?.activeAttemptId && turn.status === "running",
      );
      const activeMessage = projection.messages.find(
        (message) => message.id === active?.userMessageId,
      );
      if (
        active !== undefined &&
        activeTurn !== undefined &&
        providerThread?.providerSessionId != null &&
        (!commandNotice || providerThread.goal?.status === "active") &&
        (activeMessage === undefined || !isNativeMaintenanceCommand(activeMessage))
      ) {
        const session = yield* providerSessions
          .get(providerThread.providerSessionId)
          .pipe(Effect.orElseSucceed(() => Option.none()));
        if (
          Option.isSome(session) &&
          session.value.providerSession.capabilities.turns.supportsActiveSteering &&
          session.value.providerSession.capabilities.turns.activeSteeringInterruptsTools !== true &&
          !(yield* stopReachedRun(command, command.threadId, active.id))
        ) {
          dispatchMode = { type: "steer_active", targetRunId: active.id };
        }
      }
    }
    const notification =
      command.manualContinuationOfRunId === undefined
        ? command.notification
        : {
            source: { kind: "system" as const },
            outcome: "updated" as const,
            summary: "Resumed thread",
          };
    const dispatchText =
      command.manualContinuationOfRunId !== undefined
        ? ""
        : delegatedCompletion === undefined
          ? command.text
          : delegatedCompletionWakeDetail(delegatedCompletion.taskIds, projection.subagents);
    const sourcePlanProjection =
      command.sourcePlanRef === undefined
        ? null
        : yield* getProjectionWithPendingEvents(command.sourcePlanRef.threadId, events);
    const sourcePlanArtifact =
      command.sourcePlanRef === undefined
        ? undefined
        : yield* projectionStore
            .getPlan(command.sourcePlanRef.threadId, command.sourcePlanRef.planId)
            .pipe(mapDispatchError(command));
    const sourcePlan = sourcePlanArtifact?.kind === "proposed_plan" ? sourcePlanArtifact : null;
    if (command.sourcePlanRef !== undefined && sourcePlan === null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Proposed plan ${command.sourcePlanRef.planId} does not exist on thread ${command.sourcePlanRef.threadId}.`,
      });
    }
    if (
      sourcePlanProjection !== null &&
      sourcePlanProjection.thread.projectId !== projection.thread.projectId
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Proposed plan ${command.sourcePlanRef?.planId} belongs to a different project.`,
      });
    }
    if (sourcePlan !== null && sourcePlan.status !== "active") {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Proposed plan ${sourcePlan.id} is not active.`,
      });
    }
    const completeSourcePlan = (occurredAt: DateTime.Utc) =>
      sourcePlan === null
        ? Effect.void
        : emit(
            events,
            command,
          )({
            type: "plan.updated",
            threadId: sourcePlan.threadId,
            ...(sourcePlan.runId === null ? {} : { runId: sourcePlan.runId }),
            nodeId: sourcePlan.nodeId,
            occurredAt,
            payload: { ...sourcePlan, status: "completed" },
          });

    let steerWhenReadyRunId: RunId | undefined;
    if (dispatchMode.type === "steer_active") {
      const targetRunId = dispatchMode.targetRunId;
      const target = projection.runs.find((run) => run.id === targetRunId);
      if (
        (target?.status === "preparing" ||
          target?.status === "starting" ||
          (target?.status === "running" &&
            !projection.providerTurns.some(
              (turn) => turn.runAttemptId === target.activeAttemptId && turn.status === "running",
            ))) &&
        !(yield* stopReachedRun(command, command.threadId, target.id))
      ) {
        const adapter = yield* providerAdapters
          .get(target.providerInstanceId)
          .pipe(mapDispatchError(command));
        const capabilities = yield* adapter.getCapabilities().pipe(mapDispatchError(command));
        let deferSteer = true;
        if (command.deliveryIntent === "auto") {
          const providerSessionId = projection.providerThreads.find(
            (candidate) => candidate.id === target.providerThreadId,
          )?.providerSessionId;
          // Automatic input queues where a live turn would not take it as a steer, and beside a
          // running run whose provider session is gone.
          deferSteer =
            automaticDeliverySteers(capabilities.turns, command.creationSource === "mcp") &&
            (target.status !== "running" ||
              (providerSessionId != null &&
                Option.isSome(
                  yield* providerSessions
                    .get(providerSessionId)
                    .pipe(Effect.orElseSucceed(() => Option.none())),
                )));
        } else {
          yield* commandPolicy
            .decideSteeringExecution({
              commandId: command.commandId,
              threadId: command.threadId,
              providerInstanceId: target.providerInstanceId,
              capabilities,
            })
            .pipe(mapDispatchError(command));
        }
        if (deferSteer) {
          yield* ensureSteeringMessage(command, command);
          steerWhenReadyRunId = target.id;
        }
        dispatchMode = { type: "queue_after_active" };
      }
    }
    if (dispatchMode.type === "steer_active" || dispatchMode.type === "restart_active") {
      yield* ensureGoalCommandSupported(command, command, modelSelection.instanceId);
      const targetRunId = dispatchMode.targetRunId;
      const steering = yield* dispatchSteerIntoRun({
        command,
        events,
        effects,
        projection,
        modelSelection:
          delegatedCompletion === undefined && notification === undefined
            ? modelSelection
            : (projection.runs.find((run) => run.id === targetRunId)?.modelSelection ??
              modelSelection),
        delegatedCompletion,
        ...(notification === undefined ? {} : { notification }),
        targetRunId: dispatchMode.targetRunId,
        messageId: command.messageId,
        text: dispatchText,
        ...(command.context ? { context: command.context } : {}),
        attachments: command.attachments,
        createdBy: command.createdBy,
        creationSource: command.creationSource,
        ...(command.scheduledTaskId === undefined
          ? {}
          : { scheduledTaskId: command.scheduledTaskId }),
        ...(command.senderThreadId === undefined ? {} : { senderThreadId: command.senderThreadId }),
        ...(command.linkedSender === undefined ? {} : { linkedSender: command.linkedSender }),
        ...(command.idleCompaction === undefined ? {} : { idleCompaction: command.idleCompaction }),
        forceRestart: dispatchMode.type === "restart_active",
        history: dispatchRead.history,
      });
      if (steering !== "queue") return;
      // A projected turn may outlive its live session. Preserve automatic input as queued work.
      dispatchMode = { type: "queue_after_active" };
    }

    const activeProviderThread = dispatchProviderThread(
      projection,
      { ...command, delegatedCompletion },
      freshContext,
    );
    const activeRun = projection.runs.find(isActiveRun);
    // Messages after failed or stopped worktree setup must wait for a worktree,
    // rather than start in the project checkout.
    const latestRun = latestExecutedRun(projection.runs);
    const worktreeMissingRun =
      activeRun === undefined &&
      (dispatchMode.type === "queue_after_active" || dispatchMode.type === "start_immediately") &&
      (latestRun?.status === "failed" || latestRun?.status === "interrupted") &&
      latestRun.workspacePreparation?.type === "worktree" &&
      projection.thread.worktreePath === null
        ? latestRun
        : undefined;
    // Results whose wake failed before reaching the provider queue behind it rather than fail the
    // same start at once; startNextQueuedRun retries them after a backoff.
    const failedWakeRun =
      activeRun === undefined &&
      mergeableWakeClass({
        createdBy: command.createdBy,
        creationSource: command.creationSource,
        ...(notification === undefined ? {} : { notification }),
        ...(delegatedCompletion === undefined ? {} : { delegatedCompletion }),
      }) !== undefined &&
      latestRun?.status === "failed" &&
      (latestRun.startedAt === null ||
        (yield* failedBeforeProvider(command.threadId, latestRun).pipe(mapDispatchError(command))))
        ? latestRun
        : undefined;
    // News sent again queues behind the thread's latest run, so its start checks its watches.
    const noticeRetryRun =
      activeRun === undefined && command.messageId.startsWith(NOTICE_RETRY_MESSAGE_PREFIX)
        ? (latestRun ?? projection.runs.at(-1))
        : undefined;
    const sentByUserFromClient =
      command.createdBy === "user" &&
      (command.creationSource === "web" || command.creationSource === "mobile") &&
      command.scheduledTaskId === undefined &&
      command.steerFollowUpOfRunId === undefined;
    const steerFollowUpSource = projection.runs.find(
      (run) => run.id === command.steerFollowUpOfRunId,
    );
    const queueHoldSource =
      steerFollowUpSource ??
      (sentByUserFromClient
        ? undefined
        : (activeRun ??
          latestRun ??
          projection.runs.findLast((run) => run.status === "queued" && run.queueHeld === true)));
    const queueHoldRunId =
      queueHoldSource === undefined
        ? undefined
        : latestRun !== null &&
            (latestRun.id === queueHoldSource.id || runRanAfter(latestRun, queueHoldSource))
          ? latestRun.id
          : queueHoldSource.id;
    const holdStoppedQueue =
      queueHoldSource !== undefined &&
      ((yield* Ref.get(events)).some(
        (event) =>
          event.type === "turn-item.updated" &&
          event.payload.type === "run_interrupt_request" &&
          event.payload.runId === queueHoldRunId &&
          event.payload.holdQueue === true &&
          (event.payload.queueHoldGeneration ?? 0) >=
            (projection.thread.queueReleaseGeneration ?? 0),
      ) ||
        (yield* projectionStore
          .getThreadRecords(command.threadId, ["turnItems"], {
            turnItemTypes: ["run_interrupt_request"],
            // A later executed turn supersedes the source's Stop barrier.
            turnItemRunIds: [queueHoldRunId!],
          })
          .pipe(
            Effect.map(({ turnItems }) =>
              turnItems.some(
                (item) =>
                  item.type === "run_interrupt_request" &&
                  item.holdQueue === true &&
                  (item.queueHoldGeneration ?? 0) >=
                    (projection.thread.queueReleaseGeneration ?? 0),
              ),
            ),
            mapDispatchError(command),
          )) ||
        (steerFollowUpSource !== undefined &&
          projection.runs.some((run) => run.status === "queued" && run.queueHeld === true)));
    const queueBehindRun =
      activeRun ??
      worktreeMissingRun ??
      failedWakeRun ??
      noticeRetryRun ??
      (holdStoppedQueue ? queueHoldSource : undefined);
    const pendingMergeBackTransfers = pendingMergeBackTransfersForThread(projection);
    const startsOrQueuesRun =
      dispatchMode.type === "defer_start" ||
      dispatchMode.type === "start_immediately" ||
      dispatchMode.type === "queue_after_active";
    const shouldQueue = queueBehindRun !== undefined && startsOrQueuesRun;
    // An explicit pick queued ahead reaches the thread only when its run starts. A goal or
    // restart command that stops the active run goes first, so nothing queued applies before it.
    // Buffered provider wakes drain output their own provider already produced.
    const incomingMessage = { ...command, delegatedCompletion };
    if (
      shouldQueue &&
      steerWhenReadyRunId === undefined &&
      command.modelSelection === undefined &&
      queuedRunStop === undefined &&
      !followsCurrentConversation(incomingMessage) &&
      !isBufferedProviderNotification(incomingMessage)
    ) {
      modelSelection = queueTailModelSelection(projection, incomingMessage);
    }
    yield* ensureGoalCommandSupported(command, command, modelSelection.instanceId);
    // Client messages resume held queues; server continuations and scheduled tasks do not.
    // Provider failure or usage limits re-hold the queue.
    const worktreeSetupUnresolved = worktreeSetupPending(projection);
    const releasesQueue = sentByUserFromClient && startsOrQueuesRun && !worktreeSetupUnresolved;
    const heldRunsToRelease = releasesQueue
      ? projection.runs.filter((run) => run.status === "queued" && run.queueHeld === true)
      : [];
    const releasedAt = yield* DateTime.now;
    if (releasesQueue) {
      projection = applyToProjection(
        projection,
        yield* releaseQueueHold(command, events, projection.thread, releasedAt),
      );
    }
    for (const held of heldRunsToRelease) {
      yield* emit(
        events,
        command,
      )({
        type: "run.updated",
        threadId: command.threadId,
        runId: held.id,
        providerInstanceId: held.providerInstanceId,
        occurredAt: releasedAt,
        payload: { ...held, queueHeld: false },
      });
    }
    if (shouldQueue) {
      if (pendingMergeBackTransfers.length > 0) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Thread ${command.threadId} has a pending merge-back transfer; queued merge-back consumption is not implemented yet.`,
        });
      }
      const queueProviderThread =
        activeProviderThread ??
        projection.providerThreads.find(
          (candidate) => candidate.id === queueBehindRun.providerThreadId,
        );
      if (queueProviderThread === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Active run ${queueBehindRun.id} has no provider thread for queued dispatch.`,
        });
      }
      const now = yield* DateTime.now;
      const ordinal = nextRunOrdinal(projection);
      const runId = idAllocator.derive.run({ threadId: command.threadId, ordinal });
      const targetProviderThread = freshContext
        ? undefined
        : modelSelection.instanceId === queueProviderThread.providerInstanceId
          ? queueProviderThread
          : rootProviderThreadsForProvider(projection, modelSelection.instanceId)[0];
      const queuedAdapter = yield* providerAdapters
        .get(modelSelection.instanceId)
        .pipe(mapDispatchError(command));
      const selectedProviderSession =
        targetProviderThread?.providerSessionId === null ||
        targetProviderThread?.providerSessionId === undefined
          ? undefined
          : projection.providerSessions.find(
              (candidate) => candidate.id === targetProviderThread.providerSessionId,
            );
      const queuedCapabilities =
        selectedProviderSession?.capabilities ??
        (yield* queuedAdapter.getCapabilities().pipe(mapDispatchError(command)));
      yield* mapDispatchError(command)(
        commandPolicy.ensureQueuedMessages({
          commandId: command.commandId,
          threadId: command.threadId,
          providerInstanceId: modelSelection.instanceId,
          capabilities: queuedCapabilities,
        }),
      );
      const queuedProviderThread: OrchestrationV2ProviderThread = targetProviderThread ?? {
        id: idAllocator.derive.providerThread({
          driver: queuedAdapter.driver,
          nativeThreadId: `pending:${runId}`,
        }),
        driver: queuedAdapter.driver,
        providerInstanceId: modelSelection.instanceId,
        providerSessionId: null,
        appThreadId: command.threadId,
        ownerNodeId: null,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "not_loaded",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        createdAt: now,
        updatedAt: now,
      };
      const attemptId = idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
      const rootNodeId = idAllocator.derive.rootNode({ runId });
      const checkpointScope =
        queueBehindRun.status === "preparing" || worktreeMissingRun !== undefined
          ? null
          : yield* runtimePolicy.resolve({ thread: projection.thread, modelSelection }).pipe(
              Effect.flatMap((resolvedRuntimePolicy) =>
                prepareRootRunScope({
                  threadScopes: projection.checkpointScopes,
                  threadId: command.threadId,
                  runId,
                  rootNodeId,
                  providerThreadId: queuedProviderThread.id,
                  cwd:
                    resolvedRuntimePolicy.cwd ??
                    selectedProviderSession?.cwd ??
                    projection.thread.worktreePath ??
                    process.cwd(),
                  createdAt: now,
                }),
              ),
              Effect.mapError(
                (cause) =>
                  new OrchestratorDispatchError({
                    commandId: command.commandId,
                    commandType: command.type,
                    cause,
                  }),
              ),
            );
      const queueIsHeld =
        holdStoppedQueue ||
        projection.runs.some(
          (candidate) => candidate.status === "queued" && candidate.queueHeld === true,
        );
      // Buffered wakes need a T3 run for output the CLI already started producing.
      // Stop still holds wakes that arrive before the active run settles.
      const wakeSkipsHold =
        !worktreeSetupUnresolved &&
        activeRun !== undefined &&
        queueIsHeld &&
        command.createdBy === "agent" &&
        command.creationSource === "provider" &&
        !(yield* stopReachedRun(command, command.threadId, activeRun.id));
      // Nobody reviews unattended input, so only Stop or unresolved worktree setup holds it.
      const unattendedSkipsHold =
        queueIsHeld &&
        !holdStoppedQueue &&
        !worktreeSetupUnresolved &&
        (command.createdBy !== "user" || command.scheduledTaskId !== undefined) &&
        (activeRun === undefined ||
          !(yield* stopReachedRun(command, command.threadId, activeRun.id)));
      const queueHeld =
        worktreeMissingRun !== undefined ||
        (heldRunsToRelease.length === 0 &&
          queueIsHeld &&
          !wakeSkipsHold &&
          !unattendedSkipsHold &&
          queuedRunStop?.skipsHeldQueue !== true);
      if (
        steerWhenReadyRunId === undefined &&
        queuedRunStop === undefined &&
        (yield* joinQueuedWake({
          command,
          events,
          projection,
          message: {
            createdBy: command.createdBy,
            creationSource: command.creationSource,
            text: dispatchText,
            ...(notification === undefined ? {} : { notification }),
            ...(delegatedCompletion === undefined ? {} : { delegatedCompletion }),
          },
          queueHeld,
          delegatedCompletionOnly,
        }))
      ) {
        return;
      }
      let steerFollowUpQueuePosition: number | undefined;
      if (
        command.steerFollowUpOfRunId !== undefined &&
        command.steerFollowUpAfterRunOrdinal !== undefined
      ) {
        const afterRunOrdinal = command.steerFollowUpAfterRunOrdinal;
        const steerOrdinal =
          projection.turnItems.find(
            (item) => item.type === "user_message" && item.messageId === command.messageId,
          )?.ordinal ?? -1;
        // Earlier declined steers have newer runs but retain their original turn-item ordinals.
        const earlierSteerMessageIds = new Set(
          projection.turnItems.flatMap((item) =>
            item.type === "user_message" && item.ordinal < steerOrdinal ? [item.messageId] : [],
          ),
        );
        steerFollowUpQueuePosition =
          Math.max(
            0,
            ...projection.runs
              .filter(
                (candidate) =>
                  candidate.status === "queued" &&
                  (candidate.ordinal <= afterRunOrdinal ||
                    earlierSteerMessageIds.has(candidate.userMessageId)),
              )
              .map((candidate) => candidate.queuePosition ?? candidate.ordinal),
          ) + 1;
      }
      const run: OrchestrationV2Run = {
        id: runId,
        threadId: command.threadId,
        ordinal,
        providerInstanceId: modelSelection.instanceId,
        modelSelection,
        providerThreadId: queuedProviderThread.id,
        userMessageId: command.messageId,
        rootNodeId,
        activeAttemptId: attemptId,
        status: "queued",
        ...(steerWhenReadyRunId === undefined ? {} : { steerWhenReadyRunId }),
        ...(command.steerFollowUpOfRunId === undefined
          ? {}
          : { steerFollowUpOfRunId: command.steerFollowUpOfRunId }),
        ...(queueHeld ? { queueHeld: true } : {}),
        queuePosition:
          queuedRunStop !== undefined
            ? 1
            : (steerFollowUpQueuePosition ??
              Math.max(
                0,
                ...projection.runs
                  .filter((candidate) => candidate.status === "queued")
                  .map((candidate) => candidate.queuePosition ?? candidate.ordinal),
              ) + 1),
        requestedAt: now,
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
        ...(command.sourcePlanRef === undefined ? {} : { sourcePlanRef: command.sourcePlanRef }),
        ...(command.restartContinuationOfRunId === undefined
          ? {}
          : { restartContinuationOfRunId: command.restartContinuationOfRunId }),
        ...(command.manualContinuationOfRunId === undefined
          ? {}
          : { manualContinuationOfRunId: command.manualContinuationOfRunId }),
        ...(command.usageLimitContinuationOfRunId === undefined
          ? {}
          : { usageLimitContinuationOfRunId: command.usageLimitContinuationOfRunId }),
        ...(continuedTaskId === undefined ? {} : { delegatedTaskId: continuedTaskId }),
        ...(checkpointScope === null ? {} : { workspaceCwd: runWorkspaceOf(checkpointScope) }),
        ...(freshContext ? { freshContext: true } : {}),
      };
      const attempt: OrchestrationV2RunAttempt = {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        providerThreadId: queuedProviderThread.id,
        providerTurnId: null,
        reason: "initial",
        status: "pending",
        startedAt: null,
        completedAt: null,
      };
      const rootNode: OrchestrationV2ExecutionNode = {
        id: rootNodeId,
        threadId: command.threadId,
        runId,
        parentNodeId: null,
        rootNodeId,
        kind: "root_turn",
        status: "pending",
        countsForRun: true,
        providerThreadId: queuedProviderThread.id,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: checkpointScope?.id ?? null,
        startedAt: null,
        completedAt: null,
      };
      const message: OrchestrationV2ConversationMessage = {
        createdBy: command.createdBy,
        creationSource: command.creationSource,
        ...(command.scheduledTaskId === undefined
          ? {}
          : { scheduledTaskId: command.scheduledTaskId }),
        ...(command.senderThreadId === undefined ? {} : { senderThreadId: command.senderThreadId }),
        ...(command.linkedSender === undefined ? {} : { linkedSender: command.linkedSender }),
        ...(command.idleCompaction === undefined ? {} : { idleCompaction: command.idleCompaction }),
        ...(command.scheduledTaskId !== undefined && command.modelSelection === undefined
          ? { followsThreadSelection: true as const }
          : {}),
        id: command.messageId,
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        role: "user",
        text: dispatchText,
        ...(command.context ? { context: command.context } : {}),
        attachments: command.attachments,
        streaming: false,
        createdAt: now,
        updatedAt: now,
        ...(delegatedCompletion === undefined ? {} : { delegatedCompletion }),
        ...(notification === undefined ? {} : { notification }),
      };
      const emitEvent = emit(events, command);
      if (targetProviderThread === undefined) {
        yield* emitEvent({
          type: "provider-thread.updated",
          threadId: command.threadId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: queuedProviderThread,
        });
      }
      yield* emitEvent({
        type: "run.created",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: run,
      });
      if (steerFollowUpQueuePosition !== undefined) {
        for (const queued of projection.runs) {
          if (
            queued.status !== "queued" ||
            (queued.queuePosition ?? queued.ordinal) < steerFollowUpQueuePosition
          )
            continue;
          yield* emitEvent({
            type: "run.updated",
            threadId: command.threadId,
            runId: queued.id,
            providerInstanceId: queued.providerInstanceId,
            occurredAt: now,
            payload: { ...queued, queuePosition: (queued.queuePosition ?? queued.ordinal) + 1 },
          });
        }
      }
      yield* completeSourcePlan(now);
      yield* emitEvent({
        type: "run-attempt.created",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: attempt,
      });
      yield* emitEvent({
        type: "node.updated",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: rootNode,
      });
      if (checkpointScope !== null) {
        yield* emitEvent({
          type: "checkpoint-scope.created",
          threadId: command.threadId,
          runId,
          nodeId: rootNodeId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: checkpointScope,
        });
      }
      yield* emitEvent({
        type: "message.updated",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: message,
      });
      const existingItem = projection.turnItems.find(
        (item) => item.type === "user_message" && item.messageId === command.messageId,
      );
      if (existingItem?.type === "user_message") {
        yield* emitEvent({
          type: "turn-item.updated",
          threadId: command.threadId,
          runId,
          nodeId: rootNodeId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: {
            ...existingItem,
            runId,
            nodeId: rootNodeId,
            providerThreadId: queuedProviderThread.id,
            providerTurnId: null,
            inputIntent: "queued_turn",
            status: "pending",
            startedAt: null,
            completedAt: null,
            updatedAt: now,
          },
        });
      }
      if (queuedRunStop !== undefined) {
        return yield* stopRunForQueuedCommand({
          command,
          events,
          effects,
          projection: yield* getProjectionWithPendingEvents(command.threadId, events),
          queuedRunId: runId,
          targetRunId: queuedRunStop.runId,
          reason: queuedRunStop.reason,
          now,
        });
      }
      return;
    }
    const pendingForkTransfer = pendingForkTransferForThread(projection);
    const pendingMergeBackSourceThreadIds = new Set(
      pendingMergeBackTransfers.map((transfer) => transfer.sourceThreadId),
    );
    if (pendingMergeBackSourceThreadIds.size > 1) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} has pending merge-back transfers from multiple forks.`,
      });
    }
    const pendingMergeBackTransfer = latestContextTransfer(pendingMergeBackTransfers);
    const supersededMergeBackTransfers = pendingMergeBackTransfers.filter(
      (transfer) => transfer.id !== pendingMergeBackTransfer?.id,
    );
    const now = yield* DateTime.now;
    if (
      !modelSelectionsEqual(projection.thread.modelSelection, modelSelection) ||
      projection.thread.providerInstanceId !== modelSelection.instanceId
    ) {
      yield* emit(
        events,
        command,
      )({
        type:
          projection.thread.providerInstanceId === modelSelection.instanceId
            ? "thread.model-selection-updated"
            : "thread.provider-switched",
        threadId: command.threadId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: {
          ...projection.thread,
          providerInstanceId: modelSelection.instanceId,
          modelSelection,
          updatedAt: now,
        },
      });
    }
    if (
      freshContext &&
      (pendingForkTransfer !== undefined || pendingMergeBackTransfer !== undefined)
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "A fresh session can't start while a fork or merge is still being applied.",
      });
    }
    const ordinal = nextRunOrdinal(projection);
    const runId = idAllocator.derive.run({ threadId: command.threadId, ordinal });
    const contextBoundary = freshContext ? ordinal : contextBoundaryOrdinal(projection);
    const latestCompletedRun = projection.runs.findLast((run) => run.status === "completed");
    const latestHandoffRun = projection.runs.findLast(isHandoffSourceRun);
    const legacyImportItems =
      projection.thread.historyOrigin === "v1_import" && contextBoundary === 0
        ? yield* readHandoffItems(command.threadId, [null])
        : [];
    const isProviderSwitch =
      activeProviderThread !== undefined &&
      activeProviderThread.providerInstanceId !== modelSelection.instanceId;
    // Account overlays share native history. Selection commands may already
    // have updated the app thread, so classify against the native thread's owner.
    const providerSwitchPlan = isProviderSwitch
      ? yield* providerSwitchService
          .plan({
            projection: {
              ...projection,
              thread: {
                ...projection.thread,
                modelSelection: {
                  ...projection.thread.modelSelection,
                  instanceId: activeProviderThread.providerInstanceId,
                },
              },
            },
            targetModelSelection: modelSelection,
          })
          .pipe(mapDispatchError(command))
      : null;
    const canResumeAcrossInstances =
      !freshContext &&
      isProviderSwitch &&
      activeProviderThread.nativeThreadRef !== null &&
      providerSwitchPlan?.transition.type === "restart_and_resume";
    const releasableSessions = uniqueSessions([
      ...projection.providerSessions.filter(
        (session) =>
          providerSwitchPlan?.releaseProviderSessionIds.includes(session.id) &&
          session.status !== "stopped" &&
          session.status !== "error",
      ),
      ...(freshContext ? sessionsReleasedByFreshContext(projection, activeProviderThread) : []),
    ]);
    const sessionsToDetach: Array<OrchestrationV2ProviderSession> = [];
    for (const detaching of releasableSessions) {
      if (yield* sessionHasBackgroundWork(detaching, projection.providerThreads)) {
        // A schedule has nobody to wait or press Stop; like a queued fresh start, leave it running.
        if (freshContext && command.scheduledTaskId !== undefined) continue;
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: freshContext
            ? "The current session is still running background agents or commands, and a fresh session would end them. Wait for them to finish, or press Stop, then send the message again."
            : "The previous provider is still running background agents or commands, and switching would end them. Wait for them to finish, or press Stop, then send the message again.",
        });
      }
      sessionsToDetach.push(detaching);
    }

    if (
      pendingForkTransfer === undefined &&
      pendingMergeBackTransfer === undefined &&
      (!isProviderSwitch || freshContext)
    ) {
      const reusableProviderThread = freshContext ? undefined : activeProviderThread;
      const adapter = yield* providerAdapters.get(modelSelection.instanceId).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorProviderAdapterError({
              commandId: command.commandId,
              providerInstanceId: modelSelection.instanceId,
              cause,
            }),
        ),
      );
      const providerSessionId =
        reusableProviderThread?.providerSessionId ??
        (yield* mapDispatchError(command)(
          providerSessionIdFor({
            adapter,
            providerInstanceId: modelSelection.instanceId,
            threadId: command.threadId,
          }),
        ));
      const providerThreadId =
        reusableProviderThread?.id ??
        idAllocator.derive.providerThread({
          driver: adapter.driver,
          nativeThreadId: `pending:${runId}`,
        });
      const legacyImportHandoff = shouldPrepareLegacyImportHandoff({
        hasNativeHistory:
          activeProviderThread?.driver === "acpRegistry" &&
          activeProviderThread.nativeThreadRef !== null,
        historyOrigin: projection.thread.historyOrigin,
        hasCompletedRun: latestCompletedRun !== undefined,
        legacyImportItemCount: legacyImportItems.length,
      })
        ? yield* contextHandoffService
            .prepareLegacyImport({
              threadId: command.threadId,
              targetRunId: runId,
              toProviderThreadId: providerThreadId,
              toProviderInstanceId: modelSelection.instanceId,
              items: legacyImportItems,
              createdAt: now,
            })
            .pipe(mapDispatchError(command))
        : null;
      const providerThread: OrchestrationV2ProviderThread =
        reusableProviderThread === undefined
          ? {
              id: providerThreadId,
              driver: adapter.driver,
              providerInstanceId: modelSelection.instanceId,
              providerSessionId,
              appThreadId: command.threadId,
              ownerNodeId: null,
              nativeThreadRef: null,
              nativeConversationHeadRef: null,
              status: "not_loaded",
              firstRunOrdinal: ordinal,
              lastRunOrdinal: ordinal,
              handoffIds: legacyImportHandoff === null ? [] : [legacyImportHandoff.id],
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
            }
          : {
              ...reusableProviderThread,
              providerSessionId,
              lastRunOrdinal: ordinal,
              handoffIds: appendContextHandoffId(
                reusableProviderThread.handoffIds,
                legacyImportHandoff?.id ?? null,
              ),
              updatedAt: now,
            };
      const attemptId = idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
      const rootNodeId = idAllocator.derive.rootNode({ runId });
      const checkpointScope =
        dispatchMode.type === "defer_start"
          ? null
          : yield* runtimePolicy
              .resolve({
                thread: projection.thread,
                modelSelection,
              })
              .pipe(
                mapDispatchError(command),
                Effect.flatMap((resolvedRuntimePolicy) =>
                  prepareRootRunScope({
                    threadScopes: projection.checkpointScopes,
                    threadId: command.threadId,
                    runId,
                    rootNodeId,
                    providerThreadId,
                    cwd:
                      resolvedRuntimePolicy.cwd ?? projection.thread.worktreePath ?? process.cwd(),
                    createdAt: now,
                  }),
                ),
                mapDispatchError(command),
              );
      const run: OrchestrationV2Run = {
        id: runId,
        threadId: command.threadId,
        ordinal,
        providerInstanceId: modelSelection.instanceId,
        modelSelection,
        providerThreadId,
        userMessageId: command.messageId,
        rootNodeId,
        activeAttemptId: attemptId,
        status: dispatchMode.type === "defer_start" ? "preparing" : "starting",
        queuePosition: null,
        requestedAt: now,
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: legacyImportHandoff?.id ?? null,
        ...(command.sourcePlanRef === undefined ? {} : { sourcePlanRef: command.sourcePlanRef }),
        ...(command.restartContinuationOfRunId === undefined
          ? {}
          : { restartContinuationOfRunId: command.restartContinuationOfRunId }),
        ...(command.manualContinuationOfRunId === undefined
          ? {}
          : { manualContinuationOfRunId: command.manualContinuationOfRunId }),
        ...(command.usageLimitContinuationOfRunId === undefined
          ? {}
          : { usageLimitContinuationOfRunId: command.usageLimitContinuationOfRunId }),
        ...(dispatchMode.type === "defer_start" && dispatchMode.workspaceStrategy !== undefined
          ? { workspacePreparation: dispatchMode.workspaceStrategy }
          : {}),
        ...(continuedTaskId === undefined ? {} : { delegatedTaskId: continuedTaskId }),
        ...wakeWorkStartedAt(projection, command),
        ...(checkpointScope === null ? {} : { workspaceCwd: runWorkspaceOf(checkpointScope) }),
        ...(freshContext ? { freshContext: true } : {}),
      };
      const attempt: OrchestrationV2RunAttempt = {
        id: attemptId,
        runId,
        attemptOrdinal: 1,
        rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        providerThreadId,
        providerTurnId: null,
        reason: "initial",
        status: "pending",
        startedAt: null,
        completedAt: null,
      };
      const rootNode: OrchestrationV2ExecutionNode = {
        id: rootNodeId,
        threadId: command.threadId,
        runId,
        parentNodeId: null,
        rootNodeId,
        kind: "root_turn",
        status: "pending",
        countsForRun: true,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: checkpointScope?.id ?? null,
        startedAt: null,
        completedAt: null,
      };
      const message: OrchestrationV2ConversationMessage = {
        createdBy: command.createdBy,
        creationSource: command.creationSource,
        ...(command.scheduledTaskId === undefined
          ? {}
          : { scheduledTaskId: command.scheduledTaskId }),
        ...(command.senderThreadId === undefined ? {} : { senderThreadId: command.senderThreadId }),
        ...(command.linkedSender === undefined ? {} : { linkedSender: command.linkedSender }),
        ...(command.idleCompaction === undefined ? {} : { idleCompaction: command.idleCompaction }),
        ...(command.scheduledTaskId !== undefined && command.modelSelection === undefined
          ? { followsThreadSelection: true as const }
          : {}),
        id: command.messageId,
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        role: "user",
        text: dispatchText,
        ...(command.context ? { context: command.context } : {}),
        attachments: command.attachments,
        streaming: false,
        createdAt: now,
        updatedAt: now,
        ...(delegatedCompletion === undefined ? {} : { delegatedCompletion }),
        ...(notification === undefined ? {} : { notification }),
      };
      const turnItem: OrchestrationV2TurnItem = {
        createdBy: command.createdBy,
        creationSource: command.creationSource,
        ...(command.scheduledTaskId === undefined
          ? {}
          : { scheduledTaskId: command.scheduledTaskId }),
        ...(command.senderThreadId === undefined ? {} : { senderThreadId: command.senderThreadId }),
        ...(command.linkedSender === undefined ? {} : { linkedSender: command.linkedSender }),
        ...(command.idleCompaction === undefined ? {} : { idleCompaction: command.idleCompaction }),
        id: idAllocator.derive.userTurnItem({ messageId: command.messageId }),
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: yield* nextTurnItemOrdinal(projection),
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "user_message",
        messageId: command.messageId,
        inputIntent: "turn_start",
        text: dispatchText,
        ...(command.context ? { context: command.context } : {}),
        attachments: command.attachments,
        ...(freshContext ? { freshContext: true } : {}),
        ...(run.contextHandoffId === null
          ? yield* dispatchRead.history.modelSwitch(projection, modelSelection)
          : {}),
      };
      const preparationTurnItem: OrchestrationV2TurnItem | null =
        dispatchMode.type === "defer_start"
          ? {
              id: idAllocator.derive.turnItemFromProviderItem({
                driver: adapter.driver,
                nativeItemId: `workspace-preparation:${runId}`,
              }),
              threadId: command.threadId,
              runId,
              nodeId: rootNodeId,
              providerThreadId,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: turnItem.ordinal + 1,
              status: "running",
              title: WORKSPACE_PREPARATION_INPUT,
              startedAt: now,
              completedAt: null,
              updatedAt: now,
              type: "command_execution",
              input: WORKSPACE_PREPARATION_INPUT,
            }
          : null;
      const emitEvent = emit(events, command);
      yield* emitEvent({
        type: "provider-thread.updated",
        threadId: command.threadId,
        driver: adapter.driver,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: providerThread,
      });
      if (legacyImportHandoff !== null) {
        yield* emitEvent({
          type: "context-handoff.updated",
          threadId: command.threadId,
          runId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: legacyImportHandoff,
        });
      }
      yield* emitEvent({
        type: "run.created",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: run,
      });
      yield* completeSourcePlan(now);
      yield* emitEvent({
        type: "run-attempt.created",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: attempt,
      });
      yield* emitEvent({
        type: "node.updated",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: rootNode,
      });
      if (checkpointScope !== null) {
        yield* emitEvent({
          type: "checkpoint-scope.created",
          threadId: command.threadId,
          runId,
          nodeId: rootNodeId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: checkpointScope,
        });
      }
      yield* emitEvent({
        type: "message.updated",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: message,
      });
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: notificationTurnItem(
          turnItem,
          message,
          yield* notificationTasks(command.threadId, message, projection.subagents),
        ),
      });
      if (preparationTurnItem !== null) {
        yield* emitEvent({
          type: "turn-item.updated",
          threadId: command.threadId,
          runId,
          nodeId: rootNodeId,
          providerInstanceId: modelSelection.instanceId,
          occurredAt: now,
          payload: preparationTurnItem,
        });
      }
      for (const session of sessionsToDetach) {
        if (session.id === providerSessionId) continue;
        yield* emitEvent({
          type: "provider-session.detached",
          threadId: command.threadId,
          driver: session.driver,
          providerInstanceId: session.providerInstanceId,
          occurredAt: now,
          payload: {
            providerSessionId: session.id,
            detachedAt: now,
            reason: FRESH_CONTEXT_DETACH_REASON,
          },
        });
        yield* Ref.update(effects, (existing) => [
          ...existing,
          {
            id: `effect:${command.commandId}:provider-session.detach:${session.id}`,
            commandId: command.commandId,
            threadId: command.threadId,
            request: {
              type: "provider-session.detach",
              providerSessionId: session.id,
              detail: FRESH_CONTEXT_DETACH_REASON,
            },
          } satisfies PendingOrchestrationEffectV2,
        ]);
      }
      const pendingEffect = {
        id: `effect:${command.commandId}:provider-turn.start:${runId}`,
        commandId: command.commandId,
        threadId: command.threadId,
        request: { type: "provider-turn.start", runId },
      } satisfies PendingOrchestrationEffectV2;
      if (dispatchMode.type !== "defer_start") {
        yield* Ref.update(effects, (existing) => [...existing, pendingEffect]);
      }
      return;
    }
    const sourceProjection =
      pendingForkTransfer === undefined
        ? null
        : yield* projectionStore
            .getThreadRecords(pendingForkTransfer.sourceThreadId, [
              "runs",
              "attempts",
              "providerThreads",
              "providerTurns",
            ])
            .pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorProjectionError({
                    threadId: pendingForkTransfer.sourceThreadId,
                    cause,
                  }),
              ),
            );
    const sourceRun =
      pendingForkTransfer?.sourcePoint.runId === undefined || sourceProjection === null
        ? null
        : (sourceProjection.runs.find(
            (candidate) => candidate.id === pendingForkTransfer.sourcePoint.runId,
          ) ?? null);
    const sourceProviderThread =
      sourceProjection === null || sourceRun === null
        ? undefined
        : providerThreadForRun(sourceProjection, sourceRun);
    const sourceProviderTurnId =
      sourceProjection === null || sourceRun === null || sourceRun.activeAttemptId === null
        ? undefined
        : (latestProviderTurnForAttempt(sourceProjection.providerTurns, sourceRun.activeAttemptId)
            ?.id ??
          sourceProjection.attempts.find((candidate) => candidate.id === sourceRun.activeAttemptId)
            ?.providerTurnId ??
          undefined);
    const sideChatFork =
      pendingForkTransfer !== undefined && projection.thread.lineage.sideChat === true;
    // Opened during the parent's first turn: the digest is the whole context.
    const sideChatWithoutSource =
      sideChatFork && pendingForkTransfer.sourcePoint.runId === undefined;
    if (pendingForkTransfer !== undefined) {
      if (!sideChatWithoutSource && (sourceRun === null || sourceProviderThread === undefined)) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Pending fork transfer ${pendingForkTransfer.id} has no resolvable source provider thread.`,
        });
      }
      if (pendingForkTransfer.sourceProviderInstanceId === null) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Pending fork transfer ${pendingForkTransfer.id} has no source provider.`,
        });
      }
    }

    const adapter = yield* providerAdapters.get(modelSelection.instanceId).pipe(
      Effect.mapError(
        (cause) =>
          new OrchestratorProviderAdapterError({
            commandId: command.commandId,
            providerInstanceId: modelSelection.instanceId,
            cause,
          }),
      ),
    );
    const targetProviderThread =
      isProviderSwitch && !canResumeAcrossInstances
        ? rootProviderThreadsForProvider(projection, modelSelection.instanceId)[0]
        : activeProviderThread;
    const providerSessionId =
      (canResumeAcrossInstances ? undefined : targetProviderThread?.providerSessionId) ??
      (yield* mapDispatchError(command)(
        providerSessionIdFor({
          adapter,
          providerInstanceId: modelSelection.instanceId,
          threadId: command.threadId,
        }),
      ));
    const existingProviderSession = projection.providerSessions.find(
      (candidate) => candidate.id === providerSessionId,
    );
    const resolvedRuntimePolicy = yield* runtimePolicy
      .resolve({ thread: projection.thread, modelSelection })
      .pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorDispatchError({
              commandId: command.commandId,
              commandType: command.type,
              cause,
            }),
        ),
      );

    const capabilities = yield* adapter.getCapabilities().pipe(
      Effect.mapError(
        (cause) =>
          new OrchestratorProviderAdapterError({
            commandId: command.commandId,
            providerInstanceId: modelSelection.instanceId,
            cause,
          }),
      ),
    );
    const sourceRunsAfterFork =
      sourceProjection === null
        ? []
        : sourceProjection.runs.filter(
            (candidate) => sourceRun === null || candidate.ordinal > sourceRun.ordinal,
          );
    const forkExecution =
      pendingForkTransfer === undefined || sourceRun === null
        ? null
        : yield* mapDispatchError(command)(
            commandPolicy.decideForkExecution({
              commandId: command.commandId,
              threadId: command.threadId,
              providerInstanceId: modelSelection.instanceId,
              capabilities,
              sameProvider:
                pendingForkTransfer.sourceProviderInstanceId === modelSelection.instanceId,
              hasStrongNativeSource: sourceProviderThread?.nativeThreadRef?.strength === "strong",
              sourceRunStatus: sourceRun.status,
              fromSpecificTurn: sourceRun !== null,
              sourceBusy: sourceRunsAfterFork.some(isActiveRun),
            }),
          );
    const canResolveForkNatively = forkExecution === "native_fork";
    const requiresPortableFork = forkExecution === "portable_context";

    if (canResolveForkNatively) {
      yield* mapDispatchError(command)(
        commandPolicy.ensureNativeFork({
          commandId: command.commandId,
          threadId: command.threadId,
          providerInstanceId: modelSelection.instanceId,
          capabilities,
          fromSpecificTurn: sourceRun !== null,
        }),
      );
    }

    const ensuredProviderThread: OrchestrationV2ProviderThread =
      targetProviderThread === undefined
        ? {
            id: idAllocator.derive.providerThread({
              driver: adapter.driver,
              nativeThreadId: `pending:${runId}`,
            }),
            driver: adapter.driver,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId,
            appThreadId: command.threadId,
            ownerNodeId: null,
            nativeThreadRef: null,
            nativeConversationHeadRef: null,
            status: "not_loaded",
            firstRunOrdinal: ordinal,
            lastRunOrdinal: ordinal,
            handoffIds: [],
            forkedFrom:
              canResolveForkNatively && sourceProviderThread !== undefined
                ? {
                    providerThreadId: sourceProviderThread.id,
                    ...(sourceProviderTurnId === undefined
                      ? {}
                      : { providerTurnId: sourceProviderTurnId }),
                  }
                : null,
            createdAt: now,
            updatedAt: now,
          }
        : {
            ...targetProviderThread,
            providerInstanceId: modelSelection.instanceId,
            providerSessionId,
            updatedAt: now,
          };
    const portableForkItems =
      !requiresPortableFork || sourceProjection === null || sourceRun === null
        ? []
        : yield* readForkHistoryItems(projectionStore, sourceProjection, sourceRun).pipe(
            Effect.mapError(
              (cause) =>
                new OrchestratorProjectionError({ threadId: sourceProjection.thread.id, cause }),
            ),
          );
    const portableForkHandoff =
      !requiresPortableFork ||
      pendingForkTransfer === undefined ||
      sourceProjection === null ||
      sourceRun === null
        ? null
        : yield* contextHandoffService
            .prepareProviderHandoff({
              threadId: command.threadId,
              sourceThreadId: sourceProjection.thread.id,
              targetRunId: runId,
              transferId: pendingForkTransfer.id,
              fromProviderThreadIds:
                sourceProviderThread === undefined ? [] : [sourceProviderThread.id],
              toProviderThreadId: ensuredProviderThread.id,
              fromProviderInstanceId: sourceRun.providerInstanceId,
              toProviderInstanceId: modelSelection.instanceId,
              coveredRunOrdinals: visibleDeltaRunOrdinals(sourceProjection, portableForkItems),
              strategy: "full_thread_summary",
              items: portableForkItems,
              runs: sourceProjection.runs,
              createdAt: now,
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorDispatchError({
                    commandId: command.commandId,
                    commandType: command.type,
                    cause,
                  }),
              ),
            );
    // Without a source run the digest covers the parent's whole history: inherited fork rows
    // first, so the cap truncates them first, then imported and run items.
    const sideChatParentRunIds = [
      // Queued messages have not reached the main thread yet.
      ...sourceRunsAfterFork.filter((run) => run.status !== "queued").map((run) => run.id),
      ...(sourceRun === null && sourceProjection?.thread.historyOrigin === "v1_import"
        ? [null]
        : []),
    ];
    const sideChatParentItems =
      !sideChatFork || sourceProjection === null
        ? []
        : [
            ...(sideChatWithoutSource ? yield* readInheritedItems(sourceProjection.thread) : []),
            ...(sideChatParentRunIds.length === 0
              ? []
              : visibleConversationItems(
                  sourceProjection,
                  yield* readHandoffItems(sourceProjection.thread.id, sideChatParentRunIds),
                )),
          ];
    // The framing note goes in even when the parent has no activity after the fork point.
    const sideChatContextHandoff =
      !sideChatFork || pendingForkTransfer === undefined || sourceProjection === null
        ? null
        : yield* contextHandoffService
            .prepareSideChatContext({
              threadId: command.threadId,
              parentThreadId: sourceProjection.thread.id,
              targetRunId: runId,
              transferId: pendingForkTransfer.id,
              toProviderThreadId: ensuredProviderThread.id,
              fromProviderInstanceId:
                pendingForkTransfer.sourceProviderInstanceId ?? modelSelection.instanceId,
              toProviderInstanceId: modelSelection.instanceId,
              coveredRunOrdinals: visibleDeltaRunOrdinals(sourceProjection, sideChatParentItems),
              parentItems: sideChatParentItems,
              createdAt: now,
            })
            .pipe(mapDispatchError(command));
    const requiresFullProviderSwitchContext =
      isProviderSwitch && pendingMergeBackTransfer !== undefined;
    const targetLastCompletedRun =
      targetProviderThread === undefined
        ? undefined
        : lastDeliveredRunForProviderThread(projection, targetProviderThread.id);
    const providerSwitchCoveredRuns =
      !isProviderSwitch || canResumeAcrossInstances || latestHandoffRun === undefined
        ? []
        : projection.runs.filter(
            (run) =>
              isHandoffSourceRun(run) &&
              run.ordinal >= contextBoundary &&
              run.ordinal >
                (requiresFullProviderSwitchContext ? 0 : (targetLastCompletedRun?.ordinal ?? 0)) &&
              run.ordinal <= latestHandoffRun.ordinal,
          );
    const providerSwitchNeedsFullContext =
      targetProviderThread === undefined ||
      targetProviderThread.nativeThreadRef === null ||
      requiresFullProviderSwitchContext;
    const providerSwitchItems =
      providerSwitchCoveredRuns.length === 0
        ? []
        : [
            ...(providerSwitchNeedsFullContext && contextBoundary === 0
              ? yield* readInheritedItems(projection.thread)
              : []),
            ...(latestCompletedRun !== undefined && providerSwitchNeedsFullContext
              ? legacyImportItems
              : []),
            ...(yield* readHandoffItems(
              command.threadId,
              providerSwitchCoveredRuns.map((run) => run.id),
            )),
          ];
    const providerSwitchTransferId =
      providerSwitchCoveredRuns.length === 0 || latestHandoffRun === undefined
        ? null
        : yield* mapDispatchError(command)(
            idAllocator.allocate.contextTransfer({
              sourceThreadId: command.threadId,
              targetThreadId: command.threadId,
              type: "provider_handoff",
            }),
          );
    if (providerSwitchTransferId !== null) {
      yield* mapDispatchError(command)(
        commandPolicy.ensureContextHandoff({
          commandId: command.commandId,
          threadId: command.threadId,
          providerInstanceId: modelSelection.instanceId,
          capabilities,
          strategy: providerSwitchNeedsFullContext ? "full_thread_summary" : "delta_context",
        }),
      );
    }
    const providerSwitchHandoff =
      providerSwitchTransferId === null || latestHandoffRun === undefined
        ? null
        : yield* contextHandoffService
            .prepareProviderHandoff({
              threadId: command.threadId,
              targetRunId: runId,
              transferId: providerSwitchTransferId,
              fromProviderThreadIds: Array.from(
                new Set(
                  providerSwitchCoveredRuns.flatMap((run) =>
                    run.providerThreadId === null ? [] : [run.providerThreadId],
                  ),
                ),
              ),
              toProviderThreadId: ensuredProviderThread.id,
              fromProviderInstanceId: latestHandoffRun.providerInstanceId,
              toProviderInstanceId: modelSelection.instanceId,
              coveredRunOrdinals: {
                from: providerSwitchCoveredRuns[0]!.ordinal,
                to: providerSwitchCoveredRuns.at(-1)!.ordinal,
              },
              strategy: providerSwitchNeedsFullContext
                ? "full_thread_summary"
                : "delta_since_target_last_seen",
              items: providerSwitchItems,
              runs: projection.runs,
              createdAt: now,
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorDispatchError({
                    commandId: command.commandId,
                    commandType: command.type,
                    cause,
                  }),
              ),
            );
    const legacyImportRecoveryHandoff =
      isProviderSwitch &&
      !canResumeAcrossInstances &&
      latestCompletedRun === undefined &&
      legacyImportItems.length > 0
        ? yield* contextHandoffService
            .prepareLegacyImport({
              threadId: command.threadId,
              targetRunId: runId,
              toProviderThreadId: ensuredProviderThread.id,
              toProviderInstanceId: modelSelection.instanceId,
              items: legacyImportItems,
              createdAt: now,
            })
            .pipe(mapDispatchError(command))
        : null;
    const providerThread: OrchestrationV2ProviderThread = {
      ...ensuredProviderThread,
      status: "active",
      firstRunOrdinal: ensuredProviderThread.firstRunOrdinal ?? ordinal,
      lastRunOrdinal: ordinal,
      handoffIds: [
        ...ensuredProviderThread.handoffIds,
        ...[
          portableForkHandoff,
          sideChatContextHandoff,
          providerSwitchHandoff,
          legacyImportRecoveryHandoff,
        ].flatMap((handoff) => (handoff === null ? [] : [handoff.id])),
      ],
      updatedAt: now,
    };

    const attemptId = idAllocator.derive.runAttempt({ runId, attemptOrdinal: 1 });
    const rootNodeId = idAllocator.derive.rootNode({ runId });
    const emitEvent = emit(events, command);
    const mergeBackSourceProjection =
      pendingMergeBackTransfer === undefined
        ? null
        : yield* projectionStore
            .getThreadRecords(pendingMergeBackTransfer.sourceThreadId, [
              "runs",
              "attempts",
              "providerThreads",
              "providerTurns",
            ])
            .pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorProjectionError({
                    threadId: pendingMergeBackTransfer.sourceThreadId,
                    cause,
                  }),
              ),
            );
    const mergeBackSourceRun =
      pendingMergeBackTransfer?.sourcePoint.runId === undefined ||
      mergeBackSourceProjection === null
        ? null
        : (mergeBackSourceProjection.runs.find(
            (candidate) => candidate.id === pendingMergeBackTransfer.sourcePoint.runId,
          ) ?? null);
    if (pendingMergeBackTransfer !== undefined && mergeBackSourceRun === null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Pending merge-back transfer ${pendingMergeBackTransfer.id} has no resolvable source run.`,
      });
    }
    const mergeBackSourceProviderThread =
      mergeBackSourceProjection === null || mergeBackSourceRun === null
        ? undefined
        : providerThreadForRun(mergeBackSourceProjection, mergeBackSourceRun);
    if (pendingMergeBackTransfer !== undefined && mergeBackSourceProviderThread === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Pending merge-back transfer ${pendingMergeBackTransfer.id} has no resolvable source provider thread.`,
      });
    }
    if (pendingMergeBackTransfer !== undefined) {
      yield* mapDispatchError(command)(
        commandPolicy.ensureContextHandoff({
          commandId: command.commandId,
          threadId: command.threadId,
          providerInstanceId: modelSelection.instanceId,
          capabilities,
          strategy: "fork_delta_context",
        }),
      );
    }
    const mergeBackDeltaItems =
      mergeBackSourceProjection === null || mergeBackSourceRun === null
        ? []
        : visibleConversationItems(
            mergeBackSourceProjection,
            yield* readHandoffItems(
              mergeBackSourceProjection.thread.id,
              mergeBackSourceProjection.runs
                .filter((run) => run.ordinal <= mergeBackSourceRun.ordinal)
                .map((run) => run.id),
            ),
          );
    const mergeBackHandoff =
      pendingMergeBackTransfer === undefined ||
      mergeBackSourceProjection === null ||
      mergeBackSourceRun === null ||
      mergeBackSourceProviderThread === undefined
        ? null
        : yield* contextHandoffService
            .prepareForkDelta({
              sourceThreadId: pendingMergeBackTransfer.sourceThreadId,
              targetThreadId: command.threadId,
              targetRunId: runId,
              transferId: pendingMergeBackTransfer.id,
              fromProviderThreadIds: [mergeBackSourceProviderThread.id],
              toProviderThreadId: providerThread.id,
              fromProviderInstanceId: mergeBackSourceRun.providerInstanceId,
              toProviderInstanceId: modelSelection.instanceId,
              coveredRunOrdinals: visibleDeltaRunOrdinals(
                mergeBackSourceProjection,
                mergeBackDeltaItems,
              ),
              deltaItems: mergeBackDeltaItems,
              createdAt: now,
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorDispatchError({
                    commandId: command.commandId,
                    commandType: command.type,
                    cause,
                  }),
              ),
            );
    const checkpointScope = yield* prepareRootRunScope({
      threadScopes: projection.checkpointScopes,
      threadId: command.threadId,
      runId,
      rootNodeId,
      providerThreadId: providerThread.id,
      cwd:
        resolvedRuntimePolicy.cwd ??
        existingProviderSession?.cwd ??
        projection.thread.worktreePath ??
        process.cwd(),
      createdAt: now,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause,
          }),
      ),
    );
    const run: OrchestrationV2Run = {
      id: runId,
      threadId: command.threadId,
      ordinal,
      providerInstanceId: modelSelection.instanceId,
      modelSelection,
      providerThreadId: providerThread.id,
      userMessageId: command.messageId,
      rootNodeId,
      activeAttemptId: attemptId,
      status: "starting",
      queuePosition: null,
      requestedAt: now,
      startedAt: null,
      completedAt: null,
      checkpointId: null,
      contextHandoffId:
        portableForkHandoff?.id ??
        providerSwitchHandoff?.id ??
        mergeBackHandoff?.id ??
        legacyImportRecoveryHandoff?.id ??
        null,
      ...(command.sourcePlanRef === undefined ? {} : { sourcePlanRef: command.sourcePlanRef }),
      ...(command.restartContinuationOfRunId === undefined
        ? {}
        : { restartContinuationOfRunId: command.restartContinuationOfRunId }),
      ...(command.manualContinuationOfRunId === undefined
        ? {}
        : { manualContinuationOfRunId: command.manualContinuationOfRunId }),
      ...(command.usageLimitContinuationOfRunId === undefined
        ? {}
        : { usageLimitContinuationOfRunId: command.usageLimitContinuationOfRunId }),
      ...(continuedTaskId === undefined ? {} : { delegatedTaskId: continuedTaskId }),
      ...wakeWorkStartedAt(projection, command),
      workspaceCwd: runWorkspaceOf(checkpointScope),
    };
    const attempt: OrchestrationV2RunAttempt = {
      id: attemptId,
      runId,
      attemptOrdinal: 1,
      rootNodeId,
      providerInstanceId: modelSelection.instanceId,
      providerThreadId: providerThread.id,
      providerTurnId: null,
      reason: "initial",
      status: "pending",
      startedAt: null,
      completedAt: null,
    };
    const rootNode: OrchestrationV2ExecutionNode = {
      id: rootNodeId,
      threadId: command.threadId,
      runId,
      parentNodeId: null,
      rootNodeId,
      kind: "root_turn",
      status: "pending",
      countsForRun: true,
      providerThreadId: providerThread.id,
      providerTurnId: null,
      nativeItemRef: null,
      runtimeRequestId: null,
      checkpointScopeId: checkpointScope.id,
      startedAt: null,
      completedAt: null,
    };
    const message: OrchestrationV2ConversationMessage = {
      createdBy: command.createdBy,
      creationSource: command.creationSource,
      ...(command.scheduledTaskId === undefined
        ? {}
        : { scheduledTaskId: command.scheduledTaskId }),
      ...(command.senderThreadId === undefined ? {} : { senderThreadId: command.senderThreadId }),
      ...(command.linkedSender === undefined ? {} : { linkedSender: command.linkedSender }),
      ...(command.idleCompaction === undefined ? {} : { idleCompaction: command.idleCompaction }),
      id: command.messageId,
      threadId: command.threadId,
      runId,
      nodeId: rootNodeId,
      role: "user",
      text: dispatchText,
      ...(command.context ? { context: command.context } : {}),
      attachments: command.attachments,
      streaming: false,
      createdAt: now,
      updatedAt: now,
      ...(delegatedCompletion === undefined ? {} : { delegatedCompletion }),
      ...(notification === undefined ? {} : { notification }),
    };
    const turnItem: OrchestrationV2TurnItem = {
      createdBy: command.createdBy,
      creationSource: command.creationSource,
      ...(command.scheduledTaskId === undefined
        ? {}
        : { scheduledTaskId: command.scheduledTaskId }),
      ...(command.senderThreadId === undefined ? {} : { senderThreadId: command.senderThreadId }),
      ...(command.linkedSender === undefined ? {} : { linkedSender: command.linkedSender }),
      ...(command.idleCompaction === undefined ? {} : { idleCompaction: command.idleCompaction }),
      id: idAllocator.derive.userTurnItem({ messageId: command.messageId }),
      threadId: command.threadId,
      runId,
      nodeId: rootNodeId,
      providerThreadId: providerThread.id,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: ordinal * 100,
      status: "completed",
      title: null,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      type: "user_message",
      messageId: command.messageId,
      inputIntent: "turn_start",
      text: dispatchText,
      ...(command.context ? { context: command.context } : {}),
      attachments: command.attachments,
      ...(run.contextHandoffId === null
        ? modelSwitchSince(
            projection,
            modelSelection,
            // Side chats do not show the parent's runs.
            sideChatFork ? undefined : sourceRun?.modelSelection,
          )
        : {}),
    };
    // A side chat's "Context from" divider already marks its copied context.
    const activeHandoff =
      (sideChatFork ? null : portableForkHandoff) ?? mergeBackHandoff ?? providerSwitchHandoff;
    const handoffSourceRuns =
      portableForkHandoff !== null
        ? sourceRun === null
          ? []
          : [sourceRun]
        : providerSwitchHandoff === null
          ? mergeBackSourceRun === null
            ? []
            : [mergeBackSourceRun]
          : providerSwitchCoveredRuns;
    const handoffFromModelSelections = Array.from(
      new Map(
        handoffSourceRuns.map((run) => [
          `${run.modelSelection.instanceId}\0${run.modelSelection.model}`,
          run.modelSelection,
        ]),
      ).values(),
    );
    const handoffTurnItem: OrchestrationV2TurnItem | null =
      activeHandoff === null
        ? null
        : {
            id: idAllocator.derive.runSignalTurnItem({
              runId,
              signal: `context-handoff:${activeHandoff.id}`,
            }),
            threadId: command.threadId,
            runId,
            nodeId: rootNodeId,
            providerThreadId: providerThread.id,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: ordinal * 100 - 1,
            status: "completed",
            title:
              portableForkHandoff !== null
                ? "Fork context"
                : providerSwitchHandoff !== null
                  ? "Provider handoff"
                  : "Merge-back context",
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            type: "handoff",
            contextHandoffId: activeHandoff.id,
            fromProviderThreadIds: activeHandoff.fromProviderThreadIds,
            toProviderThreadId: activeHandoff.toProviderThreadId,
            fromProviderInstanceIds: Array.from(
              new Set(handoffSourceRuns.map((run) => run.providerInstanceId)),
            ),
            toProviderInstanceId: modelSelection.instanceId,
            fromModelSelections: handoffFromModelSelections,
            toModel: modelSelection.model,
            strategy: activeHandoff.strategy,
            summary: activeHandoff.summaryText,
          };
    const nativeForkResolution: OrchestrationV2ContextTransferResolution | null =
      !canResolveForkNatively || providerThread.nativeThreadRef === null
        ? null
        : {
            strategy: "native_fork",
            providerThreadRef: providerThread.nativeThreadRef,
          };
    const portableForkContext =
      portableForkHandoff ?? (sideChatWithoutSource ? sideChatContextHandoff : null);
    const portableForkResolution: OrchestrationV2ContextTransferResolution | null =
      pendingForkTransfer === undefined || portableForkContext === null
        ? null
        : {
            strategy: "portable_context",
            contextHandoffId: portableForkContext.id,
          };
    const mergeBackResolution: OrchestrationV2ContextTransferResolution | null =
      pendingMergeBackTransfer === undefined || mergeBackHandoff === null
        ? null
        : {
            strategy: "fork_delta_context",
            contextHandoffId: mergeBackHandoff.id,
          };

    if (pendingForkTransfer !== undefined && canResolveForkNatively) {
      yield* emitEvent({
        type: "context-transfer.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: {
          ...pendingForkTransfer,
          targetProviderInstanceId: modelSelection.instanceId,
          targetRunId: runId,
          status: "pending",
          resolution: null,
          error: null,
          updatedAt: now,
        },
      });
    }
    if (pendingForkTransfer !== undefined && portableForkResolution !== null) {
      yield* emitEvent({
        type: "context-transfer.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: {
          ...pendingForkTransfer,
          targetProviderInstanceId: modelSelection.instanceId,
          targetRunId: runId,
          status: "resolved_portable",
          resolution: portableForkResolution,
          error: null,
          updatedAt: now,
        },
      });
    }
    yield* emitEvent({
      type: "provider-thread.updated",
      threadId: command.threadId,
      providerInstanceId: modelSelection.instanceId,
      occurredAt: now,
      payload: providerThread,
    });
    if (portableForkHandoff !== null) {
      yield* emitEvent({
        type: "context-handoff.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: portableForkHandoff,
      });
    }
    if (sideChatContextHandoff !== null) {
      yield* emitEvent({
        type: "context-handoff.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: sideChatContextHandoff,
      });
    }
    if (legacyImportRecoveryHandoff !== null) {
      yield* emitEvent({
        type: "context-handoff.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: legacyImportRecoveryHandoff,
      });
    }
    if (
      providerSwitchTransferId !== null &&
      providerSwitchHandoff !== null &&
      latestHandoffRun !== undefined
    ) {
      const transfer: OrchestrationV2ContextTransfer = {
        id: providerSwitchTransferId,
        type: "provider_handoff",
        sourceThreadId: command.threadId,
        targetThreadId: command.threadId,
        sourcePoint: contextSourcePointForRun(projection, latestHandoffRun),
        basePoint:
          requiresFullProviderSwitchContext || targetLastCompletedRun === undefined
            ? null
            : contextSourcePointForRun(projection, targetLastCompletedRun),
        sourceProviderInstanceId: latestHandoffRun.providerInstanceId,
        targetProviderInstanceId: modelSelection.instanceId,
        targetRunId: runId,
        status: "consumed",
        resolution: {
          strategy:
            providerSwitchHandoff.strategy === "full_thread_summary"
              ? "portable_context"
              : "delta_context",
          contextHandoffId: providerSwitchHandoff.id,
        },
        createdBy: command.createdBy,
        error: null,
        createdAt: now,
        updatedAt: now,
        consumedAt: now,
      };
      yield* emitEvent({
        type: "context-transfer.created",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: transfer,
      });
      yield* emitEvent({
        type: "context-handoff.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: providerSwitchHandoff,
      });
    }
    if (mergeBackHandoff !== null) {
      yield* emitEvent({
        type: "context-handoff.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: mergeBackHandoff,
      });
    }
    for (const supersededTransfer of supersededMergeBackTransfers) {
      yield* emitEvent({
        type: "context-transfer.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: {
          ...supersededTransfer,
          status: "superseded",
          error:
            pendingMergeBackTransfer === undefined
              ? "Superseded while consuming merge-back transfer."
              : `Superseded by merge-back transfer ${pendingMergeBackTransfer.id}.`,
          updatedAt: now,
        },
      });
    }
    if (pendingMergeBackTransfer !== undefined && mergeBackResolution !== null) {
      yield* emitEvent({
        type: "context-transfer.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: {
          ...pendingMergeBackTransfer,
          targetProviderInstanceId: modelSelection.instanceId,
          targetRunId: runId,
          status: "consumed",
          resolution: mergeBackResolution,
          error: null,
          updatedAt: now,
          consumedAt: now,
        },
      });
    }
    yield* emitEvent({
      type: "run.created",
      threadId: command.threadId,
      runId,
      nodeId: rootNodeId,
      providerInstanceId: modelSelection.instanceId,
      occurredAt: now,
      payload: run,
    });
    yield* completeSourcePlan(now);
    yield* emitEvent({
      type: "run-attempt.created",
      threadId: command.threadId,
      runId,
      nodeId: rootNodeId,
      providerInstanceId: modelSelection.instanceId,
      occurredAt: now,
      payload: attempt,
    });
    yield* emitEvent({
      type: "node.updated",
      threadId: command.threadId,
      runId,
      nodeId: rootNodeId,
      providerInstanceId: modelSelection.instanceId,
      occurredAt: now,
      payload: rootNode,
    });
    yield* emitEvent({
      type: "checkpoint-scope.created",
      threadId: command.threadId,
      runId,
      nodeId: rootNodeId,
      providerInstanceId: modelSelection.instanceId,
      occurredAt: now,
      payload: checkpointScope,
    });
    if (handoffTurnItem !== null) {
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId,
        nodeId: rootNodeId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: handoffTurnItem,
      });
    }
    yield* emitEvent({
      type: "message.updated",
      threadId: command.threadId,
      runId,
      nodeId: rootNodeId,
      providerInstanceId: modelSelection.instanceId,
      occurredAt: now,
      payload: message,
    });
    yield* emitEvent({
      type: "turn-item.updated",
      threadId: command.threadId,
      runId,
      nodeId: rootNodeId,
      providerInstanceId: modelSelection.instanceId,
      occurredAt: now,
      payload: notificationTurnItem(
        turnItem,
        message,
        yield* notificationTasks(command.threadId, message, projection.subagents),
      ),
    });
    const forkResolution = nativeForkResolution ?? portableForkResolution;
    if (pendingForkTransfer !== undefined && forkResolution !== null) {
      yield* emitEvent({
        type: "context-transfer.updated",
        threadId: command.threadId,
        runId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: {
          ...pendingForkTransfer,
          targetProviderInstanceId: modelSelection.instanceId,
          targetRunId: runId,
          status: "consumed",
          resolution: forkResolution,
          error: null,
          updatedAt: now,
          consumedAt: now,
        },
      });
    }

    const detachDetail = canResumeAcrossInstances
      ? "Provider account changed; continuing the native thread."
      : "Provider or model selection changed.";
    for (const session of sessionsToDetach) {
      yield* emitEvent({
        type: "provider-session.detached",
        threadId: command.threadId,
        driver: session.driver,
        providerInstanceId: session.providerInstanceId,
        occurredAt: now,
        payload: {
          providerSessionId: session.id,
          detachedAt: now,
          reason: detachDetail,
        },
      });
      yield* Ref.update(effects, (existing) => [
        ...existing,
        {
          id: `effect:${command.commandId}:provider-session.detach:${session.id}`,
          commandId: command.commandId,
          threadId: command.threadId,
          request: {
            type: "provider-session.detach",
            providerSessionId: session.id,
            detail: detachDetail,
          },
        } satisfies PendingOrchestrationEffectV2,
      ]);
    }
    const pendingEffect = {
      id: `effect:${command.commandId}:provider-turn.start:${runId}`,
      commandId: command.commandId,
      threadId: command.threadId,
      request: { type: "provider-turn.start", runId },
    } satisfies PendingOrchestrationEffectV2;
    yield* Ref.update(effects, (existing) => [...existing, pendingEffect]);
  });

  const dispatchDelegatedTaskRequest = Effect.fn("orchestrationV2.dispatch.delegatedTaskRequest")(
    function* (
      command: Extract<OrchestrationV2Command, { readonly type: "delegated_task.request" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    ) {
      const parentProjection = yield* projectionStore
        .getRunProviderRecords(command.parentThreadId, command.parentRunId, command.parentNodeId)
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: command.parentThreadId, cause }),
          ),
        );
      const parentRun = parentProjection.runs.find(
        (candidate) => candidate.id === command.parentRunId,
      );
      if (parentRun === undefined || !isActiveRun(parentRun)) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Parent run ${command.parentRunId} is not active.`,
        });
      }
      if (yield* stopReachedRun(command, command.parentThreadId, parentRun.id)) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Parent run ${command.parentRunId} is stopping.`,
        });
      }
      const parentNode = parentProjection.nodes.find(
        (candidate) => candidate.id === command.parentNodeId,
      );
      if (
        parentNode === undefined ||
        parentNode.runId !== parentRun.id ||
        parentRun.rootNodeId === null
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Parent node ${command.parentNodeId} is not part of active run ${parentRun.id}.`,
        });
      }

      const targetAdapter = yield* providerAdapters.get(command.modelSelection.instanceId).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorProviderAdapterError({
              commandId: command.commandId,
              providerInstanceId: command.modelSelection.instanceId,
              cause,
            }),
        ),
      );

      const now = command.createdAt ?? (yield* DateTime.now);
      const taskNodeId = idAllocator.derive.delegatedTaskNode({
        commandId: command.commandId,
      });
      const childThreadId = idAllocator.derive.delegatedTaskThread({
        commandId: command.commandId,
      });
      const childMessageId = idAllocator.derive.delegatedTaskMessage({
        commandId: command.commandId,
      });
      const taskTurnItemId = idAllocator.derive.delegatedTaskTurnItem({
        commandId: command.commandId,
      });
      const taskTitle = subagentThreadTitle({
        parentTitle: parentProjection.thread.title,
        prompt: command.task,
        ...(command.title === undefined ? {} : { title: command.title }),
        ordinal: (yield* subagentCount(command.parentThreadId)) + 1,
      });
      const childThread: OrchestrationV2AppThread = {
        ...makeSubagentChildThread({
          parentThread: parentProjection.thread,
          childThreadId,
          parentNodeId: taskNodeId,
          activeProviderThreadId: null,
          providerInstanceId: command.modelSelection.instanceId,
          modelSelection: command.modelSelection,
          title: taskTitle,
          now,
          createdBy: command.createdBy,
          creationSource: command.creationSource,
        }),
        runtimeMode: command.runtimeMode,
        interactionMode: command.interactionMode,
        ...(command.workspaceStrategy === undefined ? {} : { branch: null, worktreePath: null }),
      };
      const task: OrchestrationV2Subagent = {
        id: taskNodeId,
        threadId: command.parentThreadId,
        runId: parentRun.id,
        parentNodeId: command.parentNodeId,
        origin: "app_owned",
        createdBy: command.createdBy,
        driver: targetAdapter.driver,
        providerInstanceId: command.modelSelection.instanceId,
        providerThreadId: null,
        childThreadId,
        nativeTaskRef: null,
        prompt: command.task,
        title: command.title ?? null,
        model: command.modelSelection.model,
        modelSelection: command.modelSelection,
        ...(command.completionWake === undefined ? {} : { completionWake: command.completionWake }),
        status: "running",
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      };
      const taskNode: OrchestrationV2ExecutionNode = {
        id: taskNodeId,
        threadId: command.parentThreadId,
        runId: parentRun.id,
        parentNodeId: command.parentNodeId,
        rootNodeId: parentRun.rootNodeId,
        kind: "subagent",
        status: "running",
        countsForRun: false,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: now,
        completedAt: null,
      };
      const parentProviderTurn = providerTurnForRun(parentProjection, parentRun);
      const taskTurnItem: OrchestrationV2TurnItem = {
        id: taskTurnItemId,
        threadId: command.parentThreadId,
        runId: parentRun.id,
        nodeId: taskNodeId,
        providerThreadId: parentRun.providerThreadId,
        providerTurnId: parentProviderTurn?.id ?? null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: yield* nextTurnItemOrdinal(parentProjection),
        status: "running",
        title: command.title ?? taskTitle,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "subagent",
        subagentId: taskNodeId,
        origin: "app_owned",
        driver: targetAdapter.driver,
        providerInstanceId: command.modelSelection.instanceId,
        childThreadId,
        prompt: command.task,
        result: null,
      };
      const emitEvent = emit(events, command);

      yield* emitEvent({
        type: "thread.created",
        threadId: childThreadId,
        driver: targetAdapter.driver,
        providerInstanceId: command.modelSelection.instanceId,
        occurredAt: now,
        payload: childThread,
      });
      yield* emitEvent({
        type: "node.updated",
        threadId: command.parentThreadId,
        runId: parentRun.id,
        nodeId: taskNodeId,
        driver: targetAdapter.driver,
        providerInstanceId: command.modelSelection.instanceId,
        occurredAt: now,
        payload: taskNode,
      });
      yield* emitEvent({
        type: "subagent.updated",
        threadId: command.parentThreadId,
        runId: parentRun.id,
        nodeId: taskNodeId,
        driver: targetAdapter.driver,
        providerInstanceId: command.modelSelection.instanceId,
        occurredAt: now,
        payload: task,
      });
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.parentThreadId,
        runId: parentRun.id,
        nodeId: taskNodeId,
        driver: targetAdapter.driver,
        providerInstanceId: command.modelSelection.instanceId,
        occurredAt: now,
        payload: taskTurnItem,
      });

      const childMessageCommand = {
        type: "message.dispatch",
        createdBy: command.createdBy,
        creationSource: command.creationSource,
        commandId: command.commandId,
        threadId: childThreadId,
        senderThreadId: command.parentThreadId,
        messageId: childMessageId,
        text: command.task,
        attachments: [],
        modelSelection: command.modelSelection,
        dispatchMode:
          command.workspaceStrategy === undefined
            ? { type: "start_immediately" }
            : { type: "defer_start", workspaceStrategy: command.workspaceStrategy },
      } satisfies Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>;
      yield* dispatchMessage(childMessageCommand, events, effects);

      const childProjection = yield* getProjectionWithPendingEvents(childThreadId, events);
      const childRun = childProjection.runs[0];
      if (childRun === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Delegated child thread ${childThreadId} did not create a run.`,
        });
      }
      const spawnTransferId = yield* mapDispatchError(command)(
        idAllocator.allocate.contextTransfer({
          sourceThreadId: command.parentThreadId,
          targetThreadId: childThreadId,
          type: "subagent_spawn",
        }),
      );
      const spawnTransfer: OrchestrationV2ContextTransfer = {
        id: spawnTransferId,
        type: "subagent_spawn",
        sourceThreadId: command.parentThreadId,
        targetThreadId: childThreadId,
        sourcePoint: {
          ...contextSourcePointForRun(parentProjection, parentRun),
          turnItemId: taskTurnItemId,
        },
        basePoint: null,
        sourceProviderInstanceId: parentRun.providerInstanceId,
        targetProviderInstanceId: command.modelSelection.instanceId,
        targetRunId: childRun.id,
        status: "consumed",
        resolution: null,
        createdBy: command.createdBy,
        error: null,
        createdAt: now,
        updatedAt: now,
        consumedAt: now,
      };
      yield* emitEvent({
        type: "context-transfer.created",
        threadId: childThreadId,
        runId: childRun.id,
        providerInstanceId: command.modelSelection.instanceId,
        occurredAt: now,
        payload: spawnTransfer,
      });
    },
  );

  const recordSubagentFollowup = Effect.fnUntraced(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "message.dispatch" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    if (!mayStartSubagentFollowup(command)) return;
    const pending = yield* Ref.get(events);
    const message = pending
      .flatMap((event) =>
        event.type === "message.updated" && event.payload.id === command.messageId
          ? [event.payload]
          : [],
      )
      .at(-1);
    if (message?.runId == null) return;
    // Only new attempts can own tasks; steers join an existing snapshot that cannot settle newly bound tasks.
    const startsAttempt = pending.some(
      (event) =>
        (event.type === "run.created" && event.payload.id === message.runId) ||
        (event.type === "run-attempt.created" && event.payload.runId === message.runId),
    );
    if (!startsAttempt) return;
    const child = yield* projectionStore.getThread(command.threadId);
    const parentThreadId = child.lineage.parentThreadId;
    // A send with no sender thread comes from a client acting for the owner, like a user send.
    if (
      child.lineage.relationshipToParent !== "subagent" ||
      parentThreadId === null ||
      (command.notification === undefined &&
        command.createdBy !== "user" &&
        command.usageLimitContinuationOfRunId === undefined &&
        command.senderThreadId !== undefined &&
        command.senderThreadId !== parentThreadId)
    )
      return;
    const run =
      pending
        .flatMap((event) =>
          (event.type === "run.created" || event.type === "run.updated") &&
          event.payload.id === message.runId
            ? [event.payload]
            : [],
        )
        .at(-1) ??
      (yield* projectionStore.getThreadRecords(command.threadId, ["runs"])).runs.find(
        (row) => row.id === message.runId,
      );
    if (run === undefined || run.delegatedTaskId !== undefined) return;
    // A wake continues the latest follow-up while it is open, as it continues the original task.
    const latestFollowupId =
      command.notification === undefined
        ? undefined
        : (yield* projectionStore
            .getThreadRecords(command.threadId, ["runs"])
            .pipe(mapDispatchError(command))).runs
            .filter((row) => row.id !== run.id && row.delegatedTaskId !== undefined)
            .toSorted((a, b) => b.ordinal - a.ordinal)[0]?.delegatedTaskId;
    const parent = yield* projectionStore
      .getThreadRecords(parentThreadId, ["runs", "subagents", "providerTurns", "attempts"], {
        subagentIds: [
          ...(child.forkedFrom?.type === "node" ? [child.forkedFrom.nodeId] : []),
          ...(latestFollowupId === undefined ? [] : [latestFollowupId]),
        ],
      })
      .pipe(mapDispatchError(command));
    const original = parent.subagents.find(
      (task) =>
        task.origin === "app_owned" &&
        task.childThreadId === child.id &&
        task.id === (child.forkedFrom?.type === "node" ? child.forkedFrom.nodeId : undefined),
    );
    if (original === undefined || original.result === null) return;
    const openFollowup = parent.subagents.find(
      (task) =>
        task.id === latestFollowupId &&
        task.origin === "app_owned" &&
        task.childThreadId === child.id &&
        task.cancelledRunId === undefined &&
        task.result === null,
    );
    if (openFollowup !== undefined) {
      yield* emit(
        events,
        command,
      )({
        type: "run.updated",
        threadId: child.id,
        runId: run.id,
        occurredAt: message.createdAt,
        payload: { ...run, delegatedTaskId: openFollowup.id },
      });
      return;
    }
    const owner = followUpOwnerRun(parent.runs);
    if (owner === null || owner.rootNodeId === null) return;
    const now = message.createdAt;
    const adapter = yield* providerAdapters.get(run.providerInstanceId);
    const taskId = idAllocator.derive.delegatedTaskNode({ commandId: command.commandId });
    const task: OrchestrationV2Subagent = {
      ...original,
      createdBy: command.createdBy,
      driver: adapter.driver,
      id: taskId,
      runId: owner.id,
      parentNodeId: owner.rootNodeId,
      providerInstanceId: run.providerInstanceId,
      providerThreadId: run.providerThreadId,
      model: run.modelSelection.model,
      modelSelection: run.modelSelection,
      prompt: command.text,
      title: child.title,
      completionWake: "always",
      completionDelivery: { state: "pending", observedByRunId: null },
      status: run.status === "queued" ? (run.queueHeld === true ? "idle" : "pending") : "running",
      progress: undefined,
      cancelledRunId: undefined,
      result: null,
      startedAt: now,
      completedAt: null,
      updatedAt: now,
    };
    const node: OrchestrationV2ExecutionNode = {
      id: taskId,
      threadId: parentThreadId,
      runId: owner.id,
      parentNodeId: owner.rootNodeId,
      rootNodeId: owner.rootNodeId,
      kind: "subagent",
      status: task.status,
      countsForRun: false,
      providerThreadId: run.providerThreadId,
      providerTurnId: null,
      nativeItemRef: null,
      runtimeRequestId: null,
      checkpointScopeId: null,
      startedAt: now,
      completedAt: null,
    };
    const turnItem: OrchestrationV2TurnItem = {
      id: idAllocator.derive.delegatedTaskTurnItem({ commandId: command.commandId }),
      threadId: parentThreadId,
      runId: owner.id,
      nodeId: taskId,
      providerThreadId: owner.providerThreadId,
      providerTurnId: providerTurnForRun(parent, owner)?.id ?? null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: yield* nextTurnItemOrdinal(parent),
      status: task.status,
      title: child.title,
      startedAt: now,
      completedAt: null,
      updatedAt: now,
      type: "subagent",
      subagentId: taskId,
      origin: "app_owned",
      driver: task.driver,
      providerInstanceId: task.providerInstanceId,
      childThreadId: child.id,
      prompt: command.text,
      result: null,
    };
    const emitEvent = emit(events, command);
    yield* emitEvent({
      type: "run.updated",
      threadId: child.id,
      runId: run.id,
      occurredAt: now,
      payload: { ...run, delegatedTaskId: taskId },
    });
    yield* emitEvent({
      type: "node.updated",
      threadId: parentThreadId,
      runId: owner.id,
      nodeId: taskId,
      occurredAt: now,
      payload: node,
    });
    yield* emitEvent({
      type: "subagent.updated",
      threadId: parentThreadId,
      runId: owner.id,
      nodeId: taskId,
      occurredAt: now,
      payload: task,
    });
    yield* emitEvent({
      type: "turn-item.updated",
      threadId: parentThreadId,
      runId: owner.id,
      nodeId: taskId,
      occurredAt: now,
      payload: turnItem,
    });
  });

  /**
   * Records a task that runs in a linked environment on its parent: the
   * task, its node and its turn item, as a local delegation does, but no child
   * thread here. The parent's own checks are the local request's.
   */
  const dispatchRemoteDelegatedTaskRequest = Effect.fn(
    "orchestrationV2.dispatch.remoteDelegatedTaskRequest",
  )(function* (
    command: Extract<
      OrchestrationV2InternalCommand,
      { readonly type: "delegated_task.remote.request" }
    >,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const parentProjection = yield* projectionStore
      .getThreadRecords(command.parentThreadId, ["runs", "providerTurns", "attempts", "subagents"])
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: command.parentThreadId, cause }),
        ),
      );
    const parentRun = parentProjection.runs.find(
      (candidate) => candidate.id === command.parentRunId,
    );
    // A follow-up to a finished task there binds as one to a child here does.
    const followsUp = parentProjection.subagents.some(
      (task) =>
        task.result !== null &&
        task.remoteChild?.environmentId === command.remoteChild.environmentId &&
        task.remoteChild.threadId === command.remoteChild.threadId,
    );
    if (
      parentRun === undefined ||
      (followsUp
        ? parentRun.id !== followUpOwnerRun(parentProjection.runs)?.id
        : !isActiveRun(parentRun)) ||
      parentRun.rootNodeId === null
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Parent run ${command.parentRunId} is not active.`,
      });
    }
    if (yield* stopReachedRun(command, command.parentThreadId, parentRun.id)) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Parent run ${command.parentRunId} is stopping.`,
      });
    }
    const now = yield* DateTime.now;
    const taskNodeId = idAllocator.derive.delegatedTaskNode({ commandId: command.commandId });
    const taskTurnItemId = idAllocator.derive.delegatedTaskTurnItem({
      commandId: command.commandId,
    });
    const taskTitle = subagentThreadTitle({
      parentTitle: parentProjection.thread.title,
      prompt: command.task,
      ...(command.title === undefined ? {} : { title: command.title }),
      ordinal: (yield* subagentCount(command.parentThreadId)) + 1,
    });
    const common = {
      threadId: command.parentThreadId,
      runId: parentRun.id,
      nodeId: taskNodeId,
      driver: command.driver,
      providerInstanceId: command.modelSelection.instanceId,
      occurredAt: now,
    };
    const emitEvent = emit(events, command);
    yield* emitEvent({
      ...common,
      type: "node.updated",
      payload: {
        id: taskNodeId,
        threadId: command.parentThreadId,
        runId: parentRun.id,
        parentNodeId: command.parentNodeId,
        rootNodeId: parentRun.rootNodeId,
        kind: "subagent",
        status: "running",
        countsForRun: false,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        runtimeRequestId: null,
        checkpointScopeId: null,
        startedAt: now,
        completedAt: null,
      },
    });
    yield* emitEvent({
      ...common,
      type: "subagent.updated",
      payload: {
        id: taskNodeId,
        threadId: command.parentThreadId,
        runId: parentRun.id,
        parentNodeId: command.parentNodeId,
        origin: "app_owned",
        createdBy: "agent",
        driver: command.driver,
        providerInstanceId: command.modelSelection.instanceId,
        providerThreadId: null,
        childThreadId: null,
        remoteChild: command.remoteChild,
        nativeTaskRef: null,
        prompt: command.task,
        title: command.title ?? null,
        model: command.modelSelection.model,
        modelSelection: command.modelSelection,
        ...(command.completionWake === undefined ? {} : { completionWake: command.completionWake }),
        status: "running",
        result: null,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
      },
    });
    yield* emitEvent({
      ...common,
      type: "turn-item.updated",
      payload: {
        id: taskTurnItemId,
        threadId: command.parentThreadId,
        runId: parentRun.id,
        nodeId: taskNodeId,
        providerThreadId: parentRun.providerThreadId,
        providerTurnId: providerTurnForRun(parentProjection, parentRun)?.id ?? null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: yield* nextTurnItemOrdinal(parentProjection),
        status: "running",
        title: command.title ?? taskTitle,
        startedAt: now,
        completedAt: null,
        updatedAt: now,
        type: "subagent",
        subagentId: taskNodeId,
        origin: "app_owned",
        driver: command.driver,
        providerInstanceId: command.modelSelection.instanceId,
        childThreadId: null,
        remoteChild: command.remoteChild,
        prompt: command.task,
        result: null,
      },
    });
  });

  /**
   * Completes a task that ran in a linked environment: the parent half of
   * `finalizeAppOwnedSubagent`, with the result its thread there ended with.
   * It runs under the parent's lock, so it never interleaves with a wake
   * policy change or a delivery on the same task.
   */
  const dispatchRemoteDelegatedTaskComplete = Effect.fn(
    "orchestrationV2.dispatch.remoteDelegatedTaskComplete",
  )(function* (
    command: Extract<
      OrchestrationV2InternalCommand,
      { readonly type: "delegated_task.remote.complete" }
    >,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    // Delivery planning reads only this task and the undelivered tasks of the queued delivery;
    // the task's own node and turn item are all it updates besides.
    const parentProjection = yield* projectionStore
      .getThreadRecords(
        command.parentThreadId,
        ["runs", "messages", "subagents", "providerTurns", "nodes", "turnItems"],
        {
          turnItemTypes: ["subagent"],
          messageRoles: ["user"],
          omitSettledSubagents: { except: [command.taskId] },
          nodeIds: [command.taskId],
          turnItemNodeIds: [command.taskId],
        },
      )
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: command.parentThreadId, cause }),
        ),
      );
    const task = parentProjection.subagents.find(
      (candidate) =>
        candidate.id === command.taskId &&
        candidate.origin === "app_owned" &&
        candidate.remoteChild !== undefined,
    );
    if (task === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Delegated task ${command.taskId} is not a remote task of thread ${command.parentThreadId}.`,
      });
    }
    if (
      command.stoppedThere === "stopped" &&
      (command.stopCount ?? 0) !== (task.remoteChild?.stopCount ?? 0)
    )
      return;
    const remoteChild =
      command.stoppedThere === undefined
        ? task.remoteChild
        : { ...task.remoteChild!, stoppedThere: true };
    // The first result stands; a repeated follower report records nothing, which
    // the dispatcher refuses. A stop there after the result is still recorded, but
    // a run that ended by itself does not settle a stop the task here still owes.
    if (task.result !== null) {
      // A Stop or cancel here that lost the race to the result still owes its stop.
      if (command.stoppedThere === undefined) {
        return yield* emitRemoteStopOwed(events, command, task, task.remoteChild!);
      }
      const owesStop =
        task.status === "cancelled" ||
        task.status === "interrupted" ||
        task.remoteChild?.stoppedThere === false;
      if (
        task.remoteChild?.stoppedThere === true ||
        (owesStop && command.stoppedThere !== "stopped")
      )
        return;
      const now = yield* DateTime.now;
      yield* emit(
        events,
        command,
      )({
        type: "subagent.updated",
        threadId: command.parentThreadId,
        ...(task.runId === null ? {} : { runId: task.runId }),
        nodeId: task.id,
        driver: task.driver,
        occurredAt: now,
        payload: { ...task, remoteChild, updatedAt: now },
      });
      return;
    }
    const now = yield* DateTime.now;
    const updatedTask: OrchestrationV2Subagent = {
      ...task,
      remoteChild,
      status: command.status,
      result: command.result,
      ...(command.executedModel === undefined ? {} : { executedModel: command.executedModel }),
      completedAt: now,
      updatedAt: now,
    };
    const parentRun =
      task.runId === null
        ? undefined
        : parentProjection.runs.find((candidate) => candidate.id === task.runId);
    const plan = yield* planDelegatedCompletionDelivery({
      parentProjection,
      parentRun,
      task,
      updatedTask,
      now,
    }).pipe(mapDispatchError(command));
    const emitEvent = emit(events, command);
    yield* emitEvent({
      type: "subagent.updated",
      threadId: command.parentThreadId,
      ...(task.runId === null ? {} : { runId: task.runId }),
      nodeId: task.id,
      driver: task.driver,
      occurredAt: now,
      payload: plan.task,
    });
    for (const event of delegatedCompletionPlanEvents(
      command.parentThreadId,
      plan,
      parentProjection.thread.providerInstanceId,
      now,
    )) {
      yield* emitEvent(event);
    }
    const node = parentProjection.nodes.find((candidate) => candidate.id === task.id);
    if (node !== undefined) {
      yield* emitEvent({
        type: "node.updated",
        threadId: command.parentThreadId,
        ...(node.runId === null ? {} : { runId: node.runId }),
        nodeId: node.id,
        driver: task.driver,
        occurredAt: now,
        payload: { ...node, status: command.status, completedAt: now },
      });
    }
    const item = parentProjection.turnItems.find(
      (candidate) => candidate.type === "subagent" && candidate.subagentId === task.id,
    );
    if (item !== undefined) {
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.parentThreadId,
        ...(item.runId === null ? {} : { runId: item.runId }),
        ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
        driver: task.driver,
        occurredAt: now,
        payload: {
          ...item,
          status: command.status,
          result: command.result,
          completedAt: now,
          updatedAt: now,
        } as OrchestrationV2TurnItem,
      });
    }
  });

  /**
   * Owes a stop to the thread there of a remote task that already has its
   * result. Only `remoteChild.stoppedThere` changes, so the result and its
   * delivery stand; the follower sends the stop and clears it.
   */
  const dispatchRemoteDelegatedTaskStop = Effect.fn(
    "orchestrationV2.dispatch.remoteDelegatedTaskStop",
  )(function* (
    command: Extract<
      OrchestrationV2InternalCommand,
      { readonly type: "delegated_task.remote.stop" }
    >,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const { subagents } = yield* projectionStore
      .getThreadRecords(command.parentThreadId, ["subagents"])
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: command.parentThreadId, cause }),
        ),
      );
    const task = subagents.find(
      (candidate) =>
        candidate.id === command.taskId &&
        candidate.origin === "app_owned" &&
        candidate.remoteChild !== undefined &&
        candidate.result !== null,
    );
    if (task?.remoteChild === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Delegated task ${command.taskId} is not a finished remote task of thread ${command.parentThreadId}.`,
      });
    }
    yield* emitRemoteStopOwed(events, command, task, task.remoteChild);
  });

  const emitRemoteStopOwed = Effect.fn("orchestrationV2.dispatch.remoteStopOwed")(function* (
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    command: Extract<
      OrchestrationV2InternalCommand,
      { readonly type: "delegated_task.remote.complete" | "delegated_task.remote.stop" }
    >,
    task: OrchestrationV2Subagent,
    remoteChild: OrchestrationV2RemoteTaskChild,
  ) {
    const now = yield* DateTime.now;
    yield* emit(
      events,
      command,
    )({
      type: "subagent.updated",
      threadId: command.parentThreadId,
      ...(task.runId === null ? {} : { runId: task.runId }),
      nodeId: task.id,
      driver: task.driver,
      occurredAt: now,
      payload: {
        ...task,
        remoteChild: {
          ...remoteChild,
          stoppedThere: false,
          stopCount: (remoteChild.stopCount ?? 0) + 1,
        },
        updatedAt: now,
      },
    });
  });

  // After blocking waits time out, enable later completions to wake the parent.
  // Hold the parent dispatch lock shared with finalization to serialize full-row writes.
  const dispatchDelegatedTaskWakePolicy = Effect.fn(
    "orchestrationV2.dispatch.delegatedTaskWakePolicy",
  )(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "delegated_task.wake-policy" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    // Delivery planning reads only this task and the undelivered tasks of the queued delivery.
    const parentProjection = yield* projectionStore
      .getThreadRecords(
        command.parentThreadId,
        ["subagents", "runs", "messages", "providerTurns"],
        {
          omitSettledSubagents: { except: [command.taskId] },
        },
      )
      .pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorProjectionError({
              threadId: command.parentThreadId,
              cause,
            }),
        ),
      );
    const task = parentProjection.subagents.find(
      (candidate) => candidate.id === command.taskId && candidate.origin === "app_owned",
    );
    if (task === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Delegated task ${command.taskId} is not an app-owned task of thread ${command.parentThreadId}.`,
      });
    }
    if (task.completionWake === command.completionWake) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Delegated task ${command.taskId} already wakes the parent with completionWake ${command.completionWake}.`,
      });
    }
    const now = yield* DateTime.now;
    const emitEvent = emit(events, command);
    const updatedTask: OrchestrationV2Subagent = {
      ...task,
      completionWake: command.completionWake,
      updatedAt: now,
    };
    // Finalization already offered for ended parent runs; another offer would duplicate the wake.
    const parentRun =
      task.runId === null
        ? undefined
        : parentProjection.runs.find((candidate) => candidate.id === task.runId);
    const completionPlan =
      command.completionWake === "always" &&
      isTerminalDelegatedTaskStatus(task.status) &&
      parentRun !== undefined &&
      hasLiveRun({ runs: [parentRun] })
        ? yield* planDelegatedCompletionDelivery({
            parentProjection,
            parentRun,
            task: updatedTask,
            updatedTask,
            now,
          })
        : undefined;
    yield* emitEvent({
      type: "subagent.updated",
      threadId: command.parentThreadId,
      ...(task.runId === null ? {} : { runId: task.runId }),
      nodeId: task.id,
      driver: task.driver,
      providerInstanceId: task.providerInstanceId,
      occurredAt: now,
      payload: completionPlan?.task ?? updatedTask,
    });
    if (completionPlan === undefined) {
      return;
    }
    if (completionPlan.parentRun !== undefined) {
      yield* emitEvent({
        type: "run.updated",
        threadId: command.parentThreadId,
        runId: completionPlan.parentRun.id,
        ...(completionPlan.parentRun.rootNodeId === null
          ? {}
          : { nodeId: completionPlan.parentRun.rootNodeId }),
        providerInstanceId: completionPlan.parentRun.providerInstanceId,
        occurredAt: now,
        payload: completionPlan.parentRun,
      });
    }
    if (completionPlan.message !== undefined) {
      yield* emitEvent({
        type: "message.updated",
        threadId: command.parentThreadId,
        ...(completionPlan.message.runId === null ? {} : { runId: completionPlan.message.runId }),
        ...(completionPlan.message.nodeId === null
          ? {}
          : { nodeId: completionPlan.message.nodeId }),
        providerInstanceId:
          completionPlan.parentRun?.providerInstanceId ??
          parentProjection.thread.providerInstanceId,
        occurredAt: now,
        payload: completionPlan.message,
      });
    }
    return completionPlan.parentRun?.id;
  });

  const dispatchCreatedThreadRecord = Effect.fn("orchestrationV2.dispatch.createdThreadRecord")(
    function* (
      command: Extract<OrchestrationV2Command, { readonly type: "thread.created.record" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    ) {
      const parentProjection = yield* projectionStore
        .getThreadRecords(
          command.parentThreadId,
          ["runs", "nodes", "turnItems", "attempts", "providerTurns"],
          { turnItemTypes: [], messageRoles: ["user"] },
        )
        .pipe(
          Effect.mapError(
            (cause) =>
              new OrchestratorProjectionError({
                threadId: command.parentThreadId,
                cause,
              }),
          ),
        );
      const targetProjection = yield* projectionStore
        .getThreadRecords(
          command.targetThreadId,
          ["runs", "nodes", "turnItems", "attempts", "providerTurns"],
          { turnItemTypes: [], messageRoles: ["user"] },
        )
        .pipe(
          Effect.mapError(
            (cause) =>
              new OrchestratorProjectionError({
                threadId: command.targetThreadId,
                cause,
              }),
          ),
        );
      const parentRun = parentProjection.runs.find(
        (candidate) => candidate.id === command.parentRunId,
      );
      const parentNode = parentProjection.nodes.find(
        (candidate) => candidate.id === command.parentNodeId,
      );
      if (
        parentRun === undefined ||
        parentNode === undefined ||
        parentNode.runId !== command.parentRunId ||
        parentRun.rootNodeId !== command.parentNodeId
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Parent node ${command.parentNodeId} is not the root of run ${command.parentRunId}.`,
        });
      }
      if (parentProjection.thread.projectId !== targetProjection.thread.projectId) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Target thread ${command.targetThreadId} belongs to another project.`,
        });
      }
      if (
        command.targetRunId !== null &&
        !targetProjection.runs.some((candidate) => candidate.id === command.targetRunId)
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Target run ${command.targetRunId} does not belong to thread ${command.targetThreadId}.`,
        });
      }

      const now = yield* DateTime.now;
      const parentProviderTurn = providerTurnForRun(parentProjection, parentRun);
      const turnItem: OrchestrationV2TurnItem = {
        id: idAllocator.derive.createdThreadTurnItem({ commandId: command.commandId }),
        threadId: command.parentThreadId,
        runId: command.parentRunId,
        nodeId: command.parentNodeId,
        providerThreadId: parentRun.providerThreadId,
        providerTurnId: parentProviderTurn?.id ?? null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: yield* nextTurnItemOrdinal(parentProjection),
        status: "completed",
        title: targetProjection.thread.title,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "thread_created",
        targetThreadId: command.targetThreadId,
        targetRunId: command.targetRunId,
        targetProviderInstanceId: targetProjection.thread.modelSelection.instanceId,
        targetModel: targetProjection.thread.modelSelection.model,
      };

      yield* emit(
        events,
        command,
      )({
        type: "turn-item.updated",
        threadId: command.parentThreadId,
        runId: command.parentRunId,
        nodeId: command.parentNodeId,
        providerInstanceId: parentRun.providerInstanceId,
        occurredAt: now,
        payload: turnItem,
      });
    },
  );

  const dispatchSecretRequestRecord = Effect.fn("orchestrationV2.dispatch.secretRequestRecord")(
    function* (
      command: Extract<OrchestrationV2InternalCommand, { readonly type: "secret_request.record" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    ) {
      const projection = yield* projectionStore
        .getThreadRecords(
          command.threadId,
          ["runs", "nodes", "turnItems", "attempts", "providerTurns"],
          { turnItemTypes: ["secret_request"], messageRoles: [] },
        )
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
          ),
        );
      const run = projection.runs.find((candidate) => candidate.id === command.runId);
      if (run === undefined || run.rootNodeId !== command.nodeId) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Node ${command.nodeId} is not the root of run ${command.runId}.`,
        });
      }
      const existing = projection.turnItems.find((item) => item.id === command.turnItemId);
      if (existing !== undefined && existing.type !== "secret_request") {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Turn item ${command.turnItemId} is not a secret request.`,
        });
      }
      if (existing !== undefined && existing.runId !== command.runId) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Secret request ${command.turnItemId} belongs to another run.`,
        });
      }
      const now = yield* DateTime.now;
      const unchanged =
        existing !== undefined &&
        (existing.secretStatus !== "pending" || command.secretStatus === "pending");
      const pending = command.secretStatus === "pending";
      const turnItem: OrchestrationV2TurnItem = unchanged
        ? existing
        : {
            id: command.turnItemId,
            threadId: command.threadId,
            runId: command.runId,
            nodeId: command.nodeId,
            providerThreadId: run.providerThreadId,
            providerTurnId: providerTurnForRun(projection, run)?.id ?? null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: existing?.ordinal ?? (yield* nextTurnItemOrdinal(projection)),
            status: pending
              ? "waiting"
              : command.secretStatus === "saved"
                ? "completed"
                : "cancelled",
            title: command.label,
            startedAt: existing?.startedAt ?? now,
            completedAt: pending ? null : now,
            updatedAt: now,
            type: "secret_request",
            label: command.label,
            reason: command.reason,
            ...(command.url === undefined ? {} : { url: command.url }),
            ...(command.placeholder === undefined ? {} : { placeholder: command.placeholder }),
            ...(command.delivery === undefined ? {} : { delivery: command.delivery }),
            secretStatus: command.secretStatus,
          };
      if (
        !unchanged &&
        command.secretStatus === "saved" &&
        projection.thread.snoozedUntil != null
      ) {
        yield* emit(
          events,
          command,
        )({
          type: "thread.unsnoozed",
          threadId: command.threadId,
          providerInstanceId: projection.thread.providerInstanceId,
          occurredAt: now,
          payload: { ...projection.thread, snoozedUntil: null, snoozedAt: null, updatedAt: now },
        });
      }
      yield* emit(
        events,
        command,
      )({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId: command.runId,
        nodeId: command.nodeId,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: turnItem,
      });
    },
  );

  /**
   * Records or updates the card for a link an agent asked the user to make.
   * The pairing code goes straight to the link and never through orchestration.
   */
  const dispatchLinkRequestRecord = Effect.fn("orchestrationV2.dispatch.linkRequestRecord")(
    function* (
      command: Extract<OrchestrationV2InternalCommand, { readonly type: "link_request.record" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    ) {
      const projection = yield* projectionStore
        .getThreadRecords(
          command.threadId,
          ["runs", "nodes", "turnItems", "attempts", "providerTurns"],
          { turnItemTypes: ["link_request"], messageRoles: [] },
        )
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
          ),
        );
      const run = projection.runs.find((candidate) => candidate.id === command.runId);
      if (run === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Run ${command.runId} was not found.`,
        });
      }
      const existing = projection.turnItems.find((item) => item.id === command.turnItemId);
      if (existing !== undefined && existing.type !== "link_request") {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Turn item ${command.turnItemId} is not a link request.`,
        });
      }
      if (existing !== undefined && existing.runId !== command.runId) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Link request ${command.turnItemId} belongs to another run.`,
        });
      }
      // Only a new card needs its run's current root and a live run. An answer
      // may land long after a steer restarted the run or the run ended, and the
      // link it reports is already made.
      if (existing === undefined && run.rootNodeId !== command.nodeId) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Node ${command.nodeId} is not the root of run ${command.runId}.`,
        });
      }
      if (
        existing === undefined &&
        (!isActiveRun(run) || (yield* stopReachedRun(command, command.threadId, run.id)))
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Run ${command.runId} is not active.`,
        });
      }
      const nodeId = existing?.nodeId ?? command.nodeId;
      const now = yield* DateTime.now;
      // A request is answered once, and a retry finds its card as it was
      // asked: either one records the card unchanged.
      const unchanged =
        existing !== undefined &&
        (existing.linkStatus !== "pending" || command.linkStatus === "pending");
      const pending = command.linkStatus === "pending";
      const {
        type: _type,
        commandId: _commandId,
        turnItemId: _turnItemId,
        answeringTurnItemIds,
        wake,
        ...fields
      } = command;
      const turnItem: OrchestrationV2TurnItem = unchanged
        ? existing
        : {
            ...fields,
            id: command.turnItemId,
            // An update keeps where the card was asked, even after a restart.
            ...(existing === undefined
              ? {
                  providerThreadId: run.providerThreadId,
                  providerTurnId: providerTurnForRun(projection, run)?.id ?? null,
                }
              : {
                  nodeId: existing.nodeId,
                  providerThreadId: existing.providerThreadId,
                  providerTurnId: existing.providerTurnId,
                }),
            nativeItemRef: null,
            parentItemId: null,
            ordinal: existing?.ordinal ?? (yield* nextTurnItemOrdinal(projection)),
            status: pending
              ? "waiting"
              : command.linkStatus === "linked"
                ? "completed"
                : command.linkStatus === "failed"
                  ? "failed"
                  : "cancelled",
            title: command.label ?? command.url ?? "Link an environment",
            startedAt: existing?.startedAt ?? now,
            completedAt: pending ? null : now,
            updatedAt: now,
            type: "link_request",
          };
      if (!unchanged && command.linkStatus === "linked" && projection.thread.snoozedUntil != null) {
        yield* emit(
          events,
          command,
        )({
          type: "thread.unsnoozed",
          threadId: command.threadId,
          providerInstanceId: projection.thread.providerInstanceId,
          occurredAt: now,
          payload: { ...projection.thread, snoozedUntil: null, snoozedAt: null, updatedAt: now },
        });
      }
      yield* emit(
        events,
        command,
      )({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId: command.runId,
        nodeId,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: turnItem,
      });
      // The link answers the older cards that asked for the same machine and
      // access. They end without waking anyone: the agent hears this link.
      // A card still being answered stays open for its own answer to end.
      if (!unchanged && command.linkStatus === "linked") {
        for (const item of projection.turnItems) {
          if (
            item.type !== "link_request" ||
            item.linkStatus !== "pending" ||
            item.ordinal >= turnItem.ordinal ||
            answeringTurnItemIds?.includes(item.id) === true ||
            !askedForLinkedMachine(item, command)
          ) {
            continue;
          }
          yield* emit(
            events,
            command,
          )({
            type: "turn-item.updated",
            threadId: command.threadId,
            ...(item.runId === null ? {} : { runId: item.runId }),
            ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
            occurredAt: now,
            payload: {
              ...item,
              status: "cancelled",
              linkStatus: "cancelled",
              completedAt: now,
              updatedAt: now,
            },
          });
        }
      }
      // The agent asked without waiting, so the answer reaches it as a
      // notification: it starts a turn, or follows the one running. Only the
      // answer that closes the card wakes it, and never a closed thread.
      if (
        wake === undefined ||
        unchanged ||
        projection.thread.archivedAt !== null ||
        projection.thread.deletedAt !== null
      ) {
        return;
      }
      yield* dispatchMessage(
        {
          type: "message.dispatch",
          commandId: command.commandId,
          threadId: command.threadId,
          messageId: wake.messageId,
          text: wake.text,
          notification: wake.notification,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        },
        events,
        effects,
      );
    },
  );

  /**
   * Closes the link cards still waiting for the user. Their agent stopped
   * waiting long ago, so nobody is woken: stopping or archiving the thread
   * means the user is done with it.
   */
  const closePendingLinkRequests = Effect.fn("orchestrationV2.closePendingLinkRequests")(
    function* (input: {
      readonly command: OrchestrationV2ServerCommand;
      readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
      readonly threadId: ThreadId;
      readonly now: DateTime.Utc;
    }) {
      const { turnItems } = yield* projectionStore
        .getThreadRecords(input.threadId, ["turnItems"], {
          turnItemTypes: ["link_request"],
          turnItemStatuses: ["waiting"],
          messageRoles: [],
        })
        .pipe(mapDispatchError(input.command));
      for (const item of turnItems) {
        if (item.type !== "link_request" || item.linkStatus !== "pending") continue;
        yield* emit(
          input.events,
          input.command,
        )({
          type: "turn-item.updated",
          threadId: input.threadId,
          ...(item.runId === null ? {} : { runId: item.runId }),
          ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
          occurredAt: input.now,
          payload: {
            ...item,
            status: "cancelled",
            linkStatus: "cancelled",
            completedAt: input.now,
            updatedAt: input.now,
          },
        });
      }
    },
  );

  const dispatchRuntimeRequestRespond = Effect.fn("Orchestrator.dispatchRuntimeRequestRespond")(
    function* (
      command: Extract<OrchestrationV2Command, { readonly type: "runtime-request.respond" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    ) {
      const context = yield* projectionStore
        .getRuntimeResponseContext(command.threadId, command.requestId)
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
          ),
        );
      const runtimeRequest = context.request;
      if (runtimeRequest === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Runtime request ${command.requestId} was not found.`,
        });
      }
      if (runtimeRequest.status !== "pending") {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Runtime request ${command.requestId} is ${runtimeRequest.status}.`,
        });
      }
      if (runtimeRequest.responseCapability.type === "not_resumable") {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: runtimeRequest.responseCapability.reason,
        });
      }
      if (
        command.feedback !== undefined &&
        (runtimeRequest.responseCapability.type !== "live" ||
          !runtimeRequest.responseCapability.supportsFeedback ||
          runtimeRequest.kind === "user_input" ||
          command.decision !== "decline" ||
          !command.feedback.trim())
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "Feedback requires a live approval that supports it and a decline decision.",
        });
      }
      const providerSessionId =
        runtimeRequest.responseCapability.type === "live"
          ? runtimeRequest.responseCapability.providerSessionId
          : null;
      const providerSession = context.session;
      if (providerSessionId !== null && providerSession === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Provider session ${providerSessionId} was not found.`,
        });
      }
      let answers = command.answers;
      if (runtimeRequest.kind === "user_input" && answers !== undefined) {
        const decoded = yield* decodeUserInputAnswers(answers).pipe(mapDispatchError(command));
        answers = Object.fromEntries(
          Object.entries(decoded).map(([questionId, answer]) => [
            questionId,
            typeof answer === "object" && "answers" in answer ? answer.answers : answer,
          ]),
        );
      }

      const approvalTurnItem = context.item;
      const resolvedNodeStatus =
        command.decision === "decline" || command.decision === "cancel"
          ? ("cancelled" as const)
          : ("completed" as const);
      const replies: string[] = [];
      if (
        runtimeRequest.responseCapability.type === "message" &&
        resolvedNodeStatus !== "cancelled"
      ) {
        if (approvalTurnItem?.type !== "user_input_request") {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: "The question for this request was not found.",
          });
        }
        const thread = yield* projectionStore
          .getThread(command.threadId)
          .pipe(mapDispatchError(command));
        const settings = yield* serverSettings.getSettings.pipe(Effect.orDie);
        if (
          !resolveProjectProviderInstanceEnabled(
            settings,
            thread.projectId,
            thread.providerInstanceId,
          )
        ) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: "The provider for this project is disabled.",
          });
        }
        const questionIds = new Set(approvalTurnItem.questions.map((question) => question.id));
        if (
          !Object.entries(answers ?? {}).every(
            ([questionId, answer]) =>
              questionIds.has(questionId) || isMessageQuestionAnswer(answer),
          )
        ) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: "Answer each question before sending.",
          });
        }
        for (const question of approvalTurnItem.questions) {
          const answer = answers?.[question.id];
          const answered = isMessageQuestionAnswer(answer);
          // Clients send "" for a skipped optional question, such as an attachment-only answer.
          if (!answered && question.required === false) continue;
          if (!answered) {
            return yield* new OrchestratorDispatchError({
              commandId: command.commandId,
              commandType: command.type,
              cause: "Answer each question before sending.",
            });
          }
          const text = typeof answer === "string" ? answer : answer.join(", ");
          replies.push(`${question.question}\n${text.trim()}`);
        }
        if (replies.length === 0) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: "Enter an answer before sending.",
          });
        }
      }

      const now = yield* DateTime.now;
      const resolvedRequest = {
        ...runtimeRequest,
        status: "resolved" as const,
        resolvedAt: now,
        ...(command.decision === undefined ? {} : { decision: command.decision }),
        ...(command.feedback === undefined ? {} : { feedback: command.feedback }),
        ...(answers === undefined ? {} : { answers }),
      };
      const emitEvent = emit(events, command);
      const requestNode = context.node;
      if (command.feedback !== undefined) {
        const messageId = MessageId.make(`approval-feedback:${command.requestId}`);
        // Record the reply for every client without dispatching a prompt to the parent agent.
        yield* emitEvent({
          type: "message.updated",
          threadId: command.threadId,
          ...(requestNode?.runId == null ? {} : { runId: requestNode.runId }),
          nodeId: runtimeRequest.nodeId,
          occurredAt: now,
          payload: {
            id: messageId,
            threadId: command.threadId,
            runId: requestNode?.runId ?? null,
            nodeId: runtimeRequest.nodeId,
            role: "user",
            text: command.feedback,
            attachments: [],
            streaming: false,
            createdBy: "user",
            creationSource: "server",
            createdAt: now,
            updatedAt: now,
          },
        });
        yield* emitEvent({
          type: "turn-item.updated",
          threadId: command.threadId,
          ...(requestNode?.runId == null ? {} : { runId: requestNode.runId }),
          nodeId: runtimeRequest.nodeId,
          occurredAt: now,
          payload: {
            id: idAllocator.derive.userTurnItem({ messageId }),
            threadId: command.threadId,
            runId: requestNode?.runId ?? null,
            nodeId: runtimeRequest.nodeId,
            providerThreadId:
              context.item?.providerThreadId ?? requestNode?.providerThreadId ?? null,
            providerTurnId: runtimeRequest.providerTurnId,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: yield* projectionStore
              .getNextTurnItemOrdinal(command.threadId)
              .pipe(
                Effect.mapError(
                  (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
                ),
              ),
            status: "completed",
            title: null,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            type: "user_message",
            messageId,
            inputIntent: "steer",
            text: command.feedback,
            attachments: [],
            createdBy: "user",
            creationSource: "server",
          },
        });
      }
      yield* emitEvent({
        type: "runtime-request.updated",
        threadId: command.threadId,
        ...(requestNode?.runId == null ? {} : { runId: requestNode.runId }),
        nodeId: runtimeRequest.nodeId,
        ...(providerSession === undefined
          ? {}
          : {
              driver: providerSession.driver,
              providerInstanceId: providerSession.providerInstanceId,
            }),
        occurredAt: now,
        payload: resolvedRequest,
      });
      if (requestNode !== undefined) {
        yield* emitEvent({
          type: "node.updated",
          threadId: command.threadId,
          ...(requestNode.runId === null ? {} : { runId: requestNode.runId }),
          nodeId: requestNode.id,
          ...(providerSession === undefined
            ? {}
            : {
                driver: providerSession.driver,
                providerInstanceId: providerSession.providerInstanceId,
              }),
          occurredAt: now,
          payload: {
            ...requestNode,
            status: resolvedNodeStatus,
            completedAt: now,
          },
        });
      }

      if (approvalTurnItem !== undefined) {
        yield* emitEvent({
          type: "turn-item.updated",
          threadId: command.threadId,
          ...(approvalTurnItem.runId === null ? {} : { runId: approvalTurnItem.runId }),
          ...(approvalTurnItem.nodeId === null ? {} : { nodeId: approvalTurnItem.nodeId }),
          ...(providerSession === undefined
            ? {}
            : {
                driver: providerSession.driver,
                providerInstanceId: providerSession.providerInstanceId,
              }),
          occurredAt: now,
          payload: {
            ...approvalTurnItem,
            ...(approvalTurnItem.type === "user_input_request" && answers !== undefined
              ? {
                  questionAnswer: {
                    requestId: command.requestId,
                    answers,
                    attachmentsByQuestionId: command.attachmentsByQuestionId ?? {},
                    questionTextById: Object.fromEntries(
                      approvalTurnItem.questions.map((question) => [
                        question.id,
                        question.question,
                      ]),
                    ),
                  },
                }
              : {}),
            status: resolvedNodeStatus,
            completedAt: now,
            updatedAt: now,
          },
        });
      }
      if (runtimeRequest.responseCapability.type === "message") {
        if (resolvedNodeStatus === "cancelled") return;
        let dispatchMode: Extract<
          OrchestrationV2Command,
          { type: "message.dispatch" }
        >["dispatchMode"] = {
          type: "queue_after_active",
        };
        const {
          run: activeRun,
          providerThread: activeProviderThread,
          providerTurn: activeProviderTurn,
        } = yield* projectionStore
          .getRunningTurnContext(command.threadId)
          .pipe(mapDispatchError(command));
        if (activeRun && activeProviderTurn && activeProviderThread?.providerSessionId) {
          const activeSession = yield* providerSessions
            .get(activeProviderThread.providerSessionId)
            .pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestratorDispatchError({
                    commandId: command.commandId,
                    commandType: command.type,
                    cause,
                  }),
              ),
            );
          if (
            Option.isSome(activeSession) &&
            activeSession.value.providerSession.capabilities.turns.supportsActiveSteering
          ) {
            dispatchMode = { type: "steer_active", targetRunId: activeRun.id };
          }
        }
        const answer = {
          type: "message.dispatch" as const,
          commandId: command.commandId,
          threadId: command.threadId,
          messageId: MessageId.make(`async-answer:${command.requestId}`),
          text: replies.join("\n\n"),
          attachments: [],
          createdBy: "user" as const,
          creationSource: "server" as const,
          dispatchMode,
        };
        yield* dispatchMessage(answer, events, effects);
        return yield* recordSubagentFollowup(answer, events).pipe(mapDispatchError(answer));
      }
      if (providerSessionId === null) return;
      if (runtimeRequest.kind === "user_input" && resolvedNodeStatus === "completed") {
        const thread = yield* projectionStore
          .getThread(command.threadId)
          .pipe(mapDispatchError(command));
        if (thread.snoozedUntil != null) {
          yield* emitEvent({
            type: "thread.unsnoozed",
            threadId: command.threadId,
            providerInstanceId: thread.providerInstanceId,
            occurredAt: now,
            payload: { ...thread, snoozedUntil: null, snoozedAt: null, updatedAt: now },
          });
        }
      }
      yield* Ref.update(effects, (existing) => [
        ...existing,
        {
          id: `effect:${command.commandId}:runtime-request.respond:${command.requestId}`,
          commandId: command.commandId,
          threadId: command.threadId,
          request: {
            type: "runtime-request.respond",
            providerSessionId,
            requestId: command.requestId,
            ...(command.decision === undefined ? {} : { decision: command.decision }),
            ...(command.feedback === undefined ? {} : { feedback: command.feedback }),
            ...(answers === undefined ? {} : { answers }),
          },
        } satisfies PendingOrchestrationEffectV2,
      ]);
    },
  );

  /**
   * Respond resolves the request before delivery, and a failed delivery cannot
   * tell whether the provider is still waiting, so the answer stays recorded and
   * the timeline shows the failure instead of a run spinning with no question.
   */
  const dispatchRuntimeRequestDeliveryFail = Effect.fn(
    "Orchestrator.dispatchRuntimeRequestDeliveryFail",
  )(function* (
    command: Extract<
      OrchestrationV2InternalCommand,
      { readonly type: "runtime-request.delivery.fail" }
    >,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const thread = yield* projectionStore
      .getThread(command.threadId)
      .pipe(mapDispatchError(command));
    if (thread.deletedAt !== null) return;
    const node = (yield* projectionStore
      .getRuntimeResponseContext(command.threadId, command.requestId)
      .pipe(mapDispatchError(command))).node;
    if (node === undefined) return;
    const now = yield* DateTime.now;
    yield* emit(
      events,
      command,
    )({
      type: "turn-item.updated",
      threadId: command.threadId,
      ...(node.runId === null ? {} : { runId: node.runId }),
      nodeId: node.id,
      providerInstanceId: thread.providerInstanceId,
      occurredAt: now,
      payload: {
        id: TurnItemId.make(`turn-item:runtime-request-delivery-failure:${command.requestId}`),
        threadId: command.threadId,
        runId: node.runId,
        nodeId: node.id,
        providerThreadId: node.providerThreadId,
        providerTurnId: node.providerTurnId,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: yield* nextTurnItemOrdinal({ thread }),
        status: "failed",
        title: "Answer delivery not confirmed",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "error",
        failure: {
          class: "transport_error",
          message: RUNTIME_REQUEST_DELIVERY_FAILED_MESSAGE,
          code: null,
          retryable: null,
        },
      },
    });
  });

  const dispatchThreadUserInputDismiss = Effect.fn("Orchestrator.dispatchThreadUserInputDismiss")(
    function* (
      command: Extract<OrchestrationV2Command, { readonly type: "thread.user-input.dismiss" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    ) {
      const request = yield* projectionStore
        .getRuntimeRequest(command.threadId, command.requestId)
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
          ),
        );
      if (request === undefined || request.status !== "pending") {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "This question has already been answered.",
        });
      }
      if (request.kind !== "user_input" || request.responseCapability.type !== "message") {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "This question needs an answer. Answer it or stop the turn.",
        });
      }
      yield* dispatchRuntimeRequestRespond(
        {
          type: "runtime-request.respond",
          commandId: command.commandId,
          threadId: command.threadId,
          requestId: command.requestId,
          decision: "cancel",
        },
        events,
        effects,
      );
    },
  );

  // Promotion reads the queued and target runs' records, not the thread's settled history. What a
  // message dispatch reads whole (readMessageDispatchProjection), or a provider switch, reads the
  // whole command projection.
  const readPromoteProjection = Effect.fnUntraced(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "queued-message.promote-to-steer" }>,
  ) {
    const projection = yield* readQueuedRunProjection(command.threadId, [
      command.queuedRunId,
      command.targetRunId,
      ...(command.mergedQueuedRunIds ?? []),
    ]);
    const queuedRun = projection.runs.find((run) => run.id === command.queuedRunId);
    const activeProviderInstanceId = projection.providerThreads.find(
      (candidate) => candidate.id === projection.thread.activeProviderThreadId,
    )?.providerInstanceId;
    if (
      projection.thread.historyOrigin !== "v1_import" &&
      pendingForkTransferForThread(projection) === undefined &&
      pendingMergeBackTransfersForThread(projection).length === 0 &&
      !liveWakeParentUnread(projection) &&
      (queuedRun === undefined ||
        activeProviderInstanceId === undefined ||
        activeProviderInstanceId === queuedRunModelSelection(projection, queuedRun).instanceId)
    ) {
      return { projection, history: storedHistory(command.threadId) };
    }
    const whole = yield* loadProjectionForCommand(
      command,
      [
        "runs",
        "nodes",
        "attempts",
        "messages",
        "providerThreads",
        "providerTurns",
        "providerSessions",
        "turnItems",
        "runtimeRequests",
        "subagents",
        "checkpointScopes",
      ],
      // User messages measure a restart's model switch. Steering reads root and live nodes only.
      { turnItemTypes: ["user_message"], messageRoles: ["user"], omitSettledLeafNodes: true },
    );
    return { projection: whole, history: projectedHistory };
  });

  const dispatchQueuedMessagePromoteToSteer = Effect.fn(
    "Orchestrator.dispatchQueuedMessagePromoteToSteer",
  )(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "queued-message.promote-to-steer" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
  ) {
    const { projection, history } = yield* readPromoteProjection(command);
    if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Thread ${command.threadId} is not active.`,
      });
    }
    const queuedRun = projection.runs.find((candidate) => candidate.id === command.queuedRunId);
    if (queuedRun === undefined || queuedRun.status !== "queued") {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Queued run ${command.queuedRunId} is not queued.`,
      });
    }
    const queuedRootNode =
      queuedRun.rootNodeId === null
        ? undefined
        : projection.nodes.find((candidate) => candidate.id === queuedRun.rootNodeId);
    const queuedAttempt =
      queuedRun.activeAttemptId === null
        ? undefined
        : projection.attempts.find((candidate) => candidate.id === queuedRun.activeAttemptId);
    const queuedMessage = projection.messages.find(
      (candidate) => candidate.id === queuedRun.userMessageId,
    );
    if (
      queuedRootNode === undefined ||
      queuedAttempt === undefined ||
      queuedMessage === undefined
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Queued run ${queuedRun.id} is missing message or execution state.`,
      });
    }
    if (
      queuedMessage.delegatedCompletion !== undefined ||
      queuedMessage.notification !== undefined
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "Automatic completion deliveries cannot be promoted to Steer.",
      });
    }
    const automaticDelivery = command.commandId.startsWith(
      QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX,
    );
    // Only automatic Send now merges queued messages into one steer.
    if ((command.mergedQueuedRunIds?.length ?? 0) > 0 && !automaticDelivery) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "Queued messages cannot join this steer.",
      });
    }

    const now = yield* DateTime.now;
    const emitEvent = emit(events, command);
    // A run waiting only for its checkpoint capture has no turn to steer. This message goes
    // first, and its wait ends once this commits, so the run-end reaction starts it.
    const waitingTarget = yield* endableWait(projection).pipe(mapDispatchError(command));
    // The sweep's pick does not go first: ending the wait starts the queue in its own order.
    if (
      waitingTarget?.run.id === command.targetRunId &&
      command.commandId.startsWith(QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX)
    ) {
      yield* emitEvent({
        type: "thread.metadata-updated",
        threadId: command.threadId,
        occurredAt: now,
        payload: projection.thread,
      });
      return;
    }
    if (waitingTarget?.run.id === command.targetRunId) {
      yield* emitEvent({
        type: "run.updated",
        threadId: command.threadId,
        runId: queuedRun.id,
        nodeId: queuedRootNode.id,
        providerInstanceId: queuedRun.providerInstanceId,
        occurredAt: now,
        payload: { ...queuedRun, queuePosition: 1, queueHeld: false },
      });
      yield* stopRunForQueuedCommand({
        command,
        events,
        effects,
        projection,
        queuedRunId: queuedRun.id,
        targetRunId: undefined,
        reason: "Sent a queued message now",
        now,
      });
      return;
    }
    // A scheduled delivery steers on the thread's current pick, so /goal must suit that provider.
    const modelSelection = queuedRunModelSelection(projection, queuedRun);
    const parsedGoalCommand = parseGoalCommand(queuedMessage);
    const goalCommand =
      parsedGoalCommand === null ||
      (yield* goalDriverFor(command, modelSelection.instanceId)) === undefined
        ? null
        : parsedGoalCommand;
    const target = projection.runs.find((run) => run.id === command.targetRunId);
    const targetMessage = projection.messages.find(
      (message) => message.id === target?.userMessageId,
    );
    const merged: Array<{
      readonly run: OrchestrationV2Run;
      readonly rootNode: OrchestrationV2ExecutionNode;
      readonly attempt: OrchestrationV2RunAttempt;
      readonly message: OrchestrationV2ConversationMessage;
    }> = [];
    for (const runId of command.mergedQueuedRunIds ?? []) {
      const run = projection.runs.find((candidate) => candidate.id === runId);
      const rootNode = projection.nodes.find((candidate) => candidate.id === run?.rootNodeId);
      const attempt = projection.attempts.find(
        (candidate) => candidate.id === run?.activeAttemptId,
      );
      const message = projection.messages.find((candidate) => candidate.id === run?.userMessageId);
      // Only plain queued messages on the same pick join a steer; a slash command goes alone.
      if (
        startsWithSlashCommand(queuedMessage) ||
        (message !== undefined && startsWithSlashCommand(message)) ||
        run?.status !== "queued" ||
        run.id === queuedRun.id ||
        run.freshContext === true ||
        merged.some((other) => other.run.id === run.id) ||
        rootNode === undefined ||
        attempt === undefined ||
        message === undefined ||
        message.delegatedCompletion !== undefined ||
        message.notification !== undefined ||
        goalCommand !== null ||
        (targetMessage !== undefined && isCompactionCommandMessage(targetMessage)) ||
        !modelSelectionsEqual(queuedRunModelSelection(projection, run), modelSelection)
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Queued run ${runId} cannot join this steer.`,
        });
      }
      merged.push({ run, rootNode, attempt, message });
    }
    // A compacting turn cannot take steering input, so steering skips the compaction instead.
    if (
      target !== undefined &&
      targetMessage !== undefined &&
      isCompactionCommandMessage(targetMessage)
    ) {
      if (goalCommand !== null) {
        yield* ensureGoalCommandSupported(command, queuedMessage, modelSelection.instanceId);
      }
      yield* emitEvent({
        type: "run.updated",
        threadId: command.threadId,
        runId: queuedRun.id,
        nodeId: queuedRootNode.id,
        providerInstanceId: queuedRun.providerInstanceId,
        occurredAt: now,
        payload: { ...queuedRun, queuePosition: 1 },
      });
      const stops =
        isLiveRun(target) && !(yield* stopReachedRun(command, command.threadId, target.id));
      return yield* stopRunForQueuedCommand({
        command,
        events,
        effects,
        projection,
        queuedRunId: queuedRun.id,
        targetRunId: stops ? target.id : undefined,
        reason: "Skipped compaction to send a queued message",
        now,
      });
    }
    // Skipping compaction starts the message as its own turn; steering would join the current one.
    if (queuedRun.freshContext === true) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "A message that starts a fresh session can't steer the current turn.",
      });
    }
    if (goalCommand !== null) {
      yield* ensureGoalCommandSupported(command, queuedMessage, modelSelection.instanceId);
      const goal = projection.providerThreads.find(
        (candidate) => candidate.id === target?.providerThreadId,
      )?.goal;
      yield* emitEvent({
        type: "run.updated",
        threadId: command.threadId,
        runId: queuedRun.id,
        nodeId: queuedRootNode.id,
        providerInstanceId: queuedRun.providerInstanceId,
        occurredAt: now,
        payload: { ...queuedRun, queuePosition: 1 },
      });
      const stops =
        target !== undefined &&
        isLiveRun(target) &&
        goalCommandStopsActiveRun(goalCommand, goal?.status ?? null) &&
        (targetMessage === undefined || !isNativeMaintenanceCommand(targetMessage)) &&
        !(yield* stopReachedRun(command, command.threadId, target.id));
      return yield* stopRunForQueuedCommand({
        command,
        events,
        effects,
        projection,
        queuedRunId: queuedRun.id,
        targetRunId: stops ? target.id : undefined,
        reason: "Stopped to apply /goal",
        now,
      });
    }
    if (
      (target?.status === "preparing" ||
        target?.status === "starting" ||
        (target?.status === "running" &&
          !projection.providerTurns.some(
            (turn) => turn.runAttemptId === target.activeAttemptId && turn.status === "running",
          ))) &&
      !(yield* stopReachedRun(command, command.threadId, target.id))
    ) {
      // The sweep checked a live turn that has ended since; its next start decides again.
      if (command.commandId.startsWith(QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX)) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Run ${target.id} has no live turn to steer.`,
        });
      }
      yield* ensureSteeringMessage(command, queuedMessage);
      const adapter = yield* providerAdapters
        .get(target.providerInstanceId)
        .pipe(mapDispatchError(command));
      const capabilities = yield* adapter.getCapabilities().pipe(mapDispatchError(command));
      yield* commandPolicy
        .decideSteeringExecution({
          commandId: command.commandId,
          threadId: command.threadId,
          providerInstanceId: target.providerInstanceId,
          capabilities,
        })
        .pipe(mapDispatchError(command));
      yield* emitEvent({
        type: "run.updated",
        threadId: command.threadId,
        runId: queuedRun.id,
        nodeId: queuedRootNode.id,
        providerInstanceId: queuedRun.providerInstanceId,
        occurredAt: now,
        payload: { ...queuedRun, steerWhenReadyRunId: target.id },
      });
      return;
    }
    for (const promoted of [
      { run: queuedRun, rootNode: queuedRootNode, attempt: queuedAttempt },
      ...merged,
    ]) {
      yield* emitEvent({
        type: "run.updated",
        threadId: command.threadId,
        runId: promoted.run.id,
        nodeId: promoted.rootNode.id,
        providerInstanceId: promoted.run.providerInstanceId,
        occurredAt: now,
        payload: {
          ...promoted.run,
          status: "cancelled",
          queuePosition: null,
          completedAt: now,
        },
      });
      yield* emitEvent({
        type: "run-attempt.updated",
        threadId: command.threadId,
        runId: promoted.run.id,
        nodeId: promoted.rootNode.id,
        providerInstanceId: promoted.run.providerInstanceId,
        occurredAt: now,
        payload: {
          ...promoted.attempt,
          status: "cancelled",
          completedAt: now,
        },
      });
      yield* emitEvent({
        type: "node.updated",
        threadId: command.threadId,
        runId: promoted.run.id,
        nodeId: promoted.rootNode.id,
        providerInstanceId: promoted.run.providerInstanceId,
        occurredAt: now,
        payload: {
          ...promoted.rootNode,
          status: "cancelled",
          completedAt: now,
        },
      });
    }

    yield* dispatchSteerIntoRun({
      command,
      events,
      effects,
      projection,
      modelSelection,
      history,
      targetRunId: command.targetRunId,
      messageId: queuedMessage.id,
      text: queuedMessage.text,
      attachments: queuedMessage.attachments,
      ...(queuedMessage.context ? { context: queuedMessage.context } : {}),
      createdBy: queuedMessage.createdBy,
      creationSource: queuedMessage.creationSource,
      ...(queuedMessage.scheduledTaskId === undefined
        ? {}
        : { scheduledTaskId: queuedMessage.scheduledTaskId }),
      ...(queuedMessage.senderThreadId === undefined
        ? {}
        : { senderThreadId: queuedMessage.senderThreadId }),
      ...(queuedMessage.linkedSender === undefined
        ? {}
        : { linkedSender: queuedMessage.linkedSender }),
      ...(queuedMessage.idleCompaction === undefined
        ? {}
        : { idleCompaction: queuedMessage.idleCompaction }),
      forceRestart: false,
      ...(automaticDelivery ? { automaticDelivery } : {}),
      ...(merged.length === 0
        ? {}
        : {
            mergedMessages: merged.map(({ message }) => ({
              messageId: message.id,
              text: message.text,
              attachments: message.attachments,
              ...(message.context ? { context: message.context } : {}),
              createdBy: message.createdBy,
              creationSource: message.creationSource,
              ...(message.scheduledTaskId === undefined
                ? {}
                : { scheduledTaskId: message.scheduledTaskId }),
              ...(message.senderThreadId === undefined
                ? {}
                : { senderThreadId: message.senderThreadId }),
              ...(message.linkedSender === undefined ? {} : { linkedSender: message.linkedSender }),
              ...(message.idleCompaction === undefined
                ? {}
                : { idleCompaction: message.idleCompaction }),
            })),
          }),
    });
  });

  const dispatchQueuedRunReorder = Effect.fn("Orchestrator.dispatchQueuedRunReorder")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "queued-run.reorder" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const { runs } = yield* loadProjectionForCommand(command, ["runs"], {
      runStatuses: ["queued"],
    });
    const { messages } =
      runs.length === 0
        ? { messages: [] }
        : yield* loadProjectionForCommand(command, ["messages"], {
            messageIds: runs.map((run) => run.userMessageId),
          });
    const projection = { runs, messages };
    const queuedRuns = queuedRunsInDeliveryOrder(projection);
    const moving = queuedRuns.find((run) => run.id === command.runId);
    if (moving === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Run ${command.runId} is not queued.`,
      });
    }
    const movingMessage = projection.messages.find(
      (candidate) => candidate.id === moving.userMessageId,
    );
    if (
      movingMessage?.delegatedCompletion !== undefined ||
      movingMessage?.notification !== undefined
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "Automatic completion deliveries cannot be reordered.",
      });
    }
    // Wakes are not listed, so a user orders only the visible messages; a move before a wake
    // puts the message after every visible one.
    const hiddenMessageIds = new Set(
      projection.messages.filter(isHiddenWake).map((message) => message.id),
    );
    const withoutMoving = queuedRuns.filter(
      (run) => run.id !== command.runId && !hiddenMessageIds.has(run.userMessageId),
    );
    const beforeIndex =
      command.beforeRunId === null ||
      queuedRuns.some(
        (run) => run.id === command.beforeRunId && hiddenMessageIds.has(run.userMessageId),
      )
        ? withoutMoving.length
        : withoutMoving.findIndex((run) => run.id === command.beforeRunId);
    if (beforeIndex === -1) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Queue target ${command.beforeRunId} is not queued.`,
      });
    }
    const reordered = [
      ...withoutMoving.slice(0, beforeIndex),
      moving,
      ...withoutMoving.slice(beforeIndex),
    ];
    const now = yield* DateTime.now;
    const emitEvent = emit(events, command);
    yield* Effect.forEach(
      reordered,
      (run, index) =>
        Effect.gen(function* () {
          const queuePosition = index + 1;
          if (run.queuePosition === queuePosition) {
            return;
          }
          yield* emitEvent({
            type: "run.updated",
            threadId: command.threadId,
            runId: run.id,
            ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: {
              ...run,
              queuePosition,
            },
          });
        }),
      { concurrency: 1 },
    );
  });

  const dispatchQueuedRunCancel = Effect.fn("Orchestrator.dispatchQueuedRunCancel")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "queued-run.cancel" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    // Settling disposes a cancelled wake's whole cohort, so a sibling's late result cannot unsettle it.
    disposes: "wake" | "cohort" = "wake",
  ) {
    // Cancelling reads the queued run, its message, root node and attempt, and for a result wake
    // the parent runs it delivers for with their tasks; never the thread's history.
    const own = yield* loadProjectionForCommand(command, ["runs", "messages"], {
      runIds: [command.runId],
      messageRoles: ["user"],
      messageRunIds: [command.runId],
    });
    const ownRun = own.runs[0];
    const parentRunIds = own.messages.flatMap((message) =>
      message.id === ownRun?.userMessageId && message.delegatedCompletion !== undefined
        ? completionWakeParts(message.delegatedCompletion).map((part) => part.parentRunId)
        : [],
    );
    const related = yield* loadProjectionForCommand(
      command,
      ["runs", "nodes", "attempts", "subagents"],
      {
        runIds: parentRunIds,
        nodeIds: ownRun?.rootNodeId == null ? [] : [ownRun.rootNodeId],
        attemptIds: ownRun?.activeAttemptId == null ? [] : [ownRun.activeAttemptId],
        subagentRunIds: parentRunIds,
      },
    );
    const projection = {
      ...related,
      runs: [...own.runs, ...related.runs.filter((run) => run.id !== command.runId)],
      messages: own.messages,
    };
    const queuedRun = projection.runs.find((candidate) => candidate.id === command.runId);
    if (queuedRun === undefined || queuedRun.status !== "queued") {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Run ${command.runId} is not queued.`,
      });
    }
    const queuedMessage = projection.messages.find(
      (candidate) => candidate.id === queuedRun.userMessageId,
    );
    const now = yield* DateTime.now;
    const queuedOwnership = queuedMessage?.delegatedCompletion;
    for (const part of queuedOwnership === undefined ? [] : completionWakeParts(queuedOwnership)) {
      yield* disposeDelegatedCompletionCohort({
        command,
        events,
        projection,
        parentRunId: part.parentRunId,
        disposition: "disposed",
        now,
        cancelQueuedDelivery: false,
        ...(disposes === "wake" ? { interruptedWakeTaskIds: queuedOwnership!.taskIds } : {}),
      });
    }
    yield* emitQueuedRunCancellation({ command, events, projection, run: queuedRun, now });
    const emitEvent = emit(events, command);
    const guest = queuedMessage?.context?.guest;
    if (guest !== undefined) {
      const thread = guestShareAfterCancelledSend(projection.thread, guest, now);
      if (thread !== projection.thread) {
        yield* emitEvent({
          type: "thread.guest-share-updated",
          threadId: command.threadId,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: now,
          payload: thread,
        });
      }
    }
  });

  const dispatchQueuedRunEdit = Effect.fn("Orchestrator.dispatchQueuedRunEdit")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "queued-run.edit" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
  ) {
    if (command.text.trim().length === 0) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Queued run ${command.runId} cannot be edited to an empty message.`,
      });
    }
    const projection = yield* loadProjectionForCommand(command, ["runs", "messages", "turnItems"], {
      turnItemTypes: ["user_message"],
      messageRoles: ["user"],
    });
    const queuedRun = projection.runs.find((candidate) => candidate.id === command.runId);
    if (queuedRun === undefined || queuedRun.status !== "queued") {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Run ${command.runId} is not queued.`,
      });
    }
    const queuedMessage = projection.messages.find(
      (candidate) => candidate.id === queuedRun.userMessageId,
    );
    if (queuedMessage === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Queued run ${queuedRun.id} has no user message.`,
      });
    }
    if (
      queuedMessage.delegatedCompletion !== undefined ||
      queuedMessage.notification !== undefined
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "Automatic completion deliveries cannot be edited.",
      });
    }
    // The agent text and the shown text of a guest message must stay the guest's words.
    if (queuedMessage.context?.guest !== undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "A guest's message cannot be edited. Cancel it instead.",
      });
    }
    yield* ensureGoalCommandSupported(
      command,
      { text: command.text, attachments: command.attachments ?? queuedMessage.attachments },
      queuedRunModelSelection(projection, queuedRun).instanceId,
    );
    const queuedTurnItem = projection.turnItems.find(
      (candidate) => candidate.type === "user_message" && candidate.messageId === queuedMessage.id,
    );

    const now = yield* DateTime.now;
    const emitEvent = emit(events, command);
    const editedAttachments =
      command.attachments === undefined ? {} : { attachments: command.attachments };
    yield* emitEvent({
      type: "message.updated",
      threadId: command.threadId,
      runId: queuedRun.id,
      ...(queuedRun.rootNodeId === null ? {} : { nodeId: queuedRun.rootNodeId }),
      providerInstanceId: queuedRun.providerInstanceId,
      occurredAt: now,
      payload: withoutGuestAttribution({
        ...queuedMessage,
        text: command.text,
        ...editedAttachments,
        ...(command.context ? { context: command.context } : {}),
        updatedAt: now,
      }),
    });
    if (queuedTurnItem !== undefined && queuedTurnItem.type === "user_message") {
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId: queuedRun.id,
        ...(queuedRun.rootNodeId === null ? {} : { nodeId: queuedRun.rootNodeId }),
        providerInstanceId: queuedRun.providerInstanceId,
        occurredAt: now,
        payload: withoutGuestAttribution({
          ...queuedTurnItem,
          text: command.text,
          ...editedAttachments,
          ...(command.context ? { context: command.context } : {}),
          updatedAt: now,
        }),
      });
    }
    // Thread deletion only cleans files the thread still references.
    const keptAttachmentIds = new Set(
      (command.attachments ?? queuedMessage.attachments).map((attachment) => attachment.id),
    );
    const droppedAttachmentIds = queuedMessage.attachments
      .map((attachment) => attachment.id)
      .filter((id) => !keptAttachmentIds.has(id));
    if (droppedAttachmentIds.length > 0) {
      yield* Ref.update(effects, (existing) => [
        ...existing,
        {
          id: `effect:${command.commandId}:attachment.cleanup`,
          commandId: command.commandId,
          threadId: command.threadId,
          request: {
            type: "attachment.cleanup",
            attachmentIds: droppedAttachmentIds,
            skipReferenced: true,
          },
        } satisfies PendingOrchestrationEffectV2,
      ]);
    }
  });

  const subagentCount = (threadId: ThreadId) =>
    projectionStore
      .getSubagentCount(threadId)
      .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));

  const loadProjectionForCommand = <K extends ProjectionRecordField>(
    command: OrchestrationV2Command,
    fields: ReadonlyArray<K>,
    filter?: ProjectionRecordFilter,
  ) =>
    projectionStore
      .getThreadRecords(commandThreadId(command), fields, filter)
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: commandThreadId(command), cause }),
        ),
      );

  const preparedRunState = (
    command: Extract<
      OrchestrationV2Command,
      {
        readonly type:
          | "prepared-run.release"
          | "prepared-run.progress"
          | "prepared-run.fail"
          | "prepared-run.retry";
      }
    >,
    projection: Pick<
      OrchestrationV2ThreadProjection,
      "runs" | "attempts" | "nodes" | "providerThreads" | "turnItems"
    >,
  ) => {
    const run = projection.runs.find((candidate) => candidate.id === command.runId);
    const attempt = projection.attempts.find((candidate) => candidate.id === run?.activeAttemptId);
    const rootNode = projection.nodes.find((candidate) => candidate.id === run?.rootNodeId);
    const providerThread = projection.providerThreads.find(
      (candidate) => candidate.id === run?.providerThreadId,
    );
    const preparationItem = projection.turnItems.find(
      (
        candidate,
      ): candidate is Extract<OrchestrationV2TurnItem, { readonly type: "command_execution" }> =>
        candidate.runId === command.runId &&
        candidate.type === "command_execution" &&
        candidate.input === WORKSPACE_PREPARATION_INPUT,
    );
    if (
      run?.status !== (command.type === "prepared-run.retry" ? "failed" : "preparing") ||
      attempt === undefined ||
      rootNode === undefined ||
      providerThread === undefined ||
      preparationItem === undefined
    ) {
      return null;
    }
    return { run, attempt, rootNode, providerThread, preparationItem } as const;
  };

  const dispatchPreparedRunProgress = Effect.fn("Orchestrator.dispatchPreparedRunProgress")(
    function* (
      command: Extract<OrchestrationV2Command, { readonly type: "prepared-run.progress" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    ) {
      const projection = yield* loadProjectionForCommand(
        command,
        ["runs", "attempts", "nodes", "providerThreads", "turnItems", "checkpointScopes"],
        { turnItemTypes: ["command_execution"], turnItemRunId: command.runId },
      );
      const state = preparedRunState(command, projection);
      if (state === null) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Run ${command.runId} is not awaiting workspace preparation.`,
        });
      }
      const now = yield* DateTime.now;
      yield* emit(
        events,
        command,
      )({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId: state.run.id,
        nodeId: state.rootNode.id,
        providerInstanceId: state.run.providerInstanceId,
        occurredAt: now,
        payload: {
          ...state.preparationItem,
          title: command.phase === "worktree" ? "Preparing worktree" : "Starting setup script",
          updatedAt: now,
        },
      });
    },
  );

  const dispatchPreparedRunRelease = Effect.fn("Orchestrator.dispatchPreparedRunRelease")(
    function* (
      command: Extract<OrchestrationV2Command, { readonly type: "prepared-run.release" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    ) {
      const projection = yield* loadProjectionForCommand(
        command,
        ["runs", "attempts", "nodes", "providerThreads", "turnItems", "checkpointScopes"],
        { turnItemTypes: ["command_execution"], turnItemRunId: command.runId },
      );
      const state = preparedRunState(command, projection);
      if (state === null) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Run ${command.runId} is not awaiting workspace preparation.`,
        });
      }
      const now = yield* DateTime.now;
      const resolvedRuntimePolicy = yield* runtimePolicy
        .resolve({ thread: projection.thread, modelSelection: state.run.modelSelection })
        .pipe(mapDispatchError(command));
      const checkpointScope = yield* prepareRootRunScope({
        threadScopes: projection.checkpointScopes,
        threadId: command.threadId,
        runId: state.run.id,
        rootNodeId: state.rootNode.id,
        providerThreadId: state.providerThread.id,
        cwd: resolvedRuntimePolicy.cwd ?? projection.thread.worktreePath ?? process.cwd(),
        createdAt: now,
      }).pipe(mapDispatchError(command));
      const emitEvent = emit(events, command);
      yield* emitEvent({
        type: "checkpoint-scope.created",
        threadId: command.threadId,
        runId: state.run.id,
        nodeId: state.rootNode.id,
        providerInstanceId: state.run.providerInstanceId,
        occurredAt: now,
        payload: checkpointScope,
      });
      yield* emitEvent({
        type: "node.updated",
        threadId: command.threadId,
        runId: state.run.id,
        nodeId: state.rootNode.id,
        providerInstanceId: state.run.providerInstanceId,
        occurredAt: now,
        payload: { ...state.rootNode, checkpointScopeId: checkpointScope.id },
      });
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId: state.run.id,
        nodeId: state.rootNode.id,
        providerInstanceId: state.run.providerInstanceId,
        occurredAt: now,
        payload: {
          ...state.preparationItem,
          status: "completed",
          title: "Workspace ready",
          output: "Workspace preparation completed.",
          exitCode: 0,
          completedAt: now,
          updatedAt: now,
        },
      });
      yield* emitEvent({
        type: "run.updated",
        threadId: command.threadId,
        runId: state.run.id,
        nodeId: state.rootNode.id,
        providerInstanceId: state.run.providerInstanceId,
        occurredAt: now,
        payload: {
          ...state.run,
          status: "starting",
          workspaceCwd: runWorkspaceOf(checkpointScope),
        },
      });
      yield* Ref.update(effects, (existing) => [
        ...existing,
        {
          id: `effect:${command.commandId}:provider-turn.start:${state.run.id}`,
          commandId: command.commandId,
          threadId: command.threadId,
          request: { type: "provider-turn.start", runId: state.run.id },
        } satisfies PendingOrchestrationEffectV2,
      ]);
    },
  );

  const dispatchPreparedRunFail = Effect.fn("Orchestrator.dispatchPreparedRunFail")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "prepared-run.fail" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const projection = yield* loadProjectionForCommand(
      command,
      ["runs", "attempts", "nodes", "providerThreads", "turnItems"],
      { turnItemTypes: ["command_execution"], turnItemRunId: command.runId },
    );
    const state = preparedRunState(command, projection);
    if (state === null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Run ${command.runId} is not awaiting workspace preparation.`,
      });
    }
    const now = yield* DateTime.now;
    const emitEvent = emit(events, command);
    yield* emitEvent({
      type: "run-attempt.updated",
      threadId: command.threadId,
      runId: state.run.id,
      nodeId: state.rootNode.id,
      providerInstanceId: state.run.providerInstanceId,
      occurredAt: now,
      payload: { ...state.attempt, status: "failed", completedAt: now },
    });
    yield* emitEvent({
      type: "node.updated",
      threadId: command.threadId,
      runId: state.run.id,
      nodeId: state.rootNode.id,
      providerInstanceId: state.run.providerInstanceId,
      occurredAt: now,
      payload: { ...state.rootNode, status: "failed", completedAt: now },
    });
    yield* emitEvent({
      type: "turn-item.updated",
      threadId: command.threadId,
      runId: state.run.id,
      nodeId: state.rootNode.id,
      providerInstanceId: state.run.providerInstanceId,
      occurredAt: now,
      payload: {
        ...state.preparationItem,
        status: "failed",
        title: "Workspace preparation failed",
        output: command.failure.message,
        exitCode: 1,
        completedAt: now,
        updatedAt: now,
      },
    });
    yield* emitEvent({
      type: "turn-item.updated",
      threadId: command.threadId,
      runId: state.run.id,
      nodeId: state.rootNode.id,
      providerInstanceId: state.run.providerInstanceId,
      occurredAt: now,
      payload: {
        id: idAllocator.derive.turnItemFromProviderItem({
          driver: state.providerThread.driver,
          nativeItemId: `workspace-preparation-failure:${state.run.id}`,
        }),
        threadId: command.threadId,
        runId: state.run.id,
        nodeId: state.rootNode.id,
        providerThreadId: state.providerThread.id,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: yield* nextTurnItemOrdinal(projection),
        status: "failed",
        title: "Workspace preparation failed",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "error",
        failure: {
          ...command.failure,
          code: ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
        },
      },
    });
    yield* emitEvent({
      type: "run.updated",
      threadId: command.threadId,
      runId: state.run.id,
      nodeId: state.rootNode.id,
      providerInstanceId: state.run.providerInstanceId,
      occurredAt: now,
      payload: { ...state.run, status: "failed", completedAt: now },
    });
  });

  const dispatchPreparedRunRetry = Effect.fn("Orchestrator.dispatchPreparedRunRetry")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "prepared-run.retry" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const projection = yield* loadProjectionForCommand(
      command,
      ["runs", "attempts", "nodes", "providerThreads", "turnItems"],
      { turnItemTypes: ["command_execution", "error"], turnItemRunId: command.runId },
    );
    const state = preparedRunState(command, projection);
    const failureItem = projection.turnItems.find(
      (candidate): candidate is Extract<OrchestrationV2TurnItem, { readonly type: "error" }> =>
        candidate.type === "error" &&
        candidate.status === "failed" &&
        candidate.failure.code === ORCHESTRATION_V2_WORKSPACE_PREPARATION_FAILURE_CODE,
    );
    if (
      state === null ||
      state.run.workspacePreparation === undefined ||
      failureItem === undefined ||
      projection.thread.archivedAt !== null ||
      projection.thread.deletedAt !== null
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Run ${command.runId} has no failed workspace preparation to retry.`,
      });
    }
    if (projection.runs.some((run) => run.id !== state.run.id && isActiveRun(run))) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "Another run is active on this thread.",
      });
    }
    const now = yield* DateTime.now;
    const emitEvent = emit(events, command);
    const scope = {
      threadId: command.threadId,
      runId: state.run.id,
      nodeId: state.rootNode.id,
      providerInstanceId: state.run.providerInstanceId,
      occurredAt: now,
    };
    yield* emitEvent({
      ...scope,
      type: "turn-item.updated",
      payload: { ...failureItem, status: "cancelled", updatedAt: now },
    });
    yield* emitEvent({
      ...scope,
      type: "turn-item.updated",
      payload: {
        ...withoutPreparationResult(state.preparationItem),
        status: "running",
        title: WORKSPACE_PREPARATION_INPUT,
        completedAt: null,
        updatedAt: now,
      },
    });
    yield* emitEvent({
      ...scope,
      type: "run-attempt.updated",
      payload: { ...state.attempt, status: "pending", completedAt: null },
    });
    yield* emitEvent({
      ...scope,
      type: "node.updated",
      payload: { ...state.rootNode, status: "pending", completedAt: null },
    });
    yield* emitEvent({
      ...scope,
      type: "run.updated",
      payload: { ...state.run, status: "preparing", completedAt: null },
    });
    // Messages held when this preparation failed follow the run again. A hold
    // from a later run (a stop, say) is not this failure's to release.
    const failureOwnsQueue = latestExecutedRun(projection.runs)?.id === state.run.id;
    if (failureOwnsQueue) yield* releaseQueueHold(command, events, projection.thread, now);
    for (const run of projection.runs) {
      if (!failureOwnsQueue || run.status !== "queued" || run.queueHeld !== true) continue;
      yield* emitEvent({
        type: "run.updated",
        threadId: command.threadId,
        runId: run.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: { ...run, queueHeld: false },
      });
    }
  });

  // Ends each native subagent's record, its own thread's runless root turns, and the
  // subagents it started there, which a provider can record on that thread rather than its parent's.
  const endNativeSubagentTrees = Effect.fn("Orchestrator.endNativeSubagentTrees")(
    function* (input: {
      readonly command: Extract<
        OrchestrationV2ServerCommand,
        { readonly type: "run.interrupt" | "thread.background-work.settle" | "thread.stop" }
      >;
      readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
      readonly dying: ReadonlyArray<DyingNativeSubagent>;
      readonly childThreadIds: Set<ThreadId>;
      readonly now: DateTime.Utc;
    }) {
      const allocateEventId = () =>
        idAllocator.allocate.event({
          threadId: input.command.threadId,
          commandId: input.command.commandId,
        });
      const dying = [...input.dying];
      const { childThreadIds } = input;
      for (const { projection, subagent } of dying) {
        const ended = yield* endOrphanedNativeSubagent({
          projection,
          subagentId: subagent.subagentId,
          status: "interrupted",
          now: input.now,
          emitted: yield* Ref.get(input.events),
          allocateEventId,
        }).pipe(mapDispatchError(input.command));
        yield* Ref.update(input.events, (events) => [...events, ...ended]);
        const childThreadId = subagent.childThreadId;
        if (
          subagent.origin !== "provider_native" ||
          childThreadId === null ||
          childThreadIds.has(childThreadId)
        ) {
          continue;
        }
        childThreadIds.add(childThreadId);
        const child = yield* projectionStore
          .getThreadRecords(childThreadId, ["nodes", "providerTurns", "subagents", "turnItems"], {
            // Runless items are the subagent's; any run there is the user's own.
            turnItemRunIds: [null],
            turnItemStatuses: ["pending", "running", "waiting"],
          })
          .pipe(
            Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
            mapDispatchError(input.command),
          );
        if (child === null) continue;
        const childEnded = yield* endRunlessRootTurns({
          threadId: childThreadId,
          providerInstanceId: subagent.providerInstanceId,
          projection: child,
          status: "interrupted",
          now: input.now,
          emitted: yield* Ref.get(input.events),
          allocateEventId,
        }).pipe(mapDispatchError(input.command));
        yield* Ref.update(input.events, (events) => [...events, ...childEnded]);
        for (const nested of child.turnItems) {
          if (nested.type === "subagent") dying.push({ projection: child, subagent: nested });
        }
        for (const nested of child.subagents) {
          if (nested.origin === "provider_native" && nested.runId === null) {
            dying.push({ projection: child, subagent: nativeSubagentRef(nested) });
          }
        }
      }
    },
  );

  const settleBackgroundWork = Effect.fn("Orchestrator.settleBackgroundWork")(function* (input: {
    readonly command: Extract<
      OrchestrationV2ServerCommand,
      { readonly type: "run.interrupt" | "thread.background-work.settle" }
    >;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly projection: Pick<
      OrchestrationV2ThreadProjection,
      "runs" | "nodes" | "subagents" | "turnItems" | "providerThreads"
    >;
    readonly stoppedProviderThreadId: OrchestrationV2ProviderThread["id"] | null;
    readonly throughRunOrdinal: number;
    readonly now: DateTime.Utc;
  }) {
    const emitEvent = emit(input.events, input.command);
    const runOrdinals = new Map(input.projection.runs.map((run) => [run.id, run.ordinal]));
    const childThreadIds = new Set<ThreadId>();
    const liveness = new Map<string, boolean>();
    const hasLiveSession = Effect.fn("Orchestrator.hasLiveSession")(function* (
      providerThreadId: OrchestrationV2ProviderThread["id"],
    ) {
      const known = liveness.get(providerThreadId);
      if (known !== undefined) return known;
      const sessionId = input.projection.providerThreads.find(
        (candidate) => candidate.id === providerThreadId,
      )?.providerSessionId;
      const live =
        sessionId !== null &&
        sessionId !== undefined &&
        Option.isSome(
          yield* providerSessions.get(sessionId).pipe(Effect.orElseSucceed(() => Option.none())),
        );
      liveness.set(providerThreadId, live);
      return live;
    });
    const stillReportsBackgroundWork = Effect.fnUntraced(function* (
      providerThread: OrchestrationV2ProviderThread,
    ) {
      if (providerThread.providerSessionId == null) return false;
      const session = yield* providerSessions
        .get(providerThread.providerSessionId)
        .pipe(Effect.orElseSucceed(() => Option.none()));
      if (Option.isNone(session) || session.value.hasPendingBackgroundWorkForThread === undefined) {
        return false;
      }
      return yield* session.value
        .hasPendingBackgroundWorkForThread(providerThread)
        // A failed probe cannot confirm that the work ended.
        .pipe(Effect.catchCause(() => Effect.succeed(true)));
    });
    for (const item of pendingBackgroundTurnItems({
      turnItems: input.projection.turnItems,
      runs: input.projection.runs,
    })) {
      const itemRunOrdinal = item.runId === null ? undefined : runOrdinals.get(item.runId);
      if (itemRunOrdinal !== undefined && itemRunOrdinal > input.throughRunOrdinal) continue;
      const providerThreadId = item.providerThreadId ?? null;
      if (
        providerThreadId !== null &&
        providerThreadId !== input.stoppedProviderThreadId &&
        (yield* hasLiveSession(providerThreadId))
      ) {
        continue;
      }
      const run = input.projection.runs.find((candidate) => candidate.id === item.runId);
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: input.command.threadId,
        ...(item.runId === null ? {} : { runId: item.runId }),
        ...(item.nodeId === null ? {} : { nodeId: item.nodeId }),
        ...(run === undefined ? {} : { providerInstanceId: run.providerInstanceId }),
        occurredAt: input.now,
        payload: {
          ...item,
          status: "interrupted",
          completedAt: input.now,
          updatedAt: input.now,
        },
      });
      if (item.type !== "subagent") continue;
      // A native subagent died with the process too.
      yield* endNativeSubagentTrees({
        command: input.command,
        events: input.events,
        dying: [{ projection: input.projection, subagent: item }],
        childThreadIds,
        now: input.now,
      });
    }
    for (const providerThread of input.projection.providerThreads) {
      if ((providerThread.pendingBackgroundTasks?.length ?? 0) === 0) continue;
      if (providerThread.id === input.stoppedProviderThreadId) {
        // A refused newer start can take ownership and reject the stopped
        // turn's roster clear. Reconcile it without hiding surviving work.
        if (yield* stillReportsBackgroundWork(providerThread)) continue;
      } else if (yield* hasLiveSession(providerThread.id)) {
        continue;
      }
      yield* emitEvent({
        type: "provider-thread.updated",
        threadId: input.command.threadId,
        driver: providerThread.driver,
        providerInstanceId: providerThread.providerInstanceId,
        occurredAt: input.now,
        payload: {
          ...providerThread,
          status:
            providerThread.status === "active" &&
            providerThread.lastRunOrdinal !== null &&
            providerThread.lastRunOrdinal <= input.throughRunOrdinal
              ? "idle"
              : providerThread.status,
          pendingBackgroundTasks: [],
          updatedAt: input.now,
        },
      });
    }
  });

  const runInterruptReason = Effect.fnUntraced(function* (
    command: Extract<
      OrchestrationV2ServerCommand,
      { readonly type: "run.interrupt" | "thread.background-work.settle" }
    >,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    runId: RunId,
  ) {
    const isRequest = (
      item: OrchestrationV2TurnItem,
    ): item is Extract<OrchestrationV2TurnItem, { readonly type: "run_interrupt_request" }> =>
      item.runId === runId && item.type === "run_interrupt_request";
    const pending = (yield* Ref.get(events))
      .flatMap((event) => (event.type === "turn-item.updated" ? [event.payload] : []))
      .findLast(isRequest);
    if (pending !== undefined) return pending.message;
    const { turnItems } = yield* projectionStore
      .getThreadRecords(command.threadId, ["turnItems"], {
        turnItemTypes: ["run_interrupt_request"],
        turnItemRunIds: [runId],
      })
      .pipe(mapDispatchError(command));
    return turnItems.find(isRequest)?.message;
  });

  // A successful Stop may have no terminal event to ingest: the session or
  // native turn already died, or its final write failed when storage filled.
  const settleInterruptedRun = Effect.fn("Orchestrator.settleInterruptedRun")(function* (input: {
    readonly command: Extract<
      OrchestrationV2ServerCommand,
      { readonly type: "run.interrupt" | "thread.background-work.settle" }
    >;
    readonly projection: Pick<
      OrchestrationV2ThreadProjection,
      | "runs"
      | "attempts"
      | "nodes"
      | "subagents"
      | "runtimeRequests"
      | "messages"
      | "turnItems"
      | "providerThreads"
    >;
    readonly providerTurn: OrchestrationV2ProviderTurn;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>;
    readonly now: DateTime.Utc;
  }) {
    const attempt = input.projection.attempts.find(
      (candidate) => candidate.id === input.providerTurn.runAttemptId,
    );
    const run = input.projection.runs.find((candidate) => candidate.id === attempt?.runId);
    const rootNode = input.projection.nodes.find((candidate) => candidate.id === run?.rootNodeId);
    if (
      attempt === undefined ||
      run === undefined ||
      rootNode === undefined ||
      run.activeAttemptId !== attempt.id ||
      run.status !== "running" ||
      attempt.status === "pending"
    )
      return;
    const emitEvent = emit(input.events, input.command);
    const base = {
      threadId: input.command.threadId,
      runId: run.id,
      nodeId: rootNode.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: input.now,
    };
    if (input.providerTurn.status === "running") {
      yield* emitEvent({
        ...base,
        type: "provider-turn.updated",
        payload: {
          ...input.providerTurn,
          status: "interrupted",
          completedAt: input.now,
        },
      });
    }
    // An attempt that already ended keeps its status; only its run was left running.
    if (attempt.status === "running") {
      yield* emitEvent({
        ...base,
        type: "run-attempt.updated",
        payload: {
          ...attempt,
          status: "interrupted",
          completedAt: input.now,
        },
      });
    }
    const providerThread = input.projection.providerThreads.find(
      (candidate) => candidate.id === input.providerTurn.providerThreadId,
    );
    if (providerThread?.status === "active" && providerThread.lastRunOrdinal === run.ordinal) {
      yield* emitEvent({
        ...base,
        type: "provider-thread.updated",
        payload: { ...providerThread, status: "idle", updatedAt: input.now },
      });
    }
    for (const message of input.projection.messages) {
      if (message.runId !== run.id || !message.streaming) continue;
      yield* emitEvent({
        ...base,
        ...(message.nodeId === null ? {} : { nodeId: message.nodeId }),
        type: "message.updated",
        payload: { ...message, streaming: false, updatedAt: input.now },
      });
    }
    const delegatedNodeIds = new Set(
      input.projection.subagents
        .filter((subagent) => subagent.origin === "app_owned")
        .map((subagent) => subagent.id),
    );
    // Callers load only tool and message items; the request cards close with the requests below.
    const requestCards = yield* projectionStore
      .getThreadRecords(run.threadId, ["turnItems"], {
        turnItemTypes: ["approval_request", "user_input_request"],
        turnItemRunIds: [run.id],
        turnItemStatuses: ["waiting"],
      })
      .pipe(mapDispatchError(input.command));
    const cascaded = yield* RunExecutionService.cascadeTerminalizeRunOwnedSubagents({
      run,
      status: "interrupted",
      completedAt: input.now,
      allocateEventId: () =>
        idAllocator.allocate.event({
          threadId: input.command.threadId,
          commandId: input.command.commandId,
        }),
      open: {
        subagents: new Map(
          input.projection.subagents
            .filter((task) => task.runId === run.id && task.origin !== "app_owned")
            .map((task) => [task.id, task]),
        ),
        nodes: new Map(
          input.projection.nodes
            .filter((node) => node.runId === run.id && !delegatedNodeIds.has(node.id))
            .map((node) => [node.id, node]),
        ),
        turnItems: new Map(),
        childTurnItems: new Map(
          [
            ...input.projection.turnItems.filter(
              (item) =>
                item.runId === run.id &&
                (item.type === "assistant_message" || item.type === "reasoning"),
            ),
            ...requestCards.turnItems,
          ].map((item) => [item.id, item]),
        ),
        linkedChildThreadIds: new Set([run.threadId]),
      },
    }).pipe(mapDispatchError(input.command));
    yield* Ref.update(input.events, (events) => [...events, ...cascaded]);
    for (const request of input.projection.runtimeRequests) {
      if (
        request.status !== "pending" ||
        !input.projection.nodes.some((node) => node.id === request.nodeId && node.runId === run.id)
      )
        continue;
      yield* emitEvent({
        ...base,
        nodeId: request.nodeId,
        type: "runtime-request.updated",
        payload: {
          ...request,
          status: "cancelled",
          resolvedAt: input.now,
          responseCapability: { type: "not_resumable", reason: "The run was interrupted." },
        },
      });
    }
    if (providerThread !== undefined) {
      yield* emitEvent({
        ...base,
        type: "turn-item.updated",
        payload: RunExecutionService.makeInterruptResultTurnItem({
          idAllocator,
          run,
          rootNode,
          providerThread,
          reason:
            (input.command.type === "thread.background-work.settle"
              ? input.command.reason
              : undefined) ?? (yield* runInterruptReason(input.command, input.events, run.id)),
          completedAt: input.now,
        }),
      });
    }
    const { delegatedCompletion: _delegatedCompletion, ...runWithoutDelegatedCompletion } = run;
    yield* emitEvent({
      ...base,
      type: "run.updated",
      payload: {
        ...runWithoutDelegatedCompletion,
        status: "interrupted",
        completedAt: input.now,
      },
    });
    const scopeId = rootNode.checkpointScopeId;
    if (scopeId !== null) {
      yield* Ref.update(input.effects, (effects) => [
        ...effects,
        {
          id: `effect:checkpoint.capture:${run.id}`,
          commandId: CommandId.make(`command:effect:checkpoint.capture:${run.id}`),
          threadId: input.command.threadId,
          request: {
            type: "checkpoint.capture",
            runId: run.id,
            scopeId,
          },
        } satisfies PendingOrchestrationEffectV2,
      ]);
    }
  });

  /** Late tool calls and restart continuations must not outlive Stop. */
  const stopReachedRun = (
    command: OrchestrationV2ServerCommand,
    threadId: ThreadId,
    runId: RunId,
  ) => projectionStore.hasRunInterruptRequest(threadId, runId).pipe(mapDispatchError(command));

  /** The thread's wait-endable run that Stop has not reached, with its root node. */
  const endableWait = Effect.fnUntraced(function* (
    projection: Pick<
      OrchestrationV2ThreadProjection,
      "runs" | "nodes" | "providerTurns" | "runtimeRequests"
    >,
  ) {
    const run = waitEndableRun(projection);
    const rootNode = projection.nodes.find((node) => node.id === run?.rootNodeId);
    if (
      run === undefined ||
      rootNode === undefined ||
      (yield* projectionStore.hasRunInterruptRequest(run.threadId, run.id))
    )
      return undefined;
    return { run, rootNode };
  });

  // The run completes now. Its capture, ahead of any later start in the thread's effect order,
  // records its checkpoint afterwards, as it does a stopped run's.
  const runWaitEndEvents = (
    wait: { readonly run: OrchestrationV2Run; readonly rootNode: OrchestrationV2ExecutionNode },
    now: DateTime.Utc,
  ): ReadonlyArray<Omit<OrchestrationV2DomainEvent, "id">> => {
    const { delegatedCompletion: _delegatedCompletion, ...run } = wait.run;
    const scope = {
      threadId: run.threadId,
      runId: run.id,
      nodeId: wait.rootNode.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
    };
    return [
      { ...scope, type: "run.updated", payload: { ...run, status: "completed", completedAt: now } },
      {
        ...scope,
        type: "node.updated",
        payload: { ...wait.rootNode, status: "completed", completedAt: now },
      },
    ];
  };

  // A run reaching its wait yields to the visible message its queue would start next.
  const endWaitForQueuedMessage = Effect.fn("Orchestrator.endWaitForQueuedMessage")(function* (
    threadId: ThreadId,
  ) {
    const active = yield* projectionStore.getThreadRecords(threadId, ["runs"], {
      runStatuses: ["preparing", "starting", "running", "waiting"],
    });
    const waiting = active.runs.find((run) => run.status === "waiting");
    if (
      waiting === undefined ||
      active.thread.archivedAt !== null ||
      active.thread.deletedAt !== null
    )
      return;
    const { runs: queued } = yield* projectionStore.getThreadRecords(threadId, ["runs"], {
      runStatuses: ["queued"],
    });
    // Held runs never start next, and a thread can hold over a thousand of them.
    const startable = [...active.runs, ...queued.filter((run) => run.queueHeld !== true)];
    if (startable.length === active.runs.length) return;
    const records = yield* projectionStore.getThreadRecords(
      threadId,
      ["messages", "nodes", "providerTurns", "runtimeRequests"],
      {
        messageIds: startable.flatMap((run) =>
          run.status === "queued" ? [run.userMessageId] : [],
        ),
        nodeIds: waiting.rootNodeId === null ? [] : [waiting.rootNodeId],
        providerTurnAttemptIds: waiting.activeAttemptId === null ? [] : [waiting.activeAttemptId],
        runtimeRequestStatuses: ["pending"],
      },
    );
    const next = nextQueuedRun({ runs: startable, messages: records.messages });
    const message = records.messages.find((candidate) => candidate.id === next?.userMessageId);
    if (
      next === undefined ||
      message === undefined ||
      isHiddenWake(message) ||
      next.restartContinuationOfRunId !== undefined
    )
      return;
    const wait = yield* endableWait({ ...records, runs: active.runs });
    if (wait === undefined || wait.run.activeAttemptId === null) return;
    const now = yield* DateTime.now;
    // The capture commits outside this lock; a run it completed meanwhile keeps its checkpoint,
    // and one it is capturing gets its end from the capture.
    yield* eventSink.writeIfRunCurrent({
      threadId,
      runId: wait.run.id,
      activeAttemptId: wait.run.activeAttemptId,
      expectedStatus: "waiting",
      unlessEffectRunning: `effect:checkpoint.capture:${wait.run.id}`,
      events: yield* Effect.forEach(runWaitEndEvents(wait, now), makeSystemEvent),
    });
  });

  // Visible input queued behind a run that only waits for its checkpoint ends that wait.
  const endWaitAfterCommand = (command: OrchestrationV2ServerCommand) =>
    (command.type === "message.dispatch" &&
      command.notification === undefined &&
      command.delegatedCompletion === undefined &&
      command.restartContinuationOfRunId === undefined) ||
    command.type === "queued-message.promote-to-steer"
      ? endWaitForQueuedMessage(command.threadId).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to end a waiting run for its queued message", {
              threadId: command.threadId,
              cause,
            }),
          ),
        )
      : Effect.void;

  const markStoppedRun = Effect.fnUntraced(function* (input: {
    readonly command: Extract<
      OrchestrationV2ServerCommand,
      { readonly type: "run.interrupt" | "thread.stop" }
    >;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly thread: OrchestrationV2AppThread;
    readonly run: OrchestrationV2Run;
    readonly now: DateTime.Utc;
  }) {
    const { command, events, thread, run, now } = input;
    if (run.rootNodeId === null || run.providerThreadId === null) return;
    const pendingItems = (yield* Ref.get(events)).flatMap((event) =>
      event.type === "turn-item.updated" ? [event.payload] : [],
    );
    const prior =
      pendingItems.findLast(
        (item) => item.runId === run.id && item.type === "run_interrupt_request",
      ) ??
      (yield* projectionStore
        .getThreadRecords(thread.id, ["turnItems"], {
          turnItemTypes: ["run_interrupt_request"],
          turnItemRunIds: [run.id],
        })
        .pipe(mapDispatchError(command))).turnItems.findLast(
        (item) => item.type === "run_interrupt_request",
      );
    const queueHoldGeneration = thread.queueReleaseGeneration ?? 0;
    if (prior?.type === "run_interrupt_request") {
      if (prior.holdQueue !== true || (prior.queueHoldGeneration ?? 0) !== queueHoldGeneration) {
        yield* emit(
          events,
          command,
        )({
          type: "turn-item.updated",
          threadId: thread.id,
          runId: run.id,
          nodeId: run.rootNodeId,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...prior, holdQueue: true, queueHoldGeneration, updatedAt: now },
        });
      }
      return;
    }
    yield* emit(
      events,
      command,
    )({
      type: "turn-item.updated",
      threadId: thread.id,
      runId: run.id,
      nodeId: run.rootNodeId,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: {
        id: idAllocator.derive.runSignalTurnItem({ runId: run.id, signal: "interrupt-request" }),
        threadId: thread.id,
        runId: run.id,
        nodeId: run.rootNodeId,
        providerThreadId: run.providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: yield* nextTurnItemOrdinal({ thread, turnItems: pendingItems }),
        status: "completed",
        title: "Interrupt requested",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "run_interrupt_request",
        holdQueue: true,
        queueHoldGeneration,
        message: command.reason ?? RunExecutionService.USER_STOP_REASON,
      },
    });
  });

  const holdStoppedThread = Effect.fn("Orchestrator.holdStoppedThread")(function* (input: {
    readonly command: Extract<
      OrchestrationV2ServerCommand,
      { readonly type: "run.interrupt" | "thread.stop" }
    >;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly projection: Pick<OrchestrationV2ThreadProjection, "thread" | "runs" | "subagents">;
    readonly cohortRunIds: ReadonlyArray<RunId>;
    readonly now: DateTime.Utc;
  }) {
    const emitEvent = emit(input.events, input.command);
    const { runs, subagents } = input.projection;
    let { thread } = input.projection;
    let recovery = thread.limitRecovery;
    const latest = latestExecutedRun(runs);
    if (latest?.status === "failed") {
      const { turnItems } = yield* projectionStore
        .getThreadRecords(thread.id, ["turnItems"], {
          turnItemTypes: ["error"],
          turnItemRunIds: [latest.id],
        })
        .pipe(mapDispatchError(input.command));
      const failure = latestRootProviderFailure(latest, turnItems);
      if (
        failure?.class === "usage_limit" &&
        failure.resetAt != null &&
        Number.isFinite(Date.parse(failure.resetAt)) &&
        (recovery?.runId !== latest.id || recovery.resetAt !== failure.resetAt)
      ) {
        recovery = {
          runId: latest.id,
          resetAt: failure.resetAt,
          autoResume: false,
          snooze: false,
        };
      }
    }
    if (recovery && (recovery !== thread.limitRecovery || recovery.autoResume)) {
      thread = {
        ...thread,
        limitRecovery: {
          ...recovery,
          autoResume: false,
          requestId: input.command.commandId,
        },
        updatedAt: input.now,
      };
      yield* emitEvent({
        type: "thread.metadata-updated",
        threadId: thread.id,
        occurredAt: input.now,
        payload: thread,
      });
    }
    const queuedRuns = runs.filter((run) => run.status === "queued");
    const queuedMessages =
      queuedRuns.length === 0
        ? []
        : (yield* projectionStore
            .getThreadRecords(thread.id, ["messages"], {
              messageIds: queuedRuns.map((run) => run.userMessageId),
            })
            .pipe(
              Effect.mapError(
                (cause) => new OrchestratorProjectionError({ threadId: thread.id, cause }),
              ),
            )).messages;
    const bufferedMessageIds = new Set(
      queuedMessages.filter(isBufferedProviderNotification).map((message) => message.id),
    );
    for (const run of runs) {
      // A resumed queue can have a lower ordinal than a pending continuation source.
      // Stop must reach those sources even when interrupting the resumed run succeeds.
      if (
        (run.status === "cancelled" ||
          (run.status === "failed" && run.restartContinuationOfRunId !== undefined)) &&
        (yield* awaitsRestartContinuation(run).pipe(mapDispatchError(input.command)))
      ) {
        yield* markStoppedRun({ ...input, thread, run });
      }
      if (run.status !== "queued") continue;
      if (bufferedMessageIds.has(run.userMessageId)) {
        yield* dispatchQueuedRunCancel(
          {
            type: "queued-run.cancel",
            commandId: input.command.commandId,
            threadId: thread.id,
            runId: run.id,
          },
          input.events,
        );
        continue;
      }
      if (run.queueHeld === true) continue;
      yield* emitEvent({
        type: "run.updated",
        threadId: thread.id,
        runId: run.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: input.now,
        payload: { ...run, queueHeld: true },
      });
    }
    const pullRequests = thread.pullRequests ?? [];
    if (pullRequests.some((link) => link.watch !== undefined)) {
      yield* emitEvent({
        type: "thread.pull-request-synced",
        threadId: thread.id,
        providerInstanceId: thread.providerInstanceId,
        occurredAt: input.now,
        payload: {
          ...thread,
          pullRequests: pullRequests.map((link) => withPullRequestWatch(link, undefined)),
        },
      });
    }
    const cohortRunIds = new Set(input.cohortRunIds);
    for (const run of runs) {
      if (run.delegatedCompletion?.delivery != null) cohortRunIds.add(run.id);
    }
    for (const task of subagents) {
      const delivery = task.completionDelivery?.state;
      if (
        task.origin === "app_owned" &&
        task.runId !== null &&
        delivery !== "acknowledged" &&
        delivery !== "delivered" &&
        delivery !== "disposed"
      ) {
        cohortRunIds.add(task.runId);
      }
    }
    // Each disposal reads the events the ones before it wrote.
    for (const parentRunId of cohortRunIds) {
      yield* disposeDelegatedCompletionCohort({
        command: input.command,
        events: input.events,
        projection: yield* getProjectionWithPendingEvents(thread.id, input.events),
        parentRunId,
        disposition: "stopped",
        now: input.now,
      });
    }
    yield* closePendingLinkRequests({ ...input, threadId: thread.id });
  });

  const dispatchInterruptUnresponsive = Effect.fn("Orchestrator.dispatchInterruptUnresponsive")(
    function* (
      command: Extract<
        OrchestrationV2InternalCommand,
        { readonly type: "thread.interrupt.unresponsive" }
      >,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    ) {
      const { providerTurn } = yield* projectionStore
        .getProviderControlContext(command.threadId, {
          providerThreadId: command.providerThreadId,
          providerTurnId: command.providerTurnId,
        })
        .pipe(mapDispatchError(command));
      const { runs } = yield* projectionStore
        .getThreadRecords(command.threadId, ["runs"])
        .pipe(mapDispatchError(command));
      const run = runs.find(
        (candidate) =>
          providerTurn !== undefined && candidate.activeAttemptId === providerTurn.runAttemptId,
      );
      // A settled run still counts: Stop may have targeted its background work.
      if (run === undefined) return;
      const { turnItems } = yield* projectionStore
        .getThreadRecords(command.threadId, ["turnItems"], {
          turnItemTypes: ["run_interrupt_request", "run_interrupt_result"],
          turnItemRunIds: [run.id],
        })
        .pipe(mapDispatchError(command));
      // A provider that stopped late already wrote the result; it needs no Force stop.
      if (turnItems.some((item) => item.type === "run_interrupt_result")) return;
      const request = turnItems.find((item) => item.type === "run_interrupt_request");
      if (request?.type !== "run_interrupt_request" || request.unresponsiveAt !== undefined) return;
      const now = yield* DateTime.now;
      yield* emit(
        events,
        command,
      )({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId: run.id,
        ...(request.nodeId === null ? {} : { nodeId: request.nodeId }),
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: { ...request, unresponsiveAt: now, updatedAt: now },
      });
    },
  );

  const dispatchBackgroundWorkSettle = Effect.fn("Orchestrator.dispatchBackgroundWorkSettle")(
    function* (
      command: Extract<
        OrchestrationV2InternalCommand,
        { readonly type: "thread.background-work.settle" }
      >,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    ) {
      const [projection, stopped] = yield* Effect.all([
        projectionStore.getThreadRecords(
          command.threadId,
          [
            "runs",
            "attempts",
            "nodes",
            "subagents",
            "runtimeRequests",
            "turnItems",
            "providerThreads",
          ],
          {
            turnItemTypes: [
              "command_execution",
              "dynamic_tool",
              "subagent",
              "assistant_message",
              "reasoning",
            ],
            turnItemStatuses: ["pending", "running", "waiting"],
          },
        ),
        projectionStore.getProviderControlContext(command.threadId, {
          providerThreadId: command.providerThreadId,
          providerTurnId: command.providerTurnId,
        }),
      ]).pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
        ),
      );
      if (command.subagent !== undefined) {
        const outcome = command.subagent.outcome;
        const subagent = projection.subagents.find((task) => task.id === command.subagent?.id);
        if (
          subagent?.origin !== "provider_native" ||
          !["pending", "running", "waiting"].includes(subagent.status) ||
          (subagent.startedAt !== null &&
            DateTime.toEpochMillis(subagent.startedAt) >
              DateTime.toEpochMillis(command.subagent.stoppedAt))
        )
          return;
        const now = yield* DateTime.now;
        const allocateEventId = () =>
          idAllocator.allocate.event({
            threadId: command.threadId,
            commandId: command.commandId,
          });
        const ended = yield* endOrphanedNativeSubagent({
          projection,
          subagentId: subagent.id,
          result: outcome.result,
          status: outcome.status,
          completedAt: outcome.completedAt,
          now,
          emitted: yield* Ref.get(events),
          allocateEventId,
        }).pipe(mapDispatchError(command));
        yield* Ref.update(events, (current) => [...current, ...ended]);
        if (subagent.childThreadId !== null) {
          const child = yield* projectionStore
            .getThreadRecords(subagent.childThreadId, ["nodes", "providerTurns", "turnItems"], {
              turnItemRunIds: [null],
              turnItemStatuses: ["pending", "running", "waiting"],
            })
            .pipe(
              Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
              mapDispatchError(command),
            );
          if (child !== null) {
            const childEnded = yield* endRunlessRootTurns({
              threadId: subagent.childThreadId,
              providerInstanceId: subagent.providerInstanceId,
              projection: child,
              status: outcome.status,
              completedAt: outcome.completedAt,
              now,
              emitted: yield* Ref.get(events),
              allocateEventId,
            }).pipe(mapDispatchError(command));
            yield* Ref.update(events, (current) => [...current, ...childEnded]);
          }
        }
        return;
      }
      const stoppedRunId = projection.attempts.find(
        (attempt) => attempt.id === stopped.providerTurn?.runAttemptId,
      )?.runId;
      const stoppedRun = projection.runs.find((run) => run.id === stoppedRunId);
      if (
        stoppedRun === undefined ||
        (["preparing", "starting", "running"].includes(stoppedRun.status) &&
          stoppedRun.activeAttemptId !== stopped.providerTurn?.runAttemptId)
      )
        return;
      const now = yield* DateTime.now;
      if (stopped.providerTurn !== undefined && stoppedRun.status === "running") {
        const output = yield* projectionStore
          .getThreadRecords(command.threadId, ["messages"], {
            messageRunIds: [stoppedRun.id],
            messageRoles: ["assistant"],
          })
          .pipe(mapDispatchError(command));
        yield* settleInterruptedRun({
          command,
          projection: { ...projection, messages: output.messages },
          providerTurn: stopped.providerTurn,
          events,
          effects,
          now,
        });
      }
      // A new turn may have started since Stop; its work is not this Stop's.
      if (
        projection.runs.some(
          (run) =>
            run.id !== stoppedRun.id &&
            (run.status === "preparing" || run.status === "starting" || run.status === "running"),
        )
      ) {
        return;
      }
      yield* settleBackgroundWork({
        command,
        events,
        projection,
        stoppedProviderThreadId: command.providerThreadId,
        throughRunOrdinal: stoppedRun.ordinal,
        now,
      });
    },
  );

  const dispatchSubagentStop = Effect.fn("Orchestrator.dispatchSubagentStop")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "subagent.stop" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
  ) {
    const projection = yield* loadProjectionForCommand(command, [
      "runs",
      "subagents",
      "providerThreads",
      "providerTurns",
    ]);
    const run = projection.runs.find((candidate) => candidate.id === command.runId);
    const subagent = projection.subagents.find((candidate) => candidate.id === command.subagentId);
    // User Stop cancels app-owned tasks with no bound child run unless they are the child's
    // original task, regardless of other child work. Bound runs keep their settlement path
    // because results may still arrive; the client's next child stop interrupts the run.
    // Rewrite ended tasks so clients with stale copies see them end.
    if (
      subagent?.origin === "app_owned" &&
      subagent.childThreadId !== null &&
      subagent.runId === command.runId
    ) {
      const childThreadId = subagent.childThreadId;
      const child = yield* projectionStore.getThreadRecords(childThreadId, ["runs"]).pipe(
        Effect.catchTags({ ProjectionStoreThreadNotFoundError: () => Effect.succeed(null) }),
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: childThreadId, cause }),
        ),
      );
      const active = isOrchestrationV2WorkActive(subagent.status);
      if (
        active &&
        child !== null &&
        ((child.thread.forkedFrom?.type === "node" &&
          child.thread.forkedFrom.nodeId === subagent.id) ||
          child.runs.some((run) => run.delegatedTaskId === subagent.id))
      )
        return;
      const records = yield* loadProjectionForCommand(command, ["nodes", "turnItems"], {
        turnItemTypes: ["subagent"],
        turnItemRunIds: [subagent.runId],
        omitSettledLeafNodes: true,
      });
      const now = yield* DateTime.now;
      return yield* writeAppOwnedTask({
        command,
        events,
        task: active ? cancelledAppOwnedTask(subagent, now) : subagent,
        nodes: records.nodes,
        turnItems: records.turnItems,
        now,
      });
    }
    if (
      subagent === undefined ||
      subagent.runId !== command.runId ||
      subagent.origin !== "provider_native" ||
      subagent.driver !== "claudeAgent" ||
      subagent.nativeTaskRef?.strength !== "strong" ||
      subagent.nativeTaskRef.nativeId === null
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "The requested native subagent cannot be stopped on this run.",
      });
    }
    if (!["pending", "running", "waiting"].includes(subagent.status)) return;
    const providerThread = projection.providerThreads.find(
      (candidate) => candidate.id === run?.providerThreadId,
    );
    const providerTurn = projection.providerTurns.findLast(
      (candidate) => candidate.providerThreadId === providerThread?.id,
    );
    if (
      providerThread?.providerSessionId == null ||
      providerTurn === undefined ||
      providerThread.driver !== subagent.driver ||
      providerThread.providerInstanceId !== subagent.providerInstanceId
    ) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: "The requested native subagent has no owning provider session.",
      });
    }
    const providerSessionId = providerThread.providerSessionId;
    const nativeTaskId = subagent.nativeTaskRef.nativeId;
    const now = yield* DateTime.now;
    yield* emit(
      events,
      command,
    )({
      type: "subagent.updated",
      threadId: command.threadId,
      runId: command.runId,
      nodeId: subagent.id,
      providerInstanceId: subagent.providerInstanceId,
      occurredAt: now,
      payload: { ...subagent, updatedAt: now },
    });
    yield* Ref.update(effects, (existing) => [
      ...existing,
      {
        id: `effect:${command.commandId}:subagent.stop:${subagent.id}`,
        commandId: command.commandId,
        threadId: command.threadId,
        request: {
          type: "provider-turn.interrupt",
          providerSessionId,
          providerThreadId: providerThread.id,
          providerTurnId: providerTurn.id,
          subagent: { id: subagent.id, nativeTaskId },
        },
      } satisfies PendingOrchestrationEffectV2,
    ]);
    return;
  });

  const dispatchRunInterrupt = Effect.fn("Orchestrator.dispatchRunInterrupt")(function* (
    command: Extract<OrchestrationV2Command, { readonly type: "run.interrupt" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
  ): Effect.fn.Return<
    | {
        readonly effectTypes: ReadonlyArray<OrchestrationEffectRequestV2["type"]>;
        readonly reason: string;
      }
    | undefined,
    OrchestratorV2Error
  > {
    const projection = yield* loadProjectionForCommand(
      command,
      [
        "runs",
        "nodes",
        "providerThreads",
        "providerTurns",
        "messages",
        "attempts",
        "turnItems",
        "subagents",
        "runtimeRequests",
      ],
      // Thread-wide, like the Waiting strip: a settled thread's background
      // work can belong to an earlier run than the one Stop targets.
      {
        turnItemTypes: [
          "command_execution",
          "dynamic_tool",
          "subagent",
          "assistant_message",
          "reasoning",
        ],
        turnItemStatuses: ["pending", "running", "waiting"],
        // Stop reads root nodes and ends open ones; a settled leaf has nothing left to end.
        omitSettledLeafNodes: true,
      },
    );
    const run = projection.runs.find((candidate) => candidate.id === command.runId);
    const rootNode =
      run?.rootNodeId === null
        ? undefined
        : projection.nodes.find((candidate) => candidate.id === run?.rootNodeId);
    const providerThread =
      run?.providerThreadId === null
        ? undefined
        : projection.providerThreads.find((candidate) => candidate.id === run?.providerThreadId);
    const hasBackgroundWork =
      run?.id === projection.runs.findLast((candidate) => candidate.status !== "queued")?.id &&
      derivePendingBackgroundWork({
        latestRun: run,
        providerThreads: projection.providerThreads,
        turnItems: projection.turnItems,
        activeProviderThreadId: projection.thread.activeProviderThreadId,
        runs: projection.runs,
      }).length > 0;
    // A failed start has no provider turn. Background work still belongs
    // to the provider thread, so Stop reaches its latest accepted turn.
    const providerTurn =
      projection.providerTurns.findLast(
        (candidate) =>
          candidate.runAttemptId === run?.activeAttemptId &&
          (run?.status === "running" || candidate.status === "running" || hasBackgroundWork),
      ) ??
      (hasBackgroundWork
        ? projection.providerTurns.findLast(
            (candidate) => candidate.providerThreadId === run?.providerThreadId,
          )
        : undefined);
    if (run === undefined || rootNode === undefined || providerThread === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Run ${command.runId} is not interruptible.`,
      });
    }
    const stoppedSteers =
      command.holdQueue === true
        ? {
            effectTypes: ["provider-turn.steer"] as const,
            reason: `Run ${run.id} was stopped before steering delivery.`,
          }
        : undefined;
    const now = yield* DateTime.now;
    const completionMessage = projection.messages.find(
      (candidate) => candidate.id === run.userMessageId,
    );
    const completionCohortRunIds =
      completionMessage?.delegatedCompletion === undefined
        ? [run.id]
        : completionWakeParts(completionMessage.delegatedCompletion).map(
            (part) => part.parentRunId,
          );
    const stopRemainingWork = () =>
      command.holdQueue === true
        ? holdStoppedThread({
            command,
            events,
            projection,
            cohortRunIds: completionCohortRunIds,
            now,
          })
        : Effect.gen(function* () {
            for (const parentRunId of new Set([...completionCohortRunIds, run.id])) {
              yield* disposeDelegatedCompletionCohort({
                command,
                events,
                projection: yield* getProjectionWithPendingEvents(command.threadId, events),
                parentRunId,
                disposition: "stopped",
                now,
                ...(parentRunId !== run.id && completionMessage?.delegatedCompletion !== undefined
                  ? { interruptedWakeTaskIds: completionMessage.delegatedCompletion.taskIds }
                  : {}),
              });
            }
          });

    const emitEvent = emit(events, command);
    const earlierInterrupts = yield* projectionStore
      .getThreadRecords(command.threadId, ["turnItems"], {
        turnItemTypes: ["run_interrupt_request"],
        turnItemRunIds: [run.id],
      })
      .pipe(mapDispatchError(command));
    const priorHold = [
      ...earlierInterrupts.turnItems,
      ...(yield* Ref.get(events)).flatMap((event) =>
        event.type === "turn-item.updated" && event.payload.runId === run.id ? [event.payload] : [],
      ),
    ].findLast(
      (item): item is Extract<OrchestrationV2TurnItem, { type: "run_interrupt_request" }> =>
        item.type === "run_interrupt_request" && item.holdQueue === true,
    );
    const interruptRequestItem: OrchestrationV2TurnItem = {
      id: idAllocator.derive.runSignalTurnItem({
        runId: run.id,
        signal: "interrupt-request",
      }),
      threadId: command.threadId,
      runId: run.id,
      nodeId: rootNode.id,
      providerThreadId: providerThread.id,
      providerTurnId: providerTurn?.id ?? null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: yield* nextTurnItemOrdinal(projection),
      status: "completed",
      title: "Interrupt requested",
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      type: "run_interrupt_request",
      holdQueue: command.holdQueue === true || priorHold !== undefined,
      queueHoldGeneration:
        command.holdQueue === true
          ? (projection.thread.queueReleaseGeneration ?? 0)
          : (priorHold?.queueHoldGeneration ?? 0),
      message: command.reason ?? RunExecutionService.USER_STOP_REASON,
    };

    const otherProviderInterrupts: Array<PendingOrchestrationEffectV2> = [];
    if (hasBackgroundWork || command.holdQueue === true) {
      const runOrdinals = new Map(
        projection.runs.map((candidate) => [candidate.id, candidate.ordinal]),
      );
      const runOrdinalOf = (item: (typeof projection.turnItems)[number]) =>
        item.runId === null ? -1 : (runOrdinals.get(item.runId) ?? -1);
      // Interrupt each provider thread at its latest pending turn so settlement covers all its work.
      // Use the item's provider turn: native subagent items name the child thread but the parent turn,
      // and interrupting the parent turn reaches the subagent.
      const latestTurnByProviderThread = new Map<
        OrchestrationV2ProviderThread["id"],
        { readonly turn: (typeof projection.providerTurns)[number]; readonly runOrdinal: number }
      >();
      for (const item of pendingBackgroundTurnItems({
        turnItems: projection.turnItems,
        runs: projection.runs,
      })) {
        const turn = projection.providerTurns.find(
          (candidate) => candidate.id === item.providerTurnId,
        );
        if (turn === undefined || turn.providerThreadId === providerThread.id) continue;
        const runOrdinal = runOrdinalOf(item);
        const latest = latestTurnByProviderThread.get(turn.providerThreadId);
        if (latest === undefined || runOrdinal > latest.runOrdinal) {
          latestTurnByProviderThread.set(turn.providerThreadId, { turn, runOrdinal });
        }
      }
      for (const { turn } of latestTurnByProviderThread.values()) {
        const owner = projection.providerThreads.find(
          (candidate) => candidate.id === turn.providerThreadId,
        );
        if (owner === undefined || owner.providerSessionId === null) continue;
        const ownerSession = yield* providerSessions
          .get(owner.providerSessionId)
          .pipe(Effect.orElseSucceed(() => Option.none()));
        if (Option.isNone(ownerSession)) continue;
        otherProviderInterrupts.push({
          id: `effect:${command.commandId}:provider-turn.interrupt:${turn.id}`,
          commandId: command.commandId,
          threadId: command.threadId,
          request: {
            type: "provider-turn.interrupt",
            providerSessionId: owner.providerSessionId,
            providerThreadId: owner.id,
            providerTurnId: turn.id,
          },
        });
      }
    }

    const preparingRestart = projection.attempts.some(
      (candidate) =>
        candidate.id === run.activeAttemptId &&
        candidate.reason === "steering_restart" &&
        candidate.providerStartState === "pending",
    );
    if (
      (providerTurn === undefined || preparingRestart) &&
      (run.status === "preparing" || run.status === "starting" || run.status === "running")
    ) {
      const attempt = projection.attempts.find((candidate) => candidate.id === run.activeAttemptId);
      if (attempt === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Run ${command.runId} has no active attempt to interrupt.`,
        });
      }
      const interruptResultItem: OrchestrationV2TurnItem = {
        id: idAllocator.derive.runSignalTurnItem({
          runId: run.id,
          signal: "interrupt-result",
        }),
        threadId: command.threadId,
        runId: run.id,
        nodeId: rootNode.id,
        providerThreadId: providerThread.id,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: interruptRequestItem.id,
        ordinal: interruptRequestItem.ordinal + 1,
        status: "interrupted",
        title: "Interrupted",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        type: "run_interrupt_result",
        message: "Run interrupted before provider start",
      };
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId: run.id,
        nodeId: rootNode.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: interruptRequestItem,
      });
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId: run.id,
        nodeId: rootNode.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: interruptResultItem,
      });
      const preparationItem = projection.turnItems.find(
        (
          candidate,
        ): candidate is Extract<OrchestrationV2TurnItem, { readonly type: "command_execution" }> =>
          candidate.runId === run.id &&
          candidate.type === "command_execution" &&
          candidate.input === WORKSPACE_PREPARATION_INPUT &&
          candidate.status === "running",
      );
      if (preparationItem !== undefined) {
        yield* emitEvent({
          type: "turn-item.updated",
          threadId: command.threadId,
          runId: run.id,
          nodeId: rootNode.id,
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: {
            ...preparationItem,
            status: "interrupted",
            title: "Workspace preparation interrupted",
            output: command.reason ?? "Interrupted before provider start",
            completedAt: now,
            updatedAt: now,
          },
        });
      }
      yield* emitEvent({
        type: "run-attempt.updated",
        threadId: command.threadId,
        runId: run.id,
        nodeId: rootNode.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: { ...attempt, status: "interrupted", completedAt: now },
      });
      yield* emitEvent({
        type: "node.updated",
        threadId: command.threadId,
        runId: run.id,
        nodeId: rootNode.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: { ...rootNode, status: "interrupted", completedAt: now },
      });
      yield* emitEvent({
        type: "run.updated",
        threadId: command.threadId,
        runId: run.id,
        nodeId: rootNode.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: { ...run, status: "interrupted", completedAt: now },
      });
      // Stop cancels a pending restart, so it must end the turn that restart could not stop.
      const supersededTurn = projection.providerTurns.findLast(
        (candidate) =>
          candidate.status === "running" &&
          projection.attempts.some(
            (attempt) => attempt.id === candidate.runAttemptId && attempt.runId === run.id,
          ),
      );
      const supersededOwner =
        supersededTurn === undefined
          ? undefined
          : projection.providerThreads.find(
              (candidate) => candidate.id === supersededTurn.providerThreadId,
            );
      if (
        supersededTurn !== undefined &&
        supersededOwner !== undefined &&
        supersededOwner.providerSessionId !== null &&
        Option.isSome(
          yield* providerSessions
            .get(supersededOwner.providerSessionId)
            .pipe(Effect.orElseSucceed(() => Option.none())),
        )
      ) {
        const id = `effect:${command.commandId}:provider-turn.interrupt:${supersededTurn.id}`;
        if (!otherProviderInterrupts.some((effect) => effect.id === id)) {
          otherProviderInterrupts.push({
            id,
            commandId: command.commandId,
            threadId: command.threadId,
            request: {
              type: "provider-turn.interrupt",
              providerSessionId: supersededOwner.providerSessionId,
              providerThreadId: supersededOwner.id,
              providerTurnId: supersededTurn.id,
            },
          });
        }
      }
      yield* stopRemainingWork();
      // The replacement input was accepted, but its provider call has not been invoked.
      // Move that same message into a new held run so Resume cannot inherit this run's Stop marker.
      if (
        command.holdQueue === true &&
        attempt.reason === "steering_restart" &&
        (attempt.status === "pending" || attempt.providerStartState === "pending") &&
        completionMessage !== undefined
      ) {
        yield* dispatchMessage(
          {
            type: "message.dispatch",
            commandId: command.commandId,
            threadId: command.threadId,
            steerFollowUpOfRunId: run.id,
            messageId: completionMessage.id,
            text: completionMessage.text,
            attachments: completionMessage.attachments,
            ...(completionMessage.context ? { context: completionMessage.context } : {}),
            modelSelection: run.modelSelection,
            dispatchMode: { type: "queue_after_active" },
            createdBy: completionMessage.createdBy,
            creationSource: completionMessage.creationSource,
            ...(completionMessage.senderThreadId === undefined
              ? {}
              : { senderThreadId: completionMessage.senderThreadId }),
            ...(completionMessage.linkedSender === undefined
              ? {}
              : { linkedSender: completionMessage.linkedSender }),
            ...(completionMessage.scheduledTaskId === undefined
              ? {}
              : { scheduledTaskId: completionMessage.scheduledTaskId }),
          },
          events,
          effects,
        );
      }
      yield* Ref.update(effects, (existing) => [...existing, ...otherProviderInterrupts]);
      return {
        effectTypes: [
          "provider-turn.start",
          "provider-turn.restart",
          ...(stoppedSteers?.effectTypes ?? []),
        ],
        reason: `Run ${run.id} was interrupted before its provider turn started.`,
      } satisfies {
        readonly effectTypes: ReadonlyArray<OrchestrationEffectRequestV2["type"]>;
        readonly reason: string;
      };
    }

    if (providerTurn === undefined) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Run ${command.runId} is not interruptible.`,
      });
    }
    const sessionOption =
      providerThread.providerSessionId === null
        ? Option.none()
        : yield* providerSessions.get(providerThread.providerSessionId).pipe(
            Effect.mapError(
              (cause) =>
                new OrchestratorProviderAdapterError({
                  commandId: command.commandId,
                  providerInstanceId: run.providerInstanceId,
                  cause,
                }),
            ),
          );
    if (Option.isNone(sessionOption)) {
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.threadId,
        runId: run.id,
        nodeId: rootNode.id,
        providerInstanceId: run.providerInstanceId,
        occurredAt: now,
        payload: interruptRequestItem,
      });
      yield* stopRemainingWork();
      yield* settleInterruptedRun({ command, projection, providerTurn, events, effects, now });
      yield* settleBackgroundWork({
        command,
        events,
        projection,
        stoppedProviderThreadId: providerThread.id,
        throughRunOrdinal: run.ordinal,
        now,
      });
      yield* Ref.update(effects, (existing) => [...existing, ...otherProviderInterrupts]);
      return stoppedSteers;
    }
    if (providerThread.providerSessionId === null) {
      return yield* new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
        cause: `Provider thread ${providerThread.id} has no active provider session.`,
      });
    }
    const providerSessionId = providerThread.providerSessionId;
    yield* mapDispatchError(command)(
      commandPolicy.ensureInterrupt({
        commandId: command.commandId,
        threadId: command.threadId,
        providerInstanceId: run.providerInstanceId,
        capabilities: sessionOption.value.providerSession.capabilities,
      }),
    );

    yield* emitEvent({
      type: "turn-item.updated",
      threadId: command.threadId,
      runId: run.id,
      nodeId: rootNode.id,
      providerInstanceId: run.providerInstanceId,
      occurredAt: now,
      payload: interruptRequestItem,
    });
    yield* stopRemainingWork();
    yield* Ref.update(effects, (existing) => [
      ...existing,
      {
        id: `effect:${command.commandId}:provider-turn.interrupt:${providerTurn.id}`,
        commandId: command.commandId,
        threadId: command.threadId,
        request: {
          type: "provider-turn.interrupt",
          providerSessionId,
          providerThreadId: providerThread.id,
          providerTurnId: providerTurn.id,
        },
      } satisfies PendingOrchestrationEffectV2,
      ...otherProviderInterrupts,
    ]);
    return stoppedSteers;
  });

  /** Writes a parent's app-owned task, and sets its node and turn items to the task's status. */
  const writeAppOwnedTask = Effect.fn("Orchestrator.writeAppOwnedTask")(function* (input: {
    readonly command: Extract<
      OrchestrationV2ServerCommand,
      { readonly type: "run.interrupt" | "thread.stop" | "subagent.stop" }
    >;
    readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
    readonly task: OrchestrationV2Subagent;
    readonly nodes: ReadonlyArray<OrchestrationV2ExecutionNode>;
    readonly turnItems: ReadonlyArray<OrchestrationV2TurnItem>;
    readonly now: DateTime.Utc;
  }) {
    const { command, task, now } = input;
    const emitEvent = emit(input.events, command);
    yield* emitEvent({
      type: "subagent.updated",
      threadId: command.threadId,
      nodeId: task.id,
      occurredAt: now,
      payload: task,
    });
    const node = input.nodes.find((candidate) => candidate.id === task.id);
    if (node !== undefined)
      yield* emitEvent({
        type: "node.updated",
        threadId: command.threadId,
        nodeId: node.id,
        occurredAt: now,
        payload: { ...node, status: task.status, completedAt: task.completedAt },
      });
    for (const item of input.turnItems) {
      if (item.type !== "subagent" || item.subagentId !== task.id) continue;
      yield* emitEvent({
        type: "turn-item.updated",
        threadId: command.threadId,
        nodeId: task.id,
        occurredAt: now,
        payload: { ...item, status: task.status, completedAt: task.completedAt, updatedAt: now },
      });
    }
  });

  const cancelIdleOrphans = Effect.fn("Orchestrator.cancelIdleOrphans")(
    function* (input: {
      readonly command: Extract<
        OrchestrationV2ServerCommand,
        { readonly type: "run.interrupt" | "thread.stop" }
      >;
      readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
      readonly projection: Pick<OrchestrationV2ThreadProjection, "subagents" | "turnItems">;
    }) {
      const { command, events, projection } = input;
      const pending = yield* Ref.get(events);
      const children = new Map<
        ThreadId,
        ProjectionRecords<"runs" | "providerThreads" | "runtimeRequests" | "turnItems"> | null
      >();
      const idleChildren = new Map<ThreadId, boolean>();
      let nodes: ReadonlyArray<OrchestrationV2ExecutionNode> | undefined;
      for (const recorded of projection.subagents) {
        const updated = pending.findLast(
          (event) => event.type === "subagent.updated" && event.payload.id === recorded.id,
        );
        const task = updated?.type === "subagent.updated" ? updated.payload : recorded;
        if (
          task.origin !== "app_owned" ||
          task.childThreadId === null ||
          !isOrchestrationV2WorkActive(task.status)
        )
          continue;
        const childThreadId = task.childThreadId;
        const child = children.has(childThreadId)
          ? (children.get(childThreadId) ?? null)
          : yield* projectionStore
              .getThreadRecords(
                childThreadId,
                ["runs", "providerThreads", "runtimeRequests", "turnItems"],
                {
                  turnItemTypes: ["command_execution", "dynamic_tool", "subagent"],
                  turnItemStatuses: ["pending", "running", "waiting"],
                },
              )
              .pipe(
                Effect.catchTags({
                  ProjectionStoreThreadNotFoundError: () => Effect.succeed(null),
                }),
                Effect.mapError(
                  (cause) => new OrchestratorProjectionError({ threadId: childThreadId, cause }),
                ),
              );
        children.set(childThreadId, child);
        if (child === null) continue;
        if (
          child.thread.lineage.parentThreadId !== command.threadId ||
          child.thread.lineage.relationshipToParent !== "subagent" ||
          (child.thread.forkedFrom?.type === "node" &&
            child.thread.forkedFrom.nodeId === task.id) ||
          child.runs.some((run) => run.delegatedTaskId === task.id)
        )
          continue;
        const idle =
          idleChildren.get(task.childThreadId) ??
          (!child.runs.some((run) => run.status === "queued" || isActiveRun(run)) &&
            !child.providerThreads.some(
              (thread) =>
                thread.status === "active" || (thread.pendingBackgroundTasks?.length ?? 0) > 0,
            ) &&
            !child.runtimeRequests.some((request) => request.status === "pending") &&
            child.turnItems.length === 0 &&
            Option.isNone(yield* Effect.findFirst(child.runs, awaitsRestartContinuation)));
        idleChildren.set(task.childThreadId, idle);
        if (!idle) continue;
        // Stop can cancel an idle orphan without guessing which completed run owned its result.
        const now = yield* DateTime.now;
        nodes ??= (yield* projectionStore.getThreadRecords(command.threadId, ["nodes"])).nodes;
        yield* writeAppOwnedTask({
          command,
          events,
          task: cancelledAppOwnedTask(task, now),
          nodes,
          turnItems: projection.turnItems,
          now,
        });
      }
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError((cause) =>
          isOrchestratorProjectionError(cause)
            ? cause
            : new OrchestratorProjectionError({ threadId: input.command.threadId, cause }),
        ),
      ),
  );

  /** Puts a queued goal command first and, given a target, stops the run in its way. */
  const stopRunForQueuedCommand = Effect.fn("Orchestrator.stopRunForQueuedCommand")(
    function* (input: {
      readonly command: Extract<
        OrchestrationV2Command,
        { readonly type: "message.dispatch" | "queued-message.promote-to-steer" }
      >;
      readonly events: Ref.Ref<Array<OrchestrationV2DomainEvent>>;
      readonly effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>;
      readonly projection: Pick<OrchestrationV2ThreadProjection, "runs" | "messages">;
      readonly queuedRunId: RunId;
      readonly targetRunId: RunId | undefined;
      readonly reason: string;
      readonly now: DateTime.Utc;
    }) {
      const { command, events, effects, projection, now, targetRunId } = input;
      const others = queuedRunsInDeliveryOrder(projection).filter(
        (run) => run.id !== input.queuedRunId,
      );
      for (const [index, run] of others.entries()) {
        const queuePosition = index + 2;
        if (run.queuePosition === queuePosition) continue;
        yield* emit(
          events,
          command,
        )({
          type: "run.updated",
          threadId: command.threadId,
          runId: run.id,
          ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
          providerInstanceId: run.providerInstanceId,
          occurredAt: now,
          payload: { ...run, queuePosition },
        });
      }
      if (targetRunId === undefined) return undefined;
      const interruptEvents = yield* Ref.make<Array<OrchestrationV2DomainEvent>>([]);
      const interruptEffects = yield* Ref.make<Array<PendingOrchestrationEffectV2>>([]);
      const interrupted = yield* Effect.exit(
        dispatchRunInterrupt(
          {
            type: "run.interrupt",
            commandId: command.commandId,
            threadId: command.threadId,
            runId: targetRunId,
            reason: input.reason,
          },
          interruptEvents,
          interruptEffects,
        ),
      );
      // A failed interrupt may have written part of its events, so they are kept apart.
      // The queued command then waits for the turn to end.
      if (Exit.isFailure(interrupted)) {
        yield* Effect.logWarning("queued command could not stop the running turn", {
          threadId: command.threadId,
          runId: targetRunId,
          cause: interrupted.cause,
        });
        return undefined;
      }
      const written = yield* Ref.get(interruptEvents);
      const enqueued = yield* Ref.get(interruptEffects);
      yield* Ref.update(events, (existing) => [...existing, ...written]);
      yield* Ref.update(effects, (existing) => [...existing, ...enqueued]);
      // Like Stop: a run stopped before its provider started also abandons that start.
      return interrupted.value;
    },
  );

  const dispatchThreadStop = Effect.fn("Orchestrator.dispatchThreadStop")(function* (
    command: Extract<OrchestrationV2ServerCommand, { readonly type: "thread.stop" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
  ) {
    const projection = yield* projectionStore
      .getThreadRecords(command.threadId, ["runs", "providerThreads", "turnItems", "subagents"], {
        turnItemTypes: ["command_execution", "dynamic_tool", "subagent"],
        turnItemStatuses: ["pending", "running", "waiting"],
      })
      .pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
        ),
      );
    if (projection.thread.deletedAt !== null) return undefined;
    // Queued after the interrupt, since a thread's effects run in order.
    const stopDelegatedTasks =
      command.stopDelegatedTasks === false ||
      !projection.subagents.some(
        (task) => task.origin === "app_owned" && task.childThreadId !== null,
      )
        ? Effect.void
        : Ref.update(effects, (existing) => [...existing, delegatedTasksStopEffect(command)]);
    const latestRun = projection.runs.findLast((run) => run.status !== "queued");
    const target =
      projection.runs.findLast(
        (run) =>
          run.status === "preparing" || run.status === "starting" || run.status === "running",
      ) ??
      (derivePendingBackgroundWork({
        latestRun,
        providerThreads: projection.providerThreads,
        turnItems: projection.turnItems,
        activeProviderThreadId: projection.thread.activeProviderThreadId,
        runs: projection.runs,
      }).length > 0
        ? latestRun
        : undefined);
    if (target !== undefined) {
      const interruptEvents = yield* Ref.make<Array<OrchestrationV2DomainEvent>>([]);
      const interruptEffects = yield* Ref.make<Array<PendingOrchestrationEffectV2>>([]);
      const interrupted = yield* Effect.exit(
        dispatchRunInterrupt(
          {
            type: "run.interrupt",
            commandId: command.commandId,
            threadId: command.threadId,
            runId: target.id,
            holdQueue: true,
            ...(command.reason === undefined ? {} : { reason: command.reason }),
          },
          interruptEvents,
          interruptEffects,
        ),
      );
      // A failed interrupt may have written part of its events, so they are kept apart.
      if (Exit.isSuccess(interrupted)) {
        const written = yield* Ref.get(interruptEvents);
        const enqueued = yield* Ref.get(interruptEffects);
        yield* Ref.update(events, (existing) => [...existing, ...written]);
        yield* Ref.update(effects, (existing) => [...existing, ...enqueued]);
        yield* cancelIdleOrphans({ command, events, projection });
        yield* stopDelegatedTasks;
        return interrupted.value;
      }
      yield* Effect.logWarning("thread.stop could not interrupt the running turn", {
        threadId: command.threadId,
        runId: target.id,
        cause: interrupted.cause,
      });
    }
    const now = yield* DateTime.now;
    // Record that Stop reached the turn it could not interrupt and the thread's last executed
    // run, so a late tool call from their agent starts nothing.
    const executed = latestExecutedRun(projection.runs);
    const reached = new Map<RunId, OrchestrationV2Run>();
    if (target !== undefined) reached.set(target.id, target);
    if (executed !== null && !hasLiveRun({ runs: [executed] })) reached.set(executed.id, executed);
    for (const run of reached.values()) {
      yield* markStoppedRun({ command, events, thread: projection.thread, run, now });
    }
    // Record and item are written together, so an active record without a live item lost its end.
    const staleNative = (target === undefined ? projection.subagents : []).filter(
      (subagent) =>
        subagent.origin === "provider_native" &&
        subagent.runId !== null &&
        isOrchestrationV2WorkActive(subagent.status) &&
        !projection.turnItems.some(
          (item) => item.type === "subagent" && item.subagentId === subagent.id,
        ),
    );
    if (staleNative.length > 0) {
      const { nodes } = yield* projectionStore
        .getThreadRecords(command.threadId, ["nodes"])
        .pipe(mapDispatchError(command));
      const threadProjection = {
        nodes,
        subagents: projection.subagents,
        turnItems: projection.turnItems,
      };
      yield* endNativeSubagentTrees({
        command,
        events,
        dying: staleNative.map((subagent) => ({
          projection: threadProjection,
          subagent: nativeSubagentRef(subagent),
        })),
        childThreadIds: new Set(),
        now,
      });
    }
    yield* holdStoppedThread({ command, events, projection, cohortRunIds: [], now });
    yield* cancelIdleOrphans({ command, events, projection });
    yield* stopDelegatedTasks;
    return {
      effectTypes: ["provider-turn.steer"] as const,
      reason: "The thread was stopped before steering delivery.",
    };
  });

  const dispatchCheckpointRollback = Effect.fn("Orchestrator.dispatchCheckpointRollback")(
    function* (
      command: Extract<OrchestrationV2Command, { readonly type: "checkpoint.rollback" }>,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
      effects: Ref.Ref<Array<PendingOrchestrationEffectV2>>,
    ) {
      const projection = yield* loadProjectionForCommand(
        command,
        ["providerThreads", "checkpoints", "checkpointScopes", "runs", "providerTurns", "attempts"],
        { turnItemTypes: [], messageRoles: ["user"] },
      );
      const providerThread = projection.providerThreads.find(
        (candidate) => candidate.id === projection.thread.activeProviderThreadId,
      );
      if (providerThread === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: "No active provider thread exists for rollback.",
        });
      }
      if (providerThread.providerSessionId === null) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Provider thread ${providerThread.id} has no provider session.`,
        });
      }

      const modelSelection = projection.thread.modelSelection;
      const capabilities = yield* providerAdapters.get(modelSelection.instanceId).pipe(
        Effect.flatMap((adapter) => adapter.getCapabilities()),
        Effect.mapError(
          (cause) =>
            new OrchestratorProviderAdapterError({
              commandId: command.commandId,
              providerInstanceId: modelSelection.instanceId,
              cause,
            }),
        ),
      );
      yield* mapDispatchError(command)(
        commandPolicy.ensureRollback({
          commandId: command.commandId,
          threadId: command.threadId,
          providerInstanceId: modelSelection.instanceId,
          capabilities,
        }),
      );

      const targetCheckpoint = projection.checkpoints.find(
        (candidate) => candidate.id === command.checkpointId,
      );
      if (targetCheckpoint === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Checkpoint ${command.checkpointId} was not found.`,
        });
      }
      // Missing snapshots support conversation rewind only.
      if (
        targetCheckpoint.status !== "ready" &&
        !(command.restoreFiles === false && targetCheckpoint.status === "missing")
      ) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Checkpoint ${command.checkpointId} is ${targetCheckpoint.status} and cannot be restored.`,
        });
      }
      const targetScope = projection.checkpointScopes.find(
        (candidate) => candidate.id === targetCheckpoint.scopeId,
      );
      if (targetScope === undefined) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Checkpoint scope ${targetCheckpoint.scopeId} was not found.`,
        });
      }
      if (targetScope.id !== command.scopeId) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Checkpoint ${command.checkpointId} belongs to scope ${targetScope.id}, not ${command.scopeId}.`,
        });
      }
      if (command.restoreFiles !== false) {
        const place = yield* checkpointRestorePlace(projection.thread, targetCheckpoint.id, {
          fileSystem,
          path,
          projections: projectionStore,
        }).pipe(
          Effect.mapError(
            (cause) =>
              new OrchestratorDispatchError({
                commandId: command.commandId,
                commandType: command.type,
                cause,
              }),
          ),
        );
        if (place.refusal !== null)
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: place.refusal,
          });
        const restoreScope =
          place.workspace === undefined ? targetScope : { ...targetScope, cwd: place.workspace };
        const isolated = yield* isCheckpointRestoreIsolated(projection.thread, restoreScope, {
          projects,
          path,
          fileSystem,
          projections: projectionStore,
        }).pipe(
          Effect.mapError(
            (cause) =>
              new OrchestratorDispatchError({
                commandId: command.commandId,
                commandType: command.type,
                cause,
              }),
          ),
        );
        if (!isolated)
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: SHARED_WORKSPACE_RESTORE_MESSAGE,
          });
      }

      const targetOrdinal = targetCheckpoint.appRunOrdinal ?? 0;
      if (targetOrdinal > 0) {
        const targetRun = projection.runs.find((run) => run.ordinal === targetOrdinal);
        const targetProviderTurn =
          targetRun === undefined ? undefined : providerTurnForRun(projection, targetRun);
        if (
          targetRun === undefined ||
          targetRun.status === "rolled_back" ||
          targetProviderTurn === undefined
        ) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: `Cannot roll back to checkpoint ${targetCheckpoint.id}: its provider turn is unavailable.`,
          });
        }
        if (targetProviderTurn.providerThreadId !== providerThread.id) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: `Cannot roll back provider thread ${providerThread.id} to checkpoint ${targetCheckpoint.id}: target provider turn ${targetProviderTurn.id} belongs to provider thread ${targetProviderTurn.providerThreadId}.`,
          });
        }
      }

      const now = yield* DateTime.now;
      yield* emit(
        events,
        command,
      )({
        type: "thread.metadata-updated",
        threadId: command.threadId,
        providerInstanceId: projection.thread.providerInstanceId,
        occurredAt: now,
        payload: {
          ...projection.thread,
          rollbackRequestId: command.commandId,
          rollbackFailure: null,
          updatedAt: now,
        },
      });
      yield* emit(
        events,
        command,
      )({
        type: "checkpoint.rollback-requested",
        threadId: command.threadId,
        providerInstanceId: modelSelection.instanceId,
        occurredAt: now,
        payload: {
          scopeId: targetScope.id,
          checkpointId: targetCheckpoint.id,
          requestedAt: now,
        },
      });
      yield* Ref.update(effects, (existing) => [
        ...existing,
        {
          id: `effect:${command.commandId}:provider-thread.rollback:${providerThread.id}:${targetCheckpoint.id}`,
          commandId: command.commandId,
          threadId: command.threadId,
          request: {
            type: "provider-thread.rollback",
            ...(command.restoreFiles === undefined ? {} : { restoreFiles: command.restoreFiles }),
            providerThreadId: providerThread.id,
            checkpointId: targetCheckpoint.id,
            scopeId: targetScope.id,
          },
        } satisfies PendingOrchestrationEffectV2,
      ]);
    },
  );

  const dispatchCheckpointRollbackFail = Effect.fn("Orchestrator.dispatchCheckpointRollbackFail")(
    function* (
      command: Extract<
        OrchestrationV2InternalCommand,
        { readonly type: "checkpoint.rollback.fail" }
      >,
      events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
    ) {
      const thread = yield* projectionStore
        .getThread(command.threadId)
        .pipe(mapDispatchError(command));
      if (thread.deletedAt !== null) return;
      if (thread.rollbackRequestId !== command.requestId) {
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause: `Rollback ${command.requestId} is no longer the thread's current rollback.`,
        });
      }
      const now = yield* DateTime.now;
      yield* emit(
        events,
        command,
      )({
        type: "thread.metadata-updated",
        threadId: command.threadId,
        providerInstanceId: thread.providerInstanceId,
        occurredAt: now,
        payload: {
          ...thread,
          rollbackFailure: { requestId: command.requestId, message: command.message },
          updatedAt: now,
        },
      });
    },
  );

  /**
   * Thread lineage and fork origin are immutable, so this
   * is safe to read without holding either thread's dispatch lock.
   */
  const appOwnedSubagentParentThreadId = Effect.fn("Orchestrator.appOwnedSubagentParentThreadId")(
    function* (childThreadId: ThreadId) {
      return appOwnedSubagentParentOf(yield* projectionStore.getThread(childThreadId));
    },
  );

  const awaitsRestartContinuation = (run: OrchestrationV2Run) =>
    effectOutbox
      .get(`effect:restart-continuation:${run.id}`)
      .pipe(
        Effect.map(
          Option.exists((effect) => effect.status === "pending" || effect.status === "running"),
        ),
      );

  // A note that a restart cut only unawaited work does not hold the result of the run it follows.
  // Only its attempt's provider turn can tell, so only a pending one that may be such a note reads it.
  const holdsForRestartContinuation = Effect.fnUntraced(function* (
    childThreadId: ThreadId,
    run: OrchestrationV2Run,
  ) {
    if (!(yield* awaitsRestartContinuation(run))) return false;
    if (run.activeAttemptId === null || !restartNoteOnly(run, [])) return true;
    const { providerTurns } = yield* projectionStore.getThreadRecords(
      childThreadId,
      ["providerTurns"],
      { providerTurnAttemptIds: [run.activeAttemptId] },
    );
    return !restartNoteOnly(run, providerTurns);
  });

  const childAwaitsRestartContinuation = (
    childThreadId: ThreadId,
    runs: ReadonlyArray<OrchestrationV2Run>,
    resultRun: OrchestrationV2Run,
    settledContinuationOf?: RunId,
  ) =>
    Effect.forEach(
      runs.filter(
        (run) =>
          run.id !== settledContinuationOf &&
          (run.id === resultRun.id || runRanAfter(run, resultRun)),
      ),
      (run) => holdsForRestartContinuation(childThreadId, run),
    ).pipe(Effect.map((pending) => pending.includes(true)));

  const planDelegatedCompletionDelivery = Effect.fn(
    "orchestrationV2.planDelegatedCompletionDelivery",
  )(function* (input: {
    readonly parentProjection: Pick<
      OrchestrationV2ThreadProjection,
      "thread" | "messages" | "runs" | "subagents"
    >;
    readonly parentRun: OrchestrationV2Run | undefined;
    readonly task: OrchestrationV2Subagent;
    readonly updatedTask: OrchestrationV2Subagent;
    readonly now: DateTime.Utc;
  }) {
    const taskDelivery = input.updatedTask.completionDelivery;
    if (
      taskDelivery?.state === "acknowledged" ||
      taskDelivery?.state === "delivered" ||
      taskDelivery?.state === "disposed"
    ) {
      return {
        task: input.updatedTask,
        parentRun: undefined,
        message: undefined,
        offer: false,
      };
    }
    const cohort = input.parentRun?.delegatedCompletion;
    if (
      input.parentRun === undefined ||
      input.parentProjection.thread.archivedAt !== null ||
      input.parentProjection.thread.deletedAt !== null ||
      (cohort !== undefined && cohort.disposition !== "open")
    ) {
      return {
        task: {
          ...input.updatedTask,
          completionDelivery: {
            state: "disposed" as const,
            observedByRunId: null,
          },
        },
        parentRun: undefined,
        message: undefined,
        offer: false,
      };
    }
    // settled_only relies on a blocking wait that ends with the spawning turn; later results need another delivery path.
    if (
      (input.task.completionWake ?? "settled_only") === "settled_only" &&
      hasLiveRun({ runs: [input.parentRun] })
    ) {
      return {
        task: input.updatedTask,
        parentRun: undefined,
        message: undefined,
        offer: false,
      };
    }

    const delivery = cohort?.delivery ?? null;
    const deliveryRun = completionDeliveryRun(input.parentProjection, delivery);
    if (delivery !== null) {
      if (deliveryRun?.status === "queued" && deliveryRun.userMessageId === delivery.messageId) {
        const taskIds = Array.from(new Set([...delivery.taskIds, input.task.id]));
        const message = completionDeliveryMessage(input.parentProjection, delivery);
        const nextCohort = {
          ...cohort!,
          delivery: {
            ...delivery,
            taskIds,
          },
        };
        return {
          task: {
            ...input.updatedTask,
            completionDelivery: {
              state: "claimed" as const,
              observedByRunId: null,
            },
          },
          parentRun: {
            ...input.parentRun,
            delegatedCompletion: nextCohort,
          },
          message:
            message === undefined
              ? undefined
              : yield* mapDelegatedCompletionError(
                  completionWakeMessage(
                    message,
                    withCompletionWakePart(
                      message.delegatedCompletion ?? {
                        parentRunId: input.parentRun.id,
                        generation: delivery.generation,
                        taskIds,
                      },
                      { parentRunId: input.parentRun.id, generation: delivery.generation, taskIds },
                    ),
                    input.parentProjection.subagents.map((task) =>
                      task.id === input.updatedTask.id ? input.updatedTask : task,
                    ),
                    input.now,
                  ),
                ),
          offer: false,
        };
      }
      if (deliveryRun !== undefined) {
        // The terminal-run listener reserves successor wakes; keep racing siblings pending to avoid competing deliveries.
        return {
          task: {
            ...input.updatedTask,
            completionDelivery: {
              state: "pending" as const,
              observedByRunId: null,
            },
          },
          parentRun: undefined,
          message: undefined,
          offer: false,
        };
      }
      const taskIds = Array.from(new Set([...delivery.taskIds, input.task.id]));
      const nextCohort = {
        ...cohort!,
        delivery: {
          ...delivery,
          taskIds,
        },
      };
      return {
        task: {
          ...input.updatedTask,
          completionDelivery: {
            state: "claimed" as const,
            observedByRunId: null,
          },
        },
        parentRun: {
          ...input.parentRun,
          delegatedCompletion: nextCohort,
        },
        message: undefined,
        offer: true,
      };
    }

    const generation = cohort?.nextGeneration ?? 1;
    const messageId = yield* mapDelegatedCompletionError(
      idAllocator.allocate.message({
        threadId: input.parentRun.threadId,
        ordinal:
          (yield* mapDelegatedCompletionError(
            projectionStore.getMessageCount(input.parentRun.threadId),
          )) + 1,
      }),
    );
    const nextCohort = {
      disposition: "open" as const,
      nextGeneration: generation + 1,
      delivery: {
        generation,
        messageId,
        taskIds: [input.task.id],
      },
    };
    return {
      task: {
        ...input.updatedTask,
        completionDelivery: {
          state: "claimed" as const,
          observedByRunId: null,
        },
      },
      parentRun: {
        ...input.parentRun,
        delegatedCompletion: nextCohort,
      },
      message: undefined,
      offer: true,
    };
  });

  // A finished delegate otherwise keeps its provider process until the idle timeout. Callers hold
  // the parent lock; the child lock orders this after any follow-up that started a run meanwhile.
  const releaseFinishedDelegateSessions = Effect.fn("Orchestrator.releaseFinishedDelegateSessions")(
    function* (childThreadId: ThreadId, resultRunId: RunId) {
      yield* threadDispatch.withLock(
        childThreadId,
        Effect.gen(function* () {
          // Any window holds the runs that decide delegatedTaskProgress's state.
          const { records: child } = yield* projectionStore.getDelegatedTaskRecords(
            childThreadId,
            1,
          );
          // Unawaited background tasks still allow a result but die with the process; the idle
          // timer keeps their process pinned.
          if (
            delegatedTaskProgress(child).state !== "result_available" ||
            child.providerThreads.some((thread) => (thread.pendingBackgroundTasks ?? []).length > 0)
          )
            return;
          const { providerSessions } = yield* projectionStore.getThreadRecords(childThreadId, [
            "providerSessions",
          ]);
          for (const session of providerSessions) {
            // Shared processes unload their threads themselves; only a per-thread process lingers.
            if (
              session.status === "stopped" ||
              session.status === "error" ||
              session.capabilities.sessions.supportsMultipleProviderThreadsPerSession
            )
              continue;
            yield* dispatchWithReceiptEffect({
              type: "provider-session.detach",
              commandId: CommandId.make(
                `command:system:delegated-task-release:${resultRunId}:${session.id}`,
              ),
              threadId: childThreadId,
              providerSessionId: session.id,
              reason: "Delegated task finished.",
            });
          }
        }),
      );
    },
    Effect.catchCause((cause) =>
      Effect.logWarning("Failed to release a finished delegate's provider session", { cause }),
    ),
  );

  /**
   * Callers must hold the parent thread's dispatch lock: wake-policy updates the same
   * subagent row with full-row payloads, so unserialized writers clobber policy or terminal status.
   */
  const finalizeAppOwnedSubagentTask = Effect.fn("Orchestrator.finalizeAppOwnedSubagentTask")(
    function* (
      childControls: DelegatedTaskRecords["records"],
      options: {
        readonly settledContinuationOf?: RunId;
        readonly syncRunId?: RunId;
        readonly task: OrchestrationV2Subagent;
        readonly resultRun?: OrchestrationV2Run;
      },
    ) {
      const childThreadId = childControls.thread.id;
      const forkedFrom = childControls.thread.forkedFrom;
      if (
        childControls.thread.lineage.relationshipToParent !== "subagent" ||
        childControls.thread.lineage.parentThreadId === null ||
        forkedFrom?.type !== "node"
      ) {
        return;
      }
      const { taskRuns, childRun } = delegatedTaskChildRun(
        childControls,
        options.task.id,
        forkedFrom.nodeId,
        options.resultRun,
      );
      if (childRun === undefined) return;
      let outcome = runOutcome(childRun);
      let terminalStatus = delegatedTaskTerminalStatus(outcome);
      if (terminalStatus === null) {
        if (childRun.delegatedTaskId === undefined || childRun.id !== options.syncRunId) return;
        const task = options.task;
        const status: OrchestrationV2Subagent["status"] =
          childRun.status === "queued"
            ? childRun.queueHeld === true
              ? "idle"
              : "pending"
            : childRun.status === "waiting"
              ? "waiting"
              : "running";
        if (task.result !== null || task.status === status) return;
        const parent = yield* projectionStore.getThreadRecords(
          childControls.thread.lineage.parentThreadId,
          ["nodes", "turnItems"],
          { nodeIds: [task.id], turnItemNodeIds: [task.id], turnItemTypes: ["subagent"] },
        );
        const now = yield* DateTime.now;
        const scope = {
          threadId: parent.thread.id,
          ...(task.runId === null ? {} : { runId: task.runId }),
          nodeId: task.id,
          providerInstanceId: task.providerInstanceId,
          occurredAt: now,
        };
        const node = parent.nodes.find((row) => row.id === task.id);
        const item = parent.turnItems.find(
          (row) => row.type === "subagent" && row.subagentId === task.id,
        );
        yield* writeSystemEvents([
          {
            ...scope,
            type: "subagent.updated",
            payload: { ...task, status, completedAt: null, updatedAt: now },
          },
          ...(node === undefined
            ? []
            : [
                {
                  ...scope,
                  type: "node.updated" as const,
                  payload: { ...node, status, completedAt: null },
                },
              ]),
          ...(item === undefined
            ? []
            : [
                {
                  ...scope,
                  type: "turn-item.updated" as const,
                  payload: { ...item, status, completedAt: null, updatedAt: now },
                },
              ]),
        ]);
        return;
      }
      // A follow-up ends with the work it awaits, as the original task does. A cancelled task
      // does not wait for the background work its cancel is stopping.
      if (
        delegatedTaskAwaitsWork(
          taskRuns,
          options.task.cancelledRunId === undefined
            ? childControls
            : { subagents: childControls.subagents, providerThreads: [] },
        )
      )
        return;
      if (
        yield* childAwaitsRestartContinuation(
          childThreadId,
          taskRuns.length === 0 ? childControls.runs : taskRuns,
          childRun,
          options?.settledContinuationOf,
        )
      ) {
        return;
      }

      const childResult = yield* projectionStore.getThreadRecords(
        childThreadId,
        ["messages", "turnItems"],
        {
          messageRoles: ["assistant"],
          messageRunIds: [childRun.id],
          turnItemRunId: childRun.id,
          turnItemTypes: ["assistant_message", "error", "system_notice"],
        },
      );
      // The result run's provider thread, attempt and turns: its source point and model note.
      const attemptIds = childRun.activeAttemptId === null ? [] : [childRun.activeAttemptId];
      const childRunRecords = yield* projectionStore.getThreadRecords(
        childThreadId,
        ["providerThreads", "attempts", "providerTurns"],
        {
          providerThreadIds: childRun.providerThreadId === null ? [] : [childRun.providerThreadId],
          attemptIds,
          providerTurnAttemptIds: attemptIds,
        },
      );
      // A steered attempt can point at a turn another attempt started.
      const attemptTurnId = childRunRecords.attempts[0]?.providerTurnId ?? null;
      const providerTurns =
        childRunRecords.providerTurns.length > 0 || attemptTurnId === null
          ? childRunRecords.providerTurns
          : (yield* projectionStore.getThreadRecords(childThreadId, ["providerTurns"], {
              providerTurnIds: [attemptTurnId],
            })).providerTurns;
      const childProjection = {
        thread: childControls.thread,
        providerThreads: childRunRecords.providerThreads,
        attempts: childRunRecords.attempts,
        providerTurns,
        messages: childResult.messages,
        turnItems: childResult.turnItems,
      };
      const parentThreadId = childControls.thread.lineage.parentThreadId;
      // The task's node and turn item share its id.
      const parentTask = yield* projectionStore.getThreadRecords(
        parentThreadId,
        ["subagents", "nodes", "turnItems"],
        {
          subagentIds: [options.task.id],
          nodeIds: [options.task.id],
          turnItemNodeIds: [options.task.id],
          turnItemTypes: ["subagent"],
        },
      );
      const task = parentTask.subagents.find(
        (candidate) =>
          candidate.id === options.task.id &&
          candidate.origin === "app_owned" &&
          candidate.childThreadId === childThreadId,
      );
      if (task === undefined) {
        return;
      }
      // A cancelled task's completed run ends it interrupted.
      const cancelled = task.cancelledRunId === childRun.id;
      if (cancelled && terminalStatus === "completed") outcome = terminalStatus = "interrupted";
      // Every result the child sent is among its own transfers, far fewer than the parent's.
      const childTransfers = yield* projectionStore.getThreadRecords(childThreadId, [
        "contextTransfers",
      ]);
      const existingResultTransfer = childTransfers.contextTransfers.find(
        (transfer) =>
          transfer.type === "subagent_result" &&
          transfer.sourceThreadId === childThreadId &&
          transfer.targetThreadId === parentThreadId &&
          (transfer.sourcePoint.runId === childRun.id ||
            (options.task.id === forkedFrom.nodeId && transfer.sourcePoint.runId === undefined)),
      );
      if (existingResultTransfer !== undefined) {
        return;
      }

      const now = yield* DateTime.now;
      const result = subagentResultForRun(childProjection, { ...childRun, status: outcome });
      const { parentRun, ...parentProjection } = yield* readDelegatedCompletionPlanRecords(
        parentThreadId,
        task,
      );
      const parentNode = parentTask.nodes.find((candidate) => candidate.id === task.id);
      const parentTurnItem = parentTask.turnItems.find(
        (candidate) => candidate.type === "subagent" && candidate.subagentId === task.id,
      );
      const updatedTask: OrchestrationV2Subagent = {
        ...task,
        providerThreadId: childRun.providerThreadId,
        status: terminalStatus,
        result: result.text,
        completedAt: now,
        updatedAt: now,
        // The cancelling parent gets no wake, and the task records the run it settled from.
        ...(cancelled
          ? {
              cancelledRunId: childRun.id,
              completionDelivery: { state: "disposed" as const, observedByRunId: null },
            }
          : {}),
      };
      const completionPlan = yield* planDelegatedCompletionDelivery({
        parentProjection,
        parentRun,
        task,
        updatedTask,
        now,
      });
      const resultTransferId = yield* idAllocator.allocate.contextTransfer({
        sourceThreadId: childThreadId,
        targetThreadId: parentThreadId,
        type: "subagent_result",
      });
      const resultTransfer: OrchestrationV2ContextTransfer = {
        id: resultTransferId,
        type: "subagent_result",
        sourceThreadId: childThreadId,
        targetThreadId: parentThreadId,
        sourcePoint: {
          ...contextSourcePointForRun(childProjection, childRun),
          ...(result.turnItemId === null ? {} : { turnItemId: result.turnItemId }),
        },
        basePoint: null,
        sourceProviderInstanceId: childRun.providerInstanceId,
        targetProviderInstanceId:
          parentRun?.providerInstanceId ?? parentProjection.thread.providerInstanceId,
        targetRunId: parentRun?.id ?? null,
        status: "consumed",
        resolution: null,
        createdBy: "system",
        error: null,
        createdAt: now,
        updatedAt: now,
        consumedAt: now,
      };

      const completionEvents: ReadonlyArray<Omit<OrchestrationV2DomainEvent, "id">> = [
        {
          type: "subagent.updated",
          threadId: parentThreadId,
          ...(task.runId === null ? {} : { runId: task.runId }),
          nodeId: task.id,
          driver: task.driver,
          occurredAt: now,
          payload: completionPlan.task,
        },
        ...delegatedCompletionPlanEvents(
          parentThreadId,
          completionPlan,
          parentProjection.thread.providerInstanceId,
          now,
        ),
        ...(parentNode === undefined
          ? []
          : [
              {
                type: "node.updated" as const,
                threadId: parentThreadId,
                ...(parentNode.runId === null ? {} : { runId: parentNode.runId }),
                nodeId: parentNode.id,
                driver: task.driver,
                occurredAt: now,
                payload: {
                  ...parentNode,
                  status: terminalStatus,
                  providerThreadId: childRun.providerThreadId,
                  completedAt: now,
                },
              },
            ]),
        ...(parentTurnItem === undefined
          ? []
          : [
              {
                type: "turn-item.updated" as const,
                threadId: parentThreadId,
                ...(parentTurnItem.runId === null ? {} : { runId: parentTurnItem.runId }),
                ...(parentTurnItem.nodeId === null ? {} : { nodeId: parentTurnItem.nodeId }),
                driver: task.driver,
                occurredAt: now,
                payload: {
                  ...parentTurnItem,
                  status: terminalStatus,
                  result: result.text,
                  completedAt: now,
                  updatedAt: now,
                },
              },
            ]),
        {
          type: "context-transfer.created",
          threadId: parentThreadId,
          ...(parentRun === undefined ? {} : { runId: parentRun.id }),
          providerInstanceId: childRun.providerInstanceId,
          occurredAt: now,
          payload: resultTransfer,
        },
      ];
      yield* writeSystemEvents(completionEvents, [], { updateDelegatedCompletionOnly: true });
      yield* releaseFinishedDelegateSessions(childThreadId, childRun.id);

      if (completionPlan.offer && completionPlan.parentRun !== undefined) {
        yield* offerDelegatedCompletionDelivery(parentThreadId, completionPlan.parentRun.id);
      }
    },
  );

  // A waiting run has ended its turn, so its task waits only on the work it left.
  const progressAfterTurn = (child: Parameters<typeof delegatedTaskProgress>[0]) =>
    delegatedTaskProgress({
      ...child,
      runs: child.runs.map((run) =>
        run.status === "waiting" ? { ...run, status: "completed" as const } : run,
      ),
    });

  // A child's questions to its parent, which a stopped result run leaves open.
  const pendingTaskQuestions = (childThreadId: ThreadId) =>
    projectionStore
      .getThreadRecords(childThreadId, ["runtimeRequests"], { runtimeRequestStatuses: ["pending"] })
      .pipe(
        Effect.map(({ runtimeRequests }) =>
          runtimeRequests.filter(
            (request) =>
              request.status === "pending" &&
              request.kind === "user_input" &&
              request.responseCapability.type === "message",
          ),
        ),
      );

  const finalizeAppOwnedSubagent = Effect.fn("Orchestrator.finalizeAppOwnedSubagent")(function* (
    childThreadId: ThreadId,
    options?: { readonly settledContinuationOf?: RunId; readonly syncRunId?: RunId },
  ) {
    const child = yield* projectionStore.getThread(childThreadId);
    const parentThreadId = child.lineage.parentThreadId;
    if (
      child.lineage.relationshipToParent !== "subagent" ||
      parentThreadId === null ||
      child.forkedFrom?.type !== "node"
    )
      return;
    const originalTaskId = child.forkedFrom.nodeId;
    // Each task of this child names it, so the parent's other tasks stay unread.
    const parent = yield* projectionStore.getThreadRecords(parentThreadId, ["subagents"], {
      subagentChildThreadIds: [childThreadId],
    });
    const { contextTransfers } = yield* projectionStore.getThreadRecords(childThreadId, [
      "contextTransfers",
    ]);
    const resultSent = contextTransfers.some(
      (transfer) =>
        transfer.type === "subagent_result" &&
        transfer.sourceThreadId === childThreadId &&
        transfer.targetThreadId === parentThreadId,
    );
    const openTasks = parent.subagents.filter(
      (task) =>
        task.origin === "app_owned" &&
        task.childThreadId === childThreadId &&
        !(task.result !== null && resultSent),
    );
    // Read before any task settles; settling writes only the parent's records and session state.
    const pendingQuestions = yield* pendingTaskQuestions(childThreadId);
    // With every task settled, only a stopped result run's open questions can be left.
    if (openTasks.length === 0 && pendingQuestions.length === 0) return;
    // Which run progress reports a result from matters only to the original task and to open
    // questions once the child stopped working; reading it walks every run that ended since.
    let resultRun = false;
    const decide = (read: DelegatedTaskRecords) => {
      const progress = delegatedTaskProgress(read.records);
      const tasks = openTasks.filter(
        (task) =>
          // A cancelled task does not wait for the work its cancel is stopping.
          !(
            task.id === originalTaskId &&
            (task.cancelledRunId !== undefined
              ? progress.state === "working"
              : progress.state !== "result_available")
          ),
      );
      const stoppedResultRun = resultRun
        ? delegatedTaskStoppedResultRun(progress, parent.subagents, originalTaskId)
        : undefined;
      return {
        progress,
        tasks,
        stoppedResultRun,
        needsResultRun:
          tasks.some((task) => task.id === originalTaskId) ||
          (pendingQuestions.length > 0 && progress.state !== "working"),
        covered: delegatedTaskReadCovers(read, {
          tasks,
          originalTaskId,
          resultRun: progress.resultRun,
          stoppedResultRun,
        }),
      };
    };
    const readChild = (runWindow: number) =>
      projectionStore.getDelegatedTaskRecords(
        childThreadId,
        runWindow,
        resultRun ? { resultRun: true } : undefined,
      );
    let runWindow = DELEGATED_TASK_RUN_WINDOW;
    let read = yield* readChild(runWindow);
    let decided = decide(read);
    while (!decided.covered || (decided.needsResultRun && !resultRun)) {
      if (decided.needsResultRun && !resultRun) resultRun = true;
      else runWindow *= 4;
      read = yield* readChild(runWindow);
      decided = decide(read);
    }
    const childControls = read.records;
    const { progress, stoppedResultRun } = decided;
    for (const task of decided.tasks) {
      yield* finalizeAppOwnedSubagentTask(childControls, {
        ...options,
        task,
        ...(progress.resultRun === undefined ? {} : { resultRun: progress.resultRun }),
      });
    }

    if (
      pendingQuestions.length === 0 ||
      stoppedResultRun === undefined ||
      (yield* childAwaitsRestartContinuation(
        childThreadId,
        childControls.runs,
        stoppedResultRun,
        options?.settledContinuationOf,
      ))
    )
      return;
    // Callers hold the parent lock; dismiss takes the child lock, as follow-up intake does.
    const { nodes } = yield* projectionStore.getThreadRecords(childThreadId, ["nodes"], {
      nodeIds: pendingQuestions.map((request) => request.nodeId),
    });
    const resultRunIds = new Set<RunId>();
    let run: OrchestrationV2Run | undefined = stoppedResultRun;
    while (run !== undefined && !resultRunIds.has(run.id)) {
      resultRunIds.add(run.id);
      const previousRunId: RunId | undefined = run.restartContinuationOfRunId;
      run =
        previousRunId === undefined
          ? undefined
          : childControls.runs.find((candidate) => candidate.id === previousRunId);
    }
    for (const request of pendingQuestions) {
      if (
        nodes.some(
          (node) =>
            node.id === request.nodeId && node.runId !== null && resultRunIds.has(node.runId),
        )
      ) {
        yield* dispatchWithReceipt({
          type: "thread.user-input.dismiss",
          commandId: CommandId.make(`command:system:delegated-task-question:${request.id}`),
          threadId: childThreadId,
          requestId: request.id,
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to dismiss task question", {
              requestId: request.id,
              cause,
            }),
          ),
        );
      }
    }
  });

  // A started run whose provider was never called, its preparation having failed.
  // A checkpoint rollback rewrites the run and its root node, not the attempt, which keeps how
  // the turn ended; a failed run's latest root error says whether the provider had the prompt.
  const failedBeforeProvider = (threadId: ThreadId, run: OrchestrationV2Run) =>
    run.status !== "failed" && run.status !== "rolled_back"
      ? Effect.succeed(false)
      : projectionStore
          .getThreadRecords(threadId, ["turnItems", "attempts"], {
            turnItemTypes: ["error"],
            turnItemRunIds: [run.id],
            attemptIds: run.activeAttemptId === null ? [] : [run.activeAttemptId],
          })
          .pipe(
            Effect.map(({ turnItems, attempts }) => {
              if (run.status === "rolled_back" && attempts[0]?.status !== "failed") return false;
              const code = latestRootProviderFailure({ ...run, status: "failed" }, turnItems)?.code;
              return code === RUN_PREPARATION_FAILED_CODE || code === RUN_START_FAILED_CODE;
            }),
          );

  // Whether a wake that failed before its provider had the prompt is the latest of
  // MAX_WAKE_START_FAILURES failed runs in a row, so it is not sent again.
  const wakeStartsExhausted = (threadId: ThreadId) =>
    projectionStore
      .getQueuedRunRecords(threadId, {
        messageIds: [],
        runIds: [],
        latestExecutedRuns: MAX_WAKE_START_FAILURES,
      })
      .pipe(Effect.map(({ runs }) => failedRunsInARow(runs) >= MAX_WAKE_START_FAILURES));

  // The notice a wake given up on leaves under its failed run, which the thread shows.
  const undeliveredWakeNotice = (
    run: OrchestrationV2Run,
    signal: string,
    message: string,
    now: DateTime.Utc,
  ): Omit<OrchestrationV2DomainEvent, "id"> => ({
    type: "turn-item.updated",
    threadId: run.threadId,
    runId: run.id,
    ...(run.rootNodeId === null ? {} : { nodeId: run.rootNodeId }),
    providerInstanceId: run.providerInstanceId,
    occurredAt: now,
    payload: {
      id: idAllocator.derive.runSignalTurnItem({ runId: run.id, signal }),
      threadId: run.threadId,
      runId: run.id,
      nodeId: run.rootNodeId,
      providerThreadId: run.providerThreadId,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      // EventSink allocates the real position, after the run's existing items.
      ordinal: 0,
      status: "completed",
      title: "Not delivered",
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      type: "system_notice",
      message,
    },
  });

  const finalizeDelegatedCompletionDelivery = Effect.fn(
    "Orchestrator.finalizeDelegatedCompletionDelivery",
  )(function* (threadId: ThreadId, runId: RunId) {
    // Ordinary turns do not own a delegated delivery. Read just their input
    // before loading the cohort state needed to settle an actual delivery.
    const message = yield* projectionStore.getRunMessage(threadId, runId);
    if (message?.delegatedCompletion === undefined) return;
    const parentRunIds = completionWakeParts(message.delegatedCompletion).map(
      (part) => part.parentRunId,
    );
    const projection = yield* projectionStore.getThreadRecords(
      threadId,
      ["runs", "messages", "subagents"],
      {
        runIds: [runId, ...parentRunIds],
        messageIds: [message.id],
        subagentRunIds: parentRunIds,
      },
    );
    const deliveryRun = projection.runs.find((candidate) => candidate.id === runId);
    const deliveryMessage =
      deliveryRun === undefined
        ? undefined
        : projection.messages.find((candidate) => candidate.id === deliveryRun.userMessageId);
    const messageOwnership = deliveryMessage?.delegatedCompletion;
    if (deliveryRun === undefined || messageOwnership === undefined) {
      return;
    }

    const now = yield* DateTime.now;
    // The prompt goes to the provider only once the run is recorded running and prepared, so a
    // wake that ended without a start time, or whose preparation failed, delivered nothing.
    const reachedProvider =
      deliveryRun.status !== "cancelled" &&
      deliveryRun.startedAt !== null &&
      !(yield* failedBeforeProvider(threadId, deliveryRun));
    const gaveUp =
      !reachedProvider &&
      deliveryRun.status === "failed" &&
      (yield* mapDelegatedCompletionError(wakeStartsExhausted(threadId)));
    const settledTaskState = reachedProvider ? "delivered" : gaveUp ? "disposed" : "pending";
    const events: Array<Omit<OrchestrationV2DomainEvent, "id">> = [];
    const givenUpTasks: Array<OrchestrationV2Subagent> = [];
    const successorParentRunIds: Array<RunId> = [];
    // Each parent run the wake carried settles its own cohort.
    for (const part of completionWakeParts(messageOwnership)) {
      const parentRun = projection.runs.find((candidate) => candidate.id === part.parentRunId);
      const cohort = parentRun?.delegatedCompletion;
      const delivery = cohort?.delivery;
      if (
        parentRun === undefined ||
        cohort === undefined ||
        delivery === null ||
        delivery === undefined ||
        delivery.messageId !== deliveryRun.userMessageId ||
        delivery.generation !== part.generation
      ) {
        continue;
      }
      const nextTaskStates = new Map<
        OrchestrationV2Subagent["id"],
        OrchestrationV2Subagent["completionDelivery"]
      >();
      for (const task of projection.subagents) {
        if (
          task.origin !== "app_owned" ||
          task.runId !== parentRun.id ||
          !delivery.taskIds.includes(task.id) ||
          task.completionDelivery?.state !== "claimed"
        ) {
          continue;
        }
        nextTaskStates.set(task.id, { state: settledTaskState, observedByRunId: null });
        if (gaveUp) givenUpTasks.push(task);
      }
      const pendingTaskIds = projection.subagents
        .filter(
          (task) =>
            task.origin === "app_owned" &&
            task.runId === parentRun.id &&
            isTerminalDelegatedTaskStatus(task.status) &&
            (task.completionDelivery?.state === "pending" ||
              nextTaskStates.get(task.id)?.state === "pending"),
        )
        .map((task) => task.id);
      const canReserveFollowUp =
        cohort.disposition === "open" &&
        projection.thread.archivedAt === null &&
        projection.thread.deletedAt === null &&
        pendingTaskIds.length > 0;
      const nextDelivery = canReserveFollowUp
        ? {
            generation: cohort.nextGeneration,
            messageId: yield* mapDelegatedCompletionError(
              idAllocator.allocate.message({
                threadId,
                ordinal:
                  (yield* mapDelegatedCompletionError(
                    projectionStore.getMessageCount(projection.thread.id),
                  )) + 1,
              }),
            ),
            taskIds: pendingTaskIds,
          }
        : null;
      if (nextDelivery !== null) {
        successorParentRunIds.push(parentRun.id);
        for (const taskId of pendingTaskIds) {
          nextTaskStates.set(taskId, {
            state: "claimed",
            observedByRunId: null,
          });
        }
      }
      const updatedParentRun: OrchestrationV2Run = {
        ...parentRun,
        delegatedCompletion: {
          ...cohort,
          nextGeneration: nextDelivery === null ? cohort.nextGeneration : cohort.nextGeneration + 1,
          delivery: nextDelivery,
        },
      };
      for (const task of projection.subagents) {
        const completionDelivery = nextTaskStates.get(task.id);
        if (completionDelivery === undefined) continue;
        events.push({
          type: "subagent.updated",
          threadId,
          ...(task.runId === null ? {} : { runId: task.runId }),
          nodeId: task.id,
          driver: task.driver,
          providerInstanceId: task.providerInstanceId,
          occurredAt: now,
          payload: {
            ...task,
            completionDelivery,
            updatedAt: now,
          },
        });
      }
      events.push({
        type: "run.updated",
        threadId,
        runId: updatedParentRun.id,
        ...(updatedParentRun.rootNodeId === null ? {} : { nodeId: updatedParentRun.rootNodeId }),
        providerInstanceId: updatedParentRun.providerInstanceId,
        occurredAt: now,
        payload: updatedParentRun,
      });
    }
    if (givenUpTasks.length > 0) {
      const titles = givenUpTasks.map((task) => task.title ?? task.id).join(", ");
      events.push(
        undeliveredWakeNotice(
          deliveryRun,
          "undelivered-results",
          `The provider failed to start ${MAX_WAKE_START_FAILURES} turns in a row, so these delegated results were not sent to it: ${titles}. Each task still has its result.`,
          now,
        ),
      );
    }
    if (events.length === 0) return;
    yield* writeSystemEvents(events);
    for (const parentRunId of successorParentRunIds) {
      yield* offerDelegatedCompletionDelivery(threadId, parentRunId);
    }
  });

  const dispatchNotificationAccepted = Effect.fn("orchestrationV2.notificationAccepted")(function* (
    command: Extract<OrchestrationV2Command, { type: "notification.delivery.accept" }>,
    events: Ref.Ref<Array<OrchestrationV2DomainEvent>>,
  ) {
    const projection = yield* getProjectionWithPendingEvents(command.threadId, events);
    const now = yield* DateTime.now;
    const parentRunId = yield* acceptDelegatedCompletionDelivery({
      command,
      events,
      projection,
      messageId: command.messageId,
      now,
    });
    if (parentRunId === undefined) {
      yield* emit(
        events,
        command,
      )({
        type: "thread.metadata-updated",
        threadId: command.threadId,
        occurredAt: now,
        payload: projection.thread,
      });
    }
    return parentRunId;
  });

  const dispatchUnsupported = (command: OrchestrationV2ServerCommand) =>
    Effect.fail(
      new OrchestratorDispatchError({
        commandId: command.commandId,
        commandType: command.type,
      }),
    );

  const dispatchOnce = Effect.fn("orchestrationV2.dispatch.once")(function* (
    command: OrchestrationV2ServerCommand,
    sideChatIds: ReadonlyArray<ThreadId>,
  ): Effect.fn.Return<
    {
      readonly events: ReadonlyArray<OrchestrationV2DomainEvent>;
      readonly effects: ReadonlyArray<PendingOrchestrationEffectV2>;
      readonly cancelUnsettledEffects?: {
        readonly effectTypes: ReadonlyArray<OrchestrationEffectRequestV2["type"]>;
        readonly reason: string;
      };
      readonly updateDelegatedCompletionOnlyRunId?: RunId;
    },
    OrchestratorV2Error
  > {
    yield* Effect.annotateCurrentSpan({
      "orchestration_v2.command_id": command.commandId,
      "orchestration_v2.command_type": command.type,
      "orchestration_v2.thread_id": commandThreadId(command),
    });

    const events = yield* Ref.make<Array<OrchestrationV2DomainEvent>>([]);
    const effects = yield* Ref.make<Array<PendingOrchestrationEffectV2>>([]);
    let cancelUnsettledEffects:
      | {
          readonly effectTypes: ReadonlyArray<OrchestrationEffectRequestV2["type"]>;
          readonly reason: string;
        }
      | undefined;
    let updateDelegatedCompletionOnlyRunId: RunId | undefined;
    switch (command.type) {
      case "thread.create": {
        yield* dispatchThreadCreate(command, events);
        const initialMessage = threadCreateInitialMessage(command);
        if (initialMessage !== undefined) {
          cancelUnsettledEffects = yield* dispatchMessage(
            withoutGuestAttribution(initialMessage),
            events,
            effects,
          );
        }
        break;
      }
      case "thread.visit":
        yield* dispatchThreadVisit(command, events);
        break;
      case "thread.auto-settle": {
        // Recheck live state; changes and explicit overrides take precedence over the sweep snapshot.
        const thread = yield* projectionStore
          .getThread(command.threadId)
          .pipe(
            Effect.mapError(
              (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
            ),
          );
        if (
          thread.settledOverride !== null ||
          DateTime.toEpochMillis(thread.updatedAt) > DateTime.toEpochMillis(command.snapshotAt)
        ) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: `Thread ${command.threadId} changed before automatic settlement.`,
          });
        }
        yield* dispatchThreadMutation(
          {
            type: "thread.settle",
            commandId: command.commandId,
            threadId: command.threadId,
            ...(command.settledAt === undefined ? {} : { settledAt: command.settledAt }),
          },
          events,
          effects,
        );
        break;
      }
      case "thread.delete": {
        const now = yield* DateTime.now;
        const planDeletion = Effect.fn("Orchestrator.planDeletion")(function* (
          deletion: typeof command,
        ) {
          const projection = yield* projectionStore
            .getThreadRecords(deletion.threadId, [
              "runs",
              "attempts",
              "nodes",
              "runtimeRequests",
              "subagents",
              "providerSessions",
            ])
            .pipe(
              Effect.mapError(
                (cause) => new OrchestratorProjectionError({ threadId: deletion.threadId, cause }),
              ),
            );
          return yield* mapDispatchError(command)(
            planThreadDeletion({
              command: deletion,
              projection,
              attachmentIds: yield* projectionStore
                .getThreadAttachmentIds(deletion.threadId)
                .pipe(mapDispatchError(command)),
              now,
              idAllocator,
            }),
          );
        });
        // Checked under the thread lock: a message sent since the client looked keeps the thread.
        if (command.onlyIfUnused === true) {
          const usage = yield* projectionStore
            .getThreadRecords(command.threadId, ["runs", "messages"])
            .pipe(
              Effect.mapError(
                (cause) => new OrchestratorProjectionError({ threadId: command.threadId, cause }),
              ),
            );
          if (usage.runs.length > 0 || usage.messages.length > 0) {
            return { events: [], effects: [] };
          }
        }
        // The dispatch wrapper holds each side chat's lock, so none can commit after this plan.
        const plans = [
          yield* planDeletion(command),
          ...(yield* Effect.forEach(sideChatIds, (threadId) =>
            planDeletion({
              ...command,
              commandId: CommandId.make(`${command.commandId}:side-chat:${threadId}`),
              threadId,
            }),
          )),
        ];
        return {
          events: plans.flatMap((plan) => plan.events),
          effects: plans.flatMap((plan) => plan.effects),
        };
      }
      case "thread.archive":
      case "thread.unarchive":
      case "thread.settle":
      case "thread.unsettle":
      case "thread.snooze":
      case "thread.unsnooze":
      case "thread.auto-settle.set":
      case "thread.pin":
      case "thread.unpin":
      case "thread.pin.reorder":
      case "thread.active.reorder":
      case "thread.tag.set":
      case "thread.mark-unread":
      case "thread.pull-request.link":
      case "thread.pull-request.unlink":
      case "thread.pull-request-link.sync":
      case "thread.pull-request.watch":
      case "thread.pull-request.sync":
      case "thread.title.regeneration.complete":
      case "thread.runtime-mode.set":
      case "thread.interaction-mode.set":
      case "thread.model-selection.set":
      case "provider.switch":
        yield* dispatchThreadMutation(command, events, effects);
        break;
      case "thread.metadata.update": {
        const continuation = metadataUpdateContinuation(command);
        if (continuation !== undefined) yield* refuseNativeSubagentMessage(command);
        yield* dispatchThreadMutation(command, events, effects);
        if (continuation !== undefined) {
          cancelUnsettledEffects = yield* dispatchMessage(continuation, events, effects);
          yield* recordSubagentFollowup(continuation, events).pipe(mapDispatchError(command));
        }
        break;
      }
      case "thread.pull-request-watch.checks-reopen":
        yield* dispatchThreadMutation(command, events, effects);
        break;
      case "thread.pull-request-watch.sync":
        yield* dispatchPullRequestWatchSync(command, events, effects);
        break;
      case "thread.guest-share.open":
      case "thread.guest-share.redeem":
      case "thread.guest-share.close":
      case "thread.guest-message.submit":
      case "thread.guest-message.dismiss":
        yield* dispatchGuestShare(command, events, effects);
        break;
      case "provider-session.detach":
        yield* dispatchProviderSessionDetach(command, events, effects);
        break;
      case "thread.project.adopt-folder":
        yield* dispatchThreadProjectAdoptFolder(command, events);
        break;
      case "message.dispatch": {
        const thread = yield* refuseNativeSubagentMessage(command);
        const guestSend =
          command.guestMessageId === undefined
            ? undefined
            : yield* emitGuestMessageSent(command, command.guestMessageId, thread, events);
        const dispatched =
          guestSend === undefined
            ? withoutGuestAttribution(command)
            : {
                ...command,
                text: guestSend.text,
                context: { version: 1 as const, records: [], guest: guestSend.guest },
              };
        const delegatedCompletionOnly = yield* Ref.make<RunId | undefined>(undefined);
        cancelUnsettledEffects = yield* dispatchMessage(
          dispatched,
          events,
          effects,
          delegatedCompletionOnly,
        );
        updateDelegatedCompletionOnlyRunId = yield* Ref.get(delegatedCompletionOnly);
        yield* recordSubagentFollowup(dispatched, events).pipe(mapDispatchError(command));
        break;
      }
      case "notification.delivery.accept":
        updateDelegatedCompletionOnlyRunId = yield* dispatchNotificationAccepted(command, events);
        break;
      case "prepared-run.release":
        yield* dispatchPreparedRunRelease(command, events, effects);
        break;
      case "prepared-run.progress":
        yield* dispatchPreparedRunProgress(command, events);
        break;
      case "prepared-run.fail":
        yield* dispatchPreparedRunFail(command, events);
        break;
      case "prepared-run.retry":
        yield* dispatchPreparedRunRetry(command, events);
        break;
      case "runtime-request.respond":
        yield* dispatchRuntimeRequestRespond(command, events, effects);
        break;
      case "thread.user-input.dismiss":
        yield* dispatchThreadUserInputDismiss(command, events, effects);
        break;
      case "subagent.stop":
        yield* dispatchSubagentStop(command, events, effects);
        break;
      case "run.interrupt":
        cancelUnsettledEffects = yield* dispatchRunInterrupt(command, events, effects);
        if (command.holdQueue === true) {
          // Stop must still commit when a child can't be read, so a failed repair's
          // partial events are dropped.
          const repairEvents = yield* Ref.make(yield* Ref.get(events));
          const repaired = yield* Effect.exit(
            loadProjectionForCommand(command, ["subagents", "turnItems"], {
              turnItemTypes: ["subagent"],
              turnItemStatuses: ["pending", "running", "waiting"],
            }).pipe(
              Effect.flatMap((projection) =>
                cancelIdleOrphans({ command, events: repairEvents, projection }),
              ),
            ),
          );
          if (Exit.isSuccess(repaired)) yield* Ref.set(events, yield* Ref.get(repairEvents));
          else
            yield* Effect.logWarning("Stop could not cancel orphaned delegated follow-ups", {
              threadId: command.threadId,
              cause: repaired.cause,
            });
          yield* Ref.update(effects, (existing) => [
            ...existing,
            delegatedTasksStopEffect(command),
          ]);
        }
        break;
      case "thread.stop":
        cancelUnsettledEffects = yield* dispatchThreadStop(command, events, effects);
        break;
      case "queued-message.promote-to-steer":
        cancelUnsettledEffects = yield* dispatchQueuedMessagePromoteToSteer(
          command,
          events,
          effects,
        );
        break;
      case "queued-run.start":
      case "queue.resume": {
        const projection = yield* loadProjectionForCommand(
          command,
          ["runs", "turnItems", "providerSessions", "messages"],
          { turnItemTypes: ["error"], messageRoles: ["user"] },
        );
        if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: `Thread ${command.threadId} is not active.`,
          });
        }
        const selectedRunId = command.type === "queued-run.start" ? command.runId : undefined;
        const selectedRun =
          selectedRunId === undefined
            ? undefined
            : projection.runs.find((run) => run.id === selectedRunId && run.status === "queued");
        if (selectedRunId !== undefined) {
          const selectedMessage = projection.messages.find(
            (message) => message.id === selectedRun?.userMessageId,
          );
          if (
            selectedRun === undefined ||
            selectedMessage === undefined ||
            selectedMessage.delegatedCompletion !== undefined ||
            selectedMessage.notification !== undefined ||
            projection.runs.some(isActiveRun)
          ) {
            return yield* new OrchestratorDispatchError({
              commandId: command.commandId,
              commandType: command.type,
              cause: "A queued user message can only be sent now while the thread is stopped.",
            });
          }
        }
        const nextRun = selectedRun ?? queuedRunsInDeliveryOrder(projection)[0];
        if (
          usageLimitHoldsQueue(
            projection,
            nextRun === undefined
              ? projection.thread.providerInstanceId
              : queuedRunModelSelection(projection, nextRun).instanceId,
          )
        ) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: "Continue the limited thread before resuming its queue.",
          });
        }
        if (worktreeSetupPending(projection)) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: "Retry worktree setup or choose a workspace before resuming the queue.",
          });
        }
        const now = yield* DateTime.now;
        const queued = projection.runs.filter((run) => run.status === "queued");
        for (const run of queued) {
          yield* emit(
            events,
            command,
          )({
            type: "run.updated",
            threadId: command.threadId,
            runId: run.id,
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...run, queueHeld: false },
          });
        }
        yield* releaseQueueHold(command, events, projection.thread, now);
        break;
      }
      case "queued-run.reorder":
        yield* dispatchQueuedRunReorder(command, events);
        break;
      case "queued-run.cancel":
        yield* dispatchQueuedRunCancel(command, events);
        break;
      case "queued-run.edit":
        yield* dispatchQueuedRunEdit(command, events, effects);
        break;
      case "checkpoint.rollback":
        yield* dispatchCheckpointRollback(command, events, effects);
        break;
      case "checkpoint.rollback.fail":
        yield* dispatchCheckpointRollbackFail(command, events);
        break;
      case "runtime-request.delivery.fail":
        yield* dispatchRuntimeRequestDeliveryFail(command, events);
        break;
      case "thread.background-work.settle":
        yield* dispatchBackgroundWorkSettle(command, events, effects);
        break;
      case "thread.interrupt.unresponsive":
        yield* dispatchInterruptUnresponsive(command, events);
        break;
      case "thread.fork":
        yield* dispatchThreadFork(command, events);
        break;
      case "thread.merge_back":
        yield* dispatchThreadMergeBack(command, events);
        break;
      case "delegated_task.request":
        yield* dispatchDelegatedTaskRequest(command, events, effects);
        break;
      case "delegated_task.remote.request":
        yield* dispatchRemoteDelegatedTaskRequest(command, events);
        break;
      case "delegated_task.remote.complete":
        yield* dispatchRemoteDelegatedTaskComplete(command, events);
        break;
      case "delegated_task.remote.stop":
        yield* dispatchRemoteDelegatedTaskStop(command, events);
        break;
      case "delegated_task.wake-policy":
        updateDelegatedCompletionOnlyRunId = yield* dispatchDelegatedTaskWakePolicy(
          command,
          events,
        );
        break;
      case "delegated_task.completion-delivery.acknowledge":
      case "delegated_task.completion-delivery.dispose":
        updateDelegatedCompletionOnlyRunId =
          yield* dispatchDelegatedTaskCompletionDeliveryResolution(command, events);
        break;
      case "thread.created.record":
        yield* dispatchCreatedThreadRecord(command, events);
        break;
      case "secret_request.record":
        yield* dispatchSecretRequestRecord(command, events);
        break;
      case "link_request.record":
        yield* dispatchLinkRequestRecord(command, events, effects);
        break;
      default:
        return yield* dispatchUnsupported(command);
    }
    return {
      events: yield* Ref.get(events),
      effects: yield* Ref.get(effects),
      ...(cancelUnsettledEffects === undefined ? {} : { cancelUnsettledEffects }),
      ...(updateDelegatedCompletionOnlyRunId === undefined
        ? {}
        : { updateDelegatedCompletionOnlyRunId }),
    };
  });

  const replayCommandReceipt = Effect.fn("orchestrationV2.dispatch.replayReceipt")(function* (
    command: OrchestrationV2ServerCommand,
    receipt: CommandReceiptV2,
  ) {
    if (receipt.status === "rejected") {
      return yield* new OrchestratorCommandPreviouslyRejectedError({
        commandId: command.commandId,
        commandType: command.type,
        detail: receipt.error ?? "Previously rejected.",
      });
    }
    const dispatchThreadId = commandThreadId(command);
    // Receipts only prove execution for the thread they were recorded against.
    if (receipt.threadId !== dispatchThreadId) {
      return yield* new OrchestratorCommandIdConflictError({
        commandId: command.commandId,
        commandType: command.type,
        receiptThreadId: receipt.threadId,
        commandThreadId: dispatchThreadId,
      });
    }
    const storedEvents = yield* eventSink.readByCommandId({ commandId: command.commandId }).pipe(
      Stream.runCollect,
      Effect.map((events): ReadonlyArray<OrchestrationV2StoredEvent> => Array.from(events)),
      Effect.mapError(
        (cause) =>
          new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause,
          }),
      ),
    );
    if (command.type === "queue.resume" || command.type === "queued-run.start") {
      yield* mapDispatchError(command)(
        startNextQueuedRun(command.threadId, {
          userStarted: true,
          ...(command.type === "queued-run.start" ? { queuedRunId: command.runId } : {}),
        }),
      );
    }
    if (receipt.status === "accepted") yield* endWaitAfterCommand(command);
    return {
      sequence: receipt.resultSequence,
      storedEvents,
      replayed: true,
    } satisfies OrchestratorV2DispatchResult;
  });

  /** `sideChatIds` are the live side chats a `thread.delete` cascades to, read under their locks. */
  const dispatchWithReceiptEffect = Effect.fn("orchestrationV2.dispatch.withReceipt")(function* (
    command: OrchestrationV2ServerCommand,
    sideChatIds: ReadonlyArray<ThreadId> = [],
    cancelledTask?: OrchestrationV2Subagent,
  ): Effect.fn.Return<OrchestratorV2DispatchResult, OrchestratorV2Error> {
    yield* Effect.annotateCurrentSpan({
      "orchestration_v2.command_id": command.commandId,
      "orchestration_v2.command_type": command.type,
      "orchestration_v2.thread_id": commandThreadId(command),
    });

    const existingReceipt = yield* commandReceipts.getByCommandId(command.commandId).pipe(
      Effect.mapError(
        (cause) =>
          new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause,
          }),
      ),
    );

    if (Option.isSome(existingReceipt)) {
      return yield* replayCommandReceipt(command, existingReceipt.value);
    }

    // Recheck under the lock; the user may have raised modes since sender preflight.
    const limit = yield* DispatchModeLimit;
    if (limit !== undefined) {
      // Fork and merge-back sources are unlocked; validate the modes copied by their dispatch.
      const threadId = commandThreadId(command);
      const shell = yield* projectionStore
        .getThreadShell(threadId)
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
      if (shell !== null) yield* refuseAboveDispatchModeLimit(command, threadId, shell);
    }

    // The run-end release can lag behind a new send; start the queue first so the send waits behind it.
    // A restart continuation resumes the ended run, so it must not be overtaken by the queue.
    if (
      command.type === "message.dispatch" &&
      command.restartContinuationOfRunId === undefined &&
      (command.dispatchMode.type === "start_immediately" ||
        command.dispatchMode.type === "queue_after_active" ||
        command.dispatchMode.type === "defer_start")
    ) {
      yield* mapDispatchError(command)(
        startNextQueuedRun(command.threadId, { latestRunEnded: true }),
      );
    }

    const plan = yield* dispatchOnce(command, sideChatIds).pipe(
      Effect.tap((planned) => refuseAboveRuntimeModeCeiling(command, planned.events)),
      Effect.filterOrFail(
        (planned) =>
          planned.events.length > 0 ||
          isUnobservedAcknowledgement(command) ||
          command.type === "thread.background-work.settle" ||
          // A stale visit changes no read state.
          command.type === "thread.visit" ||
          // A read can answer a watch again before a push's reopen lands; nothing is left to clear.
          command.type === "thread.pull-request-watch.checks-reopen" ||
          command.type === "thread.interrupt.unresponsive" ||
          command.type === "thread.stop" ||
          command.type === "subagent.stop" ||
          // Closing a share again, or after it ended, is not an error.
          command.type === "thread.guest-share.close" ||
          (command.type === "thread.delete" && command.onlyIfUnused === true),
        (_planned) =>
          new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: "Command produced no domain events.",
          }),
      ),
      Effect.catch((cause) =>
        Effect.gen(function* () {
          // Durable follow-ups retry the same accepted input after transient planning failures.
          if (command.type === "message.dispatch" && command.steerFollowUpOfRunId !== undefined)
            return yield* cause;
          // A notice retry is tried again with its command id: only a refusal it decided sticks.
          if (
            command.type === "message.dispatch" &&
            command.commandId.startsWith(NOTICE_RETRY_COMMAND_PREFIX) &&
            !(cause._tag === "OrchestratorDispatchError" && typeof cause.cause === "string")
          )
            return yield* cause;
          // Automatic Send now reuses its command id every tick: only a refusal it decided sticks.
          if (
            command.type === "queued-message.promote-to-steer" &&
            command.commandId.startsWith(QUEUED_DELIVERY_PROMOTION_COMMAND_PREFIX) &&
            !(cause._tag === "OrchestratorDispatchError" && typeof cause.cause === "string")
          )
            return yield* cause;
          // Refusals depend on modes the user can change; a recorded rejection would block the retry.
          if (
            cause._tag === "OrchestratorThreadAboveModeLimitError" ||
            cause._tag === "OrchestratorRuntimeModeAboveCeilingError"
          ) {
            return yield* cause;
          }
          // Resolving a turn's policy refuses the same way, from inside the decision.
          const ceiling = runtimePolicyCeilingCause(cause);
          if (ceiling !== undefined) {
            const { threadId, runtimeMode, maxRuntimeMode, organization } = ceiling;
            return yield* new OrchestratorRuntimeModeAboveCeilingError({
              commandId: command.commandId,
              threadId,
              refused: "turn",
              runtimeMode,
              maxRuntimeMode,
              organization,
            });
          }
          const rejectedAt = yield* DateTime.now;
          const receipt = yield* eventSink
            .commitRejectedCommand({
              commandId: command.commandId,
              threadId: commandThreadId(command),
              commandType: command.type,
              rejectedAt,
              error: cause instanceof Error ? cause.message : String(cause),
            })
            .pipe(
              Effect.mapError(
                (receiptCause) =>
                  new OrchestratorDispatchError({
                    commandId: command.commandId,
                    commandType: command.type,
                    cause: receiptCause,
                  }),
              ),
            );
          if (
            command.type === "queued-run.edit" &&
            receipt.status === "rejected" &&
            cause._tag === "OrchestratorDispatchError"
          ) {
            return yield* new OrchestratorCommandRejectedError({
              commandId: cause.commandId,
              commandType: cause.commandType,
              cause: cause.cause,
            });
          }
          return yield* cause;
        }),
      ),
    );

    if (plan.events.length === 0 && plan.effects.length === 0 && cancelledTask === undefined) {
      // A settle that ended nothing still records its receipt: a replayed Stop
      // effect then finds it instead of settling work that appeared since.
      const { resultSequence, inserted } = yield* Effect.gen(function* () {
        const sequence = yield* eventSink.latestSequence({ threadId: commandThreadId(command) });
        const inserted = yield* commandReceipts.insertIfAbsent({
          commandId: command.commandId,
          threadId: commandThreadId(command),
          commandType: command.type,
          acceptedAt: yield* DateTime.now,
          resultSequence: sequence,
          status: "accepted",
          error: null,
        });
        return { resultSequence: sequence, inserted };
      }).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorDispatchError({
              commandId: command.commandId,
              commandType: command.type,
              cause,
            }),
        ),
      );
      if (!inserted) {
        const receipt = yield* mapDispatchError(command)(
          commandReceipts.getByCommandId(command.commandId),
        );
        if (Option.isNone(receipt)) {
          return yield* new OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause: "Conflicting receipt is not a thread command receipt.",
          });
        }
        return yield* replayCommandReceipt(command, receipt.value);
      }
      return {
        sequence: resultSequence,
        storedEvents: [],
      } satisfies OrchestratorV2DispatchResult;
    }
    const acceptedAt = plan.events.at(-1)?.occurredAt ?? (yield* DateTime.now);
    const committed = yield* eventSink
      .commitCommand({
        commandId: command.commandId,
        threadId: commandThreadId(command),
        commandType: command.type,
        acceptedAt,
        events:
          cancelledTask === undefined
            ? plan.events
            : [
                ...plan.events,
                yield* makeSystemEvent({
                  type: "subagent.updated",
                  threadId: cancelledTask.threadId,
                  nodeId: cancelledTask.id,
                  occurredAt: cancelledTask.updatedAt,
                  payload: cancelledTask,
                }).pipe(mapDispatchError(command)),
              ],
        effects: plan.effects,
        ...(plan.cancelUnsettledEffects === undefined
          ? {}
          : { cancelUnsettledEffects: plan.cancelUnsettledEffects }),
        ...(plan.updateDelegatedCompletionOnlyRunId === undefined
          ? {}
          : { updateDelegatedCompletionOnlyRunId: plan.updateDelegatedCompletionOnlyRunId }),
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new OrchestratorDispatchError({
              commandId: command.commandId,
              commandType: command.type,
              cause,
            }),
        ),
      );

    if (committed.receipt.status === "rejected") {
      return yield* new OrchestratorCommandPreviouslyRejectedError({
        commandId: command.commandId,
        commandType: command.type,
        detail: committed.receipt.error ?? "Previously rejected.",
      });
    }
    // A concurrent command with this id for another thread can commit first,
    // under that thread's lock; its receipt is not this command's result.
    if (committed.receipt.threadId !== commandThreadId(command)) {
      return yield* new OrchestratorCommandIdConflictError({
        commandId: command.commandId,
        commandType: command.type,
        receiptThreadId: committed.receipt.threadId,
        commandThreadId: commandThreadId(command),
      });
    }
    if (
      command.type === "queue.resume" ||
      command.type === "queued-run.start" ||
      (command.type === "thread.runtime-mode.set" && runtimeModeCeiling !== undefined)
    ) {
      yield* mapDispatchError(command)(
        startNextQueuedRun(command.threadId, {
          userStarted: command.type !== "thread.runtime-mode.set",
          ...(command.type === "queued-run.start" ? { queuedRunId: command.runId } : {}),
        }),
      );
    }
    yield* endWaitAfterCommand(command);
    if (
      command.type === "notification.delivery.accept" ||
      // A steer restart that settled a wake can reserve the next one for its siblings.
      ((command.type === "message.dispatch" ||
        command.type === "queued-message.promote-to-steer") &&
        committed.storedEvents.some(
          ({ event }) =>
            event.type === "subagent.updated" &&
            event.payload.completionDelivery?.state === "claimed",
        ))
    ) {
      yield* mapDispatchError(command)(offerDelegatedCompletionDeliveries(command.threadId));
    }
    if (
      command.type === "delegated_task.wake-policy" ||
      command.type === "delegated_task.remote.complete" ||
      isUnobservedAcknowledgement(command)
    ) {
      yield* mapDispatchError(command)(offerDelegatedCompletionDeliveries(command.parentThreadId));
    }

    return {
      sequence: committed.receipt.resultSequence,
      storedEvents: committed.storedEvents,
      ...(committed.committed ? {} : { replayed: true as const }),
    } satisfies OrchestratorV2DispatchResult;
  }, withCommandMetrics);

  // Follow-up intake and Stop serialize on the parent before taking the child lock.
  const withSubagentParentLock = <A, E, R>(threadId: ThreadId, dispatch: Effect.Effect<A, E, R>) =>
    appOwnedSubagentParentThreadId(threadId).pipe(
      Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })),
      Effect.flatMap((parentThreadId) =>
        parentThreadId === undefined ? dispatch : threadDispatch.withLock(parentThreadId, dispatch),
      ),
    );

  // dispatchWithReceiptEffect owns the dispatch span; this wrapper selects the thread locks.
  const dispatchWithReceipt = Effect.fnUntraced(function* (command: OrchestrationV2ServerCommand) {
    const dispatch = threadDispatch.withLock(
      commandThreadId(command),
      dispatchWithReceiptEffect(command),
    );
    // A fork holds its source's lock, so a source deletion and its fork never interleave. A
    // parent's lock comes before its side chats' locks. A side chat fork from a side chat or a
    // fork onto an existing thread would invert that order; dispatch rejects both, so they skip it.
    if (command.type === "thread.fork" && command.sourceThreadId !== command.targetThreadId) {
      const source = yield* findThread(command.sourceThreadId);
      const target = yield* findThread(command.targetThreadId);
      const skipsSourceLock =
        source === null ||
        target !== null ||
        (command.sideChat === true && source.lineage.sideChat === true);
      return yield* skipsSourceLock
        ? dispatch
        : threadDispatch.withLock(command.sourceThreadId, dispatch);
    }
    if (command.type === "thread.delete")
      return yield* threadDispatch.withDeletionLock(
        threadDispatch.withLock(
          command.threadId,
          liveSideChatIds(command.threadId).pipe(
            Effect.flatMap((sideChatIds) =>
              sideChatIds.reduce(
                (locked, sideChatId) => threadDispatch.withLock(sideChatId, locked),
                dispatchWithReceiptEffect(command, sideChatIds),
              ),
            ),
          ),
        ),
      );
    // Taken before the lineage read, so a message whose read yields keeps its
    // place ahead of later messages to the same thread.
    const message =
      command.type === "thread.metadata.update" ? metadataUpdateContinuation(command) : command;
    if (message?.type === "message.dispatch")
      return yield* messageIntake.withLock(
        message.threadId,
        mayStartSubagentFollowup(message)
          ? withSubagentParentLock(message.threadId, dispatch)
          : dispatch,
      );
    // Answering a message-mode question can also start a follow-up.
    return yield* command.type === "runtime-request.respond"
      ? withSubagentParentLock(command.threadId, dispatch)
      : dispatch;
  });

  // A wake a server restart or shutdown cancelled before its provider had the prompt. A turn
  // the provider announced counts as sent: only a kill inside an adapter's announce-and-send step
  // gets past that, while sending again every wake cut mid-turn would duplicate on most deploys.
  const restartCutBeforeProvider = Effect.fn("Orchestrator.restartCutBeforeProvider")(function* (
    threadId: ThreadId,
    run: OrchestrationV2Run,
  ) {
    if (run.status !== "cancelled") return false;
    const attemptIds = run.activeAttemptId === null ? [] : [run.activeAttemptId];
    const records = yield* projectionStore.getThreadRecords(
      threadId,
      ["turnItems", "attempts", "providerTurns"],
      {
        turnItemTypes: ["system_notice"],
        turnItemRunIds: [run.id],
        attemptIds,
        providerTurnAttemptIds: attemptIds,
      },
    );
    const marker = idAllocator.derive.runSignalTurnItem({
      runId: run.id,
      signal: "server-recovery",
    });
    return (
      records.turnItems.some((item) => item.id === marker) &&
      (records.providerTurns.length === 0 ||
        records.attempts.some((attempt) => attempt.providerStartState === "pending"))
    );
  });

  // How many restarts in a row cut this news before its prompt went out, counting the latest,
  // read back through the wakes that sent it again (at most the bound's worth).
  const restartCutsInARow = Effect.fn("Orchestrator.restartCutsInARow")(function* (
    threadId: ThreadId,
    message: OrchestrationV2ConversationMessage,
  ) {
    let restarts = 1;
    let source: OrchestrationV2ConversationMessage | undefined = message;
    while (
      restarts <= MAX_PULL_REQUEST_WAKE_RESTARTS &&
      source?.id.startsWith(NOTICE_RETRY_MESSAGE_PREFIX) === true
    ) {
      const previousRunId = RunId.make(source.id.slice(NOTICE_RETRY_MESSAGE_PREFIX.length));
      const {
        runs: [previous],
      } = yield* projectionStore.getThreadRecords(threadId, ["runs"], { runIds: [previousRunId] });
      if (previous === undefined || !(yield* restartCutBeforeProvider(threadId, previous))) break;
      restarts += 1;
      source = yield* projectionStore.getRunMessage(threadId, previousRunId);
    }
    return restarts;
  });

  // Pull request news has no cohort to hold it: a wake carrying several updates (merged across
  // watch passes) that failed, or that a restart cut, before its provider had the prompt queues
  // them again. A single update is lost with its run, as any other notice is.
  const requeueUndeliveredNotice = Effect.fn("Orchestrator.requeueUndeliveredNotice")(function* (
    threadId: ThreadId,
    runId: RunId,
  ) {
    const message = yield* projectionStore.getRunMessage(threadId, runId);
    if (
      message?.notification === undefined ||
      mergeableWakeClass(message) !== "pull-request" ||
      monitorPullRequestStamps(message.notification.source).length < 2
    ) {
      return;
    }
    const {
      runs: [run],
    } = yield* projectionStore.getThreadRecords(threadId, ["runs"], { runIds: [runId] });
    if (run === undefined || run.userMessageId !== message.id) return;
    // News given up on after too many failed starts stays given up.
    const givenUp = idAllocator.derive.runSignalTurnItem({
      runId: run.id,
      signal: "undelivered-news",
    });
    const { turnItems: notices } = yield* projectionStore.getThreadRecords(
      threadId,
      ["turnItems"],
      {
        turnItemTypes: ["system_notice"],
        turnItemRunIds: [run.id],
      },
    );
    if (notices.some((item) => item.id === givenUp)) return;
    if (run.status === "failed" || run.status === "rolled_back") {
      if (run.startedAt !== null && !(yield* failedBeforeProvider(threadId, run))) return;
    } else {
      if (!(yield* restartCutBeforeProvider(threadId, run))) return;
      if ((yield* restartCutsInARow(threadId, message)) > MAX_PULL_REQUEST_WAKE_RESTARTS) {
        yield* Effect.logWarning(
          "Not sending again pull request news that restarts cut too often",
          {
            threadId,
            runId,
          },
        );
        return;
      }
    }
    // Queued, its start drops the news of watches that ended since, as any queued wake's does.
    yield* threadDispatch.withLock(
      threadId,
      Effect.gen(function* () {
        const thread = yield* projectionStore.getThread(threadId);
        if (thread.deletedAt !== null || refusesPullRequestNews(thread)) return;
        if (run.status !== "cancelled" && (yield* wakeStartsExhausted(threadId))) {
          const pullRequests = monitorPullRequestStamps(message.notification?.source)
            .map((stamp) => `${stamp.repository}#${stamp.number}`)
            .join(", ");
          yield* writeSystemEvents([
            undeliveredWakeNotice(
              run,
              "undelivered-news",
              `The provider failed to start ${MAX_WAKE_START_FAILURES} turns in a row, so the watch news of ${pullRequests} was not sent to it.`,
              yield* DateTime.now,
            ),
          ]);
          return;
        }
        yield* dispatchWithReceiptEffect({
          type: "message.dispatch",
          commandId: CommandId.make(`${NOTICE_RETRY_COMMAND_PREFIX}${run.id}`),
          threadId,
          messageId: MessageId.make(`${NOTICE_RETRY_MESSAGE_PREFIX}${run.id}`),
          text: message.text,
          notification: message.notification,
          attachments: [],
          dispatchMode: { type: "queue_after_active" },
          createdBy: "agent",
          creationSource: "server",
        });
        yield* startNextQueuedRun(threadId, { latestRunEnded: true });
      }),
    );
  });

  // Notice retries that failed for a reason other than a refusal; the idle check tries them
  // again while the server runs, and startup recovery after a restart.
  const failedNoticeRetries = new Map<RunId, ThreadId>();
  const retryUndeliveredNotice = (threadId: ThreadId, runId: RunId) =>
    requeueUndeliveredNotice(threadId, runId).pipe(
      Effect.matchEffect({
        onSuccess: () => Effect.sync(() => failedNoticeRetries.delete(runId)),
        onFailure: (error) => {
          const refused =
            (error._tag === "OrchestratorDispatchError" && typeof error.cause === "string") ||
            error._tag === "OrchestratorCommandPreviouslyRejectedError";
          return Effect.sync(() =>
            refused ? failedNoticeRetries.delete(runId) : failedNoticeRetries.set(runId, threadId),
          ).pipe(
            Effect.andThen(
              Effect.logWarning("Failed to queue pull request news again", {
                threadId,
                runId,
                refused,
                error,
              }),
            ),
          );
        },
      }),
      Effect.catchCause((cause) =>
        Effect.sync(() => failedNoticeRetries.set(runId, threadId)).pipe(
          Effect.andThen(
            Effect.logWarning("Failed to queue pull request news again", {
              threadId,
              runId,
              cause,
            }),
          ),
        ),
      ),
      Effect.asVoid,
    );

  const handleRunUpdate = Effect.fn("Orchestrator.handleRunUpdate")(
    function* (stored: OrchestrationV2StoredEvent, reaction: RunEndReaction) {
      if (stored.event.type !== "run.updated") return;
      const threadId = stored.event.threadId;
      const run = stored.event.payload;
      if (run.status === "waiting") {
        yield* threadDispatch.withLock(threadId, endWaitForQueuedMessage(threadId)).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to end a waiting run for its queued message", {
              threadId,
              cause,
            }),
          ),
        );
        return;
      }
      // Take parent and child locks sequentially, never nested: delegation holds the parent
      // while writing child events, so nesting parent inside child would invert the order.
      // The keyed semaphores are neither reentrant nor deadlock-aware.
      if (delegatedTaskTerminalStatus(run.status) !== null) {
        // Before the parent hears of the failure: a pending continuation keeps the task open.
        if (
          reaction === "full" &&
          run.status === "failed" &&
          run.restartContinuationOfRunId !== undefined
        ) {
          yield* threadDispatch.withLock(threadId, retryContinuationStart(threadId, run.id)).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Failed to schedule a restart continuation retry", {
                threadId,
                runId: run.id,
                cause,
              }),
            ),
          );
        }
        yield* threadDispatch.withLock(
          threadId,
          finalizeDelegatedCompletionDelivery(threadId, run.id),
        );
        if (run.status === "failed") {
          yield* retryUndeliveredNotice(threadId, run.id);
        }
        // A settle (a cohort rewrite, or a queued run cancelled before it ran) ended no work.
        if (reaction === "full")
          yield* threadDispatch
            .withLock(
              threadId,
              (run.status === "completed"
                ? releaseUnattendedHolds(threadId, run.id)
                : Effect.void
              ).pipe(
                Effect.andThen(
                  startNextQueuedRun(threadId, {
                    endedRunId: run.id,
                    ...(run.status === "failed" ? { failedRunId: run.id } : {}),
                  }),
                ),
              ),
            )
            .pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("Failed to start the next queued V2 run", { threadId, cause }),
              ),
            );
      }
      const parentThreadId = yield* appOwnedSubagentParentThreadId(threadId);
      if (parentThreadId !== undefined) {
        yield* threadDispatch.withLock(
          parentThreadId,
          finalizeAppOwnedSubagent(threadId, { syncRunId: run.id }),
        );
      }
    },
    (effect, stored, _reaction) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to react to V2 run update", {
            threadId: stored.event.threadId,
            sequence: stored.sequence,
            cause,
          }),
        ),
      ),
  );

  // A reaction waits for its thread's lock, which a long command can hold for minutes. Events
  // run in order per key, with REACTION_CONCURRENCY keys at once. A delegated child keys on its
  // parent, whose lock its reactions also take, so a parent sees one reaction at a time.
  const familyKeys = new Map<ThreadId, ThreadId>();
  const familyLookups = new Map<ThreadId, Deferred.Deferred<ThreadId>>();
  const familyReads = yield* FiberSet.make<void>();
  // One lineage read per thread, off the reactors' intake: a slow read holds up only its thread.
  const familyOf = (threadId: ThreadId): Effect.Effect<ThreadId> =>
    Effect.suspend(() => {
      const known = familyKeys.get(threadId);
      if (known !== undefined) return Effect.succeed(known);
      const pending = familyLookups.get(threadId);
      if (pending !== undefined) return Deferred.await(pending);
      const lookup = Deferred.makeUnsafe<ThreadId>();
      familyLookups.set(threadId, lookup);
      const read = appOwnedSubagentParentThreadId(threadId).pipe(
        Effect.map((parentThreadId) => parentThreadId ?? threadId),
        Effect.orElseSucceed(() => threadId),
        Effect.flatMap((family) =>
          Effect.suspend(() => {
            if (familyKeys.size >= FAMILY_KEY_CACHE_SIZE) {
              familyKeys.delete(familyKeys.keys().next().value!);
            }
            familyKeys.set(threadId, family);
            familyLookups.delete(threadId);
            return Deferred.succeed(lookup, family);
          }),
        ),
      );
      return FiberSet.run(familyReads, read).pipe(Effect.andThen(Deferred.await(lookup)));
    });
  const learnFamily = (threadId: ThreadId) =>
    familyKeys.has(threadId) || familyLookups.has(threadId)
      ? Effect.void
      : FiberSet.run(familyReads, familyOf(threadId)).pipe(Effect.asVoid);
  const runForEachByThread =
    <A>(
      handle: (item: A) => Effect.Effect<void>,
      threadOf: (item: A) => ThreadId,
      byFamily: boolean,
    ) =>
    <E, R>(stream: Stream.Stream<A, E, R>) =>
      Effect.gen(function* () {
        const reactor = yield* makeKeyedReactor<ThreadId, A>({
          concurrency: REACTION_CONCURRENCY,
          handle,
        });
        // A thread's events wait here, in order, while its family is looked up.
        const waiting = new Map<ThreadId, Array<A>>();
        const flushes = yield* FiberSet.make<void>();
        const flush = (threadId: ThreadId, key: ThreadId): Effect.Effect<void> =>
          Effect.suspend(() => {
            const next = waiting.get(threadId)!.shift();
            if (next !== undefined)
              return reactor.offer(key, next).pipe(Effect.andThen(flush(threadId, key)));
            waiting.delete(threadId);
            return Effect.void;
          });
        return yield* Stream.runForEach(stream, (item) =>
          Effect.suspend(() => {
            const threadId = threadOf(item);
            if (!byFamily) return reactor.offer(threadId, item);
            const queued = waiting.get(threadId);
            if (queued !== undefined) {
              queued.push(item);
              return Effect.void;
            }
            const known = familyKeys.get(threadId);
            if (known !== undefined) return reactor.offer(known, item);
            waiting.set(threadId, [item]);
            return FiberSet.run(
              flushes,
              Effect.flatMap(familyOf(threadId), (key) => flush(threadId, key)),
            ).pipe(Effect.asVoid);
          }),
        );
      });

  // Projections already contain historical terminals; replaying them delays startup queue promotion with database size.
  const terminalEventsAfterSequence = yield* eventSink.latestSequence().pipe(Effect.orDie);
  // Run creations come in the same ordered stream, so a queued run's cancel is known as one.
  // No other event type reaches the reactor.
  const runEndReactions = makeRunEndReactions(RUN_END_REACTION_MEMORY);
  yield* eventSink
    .stream({
      afterSequence: terminalEventsAfterSequence,
      eventTypes: ["run.created", "run.updated"],
    })
    .pipe(
      Stream.tap(() => Metric.update(orchestrationRunReactorEventsTotal, 1)),
      Stream.map((stored) => ({
        stored,
        reaction:
          stored.event.type === "run.updated" || stored.event.type === "run.created"
            ? runEndReactions.observe(stored.event.payload)
            : ("full" as const),
      })),
      // Learn a thread's key when its run is created, so its run end is handed off at once.
      Stream.tap(({ stored }) =>
        stored.event.type === "run.created" ? learnFamily(stored.event.threadId) : Effect.void,
      ),
      Stream.filter(
        ({ stored, reaction }) =>
          reaction !== "none" &&
          stored.event.type === "run.updated" &&
          (!String(stored.commandId).startsWith("command:runtime-reconcile:") ||
            (stored.event.payload.delegatedTaskId !== undefined &&
              stored.event.payload.status === "queued" &&
              stored.event.payload.queueHeld === true)) &&
          ((stored.event.payload.delegatedTaskId !== undefined &&
            (stored.event.payload.status === "queued" ||
              stored.event.payload.status === "starting")) ||
            stored.event.payload.status === "waiting" ||
            stored.event.payload.status === "completed" ||
            stored.event.payload.status === "interrupted" ||
            stored.event.payload.status === "failed" ||
            stored.event.payload.status === "cancelled" ||
            stored.event.payload.status === "rolled_back"),
      ),
      runForEachByThread(
        ({ stored, reaction }) => handleRunUpdate(stored, reaction),
        ({ stored }) => stored.event.threadId,
        true,
      ),
      Effect.forkScoped,
    );

  // A running turn reports usage many times; only its first report counts as its start.
  const promotedTurnStarts = new Map<ThreadId, ProviderTurnId>();
  // Callers hold the thread's dispatch lock.
  const promoteQueuedDelivery = Effect.fn("Orchestrator.promoteQueuedDelivery")(function* (
    threadId: ThreadId,
  ) {
    const now = yield* DateTime.now;
    // Messages still in their grace are read too: a steer that goes now carries them.
    const candidates = yield* projectionStore.getQueuedDeliveryPromotionCandidates({
      dueBy: DateTime.addDuration(
        DateTime.subtractDuration(now, QUEUED_DELIVERY_PROMOTION_GRACE),
        QUEUED_DELIVERY_PROMOTION_SPACING,
      ),
      threadId,
    });
    if (candidates.length === 0) return;
    const active = yield* projectionStore.getThreadRecords(threadId, ["runs"], {
      runStatuses: ["preparing", "starting", "running", "waiting"],
    });
    const target = queuedDeliveryTarget(active.runs);
    if (
      target === undefined ||
      active.thread.archivedAt !== null ||
      active.thread.deletedAt !== null
    )
      return;
    const records = yield* projectionStore.getThreadRecords(
      threadId,
      ["runs", "messages", "providerTurns", "turnItems", "runtimeRequests"],
      {
        runIds: candidates.map((candidate) => candidate.runId),
        messageIds: [target.userMessageId, ...candidates.map((candidate) => candidate.messageId)],
        providerTurnAttemptIds: target.activeAttemptId === null ? [] : [target.activeAttemptId],
        turnItemTypes: ["run_interrupt_request", "compaction", "user_message"],
        turnItemRunIds: [target.id],
        runtimeRequestStatuses: ["pending"],
      },
    );
    // Without a live turn, a turn's next running report is its start again.
    if (
      !records.providerTurns.some(
        (turn) => turn.runAttemptId === target.activeAttemptId && turn.status === "running",
      )
    )
      promotedTurnStarts.delete(threadId);
    const capabilities = yield* providerAdapters
      .get(target.providerInstanceId)
      .pipe(Effect.flatMap((adapter) => adapter.getCapabilities()));
    const { promotion, retryAt } = decideQueuedDelivery({
      now,
      runs: [...active.runs, ...records.runs],
      messages: records.messages,
      providerTurns: records.providerTurns,
      runtimeRequests: records.runtimeRequests,
      targetTurnItems: records.turnItems,
      capabilities: capabilities.turns,
      modelSelectionOf: (run) =>
        queuedRunModelSelection({ thread: active.thread, messages: records.messages }, run),
    });
    if (retryAt !== null) yield* retryQueuedDeliveryAt(threadId, retryAt);
    if (promotion === null) return;
    yield* dispatchWithReceiptEffect({
      type: "queued-message.promote-to-steer",
      commandId: promotion.commandId,
      threadId,
      queuedRunId: promotion.queuedRunId,
      ...(promotion.mergedQueuedRunIds === undefined
        ? {}
        : { mergedQueuedRunIds: promotion.mergedQueuedRunIds }),
      targetRunId: promotion.targetRunId,
    }).pipe(Effect.catchTags({ OrchestratorCommandPreviouslyRejectedError: () => Effect.void }));
    // Messages past the merge bounds take the next steer, as soon as the spacing allows.
    if (candidates.length > 1 + (promotion.mergedQueuedRunIds?.length ?? 0))
      yield* retryQueuedDeliveryAt(
        threadId,
        DateTime.addDuration(now, QUEUED_DELIVERY_PROMOTION_SPACING),
      );
  });
  // One promotion in flight per thread, so a thread whose lock is held stalls only itself.
  const queuedDeliveryPromotions = yield* FiberMap.make<ThreadId>();
  // At most one pending retry per thread; a later decision replaces it.
  const queuedDeliveryRetries = yield* FiberMap.make<ThreadId>();
  const retryQueuedDeliveryAt = (threadId: ThreadId, at: DateTime.Utc): Effect.Effect<void> =>
    FiberMap.run(
      queuedDeliveryRetries,
      threadId,
      DateTime.now.pipe(
        Effect.flatMap((now) =>
          Effect.sleep(
            Duration.millis(Math.max(0, DateTime.toEpochMillis(at) - DateTime.toEpochMillis(now))),
          ),
        ),
        Effect.andThen(Effect.suspend(() => startQueuedDeliveryPromotion(threadId))),
      ),
    ).pipe(Effect.asVoid);
  // Candidates are read outside the lock; the locked reads recheck them.
  const startQueuedDeliveryPromotion = (
    threadId: ThreadId,
    known?: ReadonlyArray<ProjectionQueuedDeliveryCandidate>,
  ) =>
    FiberMap.run(
      queuedDeliveryPromotions,
      threadId,
      (known === undefined
        ? DateTime.now.pipe(
            Effect.flatMap((now) =>
              projectionStore.getQueuedDeliveryPromotionCandidates({
                dueBy: DateTime.subtractDuration(now, QUEUED_DELIVERY_PROMOTION_GRACE),
                threadId,
              }),
            ),
          )
        : Effect.succeed(known)
      ).pipe(
        Effect.flatMap((candidates) =>
          candidates.length === 0
            ? Effect.void
            : threadDispatch.withLock(threadId, promoteQueuedDelivery(threadId)),
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to promote a queued message", { threadId, cause }),
        ),
      ),
      { onlyIfMissing: true },
    ).pipe(Effect.asVoid);
  const promoteOnTurnStart = (threadId: ThreadId, providerTurnId: ProviderTurnId) =>
    Effect.suspend(() => {
      if (promotedTurnStarts.get(threadId) === providerTurnId) return Effect.void;
      promotedTurnStarts.set(threadId, providerTurnId);
      return startQueuedDeliveryPromotion(threadId);
    });

  yield* forkParked(
    Effect.gen(function* () {
      const dueBy = DateTime.subtractDuration(yield* DateTime.now, QUEUED_DELIVERY_PROMOTION_GRACE);
      const candidates = yield* projectionStore.getQueuedDeliveryPromotionCandidates({ dueBy });
      for (const [threadId, known] of Map.groupBy(candidates, (candidate) => candidate.threadId))
        yield* startQueuedDeliveryPromotion(threadId, known);
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Failed to look for queued messages to promote", { cause }),
      ),
      Effect.delay(QUEUED_DELIVERY_PROMOTION_TICK),
      Effect.forever,
    ),
  );

  yield* eventSink
    .stream({ afterSequence: terminalEventsAfterSequence, eventType: "provider-turn.updated" })
    .pipe(
      Stream.filter(
        (stored) =>
          stored.event.type === "provider-turn.updated" &&
          stored.event.payload.status === "running",
      ),
      runForEachByThread(
        Effect.fn("Orchestrator.deliverDeferredSteers")(
          function* (stored) {
            if (stored.event.type !== "provider-turn.updated") return;
            const threadId = stored.event.threadId;
            const turn = stored.event.payload;
            const projection = yield* projectionStore.getThreadRecords(threadId, ["runs"], {
              runStatuses: ["queued", "running"],
            });
            const run = projection.runs.find(
              (candidate) =>
                candidate.activeAttemptId === turn.runAttemptId && candidate.status === "running",
            );
            if (run === undefined) return yield* promoteOnTurnStart(threadId, turn.id);
            const pending = projection.runs
              .filter(
                (queued) => queued.status === "queued" && queued.steerWhenReadyRunId === run.id,
              )
              .toSorted(
                (left, right) =>
                  (left.queuePosition ?? left.ordinal) - (right.queuePosition ?? right.ordinal),
              );
            for (const queued of pending) {
              yield* dispatchWithReceiptEffect({
                type: "queued-message.promote-to-steer",
                commandId: CommandId.make(
                  `command:deferred-steer:${queued.id}:${run.id}:${turn.runAttemptId}`,
                ),
                threadId,
                queuedRunId: queued.id,
                targetRunId: run.id,
              }).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("Failed to deliver a deferred steer", {
                    threadId,
                    runId: queued.id,
                    cause,
                  }),
                ),
              );
            }
            if (pending.length === 0) yield* promoteOnTurnStart(threadId, turn.id);
            else promotedTurnStarts.set(threadId, turn.id);
          },
          (effect, stored) => threadDispatch.withLock(stored.event.threadId, effect),
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to deliver deferred steering", { cause }),
          ),
        ),
        (stored: OrchestrationV2StoredEvent) => stored.event.threadId,
        false,
      ),
      Effect.forkScoped,
    );

  // A child whose run ends while its background tasks or its own subagents still run has no
  // result yet. When that work settles there is no run update to finalize it, so these events
  // do it. A background roster counts once it stops holding after it was seen holding.
  const providerThreadsWithBackgroundWork = new Set<string>();
  const settlesChildWork = (stored: OrchestrationV2StoredEvent) => {
    const { event } = stored;
    if (event.type === "subagent.updated")
      return !isOrchestrationV2WorkActive(event.payload.status);
    if (event.type !== "provider-thread.updated") return false;
    if (backgroundWorkHoldsCompletion(event.payload.pendingBackgroundTasks ?? [])) {
      providerThreadsWithBackgroundWork.add(event.payload.id);
      return false;
    }
    return providerThreadsWithBackgroundWork.delete(event.payload.id);
  };
  yield* Stream.merge(
    eventSink.stream({
      afterSequence: terminalEventsAfterSequence,
      eventType: "provider-thread.updated",
    }),
    eventSink.stream({ afterSequence: terminalEventsAfterSequence, eventType: "subagent.updated" }),
  ).pipe(
    Stream.filter(settlesChildWork),
    runForEachByThread(
      (stored) =>
        Effect.gen(function* () {
          const childThreadId = stored.event.threadId;
          const parentThreadId = yield* appOwnedSubagentParentThreadId(childThreadId);
          if (parentThreadId === undefined) return;
          yield* threadDispatch.withLock(parentThreadId, finalizeAppOwnedSubagent(childThreadId));
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Failed to finalize a delegated child after its work settled", {
              threadId: stored.event.threadId,
              sequence: stored.sequence,
              cause,
            }),
          ),
        ),
      (stored) => stored.event.threadId,
      true,
    ),
    Effect.forkScoped,
  );

  // A run end whose queue start was missed leaves the queue waiting on an idle thread. Each
  // thread gets one interval for its run-end handler first; one the check cannot start (a usage
  // limit, say) backs off, so a blocked queue costs a projection read rarely.
  const idleQueueChecks = new Map<
    ThreadId,
    { readonly queue: string; readonly dueAt: number; readonly backoff: number }
  >();
  let idleQueueCheckCount = 0;
  // An attempt waits for its thread's lock without holding up the next check or other threads.
  const idleQueueAttempts = yield* FiberMap.make<ThreadId>();
  const startIdleQueues = Effect.gen(function* () {
    const check = (idleQueueCheckCount += 1);
    const idleThreads = yield* projectionStore.getIdleQueuedThreads();
    const idle = new Set(idleThreads.map(({ threadId }) => threadId));
    for (const threadId of idleQueueChecks.keys()) {
      if (!idle.has(threadId)) idleQueueChecks.delete(threadId);
    }
    // A queue that changed (a new message, a start or a cancel) is checked as if new.
    const due = idleThreads.flatMap((thread) => {
      const queue = `${thread.unheldQueuedRuns}:${thread.firstQueuedOrdinal}:${thread.latestRunOrdinal}`;
      const seen = idleQueueChecks.get(thread.threadId);
      if (seen?.queue === queue) return seen.dueAt <= check ? [thread.threadId] : [];
      idleQueueChecks.set(thread.threadId, { queue, dueAt: check + 1, backoff: 1 });
      return [];
    });
    yield* Effect.forEach(
      due,
      (threadId) =>
        FiberMap.run(
          idleQueueAttempts,
          threadId,
          threadDispatch
            .withLock(threadId, startNextQueuedRun(threadId, { latestRunEnded: true }))
            .pipe(
              Effect.andThen(projectionStore.canStartQueuedRun(threadId)),
              Effect.tap((blocked) =>
                Effect.sync(() => {
                  const seen = idleQueueChecks.get(threadId);
                  if (!blocked || seen === undefined) idleQueueChecks.delete(threadId);
                  else
                    idleQueueChecks.set(threadId, {
                      queue: seen.queue,
                      dueAt: check + seen.backoff,
                      backoff: Math.min(seen.backoff * 2, IDLE_QUEUE_CHECK_MAX_BACKOFF),
                    });
                }),
              ),
              Effect.catchCause((cause) =>
                Effect.logWarning("Failed to start an idle thread's queued V2 run", {
                  threadId,
                  cause,
                }),
              ),
            ),
          { onlyIfMissing: true },
        ),
      { discard: true },
    );
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Failed to check idle threads for queued V2 runs", { cause }),
    ),
    Effect.withSpan("Orchestrator.startIdleQueues"),
  );
  yield* forkParked(
    startIdleQueues.pipe(Effect.repeat(Schedule.spaced(IDLE_QUEUE_CHECK_INTERVAL))),
  );
  // On the idle check's interval, each in a fiber of its own: one waiting on its thread holds
  // back neither the others nor the next pass.
  const noticeRetryAttempts = yield* FiberMap.make<RunId>();
  const retryFailedNotices = Effect.suspend(() =>
    Effect.forEach(
      [...failedNoticeRetries],
      ([runId, threadId]) =>
        FiberMap.run(noticeRetryAttempts, runId, retryUndeliveredNotice(threadId, runId), {
          onlyIfMissing: true,
        }),
      { discard: true },
    ),
  );
  yield* forkParked(
    retryFailedNotices.pipe(Effect.repeat(Schedule.spaced(IDLE_QUEUE_CHECK_INTERVAL))),
  );

  const recoverDelegatedTasks = Effect.gen(function* () {
    yield* projectionStore.getRecoveryThreadIds("subagent-results").pipe(
      Effect.tap((threadIds) =>
        Effect.annotateCurrentSpan("recovery.subagentResultThreads", threadIds.length),
      ),
      Effect.flatMap((threadIds) =>
        Effect.forEach(
          threadIds,
          (threadId) =>
            Effect.gen(function* () {
              const thread = yield* projectionStore.getThreadShell(threadId);
              const parentThreadId = thread?.lineage.parentThreadId;
              if (parentThreadId === undefined || parentThreadId === null) return;
              yield* threadDispatch.withLock(parentThreadId, finalizeAppOwnedSubagent(threadId));
            }).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("Failed to recover terminal app-owned subagent", {
                  childThreadId: threadId,
                  cause,
                }),
              ),
            ),
          { concurrency: 8, discard: true },
        ),
      ),
      Effect.catchCause((cause) =>
        Effect.logWarning("Failed to inspect app-owned subagents during recovery", {
          cause,
        }),
      ),
    );
    yield* projectionStore.getRecoveryThreadIds("delegated-completions").pipe(
      Effect.tap((threadIds) =>
        Effect.annotateCurrentSpan("recovery.delegatedCompletionThreads", threadIds.length),
      ),
      Effect.flatMap((threadIds) =>
        Effect.forEach(
          threadIds,
          (threadId) =>
            threadDispatch
              .withLock(
                threadId,
                Effect.gen(function* () {
                  const projection = yield* projectionStore.getThreadRecords(threadId, ["runs"]);
                  // Only the current delivery's run can settle its cohort; revisiting historical delivery runs repeatedly reloads the thread.
                  const openDeliveryMessageIds = new Set(
                    projection.runs.flatMap((run) =>
                      run.delegatedCompletion?.delivery == null
                        ? []
                        : [run.delegatedCompletion.delivery.messageId],
                    ),
                  );
                  const terminalDeliveryRunIds = projection.runs
                    .filter(
                      (run) =>
                        delegatedTaskTerminalStatus(run.status) !== null &&
                        openDeliveryMessageIds.has(run.userMessageId),
                    )
                    .map((run) => run.id);
                  for (const runId of terminalDeliveryRunIds) {
                    yield* finalizeDelegatedCompletionDelivery(threadId, runId);
                  }
                  const refreshed =
                    terminalDeliveryRunIds.length === 0
                      ? projection
                      : yield* projectionStore.getThreadRecords(threadId, ["runs"]);
                  for (const run of refreshed.runs) {
                    if (
                      run.delegatedCompletion?.delivery !== null &&
                      run.delegatedCompletion !== undefined
                    ) {
                      yield* offerDelegatedCompletionDelivery(threadId, run.id);
                    }
                  }
                }),
              )
              .pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("Failed to recover delegated completion delivery", {
                    threadId,
                    cause,
                  }),
                ),
              ),
          { concurrency: 8, discard: true },
        ),
      ),
      Effect.catchCause((cause) =>
        Effect.logWarning("Failed to inspect delegated completion delivery during recovery", {
          cause,
        }),
      ),
    );
  });

  // Pull request news the server stopped before it reacted to its wake's failure, or that the
  // startup reconcile cut before its prompt went out.
  const recoverPullRequestWakes = Effect.gen(function* () {
    yield* projectionStore
      .getPullRequestWakesToRetry(
        DateTime.subtractDuration(yield* DateTime.now, PULL_REQUEST_WAKE_RETRY_WINDOW),
      )
      .pipe(
        Effect.flatMap((wakes) =>
          Effect.forEach(wakes, ({ threadId, runId }) => retryUndeliveredNotice(threadId, runId), {
            concurrency: 8,
            discard: true,
          }),
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to inspect pull request wakes during recovery", { cause }),
        ),
      );
  });

  const delegatedTaskResultPending = Effect.fn("Orchestrator.delegatedTaskResultPending")(
    function* (childThreadId: ThreadId, taskId?: NodeId) {
      const child = yield* projectionStore.getThreadRecords(
        childThreadId,
        ["runs", "messages", "subagents", "providerThreads", "providerTurns", "attempts"],
        { messageRoles: ["user"] },
      );
      if (taskId !== undefined) {
        const runs = delegatedTaskRuns(child, taskId);
        const run = delegatedTaskResultRun(runs);
        if (
          run === undefined ||
          delegatedTaskTerminalStatus(run.status) === null ||
          delegatedTaskAwaitsWork(runs, child)
        )
          return true;
        return yield* childAwaitsRestartContinuation(childThreadId, runs, run);
      }
      const progress = delegatedTaskProgress(child);
      if (progress.state !== "result_available") return true;
      if (progress.resultRun === undefined) return false;
      return yield* childAwaitsRestartContinuation(childThreadId, child.runs, progress.resultRun);
    },
    (effect, childThreadId, _taskId?) =>
      effect.pipe(
        Effect.mapError(
          (cause) => new OrchestratorProjectionError({ threadId: childThreadId, cause }),
        ),
      ),
  );

  const recoverDelegatedTask = Effect.fn("Orchestrator.recoverDelegatedTask")(
    function* (
      threadId: ThreadId,
      options?: { readonly sourceRunId?: RunId; readonly completedRunId?: RunId },
    ) {
      const completedRunId = options?.completedRunId;
      if (completedRunId !== undefined) {
        yield* threadDispatch.withLock(
          threadId,
          Effect.gen(function* () {
            const message = yield* projectionStore.getRunMessage(threadId, completedRunId);
            if (message?.delegatedCompletion === undefined) return;
            // A prior attempt may have committed the next generation before offering its wake.
            for (const part of completionWakeParts(message.delegatedCompletion)) {
              yield* offerDelegatedCompletionDelivery(threadId, part.parentRunId);
            }
            yield* finalizeDelegatedCompletionDelivery(threadId, completedRunId);
          }),
        );
      }
      const sourceRunId = options?.sourceRunId;
      const parentThreadId = yield* appOwnedSubagentParentThreadId(threadId);
      if (parentThreadId === undefined) return;
      if (
        sourceRunId === undefined &&
        !(yield* projectionStore.hasPendingDelegatedCompletion(threadId))
      )
        return;
      yield* threadDispatch.withLock(
        parentThreadId,
        Effect.gen(function* () {
          // A task's wake lives on the run that requested it; the parent's other wakes are not this child's.
          const { subagents } = yield* projectionStore.getThreadRecords(
            parentThreadId,
            ["subagents"],
            { subagentChildThreadIds: [threadId] },
          );
          // A settled task's result was delivered already: its run's wake is not this capture's.
          const parentRunIds = new Set(
            subagents.flatMap((task) => {
              const state = task.completionDelivery?.state;
              return task.childThreadId === threadId &&
                task.runId !== null &&
                (task.result === null ||
                  (state !== "delivered" && state !== "acknowledged" && state !== "disposed"))
                ? [task.runId]
                : [];
            }),
          );
          for (const parentRunId of parentRunIds) {
            yield* offerDelegatedCompletionDelivery(parentThreadId, parentRunId);
          }
        }).pipe(
          Effect.andThen(
            finalizeAppOwnedSubagent(
              threadId,
              sourceRunId === undefined ? undefined : { settledContinuationOf: sourceRunId },
            ),
          ),
        ),
      );
    },
    (effect, threadId, _options) =>
      effect.pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause }))),
  );

  const cancelDelegatedTask: OrchestratorV2Shape["cancelDelegatedTask"] = Effect.fn(
    "Orchestrator.cancelDelegatedTask",
  )(function* (input) {
    const { command } = input;
    return yield* threadDispatch.withLock(
      input.parentThreadId,
      Effect.gen(function* () {
        const { subagents } = yield* projectionStore
          .getThreadRecords(input.parentThreadId, ["subagents"], { subagentIds: [input.taskId] })
          .pipe(mapDispatchError(command));
        const task = subagents.find(
          (candidate) =>
            candidate.id === input.taskId &&
            candidate.origin === "app_owned" &&
            candidate.childThreadId === command.threadId &&
            isOrchestrationV2WorkActive(candidate.status),
        );
        const child = yield* projectionStore
          .getThreadRecords(
            command.threadId,
            ["runs", "messages", "subagents", "providerThreads"],
            { messageRoles: ["user"] },
          )
          .pipe(mapDispatchError(command));
        const followUpRun = child.runs
          .filter((run) => run.delegatedTaskId === input.taskId)
          .toSorted((a, b) => b.ordinal - a.ordinal)[0];
        const unsettled =
          followUpRun === undefined
            ? child.thread.forkedFrom?.type === "node" &&
              child.thread.forkedFrom.nodeId === input.taskId &&
              progressAfterTurn(child).state !== "result_available"
            : delegatedTaskTerminalStatus(runOutcome(followUpRun)) === null ||
              delegatedTaskAwaitsWork(delegatedTaskRuns(child, input.taskId), child);
        yield* threadDispatch.withLock(
          command.threadId,
          Effect.gen(function* () {
            // Hold intake closed until descendants stop and this thread's Stop commits.
            yield* input.stopDescendants((command) =>
              commandThreadId(command) === input.command.threadId
                ? dispatchWithReceiptEffect(command)
                : dispatchWithReceipt(command),
            );
            const child = yield* projectionStore
              .getThreadRecords(
                command.threadId,
                ["runs", "messages", "subagents", "providerThreads"],
                { messageRoles: ["user"] },
              )
              .pipe(mapDispatchError(command));
            const followUpRun = child.runs
              .filter((run) => run.delegatedTaskId === input.taskId)
              .toSorted((a, b) => b.ordinal - a.ordinal)[0];
            const monitorRunIds = new Set(
              child.messages
                .filter((message) => message.notification?.source.kind === "monitor")
                .map((message) => message.runId),
            );
            const activeRun = child.runs
              .filter((run) => isLiveRun(run) && !monitorRunIds.has(run.id))
              .toSorted((a, b) => b.ordinal - a.ordinal)[0];
            const cancelledRun = followUpRun ?? activeRun ?? progressAfterTurn(child).resultRun;
            const now = yield* DateTime.now;
            const cancelledTask =
              task !== undefined && unsettled && cancelledRun !== undefined
                ? { ...task, cancelledRunId: cancelledRun.id, updatedAt: now }
                : undefined;
            // The marker, Stop receipt and interrupt outbox commit together under both locks.
            yield* dispatchWithReceiptEffect(command, [], cancelledTask);
          }),
        );
        yield* finalizeAppOwnedSubagent(command.threadId).pipe(mapDispatchError(command));
      }),
    );
  });

  const shellProjectionError = (cause: unknown) =>
    new OrchestratorProjectionError({ threadId: ThreadId.make("thread:shell"), cause });

  return OrchestratorV2.of({
    resumeQueuedRuns,
    recoverDelegatedTasks,
    recoverPullRequestWakes,
    recoverDelegatedTask,
    cancelDelegatedTask,
    delegatedTaskResultPending,
    restartContinuationPending: Effect.fn("OrchestratorV2.restartContinuationPending")(
      function (run) {
        return awaitsRestartContinuation(run).pipe(Effect.orElseSucceed(() => false));
      },
    ),
    awaitRestartContinuation: Effect.fn("OrchestratorV2.awaitRestartContinuation")(function (run) {
      return effectOutbox.awaitSettled(`effect:restart-continuation:${run.id}`).pipe(Effect.ignore);
    }),
    dispatch: dispatchWithReceipt,
    autoArmLimitRecovery: Effect.fn("OrchestratorV2.autoArmLimitRecovery")(function (input) {
      return threadDispatch.withLock(
        input.threadId,
        Effect.gen(function* () {
          const thread = yield* projectionStore
            .getThread(input.threadId)
            .pipe(
              Effect.mapError(
                (cause) => new OrchestratorProjectionError({ threadId: input.threadId, cause }),
              ),
            );
          // Stop or an explicit recovery choice can commit after the sweep reads its candidate.
          if (
            thread.limitRecovery?.runId === input.limitRecovery.runId &&
            thread.limitRecovery.resetAt === input.limitRecovery.resetAt
          )
            return;
          const command = { type: "thread.metadata.update", ...input } as const;
          // A Stop that reached the run before its limit failure landed had nothing to disarm.
          const stopped = yield* stopReachedRun(command, input.threadId, input.limitRecovery.runId);
          yield* dispatchWithReceiptEffect(
            stopped
              ? {
                  ...command,
                  limitRecovery: { ...input.limitRecovery, autoResume: false, snooze: false },
                }
              : command,
          );
        }),
      );
    }),
    searchThreadStream: (input) =>
      projectionStore
        .searchThreadStream(input)
        .pipe(
          Stream.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: input.threadId, cause }),
          ),
        ),
    searchThread: Effect.fn("OrchestratorV2.searchThread")(function (input) {
      return projectionStore
        .searchThread(input)
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: input.threadId, cause }),
          ),
        );
    }),
    getThreadHistoryPage: Effect.fn("OrchestratorV2.getThreadHistoryPage")(
      function (threadId, cursor, throughEntryId, conversationOnly, workMode) {
        return projectionStore
          .getThreadHistoryPage(threadId, cursor, throughEntryId, conversationOnly, workMode)
          .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
      },
    ),
    getTimelinePage: Effect.fn("OrchestratorV2.getTimelinePage")(function (threadId, options) {
      return projectionStore
        .getTimelinePage(threadId, options)
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
    }),
    getMessageCount: Effect.fn("OrchestratorV2.getMessageCount")(function (threadId) {
      return projectionStore
        .getMessageCount(threadId)
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
    }),
    getMessagesById: Effect.fn("OrchestratorV2.getMessagesById")(function (threadId, messageIds) {
      return projectionStore
        .getMessagesById(threadId, messageIds)
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
    }),
    getTranscriptByteFloor: Effect.fn("OrchestratorV2.getTranscriptByteFloor")(
      function (threadId, limit) {
        return projectionStore
          .getTranscriptByteFloor(threadId, limit)
          .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
      },
    ),
    getTurnItem: Effect.fn("OrchestratorV2.getTurnItem")(function (input) {
      return projectionStore
        .getTurnItem(input)
        .pipe(
          Effect.mapError(
            (cause) => new OrchestratorProjectionError({ threadId: input.threadId, cause }),
          ),
        );
    }),
    getThreadRecords: Effect.fn("OrchestratorV2.getThreadRecords")(
      function (threadId, fields, filter) {
        return projectionStore
          .getThreadRecords(threadId, fields, filter)
          .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
      },
    ),
    listAppOwnedChildThreadIds: Effect.fn("OrchestratorV2.listAppOwnedChildThreadIds")(
      function (threadId) {
        return projectionStore
          .listAppOwnedChildThreadIds(threadId)
          .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
      },
    ),
    getThreadProjection: Effect.fn("OrchestratorV2.getThreadProjection")(function (threadId) {
      return projectionStore
        .getThreadProjection(threadId)
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
    }),
    getCheckpointContext: Effect.fn("OrchestratorV2.getCheckpointContext")(function (threadId) {
      return projectionStore
        .getCheckpointContext(threadId)
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
    }),
    getThreadSnapshot: Effect.fn("OrchestratorV2.getThreadSnapshot")(function (threadId) {
      return projectionStore
        .getThreadSnapshot(threadId)
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
    }),
    getThreadSnapshotWindow: Effect.fn("OrchestratorV2.getThreadSnapshotWindow")(
      function (threadId, options) {
        return projectionStore
          .getThreadSnapshotWindow(threadId, options)
          .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
      },
    ),
    getPullRequestSnapshot: Effect.fn("OrchestratorV2.getPullRequestSnapshot")(function (options) {
      return projectionStore
        .getPullRequestSnapshot(options)
        .pipe(Effect.mapError(shellProjectionError));
    }),
    getAgentAwarenessSnapshot: Effect.fn("OrchestratorV2.getAgentAwarenessSnapshot")(
      function (threadId) {
        return projectionStore
          .getAgentAwarenessSnapshot(threadId)
          .pipe(Effect.mapError(shellProjectionError));
      },
    ),
    getShellSnapshot: Effect.fn("OrchestratorV2.getShellSnapshot")(function (options) {
      return projectionStore.getShellSnapshot(options).pipe(Effect.mapError(shellProjectionError));
    }),
    listActiveSubagentThreadIds: Effect.fn("OrchestratorV2.listActiveSubagentThreadIds")(
      function () {
        return projectionStore
          .listActiveSubagentThreadIds()
          .pipe(Effect.mapError(shellProjectionError));
      },
    ),
    countThreads: Effect.fn("OrchestratorV2.countThreads")(function () {
      return projectionStore.countThreads().pipe(Effect.mapError(shellProjectionError));
    }),
    listActiveShellIndex: Effect.fn("OrchestratorV2.listActiveShellIndex")(function () {
      return projectionStore.listActiveShellIndex().pipe(Effect.mapError(shellProjectionError));
    }),
    listActiveThreads: Effect.fn("OrchestratorV2.listActiveThreads")(function (projectId) {
      return projectionStore
        .listActiveThreads(projectId)
        .pipe(Effect.mapError(shellProjectionError));
    }),
    listOlderSettledThreads: Effect.fn("OrchestratorV2.listOlderSettledThreads")(function (query) {
      return projectionStore
        .listOlderSettledThreads(query)
        .pipe(Effect.mapError(shellProjectionError));
    }),
    readShellSnapshot: Effect.fn("OrchestratorV2.readShellSnapshot")(function (options) {
      return projectionStore
        .readShellSnapshot(options)
        .pipe(
          Effect.mapError(shellProjectionError),
          Effect.map(Effect.mapError(shellProjectionError)),
        );
    }),
    getThreadShell: Effect.fn("OrchestratorV2.getThreadShell")(function (threadId) {
      return projectionStore
        .getThreadShell(threadId)
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
    }),
    findGuestShareThreadId: Effect.fn("OrchestratorV2.findGuestShareThreadId")(function (shareId) {
      return projectionStore
        .findGuestShareThreadId(shareId)
        .pipe(Effect.mapError(shellProjectionError));
    }),
    getThreadLinkMetadata: Effect.fn("OrchestratorV2.getThreadLinkMetadata")(function (threadIds) {
      return projectionStore
        .getThreadLinkMetadata(threadIds)
        .pipe(Effect.mapError(shellProjectionError));
    }),
    getThreadTitles: Effect.fn("OrchestratorV2.getThreadTitles")(function (threadIds) {
      return projectionStore.getThreadTitles(threadIds).pipe(Effect.mapError(shellProjectionError));
    }),
    getThreadEventSequence: Effect.fn("OrchestratorV2.getThreadEventSequence")(function (threadId) {
      return eventSink
        .latestSequence({ threadId })
        .pipe(Effect.mapError((cause) => new OrchestratorProjectionError({ threadId, cause })));
    }),
    streamStoredEvents: eventSink.stream().pipe(
      Stream.mapError(
        (cause) =>
          new OrchestratorDomainEventStreamError({
            cause,
          }),
      ),
    ),
    streamStoredEventsFrom: (input) =>
      eventSink.stream({ ...input, bounded: true }).pipe(
        Stream.mapError(
          (cause) =>
            new OrchestratorDomainEventStreamError({
              cause,
            }),
        ),
      ),
    // Start at the latest sequence; awareness needs no historical replay.
    streamDomainEvents: Stream.unwrap(
      eventSink
        .latestSequence()
        .pipe(Effect.map((latest) => eventSink.stream({ afterSequence: latest }))),
    ).pipe(
      Stream.map((stored) => stored.event),
      Stream.mapError(
        (cause) =>
          new OrchestratorDomainEventStreamError({
            cause,
          }),
      ),
    ),
  });
});

export const layer: Layer.Layer<
  OrchestratorV2,
  never,
  | CheckpointServiceV2
  | FileSystem.FileSystem
  | Path.Path
  | CommandPolicyV2
  | CommandReceiptStoreV2
  | ContextHandoffServiceV2
  | EffectOutbox.EffectOutboxV2
  | EventSinkV2
  | IdAllocator.IdAllocatorV2
  | ProjectStore.ProjectStoreV2
  | ProviderAdapterRegistryV2
  | ProviderSessionManagerV2
  | ProviderSwitchServiceV2
  | ProjectionStoreV2
  | RuntimePolicyV2
  | ServerSettings.ServerSettingsService
  | ThreadForkServiceV2
> = Layer.effect(OrchestratorV2, makeOrchestrator()).pipe(
  Layer.provide(ThreadCommandExecutor.layer),
);

// @effect-diagnostics nodeBuiltinImport:off - node:perf_hooks is the only source of the event loop delay.
import * as NodePerfHooks from "node:perf_hooks";

import { CommandId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import {
  increment,
  metricAttributes,
  orchestrationEffectClaimsTotal,
  orchestrationEffectQueueWait,
} from "../observability/Metrics.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import * as ResourceCleanupService from "./ResourceCleanupService.ts";
import * as EffectAdmission from "./EffectAdmission.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as ProviderTurnStartService from "./ProviderTurnStartService.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import * as ThreadTitleRegenerationService from "./ThreadTitleRegenerationService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ServerSettings from "../serverSettings.ts";
import { continueRestartedRun } from "./RestartContinuation.ts";

export class OrchestrationEffectExecutionError extends Schema.TaggedError<OrchestrationEffectExecutionError>()(
  "OrchestrationEffectExecutionError",
  {
    effectId: Schema.String,
    effectType: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

/**
 * Do not apply this to `provider-turn.restart`: that compound effect also runs
 * detach and start. Swallowing a start failure that happens to mention
 * "is not active" would drop the outbox item without ever starting the
 * replacement turn.
 */
function isNonRetryableProviderTurnControlFailure(effectType: string, errorText: string): boolean {
  if (effectType !== "provider-turn.interrupt") {
    return false;
  }
  return (
    /is not active/i.test(errorText) ||
    /hard teardown is already in progress/i.test(errorText) ||
    /treating as already interrupted/i.test(errorText) ||
    /treating as already stopped/i.test(errorText)
  );
}

export interface OrchestrationEffectExecutorV2Shape {
  readonly execute: (
    effect: EffectOutbox.OrchestrationEffectV2,
    options?: { readonly willRetry: boolean },
  ) => Effect.Effect<void, OrchestrationEffectExecutionError>;
}

export class OrchestrationEffectExecutorV2 extends Context.Service<
  OrchestrationEffectExecutorV2,
  OrchestrationEffectExecutorV2Shape
>()("t3/orchestration-v2/EffectWorker/OrchestrationEffectExecutorV2") {}

export const layerExecutor: Layer.Layer<
  OrchestrationEffectExecutorV2,
  never,
  | ProviderSessionManager.ProviderSessionManagerV2
  | RunFinalizationService.RunFinalizationService
  | CheckpointRollbackService.CheckpointRollbackServiceV2
  | ProviderTurnControlService.ProviderTurnControlServiceV2
  | ProviderTurnStartService.ProviderTurnStartServiceV2
  | RuntimeRequestService.RuntimeRequestServiceV2
  | ThreadTitleRegenerationService.ThreadTitleRegenerationService
  | ThreadManagementService.ThreadManagementService
  | ServerSettings.ServerSettingsService
> = Layer.effect(
  OrchestrationEffectExecutorV2,
  Effect.gen(function* () {
    const runFinalization = yield* RunFinalizationService.RunFinalizationService;
    const resourceCleanup = yield* ResourceCleanupService.ResourceCleanupService;
    const checkpointRollback = yield* CheckpointRollbackService.CheckpointRollbackServiceV2;
    const providerSessions = yield* ProviderSessionManager.ProviderSessionManagerV2;
    const providerTurnControl = yield* ProviderTurnControlService.ProviderTurnControlServiceV2;
    const providerTurnStart = yield* ProviderTurnStartService.ProviderTurnStartServiceV2;
    const runtimeRequests = yield* RuntimeRequestService.RuntimeRequestServiceV2;
    const threadTitleRegeneration =
      yield* ThreadTitleRegenerationService.ThreadTitleRegenerationService;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const settings = yield* ServerSettings.ServerSettingsService;
    return OrchestrationEffectExecutorV2.of({
      execute: Effect.fn("OrchestrationEffectExecutorV2.execute")(function (effect, options) {
        const willRetry = options?.willRetry ?? false;
        switch (effect.request.type) {
          case "provider-runtime.continue": {
            const sourceRunId = effect.request.sourceRunId;
            return continueRestartedRun({ threadId: effect.threadId, sourceRunId }).pipe(
              Effect.provideService(ThreadManagementService.ThreadManagementService, threads),
              Effect.provideService(ServerSettings.ServerSettingsService, settings),
              Effect.tapError(() =>
                willRetry
                  ? Effect.void
                  : threads.recoverDelegatedTask(effect.threadId, sourceRunId),
              ),
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
          }
          case "provider-session.detach":
            return providerSessions
              .detach({
                providerSessionId: effect.request.providerSessionId,
                threadId: effect.threadId,
                ...(effect.request.detail === undefined ? {} : { detail: effect.request.detail }),
                ...(effect.request.revokeMcpCredential === undefined
                  ? {}
                  : { revokeMcpCredential: effect.request.revokeMcpCredential }),
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-turn.start":
            return providerTurnStart
              .start({ threadId: effect.threadId, runId: effect.request.runId, willRetry })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-turn.interrupt": {
            const request = effect.request;
            return providerTurnControl
              .interrupt({
                threadId: effect.threadId,
                providerSessionId: request.providerSessionId,
                providerThreadId: request.providerThreadId,
                providerTurnId: request.providerTurnId,
                ...(request.subagent === undefined
                  ? {}
                  : {
                      subagent: {
                        ...request.subagent,
                        requestedAt: DateTime.makeUnsafe(effect.createdAt),
                      },
                    }),
              })
              .pipe(
                Effect.tapError((cause) =>
                  request.subagent === undefined
                    ? Effect.void
                    : Effect.logWarning("orchestration-v2.subagent-stop-failed", {
                        effectId: effect.id,
                        cause,
                      }),
                ),
                Effect.map((outcome) => ({ unresponsive: false, outcome }) as const),
                // Settling here would end the run while the agent may still be running; the
                // user decides with Force stop instead.
                Effect.catchIf(
                  (cause) => request.subagent === undefined && cause.unresponsive === true,
                  () => Effect.succeed({ unresponsive: true, outcome: undefined } as const),
                ),
                Effect.catchIf(
                  (cause) =>
                    request.subagent === undefined &&
                    isNonRetryableProviderTurnControlFailure(
                      request.type,
                      Cause.pretty(Cause.fail(cause)),
                    ),
                  () => Effect.succeed({ unresponsive: false, outcome: undefined } as const),
                ),
                Effect.andThen(({ unresponsive, outcome }) =>
                  unresponsive
                    ? threads.dispatch({
                        type: "thread.interrupt.unresponsive",
                        commandId: CommandId.make(`${effect.id}:unresponsive`),
                        threadId: effect.threadId,
                        providerThreadId: request.providerThreadId,
                        providerTurnId: request.providerTurnId,
                      })
                    : request.subagent !== undefined && outcome === undefined
                      ? Effect.void
                      : threads.dispatch({
                          type: "thread.background-work.settle",
                          commandId: CommandId.make(`${effect.id}:background-work-settled`),
                          ...(request.subagent === undefined || outcome === undefined
                            ? {}
                            : {
                                subagent: {
                                  id: request.subagent.id,
                                  stoppedAt: DateTime.makeUnsafe(effect.createdAt),
                                  outcome,
                                },
                              }),
                          threadId: effect.threadId,
                          providerThreadId: request.providerThreadId,
                          providerTurnId: request.providerTurnId,
                        }),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: request.type,
                      cause,
                    }),
                ),
              );
          }
          case "provider-turn.steer":
            return providerTurnControl
              .steer({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                providerThreadId: effect.request.providerThreadId,
                providerTurnId: effect.request.providerTurnId,
                messageId: effect.request.messageId,
              })
              .pipe(
                Effect.tap(() =>
                  Effect.gen(function* () {
                    if (effect.request.type !== "provider-turn.steer") return;
                    const messageId = effect.request.messageId;
                    const projection = yield* threads.getThreadRecords(
                      effect.threadId,
                      ["messages", "runs"],
                      { messageIds: [effect.request.messageId] },
                    );
                    const message = projection.messages.find((row) => row.id === messageId);
                    if (message?.delegatedCompletion === undefined) return;
                    yield* threads.dispatch({
                      type: "notification.delivery.accept",
                      commandId: CommandId.make(`command:mailbox-accepted:${effect.id}`),
                      threadId: effect.threadId,
                      messageId: message.id,
                    });
                  }),
                ),
                Effect.catch((error) =>
                  Effect.gen(function* () {
                    if (
                      !("turnCompleted" in error) ||
                      !error.turnCompleted ||
                      effect.request.type !== "provider-turn.steer"
                    ) {
                      return yield* error;
                    }
                    const projection = yield* threads.getThreadRecords(
                      effect.threadId,
                      ["messages", "runs"],
                      { messageIds: [effect.request.messageId] },
                    );
                    const messageId = effect.request.messageId;
                    const message = projection.messages.find((item) => item.id === messageId);
                    const run = projection.runs.find((item) => item.id === message?.runId);
                    if (message === undefined || run === undefined) return yield* error;
                    // Reuse message ids and receipts so retries cannot duplicate messages or follow-ups.
                    yield* threads
                      .dispatch({
                        type: "message.dispatch",
                        commandId: CommandId.make(`command:steer-follow-up:${effect.id}`),
                        threadId: effect.threadId,
                        messageId: message.id,
                        text: message.text,
                        ...(message.context ? { context: message.context } : {}),
                        attachments: message.attachments,
                        ...(message.delegatedCompletion === undefined
                          ? {}
                          : { modelSelection: run.modelSelection }),
                        dispatchMode: {
                          type:
                            message.delegatedCompletion === undefined
                              ? "start_immediately"
                              : "queue_after_active",
                        },
                        createdBy: message.createdBy,
                        creationSource: message.creationSource,
                        ...(message.delegatedCompletion === undefined
                          ? {}
                          : { delegatedCompletion: message.delegatedCompletion }),
                        ...(message.notification === undefined
                          ? {}
                          : { notification: message.notification }),
                        ...(message.scheduledTaskId === undefined
                          ? {}
                          : { scheduledTaskId: message.scheduledTaskId }),
                        ...(message.senderThreadId === undefined
                          ? {}
                          : { senderThreadId: message.senderThreadId }),
                      })
                      .pipe(
                        // A disabled instance cannot retry this completed turn; its result stands.
                        Effect.catchTags({
                          ThreadManagementProviderInstanceDisabledError: (disabledError) =>
                            Effect.logInfo(
                              "orchestration-v2.effect-worker.steer-follow-up-disabled",
                              {
                                threadId: effect.threadId,
                                messageId: message.id,
                                projectId: disabledError.projectId,
                                instanceId: disabledError.instanceId,
                              },
                            ),
                        }),
                      );
                  }),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "provider-turn.restart":
            return providerTurnControl
              .interruptAndAwaitTerminal({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                providerThreadId: effect.request.providerThreadId,
                providerTurnId: effect.request.providerTurnId,
                interruptedAttemptId: effect.request.interruptedAttemptId,
                ...(effect.request.sessionTransition?.type === "replace"
                  ? {
                      replacementProviderSessionId:
                        effect.request.sessionTransition.replacementProviderSessionId,
                    }
                  : {}),
              })
              .pipe(
                Effect.andThen(
                  effect.request.sessionTransition?.type === "replace"
                    ? providerSessions.detach({
                        providerSessionId: effect.request.providerSessionId,
                        threadId: effect.threadId,
                        detail: "Selection change requires a provider session restart.",
                      })
                    : effect.request.sessionTransition?.type === "detach"
                      ? providerSessions.detach({
                          providerSessionId: effect.request.providerSessionId,
                          threadId: effect.threadId,
                          detail: "Provider thread handoff replaced this session binding.",
                        })
                      : Effect.void,
                ),
                Effect.andThen(
                  providerTurnStart.start({
                    threadId: effect.threadId,
                    runId: effect.request.runId,
                    willRetry,
                  }),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "runtime-request.respond": {
            const requestId = effect.request.requestId;
            return runtimeRequests
              .respond({
                threadId: effect.threadId,
                providerSessionId: effect.request.providerSessionId,
                requestId: effect.request.requestId,
                ...(effect.request.decision === undefined
                  ? {}
                  : { decision: effect.request.decision }),
                ...(effect.request.feedback === undefined
                  ? {}
                  : { feedback: effect.request.feedback }),
                ...(effect.request.answers === undefined
                  ? {}
                  : { answers: effect.request.answers }),
              })
              .pipe(
                Effect.tapCause((cause) =>
                  willRetry || Cause.hasInterruptsOnly(cause)
                    ? Effect.void
                    : threads
                        .dispatch({
                          type: "runtime-request.delivery.fail",
                          commandId: CommandId.make(`${effect.commandId}:delivery-failed`),
                          threadId: effect.threadId,
                          requestId,
                        })
                        .pipe(
                          Effect.catchCause((recordCause) =>
                            Effect.logWarning("Failed to record answer delivery failure", {
                              effectId: effect.id,
                              cause: recordCause,
                            }),
                          ),
                        ),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          }
          case "provider-thread.rollback":
            return checkpointRollback
              .execute({
                threadId: effect.threadId,
                providerThreadId: effect.request.providerThreadId,
                checkpointId: effect.request.checkpointId,
                scopeId: effect.request.scopeId,
                ...(effect.request.restoreFiles === undefined
                  ? {}
                  : { restoreFiles: effect.request.restoreFiles }),
              })
              .pipe(
                Effect.tapCause((cause) =>
                  willRetry || Cause.hasInterruptsOnly(cause)
                    ? Effect.void
                    : threads
                        .dispatch({
                          type: "checkpoint.rollback.fail",
                          commandId: CommandId.make(`${effect.commandId}:rollback-failed`),
                          threadId: effect.threadId,
                          requestId: effect.commandId,
                          message: Option.match(Cause.findErrorOption(cause), {
                            onNone: () => CheckpointRollbackService.ROLLBACK_FAILED_MESSAGE,
                            onSome: CheckpointRollbackService.rollbackFailureClientMessage,
                          }),
                        })
                        .pipe(
                          Effect.catchCause((recordCause) =>
                            Effect.logWarning("Failed to record rollback failure", {
                              effectId: effect.id,
                              cause: recordCause,
                            }),
                          ),
                        ),
                ),
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "checkpoint.capture":
            return runFinalization
              .finalize({
                threadId: effect.threadId,
                runId: effect.request.runId,
                scopeId: effect.request.scopeId,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "scheduled-tasks.cleanup":
            return resourceCleanup.cleanupScheduledTasks(effect.threadId).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
          case "terminal.cleanup":
            return resourceCleanup.cleanupTerminals(effect.threadId).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
          case "preview.cleanup":
            return resourceCleanup.cleanupPreviews(effect.threadId).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
          case "attachment.cleanup":
            return resourceCleanup.cleanupAttachments(effect.request.attachmentIds).pipe(
              Effect.mapError(
                (cause) =>
                  new OrchestrationEffectExecutionError({
                    effectId: effect.id,
                    effectType: effect.request.type,
                    cause,
                  }),
              ),
            );
          case "thread-title.generate":
            return threadTitleRegeneration
              .execute({
                threadId: effect.threadId,
                requestId: effect.commandId,
                kind: effect.request.kind,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
          case "delegated-tasks.stop":
            return threads
              .stopDelegatedTasks({
                threadId: effect.threadId,
                commandId: effect.commandId,
                reason: effect.request.reason,
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new OrchestrationEffectExecutionError({
                      effectId: effect.id,
                      effectType: effect.request.type,
                      cause,
                    }),
                ),
              );
        }
      }),
    });
  }),
);

// A running effect blocks later work on its thread and its lease is never reclaimed in-process,
// so a stop that hangs must give up and fall through to the normal retry/fail path.
const DELEGATED_TASKS_STOP_TIMEOUT = Duration.seconds(30);

const boundExecution =
  (effect: EffectOutbox.OrchestrationEffectV2) =>
  (execution: Effect.Effect<void, OrchestrationEffectExecutionError>) =>
    effect.request.type === "delegated-tasks.stop"
      ? execution.pipe(
          Effect.timeoutOrElse({
            duration: DELEGATED_TASKS_STOP_TIMEOUT,
            orElse: () =>
              Effect.fail(
                new OrchestrationEffectExecutionError({
                  effectId: effect.id,
                  effectType: effect.request.type,
                  cause: `Timed out after ${Duration.format(DELEGATED_TASKS_STOP_TIMEOUT)}.`,
                }),
              ),
          }),
        )
      : execution;

export class OrchestrationEffectWorkerError extends Schema.TaggedError<OrchestrationEffectWorkerError>()(
  "OrchestrationEffectWorkerError",
  {
    operation: Schema.String,
    effectId: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isOrchestrationEffectWorkerError = Schema.is(OrchestrationEffectWorkerError);

export interface OrchestrationEffectWorkerV2Shape {
  readonly awaitWork: Effect.Effect<void>;
  /** Wakes up to `count` idle workers to claim again, as new outbox rows do. */
  readonly notifyWork: (count?: number) => Effect.Effect<void>;
  readonly runOnce: Effect.Effect<boolean, OrchestrationEffectWorkerError>;
  readonly runRecoveryOnce: Effect.Effect<boolean, OrchestrationEffectWorkerError>;
  readonly nextClaimableAt: Effect.Effect<
    Option.Option<DateTime.Utc>,
    OrchestrationEffectWorkerError
  >;
  readonly nextClaimableAtExcluding: (
    excludeEffectTypes: ReadonlyArray<string>,
  ) => Effect.Effect<Option.Option<DateTime.Utc>, OrchestrationEffectWorkerError>;
  readonly drain: (maxEffects?: number) => Effect.Effect<number, OrchestrationEffectWorkerError>;
}

export class OrchestrationEffectWorkerV2 extends Context.Service<
  OrchestrationEffectWorkerV2,
  OrchestrationEffectWorkerV2Shape
>()("t3/orchestration-v2/EffectWorker/OrchestrationEffectWorkerV2") {}

export interface OrchestrationEffectWorkerOptions {
  readonly workerId?: string;
  readonly maxAttempts?: number;
}

export const layerWithOptions = (
  options: OrchestrationEffectWorkerOptions = {},
): Layer.Layer<
  OrchestrationEffectWorkerV2,
  never,
  EffectOutbox.EffectOutboxV2 | OrchestrationEffectExecutorV2
> =>
  Layer.effect(
    OrchestrationEffectWorkerV2,
    Effect.gen(function* () {
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const executor = yield* OrchestrationEffectExecutorV2;
      const workerId = options.workerId ?? `orchestration-v2:${process.pid}`;
      const maxAttempts = Math.max(1, options.maxAttempts ?? 5);
      const wasCancelled = (effectId: string) =>
        outbox.get(effectId).pipe(
          Effect.map(
            Option.match({
              onNone: () => false,
              onSome: (effect) => effect.status === "cancelled",
            }),
          ),
        );
      const requeueClaim = (
        effect: EffectOutbox.OrchestrationEffectV2,
        cause: Cause.Cause<unknown>,
      ) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.void
          : outbox
              .retry({
                effectId: effect.id,
                workerId,
                error: `Worker failed before settling the claimed effect: ${Cause.pretty(cause)}`,
                delayMs: 0,
              })
              .pipe(
                Effect.flatMap((requeued) =>
                  requeued
                    ? Effect.logWarning("Requeued effect after unexpected worker failure", {
                        effectId: effect.id,
                        effectType: effect.request.type,
                      })
                    : Effect.logWarning("Could not requeue effect after worker lost its lease", {
                        effectId: effect.id,
                        effectType: effect.request.type,
                      }),
                ),
                Effect.catchCause((requeueCause) =>
                  Effect.logError("Failed to requeue effect after unexpected worker failure", {
                    effectId: effect.id,
                    effectType: effect.request.type,
                    error: Cause.pretty(requeueCause),
                  }),
                ),
              );
      const terminalizeClaim = (
        effect: EffectOutbox.OrchestrationEffectV2,
        cause: Cause.Cause<unknown>,
      ) => {
        if (Cause.hasInterruptsOnly(cause)) return Effect.void;
        return outbox
          .fail({
            effectId: effect.id,
            workerId,
            error: `Worker failed to settle a process-bound effect after execution started: ${Cause.pretty(cause)}`,
          })
          .pipe(
            Effect.flatMap((failed) =>
              failed
                ? Effect.logError("Terminalized process-bound effect after settlement failure", {
                    effectId: effect.id,
                    effectType: effect.request.type,
                  })
                : Effect.logWarning(
                    "Could not terminalize process-bound effect after worker lost its lease",
                    {
                      effectId: effect.id,
                      effectType: effect.request.type,
                    },
                  ),
            ),
            Effect.catchCause((failCause) =>
              Effect.logError(
                "Failed to terminalize process-bound effect after settlement failure",
                {
                  effectId: effect.id,
                  effectType: effect.request.type,
                  error: Cause.pretty(failCause),
                },
              ),
            ),
          );
      };
      const recoverPostSuccessSettlement = (
        effect: EffectOutbox.OrchestrationEffectV2,
        cause: Cause.Cause<unknown>,
      ) =>
        EffectOutbox.REPLAY_SAFE_EFFECT_TYPES_AFTER_PROCESS_LOSS.some(
          (effectType) => effectType === effect.request.type,
        )
          ? requeueClaim(effect, cause)
          : terminalizeClaim(effect, cause);

      // Untraced: the claimed effect's own EffectWorker.runEffect span covers its execution.
      const runClaimed = Effect.fnUntraced(function* (
        effect: EffectOutbox.OrchestrationEffectV2,
        gated: boolean,
      ) {
        // Arm cancellation before rereading durable state to close the read-to-registration race.
        const cancellation = outbox
          .awaitCancellation(effect.id)
          .pipe(Effect.as("cancelled" as const));
        const { cancelledBeforeExecution, queueWaitMs } = yield* Effect.gen(function* () {
          const claimedAt = DateTime.toEpochMillis(yield* DateTime.now);
          const eligibleAt = Math.max(
            DateTime.toEpochMillis(DateTime.makeUnsafe(effect.createdAt)),
            DateTime.toEpochMillis(DateTime.makeUnsafe(effect.availableAt)),
          );
          const queueWaitMs = Math.max(0, claimedAt - eligibleAt);
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationEffectQueueWait,
              metricAttributes({ effect_type: effect.request.type }),
            ),
            Duration.millis(queueWaitMs),
          );
          // Recheck durable cancellation after registering the Deferred; cancellation may have committed after claim.
          if (yield* wasCancelled(effect.id)) {
            yield* outbox.clearCancellation(effect.id);
            return { cancelledBeforeExecution: true, queueWaitMs };
          }
          return { cancelledBeforeExecution: false, queueWaitMs };
        }).pipe(Effect.onError((cause) => requeueClaim(effect, cause)));
        if (cancelledBeforeExecution) return true;

        const execution = executor
          .execute(effect, { willRetry: effect.attemptCount < maxAttempts })
          .pipe(
            boundExecution(effect),
            Effect.withSpan("EffectWorker.runEffect", {
              attributes: {
                "effect.type": effect.request.type,
                "effect.queue_wait_ms": queueWaitMs,
                "effect.attempt": effect.attemptCount,
                "effect.gated": gated,
              },
            }),
            Effect.as("executed" as const),
          );
        const exit = yield* Effect.exit(Effect.raceFirst(execution, cancellation)).pipe(
          Effect.ensuring(outbox.clearCancellation(effect.id)),
        );
        if (Exit.isSuccess(exit) && exit.value === "cancelled") {
          return true;
        }
        if (Exit.isSuccess(exit)) {
          return yield* Effect.gen(function* () {
            const completed = yield* outbox.succeed({ effectId: effect.id, workerId });
            if (!completed) {
              if (yield* wasCancelled(effect.id)) return true;
              return yield* new OrchestrationEffectWorkerError({
                operation: "complete",
                effectId: effect.id,
                cause: "The worker no longer owns the effect lease.",
              });
            }
            return true;
          }).pipe(Effect.onError((cause) => recoverPostSuccessSettlement(effect, cause)));
        }

        const error = Cause.pretty(exit.cause);
        const nonRetryable = isNonRetryableProviderTurnControlFailure(effect.request.type, error);
        yield* Effect.logWarning("Orchestration effect execution failed", {
          effectId: effect.id,
          effectType: effect.request.type,
          attemptCount: effect.attemptCount,
          nonRetryable,
          error,
        });
        const updated = nonRetryable
          ? yield* outbox
              .succeed({ effectId: effect.id, workerId })
              .pipe(Effect.onError((cause) => terminalizeClaim(effect, cause)))
          : effect.attemptCount >= maxAttempts
            ? yield* outbox
                .fail({ effectId: effect.id, workerId, error })
                .pipe(Effect.onError((cause) => terminalizeClaim(effect, cause)))
            : yield* outbox
                .retry({
                  effectId: effect.id,
                  workerId,
                  error,
                  delayMs: Math.min(30_000, 100 * 2 ** Math.max(0, effect.attemptCount - 1)),
                })
                .pipe(Effect.onError((cause) => requeueClaim(effect, cause)));
        if (!updated) {
          if (yield* wasCancelled(effect.id)) return true;
          return yield* new OrchestrationEffectWorkerError({
            operation: "reschedule",
            effectId: effect.id,
            cause: "The worker no longer owns the effect lease.",
          });
        }
        return true;
      });

      // The worker probes for work repeatedly, including when the outbox is empty.
      const runOnce = Effect.fnUntraced(
        function* (excludeRestartContinuations = false) {
          // While the main thread is saturated a worker without a slot claims only ungated effects.
          const admission = yield* EffectAdmission.EffectAdmission;
          const reservation = yield* admission.reserve;
          const releaseReservation = Option.getOrElse(reservation, () => Effect.void);
          const claimExit = yield* Effect.exit(
            outbox.claimNext({
              workerId,
              excludeRestartContinuations,
              ...(Option.isNone(reservation)
                ? { excludeEffectTypes: EffectAdmission.GATED_EFFECT_TYPES }
                : {}),
            }),
          );
          yield* increment(orchestrationEffectClaimsTotal, {
            result: Exit.isFailure(claimExit)
              ? "error"
              : Option.isNone(claimExit.value)
                ? "empty"
                : "claimed",
          });
          if (Exit.isFailure(claimExit)) {
            yield* releaseReservation;
            return yield* Effect.failCause(claimExit.cause);
          }
          const claimed = claimExit.value;
          if (Option.isNone(claimed)) {
            yield* releaseReservation;
            return false;
          }
          const effect = claimed.value;
          const gated = EffectAdmission.isGated(effect.request.type);
          if (!gated) yield* releaseReservation;
          return yield* runClaimed(effect, gated).pipe(
            Effect.ensuring(gated ? releaseReservation : Effect.void),
          );
        },
        (effect, _excludeRestartContinuations?: boolean) =>
          effect.pipe(
            Effect.mapError((cause) =>
              isOrchestrationEffectWorkerError(cause)
                ? cause
                : new OrchestrationEffectWorkerError({ operation: "run", cause }),
            ),
          ),
      );

      return OrchestrationEffectWorkerV2.of({
        awaitWork: outbox.awaitAvailable,
        notifyWork: outbox.notifyAvailable,
        runOnce: runOnce(),
        runRecoveryOnce: runOnce(true),
        nextClaimableAt: outbox.nextClaimableAt.pipe(
          Effect.mapError(
            (cause) =>
              new OrchestrationEffectWorkerError({
                operation: "next-claimable",
                cause,
              }),
          ),
        ),
        nextClaimableAtExcluding: Effect.fnUntraced(
          function* (excludeEffectTypes: ReadonlyArray<string>) {
            return yield* outbox.nextClaimableAtExcluding(excludeEffectTypes);
          },
          Effect.mapError(
            (cause) =>
              new OrchestrationEffectWorkerError({
                operation: "next-claimable",
                cause,
              }),
          ),
        ),
        drain: Effect.fn("EffectWorker.drain")(function* (maxEffects = Number.MAX_SAFE_INTEGER) {
          let completed = 0;
          while (completed < maxEffects && (yield* runOnce())) {
            completed += 1;
          }
          return completed;
        }),
      });
    }),
  );

export const layer = layerWithOptions();

export interface OrchestrationEffectDaemonOptions {
  readonly concurrency?: number;
  /** Leave turn starts in the outbox while the loop delays work this much; `false` claims everything. */
  readonly admission?: { readonly delayAboveMs: number } | false;
  /** The pressure source (the loop's delay p99 per sample interval, in ms); tests inject one. */
  readonly pressure?: Effect.Effect<Effect.Effect<number>, never, Scope.Scope>;
  readonly livenessPollIntervalMs?: number;
}

// Effects in flight at once. A turn start's history reads hold reader permits (three by default)
// and are decoded on the main thread; more workers than that plus one moved the queue from the
// outbox to the readers on the bench and slowed every small read, so the count stays at 4.
const DEFAULT_EFFECT_WORKER_CONCURRENCY = 4;
export const EFFECT_WORKER_MAX_CONCURRENCY = 256;
const DEFAULT_EFFECT_WORKER_LIVENESS_POLL_INTERVAL_MS = 30_000;
// From this event loop delay p99 over a second, turn starts stay in the outbox; below half of it
// they resume. Two samples either way, so a single burst switches nothing.
const DEFAULT_EFFECT_ADMISSION_DELAY_MS = 40;
// The reopen mark at half must clear the histogram's resolution and an idle loop's ~11 ms p99.
export const EFFECT_ADMISSION_MIN_DELAY_MS = 30;
export const EFFECT_ADMISSION_MAX_DELAY_MS = 60_000;
const EFFECT_ADMISSION_DELAY_RESOLUTION_MS = 10;

/** The daemon option for a configured delay mark: unset keeps the default, 0 turns the gate off. */
export const admissionOptionsFor = (
  delayMs: number | undefined,
): Pick<OrchestrationEffectDaemonOptions, "admission"> =>
  delayMs === undefined ? {} : { admission: delayMs > 0 ? { delayAboveMs: delayMs } : false };
const EFFECT_ADMISSION_SAMPLES_TO_SWITCH = 2;
const EFFECT_ADMISSION_SAMPLE_INTERVAL = Duration.seconds(1);

/**
 * The event loop's delay since the previous call, from a histogram reset at each call, or the
 * sampler's own lateness when that is larger. Acquired only while the gate can act.
 */
const eventLoopDelaySource: Effect.Effect<Effect.Effect<number>, never, Scope.Scope> = Effect.gen(
  function* () {
    const histogram = yield* Effect.acquireRelease(
      Effect.sync(() => {
        const created = NodePerfHooks.monitorEventLoopDelay({
          resolution: EFFECT_ADMISSION_DELAY_RESOLUTION_MS,
        });
        created.enable();
        return created;
      }),
      (created) => Effect.sync(() => created.disable()),
    );
    const intervalMs = Duration.toMillis(EFFECT_ADMISSION_SAMPLE_INTERVAL);
    let previousMs = NodePerfHooks.performance.now();
    return Effect.sync(() => {
      const nowMs = NodePerfHooks.performance.now();
      const latenessMs = nowMs - previousMs - intervalMs;
      previousMs = nowMs;
      const p99Ms = histogram.percentile(99) / 1e6;
      histogram.reset();
      return EffectAdmission.latencyPressure(p99Ms, latenessMs);
    });
  },
);

export const runDaemonWithOptions = (options: OrchestrationEffectDaemonOptions = {}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const worker = yield* OrchestrationEffectWorkerV2;
      const requestedConcurrency = options.concurrency ?? DEFAULT_EFFECT_WORKER_CONCURRENCY;
      const concurrency = Number.isFinite(requestedConcurrency)
        ? Math.max(1, Math.floor(requestedConcurrency))
        : DEFAULT_EFFECT_WORKER_CONCURRENCY;
      const delayAboveMs =
        options.admission === false
          ? undefined
          : (options.admission?.delayAboveMs ?? DEFAULT_EFFECT_ADMISSION_DELAY_MS);
      yield* Effect.logInfo("Orchestration effect worker pool", {
        concurrency,
        configured: options.concurrency !== undefined,
        admissionDelayMs: delayAboveMs ?? null,
      });
      const admission =
        delayAboveMs === undefined
          ? undefined
          : yield* EffectAdmission.make({
              workers: concurrency,
              closeAbove: delayAboveMs,
              samplesToSwitch: EFFECT_ADMISSION_SAMPLES_TO_SWITCH,
              sampleInterval: EFFECT_ADMISSION_SAMPLE_INTERVAL,
              pressure: options.pressure ?? eventLoopDelaySource,
              onAllowanceRestored: worker.notifyWork(concurrency),
              onSlotFreed: worker.notifyWork(1),
            });
      const runOnce =
        admission === undefined
          ? worker.runOnce
          : worker.runOnce.pipe(Effect.provideService(EffectAdmission.EffectAdmission, admission));
      const requestedLivenessPollIntervalMs =
        options.livenessPollIntervalMs ?? DEFAULT_EFFECT_WORKER_LIVENESS_POLL_INTERVAL_MS;
      const livenessPollIntervalMs = Number.isFinite(requestedLivenessPollIntervalMs)
        ? Math.max(1, Math.floor(requestedLivenessPollIntervalMs))
        : DEFAULT_EFFECT_WORKER_LIVENESS_POLL_INTERVAL_MS;
      // Notifications wake promptly; availableAt persists retry timing, and liveness polling recovers missed wakeups.
      const runWorker = Effect.gen(function* () {
        while (true) {
          const outcome = yield* runOnce.pipe(
            Effect.map((worked) => (worked ? ("worked" as const) : ("idle" as const))),
            Effect.catchCause((cause) =>
              Effect.logWarning("Orchestration effect worker failed", cause).pipe(
                Effect.as("failed" as const),
              ),
            ),
          );
          if (outcome === "worked") {
            yield* Effect.yieldNow;
            continue;
          }
          if (outcome === "failed") {
            // Failed claims can leave overdue rows; cool off instead of rescheduling at the one-millisecond floor.
            yield* Effect.sleep(Duration.millis(Math.min(1_000, livenessPollIntervalMs)));
            continue;
          }

          // Skipped turn starts are not due work: a freed slot or the allowance's return wakes a
          // worker for them, so an idle worker otherwise sleeps the liveness interval.
          const nextClaimable =
            admission === undefined
              ? worker.nextClaimableAt
              : worker.nextClaimableAtExcluding(yield* admission.excludedTypes);
          const nextClaimableAt = yield* nextClaimable.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning(
                "Failed to read the next orchestration effect deadline",
                cause,
              ).pipe(Effect.as(Option.none<DateTime.Utc>())),
            ),
          );
          const now = DateTime.toEpochMillis(yield* DateTime.now);
          const sleepMs = Option.match(nextClaimableAt, {
            onNone: () => livenessPollIntervalMs,
            onSome: (availableAt) => {
              const untilAvailable = DateTime.toEpochMillis(availableAt) - now;
              return Math.min(livenessPollIntervalMs, untilAvailable > 0 ? untilAvailable : 25);
            },
          });
          yield* Effect.raceFirst(
            worker.awaitWork.pipe(Effect.as("notified" as const)),
            Effect.sleep(Duration.millis(sleepMs)).pipe(Effect.as("scheduled" as const)),
          );
        }
      });

      return yield* Effect.all(
        Array.from({ length: concurrency }, () => runWorker),
        {
          concurrency: "unbounded",
          discard: true,
        },
      );
    }),
  );

export const runDaemon = runDaemonWithOptions();

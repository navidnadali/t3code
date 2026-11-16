import {
  CommandId,
  type MessageId,
  type OrchestrationV2ThreadProjection,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import { makeKeyedReactor } from "./KeyedReactor.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import { isUndeliveredMailboxSteer } from "./NotificationMailbox.ts";

const CONTINUATION_MESSAGE_TEXT = "Background task completed.";
/** Delegation families whose provider wakes may dispatch at once. */
const WAKE_DISPATCH_CONCURRENCY = 16;
/** Completion deliveries dispatched at once, which paces the restart re-delivery wave. */
const COMPLETION_DISPATCH_CONCURRENCY = 16;
const FAMILY_KEY_CACHE_SIZE = 10_000;

function delegatedCompletionText(taskIds: ReadonlyArray<string>): string {
  const taskList = taskIds.join(", ");
  return taskIds.length === 1
    ? `Delegated task ${taskList} reached a terminal state. Use task_status with taskId ${taskList} to read the result.`
    : `Delegated tasks ${taskList} reached terminal states. Use task_status with each taskId to read the results.`;
}

function currentDelegatedCompletionDelivery(
  projection: Pick<OrchestrationV2ThreadProjection, "messages" | "runs" | "providerTurns">,
  completion: NonNullable<
    ProviderContinuationRequests.ProviderContinuationRequest["delegatedCompletion"]
  >,
) {
  const sourceRun = projection.runs.find((candidate) => candidate.id === completion.parentRunId);
  const delivery = sourceRun?.delegatedCompletion?.delivery;
  const alreadyDispatched = projection.messages.some(
    (message) => message.id === completion.messageId,
  );
  if (
    sourceRun?.delegatedCompletion?.disposition !== "open" ||
    delivery === null ||
    delivery === undefined ||
    delivery.generation !== completion.generation ||
    delivery.messageId !== completion.messageId ||
    (alreadyDispatched && !isUndeliveredMailboxSteer(projection, completion.messageId))
  ) {
    return undefined;
  }
  return delivery;
}

function delegatedCompletionRetryKey(
  request: ProviderContinuationRequests.ProviderContinuationRequest,
  completion: NonNullable<
    ProviderContinuationRequests.ProviderContinuationRequest["delegatedCompletion"]
  >,
): string {
  return `${request.threadId}:${completion.parentRunId}:${completion.generation}:${completion.messageId}`;
}

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const ids = yield* IdAllocator.IdAllocatorV2;
    const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const retryAttempts = yield* Ref.make(new Map<string, number>());

    const clearRetryAttempt = (key: string) =>
      Ref.update(retryAttempts, (current) => {
        if (!current.has(key)) return current;
        const updated = new Map(current);
        updated.delete(key);
        return updated;
      });

    const nextRetryDelay = (key: string) =>
      Ref.modify(retryAttempts, (current) => {
        const attempt = current.get(key) ?? 0;
        const updated = new Map(current);
        updated.set(key, attempt + 1);
        return [Math.min(100 * 2 ** Math.min(attempt, 6), 5_000), updated] as const;
      });

    // What currentDelegatedCompletionDelivery reads: the delivery's source run and message and, for
    // a message already dispatched, the run it joined and that run's provider turns.
    const readDeliveryState = Effect.fnUntraced(function* (
      request: ProviderContinuationRequests.ProviderContinuationRequest,
    ) {
      const completion = request.delegatedCompletion;
      const records = yield* threads.getThreadRecords(
        request.threadId,
        ["messages", "runs", "providerTurns"],
        {
          messageIds: completion === undefined ? [] : [completion.messageId],
          runIds: completion === undefined ? [] : [completion.parentRunId],
          providerTurnAttemptIds: [],
        },
      );
      const message = records.messages.find((candidate) => candidate.id === completion?.messageId);
      const joinedRunId = message?.delegatedCompletion === undefined ? null : message.runId;
      if (joinedRunId === null) return records;
      const joined =
        records.runs.find((run) => run.id === joinedRunId) ??
        (yield* threads.getThreadRecords(request.threadId, ["runs"], { runIds: [joinedRunId] }))
          .runs[0];
      if (joined === undefined) return records;
      const { providerTurns } = yield* threads.getThreadRecords(
        request.threadId,
        ["providerTurns"],
        { providerTurnAttemptIds: joined.activeAttemptId === null ? [] : [joined.activeAttemptId] },
      );
      return {
        ...records,
        runs: records.runs.some((run) => run.id === joined.id)
          ? records.runs
          : [...records.runs, joined],
        providerTurns,
      };
    });

    // Cancelling or stopping a queued continuation ends it unstarted; only the request can release the adapter's wake latch.
    const releaseIfNeverStarted = Effect.fn("ProviderContinuationService.releaseIfNeverStarted")(
      function* (
        request: ProviderContinuationRequests.ProviderContinuationRequest,
        messageId: MessageId,
        dispatched: Orchestrator.OrchestratorV2DispatchResult,
      ) {
        const onRunEndedWithoutStarting = request.onRunEndedWithoutStarting;
        if (onRunEndedWithoutStarting === undefined) return;
        const runId = dispatched.storedEvents.flatMap(({ event }) =>
          event.type === "run.created" && event.payload.userMessageId === messageId
            ? [event.payload.id]
            : [],
        )[0];
        if (runId === undefined) return;
        yield* threads
          .streamStoredEventsFrom({
            threadId: request.threadId,
            afterSequence: dispatched.sequence,
            eventType: "run.updated",
          })
          .pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.id === runId &&
                ThreadManagementService.isTerminalRunStatus(event.payload.status),
            ),
            Stream.runHead,
            Effect.flatMap((ended) =>
              Option.isSome(ended) &&
              ended.value.event.type === "run.updated" &&
              ended.value.event.payload.startedAt === null
                ? onRunEndedWithoutStarting()
                : Effect.void,
            ),
            Effect.catchCause((cause) =>
              Effect.logWarning("orchestration-v2.provider-continuation.run-watch-failed", {
                threadId: request.threadId,
                providerThreadId: request.providerThreadId,
                cause,
              }),
            ),
            Effect.forkScoped,
          );
      },
    );

    const dispatchContinuation = Effect.fn("ProviderContinuationService.dispatchContinuation")(
      function* (request: ProviderContinuationRequests.ProviderContinuationRequest) {
        const projection = yield* readDeliveryState(request);
        if (projection.thread.archivedAt !== null || projection.thread.deletedAt !== null) {
          yield* Effect.logInfo("orchestration-v2.provider-continuation.thread-archived", {
            threadId: request.threadId,
            providerThreadId: request.providerThreadId,
          });
          if (request.delegatedCompletion !== undefined) {
            yield* clearRetryAttempt(
              delegatedCompletionRetryKey(request, request.delegatedCompletion),
            );
          }
          if (request.clearIfCurrent !== undefined) {
            yield* request.clearIfCurrent();
          } else if (request.dispatchIfCurrent !== undefined) {
            yield* request.dispatchIfCurrent(Effect.void);
          }
          return;
        }
        if (request.delegatedCompletion !== undefined) {
          const retryKey = delegatedCompletionRetryKey(request, request.delegatedCompletion);
          const delivery = currentDelegatedCompletionDelivery(
            projection,
            request.delegatedCompletion,
          );
          if (delivery === undefined) {
            yield* clearRetryAttempt(retryKey);
            return;
          }
          const commandId = yield* ids.allocate.command({
            fixtureName: "delegated-completion",
            commandName: "dispatch",
          });
          yield* threads
            .dispatch({
              type: "message.dispatch",
              commandId,
              threadId: request.threadId,
              messageId: delivery.messageId,
              text: delegatedCompletionText(delivery.taskIds),
              attachments: [],
              dispatchMode: { type: "queue_after_active" },
              createdBy: "agent",
              creationSource: "server",
              delegatedCompletion: {
                parentRunId: request.delegatedCompletion.parentRunId,
                generation: delivery.generation,
                taskIds: delivery.taskIds,
              },
            })
            .pipe(
              Effect.catchTags({
                ThreadManagementProviderInstanceDisabledError: (error) =>
                  Effect.logInfo(
                    "orchestration-v2.provider-continuation.delegated-completion-disabled",
                    {
                      threadId: request.threadId,
                      projectId: error.projectId,
                      instanceId: error.instanceId,
                    },
                  ).pipe(
                    Effect.andThen(
                      Effect.forEach(
                        delivery.taskIds,
                        (taskId) =>
                          ids.allocate
                            .command({
                              fixtureName: "delegated-completion",
                              commandName: "dispose-disabled",
                            })
                            .pipe(
                              Effect.flatMap((disposeCommandId) =>
                                threads.dispatch({
                                  type: "delegated_task.completion-delivery.dispose",
                                  commandId: disposeCommandId,
                                  parentThreadId: request.threadId,
                                  taskId,
                                }),
                              ),
                            ),
                        { discard: true },
                      ),
                    ),
                    Effect.andThen(
                      request.clearIfCurrent !== undefined
                        ? request.clearIfCurrent()
                        : request.dispatchIfCurrent !== undefined
                          ? request.dispatchIfCurrent(Effect.void)
                          : Effect.void,
                    ),
                  ),
              }),
            );
          yield* clearRetryAttempt(retryKey);
          return;
        }
        const messageId =
          request.messageId ??
          (yield* ids.allocate.message({
            threadId: request.threadId,
            ordinal: projection.messages.length + 1,
          }));
        const commandId = CommandId.make(`provider-continuation:${messageId}`);
        const dispatch = threads
          .dispatch({
            type: "message.dispatch",
            commandId,
            threadId: request.threadId,
            messageId,
            text: request.detail ?? CONTINUATION_MESSAGE_TEXT,
            notification: request.notification ?? {
              source: { kind: "background_task" },
              outcome: "updated",
              summary: "Background activity updated",
            },
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            // Only buffered wakes use creationSource "provider"; Claude would drain an empty buffer instead of prompting message_text.
            creationSource: request.delivery === "message_text" ? "server" : "provider",
          })
          .pipe(Effect.tap((dispatched) => releaseIfNeverStarted(request, messageId, dispatched)));
        // Catch after dispatchIfCurrent so adapters see raw failures first; clearIfCurrent is idempotent and covers others.
        const logInstanceDisabled = (
          error: ThreadManagementService.ThreadManagementProviderInstanceDisabledError,
        ) =>
          Effect.logInfo("orchestration-v2.provider-continuation.instance-disabled", {
            threadId: request.threadId,
            providerThreadId: request.providerThreadId,
            projectId: error.projectId,
            instanceId: error.instanceId,
          }).pipe(
            Effect.andThen(
              request.clearIfCurrent !== undefined ? request.clearIfCurrent() : Effect.void,
            ),
          );
        if (request.dispatchIfCurrent === undefined) {
          yield* dispatch.pipe(
            Effect.catchTags({
              ThreadManagementProviderInstanceDisabledError: logInstanceDisabled,
            }),
          );
          return;
        }
        yield* request.dispatchIfCurrent(dispatch).pipe(
          Effect.catchTags({
            ThreadManagementProviderInstanceDisabledError: logInstanceDisabled,
          }),
        );
      },
    );

    // A wake to a delegated child takes its parent's lock first, so it keys on the parent, as the
    // run-end reactor does. A completion takes only its own thread's lock and keys on that thread.
    const familyKeys = new Map<ThreadId, ThreadId>();
    const wakeFamily = (threadId: ThreadId) => {
      const known = familyKeys.get(threadId);
      if (known !== undefined) return Effect.succeed(known);
      return threads.getThreadRecords(threadId, []).pipe(
        Effect.map(({ thread }) => Orchestrator.appOwnedSubagentParentOf(thread) ?? threadId),
        Effect.tap((family) =>
          Effect.sync(() => {
            if (familyKeys.size >= FAMILY_KEY_CACHE_SIZE) {
              familyKeys.delete(familyKeys.keys().next().value!);
            }
            familyKeys.set(threadId, family);
          }),
        ),
        // The key only orders dispatches; a thread whose lineage can't be read keys on itself.
        Effect.catchCause(() => Effect.succeed(threadId)),
      );
    };

    const scope = yield* Effect.scope;
    const handle = (request: ProviderContinuationRequests.ProviderContinuationRequest) =>
      dispatchContinuation(request).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            yield* Effect.logWarning("orchestration-v2.provider-continuation.dispatch-failed", {
              threadId: request.threadId,
              providerThreadId: request.providerThreadId,
              cause,
            });
            if (request.delegatedCompletion !== undefined) {
              const completion = request.delegatedCompletion;
              const retryKey = delegatedCompletionRetryKey(request, completion);
              const retryDelay = yield* nextRetryDelay(retryKey);
              yield* Effect.gen(function* () {
                yield* Effect.sleep(`${retryDelay} millis`);
                const projection = yield* readDeliveryState(request);
                if (currentDelegatedCompletionDelivery(projection, completion) !== undefined) {
                  yield* requests.offer(request);
                } else {
                  yield* clearRetryAttempt(retryKey);
                }
              }).pipe(
                Effect.catchCause((retryCause) =>
                  Effect.logWarning("orchestration-v2.provider-continuation.retry-check-failed", {
                    threadId: request.threadId,
                    providerThreadId: request.providerThreadId,
                    cause: retryCause,
                  }).pipe(Effect.andThen(requests.offer(request))),
                ),
                Effect.forkScoped,
              );
            } else if (request.clearIfCurrent !== undefined) {
              yield* request.clearIfCurrent();
            }
          }),
        ),
        // MCP calls from a wake turn wait for this; a retry is offered, and settled, anew.
        Effect.ensuring(requests.settle(request.threadId)),
        Scope.provide(scope),
      );
    // A dispatch can wait minutes for a lock, so each key dispatches in order, many keys at once.
    // Wakes and completions have separate pools: a restart's re-delivery wave or a few blocked
    // completions never hold up a wake.
    const wakes = yield* makeKeyedReactor<
      ThreadId,
      ProviderContinuationRequests.ProviderContinuationRequest
    >({ concurrency: WAKE_DISPATCH_CONCURRENCY, handle });
    const completions = yield* makeKeyedReactor<
      ThreadId,
      ProviderContinuationRequests.ProviderContinuationRequest
    >({ concurrency: COMPLETION_DISPATCH_CONCURRENCY, handle });

    yield* requests.take.pipe(
      Effect.flatMap((request) =>
        request.delegatedCompletion === undefined
          ? wakeFamily(request.threadId).pipe(
              Effect.flatMap((family) => wakes.offer(family, request)),
            )
          : completions.offer(request.threadId, request),
      ),
      Effect.forever,
      Effect.forkScoped,
    );
  }),
);

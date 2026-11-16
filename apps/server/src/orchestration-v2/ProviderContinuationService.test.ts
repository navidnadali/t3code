import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2Notification,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as TestClock from "effect/testing/TestClock";

import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as Orchestrator from "./Orchestrator.ts";
import type { ProjectionRecordFilter } from "./ProjectionStore.ts";
import * as ProviderContinuationService from "./ProviderContinuationService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

const threadId = ThreadId.make("thread-provider-continuation");
const providerThreadId = ProviderThreadId.make("provider-thread-continuation");
const driver = ProviderDriverKind.make("continuation-test");
const parentRunId = RunId.make("run-provider-continuation-parent");
const delegatedMessageId = MessageId.make("message-provider-continuation-delegated");
const projection = {
  thread: {
    archivedAt: null,
    deletedAt: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
  },
  messages: [],
} as unknown as OrchestrationV2ThreadProjection;

const request = (
  dispatchIfCurrent?: ProviderContinuationRequests.ProviderContinuationRequest["dispatchIfCurrent"],
  detail: string | null = null,
): ProviderContinuationRequests.ProviderContinuationRequest => ({
  threadId,
  providerThreadId,
  driver,
  detail,
  ...(dispatchIfCurrent === undefined ? {} : { dispatchIfCurrent }),
});

const delegatedProjection = (disposition: "open" | "stopped" | "disposed" = "open") =>
  ({
    ...projection,
    runs: [
      {
        id: parentRunId,
        delegatedCompletion: {
          disposition,
          nextGeneration: 2,
          delivery: {
            generation: 1,
            messageId: delegatedMessageId,
            taskIds: ["node-provider-continuation-first", "node-provider-continuation-second"],
          },
        },
      },
    ],
  }) as unknown as OrchestrationV2ThreadProjection;

const makeGuard = Effect.fnUntraced(function* (completed?: Deferred.Deferred<void>) {
  const generation = yield* Ref.make(0);
  const permit = yield* Semaphore.make(1);
  const capture = Effect.gen(function* () {
    const captured = yield* Ref.updateAndGet(generation, (value) => value + 1);
    return <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      permit
        .withPermit(
          Effect.gen(function* () {
            if ((yield* Ref.get(generation)) !== captured) return Option.none();
            return Option.some(yield* effect);
          }),
        )
        .pipe(
          completed === undefined
            ? (effect) => effect
            : Effect.ensuring(Deferred.succeed(completed, undefined)),
        );
  });
  return {
    capture,
    invalidate: permit.withPermit(Ref.update(generation, (value) => value + 1)),
  };
});

function layerTest(input: {
  readonly dispatched: Queue.Queue<unknown>;
  readonly getThreadRecords: () => Effect.Effect<OrchestrationV2ThreadProjection>;
}) {
  const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
    // A wake's dispatch key reads only the thread's lineage; the tests' reads are the deliveries'.
    getThreadRecords: ((_threadId: ThreadId, fields: ReadonlyArray<string>) =>
      fields.length === 0 ? Effect.succeed(projection) : input.getThreadRecords()) as never,
    dispatch: (command) => Queue.offer(input.dispatched, command).pipe(Effect.as({} as never)),
  });
  const layerWorker = ProviderContinuationService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
    ),
  );
  return Layer.merge(ProviderContinuationRequests.layer, layerWorker);
}

describe("ProviderContinuationService", () => {
  it.effect("releases run checks waiting on a continuation whose dispatch fails", () =>
    Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer(request());
        yield* requests.awaitSettle(0);
        assert.equal(yield* Queue.size(dispatched), 0);
      }).pipe(
        Effect.provide(
          layerTest({ dispatched, getThreadRecords: () => Effect.die("projection unavailable") }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("settles each request once in its own pool, wakes and completions alike", () =>
    Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer(request());
        yield* requests.offer({
          ...request(),
          delegatedCompletion: { parentRunId, generation: 1, messageId: delegatedMessageId },
        });
        assert.equal((yield* requests.progress(threadId)).pending, 2);
        yield* requests.awaitSettle(1);
        assert.deepEqual(yield* requests.progress(threadId), { pending: 0, settles: 2 });
      }).pipe(
        Effect.provide(
          layerTest({ dispatched, getThreadRecords: () => Effect.die("projection unavailable") }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect("recovers an unaccepted persisted steer using the same delivery identity", () =>
    Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      const original = delegatedProjection();
      const interruptedRunId = RunId.make("interrupted-mailbox-run");
      const recovered = {
        ...original,
        runs: [
          ...original.runs,
          {
            id: interruptedRunId,
            status: "interrupted",
            userMessageId: MessageId.make("original-user-input"),
          },
        ],
        messages: [
          {
            id: delegatedMessageId,
            runId: interruptedRunId,
            delegatedCompletion: { parentRunId, generation: 1, taskIds: [] },
          },
        ],
      } as unknown as OrchestrationV2ThreadProjection;
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer({
          threadId,
          providerThreadId,
          driver,
          detail: null,
          delivery: "message_text",
          delegatedCompletion: { parentRunId, generation: 1, messageId: delegatedMessageId },
        });
        const command = (yield* Queue.take(dispatched)) as {
          messageId: MessageId;
          delegatedCompletion: { generation: number; taskIds: readonly string[] };
        };
        assert.equal(command.messageId, delegatedMessageId);
        assert.equal(command.delegatedCompletion.generation, 1);
        assert.equal(command.delegatedCompletion.taskIds.length, 2);
      }).pipe(
        Effect.provide(
          layerTest({ dispatched, getThreadRecords: () => Effect.succeed(recovered) }),
        ),
        Effect.scoped,
      );
    }),
  );

  it.effect(
    "reads a delivery's runs and provider turns by id, not every run and turn of the thread",
    () =>
      Effect.gen(function* () {
        const dispatched = yield* Queue.unbounded<unknown>();
        const original = delegatedProjection();
        const joinedRunId = RunId.make("joined-mailbox-run");
        const joinedAttemptId = RunAttemptId.make("joined-mailbox-attempt");
        // A steer joined a run whose turn then completed, so the delivery goes out again.
        const thread = {
          ...original,
          runs: [
            ...original.runs,
            ...Array.from({ length: 20 }, (_, index) => ({
              id: RunId.make(`history-run-${index}`),
              status: "completed",
              userMessageId: MessageId.make(`history-input-${index}`),
              activeAttemptId: RunAttemptId.make(`history-attempt-${index}`),
            })),
            {
              id: joinedRunId,
              status: "running",
              userMessageId: MessageId.make("joined-input"),
              activeAttemptId: joinedAttemptId,
            },
          ],
          providerTurns: [
            ...Array.from({ length: 20 }, (_, index) => ({
              runAttemptId: RunAttemptId.make(`history-attempt-${index}`),
              ordinal: index,
              status: "completed",
            })),
            { runAttemptId: joinedAttemptId, ordinal: 30, status: "completed" },
          ],
          messages: [
            {
              id: delegatedMessageId,
              runId: joinedRunId,
              delegatedCompletion: { parentRunId, generation: 1, taskIds: [] },
            },
          ],
        } as unknown as OrchestrationV2ThreadProjection;
        const reads: Array<{
          readonly fields: ReadonlyArray<string>;
          readonly filter: ProjectionRecordFilter | undefined;
        }> = [];
        // The store's id filters; an absent one keeps every row.
        const keep = <A>(
          rows: ReadonlyArray<A>,
          ids: ReadonlyArray<unknown> | undefined,
          id: (row: A) => unknown,
        ) => (ids === undefined ? rows : rows.filter((row) => ids.includes(id(row))));
        const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: ((
            _threadId: ThreadId,
            fields: ReadonlyArray<string>,
            filter?: ProjectionRecordFilter,
          ) => {
            reads.push({ fields, filter });
            return Effect.succeed({
              ...thread,
              messages: keep(thread.messages, filter?.messageIds, (row) => row.id),
              runs: keep(thread.runs, filter?.runIds, (row) => row.id),
              providerTurns: keep(
                thread.providerTurns,
                filter?.providerTurnAttemptIds,
                (row) => row.runAttemptId,
              ),
            });
          }) as never,
          dispatch: (command) => Queue.offer(dispatched, command).pipe(Effect.as({} as never)),
        });
        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* requests.offer({
            threadId,
            providerThreadId,
            driver,
            detail: null,
            delivery: "message_text",
            delegatedCompletion: { parentRunId, generation: 1, messageId: delegatedMessageId },
          });
          const command = (yield* Queue.take(dispatched)) as { readonly messageId: MessageId };
          assert.equal(command.messageId, delegatedMessageId);
        }).pipe(
          Effect.provide(
            Layer.merge(
              ProviderContinuationRequests.layer,
              ProviderContinuationService.layer.pipe(
                Layer.provide(
                  Layer.mergeAll(
                    IdAllocator.layer,
                    ProviderContinuationRequests.layer,
                    layerThreads,
                  ),
                ),
              ),
            ),
          ),
          Effect.scoped,
        );
        assert.isNotEmpty(reads);
        assert.deepEqual(
          reads.flatMap(({ fields, filter }) => [
            ...(fields.includes("runs") && filter?.runIds === undefined ? ["runs"] : []),
            ...(fields.includes("providerTurns") && filter?.providerTurnAttemptIds === undefined
              ? ["providerTurns"]
              : []),
          ]),
          [],
          "a continuation read takes every run or turn of the thread",
        );
      }),
  );

  it.effect("delivers a message_text wake as a real prompt, not a buffered wake", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer({
          threadId,
          providerThreadId,
          driver,
          detail: "Delegated task completed.",
          notification: {
            source: { kind: "monitor" },
            outcome: "updated",
            summary: "Monitor updated",
          },
          delivery: "message_text",
        });
        const command = (yield* Queue.take(dispatched)) as {
          readonly creationSource: string;
          readonly text: string;
          readonly notification: OrchestrationV2Notification;
        };
        assert.notEqual(command.creationSource, "provider");
        assert.equal(command.creationSource, "server");
        assert.equal(command.text, "Delegated task completed.");
        assert.deepEqual(command.notification, {
          source: { kind: "monitor" },
          outcome: "updated",
          summary: "Monitor updated",
        });
      }).pipe(
        Effect.provide(
          layerTest({ dispatched, getThreadRecords: () => Effect.succeed(projection) }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect("dispatches a current delegated completion as one server-owned queued message", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer({
          threadId,
          providerThreadId,
          driver,
          detail: null,
          delivery: "message_text",
          delegatedCompletion: {
            parentRunId,
            generation: 1,
            messageId: delegatedMessageId,
          },
        });
        const command = (yield* Queue.take(dispatched)) as {
          readonly createdBy: string;
          readonly creationSource: string;
          readonly delegatedCompletion: {
            readonly generation: number;
            readonly parentRunId: RunId;
            readonly taskIds: ReadonlyArray<string>;
          };
          readonly dispatchMode: { readonly type: string };
          readonly messageId: MessageId;
          readonly text: string;
        };
        assert.equal(command.createdBy, "agent");
        assert.equal(command.creationSource, "server");
        assert.deepEqual(command.dispatchMode, { type: "queue_after_active" });
        assert.equal(command.messageId, delegatedMessageId);
        assert.equal(command.delegatedCompletion.parentRunId, parentRunId);
        assert.equal(command.delegatedCompletion.generation, 1);
        assert.deepEqual(command.delegatedCompletion.taskIds, [
          "node-provider-continuation-first",
          "node-provider-continuation-second",
        ]);
        assert.include(command.text, "task_status");
      }).pipe(
        Effect.provide(
          layerTest({
            dispatched,
            getThreadRecords: () => Effect.succeed(delegatedProjection()),
          }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect(
    "a project-disabled instance refuses a delegated-completion wake exactly once, disposes it, and never retries",
    () => {
      return Effect.gen(function* () {
        const attempts = yield* Ref.make(0);
        const cleared = yield* Ref.make(false);
        const disposed = yield* Queue.unbounded<unknown>();
        const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () => Effect.succeed(delegatedProjection()),
          dispatch: (command) => {
            if ((command as { type: string }).type === "message.dispatch") {
              return Ref.update(attempts, (count) => count + 1).pipe(
                Effect.andThen(
                  Effect.fail(
                    new ThreadManagementService.ThreadManagementProviderInstanceDisabledError({
                      projectId: ProjectId.make("project-provider-continuation-disabled"),
                      instanceId: ProviderInstanceId.make(
                        "instance-provider-continuation-disabled",
                      ),
                    }) as never,
                  ),
                ),
              );
            }
            return Queue.offer(disposed, command).pipe(Effect.as({} as never));
          },
        });
        const layerWorker = ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
          ),
        );

        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* requests.offer({
            threadId,
            providerThreadId,
            driver,
            detail: null,
            delivery: "message_text",
            clearIfCurrent: () => Ref.set(cleared, true),
            delegatedCompletion: {
              parentRunId,
              generation: 1,
              messageId: delegatedMessageId,
            },
          });
          yield* Effect.yieldNow;
          assert.equal(yield* Ref.get(attempts), 1);
          assert.isTrue(yield* Ref.get(cleared));

          const dispositions = (yield* Queue.takeAll(disposed)) as ReadonlyArray<{
            type: string;
            taskId: string;
          }>;
          assert.deepEqual(
            dispositions.map((command) => command.taskId),
            ["node-provider-continuation-first", "node-provider-continuation-second"],
          );
          for (const command of dispositions) {
            assert.equal(command.type, "delegated_task.completion-delivery.dispose");
          }

          yield* TestClock.adjust("1 hour");
          yield* Effect.yieldNow;
          assert.equal(yield* Ref.get(attempts), 1);
          assert.equal(yield* Queue.size(disposed), 0);
        }).pipe(
          Effect.provide(Layer.merge(ProviderContinuationRequests.layer, layerWorker)),
          Effect.scoped,
        );
      });
    },
  );

  it.effect(
    "clears the adapter's sticky continuation flag when a non-delegated wake is refused",
    () => {
      return Effect.gen(function* () {
        const requested = yield* Ref.make(true);
        const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () => Effect.succeed(projection),
          dispatch: () =>
            Effect.fail(
              new ThreadManagementService.ThreadManagementProviderInstanceDisabledError({
                projectId: ProjectId.make("project-provider-continuation-disabled"),
                instanceId: ProviderInstanceId.make("instance-provider-continuation-disabled"),
              }) as never,
            ),
        });
        const layerWorker = ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
          ),
        );

        // ACP clears its sticky continuation flag on raw dispatch failure; pre-recovery would prevent that cleanup.
        const dispatchIfCurrent = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          Effect.gen(function* () {
            const exit = yield* Effect.exit(effect);
            if (Exit.isFailure(exit)) {
              yield* Ref.set(requested, false);
              return yield* Effect.failCause(exit.cause);
            }
            return Option.some(exit.value);
          });

        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* requests.offer(request(dispatchIfCurrent));
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;
          assert.isFalse(
            yield* Ref.get(requested),
            "a disabled instance must still clear the adapter's sticky offer flag",
          );
        }).pipe(
          Effect.provide(Layer.merge(ProviderContinuationRequests.layer, layerWorker)),
          Effect.scoped,
        );
      });
    },
  );

  it.effect(
    "clears a sticky continuation offer for an adapter whose dispatchIfCurrent does not self-clear on failure",
    () => {
      return Effect.gen(function* () {
        const wakeHeld = yield* Ref.make(true);
        const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () => Effect.succeed(projection),
          dispatch: () =>
            Effect.fail(
              new ThreadManagementService.ThreadManagementProviderInstanceDisabledError({
                projectId: ProjectId.make("project-provider-continuation-disabled"),
                instanceId: ProviderInstanceId.make("instance-provider-continuation-disabled"),
              }) as never,
            ),
        });
        const layerWorker = ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
          ),
        );

        // Muse forwards dispatch failures unchanged; clearIfCurrent must release its sticky wake flag.
        const dispatchIfCurrent = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          Effect.asSome(effect);

        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* requests.offer({
            ...request(dispatchIfCurrent),
            clearIfCurrent: () => Ref.set(wakeHeld, false),
          });
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;
          assert.isFalse(
            yield* Ref.get(wakeHeld),
            "a disabled instance must clear the offer via the explicit callback " +
              "when the adapter itself does not clear on dispatch failure",
          );
        }).pipe(
          Effect.provide(Layer.merge(ProviderContinuationRequests.layer, layerWorker)),
          Effect.scoped,
        );
      });
    },
  );

  it.effect(
    "clears a sticky continuation offer when a non-delegated wake dispatch fails generically",
    () => {
      return Effect.gen(function* () {
        const cleared = yield* Deferred.make<void>();
        const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
          getThreadRecords: () => Effect.succeed(projection),
          dispatch: (command) =>
            Effect.fail(
              new Orchestrator.OrchestratorDispatchError({
                commandId: CommandId.make(String(command.commandId)),
                commandType: command.type,
                cause: new Error("SQLITE_FULL: database or disk is full"),
              }),
            ),
        });
        const layerWorker = ProviderContinuationService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
          ),
        );

        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* requests.offer({
            ...request(),
            clearIfCurrent: () => Deferred.succeed(cleared, undefined),
          });
          yield* Deferred.await(cleared);
        }).pipe(
          Effect.provide(Layer.merge(ProviderContinuationRequests.layer, layerWorker)),
          Effect.scoped,
        );
      });
    },
  );

  it.effect(
    "drops stopped and disposed delegated completions instead of reviving them after recovery",
    () => {
      return Effect.gen(function* () {
        for (const disposition of ["stopped", "disposed"] as const) {
          const dispatched = yield* Queue.unbounded<unknown>();
          yield* Effect.gen(function* () {
            const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
            yield* requests.offer({
              threadId,
              providerThreadId,
              driver,
              detail: null,
              delivery: "message_text",
              delegatedCompletion: {
                parentRunId,
                generation: 1,
                messageId: delegatedMessageId,
              },
            });
            yield* Effect.yieldNow;
            yield* Effect.yieldNow;
            assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
          }).pipe(
            Effect.provide(
              layerTest({
                dispatched,
                getThreadRecords: () => Effect.succeed(delegatedProjection(disposition)),
              }),
            ),
            Effect.scoped,
          );
        }
      });
    },
  );

  it.effect("does not redispatch a persisted delegated completion message during recovery", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer({
          threadId,
          providerThreadId,
          driver,
          detail: null,
          delivery: "message_text",
          delegatedCompletion: {
            parentRunId,
            generation: 1,
            messageId: delegatedMessageId,
          },
        });
        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
        assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
      }).pipe(
        Effect.provide(
          layerTest({
            dispatched,
            getThreadRecords: () =>
              Effect.succeed({
                ...delegatedProjection(),
                messages: [{ id: delegatedMessageId }],
              } as unknown as OrchestrationV2ThreadProjection),
          }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect("dispatches a current request exactly once", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        yield* requests.offer(request());
        yield* Queue.take(dispatched);
        yield* Effect.yieldNow;
        assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
      }).pipe(
        Effect.provide(
          layerTest({ dispatched, getThreadRecords: () => Effect.succeed(projection) }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect("drops a request invalidated while projection is blocked", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      const projectionEntered = yield* Deferred.make<void>();
      const releaseProjection = yield* Deferred.make<void>();
      const guardCompleted = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        const guard = yield* makeGuard(guardCompleted);
        const dispatchIfCurrent = yield* guard.capture;
        yield* requests.offer(request(dispatchIfCurrent));
        yield* Deferred.await(projectionEntered);
        yield* guard.invalidate;
        yield* Deferred.succeed(releaseProjection, undefined);
        yield* Deferred.await(guardCompleted);
        assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
      }).pipe(
        Effect.provide(
          layerTest({
            dispatched,
            getThreadRecords: () =>
              Deferred.succeed(projectionEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseProjection)),
                Effect.as(projection),
              ),
          }),
        ),
        Effect.scoped,
      );
    });
  });

  it.effect("does not revive an old request when a later generation is current", () => {
    return Effect.gen(function* () {
      const dispatched = yield* Queue.unbounded<unknown>();
      const firstProjectionEntered = yield* Deferred.make<void>();
      const releaseFirstProjection = yield* Deferred.make<void>();
      let projectionCalls = 0;
      yield* Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        const guard = yield* makeGuard();
        const first = yield* guard.capture;
        yield* requests.offer(request(first, "A"));
        yield* Deferred.await(firstProjectionEntered);
        const second = yield* guard.capture;
        yield* requests.offer(request(second, "B"));
        yield* Deferred.succeed(releaseFirstProjection, undefined);
        const command = yield* Queue.take(dispatched);
        assert.equal((command as { readonly text?: unknown }).text, "B");
        yield* Effect.yieldNow;
        assert.isTrue(Option.isNone(yield* Queue.poll(dispatched)));
      }).pipe(
        Effect.provide(
          layerTest({
            dispatched,
            getThreadRecords: () => {
              projectionCalls += 1;
              return projectionCalls === 1
                ? Deferred.succeed(firstProjectionEntered, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseFirstProjection)),
                    Effect.as(projection),
                  )
                : Effect.succeed(projection);
            },
          }),
        ),
        Effect.scoped,
      );
    });
  });
});

// A fleet of threads whose dispatches take the locks Orchestrator's dispatch takes: a completion
// its own thread's, a wake to a delegated child its parent's first.
interface FleetEntry {
  readonly kind: "wake" | "completion";
  readonly threadId: ThreadId;
  readonly label: string;
}

const fleetThread = (name: string) => ThreadId.make(`thread-fleet-${name}`);
const fleetRunPrefix = "run-fleet-";
const fleetMessagePrefix = "message-fleet-";

const fleetWake = (
  child: string,
  label: string = child,
  extra: Partial<ProviderContinuationRequests.ProviderContinuationRequest> = {},
): ProviderContinuationRequests.ProviderContinuationRequest => ({
  threadId: fleetThread(child),
  providerThreadId,
  driver,
  detail: label,
  ...extra,
});

const fleetCompletion = (
  thread: string,
  label: string,
): ProviderContinuationRequests.ProviderContinuationRequest => ({
  threadId: fleetThread(thread),
  providerThreadId,
  driver,
  detail: null,
  delivery: "message_text",
  delegatedCompletion: {
    parentRunId: RunId.make(`${fleetRunPrefix}${label}`),
    generation: 1,
    messageId: MessageId.make(`${fleetMessagePrefix}${label}`),
  },
});

function makeFleet(input: {
  /** Child name to parent name. */
  readonly parents?: Readonly<Record<string, string>>;
  /** How long a dispatch takes once it holds its locks. */
  readonly cost?: Duration.Input;
  readonly outcome?: (entry: FleetEntry, attempt: number) => "ok" | "fail" | "die";
}) {
  const parents = new Map(
    Object.entries(input.parents ?? {}).map(([child, parent]) => [
      fleetThread(child),
      fleetThread(parent),
    ]),
  );
  const locks = new Map<ThreadId, Semaphore.Semaphore>();
  const lockOf = (threadId: ThreadId) => {
    const known = locks.get(threadId);
    if (known !== undefined) return known;
    const lock = Semaphore.makeUnsafe(1);
    locks.set(threadId, lock);
    return lock;
  };
  const state = {
    entered: [] as Array<FleetEntry>,
    landed: [] as Array<FleetEntry>,
    inFlight: { wake: 0, completion: 0 },
    peak: { wake: 0, completion: 0 },
    attempts: new Map<string, number>(),
  };
  const landedLabels = (kind?: FleetEntry["kind"]) =>
    state.landed.filter((entry) => kind === undefined || entry.kind === kind).map((e) => e.label);
  const thread = (threadId: ThreadId) => {
    const parent = parents.get(threadId);
    return {
      archivedAt: null,
      deletedAt: null,
      lineage:
        parent === undefined
          ? { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId }
          : { parentThreadId: parent, relationshipToParent: "subagent", rootThreadId: parent },
      forkedFrom: parent === undefined ? null : { type: "node", nodeId: `node-${threadId}` },
    };
  };
  const layerThreads = Layer.mock(ThreadManagementService.ThreadManagementService)({
    getThreadRecords: ((threadId: ThreadId, _fields: unknown, filter?: ProjectionRecordFilter) =>
      Effect.sync(() => ({
        thread: thread(threadId),
        // A delivered completion is no longer current, so a retry of it is dropped.
        messages: (filter?.messageIds ?? [])
          .filter((id) =>
            state.landed.some(
              (entry) =>
                entry.kind === "completion" && `${fleetMessagePrefix}${entry.label}` === id,
            ),
          )
          .map((id) => ({ id })),
        runs: (filter?.runIds ?? []).map((id) => ({
          id,
          delegatedCompletion: {
            disposition: "open",
            nextGeneration: 2,
            delivery: {
              generation: 1,
              messageId: MessageId.make(
                `${fleetMessagePrefix}${String(id).slice(fleetRunPrefix.length)}`,
              ),
              taskIds: [`task-${String(id)}`],
            },
          },
        })),
        providerTurns: [],
      }))) as never,
    dispatch: (command) => {
      const message = command as unknown as {
        readonly threadId: ThreadId;
        readonly messageId: MessageId;
        readonly text: string;
        readonly delegatedCompletion?: unknown;
      };
      const entry: FleetEntry =
        message.delegatedCompletion === undefined
          ? { kind: "wake", threadId: message.threadId, label: message.text }
          : {
              kind: "completion",
              threadId: message.threadId,
              label: String(message.messageId).slice(fleetMessagePrefix.length),
            };
      const body = Effect.gen(function* () {
        const attempt = (state.attempts.get(entry.label) ?? 0) + 1;
        state.attempts.set(entry.label, attempt);
        if (input.cost !== undefined) yield* Effect.sleep(input.cost);
        const outcome = input.outcome?.(entry, attempt) ?? "ok";
        if (outcome === "fail") {
          return yield* new Orchestrator.OrchestratorDispatchError({
            commandId: CommandId.make(String(command.commandId)),
            commandType: command.type,
            cause: new Error("dispatch failed"),
          });
        }
        if (outcome === "die") return yield* Effect.die(new Error("dispatch died"));
        state.landed.push(entry);
        return {} as never;
      });
      const parent = entry.kind === "wake" ? parents.get(entry.threadId) : undefined;
      const own = lockOf(entry.threadId).withPermit(body);
      return Effect.sync(() => {
        state.entered.push(entry);
        state.inFlight[entry.kind] += 1;
        state.peak[entry.kind] = Math.max(state.peak[entry.kind], state.inFlight[entry.kind]);
      }).pipe(
        Effect.andThen(parent === undefined ? own : lockOf(parent).withPermit(own)),
        Effect.ensuring(Effect.sync(() => (state.inFlight[entry.kind] -= 1))),
      );
    },
  });
  const layer = Layer.merge(
    ProviderContinuationRequests.layer,
    ProviderContinuationService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(IdAllocator.layer, ProviderContinuationRequests.layer, layerThreads),
      ),
    ),
  );
  return { layer, lockOf, state, landedLabels };
}

// Runs `effect` from a test-clock wake-up, so adjusting the clock returns only once everything it
// set off has run as far as it can: one tick.
const inOneTick = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(Effect.delay(effect, "1 millis"));
    yield* TestClock.adjust("1 millis");
    return yield* Fiber.join(fiber);
  });

describe("ProviderContinuationService dispatch per key", () => {
  it.effect(
    "dispatches a wake to another parent's child within one tick while a parent's lock is held with 50 completions queued for it",
    () => {
      const fleet = makeFleet({ parents: { seat: "q" } });
      return Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        const held = fleet.lockOf(fleetThread("p"));
        yield* held.take(1);
        const completions = Array.from({ length: 50 }, (_, index) => `p-${index}`);
        yield* inOneTick(
          Effect.gen(function* () {
            for (const label of completions) yield* requests.offer(fleetCompletion("p", label));
            yield* requests.offer(fleetWake("seat"));
          }),
        );
        assert.deepEqual(fleet.landedLabels(), ["seat"]);

        yield* inOneTick(held.release(1));
        assert.deepEqual(fleet.landedLabels("completion"), completions);
        assert.deepEqual(fleet.landedLabels("wake"), ["seat"]);
      }).pipe(Effect.provide(fleet.layer), Effect.scoped);
    },
  );

  it.effect(
    "keeps each family's order: a parent's children's wakes wait for each other, not for other families",
    () => {
      const fleet = makeFleet({ parents: { s1: "p", s2: "p", s3: "p", t1: "q", t2: "q" } });
      return Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        const held = fleet.lockOf(fleetThread("p"));
        yield* held.take(1);
        yield* inOneTick(
          Effect.forEach(
            [fleetWake("s1"), fleetWake("t1"), fleetWake("s2"), fleetWake("t2"), fleetWake("s3")],
            requests.offer,
            { discard: true },
          ),
        );
        assert.deepEqual(fleet.landedLabels(), ["t1", "t2"]);
        // The family waits on its first wake; the others are not also queued on the parent's lock.
        assert.deepEqual(
          fleet.state.entered.filter((entry) => entry.label.startsWith("s")).map((e) => e.label),
          ["s1"],
        );

        yield* inOneTick(held.release(1));
        assert.deepEqual(fleet.landedLabels(), ["t1", "t2", "s1", "s2", "s3"]);
      }).pipe(Effect.provide(fleet.layer), Effect.scoped);
    },
  );

  it.effect(
    "logs a failing dispatch and keeps going: other keys are not held up, completions retry, refused wakes are cleared",
    () => {
      const fleet = makeFleet({
        parents: { seat: "q" },
        outcome: (entry, attempt) =>
          attempt > 1
            ? "ok"
            : entry.label === "p-0"
              ? "fail"
              : entry.label === "r-0"
                ? "die"
                : "ok",
      });
      const refused = makeFleet({ outcome: () => "fail" });
      return Effect.gen(function* () {
        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* inOneTick(
            Effect.forEach(
              [
                fleetCompletion("p", "p-0"),
                fleetCompletion("r", "r-0"),
                fleetCompletion("p", "p-1"),
                fleetWake("seat"),
              ],
              requests.offer,
              { discard: true },
            ),
          );
          assert.deepEqual(fleet.landedLabels().toSorted(), ["p-1", "seat"]);
          yield* TestClock.adjust("1 minute");
          assert.deepEqual(fleet.landedLabels().toSorted(), ["p-0", "p-1", "r-0", "seat"]);
          assert.equal(fleet.state.attempts.get("p-0"), 2);
          assert.equal(fleet.state.attempts.get("r-0"), 2);
          assert.equal(fleet.state.attempts.get("p-1"), 1);
        }).pipe(Effect.provide(fleet.layer), Effect.scoped);

        const cleared = yield* Ref.make<ReadonlyArray<string>>([]);
        yield* Effect.gen(function* () {
          const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
          yield* inOneTick(
            Effect.forEach(
              ["w-0", "w-1", "w-2"],
              (label) =>
                requests.offer(
                  fleetWake(label, label, {
                    clearIfCurrent: () => Ref.update(cleared, (labels) => [...labels, label]),
                  }),
                ),
              { discard: true },
            ),
          );
          yield* TestClock.adjust("1 minute");
          assert.deepEqual((yield* Ref.get(cleared)).toSorted(), ["w-0", "w-1", "w-2"]);
          assert.deepEqual(
            [...refused.state.attempts.values()],
            [1, 1, 1],
            "a refused wake is cleared, not retried",
          );
        }).pipe(Effect.provide(refused.layer), Effect.scoped);
      });
    },
  );

  // The service's bounds: completion re-deliveries and provider wakes each have their own pool.
  const completionsInFlight = 16;
  const wakesInFlight = 16;

  it.effect("dispatches a wake queued behind 1,000 completion re-deliveries first", () => {
    const fleet = makeFleet({ parents: { seat: "q" }, cost: "100 millis" });
    return Effect.gen(function* () {
      const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
      const owners = Array.from({ length: 100 }, (_, index) => `owner-${index}`);
      const redeliveries = Array.from({ length: 1_000 }, (_, index) => ({
        owner: owners[index % owners.length]!,
        label: `c-${index}`,
      }));
      yield* inOneTick(
        Effect.gen(function* () {
          for (const { owner, label } of redeliveries) {
            yield* requests.offer(fleetCompletion(owner, label));
          }
          yield* requests.offer(fleetWake("seat"));
        }),
      );
      yield* TestClock.adjust("100 millis");
      assert.deepEqual(fleet.landedLabels("wake"), ["seat"]);
      assert.isAtMost(fleet.landedLabels("completion").length, completionsInFlight);

      yield* TestClock.adjust("10 minutes");
      const landed = fleet.landedLabels("completion");
      assert.equal(landed.length, redeliveries.length);
      assert.equal(new Set(landed).size, redeliveries.length);
      for (const owner of owners) {
        assert.deepEqual(
          fleet.state.landed
            .filter((entry) => entry.threadId === fleetThread(owner))
            .map((entry) => entry.label),
          redeliveries.filter((entry) => entry.owner === owner).map((entry) => entry.label),
          owner,
        );
      }
      assert.equal(fleet.state.peak.completion, completionsInFlight);
    }).pipe(Effect.provide(fleet.layer), Effect.scoped);
  });

  it.effect(
    "bounds completions and wakes in flight separately, so blocked completions never take a wake's slot",
    () => {
      const families = Array.from({ length: 40 }, (_, index) => index);
      const fleet = makeFleet({
        parents: Object.fromEntries(families.map((index) => [`seat-${index}`, `owner-${index}`])),
      });
      return Effect.gen(function* () {
        const requests = yield* ProviderContinuationRequests.ProviderContinuationRequests;
        const held = families.flatMap((index) => [
          fleet.lockOf(fleetThread(`parent-${index}`)),
          fleet.lockOf(fleetThread(`owner-${index}`)),
        ]);
        yield* Effect.forEach(held, (lock) => lock.take(1), { discard: true });
        yield* inOneTick(
          Effect.gen(function* () {
            for (const index of families) {
              yield* requests.offer(fleetCompletion(`parent-${index}`, `c-${index}`));
            }
            for (const index of families) yield* requests.offer(fleetWake(`seat-${index}`));
          }),
        );
        assert.deepEqual(fleet.state.inFlight, {
          completion: completionsInFlight,
          wake: wakesInFlight,
        });

        yield* inOneTick(Effect.forEach(held, (lock) => lock.release(1), { discard: true }));
        assert.equal(new Set(fleet.landedLabels("completion")).size, families.length);
        assert.equal(new Set(fleet.landedLabels("wake")).size, families.length);
        assert.equal(fleet.state.landed.length, 2 * families.length);
        assert.deepEqual(fleet.state.peak, {
          completion: completionsInFlight,
          wake: wakesInFlight,
        });
      }).pipe(Effect.provide(fleet.layer), Effect.scoped);
    },
  );
});

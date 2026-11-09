import { assert, it } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointScopeId,
  CommandId,
  NodeId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  VcsProcessExitError,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import * as EffectAdmission from "./EffectAdmission.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as RunFinalizationService from "./RunFinalizationService.ts";
import * as ProviderSessionManager from "./ProviderSessionManager.ts";
import * as ProviderTurnControlService from "./ProviderTurnControlService.ts";
import * as ProviderTurnStartService from "./ProviderTurnStartService.ts";
import * as RuntimeRequestService from "./RuntimeRequestService.ts";
import * as ThreadTitleRegenerationService from "./ThreadTitleRegenerationService.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";
import * as ServerSettings from "../serverSettings.ts";

// These admission fixtures model the regular pool. The reserved control worker
// is exercised against the real outbox in FoundationPersistence.test.ts.
const mockNormalOutbox = (input: Partial<EffectOutbox.EffectOutboxV2Shape>) =>
  Layer.mock(EffectOutbox.EffectOutboxV2)({
    awaitControlAvailable: () => Effect.never,
    nextControlClaimableAt: () => Effect.succeedNone,
    ...input,
    ...(input.claimNext === undefined
      ? {}
      : {
          claimNext: (options) =>
            options.controlLane !== undefined ? Effect.succeedNone : input.claimNext!(options),
        }),
  });

const threadId = ThreadId.make("thread:effect-worker-restart");
const oldSessionId = ProviderSessionId.make("provider-session:effect-worker-restart:old");
const providerThreadId = ProviderThreadId.make("provider-thread:effect-worker-restart");
const providerTurnId = ProviderTurnId.make("provider-turn:effect-worker-restart");
const attemptId = RunAttemptId.make("run-attempt:effect-worker-restart");
const runId = RunId.make("run:effect-worker-restart");

function restartEffect(
  now: DateTime.Utc,
  sessionTransition: NonNullable<
    Extract<
      EffectOutbox.OrchestrationEffectV2["request"],
      { readonly type: "provider-turn.restart" }
    >["sessionTransition"]
  >,
): EffectOutbox.OrchestrationEffectV2 {
  const timestamp = DateTime.formatIso(now);
  return {
    id: `effect:restart:${sessionTransition.type}`,
    commandId: CommandId.make(`command:restart:${sessionTransition.type}`),
    threadId,
    request: {
      type: "provider-turn.restart",
      providerSessionId: oldSessionId,
      providerThreadId,
      providerTurnId,
      interruptedAttemptId: attemptId,
      runId,
      sessionTransition,
    },
    status: "running",
    attemptCount: 1,
    availableAt: timestamp,
    leaseOwner: "test-worker",
    createdAt: timestamp,
    updatedAt: timestamp,
    completedAt: null,
    lastError: null,
  };
}

function layerExecutorFor(input: {
  readonly events: Ref.Ref<ReadonlyArray<string>>;
  readonly failFirstStart?: Ref.Ref<boolean>;
  readonly threads?: Partial<ThreadManagementService.ThreadManagementService["Service"]>;
  readonly continueAfterRestart?: boolean;
  readonly interrupt?: ProviderTurnControlService.ProviderTurnControlServiceV2Shape["interrupt"];
  readonly rollback?: CheckpointRollbackService.CheckpointRollbackServiceV2Shape["execute"];
  readonly respond?: RuntimeRequestService.RuntimeRequestServiceV2["Service"]["respond"];
}) {
  const record = (event: string) => Ref.update(input.events, (events) => [...events, event]);
  const layerDependencies = Layer.mergeAll(
    Layer.succeed(
      ProviderTurnControlService.ProviderTurnControlServiceV2,
      ProviderTurnControlService.ProviderTurnControlServiceV2.of({
        interrupt: input.interrupt ?? (() => Effect.void),
        steer: () => Effect.void,
        interruptAndAwaitTerminal: (request) =>
          record(
            request.replacementProviderSessionId === undefined
              ? "interrupt"
              : `interrupt:${request.replacementProviderSessionId}`,
          ),
      }),
    ),
    Layer.succeed(
      ProviderSessionManager.ProviderSessionManagerV2,
      ProviderSessionManager.ProviderSessionManagerV2.of({
        shutdown: Effect.void,
        open: () => Effect.die("unused open"),
        get: () => Effect.succeedNone,
        isLive: () => Effect.succeed(false),
        close: () => Effect.void,
        closeInstance: () => Effect.void,
        release: () => record("release"),
        detach: () => record("detach"),
        otherWorkingThreads: () => Effect.succeed([]),
        releaseUnlessBusy: () => Effect.succeed(true),
      }),
    ),
    Layer.succeed(
      ProviderTurnStartService.ProviderTurnStartServiceV2,
      ProviderTurnStartService.ProviderTurnStartServiceV2.of({
        start: () =>
          Effect.gen(function* () {
            yield* record("start");
            if (
              input.failFirstStart !== undefined &&
              (yield* Ref.getAndSet(input.failFirstStart, false))
            ) {
              return yield* new ProviderTurnStartService.ProviderTurnStartError({
                runId,
                cause: "simulated first start failure",
              });
            }
          }),
      }),
    ),
    Layer.succeed(
      RunFinalizationService.RunFinalizationService,
      RunFinalizationService.RunFinalizationService.of({ finalize: () => Effect.void }),
    ),
    Layer.succeed(
      CheckpointRollbackService.CheckpointRollbackServiceV2,
      CheckpointRollbackService.CheckpointRollbackServiceV2.of({
        execute: input.rollback ?? (() => Effect.void),
      }),
    ),
    Layer.succeed(
      RuntimeRequestService.RuntimeRequestServiceV2,
      RuntimeRequestService.RuntimeRequestServiceV2.of({
        respond: input.respond ?? (() => Effect.void),
      }),
    ),
    Layer.succeed(
      ThreadTitleRegenerationService.ThreadTitleRegenerationService,
      ThreadTitleRegenerationService.ThreadTitleRegenerationService.of({
        execute: () => Effect.void,
      }),
    ),
  );
  return EffectWorker.layerExecutor.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerDependencies,
        mockNormalOutbox({ recordSteerDelivery: () => Effect.void }),
        Layer.mock(ThreadManagementService.ThreadManagementService)(input.threads ?? {}),
        ServerSettings.layerTest(
          input.continueAfterRestart === true ? { continueThreadsAfterServerUpdate: true } : {},
        ),
      ),
    ),
  );
}

it.effect.each([
  {
    name: "settles a stopped run when its adapter has already lost the native turn",
    unresponsive: false,
    dispatched: ["thread.background-work.settle"],
  },
  {
    name: "leaves the run running and flags the Stop when the provider never confirms it",
    unresponsive: true,
    dispatched: ["thread.interrupt.unresponsive"],
  },
])("$name", ({ unresponsive, dispatched }) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const layer = layerExecutorFor({
      events,
      interrupt: () =>
        new ProviderTurnControlService.ProviderTurnControlError({
          threadId,
          operation: "interrupt",
          providerTurnId,
          ...(unresponsive ? { unresponsive } : {}),
          cause: unresponsive ? "Timed out." : "Provider turn is not active.",
        }),
      threads: {
        dispatch: (command) =>
          Ref.update(events, (current) => [...current, command.type]).pipe(
            Effect.as({ sequence: 1, storedEvents: [] }),
          ),
      },
    });
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      yield* executor.execute({
        ...restartEffect(now, { type: "detach" }),
        request: {
          type: "provider-turn.interrupt",
          providerSessionId: oldSessionId,
          providerThreadId,
          providerTurnId,
        },
      });
    }).pipe(Effect.provide(layer));
    assert.deepEqual(yield* Ref.get(events), dispatched);
  }),
);

const lockedIndexMessage =
  "Git's index is locked. Wait for other Git operations to finish, then try again.";
const rollbackCheckpointId = CheckpointId.make("checkpoint:effect-worker-rollback");
const rollbackScopeId = CheckpointScopeId.make("checkpoint-scope:effect-worker-rollback");
const restoreFailure = (detail: string) =>
  new CheckpointRollbackService.CheckpointRollbackExecutionError({
    reason: "unexpected-failure",
    threadId,
    providerThreadId,
    checkpointId: rollbackCheckpointId,
    cause: new CheckpointService.CheckpointRestoreError({
      scopeId: rollbackScopeId,
      checkpointId: rollbackCheckpointId,
      cause: new VcsProcessExitError({
        operation: "GitVcsDriverCore.restoreCheckpoint",
        command: "git",
        cwd: "/private/repo",
        exitCode: 128,
        detail,
        failureKind: "command-failed",
      }),
    }),
  });

it.effect.each([
  {
    name: "a held Git index lock",
    failure: restoreFailure(lockedIndexMessage),
    message: lockedIndexMessage,
  },
  {
    name: "raw Git output",
    failure: restoreFailure("error: could not clean /private/repo/secret.txt"),
    message: CheckpointRollbackService.ROLLBACK_FAILED_MESSAGE,
  },
])("reports a rollback blocked by $name only after the last attempt", ({ failure, message }) =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const failures = yield* Ref.make<ReadonlyArray<string>>([]);
    const layer = layerExecutorFor({
      events: yield* Ref.make<ReadonlyArray<string>>([]),
      rollback: () => Effect.fail(failure),
      threads: {
        dispatch: (command) =>
          Ref.update(failures, (current) =>
            command.type === "checkpoint.rollback.fail" ? [...current, command.message] : current,
          ).pipe(Effect.as({ sequence: 1, storedEvents: [] })),
      },
    });
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      const rollback: EffectOutbox.OrchestrationEffectV2 = {
        ...restartEffect(now, { type: "detach" }),
        request: {
          type: "provider-thread.rollback",
          providerThreadId,
          checkpointId: rollbackCheckpointId,
          scopeId: rollbackScopeId,
          restoreFiles: true,
        },
      };
      yield* Effect.flip(executor.execute(rollback, { willRetry: true }));
      assert.deepEqual(yield* Ref.get(failures), []);
      yield* Effect.flip(executor.execute(rollback, { willRetry: false }));
    }).pipe(Effect.provide(layer));
    assert.deepEqual(yield* Ref.get(failures), [message]);
  }),
);

it.effect("does not report an answer whose last delivery attempt is interrupted", () =>
  Effect.gen(function* () {
    const now = yield* DateTime.now;
    const reported = yield* Ref.make(0);
    const layer = layerExecutorFor({
      events: yield* Ref.make<ReadonlyArray<string>>([]),
      respond: () => Effect.interrupt,
      threads: {
        dispatch: (command) =>
          Ref.update(reported, (count) =>
            command.type === "runtime-request.delivery.fail" ? count + 1 : count,
          ).pipe(Effect.as({ sequence: 1, storedEvents: [] })),
      },
    });
    const exit = yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      return yield* Effect.exit(
        executor.execute(
          {
            ...restartEffect(now, { type: "detach" }),
            request: {
              type: "runtime-request.respond",
              providerSessionId: oldSessionId,
              requestId: RuntimeRequestId.make("runtime-request:effect-worker-interrupted"),
              answers: { next: "Ship it" },
            },
          },
          { willRetry: false },
        ),
      );
    }).pipe(Effect.provide(layer));
    assert.isTrue(Exit.hasInterrupts(exit));
    assert.equal(yield* Ref.get(reported), 0);
  }),
);

it.effect("requeues a claim when a pre-execution worker check fails", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-pre-execution-failure";
    const workerId = "worker-pre-execution-failure";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-pre-execution-failure"),
      threadId: ThreadId.make("thread:worker-pre-execution-failure"),
      request: { type: "terminal.cleanup" },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const retries = yield* Ref.make<
      ReadonlyArray<{
        readonly effectId: string;
        readonly workerId: string;
        readonly error: string;
        readonly delayMs: number;
      }>
    >([]);
    const executionCount = yield* Ref.make(0);
    const layerOutbox = mockNormalOutbox({
      claimNext: () => Effect.succeedSome(claimedEffect),
      get: () =>
        Effect.fail(
          new EffectOutbox.EffectOutboxError({
            operation: "get",
            effectId,
            cause: "simulated cancellation-state read failure",
          }),
        ),
      retry: (input) =>
        Ref.update(retries, (existing) => [...existing, input]).pipe(Effect.as(true)),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () => Ref.update(executionCount, (count) => count + 1),
      }),
    );
    const layerWorker = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(layerWorker),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) {
      assert.include(Cause.pretty(exit.cause), "simulated cancellation-state read failure");
    }
    assert.equal(yield* Ref.get(executionCount), 0);
    const retry = (yield* Ref.get(retries))[0];
    assert.isDefined(retry);
    assert.equal(retry.effectId, effectId);
    assert.equal(retry.workerId, workerId);
    assert.equal(retry.delayMs, 0);
    assert.include(retry.error, "simulated cancellation-state read failure");
  }),
);

it.effect("arms cancellation before the durable pre-execution check", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-cancellation-registration-race";
    const workerId = "worker-cancellation-registration-race";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-cancellation-registration-race"),
      threadId: ThreadId.make("thread:worker-cancellation-registration-race"),
      request: { type: "terminal.cleanup" },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const signal = yield* Deferred.make<void>();
    let cancellationArmed = false;
    const executionCount = yield* Ref.make(0);
    const settlementCount = yield* Ref.make(0);
    const layerOutbox = mockNormalOutbox({
      claimNext: () => Effect.succeedSome(claimedEffect),
      awaitCancellation: () => {
        cancellationArmed = true;
        return Deferred.await(signal);
      },
      get: () =>
        Effect.gen(function* () {
          if (cancellationArmed) {
            yield* Deferred.succeed(signal, undefined);
          }
          return Option.some(claimedEffect);
        }),
      clearCancellation: () => Effect.void,
      succeed: () => Ref.update(settlementCount, (count) => count + 1).pipe(Effect.as(true)),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () =>
          Effect.yieldNow.pipe(Effect.andThen(Ref.update(executionCount, (count) => count + 1))),
      }),
    );
    const layerWorker = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(layerWorker),
      Effect.exit,
    );

    if (Exit.isFailure(exit)) {
      assert.fail(Cause.pretty(exit.cause));
    }
    assert.equal(yield* Ref.get(executionCount), 0);
    assert.equal(yield* Ref.get(settlementCount), 0);
  }),
);

it.effect("terminalizes a process-bound claim when success settlement fails", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const effectId = "effect:worker-process-bound-settlement-failure";
    const workerId = "worker-process-bound-settlement-failure";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      id: effectId,
      commandId: CommandId.make("command:worker-process-bound-settlement-failure"),
      threadId: ThreadId.make("thread:worker-process-bound-settlement-failure"),
      request: {
        type: "provider-turn.start",
        runId: RunId.make("run:worker-process-bound-settlement-failure"),
      },
      status: "running",
      attemptCount: 1,
      availableAt: now,
      leaseOwner: workerId,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
      lastError: null,
    };
    const retries = yield* Ref.make(0);
    const terminalErrors = yield* Ref.make<ReadonlyArray<string>>([]);
    const executionCount = yield* Ref.make(0);
    const layerOutbox = mockNormalOutbox({
      claimNext: () => Effect.succeedSome(claimedEffect),
      get: () => Effect.succeedSome(claimedEffect),
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      succeed: () =>
        Effect.fail(
          new EffectOutbox.EffectOutboxError({
            operation: "succeed",
            effectId,
            cause: "simulated success settlement failure",
          }),
        ),
      retry: () => Ref.update(retries, (count) => count + 1).pipe(Effect.as(true)),
      fail: ({ error }) =>
        Ref.update(terminalErrors, (existing) => [...existing, error]).pipe(Effect.as(true)),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () => Ref.update(executionCount, (count) => count + 1),
      }),
    );
    const layerWorker = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(layerWorker),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    assert.equal(yield* Ref.get(executionCount), 1);
    assert.equal(yield* Ref.get(retries), 0);
    const terminalError = (yield* Ref.get(terminalErrors))[0];
    assert.isDefined(terminalError);
    assert.include(terminalError, "after execution started");
    assert.include(terminalError, "simulated success settlement failure");
  }),
);

it.effect("settles a delegated child once its restart continuation fails for good", () =>
  Effect.gen(function* () {
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    const events = yield* Ref.make<ReadonlyArray<string>>([]);
    const recovered = yield* Ref.make<ReadonlyArray<ThreadId>>([]);
    const layer = layerExecutorFor({
      events,
      continueAfterRestart: true,
      threads: {
        getThreadRecords: () => Effect.fail(new Error("provider instance removed") as never),
        recoverDelegatedTask: (childThreadId) =>
          Ref.update(recovered, (ids) => [...ids, childThreadId]),
      },
    });
    const effect: EffectOutbox.OrchestrationEffectV2 = {
      id: `effect:restart-continuation:${runId}`,
      commandId: CommandId.make("command:restart-continuation-failure"),
      threadId,
      request: { type: "provider-runtime.continue", sourceRunId: runId },
      status: "running",
      attemptCount: 1,
      availableAt: timestamp,
      leaseOwner: "test-worker",
      createdAt: timestamp,
      updatedAt: timestamp,
      completedAt: null,
      lastError: null,
    };
    yield* Effect.gen(function* () {
      const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(executor.execute(effect, { willRetry: true }))),
      );
      assert.deepEqual(yield* Ref.get(recovered), []);
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(executor.execute(effect, { willRetry: false }))),
      );
      assert.deepEqual(yield* Ref.get(recovered), [threadId]);
    }).pipe(Effect.provide(layer));
  }),
);

it.effect.each(["confirmed", "unconfirmed", "failed"] as const)(
  "a native subagent stop only repairs %s provider evidence",
  (outcome) =>
    Effect.gen(function* () {
      const now = yield* DateTime.now;
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const layer = layerExecutorFor({
        events,
        interrupt: (input) => {
          assert.equal(input.subagent?.nativeTaskId, "native-task");
          return Ref.update(events, (current) => [...current, "stop-child"]).pipe(
            Effect.andThen(
              outcome === "failed"
                ? new ProviderTurnControlService.ProviderTurnControlError({
                    threadId,
                    providerTurnId,
                    operation: "interrupt",
                    cause: "SDK stop failed",
                  })
                : Effect.succeed(
                    outcome === "confirmed"
                      ? {
                          status: "cancelled" as const,
                          completedAt: now,
                          result: "Stopped by Claude",
                        }
                      : undefined,
                  ),
            ),
          );
        },
        threads: {
          dispatch: (command) => {
            assert.equal(command.type, "thread.background-work.settle");
            if (command.type === "thread.background-work.settle") {
              assert.equal(command.subagent?.id, "subagent");
              assert.deepEqual(command.subagent?.outcome, {
                status: "cancelled",
                completedAt: now,
                result: "Stopped by Claude",
              });
            }
            return Ref.update(events, (current) => [...current, "settle-child"]).pipe(
              Effect.as({ sequence: 1, storedEvents: [] }),
            );
          },
        },
      });
      const execution = yield* Effect.gen(function* () {
        const executor = yield* EffectWorker.OrchestrationEffectExecutorV2;
        yield* executor.execute({
          ...restartEffect(now, { type: "detach" }),
          request: {
            type: "provider-turn.interrupt",
            providerSessionId: oldSessionId,
            providerThreadId,
            providerTurnId,
            subagent: { id: NodeId.make("subagent"), nativeTaskId: "native-task" },
          },
        });
      }).pipe(Effect.provide(layer), Effect.exit);
      assert.equal(Exit.isFailure(execution), outcome === "failed");
      assert.deepEqual(
        yield* Ref.get(events),
        outcome === "confirmed" ? ["stop-child", "settle-child"] : ["stop-child"],
      );
    }),
);

const admissionEffect = (
  index: number,
  now: string,
  workerId: string,
  request: EffectOutbox.OrchestrationEffectV2["request"],
): EffectOutbox.OrchestrationEffectV2 => ({
  id: `effect:admission-${index}`,
  commandId: CommandId.make(`command:admission-${index}`),
  threadId: ThreadId.make(`thread:admission-${index}`),
  request,
  status: "running",
  attemptCount: 1,
  availableAt: now,
  leaseOwner: workerId,
  createdAt: now,
  updatedAt: now,
  completedAt: null,
  lastError: null,
});

/** The sampler interval the admission tests run at, the unit of every deadline below. */
const SAMPLE = Duration.seconds(1);

/**
 * Awaits `effect`, which needs no clock, for at most `samples` sampler intervals of test-clock
 * time: each step lets the fibers the clock readied run to their next suspension, so a regression
 * that parks the awaited work fails here at once, not at the suite's timeout.
 */
const within = <A, E>(effect: Effect.Effect<A, E>, samples: number, what: string) =>
  Effect.gen(function* () {
    const done = yield* Deferred.make<Exit.Exit<A, E>>();
    const fiber = yield* Effect.forkChild(
      Effect.exit(effect).pipe(Effect.tap((exit) => Deferred.succeed(done, exit))),
    );
    yield* Effect.replicateEffect(TestClock.adjust(SAMPLE), samples, { discard: true });
    if (!(yield* Deferred.isDone(done))) {
      yield* Fiber.interrupt(fiber);
      assert.fail(`${what} did not happen within ${samples} sampler intervals of test-clock time`);
    }
    return yield* yield* Deferred.await(done);
  });

/** Awaits `count` starts from the executor's queue within a few sampler intervals. */
const startsWithin = (queue: Queue.Queue<string>, count: number, what: string) =>
  within(Effect.all(Array.from({ length: count }, () => Queue.take(queue))), 3, what);

it.effect("claims only ungated effects once the admission slots are taken", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const workerId = "worker-admission-claims";
    const pending = [
      admissionEffect(1, now, workerId, {
        type: "provider-turn.start",
        runId: RunId.make("run:admission-1"),
      }),
      admissionEffect(2, now, workerId, {
        type: "provider-turn.start",
        runId: RunId.make("run:admission-2"),
      }),
      admissionEffect(3, now, workerId, {
        type: "provider-turn.interrupt",
        providerSessionId: oldSessionId,
        providerThreadId,
        providerTurnId,
      }),
    ];
    const exclusions = yield* Ref.make<ReadonlyArray<ReadonlyArray<string>>>([]);
    const started = yield* Ref.make<ReadonlyArray<string>>([]);
    const finishTurnStarts = yield* Deferred.make<void>();
    const layerOutbox = mockNormalOutbox({
      claimNext: ({ excludeEffectTypes = [] }) =>
        Ref.update(exclusions, (seen) => [...seen, excludeEffectTypes]).pipe(
          Effect.map(() => {
            const index = pending.findIndex(
              (effect) => !excludeEffectTypes.includes(effect.request.type),
            );
            return index === -1 ? Option.none() : Option.some(pending.splice(index, 1)[0]!);
          }),
        ),
      get: () => Effect.succeedNone,
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      succeed: () => Effect.succeed(true),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: (effect) =>
          Ref.update(started, (ids) => [...ids, effect.id]).pipe(
            Effect.andThen(
              effect.request.type === "provider-turn.start"
                ? Deferred.await(finishTurnStarts)
                : Effect.void,
            ),
          ),
      }),
    );
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.provide(
        EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
        ),
      ),
    );
    const admission = yield* EffectAdmission.make({
      workers: 1,
      closeAbove: 40,
      samplesToSwitch: 2,
      sampleInterval: Duration.seconds(1),
      pressure: Effect.succeed(Effect.succeed(5)),
      onAllowanceRestored: Effect.void,
      onSlotFreed: Effect.void,
    });
    const runOnce = worker.runOnce.pipe(
      Effect.provideService(EffectAdmission.EffectAdmission, admission),
    );

    const first = yield* Effect.forkChild(runOnce);
    yield* Effect.yieldNow;
    assert.deepEqual(yield* Ref.get(started), ["effect:admission-1"]);
    // The slot is taken, so this claim skips the second turn start and takes the interrupt.
    assert.isTrue(yield* within(runOnce, 3, "the interrupt's claim"));
    assert.deepEqual(yield* Ref.get(started), ["effect:admission-1", "effect:admission-3"]);
    assert.deepEqual(yield* Ref.get(exclusions), [[], EffectAdmission.GATED_EFFECT_TYPES]);
    assert.equal(pending.length, 1);

    yield* Deferred.succeed(finishTurnStarts, undefined);
    assert.isTrue(yield* Fiber.join(first));
    assert.isTrue(yield* runOnce);
    assert.deepEqual(yield* Ref.get(started), [
      "effect:admission-1",
      "effect:admission-3",
      "effect:admission-2",
    ]);
    assert.deepEqual(yield* admission.state, {
      allowed: 1,
      inFlight: 0,
      saturated: false,
      sampling: false,
    });
  }),
);

const saturatedDaemon = (options: {
  readonly wakeUpsDelivered: boolean;
  readonly admission?: false;
}) =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const workerId = "worker-admission-daemon";
    const pending: Array<EffectOutbox.OrchestrationEffectV2> = [];
    const available = yield* Queue.unbounded<void>();
    const started = yield* Queue.unbounded<string>();
    const finishTurnStarts = yield* Deferred.make<void>();
    const claimable = (excludeEffectTypes: ReadonlyArray<string>) =>
      pending.filter((effect) => !excludeEffectTypes.includes(effect.request.type));
    const claims: Array<ReadonlyArray<string>> = [];
    const layerOutbox = mockNormalOutbox({
      claimNext: ({ excludeEffectTypes = [] }) =>
        Effect.sync(() => {
          claims.push(excludeEffectTypes);
          const next = claimable(excludeEffectTypes)[0];
          if (next === undefined) return Option.none();
          pending.splice(pending.indexOf(next), 1);
          return Option.some(next);
        }),
      nextClaimableAt: Effect.sync(() =>
        claimable([]).length === 0 ? Option.none() : Option.some(DateTime.makeUnsafe(now)),
      ),
      nextClaimableAtExcluding: (excludeEffectTypes) =>
        Effect.sync(() =>
          claimable(excludeEffectTypes).length === 0
            ? Option.none()
            : Option.some(DateTime.makeUnsafe(now)),
        ),
      awaitAvailable: Queue.take(available),
      notifyAvailable: (count = 1) =>
        options.wakeUpsDelivered
          ? Queue.offerAll(
              available,
              Array.from({ length: count }, () => undefined),
            ).pipe(Effect.asVoid)
          : Effect.void,
      get: () => Effect.succeedNone,
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      succeed: () => Effect.succeed(true),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: (effect) =>
          Queue.offer(started, effect.id).pipe(
            Effect.andThen(
              effect.request.type === "provider-turn.start"
                ? Deferred.await(finishTurnStarts)
                : Effect.void,
            ),
          ),
      }),
    );
    const pressure = yield* Ref.make(60);
    const daemon = yield* EffectWorker.runDaemonWithOptions({
      concurrency: 4,
      admission: options.admission ?? { delayAboveMs: 40 },
      pressure: Effect.succeed(Ref.get(pressure)),
      livenessPollIntervalMs: 60_000,
    }).pipe(
      Effect.provide(
        EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
        ),
      ),
      Effect.forkChild,
    );
    const startEffect = (index: number) =>
      admissionEffect(index, now, workerId, {
        type: "provider-turn.start",
        runId: RunId.make(`run:admission-${index}`),
      });
    const interruptEffect = admissionEffect(5, now, workerId, {
      type: "provider-turn.interrupt",
      providerSessionId: oldSessionId,
      providerThreadId,
      providerTurnId,
    });
    if (options.admission === false) {
      // No gate: the pressure never matters. After the saturated samples that would close a gate
      // to two, the third and fourth starts are claimed while the first two still run.
      pending.push(startEffect(1), startEffect(2));
      yield* Queue.offerAll(available, [undefined, undefined]);
      const firstTwo = yield* startsWithin(started, 2, "the first two turn starts");
      assert.deepEqual([...firstTwo].toSorted(), ["effect:admission-1", "effect:admission-2"]);
      yield* TestClock.adjust("2 seconds");
      pending.push(startEffect(3), startEffect(4), interruptEffect);
      yield* Queue.offerAll(available, [undefined, undefined, undefined]);
      const rest = yield* startsWithin(started, 2, "the third and fourth turn starts");
      assert.deepEqual([...rest].toSorted(), ["effect:admission-3", "effect:admission-4"]);
      // Every worker runs a start, so the interrupt waits for one of them, not for a gate.
      assert.equal(pending.length, 1);
      yield* Deferred.succeed(finishTurnStarts, undefined);
      assert.deepEqual(yield* startsWithin(started, 1, "the interrupt"), ["effect:admission-5"]);
      yield* Fiber.interrupt(daemon);
      return;
    }
    // The first two starts arm the pressure source; two saturated samples then halve the
    // allowance to two, so the other starts stay in the outbox while the interrupt runs.
    pending.push(startEffect(1), startEffect(2));
    yield* Queue.offerAll(available, [undefined, undefined]);
    const firstTwo = yield* startsWithin(started, 2, "the first two turn starts");
    assert.deepEqual([...firstTwo].toSorted(), ["effect:admission-1", "effect:admission-2"]);
    yield* TestClock.adjust("2 seconds");
    pending.push(startEffect(3), startEffect(4), interruptEffect);
    yield* Queue.offerAll(available, [undefined, undefined, undefined]);
    assert.deepEqual(yield* startsWithin(started, 1, "the interrupt"), ["effect:admission-5"]);
    yield* TestClock.adjust("10 seconds");
    assert.deepEqual(
      pending.map((effect) => effect.id),
      ["effect:admission-3", "effect:admission-4"],
    );
    // Workers idle behind the closed gate do not re-poll: a claim per worker per wake-up, no more.
    assert.isAtMost(claims.length, 12);

    // Headroom returns: the idle workers are woken and take the remaining turn starts.
    yield* Ref.set(pressure, 5);
    yield* TestClock.adjust("2 seconds");
    const secondWave = yield* startsWithin(started, 2, "the turn starts after reopening");
    assert.deepEqual([...secondWave].toSorted(), ["effect:admission-3", "effect:admission-4"]);
    assert.equal(pending.length, 0);

    yield* Deferred.succeed(finishTurnStarts, undefined);
    yield* Fiber.interrupt(daemon);
  });

it.effect("a saturated daemon runs an interrupt while turn starts beyond the allowance wait", () =>
  saturatedDaemon({ wakeUpsDelivered: true }),
);

it.effect("a daemon with admission off claims every turn start whatever the pressure", () =>
  saturatedDaemon({ wakeUpsDelivered: true, admission: false }),
);

it("maps the configured delay mark to the daemon's admission option", () => {
  assert.deepEqual(EffectWorker.admissionOptionsFor(undefined), {});
  assert.deepEqual(EffectWorker.admissionOptionsFor(0), { admission: false });
  assert.deepEqual(EffectWorker.admissionOptionsFor(40), { admission: { delayAboveMs: 40 } });
});

it.effect("builds no gate for one worker, whose allowance a gate could not change", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const workerId = "worker-single";
    const pending = [
      admissionEffect(1, now, workerId, {
        type: "provider-turn.start",
        runId: RunId.make("run:admission-1"),
      }),
    ];
    const available = yield* Queue.unbounded<void>();
    const started = yield* Queue.unbounded<string>();
    const finish = yield* Deferred.make<void>();
    const acquired = yield* Ref.make(0);
    const wakeUps = yield* Ref.make(0);
    const layerOutbox = mockNormalOutbox({
      claimNext: () => Effect.sync(() => Option.fromUndefinedOr(pending.shift())),
      nextClaimableAt: Effect.succeedNone,
      nextClaimableAtExcluding: () => Effect.succeedNone,
      awaitAvailable: Queue.take(available),
      notifyAvailable: (count = 1) =>
        Ref.update(wakeUps, (total) => total + count).pipe(
          Effect.andThen(
            Queue.offerAll(
              available,
              Array.from({ length: count }, () => undefined),
            ),
          ),
          Effect.asVoid,
        ),
      get: () => Effect.succeedNone,
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      succeed: () => Effect.succeed(true),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: (effect) =>
          Queue.offer(started, effect.id).pipe(Effect.andThen(Deferred.await(finish))),
      }),
    );
    // The saturated allowance of one worker is one: a gate could hold nothing back, so none is
    // built — no pressure source for the start in flight, no wake-up when it finishes.
    const daemon = yield* EffectWorker.runDaemonWithOptions({
      concurrency: 1,
      admission: { delayAboveMs: 40 },
      pressure: Effect.acquireRelease(
        Ref.update(acquired, (count) => count + 1).pipe(Effect.as(Effect.succeed(60))),
        () => Effect.void,
      ),
      livenessPollIntervalMs: 60_000,
    }).pipe(
      Effect.provide(
        EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
        ),
      ),
      Effect.forkChild,
    );
    assert.deepEqual(yield* startsWithin(started, 1, "the turn start"), ["effect:admission-1"]);
    yield* TestClock.adjust("3 seconds");
    assert.equal(yield* Ref.get(acquired), 0);
    yield* Deferred.succeed(finish, undefined);
    yield* TestClock.adjust("1 second");
    assert.equal(yield* Ref.get(wakeUps), 0);
    yield* Fiber.interrupt(daemon);
  }),
);

it.effect("an expired slot wakes an idle worker, which claims the turn start held back by it", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const workerId = "worker-expiry";
    const pending = [
      admissionEffect(1, now, workerId, {
        type: "provider-turn.start",
        runId: RunId.make("run:admission-1"),
      }),
    ];
    const available = yield* Queue.unbounded<void>();
    const started = yield* Queue.unbounded<string>();
    const finishFirst = yield* Deferred.make<void>();
    const layerOutbox = mockNormalOutbox({
      claimNext: ({ excludeEffectTypes = [] }) =>
        Effect.sync(() => {
          const next = pending.find((effect) => !excludeEffectTypes.includes(effect.request.type));
          if (next === undefined) return Option.none();
          pending.splice(pending.indexOf(next), 1);
          return Option.some(next);
        }),
      nextClaimableAt: Effect.succeedNone,
      nextClaimableAtExcluding: (excludeEffectTypes) =>
        Effect.sync(() =>
          pending.some((effect) => !excludeEffectTypes.includes(effect.request.type))
            ? Option.some(DateTime.makeUnsafe(now))
            : Option.none(),
        ),
      awaitAvailable: Queue.take(available),
      notifyAvailable: (count = 1) =>
        Queue.offerAll(
          available,
          Array.from({ length: count }, () => undefined),
        ).pipe(Effect.asVoid),
      get: () => Effect.succeedNone,
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      succeed: () => Effect.succeed(true),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: (effect) =>
          Queue.offer(started, effect.id).pipe(
            Effect.andThen(
              effect.id === "effect:admission-1" ? Deferred.await(finishFirst) : Effect.void,
            ),
          ),
      }),
    );
    const pressure = yield* Ref.make(60);
    // Two workers, one slot while saturated; the liveness poll is kept out of the way so that
    // only the expiry's wake-up can bring the idle worker back.
    const daemon = yield* EffectWorker.runDaemonWithOptions({
      concurrency: 2,
      admission: { delayAboveMs: 40 },
      pressure: Effect.succeed(Ref.get(pressure)),
      livenessPollIntervalMs: 3_600_000,
    }).pipe(
      Effect.provide(
        EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
        ),
      ),
      Effect.forkChild,
    );
    yield* Queue.offerAll(available, [undefined]);
    assert.deepEqual(yield* startsWithin(started, 1, "the first turn start"), [
      "effect:admission-1",
    ]);
    yield* TestClock.adjust("2 seconds");
    pending.push(
      admissionEffect(3, now, workerId, {
        type: "provider-turn.start",
        runId: RunId.make("run:admission-3"),
      }),
    );
    yield* Queue.offerAll(available, [undefined]);
    yield* TestClock.adjust("5 seconds");
    assert.equal(pending.length, 1);
    // The first start hangs past the bound: its slot expires, which wakes the idle worker, and
    // that worker claims the third start while the first still runs.
    yield* TestClock.adjust(EffectAdmission.SLOT_MAX_HOLD);
    assert.deepEqual(yield* startsWithin(started, 1, "the third turn start after the expiry"), [
      "effect:admission-3",
    ]);
    assert.isFalse(yield* Deferred.isDone(finishFirst));
    yield* Deferred.succeed(finishFirst, undefined);
    yield* Fiber.interrupt(daemon);
  }),
);

it.effect("requeues a claim whose timestamp cannot be parsed, as before", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const workerId = "worker-corrupt-timestamp";
    const claimedEffect: EffectOutbox.OrchestrationEffectV2 = {
      ...admissionEffect(1, now, workerId, { type: "terminal.cleanup" }),
      availableAt: "not a timestamp",
    };
    const retries = yield* Ref.make<ReadonlyArray<string>>([]);
    const executionCount = yield* Ref.make(0);
    const layerOutbox = mockNormalOutbox({
      claimNext: () => Effect.succeedSome(claimedEffect),
      get: () => Effect.succeedNone,
      retry: (input) =>
        Ref.update(retries, (existing) => [...existing, input.effectId]).pipe(Effect.as(true)),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: () => Ref.update(executionCount, (count) => count + 1),
      }),
    );
    const layerWorker = EffectWorker.layerWithOptions({ workerId }).pipe(
      Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
    );

    const exit = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.flatMap((worker) => worker.runOnce),
      Effect.provide(layerWorker),
      Effect.exit,
    );

    assert.isTrue(Exit.isFailure(exit));
    assert.equal(yield* Ref.get(executionCount), 0);
    assert.deepEqual(yield* Ref.get(retries), [claimedEffect.id]);
  }),
);

it.effect("gives the admission slot back when the claim itself fails", () =>
  Effect.gen(function* () {
    const workerId = "worker-claim-failure";
    const layerOutbox = mockNormalOutbox({
      claimNext: () =>
        Effect.fail(
          new EffectOutbox.EffectOutboxError({
            operation: "claim",
            cause: "simulated claim failure",
          }),
        ),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({ execute: () => Effect.void }),
    );
    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
      Effect.provide(
        EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
        ),
      ),
    );
    const admission = yield* EffectAdmission.make({
      workers: 1,
      closeAbove: 40,
      samplesToSwitch: 2,
      sampleInterval: Duration.seconds(1),
      pressure: Effect.succeed(Effect.succeed(5)),
      onAllowanceRestored: Effect.void,
      onSlotFreed: Effect.void,
    });
    const runOnce = worker.runOnce.pipe(
      Effect.provideService(EffectAdmission.EffectAdmission, admission),
    );

    const exit = yield* Effect.exit(runOnce);
    assert.isTrue(Exit.isFailure(exit));
    assert.equal((yield* admission.state).inFlight, 0);
    assert.deepEqual(yield* admission.excludedTypes, []);
  }),
);

it.effect("a freed slot wakes one worker, which claims the next turn start", () =>
  Effect.gen(function* () {
    const now = DateTime.formatIso(yield* DateTime.now);
    const workerId = "worker-slot-freed";
    const pending = [1, 2].map((index) =>
      admissionEffect(index, now, workerId, {
        type: "provider-turn.start",
        runId: RunId.make(`run:admission-${index}`),
      }),
    );
    const available = yield* Queue.unbounded<void>();
    const started = yield* Queue.unbounded<string>();
    const finishFirst = yield* Deferred.make<void>();
    const layerOutbox = mockNormalOutbox({
      claimNext: ({ excludeEffectTypes = [] }) =>
        Effect.sync(() => {
          const next = pending.find((effect) => !excludeEffectTypes.includes(effect.request.type));
          if (next === undefined) return Option.none();
          pending.splice(pending.indexOf(next), 1);
          return Option.some(next);
        }),
      nextClaimableAt: Effect.succeedNone,
      nextClaimableAtExcluding: (excludeEffectTypes) =>
        Effect.sync(() =>
          pending.some((effect) => !excludeEffectTypes.includes(effect.request.type))
            ? Option.some(DateTime.makeUnsafe(now))
            : Option.none(),
        ),
      awaitAvailable: Queue.take(available),
      notifyAvailable: (count = 1) =>
        Queue.offerAll(
          available,
          Array.from({ length: count }, () => undefined),
        ).pipe(Effect.asVoid),
      get: () => Effect.succeedNone,
      awaitCancellation: () => Effect.never,
      clearCancellation: () => Effect.void,
      succeed: () => Effect.succeed(true),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({
        execute: (effect) =>
          Queue.offer(started, effect.id).pipe(
            Effect.andThen(
              effect.id === "effect:admission-1" ? Deferred.await(finishFirst) : Effect.void,
            ),
          ),
      }),
    );
    const pressure = yield* Ref.make(60);
    // Two workers, one slot while saturated: the second start waits for the first to finish.
    const daemon = yield* EffectWorker.runDaemonWithOptions({
      concurrency: 2,
      admission: { delayAboveMs: 40 },
      pressure: Effect.succeed(Ref.get(pressure)),
      livenessPollIntervalMs: 60_000,
    }).pipe(
      Effect.provide(
        EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
        ),
      ),
      Effect.forkChild,
    );
    // Both starts are claimed while the gate is open; that arms the source.
    yield* Queue.offerAll(available, [undefined, undefined]);
    const first = yield* startsWithin(started, 2, "the first two turn starts");
    assert.deepEqual([...first].toSorted(), ["effect:admission-1", "effect:admission-2"]);
    // Two saturated samples close the gate to one slot; a third start then waits for a slot.
    yield* TestClock.adjust("2 seconds");
    pending.push(
      admissionEffect(3, now, workerId, {
        type: "provider-turn.start",
        runId: RunId.make("run:admission-3"),
      }),
    );
    yield* Queue.offerAll(available, [undefined]);
    yield* TestClock.adjust("5 seconds");
    assert.equal(pending.length, 1);
    // The first start finishes: its slot frees and wakes one worker, which claims the third.
    yield* Deferred.succeed(finishFirst, undefined);
    assert.deepEqual(yield* startsWithin(started, 1, "the third turn start"), [
      "effect:admission-3",
    ]);
    yield* Fiber.interrupt(daemon);
  }),
);

it.effect("an empty claim gives its slot back without waking a worker", () =>
  Effect.gen(function* () {
    const workerId = "worker-empty-claim";
    const available = yield* Queue.unbounded<void>();
    const claims = yield* Ref.make(0);
    const wakeUps = yield* Ref.make(0);
    const layerOutbox = mockNormalOutbox({
      claimNext: () => Ref.update(claims, (count) => count + 1).pipe(Effect.as(Option.none())),
      nextClaimableAt: Effect.succeedNone,
      nextClaimableAtExcluding: () => Effect.succeedNone,
      awaitAvailable: Queue.take(available),
      notifyAvailable: (count = 1) =>
        Ref.update(wakeUps, (total) => total + count).pipe(
          Effect.andThen(
            Queue.offerAll(
              available,
              Array.from({ length: count }, () => undefined),
            ),
          ),
          Effect.asVoid,
        ),
    });
    const layerExecutor = Layer.succeed(
      EffectWorker.OrchestrationEffectExecutorV2,
      EffectWorker.OrchestrationEffectExecutorV2.of({ execute: () => Effect.void }),
    );
    // One worker, so its own slot is the whole allowance: a wake-up with nothing to claim must end
    // in the liveness sleep, not in the slot's return waking the worker again without end.
    const daemon = yield* EffectWorker.runDaemonWithOptions({
      concurrency: 1,
      admission: { delayAboveMs: 40 },
      pressure: Effect.succeed(Effect.succeed(5)),
      livenessPollIntervalMs: 60_000,
    }).pipe(
      Effect.provide(
        EffectWorker.layerWithOptions({ workerId }).pipe(
          Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
        ),
      ),
      Effect.forkChild,
    );
    yield* Queue.offer(available, undefined);
    yield* TestClock.adjust("3 seconds");
    // The worker's first probe and the one the wake-up caused, and no wake-up of its own making.
    assert.equal(yield* Ref.get(claims), 2);
    assert.equal(yield* Ref.get(wakeUps), 0);
    yield* Fiber.interrupt(daemon);
  }),
);

it.effect(
  "an ungated claim frees its slot for a skipped worker; an empty claim returns it unused",
  () =>
    Effect.gen(function* () {
      const now = DateTime.formatIso(yield* DateTime.now);
      const workerId = "worker-ungated-claim";
      const pending = [
        admissionEffect(1, now, workerId, {
          type: "provider-turn.interrupt",
          providerSessionId: oldSessionId,
          providerThreadId,
          providerTurnId,
        }),
      ];
      const freed = yield* Ref.make(0);
      const layerOutbox = mockNormalOutbox({
        claimNext: () => Effect.sync(() => Option.fromUndefinedOr(pending.shift())),
        get: () => Effect.succeedNone,
        awaitCancellation: () => Effect.never,
        clearCancellation: () => Effect.void,
        succeed: () => Effect.succeed(true),
      });
      const layerExecutor = Layer.succeed(
        EffectWorker.OrchestrationEffectExecutorV2,
        EffectWorker.OrchestrationEffectExecutorV2.of({ execute: () => Effect.void }),
      );
      const worker = yield* EffectWorker.OrchestrationEffectWorkerV2.pipe(
        Effect.provide(
          EffectWorker.layerWithOptions({ workerId }).pipe(
            Layer.provide(Layer.merge(layerOutbox, layerExecutor)),
          ),
        ),
      );
      // One worker, so its slot is the whole allowance: a worker refused the slot while this claim
      // was out skipped the turn starts, and the ungated effect's claim must wake it for them.
      const admission = yield* EffectAdmission.make({
        workers: 1,
        closeAbove: 40,
        samplesToSwitch: 2,
        sampleInterval: Duration.seconds(1),
        pressure: Effect.succeed(Effect.succeed(5)),
        onAllowanceRestored: Effect.void,
        onSlotFreed: Ref.update(freed, (count) => count + 1),
      });
      const runOnce = worker.runOnce.pipe(
        Effect.provideService(EffectAdmission.EffectAdmission, admission),
      );
      assert.isTrue(yield* runOnce);
      assert.equal(yield* Ref.get(freed), 1);
      // A claim that finds nothing returns the slot unused: nobody was skipped for work it holds.
      assert.isFalse(yield* runOnce);
      assert.equal(yield* Ref.get(freed), 1);
      assert.deepEqual(yield* admission.state, {
        allowed: 1,
        inFlight: 0,
        saturated: false,
        sampling: false,
      });
    }),
);

import { CheckpointScopeId, ProjectId, RunId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as FiberSet from "effect/FiberSet";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as WorkspaceEntries from "../workspace/WorkspaceEntries.ts";
import * as CheckpointCapture from "./CheckpointCaptureService.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

export class RunFinalizationError extends Schema.TaggedError<RunFinalizationError>()(
  "RunFinalizationError",
  {
    threadId: ThreadId,
    runId: RunId,
    scopeId: CheckpointScopeId,
    operation: Schema.Literals(["capture-checkpoint", "refresh-workspace"]),
    cause: Schema.Defect(),
  },
) {}

export class RunFinalizationRefreshError extends Schema.TaggedError<RunFinalizationRefreshError>()(
  "RunFinalizationRefreshError",
  { cwd: Schema.String, cause: Schema.Defect() },
) {}

export class RunFinalizationObserver extends Context.Reference<{
  /** Requests the project's pull request refresh after a turn; returns without waiting for it. */
  readonly refreshAfterTurn: (projectId: ProjectId) => Effect.Effect<void>;
  readonly refresh: (input: {
    readonly cwd: string;
    readonly threadId: ThreadId;
    readonly runId: RunId;
  }) => Effect.Effect<void, RunFinalizationRefreshError>;
}>("t3/orchestration-v2/RunFinalizationObserver", {
  defaultValue: () => ({ refresh: () => Effect.void, refreshAfterTurn: () => Effect.void }),
}) {}

export class RunFinalizationService extends Context.Service<
  RunFinalizationService,
  {
    readonly finalize: (input: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly scopeId: CheckpointScopeId;
    }) => Effect.Effect<void, RunFinalizationError>;
  }
>()("t3/orchestration-v2/RunFinalizationService") {}

const make = Effect.gen(function* () {
  const checkpointCapture = yield* CheckpointCapture.CheckpointCaptureServiceV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const observer = yield* RunFinalizationObserver;

  const finalize: RunFinalizationService["Service"]["finalize"] = Effect.fn(
    "RunFinalizationService.finalize",
  )(function* (input) {
    yield* checkpointCapture
      .execute(input)
      .pipe(
        Effect.mapError(
          (cause) => new RunFinalizationError({ ...input, operation: "capture-checkpoint", cause }),
        ),
      );
    const projection = yield* projections
      .getCheckpointContext(input.threadId)
      .pipe(
        Effect.mapError(
          (cause) => new RunFinalizationError({ ...input, operation: "refresh-workspace", cause }),
        ),
      );
    // Where the run worked; its scope keeps the spelling its workspace was first reached by.
    const cwd =
      projection.runs.find((run) => run.id === input.runId)?.workspaceCwd ??
      projection.checkpointScopes.find((scope) => scope.id === input.scopeId)?.cwd;
    if (cwd !== undefined) {
      // Retrying a display refresh after the checkpoint is durable blocks the next turn.
      yield* observer.refresh({ cwd, threadId: input.threadId, runId: input.runId }).pipe(
        Effect.catchTags({
          RunFinalizationRefreshError: (cause) =>
            Effect.logWarning("Failed to refresh workspace after checkpoint capture", {
              ...input,
              cause,
            }),
        }),
      );
    }
  });
  return RunFinalizationService.of({ finalize });
});

export const layer = Layer.effect(RunFinalizationService, make);

// Projects whose after-turn pull request refreshes may run at once; the rest wait their turn.
const AFTER_TURN_REFRESH_SLOTS = 4;

export const layerObserver = Layer.effect(
  RunFinalizationObserver,
  Effect.gen(function* () {
    const workspaceEntries = yield* WorkspaceEntries.WorkspaceEntries;
    const vcsStatus = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const pullRequests = yield* PullRequestService.PullRequestService;
    const pullRequestLookups = yield* FiberMap.make<string>();
    // The refresh waits for the project's forge reads (GitLab lookups take tens of seconds behind
    // the CLI permits), and a run is not over for its thread until its finalization returns, so it
    // runs in the background, interrupted at shutdown. One per project at a time: turns ending
    // while it runs coalesce into one more pass, which starts after their runs committed.
    const afterTurnRefreshes = yield* FiberSet.make<void>();
    const afterTurnRefreshSlots = yield* Semaphore.make(AFTER_TURN_REFRESH_SLOTS);
    const afterTurnRefreshStates = new Map<ProjectId, "queued" | "running" | "rerun">();
    const refreshProject = (projectId: ProjectId): Effect.Effect<void> =>
      Effect.sync(() => afterTurnRefreshStates.set(projectId, "running")).pipe(
        Effect.andThen(pullRequests.refreshAfterTurn(projectId)),
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterruptsOnly(cause),
          (cause) =>
            Effect.logWarning("failed to refresh pull requests after a turn", {
              projectId,
              cause: Cause.pretty(cause),
            }),
        ),
        afterTurnRefreshSlots.withPermits(1),
        Effect.andThen(
          Effect.suspend(() => {
            if (afterTurnRefreshStates.get(projectId) === "rerun") return refreshProject(projectId);
            afterTurnRefreshStates.delete(projectId);
            return Effect.void;
          }),
        ),
      );
    return {
      refreshAfterTurn: Effect.fn("RunFinalizationObserver.refreshAfterTurn")(function* (
        projectId: ProjectId,
      ) {
        const state = afterTurnRefreshStates.get(projectId);
        if (state === "running") afterTurnRefreshStates.set(projectId, "rerun");
        if (state !== undefined) return;
        afterTurnRefreshStates.set(projectId, "queued");
        yield* refreshProject(projectId).pipe(
          // A refresh interrupted on its own (not at shutdown) must not hold the project.
          Effect.onInterrupt(() => Effect.sync(() => afterTurnRefreshStates.delete(projectId))),
          FiberSet.run(afterTurnRefreshes),
        );
      }),
      refresh: Effect.fn("RunFinalizationService.refresh")(
        function* ({ cwd, threadId, runId }) {
          const [, local] = yield* Effect.all(
            [workspaceEntries.refresh(cwd), vcsStatus.refreshLocalStatus(cwd)],
            { concurrency: "unbounded" },
          );
          if (local.refName === null || local.isDefaultRef) return;
          const thread = yield* projections.getThreadShell(threadId);
          if (!thread || thread.branch !== local.refName) return;
          if (thread.activeRunId !== null && thread.activeRunId !== runId) return;
          // The forge CLI can take minutes, so the next turn must not wait for it. A newer
          // turn's lookup replaces a running one instead of queueing behind the cwd's lock.
          yield* vcsStatus.refreshPullRequestStatus(cwd).pipe(
            Effect.catch((error) =>
              Effect.logWarning("failed to refresh pull request status after run completion", {
                threadId,
                cwd,
                detail: error.message,
              }),
            ),
            FiberMap.run(pullRequestLookups, cwd),
          );
        },
        (effect, { cwd }) =>
          effect.pipe(Effect.mapError((cause) => new RunFinalizationRefreshError({ cwd, cause }))),
      ),
    };
  }),
);

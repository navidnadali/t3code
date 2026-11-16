import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  CheckpointScopeId,
  NodeId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2CheckpointScope,
  VcsProcessExitError,
  VcsProcessTimeoutError,
  VcsUnsupportedOperationError,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as CheckpointService from "./CheckpointService.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";

// The scope choice's path lookups, counted.
const realpathCalls = vi.hoisted(() => ({ paths: [] as Array<string> }));
vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  const realpathSync = Object.assign(
    (...args: Parameters<typeof actual.realpathSync>) => {
      realpathCalls.paths.push(String(args[0]));
      return actual.realpathSync(...args);
    },
    { native: actual.realpathSync.native },
  );
  return { ...actual, realpathSync, default: { ...actual, realpathSync } };
});

it.effect(
  "skips unsupported baseline and turn checkpoints without swallowing capture failures",
  () =>
    Effect.gen(function* () {
      const scope: OrchestrationV2CheckpointScope = {
        id: CheckpointScopeId.make("checkpoint-scope:capture-failure"),
        threadId: ThreadId.make("thread:capture-failure"),
        runId: RunId.make("run:capture-failure:1"),
        nodeId: NodeId.make("node:capture-failure:1"),
        parentScopeId: null,
        providerThreadId: ProviderThreadId.make("provider-thread:capture-failure"),
        kind: "root_run",
        ordinalWithinParent: 0,
        advancesAppRunCount: true,
        cwd: "/repo/ignored",
        createdAt: DateTime.makeUnsafe("2026-07-28T00:00:00.000Z"),
      };
      for (const captureError of [
        new VcsProcessTimeoutError({
          operation: "test.capture",
          command: "git",
          cwd: scope.cwd,
          timeoutMs: 30000,
        }),
        new VcsUnsupportedOperationError({
          operation: "test.capture",
          kind: "git",
          detail: "The workspace directory is ignored by its Git repository.",
        }),
      ]) {
        const layerTest = CheckpointService.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              IdAllocator.layer,
              NodeServices.layer,
              Layer.mock(CheckpointStore.CheckpointStore)({
                isGitRepository: () => Effect.succeed(true),
                hasCheckpointRef: () => Effect.succeed(false),
                captureCheckpoint: () => Effect.fail(captureError),
              }),
            ),
          ),
          Layer.provideMerge(NodeCrypto.layer),
        );
        yield* Effect.gen(function* () {
          const checkpoints = yield* CheckpointService.CheckpointServiceV2;
          const baseline = yield* Effect.result(
            checkpoints.captureBaseline({ scope, ordinalWithinScope: 0 }),
          );
          const checkpoint = yield* checkpoints.capture({
            scope,
            ordinalWithinScope: 1,
            runId: scope.runId!,
            nodeId: scope.nodeId!,
            appRunOrdinal: 1,
            capturedAt: scope.createdAt,
          });

          assert.deepEqual(
            { baseline: baseline._tag, turn: checkpoint.status, files: checkpoint.files },
            captureError._tag === "VcsUnsupportedOperationError"
              ? { baseline: "Success", turn: "missing", files: [] }
              : { baseline: "Failure", turn: "error", files: [] },
          );
          if (baseline._tag === "Failure") {
            assert.strictEqual(baseline.failure.cause, captureError);
          } else {
            const materialized = yield* checkpoints.materializeBaselineCheckpoint({
              scope,
              ordinalWithinScope: 0,
              nodeId: scope.nodeId,
              capturedAt: scope.createdAt,
            });
            assert.strictEqual(materialized.status, "missing");
          }
        }).pipe(Effect.provide(layerTest));
      }
    }),
);

// A worktree whose stored spelling was already its real path when the scope was created is that
// workspace without a lookup: no file system call on the command path (a hung mount would freeze it).
it.effect("chooses a scope created at a real path without a file system lookup", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:scope-fast-path");
    const scope = (cwd: string, realCwd: string | undefined): OrchestrationV2CheckpointScope => ({
      id: CheckpointScopeId.make("checkpoint-scope:thread:thread%3Ascope-fast-path:name:root"),
      threadId,
      runId: RunId.make("run:scope-fast-path:1"),
      nodeId: NodeId.make("node:scope-fast-path:1"),
      parentScopeId: null,
      providerThreadId: ProviderThreadId.make("provider-thread:scope-fast-path"),
      kind: "root_run",
      ordinalWithinParent: 0,
      advancesAppRunCount: true,
      cwd,
      ...(realCwd === undefined ? {} : { realCwd }),
      createdAt: DateTime.makeUnsafe("2026-07-28T00:00:00.000Z"),
    });
    const prepare = (cwd: string, threadScopes: ReadonlyArray<OrchestrationV2CheckpointScope>) =>
      Effect.gen(function* () {
        const checkpoints = yield* CheckpointService.CheckpointServiceV2;
        realpathCalls.paths.length = 0;
        const prepared = yield* checkpoints.prepareRootRunScope({
          threadId,
          runId: RunId.make("run:scope-fast-path:2"),
          rootNodeId: NodeId.make("node:scope-fast-path:2"),
          providerThreadId: ProviderThreadId.make("provider-thread:scope-fast-path"),
          cwd,
          createdAt: DateTime.makeUnsafe("2026-07-28T00:00:01.000Z"),
          threadScopes,
        });
        return { prepared, lookups: [...realpathCalls.paths] };
      });
    const layer = CheckpointService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          IdAllocator.layer,
          NodeServices.layer,
          Layer.mock(CheckpointStore.CheckpointStore)({}),
        ),
      ),
      Layer.provideMerge(NodeCrypto.layer),
    );
    yield* Effect.gen(function* () {
      const real = yield* prepare("/worktrees/real", [scope("/worktrees/real", "/worktrees/real")]);
      assert.deepEqual(real.lookups, []);
      assert.equal(real.prepared.id, scope("/worktrees/real", "/worktrees/real").id);
      // A stored spelling that was not its real path, or another spelling, is looked up.
      const linked = yield* prepare("/worktrees/link", [
        scope("/worktrees/link", "/worktrees/real"),
      ]);
      assert.isAbove(linked.lookups.length, 0);
      const other = yield* prepare("/worktrees/other", [
        scope("/worktrees/real", "/worktrees/real"),
      ]);
      assert.isAbove(other.lookups.length, 0);
    }).pipe(Effect.provide(layer));
  }),
);

// A removed worktree reached through a symlinked folder is scoped where git recreates it, as the
// restore check places it: by its nearest folder's real path, not by the spelling the link may move.
it.effect("scopes a removed worktree under a symlinked folder at its real location", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const base = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-scope-missing-" });
      yield* fileSystem.makeDirectory(path.join(base, "real"));
      yield* fileSystem.symlink(path.join(base, "real"), path.join(base, "link"));
      const spelling = path.join(base, "link", "repo", "feature");
      const real = yield* CheckpointService.resolveWorkspacePath(fileSystem, path, spelling);
      const threadId = ThreadId.make("thread:scope-missing");
      const rootScope: OrchestrationV2CheckpointScope = {
        id: CheckpointScopeId.make("checkpoint-scope:thread:thread%3Ascope-missing:name:root"),
        threadId,
        runId: RunId.make("run:scope-missing:1"),
        nodeId: NodeId.make("node:scope-missing:1"),
        parentScopeId: null,
        providerThreadId: ProviderThreadId.make("provider-thread:scope-missing"),
        kind: "root_run",
        ordinalWithinParent: 0,
        advancesAppRunCount: true,
        cwd: spelling,
        realCwd: real,
        createdAt: DateTime.makeUnsafe("2026-07-28T00:00:00.000Z"),
      };
      const prepare = (threadScopes: ReadonlyArray<OrchestrationV2CheckpointScope>) =>
        Effect.gen(function* () {
          const checkpoints = yield* CheckpointService.CheckpointServiceV2;
          return yield* checkpoints.prepareRootRunScope({
            threadId,
            runId: RunId.make("run:scope-missing:2"),
            rootNodeId: NodeId.make("node:scope-missing:2"),
            providerThreadId: ProviderThreadId.make("provider-thread:scope-missing"),
            cwd: spelling,
            createdAt: DateTime.makeUnsafe("2026-07-28T00:00:01.000Z"),
            threadScopes,
          });
        }).pipe(
          Effect.provide(
            CheckpointService.layer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  IdAllocator.layer,
                  NodeServices.layer,
                  Layer.mock(CheckpointStore.CheckpointStore)({}),
                ),
              ),
              Layer.provideMerge(NodeCrypto.layer),
            ),
          ),
        );

      assert.notEqual(real, spelling);
      const first = yield* prepare([]);
      assert.deepEqual([first.cwd, first.realCwd], [spelling, real]);
      // The scope it was recorded in while the worktree existed is the same workspace.
      assert.equal((yield* prepare([rootScope])).id, rootScope.id);
    }).pipe(Effect.provide(NodeServices.layer)),
  ),
);

it.effect(
  "bounds a slow baseline snapshot but waits for an in-flight capture of the same ref",
  () =>
    Effect.gen(function* () {
      const scope: OrchestrationV2CheckpointScope = {
        id: CheckpointScopeId.make("checkpoint-scope:slow-baseline"),
        threadId: ThreadId.make("thread:slow-baseline"),
        runId: RunId.make("run:slow-baseline:1"),
        nodeId: NodeId.make("node:slow-baseline:1"),
        parentScopeId: null,
        providerThreadId: ProviderThreadId.make("provider-thread:slow-baseline"),
        kind: "root_run",
        ordinalWithinParent: 0,
        advancesAppRunCount: true,
        cwd: "/repo/slow",
        createdAt: DateTime.makeUnsafe("2026-07-28T00:00:00.000Z"),
      };
      const refs = yield* Ref.make<ReadonlySet<string>>(new Set());
      const snapshots = yield* Ref.make(0);
      const firstSnapshotStarted = yield* Deferred.make<void>();
      const layerTest = CheckpointService.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            IdAllocator.layer,
            NodeServices.layer,
            Layer.mock(CheckpointStore.CheckpointStore)({
              isGitRepository: () => Effect.succeed(true),
              hasCheckpointRef: ({ checkpointRef }) =>
                Ref.get(refs).pipe(Effect.map((current) => current.has(checkpointRef))),
              captureCheckpoint: ({ checkpointRef }) =>
                Effect.gen(function* () {
                  yield* Ref.update(snapshots, (count) => count + 1);
                  yield* Deferred.succeed(firstSnapshotStarted, undefined);
                  yield* Effect.sleep("10 seconds");
                  yield* Ref.update(refs, (current) => new Set([...current, checkpointRef]));
                }),
            }),
          ),
        ),
        Layer.provideMerge(NodeCrypto.layer),
      );

      yield* Effect.gen(function* () {
        const checkpoints = yield* CheckpointService.CheckpointServiceV2;
        // A stopped run's post-turn capture writes the next run's baseline ref while holding the lock.
        const postTurn = yield* checkpoints
          .capture({
            scope,
            ordinalWithinScope: 1,
            runId: scope.runId!,
            nodeId: scope.nodeId!,
            appRunOrdinal: 1,
            capturedAt: scope.createdAt,
          })
          .pipe(Effect.forkChild);
        yield* Deferred.await(firstSnapshotStarted);
        const waitingBaseline = yield* checkpoints
          .captureBaseline({ scope, ordinalWithinScope: 1 })
          .pipe(Effect.result, Effect.forkChild);
        yield* TestClock.adjust("10 seconds");
        assert.strictEqual((yield* Fiber.join(postTurn)).status, "ready");
        assert.strictEqual((yield* Fiber.join(waitingBaseline))._tag, "Success");

        const slowBaseline = yield* checkpoints
          .captureBaseline({ scope, ordinalWithinScope: 2 })
          .pipe(Effect.result, Effect.forkChild);
        yield* TestClock.adjust("5 seconds");
        assert.strictEqual((yield* Fiber.join(slowBaseline))._tag, "Failure");
        assert.strictEqual(yield* Ref.get(snapshots), 2);
        assert.strictEqual((yield* Ref.get(refs)).size, 1);
      }).pipe(Effect.provide(layerTest));
    }),
);

it.effect("records a repository probe failure as an error checkpoint, not a missing one", () =>
  Effect.gen(function* () {
    const scope: OrchestrationV2CheckpointScope = {
      id: CheckpointScopeId.make("checkpoint-scope:probe-failure"),
      threadId: ThreadId.make("thread:probe-failure"),
      runId: RunId.make("run:probe-failure:1"),
      nodeId: NodeId.make("node:probe-failure:1"),
      parentScopeId: null,
      providerThreadId: ProviderThreadId.make("provider-thread:probe-failure"),
      kind: "root_run",
      ordinalWithinParent: 0,
      advancesAppRunCount: true,
      cwd: "/repo/bad-config",
      createdAt: DateTime.makeUnsafe("2026-07-28T00:00:00.000Z"),
    };
    const layerTest = CheckpointService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          IdAllocator.layer,
          NodeServices.layer,
          Layer.mock(CheckpointStore.CheckpointStore)({
            isGitRepository: () =>
              Effect.fail(
                new VcsProcessExitError({
                  operation: "GitVcsDriver.detectRepository",
                  command: "git",
                  cwd: scope.cwd,
                  exitCode: 128,
                  detail: "fatal: bad config line 1 in file .git/config",
                }),
              ),
          }),
        ),
      ),
      Layer.provideMerge(NodeCrypto.layer),
    );

    const statuses = yield* Effect.gen(function* () {
      const checkpoints = yield* CheckpointService.CheckpointServiceV2;
      const baseline = yield* checkpoints.materializeBaselineCheckpoint({
        scope,
        ordinalWithinScope: 0,
        nodeId: scope.nodeId,
        capturedAt: scope.createdAt,
      });
      const turn = yield* checkpoints.capture({
        scope,
        ordinalWithinScope: 1,
        runId: scope.runId!,
        nodeId: scope.nodeId!,
        appRunOrdinal: 1,
        capturedAt: scope.createdAt,
      });
      return { baseline: baseline.status, turn: turn.status };
    }).pipe(Effect.provide(layerTest));

    assert.deepEqual(statuses, { baseline: "error", turn: "error" });
  }),
);

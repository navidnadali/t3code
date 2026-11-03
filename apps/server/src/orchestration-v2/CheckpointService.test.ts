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
  VcsProcessTimeoutError,
  VcsUnsupportedOperationError,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

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

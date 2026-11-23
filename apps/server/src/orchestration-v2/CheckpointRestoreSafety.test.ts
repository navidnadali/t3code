import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointScopeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import * as CheckpointRollbackService from "./CheckpointRollbackService.ts";
import {
  anotherWorktreeRestoreMessage,
  checkpointRestorePlace,
  isCheckpointRestoreIsolated,
  UNPLACED_RESTORE_MESSAGE,
} from "./CheckpointRestoreSafety.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";
import { CheckpointServiceV2 } from "./CheckpointService.ts";
import { EventSinkV2 } from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { ProjectionStoreV2, type ProjectionCheckpointRollbackContext } from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { RuntimePolicyV2 } from "./RuntimePolicy.ts";
import { layerNoTaskFolders } from "./ProviderTurnStartService.testkit.ts";

it.effect.each([
  "nested",
  "ancestor",
  "provider",
  "sibling",
  "shared-provider",
  "conversation",
  "unplaced",
  "recorded-run",
] as const)("preserves overlapping workspace files, owner=%s", (owner) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-v2-restore-" });
    const cwd = path.join(parent, "worktree");
    const nested = path.join(cwd, "nested");
    const sibling = path.join(parent, "worktree2");
    yield* fs.makeDirectory(nested, { recursive: true });
    yield* fs.makeDirectory(sibling);
    const otherFile = path.join(nested, "other-thread.txt");
    yield* fs.writeFileString(otherFile, "other thread's uncommitted work");
    const threadId = ThreadId.make("restore-current");
    const otherId = ThreadId.make("restore-other");
    const projectId = ProjectId.make("restore-project");
    const providerThreadId = ProviderThreadId.make("restore-provider");
    const providerSessionId = ProviderSessionId.make("restore-session");
    const instanceId = ProviderInstanceId.make("restore-instance");
    const checkpointId = CheckpointId.make("restore-checkpoint");
    const scopeId = CheckpointScopeId.make("restore-scope");
    const calls: string[] = [];
    const providerThread = {
      id: providerThreadId,
      providerSessionId,
      providerInstanceId: instanceId,
    };
    const otherThread = {
      id: otherId,
      projectId,
      deletedAt: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: otherId },
      worktreePath:
        owner === "ancestor" || owner === "conversation"
          ? parent
          : owner === "nested"
            ? nested
            : sibling,
    };
    const projection = {
      thread: {
        id: threadId,
        projectId,
        worktreePath: cwd,
        activeProviderThreadId: providerThreadId,
        modelSelection: { instanceId, model: "test" },
        deletedAt: null,
      },
      providerThread,
      providerSession: undefined,
      providerTurns: [],
      rootNodes: [],
      targetAttempt: undefined,
      rolledBackAttemptIds: [],
      staleCheckpoints: [],
      otherRootScopes: [],
      checkpoint: { id: checkpointId, scopeId, status: "ready", appRunOrdinal: null },
      scope: { id: scopeId, cwd },
      runsToRollback: [{ id: "later-run", ordinal: 1, status: "completed", rootNodeId: null }],
    } as unknown as ProjectionCheckpointRollbackContext;
    const layerTest = CheckpointRollbackService.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          NodeServices.layer,
          Layer.mock(ServerConfig.ServerConfig)({ worktreesDir: "/synthetic-worktrees" } as never),
          ServerSettings.layerTest(),
          ThreadCommandExecutor.layer,
          IdAllocator.layer,
          layerNoTaskFolders,
          Layer.mock(GitWorkflowService.GitWorkflowService)({}),
          Layer.mock(ProjectStore.ProjectStoreV2)({
            get: () => Effect.succeedSome({ workspaceRoot: parent } as never),
          }),
          Layer.mock(ProjectionStoreV2)({
            // "unplaced": recorded before the upgrade, by a thread that ran in two worktrees.
            getCheckpointWorkspaces: () =>
              Effect.succeed(owner === "unplaced" ? [sibling, cwd] : [cwd]),
            getThread: () => Effect.succeed(projection.thread),
            getCheckpointRollbackContext: () => Effect.succeed(projection),
            getShellSnapshot: () =>
              Effect.succeed({
                schemaVersion: 1,
                snapshotSequence: 0,
                threads: [otherThread] as never,
                archivedThreads: [],
              }),
            // "recorded-run": the other thread's scope keeps a spelling that is gone (a removed
            // symlink); its run recorded the path it worked in, this worktree.
            getCheckpointContext: () =>
              Effect.succeed(
                owner === "recorded-run"
                  ? ({
                      runs: [
                        { id: "other-run", ordinal: 1, status: "completed", workspaceCwd: cwd },
                      ],
                      checkpoints: [],
                      checkpointScopes: [
                        {
                          id: "other-scope",
                          runId: "other-run",
                          kind: "root_run",
                          cwd: "/gone-link",
                        },
                      ],
                    } as never)
                  : { runs: [], checkpoints: [], checkpointScopes: [] },
              ),
            getThreadProviderContext: () =>
              Effect.succeed({
                thread: otherThread as never,
                providerThreads: [],
                providerSessions:
                  owner === "provider" || owner === "shared-provider"
                    ? ([
                        {
                          cwd: owner === "shared-provider" ? cwd : nested,
                          status: "running",
                          capabilities: {
                            sessions: {
                              supportsMultipleProviderThreadsPerSession:
                                owner === "shared-provider",
                            },
                          },
                        },
                      ] as never)
                    : [],
              }),
          }),
          Layer.mock(CheckpointServiceV2)({
            discardRepositorySnapshots: () => Effect.void,
            restore: () =>
              Effect.gen(function* () {
                calls.push("files");
                yield* fs.remove(otherFile).pipe(Effect.orDie);
              }),
          }),
          Layer.mock(EventSinkV2)({ write: () => Effect.succeed([]) }),
          Layer.mock(ProviderSessionManagerV2)({
            open: () =>
              Effect.succeed({
                rollbackThread: () =>
                  Effect.sync(() => {
                    calls.push("provider");
                    return { providerThread };
                  }),
              } as never),
          }),
          Layer.mock(RuntimePolicyV2)({ resolve: () => Effect.succeed({} as never) }),
        ),
      ),
    );
    const service = yield* CheckpointRollbackService.CheckpointRollbackServiceV2.pipe(
      Effect.provide(layerTest),
    );
    const restoreFiles = owner !== "conversation";
    const rejected = !["sibling", "shared-provider", "conversation", "unplaced"].includes(owner);
    if (owner === "unplaced") {
      const error = yield* service
        .execute({ threadId, providerThreadId, checkpointId, scopeId, restoreFiles })
        .pipe(Effect.flip);
      assert.equal(error.reason, "another-worktree");
      assert.include(error.message, "can't tell which worktree");
      assert.deepEqual(calls, []);
    } else if (rejected) {
      const error = yield* service
        .execute({ threadId, providerThreadId, checkpointId, scopeId, restoreFiles })
        .pipe(Effect.flip);
      assert.equal(error.reason, "shared-workspace");
      assert.deepEqual(calls, []);
      assert.equal(yield* fs.readFileString(otherFile), "other thread's uncommitted work");
    } else {
      yield* service.execute({ threadId, providerThreadId, checkpointId, scopeId, restoreFiles });
      assert.deepEqual(calls, restoreFiles ? ["provider", "files"] : ["provider"]);
      assert.equal(yield* fs.exists(otherFile), !restoreFiles);
    }
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("lets rollback recreate a worktree storage cleanup removed", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-v2-restore-removed-" });
    const worktreePath = path.join(parent, "worktree");
    const isolated = yield* isCheckpointRestoreIsolated(
      { id: ThreadId.make("restore-removed"), worktreePath },
      { cwd: worktreePath },
      null,
      {
        fileSystem,
        path,
        projections: {
          getShellSnapshot: () => Effect.succeed({ threads: [], archivedThreads: [] }),
        } as never,
        projects: {} as never,
      },
    );
    assert.isTrue(isolated);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

// Storage cleanup removed the worktree (and its repository folder); the checkpoint recorded its real
// path. The thread reaches it through a symlinked folder: the same worktree, unless the link moved.
it.effect.each([
  { spelling: "at its real path", link: "none", placed: true },
  { spelling: "through a symlinked folder", link: "worktrees", placed: true },
  { spelling: "through a symlinked folder pointed elsewhere", link: "elsewhere", placed: false },
] as const)("places a removed worktree reached $spelling: $placed", ({ link, placed }) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = yield* fileSystem.realPath(
      yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-v2-restore-removed-link-" }),
    );
    const worktrees = path.join(parent, "worktrees");
    const elsewhere = path.join(parent, "elsewhere");
    yield* fileSystem.makeDirectory(worktrees);
    yield* fileSystem.makeDirectory(elsewhere);
    const linked = path.join(parent, "link");
    if (link !== "none") yield* fileSystem.symlink(path.join(parent, link), linked);
    const recorded = path.join(worktrees, "repo", "feature");
    const thread = {
      id: ThreadId.make("restore-removed-link"),
      worktreePath: link === "none" ? recorded : path.join(linked, "repo", "feature"),
    };
    const place = yield* checkpointRestorePlace(
      thread,
      CheckpointId.make("restore-removed-link-checkpoint"),
      {
        fileSystem,
        path,
        projections: { getCheckpointWorkspaces: () => Effect.succeed([recorded]) },
      },
    );
    if (!placed) {
      assert.deepEqual(place, { refusal: anotherWorktreeRestoreMessage(recorded) });
      return;
    }
    assert.deepEqual(place, { refusal: null, workspace: recorded });
    const isolated = yield* isCheckpointRestoreIsolated(thread, { cwd: recorded }, null, {
      fileSystem,
      path,
      projections: {
        getShellSnapshot: () => Effect.succeed({ threads: [], archivedThreads: [] }),
      } as never,
      projects: {} as never,
    });
    assert.isTrue(isolated);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("counts a task folder's repository worktrees as part of what a restore changes", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = yield* fileSystem.realPath(
      yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-v2-restore-task-folder-" }),
    );
    // The worktrees directory is reached through a symlink; the other thread holds the real path.
    const real = path.join(parent, "real", "task");
    const named = path.join(parent, "linked", "task");
    yield* fileSystem.makeDirectory(path.join(real, "shop"), { recursive: true });
    yield* fileSystem.makeDirectory(path.join(real, "web"));
    yield* fileSystem.symlink(path.join(parent, "real"), path.join(parent, "linked"));
    const worktreePath = path.join(named, "shop");
    const other = {
      id: ThreadId.make("restore-task-folder-other"),
      projectId: ProjectId.make("restore-project"),
      deletedAt: null,
      worktreePath: path.join(real, "web"),
      activeRunId: null,
      lineage: {},
    };
    const isolated = (repositories: ReadonlyArray<string>) =>
      isCheckpointRestoreIsolated(
        { id: ThreadId.make("restore-task-folder"), worktreePath },
        { cwd: path.join(real, "shop") },
        {
          repositories: repositories.map((folder) => ({
            repositoryPath: "/tracked/web",
            worktreePath: folder,
            name: "web",
          })),
        },
        {
          fileSystem,
          path,
          projections: {
            getShellSnapshot: () => Effect.succeed({ threads: [other], archivedThreads: [] }),
            getCheckpointContext: () => Effect.succeed({ checkpointScopes: [], runs: [] }),
            getThreadProviderContext: () => Effect.succeed({ providerSessions: [] }),
          } as never,
          projects: {} as never,
        },
      );

    assert.isTrue(yield* isolated([]));
    assert.isFalse(yield* isolated([path.join(named, "web")]));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([
  { sideChat: "answering questions", running: false, toolItems: 0, isolated: true },
  { sideChat: "running", running: true, toolItems: 0, isolated: false },
  { sideChat: "after running tools", running: false, toolItems: 1, isolated: false },
] as const)(
  "a side chat $sideChat shares the parent's worktree: $isolated",
  ({ running, toolItems, isolated }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const worktreePath = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-v2-restore-side-chat-",
      });
      const parentId = ThreadId.make("restore-side-chat-parent");
      const sideChat = {
        id: ThreadId.make("restore-side-chat"),
        projectId: ProjectId.make("restore-project"),
        deletedAt: null,
        worktreePath,
        activeRunId: running ? "side-chat-run" : null,
        lineage: {
          parentThreadId: parentId,
          relationshipToParent: "fork",
          rootThreadId: parentId,
          sideChat: true,
        },
      };
      const result = yield* isCheckpointRestoreIsolated(
        { id: parentId, worktreePath },
        { cwd: worktreePath },
        null,
        {
          fileSystem,
          path,
          projections: {
            getShellSnapshot: () => Effect.succeed({ threads: [sideChat], archivedThreads: [] }),
            getThreadRecords: () =>
              Effect.succeed({
                turnItems: Array.from({ length: toolItems }, () => ({ type: "command_execution" })),
              }),
            getCheckpointContext: () => Effect.succeed({ runs: [], checkpointScopes: [] }),
            getThreadProviderContext: () => Effect.succeed({ providerSessions: [] }),
          } as never,
          projects: {} as never,
        },
      );
      assert.equal(result, isolated);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it("names the other worktree in the refusal, a long path shortened in the middle", () => {
  assert.equal(
    anotherWorktreeRestoreMessage("/repo/worktrees/feature"),
    "The files from before this message are in another worktree (/repo/worktrees/feature). Restoring them here would overwrite this worktree's files. Use \u201cRevert and keep changes\u201d to rewind the conversation only.",
  );
  const long = `/Users/someone/${"nested/".repeat(12)}worktrees/feature-with-a-long-name`;
  const shown = /\(([^)]*)\)/.exec(anotherWorktreeRestoreMessage(long))![1]!;
  assert.lengthOf(Array.from(shown), 64);
  assert.isTrue(shown.startsWith("/Users/someone/"));
  assert.isTrue(shown.endsWith("/worktrees/feature-with-a-long-name"));
  assert.include(shown, "\u2026");
});

// One check for admission and the rollback: where the checkpoint's files are, against this worktree.
it.effect.each([
  ["taken here", ["here"], null],
  ["taken in another worktree", ["other"], "another"],
  ["recorded before the upgrade by a thread in two worktrees", ["other", "here"], "unplaced"],
  ["for a thread without a worktree", ["other"], null],
] as const)("refuses a file restore of a checkpoint %s", ([name, workspaces, refusal]) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-v2-restore-refusal-" });
    const places = { here: path.join(parent, "here"), other: path.join(parent, "other") };
    yield* fileSystem.makeDirectory(places.here);
    yield* fileSystem.makeDirectory(places.other);
    const { refusal: result } = yield* checkpointRestorePlace(
      {
        id: ThreadId.make("restore-refusal"),
        worktreePath: name === "for a thread without a worktree" ? null : places.here,
      },
      CheckpointId.make("restore-refusal-checkpoint"),
      {
        fileSystem,
        path,
        projections: {
          getCheckpointWorkspaces: () => Effect.succeed(workspaces.map((place) => places[place])),
        },
      },
    );
    assert.equal(
      result,
      refusal === null
        ? null
        : refusal === "another"
          ? anotherWorktreeRestoreMessage(places.other)
          : UNPLACED_RESTORE_MESSAGE,
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.each([
  { name: "completed child", isolated: true },
  { name: "completed native child", isolated: true },
  { name: "native child with running tool", isolated: false },
  { name: "native child with provider background task", isolated: false },
  { name: "native child with parent background task", isolated: false },
  { name: "native child with another provider parent task", isolated: true },
  { name: "native child grandchild with root background task", isolated: false },
  { name: "missing task record", isolated: false },
  { name: "task belongs to another child", isolated: false },
  { name: "completed child with pending task", isolated: false },
  { name: "grandchild through fork", isolated: false },
  { name: "completed grandchild", isolated: true },
  { name: "unrelated child", isolated: false },
  { name: "running child", isolated: false },
  { name: "held queue", isolated: false },
  { name: "failed child", isolated: false },
  { name: "cyclic ancestry", isolated: false },
] as const)("parent file restore with $name: $isolated", ({ name, isolated }) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const worktreePath = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-restore-child-" });
    const parentId = ThreadId.make("restore-parent");
    const childId = ThreadId.make("restore-child");
    const ancestorId = ThreadId.make("restore-ancestor");
    const child = {
      id: childId,
      projectId: ProjectId.make("restore-project"),
      deletedAt: null,
      worktreePath,
      activeRunId: name === "running child" ? "child-run" : null,
      activityRunStatus: null,
      latestRunId: name.includes("native child") ? null : "child-run",
      creationSource: name.includes("native child") ? "provider" : "user",
      forkedFrom: { type: "node", nodeId: "child-task" },
      status: name.includes("native child")
        ? "idle"
        : name === "running child"
          ? "running"
          : name === "failed child"
            ? "failed"
            : "completed",
      pendingRuntimeRequest: null,
      pendingBackgroundTasks: [],
      lineage: {
        parentThreadId:
          name === "completed grandchild" ||
          name === "native child grandchild with root background task" ||
          name === "grandchild through fork" ||
          name === "cyclic ancestry"
            ? ancestorId
            : name === "unrelated child"
              ? ThreadId.make("another-parent")
              : parentId,
        relationshipToParent: "subagent",
        rootThreadId: parentId,
      },
    };
    const ancestor = {
      ...child,
      id: ancestorId,
      forkedFrom: { type: "node", nodeId: "ancestor-task" },
      lineage: {
        ...child.lineage,
        relationshipToParent: name === "grandchild through fork" ? "fork" : "subagent",
        parentThreadId: name === "cyclic ancestry" ? childId : parentId,
      },
    };
    const children =
      name === "completed grandchild" ||
      name === "native child grandchild with root background task" ||
      name === "grandchild through fork" ||
      name === "cyclic ancestry"
        ? [child, ancestor]
        : [child];
    const projections = {
      getShellSnapshot: () =>
        Effect.succeed({
          threads: children,
          archivedThreads: [],
        }),
      getCheckpointContext: () =>
        Effect.succeed({
          checkpointScopes: [],
          runs: name === "held queue" ? [{ status: "queued" }] : [],
        }),
      getThreadProviderContext: () => Effect.succeed({ providerSessions: [] }),
      getThreadRecords: (
        _threadId: unknown,
        _fields: readonly string[],
        filter: { subagentIds?: string[]; turnItemStatuses?: string[] } = {},
      ) =>
        Effect.succeed({
          providerThreads:
            _threadId === parentId &&
            (name.includes("parent") ||
              name === "native child grandchild with root background task") &&
            name.includes("native child")
              ? [
                  {
                    providerInstanceId:
                      name === "native child with another provider parent task"
                        ? "other-provider"
                        : "claude",
                    status: "idle",
                    pendingBackgroundTasks: [{ taskId: "background", kind: "command" }],
                  },
                ]
              : name === "native child with provider background task"
                ? [{ status: "idle", pendingBackgroundTasks: [{ taskId: "background" }] }]
                : [],
          turnItems:
            filter.turnItemStatuses !== undefined && name === "native child with running tool"
              ? [{ type: "command_execution", status: "running" }]
              : [],
          subagents:
            name === "missing task record"
              ? []
              : [
                  {
                    id: filter.subagentIds?.[0],
                    providerInstanceId: "claude",
                    nativeTaskRef: { nativeId: "native-task" },
                    childThreadId:
                      name === "task belongs to another child"
                        ? "another-child"
                        : filter.subagentIds?.[0] === "ancestor-task"
                          ? ancestorId
                          : childId,
                    origin: name.includes("native child") ? "provider_native" : "app_owned",
                    status: name === "completed child with pending task" ? "running" : "completed",
                  },
                ],
        }),
    };
    const result = yield* isCheckpointRestoreIsolated(
      { id: parentId, worktreePath },
      { cwd: worktreePath },
      null,
      { fileSystem, path, projections: projections as never, projects: {} as never },
    );
    assert.equal(result, isolated);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

import {
  isLiveSubagentShell,
  isProviderNativeSubagentThread,
  type CheckpointId,
  type OrchestrationV2AppThread,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ThreadShell,
  type TaskFolder,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";
import { resolveWorkspacePath } from "./CheckpointService.ts";
import type { ProjectionStoreV2 } from "./ProjectionStore.ts";
import type * as ProjectStore from "./ProjectStore.ts";

const UNFINISHED_RUN_STATUSES = new Set(["queued", "preparing", "starting", "running", "waiting"]);

// The other worktree is named; a long path keeps its start and, longer, its end (the worktree's own
// name), shortened in the middle as the client shortens long names.
const MAX_SHOWN_PATH_CHARACTERS = 64;
const shortenedPath = (value: string) => {
  const characters = Array.from(value);
  if (characters.length <= MAX_SHOWN_PATH_CHARACTERS) return value;
  const suffixLength = Math.ceil((MAX_SHOWN_PATH_CHARACTERS - 1) * 0.6);
  const prefixLength = MAX_SHOWN_PATH_CHARACTERS - 1 - suffixLength;
  return `${characters.slice(0, prefixLength).join("")}…${characters.slice(-suffixLength).join("")}`;
};

// In the revert dialog's terms: "this message" is where the user reverts to.
export const anotherWorktreeRestoreMessage = (workspace: string) =>
  `The files from before this message are in another worktree (${shortenedPath(workspace)}). Restoring them here would overwrite this worktree's files. Use “Revert and keep changes” to rewind the conversation only.`;

export const UNPLACED_RESTORE_MESSAGE =
  "T3 can't tell which worktree the files from before this message are in: this conversation ran in more than one before T3 recorded where each turn ran. Restoring them here could overwrite this worktree's files. Use “Revert and keep changes” to rewind the conversation only.";

// The one check admission and the rollback make before a file restore, which writes the checkpoint's
// tree: refused when it was taken in another worktree, or when that is unknown (recorded before runs
// kept their workspace, by a thread that ran in more than one); else where it was taken, which the
// isolation check and the restore use (an old root scope may name a later workspace).
export const checkpointRestorePlace = Effect.fn("orchestrationV2.checkpointRestorePlace")(
  function* (
    thread: Pick<OrchestrationV2AppThread, "id" | "worktreePath">,
    checkpointId: CheckpointId,
    dependencies: {
      readonly fileSystem: FileSystem.FileSystem;
      readonly path: Path.Path;
      readonly projections: Pick<ProjectionStoreV2["Service"], "getCheckpointWorkspaces">;
    },
  ) {
    // A thread without a worktree is a shared workspace, which the isolation check refuses.
    if (thread.worktreePath == null) return { refusal: null, workspace: undefined };
    const { fileSystem, path, projections } = dependencies;
    const workspaces = yield* projections.getCheckpointWorkspaces(thread.id, checkpointId);
    const places = new Map<string, string>();
    for (const workspace of workspaces) {
      places.set(yield* resolveWorkspacePath(fileSystem, path, workspace), workspace);
    }
    if (places.size > 1) return { refusal: UNPLACED_RESTORE_MESSAGE };
    const here = yield* resolveWorkspacePath(fileSystem, path, thread.worktreePath);
    for (const [place, workspace] of places) {
      return place === here
        ? { refusal: null, workspace }
        : { refusal: anotherWorktreeRestoreMessage(workspace) };
    }
    return { refusal: null, workspace: undefined };
  },
);

export const SHARED_WORKSPACE_RESTORE_MESSAGE =
  "File restore requires an isolated worktree. This workspace may contain changes from another thread or a side chat. Rewind the conversation without restoring files instead, or delete side chats you no longer need.";

// Check isolation at admission and rollback; checkpoints restore whole checkouts, which may become shared.
export const isCheckpointRestoreIsolated = Effect.fn("orchestrationV2.isCheckpointRestoreIsolated")(
  function* (
    thread: Pick<OrchestrationV2AppThread, "id" | "worktreePath">,
    scope: Pick<OrchestrationV2CheckpointScope, "cwd">,
    /** The task folder the thread works in: a file restore reaches its repository worktrees too. */
    taskFolder: Pick<TaskFolder, "repositories"> | null,
    dependencies: {
      readonly fileSystem: FileSystem.FileSystem;
      readonly projections: ProjectionStoreV2["Service"];
      readonly projects: ProjectStore.ProjectStoreV2["Service"];
      readonly path: Path.Path;
    },
  ) {
    const { fileSystem, projections, projects, path } = dependencies;
    const contains = (parent: string, child: string) => {
      const relative = path.relative(parent, child);
      return (
        relative === "" ||
        (!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`))
      );
    };
    const worktreePath = thread.worktreePath;
    let shared = worktreePath == null;
    if (!shared && worktreePath !== null) {
      const resolve = (target: string) => resolveWorkspacePath(fileSystem, path, target);
      const cwd = yield* resolve(scope.cwd);
      const worktreeCwd = yield* resolve(worktreePath);
      shared = cwd !== worktreeCwd;
      if (!shared) {
        const restored = [
          cwd,
          ...(yield* Effect.forEach(taskFolder?.repositories ?? [], (repository) =>
            resolve(repository.worktreePath),
          )),
        ];
        const shell = yield* projections.getShellSnapshot();
        const threads = [...shell.threads, ...shell.archivedThreads];
        const threadsById = new Map(threads.map((candidate) => [candidate.id, candidate]));
        const subagentAncestors = (candidate: OrchestrationV2ThreadShell) => {
          const ancestors: OrchestrationV2AppThread["id"][] = [];
          const seen = new Set<string>();
          let current: OrchestrationV2ThreadShell | undefined = candidate;
          while (current !== undefined && !seen.has(current.id)) {
            seen.add(current.id);
            if (current.lineage.relationshipToParent !== "subagent") return undefined;
            const parentId: OrchestrationV2AppThread["id"] | null = current.lineage.parentThreadId;
            if (parentId === null) return undefined;
            ancestors.push(parentId);
            if (parentId === thread.id) return ancestors;
            current = threadsById.get(parentId);
          }
          return undefined;
        };
        const ancestorProviders = new Map<
          OrchestrationV2AppThread["id"],
          ReadonlyArray<OrchestrationV2ProviderThread>
        >();
        const checkedPaths = new Set<string>();
        for (const otherThread of threads) {
          if (otherThread.id === thread.id || otherThread.deletedAt !== null) continue;
          // An idle side chat that only answered questions never touched the shared worktree.
          if (
            otherThread.lineage.sideChat === true &&
            otherThread.activeRunId === null &&
            otherThread.worktreePath === worktreePath
          ) {
            const { turnItems } = yield* projections.getThreadRecords(
              otherThread.id,
              ["turnItems"],
              {
                turnItemTypes: ["command_execution", "file_change", "dynamic_tool"],
              },
            );
            if (turnItems.length === 0) continue;
          }
          const other = yield* projections.getCheckpointContext(otherThread.id);
          const nativeSubagent = isProviderNativeSubagentThread(otherThread);
          const ancestors = subagentAncestors(otherThread);
          // Native children can be runless; their completion lives on the parent's task record.
          if (
            otherThread.worktreePath === worktreePath &&
            (otherThread.status === "completed" ||
              (nativeSubagent && otherThread.status === "idle" && other.runs.length === 0)) &&
            otherThread.activeRunId === null &&
            !isLiveSubagentShell(otherThread) &&
            ancestors !== undefined &&
            !other.runs.some((run) => UNFINISHED_RUN_STATUSES.has(run.status)) &&
            otherThread.lineage.parentThreadId !== null &&
            otherThread.forkedFrom?.type === "node"
          ) {
            const taskNodeId = otherThread.forkedFrom.nodeId;
            const { subagents } = yield* projections.getThreadRecords(
              otherThread.lineage.parentThreadId,
              ["subagents"],
              { subagentIds: [taskNodeId] },
            );
            const task = subagents.find(
              (candidate) =>
                candidate.id === taskNodeId && candidate.childThreadId === otherThread.id,
            );
            if (
              task?.status === "completed" &&
              task.origin === (nativeSubagent ? "provider_native" : "app_owned")
            ) {
              if (!nativeSubagent) continue;
              // Runless shells omit background work; inspect the raw records before restoring files.
              const { providerThreads, turnItems } = yield* projections.getThreadRecords(
                otherThread.id,
                ["providerThreads", "turnItems"],
                {
                  turnItemTypes: ["command_execution", "file_change", "dynamic_tool", "subagent"],
                  turnItemStatuses: ["pending", "running", "waiting"],
                },
              );
              if (
                turnItems.length === 0 &&
                !providerThreads.some(
                  (providerThread) =>
                    providerThread.status === "active" ||
                    (providerThread.pendingBackgroundTasks?.length ?? 0) > 0,
                )
              ) {
                // Claude can keep a child's unattributed background commands on an ancestor session.
                let pendingAncestorWork = false;
                for (const ancestorId of ancestors) {
                  let providers = ancestorProviders.get(ancestorId);
                  if (providers === undefined) {
                    const records = yield* projections.getThreadRecords(ancestorId, [
                      "providerThreads",
                    ]);
                    providers = records.providerThreads;
                    ancestorProviders.set(ancestorId, providers);
                  }
                  if (
                    providers.some(
                      (provider) =>
                        provider.providerInstanceId === task.providerInstanceId &&
                        (provider.pendingBackgroundTasks?.length ?? 0) > 0,
                    )
                  ) {
                    pendingAncestorWork = true;
                    break;
                  }
                }
                if (!pendingAncestorWork) continue;
              }
            }
          }
          const providerContext = yield* projections.getThreadProviderContext(otherThread.id);
          const paths = [
            otherThread.worktreePath,
            ...other.checkpointScopes.map((candidate) => candidate.cwd),
            // Where its runs worked, which a scope's first spelling (a removed symlink) may not name.
            ...other.runs.flatMap((run) =>
              run.workspaceCwd === undefined ? [] : [run.workspaceCwd],
            ),
            // Errored sessions can still stream. Shared sessions retain another thread's opening cwd;
            // turn worktrees and project roots are already covered here.
            ...providerContext.providerSessions
              .filter(
                (session) =>
                  session.status !== "stopped" &&
                  !session.capabilities.sessions.supportsMultipleProviderThreadsPerSession,
              )
              .map((session) => session.cwd),
          ].filter((value): value is string => value !== null);
          if (otherThread.worktreePath === null) {
            const project = yield* projects.get(otherThread.projectId, { includeDeleted: true });
            if (Option.isNone(project)) return false;
            paths.push(project.value.workspaceRoot);
          }
          for (const candidate of paths) {
            if (checkedPaths.has(candidate)) continue;
            checkedPaths.add(candidate);
            const otherCwd = yield* fileSystem.realPath(candidate).pipe(
              Effect.catchIf(
                (error) => error.reason._tag === "NotFound",
                (_error) => Effect.succeed(null),
              ),
            );
            if (
              otherCwd !== null &&
              restored.some((folder) => contains(folder, otherCwd) || contains(otherCwd, folder))
            ) {
              shared = true;
              break;
            }
          }
          if (shared) break;
        }
      }
    }
    return !shared;
  },
);

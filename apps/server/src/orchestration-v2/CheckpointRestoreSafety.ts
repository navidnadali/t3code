import type { OrchestrationV2AppThread, OrchestrationV2CheckpointScope } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";
import type { ProjectionStoreV2 } from "./ProjectionStore.ts";
import type * as ProjectStore from "./ProjectStore.ts";

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

export const anotherWorktreeRestoreMessage = (workspace: string) =>
  `This checkpoint was taken in another worktree (${shortenedPath(workspace)}). Restoring its files would overwrite this worktree's branch. Use “Revert and keep changes” to rewind the conversation only.`;

// Storage cleanup may have removed a worktree; rollback recreates the thread's.
const resolveWorkspace = (fileSystem: FileSystem.FileSystem, path: Path.Path, target: string) =>
  fileSystem.realPath(target).pipe(
    Effect.catchIf(
      (error) => error.reason._tag === "NotFound",
      (_error) => Effect.succeed(path.resolve(target)),
    ),
  );

// A file restore writes the checkpoint's tree into the thread's worktree; one taken in another
// worktree would replace this worktree's branch with that one's. Shared workspaces are refused apart.
export const isCheckpointFromAnotherWorktree = Effect.fn(
  "orchestrationV2.isCheckpointFromAnotherWorktree",
)(function* (
  thread: Pick<OrchestrationV2AppThread, "worktreePath">,
  workspace: string,
  dependencies: { readonly fileSystem: FileSystem.FileSystem; readonly path: Path.Path },
) {
  if (thread.worktreePath == null) return false;
  const { fileSystem, path } = dependencies;
  return (
    (yield* resolveWorkspace(fileSystem, path, workspace)) !==
    (yield* resolveWorkspace(fileSystem, path, thread.worktreePath))
  );
});

export const SHARED_WORKSPACE_RESTORE_MESSAGE =
  "File restore requires an isolated worktree. This workspace may contain changes from another thread or a side chat. Rewind the conversation without restoring files instead, or delete side chats you no longer need.";

// Check isolation at admission and rollback; checkpoints restore whole checkouts, which may become shared.
export const isCheckpointRestoreIsolated = Effect.fn("orchestrationV2.isCheckpointRestoreIsolated")(
  function* (
    thread: Pick<OrchestrationV2AppThread, "id" | "worktreePath">,
    scope: Pick<OrchestrationV2CheckpointScope, "cwd">,
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
      const resolve = (target: string) => resolveWorkspace(fileSystem, path, target);
      const cwd = yield* resolve(scope.cwd);
      const worktreeCwd = yield* resolve(worktreePath);
      shared = cwd !== worktreeCwd;
      if (!shared) {
        const shell = yield* projections.getShellSnapshot();
        const checkedPaths = new Set<string>();
        for (const otherThread of [...shell.threads, ...shell.archivedThreads]) {
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
          const providerContext = yield* projections.getThreadProviderContext(otherThread.id);
          const paths = [
            otherThread.worktreePath,
            ...other.checkpointScopes.map((candidate) => candidate.cwd),
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
            if (otherCwd !== null && (contains(cwd, otherCwd) || contains(otherCwd, cwd))) {
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

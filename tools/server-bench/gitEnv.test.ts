// @effect-diagnostics nodeBuiltinImport:off - The bench CLIs run as plain node processes with real git.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { describe, expect, it } from "vite-plus/test";

const git = (cwd: string, ...args: ReadonlyArray<string>) =>
  NodeChildProcess.execFileSync(
    "git",
    ["-c", "user.name=T", "-c", "user.email=t@localhost", ...args],
    { cwd, encoding: "utf8" },
  ).trim();

const repository = (directory: string, message: string) => {
  NodeFS.mkdirSync(directory, { recursive: true });
  git(directory, "init", "-q");
  git(directory, "commit", "--allow-empty", "-qm", message);
  return directory;
};

const moduleUrl = (file: string) =>
  NodeURL.pathToFileURL(NodePath.join(import.meta.dirname, file)).href;

describe("bench git commands", () => {
  // A bench started from a git hook or `git bisect run` inherits GIT_DIR (hooks also GIT_INDEX_FILE).
  it("work in their own repository, and pass none of the caller's selectors on", () => {
    const base = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-git-env-"));
    try {
      const caller = repository(NodePath.join(base, "caller"), "caller");
      const target = repository(NodePath.join(base, "target"), "target");
      const workspace = NodePath.join(base, "workspace");
      const callerHead = git(caller, "rev-parse", "HEAD");
      const script = `
        const run = await import(${JSON.stringify(moduleUrl("run.ts"))});
        const driver = await import(${JSON.stringify(moduleUrl("driver.ts"))});
        driver.createWorkspace(${JSON.stringify(workspace)});
        console.log(JSON.stringify({
          tree: run.treeState(${JSON.stringify(target)}).sha,
          inherited: Object.keys(run.cleanEnv()).filter((name) => /^GIT_(DIR|INDEX_FILE)$/.test(name)),
        }));`;
      const child = NodeChildProcess.spawnSync(
        process.execPath,
        ["--input-type=module", "-e", script],
        {
          cwd: base,
          encoding: "utf8",
          env: {
            ...process.env,
            GIT_DIR: NodePath.join(caller, ".git"),
            GIT_INDEX_FILE: NodePath.join(caller, ".git", "index"),
          },
        },
      );
      expect(child.stderr).toBe("");

      expect(JSON.parse(child.stdout)).toEqual({
        tree: git(target, "rev-parse", "HEAD"),
        inherited: [],
      });
      expect(git(workspace, "log", "--format=%s")).toBe("Bench workspace");
      expect(git(caller, "rev-parse", "HEAD")).toBe(callerHead);
    } finally {
      NodeFS.rmSync(base, { recursive: true, force: true });
    }
  });
});

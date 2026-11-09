// @effect-diagnostics nodeBuiltinImport:off - Builds and runs the single executable, as a user's shell does.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

const serverDir = NodePath.resolve(NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)), "..");

interface NodeFlagsReport {
  readonly applied: boolean;
  readonly maxSemiSpaceMb: number | null;
  readonly nodeOptionsForChildren: string | null;
  readonly execArgv: ReadonlyArray<string>;
  readonly sea: boolean;
  readonly keptReason?: string;
  readonly heapSizeLimitMiB?: number;
  readonly capSettingIneffective?: true;
}

const cleanEnv = (env: Record<string, string | undefined>) => ({
  ...process.env,
  NODE_OPTIONS: undefined,
  T3CODE_V8_MAX_SEMI_SPACE_MB: undefined,
  T3CODE_INHERITED_NODE_OPTIONS: undefined,
  ...env,
});

// `t3 __node-flags` as a shell would start it: the executable's own argv shape, the user's NODE_OPTIONS.
const nodeFlags = (
  executable: string,
  env: Record<string, string | undefined>,
): NodeFlagsReport => {
  const result = NodeChildProcess.spawnSync(executable, ["__node-flags"], {
    env: cleanEnv(env),
    encoding: "utf8",
    timeout: 60_000,
  });
  expect(result.status, result.stderr).toBe(0);
  return JSON.parse(result.stdout) as NodeFlagsReport;
};

const modifiedAt = (file: string): number | undefined => {
  try {
    return NodeFS.statSync(file).mtimeMs;
  } catch {
    return undefined;
  }
};

// Builds a 160 MB executable and needs the engine's exact Node: a release check, run on request with
//   T3_SEA_TEST=1 vp test run apps/server/scripts/youngGenerationSea.test.ts
// (test workers drop every T3CODE_-prefixed variable, so the gate is a T3_ name).
describe("young-generation default in the single executable", () => {
  it.live.skipIf(
    HostProcess.Platform.defaultValue() === "win32" || process.env.T3_SEA_TEST !== "1",
  )("runs with the built-in cap on every start and lets Node guard NODE_OPTIONS", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "young-generation-sea-" });
      // The release binary's argv is [exe, exe, ...arguments]; nothing else runs that shape. It is
      // built beside the test, never over the worktree's own dist-exe.
      const shipped = NodePath.join(serverDir, "dist-exe/t3");
      const shippedBefore = modifiedAt(shipped);
      const build = NodeChildProcess.spawnSync(
        process.execPath,
        ["scripts/cli.ts", "build-exe", "--out-dir", dir],
        {
          cwd: serverDir,
          env: { ...process.env, NODE_OPTIONS: undefined },
          encoding: "utf8",
          timeout: 600_000,
        },
      );
      expect(build.status, build.stderr).toBe(0);
      expect(modifiedAt(shipped)).toBe(shippedBefore);
      const executable = NodePath.join(dir, "t3");
      // The default is baked in: a bare start runs with it and never relaunches.
      const bare = nodeFlags(executable, {});
      expect(bare.sea).toBe(true);
      expect(bare.execArgv).toEqual(["--max-semi-space-size=64", "--require-module"]);
      expect(bare, bare.keptReason).toMatchObject({
        applied: true,
        maxSemiSpaceMb: 64,
        keptReason: "the young-generation cap is already in effect",
      });
      // The role's heap ceiling: Node reads NODE_OPTIONS itself, no relaunch, children get it unchanged.
      const userOptions = "--max-old-space-size=12288";
      const role = nodeFlags(executable, { NODE_OPTIONS: userOptions });
      expect(role, role.keptReason).toMatchObject({
        applied: true,
        maxSemiSpaceMb: 64,
        nodeOptionsForChildren: userOptions,
        keptReason: "the young-generation cap is already in effect",
      });
      expect(role.execArgv).toEqual(["--max-semi-space-size=64", "--require-module"]);
      expect(role.heapSizeLimitMiB!).toBeGreaterThan(12000);
      // The built-in cap cannot be changed from the environment; the server says so for the log.
      const off = nodeFlags(executable, { T3CODE_V8_MAX_SEMI_SPACE_MB: "0" });
      expect(off).toMatchObject({ applied: true, maxSemiSpaceMb: 64, capSettingIneffective: true });
      expect(off.heapSizeLimitMiB).toBe(bare.heapSizeLimitMiB);
      const explicit = nodeFlags(executable, { NODE_OPTIONS: "--max-semi-space-size=16" });
      expect(explicit).toMatchObject({ maxSemiSpaceMb: 64, capSettingIneffective: true });
      // Node's own guards stay in force: an option outside its allowlist is refused.
      const refused = NodeChildProcess.spawnSync(executable, ["__node-flags"], {
        env: cleanEnv({ NODE_OPTIONS: "--expose-internals" }),
        encoding: "utf8",
        timeout: 60_000,
      });
      expect(refused.status).toBe(9);
      expect(refused.stderr).toContain("not allowed in NODE_OPTIONS");
      // A --node-options argument is a plain argument, never Node options: the process must exit
      // on its own (a waiting debugger would hold it), show no debugger, and run with the baked
      // flags only.
      for (const args of [
        ["--node-options=--inspect-wait=127.0.0.1:0", "__node-flags"],
        ["__node-flags", "--node-options=--inspect-wait=127.0.0.1:0"],
      ]) {
        const injected = NodeChildProcess.spawnSync(executable, args, {
          env: cleanEnv({}),
          encoding: "utf8",
          timeout: 60_000,
        });
        expect(injected.error, args.join(" ")).toBeUndefined();
        expect(injected.signal).toBeNull();
        expect(`${injected.stdout}${injected.stderr}`).not.toMatch(/Debugger listening|debugger/i);
        if (injected.status === 0) {
          expect((JSON.parse(injected.stdout) as NodeFlagsReport).execArgv).toEqual([
            "--max-semi-space-size=64",
          ]);
        }
      }
      // A helper's heap bound in NODE_OPTIONS reaches V8 the ordinary way.
      const bounded = nodeFlags(executable, { NODE_OPTIONS: "--max-old-space-size=192" });
      expect(bounded.heapSizeLimitMiB!).toBeLessThan(512);
      // Through a symlink, as /usr/local/bin/t3 is.
      const link = NodePath.join(dir, "link", "t3");
      NodeFS.mkdirSync(NodePath.dirname(link));
      NodeFS.symlinkSync(executable, link);
      expect(nodeFlags(link, {})).toMatchObject({ applied: true, maxSemiSpaceMb: 64 });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

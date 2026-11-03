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
}

// `t3 __node-flags` as a shell would start it: the executable's own argv shape, the user's NODE_OPTIONS.
const nodeFlags = (
  executable: string,
  env: Record<string, string | undefined>,
): NodeFlagsReport => {
  const result = NodeChildProcess.spawnSync(executable, ["__node-flags"], {
    env: {
      ...process.env,
      NODE_OPTIONS: undefined,
      T3CODE_V8_MAX_SEMI_SPACE_MB: undefined,
      T3CODE_INHERITED_NODE_OPTIONS: undefined,
      ...env,
    },
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
  )("relaunches the built t3 with the cap, directly and through a symlink", () =>
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
      expect(bare.execArgv).toEqual(["--max-semi-space-size=64"]);
      expect(bare, bare.keptReason).toMatchObject({
        applied: true,
        maxSemiSpaceMb: 64,
        keptReason: "the young-generation cap is already in effect",
      });
      // The cap reached V8: the escape hatch, applied over the baked value, lowers the heap limit.
      const off = nodeFlags(executable, { T3CODE_V8_MAX_SEMI_SPACE_MB: "0" });
      expect(off.applied).toBe(false);
      expect(bare.heapSizeLimitMiB! - off.heapSizeLimitMiB!).toBeGreaterThanOrEqual(64);
      // NODE_OPTIONS, which the executable takes only on its command line, reach V8 through one
      // relaunch and stay exactly as they were for the children.
      const userOptions = "--max-old-space-size=12288";
      const direct = nodeFlags(executable, { NODE_OPTIONS: userOptions });
      expect(direct.execArgv).toContain(userOptions);
      expect(direct.heapSizeLimitMiB!).toBeGreaterThan(12000);
      expect(direct.sea).toBe(true);
      expect(direct, direct.keptReason).toMatchObject({
        applied: true,
        maxSemiSpaceMb: 64,
        nodeOptionsForChildren: userOptions,
      });
      const link = NodePath.join(dir, "link", "t3");
      NodeFS.mkdirSync(NodePath.dirname(link));
      NodeFS.symlinkSync(executable, link);
      expect(nodeFlags(link, { NODE_OPTIONS: userOptions })).toMatchObject({
        applied: true,
        maxSemiSpaceMb: 64,
      });
      // An explicit cap is never overridden, whatever the argument shape.
      expect(nodeFlags(executable, { NODE_OPTIONS: "--max-semi-space-size=16" })).toMatchObject({
        applied: false,
        maxSemiSpaceMb: 16,
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

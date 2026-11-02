// @effect-diagnostics nodeBuiltinImport:off -- Spawns the real CLI entrypoint to see the relaunch from inside.
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import {
  DEFAULT_MAX_SEMI_SPACE_MB,
  INHERITED_NODE_OPTIONS_ENV,
  MAX_SEMI_SPACE_ENV,
  describeNodeRuntime,
  isServerCommand,
  nodeOptionsWithMaxSemiSpace,
  planYoungGenerationRelaunch,
  resolveMaxSemiSpaceMb,
  serverChildEnvironment,
  tokenizeNodeOptions,
} from "./youngGeneration.ts";

const flag = `--max-semi-space-size=${DEFAULT_MAX_SEMI_SPACE_MB}`;

const plan = (overrides: Partial<Parameters<typeof planYoungGenerationRelaunch>[0]> = {}) =>
  planYoungGenerationRelaunch({
    argv: ["/usr/bin/node", "/opt/t3/dist/bin.mjs", "serve"],
    execArgv: [],
    env: {},
    execPath: "/usr/bin/node",
    platform: "linux",
    hasIpcChannel: false,
    isSea: false,
    canExecve: true,
    ...overrides,
  });

describe("resolveMaxSemiSpaceMb", () => {
  it("defaults to 64 MiB", () => {
    expect(resolveMaxSemiSpaceMb({}, [])).toBe(64);
  });
  it("is disabled by 0, off, false or no", () => {
    for (const value of ["0", "off", "false", "no", " OFF "]) {
      expect(resolveMaxSemiSpaceMb({ [MAX_SEMI_SPACE_ENV]: value }, [])).toBeNull();
    }
  });
  it("takes a positive integer from the environment and ignores an invalid value", () => {
    expect(resolveMaxSemiSpaceMb({ [MAX_SEMI_SPACE_ENV]: "128" }, [])).toBe(128);
    expect(resolveMaxSemiSpaceMb({ [MAX_SEMI_SPACE_ENV]: "lots" }, [])).toBe(64);
    expect(resolveMaxSemiSpaceMb({ [MAX_SEMI_SPACE_ENV]: "-8" }, [])).toBe(64);
    expect(resolveMaxSemiSpaceMb({ [MAX_SEMI_SPACE_ENV]: "1.5" }, [])).toBe(64);
  });
  it("never overrides an explicit flag in NODE_OPTIONS or execArgv", () => {
    expect(resolveMaxSemiSpaceMb({ NODE_OPTIONS: "--max-semi-space-size=16" }, [])).toBeNull();
    expect(
      resolveMaxSemiSpaceMb(
        { NODE_OPTIONS: "--max-old-space-size=16384 --max-semi-space-size 32" },
        [],
      ),
    ).toBeNull();
    expect(resolveMaxSemiSpaceMb({}, ["--max-semi-space-size=8"])).toBeNull();
    expect(resolveMaxSemiSpaceMb({ NODE_OPTIONS: "--max-old-space-size=16384" }, [])).toBe(64);
  });
});

describe("tokenizeNodeOptions", () => {
  it("splits like Node: spaces separate, double quotes group, a backslash in quotes escapes", () => {
    expect(tokenizeNodeOptions(undefined)).toEqual([]);
    expect(tokenizeNodeOptions("  --a   --b=1 ")).toEqual(["--a", "--b=1"]);
    expect(tokenizeNodeOptions('--require "./my path/file.js" --c')).toEqual([
      "--require",
      "./my path/file.js",
      "--c",
    ]);
    expect(tokenizeNodeOptions('"--max-semi-space-size=16"')).toEqual(["--max-semi-space-size=16"]);
    expect(tokenizeNodeOptions('--max-semi-space-size="16"')).toEqual(["--max-semi-space-size=16"]);
    expect(tokenizeNodeOptions('--title="say \\"hi\\""')).toEqual(['--title=say "hi"']);
  });
  it("recognises every spelling of the flag Node accepts", () => {
    for (const nodeOptions of [
      '"--max-semi-space-size=16"',
      '--max-semi-space-size="16"',
      "--max_semi_space_size=16",
      "--max-semi-space-size 16",
      "--max-semi-space-size",
    ]) {
      expect(resolveMaxSemiSpaceMb({ NODE_OPTIONS: nodeOptions }, []), nodeOptions).toBeNull();
    }
    expect(resolveMaxSemiSpaceMb({}, ["--max_semi_space_size=8"])).toBeNull();
  });
});

describe("nodeOptionsWithMaxSemiSpace", () => {
  it("appends the flag after what the user set", () => {
    expect(nodeOptionsWithMaxSemiSpace(undefined, 64)).toBe(flag);
    expect(nodeOptionsWithMaxSemiSpace("", 64)).toBe(flag);
    expect(nodeOptionsWithMaxSemiSpace(" --max-old-space-size=16384 ", 64)).toBe(
      `--max-old-space-size=16384 ${flag}`,
    );
  });
});

describe("serverChildEnvironment", () => {
  it("gives a spawned server the flag and keeps the parent's NODE_OPTIONS for the server's children", () => {
    const env = serverChildEnvironment({
      PATH: "/bin",
      NODE_OPTIONS: "--max-old-space-size=16384",
    });
    expect(env.NODE_OPTIONS).toBe(`--max-old-space-size=16384 ${flag}`);
    expect(env[INHERITED_NODE_OPTIONS_ENV]).toBe("--max-old-space-size=16384");
    expect(env.PATH).toBe("/bin");
    expect(serverChildEnvironment({}).NODE_OPTIONS).toBe(flag);
    expect(serverChildEnvironment({})[INHERITED_NODE_OPTIONS_ENV]).toBe("");
  });
  it("leaves the environment alone when disabled or already explicit", () => {
    const disabled = { [MAX_SEMI_SPACE_ENV]: "0" };
    expect(serverChildEnvironment(disabled)).toEqual(disabled);
    const explicit = { NODE_OPTIONS: "--max-semi-space-size=16" };
    expect(serverChildEnvironment(explicit)).toEqual(explicit);
  });
});

describe("describeNodeRuntime", () => {
  it("reports the cap V8 saw, the heap limit in MiB and the children's NODE_OPTIONS", () => {
    expect(
      describeNodeRuntime({
        execArgv: [],
        nodeOptionsAtStart: `--max-old-space-size=16384 ${flag}`,
        nodeOptionsForChildren: "--max-old-space-size=16384",
        heapSizeLimitBytes: 17_280_532_480,
        applied: true,
      }),
    ).toEqual({
      execArgv: [],
      nodeOptions: `--max-old-space-size=16384 ${flag}`,
      nodeOptionsForChildren: "--max-old-space-size=16384",
      heapSizeLimitMiB: 16480,
      maxSemiSpaceMiB: 64,
      youngGenerationDefault: true,
    });
    expect(
      describeNodeRuntime({
        execArgv: [],
        nodeOptionsAtStart: "--max-semi-space-size=8 --max-semi-space-size=16",
        nodeOptionsForChildren: undefined,
        heapSizeLimitBytes: 0,
        applied: false,
      }).maxSemiSpaceMiB,
    ).toBe(16);
    const plain = describeNodeRuntime({
      execArgv: ["--max-semi-space-size=32"],
      nodeOptionsAtStart: undefined,
      nodeOptionsForChildren: undefined,
      heapSizeLimitBytes: 4_395_630_592,
      applied: false,
    });
    expect(plain.maxSemiSpaceMiB).toBe(32);
    expect(plain.nodeOptions).toBeNull();
    expect(plain.heapSizeLimitMiB).toBe(4192);
    expect(
      describeNodeRuntime({
        execArgv: [],
        nodeOptionsAtStart: undefined,
        nodeOptionsForChildren: undefined,
        heapSizeLimitBytes: 0,
        applied: false,
      }).maxSemiSpaceMiB,
    ).toBe("node default");
  });
});

describe("isServerCommand", () => {
  it("recognises the commands that run the server in this process", () => {
    const argv = (...rest: Array<string>) => ["node", "bin.mjs", ...rest];
    expect(isServerCommand(argv())).toBe(true);
    expect(isServerCommand(argv("serve"))).toBe(true);
    expect(isServerCommand(argv("start", "--no-browser"))).toBe(true);
    expect(isServerCommand(argv("."))).toBe(true);
    expect(isServerCommand(argv("./my-project"))).toBe(true);
    expect(isServerCommand(argv("C:\\work"))).toBe(true);
    const isDirectory = (path: string) => path === "my-project";
    expect(isServerCommand(argv("my-project"), isDirectory)).toBe(true);
    expect(isServerCommand(argv("my-project", "--port", "3773"), isDirectory)).toBe(true);
    expect(isServerCommand(argv("nonexistent"), isDirectory)).toBe(false);
    for (const flags of [
      ["--no-browser"],
      ["--port", "3773"],
      ["--base-dir", "/tmp/t3"],
      ["--lan"],
    ]) {
      expect(isServerCommand(argv(...flags), isDirectory), flags.join(" ")).toBe(true);
    }
    for (const flag of ["--help", "-h", "--version", "-v", "-V", "--completions", "--wizard"]) {
      expect(isServerCommand(argv(flag), isDirectory), flag).toBe(false);
    }
    expect(isServerCommand(argv("auth", "session", "issue"), isDirectory)).toBe(false);
    expect(isServerCommand(argv("acp-mcp-bridge"), isDirectory)).toBe(false);
    expect(isServerCommand(argv("__process-spawner"), isDirectory)).toBe(false);
  });
});

describe("planYoungGenerationRelaunch", () => {
  it("relaunches a Node entry with the flag in NODE_OPTIONS and the original kept for children", () => {
    const result = plan({
      argv: ["/usr/bin/node", "/opt/t3/dist/bin.mjs", "start", "--no-browser"],
      execArgv: ["--experimental-strip-types"],
      env: { NODE_OPTIONS: "--max-old-space-size=16384", HOME: "/home/t3" },
    });
    expect(result).toEqual({
      kind: "relaunch",
      file: "/usr/bin/node",
      args: [
        "/usr/bin/node",
        "--experimental-strip-types",
        "/opt/t3/dist/bin.mjs",
        "start",
        "--no-browser",
      ],
      env: {
        HOME: "/home/t3",
        NODE_OPTIONS: `--max-old-space-size=16384 ${flag}`,
        [INHERITED_NODE_OPTIONS_ENV]: "--max-old-space-size=16384",
      },
    });
  });
  it("relaunches a single executable by its own path with the user arguments only", () => {
    const result = plan({
      argv: ["/usr/local/bin/t3", "/usr/local/bin/t3", "serve", "--lan"],
      execPath: "/usr/local/bin/t3",
      isSea: true,
    });
    expect(result.kind).toBe("relaunch");
    if (result.kind !== "relaunch") return;
    expect(result.args).toEqual(["/usr/local/bin/t3", "serve", "--lan"]);
    expect(result.env.NODE_OPTIONS).toBe(flag);
    expect(result.env[INHERITED_NODE_OPTIONS_ENV]).toBe("");
  });
  it("keeps the process when the default is already in effect, disabled or explicit", () => {
    expect(plan({ env: { [INHERITED_NODE_OPTIONS_ENV]: "" } }).kind).toBe("keep");
    expect(plan({ env: { [MAX_SEMI_SPACE_ENV]: "off" } }).kind).toBe("keep");
    expect(plan({ env: { NODE_OPTIONS: "--max-semi-space-size=32" } }).kind).toBe("keep");
    expect(plan({ execArgv: ["--max-semi-space-size=32"] }).kind).toBe("keep");
  });
  it("keeps the process where a relaunch is impossible or would lose a handle", () => {
    expect(plan({ argv: ["node", "bin.mjs", "auth", "session", "issue"] }).kind).toBe("keep");
    expect(plan({ platform: "win32" }).kind).toBe("keep");
    expect(plan({ canExecve: false }).kind).toBe("keep");
    expect(plan({ hasIpcChannel: true }).kind).toBe("keep");
    expect(plan({ argv: ["node", "bin.mjs", "serve", "--bootstrap-fd", "3"] }).kind).toBe("keep");
    expect(plan({ argv: ["node", "bin.mjs", "serve", "--bootstrap-fd=3"] }).kind).toBe("keep");
    expect(plan({ env: { T3CODE_BOOTSTRAP_FD: "3" } }).kind).toBe("keep");
  });
  it("relaunches root invocations that run the server with flags or an existing directory", () => {
    expect(plan({ argv: ["node", "bin.mjs", "--no-browser"] }).kind).toBe("relaunch");
    expect(plan({ argv: ["node", "bin.mjs", "--port", "3773"] }).kind).toBe("relaunch");
    expect(
      plan({
        argv: ["node", "bin.mjs", "my-project"],
        isDirectory: (path) => path === "my-project",
      }).kind,
    ).toBe("relaunch");
    expect(plan({ argv: ["node", "bin.mjs", "--help"] }).kind).toBe("keep");
  });
});

interface NodeFlagsReport {
  readonly applied: boolean;
  readonly maxSemiSpaceMb: number | null;
  readonly nodeOptionsAtStart: string | null;
  readonly nodeOptionsForChildren: string | null;
  readonly execArgv: ReadonlyArray<string>;
  readonly keptReason?: string;
}

const binPath = NodeURL.fileURLToPath(new URL("./bin.ts", import.meta.url));

const cleanEnv = {
  NODE_OPTIONS: undefined,
  [MAX_SEMI_SPACE_ENV]: undefined,
  [INHERITED_NODE_OPTIONS_ENV]: undefined,
};

describe("the entrypoint", () => {
  const run = Effect.fn("youngGeneration.test.run")(function* (
    env: Record<string, string | undefined>,
    args: ReadonlyArray<string> = [],
  ) {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(
      ChildProcess.make(
        process.execPath,
        ["--experimental-strip-types", binPath, "__node-flags", ...args],
        { env: { ...cleanEnv, ...env } },
      ),
    );
    const { stdout, stderr, exitCode } = yield* Effect.all(
      {
        stdout: child.stdout.pipe(Stream.decodeText(), Stream.mkString),
        stderr: child.stderr.pipe(Stream.decodeText(), Stream.mkString),
        exitCode: child.exitCode.pipe(Effect.map(Number)),
      },
      { concurrency: "unbounded" },
    );
    expect(exitCode, stderr).toBe(0);
    return JSON.parse(stdout) as NodeFlagsReport;
  }, Effect.scoped);

  it.live(
    "relaunches node bin.ts with the young-generation default and hides it from children",
    () =>
      Effect.gen(function* () {
        const report = yield* run({});
        expect(report.applied).toBe(true);
        expect(report.maxSemiSpaceMb).toBe(DEFAULT_MAX_SEMI_SPACE_MB);
        expect(report.nodeOptionsAtStart).toBe(flag);
        expect(report.nodeOptionsForChildren).toBeNull();
        expect(report.execArgv).toContain("--experimental-strip-types");
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("keeps the user's NODE_OPTIONS beside the flag and gives children the original", () =>
    Effect.gen(function* () {
      const report = yield* run({ NODE_OPTIONS: "--no-warnings" });
      expect(report.applied).toBe(true);
      expect(report.nodeOptionsAtStart).toBe(`--no-warnings ${flag}`);
      expect(report.nodeOptionsForChildren).toBe("--no-warnings");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("never overrides an explicit --max-semi-space-size", () =>
    Effect.gen(function* () {
      const report = yield* run({ NODE_OPTIONS: "--max-semi-space-size=16" });
      expect(report.applied).toBe(false);
      expect(report.maxSemiSpaceMb).toBe(16);
      expect(report.nodeOptionsAtStart).toBe("--max-semi-space-size=16");
      expect(report.nodeOptionsForChildren).toBe("--max-semi-space-size=16");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("honours the escape hatch and a custom size", () =>
    Effect.gen(function* () {
      const off = yield* run({ [MAX_SEMI_SPACE_ENV]: "0" });
      expect(off.applied).toBe(false);
      expect(off.maxSemiSpaceMb).toBeNull();
      const custom = yield* run({ [MAX_SEMI_SPACE_ENV]: "96" });
      expect(custom.applied).toBe(true);
      expect(custom.maxSemiSpaceMb).toBe(96);
      expect(custom.nodeOptionsAtStart).toBe("--max-semi-space-size=96");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("does not relaunch a server that inherited a bootstrap descriptor", () =>
    Effect.gen(function* () {
      const report = yield* run({}, ["--bootstrap-fd", "3"]);
      expect(report.applied).toBe(false);
      expect(report.maxSemiSpaceMb).toBeNull();
      expect(report.keptReason).toContain("descriptor");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("never overrides a quoted or underscored explicit flag either", () =>
    Effect.gen(function* () {
      const quoted = yield* run({ NODE_OPTIONS: '--max-semi-space-size="16"' });
      expect(quoted.applied).toBe(false);
      expect(quoted.maxSemiSpaceMb).toBe(16);
      const underscored = yield* run({ NODE_OPTIONS: "--max_semi_space_size=16" });
      expect(underscored.applied).toBe(false);
      expect(underscored.maxSemiSpaceMb).toBe(16);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live(
    "does not relaunch a server whose bootstrap descriptor comes from the environment, and keeps it open",
    () =>
      Effect.promise(
        () =>
          new Promise<void>((resolve, reject) => {
            const child = NodeChildProcess.spawn(
              process.execPath,
              ["--experimental-strip-types", binPath, "__node-flags"],
              {
                env: Object.fromEntries(
                  Object.entries({ ...process.env, ...cleanEnv, T3CODE_BOOTSTRAP_FD: "3" }).flatMap(
                    ([name, value]) => (value === undefined ? [] : [[name, value]]),
                  ),
                ),
                stdio: ["ignore", "pipe", "pipe", "pipe"],
              },
            );
            let stdout = "";
            let stderr = "";
            child.stdout?.on("data", (chunk: Buffer) => (stdout += chunk.toString()));
            child.stderr?.on("data", (chunk: Buffer) => (stderr += chunk.toString()));
            child.on("error", reject);
            child.on("exit", (code) => {
              try {
                expect(code, stderr).toBe(0);
                const report = JSON.parse(stdout) as NodeFlagsReport & {
                  readonly bootstrapFdOpen?: boolean;
                };
                expect(report.applied).toBe(false);
                expect(report.keptReason).toContain("descriptor");
                expect(report.bootstrapFdOpen).toBe(true);
                resolve();
              } catch (error) {
                reject(error);
              }
            });
          }),
      ),
  );

  it.live("does not relaunch a server its parent talks to over IPC, and keeps the channel", () =>
    Effect.promise(
      () =>
        new Promise<void>((resolve, reject) => {
          const child = NodeChildProcess.fork(binPath, ["__node-flags"], {
            execArgv: ["--experimental-strip-types"],
            env: Object.fromEntries(
              Object.entries({ ...process.env, ...cleanEnv }).flatMap(([name, value]) =>
                value === undefined ? [] : [[name, value]],
              ),
            ),
            stdio: ["ignore", "pipe", "pipe", "ipc"],
          });
          let stdout = "";
          let stderr = "";
          child.stdout?.on("data", (chunk: Buffer) => (stdout += chunk.toString()));
          child.stderr?.on("data", (chunk: Buffer) => (stderr += chunk.toString()));
          child.on("error", reject);
          child.on("exit", (code) => {
            try {
              expect(code, stderr).toBe(0);
              const report = JSON.parse(stdout) as NodeFlagsReport & { readonly ipc: boolean };
              expect(report.applied).toBe(false);
              expect(report.ipc).toBe(true);
              expect(report.keptReason).toContain("IPC");
              resolve();
            } catch (error) {
              reject(error);
            }
          });
        }),
    ),
  );
});

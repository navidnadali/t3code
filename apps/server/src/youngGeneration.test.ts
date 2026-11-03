// @effect-diagnostics nodeBuiltinImport:off -- Spawns the real CLI entrypoint to see the relaunch from inside.
import * as NodeChildProcess from "node:child_process";
import * as NodeURL from "node:url";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { vi } from "vite-plus/test";

// Records every synchronous spawn the module under test makes; the calls go through to the real one.
const spawned = vi.hoisted(() => ({ files: [] as Array<string> }));
// Lets a test present this process as the single executable, whose argv is [exe, exe, ...arguments].
const sea = vi.hoisted(() => ({ isSea: false }));
vi.mock("node:sea", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:sea")>();
  return { ...actual, isSea: () => sea.isSea };
});
vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return {
    ...actual,
    spawnSync: ((file: string, ...rest: ReadonlyArray<unknown>) => {
      spawned.files.push(file);
      return (actual.spawnSync as (...args: ReadonlyArray<unknown>) => unknown)(file, ...rest);
    }) as typeof actual.spawnSync,
  };
});

import {
  applyYoungGenerationDefault,
  DEFAULT_MAX_SEMI_SPACE_MB,
  INHERITED_NODE_OPTIONS_ENV,
  MAX_SEMI_SPACE_ENV,
  describeNodeRuntime,
  isServerCommand,
  nodeOptionsWithMaxSemiSpace,
  planYoungGenerationRelaunch,
  resolveMaxSemiSpaceMb,
  NON_SERVER_COMMANDS,
  nodeOptionsFromEnviron,
  runtimeFlagsOf,
  SERVER_COMMANDS,
  decideYoungGeneration,
  serverChildEnvironment,
  tokenizeNodeOptions,
} from "./youngGeneration.ts";
import type { StartupNodeOptions } from "./youngGeneration.ts";
import { SERVICE_LAUNCHER_CONTEXT_ENV } from "./cloud/serviceProtocol.ts";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

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
      "-max-semi-space-size=16",
      "-max_semi_space_size=16",
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

  it("never takes a registered subcommand for a project directory of the same name", () => {
    // A directory called `app` or `status` beside the user must not turn `t3 app` into a server start.
    const argv = (...rest: ReadonlyArray<string>) => [
      "/usr/bin/node",
      "/opt/t3/dist/bin.mjs",
      ...rest,
    ];
    const everythingIsADirectory = () => true;
    for (const command of [
      "app",
      "browser",
      "environment",
      "pair",
      "policy",
      "status",
      "theme",
      "trace",
      "triage",
      "__service-preflight",
      "__ssh-helper",
      "help",
    ]) {
      expect(isServerCommand(argv(command), everythingIsADirectory), command).toBe(false);
    }
    expect(isServerCommand(argv("my-project"), everythingIsADirectory)).toBe(true);
  });

  it("looks past a leading global flag before classifying the command", () => {
    const argv = (...rest: ReadonlyArray<string>) => [
      "/usr/bin/node",
      "/opt/t3/dist/bin.mjs",
      ...rest,
    ];
    const isDirectory = (path: string) => path.endsWith("my-project");
    expect(isServerCommand(argv("--log-level", "warn", "doctor"), isDirectory)).toBe(false);
    expect(isServerCommand(argv("--log-level=warn", "doctor", "--help"), isDirectory)).toBe(false);
    expect(isServerCommand(argv("--log-level", "warn"), isDirectory)).toBe(true);
    expect(isServerCommand(argv("--log-level", "warn", "my-project"), isDirectory)).toBe(true);
    expect(isServerCommand(argv("--log-level", "warn", "--port", "3773"), isDirectory)).toBe(true);
    expect(isServerCommand(argv("--log-level", "warn", "--help"), isDirectory)).toBe(false);
  });

  it("lists every top-level command the CLI registers, inline ones included, so none can drift into a server start", () => {
    // The root command's subcommands are the `withSubcommands([...])` list in binCli.ts: inline
    // `Command.make("name")` calls and identifiers defined as `const name = Command.make("…")` in
    // binCli.ts or exported from src/cli. Every one that is not a server command must be listed here.
    const read = (file: string) => NodeFS.readFileSync(file, "utf8");
    const binCli = read(NodePath.join(import.meta.dirname, "binCli.ts"));
    const cliDirectory = NodePath.join(import.meta.dirname, "cli");
    const definitions = new Map<string, string>();
    const collect = (source: string) => {
      for (const match of source.matchAll(
        /(?:export )?const (\w+) = Command\.make\(\s*"([a-z_-]+)"/g,
      )) {
        definitions.set(match[1]!, match[2]!);
      }
    };
    collect(binCli);
    for (const file of NodeFS.readdirSync(cliDirectory)) {
      if (file.endsWith(".ts") && !file.endsWith(".test.ts"))
        collect(read(NodePath.join(cliDirectory, file)));
    }
    const list = /Command\.withSubcommands\(\[([\s\S]*?)\]\)/.exec(binCli)?.[1];
    expect(list).toBeDefined();
    const registered = new Set<string>();
    for (const match of list!.matchAll(/Command\.make\(\s*"([a-z_-]+)"/g))
      registered.add(match[1]!);
    const inlineStripped = list!.replace(/Command\.make\([\s\S]*?\)\.pipe\([\s\S]*?\),\n/g, "");
    const unresolved: Array<string> = [];
    for (const match of inlineStripped.matchAll(/\b([a-zA-Z]+Command)\b/g)) {
      const name = definitions.get(match[1]!);
      if (name === undefined) unresolved.push(match[1]!);
      else registered.add(name);
    }
    expect(unresolved).toEqual([]);
    expect(registered.size).toBeGreaterThan(20);
    expect(registered.has("help")).toBe(true);
    const unlisted = [...registered].filter(
      (command) => !SERVER_COMMANDS.has(command) && !NON_SERVER_COMMANDS.has(command),
    );
    expect(unlisted).toEqual([]);
  });
});

describe("nodeOptionsFromEnviron", () => {
  it("decodes a /proc environ block as UTF-8 and finds NODE_OPTIONS among NUL-separated entries", () => {
    const block = Buffer.from(
      "HOME=/home/t3\0NODE_OPTIONS=--title=\u00e9t\u00e9 --no-warnings\0PATH=/usr/bin\0",
      "utf8",
    );
    expect(nodeOptionsFromEnviron(block)).toBe("--title=\u00e9t\u00e9 --no-warnings");
    expect(nodeOptionsFromEnviron(Buffer.from("HOME=/home/t3\0", "utf8"))).toBeUndefined();
  });
});

describe("applyYoungGenerationDefault", () => {
  it("does not read the startup environment for a command that never relaunches", () => {
    // Helpers the server runs per tool call (`acp-mcp-call`, `__managed-chatgpt-token`) keep Node's
    // default whatever the environment says; spawning `ps` for them on macOS is pure overhead.
    const argv = process.argv;
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { value: "darwin", configurable: true });
    process.argv = [process.execPath, binPath, "acp-mcp-call"];
    spawned.files.length = 0;
    try {
      expect(applyYoungGenerationDefault().applied).toBe(false);
    } finally {
      Object.defineProperty(process, "platform", platform);
      process.argv = argv;
    }
    expect(spawned.files.filter((file) => file.endsWith("/ps"))).toEqual([]);
  });

  it("reaches the relaunch rules with the single executable's argument shape, whatever the platform records", () => {
    // The shipped binary's argv is [exe, exe, ...arguments]: Node inserts its own path at argv[1].
    // Where no startup record exists (macOS), that must not keep the server (review R1).
    const argv = process.argv;
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { value: "darwin", configurable: true });
    process.argv = [process.execPath, process.execPath, "__node-flags"];
    sea.isSea = true;
    try {
      const outcome = applyYoungGenerationDefault();
      expect(outcome.applied).toBe(false);
      expect(outcome.keptReason).not.toMatch(/startup options|since startup/);
    } finally {
      sea.isSea = false;
      Object.defineProperty(process, "platform", platform);
      process.argv = argv;
    }
  });
});

describe("decideYoungGeneration", () => {
  const exe = "/usr/local/bin/t3";
  const pidEnv = "T3CODE_YOUNG_GENERATION_RELAUNCHED_PID";
  const view = (overrides: Partial<Parameters<typeof decideYoungGeneration>[0]> = {}) => ({
    argv: [exe, exe, "start", "--no-browser"],
    execArgv: [],
    env: { NODE_OPTIONS: "--max-old-space-size=12288" },
    execPath: exe,
    platform: "darwin",
    pid: 4242,
    hasIpcChannel: false,
    isSea: true,
    canExecve: true,
    inspectorActive: false,
    preloadRan: false,
    startup: { known: false } as StartupNodeOptions,
    ...overrides,
  });

  it("relaunches the single executable where no startup record exists, with the user arguments only", () => {
    const decision = decideYoungGeneration(view());
    expect(decision.kind).toBe("relaunch");
    if (decision.kind !== "relaunch") return;
    expect(decision.plan.args).toEqual([exe, "start", "--no-browser"]);
    expect(decision.plan.env.NODE_OPTIONS).toBe(`--max-old-space-size=12288 ${flag}`);
    expect(decision.plan.env[INHERITED_NODE_OPTIONS_ENV]).toBe("--max-old-space-size=12288");
    expect(decision.plan.env[pidEnv]).toBe("4242");
  });

  it("keeps only when a known record differs from the value in effect", () => {
    const changed = decideYoungGeneration(
      view({
        platform: "linux",
        startup: { known: true, value: "--max-semi-space-size=16 --import x.mjs" },
      }),
    );
    expect(changed.kind).toBe("keep");
    expect(changed.outcome.keptReason).toBe("NODE_OPTIONS changed since startup");
    expect(changed.outcome.nodeOptionsAtStart).toBe("--max-semi-space-size=16 --import x.mjs");
    const same = decideYoungGeneration(
      view({ platform: "linux", startup: { known: true, value: "--max-old-space-size=12288" } }),
    );
    expect(same.kind).toBe("relaunch");
  });

  it("carries a structural keep into the outcome", () => {
    const ipc = decideYoungGeneration(view({ hasIpcChannel: true }));
    expect(ipc.kind).toBe("keep");
    expect(ipc.outcome.structural).toBe(true);
    const preload = decideYoungGeneration(view({ preloadRan: true }));
    expect(preload.kind).toBe("keep");
    expect(preload.outcome.structural).toBeUndefined();
  });

  it("honours the marker as an exact prefix, bounds the relaunch by pid, and ignores a forged marker", () => {
    const marker = "--max-old-space-size=12288";
    const applied = decideYoungGeneration(
      view({
        env: {
          NODE_OPTIONS: `${marker} ${flag} --trace-warnings`,
          [INHERITED_NODE_OPTIONS_ENV]: marker,
          [pidEnv]: "4242",
        },
      }),
    );
    expect(applied.kind).toBe("applied");
    if (applied.kind !== "applied") return;
    expect(applied.nodeOptionsForChildren).toBe(`${marker} --trace-warnings`);
    expect(applied.outcome.applied).toBe(true);
    const guarded = decideYoungGeneration(
      view({ env: { NODE_OPTIONS: marker, [pidEnv]: "4242" } }),
    );
    expect(guarded.kind).toBe("keep");
    expect(guarded.outcome.keptReason).toBe("relaunched once already");
    const forged = decideYoungGeneration(
      view({ env: { NODE_OPTIONS: flag, [INHERITED_NODE_OPTIONS_ENV]: "--require /nonexistent" } }),
    );
    expect(forged.kind).toBe("keep");
    expect(forged.outcome.keptReason).toBe("disabled or set explicitly");
  });
});

describe("runtimeFlagsOf", () => {
  it("flags a server that wanted the cap and runs without it, and nothing else", () => {
    // Info hid a dead default from the fleet's logs; the server logs this at Warn.
    const kept = (keptReason: string, env: Record<string, string | undefined> = {}) =>
      runtimeFlagsOf({ applied: false, nodeOptionsAtStart: env.NODE_OPTIONS, keptReason }, env);
    expect(kept("started with a preload").youngGenerationLost).toBe(true);
    expect(kept("started with a preload").keptReason).toBe("started with a preload");
    expect(kept("started with a bootstrap descriptor").youngGenerationLost).toBe(true);
    expect(kept("started with a preload").structuralKeep).toBeUndefined();
    const structural = runtimeFlagsOf(
      {
        applied: false,
        nodeOptionsAtStart: undefined,
        keptReason: "started with a bootstrap descriptor",
        structural: true,
      },
      {},
    );
    expect(structural.youngGenerationLost).toBe(true);
    expect(structural.structuralKeep).toBe(true);
    expect(kept("NODE_OPTIONS changed since startup").youngGenerationLost).toBe(true);
    expect(
      kept("disabled or set explicitly", { NODE_OPTIONS: "--max-semi-space-size=16" })
        .youngGenerationLost,
    ).toBeUndefined();
    expect(
      kept("disabled or set explicitly", { [MAX_SEMI_SPACE_ENV]: "0" }).youngGenerationLost,
    ).toBeUndefined();
    expect(
      runtimeFlagsOf({ applied: true, nodeOptionsAtStart: flag }, { NODE_OPTIONS: undefined })
        .youngGenerationLost,
    ).toBeUndefined();
  });

  it("reports the startup value it knows, never the current environment in its place", () => {
    // NODE_OPTIONS was absent at startup; a command-line preload set a cap afterwards. V8 runs with
    // Node's default, and the log must say so without claiming uncertainty.
    const flags = runtimeFlagsOf(
      { applied: false, nodeOptionsAtStart: undefined, keptReason: "started with a preload" },
      { NODE_OPTIONS: "--max-semi-space-size=16" },
    );
    expect(flags.maxSemiSpaceMiB).toBe("node default");
    // Without an outcome (no startup pass ran) the current environment is all there is.
    expect(
      runtimeFlagsOf(undefined, { NODE_OPTIONS: "--max-semi-space-size=16" }).maxSemiSpaceMiB,
    ).toBe(16);
  });
});

describe("planYoungGenerationRelaunch", () => {
  it("relaunches a process at most once: the image that carries its own pid keeps, whatever a preload did", () => {
    // A preload can rewrite or delete NODE_OPTIONS in the new image; the pid-bound guard ends the chain.
    const first = plan({ pid: 4242 });
    expect(first.kind).toBe("relaunch");
    expect(
      first.kind === "relaunch" ? first.env.T3CODE_YOUNG_GENERATION_RELAUNCHED_PID : undefined,
    ).toBe("4242");
    expect(plan({ pid: 4242, relaunchedPid: "4242" })).toEqual({
      kind: "keep",
      reason: "relaunched once already",
    });
    // An inherited value from an ancestor never matches this process.
    expect(plan({ pid: 4242, relaunchedPid: "7" }).kind).toBe("relaunch");
  });

  it("names a service launcher that predates the default when it starts the server over IPC without the flag", () => {
    // A remote update replaces the binary but not the running launcher: the RC2 launcher supplies no
    // flag, and a server started with an IPC channel cannot relaunch itself. Say so, visibly.
    const result = plan({
      hasIpcChannel: true,
      env: { [SERVICE_LAUNCHER_CONTEXT_ENV]: '{"serviceName":"t3code"}' },
    });
    expect(result).toEqual({
      kind: "keep",
      reason: expect.stringContaining(
        "service launcher that predates the young-generation default",
      ),
      launcherPredatesDefault: true,
    });
    expect(plan({ hasIpcChannel: true })).toEqual({
      kind: "keep",
      reason: "started with an IPC channel",
    });
    // An up-to-date launcher that disabled the cap through the escape hatch is not an old launcher.
    expect(
      plan({
        hasIpcChannel: true,
        env: { [SERVICE_LAUNCHER_CONTEXT_ENV]: "{}", [MAX_SEMI_SPACE_ENV]: "0" },
      }),
    ).toEqual({ kind: "keep", reason: "started with an IPC channel" });
  });

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
  it("sees a preload in every spelling Node accepts, in execArgv and in NODE_OPTIONS", () => {
    // Node reads `_` as `-` in option names; a loader given as --experimental_loader ran twice.
    for (const execArgv of [
      ["--experimental_loader", "x.mjs"],
      ["--experimental-loader=x.mjs"],
      ["-r", "x.cjs"],
      ["--import=x.mjs"],
    ]) {
      const result = plan({ execArgv });
      expect(result.kind, execArgv.join(" ")).toBe("keep");
      if (result.kind === "keep") expect(result.reason).toBe("started with a preload");
    }
    for (const NODE_OPTIONS of ["--experimental_loader=x.mjs", "--require x.cjs", "-r x.cjs"]) {
      const result = plan({ env: { NODE_OPTIONS } });
      expect(result.kind, NODE_OPTIONS).toBe("keep");
      if (result.kind === "keep") expect(result.reason).toBe("started with a preload");
    }
  });

  it("tells a keep the start itself rules out from one the environment caused", () => {
    // The server logs a lost cap at Warn only when something changeable lost it; a bootstrap
    // descriptor, an IPC channel without a launcher, the inspector or no execve is how it was started.
    for (const structural of [
      plan({ hasIpcChannel: true }),
      plan({ argv: ["node", "bin.mjs", "serve", "--bootstrap-fd", "3"] }),
      plan({ inspectorActive: true }),
      plan({ canExecve: false }),
    ]) {
      expect(structural.kind).toBe("keep");
      if (structural.kind === "keep") expect(structural.structural, structural.reason).toBe(true);
    }
    for (const lossy of [
      plan({ execArgv: ["-r", "x.cjs"] }),
      plan({ hasIpcChannel: true, env: { [SERVICE_LAUNCHER_CONTEXT_ENV]: "1" } }),
    ]) {
      expect(lossy.kind).toBe("keep");
      if (lossy.kind === "keep") expect(lossy.structural, lossy.reason).toBeUndefined();
    }
  });

  it("keeps the process when the default is already in effect, disabled or explicit", () => {
    expect(plan({ env: { [INHERITED_NODE_OPTIONS_ENV]: "", NODE_OPTIONS: flag } }).kind).toBe(
      "keep",
    );
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

// The relaunch needs process.execve (absent on Windows); without it the entrypoint keeps Node's default by design.
const canRelaunch = typeof process.execve === "function";

describe.skipIf(!canRelaunch)("the entrypoint", () => {
  const run = Effect.fn("youngGeneration.test.run")(function* (
    env: Record<string, string | undefined>,
    args: ReadonlyArray<string> = [],
    nodeArgs: ReadonlyArray<string> = [],
  ) {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const child = yield* spawner.spawn(
      ChildProcess.make(
        process.execPath,
        ["--experimental-strip-types", ...nodeArgs, binPath, "__node-flags", ...args],
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

  it.live("ignores a marker that does not match what the relaunch would have written", () =>
    Effect.gen(function* () {
      // A forged marker beside the flag must not hand children text the server never ran with.
      const forged = yield* run({
        NODE_OPTIONS: flag,
        [INHERITED_NODE_OPTIONS_ENV]: "--require /nonexistent",
      });
      expect(forged.applied).toBe(false);
      expect(forged.nodeOptionsForChildren).toBe(flag);
      // An empty marker beside an explicit cap is exactly what an up-to-date launcher writes when the
      // escape hatch sets the cap, so it is honoured: children get the launcher's own NODE_OPTIONS.
      const launched = yield* run({
        NODE_OPTIONS: "--max-semi-space-size=16",
        [INHERITED_NODE_OPTIONS_ENV]: "",
      });
      expect(launched.applied).toBe(true);
      expect(launched.maxSemiSpaceMb).toBe(16);
      expect(launched.nodeOptionsForChildren).toBeNull();
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live(
    "relaunches exactly once even when a preload rewrites or deletes NODE_OPTIONS in the new image",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const dir = yield* fs.makeTempDirectoryScoped({ prefix: "young-generation-loop-" });
        const countFile = NodePath.join(dir, "images");
        const preload = (body: string) => {
          const file = NodePath.join(dir, `${body.length}.cjs`);
          NodeFS.writeFileSync(
            file,
            `require("node:fs").appendFileSync(process.env.RELAUNCH_COUNT_FILE, process.pid + "\\n");\n${body}\n`,
          );
          return file;
        };
        // The preload counts the images it ran in; a looping entrypoint never exits, so the timeout
        // ends it. The report comes from the image that finished, if one did.
        const images = (file: string, env: Record<string, string>, args: ReadonlyArray<string>) => {
          NodeFS.writeFileSync(countFile, "");
          const result = NodeChildProcess.spawnSync(
            process.execPath,
            ["--experimental-strip-types", ...args, binPath, "__node-flags"],
            {
              env: { ...process.env, ...cleanEnv, ...env, RELAUNCH_COUNT_FILE: countFile },
              timeout: 5000,
              encoding: "utf8",
            },
          );
          const preloadRuns = NodeFS.readFileSync(countFile, "utf8")
            .split("\n")
            .filter(Boolean).length;
          const report =
            result.status === 0 ? (JSON.parse(result.stdout) as NodeFlagsReport) : undefined;
          return { preloadRuns, report };
        };
        // A preload on the command line or in NODE_OPTIONS is visible: NODE_OPTIONS as seen now may
        // not be what Node started with, so the process keeps Node's default — one image, no relaunch.
        const strip = preload("delete process.env.NODE_OPTIONS;");
        const stripped = images(strip, {}, ["--require", strip]);
        expect(stripped.preloadRuns).toBe(1);
        expect(stripped.report?.applied).toBe(false);
        const rewrite = preload('process.env.NODE_OPTIONS = "--require " + __filename;');
        const rewritten = images(rewrite, { NODE_OPTIONS: `--require ${rewrite}` }, []);
        expect(rewritten.preloadRuns).toBe(1);
        expect(rewritten.report?.applied).toBe(false);
        // A NODE_OPTIONS preload that deletes NODE_OPTIONS hides its flag, but not the module it loaded:
        // the first image sees it in the module cache and keeps, so an explicit cap beside it survives.
        const hidden = preload("delete process.env.NODE_OPTIONS;");
        const hiddenRun = images(hidden, { NODE_OPTIONS: `--require ${hidden}` }, []);
        expect(hiddenRun.preloadRuns).toBe(1);
        expect(hiddenRun.report?.applied).toBe(false);
        const hiddenCap = images(
          hidden,
          { NODE_OPTIONS: `--max-semi-space-size=16 --require ${hidden}` },
          [],
        );
        expect(hiddenCap.preloadRuns).toBe(1);
        expect(hiddenCap.report?.applied).toBe(false);
        // The same with an ES module preload, which leaves nothing in the CommonJS cache: the process's
        // startup environment, read from the OS, still shows the preload and the explicit cap.
        const hiddenModule = NodePath.join(dir, "hidden.mjs");
        NodeFS.writeFileSync(
          hiddenModule,
          'import { appendFileSync } from "node:fs";\nappendFileSync(process.env.RELAUNCH_COUNT_FILE, process.pid + "\\n");\ndelete process.env.NODE_OPTIONS;\n',
        );
        const hiddenEsm = images(
          hiddenModule,
          { NODE_OPTIONS: `--max-semi-space-size=16 --import ${hiddenModule}` },
          [],
        );
        expect(hiddenEsm.preloadRuns).toBe(1);
        // Linux reads the startup record and keeps. Elsewhere nothing can see an ES-module preload
        // that removed itself: one relaunch without it, and the explicit cap becomes the default —
        // the documented residual for `node` starts; a single executable runs no such preload.
        expect(hiddenEsm.report?.applied).toBe(HostProcess.Platform.defaultValue() !== "linux");
        // An explicit cap beside a preload that clears NODE_OPTIONS must not be relaunched over with 64.
        const clearing = preload("delete process.env.NODE_OPTIONS;");
        const cleared = images(clearing, { NODE_OPTIONS: "--max-semi-space-size=16" }, [
          "--require",
          clearing,
        ]);
        expect(cleared.preloadRuns).toBe(1);
        expect(cleared.report?.applied).toBe(false);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    "gives children the launcher's NODE_OPTIONS without the cap even when a preload appended to it",
    () =>
      Effect.gen(function* () {
        // A launcher-started server: the marker is the user's NODE_OPTIONS (preload included) and
        // NODE_OPTIONS is that plus the cap; the preload appends an option at runtime.
        const fs = yield* FileSystem.FileSystem;
        const dir = yield* fs.makeTempDirectoryScoped({ prefix: "young-generation-append-" });
        const append = NodePath.join(dir, "append.cjs");
        NodeFS.writeFileSync(
          append,
          'process.env.NODE_OPTIONS = (process.env.NODE_OPTIONS ?? "") + " --trace-warnings";\n',
        );
        const marker = `--no-warnings --require ${append}`;
        const report = yield* run({
          NODE_OPTIONS: `${marker} ${flag}`,
          [INHERITED_NODE_OPTIONS_ENV]: marker,
        });
        expect(report.applied).toBe(true);
        expect(report.maxSemiSpaceMb).toBe(DEFAULT_MAX_SEMI_SPACE_MB);
        expect(report.nodeOptionsForChildren).toBe(`${marker} --trace-warnings`);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    "hands children the launcher's quoted preload exactly as written, and a child starts with it",
    () =>
      Effect.gen(function* () {
        // The cap is removed from NODE_OPTIONS as text; the rest is never re-tokenised, so quoting survives.
        const marker = '--import "data:text/javascript,void 0"';
        const report = yield* run({
          NODE_OPTIONS: `${marker} ${flag}`,
          [INHERITED_NODE_OPTIONS_ENV]: marker,
        });
        expect(report.applied).toBe(true);
        expect(report.nodeOptionsForChildren).toBe(marker);
        const child = NodeChildProcess.spawnSync(process.execPath, ["-e", "0"], {
          env: { ...process.env, NODE_OPTIONS: report.nodeOptionsForChildren ?? "" },
          encoding: "utf8",
        });
        expect(child.status, child.stderr).toBe(0);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live(
    "removes only the cap it inserted, never flag-like text inside a quoted payload a preload appended",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const dir = yield* fs.makeTempDirectoryScoped({ prefix: "young-generation-payload-" });
        const append = NodePath.join(dir, "append.cjs");
        // The payload a preload appends contains the flag's text inside quotes: it is data, not a flag.
        const payload =
          "--import \"data:text/javascript,globalThis.note=' --max-semi-space-size=64 '\"";
        NodeFS.writeFileSync(
          append,
          `process.env.NODE_OPTIONS = (process.env.NODE_OPTIONS ?? "") + ${JSON.stringify(` ${payload}`)};\n`,
        );
        const marker = `--no-warnings --require ${append}`;
        const report = yield* run({
          NODE_OPTIONS: `${marker} ${flag}`,
          [INHERITED_NODE_OPTIONS_ENV]: marker,
        });
        expect(report.applied).toBe(true);
        expect(report.nodeOptionsForChildren).toBe(`${marker} ${payload}`);
        const child = NodeChildProcess.spawnSync(process.execPath, ["-e", "0"], {
          env: { ...process.env, NODE_OPTIONS: report.nodeOptionsForChildren ?? "" },
          encoding: "utf8",
        });
        expect(child.status, child.stderr).toBe(0);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
    "reports the cap Node started with even when a preload deleted NODE_OPTIONS, from the startup record",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const dir = yield* fs.makeTempDirectoryScoped({ prefix: "young-generation-record-" });
        const clearing = NodePath.join(dir, "clearing.mjs");
        NodeFS.writeFileSync(clearing, "delete process.env.NODE_OPTIONS;\n");
        const report = yield* run({
          NODE_OPTIONS: `--max-semi-space-size=16 --import ${clearing}`,
        });
        expect(report.applied).toBe(false);
        expect(report.maxSemiSpaceMb).toBe(16);
        expect(report.nodeOptionsAtStart).toBe(`--max-semi-space-size=16 --import ${clearing}`);
        expect(report.nodeOptionsForChildren).toBeNull();
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("keeps Node's default and says so when started with a preload", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "young-generation-preload-" });
      const quiet = NodePath.join(dir, "quiet.mjs");
      NodeFS.writeFileSync(quiet, "export {};\n");
      const report = yield* run({}, [], ["--import", quiet]);
      expect(report.applied).toBe(false);
      expect(report.keptReason).toContain("preload");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
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

describe("review follow-ups", () => {
  it("ignores a marker that is not backed by the flag (M1)", () => {
    // A stray or stale marker must neither suppress the relaunch nor rewrite NODE_OPTIONS for children.
    const stray = plan({
      env: { [INHERITED_NODE_OPTIONS_ENV]: "", NODE_OPTIONS: "--max-old-space-size=16384" },
    });
    expect(stray.kind).toBe("relaunch");
    if (stray.kind !== "relaunch") return;
    expect(stray.env.NODE_OPTIONS).toBe(`--max-old-space-size=16384 ${flag}`);
    expect(stray.env[INHERITED_NODE_OPTIONS_ENV]).toBe("--max-old-space-size=16384");
    expect(plan({ env: { [INHERITED_NODE_OPTIONS_ENV]: "", NODE_OPTIONS: flag } }).kind).toBe(
      "keep",
    );
  });
  it("keeps the process when the inspector is active (M2)", () => {
    expect(plan({ execArgv: ["--inspect"] }).kind).toBe("keep");
    expect(plan({ execArgv: ["--inspect-brk=9230"] }).kind).toBe("keep");
    expect(plan({ env: { NODE_OPTIONS: "--inspect-port=9229" } }).kind).toBe("keep");
    expect(plan({ inspectorActive: true }).kind).toBe("keep");
    expect(plan({ execArgv: ["--experimental-strip-types"] }).kind).toBe("relaunch");
  });
  it("never stats the file system for the fast-path and known commands, nor before the cheap keep checks (N2)", () => {
    const calls: Array<string> = [];
    const spy = (path: string) => {
      calls.push(path);
      return true;
    };
    for (const command of [
      "acp-mcp-bridge",
      "acp-mcp-call",
      "__document-text",
      "__managed-chatgpt-token",
      "__process-spawner",
      "auth",
      "doctor",
      "up",
      "service",
      "connect",
      "project",
      "thread",
    ]) {
      expect(isServerCommand(["node", "bin.mjs", command], spy), command).toBe(false);
    }
    expect(calls).toEqual([]);
    expect(
      plan({ argv: ["node", "bin.mjs", "my-project"], hasIpcChannel: true, isDirectory: spy }).kind,
    ).toBe("keep");
    expect(
      plan({
        argv: ["node", "bin.mjs", "my-project"],
        env: { [MAX_SEMI_SPACE_ENV]: "0" },
        isDirectory: spy,
      }).kind,
    ).toBe("keep");
    expect(calls).toEqual([]);
    expect(plan({ argv: ["node", "bin.mjs", "my-project"], isDirectory: spy }).kind).toBe(
      "relaunch",
    );
    expect(calls).toEqual(["my-project"]);
  });
  it("bounds the escape hatch (N3)", () => {
    expect(resolveMaxSemiSpaceMb({ [MAX_SEMI_SPACE_ENV]: "1024" }, [])).toBe(1024);
    expect(resolveMaxSemiSpaceMb({ [MAX_SEMI_SPACE_ENV]: "1025" }, [])).toBe(
      DEFAULT_MAX_SEMI_SPACE_MB,
    );
    expect(resolveMaxSemiSpaceMb({ [MAX_SEMI_SPACE_ENV]: "100000" }, [])).toBe(
      DEFAULT_MAX_SEMI_SPACE_MB,
    );
  });
});

// @effect-diagnostics nodeBuiltinImport:off - Tests build real fixture templates on disk.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import {
  benchEnvHash,
  driverHash,
  fakesHash,
  fixtureInfo,
  launchersHash,
  readDriverResult,
  runOrder,
  traceRequired,
  transcriptsHash,
} from "./run.ts";

const withTempDir = async (run: (directory: string) => Promise<void>) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-run-"));
  try {
    await run(directory);
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
};

describe("readDriverResult", () => {
  it("reads the P1-004 driver.json: window, per-call latencies and event rates", () => {
    const summary = readDriverResult({
      schemaVersion: 1,
      status: "ok",
      knobs: { sessions: 80, seed: "1" },
      timeline: { warmup: "2026-10-31T21:00:00.000Z", window: "2026-10-31T21:01:00.000Z" },
      windowSeconds: 300.004,
      calls: {
        "mcp.t3_project_list": {
          count: 290,
          errors: 1,
          unfinished: 0,
          perSecond: 0.97,
          p50: 950,
          p95: 9800,
          p99: 12_000,
          max: 15_000,
        },
      },
      counters: {
        "events.thread": { count: 123_750, perSecond: 412.5, bytes: 1 },
        "events.shell": { count: 28_800, perSecond: 96, bytes: 1 },
      },
    });

    expect(summary).toEqual({
      ok: true,
      error: null,
      window: {
        startMs: Date.parse("2026-10-31T21:01:00.000Z"),
        endMs: Date.parse("2026-10-31T21:06:00.004Z"),
      },
      thread_events_per_s: 412.5,
      shell_events_per_s: 96,
      calls: {
        "mcp.t3_project_list": {
          count: 290,
          errors: 1,
          unfinished: 0,
          per_s: 0.97,
          p50_ms: 950,
          p95_ms: 9800,
          p99_ms: 12_000,
          max_ms: 15_000,
        },
      },
      knobs: { sessions: 80, seed: "1" },
    });
  });

  it("treats a failed run as failed and keeps absent numbers null", () => {
    const summary = readDriverResult({
      schemaVersion: 1,
      status: "failed",
      failure: { phase: "window", reason: "the server exited" },
      timeline: { window: "2026-10-31T21:01:00.000Z" },
    });

    expect(summary.ok).toBe(false);
    expect(summary.error).toBe("the server exited");
    expect(summary.window).toBeNull();
    expect(summary.thread_events_per_s).toBeNull();
    expect(summary.calls).toBeNull();
  });

  it("keeps the calls still running when the grace period ended", () => {
    const summary = readDriverResult({
      status: "ok",
      timeline: { window: "2026-10-31T21:01:00.000Z" },
      windowSeconds: 10,
      calls: {
        "mcp.t3_thread_read": {
          count: 1,
          errors: 0,
          unfinished: 99,
          perSecond: 10,
          p50: 4,
          p95: 4,
          p99: 4,
          max: 4,
        },
      },
    });

    expect(summary.calls?.["mcp.t3_thread_read"]).toEqual({
      count: 1,
      errors: 0,
      unfinished: 99,
      per_s: 10,
      p50_ms: 4,
      p95_ms: 4,
      p99_ms: 4,
      max_ms: 4,
    });
  });
});

describe("benchEnvHash", () => {
  it("tells raw load settings apart and ignores where things live", () => {
    const base = { BENCH_SPEED: "2", BENCH_TRANSCRIPT: "/srv/a.ndjson", T3CODE_LOG_LEVEL: "Info" };

    expect(benchEnvHash(base)).toBe(
      benchEnvHash({ ...base, BENCH_STATS_DIR: "/tmp/run-2", BENCH_NODE: "/usr/bin/node" }),
    );
    expect(benchEnvHash(base)).not.toBe(
      benchEnvHash({ ...base, BENCH_TRANSCRIPT: "/srv/b.ndjson" }),
    );
    expect(benchEnvHash(base)).toBe(benchEnvHash({ ...base, T3CODE_LOG_LEVEL: "Debug" }));
  });
});

describe("runOrder", () => {
  it("counterbalances the refs round by round (A B B A ...)", () => {
    expect(runOrder(["a", "b"], 4)).toEqual(["a", "b", "b", "a", "a", "b", "b", "a"]);
    expect(runOrder(["a", "b", "c"], 3)).toEqual(["a", "b", "c", "c", "b", "a", "a", "b", "c"]);
    expect(runOrder(["a"], 3)).toEqual(["a", "a", "a"]);
  });
});

describe("traceRequired", () => {
  it("expects spans unless the trace level drops them", () => {
    expect(traceRequired({})).toBe(true);
    expect(traceRequired({ T3CODE_TRACE_MIN_LEVEL: "Debug" })).toBe(true);
    expect(traceRequired({ T3CODE_TRACE_MIN_LEVEL: "Warn" })).toBe(false);
    expect(traceRequired({ T3CODE_TRACE_MIN_LEVEL: "none" })).toBe(false);
  });

  it("expects no timed trace when trace timing is off", () => {
    // The server's spans are untimed then (start and end 0), so every window reads trace_stale.
    expect(traceRequired({ T3CODE_TRACE_TIMING_ENABLED: "false" })).toBe(false);
    expect(traceRequired({ T3CODE_TRACE_TIMING_ENABLED: "0" })).toBe(false);
    expect(
      traceRequired({ T3CODE_TRACE_TIMING_ENABLED: "off", T3CODE_TRACE_MIN_LEVEL: "Debug" }),
    ).toBe(false);
    expect(traceRequired({ T3CODE_TRACE_TIMING_ENABLED: "true" })).toBe(true);
    expect(traceRequired({ T3CODE_TRACE_TIMING_ENABLED: "yes" })).toBe(true);
  });
});

describe("fixtureInfo", () => {
  it("refuses a template whose database or userdata is a symlink out of it", async () => {
    await withTempDir(async (root) => {
      const work = NodePath.join(root, "work");
      NodeFS.mkdirSync(work);
      const outside = NodePath.join(root, "outside");
      NodeFS.mkdirSync(outside);
      NodeFS.writeFileSync(NodePath.join(outside, "statev2.sqlite"), "original");
      const linkedFile = NodePath.join(root, "linked-file");
      NodeFS.mkdirSync(NodePath.join(linkedFile, "userdata"), { recursive: true });
      NodeFS.symlinkSync(
        NodePath.join(outside, "statev2.sqlite"),
        NodePath.join(linkedFile, "userdata", "statev2.sqlite"),
      );
      const linkedDir = NodePath.join(root, "linked-dir");
      NodeFS.mkdirSync(linkedDir);
      NodeFS.symlinkSync(outside, NodePath.join(linkedDir, "userdata"));

      await expect(fixtureInfo(linkedFile, work)).rejects.toThrow(
        /symlink at userdata\/statev2\.sqlite/,
      );
      await expect(fixtureInfo(linkedDir, work)).rejects.toThrow(/symlink at userdata\b/);
    });
  });

  it("hashes a template of regular files", async () => {
    await withTempDir(async (root) => {
      const work = NodePath.join(root, "work");
      NodeFS.mkdirSync(work);
      const template = NodePath.join(root, "template");
      NodeFS.mkdirSync(NodePath.join(template, "userdata"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(template, "userdata", "statev2.sqlite"), "database");
      NodeFS.writeFileSync(NodePath.join(template, "userdata", "settings.json"), "{}");

      const info = await fixtureInfo(template, work);

      expect(info).toMatchObject({ kind: "dir", bytes: 10 });
      expect(info.sha256).toMatch(/^[0-9a-f]{64}$/);
    });
  });
});

const write = (path: string, content: string) => {
  NodeFS.mkdirSync(NodePath.dirname(path), { recursive: true });
  NodeFS.writeFileSync(path, content);
};

describe("workload fingerprint", () => {
  it("covers the local modules the driver imports outside its module directory", async () => {
    await withTempDir(async (bench) => {
      const driver = NodePath.join(bench, "driver.ts");
      write(driver, 'import { plan } from "./providers.ts";\nimport { a } from "./driver/a.ts";\n');
      write(NodePath.join(bench, "driver", "a.ts"), 'export { b } from "../shared/b.ts";\n');
      write(NodePath.join(bench, "shared", "b.ts"), "export const b = 1;\n");
      write(NodePath.join(bench, "providers.ts"), "export const plan = 1;\n");
      write(NodePath.join(bench, "unrelated.ts"), "export const c = 1;\n");
      const first = await driverHash(driver);

      write(NodePath.join(bench, "unrelated.ts"), "export const c = 2;\n");
      expect(await driverHash(driver)).toBe(first);
      write(NodePath.join(bench, "providers.ts"), "export const plan = 2;\n");
      const second = await driverHash(driver);
      expect(second).not.toBe(first);
      write(NodePath.join(bench, "shared", "b.ts"), "export const b = 2;\n");
      expect(await driverHash(driver)).not.toBe(second);
    });
  });

  it("covers the fakes, their shared code and what they import from the server", async () => {
    await withTempDir(async (root) => {
      const bench = NodePath.join(root, "tools", "server-bench");
      write(NodePath.join(bench, "fake-claude", "claude"), "#!/bin/sh\n");
      write(NodePath.join(bench, "fake-claude", "claude.mjs"), "export {};\n");
      write(
        NodePath.join(bench, "fake-codex", "codex.mjs"),
        'import "../fake-common/replay.mjs";\n',
      );
      write(
        NodePath.join(bench, "fake-acp", "acp-agent.mjs"),
        'import { parse } from "../../../apps/server/scripts/replay.ts";\n',
      );
      write(NodePath.join(bench, "fake-acp", "transcripts", "large.mjs"), "export {};\n");
      write(NodePath.join(bench, "fake-acp", "README.md"), "docs\n");
      write(NodePath.join(bench, "fake-acp", "acp-agent.test.mjs"), "test\n");
      write(NodePath.join(bench, "fake-common", "replay.mjs"), "export {};\n");
      write(NodePath.join(root, "apps", "server", "scripts", "replay.ts"), "export {};\n");
      const hashes = [await fakesHash(bench)];
      const edit = async (path: string, changes: boolean) => {
        write(path, `${NodeFS.readFileSync(path, "utf8")}// edited\n`);
        const hash = await fakesHash(bench);
        expect(hashes.includes(hash)).toBe(!changes);
        hashes.push(hash);
      };

      await edit(NodePath.join(bench, "fake-acp", "README.md"), false);
      await edit(NodePath.join(bench, "fake-acp", "acp-agent.test.mjs"), false);
      await edit(NodePath.join(bench, "fake-claude", "claude"), true);
      await edit(NodePath.join(bench, "fake-codex", "codex.mjs"), true);
      await edit(NodePath.join(bench, "fake-acp", "transcripts", "large.mjs"), true);
      await edit(NodePath.join(bench, "fake-common", "replay.mjs"), true);
      await edit(NodePath.join(root, "apps", "server", "scripts", "replay.ts"), true);
    });
  });

  it("covers the launchers the fixture's settings run from another checkout or elsewhere", async () => {
    await withTempDir(async (root) => {
      const harness = NodePath.join(root, "harness", "tools", "server-bench");
      write(NodePath.join(harness, "fake-codex", "codex"), "#!/bin/sh\n");
      const other = NodePath.join(root, "other");
      const otherBench = NodePath.join(other, "tools", "server-bench");
      write(NodePath.join(otherBench, "fake-claude", "claude"), "#!/bin/sh\n");
      write(
        NodePath.join(otherBench, "fake-claude", "claude.mjs"),
        'import "../../../apps/server/scripts/replay.ts";\n',
      );
      write(NodePath.join(other, "apps", "server", "scripts", "replay.ts"), "export {};\n");
      const recording = NodePath.join(
        other,
        "apps/server/src/orchestration-v2/testkit/fixtures/simple/claude_transcript.ndjson",
      );
      write(recording, "{}\n");
      const agent = NodePath.join(root, "agents", "agent.mjs");
      write(agent, 'import "./lib.mjs";\n');
      write(NodePath.join(root, "agents", "lib.mjs"), "export {};\n");
      const node = NodePath.join(root, "bin", "node");
      write(node, "#!/bin/sh\n");
      const template = NodePath.join(root, "template");
      write(
        NodePath.join(template, "userdata", "settings.json"),
        JSON.stringify({
          providerInstances: {
            claudeBench: {
              config: { binaryPath: NodePath.join(otherBench, "fake-claude", "claude") },
            },
            codexBench: { config: { binaryPath: NodePath.join(harness, "fake-codex", "codex") } },
            acpBench: { config: { commandPath: node, commandArgs: ["--no-warnings", agent] } },
            codex: { enabled: false },
          },
        }),
      );
      const work = NodePath.join(root, "work");
      NodeFS.mkdirSync(work);
      const fixture = await fixtureInfo(template, work);
      const hashes = [await launchersHash(fixture, harness)];
      const edit = async (path: string, changes: boolean) => {
        write(path, `${NodeFS.readFileSync(path, "utf8")}// edited\n`);
        const hash = await launchersHash(fixture, harness);
        expect(hashes.includes(hash)).toBe(!changes);
        hashes.push(hash);
      };

      // The harness's own fakes are fakes_sha256's; here only their path counts.
      await edit(NodePath.join(harness, "fake-codex", "codex"), false);
      await edit(NodePath.join(otherBench, "fake-claude", "claude"), true);
      await edit(NodePath.join(otherBench, "fake-claude", "claude.mjs"), true);
      await edit(NodePath.join(other, "apps", "server", "scripts", "replay.ts"), true);
      await edit(recording, true);
      await edit(agent, true);
      await edit(NodePath.join(root, "agents", "lib.mjs"), true);
      await edit(node, true);
    });
  });

  it("covers the recorded transcripts and the absolute ones the settings name", async () => {
    await withTempDir(async (root) => {
      const fixtures = NodePath.join(root, "replay-fixtures");
      write(NodePath.join(fixtures, "todo_list", "registry_transcript.ndjson"), "{}\n");
      write(NodePath.join(fixtures, "todo_list", "input.ts"), "export {};\n");
      const own = NodePath.join(root, "own", "claude.ndjson");
      write(own, "{}\n");
      const viaEnv = NodePath.join(root, "own", "codex.ndjson");
      write(viaEnv, "{}\n");
      const template = NodePath.join(root, "template");
      write(
        NodePath.join(template, "userdata", "settings.json"),
        JSON.stringify({
          providerInstances: {
            claudeBench: {
              environment: [{ name: "BENCH_TRANSCRIPT", value: `todo_list:2,${own}:3` }],
            },
          },
        }),
      );
      const work = NodePath.join(root, "work");
      NodeFS.mkdirSync(work);
      const fixture = await fixtureInfo(template, work);
      const env = { BENCH_CODEX_TRANSCRIPT: viaEnv, BENCH_SPEED: "1" };
      const first = await transcriptsHash(env, fixture, fixtures);

      write(NodePath.join(fixtures, "todo_list", "input.ts"), "export const changed = 1;\n");
      expect(await transcriptsHash(env, fixture, fixtures)).toBe(first);
      write(NodePath.join(fixtures, "todo_list", "registry_transcript.ndjson"), "{}\n{}\n");
      const second = await transcriptsHash(env, fixture, fixtures);
      expect(second).not.toBe(first);
      write(own, "{}\n{}\n");
      const third = await transcriptsHash(env, fixture, fixtures);
      expect(third).not.toBe(second);
      write(viaEnv, "{}\n{}\n");
      expect(await transcriptsHash(env, fixture, fixtures)).not.toBe(third);
    });
  });
});

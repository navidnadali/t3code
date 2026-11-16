// @effect-diagnostics nodeBuiltinImport:off - Tests build real fixture templates on disk.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it, onTestFinished } from "vite-plus/test";

import { unpackRelease } from "./release.ts";

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
  seaTarget,
  seaCommand,
  addedNodeOptions,
  checkNodeFlags,
  dbReadiness,
  dbReadySpans,
  scanTraceForReadiness,
  effectiveSemiSpaceMb,
  lastSizeFlag,
  measuredSemiSpaceMb,
  nodeFlagsFromProbe,
  nodeFlagsFromReport,
  nodeOptionTokens,
  parseNodeFlagsReport,
  serverV8LogEntry,
  type NodeFlags,
  HARNESS_VERSION,
  DEFAULT_BASE,
  HELP,
  SATURATION_ELU,
  classifyProblems,
  serverElu,
  checkResourceMonitor,
  checkSeaExecutable,
  checkSeaRef,
  driverFailure,
  harnessFilesSha256,
  parseArchiveSpecs,
  resolvePreload,
  saturation,
  stopGroup,
  verifyArchive,
  awaitLoopWindow,
  driverProblems,
  composeServerEnv,
  harnessIdentity,
  loopMetrics,
  parseCount,
  parseNow,
  preloadOutcome,
  restoreFixture,
  seedCompactedAt,
  windowControlFor,
} from "./run.ts";

/** No AppleDouble (._*) members from the macOS tar: the archive holds the tree and nothing else. */
const TAR_OPTIONS = { env: { ...process.env, COPYFILE_DISABLE: "1" } };

const withTempDir = async (run: (directory: string) => Promise<void>) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-run-"));
  try {
    await run(directory);
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
};

describe("readDriverResult", () => {
  it("reads the driver.json: window, per-call latencies and event rates", () => {
    const summary = readDriverResult({
      schemaVersion: 1,
      status: "ok",
      knobs: { sessions: 24, seed: "1" },
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
      knobs: { sessions: 24, seed: "1" },
      workload: null,
      failure_code: null,
      failure_phase: null,
      findings: null,
      sessions_served: null,
      sessions_total: null,
    });
  });

  it("records the mode, the fixture/new thread split and the distribution match", () => {
    const match = { withinTolerance: true, p50Ratio: 1.02, p95Ratio: 0.97 };
    const mixed = readDriverResult({
      status: "ok",
      knobs: { mode: "mixed" },
      sessions: { count: 24 },
      workload: { sessionThreadIds: ["a", "b"], freshSessions: 22, sessionCandidates: 2 },
      distribution: { match },
    });
    expect(mixed.workload).toEqual({
      mode: "mixed",
      sessions: 24,
      fixture_thread_sessions: 2,
      new_thread_sessions: 22,
      session_candidates: 2,
      distribution_match: match,
    });
    const history = readDriverResult({
      status: "ok",
      knobs: { mode: "history" },
      sessions: { count: 2 },
      workload: { sessionThreadIds: ["a", "b"], sessionCandidates: 9 },
      distribution: { match },
    });
    expect(history.workload?.new_thread_sessions).toBe(0);
    const fresh = readDriverResult({
      status: "ok",
      knobs: { mode: "fresh" },
      sessions: { count: 8 },
      workload: { projects: 8 },
      distribution: { match: null },
    });
    expect(fresh.workload).toMatchObject({
      fixture_thread_sessions: 0,
      new_thread_sessions: 8,
      distribution_match: null,
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

describe("a window the server could not serve for every session", () => {
  const starved = {
    schemaVersion: 1,
    status: "failed",
    knobs: { sessions: 250 },
    timeline: { window: "2026-11-16T05:01:00.000Z" },
    windowSeconds: 300,
    calls: {
      "session.turn": {
        count: 87,
        errors: 0,
        unfinished: 0,
        perSecond: 0.29,
        p50: 1043,
        p95: 1082,
        p99: 1086,
        max: 1086,
      },
    },
    counters: {},
    sessions: { count: 250, withOutputInWindow: 87 },
    failure: {
      phase: "window",
      code: "sessions_starved",
      reason: "only 87 of 250 sessions produced provider output during the window",
      sessionsServed: 87,
      sessionsTotal: 250,
    },
  };

  it("keeps the statistics over the sessions served, with served and total as numbers", () => {
    const summary = readDriverResult(starved);
    expect(summary.ok).toBe(false);
    expect(summary.failure_code).toBe("sessions_starved");
    expect([summary.sessions_served, summary.sessions_total]).toEqual([87, 250]);
    expect(summary.calls?.["session.turn"]).toMatchObject({ count: 87, p50_ms: 1043 });
    expect(summary.window).not.toBeNull();
  });

  it("is its own problem, apart from a driver that broke, with the sessions served exact", () => {
    const summary = readDriverResult(starved);
    expect(driverProblems(summary, true)).toEqual(["sessions_starved"]);
    expect(saturation(summary)).toEqual({ sessions_served: 87, sessions_total: 250 });
    expect(saturation(null)).toEqual({ sessions_served: null, sessions_total: null });
    // A driver that names every finding gets each as its own problem.
    const several = readDriverResult({
      ...starved,
      failure: {
        ...starved.failure,
        code: "call_timeouts",
        findings: [
          { code: "call_timeouts", reason: "session.dispatch failed 30.00% of calls (limit 20%)" },
          {
            code: "sessions_starved",
            reason: "only 87 of 250 sessions produced provider output during the window",
          },
        ],
      },
    });
    expect(driverProblems(several, true)).toEqual(["call_timeouts", "sessions_starved"]);
    const broken = readDriverResult({
      ...starved,
      failure: { phase: "setup", reason: "provider instance missing" },
    });
    expect(driverProblems(broken, true)).toEqual(["driver_failed"]);
    expect(driverProblems(null, false)).toEqual(["driver_failed"]);
    expect(
      driverProblems(readDriverResult({ ...starved, status: "ok", failure: undefined }), false),
    ).toEqual([]);
    // A clean run reports its sessions too.
    const clean = readDriverResult({
      ...starved,
      status: "ok",
      failure: undefined,
      sessions: { count: 250, withOutputInWindow: 250 },
    });
    expect([clean.failure_code, clean.sessions_served, clean.sessions_total]).toEqual([
      null,
      250,
      250,
    ]);
    expect(saturation(clean)).toEqual({ sessions_served: 250, sessions_total: 250 });
  });
});

describe("readDriverResult keeps the driver's distribution tolerance", () => {
  it("records the tolerance the driver ran with, in its knobs and its distribution match", () => {
    const summary = readDriverResult({
      schemaVersion: 1,
      status: "ok",
      knobs: { mode: "mixed", distributionTolerance: 2.5 },
      distribution: {
        match: { p50Ratio: 3.125, p95Ratio: 1.204, tolerance: 2.5, withinTolerance: true },
      },
      timeline: { window: "2026-10-31T21:01:00.000Z" },
      windowSeconds: 120,
      calls: {},
      counters: {},
    });
    expect(summary.knobs?.distributionTolerance).toBe(2.5);
    expect(summary.workload?.distribution_match).toMatchObject({ tolerance: 2.5, p50Ratio: 3.125 });
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
      const node = NodePath.join(root, "bin", "node");
      write(node, "\u007fELF executable\n");
      const template = NodePath.join(root, "template");
      write(
        NodePath.join(template, "userdata", "settings.json"),
        JSON.stringify({
          providerInstances: {
            claudeBench: {
              config: { binaryPath: NodePath.join(otherBench, "fake-claude", "claude") },
            },
            codexBench: { config: { binaryPath: NodePath.join(harness, "fake-codex", "codex") } },
            acpBench: { config: { commandPath: node, commandArgs: ["--no-warnings"] } },
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
      await edit(node, true);
    });
  });

  it("refuses a launcher outside fake-* dirs whose code it cannot fingerprint", async () => {
    await withTempDir(async (root) => {
      const harness = NodePath.join(root, "harness", "tools", "server-bench");
      const wrapper = NodePath.join(root, "wrappers", "claude");
      write(wrapper, '#!/bin/sh\nexec node "$(dirname "$0")/claude.mjs" "$@"\n');
      write(NodePath.join(root, "wrappers", "claude.mjs"), "export {};\n");
      const agent = NodePath.join(root, "agents", "agent.cjs");
      write(agent, 'require("./lib.cjs");\n');
      write(NodePath.join(root, "agents", "lib.cjs"), "\n");
      write(NodePath.join(root, "agents", "agent"), 'require("./lib.cjs");\n');
      const node = NodePath.join(root, "bin", "node");
      write(node, "\u007fELF executable\n");
      const work = NodePath.join(root, "work");
      NodeFS.mkdirSync(work);
      for (const [index, config] of [
        { binaryPath: wrapper },
        { commandPath: node, commandArgs: [agent] },
        { binaryPath: NodePath.join(root, "wrappers", "claude.mjs") },
        // JavaScript without an extension or #!: node runs it all the same.
        { commandPath: node, commandArgs: [NodePath.join(root, "agents", "agent")] },
      ].entries()) {
        const template = NodePath.join(root, `template-${index}`);
        write(
          NodePath.join(template, "userdata", "settings.json"),
          JSON.stringify({ providerInstances: { bench: { config } } }),
        );
        const fixture = await fixtureInfo(template, work);
        await expect(launchersHash(fixture, harness)).rejects.toThrow(/outside a fake-\*/);
      }
    });
  });

  it("covers the contents of a directory a launcher runs", async () => {
    await withTempDir(async (root) => {
      const harness = NodePath.join(root, "harness", "tools", "server-bench");
      const node = NodePath.join(root, "bin", "node");
      write(node, "\u007fELF executable\n");
      // node runs a directory's package entry.
      const agent = NodePath.join(root, "agent");
      write(NodePath.join(agent, "package.json"), '{"main":"index.js"}\n');
      write(NodePath.join(agent, "index.js"), 'require("./lib/run.js");\n');
      write(NodePath.join(agent, "lib", "run.js"), "\n");
      const template = NodePath.join(root, "template");
      write(
        NodePath.join(template, "userdata", "settings.json"),
        JSON.stringify({
          providerInstances: { acpBench: { config: { commandPath: node, commandArgs: [agent] } } },
        }),
      );
      const work = NodePath.join(root, "work");
      NodeFS.mkdirSync(work);
      const fixture = await fixtureInfo(template, work);
      const hashes = [await launchersHash(fixture, harness)];
      const changed = async (change: () => void) => {
        change();
        const hash = await launchersHash(fixture, harness);
        expect(hashes).not.toContain(hash);
        hashes.push(hash);
      };

      await changed(() => write(NodePath.join(agent, "lib", "run.js"), "// edited\n"));
      await changed(() => write(NodePath.join(agent, "index.js"), 'require("./lib/run.js"); //\n'));
      await changed(() => write(NodePath.join(agent, "lib", "extra.js"), "\n"));
      await changed(() =>
        NodeFS.renameSync(
          NodePath.join(agent, "lib", "extra.js"),
          NodePath.join(agent, "extra.js"),
        ),
      );
      expect(await launchersHash(fixture, harness)).toBe(hashes.at(-1));
    });
  });

  it("ignores the launchers of disabled instances", async () => {
    await withTempDir(async (root) => {
      const harness = NodePath.join(root, "harness", "tools", "server-bench");
      const script = NodePath.join(root, "bin", "codex");
      write(script, "#!/bin/sh\n");
      const work = NodePath.join(root, "work");
      NodeFS.mkdirSync(work);
      const hashes = new Set<string>();
      // As providers.ts settings leaves a built-in: disabled, its config kept.
      for (const [index, codex] of [
        { driver: "codex", enabled: false, config: { binaryPath: script } },
        { driver: "codex", config: { enabled: false, binaryPath: script } },
        { driver: "codex", enabled: false },
      ].entries()) {
        const template = NodePath.join(root, `template-${index}`);
        write(
          NodePath.join(template, "userdata", "settings.json"),
          JSON.stringify({ providerInstances: { codex } }),
        );
        hashes.add(await launchersHash(await fixtureInfo(template, work), harness));
      }
      expect(hashes.size).toBe(1);
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

describe("seaTarget", () => {
  const sha = "41744e15c2a5b972b753a9cdc231a881875fe8f1";
  const manifest = (binary: string, extra = "") =>
    [
      `commit=${sha}`,
      "commit_subject=test(server): a streamed search gives its permit back",
      "dirty=no",
      "version=0.1.6-perf.1",
      "archive=t3-0.1.6-perf.1-linux-x64.tar.gz",
      "archive_sha256=b99c47659ffc880bc8c00d6894c63f0224f84df0f5c8218816f989f67544c9a7",
      `extracted_t3_sha256=${binary}`,
      extra,
      "",
    ].join("\n");
  const sha256 = (text: string) => NodeCrypto.createHash("sha256").update(text).digest("hex");
  const seaDir = (binary: string, manifestText: string | null) => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-sea-"));
    NodeFS.writeFileSync(NodePath.join(dir, "t3"), binary, { mode: 0o755 });
    if (manifestText !== null)
      NodeFS.writeFileSync(NodePath.join(dir, "build-manifest.txt"), manifestText);
    return dir;
  };

  it("reads the unpacked archive: commit, archive sha256 and the executable's own sha256", () => {
    const binary = "#!/bin/sh\necho 't3 v0.1.6-perf.1'\n";
    const dir = seaDir(binary, manifest(sha256(binary)));
    const target = seaTarget(dir);
    expect(target.entryKind).toBe("sea");
    expect(target.entry).toBe(NodePath.join(dir, "t3"));
    expect(target.sha).toBe(sha);
    expect(target.dirty).toBe(false);
    expect(target.tree).toBe(sha);
    expect(target.archive?.archive_sha256).toBe(
      "b99c47659ffc880bc8c00d6894c63f0224f84df0f5c8218816f989f67544c9a7",
    );
    expect(target.archive?.binary_sha256).toBe(sha256(binary));
    expect(target.archive?.manifest["version"]).toBe("0.1.6-perf.1");
  });

  it("marks a dirty build and refuses a binary that differs from the manifest", () => {
    const binary = "#!/bin/sh\necho 't3'\n";
    const dirty = seaDir(binary, manifest(sha256(binary), "").replace("dirty=no", "dirty=yes"));
    expect(seaTarget(dirty).tree).toBe(`${sha}+dirty`);
    const tampered = seaDir(binary, manifest(sha256("something else")));
    expect(() => seaTarget(tampered)).toThrow(/differs from the manifest/);
  });

  it("refuses a directory without the executable, without the manifest, or without a commit", () => {
    const binary = "#!/bin/sh\necho 't3'\n";
    expect(() => seaTarget(seaDir(binary, null))).toThrow(/no build-manifest.txt/);
    expect(() => seaTarget(seaDir(binary, "version=1\n"))).toThrow(/no 40-hex commit/);
    const noExe = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-sea-"));
    NodeFS.writeFileSync(NodePath.join(noExe, "build-manifest.txt"), manifest(sha256(binary)));
    expect(() => seaTarget(noExe)).toThrow(/no t3 executable/);
  });

  it("refuses a manifest that does not name the executable's sha256 (any binary could claim the commit)", () => {
    const binary = "#!/bin/sh\necho 't3'\n";
    const incomplete = [`commit=${sha}`, "dirty=no", ""].join("\n");
    expect(() => seaTarget(seaDir(binary, incomplete))).toThrow(/no 64-hex extracted_t3_sha256/);
    expect(() => seaTarget(seaDir(binary, manifest("not-a-hash")))).toThrow(
      /no 64-hex extracted_t3_sha256/,
    );
  });
});

describe("SEA Node options (exec-argv.json)", () => {
  const sha = "41744e15c2a5b972b753a9cdc231a881875fe8f1";
  const bench = "--max-old-space-size=8192 --require=/opt/bench/bench-preload.cjs";
  const binary = "#!/bin/sh\necho 't3'\n";
  const sea = (execArgvJson: string | null) => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-sea-argv-"));
    NodeFS.writeFileSync(NodePath.join(dir, "t3"), binary, { mode: 0o755 });
    const hash = NodeCrypto.createHash("sha256").update(binary).digest("hex");
    NodeFS.writeFileSync(
      NodePath.join(dir, "build-manifest.txt"),
      [`commit=${sha}`, "dirty=no", `extracted_t3_sha256=${hash}`, ""].join("\n"),
    );
    if (execArgvJson !== null)
      NodeFS.writeFileSync(NodePath.join(dir, "exec-argv.json"), execArgvJson);
    return dir;
  };

  it("starts a cli-mode executable with the bench's NODE_OPTIONS as a leading --node-options", () => {
    const target = seaTarget(
      sea(JSON.stringify({ execArgv: ["--max-semi-space-size=64"], execArgvExtension: "cli" })),
    );
    expect(target.archive?.exec_argv).toEqual({
      execArgv: ["--max-semi-space-size=64"],
      execArgvExtension: "cli",
    });
    // Verbatim: Node parses --node-options' value as it parses NODE_OPTIONS.
    expect(seaCommand(target, `${bench} "--title=a b"`)).toEqual([
      target.entry,
      `--node-options=${bench} "--title=a b"`,
    ]);
    // Nothing to pass: the executable starts with its baked options only.
    expect(seaCommand(target, undefined)).toEqual([target.entry]);
    expect(seaCommand(target, "  ")).toEqual([target.entry]);
  });

  it("leaves every other executable to read NODE_OPTIONS from the environment", () => {
    const plain = seaTarget(sea(null));
    expect(plain.archive?.exec_argv).toBeNull();
    expect(seaCommand(plain, bench)).toEqual([plain.entry]);
    const env = seaTarget(sea(JSON.stringify({ execArgv: [], execArgvExtension: "env" })));
    expect(seaCommand(env, bench)).toEqual([env.entry]);
  });

  it("refuses an exec-argv.json it cannot read as a declaration", () => {
    expect(() => seaTarget(sea("{not json"))).toThrow(/exec-argv.json is not JSON/);
    expect(() => seaTarget(sea(JSON.stringify({ execArgv: "x" })))).toThrow(/no string execArgv/);
    expect(() => seaTarget(sea(JSON.stringify({ execArgv: null })))).toThrow(/no string execArgv/);
    for (const root of ["null", "[]", "17", '"cli"']) {
      expect(() => seaTarget(sea(root))).toThrow(/not a declaration object/);
    }
  });
});

describe("startup flags (__node-flags)", () => {
  const bench = "--max-old-space-size=8192 --require=/opt/bench/bench-preload.cjs";
  // What ca33c1293's `t3 __node-flags` prints with the bench's NODE_OPTIONS (it relaunched itself once).
  const report = JSON.stringify({
    applied: true,
    maxSemiSpaceMb: 64,
    nodeOptionsAtStart: `${bench} --max-semi-space-size=64`,
    nodeOptionsForChildren: bench,
    execArgv: [],
    ipc: false,
    sea: true,
    pid: 1262598,
  });

  it("reads the report on the last line and ignores anything that is not one", () => {
    expect(parseNodeFlagsReport(`some warning\n${report}\n`)).toEqual({
      applied: true,
      maxSemiSpaceMb: 64,
      nodeOptionsAtStart: `${bench} --max-semi-space-size=64`,
      nodeOptionsForChildren: bench,
      keptReason: null,
    });
    expect(parseNodeFlagsReport("")).toBeNull();
    expect(parseNodeFlagsReport('ERROR\n  Unknown command "__node-flags".')).toBeNull();
    expect(parseNodeFlagsReport('{"maxSemiSpaceMb":64}')).toBeNull();
    expect(parseNodeFlagsReport("[1,2]")).toBeNull();
    // Incomplete or inconsistent reports are not reports: the bench stops instead of running without the check.
    expect(parseNodeFlagsReport('{"applied":true}')).toBeNull();
    expect(
      parseNodeFlagsReport('{"applied":true,"maxSemiSpaceMb":64,"nodeOptionsAtStart":null}'),
    ).toBeNull();
    expect(
      parseNodeFlagsReport(
        '{"applied":true,"maxSemiSpaceMb":64,"nodeOptionsAtStart":"--max-old-space-size=8192","nodeOptionsForChildren":null}',
      ),
    ).toBeNull();
    expect(
      parseNodeFlagsReport(
        '{"applied":true,"maxSemiSpaceMb":"64","nodeOptionsAtStart":"--max-semi-space-size=64","nodeOptionsForChildren":null}',
      ),
    ).toBeNull();
    expect(
      parseNodeFlagsReport('{"applied":false,"maxSemiSpaceMb":null,"nodeOptionsAtStart":null}'),
    ).toBeNull();
    expect(
      parseNodeFlagsReport(
        '{"applied":false,"maxSemiSpaceMb":16,"nodeOptionsAtStart":"--max-semi-space-size=16","nodeOptionsForChildren":"--max-semi-space-size=16","execArgv":[],"keptReason":"disabled or set explicitly"}',
      ),
    ).toMatchObject({
      applied: false,
      maxSemiSpaceMb: 16,
      keptReason: "disabled or set explicitly",
    });
    expect(
      parseNodeFlagsReport(
        '{"applied":false,"maxSemiSpaceMb":null,"nodeOptionsAtStart":"--max-old-space-size=8192","nodeOptionsForChildren":"--max-old-space-size=8192","keptReason":"disabled or set explicitly"}',
      ),
    ).toMatchObject({ applied: false, maxSemiSpaceMb: null });
    // --max-semi-space-size=0 asks Node for its default: a valid report, and nothing to check.
    const zero = parseNodeFlagsReport(
      '{"applied":false,"maxSemiSpaceMb":0,"nodeOptionsAtStart":"--max-semi-space-size=0","nodeOptionsForChildren":"--max-semi-space-size=0","execArgv":[],"keptReason":"disabled or set explicitly"}',
    );
    expect(zero).toMatchObject({ applied: false, maxSemiSpaceMb: 0 });
    expect(nodeFlagsFromReport(zero, "--max-semi-space-size=0")).toEqual({
      env: {},
      expectedSemiSpaceMb: null,
    });
    expect(
      parseNodeFlagsReport(
        '{"applied":true,"maxSemiSpaceMb":64,"nodeOptionsAtStart":"--max-semi-space-size=64","nodeOptionsForChildren":null,"execArgv":[1]}',
      ),
    ).toBeNull();
  });

  it("sets what the build's launcher sets: the server's NODE_OPTIONS and what its children inherit", () => {
    const parsed = parseNodeFlagsReport(report);
    expect(nodeFlagsFromReport(parsed, bench)).toEqual({
      env: {
        NODE_OPTIONS: `${bench} --max-semi-space-size=64`,
        T3CODE_INHERITED_NODE_OPTIONS: bench,
      },
      expectedSemiSpaceMb: 64,
    });
    const flags: NodeFlags = {
      supported: true,
      report: parsed,
      unsupportedReason: null,
      ...nodeFlagsFromReport(parsed, bench),
    };
    expect(addedNodeOptions(flags)).toBe("--max-semi-space-size=64");
    // Kept (disabled or set explicitly): nothing is added, but an explicit size is still what the server must run with.
    const kept = parseNodeFlagsReport(
      JSON.stringify({
        applied: false,
        maxSemiSpaceMb: 48,
        nodeOptionsAtStart: "--max-semi-space-size=48",
        nodeOptionsForChildren: "--max-semi-space-size=48",
        keptReason: "disabled or set explicitly",
      }),
    );
    expect(nodeFlagsFromReport(kept, "--max-semi-space-size=48")).toEqual({
      env: {},
      expectedSemiSpaceMb: 64,
    });
    expect(nodeFlagsFromReport(null, bench)).toEqual({ env: {}, expectedSemiSpaceMb: null });
    expect(addedNodeOptions(undefined)).toBeNull();
  });

  it("runs a build without the command as before, and stops on any other probe failure", () => {
    const probe = (overrides: Partial<Parameters<typeof nodeFlagsFromProbe>[0]>) => ({
      timedOut: false,
      exitCode: 0,
      signal: null,
      stdout: "",
      stderr: "",
      ...overrides,
    });
    const unknown = probe({
      exitCode: 1,
      stderr: '\nERROR\n  Unknown command "__node-flags". Use "t3 --help" for commands.\n',
    });
    expect(nodeFlagsFromProbe(unknown, bench)).toMatchObject({
      supported: false,
      env: {},
      expectedSemiSpaceMb: null,
    });
    // An explicit semi-space flag on such a build is still what the server must run with.
    expect(
      nodeFlagsFromProbe(unknown, `${bench} --max-semi-space-size=64`).expectedSemiSpaceMb,
    ).toBe(64);
    expect(nodeFlagsFromProbe(probe({ stdout: report }), bench)).toMatchObject({
      supported: true,
      expectedSemiSpaceMb: 64,
      env: { NODE_OPTIONS: `${bench} --max-semi-space-size=64` },
    });
    expect(() => nodeFlagsFromProbe(probe({ timedOut: true, exitCode: null }), bench)).toThrow(
      /no answer/,
    );
    expect(() => nodeFlagsFromProbe(probe({ exitCode: 2, stderr: "boom" }), bench)).toThrow(
      /exit 2: boom/,
    );
    expect(() => nodeFlagsFromProbe(probe({ exitCode: 1, stderr: "other error" }), bench)).toThrow(
      /exit 1/,
    );
    expect(() => nodeFlagsFromProbe(probe({ stdout: "not json" }), bench)).toThrow(/no report/);
    expect(() => nodeFlagsFromProbe(probe({ stdout: '{"applied":true}' }), bench)).toThrow(
      /no report/,
    );
    expect(() => nodeFlagsFromProbe(probe({ exitCode: null, signal: "SIGKILL" }), bench)).toThrow(
      /SIGKILL/,
    );
  });

  it("parses V8 size flags as Node does: quotes, underscores, the last value, NODE_OPTIONS before execArgv", () => {
    expect(nodeOptionTokens('--a "--b=1 2" --c')).toEqual(["--a", "--b=1 2", "--c"]);
    expect(
      lastSizeFlag(
        "--max-old-space-size",
        "--max_old_space_size=4096 --max-old-space-size=8192",
        [],
      ),
    ).toBe(8192);
    expect(lastSizeFlag("--max-semi-space-size", '--max-semi-space-size="16"', [])).toBe(16);
    expect(
      lastSizeFlag("--max-semi-space-size", "--max-semi-space-size=16", [
        "--max-semi-space-size=64",
      ]),
    ).toBe(64);
    expect(lastSizeFlag("--max-semi-space-size", "--max-semi-space-size 32", [])).toBe(32);
    expect(lastSizeFlag("--max-semi-space-size", bench, [])).toBeNull();
    expect(effectiveSemiSpaceMb(48)).toBe(64);
    expect(effectiveSemiSpaceMb(64)).toBe(64);
    expect(effectiveSemiSpaceMb(16)).toBe(16);
  });

  it("measures the semi-space size from the heap limit: (limit - old space) / 3", () => {
    // The heap limit is old space plus three semi-spaces: --max-old-space-size=8192 alone -> 8288 MiB (default semi 32), with semi 64 -> 8384 MiB.
    const config = (limitMib: number, nodeOptions: string) => ({
      heap_size_limit_bytes: limitMib * 1024 * 1024,
      node_options: nodeOptions,
      exec_argv: [],
    });
    expect(
      measuredSemiSpaceMb(config(8384, `${bench} --max-semi-space-size=64`)).semi_space_mb,
    ).toBe(64);
    expect(measuredSemiSpaceMb(config(8288, bench)).semi_space_mb).toBe(32);
    expect(
      measuredSemiSpaceMb({ heap_size_limit_mib: 4288, node_options: "--max-old-space-size=4096" })
        .semi_space_mb,
    ).toBe(64);
    expect(measuredSemiSpaceMb(config(4288, "")).semi_space_mb).toBeNull();
    expect(measuredSemiSpaceMb(undefined).semi_space_mb).toBeNull();
  });

  it("fails a run whose server does not run with the flags its build asked for", () => {
    const parsed = parseNodeFlagsReport(report);
    const flags: NodeFlags = {
      supported: true,
      report: parsed,
      unsupportedReason: null,
      ...nodeFlagsFromReport(parsed, bench),
    };
    const wanted = `${bench} --max-semi-space-size=64`;
    const heap = (limitMib: number, nodeOptions: string) => ({
      heap_size_limit_bytes: limitMib * 1024 * 1024,
      node_options: nodeOptions,
      exec_argv: [],
    });
    expect(checkNodeFlags(flags, heap(8384, wanted)).problem).toBeNull();
    // The relaunch was skipped, or the flag was not honoured: Node's default young generation.
    expect(checkNodeFlags(flags, heap(8288, bench)).problem).toBe("node_flags_mismatch");
    expect(checkNodeFlags(flags, heap(8288, wanted)).problem).toBe("node_flags_mismatch");
    expect(checkNodeFlags(flags, undefined).problem).toBe("node_flags_unverified");
    expect(checkNodeFlags(flags, heap(8384, "--max-semi-space-size=64")).problem).toBe(
      "node_flags_mismatch",
    );
    // The right NODE_OPTIONS but no --max-old-space-size anywhere: the size cannot be measured.
    const noOld = { ...flags, env: { NODE_OPTIONS: "--max-semi-space-size=64" } };
    expect(checkNodeFlags(noOld, heap(4288, "--max-semi-space-size=64")).problem).toBe(
      "node_flags_unverified",
    );
    // A build that asks for nothing (no __node-flags) is not checked.
    const none: NodeFlags = {
      supported: false,
      report: null,
      unsupportedReason: "exit 1",
      env: {},
      expectedSemiSpaceMb: null,
    };
    expect(checkNodeFlags(none, undefined).problem).toBeNull();
    expect(checkNodeFlags(none, heap(8288, bench)).measured?.semi_space_mb).toBe(32);
    // An explicit size on such a build is still checked.
    expect(checkNodeFlags({ ...none, expectedSemiSpaceMb: 64 }, heap(8288, bench)).problem).toBe(
      "node_flags_mismatch",
    );
  });

  it("keeps the server's own server.v8 entry when it logs one", () => {
    const log = [
      "[03:05:44.900] INFO (#1): Migrations ran successfully",
      "[03:05:44.959] INFO (#7670): server.v8",
      "  {",
      "    nodeOptions: '--max-semi-space-size=64',",
      "    maxSemiSpaceMiB: 64,",
      "    youngGenerationDefault: true",
      "  }",
      "[03:05:45.083] INFO (#7666): Listening on http://127.0.0.1:3897",
    ].join("\n");
    expect(serverV8LogEntry(log)).toBe(log.split("\n").slice(1, 7).join("\n"));
    expect(serverV8LogEntry("[03:05:45.083] WARN (#1): something")).toBeNull();
  });
});

describe("database readiness (load starts after the database is set up, not when the port answers)", () => {
  const line = (name: string, endMs: number, exit = "Success") =>
    JSON.stringify({
      type: "effect-span",
      name,
      spanId: "a",
      startTimeUnixNano: String((endMs - 100) * 1e6),
      endTimeUnixNano: String(endMs * 1e6),
      exit: { _tag: exit },
    });

  it("reads Sqlite.setup and runMigrations spans and ignores everything else", () => {
    const text = [
      line("runMigrations", 1_000),
      JSON.stringify({ name: "GET", endTimeUnixNano: "5" }),
      "not json",
      line("Sqlite.setup", 1_200),
      "",
    ].join("\n");
    expect(dbReadySpans(text)).toEqual([
      { name: "runMigrations", endMs: 1_000, ok: true },
      { name: "Sqlite.setup", endMs: 1_200, ok: true },
    ]);
  });

  it("is ready at Sqlite.setup, fails on a failed setup, and waits for it after runMigrations", () => {
    const migrations = dbReadySpans(line("runMigrations", 1_000));
    expect(dbReadiness([], 50_000).state).toBe("waiting");
    // Migrations done but the statistics refresh still running: not ready yet.
    expect(dbReadiness(migrations, 1_500).state).toBe("waiting");
    const setup = dbReadySpans(line("Sqlite.setup", 1_200));
    expect(dbReadiness([...migrations, ...setup], 1_300)).toEqual({
      state: "ready",
      span: setup[0],
    });
    expect(dbReadiness(dbReadySpans(line("Sqlite.setup", 1_200, "Failure")), 1_300).state).toBe(
      "failed",
    );
    expect(dbReadiness(dbReadySpans(line("runMigrations", 1_000, "Failure")), 1_300).state).toBe(
      "failed",
    );
  });

  it("ignores spans that ended before this server started, and spans without times", () => {
    const text = [line("Sqlite.setup", 1_000), line("Sqlite.setup", 0)].join("\n");
    expect(dbReadySpans(text, 2_000)).toEqual([]);
    expect(dbReadySpans(line("Sqlite.setup", 2_500), 2_000)).toEqual([
      { name: "Sqlite.setup", endMs: 2_500, ok: true },
    ]);
  });

  it("reads only new whole lines, follows rotation by file, and skips a stale trace", async () => {
    await withTempDir(async (dir) => {
      const trace = NodePath.join(dir, "server.trace.ndjson");
      const spawned = 1_000_000;
      const offsets = new Map<string, number>();
      // A trace kept from an earlier start: its Sqlite.setup must not count.
      NodeFS.writeFileSync(
        trace,
        `${line("Sqlite.setup", spawned - 60_000)}\n${"x".repeat(5_000)}\n`,
      );
      expect(scanTraceForReadiness(trace, offsets, spawned)).toEqual([]);
      // A partial last line waits for its end.
      NodeFS.appendFileSync(trace, line("runMigrations", spawned + 5).slice(0, 40));
      expect(scanTraceForReadiness(trace, offsets, spawned)).toEqual([]);
      NodeFS.appendFileSync(trace, `${line("runMigrations", spawned + 5).slice(40)}\n`);
      expect(scanTraceForReadiness(trace, offsets, spawned)).toEqual([
        { name: "runMigrations", endMs: spawned + 5, ok: true },
      ]);
      // Rotation: the active file becomes .1 and a new, shorter file takes its name with the span on its first line.
      NodeFS.renameSync(trace, `${trace}.1`);
      NodeFS.writeFileSync(trace, `${line("Sqlite.setup", spawned + 9)}\n`);
      expect(scanTraceForReadiness(trace, offsets, spawned)).toEqual([
        { name: "Sqlite.setup", endMs: spawned + 9, ok: true },
      ]);
      // Nothing new: nothing read twice.
      expect(scanTraceForReadiness(trace, offsets, spawned)).toEqual([]);
      // A name that cannot be opened any more (gone between listing and opening) is skipped, not thrown.
      NodeFS.symlinkSync(NodePath.join(dir, "gone"), `${trace}.2`);
      expect(scanTraceForReadiness(trace, offsets, spawned)).toEqual([]);
    });
  });

  it("accepts runMigrations alone on a build without Sqlite.setup, 30 s after it ended", () => {
    const migrations = dbReadySpans(line("runMigrations", 1_000));
    expect(dbReadiness(migrations, 30_999).state).toBe("waiting");
    expect(dbReadiness(migrations, 31_000)).toEqual({ state: "ready", span: migrations[0] });
  });
});

describe("seaTarget: an upstream release archive (no build-manifest.txt)", () => {
  const commit = "295a0a7332aa1b0cc2f737dae71bd5e607a6ebb9";
  const archiveSha = "7a228b35679c43c4940efd57ce7ac17ccb43e5f982b3911a1f0340621a655571";
  const sha256 = (data: string | Buffer) =>
    NodeCrypto.createHash("sha256").update(data).digest("hex");
  const binary = "#!/bin/sh\necho '0.1.10-nightly.20261115.4749'\n";
  const monitor = "#!/bin/sh\nexit 0\n";
  const release = (overrides: Record<string, unknown> = {}) => ({
    schema: "t3-server-bench/release/1",
    tag: "v0.1.10-nightly.20261115.4749",
    version: "0.1.10-nightly.20261115.4749",
    commit,
    platform: "linux-x64",
    archive_name: "t3-0.1.10-nightly.20261115.4749-linux-x64.tar.gz",
    archive_bytes: 182979396,
    archive_sha256: archiveSha,
    t3_sha256: sha256(binary),
    ...overrides,
  });
  /** An unpacked `t3-<v>-linux-x64/` as the release ships it, plus the release.json release.ts writes. */
  const releaseDir = (
    options: {
      readonly release?: Record<string, unknown> | null;
      readonly nodeModules?: boolean;
      readonly monitor?: boolean;
    } = {},
  ) => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-release-"));
    onTestFinished(() => NodeFS.rmSync(dir, { recursive: true, force: true }));
    NodeFS.writeFileSync(NodePath.join(dir, "t3"), binary, { mode: 0o755 });
    NodeFS.mkdirSync(NodePath.join(dir, "client"));
    NodeFS.writeFileSync(NodePath.join(dir, "client", "index.html"), "<!doctype html>\n");
    NodeFS.mkdirSync(NodePath.join(dir, "lib"));
    NodeFS.writeFileSync(NodePath.join(dir, "lib", "libatomic.so.1"), "\x7fELF");
    if (options.nodeModules !== false) {
      NodeFS.mkdirSync(NodePath.join(dir, "node_modules", "node-pty"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(dir, "node_modules", "node-pty", "package.json"), "{}");
    }
    if (options.monitor !== false) {
      NodeFS.mkdirSync(NodePath.join(dir, "resource-monitor", "linux-x64"), { recursive: true });
      NodeFS.writeFileSync(
        NodePath.join(dir, "resource-monitor", "linux-x64", "t3-resource-monitor"),
        monitor,
        { mode: 0o755 },
      );
    }
    if (options.release !== null) {
      NodeFS.writeFileSync(
        NodePath.join(dir, "release.json"),
        `${JSON.stringify(options.release ?? release())}\n`,
      );
    }
    return dir;
  };

  it("accepts the release layout and records the archive sha256, the release and t3's own sha256", () => {
    const dir = releaseDir();
    expect(NodeFS.existsSync(NodePath.join(dir, "build-manifest.txt"))).toBe(false);
    const target = seaTarget(dir);
    expect(target.entryKind).toBe("sea");
    expect(target.entry).toBe(NodePath.join(dir, "t3"));
    expect(target.sha).toBe(commit);
    expect(target.dirty).toBe(false);
    expect(target.tree).toBe(commit);
    expect(target.archive?.layout).toBe("release");
    expect(target.archive?.archive_sha256).toBe(archiveSha);
    expect(target.archive?.binary_sha256).toBe(sha256(binary));
    expect(target.archive?.release).toMatchObject({
      tag: "v0.1.10-nightly.20261115.4749",
      version: "0.1.10-nightly.20261115.4749",
      commit,
      platform: "linux-x64",
      archive_name: "t3-0.1.10-nightly.20261115.4749-linux-x64.tar.gz",
    });
    expect(target.archive?.resource_monitor).toEqual({
      path: NodePath.join(dir, "resource-monitor", "linux-x64", "t3-resource-monitor"),
      relative: "resource-monitor/linux-x64/t3-resource-monitor",
      sha256: sha256(monitor),
    });
    expect(target.archive?.exec_argv).toBeNull();
  });

  it("refuses a t3 changed after unpacking (one byte), naming both sha256", () => {
    const dir = releaseDir();
    const bytes = NodeFS.readFileSync(NodePath.join(dir, "t3"));
    bytes[bytes.length - 2] = bytes[bytes.length - 2]! ^ 0x01;
    NodeFS.writeFileSync(NodePath.join(dir, "t3"), bytes);
    expect(() => seaTarget(dir)).toThrow(
      new RegExp(
        `t3 sha256 ${sha256(bytes)} differs from release.json's t3_sha256 ${sha256(binary)}`,
      ),
    );
  });

  it("refuses an incomplete release.json, a layout without node_modules, and a directory with neither manifest", () => {
    expect(() => seaTarget(releaseDir({ release: release({ commit: "main" }) }))).toThrow(
      /release.json has no 40-hex commit/,
    );
    expect(() => seaTarget(releaseDir({ release: release({ archive_sha256: null }) }))).toThrow(
      /release.json has no 64-hex archive_sha256/,
    );
    expect(() => seaTarget(releaseDir({ release: release({ t3_sha256: "x" }) }))).toThrow(
      /release.json has no 64-hex t3_sha256/,
    );
    expect(() => seaTarget(releaseDir({ nodeModules: false }))).toThrow(/no node_modules/);
    expect(() => seaTarget(releaseDir({ release: null }))).toThrow(
      /no build-manifest.txt .* or release.json/,
    );
  });

  it("refuses a release without its resource monitor, and one whose monitor is not executable", () => {
    expect(() => seaTarget(releaseDir({ monitor: false }))).toThrow(
      /no resource-monitor\/linux-x64\/t3-resource-monitor in the release/,
    );
    const dir = releaseDir();
    NodeFS.chmodSync(
      NodePath.join(dir, "resource-monitor", "linux-x64", "t3-resource-monitor"),
      0o644,
    );
    expect(() => seaTarget(dir)).toThrow(
      /resource-monitor\/linux-x64\/t3-resource-monitor is not executable/,
    );
  });
});

describe("composeServerEnv (resource monitor, preload)", () => {
  const archive = {
    resource_monitor: {
      path: "/r/t3-x/resource-monitor/linux-x64/t3-resource-monitor",
      relative: "resource-monitor/linux-x64/t3-resource-monitor",
      sha256: "a".repeat(64),
    },
  };
  const preload = "/h/tools/server-bench/preload/bench-preload.cjs";

  it("points the server at the archive's own resource monitor unless the run sets one", () => {
    const own = composeServerEnv({ defaults: { A: "1" }, archive, preload: null });
    expect(own.env).toEqual({
      A: "1",
      T3CODE_RESOURCE_MONITOR_PATH: "/r/t3-x/resource-monitor/linux-x64/t3-resource-monitor",
    });
    expect(own.resourceMonitor).toEqual({ ...archive.resource_monitor, source: "archive" });
    const set = composeServerEnv({
      defaults: {},
      serverEnv: { T3CODE_RESOURCE_MONITOR_PATH: "/opt/monitor" },
      archive,
      preload: null,
    });
    expect(set.env.T3CODE_RESOURCE_MONITOR_PATH).toBe("/opt/monitor");
    expect(set.resourceMonitor).toEqual({
      path: "/opt/monitor",
      relative: null,
      sha256: null,
      source: "env",
    });
    const perRef = composeServerEnv({
      defaults: {},
      serverEnv: { T3CODE_RESOURCE_MONITOR_PATH: "/opt/monitor" },
      refEnv: { T3CODE_RESOURCE_MONITOR_PATH: "/opt/other" },
      archive,
      preload: null,
    });
    expect(perRef.env.T3CODE_RESOURCE_MONITOR_PATH).toBe("/opt/other");
    expect(composeServerEnv({ defaults: {}, archive: null, preload: null }).resourceMonitor).toBe(
      null,
    );
  });

  it("appends the preload to the server's NODE_OPTIONS as a --require", () => {
    expect(
      composeServerEnv({
        defaults: {},
        serverEnv: { NODE_OPTIONS: "--max-old-space-size=8192" },
        archive: null,
        preload,
      }).env.NODE_OPTIONS,
    ).toBe(`--max-old-space-size=8192 --require=${preload}`);
    expect(composeServerEnv({ defaults: {}, archive: null, preload }).env.NODE_OPTIONS).toBe(
      `--require=${preload}`,
    );
    const spaced = composeServerEnv({
      defaults: {},
      archive: null,
      preload: "/a b/bench-preload.cjs",
    });
    expect(spaced.env.NODE_OPTIONS).toBe('--require="/a b/bench-preload.cjs"');
    expect(nodeOptionTokens(spaced.env.NODE_OPTIONS)).toEqual(["--require=/a b/bench-preload.cjs"]);
    expect(
      composeServerEnv({
        defaults: {},
        serverEnv: { NODE_OPTIONS: "--max-old-space-size=1" },
        archive: null,
        preload: null,
      }).env.NODE_OPTIONS,
    ).toBe("--max-old-space-size=1");
  });

  it("refuses a NODE_OPTIONS that already loads a bench preload (it would run twice)", () => {
    for (const nodeOptions of [
      "--require=/opt/bench/bench-preload.cjs",
      "-r /opt/bench/bench-preload.cjs",
      "--max-old-space-size=1 --require /opt/bench/bench-preload.cjs",
    ]) {
      expect(() =>
        composeServerEnv({
          defaults: {},
          serverEnv: { NODE_OPTIONS: nodeOptions },
          archive: null,
          preload,
        }),
      ).toThrow(/already requires a bench preload/);
    }
    expect(() =>
      composeServerEnv({ defaults: {}, archive: null, preload: '/a"b/bench-preload.cjs' }),
    ).toThrow(/cannot be quoted/);
  });
});

describe("preload outcome and event-loop metrics", () => {
  it("reads what the preload wrote: loaded, entry kind, 1 Hz samples and the window histogram", async () => {
    await withTempDir(async (runDir) => {
      NodeFS.writeFileSync(
        NodePath.join(runDir, "heap-config.json"),
        `${JSON.stringify({ schema: "t3-server-bench/heap-config/1", entry_kind: "sea", node_options: "--require=/p/bench-preload.cjs" })}\n`,
      );
      NodeFS.writeFileSync(NodePath.join(runDir, "loop-1hz.ndjson"), '{"t":1}\n{"t":2}\n{"t":3}\n');
      const window = {
        schema: "t3-server-bench/loop-window/1",
        start: 1_000_000,
        end: 1_120_000,
        resolution_ms: 10,
        delay: {
          count: 100,
          p50_ms: 10.1,
          p90_ms: 11,
          p99_ms: 25.5,
          p999_ms: 40,
          max_ms: 61.2,
          mean_ms: 10.4,
        },
        elu: 0.42,
        elu_active_ms: 50400,
        gc: { minor: { n: 10, ms: 120, max_ms: 20 }, major: { n: 1, ms: 120, max_ms: 120 } },
      };
      NodeFS.writeFileSync(NodePath.join(runDir, "loop-window.json"), JSON.stringify(window));
      expect(preloadOutcome(runDir, true)).toEqual({
        requested: true,
        loaded: true,
        entry_kind: "sea",
        node_options: "--require=/p/bench-preload.cjs",
        samples: 3,
        window: true,
      });
      expect(loopMetrics(runDir)).toEqual({
        elu: 0.42,
        eld_p50_ms: 10.1,
        eld_p90_ms: 11,
        eld_p99_ms: 25.5,
        eld_p999_ms: 40,
        eld_max_ms: 61.2,
        eld_mean_ms: 10.4,
        gc_ms_per_s: 2,
        gc_max_pause_ms: 120,
      });
    });
  });

  it("says not loaded and gives no loop metrics when the preload wrote nothing", async () => {
    await withTempDir(async (runDir) => {
      expect(preloadOutcome(runDir, true)).toEqual({
        requested: true,
        loaded: false,
        entry_kind: null,
        node_options: null,
        samples: 0,
        window: false,
      });
      expect(preloadOutcome(runDir, false).requested).toBe(false);
      expect(loopMetrics(runDir)).toBeNull();
    });
  });
});

describe("window control files (the preload's window histogram)", () => {
  it("maps the driver's phase lines to window-start and window-end only", () => {
    expect(windowControlFor("[driver 2026-11-16T00:00:00.000Z] phase: window")).toBe(
      "window-start",
    );
    expect(windowControlFor("[driver 2026-11-16T00:05:00.000Z] phase: drain")).toBe("window-end");
    expect(windowControlFor("[driver 2026-11-16T00:00:00.000Z] phase: warmup")).toBeNull();
    expect(windowControlFor("[driver 2026-11-16T00:00:00.000Z] phase: windowed")).toBeNull();
    expect(windowControlFor("session 3 says phase: window")).toBeNull();
  });
});

describe("seedCompactedAt", () => {
  it("writes a fresh compacted-at marker beside the database and records what the fixture had", async () => {
    await withTempDir(async (base) => {
      NodeFS.mkdirSync(NodePath.join(base, "userdata"));
      expect(seedCompactedAt(base, 1_763_251_200_000)).toEqual({
        in_fixture: false,
        fixture_value: null,
        seeded_ms: 1_763_251_200_000,
      });
      expect(NodeFS.readFileSync(NodePath.join(base, "userdata", "compacted-at"), "utf8")).toBe(
        "1763251200000",
      );
      expect(seedCompactedAt(base, 1_763_251_300_000)).toEqual({
        in_fixture: true,
        fixture_value: "1763251200000",
        seeded_ms: 1_763_251_300_000,
      });
    });
  });
});

describe("parseNow and parseCount", () => {
  it("takes an ISO date-time with a zone and normalises it", () => {
    expect(parseNow("2026-11-17T00:00:00Z")).toBe("2026-11-17T00:00:00.000Z");
    expect(parseNow("2026-11-17T01:30:00+01:00")).toBe("2026-11-17T00:30:00.000Z");
    for (const bad of ["2026-11-17", "tomorrow", "2026-13-01T00:00:00Z", "2026-11-17T00:00:00"]) {
      expect(() => parseNow(bad)).toThrow(/--now/);
    }
  });

  it("accepts 0 where the flag allows it and whole numbers only", () => {
    expect(parseCount("0", "--pollers", 0)).toBe(0);
    expect(parseCount("25", "--sessions", 1)).toBe(25);
    expect(() => parseCount("0", "--sessions", 1)).toThrow(/--sessions/);
    expect(() => parseCount("1.5", "--pollers", 0)).toThrow(/--pollers/);
    expect(() => parseCount("-1", "--pollers", 0)).toThrow(/--pollers/);
  });
});

describe("harnessIdentity", () => {
  it("reads an installed harness's t3cp-harness.json, whose files_sha256 must match the files", async () => {
    expect(HARNESS_VERSION).toMatch(/^t3cp-harness\/\d+\.\d+\.\d+$/);
    await withTempDir(async (root) => {
      await expect(harnessIdentity(root)).rejects.toThrow(
        /no git checkout and no t3cp-harness.json/,
      );
      const sha = "1".repeat(40);
      const files = await harnessFilesSha256(root);
      const write = (fields: Record<string, unknown>) =>
        NodeFS.writeFileSync(
          NodePath.join(root, "t3cp-harness.json"),
          JSON.stringify({ version: HARNESS_VERSION, sha, files_sha256: files, ...fields }),
        );
      write({});
      expect(await harnessIdentity(root)).toEqual({
        version: HARNESS_VERSION,
        sha,
        source_sha: null,
        source: "file",
      });
      // A snapshot commit (the published copy) names the branch commit whose tree it holds.
      const source = "2".repeat(40);
      write({ source_sha: source });
      expect(await harnessIdentity(root)).toEqual({
        version: HARNESS_VERSION,
        sha,
        source_sha: source,
        source: "file",
      });
      write({ source_sha: "main" });
      await expect(harnessIdentity(root)).rejects.toThrow(/source_sha/);
      // Files changed after the install no longer are the harness the identity names.
      write({ files_sha256: "0".repeat(64) });
      await expect(harnessIdentity(root)).rejects.toThrow(
        /files_sha256 0{64} differs from the harness files' [0-9a-f]{64}/,
      );
      write({ files_sha256: undefined });
      await expect(harnessIdentity(root)).rejects.toThrow(/no 64-hex files_sha256/);
    });
  });
});

describe("harnessFilesSha256 (the bands' comparability key) covers the run-time closure", () => {
  const closure = [
    "package.json",
    "tools/server-bench/run.ts",
    "tools/server-bench/collect.ts",
    "tools/server-bench/report.ts",
    "tools/server-bench/release.ts",
    "tools/server-bench/driver.ts",
    "tools/server-bench/driver-stub.ts",
    "tools/server-bench/driver/clients.ts",
    "tools/server-bench/providers.ts",
    "tools/server-bench/preload/bench-preload.cjs",
    "tools/server-bench/preload/bench-preload.mjs",
    "tools/server-bench/fake-claude/claude",
    "tools/server-bench/fake-claude/claude.mjs",
    "tools/server-bench/fake-claude/transcripts/a.json",
    "tools/server-bench/fake-codex/codex.mjs",
    "tools/server-bench/fake-acp/acp-agent.mjs",
    "tools/server-bench/fake-common/replay.mjs",
    "tools/server-bench/install-harness.sh",
    "apps/server/scripts/dev-replay-agent.ts",
    "apps/server/src/orchestration-v2/testkit/fixtures/turn.ndjson",
  ];
  const outside = [
    "tools/server-bench/run.test.ts",
    "tools/server-bench/fake-claude/claude.test.mjs",
    "tools/server-bench/README.md",
    "tools/server-bench/settings/std.json",
    "tools/server-bench/state/fake-stats/x",
    "apps/server/src/other.ts",
  ];

  it("changes with every file the runs execute and with none of the tests, docs or install state", async () => {
    await withTempDir(async (root) => {
      for (const file of [...closure, ...outside]) {
        NodeFS.mkdirSync(NodePath.dirname(NodePath.join(root, file)), { recursive: true });
        NodeFS.writeFileSync(NodePath.join(root, file), `${file}\n`);
      }
      const base = await harnessFilesSha256(root);
      expect(base).toMatch(/^[0-9a-f]{64}$/);
      for (const file of outside) {
        NodeFS.appendFileSync(NodePath.join(root, file), "edited\n");
        expect(await harnessFilesSha256(root), file).toBe(base);
      }
      for (const file of closure) {
        const path = NodePath.join(root, file);
        const before = NodeFS.readFileSync(path);
        NodeFS.appendFileSync(path, "edited\n");
        expect(await harnessFilesSha256(root), file).not.toBe(base);
        NodeFS.writeFileSync(path, before);
      }
      // A new file in the closure counts too.
      NodeFS.writeFileSync(NodePath.join(root, "tools/server-bench/driver/new.ts"), "x\n");
      expect(await harnessFilesSha256(root)).not.toBe(base);
    });
  });
});

describe("awaitLoopWindow (a short drain must not lose the window histogram)", () => {
  /** A sleep that only counts: the wait is measured in the polls it asks for, never the wall clock. */
  const countingSleep = (onSleep: (calls: number) => void = () => {}) => {
    const calls: Array<number> = [];
    const sleep = async (ms: number) => {
      calls.push(ms);
      onSleep(calls.length);
    };
    return { calls, sleep };
  };

  it("waits for the preload's loop-window.json once window-end is touched", async () => {
    await withTempDir(async (runDir) => {
      NodeFS.mkdirSync(NodePath.join(runDir, "ctl"));
      NodeFS.writeFileSync(NodePath.join(runDir, "ctl", "window-end"), "1");
      const { calls, sleep } = countingSleep((n) => {
        if (n === 3) NodeFS.writeFileSync(NodePath.join(runDir, "loop-window.json"), "{}");
      });
      expect(await awaitLoopWindow(runDir, 3000, sleep)).toBe(true);
      expect(calls).toEqual([100, 100, 100]);
    });
  });

  it("gives up after the timeout, and does not wait when the window never closed", async () => {
    await withTempDir(async (runDir) => {
      NodeFS.mkdirSync(NodePath.join(runDir, "ctl"));
      const open = countingSleep();
      expect(await awaitLoopWindow(runDir, 3000, open.sleep)).toBe(false);
      expect(open.calls).toEqual([]);
      NodeFS.writeFileSync(NodePath.join(runDir, "ctl", "window-end"), "1");
      const closed = countingSleep();
      expect(await awaitLoopWindow(runDir, 400, closed.sleep)).toBe(false);
      expect(closed.calls).toEqual([100, 100, 100, 100]);
      NodeFS.writeFileSync(NodePath.join(runDir, "loop-window.json"), "{}");
      const written = countingSleep();
      expect(await awaitLoopWindow(runDir, 400, written.sleep)).toBe(true);
      expect(written.calls).toEqual([]);
    });
  });
});

describe("an upstream release must come from its verified archive", () => {
  const sha256 = (data: string | Buffer) =>
    NodeCrypto.createHash("sha256").update(data).digest("hex");
  const version = "1.2.3-nightly.20261115.9";
  const top = `t3-${version}-linux-x64`;
  const commit = "295a0a7332aa1b0cc2f737dae71bd5e607a6ebb9";
  const addon = Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1, 1, 0, 9, 9]);
  /** A release-shaped archive built with tar, verified and unpacked by release.ts: [unpacked dir, archive]. */
  const released = async (root: string, t3: string) => {
    const src = NodePath.join(root, "src");
    NodeFS.mkdirSync(NodePath.join(src, top, "node_modules", "x"), { recursive: true });
    NodeFS.mkdirSync(NodePath.join(src, top, "resource-monitor", "linux-x64"), { recursive: true });
    NodeFS.writeFileSync(NodePath.join(src, top, "t3"), t3, { mode: 0o755 });
    NodeFS.writeFileSync(NodePath.join(src, top, "node_modules", "x", "package.json"), "{}");
    NodeFS.writeFileSync(
      NodePath.join(src, top, "node_modules", "x", "index.js"),
      "module.exports = 1;\n",
    );
    NodeFS.writeFileSync(NodePath.join(src, top, "node_modules", "x", "addon.node"), addon, {
      mode: 0o755,
    });
    NodeFS.writeFileSync(
      NodePath.join(src, top, "resource-monitor", "linux-x64", "t3-resource-monitor"),
      "#!/bin/sh\n",
      { mode: 0o755 },
    );
    for (const name of ["package.json", "index.js"]) {
      NodeFS.chmodSync(NodePath.join(src, top, "node_modules", "x", name), 0o644);
    }
    const archive = NodePath.join(root, `${top}.tar.gz`);
    const tar = NodeChildProcess.spawnSync("tar", ["-czf", archive, "-C", src, top], TAR_OPTIONS);
    if (tar.status !== 0) throw new Error(`tar failed: ${tar.stderr.toString()}`);
    const { dir } = await unpackRelease({
      archive,
      sha256: sha256(NodeFS.readFileSync(archive)),
      tag: `v${version}`,
      commit,
      platform: "linux-x64",
      dest: NodePath.join(root, "releases"),
    });
    return { dir, archive };
  };

  it("accepts the unpacked release with the archive it came from", async () => {
    await withTempDir(async (root) => {
      const { dir, archive } = await released(root, "official t3 bytes\n");
      await expect(verifyArchive(seaTarget(dir), archive)).resolves.toBeUndefined();
    });
  });

  it("refuses a forged release.json with a wrapper t3, though the directory alone looks consistent", async () => {
    await withTempDir(async (root) => {
      const official = "official t3 bytes\n";
      const { dir, archive } = await released(root, official);
      const wrapper = `#!/bin/sh\nexec ${dir}/t3.real "$@"\n`;
      NodeFS.renameSync(NodePath.join(dir, "t3"), NodePath.join(dir, "t3.real"));
      NodeFS.writeFileSync(NodePath.join(dir, "t3"), wrapper, { mode: 0o755 });
      const forged = JSON.parse(NodeFS.readFileSync(NodePath.join(dir, "release.json"), "utf8"));
      NodeFS.writeFileSync(
        NodePath.join(dir, "release.json"),
        JSON.stringify({ ...forged, t3_sha256: sha256(wrapper) }),
      );
      const target = seaTarget(dir);
      expect(target.archive?.binary_sha256).toBe(sha256(wrapper));
      await expect(verifyArchive(target, archive)).rejects.toThrow(
        new RegExp(
          `t3 sha256 ${sha256(wrapper)} differs from the archive's t3 sha256 ${sha256(official)}`,
        ),
      );
    });
  });

  it("refuses an archive that is not the one release.json names", async () => {
    await withTempDir(async (root) => {
      const { dir } = await released(NodePath.join(root, "a"), "official t3 bytes\n");
      const other = await released(NodePath.join(root, "b"), "other t3 bytes\n");
      await expect(verifyArchive(seaTarget(dir), other.archive)).rejects.toThrow(
        /archive sha256 [0-9a-f]{64} differs from release.json's archive_sha256 [0-9a-f]{64}/,
      );
    });
  });

  it("requires --archive for a release, before anything runs", async () => {
    await withTempDir(async (root) => {
      const { dir, archive } = await released(root, "official t3 bytes\n");
      await expect(checkSeaRef(seaTarget(dir), undefined)).rejects.toThrow(
        /an upstream release needs --archive/,
      );
      // With its archive the next check is the executable itself (a test t3 is not one).
      await expect(checkSeaRef(seaTarget(dir), archive)).rejects.toThrow(/not an ELF executable/);
    });
  });

  it("refuses a native module with one byte appended, though t3 is the archive's", async () => {
    await withTempDir(async (root) => {
      const { dir, archive } = await released(root, "official t3 bytes\n");
      NodeFS.appendFileSync(NodePath.join(dir, "node_modules", "x", "addon.node"), "X");
      await expect(verifyArchive(seaTarget(dir), archive)).rejects.toThrow(
        /the unpacked tree differs from the archive in 1 place.*node_modules\/x\/addon\.node: size 11 differs from the archive's 10/,
      );
    });
  });

  it("refuses a script in node_modules with a comment appended", async () => {
    await withTempDir(async (root) => {
      const { dir, archive } = await released(root, "official t3 bytes\n");
      NodeFS.appendFileSync(NodePath.join(dir, "node_modules", "x", "index.js"), "// changed\n");
      await expect(verifyArchive(seaTarget(dir), archive)).rejects.toThrow(
        /node_modules\/x\/index\.js: size \d+ differs from the archive's \d+/,
      );
    });
  });

  it("refuses a resource monitor replaced by another executable, even one of the same size", async () => {
    await withTempDir(async (root) => {
      const { dir, archive } = await released(root, "official t3 bytes\n");
      const monitor = NodePath.join(dir, "resource-monitor", "linux-x64", "t3-resource-monitor");
      NodeFS.writeFileSync(monitor, "#!/bin/xx\n");
      await expect(verifyArchive(seaTarget(dir), archive)).rejects.toThrow(
        /resource-monitor\/linux-x64\/t3-resource-monitor: contents differ from the archive's/,
      );
    });
  });

  it("refuses a file the archive does not have, an exec-argv.json beside a release's t3 included", async () => {
    await withTempDir(async (root) => {
      const { dir, archive } = await released(root, "official t3 bytes\n");
      NodeFS.writeFileSync(NodePath.join(dir, "node_modules", "x", "preload.js"), "");
      NodeFS.writeFileSync(NodePath.join(dir, "exec-argv.json"), '{"execArgv": []}');
      await expect(verifyArchive(seaTarget(dir), archive)).rejects.toThrow(
        /differs from the archive in 2 places.*exec-argv\.json: not in the archive.*node_modules\/x\/preload\.js: not in the archive/,
      );
    });
  });

  it("refuses a file whose mode changed", async () => {
    await withTempDir(async (root) => {
      const { dir, archive } = await released(root, "official t3 bytes\n");
      NodeFS.chmodSync(NodePath.join(dir, "node_modules", "x", "index.js"), 0o755);
      await expect(verifyArchive(seaTarget(dir), archive)).rejects.toThrow(
        /node_modules\/x\/index\.js: mode 0755 differs from the archive's 0644/,
      );
    });
  });
});

describe("checkSeaExecutable (a Node single executable for the platform)", () => {
  const fuse = "NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2";
  const file = (dir: string, name: string, bytes: Buffer) => {
    const path = NodePath.join(dir, name);
    NodeFS.writeFileSync(path, bytes, { mode: 0o755 });
    return path;
  };
  const elf = Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1, 1, 0]);
  const macho = Buffer.from([0xcf, 0xfa, 0xed, 0xfe, 7, 0, 0, 1]);

  it("accepts an ELF (Linux) or Mach-O (macOS) binary whose SEA fuse is set", async () => {
    await withTempDir(async (dir) => {
      const linux = file(dir, "linux", Buffer.concat([elf, Buffer.from(`..${fuse}:1..`)]));
      const darwin = file(dir, "darwin", Buffer.concat([macho, Buffer.from(`..${fuse}:1..`)]));
      expect(() => checkSeaExecutable(linux, "linux-x64")).not.toThrow();
      expect(() => checkSeaExecutable(darwin, "darwin-arm64")).not.toThrow();
    });
  });

  it("refuses a script, the wrong binary format, and a Node binary that is not a single executable", async () => {
    await withTempDir(async (dir) => {
      const script = file(dir, "script", Buffer.from('#!/bin/sh\nexec node "$@"\n'));
      const plainNode = file(dir, "node", Buffer.concat([elf, Buffer.from(`..${fuse}:0..`)]));
      const noFuse = file(dir, "nofuse", Buffer.concat([elf, Buffer.from("....")]));
      const linux = file(dir, "linux", Buffer.concat([elf, Buffer.from(`..${fuse}:1..`)]));
      expect(() => checkSeaExecutable(script, "linux-x64")).toThrow(/not an ELF executable/);
      expect(() => checkSeaExecutable(linux, "darwin-arm64")).toThrow(/not a Mach-O executable/);
      expect(() => checkSeaExecutable(plainNode, "linux-x64")).toThrow(
        /not a Node single executable/,
      );
      expect(() => checkSeaExecutable(noFuse, "linux-x64")).toThrow(/not a Node single executable/);
    });
  });
});

describe("parseArchiveSpecs (--archive [<label>=]<path>)", () => {
  it("maps each archive to its ref", () => {
    expect(parseArchiveSpecs(["/r/t3-1-linux-x64.tar.gz"], ["n=sea:/r/x"])).toEqual(
      new Map([[0, "/r/t3-1-linux-x64.tar.gz"]]),
    );
    expect(parseArchiveSpecs(["b=/r/b.tar.gz"], ["a=sea:/r/a", "b=sea:/r/b"])).toEqual(
      new Map([[1, "/r/b.tar.gz"]]),
    );
    expect(parseArchiveSpecs([], ["a=sea:/r/a"])).toEqual(new Map());
  });

  it("refuses an unlabelled archive with several refs, an unknown label and a second archive", () => {
    expect(() => parseArchiveSpecs(["/r/a.tar.gz"], ["a=sea:/r/a", "b=sea:/r/b"])).toThrow(
      /--archive needs <label>=<path> when there are several --ref/,
    );
    expect(() => parseArchiveSpecs(["c=/r/c.tar.gz"], ["a=sea:/r/a"])).toThrow(
      /no --ref labelled c/,
    );
    expect(() => parseArchiveSpecs(["a=/r/a.tar.gz", "a=/r/b.tar.gz"], ["a=sea:/r/a"])).toThrow(
      /two --archive values for a/,
    );
  });
});

describe("checkResourceMonitor (an override must exist)", () => {
  it("refuses a T3CODE_RESOURCE_MONITOR_PATH that is missing or not executable", async () => {
    await withTempDir(async (dir) => {
      const env = (path: string) =>
        ({ path, relative: null, sha256: null, source: "env" }) as const;
      expect(() => checkResourceMonitor(env(NodePath.join(dir, "missing")))).toThrow(
        /T3CODE_RESOURCE_MONITOR_PATH .*missing is not an executable file/,
      );
      const plain = NodePath.join(dir, "plain");
      NodeFS.writeFileSync(plain, "x", { mode: 0o644 });
      expect(() => checkResourceMonitor(env(plain))).toThrow(/is not an executable file/);
      const exe = NodePath.join(dir, "exe");
      NodeFS.writeFileSync(exe, "x", { mode: 0o755 });
      expect(() => checkResourceMonitor(env(exe))).not.toThrow();
      expect(() => checkResourceMonitor(null)).not.toThrow();
    });
  });
});

describe("resolvePreload", () => {
  it("names a missing preload instead of failing on its path", async () => {
    await withTempDir(async (dir) => {
      expect(resolvePreload("none", dir)).toBeNull();
      expect(() => resolvePreload(undefined, dir)).toThrow(
        /--preload .*bench-preload.cjs does not exist/,
      );
      expect(() => resolvePreload(NodePath.join(dir, "nope", "bench-preload.cjs"), dir)).toThrow(
        /--preload .*nope.*does not exist/,
      );
      NodeFS.mkdirSync(NodePath.join(dir, "preload"));
      NodeFS.writeFileSync(NodePath.join(dir, "preload", "bench-preload.cjs"), "");
      expect(resolvePreload(undefined, dir)).toBe(
        NodeFS.realpathSync(NodePath.join(dir, "preload", "bench-preload.cjs")),
      );
    });
  });
});

describe("stopGroup (how the server stopped, for server.stop in run.json)", () => {
  /** A child in its own process group that says "ready" once its SIGTERM handling is in place. */
  const child = async (code: string) => {
    const process_ = NodeChildProcess.spawn(process.execPath, ["-e", code], {
      detached: true,
      stdio: ["ignore", "pipe", "ignore"],
    });
    await new Promise<void>((resolve) => process_.stdout!.once("data", () => resolve()));
    return process_;
  };

  /** A clock that moves 7 ms per reading, so the reported time is the clock's, not the scheduler's. */
  const ticking = () => {
    let at = 1000;
    return () => (at += 7);
  };

  it("reports a stop on SIGTERM as not forced, timed on its clock", async () => {
    const server = await child("process.stdout.write('ready'); setInterval(() => {}, 1000);");
    const stop = await stopGroup(server, 10_000, ticking());
    expect(stop).toEqual({ forced: false, signal_to_exit_ms: 7, exited_before_stop: false });
  });

  it("reports a server that outlived the grace as forced (SIGKILL)", async () => {
    const server = await child(
      "process.on('SIGTERM', () => {}); process.stdout.write('ready'); setInterval(() => {}, 1000);",
    );
    const stop = await stopGroup(server, 200, ticking());
    expect(stop).toEqual({ forced: true, signal_to_exit_ms: 7, exited_before_stop: false });
  });

  it("sends nothing to a server that had already exited", async () => {
    const server = await child("process.stdout.write('ready');");
    await new Promise<void>((resolve) => {
      if (server.exitCode !== null) resolve();
      else server.once("exit", () => resolve());
    });
    expect(await stopGroup(server, 1000)).toEqual({
      forced: false,
      signal_to_exit_ms: null,
      exited_before_stop: true,
    });
  });
});

describe("driverFailure (why an attempt is driver_failed, in run.json)", () => {
  const failed = readDriverResult({
    schemaVersion: 1,
    status: "failed",
    failure: {
      phase: "window",
      code: "error_rate",
      reason: "session.dispatch failed 25.00% of calls (limit 20%)",
    },
  });

  it("carries the driver's own phase, code and reason with its exit", () => {
    expect(driverFailure(failed, { code: 1, signal: null }, "present", false)).toEqual({
      phase: "window",
      code: "error_rate",
      reason: "session.dispatch failed 25.00% of calls (limit 20%)",
      exit_code: 1,
      signal: null,
      result_file: "present",
    });
  });

  it("names the cause when the driver left no result, was killed or ran out of time", () => {
    expect(driverFailure(null, { code: 1, signal: null }, "missing", false)).toMatchObject({
      reason: "no driver.json (the driver exited with code 1)",
      exit_code: 1,
      result_file: "missing",
    });
    expect(
      driverFailure(null, { code: null, signal: "SIGKILL" }, "unreadable", false),
    ).toMatchObject({
      reason: "driver.json could not be read (the driver was ended by SIGKILL)",
      signal: "SIGKILL",
    });
    expect(driverFailure(null, { code: null, signal: "SIGTERM" }, "missing", true)).toMatchObject({
      reason: "no driver.json (the driver did not finish within its time budget and was stopped)",
    });
    const ok = readDriverResult({ schemaVersion: 1, status: "ok" });
    expect(driverFailure(ok, { code: 0, signal: null }, "present", false)).toMatchObject({
      reason: "driver.json says ok but a driver.failed marker is beside it",
    });
  });
});

describe("classifyProblems (on a saturated server some problems are saturation, the rest defects)", () => {
  const at = (elu: number | null, ...problems: Array<string>) =>
    classifyProblems({ problems, elu, traceRequired: true });

  it("keeps a saturated server's starved sessions, missing MCP clients and timeouts valid and flagged", () => {
    expect(SATURATION_ELU).toBe(0.95);
    expect(at(0.97, "sessions_starved")).toEqual({
      problems: [{ code: "sessions_starved", kind: "saturation" }],
      valid: true,
      saturated: true,
    });
    expect(at(0.95, "session_mcp_missing", "sessions_starved")).toMatchObject({
      valid: true,
      saturated: true,
    });
    expect(at(0.999, "call_timeouts")).toMatchObject({
      problems: [{ code: "call_timeouts", kind: "saturation" }],
      valid: true,
    });
  });

  it("calls the same problems defects below 0.95 or when the server ELU is unknown", () => {
    expect(at(0.94, "call_timeouts")).toEqual({
      problems: [{ code: "call_timeouts", kind: "defect" }],
      valid: false,
      saturated: false,
    });
    expect(at(0.9, "sessions_starved")).toMatchObject({ valid: false, saturated: false });
    expect(at(null, "sessions_starved")).toMatchObject({ valid: false });
  });

  it("never hides a real failure as saturation", () => {
    // One 5xx among the timeouts: the driver names the finding error_rate.
    expect(at(0.99, "error_rate")).toMatchObject({
      problems: [{ code: "error_rate", kind: "defect" }],
      valid: false,
    });
    expect(at(0.99, "driver_saturated").valid).toBe(false);
    expect(at(0.99, "distribution_mismatch").valid).toBe(false);
    expect(at(0.99, "server_exited").valid).toBe(false);
    expect(at(0.99, "driver_failed").valid).toBe(false);
    expect(at(0.99, "sessions_starved", "server_exited")).toEqual({
      problems: [
        { code: "sessions_starved", kind: "saturation" },
        { code: "server_exited", kind: "defect" },
      ],
      valid: false,
      saturated: false,
    });
  });

  it("leaves non-fatal problems as defects that do not void the attempt", () => {
    expect(at(0.5, "otlp_no_samples", "preload_not_loaded")).toEqual({
      problems: [
        { code: "otlp_no_samples", kind: "defect" },
        { code: "preload_not_loaded", kind: "defect" },
      ],
      valid: true,
      saturated: false,
    });
    expect(
      classifyProblems({ problems: ["trace_missing"], elu: 0.5, traceRequired: false }).valid,
    ).toBe(true);
    expect(
      classifyProblems({ problems: ["trace_missing"], elu: 0.5, traceRequired: true }).valid,
    ).toBe(false);
  });
});

describe("classifyProblems: the driver's own saturation, and saturation below 0.95", () => {
  it("keeps driver_saturated a defect even at ELU 0.999, and sessions_starved a defect at ELU 0.94", () => {
    expect(
      classifyProblems({ problems: ["driver_saturated"], elu: 0.999, traceRequired: true }),
    ).toEqual({
      problems: [{ code: "driver_saturated", kind: "defect" }],
      valid: false,
      saturated: false,
    });
    expect(
      classifyProblems({ problems: ["sessions_starved"], elu: 0.94, traceRequired: true }),
    ).toEqual({
      problems: [{ code: "sessions_starved", kind: "defect" }],
      valid: false,
      saturated: false,
    });
  });
});

describe("restoreFixture (every run gets a copy of the fixture the server can write)", () => {
  it("makes the copy of a read-only fixture file 0600 and leaves the fixture as it was", async () => {
    await withTempDir(async (root) => {
      const fixture = NodePath.join(root, "fixture.sqlite");
      NodeFS.writeFileSync(fixture, "sqlite bytes");
      NodeFS.chmodSync(fixture, 0o444);
      const base = NodePath.join(root, "base");
      restoreFixture({ kind: "file", path: fixture, sha256: "unused", bytes: 12 }, base);
      const copy = NodePath.join(base, "userdata", "statev2.sqlite");
      expect(NodeFS.statSync(copy).mode & 0o777).toBe(0o600);
      expect(NodeFS.readFileSync(copy, "utf8")).toBe("sqlite bytes");
      expect(NodeFS.statSync(fixture).mode & 0o777).toBe(0o444);
    });
  });

  it("makes every file of a read-only fixture directory's copy writable by its owner, and its directories too", async () => {
    await withTempDir(async (root) => {
      const fixture = NodePath.join(root, "fixture");
      NodeFS.mkdirSync(NodePath.join(fixture, "userdata"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(fixture, "userdata", "statev2.sqlite"), "sqlite bytes");
      NodeFS.writeFileSync(NodePath.join(fixture, "userdata", "compacted-at"), "1");
      NodeFS.chmodSync(NodePath.join(fixture, "userdata", "statev2.sqlite"), 0o444);
      NodeFS.chmodSync(NodePath.join(fixture, "userdata", "compacted-at"), 0o444);
      NodeFS.chmodSync(NodePath.join(fixture, "userdata"), 0o555);
      const base = NodePath.join(root, "base");
      try {
        restoreFixture({ kind: "dir", path: fixture, sha256: "unused", bytes: 13 }, base);
        const mode = (path: string) => NodeFS.statSync(NodePath.join(base, path)).mode & 0o777;
        expect(mode("userdata/statev2.sqlite") & 0o600).toBe(0o600);
        expect(mode("userdata/compacted-at") & 0o600).toBe(0o600);
        expect(mode("userdata") & 0o700).toBe(0o700);
        expect(NodeFS.statSync(NodePath.join(fixture, "userdata")).mode & 0o777).toBe(0o555);
      } finally {
        NodeFS.chmodSync(NodePath.join(fixture, "userdata"), 0o755);
      }
    });
  });
});

describe("serverElu (which ELU decides 0.95)", () => {
  it("takes the preload's window ELU, then the server gauge, else none", () => {
    expect(serverElu({ loop: { elu: 0.97 }, otlp: { elu_mean: 0.5 } })).toEqual({
      elu: 0.97,
      source: "preload",
    });
    expect(serverElu({ loop: null, otlp: { elu_mean: 0.96 } })).toEqual({
      elu: 0.96,
      source: "server_gauge",
    });
    expect(serverElu({ loop: null, otlp: null })).toEqual({ elu: null, source: null });
    expect(serverElu(null)).toEqual({ elu: null, source: null });
  });
});

describe("review fixes: unknown window findings and relative overrides", () => {
  it("treats a window failure with a code run.ts does not know as driver_failed", () => {
    const unknown = readDriverResult({
      schemaVersion: 1,
      status: "failed",
      failure: {
        phase: "window",
        code: "assertion_failed",
        reason: "a custom driver's own check",
        findings: [{ code: "assertion_failed", reason: "a custom driver's own check" }],
      },
    });
    expect(driverProblems(unknown, true)).toEqual(["driver_failed"]);
    const mixed = readDriverResult({
      schemaVersion: 1,
      status: "failed",
      failure: {
        phase: "window",
        code: "sessions_starved",
        findings: [
          { code: "sessions_starved", reason: "" },
          { code: "assertion_failed", reason: "" },
        ],
      },
    });
    expect(driverProblems(mixed, true)).toEqual(["driver_failed"]);
    expect(
      classifyProblems({ problems: driverProblems(mixed, true), elu: 0.99, traceRequired: true })
        .valid,
    ).toBe(false);
  });

  it("resolves a relative resource monitor override before it is checked and passed on", () => {
    const composed = composeServerEnv({
      defaults: {},
      serverEnv: { T3CODE_RESOURCE_MONITOR_PATH: "bin/t3-resource-monitor" },
      archive: null,
      preload: null,
    });
    const absolute = NodePath.resolve("bin/t3-resource-monitor");
    expect(composed.env.T3CODE_RESOURCE_MONITOR_PATH).toBe(absolute);
    expect(composed.resourceMonitor?.path).toBe(absolute);
  });
});

describe("--base (the base every result records)", () => {
  it("defaults to main, a public branch name, and the usage says so", () => {
    expect(DEFAULT_BASE).toBe("main");
    expect(HELP).toMatch(/--base <ref>[\s\S]*?\[main\]\./);
  });
});

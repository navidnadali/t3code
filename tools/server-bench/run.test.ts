// @effect-diagnostics nodeBuiltinImport:off - Tests build real fixture templates on disk.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import { benchEnvHash, fixtureInfo, readDriverResult, runOrder, traceRequired } from "./run.ts";

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

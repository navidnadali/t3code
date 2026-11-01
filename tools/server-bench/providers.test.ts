// @effect-diagnostics nodeBuiltinImport:off - Bench tooling tests outside the Effect runtime.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import {
  historyAssignments,
  prepareFakeState,
  withEnvironment,
  parseProviderLoad,
  planSessions,
  readBenchKnobs,
  sessionsByInstance,
  withBenchInstances,
} from "./providers.ts";

const PRIMARY = { sessions: 2, provider: "claudeBench", model: "claude-sonnet-4-6" };

describe("bench session plan", () => {
  it("interleaves every provider's sessions in proportion, in a fixed order", () => {
    const plan = planSessions(
      parseProviderLoad({ "codex-sessions": "4", "acp-sessions": "2" }, PRIMARY),
    );

    expect(plan.map((session) => session.kind)).toEqual([
      "codex",
      "claude",
      "acp",
      "codex",
      "codex",
      "claude",
      "acp",
      "codex",
    ]);
    expect(plan.map((session) => session.index)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(plan[0]).toEqual({
      index: 0,
      kind: "codex",
      instanceId: "codexBench",
      model: "gpt-6-luna",
    });
    expect(sessionsByInstance(plan)).toEqual({ claudeBench: 2, codexBench: 4, acpBench: 2 });
  });

  it("takes the driver's own sessions as the kind its provider names", () => {
    const groups = parseProviderLoad(
      { "acp-sessions": "1", "acp-provider": "acpFault", "acp-model": "m" },
      { sessions: 3, provider: "codexBench", model: "gpt-6.1-sol" },
    );

    expect(groups.map((group) => [group.kind, group.instanceId, group.model, group.count])).toEqual(
      [
        ["codex", "codexBench", "gpt-6.1-sol", 3],
        ["codex", "codexBench", "gpt-6-luna", 0],
        ["acp", "acpFault", "m", 1],
      ],
    );
    expect(() => parseProviderLoad({ "codex-sessions": "-1" }, PRIMARY)).toThrow(
      /--codex-sessions/,
    );
    expect(() => parseProviderLoad({ "acp-sessions": "1.5" }, PRIMARY)).toThrow(/--acp-sessions/);
  });
});

describe("history mode", () => {
  it("keeps every session on its planned instance and counts the threads each takes over", () => {
    const plan = planSessions(
      parseProviderLoad(
        { "codex-sessions": "2", "acp-sessions": "1" },
        { ...PRIMARY, sessions: 1 },
      ),
    );
    const thread = (instanceId: string, model: string) => ({
      id: instanceId,
      modelSelection: { instanceId, model },
    });
    const { assignments, switches } = historyAssignments(plan, [
      thread("codex", "gpt-6.1-sol"),
      thread("claudeBench", "claude-sonnet-4-6"),
      thread("acpBench", "bench-replay"),
      thread("claudeAgent", "claude-sonnet-4-6"),
      thread("spare", "claude-sonnet-4-6"),
    ]);

    expect(
      assignments.map(({ session, thread: seeded, switchProvider }) => [
        session.instanceId,
        seeded.id,
        switchProvider,
      ]),
    ).toEqual([
      ["codexBench", "codex", true],
      ["claudeBench", "claudeBench", false],
      ["acpBench", "acpBench", false],
      ["codexBench", "claudeAgent", true],
    ]);
    expect(switches).toEqual({ codexBench: 2 });
    expect(() => historyAssignments(plan, [thread("codex", "gpt-6.1-sol")])).toThrow(/4 sessions/);
  });

  it("moves a seeded thread on its planned instance to the planned model", () => {
    const plan = planSessions(
      parseProviderLoad({ "codex-sessions": "2" }, { ...PRIMARY, sessions: 0 }),
    );
    const { assignments, switches, modelSwitches } = historyAssignments(plan, [
      { id: "sol", modelSelection: { instanceId: "codexBench", model: "gpt-6.1-sol" } },
      { id: "luna", modelSelection: { instanceId: "codexBench", model: "gpt-6-luna" } },
    ]);

    expect(
      assignments.map(({ session, thread, switchProvider }) => [
        `${session.instanceId}/${session.model}`,
        thread.id,
        switchProvider,
      ]),
    ).toEqual([
      ["codexBench/gpt-6-luna", "sol", true],
      ["codexBench/gpt-6-luna", "luna", false],
    ]);
    expect(switches).toEqual({});
    expect(modelSwitches).toEqual({ codexBench: 1 });
  });
});

describe("bench provider settings", () => {
  it("registers the fakes with their knobs, disables built-ins and keeps other settings", () => {
    const settings = withBenchInstances(
      {
        theme: "dark",
        providerInstances: { codex: { driver: "codex", config: { binaryPath: "/usr/bin/codex" } } },
      },
      {
        benchDir: "/bench",
        node: "/node",
        statsDir: "/stats",
        kinds: ["codex", "acp"],
        knobs: { speed: "4", deltasPerSec: undefined, turnLoops: "2" },
        transcripts: { acp: "simple,todo_list" },
        env: { codex: { BENCH_FAULT: "exit-mid-turn" } },
        codexHome: "/codex-home",
      },
    );
    const instances = settings.providerInstances as Record<string, Record<string, unknown>>;

    expect(settings.theme).toBe("dark");
    expect(settings.textGenerationModelSelection).toBeUndefined();
    expect(instances.codex).toEqual({
      driver: "codex",
      config: { binaryPath: "/usr/bin/codex" },
      enabled: false,
    });
    expect(instances.claudeAgent).toEqual({ driver: "claudeAgent", enabled: false });
    expect(instances.codexBench).toMatchObject({
      driver: "codex",
      config: { binaryPath: "/bench/fake-codex/codex", homePath: "/codex-home" },
    });
    expect(instances.acpBench).toMatchObject({
      driver: "acpRegistry",
      config: { source: "local", commandPath: "/bench/fake-acp/acp-agent", commandArgs: [] },
    });

    const file = NodePath.join(
      NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "bench-")),
      "s.json",
    );
    NodeFS.writeFileSync(file, JSON.stringify(settings));
    expect(readBenchKnobs(file)).toEqual({
      codexBench: {
        BENCH_STATS_DIR: "/stats",
        BENCH_SPEED: "4",
        BENCH_TURN_LOOPS: "2",
        BENCH_FAULT: "exit-mid-turn",
      },
      acpBench: {
        BENCH_STATS_DIR: "/stats",
        BENCH_SPEED: "4",
        BENCH_TURN_LOOPS: "2",
        BENCH_ACP_TRANSCRIPT: "simple,todo_list",
      },
    });
  });
});

describe("fake state per run", () => {
  it("gives each planned instance a fresh, owner-only directory under the run's --out", () => {
    const out = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "bench-run-"));
    const dirs = prepareFakeState(out, ["codexBench", "acp/Bench"]);

    expect(dirs).toEqual({
      codexBench: NodePath.join(out, "fake-state", "codexBench"),
      "acp/Bench": NodePath.join(out, "fake-state", "acp_Bench"),
    });
    for (const dir of [NodePath.join(out, "fake-state"), ...Object.values(dirs)]) {
      expect(NodeFS.readdirSync(dir).filter((name) => !name.endsWith("Bench"))).toEqual([]);
      expect(NodeFS.statSync(dir).mode & 0o777).toBe(0o700);
    }
    // A run that reuses an --out would claim ordinals after the previous run's.
    NodeFS.writeFileSync(NodePath.join(dirs.codexBench!, "ordinal-0"), "thread");
    expect(() => prepareFakeState(out, ["codexBench"])).toThrow(/not empty/);
  });

  it("sets environment variables on an instance, replacing what it had and keeping the rest", () => {
    expect(
      withEnvironment(
        [
          { name: "BENCH_STATE_DIR", value: "/old", sensitive: false },
          { name: "BENCH_SPEED", value: "4", sensitive: false },
        ],
        { BENCH_STATE_DIR: "/run/fake-state/codexBench", BENCH_MCP_CREDENTIALS_DIR: "/run/c" },
      ),
    ).toEqual([
      { name: "BENCH_SPEED", value: "4", sensitive: false },
      { name: "BENCH_STATE_DIR", value: "/run/fake-state/codexBench", sensitive: false },
      { name: "BENCH_MCP_CREDENTIALS_DIR", value: "/run/c", sensitive: false },
    ]);
  });
});

describe("bench provider settings command", () => {
  it("leaves the fakes' state directory to the load driver", () => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "bench-settings-"));
    const out = NodePath.join(dir, "settings.json");
    const run = NodeChildProcess.spawnSync(
      process.execPath,
      [
        NodePath.join(import.meta.dirname, "providers.ts"),
        "settings",
        "--out",
        out,
        "--stats-dir",
        NodePath.join(dir, "stats"),
      ],
      { encoding: "utf8" },
    );

    expect(run.status).toBe(0);
    expect(NodeFS.readFileSync(out, "utf8")).not.toContain("BENCH_STATE_DIR");
  });

  it.each([
    [["--speed=-1"], /BENCH_SPEED/],
    [["--turn-loops=0"], /BENCH_TURN_LOOPS/],
    [["--codex-transcripts", "no_such_fixture"], /no_such_fixture/],
    [["--env", "acp:BENCH_FAULT=explode"], /BENCH_FAULT/],
  ])("refuses %j before writing anything", (args, problem) => {
    const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "bench-settings-"));
    const out = NodePath.join(dir, "settings.json");
    const run = NodeChildProcess.spawnSync(
      process.execPath,
      [
        NodePath.join(import.meta.dirname, "providers.ts"),
        "settings",
        "--out",
        out,
        "--stats-dir",
        NodePath.join(dir, "stats"),
        ...args,
      ],
      { encoding: "utf8" },
    );

    expect(run.status).toBe(2);
    expect(run.stderr).toMatch(problem);
    expect(NodeFS.existsSync(out)).toBe(false);
  });
});

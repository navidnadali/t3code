// @effect-diagnostics nodeBuiltinImport:off - Bench tooling tests outside the Effect runtime.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import {
  compareItems,
  failureCause,
  type ServerItem,
  type TurnExpectation,
} from "./provider-check.ts";

const CHECK = NodePath.join(import.meta.dirname, "provider-check.ts");

/** Runs provider-check on a scratch home holding only settings; returns its exit and result. */
function runCheck(args: ReadonlyArray<string>) {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "provider-check-"));
  NodeFS.mkdirSync(NodePath.join(dir, "home", "userdata"), { recursive: true });
  NodeFS.writeFileSync(
    NodePath.join(dir, "home", "userdata", "settings.json"),
    JSON.stringify({ providerInstances: {} }),
  );
  const out = NodePath.join(dir, "out");
  NodeFS.mkdirSync(out);
  NodeFS.writeFileSync(NodePath.join(out, "provider-check.json"), '{"ok":true,"verdict":"MATCH"}');
  const run = NodeChildProcess.spawnSync(
    process.execPath,
    [CHECK, "--base-dir", NodePath.join(dir, "home"), "--out", out, ...args],
    { encoding: "utf8" },
  );
  return {
    status: run.status,
    result: JSON.parse(NodeFS.readFileSync(NodePath.join(out, "provider-check.json"), "utf8")),
    marker: NodeFS.existsSync(NodePath.join(out, "provider-check.failed")),
  };
}

// The tail of what the server logs when a provider session cannot open (badknob run).
const SERVER_LOG = `[22:31:00.726] WARN (#8057): Orchestration effect execution failed
  {
    effectId: 'effect:mcp:14e98b13-d9e4-4f85-b334-9bdcceb77696:release:provider-turn.start:run:thread:mcp%3A14e98b13-d9e4-4f85-b334-9bdcceb77696:ordinal:1',
    effectType: 'provider-turn.start',
    error: 'OrchestrationEffectExecutionError: \\n' +
      '    at file:///dist/binCli.mjs:302540:32\\n' +
      '  [cause]: ProviderTurnStartError: \\n' +
      '      at file:///dist/binCli.mjs:302191:98 {\\n' +
      '    [cause]: ProviderSessionOpenError: Failed to open provider instance codexBench session provider-session:provider-instance:codexBench:shared.\\n' +
      '        at file:///dist/binCli.mjs:282275:31 {\\n' +
      '        [cause]: CodexAppServerProcessExitedError: Codex App Server process exited with code 2: fake codex: BENCH_SPEED must be a number >= 0, not "-1".\\n' +
      '            at file:///dist/binCli.mjs:227435:10\\n' +
      '}'
  }
[22:31:00.743] WARN (#8067): Orchestration effect execution failed
  {
    effectId: 'effect:mcp:b133373f-358e-408b-9e4e-43ad18bfe451:release:provider-turn.start:run:thread:mcp%3Ab133373f-358e-408b-9e4e-43ad18bfe451:ordinal:1',
    error: 'OrchestrationEffectExecutionError: \\n' +
      '        [cause]: AcpProcessExitedError: ACP process exited with code 2\\n' +
      '        fake acp agent: BENCH_TURN_LOOPS must be an integer >= 1, not "0".\\n' +
      '            at file:///dist/binCli.mjs:171134:40\\n' +
      '}'
  }
`;

const turn = (items: TurnExpectation["items"], todoLists = 0): TurnExpectation => ({
  frames: {},
  items,
  todoLists,
  reply: null,
});
const rows = (...types: ReadonlyArray<string>): ReadonlyArray<ServerItem> =>
  types.map((type, index) => ({ type, status: "completed", id: `turn-item:${type}:${index}` }));
const SERVER_STEPS = rows("checkpoint").concat([
  { type: "command_execution", status: "completed", id: "native-item:workspace-preparation%3Arun" },
]);

describe("provider check items", () => {
  it("matches a Codex plan turn only while its proposed plan is there", () => {
    const played = [turn({ userMessage: 1, plan: 1 })];

    expect(
      compareItems("codex", played, [...SERVER_STEPS, ...rows("user_message", "proposed_plan")]),
    ).toMatchObject({ match: true, expected: { user_message: 1, proposed_plan: 1 } });
    expect(compareItems("codex", played, [...SERVER_STEPS, ...rows("user_message")]).match).toBe(
      false,
    );
  });

  it("never matches a transcript item it cannot map or a server item nobody expected", () => {
    const subagent = compareItems(
      "codex",
      [turn({ userMessage: 1, agentMessage: 1, collabAgentToolCall: 1 })],
      rows("user_message", "assistant_message"),
    );
    const extra = compareItems(
      "codex",
      [turn({ userMessage: 1, agentMessage: 1 })],
      rows("user_message", "assistant_message", "dynamic_tool"),
    );

    expect(subagent).toMatchObject({ match: false, unmapped: ["collabAgentToolCall"] });
    expect(extra).toMatchObject({ match: false, actual: { dynamic_tool: 1 } });
  });

  it("counts any other ACP item as a tool call", () => {
    const played = [turn({ assistant_message: 2, reasoning: 0, tool: 2 }, 1)];
    const server = rows("user_message", "assistant_message", "assistant_message", "todo_list");

    expect(
      compareItems("acp", played, [...server, ...rows("dynamic_tool", "file_read")]),
    ).toMatchObject({
      match: true,
      actual: { user_message: 1, assistant_message: 2, todo_list: 1, tool: 2 },
    });
    expect(compareItems("acp", played, [...server, ...rows("dynamic_tool")]).match).toBe(false);
  });
});

describe("provider check runs", () => {
  it("reports what made a provider session fail, from the server log", () => {
    expect(failureCause(SERVER_LOG, "mcp:14e98b13-d9e4-4f85-b334-9bdcceb77696")).toBe(
      'CodexAppServerProcessExitedError: Codex App Server process exited with code 2: fake codex: BENCH_SPEED must be a number >= 0, not "-1".',
    );
    expect(failureCause(SERVER_LOG, "mcp:b133373f-358e-408b-9e4e-43ad18bfe451")).toBe(
      'AcpProcessExitedError: ACP process exited with code 2; fake acp agent: BENCH_TURN_LOOPS must be an integer >= 1, not "0".',
    );
    expect(failureCause(SERVER_LOG, "mcp:00000000-0000-4000-8000-000000000000")).toBeNull();
  });

  it("ends a run it cannot start with a failed result, replacing an old one", () => {
    const run = runCheck(["--project", "/nonexistent/project", "--codex-sessions", "1"]);

    expect(run.status).toBe(2);
    expect(run.result).toMatchObject({ ok: false, verdict: "ERROR" });
    expect(run.result.error).toMatch(/--project \/nonexistent\/project does not exist/);
    expect(run.marker).toBe(true);
  });

  it("takes --call-timeout as a duration, as the load driver does", () => {
    const run = runCheck([
      "--project",
      "/nonexistent/project",
      "--codex-sessions",
      "1",
      "--call-timeout",
      "90s",
    ]);

    expect(run.result.error).toMatch(/--project/);
  });
});

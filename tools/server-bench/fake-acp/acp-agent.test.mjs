import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

import { describe, expect, it } from "vite-plus/test";

import * as AcpSchema from "../../../packages/effect-acp/src/_generated/schema-v1.gen.ts";
import { FIXTURES_DIR, transcriptPath } from "../fake-common/replay.mjs";
import {
  compileTranscript,
  expectedTurn,
  makeAnswer,
  TRANSCRIPT_FILE,
  turnLines,
} from "./acp-agent.mjs";

// effect resolves from the packages that depend on it, not from the repo root.
const Schema = await import(
  NodeModule.createRequire(
    new URL("../../../packages/effect-acp/package.json", import.meta.url),
  ).resolve("effect/Schema")
);

const AGENT = NodeURL.fileURLToPath(new URL("./acp-agent.mjs", import.meta.url));
const LIVE = { sessionId: "11111111-2222-4333-8444-555555555555", cwd: "/live/project" };
const FIXTURES = NodeFS.readdirSync(FIXTURES_DIR).filter((name) =>
  NodeFS.existsSync(NodePath.join(FIXTURES_DIR, name, TRANSCRIPT_FILE)),
);
const transcriptOf = (name) => NodeFS.readFileSync(transcriptPath(name, TRANSCRIPT_FILE), "utf8");
const decodes = (schema, value) => {
  try {
    Schema.decodeUnknownSync(schema)(value);
    return true;
  } catch {
    return false;
  }
};

function startAgent(env) {
  const child = NodeChildProcess.spawn(process.execPath, [AGENT], {
    env: { ...process.env, BENCH_SPEED: "0", ...env },
    stdio: ["pipe", "pipe", "inherit"],
  });
  const lines = NodeReadline.createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  return {
    exited: new Promise((resolve) => child.once("exit", (code) => resolve(code))),
    send: (...messages) =>
      child.stdin.write(
        messages.map((message) => `${JSON.stringify({ jsonrpc: "2.0", ...message })}\n`).join(""),
      ),
    /** Reads stdout lines up to and including the first one `match` accepts. */
    until: async (match) => {
      const read = [];
      for (;;) {
        const next = await lines.next();
        if (next.done) return read;
        read.push(next.value);
        if (match(next.value)) return read;
      }
    },
    end: () => child.stdin.end(),
    [Symbol.dispose]: () => child.kill(),
  };
}

const frameOf = (line) => {
  try {
    return JSON.parse(line);
  } catch {
    return undefined;
  }
};
const answerTo = (id) => (line) => frameOf(line)?.id === id && frameOf(line)?.method === undefined;

/** Plays `sessions` sessions (one agent each) of `turns` prompts; returns each turn's plan. */
async function offeredPlan(env, sessions, turns) {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fake-acp-plan-"));
  const stats = NodePath.join(dir, "stats");
  for (let session = 0; session < sessions; session++) {
    using agent = startAgent({
      ...env,
      BENCH_STATS_DIR: stats,
      BENCH_STATE_DIR: NodePath.join(dir, "state"),
    });
    const sessionId = await openSession(agent);
    for (let turn = 0; turn < turns; turn++) {
      agent.send({ id: 10 + turn, method: "session/prompt", params: { sessionId, prompt: [] } });
      await agent.until(answerTo(10 + turn));
    }
    agent.end();
    await agent.exited;
  }
  return NodeFS.readdirSync(stats)
    .flatMap((file) =>
      NodeFS.readFileSync(NodePath.join(stats, file), "utf8")
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line)),
    )
    .toSorted((left, right) => left.ordinal - right.ordinal || left.at.localeCompare(right.at))
    .map(({ ordinal, transcript, turn, seed, mix, frames }) => ({
      ordinal,
      transcript,
      turn,
      seed,
      mix,
      frames,
    }));
}

async function openSession(agent) {
  agent.send(
    { id: 1, method: "initialize", params: { protocolVersion: 1, clientCapabilities: {} } },
    { id: 2, method: "session/new", params: { cwd: LIVE.cwd, mcpServers: [] } },
  );
  const frames = (await agent.until(answerTo(2))).map(frameOf);
  expect(decodes(AcpSchema.InitializeResponse, frames.find((frame) => frame.id === 1).result)).toBe(
    true,
  );
  expect(decodes(AcpSchema.NewSessionResponse, frames.at(-1).result)).toBe(true);
  return frames.at(-1).result.sessionId;
}

describe("fake acp transcripts", () => {
  // malformed_session_update records frames the schema rejects, on purpose.
  it.each(FIXTURES.filter((name) => name !== "malformed_session_update"))(
    "replays %s as session updates the ACP schema accepts",
    (name) => {
      const text = transcriptOf(name);
      for (const turn of compileTranscript(text)) {
        const lines = [...turnLines(turn, LIVE, 2)].map(({ line }) => line);
        const updates = lines
          .map((line) => JSON.parse(line))
          .filter((frame) => frame.id === undefined);

        expect(
          updates.filter((frame) => !decodes(AcpSchema.SessionNotification, frame.params)),
        ).toEqual([]);
        expect(new Set(updates.map((frame) => frame.params.sessionId))).toEqual(
          new Set(updates.length === 0 ? [] : [LIVE.sessionId]),
        );
        expect(lines.join("\n")).not.toContain("<workspace>");
        const started = updates
          .filter((frame) => frame.params.update.sessionUpdate === "tool_call")
          .map((frame) => frame.params.update.toolCallId);
        expect(new Set(started).size).toBe(started.length);
      }
    },
  );

  it("drops prompt turns the recording ended with a cancel", () => {
    expect(
      compileTranscript(transcriptOf("message_steering")).map((turn) => turn.stopReason),
    ).toEqual(["end_turn"]);
  });

  it("streams text in chunks and counts a run of chunks as one message", async () => {
    const [turn] = compileTranscript(transcriptOf("todo_list"));
    const kinds = turn.frames.map((frame) => frame.stat);

    expect(kinds.filter((kind) => kind.endsWith("agent_message_chunk")).length).toBeGreaterThan(2);
    expect(await expectedTurn("todo_list", 0, 2)).toEqual({
      frames: Object.fromEntries(
        Object.entries(Object.groupBy(kinds, (kind) => kind)).map(([kind, list]) => [
          kind,
          list.length * 2,
        ]),
      ),
      // The last chunk of one loop and the first of the next stream as one message.
      items: { assistant_message: 3, reasoning: 0, tool: 4 },
      todoLists: 1,
      reply: "todo list fixture complete",
    });
  });
});

describe("fake acp agent", () => {
  it("answers a prompt with the replayed turn and a stop reason the client decodes", async () => {
    using agent = startAgent({ BENCH_ACP_TRANSCRIPT: "simple" });
    const sessionId = await openSession(agent);
    agent.send({ id: 3, method: "session/prompt", params: { sessionId, prompt: [] } });
    const frames = (await agent.until(answerTo(3))).map(frameOf);

    const kinds = frames
      .slice(0, -1)
      .map((frame) => `session/update:${frame.params.update.sessionUpdate}`);
    expect(
      Object.fromEntries(
        Object.entries(Object.groupBy(kinds, (kind) => kind)).map(([kind, list]) => [
          kind,
          list.length,
        ]),
      ),
    ).toEqual((await expectedTurn("simple", 0, 1)).frames);
    expect(frames.at(-1).result).toEqual({ stopReason: "end_turn" });
    expect(decodes(AcpSchema.PromptResponse, frames.at(-1).result)).toBe(true);
  });

  it("follows the streamed answer with the live cwd in place of the recorded workspace", () => {
    const [turn] = compileTranscript(
      transcriptOf("todo_list").replaceAll("todo list fixture complete", "done in <workspace>"),
    );
    const answer = makeAnswer();
    for (const { frame, line } of turnLines(turn, LIVE, 1)) answer.add(frame, line);

    expect(answer.text).toBe(`done in ${LIVE.cwd}`);
  });

  it("replays large-tool-payload with megabyte tool output the ACP schema accepts", async () => {
    using agent = startAgent({
      BENCH_ACP_TRANSCRIPT: "large-tool-payload",
      BENCH_TOOL_PAYLOAD_BYTES: "1048576",
    });
    const sessionId = await openSession(agent);
    agent.send({ id: 3, method: "session/prompt", params: { sessionId, prompt: [] } });
    const lines = await agent.until(answerTo(3));
    const updates = lines
      .map(frameOf)
      .filter((frame) => frame.method === "session/update")
      .map((frame) => frame.params);

    expect(updates.filter((params) => !decodes(AcpSchema.SessionNotification, params))).toEqual([]);
    expect(lines.filter((line) => Buffer.byteLength(line) > 1_048_576)).toHaveLength(2);
    expect(
      updates
        .filter((params) => params.update.sessionUpdate === "tool_call_update")
        .map((params) => params.update.status),
    ).toEqual(["completed", "completed"]);
    expect(frameOf(lines.at(-1)).result).toEqual({ stopReason: "end_turn" });
  });

  it.each([
    [
      "a stdio bridge",
      {
        name: "t3-code",
        command: "/usr/bin/node",
        args: ["acp-mcp-bridge"],
        env: [
          { name: "T3_ACP_MCP_ENDPOINT", value: "http://127.0.0.1:3773/mcp" },
          { name: "T3_ACP_MCP_AUTHORIZATION", value: "Bearer secret" },
        ],
      },
    ],
    [
      "HTTP",
      {
        type: "http",
        name: "t3-code",
        url: "http://127.0.0.1:3773/mcp",
        headers: [{ name: "Authorization", value: "Bearer secret" }],
      },
    ],
  ])("hands the session's T3 MCP credentials over %s to the load driver", async (_, server) => {
    const dir = NodePath.join(
      NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fake-acp-mcp-")),
      "credentials",
    );
    using agent = startAgent({ BENCH_MCP_CREDENTIALS_DIR: dir });
    agent.send(
      { id: 1, method: "initialize", params: { protocolVersion: 1 } },
      { id: 2, method: "session/new", params: { cwd: LIVE.cwd, mcpServers: [server] } },
    );
    const lines = await agent.until(answerTo(2));
    const file = NodePath.join(dir, `${frameOf(lines.at(-1)).result.sessionId}.json`);

    expect(JSON.parse(NodeFS.readFileSync(file, "utf8"))).toEqual({
      url: "http://127.0.0.1:3773/mcp",
      authorization: "Bearer secret",
    });
    expect(NodeFS.statSync(file).mode & 0o777).toBe(0o600);
    expect(NodeFS.statSync(dir).mode & 0o777).toBe(0o700);
    expect(lines.join("\n")).not.toContain("Bearer secret");
  });

  it("stops a prompt cancelled with it and plays the next prompt after it", async () => {
    using agent = startAgent({});
    const sessionId = await openSession(agent);
    agent.send(
      { id: 3, method: "session/prompt", params: { sessionId, prompt: [] } },
      { method: "session/cancel", params: { sessionId } },
      { id: 4, method: "session/prompt", params: { sessionId, prompt: [] } },
    );
    const first = (await agent.until(answerTo(3))).map(frameOf);
    const second = (await agent.until(answerTo(4))).map(frameOf);

    expect(first).toEqual([{ jsonrpc: "2.0", id: 3, result: { stopReason: "cancelled" } }]);
    expect(second.length).toBeGreaterThan(1);
    expect(second.at(-1).result).toEqual({ stopReason: "end_turn" });
  });

  it("offers the same seeded, weighted plan on every run, whatever the session ids", async () => {
    const env = { BENCH_SEED: "3", BENCH_ACP_TRANSCRIPT: "multi_turn:2,simple,todo_list:1" };
    const first = await offeredPlan(env, 5, 2);

    expect(await offeredPlan(env, 5, 2)).toEqual(first);
    // The weighted cycle is multi_turn, simple, todo_list, multi_turn; seed 3 starts at its end.
    expect(first.map(({ ordinal, transcript, turn }) => [ordinal, transcript, turn])).toEqual([
      [0, "multi_turn", 0],
      [0, "multi_turn", 1],
      [1, "multi_turn", 0],
      [1, "multi_turn", 1],
      [2, "simple", 0],
      [2, "simple", 0],
      [3, "todo_list", 0],
      [3, "todo_list", 0],
      [4, "multi_turn", 0],
      [4, "multi_turn", 1],
    ]);
    expect(first[0]).toMatchObject({ seed: 3, mix: { multi_turn: 2, simple: 1, todo_list: 1 } });
  });

  it.each([
    ["exit-mid-turn", 0],
    ["truncated-frame-exit", 1],
  ])("with BENCH_FAULT=%s exits 1 mid-turn after %i cut-off lines", async (fault, cut) => {
    using agent = startAgent({ BENCH_FAULT: fault });
    const sessionId = await openSession(agent);
    agent.send({ id: 3, method: "session/prompt", params: { sessionId, prompt: [] } });
    const lines = await agent.until(answerTo(3));

    expect(lines.filter((line) => frameOf(line) === undefined)).toHaveLength(cut);
    expect(lines.some(answerTo(3))).toBe(false);
    expect(await agent.exited).toBe(1);
  });
});

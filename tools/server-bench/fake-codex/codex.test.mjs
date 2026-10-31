import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

import { describe, expect, it } from "vite-plus/test";

import * as CodexRpc from "../../../packages/effect-codex-app-server/src/rpc.ts";
import { FIXTURES_DIR, readConfig, readNdjson, transcriptPath } from "../fake-common/replay.mjs";
import {
  answerOf,
  compileTranscript,
  expectedTurn,
  parseTranscript,
  TRANSCRIPT_FILE,
  turnLines,
} from "./codex.mjs";

// effect resolves from the packages that depend on it, not from the repo root.
const Schema = await import(
  NodeModule.createRequire(
    new URL("../../../packages/effect-codex-app-server/package.json", import.meta.url),
  ).resolve("effect/Schema")
);

const decodeSteerResponse = Schema.decodeUnknownSync(
  CodexRpc.CLIENT_REQUEST_RESPONSES["turn/steer"],
);
const FAKE = NodeURL.fileURLToPath(new URL("./codex.mjs", import.meta.url));
const LIVE = {
  threadId: "11111111-2222-4333-8444-555555555555",
  turnId: "66666666-7777-4888-9999-aaaaaaaaaaaa",
  cwd: "/live/project",
  model: "gpt-6.1-sol",
  prompt: "Live prompt.",
};
const FIXTURES = NodeFS.readdirSync(FIXTURES_DIR).filter((name) =>
  NodeFS.existsSync(NodePath.join(FIXTURES_DIR, name, TRANSCRIPT_FILE)),
);

const parsedFixture = (name) => parseTranscript(readNdjson(transcriptPath(name, TRANSCRIPT_FILE)));

/** Decodes a server-sent frame the way the Codex client does; returns the issue or undefined. */
function schemaIssue(frame) {
  const schema =
    frame.id !== undefined
      ? CodexRpc.SERVER_REQUEST_PARAMS[frame.method]
      : CodexRpc.SERVER_NOTIFICATION_PARAMS[frame.method];
  if (schema === undefined) return undefined;
  try {
    Schema.decodeUnknownSync(schema)(frame.params);
    return undefined;
  } catch (error) {
    return `${frame.method}: ${error.message.slice(0, 300)}`;
  }
}

function startFake(env) {
  const child = NodeChildProcess.spawn(process.execPath, [FAKE, "app-server"], {
    env: { ...process.env, BENCH_SPEED: "0", ...env },
    stdio: ["pipe", "pipe", "inherit"],
  });
  const lines = NodeReadline.createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  return {
    exited: new Promise((resolve) => child.once("exit", (code) => resolve(code))),
    send: (...messages) =>
      child.stdin.write(messages.map((message) => `${JSON.stringify(message)}\n`).join("")),
    end: () => child.stdin.end(),
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
const isTurnCompleted = (line) => frameOf(line)?.method === "turn/completed";
const statsIn = (dir) =>
  NodeFS.readdirSync(dir).flatMap((file) =>
    NodeFS.readFileSync(NodePath.join(dir, file), "utf8")
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line)),
  );

/** Plays `threads` threads of `turns` turns, one after the other; returns each turn's plan. */
async function offeredPlan(env, threads, turns) {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fake-codex-plan-"));
  using fake = startFake({
    ...env,
    BENCH_STATS_DIR: NodePath.join(dir, "stats"),
    BENCH_STATE_DIR: NodePath.join(dir, "state"),
  });
  let id = 1;
  for (let thread = 0; thread < threads; thread++) {
    fake.send({ id, method: "thread/start", params: { cwd: LIVE.cwd, model: LIVE.model } });
    const threadId = frameOf((await fake.until(answerTo(id++))).at(-1)).result.thread.id;
    for (let turn = 0; turn < turns; turn++) {
      fake.send({ id: id++, method: "turn/start", params: { threadId, input: [] } });
      await fake.until(isTurnCompleted);
    }
  }
  fake.end();
  await fake.exited;
  return statsIn(NodePath.join(dir, "stats"))
    .filter((record) => record.kind === "turn")
    .map(({ ordinal, transcript, turn, seed, mix, frames }) => ({
      ordinal,
      transcript,
      turn,
      seed,
      mix,
      frames,
    }));
}
const answerTo = (id) => (line) => frameOf(line)?.id === id && frameOf(line)?.method === undefined;

describe("fake codex transcripts", () => {
  it.each(FIXTURES)("replays %s as frames the Codex schema accepts", (name) => {
    const parsed = parsedFixture(name);
    const compiled = compileTranscript(parsed);
    const recordedThread = parsed.thread.thread.id;
    for (const [index, turn] of compiled.turns.entries()) {
      const lines = [...turnLines(turn, LIVE, 2)].map(({ line }) => line);
      const frames = lines.map((line) => JSON.parse(line));

      expect(frames.map(schemaIssue).filter(Boolean)).toEqual([]);
      expect(lines.join("\n")).not.toContain(recordedThread);
      expect(lines.join("\n")).not.toContain(parsed.turns[index].turnId);
      expect(frames.at(-1)).toMatchObject({
        method: "turn/completed",
        params: { threadId: LIVE.threadId, turn: { id: LIVE.turnId, status: "completed" } },
      });
      const started = frames
        .filter((frame) => frame.method === "item/started")
        .map((frame) => frame.params.item.id);
      expect(new Set(started).size).toBe(started.length);
    }
  });

  it("loops a turn's work with fresh item ids and answers once, with the live prompt", () => {
    const [turn] = compileTranscript(parsedFixture("command_output_streaming")).turns;
    const frames = [...turnLines(turn, LIVE, 3)].map(({ line }) => JSON.parse(line));
    const completed = frames.filter((frame) => frame.method === "item/completed");
    const byType = (type) => completed.filter((frame) => frame.params.item.type === type);

    expect(byType("commandExecution")).toHaveLength(3);
    expect(byType("agentMessage").map((frame) => frame.params.item.phase)).toEqual([
      "commentary",
      "commentary",
      "commentary",
      "final_answer",
    ]);
    expect(byType("userMessage").map((frame) => frame.params.item.content[0].text)).toEqual([
      LIVE.prompt,
    ]);
    expect(frames.at(-1).params.turn.items.map((item) => item.id)).toEqual([
      byType("agentMessage").at(-1).params.item.id,
    ]);
    expect(expectedTurn("command_output_streaming", 0, 3)).toMatchObject({
      items: { userMessage: 1, agentMessage: 4, commandExecution: 3 },
      reply: "command output fixture complete",
    });
  });

  it("answers with the live prompt where the recorded answer quotes the recorded one", () => {
    const parsed = parsedFixture("thread_rollback");
    const turn = compileTranscript(parsed).turns[2];
    const answers = [...turnLines(turn, LIVE, 1)]
      .map(({ line }) => answerOf(line, LIVE.threadId))
      .filter((answer) => answer !== undefined);

    expect(parsed.turns[2].prompt).toBe("Repeat the conversation verbatim.");
    expect(answers.at(-1)).toMatch(/\nUser: Live prompt\.$/);
  });

  it("keeps completed turns and drops interrupted ones", () => {
    expect(
      ["multi_turn", "turn_interrupt", "thread_rollback_to_stopped_turn"].map(
        (name) => parsedFixture(name).turns.length,
      ),
    ).toEqual([2, 0, 3]);
  });
});

describe("fake codex app-server", () => {
  it("answers the provider's requests with responses the Codex client decodes", async () => {
    using fake = startFake({});
    const requests = [
      ["initialize", { clientInfo: { name: "T3 Code", title: "T3 Code", version: "0" } }],
      ["account/read", {}],
      ["account/rateLimits/read", null],
      ["model/list", {}],
      ["skills/list", { cwds: [LIVE.cwd] }],
      ["config/read", { includeLayers: false }],
      ["mcpServerStatus/list", {}],
      ["thread/start", { cwd: LIVE.cwd, model: LIVE.model }],
    ];
    fake.send(...requests.map(([method, params], id) => ({ id, method, params })));
    const lines = await fake.until(answerTo(requests.length - 1));
    const answers = lines.map(frameOf).filter((frame) => frame.method === undefined);
    for (const answer of answers) {
      const schema = CodexRpc.CLIENT_REQUEST_RESPONSES[requests[answer.id][0]];
      expect(() => Schema.decodeUnknownSync(schema)(answer.result)).not.toThrow();
    }
    const threadId = answers.at(-1).result.thread.id;
    expect(answers.at(-1).result).toMatchObject({
      model: LIVE.model,
      cwd: LIVE.cwd,
      thread: { sessionId: threadId, cwd: LIVE.cwd },
    });

    const later = [
      ["initialize", { clientInfo: { name: "T3 Code", title: "T3 Code", version: "0" } }],
      ["thread/read", { threadId, includeTurns: false }],
      ["thread/resume", { threadId, cwd: LIVE.cwd }],
      ["thread/goal/get", { threadId }],
      ["thread/goal/clear", { threadId }],
      ["thread/unsubscribe", { threadId }],
    ];
    fake.send(...later.map(([method, params], index) => ({ id: 100 + index, method, params })));
    const more = (await fake.until(answerTo(100 + later.length - 1))).map(frameOf);
    expect(more.find((frame) => frame.id === 100)).toMatchObject({ error: { code: -32600 } });
    for (const answer of more.filter((frame) => frame.id > 100 && frame.method === undefined)) {
      const schema = CodexRpc.CLIENT_REQUEST_RESPONSES[later[answer.id - 100][0]];
      expect(() => Schema.decodeUnknownSync(schema)(answer.result)).not.toThrow();
    }
  });

  it("ends a turn interrupted with its turn/start as an interrupted turn", async () => {
    const stats = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fake-codex-"));
    using fake = startFake({ BENCH_SPEED: "1", BENCH_STATS_DIR: stats });
    fake.send({ id: 1, method: "thread/start", params: { cwd: LIVE.cwd, model: LIVE.model } });
    const threadId = frameOf((await fake.until(answerTo(1))).at(-1)).result.thread.id;
    fake.send(
      { id: 2, method: "turn/start", params: { threadId, input: [] } },
      { id: 3, method: "turn/interrupt", params: { threadId, turnId: "any" } },
    );
    const frames = (await fake.until(isTurnCompleted)).map(frameOf);

    expect(frames.find((frame) => frame.id === 3)).toMatchObject({ result: {} });
    expect(frames.at(-1).params.turn).toMatchObject({
      id: frames.find((frame) => frame.id === 2).result.turn.id,
      status: "interrupted",
    });
    expect(frames.some((frame) => frame.method === "item/started")).toBe(false);
    fake.end();
    expect(await fake.exited).toBe(0);
    const [record] = NodeFS.readdirSync(stats).map((file) =>
      JSON.parse(NodeFS.readFileSync(NodePath.join(stats, file), "utf8")),
    );
    expect(record).toMatchObject({ fake: "codex", sessionId: threadId, aborted: true });
  });

  it.each([
    ["exit-mid-turn", "command_output_streaming", 1, 0],
    // Its turns hold only the answer: no work to loop, still a mid-turn fault.
    ["exit-mid-turn", "multi_turn", 1, 0],
    ["truncated-frame-exit", "command_output_streaming", 1, 1],
    ["truncated-frame", "command_output_streaming", null, 1],
  ])(
    "with BENCH_FAULT=%s on %s exits %s after %i cut-off lines",
    async (fault, name, code, cut) => {
      using fake = startFake({ BENCH_FAULT: fault, BENCH_CODEX_TRANSCRIPT: name });
      fake.send({ id: 1, method: "thread/start", params: { cwd: LIVE.cwd, model: LIVE.model } });
      const threadId = frameOf((await fake.until(answerTo(1))).at(-1)).result.thread.id;
      fake.send({ id: 2, method: "turn/start", params: { threadId, input: [] } });
      const lines = await fake.until(isTurnCompleted);

      expect(lines.filter((line) => frameOf(line) === undefined)).toHaveLength(cut);
      expect(lines.some(isTurnCompleted)).toBe(code === null);
      if (code !== null) expect(await fake.exited).toBe(code);
    },
  );

  it("with BENCH_FAULT=bad-shape sends one frame the schema rejects and completes the turn", async () => {
    using fake = startFake({ BENCH_FAULT: "bad-shape" });
    fake.send({ id: 1, method: "thread/start", params: { cwd: LIVE.cwd, model: LIVE.model } });
    const threadId = frameOf((await fake.until(answerTo(1))).at(-1)).result.thread.id;
    fake.send({ id: 2, method: "turn/start", params: { threadId, input: [] } });
    const frames = (await fake.until(isTurnCompleted)).map(frameOf);

    expect(frames.map(schemaIssue).filter(Boolean)).toHaveLength(1);
    expect(frames.at(-1).params.turn.status).toBe("completed");
  });

  it("steers only the turn the request expects", async () => {
    using fake = startFake({ BENCH_SPEED: "1" });
    fake.send({ id: 1, method: "thread/start", params: { cwd: LIVE.cwd, model: LIVE.model } });
    const threadId = frameOf((await fake.until(answerTo(1))).at(-1)).result.thread.id;
    fake.send({ id: 2, method: "turn/start", params: { threadId, input: [] } });
    const turnId = frameOf((await fake.until(answerTo(2))).at(-1)).result.turn.id;
    const steer = (id, expectedTurnId) => ({
      id,
      method: "turn/steer",
      params: { threadId, expectedTurnId, input: [{ type: "text", text: "Steer." }] },
    });
    fake.send(steer(3, "stale-turn"), steer(4, turnId));
    const frames = (await fake.until(answerTo(4))).map(frameOf);

    expect(frames.find((frame) => frame.id === 3)).toMatchObject({ error: { code: -32600 } });
    expect(frames.at(-1).result).toEqual({ turnId });
    expect(() => decodeSteerResponse(frames.at(-1).result)).not.toThrow();
  });

  it("offers the same seeded, weighted plan on every run, whatever the thread ids", async () => {
    const env = { BENCH_SEED: "3", BENCH_CODEX_TRANSCRIPT: "multi_turn:2,simple,web_search:1" };
    const first = await offeredPlan(env, 5, 2);

    expect(await offeredPlan(env, 5, 2)).toEqual(first);
    // The weighted cycle is multi_turn, simple, web_search, multi_turn; seed 3 starts at its end.
    expect(first.map(({ ordinal, transcript, turn }) => [ordinal, transcript, turn])).toEqual([
      [0, "multi_turn", 0],
      [0, "multi_turn", 1],
      [1, "multi_turn", 0],
      [1, "multi_turn", 1],
      [2, "simple", 0],
      [2, "simple", 0],
      [3, "web_search", 0],
      [3, "web_search", 0],
      [4, "multi_turn", 0],
      [4, "multi_turn", 1],
    ]);
    expect(first[0]).toMatchObject({ seed: 3, mix: { multi_turn: 2, simple: 1, web_search: 1 } });
  });

  it("reads the mix and rejects knobs out of range", () => {
    const read = (env) =>
      readConfig(env, {
        transcriptVar: "BENCH_CODEX_TRANSCRIPT",
        defaultMix: [{ name: "simple", weight: 1 }],
        defaultStateDir: "/state",
      });
    expect(read({ BENCH_CODEX_TRANSCRIPT: " web_search:3, simple,web_search " }).mix).toEqual([
      { name: "web_search", weight: 4 },
      { name: "simple", weight: 1 },
    ]);
    expect(read({}).mix).toEqual([{ name: "simple", weight: 1 }]);
    expect(() => read({ BENCH_CODEX_TRANSCRIPT: "simple:0" })).toThrow(/weight of simple/);
    expect(() => read({ BENCH_SEED: "-1" })).toThrow(/BENCH_SEED/);
    expect(() => read({ BENCH_TURN_LOOPS: "0" })).toThrow(/BENCH_TURN_LOOPS/);
    expect(() => read({ BENCH_SPEED: "-1" })).toThrow(/BENCH_SPEED/);
    expect(() => read({ BENCH_FAULT: "explode" })).toThrow(/BENCH_FAULT/);
  });
});

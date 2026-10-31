import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

import { describe, expect, it } from "vite-plus/test";

import {
  compileGroup,
  loadTranscriptEntries,
  mixCycle,
  parseMix,
  readConfig,
  sampleJsonSchema,
  splitTurns,
  turnLines,
} from "./claude.mjs";

const FAKE = NodeURL.fileURLToPath(new URL("./claude.mjs", import.meta.url));
const SESSION = "11111111-2222-4333-8444-555555555555";
const PROMPT = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const streamArgs = (session = `--session-id=${SESSION}`) => [
  "--output-format",
  "stream-json",
  "--verbose",
  "--input-format",
  "stream-json",
  session,
  "--model",
  "claude-sonnet-4-6",
  "--permission-mode",
  "bypassPermissions",
];
const STREAM_ARGS = streamArgs();

const groupsOf = async (name, env = {}) =>
  splitTurns(await loadTranscriptEntries(name, readConfig(env)));

const render = (group, loops) =>
  [
    ...turnLines(
      compileGroup(group),
      {
        sessionId: SESSION,
        promptUuid: PROMPT,
        cwd: "/live/project",
        model: "claude-opus-5",
        permissionMode: "default",
      },
      loops,
    ),
  ].map(({ line }) => line);

const tempDir = (prefix) => {
  const path = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), prefix));
  return { path, [Symbol.dispose]: () => NodeFS.rmSync(path, { recursive: true, force: true }) };
};

/**
 * Runs the fake over stdio, with its own ordinal state unless `env` names one; `until` reads
 * its stdout lines up to and including a match.
 */
function startFake(args, env) {
  const state = tempDir("fake-claude-state-");
  const child = NodeChildProcess.spawn(process.execPath, [FAKE, ...args], {
    env: { ...process.env, BENCH_STATE_DIR: state.path, ...env },
    stdio: ["pipe", "pipe", "ignore"],
  });
  const lines = NodeReadline.createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  const exited = new Promise((resolve) => child.once("exit", (code) => resolve(code)));
  return {
    exited,
    send: (...messages) =>
      child.stdin.write(messages.map((message) => `${JSON.stringify(message)}\n`).join("")),
    end: () => child.stdin.end(),
    until: async (match) => {
      const read = [];
      for (;;) {
        const next = await lines.next();
        if (next.done) return read;
        read.push(next.value);
        if (match(next.value)) return read;
      }
    },
    [Symbol.dispose]: () => {
      child.kill();
      state[Symbol.dispose]();
    },
  };
}

const frameOf = (line) => {
  try {
    return JSON.parse(line);
  } catch {
    return undefined;
  }
};
const isResult = (line) => frameOf(line)?.type === "result";
const isMalformed = (line) => {
  const frame = frameOf(line);
  return frame === undefined || (frame.type === "assistant" && frame.message === undefined);
};
const parseAll = (lines) => lines.map((line) => JSON.parse(line));
const readStats = (dir) =>
  NodeFS.readdirSync(dir).flatMap((file) =>
    parseAll(NodeFS.readFileSync(NodePath.join(dir, file), "utf8").trim().split("\n")),
  );

describe("fake claude transcripts", () => {
  it.each([
    // Each group: [wake turns played after it, system/init frames in it].
    [
      "claude_background_task_wake",
      [
        [1, 2],
        [0, 1],
      ],
    ],
    [
      "message_steering",
      [
        [0, 1],
        [0, 1],
      ],
    ],
    // The interrupted first process never finished its turn.
    ["turn_interrupt_restart", [[0, 1]]],
    ["simple", [[0, 1]]],
  ])("splits %s into prompted turns and their wake turns", async (name, expected) => {
    const groups = await groupsOf(name);
    expect(
      groups.map((group) => [
        group.wakes.length,
        [group.frames, ...group.wakes]
          .flat()
          .filter((frame) => frame.type === "system" && frame.subtype === "init").length,
      ]),
    ).toEqual(expected);
  });

  it("replays a turn with the live session and fresh ids on every loop", async () => {
    const [group] = await groupsOf("command_output_streaming");
    const recorded = JSON.stringify(group);
    const lines = render(group, 3);
    const frames = parseAll(lines);
    const deltas = (list) =>
      list.filter((frame) => frame.event?.type === "content_block_delta").length;

    expect(frames.filter((frame) => frame.type === "result")).toEqual([frames.at(-1)]);
    expect(frames.at(-1)).toMatchObject({ subtype: "success", user_message_uuids: [PROMPT] });
    expect(new Set(frames.map((frame) => frame.session_id))).toEqual(new Set([SESSION]));
    expect(frames.find((frame) => frame.subtype === "init")).toMatchObject({
      cwd: "/live/project",
      model: "claude-opus-5",
      permissionMode: "default",
    });
    expect(deltas(frames)).toBe(3 * deltas(group.frames));

    const messageIds = new Set(
      frames.filter((frame) => frame.type === "assistant").map((frame) => frame.message.id),
    );
    const toolUses = frames.flatMap((frame) =>
      frame.type === "assistant"
        ? frame.message.content.filter((block) => block.type === "tool_use")
        : [],
    );
    const toolResults = frames.flatMap((frame) =>
      frame.type === "user" ? frame.message.content.map((block) => block.tool_use_id) : [],
    );
    expect(messageIds.size).toBe(6);
    expect(new Set(toolUses.map((block) => block.id)).size).toBe(3);
    expect(toolResults).toEqual(toolUses.map((block) => block.id));
    for (const id of [...messageIds, ...toolResults]) expect(recorded).not.toContain(id);
  });

  it("rewrites ids split across streamed tool input like the final tool input", async () => {
    const groups = await groupsOf("claude_background_subagent_lifecycle");
    const streamed = new Map();
    const open = new Map();
    const finals = [];
    for (const frame of parseAll(groups.flatMap((group) => render(group, 2)))) {
      const event = frame.event;
      const key = `${frame.parent_tool_use_id}:${event?.index}`;
      if (event?.type === "content_block_start" && event.content_block.type === "tool_use") {
        open.set(key, event.content_block.id);
        streamed.set(event.content_block.id, "");
      }
      if (event?.delta?.type === "input_json_delta") {
        const id = open.get(key);
        streamed.set(id, streamed.get(id) + event.delta.partial_json);
      }
      if (frame.type === "assistant") {
        finals.push(...frame.message.content.filter((block) => block.type === "tool_use"));
      }
    }
    const checked = finals.filter((block) => streamed.has(block.id));

    expect(checked.length).toBeGreaterThan(0);
    // The CLI adds fields to some final inputs, so the streamed input is a subset.
    for (const block of checked) {
      const json = streamed.get(block.id);
      expect(block.input).toMatchObject(json === "" ? {} : JSON.parse(json));
    }
  });

  it("generates large tool payloads of the configured size", async () => {
    const [group] = await groupsOf("large-tool-payload", { BENCH_TOOL_PAYLOAD_BYTES: "4096" });
    const frames = parseAll(render(group, 1));
    const results = frames.flatMap((frame) =>
      frame.type === "user" ? frame.message.content.map((block) => block.content) : [],
    );
    const write = frames
      .flatMap((frame) => (frame.type === "assistant" ? frame.message.content : []))
      .find((block) => block.name === "Write");

    expect(results.map((content) => Buffer.byteLength(content) >= 4096)).toEqual([
      true,
      true,
      false,
    ]);
    expect(Buffer.byteLength(write.input.content)).toBeGreaterThanOrEqual(4096);
    expect(write.input.content).toContain("résumé");
    expect(frames.at(-1)).toMatchObject({ type: "result", user_message_uuids: [PROMPT] });
  });
});

describe("fake claude process", () => {
  it("answers the SDK's control requests and plays a turn per user message", async () => {
    using stats = tempDir("fake-claude-stats-");
    using fake = startFake(STREAM_ARGS, {
      BENCH_SPEED: "0",
      BENCH_TRANSCRIPT: "large-tool-payload",
      BENCH_TOOL_PAYLOAD_BYTES: "2048",
      BENCH_STATS_DIR: stats.path,
    });
    const control = (id, request) =>
      fake.send({ type: "control_request", request_id: id, request });
    control("init", { subtype: "initialize", hooks: {} });
    control("usage", { subtype: "get_usage", skip_behaviors: true });
    control("mcp", { subtype: "mcp_set_servers", servers: { "t3-code": { type: "http" } } });
    control("model", { subtype: "set_model", model: "claude-opus-5" });
    const responses = parseAll(await fake.until((line) => line.includes('"request_id":"model"')));
    fake.send({ type: "user", message: { role: "user", content: "hi" }, uuid: PROMPT });
    const lines = await fake.until(isResult);
    const turn = parseAll(lines);
    fake.end();
    await fake.exited;
    const [record] = readStats(stats.path);

    expect(responses.map(({ response }) => [response.request_id, response.subtype])).toEqual([
      ["init", "success"],
      ["usage", "success"],
      ["mcp", "success"],
      ["model", "success"],
    ]);
    expect(responses[0].response.response.account.email).toBeDefined();
    expect(responses[1].response.response.rate_limits_available).toBe(true);
    expect(responses[2].response.response.added).toEqual(["t3-code"]);
    expect(turn.find((frame) => frame.subtype === "init")).toMatchObject({
      session_id: SESSION,
      model: "claude-opus-5",
    });
    expect(turn.at(-1)).toMatchObject({ subtype: "success", session_id: SESSION });
    expect(record).toMatchObject({ sessionId: SESSION, transcript: "large-tool-payload" });
    expect(record.bytes).toBe(lines.reduce((sum, line) => sum + Buffer.byteLength(line) + 1, 0));
  });

  it.each([
    ["while it streams", false],
    ["in the same write as its prompt", true],
  ])("ends a turn interrupted %s with an aborted result", async (_when, together) => {
    // Paced so the turn waits at its first delta until the interrupt arrives.
    using fake = startFake(STREAM_ARGS, {
      BENCH_SPEED: "1000",
      BENCH_DELTAS_PER_SEC: "0.000001",
      BENCH_TRANSCRIPT: "command_output_streaming",
    });
    const prompt = { type: "user", message: { role: "user", content: "hi" }, uuid: PROMPT };
    const interrupt = {
      type: "control_request",
      request_id: "stop",
      request: { subtype: "interrupt" },
    };
    if (together) {
      fake.send(prompt, interrupt);
    } else {
      fake.send(prompt);
      await fake.until((line) => line.includes('"type":"content_block_start"'));
      fake.send(interrupt);
    }
    const lines = await fake.until(isResult);
    const frames = parseAll(lines);

    expect(frames.some((frame) => frame.response?.request_id === "stop")).toBe(true);
    expect(frames.filter((frame) => frame.event?.type === "content_block_delta")).toEqual([]);
    expect(frames.at(-1)).toMatchObject({
      subtype: "error_during_execution",
      is_error: true,
      user_message_uuids: [PROMPT],
    });
  });

  it.each([
    ["truncated-frame", { malformed: 1, result: true, exitCode: null }],
    ["truncated-frame-exit", { malformed: 1, result: false, exitCode: 1 }],
    ["bad-shape", { malformed: 1, result: true, exitCode: null }],
  ])("injects a %s fault into the first turn", async (fault, expected) => {
    using fake = startFake(STREAM_ARGS, {
      BENCH_SPEED: "0",
      BENCH_TRANSCRIPT: "command_output_streaming",
      BENCH_FAULT: fault,
    });
    fake.send({ type: "user", message: { role: "user", content: "hi" }, uuid: PROMPT });
    const lines = await fake.until(isResult);
    const exitCode = expected.result ? null : await fake.exited;

    expect({
      malformed: lines.filter(isMalformed).length,
      result: lines.some(isResult),
      exitCode,
    }).toEqual(expected);
  });

  it("assigns transcripts by claimed ordinal, keeps a resumed session's, and records it", async () => {
    using state = tempDir("fake-claude-mix-");
    using stats = tempDir("fake-claude-mix-stats-");
    const env = {
      BENCH_SPEED: "0",
      BENCH_TRANSCRIPT: "simple:2,multi_turn",
      BENCH_STATE_DIR: state.path,
      BENCH_STATS_DIR: stats.path,
    };
    const sessions = [
      "00000000-0000-4000-8000-00000000000a",
      "00000000-0000-4000-8000-00000000000b",
      "00000000-0000-4000-8000-00000000000c",
    ];
    for (const session of [
      ...sessions.map((id) => `--session-id=${id}`),
      `--resume=${sessions[1]}`,
    ]) {
      using fake = startFake(streamArgs(session), env);
      fake.send({ type: "user", message: { role: "user", content: "hi" }, uuid: PROMPT });
      await fake.until(isResult);
      fake.end();
      await fake.exited;
    }

    expect(
      readStats(stats.path)
        .toSorted((left, right) => left.at.localeCompare(right.at))
        .map(({ sessionId, ordinal, transcript, seed, mix }) => [
          sessionId,
          ordinal,
          transcript,
          seed,
          mix,
        ]),
    ).toEqual([
      [sessions[0], 0, "simple", 0, { simple: 2, multi_turn: 1 }],
      [sessions[1], 1, "multi_turn", 0, { simple: 2, multi_turn: 1 }],
      [sessions[2], 2, "simple", 0, { simple: 2, multi_turn: 1 }],
      [sessions[1], 1, "multi_turn", 0, { simple: 2, multi_turn: 1 }],
    ]);
  });

  it.each([
    [
      "an unknown transcript",
      () => ({ BENCH_TRANSCRIPT: "simple,no_such_transcript" }),
      "Unknown transcript no_such_transcript",
    ],
    ["a bad knob", () => ({ BENCH_SPEED: "fast" }), "BENCH_SPEED must be a number >= 0"],
    [
      "a malformed recorded frame",
      (dir) => {
        const file = NodePath.join(dir, "malformed.ndjson");
        const frames = [
          { type: "system", subtype: "init", session_id: SESSION },
          { type: "assistant", session_id: SESSION },
          { type: "result", subtype: "success", session_id: SESSION },
        ];
        NodeFS.writeFileSync(
          file,
          frames.map((frame) => `${JSON.stringify({ type: "emit_inbound", frame })}\n`).join(""),
        );
        return { BENCH_TRANSCRIPT: file };
      },
      "malformed.ndjson has an assistant frame without content on line 2",
    ],
  ])("fails a session started with %s through an error result", async (_what, envIn, cause) => {
    using dir = tempDir("fake-claude-bad-");
    using fake = startFake(STREAM_ARGS, envIn(dir.path));
    const lines = await fake.until(() => false);

    expect(await fake.exited).toBe(1);
    expect(parseAll(lines)).toEqual([
      expect.objectContaining({
        type: "result",
        is_error: true,
        session_id: SESSION,
        errors: [expect.stringContaining(cause)],
      }),
    ]);
  });

  it("answers -p with structured output that fits the requested schema", async () => {
    const schema = {
      type: "object",
      properties: { title: { type: "string", maxLength: 80 } },
      required: ["title"],
    };
    using fake = startFake(
      ["-p", "--output-format", "json", "--json-schema", JSON.stringify(schema)],
      {},
    );
    fake.send("Name this thread.");
    fake.end();
    const [output] = parseAll(await fake.until(() => true));

    expect(output).toMatchObject({ type: "result", is_error: false });
    expect(typeof output.structured_output.title).toBe("string");
  });
});

describe("fake claude configuration", () => {
  it.each([
    [{ BENCH_SPEED: "-1" }, "BENCH_SPEED"],
    [{ BENCH_DELTAS_PER_SEC: "0" }, "BENCH_DELTAS_PER_SEC"],
    [{ BENCH_TURN_LOOPS: "1.5" }, "BENCH_TURN_LOOPS"],
    [{ BENCH_TOOL_PAYLOAD_BYTES: "lots" }, "BENCH_TOOL_PAYLOAD_BYTES"],
    [{ BENCH_FAULT: "explode" }, "BENCH_FAULT"],
    [{ BENCH_SEED: "-1" }, "BENCH_SEED"],
    [{ BENCH_TRANSCRIPT: "simple:0" }, "BENCH_TRANSCRIPT"],
  ])("rejects %o", (env, name) => {
    expect(() => readConfig(env)).toThrow(name);
  });

  it("reads BENCH_TRANSCRIPT as a weighted mix spread evenly over its cycle", () => {
    expect(parseMix(" simple:2, multi_turn ,simple")).toEqual([
      { name: "simple", weight: 3 },
      { name: "multi_turn", weight: 1 },
    ]);
    expect(parseMix("")).toEqual([{ name: "command_output_streaming", weight: 1 }]);
    expect(mixCycle(parseMix("a:3,b:1"))).toEqual(["a", "a", "b", "a"]);
    expect(mixCycle(parseMix("a:2,b:2,c:1"))).toEqual(["a", "b", "c", "a", "b"]);
  });

  it("samples JSON schemas the way text generation declares them", () => {
    expect(
      sampleJsonSchema({
        $defs: { Kind: { enum: ["feat", "fix"] } },
        type: "object",
        properties: {
          kind: { $ref: "#/$defs/Kind" },
          scope: { anyOf: [{ type: "null" }, { type: "string", minLength: 8 }] },
          files: { type: "array", minItems: 2, items: { type: "integer", minimum: 1 } },
          draft: { type: "boolean" },
        },
      }),
    ).toEqual({ kind: "feat", scope: "Benchxxx", files: [1, 1], draft: false });
  });
});

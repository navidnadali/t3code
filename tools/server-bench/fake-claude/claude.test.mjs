import * as NodeChildProcess from "node:child_process";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

import { describe, expect, it } from "vite-plus/test";

import {
  compileGroup,
  loadTranscriptEntries,
  readConfig,
  sampleJsonSchema,
  splitTurns,
  turnLines,
} from "./claude.mjs";

const FAKE = NodeURL.fileURLToPath(new URL("./claude.mjs", import.meta.url));
const SESSION = "11111111-2222-4333-8444-555555555555";
const PROMPT = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const STREAM_ARGS = [
  "--output-format",
  "stream-json",
  "--verbose",
  "--input-format",
  "stream-json",
  `--session-id=${SESSION}`,
  "--model",
  "claude-sonnet-4-6",
  "--permission-mode",
  "bypassPermissions",
];

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

/** Runs the fake over stdio; `until` reads its stdout lines up to and including a match. */
function startFake(args, env) {
  const child = NodeChildProcess.spawn(process.execPath, [FAKE, ...args], {
    env: { ...process.env, ...env },
    stdio: ["pipe", "pipe", "inherit"],
  });
  const lines = NodeReadline.createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  const exited = new Promise((resolve) => child.once("exit", (code) => resolve(code)));
  return {
    exited,
    send: (message) => child.stdin.write(`${JSON.stringify(message)}\n`),
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
const isResult = (line) => frameOf(line)?.type === "result";
const isMalformed = (line) => {
  const frame = frameOf(line);
  return frame === undefined || (frame.type === "assistant" && frame.message === undefined);
};
const parseAll = (lines) => lines.map((line) => JSON.parse(line));

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
    using fake = startFake(STREAM_ARGS, { BENCH_SPEED: "0", BENCH_TRANSCRIPT: "simple" });
    const control = (id, request) =>
      fake.send({ type: "control_request", request_id: id, request });
    control("init", { subtype: "initialize", hooks: {} });
    control("usage", { subtype: "get_usage", skip_behaviors: true });
    control("mcp", { subtype: "mcp_set_servers", servers: { "t3-code": { type: "http" } } });
    control("model", { subtype: "set_model", model: "claude-opus-5" });
    const responses = parseAll(await fake.until((line) => line.includes('"request_id":"model"')));
    fake.send({ type: "user", message: { role: "user", content: "hi" }, uuid: PROMPT });
    const turn = parseAll(await fake.until(isResult));

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
  });

  it("ends an interrupted turn with an aborted result", async () => {
    // The turn waits at its first delta until the interrupt arrives.
    using fake = startFake(STREAM_ARGS, {
      BENCH_SPEED: "1000",
      BENCH_DELTAS_PER_SEC: "0.000001",
      BENCH_TRANSCRIPT: "command_output_streaming",
    });
    fake.send({ type: "user", message: { role: "user", content: "hi" }, uuid: PROMPT });
    await fake.until((line) => line.includes('"type":"content_block_start"'));
    fake.send({ type: "control_request", request_id: "stop", request: { subtype: "interrupt" } });
    const tail = parseAll(await fake.until(isResult));

    expect(tail.some((frame) => frame.response?.request_id === "stop")).toBe(true);
    expect(tail.at(-1)).toMatchObject({
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
  ])("rejects %o", (env, name) => {
    expect(() => readConfig(env)).toThrow(name);
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

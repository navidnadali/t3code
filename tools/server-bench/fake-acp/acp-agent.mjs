#!/usr/bin/env node
// A replaying ACP agent for server benchmarks. It answers the ACP requests an acpRegistry
// provider instance sends and replays recorded prompt turns of the ACP registry fixtures, parsed
// by the dev replay agent, with live ids and bench pacing. `--help` lists its knobs.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeReadline from "node:readline";

import { parseReplayTurns, streamChunks } from "../../../apps/server/scripts/dev-replay-agent.ts";
import {
  compileFrames,
  delayMs,
  hashOf,
  injectExitFault,
  KNOBS_HELP,
  makeStatsSink,
  makeWriter,
  nextTick,
  readConfig,
  render,
  sleep,
  transcriptPath,
} from "../fake-common/replay.mjs";

export const TRANSCRIPT_FILE = "registry_transcript.ndjson";
export const BENCH_MODEL = "bench-replay";
const DEFAULT_TRANSCRIPTS = ["todo_list"];
// As the dev replay agent: frames without recorded timing wait this long, recorded waits are cut.
const FRAME_DELAY_MS = 250;
const MAX_RECORDED_DELAY_MS = 2_000;
const FRAMES_PER_YIELD = 64;

const HELP = `Replaying ACP agent for T3 Code server benchmarks: no model, no network.
Point an acpRegistry provider instance (source "local") at the \`acp-agent\` launcher beside this file.

Each session/prompt replays the next recorded prompt turn (the frames the dev replay agent
replays: message, thought, tool, plan and usage updates), with the live session id and cwd and
fresh tool call ids; a session starts at a turn its id picks. Text streams in small chunks.

  BENCH_ACP_TRANSCRIPT      Comma-separated transcripts (default ${DEFAULT_TRANSCRIPTS.join(",")}): a
                            fixture under apps/server/src/orchestration-v2/testkit/fixtures/ with
                            a ${TRANSCRIPT_FILE}, or an absolute .ndjson path.
${KNOBS_HELP}

Recorded permission and elicitation requests are sent and wait for the client's answer, so bench
transcripts should not hold any.
`;

const SESSION = 0;
const CWD = 1;
const FIRST_ID = 2;

/** The session id the transcript's session/new answered; frames name it. */
function recordedSessionId(text) {
  for (const line of text.split("\n")) {
    if (line.trim().length === 0) continue;
    const entry = JSON.parse(line);
    const frame = entry.frame;
    if (entry.type === "emit_inbound" && frame?.kind === "response") {
      const id = frame.method === "session/new" ? frame.result?.sessionId : undefined;
      if (typeof id === "string") return id;
    }
  }
  return undefined;
}

export const statKey = (message) =>
  message.method === "session/update"
    ? `session/update:${message.params?.update?.sessionUpdate}`
    : `request:${message.method}`;

/** One recorded turn as the frames it sends, text split into the chunks it streams. */
export function expandTurn(turn) {
  const frames = [];
  for (const { frame, afterMs } of turn.frames) {
    const update = frame.params?.update;
    const content = update?.content;
    const text =
      (update?.sessionUpdate === "agent_message_chunk" ||
        update?.sessionUpdate === "agent_thought_chunk") &&
      content?.type === "text"
        ? content.text
        : undefined;
    const message = (params) =>
      frame.kind === "request"
        ? { jsonrpc: "2.0", id: 0, method: frame.method, params }
        : { jsonrpc: "2.0", method: frame.method, params };
    const firstPace = { ms: Math.min(afterMs ?? FRAME_DELAY_MS, MAX_RECORDED_DELAY_MS) };
    if (text === undefined) {
      frames.push({ message: message(frame.params), pace: firstPace });
      continue;
    }
    for (const [index, chunk] of streamChunks(text).entries()) {
      frames.push({
        message: message({
          ...frame.params,
          update: { ...update, content: { ...content, text: chunk } },
        }),
        pace: index === 0 && afterMs !== undefined ? firstPace : { delta: true },
      });
    }
  }
  return frames;
}

/** Compiles one transcript's prompt turns; tool call ids become slots fresh on every loop. */
export function compileTranscript(text) {
  const sessionId = recordedSessionId(text);
  const turns = parseReplayTurns(text).filter((turn) => turn.frames.length > 0);
  return turns.map((turn) => {
    const expanded = expandTurn(turn);
    const toolSlots = new Map();
    const keySlots = (key, value) => {
      if (key !== "toolCallId") return undefined;
      if (!toolSlots.has(value)) toolSlots.set(value, FIRST_ID + toolSlots.size);
      return toolSlots.get(value);
    };
    const { templates } = compileFrames(
      expanded.map((frame) => frame.message),
      {
        fixed: new Map([
          [sessionId, SESSION],
          ["<workspace>", CWD],
        ]),
        firstId: FIRST_ID,
        keySlots,
      },
    );
    return {
      stopReason: turn.stopReason,
      messages: expanded.map((frame) => frame.message),
      frames: expanded.map((frame, index) => ({
        ...templates[index],
        pace: frame.pace,
        stat: statKey(frame.message),
        request: frame.message.id !== undefined,
      })),
      toolIds: [...toolSlots.keys()],
    };
  });
}

/** Every frame one prompt plays, the recorded turn `loops` times with fresh tool call ids. */
export function* turnLines(compiled, live, loops) {
  const values = [live.sessionId, live.cwd, ...compiled.toolIds];
  const faultIndex = Math.floor(compiled.frames.length / 2);
  for (let loop = 0; loop < loops; loop++) {
    const suffix = NodeCrypto.randomBytes(4).toString("hex");
    compiled.toolIds.forEach((id, index) => (values[FIRST_ID + index] = `${id}-${suffix}`));
    for (const [index, frame] of compiled.frames.entries()) {
      yield { frame, line: render(frame, values), faultPoint: loop === 0 && index === faultIndex };
    }
  }
}

export function loadTranscripts(config) {
  return config.transcripts.map((name) => {
    const turns = compileTranscript(
      NodeFS.readFileSync(transcriptPath(name, TRANSCRIPT_FILE), "utf8"),
    );
    if (turns.length === 0) throw new Error(`Transcript ${name} has no prompt turn with frames.`);
    return { name, turns };
  });
}

const ITEM_UPDATES = { agent_message_chunk: "assistant_message", agent_thought_chunk: "reasoning" };

/**
 * What one played prompt sends and leaves behind: frames by kind, then message and thought items
 * (a run of chunks is one item), tool calls and turns with a plan, and the final answer.
 */
export function expectedTurn(name, index, loops) {
  const text = NodeFS.readFileSync(transcriptPath(name, TRANSCRIPT_FILE), "utf8");
  const compiled = compileTranscript(text)[index];
  if (compiled === undefined) throw new Error(`Transcript ${name} has no prompt turn ${index}.`);
  const frames = {};
  for (const frame of compiled.frames) frames[frame.stat] = (frames[frame.stat] ?? 0) + loops;
  const items = { assistant_message: 0, reasoning: 0 };
  let open;
  let reply = null;
  let plans = 0;
  for (let loop = 0; loop < loops; loop++) {
    for (const message of compiled.messages) {
      const update = message.params?.update;
      const kind = ITEM_UPDATES[update?.sessionUpdate];
      if (update?.sessionUpdate === "plan") plans = 1;
      if (kind === undefined) {
        if (update?.sessionUpdate !== "usage_update") open = undefined;
        continue;
      }
      if (open !== kind) items[kind] += 1;
      if (kind === "assistant_message")
        reply = open === kind ? reply + update.content.text : update.content.text;
      open = kind;
    }
  }
  return {
    frames,
    items: { ...items, tool: compiled.toolIds.length * loops },
    todoLists: plans,
    reply,
  };
}

const BAD_SHAPE = (sessionId) =>
  JSON.stringify({
    jsonrpc: "2.0",
    method: "session/update",
    params: { sessionId, update: { sessionUpdate: "agent_message_chunk" } },
  });

async function runAgent(config) {
  const write = makeWriter(process.stdout);
  const recordStats = makeStatsSink(config.statsDir, "fake-acp");
  const transcripts = loadTranscripts(config);
  const turnCount = transcripts.reduce((sum, entry) => sum + entry.turns.length, 0);
  const requests = {};
  const sessions = new Map();
  const pendingAgentRequests = new Map();
  let nextAgentRequestId = 1;
  let faultPending = config.fault !== undefined;

  const send = (message) => write(JSON.stringify({ jsonrpc: "2.0", ...message }));

  const pickTurn = (session) => {
    let index = (hashOf(session.sessionId) + session.played++) % turnCount;
    for (const { name, turns } of transcripts) {
      if (index < turns.length) return { name, turn: index, compiled: turns[index] };
      index -= turns.length;
    }
    throw new Error("unreachable");
  };

  const play = async (session, controller) => {
    const startedAt = Date.now();
    const pick = pickTurn(session);
    const frames = {};
    let bytes = 0;
    let unpaced = 0;
    for (const { frame, line, faultPoint } of turnLines(pick.compiled, session, config.turnLoops)) {
      const wait = delayMs(frame.pace, config);
      if (wait > 0) {
        unpaced = 0;
        await sleep(wait, controller.signal);
      } else if (++unpaced >= FRAMES_PER_YIELD) {
        unpaced = 0;
        await nextTick();
      }
      if (controller.signal.aborted) break;
      if (faultPoint && faultPending) {
        faultPending = false;
        if (config.fault === "bad-shape") await write(BAD_SHAPE(session.sessionId));
        else if (config.fault === "truncated-frame") {
          await write(line.slice(0, Math.max(1, Math.floor(line.length / 2))));
        } else await injectExitFault(config.fault, line, write);
      }
      if (frame.request) {
        const id = nextAgentRequestId++;
        const answered = new Promise((resolve) => pendingAgentRequests.set(String(id), resolve));
        await send({ ...JSON.parse(line), id });
        await Promise.race([
          answered,
          new Promise((resolve) => controller.signal.addEventListener("abort", resolve)),
        ]);
        pendingAgentRequests.delete(String(id));
      } else {
        await write(line);
      }
      frames[frame.stat] = (frames[frame.stat] ?? 0) + 1;
      bytes += Buffer.byteLength(line) + 1;
    }
    const aborted = controller.signal.aborted;
    recordStats({
      at: new Date().toISOString(),
      pid: process.pid,
      fake: "acp",
      sessionId: session.sessionId,
      transcript: pick.name,
      turn: pick.turn,
      loops: config.turnLoops,
      aborted,
      frames,
      bytes,
      ms: Date.now() - startedAt,
      rssBytes: process.memoryUsage.rss(),
      requests,
    });
    return { stopReason: aborted ? "cancelled" : pick.compiled.stopReason };
  };

  const cancel = (sessionId) => {
    for (const controller of sessions.get(sessionId)?.prompts ?? []) controller.abort();
  };

  const models = (current) => ({
    currentModelId: current,
    availableModels: [{ modelId: BENCH_MODEL, name: "Bench replay" }],
  });

  const handleRequest = async (message) => {
    const params = message.params ?? {};
    switch (message.method) {
      case "initialize":
        return {
          protocolVersion: 1,
          agentCapabilities: {
            loadSession: true,
            promptCapabilities: { image: false, audio: false, embeddedContext: true },
          },
          authMethods: [],
          agentInfo: { name: "t3-bench-replay", version: "1" },
        };
      case "session/new":
      case "session/load": {
        const sessionId =
          message.method === "session/load" ? String(params.sessionId) : NodeCrypto.randomUUID();
        const cwd = typeof params.cwd === "string" ? params.cwd : process.cwd();
        sessions.set(sessionId, {
          sessionId,
          cwd,
          model: BENCH_MODEL,
          played: 0,
          prompts: new Set(),
        });
        return {
          ...(message.method === "session/new" ? { sessionId } : {}),
          models: models(BENCH_MODEL),
        };
      }
      case "session/set_model": {
        const session = sessions.get(String(params.sessionId));
        if (session !== undefined) session.model = String(params.modelId);
        return {};
      }
      case "session/close":
        cancel(String(params.sessionId));
        sessions.delete(String(params.sessionId));
        return {};
      case "session/prompt":
        throw new Error(`Unknown session ${params.sessionId}`);
      case "authenticate":
      case "session/set_mode":
      case "session/set_config_option":
        return {};
      default:
        throw Object.assign(new Error(`Method not found: ${message.method}`), { code: -32601 });
    }
  };

  const input = NodeReadline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  input.on("line", (line) => {
    if (line.trim().length === 0) return;
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      return;
    }
    if (message.method === undefined) {
      pendingAgentRequests.get(String(message.id))?.(message);
      return;
    }
    requests[message.method] = (requests[message.method] ?? 0) + 1;
    if (message.id === undefined || message.id === null) {
      if (message.method === "session/cancel") cancel(String(message.params?.sessionId));
      return;
    }
    const id = message.id;
    const answer = (work) =>
      work.then(
        (result) => send({ id, result }),
        (error) => send({ id, error: { code: error.code ?? -32603, message: error.message } }),
      );
    const session = sessions.get(String(message.params?.sessionId));
    if (message.method !== "session/prompt" || session === undefined) {
      void answer(handleRequest(message));
      return;
    }
    // Prompts on one session play in order, each answered before the next starts;
    // session/cancel stops every prompt sent before it.
    const controller = new AbortController();
    session.prompts.add(controller);
    session.turn = (session.turn ?? Promise.resolve())
      .then(() => answer(play(session, controller)))
      .finally(() => session.prompts.delete(controller));
  });
  input.on("close", () => process.exit(0));
}

async function main(argv, env) {
  if (argv.includes("--help") || argv.includes("-h")) {
    process.stdout.write(HELP);
    return;
  }
  await runAgent(
    readConfig(env, {
      transcriptVar: "BENCH_ACP_TRANSCRIPT",
      defaultTranscripts: DEFAULT_TRANSCRIPTS,
    }),
  );
}

if (import.meta.main) {
  main(process.argv.slice(2), process.env).catch((error) => {
    process.stderr.write(`fake acp agent: ${error.message}\n`);
    process.exit(2);
  });
}

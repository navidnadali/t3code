#!/usr/bin/env node
// A fake Claude Code CLI for server benchmarks. It answers the Agent SDK's stream-json control
// protocol and replays recorded turns, so the real SDK, adapter and orchestration run with no
// model and no network. `claude.mjs --help` lists its knobs.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

// Newer than every Claude model's minimum version in the bundled catalog.
export const FAKE_CLAUDE_VERSION = "2.1.300";

const FIXTURES_DIR = NodeURL.fileURLToPath(
  new URL("../../../apps/server/src/orchestration-v2/testkit/fixtures/", import.meta.url),
);
const TRANSCRIPTS_DIR = NodeURL.fileURLToPath(new URL("./transcripts/", import.meta.url));
const DEFAULT_MIX = [{ name: "command_output_streaming", weight: 1 }];
// The assignment cycle is materialised, so weights stay small.
const MAX_MIX_WEIGHT = 10_000;

// Waits at BENCH_SPEED=1. Recorded time-to-first-token is kept up to the cap.
const DEFAULT_FIRST_TOKEN_MS = 800;
const MAX_FIRST_TOKEN_MS = 3_000;
const TOOL_RESULT_MS = 250;
const UNSTREAMED_MESSAGE_MS = 500;
// Unpaced replay still yields this often, so stdin control requests such as interrupt run.
const FRAMES_PER_YIELD = 64;

export const FAULTS = ["truncated-frame", "truncated-frame-exit", "bad-shape"];

const README = new URL("./README.md", import.meta.url);

export function readConfig(env) {
  const number = (name, fallback, valid, expected) => {
    const raw = env[name]?.trim();
    if (raw === undefined || raw === "") return fallback;
    const value = Number(raw);
    if (!valid(value)) throw new Error(`${name} must be ${expected}, not ${JSON.stringify(raw)}.`);
    return value;
  };
  const count = (value) => Number.isSafeInteger(value) && value >= 1;
  const fault = env.BENCH_FAULT?.trim() || undefined;
  if (fault !== undefined && !FAULTS.includes(fault)) {
    throw new Error(
      `BENCH_FAULT must be one of ${FAULTS.join(", ")}, not ${JSON.stringify(fault)}.`,
    );
  }
  const configDir = env.CLAUDE_CONFIG_DIR?.trim();
  return {
    speed: number("BENCH_SPEED", 1, (value) => value >= 0 && value < Infinity, "a number >= 0"),
    deltasPerSec: number(
      "BENCH_DELTAS_PER_SEC",
      40,
      (value) => value > 0 && value < Infinity,
      "a number > 0",
    ),
    turnLoops: number("BENCH_TURN_LOOPS", 1, count, "an integer >= 1"),
    payloadBytes: number("BENCH_TOOL_PAYLOAD_BYTES", 262_144, count, "an integer >= 1"),
    mix: parseMix(env.BENCH_TRANSCRIPT),
    seed: number(
      "BENCH_SEED",
      0,
      (value) => Number.isSafeInteger(value) && value >= 0,
      "an integer >= 0",
    ),
    stateDir:
      env.BENCH_STATE_DIR?.trim() ||
      (configDir
        ? NodePath.join(configDir, "fake-claude")
        : NodePath.join(NodeOS.tmpdir(), "t3-fake-claude")),
    statsDir: env.BENCH_STATS_DIR?.trim() || undefined,
    mcpCredentialsDir: env.BENCH_MCP_CREDENTIALS_DIR?.trim() || undefined,
    fault,
  };
}

/** BENCH_TRANSCRIPT: comma-separated `name` or `name:weight` entries; a weight defaults to 1. */
export function parseMix(value) {
  const weights = new Map();
  for (const entry of (value ?? "").split(",")) {
    if (entry.trim() === "") continue;
    const weighted = /^(.+):(\d+)$/.exec(entry.trim());
    const name = (weighted?.[1] ?? entry).trim();
    const weight = weighted === null ? 1 : Number(weighted[2]);
    if (weight < 1) throw new Error(`BENCH_TRANSCRIPT weight of ${name} must be at least 1.`);
    weights.set(name, (weights.get(name) ?? 0) + weight);
  }
  const mix = [...weights].map(([name, weight]) => ({ name, weight }));
  if (mix.reduce((sum, { weight }) => sum + weight, 0) > MAX_MIX_WEIGHT) {
    throw new Error(`BENCH_TRANSCRIPT weights must add up to at most ${MAX_MIX_WEIGHT}.`);
  }
  return mix.length > 0 ? mix : DEFAULT_MIX;
}

/**
 * The mix as one assignment cycle, spread by smooth weighted round-robin: the cycle holds each
 * transcript `weight` times, and each of its prefixes is within one of its weighted share.
 */
export function mixCycle(mix) {
  const total = mix.reduce((sum, { weight }) => sum + weight, 0);
  const current = mix.map(() => 0);
  const cycle = [];
  for (let step = 0; step < total; step++) {
    let best = 0;
    mix.forEach(({ weight }, index) => {
      current[index] += weight;
      if (current[index] > current[best]) best = index;
    });
    current[best] -= total;
    cycle.push(mix[best].name);
  }
  return cycle;
}

/**
 * The session's ordinal: the next free one in `stateDir`, claimed with an exclusive create so
 * concurrent fakes never share one. A resumed session keeps the ordinal it claimed first.
 */
export function claimOrdinal(stateDir, sessionId) {
  NodeFS.mkdirSync(stateDir, { recursive: true });
  const sessionFile = NodePath.join(stateDir, `session-${sessionId.replace(/[^\w-]/g, "_")}`);
  if (NodeFS.existsSync(sessionFile)) return Number(NodeFS.readFileSync(sessionFile, "utf8"));
  const claimed = NodeFS.readdirSync(stateDir).filter((file) => file.startsWith("ordinal-"));
  for (let ordinal = claimed.length; ; ordinal++) {
    try {
      NodeFS.writeFileSync(NodePath.join(stateDir, `ordinal-${ordinal}`), sessionId, {
        flag: "wx",
      });
    } catch (error) {
      if (error.code === "EEXIST") continue;
      throw error;
    }
    NodeFS.writeFileSync(sessionFile, String(ordinal));
    return ordinal;
  }
}

// Flags the SDK passes with a separate value; any other flag reads as a boolean.
const VALUE_FLAGS = new Set([
  "--add-dir",
  "--agent",
  "--allowedTools",
  "--append-system-prompt",
  "--betas",
  "--debug-file",
  "--disallowedTools",
  "--effort",
  "--fallback-model",
  "--input-format",
  "--json-schema",
  "--max-budget-usd",
  "--max-thinking-tokens",
  "--max-turns",
  "--mcp-config",
  "--model",
  "--output-format",
  "--permission-mode",
  "--permission-prompt-tool",
  "--resume",
  "--session-id",
  "--setting-sources",
  "--settings",
  "--thinking",
  "--thinking-display",
  "--tools",
]);

export function parseArgs(argv) {
  const flags = new Map();
  for (let index = 0; index < argv.length; index++) {
    const arg = argv[index];
    if (!arg.startsWith("-")) continue;
    const equals = arg.indexOf("=");
    if (equals > 0) flags.set(arg.slice(0, equals), arg.slice(equals + 1));
    else if (VALUE_FLAGS.has(arg) && index + 1 < argv.length) flags.set(arg, argv[++index]);
    else flags.set(arg, true);
  }
  return flags;
}

const stringFlag = (flags, name) => {
  const value = flags.get(name);
  return typeof value === "string" && value.length > 0 ? value : undefined;
};

/** The session id the real CLI would report: --session-id, the resumed id, or a new one. */
export function sessionIdFor(flags) {
  const resumed = stringFlag(flags, "--resume");
  if (resumed !== undefined && flags.get("--fork-session") !== true) return resumed;
  return stringFlag(flags, "--session-id") ?? NodeCrypto.randomUUID();
}

const parseNdjson = (text) =>
  text
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line));

export async function loadTranscriptEntries(name, config) {
  if (NodePath.isAbsolute(name)) return parseNdjson(NodeFS.readFileSync(name, "utf8"));
  if (!/^[\w.-]+$/.test(name) || name.startsWith(".")) {
    throw new Error(`Transcript ${JSON.stringify(name)} is neither a plain name nor absolute.`);
  }
  const generator = NodePath.join(TRANSCRIPTS_DIR, `${name}.mjs`);
  if (NodeFS.existsSync(generator)) {
    const module = await import(NodeURL.pathToFileURL(generator).href);
    return module.makeTranscript(config);
  }
  for (const file of [
    NodePath.join(TRANSCRIPTS_DIR, `${name}.ndjson`),
    NodePath.join(FIXTURES_DIR, name, "claude_transcript.ndjson"),
  ]) {
    if (NodeFS.existsSync(file)) return parseNdjson(NodeFS.readFileSync(file, "utf8"));
  }
  throw new Error(`Unknown transcript ${name}: not in transcripts/ or the replay fixtures.`);
}

const isInit = (frame) => frame.type === "system" && frame.subtype === "init";

/**
 * Splits recorded CLI output into turns that end at each result. A result with a non-human
 * origin closes a wake turn, which plays right after the prompted turn before it. A process
 * that exited mid-turn (interrupt fixtures) leaves frames with no result; those are dropped.
 */
export function splitTurns(entries) {
  const turns = [];
  let pending = [];
  const endProcess = () => {
    if (!pending.some(isInit)) turns.at(-1)?.frames.push(...pending);
    pending = [];
  };
  for (const entry of entries) {
    if (entry.type === "runtime_exit") endProcess();
    const frame = entry.frame;
    // Dotted types such as permission.request are recorder bookkeeping, not CLI output.
    if (entry.type !== "emit_inbound" || typeof frame?.type !== "string") continue;
    if (frame.type.includes(".")) continue;
    pending.push(frame);
    if (frame.type !== "result") continue;
    const origin = frame.origin?.kind;
    turns.push({ frames: pending, wake: origin !== undefined && origin !== "human" });
    pending = [];
  }
  endProcess();
  const groups = [];
  for (const turn of turns) {
    if (turn.wake) groups.at(-1)?.wakes.push(turn.frames);
    else groups.push({ frames: turn.frames, wakes: [] });
  }
  return groups;
}

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
const EXACT_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const API_ID = /\b(?:msg|toolu|srvtoolu|req)_[A-Za-z0-9]{8,}/g;
// Task and agent ids also appear inside output paths and tool result text.
const ID_KEYS = new Set(["task_id", "taskId", "agent_id", "agentId"]);
const TOKEN_OPEN = "\uE000";
const TOKEN_CLOSE = "\uE001";
const TOKEN_SPLIT = /\uE000(\d+)\uE001/;
const SESSION = 0;
const PROMPT = 1;
const CWD = 2;
const MODEL = 3;
const PERMISSION_MODE = 4;
const FIRST_ID = 5;

const token = (index) => `${TOKEN_OPEN}${index}${TOKEN_CLOSE}`;
const jsonText = (value) => JSON.stringify(String(value)).slice(1, -1);
const escapeRegExp = (text) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** Calls `visit` with every string in `value`, object keys included, keyed by its property. */
function visitStrings(value, visit, key) {
  if (typeof value === "string") visit(key, value);
  else if (Array.isArray(value)) for (const entry of value) visitStrings(entry, visit, key);
  else if (typeof value === "object" && value !== null) {
    for (const [entryKey, entry] of Object.entries(value)) {
      visit(undefined, entryKey);
      visitStrings(entry, visit, entryKey);
    }
  }
}

/** A copy of `value` with `replace` applied to every string, object keys included. */
function rewriteStrings(value, replace) {
  if (typeof value === "string") return replace(value);
  if (Array.isArray(value)) return value.map((entry) => rewriteStrings(entry, replace));
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [replace(key), rewriteStrings(entry, replace)]),
  );
}

function statKey(frame) {
  if (frame.type === "stream_event") return `stream_event:${frame.event?.type}`;
  if (frame.type === "system" || frame.type === "result") return `${frame.type}:${frame.subtype}`;
  return frame.type;
}

function paceOf(frame, streamed) {
  if (frame.type === "stream_event") {
    if (frame.event?.type === "content_block_delta") return { delta: true };
    if (frame.event?.type !== "message_start") return null;
    const recorded = typeof frame.ttft_ms === "number" ? frame.ttft_ms : DEFAULT_FIRST_TOKEN_MS;
    return { ms: Math.min(Math.max(recorded, 0), MAX_FIRST_TOKEN_MS) };
  }
  const content = frame.type === "user" ? frame.message?.content : undefined;
  if (Array.isArray(content) && content.some((block) => block?.type === "tool_result")) {
    return { ms: TOOL_RESULT_MS };
  }
  return frame.type === "assistant" && !streamed ? { ms: UNSTREAMED_MESSAGE_MS } : null;
}

/** Live values the frames of the root agent take: the session's model and permission mode. */
function markLiveFields(frame) {
  if (isInit(frame)) {
    frame.claude_code_version = FAKE_CLAUDE_VERSION;
    frame.model = token(MODEL);
  }
  if (frame.type === "system" && typeof frame.permissionMode === "string") {
    frame.permissionMode = token(PERMISSION_MODE);
  }
  if ((frame.parent_tool_use_id ?? null) !== null) return;
  const message = frame.type === "assistant" ? frame.message : frame.event?.message;
  if (typeof message?.model === "string") message.model = token(MODEL);
}

const DELTA_TEXT_FIELDS = {
  input_json_delta: "partial_json",
  text_delta: "text",
  thinking_delta: "thinking",
};

/**
 * Copies `frames` with each content block's delta boundaries moved off `pattern` matches, so a
 * recorded id split across streamed chunks is rewritten whole, as in the final message.
 */
function alignDeltas(frames, pattern) {
  const open = new Map();
  const runs = [];
  frames.forEach((frame, index) => {
    const event = frame.type === "stream_event" ? frame.event : undefined;
    const key = `${frame.parent_tool_use_id ?? ""}:${event?.index}`;
    if (event?.type === "content_block_start") {
      const run = [];
      open.set(key, run);
      runs.push(run);
    }
    const field = DELTA_TEXT_FIELDS[event?.delta?.type];
    if (event?.type === "content_block_delta" && typeof event.delta[field] === "string") {
      open.get(key)?.push({ index, field });
    }
  });
  const aligned = [...frames];
  for (const run of runs) {
    const pieces = run.map(({ index, field }) => frames[index].event.delta[field]);
    const text = pieces.join("");
    let offset = 0;
    const ends = pieces.map((piece) => (offset += piece.length));
    for (const match of text.matchAll(pattern)) {
      const end = match.index + match[0].length;
      for (let at = 0; at < ends.length; at++) {
        if (ends[at] > match.index && ends[at] < end) ends[at] = end;
      }
    }
    run.forEach(({ index, field }, at) => {
      const frame = structuredClone(frames[index]);
      frame.event.delta[field] = text.slice(at === 0 ? 0 : ends[at - 1], ends[at]);
      aligned[index] = frame;
    });
  }
  return aligned;
}

/**
 * Compiles a prompted turn and its wake turns into line templates. Recorded ids become slots:
 * the session id, the prompt the result echoes and the recorded cwd take live values, ids seen
 * only in the loop body change on every loop, and the rest change once per play.
 */
export function compileGroup(group) {
  const frames = group.frames;
  const wakes = group.wakes;
  const all = [...frames, ...wakes.flat()];
  const resultIndex = frames.findIndex((frame) => frame.type === "result");
  const bodyStart = frames.findIndex(isInit) + 1;
  const result = frames[resultIndex];
  const echoed = Array.isArray(result.user_message_uuids)
    ? result.user_message_uuids[0]
    : result.user_message_uuid;
  const init = all.find(isInit);
  if (JSON.stringify(all).includes(TOKEN_OPEN)) {
    throw new Error("A recorded frame contains the U+E000 character the templates reserve.");
  }

  const slots = new Map();
  const ids = [];
  const addId = (id) => {
    if (slots.has(id)) return;
    slots.set(id, FIRST_ID + ids.length);
    ids.push(id);
  };
  for (const frame of all) {
    visitStrings(frame, (key, value) => {
      if (key === "session_id") slots.set(value, SESSION);
      if (ID_KEYS.has(key) && value.length >= 6) addId(value);
      for (const match of value.matchAll(UUID)) addId(match[0]);
      for (const match of value.matchAll(API_ID)) addId(match[0]);
    });
  }
  if (typeof echoed === "string") slots.set(echoed, PROMPT);
  if (typeof init?.cwd === "string" && init.cwd.length > 1) slots.set(init.cwd, CWD);

  const pattern =
    slots.size === 0
      ? undefined
      : new RegExp(
          [...slots.keys()]
            .toSorted((left, right) => right.length - left.length)
            .map(escapeRegExp)
            .join("|"),
          "g",
        );
  const inBody = new Set();
  const outsideBody = new Set();
  const streamed = (turnFrames) => turnFrames.some((frame) => frame.type === "stream_event");
  const compile = (recorded, isBody, turnStreamed) => {
    const seen = isBody ? inBody : outsideBody;
    const frame =
      pattern === undefined
        ? structuredClone(recorded)
        : rewriteStrings(recorded, (value) =>
            value.replace(pattern, (match) => {
              const slot = slots.get(match);
              seen.add(slot);
              return token(slot);
            }),
          );
    markLiveFields(frame);
    const parts = JSON.stringify(frame).split(TOKEN_SPLIT);
    for (let index = 1; index < parts.length; index += 2) parts[index] = Number(parts[index]);
    return { parts, pace: paceOf(frame, turnStreamed), stat: statKey(frame) };
  };
  const align = (turn) => (pattern === undefined ? turn : alignDeltas(turn, pattern));
  const promptStreamed = streamed(frames);
  const compiled = align(frames).map((frame, index) =>
    compile(frame, index >= bodyStart && index < resultIndex, promptStreamed),
  );
  const compiledWakes = wakes.flatMap((turn) => {
    const turnStreamed = streamed(turn);
    return align(turn).map((frame) => compile(frame, false, turnStreamed));
  });
  const idSlots = ids.map((id) => slots.get(id)).filter((slot) => slot >= FIRST_ID);
  return {
    prefix: compiled.slice(0, bodyStart),
    body: compiled.slice(bodyStart, resultIndex),
    suffix: [...compiled.slice(resultIndex), ...compiledWakes],
    originals: ["", "", "", "", "", ...ids],
    loopSlots: idSlots.filter((slot) => inBody.has(slot) && !outsideBody.has(slot)),
    playSlots: idSlots.filter((slot) => !inBody.has(slot) || outsideBody.has(slot)),
    recordedModel: typeof init?.model === "string" ? init.model : "claude-sonnet-4-6",
  };
}

const randomOf = (alphabet) => alphabet[Math.floor(Math.random() * alphabet.length)];
const LOWER = "abcdefghijklmnopqrstuvwxyz";
const UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const DIGITS = "0123456789";
const HEX = "0123456789abcdef";

/** A new id shaped like the recorded one: same prefix, length and character classes. */
export function freshId(original) {
  if (EXACT_UUID.test(original)) return NodeCrypto.randomUUID();
  const underscore = original.indexOf("_");
  const kept = underscore >= 0 ? underscore + 1 : 1;
  const rest = original.slice(kept);
  const hex = /^[0-9a-f]+$/.test(rest);
  let id = original.slice(0, kept);
  for (const char of rest) {
    if (hex) id += randomOf(HEX);
    else if (DIGITS.includes(char)) id += randomOf(DIGITS);
    else if (LOWER.includes(char)) id += randomOf(LOWER);
    else if (UPPER.includes(char)) id += randomOf(UPPER);
    else id += char;
  }
  return id;
}

function render(frame, values) {
  let line = frame.parts[0];
  for (let index = 1; index < frame.parts.length; index += 2) {
    line += values[frame.parts[index]] + frame.parts[index + 1];
  }
  return line;
}

/**
 * The lines one prompt plays: prefix, the body `loops` times, then the result and wake turns.
 * `live` is read at every loop, so set_model and set_permission_mode apply mid-turn.
 */
export function* turnLines(compiled, live, loops) {
  const values = [...compiled.originals];
  values[SESSION] = jsonText(live.sessionId);
  values[PROMPT] = jsonText(live.promptUuid ?? NodeCrypto.randomUUID());
  values[CWD] = jsonText(live.cwd);
  const refreshLive = () => {
    values[MODEL] = jsonText(live.model ?? compiled.recordedModel);
    values[PERMISSION_MODE] = jsonText(live.permissionMode);
  };
  refreshLive();
  for (const slot of compiled.playSlots) values[slot] = freshId(compiled.originals[slot]);
  for (const frame of compiled.prefix) yield { frame, line: render(frame, values) };
  const faultIndex = Math.floor(compiled.body.length / 2);
  for (let loop = 0; loop < (compiled.body.length === 0 ? 0 : loops); loop++) {
    refreshLive();
    for (const slot of compiled.loopSlots) values[slot] = freshId(compiled.originals[slot]);
    for (let index = 0; index < compiled.body.length; index++) {
      const frame = compiled.body[index];
      yield { frame, line: render(frame, values), faultPoint: loop === 0 && index === faultIndex };
    }
  }
  refreshLive();
  for (const frame of compiled.suffix) yield { frame, line: render(frame, values) };
}

export function delayMs(pace, config) {
  if (pace === null || config.speed === 0) return 0;
  return (pace.delta ? 1_000 / config.deltasPerSec : pace.ms) / config.speed;
}

const sleep = (ms, signal) =>
  new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done, { once: true });
  });

function abortedResult(live, startedAt) {
  return {
    type: "result",
    subtype: "error_during_execution",
    duration_ms: Date.now() - startedAt,
    duration_api_ms: 0,
    is_error: true,
    num_turns: 1,
    stop_reason: null,
    session_id: live.sessionId,
    total_cost_usd: 0,
    usage: { input_tokens: 0, output_tokens: 0 },
    modelUsage: {},
    permission_denials: [],
    terminal_reason: "aborted_streaming",
    uuid: NodeCrypto.randomUUID(),
    errors: ["Error: Request was aborted."],
    ...(live.promptUuid === undefined
      ? {}
      : { user_message_uuid: live.promptUuid, user_message_uuids: [live.promptUuid] }),
  };
}

/** A minimal value for a JSON schema, enough for text generation's structured output. */
export function sampleJsonSchema(schema, root = schema) {
  if (typeof schema !== "object" || schema === null) return null;
  if (typeof schema.$ref === "string" && schema.$ref.startsWith("#/")) {
    const target = schema.$ref
      .slice(2)
      .split("/")
      .reduce((node, key) => node?.[key.replaceAll("~1", "/").replaceAll("~0", "~")], root);
    return sampleJsonSchema(target, root);
  }
  if ("const" in schema) return schema.const;
  if (Array.isArray(schema.enum)) return schema.enum[0];
  const options = schema.anyOf ?? schema.oneOf;
  if (Array.isArray(options)) {
    return sampleJsonSchema(options.find((option) => option?.type !== "null") ?? options[0], root);
  }
  if (Array.isArray(schema.allOf))
    return sampleJsonSchema(Object.assign({}, ...schema.allOf), root);
  const type = Array.isArray(schema.type)
    ? (schema.type.find((candidate) => candidate !== "null") ?? "null")
    : schema.type;
  switch (type ?? (schema.properties ? "object" : "string")) {
    case "object":
      return Object.fromEntries(
        Object.entries(schema.properties ?? {}).map(([key, property]) => [
          key,
          sampleJsonSchema(property, root),
        ]),
      );
    case "array":
      return Array.from({ length: schema.minItems ?? 0 }, () =>
        sampleJsonSchema(schema.items, root),
      );
    case "integer":
    case "number":
      return schema.minimum ?? schema.exclusiveMinimum ?? 0;
    case "boolean":
      return false;
    case "null":
      return null;
    default: {
      const text = "Bench";
      const length = Math.min(Math.max(text.length, schema.minLength ?? 0), schema.maxLength ?? 64);
      return text.padEnd(length, "x").slice(0, length);
    }
  }
}

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

async function runPrint(flags) {
  await readStdin();
  const schema = stringFlag(flags, "--json-schema");
  const structured = schema === undefined ? undefined : sampleJsonSchema(JSON.parse(schema));
  const result = {
    type: "result",
    subtype: "success",
    is_error: false,
    duration_ms: 0,
    duration_api_ms: 0,
    num_turns: 1,
    result: "Bench reply.",
    session_id: sessionIdFor(flags),
    total_cost_usd: 0,
    ...(structured === undefined ? {} : { structured_output: structured }),
  };
  const format = stringFlag(flags, "--output-format") ?? "text";
  process.stdout.write(format === "text" ? `${result.result}\n` : `${JSON.stringify(result)}\n`);
}

function makeWriter(stream) {
  // A closed pipe means the server is gone; there is no one left to answer.
  stream.on("error", () => process.exit(0));
  return (line) =>
    stream.write(`${line}\n`) ? undefined : new Promise((resolve) => stream.once("drain", resolve));
}

function makeStatsSink(statsDir) {
  if (statsDir === undefined) return () => {};
  NodeFS.mkdirSync(statsDir, { recursive: true });
  const file = NodePath.join(statsDir, `fake-claude-${process.pid}.ndjson`);
  return (record) => NodeFS.appendFileSync(file, `${JSON.stringify(record)}\n`);
}

/** What a recorded CLI frame lacks for replay, if anything. */
function frameProblem(frame) {
  switch (frame.type) {
    case "assistant":
      return Array.isArray(frame.message?.content)
        ? undefined
        : "an assistant frame without content";
    case "user": {
      const content = frame.message?.content;
      return typeof content === "string" || Array.isArray(content)
        ? undefined
        : "a user frame without content";
    }
    case "stream_event":
      return typeof frame.event?.type === "string" ? undefined : "a stream_event without an event";
    case "system":
    case "result":
      return typeof frame.subtype === "string"
        ? undefined
        : `a ${frame.type} frame without a subtype`;
    default:
      return undefined;
  }
}

/**
 * The session's T3 MCP endpoint from an `mcp_set_servers` request, or undefined when it names
 * none: the `t3-code` server the adapter registers, else the first server with a URL.
 */
export function mcpCredentials(servers) {
  const entries = Object.entries(servers ?? {});
  const server =
    servers?.["t3-code"] ?? entries.find(([, config]) => typeof config?.url === "string")?.[1];
  if (typeof server?.url !== "string") return undefined;
  const header = Object.entries(server.headers ?? {}).find(
    ([name]) => name.toLowerCase() === "authorization",
  );
  return { url: server.url, authorization: typeof header?.[1] === "string" ? header[1] : null };
}

/** Replaces `<dir>/<session>.json` atomically, readable by its owner only; it holds a token. */
export function writeMcpCredentials(dir, sessionId, credentials) {
  const name = `${sessionId.replace(/[^\w-]/g, "_")}.json`;
  const temp = NodePath.join(dir, `.${name}.${process.pid}.tmp`);
  NodeFS.writeFileSync(temp, `${JSON.stringify(credentials)}\n`, { mode: 0o600, flag: "w" });
  NodeFS.chmodSync(temp, 0o600);
  NodeFS.renameSync(temp, NodePath.join(dir, name));
}

/** Loads and compiles every transcript in the mix; a bad name or recording throws here. */
export async function loadMix(config) {
  const transcripts = new Map();
  for (const { name } of config.mix) {
    const entries = await loadTranscriptEntries(name, config);
    entries.forEach((entry, index) => {
      const frame = entry.frame;
      if (entry.type !== "emit_inbound" || typeof frame?.type !== "string") return;
      const problem = frame.type.includes(".") ? undefined : frameProblem(frame);
      if (problem !== undefined)
        throw new Error(`Transcript ${name} has ${problem} on line ${index + 1}.`);
    });
    const groups = splitTurns(entries);
    if (groups.length === 0) throw new Error(`Transcript ${name} has no completed turn.`);
    transcripts.set(name, groups.map(compileGroup));
  }
  return transcripts;
}

/** An error result, then exit 1: the SDK reports the result's text instead of the exit code. */
function failStream(sessionId, error) {
  const message = `fake claude: ${error instanceof Error ? error.message : String(error)}`;
  const result = {
    type: "result",
    subtype: "error_during_execution",
    duration_ms: 0,
    duration_api_ms: 0,
    is_error: true,
    num_turns: 0,
    stop_reason: null,
    session_id: sessionId,
    total_cost_usd: 0,
    usage: { input_tokens: 0, output_tokens: 0 },
    modelUsage: {},
    permission_denials: [],
    uuid: NodeCrypto.randomUUID(),
    errors: [message],
  };
  process.stderr.write(`${message}\n`);
  process.stdout.write(`${JSON.stringify(result)}\n`, () => process.exit(1));
}

async function runStream(flags, config) {
  const live = {
    sessionId: sessionIdFor(flags),
    model: stringFlag(flags, "--model"),
    permissionMode: stringFlag(flags, "--permission-mode") ?? "default",
    cwd: process.cwd(),
    promptUuid: undefined,
  };
  const write = makeWriter(process.stdout);
  const recordStats = makeStatsSink(config.statsDir);
  const controlRequests = {};
  const prompts = [];
  const transcripts = await loadMix(config);
  if (config.mcpCredentialsDir !== undefined) {
    NodeFS.mkdirSync(config.mcpCredentialsDir, { recursive: true, mode: 0o700 });
    // The mode above only applies to a directory mkdirSync creates.
    NodeFS.chmodSync(config.mcpCredentialsDir, 0o700);
  }
  const mix = Object.fromEntries(config.mix.map(({ name, weight }) => [name, weight]));
  let assignment;
  let played = 0;
  let active;
  let draining = false;
  let faultPending = config.fault !== undefined;

  const injectFault = async (line) => {
    faultPending = false;
    if (config.fault === "bad-shape") {
      const frame = {
        type: "assistant",
        session_id: live.sessionId,
        uuid: NodeCrypto.randomUUID(),
      };
      return write(JSON.stringify(frame));
    }
    await write(line.slice(0, Math.max(1, Math.floor(line.length / 2))));
    if (config.fault === "truncated-frame-exit") {
      process.stdout.end(() => process.exit(1));
      await new Promise(() => {});
    }
  };

  const play = async (message) => {
    // Set before the first await, so an interrupt sent with the prompt still cancels it.
    const controller = new AbortController();
    active = controller;
    live.promptUuid = typeof message.uuid === "string" ? message.uuid : undefined;
    const startedAt = Date.now();
    if (assignment === undefined) {
      const ordinal = claimOrdinal(config.stateDir, live.sessionId);
      const cycle = mixCycle(config.mix);
      assignment = { ordinal, transcript: cycle[(ordinal + config.seed) % cycle.length] };
    }
    const groups = transcripts.get(assignment.transcript);
    const turn = played++ % groups.length;
    const frames = {};
    let bytes = 0;
    let unpaced = 0;
    for (const { frame, line, faultPoint } of turnLines(groups[turn], live, config.turnLoops)) {
      const wait = delayMs(frame.pace, config);
      if (wait > 0) {
        unpaced = 0;
        await sleep(wait, controller.signal);
      } else if (++unpaced >= FRAMES_PER_YIELD) {
        unpaced = 0;
        await new Promise((resolve) => setImmediate(resolve));
      }
      if (controller.signal.aborted) break;
      if (faultPoint && faultPending) await injectFault(line);
      await write(line);
      frames[frame.stat] = (frames[frame.stat] ?? 0) + 1;
      bytes += Buffer.byteLength(line) + 1;
    }
    const aborted = controller.signal.aborted;
    if (aborted) await write(JSON.stringify(abortedResult(live, startedAt)));
    active = undefined;
    recordStats({
      at: new Date().toISOString(),
      pid: process.pid,
      sessionId: live.sessionId,
      transcript: assignment.transcript,
      turn,
      ordinal: assignment.ordinal,
      seed: config.seed,
      mix,
      loops: config.turnLoops,
      aborted,
      frames,
      bytes,
      ms: Date.now() - startedAt,
      rssBytes: process.memoryUsage.rss(),
      controlRequests,
    });
  };

  const drain = async () => {
    draining = true;
    while (prompts.length > 0) await play(prompts.shift());
    draining = false;
  };

  const respond = (requestId, response = {}) =>
    write(
      JSON.stringify({
        type: "control_response",
        response: { subtype: "success", request_id: requestId, response },
      }),
    );

  const handleControl = (requestId, request) => {
    controlRequests[request.subtype] = (controlRequests[request.subtype] ?? 0) + 1;
    switch (request.subtype) {
      case "initialize":
        return respond(requestId, {
          commands: [],
          agents: [],
          models: [],
          output_style: "default",
          available_output_styles: ["default"],
          account: {
            email: "bench@example.invalid",
            subscriptionType: "max",
            tokenSource: "oauth",
          },
          pid: process.pid,
        });
      case "get_usage":
        return respond(requestId, {
          session: {},
          subscription_type: "max",
          rate_limits_available: true,
          rate_limits: { five_hour: { utilization: 0, resets_at: null } },
        });
      case "mcp_set_servers": {
        const credentials = mcpCredentials(request.servers);
        if (config.mcpCredentialsDir !== undefined && credentials !== undefined) {
          try {
            writeMcpCredentials(config.mcpCredentialsDir, live.sessionId, credentials);
          } catch (error) {
            // The message names the path and the errno only, never the token.
            return failStream(live.sessionId, `cannot write MCP credentials: ${error.message}`);
          }
        }
        return respond(requestId, {
          added: Object.keys(request.servers ?? {}),
          removed: [],
          errors: {},
        });
      }
      case "mcp_status":
        return respond(requestId, { mcpServers: [] });
      case "background_tasks":
        return respond(requestId, { backgrounded: false });
      case "set_model":
        live.model = typeof request.model === "string" ? request.model : undefined;
        return respond(requestId);
      case "set_permission_mode":
        if (typeof request.mode === "string") live.permissionMode = request.mode;
        return respond(requestId);
      case "interrupt":
        if (request.cancel_queued === true) prompts.length = 0;
        active?.abort();
        return respond(requestId);
      default:
        return respond(requestId);
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
    if (message.type === "control_request" && typeof message.request?.subtype === "string") {
      void handleControl(message.request_id, message.request);
      return;
    }
    if (message.type !== "user") return;
    // A steer interrupts the running turn; other prompts wait for it, as the CLI queues them.
    if (message.priority === "now") active?.abort();
    prompts.push(message);
    if (!draining) void drain().catch((error) => failStream(live.sessionId, error));
  });
  input.on("close", () => process.exit(0));
}

async function main(argv, env) {
  if (argv.includes("--version") || argv.includes("-v")) {
    process.stdout.write(`${FAKE_CLAUDE_VERSION} (Claude Code)\n`);
    return;
  }
  if (argv.includes("--help") || argv.includes("-h")) {
    process.stdout.write(NodeFS.readFileSync(README, "utf8"));
    return;
  }
  const flags = parseArgs(argv);
  // Checked before the handshake, so a bad knob or transcript fails every session with its cause.
  if (stringFlag(flags, "--input-format") === "stream-json") {
    return Promise.resolve()
      .then(() => runStream(flags, readConfig(env)))
      .catch((error) => failStream(sessionIdFor(flags), error));
  }
  if (flags.has("-p") || flags.has("--print")) return runPrint(flags);
  throw new Error(`unsupported invocation: ${argv.join(" ")}`);
}

if (import.meta.main) {
  main(process.argv.slice(2), process.env).catch((error) => {
    process.stderr.write(`fake claude: ${error.message}\n`);
    process.exit(2);
  });
}

#!/usr/bin/env node
// A fake `codex app-server` for server benchmarks. It answers the JSON-RPC requests the Codex
// provider sends and replays recorded turns of the Codex replay fixtures with live ids, so the
// real client, adapter and orchestration run with no model and no network. `--help` lists knobs.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeURL from "node:url";

import {
  assign,
  claimOrdinal,
  codexMcpCredentials,
  compileFrames,
  delayMs,
  freshId,
  injectExitFault,
  loadTranscriptEntries,
  makeStatsSink,
  makeWriter,
  mixRecord,
  nextTick,
  prepareCredentialsDir,
  readConfig,
  render,
  sleep,
  writeMcpCredentials,
} from "../fake-common/replay.mjs";

// Inside the range the bundled model manifest recommends for Codex.
export const FAKE_CODEX_VERSION = "0.159.0";
export const TRANSCRIPT_FILE = "codex_transcript.ndjson";
const TRANSCRIPTS_DIR = NodeURL.fileURLToPath(new URL("./transcripts/", import.meta.url));
const DEFAULT_MIX = [{ name: "command_output_streaming", weight: 1 }];
// Recorded gaps between frames are kept up to this cap at BENCH_SPEED=1.
const MAX_GAP_MS = 3_000;
// Unpaced replay still yields this often, so stdin requests such as turn/interrupt run.
const FRAMES_PER_YIELD = 64;

const README = new URL("./README.md", import.meta.url);

const THREAD = 0;
const TURN = 1;
const CWD = 2;
const MODEL = 3;
const PROMPT = 4;
const NOW_MS = 5;
const NOW_S = 6;
const FIRST_ID = 7;

const ID_PATTERN =
  /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\b(?:msg|rs|call|ws|fc|ctc|ig)_[A-Za-z0-9]{8,}/gi;
const MS_KEYS = new Set(["emittedAtMs", "startedAtMs", "completedAtMs", "updatedAtMs"]);
const S_KEYS = new Set(["startedAt", "completedAt", "createdAt", "updatedAt", "recencyAt"]);
const timeSlot = (key, value) =>
  MS_KEYS.has(key) && value > 1e12 ? NOW_MS : S_KEYS.has(key) && value > 1e9 ? NOW_S : undefined;

// Sent once per app-server after `initialized`, not per thread: the recordings show them while
// the first thread/start is pending.
const PROCESS_METHODS = new Set([
  "configWarning",
  "remoteControl/status/changed",
  "account/updated",
  "deprecationNotice",
  "warning",
]);
// What thread/resume sends besides its answer (provider_thread_resume).
const RESUME_METHODS = new Set(["mcpServer/startupStatus/updated"]);
const OPENING_METHODS = new Set(["thread/start", "thread/fork", "thread/resume"]);

const isDelta = (method) => method.endsWith("/delta") || method.endsWith("Delta");
const promptText = (input) =>
  (Array.isArray(input) ? input : [])
    .filter((part) => part?.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n");

/** Older recordings miss fields today's schema requires; as `normalizeLegacyInboundFrame` in effect-codex-app-server/src/replay.ts. */
export function normalizeLegacyFrame(value) {
  if (Array.isArray(value)) return value.map(normalizeLegacyFrame);
  if (typeof value !== "object" || value === null) return value;
  const normalized = Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [key, normalizeLegacyFrame(entry)]),
  );
  if (
    typeof normalized.id === "string" &&
    "modelProvider" in normalized &&
    "status" in normalized
  ) {
    normalized.sessionId ??= normalized.id;
    normalized.projectId ??= null;
  }
  const params = normalized.params;
  if (typeof normalized.method !== "string" || typeof params !== "object" || params === null) {
    return normalized;
  }
  if (normalized.method === "item/started") params.startedAtMs ??= 0;
  if (normalized.method === "item/completed") params.completedAtMs ??= 0;
  if (normalized.method.endsWith("/requestApproval")) params.startedAtMs ??= 0;
  if (normalized.method === "item/tool/requestUserInput") params.isBlocking ??= true;
  return normalized;
}

/**
 * Splits a recording into the thread it starts and its completed turns. A turn holds the frames
 * Codex sent after answering turn/start, up to that turn's turn/completed; frames sent while the
 * first turn/start was pending belong to the thread start. Interrupted turns are dropped. A turn
 * on a forked thread replays on the live thread like any other.
 */
export function parseTranscript(entries) {
  const start = entries.find((entry) => entry.type === "transcript_start");
  const methods = new Map();
  let thread;
  const setup = [];
  const turns = [];
  let turn;
  for (const entry of entries) {
    const frame = normalizeLegacyFrame(entry.frame);
    if (entry.type === "expect_outbound") {
      if (typeof frame?.method === "string" && frame.id !== undefined) {
        methods.set(frame.id, frame.method);
        if (frame.method === "turn/start" && thread !== undefined && turn === undefined) {
          turn = { prompt: promptText(frame.params?.input), id: frame.id, before: [], frames: [] };
        }
      }
      continue;
    }
    if (entry.type !== "emit_inbound" || typeof frame !== "object" || frame === null) continue;
    if (frame.method === undefined) {
      const method = methods.get(frame.id);
      if (method === "thread/start" && thread === undefined) thread = frame.result;
      if (turn !== undefined && frame.id === turn.id) turn.turnId = frame.result?.turn?.id;
      continue;
    }
    if (thread === undefined) continue;
    if (turn === undefined || turn.turnId === undefined) {
      // Frames after a turn ended and before the next turn/start trail the turn: not replayed.
      if (turns.length === 0) setup.push(frame);
      else turn?.before.push(frame);
      continue;
    }
    turn.frames.push(frame);
    if (frame.method === "turn/completed" && frame.params?.turn?.id === turn.turnId) {
      turn.threadId = frame.params.threadId;
      if (frame.params.turn.status === "completed") turns.push(turn);
      turn = undefined;
    }
  }
  if (thread === undefined) throw new Error("The transcript never answers thread/start.");
  return { version: start?.version, thread, setup, turns };
}

/**
 * Frames up to the prompt's echo, the work in between, and the final answer through
 * turn/completed. Only the work loops: Codex keeps one final answer per turn.
 */
export function splitTurn(turn, rootThreadId) {
  const frames = turn.frames;
  const root = (frame) => (frame.params?.threadId ?? rootThreadId) === rootThreadId;
  let prefixEnd = frames.findIndex((frame) => frame.method === "turn/started" && root(frame));
  const echo = frames.findIndex(
    (frame) =>
      frame.method === "item/completed" && root(frame) && frame.params.item?.type === "userMessage",
  );
  if (echo > prefixEnd) prefixEnd = echo;
  const answerStart = (final) =>
    frames.findLastIndex(
      (frame) =>
        frame.method === "item/started" &&
        root(frame) &&
        frame.params.item?.type === "agentMessage" &&
        (!final || frame.params.item.phase === "final_answer"),
    );
  const idle = frames.findLastIndex(
    (frame) =>
      frame.method === "thread/status/changed" &&
      root(frame) &&
      frame.params.status?.type === "idle",
  );
  const suffixStart = [answerStart(true), answerStart(false), idle, frames.length - 1].find(
    (index) => index > prefixEnd,
  );
  return {
    prefix: frames.slice(0, prefixEnd + 1),
    body: frames.slice(prefixEnd + 1, suffixStart),
    suffix: frames.slice(suffixStart),
  };
}

function paces(frames) {
  let previous;
  return frames.map((frame) => {
    const at = typeof frame.emittedAtMs === "number" ? frame.emittedAtMs : undefined;
    const gap = at !== undefined && previous !== undefined ? at - previous : undefined;
    if (at !== undefined) previous = at;
    if (typeof frame.method === "string" && isDelta(frame.method)) return { delta: true };
    return gap === undefined || gap <= 0 ? null : { ms: Math.min(gap, MAX_GAP_MS) };
  });
}

export const statKey = (frame) =>
  frame.method === undefined
    ? "response"
    : frame.id !== undefined
      ? `request:${frame.method}`
      : frame.method;

/** Compiles a parsed recording: the thread start answer and each turn's line templates. */
export function compileTranscript(parsed) {
  const threadId = parsed.thread.thread.id;
  const fixed = new Map([
    [threadId, THREAD],
    [parsed.thread.thread.cwd ?? parsed.thread.cwd, CWD],
    [parsed.thread.model, MODEL],
  ]);
  const processFrames = parsed.setup.filter((frame) => PROCESS_METHODS.has(frame.method));
  const threadFrames = parsed.setup.filter((frame) => !PROCESS_METHODS.has(frame.method));
  const setup = compileFrames([parsed.thread, ...threadFrames, ...processFrames], {
    fixed,
    idPattern: ID_PATTERN,
    firstId: FIRST_ID,
    rawSlots: timeSlot,
  });
  const withStats = (templates, frames) =>
    templates.map((template, index) => ({ ...template, stat: statKey(frames[index]) }));
  const turns = parsed.turns.map((turn) => {
    const parts = splitTurn(turn, turn.threadId);
    const turnFixed = new Map([...fixed, [turn.threadId, THREAD], [turn.turnId, TURN]]);
    if (turn.prompt.length > 0) turnFixed.set(turn.prompt, PROMPT);
    const ordered = [...turn.before, ...parts.prefix, ...parts.body, ...parts.suffix];
    const { templates, ids } = compileFrames(ordered, {
      fixed: turnFixed,
      idPattern: ID_PATTERN,
      firstId: FIRST_ID,
      rawSlots: timeSlot,
    });
    const pace = paces(ordered);
    const meta = ordered.map((frame, index) => ({
      ...templates[index],
      pace: pace[index],
      stat: statKey(frame),
      request: frame.id !== undefined && frame.method !== undefined,
    }));
    const beforeEnd = turn.before.length;
    const bodyStart = beforeEnd + parts.prefix.length;
    const bodyEnd = bodyStart + parts.body.length;
    const outside = new Set();
    const inBody = new Set();
    for (const [index, template] of templates.entries()) {
      for (let at = 1; at < template.parts.length; at += 2) {
        const slot = template.parts[at];
        if (slot < FIRST_ID) continue;
        (index >= bodyStart && index < bodyEnd ? inBody : outside).add(slot);
      }
    }
    // An id first seen in the body changes every loop; the suffix shows the last loop's ids.
    const prefixSlots = new Set();
    for (const template of templates.slice(0, bodyStart)) {
      for (let at = 1; at < template.parts.length; at += 2) prefixSlots.add(template.parts[at]);
    }
    const idSlots = ids.map((_, index) => FIRST_ID + index);
    return {
      before: meta.slice(0, beforeEnd),
      prefix: meta.slice(beforeEnd, bodyStart),
      body: meta.slice(bodyStart, bodyEnd),
      suffix: meta.slice(bodyEnd),
      originals: [turn.threadId, turn.turnId, "", "", "", 0, 0, ...ids],
      // Codex keys some items by their turn (plans): looping those would start one twice.
      maxLoops: parts.body.some(
        (frame) => frame.method === "item/started" && frame.params.item?.id?.includes(turn.turnId),
      )
        ? 1
        : Infinity,
      loopSlots: idSlots.filter((slot) => inBody.has(slot) && !prefixSlots.has(slot)),
      playSlots: idSlots.filter((slot) => !inBody.has(slot) || prefixSlots.has(slot)),
    };
  });
  return {
    recordedModel: parsed.thread.model,
    threadAnswer: setup.templates[0],
    threadSetup: withStats(setup.templates.slice(1, 1 + threadFrames.length), threadFrames),
    processSetup: withStats(setup.templates.slice(1 + threadFrames.length), processFrames),
    setupIds: setup.ids,
    turns,
  };
}

/** Every frame one turn/start plays, its work `loops` times (once for a turn that cannot loop). */
export function* turnLines(compiled, live, loops) {
  const values = [...compiled.originals];
  values[THREAD] = live.threadId;
  values[TURN] = live.turnId;
  values[CWD] = live.cwd;
  values[MODEL] = live.model;
  values[PROMPT] = live.prompt;
  for (const slot of compiled.playSlots) values[slot] = freshId(compiled.originals[slot]);
  const line = (frame) => {
    const now = Date.now();
    values[NOW_MS] = now;
    values[NOW_S] = Math.floor(now / 1000);
    return render(frame, values);
  };
  const plays = compiled.body.length === 0 ? 0 : Math.min(loops, compiled.maxLoops);
  const head = [...compiled.before, ...compiled.prefix];
  // The fault point is the middle frame of the turn's first play, before turn/completed.
  const faultAt = Math.floor((head.length + compiled.body.length + compiled.suffix.length) / 2);
  let position = 0;
  const next = (frame, counted) => ({
    frame,
    line: line(frame),
    faultPoint: counted && position++ === faultAt,
  });
  for (const frame of head) yield next(frame, true);
  for (let loop = 0; loop < plays; loop++) {
    for (const slot of compiled.loopSlots) values[slot] = freshId(compiled.originals[slot]);
    for (const frame of compiled.body) yield next(frame, loop === 0);
  }
  for (const frame of compiled.suffix) yield next(frame, true);
}

/** The answer to thread/start (or resume, fork) and the frames that follow it, with live values. */
function threadLines(compiled, live, resumed = false) {
  const values = [live.threadId, "", live.cwd, live.model, "", 0, 0];
  for (const id of compiled.setupIds) values.push(freshId(id));
  const now = Date.now();
  values[NOW_MS] = now;
  values[NOW_S] = Math.floor(now / 1000);
  const setup = resumed
    ? compiled.threadSetup.filter((frame) => RESUME_METHODS.has(frame.stat))
    : compiled.threadSetup;
  return {
    answer: render(compiled.threadAnswer, values),
    setup: setup.map((frame) => ({ stat: frame.stat, line: render(frame, values) })),
    process: compiled.processSetup.map((frame) => ({
      stat: frame.stat,
      line: render(frame, values),
    })),
  };
}

/** Frames and bytes sent, by stat key. */
function makeTally() {
  const frames = {};
  let bytes = 0;
  return {
    add: (stat, line) => {
      frames[stat] = (frames[stat] ?? 0) + 1;
      bytes += Buffer.byteLength(line) + 1;
    },
    get frames() {
      return frames;
    },
    get bytes() {
      return bytes;
    },
  };
}

/** A transcript of the mix, parsed: generated (`transcripts/<name>.mjs`) or recorded. */
export async function loadParsed(name, config) {
  const entries = await loadTranscriptEntries(name, {
    fileName: TRANSCRIPT_FILE,
    transcriptsDir: TRANSCRIPTS_DIR,
    config,
  });
  const parsed = parseTranscript(entries);
  if (parsed.turns.length === 0) throw new Error(`Transcript ${name} has no completed turn.`);
  return parsed;
}

/** Loads and compiles every transcript in the mix; a bad name or recording throws here. */
export async function loadTranscripts(config) {
  const loaded = new Map();
  for (const { name } of config.mix) {
    loaded.set(name, compileTranscript(await loadParsed(name, config)));
  }
  return loaded;
}

const REASONING_DELTAS = {
  "item/reasoning/summaryTextDelta": ["summary", "summaryIndex"],
  "item/reasoning/textDelta": ["content", "contentIndex"],
};

/** The reasoning parts with text in `frames`: T3 keeps one reasoning item per part. */
function reasoningParts(frames) {
  const parts = new Set();
  for (const frame of frames) {
    const delta = REASONING_DELTAS[frame.method];
    if (delta !== undefined && frame.params.delta.length > 0) {
      parts.add(`${frame.params.itemId}:${delta[0]}:${frame.params[delta[1]]}`);
    }
    const item = frame.method === "item/completed" ? frame.params.item : undefined;
    if (item?.type !== "reasoning") continue;
    for (const stream of ["summary", "content"]) {
      for (const [index, text] of (item[stream] ?? []).entries()) {
        if (text.length > 0) parts.add(`${item.id}:${stream}:${index}`);
      }
    }
  }
  return parts.size;
}

/** The plan items in `frames` with text: an empty plan leaves no proposed plan. */
function textedPlans(frames) {
  const plans = new Set();
  for (const frame of frames) {
    if (frame.method === "item/plan/delta" && frame.params.delta.length > 0) {
      plans.add(frame.params.itemId);
    }
    const item = frame.method === "item/completed" ? frame.params.item : undefined;
    if (item?.type === "plan" && item.text.length > 0) plans.add(item.id);
  }
  return plans.size;
}

/**
 * What one played turn sends and leaves behind: frames by kind, completed items of the root
 * thread by Codex item type (reasoning by part, plans with text), turns with a todo plan, and
 * the final answer.
 */
export async function expectedTurn(name, index, requestedLoops, payloadBytes) {
  const parsed = await loadParsed(name, { payloadBytes });
  const compiled = compileTranscript(parsed).turns[index];
  const turn = parsed.turns[index];
  if (compiled === undefined) throw new Error(`Transcript ${name} has no turn ${index}.`);
  const loops = Math.min(requestedLoops, compiled.maxLoops);
  const times = (part) => (part === "body" ? (compiled.body.length === 0 ? 0 : loops) : 1);
  const frames = {};
  for (const part of ["before", "prefix", "body", "suffix"]) {
    for (const frame of compiled[part])
      frames[frame.stat] = (frames[frame.stat] ?? 0) + times(part);
  }
  const root = turn.threadId;
  const split = splitTurn(turn, root);
  const items = {};
  let plans = 0;
  let reply = null;
  for (const [part, list] of [["before", turn.before], ...Object.entries(split)]) {
    const own = list.filter((frame) => (frame.params?.threadId ?? root) === root);
    for (const [type, count] of [
      ["reasoning", reasoningParts(own)],
      ["plan", textedPlans(own)],
    ]) {
      if (count > 0) items[type] = (items[type] ?? 0) + count * times(part);
    }
    for (const frame of own) {
      if (frame.method === "turn/plan/updated") plans = 1;
      const type = frame.method === "item/completed" ? frame.params.item.type : undefined;
      if (type === undefined || type === "reasoning" || type === "plan") continue;
      items[type] = (items[type] ?? 0) + times(part);
      if (type === "agentMessage") reply = frame.params.item.text;
    }
  }
  return { frames, items, todoLists: plans, reply };
}

/** The text of a sent line that completes an answer of `threadId`: what the server shows as the reply. */
export function answerOf(line, threadId) {
  const params = JSON.parse(line).params;
  return params?.threadId === threadId && params.item?.type === "agentMessage"
    ? params.item.text
    : undefined;
}

const BAD_SHAPE = (threadId, turnId) =>
  JSON.stringify({ method: "item/completed", params: { threadId, turnId, item: { type: 7 } } });

async function runAppServer(config) {
  const write = makeWriter(process.stdout);
  const recordStats = makeStatsSink(config.statsDir, "fake-codex");
  const transcripts = await loadTranscripts(config);
  prepareCredentialsDir(config.mcpCredentialsDir);
  const mix = mixRecord(config.mix);
  const fallback = transcripts.get(config.mix[0].name);
  const requests = {};
  const threads = new Map();
  const pendingServerRequests = new Map();
  let initialized = false;
  let processSetupSent = false;
  let nextServerRequestId = 0;
  let faultPending = config.fault !== undefined;

  const send = (message) => write(JSON.stringify(message));
  // Request counts and RSS cover every thread this app-server serves.
  const processFigures = () => ({ rssBytes: process.memoryUsage.rss(), requests: { ...requests } });
  const recordOutsideTurn = (kind, sessionId, tally) =>
    recordStats({
      at: new Date().toISOString(),
      kind,
      pid: process.pid,
      fake: "codex",
      sessionId,
      frames: tally.frames,
      bytes: tally.bytes,
      process: processFigures(),
    });
  const sendCounted = (tally, stat, line) => {
    tally.add(stat, line);
    return write(line);
  };
  const respond = (id, result) => send({ id, result });
  const fail = (id, code, message) => send({ id, error: { code, message } });

  // A thread plays the transcript its ordinal picks, turn after turn; one app-server serves
  // every thread of its instance, and the state directory keeps ordinals across its restarts.
  const pickTurn = (thread) => {
    const turns = transcripts.get(thread.assignment.transcript).turns;
    const turn = thread.played++ % turns.length;
    return { name: thread.assignment.transcript, turn, compiled: turns[turn] };
  };

  const openThread = (id, method, params, threadId) => {
    const credentials = codexMcpCredentials(params.config);
    if (config.mcpCredentialsDir !== undefined && credentials !== undefined) {
      try {
        writeMcpCredentials(config.mcpCredentialsDir, threadId, credentials);
      } catch (error) {
        return fail(id, -32603, `cannot write MCP credentials: ${error.message}`);
      }
    }
    const assignment = assign(config, claimOrdinal(config.stateDir, threadId));
    const compiled = transcripts.get(assignment.transcript);
    const thread = threads.get(threadId) ?? {
      threadId,
      assignment,
      requests: {},
      cwd: typeof params.cwd === "string" ? params.cwd : process.cwd(),
      model: typeof params.model === "string" ? params.model : compiled.recordedModel,
      played: 0,
      queue: [],
      active: undefined,
      draining: false,
    };
    threads.set(threadId, thread);
    countThreadRequest(thread, method);
    const lines = threadLines(compiled, thread, method === "thread/resume");
    const tally = makeTally();
    sendCounted(tally, "response", JSON.stringify({ id, result: JSON.parse(lines.answer) }));
    for (const { stat, line } of lines.setup) sendCounted(tally, stat, line);
    recordOutsideTurn("thread-start", threadId, tally);
  };

  const countThreadRequest = (thread, method) => {
    if (thread !== undefined) thread.requests[method] = (thread.requests[method] ?? 0) + 1;
  };

  const sendProcessSetup = () => {
    const tally = makeTally();
    for (const { stat, line } of threadLines(fallback, { threadId: "", cwd: "", model: "" })
      .process) {
      sendCounted(tally, stat, line);
    }
    recordOutsideTurn("process-start", null, tally);
  };

  const play = async (thread, request) => {
    const controller = new AbortController();
    const turnId = NodeCrypto.randomUUID();
    thread.active = { controller, turnId };
    const startedAt = Date.now();
    const tally = makeTally();
    const started = {
      id: turnId,
      items: [],
      itemsView: "notLoaded",
      status: "inProgress",
      error: null,
      startedAt: null,
      completedAt: null,
      durationMs: null,
    };
    await sendCounted(
      tally,
      "response",
      JSON.stringify({ id: request.id, result: { turn: started } }),
    );
    const pick = pickTurn(thread);
    const live = { ...thread, turnId, prompt: promptText(request.params?.input) };
    let unpaced = 0;
    let reply = null;
    const loops = Math.min(config.turnLoops, pick.compiled.maxLoops);
    for (const { frame, line, faultPoint } of turnLines(pick.compiled, live, loops)) {
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
        if (config.fault === "bad-shape") {
          await sendCounted(tally, "fault", BAD_SHAPE(thread.threadId, turnId));
        } else if (config.fault === "truncated-frame") {
          await sendCounted(
            tally,
            "fault",
            line.slice(0, Math.max(1, Math.floor(line.length / 2))),
          );
        } else await injectExitFault(config.fault, line, write);
      }
      if (frame.request) {
        const id = nextServerRequestId++;
        const answered = new Promise((resolve) => pendingServerRequests.set(id, resolve));
        await sendCounted(tally, frame.stat, JSON.stringify({ ...JSON.parse(line), id }));
        await Promise.race([
          answered,
          new Promise((resolve) => controller.signal.addEventListener("abort", resolve)),
        ]);
        pendingServerRequests.delete(id);
      } else {
        await sendCounted(tally, frame.stat, line);
      }
      if (frame.stat === "item/completed") reply = answerOf(line, thread.threadId) ?? reply;
    }
    const aborted = controller.signal.aborted;
    if (aborted) {
      const now = Date.now();
      const idle = {
        method: "thread/status/changed",
        params: { threadId: thread.threadId, status: { type: "idle" } },
        emittedAtMs: now,
      };
      await sendCounted(tally, idle.method, JSON.stringify(idle));
      const completed = {
        method: "turn/completed",
        params: {
          threadId: thread.threadId,
          turn: {
            id: turnId,
            items: [],
            itemsView: "notLoaded",
            status: "interrupted",
            error: null,
            startedAt: Math.floor(startedAt / 1000),
            completedAt: Math.floor(now / 1000),
            durationMs: now - startedAt,
          },
        },
        emittedAtMs: now,
      };
      await sendCounted(tally, completed.method, JSON.stringify(completed));
    }
    thread.active = undefined;
    recordStats({
      at: new Date().toISOString(),
      kind: "turn",
      pid: process.pid,
      fake: "codex",
      sessionId: thread.threadId,
      ordinal: thread.assignment.ordinal,
      seed: config.seed,
      mix,
      transcript: pick.name,
      turn: pick.turn,
      loops,
      payloadBytes: config.payloadBytes,
      aborted,
      frames: tally.frames,
      bytes: tally.bytes,
      reply,
      ms: Date.now() - startedAt,
      sessionRequests: { ...thread.requests },
      process: processFigures(),
    });
  };

  const drain = async (thread) => {
    thread.draining = true;
    while (thread.queue.length > 0) await play(thread, thread.queue.shift());
    thread.draining = false;
  };

  const handleRequest = (message) => {
    const { id, method } = message;
    const params = message.params ?? {};
    requests[method] = (requests[method] ?? 0) + 1;
    if (!OPENING_METHODS.has(method))
      countThreadRequest(threads.get(String(params.threadId)), method);
    switch (method) {
      case "initialize":
        if (initialized) return fail(id, -32600, "Already initialized");
        initialized = true;
        return respond(id, {
          userAgent: `T3 Code/${FAKE_CODEX_VERSION} (bench fake) codex-app-server`,
          codexHome: process.env.CODEX_HOME ?? NodeOS.tmpdir(),
          platformFamily: "unix",
          platformOs: "linux",
        });
      case "account/read":
        return respond(id, { account: { type: "apiKey" }, requiresOpenaiAuth: false });
      case "account/rateLimits/read":
        return respond(id, { rateLimits: { limitId: "codex", primary: null, secondary: null } });
      case "model/list":
      case "mcpServerStatus/list":
        return respond(id, { data: [], nextCursor: null });
      case "skills/list":
        return respond(id, { data: [] });
      case "config/read":
        return respond(id, { config: {}, origins: {} });
      case "thread/start":
      case "thread/fork":
        return openThread(id, method, params, NodeCrypto.randomUUID());
      case "thread/resume":
        return openThread(id, method, params, String(params.threadId));
      case "thread/read": {
        const live = threads.get(String(params.threadId)) ?? {
          threadId: String(params.threadId),
          cwd: process.cwd(),
          model: fallback.recordedModel,
        };
        const answer = JSON.parse(threadLines(fallback, live).answer);
        return respond(id, { thread: answer.thread });
      }
      case "turn/start": {
        const thread = threads.get(String(params.threadId));
        if (thread === undefined) return fail(id, -32602, `Unknown thread ${params.threadId}`);
        if (typeof params.model === "string") thread.model = params.model;
        thread.queue.push(message);
        if (!thread.draining) {
          void drain(thread).catch((error) => {
            process.stderr.write(`fake codex: ${error.stack ?? error}\n`);
            process.exit(1);
          });
        }
        return;
      }
      case "turn/steer": {
        const active = threads.get(String(params.threadId))?.active;
        if (active === undefined) return fail(id, -32600, "No active turn to steer");
        if (params.expectedTurnId !== active.turnId) {
          return fail(
            id,
            -32600,
            `Expected turn ${params.expectedTurnId}, active ${active.turnId}`,
          );
        }
        return respond(id, { turnId: active.turnId });
      }
      case "turn/interrupt": {
        const active = threads.get(String(params.threadId))?.active;
        if (active === undefined) return fail(id, -32600, "No active turn to interrupt");
        if (params.turnId !== active.turnId) {
          return fail(id, -32600, `Expected turn ${params.turnId}, active ${active.turnId}`);
        }
        active.controller.abort();
        return respond(id, {});
      }
      case "thread/unsubscribe":
        return respond(id, { status: "unsubscribed" });
      case "thread/goal/get":
        return respond(id, { goal: null });
      case "thread/goal/clear":
        return respond(id, { cleared: false });
      case "thread/inject_items":
        return respond(id, {});
      default:
        return fail(id, -32601, `Method not found: ${method}`);
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
    if (typeof message.method === "string") {
      if (message.id !== undefined && message.id !== null) void handleRequest(message);
      else if (message.method === "initialized" && !processSetupSent) {
        processSetupSent = true;
        sendProcessSetup();
      }
      return;
    }
    pendingServerRequests.get(message.id)?.(message);
  });
  input.on("close", () => process.exit(0));
}

/** The knobs in `env`; a bad value or mix throws. */
export const configFrom = (env) =>
  readConfig(env, {
    transcriptVar: "BENCH_CODEX_TRANSCRIPT",
    defaultMix: DEFAULT_MIX,
    defaultStateDir: env.CODEX_HOME?.trim()
      ? NodePath.join(env.CODEX_HOME.trim(), "fake-codex")
      : NodePath.join(NodeOS.tmpdir(), "t3-fake-codex"),
  });

async function main(argv, env) {
  if (argv.includes("--version") || argv.includes("-V")) {
    process.stdout.write(`codex-cli ${FAKE_CODEX_VERSION}\n`);
    return;
  }
  if (argv.includes("--help") || argv.includes("-h")) {
    process.stdout.write(NodeFS.readFileSync(README, "utf8"));
    return;
  }
  if (argv[0] !== "app-server") throw new Error(`unsupported invocation: ${argv.join(" ")}`);
  await runAppServer(configFrom(env));
}

if (import.meta.main) {
  main(process.argv.slice(2), process.env).catch((error) => {
    process.stderr.write(`fake codex: ${error.message}\n`);
    process.exit(2);
  });
}

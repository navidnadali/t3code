// Shared by the bench's replaying provider fakes (fake-codex, fake-acp): load knobs, pacing,
// per-turn stats, fault injection and id templates. The knobs and their names follow the fake
// Claude CLI in fake-claude, so one BENCH_* setting paces every fake the same way.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

export const FIXTURES_DIR = NodeURL.fileURLToPath(
  new URL("../../../apps/server/src/orchestration-v2/testkit/fixtures/", import.meta.url),
);

export const FAULTS = ["truncated-frame", "truncated-frame-exit", "bad-shape", "exit-mid-turn"];

export const KNOBS_HELP = `  BENCH_SPEED               Time scale for every wait (default 1). 0 never waits.
  BENCH_DELTAS_PER_SEC      Streamed delta rate at speed 1 (default 40).
  BENCH_TURN_LOOPS          Times a turn's body plays before the turn ends (default 1).
  BENCH_STATS_DIR           Append a JSON line per played turn to <dir>/<fake>-<pid>.ndjson.
  BENCH_FAULT               Halfway through the first turn a process plays:
                              truncated-frame       a cut-off JSON line, then the turn goes on
                              truncated-frame-exit  a cut-off JSON line, then exit status 1
                              bad-shape             a well-formed frame the schema rejects
                              exit-mid-turn         exit status 1 with the turn unfinished
  BENCH_NODE                The node binary the launcher runs (default: node on PATH).`;

/** The knobs in `env`; `transcriptVar` names the fake's own transcript list. */
export function readConfig(env, { transcriptVar, defaultTranscripts }) {
  const number = (name, fallback, valid, expected) => {
    const raw = env[name]?.trim();
    if (raw === undefined || raw === "") return fallback;
    const value = Number(raw);
    if (!valid(value)) throw new Error(`${name} must be ${expected}, not ${JSON.stringify(raw)}.`);
    return value;
  };
  const fault = env.BENCH_FAULT?.trim() || undefined;
  if (fault !== undefined && !FAULTS.includes(fault)) {
    throw new Error(
      `BENCH_FAULT must be one of ${FAULTS.join(", ")}, not ${JSON.stringify(fault)}.`,
    );
  }
  const transcripts = (env[transcriptVar] ?? "")
    .split(",")
    .map((name) => name.trim())
    .filter((name) => name.length > 0);
  return {
    speed: number("BENCH_SPEED", 1, (value) => value >= 0 && value < Infinity, "a number >= 0"),
    deltasPerSec: number(
      "BENCH_DELTAS_PER_SEC",
      40,
      (value) => value > 0 && value < Infinity,
      "a number > 0",
    ),
    turnLoops: number(
      "BENCH_TURN_LOOPS",
      1,
      (value) => Number.isSafeInteger(value) && value >= 1,
      "an integer >= 1",
    ),
    transcripts: transcripts.length > 0 ? transcripts : defaultTranscripts,
    statsDir: env.BENCH_STATS_DIR?.trim() || undefined,
    fault,
  };
}

/** A fixture name (its `fileName` under the replay fixtures) or an absolute .ndjson path. */
export function transcriptPath(name, fileName) {
  if (NodePath.isAbsolute(name)) return name;
  if (!/^[\w.-]+$/.test(name) || name.startsWith(".")) {
    throw new Error(`Transcript ${JSON.stringify(name)} is neither a fixture name nor absolute.`);
  }
  const file = NodePath.join(FIXTURES_DIR, name, fileName);
  if (!NodeFS.existsSync(file)) throw new Error(`No ${fileName} in the fixture ${name}.`);
  return file;
}

export const readNdjson = (file) =>
  NodeFS.readFileSync(file, "utf8")
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line));

/** Milliseconds to wait before a frame paced `pace`: `{ delta: true }`, `{ ms }` or null. */
export function delayMs(pace, config) {
  if (pace === null || config.speed === 0) return 0;
  return (pace.delta ? 1_000 / config.deltasPerSec : pace.ms) / config.speed;
}

export const sleep = (ms, signal) =>
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

export const nextTick = () => new Promise((resolve) => setImmediate(resolve));

/** Writes NDJSON lines, waiting for drain; a closed pipe means the server is gone. */
export function makeWriter(stream) {
  stream.on("error", () => process.exit(0));
  return (line) =>
    stream.write(`${line}\n`) ? undefined : new Promise((resolve) => stream.once("drain", resolve));
}

export function makeStatsSink(statsDir, fake) {
  if (statsDir === undefined) return () => {};
  NodeFS.mkdirSync(statsDir, { recursive: true });
  const file = NodePath.join(statsDir, `${fake}-${process.pid}.ndjson`);
  return (record) => NodeFS.appendFileSync(file, `${JSON.stringify(record)}\n`);
}

/** Ends the process the way `fault` says, after `line` (the frame it would have written). */
export async function injectExitFault(fault, line, write) {
  if (fault === "truncated-frame-exit") {
    await write(line.slice(0, Math.max(1, Math.floor(line.length / 2))));
  }
  process.stdout.end(() => process.exit(1));
  await new Promise(() => {});
}

export const hashOf = (text) => {
  let hash = 0x811c9dc5;
  for (let index = 0; index < text.length; index++) {
    hash = Math.imul(hash ^ text.charCodeAt(index), 0x01000193) >>> 0;
  }
  return hash;
};

const EXACT_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const randomOf = (alphabet) => alphabet[NodeCrypto.randomInt(alphabet.length)];
const LOWER = "abcdefghijklmnopqrstuvwxyz";
const UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const DIGITS = "0123456789";
const HEX = "0123456789abcdef";

/** A new id shaped like the recorded one: same prefix, length and character classes. */
export function freshId(original) {
  if (EXACT_UUID.test(original)) return NodeCrypto.randomUUID();
  const underscore = original.indexOf("_");
  const kept = underscore >= 0 ? underscore + 1 : 0;
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

const TOKEN_OPEN = "";
const TOKEN_CLOSE = "";
const RAW_OPEN = "";
const RAW_CLOSE = "";
const RAW_TOKEN = /"(\d+)"/g;
const TOKEN_SPLIT = /(r?)(\d+)/;
const token = (slot) => `${TOKEN_OPEN}${slot}${TOKEN_CLOSE}`;
const escapeRegExp = (text) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
export const jsonText = (value) => JSON.stringify(String(value)).slice(1, -1);

function visitStrings(value, visit) {
  if (typeof value === "string") visit(value);
  else if (Array.isArray(value)) for (const entry of value) visitStrings(entry, visit);
  else if (typeof value === "object" && value !== null) {
    for (const [key, entry] of Object.entries(value)) {
      visit(key);
      visitStrings(entry, visit);
    }
  }
}

function rewrite(value, hooks) {
  if (typeof value === "string") return hooks.replace(value);
  if (Array.isArray(value)) return value.map((entry) => rewrite(entry, hooks));
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => {
      const raw = typeof entry === "number" ? hooks.rawSlots(key, entry) : undefined;
      if (raw !== undefined) return [hooks.replace(key), `${RAW_OPEN}${raw}${RAW_CLOSE}`];
      const whole = typeof entry === "string" ? hooks.keySlots(key, entry) : undefined;
      return [hooks.replace(key), whole === undefined ? rewrite(entry, hooks) : token(whole)];
    }),
  );
}

/**
 * Compiles recorded frames into line templates. Strings in `fixed` take the slot it maps them
 * to; other `idPattern` matches become slots from `firstId` on, listed in `ids`. `keySlots(key,
 * string)` may give a whole property value a slot, and `rawSlots(key, number)` a number a slot
 * rendered unquoted, such as a live timestamp.
 */
export function compileFrames(
  frames,
  { fixed, idPattern, firstId, keySlots = () => undefined, rawSlots = () => undefined },
) {
  if (JSON.stringify(frames).match(/[-]/)) {
    throw new Error("A recorded frame contains a character the templates reserve.");
  }
  const slots = new Map([...fixed].filter(([key]) => typeof key === "string" && key.length > 0));
  const ids = [];
  for (const frame of frames) {
    if (idPattern === undefined) continue;
    visitStrings(frame, (text) => {
      for (const match of text.matchAll(idPattern)) {
        if (slots.has(match[0])) continue;
        slots.set(match[0], firstId + ids.length);
        ids.push(match[0]);
      }
    });
  }
  const keys = [...slots.keys()];
  const pattern =
    keys.length === 0
      ? undefined
      : new RegExp(
          keys
            .toSorted((left, right) => right.length - left.length)
            .map(escapeRegExp)
            .join("|"),
          "g",
        );
  const replace = (text) =>
    pattern === undefined ? text : text.replace(pattern, (match) => token(slots.get(match)));
  const templates = frames.map((frame) => {
    const json = JSON.stringify(rewrite(frame, { replace, keySlots, rawSlots })).replace(
      RAW_TOKEN,
      (_, slot) => `${TOKEN_OPEN}r${slot}${TOKEN_CLOSE}`,
    );
    const pieces = json.split(TOKEN_SPLIT);
    const parts = [];
    const raw = new Set();
    for (let index = 0; index < pieces.length; index += 3) {
      parts.push(pieces[index]);
      if (index + 2 >= pieces.length) break;
      if (pieces[index + 1] === "r") raw.add(parts.length);
      parts.push(Number(pieces[index + 2]));
    }
    return { parts, raw };
  });
  return { templates, ids };
}

/** One line from a template; `values[slot]` is a string, or a number for a raw slot. */
export function render(template, values) {
  const { parts, raw } = template;
  let line = parts[0];
  for (let index = 1; index < parts.length; index += 2) {
    const value = values[parts[index]];
    line += (raw.has(index) ? String(value) : jsonText(value)) + parts[index + 1];
  }
  return line;
}

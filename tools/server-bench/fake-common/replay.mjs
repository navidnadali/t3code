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
// The assignment cycle is materialised, so weights stay small.
const MAX_MIX_WEIGHT = 10_000;

/** The knobs in `env`; `transcriptVar` names the fake's own transcript mix. */
export function readConfig(env, { transcriptVar, defaultMix, defaultStateDir }) {
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
  return {
    speed: number("BENCH_SPEED", 1, (value) => value >= 0 && value < Infinity, "a number >= 0"),
    deltasPerSec: number(
      "BENCH_DELTAS_PER_SEC",
      40,
      (value) => value > 0 && value < Infinity,
      "a number > 0",
    ),
    maxGapMs: number(
      "BENCH_MAX_GAP_MS",
      undefined,
      (value) => value >= 0 && value < Infinity,
      "a number >= 0",
    ),
    turnLoops: number(
      "BENCH_TURN_LOOPS",
      1,
      (value) => Number.isSafeInteger(value) && value >= 1,
      "an integer >= 1",
    ),
    payloadBytes: number(
      "BENCH_TOOL_PAYLOAD_BYTES",
      262_144,
      (value) => Number.isSafeInteger(value) && value >= 1,
      "an integer >= 1",
    ),
    mix: parseMix(env[transcriptVar], transcriptVar, defaultMix),
    seed: number(
      "BENCH_SEED",
      0,
      (value) => Number.isSafeInteger(value) && value >= 0,
      "an integer >= 0",
    ),
    stateDir: env.BENCH_STATE_DIR?.trim() || defaultStateDir,
    statsDir: env.BENCH_STATS_DIR?.trim() || undefined,
    mcpCredentialsDir: env.BENCH_MCP_CREDENTIALS_DIR?.trim() || undefined,
    fault,
  };
}

/** Comma-separated `name` or `name:weight` entries; a weight defaults to 1. */
export function parseMix(value, variable, fallback) {
  const weights = new Map();
  for (const entry of (value ?? "").split(",")) {
    if (entry.trim() === "") continue;
    const weighted = /^(.+):(\d+)$/.exec(entry.trim());
    const name = (weighted?.[1] ?? entry).trim();
    const weight = weighted === null ? 1 : Number(weighted[2]);
    if (weight < 1) throw new Error(`${variable} weight of ${name} must be at least 1.`);
    weights.set(name, (weights.get(name) ?? 0) + weight);
  }
  const mix = [...weights].map(([name, weight]) => ({ name, weight }));
  if (mix.reduce((sum, { weight }) => sum + weight, 0) > MAX_MIX_WEIGHT) {
    throw new Error(`${variable} weights must add up to at most ${MAX_MIX_WEIGHT}.`);
  }
  return mix.length > 0 ? mix : fallback;
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
 * concurrent fakes never share one. A session claims once and keeps its ordinal.
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

/** The transcript an ordinal plays: its place in the mix's cycle, rotated by the seed. */
export function assign(config, ordinal) {
  const cycle = mixCycle(config.mix);
  return { ordinal, transcript: cycle[(ordinal + config.seed) % cycle.length] };
}

/** The mix as `{ name: weight }`, for stats lines. */
export const mixRecord = (mix) => Object.fromEntries(mix.map(({ name, weight }) => [name, weight]));

/**
 * A transcript's recorded entries: a generator (`<transcriptsDir>/<name>.mjs`, given the knobs),
 * a file in `transcriptsDir`, a fixture with `fileName`, or an absolute .ndjson path.
 */
export async function loadTranscriptEntries(name, { fileName, transcriptsDir, config }) {
  if (!NodePath.isAbsolute(name)) {
    if (!/^[\w.-]+$/.test(name) || name.startsWith(".")) {
      throw new Error(`Transcript ${JSON.stringify(name)} is neither a plain name nor absolute.`);
    }
    const generator = NodePath.join(transcriptsDir, `${name}.mjs`);
    if (NodeFS.existsSync(generator)) {
      const module = await import(NodeURL.pathToFileURL(generator).href);
      return module.makeTranscript(config);
    }
    const file = NodePath.join(transcriptsDir, `${name}.ndjson`);
    if (NodeFS.existsSync(file)) return readNdjson(file);
  }
  return readNdjson(transcriptPath(name, fileName));
}

/** Text of at least `bytes` UTF-8 bytes in generated source lines, with multi-byte characters. */
export function syntheticText(bytes, label) {
  const lines = [];
  let size = 0;
  for (let line = 1; size < bytes; line++) {
    const text = `export const ${label}${line} = compute("${label}-${line}", ${(line * 37) % 101}, { note: "naïve → résumé ✓\\t#${line}" });`;
    lines.push(text);
    size += Buffer.byteLength(text) + 1;
  }
  return lines.join("\n");
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

/**
 * Milliseconds to wait before a frame paced `pace`: `{ delta: true }`, `{ ms }` or null. A
 * recorded wait is capped at BENCH_MAX_GAP_MS when set; deltas keep their rate.
 */
export function delayMs(pace, config) {
  if (pace === null || config.speed === 0) return 0;
  if (pace.delta) return 1_000 / config.deltasPerSec / config.speed;
  return Math.min(pace.ms, config.maxGapMs ?? Infinity) / config.speed;
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

/** Creates the credentials directory, readable by its owner only, before any session starts. */
export function prepareCredentialsDir(dir) {
  if (dir !== undefined) NodeFS.mkdirSync(dir, { recursive: true, mode: 0o700 });
}

/**
 * Replaces `<dir>/<session>.json` atomically, readable by its owner only: it holds a token. An
 * error names the path and the errno, never the token.
 */
export function writeMcpCredentials(dir, sessionId, credentials) {
  const name = `${sessionId.replace(/[^\w-]/g, "_")}.json`;
  const temp = NodePath.join(dir, `.${name}.${process.pid}.tmp`);
  NodeFS.writeFileSync(temp, `${JSON.stringify(credentials)}\n`, { mode: 0o600, flag: "w" });
  NodeFS.chmodSync(temp, 0o600);
  NodeFS.renameSync(temp, NodePath.join(dir, name));
}

const authorizationOf = (entries) =>
  entries.find(([name]) => name.toLowerCase() === "authorization")?.[1] ?? null;

/** The T3 MCP endpoint in a Codex thread's config (`mcp_servers.t3-code`), if any. */
export function codexMcpCredentials(config) {
  const server = config?.["mcp_servers.t3-code"] ?? config?.mcp_servers?.["t3-code"];
  if (typeof server?.url !== "string") return undefined;
  return {
    url: server.url,
    authorization: authorizationOf(Object.entries(server.http_headers ?? {})),
  };
}

/** The T3 MCP endpoint in an ACP session's `mcpServers`: HTTP, or the stdio bridge's env. */
export function acpMcpCredentials(servers) {
  const server = (Array.isArray(servers) ? servers : []).find((entry) => entry?.name === "t3-code");
  const pairs = (list) =>
    Array.isArray(list) ? list.map((entry) => [entry?.name, entry?.value]) : [];
  if (typeof server?.url === "string") {
    return { url: server.url, authorization: authorizationOf(pairs(server.headers)) };
  }
  const env = Object.fromEntries(pairs(server?.env));
  if (typeof env.T3_ACP_MCP_ENDPOINT !== "string") return undefined;
  return { url: env.T3_ACP_MCP_ENDPOINT, authorization: env.T3_ACP_MCP_AUTHORIZATION ?? null };
}

/** Ends the process the way `fault` says, after `line` (the frame it would have written). */
export async function injectExitFault(fault, line, write) {
  if (fault === "truncated-frame-exit") {
    await write(line.slice(0, Math.max(1, Math.floor(line.length / 2))));
  }
  process.stdout.end(() => process.exit(1));
  await new Promise(() => {});
}

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

const TOKEN_OPEN = "\uE000";
const TOKEN_CLOSE = "\uE001";
const RAW_OPEN = "\uE002";
const RAW_CLOSE = "\uE003";
const RAW_TOKEN = /"\uE002(\d+)\uE003"/g;
const TOKEN_SPLIT = /\uE000(r?)(\d+)\uE001/;
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
  if (/[\uE000-\uE003]/.test(JSON.stringify(frames))) {
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

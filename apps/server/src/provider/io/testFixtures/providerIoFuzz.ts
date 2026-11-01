// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

// Seeded stdout inputs for provider I/O equivalence tests: replay-fixture lines mixed with
// synthetic ones, framed with LF / CRLF / CR CRLF, then split at seeded points.

export type Random = () => number;

export function mulberry32(seed: number): Random {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export const below = (random: Random, bound: number) => Math.floor(random() * bound);
export const pick = <A>(random: Random, items: ReadonlyArray<A>): A =>
  items[below(random, items.length)]!;

const encoder = new TextEncoder();
export const utf8 = (text: string) => encoder.encode(text);

const concat = (parts: ReadonlyArray<Uint8Array>) => {
  const bytes = new Uint8Array(parts.reduce((total, part) => total + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.length;
  }
  return bytes;
};

const FIXTURES = NodePath.join(import.meta.dirname, "../../../orchestration-v2/testkit/fixtures");

export const inboundFrames = (transcript: string) =>
  NodeFS.readdirSync(FIXTURES, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .flatMap((entry) => {
      const file = NodePath.join(FIXTURES, entry.name, transcript);
      if (!NodeFS.existsSync(file)) return [];
      const frames = NodeFS.readFileSync(file, "utf8")
        .split("\n")
        .filter((line) => line.trim().length > 0)
        .map((line) => JSON.parse(line) as { readonly type?: unknown; readonly frame?: unknown })
        .filter((entry) => entry.type === "emit_inbound")
        .map((entry) => entry.frame);
      return [{ fixture: entry.name, frames }];
    });

export const acpWireLines = (frames: ReadonlyArray<unknown>) =>
  frames.map((frame, index) => {
    const { kind, method, params, result } = frame as Record<string, unknown>;
    const id = 100 + index;
    if (kind === "notification") return JSON.stringify({ jsonrpc: "2.0", method, params });
    if (kind === "request") return JSON.stringify({ jsonrpc: "2.0", id, method, params });
    return JSON.stringify({ jsonrpc: "2.0", id, result: result ?? null });
  });

export const ACP_SYNTHETIC_LINES = [
  '{"jsonrpc":"2.0","id":5,"error":{"code":-32000,"message":"denied é"}}',
  '{"jsonrpc":"2.0","id":7,"error":{"_tag":"Cause","code":-32603,"message":"x","data":[{"_tag":"Die","defect":{"code":-32000,"message":"boom"}}]}}',
  '{"jsonrpc":"2.0","method":"session/update","params":{}}',
  '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"state_update","state":"idle","error":{"code":1,"message":"m"}}}}',
  '{"jsonrpc":"2.0","method":"elicitation/complete","params":{"elicitationId":"e1"}}',
  '{"jsonrpc":"2.0","method":"session/elicitation/complete","params":{"bad":true}}',
  '{"jsonrpc":"2.0","method":"x/ext","params":{"t":"🙂最"}}',
  '{"jsonrpc":"2.0","id":"str-1","method":"x/ext-request","params":{}}',
];

// Protocol-log redaction applies to these keys and to Bearer values, also inside JSON strings.
const SENSITIVE_PARAMS = [
  { Authorization: "Bearer sk-live-1", nested: { apiKey: "k-2", note: "plain" } },
  { headers: { "x-api-key": "k-3", accessToken: "t-4" }, password: "p-5" },
  { text: '{"client_secret":"s-6","ok":true}', other: "Bearer abc.def" },
];

const MULTIBYTE = ["é", "ß", "€", "最", "🙂", "𝄞", "ࠀ", "�"];
// Invalid UTF-8: a lone continuation byte, an overlong NUL, 0xFF, and truncated sequences.
const INVALID_UTF8 = [[0x80], [0xc0, 0x80], [0xff], [0xe2, 0x82], [0xf0, 0x9f, 0x99]];
const INCOMPLETE_TAILS = [[0xc3], [0xe2, 0x82], [0xf0, 0x9f], [0xf0, 0x9f, 0x99]];
const BOM = [0xef, 0xbb, 0xbf];

const randomText = (random: Random, length = below(random, 12)) =>
  Array.from({ length }, () =>
    random() < 0.5 ? pick(random, MULTIBYTE) : String.fromCharCode(32 + below(random, 95)),
  ).join("");

// Long strings: summarised log records (over 64 KiB), the 64,000-65,536 byte band, and 16-64 KiB.
const longText = (random: Random) => {
  const band = pick(random, [
    [16_000, 63_000],
    [64_000, 65_400],
    [65_700, 72_000],
  ] as const);
  const length = band[0] + below(random, band[1] - band[0]);
  return "l".repeat(length - 2) + pick(random, ["é", "ab", "🙂"]);
};

export type FuzzProtocol = "codex" | "acp";

const syntheticLine = (random: Random, protocol: FuzzProtocol, recorded: ReadonlyArray<string>) => {
  const roll = random();
  if (roll < 0.4) return pick(random, recorded);
  const text = roll < 0.43 ? longText(random) : randomText(random);
  if (protocol === "codex") {
    // Deltas are not kept in the native log, so long text goes into a completed item.
    if (roll < 0.43) {
      return JSON.stringify({
        method: "item/completed",
        params: { threadId: "t", turnId: "u", item: { type: "agentMessage", id: "i", text } },
      });
    }
    if (roll < 0.66) {
      return JSON.stringify({
        method: "item/agentMessage/delta",
        params: { threadId: "t", turnId: "u", itemId: "i", delta: text },
      });
    }
    if (roll < 0.72)
      return JSON.stringify({ method: `x/${randomText(random, 3)}`, params: { text } });
    if (roll < 0.76) {
      return JSON.stringify({ method: "x/auth", params: pick(random, SENSITIVE_PARAMS) });
    }
    if (roll < 0.8) return JSON.stringify({ id: below(random, 9), result: { text } });
    if (roll < 0.85) return pick(random, ["", " \t", "\r", "[1,2]"]);
    if (roll < 0.95) return `${text}${"x".repeat(below(random, 300))}`;
    return `{"id":1,"text":${JSON.stringify(text)}`;
  }
  if (roll < 0.8) {
    return JSON.stringify({
      jsonrpc: "2.0",
      method: "session/update",
      params: {
        sessionId: "s",
        update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text } },
      },
    });
  }
  if (roll < 0.95) return JSON.stringify({ jsonrpc: "2.0", method: "x/ext", params: { text } });
  if (roll < 0.97) return "";
  return `{"jsonrpc":"2.0","method":${JSON.stringify(text)}`;
};

// Puts invalid UTF-8 inside the line's first JSON string, or at its start when it has none.
const withInvalidUtf8 = (random: Random, line: string) => {
  const quote = line.indexOf('"', line.indexOf(":") + 1);
  const at = quote === -1 ? 0 : quote + 1;
  return concat([
    utf8(line.slice(0, at)),
    Uint8Array.from(pick(random, INVALID_UTF8)),
    utf8(line.slice(at)),
  ]);
};

const terminator = (random: Random) => {
  const roll = random();
  return roll < 0.6 ? "\n" : roll < 0.95 ? "\r\n" : "\r\r\n";
};

interface FuzzInput {
  readonly bytes: Uint8Array;
  /** What the case covers, for failure messages. */
  readonly features: ReadonlyArray<string>;
}

export const fuzzInput = (
  random: Random,
  protocol: FuzzProtocol,
  recorded: ReadonlyArray<string>,
): FuzzInput => {
  const features: Array<string> = [];
  const parts: Array<Uint8Array> = [];
  if (random() < 0.05) {
    parts.push(Uint8Array.from(BOM));
    features.push("bom");
  }
  const count = 1 + below(random, 8);
  for (let index = 0; index < count; index += 1) {
    const line = syntheticLine(random, protocol, recorded);
    const last = index === count - 1;
    if (random() < 0.05) {
      parts.push(withInvalidUtf8(random, line));
      features.push("invalid-utf8");
    } else {
      parts.push(utf8(line));
    }
    const roll = random();
    if (last && roll < 0.08) {
      parts.push(Uint8Array.from(pick(random, INCOMPLETE_TAILS)));
      features.push("incomplete-utf8-at-eof");
    } else if (last && roll < 0.16) {
      parts.push(utf8("\r"));
      features.push("tail-ending-in-cr");
    } else if (last && roll < 0.3) {
      features.push("unterminated-tail");
    } else {
      parts.push(utf8(terminator(random)));
    }
  }
  return { bytes: concat(parts), features };
};

// Seeded split points, half of them pulled into a UTF-8 sequence or between CR and LF.
export const splitPoints = (random: Random, bytes: Uint8Array) => {
  if (bytes.length < 2) return [];
  const points = new Set<number>();
  for (let count = 1 + below(random, Math.min(16, bytes.length - 1)); count > 0; count -= 1) {
    let point = 1 + below(random, bytes.length - 1);
    if (random() < 0.5) {
      for (let step = 0; step < Math.min(bytes.length, 4096); step += 1) {
        const at = ((point - 1 + step) % (bytes.length - 1)) + 1;
        if ((bytes[at]! & 0xc0) === 0x80 || (bytes[at - 1] === 0x0d && bytes[at] === 0x0a)) {
          point = at;
          break;
        }
      }
    }
    points.add(point);
  }
  return [...points].toSorted((a, b) => a - b);
};

// Chunks arrive as views into a larger buffer, as owned arrays, or as Node Buffers.
export const materialize = (
  random: Random,
  bytes: Uint8Array,
  points: ReadonlyArray<number>,
): Array<Uint8Array> =>
  [0, ...points].map((start, index) => {
    const view = bytes.subarray(start, points[index] ?? bytes.length);
    const kind = below(random, 3);
    return kind === 0 ? view : kind === 1 ? view.slice() : Buffer.from(view);
  });

/**
 * Lines of exactly `limit` UTF-8 bytes and one byte over (CR included, LF excluded), ending in
 * ASCII or a multi-byte character, with LF, CRLF or no terminator.
 */
export const limitInputs = (limits: ReadonlyArray<number>) =>
  limits.flatMap((limit) =>
    [limit, limit + 1].flatMap((size) =>
      (["\n", "\r\n", ""] as const).flatMap((end) =>
        (["a", "é"] as const).map((last) => {
          const body = end === "\r\n" ? size - 1 : size;
          const prefix = '{"x":"';
          const suffix = `${last}"}`;
          const fill = body - utf8(prefix).length - utf8(suffix).length;
          const line = `${prefix}${"p".repeat(Math.max(0, fill))}${suffix}`;
          return {
            name: `${utf8(line).length + (end === "\r\n" ? 1 : 0)} bytes vs limit ${limit}, ${JSON.stringify(end)}, ends ${last}`,
            limit,
            bytes: utf8(`${line}${end}`),
          };
        }),
      ),
    ),
  );

/** Codex lines whose log records land around 16 KiB, 64,000 bytes, the 64 KiB limit and above. */
export const logSizeInputs = () =>
  [16_000, 16_500, 40_000, 63_850, 64_100, 65_000, 65_300, 65_600, 70_000].map((size) => ({
    name: `log record of a ${size}-character delta`,
    bytes: utf8(
      `${JSON.stringify({
        method: "item/completed",
        params: {
          threadId: "t",
          turnId: "u",
          item: { type: "agentMessage", id: "i", text: "d".repeat(size - 1) + "é" },
        },
      })}\n`,
    ),
  }));

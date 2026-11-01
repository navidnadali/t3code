// @effect-diagnostics nodeBuiltinImport:off - reads raw file bytes in chunks.
import * as NodeFS from "node:fs";

import { parseJsonContainer, rewriteJson } from "./json.ts";

interface TokenPattern {
  readonly name: string;
  readonly pattern: RegExp;
  /** A match only counts when this holds (Basic auth must decode to `user:password`). */
  readonly confirm?: (match: RegExpMatchArray) => boolean;
}

// No `\b`: a token right after `\n` in JSON text follows the escape's `n`, a word character.
// The lookbehinds only refuse a letter or digit run-in on decoded text; raw bytes are de-escaped.
const AFTER = "(?<![A-Za-z0-9])";

const basicCredentials = (match: RegExpMatchArray) => {
  const decoded = Buffer.from(match.groups?.secret ?? "", "base64").toString("latin1");
  return /^[\x20-\x7e]+:[\x20-\x7e]+$/.test(decoded);
};

/** Credential formats with a recognisable shape; safe on raw bytes, where escapes are blanked first. */
export const TOKEN_PATTERNS: ReadonlyArray<TokenPattern> = [
  { name: "private-key", pattern: /-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----/g },
  { name: "github-token", pattern: new RegExp(`${AFTER}gh[pousr]_[A-Za-z0-9]{36,}`, "g") },
  { name: "github-pat", pattern: new RegExp(`${AFTER}github_pat_[A-Za-z0-9_]{22,}`, "g") },
  { name: "gitlab-token", pattern: new RegExp(`${AFTER}glpat-[A-Za-z0-9_-]{20,}`, "g") },
  { name: "anthropic-key", pattern: new RegExp(`${AFTER}sk-ant-[A-Za-z0-9_-]{20,}`, "g") },
  {
    name: "openai-key",
    pattern: new RegExp(`${AFTER}sk-(?:proj-|svcacct-|admin-)?[A-Za-z0-9_-]{32,}`, "g"),
  },
  {
    name: "aws-access-key",
    pattern: new RegExp(`${AFTER}(?:AKIA|ASIA)[0-9A-Z]{16}(?![0-9A-Z])`, "g"),
  },
  { name: "google-api-key", pattern: new RegExp(`${AFTER}AIza[0-9A-Za-z_-]{35}`, "g") },
  { name: "google-oauth", pattern: new RegExp(`${AFTER}ya29\\.[0-9A-Za-z_-]{20,}`, "g") },
  { name: "slack-token", pattern: new RegExp(`${AFTER}xox[abcdeoprs]-[A-Za-z0-9-]{10,}`, "g") },
  { name: "slack-app-token", pattern: new RegExp(`${AFTER}xapp-[A-Za-z0-9-]{10,}`, "g") },
  { name: "slack-webhook", pattern: /hooks\.slack\.com\/services\/[A-Za-z0-9/_-]{20,}/g },
  { name: "atlassian-token", pattern: new RegExp(`${AFTER}ATATT[A-Za-z0-9_=-]{20,}`, "g") },
  { name: "stripe-key", pattern: new RegExp(`${AFTER}[rs]k_live_[0-9A-Za-z]{20,}`, "g") },
  { name: "npm-token", pattern: new RegExp(`${AFTER}npm_[A-Za-z0-9]{36}`, "g") },
  { name: "huggingface-token", pattern: new RegExp(`${AFTER}hf_[A-Za-z0-9]{30,}`, "g") },
  {
    name: "sendgrid-key",
    pattern: new RegExp(`${AFTER}SG\\.[A-Za-z0-9_-]{22}\\.[A-Za-z0-9_-]{43}`, "g"),
  },
  { name: "mailgun-key", pattern: new RegExp(`${AFTER}key-[0-9a-f]{32}(?![0-9a-f])`, "g") },
  { name: "tokenfeed-token", pattern: new RegExp(`${AFTER}tfe_[A-Za-z0-9_-]{16,}`, "g") },
  {
    name: "jwt",
    pattern: new RegExp(
      `${AFTER}eyJ[A-Za-z0-9_-]{8,}\\.eyJ[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}`,
      "g",
    ),
  },
  { name: "bearer", pattern: new RegExp(`${AFTER}[Bb]earer\\s+[A-Za-z0-9._~+/-]{20,}=*`, "g") },
  {
    name: "basic-auth",
    pattern: new RegExp(`${AFTER}[Bb]asic\\s+(?<secret>[A-Za-z0-9+/]{8,}={0,2})`, "g"),
    confirm: basicCredentials,
  },
  // The scheme is bounded, so a long run of letters cannot make this quadratic.
  {
    name: "url-credentials",
    pattern:
      /(?<![A-Za-z0-9+.-])[A-Za-z][A-Za-z0-9+.-]{0,31}:\/\/[^\s/:@"'\\]{0,256}:(?<secret>[^\s/@"'\\]{1,256})@/g,
    // A password already masked to `*` is not one.
    confirm: (match) => /[^*]/.test(match.groups?.secret ?? ""),
  },
];

/** A name that says its value is a credential: DB_PASSWORD, cfApiToken, refresh_token, SSH_KEY … */
export const CREDENTIAL_NAME =
  /(?:password|passwd|passphrase|secret|token|credential|api[_-]?key|access[_-]?key|private[_-]?key|auth)|(?:^|[_.-])(?:key|pass|pwd)(?:$|[_.-])/i;

/** `NAME=value`, `NAME: value`, `"name": "value"`, quoted or not; the name decides. */
const ASSIGNMENT =
  /(?<![A-Za-z0-9_.-])(?<name>[A-Za-z_][A-Za-z0-9_.-]{0,80})["']?\s*[:=]\s*["']?(?<secret>[^\s"'`,;&|<>(){}[\]]{6,})/g;

/** Long runs of base64 and token characters. */
const HIGH_ENTROPY = /(?<![A-Za-z0-9+/=_-])[A-Za-z0-9+/=_-]{24,}(?![A-Za-z0-9+/=_-])/g;

/**
 * Random runs, as [start, end) pairs. A URL escape (`%3A`, or `%253A` encoded twice) splits a
 * run, so URL-encoded ids in refs read as their parts.
 */
const randomRuns = function* (text: string) {
  const split = text.includes("%")
    ? text.replace(/%(?:25)*[0-9A-Fa-f]{2}/g, (escape) => " ".repeat(escape.length))
    : text;
  for (const match of split.matchAll(HIGH_ENTROPY)) {
    if (looksRandom(match[0])) yield [match.index, match.index + match[0].length] as const;
  }
};

const entropyBits = (text: string) => {
  const counts = new Map<string, number>();
  for (const char of text) counts.set(char, (counts.get(char) ?? 0) + 1);
  let bits = 0;
  for (const count of counts.values()) {
    const p = count / text.length;
    bits -= p * Math.log2(p);
  }
  return bits;
};

/**
 * Random keys mix upper case, lower case and digits with the entropy of noise. Hex digests and
 * UUIDs (one letter case) are ids, and a run of seven lower-case letters is a word: a path or a
 * camelCase name, not a key (random base64 has one in about 6 % of 40-character keys).
 */
export const looksRandom = (token: string) =>
  /[a-z]/.test(token) &&
  /[A-Z]/.test(token) &&
  /[0-9]/.test(token) &&
  !/[a-z]{7}/.test(token) &&
  entropyBits(token) >= 3.5;

/** Letters and digits become `*`; the shape and length stay. */
/**
 * Every visible character becomes `*` (whitespace stays, so lines and length stay): no
 * punctuation is left for a looser check to read as a value (`*//****` still looks like one).
 */
const mask = (text: string) => text.replace(/\S/g, "*");

// A key split across strings (a diff's lines in an array): the markers and the base64 body lines
// on their own, as P1-002's extended check reads them.
const KEY_MARKER = /-----(?:BEGIN|END)[A-Z0-9 ]{0,40}PRIVATE KEY(?: BLOCK)?-----/g;
const KEY_BODY_LINE = /^[ +-]?[A-Za-z0-9+/]{60,76}={0,2}$/gm;

const KEY_HEADER = /-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----/g;
const KEY_FOOTER = /-----END [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----/g;

/** Header to footer, else to the end of the text; index scans, no backtracking on big text. */
const maskKeyBlocks = (text: string): string => {
  let out = "";
  let copied = 0;
  KEY_HEADER.lastIndex = 0;
  for (let header = KEY_HEADER.exec(text); header !== null; header = KEY_HEADER.exec(text)) {
    KEY_FOOTER.lastIndex = header.index + header[0].length;
    const footer = KEY_FOOTER.exec(text);
    const end = footer === null ? text.length : footer.index + footer[0].length;
    out += text.slice(copied, header.index) + mask(text.slice(header.index, end));
    copied = end;
    KEY_HEADER.lastIndex = end;
  }
  return copied === 0 ? text : out + text.slice(copied);
};

export interface DecodedScan {
  /** Also flag high-entropy runs: right for free text, wrong for kept ids (tool ids look random). */
  readonly entropy: boolean;
}

/** A credential name with a value that still holds a letter or digit (a masked one does not). */
/**
 * A credential name with a value that still holds a letter or digit (a masked one does not) and
 * is not a plain number, time or version (`inputTokens: 123456` counts tokens).
 */
const isAssignment = (name: string | undefined, secret: string | undefined) =>
  CREDENTIAL_NAME.test(name ?? "") &&
  /[A-Za-z0-9]/.test(secret ?? "") &&
  !/^[\d.:+-]+$/.test(secret ?? "");

const assignments = function* (text: string) {
  for (const match of text.matchAll(ASSIGNMENT)) {
    if (isAssignment(match.groups?.name, match.groups?.secret)) yield match;
  }
};

// Terminal colour and cursor codes read as spaces, so a token printed in colour is still found.
const ANSI = /\x1b\[[0-9;?]*[ -/]*[@-~]/g;
const withoutAnsi = (text: string) =>
  text.includes("\x1b") ? text.replace(ANSI, (code) => " ".repeat(code.length)) : text;

/** Credential kinds found in one decoded string. */
export const scanDecoded = (raw: string, options: DecodedScan): Array<string> => {
  const text = withoutAnsi(raw);
  const hits = new Set<string>();
  for (const { name, pattern, confirm } of TOKEN_PATTERNS) {
    for (const match of text.matchAll(pattern)) {
      if (confirm === undefined || confirm(match)) {
        hits.add(name);
        break;
      }
    }
  }
  for (const _ of assignments(text)) {
    hits.add("credential-assignment");
    break;
  }
  KEY_MARKER.lastIndex = 0;
  KEY_BODY_LINE.lastIndex = 0;
  if (KEY_MARKER.test(text) || KEY_BODY_LINE.test(text)) hits.add("private-key");
  if (options.entropy) {
    for (const _ of randomRuns(text)) {
      hits.add("high-entropy");
      break;
    }
  }
  return [...hits];
};

/** Masks every finding in one decoded string: key blocks, tokens, assigned values, random runs. */
export const redactDecoded = (raw: string, options: DecodedScan): string => {
  const text = withoutAnsi(raw);
  let out = maskKeyBlocks(text).replace(KEY_MARKER, mask).replace(KEY_BODY_LINE, mask);
  const maskMatch = (match: string, ...rest: Array<unknown>) => {
    const groups = rest.at(-1) as { secret?: string } | string;
    const secret = typeof groups === "object" ? groups.secret : undefined;
    if (secret === undefined) return mask(match);
    // The secret is the last capture but need not end the match (a URL's password precedes `@`).
    const at = match.lastIndexOf(secret);
    return match.slice(0, at) + mask(secret) + match.slice(at + secret.length);
  };
  for (const { pattern, confirm } of TOKEN_PATTERNS) {
    out = out.replace(pattern, (match: string, ...rest: Array<unknown>) => {
      if (confirm !== undefined) {
        const groups = rest.at(-1);
        const probe = Object.assign([match], { groups }) as unknown as RegExpMatchArray;
        if (!confirm(probe)) return match;
      }
      return maskMatch(match, ...rest);
    });
  }
  out = out.replace(ASSIGNMENT, (match: string, ...rest: Array<unknown>) => {
    const groups = rest.at(-1) as { name?: string; secret?: string };
    return isAssignment(groups.name, groups.secret) ? maskMatch(match, ...rest) : match;
  });
  if (options.entropy) {
    // Masking keeps length, so the runs found on the masked text sit at the same places.
    const parts: Array<string> = [];
    let copied = 0;
    for (const [start, end] of randomRuns(out)) {
      parts.push(out.slice(copied, start), mask(out.slice(start, end)));
      copied = end;
    }
    if (copied > 0) out = parts.join("") + out.slice(copied);
  }
  if (text === raw) return out;
  // Masking keeps length, so the masked positions carry back onto the text with its colour codes.
  let merged = "";
  for (let index = 0; index < raw.length; index++) {
    merged += out[index] !== text[index] ? out[index] : raw[index];
  }
  return merged;
};

/**
 * Redacts text, JSON or not: inside JSON each decoded string (nested JSON unwrapped) is redacted on
 * its own, and a value under a credential-named key is masked whatever it holds.
 */
export const redact = (text: string, options: DecodedScan = { entropy: true }): string => {
  if (parseJsonContainer(text) === null) return redactDecoded(text, options);
  return rewriteJson(text, (decoded, where) => {
    if (
      where.role === "value" &&
      where.key !== null &&
      CREDENTIAL_NAME.test(where.key) &&
      decoded !== ""
    ) {
      return mask(decoded);
    }
    return redact(decoded, options);
  });
};

/** Credential kinds in text, JSON or not, read on decoded strings (nested JSON unwrapped). */
export const secretHits = (
  text: string,
  options: DecodedScan = { entropy: true },
): Array<string> => {
  if (parseJsonContainer(text) === null) return scanDecoded(text, options);
  const hits = new Set<string>();
  rewriteJson(text, (decoded, where) => {
    if (
      where.role === "value" &&
      where.key !== null &&
      CREDENTIAL_NAME.test(where.key) &&
      /[A-Za-z0-9]/.test(decoded)
    ) {
      hits.add("credential-field");
    }
    for (const hit of secretHits(decoded, options)) hits.add(hit);
    return decoded;
  });
  return [...hits];
};

/** Columns whose name says they hold a credential; a sanitised table keeps none of them non-null. */
export const CREDENTIAL_COLUMN =
  /(?:secret|token|credential|password|passwd|cookie|api_?key|private_?key)/i;

// JSON escapes (any depth: a backslash run and what it escapes) and colour codes, escaped or not,
// read as spaces on raw bytes, so a token right after one is still found.
const ESCAPES =
  /\\+u001[bB]\[[0-9;?]*[A-Za-z]|\x1b\[[0-9;?]*[A-Za-z]|\\+(?:u[0-9a-fA-F]{4}|[^\\])/g;

/** Token formats in the raw file bytes (free pages and indexes included), in overlapping chunks. */
export const scanFileBytes = (path: string, chunkBytes = 8 << 20): Map<string, number> => {
  const hits = new Map<string, number>();
  const overlap = 4096;
  const buffer = Buffer.alloc(chunkBytes + overlap);
  const handle = NodeFS.openSync(path, "r");
  try {
    let tail = 0;
    let position = 0;
    for (;;) {
      const read = NodeFS.readSync(handle, buffer, tail, chunkBytes, position);
      if (read === 0) break;
      position += read;
      const length = tail + read;
      const text = buffer
        .toString("latin1", 0, length)
        .replace(ESCAPES, (escape) => " ".repeat(escape.length));
      const count = (name: string, match: RegExpMatchArray) => {
        // A match ending inside the carried tail was counted with the previous chunk.
        if ((match.index ?? 0) + match[0].length > tail) hits.set(name, (hits.get(name) ?? 0) + 1);
      };
      for (const { name, pattern, confirm } of TOKEN_PATTERNS) {
        for (const match of text.matchAll(pattern)) {
          if (confirm === undefined || confirm(match)) count(name, match);
        }
      }
      const keep = Math.min(overlap, length);
      buffer.copy(buffer, 0, length - keep, length);
      tail = keep;
    }
  } finally {
    NodeFS.closeSync(handle);
  }
  return hits;
};

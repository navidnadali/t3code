// @effect-diagnostics nodeBuiltinImport:off - reads raw file bytes in chunks.
import * as NodeFS from "node:fs";

/** Credential shapes the sanitiser redacts and the self-check must find none of. */
export const SECRET_PATTERNS: ReadonlyArray<{ readonly name: string; readonly pattern: RegExp }> = [
  // Found by its header; `redact` masks the whole block inside its decoded string (redactKeys).
  { name: "private-key", pattern: /-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/g },
  { name: "github-token", pattern: /\bgh[pousr]_[A-Za-z0-9]{36,}/g },
  { name: "github-pat", pattern: /\bgithub_pat_[A-Za-z0-9_]{22,}/g },
  { name: "gitlab-token", pattern: /\bglpat-[A-Za-z0-9_-]{20,}/g },
  { name: "anthropic-key", pattern: /\bsk-ant-[A-Za-z0-9_-]{20,}/g },
  { name: "openai-key", pattern: /\bsk-(?:proj-|svcacct-|admin-)?[A-Za-z0-9_-]{32,}/g },
  { name: "aws-access-key", pattern: /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/g },
  { name: "google-api-key", pattern: /\bAIza[0-9A-Za-z_-]{35}/g },
  { name: "slack-token", pattern: /\bxox[abeprs]-[A-Za-z0-9-]{10,}/g },
  { name: "slack-webhook", pattern: /hooks\.slack\.com\/services\/[A-Za-z0-9/_-]{20,}/g },
  { name: "stripe-key", pattern: /\b[rs]k_live_[0-9A-Za-z]{20,}/g },
  { name: "npm-token", pattern: /\bnpm_[A-Za-z0-9]{36}\b/g },
  { name: "huggingface-token", pattern: /\bhf_[A-Za-z0-9]{30,}/g },
  { name: "sendgrid-key", pattern: /\bSG\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43}\b/g },
  { name: "mailgun-key", pattern: /\bkey-[0-9a-f]{32}\b/g },
  { name: "tokenfeed-token", pattern: /\btfe_[A-Za-z0-9_-]{16,}/g },
  { name: "jwt", pattern: /\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/g },
  { name: "bearer", pattern: /\b[Bb]earer\s+[A-Za-z0-9._~+/-]{20,}=*/g },
  { name: "url-credentials", pattern: /\b[a-z][a-z0-9+.-]*:\/\/[^\s/:@"'\\]+:[^\s/@"'\\]+@/g },
  {
    name: "assignment",
    pattern:
      /\b(?:api[_-]?key|api[_-]?token|access[_-]?token|auth[_-]?token|secret[_-]?key|client[_-]?secret|password|passwd)\b\\*["']?\s*[:=]\s*\\*["']?(?<secret>[A-Za-z0-9_\-+/=.]{12,})/gi,
  },
];

/**
 * Same length, so payload sizes keep their shape: letters and digits become `*`. Punctuation and
 * every escape (a backslash run and the character or `uXXXX` after it, at any JSON nesting depth)
 * stay, so the JSON around a match still parses.
 */
const mask = (match: string) => {
  let out = "";
  for (let index = 0; index < match.length; index++) {
    const char = match[index] as string;
    if (char !== "\\") {
      out += /[A-Za-z0-9]/.test(char) ? "*" : char;
      continue;
    }
    let end = index;
    while (match[end] === "\\") end++;
    const keep = match[end] === "u" ? 5 : end < match.length ? 1 : 0;
    out += match.slice(index, end + keep);
    index = end + keep - 1;
  }
  return out;
};

/** One case-insensitive pass that every precise pattern implies; rows without a match skip redaction. */
export const MAYBE_SECRET = new RegExp(
  SECRET_PATTERNS.map(({ pattern }) => `(?:${pattern.source})`).join("|"),
  "i",
);

const KEY_HEADER = /-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/g;
const KEY_FOOTER = /-----END [A-Z ]*PRIVATE KEY(?: BLOCK)?-----/g;

/** Header to footer, else to the end of the text; plain index scans, no backtracking on big text. */
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

/** The index of the quote closing a JSON string that starts before `from`: one after an even backslash run. */
const closingQuote = (text: string, from: number) => {
  for (let at = text.indexOf('"', from); at !== -1; at = text.indexOf('"', at + 1)) {
    let slashes = 0;
    while (at - slashes - 1 >= from && text[at - slashes - 1] === "\\") slashes++;
    if (slashes % 2 === 0) return at;
  }
  return -1;
};

const parsesAsJson = (text: string) => {
  const start = text.trimStart()[0];
  if (start !== "{" && start !== "[") return false;
  try {
    JSON.parse(text);
    return true;
  } catch {
    return false;
  }
};

/**
 * Masks whole private key blocks inside the decoded string value that holds them, recursing into
 * JSON-encoded strings, so armor headers with quotes stay covered and no match crosses a field.
 */
const redactKeys = (text: string): string => {
  if (!text.includes("PRIVATE KEY")) return text;
  if (!parsesAsJson(text)) return maskKeyBlocks(text);
  let out = "";
  let copied = 0;
  for (let open = text.indexOf('"'); open !== -1;) {
    const close = closingQuote(text, open + 1);
    if (close === -1) break;
    const token = text.slice(open, close + 1);
    if (token.includes("PRIVATE KEY")) {
      const decoded = JSON.parse(token) as string;
      const redacted = redactKeys(decoded);
      if (redacted !== decoded) {
        out += text.slice(copied, open) + JSON.stringify(redacted);
        copied = close + 1;
      }
    }
    open = text.indexOf('"', close + 1);
  }
  return copied === 0 ? text : out + text.slice(copied);
};

/** Masks each match; a pattern with a `secret` group masks only that part (an assignment's value). */
export const redact = (text: string): string => {
  let out = redactKeys(text);
  for (const { name, pattern } of SECRET_PATTERNS) {
    if (name === "private-key") continue;
    out = out.replace(pattern, (match: string, ...rest: Array<unknown>) => {
      const groups = rest.at(-1) as { secret?: string } | string;
      const secret = typeof groups === "object" ? groups.secret : undefined;
      if (secret === undefined) return mask(match);
      const start = match.length - secret.length;
      return match.slice(0, start) + mask(secret);
    });
  }
  return out;
};

export const secretHits = (text: string): Array<string> => {
  const hits: Array<string> = [];
  for (const { name, pattern } of SECRET_PATTERNS) {
    pattern.lastIndex = 0;
    if (pattern.test(text)) hits.push(name);
    pattern.lastIndex = 0;
  }
  return hits;
};

/** Columns whose name says they hold a credential; a sanitised table keeps none of them non-null. */
export const CREDENTIAL_COLUMN =
  /(?:secret|token|credential|password|passwd|cookie|api_?key|private_?key)/i;

/** Scans the raw file bytes, free pages and indexes included, in overlapping chunks. */
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
      const text = buffer.toString("latin1", 0, length);
      for (const { name, pattern } of SECRET_PATTERNS) {
        for (const match of text.matchAll(pattern)) {
          // A match ending inside the carried tail was counted with the previous chunk.
          if ((match.index ?? 0) + match[0].length > tail)
            hits.set(name, (hits.get(name) ?? 0) + 1);
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

// @effect-diagnostics nodeBuiltinImport:off - reads raw file bytes in chunks.
import * as NodeFS from "node:fs";

/** Credential shapes the sanitiser redacts and the self-check must find none of. */
export const SECRET_PATTERNS: ReadonlyArray<{ readonly name: string; readonly pattern: RegExp }> = [
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
      /\b(?:api[_-]?key|api[_-]?token|access[_-]?token|auth[_-]?token|secret[_-]?key|client[_-]?secret|password|passwd)\b\\?["']?\s*[:=]\s*\\?["']?[A-Za-z0-9_\-+/=.]{12,}/gi,
  },
];

/** Same length, so payload sizes keep their shape; JSON-safe characters only. */
const mask = (match: string) => "x".repeat(match.length);

/** One case-insensitive pass that every precise pattern implies; rows without a match skip redaction. */
export const MAYBE_SECRET = new RegExp(
  SECRET_PATTERNS.map(({ pattern }) => `(?:${pattern.source})`).join("|"),
  "i",
);

export const redact = (text: string): string => {
  let out = text;
  for (const { pattern } of SECRET_PATTERNS) out = out.replace(pattern, mask);
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

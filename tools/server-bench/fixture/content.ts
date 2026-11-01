// @effect-diagnostics nodeBuiltinImport:off - keys filler with node:crypto.
import * as NodeCrypto from "node:crypto";

// By path, as the server modules are: the bench tests load this file without the CLI's resolve hook.
import * as Contracts from "../../../packages/contracts/src/index.ts";
import type * as SchemaAST from "effect/SchemaAST";

import { NAMEY, highEntropy } from "./extended.ts";
import { parseJsonContainer, rewriteJson } from "./json.ts";
import { CREDENTIAL_NAME, redactDecoded, scanDecoded } from "./secrets.ts";

/**
 * Filler: words of the original's UTF-8 and UTF-16 size, checkable by regenerating them (a seed
 * opens each long segment). Its letters cannot spell a credential word (no c, h, k, t, w, y, no
 * two consonants together), words run at most seven letters, and it has no digits, capitals, `:`
 * or `=`, so no pattern of either check can match in or right after it.
 */
const CONSONANTS = "bdfglmnprsvz";
const FINALS = "nrsl";
const VOWELS = "aeiou";
const SYLLABLES = [...CONSONANTS].flatMap((consonant) =>
  [...VOWELS].map((vowel) => `${consonant}${vowel}`),
);
// Five syllables as `ss ss s`-shaped words: 7.8e8 seeds, so blanked values of one length almost
// never collide (a unique column holding blanked text fails the pass on a collision).
const SEED_SYLLABLES = 5;
const SEEDS = SYLLABLES.length ** SEED_SYLLABLES;
const SEED_LENGTH = 12;
const SHORT = "vamo lire sab";
const MIN_SEEDED = SEED_LENGTH + 1;
const WIDE_LETTER: Readonly<Record<number, string>> = { 2: "é", 3: "あ", 4: "𝑥" };
const WIDE_SPACE: Readonly<Record<number, string>> = { 2: " ", 3: "　", 4: "𝑥" };

const mulberry32 = (seed: number) => {
  let state = seed | 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
};

/** A fixed vocabulary, drawn by a Zipf-like rank so word frequencies look like prose. */
const VOCABULARY: ReadonlyArray<string> = (() => {
  const next = mulberry32(0x7e3c_51a9);
  return Array.from({ length: 4096 }, () => {
    let word = "";
    for (let syllables = 1 + Math.floor(next() * 3); syllables > 0; syllables--) {
      word += SYLLABLES[Math.floor(next() * SYLLABLES.length)];
    }
    return next() < 0.4 ? `${word}${FINALS[Math.floor(next() * FINALS.length)]}` : word;
  });
})();

const syllablesOf = (seed: number) => {
  const syllables: Array<string> = [];
  for (let index = 0, rest = seed; index < SEED_SYLLABLES; index++) {
    syllables.push(SYLLABLES[rest % SYLLABLES.length] as string);
    rest = Math.floor(rest / SYLLABLES.length);
  }
  return syllables;
};

const seedWords = (seed: number) => {
  const [a, b, c, d, e] = syllablesOf(seed);
  return `${a}${b} ${c}${d} ${e}`;
};

const seedOf = (text: string) => {
  if (text[4] !== " " || text[9] !== " ") return -1;
  const letters = `${text.slice(0, 4)}${text.slice(5, 9)}${text.slice(10, 12)}`;
  let seed = 0;
  for (let index = 0, scale = 1; index < SEED_SYLLABLES; index++, scale *= SYLLABLES.length) {
    const consonant = CONSONANTS.indexOf(letters[index * 2] ?? "");
    const vowel = VOWELS.indexOf(letters[index * 2 + 1] ?? "");
    if (consonant < 0 || vowel < 0) return -1;
    seed += (consonant * VOWELS.length + vowel) * scale;
  }
  return seed;
};

/** `length` characters of filler words for a seed; never starts or ends with a space. */
export const fillAscii = (seed: number, length: number): string => {
  if (length < MIN_SEEDED) {
    const short = SHORT.slice(0, length);
    return short.endsWith(" ") ? `${short.slice(0, -1)}a` : short;
  }
  const next = mulberry32(seed ^ 0x5bd1e995);
  const words = [seedWords(seed)];
  for (let size = SEED_LENGTH; size < length;) {
    const word = VOCABULARY[Math.floor(VOCABULARY.length ** next()) - 1] as string;
    words.push(word);
    size += word.length + 1;
  }
  const text = words.join(" ").slice(0, length);
  return text.endsWith(" ") ? `${text.slice(0, -1)}a` : text;
};

/** Characters filler keeps in place: controls (newlines, tabs, escapes), quotes, backslashes. */
const isKept = (code: number) => code < 0x20 || code === 0x22 || code === 0x5c || code === 0x7f;

const NON_ASCII = /[^\x00-\x7f]/;

const widthOf = (character: string) => {
  const code = character.codePointAt(0) as number;
  // A lone surrogate is written as U+FFFD, three bytes.
  return code < 0x80 ? 1 : code < 0x800 ? 2 : code < 0x10000 ? 3 : 4;
};

/** A segment with wider characters: one fixed stream over its characters, widths kept. */
const fillWide = (characters: ReadonlyArray<string>) => {
  const stream = fillAscii(0, characters.length);
  return characters
    .map((character, index) => {
      const width = widthOf(character);
      const unit = stream[index] as string;
      return width === 1 ? unit : unit === " " ? WIDE_SPACE[width] : WIDE_LETTER[width];
    })
    .join("");
};

const fillSegment = (segment: string, seed: number): string =>
  NON_ASCII.test(segment) ? fillWide([...segment]) : fillAscii(seed, segment.length);

const isFillerSegment = (segment: string): boolean => {
  if (NON_ASCII.test(segment)) return fillWide([...segment]) === segment;
  if (segment.length < MIN_SEEDED) return segment === fillAscii(0, segment.length);
  const seed = seedOf(segment);
  return seed >= 0 && segment === fillAscii(seed, segment.length);
};

const segments = function* (text: string) {
  let start = 0;
  for (let index = 0; index <= text.length; index++) {
    if (index === text.length || isKept(text.charCodeAt(index))) {
      if (index > start) yield { kept: false, text: text.slice(start, index) };
      if (index < text.length) yield { kept: true, text: text[index] as string };
      start = index + 1;
    }
  }
};

/** Filler for `text` from a seed: same UTF-8 and UTF-16 length, same kept characters. */
export const fillText = (text: string, seed: number): string => {
  const parts: Array<string> = [];
  let index = 0;
  for (const part of segments(text)) {
    parts.push(part.kept ? part.text : fillSegment(part.text, (seed + index++ * 104_729) % SEEDS));
  }
  return parts.join("");
};

/** True when every segment of `text` is filler: the self-check's test that nothing was left. */
export const isFiller = (text: string): boolean => {
  for (const part of segments(text)) {
    if (!part.kept && !isFillerSegment(part.text)) return false;
  }
  return true;
};

// One random key per run, never stored: the same text gets the same filler within a run (so
// duplicate texts stay duplicates), and filler cannot be matched against a guessed original.
const RUN_KEY = NodeCrypto.randomBytes(32);
const keyed = (text: string) =>
  NodeCrypto.createHmac("sha256", RUN_KEY).update(text).digest().readUInt32BE(0);

const blankText = (text: string) => fillText(text, keyed(text) % SEEDS);

/** Filler syllables, one letter per UTF-8 byte of the key: always a valid identifier. */
const blankKey = (key: string) => {
  const next = mulberry32(keyed(key));
  let out = "";
  for (let bytes = Buffer.byteLength(key, "utf8"); bytes > 0; bytes -= 2) {
    out +=
      bytes === 1
        ? FINALS[Math.floor(next() * FINALS.length)]
        : SYLLABLES[Math.floor(next() * SYLLABLES.length)];
  }
  return out;
};

/** Every string literal and enum value the contracts declare: the values enum fields can take. */
export const KNOWN_LITERALS: ReadonlySet<string> = (() => {
  const literals = new Set<string>();
  const seen = new Set<SchemaAST.AST>();
  const visit = (ast: SchemaAST.AST | undefined): void => {
    if (ast === undefined || seen.has(ast)) return;
    seen.add(ast);
    switch (ast._tag) {
      case "Literal":
        if (typeof ast.literal === "string" && ast.literal.length <= 64) literals.add(ast.literal);
        break;
      case "Enum":
        for (const [, value] of ast.enums) if (typeof value === "string") literals.add(value);
        break;
      case "Union":
        ast.types.forEach(visit);
        break;
      case "Arrays":
        ast.elements.forEach(visit);
        ast.rest.forEach(visit);
        break;
      case "Objects":
        for (const property of ast.propertySignatures) visit(property.type);
        for (const index of ast.indexSignatures) visit(index.type);
        break;
      case "Suspend":
        visit(ast.thunk());
        break;
      case "Declaration":
        ast.typeParameters.forEach(visit);
        break;
      default:
        break;
    }
    for (const link of ast.encoding ?? []) visit(link.to);
  };
  for (const value of Object.values(Contracts)) {
    if ((typeof value === "object" || typeof value === "function") && value !== null) {
      const ast = (value as { readonly ast?: SchemaAST.AST }).ast;
      if (ast !== undefined && typeof ast === "object" && "_tag" in ast) visit(ast);
    }
  }
  return literals;
})();

const TIMESTAMP =
  /^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:?\d{2})?)?$/;
const ID_KEY = /^(?:id|ids|ID|uuid)$|_(?:id|ids)$|[a-z0-9](?:Id|Ids|ID|IDs)$/;
// Composite ids nest encoded ids: event ids run to about 900 characters on real data.
const ID_VALUE = /^[^\s\x00-\x1f\x7f]{1,4096}$/u;
const ENUM_KEY =
  /^(?:type|kind|status|role)$|_(?:type|kind|status|role)$|[a-z0-9](?:Type|Kind|Status|Role)$/;
const ENUM_VALUE = /^[A-Za-z][A-Za-z0-9_.:/-]{0,63}$/;
const MODEL_KEY =
  /(?:^|_|[a-z0-9])(?:model|Model|provider|Provider|driver|Driver)(?:_name|Name|Slug|_slug)?$/;
const MODEL_VALUE = /^[A-Za-z0-9][A-Za-z0-9._:/@[\]+-]{0,127}$/;
const HASH_KEY = /(?:^|_|[a-z0-9])(?:sha|Sha|SHA|hash|Hash|oid|Oid|commit|Commit|revision)$/;
const HASH_VALUE = /^[0-9a-f]{7,64}$/i;
/** A key ending in a credential word: P1-002's check reads any 8-character value after it. */
const CREDENTIAL_KEY_END =
  /(?:token|secret|password|passwd|pwd|access[_-]?key|api[_-]?key|apikey|private[_-]?key)$/i;
/** Object keys kept as written; any other key becomes lowercase letters of its size. */
const IDENTIFIER = /^[A-Za-z_$@][A-Za-z0-9_$@.:-]{0,63}$/;

export type Kept = "literal" | "timestamp" | "id" | "enum" | "model" | "hash";

/**
 * Why a string may stay, or null when it is blanked. `key` is the JSON key it sits under (array
 * items inherit it) or the column name. A credential-named key keeps only known literals.
 */
export const keptAs = (key: string | null, value: string): Kept | null => {
  if (key !== null && value.length >= 8 && CREDENTIAL_KEY_END.test(key)) return null;
  if (KNOWN_LITERALS.has(value)) return "literal";
  if (TIMESTAMP.test(value)) return "timestamp";
  if (key === null || CREDENTIAL_NAME.test(key)) return null;
  if (ID_KEY.test(key) && ID_VALUE.test(value)) return "id";
  if (ENUM_KEY.test(key) && ENUM_VALUE.test(value)) return "enum";
  if (MODEL_KEY.test(key) && MODEL_VALUE.test(value)) return "model";
  if (HASH_KEY.test(key) && HASH_VALUE.test(value)) return "hash";
  return null;
};

/**
 * The pattern layer on a kept value. No entropy test: ids, hashes, model names and event types
 * look random by design, and the shapes above already exclude prose.
 */
const keptFindings = (value: string, kept: Kept, key: string | null): Array<string> => {
  if (kept === "literal" || kept === "timestamp") return [];
  const findings = scanDecoded(value, { entropy: false });
  // P1-002's rule: a high-entropy value under a key/token/session/auth/… name is a credential.
  if (key !== null && NAMEY.test(key) && highEntropy(value)) findings.push("high-entropy");
  return findings;
};

const keyFindings = (key: string) =>
  key === "" || /^[a-z]+$/.test(key)
    ? []
    : IDENTIFIER.test(key)
      ? scanDecoded(key, { entropy: false })
      : ["key"];

/** `blank` (default): filler for all but the allowlist. `redact`: the pattern layer alone. */
export type ContentMode = "blank" | "redact";

export interface ContentCounts {
  /** Strings replaced by filler (blank) or masked (redact). */
  changed: number;
  /** Allowlisted strings the pattern layer still flagged, so blanked after all. */
  flaggedKept: number;
  /** Object keys rewritten: not an identifier, or flagged. */
  keys: number;
}

export const emptyCounts = (): ContentCounts => ({ changed: 0, flaggedKept: 0, keys: 0 });

const mask = (text: string) => text.replace(/\S/g, "*");

/** The same member counts at every depth: rewritten keys did not collide. */
const sameShape = (left: unknown, right: unknown): boolean => {
  if (Array.isArray(left))
    return (
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((item, index) => sameShape(item, right[index]))
    );
  if (typeof left === "object" && left !== null) {
    if (typeof right !== "object" || right === null || Array.isArray(right)) return false;
    const leftValues = Object.values(left);
    const rightValues = Object.values(right);
    return (
      leftValues.length === rightValues.length &&
      leftValues.every((item, index) => sameShape(item, rightValues[index]))
    );
  }
  return true;
};

export class ContentRewriteError extends Error {}

/**
 * One stored string, rewritten: JSON containers walk every key and string (nested JSON in a
 * string is walked too); other text is kept when allowlisted and unflagged, else blanked.
 */
export const rewriteContent = (
  text: string,
  key: string | null,
  mode: ContentMode,
  counts: ContentCounts,
): string => {
  if (text === "") return text;
  if (parseJsonContainer(text) !== null) {
    let keysChanged = false;
    const out = rewriteJson(
      text,
      (decoded, where) => {
        if (where.role === "value") return rewriteContent(decoded, where.key, mode, counts);
        if (keyFindings(decoded).length === 0) return decoded;
        keysChanged = true;
        counts.keys++;
        return blankKey(decoded);
      },
      key,
    );
    if (keysChanged && !sameShape(JSON.parse(text), JSON.parse(out))) {
      throw new ContentRewriteError("Two rewritten object keys collided; nothing was written.");
    }
    return out;
  }
  const kept = keptAs(key, text);
  if (kept !== null) {
    if (keptFindings(text, kept, key).length === 0) return text;
    counts.flaggedKept++;
    counts.changed++;
    return mode === "blank" ? blankText(text) : mask(text);
  }
  if (mode === "blank") {
    counts.changed++;
    return blankText(text);
  }
  const masked =
    key !== null && CREDENTIAL_NAME.test(key) ? mask(text) : redactDecoded(text, { entropy: true });
  if (masked !== text) counts.changed++;
  return masked;
};

/**
 * What a stored string still holds, by kind: `not-blanked` for text that is neither allowlisted nor
 * filler (blank mode), `key` for a key neither identifier nor rewritten, else the pattern kinds.
 */
export const contentFindings = (
  text: string,
  key: string | null,
  mode: ContentMode,
  report: (kind: string) => void,
): void => {
  if (text === "") return;
  if (parseJsonContainer(text) !== null) {
    rewriteJson(
      text,
      (decoded, where) => {
        if (where.role === "key") keyFindings(decoded).forEach(report);
        else contentFindings(decoded, where.key, mode, report);
        return decoded;
      },
      key,
    );
    return;
  }
  const kept = keptAs(key, text);
  if (kept !== null) {
    keptFindings(text, kept, key).forEach(report);
    return;
  }
  if (mode === "blank") {
    if (!isFiller(text)) report("not-blanked");
    return;
  }
  if (key !== null && CREDENTIAL_NAME.test(key) && /[A-Za-z0-9]/.test(text)) {
    report("credential-field");
  }
  scanDecoded(text, { entropy: true }).forEach(report);
};

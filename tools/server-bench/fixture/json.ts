/**
 * Where a JSON string sits: an object key, or a value under the nearest key (arrays inherit it).
 * `marked` is true when that key or any enclosing one satisfies the caller's `mark`.
 */
export type StringRole =
  | { readonly role: "key" }
  | { readonly role: "value"; readonly key: string | null; readonly marked: boolean };

/** Where a JSON number sits: the nearest key, and whether it or an enclosing key is marked. */
export interface NumberPlace {
  readonly key: string | null;
  readonly marked: boolean;
}

export interface RewriteOptions {
  /** The key a top-level array's items sit under (the column or the enclosing field). */
  readonly rootKey?: string | null;
  /** True when the text itself sits under a marked key (nested JSON inside a marked field). */
  readonly rootMarked?: boolean;
  readonly mark?: (key: string) => boolean;
  /** Called with each number as written; a different return replaces it. */
  readonly onNumber?: (raw: string, where: NumberPlace) => string;
}

/** The text when it is a JSON object or array, else null. Scalars stay plain text. */
export const parseJsonContainer = (text: string): unknown => {
  const start = text.trimStart()[0];
  if (start !== "{" && start !== "[") return null;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return null;
  }
};

/** The index of the quote closing a JSON string that opened before `from`. */
const closingQuote = (text: string, from: number) => {
  for (let at = text.indexOf('"', from); at !== -1; at = text.indexOf('"', at + 1)) {
    let slashes = 0;
    while (at - slashes - 1 >= from && text[at - slashes - 1] === "\\") slashes++;
    if (slashes % 2 === 0) return at;
  }
  return -1;
};

const NUMBER_CHARACTER = /[-+0-9.eE]/;

interface Frame {
  readonly kind: "object" | "array";
  readonly marked: boolean;
  key: string | null;
  expectKey: boolean;
}

/**
 * Rewrites the string (and, with `onNumber`, number) tokens of valid JSON text in place: key order
 * and formatting stay byte for byte, and only a token the callback changes is re-encoded. Callers
 * check validity first. Iterative, so nesting depth costs no stack.
 */
export const rewriteJson = (
  text: string,
  onString: (decoded: string, where: StringRole) => string,
  options: RewriteOptions = {},
): string => {
  const { rootKey = null, rootMarked = false, mark, onNumber } = options;
  const stack: Array<Frame> = [];
  let out = "";
  let copied = 0;
  const isMarked = (key: string | null) => key !== null && mark !== undefined && mark(key);
  const valueKey = () => (stack.length === 0 ? rootKey : (stack.at(-1)?.key ?? null));
  const markedHere = () => {
    const top = stack.at(-1);
    return top === undefined ? rootMarked || isMarked(rootKey) : top.marked || isMarked(top.key);
  };
  const replace = (start: number, end: number, next: string) => {
    out += text.slice(copied, start) + next;
    copied = end;
  };
  for (let index = 0; index < text.length; index++) {
    const char = text[index] as string;
    if (char === "{" || char === "[") {
      const inherited = char === "[" ? valueKey() : null;
      stack.push({
        kind: char === "{" ? "object" : "array",
        marked: markedHere(),
        key: inherited,
        expectKey: char === "{",
      });
    } else if (char === "}" || char === "]") {
      stack.pop();
    } else if (char === ",") {
      const top = stack.at(-1);
      if (top?.kind === "object") top.expectKey = true;
    } else if (char === ":") {
      const top = stack.at(-1);
      if (top !== undefined) top.expectKey = false;
    } else if (char === '"') {
      const close = closingQuote(text, index + 1);
      if (close === -1) break;
      const decoded = JSON.parse(text.slice(index, close + 1)) as string;
      const top = stack.at(-1);
      const isKey = top?.kind === "object" && top.expectKey;
      const next = onString(
        decoded,
        isKey ? { role: "key" } : { role: "value", key: valueKey(), marked: markedHere() },
      );
      if (isKey && top !== undefined) top.key = decoded;
      if (next !== decoded) replace(index, close + 1, JSON.stringify(next));
      index = close;
    } else if (onNumber !== undefined && (char === "-" || (char >= "0" && char <= "9"))) {
      let end = index + 1;
      while (end < text.length && NUMBER_CHARACTER.test(text[end] as string)) end++;
      const raw = text.slice(index, end);
      const next = onNumber(raw, { key: valueKey(), marked: markedHere() });
      if (next !== raw) replace(index, end, next);
      index = end - 1;
    }
  }
  return copied === 0 ? text : out + text.slice(copied);
};

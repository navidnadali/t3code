/** Where a JSON string sits: an object key, or a value under the nearest key (arrays inherit it). */
export type StringRole =
  | { readonly role: "key" }
  | { readonly role: "value"; readonly key: string | null };

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

interface Frame {
  readonly kind: "object" | "array";
  key: string | null;
  expectKey: boolean;
}

/**
 * Rewrites the string tokens of valid JSON text in place: numbers, key order and formatting stay
 * byte for byte, and only a string `onString` changes is re-encoded. Callers check validity first.
 * `rootKey` is the key a top-level array's strings sit under (the column or the enclosing field).
 */
export const rewriteJson = (
  text: string,
  onString: (decoded: string, where: StringRole) => string,
  rootKey: string | null = null,
): string => {
  const stack: Array<Frame> = [];
  let out = "";
  let copied = 0;
  const valueKey = () => (stack.length === 0 ? rootKey : (stack.at(-1)?.key ?? null));
  for (let index = 0; index < text.length; index++) {
    const char = text[index];
    if (char === "{" || char === "[") {
      const inherited = char === "[" ? valueKey() : null;
      stack.push({
        kind: char === "{" ? "object" : "array",
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
      const token = text.slice(index, close + 1);
      const decoded = JSON.parse(token) as string;
      const top = stack.at(-1);
      const isKey = top?.kind === "object" && top.expectKey;
      const next = onString(decoded, isKey ? { role: "key" } : { role: "value", key: valueKey() });
      if (isKey && top !== undefined) top.key = decoded;
      if (next !== decoded) {
        out += text.slice(copied, index) + JSON.stringify(next);
        copied = close + 1;
      }
      index = close;
    }
  }
  return copied === 0 ? text : out + text.slice(copied);
};

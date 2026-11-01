const BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/**
 * Words either pattern layer reads as a credential name (case-insensitive, so `DSN`, `PASS` and
 * `Key` too): random base62 spelling one, followed by `:` in a composite id, reads as assignment.
 */
const CREDENTIAL_WORD =
  /auth|token|secret|pass|pwd|dsn|credential|key|cookie|session|private|webhook/i;

/**
 * A Claude-style native id (`toolu_01…`, `msg_01…`): 14 random base62 characters, drawn again
 * while they spell a credential word, then the counter as 8 decimal digits (unique below 10^8;
 * digits spell nothing, so no tail can bring a word back).
 */
export const nativeId = (prefix: string, random: () => number, counter: number): string => {
  let body = "";
  do {
    body = "";
    for (let index = 0; index < 14; index++) body += BASE62[Math.floor(random() * 62)];
  } while (CREDENTIAL_WORD.test(body));
  return `${prefix}_01${body}${String(counter % 100_000_000).padStart(8, "0")}`;
};

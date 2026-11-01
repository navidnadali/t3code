const BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/** Credential words random base62 can spell; the pattern layer would read them as names. */
const CREDENTIAL_WORD = /auth|token|secret|passw|pwd|credential|api_?key|access_?key/i;

/**
 * A Claude-style native id (`toolu_01…`, `msg_01…`): 14 random base62 characters, drawn again
 * while they spell a credential word, then an 8-character tail from `counter` that keeps it unique.
 */
export const nativeId = (prefix: string, random: () => number, counter: number): string => {
  let body = "";
  do {
    body = "";
    for (let index = 0; index < 14; index++) body += BASE62[Math.floor(random() * 62)];
  } while (CREDENTIAL_WORD.test(body));
  let id = `${prefix}_01${body}`;
  for (let index = 0, tail = counter; index < 8; index++, tail = Math.floor(tail / 62)) {
    id += BASE62[tail % 62];
  }
  return CREDENTIAL_WORD.test(id) ? nativeId(prefix, random, counter) : id;
};

/**
 * P1-002's extended check (`p1002_extended_check.py` and the 52 patterns of `p1002_sanitise.py`),
 * ported rule for rule. Each value is read as stored (`raw`) and, if JSON, as its decoded strings
 * and keys (`decoded`). Counts by kind only; a clean fixture scores 0 in every row.
 */

const ALNUM = /[A-Za-z0-9]/;
const WORD = /[A-Za-z0-9_-]/;
const HEX = /[0-9a-f]/;
const DIGIT = /[0-9]/;
const HEXU = /[0-9a-fABCDEFu]/;
const BACKSLASH = "\\";

const at = (text: string, index: number) => text[index] ?? "";

/** True if text[i] is a backslash that starts an escape (an odd run of backslashes ends at i). */
const oddBackslashesEndingAt = (text: string, index: number) => {
  let run = 0;
  for (let i = index; i >= 0 && text[i] === BACKSLASH; i--) run++;
  return run % 2 === 1;
};

/** The characters just before `start` end a JSON escape (`\n`, `\uXXXX`): a token boundary. */
const jsonEscapeTailBefore = (text: string, start: number) => {
  if (
    start >= 2 &&
    text[start - 2] === BACKSLASH &&
    oddBackslashesEndingAt(text, start - 2) &&
    "nrtbf/".includes(at(text, start - 1))
  ) {
    return true;
  }
  return (
    start >= 6 &&
    text[start - 6] === BACKSLASH &&
    oddBackslashesEndingAt(text, start - 6) &&
    text[start - 5] === "u" &&
    /^[0-9a-fA-F]{4}$/.test(text.slice(start - 4, start))
  );
};

/** Moves a span start out of a JSON escape it would begin inside. */
const escapeFreeStart = (text: string, start: number) => {
  for (let k = 1; k < 6; k++) {
    const i = start - k;
    if (i < 0) return start;
    if (text[i] === BACKSLASH && oddBackslashesEndingAt(text, i)) {
      if (k === 1) return text[start] === "u" ? start + 5 : start + 1;
      return text[i + 1] === "u" ? Math.max(start, i + 6) : start;
    }
    if (!HEXU.test(at(text, i))) return start;
  }
  return start;
};

const boundaryOk = (text: string, start: number, chars: RegExp) =>
  start === 0 || !chars.test(at(text, start - 1)) || jsonEscapeTailBefore(text, start);

type Check = (text: string, start: number, end: number) => boolean;
const notAfter =
  (chars: RegExp): Check =>
  (text, start) =>
    boundaryOk(text, start, chars);

const telegramOk: Check = (text, start) => {
  let i = start;
  while (i > 0 && DIGIT.test(at(text, i - 1))) i--;
  return start - i >= 8 && start - i <= 10 && boundaryOk(text, i, ALNUM);
};

const mailgunOk: Check = (text, start) =>
  start >= 32 &&
  /^[0-9a-f]{32}$/.test(text.slice(start - 32, start)) &&
  (start === 32 || !HEX.test(at(text, start - 33)));

interface P1002Pattern {
  readonly name: string;
  readonly kind: "token" | "value";
  readonly pattern: RegExp;
  readonly lower?: boolean;
  readonly check?: Check;
  readonly spanFrom?: number;
}

const p = (
  name: string,
  kind: "token" | "value",
  source: string,
  options: Omit<P1002Pattern, "name" | "kind" | "pattern"> = {},
): P1002Pattern => ({
  name,
  kind,
  // `d`: group offsets, so a value's span is exact.
  pattern: new RegExp(source, kind === "value" ? "dg" : "g"),
  ...options,
});

const NOT_AFTER_ALNUM = notAfter(ALNUM);
const NOT_AFTER_WORD = notAfter(WORD);

/** `p1002_sanitise.py` PATTERNS, in order. */
export const P1002_PATTERNS: ReadonlyArray<P1002Pattern> = [
  p("anthropic/openai sk-", "token", String.raw`sk-[A-Za-z0-9_\-]{20,}`, { check: NOT_AFTER_WORD }),
  p("github token", "token", String.raw`gh[pousr]_[A-Za-z0-9]{30,}`, { check: NOT_AFTER_ALNUM }),
  p("github fine-grained", "token", String.raw`github_pat_[A-Za-z0-9_]{30,}`),
  p(
    "gitlab token",
    "token",
    String.raw`gl(?:pat|dt|ptt|rt|cbt|imt|soat|oas|agent|ffct|ft|wt)-[A-Za-z0-9_\-]{20,}`,
    { check: NOT_AFTER_ALNUM },
  ),
  p("gitlab runner", "token", String.raw`GR1348941[A-Za-z0-9_\-]{20,}`),
  p("slack token", "token", String.raw`xox[abposre]-[A-Za-z0-9\-]{10,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("slack app token", "token", String.raw`xapp-[0-9]-[A-Za-z0-9\-]{10,}`),
  p(
    "slack webhook",
    "token",
    String.raw`hooks\.slack\.com/(?:services|workflows|triggers)/[A-Za-z0-9/_\-]{20,}`,
  ),
  p("aws access key", "token", String.raw`A(?:KIA|SIA|BIA|CCA)[A-Z0-9]{16}(?![A-Z0-9])`, {
    check: notAfter(/[A-Z0-9]/),
  }),
  p("google api key", "token", String.raw`AIza[0-9A-Za-z_\-]{35}`),
  p("google oauth access", "token", String.raw`ya29\.[0-9A-Za-z_\-]{20,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("google oauth refresh", "token", String.raw`1//0[0-9A-Za-z_\-]{30,}`, {
    check: notAfter(/[A-Za-z0-9/]/),
  }),
  p("google client secret", "token", String.raw`GOCSPX-[A-Za-z0-9_\-]{20,}`),
  p(
    "private key block",
    "token",
    String.raw`-----BEGIN[A-Z0-9 ]{0,40}PRIVATE KEY(?: BLOCK)?-----(?:[A-Za-z0-9+/=:,\t \r\n]|\\[nrt]|-(?!----))*` +
      String.raw`(?:-----END[A-Z0-9 ]{0,40}PRIVATE KEY(?: BLOCK)?-----)?`,
  ),
  p("jwt", "token", String.raw`eyJ[A-Za-z0-9_\-]{8,}\.eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]*`, {
    check: NOT_AFTER_WORD,
  }),
  p("stripe key", "token", String.raw`sk_(?:live|test)_[A-Za-z0-9]{16,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("stripe restricted key", "token", String.raw`rk_(?:live|test)_[A-Za-z0-9]{16,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("stripe webhook secret", "token", String.raw`whsec_[A-Za-z0-9]{20,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("mailgun key", "token", String.raw`key-[0-9a-f]{32}(?![0-9a-f])`, { check: NOT_AFTER_ALNUM }),
  p("mailgun private key", "token", String.raw`-[0-9a-f]{8}-[0-9a-f]{8}(?![0-9a-f])`, {
    check: mailgunOk,
    spanFrom: -32,
  }),
  p("sendgrid key", "token", String.raw`SG\.[A-Za-z0-9_\-]{16,}\.[A-Za-z0-9_\-]{16,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("npm token", "token", String.raw`npm_[A-Za-z0-9]{36}`, { check: NOT_AFTER_ALNUM }),
  p("pypi token", "token", String.raw`pypi-AgEIcHlwaS5vcmc[A-Za-z0-9_\-]{50,}`),
  p("huggingface token", "token", String.raw`hf_[A-Za-z0-9]{30,}`, { check: NOT_AFTER_ALNUM }),
  p("docker pat", "token", String.raw`dckr_pat_[A-Za-z0-9_\-]{20,}`),
  p("digitalocean token", "token", String.raw`do[opr]_v1_[a-f0-9]{64}`, { check: NOT_AFTER_ALNUM }),
  p("linear key", "token", String.raw`lin_(?:api|oauth)_[A-Za-z0-9]{30,}`),
  p("atlassian token", "token", String.raw`AT(?:ATT|CTT)3[A-Za-z0-9_\-=]{50,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("notion token", "token", String.raw`secret_[A-Za-z0-9]{40,}`, { check: NOT_AFTER_ALNUM }),
  p("notion token (ntn)", "token", String.raw`ntn_[A-Za-z0-9]{40,}`, { check: NOT_AFTER_ALNUM }),
  p("shopify token", "token", String.raw`shp(?:at|ca|pa|ss)_[a-fA-F0-9]{32}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("sentry token", "token", String.raw`sntr[ysu]s?_[A-Za-z0-9+/=_\-]{40,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("grafana token", "token", String.raw`gl(?:sa|c)_[A-Za-z0-9_\-+/=]{32,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("vault token", "token", String.raw`hv[sb]\.[A-Za-z0-9_\-]{24,}`, { check: NOT_AFTER_ALNUM }),
  p("age secret key", "token", String.raw`AGE-SECRET-KEY-1[0-9A-Z]{50,}`),
  p("telegram bot token", "token", String.raw`:AA[A-Za-z0-9_\-]{33}(?![A-Za-z0-9_\-])`, {
    check: telegramOk,
  }),
  p("twilio key", "token", String.raw`SK[0-9a-f]{32}(?![0-9a-f])`, { check: NOT_AFTER_ALNUM }),
  p("postman key", "token", String.raw`PMAK-[a-f0-9]{24}-[a-f0-9]{34}`),
  p("cloudflare origin ca", "token", String.raw`v1\.0-[a-f0-9]{24}-[a-f0-9]{146}`),
  p("tokenfeed token", "token", String.raw`tfe_[A-Za-z0-9_\-]{16,}`, { check: NOT_AFTER_ALNUM }),
  p("tailscale key", "token", String.raw`tskey-[A-Za-z0-9\-]{20,}`, { check: NOT_AFTER_ALNUM }),
  p("doppler token", "token", String.raw`dp\.(?:pt|st|sa|ct|scim|audit)\.[A-Za-z0-9]{40,}`, {
    check: NOT_AFTER_ALNUM,
  }),
  p("azure account key", "value", String.raw`AccountKey=(?<v>[A-Za-z0-9+/=]{40,})`),
  p("azure shared access key", "value", String.raw`SharedAccessKey=(?<v>[A-Za-z0-9+/=]{40,})`),
  p("url credentials", "value", String.raw`://[^\s/:@"'<>\\]{1,128}:(?<v>[^\s/@"'<>\\]{3,})@`),
  p(
    "authorization header",
    "value",
    String.raw`authori[sz]ation(?:\\["']|["'])?\s*[:=]\s*(?:\\["']|["'])?(?:bearer|basic|token|bot)\s+(?<v>[a-z0-9._~+/=\-]{8,})`,
    { lower: true },
  ),
  p("bearer token", "value", String.raw`bearer\s+(?<v>[a-z0-9._~+/\-]{20,}=*)`, {
    lower: true,
    check: NOT_AFTER_ALNUM,
  }),
  p(
    "api key header",
    "value",
    String.raw`(?:api|auth|access)[-_](?:key|token)(?:\\["']|["'])?\s*[:=]\s*(?:\\["']|["'])?(?<v>[a-z0-9._~+/=\-]{12,})`,
    { lower: true },
  ),
  p(
    "t3 session cookie",
    "value",
    String.raw`t3_(?:dev_)?session[A-Za-z0-9_]*=(?<v>[A-Za-z0-9._\-]{16,})`,
  ),
  p(
    "cookie",
    "value",
    String.raw`cookie(?:\\["']|["'])?\s*[:=]\s*(?:\\["']|["'])?[a-z0-9_\-.]{1,64}=(?<v>[a-z0-9%._~+/=\-]{16,})`,
    { lower: true },
  ),
  p(
    "secret assignment",
    "value",
    String.raw`(?:passw(?:or)?d|pwd|passphrase|secret|api[_\-]?key|apikey|access[_\-]?key|private[_\-]?key|token)` +
      String.raw`(?:\\["']|["'])?\s*[:=]\s*(?:\\["']|["'])?(?<v>[a-z0-9_\-+/=.~!@#$%^&*]{12,})`,
    { lower: true },
  ),
  p(
    "secret env var",
    "value",
    String.raw`(?:TOKEN|SECRET|PASSWORD|PASSWD|PASS|API_KEY|APIKEY|ACCESS_KEY|PRIVATE_KEY|CREDENTIALS?|AUTH|DSN|` +
      String.raw`WEBHOOK_URL)[A-Z0-9_]*\s*[=:]\s*(?:\\["']|["'])?(?<v>[^\s"'\\]{8,})`,
  ),
];

/** ASCII-only lower case: the same length, so spans line up with the original. */
const asciiLower = (text: string) => text.replace(/[A-Z]+/g, (run) => run.toLowerCase());

const plausible = (secret: string) => {
  if (secret.replaceAll("*", "") === "") return false;
  return (/[A-Za-z]/.test(secret) && /[0-9]/.test(secret)) || secret.length >= 24;
};

/** `find_secrets`: the names of the p1002 patterns that match, once per match. */
const p1002Findings = function* (value: string) {
  let lowered: string | undefined;
  for (const pattern of P1002_PATTERNS) {
    const buffer = pattern.lower === true ? (lowered ??= asciiLower(value)) : value;
    for (const match of buffer.matchAll(pattern.pattern)) {
      const matchStart = match.index;
      const matchEnd = matchStart + match[0].length;
      if (pattern.check !== undefined && !pattern.check(buffer, matchStart, matchEnd)) continue;
      if (pattern.kind === "token") {
        const start = escapeFreeStart(value, matchStart + (pattern.spanFrom ?? 0));
        if (start >= matchEnd || value.slice(start, matchEnd).replaceAll("*", "") === "") continue;
        yield pattern.name;
      } else {
        const span = match.indices?.groups?.v;
        if (span === undefined) continue;
        const start = escapeFreeStart(value, span[0]);
        if (start < span[1] && plausible(value.slice(start, span[1]))) yield pattern.name;
      }
    }
  }
};

const ANSI = /\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07]*\x07|\x1b[@-_]/g;
const EXTRA: ReadonlyArray<readonly [string, RegExp]> = [
  ["slack xoxc/xoxd", /(?<![A-Za-z0-9])xox[cd]-[A-Za-z0-9-]{10,}/dg],
  ["slack xapp", /(?<![A-Za-z0-9])xapp-[0-9]-[A-Za-z0-9-]{10,}/dg],
  ["google ya29", /(?<![A-Za-z0-9])ya29\.[0-9A-Za-z_-]{20,}/dg],
  ["atlassian ATATT/ATCTT", /(?<![A-Za-z0-9])AT(?:ATT|CTT)3[A-Za-z0-9_\-=]{30,}/dg],
  ["authorization basic", /authori[sz]ation["']?\s*[:=]\s*["']?basic\s+([A-Za-z0-9+/=]{8,})/dgi],
  [
    "url credentials (any user, no filter)",
    /[A-Za-z][A-Za-z0-9+.-]{1,20}:\/\/([^\s/:@"'<>\\]{0,128}):([^\s/@"'<>\\]{2,})@[A-Za-z0-9.\-[]/dg,
  ],
];
const KEYWORD_ASSIGN =
  /(token|secret|password|passwd|pwd|refresh[_-]?token|access[_-]?key|session[_-]?token|api[_-]?key|apikey|client[_-]?secret|private[_-]?key)["']?\s*[:=]\s*["']?([^\s"'\\,;)}\]]{8,})/dg;
const ENV_ASSIGN =
  /(?:PASSWORD|PASSWD|SECRET|TOKEN|ACCESS_KEY|API_KEY|APIKEY|PRIVATE_KEY|CREDENTIALS?)[A-Z0-9_]{0,40}\s*[=:]\s*["']?([^\s"'\\]{8,})/dg;
/** A name that makes a high-entropy value next to it count as a credential. */
export const NAMEY = /key|token|secret|passw|pwd|credential|auth|cookie|session|private/i;
const NAME_VALUE =
  /(key|token|secret|passw|pwd|credential|auth|cookie|session|private)[a-z0-9_.-]{0,30}["']?\s*[:=]\s*["']?([a-z0-9+/=_-]{24,})/dg;
const B64LINE = /^[ +-]?[A-Za-z0-9+/]{60,76}={0,2}$/;
const PEM_MARK = /-----(BEGIN|END)[A-Z0-9 ]{0,40}PRIVATE KEY(?: BLOCK)?-----/g;
const TOKEN_CHARSET = /^[A-Za-z0-9+/=_-]{24,}$/;

const entropy = (text: string) => {
  const counts = new Map<string, number>();
  const characters = [...text];
  for (const character of characters) counts.set(character, (counts.get(character) ?? 0) + 1);
  let bits = 0;
  for (const n of counts.values())
    bits -= (n / characters.length) * Math.log2(n / characters.length);
  return bits;
};

const classes = (text: string) =>
  [/\p{Ll}/u, /\p{Lu}/u, /\p{Nd}/u].filter((pattern) => pattern.test(text)).length;

const countedValue = (value: string) =>
  value.replace(/^\*+|\*+$/g, "") !== "" && !/^[$%<{]/.test(value);

const strict = (value: string) =>
  ([...value].length >= 12 && /\p{L}/u.test(value) && /\p{Nd}/u.test(value)) ||
  [...value].length >= 20;

/** 24+ token characters, two or more character classes, at least 4 bits per character. */
export const highEntropy = (value: string) =>
  [...value].length >= 24 &&
  TOKEN_CHARSET.test(value) &&
  countedValue(value) &&
  classes(value) >= 2 &&
  entropy(value) >= 4;

/** The span of a match's last participating group (Python's `m.lastindex`), else the match. */
const lastGroup = (match: RegExpMatchArray) => {
  for (let index = match.length - 1; index >= 1; index--) {
    const span = match.indices?.[index];
    if (match[index] !== undefined && span !== undefined) {
      return { text: match[index] as string, start: span[0] };
    }
  }
  return { text: match[0], start: match.index ?? 0 };
};

const checkText = (raw: string, scope: string, report: (row: string) => void) => {
  const text = raw.includes("\x1b") ? raw.replace(ANSI, " ") : raw;
  const low = asciiLower(text);
  for (const name of p1002Findings(text)) report(`${scope}|p1002:${name}`);
  for (const [name, pattern] of EXTRA) {
    for (const match of text.matchAll(pattern)) {
      if (countedValue(lastGroup(match).text)) report(`${scope}|extra:${name}`);
    }
  }
  for (const [pattern, label, buffer] of [
    [KEYWORD_ASSIGN, "keyword assignment", low],
    [ENV_ASSIGN, "env-var assignment", text],
  ] as const) {
    for (const match of buffer.matchAll(pattern)) {
      const group = lastGroup(match);
      const value = text.slice(group.start, group.start + group.text.length);
      if (!countedValue(value)) continue;
      report(`${scope}|${label} (loose)`);
      if (strict(value)) report(`${scope}|${label} (strict)`);
    }
  }
  for (const match of low.matchAll(NAME_VALUE)) {
    const group = lastGroup(match);
    if (highEntropy(text.slice(group.start, group.start + group.text.length))) {
      report(`${scope}|entropy: name=value in text`);
    }
  }
  for (const match of text.matchAll(PEM_MARK)) report(`${scope}|pem: ${match[1]} marker`);
  for (const line of text.split("\n")) {
    if (line.length >= 60 && line.length <= 79 && B64LINE.test(line)) {
      report(`${scope}|pem: base64 60-76 col lines`);
    }
  }
};

const walk = (
  node: unknown,
  report: (row: string) => void,
  leaves: Array<string>,
  depth: number,
): void => {
  if (Array.isArray(node)) {
    let lines = 0;
    for (const item of node) {
      walk(item, report, leaves, depth);
      if (typeof item === "string" && B64LINE.test(item.replace(/\n+$/, ""))) lines++;
    }
    if (lines >= 3) report("decoded|pem: array with >= 3 base64 key-body lines");
  } else if (typeof node === "object" && node !== null) {
    for (const [key, value] of Object.entries(node)) {
      if (typeof value === "string" && NAMEY.test(key) && highEntropy(value)) {
        report("decoded|entropy: JSON key/value");
      }
      leaves.push(key);
      walk(value, report, leaves, depth);
    }
  } else if (typeof node === "string") {
    if (depth < 4 && node.length > 1 && (node[0] === "{" || node[0] === "[")) {
      let inner: unknown = null;
      try {
        inner = JSON.parse(node);
      } catch {
        inner = null;
      }
      if (typeof inner === "object" && inner !== null) {
        walk(inner, report, leaves, depth + 1);
        return;
      }
    }
    leaves.push(node);
  }
};

/** Every finding in one stored value, by `scope|kind` row. */
export const extendedFindings = (stored: string, report: (row: string) => void): void => {
  if (stored === "") return;
  checkText(stored, "raw", report);
  if (stored[0] !== "{" && stored[0] !== "[") return;
  let node: unknown = null;
  try {
    node = JSON.parse(stored);
  } catch {
    return;
  }
  if (typeof node !== "object" || node === null) return;
  const leaves: Array<string> = [];
  walk(node, report, leaves, 0);
  checkText(leaves.join("\n"), "decoded", report);
};

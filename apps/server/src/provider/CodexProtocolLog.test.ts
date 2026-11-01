import { assert, it } from "@effect/vitest";

import { codexProtocolLogRecord } from "./CodexProtocolLog.ts";
import { boundProviderEventForLogging } from "./EventNdjsonRecord.ts";

// The redaction as it was before key checks were cached.
const isSensitiveKey = (key: string) => {
  const normalized = key.replace(/[^a-z0-9]/gi, "").toLowerCase();
  return ["authorization", "apikey", "token", "password", "secret"].some((suffix) =>
    normalized.endsWith(suffix),
  );
};
const reference = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(reference);
  if (value === null || typeof value !== "object") {
    if (typeof value !== "string") return value;
    if (/^Bearer\s+/i.test(value)) return "[REDACTED]";
    const trimmed = value.trim();
    if (
      !(
        (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
        (trimmed.startsWith("[") && trimmed.endsWith("]"))
      )
    ) {
      return value;
    }
    try {
      return JSON.stringify(reference(boundProviderEventForLogging(JSON.parse(trimmed))));
    } catch {
      return value;
    }
  }
  return Object.fromEntries(
    Object.entries(value).map(([key, nested]) => [
      key,
      isSensitiveKey(key) ? "[REDACTED]" : reference(nested),
    ]),
  );
};

const keys = [
  "Authorization",
  "proxy-authorization",
  "x-api-key",
  "API_KEY",
  "apiKeys",
  "accessToken",
  "refresh_token",
  "tokens",
  "tokenCount",
  "Password",
  "client.secret",
  "secretName",
  "ſecret",
  "id",
  `${"k".repeat(300)}_token`,
  "k".repeat(300),
];

const payload = (salt: string) => ({
  method: "thread/event",
  params: {
    ...Object.fromEntries(keys.map((key) => [key, `${key}-${salt}`])),
    nested: [{ headers: { Authorization: "Bearer x", "X-Token": salt } }],
    raw: JSON.stringify({ api_key: salt, keep: [1, { password: salt }] }),
    bearer: "bearer abc",
  },
});

it("redacts Codex protocol events exactly as before, including after many distinct keys", () => {
  const check = (salt: string) => {
    const event = { direction: "incoming", stage: "decoded", payload: payload(salt) } as const;
    const record = codexProtocolLogRecord(event, "session-1");
    assert.deepEqual(record, {
      provider: "codex",
      protocol: "codex.app-server",
      kind: "protocol",
      providerSessionId: "session-1",
      event: reference(boundProviderEventForLogging(event)),
    });
  };

  check("first");
  // Distinct keys past the cache limit evict it; the same keys still redact the same.
  for (let index = 0; index < 5000; index++) {
    const key = index % 2 === 0 ? `field${index}Token` : `field${index}`;
    const event = {
      direction: "incoming",
      stage: "decoded",
      payload: { method: "x/y", params: { [key]: "v" } },
    } as const;
    assert.deepEqual(codexProtocolLogRecord(event, "s"), {
      provider: "codex",
      protocol: "codex.app-server",
      kind: "protocol",
      providerSessionId: "s",
      event: reference(event),
    });
  }
  check("again");
});

it("skips the events the native log does not keep", () => {
  assert.isUndefined(
    codexProtocolLogRecord({ direction: "incoming", stage: "raw", payload: "{}" }, "s"),
  );
  assert.isUndefined(
    codexProtocolLogRecord(
      {
        direction: "incoming",
        stage: "decoded",
        payload: { method: "item/agentMessage/delta", params: {} },
      },
      "s",
    ),
  );
});

import { pipe } from "effect/Function";
import type { CodexAppServerProtocolLogEvent } from "effect-codex-app-server/protocol";

import { boundProviderEventForLogging, shouldPersistProviderEvent } from "./EventNdjsonRecord.ts";

/** The native-log record of a Codex protocol event, or undefined when it is not persisted. */
export function codexProtocolLogRecord(
  event: CodexAppServerProtocolLogEvent,
  providerSessionId: string,
): object | undefined {
  if (!shouldPersistProviderEvent("native", event)) return undefined;
  return {
    provider: "codex",
    protocol: "codex.app-server",
    kind: "protocol",
    providerSessionId,
    event: redactCodexProtocolValue(boundProviderEventForLogging(event)),
  };
}

// Plain objects and arrays with nothing to redact come back as they are, so the native log
// reuses the size check made on them. Frames must hold plain data properties only: the log reads
// the original again, so an accessor could hand it a secret.
function redactCodexProtocolValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    const redacted = value.map(redactCodexProtocolValue);
    for (let index = 0; index < value.length; index += 1) {
      if (!Object.prototype.propertyIsEnumerable.call(value, index)) return redacted;
      if (redacted[index] !== value[index]) return redacted;
    }
    // Own keys must be just the indices and length, as on the copy.
    return Object.getPrototypeOf(value) === Array.prototype &&
      Object.getOwnPropertyNames(value).length === value.length + 1
      ? value
      : redacted;
  }
  if (value === null || typeof value !== "object") {
    if (typeof value !== "string") {
      return value;
    }
    if (/^Bearer\s+/i.test(value)) {
      return "[REDACTED]";
    }
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
      return pipe(
        JSON.parse(trimmed) as unknown,
        boundProviderEventForLogging,
        redactCodexProtocolValue,
        JSON.stringify,
      );
    } catch {
      return value;
    }
  }
  const entries = Object.entries(value);
  const redacted = entries.map(([key, nested]): [string, unknown] => [
    key,
    isSensitiveCodexProtocolKey(key) ? "[REDACTED]" : redactCodexProtocolValue(nested),
  ]);
  return Object.getPrototypeOf(value) === Object.prototype &&
    redacted.every(([, nested], index) => nested === entries[index]?.[1]) &&
    Object.getOwnPropertyNames(value).length === entries.length
    ? value
    : Object.fromEntries(redacted);
}

const SENSITIVE_KEY_CACHE_LIMIT = 4096;
const SENSITIVE_KEY_CACHE_MAX_LENGTH = 256;
// Frames repeat a small set of keys; bounded because keys can also be data (e.g. paths).
const sensitiveKeys = new Map<string, boolean>();

function isSensitiveCodexProtocolKey(key: string): boolean {
  const cached = sensitiveKeys.get(key);
  if (cached !== undefined) return cached;
  const normalized = key.replace(/[^a-z0-9]/gi, "").toLowerCase();
  const sensitive =
    normalized.endsWith("authorization") ||
    normalized.endsWith("apikey") ||
    normalized.endsWith("token") ||
    normalized.endsWith("password") ||
    normalized.endsWith("secret");
  if (key.length > SENSITIVE_KEY_CACHE_MAX_LENGTH) return sensitive;
  if (sensitiveKeys.size >= SENSITIVE_KEY_CACHE_LIMIT) sensitiveKeys.clear();
  sensitiveKeys.set(key, sensitive);
  return sensitive;
}

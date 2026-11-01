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

function redactCodexProtocolValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(redactCodexProtocolValue);
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
  return Object.fromEntries(
    Object.entries(value).map(([key, nested]) => [
      key,
      isSensitiveCodexProtocolKey(key) ? "[REDACTED]" : redactCodexProtocolValue(nested),
    ]),
  );
}

const SENSITIVE_KEY_CACHE_LIMIT = 4096;
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
  if (sensitiveKeys.size >= SENSITIVE_KEY_CACHE_LIMIT) sensitiveKeys.clear();
  sensitiveKeys.set(key, sensitive);
  return sensitive;
}

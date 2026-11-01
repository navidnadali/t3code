import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

const MAX_RECORD_CHARACTERS = 64 * 1024;
const MAX_RECORD_FIELDS = 1_024;
const MAX_RECORD_DEPTH = 16;
const encodeUnknownJsonString = Schema.encodeUnknownResult(Schema.fromJsonString(Schema.Unknown));

const transientCanonicalEventTypes = new Set([
  "content.delta",
  "hook.progress",
  "item.updated",
  "task.progress",
  "thread.realtime.audio.delta",
  "tool.progress",
  "turn.proposed.delta",
]);
const transientNativeMethods = new Set([
  "item/agentMessage/delta",
  "item/commandExecution/outputDelta",
  "item/fileChange/outputDelta",
  "item/plan/delta",
  "item/reasoning/summaryTextDelta",
  "item/reasoning/textDelta",
  "thread/realtime/outputAudio/delta",
  "thread/realtime/transcript/delta",
  "turn/diff/updated",
]);
const transientAcpUpdates = new Set(["agent_message_chunk", "agent_thought_chunk"]);

export type EventNdjsonStream = "native" | "canonical" | "orchestration";

export type SerializedEventNdjsonRecord =
  | { readonly _tag: "Skipped" }
  | { readonly _tag: "Serialized"; readonly payload: string }
  | { readonly _tag: "Failed"; readonly error: Schema.SchemaError };

const skipped: SerializedEventNdjsonRecord = { _tag: "Skipped" };

function resolveStreamLabel(stream: EventNdjsonStream): string {
  return stream === "native" ? "NTIVE" : stream === "orchestration" ? "ORCH" : "CANON";
}

export function shouldPersistProviderEvent(stream: EventNdjsonStream, event: unknown): boolean {
  if (stream === "orchestration" || typeof event !== "object" || event === null) {
    return true;
  }
  try {
    const type = Reflect.get(event, "type");
    if (typeof type === "string" && transientCanonicalEventTypes.has(type)) {
      return false;
    }
    if (stream !== "native") return true;

    const nested = Reflect.get(event, "event");
    const envelope = typeof nested === "object" && nested !== null ? nested : event;
    // Decoded frames preserve raw-frame information without duplicating every token delta.
    if (Reflect.get(envelope, "stage") === "raw") return false;
    const decodedPayload = Reflect.get(envelope, "payload");
    const nativeEvent =
      Reflect.get(envelope, "stage") === "decoded" &&
      typeof decodedPayload === "object" &&
      decodedPayload !== null
        ? decodedPayload
        : envelope;
    const method = Reflect.get(nativeEvent, "method");
    if (
      typeof method === "string" &&
      (transientNativeMethods.has(method) ||
        method.startsWith("claude/stream_event/content_block_delta/"))
    ) {
      return false;
    }

    const nativeType = Reflect.get(nativeEvent, "type");
    if (nativeType === "system" && Reflect.get(nativeEvent, "subtype") === "thinking_tokens") {
      return false;
    }
    if (nativeType === "message.part.delta") return false;
    if (nativeType === "stream_event") {
      const streamEvent = Reflect.get(nativeEvent, "event");
      if (
        typeof streamEvent === "object" &&
        streamEvent !== null &&
        Reflect.get(streamEvent, "type") === "content_block_delta"
      ) {
        return false;
      }
    }

    const payload = Reflect.get(nativeEvent, "payload");
    if (typeof payload !== "object" || payload === null) return true;

    if (method === "session/update") {
      const update = Reflect.get(payload, "update");
      if (typeof update !== "object" || update === null) return true;
      const updateType = Reflect.get(update, "sessionUpdate");
      return typeof updateType !== "string" || !transientAcpUpdates.has(updateType);
    }

    if (nativeType === "message.part.updated") {
      const properties = Reflect.get(payload, "properties");
      if (typeof properties !== "object" || properties === null) return true;
      const part = Reflect.get(properties, "part");
      if (typeof part !== "object" || part === null) return true;
      const partType = Reflect.get(part, "type");
      if (partType === "text" || partType === "reasoning") return false;
      if (partType === "tool") {
        // Running snapshots repeat growing output.
        const state = Reflect.get(part, "state");
        if (typeof state === "object" && state !== null) {
          return Reflect.get(state, "status") !== "running";
        }
      }
      return true;
    }

    return true;
  } catch {
    return true;
  }
}

const summaryFields = [
  "provider",
  "protocol",
  "kind",
  "providerSessionId",
  "direction",
  "stage",
  "type",
  "subtype",
  "method",
  "id",
  "threadId",
  "turnId",
  "requestId",
  "session_id",
  "status",
  "is_error",
  "api_error_status",
  "terminal_reason",
  "stop_reason",
  "operation",
  "code",
  "willRetry",
  "message",
  "event",
  "payload",
  "params",
  "result",
  "thread",
  "turn",
  "error",
  "turns",
  "items",
  "content",
] as const;

function summarizeProviderEvent(event: unknown): unknown {
  let remainingFields = 128;
  let remainingCharacters = 8 * 1024;
  const summarize = (value: unknown, depth: number): unknown => {
    if (typeof value === "string") {
      if (value.length > Math.min(1_024, remainingCharacters)) {
        return { omittedCharacters: value.length };
      }
      remainingCharacters -= value.length;
      return value;
    }
    if (value === null || typeof value === "number" || typeof value === "boolean") return value;
    if (typeof value !== "object") return undefined;
    if (Array.isArray(value)) return { itemCount: value.length };
    const summary: Record<string, unknown> = { truncated: true };
    if (depth >= 6) return summary;
    for (const key of summaryFields) {
      if (remainingFields <= 0) break;
      const nested = Reflect.get(value, key);
      if (nested === undefined) continue;
      remainingFields -= 1;
      summary[key] = summarize(nested, depth + 1);
    }
    return summary;
  };
  try {
    return summarize(event, 0);
  } catch {
    return { truncated: true };
  }
}

export function boundProviderEventForLogging(event: unknown): unknown {
  let remainingCharacters = MAX_RECORD_CHARACTERS;
  let remainingFields = MAX_RECORD_FIELDS;
  const ancestors = new WeakSet<object>();
  const fits = (value: unknown, depth: number): boolean => {
    if (typeof value === "string") {
      remainingCharacters -= value.length;
      return remainingCharacters >= 0;
    }
    if (typeof value !== "object" || value === null) return true;
    if (depth > MAX_RECORD_DEPTH || ancestors.has(value)) return false;
    if (Array.isArray(value) && value.length > remainingFields) return false;
    ancestors.add(value);
    for (const key in value) {
      if (!Object.hasOwn(value, key)) continue;
      remainingFields -= 1;
      remainingCharacters -= key.length;
      if (
        remainingFields < 0 ||
        remainingCharacters < 0 ||
        !fits(Reflect.get(value, key), depth + 1)
      )
        return false;
    }
    ancestors.delete(value);
    return true;
  };
  try {
    if (fits(event, 0)) return event;
  } catch {}
  return summarizeProviderEvent(event);
}

const serialize = (event: unknown): SerializedEventNdjsonRecord => {
  const encoded = encodeUnknownJsonString(event);
  return Result.isFailure(encoded)
    ? { _tag: "Failed", error: encoded.failure }
    : { _tag: "Serialized", payload: encoded.success };
};

export function serializeEventNdjsonRecord(
  stream: EventNdjsonStream,
  event: unknown,
): SerializedEventNdjsonRecord {
  if (!shouldPersistProviderEvent(stream, event)) return skipped;
  const record = serialize(boundProviderEventForLogging(event));
  // Escaping can expand strings beyond their input size.
  if (record._tag === "Serialized" && Buffer.byteLength(record.payload) > MAX_RECORD_CHARACTERS) {
    return serialize(summarizeProviderEvent(event));
  }
  return record;
}

export function formatEventNdjsonLine(
  observedAt: string,
  stream: EventNdjsonStream,
  payload: string,
): string {
  return `[${observedAt}] ${resolveStreamLabel(stream)}: ${payload}\n`;
}

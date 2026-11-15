import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import * as CodexRpc from "./_generated/meta.gen.ts";
import * as CodexError from "./errors.ts";
import { isJsonRpcId, isObject } from "./_internal/shared.ts";
import type {
  CodexAppServerIncomingNotification,
  CodexAppServerIncomingRequest,
} from "./protocol.ts";

// Bound JSONL lines before joining and parsing; the LF delimiter is excluded.
export const MAX_INCOMING_MESSAGE_BYTES = 128 * 1024 * 1024;

export interface FramedLines {
  readonly lines: ReadonlyArray<string>;
  /** The line being read passed the byte limit; the framer stays in this state. */
  readonly exceeded: boolean;
}

export interface FramedTail {
  /** The unterminated tail, without CR stripping; empty once the line passed the byte limit. */
  readonly tail: string;
  readonly exceeded: boolean;
}

export interface LineFramer {
  readonly push: (chunk: Uint8Array) => FramedLines;
  /** At end of input, as upstream's `Stream.decodeText`: a cut UTF-8 sequence ends the tail as U+FFFD. */
  readonly end: () => FramedTail;
}

export function makeLineFramer(maxLineBytes: number = MAX_INCOMING_MESSAGE_BYTES): LineFramer {
  const decoder = new TextDecoder();
  const remainder: Array<string> = [];
  let remainderBytes = 0;
  let exceeded = false;
  const frame = (chunk: string): FramedLines => {
    const lines: Array<string> = [];
    let start = 0;
    while (start < chunk.length) {
      const newline = chunk.indexOf("\n", start);
      const fragment = chunk.slice(start, newline === -1 ? chunk.length : newline);
      // The streaming decoder holds back split UTF-8 sequences, so fragment byte sums are exact.
      remainderBytes += Buffer.byteLength(fragment, "utf8");
      if (remainderBytes > maxLineBytes) {
        remainder.length = 0;
        exceeded = true;
        break;
      }
      remainder.push(fragment);
      if (newline === -1) break;
      lines.push(remainder.join("").replace(/\r$/, ""));
      remainder.length = 0;
      remainderBytes = 0;
      start = newline + 1;
    }
    return { lines, exceeded };
  };
  return {
    push: (bytes) =>
      exceeded ? { lines: [], exceeded } : frame(decoder.decode(bytes, { stream: true })),
    end: () => {
      // The flush holds no LF, only the U+FFFD of a sequence cut at EOF; it counts toward the limit.
      if (!exceeded) frame(decoder.decode());
      const tail = remainder.join("");
      remainder.length = 0;
      remainderBytes = 0;
      return { tail, exceeded };
    },
  };
}

const decodeJsonString = Schema.decodeUnknownResult(Schema.fromJsonString(Schema.Unknown));

export const decodeWireLine = (
  line: string,
): Result.Result<unknown, CodexError.CodexAppServerProtocolParseError> => {
  // The schema runs this same JSON.parse; it is only needed to build the error.
  try {
    const message: unknown = JSON.parse(line);
    return Result.succeed(message);
  } catch {
    return Result.mapError(decodeJsonString(line), (cause) =>
      CodexError.CodexAppServerProtocolParseError.fromSchemaError("decode-wire-message", cause),
    );
  }
};

export const decodeFailedLogPayload = (error: CodexError.CodexAppServerProtocolParseError) => ({
  operation: error.operation,
  ...(error.method === undefined ? {} : { method: error.method }),
  ...(error.requestId === undefined ? {} : { requestId: error.requestId }),
  ...(error.issueCount === undefined ? {} : { issueCount: error.issueCount }),
  ...(error.issueKinds === undefined ? {} : { issueKinds: error.issueKinds }),
  ...(error.maximumPathDepth === undefined ? {} : { maximumPathDepth: error.maximumPathDepth }),
});

export function isIncomingRequest(value: unknown): value is CodexAppServerIncomingRequest {
  if (!isObject(value) || typeof value.method !== "string") {
    return false;
  }
  return isJsonRpcId(value.id);
}

export function isIncomingNotification(
  value: unknown,
): value is CodexAppServerIncomingNotification {
  return isObject(value) && typeof value.method === "string" && !("id" in value);
}

export const decodeNotificationPayload = <A, I>(
  method: string,
  schema: Schema.Codec<A, I>,
  raw: unknown,
): Result.Result<A, CodexError.CodexAppServerProtocolParseError> =>
  Result.mapError(Schema.decodeUnknownResult(schema)(raw), (error) =>
    CodexError.CodexAppServerProtocolParseError.fromRequestError(
      "decode-notification-payload",
      method,
      CodexError.CodexAppServerRequestError.invalidPayload(method, "decode-payload", error),
    ),
  );

export const serverNotificationParamsSchema = (method: string) =>
  method in CodexRpc.SERVER_NOTIFICATION_PARAMS
    ? CodexRpc.SERVER_NOTIFICATION_PARAMS[method as CodexRpc.ServerNotificationMethod]
    : undefined;

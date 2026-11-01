import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import type * as RpcMessage from "effect/rpc/RpcMessage";
import * as RpcSerialization from "effect/rpc/RpcSerialization";

import * as AcpSchema from "./schema.ts";
import * as AcpSchemaV1 from "./_generated/schema-v1.gen.ts";
import * as AcpError from "./errors.ts";
import type { AcpStageTiming } from "./protocol.ts";

export type WireMessage = RpcMessage.FromClientEncoded | RpcMessage.FromServerEncoded;

export const decodeSessionUpdate = Schema.decodeUnknownResult(
  Schema.Union([
    // Claude ACP v2 reports prompt failures on idle; alpha.3 omits that field.
    Schema.Struct({
      ...AcpSchema.UpdateSessionNotification.fields,
      update: Schema.Struct({
        sessionUpdate: Schema.Literal("state_update"),
        state: Schema.Literal("idle"),
        stopReason: Schema.optionalKey(Schema.NullOr(AcpSchema.StopReason)),
        usage: Schema.optionalKey(Schema.NullOr(AcpSchema.Usage)),
        error: Schema.optionalKey(Schema.NullOr(AcpSchema.Error)),
        _meta: AcpSchema.UpdateSessionNotification.fields._meta,
      }),
    }),
    AcpSchema.UpdateSessionNotification,
    AcpSchemaV1.SessionNotification,
  ]),
);
export const decodeElicitationComplete = Schema.decodeUnknownResult(
  AcpSchema.CompleteElicitationNotification,
);
// ndJsonRpc skips lines that are not JSON. A malformed agent line has to end the
// session, so frames are split here and each one goes through the strict codec.
export const makeStrictNdJsonRpcParser = (timing?: AcpStageTiming) => {
  const codec = RpcSerialization.jsonRpc().makeUnsafe();
  const decoder = new TextDecoder();
  let buffer = "";
  return {
    decode: (bytes: Uint8Array | string): ReadonlyArray<unknown> => {
      const startedAt = timing?.now() ?? 0;
      buffer += typeof bytes === "string" ? bytes : decoder.decode(bytes, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      if (timing === undefined) return lines.flatMap((line) => codec.decode(line));
      timing.record(
        "framing",
        timing.now() - startedAt,
        typeof bytes === "string" ? Buffer.byteLength(bytes, "utf8") : bytes.byteLength,
      );
      return lines.flatMap((line) => {
        const lineBytes = Buffer.byteLength(line, "utf8");
        const lineStartedAt = timing.now();
        const messages = codec.decode(line);
        timing.record("json", timing.now() - lineStartedAt, lineBytes);
        return messages;
      });
    },
    encode: (response: Parameters<typeof codec.encode>[0]) => {
      const encoded = codec.encode(response);
      return encoded === undefined ? undefined : `${encoded}\n`;
    },
  };
};
export type StrictNdJsonRpcParser = ReturnType<typeof makeStrictNdJsonRpcParser>;

/** A malformed line fails the whole chunk, so none of its messages are routed. */
export const decodeWireChunk = (
  parser: StrictNdJsonRpcParser,
  data: Uint8Array | string,
): Result.Result<ReadonlyArray<WireMessage>, AcpError.AcpProtocolParseError> =>
  Result.try({
    try: () => (parser.decode(data) as ReadonlyArray<WireMessage>).map(normalizeAcpJsonRpcError),
    catch: (cause) =>
      new AcpError.AcpProtocolParseError({
        operation: "decode-wire-message",
        cause,
      }),
  });

export const decodeFailureDiagnostics = (error: AcpError.AcpProtocolParseError) => ({
  operation: error.operation,
  ...(error.method === undefined ? {} : { method: error.method }),
  ...(error.requestId === undefined ? {} : { requestId: error.requestId }),
  ...(error.issueCount === undefined ? {} : { issueCount: error.issueCount }),
  ...(error.issueKinds === undefined ? {} : { issueKinds: error.issueKinds }),
  ...(error.maximumPathDepth === undefined ? {} : { maximumPathDepth: error.maximumPathDepth }),
  ...(error.issueSummary === undefined ? {} : { issueSummary: error.issueSummary }),
});

export const isProtocolError = Schema.is(AcpSchema.Error);

/**
 * Effect RPC's JSON-RPC codec treats a standard JSON-RPC error object as a
 * defect unless it carries Effect's private `_tag: "Cause"` envelope.
 */
function normalizeAcpJsonRpcError(message: WireMessage): WireMessage {
  if (message._tag !== "Exit" || message.exit._tag !== "Failure") return message;
  const [failure] = message.exit.cause;
  if (
    message.exit.cause.length !== 1 ||
    failure?._tag !== "Die" ||
    !isProtocolError(failure.defect)
  ) {
    return message;
  }
  return {
    ...message,
    exit: {
      _tag: "Failure",
      cause: [{ _tag: "Fail", error: failure.defect }],
    },
  };
}

/** Splits stdout into complete lines as `makeStrictNdJsonRpcParser` does. */
export const makeLineSplitter = () => {
  const decoder = new TextDecoder();
  let buffer = "";
  return (bytes: Uint8Array | string): ReadonlyArray<string> => {
    buffer += typeof bytes === "string" ? bytes : decoder.decode(bytes, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    return lines;
  };
};

/** A JSON-RPC batch: its request ids are state the encoder of the responses must hold. */
export const isBatchLine = (line: string) => /^[ \t\r]*\[/.test(line);

/** Finishes the messages of a line `makeOffloadedLineDecoder` decoded, as `decodeWireChunk` does. */
export const normalizeOffloadedMessages = (
  messages: ReadonlyArray<WireMessage>,
): Result.Result<ReadonlyArray<WireMessage>, AcpError.AcpProtocolParseError> =>
  Result.try({
    try: () => messages.map(normalizeAcpJsonRpcError),
    catch: (cause) =>
      new AcpError.AcpProtocolParseError({
        operation: "decode-wire-message",
        cause,
      }),
  });

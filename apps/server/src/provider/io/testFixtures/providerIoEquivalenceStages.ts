import * as AcpWire from "effect-acp/wire";
import * as CodexWire from "effect-codex-app-server/wire";
import * as Result from "effect/Result";
import type * as Schema from "effect/Schema";

import { serializeEventNdjsonRecord } from "../../EventNdjsonRecord.ts";
import type { ProviderIoStage } from "../providerIoProtocol.ts";

// The provider stages P1-023 made pure, composed the way a worker would run them: framing, JSON,
// schema decode and protocol-log record building. Terminal conditions throw, as they end the session.

export type LogRecord =
  | { readonly _tag: "Skipped" }
  | { readonly _tag: "Serialized"; readonly payload: string }
  | { readonly _tag: "Failed"; readonly error: string };

const logRecord = (event: unknown): LogRecord => {
  const record = serializeEventNdjsonRecord("native", event);
  return record._tag === "Failed" ? { _tag: "Failed", error: record.error.message } : record;
};

const incoming = (provider: string, stage: string, payload: unknown) => ({
  provider,
  kind: "protocol",
  event: { direction: "incoming", stage, payload },
});

export interface CodexWireOptions {
  readonly maxLineBytes?: number | undefined;
}

const decodeCodexLine = (line: string) => {
  const raw = logRecord(incoming("codex", "raw", line));
  const decoded = CodexWire.decodeWireLine(line);
  if (Result.isFailure(decoded)) {
    const failure = CodexWire.decodeFailedLogPayload(decoded.failure);
    return {
      type: "decode-failed",
      line,
      failure,
      logs: [raw, logRecord(incoming("codex", "decode_failed", failure))],
    } as const;
  }
  const message = decoded.success;
  const logs = [raw, logRecord(incoming("codex", "decoded", message))];
  if (CodexWire.isIncomingRequest(message)) return { type: "request", message, logs } as const;
  if (CodexWire.isIncomingNotification(message)) {
    const schema = CodexWire.serverNotificationParamsSchema(message.method);
    if (schema === undefined) {
      return { type: "notification", message, params: { unknown: message.params }, logs } as const;
    }
    const params = CodexWire.decodeNotificationPayload(message.method, schema, message.params);
    return {
      type: "notification",
      message,
      params: Result.isSuccess(params)
        ? { decoded: params.success }
        : { dropped: CodexWire.decodeFailedLogPayload(params.failure) },
      logs,
    } as const;
  }
  return { type: "response-or-unroutable", message, logs } as const;
};

export type CodexWireFrame = ReturnType<typeof decodeCodexLine>;

const makeCodexWireStage = (
  makeFramer: (maxLineBytes?: number) => CodexWire.LineFramer,
): ProviderIoStage<CodexWireOptions, CodexWireFrame> => ({
  name: "equivalence-codex-wire",
  open: ({ maxLineBytes }) => {
    const framer = makeFramer(maxLineBytes);
    const handle = (line: string, emit: (frame: CodexWireFrame) => void) => {
      if (line.trim().length === 0) return;
      const frame = decodeCodexLine(line);
      emit(frame);
      if (frame.type === "decode-failed") throw new Error(`decode-failed: ${frame.line}`);
    };
    return {
      push: (chunk, emit) => {
        const framed = framer.push(chunk);
        for (const line of framed.lines) handle(line, emit);
        if (framed.exceeded) throw new Error(`Incoming message exceeded ${maxLineBytes} bytes.`);
      },
      end: (emit) => handle(framer.end(), emit),
    };
  },
});

const notificationParams = <A>(decoded: Result.Result<A, Schema.SchemaError>) =>
  Result.isSuccess(decoded) ? { decoded: decoded.success } : { dropped: decoded.failure.message };

const acpFrame = (message: AcpWire.WireMessage) => {
  const log = logRecord(incoming("acp", "decoded", message));
  if (message._tag !== "Request" || message.id !== "") return { message, log };
  if (message.tag === "session/update") {
    return {
      message,
      log,
      params: notificationParams(AcpWire.decodeSessionUpdate(message.payload)),
    };
  }
  if (message.tag === "elicitation/complete" || message.tag === "session/elicitation/complete") {
    const params = notificationParams(AcpWire.decodeElicitationComplete(message.payload));
    return { message, log, params };
  }
  return { message, log };
};

export type AcpWireFrame = ReturnType<typeof acpFrame>;

const makeAcpWireStage = (
  makeParser: () => AcpWire.StrictNdJsonRpcParser,
): ProviderIoStage<void, AcpWireFrame> => ({
  name: "equivalence-acp-wire",
  open: () => {
    const parser = makeParser();
    return {
      push: (chunk, emit) => {
        const decoded = AcpWire.decodeWireChunk(parser, chunk);
        if (Result.isFailure(decoded)) {
          const cause = decoded.failure.cause;
          throw new Error(
            JSON.stringify({
              ...AcpWire.decodeFailureDiagnostics(decoded.failure),
              cause: cause instanceof Error ? cause.message : String(cause),
            }),
          );
        }
        for (const message of decoded.success) emit(acpFrame(message));
      },
      end: () => {},
    };
  },
});

export interface ClaudeSdkLogFrame {
  readonly line: string;
  readonly log: LogRecord;
}

export const claudeSdkLogStage: ProviderIoStage<void, ClaudeSdkLogFrame> = {
  name: "equivalence-claude-sdk-log",
  open: () => {
    const framer = CodexWire.makeLineFramer();
    const handle = (line: string, emit: (frame: ClaudeSdkLogFrame) => void) => {
      if (line.trim().length === 0) return;
      emit({ line, log: logRecord(incoming("claudeAgent", "decoded", JSON.parse(line))) });
    };
    return {
      push: (chunk, emit) => {
        for (const line of framer.push(chunk).lines) handle(line, emit);
      },
      end: (emit) => handle(framer.end(), emit),
    };
  },
};

export const codexWireStage = makeCodexWireStage(CodexWire.makeLineFramer);
export const acpWireStage = makeAcpWireStage(() => AcpWire.makeStrictNdJsonRpcParser());

// Each mutant breaks exactly one framing rule on the worker side only.
const utf8PerChunk = (chunk: Uint8Array) => new TextDecoder().decode(chunk);

const codexMutants = {
  "decodes UTF-8 per chunk": (maxLineBytes?: number) => {
    const framer = CodexWire.makeLineFramer(maxLineBytes);
    const encoder = new TextEncoder();
    return { push: (chunk) => framer.push(encoder.encode(utf8PerChunk(chunk))), end: framer.end };
  },
  "keeps the CR of a CRLF": (maxLineBytes?: number) => {
    const framer = CodexWire.makeLineFramer(maxLineBytes);
    const crBeforeLf: Array<boolean> = [];
    let previous = 0;
    return {
      push: (chunk) => {
        for (const byte of chunk) {
          if (byte === 0x0a) crBeforeLf.push(previous === 0x0d);
          previous = byte;
        }
        const framed = framer.push(chunk);
        const lines = framed.lines.map((line) =>
          crBeforeLf.shift() === true ? `${line}\r` : line,
        );
        return { ...framed, lines };
      },
      end: framer.end,
    };
  },
  "ignores the line limit": () => CodexWire.makeLineFramer(),
} satisfies Record<string, (maxLineBytes?: number) => CodexWire.LineFramer>;

const acpMutants = {
  "decodes UTF-8 per chunk": () => {
    const parser = AcpWire.makeStrictNdJsonRpcParser();
    return {
      ...parser,
      decode: (bytes) => parser.decode(typeof bytes === "string" ? bytes : utf8PerChunk(bytes)),
    };
  },
  "skips a chunk with a malformed line": () => {
    const parser = AcpWire.makeStrictNdJsonRpcParser();
    return {
      ...parser,
      decode: (bytes) => {
        try {
          return parser.decode(bytes);
        } catch {
          return [];
        }
      },
    };
  },
} satisfies Record<string, () => AcpWire.StrictNdJsonRpcParser>;

export type EquivalenceWorkerVariant =
  | { readonly framer: "intact" }
  | { readonly framer: "codex"; readonly mutant: keyof typeof codexMutants }
  | { readonly framer: "acp"; readonly mutant: keyof typeof acpMutants };

export const codexMutantNames = Object.keys(codexMutants) as Array<keyof typeof codexMutants>;
export const acpMutantNames = Object.keys(acpMutants) as Array<keyof typeof acpMutants>;

export const equivalenceWorkerStages = (variant: EquivalenceWorkerVariant) => [
  variant.framer === "codex" ? makeCodexWireStage(codexMutants[variant.mutant]) : codexWireStage,
  variant.framer === "acp" ? makeAcpWireStage(acpMutants[variant.mutant]) : acpWireStage,
  claudeSdkLogStage,
];

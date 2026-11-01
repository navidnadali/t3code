import * as AcpWire from "effect-acp/wire";
import * as CodexWire from "effect-codex-app-server/wire";
import * as Result from "effect/Result";
import type * as Schema from "effect/Schema";

import {
  boundProviderEventForLogging,
  serializeEventNdjsonRecord,
} from "../../EventNdjsonRecord.ts";
import type { ProviderIoStage } from "../providerIoProtocol.ts";

// The provider stages P1-023 made pure, composed the way a worker would run them: framing, JSON,
// schema decode and protocol-log record building. Terminal conditions throw, as they end the session.

export type LogRecord =
  | { readonly _tag: "Skipped" }
  | { readonly _tag: "Serialized"; readonly payload: string }
  | { readonly _tag: "Failed"; readonly error: string };

type BuildLogRecord = (event: unknown) => LogRecord;

const logRecord: BuildLogRecord = (event) => {
  const record = serializeEventNdjsonRecord("native", event);
  if (record._tag === "Failed") return { _tag: "Failed", error: record.error.message };
  return record._tag === "Serialized" ? { _tag: "Serialized", payload: record.payload } : record;
};

const incoming = (provider: string, stage: string, payload: unknown) => ({
  provider,
  kind: "protocol",
  event: { direction: "incoming", stage, payload },
});

export interface CodexWireOptions {
  readonly maxLineBytes?: number | undefined;
}

const decodeCodexLine = (line: string, log: BuildLogRecord) => {
  const raw = log(incoming("codex", "raw", line));
  const decoded = CodexWire.decodeWireLine(line);
  if (Result.isFailure(decoded)) {
    const failure = CodexWire.decodeFailedLogPayload(decoded.failure);
    return {
      type: "decode-failed",
      line,
      failure,
      logs: [raw, log(incoming("codex", "decode_failed", failure))],
    } as const;
  }
  const message = decoded.success;
  const logs = [raw, log(incoming("codex", "decoded", message))];
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
  log: BuildLogRecord = logRecord,
): ProviderIoStage<CodexWireOptions, CodexWireFrame> => ({
  name: "equivalence-codex-wire",
  open: ({ maxLineBytes }) => {
    const framer = makeFramer(maxLineBytes);
    const handle = (line: string, emit: (frame: CodexWireFrame) => void) => {
      if (line.trim().length === 0) return;
      const frame = decodeCodexLine(line, log);
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

const acpFrame = (message: AcpWire.WireMessage, log: BuildLogRecord) => {
  const record = log(incoming("acp", "decoded", message));
  if (message._tag !== "Request" || message.id !== "") return { message, log: record };
  if (message.tag === "session/update") {
    const params = notificationParams(AcpWire.decodeSessionUpdate(message.payload));
    return { message, log: record, params };
  }
  if (message.tag === "elicitation/complete" || message.tag === "session/elicitation/complete") {
    const params = notificationParams(AcpWire.decodeElicitationComplete(message.payload));
    return { message, log: record, params };
  }
  return { message, log: record };
};

export type AcpWireFrame = ReturnType<typeof acpFrame>;

const makeAcpWireStage = (
  makeParser: () => AcpWire.StrictNdJsonRpcParser,
  log: BuildLogRecord = logRecord,
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
              cause: cause instanceof Error ? `${cause.name}: ${cause.message}` : String(cause),
            }),
          );
        }
        for (const message of decoded.success) emit(acpFrame(message, log));
      },
      end: () => {},
    };
  },
});

export interface ClaudeSdkLogFrame {
  readonly line: string;
  readonly log: LogRecord;
}

const makeClaudeSdkLogStage = (
  log: BuildLogRecord = logRecord,
): ProviderIoStage<void, ClaudeSdkLogFrame> => ({
  name: "equivalence-claude-sdk-log",
  open: () => {
    const framer = CodexWire.makeLineFramer();
    const handle = (line: string, emit: (frame: ClaudeSdkLogFrame) => void) => {
      if (line.trim().length === 0) return;
      emit({ line, log: log(incoming("claudeAgent", "decoded", JSON.parse(line))) });
    };
    return {
      push: (chunk, emit) => {
        for (const line of framer.push(chunk).lines) handle(line, emit);
      },
      end: (emit) => handle(framer.end(), emit),
    };
  },
});

export const codexWireStage = makeCodexWireStage(CodexWire.makeLineFramer);
export const acpWireStage = makeAcpWireStage(() => AcpWire.makeStrictNdJsonRpcParser());
export const claudeSdkLogStage = makeClaudeSdkLogStage();

// Each mutant breaks exactly one rule, on the worker side only.
const utf8PerChunk = (chunk: Uint8Array) => new TextDecoder().decode(chunk);
const BOM = [0xef, 0xbb, 0xbf];

// Wraps the real framer; `onBytes` sees every chunk first and `onEnd` may rewrite the EOF tail.
const wrapFramer = (
  maxLineBytes: number | undefined,
  hooks: {
    readonly onBytes?: (chunk: Uint8Array) => void;
    readonly onLines?: (lines: ReadonlyArray<string>) => ReadonlyArray<string>;
    readonly onEnd?: (tail: string) => string;
  },
): CodexWire.LineFramer => {
  const framer = CodexWire.makeLineFramer(maxLineBytes);
  return {
    push: (chunk) => {
      hooks.onBytes?.(chunk);
      const framed = framer.push(chunk);
      return hooks.onLines === undefined
        ? framed
        : { ...framed, lines: hooks.onLines(framed.lines) };
    },
    end: () => (hooks.onEnd === undefined ? framer.end() : hooks.onEnd(framer.end())),
  };
};

// Tells, as bytes arrive, whether the stream starts with a UTF-8 BOM.
const bomWatch = () => {
  const head: Array<number> = [];
  return {
    see: (chunk: Uint8Array) => {
      for (const byte of chunk) {
        if (head.length === BOM.length) return;
        head.push(byte);
      }
    },
    started: () => head.length === BOM.length && head.every((byte, at) => byte === BOM[at]),
  };
};

const codexMutants = {
  "decodes UTF-8 per chunk": (maxLineBytes?: number) => {
    const framer = CodexWire.makeLineFramer(maxLineBytes);
    const encoder = new TextEncoder();
    return { push: (chunk) => framer.push(encoder.encode(utf8PerChunk(chunk))), end: framer.end };
  },
  "keeps the CR of a CRLF": (maxLineBytes?: number) => {
    const crBeforeLf: Array<boolean> = [];
    let previous = 0;
    return wrapFramer(maxLineBytes, {
      onBytes: (chunk) => {
        for (const byte of chunk) {
          if (byte === 0x0a) crBeforeLf.push(previous === 0x0d);
          previous = byte;
        }
      },
      onLines: (lines) => lines.map((line) => (crBeforeLf.shift() === true ? `${line}\r` : line)),
    });
  },
  "ignores the line limit": () => CodexWire.makeLineFramer(),
  "strips the CR of the unterminated tail": (maxLineBytes?: number) =>
    wrapFramer(maxLineBytes, { onEnd: (tail) => tail.replace(/\r$/, "") }),
  "flushes an incomplete UTF-8 sequence at EOF as U+FFFD": (maxLineBytes?: number) => {
    const shadow = new TextDecoder();
    return wrapFramer(maxLineBytes, {
      onBytes: (chunk) => shadow.decode(chunk, { stream: true }),
      onEnd: (tail) => tail + shadow.decode(),
    });
  },
  "decodes with a fatal decoder": (maxLineBytes?: number) => {
    const fatal = new TextDecoder("utf-8", { fatal: true });
    return wrapFramer(maxLineBytes, {
      onBytes: (chunk) => fatal.decode(chunk, { stream: true }),
      onEnd: (tail) => {
        fatal.decode();
        return tail;
      },
    });
  },
  "keeps a leading BOM": (maxLineBytes?: number) => {
    const bom = bomWatch();
    let applied = false;
    const keep = (text: string) => {
      if (applied || !bom.started()) return text;
      applied = true;
      return `﻿${text}`;
    };
    return wrapFramer(maxLineBytes, {
      onBytes: bom.see,
      onLines: (lines) => lines.map((line, at) => (at === 0 ? keep(line) : line)),
      onEnd: keep,
    });
  },
} satisfies Record<string, (maxLineBytes?: number) => CodexWire.LineFramer>;

// Decodes bytes itself and hands the parser strings, so only the decoding rule changes.
const acpDecodingWith =
  (decoderOptions: { readonly fatal?: boolean; readonly ignoreBOM?: boolean }) => () => {
    const parser = AcpWire.makeStrictNdJsonRpcParser();
    const decoder = new TextDecoder("utf-8", decoderOptions);
    return {
      ...parser,
      decode: (bytes: Uint8Array | string) =>
        parser.decode(typeof bytes === "string" ? bytes : decoder.decode(bytes, { stream: true })),
    };
  };

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
  "keeps a leading BOM": (): AcpWire.StrictNdJsonRpcParser =>
    acpDecodingWith({ ignoreBOM: true })(),
  "decodes with a fatal decoder": (): AcpWire.StrictNdJsonRpcParser =>
    acpDecodingWith({ fatal: true })(),
} satisfies Record<string, () => AcpWire.StrictNdJsonRpcParser>;

const payloadBytes = (record: LogRecord) =>
  record._tag === "Serialized" ? Buffer.byteLength(record.payload) : 0;

// A record the real serializer summarises: the event padded past the 64 KiB record limit.
const summarised = (event: unknown) =>
  logRecord(
    typeof event === "object" && event !== null ? { ...event, pad: "x".repeat(70_000) } : event,
  );

const logMutants = {
  "falls back to the summary from 64,000 bytes": (event: unknown) => {
    const record = logRecord(event);
    return payloadBytes(record) > 64_000 ? summarised(event) : record;
  },
  "falls back to the summary from 16 KiB": (event: unknown) => {
    const record = logRecord(event);
    return payloadBytes(record) > 16 * 1024 ? summarised(event) : record;
  },
  "changes the summary's shape": (event: unknown) => {
    const record = logRecord(event);
    if (record._tag !== "Serialized" || boundProviderEventForLogging(event) === event) {
      return record;
    }
    return { _tag: "Serialized", payload: `{"shape":"changed",${record.payload.slice(1)}` };
  },
} satisfies Record<string, BuildLogRecord>;

export type EquivalenceWorkerVariant =
  | { readonly part: "intact" }
  | { readonly part: "codex framer"; readonly mutant: keyof typeof codexMutants }
  | { readonly part: "acp parser"; readonly mutant: keyof typeof acpMutants }
  | { readonly part: "log records"; readonly mutant: keyof typeof logMutants };

export const mutantVariants: ReadonlyArray<Exclude<EquivalenceWorkerVariant, { part: "intact" }>> =
  [
    ...(Object.keys(codexMutants) as Array<keyof typeof codexMutants>).map((mutant) => ({
      part: "codex framer" as const,
      mutant,
    })),
    ...(Object.keys(acpMutants) as Array<keyof typeof acpMutants>).map((mutant) => ({
      part: "acp parser" as const,
      mutant,
    })),
    ...(Object.keys(logMutants) as Array<keyof typeof logMutants>).map((mutant) => ({
      part: "log records" as const,
      mutant,
    })),
  ];

export const equivalenceWorkerStages = (variant: EquivalenceWorkerVariant) => {
  const log = variant.part === "log records" ? logMutants[variant.mutant] : logRecord;
  return [
    makeCodexWireStage(
      variant.part === "codex framer" ? codexMutants[variant.mutant] : CodexWire.makeLineFramer,
      log,
    ),
    makeAcpWireStage(
      variant.part === "acp parser"
        ? acpMutants[variant.mutant]
        : () => AcpWire.makeStrictNdJsonRpcParser(),
      log,
    ),
    makeClaudeSdkLogStage(log),
  ];
};

import * as AcpWire from "effect-acp/wire";
import * as CodexWire from "effect-codex-app-server/wire";
import * as Result from "effect/Result";

import type { ProviderIoStage } from "../providerIoProtocol.ts";

// P1-023's pure stages wrapped for the pool, so tests run real provider code in a worker. The
// production registry is P1-025's. Outputs are plain data; errors cross as their diagnostics.
export type CodexWireOutput =
  | { readonly _tag: "Message"; readonly line: string; readonly message: unknown }
  | {
      readonly _tag: "DecodeFailed";
      readonly line: string;
      readonly failure: ReturnType<typeof CodexWire.decodeFailedLogPayload>;
    }
  | { readonly _tag: "Exceeded"; readonly maxLineBytes: number };

/**
 * Codex app-server stdout, framed and JSON-decoded per line as `protocol.ts` does in-thread:
 * blank lines are skipped, and nothing follows a decode failure or a line over the limit.
 */
export const codexWireStage: ProviderIoStage<{ readonly maxLineBytes?: number }, CodexWireOutput> =
  {
    name: "codex-wire",
    open: ({ maxLineBytes = CodexWire.MAX_INCOMING_MESSAGE_BYTES }) => {
      const framer = CodexWire.makeLineFramer(maxLineBytes);
      let stopped = false;
      const handle = (line: string, emit: (output: CodexWireOutput) => void) => {
        if (stopped || line.trim().length === 0) return;
        const decoded = CodexWire.decodeWireLine(line);
        if (Result.isSuccess(decoded)) {
          emit({ _tag: "Message", line, message: decoded.success });
          return;
        }
        stopped = true;
        emit({
          _tag: "DecodeFailed",
          line,
          failure: CodexWire.decodeFailedLogPayload(decoded.failure),
        });
      };
      return {
        push: (chunk, emit) => {
          if (stopped) return;
          const { lines, exceeded } = framer.push(chunk);
          for (const line of lines) handle(line, emit);
          if (exceeded && !stopped) {
            stopped = true;
            emit({ _tag: "Exceeded", maxLineBytes });
          }
        },
        end: (emit) => handle(framer.end(), emit),
      };
    },
  };

export type AcpWireOutput =
  | { readonly _tag: "Message"; readonly message: AcpWire.WireMessage }
  | {
      readonly _tag: "DecodeFailed";
      readonly failure: ReturnType<typeof AcpWire.decodeFailureDiagnostics>;
    };

/**
 * ACP agent stdout through the strict NDJSON-RPC parser. A malformed line fails its whole chunk
 * and ends decoding. `call` encodes outgoing messages on the same parser, whose JSON-RPC batch
 * state the decoder shares.
 */
export const acpWireStage: ProviderIoStage<
  void,
  AcpWireOutput,
  Parameters<AcpWire.StrictNdJsonRpcParser["encode"]>[0],
  string | undefined
> = {
  name: "acp-wire",
  open: () => {
    const parser = AcpWire.makeStrictNdJsonRpcParser();
    let failed = false;
    return {
      push: (chunk, emit) => {
        if (failed) return;
        const decoded = AcpWire.decodeWireChunk(parser, chunk);
        if (Result.isFailure(decoded)) {
          failed = true;
          emit({
            _tag: "DecodeFailed",
            failure: AcpWire.decodeFailureDiagnostics(decoded.failure),
          });
          return;
        }
        for (const message of decoded.success) emit({ _tag: "Message", message });
      },
      end: () => {},
      call: (message) => parser.encode(message),
    };
  },
};

import * as Result from "effect/Result";

import { assert, it } from "@effect/vitest";

import * as AcpError from "./errors.ts";
import * as AcpWire from "./wire.ts";

const encoder = new TextEncoder();

const decodeChunks = (chunks: ReadonlyArray<Uint8Array | string>) => {
  const parser = AcpWire.makeStrictNdJsonRpcParser();
  return chunks.flatMap((chunk) => Result.getOrThrow(AcpWire.decodeWireChunk(parser, chunk)));
};

it("decodes JSON-RPC lines identically however the bytes are chunked", () => {
  const bytes = encoder.encode(
    [
      '{"jsonrpc":"2.0","id":7,"method":"x/test","params":{"text":"hé🙂"}}\r\n',
      '{"jsonrpc":"2.0","method":"session/update","params":{"text":"最後"}}\n',
      '{"jsonrpc":"2.0","id":3,"error":{"_tag":"Cause","code":-32603,"message":"x","data":[{"_tag":"Die","defect":{"code":-32000,"message":"denied"}}]}}\n',
      '{"jsonrpc":"2.0","id":4,"result":{"ok":true}}',
    ].join(""),
  );
  const expected = decodeChunks([bytes, "\n"]);
  assert.lengthOf(expected, 4);
  assert.deepInclude(expected[0], { _tag: "Request", tag: "x/test", payload: { text: "hé🙂" } });
  assert.deepInclude(expected[1], { _tag: "Request", tag: "session/update", id: "" });
  assert.deepInclude(expected[2], {
    _tag: "Exit",
    exit: {
      _tag: "Failure",
      cause: [{ _tag: "Fail", error: { code: -32000, message: "denied" } }],
    },
  });

  for (let split = 0; split <= bytes.length; split += 1) {
    const chunks = [bytes.subarray(0, split), bytes.subarray(split), "\n"];
    assert.deepEqual(decodeChunks(chunks), expected, `split ${split}`);
  }
  for (let size = 1; size <= 8; size += 1) {
    const chunks: Array<Uint8Array | string> = [];
    for (let offset = 0; offset < bytes.length; offset += size) {
      chunks.push(bytes.subarray(offset, offset + size));
    }
    chunks.push("\n");
    assert.deepEqual(decodeChunks(chunks), expected, `size ${size}`);
  }
});

it("keeps a partial line until its newline, with no size limit", () => {
  const parser = AcpWire.makeStrictNdJsonRpcParser();
  const text = "x".repeat(1024 * 1024);
  const bytes = encoder.encode(`{"jsonrpc":"2.0","method":"x/big","params":{"text":"${text}"}}`);

  for (let offset = 0; offset < bytes.length; offset += 64 * 1024) {
    assert.deepEqual(
      AcpWire.decodeWireChunk(parser, bytes.subarray(offset, offset + 64 * 1024)),
      Result.succeed([]),
    );
  }
  const [message] = Result.getOrThrow(AcpWire.decodeWireChunk(parser, "\n"));
  assert.deepInclude(message, { tag: "x/big", payload: { text } });
});

it("fails the whole chunk on a malformed line", () => {
  const parser = AcpWire.makeStrictNdJsonRpcParser();
  const decoded = AcpWire.decodeWireChunk(
    parser,
    '{"jsonrpc":"2.0","method":"x/first","params":{}}\n{"jsonrpc":\n',
  );

  assert.isTrue(Result.isFailure(decoded));
  if (Result.isFailure(decoded)) {
    assert.instanceOf(decoded.failure, AcpError.AcpProtocolParseError);
    assert.equal(decoded.failure.operation, "decode-wire-message");
  }
});

it("decodes an idle state update carrying a prompt failure", () => {
  const params = {
    sessionId: "session-1",
    update: {
      sessionUpdate: "state_update",
      state: "idle",
      stopReason: "end_turn",
      error: { code: -32000, message: "prompt failed" },
    },
  };

  assert.deepEqual(AcpWire.decodeSessionUpdate(params), Result.succeed(params));
  assert.isTrue(Result.isFailure(AcpWire.decodeSessionUpdate({ sessionId: 1 })));
});

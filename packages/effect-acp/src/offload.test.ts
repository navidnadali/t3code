import * as Result from "effect/Result";

import { assert, it } from "@effect/vitest";

import * as AcpOffload from "./offload.ts";
import * as AcpWire from "./wire.ts";

it("decodes offloaded lines as the strict parser does once main normalizes them", () => {
  const lines = [
    '{"jsonrpc":"2.0","id":7,"method":"x/test","params":{"text":"hé🙂"}}\r',
    '{"jsonrpc":"2.0","id":3,"error":{"_tag":"Cause","code":-32603,"message":"x","data":[{"_tag":"Die","defect":{"code":-32000,"message":"denied"}}]}}',
    '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hi"}}}}',
  ];
  const decode = AcpOffload.makeOffloadedLineDecoder();
  const parser = AcpWire.makeStrictNdJsonRpcParser();
  for (const line of lines) {
    const decoded = decode(line);
    assert.equal(decoded._tag, "Decoded");
    if (decoded._tag !== "Decoded") continue;
    assert.deepEqual(
      AcpWire.normalizeOffloadedMessages(decoded.messages),
      AcpWire.decodeWireChunk(parser, `${line}\n`),
    );
  }

  const malformed = decode('{"jsonrpc":"2.0",');
  assert.equal(malformed._tag, "Failed");
  if (malformed._tag === "Failed") assert.instanceOf(malformed.cause, SyntaxError);
});

it("fails normalizing as the strict parser fails on a JSON-RPC error it cannot read", () => {
  const line = '{"jsonrpc":"2.0","id":3,"error":{"_tag":"Cause","code":1,"message":"x","data":7}}';
  const decoded = AcpOffload.makeOffloadedLineDecoder()(line);
  assert.equal(decoded._tag, "Decoded");
  if (decoded._tag !== "Decoded") return;
  const offloaded = AcpWire.normalizeOffloadedMessages(decoded.messages);
  const inThread = AcpWire.decodeWireChunk(AcpWire.makeStrictNdJsonRpcParser(), `${line}\n`);
  assert.isTrue(Result.isFailure(inThread));
  assert.isTrue(Result.isFailure(offloaded));
  if (Result.isFailure(offloaded) && Result.isFailure(inThread)) {
    assert.equal(offloaded.failure.operation, inThread.failure.operation);
    assert.equal(String(offloaded.failure.cause), String(inThread.failure.cause));
  }
});

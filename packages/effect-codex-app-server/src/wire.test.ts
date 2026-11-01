import * as Result from "effect/Result";

import { assert, it } from "@effect/vitest";

import * as CodexError from "./errors.ts";
import * as CodexWire from "./wire.ts";

const encoder = new TextEncoder();

const frameChunks = (framer: CodexWire.LineFramer, chunks: ReadonlyArray<Uint8Array>) => {
  const lines: Array<string> = [];
  for (const chunk of chunks) {
    const framed = framer.push(chunk);
    assert.isFalse(framed.exceeded);
    lines.push(...framed.lines);
  }
  return { lines, tail: framer.end() };
};

it("frames lines identically however the bytes are chunked", () => {
  const bytes = encoder.encode('\n \t\r\n{"a":"hé🙂"}\r\n\nx\ry\r\r\n最後\r\n{"tail":1}\r');
  const expected = {
    lines: ["", " \t", '{"a":"hé🙂"}', "", "x\ry\r", "最後"],
    tail: '{"tail":1}\r',
  };

  for (let split = 0; split <= bytes.length; split += 1) {
    const chunks = [bytes.subarray(0, split), bytes.subarray(split)];
    assert.deepEqual(frameChunks(CodexWire.makeLineFramer(), chunks), expected, `split ${split}`);
  }
  for (let size = 1; size <= 8; size += 1) {
    const chunks: Array<Uint8Array> = [];
    for (let offset = 0; offset < bytes.length; offset += size) {
      chunks.push(bytes.subarray(offset, offset + size));
    }
    assert.deepEqual(frameChunks(CodexWire.makeLineFramer(), chunks), expected, `size ${size}`);
  }
});

it("holds back a split UTF-8 sequence until its last byte arrives", () => {
  const framer = CodexWire.makeLineFramer();
  const bytes = encoder.encode("🙂\n");

  assert.deepEqual(framer.push(bytes.subarray(0, 3)), { lines: [], exceeded: false });
  assert.deepEqual(framer.push(bytes.subarray(3)), { lines: ["🙂"], exceeded: false });
  assert.equal(framer.end(), "");
});

it("limits a line by its UTF-8 bytes, excluding the LF, and stays exceeded", () => {
  const framer = CodexWire.makeLineFramer(4);

  assert.deepEqual(framer.push(encoder.encode("éé\nabcd\nab")), {
    lines: ["éé", "abcd"],
    exceeded: false,
  });
  assert.deepEqual(framer.push(encoder.encode("c\nééx\n")), {
    lines: ["abc"],
    exceeded: true,
  });
  assert.deepEqual(framer.push(encoder.encode("a\n")), { lines: [], exceeded: true });
  assert.equal(framer.end(), "");
});

it("accepts an unterminated line of exactly 128 MiB and rejects one more byte", () => {
  const framer = CodexWire.makeLineFramer();
  const chunk = new Uint8Array(8 * 1024 * 1024).fill(0x61);
  for (let index = 0; index < CodexWire.MAX_INCOMING_MESSAGE_BYTES / chunk.length; index += 1) {
    assert.deepEqual(framer.push(chunk), { lines: [], exceeded: false });
  }

  assert.deepEqual(framer.push(encoder.encode("a")), { lines: [], exceeded: true });
});

it("decodes wire lines and notification params without an Effect runtime", () => {
  assert.deepEqual(
    CodexWire.decodeWireLine('{"id":1,"result":{}}\r'),
    Result.succeed({
      id: 1,
      result: {},
    }),
  );
  const malformed = CodexWire.decodeWireLine('{"id":1,');
  assert.isTrue(Result.isFailure(malformed));
  if (Result.isFailure(malformed)) {
    assert.instanceOf(malformed.failure, CodexError.CodexAppServerProtocolParseError);
    assert.equal(malformed.failure.operation, "decode-wire-message");
  }

  const method = "item/agentMessage/delta";
  const schema = CodexWire.serverNotificationParamsSchema(method);
  assert.isDefined(schema);
  assert.isUndefined(CodexWire.serverNotificationParamsSchema("x/unknown"));
  if (schema === undefined) return;
  const delta = { threadId: "t", turnId: "u", itemId: "i", delta: "d" };
  assert.deepEqual(
    CodexWire.decodeNotificationPayload(method, schema, delta),
    Result.succeed(delta),
  );
  const invalid = CodexWire.decodeNotificationPayload(method, schema, {});
  assert.isTrue(Result.isFailure(invalid));
  if (Result.isFailure(invalid)) {
    assert.equal(invalid.failure.operation, "decode-notification-payload");
    assert.equal(invalid.failure.method, method);
  }
});

import { assert, it } from "@effect/vitest";

import * as CodexOffload from "./offload.ts";

it("parses an offloaded line and builds its log entries as the protocol does in-thread", () => {
  const delta = { threadId: "t", turnId: "u", itemId: "i", delta: "d" };
  const line = JSON.stringify({ method: "item/agentMessage/delta", params: delta });
  const stages: Array<string> = [];
  const logEntry = (event: { readonly stage: string; readonly payload: unknown }) => {
    stages.push(event.stage);
    return event.stage === "raw"
      ? ("skip" as const)
      : { stream: "native", payload: JSON.stringify(event.payload) };
  };

  assert.deepEqual(CodexOffload.decodeOffloadedLine(line, logEntry), {
    _tag: "Decoded",
    message: { method: "item/agentMessage/delta", params: delta },
    logs: [{ stream: "native", payload: line }],
  });
  assert.deepEqual(stages, ["raw", "decoded"]);
  assert.deepEqual(CodexOffload.decodeOffloadedLine('{"id":3,"result":{"ok":true}}'), {
    _tag: "Decoded",
    message: { id: 3, result: { ok: true } },
  });
});

it("hands a line back to main when it does not parse or main must log it", () => {
  assert.deepEqual(CodexOffload.decodeOffloadedLine('{"id":1,'), { _tag: "InThread" });
  assert.deepEqual(
    CodexOffload.decodeOffloadedLine('{"id":1,"result":{}}', (event) =>
      event.stage === "decoded" ? "in-thread" : "skip",
    ),
    { _tag: "InThread" },
  );
});

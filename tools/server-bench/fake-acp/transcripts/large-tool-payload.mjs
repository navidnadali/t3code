// A synthetic ACP prompt turn with large tool output, in the registry transcript format: a file
// read and a command run whose results carry `payloadBytes` of text each. Generated, so it holds
// no recorded data; the agent gives its tool calls fresh ids.
import { syntheticText } from "../../fake-common/replay.mjs";

const SESSION = "bench-large-tool-payload-session";
const PROMPT = "Read src/generated.ts, run the report script and summarise both.";

export function makeTranscript({ payloadBytes }) {
  const entries = [
    {
      type: "transcript_start",
      provider: "acpRegistry",
      protocol: "acp.ndjson-jsonrpc",
      version: "1",
      scenario: "large-tool-payload",
      metadata: { source: "tools/server-bench/fake-acp/transcripts/large-tool-payload.mjs" },
    },
  ];
  const expect = (frame) => entries.push({ type: "expect_outbound", label: frame.method, frame });
  const emit = (frame, afterMs) =>
    entries.push({
      type: "emit_inbound",
      label: frame.method,
      frame,
      ...(afterMs === undefined ? {} : { afterMs }),
    });
  const update = (value, afterMs) =>
    emit(
      {
        kind: "notification",
        method: "session/update",
        params: { sessionId: SESSION, update: value },
      },
      afterMs,
    );
  const tool = (toolCallId, title, kind, rawInput, text) => {
    update({ sessionUpdate: "tool_call", toolCallId, title, kind, status: "pending", rawInput });
    update(
      {
        sessionUpdate: "tool_call_update",
        toolCallId,
        status: "completed",
        content: [{ type: "content", content: { type: "text", text } }],
      },
      600,
    );
  };

  expect({ kind: "request", method: "initialize", params: { protocolVersion: 1 } });
  emit({
    kind: "response",
    method: "initialize",
    result: { protocolVersion: 1, agentCapabilities: { loadSession: true }, authMethods: [] },
  });
  expect({ kind: "request", method: "session/new", params: { cwd: "<workspace>" } });
  emit({ kind: "response", method: "session/new", result: { sessionId: SESSION } });
  expect({
    kind: "request",
    method: "session/prompt",
    params: { sessionId: SESSION, prompt: [{ type: "text", text: PROMPT }] },
  });
  update({
    sessionUpdate: "agent_message_chunk",
    content: { type: "text", text: "I'll read the generated module, then run the report." },
  });
  tool(
    "read-generated",
    "Read src/generated.ts",
    "read",
    { path: "<workspace>/src/generated.ts" },
    syntheticText(payloadBytes, "module"),
  );
  tool(
    "run-report",
    "Run node scripts/report.mjs --all",
    "execute",
    { command: "node scripts/report.mjs --all", cwd: "<workspace>" },
    syntheticText(payloadBytes, "report"),
  );
  update({
    sessionUpdate: "agent_message_chunk",
    content: { type: "text", text: "large tool payload fixture complete" },
  });
  emit({ kind: "response", method: "session/prompt", result: { stopReason: "end_turn" } });
  entries.push({ type: "runtime_exit", status: "success" });
  return entries;
}

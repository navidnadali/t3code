// A synthetic turn with large tool payloads, shaped like Claude Code 2.1 stream-json output:
// a Read result, a Bash output and a streamed Write input of `payloadBytes` each, with
// multi-byte text. Generated, so it holds no recorded data; the fake gives it fresh ids.
const MODEL = "claude-sonnet-4-6";
const CWD = "/tmp/claude-bench-large-tool-payload";
const PROMPT =
  "Read src/generated.ts, run the report script, then write the regenerated module back.";
const TEXT_CHUNK = 24;
const INPUT_CHUNK = 2_048;

const chunks = (text, size) => {
  const parts = [];
  for (let index = 0; index < text.length; index += size)
    parts.push(text.slice(index, index + size));
  return parts.length === 0 ? [""] : parts;
};

function syntheticLines(bytes, label) {
  const lines = [];
  let size = 0;
  for (let line = 1; size < bytes; line++) {
    const text = `export const ${label}${line} = compute("${label}-${line}", ${(line * 37) % 101}, { note: "naïve → résumé ✓\\t#${line}" });`;
    lines.push(text);
    size += Buffer.byteLength(text) + 1;
  }
  return lines;
}

export function makeTranscript({ payloadBytes }) {
  let next = 0;
  const hex = (length) => (++next).toString(16).padStart(length, "0");
  const uuid = () => `00000000-0000-4000-8000-${hex(12)}`;
  const apiId = (prefix) => `${prefix}_01Bench${hex(17)}`;
  const sessionId = uuid();
  const promptUuid = uuid();
  const entries = [];
  const emit = (frame) =>
    entries.push({
      type: "emit_inbound",
      label: frame.type,
      frame: { ...frame, session_id: sessionId },
    });
  const streamEvent = (event, extra = {}) =>
    emit({ type: "stream_event", event, parent_tool_use_id: null, uuid: uuid(), ...extra });
  const usage = (output) => ({
    input_tokens: 3,
    cache_creation_input_tokens: 2_048,
    cache_read_input_tokens: 24_576,
    output_tokens: output,
    service_tier: "standard",
  });

  const message = (blocks, stopReason) => {
    const id = apiId("msg");
    const base = { model: MODEL, id, type: "message", role: "assistant", stop_sequence: null };
    emit({ type: "system", subtype: "status", status: "requesting", uuid: uuid() });
    streamEvent(
      {
        type: "message_start",
        message: { ...base, content: [], stop_reason: null, usage: usage(1) },
      },
      { ttft_ms: 900, user_message_uuid: promptUuid, user_message_uuids: [promptUuid] },
    );
    blocks.forEach((block, index) => {
      if (block.type === "tool_use") {
        streamEvent({ type: "content_block_start", index, content_block: { ...block, input: {} } });
        for (const partial of chunks(JSON.stringify(block.input), INPUT_CHUNK)) {
          streamEvent({
            type: "content_block_delta",
            index,
            delta: { type: "input_json_delta", partial_json: partial },
          });
        }
      } else {
        const field = block.type === "thinking" ? "thinking" : "text";
        const empty = block.type === "thinking" ? { thinking: "", signature: "" } : { text: "" };
        streamEvent({
          type: "content_block_start",
          index,
          content_block: { type: block.type, ...empty },
        });
        for (const text of chunks(block[field], TEXT_CHUNK)) {
          streamEvent({
            type: "content_block_delta",
            index,
            delta: { type: `${field}_delta`, [field]: text },
          });
        }
        if (block.type === "thinking") {
          streamEvent({
            type: "content_block_delta",
            index,
            delta: { type: "signature_delta", signature: block.signature },
          });
        }
      }
      emit({
        type: "assistant",
        message: { ...base, content: [block], stop_reason: null, usage: usage(1) },
        parent_tool_use_id: null,
        uuid: uuid(),
      });
      streamEvent({ type: "content_block_stop", index });
    });
    streamEvent({
      type: "message_delta",
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage: usage(Math.ceil(JSON.stringify(blocks).length / 4)),
    });
    streamEvent({ type: "message_stop" });
  };

  const toolResult = (toolUseId, content, toolUseResult) =>
    emit({
      type: "user",
      message: {
        role: "user",
        content: [{ tool_use_id: toolUseId, type: "tool_result", content, is_error: false }],
      },
      parent_tool_use_id: null,
      uuid: uuid(),
      timestamp: "2026-10-31T00:00:00.000Z",
      tool_use_result: toolUseResult,
    });

  entries.push({
    type: "transcript_start",
    provider: "claudeAgent",
    protocol: "claude-agent-sdk.query",
    scenario: "large_tool_payload",
    metadata: { prompts: [PROMPT], model: MODEL, nativeSessionId: sessionId, payloadBytes },
  });
  entries.push({
    type: "expect_outbound",
    label: "prompt.offer:1",
    frame: {
      type: "prompt.offer",
      message: {
        type: "user",
        message: { role: "user", content: PROMPT },
        parent_tool_use_id: null,
        uuid: promptUuid,
      },
    },
  });
  emit({
    type: "system",
    subtype: "init",
    agents: [],
    apiKeySource: "none",
    claude_code_version: "2.1.300",
    cwd: CWD,
    tools: ["Bash", "Read", "Write"],
    mcp_servers: [],
    model: MODEL,
    permissionMode: "bypassPermissions",
    slash_commands: [],
    output_style: "default",
    skills: [],
    plugins: [],
    fast_mode_state: "off",
    uuid: uuid(),
  });

  const filePath = `${CWD}/src/generated.ts`;
  const source = syntheticLines(payloadBytes, "readValue");
  const read = {
    type: "tool_use",
    id: apiId("toolu"),
    name: "Read",
    input: { file_path: filePath },
  };
  message(
    [
      {
        type: "thinking",
        thinking:
          "The module is large; read it whole before changing anything, then run the report.",
        signature: "QmVuY2hTaWduYXR1cmU=",
      },
      { type: "text", text: "I'll read the generated module first." },
      read,
    ],
    "tool_use",
  );
  const numbered = source.map((line, index) => `${String(index + 1).padStart(6)}\t${line}`);
  toolResult(read.id, numbered.join("\n"), {
    type: "text",
    file: {
      filePath,
      content: source.join("\n"),
      numLines: source.length,
      startLine: 1,
      totalLines: source.length,
    },
  });

  const bash = {
    type: "tool_use",
    id: apiId("toolu"),
    name: "Bash",
    input: { command: "node scripts/report.mjs --verbose", description: "Run the report script" },
  };
  message([{ type: "text", text: "Now the report script." }, bash], "tool_use");
  const stdout = syntheticLines(payloadBytes, "reportLine").join("\n");
  toolResult(bash.id, stdout, {
    stdout,
    stderr: "",
    interrupted: false,
    isImage: false,
    noOutputExpected: false,
  });

  const content = syntheticLines(payloadBytes, "writtenValue").join("\n");
  const write = {
    type: "tool_use",
    id: apiId("toolu"),
    name: "Write",
    input: { file_path: filePath, content },
  };
  message([{ type: "text", text: "Writing the regenerated module." }, write], "tool_use");
  toolResult(write.id, `File created successfully at: ${filePath}`, {
    type: "create",
    filePath,
    content,
    structuredPatch: [],
    originalFile: null,
  });

  const answer = "Read, ran and rewrote src/generated.ts; the report finished without errors.";
  message([{ type: "text", text: answer }], "end_turn");
  emit({
    type: "result",
    subtype: "success",
    is_error: false,
    duration_ms: 12_000,
    duration_api_ms: 9_000,
    num_turns: 4,
    result: answer,
    stop_reason: "end_turn",
    total_cost_usd: 0,
    usage: usage(512),
    modelUsage: {},
    permission_denials: [],
    uuid: uuid(),
    user_message_uuid: promptUuid,
    user_message_uuids: [promptUuid],
  });
  entries.push({ type: "runtime_exit", status: "success" });
  return entries;
}

// A synthetic Codex app-server turn with large tool output, shaped like codex 0.157: a command
// whose output streams in chunks and completes as one `payloadBytes` item, then an MCP tool result
// of the same size. Generated, so it holds no recorded data; the fake gives it fresh ids.
import { syntheticText } from "../../fake-common/replay.mjs";

const THREAD = "00000000-0000-4000-8000-0000000000a1";
const TURN = "00000000-0000-4000-8000-0000000000b1";
const CWD = "/tmp/codex-bench-large-tool-payload";
const MODEL = "gpt-6-luna";
const VERSION = "0.159.0";
const PROMPT =
  "Run the report script, then read src/generated.ts through the repository MCP server and summarise both.";
const OUTPUT_CHUNKS = 8;

const words = (text) => text.match(/\S+\s*/g) ?? [text];

function chunks(text, count) {
  const size = Math.ceil(text.length / count);
  return Array.from({ length: count }, (_, index) => text.slice(index * size, (index + 1) * size));
}

export function makeTranscript({ payloadBytes }) {
  let at = 1_790_000_000_000;
  const seconds = () => Math.floor(at / 1000);
  const entries = [
    {
      type: "transcript_start",
      provider: "codex",
      protocol: "codex.app-server",
      version: VERSION,
      scenario: "large-tool-payload",
      metadata: { source: "tools/server-bench/fake-codex/transcripts/large-tool-payload.mjs" },
    },
  ];
  const expect = (frame) => entries.push({ type: "expect_outbound", label: frame.method, frame });
  const answer = (id, result) =>
    entries.push({ type: "emit_inbound", label: "response", frame: { id, result } });
  const emit = (method, params, gapMs = 20) => {
    at += gapMs;
    entries.push({
      type: "emit_inbound",
      label: method,
      frame: { method, params, emittedAtMs: at },
    });
  };
  const inTurn = (params) => ({ threadId: THREAD, turnId: TURN, ...params });
  const item = (event, value, gapMs) =>
    emit(
      `item/${event}`,
      inTurn({ item: value, [event === "started" ? "startedAtMs" : "completedAtMs"]: at + gapMs }),
      gapMs,
    );
  const agentMessage = (id, text, phase) => {
    const message = { type: "agentMessage", id, text: "", phase, memoryCitation: null };
    item("started", { ...message, delivery: null, questions: null }, 400);
    for (const delta of words(text)) {
      emit("item/agentMessage/delta", inTurn({ itemId: id, delta }), 25);
    }
    item("completed", { ...message, text, delivery: null, questions: null }, 20);
    return { ...message, text, delivery: null, questions: null };
  };

  const thread = {
    id: THREAD,
    environments: [{ environmentId: "local", cwd: CWD, runtimeWorkspaceRoots: [CWD] }],
    extra: null,
    sessionId: THREAD,
    forkedFromId: null,
    parentThreadId: null,
    preview: "",
    ephemeral: false,
    section: null,
    sectionEnteredAt: null,
    projectId: null,
    historyMode: "paginated",
    modelProvider: "openai",
    model: MODEL,
    reasoningEffort: null,
    createdAt: seconds(),
    updatedAt: seconds(),
    recencyAt: seconds(),
    status: { type: "idle" },
    path: `/tmp/codex-bench-home/sessions/rollout-${THREAD}.jsonl`,
    cwd: CWD,
    cliVersion: VERSION,
    originator: "T3 Code",
    source: "vscode",
    canAcceptDirectInput: true,
    threadSource: null,
    agentNickname: null,
    agentRole: null,
    gitInfo: null,
    name: null,
    daybreakEnabled: null,
    turns: [],
  };

  expect({ id: 1, method: "initialize", params: {} });
  answer(1, {
    userAgent: `T3 Code/${VERSION} (bench) codex-app-server`,
    codexHome: "/tmp/codex-bench-home",
    platformFamily: "unix",
    platformOs: "linux",
  });
  expect({ method: "initialized" });
  expect({ id: 2, method: "thread/start", params: { cwd: CWD, model: MODEL } });
  answer(2, {
    thread,
    model: MODEL,
    modelProvider: "openai",
    serviceTier: "default",
    disabledPluginIds: [],
    cwd: CWD,
    runtimeWorkspaceRoots: [CWD],
    instructionSources: [],
    approvalPolicy: "never",
    approvalsReviewer: "user",
    sandbox: { type: "dangerFullAccess" },
    activePermissionProfile: null,
    reasoningEffort: null,
    multiAgentMode: "explicitRequestOnly",
  });
  emit("thread/started", { thread });
  emit("thread/settings/updated", {
    threadId: THREAD,
    threadSettings: {
      disabledPluginIds: [],
      cwd: CWD,
      approvalPolicy: "never",
      approvalsReviewer: "user",
      sandboxPolicy: { type: "dangerFullAccess" },
      activePermissionProfile: null,
      model: MODEL,
      modelProvider: "openai",
      serviceTier: "default",
      effort: null,
      summary: "detailed",
      collaborationMode: {
        mode: "default",
        settings: { model: MODEL, reasoning_effort: null, developer_instructions: null },
      },
      multiAgentMode: "explicitRequestOnly",
      personality: null,
    },
  });

  expect({
    id: 3,
    method: "turn/start",
    params: { threadId: THREAD, input: [{ type: "text", text: PROMPT }], model: MODEL },
  });
  const started = {
    id: TURN,
    items: [],
    itemsView: "notLoaded",
    status: "inProgress",
    error: null,
    startedAt: null,
    completedAt: null,
    durationMs: null,
  };
  answer(3, { turn: started });
  const turnStartedAt = seconds();
  emit("thread/status/changed", { threadId: THREAD, status: { type: "active", activeFlags: [] } });
  emit("turn/started", { threadId: THREAD, turn: { ...started, startedAt: turnStartedAt } });
  const user = {
    type: "userMessage",
    id: "00000000-0000-4000-8000-0000000000c1",
    clientId: null,
    content: [{ type: "text", text: PROMPT, text_elements: [] }],
  };
  item("started", user, 300);
  item("completed", user, 5);
  agentMessage(
    "msg_00000000000000000000000000000000000000000000000001",
    "I'll run the report, then read the generated module through MCP.",
    "commentary",
  );

  const output = syntheticText(payloadBytes, "report");
  const command = {
    type: "commandExecution",
    id: "exec-00000000-0000-4000-8000-0000000000d1",
    pluginId: null,
    scriptPath: null,
    command: "/bin/zsh -lc 'node scripts/report.mjs --all'",
    cwd: CWD,
    processId: "4242",
    source: "unifiedExecStartup",
    commandActions: [{ type: "unknown", command: "node scripts/report.mjs --all" }],
  };
  item(
    "started",
    { ...command, status: "inProgress", aggregatedOutput: null, exitCode: null, durationMs: null },
    600,
  );
  for (const delta of chunks(output, OUTPUT_CHUNKS)) {
    emit("item/commandExecution/outputDelta", inTurn({ itemId: command.id, delta }), 250);
  }
  item(
    "completed",
    { ...command, status: "completed", aggregatedOutput: output, exitCode: 0, durationMs: 2_400 },
    50,
  );

  const tool = {
    type: "mcpToolCall",
    id: "call_BenchLargePayload01",
    server: "repository",
    tool: "read_file",
    arguments: { path: "src/generated.ts" },
    error: null,
  };
  item("started", { ...tool, status: "inProgress", result: null, durationMs: null }, 400);
  item(
    "completed",
    {
      ...tool,
      status: "completed",
      result: { content: [{ type: "text", text: syntheticText(payloadBytes, "module") }] },
      durationMs: 180,
    },
    180,
  );
  emit("thread/tokenUsage/updated", {
    threadId: THREAD,
    turnId: TURN,
    tokenUsage: {
      total: {
        totalTokens: 40_960,
        inputTokens: 40_000,
        cachedInputTokens: 8_192,
        cacheWriteInputTokens: 0,
        outputTokens: 960,
        reasoningOutputTokens: 0,
      },
      last: {
        totalTokens: 40_960,
        inputTokens: 40_000,
        cachedInputTokens: 8_192,
        cacheWriteInputTokens: 0,
        outputTokens: 960,
        reasoningOutputTokens: 0,
      },
      modelContextWindow: 258_400,
    },
  });
  const final = agentMessage(
    "msg_00000000000000000000000000000000000000000000000002",
    "large tool payload fixture complete",
    "final_answer",
  );
  emit("thread/status/changed", { threadId: THREAD, status: { type: "idle" } });
  emit("turn/completed", {
    threadId: THREAD,
    turn: {
      ...started,
      items: [final],
      itemsView: "summary",
      status: "completed",
      startedAt: turnStartedAt,
      completedAt: seconds(),
      durationMs: (seconds() - turnStartedAt) * 1000,
    },
  });
  entries.push({ type: "runtime_exit", status: "success" });
  return entries;
}

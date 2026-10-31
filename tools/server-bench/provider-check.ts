#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalTimers:off preferSchemaOverJson:off - Standalone bench check outside the Effect runtime.
/**
 * Runs bench sessions on a scratch server's replaying provider instances through the `t3` CLI,
 * then checks each Codex and ACP session against the transcript turns its fake played: frames,
 * server items, replies. A failed, hung or mismatched run also writes provider-check.failed.
 */
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeUtil from "node:util";

import {
  BENCH_PROVIDERS,
  type PlannedSession,
  PROVIDER_LOAD_FLAGS,
  PROVIDER_LOAD_USAGE,
  type ProviderKind,
  parseProviderLoad,
  planSessions,
  readBenchKnobs,
  sessionsByInstance,
} from "./providers.ts";

const RESULT_FILE = "provider-check.json";
const FAILED_MARKER = "provider-check.failed";
const repoRoot = NodePath.resolve(import.meta.dirname, "../..");

const USAGE = `Usage: node tools/server-bench/provider-check.ts --base-dir <scratch home> --project <dir> --out <dir> [options]

Starts each session with \`t3 thread start --wait\` and sends its later turns with
\`t3 thread send --wait\`, against the server already running on <scratch home>, whose settings
come from \`providers.ts settings\` (each fake needs BENCH_STATS_DIR).

  --sessions N              sessions on --provider (default 0)
  --provider ID --model SLUG  (default ${BENCH_PROVIDERS.claude.instanceId}, ${BENCH_PROVIDERS.claude.model})
${PROVIDER_LOAD_USAGE}
  --turns N                 turns per session (default 2)
  --concurrency N           sessions in flight at once (default 8)
  --call-timeout SECONDS    a CLI call still waiting this long is killed; its turn is hung (default 300)
  --t3 PATH                 the server CLI (default apps/server/dist/bin.mjs)
`;

// Codex item types the Codex adapter turns into these turn items.
const CODEX_ITEM_TYPES: Readonly<Record<string, string>> = {
  agentMessage: "assistant_message",
  reasoning: "reasoning",
  commandExecution: "command_execution",
  fileChange: "file_change",
  webSearch: "web_search",
};
const NON_TOOL_ITEMS = new Set([
  "user_message",
  "assistant_message",
  "reasoning",
  "checkpoint",
  "todo_list",
]);

interface TurnExpectation {
  readonly frames: Readonly<Record<string, number>>;
  readonly items: Readonly<Record<string, number>>;
  readonly todoLists: number;
  readonly reply: string | null;
}

interface FakeModule {
  readonly expectedTurn: (name: string, index: number, loops: number) => TurnExpectation;
}

interface StatsRecord {
  readonly at: string;
  readonly fake: string;
  readonly sessionId: string;
  readonly transcript: string;
  readonly turn: number;
  readonly loops: number;
  readonly aborted: boolean;
  readonly frames: Readonly<Record<string, number>>;
  readonly bytes: number;
  readonly ms: number;
}

interface TurnOutcome {
  readonly status: string;
  readonly error: string | null;
  readonly reply: string | null;
  readonly runId: string | null;
}

interface SessionRun {
  readonly session: PlannedSession;
  threadId: string | null;
  readonly turns: Array<TurnOutcome>;
}

interface CliResult {
  readonly code: number | null;
  readonly stdout: string;
  readonly stderr: string;
  readonly timedOut: boolean;
}

function runCli(
  command: string,
  args: ReadonlyArray<string>,
  cwd: string,
  timeoutMs: number,
): Promise<CliResult> {
  return new Promise((resolve) => {
    const child = NodeChildProcess.spawn(command, args, { cwd, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    child.stdout.on("data", (chunk: Buffer) => (stdout += chunk.toString("utf8")));
    child.stderr.on("data", (chunk: Buffer) => (stderr += chunk.toString("utf8")));
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);
    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({ code, stdout, stderr, timedOut });
    });
  });
}

interface ThreadAnswer {
  readonly threadId?: string;
  readonly runId?: string | null;
  readonly status?: string;
  readonly reply?: string | null;
  readonly error?: string | null;
}

/** The CLI's `--json` answer: the last line that parses as an object with a status. */
function parseThreadAnswer(stdout: string): ThreadAnswer | null {
  for (const line of stdout.trim().split("\n").toReversed()) {
    try {
      const value = JSON.parse(line) as unknown;
      if (typeof value === "object" && value !== null && "status" in value)
        return value as ThreadAnswer;
    } catch {
      // Text output around the JSON answer.
    }
  }
  return null;
}

async function forEachLimited<T>(
  items: ReadonlyArray<T>,
  limit: number,
  run: (item: T) => Promise<void>,
): Promise<void> {
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) await run(items[next++] as T);
    }),
  );
}

const countBy = <T>(values: ReadonlyArray<T>, key: (value: T) => string) => {
  const counts: Record<string, number> = {};
  for (const value of values) counts[key(value)] = (counts[key(value)] ?? 0) + 1;
  return counts;
};

const sameCounts = (
  left: Readonly<Record<string, number>>,
  right: Readonly<Record<string, number>>,
) =>
  [...new Set([...Object.keys(left), ...Object.keys(right)])].every(
    (key) => (left[key] ?? 0) === (right[key] ?? 0),
  );

function readStats(statsDirs: ReadonlySet<string>): ReadonlyArray<StatsRecord> {
  const records: Array<StatsRecord> = [];
  for (const dir of statsDirs) {
    if (!NodeFS.existsSync(dir)) continue;
    for (const file of NodeFS.readdirSync(dir)) {
      if (!file.endsWith(".ndjson")) continue;
      for (const line of NodeFS.readFileSync(NodePath.join(dir, file), "utf8").split("\n")) {
        if (line.trim().length > 0) records.push(JSON.parse(line) as StatsRecord);
      }
    }
  }
  return records.toSorted((left, right) => left.at.localeCompare(right.at));
}

interface ThreadRows {
  readonly nativeId: string | null;
  readonly runs: ReadonlyArray<string>;
  readonly items: ReadonlyArray<{
    readonly type: string;
    readonly status: string;
    readonly id: string;
  }>;
}

function readThread(db: NodeSqlite.DatabaseSync, threadId: string): ThreadRows {
  const provider = db
    .prepare(
      "select json_extract(payload_json, '$.nativeThreadRef.nativeId') as nativeId from orchestration_v2_projection_provider_threads where json_extract(payload_json, '$.appThreadId') = ? and json_extract(payload_json, '$.nativeThreadRef.nativeId') is not null",
    )
    .get(threadId) as { nativeId: string } | undefined;
  const runs = db
    .prepare(
      "select status from orchestration_v2_projection_runs where thread_id = ? order by rowid",
    )
    .all(threadId) as Array<{ status: string }>;
  const items = db
    .prepare(
      "select type, status, turn_item_id as id from orchestration_v2_projection_turn_items where thread_id = ?",
    )
    .all(threadId) as Array<{ type: string; status: string; id: string }>;
  return { nativeId: provider?.nativeId ?? null, runs: runs.map((run) => run.status), items };
}

/** Items the provider produced: no checkpoints, no workspace preparation step. */
const providerItems = (rows: ThreadRows) =>
  rows.items.filter(
    (item) => item.type !== "checkpoint" && !item.id.includes("workspace-preparation"),
  );

function expectedItems(
  kind: ProviderKind,
  expectations: ReadonlyArray<TurnExpectation>,
): { readonly counts: Record<string, number>; readonly unchecked: ReadonlyArray<string> } {
  const counts: Record<string, number> = { user_message: expectations.length, todo_list: 0 };
  const unchecked = new Set<string>();
  for (const expectation of expectations) {
    counts.todo_list = (counts.todo_list ?? 0) + expectation.todoLists;
    for (const [type, count] of Object.entries(expectation.items)) {
      const itemType = kind === "codex" ? CODEX_ITEM_TYPES[type] : type === "tool" ? "tool" : type;
      if (kind === "codex" && type === "userMessage") continue;
      if (itemType === undefined) {
        unchecked.add(type);
        continue;
      }
      counts[itemType] = (counts[itemType] ?? 0) + count;
    }
  }
  return { counts, unchecked: [...unchecked] };
}

function actualItems(kind: ProviderKind, rows: ThreadRows, checked: ReadonlySet<string>) {
  const counts: Record<string, number> = {};
  const other: Record<string, number> = {};
  for (const item of providerItems(rows)) {
    const type = kind === "acp" && !NON_TOOL_ITEMS.has(item.type) ? "tool" : item.type;
    const target = checked.has(type) ? counts : other;
    target[type] = (target[type] ?? 0) + 1;
  }
  return { counts, other };
}

async function main(argv: ReadonlyArray<string>): Promise<number> {
  const { values } = NodeUtil.parseArgs({
    args: [...argv],
    options: {
      "base-dir": { type: "string" },
      project: { type: "string" },
      out: { type: "string" },
      sessions: { type: "string", default: "0" },
      provider: { type: "string", default: BENCH_PROVIDERS.claude.instanceId },
      model: { type: "string", default: BENCH_PROVIDERS.claude.model },
      ...PROVIDER_LOAD_FLAGS,
      turns: { type: "string", default: "2" },
      concurrency: { type: "string", default: "8" },
      "call-timeout": { type: "string", default: "300" },
      t3: { type: "string", default: NodePath.join(repoRoot, "apps/server/dist/bin.mjs") },
      help: { type: "boolean", default: false },
    },
  });
  if (values.help) {
    process.stdout.write(USAGE);
    return 0;
  }
  const baseDir = values["base-dir"];
  const project = values.project;
  const out = values.out;
  if (baseDir === undefined || project === undefined || out === undefined) {
    process.stderr.write(USAGE);
    return 2;
  }
  const turns = Number(values.turns);
  const concurrency = Number(values.concurrency);
  const callTimeoutMs = Number(values["call-timeout"]) * 1000;
  if (![turns, concurrency].every((value) => Number.isSafeInteger(value) && value >= 1)) {
    throw new Error("--turns and --concurrency must be integers of at least 1");
  }
  if (!Number.isFinite(callTimeoutMs) || callTimeoutMs <= 0) {
    throw new Error("--call-timeout must be a positive number of seconds");
  }
  const sessions = Number(values.sessions);
  if (!Number.isSafeInteger(sessions) || sessions < 0) throw new Error("--sessions must be >= 0");
  const plan = planSessions(
    parseProviderLoad(values, { sessions, provider: values.provider, model: values.model }),
  );
  if (plan.length === 0)
    throw new Error("No sessions: pass --sessions, --codex-sessions or --acp-sessions");

  const settingsPath = NodePath.join(baseDir, "userdata", "settings.json");
  const knobs = readBenchKnobs(settingsPath);
  const statsDirs = new Set(
    plan.flatMap((session) => {
      const dir = knobs[session.instanceId]?.BENCH_STATS_DIR;
      return dir === undefined ? [] : [dir];
    }),
  );
  NodeFS.mkdirSync(out, { recursive: true });
  NodeFS.rmSync(NodePath.join(out, FAILED_MARKER), { force: true });
  const startedAt = new Date();
  process.stderr.write(
    `[provider-check] ${plan.length} sessions × ${turns} turns: ${JSON.stringify(sessionsByInstance(plan))}\n`,
  );

  const runs: Array<SessionRun> = plan.map((session) => ({ session, threadId: null, turns: [] }));
  await forEachLimited(runs, concurrency, async (run) => {
    const { session } = run;
    for (let turn = 1; turn <= turns; turn++) {
      const text = `Bench turn ${turn} of ${session.kind} session ${session.index}.`;
      const args =
        run.threadId === null
          ? [
              "thread",
              "start",
              "--base-dir",
              baseDir,
              project,
              text,
              "--provider",
              session.instanceId,
              "--model",
              session.model,
            ]
          : ["thread", "send", "--base-dir", baseDir, run.threadId, text];
      const result = await runCli(
        process.execPath,
        [values.t3, ...args, "--wait", "--json"],
        project,
        callTimeoutMs,
      );
      const answer = parseThreadAnswer(result.stdout);
      run.threadId ??= answer?.threadId ?? null;
      run.turns.push({
        status: result.timedOut ? "hung" : (answer?.status ?? "cli-error"),
        error: result.timedOut
          ? `no answer after ${values["call-timeout"]} s`
          : (answer?.error ??
            (answer === null ? result.stderr.trim().slice(-500) || `exit ${result.code}` : null)),
        reply: answer?.reply ?? null,
        runId: answer?.runId ?? null,
      });
      if (run.turns.at(-1)?.status !== "completed" || run.threadId === null) break;
    }
  });

  const records = readStats(statsDirs);
  const db = new NodeSqlite.DatabaseSync(NodePath.join(baseDir, "userdata", "statev2.sqlite"), {
    readOnly: true,
  });
  const fakes: Partial<Record<ProviderKind, FakeModule>> = {};
  for (const kind of ["codex", "acp"] as const) {
    if (plan.some((session) => session.kind === kind)) {
      const module = NodePath.join(import.meta.dirname, BENCH_PROVIDERS[kind].launcher + ".mjs");
      fakes[kind] = (await import(module)) as FakeModule;
    }
  }

  const sessionResults = runs.map((run) => {
    const { session } = run;
    const completed = run.turns.filter((turn) => turn.status === "completed").length;
    const rows = run.threadId === null ? null : readThread(db, run.threadId);
    const played =
      rows?.nativeId == null ? [] : records.filter((record) => record.sessionId === rows.nativeId);
    const checks: Record<string, boolean> = {
      "every turn completed": run.turns.length === turns && completed === turns,
      "server runs completed":
        rows !== null &&
        rows.runs.length === turns &&
        rows.runs.every((status) => status === "completed"),
    };
    const fake = fakes[session.kind];
    let detail: Record<string, unknown> = {};
    if (fake !== undefined && rows !== null) {
      const expectations = played.map((record) =>
        fake.expectedTurn(record.transcript, record.turn, record.loops),
      );
      const expected = expectedItems(session.kind, expectations);
      const actual = actualItems(session.kind, rows, new Set(Object.keys(expected.counts)));
      checks["fake played one turn per completed turn"] =
        played.length === completed && played.every((record) => !record.aborted);
      checks["fake frames == transcript frames"] = played.every((record, index) =>
        sameCounts(record.frames, expectations[index]?.frames ?? {}),
      );
      checks["server items == transcript items"] = sameCounts(actual.counts, expected.counts);
      // The Codex adapter never completes a todo_list item; its steps carry the state.
      checks["every message and tool item completed"] = providerItems(rows).every(
        (item) => item.type === "todo_list" || item.status === "completed",
      );
      checks["replies == recorded final answers"] = run.turns.every(
        (turn, index) => turn.status !== "completed" || turn.reply === expectations[index]?.reply,
      );
      detail = {
        played: played.map(({ transcript, turn, loops, bytes, ms, frames }) => ({
          transcript,
          turn,
          loops,
          bytes,
          ms,
          frames,
        })),
        expectedItems: expected.counts,
        serverItems: actual.counts,
        otherServerItems: actual.other,
        uncheckedTranscriptItems: expected.unchecked,
      };
    } else if (rows !== null) {
      checks["fake played one turn per completed turn"] = played.length === completed;
      detail = {
        played: played.map(({ transcript, turn, loops, bytes, ms }) => ({
          transcript,
          turn,
          loops,
          bytes,
          ms,
        })),
      };
    }
    return {
      index: session.index,
      kind: session.kind,
      instanceId: session.instanceId,
      model: session.model,
      threadId: run.threadId,
      nativeSessionId: rows?.nativeId ?? null,
      turns: run.turns,
      serverRuns: rows?.runs ?? [],
      ...detail,
      checks,
      verdict: Object.values(checks).every(Boolean) ? "MATCH" : "MISMATCH",
    };
  });
  db.close();

  const turnsByProvider: Record<string, Record<string, number>> = {};
  for (const result of sessionResults) {
    const counts = (turnsByProvider[result.instanceId] ??= { requested: 0 });
    counts.requested = (counts.requested ?? 0) + turns;
    for (const [status, count] of Object.entries(countBy(result.turns, (turn) => turn.status))) {
      counts[status] = (counts[status] ?? 0) + count;
    }
  }
  const providerErrors = sessionResults.flatMap((result) =>
    result.turns.flatMap((turn, index) =>
      turn.status === "completed"
        ? []
        : [
            {
              instanceId: result.instanceId,
              threadId: result.threadId,
              turn: index + 1,
              status: turn.status,
              error: turn.error,
            },
          ],
    ),
  );
  const hung = providerErrors.filter((error) => error.status === "hung").length;
  const mismatched = sessionResults.filter(
    (result) =>
      result.verdict !== "MATCH" && result.turns.every((turn) => turn.status === "completed"),
  ).length;
  const problems = [
    ...(providerErrors.length > hung
      ? [`${providerErrors.length - hung} turns failed with a provider error`]
      : []),
    ...(hung > 0 ? [`${hung} turns hung`] : []),
    ...(mismatched > 0 ? [`${mismatched} sessions do not match their transcripts`] : []),
  ];
  const finishedAt = new Date();
  const result = {
    schema: 1,
    ok: problems.length === 0,
    error: problems.length === 0 ? null : problems.join("; "),
    verdict: problems.length === 0 ? "MATCH" : providerErrors.length > 0 ? "FAILED" : "MISMATCH",
    startedAt: startedAt.toISOString(),
    finishedAt: finishedAt.toISOString(),
    durationMs: finishedAt.getTime() - startedAt.getTime(),
    options: { turns, concurrency, callTimeoutS: Number(values["call-timeout"]), t3: values.t3 },
    knobs,
    sessionsByProvider: sessionsByInstance(plan),
    turnsByProvider,
    providerErrors,
    sessions: sessionResults,
  };
  NodeFS.writeFileSync(NodePath.join(out, RESULT_FILE), `${JSON.stringify(result, null, 2)}\n`);
  if (!result.ok) NodeFS.writeFileSync(NodePath.join(out, FAILED_MARKER), `${result.error}\n`);
  process.stderr.write(
    `[provider-check] ${result.verdict}: ${JSON.stringify(result.sessionsByProvider)} turns ${JSON.stringify(turnsByProvider)}${result.ok ? "" : ` — ${result.error}`}\n`,
  );
  return result.ok ? 0 : 1;
}

if (import.meta.main) {
  main(process.argv.slice(2)).then(
    (code) => process.exit(code),
    (error: unknown) => {
      process.stderr.write(
        `[provider-check] ${error instanceof Error ? error.message : String(error)}\n`,
      );
      process.exit(2);
    },
  );
}

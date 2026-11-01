// @effect-diagnostics nodeBuiltinImport:off globalFetch:off - drives the CLI and a real server as subprocesses.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeSqlite from "node:sqlite";

import { afterAll, describe, expect, it, onTestFinished } from "vite-plus/test";

import { BUILD_MIGRATION_HEAD } from "../../apps/server/src/persistence/databaseMigrationHead.ts";
import {
  contentFindings,
  emptyCounts,
  fillText,
  isFiller,
  keptAs,
  rewriteContent,
} from "./fixture/content.ts";
import { extendedFindings } from "./fixture/extended.ts";
import { UnsafePathError, assertSafeDestination, withSidecars } from "./fixture/guards.ts";
import { Spreader, apportion, planSynth } from "./fixture/plan.ts";
import { TOKEN_PATTERNS, redact, scanFileBytes, secretHits } from "./fixture/secrets.ts";
import { ShapeError, measureShape, parseShape, quantile, shapeRows } from "./fixture/shape.ts";

const ROOT = NodePath.resolve(import.meta.dirname, "..", "..");
const FIXTURE = NodePath.join(import.meta.dirname, "fixture.ts");
const SERVER = NodePath.join(ROOT, "apps", "server", "src", "bin.ts");
const EXAMPLE = NodePath.join(import.meta.dirname, "fixture", "example-shape.json");
const example = parseShape(JSON.parse(NodeFS.readFileSync(EXAMPLE, "utf8")));

/** A temp directory removed when the current test finishes. */
const scratch = (name: string) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), `fixture-${name}-`));
  onTestFinished(() => NodeFS.rmSync(directory, { recursive: true, force: true }));
  return directory;
};

// Credential-shaped samples, assembled at run time so no literal token sits in the source.
const SAMPLES: Record<string, string> = {
  "private-key": [
    `-----BEGIN RSA ${"PRIVATE"} KEY-----`,
    `MIIEow${"Ab12".repeat(15)}`,
    `${"Cd34".repeat(16)}==`,
    `-----END RSA ${"PRIVATE"} KEY-----`,
  ].join("\n"),
  "github-token": `gh${"p"}_${"A1b2".repeat(9)}`,
  "github-pat": `github_${"pat"}_${"A1b2_".repeat(5)}`,
  "gitlab-token": `glpat-${"Ab12-".repeat(5)}`,
  "anthropic-key": `sk-${"ant"}-${"Ab12_".repeat(5)}`,
  "openai-key": `sk-${"proj"}-${"Ab12".repeat(9)}`,
  "aws-access-key": `AKIA${"ABCD1234".repeat(2)}`,
  "google-api-key": `AIza${"Ab1_".repeat(9)}x`,
  "slack-token": `xox${"b"}-${"1234-".repeat(3)}`,
  "slack-webhook": `hooks.slack.com/services/${"T000/B000/".repeat(3)}`,
  "stripe-key": `sk_${"live"}_${"Ab12".repeat(6)}`,
  "npm-token": `npm_${"Ab12".repeat(9)}`,
  "huggingface-token": `hf_${"Ab12".repeat(8)}`,
  "sendgrid-key": `SG.${"Ab12_".repeat(4)}Ab.${"Ab12-".repeat(8)}Ab1`,
  "mailgun-key": `key-${"0123456789abcdef".repeat(2)}`,
  "tokenfeed-token": `tfe_${"Ab12".repeat(5)}`,
  jwt: `eyJ${"hbGciOi"}J.eyJ${"zdWIiOi"}J.${"Ab12".repeat(3)}`,
  bearer: `Bearer ${"Ab12.".repeat(5)}`,
  "url-credentials": `postgres://${"bench"}:${"hunter2"}@db.invalid/x`,
  "credential-assignment": `api_key = "${"Ab12".repeat(4)}"`,
  "google-oauth": `ya${"29"}.${"A0b1C2".repeat(8)}`,
  "slack-app-token": `xap${"p"}-1-${"A1B2".repeat(6)}`,
  "atlassian-token": `ATA${"TT"}${"xFfGF0".repeat(8)}`,
  "basic-auth": `Basic ${Buffer.from("bench:hunter2hunter2").toString("base64")}`,
};

const run = (args: ReadonlyArray<string>, env: NodeJS.ProcessEnv = {}) =>
  NodeChildProcess.spawnSync(process.execPath, [FIXTURE, ...args], {
    cwd: ROOT,
    env: { ...process.env, ...env },
    encoding: "utf8",
  });

const synth = (destination: string, scale: string, seed = "1", report?: string) => {
  const result = run([
    "synth",
    "--like",
    EXAMPLE,
    "--scale",
    scale,
    "--seed",
    seed,
    ...(report === undefined ? [] : ["--report", report]),
    destination,
  ]);
  expect(result.stderr).toBe("");
  expect(result.status).toBe(0);
};

// One synthetic database at scale 0.25 for every test that seeds and sanitises: each test
// mutates its own copy. Synthesising it once per test cost the most time on a loaded machine.
const sharedDirectory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fixture-shared-"));
afterAll(() => NodeFS.rmSync(sharedDirectory, { recursive: true, force: true }));
let sharedDatabase: string | undefined;

/** A fresh copy of the shared scale-0.25 synthetic database at `destination`. */
const synthCopy = (destination: string) => {
  if (sharedDatabase === undefined) {
    const database = NodePath.join(sharedDirectory, "synthetic.sqlite");
    synth(database, "0.25");
    sharedDatabase = database;
  }
  NodeFS.copyFileSync(sharedDatabase, destination);
};

/**
 * Migration 77's turn item counts against `COUNT(*)` of the items, both ways: rows whose count is
 * off, plus (thread, run) pairs with items but no row.
 */
const turnItemCountsMismatch = (path: string) => ({
  rows: count(path, `SELECT COUNT(*) AS n FROM orchestration_v2_projection_turn_item_counts`),
  wrong: count(
    path,
    `SELECT COUNT(*) AS n FROM (
       SELECT thread_id, COALESCE(run_id, '') AS run_id, COUNT(*) AS items
       FROM orchestration_v2_projection_turn_items GROUP BY thread_id, COALESCE(run_id, '')) AS i
     FULL OUTER JOIN orchestration_v2_projection_turn_item_counts AS c
       ON c.thread_id = i.thread_id AND c.run_id = i.run_id
     WHERE c.item_count IS NOT i.items`,
  ),
});

const count = (path: string, sql: string) => {
  const db = new NodeSqlite.DatabaseSync(path, { readOnly: true });
  try {
    return Number((db.prepare(sql).get() as { n: number }).n);
  } finally {
    db.close();
  }
};

const freePort = async () => {
  const server = NodeNet.createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as NodeNet.AddressInfo;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return port;
};

/** Boots a server on `database` in a scratch home with no provider on PATH; resolves its log once ready. */
const boot = async (database: string) => {
  const home = scratch("home");
  NodeFS.mkdirSync(NodePath.join(home, "userdata"));
  const copy = NodePath.join(home, "userdata", "statev2.sqlite");
  NodeFS.copyFileSync(database, copy);
  const port = await freePort();
  const child = NodeChildProcess.spawn(
    process.execPath,
    [SERVER, "serve", "--base-dir", home, "--port", String(port), "--host", "127.0.0.1"],
    {
      cwd: home,
      env: {
        HOME: home,
        PATH: `${NodePath.dirname(process.execPath)}${NodePath.delimiter}/usr/bin${NodePath.delimiter}/bin`,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const log: Array<string> = [];
  const stderr = NodeReadline.createInterface({ input: child.stderr });
  stderr.on("line", (line) => log.push(line));
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  for await (const line of NodeReadline.createInterface({ input: child.stdout })) {
    log.push(line);
    if (line.includes("T3 Code server is ready.")) break;
  }
  const environment = await fetch(`http://127.0.0.1:${port}/.well-known/t3/environment`);
  const body = (await environment.json()) as { databaseMigration: number };
  child.kill("SIGTERM");
  await exited;
  return {
    status: environment.status,
    migration: body.databaseMigration,
    log: log.join("\n"),
    copy,
  };
};

const recoveryCounts = (log: string) =>
  Object.fromEntries(
    [
      ...log.matchAll(
        /(terminalizedRuns|stoppedSessions|closedRequests|retiredEffects|requeuedEffects): (\d+)/g,
      ),
    ].map((match) => [match[1], Number(match[2])]),
  );

/** Leaves work a server would act on at boot in a synthetic database, plus credentials in text. */
const seedPendingWork = (path: string) => {
  const db = new NodeSqlite.DatabaseSync(path);
  const at = "2026-01-01T00:00:00.000Z";
  const native = db
    .prepare(
      `SELECT DISTINCT r.thread_id AS id FROM orchestration_v2_projection_runs r
       JOIN orchestration_v2_projection_threads t ON t.thread_id = r.thread_id
       WHERE json_extract(t.payload_json, '$.lineage.relationshipToParent') IS NULL
       ORDER BY r.thread_id LIMIT 4`,
    )
    .all() as Array<{ id: string }>;
  const [running, queued, limited, watched] = native.map((row) => row.id) as [
    string,
    string,
    string,
    string,
  ];
  const projectId = (
    db.prepare(`SELECT project_id AS id FROM projection_projects LIMIT 1`).get() as { id: string }
  ).id;
  db.prepare(
    `UPDATE orchestration_v2_projection_runs
     SET status = 'running', completed_at = NULL,
       payload_json = json_set(payload_json, '$.status', 'running', '$.completedAt', NULL)
     WHERE run_id = (SELECT run_id FROM orchestration_v2_projection_runs WHERE thread_id = ? ORDER BY ordinal DESC LIMIT 1)`,
  ).run(running);
  db.prepare(
    `INSERT INTO orchestration_v2_effect_outbox
       (effect_id, command_id, thread_id, effect_type, payload_json, status, available_at, created_at, updated_at)
     VALUES ('effect-1', 'command-1', ?, 'provider.turn.start', '{}', 'pending', ?, ?, ?)`,
  ).run(queued, at, at, at);
  db.prepare(
    `UPDATE orchestration_v2_projection_threads
     SET payload_json = json_set(payload_json, '$.limitRecovery',
       json_object('runId', 'run-x', 'resetAt', ?, 'autoResume', json('true')))
     WHERE thread_id = ?`,
  ).run(at, limited);
  db.prepare(
    `INSERT INTO scheduled_tasks (task_id, title, prompt, enabled, schedule_json, project_id,
       workspace_strategy_json, model_selection_json, runtime_mode, interaction_mode, created_by,
       creation_source, created_at, updated_at, next_run_at, last_run_status, run_count, webhook_token, webhook_secret)
     VALUES ('task-1', 'Nightly', 'Run the nightly job', 1, '{"type":"interval","minutes":5}', ?, '{"type":"root"}',
       '{"instanceId":"claudeAgent","model":"claude-sonnet-4-6"}', 'full-access', 'default', 'user', 'web',
       ?, ?, ?, 'never', 0, ?, ?)`,
  ).run(projectId, at, at, at, SAMPLES["github-token"] as string, "webhook-secret-value");
  db.prepare(
    `INSERT INTO scheduled_task_webhook_pending
       (delivery_id, task_id, coalesce_key, due_at, task_json, request_json, delivery_ids_json)
     VALUES ('delivery-1', 'task-1', 'key-1', 0, '{}', '{}', '["delivery-1"]')`,
  ).run();
  db.prepare(
    `INSERT INTO provider_session_runtime (thread_id, provider_name, adapter_key, status, last_seen_at)
     VALUES (?, 'claudeAgent', 'claudeAgent', 'running', ?)`,
  ).run(running, at);
  db.prepare(
    `INSERT INTO orchestration_v2_thread_launch_workflows
       (command_id, thread_id, project_id, status, title, created_at, updated_at)
     VALUES ('launch-1', 'thread-launching', ?, 'pending', 'Launching', ?, ?)`,
  ).run(projectId, at, at);
  db.prepare(
    `INSERT INTO peer_environment_links (environment_id, secret_name, label, urls_json, access, linked_at, expires_at)
     VALUES ('peer-1', 'peer-secret', 'Other machine', '["https://peer.invalid"]', 'full', ?, '2099-01-01T00:00:00.000Z')`,
  ).run(at);
  db.prepare(
    `INSERT INTO auth_sessions (session_id, subject, scopes, method, issued_at, expires_at)
     VALUES ('session-1', 'owner', '[]', 'pairing', ?, '2099-01-01T00:00:00.000Z')`,
  ).run(at);
  db.prepare(
    `INSERT INTO auth_pairing_links (id, credential, method, scopes, subject, created_at, expires_at)
     VALUES ('pair-1', 'pairing-credential', 'pairing', '[]', 'owner', ?, '2099-01-01T00:00:00.000Z')`,
  ).run(at);
  db.exec(`UPDATE projection_projects SET auto_pull = 1`);
  const watch = { startedAt: at, headSha: null, failedChecks: [], passed: false, passedChecks: [] };
  const link = {
    host: "github.com",
    repository: "bench/fixture",
    number: 7,
    url: "https://github.com/bench/fixture/pull/7",
    source: "agent",
    linkedAt: at,
    snapshot: null,
    stack: null,
    watch: { ...watch, remarksThrough: at, remarkIds: [], conflicting: false, wakes: 0 },
  };
  db.prepare(
    `UPDATE orchestration_v2_projection_threads
     SET payload_json = json_set(payload_json, '$.pullRequests', json(?)) WHERE thread_id = ?`,
  ).run(JSON.stringify([link]), watched);
  // An idle thread the settle sweep would pick: unsettled, last active long ago.
  const idle = (
    db
      .prepare(
        `SELECT thread_id AS id FROM orchestration_v2_projection_threads
         WHERE thread_id LIKE 'import:%' ORDER BY thread_id DESC LIMIT 1`,
      )
      .get() as { id: string }
  ).id;
  db.prepare(
    `UPDATE orchestration_v2_projection_threads
     SET payload_json = json_set(payload_json, '$.settledOverride', NULL, '$.settledAt', NULL)
     WHERE thread_id = ?`,
  ).run(idle);
  // Credentials pasted into a kept thread: a message, a turn item and an event payload.
  // A v1 import: a root with no runs, so no seeded thread's family can take it along. It also
  // links a pull request (no watch) on a branch: the sync and branch discovery read the host.
  const kept = (
    db
      .prepare(
        `SELECT thread_id AS id FROM orchestration_v2_projection_messages
     WHERE thread_id LIKE 'import:%' ORDER BY thread_id LIMIT 1`,
      )
      .get() as { id: string }
  ).id;
  db.prepare(
    `UPDATE orchestration_v2_projection_threads
     SET payload_json = json_set(payload_json, '$.pullRequests', json(?), '$.branch', 'feature/bench')
     WHERE thread_id = ?`,
  ).run(JSON.stringify([{ ...link, number: 8, watch: undefined }]), kept);
  db.prepare(
    `UPDATE orchestration_v2_projection_messages SET payload_json = json_set(payload_json, '$.text', ?)
     WHERE message_id = (SELECT message_id FROM orchestration_v2_projection_messages WHERE thread_id = ? LIMIT 1)`,
  ).run(`deploy with ${SAMPLES["anthropic-key"]}`, kept);
  db.prepare(
    `UPDATE orchestration_v2_projection_turn_items SET payload_json = json_set(payload_json, '$.title', ?)
     WHERE turn_item_id = (SELECT turn_item_id FROM orchestration_v2_projection_turn_items WHERE thread_id = ? LIMIT 1)`,
  ).run(SAMPLES["aws-access-key"] as string, kept);
  db.prepare(
    `UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.title', ?)
     WHERE sequence = (SELECT MAX(sequence) FROM orchestration_events WHERE stream_id = ?)`,
  ).run(SAMPLES["private-key"] as string, kept);
  // v1 imports the server would run on start: a transcript still to copy, a v1-only thread.
  const transcript = (
    db
      .prepare(
        `SELECT thread_id AS id FROM orchestration_v2_legacy_imports
         WHERE thread_id NOT IN (?, ?) ORDER BY thread_id LIMIT 1`,
      )
      .get(kept, idle) as { id: string }
  ).id;
  db.prepare(
    `UPDATE orchestration_v2_legacy_imports SET transcript_imported_at = NULL WHERE thread_id = ?`,
  ).run(transcript);
  db.prepare(
    `INSERT INTO projection_threads (thread_id, project_id, title, created_at, updated_at)
     VALUES ('v1-only-thread', ?, 'Old', ?, ?)`,
  ).run(projectId, at, at);
  // A queued run free to start, a snooze, a pending subagent delivery, a message still streaming
  // and a live v1 session: a boot starts, wakes, delivers or finishes each.
  const startable = (
    db
      .prepare(
        `SELECT r.thread_id AS id FROM orchestration_v2_projection_runs r
         JOIN orchestration_v2_projection_threads t ON t.thread_id = r.thread_id
         WHERE json_extract(t.payload_json, '$.lineage.relationshipToParent') IS NULL
           AND r.thread_id NOT IN (?, ?, ?, ?, ?, ?)
         ORDER BY r.thread_id LIMIT 1`,
      )
      .get(running, queued, limited, watched, kept, idle) as { id: string }
  ).id;
  db.prepare(
    `UPDATE orchestration_v2_projection_runs
     SET status = 'queued', completed_at = NULL,
       payload_json = json_set(payload_json, '$.status', 'queued', '$.completedAt', NULL, '$.startedAt', NULL)
     WHERE run_id = (SELECT run_id FROM orchestration_v2_projection_runs WHERE thread_id = ? ORDER BY ordinal DESC LIMIT 1)`,
  ).run(startable);
  db.prepare(
    `UPDATE orchestration_v2_projection_threads
     SET payload_json = json_set(payload_json, '$.snoozedUntil', ?, '$.snoozedAt', ?) WHERE thread_id = ?`,
  ).run("2099-01-01T00:00:00.000Z", at, idle);
  db.exec(`UPDATE orchestration_v2_projection_subagents
    SET payload_json = json_set(payload_json, '$.completionDelivery',
      json_object('state', 'pending', 'observedByRunId', json('null')))
    WHERE subagent_id = (SELECT MIN(subagent_id) FROM orchestration_v2_projection_subagents)`);
  db.prepare(
    `UPDATE orchestration_v2_projection_messages
     SET streaming = 1, payload_json = json_set(payload_json, '$.streaming', json('true'))
     WHERE message_id = (SELECT MIN(message_id) FROM orchestration_v2_projection_messages WHERE thread_id = ?)`,
  ).run(idle);
  db.prepare(
    `INSERT INTO projection_thread_sessions (thread_id, status, provider_name, updated_at)
     VALUES (?, 'running', 'claudeAgent', ?)`,
  ).run(kept, at);
  // A real usage-limit failure: the latest run failed on a usage_limit error item of its root node.
  const limitedRun = db
    .prepare(
      `SELECT run_id AS id, json_extract(payload_json, '$.rootNodeId') AS root
         FROM orchestration_v2_projection_runs WHERE thread_id = ? ORDER BY ordinal DESC LIMIT 1`,
    )
    .get(limited) as { id: string; root: string };
  const ended = "2027-01-01T00:00:00.000Z";
  db.prepare(
    `UPDATE orchestration_v2_projection_runs SET status = 'failed', completed_at = ?,
       payload_json = json_set(payload_json, '$.status', 'failed', '$.completedAt', ?)
     WHERE run_id = ?`,
  ).run(ended, ended, limitedRun.id);
  db.prepare(
    `UPDATE orchestration_v2_projection_threads
     SET payload_json = json_set(payload_json, '$.settledOverride', NULL, '$.settledAt', NULL)
     WHERE thread_id = ?`,
  ).run(limited);
  db.prepare(
    `UPDATE orchestration_v2_projection_turn_items SET type = 'error', status = 'failed', node_id = ?,
       payload_json = json_set(payload_json, '$.type', 'error', '$.status', 'failed', '$.nodeId', ?,
         '$.failure', json_object('class', 'usage_limit', 'message', 'Usage limit reached',
           'code', json('null'), 'retryable', json('false'), 'resetAt', '2099-01-01T00:00:00.000Z'))
     WHERE turn_item_id = (SELECT MIN(turn_item_id) FROM orchestration_v2_projection_turn_items WHERE run_id = ?)`,
  ).run(limitedRun.root, limitedRun.root, limitedRun.id);
  // A question the restart recovery keeps open for a message answer.
  db.exec(`UPDATE orchestration_v2_projection_runtime_requests
    SET status = 'pending', resolved_at = NULL,
      payload_json = json_set(payload_json, '$.status', 'pending', '$.resolvedAt', json('null'),
        '$.responseCapability', json_object('type', 'message'))
    WHERE runtime_request_id = (SELECT MIN(runtime_request_id) FROM orchestration_v2_projection_runtime_requests)`);
  // An app-owned delegation still running in its parent, its child finished but undelivered.
  const child = db
    .prepare(
      `SELECT t.thread_id AS id, json_extract(t.payload_json, '$.lineage.parentThreadId') AS parent
         FROM orchestration_v2_projection_threads t
         WHERE json_extract(t.payload_json, '$.lineage.relationshipToParent') = 'subagent'
           AND json_extract(t.payload_json, '$.forkedFrom.type') = 'node'
           AND EXISTS (SELECT 1 FROM orchestration_v2_projection_subagents s
             WHERE s.thread_id = json_extract(t.payload_json, '$.lineage.parentThreadId'))
         ORDER BY t.thread_id LIMIT 1`,
    )
    .get() as { id: string; parent: string };
  const task = (
    db
      .prepare(
        `SELECT subagent_id AS id FROM orchestration_v2_projection_subagents
         WHERE thread_id = ? ORDER BY subagent_id LIMIT 1`,
      )
      .get(child.parent) as { id: string }
  ).id;
  db.prepare(
    `UPDATE orchestration_v2_projection_subagents
     SET origin = 'app_owned', child_thread_id = ?, status = 'running', completed_at = NULL,
       payload_json = json_set(payload_json, '$.origin', 'app_owned', '$.childThreadId', ?,
         '$.status', 'running', '$.completedAt', json('null'), '$.result', json('null'))
     WHERE subagent_id = ?`,
  ).run(child.id, child.id, task);
  db.prepare(
    `UPDATE orchestration_v2_projection_nodes SET status = 'running', completed_at = NULL,
       payload_json = json_set(payload_json, '$.status', 'running', '$.completedAt', json('null'))
     WHERE node_id = ?`,
  ).run(task);
  db.prepare(
    `UPDATE orchestration_v2_projection_turn_items SET status = 'running',
       payload_json = json_set(payload_json, '$.status', 'running', '$.origin', 'app_owned',
         '$.childThreadId', ?, '$.completedAt', json('null'), '$.result', json('null'))
     WHERE type = 'subagent' AND json_extract(payload_json, '$.subagentId') = ?`,
  ).run(child.id, task);
  db.prepare(
    `DELETE FROM orchestration_v2_projection_context_transfers
     WHERE source_thread_id = ? AND type = 'subagent_result'`,
  ).run(child.id);
  // A task delegated to another machine, still without its result: its follower resumes on start.
  db.prepare(
    `UPDATE orchestration_v2_projection_subagents
     SET origin = 'app_owned', child_thread_id = NULL, status = 'running', completed_at = NULL,
       payload_json = json_set(payload_json, '$.origin', 'app_owned', '$.childThreadId', json('null'),
         '$.status', 'running', '$.completedAt', json('null'), '$.result', json('null'),
         '$.remoteChild', json_object('environmentId', 'peer-1', 'threadId', 'remote-thread', 'label', 'Peer'))
     WHERE subagent_id = (SELECT MAX(subagent_id) FROM orchestration_v2_projection_subagents WHERE subagent_id <> ?)`,
  ).run(task);
  db.close();
  return {
    busy: [running, queued, limited, watched, transcript, startable, child.id, child.parent],
    kept,
    idle,
    running,
    limited,
    task,
  };
};

/** Plants the review's credential cases where real data held them; returns what must not survive. */
const seedCredentials = (path: string) => {
  const db = new NodeSqlite.DatabaseSync(path);
  const at = "2026-01-01T00:00:00.000Z";
  const item = (type: string) =>
    (
      db
        .prepare(
          `SELECT turn_item_id AS id FROM orchestration_v2_projection_turn_items
           WHERE type = ? ORDER BY turn_item_id LIMIT 1`,
        )
        .get(type) as { id: string }
    ).id;
  const setItem = (id: string, path: string, value: unknown) =>
    db
      .prepare(
        `UPDATE orchestration_v2_projection_turn_items SET payload_json = json_set(payload_json, ?, json(?))
         WHERE turn_item_id = ?`,
      )
      .run(path, JSON.stringify(value), id);
  const command = item("command_execution");
  setItem(command, "$.output", `gh auth token\n${TOKENS.github}\n`);
  setItem(command, "$.input", FORMS.cfApiToken?.text);
  const tool = item("dynamic_tool");
  setItem(
    tool,
    "$.output",
    JSON.stringify({ stdout: `token:\n${TOKENS.github}`, aws: `\t${TOKENS.aws}` }),
  );
  const reasoning = item("reasoning");
  setItem(reasoning, "$.text", `\u001b[31m${TOKENS.openai}\u001b[0m then ${TOKENS.slack}`);
  const change = item("file_change");
  const pem = [
    `-----BEGIN RSA ${"PRIVATE"} KEY-----`,
    ...KEY_LINES,
    `-----END RSA ${"PRIVATE"} KEY-----`,
  ];
  setItem(
    change,
    "$.changes",
    pem.map((line) => `+${line}`),
  );
  setItem(
    change,
    "$.diffStr",
    [
      FORMS.anthropicAuth,
      FORMS.base64Anthropic,
      FORMS.hexPassword,
      FORMS.accessKey,
      FORMS.sessionToken,
    ]
      .map((form) => form?.text)
      .join("\n"),
  );
  db.prepare(
    `UPDATE orchestration_events SET payload_json = json_set(payload_json, '$.output', ?)
     WHERE sequence = (SELECT MIN(sequence) FROM orchestration_events WHERE event_type = 'turn-item.updated')`,
  ).run(`result\n${TOKENS.jwt}`);
  db.prepare(
    `UPDATE orchestration_events SET metadata_json = json_set(metadata_json, '$.note', ?)
     WHERE sequence = (SELECT MAX(sequence) FROM orchestration_events WHERE event_type = 'run.created')`,
  ).run(FORMS.basicAuth?.text ?? "");
  db.prepare(
    `UPDATE orchestration_v2_projection_messages SET payload_json = json_set(payload_json, '$.text', ?)
     WHERE message_id = (SELECT message_id FROM orchestration_v2_projection_messages ORDER BY message_id LIMIT 1)`,
  ).run(`${FORMS.googleOauth?.text} and ${FORMS.emptyUserUrl?.text}`);
  db.prepare(
    `UPDATE projection_thread_messages SET text = ? WHERE message_id = (SELECT MIN(message_id) FROM projection_thread_messages)`,
  ).run(`slack says\n${FORMS.slackXoxc?.text}`);
  const thread = (
    db
      .prepare(
        `SELECT thread_id AS id FROM orchestration_v2_projection_threads ORDER BY thread_id LIMIT 1`,
      )
      .get() as {
      id: string;
    }
  ).id;
  db.prepare(
    `INSERT INTO mcp_app_model_context (thread_id, item_id, server, tool, text, updated_at)
     VALUES (?, 'item-1', 'drive', 'oauth', ?, ?)`,
  ).run(thread, FORMS.refreshToken?.text ?? "", at);
  db.prepare(`UPDATE projection_projects SET scripts_json = ?`).run(
    JSON.stringify([
      {
        id: "migrate",
        name: "Migrate",
        command: `${FORMS.dbPassword?.text} vp run migrate`,
        icon: "play",
        runOnWorktreeCreate: false,
      },
    ]),
  );
  db.prepare(
    `UPDATE orchestration_v2_projection_provider_sessions SET payload_json = json_set(payload_json, '$.lastError', ?)`,
  ).run(FORMS.sessionToken?.text ?? "");
  db.prepare(
    `UPDATE orchestration_v2_projection_threads
     SET title = ?, payload_json = json_set(payload_json, '$.title', ?) WHERE thread_id = ?`,
  ).run(FORMS.awsSecret?.text ?? "", FORMS.awsSecret?.text ?? "", thread);
  db.prepare(
    `UPDATE orchestration_v2_projection_subagents SET payload_json = json_set(payload_json, '$.prompt', ?)
     WHERE subagent_id = (SELECT MIN(subagent_id) FROM orchestration_v2_projection_subagents)`,
  ).run(`${FORMS.slackXoxd?.text}\n${FORMS.slackApp?.text}`);
  db.prepare(
    `UPDATE orchestration_v2_projection_runs SET payload_json = json_set(payload_json, '$.note', ?)
     WHERE run_id = (SELECT MIN(run_id) FROM orchestration_v2_projection_runs)`,
  ).run(FORMS.atlassian?.text ?? "");
  db.close();
  return [
    ...Object.values(TOKENS),
    ...Object.values(FORMS).map((form) => form.secret),
    ...KEY_LINES,
  ];
};

describe("assertSafeDestination", () => {
  it("refuses a destination inside a T3 home, directly or through a symlink", () => {
    const root = scratch("guard");
    const home = NodePath.join(root, ".t3");
    NodeFS.mkdirSync(NodePath.join(home, "userdata"), { recursive: true });
    NodeFS.symlinkSync(home, NodePath.join(root, "link"));
    const destinations = [
      NodePath.join(home, "userdata", "statev2.sqlite"),
      NodePath.join(root, "link", "copy.sqlite"),
      NodePath.join(home, "..fixture.sqlite"),
      home,
    ];
    const refused = destinations.filter((destination) => {
      try {
        assertSafeDestination({ destination, homes: [home] });
        return false;
      } catch (error) {
        return error instanceof UnsafePathError;
      }
    });
    expect(refused).toEqual(destinations);
    expect(
      assertSafeDestination({ destination: NodePath.join(root, "copy.sqlite"), homes: [home] }),
    ).toBe(NodePath.join(NodeFS.realpathSync(root), "copy.sqlite"));
  });

  it("resolves `symlink/..` the way the OS does before checking a destination", () => {
    const root = scratch("dotdot");
    const home = NodePath.join(root, ".t3");
    NodeFS.mkdirSync(NodePath.join(home, "userdata"), { recursive: true });
    NodeFS.symlinkSync(NodePath.join(home, "userdata"), NodePath.join(root, "link"));
    // Built as text: NodePath.join would fold `link/..` away before the guard sees it.
    const physical = [root, "link", "..", "fixture.sqlite"].join(NodePath.sep);
    expect(() => assertSafeDestination({ destination: physical, homes: [home] })).toThrow(
      /inside the T3 home/,
    );
  });

  it("refuses to overwrite its source, the source's WAL, or any existing file", () => {
    const root = scratch("source");
    const source = NodePath.join(root, "state.sqlite");
    NodeFS.writeFileSync(source, "");
    expect(() => assertSafeDestination({ destination: source, source, homes: [] })).toThrow(
      /source database/,
    );
    expect(() =>
      assertSafeDestination({ destination: `${source}-wal`, source, homes: [] }),
    ).toThrow(/source database/);
    expect(() =>
      assertSafeDestination({
        destination: NodePath.join(root, "out.sqlite"),
        source,
        scratch: [source],
        homes: [],
      }),
    ).toThrow(/source database/);
    expect(() => assertSafeDestination({ destination: source, homes: [] })).toThrow(
      /already exists/,
    );
    const database = NodePath.join(root, "fixture.sqlite");
    expect(() =>
      assertSafeDestination({
        destination: `${database}-wal`,
        distinctFrom: withSidecars(database),
        homes: [],
      }),
    ).toThrow(/something else/);
  });
});

describe("secrets", () => {
  it("redacts every credential pattern to the same length, leaving nothing to find", () => {
    const kinds = [...TOKEN_PATTERNS.map(({ name }) => name), "credential-assignment"];
    expect(Object.keys(SAMPLES).sort()).toEqual(kinds.sort());
    const text = Object.values(SAMPLES).join(" | ");
    expect(secretHits(text, { entropy: false }).sort()).toEqual(Object.keys(SAMPLES).sort());
    const redacted = redact(text);
    expect(redacted.length).toBe(text.length);
    expect(secretHits(redacted)).toEqual([]);
  });

  it("keeps JSON valid at any nesting depth and removes whole private key blocks", () => {
    const pem = SAMPLES["private-key"] as string;
    const pgp = [
      `-----BEGIN PGP ${"PRIVATE"} KEY BLOCK-----`,
      "Version: GnuPG v1.4.23",
      "",
      `lQOYBF${"Ef56".repeat(15)}`,
      `-----END PGP ${"PRIVATE"} KEY BLOCK-----`,
    ].join("\n");
    const deepest = JSON.stringify({ password: "Gh78".repeat(5) });
    const inner = JSON.stringify({ password: "Ab12".repeat(5), key: pem, body: deepest });
    const payload = JSON.stringify({ input: inner, note: `${SAMPLES.bearer} and ${pgp}` });
    const redacted = redact(payload);
    expect(redacted.length).toBe(payload.length);
    const parsed = JSON.parse(redacted) as { input: string; note: string };
    const parsedInner = JSON.parse(parsed.input) as { body: string };
    expect(Object.keys(parsedInner)).toEqual(["password", "key", "body"]);
    expect(Object.keys(JSON.parse(parsedInner.body) as object)).toEqual(["password"]);
    expect(secretHits(redacted)).toEqual([]);
    const leftovers = ["Ab12", "Cd34", "Ef56", "Gh78"].filter((part) => redacted.includes(part));
    expect(leftovers).toEqual([]);
  });

  it("masks a PGP block whose armor header quotes text, in plain text and nested JSON", () => {
    const pgp = [
      `-----BEGIN PGP ${"PRIVATE"} KEY BLOCK-----`,
      'Comment: "benchmark"',
      "",
      `lQOYBF${"Ij90".repeat(15)}`,
      `-----END PGP ${"PRIVATE"} KEY BLOCK-----`,
    ].join("\n");
    const nested = JSON.stringify({ input: JSON.stringify({ output: pgp, after: "kept" }) });
    const plain = redact(pgp);
    const decoded = JSON.parse((JSON.parse(redact(nested)) as { input: string }).input) as {
      output: string;
      after: string;
    };
    expect([plain.includes("Ij90"), decoded.output.includes("Ij90"), decoded.after]).toEqual([
      false,
      false,
      "kept",
    ]);
    expect([secretHits(plain), secretHits(redact(nested))]).toEqual([[], []]);
  });

  it("redacts a key block beside a multi-megabyte string without exhausting the stack", () => {
    const payload = JSON.stringify({
      output: "x".repeat(5_000_000),
      key: SAMPLES["private-key"],
      quoted: 'say "hi" \\ done',
    });
    const decoded = JSON.parse(redact(payload)) as { output: string; key: string; quoted: string };
    expect([decoded.output.length, decoded.key.includes("Ab12"), decoded.quoted]).toEqual([
      5_000_000,
      false,
      'say "hi" \\ done',
    ]);
  });

  it("never carries a private key match across JSON fields", () => {
    const pem = SAMPLES["private-key"] as string;
    const item = {
      type: "command_execution",
      input: `grep -l "-----BEGIN RSA ${"PRIVATE"} KEY-----" keys/`,
      output: pem,
      exitCode: 0,
    };
    const once = JSON.stringify(item);
    const twice = JSON.stringify({ input: once });
    const decoded = JSON.parse(redact(once)) as typeof item;
    const nested = JSON.parse(
      (JSON.parse(redact(twice)) as { input: string }).input,
    ) as typeof item;
    expect([Object.keys(decoded), Object.keys(nested)]).toEqual([
      Object.keys(item),
      Object.keys(item),
    ]);
    expect([decoded.type, decoded.exitCode, nested.type]).toEqual([
      "command_execution",
      0,
      "command_execution",
    ]);
    expect([secretHits(redact(once)), secretHits(redact(twice))]).toEqual([[], []]);
    expect([decoded.output.includes("Ab12"), nested.output.includes("Cd34")]).toEqual([
      false,
      false,
    ]);
  });

  it("masks a URL password whole, up to the `@`, however short", () => {
    expect([redact("redis://:hunter2@cache.invalid"), redact("redis://:x@cache.invalid")]).toEqual([
      "redis://:*******@cache.invalid",
      "redis://:*@cache.invalid",
    ]);
  });

  it("finds a credential that straddles a chunk boundary of the raw file", () => {
    const path = NodePath.join(scratch("raw"), "file.bin");
    const token = SAMPLES["github-token"] as string;
    NodeFS.writeFileSync(path, `${" ".repeat(100 - 10)}${token} ${"y".repeat(300)}`);
    expect(Object.fromEntries(scanFileBytes(path, 100))).toEqual({ "github-token": 1 });
    NodeFS.writeFileSync(path, "x".repeat(1000));
    expect(scanFileBytes(path, 100).size).toBe(0);
  });
});

// Each credential the review found surviving: the text it sat in and the part that must go.
const TOKENS: Record<string, string> = {
  github: `gh${"p"}_${"A1b2".repeat(9)}`,
  anthropic: `sk-${"ant"}-${"Ab12_".repeat(5)}`,
  openai: `sk-${"proj"}-${"Ab12".repeat(9)}`,
  aws: `AKIA${"ABCD1234".repeat(2)}`,
  jwt: `eyJ${"hbGciOi"}J.eyJ${"zdWIiOi"}J.${"Ab12".repeat(3)}`,
  slack: `xox${"b"}-${"1234-".repeat(3)}`,
};
const KEY_LINES = [
  `MIIEowIBAAKCAQEA${"q8Zr3Vt9Lp2Xw7Ms".repeat(3)}`,
  `${"Hn4Kd8Yb1Rc6Gf0Tj".repeat(3)}a+b/`.slice(0, 64),
  `${"wE5uI7oP2aS9dF3gH".repeat(3)}Zx==`.slice(0, 64),
];
const FORMS: Record<string, { readonly text: string; readonly secret: string }> = {
  dbPassword: { text: `DB_PASSWORD=${"Zx9q".repeat(4)}`, secret: "Zx9q".repeat(4) },
  cfApiToken: { text: `export CF_API_TOKEN=${"Qw3rTy".repeat(7)}`, secret: "Qw3rTy".repeat(7) },
  awsSecret: {
    text: `AWS_SECRET_ACCESS_KEY="${"wJalrXUtnFEMI".repeat(3)}"`,
    secret: "wJalrXUtnFEMI".repeat(3),
  },
  anthropicAuth: { text: `ANTHROPIC_AUTH_TOKEN=${"Mn8v".repeat(8)}`, secret: "Mn8v".repeat(8) },
  refreshToken: {
    text: JSON.stringify({ refresh_token: `1//0g${"Ab3".repeat(10)}` }),
    secret: `1//0g${"Ab3".repeat(10)}`,
  },
  sessionToken: { text: `session_token: ${"Kp7w".repeat(8)}`, secret: "Kp7w".repeat(8) },
  accessKey: { text: `access_key=${"Hj5r".repeat(6)}`, secret: "Hj5r".repeat(6) },
  hexPassword: {
    text: `PASSWORD=${"0123456789abcdef".repeat(2)}`,
    secret: "0123456789abcdef".repeat(2),
  },
  googleOauth: { text: `ya${"29"}.${"A0b1C2".repeat(8)}`, secret: "A0b1C2".repeat(8) },
  slackXoxc: { text: `xox${"c"}-${"1234-".repeat(4)}abcd`, secret: `${"1234-".repeat(4)}abcd` },
  slackXoxd: { text: `xox${"d"}-${"Ab12".repeat(8)}`, secret: "Ab12".repeat(8) },
  slackApp: { text: `xap${"p"}-1-${"A1B2".repeat(6)}`, secret: "A1B2".repeat(6) },
  atlassian: { text: `ATA${"TT"}${"xFfGF0".repeat(8)}`, secret: "xFfGF0".repeat(8) },
  basicAuth: {
    text: `Authorization: Basic ${Buffer.from("bench:hunter2hunter2").toString("base64")}`,
    secret: Buffer.from("bench:hunter2hunter2").toString("base64"),
  },
  emptyUserUrl: { text: "redis://:s3cr3tPw0rd@cache.invalid:6379/0", secret: "s3cr3tPw0rd" },
  base64Anthropic: {
    text: `blob ${Buffer.from(`sk-${"ant"}-${"Ab12_".repeat(5)}`).toString("base64")} end`,
    secret: Buffer.from(`sk-${"ant"}-${"Ab12_".repeat(5)}`).toString("base64"),
  },
};

describe("credential detection", () => {
  it("finds and removes a token right after a newline, tab or ANSI escape, at any JSON depth", () => {
    const cases = Object.entries(TOKENS).flatMap(([name, token]) =>
      [
        JSON.stringify({ output: `ok\n${token}\n` }),
        JSON.stringify({ output: `\t${token}` }),
        JSON.stringify({ output: `\u001b[32m${token}\u001b[0m` }),
        JSON.stringify({ input: JSON.stringify({ result: `gh auth token\n${token}` }) }),
      ].map((text, index) => ({ name: `${name}#${index}`, token, text })),
    );
    const missed = cases
      .filter(({ text }) => secretHits(text).length === 0)
      .map(({ name }) => name);
    const kept = cases
      .filter(({ text, token }) => redact(text).includes(token))
      .map(({ name }) => name);
    expect({ missed, kept }).toEqual({ missed: [], kept: [] });
  });

  it("finds and removes every credential form, in plain text and inside JSON", () => {
    const cases = Object.entries(FORMS).flatMap(([name, { text, secret }]) => [
      { name, secret, text },
      { name: `${name}#json`, secret, text: JSON.stringify({ output: `x\n${text}\n` }) },
    ]);
    const missed = cases
      .filter(({ text }) => secretHits(text).length === 0)
      .map(({ name }) => name);
    const kept = cases
      .filter(({ text, secret }) => redact(text).includes(secret))
      .map(({ name }) => name);
    expect({ missed, kept }).toEqual({ missed: [], kept: [] });
  });

  it("removes a private key whose lines sit in separate JSON strings", () => {
    const pem = [
      `-----BEGIN RSA ${"PRIVATE"} KEY-----`,
      ...KEY_LINES,
      `-----END RSA ${"PRIVATE"} KEY-----`,
    ];
    const diff = JSON.stringify({ type: "file_change", lines: pem.map((line) => `+${line}`) });
    const redacted = redact(diff);
    expect(KEY_LINES.filter((line) => redacted.includes(line))).toEqual([]);
  });
});

describe("shape", () => {
  it("rejects malformed shapes by the field at fault", () => {
    expect(() => parseShape({ ...example, format: "other" })).toThrow(ShapeError);
    expect(() =>
      parseShape({
        ...example,
        distributions: {
          turnItemsPerThread: [
            [0.5, 10],
            [0.9, 5],
            [1, 20],
          ],
        },
      }),
    ).toThrow(/ascend/);
    expect(() =>
      parseShape({ ...example, distributions: { turnItemsPerThread: [[0.5, 10]] } }),
    ).toThrow(/q = 1/);
  });

  it("measures tables whose names need quoting", () => {
    const path = NodePath.join(scratch("quoted"), "odd.sqlite");
    const db = new NodeSqlite.DatabaseSync(path);
    db.exec(`CREATE TABLE "odd""name" (payload_json TEXT); CREATE TABLE "order" (text TEXT);`);
    db.exec(`INSERT INTO "odd""name" VALUES ('{}'); INSERT INTO "order" VALUES ('x');`);
    const measured = measureShape(db);
    db.close();
    expect([measured.tables['odd"name']?.rows, measured.tables.order?.rows]).toEqual([1, 1]);
  });

  it("interpolates quantiles between recorded points", () => {
    const quantiles = [
      [0, 0],
      [0.5, 10],
      [1, 30],
    ] as const;
    expect([0, 0.25, 0.5, 0.75, 1].map((q) => quantile(quantiles, q))).toEqual([0, 5, 10, 20, 30]);
  });
});

describe("planSynth", () => {
  it("plans every table of the example within 10 % of its rows at scale 1", () => {
    const plan = planSynth(example, 1, 2);
    const off = Object.entries(plan.expected).filter(([table, planned]) => {
      const target = shapeRows(example, table);
      return target > 0 && Math.abs(planned - target) / target > 0.1;
    });
    expect(off).toEqual([]);
  });

  it("scales the thread count and keeps the per-thread distribution, long-history tail included", () => {
    const plan = planSynth(example, 0.5, 1);
    expect(plan.threads.length).toBe(60);
    expect(plan.totals.turnItems).toBe(
      Math.round(shapeRows(example, "orchestration_v2_projection_turn_items") * 0.5),
    );
    const items = plan.threads.map((thread) =>
      thread.runs.reduce((acc, turn) => acc + turn.items.length, 0),
    );
    const sorted = items.toSorted((a, b) => a - b);
    const p50 = sorted[Math.floor(sorted.length / 2)] as number;
    const expected = quantile(example.distributions?.turnItemsPerThread ?? [[1, 0]], 0.5);
    expect(Math.abs(p50 - expected) / expected).toBeLessThan(0.25);
    expect(Math.max(...items)).toBeGreaterThan(
      quantile(example.distributions?.turnItemsPerThread ?? [[1, 0]], 0.95),
    );
  });

  it("branches each fork from a run of a native root", () => {
    const plan = planSynth(example, 1, 3);
    const forks = plan.threads.filter((thread) => thread.fork !== null);
    expect(forks.length).toBe(example.lineage?.fork);
    const sources = forks.map((thread) => plan.threads[thread.fork?.thread ?? -1]);
    expect(
      sources.every(
        (source, index) =>
          source !== undefined &&
          !source.runless &&
          source.parent === null &&
          source.fork === null &&
          source.runs.length > (forks[index]?.fork?.run ?? Infinity),
      ),
    ).toBe(true);
  });

  it("is deterministic for a seed", () => {
    expect(JSON.stringify(planSynth(example, 0.2, 7))).toBe(
      JSON.stringify(planSynth(example, 0.2, 7)),
    );
    expect(JSON.stringify(planSynth(example, 0.2, 7))).not.toBe(
      JSON.stringify(planSynth(example, 0.2, 8)),
    );
  });

  it("deals and apportions exact totals", () => {
    const spreader = new Spreader(7, 3);
    expect([spreader.take(), spreader.take(), spreader.take()]).toEqual([2, 2, 3]);
    expect(apportion(10, [1, 1, 2])).toEqual([3, 2, 5]);
  });
});

describe("fixture CLI", () => {
  it("sanitise refuses a destination inside ~/.t3 and overwriting its source, writing nothing", () => {
    const home = scratch("cli-home");
    NodeFS.mkdirSync(NodePath.join(home, ".t3", "userdata"), { recursive: true });
    const source = NodePath.join(scratch("cli-source"), "state.sqlite");
    new NodeSqlite.DatabaseSync(source).close();
    const before = NodeCrypto.createHash("sha256")
      .update(NodeFS.readFileSync(source))
      .digest("hex");
    const intoHome = NodePath.join(home, ".t3", "userdata", "statev2.sqlite");
    const refusedHome = run(["sanitise", source, intoHome], { HOME: home });
    const refusedSource = run(["sanitise", source, source], { HOME: home });
    const fresh = NodePath.join(scratch("cli-out"), "fixture.sqlite");
    const refusedReport = run(["sanitise", "--report", source, source, fresh], { HOME: home });
    expect([refusedHome.status, refusedSource.status, refusedReport.status]).toEqual([2, 2, 2]);
    expect(NodeFS.existsSync(fresh)).toBe(false);
    expect(refusedHome.stderr).toMatch(/inside the T3 home/);
    expect(refusedSource.stderr).toMatch(/overwrite the source database/);
    expect(NodeFS.existsSync(intoHome)).toBe(false);
    expect(NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(source)).digest("hex")).toBe(
      before,
    );
  });

  it("sanitise refuses a --report that reaches the live database through `symlink/..`", () => {
    const home = scratch("cli-dotdot");
    const userdata = NodePath.join(home, ".t3", "userdata");
    NodeFS.mkdirSync(userdata, { recursive: true });
    const live = NodePath.join(userdata, "statev2.sqlite");
    new NodeSqlite.DatabaseSync(live).close();
    const before = NodeFS.readFileSync(live);
    const work = scratch("cli-dotdot-work");
    NodeFS.symlinkSync(userdata, NodePath.join(work, "link"));
    const source = NodePath.join(work, "source.sqlite");
    new NodeSqlite.DatabaseSync(source).close();
    const report = [work, "link", "..", "userdata", "statev2.sqlite"].join(NodePath.sep);
    const refused = run(
      ["sanitise", "--report", report, source, NodePath.join(work, "out.sqlite")],
      { HOME: home },
    );
    expect(refused.status).toBe(2);
    expect(NodeFS.readFileSync(live).equals(before)).toBe(true);
    expect(NodeFS.existsSync(NodePath.join(work, "out.sqlite"))).toBe(false);
  });

  it("synth builds the example within 10 % of its size and every table's rows at scale 1", () => {
    const directory = scratch("synth");
    const report = NodePath.join(directory, "report.json");
    synth(NodePath.join(directory, "example.sqlite"), "1", "2", report);
    const result = JSON.parse(NodeFS.readFileSync(report, "utf8")) as {
      sizeDeviation: number;
      tables: Array<{ table: string; target: number; deviation: number | null }>;
    };
    expect(Math.abs(result.sizeDeviation)).toBeLessThan(0.1);
    const off = result.tables.filter((row) => row.target > 0 && Math.abs(row.deviation ?? 1) > 0.1);
    expect(off).toEqual([]);
    // Migration 77's turn item counts, kept by its triggers while synth writes: exact.
    expect(turnItemCountsMismatch(NodePath.join(directory, "example.sqlite"))).toEqual({
      rows: expect.any(Number),
      wrong: 0,
    });
  });

  it("sanitise leaves a seeded fixture with nothing runnable and no credentials", () => {
    const directory = scratch("sanitise");
    const seeded = NodePath.join(directory, "seeded.sqlite");
    synthCopy(seeded);
    const { busy, kept, idle, running, limited, task } = seedPendingWork(seeded);
    const before = run(["scan", seeded]);
    expect(before.status).toBe(1);
    const found = (JSON.parse(before.stdout) as { runnable: Record<string, number> }).runnable;
    const missed = Object.entries(found).filter(([, value]) => value === 0);
    expect(missed).toEqual([]);
    const report = NodePath.join(directory, "report.json");
    const clean = NodePath.join(directory, "clean.sqlite");
    const result = run(["sanitise", "--report", report, seeded, clean]);
    expect(result.stderr).toBe("");
    expect(result.status).toBe(0);
    const summary = JSON.parse(NodeFS.readFileSync(report, "utf8")) as {
      check: {
        secretHits: object;
        credentialColumns: object;
        rawHits: object;
        runnable: Record<string, number>;
      };
    };
    expect(summary.check.secretHits).toEqual({});
    expect(summary.check.credentialColumns).toEqual({});
    expect(summary.check.rawHits).toEqual({});
    expect(Object.values(summary.check.runnable).every((value) => value === 0)).toBe(true);
    // Every thread stays: start-up work is done in place, as a restart does it.
    const kept_ = count(
      clean,
      `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads WHERE thread_id IN ('${busy.join("','")}')`,
    );
    expect(kept_).toBe(new Set(busy).size);
    // ... plus the v1-only thread, which the server's importer turned into a v2 thread.
    expect(count(seeded, `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads`) + 1).toBe(
      count(clean, `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads`),
    );
    const cancelled = `SELECT COUNT(*) AS n FROM orchestration_v2_projection_runs
      WHERE thread_id = '${running}' AND status = 'cancelled'
        AND ordinal = (SELECT MAX(ordinal) FROM orchestration_v2_projection_runs WHERE thread_id = '${running}')`;
    expect(count(clean, cancelled)).toBe(1);
    // A usage-limit failure stays, handled with auto-resume off; questions expire; the delegation
    // ends with its child's result.
    const handled = `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads WHERE thread_id = '${limited}'
      AND json_extract(payload_json, '$.limitRecovery.autoResume') = 0`;
    expect(count(clean, handled)).toBe(1);
    expect(
      count(
        clean,
        `SELECT COUNT(*) AS n FROM orchestration_v2_projection_runtime_requests WHERE status = 'pending'`,
      ),
    ).toBe(0);
    // Migration 77's turn item counts stay, exact for the items the fixture holds.
    const counted = turnItemCountsMismatch(clean);
    expect([counted.rows > 0, counted.wrong]).toEqual([true, 0]);
    const settled = `SELECT COUNT(*) AS n FROM orchestration_v2_projection_subagents
      WHERE subagent_id = '${task}' AND status = 'completed' AND json_extract(payload_json, '$.result') IS NOT NULL`;
    expect(count(clean, settled)).toBe(1);
    expect(
      count(
        clean,
        `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads WHERE thread_id = '${kept}'`,
      ),
    ).toBe(1);
    const autoSettleOff = `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads
      WHERE thread_id = '${idle}' AND json_extract(payload_json, '$.autoSettleDisabledAt') IS NOT NULL`;
    expect(count(clean, autoSettleOff)).toBe(1);
    const unlinked = `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads WHERE thread_id = '${kept}'
      AND json_array_length(payload_json, '$.pullRequests') = 0 AND json_extract(payload_json, '$.branch') IS NULL`;
    expect(count(clean, unlinked)).toBe(1);
  });
});

describe("sanitise content", () => {
  it("leaves none of the review's planted credentials, and scan fails on any planted afterwards", () => {
    const directory = scratch("credentials");
    const seeded = NodePath.join(directory, "seeded.sqlite");
    synthCopy(seeded);
    const secrets = seedCredentials(seeded);
    const clean = NodePath.join(directory, "clean.sqlite");
    const result = run(["sanitise", seeded, clean]);
    expect(result.status).toBe(0);
    const raw = NodeFS.readFileSync(clean).toString("latin1");
    const survivors = secrets.filter(
      (secret) => raw.includes(secret) || raw.includes(JSON.stringify(secret).slice(1, -1)),
    );
    expect(survivors).toEqual([]);
    expect(run(["scan", clean]).status).toBe(0);

    // Planted afterwards, each in its own row, all in one copy and one scan: a token in an id
    // field, prose in a message, and a random value under a session name (P1-002 reads it as one).
    const planted = NodePath.join(directory, "planted.sqlite");
    NodeFS.copyFileSync(clean, planted);
    const db = new NodeSqlite.DatabaseSync(planted);
    db.prepare(
      `UPDATE orchestration_v2_projection_turn_items
       SET payload_json = json_set(payload_json, '$.nativeItemRef', json_object('driver', 'claudeAgent', 'nativeId', ?, 'strength', 'strong'))
       WHERE turn_item_id = (SELECT MIN(turn_item_id) FROM orchestration_v2_projection_turn_items)`,
    ).run(TOKENS.github as string);
    db.prepare(
      `UPDATE orchestration_v2_projection_messages SET payload_json = json_set(payload_json, '$.text', ?)
       WHERE message_id = (SELECT MIN(message_id) FROM orchestration_v2_projection_messages)`,
    ).run("the staging db password is hunter two, keep it quiet");
    db.prepare(
      `UPDATE orchestration_v2_projection_messages SET payload_json = json_set(payload_json, '$.sessionId', ?)
       WHERE message_id = (SELECT MAX(message_id) FROM orchestration_v2_projection_messages)`,
    ).run(`${"q8Zr3Vt9Lp2Xw7Ms".repeat(2)}`);
    db.close();
    const scan = run(["scan", planted]);
    const found = JSON.parse(scan.stdout) as {
      secretHits: Record<string, number>;
      notBlanked: Record<string, number>;
    };
    expect([
      scan.status,
      (found.secretHits["orchestration_v2_projection_turn_items:github-token"] ?? 0) > 0,
      (found.notBlanked["orchestration_v2_projection_messages.payload_json"] ?? 0) > 0,
      (found.secretHits["orchestration_v2_projection_messages:high-entropy"] ?? 0) > 0,
    ]).toEqual([1, true, true, true]);
  });
});

describe("content blanking", () => {
  it("fills text to the same UTF-8 and UTF-16 size, keeps lines and escapes, and recognises only filler", () => {
    const texts = [
      "short",
      "deploy with the staging key, then restart",
      'line one\n\tline "two" \\ /usr/local/bin\nnaïve café 日本語 🚀 done',
      "x".repeat(10_000),
    ];
    const filled = texts.map((text, index) => fillText(text, 12_345 + index));
    expect(filled.map((text) => Buffer.byteLength(text))).toEqual(
      texts.map((text) => Buffer.byteLength(text)),
    );
    expect(filled.map((text) => text.length)).toEqual(texts.map((text) => text.length));
    expect(filled[2]?.split("\n").length).toBe(3);
    expect(filled.every(isFiller)).toBe(true);
    expect(texts.filter(isFiller)).toEqual([]);
    expect(isFiller(`${filled[1]}x`)).toBe(false);
  });

  it("keeps only the allowlist and blanks the rest, at any JSON depth", () => {
    const payload = JSON.stringify({
      id: "turn-item:provider:claudeAgent:native-item:toolu_01",
      type: "command_execution",
      status: "completed",
      createdAt: "2026-01-01T00:00:00.000Z",
      model: "claude-sonnet-4-6",
      exitCode: 0,
      input: JSON.stringify({ command: "cat ~/.netrc", cwd: "/Users/someone/project" }),
      env: { DB_PASSWORD: "plain", "/odd key": "value" },
      token: "completed",
      tokenId: "abc",
    });
    const counts = emptyCounts();
    const out = JSON.parse(rewriteContent(payload, "payload_json", "blank", counts)) as Record<
      string,
      unknown
    >;
    expect([out.id, out.type, out.status, out.createdAt, out.model, out.exitCode]).toEqual([
      "turn-item:provider:claudeAgent:native-item:toolu_01",
      "command_execution",
      "completed",
      "2026-01-01T00:00:00.000Z",
      "claude-sonnet-4-6",
      0,
    ]);
    const input = JSON.parse(out.input as string) as { command: string; cwd: string };
    expect([isFiller(input.command), isFiller(input.cwd), input.cwd.length]).toEqual([
      true,
      true,
      "/Users/someone/project".length,
    ]);
    // A key past the identifier limit is rewritten whatever its letters; planted back, it fails.
    const long = "mypasswordis".repeat(8);
    // Letters filler could have written are still rewritten when the original key is not allowed.
    const fillerShaped = "banana".repeat(16);
    const reshaped = Object.keys(
      JSON.parse(
        rewriteContent(JSON.stringify({ [fillerShaped]: "x" }), null, "blank", emptyCounts()),
      ),
    );
    expect(reshaped[0] === fillerShaped).toBe(false);
    const plantedBack: Array<string> = [];
    contentFindings(JSON.stringify({ [fillerShaped]: "vamoa" }), null, "blank", (kind) =>
      plantedBack.push(kind),
    );
    expect(plantedBack).toEqual(["key"]);
    const rewritten = Object.keys(
      JSON.parse(rewriteContent(JSON.stringify({ [long]: "x" }), null, "blank", emptyCounts())),
    );
    expect([rewritten.length, rewritten[0] === long, rewritten[0]?.length]).toEqual([1, false, 96]);
    const findings: Array<string> = [];
    contentFindings(JSON.stringify({ [long]: "vamoa" }), null, "blank", (kind) =>
      findings.push(kind),
    );
    expect(findings).toEqual(["key"]);
    // Rewritten keys of base64-line length carry separators: neither check reads them as a key body.
    for (const length of [60, 64, 65, 76]) {
      const path = `/${"x".repeat(length - 1)}`;
      const stored = rewriteContent(JSON.stringify({ [path]: "x" }), null, "blank", emptyCounts());
      const rows: Array<string> = [];
      contentFindings(stored, null, "blank", (kind) => rows.push(kind));
      extendedFindings(stored, (row) => rows.push(row));
      expect([length, Object.keys(JSON.parse(stored))[0]?.length, rows]).toEqual([
        length,
        length,
        [],
      ]);
    }
    const env = out.env as Record<string, string>;
    expect(Object.keys(env)[0]).toBe("DB_PASSWORD");
    // A rewritten key has at least ten letters, so keys of one object stay distinct.
    expect(Object.keys(env)[1]).toMatch(/^[a-z]{10}$/);
    expect(Object.values(env).every(isFiller)).toBe(true);
    // A credential-named key keeps no value P1-002's check could read; an id under one is blanked.
    expect([isFiller(out.token as string), isFiller(out.tokenId as string)]).toEqual([true, true]);
    expect(keptAs("thread_id", `gh${"p"}_${"A1b2".repeat(9)}`)).toBe("id");
    expect(
      isFiller(rewriteContent(`gh${"p"}_${"A1b2".repeat(9)}`, "thread_id", "blank", emptyCounts())),
    ).toBe(true);
  });

  it("--keep-content removes every planted credential with the pattern layer alone", () => {
    const directory = scratch("keep-content");
    const seeded = NodePath.join(directory, "seeded.sqlite");
    synthCopy(seeded);
    const secrets = seedCredentials(seeded);
    const kept = NodePath.join(directory, "kept.sqlite");
    const sanitised = run(["sanitise", "--keep-content", seeded, kept]);
    expect(sanitised.status).toBe(0);
    // Its summary names the checks it ran, not the blank-mode filler check.
    expect(sanitised.stdout).not.toMatch(/allowlisted or filler/);
    const raw = NodeFS.readFileSync(kept).toString("latin1");
    const survivors = secrets.filter(
      (secret) => raw.includes(secret) || raw.includes(JSON.stringify(secret).slice(1, -1)),
    );
    expect(survivors).toEqual([]);
    expect([run(["scan", "--keep-content", kept]).status, run(["scan", kept]).status]).toEqual([
      0, 1,
    ]);
  });
});

describe("allowlist limits", () => {
  const secret = "q8Zr3Vt9Lp2Xw7MsHn4Kd8Yb";

  it("blanks ids under credential-named ancestors and token-like keys, keeping names and prefixed ids", () => {
    const payload = JSON.stringify({
      credentials: { id: secret, clientId: secret },
      session: { token: { refreshTokenId: secret } },
      threadId: "thread-1",
      [secret]: 1,
      cacheReadInputTokens: 7,
      mcp__atlassian__getJiraIssue: 2,
      toolu_01ABCdefGHIjklMNOpqrSTU: 3,
    });
    const out = JSON.parse(rewriteContent(payload, "payload_json", "blank", emptyCounts())) as {
      credentials: { id: string; clientId: string };
      session: { token: { refreshTokenId: string } };
      threadId: string;
    };
    expect([
      isFiller(out.credentials.id),
      isFiller(out.credentials.clientId),
      isFiller(out.session.token.refreshTokenId),
      out.threadId,
    ]).toEqual([true, true, true, "thread-1"]);
    const keys = Object.keys(out);
    expect(keys.includes(secret)).toBe(false);
    expect(
      [
        "cacheReadInputTokens",
        "mcp__atlassian__getJiraIssue",
        "toolu_01ABCdefGHIjklMNOpqrSTU",
      ].filter((key) => !keys.includes(key)),
    ).toEqual([]);
    const planted = (value: unknown) => {
      const kinds: Array<string> = [];
      contentFindings(JSON.stringify(value), null, "blank", (kind) => kinds.push(kind));
      return kinds;
    };
    expect([planted({ credentials: { id: secret } }), planted({ [secret]: 1 })]).toEqual([
      ["not-blanked"],
      ["key"],
    ]);
  });

  it("zeroes numbers under credential names, keeping counts such as token usage", () => {
    const payload = JSON.stringify({
      pin: 123456,
      otp: 654321,
      passcode: -42.5e3,
      inputTokens: 1234,
      exitCode: 1,
      auth: { expiresIn: 3600 },
      // No exemption for `pass`: a tally there costs a bench fixture nothing, a password a lot.
      runs: { pass: 12, fail: 1 },
      PASS: 3,
      db_pass: 20241231,
      smtp: { pass: 654321 },
      dbPass: 4242,
    });
    const stored = rewriteContent(payload, "payload_json", "blank", emptyCounts());
    expect(stored.length).toBe(payload.length);
    const out = JSON.parse(stored) as Record<string, unknown>;
    expect([
      out.pin,
      out.otp,
      out.passcode,
      out.inputTokens,
      out.exitCode,
      out.auth,
      out.runs,
      out.PASS,
      out.db_pass,
      out.smtp,
      out.dbPass,
    ]).toEqual([0, 0, 0, 1234, 1, { expiresIn: 0 }, { pass: 0, fail: 1 }, 0, 0, { pass: 0 }, 0]);
    // The re-review's cases, planted after sanitising: the check must report each.
    const planted = (value: unknown) => {
      const kinds: Array<string> = [];
      contentFindings(JSON.stringify(value), null, "blank", (kind) => kinds.push(kind));
      return kinds;
    };
    expect([
      planted({ pin: 123456 }),
      planted({ db_pass: 20241231 }),
      planted({ pass: 77310042 }),
      planted({ smtp: { pass: 654321 } }),
      planted({ pass: { id: "q8Zr3Vt9Lp2Xw7MsHn4Kd8Yb" } }),
    ]).toEqual([
      ["credential-number"],
      ["credential-number"],
      ["credential-number"],
      ["credential-number"],
      ["not-blanked"],
    ]);
    // ... and sanitising blanks an id under a pass-named ancestor.
    const nested = JSON.parse(
      rewriteContent(
        JSON.stringify({ pass: { id: "q8Zr3Vt9Lp2Xw7MsHn4Kd8Yb" } }),
        "payload_json",
        "blank",
        emptyCounts(),
      ),
    ) as { pass: { id: string } };
    expect(isFiller(nested.pass.id)).toBe(true);
  });

  // The project timeout (60 s) is the linearity check: a quadratic rule needs minutes on 1 MB here.
  it("runs the extended rules in linear time and walks deep JSON without recursion", () => {
    const rows: Array<string> = [];
    extendedFindings("TOKEN".repeat(200_000), (row) => rows.push(row));
    extendedFindings("t3_session".repeat(100_000), (row) => rows.push(row));
    expect(rows).toEqual([]);
    const found: Array<string> = [];
    extendedFindings(`export DB_PASSWORD=${"Zx9q".repeat(4)}`, (row) => found.push(row));
    extendedFindings(`t3_session_id=${"Ab12".repeat(5)}`, (row) => found.push(row));
    // Both linear rules still find what their regexes found.
    expect(
      ["raw|p1002:secret env var", "raw|p1002:t3 session cookie"].filter(
        (row) => !found.includes(row),
      ),
    ).toEqual([]);
    const deepArray = `${"[".repeat(5000)}"x"${"]".repeat(5000)}`;
    const deepObject = `${'{"a":'.repeat(5000)}{"/k":1}${"}".repeat(5000)}`;
    expect(() => {
      extendedFindings(deepArray, () => undefined);
      extendedFindings(deepObject, () => undefined);
      rewriteContent(deepObject, null, "blank", emptyCounts());
    }).not.toThrow();
  });
});

describe("v1 imports", () => {
  it("scan counts pending v1 shell and transcript imports, and sanitise leaves none", () => {
    const directory = scratch("legacy");
    const seeded = NodePath.join(directory, "seeded.sqlite");
    synthCopy(seeded);
    const db = new NodeSqlite.DatabaseSync(seeded);
    const at = "2026-01-01T00:00:00.000Z";
    db.exec(`UPDATE orchestration_v2_legacy_imports SET transcript_imported_at = NULL
      WHERE thread_id = (SELECT MIN(thread_id) FROM orchestration_v2_legacy_imports)`);
    db.prepare(
      `INSERT INTO projection_threads (thread_id, project_id, title, created_at, updated_at)
       VALUES ('v1-only-thread', (SELECT MIN(project_id) FROM projection_projects), 'Old', ?, ?)`,
    ).run(at, at);
    db.close();
    const before = run(["scan", seeded]);
    const found = (JSON.parse(before.stdout) as { runnable: Record<string, number> }).runnable;
    expect([found.legacyShellImports, found.legacyTranscriptImports]).toEqual([1, 1]);
    const clean = NodePath.join(directory, "clean.sqlite");
    expect(run(["sanitise", seeded, clean]).status).toBe(0);
    const after = JSON.parse(run(["scan", clean]).stdout) as { runnable: Record<string, number> };
    expect([after.runnable.legacyShellImports, after.runnable.legacyTranscriptImports]).toEqual([
      0, 0,
    ]);
  });
});

describe("server on a fixture", () => {
  it("a perf/base server boots on a synthetic database and runs nothing", async () => {
    const database = NodePath.join(scratch("boot"), "synthetic.sqlite");
    synth(database, "0.05");
    const events = count(database, `SELECT COUNT(*) AS n FROM orchestration_events`);
    const booted = await boot(database);
    expect([booted.status, booted.migration]).toEqual([200, BUILD_MIGRATION_HEAD]);
    expect(Object.values(recoveryCounts(booted.log))).toEqual([0, 0, 0, 0, 0]);
    expect(count(booted.copy, `SELECT COUNT(*) AS n FROM orchestration_events`)).toBe(events);
  });

  it("runs none of a sanitised fixture's seeded work, where the unsanitised copy recovers it", async () => {
    const directory = scratch("boot-seeded");
    const seeded = NodePath.join(directory, "seeded.sqlite");
    synthCopy(seeded);
    seedPendingWork(seeded);
    const clean = NodePath.join(directory, "clean.sqlite");
    expect(run(["sanitise", seeded, clean]).status).toBe(0);
    const events = count(clean, `SELECT COUNT(*) AS n FROM orchestration_events`);
    const sanitised = await boot(clean);
    expect(Object.values(recoveryCounts(sanitised.log))).toEqual([0, 0, 0, 0, 0]);
    expect(count(sanitised.copy, `SELECT COUNT(*) AS n FROM orchestration_events`)).toBe(events);
    expect(count(sanitised.copy, `SELECT COUNT(*) AS n FROM orchestration_v2_effect_outbox`)).toBe(
      0,
    );
    const unsanitised = await boot(seeded);
    expect(recoveryCounts(unsanitised.log).terminalizedRuns).toBeGreaterThan(0);
  });
});

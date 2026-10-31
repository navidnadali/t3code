// @effect-diagnostics nodeBuiltinImport:off globalFetch:off - drives the CLI and a real server as subprocesses.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeReadline from "node:readline";
import * as NodeSqlite from "node:sqlite";

import { describe, expect, it, onTestFinished } from "vite-plus/test";

import { UnsafePathError, assertSafeDestination, withSidecars } from "./fixture/guards.ts";
import { Spreader, apportion, planSynth } from "./fixture/plan.ts";
import { SECRET_PATTERNS, redact, scanFileBytes, secretHits } from "./fixture/secrets.ts";
import { ShapeError, parseShape, quantile, shapeRows } from "./fixture/shape.ts";

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
  assignment: `api_key = "${"Ab12".repeat(4)}"`,
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
  db.close();
  return { dropped: [running, queued, limited, watched], kept, idle };
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
    const physical = NodePath.join(root, "link", "..", "fixture.sqlite");
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
    expect(Object.keys(SAMPLES).sort()).toEqual(SECRET_PATTERNS.map(({ name }) => name).sort());
    const text = Object.values(SAMPLES).join(" | ");
    expect(secretHits(text).sort()).toEqual(Object.keys(SAMPLES).sort());
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
    const report = NodePath.join(work, "link", "..", "userdata", "statev2.sqlite");
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
  });

  it("sanitise leaves a seeded fixture with nothing runnable and no credentials", () => {
    const directory = scratch("sanitise");
    const seeded = NodePath.join(directory, "seeded.sqlite");
    synth(seeded, "0.25");
    const { dropped, kept, idle } = seedPendingWork(seeded);
    const before = run(["scan", seeded]);
    expect(before.status).toBe(1);
    const found = (JSON.parse(before.stdout) as { runnable: Record<string, number> }).runnable;
    const missed = Object.entries(found).filter(([, value]) => value === 0);
    expect(missed).toEqual([]);
    const report = NodePath.join(directory, "report.json");
    const clean = NodePath.join(directory, "clean.sqlite");
    const result = run(["sanitise", "--report", report, seeded, clean]);
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
    const remaining = count(
      clean,
      `SELECT COUNT(*) AS n FROM orchestration_v2_projection_threads WHERE thread_id IN ('${dropped.join("','")}')`,
    );
    expect(remaining).toBe(0);
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
    synth(seeded, "0.25");
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

    const plant = (name: string, sql: string, value: string) => {
      const copy = NodePath.join(directory, `${name}.sqlite`);
      NodeFS.copyFileSync(clean, copy);
      const db = new NodeSqlite.DatabaseSync(copy);
      db.prepare(sql).run(value);
      db.close();
      return run(["scan", copy]).status;
    };
    const inId = plant(
      "token-in-id",
      `UPDATE orchestration_v2_projection_turn_items
       SET payload_json = json_set(payload_json, '$.nativeItemRef', json_object('driver', 'claudeAgent', 'nativeId', ?, 'strength', 'strong'))
       WHERE turn_item_id = (SELECT MIN(turn_item_id) FROM orchestration_v2_projection_turn_items)`,
      TOKENS.github as string,
    );
    const prose = plant(
      "prose",
      `UPDATE orchestration_v2_projection_messages SET payload_json = json_set(payload_json, '$.text', ?)
       WHERE message_id = (SELECT MIN(message_id) FROM orchestration_v2_projection_messages)`,
      "the staging db password is hunter two, keep it quiet",
    );
    expect([inId, prose]).toEqual([1, 1]);
  });
});

describe("server on a fixture", () => {
  it("a perf/base server boots on a synthetic database and runs nothing", async () => {
    const database = NodePath.join(scratch("boot"), "synthetic.sqlite");
    synth(database, "0.05");
    const events = count(database, `SELECT COUNT(*) AS n FROM orchestration_events`);
    const booted = await boot(database);
    expect([booted.status, booted.migration]).toEqual([200, 76]);
    expect(Object.values(recoveryCounts(booted.log))).toEqual([0, 0, 0, 0, 0]);
    expect(count(booted.copy, `SELECT COUNT(*) AS n FROM orchestration_events`)).toBe(events);
  });

  it("runs none of a sanitised fixture's seeded work, where the unsanitised copy recovers it", async () => {
    const directory = scratch("boot-seeded");
    const seeded = NodePath.join(directory, "seeded.sqlite");
    synth(seeded, "0.25");
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

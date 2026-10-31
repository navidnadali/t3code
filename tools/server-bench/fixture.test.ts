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

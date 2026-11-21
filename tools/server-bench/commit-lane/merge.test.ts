import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { describe, expect, it } from "vite-plus/test";

const MERGE = NodePath.join(import.meta.dirname, "merge.mjs");

// The pad size comes from the command line only: a flag, or a file the flag names.
describe("merge.mjs pad size", () => {
  const padLine = (args: ReadonlyArray<string>) => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "merge-pad-"));
    try {
      NodeFS.mkdirSync(NodePath.join(directory, "mb"));
      NodeFS.writeFileSync(NodePath.join(directory, "mb", "pad-events-bytes"), "512\n");
      NodeFS.writeFileSync(NodePath.join(directory, "pad"), "768\n");
      const shard = NodePath.join(directory, "shard.sqlite");
      new NodeSqlite.DatabaseSync(shard).close();
      const result = NodeChildProcess.spawnSync(
        process.execPath,
        [
          MERGE,
          "--dest",
          NodePath.join(directory, "merged.sqlite"),
          ...args.map((arg) => arg.replace("<dir>", directory)),
          shard,
        ],
        { encoding: "utf8", env: { ...process.env, HOME: directory } },
      );
      return /pad events: (\d+)/.exec(result.stdout)?.[1];
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  };

  it("reads no file in the home directory", () => {
    expect(padLine([])).toBe("0");
  });

  it("reads the file --pad-events-bytes-file names, and --pad-events-bytes wins over it", () => {
    expect(padLine(["--pad-events-bytes-file", "<dir>/pad"])).toBe("768");
    expect(padLine(["--pad-events-bytes-file", "<dir>/pad", "--pad-events-bytes", "64"])).toBe(
      "64",
    );
  });

  it("reads no file when --pad-events-bytes is given, not even one it cannot read", () => {
    expect(padLine(["--pad-events-bytes-file", "<dir>", "--pad-events-bytes", "64"])).toBe("64");
  });
});

// The fork's per-thread item totals come from triggers on runs and items, so the merge leaves the
// table to the destination's triggers instead of copying it.
describe("merge.mjs derived tables", () => {
  const SCHEMA = `
    CREATE TABLE orchestration_events (sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT NOT NULL, payload_json TEXT NOT NULL);
    CREATE TABLE orchestration_v2_projection_metadata (projection_name TEXT PRIMARY KEY, schema_version INTEGER, last_sequence INTEGER, updated_at TEXT);
    CREATE TABLE orchestration_v2_projection_threads (thread_id TEXT PRIMARY KEY);
    CREATE TABLE orchestration_v2_projection_runs (run_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL);
    CREATE TABLE orchestration_v2_projection_turn_items (turn_item_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, run_id TEXT);
    CREATE TABLE t3_perf_thread_item_totals (thread_id TEXT PRIMARY KEY NOT NULL, item_count INTEGER NOT NULL) WITHOUT ROWID;
    CREATE TRIGGER t3_perf_thread_item_totals_item_insert AFTER INSERT ON orchestration_v2_projection_turn_items
    WHEN (COALESCE(new.run_id, '') = '' OR EXISTS (SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id = new.run_id))
    BEGIN
      INSERT INTO t3_perf_thread_item_totals (thread_id, item_count) VALUES (new.thread_id, 1)
      ON CONFLICT (thread_id) DO UPDATE SET item_count = item_count + excluded.item_count;
    END;
    CREATE TRIGGER t3_perf_thread_item_totals_run_insert AFTER INSERT ON orchestration_v2_projection_runs
    WHEN new.run_id <> ''
    BEGIN
      INSERT INTO t3_perf_thread_item_totals (thread_id, item_count)
      SELECT thread_id, COUNT(*) FROM orchestration_v2_projection_turn_items WHERE run_id = new.run_id GROUP BY thread_id
      ON CONFLICT (thread_id) DO UPDATE SET item_count = item_count + excluded.item_count;
    END;`;

  const shard = (directory: string, name: string, thread: string, items: number) => {
    const path = NodePath.join(directory, name);
    const db = new NodeSqlite.DatabaseSync(path);
    db.exec(SCHEMA);
    db.exec(
      `INSERT INTO orchestration_v2_projection_metadata VALUES ('thread-projections', 1, 0, '')`,
    );
    db.exec(
      `INSERT INTO orchestration_events (event_type, payload_json) VALUES ('thread.created', '{}')`,
    );
    db.prepare(`INSERT INTO orchestration_v2_projection_threads VALUES (?)`).run(thread);
    db.prepare(`INSERT INTO orchestration_v2_projection_runs VALUES (?, ?)`).run(
      `${thread}:run`,
      thread,
    );
    for (let index = 0; index < items; index += 1) {
      db.prepare(`INSERT INTO orchestration_v2_projection_turn_items VALUES (?, ?, ?)`).run(
        `${thread}:item:${index}`,
        thread,
        `${thread}:run`,
      );
    }
    db.close();
    return path;
  };

  it("merges shards whose totals their own triggers filled, recounting them in the destination", () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "merge-derived-"));
    try {
      const shards = [
        shard(directory, "a.sqlite", "thread:a", 3),
        shard(directory, "b.sqlite", "thread:b", 2),
      ];
      const dest = NodePath.join(directory, "merged.sqlite");
      const result = NodeChildProcess.spawnSync(
        process.execPath,
        [MERGE, "--dest", dest, ...shards],
        {
          encoding: "utf8",
        },
      );
      expect(result.status, result.stderr).toBe(0);
      const merged = new NodeSqlite.DatabaseSync(dest, { readOnly: true });
      try {
        expect(
          merged
            .prepare(
              `SELECT thread_id, item_count FROM t3_perf_thread_item_totals ORDER BY thread_id`,
            )
            .all()
            .map((row) => ({ ...row })),
        ).toEqual([
          { thread_id: "thread:a", item_count: 3 },
          { thread_id: "thread:b", item_count: 2 },
        ]);
      } finally {
        merged.close();
      }
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  });
});

// @effect-diagnostics nodeBuiltinImport:off - builds SQLite files directly.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { describe, expect, it, onTestFinished } from "vite-plus/test";

import { type BlankStep, blankTables } from "./fixture/blank.ts";
import { scanFileBytes, scanFileBytesSteps } from "./fixture/secrets.ts";

describe("blanking pass memory", () => {
  // node:sqlite keeps every value a user function sees until its statement ends, so one UPDATE
  // per table held a whole table in the heap. The pass must stream rows in bounded batches.
  it("streams each table in batches bounded by rows and bytes, committing and reporting each", () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fixture-memory-"));
    onTestFinished(() => NodeFS.rmSync(directory, { recursive: true, force: true }));
    const db = new NodeSqlite.DatabaseSync(NodePath.join(directory, "big.sqlite"));
    db.exec(`CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT, payload_json TEXT)`);
    db.exec(`CREATE TABLE tags (tag TEXT PRIMARY KEY, label TEXT) WITHOUT ROWID`);
    const payload = JSON.stringify({ text: "word ".repeat(40_000) });
    const insert = db.prepare(`INSERT INTO notes (title, payload_json) VALUES (?, ?)`);
    for (let index = 0; index < 120; index++) insert.run(`Title ${index}`, payload);
    db.prepare(`INSERT INTO tags VALUES (?, ?)`).run("tag-1", "Some label");

    const steps: Array<BlankStep> = [];
    const pass = blankTables(db, ["notes", "tags"], "blank", { rows: 25, bytes: 1_000_000 });
    for (let step = pass.next(); step.done !== true; step = pass.next()) {
      expect(db.isTransaction).toBe(false);
      steps.push(step.value);
    }

    const notes = steps.filter((step) => step.table === "notes");
    const largestRow = Buffer.byteLength(payload) + 16;
    expect(notes.filter((step) => step.rows > 25 || step.bytes > 1_000_000 + largestRow)).toEqual(
      [],
    );
    expect([
      notes.reduce((rows, step) => rows + step.rows, 0),
      notes.at(-1)?.done,
      notes.at(-1)?.total,
    ]).toEqual([120, 120, 120]);
    expect(steps.some((step) => step.table === "tags")).toBe(true);
    const left = db
      .prepare(
        `SELECT (SELECT COUNT(*) FROM notes WHERE payload_json LIKE '%word%' OR title LIKE 'Title%')
           + (SELECT COUNT(*) FROM tags WHERE label = 'Some label') AS n`,
      )
      .get() as { n: number };
    expect(left.n).toBe(0);
    db.close();
  });

  // The raw-byte scan reads the whole file; it must hand back control after every chunk, so a
  // signal can clean up during it.
  it("scans raw bytes a chunk per step, finding what the one-shot scan finds", () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fixture-raw-steps-"));
    onTestFinished(() => NodeFS.rmSync(directory, { recursive: true, force: true }));
    const path = NodePath.join(directory, "file.bin");
    const token = `gh${"p"}_${"A1b2".repeat(9)}`;
    NodeFS.writeFileSync(path, `${" ".repeat(950)}${token}${"y".repeat(1000)}`);
    const steps = scanFileBytesSteps(path, 100);
    let yields = 0;
    let step = steps.next();
    for (; step.done !== true; step = steps.next()) yields++;
    expect([yields >= 19, Object.fromEntries(step.value)]).toEqual([
      true,
      Object.fromEntries(scanFileBytes(path, 100)),
    ]);
    expect(Object.fromEntries(step.value)).toEqual({ "github-token": 1 });
  });

  // Rowids run from -2^63: the first batch must include the smallest one.
  it("rewrites a row at the smallest rowid", () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fixture-rowid-"));
    onTestFinished(() => NodeFS.rmSync(directory, { recursive: true, force: true }));
    const db = new NodeSqlite.DatabaseSync(NodePath.join(directory, "edge.sqlite"));
    db.exec(`CREATE TABLE notes (title TEXT)`);
    const insert = db.prepare(`INSERT INTO notes (rowid, title) VALUES (?, ?)`);
    for (const rowid of [-(2n ** 63n), -1n, 0n, 1n, 2n ** 63n - 1n])
      insert.run(rowid, "Title kept?");
    const pass = blankTables(db, ["notes"], "blank", { rows: 2, bytes: 1_000_000 });
    let rows = 0;
    for (let step = pass.next(); step.done !== true; step = pass.next()) rows += step.value.rows;
    const left = db
      .prepare(`SELECT COUNT(*) AS n FROM notes WHERE title = 'Title kept?'`)
      .get() as {
      n: number;
    };
    expect([rows, left.n]).toEqual([5, 0]);
    db.close();
  });
});

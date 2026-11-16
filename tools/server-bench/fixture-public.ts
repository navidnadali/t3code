#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - a host CLI: checks the shape, then runs fixture.ts as a subprocess.
// The public fixture recipe, guarded: `check --shape <fixture/public-large.shape.json>` or `build --shape <...>
// --out <fixture.sqlite> [--summary <file.json>]`. Any other shape, or a SQLite but 3.54.0 for build, exits 2
// before anything is written; build runs fixture.ts synth with the recipe's fixed flags, then scan --keep-content.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import {
  PUBLIC_RECIPE,
  PublicRecipeError,
  checkPublicShape,
  checkSqliteVersion,
} from "./fixture/public.ts";

const FIXTURE = NodePath.join(import.meta.dirname, "fixture.ts");
const USAGE =
  "usage: fixture-public.ts check --shape <fixture/public-large.shape.json>\n" +
  "       fixture-public.ts build --shape <fixture/public-large.shape.json> --out <fixture.sqlite> [--summary <file.json>]";

const fail = (code: number, message: string): never => {
  process.stderr.write(`fixture-public: ${message}\n`);
  process.exit(code);
};

const options = (args: ReadonlyArray<string>) => {
  const values: Record<string, string> = {};
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index] as string;
    const value = args[index + 1];
    if (!["--shape", "--out", "--summary"].includes(flag) || value === undefined) {
      fail(2, `unexpected argument ${flag}\n${USAGE}`);
    }
    values[flag.slice(2)] = value as string;
  }
  return values;
};

const sha256File = (path: string): string => {
  const hash = NodeCrypto.createHash("sha256");
  const fd = NodeFS.openSync(path, "r");
  try {
    const buffer = Buffer.allocUnsafe(8 * 1024 * 1024);
    let read: number;
    while ((read = NodeFS.readSync(fd, buffer, 0, buffer.length, null)) > 0) {
      hash.update(buffer.subarray(0, read));
    }
  } finally {
    NodeFS.closeSync(fd);
  }
  return hash.digest("hex");
};

const migrationHead = (path: string): number => {
  const db = new NodeSqlite.DatabaseSync(path, { readOnly: true });
  try {
    const row = db.prepare("SELECT max(migration_id) AS head FROM effect_sql_migrations").get() as {
      head: number;
    };
    return Number(row.head);
  } finally {
    db.close();
  }
};

const run = (args: ReadonlyArray<string>) => {
  const result = NodeChildProcess.spawnSync(process.execPath, [FIXTURE, ...args], {
    stdio: ["ignore", "inherit", "inherit"],
  });
  if (result.status !== 0) {
    fail(result.status ?? 1, `fixture.ts ${args[0]} failed (${result.status ?? result.signal})`);
  }
};

const [command, ...rest] = process.argv.slice(2);
if (command !== "check" && command !== "build") fail(2, USAGE);
const values = options(rest);
if (values.shape === undefined) fail(2, `--shape is required\n${USAGE}`);
const shapePath = values.shape as string;
try {
  checkPublicShape(shapePath, NodeFS.readFileSync(shapePath, "utf8"));
} catch (error) {
  if (error instanceof PublicRecipeError) fail(2, error.message);
  if ((error as NodeJS.ErrnoException).code === "ENOENT") fail(2, `${shapePath}: no such file`);
  throw error;
}
const shapeSha256 = sha256File(shapePath);
if (command === "check") {
  process.stdout.write(`${JSON.stringify({ shape: { sha256: shapeSha256 } })}\n`);
  process.exit(0);
}
if (values.out === undefined) fail(2, `--out is required\n${USAGE}`);
const out = values.out as string;
try {
  checkSqliteVersion(process.versions.sqlite);
} catch (error) {
  if (error instanceof PublicRecipeError) fail(2, error.message);
  throw error;
}
run([
  "synth",
  "--like",
  shapePath,
  "--scale",
  String(PUBLIC_RECIPE.scale),
  "--seed",
  String(PUBLIC_RECIPE.seed),
  "--now",
  PUBLIC_RECIPE.now,
  "--workspace-root",
  PUBLIC_RECIPE.workspaceRoot,
  out,
]);
run(["scan", "--keep-content", out]);
const summary = {
  recipe: PUBLIC_RECIPE,
  shape: { file: "fixture/public-large.shape.json", sha256: shapeSha256 },
  fixture: {
    file: NodePath.basename(out),
    bytes: NodeFS.statSync(out).size,
    sha256: sha256File(out),
    migrationHead: migrationHead(out),
  },
  scan: "fixture.ts scan --keep-content: clean",
};
const text = `${JSON.stringify(summary, null, 2)}\n`;
if (values.summary !== undefined) NodeFS.writeFileSync(values.summary, text, { flag: "wx" });
process.stdout.write(text);

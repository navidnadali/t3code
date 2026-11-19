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

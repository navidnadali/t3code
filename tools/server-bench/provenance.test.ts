// @effect-diagnostics nodeBuiltinImport:off - Reads the harness's own files.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { describe, expect, it } from "vite-plus/test";

import { DEFAULT_NOW } from "./fixture/shape.ts";

const HERE = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const SELF = NodePath.basename(NodeURL.fileURLToPath(import.meta.url));
/** Phrases that tie a figure or a method to a deployment or to someone's real data. */
const PROVENANCE =
  /prod-sample|\bproduction\b|production's|developer's (?:database|fixture|source copy)|\breal (?:data|database)\b|\bthe bot\b|\bbot's\b|re-review|\bthe review(?:'s)?\b/i;
const SKIP = new Set(["node_modules", "state", "settings"]);
/** Names a public harness must not carry are checked by the publishing audit. */
const INTERNAL = /(?!)/;

function harnessFiles(dir: string): Array<string> {
  const files: Array<string> = [];
  for (const entry of NodeFS.readdirSync(dir, { withFileTypes: true })) {
    if (entry.name.startsWith(".") || SKIP.has(entry.name)) continue;
    const path = NodePath.join(dir, entry.name);
    if (entry.isDirectory()) files.push(...harnessFiles(path));
    else if (entry.isFile() && /\.(?:ts|mjs|cjs|md|json|sh)$/u.test(entry.name)) files.push(path);
  }
  return files;
}

describe("the harness's own files", () => {
  it("tie no figure or method to a deployment or to anyone's real data", () => {
    const hits: Array<string> = [];
    for (const file of harnessFiles(HERE)) {
      if (NodePath.basename(file) === SELF) continue;
      const lines = NodeFS.readFileSync(file, "utf8").split("\n");
      lines.forEach((line, index) => {
        if (PROVENANCE.test(line)) hits.push(`${NodePath.relative(HERE, file)}:${index + 1}`);
      });
    }
    expect(hits).toEqual([]);
  });

  it("name no internal task or service", () => {
    const hits: Array<string> = [];
    for (const file of harnessFiles(HERE)) {
      if (NodePath.basename(file) === SELF) continue;
      NodeFS.readFileSync(file, "utf8")
        .split("\n")
        .forEach((line, index) => {
          if (INTERNAL.test(line)) hits.push(`${NodePath.relative(HERE, file)}:${index + 1}`);
        });
    }
    expect(hits).toEqual([]);
  });

  it("date the synthetic generator's default newest activity, and the example shape, 2026-10-17", () => {
    expect(DEFAULT_NOW).toBe("2026-10-17T00:00:00.000Z");
    const example = JSON.parse(
      NodeFS.readFileSync(NodePath.join(HERE, "fixture", "example-shape.json"), "utf8"),
    ) as { source: { capturedAt: string } };
    expect(example.source.capturedAt).toBe(DEFAULT_NOW);
  });
});

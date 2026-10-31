// @effect-diagnostics nodeBuiltinImport:off - Exercises a plain-fs reader with real files.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import { parseCredential, readCredentials } from "./sessionMcp.ts";

const credential = { url: "http://127.0.0.1:3773/mcp", authorization: "Bearer session-token" };

describe("parseCredential", () => {
  it("reads a provider session's MCP endpoint and Authorization value", () => {
    expect(parseCredential(JSON.stringify(credential))).toEqual(credential);
  });

  it.each([
    "{",
    "null",
    JSON.stringify({ url: "file:///etc/passwd", authorization: "Bearer x" }),
    JSON.stringify({ url: credential.url, authorization: " " }),
    JSON.stringify({ url: credential.url }),
  ])("ignores %s", (text) => {
    expect(parseCredential(text)).toBeNull();
  });
});

describe("readCredentials", () => {
  it("keeps this run's credential files and skips stale or unreadable ones", () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "bench-mcp-credentials-"));
    try {
      const write = (name: string, text: string, mtimeSeconds: number) => {
        const path = NodePath.join(directory, name);
        NodeFS.writeFileSync(path, text);
        NodeFS.utimesSync(path, mtimeSeconds, mtimeSeconds);
      };
      write("current.json", JSON.stringify(credential), 2_000);
      write("stale.json", JSON.stringify(credential), 1_000);
      write("partial.json", '{"url":', 2_000);
      write("notes.txt", JSON.stringify(credential), 2_000);
      expect([...readCredentials(directory, 1_500_000)]).toEqual([["current.json", credential]]);
      expect(readCredentials(NodePath.join(directory, "missing"), 0).size).toBe(0);
    } finally {
      NodeFS.rmSync(directory, { recursive: true, force: true });
    }
  });
});

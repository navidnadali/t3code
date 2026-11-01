// @effect-diagnostics nodeBuiltinImport:off - Exercises a plain-fs reader with real files.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import {
  CredentialCallers,
  type McpCredential,
  parseCredential,
  readCredentials,
} from "./sessionMcp.ts";

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

describe("CredentialCallers", () => {
  /** Sign-ins the test completes by hand, in any order. */
  const harness = (limit: number) => {
    const started: Array<string> = [];
    const stopped: Array<string> = [];
    const signIns: Array<{
      readonly authorization: string;
      readonly finish: () => void;
      readonly fail: () => void;
    }> = [];
    const callers = new CredentialCallers(limit, (_name, credential: McpCredential) => {
      const done = Promise.withResolvers<{ start: () => void; stop: () => Promise<void> }>();
      const caller = {
        start: () => void started.push(credential.authorization),
        stop: async () => void stopped.push(credential.authorization),
      };
      signIns.push({
        authorization: credential.authorization,
        finish: () => done.resolve(caller),
        fail: () => done.reject(new Error("401")),
      });
      return done.promise;
    });
    const settle = () => new Promise<void>((resolve) => queueMicrotask(resolve));
    return { callers, started, stopped, signIns, settle };
  };
  const as = (authorization: string) => ({ url: "http://127.0.0.1:1/mcp", authorization });

  it("keeps only the newest credential when sign-ins finish out of order", async () => {
    const { callers, started, stopped, signIns, settle } = harness(1);
    callers.update(new Map([["a.json", as("Bearer old")]]));
    callers.update(new Map([["a.json", as("Bearer new")]]));
    signIns[1]?.finish();
    await settle();
    signIns[0]?.finish();
    await settle();
    expect(started).toEqual(["Bearer new"]);
    expect(stopped).toEqual(["Bearer old"]);
    expect(callers.callers()).toHaveLength(1);
  });

  it("never lets an older sign-in replace a newer one after a failed one in between", async () => {
    const { callers, started, stopped, signIns, settle } = harness(1);
    callers.update(new Map([["a.json", as("Bearer old")]]));
    callers.update(new Map([["a.json", as("Bearer middle")]]));
    signIns[1]?.fail();
    await settle();
    await settle();
    callers.update(new Map([["a.json", as("Bearer new")]]));
    signIns[2]?.finish();
    await settle();
    signIns[0]?.finish();
    await settle();
    expect(started).toEqual(["Bearer new"]);
    expect(stopped).toEqual(["Bearer old"]);
    expect(callers.callers()).toHaveLength(1);
  });

  it("replaces a running caller when its session's token rotates", async () => {
    const { callers, started, stopped, signIns, settle } = harness(1);
    callers.update(new Map([["a.json", as("Bearer one")]]));
    signIns[0]?.finish();
    await settle();
    callers.update(new Map([["a.json", as("Bearer two")]]));
    signIns[1]?.finish();
    await settle();
    expect(started).toEqual(["Bearer one", "Bearer two"]);
    expect(stopped).toEqual(["Bearer one"]);
    expect(callers.callers()).toHaveLength(1);
  });

  it("counts sign-ins in progress against the limit and frees the slot when one fails", async () => {
    const { callers, signIns, settle } = harness(1);
    callers.update(new Map([["a.json", as("Bearer a")]]));
    callers.update(new Map([["b.json", as("Bearer b")]]));
    expect(signIns).toHaveLength(1);
    signIns[0]?.fail();
    await settle();
    await settle();
    callers.update(
      new Map([
        ["a.json", as("Bearer a")],
        ["b.json", as("Bearer b")],
      ]),
    );
    expect(signIns.map((signIn) => signIn.authorization)).toEqual(["Bearer a", "Bearer b"]);
  });
});

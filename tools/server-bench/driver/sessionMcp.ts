// @effect-diagnostics nodeBuiltinImport:off - The standalone bench driver reads provider-written files without Effect.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

/** The `t3-code` MCP server a provider session was given: its endpoint and Authorization value. */
export interface McpCredential {
  readonly url: string;
  readonly authorization: string;
}

/**
 * One provider session's credential as the provider (for example the bench's fake Claude CLI)
 * writes it: `{"url": "http://…/mcp", "authorization": "Bearer …"}`, one file per session,
 * written atomically. Anything else is ignored.
 */
export function parseCredential(text: string): McpCredential | null {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    return null;
  }
  if (typeof value !== "object" || value === null) return null;
  const { url, authorization } = value as { url?: unknown; authorization?: unknown };
  if (typeof url !== "string" || typeof authorization !== "string") return null;
  if (!/^https?:\/\//u.test(url) || authorization.trim().length === 0) return null;
  return { url, authorization };
}

/** Credentials written at or after `sinceEpochMs`, by file name; older files are stale runs. */
export function readCredentials(
  directory: string,
  sinceEpochMs: number,
): Map<string, McpCredential> {
  const credentials = new Map<string, McpCredential>();
  let names: Array<string>;
  try {
    names = NodeFS.readdirSync(directory);
  } catch {
    return credentials;
  }
  for (const name of names.filter((entry) => entry.endsWith(".json")).toSorted()) {
    const path = NodePath.join(directory, name);
    try {
      if (NodeFS.statSync(path).mtimeMs < sinceEpochMs) continue;
      const credential = parseCredential(NodeFS.readFileSync(path, "utf8"));
      if (credential !== null) credentials.set(name, credential);
    } catch {
      // Removed between listing and reading: the session ended.
    }
  }
  return credentials;
}

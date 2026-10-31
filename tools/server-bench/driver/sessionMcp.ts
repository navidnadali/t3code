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

export interface CredentialCaller {
  readonly start: () => void;
  readonly stop: (closeSession: boolean) => Promise<void>;
}

/**
 * One caller per provider session, signed in with that session's credential. Sign-ins run in the
 * background: a newer credential for the same session supersedes any older one still signing in,
 * and the caller limit counts sign-ins in progress, so a rotation never leaves two callers running.
 */
export class CredentialCallers<C extends CredentialCaller> {
  private readonly entries = new Map<
    string,
    { generation: number; authorization: string; caller: C | null }
  >();
  private readonly failed = new Set<string>();
  private closed = false;
  private readonly limit: number;
  private readonly connect: (name: string, credential: McpCredential) => Promise<C>;

  constructor(limit: number, connect: (name: string, credential: McpCredential) => Promise<C>) {
    this.limit = limit;
    this.connect = connect;
  }

  update(credentials: ReadonlyMap<string, McpCredential>): void {
    for (const [name, credential] of credentials) {
      if (this.closed) return;
      const entry = this.entries.get(name);
      const attempt = `${name}\0${credential.authorization}`;
      if (entry?.authorization === credential.authorization || this.failed.has(attempt)) continue;
      if (entry === undefined && this.entries.size >= this.limit) continue;
      const generation = (entry?.generation ?? 0) + 1;
      this.entries.set(name, {
        generation,
        authorization: credential.authorization,
        caller: entry?.caller ?? null,
      });
      this.connect(name, credential).then(
        (caller) => {
          const current = this.entries.get(name);
          if (this.closed || current?.generation !== generation) {
            void caller.stop(true);
            return;
          }
          void current.caller?.stop(false);
          current.caller = caller;
          caller.start();
        },
        () => {
          this.failed.add(attempt);
          const current = this.entries.get(name);
          if (current?.generation === generation && current.caller === null) {
            this.entries.delete(name);
          }
        },
      );
    }
  }

  /** Callers running now, one per provider session. */
  callers(): Array<C> {
    return [...this.entries.values()].flatMap((entry) =>
      entry.caller === null ? [] : [entry.caller],
    );
  }

  async close(closeSessions: boolean): Promise<void> {
    this.closed = true;
    await Promise.all(this.callers().map((caller) => caller.stop(closeSessions)));
  }
}

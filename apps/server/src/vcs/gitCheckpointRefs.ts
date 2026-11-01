// @effect-diagnostics nodeBuiltinImport:off - reads loose refs, packed-refs and loose objects as the files Git writes; Effect's FileSystem has no inflate and its stat lacks the inode.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";

import * as Effect from "effect/Effect";

const OBJECT_ID = /^[0-9a-f]{40}(?:[0-9a-f]{24})?$/;

export const isObjectId = (value: string): boolean => OBJECT_ID.test(value);

/** What `git log --format=%T %P %ct` prints for a commit. */
export interface CommitFacts {
  readonly tree: string;
  readonly parents: ReadonlyArray<string>;
  readonly committerTime: string;
}

export interface CheckpointRefMemory {
  /** A ref this process wrote or resolved, so a loose file still holding `oid` answers without Git. */
  readonly rememberRef: (commonDir: string, ref: string, oid: string) => void;
  readonly forgetRef: (commonDir: string, ref: string) => void;
  /**
   * The commit a remembered ref names: its oid, null when the ref exists neither loose nor packed,
   * undefined when Git must answer (unknown ref, another value, a symbolic ref, an unreadable store).
   */
  readonly resolveRef: (commonDir: string, ref: string) => Effect.Effect<string | null | undefined>;
  readonly rememberCommit: (oid: string, facts: CommitFacts) => void;
  /** Facts from memory or the loose object; null when Git must answer (packed or missing object). */
  readonly commitFacts: (objectsDir: string, oid: string) => Effect.Effect<CommitFacts | null>;
}

// Caches, not state: an evicted entry costs the Git command it would have replaced. A turn keeps
// two refs and two commits live, so these cover more than a thousand concurrent threads.
const REFS_CAPACITY = 16_384;
const COMMITS_CAPACITY = 16_384;
const PACKED_REFS_CAPACITY = 256;
// Only refs under this prefix are kept from packed-refs; others are asked of Git.
const PACKED_REFS_PREFIX = "refs/t3/";

class BoundedMap<K, V> {
  readonly #entries = new Map<K, V>();
  readonly #capacity: number;
  constructor(capacity: number) {
    this.#capacity = capacity;
  }
  get(key: K): V | undefined {
    const value = this.#entries.get(key);
    if (value !== undefined) {
      this.#entries.delete(key);
      this.#entries.set(key, value);
    }
    return value;
  }
  set(key: K, value: V): void {
    if (!this.#entries.has(key) && this.#entries.size >= this.#capacity) {
      this.#entries.delete(this.#entries.keys().next().value!);
    }
    this.#entries.delete(key);
    this.#entries.set(key, value);
  }
  delete(key: K): void {
    this.#entries.delete(key);
  }
}

type LooseRef =
  | { readonly kind: "oid"; readonly oid: string }
  | { readonly kind: "missing" }
  | { readonly kind: "other" };

const readLooseRef = (file: string) =>
  Effect.promise(
    () =>
      new Promise<LooseRef>((resolve) => {
        try {
          NodeFS.readFile(file, (error, data) => {
            if (error) {
              resolve(
                error.code === "ENOENT" || error.code === "ENOTDIR"
                  ? { kind: "missing" }
                  : { kind: "other" },
              );
              return;
            }
            const content = data.toString("latin1").trimEnd();
            resolve(OBJECT_ID.test(content) ? { kind: "oid", oid: content } : { kind: "other" });
          });
        } catch {
          resolve({ kind: "other" });
        }
      }),
  );

/** The file's text, or null when it cannot be read. */
export const readTextFile = (file: string): Effect.Effect<string | null> =>
  Effect.promise(
    () =>
      new Promise<string | null>((resolve) => {
        try {
          NodeFS.readFile(file, "utf8", (error, text) => resolve(error ? null : text));
        } catch {
          resolve(null);
        }
      }),
  );

const statFingerprint = (file: string) =>
  new Promise<string>((resolve) => {
    try {
      NodeFS.stat(file, { bigint: true }, (error, stats) =>
        resolve(
          error
            ? `!${error.code ?? "error"}`
            : `${stats.dev}:${stats.ino}:${stats.size}:${stats.mtimeNs}:${stats.ctimeNs}`,
        ),
      );
    } catch {
      resolve("!invalid");
    }
  });

interface PackedRefs {
  readonly fingerprint: string;
  readonly refs: ReadonlyMap<string, string>;
}

// `<oid> <name>` lines; `#` is the header and `^` the peeled object of the tag above it.
export const parsePackedRefs = (content: string, prefix: string): ReadonlyMap<string, string> => {
  const refs = new Map<string, string>();
  for (const line of content.split("\n")) {
    if (line.length === 0 || line.startsWith("#") || line.startsWith("^")) continue;
    const separator = line.indexOf(" ");
    if (separator === -1) continue;
    const oid = line.slice(0, separator);
    const name = line.slice(separator + 1);
    if (OBJECT_ID.test(oid) && name.startsWith(prefix)) refs.set(name, oid);
  }
  return refs;
};

// Stat before reading, so a rewrite between the two is seen by the next stat; null when unreadable.
const readPackedRefs = (commonDir: string, cached: PackedRefs | undefined) =>
  Effect.promise(async (): Promise<PackedRefs | null> => {
    const file = NodePath.join(commonDir, "packed-refs");
    const fingerprint = await statFingerprint(file);
    if (cached !== undefined && cached.fingerprint === fingerprint) return cached;
    if (fingerprint === "!ENOENT") return { fingerprint, refs: new Map() };
    if (fingerprint.startsWith("!")) return null;
    const content = await new Promise<string | null>((resolve) => {
      try {
        NodeFS.readFile(file, "latin1", (error, text) => resolve(error ? null : text));
      } catch {
        resolve(null);
      }
    });
    if (content === null) return null;
    return { fingerprint, refs: parsePackedRefs(content, PACKED_REFS_PREFIX) };
  });

/** A commit object's header; null when it is not a well-formed commit. */
export const parseCommitObject = (object: Buffer): CommitFacts | null => {
  const headerEnd = object.indexOf(0);
  if (headerEnd === -1) return null;
  const header = object.toString("latin1", 0, headerEnd);
  if (!header.startsWith("commit ") || Number(header.slice(7)) !== object.length - headerEnd - 1) {
    return null;
  }
  let tree: string | null = null;
  const parents: string[] = [];
  let committerTime: string | null = null;
  for (const line of object.toString("latin1", headerEnd + 1).split("\n")) {
    if (line.length === 0) break;
    if (line.startsWith("tree ")) {
      if (tree !== null || !OBJECT_ID.test(line.slice(5))) return null;
      tree = line.slice(5);
    } else if (line.startsWith("parent ")) {
      if (!OBJECT_ID.test(line.slice(7))) return null;
      parents.push(line.slice(7));
    } else if (line.startsWith("committer ")) {
      const match = /^committer [^\n]* (\d+) [+-]\d{4}$/.exec(line);
      if (match === null || committerTime !== null) return null;
      committerTime = match[1]!;
    }
  }
  return tree === null || committerTime === null ? null : { tree, parents, committerTime };
};

const readLooseCommit = (objectsDir: string, oid: string) =>
  Effect.promise(
    () =>
      new Promise<CommitFacts | null>((resolve) => {
        try {
          NodeFS.readFile(
            NodePath.join(objectsDir, oid.slice(0, 2), oid.slice(2)),
            (error, data) => {
              if (error) {
                resolve(null);
                return;
              }
              try {
                resolve(parseCommitObject(NodeZlib.inflateSync(data)));
              } catch {
                resolve(null);
              }
            },
          );
        } catch {
          resolve(null);
        }
      }),
  );

const isCheckpointRefName = (ref: string) =>
  ref.startsWith(PACKED_REFS_PREFIX) &&
  ref.split("/").every((part) => part.length > 0 && part !== "." && part !== "..");

export const makeCheckpointRefMemory = (): CheckpointRefMemory => {
  const refs = new BoundedMap<string, string>(REFS_CAPACITY);
  const commits = new BoundedMap<string, CommitFacts>(COMMITS_CAPACITY);
  const packed = new BoundedMap<string, PackedRefs>(PACKED_REFS_CAPACITY);
  const refKey = (commonDir: string, ref: string) => `${commonDir}\0${ref}`;

  // Git's files backend: the loose file wins, then packed-refs; absent from both is absent.
  const resolveRef = Effect.fnUntraced(function* (commonDir: string, ref: string) {
    const known = refs.get(refKey(commonDir, ref));
    if (known === undefined || !isCheckpointRefName(ref)) return undefined;
    const loose = yield* readLooseRef(NodePath.join(commonDir, ref));
    if (loose.kind === "oid") return loose.oid === known ? known : undefined;
    if (loose.kind === "other") return undefined;
    const packedRefs = yield* readPackedRefs(commonDir, packed.get(commonDir));
    if (packedRefs === null) return undefined;
    packed.set(commonDir, packedRefs);
    const entry = packedRefs.refs.get(ref);
    if (entry === undefined) return null;
    return entry === known ? known : undefined;
  });

  const commitFacts = Effect.fnUntraced(function* (objectsDir: string, oid: string) {
    const remembered = commits.get(oid);
    if (remembered !== undefined) return remembered;
    const facts = yield* readLooseCommit(objectsDir, oid);
    if (facts !== null) commits.set(oid, facts);
    return facts;
  });

  return {
    rememberRef: (commonDir, ref, oid) => refs.set(refKey(commonDir, ref), oid),
    forgetRef: (commonDir, ref) => refs.delete(refKey(commonDir, ref)),
    resolveRef,
    rememberCommit: (oid, facts) => commits.set(oid, facts),
    commitFacts,
  };
};

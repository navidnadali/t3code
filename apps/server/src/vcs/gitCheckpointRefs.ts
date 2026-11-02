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

export interface CheckpointRepository {
  readonly commonDir: string;
  readonly objectsDir: string;
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
  /** Keeps facts Git reported, while the repository's history is the objects' own. */
  readonly rememberCommits: (
    repository: CheckpointRepository,
    facts: ReadonlyArray<readonly [oid: string, facts: CommitFacts]>,
  ) => Effect.Effect<void>;
  /**
   * Facts from memory or the loose object; null when Git must answer: a packed or missing object,
   * or a history Git reads differently from the objects (shallow, grafts, replacements).
   */
  readonly commitFacts: (
    repository: CheckpointRepository,
    oid: string,
  ) => Effect.Effect<CommitFacts | null>;
}

// Caches, not state: an evicted entry costs the Git command it would have replaced. A turn keeps
// two refs and two commits live, so these cover more than a thousand concurrent threads.
const REFS_CAPACITY = 16_384;
const COMMITS_CAPACITY = 16_384;
// Parsed packed-refs files kept, and the entries they may hold in all; a file with more is left to Git.
const PACKED_REFS_CAPACITY = 256;
const PACKED_REFS_ENTRY_BUDGET = 16_384;
// Only refs under this prefix are kept from packed-refs; others are asked of Git.
const PACKED_REFS_PREFIX = "refs/t3/";
const REPLACE_REFS_PREFIX = "refs/replace/";

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

// Git reads a symbolic link's target as a ref name, so a link is never read as a file here.
const readLooseRef = (file: string) =>
  Effect.promise(
    () =>
      new Promise<LooseRef>((resolve) => {
        try {
          NodeFS.lstat(file, (statError, stats) => {
            if (statError) {
              resolve(
                statError.code === "ENOENT" || statError.code === "ENOTDIR"
                  ? { kind: "missing" }
                  : { kind: "other" },
              );
              return;
            }
            if (!stats.isFile()) {
              resolve({ kind: "other" });
              return;
            }
            NodeFS.readFile(file, (error, data) => {
              if (error) {
                resolve({ kind: "other" });
                return;
              }
              const content = data.toString("latin1").trimEnd();
              resolve(OBJECT_ID.test(content) ? { kind: "oid", oid: content } : { kind: "other" });
            });
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

export interface ParsedPackedRefs {
  // Null when the file holds more refs under the prefix than `maxEntries`.
  readonly refs: ReadonlyMap<string, string> | null;
  readonly replaceRefs: boolean;
}

// `<oid> <name>` lines; `#` is the header and `^` the peeled object of the tag above it.
export const parsePackedRefs = (
  content: string,
  prefix: string,
  maxEntries: number,
): ParsedPackedRefs => {
  let refs: Map<string, string> | null = new Map();
  let replaceRefs = false;
  for (const line of content.split("\n")) {
    if (line.length === 0 || line.startsWith("#") || line.startsWith("^")) continue;
    const separator = line.indexOf(" ");
    if (separator === -1) continue;
    const name = line.slice(separator + 1);
    if (name.startsWith(REPLACE_REFS_PREFIX)) replaceRefs = true;
    if (refs === null || !name.startsWith(prefix)) continue;
    const oid = line.slice(0, separator);
    if (!OBJECT_ID.test(oid)) continue;
    if (refs.size >= maxEntries) refs = null;
    else refs.set(name, oid);
  }
  return { refs, replaceRefs };
};

interface PackedRefs extends ParsedPackedRefs {
  readonly fingerprint: string;
}

// Stat before reading, so a rewrite between the two is seen by the next stat; null when unreadable.
const readPackedRefs = (commonDir: string, cached: PackedRefs | undefined) =>
  Effect.promise(async (): Promise<PackedRefs | null> => {
    const file = NodePath.join(commonDir, "packed-refs");
    const fingerprint = await statFingerprint(file);
    if (cached !== undefined && cached.fingerprint === fingerprint) return cached;
    if (fingerprint === "!ENOENT") return { fingerprint, refs: new Map(), replaceRefs: false };
    if (fingerprint.startsWith("!")) return null;
    const content = await new Promise<string | null>((resolve) => {
      try {
        NodeFS.readFile(file, "latin1", (error, text) => resolve(error ? null : text));
      } catch {
        resolve(null);
      }
    });
    if (content === null) return null;
    return {
      fingerprint,
      ...parsePackedRefs(content, PACKED_REFS_PREFIX, PACKED_REFS_ENTRY_BUDGET),
    };
  });

// Whether `git log` reports a commit as its object says: no shallow boundary, grafts or replacements
// (replace refs are also looked for in packed-refs by the caller).
const plainHistoryFiles = (commonDir: string) =>
  Effect.promise(async () => {
    if (process.env.GIT_REPLACE_REF_BASE !== undefined) return false;
    const [shallow, grafts, replaceRefs] = await Promise.all([
      statFingerprint(NodePath.join(commonDir, "shallow")),
      statFingerprint(NodePath.join(commonDir, "info", "grafts")),
      // Deleting the last replacement can leave the directory; only entries count.
      new Promise<ReadonlyArray<string> | null>((resolve) => {
        try {
          NodeFS.readdir(NodePath.join(commonDir, "refs", "replace"), (error, names) =>
            resolve(
              error ? (error.code === "ENOENT" || error.code === "ENOTDIR" ? [] : null) : names,
            ),
          );
        } catch {
          resolve(null);
        }
      }),
    ]);
    return shallow === "!ENOENT" && grafts === "!ENOENT" && replaceRefs?.length === 0;
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
  // Parsed packed-refs per common directory (insertion order = age), bounded in files and entries.
  const packed = new Map<string, PackedRefs>();
  let packedEntries = 0;
  const refKey = (commonDir: string, ref: string) => `${commonDir}\0${ref}`;

  const packedRefs = Effect.fnUntraced(function* (commonDir: string) {
    const cached = packed.get(commonDir);
    const current = yield* readPackedRefs(commonDir, cached);
    if (current === null || current === cached) return current;
    if (cached !== undefined) {
      packed.delete(commonDir);
      packedEntries -= cached.refs?.size ?? 0;
    }
    const size = current.refs?.size ?? 0;
    for (const [key, entry] of packed) {
      if (packed.size < PACKED_REFS_CAPACITY && packedEntries + size <= PACKED_REFS_ENTRY_BUDGET) {
        break;
      }
      packed.delete(key);
      packedEntries -= entry.refs?.size ?? 0;
    }
    packed.set(commonDir, current);
    packedEntries += size;
    return current;
  });

  // Git's files backend: the loose file wins, then packed-refs; absent from both is absent.
  const resolveRef = Effect.fnUntraced(function* (commonDir: string, ref: string) {
    const known = refs.get(refKey(commonDir, ref));
    if (known === undefined || !isCheckpointRefName(ref)) return undefined;
    const loose = yield* readLooseRef(NodePath.join(commonDir, ref));
    if (loose.kind === "oid") return loose.oid === known ? known : undefined;
    if (loose.kind === "other") return undefined;
    const current = yield* packedRefs(commonDir);
    if (current === null || current.refs === null) return undefined;
    const entry = current.refs.get(ref);
    if (entry === undefined) return null;
    return entry === known ? known : undefined;
  });

  const plainHistory = Effect.fnUntraced(function* (commonDir: string) {
    if (!(yield* plainHistoryFiles(commonDir))) return false;
    const current = yield* packedRefs(commonDir);
    return current !== null && !current.replaceRefs;
  });

  const commitFacts = Effect.fnUntraced(function* (repository: CheckpointRepository, oid: string) {
    if (!(yield* plainHistory(repository.commonDir))) return null;
    const remembered = commits.get(oid);
    if (remembered !== undefined) return remembered;
    const facts = yield* readLooseCommit(repository.objectsDir, oid);
    if (facts !== null) commits.set(oid, facts);
    return facts;
  });

  const rememberCommits = Effect.fnUntraced(function* (
    repository: CheckpointRepository,
    facts: ReadonlyArray<readonly [oid: string, facts: CommitFacts]>,
  ) {
    if (facts.length === 0 || !(yield* plainHistory(repository.commonDir))) return;
    for (const [oid, commit] of facts) commits.set(oid, commit);
  });

  return {
    rememberRef: (commonDir, ref, oid) => refs.set(refKey(commonDir, ref), oid),
    forgetRef: (commonDir, ref) => refs.delete(refKey(commonDir, ref)),
    resolveRef,
    rememberCommits,
    commitFacts,
  };
};

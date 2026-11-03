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
  /**
   * A ref this process wrote or resolved, so a loose file still holding `oid` answers without Git.
   * Remembered per object store (directory and alternates): the commit must stay findable.
   */
  readonly rememberRef: (
    repository: CheckpointRepository,
    ref: string,
    oid: string,
  ) => Effect.Effect<void>;
  readonly forgetRef: (repository: CheckpointRepository, ref: string) => Effect.Effect<void>;
  /**
   * The commit a remembered ref names, or undefined when Git must answer: an unknown ref, another
   * value, a symbolic ref, an unreadable or oversized store, a reftable store, a ref found neither
   * loose nor packed (rev-parse then tries other names), or an environment that moves refs.
   */
  readonly resolveRef: (
    repository: CheckpointRepository,
    ref: string,
  ) => Effect.Effect<string | undefined>;
  /**
   * Everything that decides whether Git reads commits as their objects say (shallow boundary,
   * grafts, replacements, the environment), as one string; null when it does not.
   */
  readonly historyState: (commonDir: string) => Effect.Effect<string | null>;
  /** Keeps facts Git reported, when the history state is the one taken before Git ran. */
  readonly rememberCommits: (
    repository: CheckpointRepository,
    facts: ReadonlyArray<readonly [oid: string, facts: CommitFacts]>,
    stateBefore: string | null,
  ) => Effect.Effect<void>;
  /** Facts from memory or the loose object; null when Git must answer. */
  readonly commitFacts: (
    repository: CheckpointRepository,
    oid: string,
  ) => Effect.Effect<CommitFacts | null>;
  /** Entries currently kept from packed-refs files, for tests and diagnostics. */
  readonly packedRefsEntries: () => number;
}

// Caches, not state: an evicted entry costs the Git command it would have replaced. A turn keeps
// two refs and two commits live, so these cover more than a thousand concurrent threads.
const REFS_CAPACITY = 16_384;
const COMMITS_CAPACITY = 16_384;
// Parsed packed-refs files kept, and the entries they may hold in all; a file with more is left to Git.
const PACKED_REFS_CAPACITY = 256;
const PACKED_REFS_ENTRY_BUDGET = 16_384;
// A cold parse runs on the main thread at about 1.8 ms and 1.2 MB per MiB of file (review bench:
// 6.2 MB → 11 ms, 62 MB → 184 ms), against 19–25 ms for a `git rev-parse` child; 8 MiB keeps one
// parse below one child. Larger files are left to Git.
const PACKED_REFS_MAX_BYTES = 8 * 1024 * 1024;
// Only refs under this prefix are kept from packed-refs; others are asked of Git.
const PACKED_REFS_PREFIX = "refs/t3/";
const REPLACE_REFS_PREFIX = "refs/replace/";
// The generated names: base64url scope keys, "ordinal" and decimal ordinals.
const CHECKPOINT_REF_NAME = /^refs\/t3\/[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_-]+)+$/;
// Variables that move refs or change which commits Git sees; any of them set disables the fast paths.
const HISTORY_ENVIRONMENT = [
  "GIT_NAMESPACE",
  "GIT_GRAFT_FILE",
  "GIT_SHALLOW_FILE",
  "GIT_REPLACE_REF_BASE",
] as const;

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

/** Whether the path is a symbolic link; null when it cannot be examined. */
export const isSymbolicLink = (file: string): Effect.Effect<boolean | null> =>
  Effect.promise(
    () =>
      new Promise<boolean | null>((resolve) => {
        try {
          NodeFS.lstat(file, (error, stats) => resolve(error ? null : stats.isSymbolicLink()));
        } catch {
          resolve(null);
        }
      }),
  );

interface StatInfo {
  readonly fingerprint: string;
  readonly size: number;
}

const statInfo = (file: string) =>
  new Promise<StatInfo>((resolve) => {
    try {
      NodeFS.stat(file, { bigint: true }, (error, stats) =>
        resolve(
          error
            ? { fingerprint: `!${error.code ?? "error"}`, size: 0 }
            : {
                fingerprint: `${stats.dev}:${stats.ino}:${stats.size}:${stats.mtimeNs}:${stats.ctimeNs}`,
                size: Number(stats.size),
              },
        ),
      );
    } catch {
      resolve({ fingerprint: "!invalid", size: 0 });
    }
  });

const statFingerprint = (file: string) => statInfo(file).then((info) => info.fingerprint);

export interface ParsedPackedRefs {
  // Null when the file holds more refs under the prefix than `maxEntries`.
  readonly refs: ReadonlyMap<string, string> | null;
  // Null when the content was not parsed (more than `maxBytes`), so replacements are unknown.
  readonly replaceRefs: boolean | null;
}

// `<oid> <name>` lines; `#` is the header and `^` the peeled object of the tag above it. The
// content is latin1, one byte per character, so its length is the file's size as read.
export const parsePackedRefs = (
  content: string,
  prefix: string,
  maxEntries: number,
  maxBytes: number,
): ParsedPackedRefs => {
  if (content.length > maxBytes) return { refs: null, replaceRefs: null };
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
    const { fingerprint, size } = await statInfo(file);
    if (cached !== undefined && cached.fingerprint === fingerprint) return cached;
    if (fingerprint === "!ENOENT") return { fingerprint, refs: new Map(), replaceRefs: false };
    if (fingerprint.startsWith("!")) return null;
    if (size > PACKED_REFS_MAX_BYTES) return { fingerprint, refs: null, replaceRefs: null };
    const content = await new Promise<string | null>((resolve) => {
      try {
        NodeFS.readFile(file, "latin1", (error, text) => resolve(error ? null : text));
      } catch {
        resolve(null);
      }
    });
    if (content === null) return null;
    // The file may have grown between the stat and the read; the bytes read decide.
    return {
      fingerprint,
      ...parsePackedRefs(
        content,
        PACKED_REFS_PREFIX,
        PACKED_REFS_ENTRY_BUDGET,
        PACKED_REFS_MAX_BYTES,
      ),
    };
  });

const environmentPlain = () => HISTORY_ENVIRONMENT.every((name) => process.env[name] === undefined);

// The shallow file, grafts and loose replacements as one string, or null when any is present
// (deleting the last replacement can leave its directory; only entries count). The directories
// that would hold them are part of the string: their mtime and inode move on every entry added or
// removed, so a file that existed only between two readings still shows.
const historyFiles = (commonDir: string) =>
  Effect.promise(async () => {
    const [shallow, grafts, replaceRefs, ...directories] = await Promise.all([
      statFingerprint(NodePath.join(commonDir, "shallow")),
      statFingerprint(NodePath.join(commonDir, "info", "grafts")),
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
      ...[".", "info", "refs", NodePath.join("refs", "replace")].map((directory) =>
        statFingerprint(NodePath.resolve(commonDir, directory)),
      ),
    ]);
    return shallow === "!ENOENT" && grafts === "!ENOENT" && replaceRefs?.length === 0
      ? `${shallow}\n${grafts}\n${directories.join("\n")}\n`
      : null;
  });

/**
 * A commit object's header as `git log` reads it: the tree on the first line, parents right after
 * it, then the author and the committer with `<email> <time> <zone>`; null for anything else,
 * which Git rejects or reads differently.
 */
export const parseCommitObject = (object: Buffer): CommitFacts | null => {
  const headerEnd = object.indexOf(0);
  if (headerEnd === -1) return null;
  const header = object.toString("latin1", 0, headerEnd);
  if (!header.startsWith("commit ") || Number(header.slice(7)) !== object.length - headerEnd - 1) {
    return null;
  }
  const lines = object.toString("latin1", headerEnd + 1).split("\n");
  const tree = lines[0]?.startsWith("tree ") ? lines[0].slice(5) : null;
  if (tree === null || !OBJECT_ID.test(tree)) return null;
  const parents: string[] = [];
  let index = 1;
  while (lines[index]?.startsWith("parent ")) {
    const parent = lines[index]!.slice(7);
    if (!OBJECT_ID.test(parent)) return null;
    parents.push(parent);
    index++;
  }
  if (!lines[index]?.startsWith("author ")) return null;
  const committer = /^committer [^<>\n]*<[^<>\n]*> (\d+) [+-]\d{4}$/.exec(lines[index + 1] ?? "");
  if (committer === null) return null;
  return { tree, parents, committerTime: committer[1]! };
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

export const makeCheckpointRefMemory = (): CheckpointRefMemory => {
  const refs = new BoundedMap<string, string>(REFS_CAPACITY);
  const commits = new BoundedMap<string, CommitFacts>(COMMITS_CAPACITY);
  // Parsed packed-refs per common directory (insertion order = age), bounded in files and entries.
  const packed = new Map<string, PackedRefs>();
  let packedEntries = 0;
  // The store a ref was resolved against: its objects directory, alternates file and the
  // alternates named by the environment; a different store may lack the commit. Git reads a
  // relative alternate from each command's own directory, so such a store has no one key.
  const refKey = (repository: CheckpointRepository, ref: string) =>
    Effect.promise(async () => {
      const environment = process.env.GIT_ALTERNATE_OBJECT_DIRECTORIES ?? "";
      const relative = environment
        .split(NodePath.delimiter)
        .some((entry) => entry.length > 0 && !NodePath.isAbsolute(entry));
      if (relative) return null;
      const alternates = await statFingerprint(
        NodePath.join(repository.objectsDir, "info", "alternates"),
      );
      return `${repository.commonDir}\0${repository.objectsDir}\0${alternates}\0${environment}\0${ref}`;
    });

  const packedRefs = Effect.fnUntraced(function* (commonDir: string) {
    const current = yield* readPackedRefs(commonDir, packed.get(commonDir));
    // Concurrent reloads of one file: account for what the map holds after the read, not before.
    const present = packed.get(commonDir);
    if (current === null || current === present) return current;
    if (present !== undefined) {
      packed.delete(commonDir);
      packedEntries -= present.refs?.size ?? 0;
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

  // Git's files backend: the loose file wins, then packed-refs. A reftable store (which only the
  // repository's own config declares, so the snapshot may still say files) keeps neither, and a
  // name absent from both is one rev-parse would try under refs/heads and the like, so Git answers.
  const resolveRef = Effect.fnUntraced(function* (repository: CheckpointRepository, ref: string) {
    const { commonDir } = repository;
    if (!CHECKPOINT_REF_NAME.test(ref) || !environmentPlain()) return undefined;
    const key = yield* refKey(repository, ref);
    const known = key === null ? undefined : refs.get(key);
    if (known === undefined) return undefined;
    const reftable = yield* Effect.promise(() =>
      statFingerprint(NodePath.join(commonDir, "reftable")),
    );
    if (reftable !== "!ENOENT") return undefined;
    const loose = yield* readLooseRef(NodePath.join(commonDir, ref));
    if (loose.kind === "oid") return loose.oid === known ? known : undefined;
    if (loose.kind === "other") return undefined;
    const current = yield* packedRefs(commonDir);
    if (current === null || current.refs === null) return undefined;
    const entry = current.refs.get(ref);
    return entry === known ? known : undefined;
  });

  const historyState = Effect.fnUntraced(function* (commonDir: string) {
    if (!environmentPlain()) return null;
    const files = yield* historyFiles(commonDir);
    if (files === null) return null;
    const current = yield* packedRefs(commonDir);
    if (current === null || current.replaceRefs !== false) return null;
    return `${files}${current.fingerprint}`;
  });

  const commitFacts = Effect.fnUntraced(function* (repository: CheckpointRepository, oid: string) {
    if ((yield* historyState(repository.commonDir)) === null) return null;
    const remembered = commits.get(oid);
    if (remembered !== undefined) return remembered;
    const facts = yield* readLooseCommit(repository.objectsDir, oid);
    if (facts !== null) commits.set(oid, facts);
    return facts;
  });

  // Facts Git reported describe the history as it was while Git ran; keep them only when the
  // state taken before the run is still the state after it.
  const rememberCommits = Effect.fnUntraced(function* (
    repository: CheckpointRepository,
    facts: ReadonlyArray<readonly [oid: string, facts: CommitFacts]>,
    stateBefore: string | null,
  ) {
    if (facts.length === 0 || stateBefore === null) return;
    if ((yield* historyState(repository.commonDir)) !== stateBefore) return;
    for (const [oid, commit] of facts) commits.set(oid, commit);
  });

  return {
    rememberRef: (repository, ref, oid) =>
      Effect.map(refKey(repository, ref), (key) => {
        if (key !== null) refs.set(key, oid);
      }),
    forgetRef: (repository, ref) =>
      Effect.map(refKey(repository, ref), (key) => {
        if (key !== null) refs.delete(key);
      }),
    resolveRef,
    historyState,
    rememberCommits,
    commitFacts,
    packedRefsEntries: () => packedEntries,
  };
};

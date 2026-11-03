// @effect-diagnostics-next-line nodeBuiltinImport:off - Effect's File.Info has no ctime or nanosecond mtime, which the fingerprint needs.
import * as NodeFS from "node:fs";

import type { GitCommandError, VcsError } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Cache from "effect/Cache";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import type * as GitVcsDriver from "./GitVcsDriver.ts";

export interface GitConfigEntry {
  readonly origin: string;
  readonly key: string;
  readonly value: string | null;
}

// `git config --list --show-origin -z` prints origin NUL key [LF value] NUL; no LF means no value.
export const parseGitConfigList = (stdout: string): ReadonlyArray<GitConfigEntry> | null => {
  if (stdout.length === 0) return [];
  if (!stdout.endsWith("\0")) return null;
  const records = stdout.slice(0, -1).split("\0");
  if (records.length % 2 !== 0) return null;
  const entries: GitConfigEntry[] = [];
  for (let index = 0; index < records.length; index += 2) {
    const record = records[index + 1]!;
    const newline = record.indexOf("\n");
    entries.push({
      origin: records[index]!,
      key: newline === -1 ? record : record.slice(0, newline),
      value: newline === -1 ? null : record.slice(newline + 1),
    });
  }
  return entries;
};

const lowerAscii = (value: string) => value.replace(/[A-Z]+/g, (letters) => letters.toLowerCase());

// Git matches keys with the section and variable name lowercased and the subsection as written.
const canonicalGitConfigKey = (key: string) => {
  const first = key.indexOf(".");
  if (first === -1) return key;
  const last = key.lastIndexOf(".");
  return `${lowerAscii(key.slice(0, first))}${key.slice(first, last + 1)}${lowerAscii(key.slice(last + 1))}`;
};

// What `git config --get <key>` prints: the last value, an empty line for a key without one.
export const gitConfigGetOutput = (
  values: ReadonlyMap<string, string | null>,
  key: string,
): string => {
  const canonicalKey = canonicalGitConfigKey(key);
  if (!values.has(canonicalKey)) return "";
  return `${values.get(canonicalKey) ?? ""}\n`;
};

const GIT_BOOLEAN_WORDS = new Map([
  ["true", true],
  ["yes", true],
  ["on", true],
  ["false", false],
  ["no", false],
  ["off", false],
]);

const gitBoolean = (value: string | null): boolean | null => {
  if (value === null) return true;
  if (value === "") return false;
  const word = GIT_BOOLEAN_WORDS.get(lowerAscii(value));
  if (word !== undefined) return word;
  return /^(?:0|[1-9][0-9]{0,8})$/.test(value) ? value !== "0" : null;
};

/**
 * Whether `git config --bool <key>` prints "true": Git parses every value of the key and prints the
 * last. Null when one is an integer beyond plain decimals or invalid, which Git parses (or rejects).
 */
export const gitConfigBool = (
  valueLists: ReadonlyMap<string, ReadonlyArray<string | null>>,
  key: string,
): boolean | null => {
  let parsed: boolean | null = false;
  for (const value of valueLists.get(canonicalGitConfigKey(key)) ?? []) {
    parsed = gitBoolean(value);
    if (parsed === null) return null;
  }
  return parsed;
};

const INCLUDE_CONDITION_PREFIXES = ["gitdir:", "gitdir/i:", "hasconfig:remote.*.url:"] as const;

const includeTarget = (
  value: string | null,
  originPath: string | null,
  options: GitConfigDependencyOptions,
): string | null => {
  if (value === null || value.length === 0) return null;
  if (value.startsWith("~")) {
    if (value !== "~" && !value.startsWith("~/")) return null;
    return options.home ? `${options.home}${value.slice(1)}` : null;
  }
  if (value.startsWith("%(prefix)/")) return null;
  if (options.isAbsolute(value)) return value;
  if (originPath === null || !options.isAbsolute(originPath)) return null;
  // Git appends a relative include to the including file's directory without normalising it.
  const separator = Math.max(
    originPath.lastIndexOf("/"),
    options.windows ? originPath.lastIndexOf("\\") : -1,
  );
  return `${originPath.slice(0, separator + 1)}${value}`;
};

export interface GitConfigDependencyOptions {
  readonly home: string | undefined;
  readonly windows: boolean;
  readonly isAbsolute: (path: string) => boolean;
}

/**
 * Files the listed values came from, and the includes they name (read or not), so a change to
 * any of them is seen. Relative origins are the repository's own config files, which the caller
 * tracks. `supported` is false when a value may depend on something these files do not cover.
 */
export const gitConfigListDependencies = (
  entries: ReadonlyArray<GitConfigEntry>,
  options: GitConfigDependencyOptions,
): { readonly paths: ReadonlyArray<string>; readonly supported: boolean } => {
  const paths = new Set<string>();
  let supported = true;
  for (const entry of entries) {
    let originPath: string | null = null;
    if (entry.origin.startsWith("file:")) {
      originPath = entry.origin.slice("file:".length);
      if (options.isAbsolute(originPath)) paths.add(originPath);
    } else if (entry.origin !== "command line:") {
      supported = false;
    }
    const conditional = entry.key.startsWith("includeif.") && entry.key.endsWith(".path");
    if (entry.key !== "include.path" && !conditional) continue;
    if (conditional) {
      const condition = entry.key.slice("includeif.".length, -".path".length);
      if (!INCLUDE_CONDITION_PREFIXES.some((prefix) => condition.startsWith(prefix))) {
        supported = false;
      }
    }
    const target = includeTarget(entry.value, originPath, options);
    if (target === null) supported = false;
    else paths.add(target);
  }
  return { paths: [...paths], supported };
};

const CONFIG_ENVIRONMENT_NAMES = [
  "HOME",
  "XDG_CONFIG_HOME",
  "PATH",
  "GIT_CONFIG_GLOBAL",
  "GIT_CONFIG_SYSTEM",
  "GIT_CONFIG_NOSYSTEM",
  "GIT_CONFIG_PARAMETERS",
  "GIT_CONFIG_COUNT",
  "GIT_DIR",
  "GIT_COMMON_DIR",
  "GIT_WORK_TREE",
  "GIT_CEILING_DIRECTORIES",
  "GIT_DISCOVERY_ACROSS_FILESYSTEM",
  "GIT_INDEX_FILE",
  "GIT_OBJECT_DIRECTORY",
  "GIT_ALTERNATE_OBJECT_DIRECTORIES",
] as const;
const MAX_CONFIG_ENVIRONMENT_ENTRIES = 1_000;

// The environment that chooses which config Git reads; null when `GIT_CONFIG` names the only file
// or there are more `GIT_CONFIG_KEY_<n>` overrides than the signature takes.
export const gitConfigEnvironmentSignature = (env: NodeJS.ProcessEnv): string | null => {
  if (env.GIT_CONFIG !== undefined) return null;
  const parts: Array<string | null> = CONFIG_ENVIRONMENT_NAMES.map((name) => env[name] ?? null);
  const count = Number.parseInt(env.GIT_CONFIG_COUNT ?? "", 10);
  if (count > MAX_CONFIG_ENVIRONMENT_ENTRIES) return null;
  for (let index = 0; index < count; index++) {
    parts.push(env[`GIT_CONFIG_KEY_${index}`] ?? null, env[`GIT_CONFIG_VALUE_${index}`] ?? null);
  }
  return JSON.stringify(parts);
};

export interface GitConfigDependency {
  readonly path: string;
  /**
   * content: any change. identity: a directory's identity (a file's content), for the `.git` Git
   * rewrites often. presence: whether it exists. absent: the snapshot holds only while it is missing,
   * for the markers that would make Git find another repository on the way up. physical: the path
   * with symbolic links resolved. entry: like content, for the path itself when it is a symbolic link.
   */
  readonly kind: "content" | "identity" | "presence" | "absent" | "physical" | "entry";
}

const dependencyKey = (dependency: GitConfigDependency) => `${dependency.kind}:${dependency.path}`;

// Git replaces config files by rename, which changes the inode. An in-place edit of the same size can
// go unseen when it lands within the filesystem's timestamp granularity (Linux stamps from a coarse
// clock, ~4 ms) or behind NFS attribute caching. Ownership counts only on the read directory and the
// found `.git`, so a chown elsewhere that trips safe.directory is not seen.
const statFingerprint = (dependency: GitConfigDependency) =>
  new Promise<string>((resolve) => {
    try {
      if (dependency.kind === "physical") {
        NodeFS.realpath.native(dependency.path, (error, resolved) =>
          resolve(error ? `!${error.code ?? "error"}` : resolved),
        );
        return;
      }
      const stat = dependency.kind === "entry" ? NodeFS.lstat : NodeFS.stat;
      stat(dependency.path, { bigint: true }, (error, stats) => {
        if (error) {
          resolve(`!${error.code ?? "error"}`);
          return;
        }
        if (dependency.kind === "presence" || dependency.kind === "absent") {
          resolve(stats.isDirectory() ? "directory" : "file");
          return;
        }
        const identity = `${stats.dev}:${stats.ino}:${stats.mode}:${stats.uid}`;
        resolve(
          dependency.kind === "identity" && stats.isDirectory()
            ? identity
            : `${identity}:${stats.size}:${stats.mtimeNs}:${stats.ctimeNs}`,
        );
      });
    } catch {
      resolve("!invalid");
    }
  });

const fingerprintDependencies = (dependencies: ReadonlyArray<GitConfigDependency>) =>
  Effect.promise(() => Promise.all(dependencies.map(statFingerprint)));

const fileIdentity = (path: string) =>
  new Promise<string | null>((resolve) => {
    try {
      NodeFS.stat(path, { bigint: true }, (error, stats) =>
        resolve(error ? null : `${stats.dev}:${stats.ino}`),
      );
    } catch {
      resolve(null);
    }
  });

const isSameFile = (left: string, right: string) =>
  Effect.promise(() => Promise.all([fileIdentity(left), fileIdentity(right)])).pipe(
    Effect.map(
      ([leftIdentity, rightIdentity]) => leftIdentity !== null && leftIdentity === rightIdentity,
    ),
  );

export interface GitConfigSnapshot {
  readonly values: ReadonlyMap<string, string | null>;
  // Every value of each key, in Git's order.
  readonly valueLists: ReadonlyMap<string, ReadonlyArray<string | null>>;
  // Results of commands that read only config, valid while the snapshot is.
  readonly results: Map<string, GitVcsDriver.ExecuteGitResult>;
  // What `git rev-parse --git-path index` prints in the directory.
  readonly indexPath: string;
  // The common directory while refs are files there (Git's files backend), else null.
  readonly commonDir: string | null;
  // Results of commands that read only config and refs, see runWithRefs.
  readonly refResults: Map<string, CachedGitRefResult>;
  // Results that depend only on config and on named files, see runWithFiles.
  readonly fileResults: Map<string, CachedFileResult>;
  // What `git rev-parse --show-toplevel` prints, and the directory's path below it.
  readonly toplevel: string;
  readonly prefixSegments: ReadonlyArray<string>;
  // Absolute: the git directory, the common directory, the index, the object store, HEAD and info/exclude.
  readonly gitDir: string;
  readonly gitCommonDir: string;
  readonly absoluteIndexPath: string;
  readonly objectsDir: string;
  readonly headPath: string;
  readonly infoExcludePath: string;
}

interface CachedFileResult {
  readonly dependencies: ReadonlyArray<GitConfigDependency>;
  readonly fingerprint: string;
  readonly value: unknown;
}

interface CachedGitRefResult {
  readonly dependencies: ReadonlyArray<GitConfigDependency>;
  // Null when the run was not preceded by a stat of every dependency, so it must run again.
  readonly fingerprint: string | null;
  readonly result: GitVcsDriver.ExecuteGitResult;
}

interface CachedGitConfigSnapshot {
  readonly environment: string;
  readonly dependencies: ReadonlyArray<GitConfigDependency>;
  readonly fingerprint: string;
  // null: a value may depend on something the dependencies miss, so every read runs Git.
  readonly snapshot: GitConfigSnapshot | null;
}

type RunGit = (
  operation: string,
  cwd: string,
  args: ReadonlyArray<string>,
) => Effect.Effect<GitVcsDriver.ExecuteGitResult, GitCommandError | VcsError>;

const CONFIG_SNAPSHOT_CACHE_CAPACITY = 2_048;
const CONFIG_SNAPSHOT_RETRY_TTL = Duration.seconds(1);
const CONFIG_SNAPSHOT_LOAD_ATTEMPTS = 3;
const CONFIG_FILE_LOCATIONS_CAPACITY = 16;
const REF_RESULTS_CAPACITY = 256;
const REPOSITORY_PATH_COUNT = 12;
const FILE_RESULTS_CAPACITY = 64;
const REUSED_RESULTS_CAPACITY = 64;
// Ref names whose loose file Git's files backend keeps at <common dir>/<name>: refs shared by every
// worktree, under refs/ or (like `<remote>/<branch>` when Git resolves a short name) outside it.
const isLooseRefName = (name: string) =>
  (name.startsWith("refs/")
    ? !/^refs\/(?:worktree|bisect|rewritten)\//.test(name)
    : name.includes("/") && !/^(?:main-worktree|worktrees)\//.test(name)) &&
  !/[\0-\x20\x7f~^:?*[\\]|\.\.|@\{|\.$/.test(name) &&
  name
    .split("/")
    .every((part) => part.length > 0 && !part.startsWith(".") && !part.endsWith(".lock"));

const isReusableResult = (result: GitVcsDriver.ExecuteGitResult) =>
  result.exitCode !== null &&
  result.exitCode < 128 &&
  !result.stdoutTruncated &&
  !result.stderrTruncated;

// A loose ref that names another ("ref: ...", or a symbolic link Git reads relative to its directory).
const readsSymbolicRef = (file: string) =>
  new Promise<boolean>((resolve) => {
    try {
      NodeFS.lstat(file, (statError, stats) => {
        if (statError) return resolve(false);
        if (stats.isSymbolicLink()) return resolve(true);
        NodeFS.readFile(file, (error, data) =>
          resolve(error === null && data.subarray(0, 4).toString("latin1") === "ref:"),
        );
      });
    } catch {
      resolve(true);
    }
  });

const REPOSITORY_PATH_ARGS = [
  "rev-parse",
  // Before --path-format, so it prints what `git rev-parse --git-path index` prints.
  "--git-path",
  "index",
  "--path-format=absolute",
  "--show-toplevel",
  "--absolute-git-dir",
  "--git-path",
  "config",
  "--git-path",
  "config.worktree",
  "--git-path",
  "commondir",
  "--git-path",
  "remotes",
  "--git-path",
  "branches",
  "--git-path",
  "HEAD",
  "--git-path",
  "objects",
  "--git-path",
  "refs",
  // The real common directory: a --git-path answer resolves symbolic links, so a symlinked
  // refs directory would otherwise name its target's parent.
  "--git-common-dir",
  "--git-path",
  "index",
  "--show-prefix",
] as const;

/**
 * Per-directory snapshots of `git config --list`, reused while the files that chose and supplied
 * those values keep the same stat. Every dependency is stat'ed before the read that depends on it,
 * so a write during a load leaves the snapshot stale rather than wrong.
 */
export const makeGitConfigSnapshots = Effect.fn("makeGitConfigSnapshots")(function* (
  runGit: RunGit,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const windows = (yield* HostProcess.Platform) === "win32";
  // Git before 2.42 has no `git var GIT_CONFIG_*`, so it cannot name its global and system files.
  let supported = true;
  const configFileLocations = new Map<string, ReadonlyArray<string>>();
  const readOnce = new Set<string>();
  const reloadDependencies = new Map<string, ReadonlyArray<GitConfigDependency>>();

  const listDirectory = (directory: string) =>
    fileSystem.readDirectory(directory).pipe(
      Effect.map((names) => names.map((name) => path.join(directory, name))),
      Effect.orElseSucceed((): ReadonlyArray<string> => []),
    );

  const configFiles = (result: GitVcsDriver.ExecuteGitResult) => {
    if (result.exitCode === 0) return result.stdout.split("\n").filter((line) => line.length > 0);
    if (result.exitCode === 1) return [];
    if (result.exitCode === 129) supported = false;
    return null;
  };

  // The global and system files depend only on the environment and the Git build.
  const readConfigFileLocations = Effect.fn("GitVcsDriver.configSnapshot.configFiles")(function* (
    cwd: string,
    environment: string,
  ) {
    const known = configFileLocations.get(environment);
    if (known !== undefined) return known;
    const [globalResult, systemResult] = yield* Effect.all(
      [
        runGit("GitVcsDriver.configSnapshot.globalFiles", cwd, ["var", "GIT_CONFIG_GLOBAL"]),
        runGit("GitVcsDriver.configSnapshot.systemFiles", cwd, ["var", "GIT_CONFIG_SYSTEM"]),
      ],
      { concurrency: 2 },
    );
    const globalFiles = configFiles(globalResult);
    const systemFiles = configFiles(systemResult);
    if (globalFiles === null || systemFiles === null) return null;
    if (configFileLocations.size >= CONFIG_FILE_LOCATIONS_CAPACITY) configFileLocations.clear();
    const files = [...globalFiles, ...systemFiles];
    configFileLocations.set(environment, files);
    return files;
  });

  const readConfigState = Effect.fn("GitVcsDriver.configSnapshot.read")(function* (
    cwd: string,
    environment: string,
  ) {
    const pathsResult = yield* runGit(
      "GitVcsDriver.configSnapshot.paths",
      cwd,
      REPOSITORY_PATH_ARGS,
    );
    if (pathsResult.exitCode !== 0) return null;
    const lines = pathsResult.stdout.split("\n");
    // Git before 2.31 echoes --path-format; a path containing a newline spans lines.
    if (lines[1] === "--path-format=absolute") {
      supported = false;
      return null;
    }
    const [indexPath = "", ...rest] = lines;
    const paths = rest.slice(0, REPOSITORY_PATH_COUNT);
    const prefix = rest[REPOSITORY_PATH_COUNT];
    if (
      lines.length !== REPOSITORY_PATH_COUNT + 3 ||
      prefix === undefined ||
      lines.at(-1) !== "" ||
      indexPath.length === 0 ||
      !paths.every((line) => line.length > 0 && path.isAbsolute(line))
    ) {
      return null;
    }
    const [
      toplevel,
      gitDir,
      config,
      configWorktree,
      commondir,
      remotes,
      branches,
      head,
      objects,
      refs,
      gitCommonDir,
      absoluteIndexPath,
    ] = paths as [
      string,
      string,
      string,
      string,
      string,
      string,
      string,
      string,
      string,
      string,
      string,
      string,
    ];
    const segments = prefix.split("/").filter((segment) => segment.length > 0);
    const [locations, listResult, remoteFiles, branchFiles, insideTopLevel] = yield* Effect.all(
      [
        readConfigFileLocations(cwd, environment),
        runGit("GitVcsDriver.configSnapshot.list", cwd, [
          "config",
          "--list",
          "--show-origin",
          "-z",
        ]),
        listDirectory(remotes),
        listDirectory(branches),
        isSameFile(cwd, path.join(toplevel, ...segments)),
      ],
      { concurrency: "unbounded" },
    );
    // A directory outside the work tree (GIT_WORK_TREE elsewhere) has no prefix to walk.
    if (locations === null || !insideTopLevel) return null;

    // Discovery: the directory, what would make Git stop below the top level (a `.git`, or HEAD of a
    // bare repository), the `.git` it found, and what keeps that a repository. The prefix comes from
    // Git, cased as on disk like the top level, whatever case the directory was given in; the physical
    // path keeps it, and the index path Git prints from it, current.
    const dependencies: GitConfigDependency[] = [
      { path: cwd, kind: "identity" },
      { path: cwd, kind: "physical" },
    ];
    for (let depth = segments.length; depth > 0; depth--) {
      const directory = path.join(toplevel, ...segments.slice(0, depth));
      dependencies.push(
        { path: path.join(directory, ".git"), kind: "absent" },
        { path: path.join(directory, "HEAD"), kind: "absent" },
      );
    }
    // The alternates decide which objects a ref may point to.
    dependencies.push(
      { path: path.join(toplevel, ".git"), kind: "identity" },
      { path: head, kind: "content" },
      // With core.preferSymlinkRefs HEAD is a link whose target names the branch.
      { path: path.join(gitDir, "HEAD"), kind: "entry" },
      { path: objects, kind: "presence" },
      { path: path.join(objects, "info", "alternates"), kind: "content" },
      { path: refs, kind: "presence" },
    );
    const files = [
      config,
      configWorktree,
      commondir,
      remotes,
      ...remoteFiles,
      branches,
      ...branchFiles,
      ...locations,
    ];
    let snapshotSupported = locations.every((file) => path.isAbsolute(file));
    const entries =
      listResult.exitCode === 0 && !listResult.stdoutTruncated
        ? parseGitConfigList(listResult.stdout)
        : null;
    if (entries === null) {
      snapshotSupported = false;
    } else {
      const listed = gitConfigListDependencies(entries, {
        home: process.env.HOME,
        windows,
        isAbsolute: path.isAbsolute,
      });
      files.push(...listed.paths);
      snapshotSupported &&= listed.supported;
    }
    for (const file of files) dependencies.push({ path: file, kind: "content" });
    const values = new Map<string, string | null>();
    const valueLists = new Map<string, Array<string | null>>();
    for (const entry of entries ?? []) {
      values.set(entry.key, entry.value);
      const list = valueLists.get(entry.key);
      if (list === undefined) valueLists.set(entry.key, [entry.value]);
      else list.push(entry.value);
    }
    const refStorage = values.get("extensions.refstorage");
    return {
      dependencies,
      values: snapshotSupported ? values : null,
      valueLists,
      indexPath,
      // Refs live in the common directory.
      commonDir: refStorage === undefined || refStorage === "files" ? gitCommonDir : null,
      toplevel,
      prefixSegments: segments,
      gitDir,
      gitCommonDir,
      absoluteIndexPath,
      objectsDir: objects,
      headPath: head,
      // Not --git-path info/exclude, which git prints with a symbolic link resolved: the link itself
      // must be the dependency, so retargeting it is seen.
      infoExcludePath: path.join(gitCommonDir, "info", "exclude"),
    };
  });

  const load = Effect.fn("GitVcsDriver.configSnapshot.load")(
    function* (cwd: string) {
      const environment = gitConfigEnvironmentSignature(process.env);
      if (environment === null) return null;
      let dependencies = reloadDependencies.get(cwd) ?? [];
      reloadDependencies.delete(cwd);
      for (let attempt = 0; attempt < CONFIG_SNAPSHOT_LOAD_ATTEMPTS; attempt++) {
        const fingerprints = yield* fingerprintDependencies(dependencies);
        const state = yield* readConfigState(cwd, environment);
        if (state === null) return null;
        const fingerprintByKey = new Map(
          dependencies.map((dependency, index) => [
            dependencyKey(dependency),
            fingerprints[index]!,
          ]),
        );
        const unique = [
          ...new Map(
            state.dependencies.map((dependency) => [dependencyKey(dependency), dependency]),
          ),
        ].map(([, dependency]) => dependency);
        if (unique.every((dependency) => fingerprintByKey.has(dependencyKey(dependency)))) {
          const fingerprint = unique.map((dependency) =>
            fingerprintByKey.get(dependencyKey(dependency))!,
          );
          const discovered = unique.every(
            (dependency, index) =>
              dependency.kind !== "absent" || fingerprint[index]!.startsWith("!"),
          );
          return {
            environment,
            dependencies: unique,
            fingerprint: fingerprint.join("\n"),
            snapshot:
              state.values === null || !discovered
                ? null
                : {
                    values: state.values,
                    valueLists: state.valueLists,
                    results: new Map(),
                    indexPath: state.indexPath,
                    commonDir: state.commonDir,
                    refResults: new Map(),
                    fileResults: new Map(),
                    toplevel: state.toplevel,
                    prefixSegments: state.prefixSegments,
                    gitDir: state.gitDir,
                    gitCommonDir: state.gitCommonDir,
                    absoluteIndexPath: state.absoluteIndexPath,
                    objectsDir: state.objectsDir,
                    headPath: state.headPath,
                    infoExcludePath: state.infoExcludePath,
                  },
          } satisfies CachedGitConfigSnapshot;
        }
        // Something was read before it was stat'ed; stat it and read again.
        dependencies = unique;
      }
      return null;
    },
    Effect.orElseSucceed(() => null),
  );

  const snapshots = yield* Cache.makeWith(load, {
    capacity: CONFIG_SNAPSHOT_CACHE_CAPACITY,
    timeToLive: (exit) =>
      Exit.isSuccess(exit) && exit.value !== null ? Duration.infinity : CONFIG_SNAPSHOT_RETRY_TTL,
  });

  const isCurrent = (cached: CachedGitConfigSnapshot) =>
    cached.environment === gitConfigEnvironmentSignature(process.env)
      ? fingerprintDependencies(cached.dependencies).pipe(
          Effect.map((fingerprints) => fingerprints.join("\n") === cached.fingerprint),
        )
      : Effect.succeed(false);

  // A one-off read runs one command; a directory read again, such as on every refresh, is loaded.
  const isReadAgain = (cwd: string) => {
    if (readOnce.has(cwd)) return true;
    if (readOnce.size >= CONFIG_SNAPSHOT_CACHE_CAPACITY) {
      readOnce.delete(readOnce.values().next().value!);
    }
    readOnce.add(cwd);
    return false;
  };

  /**
   * The current snapshot for `cwd`, or null when this read must run Git. Untraced: it runs on every
   * config read, several per status refresh; loads keep their spans.
   */
  const read = Effect.fnUntraced(function* (cwd: string) {
    if (!supported) return null;
    if (!(yield* Cache.has(snapshots, cwd)) && !isReadAgain(cwd)) return null;
    const cached = yield* Cache.get(snapshots, cwd);
    if (cached === null) return null;
    if (yield* isCurrent(cached)) return cached.snapshot;
    reloadDependencies.set(cwd, cached.dependencies);
    yield* Cache.invalidateWhen(snapshots, cwd, (current) => current === cached);
    // Whatever replaces the stale entry loaded after this read began, so it needs no check.
    const reloaded = yield* Cache.get(snapshots, cwd);
    reloadDependencies.delete(cwd);
    return reloaded?.snapshot ?? null;
  });

  // Like read, but a directory without a snapshot stays without one (a non-repository polled for status).
  const peek = Effect.fnUntraced(function* (cwd: string) {
    if (!supported || !(yield* Cache.has(snapshots, cwd))) return null;
    return yield* read(cwd);
  });

  // The refs' loose files (kind entry), packed-refs, and reftable, which must stay absent: Git takes
  // extensions from the repository's own config file, not from the merged config the snapshot holds.
  const refDependencies = (
    commonDir: string,
    names: ReadonlyArray<string>,
  ): ReadonlyArray<GitConfigDependency> => [
    ...names.map((name) => ({ path: path.join(commonDir, name), kind: "entry" as const })),
    { path: path.join(commonDir, "packed-refs"), kind: "content" },
    { path: path.join(commonDir, "reftable"), kind: "presence" },
  ];

  /**
   * Runs a command whose result depends only on the snapshot and on some refs, once per state of
   * their loose files and packed-refs. `refs` names those refs before the run (called without a
   * result) or from its result; null means the result cannot be reused. Untraced like read: it runs
   * for every ref probe, and the Git command it may run keeps its own span.
   */
  const runWithRefs = Effect.fnUntraced(function* <E>(
    snapshot: GitConfigSnapshot | null,
    args: ReadonlyArray<string>,
    refs: (result?: GitVcsDriver.ExecuteGitResult) => ReadonlyArray<string> | null,
    run: Effect.Effect<GitVcsDriver.ExecuteGitResult, E>,
  ) {
    const commonDir = snapshot?.commonDir ?? null;
    if (snapshot === null || commonDir === null) return yield* run;
    const key = args.join("\0");
    const cached = snapshot.refResults.get(key);
    const known = refs();
    const dependencies =
      cached?.dependencies ?? (known === null ? [] : refDependencies(commonDir, known));
    const fingerprints = yield* fingerprintDependencies(dependencies);
    const fingerprint = fingerprints.join("\n");
    if (cached !== undefined && cached.fingerprint === fingerprint) return cached.result;
    const result = yield* run;
    const names = isReusableResult(result) ? refs(result) : null;
    const needed = names?.every(isLooseRefName) ? refDependencies(commonDir, names) : null;
    // A symbolic ref's result depends on its target, which is not tracked.
    const symbolic =
      needed !== null &&
      (yield* Effect.promise(() =>
        Promise.all(
          needed
            .filter((dependency) => dependency.kind === "entry")
            .map((dependency) => readsSymbolicRef(dependency.path)),
        ),
      )).some(Boolean);
    const statedFirst =
      needed !== null &&
      needed.length === dependencies.length &&
      needed.every((dependency, index) => dependency.path === dependencies[index]!.path);
    const reftable = statedFirst && !fingerprints.at(-1)!.startsWith("!");
    if (needed === null || symbolic || reftable) {
      snapshot.refResults.delete(key);
      return result;
    }
    if (!snapshot.refResults.has(key) && snapshot.refResults.size >= REF_RESULTS_CAPACITY) {
      snapshot.refResults.delete(snapshot.refResults.keys().next().value!);
    }
    snapshot.refResults.set(key, {
      dependencies: needed,
      fingerprint: statedFirst ? fingerprint : null,
      result,
    });
    return result;
  });

  /**
   * Runs an effect whose result depends only on the snapshot and on the content of `files`, once
   * per state of those files. Every file is stat'ed before the run, so a write during the run
   * leaves the result stale for one read rather than wrong. Failures are not kept.
   */
  const runWithFiles = Effect.fnUntraced(function* <A, E, R>(
    snapshot: GitConfigSnapshot,
    key: string,
    files: ReadonlyArray<string>,
    run: Effect.Effect<A, E, R>,
  ) {
    const dependencies = files.map((file): GitConfigDependency => ({
      path: file,
      kind: "content",
    }));
    const fingerprint = (yield* fingerprintDependencies(dependencies)).join("\n");
    const cached = snapshot.fileResults.get(key);
    if (
      cached !== undefined &&
      cached.fingerprint === fingerprint &&
      cached.dependencies.length === dependencies.length &&
      cached.dependencies.every((dependency, index) => dependency.path === files[index])
    ) {
      return cached.value as A;
    }
    const value = yield* run;
    if (!snapshot.fileResults.has(key) && snapshot.fileResults.size >= FILE_RESULTS_CAPACITY) {
      snapshot.fileResults.delete(snapshot.fileResults.keys().next().value!);
    }
    snapshot.fileResults.set(key, { dependencies, fingerprint, value });
    return value;
  });

  return { read, peek, runWithRefs, runWithFiles };
});

/**
 * Runs a command whose result depends only on config (such as `git remote`) once per snapshot.
 * Exit codes from 128 up are fatal errors or signals that a later run could avoid, so they rerun.
 */
export const runWithGitConfigSnapshot = (
  snapshot: GitConfigSnapshot | null,
  args: ReadonlyArray<string>,
  run: Effect.Effect<GitVcsDriver.ExecuteGitResult, GitCommandError>,
): Effect.Effect<GitVcsDriver.ExecuteGitResult, GitCommandError> => {
  if (snapshot === null) return run;
  const key = args.join("\0");
  const reused = snapshot.results.get(key);
  if (reused !== undefined) return Effect.succeed(reused);
  return run.pipe(
    Effect.tap((result) =>
      Effect.sync(() => {
        if (isReusableResult(result)) {
          if (snapshot.results.size >= REUSED_RESULTS_CAPACITY) {
            snapshot.results.delete(snapshot.results.keys().next().value!);
          }
          snapshot.results.set(key, result);
        }
      }),
    ),
  );
};

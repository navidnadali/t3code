import * as Clock from "effect/Clock";
// @effect-diagnostics nodeBuiltinImport:off - the git pool is sized from the machine (os) and its cgroup CPU quota (fs).
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Match from "effect/Match";
import * as Schedule from "effect/Schedule";
import * as Semaphore from "effect/Semaphore";
import { ChildProcessSpawner } from "effect/process";

import {
  type VcsError,
  type VcsProcessErrorContext,
  VcsProcessExitError,
  type VcsProcessExitFailureKind,
  VcsProcessMissingExitCodeError,
  VcsProcessOutputLimitError,
  VcsProcessOutputReadError,
  VcsProcessSpawnError,
  VcsProcessStdinWriteError,
  VcsProcessTimeoutError,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import type { SourceControlProcessInput } from "@t3tools/source-control-core/server/SourceControlHost";

import { gitCommandDuration, gitCommandsTotal, withMetrics } from "../observability/Metrics.ts";
import * as WorkerChildProcessSpawner from "../process/workerChildProcessSpawner.ts";
import * as ProcessRunner from "../processRunner.ts";

export interface VcsProcessInput {
  readonly operation: string;
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly spawnCwd?: string;
  readonly stdin?: string;
  readonly onStdoutChunk?: (chunk: Uint8Array) => void;
  readonly env?: NodeJS.ProcessEnv;
  readonly allowNonZeroExit?: boolean;
  readonly timeoutMs?: number;
  readonly maxOutputBytes?: number;
  readonly outputMode?: ProcessRunner.ProcessRunInput["outputMode"];
  readonly appendTruncationMarker?: boolean;
  readonly classifyFailure?: SourceControlProcessInput["classifyFailure"];
  readonly notFoundDetail?: string;
  readonly concurrency?: SourceControlProcessInput["concurrency"];
}

export interface VcsProcessOutput {
  readonly exitCode: ChildProcessSpawner.ExitCode;
  readonly stdout: string;
  readonly stderr: string;
  readonly stdoutTruncated: boolean;
  readonly stderrTruncated: boolean;
  readonly stdoutInvalidUtf8?: boolean;
  readonly stderrInvalidUtf8?: boolean;
}

export class VcsProcess extends Context.Service<
  VcsProcess,
  {
    readonly run: (input: VcsProcessInput) => Effect.Effect<VcsProcessOutput, VcsError>;
  }
>()("t3/vcs/VcsProcess") {}

const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_OUTPUT_BYTES = 1_000_000;
const OUTPUT_TRUNCATED_MARKER = "\n\n[truncated]";
// A quarter of the cores, between 8 (upstream's constant: no slot wait at 26 commands/s on the
// bench) and 32: git is I/O plus short CPU, and this leaves headroom for the host's other work,
// since more git processes than it can run in parallel only move the queue from this semaphore
// into the OS run queue, where it slows everything else on the box.
const GIT_PROCESS_CONCURRENCY_FLOOR = 8;
const GIT_PROCESS_CONCURRENCY_CEILING = 32;
const GIT_PROCESS_CORES_SHARE = 4;
// Forge CLIs (glab, gh, tea) are bounded for the remote's sake, not the machine's: each call is a
// network conversation of seconds, more at once gains nothing and risks the API's rate limit. They
// hold no git permit, so a forge call never delays a git command.
const FORGE_PROCESS_CONCURRENCY = 4;

/** Git processes allowed at once on a machine with `parallelism` cores and a cgroup CPU quota. */
export const gitProcessConcurrency = (parallelism: number, cpuQuota: number | null): number => {
  const cores = cpuQuota === null ? parallelism : Math.min(parallelism, cpuQuota);
  return Math.max(
    GIT_PROCESS_CONCURRENCY_FLOOR,
    Math.min(GIT_PROCESS_CONCURRENCY_CEILING, Math.floor(cores / GIT_PROCESS_CORES_SHARE)),
  );
};

export interface CgroupLocation {
  /** The mount table, /proc/self/mountinfo: where cgroup2 and the v1 cpu controller are mounted. */
  readonly mountinfo?: string;
  /** The process's cgroup membership file, /proc/self/cgroup. */
  readonly selfCgroup?: string;
}

const readText = (file: string) => {
  try {
    return NodeFS.readFileSync(file, "utf8");
  } catch {
    return null;
  }
};

interface CgroupMount {
  readonly mountPoint: string;
  // The hierarchy subtree the mount shows (mountinfo's root field), as path segments.
  readonly rootSegments: ReadonlyArray<string>;
}

const pathSegments = (value: string) => value.split("/").filter((segment) => segment.length > 0);

// cgroup2 mounts and the v1 mounts carrying the cpu controller, from mountinfo lines
// `… <root> <mount point> … - <fstype> <source> <super options>`.
const cgroupMounts = (mountinfo: string) => {
  const v2: CgroupMount[] = [];
  const v1Cpu: CgroupMount[] = [];
  for (const line of mountinfo.split("\n")) {
    const separator = line.indexOf(" - ");
    if (separator === -1) continue;
    const fields = line.slice(0, separator).split(" ");
    const [type, , options = ""] = line.slice(separator + 3).split(" ");
    const mountRoot = fields[3];
    const mountPoint = fields[4];
    if (mountRoot === undefined || mountPoint === undefined) continue;
    const mount = { mountPoint, rootSegments: pathSegments(mountRoot) };
    if (type === "cgroup2") v2.push(mount);
    else if (type === "cgroup" && options.split(",").includes("cpu")) v1Cpu.push(mount);
  }
  return { v2, v1Cpu };
};

// Where a cgroup (a hierarchy path) sits under a mount that shows only its root subtree: the
// path below the mount point, or null when the cgroup is outside that subtree.
const cgroupDirectory = (mount: CgroupMount, segments: ReadonlyArray<string>) => {
  const { rootSegments } = mount;
  if (rootSegments.some((segment, index) => segments[index] !== segment)) return null;
  return segments.slice(rootSegments.length);
};

/**
 * The CPU quota in cores that applies to this process: the tightest `cpu.max` (cgroup v2) or
 * cfs quota (v1) along its own cgroup path, since a service's limit sits in its own cgroup and
 * an ancestor's binds it too; null when none is enforced or the files cannot be read.
 */
export const cgroupCpuQuota = (location: CgroupLocation = {}): number | null => {
  const mountinfo = readText(location.mountinfo ?? "/proc/self/mountinfo");
  const membership = readText(location.selfCgroup ?? "/proc/self/cgroup");
  if (mountinfo === null || membership === null) return null;
  const mounts = cgroupMounts(mountinfo);
  const quotas: Array<number> = [];
  // Each line is hierarchy:controllers:path; the path may itself contain colons.
  for (const line of membership.split("\n")) {
    const first = line.indexOf(":");
    const second = first === -1 ? -1 : line.indexOf(":", first + 1);
    if (second === -1) continue;
    const controllers = line.slice(first + 1, second).split(",");
    const segments = pathSegments(line.slice(second + 1));
    // A process outside its cgroup namespace's root reports `..`: no limit is read from there.
    if (segments.includes("..")) continue;
    const isV2 = controllers.length === 1 && controllers[0] === "";
    if (!isV2 && !controllers.includes("cpu")) continue;
    for (const mount of isV2 ? mounts.v2 : mounts.v1Cpu) {
      const below = cgroupDirectory(mount, segments);
      if (below === null) continue;
      for (let depth = below.length; depth >= 0; depth--) {
        const directory = NodePath.join(mount.mountPoint, ...below.slice(0, depth));
        const cores = isV2
          ? parseCpuMax(readText(NodePath.join(directory, "cpu.max")))
          : parseCfs(
              readText(NodePath.join(directory, "cpu.cfs_quota_us")),
              readText(NodePath.join(directory, "cpu.cfs_period_us")),
            );
        if (cores !== null) quotas.push(cores);
      }
    }
  }
  return quotas.length === 0 ? null : Math.min(...quotas);
};

// "max 100000" is unlimited; "<quota> <period>" in microseconds.
const parseCpuMax = (content: string | null): number | null => {
  if (content === null) return null;
  const [quota, period] = content.trim().split(/\s+/);
  if (quota === undefined || quota === "max") return null;
  const cores = Number(quota) / Number(period ?? "100000");
  return Number.isFinite(cores) && cores > 0 ? cores : null;
};

const parseCfs = (quota: string | null, period: string | null): number | null => {
  const quotaUs = Number((quota ?? "-1").trim());
  const periodUs = Number((period ?? "100000").trim());
  return quotaUs > 0 && periodUs > 0 ? quotaUs / periodUs : null;
};

export interface VcsProcessPools {
  readonly parallelism?: number;
  readonly cpuQuota?: number | null;
}

/** Shown to users when a Git command fails because another process holds the index lock. */
export const GIT_INDEX_LOCKED_DETAIL =
  "Git's index is locked. Wait for other Git operations to finish, then try again.";

export const CHECKPOINT_CAPTURE_OPERATION = "GitVcsDriver.checkpoints.captureCheckpoint";

// Caller operations are diagnostic strings; only known labels may become metric attributes.
const gitMetricOperations = new Set([
  CHECKPOINT_CAPTURE_OPERATION,
  "GitVcsDriver.checkpoints.deleteCheckpointRefs",
  "GitVcsDriver.checkpoints.diffCheckpoints",
  "GitVcsDriver.checkpoints.hasHeadCommit",
  "GitVcsDriver.checkpoints.listAuthoredPaths",
  "GitVcsDriver.checkpoints.resolveCheckpointCommit",
  "GitVcsDriver.checkpoints.resolveGitCommonDir",
  "GitVcsDriver.checkpoints.resolveHeadCommit",
  "GitVcsDriver.checkpoints.restoreCheckpoint",
  "GitVcsDriver.detectRepository",
  "GitVcsDriver.filterIgnoredPaths",
  "GitVcsDriver.initRepository",
  "GitVcsDriver.isInsideWorkTree",
  "GitVcsDriver.listRemotes",
  "GitVcsDriver.listWorkspaceFiles",
  "GitVcsDriver.resolveGitCommonDir",
]);

/**
 * Append after inherited entries without changing argv or the user's config.
 * Windows env keys ignore case: read the last spelling of the count so command overrides win,
 * then update every spelling because Node may keep any one.
 * Leave invalid counts untouched so Git reports them.
 */
export const gitConfigEnv = (
  env: NodeJS.ProcessEnv,
  key: string,
  value: string,
  platform: NodeJS.Platform,
): NodeJS.ProcessEnv => {
  const countKeys = Object.keys(env).filter((name) =>
    platform === "win32" ? name.toUpperCase() === "GIT_CONFIG_COUNT" : name === "GIT_CONFIG_COUNT",
  );
  const countKey = countKeys.at(-1) ?? "GIT_CONFIG_COUNT";
  const inherited = env[countKey];
  if (
    inherited !== undefined &&
    inherited !== "" &&
    // Match Git's strtoul count syntax, including whitespace, signs and non-negative -0.
    /^[ \t\r\n\v\f]*(?:\+?\d+|-0+)/.exec(inherited)?.[0] !== inherited
  ) {
    return {};
  }
  const count = inherited === undefined || inherited === "" ? 0 : Number.parseInt(inherited, 10);
  return {
    ...Object.fromEntries([...countKeys, countKey].map((name) => [name, String(count + 1)])),
    [`GIT_CONFIG_KEY_${count}`]: key,
    [`GIT_CONFIG_VALUE_${count}`]: value,
  };
};

/**
 * A project can contain a fake bare repository whose config runs commands such as `core.fsmonitor`.
 * Require `--git-dir` or `GIT_DIR` to open bare repositories; Git before 2.38 ignores this setting.
 */
export const explicitBareRepositoryEnv = (env: NodeJS.ProcessEnv, platform: NodeJS.Platform) =>
  gitConfigEnv(env, "safe.bareRepository", "explicit", platform);

const classifyNonZeroExit = (
  stderr: string,
  classifyFailure: VcsProcessInput["classifyFailure"],
): VcsProcessExitFailureKind => {
  const normalized = stderr.toLowerCase();
  const commandKind = classifyFailure?.(stderr) ?? null;

  if (
    commandKind === "authentication" ||
    normalized.includes("authentication failed") ||
    normalized.includes("not logged in") ||
    normalized.includes("no oauth token") ||
    normalized.includes("unauthorized")
  ) {
    return "authentication";
  }

  if (
    commandKind === "rate-limited" ||
    normalized.includes("api rate limit") ||
    normalized.includes("rate limit exceeded") ||
    normalized.includes("secondary rate limit") ||
    normalized.includes("too many requests") ||
    normalized.includes("http 429")
  ) {
    return "rate-limited";
  }

  return commandKind ?? "command-failed";
};

// Classify before discarding stderr; keep paths and process output out of errors.
const isTransientGitExit = (stderr: string) =>
  /unable to create [^\n]*\.lock['"]?: file exists/i.test(stderr) ||
  /(?:unable to stat|lstat\(|error: open\()[^\n]+: no such file or directory/i.test(stderr);

// On macOS, Git's fsync uses F_FULLFSYNC; writeout-only still uses fsync(2).
// Network mounts can reject F_FULLFSYNC. Ref writes omit errno, so retry their
// bare write error too; explicit unrelated errors remain failures.
// Linux writeout-only only starts writeback, so it must not use this fallback.
const FULL_FSYNC_ARG = "core.fsyncMethod=fsync";
const BATCH_FSYNC_ARG = "core.fsyncMethod=batch";
const WRITEOUT_ONLY_FSYNC_ARG = "core.fsyncMethod=writeout-only";
const canRetryFullFsync = (stderr: string) =>
  /fsync error on [^\r\n]+: operation not supported\r?$/im.test(stderr) ||
  /cannot update ref [^\r\n]+: couldn't write '[^\r\n]+\.lock'\r?$/im.test(stderr);

export const makeWith = Effect.fn("VcsProcess.makeWith")(function* (pools: VcsProcessPools) {
  const processRunner = yield* ProcessRunner.ProcessRunner;
  const hostPlatform = yield* HostProcess.Platform;
  const gitProcesses = yield* Semaphore.make(
    gitProcessConcurrency(
      pools.parallelism ?? NodeOS.availableParallelism(),
      pools.cpuQuota === undefined ? cgroupCpuQuota() : pools.cpuQuota,
    ),
  );
  const forgeProcesses = yield* Semaphore.make(FORGE_PROCESS_CONCURRENCY);
  const keyedProcesses = new Map<string, Semaphore.Semaphore>();
  const keyedPermit = (concurrency: NonNullable<VcsProcessInput["concurrency"]>) => {
    let semaphore = keyedProcesses.get(concurrency.key);
    if (semaphore === undefined) {
      semaphore = Semaphore.makeUnsafe(concurrency.limit);
      keyedProcesses.set(concurrency.key, semaphore);
    }
    return semaphore.withPermits(1);
  };

  const spawn = Effect.fn("VcsProcess.spawn")(function* (
    input: VcsProcessInput,
    baseError: VcsProcessErrorContext,
  ) {
    let failedExit = false;
    const execution = processRunner
      .run({
        command: input.command,
        args: input.args,
        cwd: input.cwd,
        ...(input.spawnCwd !== undefined ? { spawnCwd: input.spawnCwd } : {}),
        ...(input.stdin !== undefined ? { stdin: input.stdin } : {}),
        ...(input.onStdoutChunk !== undefined ? { onStdoutChunk: input.onStdoutChunk } : {}),
        env: {
          // Failures are read by English patterns; without LANG, macOS Git follows the system language.
          // LANGUAGE leaves LC_CTYPE, which hooks inherit, alone.
          ...(input.command === "git" ? { LANGUAGE: "C" } : {}),
          ...input.env,
          ...explicitBareRepositoryEnv({ ...process.env, ...input.env }, hostPlatform),
        },
        timeout: input.timeoutMs ?? DEFAULT_TIMEOUT_MS,
        maxOutputBytes: input.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES,
        outputMode: input.outputMode ?? "truncate",
        truncatedMarker: input.appendTruncationMarker ? OUTPUT_TRUNCATED_MARKER : "",
        timeoutBehavior: "error",
      })
      .pipe(
        Effect.tap((result) => {
          failedExit = result.code === null || (!input.allowNonZeroExit && result.code !== 0);
          return Effect.void;
        }),
        Effect.mapError(
          Match.valueTags({
            ProcessSpawnError: (error) =>
              VcsProcessSpawnError.fromProcessSpawnError(baseError, error),
            ProcessOutputLimitError: (error) =>
              new VcsProcessOutputLimitError({
                ...baseError,
                stream: error.stream,
                maxBytes: error.maxBytes,
                observedBytes: error.observedBytes,
              }),
            ProcessTimeoutError: (error) =>
              VcsProcessTimeoutError.fromProcessTimeoutError(baseError, error),
            ProcessStdinError: (error) =>
              new VcsProcessStdinWriteError({
                ...baseError,
                stdinBytes: error.stdinBytes,
                cause: error.cause,
              }),
            ProcessReadError: (error) =>
              new VcsProcessOutputReadError({
                ...baseError,
                stream: error.stream,
                cause: error.cause,
              }),
          }),
        ),
      );

    return yield* input.command === "git"
      ? execution.pipe(
          withMetrics({
            counter: gitCommandsTotal,
            timer: gitCommandDuration,
            attributes: {
              operation: gitMetricOperations.has(input.operation) ? input.operation : "other",
            },
            outcomeAttributes: (outcome) => ({
              outcome: outcome === "success" && failedExit ? "failure" : outcome,
            }),
          }),
        )
      : execution;
  });

  const runUnbounded = Effect.fn("VcsProcess.runUnbounded")(function* (input: VcsProcessInput) {
    const baseError = {
      operation: input.operation,
      command: input.command,
      cwd: input.cwd,
      argumentCount: input.args.length,
    };

    let result = yield* spawn(input, baseError);

    if (
      hostPlatform === "darwin" &&
      input.operation === CHECKPOINT_CAPTURE_OPERATION &&
      !input.allowNonZeroExit &&
      result.code !== null &&
      result.code !== 0 &&
      input.command === "git" &&
      (input.args.includes(FULL_FSYNC_ARG) || input.args.includes(BATCH_FSYNC_ARG)) &&
      canRetryFullFsync(result.stderr)
    ) {
      result = yield* spawn(
        {
          ...input,
          args: input.args.map((arg) =>
            arg === FULL_FSYNC_ARG || arg === BATCH_FSYNC_ARG ? WRITEOUT_ONLY_FSYNC_ARG : arg,
          ),
        },
        baseError,
      );
    }

    if (result.code === null) {
      return yield* new VcsProcessMissingExitCodeError(baseError);
    }

    if (!input.allowNonZeroExit && result.code !== 0) {
      const failureKind = classifyNonZeroExit(result.stderr, input.classifyFailure);
      const error = VcsProcessExitError.fromProcessExit(
        baseError,
        {
          exitCode: result.code,
          stderr: result.stderr,
          stderrTruncated: result.stderrTruncated,
        },
        failureKind,
        {
          retryable:
            input.command === "git" &&
            failureKind === "command-failed" &&
            isTransientGitExit(result.stderr),
          ...(input.notFoundDetail === undefined ? {} : { notFoundDetail: input.notFoundDetail }),
        },
      );
      if (
        input.command === "git" &&
        /^fatal: unable to create ['"][^\r\n]*[/\\]index\.lock['"]: file exists\.?$/im.test(
          result.stderr,
        )
      ) {
        return yield* new VcsProcessExitError({
          ...error,
          detail: GIT_INDEX_LOCKED_DETAIL,
        });
      }
      return yield* error;
    }

    return {
      exitCode: result.code,
      stdout: result.stdout,
      stderr: result.stderr,
      stdoutTruncated: result.stdoutTruncated,
      stderrTruncated: result.stderrTruncated,
      stdoutInvalidUtf8: result.stdoutInvalidUtf8 ?? false,
      stderrInvalidUtf8: result.stderrInvalidUtf8 ?? false,
    } satisfies VcsProcessOutput;
  });

  const run = Effect.fn("VcsProcess.run")(function* (input: VcsProcessInput) {
    // Each command waits only for its own pool: git's, sized by the machine, or the forge CLIs'.
    const pool =
      input.command === "git"
        ? { name: "git", processes: gitProcesses }
        : { name: "forge", processes: forgeProcesses };
    yield* Effect.annotateCurrentSpan("vcs.pool", pool.name);
    // Time waiting for a slot, keyed and pooled, summed over retries; a retry times only its own wait.
    let permitWaitMs = 0;
    let queuedAtMs: number | undefined = yield* Clock.currentTimeMillis;
    const bounded = Effect.gen(function* () {
      const attemptQueuedAtMs = queuedAtMs ?? (yield* Clock.currentTimeMillis);
      queuedAtMs = undefined;
      return yield* pool.processes.withPermits(1)(
        Effect.gen(function* () {
          permitWaitMs += (yield* Clock.currentTimeMillis) - attemptQueuedAtMs;
          yield* Effect.annotateCurrentSpan("vcs.permit_wait_ms", permitWaitMs);
          return yield* runUnbounded(input);
        }),
      );
    });
    if (
      input.command === "git" &&
      input.operation === CHECKPOINT_CAPTURE_OPERATION &&
      input.onStdoutChunk === undefined
    ) {
      return yield* bounded.pipe(
        Effect.tapError((error) =>
          Effect.logDebug("checkpoint Git command failed", {
            operation: input.operation,
            errorTag: error._tag,
          }),
        ),
        Effect.retry({
          times: 2,
          while: (error) => error._tag === "VcsProcessExitError" && error.retryable === true,
          schedule: Schedule.spaced(Duration.millis(75)),
        }),
      );
    }
    return yield* input.concurrency === undefined
      ? bounded
      : keyedPermit(input.concurrency)(bounded);
  });

  return VcsProcess.of({ run });
});

export const make = makeWith({});

// Fresh: a ProcessRunner provided around this one (a CLI's, a test's) may be built on Node's
// spawner, and would otherwise be reused here and bypass the spawner the knobs choose.
export const layer = Layer.effect(VcsProcess, make).pipe(
  Layer.provide(Layer.fresh(ProcessRunner.layer)),
  Layer.provide(WorkerChildProcessSpawner.layerFromConfig),
);

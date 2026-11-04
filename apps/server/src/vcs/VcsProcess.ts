import * as Clock from "effect/Clock";
// @effect-diagnostics nodeBuiltinImport:off - the git pool is sized from the machine (os) and its cgroup CPU quota (fs).
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";

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
// Git is I/O plus short CPU, so one process per core keeps up without oversubscribing the spawner's
// hand-off bound (max(2, cores)). The floor is upstream's constant (no slot wait at 26 commands/s on
// the bench); past the ceiling the disk, not the CPU, bounds concurrent index copies.
const GIT_PROCESS_CONCURRENCY_FLOOR = 8;
const GIT_PROCESS_CONCURRENCY_CEILING = 64;
// Forge CLIs (glab, gh, tea) are bounded for the remote's sake, not the machine's: each call is a
// network conversation of seconds, more at once gains nothing and risks the API's rate limit. They
// hold no git permit, so a forge call never delays a git command.
const FORGE_PROCESS_CONCURRENCY = 4;

/** Git processes allowed at once on a machine with `parallelism` cores and a cgroup CPU quota. */
export const gitProcessConcurrency = (parallelism: number, cpuQuota: number | null): number => {
  const cores = cpuQuota === null ? parallelism : Math.min(parallelism, cpuQuota);
  return Math.min(
    GIT_PROCESS_CONCURRENCY_CEILING,
    Math.max(GIT_PROCESS_CONCURRENCY_FLOOR, Math.floor(cores)),
  );
};

// The container's CPU quota in cores (cgroup v2 `cpu.max`, else v1 cfs files); null when unlimited.
export const cgroupCpuQuota = (): number | null => {
  const read = (file: string) => {
    try {
      return NodeFS.readFileSync(file, "utf8").trim();
    } catch {
      return null;
    }
  };
  const v2 = read("/sys/fs/cgroup/cpu.max");
  if (v2 !== null) {
    const [quota, period] = v2.split(/\s+/);
    if (quota === undefined || quota === "max") return null;
    const cores = Number(quota) / Number(period ?? "100000");
    return Number.isFinite(cores) && cores > 0 ? Math.ceil(cores) : null;
  }
  const quota = Number(read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") ?? "-1");
  const period = Number(read("/sys/fs/cgroup/cpu/cpu.cfs_period_us") ?? "100000");
  if (!(quota > 0) || !(period > 0)) return null;
  return Math.ceil(quota / period);
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

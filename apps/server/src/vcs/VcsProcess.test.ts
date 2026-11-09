import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as Tracer from "effect/Tracer";
import { ChildProcessSpawner } from "effect/process";

import { VcsProcessExitError, VcsProcessSpawnError } from "@t3tools/contracts";
import * as ProcessRunner from "../processRunner.ts";
import * as VcsProcess from "./VcsProcess.ts";

const encodeExitError = Schema.encodeEffect(Schema.fromJsonString(VcsProcessExitError));

const run = (input: VcsProcess.VcsProcessInput) =>
  Effect.gen(function* () {
    const process = yield* VcsProcess.VcsProcess;
    return yield* process.run(input);
  });

const layerLive = VcsProcess.layer.pipe(Layer.provide(NodeServices.layer));

const provideLive = <A, E, R>(effect: Effect.Effect<A, E, R | VcsProcess.VcsProcess>) =>
  effect.pipe(Effect.provide(layerLive));

const baseInput = {
  operation: "test.process-boundary",
  command: "git",
  args: ["status", "--short"],
  cwd: "/workspace",
} satisfies VcsProcess.VcsProcessInput;

const captureProcessResult = (
  result: Effect.Effect<ProcessRunner.ProcessRunOutput, ProcessRunner.ProcessRunError>,
) =>
  VcsProcess.make.pipe(
    Effect.provideService(
      ProcessRunner.ProcessRunner,
      ProcessRunner.ProcessRunner.of({ run: () => result }),
    ),
    Effect.flatMap((service) => service.run(baseInput)),
    Effect.flip,
  );

describe("VcsProcess.run", () => {
  it.effect("explains a locked Git index and allows restore after the lock is released", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-index-lock-" });
      const git = (args: readonly string[]) =>
        run({ operation: "test.checkpoint.restore", command: "git", cwd, args });
      yield* git(["init"]);
      const file = path.join(cwd, "README.md");
      yield* fs.writeFileString(file, "before\n");
      yield* git(["add", "README.md"]);
      yield* git([
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "-m",
        "initial",
      ]);
      yield* fs.writeFileString(file, "after\n");
      const lock = path.join(cwd, ".git", "index.lock");
      yield* fs.writeFileString(lock, "held by another process\n");
      const restore = ["restore", "--source=HEAD", "--worktree", "--staged", "--", "README.md"];

      const error = yield* Effect.flip(git(restore));
      expect(error).toBeInstanceOf(VcsProcessExitError);
      expect(error).toMatchObject({
        exitCode: 128,
        failureKind: "command-failed",
        detail: "Git's index is locked. Wait for other Git operations to finish, then try again.",
      });
      expect(error.message).not.toContain("index.lock");
      expect(yield* fs.readFileString(file)).toBe("after\n");
      expect(yield* fs.readFileString(lock)).toBe("held by another process\n");

      yield* fs.remove(lock);
      yield* git(restore);
      expect(yield* fs.readFileString(file)).toBe("before\n");
    }).pipe(provideLive, Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect.each([
    {
      stderr: "fatal: Unable to create '/private/repo/index.lock': File exists",
      retryable: true,
      detail: "Git's index is locked. Wait for other Git operations to finish, then try again.",
    },
    {
      stderr: "fatal: Unable to create 'C:\\private\\repo\\.git\\index.lock': File exists.",
      retryable: true,
      detail: "Git's index is locked. Wait for other Git operations to finish, then try again.",
    },
    {
      stderr: "fatal: Unable to create '/private/repo/.git/config.lock': File exists.",
      retryable: true,
    },
    {
      stderr: "fatal: Unable to create '/private/repo/index.lock': Permission denied",
      retryable: false,
    },
    { stderr: 'error: open("/private/repo/file"): No such file or directory', retryable: true },
    {
      stderr: "fatal: unable to stat '/private/repo/file': No such file or directory",
      retryable: true,
    },
    { stderr: 'error: open("/private/repo/file"): Permission denied', retryable: false },
    {
      stderr: "error: '/private/repo/child/' does not have a commit checked out",
      retryable: false,
    },
    { stderr: "fatal: index file corrupt", retryable: false },
  ])(
    "classifies Git exit retryability and safe detail without retaining stderr: $stderr",
    ({ stderr, retryable, detail = "Process exited with a non-zero status." }) =>
      Effect.gen(function* () {
        const error = yield* captureProcessResult(
          Effect.succeed({
            stdout: "",
            stderr,
            code: ChildProcessSpawner.ExitCode(128),
            timedOut: false,
            stdoutTruncated: false,
            stderrTruncated: false,
            stdoutInvalidUtf8: false,
            stderrInvalidUtf8: false,
          }),
        );
        assert.instanceOf(error, VcsProcessExitError);
        expect(error.retryable === true).toBe(retryable);
        expect(error.detail).toBe(detail);
        const encoded = yield* encodeExitError(error);
        expect(encoded).not.toContain("private");
      }),
  );

  // Without LANG, macOS Git follows the system language, as in an app launched from Finder.
  it.effect("reads Git failures in English when Git would follow the system language", () =>
    Effect.gen(function* () {
      let attempts = 0;
      const service = yield* VcsProcess.make.pipe(
        Effect.provideService(
          ProcessRunner.ProcessRunner,
          ProcessRunner.ProcessRunner.of({
            run: (input) =>
              Effect.sync(() => {
                attempts += 1;
                return {
                  stdout: "",
                  stderr:
                    attempts % 2 === 0
                      ? ""
                      : input.env?.LANGUAGE === "C"
                        ? "fatal: Unable to create '/private/repo/.git/index.lock': File exists.\n"
                        : "致命錯誤: 無法建立「/private/repo/.git/index.lock」：File exists。\n",
                  code: ChildProcessSpawner.ExitCode(attempts % 2 === 0 ? 0 : 128),
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                };
              }),
          }),
        ),
      );

      const capture = yield* service
        .run({
          ...baseInput,
          operation: VcsProcess.CHECKPOINT_CAPTURE_OPERATION,
          args: ["add", "--all"],
        })
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* TestClock.adjust("1 second");
      expect(Exit.isSuccess(yield* Fiber.join(capture))).toBe(true);
      expect(attempts).toBe(2);

      attempts = 0;
      const error = yield* Effect.flip(service.run(baseInput));
      assert.instanceOf(error, VcsProcessExitError);
      expect(error.detail).toBe(VcsProcess.GIT_INDEX_LOCKED_DETAIL);
    }),
  );

  it.effect.each([
    {
      stderr: "fatal: fsync error on '.git/objects/a5/tmp_obj_Pseg66': Operation not supported",
      fsyncMethods: ["fsync", "writeout-only"],
    },
    {
      stderr:
        "fatal: update_ref failed for ref 'refs/t3/checkpoints/x': cannot update ref 'refs/t3/checkpoints/x': couldn't write '/Volumes/share/.git/refs/t3/checkpoints/x.lock'",
      fsyncMethods: ["fsync", "writeout-only"],
    },
    { stderr: "fatal: Operation not supported", fsyncMethods: ["fsync"] },
    {
      stderr:
        "fatal: update_ref failed for ref 'refs/t3/checkpoints/x': cannot update ref 'refs/t3/checkpoints/x': couldn't write '/Volumes/share/.git/refs/t3/checkpoints/x.lock': Input/output error",
      fsyncMethods: ["fsync"],
    },
    {
      stderr: "fatal: fsync error on '.git/objects/a5/tmp_obj_Pseg66': Operation not supported",
      fsyncMethods: ["fsync", "writeout-only"],
      fallbackFails: true,
    },
    {
      platform: "linux",
      stderr: "fatal: fsync error on '.git/objects/a5/tmp_obj_Pseg66': Operation not supported",
      fsyncMethods: ["fsync"],
    },
    {
      operation: baseInput.operation,
      stderr: "fatal: fsync error on '.git/objects/a5/tmp_obj_Pseg66': Operation not supported",
      fsyncMethods: ["fsync"],
    },
    {
      stderr: "fatal: fsync error on '.git/objects/bulk_fsync_Pseg66': Operation not supported",
      fsyncMethods: ["batch", "writeout-only"],
    },
    {
      stderr:
        "fatal: update_ref failed for ref 'refs/t3/checkpoints/x': cannot update ref 'refs/t3/checkpoints/x': couldn't write '/Volumes/share/.git/refs/t3/checkpoints/x.lock'",
      fsyncMethods: ["batch", "writeout-only"],
    },
  ] as const)(
    "falls back to writeout-only only for unsupported flushes on macOS ($fsyncMethods): $stderr",
    ({ stderr, fsyncMethods, ...scenario }) =>
      Effect.gen(function* () {
        const seen: Array<ReadonlyArray<string>> = [];
        const service = yield* VcsProcess.make.pipe(
          Effect.provideService(
            HostProcess.Platform,
            "platform" in scenario ? scenario.platform : "darwin",
          ),
          Effect.provideService(ProcessRunner.ProcessRunner, {
            run: (input) =>
              Effect.sync(() => {
                seen.push(input.args);
                const failed =
                  input.args.includes(`core.fsyncMethod=${fsyncMethods[0]}`) ||
                  ("fallbackFails" in scenario && scenario.fallbackFails);
                return {
                  stdout: "",
                  stderr: failed ? stderr : "",
                  code: ChildProcessSpawner.ExitCode(failed ? 128 : 0),
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                };
              }),
          }),
        );
        const exit = yield* service
          .run({
            ...baseInput,
            operation:
              "operation" in scenario
                ? scenario.operation
                : VcsProcess.CHECKPOINT_CAPTURE_OPERATION,
            args: [
              "-c",
              "core.fsync=objects,reference",
              "-c",
              ...(fsyncMethods[0] === "batch" ? ["core.fsyncMethod=fsync", "-c"] : []),
              `core.fsyncMethod=${fsyncMethods[0]}`,
              "add",
            ],
          })
          .pipe(Effect.exit);
        expect(seen).toEqual(
          fsyncMethods.map((method) => [
            "-c",
            "core.fsync=objects,reference",
            "-c",
            ...(fsyncMethods[0] === "batch"
              ? [`core.fsyncMethod=${method === "batch" ? "fsync" : method}`, "-c"]
              : []),
            `core.fsyncMethod=${method}`,
            "add",
          ]),
        );
        expect(Exit.isSuccess(exit)).toBe(
          fsyncMethods.length === 2 && !("fallbackFails" in scenario && scenario.fallbackFails),
        );
        if (Exit.isFailure(exit)) {
          expect(Cause.findErrorOption(exit.cause)).toMatchObject({
            _tag: "Some",
            value: { _tag: "VcsProcessExitError", exitCode: 128 },
          });
        }
      }),
  );

  it.effect("bounds a burst of processes that share a concurrency key", () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const starts = yield* Queue.unbounded<number>();
      const active = yield* Ref.make(0);
      const peak = yield* Ref.make(0);
      const total = yield* Ref.make(0);
      const service = yield* VcsProcess.make.pipe(
        Effect.provideService(
          ProcessRunner.ProcessRunner,
          ProcessRunner.ProcessRunner.of({
            run: () =>
              Effect.gen(function* () {
                const count = yield* Ref.updateAndGet(active, (held) => held + 1);
                yield* Ref.update(peak, (held) => Math.max(held, count));
                yield* Ref.update(total, (held) => held + 1);
                yield* Queue.offer(starts, count);
                yield* Deferred.await(gate);
                return {
                  stdout: "",
                  stderr: "",
                  code: ChildProcessSpawner.ExitCode(0),
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                };
              }).pipe(Effect.ensuring(Ref.update(active, (count) => count - 1))),
          }),
        ),
      );

      const burst = yield* Effect.all(
        Array.from({ length: 32 }, (_, index) =>
          service.run({
            operation: `synthetic.cli.${index}`,
            command: "host-cli",
            args: ["api", "user"],
            cwd: "/workspace",
            concurrency: { key: "host-cli", limit: 4 },
          }),
        ),
        { concurrency: "unbounded" },
      ).pipe(Effect.forkChild);

      yield* Effect.all(Array.from({ length: 4 }, () => Queue.take(starts)));
      yield* Effect.yieldNow;
      expect(yield* Queue.size(starts)).toBe(0);
      expect(yield* Ref.get(peak)).toBe(4);

      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.join(burst);
      expect(yield* Ref.get(total)).toBe(32);
      expect(yield* Ref.get(peak)).toBe(4);
    }),
  );

  it.effect("records only the wait for a process slot, not an earlier attempt's run time", () =>
    Effect.gen(function* () {
      const spans: Array<Tracer.NativeSpan> = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      const firstStarted = yield* Deferred.make<void>();
      const firstMayFinish = yield* Deferred.make<void>();
      let attempts = 0;
      const output = {
        stdout: "",
        timedOut: false,
        stdoutTruncated: false,
        stderrTruncated: false,
        stdoutInvalidUtf8: false,
        stderrInvalidUtf8: false,
      };
      const service = yield* VcsProcess.make.pipe(
        Effect.provideService(ProcessRunner.ProcessRunner, {
          run: () =>
            Effect.gen(function* () {
              attempts += 1;
              if (attempts > 1) {
                return { ...output, code: ChildProcessSpawner.ExitCode(0), stderr: "" };
              }
              yield* Deferred.succeed(firstStarted, undefined);
              yield* Deferred.await(firstMayFinish);
              return {
                ...output,
                code: ChildProcessSpawner.ExitCode(128),
                stderr:
                  "fatal: Unable to create '/workspace/.git/refs/heads/main.lock': File exists.",
              };
            }),
        }),
      );

      const capture = yield* service
        .run({
          operation: VcsProcess.CHECKPOINT_CAPTURE_OPERATION,
          command: "git",
          args: ["commit", "--quiet", "-m", "checkpoint"],
          cwd: "/workspace",
        })
        .pipe(Effect.withTracer(tracer), Effect.forkChild);
      // The first attempt runs for ten seconds, fails with a transient lock error and is retried
      // after the 75 ms backoff; the slot was free both times.
      yield* Deferred.await(firstStarted);
      yield* TestClock.adjust("10 seconds");
      yield* Deferred.succeed(firstMayFinish, undefined);
      yield* TestClock.adjust("75 millis");
      const result = yield* Fiber.join(capture);

      expect(result.exitCode).toBe(0);
      expect(attempts).toBe(2);
      const runSpan = spans.find((span) => span.name === "VcsProcess.run");
      expect(runSpan?.attributes.get("vcs.permit_wait_ms")).toBe(0);
    }),
  );

  it.effect("leaves git a slot while slow forge CLI processes are running", () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const service = yield* VcsProcess.make.pipe(
        Effect.provideService(
          ProcessRunner.ProcessRunner,
          ProcessRunner.ProcessRunner.of({
            run: (input) =>
              Effect.gen(function* () {
                if (input.command !== "git") yield* Deferred.await(gate);
                return {
                  stdout: "",
                  stderr: "",
                  code: ChildProcessSpawner.ExitCode(0),
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                };
              }),
          }),
        ),
      );

      const forgeRuns = yield* Effect.forEach(Array.from({ length: 12 }), () =>
        service
          .run({
            operation: "synthetic.forge.listChangeRequests",
            command: "tea",
            args: ["pulls", "list"],
            cwd: "/workspace",
          })
          .pipe(Effect.forkChild({ startImmediately: true })),
      );
      const checkpoint = yield* service
        .run({
          ...baseInput,
          operation: VcsProcess.CHECKPOINT_CAPTURE_OPERATION,
          args: ["rev-parse", "HEAD"],
        })
        .pipe(Effect.timeoutOption("1 second"), Effect.forkChild({ startImmediately: true }));
      yield* TestClock.adjust("1 second");

      const gitResult = yield* Fiber.join(checkpoint);
      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.joinAll(forgeRuns);
      expect(
        Option.isSome(gitResult),
        "git waited behind forge CLI runs that held every VCS process slot",
      ).toBe(true);
    }),
  );

  it.effect("a slow forge call never delays a git command", () =>
    Effect.gen(function* () {
      const forgeGate = yield* Deferred.make<void>();
      const gitGate = yield* Deferred.make<void>();
      const gitStarts = yield* Queue.unbounded<void>();
      const forgeStarts = yield* Queue.unbounded<void>();
      // A small machine: git's pool is the floor of 8, so forge calls holding git permits would show.
      const service = yield* VcsProcess.makeWith({ parallelism: 8, cpuQuota: null }).pipe(
        Effect.provideService(
          ProcessRunner.ProcessRunner,
          ProcessRunner.ProcessRunner.of({
            run: (input) =>
              Effect.gen(function* () {
                if (input.command === "git") {
                  yield* Queue.offer(gitStarts, undefined);
                  yield* Deferred.await(gitGate);
                } else {
                  yield* Queue.offer(forgeStarts, undefined);
                  yield* Deferred.await(forgeGate);
                }
                return {
                  stdout: "",
                  stderr: "",
                  code: ChildProcessSpawner.ExitCode(0),
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                };
              }),
          }),
        ),
      );

      // Forge calls fill their own pool and stay there (a merge-request detail can take 30 s).
      const forgeRuns = yield* Effect.forEach(Array.from({ length: 12 }), () =>
        service
          .run({
            operation: "GitLabPullRequestCli.getMergeRequestDetail",
            command: "glab",
            args: ["mr", "view", "1"],
            cwd: "/workspace",
          })
          .pipe(Effect.forkChild({ startImmediately: true })),
      );
      // The forge pool is full (its four permits taken) before any git command is asked for.
      yield* Effect.all(Array.from({ length: 4 }, () => Queue.take(forgeStarts)));
      // Meanwhile eight checkpoint commands must all start: forge calls hold no git permit.
      const gitRuns = yield* Effect.forEach(Array.from({ length: 8 }), () =>
        service
          .run({
            ...baseInput,
            operation: VcsProcess.CHECKPOINT_CAPTURE_OPERATION,
            args: ["rev-parse", "HEAD"],
          })
          .pipe(Effect.forkChild({ startImmediately: true })),
      );
      const allStarted = yield* Effect.all(
        Array.from({ length: 8 }, () => Queue.take(gitStarts)),
      ).pipe(Effect.timeoutOption("1 second"), Effect.forkChild({ startImmediately: true }));
      yield* TestClock.adjust("1 second");
      const started = yield* Fiber.join(allStarted);

      yield* Deferred.succeed(gitGate, undefined);
      yield* Deferred.succeed(forgeGate, undefined);
      yield* Fiber.joinAll([...gitRuns, ...forgeRuns]);
      expect(Option.isSome(started), "git commands waited behind forge CLI runs").toBe(true);
    }),
  );

  it.effect("git concurrency follows the machine's parallelism", () =>
    Effect.gen(function* () {
      // A quarter of the cores, between 8 and 32: 8 on small machines, 16 on a 64-core host.
      expect(VcsProcess.gitProcessConcurrency(2, null)).toBe(8);
      expect(VcsProcess.gitProcessConcurrency(16, null)).toBe(8);
      expect(VcsProcess.gitProcessConcurrency(64, null)).toBe(16);
      expect(VcsProcess.gitProcessConcurrency(128, null)).toBe(32);
      expect(VcsProcess.gitProcessConcurrency(256, null)).toBe(32);
      expect(VcsProcess.gitProcessConcurrency(64, 48)).toBe(12);
      expect(VcsProcess.gitProcessConcurrency(64, 3)).toBe(8);

      const gate = yield* Deferred.make<void>();
      const starts = yield* Queue.unbounded<void>();
      const active = yield* Ref.make(0);
      const peak = yield* Ref.make(0);
      const service = yield* VcsProcess.makeWith({ parallelism: 64, cpuQuota: null }).pipe(
        Effect.provideService(
          ProcessRunner.ProcessRunner,
          ProcessRunner.ProcessRunner.of({
            run: () =>
              Effect.gen(function* () {
                const count = yield* Ref.updateAndGet(active, (held) => held + 1);
                yield* Ref.update(peak, (held) => Math.max(held, count));
                yield* Queue.offer(starts, undefined);
                yield* Deferred.await(gate);
                return {
                  stdout: "",
                  stderr: "",
                  code: ChildProcessSpawner.ExitCode(0),
                  timedOut: false,
                  stdoutTruncated: false,
                  stderrTruncated: false,
                  stdoutInvalidUtf8: false,
                  stderrInvalidUtf8: false,
                };
              }).pipe(Effect.ensuring(Ref.update(active, (count) => count - 1))),
          }),
        ),
      );
      const burst = yield* Effect.all(
        Array.from({ length: 40 }, () => service.run(baseInput)),
        { concurrency: "unbounded" },
      ).pipe(Effect.forkChild);
      yield* Effect.all(Array.from({ length: 16 }, () => Queue.take(starts)));
      yield* Effect.yieldNow;
      expect(yield* Queue.size(starts)).toBe(0);
      expect(yield* Ref.get(peak)).toBe(16);
      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.join(burst);
      expect(yield* Ref.get(peak)).toBe(16);
    }),
  );

  it.effect("reads the CPU quota of the process's own cgroup, and the tightest ancestor's", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-cgroup-" });
      const write = (relative: string, content: string) =>
        fileSystem
          .makeDirectory(path.dirname(path.join(root, relative)), { recursive: true })
          .pipe(Effect.andThen(fileSystem.writeFileString(path.join(root, relative), content)));
      // The mounts are discovered, not assumed: cgroup2 here sits at a hybrid layout's `unified`,
      // and the v1 cpu controller's mount is named in another order than the membership lists.
      const mounts = [
        `36 35 0:30 / ${path.join(root, "sys", "unified")} rw,nosuid - cgroup2 cgroup2 rw`,
        `37 35 0:31 / ${path.join(root, "sys", "cpu,cpuacct")} rw,nosuid - cgroup cgroup rw,cpu,cpuacct`,
        `38 35 0:32 / ${path.join(root, "sys", "memory")} rw,nosuid - cgroup cgroup rw,memory`,
      ].join("\n");
      yield* write("mountinfo", `${mounts}\n`);
      const quota = (selfCgroup: string) =>
        VcsProcess.cgroupCpuQuota({
          mountinfo: path.join(root, "mountinfo"),
          selfCgroup: path.join(root, selfCgroup),
        });

      // cgroup v2: a systemd service's quota sits in its own cgroup, the root has no cpu.max.
      yield* write("sys/unified/system.slice/cpu.max", "max 100000\n");
      yield* write("sys/unified/system.slice/t3.service/cpu.max", "1200000 100000\n");
      yield* write("proc-v2", "0::/system.slice/t3.service\n");
      expect(quota("proc-v2")).toBe(12);
      // The tightest quota along the path applies.
      yield* write("sys/unified/system.slice/cpu.max", "400000 100000\n");
      expect(quota("proc-v2")).toBe(4);
      // No quota anywhere on the path: unlimited.
      yield* write("proc-root", "0::/\n");
      expect(quota("proc-root")).toBeNull();
      // cgroup v1 through the combined cpu,cpuacct controller mount, listed as cpuacct,cpu.
      yield* write("sys/cpu,cpuacct/docker/abc/cpu.cfs_quota_us", "350000\n");
      yield* write("sys/cpu,cpuacct/docker/abc/cpu.cfs_period_us", "100000\n");
      yield* write("proc-v1", "3:cpuacct,cpu:/docker/abc\n2:memory:/docker/abc\n");
      expect(quota("proc-v1")).toBe(3.5);
      // A fractional quota is not rounded before sizing: 35.5 cores is still eight permits.
      yield* write("sys/unified/system.slice/t3.service/cpu.max", "3550000 100000\n");
      yield* write("sys/unified/system.slice/cpu.max", "max 100000\n");
      expect(quota("proc-v2")).toBe(35.5);
      expect(VcsProcess.gitProcessConcurrency(64, 35.5)).toBe(8);
      // A cgroup path may itself contain a colon: everything after the second one is the path.
      yield* write("sys/unified/system.slice/t3:colon.service/cpu.max", "600000 100000\n");
      yield* write("proc-colon", "0::/system.slice/t3:colon.service\n");
      expect(quota("proc-colon")).toBe(6);
      // A process outside its cgroup namespace's root reports `..` segments: no limit is read from
      // there, and nothing above the mount is ever read.
      yield* write("sys/cpu.max", "100000 100000\n");
      yield* write("proc-outside", "0::/../../outside\n");
      expect(quota("proc-outside")).toBeNull();
      // A mount whose root is a subtree (a container's cgroup mounted at /sys/fs/cgroup): the
      // membership path is relative to the hierarchy, the files sit under the mount minus that root.
      const tenant = [
        `40 35 0:30 /tenant ${path.join(root, "sys", "tenant-unified")} rw,nosuid - cgroup2 cgroup2 rw`,
        `41 35 0:31 /tenant ${path.join(root, "sys", "tenant-cpu")} rw,nosuid - cgroup cgroup rw,cpu,cpuacct`,
      ].join("\n");
      yield* write("mountinfo-tenant", `${tenant}\n`);
      yield* write("sys/tenant-unified/t3.service/cpu.max", "1200000 100000\n");
      yield* write("sys/tenant-unified/tenant/t3.service/cpu.max", "max 100000\n");
      yield* write("proc-tenant", "0::/tenant/t3.service\n");
      expect(
        VcsProcess.cgroupCpuQuota({
          mountinfo: path.join(root, "mountinfo-tenant"),
          selfCgroup: path.join(root, "proc-tenant"),
        }),
      ).toBe(12);
      yield* write("sys/tenant-cpu/t3.service/cpu.cfs_quota_us", "500000\n");
      yield* write("sys/tenant-cpu/t3.service/cpu.cfs_period_us", "100000\n");
      yield* write("proc-tenant-v1", "3:cpu,cpuacct:/tenant/t3.service\n");
      expect(
        VcsProcess.cgroupCpuQuota({
          mountinfo: path.join(root, "mountinfo-tenant"),
          selfCgroup: path.join(root, "proc-tenant-v1"),
        }),
      ).toBe(5);
      // A membership outside the mount's root subtree is not under this mount at all.
      yield* write("proc-elsewhere", "0::/other/t3.service\n");
      expect(
        VcsProcess.cgroupCpuQuota({
          mountinfo: path.join(root, "mountinfo-tenant"),
          selfCgroup: path.join(root, "proc-elsewhere"),
        }),
      ).toBeNull();
      // A nested quota bounds the git pool: 128 cores under a 12-core quota is the floor of 8.
      expect(VcsProcess.gitProcessConcurrency(128, 12)).toBe(8);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("names the pool a command waited for", () =>
    Effect.gen(function* () {
      const spans: Array<Tracer.NativeSpan> = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });
      const service = yield* VcsProcess.make.pipe(
        Effect.provideService(ProcessRunner.ProcessRunner, {
          run: () =>
            Effect.succeed({
              stdout: "",
              stderr: "",
              code: ChildProcessSpawner.ExitCode(0),
              timedOut: false,
              stdoutTruncated: false,
              stderrTruncated: false,
              stdoutInvalidUtf8: false,
              stderrInvalidUtf8: false,
            }),
        }),
      );
      yield* service.run(baseInput).pipe(Effect.withTracer(tracer));
      yield* service
        .run({ operation: "forge.test", command: "glab", args: ["mr", "list"], cwd: "/workspace" })
        .pipe(Effect.withTracer(tracer));
      const pools = spans
        .filter((span) => span.name === "VcsProcess.run")
        .map((span) => span.attributes.get("vcs.pool"));
      expect(pools).toEqual(["git", "forge"]);
    }),
  );

  it.effect("keeps git from opening a folder laid out as a bare repository", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-vcs-bare-" });
      yield* fileSystem.makeDirectory(path.join(cwd, "objects", "info"), { recursive: true });
      yield* fileSystem.makeDirectory(path.join(cwd, "refs", "heads"), { recursive: true });
      yield* fileSystem.writeFileString(path.join(cwd, "HEAD"), "ref: refs/heads/main\n");
      yield* fileSystem.writeFileString(
        path.join(cwd, "config"),
        '[core]\n\trepositoryformatversion = 0\n\tbare = false\n\tworktree = .\n\tfsmonitor = "echo ran >> fsmonitor-runs; false"\n',
      );

      const result = yield* run({
        operation: "test.bare-repository",
        command: "git",
        args: ["status"],
        cwd,
        allowNonZeroExit: true,
      });

      expect(result.exitCode).not.toBe(0);
      expect(yield* fileSystem.exists(path.join(cwd, "fsmonitor-runs"))).toBe(false);
    }).pipe(Effect.scoped, provideLive, Effect.provide(NodeServices.layer)),
  );

  it.effect("classifies authentication failures without retaining stderr", () =>
    Effect.gen(function* () {
      const secretStderr = "authentication failed for token super-secret-token";
      const error = yield* run({
        operation: "test.authentication",
        command: "node",
        args: ["-e", "process.stderr.write(process.argv[1]); process.exit(1)", secretStderr],
        cwd: process.cwd(),
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(VcsProcessExitError);
      expect(error).toMatchObject({
        operation: "test.authentication",
        command: "node",
        exitCode: 1,
        detail: "Authentication failed.",
        failureKind: "authentication",
        stderrLength: secretStderr.length,
        stderrTruncated: false,
      });
      expect(error.message).not.toContain(secretStderr);
      expect(error.message).not.toContain("super-secret-token");
    }).pipe(provideLive),
  );

  it.effect("classifies API rate limits without retaining provider stderr", () =>
    Effect.gen(function* () {
      const providerStderr =
        "GraphQL: API rate limit already exceeded for user ID 51714798 and token secret-value.";
      const error = yield* run({
        operation: "test.rate-limit",
        command: "node",
        args: ["-e", "process.stderr.write(process.argv[1]); process.exit(1)", providerStderr],
        cwd: process.cwd(),
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(VcsProcessExitError);
      expect(error).toMatchObject({
        command: "node",
        exitCode: 1,
        detail: "API rate limit exceeded.",
        failureKind: "rate-limited",
        stderrLength: providerStderr.length,
        stderrTruncated: false,
      });
      expect(error.message).not.toContain(providerStderr);
      expect(error.message).not.toContain("secret-value");
    }).pipe(provideLive),
  );

  it.effect("reads a failure with the command's own classifier and names it in its terms", () =>
    Effect.gen(function* () {
      const fail = (input: Partial<VcsProcess.VcsProcessInput>) =>
        run({
          operation: "test.classify",
          command: "node",
          args: ["-e", "process.stderr.write('HTTP 404: 404 Not Found'); process.exit(1)"],
          cwd: process.cwd(),
          ...input,
        }).pipe(Effect.flip);

      expect(yield* fail({})).toMatchObject({
        failureKind: "command-failed",
        detail: "Process exited with a non-zero status.",
      });
      expect(
        yield* fail({
          classifyFailure: (stderr) => (stderr.includes("404") ? "not-found" : null),
          notFoundDetail: "Merge request not found.",
        }),
      ).toMatchObject({ failureKind: "not-found", detail: "Merge request not found." });
    }).pipe(provideLive),
  );

  it.effect("retains spawn causes without exposing process arguments in the error message", () =>
    Effect.gen(function* () {
      const secretArgument = "--token=super-secret-token";
      const error = yield* run({
        operation: "test.spawn",
        command: "definitely-not-a-t3code-executable",
        args: [secretArgument],
        cwd: process.cwd(),
      }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(VcsProcessSpawnError);
      expect(error).toMatchObject({
        operation: "test.spawn",
        command: "definitely-not-a-t3code-executable",
        argumentCount: 1,
      });
      expect(error).toHaveProperty("cause");
      expect(error.message).not.toContain(secretArgument);
    }).pipe(provideLive),
  );

  it.effect("truncates output and appends the marker when requested", () =>
    Effect.gen(function* () {
      const result = yield* run({
        operation: "test.truncate-marker",
        command: "node",
        args: ["-e", "process.stdout.write('x'.repeat(2048))"],
        cwd: process.cwd(),
        maxOutputBytes: 128,
        appendTruncationMarker: true,
      });

      expect(result.stdoutTruncated).toBe(true);
      expect(result.stdout).toContain("[truncated]");
      expect(result.stderrTruncated).toBe(false);
    }).pipe(provideLive),
  );

  it.effect("fails with measured byte counts when output must not be truncated", () =>
    Effect.gen(function* () {
      const error = yield* run({
        operation: "test.output-limit",
        command: "node",
        args: ["-e", "process.stdout.write('x'.repeat(2048))"],
        cwd: yield* HostProcess.WorkingDirectory,
        maxOutputBytes: 128,
        outputMode: "error",
      }).pipe(Effect.flip);

      assert(error._tag === "VcsProcessOutputLimitError");
      expect(error.stream).toBe("stdout");
      expect(error.maxBytes).toBe(128);
      expect(error.observedBytes).toBeGreaterThan(error.maxBytes);
    }).pipe(provideLive),
  );

  it.effect("streams all stdout bytes beyond the buffered output cap", () =>
    Effect.gen(function* () {
      const chunks: Uint8Array[] = [];
      const result = yield* run({
        operation: "test.stream-output",
        command: "node",
        args: ["-e", "process.stdout.write('x'.repeat(131072) + '\\0S final\\0')"],
        cwd: process.cwd(),
        maxOutputBytes: 8,
        onStdoutChunk: (chunk) => chunks.push(chunk),
      });

      expect(result.stdout).toBe("xxxxxxxx");
      expect(result.stdoutTruncated).toBe(true);
      expect(Buffer.concat(chunks).toString()).toBe("x".repeat(131072) + "\0S final\0");
    }).pipe(provideLive),
  );
});

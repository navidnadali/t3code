import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/process";
import * as Metric from "effect/Metric";
import { assert, it } from "@effect/vitest";

import { CheckpointRef, GitCommandError, VcsProcessExitError } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import * as ServerConfig from "../config.ts";
import { gitCommandDuration, gitCommandsTotal } from "../observability/Metrics.ts";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as GitVcsDriver from "./GitVcsDriver.ts";
import {
  makeCheckpointRefMemory,
  parseCommitObject,
  parsePackedRefs,
} from "./gitCheckpointRefs.ts";
import { gitConfigBool, parseGitConfigList } from "./gitConfigSnapshot.ts";
import type * as VcsDriver from "./VcsDriver.ts";
import * as VcsDriverRegistry from "./VcsDriverRegistry.ts";
import * as VcsProcess from "./VcsProcess.ts";
import { runVcsDriverContractSuite } from "./testing/VcsDriverContractHarness.ts";

const layerServerConfig = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-git-vcs-contract-",
});
const layerGitContract = Layer.mergeAll(GitVcsDriver.layerVcs, GitVcsDriver.layer).pipe(
  Layer.provide(layerServerConfig),
  Layer.provideMerge(VcsProcess.layer),
  Layer.provideMerge(NodeServices.layer),
);
const layerGitCaptureContract = Layer.merge(
  layerGitContract,
  ProcessRunner.layer.pipe(Layer.provide(NodeServices.layer)),
);

const runGit = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const driver = yield* GitVcsDriver.GitVcsDriver;
    yield* driver.execute({
      operation: "GitVcsDriver.contract.git",
      cwd,
      args,
      timeoutMs: 10_000,
    });
  });

const makeCaptureStore = Effect.fn("test.makeCaptureStore")(function* (
  driver: VcsDriver.VcsDriver["Service"],
  cwd: string,
) {
  const repository = yield* driver.detectRepository(cwd);
  if (repository === null) return yield* Effect.die("Expected a test Git repository");
  const handle = { kind: repository.kind, repository, driver };
  return yield* CheckpointStore.make.pipe(
    Effect.provideService(VcsDriverRegistry.VcsDriverRegistry, {
      get: () => Effect.succeed(driver),
      detect: () => Effect.succeed(handle),
      resolve: () => Effect.succeed(handle),
    }),
  );
});

type GitContractError = GitCommandError | PlatformError.PlatformError;

runVcsDriverContractSuite<GitVcsDriver.GitVcsDriver, GitContractError>({
  name: "Git",
  kind: "git",
  layer: layerGitContract,
  fixture: {
    createRepo: (cwd) =>
      Effect.gen(function* () {
        yield* runGit(cwd, ["init"]);
        yield* runGit(cwd, ["config", "user.email", "test@test.com"]);
        yield* runGit(cwd, ["config", "user.name", "Test"]);
      }),
    writeFile: (cwd, relativePath, contents) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const absolutePath = path.join(cwd, relativePath);
        yield* fileSystem.makeDirectory(path.dirname(absolutePath), { recursive: true });
        yield* fileSystem.writeFileString(absolutePath, contents);
      }),
    trackFile: (cwd, relativePath) => runGit(cwd, ["add", relativePath]),
    commit: (cwd, message) => runGit(cwd, ["commit", "-m", message]),
    ignorePath: (cwd, pattern) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        yield* fileSystem.writeFileString(path.join(cwd, ".gitignore"), `${pattern}\n`);
      }),
  },
});

it.effect("lists the fetch and push URLs of a partial clone remote", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const cwd = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-partial-clone-remotes-",
    });
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    yield* runGit(cwd, ["init"]);
    yield* runGit(cwd, ["remote", "add", "origin", "https://github.com/pingdotgg/t3code.git"]);
    yield* runGit(cwd, [
      "remote",
      "set-url",
      "--push",
      "origin",
      "git@github.com:octocat/t3code.git",
    ]);
    yield* runGit(cwd, ["config", "remote.origin.promisor", "true"]);
    yield* runGit(cwd, ["config", "remote.origin.partialclonefilter", "blob:none"]);

    const result = yield* driver.listRemotes(cwd);
    assert.deepEqual(result.remotes, [
      {
        name: "origin",
        url: "https://github.com/pingdotgg/t3code.git",
        pushUrl: Option.some("git@github.com:octocat/t3code.git"),
        isPrimary: true,
      },
    ]);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

const makeCheckpointFixture = Effect.fn("makeCheckpointFixture")(function* (
  driver: Effect.Success<ReturnType<typeof GitVcsDriver.makeVcsDriverShape>>,
  cwd: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const git = (args: ReadonlyArray<string>) =>
    driver.execute({ operation: "checkpoint-test", cwd, args });
  yield* git(["init"]);
  yield* git(["config", "user.name", "Test"]);
  yield* git(["config", "user.email", "test@test.com"]);
  yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "initial\n");
  yield* git(["add", "."]);
  yield* git(["commit", "-m", "initial"]);
  const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/test");
  yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "staged\n");
  yield* git(["add", "."]);
  yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "unstaged\n");
  return { git, checkpointRef };
});

it.effect("checkpoint capture skips untracked nested repositories without a commit", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-unborn-" });
    const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
    const nested = "scratch/empty [repo]";
    yield* git(["init", nested]);
    yield* git(["init", "another empty"]);
    yield* fileSystem.writeFileString(path.join(cwd, nested, "private.txt"), "nested\n");
    yield* git(["init", "committed"]);
    yield* git([
      "-C",
      "committed",
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@test.com",
      "commit",
      "--allow-empty",
      "-m",
      "initial",
    ]);
    const nestedHead = (yield* git(["-C", "committed", "rev-parse", "HEAD"])).stdout.trim();
    yield* fileSystem.writeFileString(path.join(cwd, "untracked.txt"), "new\n");
    const originalIndex = yield* fileSystem.readFile(path.join(cwd, ".git", "index"));

    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
    assert.strictEqual((yield* git(["show", `${checkpointRef}:untracked.txt`])).stdout, "new\n");
    assert.strictEqual((yield* git(["ls-tree", "-r", checkpointRef, "--", nested])).stdout, "");
    assert.strictEqual((yield* git(["ls-tree", checkpointRef, "--", "another empty"])).stdout, "");
    assert.strictEqual(
      (yield* git(["ls-tree", checkpointRef, "--", "committed"])).stdout,
      `160000 commit ${nestedHead}\tcommitted\n`,
    );
    assert.deepEqual(yield* fileSystem.readFile(path.join(cwd, ".git", "index")), originalIndex);
    assert.strictEqual(
      yield* fileSystem.readFileString(path.join(cwd, nested, "private.txt")),
      "nested\n",
    );
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.skipIf(!symlinksSupported)(
  "checkpoint capture writes its index into the real git dir when cwd is a symlinked subdirectory",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveRunner = yield* ProcessRunner.ProcessRunner;
      const indexFiles: string[] = [];
      const captureProcess = yield* VcsProcess.make.pipe(
        Effect.provideService(ProcessRunner.ProcessRunner, {
          run: (input) => {
            const indexFile = input.env?.GIT_INDEX_FILE;
            if (indexFile !== undefined) indexFiles.push(indexFile);
            return liveRunner.run(input);
          },
        }),
      );
      const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, captureProcess),
      );
      const sandbox = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-checkpoint-symlink-",
      });
      const repo = path.join(sandbox, "repo");
      const subdir = path.join(repo, "sub", "dir");
      const link = path.join(sandbox, "nest", "link");
      yield* fileSystem.makeDirectory(subdir, { recursive: true });
      yield* fileSystem.makeDirectory(path.dirname(link), { recursive: true });
      yield* fileSystem.symlink(subdir, link);
      const { git, checkpointRef } = yield* makeCheckpointFixture(driver, repo);
      // Git returns ../../.git from the target; resolving it against the link reaches the decoy.
      yield* git(["-C", sandbox, "init"]);
      yield* fileSystem.writeFileString(path.join(subdir, "nested.txt"), "from-link\n");
      const realCommonDir = yield* fileSystem.realPath(path.join(repo, ".git"));

      yield* driver.checkpoints.captureCheckpoint({ cwd: link, checkpointRef });

      assert.isTrue(indexFiles.length > 0);
      for (const indexFile of indexFiles) {
        assert.strictEqual(yield* fileSystem.realPath(path.dirname(indexFile)), realCommonDir);
      }
      assert.strictEqual(
        (yield* git(["show", `${checkpointRef}:sub/dir/nested.txt`])).stdout,
        "from-link\n",
      );
    }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect("checkpoint capture keeps the shared index a split index still uses", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-split-" });
    const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
    yield* git(["config", "core.splitIndex", "true"]);
    yield* git(["config", "splitIndex.sharedIndexExpire", "now"]);
    yield* git(["update-index", "--split-index"]);
    yield* fileSystem.writeFileString(path.join(cwd, "untracked.txt"), "new\n");

    yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.strictEqual((yield* git(["show", `${checkpointRef}:untracked.txt`])).stdout, "new\n");
    assert.strictEqual((yield* git(["ls-files", "--cached"])).stdout, "file.txt\n");
    assert.strictEqual((yield* git(["diff", "--cached", "--name-only"])).stdout, "file.txt\n");
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint recovery discovers nested HEAD independently of inherited GIT_DIR", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-git-dir-" });
    const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
    yield* git(["init", "empty"]);
    const originalIndex = yield* fs.readFile(path.join(cwd, ".git", "index"));
    yield* Effect.acquireUseRelease(
      Effect.sync(() => {
        const previous = process.env.GIT_DIR;
        process.env.GIT_DIR = path.join(cwd, ".git");
        return previous;
      }),
      () => driver.checkpoints.captureCheckpoint({ cwd, checkpointRef }),
      (previous) =>
        Effect.sync(() => {
          if (previous === undefined) delete process.env.GIT_DIR;
          else process.env.GIT_DIR = previous;
        }),
    );
    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
    assert.strictEqual((yield* git(["ls-tree", "-r", checkpointRef, "--", "empty"])).stdout, "");
    assert.deepEqual(yield* fs.readFile(path.join(cwd, ".git", "index")), originalIndex);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture still fails when a clean filter rejects a file", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fileSystem.makeTempDirectoryScoped({
      prefix: "t3-checkpoint-filter-failure-",
    });
    const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
    yield* fileSystem.writeFileString(path.join(cwd, ".gitattributes"), "file.txt filter=reject\n");
    yield* git(["config", "filter.reject.clean", "false"]);
    yield* git(["config", "filter.reject.required", "true"]);
    const originalIndex = yield* fileSystem.readFile(path.join(cwd, ".git", "index"));

    const result = yield* Effect.result(
      driver.checkpoints.captureCheckpoint({ cwd, checkpointRef }),
    );

    assert.strictEqual(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.strictEqual(result.failure._tag, "VcsProcessExitError");
    }
    assert.deepEqual(yield* fileSystem.readFile(path.join(cwd, ".git", "index")), originalIndex);
    assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture refuses a truncated nested repository listing", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-truncated-" });
    const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
    yield* git(["init", "empty"]);
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) =>
          liveProcess
            .run(input)
            .pipe(
              Effect.map((result) =>
                input.args.includes("--others") ? { ...result, stdoutTruncated: true } : result,
              ),
            ),
      }),
    );

    const result = yield* Effect.result(
      captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef }),
    );

    assert.strictEqual(result._tag, "Failure");
    assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture skips an ignored project while keeping its allowed sibling", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-ignored-" });
    const { git } = yield* makeCheckpointFixture(driver, parent);
    yield* fs.writeFileString(
      path.join(parent, ".gitignore"),
      "*\n!.gitignore\n!file.txt\n!keep/\n!keep/**\n",
    );
    const ignoredCwd = path.join(parent, "ignored");
    const keepCwd = path.join(parent, "keep");
    yield* fs.makeDirectory(ignoredCwd);
    yield* fs.makeDirectory(keepCwd);
    yield* fs.writeFileString(path.join(ignoredCwd, "secret.txt"), "private\n");
    yield* fs.writeFileString(path.join(keepCwd, "new.txt"), "new\n");
    const originalIndex = yield* fs.readFile(path.join(parent, ".git", "index"));
    const ignoredRef = CheckpointRef.make("refs/t3/checkpoints/ignored");
    const keepRef = CheckpointRef.make("refs/t3/checkpoints/keep");

    const ignored = yield* Effect.result(
      driver.checkpoints.captureCheckpoint({ cwd: ignoredCwd, checkpointRef: ignoredRef }),
    );
    yield* driver.checkpoints.captureCheckpoint({ cwd: keepCwd, checkpointRef: keepRef });

    assert.strictEqual((yield* git(["show", `${keepRef}:keep/new.txt`])).stdout, "new\n");
    assert.strictEqual((yield* git(["ls-tree", "-r", keepRef, "--", "ignored"])).stdout, "");
    assert.isFalse(
      yield* driver.checkpoints.hasCheckpointRef({ cwd: parent, checkpointRef: ignoredRef }),
    );
    assert.deepEqual(yield* fs.readFile(path.join(parent, ".git", "index")), originalIndex);
    assert.strictEqual(ignored._tag, "Failure");
    if (ignored._tag === "Failure") {
      assert.strictEqual(ignored.failure._tag, "VcsUnsupportedOperationError");
    }
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each([
  { tracking: "committed-only", committed: ["committed.txt"], staged: [] },
  { tracking: "staged-only", committed: [], staged: ["staged.txt"] },
  { tracking: "mixed", committed: ["committed.txt"], staged: ["staged.txt"] },
])(
  "checkpoint capture keeps $tracking tracked edits but excludes new files in a project ignored later",
  ({ committed, staged }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const parent = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-ignored-later-" });
      const { git, checkpointRef } = yield* makeCheckpointFixture(driver, parent);
      const cwd = path.join(parent, "sub");
      yield* fs.makeDirectory(cwd);
      for (const file of committed) {
        yield* fs.writeFileString(path.join(cwd, file), "committed\n");
        yield* git(["add", `sub/${file}`]);
      }
      if (committed.length > 0) {
        yield* git(["commit", "-m", "track project"]);
      }
      for (const file of staged) {
        yield* fs.writeFileString(path.join(cwd, file), "staged\n");
        yield* git(["add", `sub/${file}`]);
      }
      const tracked = [...committed, ...staged];
      yield* fs.writeFileString(path.join(parent, ".gitignore"), "sub/\n");
      for (const file of tracked) {
        yield* fs.writeFileString(path.join(cwd, file), `${file} working\n`);
      }
      yield* fs.writeFileString(path.join(cwd, "secret.txt"), "private\n");
      const originalIndex = yield* fs.readFile(path.join(parent, ".git", "index"));

      const captured = yield* Effect.result(
        driver.checkpoints.captureCheckpoint({ cwd, checkpointRef }),
      );

      assert.deepEqual(yield* fs.readFile(path.join(parent, ".git", "index")), originalIndex);
      if (captured._tag === "Failure") return yield* captured.failure;
      assert.strictEqual(
        (yield* git(["ls-tree", "-r", "--name-only", checkpointRef, "--", "sub"])).stdout,
        tracked.map((file) => `sub/${file}\n`).join(""),
      );
      for (const file of tracked) {
        assert.strictEqual(
          (yield* git(["show", `${checkpointRef}:sub/${file}`])).stdout,
          `${file} working\n`,
        );
      }
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each([
  { phase: "add", nestedRecovery: false, expireRecovery: false },
  { phase: "update-ref", nestedRecovery: false, expireRecovery: false },
  { phase: "update-ref", nestedRecovery: true, expireRecovery: false },
  { phase: "add", nestedRecovery: true, expireRecovery: false },
  { phase: "add", nestedRecovery: true, expireRecovery: true },
])(
  "checkpoint handles a $phase lock with nestedRecovery=$nestedRecovery, expireRecovery=$expireRecovery",
  ({ phase, nestedRecovery, expireRecovery }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveRunner = yield* ProcessRunner.ProcessRunner;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-ref-race-" });
      const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
      if (nestedRecovery) yield* git(["init", "empty"]);
      const originalIndex = yield* fs.readFile(path.join(cwd, ".git", "index"));
      const refLockPath = path.join(cwd, ".git", `${checkpointRef}.lock`);
      const failed = yield* Deferred.make<void>();
      const retryReached = yield* Deferred.make<void>();
      const allowRetry = yield* Deferred.make<void>();
      const clock = yield* Clock.Clock;
      const privateIndexes = new Set<string>();
      let racedAttempts = 0;
      let discoveries = 0;
      let stageError: VcsProcessExitError | undefined;
      const captureProcess = yield* VcsProcess.make.pipe(
        Effect.provideService(ProcessRunner.ProcessRunner, {
          run: (input) => {
            if (input.args.includes("--others")) discoveries += 1;
            if (input.env?.GIT_INDEX_FILE) privateIndexes.add(input.env.GIT_INDEX_FILE);
            const initialStage =
              phase === "add" &&
              nestedRecovery &&
              !input.args.some((arg) => arg.startsWith(":(exclude,literal)"));
            if (!input.args.includes(phase) || initialStage || ++racedAttempts !== 1) {
              return liveRunner.run(input);
            }
            const lockPath = phase === "add" ? `${input.env!.GIT_INDEX_FILE!}.lock` : refLockPath;
            return Effect.gen(function* () {
              yield* fs
                .makeDirectory(path.dirname(lockPath), { recursive: true })
                .pipe(Effect.orDie);
              yield* fs.writeFileString(lockPath, "concurrent ref writer").pipe(Effect.orDie);
              return yield* liveRunner.run(input).pipe(
                Effect.ensuring(fs.remove(lockPath).pipe(Effect.orDie)),
                Effect.tap(() => Deferred.succeed(failed, undefined)),
              );
            });
          },
        }),
      );
      const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) =>
            captureProcess.run(input).pipe(
              Effect.tapError((error) => {
                if (input.args.includes("add") && error._tag === "VcsProcessExitError")
                  stageError = error;
                return Effect.void;
              }),
            ),
        }),
      );
      const captureStore = yield* makeCaptureStore(captureDriver, cwd);
      const fiber = yield* captureStore.captureCheckpoint({ cwd, checkpointRef }).pipe(
        Effect.provideService(Clock.Clock, {
          ...clock,
          sleep: (duration) =>
            Duration.toMillis(duration) === 75
              ? Deferred.succeed(retryReached, undefined).pipe(
                  Effect.andThen(Deferred.await(allowRetry)),
                )
              : clock.sleep(duration),
        }),
        Effect.exit,
        Effect.forkScoped,
      );
      yield* Deferred.await(failed);
      yield* Deferred.await(retryReached);
      if (expireRecovery) yield* TestClock.adjust("5 seconds");
      else yield* Deferred.succeed(allowRetry, undefined);
      const result = yield* Fiber.join(fiber);
      if (expireRecovery) {
        if (Exit.isSuccess(result))
          return yield* Effect.die("Expected the recovery deadline to expire");
        const error = Cause.findErrorOption(result.cause);
        assert.isTrue(error._tag === "Some");
        if (error._tag === "Some") assert.strictEqual(error.value, stageError);
      } else assert.isTrue(Exit.isSuccess(result));
      assert.strictEqual(racedAttempts, expireRecovery ? 1 : 2);
      assert.strictEqual(discoveries, nestedRecovery ? 1 : 0);
      assert.strictEqual(privateIndexes.size, 1);
      assert.strictEqual(
        yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }),
        !expireRecovery,
      );
      for (const index of privateIndexes) {
        assert.isFalse(yield* fs.exists(index));
        assert.isFalse(yield* fs.exists(`${index}.lock`));
      }
      assert.deepEqual(yield* fs.readFile(path.join(cwd, ".git", "index")), originalIndex);
    }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect("checkpoint recovery preserves interruption and removes the private index", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-recovery-interrupt-" });
    const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
    yield* git(["init", "empty"]);
    const entered = yield* Deferred.make<void>();
    let privateIndex: string | undefined;
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) => {
          if (input.args.includes("add") && input.args.includes("-A"))
            privateIndex = input.env?.GIT_INDEX_FILE;
          return input.args.includes("--others")
            ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
            : liveProcess.run(input);
        },
      }),
    );
    const fiber = yield* captureDriver.checkpoints
      .captureCheckpoint({ cwd, checkpointRef })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(fiber);
    const exit = yield* Fiber.await(fiber);
    assert.isTrue(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
    assert.isDefined(privateIndex);
    assert.isFalse(yield* fs.exists(privateIndex!));
    assert.isFalse(yield* driver.checkpoints.hasCheckpointRef({ cwd, checkpointRef }));
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("reads a boolean the way git config --bool does", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-config-bool-" });
    const git = (args: ReadonlyArray<string>) =>
      driver.execute({ operation: "config-bool-test", cwd, args, allowNonZeroExit: true });
    yield* git(["init", "-q"]);
    // Each case: the key's values in order, and whether gitConfigBool answers without Git.
    const cases: ReadonlyArray<readonly [values: ReadonlyArray<string | null>, parsed: boolean]> = [
      [[null], true],
      [[""], true],
      [["true"], true],
      [["TRUE"], true],
      [["Yes"], true],
      [["on"], true],
      [["false"], true],
      [["No"], true],
      [["OFF"], true],
      [["0"], true],
      [["1"], true],
      [["2"], true],
      [["123456789"], true],
      [["1234567890"], false],
      [["007"], false],
      [["08"], false],
      [["0x1"], false],
      [["1k"], false],
      [["-1"], false],
      [['" 1"'], false],
      [["maybe"], false],
      [["yes", "off"], true],
      [["maybe", "true"], false],
      [["0x1", "false"], false],
    ];
    const include = path.join(cwd, "flags.inc");
    yield* fileSystem.writeFileString(
      include,
      `[t3]\n${cases
        .flatMap(([values], index) =>
          values.map((value) =>
            value === null ? `\tflag${index}\n` : `\tflag${index} = ${value}\n`,
          ),
        )
        .join("")}`,
    );
    yield* git(["config", "include.path", include]);
    const listed = parseGitConfigList(
      (yield* git(["config", "--list", "--show-origin", "-z"])).stdout,
    );
    const valueLists = new Map<string, Array<string | null>>();
    for (const { key, value } of listed ?? []) {
      valueLists.set(key, [...(valueLists.get(key) ?? []), value]);
    }
    const keys = [...cases.keys(), cases.length].map((index) => `t3.Flag${index}`);

    const parsed = keys.map((key) => gitConfigBool(valueLists, key));

    const printed = yield* Effect.forEach(keys, (key) =>
      git(["config", "--bool", key]).pipe(Effect.map(({ stdout }) => stdout.trim() === "true")),
    );
    assert.deepStrictEqual(
      parsed.map((value, index) => value ?? printed[index]),
      printed,
    );
    assert.deepStrictEqual(
      parsed.map((value) => value !== null),
      [...cases.map(([, known]) => known), true],
    );
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture reads core.sparseCheckout again only after the config changes", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const liveRunner = yield* ProcessRunner.ProcessRunner;
    const spawned: Array<ReadonlyArray<string>> = [];
    const countingProcess = yield* VcsProcess.make.pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, {
        run: (input) => {
          spawned.push(input.args);
          return liveRunner.run(input);
        },
      }),
    );
    const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, countingProcess),
    );
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-toggle-" });
    const { git } = yield* makeCheckpointFixture(driver, cwd);
    for (const name of ["in/file", "out/file"]) {
      yield* fileSystem.makeDirectory(path.join(cwd, path.dirname(name)), { recursive: true });
      yield* fileSystem.writeFileString(path.join(cwd, name), `${name}\n`);
    }
    yield* git(["add", "."]);
    yield* git(["commit", "-m", "fixture"]);
    let turn = 0;
    const capture = Effect.gen(function* () {
      const checkpointRef = CheckpointRef.make(`refs/t3/checkpoints/toggle/${++turn}`);
      spawned.length = 0;
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      const coldRef = CheckpointRef.make(`refs/t3/checkpoints/toggle/cold-${turn}`);
      const cold = yield* GitVcsDriver.makeVcsDriverShape();
      yield* cold.checkpoints.captureCheckpoint({ cwd, checkpointRef: coldRef });
      const tree = (ref: string) => git(["rev-parse", `${ref}^{tree}`]);
      assert.strictEqual((yield* tree(checkpointRef)).stdout, (yield* tree(coldRef)).stdout);
      return {
        readsFlag: spawned.some((args) => args.includes("core.sparseCheckout")),
        sparse: spawned.some((args) => args.includes("add") && args.includes("-h")),
      };
    });

    const first = yield* capture;
    yield* capture;
    const warm = yield* capture;
    yield* git(["sparse-checkout", "set", "--cone", "in"]);
    const sparse = yield* capture;
    yield* git(["sparse-checkout", "disable"]);
    const dense = yield* capture;

    assert.deepStrictEqual(first, { readsFlag: true, sparse: false });
    assert.deepStrictEqual(warm, { readsFlag: false, sparse: false });
    assert.isTrue(sparse.sparse);
    assert.isFalse(dense.sparse);
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect.each(
  [false, true].flatMap((nested) =>
    (["sparse", "flags", "manual-skip", "missing", "non-cone-missing"] as const).map(
      (indexState) => ({ nested, indexState }),
    ),
  ),
)(
  "sparse checkpoint preserves two captures (nested=$nested, index=$indexState)",
  ({ nested, indexState }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-sparse-" });
      const { git } = yield* makeCheckpointFixture(driver, cwd);
      const write = Effect.fn(function* (name: string, contents: string) {
        yield* fs.makeDirectory(path.dirname(path.join(cwd, name)), { recursive: true });
        yield* fs.writeFileString(path.join(cwd, name), contents);
      });
      for (const name of [
        "scope/in/edit",
        "scope/in/delete",
        "scope/out/deep/absent",
        "scope/out/present",
        "elsewhere/file",
      ]) {
        yield* write(name, "original\n");
      }
      yield* git(["add", "."]);
      yield* git(["commit", "-m", "sparse fixture"]);
      yield* git(["sparse-checkout", "set", "--cone", "--sparse-index", "scope/in", "elsewhere"]);
      if (indexState === "non-cone-missing")
        yield* git(["sparse-checkout", "set", "--no-cone", "/scope/in/", "/elsewhere/"]);
      yield* write("scope/in/edit", "staged\n");
      yield* write("elsewhere/file", "staged outside\n");
      yield* git(["add", "."]);
      if (indexState === "flags")
        yield* git(["update-index", "--assume-unchanged", "scope/in/delete"]);
      if (indexState === "manual-skip")
        yield* git(["update-index", "--skip-worktree", "scope/in/delete"]);
      yield* git(["config", "sparse.expectFilesOutsideOfPatterns", "true"]);
      yield* write("scope/in/edit", "working\n");
      yield* write("scope/out/present", "modified skipped\n");
      yield* write("scope/out/new file", "new outside cone\n");
      yield* write("elsewhere/file", "working outside\n");
      yield* fs.remove(path.join(cwd, "scope/in/delete"));
      const indexPath = path.join(cwd, ".git/index");
      if (indexState.endsWith("missing")) yield* fs.remove(indexPath);
      const originalIndex = yield* fs.readFile(indexPath).pipe(Effect.orElseSucceed(() => null));
      const captureCwd = nested ? path.join(cwd, "scope") : cwd;
      for (const turn of [1, 2]) {
        const ref = CheckpointRef.make(`refs/t3/checkpoints/sparse/${turn}`);
        if (turn === 2) {
          yield* write("scope/in/edit", "second\n");
          yield* fs.remove(path.join(cwd, "scope/out/new file"));
          yield* write("scope/out/second", "second addition\n");
        }
        const capture = driver.checkpoints.captureCheckpoint({
          cwd: captureCwd,
          checkpointRef: ref,
        });
        if (indexState === "non-cone-missing") {
          assert.strictEqual((yield* capture.pipe(Effect.flip))._tag, "VcsProcessExitError");
          assert.isFalse(
            yield* driver.checkpoints.hasCheckpointRef({ cwd: captureCwd, checkpointRef: ref }),
          );
          assert.isFalse(yield* fs.exists(indexPath));
          break;
        }
        yield* capture;
        for (const [name, content] of [
          ["scope/out/deep/absent", "original\n"],
          ["scope/out/present", "modified skipped\n"],
          ["scope/in/edit", turn === 1 ? "working\n" : "second\n"],
          ["elsewhere/file", nested ? "original\n" : "working outside\n"],
          [
            turn === 1 ? "scope/out/new file" : "scope/out/second",
            turn === 1 ? "new outside cone\n" : "second addition\n",
          ],
        ]) {
          assert.strictEqual((yield* git(["show", `${ref}:${name}`])).stdout, content);
        }
        const files = (yield* git(["ls-tree", "-rz", "--name-only", ref])).stdout.split("\0");
        assert.notInclude(files, "scope/in/delete");
        if (turn === 2) assert.notInclude(files, "scope/out/new file");
        assert.deepEqual(
          yield* fs.readFile(indexPath).pipe(Effect.orElseSucceed(() => null)),
          originalIndex,
        );
        assert.isFalse(yield* fs.exists(path.join(cwd, "scope/out/deep/absent")));
        assert.strictEqual(
          yield* fs.readFileString(path.join(cwd, "elsewhere/file")),
          "working outside\n",
        );
      }
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(
  (["normal", "flags", "sparse"] as const).flatMap((indexMode) =>
    [1, 3, 4096].map((chunkSize) => ({ indexMode, chunkSize })),
  ),
)(
  "checkpoint index inspection handles entries beyond the output cap (index=$indexMode, chunk=$chunkSize)",
  ({ indexMode, chunkSize }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const liveProcess = yield* VcsProcess.VcsProcess;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-checkpoint-inspection-" });
      const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
      yield* fs.writeFileString(path.join(cwd, ".gitattributes"), "stable filter=probe\n");
      yield* fs.writeFileString(path.join(cwd, "stable"), "unchanged\n");
      yield* fs.writeFileString(path.join(cwd, "z-skipped"), "original\n");
      yield* fs.makeDirectory(path.join(cwd, "excluded"));
      yield* fs.writeFileString(path.join(cwd, "excluded/file"), "absent\n");
      yield* fs.writeFileString(
        path.join(cwd, ".git/filter.cjs"),
        'require("node:fs").appendFileSync(".git/reads", "read\\n"); process.stdin.pipe(process.stdout);',
      );
      yield* git(["config", "filter.probe.clean", "node .git/filter.cjs"]);
      yield* fs.utimes(path.join(cwd, "stable"), 1_700_000_000, 1_700_000_000);
      yield* git(["add", "."]);
      yield* git(["commit", "-m", "inspection fixture"]);
      if (indexMode === "flags") yield* git(["update-index", "--skip-worktree", "z-skipped"]);
      if (indexMode === "sparse")
        yield* git(["sparse-checkout", "set", "--cone", "--sparse-index", "included"]);
      yield* fs.writeFileString(path.join(cwd, "z-skipped"), "modified\n");
      yield* fs.writeFileString(path.join(cwd, ".git/reads"), "");
      const originalIndex = yield* fs.readFile(path.join(cwd, ".git/index"));
      const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provideService(VcsProcess.VcsProcess, {
          run: (input) =>
            liveProcess.run(
              input.args.includes("ls-files")
                ? {
                    ...input,
                    maxOutputBytes: 8,
                    onStdoutChunk: (chunk) => {
                      for (let i = 0; i < chunk.length; i += chunkSize)
                        input.onStdoutChunk?.(chunk.subarray(i, i + chunkSize));
                    },
                  }
                : input,
            ),
        }),
      );
      yield* captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      assert.strictEqual((yield* git(["show", `${checkpointRef}:z-skipped`])).stdout, "modified\n");
      if (indexMode !== "flags")
        assert.strictEqual(yield* fs.readFileString(path.join(cwd, ".git/reads")), "");
      assert.deepEqual(yield* fs.readFile(path.join(cwd, ".git/index")), originalIndex);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each([1_700_000_000, 1_700_000_000.9999])(
  "checkpoint capture preserves same-size edits with racy index timestamps (%s)",
  (timestamp) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-racy-" });
      const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
      const filePath = path.join(cwd, "file.txt");
      const indexPath = path.join(cwd, ".git", "index");
      yield* git(["config", "core.trustctime", "false"]);
      yield* fileSystem.writeFileString(filePath, "before\n");
      yield* fileSystem.utimes(filePath, timestamp, timestamp);
      yield* git(["add", "file.txt"]);
      yield* git(["commit", "-m", "record racy file"]);
      yield* fileSystem.utimes(indexPath, timestamp, timestamp);
      const originalIndex = yield* fileSystem.readFile(indexPath);
      const originalIndexMtime = (yield* fileSystem.stat(indexPath)).mtime;
      yield* fileSystem.writeFileString(filePath, "after!\n");
      yield* fileSystem.utimes(filePath, timestamp, timestamp);

      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

      assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "after!\n");
      assert.deepEqual(yield* fileSystem.readFile(indexPath), originalIndex);
      assert.deepEqual((yield* fileSystem.stat(indexPath)).mtime, originalIndexMtime);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture preserves racy edits made after resetting the index", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-racy-reset-" });
    const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
    const racyPath = path.join(cwd, "racy.txt");
    const indexPath = path.join(cwd, ".git", "index");
    const timestamp = 1_700_000_000;
    yield* git(["config", "core.trustctime", "false"]);
    yield* fileSystem.writeFileString(racyPath, "before\n");
    yield* fileSystem.utimes(racyPath, timestamp, timestamp);
    yield* git(["add", "."]);
    yield* git(["commit", "-m", "record racy file"]);
    yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "staged\n");
    yield* git(["add", "file.txt"]);
    yield* fileSystem.utimes(indexPath, timestamp, timestamp);
    const originalIndex = yield* fileSystem.readFile(indexPath);
    const originalIndexMtime = (yield* fileSystem.stat(indexPath)).mtime;
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: Effect.fn(function* (input: VcsProcess.VcsProcessInput) {
          const result = yield* liveProcess.run(input);
          if (input.args.includes("read-tree") && input.args.includes("--reset")) {
            yield* fileSystem.writeFileString(racyPath, "after!\n").pipe(Effect.orDie);
            yield* fileSystem.utimes(racyPath, timestamp, timestamp).pipe(Effect.orDie);
          }
          return result;
        }),
      }),
    );

    yield* captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.strictEqual((yield* git(["show", `${checkpointRef}:racy.txt`])).stdout, "after!\n");
    assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "staged\n");
    assert.deepEqual(yield* fileSystem.readFile(indexPath), originalIndex);
    assert.deepEqual((yield* fileSystem.stat(indexPath)).mtime, originalIndexMtime);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(
  [false, true].flatMap((nested) =>
    (["normal", "flags", "split"] as const).map((indexMode) => ({ nested, indexMode })),
  ),
)(
  "checkpoint index reuse preserves two turns (nested=$nested, index=$indexMode)",
  ({ nested, indexMode }) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-turns-" });
      const { git } = yield* makeCheckpointFixture(driver, cwd);
      const write = (name: string, contents: string) =>
        fileSystem.writeFileString(path.join(cwd, name), contents);
      yield* fileSystem.makeDirectory(path.join(cwd, "scope"));
      for (const name of [
        "scope/staged",
        "scope/deleted",
        "scope/assumed",
        "scope/skipped",
        "outside",
      ]) {
        yield* write(name, "original\n");
      }
      yield* git(["add", "."]);
      yield* git(["commit", "-m", "initial scoped files"]);
      yield* write("scope/staged", "staged\n");
      yield* write("scope/new-deleted", "staged then deleted\n");
      yield* write("outside", "staged outside\n");
      yield* git(["add", "."]);
      if (indexMode === "flags") {
        yield* git(["update-index", "--assume-unchanged", "scope/assumed"]);
        yield* git(["update-index", "--skip-worktree", "scope/skipped"]);
      }
      if (indexMode === "split") {
        yield* git(["update-index", "--split-index"]);
      }
      const originalIndex = yield* fileSystem.readFile(path.join(cwd, ".git", "index"));
      for (const name of ["scope/staged", "scope/assumed", "scope/skipped", "outside"]) {
        yield* write(name, "working\n");
      }
      yield* write("scope/new", "first\n");
      yield* fileSystem.remove(path.join(cwd, "scope/deleted"));
      yield* fileSystem.remove(path.join(cwd, "scope/new-deleted"));
      const captureCwd = nested ? path.join(cwd, "scope") : cwd;
      const first = CheckpointRef.make("refs/t3/checkpoints/turns/1");
      const second = CheckpointRef.make("refs/t3/checkpoints/turns/2");
      yield* driver.checkpoints.captureCheckpoint({ cwd: captureCwd, checkpointRef: first });
      for (const name of ["scope/staged", "scope/assumed", "scope/skipped"]) {
        assert.strictEqual((yield* git(["show", `${first}:${name}`])).stdout, "working\n");
      }
      assert.strictEqual(
        (yield* git(["show", `${first}:outside`])).stdout,
        nested ? "original\n" : "working\n",
      );
      const files = (yield* git(["ls-tree", "-r", "--name-only", first])).stdout.split("\n");
      assert.notInclude(files, "scope/deleted");
      assert.notInclude(files, "scope/new-deleted");
      assert.include(files, "scope/new");

      yield* write("scope/staged", "second\n");
      yield* fileSystem.remove(path.join(cwd, "scope/new"));
      yield* write("scope/second", "added in second turn\n");
      yield* driver.checkpoints.captureCheckpoint({ cwd: captureCwd, checkpointRef: second });
      assert.strictEqual(
        (yield* git(["diff", "--name-only", first, second])).stdout,
        "scope/new\nscope/second\nscope/staged\n",
      );
      assert.strictEqual((yield* git(["show", `${second}:scope/staged`])).stdout, "second\n");
      assert.strictEqual(
        (yield* git(["show", `${second}:scope/second`])).stdout,
        "added in second turn\n",
      );
      assert.deepEqual(yield* fileSystem.readFile(path.join(cwd, ".git", "index")), originalIndex);
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.each(["missing", "invalid"] as const)(
  "checkpoint capture falls back when the user index is %s",
  (indexState) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-index-" });
      const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
      const indexPath = path.join(cwd, ".git", "index");
      if (indexState === "missing") {
        yield* fileSystem.remove(indexPath);
      } else {
        yield* fileSystem.writeFileString(indexPath, "invalid index");
      }

      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

      assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
      if (indexState === "missing") {
        assert.isFalse(yield* fileSystem.exists(indexPath));
      } else {
        assert.strictEqual(yield* fileSystem.readFileString(indexPath), "invalid index");
      }
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

const captureRecordingGitCommands = Effect.fn("test.captureRecordingGitCommands")(function* (
  cwd: string,
  checkpointRef: CheckpointRef,
) {
  const liveProcess = yield* VcsProcess.VcsProcess;
  const commands: Array<string | undefined> = [];
  const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
    Effect.provideService(VcsProcess.VcsProcess, {
      run: (input) => {
        let index = 0;
        while (input.args[index] === "-C" || input.args[index] === "-c") index += 2;
        commands.push(input.args[index]);
        return liveProcess.run(input);
      },
    }),
  );
  yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
  return commands;
});

it.effect.each(["committed", "unborn"] as const)(
  "checkpoint capture reads the repository with one rev-parse (HEAD=%s)",
  (head) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-processes-" });
      const git = (args: ReadonlyArray<string>) =>
        driver.execute({ operation: "checkpoint-test", cwd, args });
      const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/test");
      if (head === "committed") {
        yield* makeCheckpointFixture(driver, cwd);
      } else {
        yield* git(["init"]);
        yield* fileSystem.writeFileString(path.join(cwd, "file.txt"), "unstaged\n");
      }

      const commands = yield* captureRecordingGitCommands(cwd, checkpointRef);

      assert.deepStrictEqual(commands, [
        "rev-parse",
        "config",
        ...(head === "committed" ? ["read-tree", "ls-files"] : []),
        "check-ignore",
        "add",
        "write-tree",
        "commit-tree",
        "update-ref",
      ]);
      assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
      const [, ...parents] = (yield* git([
        "rev-list",
        "--parents",
        "-n",
        "1",
        checkpointRef,
      ])).stdout
        .trim()
        .split(" ");
      assert.deepStrictEqual(
        parents,
        head === "committed" ? [(yield* git(["rev-parse", "HEAD"])).stdout.trim()] : [],
      );
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("checkpoint capture outside a repository fails with the common dir lookup error", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-no-repo-" });

    const error = yield* captureRecordingGitCommands(
      cwd,
      CheckpointRef.make("refs/t3/checkpoints/test"),
    ).pipe(Effect.flip);

    assert.instanceOf(error, VcsProcessExitError);
    assert.strictEqual(error.operation, "GitVcsDriver.checkpoints.resolveGitCommonDir");
    assert.strictEqual(error.exitCode, 128);
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "checkpoint capture handles a repository path containing a newline",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const driver = yield* GitVcsDriver.makeVcsDriverShape();
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-new\nline-" });
      const { git, checkpointRef } = yield* makeCheckpointFixture(driver, cwd);
      const originalIndex = yield* fileSystem.readFile(path.join(cwd, ".git", "index"));

      const commands = yield* captureRecordingGitCommands(cwd, checkpointRef);

      assert.deepStrictEqual(commands, [
        "rev-parse",
        "rev-parse",
        "config",
        "rev-parse",
        "read-tree",
        "ls-files",
        "check-ignore",
        "add",
        "write-tree",
        "commit-tree",
        "update-ref",
      ]);
      assert.strictEqual((yield* git(["show", `${checkpointRef}:file.txt`])).stdout, "unstaged\n");
      assert.deepEqual(yield* fileSystem.readFile(path.join(cwd, ".git", "index")), originalIndex);
      assert.deepStrictEqual(
        (yield* fileSystem.readDirectory(path.join(cwd, ".git"))).filter((name) =>
          name.startsWith("t3-checkpoint-index-"),
        ),
        [],
      );
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect("restores empty checkpoints without changing paths outside the workspace", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    for (const nested of [false, true]) {
      const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-empty-checkpoint-" });
      yield* runGit(root, ["init"]);
      yield* runGit(root, ["config", "user.email", "test@test.com"]);
      yield* runGit(root, ["config", "user.name", "Test"]);
      if (nested) {
        yield* fileSystem.writeFileString(path.join(root, "outside.txt"), "original\n");
        yield* runGit(root, ["add", "."]);
      }
      yield* runGit(root, ["commit", "--allow-empty", "-m", "initial"]);
      const cwd = nested ? path.join(root, "nested") : root;
      yield* fileSystem.makeDirectory(cwd, { recursive: true });
      const checkpointRef = CheckpointRef.make("refs/t3/checkpoints/empty");
      yield* driver.checkpoints.captureCheckpoint({ cwd, checkpointRef });
      if (nested) {
        yield* fileSystem.writeFileString(path.join(root, "outside.txt"), "changed\n");
        yield* runGit(root, ["add", "outside.txt"]);
      }
      for (const staged of [false, true]) {
        const addedPath = path.join(cwd, "added.txt");
        yield* fileSystem.writeFileString(addedPath, "new\n");
        if (staged) yield* runGit(cwd, ["add", "added.txt"]);
        assert.isTrue(
          yield* driver.checkpoints.restoreCheckpoint({
            cwd,
            checkpointRef,
            fallbackToHead: false,
          }),
        );
        assert.isTrue(yield* fileSystem.exists(cwd));
        assert.isFalse(yield* fileSystem.exists(addedPath));
      }
      yield* fileSystem.writeFileString(
        path.join(root, ".git", "info", "exclude"),
        "ignored.txt\n",
      );
      yield* fileSystem.writeFileString(path.join(cwd, "ignored.txt"), "keep\n");
      yield* fileSystem.makeDirectory(path.join(cwd, "untracked"));
      yield* fileSystem.writeFileString(path.join(cwd, "untracked", "file.txt"), "remove\n");
      assert.isTrue(
        yield* driver.checkpoints.restoreCheckpoint({ cwd, checkpointRef, fallbackToHead: false }),
      );
      assert.strictEqual(yield* fileSystem.readFileString(path.join(cwd, "ignored.txt")), "keep\n");
      assert.isFalse(yield* fileSystem.exists(path.join(cwd, "untracked")));
      if (nested) {
        assert.strictEqual(
          yield* fileSystem.readFileString(path.join(root, "outside.txt")),
          "changed\n",
        );
        const staged = yield* driver.execute({
          operation: "test",
          cwd: root,
          args: ["diff", "--cached", "--name-only"],
        });
        assert.strictEqual(staged.stdout.trim(), "outside.txt");
      }
    }
  }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

// The real VCS process, spawning `command` wherever the driver runs git.
const vcsProcessSpawning = (command: string) =>
  Layer.effect(
    VcsProcess.VcsProcess,
    Effect.map(VcsProcess.make, (vcsProcess) =>
      VcsProcess.VcsProcess.of({ run: (input) => vcsProcess.run({ ...input, command }) }),
    ),
  ).pipe(Layer.provide(ProcessRunner.layer), Layer.provideMerge(NodeServices.layer));

it.effect("GitVcsDriver treats a missing git executable as outside any repository", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-missing-git-" });
    const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provide(vcsProcessSpawning(path.join(cwd, "git"))),
    );

    assert.strictEqual(yield* driver.isInsideWorkTree(cwd), false);
    assert.strictEqual(yield* driver.detectRepository(cwd), null);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "GitVcsDriver detects a repository whose path contains a newline",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parent = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-newline-repo-" });
      const cwd = path.join(parent, "multi\nline");
      yield* fileSystem.makeDirectory(cwd);
      yield* runGit(cwd, ["init"]);
      const driver = yield* GitVcsDriver.makeVcsDriverShape();

      const repository = yield* driver.detectRepository(cwd);
      assert.strictEqual(repository?.rootPath, yield* fileSystem.realPath(cwd));
      assert.strictEqual(repository?.metadataPath, ".git");
    }).pipe(Effect.scoped, Effect.provide(layerGitContract)),
);

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "GitVcsDriver still fails detection when git exists but cannot be spawned",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      // A file without the execute bit fails to spawn with a permission error.
      const notExecutable = yield* fileSystem.makeTempFileScoped({ prefix: "t3-git-" });
      const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
        Effect.provide(vcsProcessSpawning(notExecutable)),
      );

      const error = yield* driver.isInsideWorkTree(process.cwd()).pipe(Effect.flip);
      assert.strictEqual(error._tag, "VcsProcessSpawnError");
      const detectionError = yield* driver.detectRepository(process.cwd()).pipe(Effect.flip);
      assert.strictEqual(detectionError._tag, "VcsProcessSpawnError");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

const gitCommandCount = (operation: string, outcome: string) =>
  Metric.value(
    Metric.withAttributes(gitCommandsTotal, [
      ["operation", operation],
      ["outcome", outcome],
    ]),
  ).pipe(Effect.map((state) => state.count));

const gitCommandTiming = (operation: string) =>
  Metric.value(Metric.withAttributes(gitCommandDuration, [["operation", operation]]));

// Keep VcsProcess's exit classification; replace only the subprocess and its clock.
const makeMeteredDriver = Effect.fn("test.makeMeteredDriver")(function* (exitCode: number) {
  const process = yield* VcsProcess.make.pipe(
    Effect.provideService(ProcessRunner.ProcessRunner, {
      run: () =>
        TestClock.adjust("250 millis").pipe(
          Effect.as({
            code: ChildProcessSpawner.ExitCode(exitCode),
            stdout: "git output",
            stderr: exitCode === 0 ? "" : "fatal: test failure",
            timedOut: false,
            stdoutTruncated: false,
            stderrTruncated: false,
            stdoutInvalidUtf8: false,
            stderrInvalidUtf8: false,
          }),
        ),
    }),
  );
  return yield* GitVcsDriver.makeVcsDriverShape().pipe(
    Effect.provideService(VcsProcess.VcsProcess, process),
  );
});

it.effect("GitVcsDriver meters each git command once under its operation", () =>
  Effect.gen(function* () {
    const operation = "GitVcsDriver.checkpoints.resolveHeadCommit";
    const driver = yield* makeMeteredDriver(0);

    const result = yield* driver.execute({ operation, cwd: "/repo", args: ["rev-parse", "HEAD"] });
    yield* driver.execute({ operation, cwd: "/another-repo", args: ["rev-parse", "other-ref"] });

    assert.strictEqual(result.stdout, "git output");
    assert.strictEqual(yield* gitCommandCount(operation, "success"), 2);
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 0);
    const timing = yield* gitCommandTiming(operation);
    assert.strictEqual(timing.count, 2);
    assert.strictEqual(timing.sum, 500);
  }).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("GitVcsDriver counts an allowed non-zero exit as a success", () =>
  Effect.gen(function* () {
    const operation = "GitVcsDriver.checkpoints.hasHeadCommit";
    const driver = yield* makeMeteredDriver(1);

    const result = yield* driver.execute({
      operation,
      cwd: "/repo",
      args: ["rev-parse", "--verify", "HEAD"],
      allowNonZeroExit: true,
    });

    assert.strictEqual(result.exitCode, 1);
    assert.strictEqual(yield* gitCommandCount(operation, "success"), 1);
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 0);
    const timing = yield* gitCommandTiming(operation);
    assert.strictEqual(timing.count, 1);
    assert.strictEqual(timing.sum, 250);
  }).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("GitVcsDriver counts a failed git command as a failure", () =>
  Effect.gen(function* () {
    const operation = "GitVcsDriver.checkpoints.resolveHeadCommit";
    const driver = yield* makeMeteredDriver(128);

    const error = yield* driver
      .execute({ operation, cwd: "/repo", args: ["rev-parse", "HEAD"] })
      .pipe(Effect.flip);

    assert.strictEqual(error._tag, "VcsProcessExitError");
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 1);
    assert.strictEqual(yield* gitCommandCount(operation, "success"), 0);
    const timing = yield* gitCommandTiming(operation);
    assert.strictEqual(timing.count, 1);
    assert.strictEqual(timing.sum, 250);
  }).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("checkpoint capture meters every git process it starts, including streamed ones", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const liveProcess = yield* VcsProcess.VcsProcess;
    const operation = VcsProcess.CHECKPOINT_CAPTURE_OPERATION;
    let started = 0;
    let streamed = 0;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const captureDriver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, {
        run: (input) => {
          if (input.operation === operation) {
            started++;
            if (input.onStdoutChunk !== undefined) streamed++;
          }
          return liveProcess.run(input);
        },
      }),
    );
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-checkpoint-metrics-" });
    const { checkpointRef } = yield* makeCheckpointFixture(driver, cwd);

    yield* captureDriver.checkpoints.captureCheckpoint({ cwd, checkpointRef });

    assert.isAbove(streamed, 0);
    assert.strictEqual(yield* gitCommandCount(operation, "success"), started);
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 0);
    assert.strictEqual((yield* gitCommandTiming(operation)).count, started);
  }).pipe(
    Effect.scoped,
    Effect.provide(layerGitContract),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("git metrics finalize cancellation without exposing arbitrary operation labels", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const process = yield* VcsProcess.make.pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, {
        run: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
      }),
    );
    const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, process),
    );
    const operation = "arbitrary operation /private/path";
    const fiber = yield* driver
      .execute({ operation, cwd: "/repo", args: ["rev-parse", "HEAD"] })
      .pipe(Effect.forkChild);
    yield* Deferred.await(started);
    yield* TestClock.adjust("250 millis");
    yield* Fiber.interrupt(fiber);

    assert.strictEqual(yield* gitCommandCount("other", "interrupt"), 1);
    assert.strictEqual(yield* gitCommandCount(operation, "interrupt"), 0);
    const timing = yield* gitCommandTiming("other");
    assert.strictEqual(timing.count, 1);
    assert.strictEqual(timing.sum, 250);
  }).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

it.effect("git metrics count fsync fallback and transient retry subprocesses separately", () =>
  Effect.gen(function* () {
    const retryReady = yield* Deferred.make<void>();
    const operation = VcsProcess.CHECKPOINT_CAPTURE_OPERATION;
    let attempts = 0;
    const process = yield* VcsProcess.make.pipe(
      Effect.provideService(HostProcess.Platform, "darwin"),
      Effect.provideService(ProcessRunner.ProcessRunner, {
        run: (input) =>
          Effect.gen(function* () {
            attempts++;
            yield* TestClock.adjust("250 millis");
            if (attempts === 2) {
              assert.include(input.args, "core.fsyncMethod=writeout-only");
              yield* Deferred.succeed(retryReady, undefined);
            }
            return {
              code: ChildProcessSpawner.ExitCode(attempts < 3 ? 128 : 0),
              stdout: "",
              stderr:
                attempts === 1
                  ? "fatal: fsync error on '.git/objects/test': Operation not supported"
                  : attempts === 2
                    ? "fatal: Unable to create '/repo/.git/index.lock': File exists."
                    : "",
              timedOut: false,
              stdoutTruncated: false,
              stderrTruncated: false,
              stdoutInvalidUtf8: false,
              stderrInvalidUtf8: false,
            };
          }),
      }),
    );
    const driver = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, process),
    );
    const fiber = yield* driver
      .execute({ operation, cwd: "/repo", args: ["-c", "core.fsyncMethod=batch", "add"] })
      .pipe(Effect.forkChild);
    yield* Deferred.await(retryReady);
    yield* TestClock.adjust("75 millis");
    yield* Fiber.join(fiber);

    assert.strictEqual(attempts, 3);
    assert.strictEqual(yield* gitCommandCount(operation, "failure"), 2);
    assert.strictEqual(yield* gitCommandCount(operation, "success"), 1);
    const timing = yield* gitCommandTiming(operation);
    assert.strictEqual(timing.count, 3);
    assert.strictEqual(timing.sum, 750);
  }).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(Metric.MetricRegistry, new Map()),
  ),
);

// The git commands a turn runs, as the orchestrator issues them: the baseline probe at run start
// (RunExecutionService.startRootRun → captureBaseline), then CheckpointService.capture, then the
// turn diff a client asks for. Equivalence tests run it on a warm driver and on a fresh one.
const TURN_REFS_PREFIX = "refs/t3/orchestration-v2/checkpoints/dGVzdA/ordinal";
const turnRef = (ordinal: number) => CheckpointRef.make(`${TURN_REFS_PREFIX}/${ordinal}`);

const checkpointTurn = Effect.fn("test.checkpointTurn")(function* (
  driver: VcsDriver.VcsDriver["Service"],
  cwd: string,
  ordinal: number,
) {
  const checkpoints = driver.checkpoints;
  const previousRef = turnRef(Math.max(0, ordinal - 1));
  const baselineExists = yield* checkpoints.hasCheckpointRef({ cwd, checkpointRef: previousRef });
  if (!baselineExists) {
    yield* checkpoints.captureCheckpoint({ cwd, checkpointRef: previousRef });
  }
  yield* checkpoints.captureCheckpoint({ cwd, checkpointRef: turnRef(ordinal) });
  const previousExists = yield* checkpoints.hasCheckpointRef({ cwd, checkpointRef: previousRef });
  const refs = { cwd, fromCheckpointRef: previousRef, toCheckpointRef: turnRef(ordinal) };
  const [numstat, authored] = previousExists
    ? yield* Effect.all([
        checkpoints.diffCheckpoints({
          ...refs,
          fallbackFromToHead: false,
          ignoreWhitespace: false,
          format: "numstat",
        }),
        checkpoints.listAuthoredPaths(refs),
      ])
    : (["", null] as const);
  const patch = previousExists
    ? yield* checkpoints.diffCheckpoints({
        ...refs,
        fallbackFromToHead: false,
        ignoreWhitespace: true,
      })
    : "";
  return {
    baselineExists,
    previousExists,
    numstat,
    authored: authored === null ? null : [...authored].sort(),
    patch,
  };
});

const gitSubcommand = (args: ReadonlyArray<string>) => {
  for (let index = 0; index < args.length; index++) {
    const argument = args[index]!;
    if (argument === "-C" || argument === "-c") {
      index++;
      continue;
    }
    if (argument.startsWith("-")) continue;
    return argument;
  }
  return "?";
};

const FIXED_COMMIT_ENV = {
  ...process.env,
  GIT_AUTHOR_DATE: "2024-01-01T00:00:00Z",
  GIT_COMMITTER_DATE: "2024-01-01T00:00:00Z",
};

// A warm driver whose git commands are recorded, a fresh driver per call for comparison, and two
// repositories with identical commits (fixed dates) so checkpoint parents match across them.
const makeTurnHarness = Effect.fn("test.makeTurnHarness")(function* (
  prefix: string,
  initArgs: ReadonlyArray<string> = [],
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const liveRunner = yield* ProcessRunner.ProcessRunner;
  const spawned: Array<ReadonlyArray<string>> = [];
  const countingProcess = yield* VcsProcess.make.pipe(
    Effect.provideService(ProcessRunner.ProcessRunner, {
      run: (input) => {
        spawned.push(input.args);
        return liveRunner.run(input);
      },
    }),
  );
  const warm = yield* GitVcsDriver.makeVcsDriverShape().pipe(
    Effect.provideService(VcsProcess.VcsProcess, countingProcess),
  );
  const reference = yield* GitVcsDriver.makeVcsDriverShape();
  const root = yield* fileSystem.makeTempDirectoryScoped({ prefix });
  const repos = { warm: path.join(root, "warm"), cold: path.join(root, "cold") } as const;
  const gitIn = (cwd: string, args: ReadonlyArray<string>, env?: NodeJS.ProcessEnv) =>
    reference.execute({
      operation: "turn-test",
      cwd,
      args,
      allowNonZeroExit: true,
      ...(env === undefined ? {} : { env }),
    });
  // Runs the same command in both repositories.
  const gitBoth = (args: ReadonlyArray<string>, env?: NodeJS.ProcessEnv) =>
    Effect.forEach([repos.warm, repos.cold], (cwd) => gitIn(cwd, args, env), { discard: true });
  const writeBoth = (relativePath: string, contents: string) =>
    Effect.forEach(
      [repos.warm, repos.cold],
      (cwd) =>
        fileSystem
          .makeDirectory(path.join(cwd, path.dirname(relativePath)), { recursive: true })
          .pipe(Effect.andThen(fileSystem.writeFileString(path.join(cwd, relativePath), contents))),
      { discard: true },
    );
  const removeBoth = (relativePath: string) =>
    Effect.forEach(
      [repos.warm, repos.cold],
      (cwd) => fileSystem.remove(path.join(cwd, relativePath), { force: true }),
      { discard: true },
    );
  const commitBoth = (message: string) =>
    gitBoth(["commit", "-q", "-m", message], FIXED_COMMIT_ENV);
  let initialized = true;
  for (const cwd of [repos.warm, repos.cold]) {
    yield* fileSystem.makeDirectory(cwd, { recursive: true });
    const init = yield* gitIn(cwd, ["init", "-q", "-b", "main", ...initArgs]);
    initialized &&= init.exitCode === 0;
    yield* gitIn(cwd, ["config", "user.name", "Test"]);
    yield* gitIn(cwd, ["config", "user.email", "test@test.com"]);
  }
  // Checkpoint commits carry the capture time; compare what the turn reads from them instead.
  const describeRef = (cwd: string, ordinal: number) =>
    gitIn(cwd, ["log", "-1", "--format=%T %P", `${turnRef(ordinal)}^{commit}`]).pipe(
      Effect.map((result) => (result.exitCode === 0 ? result.stdout.trim() : "missing")),
    );
  // One turn on both repositories; the fresh driver has nothing remembered.
  const turn = Effect.fn("test.turn")(function* (ordinal: number, subdirectory = "") {
    spawned.length = 0;
    const warmResult = yield* Effect.result(
      checkpointTurn(warm, path.join(repos.warm, subdirectory), ordinal),
    );
    const commands = spawned.map((args) => gitSubcommand(args.slice(2)));
    const cold = yield* GitVcsDriver.makeVcsDriverShape();
    const coldResult = yield* Effect.result(
      checkpointTurn(cold, path.join(repos.cold, subdirectory), ordinal),
    );
    if (warmResult._tag === "Failure" || coldResult._tag === "Failure") {
      assert.strictEqual(warmResult._tag, coldResult._tag);
      const warmTag = warmResult._tag === "Failure" ? warmResult.failure._tag : null;
      const coldTag = coldResult._tag === "Failure" ? coldResult.failure._tag : null;
      assert.strictEqual(warmTag, coldTag);
      return {
        commands,
        failed: warmTag,
        baselineExists: false,
        previousExists: false,
        numstat: "",
        authored: null,
        patch: "",
      };
    }
    assert.deepStrictEqual(warmResult.success, coldResult.success);
    for (const probe of [ordinal - 1, ordinal]) {
      assert.strictEqual(
        yield* describeRef(repos.warm, Math.max(0, probe)),
        yield* describeRef(repos.cold, Math.max(0, probe)),
        `checkpoint ${probe} differs between the warm and the fresh driver`,
      );
    }
    return { commands, failed: null, ...warmResult.success };
  });
  return {
    initialized,
    root,
    repos,
    gitIn,
    gitBoth,
    writeBoth,
    removeBoth,
    commitBoth,
    turn,
    spawned,
    countingProcess,
  };
});

const STEADY_TURN_COMMANDS = [
  "read-tree",
  "ls-files",
  "add",
  "write-tree",
  "commit-tree",
  "update-ref",
];

it.effect("checkpoint turns reuse refs, commits and repository paths it already knows", () =>
  Effect.gen(function* () {
    const h = yield* makeTurnHarness("t3-checkpoint-turns-");
    yield* h.writeBoth("file.txt", "initial\n");
    yield* h.writeBoth("other.txt", "other\n");
    yield* h.gitBoth(["add", "."]);
    yield* h.commitBoth("initial");
    yield* h.writeBoth("file.txt", "staged\n");
    yield* h.gitBoth(["add", "file.txt"]);
    yield* h.writeBoth("file.txt", "unstaged\n");

    const first = yield* h.turn(1);
    assert.strictEqual(first.baselineExists, false);
    assert.strictEqual(first.previousExists, true);
    yield* h.turn(2);
    const steady = yield* h.turn(3);
    assert.deepStrictEqual(steady.commands, STEADY_TURN_COMMANDS);
    assert.deepStrictEqual(
      { numstat: steady.numstat, authored: steady.authored, patch: steady.patch },
      { numstat: "", authored: null, patch: "" },
    );

    // A turn that changes files runs the diff it needs and nothing else.
    yield* h.writeBoth("file.txt", "edited\n");
    const edited = yield* h.turn(4);
    assert.deepStrictEqual(edited.commands, [...STEADY_TURN_COMMANDS, "diff", "diff"]);
    assert.strictEqual(edited.numstat, "1\t1\tfile.txt\0");
    assert.isTrue(edited.patch.includes("+edited"));

    // HEAD moved: the HEAD probe runs once more and the authored-path listing runs (never git log
    // for the checkpoint commits themselves).
    yield* h.gitBoth(["add", "file.txt"]);
    yield* h.commitBoth("edit");
    const moved = yield* h.turn(5);
    assert.deepStrictEqual(
      [...moved.commands].sort(),
      [...STEADY_TURN_COMMANDS, "rev-parse", "diff", "diff", "log", "log"].sort(),
    );
    assert.deepStrictEqual(moved.authored, ["file.txt"]);
    assert.strictEqual(moved.numstat, "");

    const settled = yield* h.turn(6);
    assert.deepStrictEqual(settled.commands, STEADY_TURN_COMMANDS);
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect("checkpoint ref memory follows deletions, rewrites, packed refs and packed objects", () =>
  Effect.gen(function* () {
    const h = yield* makeTurnHarness("t3-checkpoint-refs-");
    yield* h.writeBoth("file.txt", "initial\n");
    yield* h.gitBoth(["add", "."]);
    yield* h.commitBoth("initial");
    yield* h.turn(1);
    yield* h.turn(2);
    yield* h.turn(3);

    // Deleted behind the driver's back: the baseline is missing and gets captured again.
    yield* h.gitBoth(["update-ref", "-d", turnRef(3)]);
    const recreated = yield* h.turn(4);
    assert.deepStrictEqual(
      { baseline: recreated.baselineExists, previous: recreated.previousExists },
      { baseline: false, previous: true },
    );
    assert.isFalse(recreated.commands.includes("rev-parse"));

    // Rewritten to another commit: git answers once, then the new value is known.
    yield* h.gitBoth(["update-ref", turnRef(4), "HEAD"]);
    const rewritten = yield* h.turn(5);
    assert.strictEqual(rewritten.commands.filter((command) => command === "rev-parse").length, 1);
    assert.strictEqual(rewritten.previousExists, true);

    // Packed: the loose files are gone; packed-refs answers for checkpoint refs and git once for HEAD.
    yield* h.gitBoth(["pack-refs", "--all"]);
    const packed = yield* h.turn(6);
    assert.strictEqual(packed.baselineExists, true);
    assert.deepStrictEqual(
      [...packed.commands].sort(),
      [...STEADY_TURN_COMMANDS, "rev-parse"].sort(),
    );
    const afterPacked = yield* h.turn(7);
    assert.deepStrictEqual(afterPacked.commands, STEADY_TURN_COMMANDS);

    // Objects packed: a driver that has not read these commits asks git log once, then remembers.
    yield* h.gitBoth(["gc", "--prune=now", "-q"]);
    const later = yield* GitVcsDriver.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, h.countingProcess),
    );
    const cold = yield* GitVcsDriver.makeVcsDriverShape();
    const refs = (cwd: string) => ({
      cwd,
      fromCheckpointRef: turnRef(1),
      toCheckpointRef: turnRef(2),
    });
    // A directory's first read runs git without loading its snapshot, so probe ref 1 again.
    for (const ref of [turnRef(1), turnRef(2), turnRef(1)]) {
      assert.isTrue(
        yield* later.checkpoints.hasCheckpointRef({ cwd: h.repos.warm, checkpointRef: ref }),
      );
    }
    h.spawned.length = 0;
    const authored = yield* later.checkpoints.listAuthoredPaths(refs(h.repos.warm));
    assert.deepStrictEqual(
      h.spawned.map((args) => gitSubcommand(args.slice(2))),
      ["log"],
    );
    assert.strictEqual(authored, yield* cold.checkpoints.listAuthoredPaths(refs(h.repos.cold)));
    h.spawned.length = 0;
    yield* later.checkpoints.listAuthoredPaths(refs(h.repos.warm));
    const diff = yield* later.checkpoints.diffCheckpoints({
      ...refs(h.repos.warm),
      fallbackFromToHead: false,
      ignoreWhitespace: false,
      format: "numstat",
    });
    assert.deepStrictEqual(h.spawned, []);
    assert.strictEqual(diff, "");
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect("checkpoint capture sees HEAD being born, switched and detached", () =>
  Effect.gen(function* () {
    const h = yield* makeTurnHarness("t3-checkpoint-head-");
    yield* h.writeBoth("file.txt", "initial\n");
    yield* h.turn(1);
    const unborn = yield* h.turn(2);
    assert.deepStrictEqual(unborn.commands, ["add", "write-tree", "commit-tree", "update-ref"]);
    assert.deepStrictEqual(
      { numstat: unborn.numstat, authored: unborn.authored },
      { numstat: "", authored: null },
    );

    yield* h.gitBoth(["add", "."]);
    yield* h.commitBoth("first");
    const born = yield* h.turn(3);
    assert.deepStrictEqual(born.commands, ["rev-parse", ...STEADY_TURN_COMMANDS]);

    // A new HEAD reloads the snapshot: its paths and config, then the HEAD and ignore probes.
    const reloaded = [...STEADY_TURN_COMMANDS, "check-ignore", "config", "rev-parse", "rev-parse"];
    yield* h.gitBoth(["checkout", "-q", "-b", "feature"]);
    const switched = yield* h.turn(4);
    assert.deepStrictEqual([...switched.commands].sort(), [...reloaded].sort());

    yield* h.gitBoth(["checkout", "-q", "--detach"]);
    const detached = yield* h.turn(5);
    assert.deepStrictEqual([...detached.commands].sort(), [...reloaded].sort());
    const settled = yield* h.turn(6);
    assert.deepStrictEqual(settled.commands, STEADY_TURN_COMMANDS);
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

const IGNORED_TURN_COMMANDS = [
  "read-tree",
  "ls-files",
  "diff",
  "ls-files",
  "add",
  "write-tree",
  "commit-tree",
  "update-ref",
];
const withProbe = (commands: ReadonlyArray<string>) => [
  ...commands.slice(0, 2),
  "check-ignore",
  ...commands.slice(2),
];

it.effect("checkpoint capture probes the ignore files again only after one changes", () =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const fileSystem = yield* FileSystem.FileSystem;
    const h = yield* makeTurnHarness("t3-checkpoint-ignore-");
    yield* h.writeBoth("file.txt", "initial\n");
    yield* h.gitBoth(["add", "."]);
    yield* h.commitBoth("initial");
    yield* h.turn(1);
    assert.deepStrictEqual((yield* h.turn(2)).commands, STEADY_TURN_COMMANDS);

    const expectTurns = Effect.fn("test.expectTurns")(function* (
      ordinal: number,
      first: ReadonlyArray<string>,
      second: ReadonlyArray<string>,
    ) {
      assert.deepStrictEqual((yield* h.turn(ordinal)).commands, first);
      assert.deepStrictEqual((yield* h.turn(ordinal + 1)).commands, second);
    });

    yield* h.writeBoth(".gitignore", "*\n");
    yield* expectTurns(3, withProbe(IGNORED_TURN_COMMANDS), IGNORED_TURN_COMMANDS);
    yield* h.removeBoth(".gitignore");
    yield* expectTurns(5, withProbe(STEADY_TURN_COMMANDS), STEADY_TURN_COMMANDS);

    yield* h.writeBoth(".git/info/exclude", "*\n");
    yield* expectTurns(7, withProbe(IGNORED_TURN_COMMANDS), IGNORED_TURN_COMMANDS);
    yield* h.writeBoth(".git/info/exclude", "");
    yield* expectTurns(9, withProbe(STEADY_TURN_COMMANDS), STEADY_TURN_COMMANDS);

    // A new excludes file is a config change (snapshot reload) and a new probe; edits to it a probe.
    const excludes = path.join(h.root, "excludes");
    yield* fileSystem.writeFileString(excludes, "");
    yield* h.gitBoth(["config", "core.excludesFile", excludes]);
    const configured = yield* h.turn(11);
    assert.deepStrictEqual(
      [...configured.commands].sort(),
      [...withProbe(STEADY_TURN_COMMANDS), "config", "rev-parse", "rev-parse"].sort(),
    );
    yield* fileSystem.writeFileString(excludes, "*\n");
    yield* expectTurns(12, withProbe(IGNORED_TURN_COMMANDS), IGNORED_TURN_COMMANDS);
    yield* fileSystem.writeFileString(excludes, "# nothing\n");
    yield* expectTurns(14, withProbe(STEADY_TURN_COMMANDS), STEADY_TURN_COMMANDS);
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect("checkpoint capture in a subdirectory depends on the .gitignore files above it", () =>
  Effect.gen(function* () {
    const h = yield* makeTurnHarness("t3-checkpoint-subdir-");
    yield* h.writeBoth("sub/file.txt", "initial\n");
    yield* h.writeBoth("top.txt", "top\n");
    yield* h.gitBoth(["add", "."]);
    yield* h.commitBoth("initial");
    yield* h.turn(1, "sub");
    assert.deepStrictEqual((yield* h.turn(2, "sub")).commands, STEADY_TURN_COMMANDS);

    yield* h.writeBoth(".gitignore", "sub/\n");
    assert.deepStrictEqual((yield* h.turn(3, "sub")).commands, withProbe(IGNORED_TURN_COMMANDS));
    assert.deepStrictEqual((yield* h.turn(4, "sub")).commands, IGNORED_TURN_COMMANDS);
    yield* h.removeBoth(".gitignore");
    assert.deepStrictEqual((yield* h.turn(5, "sub")).commands, withProbe(STEADY_TURN_COMMANDS));

    // Edits below the subdirectory show up in its checkpoints; edits beside it do not.
    yield* h.writeBoth("sub/file.txt", "edited\n");
    yield* h.writeBoth("top.txt", "edited\n");
    const edited = yield* h.turn(6, "sub");
    assert.strictEqual(edited.numstat, "1\t1\tsub/file.txt\0");
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect("reads commit objects and packed-refs the way git log and rev-parse report them", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-commit-objects-" });
    const git = (args: ReadonlyArray<string>) =>
      driver
        .execute({ operation: "commit-object-test", cwd, args, env: FIXED_COMMIT_ENV })
        .pipe(Effect.map((result) => result.stdout.trim()));
    yield* git(["init", "-q", "-b", "main"]);
    yield* git(["config", "user.name", "Test"]);
    yield* git(["config", "user.email", "test@test.com"]);
    yield* git(["commit", "-q", "--allow-empty", "-m", "root"]);
    yield* git(["checkout", "-q", "-b", "side"]);
    yield* git(["commit", "-q", "--allow-empty", "-m", "side"]);
    yield* git(["checkout", "-q", "main"]);
    yield* git(["commit", "-q", "--allow-empty", "-m", "main"]);
    yield* git(["merge", "-q", "--no-ff", "-m", "merge", "side"]);
    const inflate = yield* Effect.promise(() => import("node:zlib"));
    for (const revision of ["HEAD", "HEAD^1", "HEAD^2"]) {
      const oid = yield* git(["rev-parse", revision]);
      const object = yield* fileSystem.readFile(
        path.join(cwd, ".git", "objects", oid.slice(0, 2), oid.slice(2)),
      );
      const facts = parseCommitObject(inflate.inflateSync(Buffer.from(object)));
      assert.isNotNull(facts);
      assert.strictEqual(
        `${facts!.tree} ${facts!.parents.join(" ")} ${facts!.committerTime}`,
        yield* git(["log", "-1", "--format=%T %P %ct", oid]),
      );
    }
    assert.isNull(parseCommitObject(Buffer.from("blob 5\0hello")));
    assert.isNull(parseCommitObject(Buffer.from("commit 3\0tree x")));

    yield* git(["tag", "-a", "-m", "tagged", "v1", "HEAD^1"]);
    yield* git(["update-ref", "refs/t3/checkpoints/one", "HEAD^2"]);
    yield* git(["update-ref", "refs/t3/checkpoints/two", "v1"]);
    yield* git(["pack-refs", "--all"]);
    const packed = yield* fileSystem.readFileString(path.join(cwd, ".git", "packed-refs"));
    assert.isTrue(packed.includes("\n^"));
    const refs = parsePackedRefs(packed, "refs/t3/", 2);
    assert.deepStrictEqual([...refs.refs!.entries()].sort(), [
      ["refs/t3/checkpoints/one", yield* git(["rev-parse", "HEAD^2"])],
      ["refs/t3/checkpoints/two", yield* git(["rev-parse", "v1"])],
    ]);
    assert.isFalse(refs.replaceRefs);
    // Beyond the budget the parse keeps nothing, so git answers; replace refs are still noticed.
    const oversized = parsePackedRefs(`${packed}${"0".repeat(40)} refs/replace/x\n`, "refs/t3/", 1);
    assert.deepStrictEqual(
      { refs: oversized.refs, replaceRefs: oversized.replaceRefs },
      {
        refs: null,
        replaceRefs: true,
      },
    );
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

// A seed repository both harness repositories fetch from, so their histories match commit for commit.
const seedRepository = Effect.fn("test.seedRepository")(function* (
  h: Effect.Success<ReturnType<typeof makeTurnHarness>>,
  commits: number,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const seed = path.join(h.root, "seed");
  yield* fileSystem.makeDirectory(seed);
  yield* h.gitIn(seed, ["init", "-q", "-b", "main"]);
  yield* h.gitIn(seed, ["config", "user.name", "Test"]);
  yield* h.gitIn(seed, ["config", "user.email", "test@test.com"]);
  for (let index = 1; index <= commits; index++) {
    yield* fileSystem.writeFileString(path.join(seed, "file.txt"), `version ${index}\n`);
    yield* h.gitIn(seed, ["add", "."]);
    yield* h.gitIn(seed, ["commit", "-q", "-m", `commit ${index}`], FIXED_COMMIT_ENV);
  }
  return seed;
});

it.effect("checkpoint commit facts come from git while the history is shallow", () =>
  Effect.gen(function* () {
    const h = yield* makeTurnHarness("t3-checkpoint-shallow-");
    const seed = yield* seedRepository(h, 3);
    yield* h.gitBoth(["fetch", "-q", "--depth=1", seed, "main"]);
    yield* h.gitBoth(["checkout", "-q", "-B", "main", "FETCH_HEAD"]);
    yield* h.turn(1);
    yield* h.turn(2);
    // The boundary commit has parents in its object but none for git.
    yield* h.gitBoth(["update-ref", turnRef(2), "HEAD"]);
    const boundary = yield* h.turn(3);
    assert.strictEqual(boundary.authored, null);
    assert.isTrue(boundary.commands.includes("log"));
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect("checkpoint commit facts come from git while grafts or replacements apply", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const h = yield* makeTurnHarness("t3-checkpoint-replace-");
    yield* h.writeBoth("file.txt", "initial\n");
    yield* h.gitBoth(["add", "."]);
    yield* h.commitBoth("initial");
    yield* h.turn(1);
    yield* h.writeBoth("file.txt", "edited\n");
    yield* h.turn(2);
    const commitOf = (cwd: string, ordinal: number) =>
      h
        .gitIn(cwd, ["rev-parse", `${turnRef(ordinal)}^{commit}`])
        .pipe(Effect.map((result) => result.stdout.trim()));

    // A graft removes checkpoint 2's parent for git: no authored paths for 2 → 3.
    for (const cwd of [h.repos.warm, h.repos.cold]) {
      yield* fileSystem.writeFileString(
        path.join(cwd, ".git", "info", "grafts"),
        `${yield* commitOf(cwd, 2)}\n`,
      );
    }
    const grafted = yield* h.turn(3);
    assert.strictEqual(grafted.authored, null);
    assert.isTrue(grafted.commands.includes("log"));
    yield* h.removeBoth(".git/info/grafts");

    // Replacing checkpoint 3 by checkpoint 1 gives git the initial tree: the diff 3 → 4 is the edit.
    for (const cwd of [h.repos.warm, h.repos.cold]) {
      yield* h.gitIn(cwd, ["replace", "-f", yield* commitOf(cwd, 3), yield* commitOf(cwd, 1)]);
    }
    const replaced = yield* h.turn(4);
    assert.strictEqual(replaced.numstat, "1\t1\tfile.txt\0");
    for (const cwd of [h.repos.warm, h.repos.cold]) {
      yield* h.gitIn(cwd, ["replace", "-d", yield* commitOf(cwd, 3)]);
    }
    const plain = yield* h.turn(5);
    assert.deepStrictEqual(plain.commands, STEADY_TURN_COMMANDS);
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect.skipIf(!symlinksSupported)(
  "a checkpoint ref kept as a symbolic link is resolved by git",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const h = yield* makeTurnHarness("t3-checkpoint-symref-");
      yield* h.writeBoth("file.txt", "initial\n");
      yield* h.gitBoth(["add", "."]);
      yield* h.commitBoth("initial");
      yield* h.turn(1);
      yield* h.turn(2);
      // Git reads a link's target as a ref name; a file read would follow it as a path and fail.
      for (const cwd of [h.repos.warm, h.repos.cold]) {
        const file = path.join(cwd, ".git", turnRef(2));
        yield* fileSystem.remove(file);
        yield* fileSystem.symlink("refs/heads/main", file);
      }
      const linked = yield* h.turn(3);
      assert.strictEqual(linked.baselineExists, true);
      assert.isTrue(linked.commands.includes("rev-parse"));
    }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect("packed-refs memory stays accounted under concurrent reloads", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const driver = yield* GitVcsDriver.makeVcsDriverShape();
    const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-packed-memory-" });
    const git = (args: ReadonlyArray<string>) =>
      driver
        .execute({ operation: "packed-memory-test", cwd, args, env: FIXED_COMMIT_ENV })
        .pipe(Effect.map((result) => result.stdout.trim()));
    yield* git(["init", "-q", "-b", "main"]);
    yield* git(["config", "user.name", "Test"]);
    yield* git(["config", "user.email", "test@test.com"]);
    yield* git(["commit", "-q", "--allow-empty", "-m", "root"]);
    const head = yield* git(["rev-parse", "HEAD"]);
    const commonDir = path.join(cwd, ".git");
    const memory = makeCheckpointRefMemory();
    const refs = Array.from({ length: 24 }, (_, index) => `refs/t3/memory/${index}`);
    for (const ref of refs) {
      yield* git(["update-ref", ref, head]);
      memory.rememberRef(commonDir, ref, head);
    }
    const resolveAll = Effect.forEach(refs, (ref) => memory.resolveRef(commonDir, ref), {
      concurrency: "unbounded",
    });
    const packedCount = Effect.map(
      fileSystem.readFileString(path.join(commonDir, "packed-refs")),
      (content) => parsePackedRefs(content, "refs/t3/", 1_000).refs!.size,
    );

    // Loose refs answer first; packing moves them to packed-refs, which every call reloads at once.
    assert.deepStrictEqual(
      yield* resolveAll,
      refs.map(() => head),
    );
    assert.strictEqual(memory.packedRefsEntries(), 0);
    yield* git(["pack-refs", "--all"]);
    assert.deepStrictEqual(
      yield* resolveAll,
      refs.map(() => head),
    );
    assert.strictEqual(memory.packedRefsEntries(), yield* packedCount);
    // A rewritten packed-refs is reloaded by every concurrent call; the count follows the file.
    yield* git(["update-ref", "refs/t3/memory/extra", head]);
    yield* git(["pack-refs", "--all"]);
    assert.deepStrictEqual(
      yield* resolveAll,
      refs.map(() => head),
    );
    assert.strictEqual(memory.packedRefsEntries(), yield* packedCount);
    assert.strictEqual(memory.packedRefsEntries(), refs.length + 1);
  }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

it.effect(
  "checkpoint refs in a reftable repository are resolved by git even when an include claims files",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const h = yield* makeTurnHarness("t3-checkpoint-reftable-", ["--ref-format=reftable"]);
      // Git before 2.45 cannot create a reftable repository.
      if (!h.initialized) return;
      // Git takes extensions from the repository's own config file only, not from includes.
      const include = path.join(h.root, "storage.inc");
      yield* fileSystem.writeFileString(include, "[extensions]\n\trefStorage = files\n");
      yield* h.gitBoth(["config", "include.path", include]);
      yield* h.writeBoth("file.txt", "initial\n");
      yield* h.gitBoth(["add", "."]);
      yield* h.commitBoth("initial");
      yield* h.turn(1);
      yield* h.turn(2);
      const third = yield* h.turn(3);
      assert.deepStrictEqual(
        { baseline: third.baselineExists, previous: third.previousExists },
        { baseline: true, previous: true },
      );
      assert.isTrue(third.commands.includes("rev-parse"));
      yield* h.gitBoth(["update-ref", "-d", turnRef(3)]);
      const recreated = yield* h.turn(4);
      assert.strictEqual(recreated.baselineExists, false);
    }).pipe(Effect.scoped, Effect.provide(layerGitCaptureContract)),
);

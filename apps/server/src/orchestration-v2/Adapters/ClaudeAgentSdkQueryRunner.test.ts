// @effect-diagnostics nodeBuiltinImport:off -- the fake CLI and its leftover child are real processes.
import * as ClaudeSdk from "@anthropic-ai/claude-agent-sdk";
import * as NodeChildProcess from "node:child_process";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeStream from "node:stream";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scheduler from "effect/Scheduler";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import * as HostProcess from "@t3tools/shared/HostProcess";
import { AgentScope } from "@t3tools/shared/AgentScope";
import { ProviderEventLoggers } from "@t3tools/provider-core/server/ProviderEventLoggers";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { signalProcessGroup } from "@t3tools/provider-core/server/processGroup";
import * as ServerConfig from "../../config.ts";
import * as PortScanner from "../../preview/PortScanner.ts";
import { HandoffProcess, type HandoffHost } from "../../process/handoffProcess.ts";
import * as ProviderProcesses from "../../process/providerProcesses.ts";
import { spawnerProcessIds } from "../../process/spawnerProcesses.ts";
import * as WorkerChildProcessSpawner from "../../process/workerChildProcessSpawner.ts";
import {
  commandsLeaveTheServer,
  withServerCommandSpawner,
} from "../../testUtils/serverCommandSpawner.ts";
import {
  ClaudeAgentSdkQueryRunner,
  type ClaudeAgentSdkQueryOpenInput,
  claudeProcessRead,
  layerQueryRunner,
  makeClaudeDescendantTracker,
  parseClaudeProcessTable,
  spawnClaudeProcessGroup,
} from "./ClaudeAdapterV2.ts";

vi.mock("@anthropic-ai/claude-agent-sdk", { spy: true });

const FORK_SESSION_ID = "11111111-1111-4111-8111-111111111111";

const sessionEntry = (uuid: string, parentUuid: string | null, message: unknown) => ({
  type: parentUuid === null ? "user" : "assistant",
  uuid,
  parentUuid,
  sessionId: FORK_SESSION_ID,
  cwd: "/repo",
  isSidechain: false,
  timestamp: "2026-10-01T00:00:00.000Z",
  message,
});

const writeClaudeSession = Effect.fnUntraced(function* (
  configDir: string,
  entries: ReadonlyArray<unknown> = [
    sessionEntry("aaaaaaaa-0000-4000-8000-000000000001", null, { role: "user", content: "hello" }),
    sessionEntry("aaaaaaaa-0000-4000-8000-000000000002", "aaaaaaaa-0000-4000-8000-000000000001", {
      role: "assistant",
      content: [{ type: "text", text: "hi" }],
    }),
  ],
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const projectDir = path.join(configDir, "projects", "-repo");
  yield* fileSystem.makeDirectory(projectDir, { recursive: true });
  yield* fileSystem.writeFileString(
    path.join(projectDir, `${FORK_SESSION_ID}.jsonl`),
    entries.map((line) => JSON.stringify(line)).join("\n"),
  );
  return projectDir;
});

const forkRunnerLayer = layerQueryRunner.pipe(
  Layer.provide(PortScanner.layerTest),
  Layer.provide(Layer.succeed(ProviderEventLoggers, { native: undefined, canonical: undefined })),
);

it.effect("forks a session stored under the instance's own CLAUDE_CONFIG_DIR", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const configDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-claude-fork-config-",
      });
      const projectDir = yield* writeClaudeSession(configDir);
      const hostConfigDir = (yield* HostProcess.Environment).CLAUDE_CONFIG_DIR;
      const inProcessFork = vi.spyOn(ClaudeSdk, "forkSession");
      yield* Effect.addFinalizer(() => Effect.sync(() => inProcessFork.mockRestore()));

      const runner = yield* ClaudeAgentSdkQueryRunner;
      const forked = yield* runner.forkSession({
        sessionId: FORK_SESSION_ID,
        options: { dir: "/repo" },
        environment: { ...process.env, CLAUDE_CONFIG_DIR: configDir },
        threadId: ThreadId.make("thread-fork-config-dir"),
        providerSessionId: ProviderSessionId.make("session-fork-config-dir"),
      });

      assert.notEqual(forked.sessionId, FORK_SESSION_ID);
      const transcript = yield* fileSystem.readFileString(
        path.join(projectDir, `${forked.sessionId}.jsonl`),
      );
      assert.include(transcript, '"content":"hello"');
      assert.include(transcript, '"text":"hi"');
      const inheritedHead = transcript
        .trim()
        .split("\n")
        .findLast((line) => line.includes('"type":"assistant"'))
        ?.match(/"uuid"\s*:\s*"([^"]+)"/)?.[1];
      assert.isString(inheritedHead);
      assert.equal(forked.headMessageId, inheritedHead);
      assert.equal((yield* HostProcess.Environment).CLAUDE_CONFIG_DIR, hostConfigDir);
      assert.equal(inProcessFork.mock.calls.length, 0);
    }).pipe(Effect.provide(forkRunnerLayer)),
  ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
it.effect("forked Claude sessions keep their compaction chain", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const configDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-claude-fork-compact-",
      });
      const uuid = (n: number) => `bbbbbbbb-0000-4000-8000-00000000000${n}`;
      const turn = (id: number, parent: string | null, role: "user" | "assistant") => ({
        ...sessionEntry(uuid(id), parent, {
          role,
          content: role === "user" ? `question ${id}` : [{ type: "text", text: `answer ${id}` }],
        }),
        type: role,
      });
      const projectDir = yield* writeClaudeSession(configDir, [
        turn(1, null, "user"),
        turn(2, uuid(1), "assistant"),
        turn(3, uuid(2), "user"),
        turn(4, uuid(3), "assistant"),
        {
          ...sessionEntry(uuid(5), null, undefined),
          type: "system",
          subtype: "compact_boundary",
          content: "Conversation compacted",
          logicalParentUuid: uuid(4),
          compactMetadata: {
            trigger: "manual",
            preTokens: 19979,
            preservedSegment: { headUuid: uuid(3), anchorUuid: uuid(6), tailUuid: uuid(4) },
            preservedMessages: {
              anchorUuid: uuid(6),
              uuids: [uuid(3), uuid(4)],
              allUuids: [uuid(3), uuid(4)],
            },
            postTokens: 3589,
          },
        },
        {
          ...turn(6, uuid(5), "user"),
          isCompactSummary: true,
          isVisibleInTranscriptOnly: true,
        },
        turn(7, uuid(4), "user"),
        turn(8, uuid(7), "assistant"),
      ]);

      const runner = yield* ClaudeAgentSdkQueryRunner;
      const forked = yield* runner.forkSession({
        sessionId: FORK_SESSION_ID,
        options: { dir: "/repo" },
        environment: { ...process.env, CLAUDE_CONFIG_DIR: configDir },
        threadId: ThreadId.make("thread-fork-compaction"),
        providerSessionId: ProviderSessionId.make("session-fork-compaction"),
      });

      const entries: ReadonlyArray<{
        readonly uuid?: string;
        readonly subtype?: string;
        readonly compactMetadata?: {
          readonly preservedSegment: Record<"headUuid" | "anchorUuid" | "tailUuid", string>;
          readonly preservedMessages: {
            readonly anchorUuid: string;
            readonly uuids: string[];
            readonly allUuids: string[];
          };
        };
      }> = (yield* fileSystem.readFileString(path.join(projectDir, `${forked.sessionId}.jsonl`)))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line));
      const forkedUuids = new Set(entries.map((entry) => entry.uuid));
      const boundary = entries.find((entry) => entry.subtype === "compact_boundary");
      assert.isDefined(boundary?.compactMetadata);
      const { preservedSegment, preservedMessages } = boundary!.compactMetadata!;
      const danglingRefs = [
        ...Object.values(preservedSegment),
        preservedMessages.anchorUuid,
        ...preservedMessages.uuids,
        ...preservedMessages.allUuids,
      ].filter((ref) => !forkedUuids.has(ref));
      assert.deepEqual(danglingRefs, [], "compaction refs missing from the forked transcript");
    }).pipe(Effect.provide(forkRunnerLayer)),
  ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

const realQueryRunner = layerQueryRunner.pipe(
  Layer.provide(PortScanner.layerTest),
  Layer.provide(
    Layer.succeed(ProviderEventLoggers, {
      native: undefined,
      canonical: undefined,
    }),
  ),
  Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);

const resumedMcpQueryInput = {
  threadId: ThreadId.make("thread-restart-registration"),
  providerSessionId: ProviderSessionId.make("session-restart-registration"),
  options: {
    resume: FORK_SESSION_ID,
    model: "claude-sonnet-4-6",
    tools: [],
    permissionMode: "default",
    mcpServers: {
      "t3-code": {
        type: "http",
        url: "http://localhost:3773/mcp",
        headers: { Authorization: "Bearer test-mcp-credential" },
      },
    },
  },
} satisfies ClaudeAgentSdkQueryOpenInput;

// The history worker runs from the server's command spawner, so a fork or a resume never
// forks the server itself.
const historyWorkerRoute = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const executable = yield* HostProcess.ExecutablePath;
    const { ambientCommands, serverCommands } = yield* withServerCommandSpawner(effect);
    return {
      ambient: ambientCommands.includes(executable),
      server: serverCommands.includes(executable),
    };
  });

it.effect.skipIf(!commandsLeaveTheServer)("forks a session from the server's command spawner", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const configDir = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "t3-claude-fork-spawner-",
      });
      yield* writeClaudeSession(configDir);
      const route = yield* historyWorkerRoute(
        Effect.gen(function* () {
          const runner = yield* ClaudeAgentSdkQueryRunner;
          yield* runner.forkSession({
            sessionId: FORK_SESSION_ID,
            options: { dir: "/repo" },
            environment: { ...process.env, CLAUDE_CONFIG_DIR: configDir },
            threadId: ThreadId.make("thread-fork-spawner"),
            providerSessionId: ProviderSessionId.make("session-fork-spawner"),
          });
        }).pipe(Effect.provide(forkRunnerLayer)),
      );
      assert.deepStrictEqual(route, { ambient: false, server: true });
    }),
  ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect.skipIf(!commandsLeaveTheServer)(
  "reads a resumed session's messages from the server's command spawner",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const configDir = yield* fileSystem.makeTempDirectoryScoped({
          prefix: "t3-claude-resume-spawner-",
        });
        yield* writeClaudeSession(configDir);
        const querySpy = vi
          .spyOn(ClaudeSdk, "query")
          .mockImplementation(() => ({ close: vi.fn() }) as unknown as ClaudeSdk.Query);
        yield* Effect.addFinalizer(() => Effect.sync(() => querySpy.mockRestore()));
        const route = yield* historyWorkerRoute(
          Effect.gen(function* () {
            const runner = yield* ClaudeAgentSdkQueryRunner;
            const { mcpServers: _mcpServers, ...options } = resumedMcpQueryInput.options;
            const session = yield* runner.open({
              ...resumedMcpQueryInput,
              resume: true,
              options: {
                ...options,
                cwd: "/repo",
                env: { ...process.env, CLAUDE_CONFIG_DIR: configDir },
              },
            });
            yield* session.close;
          }).pipe(Effect.provide(forkRunnerLayer)),
        );
        assert.equal(querySpy.mock.calls.length, 1);
        assert.deepStrictEqual(route, { ambient: false, server: true });
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);

it.effect("reopens a stalled MCP registration before delivering the resumed prompt", () =>
  Effect.gen(function* () {
    const registrationStarted = Promise.withResolvers<void>();
    const disposalStarted = Promise.withResolvers<void>();
    const finishDisposal = Promise.withResolvers<void>();
    const prompts: AsyncIterable<ClaudeSdk.SDKUserMessage>[] = [];
    const firstClose = vi.fn();
    const secondClose = vi.fn();
    const firstRegistration = vi.fn(() => {
      registrationStarted.resolve();
      return new Promise<ClaudeSdk.McpSetServersResult>(() => {});
    });
    const secondRegistration = vi.fn(async () => ({
      added: ["t3-code"],
      removed: [],
      errors: {},
    }));
    const querySpy = vi.spyOn(ClaudeSdk, "query").mockImplementation(({ prompt }) => {
      assert.notTypeOf(prompt, "string");
      if (typeof prompt === "string") throw new Error("Expected streaming input");
      prompts.push(prompt);
      return {
        initializationResult: async () => ({}),
        setMcpServers: prompts.length === 1 ? firstRegistration : secondRegistration,
        close: prompts.length === 1 ? firstClose : secondClose,
        [Symbol.asyncDispose]: async () => {
          firstClose();
          disposalStarted.resolve();
          await finishDisposal.promise;
        },
      } as unknown as ClaudeSdk.Query;
    });
    yield* Effect.addFinalizer(() => Effect.sync(() => querySpy.mockRestore()));
    const runner = yield* ClaudeAgentSdkQueryRunner;
    const opening = yield* runner.open(resumedMcpQueryInput).pipe(Effect.forkChild);
    yield* Effect.promise(() => registrationStarted.promise);
    yield* TestClock.adjust("90 seconds");
    yield* Effect.promise(() => disposalStarted.promise);
    assert.equal(querySpy.mock.calls.length, 1);
    finishDisposal.resolve();
    const session = yield* Fiber.join(opening);
    yield* Effect.addFinalizer(() => session.close.pipe(Effect.orDie));

    assert.equal(firstClose.mock.calls.length, 1);
    assert.equal(secondClose.mock.calls.length, 0);
    assert.equal(querySpy.mock.calls.length, 2);
    for (const [call] of querySpy.mock.calls) {
      assert.equal(call.options?.resume, FORK_SESSION_ID);
      assert.isUndefined(call.options?.mcpServers);
    }
    assert.deepEqual(secondRegistration.mock.calls, [[resumedMcpQueryInput.options.mcpServers]]);
    const message = {
      type: "user",
      message: { role: "user", content: "Continue where you left off." },
      parent_tool_use_id: null,
      session_id: FORK_SESSION_ID,
    } satisfies ClaudeSdk.SDKUserMessage;
    yield* session.offer(message);
    assert.isTrue((yield* Effect.promise(() => prompts[0]![Symbol.asyncIterator]().next())).done);
    const delivered = yield* Effect.promise(() => prompts[1]![Symbol.asyncIterator]().next());
    assert.deepEqual(delivered.value, message);
    assert.isFalse(delivered.done);
  }).pipe(Effect.provide(realQueryRunner)),
);

it.effect("fails a guarded prompt whose query closes while its submission waits", () =>
  Effect.gen(function* () {
    const querySpy = vi
      .spyOn(ClaudeSdk, "query")
      .mockImplementation(() => ({ close: vi.fn() }) as unknown as ClaudeSdk.Query);
    yield* Effect.addFinalizer(() => Effect.sync(() => querySpy.mockRestore()));
    const runner = yield* ClaudeAgentSdkQueryRunner;
    const { mcpServers: _mcpServers, ...options } = resumedMcpQueryInput.options;
    const session = yield* runner.open({ ...resumedMcpQueryInput, options });
    const waiting = yield* Deferred.make<void>();
    const claimed = yield* Deferred.make<void>();
    let queued = false;
    const offering = yield* session
      .offer(
        {
          type: "user",
          message: { role: "user", content: "Restart with this." },
          parent_tool_use_id: null,
          session_id: FORK_SESSION_ID,
        },
        () => {
          queued = true;
        },
        (enqueue) =>
          Deferred.succeed(waiting, undefined).pipe(
            Effect.andThen(Deferred.await(claimed)),
            Effect.andThen(Effect.sync(enqueue)),
          ),
      )
      .pipe(Effect.forkChild);
    yield* Deferred.await(waiting);
    yield* session.close;
    yield* Deferred.succeed(claimed, undefined);

    assert.isTrue(Exit.isFailure(yield* Fiber.await(offering)));
    assert.isFalse(queued);
  }).pipe(Effect.provide(realQueryRunner)),
);

it.effect.each([
  { case: "a readable rule list", errors: undefined, readable: true },
  { case: "a settings file Claude skipped", errors: [{ file: "settings.json" }], readable: false },
  { case: "a reply without a rule list", response: {}, readable: false },
])("reads Claude's permission rules only when no settings file was skipped: $case", (testCase) =>
  Effect.gen(function* () {
    const rules = [{ behavior: "deny" as const, rule: "mcp__probe__redeploy" }];
    const querySpy = vi.spyOn(ClaudeSdk, "query").mockImplementation(
      () =>
        ({
          close: vi.fn(),
          request: async () => ({
            response:
              "response" in testCase
                ? testCase.response
                : { state: { rules, errors: testCase.errors } },
          }),
        }) as unknown as ClaudeSdk.Query,
    );
    yield* Effect.addFinalizer(() => Effect.sync(() => querySpy.mockRestore()));
    const runner = yield* ClaudeAgentSdkQueryRunner;
    const { mcpServers: _mcpServers, ...options } = resumedMcpQueryInput.options;
    const session = yield* runner.open({ ...resumedMcpQueryInput, options });
    yield* Effect.addFinalizer(() => session.close.pipe(Effect.orDie));
    const exit = yield* Effect.exit(session.listPermissionRules);
    if (testCase.readable) assert.deepEqual(exit, Exit.succeed(rules));
    else assert.isTrue(Exit.isFailure(exit));
  }).pipe(Effect.provide(realQueryRunner)),
);

it.effect.each([true, false])("only retries after confirmed process exit: %s", (exits) =>
  Effect.gen(function* () {
    const registrationStarted = Promise.withResolvers<void>();
    const cleanupProgress = Promise.withResolvers<"kill" | "retry">();
    const events = new NodeEvents.EventEmitter();
    const stdin = new NodeStream.PassThrough();
    const stdout = new NodeStream.PassThrough();
    const child = {
      stdin,
      stdout,
      killed: false,
      exitCode: null,
      kill: vi.fn(() => {
        cleanupProgress.resolve("kill");
        return true;
      }),
      on: events.on.bind(events),
      once: events.once.bind(events),
      off: events.off.bind(events),
    } satisfies ClaudeSdk.SpawnedProcess;
    const signal = yield* Effect.abortSignal;
    const disposed = vi.fn(async () => {});
    const querySpy = vi.spyOn(ClaudeSdk, "query").mockImplementation(({ options }) => {
      const first = querySpy.mock.calls.length === 1;
      if (first) {
        options?.spawnClaudeCodeProcess?.({ command: "claude", args: [], env: {}, signal });
      } else {
        cleanupProgress.resolve("retry");
      }
      return {
        initializationResult: async () => ({}),
        setMcpServers: () => {
          registrationStarted.resolve();
          return first
            ? new Promise<ClaudeSdk.McpSetServersResult>(() => {})
            : Promise.resolve({ added: ["t3-code"], removed: [], errors: {} });
        },
        close: vi.fn(),
        [Symbol.asyncDispose]: disposed,
      } as unknown as ClaudeSdk.Query;
    });
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        querySpy.mockRestore();
        stdin.destroy();
        stdout.destroy();
      }),
    );
    const runner = yield* ClaudeAgentSdkQueryRunner;
    const opening = yield* runner
      .open({
        ...resumedMcpQueryInput,
        options: { ...resumedMcpQueryInput.options, spawnClaudeCodeProcess: () => child },
      })
      .pipe(Effect.exit, Effect.forkChild);
    yield* Effect.promise(() => registrationStarted.promise);
    yield* TestClock.adjust("90 seconds");

    assert.equal(yield* Effect.promise(() => cleanupProgress.promise), "kill");
    assert.equal(querySpy.mock.calls.length, 1);
    assert.deepEqual(child.kill.mock.calls, [["SIGKILL"]]);
    if (exits) events.emit("exit", null, "SIGKILL");
    else yield* TestClock.adjust("10 seconds");
    const result = yield* Fiber.join(opening);
    assert.equal(Exit.isSuccess(result), exits);
    if (Exit.isSuccess(result)) yield* result.value.close;
    assert.equal(querySpy.mock.calls.length, exits ? 2 : 1);
    assert.equal(disposed.mock.calls.length, 1);
  }).pipe(Effect.provide(realQueryRunner)),
);

it.effect("stops retrying MCP registration after two timeouts and reports the cause", () =>
  Effect.gen(function* () {
    const firstRegistration = Promise.withResolvers<void>();
    const secondRegistration = Promise.withResolvers<void>();
    const disposed = vi.fn(async () => {});
    const querySpy = vi.spyOn(ClaudeSdk, "query").mockImplementation(() => {
      const started = querySpy.mock.calls.length === 1 ? firstRegistration : secondRegistration;
      return {
        initializationResult: async () => ({}),
        setMcpServers: () => {
          started.resolve();
          return new Promise<ClaudeSdk.McpSetServersResult>(() => {});
        },
        [Symbol.asyncDispose]: disposed,
      } as unknown as ClaudeSdk.Query;
    });
    yield* Effect.addFinalizer(() => Effect.sync(() => querySpy.mockRestore()));
    const runner = yield* ClaudeAgentSdkQueryRunner;
    const opening = yield* runner.open(resumedMcpQueryInput).pipe(Effect.result, Effect.forkChild);
    yield* Effect.promise(() => firstRegistration.promise);
    yield* TestClock.adjust("90 seconds");
    yield* Effect.promise(() => secondRegistration.promise);
    yield* TestClock.adjust("90 seconds");
    const result = yield* Fiber.join(opening);

    assert.equal(result._tag, "Failure");
    if (result._tag !== "Failure") return;
    assert.equal(querySpy.mock.calls.length, 2);
    assert.equal(disposed.mock.calls.length, 2);
    assert.equal(
      makeProviderFailure({
        cause: { _tag: "ProviderAdapterTurnStartError", cause: result.failure },
      }).message,
      "Claude timed out while registering its MCP tools. Retry the turn; if it keeps failing, check Claude and the server logs.",
    );
  }).pipe(Effect.provide(realQueryRunner)),
);

it.effect("cancels a stalled registration without retrying or retaining its prompt queue", () =>
  Effect.gen(function* () {
    const registrationStarted = Promise.withResolvers<void>();
    const disposed = vi.fn(async () => {});
    const querySpy = vi.spyOn(ClaudeSdk, "query").mockImplementation(
      () =>
        ({
          initializationResult: async () => ({}),
          setMcpServers: () => {
            registrationStarted.resolve();
            return new Promise<ClaudeSdk.McpSetServersResult>(() => {});
          },
          [Symbol.asyncDispose]: disposed,
        }) as unknown as ClaudeSdk.Query,
    );
    yield* Effect.addFinalizer(() => Effect.sync(() => querySpy.mockRestore()));
    const runner = yield* ClaudeAgentSdkQueryRunner;
    const opening = yield* runner.open(resumedMcpQueryInput).pipe(Effect.forkChild);
    yield* Effect.promise(() => registrationStarted.promise);
    yield* Fiber.interrupt(opening);

    assert.equal(querySpy.mock.calls.length, 1);
    assert.equal(disposed.mock.calls.length, 1);
    const prompt = querySpy.mock.calls[0]![0].prompt;
    if (typeof prompt === "string") throw new Error("Expected streaming input");
    assert.isTrue((yield* Effect.promise(() => prompt[Symbol.asyncIterator]().next())).done);
  }).pipe(Effect.provide(realQueryRunner)),
);

it.effect("does not reuse a fresh session ID after MCP registration times out", () =>
  Effect.gen(function* () {
    const registrationStarted = Promise.withResolvers<void>();
    const disposed = vi.fn(async () => {});
    const querySpy = vi.spyOn(ClaudeSdk, "query").mockImplementation(
      () =>
        ({
          initializationResult: async () => ({}),
          setMcpServers: () => {
            registrationStarted.resolve();
            return new Promise<ClaudeSdk.McpSetServersResult>(() => {});
          },
          [Symbol.asyncDispose]: disposed,
        }) as unknown as ClaudeSdk.Query,
    );
    yield* Effect.addFinalizer(() => Effect.sync(() => querySpy.mockRestore()));
    const runner = yield* ClaudeAgentSdkQueryRunner;
    const { resume, ...options } = resumedMcpQueryInput.options;
    const opening = yield* runner
      .open({
        ...resumedMcpQueryInput,
        options: { ...options, sessionId: resume },
      })
      .pipe(Effect.result, Effect.forkChild);
    yield* Effect.promise(() => registrationStarted.promise);
    yield* TestClock.adjust("90 seconds");
    const result = yield* Fiber.join(opening);

    assert.equal(result._tag, "Failure");
    assert.equal(querySpy.mock.calls.length, 1);
    assert.equal(disposed.mock.calls.length, 1);
  }).pipe(Effect.provide(realQueryRunner)),
);

it.effect.each([
  ["opens after a slow CLI boot", "120 seconds", true],
  ["times out a CLI that never boots", "5 minutes", false],
] as const)("%s", ([, bootWait, boots]) =>
  Effect.gen(function* () {
    const waiting = Promise.withResolvers<void>();
    const booted = Promise.withResolvers<void>();
    const querySpy = vi.spyOn(ClaudeSdk, "query").mockImplementation(
      () =>
        ({
          initializationResult: () => {
            waiting.resolve();
            return booted.promise;
          },
          // The CLI answers control requests only after it has booted.
          setMcpServers: async () => {
            waiting.resolve();
            await booted.promise;
            return { added: ["t3-code"], removed: [], errors: {} };
          },
          close: vi.fn(),
          [Symbol.asyncDispose]: async () => {},
        }) as unknown as ClaudeSdk.Query,
    );
    yield* Effect.addFinalizer(() => Effect.sync(() => querySpy.mockRestore()));
    const runner = yield* ClaudeAgentSdkQueryRunner;
    const { resume, ...options } = resumedMcpQueryInput.options;
    const opening = yield* runner
      .open({ ...resumedMcpQueryInput, options: { ...options, sessionId: resume } })
      .pipe(Effect.result, Effect.forkChild);
    yield* Effect.promise(() => waiting.promise);
    yield* TestClock.adjust(bootWait);
    if (boots) booted.resolve();
    const result = yield* Fiber.join(opening);

    assert.equal(result._tag, boots ? "Success" : "Failure");
    if (result._tag === "Success") yield* result.success.close;
    else
      assert.equal(
        makeProviderFailure({
          cause: { _tag: "ProviderAdapterTurnStartError", cause: result.failure },
        }).message,
        "Claude timed out while starting up. Retry the turn; if it keeps failing, check Claude and the server logs.",
      );
  }).pipe(Effect.provide(realQueryRunner)),
);

// Answers every `ps` with `table()` at once (a failed `ps` when it is undefined, or when an
// effect succeeds), so the clock alone decides when the CLIs are polled.
const processTableSpawner = (
  table: () => string | undefined | Effect.Effect<string>,
  onRead: () => void = () => {},
) =>
  Layer.succeed(
    ChildProcessSpawner.ChildProcessSpawner,
    ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        if (command._tag === "StandardCommand" && command.command === "ps") onRead();
        const output = table();
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(output === undefined ? 1 : 0)),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.encodeText(
            typeof output === "object" ? Stream.fromEffect(output) : Stream.make(output ?? ""),
          ),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        });
      }),
    ),
  );

// Counts the CLIs started, so a test knows each one is being tracked.
const countedAgents = (count: number) => {
  let started = 0;
  const allStarted = Deferred.makeUnsafe<void>();
  const layer = Layer.mock(PortScanner.PortDiscovery)({
    trackAgent: () => {
      if (++started === count) Deferred.doneUnsafe(allStarted, Effect.void);
      return () => {};
    },
  });
  return { layer, allStarted: Deferred.await(allStarted) };
};

const openNumberedFakeCli = (cli: string, index: number) =>
  Effect.gen(function* () {
    const runner = yield* ClaudeAgentSdkQueryRunner;
    return yield* runner.open({
      threadId: ThreadId.make(`thread-fake-cli-${index}`),
      providerSessionId: ProviderSessionId.make(`session-fake-cli-${index}`),
      options: {
        sessionId: `native-session-fake-cli-${index}`,
        model: "claude-sonnet-4-6",
        tools: [],
        permissionMode: "default",
        pathToClaudeCodeExecutable: cli,
      },
    });
  });

function fakeClaudeCli(script: string): string {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-claude-cli-"));
  const cli = NodePath.join(dir, "claude");
  NodeFS.writeFileSync(cli, `#!/bin/sh\n${script}\n`, { mode: 0o755 });
  return cli;
}

const openFakeCli = (cli: string) =>
  Effect.gen(function* () {
    const runner = yield* ClaudeAgentSdkQueryRunner;
    return yield* runner.open({
      threadId: ThreadId.make("thread-fake-cli"),
      providerSessionId: ProviderSessionId.make("session-fake-cli"),
      options: {
        sessionId: "native-session-fake-cli",
        model: "claude-sonnet-4-6",
        tools: [],
        permissionMode: "default",
        pathToClaudeCodeExecutable: cli,
      },
    });
  });

describe.skipIf(HostProcess.Platform.defaultValue() === "win32")("Claude CLI process", () => {
  // The leftover child holds a FIFO open; its death produces EOF without polling.
  it.effect("stops what the CLI left running when the session closes", () =>
    Effect.gen(function* () {
      const cli = fakeClaudeCli(
        [
          'dir=$(dirname "$0")',
          'sleep 600 > "$dir/alive" &',
          'echo $! > "$dir/leftover.pid"',
          "cat > /dev/null",
        ].join("\n"),
      );
      const dir = NodePath.dirname(cli);
      NodeChildProcess.execFileSync("mkfifo", [NodePath.join(dir, "alive")]);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          try {
            process.kill(Number(NodeFS.readFileSync(NodePath.join(dir, "leftover.pid"), "utf8")));
          } catch {}
          NodeFS.rmSync(dir, { recursive: true, force: true });
        }),
      );
      const alive = NodeFS.createReadStream(NodePath.join(dir, "alive"));
      const leftoverStarted = new Promise<void>((resolve) => alive.once("open", () => resolve()));
      const leftoverStopped = new Promise<void>((resolve) => alive.once("close", () => resolve()));
      alive.resume();

      const stopped = yield* Deferred.make<string>();
      const session = yield* openFakeCli(cli).pipe(
        Effect.provide(
          realQueryRunner.pipe(
            Layer.provide(
              Layer.succeed(AgentScope, {
                ...AgentScope.defaultValue(),
                wrap: ({ command, args }) =>
                  Effect.succeed({ command, args, unit: "t3code-claude-test.scope" }),
                stop: (unit) => Deferred.succeed(stopped, unit).pipe(Effect.asVoid),
              }),
            ),
          ),
        ),
      );
      yield* Effect.promise(() => leftoverStarted);
      yield* session.close;
      yield* Effect.promise(() => leftoverStopped);
      assert.strictEqual(yield* Deferred.await(stopped), "t3code-claude-test.scope");
    }),
  );

  // The default AgentScope is what macOS gets: no systemd scope to catch a job outside the CLI's group.
  it.effect("stops a job the CLI started in its own process group when the session closes", () =>
    Effect.gen(function* () {
      const cli = fakeClaudeCli(
        [
          'dir=$(dirname "$0")',
          `perl -e 'setpgrp(0, 0); exec @ARGV' sleep 600 > "$dir/alive" &`,
          'echo $! > "$dir/leftover.pid"',
          "cat > /dev/null",
        ].join("\n"),
      );
      const dir = NodePath.dirname(cli);
      NodeChildProcess.execFileSync("mkfifo", [NodePath.join(dir, "alive")]);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          try {
            process.kill(Number(NodeFS.readFileSync(NodePath.join(dir, "leftover.pid"), "utf8")));
          } catch {}
          NodeFS.rmSync(dir, { recursive: true, force: true });
        }),
      );
      const alive = NodeFS.createReadStream(NodePath.join(dir, "alive"));
      const leftoverStarted = new Promise<void>((resolve) => alive.once("open", () => resolve()));
      const leftoverStopped = new Promise<void>((resolve) => alive.once("close", () => resolve()));
      alive.resume();

      const session = yield* openFakeCli(cli).pipe(Effect.provide(realQueryRunner));
      yield* Effect.promise(() => leftoverStarted);
      yield* session.close;
      yield* Effect.promise(() => leftoverStopped);
    }),
  );

  // Live clock: the job is found by polling while the CLI runs, and the CLI exits on its own.
  it.live("stops a job the CLI started in its own process group when the CLI exits", () =>
    Effect.gen(function* () {
      const cli = fakeClaudeCli(
        [
          'dir=$(dirname "$0")',
          `perl -e 'setpgrp(0, 0); exec @ARGV' sleep 600 > "$dir/alive" &`,
          'echo $! > "$dir/leftover.pid"',
          "sleep 3",
        ].join("\n"),
      );
      const dir = NodePath.dirname(cli);
      NodeChildProcess.execFileSync("mkfifo", [NodePath.join(dir, "alive")]);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          try {
            process.kill(Number(NodeFS.readFileSync(NodePath.join(dir, "leftover.pid"), "utf8")));
          } catch {}
          NodeFS.rmSync(dir, { recursive: true, force: true });
        }),
      );
      const alive = NodeFS.createReadStream(NodePath.join(dir, "alive"));
      const leftoverStopped = new Promise<void>((resolve) => alive.once("close", () => resolve()));
      alive.resume();

      yield* openFakeCli(cli).pipe(Effect.provide(realQueryRunner));
      yield* Effect.promise(() => leftoverStopped);
    }),
  );

  const fakeTableRunner = (
    spawner: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner>,
    agents: Layer.Layer<PortScanner.PortDiscovery>,
  ) =>
    layerQueryRunner.pipe(
      Layer.provide(agents),
      Layer.provide(
        Layer.succeed(ProviderEventLoggers, { native: undefined, canonical: undefined }),
      ),
      Layer.provide(spawner),
      Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    );

  // One `ps` per poll serves every CLI: per CLI, a host with hundreds of sessions forks hundreds a second.
  it.effect("reads the process table once per poll however many CLIs run", () => {
    let tableReads = 0;
    const agents = countedAgents(3);
    return Effect.gen(function* () {
      const sessions = yield* Effect.forEach([0, 1, 2], (index) => {
        const cli = fakeClaudeCli("cat > /dev/null");
        return Effect.addFinalizer(() =>
          Effect.sync(() => NodeFS.rmSync(NodePath.dirname(cli), { recursive: true, force: true })),
        ).pipe(Effect.andThen(openNumberedFakeCli(cli, index)));
      });
      yield* agents.allStarted;
      yield* TestClock.adjust("1 second");
      const before = tableReads;
      yield* TestClock.adjust("1 second");
      assert.strictEqual(tableReads - before, 1);
      yield* Effect.forEach(sessions, (session) => session.close);
    }).pipe(
      Effect.provide(
        fakeTableRunner(
          processTableSpawner(
            () => "",
            () => tableReads++,
          ),
          agents.layer,
        ),
      ),
    );
  });

  // The shared table names each CLI's own job, which only that CLI's exit stops: also after a
  // poll whose `ps` failed or came back empty, and once the job is re-parented.
  it.effect("stops the job each CLI moved out of its group, found by one shared poll", () => {
    const jobRows: Array<{ readonly pid: number | undefined; readonly cliPid: string }> = [];
    let read: "table" | "failed" | "empty" = "table";
    let reparented = false;
    let tableReads = 0;
    const agents = countedAgents(2);
    return Effect.gen(function* () {
      const dirs = [0, 1].map(() =>
        NodePath.dirname(
          fakeClaudeCli(
            ['dir=$(dirname "$0")', 'echo $$ > "$dir/pid"', 'read -r _ < "$dir/go"'].join("\n"),
          ),
        ),
      );
      for (const dir of dirs) {
        NodeChildProcess.execFileSync("mkfifo", [
          NodePath.join(dir, "pid"),
          NodePath.join(dir, "go"),
        ]);
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true })),
        );
      }
      const sessions = yield* Effect.forEach(dirs, (dir, index) =>
        openNumberedFakeCli(NodePath.join(dir, "claude"), index),
      );
      yield* agents.allStarted;
      const jobs = yield* Effect.forEach(dirs, (dir) =>
        Effect.gen(function* () {
          const cliPid = (yield* Effect.promise(() =>
            NodeFS.promises.readFile(NodePath.join(dir, "pid"), "utf8"),
          )).trim();
          // Its own group, so the CLI's group kill misses it.
          const job = NodeChildProcess.spawn("sleep", ["600"], { detached: true, stdio: "ignore" });
          yield* Effect.addFinalizer(() => Effect.sync(() => job.kill("SIGKILL")));
          jobRows.push({ pid: job.pid, cliPid });
          return job;
        }),
      );
      yield* TestClock.adjust("1 second");
      const before = tableReads;
      read = "failed";
      yield* TestClock.adjust("1 second");
      read = "empty";
      yield* TestClock.adjust("1 second");
      assert.strictEqual(tableReads - before, 2);
      read = "table";
      reparented = true;
      const stopped = jobs.map((job) => new Promise((resolve) => job.once("exit", resolve)));
      for (const dir of dirs) {
        yield* Effect.promise(() => NodeFS.promises.writeFile(NodePath.join(dir, "go"), "\n"));
      }
      yield* Effect.promise(() => Promise.all(stopped));
      yield* Effect.forEach(sessions, (session) => session.close);
    }).pipe(
      Effect.provide(
        fakeTableRunner(
          processTableSpawner(
            () =>
              read === "failed"
                ? undefined
                : read === "empty"
                  ? ""
                  : jobRows
                      .map(
                        ({ pid, cliPid }) =>
                          `${pid} ${reparented ? 1 : cliPid} ${pid} Sat Nov 14 22:00:00 2026`,
                      )
                      .join("\n"),
            () => tableReads++,
          ),
          agents.layer,
        ),
      ),
    );
  });

  // A close records what started since the last poll, but its read can begin before a poll that
  // ends first. The close's read sees an orphaned job with the child it just started; the poll sees
  // that job gone, the child orphaned, and a job started since. Exit stops both jobs it can reach.
  it.effect("stops at exit what a poll and the close's own earlier read each found", () => {
    const agents = countedAgents(1);
    const firstRead = Deferred.makeUnsafe<void>();
    const closeReadStarted = Deferred.makeUnsafe<void>();
    const closeReadHeld = Deferred.makeUnsafe<void>();
    let phase: "open" | "tree" | "orphaned" | "closing" | "polling" | "exiting" = "open";
    let cliPid = "";
    let jobPid: number | undefined;
    let childPid: number | undefined;
    // Never live pids: above Linux's largest pid_max.
    const shell = 4_194_400;
    const orphan = 4_194_401;
    const table = () => {
      const row = (pid: number | string | undefined, ppid: number | string, started: number) =>
        `${pid} ${ppid} ${pid} Sun Nov 15 10:00:0${started} 2026`;
      const cli = row(cliPid, 10, 0);
      switch (phase) {
        case "open":
          Deferred.doneUnsafe(firstRead, Effect.void);
          return cli;
        case "tree":
          return [cli, row(shell, cliPid, 1), row(orphan, shell, 1)].join("\n");
        case "orphaned":
          return [cli, row(orphan, 1, 1)].join("\n");
        case "closing": {
          Deferred.doneUnsafe(closeReadStarted, Effect.void);
          const seen = [cli, row(orphan, 1, 1), row(childPid, orphan, 2)].join("\n");
          return Deferred.await(closeReadHeld).pipe(Effect.as(seen));
        }
        case "polling":
          return [cli, row(jobPid, cliPid, 2), row(childPid, 1, 2)].join("\n");
        case "exiting":
          return [row(jobPid, 1, 2), row(childPid, 1, 2)].join("\n");
      }
    };
    return Effect.gen(function* () {
      const dir = NodePath.dirname(
        fakeClaudeCli(
          ['dir=$(dirname "$0")', 'echo $$ > "$dir/pid"', "cat > /dev/null"].join("\n"),
        ),
      );
      NodeChildProcess.execFileSync("mkfifo", [NodePath.join(dir, "pid")]);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true })),
      );
      const session = yield* openNumberedFakeCli(NodePath.join(dir, "claude"), 0);
      yield* agents.allStarted;
      cliPid = (yield* Effect.promise(() =>
        NodeFS.promises.readFile(NodePath.join(dir, "pid"), "utf8"),
      )).trim();
      yield* Deferred.await(firstRead);
      phase = "tree";
      yield* TestClock.adjust("1 second");
      phase = "orphaned";
      yield* TestClock.adjust("1 second");
      const sleeper = () =>
        NodeChildProcess.spawn("sleep", ["600"], { detached: true, stdio: "ignore" });
      const job = sleeper();
      const child = sleeper();
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => [job, child].forEach((spawned) => spawned.kill("SIGKILL"))),
      );
      jobPid = job.pid;
      childPid = child.pid;
      const stopped = [job, child].map(
        (spawned) => new Promise((resolve) => spawned.once("exit", resolve)),
      );
      phase = "closing";
      const closing = yield* session.close.pipe(Effect.forkChild);
      yield* Deferred.await(closeReadStarted);
      phase = "polling";
      yield* TestClock.adjust("1 second");
      phase = "exiting";
      Deferred.doneUnsafe(closeReadHeld, Effect.void);
      yield* Effect.promise(() => Promise.all(stopped));
      yield* Fiber.join(closing);
    }).pipe(Effect.provide(fakeTableRunner(processTableSpawner(table), agents.layer)));
  });

  const psTable = (rows: ReadonlyArray<readonly [pid: number, ppid: number, started: number]>) =>
    parseClaudeProcessTable(
      rows
        .map(([pid, ppid, started]) => `${pid} ${ppid} ${pid} Sun Nov 15 10:00:0${started} 2026`)
        .join("\n"),
      false,
    );
  const byNumber = (left: number, right: number) => left - right;

  // Each poll walked every job each CLI had ever run; with many long-lived CLIs that blocked the
  // server for hundreds of ms a poll.
  it("keeps only the descendants each CLI still has running, however many jobs it ran", () => {
    const clis = [1000, 2000, 3000];
    const trackers = clis.map(makeClaudeDescendantTracker);
    const jobs = (cli: number, poll: number) =>
      [0, 1, 2, 3].map((job) => cli + 100 + poll * 4 + job);
    for (let poll = 0; poll < 50; poll++) {
      const table = psTable(
        clis.flatMap((cli) => [
          [cli, 10, 0] as const,
          [cli + 1, cli, 0] as const,
          // A job that outlives the shell that started it is re-parented.
          ...(poll === 0 ? [[cli + 3, cli, 0] as const] : []),
          [cli + 2, poll === 0 ? cli + 3 : 1, 0] as const,
          ...jobs(cli, poll).map((pid) => [pid, cli + 1, 1] as const),
        ]),
      );
      for (const tracker of trackers) tracker.record(table);
    }
    assert.deepStrictEqual(
      trackers.map((tracker) => tracker.tracked.size),
      clis.map(() => 6),
    );
    assert.deepStrictEqual(
      trackers.map((tracker) =>
        [...tracker.tracked.values()].map((row) => row.pid).toSorted(byNumber),
      ),
      clis.map((cli) => [cli + 1, cli + 2, ...jobs(cli, 49)].toSorted(byNumber)),
    );
  });

  it("stops at exit only what the CLI left running, never a process that took a job's pid", () => {
    const tracker = makeClaudeDescendantTracker(1000);
    const orphan = [1300, 1, 1] as const;
    tracker.record(
      psTable([
        [1000, 10, 0],
        [1001, 1000, 0],
        [1100, 1001, 1],
        [1200, 1001, 1],
        [1300, 1001, 1],
      ]),
    );
    tracker.record(psTable([[1000, 10, 0], orphan]));
    // Unrelated processes take both finished jobs' pids, one in the second its job started.
    const unrelated = [
      [1100, 1, 2],
      [1200, 1, 1],
    ] as const;
    tracker.record(psTable([[1000, 10, 0], ...unrelated, orphan]));
    tracker.cliExited();
    assert.deepStrictEqual(
      tracker.leftovers(psTable([...unrelated, orphan])).map((row) => row.pid),
      [1300],
    );
  });

  // A read is interrupted whenever the last CLI's exit stops the poll. One left in flight would
  // mark every later read overlapped, and no CLI's record would be pruned again.
  it.effect("leaves no read in flight wherever a read is interrupted", () =>
    Effect.gen(function* () {
      for (let steps = 0; ; steps++) {
        const tasks: Array<() => void> = [];
        // Yields before every operation, so each task runs one and the test chooses where to stop.
        const stepper: Scheduler.Scheduler = {
          executionMode: "async",
          shouldYield: () => true,
          makeDispatcher: () => ({ scheduleTask: (task) => tasks.push(task), flush: () => {} }),
        };
        const inFlight = new Set<{ overlapped: boolean }>();
        const reading = yield* claudeProcessRead(inFlight, () => Effect.never).pipe(
          Effect.provideService(Scheduler.Scheduler, stepper),
          Effect.forkChild({ startImmediately: true }),
        );
        for (let step = 0; step < steps; step++) tasks.shift()?.();
        const reachedUse = tasks.length === 0;
        reading.interruptUnsafe();
        while (tasks.length > 0) tasks.shift()?.();
        const next = yield* claudeProcessRead(inFlight, (read) => Effect.succeed(read.overlapped));
        assert.isFalse(next, `a read interrupted after ${steps} steps stayed in flight`);
        if (reachedUse) break;
      }
    }),
  );

  // The server's command spawner runs `ps`, so polling never forks the server itself.
  it.effect("reads the process table through the server's command spawner", () =>
    Effect.gen(function* () {
      const cli = fakeClaudeCli("cat > /dev/null");
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(NodePath.dirname(cli), { recursive: true, force: true })),
      );
      const { ambientCommands, serverCommands } = yield* withServerCommandSpawner(
        Effect.gen(function* () {
          const session = yield* openFakeCli(cli);
          yield* session.close;
        }).pipe(
          Effect.provide(
            layerQueryRunner.pipe(
              Layer.provide(PortScanner.layerTest),
              Layer.provide(
                Layer.succeed(ProviderEventLoggers, { native: undefined, canonical: undefined }),
              ),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(
        { ambient: ambientCommands.includes("ps"), server: serverCommands.includes("ps") },
        { ambient: false, server: true },
      );
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  const rootRefusal =
    "--dangerously-skip-permissions cannot be used with root/sudo privileges for security reasons";

  const startupFailureMessage = (stderr: string) =>
    Effect.gen(function* () {
      const cli = fakeClaudeCli(`printf '%s\\n' '${stderr}' >&2\nexit 1`);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(NodePath.dirname(cli), { recursive: true, force: true })),
      );
      const session = yield* openFakeCli(cli);
      const exit = yield* Stream.runDrain(session.messages).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(exit));
      return Exit.isFailure(exit)
        ? makeProviderFailure({ cause: exit.cause, class: "transport_error" }).message
        : "";
    });

  // Startup stderr can carry paths, tokens, or settings, so only the known
  // refusal reaches the failure the client shows.
  it.effect("keeps other startup stderr out of the turn failure", () =>
    Effect.gen(function* () {
      const surrounded = yield* startupFailureMessage(
        `token=sk-private-123\n${rootRefusal}\nat /home/private/settings.json`,
      );
      assert.include(surrounded, rootRefusal);
      assert.notInclude(surrounded, "sk-private-123");
      assert.notInclude(surrounded, "/home/private");

      const unknown = yield* startupFailureMessage("token=sk-private-123");
      assert.notInclude(unknown, "sk-private-123");

      const missingConversation = yield* startupFailureMessage(
        "No conversation found with session ID: sk-private-123",
      );
      assert.include(missingConversation, "could not find this conversation");
      assert.notInclude(missingConversation, "sk-private-123");
    }).pipe(Effect.provide(realQueryRunner)),
  );

  // What Claude Code writes when the session store behind --resume is gone.
  it.effect("names a resumed conversation Claude Code cannot find", () =>
    Effect.gen(function* () {
      const missing = "No conversation found with session ID: native-session-fake-cli";
      const result = JSON.stringify({
        type: "result",
        subtype: "error_during_execution",
        is_error: true,
        num_turns: 0,
        duration_ms: 0,
        duration_api_ms: 0,
        total_cost_usd: 0,
        usage: {},
        modelUsage: {},
        permission_denials: [],
        errors: [missing],
        session_id: "native-session-fake-cli",
        uuid: "00000000-0000-4000-8000-000000000000",
      });
      const cli = fakeClaudeCli(
        `printf '%s\\n' '${result}'\nprintf '%s\\n' '${missing}' >&2\nexit 1`,
      );
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(NodePath.dirname(cli), { recursive: true, force: true })),
      );
      const session = yield* openFakeCli(cli);
      const exit = yield* Stream.runDrain(session.messages).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        assert.include(
          makeProviderFailure({ cause: exit.cause, class: "transport_error" }).message,
          "could not find this conversation",
        );
      }
    }).pipe(Effect.provide(realQueryRunner)),
  );

  // The provider launcher (hand-off on Linux, or Node's spawner when it is off) carries the
  // CLI's stderr as Node's own spawn does, so the refusal reads the same however it started.
  it.live("reports the CLI's root refusal the same whichever launcher starts the CLI", () => {
    const launcherRunner = (knobs: Record<string, string>) =>
      layerQueryRunner.pipe(
        Layer.provide(PortScanner.layerTest),
        Layer.provide(
          Layer.succeed(ProviderEventLoggers, { native: undefined, canonical: undefined }),
        ),
        Layer.provide(
          WorkerChildProcessSpawner.layerProviderLauncher.pipe(
            Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-claude-refusal-" })),
            Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(knobs))),
          ),
        ),
        Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
      );
    const message = <E>(runner: Layer.Layer<ClaudeAgentSdkQueryRunner, E>) =>
      startupFailureMessage(rootRefusal).pipe(Effect.scoped, Effect.provide(runner));
    return Effect.gen(function* () {
      const nodeSpawner = yield* message(realQueryRunner);
      assert.include(nodeSpawner, rootRefusal);
      const handedOff = yield* message(launcherRunner({}));
      const launcherDirect = yield* message(
        launcherRunner({ [WorkerChildProcessSpawner.PROVIDER_SPAWNER_ENV]: "direct" }),
      );
      assert.deepStrictEqual(
        { handedOff, launcherDirect },
        { handedOff: nodeSpawner, launcherDirect: nodeSpawner },
      );
    });
  });

  // The CLI keeps running its turn after its pipes close, so a server that
  // crashed would leave it working next to the session it resumes.
  it.effect.each([
    { launcher: "none" },
    ...(HostProcess.Platform.defaultValue() === "linux" ? [{ launcher: "provider" }] : []),
  ] as const)(
    "stops the CLI when the server that started it goes away ($launcher launcher)",
    ({ launcher }) =>
      Effect.gen(function* () {
        const cli = fakeClaudeCli(['dir=$(dirname "$0")', 'sleep 600 > "$dir/alive"'].join("\n"));
        const dir = NodePath.dirname(cli);
        NodeChildProcess.execFileSync("mkfifo", [NodePath.join(dir, "alive")]);
        const providerLauncher =
          launcher === "none"
            ? undefined
            : yield* WorkerChildProcessSpawner.ProviderProcessLauncher;
        const child = spawnClaudeProcessGroup(
          {
            command: cli,
            args: [],
            cwd: dir,
            env: process.env,
            // @effect-diagnostics-next-line abortControllerInEffect:off - This crash fixture closes pipes itself; aborting on test teardown raises an unrelated process error.
            signal: new AbortController().signal,
          },
          undefined,
          undefined,
          undefined,
          undefined,
          providerLauncher,
        ) as NodeChildProcess.ChildProcess & { readonly held?: ReadonlyArray<NodeStream.Duplex> };
        // Its descriptors are the server's once it has started (a handed-off CLI can open its
        // fifo before the spawner's answer reaches the server).
        const started = new Promise<void>((resolve) => child.once("spawn", () => resolve()));
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            try {
              signalProcessGroup(child.pid ?? 0, "SIGKILL");
            } catch {}
            NodeFS.rmSync(dir, { recursive: true, force: true });
          }),
        );
        const alive = NodeFS.createReadStream(NodePath.join(dir, "alive"));
        const turnStarted = new Promise<void>((resolve) => alive.once("open", () => resolve()));
        const turnStopped = new Promise<void>((resolve) => alive.once("close", () => resolve()));
        alive.resume();
        yield* Effect.promise(() => Promise.all([started, turnStarted]));

        // A dying server's descriptors close; the kernel would do this for it.
        for (const pipe of [...child.stdio, ...(child.held ?? [])]) pipe?.destroy();
        yield* Effect.promise(() => turnStopped);
      }).pipe(
        Effect.provide(
          WorkerChildProcessSpawner.layerProviderLauncher.pipe(
            Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-claude-handoff-" })),
            Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
            Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          ),
        ),
      ),
  );

  // Linux: a handed-off CLI is known by its PID and /proc start time. Its exit can be learned late
  // (its spawner died), when its PID may name another process: that one's group is not signalled.
  it.live.each(
    HostProcess.Platform.defaultValue() === "linux"
      ? ([
          { pid: "still names the CLI", reused: false },
          { pid: "names another process", reused: true },
        ] as const)
      : [],
  )("kills the group of an exited, handed-off CLI only while its PID $pid", ({ reused }) =>
    Effect.gen(function* () {
      // Leads its own group, as the CLI's group would, or as a process that took its PID might.
      const leader = NodeChildProcess.spawn("sleep", ["60"], { detached: true, stdio: "ignore" });
      yield* Effect.promise(() => new Promise((resolve) => leader.once("spawn", resolve)));
      yield* Effect.addFinalizer(() => Effect.sync(() => leader.kill("SIGKILL")));
      const pid = leader.pid!;
      const stat = NodeFS.readFileSync(`/proc/${pid}/stat`, "utf8");
      const startTime = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19]!;
      const exitListeners = new Set<(code: number | null, signal: NodeJS.Signals | null) => void>();
      // A spawner host that hands off nothing: it names the leader as the CLI it started.
      const host: HandoffHost = {
        spawnHandoff: (_command, _args, _options, _request, callbacks) => {
          queueMicrotask(() => callbacks.onSpawn());
          return {
            id: 1,
            pid,
            startTime: reused ? `${startTime}0` : startTime,
            sockets: new Map(),
            exitSignal: Deferred.makeUnsafe(),
            exitListeners,
            orphaned: true,
            referenced: true,
            released: false,
          };
        },
        abandonHandoff: () => {},
        killHandoff: (_child, _signal, callback) => callback({ _tag: "Sent", killed: false }),
        setHandoffReferenced: () => {},
      };
      const child = spawnClaudeProcessGroup(
        {
          command: "claude",
          args: [],
          cwd: process.cwd(),
          env: process.env,
          // @effect-diagnostics-next-line abortControllerInEffect:off - The SDK's spawn options carry one; nothing aborts here.
          signal: new AbortController().signal,
        },
        undefined,
        undefined,
        undefined,
        undefined,
        WorkerChildProcessSpawner.ProviderProcessLauncher.of({
          spawn: (command, args, options) =>
            new HandoffProcess(host, command, args, options, () => {}),
          spawner: undefined as never,
        }),
      );
      yield* Effect.promise(
        () => new Promise((resolve) => (child as unknown as HandoffProcess).once("spawn", resolve)),
      );
      // Its exit as the server reports one it learned by watching: no status.
      for (const listener of exitListeners) listener(null, null);
      const killed = yield* Effect.callback<boolean>((resume) => {
        if (leader.exitCode !== null || leader.signalCode !== null) {
          return resume(Effect.succeed(true));
        }
        leader.once("exit", () => resume(Effect.succeed(true)));
      }).pipe(Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.succeed(false) }));
      assert.strictEqual(killed, !reused);
    }).pipe(Effect.scoped),
  );

  // Linux: a provider handed off by the spawner process identifies by /proc start time.
  it.live.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
    "starts the CLI from the server's provider launcher, not from the server",
    () => {
      const runner = layerQueryRunner.pipe(
        Layer.provide(PortScanner.layerTest),
        Layer.provide(
          Layer.succeed(ProviderEventLoggers, { native: undefined, canonical: undefined }),
        ),
        Layer.provide(
          WorkerChildProcessSpawner.layerProviderLauncher.pipe(
            Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-claude-handoff-" })),
            Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
          ),
        ),
        Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
      );
      return Effect.gen(function* () {
        const cli = fakeClaudeCli(
          ['dir=$(dirname "$0")', 'printf "%s" "$PPID" > "$dir/parent"', "cat > /dev/null"].join(
            "\n",
          ),
        );
        const dir = NodePath.dirname(cli);
        NodeChildProcess.execFileSync("mkfifo", [NodePath.join(dir, "parent")]);
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true })),
        );
        const parentRead = new Promise<string>((resolve) => {
          let parent = "";
          NodeFS.createReadStream(NodePath.join(dir, "parent"), "utf8")
            .on("data", (chunk) => (parent += chunk))
            .once("end", () => resolve(parent));
        });
        const session = yield* openFakeCli(cli);
        const parent = Number(yield* Effect.promise(() => parentRead));
        assert.isTrue(spawnerProcessIds.has(parent), `parent ${parent} is not a spawner`);
        yield* session.close;
      }).pipe(Effect.scoped, Effect.provide(runner));
    },
  );

  // A native cwd makes libuv wait for chdir, which a hung mount stalls with the whole server.
  it.effect("enters the cwd in the child, so a missing one fails the CLI and not the spawn", () =>
    Effect.gen(function* () {
      const cli = fakeClaudeCli('pwd -P > "$(dirname "$0")/cwd"');
      const dir = NodePath.dirname(cli);
      const workspace = NodePath.join(dir, "work space");
      NodeFS.mkdirSync(workspace);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true })),
      );
      const exitCode = (cwd: string) =>
        Effect.promise(
          () =>
            new Promise<number | null>((resolve) =>
              spawnClaudeProcessGroup({
                command: cli,
                args: [],
                cwd,
                env: process.env,
                signal: new AbortController().signal,
              }).once("exit", resolve),
            ),
        );
      assert.equal(yield* exitCode(workspace), 0);
      assert.equal(
        NodeFS.readFileSync(NodePath.join(dir, "cwd"), "utf8"),
        `${NodeFS.realpathSync(workspace)}\n`,
      );
      assert.equal(yield* exitCode(NodePath.join(dir, "missing")), 125);
    }),
  );

  // A restarted server reaps recorded CLIs, so a CLI must never run before its record exists,
  // however it starts: Node's spawner, the provider launcher's, or hand-off from the spawner.
  it.effect.each(["none", "direct", "handoff"] as const)(
    "records the CLI before it runs and forgets it once it exits (%s launcher)",
    (launcherKind) =>
      Effect.gen(function* () {
        const node = yield* ChildProcessSpawner.ChildProcessSpawner;
        const providerLauncher =
          launcherKind === "none"
            ? undefined
            : launcherKind === "direct"
              ? yield* WorkerChildProcessSpawner.ProviderProcessLauncher.pipe(
                  Effect.provide(
                    WorkerChildProcessSpawner.layerProviderLauncher.pipe(
                      Layer.provide(
                        ServerConfig.layerTest(process.cwd(), { prefix: "t3-claude-record-" }),
                      ),
                      Layer.provide(
                        ConfigProvider.layer(
                          ConfigProvider.fromUnknown({
                            [WorkerChildProcessSpawner.PROVIDER_SPAWNER_ENV]: "direct",
                          }),
                        ),
                      ),
                    ),
                  ),
                )
              : yield* WorkerChildProcessSpawner.makeProcessSpawnerWithHost(node).pipe(
                  Effect.flatMap(({ host }) =>
                    WorkerChildProcessSpawner.makeProviderLauncher(node, host!),
                  ),
                );
        const cli = fakeClaudeCli(
          [
            'dir=$(dirname "$0")',
            'printf \'%s %s\' "$$" "$(cat "$dir/record")" > "$dir/seen"',
          ].join("\n"),
        );
        const dir = NodePath.dirname(cli);
        NodeChildProcess.execFileSync("mkfifo", [NodePath.join(dir, "seen")]);
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true })),
        );
        const forgotten = yield* Deferred.make<void>();
        let registered = 0;
        spawnClaudeProcessGroup(
          {
            command: cli,
            args: [],
            cwd: dir,
            env: process.env,
            // @effect-diagnostics-next-line abortControllerInEffect:off - The CLI exits on its own.
            signal: new AbortController().signal,
          },
          undefined,
          undefined,
          undefined,
          (pid) => {
            registered = pid;
            NodeFS.writeFileSync(NodePath.join(dir, "record"), String(pid));
            return () => Deferred.doneUnsafe(forgotten, Effect.void);
          },
          providerLauncher,
        );
        const seen = yield* Effect.promise(() =>
          NodeFS.promises.readFile(NodePath.join(dir, "seen"), "utf8"),
        );
        assert.equal(seen, `${registered} ${registered}`);
        assert.notEqual(registered, 0);
        yield* Deferred.await(forgotten);
      }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  );

  // Linux: a handed-off CLI is the spawner's child; the server records it by the spawner's start time.
  it.live.skipIf(HostProcess.Platform.defaultValue() !== "linux")(
    "records a handed-off CLI in the server's own provider record until it exits",
    () =>
      Effect.gen(function* () {
        const cli = fakeClaudeCli("cat > /dev/null");
        const dir = NodePath.dirname(cli);
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true })),
        );
        const node = yield* ChildProcessSpawner.ChildProcessSpawner;
        const { host } = yield* WorkerChildProcessSpawner.makeProcessSpawnerWithHost(node);
        const launcher = yield* WorkerChildProcessSpawner.makeProviderLauncher(node, host!);
        const { register } = Context.get(
          yield* Layer.build(ProviderProcesses.layer),
          ProviderProcesses.ProviderProcesses,
        );
        const directory = NodePath.join(
          (yield* ServerConfig.ServerConfig).stateDir,
          "provider-processes",
        );
        const recorded = () =>
          NodeFS.readdirSync(directory).flatMap((file) =>
            (
              JSON.parse(NodeFS.readFileSync(NodePath.join(directory, file), "utf8")) as {
                readonly processes: ReadonlyArray<{ readonly pid: number }>;
              }
            ).processes.map((entry) => entry.pid),
          );
        const child = spawnClaudeProcessGroup(
          {
            command: cli,
            args: [],
            cwd: dir,
            env: process.env,
            // @effect-diagnostics-next-line abortControllerInEffect:off - The CLI exits once its stdin ends.
            signal: new AbortController().signal,
          },
          undefined,
          undefined,
          undefined,
          register,
          launcher,
        ) as unknown as HandoffProcess;
        yield* Effect.promise(() => new Promise((resolve) => child.once("spawn", resolve)));
        assert.deepStrictEqual(recorded(), [child.pid]);
        const exited = new Promise((resolve) => child.once("exit", resolve));
        child.stdin!.end();
        yield* Effect.promise(() => exited);
        assert.deepStrictEqual(recorded(), []);
      }).pipe(
        Effect.scoped,
        Effect.provide(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-claude-recorded-" }).pipe(
            Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          ),
        ),
      ),
  );

  it.effect("marks the CLI's jobs with its thread and counts it as an agent until it exits", () => {
    let runningAgents = 0;
    let releaseAgent!: () => void;
    const agentReleased = new Promise<void>((resolve) => (releaseAgent = resolve));
    const runner = layerQueryRunner.pipe(
      Layer.provide(
        Layer.mock(PortScanner.PortDiscovery)({
          trackAgent: () => {
            runningAgents += 1;
            return () => {
              runningAgents -= 1;
              releaseAgent();
            };
          },
        }),
      ),
      Layer.provide(
        Layer.succeed(ProviderEventLoggers, { native: undefined, canonical: undefined }),
      ),
      Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    );
    return Effect.gen(function* () {
      const cli = fakeClaudeCli(
        [
          'dir=$(dirname "$0")',
          `printf '%s' "$${PortScanner.AGENT_THREAD_ENV}" > "$dir/thread"`,
          "cat > /dev/null",
        ].join("\n"),
      );
      const dir = NodePath.dirname(cli);
      NodeChildProcess.execFileSync("mkfifo", [NodePath.join(dir, "thread")]);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(dir, { recursive: true, force: true })),
      );
      const threadRead = new Promise<string>((resolve) => {
        let thread = "";
        NodeFS.createReadStream(NodePath.join(dir, "thread"), "utf8")
          .on("data", (chunk) => (thread += chunk))
          .once("end", () => resolve(thread));
      });

      const session = yield* openFakeCli(cli);
      assert.equal(yield* Effect.promise(() => threadRead), "thread-fake-cli");
      assert.equal(runningAgents, 1);
      yield* session.close;
      yield* Effect.promise(() => agentReleased);
      assert.equal(runningAgents, 0);
    }).pipe(Effect.provide(runner));
  });
});

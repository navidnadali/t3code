import type * as NodeWorkerThreads from "node:worker_threads";

import {
  type CanUseTool,
  type HookCallback,
  type HookCallbackMatcher,
  InMemorySessionStore,
  type McpServerConfig,
  type Options as ClaudeQueryOptions,
  type SDKMessage,
  type SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderSessionId, ThreadId } from "@t3tools/contracts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import * as NetService from "@t3tools/shared/Net";
import { ProviderEventLoggers } from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as Deferred from "effect/Deferred";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import * as ProviderIoWorkerPool from "../../provider/io/ProviderIoWorkerPool.ts";
import { resolveServerConfig } from "../../cli/config.ts";
import * as ServerConfig from "../../config.ts";
import * as PortScanner from "../../preview/PortScanner.ts";
import {
  ClaudeAgentSdkQueryRunner,
  ClaudeAgentSdkQueryRunnerError,
  type ClaudeAgentSdkQueryOpenInput,
  type ClaudeAgentSdkQuerySession,
  layerQueryRunner,
  makeClaudeQueryOptions,
} from "./ClaudeAdapterV2.ts";
import { CLAUDE_MODEL_SELECTION } from "../testkit/fixtures/shared.ts";
import {
  FakeClaudeCli,
  type FakeClaudeCliOptions,
  normalizedFrames,
} from "./ClaudeAgentSdkFakeCli.testkit.ts";
import {
  ClaudeAgentSdkWorkerPool,
  claudeAgentSdkWorkerSetup,
  layerClaudeAgentSdkWorkerPool,
  startClaudeQueryInWorker,
} from "./ClaudeAgentSdkWorkerQuery.ts";
import type {
  ClaudeAgentSdkWorkerEvent,
  ClaudeAgentSdkWorkerRequest,
} from "../../provider/io/claudeAgentSdkProtocol.ts";

const workers = vi.hoisted(() => ({
  started: new Array<NodeWorkerThreads.Worker>(),
  opened: new Array<{
    readonly worker: NodeWorkerThreads.Worker;
    readonly session: number;
    readonly stage: unknown;
  }>(),
  onBytes: undefined as ((chunk: Uint8Array) => void) | undefined,
}));

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      super(...args);
      workers.started.push(this);
    }
    override postMessage(...args: Parameters<NodeWorkerThreads.Worker["postMessage"]>) {
      const [message] = args as [
        { readonly type?: unknown; readonly session?: unknown; readonly stage?: unknown },
      ];
      if (message.type === "open" && typeof message.session === "number") {
        workers.opened.push({ worker: this, session: message.session, stage: message.stage });
      }
      if (message.type === "bytes") workers.onBytes?.((message as { chunk: Uint8Array }).chunk);
      super.postMessage(...args);
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

type Mode = "thread" | "worker";

const SESSION_ID = "11111111-2222-4333-8444-555555555555";

interface Recorded {
  readonly opens: Array<boolean>;
  readonly requests: Array<string>;
  /** Called with each event's type as it reaches main. */
  onEvent?: (type: string) => void;
}

// Records what crosses to the worker, so a test can tell the worker ran the query.
const recordingPool = (
  pool: ProviderIoWorkerPool.ProviderIoWorkerPool["Service"],
  recorded: Recorded,
): ProviderIoWorkerPool.ProviderIoWorkerPool["Service"] => ({
  open: (input) =>
    pool
      .open({
        ...input,
        onEvent: (event) => {
          input.onEvent?.(event);
          recorded.onEvent?.((event as { readonly type: string }).type);
        },
      })
      .pipe(
        Effect.map((opened) => {
          recorded.opens.push(Option.isSome(opened));
          return Option.map(opened, (session) => ({
            ...session,
            call: (request) => {
              recorded.requests.push((request as { readonly type: string }).type);
              return session.call(request);
            },
            notify: (request) => {
              recorded.requests.push((request as { readonly type: string }).type);
              return session.notify(request);
            },
          }));
        }),
      ),
});

interface Harness {
  readonly runner: ClaudeAgentSdkQueryRunner["Service"];
  readonly logs: Array<unknown>;
  readonly recorded: Recorded;
}

const withRunner = <A, E>(
  mode: Mode,
  use: (harness: Harness) => Effect.Effect<A, E, Scope.Scope>,
  options: { readonly workers?: number } = {},
) =>
  Effect.gen(function* () {
    const recorded: Recorded = { opens: [], requests: [] };
    const logs: Array<unknown> = [];
    const pool =
      mode === "worker"
        ? recordingPool(
            yield* ProviderIoWorkerPool.make({ workers: options.workers ?? 2 }),
            recorded,
          )
        : undefined;
    const runnerLayer = layerQueryRunner.pipe(
      Layer.provide(PortScanner.layerTest),
      Layer.provide(
        Layer.succeed(ProviderEventLoggers, {
          native: {
            filePath: "/dev/null",
            write: (event) => Effect.sync(() => logs.push(event)),
            close: () => Effect.void,
          },
          canonical: undefined,
        }),
      ),
      Layer.provide(Layer.succeed(ClaudeAgentSdkWorkerPool, pool)),
      Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    );
    return yield* Effect.gen(function* () {
      return yield* use({ runner: yield* ClaudeAgentSdkQueryRunner, logs, recorded });
    }).pipe(Effect.provide(runnerLayer));
  }).pipe(Effect.scoped);

const openInput = (
  cli: FakeClaudeCli,
  options: Partial<ClaudeAgentSdkQueryOpenInput["options"]> = {},
): ClaudeAgentSdkQueryOpenInput => ({
  threadId: ThreadId.make("thread-worker-query"),
  providerSessionId: ProviderSessionId.make("session-worker-query"),
  options: {
    sessionId: SESSION_ID,
    model: "claude-sonnet-4-6",
    tools: [],
    permissionMode: "default",
    pathToClaudeCodeExecutable: "claude",
    spawnClaudeCodeProcess: cli.spawn,
    ...options,
  } as ClaudeAgentSdkQueryOpenInput["options"],
});

const prompt = (text: string): SDKUserMessage => ({
  type: "user",
  message: { role: "user", content: text },
  parent_tool_use_id: null,
  session_id: SESSION_ID,
});

const LARGE_TOOL_OUTPUT = "x".repeat(256 * 1024);

// One turn the size of the bench's large-tool-payload one: deltas, a 256 KiB tool result, a result.
const playTurn = (cli: FakeClaudeCli, turn: number) => {
  cli.write({
    type: "system",
    subtype: "init",
    session_id: SESSION_ID,
    uuid: `init-${turn}`,
    model: "claude-sonnet-4-6",
    permissionMode: "default",
    cwd: "/repo",
    tools: [],
    mcp_servers: [],
  });
  for (let delta = 0; delta < 64; delta++) {
    cli.write({
      type: "stream_event",
      event: {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: `turn ${turn} delta ${delta} ` },
      },
      parent_tool_use_id: null,
      session_id: SESSION_ID,
      uuid: `delta-${turn}-${delta}`,
    });
  }
  cli.write({
    type: "user",
    message: {
      role: "user",
      content: [{ type: "tool_result", tool_use_id: `toolu-${turn}`, content: LARGE_TOOL_OUTPUT }],
    },
    parent_tool_use_id: null,
    session_id: SESSION_ID,
    uuid: `tool-${turn}`,
  });
  cli.write({
    type: "result",
    subtype: "success",
    is_error: false,
    duration_ms: 1,
    duration_api_ms: 1,
    num_turns: 1,
    result: `turn ${turn} done`,
    session_id: SESSION_ID,
    total_cost_usd: 0,
    usage: { input_tokens: 1, output_tokens: 1 },
    modelUsage: {},
    permission_denials: [],
    uuid: `result-${turn}`,
  });
};

const playOnPrompt: FakeClaudeCliOptions["onUser"] = (_message, cli) => {
  const turn = cli.frames.filter((frame) => frame.type === "user").length;
  playTurn(cli, turn);
};

const takeTurn = (session: ClaudeAgentSdkQuerySession) =>
  session.messages.pipe(
    Stream.takeUntil((message) => message.type === "result"),
    Stream.runCollect,
  );

interface TurnRun {
  readonly messages: ReadonlyArray<SDKMessage>;
  readonly frames: ReadonlyArray<unknown>;
  readonly logs: ReadonlyArray<unknown>;
}

const runOneTurn = (mode: Mode) =>
  withRunner(mode, ({ runner, logs, recorded }) =>
    Effect.gen(function* () {
      const cli = new FakeClaudeCli({ onUser: playOnPrompt });
      const session = yield* runner.open(openInput(cli));
      yield* session.offer(prompt("Run the tool."));
      const messages = yield* takeTurn(session);
      yield* Effect.promise(() => cli.stdinEnded);
      return {
        run: { messages, frames: normalizedFrames(cli.frames), logs } satisfies TurnRun,
        recorded,
        cli,
      };
    }),
  );

describe("Claude Agent SDK query in a provider I/O worker", () => {
  it.effect("runs a turn on the worker with the same messages, wire frames and logs", () =>
    Effect.gen(function* () {
      const inThread = yield* runOneTurn("thread");
      const inWorker = yield* runOneTurn("worker");

      assert.deepStrictEqual(inWorker.recorded.opens, [true]);
      assert.includeMembers(inWorker.recorded.requests, ["start", "prompt", "return"]);
      assert.strictEqual(inThread.run.messages.length, 67);
      assert.deepStrictEqual(inWorker.run.messages, inThread.run.messages);
      assert.deepStrictEqual(inWorker.run.frames, inThread.run.frames);
      assert.deepStrictEqual(inWorker.run.logs, inThread.run.logs);
      assert.deepStrictEqual(inWorker.cli.spawnOptions, inThread.cli.spawnOptions);
    }),
  );

  const controlCases: ReadonlyArray<
    readonly [
      string,
      (
        session: ClaudeAgentSdkQuerySession,
      ) => Effect.Effect<unknown, ClaudeAgentSdkQueryRunnerError>,
      Record<string, unknown>,
    ]
  > = [
    [
      "setModel",
      (session) => session.setModel("claude-opus-4-1"),
      { subtype: "set_model", model: "claude-opus-4-1" },
    ],
    [
      "applyFlagSettings",
      (session) => session.applyFlagSettings({ alwaysThinkingEnabled: false }),
      { subtype: "apply_flag_settings", settings: { alwaysThinkingEnabled: false } },
    ],
    [
      "setPermissionMode",
      (session) => session.setPermissionMode("plan"),
      { subtype: "set_permission_mode", mode: "plan" },
    ],
    [
      "stopTask",
      (session) => session.stopTask("task-1"),
      { subtype: "stop_task", task_id: "task-1" },
    ],
    [
      "backgroundTasks",
      (session) => session.backgroundTasks("toolu-1"),
      { subtype: "background_tasks", tool_use_id: "toolu-1" },
    ],
    ["interrupt", (session) => session.interrupt, { subtype: "interrupt" }],
  ];

  const runControl = (
    mode: Mode,
    act: (
      session: ClaudeAgentSdkQuerySession,
    ) => Effect.Effect<unknown, ClaudeAgentSdkQueryRunnerError>,
    cliOptions: FakeClaudeCliOptions = {},
  ) =>
    withRunner(mode, ({ runner, recorded }) =>
      Effect.gen(function* () {
        const cli = new FakeClaudeCli(cliOptions);
        const session = yield* runner.open(openInput(cli));
        const result = yield* act(session).pipe(Effect.result);
        yield* session.close;
        yield* Effect.promise(() => cli.stdinEnded);
        return { result, frames: normalizedFrames(cli.frames), recorded };
      }),
    );

  it.effect.each(controlCases)(
    "relays %s to the worker's query and back",
    ([method, act, request]) =>
      Effect.gen(function* () {
        const inThread = yield* runControl("thread", act);
        const inWorker = yield* runControl("worker", act);

        assert.include(inWorker.recorded.requests, method);
        assert.deepStrictEqual(inWorker.result, inThread.result);
        assert.strictEqual(inWorker.result._tag, "Success");
        assert.deepStrictEqual(inWorker.frames, inThread.frames);
        assert.deepInclude(
          inWorker.frames.map((frame) => (frame as { readonly request?: unknown }).request),
          request,
        );
      }),
  );

  it.effect("returns what backgroundTasks answers and fails a refused call like in-thread", () =>
    Effect.gen(function* () {
      const cliOptions: FakeClaudeCliOptions = {
        control: (request) =>
          request.subtype === "background_tasks"
            ? { response: { backgrounded: false } }
            : request.subtype === "set_model"
              ? { error: "unknown model" }
              : undefined,
      };
      const act = (session: ClaudeAgentSdkQuerySession) =>
        Effect.all([
          session.backgroundTasks("toolu-1"),
          session
            .setModel("claude-nope")
            .pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined })),
        ]);
      const inThread = yield* runControl("thread", act, cliOptions);
      const inWorker = yield* runControl("worker", act, cliOptions);

      for (const run of [inThread, inWorker]) {
        assert.strictEqual(run.result._tag, "Success");
        if (run.result._tag !== "Success") return;
        const [backgrounded, error] = run.result.success as readonly [
          boolean,
          { readonly _tag: string; readonly method: string; readonly cause: unknown },
        ];
        assert.isFalse(backgrounded);
        assert.strictEqual(error._tag, "ClaudeAgentSdkQueryRunnerError");
        assert.strictEqual(error.method, "setModel");
        assert.include(String((error.cause as Error).message), "unknown model");
      }
      assert.deepStrictEqual(inWorker.frames, inThread.frames);
    }),
  );

  it.effect("registers MCP servers through the worker before the query is used", () =>
    Effect.gen(function* () {
      const mcpServers = {
        "t3-code": {
          type: "http" as const,
          url: "http://localhost:3773/mcp",
          headers: { Authorization: "Bearer test-mcp-credential" },
        },
      };
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner, recorded, logs }) =>
          Effect.gen(function* () {
            const cli = new FakeClaudeCli({
              control: (request) =>
                request.subtype === "mcp_set_servers"
                  ? { response: { added: [], removed: [], errors: { "t3-code": "refused" } } }
                  : undefined,
            });
            const session = yield* runner.open(openInput(cli, { mcpServers }));
            yield* session.close;
            yield* Effect.promise(() => cli.stdinEnded);
            return { frames: normalizedFrames(cli.frames), recorded, logs };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.include(inWorker.recorded.requests, "setMcpServers");
      assert.deepStrictEqual(inWorker.frames, inThread.frames);
      assert.deepStrictEqual(inWorker.logs, inThread.logs);
      assert.deepInclude(
        inWorker.frames.map((frame) => (frame as { readonly request?: unknown }).request),
        { subtype: "mcp_set_servers", servers: mcpServers },
      );
    }),
  );

  it.effect("kills the CLI and disposes the worker's query when MCP registration times out", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner, recorded }) =>
          Effect.gen(function* () {
            const cli = new FakeClaudeCli({
              control: (request) =>
                request.subtype === "mcp_set_servers" ? new Promise(() => {}) : undefined,
            });
            const opening = yield* runner
              .open(
                openInput(cli, {
                  mcpServers: { "t3-code": { type: "http", url: "http://localhost:3773/mcp" } },
                }),
              )
              .pipe(Effect.flip, Effect.forkChild);
            yield* Effect.promise(() => cli.waitForControl("mcp_set_servers"));
            yield* TestClock.adjust("90 seconds");
            const error = yield* Fiber.join(opening);
            return { error, kills: cli.kills, recorded };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.include(inWorker.recorded.requests, "asyncDispose");
      for (const { error, kills } of [inThread, inWorker]) {
        assert.strictEqual(error.method, "setMcpServers");
        assert.strictEqual(
          (error.cause as { readonly _tag: string })._tag,
          "ClaudeMcpRegistrationTimeoutError",
        );
        assert.deepStrictEqual(kills, ["SIGKILL"]);
      }
    }),
  );

  // A turn that streams, then waits for the test to interrupt or close it.
  const streamThenWait: FakeClaudeCliOptions = {
    onUser: (_message, cli) => {
      for (let delta = 0; delta < 16; delta++) {
        cli.write({
          type: "stream_event",
          event: {
            type: "content_block_delta",
            index: 0,
            delta: { type: "text_delta", text: `delta ${delta} ` },
          },
          parent_tool_use_id: null,
          session_id: SESSION_ID,
          uuid: `delta-${delta}`,
        });
      }
    },
    control: (request, cli) => {
      if (request.subtype !== "interrupt") return undefined;
      queueMicrotask(() =>
        cli.write({
          type: "result",
          subtype: "error_during_execution",
          is_error: true,
          duration_ms: 1,
          duration_api_ms: 1,
          num_turns: 1,
          session_id: SESSION_ID,
          total_cost_usd: 0,
          usage: { input_tokens: 1, output_tokens: 1 },
          modelUsage: {},
          permission_denials: [],
          errors: [],
          uuid: "result-interrupted",
        }),
      );
      return { response: {} };
    },
  };

  const streamUntilDelta = (session: ClaudeAgentSdkQuerySession, uuid: string) =>
    Effect.gen(function* () {
      const seen = yield* Deferred.make<void>();
      const turn = yield* session.messages.pipe(
        Stream.tap((message) =>
          "uuid" in message && message.uuid === uuid
            ? Deferred.succeed(seen, undefined)
            : Effect.void,
        ),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* Deferred.await(seen);
      return turn;
    });

  it.effect("ends a streaming turn on interrupt after the frames before it, as in-thread", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner, logs }) =>
          Effect.gen(function* () {
            const cli = new FakeClaudeCli(streamThenWait);
            const session = yield* runner.open(openInput(cli));
            yield* session.offer(prompt("Stream until stopped."));
            const turn = yield* streamUntilDelta(session, "delta-15");
            yield* session.interrupt;
            yield* Effect.promise(() => cli.waitForControl("interrupt"));
            yield* session.close;
            const messages = yield* Fiber.join(turn);
            return { messages, frames: normalizedFrames(cli.frames), logs };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.deepStrictEqual(inWorker.messages, inThread.messages);
      assert.deepStrictEqual(inWorker.frames, inThread.frames);
      assert.deepStrictEqual(inWorker.logs, inThread.logs);
      const types = inWorker.messages.map((message) => message.type);
      assert.deepStrictEqual(types, [...Array(16).fill("stream_event"), "result"]);
      assert.deepInclude(inWorker.messages.at(-1), { subtype: "error_during_execution" });
      const prompted = inWorker.frames.findIndex((frame) => frame.type === "user");
      const interrupted = inWorker.frames.findIndex(
        (frame) =>
          (frame.request as { readonly subtype?: unknown } | undefined)?.subtype === "interrupt",
      );
      assert.isAbove(prompted, -1);
      assert.isAbove(interrupted, prompted);
    }),
  );

  it.effect("ends the messages when the query closes mid-turn, as in-thread", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner, logs }) =>
          Effect.gen(function* () {
            const cli = new FakeClaudeCli(streamThenWait);
            const session = yield* runner.open(openInput(cli));
            yield* session.offer(prompt("Stream until closed."));
            const turn = yield* streamUntilDelta(session, "delta-15");
            yield* session.close;
            const messages = yield* Fiber.join(turn);
            yield* Effect.promise(() => cli.stdinEnded);
            return { messages, frames: normalizedFrames(cli.frames), logs, kills: cli.kills };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.strictEqual(inWorker.messages.length, 16);
      assert.deepStrictEqual(inWorker.messages, inThread.messages);
      assert.deepStrictEqual(inWorker.frames, inThread.frames);
      assert.deepStrictEqual(inWorker.logs, inThread.logs);
      assert.deepStrictEqual(inWorker.kills, inThread.kills);
    }),
  );

  it.effect("fails a turn whose worker dies like a turn whose CLI dies, and kills the CLI", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner }) =>
          Effect.gen(function* () {
            const cli = new FakeClaudeCli(streamThenWait);
            const session = yield* runner.open(openInput(cli));
            yield* session.offer(prompt("Stream until the worker dies."));
            const turn = yield* streamUntilDelta(session, "delta-15");
            if (mode === "worker") {
              yield* Effect.promise(() =>
                Promise.all(workers.started.map((worker) => worker.terminate())),
              );
            } else {
              cli.exit(1, null);
            }
            const error = yield* Fiber.join(turn).pipe(Effect.flip);
            return { error, kills: cli.kills };
          }),
        );
      const inThread = yield* run("thread");
      workers.started.length = 0;
      const inWorker = yield* run("worker");

      assert.strictEqual(inWorker.error._tag, "ClaudeAgentSdkQueryRunnerError");
      assert.strictEqual(inWorker.error.method, inThread.error.method);
      assert.strictEqual(
        (inWorker.error.cause as { readonly _tag: string })._tag,
        "ProviderIoWorkerError",
      );
      assert.deepStrictEqual(
        makeProviderFailure({ cause: inWorker.error, class: "transport_error" }),
        makeProviderFailure({ cause: inThread.error, class: "transport_error" }),
      );
      assert.deepStrictEqual(inWorker.kills, ["SIGKILL"]);
    }),
  );

  const resultFrame = (uuid: string, text: string) => ({
    type: "result",
    subtype: "success",
    is_error: false,
    duration_ms: 1,
    duration_api_ms: 1,
    num_turns: 1,
    result: text,
    session_id: SESSION_ID,
    total_cost_usd: 0,
    usage: { input_tokens: 1, output_tokens: 1 },
    modelUsage: {},
    permission_denials: [],
    uuid,
  });

  // Main has posted the bytes holding `marker` to the worker.
  const postedToWorker = (marker: string) =>
    Effect.callback<void>((resume) => {
      workers.onBytes = (chunk) => {
        if (!Buffer.from(chunk).includes(marker)) return;
        workers.onBytes = undefined;
        resume(Effect.void);
      };
    });

  it.effect(
    "delivers the frames read before the query closes and none sent after, as in-thread",
    () =>
      Effect.gen(function* () {
        const run = (mode: Mode) =>
          withRunner(mode, ({ runner }) =>
            Effect.gen(function* () {
              const cli = new FakeClaudeCli(streamThenWait);
              const session = yield* runner.open(openInput(cli));
              yield* session.offer(prompt("Stream until closed."));
              const streamed = yield* Deferred.make<void>();
              const handedOver = yield* Deferred.make<void>();
              const turn = yield* session.messages.pipe(
                Stream.tap((message) =>
                  !("uuid" in message)
                    ? Effect.void
                    : String(message.uuid) === "delta-15"
                      ? Deferred.succeed(streamed, undefined)
                      : String(message.uuid) === "result-read"
                        ? Deferred.succeed(handedOver, undefined)
                        : Effect.void,
                ),
                Stream.runCollect,
                Effect.forkChild,
              );
              yield* Deferred.await(streamed);
              // Read before the close: in-thread the SDK hands it to the reader, on a worker main sends it there.
              const read =
                mode === "worker"
                  ? yield* Effect.forkChild(postedToWorker("result-read"), {
                      startImmediately: true,
                    })
                  : undefined;
              cli.write(resultFrame("result-read", "x".repeat(2 * 1024 * 1024)));
              yield* read === undefined ? Deferred.await(handedOver) : Fiber.join(read);
              yield* session.close;
              cli.write(resultFrame("result-after-close", "late"));
              const messages = yield* Fiber.join(turn);
              return messages.map((message) => ("uuid" in message ? message.uuid : message.type));
            }),
          );
        const inThread = yield* run("thread");
        const inWorker = yield* run("worker");

        assert.deepStrictEqual(inThread, [
          ...Array.from({ length: 16 }, (_, delta) => `delta-${delta}`),
          "result-read",
        ]);
        assert.deepStrictEqual(inWorker, inThread);
      }),
  );

  it.effect("stops the CLI when a query closes after its worker session failed", () =>
    withRunner("worker", ({ runner }) =>
      Effect.gen(function* () {
        const cli = new FakeClaudeCli({ exitOnStdinEnd: false, exitOnKill: false });
        const openedBefore = workers.opened.length;
        const session = yield* runner.open(openInput(cli));
        const messages = yield* Stream.runDrain(session.messages).pipe(
          Effect.flip,
          Effect.forkChild,
        );
        const [opened] = workers.opened.slice(openedBefore);
        assert.isDefined(opened);
        // What main hears when the stage throws on the worker.
        opened.worker.emit("message", {
          type: "fail",
          session: opened.session,
          error: new Error("injected stage failure"),
        });
        const error = yield* Fiber.join(messages);
        assert.strictEqual(error.method, "fromAsyncIterable");
        yield* session.close;
        yield* Effect.promise(() => cli.stdinEnded);
        yield* TestClock.adjust("2 seconds");
        yield* Effect.promise(() => cli.waitForKill("SIGTERM"));
        yield* TestClock.adjust("5 seconds");
        yield* Effect.promise(() => cli.waitForKill("SIGKILL"));
        assert.deepStrictEqual(cli.kills, ["SIGTERM", "SIGKILL"]);
      }),
    ),
  );

  it.effect("stops the CLI when the worker session fails after the close was sent", () =>
    withRunner("worker", ({ runner }) =>
      Effect.gen(function* () {
        const cli = new FakeClaudeCli({ exitOnStdinEnd: false, exitOnKill: false });
        const openedBefore = workers.opened.length;
        const session = yield* runner.open(openInput(cli));
        const messages = yield* Stream.runDrain(session.messages).pipe(
          Effect.flip,
          Effect.forkChild,
        );
        const [opened] = workers.opened.slice(openedBefore);
        assert.isDefined(opened);
        yield* session.close;
        // The session fails before the worker's SDK could end the CLI's input or kill it.
        opened.worker.emit("message", {
          type: "fail",
          session: opened.session,
          error: new Error("injected stage failure"),
        });
        yield* Fiber.join(messages);
        yield* Effect.promise(() => cli.stdinEnded);
        yield* TestClock.adjust("2 seconds");
        yield* Effect.promise(() => cli.waitForKill("SIGTERM"));
        yield* TestClock.adjust("5 seconds");
        yield* Effect.promise(() => cli.waitForKill("SIGKILL"));
        assert.deepStrictEqual(cli.kills, ["SIGTERM", "SIGKILL"]);
      }),
    ),
  );

  it.effect.each([
    { malformed: "an unknown signal", event: { type: "kill", signal: "SIGBOGUS" } },
    { malformed: "stdin data that is not text or bytes", event: { type: "stdin", data: 42 } },
  ])(
    "fails and kills only the session whose worker sends $malformed; the others go on",
    ({ event }) =>
      withRunner(
        "worker",
        ({ runner }) =>
          Effect.gen(function* () {
            const broken = new FakeClaudeCli();
            const openedBefore = workers.opened.length;
            const brokenSession = yield* runner.open(openInput(broken));
            const [opened] = workers.opened.slice(openedBefore);
            assert.isDefined(opened);
            const failed = yield* Stream.runDrain(brokenSession.messages).pipe(
              Effect.flip,
              Effect.forkChild,
            );
            const healthy = new FakeClaudeCli({ onUser: playOnPrompt });
            const healthySession = yield* runner.open(openInput(healthy));

            opened.worker.emit("message", { type: "event", session: opened.session, event });
            assert.notInclude(broken.kills, "SIGBOGUS");
            const error = yield* Fiber.join(failed);
            assert.strictEqual(error.method, "fromAsyncIterable");
            yield* Effect.promise(() => broken.waitForKill("SIGKILL"));
            assert.deepStrictEqual(broken.kills, ["SIGKILL"]);

            yield* healthySession.offer(prompt("Run the tool."));
            const messages = yield* takeTurn(healthySession);
            assert.strictEqual(messages.at(-1)?.type, "result");
            assert.deepStrictEqual(healthy.kills, []);
          }),
        { workers: 1 },
      ),
  );

  it.effect("fails a control call after the query ended with the in-thread error", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner }) =>
          Effect.gen(function* () {
            const cli = new FakeClaudeCli();
            const session = yield* runner.open(openInput(cli));
            yield* session.close;
            yield* Stream.runDrain(session.messages);
            yield* Effect.promise(() => cli.stdinEnded);
            const error = yield* Effect.flip(session.setModel("claude-opus-4-1"));
            return { method: error.method, message: (error.cause as Error).message };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.deepStrictEqual(inWorker, inThread);
    }),
  );

  it.effect("kills a CLI that outlives its output when its worker dies", () =>
    withRunner("worker", ({ runner }) =>
      Effect.gen(function* () {
        const cli = new FakeClaudeCli({ exitOnStdinEnd: false, exitOnKill: false });
        const session = yield* runner.open(openInput(cli));
        yield* session.close;
        yield* Stream.runDrain(session.messages);
        assert.isNull(cli.exitCode);
        yield* Effect.promise(() =>
          Promise.all(workers.started.map((worker) => worker.terminate())),
        );
        yield* Effect.promise(() => cli.waitForKill("SIGKILL"));
        assert.include(cli.kills, "SIGKILL");
      }),
    ),
  );

  it.effect("stops a CLI whose query is closing before the pool's close returns", () =>
    Effect.gen(function* () {
      const cli = new FakeClaudeCli({ exitOnStdinEnd: false });
      yield* withRunner("worker", ({ runner }) =>
        Effect.gen(function* () {
          const session = yield* runner.open(openInput(cli));
          yield* session.close;
          yield* Effect.promise(() => cli.stdinEnded);
        }),
      );
      assert.deepStrictEqual(cli.kills, ["SIGTERM"]);
      assert.strictEqual(cli.signalCode, "SIGTERM");
    }),
  );

  it.effect("keeps the query in-thread when the pool has no workers", () =>
    Effect.gen(function* () {
      const startedBefore = workers.started.length;
      const { run, recorded } = yield* withRunner(
        "worker",
        ({ runner, logs, recorded }) =>
          Effect.gen(function* () {
            const cli = new FakeClaudeCli({ onUser: playOnPrompt });
            const session = yield* runner.open(openInput(cli));
            yield* session.offer(prompt("Run the tool."));
            const messages = yield* takeTurn(session);
            return { run: { messages, logs }, recorded };
          }),
        { workers: 0 },
      );
      assert.deepStrictEqual(recorded.opens, [false]);
      assert.strictEqual(workers.started.length, startedBefore);
      assert.strictEqual(run.messages.at(-1)?.type, "result");
    }),
  );

  // A callback's arguments without its abort signal, which each run makes anew, and its options' keys
  // in order (the SDK puts the signal first).
  const recordCall = (calls: Array<unknown>, args: ReadonlyArray<unknown>) => {
    const { signal, ...options } = args.at(-1) as { readonly signal: AbortSignal };
    assert.instanceOf(signal, AbortSignal);
    calls.push([...args.slice(0, -1), options, Object.keys(args.at(-1) as object)]);
    return signal;
  };

  const permissionRequest = (toolUseId: string) => ({
    subtype: "can_use_tool",
    tool_name: "Bash",
    input: { command: `echo ${toolUseId}` },
    tool_use_id: toolUseId,
    permission_suggestions: [
      {
        type: "addRules",
        rules: [{ toolName: "Bash", ruleContent: "echo:*" }],
        behavior: "allow",
        destination: "session",
      },
    ],
    decision_reason: "Bash asks",
    title: "Run echo",
  });
  const elicitationRequest = (message: string) => ({
    subtype: "elicitation",
    mcp_server_name: "t3-code",
    message,
    requested_schema: { type: "object", properties: { choice: { type: "string" } } },
  });
  const dialogRequest = (toolUseId: string) => ({
    subtype: "request_user_dialog",
    dialog_kind: "resume_return",
    payload: { sessionAgeMinutes: 90 },
    tool_use_id: toolUseId,
  });
  const hookRequest = (callbackId: string, toolUseId: string) => ({
    subtype: "hook_callback",
    callback_id: callbackId,
    input: { hook_event_name: "PreToolUse", tool_name: "Bash", tool_input: { command: "ls" } },
    tool_use_id: toolUseId,
  });

  const PROBE = "toolu-probe";
  class JsonList extends Array<string> {
    toJSON() {
      return { list: [...this] };
    }
  }
  // The SDK reads a matcher's fields as properties, so getters and fields that are not enumerable count.
  class GetterMatcher {
    readonly #hooks: ReadonlyArray<HookCallback>;
    constructor(hooks: ReadonlyArray<HookCallback>) {
      this.#hooks = hooks;
      Object.defineProperty(this, "timeout", { value: 5, enumerable: false });
    }
    get matcher() {
      return "Bash";
    }
    get hooks() {
      return this.#hooks;
    }
  }
  class KeyedJson {
    toJSON(key: string) {
      return { continue: true, writtenUnder: key };
    }
  }
  class ConvertsToJson {
    readonly #json: unknown;
    constructor(json: unknown) {
      this.#json = json;
    }
    toJSON() {
      return this.#json;
    }
  }
  type Record_ = (args: ReadonlyArray<unknown>) => AbortSignal;
  type Callbacks = Pick<
    ClaudeQueryOptions,
    "canUseTool" | "onElicitation" | "onUserDialog" | "hooks"
  >;

  const permissionCase = (
    label: string,
    answer: () => ReturnType<CanUseTool>,
  ): readonly [string, (record: Record_) => Callbacks, ReadonlyArray<Record<string, unknown>>] => [
    `canUseTool ${label}`,
    (record) => ({
      canUseTool: (...args) => {
        record(args);
        return args[2].toolUseID === PROBE
          ? Promise.resolve({ behavior: "allow", updatedInput: { probed: true } })
          : answer();
      },
    }),
    [permissionRequest("toolu-case"), permissionRequest(PROBE)],
  ];

  // Each case's request, then a probe the same callback answers: the SDK writes the probe's
  // response after the first one's, so awaiting it shows whether the first answer was written.
  const callbackCases: ReadonlyArray<
    readonly [string, (record: Record_) => Callbacks, ReadonlyArray<Record<string, unknown>>]
  > = [
    permissionCase("approves", () =>
      Promise.resolve({ behavior: "allow", updatedInput: { command: "echo approved" } }),
    ),
    permissionCase("denies", () =>
      Promise.resolve({ behavior: "deny", message: "Not this one.", interrupt: true }),
    ),
    permissionCase("answers null", () => Promise.resolve(null as never)),
    permissionCase("throws an Error", () =>
      Promise.reject(new Error("The approval store is unavailable.")),
    ),
    permissionCase("throws synchronously", () => {
      throw new TypeError("Cannot read the approval.");
    }),
    permissionCase("rejects with a string", () => Promise.reject("plain refusal")),
    // The SDK writes an Error's message as its error field, whatever its type.
    permissionCase("throws an Error whose message is a number", () =>
      Promise.reject(Object.assign(new Error(), { message: 42 })),
    ),
    permissionCase("throws an Error without a message", () =>
      Promise.reject(Object.assign(new Error(), { message: undefined })),
    ),
    permissionCase("throws an Error whose message has toJSON", () =>
      Promise.reject(Object.assign(new Error(), { message: new KeyedJson() })),
    ),
    // No "throws an Error whose message JSON cannot write" case: from SDK 0.3.284 the failed
    // write escapes the SDK's control-request handler as an unhandled rejection in-thread. A
    // worker-only test below pins what a worker does instead.
    // Each of these clones without error but not as the SDK's JSON writes it.
    permissionCase("answers with values JSON converts", () =>
      Promise.resolve({
        behavior: "allow",
        updatedInput: {
          bytes: Buffer.from([1, 2]),
          converted: new ConvertsToJson("converted"),
          entries: new Map([["key", "value"]]),
        },
      }),
    ),
    permissionCase("answers with an array whose class has toJSON", () =>
      Promise.resolve({ behavior: "allow", updatedInput: { list: JsonList.from(["a", "b"]) } }),
    ),
    permissionCase("answers with a toJSON that is not enumerable", () =>
      Promise.resolve({
        behavior: "allow",
        updatedInput: {
          hidden: Object.defineProperty({ shown: true }, "toJSON", {
            value: () => "hidden",
            enumerable: false,
          }),
        },
      }),
    ),
    // The SDK spreads canUseTool's answer, which copies an own toJSON into what it writes.
    permissionCase("answers with its own toJSON", () =>
      Promise.resolve({
        behavior: "allow",
        updatedInput: {},
        toJSON: () => "written instead",
      } as never),
    ),
    permissionCase("answers through a Proxy", () =>
      Promise.resolve({
        behavior: "allow",
        updatedInput: new Proxy({ command: "echo proxied" }, {}),
      }),
    ),
    permissionCase("answers with a value JSON cannot write", () =>
      Promise.resolve({ behavior: "allow", updatedInput: { count: 1n } }),
    ),
    permissionCase("answers with a function JSON drops", () =>
      Promise.resolve({
        behavior: "allow",
        updatedInput: { kept: true, dropped: () => "dropped" },
      }),
    ),
    [
      "onElicitation accepts",
      (record) => ({
        onElicitation: async (...args) => {
          record(args);
          return args[0].message === PROBE
            ? { action: "decline" }
            : { action: "accept", content: { choice: "a" } };
        },
      }),
      [elicitationRequest("Pick one"), elicitationRequest(PROBE)],
    ],
    [
      "onElicitation answers null",
      (record) => ({
        onElicitation: async (...args) => {
          record(args);
          return args[0].message === PROBE ? { action: "decline" } : (null as never);
        },
      }),
      [elicitationRequest("Pick one"), elicitationRequest(PROBE)],
    ],
    [
      "onElicitation answers with a value whose JSON is null",
      (record) => ({
        onElicitation: async (...args) => {
          record(args);
          return args[0].message === PROBE
            ? { action: "decline" }
            : ({ toJSON: () => null } as never);
        },
      }),
      [elicitationRequest("Pick one"), elicitationRequest(PROBE)],
    ],
    [
      "onElicitation throws",
      (record) => ({
        onElicitation: async (...args) => {
          record(args);
          if (args[0].message === PROBE) return { action: "decline" };
          throw new Error("The elicitation card is gone.");
        },
      }),
      [elicitationRequest("Pick one"), elicitationRequest(PROBE)],
    ],
    [
      "onUserDialog completes",
      (record) => ({
        onUserDialog: async (...args) => {
          record(args);
          return args[0].toolUseID === PROBE
            ? { behavior: "cancelled" }
            : { behavior: "completed", result: "continue" };
        },
      }),
      [dialogRequest("toolu-case"), dialogRequest(PROBE)],
    ],
    [
      "onUserDialog throws",
      (record) => ({
        onUserDialog: async (...args) => {
          record(args);
          if (args[0].toolUseID === PROBE) return { behavior: "cancelled" };
          throw new Error("No dialog for this session.");
        },
      }),
      [dialogRequest("toolu-case"), dialogRequest(PROBE)],
    ],
    [
      "a PreToolUse hook, in its matcher with its timeout",
      (record) => ({
        hooks: {
          PreToolUse: [
            {
              matcher: "Bash",
              timeout: 5,
              hooks: [
                async (...args) => {
                  record(args);
                  return { decision: "block", reason: `blocked ${args[1]}` };
                },
                async (...args) => {
                  record(args);
                  return { continue: true };
                },
              ],
            },
          ],
        },
      }),
      [hookRequest("hook_0", "toolu-case"), hookRequest("hook_1", PROBE)],
    ],
    [
      "a hook whose matcher reads its fields from getters",
      (record) => ({
        hooks: {
          PreToolUse: [
            new GetterMatcher([
              async (...args) => {
                record(args);
                return { continue: true, reason: "getter matcher" };
              },
              async (...args) => {
                record(args);
                return { continue: true };
              },
            ]) as never,
          ],
        },
      }),
      [hookRequest("hook_0", "toolu-case"), hookRequest("hook_1", PROBE)],
    ],
    [
      "a hook whose answer JSON converts",
      (record) => ({
        hooks: {
          PreToolUse: [
            {
              hooks: [
                async (...args) => {
                  record(args);
                  return new ConvertsToJson({ continue: false, stopReason: "converted" }) as never;
                },
                async (...args) => {
                  record(args);
                  return { continue: true };
                },
              ],
            },
          ],
        },
      }),
      [hookRequest("hook_0", "toolu-case"), hookRequest("hook_1", PROBE)],
    ],
    [
      "a hook whose toJSON reads the key it is written under",
      (record) => ({
        hooks: {
          PreToolUse: [
            {
              hooks: [
                async (...args) => {
                  record(args);
                  return new KeyedJson() as never;
                },
                async (...args) => {
                  record(args);
                  return { continue: true };
                },
              ],
            },
          ],
        },
      }),
      [hookRequest("hook_0", "toolu-case"), hookRequest("hook_1", PROBE)],
    ],
    [
      "a hook that throws",
      (record) => ({
        hooks: {
          SubagentStart: [
            {
              hooks: [
                async (...args) => {
                  record(args);
                  throw new Error("The hook failed.");
                },
                async (...args) => {
                  record(args);
                  return {};
                },
              ],
            },
          ],
        },
      }),
      [hookRequest("hook_0", "toolu-case"), hookRequest("hook_1", PROBE)],
    ],
  ];

  it.effect.each(callbackCases)(
    "relays %s to main and returns its answer as in-thread",
    ([, callbacks, requests]) =>
      Effect.gen(function* () {
        const run = (mode: Mode) =>
          withRunner(mode, ({ runner, recorded }) =>
            Effect.gen(function* () {
              const calls: Array<unknown> = [];
              const cli = new FakeClaudeCli();
              const session = yield* runner.open(
                openInput(
                  cli,
                  callbacks((args) => recordCall(calls, args)),
                ),
              );
              const sent = requests.map((request) => cli.request(request));
              yield* Effect.promise(() => sent.at(-1)!);
              yield* session.close;
              yield* Effect.promise(() => cli.stdinEnded);
              return { calls, frames: normalizedFrames(cli.frames), recorded };
            }),
          );
        const inThread = yield* run("thread");
        const inWorker = yield* run("worker");

        assert.deepStrictEqual(inWorker.recorded.opens, [true]);
        assert.include(inWorker.recorded.requests, "callbackResult");
        assert.lengthOf(inThread.calls, 2);
        assert.deepStrictEqual(inWorker.calls, inThread.calls);
        assert.deepStrictEqual(inWorker.frames, inThread.frames);
      }),
  );

  // From SDK 0.3.284 an error response the SDK cannot write escapes its control-request handler as an
  // unhandled rejection, so in-thread is no oracle here: on a worker it must fail only that request.
  const unwritableErrors: ReadonlyArray<readonly [string, () => unknown, string]> = [
    [
      "a BigInt message",
      () => Object.assign(new Error(), { message: 1n }),
      "A T3 Code callback failed with an error that cannot be sent to Claude: Do not know how to serialize a BigInt",
    ],
    [
      "a message whose JSON fails with an error that cannot be printed",
      () =>
        Object.assign(new Error(), {
          message: {
            toJSON: () => {
              throw Object.assign(new Error(), { message: Symbol("unprintable") });
            },
          },
        }),
      "A T3 Code callback failed with an error that cannot be sent to Claude",
    ],
  ];

  it.effect.each(unwritableErrors)(
    "fails a callback whose error has %s, clearly and alone, and keeps its worker",
    ([, rejection, expected]) =>
      withRunner(
        "worker",
        ({ runner, recorded }) =>
          Effect.gen(function* () {
            const unwritable = new FakeClaudeCli();
            const neighbour = new FakeClaudeCli();
            yield* runner.open(
              openInput(unwritable, { canUseTool: () => Promise.reject(rejection()) }),
            );
            yield* runner.open(
              openInput(neighbour, {
                canUseTool: async () => ({ behavior: "allow", updatedInput: { allowed: true } }),
              }),
            );
            // A dead worker kills its sessions' CLIs; a throw on main is an unhandled rejection there.
            const unhandled = Promise.withResolvers<{ readonly unhandled: unknown }>();
            const onUnhandled = (reason: unknown) => unhandled.resolve({ unhandled: reason });
            process.on("unhandledRejection", onUnhandled);
            const outcome = (cli: FakeClaudeCli, toolUseId: string) =>
              Promise.race([
                cli.request(permissionRequest(toolUseId)).then((response) => ({ response })),
                new Promise<{ readonly killed: true }>((resolve) =>
                  cli.once("exit", () => resolve({ killed: true })),
                ),
                unhandled.promise,
              ]);
            const failed = yield* Effect.promise(() => outcome(unwritable, "toolu-unwritable"));
            const answered = yield* Effect.promise(() => outcome(neighbour, "toolu-neighbour"));
            process.off("unhandledRejection", onUnhandled);

            assert.deepStrictEqual(recorded.opens, [true, true]);
            assert.deepStrictEqual(failed, {
              response: { subtype: "error", request_id: "cli-0", error: expected },
            });
            assert.deepStrictEqual(answered, {
              response: {
                subtype: "success",
                request_id: "cli-0",
                response: {
                  behavior: "allow",
                  updatedInput: { allowed: true },
                  toolUseID: "toolu-neighbour",
                },
              },
            });
            assert.deepStrictEqual([unwritable.kills, neighbour.kills], [[], []]);
          }),
        { workers: 1 },
      ),
  );

  // Answers once its request is cancelled, recording whether its signal said so.
  const answerOnAbort =
    (called: PromiseWithResolvers<void>, aborted: Array<boolean>): CanUseTool =>
    (_toolName, _input, { signal }) => {
      called.resolve();
      return new Promise((resolve) =>
        signal.addEventListener(
          "abort",
          () => {
            aborted.push(signal.aborted);
            resolve({ behavior: "deny", message: "Cancelled." });
          },
          { once: true },
        ),
      );
    };

  it.effect("carries the CLI's cancel of a callback to main, whose answer still goes back", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner, recorded }) =>
          Effect.gen(function* () {
            const called = Promise.withResolvers<void>();
            const aborted: Array<boolean> = [];
            const cli = new FakeClaudeCli();
            const session = yield* runner.open(
              openInput(cli, { canUseTool: answerOnAbort(called, aborted) }),
            );
            const answered = cli.request(permissionRequest("toolu-cancelled"));
            yield* Effect.promise(() => called.promise);
            cli.write({ type: "control_cancel_request", request_id: "cli-0" });
            yield* Effect.promise(() => answered);
            yield* session.close;
            yield* Effect.promise(() => cli.stdinEnded);
            return { aborted, frames: normalizedFrames(cli.frames), recorded };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.include(inWorker.recorded.requests, "callbackResult");
      assert.deepStrictEqual(inThread.aborted, [true]);
      assert.deepStrictEqual(inWorker.aborted, inThread.aborted);
      assert.deepStrictEqual(inWorker.frames, inThread.frames);
      assert.deepInclude(
        inWorker.frames.map((frame) => (frame as { readonly response?: unknown }).response),
        {
          subtype: "success",
          request_id: "request-1",
          response: { behavior: "deny", message: "Cancelled.", toolUseID: "toolu-cancelled" },
        },
      );
    }),
  );

  it.effect("aborts a callback main is running when the query closes, as in-thread", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner }) =>
          Effect.gen(function* () {
            const called = Promise.withResolvers<void>();
            const aborted: Array<boolean> = [];
            const cli = new FakeClaudeCli();
            const session = yield* runner.open(
              openInput(cli, { canUseTool: answerOnAbort(called, aborted) }),
            );
            void cli.request(permissionRequest("toolu-closed"));
            yield* Effect.promise(() => called.promise);
            yield* session.close;
            const abortedAtClose = [...aborted];
            yield* Effect.promise(() => cli.stdinEnded);
            return { abortedAtClose, frames: normalizedFrames(cli.frames) };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.deepStrictEqual(inThread.abortedAtClose, [true]);
      assert.deepStrictEqual(inWorker.abortedAtClose, inThread.abortedAtClose);
      assert.deepStrictEqual(inWorker.frames, inThread.frames);
      assert.notInclude(
        inWorker.frames.map((frame) => frame.type),
        "control_response",
      );
    }),
  );

  it.effect("answers concurrent callbacks from several sessions, each with its own answer", () =>
    Effect.gen(function* () {
      const SESSIONS = 3;
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner, recorded }) =>
          Effect.gen(function* () {
            const allPending = Promise.withResolvers<void>();
            let pending = 0;
            const opened = yield* Effect.forEach(
              Array.from({ length: SESSIONS }, (_, index) => index),
              (index) =>
                Effect.gen(function* () {
                  const cli = new FakeClaudeCli();
                  const session = yield* runner.open(
                    openInput(cli, {
                      canUseTool: async (_toolName, input) => {
                        pending += 1;
                        if (pending === SESSIONS * 2) allPending.resolve();
                        await allPending.promise;
                        return { behavior: "allow", updatedInput: { ...input, session: index } };
                      },
                    }),
                  );
                  return { cli, session };
                }),
            );
            const responses = yield* Effect.promise(() =>
              Promise.all(
                opened.map(({ cli }) =>
                  Promise.all([
                    cli.request(permissionRequest("toolu-a")),
                    cli.request(permissionRequest("toolu-b")),
                  ]),
                ),
              ),
            );
            yield* Effect.forEach(opened, ({ session }) => session.close, { discard: true });
            return {
              responses: responses.map((answers) => answers.map(({ response }) => response)),
              recorded,
            };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.deepStrictEqual(inWorker.recorded.opens, Array(SESSIONS).fill(true));
      assert.deepStrictEqual(inWorker.responses, inThread.responses);
      assert.deepStrictEqual(
        inWorker.responses,
        Array.from({ length: SESSIONS }, (_, session) =>
          ["toolu-a", "toolu-b"].map((toolUseID) => ({
            behavior: "allow",
            updatedInput: { command: `echo ${toolUseID}`, session },
            toolUseID,
          })),
        ),
      );
    }),
  );

  it.effect("fails the turn, kills the CLI and aborts main's callback when the worker dies", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner }) =>
          Effect.gen(function* () {
            const called = Promise.withResolvers<void>();
            const aborted: Array<boolean> = [];
            const cli = new FakeClaudeCli({
              onUser: (_message, cli) => {
                streamThenWait.onUser?.(_message, cli);
                void cli.request(permissionRequest("toolu-pending"));
              },
            });
            const session = yield* runner.open(
              openInput(cli, { canUseTool: answerOnAbort(called, aborted) }),
            );
            yield* session.offer(prompt("Ask, then lose the worker."));
            const turn = yield* session.messages.pipe(Stream.runCollect, Effect.forkChild);
            yield* Effect.promise(() => called.promise);
            if (mode === "worker") {
              yield* Effect.promise(() =>
                Promise.all(workers.started.map((worker) => worker.terminate())),
              );
            } else {
              cli.exit(1, null);
            }
            const error = yield* Fiber.join(turn).pipe(Effect.flip);
            return { error, aborted, kills: cli.kills };
          }),
        );
      const inThread = yield* run("thread");
      workers.started.length = 0;
      const inWorker = yield* run("worker");

      assert.deepStrictEqual(inThread.aborted, [true]);
      assert.deepStrictEqual(inWorker.aborted, inThread.aborted);
      assert.strictEqual(inWorker.error._tag, "ClaudeAgentSdkQueryRunnerError");
      assert.strictEqual(inWorker.error.method, inThread.error.method);
      assert.strictEqual(
        (inWorker.error.cause as { readonly _tag: string })._tag,
        "ProviderIoWorkerError",
      );
      assert.deepStrictEqual(inWorker.kills, ["SIGKILL"]);
    }),
  );

  const streamThenAsk =
    (deltas: number): NonNullable<FakeClaudeCliOptions["onUser"]> =>
    (_message, cli) => {
      for (let delta = 0; delta < deltas; delta++) {
        cli.write({
          type: "stream_event",
          event: {
            type: "content_block_delta",
            index: 0,
            delta: { type: "text_delta", text: `delta ${delta} ` },
          },
          parent_tool_use_id: null,
          session_id: SESSION_ID,
          uuid: `delta-${delta}`,
        });
      }
      void cli.request(permissionRequest("toolu-ordered"));
    };

  it.effect("starts a callback once the consumer has handled the messages read before it", () =>
    Effect.gen(function* () {
      const run = (mode: Mode, requestsInOwnRead: boolean) =>
        withRunner(mode, ({ runner }) =>
          Effect.gen(function* () {
            let handled = 0;
            const handledAtCallback: Array<number> = [];
            const answered = Promise.withResolvers<void>();
            const cli = new FakeClaudeCli({
              pipeLatency: true,
              requestsInOwnRead,
              onUser: streamThenAsk(32),
            });
            const session = yield* runner.open(
              openInput(cli, {
                canUseTool: async () => {
                  handledAtCallback.push(handled);
                  answered.resolve();
                  return { behavior: "allow", updatedInput: {} };
                },
              }),
            );
            yield* session.messages.pipe(
              Stream.runForEach(() => Effect.sync(() => (handled += 1))),
              Effect.forkChild,
            );
            yield* session.offer(prompt("Stream, then ask."));
            yield* Effect.promise(() => answered.promise);
            yield* session.close;
            return handledAtCallback;
          }),
        );
      // In-thread, only a request in its own read comes after the consumer has had the frames.
      const inThread = yield* run("thread", true);
      const inWorker = yield* run("worker", false);
      const inWorkerOwnRead = yield* run("worker", true);

      assert.deepStrictEqual(inThread, [32]);
      assert.deepStrictEqual(inWorker, inThread);
      assert.deepStrictEqual(inWorkerOwnRead, inThread);
    }),
  );

  it.effect("starts a callback at once while nothing consumes the messages yet", () =>
    withRunner("worker", ({ runner }) =>
      Effect.gen(function* () {
        const calls: Array<unknown> = [];
        const cli = new FakeClaudeCli({ pipeLatency: true, onUser: streamThenAsk(3) });
        const session = yield* runner.open(
          openInput(cli, {
            canUseTool: async (...args) => {
              recordCall(calls, args);
              return { behavior: "allow", updatedInput: {} };
            },
          }),
        );
        yield* session.offer(prompt("Ask before anyone reads."));
        const answer = yield* Effect.promise(() =>
          cli.waitFor((frame) => frame.type === "control_response"),
        );
        assert.lengthOf(calls, 1);
        assert.deepInclude(answer.response as object, { subtype: "success" });
        const messages = yield* session.messages.pipe(Stream.take(3), Stream.runCollect);
        assert.lengthOf(messages, 3);
      }),
    ),
  );

  // Known limit of the ordering, and why the consumer must never wait on a callback's result: on a
  // worker a callback starts only when the consumer asks for more, so a consumer that waits on it
  // would deadlock where in-thread it would not.
  it.effect("starts a callback only when a busy consumer asks for more, unlike in-thread", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner, recorded }) =>
          Effect.gen(function* () {
            const consumerBusy = Promise.withResolvers<void>();
            const callbackArrived = Promise.withResolvers<void>();
            const called = Promise.withResolvers<void>();
            const releaseConsumer = yield* Deferred.make<void>();
            let callbackStarted = false;
            recorded.onEvent = (type) => {
              if (type === "callback") callbackArrived.resolve();
            };
            const cli = new FakeClaudeCli({ pipeLatency: true, onUser: streamThenAsk(1) });
            const session = yield* runner.open(
              openInput(cli, {
                canUseTool: async () => {
                  callbackStarted = true;
                  called.resolve();
                  return { behavior: "allow", updatedInput: {} };
                },
              }),
            );
            yield* session.messages.pipe(
              Stream.runForEach(() =>
                Effect.sync(() => consumerBusy.resolve()).pipe(
                  Effect.andThen(Deferred.await(releaseConsumer)),
                ),
              ),
              Effect.forkChild,
            );
            yield* session.offer(prompt("Stream one frame, then ask."));
            yield* Effect.promise(() => consumerBusy.promise);
            // In-thread the callback runs while the consumer is busy; on a worker it has arrived but waits.
            if (mode === "thread") yield* Effect.promise(() => called.promise);
            else yield* Effect.promise(() => callbackArrived.promise);
            const startedWhileBusy = callbackStarted;
            yield* Deferred.succeed(releaseConsumer, undefined);
            yield* Effect.promise(() => called.promise);
            yield* session.close;
            return startedWhileBusy;
          }),
        );

      assert.isTrue(yield* run("thread"));
      assert.isFalse(yield* run("worker"));
    }),
  );

  it.effect("runs the hooks the query opened with, as the SDK captures them at initialize", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner }) =>
          Effect.gen(function* () {
            const ran: Array<string> = [];
            const matcher: HookCallbackMatcher = {
              matcher: "Bash",
              hooks: [
                async () => {
                  ran.push("opened with");
                  return { continue: true };
                },
              ],
            };
            const hooks: NonNullable<ClaudeQueryOptions["hooks"]> = { PreToolUse: [matcher] };
            const cli = new FakeClaudeCli();
            const session = yield* runner.open(openInput(cli, { hooks }));
            // Neither replacing a matcher's hook nor the event's matchers reaches the open query.
            matcher.hooks[0] = async () => {
              ran.push("replaced");
              return { continue: false };
            };
            hooks.PreToolUse = [];
            const answer = yield* Effect.promise(() =>
              cli.request(hookRequest("hook_0", "toolu-replaced")),
            );
            yield* session.close;
            return { ran, answer: answer.response };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.deepStrictEqual(inThread.ran, ["opened with"]);
      assert.deepStrictEqual(inWorker, inThread);
    }),
  );

  it.effect("relays T3's SubagentStart hook to main and answers it as in-thread", () =>
    Effect.gen(function* () {
      const run = (mode: Mode) =>
        withRunner(mode, ({ runner, recorded }) =>
          Effect.gen(function* () {
            const cli = new FakeClaudeCli();
            const options = makeClaudeQueryOptions({
              modelSelection: CLAUDE_MODEL_SELECTION,
              nativeThreadId: SESSION_ID,
              resume: false,
              cwd: null,
              mcpServers: { "t3-code": { type: "http", url: "http://localhost:3773/mcp" } },
              deviceToolsAvailable: true,
            });
            yield* runner.open({
              ...openInput(cli),
              options: {
                ...options,
                pathToClaudeCodeExecutable: "claude",
                spawnClaudeCodeProcess: cli.spawn,
              },
            });
            const initialize = yield* Effect.promise(() => cli.waitForControl("initialize"));
            const hooks = (initialize.request as { readonly hooks: Record<string, unknown> }).hooks;
            const [matcher] = hooks.SubagentStart as ReadonlyArray<{
              readonly hookCallbackIds: ReadonlyArray<string>;
            }>;
            const answer = yield* Effect.promise(() =>
              cli.request({
                subtype: "hook_callback",
                callback_id: matcher!.hookCallbackIds[0],
                input: {
                  hook_event_name: "SubagentStart",
                  agent_id: "agent-1",
                  agent_type: "general",
                },
              }),
            );
            return { answer, recorded, frames: normalizedFrames(cli.frames) };
          }),
        );
      const inThread = yield* run("thread");
      const inWorker = yield* run("worker");

      assert.deepStrictEqual(inWorker.recorded.opens, [true]);
      assert.include(inWorker.recorded.requests, "callbackResult");
      assert.deepStrictEqual(inWorker.answer, {
        ...inThread.answer,
        request_id: inWorker.answer.request_id,
      });
      assert.deepStrictEqual(inWorker.frames, inThread.frames);
      assert.strictEqual(
        (
          inWorker.answer.response as {
            readonly hookSpecificOutput: { readonly hookEventName: string };
          }
        ).hookSpecificOutput.hookEventName,
        "SubagentStart",
      );
    }),
  );
});

// The server's own wiring, from the environment to the session the runner opens.
const runWired = (env: Record<string, string>) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const baseDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-claude-wiring-" });
    const config = yield* resolveServerConfig(
      {
        mode: Option.some("desktop" as const),
        port: Option.some(4888),
        host: Option.none<string>(),
        baseDir: Option.some(baseDir),
        cwd: Option.none<string>(),
        devUrl: Option.none<URL>(),
        noBrowser: Option.none<boolean>(),
        bootstrapFd: Option.none<number>(),
        autoBootstrapProjectFromCwd: Option.none<boolean>(),
        logWebSocketEvents: Option.none<boolean>(),
        tailscaleServeEnabled: Option.none<boolean>(),
        tailscaleServePort: Option.none<number>(),
        lan: Option.none<boolean>(),
      },
      Option.none(),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(ConfigProvider.layer(ConfigProvider.fromEnv({ env })), NetService.layer),
      ),
    );
    const runnerLayer = layerQueryRunner.pipe(
      Layer.provide(
        layerClaudeAgentSdkWorkerPool.pipe(
          Layer.provideMerge(ProviderIoWorkerPool.layer),
          Layer.provide(Layer.succeed(ServerConfig.ServerConfig, config)),
        ),
      ),
      Layer.provide(PortScanner.layerTest),
      Layer.provide(
        Layer.succeed(ProviderEventLoggers, { native: undefined, canonical: undefined }),
      ),
      Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    );
    const openedBefore = workers.opened.length;
    const messages = yield* Effect.gen(function* () {
      const runner = yield* ClaudeAgentSdkQueryRunner;
      const cli = new FakeClaudeCli({ onUser: playOnPrompt });
      const session = yield* runner.open(openInput(cli));
      yield* session.offer(prompt("Run the tool."));
      return yield* takeTurn(session);
    }).pipe(Effect.provide(runnerLayer));
    return {
      stages: workers.opened.slice(openedBefore).map((opened) => opened.stage),
      last: messages.at(-1)?.type,
    };
  }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));

describe("Claude Agent SDK worker wiring", () => {
  it.effect("runs Claude queries on a worker when the environment opts in", () =>
    Effect.gen(function* () {
      assert.deepStrictEqual(
        yield* runWired({ T3CODE_PROVIDER_IO_WORKERS: "2", T3CODE_PROVIDER_IO_CLAUDE_SDK: "1" }),
        { stages: ["claude-agent-sdk.query"], last: "result" },
      );
    }),
  );

  it.effect("keeps them in-thread with workers but no Claude opt-in, and with neither", () =>
    Effect.gen(function* () {
      for (const env of [{ T3CODE_PROVIDER_IO_WORKERS: "2" }, {}]) {
        assert.deepStrictEqual(yield* runWired(env), { stages: [], last: "result" });
      }
    }),
  );
});

// A pool whose session takes calls and notifications in place, so a test can raise any worker event.
const scriptedPool = () => {
  const notified: Array<ClaudeAgentSdkWorkerRequest> = [];
  const waiters: Array<() => void> = [];
  let raise: ((event: ClaudeAgentSdkWorkerEvent) => void) | undefined;
  let onPoolClose: Effect.Effect<void> | undefined;
  const pool: ProviderIoWorkerPool.ProviderIoWorkerPool["Service"] = {
    open: (input) =>
      Effect.sync(() => {
        raise = input.onEvent as (event: ClaudeAgentSdkWorkerEvent) => void;
        onPoolClose = input.onPoolClose;
        return Option.some({
          write: () => Effect.void,
          end: Effect.void,
          output: Stream.never,
          call: () => Effect.succeed(undefined as never),
          notify: (request: ClaudeAgentSdkWorkerRequest) => {
            notified.push(request);
            for (const wake of waiters.splice(0)) wake();
            return true;
          },
        } as never);
      }),
  };
  const notifiedOf = (type: ClaudeAgentSdkWorkerRequest["type"]) =>
    new Promise<ReadonlyArray<ClaudeAgentSdkWorkerRequest>>((resolve) => {
      const check = () => {
        const matching = notified.filter((request) => request.type === type);
        if (matching.length > 0) resolve(matching);
        else waiters.push(check);
      };
      check();
    });
  return {
    pool,
    raise: (event: ClaudeAgentSdkWorkerEvent) => raise?.(event),
    notifiedOf,
    closePool: Effect.suspend(() => onPoolClose ?? Effect.void),
  };
};

const startScripted = (
  callbacks: Parameters<typeof startClaudeQueryInWorker>[0]["callbacks"],
  spawn: Parameters<typeof startClaudeQueryInWorker>[0]["spawn"] = () => {
    throw new Error("No CLI in this test.");
  },
) =>
  Effect.gen(function* () {
    const scripted = scriptedPool();
    yield* startClaudeQueryInWorker({
      pool: scripted.pool,
      stageOptions: { options: {}, callbacks: ["canUseTool"] },
      callbacks,
      prompt: (async function* () {})(),
      spawn,
      toError: (cause, method) => new ClaudeAgentSdkQueryRunnerError({ cause, method }),
    });
    return scripted;
  });

describe("startClaudeQueryInWorker callbacks", () => {
  it.effect("answers a malformed callback request with an error instead of throwing", () =>
    Effect.gen(function* () {
      const scripted = yield* startScripted({ canUseTool: vi.fn() });
      assert.doesNotThrow(() =>
        scripted.raise({
          type: "callback",
          id: 0,
          after: 0,
          call: { name: "canUseTool" } as never,
        }),
      );
      const [answer] = yield* Effect.promise(() => scripted.notifiedOf("callbackError"));
      assert.deepInclude(answer, { type: "callbackError", id: 0 });
    }),
  );

  it.effect("kills a CLI that ignores SIGTERM before its pool's close returns", () =>
    Effect.gen(function* () {
      const cli = new FakeClaudeCli({ exitOnStdinEnd: false, exitOnKill: false });
      const scripted = yield* startScripted({}, cli.spawn);
      scripted.raise({ type: "spawn", command: "claude", args: [], cwd: undefined, env: {} });
      const closing = yield* Effect.forkChild(scripted.closePool);
      yield* Effect.promise(() => cli.waitForKill("SIGTERM"));
      yield* TestClock.adjust("5 seconds");
      yield* Fiber.join(closing);
      yield* Effect.promise(() => cli.stdinEnded);
      assert.deepStrictEqual(cli.kills, ["SIGTERM", "SIGKILL"]);
    }),
  );

  it.effect("spawns no CLI once its pool has started closing", () =>
    Effect.gen(function* () {
      const spawn = vi.fn(() => {
        throw new Error("No CLI in this test.");
      });
      const scripted = yield* startScripted({}, spawn);
      // A query still starting when the pool closes: its spawn comes after the session's cleanup.
      yield* scripted.closePool;
      scripted.raise({ type: "spawn", command: "claude", args: [], cwd: undefined, env: {} });
      assert.strictEqual(spawn.mock.calls.length, 0);
    }),
  );

  it.effect.each([
    { call: { name: "toString", args: [{}] } },
    { call: { name: "hook", hook: ["PreToolUse", 0, "constructor"], args: [{}, "toolu-1", {}] } },
  ])("answers a request for a callback main does not own with an error: $call.name", ({ call }) =>
    Effect.gen(function* () {
      const scripted = yield* startScripted({
        canUseTool: vi.fn(),
        hooks: { PreToolUse: [{ hooks: [vi.fn()] }] },
      });
      scripted.raise({ type: "callback", id: 0, after: 0, call: call as never });
      const [answer] = yield* Effect.promise(() =>
        Promise.race([scripted.notifiedOf("callbackResult"), scripted.notifiedOf("callbackError")]),
      );
      assert.deepInclude(answer, { type: "callbackError", id: 0 });
    }),
  );

  it.effect(
    "runs main's callback with main's abort signal, first, whatever the worker relays",
    () =>
      Effect.gen(function* () {
        const canUseTool = vi.fn(() => new Promise<never>(() => {}));
        const scripted = yield* startScripted({ canUseTool });
        scripted.raise({
          type: "callback",
          id: 0,
          after: 0,
          call: {
            name: "canUseTool",
            args: ["Bash", { command: "ls" }, { toolUseID: "toolu-1", signal: "relayed" }],
          } as never,
        });
        const [, , options] = canUseTool.mock.calls[0] as unknown as [
          unknown,
          unknown,
          Record<string, unknown>,
        ];
        assert.instanceOf(options.signal, AbortSignal);
        assert.deepStrictEqual(Object.keys(options), ["signal", "toolUseID"]);
      }),
  );

  it.effect("ignores a callback request whose id is already running", () =>
    Effect.gen(function* () {
      const canUseTool = vi.fn(() => new Promise<never>(() => {}));
      const scripted = yield* startScripted({ canUseTool });
      const call = {
        name: "canUseTool",
        args: ["Bash", { command: "ls" }, { toolUseID: "toolu-1" }],
      } as const;
      scripted.raise({ type: "callback", id: 7, after: 0, call: call as never });
      scripted.raise({ type: "callback", id: 7, after: 0, call: call as never });
      assert.strictEqual(canUseTool.mock.calls.length, 1);
    }),
  );
});

describe("claudeAgentSdkWorkerSetup", () => {
  const hook = () => Promise.resolve({});
  const base = { model: "claude-sonnet-4-6", pathToClaudeCodeExecutable: "claude" };

  it("passes data and the hook shape to the worker, and keeps main's callbacks as they are now", () => {
    const canUseTool = vi.fn();
    const onUserDialog = vi.fn();
    const otherHook = vi.fn();
    const preToolUseHooks = [hook, otherHook];
    const setup = claudeAgentSdkWorkerSetup(
      {
        ...base,
        canUseTool,
        onUserDialog,
        spawnClaudeCodeProcess: vi.fn(),
        stderr: vi.fn(),
        hooks: {
          SubagentStart: [{ hooks: [hook] }],
          PreToolUse: [{ matcher: "Bash", timeout: 5, hooks: preToolUseHooks }, { hooks: [] }],
        },
      },
      { "t3-code": { type: "http", url: "http://localhost:3773/mcp" } },
    );
    assert.deepStrictEqual(setup?.stageOptions, {
      options: base,
      callbacks: ["canUseTool", "onUserDialog"],
      hooks: {
        SubagentStart: [{ hooks: 1 }],
        PreToolUse: [{ matcher: "Bash", timeout: 5, hooks: 2 }, { hooks: 0 }],
      },
    });
    assert.deepStrictEqual(setup?.callbacks, {
      canUseTool,
      onUserDialog,
      hooks: {
        SubagentStart: [{ hooks: [hook] }],
        PreToolUse: [{ hooks: [hook, otherHook] }, { hooks: [] }],
      },
    });
    assert.notStrictEqual(setup?.callbacks.hooks?.PreToolUse?.[0]?.hooks, preToolUseHooks);
    const getterMatcher = Object.defineProperties(
      {},
      {
        matcher: { get: () => "Bash", enumerable: false },
        timeout: { value: 5, enumerable: false },
        hooks: { value: [hook] },
      },
    ) as HookCallbackMatcher;
    assert.deepStrictEqual(
      claudeAgentSdkWorkerSetup({ ...base, hooks: { PreToolUse: [getterMatcher] } }, undefined)
        ?.stageOptions,
      {
        options: base,
        callbacks: [],
        hooks: { PreToolUse: [{ matcher: "Bash", timeout: 5, hooks: 1 }] },
      },
    );
    assert.deepStrictEqual(
      claudeAgentSdkWorkerSetup({ ...base, hooks: {} }, undefined)?.stageOptions,
      {
        options: base,
        callbacks: [],
        hooks: {},
      },
    );
  });

  it("keeps a query in-thread when the worker could not run it the same way", () => {
    const cases: ReadonlyArray<
      readonly [ClaudeQueryOptions, Record<string, McpServerConfig> | undefined]
    > = [
      [{ model: "claude-sonnet-4-6" }, undefined],
      [{ ...base, abortController: new AbortController() }, undefined],
      [{ ...base, hooks: { SubagentStart: [{ hooks: ["context" as never] }] } }, undefined],
      [{ ...base, hooks: { SubagentStart: undefined as never } }, undefined],
      [{ ...base, hooks: { SubagentStart: [null as never] } }, undefined],
      [
        { ...base, hooks: { SubagentStart: [{ hooks: [hook], matcher: new Map() as never }] } },
        undefined,
      ],
      [{ ...base, includeHookEvents: vi.fn() as never }, undefined],
      [base, { local: { type: "sdk", name: "local", instance: {} as never } }],
      [{ ...base, sessionStore: { load: vi.fn(), append: vi.fn() } as never }, undefined],
      [{ ...base, sessionStore: new InMemorySessionStore() }, undefined],
      [{ ...base, settings: { env: new Map() } as never }, undefined],
    ];
    for (const [options, mcpServers] of cases) {
      assert.isUndefined(claudeAgentSdkWorkerSetup(options, mcpServers));
    }
  });
});

import type * as NodeWorkerThreads from "node:worker_threads";

import type {
  McpServerConfig,
  Options as ClaudeQueryOptions,
  SDKMessage,
  SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderSessionId, ThreadId } from "@t3tools/contracts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { ProviderEventLoggers } from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import * as ProviderIoWorkerPool from "../../provider/io/ProviderIoWorkerPool.ts";
import * as PortScanner from "../../preview/PortScanner.ts";
import {
  ClaudeAgentSdkQueryRunner,
  type ClaudeAgentSdkQueryRunnerError,
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
  claudeAgentSdkStageOptions,
} from "./ClaudeAgentSdkWorkerQuery.ts";

const workers = vi.hoisted(() => ({ started: new Array<NodeWorkerThreads.Worker>() }));

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      super(...args);
      workers.started.push(this);
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

type Mode = "thread" | "worker";

const SESSION_ID = "11111111-2222-4333-8444-555555555555";

interface Recorded {
  readonly opens: Array<boolean>;
  readonly requests: Array<string>;
}

// Records what crosses to the worker, so a test can tell the worker ran the query.
const recordingPool = (
  pool: ProviderIoWorkerPool.ProviderIoWorkerPool["Service"],
  recorded: Recorded,
): ProviderIoWorkerPool.ProviderIoWorkerPool["Service"] => ({
  open: (input) =>
    pool.open(input).pipe(
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
            session.notify(request);
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
      Layer.provide(NodeServices.layer),
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

  it.effect("refuses callbacks main must answer, without hanging the turn", () =>
    withRunner("worker", ({ runner }) =>
      Effect.gen(function* () {
        const canUseTool = vi.fn();
        const onElicitation = vi.fn();
        const onUserDialog = vi.fn();
        const cli = new FakeClaudeCli();
        yield* runner.open(openInput(cli, { canUseTool, onElicitation, onUserDialog }));
        const permission = yield* Effect.promise(() =>
          cli.request({
            subtype: "can_use_tool",
            tool_name: "Bash",
            input: { command: "ls" },
            tool_use_id: "toolu-1",
          }),
        );
        const elicitation = yield* Effect.promise(() =>
          cli.request({
            subtype: "elicitation",
            mcp_server_name: "t3-code",
            message: "Pick one",
            requested_schema: { type: "object", properties: {} },
          }),
        );
        const dialog = yield* Effect.promise(() =>
          cli.request({
            subtype: "request_user_dialog",
            dialog_kind: "resume_return",
            payload: {},
          }),
        );

        assert.strictEqual(permission.subtype, "success");
        assert.deepInclude(permission.response as object, { behavior: "deny" });
        assert.deepInclude(elicitation.response as object, { action: "decline" });
        assert.deepStrictEqual(dialog.response, { behavior: "cancelled" });
        for (const callback of [canUseTool, onElicitation, onUserDialog]) {
          assert.strictEqual(callback.mock.calls.length, 0);
        }
      }),
    ),
  );

  it.effect("answers T3's SubagentStart hook in the worker as in-thread", () =>
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

describe("claudeAgentSdkStageOptions", () => {
  const hook = () => Promise.resolve({});
  const subagentStart = { hook, context: "device tools" };
  const base = { model: "claude-sonnet-4-6", pathToClaudeCodeExecutable: "claude" };

  it("passes data, refuses main's callbacks and carries T3's SubagentStart context", () => {
    assert.deepStrictEqual(
      claudeAgentSdkStageOptions(
        {
          ...base,
          canUseTool: vi.fn(),
          onUserDialog: vi.fn(),
          spawnClaudeCodeProcess: vi.fn(),
          stderr: vi.fn(),
          hooks: { SubagentStart: [{ hooks: [hook] }] },
        },
        { "t3-code": { type: "http", url: "http://localhost:3773/mcp" } },
        subagentStart,
      ),
      {
        options: base,
        refuse: ["canUseTool", "onUserDialog"],
        subagentStartContext: "device tools",
      },
    );
  });

  it("keeps a query in-thread when the worker could not run it the same way", () => {
    const cases: ReadonlyArray<
      readonly [ClaudeQueryOptions, Record<string, McpServerConfig> | undefined]
    > = [
      [{ model: "claude-sonnet-4-6" }, undefined],
      [{ ...base, abortController: new AbortController() }, undefined],
      [{ ...base, hooks: { SubagentStart: [{ hooks: [vi.fn()] }] } }, undefined],
      [
        { ...base, hooks: { SubagentStart: [{ hooks: [hook] }], Stop: [{ hooks: [hook] }] } },
        undefined,
      ],
      [{ ...base, hooks: { SubagentStart: [{ hooks: [hook], timeout: 5 }] } }, undefined],
      [{ ...base, includeHookEvents: vi.fn() as never }, undefined],
      [base, { local: { type: "sdk", name: "local", instance: {} as never } }],
    ];
    for (const [options, mcpServers] of cases) {
      assert.isUndefined(claudeAgentSdkStageOptions(options, mcpServers, subagentStart));
    }
  });
});

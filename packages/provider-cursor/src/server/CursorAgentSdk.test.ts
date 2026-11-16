import type { ListRunsOptions } from "@cursor/sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderSessionId, ThreadId, ProviderThreadId, RunId } from "@t3tools/contracts";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import { vi } from "vite-plus/test";

import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as CursorAgentSdk from "./CursorAgentSdk.ts";
import * as CursorSdk from "./CursorSdk.ts";

const cursorSdkMock = vi.hoisted(() => {
  const closeExecutionOrder: Array<string> = [];
  const runOperations: Array<string> = [];
  let agentCloseFailure: unknown;
  // The local store lists runs oldest first, like the real SDK.
  let storedRuns: Array<{ readonly id: string; readonly status: string }> = [];
  const runWait = vi.fn(async () => ({
    id: "run-cursor-agent-sdk-test",
    requestId: "request-cursor-agent-sdk-test",
    status: "finished",
    result: "ok",
    model: { id: "default" },
    durationMs: 1,
  }));
  const runCancel = vi.fn(async () => {});
  const runSteer = vi.fn(async () => "complete_delivered" as const);
  const agentClose = vi.fn(() => {
    closeExecutionOrder.push("sdk:agent.close");
    if (agentCloseFailure !== undefined) {
      throw agentCloseFailure;
    }
  });
  const send = vi.fn(
    async (
      _message: unknown,
      options: { readonly onDelta?: (input: { readonly update: unknown }) => Promise<void> },
    ) => {
      runOperations.push("run.start");
      if (storedRuns.some((run) => run.status === "running")) {
        throw new Error("Agent agent-cursor-agent-sdk-test already has active run");
      }
      await options.onDelta?.({
        update: {
          type: "assistant-message-chunk",
          text: "hello",
        },
      });
      return {
        id: "run-cursor-agent-sdk-test",
        agentId: "agent-cursor-agent-sdk-test",
        wait: runWait,
        cancel: runCancel,
        steer: runSteer as typeof runSteer | undefined,
      };
    },
  );
  const create = vi.fn(async () => ({
    agentId: "agent-cursor-agent-sdk-test",
    send,
    close: agentClose,
  }));
  const resume = vi.fn(async () => ({
    agentId: "agent-cursor-agent-sdk-test",
    send,
    close: agentClose,
  }));
  const listRuns = vi.fn(async (_agentId: string, options?: ListRunsOptions) => {
    runOperations.push("agent.listRuns");
    // Mirrors the SDK's opaque cursor pagination with its default page size of 50.
    const offset = options?.cursor === undefined ? 0 : Number(options.cursor);
    const end = offset + (options?.limit ?? 50);
    return {
      items: storedRuns.slice(offset, end),
      ...(end < storedRuns.length ? { nextCursor: String(end) } : {}),
    };
  });
  const cancelRun = vi.fn(async (runId: string) => {
    runOperations.push("agent.cancelRun");
    storedRuns = storedRuns.map((run) =>
      run.id === runId ? { ...run, status: "cancelled" } : run,
    );
  });

  return {
    agentClose,
    cancelRun,
    closeExecutionOrder,
    create,
    listRuns,
    resume,
    runOperations,
    runCancel,
    runSteer,
    runWait,
    send,
    setAgentCloseFailure: (failure: unknown) => {
      agentCloseFailure = failure;
    },
    setStoredRuns: (runs: Array<{ readonly id: string; readonly status: string }>) => {
      storedRuns = runs;
    },
  };
});

vi.mock("./sdk.ts", () => ({
  loadCursorSdk: () => ({
    CursorAgentPlatform: class {
      async acquireLocalExecutor() {
        throw new Error("unused executor");
      }
    },
    Agent: {
      create: cursorSdkMock.create,
      resume: cursorSdkMock.resume,
      listRuns: cursorSdkMock.listRuns,
      cancelRun: cursorSdkMock.cancelRun,
      messages: {
        list: vi.fn(async () => []),
      },
    },
    createAgentPlatform: vi.fn(),
  }),
  loadCursorSqlite: () => ({ SqliteLocalAgentStore: { open: vi.fn() } }),
}));

const layerTest = CursorAgentSdk.layer.pipe(
  Layer.provide(
    Layer.succeed(ProviderEventLoggers.ProviderEventLoggers, {
      native: {
        filePath: "cursor-agent-sdk-test.log",
        write: (event: unknown) =>
          Effect.sync(() => {
            if (
              typeof event === "object" &&
              event !== null &&
              Reflect.get(Reflect.get(event, "event"), "payload")?.type === "agent.close"
            ) {
              cursorSdkMock.closeExecutionOrder.push("protocol:agent.close");
            }
          }),
        close: () => Effect.void,
      },
      canonical: undefined,
    }),
  ),
  Layer.provide(CursorSdk.layer),
  Layer.provide(NodeServices.layer),
);

describe("CursorAgentSdkRunner", () => {
  it.effect.each(["model", "checkpoint", "executor", "incoming"] as const)(
    "cancels an installed Cursor SDK start during %s",
    (stage) =>
      Effect.scoped(
        Effect.gen(function* () {
          const real = (yield* Effect.promise(() =>
            vi.importActual<typeof import("./sdk.ts")>("./sdk.ts"),
          )).loadCursorSdk();
          const fs = yield* FileSystem.FileSystem;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "cursor-submission-" });
          const platform = yield* Effect.promise(() =>
            real.createAgentPlatform({
              localStore: new real.JsonlLocalAgentStore(directory),
              workspaceRef: directory,
            }),
          );
          const entered = Promise.withResolvers<void>();
          const release = Promise.withResolvers<void>();
          const settled = Promise.withResolvers<void>();
          const cancellationComplete = Promise.withResolvers<void>();
          const resumeEntered = Promise.withResolvers<void>();
          let loggedStarts = 0;
          let activeStarts = 0;
          let sendCalls = 0;
          let overlappingStarts = 0;
          const done = Promise.withResolvers<void>();
          let sending = false;
          let stopped = false;
          let claims = 0;
          let cancellations = 0;
          let nativeRun: import("@cursor/sdk").Run | undefined;
          const received: Array<unknown> = [];
          using _model = vi
            .spyOn(platform, "resolveLocalModelSelection")
            .mockImplementation(async (model) => {
              if (sending && stage === "model") {
                entered.resolve();
                await release.promise;
              }
              return model;
            });
          const loadLatest = platform.checkpointStore.loadLatest.bind(platform.checkpointStore);
          let checkpointLoads = 0;
          using _checkpoint = vi
            .spyOn(platform.checkpointStore, "loadLatest")
            .mockImplementation(async (id) => {
              if (stage === "checkpoint" && ++checkpointLoads === 1) {
                entered.resolve();
                await release.promise;
              }
              return loadLatest(id);
            });
          using _executor = vi
            .spyOn(real.CursorAgentPlatform.prototype, "acquireLocalExecutor")
            .mockResolvedValue({
              cacheMiss: true,
              release: async () => {},
              handle: {
                run: async (...args: Array<unknown>) => {
                  received.push(args[0]);
                  if (stage === "executor") {
                    entered.resolve();
                    await release.promise;
                  }
                  return {
                    abort: () => {
                      cancellations += 1;
                      done.resolve();
                    },
                    done: done.promise,
                  };
                },
              },
            });
          using _create = vi.spyOn(real.Agent, "create").mockImplementation(async (options) => {
            const agent = await platform.createAgent(options);
            const send = agent.send.bind(agent);
            agent.send = (...args) => {
              const index = ++sendCalls;
              if (activeStarts > 0) overlappingStarts += 1;
              activeStarts += 1;
              return send(...args)
                .then((run) => {
                  nativeRun = run;
                  const cancel = run.cancel.bind(run);
                  run.cancel = () => cancel().finally(() => cancellationComplete.resolve());
                  return run;
                })
                .finally(() => {
                  activeStarts -= 1;
                  if (index === 1) settled.resolve();
                });
            };
            return agent;
          });
          const runner = yield* CursorAgentSdk.makeCursorAgentSdkRunner(
            () => (event) =>
              event.payload.type === "run.start"
                ? Effect.sync(() => {
                    if (++loggedStarts === 2) resumeEntered.resolve();
                  })
                : stage === "incoming" && event.payload.type === "run.started"
                  ? Effect.promise(async () => {
                      entered.resolve();
                      await release.promise;
                    })
                  : Effect.void,
          ).pipe(
            Effect.provideService(CursorSdk.CursorSdk, {
              load: () => real,
              loadSqlite: () => {
                throw new Error("unused SQLite");
              },
            }),
          );
          const session = yield* runner.open({
            operation: "create",
            options: { model: { id: "default" }, local: { cwd: directory, settingSources: [] } },
            threadId: ThreadId.make("cursor-submit"),
            providerSessionId: ProviderSessionId.make("cursor-submit"),
          });
          const submit: NonNullable<CursorAgentSdk.CursorAgentSdkSendInput<never>["submit"]> = (
            enqueue,
          ) =>
            Effect.suspend(() =>
              stopped
                ? Effect.fail(
                    new ProviderAdapter.ProviderAdapterTurnStartError({
                      driver: CursorAgentSdk.CURSOR_PROVIDER,
                      threadId: ThreadId.make("cursor-submit"),
                      providerThreadId: ProviderThreadId.make("cursor-submit"),
                      runId: RunId.make("cursor-submit"),
                      cause: "Stopped",
                    }),
                  )
                : Effect.sync(() => {
                    claims += 1;
                    enqueue();
                  }),
            );
          sending = true;
          const fiber = yield* session.send({ message: "once", submit }).pipe(Effect.forkScoped);
          yield* Effect.promise(() => entered.promise);
          stopped = true;
          yield* Fiber.interrupt(fiber);
          if (stage === "checkpoint") {
            stopped = false;
            const resumedFiber = yield* session
              .send({ message: "once", submit })
              .pipe(Effect.forkScoped);
            yield* Effect.promise(() => resumeEntered.promise);
            release.resolve();
            const resumed = yield* Fiber.await(resumedFiber);
            yield* Effect.promise(() => settled.promise);
            done.resolve();
            if (Exit.isSuccess(resumed)) yield* resumed.value.wait;
            yield* session.close;
            assert.isTrue(
              Exit.isSuccess(resumed),
              "Resume must survive the previous SDK reservation",
            );
            assert.equal(overlappingStarts, 0);
            assert.equal(claims, 1);
            assert.lengthOf(received, 1);
            return;
          }
          release.resolve();
          yield* Effect.promise(() => settled.promise);
          if (stage !== "model") yield* Effect.promise(() => cancellationComplete.promise);
          if (nativeRun !== undefined) yield* Effect.promise(() => nativeRun!.wait());
          if (stage === "model") {
            assert.equal(claims, 0);
            assert.deepStrictEqual(received, []);
            stopped = false;
            const resumed = yield* session.send({ message: "once", submit });
            done.resolve();
            yield* resumed.wait;
            assert.equal(claims, 1);
            assert.lengthOf(received, 1);
            assert.equal(Reflect.get(received[0] as object, "text"), "once");
          } else {
            assert.equal(claims, 1);
            assert.lengthOf(received, 1);
            assert.isAbove(cancellations, 0);
          }
          yield* session.close;
        }).pipe(Effect.provide(NodeServices.layer)),
      ),
  );

  it.effect("resumes from the original store after the working directory changes", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const workspace = yield* fileSystem.makeTempDirectoryScoped({ prefix: "cursor-resume-" });
      const loaders = yield* Effect.promise(() =>
        vi.importActual<typeof import("./sdk.ts")>("./sdk.ts"),
      );
      const sdk = loaders.loadCursorSdk();
      const store = new sdk.JsonlLocalAgentStore(workspace);
      const storeCwd = `${workspace}/checkout`;
      const cwd = `${workspace}/worktree`;
      const platform = yield* Effect.promise(() =>
        sdk.createAgentPlatform({ localStore: store, workspaceRef: storeCwd }),
      );
      const { agent, run } = yield* Effect.promise(() =>
        platform.store.createAgent({ agentId: "agent-moved", workspaceRef: storeCwd }),
      );
      const runner = yield* CursorAgentSdk.makeCursorAgentSdkRunner(() => undefined).pipe(
        Effect.provideService(CursorSdk.CursorSdk, {
          load: loaders.loadCursorSdk,
          loadSqlite: loaders.loadCursorSqlite,
        }),
      );
      const session = yield* runner.open({
        operation: "resume",
        agentId: agent.agentId,
        storeCwd,
        options: { apiKey: "synthetic-cursor-key", local: { cwd, store } },
        threadId: ThreadId.make("moved-workspace"),
        providerSessionId: ProviderSessionId.make("moved-workspace"),
      });
      assert.equal(session.agentId, agent.agentId);
      assert.deepStrictEqual(yield* session.listMessages, []);
      assert.strictEqual(yield* session.readContextUsage!, undefined);

      // Cursor's conversation holds a run's request only once that run saved its own checkpoint.
      assert.isFalse(yield* session.runCheckpointed!());
      yield* Effect.promise(() =>
        platform.store.patchCheckpoint(agent.agentId, run.runId, {
          blobId: "checkpoint-1",
          storeKind: "local",
        }),
      );
      assert.isTrue(yield* session.runCheckpointed!());
      yield* Effect.promise(() =>
        platform.store.markRunTerminal(agent.agentId, run.runId, { status: "CANCELLED" }),
      );
      const followUp = yield* Effect.promise(() =>
        platform.store.createFollowUpRun(agent.agentId, {}),
      );
      assert.isFalse(yield* session.runCheckpointed!(), "the follow-up has no checkpoint yet");
      assert.isTrue(yield* session.runCheckpointed!(run.runId));
      assert.isFalse(yield* session.runCheckpointed!(followUp.runId));
      yield* session.close;
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("constructs without loading and reports first-use load failures as runner errors", () =>
    Effect.gen(function* () {
      const failure = new Error("missing SDK chunk");
      const load = vi.fn(() => {
        throw failure;
      });
      const runner = yield* CursorAgentSdk.makeCursorAgentSdkRunner(() => undefined).pipe(
        Effect.provideService(CursorSdk.CursorSdk, {
          load,
          loadSqlite: () => {
            throw new Error("unused SQLite");
          },
        }),
      );
      assert.strictEqual(load.mock.calls.length, 0);
      const result = yield* runner
        .open({
          operation: "create",
          options: {},
          threadId: ThreadId.make("lazy-load"),
          providerSessionId: ProviderSessionId.make("lazy-load"),
        })
        .pipe(Effect.exit);
      assert(Exit.isFailure(result));
      assert.deepStrictEqual(
        Cause.squash(result.cause),
        new CursorAgentSdk.CursorAgentSdkRunnerError({
          method: "agent.create",
          cause: failure,
        }),
      );
      assert.strictEqual(load.mock.calls.length, 1);
    }),
  );

  it.effect("retries a resumed send after cancelling its abandoned local run", () =>
    Effect.gen(function* () {
      cursorSdkMock.runOperations.length = 0;
      cursorSdkMock.listRuns.mockClear();
      cursorSdkMock.cancelRun.mockClear();
      cursorSdkMock.setStoredRuns([
        ...Array.from({ length: 50 }, (_, index) => ({
          id: `run-terminal-${index}`,
          status: index === 0 ? "cancelled" : "finished",
        })),
        { id: "run-stale", status: "running" },
      ]);

      const runner = yield* CursorAgentSdk.CursorAgentSdkRunner;
      const session = yield* runner.open({
        operation: "resume",
        agentId: "agent-cursor-agent-sdk-test",
        options: { model: { id: "default" }, mode: "agent", local: { cwd: process.cwd() } },
        threadId: ThreadId.make("thread-cursor-agent-sdk-stale-run-test"),
        providerSessionId: ProviderSessionId.make(
          "provider-session-cursor-agent-sdk-stale-run-test",
        ),
      });
      assert.deepStrictEqual(cursorSdkMock.runOperations, []);
      const run = yield* session.send({ message: "continue" });
      yield* run.wait;

      assert.deepStrictEqual(cursorSdkMock.runOperations, [
        "run.start",
        "agent.listRuns",
        "agent.listRuns",
        "agent.cancelRun",
        "run.start",
      ]);
      assert.deepStrictEqual(cursorSdkMock.listRuns.mock.calls[0], [
        "agent-cursor-agent-sdk-test",
        { runtime: "local", cwd: process.cwd() },
      ]);
      assert.strictEqual(cursorSdkMock.listRuns.mock.calls.length, 2);
      assert.deepStrictEqual(cursorSdkMock.listRuns.mock.calls[1], [
        "agent-cursor-agent-sdk-test",
        { runtime: "local", cwd: process.cwd(), cursor: "50" },
      ]);
      assert.deepStrictEqual(cursorSdkMock.cancelRun.mock.calls[0], [
        "run-stale",
        {
          runtime: "local",
          cwd: process.cwd(),
        },
      ]);
    }).pipe(Effect.provide(layerTest)),
  );

  it.effect("forwards the agent's own shell stream output as shell-output-delta", () =>
    Effect.gen(function* () {
      const publish = (conversationId: string, data: string) =>
        (Reflect.get(globalThis, Symbol.for("@cursor/sdk.shellStreamOutput")) as Function)({
          conversationId,
          toolCallId: "tool-shell",
          event: { case: "stdout", value: { data } },
        });
      cursorSdkMock.runWait.mockImplementationOnce(async () => {
        publish("agent-cursor-agent-sdk-test", "tick 1\n");
        publish("agent-someone-else", "not ours\n");
        return {
          id: "run-cursor-agent-sdk-test",
          requestId: "request-cursor-agent-sdk-test",
          status: "finished",
          result: "ok",
          model: { id: "default" },
          durationMs: 1,
        };
      });
      const updates: Array<unknown> = [];
      const runner = yield* CursorAgentSdk.CursorAgentSdkRunner;
      const session = yield* runner.open({
        operation: "create",
        options: {},
        threadId: ThreadId.make("thread-cursor-agent-sdk-shell"),
        providerSessionId: ProviderSessionId.make("provider-session-cursor-agent-sdk-shell"),
      });
      const run = yield* session.send({
        message: "run it",
        onDelta: (update) => Effect.sync(() => updates.push(update)),
      });
      yield* run.wait;
      publish("agent-cursor-agent-sdk-test", "after the run\n");

      assert.deepStrictEqual(
        updates.filter(
          (update) => (update as { readonly type: string }).type === "shell-output-delta",
        ),
        [
          {
            type: "shell-output-delta",
            event: { case: "stdout", value: { data: "tick 1\n" }, toolCallId: "tool-shell" },
          },
        ],
      );
    }).pipe(Effect.provide(layerTest)),
  );

  it.effect("forwards Run.steer and reports a missing method as a follow-up", () =>
    Effect.gen(function* () {
      cursorSdkMock.runSteer.mockClear();
      const runner = yield* CursorAgentSdk.CursorAgentSdkRunner;
      const session = yield* runner.open({
        operation: "create",
        options: {},
        threadId: ThreadId.make("thread-cursor-agent-sdk-steer"),
        providerSessionId: ProviderSessionId.make("provider-session-cursor-agent-sdk-steer"),
      });
      const run = yield* session.send({ message: "continue" });
      assert.equal(yield* run.steer!("keep going"), "complete_delivered");
      assert.deepStrictEqual(cursorSdkMock.runSteer.mock.calls, [["keep going"]]);

      cursorSdkMock.runSteer.mockImplementationOnce(async () => {
        throw new Error("steer transport failed");
      });
      const failed = yield* run.steer!("again").pipe(Effect.exit);
      assert(Exit.isFailure(failed));

      cursorSdkMock.send.mockImplementationOnce(async () => ({
        id: "run-without-steer",
        agentId: "agent-cursor-agent-sdk-test",
        wait: cursorSdkMock.runWait,
        cancel: cursorSdkMock.runCancel,
        steer: undefined,
      }));
      const plain = yield* session.send({ message: "plain" });
      assert.equal(yield* plain.steer!("later"), "revert_to_followup");
    }).pipe(Effect.provide(layerTest)),
  );
});

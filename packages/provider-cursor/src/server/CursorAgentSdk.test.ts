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
  let staleRunActive = false;
  const runWait = vi.fn(async () => ({
    id: "run-cursor-agent-sdk-test",
    requestId: "request-cursor-agent-sdk-test",
    status: "finished",
    result: "ok",
    model: { id: "default" },
    durationMs: 1,
  }));
  const runCancel = vi.fn(async () => {});
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
      if (staleRunActive) {
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
  const listRuns = vi.fn(async () => {
    runOperations.push("agent.listRuns");
    return {
      items: staleRunActive ? [{ id: "run-stale", status: "running" }] : [],
    };
  });
  const cancelRun = vi.fn(async () => {
    runOperations.push("agent.cancelRun");
    staleRunActive = false;
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
    runWait,
    send,
    setAgentCloseFailure: (failure: unknown) => {
      agentCloseFailure = failure;
    },
    setStaleRunActive: (active: boolean) => {
      staleRunActive = active;
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
      cursorSdkMock.setStaleRunActive(true);

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
        "agent.cancelRun",
        "run.start",
      ]);
      assert.deepStrictEqual(cursorSdkMock.listRuns.mock.calls[0], [
        "agent-cursor-agent-sdk-test",
        { runtime: "local", cwd: process.cwd(), limit: 1 },
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
});

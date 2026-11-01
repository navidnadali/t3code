import type { CanUseTool, PermissionResult, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import type {
  ProviderAdapterV2,
  ProviderAdapterV2Event,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderEventLoggers } from "@t3tools/provider-core/server/ProviderEventLoggers";
import type { ProviderReplayGate } from "@t3tools/provider-testing/replayGate";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import type { ProviderIoWorkerPool } from "../../provider/io/ProviderIoWorkerPool.ts";
import * as PortScanner from "../../preview/PortScanner.ts";
import * as ClaudeAdapterV2 from "./ClaudeAdapterV2.ts";
import {
  ClaudeOrchestratorReplayHarness,
  layerClaudeProviderAdapterRegistryReplay,
  makeReplayQueryRunner,
  nativeSessionIdFor,
  replayQueryRunnerError,
} from "./ClaudeAdapterV2.testkit.ts";
import { FakeClaudeCli, type FakeClaudeCliAnswer } from "./ClaudeAgentSdkFakeCli.testkit.ts";
import { ClaudeAgentSdkWorkerPool } from "./ClaudeAgentSdkWorkerQuery.ts";

type ClaudeReplayTranscript = Parameters<typeof makeReplayQueryRunner>[0];

export interface ClaudeWireReplayRecording {
  readonly adapterEvents: Array<ProviderAdapterV2Event>;
  readonly protocolLogs: Array<unknown>;
}

const settled = (promise: Promise<unknown>, response: (value: unknown) => unknown = () => ({})) =>
  promise.then(
    (value): FakeClaudeCliAnswer => ({ response: response(value) }),
    (error: unknown): FakeClaudeCliAnswer => ({ error: String(error) }),
  );

/**
 * The real query runner (in-thread or on a provider I/O worker) over a CLI that plays the
 * transcript: the SDK's prompts and control requests advance the replay, the replay's frames go
 * to stdout as stream-json, and permission requests reach the SDK as `can_use_tool`.
 */
const layerWireReplayQueryRunner = (
  transcript: ClaudeReplayTranscript,
  replayGate: ProviderReplayGate | undefined,
  input: {
    readonly pool: ProviderIoWorkerPool["Service"] | undefined;
    readonly recording: ClaudeWireReplayRecording;
  },
) =>
  Layer.effect(
    ClaudeAdapterV2.ClaudeAgentSdkQueryRunner,
    Effect.gen(function* () {
      const real = yield* ClaudeAdapterV2.ClaudeAgentSdkQueryRunner;
      const replay = makeReplayQueryRunner(
        transcript,
        replayGate === undefined ? {} : { replayGate },
      );
      yield* Effect.addFinalizer(() => Effect.sync(() => replay.assertComplete()));
      const context = yield* Effect.context<never>();
      const runFork = Effect.runForkWith(context);
      const runPromise = Effect.runPromiseWith(context);
      const replayed = <A>(run: () => A) =>
        Effect.try({ try: run, catch: (cause) => replayQueryRunnerError(transcript, cause) });

      const open = Effect.fn("ClaudeWireReplay.open")(function* (
        openInput: ClaudeAdapterV2.ClaudeAgentSdkQueryOpenInput,
      ) {
        let session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession | undefined;
        const call = (
          effect: (
            session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession,
          ) => Effect.Effect<unknown, ClaudeAdapterV2.ClaudeAgentSdkQueryRunnerError>,
        ) =>
          session === undefined
            ? Promise.reject(new Error("The replayed query has not opened."))
            : runPromise(effect(session));
        const cli = new FakeClaudeCli({
          exitOnStdinEnd: false,
          pipeLatency: true,
          onUser: (frame) => {
            void call((replaying) => replaying.offer(frame as unknown as SDKUserMessage)).catch(
              () => {},
            );
          },
          control: (request) => {
            switch (request.subtype) {
              case "set_model":
                return settled(call((replaying) => replaying.setModel(String(request.model))));
              case "set_permission_mode":
                return settled(
                  call((replaying) =>
                    replaying.setPermissionMode(
                      request.mode as Parameters<typeof replaying.setPermissionMode>[0],
                    ),
                  ),
                );
              case "apply_flag_settings":
                return settled(
                  call((replaying) =>
                    replaying.applyFlagSettings(
                      request.settings as Parameters<typeof replaying.applyFlagSettings>[0],
                    ),
                  ),
                );
              case "stop_task":
                return settled(call((replaying) => replaying.stopTask(String(request.task_id))));
              case "background_tasks":
                return settled(
                  call((replaying) => replaying.backgroundTasks(String(request.tool_use_id))),
                  (backgrounded) => ({ backgrounded }),
                );
              case "interrupt":
                return settled(call((replaying) => replaying.interrupt));
              default:
                return undefined;
            }
          },
        });
        const canUseTool: CanUseTool = async (toolName, toolInput, options) => {
          const answer = await cli.request({
            subtype: "can_use_tool",
            tool_name: toolName,
            input: toolInput,
            tool_use_id: options.toolUseID,
            ...(options.suggestions === undefined
              ? {}
              : { permission_suggestions: options.suggestions }),
            ...(options.blockedPath === undefined ? {} : { blocked_path: options.blockedPath }),
            ...(options.decisionReason === undefined
              ? {}
              : { decision_reason: options.decisionReason }),
            ...(options.title === undefined ? {} : { title: options.title }),
            ...(options.displayName === undefined ? {} : { display_name: options.displayName }),
            ...(options.description === undefined ? {} : { description: options.description }),
            ...(options.agentID === undefined ? {} : { agent_id: options.agentID }),
          });
          if (answer.subtype !== "success") throw new Error(String(answer.error));
          return answer.response as PermissionResult;
        };
        session = yield* replayed(() =>
          replay.open({ ...openInput, options: { ...openInput.options, canUseTool } }),
        );
        // Like the CLI, this one exits when its input ends, but only once it has written every frame
        // the transcript has before the next outbound one, so a close never races those frames.
        let written = 0;
        let inputEnded = false;
        const drained = () =>
          inputEnded && replay.awaitsOutbound() && written === replay.inboundYielded();
        const playing = runFork(
          session.messages.pipe(
            Stream.takeUntilEffect((message) =>
              Effect.sync(() => {
                cli.write(message);
                written += 1;
                return drained();
              }),
            ),
            Stream.runDrain,
            Effect.match({
              onSuccess: () => cli.exit(0, null),
              onFailure: () => cli.exit(1, null),
            }),
          ),
        );
        cli.stdin.once("end", () => {
          inputEnded = true;
          if (drained()) cli.exit(0, null);
        });
        // A CLI that exits (closed, killed) stops playing, so it cannot take the next query's frames.
        cli.once("exit", () => runFork(Fiber.interrupt(playing)));
        return yield* real.open({
          ...openInput,
          options: { ...openInput.options, spawnClaudeCodeProcess: cli.spawn },
        });
      });

      return ClaudeAdapterV2.ClaudeAgentSdkQueryRunner.of({
        allocateSessionId: Effect.succeed(nativeSessionIdFor(transcript)),
        open,
        forkSession: (forkInput) => replayed(() => replay.forkSession(forkInput)),
        subagentLaunchToolUseId: (lookup) => replayed(() => replay.subagentLaunchToolUseId(lookup)),
        assertComplete: replayed(() => replay.assertComplete()),
      });
    }),
  ).pipe(
    Layer.provide(
      ClaudeAdapterV2.layerQueryRunner.pipe(
        Layer.provide(PortScanner.layerTest),
        Layer.provide(
          Layer.succeed(ProviderEventLoggers, {
            native: {
              filePath: "/dev/null",
              write: (event) => Effect.sync(() => input.recording.protocolLogs.push(event)),
              close: () => Effect.void,
            },
            canonical: undefined,
          }),
        ),
        Layer.provide(Layer.succeed(ClaudeAgentSdkWorkerPool, input.pool)),
        Layer.provide(NodeServices.layer),
      ),
    ),
  );

const recordAdapterEvents = (
  adapter: ProviderAdapterV2["Service"],
  events: Array<ProviderAdapterV2Event>,
): ProviderAdapterV2["Service"] => ({
  ...adapter,
  openSession: (openInput) =>
    adapter.openSession(openInput).pipe(
      Effect.map((runtime) => ({
        ...runtime,
        events: runtime.events.pipe(Stream.tap((event) => Effect.sync(() => events.push(event)))),
      })),
    ),
});

/** The Claude replay harness at the SDK's wire level, with the SDK in-thread or on `pool`. */
export function makeClaudeWireReplayHarness(input: {
  readonly pool: ProviderIoWorkerPool["Service"] | undefined;
  readonly recording: ClaudeWireReplayRecording;
}): typeof ClaudeOrchestratorReplayHarness {
  return {
    ...ClaudeOrchestratorReplayHarness,
    makeProviderAdapterRegistryLayer: (transcript, options) =>
      layerClaudeProviderAdapterRegistryReplay(transcript, {
        ...options,
        queryRunnerLayer: layerWireReplayQueryRunner(transcript, options?.replayGate, input),
        wrapAdapter: (adapter) => recordAdapterEvents(adapter, input.recording.adapterEvents),
      }),
  };
}

import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { CodexSettings, type ProviderReplayTranscript } from "@t3tools/contracts";
import * as CodexClient from "effect-codex-app-server/client";
import type * as CodexError from "effect-codex-app-server/errors";
import * as CodexReplay from "effect-codex-app-server/replay";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import * as ServerConfig from "../../config.ts";
import { CodexLineOffloads } from "../../provider/io/ProviderIoOffloads.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import { ProviderAdapterDriverCreateError } from "@t3tools/provider-core/server/adapterDriver";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import {
  makeReplayServerConfig,
  type OrchestratorV2ProviderReplayHarness,
} from "../testkit/ProviderReplayHarness.ts";
import type { ProviderReplayGate } from "@t3tools/provider-testing/replayGate";
import { ReplayStdoutChunking } from "../testkit/ReplayStdoutChunking.ts";
import * as CodexAdapterV2 from "./CodexAdapterV2.ts";

const DEFAULT_CODEX_SETTINGS = Schema.decodeSync(CodexSettings)({});

export class CodexReplayTranscriptDecodeError extends Schema.TaggedError<CodexReplayTranscriptDecodeError>()(
  "CodexReplayTranscriptDecodeError",
  {
    driver: Schema.optional(Schema.String),
    protocol: Schema.optional(Schema.String),
    scenario: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to decode Codex app-server replay transcript for scenario ${this.scenario ?? "<unknown>"}.`;
  }
}

export const CodexOrchestratorReplayHarnessError = Schema.Union([
  CodexReplayTranscriptDecodeError,
  CodexReplay.CodexAppServerReplayError,
  ProviderAdapterDriverCreateError,
]);
export type CodexOrchestratorReplayHarnessError = typeof CodexOrchestratorReplayHarnessError.Type;

export function withCodexReplayChildMetadata(
  client: CodexClient.CodexAppServerClient["Service"],
  transcript: CodexReplay.CodexAppServerReplayTranscript,
  readMetadata: (
    threadId: string,
    method: "thread/read" | "thread/resume",
  ) => Effect.Effect<unknown, CodexError.CodexAppServerError> = (threadId) =>
    Effect.succeed({ thread: { id: threadId }, model: null }),
): CodexClient.CodexAppServerClient["Service"] {
  const childThreadIds = new Set(
    transcript.entries.flatMap((entry) => {
      if (entry.type !== "emit_inbound" || !Predicate.isObject(entry.frame)) return [];
      const params = entry.frame.params;
      if (!Predicate.isObject(params) || !Predicate.isObject(params.item)) return [];
      const item = params.item;
      if (item.type === "subAgentActivity" && typeof item.agentThreadId === "string") {
        return [item.agentThreadId];
      }
      return item.type === "collabAgentToolCall" && Array.isArray(item.receiverThreadIds)
        ? item.receiverThreadIds.filter(Predicate.isString)
        : [];
    }),
  );
  return {
    ...client,
    raw: {
      ...client.raw,
      request: (method, params) =>
        (method === "thread/read" || method === "thread/resume") &&
        Predicate.isObject(params) &&
        (method === "thread/read" ? params.includeTurns === false : params.excludeTurns === true) &&
        typeof params.threadId === "string" &&
        childThreadIds.has(params.threadId)
          ? readMetadata(params.threadId, method)
          : client.raw.request(method, params),
    },
  };
}

function metadataFromTranscript(transcript: ProviderReplayTranscript): {
  readonly provider?: string;
  readonly protocol?: string;
  readonly scenario?: string;
} {
  return {
    provider: transcript.provider,
    protocol: transcript.protocol,
    scenario: transcript.scenario,
  };
}

export function layer(input: {
  readonly transcript: CodexReplay.CodexAppServerReplayTranscript;
  readonly driver?: CodexReplay.CodexAppServerReplayDriver;
}) {
  // Like the real factory, replays log and offload long lines when those services are provided.
  const layerReplayClientFactory = Layer.effect(
    CodexAdapterV2.CodexAppServerClientFactory,
    Effect.gen(function* () {
      const loggers = yield* Effect.serviceOption(ProviderEventLoggers.ProviderEventLoggers);
      const nativeEventLogger = Option.isSome(loggers) ? loggers.value.native : undefined;
      const offloads = yield* Effect.serviceOption(CodexLineOffloads);
      const chunking = yield* Effect.serviceOption(ReplayStdoutChunking);
      return CodexAdapterV2.CodexAppServerClientFactory.of({
        open: (openInput) =>
          Effect.gen(function* () {
            const logger = CodexAdapterV2.makeCodexAppServerProtocolLogger({
              nativeEventLogger,
              threadId: openInput.threadId,
              providerSessionId: openInput.providerSessionId,
            });
            const writeSerialized = nativeEventLogger?.writeSerialized;
            // As the real factory; a replay has no process for a dead worker to stop.
            const lineOffload =
              Option.isNone(offloads) ||
              (nativeEventLogger !== undefined && writeSerialized === undefined)
                ? undefined
                : Option.getOrUndefined(
                    yield* offloads.value.open({
                      kill: Effect.void,
                      ...(writeSerialized === undefined
                        ? {}
                        : {
                            log: {
                              providerSessionId: openInput.providerSessionId,
                              writeSerialized: (record) =>
                                writeSerialized(record, openInput.threadId).pipe(Effect.ignore),
                            },
                          }),
                    }),
                  );
            const options = {
              ...(logger === undefined ? {} : { logIncoming: true, logOutgoing: true, logger }),
              ...(lineOffload === undefined ? {} : { lineOffload }),
            };
            const inbound = Option.isSome(chunking)
              ? { split: chunking.value.makeSplitter() }
              : undefined;
            const layerReplay =
              input.driver === undefined
                ? CodexReplay.layerReplay(input.transcript, options, inbound)
                : CodexReplay.layerReplayWithDriver(input.driver, options, inbound);
            const context = yield* Layer.build(layerReplay).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapter.ProviderAdapterOpenSessionError({
                    driver: CodexAdapterV2.CODEX_DRIVER_KIND,
                    providerSessionId: openInput.providerSessionId,
                    cause,
                  }),
              ),
            );
            return yield* Effect.service(CodexClient.CodexAppServerClient).pipe(
              Effect.map((client) => withCodexReplayChildMetadata(client, input.transcript)),
              Effect.provide(context),
            );
          }),
      });
    }),
  );
  const layerServerConfig = Layer.effect(
    ServerConfig.ServerConfig,
    makeReplayServerConfig(input.transcript.scenario).pipe(Effect.orDie),
  ).pipe(Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));
  const layerRegistry = ProviderAdapterRegistry.layerFromAdaptersEffect(
    CodexAdapterV2.createCodexAdapterV2({
      instanceId: CodexAdapterV2.CODEX_DEFAULT_INSTANCE_ID,
      displayName: undefined,
      environment: [],
      enabled: true,
      config: DEFAULT_CODEX_SETTINGS,
    }).pipe(Effect.map((adapter) => [adapter])),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        layerReplayClientFactory,
        layerServerConfig,
        NodeServices.layer,
        NodeCrypto.layer,
        IdAllocator.layer,
      ),
    ),
  );

  return layerRegistry;
}

const decodeCodexAppServerReplayTranscript = Schema.decodeUnknownEffect(
  CodexReplay.CodexAppServerReplayTranscript,
);

export const CodexOrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  CodexReplay.CodexAppServerReplayTranscript,
  CodexOrchestratorReplayHarnessError
> = {
  driver: CodexAdapterV2.CODEX_DRIVER_KIND,
  decodeTranscript: (transcript) =>
    decodeCodexAppServerReplayTranscript(transcript).pipe(
      Effect.mapError(
        (cause) =>
          new CodexReplayTranscriptDecodeError({
            ...metadataFromTranscript(transcript),
            cause,
          }),
      ),
    ),
  makeProviderAdapterRegistryLayer: (
    transcript,
    options: { readonly replayGate?: ProviderReplayGate } = {},
  ) => {
    return Layer.effectContext(
      Effect.gen(function* () {
        const replayGate = options.replayGate;
        if (replayGate !== undefined) {
          yield* Effect.addFinalizer(() => Effect.sync(() => replayGate.releaseAll()));
        }
        const driver = yield* CodexReplay.makeReplayDriver(
          transcript,
          replayGate === undefined
            ? {}
            : {
                beforeEmitInbound: (entry) =>
                  Effect.promise((signal) => replayGate.beforeEmit(entry.label, signal)),
              },
        );
        return yield* Layer.build(layer({ transcript, driver }));
      }),
    );
  },
};

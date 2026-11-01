import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import { AcpTransportError } from "effect-acp/errors";
import { CodexAppServerTransportError } from "effect-codex-app-server/errors";
import type { CodexAppServerLineOffload } from "effect-codex-app-server/protocol";
import { AcpLineOffloads } from "@t3tools/provider-acp/server/AcpSessionRuntime";

import * as ServerConfig from "../../config.ts";
import { acpIoStage } from "./acpIoStage.ts";
import { codexIoStage } from "./codexIoStage.ts";
import { ProviderIoWorkerPool } from "./ProviderIoWorkerPool.ts";

// Shorter lines cost main less in-thread than a worker round trip (P1-025 measurements).
const DEFAULT_CODEX_OFFLOAD_MIN_CHARS = 2048;
const DEFAULT_ACP_OFFLOAD_MIN_CHARS = 16384;

export interface CodexLineOffloadInput {
  /** Stops the app-server when its worker dies. */
  readonly kill: Effect.Effect<void>;
  /** Set when incoming protocol events go to the native log. */
  readonly log?: {
    readonly providerSessionId: string;
    readonly writeSerialized: (payload: string) => Effect.Effect<void>;
  };
}

/** Parses, logs and decodes long Codex app-server lines on a provider I/O worker. */
export class CodexLineOffloads extends Context.Service<
  CodexLineOffloads,
  {
    /** None when the workers are off or unavailable: the client reads in-thread. */
    readonly open: (
      input: CodexLineOffloadInput,
    ) => Effect.Effect<Option.Option<CodexAppServerLineOffload>, never, Scope.Scope>;
  }
>()("t3/provider/io/ProviderIoOffloads/CodexLineOffloads") {}

export interface LineOffloadThresholds {
  readonly codex: number;
  readonly acp: number;
}

export const make = (pool: ProviderIoWorkerPool["Service"], minChars: LineOffloadThresholds) => ({
  codex: CodexLineOffloads.of({
    open: Effect.fn("CodexLineOffloads.open")(function* (input) {
      const session = yield* pool.open({
        stage: codexIoStage,
        options:
          input.log === undefined ? {} : { logProviderSessionId: input.log.providerSessionId },
        onFailure: (cause) =>
          new CodexAppServerTransportError({ operation: "read-input-stream", cause }),
        kill: input.kill,
      });
      return Option.map(session, (opened): CodexAppServerLineOffload => ({
        minChars: minChars.codex,
        decode: opened.call,
        writeLog: input.log?.writeSerialized ?? (() => Effect.void),
      }));
    }),
  }),
  acp: AcpLineOffloads.of({
    open: Effect.fn("AcpLineOffloads.open")(function* (input) {
      const session = yield* pool.open({
        stage: acpIoStage,
        options: undefined,
        onFailure: (cause) => new AcpTransportError({ operation: "read-input-stream", cause }),
        kill: input.kill,
      });
      return Option.map(session, (opened) => ({ minChars: minChars.acp, decode: opened.call }));
    }),
  }),
});

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const pool = yield* ProviderIoWorkerPool;
    const config = yield* ServerConfig.ServerConfig;
    const { codex, acp } = make(pool, {
      codex: config.providerIoCodexOffloadMinChars ?? DEFAULT_CODEX_OFFLOAD_MIN_CHARS,
      acp: config.providerIoAcpOffloadMinChars ?? DEFAULT_ACP_OFFLOAD_MIN_CHARS,
    });
    return Layer.mergeAll(
      Layer.succeed(CodexLineOffloads, codex),
      Layer.succeed(AcpLineOffloads, acp),
    );
  }),
);

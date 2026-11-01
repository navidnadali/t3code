import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { AcpTransportError } from "effect-acp/errors";
import { CodexAppServerTransportError } from "effect-codex-app-server/errors";
import type { AcpLineOffload } from "effect-acp/protocol";
import type { CodexAppServerLineOffload } from "effect-codex-app-server/protocol";
import { AcpLineOffloads } from "@t3tools/provider-acp/server/AcpSessionRuntime";

import * as ServerConfig from "../../config.ts";
import { acpIoStage } from "./acpIoStage.ts";
import { codexIoStage } from "./codexIoStage.ts";
import type { ProviderIoStage } from "./providerIoProtocol.ts";
import {
  type ProviderIoStageError,
  type ProviderIoWorkerError,
  ProviderIoWorkerPool,
} from "./ProviderIoWorkerPool.ts";

// Below this a worker round trip can cost main more than the line itself when few sessions are
// busy; above it the offload paid in every measurement (P1-025).
const DEFAULT_OFFLOAD_MIN_CHARS = 64 * 1024;

export interface CodexLineOffloadInput {
  /** Stops the app-server when its worker dies. */
  readonly kill: Effect.Effect<void>;
  /** Set when incoming protocol events go to the native log. */
  readonly log?: {
    readonly providerSessionId: string;
    readonly writeSerialized: (payload: string) => Effect.Effect<void>;
  };
}

/** Parses long Codex app-server lines and builds their log records on a provider I/O worker. */
export class CodexLineOffloads extends Context.Service<
  CodexLineOffloads,
  {
    /** None when the workers are off or unavailable: the client reads in-thread. */
    readonly open: (
      input: CodexLineOffloadInput,
    ) => Effect.Effect<Option.Option<CodexAppServerLineOffload>, never, Scope.Scope>;
  }
>()("t3/provider/io/ProviderIoOffloads/CodexLineOffloads") {}

// The line stages emit no outputs, so the output stream only fails: when the worker dies. The
// child is killed only once that failure is seen (or the watch has ended with the session), so
// the failure explains the input end the kill causes.
const openLineSession = Effect.fnUntraced(function* <Options, Reply, E>(
  pool: ProviderIoWorkerPool["Service"],
  input: {
    readonly stage: ProviderIoStage<Options, never, string, Reply>;
    readonly options: Options;
    readonly onFailure: (error: ProviderIoWorkerError | ProviderIoStageError) => E;
    readonly kill: Effect.Effect<void>;
  },
) {
  const failed = yield* Deferred.make<never, E>();
  const watched = yield* Deferred.make<void>();
  const session = yield* pool.open({
    ...input,
    kill: Deferred.await(watched).pipe(Effect.andThen(input.kill)),
  });
  if (Option.isNone(session)) return Option.none();
  // Also on close: the scope can close before the watch below has started.
  yield* Effect.addFinalizer(() => Deferred.succeed(watched, undefined));
  yield* session.value.output.pipe(
    Stream.runDrain,
    Effect.catch((error) => Deferred.fail(failed, error)),
    Effect.ensuring(Deferred.succeed(watched, undefined)),
    Effect.forkScoped,
  );
  return Option.some({ call: session.value.call, failure: Deferred.await(failed) });
});

export interface LineOffloadThresholds {
  readonly codex: number;
  readonly acp: number;
}

export const make = (pool: ProviderIoWorkerPool["Service"], minChars: LineOffloadThresholds) => ({
  codex: CodexLineOffloads.of({
    open: Effect.fn("CodexLineOffloads.open")(function* (input) {
      const session = yield* openLineSession(pool, {
        stage: codexIoStage,
        options:
          input.log === undefined ? {} : { logProviderSessionId: input.log.providerSessionId },
        onFailure: (cause) =>
          new CodexAppServerTransportError({ operation: "read-input-stream", cause }),
        kill: input.kill,
      });
      return Option.map(session, ({ call, failure }): CodexAppServerLineOffload => ({
        minChars: minChars.codex,
        decode: call,
        writeLog: input.log?.writeSerialized ?? (() => Effect.void),
        failure,
      }));
    }),
  }),
  acp: AcpLineOffloads.of({
    open: Effect.fn("AcpLineOffloads.open")(function* (input) {
      const session = yield* openLineSession(pool, {
        stage: acpIoStage,
        options: undefined,
        onFailure: (cause) => new AcpTransportError({ operation: "read-input-stream", cause }),
        kill: input.kill,
      });
      return Option.map(session, ({ call, failure }): AcpLineOffload => ({
        minChars: minChars.acp,
        decode: call,
        failure,
      }));
    }),
  }),
});

const openNothing = Effect.fnUntraced(function* () {
  return Option.none<never>();
});

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const pool = yield* ProviderIoWorkerPool;
    const config = yield* ServerConfig.ServerConfig;
    // Without workers, sessions open nothing at all.
    const { codex, acp } =
      (config.providerIoWorkers ?? 0) === 0
        ? {
            codex: CodexLineOffloads.of({ open: openNothing }),
            acp: AcpLineOffloads.of({ open: openNothing }),
          }
        : make(pool, {
            codex: config.providerIoCodexOffloadMinChars ?? DEFAULT_OFFLOAD_MIN_CHARS,
            acp: config.providerIoAcpOffloadMinChars ?? DEFAULT_OFFLOAD_MIN_CHARS,
          });
    return Layer.mergeAll(
      Layer.succeed(CodexLineOffloads, codex),
      Layer.succeed(AcpLineOffloads, acp),
    );
  }),
);

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import * as CodexError from "../errors.ts";
import type { CodexAppServerStageTiming } from "../protocol.ts";

export const JsonRpcId = Schema.Union([Schema.Number, Schema.String]);
export const isJsonRpcId = Schema.is(JsonRpcId);

export function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

const JsonRpcError = Schema.Struct({
  code: Schema.Number,
  message: Schema.String,
  data: Schema.optional(Schema.Unknown),
});

export const JsonRpcResponseEnvelope = Schema.Struct({
  id: JsonRpcId,
  result: Schema.optional(Schema.Unknown),
  error: Schema.optional(JsonRpcError),
});

export const decodeOptionalPayload = <A, I>(
  method: string,
  schema: Schema.Codec<A, I> | undefined,
  raw: unknown,
): Effect.Effect<A, CodexError.CodexAppServerRequestError> => {
  if (!schema) {
    if (raw === undefined) {
      return Effect.sync(() => undefined as A);
    }
    return Effect.fail(
      CodexError.CodexAppServerRequestError.unexpectedPayload(method, "decode-payload", raw),
    );
  }

  return Schema.decodeUnknownEffect(schema)(raw).pipe(
    Effect.mapError((error) =>
      CodexError.CodexAppServerRequestError.invalidPayload(method, "decode-payload", error),
    ),
  );
};

export const encodeOptionalPayload = <A, I>(
  method: string,
  schema: Schema.Codec<A, I> | undefined,
  payload: A,
): Effect.Effect<I | undefined, CodexError.CodexAppServerRequestError> => {
  if (!schema) {
    if (payload === undefined) {
      return Effect.undefined;
    }
    return Effect.fail(
      CodexError.CodexAppServerRequestError.unexpectedPayload(method, "encode-payload", payload),
    );
  }

  return Schema.encodeEffect(schema)(payload).pipe(
    Effect.mapError((error) =>
      CodexError.CodexAppServerRequestError.invalidPayload(method, "encode-payload", error),
    ),
  );
};

export const runHandler = Effect.fnUntraced(function* <A, B>(
  handler: ((payload: A) => Effect.Effect<B, CodexError.CodexAppServerError>) | undefined,
  payload: A,
  method: string,
) {
  if (!handler) {
    return yield* CodexError.CodexAppServerRequestError.methodNotFound(method);
  }

  return yield* handler(payload).pipe(
    Effect.mapError((error) =>
      CodexError.CodexAppServerRequestError.fromAppServerError(error, method),
    ),
  );
});

/**
 * Builds the effect where it is called, as when timing is off, and records building it (schema
 * decoders validate when called) plus running it: wall time, which is main-thread busy time while
 * the effect does not suspend.
 */
export const timeStage = <A, E, R>(
  timing: CodexAppServerStageTiming | undefined,
  stage: Parameters<CodexAppServerStageTiming["record"]>[0],
  bytes: number,
  makeEffect: () => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> => {
  if (timing === undefined) return makeEffect();
  const buildStartedAt = timing.now();
  const effect = makeEffect();
  const buildMs = timing.now() - buildStartedAt;
  return Effect.suspend(() => {
    const startedAt = timing.now();
    return Effect.onExit(effect, () =>
      Effect.sync(() => timing.record(stage, buildMs + timing.now() - startedAt, bytes)),
    );
  });
};

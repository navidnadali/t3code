import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FiberMap from "effect/FiberMap";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";

import * as CodexError from "./errors.ts";
import { JsonRpcId, JsonRpcResponseEnvelope, timeStage } from "./_internal/shared.ts";
import type * as CodexOffload from "./offload.ts";
import * as CodexWire from "./wire.ts";
const isJsonRpcId = Schema.is(JsonRpcId);
const isCodexAppServerError = Schema.is(CodexError.CodexAppServerError);
const MAX_BUFFERED_RAW_MESSAGES = 32;
const EXIT_STATUS_GRACE = "250 millis";

export interface CodexAppServerProtocolLogEvent {
  readonly direction: "incoming" | "outgoing";
  readonly stage: "raw" | "decoded" | "decode_failed";
  readonly payload: unknown;
}

/** Optional main-thread timing of incoming frames; unset costs nothing. */
export interface CodexAppServerStageTiming {
  readonly now: () => number;
  readonly record: (
    stage: "framing" | "json" | "decode" | "adapter",
    elapsedMs: number,
    bytes: number,
  ) => void;
}

export interface CodexAppServerIncomingNotification {
  readonly method: string;
  readonly params?: unknown;
}

export interface CodexAppServerIncomingRequest {
  readonly id: string | number;
  readonly method: string;
  readonly params?: unknown;
}

/** Parses long incoming lines and builds their log entries off the main thread. */
export interface CodexAppServerLineOffload {
  /** Lines at least this many characters long go off-thread; shorter ones cost less in-thread. */
  readonly minChars: number;
  readonly decode: (
    line: string,
  ) => Effect.Effect<CodexOffload.OffloadedLine, CodexError.CodexAppServerError>;
  /** Appends an incoming log entry built off-thread. */
  readonly writeLog: (payload: string) => Effect.Effect<void>;
  /** Fails when the offload stops working (its worker died); that error then ends the protocol. */
  readonly failure: Effect.Effect<never, CodexError.CodexAppServerError>;
}

export interface CodexAppServerPatchedProtocolOptions {
  readonly stdio: Stdio.Stdio;
  readonly lineOffload?: CodexAppServerLineOffload;
  readonly terminationError?: Effect.Effect<CodexError.CodexAppServerError>;
  readonly logIncoming?: boolean;
  readonly logOutgoing?: boolean;
  // Test override; production uses MAX_INCOMING_MESSAGE_BYTES.
  readonly maxIncomingMessageBytes?: number;
  readonly logger?: (event: CodexAppServerProtocolLogEvent) => Effect.Effect<void, never>;
  readonly stageTiming?: CodexAppServerStageTiming;
  readonly onNotification?: (
    notification: CodexAppServerIncomingNotification,
  ) => Effect.Effect<void, never>;
  readonly onRequest?: (
    request: CodexAppServerIncomingRequest,
  ) => Effect.Effect<unknown, CodexError.CodexAppServerError>;
  readonly onTermination?: (error: CodexError.CodexAppServerError) => Effect.Effect<void, never>;
}

export interface CodexAppServerPatchedProtocol {
  readonly incomingNotifications: Stream.Stream<CodexAppServerIncomingNotification>;
  readonly incomingRequests: Stream.Stream<CodexAppServerIncomingRequest>;
  readonly request: (
    method: string,
    payload?: unknown,
  ) => Effect.Effect<unknown, CodexError.CodexAppServerError>;
  readonly notify: (
    method: string,
    payload?: unknown,
  ) => Effect.Effect<void, CodexError.CodexAppServerError>;
  readonly respond: (
    requestId: string | number,
    result: unknown,
  ) => Effect.Effect<void, CodexError.CodexAppServerError>;
  readonly respondError: (
    requestId: string | number,
    error: CodexError.CodexAppServerRequestError,
  ) => Effect.Effect<void, CodexError.CodexAppServerError>;
}

interface CodexAppServerPendingRequest {
  readonly deferred: Deferred.Deferred<unknown, CodexError.CodexAppServerError>;
  readonly method: string;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

const encodeJsonString = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

const encodeWireMessage = (
  message: Record<string, unknown>,
): Effect.Effect<string, CodexError.CodexAppServerProtocolParseError> =>
  encodeJsonString(message).pipe(
    Effect.map((encoded) => `${encoded}\n`),
    Effect.mapError((cause) => {
      const method = typeof message.method === "string" ? message.method : undefined;
      const requestId =
        typeof message.id === "string" || typeof message.id === "number"
          ? String(message.id)
          : undefined;
      return CodexError.CodexAppServerProtocolParseError.fromSchemaError(
        "encode-wire-message",
        cause,
        {
          ...(method === undefined ? {} : { method }),
          ...(requestId === undefined ? {} : { requestId }),
        },
      );
    }),
  );

const decodeWireMessage = (
  line: string,
): Effect.Effect<unknown, CodexError.CodexAppServerProtocolParseError> =>
  Effect.fromResult(CodexWire.decodeWireLine(line));

const normalizeIncomingError = (
  error: unknown,
  operation: CodexError.CodexAppServerTransportOperation,
): CodexError.CodexAppServerError =>
  isCodexAppServerError(error)
    ? error
    : new CodexError.CodexAppServerTransportError({
        operation,
        cause: error,
      });

const toProtocolMessage = (
  requestId: string | number,
  fields: {
    readonly result?: unknown;
    readonly error?: CodexError.CodexAppServerProtocolErrorShape;
  },
): { readonly [key: string]: unknown } => ({
  id: requestId,
  ...(fields.result !== undefined ? { result: fields.result } : {}),
  ...(fields.error !== undefined ? { error: fields.error } : {}),
});

export const makeCodexAppServerPatchedProtocol = Effect.fn("makeCodexAppServerPatchedProtocol")(
  function* (
    options: CodexAppServerPatchedProtocolOptions,
  ): Effect.fn.Return<CodexAppServerPatchedProtocol, never, Scope.Scope> {
    const protocolScope = yield* Scope.Scope;
    const requestHandlerScope = yield* Scope.fork(protocolScope, "parallel");
    const outgoing = yield* Queue.unbounded<string, Cause.Done<void>>();
    const incomingNotifications =
      yield* Queue.sliding<CodexAppServerIncomingNotification>(MAX_BUFFERED_RAW_MESSAGES);
    const incomingRequests =
      yield* Queue.sliding<CodexAppServerIncomingRequest>(MAX_BUFFERED_RAW_MESSAGES);
    const pending = yield* Ref.make(new Map<string, CodexAppServerPendingRequest>());
    const nextRequestId = yield* Ref.make(1);
    const maxIncomingMessageBytes =
      options.maxIncomingMessageBytes ?? CodexWire.MAX_INCOMING_MESSAGE_BYTES;
    const framer = CodexWire.makeLineFramer(maxIncomingMessageBytes);
    const timing = options.stageTiming;
    const terminationHandled = yield* Ref.make(false);
    const terminationFailure = yield* Ref.make(Option.none<CodexError.CodexAppServerError>());
    const terminationSignal = yield* Deferred.make<void>();
    const activeRequestHandlers = yield* Ref.make(0);
    const requestHandlers = yield* FiberMap.make<string | number>().pipe(
      Scope.provide(requestHandlerScope),
    );

    const logProtocol = (event: CodexAppServerProtocolLogEvent) => {
      if (event.direction === "incoming" && !options.logIncoming) {
        return Effect.void;
      }
      if (event.direction === "outgoing" && !options.logOutgoing) {
        return Effect.void;
      }
      return (
        options.logger?.(event) ??
        Effect.logDebug("Codex App Server protocol event").pipe(Effect.annotateLogs({ event }))
      );
    };

    const failAllPending = (error: CodexError.CodexAppServerError) =>
      Ref.get(pending).pipe(
        Effect.flatMap((current) =>
          Effect.forEach([...current.values()], ({ deferred }) => Deferred.fail(deferred, error), {
            discard: true,
          }),
        ),
        Effect.andThen(Ref.set(pending, new Map())),
      );

    const offloadFailure = options.lineOffload?.failure;
    const handleTermination = (classify: () => Effect.Effect<CodexError.CodexAppServerError>) =>
      Ref.modify(terminationHandled, (handled) => {
        if (handled) {
          return [Effect.void, true] as const;
        }
        return [
          Effect.gen(function* () {
            // A dead offload kills the process, so its error explains the input end that follows.
            const error = yield* offloadFailure === undefined
              ? classify()
              : Effect.raceFirst(Effect.flip(offloadFailure), classify());
            yield* Ref.set(terminationFailure, Option.some(error));
            yield* failAllPending(error);
            yield* Queue.end(outgoing);
            yield* Deferred.succeed(terminationSignal, undefined);
            yield* Scope.close(requestHandlerScope, Exit.void).pipe(
              Effect.forkIn(protocolScope, { startImmediately: true }),
              Effect.asVoid,
            );
            if (options.onTermination) {
              yield* options.onTermination(error);
            }
          }),
          true,
        ] as const;
      }).pipe(Effect.flatten);

    if (offloadFailure !== undefined) {
      yield* offloadFailure.pipe(
        Effect.catch((error) => handleTermination(() => Effect.succeed(error))),
        Effect.forkScoped,
      );
    }

    const offerOutgoing =
      /* Every protocol write reaches this helper; request spans cover the operation. */
      Effect.fnUntraced(function* (message: Record<string, unknown>) {
        const failure = yield* Ref.get(terminationFailure);
        if (Option.isSome(failure)) return yield* failure.value;

        yield* logProtocol({
          direction: "outgoing",
          stage: "decoded",
          payload: message,
        });
        const encoded = yield* encodeWireMessage(message);
        yield* logProtocol({
          direction: "outgoing",
          stage: "raw",
          payload: encoded,
        });
        const accepted = yield* Queue.offer(outgoing, encoded);
        if (!accepted) {
          const closed = yield* Ref.get(terminationFailure);
          return yield* Option.getOrElse(
            closed,
            () => new CodexError.CodexAppServerInputStreamEndedError({}),
          );
        }
      });

    const removePending = (requestId: string) =>
      Ref.update(pending, (current) => {
        if (!current.has(requestId)) {
          return current;
        }
        const next = new Map(current);
        next.delete(requestId);
        return next;
      });

    const resolvePending = (
      requestId: string,
      handler: (pendingRequest: CodexAppServerPendingRequest) => Effect.Effect<void>,
    ) =>
      Ref.modify(pending, (current) => {
        const pendingRequest = current.get(requestId);
        if (!pendingRequest) {
          return [Effect.void, current] as const;
        }
        const next = new Map(current);
        next.delete(requestId);
        return [handler(pendingRequest), next] as const;
      }).pipe(Effect.flatten);

    const respond = (requestId: string | number, result: unknown) =>
      offerOutgoing(toProtocolMessage(requestId, { result }));

    const respondError = (
      requestId: string | number,
      error: CodexError.CodexAppServerRequestError,
    ) => offerOutgoing(toProtocolMessage(requestId, { error: error.toProtocolError() }));

    const handleResponse = (response: typeof JsonRpcResponseEnvelope.Type) => {
      const requestId = String(response.id);
      const protocolError = response.error;
      if (protocolError !== undefined) {
        return resolvePending(requestId, ({ deferred, method }) =>
          Deferred.fail(
            deferred,
            CodexError.CodexAppServerRequestError.fromProtocolError(
              protocolError,
              method,
              requestId,
            ),
          ),
        );
      }
      return resolvePending(requestId, ({ deferred }) =>
        Deferred.succeed(deferred, response.result),
      );
    };

    const handleRequest = (request: CodexAppServerIncomingRequest) =>
      Queue.offer(incomingRequests, request).pipe(
        Effect.flatMap(() => {
          const handler = options.onRequest;
          if (!handler) return Effect.void;

          return Ref.modify(activeRequestHandlers, (count) =>
            count >= MAX_BUFFERED_RAW_MESSAGES ? [false, count] : [true, count + 1],
          ).pipe(
            Effect.flatMap((accepted) => {
              if (!accepted) {
                return respondError(
                  request.id,
                  CodexError.CodexAppServerRequestError.overloaded(
                    "Too many Codex requests are already active.",
                  ),
                );
              }

              return handler(request).pipe(
                Effect.matchEffect({
                  onFailure: (error) =>
                    respondError(
                      request.id,
                      CodexError.CodexAppServerRequestError.fromAppServerError(
                        error,
                        request.method,
                      ),
                    ),
                  onSuccess: (result) => respond(request.id, result),
                }),
                Effect.ensuring(
                  Ref.update(activeRequestHandlers, (count) => Math.max(0, count - 1)),
                ),
                Effect.catch((error) =>
                  handleTermination(() => Effect.succeed(error)).pipe(
                    Effect.forkIn(protocolScope),
                    Effect.asVoid,
                  ),
                ),
                FiberMap.run(requestHandlers, request.id),
                Effect.asVoid,
              );
            }),
          );
        }),
        Effect.asVoid,
      );

    // Codex settles requests itself when their turn ends; a handler still waiting would hold its slot.
    const releaseResolvedRequest = (notification: CodexAppServerIncomingNotification) =>
      notification.method === "serverRequest/resolved" &&
      isObject(notification.params) &&
      isJsonRpcId(notification.params.requestId)
        ? FiberMap.remove(requestHandlers, notification.params.requestId)
        : Effect.void;

    const handleNotification = (notification: CodexAppServerIncomingNotification) =>
      releaseResolvedRequest(notification).pipe(
        Effect.andThen(Queue.offer(incomingNotifications, notification)),
        Effect.andThen(options.onNotification ? options.onNotification(notification) : Effect.void),
        Effect.asVoid,
      );

    const routeMessage = Effect.fnUntraced(function* (message: unknown) {
      if (Option.isSome(yield* Ref.get(terminationFailure))) return;
      if (CodexWire.isIncomingRequest(message)) return yield* handleRequest(message);
      if (CodexWire.isIncomingNotification(message)) return yield* handleNotification(message);
      if (CodexWire.isIncomingResponse(message)) return yield* handleResponse(message);
      return yield* CodexError.CodexAppServerProtocolParseError.fromUnroutableMessage(message);
    });

    const lineOffload = options.lineOffload;
    const handleLine = (line: string): Effect.Effect<void, CodexError.CodexAppServerError> => {
      if (line.trim().length === 0) {
        return Effect.void;
      }
      if (lineOffload !== undefined && line.length >= lineOffload.minChars) {
        return lineOffload.decode(line).pipe(
          Effect.flatMap((offloaded) =>
            offloaded._tag === "InThread"
              ? handleLineInThread(line)
              : Effect.forEach(offloaded.logs ?? [], lineOffload.writeLog, {
                  discard: true,
                }).pipe(Effect.andThen(routeMessage(offloaded.message))),
          ),
        );
      }
      return handleLineInThread(line);
    };

    const handleLineInThread = (
      line: string,
    ): Effect.Effect<void, CodexError.CodexAppServerError> =>
      logProtocol({
        direction: "incoming",
        stage: "raw",
        payload: line,
      }).pipe(
        Effect.flatMap(() =>
          timing === undefined
            ? decodeWireMessage(line)
            : timeStage(timing, "json", Buffer.byteLength(line, "utf8"), () =>
                decodeWireMessage(line),
              ),
        ),
        Effect.tap((decoded) =>
          logProtocol({
            direction: "incoming",
            stage: "decoded",
            payload: decoded,
          }),
        ),
        Effect.tapErrorTag("CodexAppServerProtocolParseError", (error) =>
          logProtocol({
            direction: "incoming",
            stage: "decode_failed",
            payload: CodexWire.decodeFailedLogPayload(error),
          }),
        ),
        Effect.flatMap(routeMessage),
      );

    yield* options.stdio.stdin.pipe(
      Stream.interruptWhen(Deferred.await(terminationSignal)),
      Stream.runForEach(
        Effect.fnUntraced(function* (chunk) {
          const startedAt = timing?.now() ?? 0;
          const framed = framer.push(chunk);
          const framingMs = timing === undefined ? 0 : timing.now() - startedAt;
          for (const line of framed.lines) {
            yield* handleLine(line);
          }
          if (framed.exceeded) {
            return yield* new CodexError.CodexAppServerTransportError({
              operation: "read-input-stream",
              cause: new Error(`Incoming message exceeded ${maxIncomingMessageBytes} bytes.`),
            });
          }
          timing?.record("framing", framingMs, chunk.byteLength);
        }),
      ),
      Effect.matchEffect({
        onFailure: (error) =>
          handleTermination(() =>
            Effect.succeed(normalizeIncomingError(error, "read-input-stream")),
          ),
        onSuccess: () =>
          Effect.sync(() => framer.end()).pipe(
            Effect.flatMap(handleLine),
            Effect.matchEffect({
              onFailure: (error) => handleTermination(() => Effect.succeed(error)),
              onSuccess: () =>
                handleTermination(() => {
                  const ended = new CodexError.CodexAppServerInputStreamEndedError({});
                  // Node ends stdout before it reports the exit of a crashed process, so
                  // wait briefly for the exit status. stdout can also close while the
                  // process keeps running, so the wait must not hang pending requests.
                  return options.terminationError === undefined
                    ? Effect.succeed(ended)
                    : options.terminationError.pipe(
                        Effect.timeoutOrElse({
                          duration: EXIT_STATUS_GRACE,
                          orElse: () => Effect.succeed(ended),
                        }),
                      );
                }),
            }),
          ),
      }),
      Effect.forkScoped,
    );

    yield* Stream.fromQueue(outgoing).pipe(Stream.run(options.stdio.stdout()), Effect.forkScoped);

    const request = Effect.fn("CodexAppServerProtocol.request")(function* (
      method: string,
      payload?: unknown,
    ) {
      const requestId = yield* Ref.modify(
        nextRequestId,
        (current) => [current, current + 1] as const,
      );
      const deferred = yield* Deferred.make<unknown, CodexError.CodexAppServerError>();
      yield* Ref.update(pending, (current) =>
        new Map(current).set(String(requestId), { deferred, method }),
      );
      yield* offerOutgoing({
        id: requestId,
        method,
        ...(payload !== undefined ? { params: payload } : {}),
      }).pipe(Effect.tapError(() => removePending(String(requestId))));
      return yield* Deferred.await(deferred).pipe(
        Effect.onInterrupt(() => removePending(String(requestId))),
      );
    });

    const notify = (method: string, payload?: unknown) =>
      offerOutgoing({
        method,
        ...(payload !== undefined ? { params: payload } : {}),
      });

    return {
      incomingNotifications: Stream.fromQueue(incomingNotifications),
      incomingRequests: Stream.fromQueue(incomingRequests),
      request,
      notify,
      respond,
      respondError,
    } satisfies CodexAppServerPatchedProtocol;
  },
);

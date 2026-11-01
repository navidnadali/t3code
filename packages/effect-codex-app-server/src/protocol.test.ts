import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";

import * as CodexError from "./errors.ts";
import * as CodexProtocol from "./protocol.ts";
import * as CodexOffload from "./offload.ts";
import { makeInMemoryStdio } from "./_internal/stdio.ts";
const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const encoder = new TextEncoder();

const encodeJsonl = (value: unknown) => encoder.encode(`${encodeUnknownJsonString(value)}\n`);

const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));

it.layer(NodeServices.layer)("effect-codex-app-server protocol", (it) => {
  it.effect(
    "encodes requests without a jsonrpc field and routes inbound requests and notifications",
    () =>
      Effect.gen(function* () {
        const { stdio, input, output } = yield* makeInMemoryStdio();
        const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({ stdio });

        const notificationDeferred =
          yield* Deferred.make<ReadonlyArray<CodexProtocol.CodexAppServerIncomingNotification>>();
        const requestDeferred =
          yield* Deferred.make<ReadonlyArray<CodexProtocol.CodexAppServerIncomingRequest>>();

        yield* transport.incomingNotifications.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.flatMap((notifications) => Deferred.succeed(notificationDeferred, notifications)),
          Effect.forkScoped,
        );

        yield* transport.incomingRequests.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.flatMap((requests) => Deferred.succeed(requestDeferred, requests)),
          Effect.forkScoped,
        );

        yield* transport.notify("initialized");
        assert.equal(yield* Queue.take(output), '{"method":"initialized"}\n');

        const initializeParams = {
          clientInfo: {
            name: "effect-codex-app-server-test",
            title: "Effect Codex App Server Test",
            version: "0.0.0",
          },
          capabilities: {
            experimentalApi: true,
            optOutNotificationMethods: null,
          },
        };

        const pendingInitialize = yield* transport
          .request("initialize", initializeParams)
          .pipe(Effect.forkScoped);
        assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
          id: 1,
          method: "initialize",
          params: initializeParams,
        });

        yield* Queue.offer(
          input,
          encodeJsonl({
            method: "item/agentMessage/delta",
            params: {
              delta: "Hello from the mock peer.",
              itemId: "item-1",
              threadId: "thread-1",
              turnId: "turn-1",
            },
          }),
        );
        yield* Queue.offer(
          input,
          encodeJsonl({
            id: 77,
            method: "item/tool/requestUserInput",
            params: {
              itemId: "item-approval-1",
              threadId: "thread-1",
              turnId: "turn-1",
              questions: [
                {
                  id: "approved",
                  header: "Approve",
                  question: "Continue?",
                },
              ],
            },
          }),
        );
        yield* Queue.offer(
          input,
          encodeJsonl({
            id: 1,
            result: {
              userAgent: "mock-codex-app-server",
              codexHome: "/tmp/codex-home",
              platformFamily: "unix",
              platformOs: "macos",
            },
          }),
        );

        assert.deepEqual(yield* Fiber.join(pendingInitialize), {
          userAgent: "mock-codex-app-server",
          codexHome: "/tmp/codex-home",
          platformFamily: "unix",
          platformOs: "macos",
        });
        assert.deepEqual(yield* Deferred.await(notificationDeferred), [
          {
            method: "item/agentMessage/delta",
            params: {
              delta: "Hello from the mock peer.",
              itemId: "item-1",
              threadId: "thread-1",
              turnId: "turn-1",
            },
          },
        ]);
        assert.deepEqual(yield* Deferred.await(requestDeferred), [
          {
            id: 77,
            method: "item/tool/requestUserInput",
            params: {
              itemId: "item-approval-1",
              threadId: "thread-1",
              turnId: "turn-1",
              questions: [
                {
                  id: "approved",
                  header: "Approve",
                  question: "Continue?",
                },
              ],
            },
          },
        ]);

        yield* transport.respond(77, {
          answers: {
            approved: {
              answers: ["yes"],
            },
          },
        });
        assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
          id: 77,
          result: {
            answers: {
              approved: {
                answers: ["yes"],
              },
            },
          },
        });

        yield* transport.respondError(
          78,
          CodexError.CodexAppServerRequestError.methodNotFound("x/test"),
        );
        assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
          id: 78,
          error: {
            code: -32601,
            message: "Method not found: x/test",
          },
        });
      }),
  );

  it.effect.each(
    [1, 7, 1024].flatMap((chunkSize) => [false, true].map((timed) => ({ chunkSize, timed }))),
  )(
    "preserves JSONL framing and UTF-8 across $chunkSize-byte input chunks (stage timing $timed)",
    ({ chunkSize, timed }) =>
      Effect.gen(function* () {
        const { stdio, input } = yield* makeInMemoryStdio();
        const notifications: Array<CodexProtocol.CodexAppServerIncomingNotification> = [];
        const rawLines: Array<unknown> = [];
        // Text decoding and JSON parsing each advance the clock, so only their windows see time.
        let clock = 0;
        const parse = JSON.parse;
        using _parse = vi.spyOn(JSON, "parse").mockImplementation((text, reviver) => {
          clock += 5;
          return parse(text, reviver);
        });
        const decode = TextDecoder.prototype.decode;
        using _decode = vi.spyOn(TextDecoder.prototype, "decode").mockImplementation(function (
          this: TextDecoder,
          input,
          options,
        ) {
          clock += 7;
          return decode.call(this, input, options);
        });
        const framingMs: Array<number> = [];
        let framingBytes = 0;
        const jsonFrames: Array<{ readonly elapsedMs: number; readonly bytes: number }> = [];
        const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
        const stageTiming: CodexProtocol.CodexAppServerStageTiming = {
          now: () => clock,
          record: (stage, elapsedMs, bytes) => {
            if (stage === "json") jsonFrames.push({ elapsedMs, bytes });
            if (stage !== "framing") return;
            framingMs.push(elapsedMs);
            framingBytes += bytes;
          },
        };
        yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
          stdio,
          ...(timed ? { stageTiming } : {}),
          logIncoming: true,
          logger: (event) =>
            Effect.sync(() => {
              if (event.stage === "raw") {
                rawLines.push(event.payload);
              }
            }),
          onNotification: (notification) =>
            Effect.sync(() => {
              notifications.push(notification);
            }),
          onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
        });

        const firstLine = '{"method":"x/first",\r"params":{"text":"hé🙂"}}';
        const secondLine = '{"method":"x/second","params":{"value":2}}';
        const finalLine = '{"method":"x/final","params":{"text":"最後"}}\r';
        const bytes = encoder.encode(`\n \t\r\n${firstLine}\r\n\n${secondLine}\n${finalLine}`);
        for (let offset = 0; offset < bytes.length; offset += chunkSize) {
          yield* Queue.offer(input, bytes.subarray(offset, offset + chunkSize));
        }
        yield* Queue.end(input);

        assert.instanceOf(
          yield* Deferred.await(termination),
          CodexError.CodexAppServerInputStreamEndedError,
        );
        assert.deepEqual(notifications, [
          { method: "x/first", params: { text: "hé🙂" } },
          { method: "x/second", params: { value: 2 } },
          { method: "x/final", params: { text: "最後" } },
        ]);
        assert.deepEqual(rawLines, [firstLine, secondLine, finalLine]);
        assert.deepEqual(
          jsonFrames,
          timed
            ? [firstLine, secondLine, finalLine].map((line) => ({
                elapsedMs: 5,
                bytes: encoder.encode(line).byteLength,
              }))
            : [],
        );
        assert.deepEqual(
          framingMs,
          timed ? Array.from({ length: Math.ceil(bytes.byteLength / chunkSize) }, () => 7) : [],
        );
        assert.equal(framingBytes, timed ? bytes.byteLength : 0);
      }),
  );

  it.effect.each(["newline-terminated", "unfinished"])(
    "rejects an oversized %s message using UTF-8 bytes before logging or parsing",
    (framing) =>
      Effect.gen(function* () {
        const { stdio, input, output } = yield* makeInMemoryStdio();
        const notifications: Array<CodexProtocol.CodexAppServerIncomingNotification> = [];
        const logged: Array<unknown> = [];
        const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
        let reachedEnd = false;
        const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
          stdio: Stdio.make({
            ...stdio,
            stdin: stdio.stdin.pipe(
              Stream.concat(
                Stream.suspend(() => {
                  reachedEnd = true;
                  return Stream.empty;
                }),
              ),
            ),
          }),
          maxIncomingMessageBytes: 16,
          logIncoming: true,
          logger: (event) =>
            Effect.sync(() => {
              logged.push(event.payload);
            }),
          onNotification: (notification) =>
            Effect.sync(() => {
              notifications.push(notification);
            }),
          onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
        });
        const pending = yield* Effect.forEach(["thread/read", "thread/list"], (method) =>
          transport.request(method, {}).pipe(Effect.forkScoped),
        );
        yield* Effect.forEach(pending, () => Queue.take(output));

        // Two 16-byte lines must survive; the 17-byte line is only 15 UTF-16 code units.
        const firstLine = '{"method":"xé"}';
        const secondLine = '{"method":"x/a"}';
        const bytes = encoder.encode(
          `${firstLine}\n${secondLine}\n{"method":"éé"}${framing === "unfinished" ? "" : "\n"}`,
        );
        // Split a UTF-8 sequence on both sides of the byte limit.
        const firstSplit = bytes.indexOf(0xc3) + 1;
        yield* Queue.offer(input, bytes.subarray(0, firstSplit));
        if (framing === "unfinished") {
          const lastSplit = bytes.lastIndexOf(0xc3) + 1;
          yield* Queue.offer(input, bytes.subarray(firstSplit, lastSplit));
          yield* Queue.offer(input, bytes.subarray(lastSplit));
        } else {
          yield* Queue.offer(input, bytes.subarray(firstSplit));
        }
        yield* Queue.end(input);

        const error = yield* Deferred.await(termination);
        assert.instanceOf(error, CodexError.CodexAppServerTransportError);
        assert.equal(error.operation, "read-input-stream");
        assert.isFalse(reachedEnd, "Must reject the unfinished line before reading EOF");
        for (const response of pending) {
          const failure = yield* Fiber.join(response).pipe(
            Effect.match({
              onFailure: (error) => error,
              onSuccess: () => assert.fail("Expected the oversized message to fail the request"),
            }),
          );
          assert.strictEqual(failure, error);
        }
        assert.deepEqual(notifications, [{ method: "xé" }, { method: "x/a" }]);
        assert.deepEqual(logged, [firstLine, { method: "xé" }, secondLine, { method: "x/a" }]);
      }),
  );

  it.effect("reports a malformed fragmented final line before input stream termination", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });
      const response = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);

      yield* Queue.offer(input, encoder.encode('{"id":1,'));
      yield* Queue.offer(input, encoder.encode('"result":'));
      yield* Queue.end(input);

      const error = yield* Deferred.await(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProtocolParseError);
      assert.equal(error.operation, "decode-wire-message");
      const responseError = yield* Fiber.join(response).pipe(
        Effect.match({
          onFailure: (failure) => failure,
          onSuccess: () => assert.fail("Expected the malformed response to fail the request"),
        }),
      );
      assert.strictEqual(responseError, error);
    }),
  );

  it.effect("keeps processing protocol messages while an approval is pending", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const approvalStarted = yield* Deferred.make<void>();
      const approvalDecision = yield* Deferred.make<{ readonly decision: string }>();
      const notificationReceived = yield* Deferred.make<void>();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Deferred.succeed(approvalStarted, undefined).pipe(
            Effect.andThen(Deferred.await(approvalDecision)),
          ),
        onNotification: () => Deferred.succeed(notificationReceived, undefined).pipe(Effect.asVoid),
      });

      const pendingRequest = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);
      yield* Queue.offer(
        input,
        encoder.encode(
          `${[
            encodeUnknownJsonString({ id: 7, method: "item/tool/requestUserInput", params: {} }),
            encodeUnknownJsonString({ method: "item/agentMessage/delta", params: { delta: "ok" } }),
            encodeUnknownJsonString({ id: 1, result: { threadId: "thread-1" } }),
          ].join("\n")}\n`,
        ),
      );

      yield* Deferred.await(approvalStarted);
      yield* Deferred.await(notificationReceived);
      assert.deepEqual(yield* Fiber.join(pendingRequest), { threadId: "thread-1" });

      yield* Deferred.succeed(approvalDecision, { decision: "accept" });
      assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
        id: 7,
        result: { decision: "accept" },
      });
    }),
  );

  it.effect("rejects incoming requests after the active handler limit is reached", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const handlersStarted = yield* Deferred.make<void>();
      const releaseHandlers = yield* Deferred.make<void>();
      let activeHandlers = 0;
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Effect.sync(() => ++activeHandlers).pipe(
            Effect.flatMap((count) =>
              count === 32
                ? Deferred.succeed(handlersStarted, undefined).pipe(Effect.asVoid)
                : Effect.void,
            ),
            Effect.andThen(Deferred.await(releaseHandlers)),
            Effect.as({ decision: "accept" }),
          ),
      });

      const requests = Array.from({ length: 33 }, (_, index) =>
        encodeUnknownJsonString({
          id: index + 1,
          method: "item/tool/requestUserInput",
          params: {},
        }),
      );
      yield* Queue.offer(input, encoder.encode(`${requests.join("\n")}\n`));
      yield* Deferred.await(handlersStarted);

      assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
        id: 33,
        error: {
          code: -32001,
          message: "Too many Codex requests are already active.",
        },
      });
      assert.equal(activeHandlers, 32);

      yield* Deferred.succeed(releaseHandlers, undefined);
      yield* Effect.forEach(Array.from({ length: 32 }), () => Queue.take(output), {
        discard: true,
      });
    }),
  );

  it.effect("releases a waiting request handler when Codex resolves its request", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: (request) =>
          request.id === 33 ? Effect.succeed({ decision: "accept" }) : Effect.never,
      });

      const messages = Array.from({ length: 32 }, (_, index) =>
        encodeUnknownJsonString({
          id: index + 1,
          method: "item/commandExecution/requestApproval",
          params: {},
        }),
      );
      for (let requestId = 1; requestId <= 32; requestId++) {
        messages.push(
          encodeUnknownJsonString({
            method: "serverRequest/resolved",
            params: { threadId: "thread-1", requestId },
          }),
        );
      }
      messages.push(
        encodeUnknownJsonString({
          id: 33,
          method: "item/commandExecution/requestApproval",
          params: {},
        }),
      );
      yield* Queue.offer(input, encoder.encode(`${messages.join("\n")}\n`));

      assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
        id: 33,
        result: { decision: "accept" },
      });
    }),
  );

  it.effect("keeps numeric and string request ids apart when Codex resolves one", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const interrupted: Array<string | number> = [];
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: (request) =>
          request.id === 2
            ? Effect.sync(() => ({ interrupted }))
            : Effect.never.pipe(
                Effect.onInterrupt(() => Effect.sync(() => interrupted.push(request.id))),
              ),
      });

      const messages = [
        { id: 1, method: "item/commandExecution/requestApproval", params: {} },
        { id: "1", method: "item/commandExecution/requestApproval", params: {} },
        { method: "serverRequest/resolved", params: { threadId: "thread-1", requestId: "1" } },
        { id: 2, method: "item/commandExecution/requestApproval", params: {} },
      ].map((message) => encodeUnknownJsonString(message));
      yield* Queue.offer(input, encoder.encode(`${messages.join("\n")}\n`));

      assert.deepEqual(yield* decodeJson(yield* Queue.take(output)), {
        id: 2,
        result: { interrupted: ["1"] },
      });
    }),
  );

  it.effect("interrupts pending request handlers when the protocol terminates", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const approvalStarted = yield* Deferred.make<void>();
      const approvalInterrupted = yield* Deferred.make<void>();
      const terminated = yield* Deferred.make<void>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Deferred.succeed(approvalStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() =>
              Deferred.succeed(approvalInterrupted, undefined).pipe(Effect.asVoid),
            ),
          ),
        onTermination: () => Deferred.succeed(terminated, undefined).pipe(Effect.asVoid),
      });

      yield* Queue.offer(
        input,
        encodeJsonl({ id: 7, method: "item/tool/requestUserInput", params: {} }),
      );
      yield* Deferred.await(approvalStarted);
      yield* Queue.end(input);

      yield* Deferred.await(approvalInterrupted);
      yield* Deferred.await(terminated);
    }),
  );

  it.effect("rejects outgoing messages after an approval response cannot be encoded", () =>
    Effect.gen(function* () {
      const { stdio: baseStdio, input } = yield* makeInMemoryStdio();
      const terminated = yield* Deferred.make<CodexError.CodexAppServerError>();
      const readerStopped = yield* Deferred.make<void>();
      let notificationCount = 0;
      let requestCount = 0;
      const stdio = Stdio.make({
        args: baseStdio.args,
        stdin: baseStdio.stdin.pipe(
          Stream.ensuring(Deferred.succeed(readerStopped, undefined).pipe(Effect.asVoid)),
        ),
        stdout: baseStdio.stdout,
        stderr: baseStdio.stderr,
      });
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Effect.sync(() => ++requestCount).pipe(
            Effect.map((count) => (count === 1 ? { invalid: 1n } : { ok: true })),
          ),
        onNotification: () => Effect.sync(() => notificationCount++).pipe(Effect.asVoid),
        onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(
        input,
        encodeJsonl({ id: 7, method: "item/tool/requestUserInput", params: {} }),
      );

      const failure = yield* Deferred.await(terminated);
      assert.instanceOf(failure, CodexError.CodexAppServerProtocolParseError);
      const requestFailure = yield* transport.request("thread/read", {}).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected a terminated protocol request to fail"),
        }),
      );
      const notificationFailure = yield* transport.notify("initialized").pipe(Effect.flip);
      assert.strictEqual(requestFailure, failure);
      assert.strictEqual(notificationFailure, failure);
      yield* Deferred.await(readerStopped);

      yield* Queue.offer(
        input,
        encoder.encode(
          `${[
            encodeUnknownJsonString({ method: "x/late-notification" }),
            encodeUnknownJsonString({ id: 8, method: "x/late-request" }),
          ].join("\n")}\n`,
        ),
      );

      assert.equal(notificationCount, 0);
      assert.equal(requestCount, 1);
      assert.equal(yield* Queue.size(input), 1);
    }),
  );

  it.effect("fails pending requests before interrupted handler cleanup completes", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const handlerStarted = yield* Deferred.make<void>();
      const finalizerStarted = yield* Deferred.make<void>();
      const releaseFinalizer = yield* Deferred.make<void>();
      const terminated = yield* Deferred.make<CodexError.CodexAppServerError>();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onRequest: () =>
          Deferred.succeed(handlerStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() =>
              Deferred.succeed(finalizerStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFinalizer)),
              ),
            ),
          ),
        onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
      });
      const pending = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);
      yield* Queue.offer(input, encodeJsonl({ id: 7, method: "x/approval" }));
      yield* Deferred.await(handlerStarted);
      yield* Queue.end(input);

      const failure = yield* Deferred.await(terminated);
      yield* Deferred.await(finalizerStarted);
      const pendingFailure = yield* Fiber.join(pending).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected the pending request to fail"),
        }),
      );
      assert.strictEqual(pendingFailure, failure);

      yield* Deferred.succeed(releaseFinalizer, undefined);
    }),
  );

  it.effect("correlates response errors with the originating request", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({ stdio });

      const response = yield* transport.request("thread/start", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);
      yield* Queue.offer(
        input,
        encodeJsonl({
          id: 1,
          error: {
            code: -32602,
            message: "Invalid params",
            data: { field: "cwd" },
          },
        }),
      );

      const error = yield* Fiber.join(response).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected Codex App Server request to fail"),
        }),
      );
      assert.instanceOf(error, CodexError.CodexAppServerRequestError);
      assert.deepInclude(error, {
        code: -32602,
        errorMessage: "Invalid params",
        method: "thread/start",
        requestId: "1",
        operation: "receive-response",
      });
    }),
  );

  it.effect("logs decode failures without copying the cause or wire payload", () =>
    Effect.gen(function* () {
      const secret = "codex-wire-secret-sentinel";
      const { stdio, input } = yield* makeInMemoryStdio();
      const events: Array<CodexProtocol.CodexAppServerProtocolLogEvent> = [];
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        logIncoming: true,
        logger: (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(input, encoder.encode(`{"secret":"${secret}"\n`));
      yield* Deferred.await(termination);

      const event = events.find(({ stage }) => stage === "decode_failed");
      assert.exists(event);
      assert.equal(event.direction, "incoming");
      const payload = event.payload as Record<string, unknown>;
      assert.equal(payload.operation, "decode-wire-message");
      assert.isNumber(payload.issueCount);
      assert.isArray(payload.issueKinds);
      assert.isNumber(payload.maximumPathDepth);
      assert.equal("cause" in payload, false);
      assert.equal("detail" in payload, false);
      assert.notInclude(encodeUnknownJsonString(event), secret);
    }),
  );

  it.effect("fails malformed and unroutable lines with the schema decoder's errors", () =>
    Effect.forEach(
      [
        '{"id":1,"result":',
        "not json",
        '{"id":1}}',
        "{'id':1}",
        "[1,",
        "42",
        '"text"',
        "null",
        "[1]",
      ],
      (line) =>
        Effect.gen(function* () {
          const { stdio, input } = yield* makeInMemoryStdio();
          const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
          yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
            stdio,
            onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
          });
          yield* Queue.offer(input, encoder.encode(`${line}\n`));
          const error = yield* Deferred.await(termination);

          const expected = yield* decodeJson(line).pipe(
            Effect.match({
              onFailure: (cause) =>
                CodexError.CodexAppServerProtocolParseError.fromSchemaError(
                  "decode-wire-message",
                  cause,
                ),
              onSuccess: (message) =>
                CodexError.CodexAppServerProtocolParseError.fromUnroutableMessage(message),
            }),
          );
          assert.instanceOf(error, CodexError.CodexAppServerProtocolParseError);
          assert.equal(error.message, expected.message);
          assert.deepEqual(
            { ...error, cause: String(error.cause) },
            { ...expected, cause: String(expected.cause) },
          );
        }).pipe(Effect.scoped),
      { discard: true },
    ),
  );

  it.effect("describes unroutable messages with safe structural diagnostics", () =>
    Effect.gen(function* () {
      const secret = "codex-unroutable-secret-sentinel";
      const { stdio, input } = yield* makeInMemoryStdio();
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(
        input,
        encodeJsonl({ id: true, method: "thread/start", params: { token: secret } }),
      );

      const error = yield* Deferred.await(termination);
      assert.instanceOf(error, CodexError.CodexAppServerProtocolParseError);
      assert.deepInclude(error, {
        operation: "route-wire-message",
        method: "thread/start",
        payloadKind: "object",
        presentFields: ["id", "method", "params"],
      });
      assert.isUndefined(error.requestId);
      assert.notProperty(error, "detail");
      assert.notProperty(error, "cause");
      assert.notInclude(error.message, secret);
    }),
  );

  it.effect("fails pending requests when input ends while the process is still running", () =>
    Effect.gen(function* () {
      const { stdio, input, output } = yield* makeInMemoryStdio();
      const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        terminationError: Effect.never,
      });
      const pending = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);
      yield* Queue.take(output);

      yield* Queue.end(input);
      yield* TestClock.adjust("249 millis");
      assert.isUndefined(pending.pollUnsafe());
      yield* TestClock.adjust("1 millis");

      const failure = yield* Fiber.join(pending).pipe(
        Effect.match({
          onFailure: (error) => error,
          onSuccess: () => assert.fail("Expected the pending request to fail"),
        }),
      );
      assert.instanceOf(failure, CodexError.CodexAppServerInputStreamEndedError);
    }),
  );

  it.effect("reports a process exit that follows input end", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const exitError = new CodexError.CodexAppServerProcessExitedError({ code: 1, pid: 123 });
      const exited = yield* Deferred.make<CodexError.CodexAppServerError>();
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        terminationError: Deferred.await(exited),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.end(input);
      yield* TestClock.adjust("100 millis");
      yield* Deferred.succeed(exited, exitError);

      assert.strictEqual(yield* Deferred.await(termination), exitError);
    }),
  );
});

it.layer(NodeServices.layer)("effect-codex-app-server protocol line offload", (it) => {
  const longParams = { threadId: "t", turnId: "u", itemId: "i", delta: "x".repeat(80) };
  const shortLine = (method: string) => JSON.stringify({ method, params: {} });
  const longLine = JSON.stringify({ method: "item/agentMessage/delta", params: longParams });

  it.effect("sends long lines off-thread, keeps short ones and routes them in stdout order", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const routed: Array<{ readonly method: string; readonly params?: unknown }> = [];
      const logged: Array<CodexProtocol.CodexAppServerProtocolLogEvent> = [];
      const written: Array<string> = [];
      const offloaded = yield* Deferred.make<string>();
      const release = yield* Deferred.make<void>();
      const allRouted = yield* Deferred.make<void>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        logIncoming: true,
        logger: (event) =>
          Effect.sync(() => {
            logged.push(event);
          }),
        lineOffload: {
          minChars: 64,
          decode: (line) =>
            Deferred.succeed(offloaded, line).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(
                CodexOffload.decodeOffloadedLine(line, (event) => ({
                  stream: "native",
                  payload: `${event.stage}:${JSON.stringify(event.payload).length}`,
                })),
              ),
            ),
          writeLog: (record) =>
            Effect.sync(() => {
              written.push(`${record.stream}/${record.payload}`);
            }),
          failure: Effect.never,
        },
        onNotification: (notification) =>
          Effect.suspend(() => {
            routed.push({ method: notification.method, params: notification.params });
            return routed.length === 3 ? Deferred.succeed(allRouted, undefined) : Effect.void;
          }).pipe(Effect.asVoid),
      });

      yield* Queue.offer(input, encoder.encode(`${shortLine("x/first")}\n`));
      yield* Queue.offer(input, encoder.encode(`${longLine}\n`));
      assert.equal(yield* Deferred.await(offloaded), longLine);
      // A read before the long one is handled at once; the reads after it wait for it.
      yield* Queue.offer(input, encoder.encode(`${shortLine("x/last")}\n`));
      yield* Effect.yieldNow;
      assert.deepEqual(routed, [{ method: "x/first", params: {} }]);

      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(allRouted);

      assert.deepEqual(routed, [
        { method: "x/first", params: {} },
        { method: "item/agentMessage/delta", params: longParams },
        { method: "x/last", params: {} },
      ]);
      assert.deepEqual(written, [
        `native/raw:${JSON.stringify(longLine).length}`,
        `native/decoded:${longLine.length}`,
      ]);
      assert.deepEqual(
        logged.map(({ stage, payload }) => [stage, payload]),
        [
          ["raw", shortLine("x/first")],
          ["decoded", { method: "x/first", params: {} }],
          ["raw", shortLine("x/last")],
          ["decoded", { method: "x/last", params: {} }],
        ],
      );
    }),
  );

  // Chunks pulled together are handled with no yield between them, so a fiber the first message
  // starts runs only after the last; an offload must not let it in earlier.
  it.effect("handles chunks read together in one burst, off-thread lines included", () =>
    Effect.gen(function* () {
      const run = (lineOffload: CodexProtocol.CodexAppServerLineOffload | undefined) =>
        Effect.gen(function* () {
          const scope = yield* Effect.scope;
          const { stdio } = yield* makeInMemoryStdio();
          const seen: Array<string> = [];
          const done = yield* Deferred.make<void>();
          const chunks = ["x/a", "x/b", "x/c"].map((method) =>
            encoder.encode(`${JSON.stringify({ method, params: longParams })}\n`),
          );
          yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
            stdio: Stdio.make({ ...stdio, stdin: Stream.make(...chunks) }),
            ...(lineOffload === undefined ? {} : { lineOffload }),
            onNotification: (notification) =>
              Effect.gen(function* () {
                seen.push(notification.method);
                if (notification.method === "x/a") {
                  yield* Effect.sync(() => seen.push("other fiber")).pipe(
                    Effect.andThen(Deferred.succeed(done, undefined)),
                    Effect.forkIn(scope),
                  );
                }
              }),
          });
          yield* Deferred.await(done);
          return seen;
        }).pipe(Effect.scoped);

      const inThread = yield* run(undefined);
      assert.deepEqual(inThread, ["x/a", "x/b", "x/c", "other fiber"]);
      // A worker answers later, as each decode here does after a yield.
      const offloaded = yield* run({
        minChars: 64,
        decode: (line) => Effect.yieldNow.pipe(Effect.as(CodexOffload.decodeOffloadedLine(line))),
        writeLog: () => Effect.void,
        failure: Effect.never,
      });
      assert.deepEqual(offloaded, inThread);
    }),
  );

  it.effect("keeps at most 256 reads pending behind lines still off-thread", () =>
    Effect.gen(function* () {
      const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
      const { stdio } = yield* makeInMemoryStdio();
      const routed: Array<string> = [];
      const releases = [yield* Deferred.make<void>(), yield* Deferred.make<void>()];
      const allRouted = yield* Deferred.make<void>();
      const count = 300;
      let calls = 0;
      let pulls = 0;
      // Every fiber that can run does, until it waits; no clock involved.
      const settle = Effect.forEach(Array.from({ length: 2048 }), () => Effect.yieldNow, {
        discard: true,
      });
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        // One read per pull, as a pipe delivers them.
        stdio: Stdio.make({
          ...stdio,
          stdin: Stream.fromEffectRepeat(
            Queue.take(input).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  pulls += 1;
                }),
              ),
            ),
          ),
        }),
        lineOffload: {
          minChars: 64,
          // The first two worker calls answer only when released.
          decode: (line) =>
            Effect.suspend(() => {
              const release = releases[calls++];
              return release === undefined ? Effect.void : Deferred.await(release);
            }).pipe(Effect.as(CodexOffload.decodeOffloadedLine(line))),
          writeLog: () => Effect.void,
          failure: Effect.never,
        },
        onNotification: (notification) =>
          Effect.suspend(() => {
            routed.push(notification.method);
            return routed.length === count + 2
              ? Deferred.succeed(allRouted, undefined)
              : Effect.void;
          }).pipe(Effect.asVoid),
      });

      yield* Queue.offer(input, encoder.encode(`${longLine}\n`));
      yield* Queue.offer(input, encoder.encode(`${longLine}\n`));
      for (let index = 0; index < count; index++) {
        yield* Queue.offer(input, encoder.encode(`${shortLine(`x/${index}`)}\n`));
      }
      // 256 reads pending, one more waiting for a slot and one the termination watch
      // (`Stream.interruptWhen`) pulls ahead, as in-thread; none overtakes the long lines.
      yield* settle;
      assert.deepEqual(routed, []);
      assert.isAtLeast(pulls, 256);
      assert.isAtMost(pulls, 258);
      assert.isAbove(yield* Queue.size(input), 0);

      // One read handled frees one slot, also while the second long line is still on the worker.
      yield* Deferred.succeed(releases[0]!, undefined);
      yield* settle;
      assert.deepEqual(routed, ["item/agentMessage/delta"]);
      assert.isAtMost(pulls, 259);

      yield* Deferred.succeed(releases[1]!, undefined);
      yield* Deferred.await(allRouted);
      assert.deepEqual(routed, [
        "item/agentMessage/delta",
        "item/agentMessage/delta",
        ...Array.from({ length: count }, (_, index) => `x/${index}`),
      ]);
    }),
  );

  it.effect("handles a line the offload hands back exactly as an in-thread line", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const logged: Array<CodexProtocol.CodexAppServerProtocolLogEvent> = [];
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      const malformed = `{"method":"x/long","params":"${"y".repeat(80)}"`;
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        logIncoming: true,
        logger: (event) =>
          Effect.sync(() => {
            logged.push(event);
          }),
        lineOffload: {
          minChars: 64,
          decode: (line) => Effect.succeed(CodexOffload.decodeOffloadedLine(line)),
          writeLog: () => Effect.die("no log entries expected"),
          failure: Effect.never,
        },
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(input, encoder.encode(`${malformed}\n`));
      const error = yield* Deferred.await(termination);

      assert.instanceOf(error, CodexError.CodexAppServerProtocolParseError);
      assert.equal(error.operation, "decode-wire-message");
      assert.deepEqual(
        logged.map(({ stage }) => stage),
        ["raw", "decode_failed"],
      );
    }),
  );

  it.effect("times a line sent off-thread as offload and a short line as json", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const recorded: Array<readonly [string, number]> = [];
      const routed = yield* Deferred.make<void>();
      let count = 0;
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        stageTiming: {
          now: () => 0,
          record: (stage, _elapsedMs, bytes) => {
            if (stage !== "framing") recorded.push([stage, bytes]);
          },
        },
        lineOffload: {
          minChars: 64,
          decode: (line) => Effect.succeed(CodexOffload.decodeOffloadedLine(line)),
          writeLog: () => Effect.void,
          failure: Effect.never,
        },
        onNotification: () =>
          Effect.suspend(() =>
            ++count === 2 ? Deferred.succeed(routed, undefined) : Effect.void,
          ).pipe(Effect.asVoid),
      });

      yield* Queue.offer(input, encoder.encode(`${shortLine("x/short")}\n${longLine}\n`));
      yield* Deferred.await(routed);

      // Long lines go to the worker before short ones decode on main, so only the set is fixed.
      assert.deepEqual(recorded.toSorted(), [
        ["json", shortLine("x/short").length],
        ["offload", longLine.length],
      ]);
    }),
  );

  // In-thread, termination stops stdout at its next read; a read already being handled completes
  // (its later messages are dropped, as every message is once the protocol has terminated).
  it.effect("completes a read it is handling when the protocol terminates, as in-thread", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const hold = yield* Deferred.make<void>();
      const holding = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      const firstHandled = yield* Deferred.make<void>();
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      const died = yield* Deferred.make<never, CodexError.CodexAppServerError>();
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        lineOffload: {
          minChars: 64,
          decode: (line) => Effect.succeed(CodexOffload.decodeOffloadedLine(line)),
          writeLog: () => Effect.void,
          failure: Deferred.await(died),
        },
        onNotification: () =>
          Deferred.succeed(holding, undefined).pipe(
            Effect.andThen(Deferred.await(hold)),
            Effect.andThen(Deferred.succeed(firstHandled, undefined)),
            Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
            Effect.asVoid,
          ),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(
        input,
        encoder.encode(`${shortLine("x/first")}\n${shortLine("x/second")}\n`),
      );
      yield* Deferred.await(holding);
      const workerStopped = new CodexError.CodexAppServerTransportError({
        operation: "read-input-stream",
        cause: new Error("Provider I/O worker stopped."),
      });
      yield* Deferred.fail(died, workerStopped);
      assert.strictEqual(yield* Deferred.await(termination), workerStopped);
      yield* Deferred.succeed(hold, undefined);

      const outcome = yield* Effect.raceFirst(
        Deferred.await(firstHandled).pipe(Effect.as("read completed")),
        Deferred.await(interrupted).pipe(Effect.as("read interrupted")),
      );
      assert.equal(outcome, "read completed");
    }),
  );

  it.effect("routes the lines before a long line whose worker call fails, as in-thread", () =>
    Effect.gen(function* () {
      const { stdio, input } = yield* makeInMemoryStdio();
      const routed: Array<string> = [];
      const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
      const callFailed = new CodexError.CodexAppServerTransportError({
        operation: "read-input-stream",
        cause: new Error("Provider I/O call failed."),
      });
      yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
        stdio,
        lineOffload: {
          minChars: 64,
          decode: () => Effect.fail(callFailed),
          writeLog: () => Effect.void,
          failure: Effect.never,
        },
        onNotification: (notification) =>
          Effect.sync(() => {
            routed.push(notification.method);
          }),
        onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
      });

      yield* Queue.offer(
        input,
        encoder.encode(`${shortLine("x/first")}\n${longLine}\n${shortLine("x/last")}\n`),
      );

      assert.strictEqual(yield* Deferred.await(termination), callFailed);
      assert.deepEqual(routed, ["x/first"]);
    }),
  );

  it.effect.each(["while it decodes a line", "before stdout closes", "after stdout closes"])(
    "ends the protocol with the offload's error when its worker dies %s",
    (when) =>
      Effect.gen(function* () {
        const { stdio, input, output } = yield* makeInMemoryStdio();
        const termination = yield* Deferred.make<CodexError.CodexAppServerError>();
        const died = yield* Deferred.make<never, CodexError.CodexAppServerError>();
        const classifying = yield* Deferred.make<void>();
        const exited = yield* Deferred.make<CodexError.CodexAppServerError>();
        const workerStopped = new CodexError.CodexAppServerTransportError({
          operation: "read-input-stream",
          cause: new Error("Provider I/O worker stopped."),
        });
        const transport = yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
          stdio,
          terminationError: Deferred.succeed(classifying, undefined).pipe(
            Effect.andThen(Deferred.await(exited)),
          ),
          lineOffload: {
            minChars: 64,
            decode: () => Deferred.await(died),
            writeLog: () => Effect.void,
            failure: Deferred.await(died),
          },
          onTermination: (error) => Deferred.succeed(termination, error).pipe(Effect.asVoid),
        });
        const pending = yield* transport.request("thread/read", {}).pipe(Effect.forkScoped);
        yield* Queue.take(output);

        // The pool kills the process when the worker dies, which closes stdout.
        const processExited = new CodexError.CodexAppServerProcessExitedError({ code: 143 });
        if (when === "while it decodes a line") {
          yield* Queue.offer(input, encoder.encode(`${longLine}\n`));
          yield* Deferred.fail(died, workerStopped);
        } else if (when === "before stdout closes") {
          yield* Deferred.fail(died, workerStopped);
          yield* Queue.end(input);
        } else {
          yield* Queue.end(input);
          yield* Deferred.await(classifying);
          yield* Deferred.fail(died, workerStopped);
        }
        yield* Deferred.succeed(exited, processExited);

        assert.strictEqual(yield* Deferred.await(termination), workerStopped);
        const failure = yield* Fiber.join(pending).pipe(
          Effect.match({
            onFailure: (error) => error,
            onSuccess: () => assert.fail("Expected the pending request to fail"),
          }),
        );
        assert.strictEqual(failure, workerStopped);
      }),
  );
});

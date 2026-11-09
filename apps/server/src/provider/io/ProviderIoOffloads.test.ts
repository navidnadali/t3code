// @effect-diagnostics nodeBuiltinImport:off - Fixture files are read once at module load.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import type * as NodeWorkerThreads from "node:worker_threads";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import type * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as References from "effect/References";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import * as AcpClient from "effect-acp/client";
import * as AcpError from "effect-acp/errors";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexError from "effect-codex-app-server/errors";
import * as CodexRpc from "effect-codex-app-server/rpc";
import { AcpLineOffloads } from "@t3tools/provider-acp/server/AcpSessionRuntime";
import { vi } from "vite-plus/test";

import * as ServerConfig from "../../config.ts";
import { makeCodexAppServerProtocolLogger } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { serializedRecordProblem, serializeEventNdjsonRecord } from "../EventNdjsonRecord.ts";
import * as ProviderIoOffloads from "./ProviderIoOffloads.ts";
import * as ProviderIoWorkerPool from "./ProviderIoWorkerPool.ts";

const workers = vi.hoisted(() => ({
  started: new Array<NodeWorkerThreads.Worker>(),
  exits: new WeakMap<NodeWorkerThreads.Worker, Promise<number>>(),
}));

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      super(...args);
      workers.started.push(this);
      workers.exits.set(this, new Promise((resolve) => this.once("exit", resolve)));
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

const fixturesDir = NodePath.join(import.meta.dirname, "../../orchestration-v2/testkit/fixtures");
const encoder = new TextEncoder();
const threadId = ThreadId.make("thread-offload");
const providerSessionId = "provider-session-offload";

const transcriptFrames = (file: string) =>
  NodeFS.readdirSync(fixturesDir).flatMap((dir) => {
    const path = NodePath.join(fixturesDir, dir, file);
    if (!NodeFS.existsSync(path)) return [];
    return NodeFS.readFileSync(path, "utf8")
      .split("\n")
      .filter((line) => line.trim() !== "")
      .map((line) => JSON.parse(line) as { type: string; frame?: Record<string, unknown> })
      .filter((entry) => entry.type === "emit_inbound" && entry.frame !== undefined)
      .map((entry) => entry.frame!);
  });

const big = (size: number, seed: string) => `${seed}:${"0123456789abcdef".repeat(size / 16)}`;

const codexLines = [
  ...transcriptFrames("codex_transcript.ndjson").map((frame) => JSON.stringify(frame)),
  JSON.stringify({
    method: "item/completed",
    params: {
      threadId: "t",
      turnId: "u",
      item: {
        type: "commandExecution",
        id: "item-big",
        command: "cat big",
        aggregatedOutput: big(96 * 1024, "out"),
        headers: { Authorization: "Bearer secret", note: '{"apiKey":"k","ok":[1,2]}' },
      },
    },
  }),
  JSON.stringify({
    method: "item/commandExecution/outputDelta",
    params: { threadId: "t", turnId: "u", itemId: "item-big", delta: big(20 * 1024, "delta") },
  }),
  JSON.stringify({ id: 99, result: { thread: { id: "t", notes: big(8 * 1024, "result") } } }),
  `{"method":"x/never-closed","params":"${big(4096, "unterminated")}"`,
];

const acpLines = (() => {
  let nextId = 1;
  return [
    ...transcriptFrames("registry_transcript.ndjson").flatMap((frame) =>
      frame.kind === "notification"
        ? [JSON.stringify({ jsonrpc: "2.0", method: frame.method, params: frame.params })]
        : frame.kind === "request"
          ? [
              JSON.stringify({
                jsonrpc: "2.0",
                id: nextId++,
                method: frame.method,
                params: frame.params,
              }),
            ]
          : [],
    ),
    JSON.stringify({
      jsonrpc: "2.0",
      method: "session/update",
      params: {
        sessionId: "s",
        update: {
          sessionUpdate: "tool_call_update",
          toolCallId: "tool-big",
          status: "completed",
          content: [{ type: "content", content: { type: "text", text: big(96 * 1024, "acp") } }],
        },
      },
    }),
    JSON.stringify({
      jsonrpc: "2.0",
      method: "session/update",
      params: { sessionId: "s", update: { sessionUpdate: "unknown_kind", note: big(4096, "x") } },
    }),
    JSON.stringify([
      { jsonrpc: "2.0", id: 900, method: "x/batched", params: { note: big(4096, "batch") } },
    ]),
  ];
})();

// Mixed chunk sizes, so lines start and end anywhere, including inside UTF-8 sequences.
const chunked = (lines: ReadonlyArray<string>) => {
  const bytes = encoder.encode(`${lines.join("\n")}\n`);
  const sizes = [1, 7, 64, 333, 4096, 65536];
  const chunks: Array<Uint8Array> = [];
  for (let offset = 0, index = 0; offset < bytes.length; index++) {
    const size = sizes[index % sizes.length]!;
    chunks.push(bytes.slice(offset, offset + size));
    offset += size;
  }
  return chunks;
};

const captureWarnings = (warnings: Array<unknown>) =>
  Logger.layer([
    Logger.make(({ logLevel, message, fiber }) => {
      if (logLevel === "Warn") {
        warnings.push([message, fiber.getRef(References.CurrentLogAnnotations)]);
      }
    }),
  ]);

// Replies to the provider's requests come from other fibers, so their timing is not compared.
const makeStdio = Effect.gen(function* () {
  const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
  const stdio = Stdio.make({
    args: Effect.succeed([]),
    stdin: Stream.fromQueue(input),
    stdout: () => Sink.drain,
    stderr: () => Sink.drain,
  });
  return { input, stdio };
});

const isIncoming = (payload: string) => JSON.parse(payload).event?.direction === "incoming";

const nativeLogger = (payloads: Array<string>): ProviderEventLoggers.EventNdjsonLogger => ({
  filePath: "/tmp/provider-events.log",
  write: (event) =>
    Effect.sync(() => {
      const record = serializeEventNdjsonRecord("native", event);
      if (record._tag === "Serialized") payloads.push(record.payload);
    }),
  // As the real view: a record serialized for another stream, or one write never builds, is refused.
  writeSerialized: (record) =>
    Effect.sync(() => {
      const problem = serializedRecordProblem("native", record);
      payloads.push(problem === undefined ? record.payload : `refused: ${problem}`);
    }),
  close: () => Effect.void,
});

const runCodex = (
  chunks: ReadonlyArray<Uint8Array>,
  offloads: ReturnType<typeof ProviderIoOffloads.make> | undefined,
) =>
  Effect.gen(function* () {
    const seen: Array<unknown> = [];
    const logs: Array<string> = [];
    const warnings: Array<unknown> = [];
    const native = nativeLogger(logs);
    const { input, stdio } = yield* makeStdio;
    const lineOffload =
      offloads === undefined
        ? undefined
        : Option.getOrThrow(
            yield* offloads.codex.open({
              kill: Effect.void,
              log: {
                providerSessionId,
                writeSerialized: (record) => native.writeSerialized!(record, threadId),
              },
            }),
          );
    const logger = makeCodexAppServerProtocolLogger({
      nativeEventLogger: native,
      threadId,
      providerSessionId: providerSessionId as never,
    })!;
    const client = yield* CodexClient.make(stdio, {
      logIncoming: true,
      logOutgoing: true,
      logger,
      ...(lineOffload === undefined ? {} : { lineOffload }),
    }).pipe(Effect.provide(captureWarnings(warnings)));
    for (const method of Object.keys(CodexRpc.SERVER_NOTIFICATION_PARAMS)) {
      yield* client.handleServerNotification(
        method as CodexRpc.ServerNotificationMethod,
        (params) =>
          Effect.sync(() => {
            seen.push(["notification", method, params]);
          }),
      );
    }
    yield* client.handleUnknownServerNotification((method, params) =>
      Effect.sync(() => {
        seen.push(["unknown", method, params]);
      }),
    );
    for (const chunk of chunks) yield* Queue.offer(input, chunk);
    yield* Queue.end(input);
    const termination = yield* client.awaitTermination.pipe(Effect.flip);
    return { seen, logs: logs.filter(isIncoming), warnings, termination: termination._tag };
  }).pipe(Effect.scoped);

const runAcp = (
  chunks: ReadonlyArray<Uint8Array>,
  offloads: ReturnType<typeof ProviderIoOffloads.make> | undefined,
) =>
  Effect.gen(function* () {
    const seen: Array<unknown> = [];
    const logged: Array<string> = [];
    const warnings: Array<unknown> = [];
    const { input, stdio } = yield* makeStdio;
    const lineOffload =
      offloads === undefined
        ? undefined
        : Option.getOrThrow(yield* offloads.acp.open({ kill: Effect.void }));
    const terminated = yield* Deferred.make<AcpError.AcpError>();
    const client = yield* AcpClient.make(
      stdio,
      {
        logIncoming: true,
        logOutgoing: true,
        // Copied now: the RPC server later rewrites request messages it handles.
        logger: (event) =>
          Effect.sync(() => {
            if (event.direction === "incoming") logged.push(JSON.stringify(event));
          }),
        onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
        ...(lineOffload === undefined ? {} : { lineOffload }),
      },
      Effect.succeed(new AcpError.AcpInputStreamEndedError({})),
    ).pipe(Effect.provide(captureWarnings(warnings)));
    yield* client.handleSessionUpdate((params) =>
      Effect.sync(() => {
        seen.push(["update", params]);
      }),
    );
    yield* client.handleElicitationComplete((params) =>
      Effect.sync(() => {
        seen.push(["elicitationComplete", params]);
      }),
    );
    yield* client.handleUnknownExtNotification((method, params) =>
      Effect.sync(() => {
        seen.push(["ext", method, params]);
      }),
    );
    for (const chunk of chunks) yield* Queue.offer(input, chunk);
    yield* Queue.end(input);
    const termination = yield* Deferred.await(terminated);
    return { seen, logged, warnings, termination: termination._tag };
  }).pipe(Effect.scoped);

const withOffloads = <A, E, R>(
  minChars: ProviderIoOffloads.LineOffloadThresholds,
  use: (offloads: ReturnType<typeof ProviderIoOffloads.make>) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const pool = yield* ProviderIoWorkerPool.make({ workers: 2 });
    return yield* use(ProviderIoOffloads.make(pool, minChars));
  }).pipe(Effect.scoped);

it.layer(NodeServices.layer)("provider I/O line offloads", (it) => {
  it.effect.each([0, 2048])(
    "Codex replay fixtures give the same events, logs and writes off-thread from %i characters",
    (minChars) =>
      Effect.gen(function* () {
        for (const chunks of [
          chunked(codexLines),
          codexLines.map((line) => encoder.encode(`${line}\n`)),
        ]) {
          const inThread = yield* runCodex(chunks, undefined);
          const offloaded = yield* withOffloads({ codex: minChars, acp: minChars }, (offloads) =>
            runCodex(chunks, offloads),
          );
          assert.isAbove(inThread.seen.length, 1000);
          assert.isAbove(inThread.logs.length, 100);
          assert.equal(inThread.termination, "CodexAppServerProtocolParseError");
          assert.deepStrictEqual(offloaded, inThread);
        }
      }),
  );

  it.effect.each([0, 2048])(
    "ACP replay fixtures give the same updates, logs and writes off-thread from %i characters",
    (minChars) =>
      Effect.gen(function* () {
        for (const chunks of [
          chunked(acpLines),
          acpLines.map((line) => encoder.encode(`${line}\n`)),
        ]) {
          const inThread = yield* runAcp(chunks, undefined);
          const offloaded = yield* withOffloads({ codex: minChars, acp: minChars }, (offloads) =>
            runAcp(chunks, offloads),
          );
          // Session updates and elicitation completion, excluding the unknown kind warned about once.
          assert.equal(inThread.seen.length, 40);
          assert.lengthOf(inThread.warnings, 1);
          assert.deepStrictEqual(offloaded, inThread);
        }
      }),
  );

  const deltaLine = (delta: string) =>
    `${JSON.stringify({
      method: "item/agentMessage/delta",
      params: { threadId: "t", turnId: "u", itemId: "i", delta },
    })}\n`;

  // Killing the child closes its stdout; its exit status is known from then on.
  const codexSession = (
    offloads: ReturnType<typeof ProviderIoOffloads.make>,
    killed: Deferred.Deferred<void>,
  ) =>
    Effect.gen(function* () {
      const { input, stdio } = yield* makeStdio;
      const lineOffload = Option.getOrThrow(
        yield* offloads.codex.open({
          kill: Deferred.succeed(killed, undefined).pipe(Effect.andThen(Queue.end(input))),
        }),
      );
      const client = yield* CodexClient.make(
        stdio,
        { lineOffload },
        Deferred.await(killed).pipe(
          Effect.as(new CodexError.CodexAppServerProcessExitedError({ code: 143 })),
        ),
      );
      const deltas = yield* Queue.unbounded<unknown>();
      yield* client.handleServerNotification("item/agentMessage/delta", (params) =>
        Queue.offer(deltas, params).pipe(Effect.asVoid),
      );
      const send = (delta: string) => Queue.offer(input, encoder.encode(deltaLine(delta)));
      return { client, deltas, send };
    });

  it.effect(
    "a worker that dies mid-session fails that Codex session and kills its app-server",
    () =>
      Effect.gen(function* () {
        const startedBefore = workers.started.length;
        const pool = yield* ProviderIoWorkerPool.make({ workers: 1 });
        const offloads = ProviderIoOffloads.make(pool, { codex: 0, acp: 0 });
        const killed = yield* Deferred.make<void>();

        yield* Effect.gen(function* () {
          const session = yield* codexSession(offloads, killed);
          yield* session.send("before");
          assert.deepInclude(yield* Queue.take(session.deltas), { delta: "before" });

          const worker = workers.started[startedBefore]!;
          yield* Effect.promise(() => worker.terminate());
          yield* Deferred.await(killed);

          const error = yield* session.client.awaitTermination.pipe(Effect.flip);
          assert.instanceOf(error, CodexError.CodexAppServerTransportError);
          assert.equal(error.operation, "read-input-stream");
          assert.instanceOf(error.cause, ProviderIoWorkerPool.ProviderIoWorkerError);
          assert.equal(yield* Queue.size(session.deltas), 0);
        }).pipe(Effect.scoped);

        // The next session restarts the worker.
        yield* Effect.gen(function* () {
          const session = yield* codexSession(offloads, yield* Deferred.make<void>());
          yield* session.send("next");
          assert.deepInclude(yield* Queue.take(session.deltas), { delta: "next" });
        }).pipe(Effect.scoped);
        assert.equal(workers.started.length - startedBefore, 2);
      }).pipe(Effect.scoped),
  );

  it.effect("a worker that dies mid-session fails that ACP session and kills its agent", () =>
    Effect.gen(function* () {
      const startedBefore = workers.started.length;
      const pool = yield* ProviderIoWorkerPool.make({ workers: 1 });
      const offloads = ProviderIoOffloads.make(pool, { codex: 0, acp: 0 });
      const update = (text: string) =>
        `${JSON.stringify({
          jsonrpc: "2.0",
          method: "session/update",
          params: {
            sessionId: "s",
            update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text } },
          },
        })}\n`;
      const acpSession = (killed: Deferred.Deferred<void>) =>
        Effect.gen(function* () {
          const { input, stdio } = yield* makeStdio;
          const lineOffload = Option.getOrThrow(
            yield* offloads.acp.open({
              kill: Deferred.succeed(killed, undefined).pipe(Effect.andThen(Queue.end(input))),
            }),
          );
          const terminated = yield* Deferred.make<AcpError.AcpError>();
          const client = yield* AcpClient.make(
            stdio,
            {
              lineOffload,
              onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
            },
            Deferred.await(killed).pipe(
              Effect.as(new AcpError.AcpProcessExitedError({ code: 143 })),
            ),
          );
          const updates = yield* Queue.unbounded<unknown>();
          yield* client.handleSessionUpdate((params) =>
            Queue.offer(updates, params).pipe(Effect.asVoid),
          );
          const send = (text: string) => Queue.offer(input, encoder.encode(update(text)));
          return { terminated, updates, send };
        });
      const killed = yield* Deferred.make<void>();

      yield* Effect.gen(function* () {
        const session = yield* acpSession(killed);
        yield* session.send("before");
        assert.deepInclude(yield* Queue.take(session.updates), { sessionId: "s" });

        yield* Effect.promise(() => workers.started[startedBefore]!.terminate());
        yield* Deferred.await(killed);

        const error = yield* Deferred.await(session.terminated);
        assert.instanceOf(error, AcpError.AcpTransportError);
        assert.equal(error.operation, "read-input-stream");
        assert.instanceOf(error.cause, ProviderIoWorkerPool.ProviderIoWorkerError);
        assert.equal(yield* Queue.size(session.updates), 0);
      }).pipe(Effect.scoped);

      yield* Effect.gen(function* () {
        const session = yield* acpSession(yield* Deferred.make<void>());
        yield* session.send("next");
        assert.deepInclude(yield* Queue.take(session.updates), { sessionId: "s" });
      }).pipe(Effect.scoped);
      assert.equal(workers.started.length - startedBefore, 2);
    }).pipe(Effect.scoped),
  );

  // Past about 2,400 nested objects main cannot read a clone back; at 30,000 the worker cannot send.
  const nested = (shape: "objects" | "arrays", depth: number) =>
    shape === "objects"
      ? `${'{"a":'.repeat(depth)}1${"}".repeat(depth)}`
      : `${"[".repeat(depth)}${"]".repeat(depth)}`;
  const depthOf = (value: unknown) => {
    let deepest = 0;
    const stack: Array<readonly [unknown, number]> = [[value, 0]];
    while (stack.length > 0) {
      const [current, depth] = stack.pop()!;
      if (typeof current !== "object" || current === null) continue;
      deepest = Math.max(deepest, depth + 1);
      for (const child of Object.values(current)) stack.push([child, depth + 1]);
    }
    return deepest;
  };

  // A session whose next line or termination is awaited, so a dead worker fails rather than hangs.
  const codexLineSession = (
    offloads: ReturnType<typeof ProviderIoOffloads.make> | undefined,
    logs: Array<string>,
  ) =>
    Effect.gen(function* () {
      const { input, stdio } = yield* makeStdio;
      const native = nativeLogger(logs);
      const lineOffload =
        offloads === undefined
          ? undefined
          : Option.getOrThrow(
              yield* offloads.codex.open({
                kill: Effect.void,
                log: {
                  providerSessionId,
                  writeSerialized: (record) => native.writeSerialized!(record, threadId),
                },
              }),
            );
      const client = yield* CodexClient.make(stdio, {
        logIncoming: true,
        logOutgoing: true,
        logger: makeCodexAppServerProtocolLogger({
          nativeEventLogger: native,
          threadId,
          providerSessionId: providerSessionId as never,
        })!,
        ...(lineOffload === undefined ? {} : { lineOffload }),
      });
      const received = yield* Queue.unbounded<unknown>();
      yield* client.handleUnknownServerNotification((_method, params) =>
        Queue.offer(received, params).pipe(Effect.asVoid),
      );
      const send = (params: string) =>
        Queue.offer(input, encoder.encode(`{"method":"x/line","params":${params}}\n`));
      const next = Effect.raceFirst(
        Queue.take(received),
        client.awaitTermination.pipe(
          Effect.flip,
          Effect.flatMap((error) => Effect.die(`Codex session ended: ${error._tag}`)),
        ),
      );
      return { send, next };
    });

  const acpLineSession = (offloads: ReturnType<typeof ProviderIoOffloads.make> | undefined) =>
    Effect.gen(function* () {
      const { input, stdio } = yield* makeStdio;
      const lineOffload =
        offloads === undefined
          ? undefined
          : Option.getOrThrow(yield* offloads.acp.open({ kill: Effect.void }));
      const terminated = yield* Deferred.make<AcpError.AcpError>();
      const client = yield* AcpClient.make(stdio, {
        ...(lineOffload === undefined ? {} : { lineOffload }),
        onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
      });
      const received = yield* Queue.unbounded<unknown>();
      yield* client.handleUnknownExtNotification((_method, params) =>
        Queue.offer(received, params).pipe(Effect.asVoid),
      );
      const send = (params: string) =>
        Queue.offer(
          input,
          encoder.encode(`{"jsonrpc":"2.0","method":"x/line","params":{"tree":${params}}}\n`),
        );
      const next = Effect.raceFirst(
        Queue.take(received),
        Deferred.await(terminated).pipe(
          Effect.flatMap((error) => Effect.die(`ACP session ended: ${error._tag}`)),
        ),
      );
      return { send, next };
    });

  it.effect.each([
    ["codex", "objects", 3000],
    ["codex", "arrays", 30_000],
    ["acp", "objects", 3000],
    ["acp", "arrays", 30_000],
  ] as const)(
    "%s: a line of %s nested %i deep gives the in-thread result and spares other sessions",
    ([kind, shape, depth]) =>
      Effect.gen(function* () {
        const deep = nested(shape, depth);
        const run = (offloads: ReturnType<typeof ProviderIoOffloads.make> | undefined) =>
          Effect.gen(function* () {
            const logs: Array<string> = [];
            const session =
              kind === "codex"
                ? yield* codexLineSession(offloads, logs)
                : yield* acpLineSession(offloads);
            yield* session.send(deep);
            const deepResult = depthOf(yield* session.next);
            yield* session.send('"after"');
            const after = yield* session.next;
            return { deepResult, after, logs: logs.filter(isIncoming) };
          });

        const inThread = yield* run(undefined).pipe(Effect.scoped);
        const offloaded = yield* Effect.gen(function* () {
          const pool = yield* ProviderIoWorkerPool.make({ workers: 1 });
          const offloads = ProviderIoOffloads.make(pool, { codex: 0, acp: 0 });
          const codexBystander = yield* codexLineSession(offloads, []);
          const acpBystander = yield* acpLineSession(offloads);
          yield* codexBystander.send('"before"');
          yield* acpBystander.send('"before"');
          assert.equal(yield* codexBystander.next, "before");
          assert.deepEqual(yield* acpBystander.next, { tree: "before" });

          const result = yield* run(offloads);

          yield* codexBystander.send('"still"');
          yield* acpBystander.send('"still"');
          assert.equal(yield* codexBystander.next, "still");
          assert.deepEqual(yield* acpBystander.next, { tree: "still" });
          return result;
        }).pipe(Effect.scoped);

        assert.equal(inThread.deepResult, kind === "codex" ? depth : depth + 1);
        assert.deepStrictEqual(offloaded, inThread);
      }),
  );

  it.effect.each([0, 1])(
    "with %i workers the offload layer opens offloads only when there are workers",
    (count) =>
      Effect.gen(function* () {
        const startedBefore = workers.started.length;
        const config = {
          providerIoWorkers: count,
        } as unknown as ServerConfig.ServerConfig["Service"];
        const opened = yield* Effect.gen(function* () {
          const codex = yield* ProviderIoOffloads.CodexLineOffloads;
          const acp = yield* AcpLineOffloads;
          return [
            Option.isSome(yield* codex.open({ kill: Effect.void })),
            Option.isSome(yield* acp.open({ kill: Effect.void })),
          ];
        }).pipe(
          Effect.provide(
            ProviderIoOffloads.layer.pipe(
              Layer.provideMerge(ProviderIoWorkerPool.layer),
              Layer.provide(Layer.succeed(ServerConfig.ServerConfig, config)),
            ),
          ),
          Effect.scoped,
        );
        assert.deepEqual(opened, [count > 0, count > 0]);
        assert.equal(workers.started.length - startedBefore, count);
      }),
  );

  // The worker dies inside the session scope's close, after the offload's own finalizers ran.
  it.effect.each(["after", "before"])(
    "kills the child of a session whose worker dies while it closes, %s its failure watch started",
    (when) =>
      Effect.gen(function* () {
        const startedBefore = workers.started.length;
        const pool = yield* ProviderIoWorkerPool.make({ workers: 1 });
        const dyingPool: ProviderIoWorkerPool.ProviderIoWorkerPool["Service"] = {
          open: <Options, Output, E, Request = never, Reply = never, Event = never>(
            input: ProviderIoWorkerPool.ProviderIoOpenInput<
              Options,
              Output,
              E,
              Request,
              Reply,
              Event
            >,
          ) =>
            pool
              .open(input)
              .pipe(
                Effect.tap(() =>
                  Effect.addFinalizer(() =>
                    Effect.promise(() => workers.started[startedBefore]!.terminate()),
                  ),
                ),
              ),
        };
        const offloads = ProviderIoOffloads.make(dyingPool, { codex: 0, acp: 0 });
        const killed = yield* Deferred.make<void>();

        yield* Effect.gen(function* () {
          const offload = Option.getOrThrow(
            yield* offloads.codex.open({ kill: Deferred.succeed(killed, undefined) }),
          );
          // A round trip lets the failure watch start; without it the scope closes first.
          if (when === "after") yield* offload.decode('{"id":1,"result":{}}');
        }).pipe(Effect.scoped);

        yield* Deferred.await(killed);
      }).pipe(Effect.scoped),
  );
});

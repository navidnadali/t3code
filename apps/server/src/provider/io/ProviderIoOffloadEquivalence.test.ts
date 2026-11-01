// @effect-diagnostics nodeBuiltinImport:off
import * as NodeUtil from "node:util";
import type * as NodeWorkerThreads from "node:worker_threads";

import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import type * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as AcpProtocol from "effect-acp/protocol";
import * as CodexProtocol from "effect-codex-app-server/protocol";
import type * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { vi } from "vite-plus/test";

import { makeCodexAppServerProtocolLogger } from "../../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { serializeEventNdjsonRecord } from "../EventNdjsonRecord.ts";
import * as ProviderIoOffloads from "./ProviderIoOffloads.ts";
import * as ProviderIoWorkerPool from "./ProviderIoWorkerPool.ts";
import {
  ACP_SYNTHETIC_LINES,
  acpWireLines,
  below,
  fuzzInput,
  inboundFrames,
  limitInputs,
  logSizeInputs,
  materialize,
  mulberry32,
  pick,
  splitPoints,
  utf8,
  type FuzzProtocol,
  type Random,
} from "./testFixtures/providerIoFuzz.ts";

// The production worker and its stages run unchanged; this only records which stage each opens.
const workers = vi.hoisted(() => ({ stages: new Map<object, Array<string>>() }));

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  class RecordedWorker extends actual.Worker {
    constructor(...args: ConstructorParameters<typeof actual.Worker>) {
      super(...args);
      workers.stages.set(this, []);
    }
    override postMessage(...args: Parameters<NodeWorkerThreads.Worker["postMessage"]>) {
      const message = args[0] as { readonly type?: unknown; readonly stage?: unknown };
      if (message.type === "open") workers.stages.get(this)?.push(String(message.stage));
      super.postMessage(...args);
    }
  }
  return { ...actual, Worker: RecordedWorker };
});

type Pool = ProviderIoWorkerPool.ProviderIoWorkerPool["Service"];

const threadId = ThreadId.make("thread-offload-equivalence");
const providerSessionId = "provider-session-offload-equivalence";

// Class, tag, message, fields and cause: a worker error rebuilt on main must match in-thread's.
const describeError = (error: unknown): unknown => {
  if (!(error instanceof Error)) return error === undefined ? undefined : NodeUtil.inspect(error);
  const { cause, ...fields } = { ...error } as Record<string, unknown>;
  return {
    constructor: error.constructor.name,
    name: error.name,
    tag: (error as { readonly _tag?: unknown })._tag,
    message: error.message,
    fields,
    cause: describeError(cause ?? (error as { readonly cause?: unknown }).cause),
  };
};

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

// Native log payloads per direction: replies to requests are written by other fibers.
const nativeLogger = (
  logs: Record<string, Array<string>>,
): ProviderEventLoggers.EventNdjsonLogger => {
  const append = (payload: string) => {
    const direction = String(
      (JSON.parse(payload) as { readonly event?: { readonly direction?: unknown } }).event
        ?.direction,
    );
    (logs[direction] ??= []).push(payload);
  };
  return {
    filePath: "/dev/null",
    write: (event) =>
      Effect.sync(() => {
        const record = serializeEventNdjsonRecord("native", event);
        if (record._tag === "Serialized") append(record.payload);
      }),
    writeSerialized: (record) => Effect.sync(() => append(record.payload)),
    close: () => Effect.void,
  };
};

interface Run {
  readonly routed: ReadonlyArray<unknown>;
  readonly logs: Readonly<Record<string, ReadonlyArray<unknown>>>;
  readonly termination: unknown;
}

interface Offload {
  readonly pool: Pool;
  readonly minChars: number;
}

const runCodex = (
  chunks: ReadonlyArray<Uint8Array>,
  maxIncomingMessageBytes: number | undefined,
  offload: Offload | undefined,
) =>
  Effect.gen(function* () {
    const routed: Array<unknown> = [];
    const logs: Record<string, Array<string>> = {};
    const native = nativeLogger(logs);
    const { input, stdio } = yield* makeStdio;
    const lineOffload =
      offload === undefined
        ? undefined
        : Option.getOrThrow(
            yield* ProviderIoOffloads.make(offload.pool, {
              codex: offload.minChars,
              acp: offload.minChars,
            }).codex.open({
              kill: Effect.void,
              log: {
                providerSessionId,
                writeSerialized: (payload) =>
                  native.writeSerialized!({ stream: "native", payload }, threadId),
              },
            }),
          );
    const terminated = yield* Deferred.make<unknown>();
    yield* CodexProtocol.makeCodexAppServerPatchedProtocol({
      stdio,
      logIncoming: true,
      logOutgoing: true,
      logger: makeCodexAppServerProtocolLogger({
        nativeEventLogger: native,
        threadId,
        providerSessionId: providerSessionId as never,
      })!,
      ...(lineOffload === undefined ? {} : { lineOffload }),
      ...(maxIncomingMessageBytes === undefined ? {} : { maxIncomingMessageBytes }),
      onNotification: (notification) =>
        Effect.sync(() => {
          routed.push(["notification", notification]);
        }),
      onRequest: (request) =>
        Effect.sync(() => {
          routed.push(["request", request]);
          return { ok: true };
        }),
      onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
    });
    for (const chunk of chunks) yield* Queue.offer(input, chunk);
    yield* Queue.end(input);
    const termination = describeError(yield* Deferred.await(terminated));
    return { routed, logs, termination } satisfies Run;
  }).pipe(Effect.scoped);

const runAcp = (chunks: ReadonlyArray<Uint8Array>, offload: Offload | undefined) =>
  Effect.gen(function* () {
    const routed: Array<unknown> = [];
    const logs: Record<string, Array<string>> = {};
    const { input, stdio } = yield* makeStdio;
    const lineOffload =
      offload === undefined
        ? undefined
        : Option.getOrThrow(
            yield* ProviderIoOffloads.make(offload.pool, {
              codex: offload.minChars,
              acp: offload.minChars,
            }).acp.open({ kill: Effect.void }),
          );
    const terminated = yield* Deferred.make<unknown>();
    yield* AcpProtocol.makeAcpPatchedProtocol({
      stdio,
      serverRequestMethods: new Set(["session/request_permission", "fs/read_text_file"]),
      logIncoming: true,
      logOutgoing: true,
      // Copied now: the RPC server later rewrites request messages it handles.
      logger: (event) =>
        Effect.sync(() => {
          (logs[event.direction] ??= []).push(JSON.stringify([event.stage, event.payload]));
        }),
      ...(lineOffload === undefined ? {} : { lineOffload }),
      onNotification: (notification) =>
        Effect.sync(() => {
          routed.push(["notification", notification]);
        }),
      onIncomingRequest: (requestId, method, payload) =>
        Effect.sync(() => {
          routed.push(["request", requestId, method, payload]);
        }),
      onExtRequest: (method, params) =>
        Effect.sync(() => {
          routed.push(["ext", method, params]);
          return { ok: true };
        }),
      onTermination: (error) => Deferred.succeed(terminated, error).pipe(Effect.asVoid),
    });
    for (const chunk of chunks) yield* Queue.offer(input, chunk);
    yield* Queue.end(input);
    const termination = describeError(yield* Deferred.await(terminated));
    return { routed, logs, termination } satisfies Run;
  }).pipe(Effect.scoped);

interface OffloadCase {
  readonly name: string;
  readonly minChars: number;
  readonly run: (offload: Offload | undefined) => Effect.Effect<Run>;
}

// Counts the lines a case sends to the workers.
const countingPool = (pool: Pool, counter: { calls: number }): Pool =>
  ProviderIoWorkerPool.ProviderIoWorkerPool.of({
    open: (input) =>
      pool.open(input).pipe(
        Effect.map(
          Option.map((session) => ({
            ...session,
            call: (request) => {
              counter.calls += 1;
              return session.call(request);
            },
          })),
        ),
      ),
  });

const preview = (value: unknown) =>
  NodeUtil.inspect(value, { depth: 6, breakLength: Infinity, maxStringLength: 160 }).slice(0, 900);

const compareInThreadAndOffloaded = (cases: ReadonlyArray<OffloadCase>) =>
  Effect.gen(function* () {
    workers.stages.clear();
    const pool = yield* ProviderIoWorkerPool.make({ workers: 2 });
    const mismatches: Array<string> = [];
    let offloadedCases = 0;
    yield* Effect.forEach(
      cases,
      (offloadCase) =>
        Effect.gen(function* () {
          const counter = { calls: 0 };
          const inThread = yield* offloadCase.run(undefined);
          const offloaded = yield* offloadCase.run({
            pool: countingPool(pool, counter),
            minChars: offloadCase.minChars,
          });
          if (counter.calls > 0) offloadedCases += 1;
          if (!NodeUtil.isDeepStrictEqual(offloaded, inThread)) {
            const channel = (["termination", "routed", "logs"] as const).find(
              (key) => !NodeUtil.isDeepStrictEqual(offloaded[key], inThread[key]),
            )!;
            mismatches.push(
              `${offloadCase.name} (offload from ${offloadCase.minChars} chars), ${channel}: in-thread ${preview(inThread[channel])} / offloaded ${preview(offloaded[channel])}`,
            );
          }
        }),
      { concurrency: 16, discard: true },
    );
    return { mismatches, offloadedCases };
  }).pipe(Effect.scoped);

const FUZZ_SEED = 0x2029_f025;
const FUZZ_CASES = 10_000;
const THRESHOLDS = [0, 0, 1, 64, 2048, 16_384];

const recordedLines = (protocol: FuzzProtocol) =>
  (protocol === "codex"
    ? inboundFrames("codex_transcript.ndjson").flatMap(({ frames }) =>
        frames.map((frame) => JSON.stringify(frame)),
      )
    : [
        ...inboundFrames("registry_transcript.ndjson").flatMap(({ frames }) =>
          acpWireLines(frames),
        ),
        ...ACP_SYNTHETIC_LINES,
      ]
  ).filter((line) => utf8(line).length <= 600);

const offloadCase = (
  protocol: FuzzProtocol,
  name: string,
  random: Random,
  bytes: Uint8Array,
  maxIncomingMessageBytes: number | undefined,
): OffloadCase => {
  const points = splitPoints(random, bytes);
  const chunks = materialize(random, bytes, points);
  return {
    name: `${name} split at ${points.join(",")}`,
    minChars: pick(random, THRESHOLDS),
    run: (offload) =>
      protocol === "codex"
        ? runCodex(chunks, maxIncomingMessageBytes, offload)
        : runAcp(chunks, offload),
  };
};

const fuzzCases = (protocol: FuzzProtocol) => {
  const random = mulberry32(FUZZ_SEED ^ (protocol === "codex" ? 0x0c0d : 0x0acb));
  const recorded = recordedLines(protocol);
  const cases = Array.from({ length: FUZZ_CASES }, (_, index) => {
    const { bytes, features } = fuzzInput(random, protocol, recorded);
    const limit = protocol === "codex" && random() < 0.3 ? 16 + below(random, 400) : undefined;
    const name = `${protocol} case ${index} (seed ${FUZZ_SEED.toString(16)}; ${features.join(" ") || "plain"}${limit === undefined ? "" : `; limit ${limit}`}) ${NodeUtil.inspect(Buffer.from(bytes).toString("latin1").slice(0, 240))}`;
    return offloadCase(protocol, name, random, bytes, limit);
  });
  if (protocol === "codex") {
    for (const input of limitInputs([16, 64, 100, 257])) {
      for (let chunking = 0; chunking < 3; chunking += 1) {
        cases.push(offloadCase(protocol, `limit ${input.name}`, random, input.bytes, input.limit));
      }
    }
    for (const input of logSizeInputs()) {
      for (let chunking = 0; chunking < 2; chunking += 1) {
        cases.push(offloadCase(protocol, input.name, random, input.bytes, undefined));
      }
    }
  }
  return cases;
};

const STAGES: Record<FuzzProtocol, string> = { codex: "codex-app-server", acp: "acp" };

it.effect.each([["codex"], ["acp"]] as const)(
  "%s stdout gives the same messages, log records and termination with long lines on P1-025's production worker stages over 10,000 seeded cases",
  ([protocol]) =>
    Effect.gen(function* () {
      const cases = fuzzCases(protocol);
      const { mismatches, offloadedCases } = yield* compareInThreadAndOffloaded(cases);
      if (mismatches.length > 0) {
        assert.fail(
          `${mismatches.length} of ${cases.length} cases differ:\n${mismatches.slice(0, 3).join("\n")}`,
        );
      }
      assert.isAbove(offloadedCases, cases.length / 2, "the workers took most cases' lines");
      const perWorker = [...workers.stages.values()];
      assert.equal(perWorker.length, 2);
      for (const stages of perWorker) {
        assert.isNotEmpty(stages, "both workers took sessions");
        assert.deepEqual([...new Set(stages)], [STAGES[protocol]]);
      }
    }),
);

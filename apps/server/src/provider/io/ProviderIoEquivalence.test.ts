// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import type * as NodeWorkerThreads from "node:worker_threads";

import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { vi } from "vite-plus/test";

import * as ProviderIoWorkerPool from "./ProviderIoWorkerPool.ts";
import type { ProviderIoStage } from "./providerIoProtocol.ts";
import {
  acpMutantNames,
  acpWireStage,
  claudeSdkLogStage,
  codexMutantNames,
  codexWireStage,
  type CodexWireOptions,
  type EquivalenceWorkerVariant,
} from "./testFixtures/providerIoEquivalenceStages.ts";

const workers = vi.hoisted(() => ({
  variant: { framer: "intact" } as EquivalenceWorkerVariant,
  sessions: new Map<object, number>(),
}));

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  // Pool workers run the equivalence stages, intact or with one framer broken on purpose.
  class EquivalenceWorker extends actual.Worker {
    constructor(..._args: ConstructorParameters<typeof actual.Worker>) {
      super(new URL("./testFixtures/providerIoEquivalenceWorker.ts", import.meta.url), {
        name: "t3-provider-io",
        workerData: workers.variant,
      });
      workers.sessions.set(this, 0);
    }
    override postMessage(...args: Parameters<NodeWorkerThreads.Worker["postMessage"]>) {
      if ((args[0] as { readonly type?: unknown }).type === "open") {
        workers.sessions.set(this, (workers.sessions.get(this) ?? 0) + 1);
      }
      super.postMessage(...args);
    }
  }
  return { ...actual, Worker: EquivalenceWorker };
});

type Pool = ProviderIoWorkerPool.ProviderIoWorkerPool["Service"];

interface StageRun {
  readonly outputs: ReadonlyArray<unknown>;
  readonly failure?: string;
}

interface StageCase {
  readonly name: string;
  readonly run: (
    pool: Pool,
  ) => Effect.Effect<{ readonly run: StageRun; readonly onWorker: boolean }>;
}

const messageOf = (cause: unknown) => (cause instanceof Error ? cause.message : String(cause));

const runInThread = <Options, Output>(
  stage: ProviderIoStage<Options, Output>,
  options: Options,
  chunks: ReadonlyArray<Uint8Array>,
): StageRun => {
  const outputs: Array<unknown> = [];
  const emit = (output: Output) => {
    outputs.push(output);
  };
  try {
    const session = stage.open(options);
    for (const chunk of chunks) session.push(chunk, emit);
    session.end(emit);
    return { outputs };
  } catch (error) {
    return { outputs, failure: messageOf(error) };
  }
};

// The caller's side of the pool: in-thread when `open` gives None (0 workers), else on a worker.
const stageCase = <Options, Output>(
  name: string,
  stage: ProviderIoStage<Options, Output>,
  options: Options,
  chunks: ReadonlyArray<Uint8Array>,
  highWaterMark?: number,
): StageCase => ({
  name,
  run: (pool) =>
    Effect.gen(function* () {
      const opened = yield* pool.open({
        stage,
        options,
        onFailure: (error) => error,
        kill: Effect.void,
        ...(highWaterMark === undefined ? {} : { highWaterMark }),
      });
      if (Option.isNone(opened))
        return { run: runInThread(stage, options, chunks), onWorker: false };
      const session = opened.value;
      const outputs: Array<unknown> = [];
      const write = Effect.forEach(chunks, (chunk) => session.write(chunk), {
        discard: true,
      }).pipe(Effect.andThen(session.end), Effect.ignore);
      const read = Stream.runForEach(session.output, (output) =>
        Effect.sync(() => {
          outputs.push(output);
        }),
      ).pipe(
        Effect.match({
          onSuccess: (): StageRun => ({ outputs }),
          onFailure: (error): StageRun => ({
            outputs,
            failure:
              error._tag === "ProviderIoStageError"
                ? messageOf(error.cause)
                : `worker stopped: ${messageOf(error.cause)}`,
          }),
        }),
      );
      const [, run] = yield* Effect.all([write, read], { concurrency: 2 });
      return { run, onWorker: true };
    }).pipe(Effect.scoped),
});

const preview = (value: unknown) =>
  NodeUtil.inspect(value, { depth: 8, breakLength: Infinity, maxStringLength: 120 }).slice(0, 600);

const describeMismatch = (inThread: StageRun, onWorker: StageRun) => {
  if (NodeUtil.isDeepStrictEqual(onWorker, inThread)) return undefined;
  const length = Math.max(inThread.outputs.length, onWorker.outputs.length);
  let index = 0;
  while (
    index < length &&
    NodeUtil.isDeepStrictEqual(onWorker.outputs[index], inThread.outputs[index])
  ) {
    index += 1;
  }
  return index < length
    ? `output ${index} (of ${inThread.outputs.length} in-thread, ${onWorker.outputs.length} on a worker): in-thread ${preview(inThread.outputs[index])} / worker ${preview(onWorker.outputs[index])}`
    : `failure: in-thread ${String(inThread.failure)} / worker ${String(onWorker.failure)}`;
};

interface Comparison {
  readonly mismatches: ReadonlyArray<string>;
  readonly offloaded: number;
}

// Runs every case at 0 workers and through a 2-worker pool and compares them output by output.
const compareAtZeroAndTwoWorkers = (
  variant: EquivalenceWorkerVariant,
  cases: ReadonlyArray<StageCase>,
): Effect.Effect<Comparison> =>
  Effect.gen(function* () {
    workers.variant = variant;
    workers.sessions.clear();
    const inThread = yield* ProviderIoWorkerPool.make({ workers: 0 });
    const onWorkers = yield* ProviderIoWorkerPool.make({ workers: 2 });
    let offloaded = 0;
    const mismatches: Array<string> = [];
    yield* Effect.forEach(
      cases,
      (stageCase) =>
        Effect.gen(function* () {
          const reference = yield* stageCase.run(inThread);
          const candidate = yield* stageCase.run(onWorkers);
          assert.isFalse(reference.onWorker);
          if (candidate.onWorker) offloaded += 1;
          const mismatch = describeMismatch(reference.run, candidate.run);
          if (mismatch !== undefined) mismatches.push(`${stageCase.name}: ${mismatch}`);
        }),
      { concurrency: 16, discard: true },
    );
    return { mismatches, offloaded };
  }).pipe(Effect.scoped);

const assertEquivalent = (comparison: Comparison, cases: number) => {
  if (comparison.mismatches.length > 0) {
    assert.fail(
      `${comparison.mismatches.length} of ${cases} cases differ:\n${comparison.mismatches.slice(0, 3).join("\n")}`,
    );
  }
  assert.equal(comparison.offloaded, cases, "every case ran on a worker");
  const perWorker = [...workers.sessions.values()];
  assert.equal(perWorker.length, 2);
  assert.isTrue(
    perWorker.every((sessions) => sessions > 0),
    "both workers took sessions",
  );
};

function mulberry32(seed: number) {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type Random = () => number;
const below = (random: Random, bound: number) => Math.floor(random() * bound);
const pick = <A>(random: Random, items: ReadonlyArray<A>): A => items[below(random, items.length)]!;

const encoder = new TextEncoder();

const FIXTURES = NodePath.join(import.meta.dirname, "../../orchestration-v2/testkit/fixtures");

const inboundFrames = (transcript: string) =>
  NodeFS.readdirSync(FIXTURES, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .flatMap((entry) => {
      const file = NodePath.join(FIXTURES, entry.name, transcript);
      if (!NodeFS.existsSync(file)) return [];
      const frames = NodeFS.readFileSync(file, "utf8")
        .split("\n")
        .filter((line) => line.trim().length > 0)
        .map((line) => JSON.parse(line) as { readonly type?: unknown; readonly frame?: unknown })
        .filter((entry) => entry.type === "emit_inbound")
        .map((entry) => entry.frame);
      return [{ fixture: entry.name, frames }];
    });

const fixedChunks = (bytes: Uint8Array, size: number) =>
  Array.from({ length: Math.ceil(bytes.length / size) }, (_, index) =>
    bytes.subarray(index * size, (index + 1) * size),
  );

const randomChunks = (bytes: Uint8Array, random: Random) => {
  const chunks: Array<Uint8Array> = [];
  for (let offset = 0; offset < bytes.length;) {
    const size = 1 + below(random, 700);
    chunks.push(bytes.subarray(offset, offset + size));
    offset += size;
  }
  return chunks;
};

const fixtureChunkings = (text: string, seed: number) => {
  const bytes = encoder.encode(text);
  return [
    { name: "whole", chunks: [bytes] },
    { name: "4096-byte", chunks: fixedChunks(bytes, 4096) },
    { name: "61-byte", chunks: fixedChunks(bytes, 61) },
    { name: "random", chunks: randomChunks(bytes, mulberry32(seed)) },
  ];
};

const casesFor = <Options>(
  fixture: string,
  stage: ProviderIoStage<Options, unknown>,
  texts: ReadonlyArray<{ readonly name: string; readonly text: string; readonly options: Options }>,
): Array<StageCase> =>
  texts.flatMap(({ name, text, options }, index) =>
    fixtureChunkings(text, fixture.length * 31 + index).map(({ name: chunking, chunks }) =>
      stageCase(`${fixture} ${name} ${chunking}`, stage, options, chunks),
    ),
  );

const codexFixtureCases = () =>
  inboundFrames("codex_transcript.ndjson").flatMap(({ fixture, frames }) => {
    const lines = frames.map((frame) => JSON.stringify(frame));
    const sizes = lines.map((line) => encoder.encode(line).length).toSorted((a, b) => a - b);
    const median = sizes[Math.floor(sizes.length / 2)] ?? 64;
    return casesFor<CodexWireOptions>(fixture, codexWireStage, [
      { name: "lf", text: `${lines.join("\n")}\n`, options: {} },
      { name: "crlf-blank-tail", text: `\r\n \t\n${lines.join("\r\n\n")}`, options: {} },
      { name: "oversize", text: `${lines.join("\n")}\n`, options: { maxLineBytes: median } },
    ]);
  });

const ACP_SYNTHETIC_LINES = [
  '{"jsonrpc":"2.0","id":5,"error":{"code":-32000,"message":"denied é"}}',
  '{"jsonrpc":"2.0","id":7,"error":{"_tag":"Cause","code":-32603,"message":"x","data":[{"_tag":"Die","defect":{"code":-32000,"message":"boom"}}]}}',
  '{"jsonrpc":"2.0","method":"session/update","params":{}}',
  '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"state_update","state":"idle","error":{"code":1,"message":"m"}}}}',
  '{"jsonrpc":"2.0","method":"elicitation/complete","params":{"elicitationId":"e1"}}',
  '{"jsonrpc":"2.0","method":"session/elicitation/complete","params":{"bad":true}}',
  '{"jsonrpc":"2.0","method":"x/ext","params":{"t":"🙂最"}}',
  '{"jsonrpc":"2.0","id":"str-1","method":"x/ext-request","params":{}}',
];

const acpWireLines = (frames: ReadonlyArray<unknown>) =>
  frames.map((frame, index) => {
    const { kind, method, params, result } = frame as Record<string, unknown>;
    const id = 100 + index;
    if (kind === "notification") return JSON.stringify({ jsonrpc: "2.0", method, params });
    if (kind === "request") return JSON.stringify({ jsonrpc: "2.0", id, method, params });
    return JSON.stringify({ jsonrpc: "2.0", id, result: result ?? null });
  });

const acpFixtureCases = () =>
  inboundFrames("registry_transcript.ndjson").flatMap(({ fixture, frames }) => {
    const lines = [...acpWireLines(frames), ...ACP_SYNTHETIC_LINES];
    return casesFor<void>(fixture, acpWireStage, [
      { name: "lf", text: `${lines.join("\n")}\n`, options: undefined },
      { name: "crlf", text: `${lines.join("\r\n")}\r\n`, options: undefined },
      { name: "tail", text: lines.join("\n"), options: undefined },
      {
        name: "malformed",
        text: `${lines.slice(0, 2).join("\n")}\n{"jsonrpc":\n${lines.slice(2).join("\n")}\n`,
        options: undefined,
      },
    ]);
  });

const claudeFixtureCases = () =>
  inboundFrames("claude_transcript.ndjson").flatMap(({ fixture, frames }) => {
    const lines = frames.map((frame) => JSON.stringify(frame));
    return casesFor<void>(fixture, claudeSdkLogStage, [
      { name: "lf", text: `${lines.join("\n")}\n`, options: undefined },
      { name: "crlf", text: `${lines.join("\r\n")}`, options: undefined },
    ]);
  });

it.effect.each([
  { provider: "Codex", fixtures: 20, cases: codexFixtureCases },
  { provider: "ACP", fixtures: 10, cases: acpFixtureCases },
  { provider: "Claude", fixtures: 30, cases: claudeFixtureCases },
])(
  "$provider replay fixtures decode to the same frames and log records at 0 and 2 workers",
  ({ fixtures, cases }) =>
    Effect.gen(function* () {
      const all = cases();
      assert.isAbove(all.length, fixtures * 4);
      assertEquivalent(yield* compareAtZeroAndTwoWorkers({ framer: "intact" }, all), all.length);
    }),
);

const FUZZ_SEED = 0x2029_f022;
const FUZZ_CASES = 10_000;
const MULTIBYTE = ["é", "ß", "€", "最", "🙂", "𝄞", "ࠀ", "�"];

const randomText = (random: Random) =>
  Array.from({ length: below(random, 12) }, () =>
    random() < 0.5 ? pick(random, MULTIBYTE) : String.fromCharCode(32 + below(random, 95)),
  ).join("");

// Short real frames, so the fuzz mixes known, unknown and malformed payloads.
const shortLines = (lines: ReadonlyArray<string>) =>
  lines.filter((line) => encoder.encode(line).length <= 600);

const codexFuzzLines = () =>
  shortLines(
    inboundFrames("codex_transcript.ndjson").flatMap(({ frames }) =>
      frames.map((frame) => JSON.stringify(frame)),
    ),
  );

const acpFuzzLines = () =>
  shortLines([
    ...inboundFrames("registry_transcript.ndjson").flatMap(({ frames }) => acpWireLines(frames)),
    ...ACP_SYNTHETIC_LINES,
  ]);

const codexFuzzLine = (random: Random, recorded: ReadonlyArray<string>) => {
  const roll = random();
  if (roll < 0.45) return pick(random, recorded);
  const text = randomText(random);
  if (roll < 0.7) {
    return JSON.stringify({
      method: "item/agentMessage/delta",
      params: { threadId: "t", turnId: "u", itemId: "i", delta: text },
    });
  }
  if (roll < 0.8) return JSON.stringify({ method: `x/${text}`, params: { text } });
  if (roll < 0.85) return JSON.stringify({ id: below(random, 9), result: { text } });
  if (roll < 0.9) return pick(random, ["", " \t", "\r", "[1,2]"]);
  if (roll < 0.95) return `${text}${"x".repeat(below(random, 300))}`;
  return `{"id":1,"text":${JSON.stringify(text)}`;
};

const acpFuzzLine = (random: Random, recorded: ReadonlyArray<string>) => {
  const roll = random();
  if (roll < 0.45) return pick(random, recorded);
  const text = randomText(random);
  if (roll < 0.8) {
    return JSON.stringify({
      jsonrpc: "2.0",
      method: "session/update",
      params: {
        sessionId: "s",
        update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text } },
      },
    });
  }
  if (roll < 0.95) return JSON.stringify({ jsonrpc: "2.0", method: "x/ext", params: { text } });
  if (roll < 0.97) return "";
  return `{"jsonrpc":"2.0","method":${JSON.stringify(text)}`;
};

// Split points favour the middle of a UTF-8 sequence and the gap between CR and LF.
const splitPoints = (random: Random, bytes: Uint8Array) => {
  if (bytes.length < 2) return [];
  const points = new Set<number>();
  for (let count = 1 + below(random, Math.min(16, bytes.length - 1)); count > 0; count -= 1) {
    let point = 1 + below(random, bytes.length - 1);
    if (random() < 0.5) {
      for (let step = 0; step < bytes.length; step += 1) {
        const at = ((point - 1 + step) % (bytes.length - 1)) + 1;
        if ((bytes[at]! & 0xc0) === 0x80 || (bytes[at - 1] === 0x0d && bytes[at] === 0x0a)) {
          point = at;
          break;
        }
      }
    }
    points.add(point);
  }
  return [...points].toSorted((a, b) => a - b);
};

// Chunks arrive as views into a larger buffer, as owned arrays, or as Node Buffers.
const materialize = (random: Random, bytes: Uint8Array, points: ReadonlyArray<number>) =>
  [0, ...points].map((start, index) => {
    const view = bytes.subarray(start, points[index] ?? bytes.length);
    const kind = below(random, 3);
    return kind === 0 ? view : kind === 1 ? view.slice() : Buffer.from(view);
  });

const fuzzCases = (framer: "codex" | "acp") => {
  const random = mulberry32(FUZZ_SEED ^ (framer === "codex" ? 0x0c0d : 0x0acb));
  const recorded = framer === "codex" ? codexFuzzLines() : acpFuzzLines();
  let splits = 0;
  const cases = Array.from({ length: FUZZ_CASES }, (_, index): StageCase => {
    const lines = Array.from({ length: 1 + below(random, 8) }, () =>
      framer === "codex" ? codexFuzzLine(random, recorded) : acpFuzzLine(random, recorded),
    );
    const text = lines
      .map((line, at) => {
        const roll = random();
        if (at === lines.length - 1 && roll < 0.25) return line;
        return line + (roll < 0.6 ? "\n" : roll < 0.95 ? "\r\n" : "\r\r\n");
      })
      .join("");
    const bytes = encoder.encode(text);
    const points = splitPoints(random, bytes);
    splits += points.length;
    const chunks = materialize(random, bytes, points);
    const highWaterMark = pick(random, [1, 8, 64, undefined]);
    const name = `${framer} fuzz case ${index} (seed ${FUZZ_SEED.toString(16)}) ${JSON.stringify(text)} split at ${points.join(",")}`;
    if (framer === "acp") return stageCase(name, acpWireStage, undefined, chunks, highWaterMark);
    const maxLineBytes = random() < 0.4 ? 16 + below(random, 400) : undefined;
    return stageCase(name, codexWireStage, { maxLineBytes }, chunks, highWaterMark);
  });
  return { cases, splits };
};

it.effect.each(["codex", "acp"] as const)(
  "the %s framer gives the same output in-thread and on a worker over 10,000 seeded random splits",
  (framer) =>
    Effect.gen(function* () {
      const { cases, splits } = fuzzCases(framer);
      assert.isAtLeast(splits, FUZZ_CASES);
      assertEquivalent(
        yield* compareAtZeroAndTwoWorkers({ framer: "intact" }, cases),
        cases.length,
      );
    }),
);

it.effect.each([
  ...codexMutantNames.map((mutant) => ({ framer: "codex" as const, mutant })),
  ...acpMutantNames.map((mutant) => ({ framer: "acp" as const, mutant })),
])("fails when the worker's $framer framer $mutant", (variant) =>
  Effect.gen(function* () {
    const { cases } = fuzzCases(variant.framer);
    const comparison = yield* compareAtZeroAndTwoWorkers(variant, cases.slice(0, 2_000));
    assert.isNotEmpty(comparison.mismatches);
  }),
);

// @effect-diagnostics nodeBuiltinImport:off
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
  acpWireStage,
  claudeSdkLogStage,
  codexWireStage,
  mutantVariants,
  type CodexWireOptions,
  type EquivalenceWorkerVariant,
} from "./testFixtures/providerIoEquivalenceStages.ts";
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

const workers = vi.hoisted(() => ({
  variant: { part: "intact" } as EquivalenceWorkerVariant,
  sessions: new Map<object, number>(),
}));

vi.mock("node:worker_threads", async (importOriginal) => {
  const actual = await importOriginal<typeof NodeWorkerThreads>();
  // Pool workers run the equivalence stages, intact or with one rule broken on purpose.
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

// Class, message and own fields: a worker that fails with another class or shape must not pass.
const describeFailure = (error: unknown) =>
  error instanceof Error
    ? {
        name: error.name,
        constructor: error.constructor.name,
        message: error.message,
        fields: { ...error },
      }
    : { thrown: NodeUtil.inspect(error) };

interface StageRun {
  readonly outputs: ReadonlyArray<unknown>;
  readonly failure?: unknown;
}

interface StageCase {
  readonly name: string;
  readonly run: (
    pool: Pool,
  ) => Effect.Effect<{ readonly run: StageRun; readonly onWorker: boolean }>;
}

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
    return { outputs, failure: describeFailure(error) };
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
      // Opening fails only for options that do not clone; these are plain data.
      const opened = yield* pool
        .open({
          stage,
          options,
          onFailure: (error) => error,
          kill: Effect.void,
          ...(highWaterMark === undefined ? {} : { highWaterMark }),
        })
        .pipe(Effect.orDie);
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
                ? describeFailure(error.cause)
                : { workerStopped: describeFailure(error.cause) },
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
    : `failure: in-thread ${preview(inThread.failure)} / worker ${preview(onWorker.failure)}`;
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

// Every case ran on a worker and both workers took sessions, so a crashed worker is not a pass.
const assertOffloaded = (comparison: Comparison, cases: number) => {
  assert.equal(comparison.offloaded, cases, "every case ran on a worker");
  const perWorker = [...workers.sessions.values()];
  assert.equal(perWorker.length, 2);
  assert.isTrue(
    perWorker.every((sessions) => sessions > 0),
    "both workers took sessions",
  );
};

const assertEquivalent = (comparison: Comparison, cases: number) => {
  if (comparison.mismatches.length > 0) {
    assert.fail(
      `${comparison.mismatches.length} of ${cases} cases differ:\n${comparison.mismatches.slice(0, 3).join("\n")}`,
    );
  }
  assertOffloaded(comparison, cases);
};

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
  const bytes = utf8(text);
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
    const sizes = lines.map((line) => utf8(line).length).toSorted((a, b) => a - b);
    const median = sizes[Math.floor(sizes.length / 2)] ?? 64;
    return casesFor<CodexWireOptions>(fixture, codexWireStage, [
      { name: "lf", text: `${lines.join("\n")}\n`, options: {} },
      { name: "crlf-blank-tail", text: `\r\n \t\n${lines.join("\r\n\n")}`, options: {} },
      { name: "oversize", text: `${lines.join("\n")}\n`, options: { maxLineBytes: median } },
    ]);
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
  ["Codex", 20, codexFixtureCases],
  ["ACP", 10, acpFixtureCases],
  ["Claude", 30, claudeFixtureCases],
] as const)(
  "%s replay fixtures decode to the same frames and log records at 0 and 2 workers",
  ([, fixtures, cases]) =>
    Effect.gen(function* () {
      const all = cases();
      assert.isAbove(all.length, fixtures * 4);
      assertEquivalent(yield* compareAtZeroAndTwoWorkers({ part: "intact" }, all), all.length);
    }),
);

const FUZZ_SEED = 0x2029_f022;
const FUZZ_CASES = 10_000;
const LINE_LIMITS = [16, 64, 100, 257];

// Short real frames, so the fuzz mixes known, unknown and malformed payloads.
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

const caseFor = (
  protocol: FuzzProtocol,
  name: string,
  random: Random,
  bytes: Uint8Array,
  maxLineBytes: number | undefined,
) => {
  const points = splitPoints(random, bytes);
  const chunks = materialize(random, bytes, points);
  const highWaterMark = pick(random, [1, 8, 64, undefined]);
  const title = `${name} split at ${points.join(",")}`;
  const options: CodexWireOptions = { maxLineBytes };
  return {
    splits: points.length,
    stageCase:
      protocol === "acp"
        ? stageCase(title, acpWireStage, undefined, chunks, highWaterMark)
        : stageCase(title, codexWireStage, options, chunks, highWaterMark),
  };
};

const fuzzCases = (protocol: FuzzProtocol) => {
  const random = mulberry32(FUZZ_SEED ^ (protocol === "codex" ? 0x0c0d : 0x0acb));
  const recorded = recordedLines(protocol);
  let splits = 0;
  const cases = Array.from({ length: FUZZ_CASES }, (_, index) => {
    const { bytes, features } = fuzzInput(random, protocol, recorded);
    const maxLineBytes =
      protocol === "codex" && random() < 0.4 ? 16 + below(random, 400) : undefined;
    const name = `${protocol} fuzz case ${index} (seed ${FUZZ_SEED.toString(16)}; ${features.join(" ") || "plain"}) ${NodeUtil.inspect(Buffer.from(bytes).toString("latin1").slice(0, 300))}`;
    const made = caseFor(protocol, name, random, bytes, maxLineBytes);
    splits += made.splits;
    return made.stageCase;
  });
  // Lines at exactly the Codex line limit and one byte over, each split three ways.
  if (protocol === "codex") {
    for (const input of limitInputs(LINE_LIMITS)) {
      for (let chunking = 0; chunking < 3; chunking += 1) {
        const made = caseFor(protocol, `limit ${input.name}`, random, input.bytes, input.limit);
        splits += made.splits;
        cases.push(made.stageCase);
      }
    }
    for (const input of logSizeInputs()) {
      for (let chunking = 0; chunking < 2; chunking += 1) {
        const made = caseFor(protocol, input.name, random, input.bytes, undefined);
        splits += made.splits;
        cases.push(made.stageCase);
      }
    }
  }
  return { cases, splits };
};

it.effect.each([["codex"], ["acp"]] as const)(
  "the %s framer gives the same output in-thread and on a worker over 10,000 seeded random splits",
  ([protocol]) =>
    Effect.gen(function* () {
      const { cases, splits } = fuzzCases(protocol);
      assert.isAtLeast(splits, FUZZ_CASES);
      assertEquivalent(yield* compareAtZeroAndTwoWorkers({ part: "intact" }, cases), cases.length);
    }),
);

// The fuzz cases each mutant must fail on: its part's own protocol (log mutants: Codex).
const MUTANT_CASES = 2_500;

it.effect.each(mutantVariants.map((variant) => [variant.part, variant.mutant, variant] as const))(
  "fails when the worker's %s %s",
  ([, , variant]) =>
    Effect.gen(function* () {
      const { cases } = fuzzCases(variant.part === "acp parser" ? "acp" : "codex");
      const sample = [...cases.slice(0, MUTANT_CASES), ...cases.slice(FUZZ_CASES)];
      const comparison = yield* compareAtZeroAndTwoWorkers(variant, sample);
      assertOffloaded(comparison, sample.length);
      assert.isNotEmpty(comparison.mismatches);
    }),
);

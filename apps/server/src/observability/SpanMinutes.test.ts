// @effect-diagnostics nodeBuiltinImport:off - the test gunzips archives the way a reader would.
import * as NodeZlib from "node:zlib";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
import { makeLocalFileTracer, type TraceSink } from "@t3tools/shared/observability";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import * as ServerConfig from "../config.ts";
import * as ResourceAttribution from "../resourceTelemetry/ResourceAttribution.ts";
import * as Observability from "./Observability.ts";
import * as SpanMinutes from "./SpanMinutes.ts";

const nanos = (ms: number) => BigInt(Math.round(ms * 1_000_000));

const decodeLine = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const readings: SpanMinutes.ProcessReadings = {
  loop: { delayP50Ms: 0.42, delayP99Ms: 2.1, delayMaxMs: 41.5, utilization: 0.27 },
  cpu: { userMs: 8_100, systemMs: 900 },
  memory: { rssBytes: 1_200_000_000, heapUsedBytes: 600_000_000, heapTotalBytes: 700_000_000 },
};

const discardingSink: TraceSink = {
  filePath: "unused",
  push: () => {},
  flush: Effect.void,
  flushSync: () => {},
  close: () => Effect.void,
};

describe("SpanMinutes", () => {
  it("counts every span per name with its total, max and a p95 within the histogram's error", () => {
    const aggregate = SpanMinutes.makeSpanAggregate();
    for (let index = 1; index <= 1_000; index++) {
      aggregate.record("ProjectionStore.getThread", 0n, nanos(index * 0.37));
    }
    for (let index = 0; index < 990; index++) aggregate.record("sqlite", nanos(5), nanos(5.05));
    for (let index = 0; index < 10; index++) aggregate.record("sqlite", 0n, nanos(5_000));
    aggregate.record("Orchestrator.dispatch", 0n, nanos(351.5));
    aggregate.record("server.subscription", 0n, nanos(2 * 60 * 60_000));
    aggregate.record("untimed", 0n, 0n);
    for (let index = 0; index < 1_000; index++) aggregate.record("sqlite.cached", 0n, 14_000n);
    aggregate.record("tick", 0n, 4_000n);
    for (let index = 0; index < 950; index++) aggregate.record("Codec.decode", 0n, 1_000n);
    for (let index = 0; index < 50; index++) aggregate.record("Codec.decode", 0n, nanos(1));

    const stats = aggregate.drain();
    // Exact count, total and max; the p95 within 1/16 of the nearest-rank one (the 950th of 1,000).
    const expected: Record<string, SpanMinutes.SpanStat> = {
      "ProjectionStore.getThread": [1_000, 185_185, 370, 351.5],
      sqlite: [1_000, 50_050, 5_000, 0.05],
      "Orchestrator.dispatch": [1, 351.5, 351.5, 351.5],
      "server.subscription": [1, 7_200_000, 7_200_000, 7_200_000],
      untimed: [1, 0, 0, 0],
      "sqlite.cached": [1_000, 14, 0.014, 0.014],
      tick: [1, 0.004, 0.004, 0.004],
      "Codec.decode": [1_000, 50.95, 1, 0.001],
    };
    assert.deepStrictEqual(Object.keys(stats), Object.keys(expected));
    for (const [name, [count, totalMs, maxMs, p95Ms]] of Object.entries(expected)) {
      const [actualCount, actualTotalMs, actualMaxMs, actualP95Ms] = stats[name]!;
      assert.deepStrictEqual([actualCount, actualTotalMs, actualMaxMs], [count, totalMs, maxMs]);
      assert.closeTo(actualP95Ms, p95Ms, p95Ms / 16, name);
    }
    // Up to 19 spans the p95 is the slowest one, so it is exact.
    assert.strictEqual(stats["Orchestrator.dispatch"]![3], 351.5);
    assert.deepStrictEqual(aggregate.drain(), {});
  });

  it("folds names past the cap into one entry and drops names idle for a minute", () => {
    const aggregate = SpanMinutes.makeSpanAggregate();
    const longName = `McpServer.${"x".repeat(SpanMinutes.MAX_SPAN_NAME_LENGTH)}`;
    aggregate.record(longName, 0n, nanos(1));
    for (let index = 0; index < SpanMinutes.MAX_SPAN_NAMES + 4; index++) {
      aggregate.record(`span-${index}`, 0n, nanos(2));
    }
    const first = aggregate.drain();
    assert.strictEqual(Object.keys(first).length, SpanMinutes.MAX_SPAN_NAMES + 1);
    assert.deepStrictEqual(
      first[longName.slice(0, SpanMinutes.MAX_SPAN_NAME_LENGTH)],
      [1, 1, 1, 1],
    );
    assert.deepStrictEqual(first[SpanMinutes.OTHER_SPAN_NAME], [5, 10, 2, 2]);

    aggregate.record("span-0", 0n, nanos(2));
    assert.deepStrictEqual(Object.keys(aggregate.drain()), ["span-0"]);
    for (let index = 1; index < SpanMinutes.MAX_SPAN_NAMES; index++) {
      aggregate.record(`fresh-${index}`, 0n, nanos(3));
    }
    const third = aggregate.drain();
    assert.strictEqual(Object.keys(third).length, SpanMinutes.MAX_SPAN_NAMES - 1);
    assert.isUndefined(third[SpanMinutes.OTHER_SPAN_NAME]);
  });

  it.effect(
    "writes one line per wall-clock minute with every span's name and none of its data",
    () =>
      Effect.gen(function* () {
        const lines = yield* Queue.unbounded<string>();
        const secrets = [
          "deploy the reactor tonight",
          "/home/someone/private-repo",
          "thread-7f3a9c",
        ];
        yield* Effect.scoped(
          Effect.gen(function* () {
            const minutes = yield* SpanMinutes.makeWith({
              sampler: Effect.succeed(Effect.succeed(readings)),
              append: (line) => Queue.offer(lines, line).pipe(Effect.asVoid),
            });
            const tracer = yield* makeLocalFileTracer({
              filePath: "unused",
              maxBytes: 1024,
              maxFiles: 1,
              batchWindowMs: 10_000,
              sink: discardingSink,
              sqlMinDurationMs: 2,
              onSpanEnd: minutes.record,
            });
            const runSpan = (
              name: string,
              startMs: number,
              durationMs: number,
              options: {
                readonly sampled?: boolean;
                readonly exit?: Exit.Exit<unknown, unknown>;
              } = {},
            ) => {
              const span = tracer.span({
                name,
                parent: Option.none(),
                annotations: Context.empty(),
                links: [],
                startTime: nanos(startMs),
                kind: "internal",
                root: false,
                sampled: options.sampled ?? true,
              });
              span.attribute("thread.message", secrets[0]);
              span.attribute("project.path", secrets[1]);
              span.event("delivered", nanos(startMs), { "thread.id": secrets[2] });
              span.end(nanos(startMs + durationMs), options.exit ?? Exit.void);
            };

            runSpan("Orchestrator.dispatch", 1_000, 12);
            runSpan("Orchestrator.dispatch", 2_000, 30, { exit: Exit.fail(secrets[2]) });
            // Below the minimum trace level, so never written to the trace file.
            runSpan("ProviderIo.decode", 3_000, 0.5, { sampled: false });
            // Counted by the SQL summary instead of written.
            runSpan("sqlite", 4_000, 0.25);
            yield* TestClock.adjust("1 minute");
            const first = yield* Queue.take(lines);
            for (const secret of secrets) expect(first).not.toContain(secret);
            assert.deepStrictEqual(decodeLine(first), {
              start: "1970-01-01T00:00:00.000Z",
              end: "1970-01-01T00:01:00.000Z",
              pid: process.pid,
              spans: {
                "Orchestrator.dispatch": [2, 42, 30, 30],
                "ProviderIo.decode": [1, 0.5, 0.5, 0.5],
                sqlite: [1, 0.25, 0.25, 0.25],
              },
              ...readings,
            });

            runSpan("Orchestrator.dispatch", 61_000, 5);
            yield* TestClock.adjust("30 seconds");
          }),
        );
        assert.deepStrictEqual(decodeLine(yield* Queue.take(lines)), {
          start: "1970-01-01T00:01:00.000Z",
          end: "1970-01-01T00:01:30.000Z",
          pid: process.pid,
          spans: { "Orchestrator.dispatch": [1, 5, 5, 5] },
          ...readings,
        });
        assert.strictEqual(yield* Queue.size(lines), 0);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("gzips each closed file and keeps only the newest archives within both caps", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-span-minutes-" });
      const filePath = path.join(directory, SpanMinutes.SPAN_MINUTES_FILE_NAME);
      const listing = fs.readDirectory(directory).pipe(Effect.map((entries) => entries.toSorted()));
      const gunzip = (name: string) =>
        fs
          .readFile(path.join(directory, name))
          .pipe(Effect.map((data) => NodeZlib.gunzipSync(data).toString()));
      const line = (minute: number) => `${String(minute).repeat(40)}\n`;

      // What a crash can leave: a closed file not yet compressed, and a partial archive.
      yield* fs.writeFileString(`${filePath}.19691231T235900Z`, line(9));
      yield* fs.writeFileString(`${filePath}.19691231T235800Z.gz.tmp`, "partial");
      const options = { filePath, maxBytes: 64, maxArchives: 2, maxArchiveBytes: 1024 * 1024 };
      const log = yield* SpanMinutes.makeSpanMinuteLog(options);
      assert.deepStrictEqual(yield* listing, ["server.span-minutes.ndjson.19691231T235900Z.gz"]);
      assert.strictEqual(yield* gunzip("server.span-minutes.ndjson.19691231T235900Z.gz"), line(9));

      for (let minute = 0; minute < 4; minute++) {
        yield* log.append(line(minute));
        yield* TestClock.adjust("1 minute");
      }
      assert.deepStrictEqual(yield* listing, [
        "server.span-minutes.ndjson",
        "server.span-minutes.ndjson.19700101T000200Z.gz",
        "server.span-minutes.ndjson.19700101T000300Z.gz",
      ]);
      assert.strictEqual(yield* gunzip("server.span-minutes.ndjson.19700101T000200Z.gz"), line(1));
      assert.strictEqual(yield* gunzip("server.span-minutes.ndjson.19700101T000300Z.gz"), line(2));
      assert.strictEqual(yield* fs.readFileString(filePath), line(3));

      const newest = yield* fs.stat(`${filePath}.19700101T000300Z.gz`);
      yield* SpanMinutes.makeSpanMinuteLog({
        ...options,
        maxArchives: 10,
        maxArchiveBytes: Number(newest.size),
      });
      assert.deepStrictEqual(yield* listing, [
        "server.span-minutes.ndjson",
        "server.span-minutes.ndjson.19700101T000300Z.gz",
      ]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("drops a torn last line at startup and after a failed append, and still archives", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-span-minutes-torn-" });
      const filePath = path.join(directory, SpanMinutes.SPAN_MINUTES_FILE_NAME);
      // A crash in the middle of an append.
      yield* fs.writeFileString(filePath, '{"minute":0}\n{"minu');
      let tearNextWrite = false;
      const tearing = FileSystem.FileSystem.of({
        ...fs,
        writeFile: (target, data, options) => {
          if (!tearNextWrite) return fs.writeFile(target, data, options);
          tearNextWrite = false;
          return fs.writeFile(target, data.subarray(0, 5), options).pipe(
            Effect.andThen(
              Effect.fail(
                PlatformError.systemError({
                  _tag: "WriteZero",
                  module: "FileSystem",
                  method: "writeFile",
                  pathOrDescriptor: target,
                }),
              ),
            ),
          );
        },
      });
      const log = yield* SpanMinutes.makeSpanMinuteLog({
        filePath,
        maxBytes: 1024 * 1024,
        maxArchives: 2,
        maxArchiveBytes: 1024 * 1024,
      }).pipe(Effect.provideService(FileSystem.FileSystem, tearing));

      yield* log.append('{"minute":1}\n');
      tearNextWrite = true;
      yield* Effect.flip(log.append('{"minute":2}\n'));
      yield* log.append('{"minute":3}\n');
      assert.strictEqual(
        yield* fs.readFileString(filePath),
        '{"minute":0}\n{"minute":1}\n{"minute":3}\n',
      );

      // An append that fails just after closing the file must not leave that file unarchived.
      const rotating = yield* SpanMinutes.makeSpanMinuteLog({
        filePath,
        maxBytes: 64,
        maxArchives: 1,
        maxArchiveBytes: 1024 * 1024,
      }).pipe(Effect.provideService(FileSystem.FileSystem, tearing));
      yield* TestClock.adjust("1 minute");
      tearNextWrite = true;
      yield* Effect.flip(rotating.append(`${"4".repeat(60)}\n`));
      yield* TestClock.adjust("1 minute");
      yield* rotating.append('{"minute":5}\n');
      assert.deepStrictEqual((yield* fs.readDirectory(directory)).toSorted(), [
        "server.span-minutes.ndjson",
        "server.span-minutes.ndjson.19700101T000100Z.gz",
      ]);
      assert.strictEqual(yield* fs.readFileString(filePath), '{"minute":5}\n');
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("writes span minutes beside the trace file unless the knob turns them off", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      for (const traceSpanMinutes of [undefined, false]) {
        const baseDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-span-minutes-knob-" });
        const config = yield* ServerConfig.ServerConfig.pipe(
          Effect.provide(ServerConfig.layerTest(baseDir, baseDir)),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              Observability.layer.pipe(
                Layer.provide(
                  Layer.succeed(ServerConfig.ServerConfig, {
                    ...config,
                    ...(traceSpanMinutes === undefined ? {} : { traceSpanMinutes }),
                  }),
                ),
                Layer.provide(ResourceAttribution.layer),
                Layer.provide(FetchHttpClient.layer),
              ),
            );
            yield* Effect.void.pipe(Effect.withSpan("Probe.run"), Effect.provideContext(context));
          }),
        );
        const written = yield* fs
          .readFileString(
            path.join(path.dirname(config.serverTracePath), SpanMinutes.SPAN_MINUTES_FILE_NAME),
          )
          .pipe(Effect.option);
        if (traceSpanMinutes === false) {
          assert.isTrue(Option.isNone(written));
        } else {
          const line = decodeLine(Option.getOrThrow(written)) as SpanMinutes.SpanMinuteLine;
          assert.strictEqual(line.spans["Probe.run"]?.[0], 1);
        }
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

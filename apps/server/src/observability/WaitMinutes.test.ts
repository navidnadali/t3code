import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import * as WaitMinutes from "./WaitMinutes.ts";

const MS = 1_000_000;

describe("WaitMinutes", () => {
  it.effect(
    "counts each site's waits with percentiles, top holders and the depth arrivals saw",
    () =>
      Effect.gen(function* () {
        const waits = WaitMinutes.makeWaitAggregate();
        const { observer } = waits;
        for (let index = 1; index <= 100; index++) {
          observer.wait("ThreadCommandExecutor.lock", index * MS);
        }
        // Holders rank by the wait they caused; past the top five the rest fold into one.
        const holders = ["A.one", "A.two", "A.three", "A.four", "A.five", "A.six", "A.seven"];
        holders.forEach((holder, index) => {
          for (let repeat = 0; repeat <= index; repeat++) {
            observer.wait("Orchestrator.messageIntake", (10 - index) * MS, holder);
          }
        });
        for (const depth of [0, 1, 1, 2, 3, 3, 4, 5, 6, 40]) {
          observer.depth("ProviderCommitBatch.queue", depth);
        }
        observer.wait("ProviderCommitBatch.queue", 2 * MS, "EventSink.commitCommand");

        const first = yield* waits.drain;
        const lock = first.waits["ThreadCommandExecutor.lock"]!;
        assert.deepStrictEqual([lock.n, lock.totalMs, lock.maxMs], [100, 5_050, 100]);
        // Nearest rank within the histogram's 1/16.
        for (const [actual, expected] of [
          [lock.p50Ms, 50],
          [lock.p90Ms, 90],
          [lock.p99Ms, 99],
        ] as const) {
          assert.closeTo(actual, expected, expected / 16);
        }
        assert.isUndefined(lock.holders);
        assert.isUndefined(lock.depth);
        const intake = first.waits["Orchestrator.messageIntake"]!;
        assert.strictEqual(intake.n, 28);
        assert.deepStrictEqual(intake.holders, {
          "A.three": [3, 24],
          "A.four": [4, 28],
          "A.five": [5, 30],
          "A.six": [6, 30],
          "A.seven": [7, 28],
          "(other)": [3, 28],
        });
        assert.deepStrictEqual(first.waits["ProviderCommitBatch.queue"], {
          n: 1,
          totalMs: 2,
          maxMs: 2,
          p50Ms: 2,
          p90Ms: 2,
          p99Ms: 2,
          holders: { "EventSink.commitCommand": [1, 2] },
          // p50, p90 and max of the lengths arrivals found.
          depth: [3, 6, 40],
        });

        // A site with nothing in a minute is left out; uncontended waits are exact zeros.
        for (const waitNs of [0, 0, 0, 0, 0, 0, 0, 0, 0, 3 * MS]) {
          observer.wait("ThreadCommandExecutor.lock", waitNs);
        }
        const second = yield* waits.drain;
        assert.deepStrictEqual(Object.keys(second.waits), ["ThreadCommandExecutor.lock"]);
        assert.deepStrictEqual(second.waits["ThreadCommandExecutor.lock"], {
          n: 10,
          totalMs: 3,
          maxMs: 3,
          p50Ms: 0,
          p90Ms: 0,
          p99Ms: 3,
        });
      }),
  );

  it.effect("ranks the top keys by wait and by hold with only the sizer's classes", () =>
    Effect.gen(function* () {
      const waits = WaitMinutes.makeWaitAggregate();
      const { observer } = waits;
      const sized: Array<ReadonlyArray<unknown>> = [];
      observer.sizeKeys("ThreadCommandExecutor.lock", (keys) =>
        Effect.sync(() => {
          sized.push(keys);
          return new Map(
            keys
              .filter((key) => key !== "thread-quiet")
              .map((key) => [key, { runs: 10_000, subagents: 1_000 }] as const),
          );
        }),
      );
      // thread-0 waits most and holds least; thread-6 the reverse.
      for (let index = 0; index < 7; index++) {
        const key = `thread-${index}`;
        observer.wait("ThreadCommandExecutor.lock", (7 - index) * MS, "Probe.holder", key);
        observer.wait("ThreadCommandExecutor.lock", 0, undefined, key);
        observer.hold("ThreadCommandExecutor.lock", key, (index + 1) * 10 * MS);
      }
      observer.hold("ThreadCommandExecutor.lock", "thread-quiet", 500 * MS);

      const minute = yield* waits.drain;
      const { byWait = [], byHold = [] } = minute.threads["ThreadCommandExecutor.lock"]!;
      assert.deepStrictEqual(
        byWait.map((rank) => [rank.waitMs, rank.holdMs, rank.n]),
        [
          [7, 10, 2],
          [6, 20, 2],
          [5, 30, 2],
          [4, 40, 2],
          [3, 50, 2],
        ],
      );
      assert.deepStrictEqual(byHold[0], { waitMs: 0, holdMs: 500, n: 0 });
      assert.deepStrictEqual(byHold[1], {
        waitMs: 1,
        holdMs: 70,
        n: 2,
        runs: 10_000,
        subagents: 1_000,
      });
      assert.strictEqual(byHold.length, 5);
      // The sizer saw each ranked key once; the line names none of them.
      assert.strictEqual(sized.length, 1);
      assert.strictEqual(new Set(sized[0]).size, sized[0]!.length);
      assert.notInclude(JSON.stringify(minute), "thread-");
      // Keys are forgotten with their minute.
      assert.deepStrictEqual((yield* waits.drain).threads, {});
    }),
  );

  it.effect("leaves the classes out when the sizer fails, and ranks a site without a sizer", () =>
    Effect.gen(function* () {
      const waits = WaitMinutes.makeWaitAggregate();
      const { observer } = waits;
      observer.sizeKeys("Orchestrator.messageIntake", () => Effect.die("database gone"));
      observer.wait("Orchestrator.messageIntake", 3 * MS, "Probe.holder", "thread-a");
      observer.wait("ThreadCommandExecutor.lock", 4 * MS, "Probe.holder", "thread-b");
      const minute = yield* waits.drain;
      assert.deepStrictEqual(minute.threads["Orchestrator.messageIntake"]!.byWait, [
        { waitMs: 3, holdMs: 0, n: 1 },
      ]);
      assert.deepStrictEqual(minute.threads["ThreadCommandExecutor.lock"]!.byWait, [
        { waitMs: 4, holdMs: 0, n: 1 },
      ]);
    }),
  );

  it.effect("counts each site's waiters and outcomes like its holders", () =>
    Effect.gen(function* () {
      const waits = WaitMinutes.makeWaitAggregate();
      const { observer } = waits;
      observer.wait(
        "McpToolAccess.wakeRunWait",
        2 * MS,
        undefined,
        undefined,
        undefined,
        "became-active",
      );
      observer.wait(
        "McpToolAccess.wakeRunWait",
        60_000 * MS,
        undefined,
        undefined,
        undefined,
        "timed-out",
      );
      observer.wait(
        "McpToolAccess.wakeRunWait",
        4 * MS,
        undefined,
        undefined,
        undefined,
        "became-active",
      );
      observer.wait("ThreadCommandExecutor.lock", 9 * MS, "Holder.run", "thread-a", "Waiter.run");
      const minute = yield* waits.drain;
      assert.deepStrictEqual(minute.waits["McpToolAccess.wakeRunWait"]!.outcomes, {
        "became-active": [2, 6],
        "timed-out": [1, 60_000],
      });
      assert.isUndefined(minute.waits["McpToolAccess.wakeRunWait"]!.waiters);
      assert.deepStrictEqual(minute.waits["ThreadCommandExecutor.lock"]!.waiters, {
        "Waiter.run": [1, 9],
      });
    }),
  );

  it.effect("reads a gauge as it stands and a counter as its increase each minute", () =>
    Effect.gen(function* () {
      const waits = WaitMinutes.makeWaitAggregate();
      let depth = 3;
      // A counter counts from its registration.
      let closedMs = 1_000;
      waits.observer.register("ProviderCommitBatch.depth", "gauge", () => depth);
      const unregister = waits.observer.register(
        "EffectAdmission.closedMs",
        "counter",
        () => closedMs,
      );
      closedMs = 31_000;
      assert.deepStrictEqual((yield* waits.drain).gauges, {
        "ProviderCommitBatch.depth": 3,
        "EffectAdmission.closedMs": 30_000,
      });
      depth = 0;
      closedMs = 91_000;
      assert.deepStrictEqual((yield* waits.drain).gauges, {
        "ProviderCommitBatch.depth": 0,
        "EffectAdmission.closedMs": 60_000,
      });
      unregister();
      assert.deepStrictEqual((yield* waits.drain).gauges, { "ProviderCommitBatch.depth": 0 });
    }),
  );

  it.effect("sums keyed gauges and ranks their keys by value with the sizer's classes", () =>
    Effect.gen(function* () {
      const waits = WaitMinutes.makeWaitAggregate();
      const { observer } = waits;
      observer.sizeKeys("ProviderIngest.backlog", (keys) =>
        Effect.succeed(new Map(keys.map((key) => [key, { runs: 100 }] as const))),
      );
      const backlogs = [0, 3, 9, 1, 4, 7, 2];
      const unregister = backlogs.map((_, index) =>
        observer.registerKeyed("ProviderIngest.backlog", `thread-${index}`, () => backlogs[index]!),
      );
      // Two subscriptions of one thread count together.
      observer.registerKeyed("ProviderIngest.backlog", "thread-1", () => 5);
      const minute = yield* waits.drain;
      assert.strictEqual(minute.gauges["ProviderIngest.backlog"], 31);
      assert.deepStrictEqual(minute.threads["ProviderIngest.backlog"], {
        byDepth: [9, 8, 7, 4, 2].map((depth) => ({ depth, runs: 100 })),
      });
      assert.notInclude(JSON.stringify(minute), "thread-");

      for (const undo of unregister) undo();
      backlogs.fill(0);
      const idle = yield* waits.drain;
      assert.strictEqual(idle.gauges["ProviderIngest.backlog"], 5);
    }),
  );

  it.effect("bounds the ranked sites, gauges and keyed gauges as it bounds waits", () =>
    Effect.gen(function* () {
      const waits = WaitMinutes.makeWaitAggregate();
      const { observer } = waits;
      const long = "x".repeat(500);
      for (let index = 0; index < 1_000; index++) {
        observer.wait(`Keyed.site-${index}`, MS, "Probe.holder", "thread-a");
        observer.register(`Gauge.${index}${long}`, "gauge", () => 1);
        observer.registerKeyed(`Backlog.site-${index}`, "thread-a", () => 1);
      }
      for (let index = 0; index < 20_000; index++) {
        observer.registerKeyed("Backlog.site-0", `thread-${index}`, () => 1);
      }
      const minute = yield* waits.drain;
      assert.isAtMost(Object.keys(minute.threads).length, WaitMinutes.MAX_RANKED_SITES);
      // Each ranked keyed gauge also writes its sum as a gauge.
      assert.isAtMost(
        Object.keys(minute.gauges).length,
        WaitMinutes.MAX_GAUGES + WaitMinutes.MAX_RANKED_SITES,
      );
      for (const name of [...Object.keys(minute.gauges), ...Object.keys(minute.threads)]) {
        assert.isAtMost(name.length, 200);
      }
      assert.isAtMost(minute.gauges["Backlog.site-0"]!, 10_001);
    }),
  );

  it.effect("folds sites past the cap into one", () =>
    Effect.gen(function* () {
      const waits = WaitMinutes.makeWaitAggregate();
      for (let index = 0; index < WaitMinutes.MAX_WAIT_SITES + 3; index++) {
        waits.observer.wait(`EffectOutbox.type-${index}`, MS);
      }
      const minute = yield* waits.drain;
      assert.strictEqual(Object.keys(minute.waits).length, WaitMinutes.MAX_WAIT_SITES + 1);
      assert.strictEqual(minute.waits[WaitMinutes.OTHER_SITE]!.n, 3);
    }),
  );
});

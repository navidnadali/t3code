import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  makeProviderIoReadCounter,
  makeProviderIoStageRecorder,
  providerIoStageRecorderFromEnv,
  timeProviderIoLogShape,
  timeProviderIoStage,
} from "./ProviderIoStageTiming.ts";

const makeClock = () => {
  let ticks = 0;
  return {
    now: () => ticks,
    advance: (by: number) => {
      ticks += by;
    },
  };
};

describe("ProviderIoStageTiming", () => {
  it("is off unless the knob is exactly 1", () => {
    assert.isUndefined(providerIoStageRecorderFromEnv({}));
    assert.isUndefined(providerIoStageRecorderFromEnv({ T3CODE_PROVIDER_IO_STAGE_TIMING: "0" }));
    assert.isUndefined(providerIoStageRecorderFromEnv({ T3CODE_PROVIDER_IO_STAGE_TIMING: "true" }));
    assert.isDefined(providerIoStageRecorderFromEnv({ T3CODE_PROVIDER_IO_STAGE_TIMING: "1" }));
  });

  it("totals count, time, max and bytes per provider and stage until drained", () => {
    const recorder = makeProviderIoStageRecorder(makeClock().now);
    const codex = recorder.forProvider("codex");
    assert.strictEqual(recorder.forProvider("codex"), codex);
    codex.record("framing", 2, 100);
    codex.record("framing", 5, 40);
    codex.record("json", 1, 90);
    recorder.forProvider("claudeAgent").record("sdkMessage", 0, 300);

    assert.deepStrictEqual(recorder.drain(), [
      { provider: "codex", stage: "framing", count: 2, totalMs: 7, maxMs: 5, bytes: 140 },
      { provider: "codex", stage: "json", count: 1, totalMs: 1, maxMs: 1, bytes: 90 },
      { provider: "claudeAgent", stage: "sdkMessage", count: 1, totalMs: 0, maxMs: 0, bytes: 300 },
    ]);
    assert.deepStrictEqual(recorder.drain(), []);

    codex.record("json", 3, 10);
    assert.deepStrictEqual(recorder.drain(), [
      { provider: "codex", stage: "json", count: 1, totalMs: 3, maxMs: 3, bytes: 10 },
    ]);
  });

  it.effect("times building and running an effect, and leaves it untouched when off", () =>
    Effect.gen(function* () {
      const clock = makeClock();
      const recorder = makeProviderIoStageRecorder(clock.now);
      const timer = recorder.forProvider("acp");
      const work = Effect.sync(() => clock.advance(4));
      // Like a schema decoder, which validates when called rather than when run.
      const eagerWork = () => {
        clock.advance(2);
        return work;
      };

      assert.strictEqual(
        timeProviderIoStage(undefined, "decode", 10, () => work),
        work,
      );
      // Built where it is called, as when timing is off, so a throwing constructor fails the same way.
      const timed = timeProviderIoStage(timer, "decode", 10, eagerWork);
      assert.strictEqual(clock.now(), 2);
      yield* timed;
      yield* timeProviderIoStage(timer, "decode", 6, () =>
        work.pipe(Effect.andThen(Effect.fail("bad payload"))),
      ).pipe(Effect.ignore);

      assert.deepStrictEqual(recorder.drain(), [
        { provider: "acp", stage: "decode", count: 2, totalMs: 10, maxMs: 6, bytes: 16 },
      ]);
    }),
  );

  it("times log shaping of incoming events only, without changing the logger when off", () => {
    const clock = makeClock();
    const recorder = makeProviderIoStageRecorder(clock.now);
    const logger = (event: {
      readonly direction: "incoming" | "outgoing";
      readonly cost: number;
    }) => {
      clock.advance(event.cost);
      return `write ${event.cost}`;
    };

    assert.strictEqual(timeProviderIoLogShape(undefined, logger), logger);
    const timed = timeProviderIoLogShape(recorder.forProvider("codex"), logger);
    assert.strictEqual(timed({ direction: "incoming", cost: 2 }), "write 2");
    assert.strictEqual(timed({ direction: "outgoing", cost: 7 }), "write 7");
    assert.strictEqual(timed({ direction: "incoming", cost: 3 }), "write 3");
    assert.deepStrictEqual(recorder.drain(), [
      { provider: "codex", stage: "logShape", count: 2, totalMs: 5, maxMs: 3, bytes: 0 },
    ]);
  });

  it("counts read bytes since the previous message", () => {
    const recorder = makeProviderIoStageRecorder(makeClock().now);
    let bytesRead = 0;
    const count = makeProviderIoReadCounter(
      recorder.forProvider("claudeAgent"),
      "sdkMessage",
      () => bytesRead,
    );
    bytesRead = 120;
    count();
    count();
    bytesRead = 200;
    count();

    assert.deepStrictEqual(recorder.drain(), [
      { provider: "claudeAgent", stage: "sdkMessage", count: 3, totalMs: 0, maxMs: 0, bytes: 200 },
    ]);
  });
});

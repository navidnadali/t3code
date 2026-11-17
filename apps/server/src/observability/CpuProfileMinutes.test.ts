import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import * as CpuProfileMinutes from "./CpuProfileMinutes.ts";

const node = (
  id: number,
  functionName: string,
  url: string,
  lineNumber: number,
  hitCount: number,
): CpuProfileMinutes.ProfileNode => ({
  id,
  callFrame: { functionName, url, lineNumber },
  hitCount,
});

describe("CpuProfileMinutes", () => {
  it("adds self time by function and file name, without the path, keeping the top few", () => {
    // 1,000 samples over 10 s: 10 ms each.
    const profile: CpuProfileMinutes.Profile = {
      startTime: 0,
      endTime: 10_000_000,
      samples: Array.from({ length: 1_000 }, () => 1),
      nodes: [
        node(1, "(root)", "", -1, 0),
        node(2, "(program)", "", -1, 100),
        node(3, "decodeProjection", "file:///srv/app/t3/dist/server.mjs", 41, 500),
        // Two nodes of one function on different stacks count together.
        node(4, "decodeProjection", "file:///srv/app/t3/dist/server.mjs", 41, 150),
        node(5, "", "file:///srv/app/t3/dist/server.mjs", 99, 200),
        node(6, "(garbage collector)", "", -1, 30),
        node(7, "small", "node:internal/streams/readable", 7, 20),
      ],
    };
    const minute = CpuProfileMinutes.aggregateProfile(profile, 3);
    assert.deepStrictEqual(minute, {
      samples: 1_000,
      sampledMs: 10_000,
      top: [
        ["decodeProjection server.mjs:42", 6_500],
        ["(anonymous) server.mjs:100", 2_000],
        ["(program)", 1_000],
      ],
      otherMs: 500,
    });
    assert.notInclude(JSON.stringify(minute), "/srv/app");
  });

  it("writes an empty profile for a minute with no samples", () => {
    assert.deepStrictEqual(
      CpuProfileMinutes.aggregateProfile(
        { startTime: 0, endTime: 2_000, samples: [], nodes: [node(1, "(root)", "", -1, 0)] },
        3,
      ),
      { samples: 0, sampledMs: 2, top: [], otherMs: 0 },
    );
  });

  it.effect(
    "samples the process each minute while the next minute's profile is already running",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const drain = yield* CpuProfileMinutes.makeSection({ intervalMs: 1 });
          let checksum = 0;
          for (let index = 0; index < 2_000_000; index++)
            checksum = (checksum + index * 7) % 1_000_003;
          const first = yield* drain;
          const second = yield* drain;
          assert.isAbove(checksum, -1);
          // A build without an inspector writes no profile; this one has one.
          assert.isTrue(process.features.inspector);
          for (const minute of [first, second]) {
            assert.isDefined(minute.cpuProfile);
            assert.isAtMost(minute.cpuProfile!.top.length, CpuProfileMinutes.TOP_FUNCTIONS);
          }
        }),
      ),
  );
});

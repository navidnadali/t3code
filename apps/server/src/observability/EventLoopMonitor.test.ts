import { assert, describe, it } from "@effect/vitest";

import * as EventLoopMonitor from "./EventLoopMonitor.ts";

const ms = (value: number) => value * 1e6;

// Node reports a stall of S as a gap of up to S + 1 s, the histogram resolution.
const stalled: EventLoopMonitor.EventLoopReadings = {
  delayMaxNs: ms(5_950),
  activeMs: 6_200,
  utilization: 0.176,
  usage: {
    userCPUTime: 310_400,
    systemCPUTime: 95_600,
    majorPageFault: 8_412,
    minorPageFault: 20_031,
    involuntaryContextSwitches: 57,
  },
  rssBytes: 1536 * 1024 * 1024,
  heapUsedBytes: 512 * 1024 * 1024,
  elapsedMs: 30_000,
  loadAverage: 219.43,
  cpuCount: 18,
  mainThreadSchedStat: { available: true, cpuMs: 412.6, runQueueWaitMs: 4870.4 },
};

// What the stall record held before the main thread's scheduler numbers were added.
const earlierFields = {
  delayMaxMs: 4_950,
  utilization: 0.18,
  cpuUserMs: 310,
  cpuSystemMs: 96,
  majorPageFaults: 8_412,
  minorPageFaults: 20_031,
  involuntaryContextSwitches: 57,
  rssMb: 1536,
};

describe("EventLoopMonitor", () => {
  it("ignores delay the loop spent idle, such as a system sleep", () => {
    const asleep: EventLoopMonitor.EventLoopReadings = {
      ...stalled,
      delayMaxNs: ms(600_000),
      activeMs: 900,
    };
    assert.isUndefined(EventLoopMonitor.stallMs(asleep));
    assert.strictEqual(EventLoopMonitor.stallMs({ ...asleep, activeMs: 600_000 }), 599_000);
  });

  it("records the main thread's run-queue wait and CPU over the stall's window", () => {
    // Waiting for a CPU, not busy: host starvation.
    assert.deepStrictEqual(EventLoopMonitor.stallRecord(stalled, 4_950), {
      ...earlierFields,
      mainThreadRunQueueWaitMs: 4_870,
      mainThreadCpuMs: 413,
    });
  });

  it("says why the main thread's scheduler numbers are missing, and keeps every earlier field", () => {
    for (const reason of ["platform", "unreadable", "malformed"] as const) {
      assert.deepStrictEqual(
        EventLoopMonitor.stallRecord(
          { ...stalled, mainThreadSchedStat: { available: false, reason } },
          4_950,
        ),
        { ...earlierFields, mainThreadSchedStatUnavailable: reason },
      );
    }
  });
});

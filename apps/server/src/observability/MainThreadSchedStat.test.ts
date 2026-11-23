// @effect-diagnostics nodeBuiltinImport:off - the real read uses the file system as the server does.
import * as NodeFS from "node:fs";

import { assert, describe, it } from "@effect/vitest";

import { makeMainThreadSchedStatSampler } from "./MainThreadSchedStat.ts";

// Linux keeps per-thread scheduler numbers here; other hosts have no such file.
const schedStatPath = `/proc/self/task/${process.pid}/schedstat`;
const hasSchedStat = NodeFS.existsSync(schedStatPath);

/** A reader that answers each read with the next line, or throws what the step says. */
const reads = (...steps: ReadonlyArray<string | Error>) => {
  const paths: Array<string> = [];
  let index = 0;
  return {
    paths,
    readFile: (path: string) => {
      paths.push(path);
      const step = steps[Math.min(index++, steps.length - 1)]!;
      if (step instanceof Error) throw step;
      return step;
    },
  };
};

describe("MainThreadSchedStat", () => {
  it("reports the window's run-queue wait and CPU, not the totals since the thread started", () => {
    const reader = reads("1000000000 500000000 10\n", "1800000000 2300000000 15\n");
    const sample = makeMainThreadSchedStatSampler({ platform: "linux", pid: 4242, ...reader });
    assert.deepStrictEqual(sample(), { available: true, cpuMs: 800, runQueueWaitMs: 1800 });
    // The main thread's own numbers: on Linux its thread id is the process id.
    assert.deepStrictEqual(reader.paths, [
      "/proc/self/task/4242/schedstat",
      "/proc/self/task/4242/schedstat",
    ]);
  });

  it("is unavailable off Linux, without reading anything", () => {
    for (const platform of ["darwin", "win32"] as const) {
      const reader = reads("1 2 3\n");
      const sample = makeMainThreadSchedStatSampler({ platform, pid: 1, ...reader });
      assert.deepStrictEqual(sample(), { available: false, reason: "platform" });
      assert.deepStrictEqual(reader.paths, []);
    }
  });

  it("is unavailable when the file cannot be read, until a window has a start again", () => {
    const missing = Object.assign(new Error("ENOENT: no such file"), { code: "ENOENT" });
    const reader = reads("100 200 1\n", missing, "300 500 2\n", "1300 2500 3\n");
    const sample = makeMainThreadSchedStatSampler({ platform: "linux", pid: 7, ...reader });
    assert.deepStrictEqual(sample(), { available: false, reason: "unreadable" });
    // That read starts the next window; this window had no start.
    assert.deepStrictEqual(sample(), { available: false, reason: "unreadable" });
    assert.deepStrictEqual(sample(), { available: true, cpuMs: 0.001, runQueueWaitMs: 0.002 });
    // A window says why its start is missing, even when its end fails another way.
    const twice = reads("1 2\n", missing);
    const failing = makeMainThreadSchedStatSampler({ platform: "linux", pid: 7, ...twice });
    assert.deepStrictEqual(failing(), { available: false, reason: "malformed" });
  });

  it("is malformed, never an exception or a negative wait, for a line it cannot trust", () => {
    for (const line of ["1 2\n", "a b c\n", "1 2 3 4\n", "-1 2 3\n", "\n", "1.5 2 3\n"]) {
      const reader = reads("10 20 1\n", line);
      const sample = makeMainThreadSchedStatSampler({ platform: "linux", pid: 7, ...reader });
      assert.deepStrictEqual(sample(), { available: false, reason: "malformed" }, line);
    }
    // A counter that goes backwards is no wait at all, the timeslice count included.
    for (const later of ["4000000 9500000 5\n", "6000000 9500000 3\n"]) {
      const backwards = reads("5000000 9000000 4\n", later);
      const sample = makeMainThreadSchedStatSampler({ platform: "linux", pid: 7, ...backwards });
      assert.deepStrictEqual(sample(), { available: false, reason: "malformed" }, later);
    }
    // A thrown non-Error is still only unreadable.
    const odd = makeMainThreadSchedStatSampler({
      platform: "linux",
      pid: 7,
      readFile: () => {
        throw "boom";
      },
    });
    assert.deepStrictEqual(odd(), { available: false, reason: "unreadable" });
  });

  it("keeps counters past 2^53 exact", () => {
    const reader = reads(
      "9007199254740993000 9007199254740993000 1\n",
      "9007199254741993000 9007199254742993000 2\n",
    );
    const sample = makeMainThreadSchedStatSampler({ platform: "linux", pid: 7, ...reader });
    assert.deepStrictEqual(sample(), { available: true, cpuMs: 1, runQueueWaitMs: 2 });
  });

  it.skipIf(!hasSchedStat)("reads this process's main thread on Linux", () => {
    const sample = makeMainThreadSchedStatSampler({
      platform: "linux",
      pid: process.pid,
      readFile: (path) => NodeFS.readFileSync(path, "utf8"),
    });
    let spin = 0;
    for (let index = 0; index < 1_000_000; index++) spin += index;
    const result = sample();
    assert.isTrue(spin > 0);
    assert.strictEqual(result.available, true);
    if (result.available) {
      assert.isTrue(Number.isFinite(result.cpuMs) && result.cpuMs >= 0);
      assert.isTrue(Number.isFinite(result.runQueueWaitMs) && result.runQueueWaitMs >= 0);
    }
  });
});

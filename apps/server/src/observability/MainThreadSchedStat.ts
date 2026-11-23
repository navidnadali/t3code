/** Why the main thread's scheduler numbers are missing from a sample. */
export type MainThreadSchedStatUnavailable = "platform" | "unreadable" | "malformed";

/** The main thread's time on a CPU and waiting on a run queue over one sample window, in ms. */
export type MainThreadSchedStat =
  | { readonly available: true; readonly cpuMs: number; readonly runQueueWaitMs: number }
  | { readonly available: false; readonly reason: MainThreadSchedStatUnavailable };

export interface MainThreadSchedStatOptions {
  readonly platform: NodeJS.Platform;
  /** On Linux the main thread's id is the process id. */
  readonly pid: number;
  readonly readFile: (path: string) => string;
}

interface Counters {
  readonly cpuNs: bigint;
  readonly runQueueWaitNs: bigint;
  readonly timeslices: bigint;
}

// `/proc/<pid>/task/<tid>/schedstat`: time on a CPU (ns), time waiting on a run queue (ns), timeslices.
const SCHEDSTAT_LINE = /^(\d+) (\d+) (\d+)\n?$/;

const parse = (text: string): Counters | undefined => {
  const match = SCHEDSTAT_LINE.exec(text);
  return match === null
    ? undefined
    : {
        cpuNs: BigInt(match[1]!),
        runQueueWaitNs: BigInt(match[2]!),
        timeslices: BigInt(match[3]!),
      };
};

const nsToMs = (ns: bigint) => Number(ns) / 1e6;

/**
 * Samples the main thread's scheduler numbers once per call, as deltas since the last call (the
 * first since the sampler was made), so they cover the same window as the event-loop sample. Never
 * throws and never logs: a missing reading is `available: false` with why. Linux only; elsewhere
 * nothing is read.
 */
export const makeMainThreadSchedStatSampler = (options: MainThreadSchedStatOptions) => {
  const path = `/proc/self/task/${options.pid}/schedstat`;
  const read = ():
    | { readonly counters: Counters }
    | { readonly reason: MainThreadSchedStatUnavailable } => {
    let text: string;
    try {
      text = options.readFile(path);
    } catch {
      return { reason: "unreadable" };
    }
    const counters = parse(text);
    return counters === undefined ? { reason: "malformed" } : { counters };
  };
  if (options.platform !== "linux") {
    return (): MainThreadSchedStat => ({ available: false, reason: "platform" });
  }
  const first = read();
  let previous: Counters | undefined = "counters" in first ? first.counters : undefined;
  // Why the current window has no start, when the read that would have started it failed.
  let missingStart: MainThreadSchedStatUnavailable =
    "reason" in first ? first.reason : "unreadable";
  return (): MainThreadSchedStat => {
    const next = read();
    if ("reason" in next) {
      // A window with no start says why its start is missing; its end starts nothing either.
      const reason = previous === undefined ? missingStart : next.reason;
      previous = undefined;
      missingStart = next.reason;
      return { available: false, reason };
    }
    const start = previous;
    previous = next.counters;
    if (start === undefined) return { available: false, reason: missingStart };
    const cpuNs = next.counters.cpuNs - start.cpuNs;
    const runQueueWaitNs = next.counters.runQueueWaitNs - start.runQueueWaitNs;
    // Counters only grow; one that went backwards is no reading to trust.
    if (cpuNs < 0n || runQueueWaitNs < 0n || next.counters.timeslices < start.timeslices)
      return { available: false, reason: "malformed" };
    return { available: true, cpuMs: nsToMs(cpuNs), runQueueWaitMs: nsToMs(runQueueWaitNs) };
  };
};

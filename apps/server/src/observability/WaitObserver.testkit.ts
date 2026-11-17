import type { KeySizer, WaitObserver } from "@t3tools/shared/WaitObserver";

/** Records every report, and keeps each registered reader and sizer so a test can call it. */
export const recordingWaitObserver = () => {
  const calls: Array<ReadonlyArray<unknown>> = [];
  const readers = new Map<string, () => number>();
  const sizers = new Map<string, KeySizer>();
  const keyedReaders: Array<{
    readonly site: string;
    readonly key: unknown;
    readonly read: () => number;
  }> = [];
  const observer: WaitObserver = {
    wait: (...args) => void calls.push(["wait", ...args]),
    hold: (...args) => void calls.push(["hold", ...args]),
    depth: (...args) => void calls.push(["depth", ...args]),
    register: (name, _kind, read) => {
      readers.set(name, read);
      return () => readers.delete(name);
    },
    registerKeyed: (site, key, read) => {
      const entry = { site, key, read };
      keyedReaders.push(entry);
      return () => {
        const index = keyedReaders.indexOf(entry);
        if (index !== -1) keyedReaders.splice(index, 1);
      };
    },
    sizeKeys: (site, sizer) => {
      sizers.set(site, sizer);
      return () => sizers.delete(site);
    },
  };
  const reports = (kind: "wait" | "hold" | "depth") =>
    calls.filter(([recorded]) => recorded === kind).map(([, ...args]) => args);
  return { calls, readers, keyedReaders, sizers, observer, reports };
};

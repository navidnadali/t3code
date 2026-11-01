import * as NodeWorkerThreads from "node:worker_threads";

import type { ProviderIoStage } from "../providerIoProtocol.ts";

export const linesStage: ProviderIoStage<void, string> = {
  name: "test-lines",
  open: () => {
    const decoder = new TextDecoder();
    let rest = "";
    return {
      push: (chunk, emit) => {
        const lines = (rest + decoder.decode(chunk, { stream: true })).split("\n");
        rest = lines.pop() ?? "";
        for (const line of lines) emit(line);
      },
      end: (emit) => {
        const tail = rest + decoder.decode();
        if (tail !== "") emit(tail);
      },
    };
  },
};

// Echoes each chunk; the words below make the worker misbehave on cue.
export const echoStage: ProviderIoStage<{ readonly tag: string }, string> = {
  name: "test-echo",
  open: ({ tag }) => {
    const decoder = new TextDecoder();
    return {
      push: (chunk, emit) => {
        const text = decoder.decode(chunk);
        if (text === "exit") process.exit(3);
        if (text === "throw") {
          emit(`${tag}:before-throw`);
          throw new Error("stage threw");
        }
        if (text === "unknown") NodeWorkerThreads.parentPort?.postMessage({ type: "bogus" });
        if (text === "malformed") {
          NodeWorkerThreads.parentPort?.postMessage({ type: "output", session: "x", outputs: 1 });
        }
        emit(`${tag}:${text}`);
      },
      end: (emit) => emit(`${tag}:end`),
    };
  },
};

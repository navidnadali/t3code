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
export const echoStage: ProviderIoStage<{ readonly tag: string }, string, string, string> = {
  name: "test-echo",
  open: ({ tag }) => {
    const decoder = new TextDecoder();
    let chunks = 0;
    return {
      call: (request) => {
        if (request === "throw") throw new Error("call threw");
        return `${tag}:${request}:${chunks}`;
      },
      push: (chunk, emit) => {
        chunks += 1;
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

let closedAsyncSessions = 0;

// Emits, settles and ends outside push, as a stage that runs a library does.
export const asyncStage: ProviderIoStage<void, string, string, string, string> = {
  name: "test-async",
  open: (_options, host) => {
    const decoder = new TextDecoder();
    return {
      push: (chunk, emit) => {
        const text = decoder.decode(chunk);
        if (text === "emit-then-signal") {
          emit("emitted-in-push");
          host.signal("signalled-in-push");
          return;
        }
        queueMicrotask(() => host.emit(`late:${text}`));
      },
      end: () => {},
      call: (request) => {
        switch (request) {
          case "signal":
            host.signal("event");
            return "signalled";
          case "emit-then-settle":
            return Promise.resolve().then(() => {
              queueMicrotask(() => host.emit("emitted-while-settling"));
              return "settled";
            });
          case "reject":
            return Promise.reject(new Error("rejected"));
          case "finish":
            host.finish();
            return "finished";
          case "fail":
            host.finish(new Error("library failed"));
            return "failed";
          case "closed":
            return String(closedAsyncSessions);
          default:
            return request;
        }
      },
      close: () => {
        closedAsyncSessions += 1;
      },
    };
  },
};

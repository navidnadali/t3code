import * as NodeWorkerThreads from "node:worker_threads";

import type {
  ProviderIoMainMessage,
  ProviderIoStage,
  ProviderIoStageSession,
  ProviderIoWorkerMessage,
} from "./providerIoProtocol.ts";

interface WorkerSession {
  readonly stage: ProviderIoStageSession<unknown>;
  readonly emit: (output: unknown) => void;
  outputs: Array<unknown>;
  bytes: number;
}

const openSession = (stage: ProviderIoStage<never, unknown>, options: unknown): WorkerSession => {
  const buffer = { outputs: new Array<unknown>(), bytes: 0 };
  const emit = (output: unknown) => {
    buffer.outputs.push(output);
  };
  return Object.assign(buffer, { stage: stage.open(options as never), emit });
};

// Runs inside the worker bundle, which may require only Node built-ins.
export const runProviderIoWorker = (stages: ReadonlyArray<ProviderIoStage<never, unknown>>) => {
  const port = NodeWorkerThreads.parentPort;
  if (port === null) throw new Error("The provider I/O worker must run in a worker thread.");
  const registry = new Map(stages.map((stage) => [stage.name, stage]));
  const sessions = new Map<number, WorkerSession>();
  const unflushed = new Set<number>();
  let flushScheduled = false;

  const post = (message: ProviderIoWorkerMessage) => port.postMessage(message);

  const fail = (id: number, error: unknown) => {
    sessions.delete(id);
    unflushed.delete(id);
    try {
      post({ type: "fail", session: id, error });
    } catch {
      post({ type: "fail", session: id, error: new Error(String(error)) });
    }
  };

  // An output that cannot be cloned fails only its own session.
  const flush = (id: number, session: WorkerSession) => {
    unflushed.delete(id);
    if (session.outputs.length === 0 && session.bytes === 0) return true;
    const { outputs, bytes } = session;
    session.outputs = [];
    session.bytes = 0;
    try {
      post({ type: "output", session: id, outputs, bytes });
      return true;
    } catch (error) {
      fail(id, error);
      return false;
    }
  };

  const flushAll = () => {
    flushScheduled = false;
    for (const id of unflushed) {
      const session = sessions.get(id);
      if (session !== undefined) flush(id, session);
    }
    unflushed.clear();
  };

  // Outputs emitted before a stage throws still reach main ahead of the failure.
  const run = (id: number, session: WorkerSession, step: () => void) => {
    try {
      step();
      return true;
    } catch (error) {
      if (flush(id, session)) fail(id, error);
      return false;
    }
  };

  port.on("message", (message: ProviderIoMainMessage) => {
    const id = message.session;
    if (message.type === "open") {
      const stage = registry.get(message.stage);
      if (stage === undefined) {
        fail(id, new Error(`Unknown provider I/O stage "${message.stage}".`));
        return;
      }
      try {
        sessions.set(id, openSession(stage, message.options));
      } catch (error) {
        fail(id, error);
      }
      return;
    }
    const session = sessions.get(id);
    if (session === undefined) return;
    switch (message.type) {
      case "bytes":
        session.bytes += message.chunk.byteLength;
        if (!run(id, session, () => session.stage.push(message.chunk, session.emit))) return;
        unflushed.add(id);
        if (!flushScheduled) {
          flushScheduled = true;
          setImmediate(flushAll);
        }
        return;
      case "eof":
        if (!run(id, session, () => session.stage.end(session.emit))) return;
        if (!flush(id, session)) return;
        sessions.delete(id);
        post({ type: "end", session: id });
        return;
      case "close":
        sessions.delete(id);
        unflushed.delete(id);
        return;
    }
  });

  post({ type: "ready" });
};

import * as NodeWorkerThreads from "node:worker_threads";

import type {
  ProviderIoMainMessage,
  ProviderIoStage,
  ProviderIoStageSession,
  ProviderIoWorkerMessage,
} from "./providerIoProtocol.ts";

type AnyStage = ProviderIoStage<never, unknown, never, unknown>;

interface WorkerSession {
  readonly stage: ProviderIoStageSession<unknown, never, unknown>;
  readonly emit: (output: unknown) => void;
  outputs: Array<unknown>;
  // Credit of chunks that emitted outputs (returned when main takes them) and of silent ones.
  held: number;
  freed: number;
}

const openSession = (stage: AnyStage, options: unknown): WorkerSession => {
  const buffer = { outputs: new Array<unknown>(), held: 0, freed: 0 };
  const emit = (output: unknown) => {
    buffer.outputs.push(output);
  };
  return Object.assign(buffer, { stage: stage.open(options as never), emit });
};

// Runs inside the worker bundle, which may require only Node built-ins.
export const runProviderIoWorker = (stages: ReadonlyArray<AnyStage>) => {
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
    const { outputs, held, freed } = session;
    session.outputs = [];
    session.held = 0;
    session.freed = 0;
    try {
      if (freed > 0) post({ type: "output", session: id, outputs: [], bytes: freed });
      if (outputs.length > 0) post({ type: "output", session: id, outputs, bytes: held });
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

  const reply = (id: number, call: number, ok: boolean, value: unknown) => {
    try {
      post({ type: "reply", session: id, call, ok, value });
    } catch (error) {
      post({ type: "reply", session: id, call, ok: false, value: new Error(String(error)) });
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
    if (session === undefined) {
      // Main may still be waiting on a call to a session the worker has ended or failed.
      if (message.type === "call") {
        reply(id, message.call, false, new Error("The provider I/O session has ended."));
      }
      return;
    }
    switch (message.type) {
      case "bytes": {
        const emitted = session.outputs.length;
        if (!run(id, session, () => session.stage.push(message.chunk, session.emit))) return;
        if (session.outputs.length > emitted) session.held += message.chunk.byteLength;
        else session.freed += message.chunk.byteLength;
        unflushed.add(id);
        if (!flushScheduled) {
          flushScheduled = true;
          setImmediate(flushAll);
        }
        return;
      }
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
      case "call": {
        const call = session.stage.call;
        if (call === undefined) {
          reply(id, message.call, false, new Error("This provider I/O stage takes no calls."));
          return;
        }
        let value: unknown;
        try {
          value = call(message.request as never);
        } catch (error) {
          reply(id, message.call, false, error);
          return;
        }
        reply(id, message.call, true, value);
        return;
      }
    }
  });

  post({ type: "ready" });
};

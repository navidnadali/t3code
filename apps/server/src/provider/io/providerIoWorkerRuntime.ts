import * as NodeWorkerThreads from "node:worker_threads";

import type {
  ProviderIoMainMessage,
  ProviderIoStage,
  ProviderIoStageHost,
  ProviderIoStageSession,
  ProviderIoWorkerMessage,
} from "./providerIoProtocol.ts";

type AnyStage = ProviderIoStage<never, unknown, never, unknown, unknown>;

interface WorkerSession {
  stage: ProviderIoStageSession<unknown, never, unknown>;
  readonly emit: (output: unknown) => void;
  outputs: Array<unknown>;
  // Credit of chunks that emitted outputs (returned when main takes them) and of silent ones.
  held: number;
  freed: number;
  finished: boolean;
  // Async replies, posted after the outputs emitted while they were pending.
  replies: Array<ProviderIoWorkerMessage>;
}

const isThenable = (value: unknown): value is PromiseLike<unknown> =>
  typeof value === "object" &&
  value !== null &&
  typeof (value as { readonly then?: unknown }).then === "function";

// Runs inside the worker bundle, which may require only Node built-ins.
export const runProviderIoWorker = (stages: ReadonlyArray<AnyStage>) => {
  const port = NodeWorkerThreads.parentPort;
  if (port === null) throw new Error("The provider I/O worker must run in a worker thread.");
  const registry = new Map(stages.map((stage) => [stage.name, stage]));
  const sessions = new Map<number, WorkerSession>();
  const unflushed = new Set<number>();
  let flushScheduled = false;

  const post = (message: ProviderIoWorkerMessage) => port.postMessage(message);

  const closeStage = (session: WorkerSession | undefined) => {
    try {
      session?.stage.close?.();
    } catch {}
  };

  const fail = (id: number, error: unknown) => {
    closeStage(sessions.get(id));
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
    const { outputs, held, freed, replies } = session;
    session.outputs = [];
    session.held = 0;
    session.freed = 0;
    session.replies = [];
    try {
      if (freed > 0) post({ type: "output", session: id, outputs: [], bytes: freed });
      if (outputs.length > 0) post({ type: "output", session: id, outputs, bytes: held });
    } catch (error) {
      fail(id, error);
      return false;
    }
    for (const message of replies) postReply(message);
    return true;
  };

  const flushAll = () => {
    flushScheduled = false;
    for (const id of unflushed) {
      const session = sessions.get(id);
      if (session !== undefined) flush(id, session);
    }
    unflushed.clear();
  };

  const schedule = (id: number) => {
    unflushed.add(id);
    if (!flushScheduled) {
      flushScheduled = true;
      setImmediate(flushAll);
    }
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

  const postReply = (message: ProviderIoWorkerMessage) => {
    if (message.type !== "reply") return;
    try {
      post(message);
    } catch (error) {
      post({ ...message, ok: false, value: new Error(String(error)) });
    }
  };

  // Replies and events never overtake the outputs their session emitted before them.
  const flushFirst = (id: number) => {
    const session = sessions.get(id);
    return session === undefined || !unflushed.has(id) || flush(id, session);
  };

  const reply = (id: number, call: number, ok: boolean, value: unknown) => {
    flushFirst(id);
    postReply({ type: "reply", session: id, call, ok, value });
  };

  // An async call settles in the same turn as the work behind it, which may still be emitting.
  const replyLater = (id: number, call: number, ok: boolean, value: unknown) => {
    const session = sessions.get(id);
    if (session === undefined) {
      reply(id, call, ok, value);
      return;
    }
    session.replies.push({ type: "reply", session: id, call, ok, value });
    schedule(id);
  };

  // Registers the session before the stage opens, so the stage can use its host at once.
  const openSession = (id: number, stage: AnyStage, options: unknown) => {
    const session: WorkerSession = {
      stage: { push: () => {}, end: () => {} },
      outputs: [],
      held: 0,
      freed: 0,
      finished: false,
      replies: [],
      emit: (output) => {
        session.outputs.push(output);
      },
    };
    const live = () => sessions.get(id) === session;
    const host: ProviderIoStageHost<unknown, unknown> = {
      emit: (output) => {
        if (!live() || session.finished) return;
        session.outputs.push(output);
        schedule(id);
      },
      signal: (event) => {
        if (!live() || !flushFirst(id)) return;
        try {
          post({ type: "event", session: id, event });
        } catch (error) {
          fail(id, error);
        }
      },
      finish: (...error) => {
        if (!live() || session.finished) return;
        session.finished = true;
        if (!flush(id, session)) return;
        if (error.length === 0) {
          post({ type: "end", session: id });
          return;
        }
        try {
          post({ type: "end", session: id, failed: true, error: error[0] });
        } catch {
          post({ type: "end", session: id, failed: true, error: new Error(String(error[0])) });
        }
      },
    };
    sessions.set(id, session);
    session.stage = stage.open(options as never, host);
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
        openSession(id, stage, message.options);
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
        schedule(id);
        return;
      }
      case "eof":
        if (!run(id, session, () => session.stage.end(session.emit))) return;
        if (!flush(id, session)) return;
        sessions.delete(id);
        post({ type: "end", session: id });
        return;
      case "close":
        closeStage(session);
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
        if (isThenable(value)) {
          value.then(
            (resolved) => replyLater(id, message.call, true, resolved),
            (error: unknown) => replyLater(id, message.call, false, error),
          );
          return;
        }
        reply(id, message.call, true, value);
        return;
      }
      case "notify": {
        const call = session.stage.call;
        if (call === undefined) return;
        let value: unknown;
        if (!run(id, session, () => (value = call(message.request as never)))) return;
        if (isThenable(value)) value.then(undefined, () => {});
        return;
      }
    }
  });

  post({ type: "ready" });
};

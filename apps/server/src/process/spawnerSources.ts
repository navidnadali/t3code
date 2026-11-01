// Inline sources also run in bundles and single-executable builds without module files.
const spawnProtocolSource = `
const streamNames = ["stdout", "stderr"];
const children = new Map();
const errorFields = (error) => {
  if (!(error instanceof Error)) return { name: "Error", message: String(error) };
  const fields = { name: error.name, message: error.message, stack: error.stack };
  for (const key of ["code", "errno", "syscall", "path", "spawnargs"]) {
    if (error[key] !== undefined) fields[key] = error[key];
  }
  return fields;
};
const start = (message) => {
  const id = message.id;
  let child;
  try {
    child = spawn(message.command, message.args, message.options);
  } catch (error) {
    post({ t: "throw", id, error: errorFields(error) });
    return;
  }
  const entry = { child, detached: message.options.detached === true, inflight: [0, 0], paused: [false, false] };
  children.set(id, entry);
  child.on("spawn", () => post({ t: "spawn", id, pid: child.pid }));
  child.on("error", (error) => post({ t: "error", id, error: errorFields(error) }));
  child.on("exit", (code, signal) => post({ t: "exit", id, code, signal }));
  child.on("close", () => {
    children.delete(id);
    post({ t: "close", id });
  });
  child.stdin.on("error", (error) => post({ t: "stdin-error", id, error: errorFields(error) }));
  streamNames.forEach((name, s) => {
    const stream = child[name];
    stream.on("data", (chunk) => {
      const data = new Uint8Array(chunk.byteLength);
      data.set(chunk);
      entry.inflight[s] += data.byteLength;
      post({ t: "data", id, s, chunk: data }, [data.buffer]);
      if (entry.inflight[s] >= windowBytes && !entry.paused[s]) {
        entry.paused[s] = true;
        stream.pause();
      }
    });
    stream.on("end", () => post({ t: "end", id, s }));
    stream.on("error", (error) => post({ t: "stream-error", id, s, error: errorFields(error) }));
  });
};
const destroyedStdin = { name: "Error", message: "Cannot call write after a stream was destroyed", code: "ERR_STREAM_DESTROYED" };
const onMessage = (message) => {
  const entry = children.get(message.id);
  switch (message.t) {
    case "spawn":
      start(message);
      return;
    case "stdin":
      if (entry === undefined) return post({ t: "stdin-done", id: message.id, seq: message.seq, error: destroyedStdin });
      entry.child.stdin.write(message.chunk, (error) =>
        post({ t: "stdin-done", id: message.id, seq: message.seq, error: error ? errorFields(error) : undefined }),
      );
      return;
    case "stdin-end":
      if (entry === undefined) return post({ t: "stdin-done", id: message.id, seq: message.seq, error: destroyedStdin });
      entry.child.stdin.end((error) =>
        post({ t: "stdin-done", id: message.id, seq: message.seq, error: error ? errorFields(error) : undefined }),
      );
      return;
    case "stdin-destroy":
      if (entry !== undefined) entry.child.stdin.destroy();
      return;
    case "kill": {
      if (entry === undefined) return post({ t: "kill-result", id: message.id, seq: message.seq, killed: false });
      let killed;
      try {
        killed = entry.child.kill(message.signal);
      } catch (error) {
        return post({ t: "kill-result", id: message.id, seq: message.seq, killed: false, error: errorFields(error) });
      }
      return post({ t: "kill-result", id: message.id, seq: message.seq, killed });
    }
    case "ack":
      if (entry === undefined) return;
      entry.inflight[message.s] -= message.bytes;
      if (entry.paused[message.s] && entry.inflight[message.s] < windowBytes) {
        entry.paused[message.s] = false;
        entry.child[streamNames[message.s]].resume();
      }
      return;
  }
};
`;

export const spawnWorkerSource = `
"use strict";
const { parentPort, workerData } = require("node:worker_threads");
const { spawn } = require("node:child_process");
const windowBytes = workerData.windowBytes;
const post = (message, transfer) => parentPort.postMessage(message, transfer);
${spawnProtocolSource}
parentPort.on("message", onMessage);
`;

/**
 * The spawner process: the server's IPC channel closes when the server exits, however it
 * exits, and the spawner then kills every child it started and exits too.
 */
export const spawnerProcessSource = `
"use strict";
const { spawn } = require("node:child_process");
const windowBytes = Number(process.argv[process.argv.length - 1]);
const post = (message) => {
  if (process.connected) process.send(message);
};
${spawnProtocolSource}
process.title = "t3-process-spawner";
process.on("message", onMessage);
process.on("disconnect", () => {
  for (const { child, detached } of children.values()) {
    try {
      if (detached && child.pid > 1) process.kill(-child.pid, "SIGKILL");
      else child.kill("SIGKILL");
    } catch {}
  }
  process.exit(0);
});
`;

/** Entry of `t3 __process-spawner <windowBytes>`: single executables cannot evaluate `-e`. */
export const runProcessSpawner = () => {
  new Function("require", spawnerProcessSource)((id: string) => process.getBuiltinModule(id));
};

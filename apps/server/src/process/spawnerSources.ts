// @effect-diagnostics nodeBuiltinImport:off - The handshake id is a constant computed when the module loads.
import * as NodeCrypto from "node:crypto";

import serverPackage from "../../package.json" with { type: "json" };

/** The spawner executable a single-executable build ships beside `t3`. */
export const PROCESS_SPAWNER_EXECUTABLE = "t3-process-spawner";

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
  if (child.pid !== undefined) started(id, child.pid);
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
      if (message.handoff !== undefined) startHandoff(message);
      else start(message);
      return;
    case "handoff-cancel":
      cancelHandoff(message.id);
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
const started = () => {};
const startHandoff = (message) =>
  post({ t: "handoff-failed", id: message.id, error: { name: "Error", message: "A worker thread hands off no descriptors" } });
const cancelHandoff = () => {};
${spawnProtocolSource}
parentPort.on("message", onMessage);
`;

// Hand-off: the child's stdio are Unix sockets the server connects, so its output never passes
// through this process. pauseOnConnect: an accepted socket reads nothing until its header is
// checked, and the server writes nothing past the header before the start is answered.
const handoffSource = `
const handoffs = new Map();
const cancelHandoff = (id) => handoffs.get(id)?.cancel();
const processStartTime = (pid) => {
  try {
    const stat = require("node:fs").readFileSync("/proc/" + pid + "/stat", "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
  } catch {
    return undefined;
  }
};
const startHandoff = (message) => {
  const id = message.id;
  const { path: socketPath, nonce, fds } = message.handoff;
  const headerBytes = nonce.byteLength + 1;
  const accepted = new Map();
  const unchecked = new Set();
  let closed = false;
  const close = () => {
    if (closed) return;
    closed = true;
    handoffs.delete(id);
    server.close();
    require("node:fs").rm(socketPath, { force: true }, () => {});
    for (const socket of unchecked) socket.destroy();
  };
  const refuse = (socket) => {
    unchecked.delete(socket);
    socket.destroy();
    post({ t: "handoff-refused", id });
  };
  const server = require("node:net").createServer({ pauseOnConnect: true }, (socket) => {
    socket.on("error", () => {});
    if (closed) return refuse(socket);
    unchecked.add(socket);
    const chunks = [];
    let length = 0;
    const onData = (chunk) => {
      chunks.push(chunk);
      length += chunk.byteLength;
      if (length < headerBytes) return;
      socket.off("data", onData);
      socket.off("end", onEnd);
      socket.pause();
      const header = Buffer.concat(chunks);
      const fd = header[nonce.byteLength];
      if (
        closed ||
        header.byteLength !== headerBytes ||
        !require("node:crypto").timingSafeEqual(header.subarray(0, nonce.byteLength), nonce) ||
        !fds.includes(fd) ||
        accepted.has(fd)
      ) {
        return refuse(socket);
      }
      unchecked.delete(socket);
      accepted.set(fd, socket);
      if (accepted.size === fds.length) {
        close();
        launch();
      }
    };
    const onEnd = () => refuse(socket);
    socket.on("data", onData);
    socket.on("end", onEnd);
    socket.resume();
  });
  server.on("error", (error) => {
    close();
    for (const socket of accepted.values()) socket.destroy();
    post({ t: "handoff-failed", id, error: errorFields(error) });
  });
  handoffs.set(id, {
    cancel: () => {
      close();
      for (const socket of accepted.values()) socket.destroy();
    },
  });
  server.listen(socketPath, () => {
    if (!closed) post({ t: "handoff", id });
  });
  const launch = () => {
    const stdio = [...message.options.stdio];
    for (const [fd, socket] of accepted) stdio[fd] = socket;
    let child;
    try {
      child = spawn(message.command, message.args, { ...message.options, stdio });
    } catch (error) {
      post({ t: "throw", id, error: errorFields(error) });
      return;
    } finally {
      for (const socket of accepted.values()) socket.destroy();
    }
    children.set(id, { child, detached: message.options.detached === true, handoff: true });
    // Read before this process can reap the child, so the PID still names it.
    const startTime = child.pid === undefined ? undefined : processStartTime(child.pid);
    if (child.pid !== undefined) started(id, child.pid);
    child.on("spawn", () => post({ t: "spawn", id, pid: child.pid, startTime }));
    child.on("error", (error) => post({ t: "error", id, error: errorFields(error) }));
    child.on("exit", (code, signal) => post({ t: "exit", id, code, signal }));
    child.on("close", () => {
      children.delete(id);
      post({ t: "close", id });
    });
  };
};
`;

const spawnerProcessBody = `
const { spawn } = require("node:child_process");
const windowBytes = Number(process.argv[process.argv.length - 1]);
// A reply racing the server's disconnect must not crash the spawner before it kills its children.
const post = (message) => {
  if (process.connected) process.send(message, () => {});
};
// Written at once, before any reply, so the server can kill this child even if the spawner dies.
const started = (id, pid) => {
  try {
    require("node:fs").writeSync(1, id + " " + pid + "\\n");
  } catch {}
};
${spawnProtocolSource}
${handoffSource}
process.title = "t3-process-spawner";
process.on("message", onMessage);
post({ t: "ready", protocol: handshake.protocol, build: handshake.build });
// A child whose descriptors the server holds talks to the server, not to the spawner: only the
// server's exit (the channel closing) ends it here.
const killChildren = (handedOff) => {
  for (const { child, detached, handoff } of children.values()) {
    if (handoff && !handedOff) continue;
    try {
      if (detached && child.pid > 1) process.kill(-child.pid, "SIGKILL");
      else child.kill("SIGKILL");
    } catch {}
  }
};
// Launched detached, the spawner leads its own process group (never the server's): ending that group
// also ends what children spawned with detached: false left behind, and the spawner with them.
const exitWithGroup = (code, reason, handedOff = false) => {
  try {
    require("node:fs").writeSync(1, "exit " + reason.replace(/\\s+/g, " ") + "\\n");
  } catch {}
  killChildren(handedOff);
  try {
    if (process.pid > 1) process.kill(-process.pid, "SIGKILL");
  } catch {}
  process.exit(code);
};
process.on("disconnect", () => exitWithGroup(0, "its channel closed", true));
process.on("uncaughtException", (error) => {
  try {
    require("node:fs").writeSync(2, "t3-process-spawner: " + (error && error.stack ? error.stack : String(error)) + "\\n");
  } catch {}
  exitWithGroup(70, "uncaught error: " + (error && error.message ? error.message : String(error)));
});
for (const signal of ["SIGTERM", "SIGINT", "SIGHUP"]) {
  process.once(signal, () => exitWithGroup(128 + require("node:os").constants.signals[signal], "received " + signal));
}
`;

/** What a spawner process says first: the server kills one that says anything else. */
export const spawnerHandshake = {
  protocol: NodeCrypto.createHash("sha256").update(spawnerProcessBody).digest("hex").slice(0, 16),
  build: serverPackage.version,
};

/**
 * The spawner process: the server's IPC channel closes when the server exits, however it exits,
 * and the spawner then kills every child it started and exits too; it does the same when it
 * fails or is asked to stop. Only a SIGKILL of the spawner between a fork and the PID report
 * written right after it can leave a child the server does not know (Node exposes no PDEATHSIG).
 */
export const spawnerProcessSource = `"use strict";
const handshake = { protocol: "${spawnerHandshake.protocol}", build: "${spawnerHandshake.build}" };
${spawnerProcessBody}`;

/** Entry of `t3 __process-spawner <windowBytes>`: single executables cannot evaluate `-e`. */
export const runProcessSpawner = () => {
  new Function("require", spawnerProcessSource)((id: string) => process.getBuiltinModule(id));
};

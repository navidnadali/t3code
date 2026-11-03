// @effect-diagnostics nodeBuiltinImport:off - The handshake id is a constant computed when the module loads.
import * as NodeCrypto from "node:crypto";

import serverPackage from "../../package.json" with { type: "json" };

/** The spawner executable a single-executable build ships beside `t3`. */
export const PROCESS_SPAWNER_EXECUTABLE = "t3-process-spawner";

/** The spawner executable's argument that makes it a handed-off command's shim. */
export const EXEC_AFTER_GO = "__exec-after-go";

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
    case "handoff-go":
      releaseHandoff(message.id);
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
const releaseHandoff = () => {};
${spawnProtocolSource}
parentPort.on("message", onMessage);
`;

// Hand-off: the child's stdio are the ends of ordinary pipes that this process passes to the server
// over its channel, keeping no copy, so its output never passes through here and no path or name
// leads to them. The child starts as a waiting shim (`execAfterGoBody`) that has written nothing
// when the server holds them all; it is then told, on descriptor 3, what to become.
const handoffSource = `
const handoffs = new Map();
const processStartTime = (pid) => {
  try {
    const stat = require("node:fs").readFileSync("/proc/" + pid + "/stat", "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).split(" ")[19];
  } catch {
    return undefined;
  }
};
// The file execvp would run for Node's spawn, or the code it would fail with.
const resolveExecutable = (command, env, cwd) => {
  const fs = require("node:fs");
  const path = require("node:path");
  const check = (file) => {
    try {
      if (!fs.statSync(file).isFile()) return { problem: "EACCES" };
      fs.accessSync(file, fs.constants.X_OK);
      return { file };
    } catch (error) {
      return { problem: error && error.code === "EACCES" ? "EACCES" : "ENOENT" };
    }
  };
  const base = cwd ?? process.cwd();
  if (command.includes("/")) return check(path.resolve(base, command));
  let problem = "ENOENT";
  for (const directory of (env.PATH ?? "/usr/bin:/bin").split(":")) {
    const found = check(path.resolve(base, directory || ".", command));
    if (found.file !== undefined) return found;
    if (found.problem === "EACCES") problem = "EACCES";
  }
  return { problem };
};
// An error about the command, not the shim that starts it: as Node's spawn reports it.
const commandError = (error, command, args) => {
  const code = error && error.code;
  const failure = new Error("spawn " + command + (code ? " " + code : ""));
  if (code !== undefined) failure.code = code;
  if (error && error.errno !== undefined) failure.errno = error.errno;
  failure.syscall = "spawn " + command;
  failure.path = command;
  failure.spawnargs = [...args];
  return failure;
};
const cancelHandoff = (id) => {
  const handoff = handoffs.get(id);
  if (handoff === undefined) return;
  handoffs.delete(id);
  handoff.cancel();
};
const startHandoff = (message) => {
  const id = message.id;
  const { fds, shim } = message.handoff;
  const options = message.options;
  const env = options.env ?? process.env;
  const resolved = resolveExecutable(message.command, env, options.cwd);
  if (resolved.file === undefined) {
    const errno = resolved.problem === "EACCES" ? -13 : -2;
    post({ t: "error", id, error: errorFields(commandError({ code: resolved.problem, errno }, message.command, message.args)) });
    post({ t: "close", id });
    return;
  }
  const stdio = [...options.stdio];
  for (const fd of fds) stdio[fd] = "pipe";
  stdio[3] = "pipe";
  let child;
  try {
    // Its own environment is empty: nothing of the command's (NODE_OPTIONS included) applies to it.
    child = spawn(shim.command, shim.args, { cwd: options.cwd, detached: options.detached, env: {}, stdio, windowsHide: true });
  } catch (error) {
    post({ t: "throw", id, error: errorFields(commandError(error, message.command, message.args)) });
    return;
  }
  const go = child.stdio[3];
  go.on("error", () => {});
  let cancelled = false;
  const cancel = () => {
    cancelled = true;
    // Never released: the shim reads end of input and exits without running the command.
    go.destroy();
    for (const fd of fds) child.stdio[fd].destroy();
  };
  children.set(id, { child, detached: options.detached === true, handoff: true });
  // Read before this process can reap the child, so the PID still names it.
  const startTime = child.pid === undefined ? undefined : processStartTime(child.pid);
  if (child.pid !== undefined) started(id, child.pid, startTime);
  handoffs.set(id, {
    cancel,
    // The server holds every end: the command may run, and nothing it writes comes here.
    release: () => {
      handoffs.delete(id);
      const argv = [options.argv0 ?? message.command, ...message.args];
      go.end(JSON.stringify({ file: resolved.file, argv, env }));
      post({ t: "spawn", id, pid: child.pid, startTime });
    },
  });
  child.on("spawn", () => {
    if (cancelled) return;
    for (const fd of fds) {
      process.send({ t: "handoff-fd", id, fd }, child.stdio[fd], { keepOpen: false }, (error) => {
        if (error === null || error === undefined || cancelled) return;
        handoffs.delete(id);
        cancel();
        post({ t: "handoff-failed", id, error: errorFields(error) });
      });
    }
  });
  child.on("error", (error) => {
    handoffs.delete(id);
    post({ t: "error", id, error: errorFields(commandError(error, message.command, message.args)) });
  });
  child.on("exit", (code, signal) => post({ t: "exit", id, code, signal }));
  child.on("close", () => {
    children.delete(id);
    post({ t: "close", id });
  });
};
const releaseHandoff = (id) => handoffs.get(id)?.release();
`;

// A handed-off command's shim: once the server holds its stdio it becomes the command (execve: same
// PID and group). File, argv and environment arrive on descriptor 3, never on any argv; its own
// environment is empty. SIGPIPE and SIGXFSZ, which Node ignores and an exec would keep ignored, are
// caught instead, so the command starts with them at their defaults, as after Node's spawn.
const execAfterGoBody = `
const fs = require("node:fs");
const request = [];
const buffer = Buffer.alloc(65536);
for (;;) {
  let count;
  try {
    count = fs.readSync(3, buffer, 0, buffer.length, null);
  } catch (error) {
    if (error && error.code === "EAGAIN") {
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
      continue;
    }
    process.exit(125);
  }
  if (count === 0) break;
  request.push(Buffer.from(buffer.subarray(0, count)));
}
fs.closeSync(3);
// Never released: the start was cancelled or the spawner went away, and the command never runs.
if (request.length === 0) process.exit(125);
const { file, argv, env } = JSON.parse(Buffer.concat(request).toString("utf8"));
for (const signal of ["SIGPIPE", "SIGXFSZ"]) process.on(signal, () => {});
try {
  try {
    process.execve(file, argv, env);
  } catch (error) {
    // As execvp does for a script without a #! line.
    if (!error || error.code !== "ENOEXEC") throw error;
    process.execve("/bin/sh", ["/bin/sh", file, ...argv.slice(1)], env);
  }
} catch (error) {
  try {
    fs.writeSync(2, "t3: cannot run " + file + ": " + (error && error.message) + "\\n");
  } catch {}
  process.exit(error && error.code === "ENOENT" ? 127 : 126);
}
`;

/** The shim as a script for `node -e` (the spawner's own Node runs it outside single executables). */
export const execAfterGoSource = `"use strict";
${execAfterGoBody}`;

const spawnerProcessBody = `
const { spawn } = require("node:child_process");
const windowBytes = Number(process.argv[process.argv.length - 1]);
// A reply racing the server's disconnect must not crash the spawner before it kills its children.
const post = (message) => {
  if (process.connected) process.send(message, () => {});
};
// Written at once, before any reply, so the server can kill this child even if the spawner dies;
// a handed-off child's start time comes with it, its identity once its PID can name another.
const started = (id, pid, startTime) => {
  try {
    require("node:fs").writeSync(1, id + " " + pid + (startTime === undefined ? "" : " " + startTime) + "\\n");
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
  protocol: NodeCrypto.createHash("sha256")
    .update(spawnerProcessBody)
    .update(execAfterGoBody)
    .digest("hex")
    .slice(0, 16),
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
if (process.argv.includes("${EXEC_AFTER_GO}")) {
${execAfterGoBody}
} else {
${spawnerProcessBody}
}`;

/** Entry of `t3 __process-spawner <windowBytes>`: single executables cannot evaluate `-e`. */
export const runProcessSpawner = () => {
  new Function("require", spawnerProcessSource)((id: string) => process.getBuiltinModule(id));
};

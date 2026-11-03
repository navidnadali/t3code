// @effect-diagnostics nodeBuiltinImport:off - Signals need a process's identity now, from /proc.
import * as NodeFS from "node:fs";

// The fields of /proc/<pid>/stat after the command name: state first, start time 20th.
const statFields = (pid: number): ReadonlyArray<string> | undefined => {
  let stat: string;
  try {
    stat = NodeFS.readFileSync(`/proc/${pid}/stat`, "utf8");
  } catch {
    return undefined;
  }
  return stat.slice(stat.lastIndexOf(")") + 2).split(" ");
};

/**
 * Whether the process with this PID is still the one the spawner started: same /proc start time,
 * not a zombie. A PID that names another process (reused) or none means the child exited.
 */
export const isSameProcess = (pid: number, startTime: string): boolean => {
  const fields = statFields(pid);
  return fields !== undefined && fields[0] !== "Z" && fields[0] !== "X" && fields[19] === startTime;
};

/**
 * Whether this PID now names a process other than the one that started at `startTime`, so a
 * signal to it or to the group it leads would reach a stranger. A PID that names no process is not
 * reused: while the old process group has members, Linux gives its number to no new process.
 */
export const isPidReused = (pid: number, startTime: string): boolean => {
  const fields = statFields(pid);
  return fields !== undefined && fields[19] !== startTime;
};

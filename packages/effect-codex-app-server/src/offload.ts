// Runs on a provider I/O worker, so it imports nothing that builds the generated schemas.
import type { CodexAppServerProtocolLogEvent } from "./protocol.ts";

/** A log record serialized off the main thread, for the log stream it was serialized for. */
export interface SerializedLogRecord {
  readonly stream: string;
  readonly payload: string;
}

/** A protocol-log entry for an incoming event: a record to write, nothing, or main must log it. */
export type IncomingLogEntry = SerializedLogRecord | "skip" | "in-thread";

/** A record built off-thread, with the stage of the incoming event it was built for. */
export interface OffloadedLogRecord extends SerializedLogRecord {
  readonly stage: "raw" | "decoded";
}

/** One incoming line handled off the main thread. */
export type OffloadedLine =
  /** Main handles the line as it would in-thread (e.g. it does not parse, so main reports it). */
  | { readonly _tag: "InThread" }
  /** Parsed, with its incoming log entries built. */
  | {
      readonly _tag: "Decoded";
      readonly message: unknown;
      readonly logs?: ReadonlyArray<OffloadedLogRecord>;
    };

const inThread: OffloadedLine = { _tag: "InThread" };

/** Parses one line and builds its incoming log entries (`logEntry` unset: none), as in-thread. */
export function decodeOffloadedLine(
  line: string,
  logEntry?: (event: CodexAppServerProtocolLogEvent) => IncomingLogEntry,
): OffloadedLine {
  let message: unknown;
  try {
    // `decodeWireLine` parses with this same JSON.parse; main reports a line that fails.
    message = JSON.parse(line);
  } catch {
    return inThread;
  }
  const logs: Array<OffloadedLogRecord> = [];
  if (logEntry !== undefined) {
    for (const event of [
      { direction: "incoming", stage: "raw", payload: line },
      { direction: "incoming", stage: "decoded", payload: message },
    ] as const) {
      const entry = logEntry(event);
      if (entry === "in-thread") return inThread;
      if (entry === "skip") continue;
      logs.push({ stream: entry.stream, payload: entry.payload, stage: event.stage });
    }
  }
  return logs.length === 0 ? { _tag: "Decoded", message } : { _tag: "Decoded", message, logs };
}

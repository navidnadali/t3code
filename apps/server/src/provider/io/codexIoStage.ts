import * as CodexOffload from "effect-codex-app-server/offload";
import type { CodexAppServerProtocolLogEvent } from "effect-codex-app-server/protocol";

import { codexProtocolLogRecord } from "../CodexProtocolLog.ts";
import { serializeEventNdjsonRecord } from "../EventNdjsonRecord.ts";
import type { ProviderIoStage } from "./providerIoProtocol.ts";

export interface CodexIoStageOptions {
  /** Set when incoming protocol events go to the native log. */
  readonly logProviderSessionId?: string;
}

const logEntry = (
  event: CodexAppServerProtocolLogEvent,
  providerSessionId: string,
): CodexOffload.IncomingLogEntry => {
  const record = codexProtocolLogRecord(event, providerSessionId);
  if (record === undefined) return "skip";
  const serialized = serializeEventNdjsonRecord("native", record);
  switch (serialized._tag) {
    case "Skipped":
      return "skip";
    case "Serialized":
      return { payload: serialized.payload };
    case "Failed":
      // Main logs it, so the logger reports the failure as it does today.
      return "in-thread";
  }
};

/** Takes one long stdout line per call; main frames stdout and keeps short lines. */
export const codexIoStage: ProviderIoStage<
  CodexIoStageOptions,
  never,
  string,
  CodexOffload.OffloadedLine
> = {
  name: "codex-app-server",
  open: (options) => {
    const providerSessionId = options.logProviderSessionId;
    const log =
      providerSessionId === undefined
        ? undefined
        : (event: CodexAppServerProtocolLogEvent) => logEntry(event, providerSessionId);
    return {
      push: () => {},
      end: () => {},
      call: (line) => CodexOffload.decodeOffloadedLine(line, log),
    };
  },
};

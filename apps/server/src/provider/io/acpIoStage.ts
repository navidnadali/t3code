import * as AcpOffload from "effect-acp/offload";

import type { ProviderIoStage } from "./providerIoProtocol.ts";
import { MAX_OFFLOADED_RESULT_DEPTH, nestsDeeperThan } from "./resultDepth.ts";

/** Takes one long stdout line per call; main splits stdout and keeps short lines and batches. */
export const acpIoStage: ProviderIoStage<void, never, string, AcpOffload.OffloadedLine> = {
  name: "acp",
  open: () => {
    const decode = AcpOffload.makeOffloadedLineDecoder();
    return {
      push: () => {},
      end: () => {},
      call: (line) => {
        const result = decode(line);
        return result._tag === "Decoded" &&
          nestsDeeperThan(result.messages, MAX_OFFLOADED_RESULT_DEPTH)
          ? { _tag: "InThread" }
          : result;
      },
    };
  },
};

import * as AcpOffload from "effect-acp/offload";

import type { ProviderIoStage } from "./providerIoProtocol.ts";

/** Takes one long stdout line per call; main splits stdout and keeps short lines and batches. */
export const acpIoStage: ProviderIoStage<void, never, string, AcpOffload.OffloadedLine> = {
  name: "acp",
  open: () => ({
    push: () => {},
    end: () => {},
    call: AcpOffload.makeOffloadedLineDecoder(),
  }),
};

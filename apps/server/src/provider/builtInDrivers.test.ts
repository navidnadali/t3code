import { assert, it } from "@effect/vitest";
import { PROVIDER_IO_STAGE_PROVIDERS } from "@t3tools/provider-core/server/ProviderIoStageTiming";

import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";

it("times every built-in driver's provider I/O under its own name", () => {
  for (const { driverKind } of BUILT_IN_DRIVERS) {
    assert.isTrue(PROVIDER_IO_STAGE_PROVIDERS.has(driverKind), driverKind);
  }
});

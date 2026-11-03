import { describe, expect, it } from "@effect/vitest";

import { SEA_EXEC_ARGV, SEA_EXEC_ARGV_EXTENSION } from "./seaBuild.ts";
import { DEFAULT_MAX_SEMI_SPACE_MB } from "./youngGeneration.ts";

describe("the single executable's build", () => {
  it("bakes the young-generation default into the executable and takes options on its command line", () => {
    // Node applies NODE_OPTIONS before the baked execArgv, so a baked cap could never be changed from the
    // environment; "cli" lets --node-options extend and override it, and the server carries NODE_OPTIONS there.
    expect(SEA_EXEC_ARGV).toEqual([`--max-semi-space-size=${DEFAULT_MAX_SEMI_SPACE_MB}`]);
    expect(SEA_EXEC_ARGV_EXTENSION).toBe("cli");
  });
});


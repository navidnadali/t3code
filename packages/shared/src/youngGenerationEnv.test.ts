import { describe, expect, it } from "@effect/vitest";

import {
  DEFAULT_MAX_SEMI_SPACE_MB,
  SEA_EXEC_ARGV,
  SEA_EXEC_ARGV_EXTENSION,
  seaBuildOptions,
} from "./youngGenerationEnv.ts";

describe("the single executable's build", () => {
  it("bakes the young-generation default into the executable and takes options on its command line", () => {
    // Node applies NODE_OPTIONS before the baked execArgv, so a baked cap could never be changed from the
    // environment; "cli" lets --node-options extend and override it, and the server carries NODE_OPTIONS there.
    expect(SEA_EXEC_ARGV).toEqual([`--max-semi-space-size=${DEFAULT_MAX_SEMI_SPACE_MB}`]);
    expect(SEA_EXEC_ARGV_EXTENSION).toBe("cli");
  });

  it("bakes nothing into a Windows executable and lets it read NODE_OPTIONS itself", () => {
    // Windows has no execve to carry NODE_OPTIONS onto the command line, so a baked cap there could
    // never be overridden and NODE_OPTIONS would be lost; it stays as before (the launcher passes the cap).
    expect(seaBuildOptions("win")).toEqual({ execArgv: [], execArgvExtension: "env" });
    expect(seaBuildOptions("darwin")).toEqual({
      execArgv: SEA_EXEC_ARGV,
      execArgvExtension: SEA_EXEC_ARGV_EXTENSION,
    });
    expect(seaBuildOptions("linux")).toEqual({
      execArgv: SEA_EXEC_ARGV,
      execArgvExtension: SEA_EXEC_ARGV_EXTENSION,
    });
  });
});

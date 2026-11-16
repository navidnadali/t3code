import { assert, describe, it } from "@effect/vitest";
import { vi } from "vite-plus/test";

import type {
  ClaudeAgentSdkWorkerEvent,
  ClaudeAgentSdkWorkerQueryOptions,
} from "./claudeAgentSdkProtocol.ts";
import { claudeAgentSdkStage } from "./claudeAgentSdkStage.ts";

describe("claudeAgentSdkStage SDK warnings", () => {
  it("opens a worker's query without the SDK's canUseTool shadowing warning, as in-thread", async () => {
    using emitWarning = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    const events: Array<ClaudeAgentSdkWorkerEvent["type"]> = [];
    for (const options of [
      { permissionMode: "default", allowedTools: ["mcp__t3-code__*"] },
      { permissionMode: "bypassPermissions", allowDangerouslySkipPermissions: true },
    ] satisfies ReadonlyArray<ClaudeAgentSdkWorkerQueryOptions>) {
      const session = claudeAgentSdkStage.open(
        {
          options: { ...options, pathToClaudeCodeExecutable: "/nonexistent/claude" },
          callbacks: ["canUseTool"],
        },
        { emit: () => {}, signal: (event) => events.push(event.type), finish: () => {} },
      );
      await session.call?.({ type: "start" });
      session.close?.();
    }

    assert.deepEqual(
      emitWarning.mock.calls.map(([, options]) =>
        typeof options === "object" ? options.code : options,
      ),
      [],
    );
    assert.strictEqual<unknown>(process.emitWarning, emitWarning);
    assert.deepEqual(
      events.filter((type) => type === "spawn"),
      ["spawn", "spawn"],
    );
  });
});

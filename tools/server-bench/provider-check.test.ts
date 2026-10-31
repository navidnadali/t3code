import { describe, expect, it } from "vite-plus/test";

import { compareItems, type ServerItem, type TurnExpectation } from "./provider-check.ts";

const turn = (items: TurnExpectation["items"], todoLists = 0): TurnExpectation => ({
  frames: {},
  items,
  todoLists,
  reply: null,
});
const rows = (...types: ReadonlyArray<string>): ReadonlyArray<ServerItem> =>
  types.map((type, index) => ({ type, status: "completed", id: `turn-item:${type}:${index}` }));
const SERVER_STEPS = rows("checkpoint").concat([
  { type: "command_execution", status: "completed", id: "native-item:workspace-preparation%3Arun" },
]);

describe("provider check items", () => {
  it("matches a Codex plan turn only while its proposed plan is there", () => {
    const played = [turn({ userMessage: 1, plan: 1 })];

    expect(
      compareItems("codex", played, [...SERVER_STEPS, ...rows("user_message", "proposed_plan")]),
    ).toMatchObject({ match: true, expected: { user_message: 1, proposed_plan: 1 } });
    expect(compareItems("codex", played, [...SERVER_STEPS, ...rows("user_message")]).match).toBe(
      false,
    );
  });

  it("never matches a transcript item it cannot map or a server item nobody expected", () => {
    const subagent = compareItems(
      "codex",
      [turn({ userMessage: 1, agentMessage: 1, collabAgentToolCall: 1 })],
      rows("user_message", "assistant_message"),
    );
    const extra = compareItems(
      "codex",
      [turn({ userMessage: 1, agentMessage: 1 })],
      rows("user_message", "assistant_message", "dynamic_tool"),
    );

    expect(subagent).toMatchObject({ match: false, unmapped: ["collabAgentToolCall"] });
    expect(extra).toMatchObject({ match: false, actual: { dynamic_tool: 1 } });
  });

  it("counts any other ACP item as a tool call", () => {
    const played = [turn({ assistant_message: 2, reasoning: 0, tool: 2 }, 1)];
    const server = rows("user_message", "assistant_message", "assistant_message", "todo_list");

    expect(
      compareItems("acp", played, [...server, ...rows("dynamic_tool", "file_read")]),
    ).toMatchObject({
      match: true,
      actual: { user_message: 1, assistant_message: 2, todo_list: 1, tool: 2 },
    });
    expect(compareItems("acp", played, [...server, ...rows("dynamic_tool")]).match).toBe(false);
  });
});

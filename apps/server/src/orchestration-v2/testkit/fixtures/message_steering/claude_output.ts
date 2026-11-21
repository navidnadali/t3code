import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertRuntimeRequestCounts,
  assertSemanticProjectionIntegrity,
  assertTurnItemTypes,
  assertUserMessageInputIntents,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  MESSAGE_STEERING_INITIAL_PROMPT,
  MESSAGE_STEERING_STEER_PROMPT,
  projectionFor,
} from "../shared.ts";

export function assertClaudeMessageSteeringOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assert.equal(transcript.provider, "claudeAgent");
  assertBaseProjection({ result, transcript, runCount: 1, runStatuses: ["completed"] });

  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertTurnItemTypes(projection, ["user_message", "assistant_message"]);
  assertRuntimeRequestCounts(projection, { total: 0 });
  assertUserMessagesInclude(projection, [
    MESSAGE_STEERING_INITIAL_PROMPT,
    MESSAGE_STEERING_STEER_PROMPT,
  ]);
  assertUserMessageInputIntents(projection, ["turn_start", "steer"]);
  // Recorded from Claude Code 2.1.330 with the steer at priority "next" and no tool call left in
  // the turn: Claude reports the steer queued, finishes the first reply, then answers the steer in
  // a turn of its own. Both replies belong to the one run the steer joined. (The "now" recording,
  // where Claude aborted the first reply instead, is claude_now_abort_transcript.ndjson.)
  assertAssistantTextIncludes(projection, "steering fixture initial response");
  assertAssistantTextIncludes(projection, "steering fixture observed");
  assert.equal(projection.runs.length, 1, "steering must attach to the active run");
  assert.equal(
    projection.providerTurns.length,
    1,
    "active steering must not create a new provider turn",
  );
}

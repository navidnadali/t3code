import { assert } from "@effect/vitest";
import type { ProviderReplayTranscript } from "@t3tools/contracts";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";

import type { OrchestratorV2ScenarioResult } from "../../OrchestratorScenario.ts";
import { cancelledTurnItemWork } from "../../../RestartBackgroundNote.ts";
import {
  assertBaseProjection,
  assertSemanticProjectionIntegrity,
  assertUserMessagesInclude,
  assertVisibleTurnItemsMirrorLocalTurnItems,
  backgroundNotifications,
  projectionFor,
} from "../shared.ts";
import { GROK_MONITOR_PROMPT, GROK_MONITOR_TICKS } from "./input.ts";

// Monitor ticks report in_progress with exit_code 0; only _x.ai/task_completed ends them.
// Hold run 1 until then; Grok's later reply is a continuation.
export function assertGrokMonitorOutput(
  result: OrchestratorV2ScenarioResult,
  transcript: ProviderReplayTranscript,
) {
  assertBaseProjection({
    result,
    transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertVisibleTurnItemsMirrorLocalTurnItems(projection);
  assertUserMessagesInclude(projection, [GROK_MONITOR_PROMPT]);

  const [rootRun, wakeRun] = projection.runs;
  const monitors = projection.turnItems.filter((item) => item.type === "command_execution");
  assert.lengthOf(monitors, 1);
  const monitor = monitors[0];
  assert.equal(monitor?.runId, rootRun?.id, "the monitor belongs to the run that started it");
  assert.equal(monitor?.status, "completed");
  assert.equal(monitor?.output, GROK_MONITOR_TICKS.map((tick) => `${tick}\n`).join(""));

  const monitorUpdates = result.domainEvents.flatMap((event) =>
    event.type === "turn-item.updated" &&
    event.payload.id === monitor?.id &&
    event.payload.type === "command_execution"
      ? [event.payload]
      : [],
  );
  const firstCompleted = monitorUpdates.findIndex((update) => update.status === "completed");
  assert.isAtLeast(firstCompleted, 0, "the monitor never completed");
  // A tick's running state can be superseded before it is written (the mailbox writes an
  // item's newest state), so a tick shows first in a running state or in the completed one. The
  // monitor completes only once every tick is in its output.
  for (const tick of GROK_MONITOR_TICKS) {
    const tickUpdate = monitorUpdates.findIndex((update) => update.output?.includes(tick));
    assert.isAtLeast(tickUpdate, 0, `missing monitor update for ${tick}`);
    assert.include(monitorUpdates[firstCompleted]?.output, tick, `completed before ${tick}`);
    if (tickUpdate < firstCompleted)
      assert.equal(monitorUpdates[tickUpdate]?.status, "running", `${tick} must show running`);
  }

  // A persistent monitor keeps running after its run ends. Background work and
  // the restart note must then name it a monitor, as they do for Claude's Monitor.
  // Its stored running state, or (each superseded) its completed one as it ran.
  const runningMonitor = monitorUpdates.find((update) => update.status === "running") ?? {
    ...monitorUpdates[firstCompleted]!,
    status: "running" as const,
  };
  assert.deepEqual(
    derivePendingBackgroundWork({
      latestRun: rootRun,
      providerThreads: [],
      turnItems: [runningMonitor],
    }).map((task) => task.kind),
    ["monitor"],
  );
  assert.equal(cancelledTurnItemWork(runningMonitor)?.kind, "monitor");

  const monitorCompletedAt = result.domainEvents.findIndex(
    (event) =>
      event.type === "turn-item.updated" &&
      event.payload.id === monitor?.id &&
      event.payload.status === "completed",
  );
  const rootCompletedAt = result.domainEvents.findIndex(
    (event) =>
      event.type === "run.updated" &&
      event.payload.id === rootRun?.id &&
      event.payload.status === "completed",
  );
  assert.isAtLeast(monitorCompletedAt, 0);
  assert.isAbove(rootCompletedAt, monitorCompletedAt, "run 1 completed before the monitor ended");

  const rootTexts = projection.turnItems.flatMap((item) =>
    item.runId === rootRun?.id && item.type === "assistant_message" ? [item.text.trim()] : [],
  );
  assert.include(rootTexts, "ROOT_DONE");

  const permissionAnswer = transcript.entries.find(
    (entry) =>
      entry.type === "expect_outbound" &&
      (entry.frame as { kind?: unknown; method?: unknown }).kind === "response" &&
      (entry.frame as { method?: unknown }).method === "session/request_permission",
  );
  assert.deepEqual(
    permissionAnswer?.type === "expect_outbound"
      ? (permissionAnswer.frame as { result?: unknown }).result
      : undefined,
    { outcome: { outcome: "selected", optionId: "allow-once" } },
  );

  assert.deepEqual(backgroundNotifications(projection), [
    {
      summary: 'Monitor "Watch three tick echoes" finished',
      outcome: "completed",
      source: { kind: "monitor" },
    },
  ]);

  const wakeMessage = projection.messages.find((message) => message.id === wakeRun?.userMessageId);
  assert.equal(`${wakeMessage?.createdBy}:${wakeMessage?.creationSource}`, "agent:provider");
  assert.isNotEmpty(
    projection.turnItems.filter(
      (item) => item.runId === wakeRun?.id && item.type === "assistant_message",
    ),
    "Grok's reply to the finished monitor must land in the continuation run",
  );
}

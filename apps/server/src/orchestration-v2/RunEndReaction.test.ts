import { assert, it } from "@effect/vitest";
import { MessageId, NodeId, RunId, type OrchestrationV2Run } from "@t3tools/contracts";

import { makeRunEndReactions } from "./RunEndReaction.ts";

const update = (
  id: string,
  status: OrchestrationV2Run["status"],
  delegatedCompletion?: OrchestrationV2Run["delegatedCompletion"],
) => ({
  id: RunId.make(id),
  status,
  ...(delegatedCompletion === undefined ? {} : { delegatedCompletion }),
});

const cohort = (
  delivery: NonNullable<OrchestrationV2Run["delegatedCompletion"]>["delivery"],
  disposition: "open" | "disposed" = "open",
): OrchestrationV2Run["delegatedCompletion"] => ({
  disposition,
  nextGeneration: 2,
  delivery,
});

it("reacts in full to a run's first end and not to a rewrite that keeps its status", () => {
  const reactions = makeRunEndReactions(10);
  assert.equal(reactions.observe(update("run:1", "running")), "full");
  assert.equal(reactions.observe(update("run:1", "waiting")), "full");
  assert.equal(reactions.observe(update("run:1", "completed")), "full");
  // Delegated-completion bookkeeping rewrites the ended run without changing its status.
  assert.equal(reactions.observe(update("run:1", "completed")), "none");
  assert.equal(reactions.observe(update("run:1", "completed")), "none");
  // A status change is a real transition again.
  assert.equal(reactions.observe(update("run:1", "rolled_back")), "full");
  assert.equal(reactions.observe(update("run:2", "interrupted")), "full");
  assert.equal(reactions.observe(update("run:2", "interrupted")), "none");
});

it("settles a queued run cancelled before it ran without the run-end start", () => {
  const reactions = makeRunEndReactions(10);
  assert.equal(reactions.observe(update("run:queued", "queued")), "full");
  assert.equal(reactions.observe(update("run:queued", "cancelled")), "settle");
  // A run cancelled while it was starting ended work on the thread.
  assert.equal(reactions.observe(update("run:starting", "starting")), "full");
  assert.equal(reactions.observe(update("run:starting", "cancelled")), "full");
  // A queued run that failed to start holds the queue as a run end does.
  assert.equal(reactions.observe(update("run:failed", "queued")), "full");
  assert.equal(reactions.observe(update("run:failed", "failed")), "full");
});

it("reacts in full to a run it has not seen, and forgets the oldest runs past its capacity", () => {
  const reactions = makeRunEndReactions(2);
  assert.equal(reactions.observe(update("run:a", "completed")), "full");
  assert.equal(reactions.observe(update("run:b", "completed")), "full");
  assert.equal(reactions.observe(update("run:c", "completed")), "full");
  // run:a was the oldest, so its rewrite is unknown again; run:c is still remembered.
  assert.equal(reactions.observe(update("run:a", "completed")), "full");
  assert.equal(reactions.observe(update("run:c", "completed")), "none");
});

it("settles a rewrite that changes an ended run's delegated completions, and drops one that does not", () => {
  const reactions = makeRunEndReactions(10);
  const delivery = {
    generation: 1,
    messageId: MessageId.make("message:wake"),
    taskIds: [NodeId.make("node:task")],
  };
  assert.equal(reactions.observe(update("run:parent", "completed", cohort(null))), "full");
  // A child finishing after its parent ended reserves a wake on the parent's run.
  assert.equal(reactions.observe(update("run:parent", "completed", cohort(delivery))), "settle");
  assert.equal(reactions.observe(update("run:parent", "completed", cohort(delivery))), "none");
  assert.equal(
    reactions.observe(update("run:parent", "completed", cohort(delivery, "disposed"))),
    "settle",
  );
  assert.equal(
    reactions.observe(update("run:parent", "completed", cohort(null, "disposed"))),
    "settle",
  );
});

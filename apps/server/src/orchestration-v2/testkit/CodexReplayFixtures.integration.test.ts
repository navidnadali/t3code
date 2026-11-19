import { assert, describe, it } from "@effect/vitest";
import type { OrchestrationV2DomainEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";

import { materializeReplayTranscriptWorkspace } from "@t3tools/provider-testing/replayTranscript";
import { CodexOrchestratorReplayHarness } from "../Adapters/CodexAdapterV2.testkit.ts";
import {
  findReplayFixture,
  itRunsReplayFixtures,
  runReplayFixture,
} from "./ProviderReplayFixtures.ts";

describe("Codex replay fixtures", () => {
  itRunsReplayFixtures("codex", (input) =>
    runReplayFixture({
      ...input,
      harness: CodexOrchestratorReplayHarness,
      materializeWorkspace: (transcript, workspace) =>
        Effect.succeed(materializeReplayTranscriptWorkspace(transcript, workspace)),
    }),
  );
});

// A provider transaction may group any number of queued snapshots; what it must not do is carry a
// run's lifecycle writes or reorder a run's events.
const snapshotTypes = new Set([
  "node.updated",
  "message.updated",
  "turn-item.updated",
  "plan.updated",
]);
const isSnapshotWrite = (event: OrchestrationV2DomainEvent) =>
  snapshotTypes.has(event.type) ||
  (event.type === "provider-turn.updated" && event.payload.status === "running");

it.effect(
  "persists recorded Codex snapshots through provider transactions in the real run consumer",
  () =>
    Effect.gen(function* () {
      const fixture = findReplayFixture("multi_turn")!;
      const driver = fixture.providers.find((provider) => provider.driver === "codex")!;
      // Each applied event's transaction. The commit coordinator runs one provider batch at a time,
      // and a write inside it has no transaction of its own; any other write opens its own.
      const transactionOf = new Map<string, string>();
      const providerTransactions = new Set<string>();
      let openBatch: string | undefined;
      const tracer = Tracer.make({
        span(options) {
          const span = new Tracer.NativeSpan(options);
          if (span.name === "orchestrationV2.EventSink.providerBatch") {
            openBatch = span.spanId;
            providerTransactions.add(span.spanId);
            const end = span.end.bind(span);
            span.end = (time, exit) => {
              end(time, exit);
              if (openBatch === span.spanId) openBatch = undefined;
            };
          }
          return span;
        },
      });
      const transactionKey = (span: Tracer.AnySpan | undefined): string => {
        for (let current = span; current?._tag === "Span";) {
          if (current.name === "sql.transaction") return current.spanId;
          if (current.name.startsWith("orchestrationV2.EventSink.")) break;
          current = Option.getOrUndefined(current.parent);
        }
        return openBatch ?? "outside-any-transaction";
      };
      const result = yield* runReplayFixture({
        fixtureName: fixture.name,
        buildInput: fixture.buildInput,
        driver,
        harness: CodexOrchestratorReplayHarness,
        materializeWorkspace: (transcript, workspace) =>
          Effect.succeed(materializeReplayTranscriptWorkspace(transcript, workspace)),
        wrapProjectionStore: (store) => ({
          ...store,
          apply: (event) =>
            Effect.option(Effect.currentSpan).pipe(
              Effect.tap((span) =>
                Effect.sync(() =>
                  transactionOf.set(event.id, transactionKey(Option.getOrUndefined(span))),
                ),
              ),
              Effect.andThen(store.apply(event)),
            ),
        }),
      }).pipe(Effect.withTracer(tracer), Effect.withTracerEnabled(true));

      const stored = result.storedEvents;
      const batches = new Map<string, Array<OrchestrationV2DomainEvent>>();
      for (const { event } of stored) {
        const key = transactionOf.get(event.id);
        if (key === undefined || !providerTransactions.has(key)) continue;
        batches.set(key, [...(batches.get(key) ?? []), event]);
      }
      for (const events of batches.values()) {
        // (i) A provider transaction carries snapshots only: never a terminal or another lifecycle
        // write, which commit on their own after the snapshots queued before them.
        assert.deepEqual(
          events.filter((event) => !isSnapshotWrite(event)).map((event) => event.type),
          [],
        );
        // In this one-thread fixture a provider transaction never spans a run boundary.
        assert.equal(new Set(events.map((event) => event.runId)).size, 1);
      }
      // Both turns' item snapshots (node, message, turn item) go through provider transactions.
      // A later change added each turn's running usage report there too (8 writes). Since coalescing, that
      // report, arriving in the same backlog as the turn's first report with no control between,
      // replaces it in place: one provider turn's whole state, newest wins.
      assert.equal(
        [...batches.values()].reduce((total, events) => total + events.length, 0),
        6,
      );
      // (ii) Each turn's running state is stored once, before the turn's end, and it is the newest
      // report: the usage the provider sent after the turn's text is kept, not the bare first report.
      const turnStates = new Map<
        string,
        Array<{ sequence: number; event: OrchestrationV2DomainEvent }>
      >();
      for (const { sequence, event } of stored)
        if (event.type === "provider-turn.updated")
          turnStates.set(event.payload.id, [
            ...(turnStates.get(event.payload.id) ?? []),
            { sequence, event },
          ]);
      assert.equal(turnStates.size, 2);
      for (const [turn, states] of turnStates) {
        const running = states.filter(
          ({ event }) =>
            event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        const ended = states.find(
          ({ event }) =>
            event.type === "provider-turn.updated" && event.payload.status === "completed",
        );
        assert.equal(running.length, 1, `${turn}: running state stored ${running.length} times`);
        assert.isDefined(ended);
        assert.isBelow(running[0]!.sequence, ended!.sequence);
        const usage =
          running[0]!.event.type === "provider-turn.updated"
            ? running[0]!.event.payload.tokenUsage
            : undefined;
        assert.isAbove(
          usage?.usedTokens ?? 0,
          0,
          `${turn}: the stored running state lost its usage`,
        );
      }
      // (iii) Per run, every snapshot from its provider commits before the run settles, and per
      // item the stored order is the provider's: no item goes back from a settled status.
      const settledAt = new Map<string, number>();
      for (const { sequence, event } of stored)
        if (event.type === "run.updated" && event.payload.status === "waiting" && event.runId)
          settledAt.set(event.runId, Math.min(settledAt.get(event.runId) ?? sequence, sequence));
      const itemStatuses = new Map<string, Array<string>>();
      for (const { sequence, event } of stored) {
        const key = transactionOf.get(event.id);
        const settled = event.runId === undefined ? undefined : settledAt.get(event.runId);
        if (key !== undefined && providerTransactions.has(key) && settled !== undefined)
          assert.isBelow(sequence, settled, `${event.type} committed after its run settled`);
        if (event.type === "turn-item.updated" || event.type === "node.updated") {
          const item = `${event.type}:${event.payload.id}`;
          itemStatuses.set(item, [...(itemStatuses.get(item) ?? []), event.payload.status]);
        }
      }
      for (const [key, statuses] of itemStatuses) {
        const firstSettled = statuses.findIndex(
          (status) => status !== "running" && status !== "pending",
        );
        if (firstSettled !== -1)
          assert.deepEqual(
            statuses
              .slice(firstSettled)
              .filter((status) => status === "running" || status === "pending"),
            [],
            `${key} went back to running after it settled`,
          );
      }
    }),
);

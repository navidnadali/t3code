import { assert, it } from "@effect/vitest";
import { MessageId, ProviderDriverKind, ProviderTurnId, ThreadId } from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { CurrentWaitObserver } from "@t3tools/shared/WaitObserver";

import { recordingWaitObserver } from "../observability/WaitObserver.testkit.ts";
import * as IngestLag from "./IngestLag.ts";
import * as ProviderCommitBatch from "./ProviderCommitBatch.ts";

const driver = ProviderDriverKind.make("codex");
function event(index: number, text = `delta:${index}`): ProviderAdapter.ProviderAdapterV2Event {
  const threadId = ThreadId.make("single-thread");
  const now = DateTime.makeUnsafe("2026-11-03T00:00:00Z");
  return {
    type: "message.updated",
    driver,
    message: {
      id: MessageId.make(`message:${index}`),
      threadId,
      runId: null,
      nodeId: null,
      createdBy: "agent",
      creationSource: "provider",
      role: "assistant",
      text,
      attachments: [],
      streaming: true,
      createdAt: now,
      updatedAt: now,
    },
  };
}
function snapshot(id: string, text: string, threadId = "single-thread") {
  const base = event(0, text);
  return base.type === "message.updated"
    ? {
        ...base,
        message: { ...base.message, id: MessageId.make(id), threadId: ThreadId.make(threadId) },
      }
    : base;
}
const control: ProviderAdapter.ProviderAdapterV2Event = {
  type: "background_work.updated",
  driver,
  providerTurnId: ProviderTurnId.make("turn"),
  pending: false,
};

const recordBatches = Effect.fn("test.recordProviderBatches")(function* (
  beforeCommit: Effect.Effect<void> = Effect.void,
  isViewed?: (threadId: string) => boolean,
  coalesceSnapshots = false,
) {
  const output = yield* Queue.unbounded<ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>>();
  let current: ProviderAdapter.ProviderAdapterV2Event[] = [];
  const commitBatch = yield* ProviderCommitBatch.make(
    (work) =>
      Effect.gen(function* () {
        yield* beforeCommit;
        current = [];
        yield* work;
        if (current.length > 0) yield* Queue.offer(output, current);
      }),
    true,
    undefined,
    isViewed,
    coalesceSnapshots,
  );
  const process = (raw: ProviderAdapter.ProviderAdapterV2Event) =>
    raw.type === "background_work.updated"
      ? Queue.offer(output, [raw]).pipe(Effect.asVoid)
      : Effect.sync(() => {
          current.push(raw);
        });
  return { ...commitBatch, process, output };
});

it.effect("publishes preceding snapshots before a control", () =>
  Effect.gen(function* () {
    const recorder = yield* recordBatches();
    const input = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const consumer = yield* recorder
      .consume(Stream.fromQueue(input), recorder.process, Effect.succeed(true))
      .pipe(Effect.forkScoped);
    yield* Queue.offer(input, event(1));
    assert.deepEqual(yield* Queue.take(recorder.output), [event(1)]);
    yield* Queue.offer(input, event(2));
    yield* Queue.offer(input, control);
    assert.deepEqual(yield* Queue.take(recorder.output), [event(2)]);
    assert.deepEqual(yield* Queue.take(recorder.output), [control]);
    yield* Fiber.join(consumer);
  }),
);

it.effect("publishes isolated snapshots without advancing the clock", () =>
  Effect.gen(function* () {
    const recorder = yield* recordBatches();
    const input = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    const pulled = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    yield* recorder
      .consume(
        Stream.fromQueue(input).pipe(Stream.tap((raw) => Queue.offer(pulled, raw))),
        recorder.process,
        Effect.succeed(false),
      )
      .pipe(Effect.forkScoped);
    for (const raw of [event(1), event(2)]) {
      yield* Queue.offer(input, raw);
      yield* Queue.take(pulled);
      yield* TestClock.adjust(0);
      const published = yield* Queue.poll(recorder.output);
      assert.equal(published._tag, "Some");
      if (published._tag === "Some") assert.deepEqual(published.value, [raw]);
    }
  }),
);

it.effect("bounds transactions by count and bytes, including the final partial group", () =>
  Effect.gen(function* () {
    const workloads = [
      { events: Array.from({ length: 129 }, (_, index) => event(index)) },
      { events: [event(0), event(1, "x".repeat(1024 * 1024)), event(2)] },
      {
        events: [event(0, "x".repeat(600_000)), event(1, "y".repeat(600_000)), event(2)],
      },
    ];
    for (const { events } of workloads) {
      const recorder = yield* recordBatches();
      const pulled = yield* Deferred.make<void>();
      let arrivals = 0;
      const consumer = yield* recorder
        .consume(
          Stream.fromArray(events).pipe(
            Stream.tap(() =>
              ++arrivals === events.length ? Deferred.succeed(pulled, undefined) : Effect.void,
            ),
          ),
          recorder.process,
          Effect.succeed(false),
        )
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(pulled);
      yield* TestClock.adjust(0);
      yield* recorder.barrier(Effect.void);
      yield* Fiber.join(consumer);
      const batches = yield* Queue.takeAll(recorder.output);
      assert.isTrue(batches.every((batch) => batch.length <= 64));
      if (events.length === 129) assert.isTrue(batches.some((batch) => batch.length > 1));
      assert.isTrue(
        batches.every(
          (batch) =>
            batch.length === 1 ||
            batch.reduce((bytes, raw) => bytes + Buffer.byteLength(JSON.stringify(raw)), 0) <=
              1024 * 1024,
        ),
      );
      assert.deepEqual(batches.flat(), events);
    }
  }),
);

it.effect("keeps updates immediate and ordered when batching is disabled", () =>
  Effect.gen(function* () {
    const seen: ProviderAdapter.ProviderAdapterV2Event[] = [];
    const commitBatch = yield* ProviderCommitBatch.make(() => Effect.die("must not batch"), false);
    yield* commitBatch.consume(
      Stream.make(event(1), control, event(2)),
      (raw) =>
        Effect.sync(() => {
          seen.push(raw);
        }),
      Effect.succeed(false),
    );
    assert.deepEqual(seen, [event(1), control, event(2)]);
  }),
);

it.effect.each([
  { streams: 1, updates: 10 },
  { streams: 10, updates: 1 },
  { streams: 10, updates: 10 },
])(
  "shares bounded transactions across $streams streams with $updates updates each",
  ({ streams, updates }) =>
    Effect.gen(function* () {
      const commitBatch = yield* recordBatches();
      const inputs = yield* Effect.forEach(Array.from({ length: streams }), () =>
        Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>(),
      );
      const pulled = yield* Deferred.make<void>();
      let arrivals = 0;
      const consumers = yield* Effect.forEach(inputs, (input) =>
        commitBatch
          .consume(
            Stream.fromQueue(input).pipe(
              Stream.tap(() => {
                arrivals++;
                return arrivals === streams * updates
                  ? Deferred.succeed(pulled, undefined)
                  : Effect.void;
              }),
            ),
            commitBatch.process,
            Effect.succeed(true),
          )
          .pipe(Effect.forkScoped({ startImmediately: true })),
      );
      for (let n = 0; n < updates; n++) {
        for (let stream = 0; stream < streams; stream++)
          yield* Queue.offer(inputs[stream]!, event(stream * updates + n));
      }
      yield* Deferred.await(pulled);
      yield* TestClock.adjust(0);

      yield* commitBatch.barrier(Effect.void);
      const batches: Array<ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event>> = [];
      let count = 0;
      while (count < streams * updates) {
        const batch = yield* Queue.take(commitBatch.output);
        batches.push(batch);
        count += batch.length;
      }
      assert.deepEqual(
        batches.map((batch) => batch.length),
        streams * updates > 64 ? [64, 36] : [10],
      );
      assert.equal(
        new Set(
          batches
            .flat()
            .flatMap((raw) => (raw.type === "message.updated" ? [raw.message.text] : [])),
        ).size,
        streams * updates,
      );
      for (const input of inputs) yield* Queue.offer(input, control);
      yield* Effect.forEach(consumers, Fiber.join);
    }),
);

it.effect("does not hold the shared worker while one consumer checks background work", () =>
  Effect.gen(function* () {
    const checking = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const commitBatch = yield* ProviderCommitBatch.make((work) => work, true);
    const consumer = yield* commitBatch
      .consume(
        Stream.make(control),
        () => Effect.void,
        Deferred.succeed(checking, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(true),
        ),
      )
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.await(checking);
    assert.equal(
      yield* commitBatch.barrier(Effect.succeed("another run can commit")),
      "another run can commit",
    );
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(consumer);
  }),
);

it.effect("runs a control without queueing behind another consumer's commit", () =>
  Effect.gen(function* () {
    const committing = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const commitBatch = yield* ProviderCommitBatch.make((work) => work, true);
    const busy = yield* commitBatch
      .consume(
        Stream.make(event(1)),
        () => Deferred.succeed(committing, undefined).pipe(Effect.andThen(Deferred.await(release))),
        Effect.succeed(false),
      )
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.await(committing);
    // A run's turn ends with controls in a row; each waited a full queue pass that wrote nothing.
    const handled = yield* Deferred.make<void>();
    const consumer = yield* commitBatch
      .consume(
        Stream.make(control),
        () => Deferred.succeed(handled, undefined),
        Effect.succeed(true),
      )
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* TestClock.adjust(0);
    assert.isTrue(yield* Deferred.isDone(handled));
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(consumer);
    yield* Fiber.join(busy);
  }),
);

it.effect.each(["job", "commit"] as const)(
  "isolates a %s failure at its transaction boundary",
  (mode) =>
    Effect.gen(function* () {
      const commitBatch = yield* ProviderCommitBatch.make(
        (work) =>
          mode === "commit"
            ? work.pipe(
                Effect.andThen(
                  Effect.fail(new ProviderCommitBatch.CommitBatchError({ cause: "COMMIT failed" })),
                ),
              )
            : work,
        true,
      );
      const inputs = yield* Effect.forEach([0, 1], () =>
        Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>(),
      );
      const consumers = yield* Effect.forEach(inputs, (input, index) =>
        commitBatch
          .consume(
            Stream.fromQueue(input),
            () => (mode === "job" && index === 1 ? Effect.fail("invalid update") : Effect.void),
            Effect.succeed(true),
          )
          .pipe(Effect.exit, Effect.forkScoped({ startImmediately: true })),
      );
      for (const input of inputs) yield* Queue.offer(input, event(0));
      yield* TestClock.adjust(0);
      yield* commitBatch.barrier(Effect.void);
      assert.isTrue(Exit.isFailure(yield* Fiber.join(consumers[1]!)));
      if (mode === "commit") assert.isTrue(Exit.isFailure(yield* Fiber.join(consumers[0]!)));
      else {
        assert.isUndefined(consumers[0]!.pollUnsafe());
        yield* Queue.offer(inputs[0]!, control);
        assert.isTrue(Exit.isSuccess(yield* Fiber.join(consumers[0]!)));
      }
      assert.equal(yield* commitBatch.barrier(Effect.succeed("recovered")), "recovered");
      assert.deepEqual(
        yield* commitBatch.barrier(Effect.fail("original error")).pipe(Effect.exit),
        Exit.fail("original error"),
      );
    }),
);

it.effect("keeps a transaction failure when rolling back one failed snapshot", () =>
  Effect.gen(function* () {
    const transactionError = new ProviderCommitBatch.CommitBatchError({ cause: "ROLLBACK failed" });
    const commitBatch = yield* ProviderCommitBatch.make(
      (work) => work.pipe(Effect.mapError(() => transactionError)),
      true,
    );
    const exit = yield* commitBatch
      .consume(
        Stream.concat(Stream.make(event(1)), Stream.never),
        () => Effect.fail("invalid snapshot"),
        Effect.succeed(false),
      )
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(exit));
    if (Exit.isFailure(exit)) assert.strictEqual(Cause.squash(exit.cause), transactionError);
  }),
);

it.effect(
  "allows a command holding the thread lock to commit while a provider control waits for it",
  () =>
    Effect.gen(function* () {
      const commitBatch = yield* ProviderCommitBatch.make((work) => work, true);
      const locks = yield* KeyedLock.make<ThreadId>();
      const threadId = ThreadId.make("locked-thread");
      const held = yield* Deferred.make<void>();
      const waiting = yield* Deferred.make<void>();
      const committed = yield* Deferred.make<void>();
      const command = yield* locks
        .withLock(
          threadId,
          Deferred.succeed(held, undefined).pipe(
            Effect.andThen(Deferred.await(waiting)),
            Effect.andThen(commitBatch.barrier(Deferred.succeed(committed, undefined))),
          ),
        )
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(held);
      const consumer = yield* commitBatch
        .consume(
          Stream.make(control),
          () =>
            Deferred.succeed(waiting, undefined).pipe(
              Effect.andThen(locks.withLock(threadId, Effect.void)),
            ),
          Effect.succeed(true),
        )
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(waiting);
      yield* TestClock.adjust(0);
      assert.isTrue(yield* Deferred.isDone(committed));
      yield* Fiber.join(command);
      yield* Fiber.join(consumer);
    }),
);

it.effect.each([false, true])(
  "batches work accumulated behind a busy writer with cancellation=%s",
  (cancel) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let first = true;
      const recorder = yield* recordBatches(
        Effect.gen(function* () {
          if (!first) return;
          first = false;
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(release);
        }),
      );
      const leading = yield* recorder
        .consume(Stream.make(event(-1)), recorder.process, Effect.succeed(false))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(started);
      const pulled = yield* Deferred.make<void>();
      let arrivals = 0;
      const consumers = yield* Effect.forEach(Array.from({ length: 10 }), (_, index) =>
        recorder
          .consume(
            Stream.make(event(index)).pipe(
              Stream.tap(() =>
                ++arrivals === 10 ? Deferred.succeed(pulled, undefined) : Effect.void,
              ),
            ),
            recorder.process,
            Effect.succeed(false),
          )
          .pipe(Effect.forkScoped({ startImmediately: true })),
      );
      yield* Deferred.await(pulled);
      yield* TestClock.adjust(0);
      if (cancel) yield* Fiber.interrupt(consumers[0]!);
      assert.equal(yield* Queue.size(recorder.output), 0);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(leading);
      yield* Effect.forEach(consumers.slice(cancel ? 1 : 0), Fiber.join);
      assert.deepEqual(yield* Queue.takeAll(recorder.output), [
        [event(-1)],
        Array.from({ length: cancel ? 9 : 10 }, (_, index) => event(index + (cancel ? 1 : 0))),
      ]);
    }),
);

it.effect.each(["none", "writer", "snapshots"] as const)(
  "runs another stream's control during a busy commit; splits slow backlogs only for a write waiting on the writer (%s)",
  (waitingOn) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let first = true;
      const recorder = yield* recordBatches(
        Effect.gen(function* () {
          if (!first) return;
          first = false;
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(release);
        }),
      );
      const leading = yield* recorder
        .consume(Stream.make(event(-1)), recorder.process, Effect.succeed(false))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(started);
      yield* recorder.consume(Stream.make(control), recorder.process, Effect.succeed(true));
      assert.deepEqual(yield* Queue.takeAll(recorder.output), [[control]]);

      const pulled = yield* Deferred.make<void>();
      const backlog = [event(0), event(1), event(2)];
      const slow = yield* recorder
        .consume(
          Stream.fromArray(backlog).pipe(
            Stream.tap((raw) =>
              raw === backlog.at(-1) ? Deferred.succeed(pulled, undefined) : Effect.void,
            ),
          ),
          (raw) => recorder.process(raw).pipe(Effect.andThen(TestClock.adjust("100 millis"))),
          Effect.succeed(false),
        )
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(pulled);
      yield* TestClock.adjust(0);
      // An unrelated write stands in for one waiting on the writer the batch holds.
      const writerFree = yield* Deferred.make<void>();
      const write = yield* recorder
        .barrier(
          waitingOn === "writer" ? Deferred.await(writerFree) : Effect.void,
          waitingOn === "snapshots" ? ["single-thread"] : ["other-thread"],
        )
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(leading);
      yield* Fiber.join(slow);
      yield* Deferred.succeed(writerFree, undefined);
      yield* Fiber.join(write);
      assert.deepEqual(
        yield* Queue.takeAll(recorder.output),
        // A write still waiting on its own thread's snapshots gains nothing from a shorter batch.
        waitingOn !== "writer"
          ? [[event(-1)], backlog]
          : [[event(-1)], ...backlog.map((raw) => [raw])],
      );
    }),
);

it.effect("splits a slow batch once a waiting write's own snapshot has run", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let first = true;
    const recorder = yield* recordBatches(
      Effect.gen(function* () {
        if (!first) return;
        first = false;
        yield* Deferred.succeed(started, undefined);
        yield* Deferred.await(release);
      }),
    );
    const leading = yield* recorder
      .consume(Stream.make(event(-1)), recorder.process, Effect.succeed(false))
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.await(started);
    const own = snapshot("a", "a", "thread-a");
    const others = [0, 1, 2, 3].map((index) => snapshot(`b${index}`, "b", "thread-b"));
    const pulled = yield* Deferred.make<void>();
    const slow = yield* recorder
      .consume(
        Stream.fromArray([own, ...others]).pipe(
          Stream.tap((raw) =>
            raw === others.at(-1) ? Deferred.succeed(pulled, undefined) : Effect.void,
          ),
        ),
        (raw) => recorder.process(raw).pipe(Effect.andThen(TestClock.adjust("100 millis"))),
        Effect.succeed(false),
      )
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.await(pulled);
    yield* TestClock.adjust(0);
    const write = yield* recorder
      .barrier(Queue.offer(recorder.output, [control]), ["thread-a"])
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(leading);
    yield* Fiber.join(write);
    yield* Fiber.join(slow);
    assert.deepEqual(yield* Queue.takeAll(recorder.output), [
      [event(-1)],
      [own],
      [control],
      others,
    ]);
  }),
);

const blockFirstCommit = Effect.gen(function* () {
  const started = yield* Deferred.make<void>();
  const release = yield* Deferred.make<void>();
  let first = true;
  const block = Effect.gen(function* () {
    if (!first) return;
    first = false;
    yield* Deferred.succeed(started, undefined);
    yield* Deferred.await(release);
  });
  return { started, release, block };
});

it.effect.each([
  { keys: ["a", "b", "a", "b", "a", "b"], kept: [0, 4, 5] },
  // A first appearance between two snapshots of an entity keeps the earlier snapshot.
  { keys: ["a", "b", "a", "b", "c", "a", "b"], kept: [0, 2, 3, 4, 5, 6] },
])("combines superseded snapshots queued behind a busy commit: $keys", ({ keys, kept }) =>
  Effect.gen(function* () {
    const writer = yield* blockFirstCommit;
    const recorder = yield* recordBatches(writer.block, undefined, true);
    const leading = yield* recorder
      .consume(Stream.make(event(-1)), recorder.process, Effect.succeed(false))
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.await(writer.started);
    const snapshots = keys.map((key, index) => snapshot(key, `${key}:${index}`));
    const pulled = yield* Deferred.make<void>();
    const consumer = yield* recorder
      .consume(
        Stream.fromIterable(snapshots).pipe(
          Stream.tap((raw) =>
            raw === snapshots.at(-1) ? Deferred.succeed(pulled, undefined) : Effect.void,
          ),
        ),
        recorder.process,
        Effect.succeed(false),
      )
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.await(pulled);
    yield* TestClock.adjust(0);
    yield* Deferred.succeed(writer.release, undefined);
    yield* Fiber.join(leading);
    yield* Fiber.join(consumer);
    assert.deepEqual(yield* Queue.takeAll(recorder.output), [
      [event(-1)],
      kept.map((index) => snapshots[index]!),
    ]);
  }),
);

// A run's ingestion folds its own backlog before the lane and settles a count per processed item:
// by default the lane processes every snapshot it is handed, also an entity's earlier one.
it.effect("processes every queued snapshot of an entity unless asked to combine them", () =>
  Effect.gen(function* () {
    const writer = yield* blockFirstCommit;
    const recorder = yield* recordBatches(writer.block);
    const leading = yield* recorder
      .consume(Stream.make(event(-1)), recorder.process, Effect.succeed(false))
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.await(writer.started);
    const snapshots = ["a", "b", "a", "b"].map((key, index) => snapshot(key, `${key}:${index}`));
    const pulled = yield* Deferred.make<void>();
    const consumer = yield* recorder
      .consume(
        Stream.fromIterable(snapshots).pipe(
          Stream.tap((raw) =>
            raw === snapshots.at(-1) ? Deferred.succeed(pulled, undefined) : Effect.void,
          ),
        ),
        recorder.process,
        Effect.succeed(false),
      )
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.await(pulled);
    yield* TestClock.adjust(0);
    yield* Deferred.succeed(writer.release, undefined);
    yield* Fiber.join(leading);
    yield* Fiber.join(consumer);
    assert.deepEqual(yield* Queue.takeAll(recorder.output), [[event(-1)], snapshots]);
  }),
);

it.effect("makes streams wait to read more once the shared backlog is full", () =>
  Effect.gen(function* () {
    const writer = yield* blockFirstCommit;
    const recorder = yield* recordBatches(writer.block);
    const leading = yield* recorder
      .consume(Stream.make(event(-1)), recorder.process, Effect.succeed(false))
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* Deferred.await(writer.started);
    let arrivals = 0;
    const streams = Array.from({ length: 5 }, (_, stream) =>
      Array.from({ length: 64 }, (_, n) => snapshot(`${stream}:${n}`, `${stream}:${n}`)),
    );
    const consumers = yield* Effect.forEach(streams, (snapshots) =>
      recorder
        .consume(
          Stream.fromIterable(snapshots).pipe(
            Stream.tap(() =>
              Effect.sync(() => {
                arrivals++;
              }),
            ),
          ),
          recorder.process,
          Effect.succeed(false),
        )
        .pipe(Effect.forkScoped({ startImmediately: true })),
    );
    yield* TestClock.adjust(0);
    // 256 queued, plus at most one read per stream waiting for room.
    assert.isAtMost(arrivals, 256 + streams.length);
    yield* Deferred.succeed(writer.release, undefined);
    yield* Fiber.join(leading);
    yield* Effect.forEach(consumers, Fiber.join, { discard: true });
    assert.lengthOf((yield* Queue.takeAll(recorder.output)).flat(), 1 + 5 * 64);
  }),
);

it.effect.each([false, true])(
  "orders a backlog by focus, thread kind and a finishing control with starved=%s",
  (starved) =>
    Effect.gen(function* () {
      const writer = yield* blockFirstCommit;
      const recorder = yield* recordBatches(writer.block, (threadId) => threadId === "focused");
      const leading = yield* recorder
        .consume(Stream.make(event(-1)), recorder.process, Effect.succeed(false))
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(writer.started);
      const streams = [
        { name: "subagent", delegated: true, finishing: false },
        { name: "main", delegated: false, finishing: false },
        { name: "finishing", delegated: false, finishing: true },
        { name: "focused", delegated: false, finishing: false },
      ];
      const pulled = yield* Deferred.make<void>();
      let arrivals = 0;
      const consumers = yield* Effect.forEach(streams, ({ name, delegated, finishing }) =>
        recorder
          .consume(
            // Open streams, so only the control marks a stream as finishing; EOF would too.
            Stream.concat(
              Stream.make(snapshot(name, name, name), ...(finishing ? [control] : [])),
              Stream.never,
            ).pipe(
              Stream.tap(() =>
                ++arrivals === streams.length + 1
                  ? Deferred.succeed(pulled, undefined)
                  : Effect.void,
              ),
            ),
            recorder.process,
            Effect.succeed(false),
            undefined,
            { threadId: name, delegated },
          )
          .pipe(Effect.forkScoped({ startImmediately: true })),
      );
      yield* Deferred.await(pulled);
      yield* TestClock.adjust(starved ? "30 seconds" : 0);
      yield* Deferred.succeed(writer.release, undefined);
      yield* Fiber.join(leading);
      const outputs = yield* Queue.takeN(recorder.output, 3);
      yield* Effect.forEach(consumers, Fiber.interrupt, { discard: true });
      // Waiting 30 seconds ranks a subagent like a main thread.
      const order = starved
        ? ["focused", "finishing", "subagent", "main"]
        : ["focused", "finishing", "main", "subagent"];
      assert.deepEqual(Array.from(outputs), [
        [event(-1)],
        order.map((name) => snapshot(name, name, name)),
        [control],
      ]);
    }),
);

it.effect.each(["failure", "stop"] as const)(
  "flushes the prefix and respects a source %s barrier",
  (mode) =>
    Effect.gen(function* () {
      const seen: ProviderAdapter.ProviderAdapterV2Event[] = [];
      const commitBatch = yield* ProviderCommitBatch.make((work) => work, true);
      const source =
        mode === "failure"
          ? Stream.concat(Stream.make(event(1), event(2)), Stream.fail("provider failed"))
          : Stream.make(event(1), control, event(2));
      const exit = yield* commitBatch
        .consume(
          source,
          (raw) =>
            Effect.sync(() => {
              seen.push(raw);
            }),
          Effect.succeed(mode === "stop"),
        )
        .pipe(Effect.exit);
      assert.deepEqual(seen, mode === "failure" ? [event(1), event(2)] : [event(1), control]);
      assert.equal(Exit.isFailure(exit), mode === "failure");
      assert.equal(
        yield* commitBatch.barrier(Effect.succeed("still available")),
        "still available",
      );
    }),
);

it.effect("does not force a separate commit for every consumer that ends", () =>
  Effect.gen(function* () {
    const pulled = yield* Deferred.make<void>();
    let arrivals = 0;
    const commitBatch = yield* recordBatches();
    for (let index = 0; index < 10; index++) {
      yield* commitBatch
        .consume(
          Stream.make(event(index)).pipe(
            Stream.tap(() =>
              ++arrivals === 10 ? Deferred.succeed(pulled, undefined) : Effect.void,
            ),
          ),
          commitBatch.process,
          Effect.succeed(false),
        )
        .pipe(Effect.forkScoped({ startImmediately: true }));
    }
    yield* Deferred.await(pulled);
    yield* TestClock.adjust(0);
    yield* commitBatch.barrier(Effect.void);
    assert.deepEqual(
      (yield* Queue.takeAll(commitBatch.output)).map((batch) => batch.length),
      [10],
    );
  }),
);

it.effect("settles calls made after the commit batch's scope closes", () =>
  Effect.gen(function* () {
    const commitBatch = yield* ProviderCommitBatch.make((work) => work, true).pipe(Effect.scoped);
    const exit = yield* commitBatch
      .barrier(Effect.die("must not execute after shutdown"))
      .pipe(Effect.exit);
    assert.isTrue(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
  }),
);

it.effect("reports each job's wait before its flush, the job flushing and the depth found", () => {
  const { observer, readers, reports } = recordingWaitObserver();
  return Effect.gen(function* () {
    const committing = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let commits = 0;
    const commitBatch = yield* ProviderCommitBatch.make(
      (work) =>
        commits++ === 0
          ? Deferred.succeed(committing, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(work),
            )
          : work,
      true,
    );
    const input = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
    yield* commitBatch
      .consume(Stream.fromQueue(input), () => Effect.void, Effect.succeed(false))
      .pipe(Effect.withSpan("Probe.consume"), Effect.forkScoped);
    yield* Queue.offer(input, event(1));
    yield* Deferred.await(committing);
    const command = yield* commitBatch
      .barrier(Effect.void)
      .pipe(Effect.withSpan("Probe.command"), Effect.forkScoped);
    yield* Effect.yieldNow;
    assert.equal(readers.get("ProviderCommitBatch.depth")?.(), 2);
    yield* TestClock.adjust("1 second");
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(command);
    // With nothing queued, a barrier commits at once.
    yield* commitBatch.barrier(Effect.void);

    assert.deepEqual(
      reports("depth").filter(([site]) => site === "ProviderCommitBatch.queue"),
      [
        ["ProviderCommitBatch.queue", 0],
        ["ProviderCommitBatch.queue", 1],
        ["ProviderCommitBatch.queue", 0],
      ],
    );
    // Each wait also counts under its kind of job: a provider snapshot or a barrier.
    assert.deepEqual(reports("wait"), [
      ["ProviderCommitBatch.queue", 0, undefined],
      ["ProviderCommitBatch.queue.snapshot", 0, undefined],
      ["ProviderCommitBatch.queue", 1e9, "Probe.consume"],
      ["ProviderCommitBatch.queue.barrier", 1e9, undefined],
      ["ProviderCommitBatch.queue", 0, undefined],
      ["ProviderCommitBatch.queue.barrier", 0, undefined],
    ]);
    assert.equal(readers.get("ProviderCommitBatch.depth")?.(), 0);
  }).pipe(Effect.provideService(CurrentWaitObserver, observer));
});

it.effect(
  "reports each stamped event's ingest lag from its arrival to its commit, by class",
  () => {
    const recording = recordingWaitObserver();
    const { reports } = recording;
    // The lifecycle event's lag is reported once the lane's reply returns, after `done`.
    const lifecycleReported = Deferred.makeUnsafe<void>();
    const observer: typeof recording.observer = {
      ...recording.observer,
      wait: (...args) => {
        recording.observer.wait(...args);
        if (args[0] === "ProviderIngest.lag.lifecycle") {
          Deferred.doneUnsafe(lifecycleReported, Effect.void);
        }
      },
    };
    return Effect.gen(function* () {
      const committing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let commits = 0;
      const commitBatch = yield* ProviderCommitBatch.make(
        (work) =>
          commits++ === 0
            ? Deferred.succeed(committing, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(work),
              )
            : work,
        true,
      );
      const base = event(1);
      // The adapter stamped this snapshot when the test clock started.
      const delta: ProviderAdapter.ProviderAdapterV2Event =
        base.type === "message.updated"
          ? { ...base, message: { ...base.message, updatedAt: DateTime.makeUnsafe(0) } }
          : base;
      const unstamped = event(2);
      const lifecycle: ProviderAdapter.ProviderAdapterV2Event = { ...control };
      const input = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
      const done = yield* Deferred.make<void>();
      yield* commitBatch
        .consume(
          Stream.fromQueue(input),
          (raw) =>
            IngestLag.markPersisted.pipe(
              Effect.andThen(raw === lifecycle ? Deferred.succeed(done, undefined) : Effect.void),
            ),
          Effect.succeed(false),
        )
        .pipe(Effect.forkScoped);
      IngestLag.markArrival(delta, yield* Clock.monotonicTimeNanos);
      yield* Queue.offer(input, delta);
      yield* Deferred.await(committing);
      yield* TestClock.adjust("2 seconds");
      IngestLag.markArrival(lifecycle, yield* Clock.monotonicTimeNanos);
      yield* Queue.offer(input, unstamped);
      yield* Queue.offer(input, lifecycle);
      yield* TestClock.adjust("1 second");
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(done);
      yield* Deferred.await(lifecycleReported);

      assert.deepEqual(
        reports("wait").filter(([site]) => String(site).startsWith("ProviderIngest.")),
        [
          ["ProviderIngest.lag.delta", 3e9, undefined],
          // From the adapter's own stamp, where the event carries one.
          ["ProviderIngest.lagFromAdapter.delta", 3e9, undefined],
          ["ProviderIngest.lag.lifecycle", 1e9, undefined],
        ],
      );
      // Events per commit, as the length of each flushed batch. The control takes no commit of
      // its own: it writes nothing here.
      assert.deepEqual(
        reports("depth").filter(([site]) => site === "ProviderCommitBatch.batch"),
        [
          ["ProviderCommitBatch.batch", 1],
          ["ProviderCommitBatch.batch", 1],
        ],
      );
    }).pipe(Effect.provideService(CurrentWaitObserver, observer));
  },
);

it.effect("reports ingest lag with batching off too", () => {
  const { observer, reports } = recordingWaitObserver();
  return Effect.gen(function* () {
    const commitBatch = yield* ProviderCommitBatch.make((work) => work, false);
    const raw = event(1);
    IngestLag.markArrival(raw, yield* Clock.monotonicTimeNanos);
    yield* commitBatch.consume(
      Stream.make(raw),
      () => IngestLag.markPersisted.pipe(Effect.andThen(TestClock.adjust("1 second"))),
      Effect.succeed(true),
    );
    assert.deepEqual(
      reports("wait").filter(([site]) => String(site).startsWith("ProviderIngest.lag.")),
      [["ProviderIngest.lag.delta", 1e9, undefined]],
    );
  }).pipe(Effect.provideService(CurrentWaitObserver, observer));
});

it.effect("reports ingest lag only from a consumer that persisted the event", () => {
  const { observer, reports } = recordingWaitObserver();
  return Effect.gen(function* () {
    const commitBatch = yield* ProviderCommitBatch.make((work) => work, true);
    const shared = event(1);
    IngestLag.markArrival(shared, yield* Clock.monotonicTimeNanos);
    // A shared session hands the event to every run; only its owner writes it.
    yield* commitBatch.consume(Stream.make(shared), () => Effect.void, Effect.succeed(true));
    yield* TestClock.adjust("2 seconds");
    yield* commitBatch.consume(
      Stream.make(shared),
      () => IngestLag.markPersisted,
      Effect.succeed(true),
    );
    assert.deepEqual(
      reports("wait").filter(([site]) => String(site).startsWith("ProviderIngest.lag.")),
      [["ProviderIngest.lag.delta", 2e9, undefined]],
    );
  }).pipe(Effect.provideService(CurrentWaitObserver, observer));
});

it.effect("lends a caller's slot out only while its write waits on the lane", () =>
  Effect.gen(function* () {
    const commitBatch = yield* ProviderCommitBatch.make((work) => work, true);
    const steps: Array<string> = [];
    // Lent on the fiber that writes, as the effect worker does for its execution's own fiber.
    const withLaneWait = <A, E>(effect: Effect.Effect<A, E>) =>
      Effect.withFiber((fiber) =>
        Effect.provideService(effect, ProviderCommitBatch.CurrentLaneWait, {
          owner: fiber.id,
          suspend: Effect.sync(() => steps.push("suspend")),
          resume: Effect.sync(() => steps.push("resume")),
        }),
      );
    // An idle lane runs the write at once: nothing to lend.
    yield* withLaneWait(commitBatch.barrier(Effect.sync(() => steps.push("write"))));
    assert.deepEqual(steps, ["write"]);
    steps.length = 0;

    // A held lane holds the write back: the slot goes out while it waits and comes back before the
    // write runs, also when the wait fails or is interrupted.
    const held = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    yield* commitBatch
      .consume(
        Stream.make(snapshot("held", "held")),
        () => Deferred.succeed(held, undefined).pipe(Effect.andThen(Deferred.await(release))),
        Effect.succeed(false),
      )
      .pipe(Effect.forkChild);
    yield* Deferred.await(held);
    const queued = yield* withLaneWait(
      commitBatch.barrier(Effect.sync(() => steps.push("write"))),
    ).pipe(Effect.forkChild);
    const failing = yield* withLaneWait(commitBatch.barrier(Effect.fail("write failed"))).pipe(
      Effect.exit,
      Effect.forkChild,
    );
    const interrupted = yield* withLaneWait(commitBatch.barrier(Effect.void)).pipe(
      Effect.forkChild,
    );
    yield* Effect.yieldNow;
    assert.deepEqual(steps, ["suspend", "suspend", "suspend"]);
    yield* Fiber.interrupt(interrupted);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(queued);
    assert.isTrue(Exit.isFailure(yield* Fiber.join(failing)));
    assert.deepEqual(steps, [
      "suspend",
      "suspend",
      "suspend",
      "resume",
      "resume",
      "write",
      "resume",
    ]);
    // A fiber the owner forks inherits the lane wait, not the slot: its write lends nothing.
    steps.length = 0;
    const forkedHeld = yield* Deferred.make<void>();
    const forkedRelease = yield* Deferred.make<void>();
    yield* commitBatch
      .consume(
        Stream.make(snapshot("forked", "forked")),
        () =>
          Deferred.succeed(forkedHeld, undefined).pipe(
            Effect.andThen(Deferred.await(forkedRelease)),
          ),
        Effect.succeed(false),
      )
      .pipe(Effect.forkChild);
    yield* Deferred.await(forkedHeld);
    const forked = yield* withLaneWait(
      commitBatch.barrier(Effect.sync(() => steps.push("write"))).pipe(Effect.forkChild),
    );
    yield* Effect.yieldNow;
    yield* Deferred.succeed(forkedRelease, undefined);
    yield* Fiber.join(forked);
    assert.deepEqual(steps, ["write"]);
  }),
);

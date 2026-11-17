import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import { recordingWaitObserver } from "../observability/WaitObserver.testkit.ts";
import * as IngestLag from "./IngestLag.ts";

it.effect("reports a run's backlog as what its subscription holds plus what it took ahead", () =>
  Effect.gen(function* () {
    const { observer, keyedReaders } = recordingWaitObserver();
    let held = 4;
    let buffered = 3;
    const subscription = IngestLag.observeBacklog(
      observer,
      { events: Stream.empty, close: Effect.void, pending: () => held },
      "thread-a",
      () => buffered,
    );
    assert.deepStrictEqual(
      keyedReaders.map(({ site, key, read }) => [site, key, read()]),
      [[IngestLag.BACKLOG_SITE, "thread-a", 7]],
    );
    held = 0;
    buffered = 1;
    assert.strictEqual(keyedReaders[0]!.read(), 1);
    yield* subscription.close;
    assert.lengthOf(keyedReaders, 0);
  }),
);

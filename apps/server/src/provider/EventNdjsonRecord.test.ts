import { assert, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
  boundProviderEventForLogging,
  formatEventNdjsonLine,
  serializedRecordProblem,
  serializeEventNdjsonRecord,
} from "./EventNdjsonRecord.ts";

it("serializes provider log records without an Effect runtime", () => {
  assert.deepEqual(serializeEventNdjsonRecord("canonical", { type: "content.delta" }), {
    _tag: "Skipped",
  });
  assert.deepEqual(serializeEventNdjsonRecord("native", { id: "event", ok: true }), {
    _tag: "Serialized",
    stream: "native",
    payload: '{"id":"event","ok":true}',
  });
  // Under the character budget, but escaping each NUL six-fold exceeds 64 KiB of JSON.
  assert.deepEqual(
    serializeEventNdjsonRecord("native", { id: "escaped", output: "\u0000".repeat(20_000) }),
    { _tag: "Serialized", stream: "native", payload: '{"truncated":true,"id":"escaped"}' },
  );

  const failed = serializeEventNdjsonRecord("orchestration", { id: 1n });
  assert.equal(failed._tag, "Failed");
  assert.isTrue(failed._tag === "Failed" && Schema.isSchemaError(failed.error));

  const record = { stream: "native", payload: '{"id":"event"}' };
  assert.isUndefined(serializedRecordProblem("native", record));
  assert.equal(serializedRecordProblem("canonical", record), "stream");
  assert.equal(serializedRecordProblem("native", { ...record, payload: "{}\r{}" }), "line-break");
  assert.equal(
    serializedRecordProblem("native", { ...record, payload: "é".repeat(32 * 1_024 + 1) }),
    "size",
  );

  assert.equal(
    formatEventNdjsonLine("2026-11-01T00:00:00.000Z", "orchestration", '{"id":1}'),
    '[2026-11-01T00:00:00.000Z] ORCH: {"id":1}\n',
  );
});

it("serializes a record the same whether or not its event was bounded on this thread", () => {
  const fields = (count: number) => ({
    params: Object.fromEntries(Array.from({ length: count }, (_, index) => [`k${index}`, 0])),
  });
  const sparse = (length: number) => ({ list: new Array(length), last: 0 });
  const events = [fields(1_021), fields(1_022), sparse(1_021), sparse(1_022), { id: "small" }];
  for (const event of events) {
    const bounded = { kind: "protocol", event: boundProviderEventForLogging(event) };
    // A copy, as a worker receives it: nothing was bounded on its thread.
    const copied = { kind: "protocol", event: structuredClone(bounded.event) };
    assert.deepEqual(
      serializeEventNdjsonRecord("native", bounded),
      serializeEventNdjsonRecord("native", copied),
    );
  }
});

it("measures a bounded event again after its size was used once, and only reuses it natively", () => {
  const payload = (stream: "native" | "canonical", record: unknown) => {
    const serialized = serializeEventNdjsonRecord(stream, record);
    return serialized._tag === "Serialized" ? serialized.payload : serialized._tag;
  };
  const many = Object.fromEntries(Array.from({ length: 1_030 }, (_, index) => [`k${index}`, 0]));
  const summary =
    '{"truncated":true,"kind":"protocol","event":{"truncated":true,"params":{"truncated":true}}}';

  const written: Record<string, unknown> = { params: {} };
  boundProviderEventForLogging(written);
  assert.equal(
    payload("native", { kind: "protocol", event: written }),
    '{"kind":"protocol","event":{"params":{}}}',
  );
  // Changed after its record was written, then written again without being bounded again.
  written.params = many;
  assert.equal(payload("native", { kind: "protocol", event: written }), summary);

  const elsewhere: Record<string, unknown> = { params: {} };
  boundProviderEventForLogging(elsewhere);
  elsewhere.params = many;
  assert.equal(payload("canonical", { kind: "protocol", event: elsewhere }), summary);
});

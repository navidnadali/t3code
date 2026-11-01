import { assert, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import {
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

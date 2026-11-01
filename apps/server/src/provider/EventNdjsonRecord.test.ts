import { assert, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { formatEventNdjsonLine, serializeEventNdjsonRecord } from "./EventNdjsonRecord.ts";

it("serializes provider log records without an Effect runtime", () => {
  assert.deepEqual(serializeEventNdjsonRecord("canonical", { type: "content.delta" }), {
    _tag: "Skipped",
  });
  assert.deepEqual(serializeEventNdjsonRecord("native", { id: "event", ok: true }), {
    _tag: "Serialized",
    payload: '{"id":"event","ok":true}',
  });
  // Under the character budget, but escaping each NUL six-fold exceeds 64 KiB of JSON.
  assert.deepEqual(
    serializeEventNdjsonRecord("native", { id: "escaped", output: "\u0000".repeat(20_000) }),
    { _tag: "Serialized", payload: '{"truncated":true,"id":"escaped"}' },
  );

  const failed = serializeEventNdjsonRecord("orchestration", { id: 1n });
  assert.equal(failed._tag, "Failed");
  assert.isTrue(failed._tag === "Failed" && Schema.isSchemaError(failed.error));

  assert.equal(
    formatEventNdjsonLine("2026-11-01T00:00:00.000Z", "orchestration", '{"id":1}'),
    '[2026-11-01T00:00:00.000Z] ORCH: {"id":1}\n',
  );
});

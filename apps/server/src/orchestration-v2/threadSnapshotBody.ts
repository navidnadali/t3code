import {
  OrchestrationV2ProjectedTurnItem,
  OrchestrationV2ThreadDetailSnapshot,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

const ENCODE_BATCH_ITEMS = 256;
const CHUNK_CHARS = 256 * 1024;

type Encode = (input: unknown) => Effect.Effect<unknown, Schema.SchemaError>;

const encodeSnapshot: Encode = Schema.encodeUnknownEffect(OrchestrationV2ThreadDetailSnapshot);
const encodeProjectionField = new Map<string, Encode>(
  Object.entries(OrchestrationV2ThreadProjection.fields).map(([key, schema]) => [
    key,
    Schema.encodeUnknownEffect(schema as Schema.Codec<unknown, unknown>),
  ]),
);
const encodeRowsWithoutItem: Encode = Schema.encodeUnknownEffect(
  Schema.Array(OrchestrationV2ProjectedTurnItem.mapFields(({ item: _item, ...fields }) => fields)),
);
// A row encodes its fields in schema order; a reused item matches that only as the last field.
const ROW_ITEM_IS_LAST = Object.keys(OrchestrationV2ProjectedTurnItem.fields).at(-1) === "item";

type Batches = ReadonlyArray<ReadonlyArray<unknown>>;

const batchesOf = <A>(values: ReadonlyArray<A>) =>
  Array.from({ length: Math.ceil(values.length / ENCODE_BATCH_ITEMS) }, (_, index) =>
    values.slice(index * ENCODE_BATCH_ITEMS, (index + 1) * ENCODE_BATCH_ITEMS),
  );

// Visible rows repeat local turn items; reuse their encoding instead of encoding each item twice.
// Untraced: it runs once per batch, inside the traced encode.
const encodeVisibleRows = Effect.fnUntraced(function* (
  rows: ReadonlyArray<{ readonly item: unknown }>,
  encodedItems: ReadonlyMap<unknown, unknown>,
  encodeRows: Encode,
) {
  const rests = (yield* encodeRowsWithoutItem(
    rows.map(({ item: _item, ...rest }) => rest),
  )) as ReadonlyArray<object>;
  const encoded: Array<unknown> = [];
  const missed: Array<number> = [];
  rows.forEach((row, index) => {
    const item = ROW_ITEM_IS_LAST ? encodedItems.get(row.item) : undefined;
    if (item === undefined) missed.push(index);
    encoded.push(item === undefined ? undefined : { ...rests[index], item });
  });
  if (missed.length > 0) {
    const full = (yield* encodeRows(missed.map((index) => rows[index]))) as ReadonlyArray<unknown>;
    missed.forEach((index, position) => (encoded[index] = full[position]));
  }
  return encoded;
});

const isSkipped = (value: unknown) =>
  value === undefined || typeof value === "function" || typeof value === "symbol";

// JSON.stringify calls a member's toJSON with its key before serializing it.
const toJsonValue = (value: unknown, key: string) =>
  value !== null &&
  (typeof value === "object" || typeof value === "bigint") &&
  typeof (value as { readonly toJSON?: unknown }).toJSON === "function"
    ? (value as { readonly toJSON: (key: string) => unknown }).toJSON(key)
    : value;

const hasToJson = (value: object) =>
  typeof (value as { readonly toJSON?: unknown }).toJSON === "function";

// About the JSON length of a value already through toJSON (escapes can add more); it stops early.
// Any toJSON but DateTime's counts as large, so the writer calls it once, member by member.
const exceedsChunk = (value: unknown) => {
  let budget = CHUNK_CHARS;
  const visit = (current: unknown, throughToJson: boolean): boolean => {
    if (typeof current === "string") return (budget -= current.length + 2) < 0;
    if (current === null || typeof current !== "object") return (budget -= 8) < 0;
    if (!throughToJson && hasToJson(current)) {
      return !DateTime.isDateTime(current) || (budget -= 32) < 0;
    }
    for (const member in current) {
      budget -= member.length + 4;
      const next = (current as Readonly<Record<string, unknown>>)[member];
      if (budget < 0 || visit(next, false)) return true;
    }
    return (budget -= 2) < 0;
  };
  return visit(value, true);
};

// The internal slot check JSON.stringify uses to unwrap a box, whatever the box's own properties.
const isBoxOf = (value: object, valueOf: () => unknown) => {
  try {
    valueOf.call(value);
    return true;
  } catch {
    return false;
  }
};

// Never between the halves of a surrogate pair: apart, each half encodes differently.
const sliceEnd = (text: string, start: number) => {
  const end = Math.min(start + CHUNK_CHARS, text.length);
  const last = text.charCodeAt(end - 1);
  return end < text.length && last >= 0xd800 && last <= 0xdbff ? end - 1 : end;
};

/** JSON.stringify of a value already through toJSON, in parts of about a chunk or less. */
function* jsonParts(value: unknown): Generator<string> {
  // A value already through toJSON is written without calling its toJSON again.
  const structural = value !== null && typeof value === "object" && hasToJson(value);
  if (!structural && !exceedsChunk(value)) {
    yield JSON.stringify(value);
  } else if (typeof value === "string") {
    yield '"';
    for (let start = 0; start < value.length;) {
      const end = sliceEnd(value, start);
      yield JSON.stringify(value.slice(start, end)).slice(1, -1);
      start = end;
    }
    yield '"';
  } else if (value === null || typeof value !== "object") {
    yield JSON.stringify(value);
  } else if (isBoxOf(value, String.prototype.valueOf)) {
    yield* jsonParts(String(value));
  } else if (isBoxOf(value, Number.prototype.valueOf)) {
    yield JSON.stringify(Number(value));
  } else if (isBoxOf(value, Boolean.prototype.valueOf)) {
    yield JSON.stringify(Boolean.prototype.valueOf.call(value));
  } else if (isBoxOf(value, BigInt.prototype.valueOf)) {
    yield JSON.stringify(BigInt.prototype.valueOf.call(value));
  } else if (Array.isArray(value)) {
    yield* arrayParts([value]);
  } else {
    yield* objectParts(value as Readonly<Record<string, unknown>>, () => undefined);
  }
}

function* objectParts(
  value: Readonly<Record<string, unknown>>,
  nested: (key: string) => Iterable<string> | undefined,
): Generator<string> {
  yield "{";
  let separator = "";
  for (const key of Object.keys(value)) {
    const parts = nested(key);
    const member = parts === undefined ? toJsonValue(value[key], key) : undefined;
    if (parts === undefined && isSkipped(member)) continue;
    yield separator;
    yield* jsonParts(key);
    yield ":";
    yield* parts ?? jsonParts(member);
    separator = ",";
  }
  yield "}";
}

function* arrayParts(batches: Batches): Generator<string> {
  yield "[";
  let index = 0;
  for (const batch of batches) {
    for (const element of batch) {
      if (index > 0) yield ",";
      const value = toJsonValue(element, String(index++));
      yield* isSkipped(value) ? ["null"] : jsonParts(value);
    }
  }
  yield "]";
}

function* chunks(parts: Iterable<string>): Generator<Uint8Array> {
  let pending = "";
  for (const part of parts) {
    pending += part;
    if (pending.length >= CHUNK_CHARS) {
      yield Buffer.from(pending, "utf8");
      pending = "";
    }
  }
  if (pending.length > 0) yield Buffer.from(pending, "utf8");
}

/**
 * The thread snapshot's JSON body, byte for byte `JSON.stringify` of its schema encoding. It encodes
 * in batches before returning, so a schema failure still precedes the response; the text is made
 * chunk by chunk as the body is read, so the whole document never exists at once.
 */
export const encodeThreadSnapshotBody = Effect.fn("ThreadSnapshotBody.encode")(function* (
  snapshot: OrchestrationV2ThreadDetailSnapshot,
) {
  const projection = snapshot.projection as unknown as Readonly<Record<string, unknown>>;
  const arrayKeys = [...encodeProjectionField.keys()].filter((key) =>
    Array.isArray(projection[key]),
  );
  const envelope = (yield* encodeSnapshot({
    ...snapshot,
    projection: { ...projection, ...Object.fromEntries(arrayKeys.map((key) => [key, []])) },
  })) as Readonly<Record<string, unknown>>;
  const encodedItems = new Map<unknown, unknown>();
  const arrays = new Map<string, Batches>();
  for (const key of arrayKeys) {
    const encode = encodeProjectionField.get(key)!;
    const batches: Array<ReadonlyArray<unknown>> = [];
    for (const batch of batchesOf(projection[key] as ReadonlyArray<unknown>)) {
      const encoded =
        key === "visibleTurnItems"
          ? yield* encodeVisibleRows(
              batch as ReadonlyArray<{ readonly item: unknown }>,
              encodedItems,
              encode,
            )
          : ((yield* encode(batch)) as ReadonlyArray<unknown>);
      if (key === "turnItems")
        batch.forEach((item, index) => encodedItems.set(item, encoded[index]));
      batches.push(encoded);
      yield* Effect.yieldNow;
    }
    arrays.set(key, batches);
  }
  encodedItems.clear();
  return Stream.suspend(() =>
    Stream.fromIterable(
      chunks(
        objectParts(envelope, (key) =>
          key === "projection"
            ? objectParts(envelope.projection as Readonly<Record<string, unknown>>, (field) => {
                const batches = arrays.get(field);
                return batches === undefined ? undefined : arrayParts(batches);
              })
            : undefined,
        ),
      ),
      { chunkSize: 1 },
    ),
  );
});

import {
  OrchestrationV2ProjectedTurnItem,
  OrchestrationV2ThreadDetailSnapshot,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
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

function* objectParts(
  value: Readonly<Record<string, unknown>>,
  nested: (key: string, value: unknown) => Iterable<string> | undefined,
): Generator<string> {
  yield "{";
  let separator = "";
  for (const key of Object.keys(value)) {
    const parts = nested(key, value[key]);
    if (parts !== undefined) {
      yield `${separator}${JSON.stringify(key)}:`;
      yield* parts;
      separator = ",";
      continue;
    }
    // A one-key object keeps JSON.stringify's own rules: toJSON(key), and no key for undefined.
    const member = JSON.stringify({ [key]: value[key] }).slice(1, -1);
    if (member === "") continue;
    yield `${separator}${member}`;
    separator = ",";
  }
  yield "}";
}

function* arrayParts(batches: Batches): Generator<string> {
  yield "[";
  let separator = "";
  for (const batch of batches) {
    for (const element of batch) {
      // A one-element array keeps JSON.stringify's array rules, such as null for undefined.
      yield `${separator}${JSON.stringify([element]).slice(1, -1)}`;
      separator = ",";
    }
  }
  yield "]";
}

// Never between the halves of a surrogate pair, which would encode as two replacement characters.
const chunkEnd = (text: string) => {
  const last = text.charCodeAt(CHUNK_CHARS - 1);
  return last >= 0xd800 && last <= 0xdbff ? CHUNK_CHARS - 1 : CHUNK_CHARS;
};

function* chunks(parts: Iterable<string>): Generator<Uint8Array> {
  let pending = "";
  for (const part of parts) {
    pending += part;
    while (pending.length >= CHUNK_CHARS) {
      const end = chunkEnd(pending);
      yield Buffer.from(pending.slice(0, end), "utf8");
      pending = pending.slice(end);
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
      if (key === "turnItems") batch.forEach((item, index) => encodedItems.set(item, encoded[index]));
      batches.push(encoded);
      yield* Effect.yieldNow;
    }
    arrays.set(key, batches);
  }
  encodedItems.clear();
  return Stream.suspend(() =>
    Stream.fromIterable(
      chunks(
        objectParts(envelope, (key, value) =>
          key === "projection"
            ? objectParts(value as Readonly<Record<string, unknown>>, (field) => {
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

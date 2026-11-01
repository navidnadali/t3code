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

// The endpoint encodes its success with toCodecJson (HttpApiEndpoint): values come out as JSON.
const encodeJson = (schema: Schema.Top): Encode =>
  Schema.encodeUnknownEffect(
    Schema.toCodecJson(schema) as unknown as Schema.Codec<unknown, unknown>,
  );

const encodeSnapshot = encodeJson(OrchestrationV2ThreadDetailSnapshot);
const encodeProjectionField = new Map<string, Encode>(
  Object.entries(OrchestrationV2ThreadProjection.fields).map(([key, schema]) => [
    key,
    encodeJson(schema),
  ]),
);
const encodeRowsWithoutItem = encodeJson(
  Schema.Array(OrchestrationV2ProjectedTurnItem.mapFields(({ item: _item, ...fields }) => fields)),
);
// A row encodes its fields in schema order; a reused item matches that only as the last field.
const ROW_ITEM_IS_LAST = Object.keys(OrchestrationV2ProjectedTurnItem.fields).at(-1) === "item";

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

type Json =
  | null
  | boolean
  | number
  | string
  | ReadonlyArray<Json>
  | { readonly [key: string]: Json };
type Container = ReadonlyArray<Json> | { readonly [key: string]: Json };

const isContainer = (value: Json): value is Container =>
  typeof value === "object" && value !== null;

const childrenOf = (container: Container): ReadonlyArray<Json> =>
  Array.isArray(container) ? container : Object.values(container);

// Deeper values are written level by level: JSON.stringify recurses, and a deep enough one throws.
const MAX_NATIVE_DEPTH = 1_000;

// Whether to write it level by level: about its JSON length (escapes can add more) exceeds a chunk,
// or it nests too deep for JSON.stringify. Stops as soon as either holds, so it costs a chunk at most.
const tooLargeForOneString = (root: Json) => {
  let budget = CHUNK_CHARS;
  const pending: Array<readonly [Json, number]> = [[root, 0]];
  while (pending.length > 0 && budget >= 0) {
    const [value, depth] = pending.pop()!;
    if (typeof value === "string") budget -= value.length + 2;
    else if (!isContainer(value)) budget -= 8;
    else if (depth >= MAX_NATIVE_DEPTH) return true;
    else {
      const keys = Array.isArray(value) ? [] : Object.keys(value);
      const children = childrenOf(value);
      budget -= children.length + 2;
      for (let index = 0; index < children.length && budget >= 0; index++) {
        budget -= (keys[index]?.length ?? 0) + 2;
        pending.push([children[index]!, depth + 1]);
      }
    }
  }
  return budget < 0;
};

// Each container's JSON length (before escapes, capped just above a chunk) and nesting height, in
// one post-order pass.
const containerSizes = (root: Container) => {
  const sizes = new Map<Container, { readonly length: number; readonly height: number }>();
  const stack = [{ container: root, children: childrenOf(root), index: 0 }];
  while (stack.length > 0) {
    const frame = stack.at(-1)!;
    const child = frame.children[frame.index];
    if (child !== undefined && isContainer(child) && !sizes.has(child)) {
      stack.push({ container: child, children: childrenOf(child), index: 0 });
      continue;
    }
    if (frame.index++ < frame.children.length) continue;
    stack.pop();
    let length = 2 + frame.children.length;
    let height = 1;
    if (!Array.isArray(frame.container)) {
      for (const key of Object.keys(frame.container)) length += key.length + 2;
    }
    for (const value of frame.children) {
      if (typeof value === "string") length += value.length + 2;
      else if (!isContainer(value)) length += 8;
      else {
        const size = sizes.get(value)!;
        length += size.length;
        height = Math.max(height, size.height + 1);
      }
    }
    sizes.set(frame.container, { length: Math.min(length, CHUNK_CHARS + 1), height });
  }
  return sizes;
};

// Never between the halves of a surrogate pair: apart, each half encodes differently.
const sliceEnd = (text: string, start: number) => {
  const end = Math.min(start + CHUNK_CHARS, text.length);
  const last = text.charCodeAt(end - 1);
  return end < text.length && last >= 0xd800 && last <= 0xdbff ? end - 1 : end;
};

function* stringParts(text: string): Generator<string> {
  if (text.length <= CHUNK_CHARS) {
    yield JSON.stringify(text);
    return;
  }
  yield '"';
  for (let start = 0; start < text.length;) {
    const end = sliceEnd(text, start);
    yield JSON.stringify(text.slice(start, end)).slice(1, -1);
    start = end;
  }
  yield '"';
}

const isOneString = ({ length, height }: { readonly length: number; readonly height: number }) =>
  length <= CHUNK_CHARS && height <= MAX_NATIVE_DEPTH;

/** JSON.stringify of a JSON value in parts of about a chunk or less, without recursion. */
function* jsonParts(root: Json): Generator<string> {
  if (!tooLargeForOneString(root)) {
    yield JSON.stringify(root);
    return;
  }
  if (!isContainer(root)) {
    yield* stringParts(root as string);
    return;
  }
  const sizes = containerSizes(root);
  const stack: Array<{
    readonly container: Container;
    readonly keys: ReadonlyArray<string> | undefined;
    readonly children: ReadonlyArray<Json>;
    index: number;
  }> = [];
  let next: Json | undefined = root;
  while (true) {
    if (next !== undefined) {
      if (typeof next === "string") yield* stringParts(next);
      else if (!isContainer(next) || isOneString(sizes.get(next)!)) yield JSON.stringify(next);
      else {
        const keys = Array.isArray(next) ? undefined : Object.keys(next);
        yield keys === undefined ? "[" : "{";
        stack.push({ container: next, keys, children: childrenOf(next), index: 0 });
      }
      next = undefined;
    }
    const frame = stack.at(-1);
    if (frame === undefined) return;
    if (frame.index === frame.children.length) {
      stack.pop();
      yield frame.keys === undefined ? "]" : "}";
      continue;
    }
    if (frame.index > 0) yield ",";
    if (frame.keys !== undefined) {
      yield* stringParts(frame.keys[frame.index]!);
      yield ":";
    }
    next = frame.children[frame.index++]!;
  }
}

function* objectParts(
  value: Readonly<Record<string, Json>>,
  memberParts: (key: string, member: Json) => Iterable<string>,
): Generator<string> {
  yield "{";
  let separator = "";
  for (const key of Object.keys(value)) {
    yield separator;
    yield* stringParts(key);
    yield ":";
    yield* memberParts(key, value[key]!);
    separator = ",";
  }
  yield "}";
}

function* elementsParts(elements: ReadonlyArray<Json>): Generator<string> {
  yield "[";
  for (let index = 0; index < elements.length; index++) {
    if (index > 0) yield ",";
    yield* jsonParts(elements[index]!);
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
 * The thread snapshot's body, byte for byte what HttpApiBuilder sends: `JSON.stringify` of its
 * toCodecJson encoding. It encodes in batches before returning, so a schema failure still precedes
 * the response; the text is made chunk by chunk as the body is read.
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
  })) as Readonly<Record<string, Json>>;
  const encodedItems = new Map<unknown, unknown>();
  const arrays = new Map<string, ReadonlyArray<Json>>();
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
    arrays.set(key, batches.flat() as ReadonlyArray<Json>);
  }
  encodedItems.clear();
  // The encoded arrays replace the envelope's empty ones, in the envelope's key order.
  return Stream.suspend(() =>
    Stream.fromIterable(
      chunks(
        objectParts(envelope, (key, value) =>
          key === "projection"
            ? objectParts(value as Readonly<Record<string, Json>>, (field, member) => {
                const elements = arrays.get(field);
                return elements === undefined ? jsonParts(member) : elementsParts(elements);
              })
            : jsonParts(value),
        ),
      ),
      { chunkSize: 1 },
    ),
  );
});

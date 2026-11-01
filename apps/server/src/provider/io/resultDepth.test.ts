import { assert, it } from "@effect/vitest";

import { nestsDeeperThan } from "./resultDepth.ts";

const nest = (depth: number, leaf: unknown, array: boolean): unknown => {
  let value = leaf;
  for (let level = 0; level < depth; level++) value = array ? [value] : { a: value };
  return value;
};

it("counts nested objects and arrays, not primitives, and stops at the limit", () => {
  for (const array of [false, true]) {
    assert.isFalse(nestsDeeperThan(nest(3, "leaf", array), 3));
    assert.isTrue(nestsDeeperThan(nest(4, "leaf", array), 3));
  }
  assert.isFalse(nestsDeeperThan("text", 0));
  assert.isFalse(nestsDeeperThan(null, 0));
  assert.isTrue(nestsDeeperThan({}, 0));
  // Wide is not deep; the deepest branch counts wherever it is.
  assert.isFalse(nestsDeeperThan({ a: [1, 2, 3], b: { c: "x" }, d: Array(1000).fill({}) }, 3));
  assert.isTrue(nestsDeeperThan([{}, { a: [{ b: {} }] }, {}], 4));
  assert.isTrue(nestsDeeperThan(nest(30_000, 1, true), 1000));
});

import { describe, expect, it } from "vite-plus/test";

import { extendedFindings } from "./fixture/extended.ts";
import { nativeId } from "./fixture/ids.ts";
import { scanDecoded } from "./fixture/secrets.ts";

const BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const draws = (text: string) =>
  [...text].map((character) => (BASE62.indexOf(character) + 0.5) / 62);
const sequence = (values: ReadonlyArray<number>) => {
  let at = 0;
  return () => values[at++ % values.length] as number;
};

/** What either pattern layer reports for an id inside synth's composite shapes. */
const findings = (id: string) => {
  const rows: Array<string> = [];
  for (const text of [`${id}:thinking:0`, `node:provider:claudeAgent:native-item:${id}`]) {
    extendedFindings(text, (row) => rows.push(row));
    rows.push(...scanDecoded(text, { entropy: false }));
  }
  return rows;
};

describe("synthetic native ids", () => {
  // Random base62 can spell `Auth`, `token` or `DSN`; inside a composite id followed by `:` either
  // pattern layer reads that as a credential, so synth's self-check failed from scale 6 up.
  it("never spell a credential word, and stay deterministic", () => {
    const values = [
      ...draws("AuthXyzTokenQ1w"),
      ...draws("Pa55wdSecretZ9x"),
      ...draws("uh3rxsUtYiDSNq"),
      ...draws("b".repeat(42)),
    ];
    const first = nativeId("toolu", sequence(values), 7);
    expect(findings(first)).toEqual([]);
    expect(first).toMatch(/^toolu_01[0-9A-Za-z]{22}$/);
    expect(nativeId("toolu", sequence(values), 7)).toBe(first);
    // A counter whose base62 tail spells `Auth`: redrawing the random part cannot fix the tail.
    const spelling = 10 + 56 * 62 + 55 * 62 ** 2 + 43 * 62 ** 3;
    expect(findings(nativeId("toolu", sequence(values), spelling))).toEqual([]);
  });

  it("spell nothing either layer reads across 100,000 seeded draws", () => {
    let state = 1;
    const random = () => {
      state = (state * 48271) % 2147483647;
      return state / 2147483647;
    };
    const flagged: Array<string> = [];
    for (let counter = 0; counter < 100_000; counter++) {
      const id = nativeId(counter % 2 === 0 ? "msg" : "toolu", random, counter);
      if (findings(id).length > 0) flagged.push(id);
    }
    expect(flagged).toEqual([]);
  });
});

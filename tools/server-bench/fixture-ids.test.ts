import { describe, expect, it } from "vite-plus/test";

import { nativeId } from "./fixture/ids.ts";

describe("synthetic native ids", () => {
  // Random base62 can spell `Auth` or `token`; inside a composite id followed by `:` the pattern
  // layer reads that as a credential assignment, so synth's self-check failed from scale 6 up.
  it("never spell a credential word, and stay deterministic", () => {
    const BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    const draws = (text: string) =>
      [...text].map((character) => (BASE62.indexOf(character) + 0.5) / 62);
    const sequence = [
      ...draws("AuthXyzTokenQ1w"),
      ...draws("Pa55wdSecretZ9x"),
      ...draws("b".repeat(42)),
    ];
    const random = () => {
      let at = 0;
      return () => sequence[at++ % sequence.length] as number;
    };
    const first = nativeId("toolu", random(), 7);
    expect(first).not.toMatch(/auth|token|secret|passw|pwd|credential|api_?key|access_?key/i);
    expect(first).toMatch(/^toolu_01[0-9A-Za-z]{22}$/);
    expect(nativeId("toolu", random(), 7)).toBe(first);
  });
});

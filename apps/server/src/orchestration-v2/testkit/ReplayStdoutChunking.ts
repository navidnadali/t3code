import * as Context from "effect/Context";

/** Replays deliver each provider stdout frame as these chunks; without it, as one chunk. */
export class ReplayStdoutChunking extends Context.Service<
  ReplayStdoutChunking,
  {
    /** One splitter per provider process; the same seed splits the same frames the same way. */
    readonly makeSplitter: () => (frame: Uint8Array) => ReadonlyArray<Uint8Array>;
  }
>()("t3/orchestration-v2/testkit/ReplayStdoutChunking") {}

function mulberry32(seed: number) {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const withCrlf = (frame: Uint8Array) => {
  const bytes = new Uint8Array(frame.length + 1);
  bytes.set(frame.subarray(0, -1));
  bytes[frame.length - 1] = 0x0d;
  bytes[frame.length] = 0x0a;
  return bytes;
};

// A replay waits for the client between frames, so splits fall inside each frame: a seeded share
// end in CRLF, a frame with a multi-byte character is split inside one, and a CRLF frame is often
// split between CR and LF.
export const seededStdoutChunking = (seed: number) =>
  ReplayStdoutChunking.of({
    makeSplitter: () => {
      const random = mulberry32(seed);
      return (frame) => {
        const bytes = frame.at(-1) === 0x0a && random() < 0.3 ? withCrlf(frame) : frame;
        if (bytes.length < 2) return [bytes];
        const midUtf8: Array<number> = [];
        for (let index = 1; index < bytes.length; index += 1) {
          if ((bytes[index]! & 0xc0) === 0x80) midUtf8.push(index);
        }
        const points = new Set<number>();
        if (midUtf8.length > 0) points.add(midUtf8[Math.floor(random() * midUtf8.length)]!);
        if (bytes.at(-2) === 0x0d && random() < 0.5) points.add(bytes.length - 1);
        for (let count = Math.floor(random() * 3); count > 0; count -= 1) {
          points.add(1 + Math.floor(random() * (bytes.length - 1)));
        }
        const sorted = [...points].toSorted((a, b) => a - b);
        return [0, ...sorted].map((start, index) =>
          bytes.subarray(start, sorted[index] ?? bytes.length),
        );
      };
    },
  });

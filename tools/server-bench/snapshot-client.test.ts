import { describe, expect, it } from "vite-plus/test";

import { parseOptions } from "./snapshot-client.ts";

describe("parseOptions", () => {
  it("defaults to the bot's cadence and timeout with the body left unread", () => {
    expect(parseOptions(["--url", "http://127.0.0.1:3773/", "--thread", "t-1"])).toEqual({
      url: "http://127.0.0.1:3773",
      threadId: "t-1",
      intervalMs: 300_000,
      timeoutMs: 23_000,
      timeoutMaxMs: 23_000,
      read: "none",
      acceptEncoding: "gzip, deflate",
      count: null,
      protocol: null,
      out: null,
      tokenFile: null,
    });
  });

  it("reads durations in ms, s and m and rejects bad values", () => {
    const options = parseOptions([
      "--url=http://h",
      "--thread=t",
      "--interval=2m",
      "--timeout=500ms",
      "--read=full",
      "--count=3",
    ]);
    expect([options.intervalMs, options.timeoutMs, options.read, options.count]).toEqual([
      120_000,
      500,
      "full",
      3,
    ]);
    expect(() => parseOptions(["--url=http://h"])).toThrow(/--thread are required/);
    expect(() => parseOptions(["--url=http://h", "--thread=t", "--read=some"])).toThrow(/--read/);
    expect(() => parseOptions(["--url=http://h", "--thread=t", "--count=0"])).toThrow(/positive/);
    expect(parseOptions(["--url=http://h", "--thread=t", "--timeout=2s-9s"])).toMatchObject({
      timeoutMs: 2_000,
      timeoutMaxMs: 9_000,
    });
    expect(() => parseOptions(["--url=http://h", "--thread=t", "--timeout=9s-2s"])).toThrow(
      /range/,
    );
    expect(() => parseOptions(["--url=http://h", "--thread=t", "--timeout=soon"])).toThrow(
      /duration/,
    );
  });
});

import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  CallError,
  classifyFetchError,
  FatalError,
  parseMcpPayloads,
  RpcSocket,
} from "./protocol.ts";

describe("parseMcpPayloads", () => {
  it("reads a JSON body", () => {
    expect(parseMcpPayloads("application/json", '{"jsonrpc":"2.0","id":"a"}')).toEqual([
      { jsonrpc: "2.0", id: "a" },
    ]);
    expect(parseMcpPayloads("application/json", "  ")).toEqual([]);
  });

  it("reads every data event of a server-sent event stream", () => {
    const body =
      'event: message\r\ndata: {"id":"1"}\r\n\r\n: keep-alive\n\ndata: {"id":\ndata: "2"}\n\n';
    expect(parseMcpPayloads("text/event-stream; charset=utf-8", body)).toEqual([
      { id: "1" },
      { id: "2" },
    ]);
  });
});

describe("classifyFetchError", () => {
  const withCode = (code: string) =>
    new TypeError("fetch failed", { cause: Object.assign(new Error(code), { code }) });

  it("treats a refused or reset connection as a lost server", () => {
    for (const code of ["ECONNREFUSED", "ECONNRESET", "UND_ERR_SOCKET"]) {
      const error = classifyFetchError(withCode(code), "GET /api/orchestration/shell");
      expect(error).toBeInstanceOf(FatalError);
      expect((error as FatalError).serverLost).toBe(true);
    }
  });

  it("counts a timeout or any other failure as one failed call", () => {
    const timeout = new DOMException("The operation was aborted due to timeout", "TimeoutError");
    expect(classifyFetchError(timeout, "MCP request")).toEqual(
      new CallError("MCP request: timed out"),
    );
    expect(classifyFetchError(withCode("ERR_INVALID_URL"), "GET /")).toBeInstanceOf(CallError);
  });
});

class FakeWebSocket extends EventTarget {
  static readonly OPEN = 1;
  static last: FakeWebSocket | undefined;
  readyState = 0;
  constructor() {
    super();
    FakeWebSocket.last = this;
  }
  send(): void {}
  close(): void {
    this.readyState = 3;
  }
  serverSays(message: object): void {
    this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(message) }));
  }
}

describe("RpcSocket", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  // The server answers a frame it cannot encode with a Defect that names no request.
  it("ends its open streams when the server sends a Defect", async () => {
    vi.stubGlobal("WebSocket", FakeWebSocket);
    const opening = RpcSocket.open("ws://bench.invalid/ws", 1_000);
    const fake = FakeWebSocket.last!;
    fake.readyState = FakeWebSocket.OPEN;
    fake.dispatchEvent(new Event("open"));
    const socket = await opening;
    const ends: Array<string | null> = [];
    const lost: Array<string> = [];
    socket.onConnectionLost((error) => lost.push(error.message));
    socket.stream(
      "orchestration.subscribeThread",
      { threadId: "t" },
      {
        onValues: () => undefined,
        onEnd: (error) => ends.push(error?.message ?? null),
      },
    );

    fake.serverSays({
      _tag: "Defect",
      defect: { name: "RangeError", message: "Invalid string length" },
    });

    expect(lost).toHaveLength(1);
    expect(ends).toEqual([lost[0]]);
    socket.close();
  });
});

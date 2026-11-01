import { describe, expect, it } from "vite-plus/test";

import { CallError, classifyFetchError, FatalError, parseMcpPayloads } from "./protocol.ts";

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

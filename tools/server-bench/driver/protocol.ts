// @effect-diagnostics globalFetch:off globalTimers:off preferSchemaOverJson:off - A dependency-free load client that speaks the wire protocol directly.
/**
 * Wire clients for the bench driver. They speak the server's HTTP, MCP and WebSocket RPC
 * protocols without decoding payloads into schemas, so one driver process can stand in for
 * many clients without becoming the bottleneck.
 */

/** The run cannot produce a valid table; `serverLost` when the server died or stopped answering. */
export class FatalError extends Error {
  override readonly name = "FatalError";
  readonly serverLost: boolean;

  constructor(message: string, options: { readonly serverLost?: boolean } = {}) {
    super(message);
    this.serverLost = options.serverLost === true;
  }
}

const serverLost = { serverLost: true } as const;

/** One call failed; it counts against the error budget but the run continues. */
export class CallError extends Error {
  override readonly name = "CallError";
  /** The HTTP status that failed the call, when there was one. */
  readonly status: number | undefined;

  constructor(message: string, options: { readonly status?: number } = {}) {
    super(message);
    this.status = options.status;
  }
}

const FATAL_NETWORK_CODES = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "EPIPE",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "UND_ERR_SOCKET",
  "UND_ERR_CLOSED",
]);

function networkCode(error: unknown): string | undefined {
  let current: unknown = error;
  for (let depth = 0; depth < 4 && current instanceof Error; depth++) {
    const code = (current as { code?: unknown }).code;
    if (typeof code === "string") return code;
    current = current.cause;
  }
  return undefined;
}

/** Maps a fetch rejection: a refused or reset connection is fatal, a timeout is one failed call. */
export function classifyFetchError(error: unknown, what: string): FatalError | CallError {
  if (error instanceof FatalError || error instanceof CallError) return error;
  const code = networkCode(error);
  if (code !== undefined && FATAL_NETWORK_CODES.has(code)) {
    return new FatalError(`${what}: server unreachable (${code})`, serverLost);
  }
  if (error instanceof Error && (error.name === "TimeoutError" || error.name === "AbortError")) {
    return new CallError(`${what}: timed out`);
  }
  return new CallError(`${what}: ${error instanceof Error ? error.message : String(error)}`);
}

export interface HttpResult {
  readonly status: number;
  readonly text: string;
  readonly bytes: number;
  readonly headers: Headers;
}

export interface HttpTarget {
  readonly origin: string;
  readonly token: string;
  readonly timeoutMs: number;
  readonly protocolVersion: number;
}

export async function httpRequest(
  target: HttpTarget,
  path: string,
  init: { readonly method?: string; readonly body?: string; readonly form?: URLSearchParams } = {},
): Promise<HttpResult> {
  const headers: Record<string, string> = {
    authorization: `Bearer ${target.token}`,
    "x-t3-orchestration-protocol": String(target.protocolVersion),
  };
  if (init.body !== undefined) headers["content-type"] = "application/json";
  try {
    const response = await fetch(`${target.origin}${path}`, {
      method: init.method ?? (init.body === undefined && init.form === undefined ? "GET" : "POST"),
      headers,
      body: init.form ?? init.body ?? null,
      signal: AbortSignal.timeout(target.timeoutMs),
    });
    const text = await response.text();
    return {
      status: response.status,
      text,
      bytes: Buffer.byteLength(text),
      headers: response.headers,
    };
  } catch (error) {
    throw classifyFetchError(error, `${init.method ?? "GET"} ${path.split("?")[0]}`);
  }
}

export async function httpJson<T>(
  target: HttpTarget,
  path: string,
  init?: Parameters<typeof httpRequest>[2],
): Promise<{ readonly value: T; readonly bytes: number }> {
  const result = await httpRequest(target, path, init);
  if (result.status < 200 || result.status >= 300) {
    throw new CallError(
      `${path.split("?")[0]} answered ${result.status}: ${result.text.slice(0, 300)}`,
    );
  }
  return { value: JSON.parse(result.text) as T, bytes: result.bytes };
}

/** JSON-RPC payloads of an MCP streamable-HTTP response body, JSON or server-sent events. */
export function parseMcpPayloads(contentType: string, text: string): Array<unknown> {
  if (text.trim().length === 0) return [];
  if (!contentType.includes("text/event-stream")) return [JSON.parse(text)];
  const payloads: Array<unknown> = [];
  for (const event of text.split(/\r?\n\r?\n/u)) {
    const data = event
      .split(/\r?\n/u)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice("data:".length).trimStart())
      .join("\n");
    if (data.length > 0) payloads.push(JSON.parse(data));
  }
  return payloads;
}

interface JsonRpcEnvelope {
  readonly id?: unknown;
  readonly result?: { readonly isError?: boolean; readonly content?: unknown };
  readonly error?: unknown;
}

const MCP_PROTOCOL_VERSION = "2025-06-18";

/** One initialized MCP session on the server's MCP endpoint. */
export class McpSession {
  private sessionId: string | null = null;
  private protocolVersion: string | null = null;
  private nextId = 0;
  private readonly endpoint: string;
  private readonly authorization: string;
  private readonly timeoutMs: number;

  /** `authorization` is the whole header value, e.g. `Bearer <token>`. */
  constructor(endpoint: string, authorization: string, timeoutMs: number) {
    this.endpoint = endpoint;
    this.authorization = authorization;
    this.timeoutMs = timeoutMs;
  }

  private headers(): Record<string, string> {
    return {
      accept: "application/json, text/event-stream",
      authorization: this.authorization,
      "content-type": "application/json",
      ...(this.sessionId === null ? {} : { "mcp-session-id": this.sessionId }),
      ...(this.protocolVersion === null ? {} : { "mcp-protocol-version": this.protocolVersion }),
    };
  }

  private async post(message: object): Promise<{ payloads: Array<unknown>; bytes: number }> {
    let response: Response;
    let text: string;
    try {
      response = await fetch(this.endpoint, {
        method: "POST",
        headers: this.headers(),
        body: JSON.stringify(message),
        signal: AbortSignal.timeout(this.timeoutMs),
      });
      text = await response.text();
    } catch (error) {
      throw classifyFetchError(error, "MCP request");
    }
    this.sessionId = response.headers.get("mcp-session-id") ?? this.sessionId;
    if (!response.ok) {
      throw new CallError(`MCP answered ${response.status}: ${text.slice(0, 300)}`, {
        status: response.status,
      });
    }
    return {
      payloads: parseMcpPayloads(response.headers.get("content-type") ?? "", text),
      bytes: Buffer.byteLength(text),
    };
  }

  private async request(method: string, params: unknown) {
    const id = `bench-${this.nextId++}`;
    const { payloads, bytes } = await this.post({ jsonrpc: "2.0", id, method, params });
    const envelope = payloads.find(
      (payload): payload is JsonRpcEnvelope =>
        typeof payload === "object" && payload !== null && (payload as JsonRpcEnvelope).id === id,
    );
    if (envelope === undefined) throw new CallError(`MCP ${method}: no response`);
    if (envelope.error !== undefined) {
      throw new CallError(`MCP ${method}: ${JSON.stringify(envelope.error).slice(0, 300)}`);
    }
    return { envelope, bytes };
  }

  async initialize(clientName: string): Promise<void> {
    const { envelope } = await this.request("initialize", {
      protocolVersion: MCP_PROTOCOL_VERSION,
      capabilities: {},
      clientInfo: { name: clientName, version: "0.0.0" },
    });
    const version = (envelope.result as { protocolVersion?: unknown } | undefined)?.protocolVersion;
    this.protocolVersion = typeof version === "string" ? version : MCP_PROTOCOL_VERSION;
    await this.post({ jsonrpc: "2.0", method: "notifications/initialized" });
  }

  /** Resolves with the response size; a tool-level failure (`isError`) is a CallError. */
  async callTool(name: string, args: Record<string, unknown>): Promise<number> {
    const { envelope, bytes } = await this.request("tools/call", { name, arguments: args });
    if (envelope.result?.isError === true) {
      throw new CallError(`${name}: ${JSON.stringify(envelope.result.content).slice(0, 300)}`);
    }
    return bytes;
  }

  async close(): Promise<void> {
    if (this.sessionId === null) return;
    await fetch(this.endpoint, {
      method: "DELETE",
      headers: this.headers(),
      signal: AbortSignal.timeout(this.timeoutMs),
    })
      .then((response) => response.body?.cancel())
      .catch(() => undefined);
  }
}

interface StreamHandlers {
  readonly onValues: (values: ReadonlyArray<unknown>, bytes: number) => void;
  readonly onEnd: (error: CallError | null) => void;
}

type Pending =
  | {
      readonly kind: "call";
      readonly resolve: (value: unknown) => void;
      readonly reject: (error: Error) => void;
      readonly timer: ReturnType<typeof setTimeout>;
    }
  | ({ readonly kind: "stream" } & StreamHandlers);

interface ServerMessage {
  readonly _tag?: string;
  readonly requestId?: string | number;
  readonly values?: ReadonlyArray<unknown>;
  readonly exit?: { readonly _tag: string; readonly value?: unknown; readonly cause?: unknown };
  readonly defect?: unknown;
  readonly error?: unknown;
}

export function describeFailureCause(cause: unknown): string {
  const text = JSON.stringify(cause) ?? String(cause);
  return text.length > 300 ? `${text.slice(0, 300)}…` : text;
}

const PING_INTERVAL_MS = 5_000;

/**
 * One WebSocket speaking Effect RPC's JSON protocol (`Request`, `Chunk`, `Ack`, `Exit`). Chunks
 * are acknowledged as soon as they are parsed, the way a client that keeps up would.
 */
export class RpcSocket {
  private readonly socket: WebSocket;
  private readonly pending = new Map<string, Pending>();
  private nextId = 0;
  private finished = false;
  private pinger: ReturnType<typeof setInterval> | undefined;
  /** When the oldest ping still waiting for any answer was sent. */
  private unansweredPingAt: number | null = null;
  private onLost: (error: FatalError) => void = () => undefined;

  private constructor(socket: WebSocket) {
    this.socket = socket;
  }

  static open(url: string, timeoutMs: number): Promise<RpcSocket> {
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(url);
      const client = new RpcSocket(socket);
      const timer = setTimeout(() => {
        socket.close();
        reject(new CallError("WebSocket open timed out"));
      }, timeoutMs);
      socket.addEventListener("open", () => {
        clearTimeout(timer);
        client.pinger = setInterval(() => client.ping(), PING_INTERVAL_MS);
        resolve(client);
      });
      socket.addEventListener("error", () => {
        clearTimeout(timer);
        reject(new FatalError("WebSocket connection failed", serverLost));
      });
      socket.addEventListener("message", (event) => client.receive(event.data));
      socket.addEventListener("close", (event) => client.closed(event.code));
    });
  }

  /** Called once if the server closes the socket or it breaks; never after `close()`. */
  onConnectionLost(handler: (error: FatalError) => void): void {
    this.onLost = handler;
  }

  private send(message: object): void {
    if (this.socket.readyState === WebSocket.OPEN) this.socket.send(JSON.stringify(message));
  }

  private ping(): void {
    this.unansweredPingAt ??= performance.now();
    this.send({ _tag: "Ping" });
  }

  /** Milliseconds the server has left a ping unanswered; 0 while it keeps talking. */
  silentFor(at: number): number {
    return this.finished || this.unansweredPingAt === null ? 0 : at - this.unansweredPingAt;
  }

  private receive(data: unknown): void {
    this.unansweredPingAt = null;
    const text = typeof data === "string" ? data : Buffer.from(data as ArrayBuffer).toString();
    try {
      const parsed = JSON.parse(text) as ServerMessage | ReadonlyArray<ServerMessage>;
      const messages = Array.isArray(parsed) ? parsed : [parsed as ServerMessage];
      const bytes = Buffer.byteLength(text);
      for (const message of messages) this.dispatch(message, bytes / messages.length);
    } catch (error) {
      this.fail(
        new FatalError(
          `Could not handle a server frame: ${error instanceof Error ? error.message : String(error)}`,
        ),
      );
    }
  }

  private dispatch(message: ServerMessage, bytes: number): void {
    if (message._tag === "Pong") return;
    if (message._tag === "Defect" || message._tag === "ClientProtocolError") {
      this.fail(
        new FatalError(`RPC protocol failure: ${describeFailureCause(message)}`, serverLost),
      );
      return;
    }
    const id = String(message.requestId);
    const entry = this.pending.get(id);
    if (entry === undefined) return;
    if (message._tag === "Chunk" && entry.kind === "stream") {
      this.send({ _tag: "Ack", requestId: id });
      entry.onValues(message.values ?? [], bytes);
      return;
    }
    if (message._tag !== "Exit") return;
    this.pending.delete(id);
    const exit = message.exit;
    const failure =
      exit?._tag === "Success" ? null : new CallError(describeFailureCause(exit?.cause ?? exit));
    if (entry.kind === "call") {
      clearTimeout(entry.timer);
      if (failure === null) entry.resolve(exit?.value);
      else entry.reject(failure);
    } else {
      entry.onEnd(failure);
    }
  }

  call(tag: string, payload: unknown, timeoutMs: number): Promise<unknown> {
    if (this.finished) {
      return Promise.reject(new FatalError(`${tag}: the WebSocket is closed`, serverLost));
    }
    const id = String(++this.nextId);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending.delete(id)) {
          this.send({ _tag: "Interrupt", requestId: id, interruptors: [] });
          reject(new CallError(`${tag}: timed out`));
        }
      }, timeoutMs);
      this.pending.set(id, { kind: "call", resolve, reject, timer });
      this.send({ _tag: "Request", id, tag, payload, headers: [] });
    });
  }

  /** Starts a streaming request; the returned function interrupts it. */
  stream(tag: string, payload: unknown, handlers: StreamHandlers): () => void {
    const id = String(++this.nextId);
    this.pending.set(id, { kind: "stream", ...handlers });
    this.send({ _tag: "Request", id, tag, payload, headers: [] });
    return () => {
      if (this.pending.delete(id)) {
        this.send({ _tag: "Interrupt", requestId: id, interruptors: [] });
      }
    };
  }

  private fail(error: FatalError): void {
    if (this.finished) return;
    this.finished = true;
    clearInterval(this.pinger);
    for (const entry of this.pending.values()) {
      if (entry.kind === "call") {
        clearTimeout(entry.timer);
        entry.reject(error);
      }
    }
    this.pending.clear();
    this.socket.close();
    this.onLost(error);
  }

  private closed(code: number): void {
    this.fail(new FatalError(`WebSocket closed by the server (code ${code})`, serverLost));
  }

  close(): void {
    if (this.finished) return;
    this.finished = true;
    clearInterval(this.pinger);
    for (const entry of this.pending.values()) {
      if (entry.kind === "call") clearTimeout(entry.timer);
    }
    this.pending.clear();
    this.socket.close();
  }
}

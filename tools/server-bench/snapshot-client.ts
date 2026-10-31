#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalTimers:off globalFetch:off globalRandom:off preferSchemaOverJson:off - standalone bench client outside the Effect runtime; it owns its pacing timers.
import * as NodeFS from "node:fs";
import * as NodeUtil from "node:util";

export const HELP = `Usage:
  node tools/server-bench/snapshot-client.ts --url <origin> --thread <id> [options]

Models a client that polls a thread's full snapshot (GET /api/orchestration/threads/:id) and gives
up on slow responses, like the bot that grew ws4-navid's heap in P1-019. It sends one request
at a time; each stops reading the body and aborts --timeout after it starts. A
body bigger than the socket buffers is then still being written when the client leaves, which is
the disconnect a server must release. The bearer token comes from T3_BENCH_TOKEN or --token-file
(scope orchestration:read). One JSON line per request goes to --out (or stdout).

  --thread ID            thread to fetch (required)
  --interval DUR         time between request starts (default 300s, the bot's cadence); a request
                         that outlasts it delays the next one, as a single poller would
  --timeout DUR[-DUR]    client-side abort after the request starts (default 23s); a range such
                         as 2s-9s picks a uniform value per request, so the abort lands at every
                         phase of a response whose duration varies (handler, encoding, transfer)
  --read none|full       none (default): leave the body unread until the abort; full: read it
  --accept-encoding V    request header (default "gzip, deflate", as Node's fetch sends)
  --count N              requests to send, then exit (default: until SIGINT/SIGTERM)
  --protocol N           x-t3-orchestration-protocol (default: the server's descriptor, else 2)
  --out FILE             append the per-request lines here`;

const duration = (value: string, flag: string) => {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m)?$/.exec(value.trim());
  const unit = match?.[2] ?? "s";
  const ms = Number(match?.[1]) * (unit === "ms" ? 1 : unit === "m" ? 60_000 : 1_000);
  if (match === null || !Number.isFinite(ms)) {
    throw new Error(`${flag} must be a duration such as 23s, 500ms or 5m`);
  }
  return ms;
};

const timeoutRange = (value: string) => {
  const parts = value.split("-");
  if (parts.length > 2) throw new Error("--timeout is one duration or a low-high range");
  const [low = "", high = low] = parts;
  const timeoutMs = duration(low, "--timeout");
  const timeoutMaxMs = duration(high, "--timeout");
  if (timeoutMaxMs < timeoutMs) throw new Error("--timeout range must go from low to high");
  return { timeoutMs, timeoutMaxMs };
};

export interface SnapshotClientOptions {
  readonly url: string;
  readonly threadId: string;
  readonly intervalMs: number;
  readonly timeoutMs: number;
  /** Upper end of a --timeout range; equal to timeoutMs for a fixed timeout. */
  readonly timeoutMaxMs: number;
  readonly read: "none" | "full";
  readonly acceptEncoding: string;
  readonly count: number | null;
  readonly protocol: number | null;
  readonly out: string | null;
  readonly tokenFile: string | null;
}

export function parseOptions(argv: ReadonlyArray<string>): SnapshotClientOptions {
  const { values } = NodeUtil.parseArgs({
    args: [...argv],
    options: {
      url: { type: "string" },
      thread: { type: "string" },
      interval: { type: "string", default: "300s" },
      timeout: { type: "string", default: "23s" },
      read: { type: "string", default: "none" },
      "accept-encoding": { type: "string", default: "gzip, deflate" },
      count: { type: "string" },
      protocol: { type: "string" },
      out: { type: "string" },
      "token-file": { type: "string" },
      help: { type: "boolean", default: false },
    },
  });
  if (values.help) throw new Error(HELP);
  if (values.url === undefined || values.thread === undefined) {
    throw new Error(`--url and --thread are required\n\n${HELP}`);
  }
  if (values.read !== "none" && values.read !== "full") throw new Error("--read is none or full");
  const positive = (value: string | undefined, flag: string) => {
    if (value === undefined) return null;
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed < 1)
      throw new Error(`${flag} must be a positive integer`);
    return parsed;
  };
  return {
    url: values.url.replace(/\/+$/, ""),
    threadId: values.thread,
    intervalMs: duration(values.interval, "--interval"),
    ...timeoutRange(values.timeout),
    read: values.read,
    acceptEncoding: values["accept-encoding"],
    count: positive(values.count, "--count"),
    protocol: positive(values.protocol, "--protocol"),
    out: values.out ?? null,
    tokenFile: values["token-file"] ?? null,
  };
}

/** What became of one request: answered in full, or where the client gave up. */
export type Outcome = "complete" | "aborted_before_headers" | "aborted_during_body" | "error";

export interface RequestRecord {
  readonly at: string;
  readonly seq: number;
  readonly outcome: Outcome;
  readonly status: number | null;
  readonly contentEncoding: string | null;
  readonly contentLength: number | null;
  readonly bytesRead: number;
  readonly timeoutMs: number;
  readonly headersMs: number | null;
  readonly endMs: number;
  readonly error?: string;
}

export async function fetchOnce(
  options: SnapshotClientOptions,
  token: string,
  protocol: number,
  seq: number,
  stop?: AbortSignal,
): Promise<RequestRecord> {
  const at = new Date().toISOString();
  const started = performance.now();
  const controller = new AbortController();
  const timeoutMs = options.timeoutMs + Math.random() * (options.timeoutMaxMs - options.timeoutMs);
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const abort = () => controller.abort();
  stop?.addEventListener("abort", abort, { once: true });
  if (stop?.aborted) abort();
  let status: number | null = null;
  let contentEncoding: string | null = null;
  let contentLength: number | null = null;
  let headersMs: number | null = null;
  let bytesRead = 0;
  const elapsed = () => Math.round(performance.now() - started);
  try {
    const response = await fetch(
      `${options.url}/api/orchestration/threads/${encodeURIComponent(options.threadId)}`,
      {
        headers: {
          authorization: `Bearer ${token}`,
          "accept-encoding": options.acceptEncoding,
          "x-t3-orchestration-protocol": String(protocol),
        },
        signal: controller.signal,
      },
    );
    headersMs = elapsed();
    status = response.status;
    contentEncoding = response.headers.get("content-encoding");
    const length = response.headers.get("content-length");
    contentLength = length === null ? null : Number(length);
    if (options.read === "none") {
      // Hold the body unread until the abort, as a client stuck behind a slow consumer.
      await new Promise<void>((resolve) => {
        if (controller.signal.aborted) resolve();
        controller.signal.addEventListener("abort", () => resolve(), { once: true });
      });
      await response.body?.cancel().catch(() => undefined);
      return {
        at,
        seq,
        outcome: "aborted_during_body",
        status,
        contentEncoding,
        contentLength,
        bytesRead,
        timeoutMs: Math.round(timeoutMs),
        headersMs,
        endMs: elapsed(),
      };
    }
    const reader = response.body?.getReader();
    if (reader !== undefined) {
      for (;;) {
        const chunk = await reader.read();
        if (chunk.done) break;
        bytesRead += chunk.value.byteLength;
      }
    }
    return {
      at,
      seq,
      outcome: "complete",
      status,
      contentEncoding,
      contentLength,
      bytesRead,
      timeoutMs: Math.round(timeoutMs),
      headersMs,
      endMs: elapsed(),
    };
  } catch (error) {
    const aborted = controller.signal.aborted;
    return {
      at,
      seq,
      outcome: !aborted
        ? "error"
        : headersMs === null
          ? "aborted_before_headers"
          : "aborted_during_body",
      status,
      contentEncoding,
      contentLength,
      bytesRead,
      timeoutMs: Math.round(timeoutMs),
      headersMs,
      endMs: elapsed(),
      ...(aborted ? {} : { error: error instanceof Error ? error.message : String(error) }),
    };
  } finally {
    clearTimeout(timer);
    stop?.removeEventListener("abort", abort);
  }
}

async function serverProtocol(url: string): Promise<number> {
  try {
    const response = await fetch(`${url}/.well-known/t3/environment`, {
      signal: AbortSignal.timeout(10_000),
    });
    const descriptor = (await response.json()) as { orchestrationProtocolVersion?: number };
    return descriptor.orchestrationProtocolVersion ?? 2;
  } catch {
    return 2;
  }
}

async function main() {
  let options: SnapshotClientOptions;
  try {
    options = parseOptions(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(2);
  }
  const token =
    options.tokenFile !== null
      ? NodeFS.readFileSync(options.tokenFile, "utf8").trim()
      : (process.env.T3_BENCH_TOKEN ?? "").trim();
  if (token === "") {
    process.stderr.write("No token: set T3_BENCH_TOKEN or pass --token-file\n");
    process.exit(2);
  }
  const protocol = options.protocol ?? (await serverProtocol(options.url));
  const stopping = new AbortController();
  const stop = () => stopping.abort();
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  for (let seq = 1; options.count === null || seq <= options.count; seq++) {
    if (stopping.signal.aborted) break;
    const started = Date.now();
    const record = await fetchOnce(options, token, protocol, seq, stopping.signal);
    const line = `${JSON.stringify(record)}\n`;
    if (options.out === null) process.stdout.write(line);
    else NodeFS.appendFileSync(options.out, line);
    if (options.count !== null && seq >= options.count) break;
    const waitMs = options.intervalMs - (Date.now() - started);
    if (waitMs > 0 && !stopping.signal.aborted) {
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer);
          stopping.signal.removeEventListener("abort", done);
          resolve();
        };
        const timer = setTimeout(done, waitMs);
        stopping.signal.addEventListener("abort", done, { once: true });
      });
    }
  }
}

if (import.meta.main) await main();

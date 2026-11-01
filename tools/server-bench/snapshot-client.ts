#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalTimers:off globalFetch:off preferSchemaOverJson:off - standalone bench client outside the Effect runtime; it owns its pacing timers.
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeHttps from "node:https";
import * as NodeUtil from "node:util";

import { createRng, type Rng } from "./driver/sampling.ts";

/** Each poll makes the server build a full snapshot; a typo must not turn into a flood. */
export const MIN_INTERVAL_MS = 5_000;
export const MIN_TIMEOUT_MS = 100;
/** Longer delays overflow setTimeout and fire at once. */
export const MAX_TIMER_MS = 2_147_483_647;

export const HELP = `Usage:
  node tools/server-bench/snapshot-client.ts --url <origin> --thread <id> [options]

Models a client that polls a thread's full snapshot (GET /api/orchestration/threads/:id) and gives
up on slow responses, like the bot (Node's fetch) that grew ws4-navid's heap in P1-019. It sends one request
at a time; each stops reading the body and aborts --timeout after it starts. An abort after the
handler returned but before the server wrote the response is the disconnect a server must
release. The bearer token comes from T3_BENCH_TOKEN or --token-file (scope orchestration:read).
One JSON line per request, then one summary line, go to --out (or stdout).

  --thread ID            thread to fetch (required)
  --interval DUR         time between request starts (default 300s, the bot's cadence; at least
                         ${MIN_INTERVAL_MS / 1000}s); a request that outlasts it delays the next one, as one poller would
  --timeout DUR[-DUR]    client-side abort after the request starts (default 23s; ${MIN_TIMEOUT_MS}ms or more);
                         a range such as 2s-9s picks a value per request, uniform under --seed, so
                         the abort lands at every phase of a response whose duration varies
  --seed TEXT            drives the --timeout draws, so two runs get the same abort times (default 1)
  --read none|full       none (default): leave the body unread until the abort; full: read it
  --accept-encoding V    request header (default "gzip, deflate", as Node's fetch sends)
  --count N              requests to send, then exit (default: until SIGINT/SIGTERM)
  --protocol N           x-t3-orchestration-protocol (default: the server's descriptor, else 2)
  --out FILE             append the lines here

Records count wireBytes: body bytes as sent, before any content decoding. A request with the
body unread that had all of it in its buffers at the deadline counts as complete.`;

const duration = (value: string, flag: string, minMs: number) => {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m)?$/.exec(value.trim());
  const unit = match?.[2] ?? "s";
  const ms = Number(match?.[1]) * (unit === "ms" ? 1 : unit === "m" ? 60_000 : 1_000);
  if (match === null || !Number.isFinite(ms)) {
    throw new Error(`${flag} must be a duration such as 23s, 500ms or 5m`);
  }
  if (ms < minMs || ms > MAX_TIMER_MS) {
    throw new Error(`${flag} must be between ${minMs}ms and ${MAX_TIMER_MS}ms`);
  }
  return ms;
};

const timeoutRange = (value: string) => {
  const parts = value.split("-");
  if (parts.length > 2) throw new Error("--timeout is one duration or a low-high range");
  const [low = "", high = low] = parts;
  const timeoutMs = duration(low, "--timeout", MIN_TIMEOUT_MS);
  const timeoutMaxMs = duration(high, "--timeout", MIN_TIMEOUT_MS);
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
  readonly seed: string;
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
      seed: { type: "string", default: "1" },
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
    intervalMs: duration(values.interval, "--interval", MIN_INTERVAL_MS),
    ...timeoutRange(values.timeout),
    seed: values.seed,
    read: values.read,
    acceptEncoding: values["accept-encoding"],
    count: positive(values.count, "--count"),
    protocol: positive(values.protocol, "--protocol"),
    out: values.out ?? null,
    tokenFile: values["token-file"] ?? null,
  };
}

/** The abort time of each request: uniform over the --timeout range, reproducible by --seed. */
export function makeTimeoutDraw(options: SnapshotClientOptions): () => number {
  const rng: Rng = createRng(options.seed).fork("timeout");
  return () =>
    Math.round(options.timeoutMs + rng.next() * (options.timeoutMaxMs - options.timeoutMs));
}

/** What became of one request: answered in full, or where the client gave up. */
export type Outcome = "complete" | "aborted_before_headers" | "aborted_during_body" | "error";

export interface RequestRecord {
  readonly at: string;
  readonly seq: number;
  readonly seed: string;
  readonly outcome: Outcome;
  readonly status: number | null;
  readonly contentEncoding: string | null;
  readonly contentLength: number | null;
  /** Body bytes as sent (still content-encoded) that reached this client. */
  readonly wireBytes: number;
  readonly timeoutMs: number;
  readonly headersMs: number | null;
  readonly endMs: number;
  readonly error?: string;
}

/** The outcome mix and abort times a run realised, written after its last request. */
export function summarize(seed: string, records: ReadonlyArray<RequestRecord>) {
  const outcomes: Partial<Record<Outcome, number>> = {};
  for (const record of records) outcomes[record.outcome] = (outcomes[record.outcome] ?? 0) + 1;
  const timeouts = records.map((record) => record.timeoutMs).toSorted((a, b) => a - b);
  return {
    summary: {
      seed,
      requests: records.length,
      outcomes,
      timeoutMs:
        timeouts.length === 0
          ? null
          : {
              min: timeouts[0],
              median: timeouts[Math.floor((timeouts.length - 1) / 2)],
              max: timeouts.at(-1),
            },
    },
  };
}

export function fetchOnce(
  options: SnapshotClientOptions,
  token: string,
  protocol: number,
  seq: number,
  timeoutMs: number,
  stop?: AbortSignal,
): Promise<RequestRecord> {
  const at = new Date().toISOString();
  const started = performance.now();
  const elapsed = () => Math.round(performance.now() - started);
  const url = new URL(
    `${options.url}/api/orchestration/threads/${encodeURIComponent(options.threadId)}`,
  );
  return new Promise<RequestRecord>((resolve) => {
    let response: NodeHttp.IncomingMessage | undefined;
    let headersMs: number | null = null;
    let wireBytes = 0;
    let settled = false;
    const finish = (outcome: Outcome, error?: string) => {
      if (settled) return;
      settled = true;
      // Unread, the arrived body sits in the response's buffer.
      if (options.read === "none" && response !== undefined) wireBytes = response.readableLength;
      clearTimeout(timer);
      stop?.removeEventListener("abort", leave);
      request.destroy();
      const length = response?.headers["content-length"];
      resolve({
        at,
        seq,
        seed: options.seed,
        outcome,
        status: response?.statusCode ?? null,
        contentEncoding: response?.headers["content-encoding"] ?? null,
        contentLength: length === undefined ? null : Number(length),
        wireBytes,
        timeoutMs,
        headersMs,
        endMs: elapsed(),
        ...(error === undefined ? {} : { error }),
      });
    };
    // Leaving with the body unread still counts as complete when all of it had arrived.
    const leave = () => {
      if (response === undefined) return finish("aborted_before_headers");
      finish(response.complete ? "complete" : "aborted_during_body");
    };
    const request = (url.protocol === "https:" ? NodeHttps : NodeHttp).get(
      url,
      {
        agent: false,
        headers: {
          authorization: `Bearer ${token}`,
          "accept-encoding": options.acceptEncoding,
          "user-agent": "node",
          "x-t3-orchestration-protocol": String(protocol),
        },
      },
      (incoming) => {
        response = incoming;
        headersMs = elapsed();
        // A server that stops before the end of the body is an error, not this client's timeout.
        const cutShort = () => finish("error", "the server closed the response before its end");
        incoming.on("aborted", cutShort);
        incoming.on("error", cutShort);
        incoming.on("close", () => {
          if (!incoming.complete) cutShort();
        });
        if (options.read === "none") {
          incoming.pause();
          return;
        }
        incoming.on("data", (chunk: Buffer) => (wireBytes += chunk.byteLength));
        incoming.on("end", () => finish("complete"));
      },
    );
    request.on("error", (error) => finish("error", error.message));
    const timer = setTimeout(leave, timeoutMs);
    stop?.addEventListener("abort", leave, { once: true });
    if (stop?.aborted) leave();
  });
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
  const write = (value: unknown) => {
    const line = `${JSON.stringify(value)}\n`;
    if (options.out === null) process.stdout.write(line);
    else NodeFS.appendFileSync(options.out, line);
  };
  const protocol = options.protocol ?? (await serverProtocol(options.url));
  const nextTimeout = makeTimeoutDraw(options);
  const records: Array<RequestRecord> = [];
  const stopping = new AbortController();
  const stop = () => stopping.abort();
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  for (let seq = 1; options.count === null || seq <= options.count; seq++) {
    if (stopping.signal.aborted) break;
    const started = Date.now();
    const record = await fetchOnce(options, token, protocol, seq, nextTimeout(), stopping.signal);
    records.push(record);
    write(record);
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
  write(summarize(options.seed, records));
}

if (import.meta.main) await main();

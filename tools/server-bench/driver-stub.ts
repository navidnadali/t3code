// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalTimers:off globalFetch:off preferSchemaOverJson:off - standalone bench load stub outside the Effect runtime; it owns its pacing timers.
import * as NodeFS from "node:fs";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeUtil from "node:util";

export const HELP = `Usage:
  node tools/server-bench/driver-stub.ts --url <origin> --out <driver.json> --warmup <s> --window <s>
                                         [--pollers 4] [--rate 20]

Stand-in for the P1-004 load driver with the same contract, so run.ts works before driver.ts
exists. It exchanges T3_BENCH_BOOTSTRAP_TOKEN at /oauth/token once (when set), then <pollers>
loops GET /.well-known/t3/environment at <rate> requests/s in total through the warm-up and the
window. driver.json records the window (epoch ms), the latency of the requests that started in
it (call type "http.environment") and ok=false with an error when the server stops answering.
--sessions and --subscribers are accepted and ignored.`;

interface CallTotals {
  readonly latencies: Array<number>;
  errors: number;
}

const percentile = (sorted: ReadonlyArray<number>, p: number) =>
  sorted.length === 0
    ? null
    : (sorted[Math.max(1, Math.ceil((p / 100) * sorted.length)) - 1] ?? null);

const seconds = (value: string, flag: string) => {
  const match = /^(\d+(?:\.\d+)?)(s|m)?$/.exec(value.trim());
  if (match === null) throw new Error(`${flag} must be a duration such as 60 or 60s`);
  return Number(match[1]) * (match[2] === "m" ? 60 : 1);
};

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, ms)));

async function exchangeBootstrap(origin: string, token: string): Promise<"ok" | "failed"> {
  try {
    const response = await fetch(`${origin}/oauth/token`, {
      method: "POST",
      body: new URLSearchParams({
        grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
        subject_token: token,
        subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
        requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
      }),
    });
    const body = (await response.json()) as { access_token?: unknown };
    return response.ok && typeof body.access_token === "string" ? "ok" : "failed";
  } catch {
    return "failed";
  }
}

async function main(argv: ReadonlyArray<string>) {
  const { values } = NodeUtil.parseArgs({
    args: [...argv],
    options: {
      url: { type: "string" },
      out: { type: "string" },
      warmup: { type: "string", default: "10s" },
      window: { type: "string", default: "30s" },
      pollers: { type: "string", default: "4" },
      rate: { type: "string", default: "20" },
      sessions: { type: "string" },
      subscribers: { type: "string" },
      help: { type: "boolean", short: "h" },
    },
  });
  if (values.help) {
    console.log(HELP);
    return 0;
  }
  if (values.url === undefined || values.out === undefined)
    throw new Error("--url and --out are required");
  const origin = values.url.replace(/\/+$/, "");
  const warmupS = seconds(values.warmup, "--warmup");
  const windowS = seconds(values.window, "--window");
  const pollers = Number(values.pollers);
  const rate = Number(values.rate);
  if (!Number.isInteger(pollers) || pollers < 1)
    throw new Error("--pollers must be a positive integer");
  if (!Number.isFinite(rate) || rate <= 0) throw new Error("--rate must be positive");

  const token = process.env.T3_BENCH_BOOTSTRAP_TOKEN;
  const auth =
    token === undefined || token === "" ? "skipped" : await exchangeBootstrap(origin, token);
  const startedAt = Date.now();
  const windowStart = startedAt + warmupS * 1000;
  const windowEnd = windowStart + windowS * 1000;
  const totals: CallTotals = { latencies: [], errors: 0 };
  let consecutiveFailures = 0;
  let failure: string | undefined;
  const periodMs = (pollers / rate) * 1000;

  const poller = async (index: number) => {
    for (let tick = 0; failure === undefined; tick++) {
      const due = startedAt + (tick + index / pollers) * periodMs;
      if (due >= windowEnd) return;
      await sleep(due - Date.now());
      const sentAt = Date.now();
      const begin = NodePerfHooks.performance.now();
      let ok = false;
      try {
        const response = await fetch(`${origin}/.well-known/t3/environment`, {
          signal: AbortSignal.timeout(10_000),
        });
        await response.arrayBuffer();
        ok = response.ok;
      } catch {
        ok = false;
      }
      const elapsed = NodePerfHooks.performance.now() - begin;
      consecutiveFailures = ok ? 0 : consecutiveFailures + 1;
      if (consecutiveFailures >= 3 * pollers) failure = "server stopped answering";
      if (sentAt < windowStart || sentAt > windowEnd) continue;
      if (ok) totals.latencies.push(elapsed);
      else totals.errors++;
    }
  };
  await Promise.all(Array.from({ length: pollers }, (_, index) => poller(index)));

  const sorted = totals.latencies.toSorted((a, b) => a - b);
  const ok = failure === undefined && auth !== "failed";
  const output = {
    schema: "t3-server-bench/driver/1",
    driver: "stub",
    ok,
    error: failure ?? (auth === "failed" ? "bootstrap token exchange failed" : null),
    window: { start_unix_ms: windowStart, end_unix_ms: windowEnd },
    knobs: {
      warmup_s: warmupS,
      window_s: windowS,
      pollers,
      rate_per_s: rate,
      auth,
    },
    calls: {
      "http.environment": {
        count: sorted.length,
        errors: totals.errors,
        p50_ms: percentile(sorted, 50),
        p95_ms: percentile(sorted, 95),
        p99_ms: percentile(sorted, 99),
        max_ms: sorted.at(-1) ?? null,
      },
    },
    events_per_s: null,
  };
  NodeFS.writeFileSync(values.out, `${JSON.stringify(output, null, 2)}\n`);
  return ok ? 0 : 1;
}

if (import.meta.main) {
  main(process.argv.slice(2)).then(
    (code) => process.exit(code),
    (error: unknown) => {
      console.error(`driver-stub: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(2);
    },
  );
}

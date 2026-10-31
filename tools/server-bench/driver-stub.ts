// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalTimers:off globalFetch:off preferSchemaOverJson:off - standalone bench load stub outside the Effect runtime; it owns its pacing timers.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeUtil from "node:util";

export const HELP = `Usage:
  node tools/server-bench/driver-stub.ts --url <origin> --out <dir> --warmup <s> --window <s>
                                         [--pollers 4] [--rate 20]

A light stand-in for the P1-004 load driver (driver.ts) with the same command line and
driver.json shape, for smoke runs of run.ts: <pollers> loops GET /.well-known/t3/environment at
<rate> requests/s in total through the warm-up and the window, and <out>/driver.json records the
window and the latency of the requests that started in it (call type "http.environment").
A server that stops answering ends the run with status "failed" and a driver.failed marker.
--sessions, --subscribers and --server-pid are accepted and ignored; T3_BENCH_TOKEN is not used.`;

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

const round = (value: number) => Math.round(value * 1000) / 1000;

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
      "server-pid": { type: "string" },
      help: { type: "boolean", short: "h" },
    },
  });
  if (values.help) {
    console.log(HELP);
    return 0;
  }
  if (values.url === undefined || values.out === undefined) {
    throw new Error("--url and --out are required");
  }
  const origin = values.url.replace(/\/+$/, "");
  const warmupS = seconds(values.warmup, "--warmup");
  const windowS = seconds(values.window, "--window");
  const pollers = Number(values.pollers);
  const rate = Number(values.rate);
  if (!Number.isInteger(pollers) || pollers < 1) {
    throw new Error("--pollers must be a positive integer");
  }
  if (!Number.isFinite(rate) || rate <= 0) throw new Error("--rate must be positive");
  NodeFS.mkdirSync(values.out, { recursive: true });
  for (const file of ["driver.json", "driver.failed"]) {
    NodeFS.rmSync(NodePath.join(values.out, file), { force: true });
  }

  const startedAt = Date.now();
  const windowStart = startedAt + warmupS * 1000;
  const windowEnd = windowStart + windowS * 1000;
  const latencies: Array<number> = [];
  let errors = 0;
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
      if (consecutiveFailures >= 3 * pollers) failure = "the server stopped answering";
      if (sentAt < windowStart || sentAt > windowEnd) continue;
      if (ok) latencies.push(elapsed);
      else errors++;
    }
  };
  await Promise.all(Array.from({ length: pollers }, (_, index) => poller(index)));

  const sorted = latencies.toSorted((a, b) => a - b);
  const percentileOrNull = (p: number) => {
    const value = percentile(sorted, p);
    return value === null ? null : round(value);
  };
  const base = {
    schemaVersion: 1,
    status: failure === undefined ? "ok" : "failed",
    driver: { name: "stub", node: process.version },
    knobs: { warmup_s: warmupS, window_s: windowS, pollers, rate_per_s: rate },
    timeline: {
      warmup: new Date(startedAt).toISOString(),
      window: new Date(windowStart).toISOString(),
      measured: new Date(windowEnd).toISOString(),
    },
  };
  const failed = failure === undefined ? undefined : { phase: "window", reason: failure };
  const result =
    failed === undefined
      ? {
          ...base,
          windowSeconds: windowS,
          calls: {
            "http.environment": {
              count: sorted.length,
              errors,
              perSecond: round((sorted.length + errors) / windowS),
              p50: percentileOrNull(50),
              p95: percentileOrNull(95),
              p99: percentileOrNull(99),
              max: sorted.length === 0 ? null : round(sorted.at(-1)!),
            },
          },
          counters: {},
        }
      : { ...base, failure: failed };
  if (failed !== undefined) {
    NodeFS.writeFileSync(NodePath.join(values.out, "driver.failed"), `${JSON.stringify(failed)}\n`);
  }
  NodeFS.writeFileSync(
    NodePath.join(values.out, "driver.json"),
    `${JSON.stringify(result, null, 2)}\n`,
  );
  return failure === undefined ? 0 : 1;
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

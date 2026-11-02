# Observability

> For maintainers. Using T3 Code? See [docs/user](../user/).

T3 Code has one server-side observability model:

- pretty logs go to stdout for humans
- completed spans go to a local NDJSON trace file
- traces, metrics, and logs can also be exported over OTLP to a real backend like Grafana LGTM

The local trace file is the persisted source of truth for normal local launches. Those launches do not
write a separate server log file, but SSH-managed launches also persist the remote process's
stdout/stderr at `~/.t3/ssh-launch/<state>/server.log`.

## Where To Find Things

### Logs

Logs are human-facing:

- destination: stdout
- format: `Logger.consolePretty()`
- normal local persistence: none
- SSH-managed launch persistence: `~/.t3/ssh-launch/<state>/server.log`
- remote export: OTLP only, when configured

If you want a log message to show up in the trace file, emit it inside an active span with `Effect.log...`. `Logger.tracerLogger` will attach it as a span event.

Configuring a logs endpoint takes over that job. The server then exports log records, which cover
every message instead of only the ones inside an active span and carry the trace and span ids so
they still line up with the trace. `Logger.tracerLogger` is dropped in that mode, so the same
message is not exported twice and the trace file stops carrying log messages. stdout output and
SSH-managed launch persistence stay unchanged either way.

### Traces

Completed spans are written as NDJSON records to `serverTracePath`. The default depends on how the
server starts: production and explicitly configured homes use
`<home>/userdata/logs/server.trace.ndjson` (so `~/.t3/userdata/...` by default, or
`/custom/path/userdata/...` with `--home-dir /custom/path`), a linked worktree dev run uses
`<worktree>/.t3/userdata/logs/server.trace.ndjson`, and an implicit dev run outside a linked
worktree uses `~/.t3/dev/logs/server.trace.ndjson`.

Important fields common to both record types:

- `type`: `effect-span` or `otlp-span`
- `name`: span name
- `traceId`, `spanId`, `parentSpanId`: correlation
- `durationMs`: elapsed time
- `attributes`: structured context
- `events`: embedded logs and custom events

`effect-span` records also contain `exit` with `Success`, `Failure`, or `Interrupted`. `otlp-span`
records instead carry OTLP resource, scope, and optional status fields.

The `TraceRecord`, `EffectTraceRecord`, `OtlpTraceRecord`, and `SpanSummaryTraceRecord` schemas
live in `packages/shared/src/observability.ts`.

SQL statement spans (named `sqlite`) and `sql.transaction` spans carry `db.connection` and
`db.lock_wait_ms`. `db.connection` is `writer` for the main connection, which runs on the event loop,
or `reader` for the read-only connection in a worker thread. `db.lock_wait_ms` is the part of the
span spent waiting for that connection, behind other SQL. That wait does not block the event loop,
so subtract it before you blame a slow span on the query. Statements inside a transaction use its
connection and do not carry these attributes.

An MCP request over HTTP has a `McpHttpServer.authenticateRequest` span that covers only admitting
it: the credential check, the mode limit, and the Linked environments gate. The request runs after
it, under the `McpServer.…/<method>` span, and each tool call has its own `mcp.tool.<name>` span
that times it. That span's attributes name the tool, never its arguments.

DPoP proof failures include the safe `environment.dpop.failure_code` span
attribute. A `time_window` failure means that a signed proof was too old or too
far in the future for the environment server's allowed window. It can point to
a date or time problem on either device, but it can also result from a delayed
request.

#### Fast SQL spans

SQL spans are most of the file on a busy server. `T3CODE_TRACE_SQL_MIN_DURATION_MS` (default `0`,
which writes every span) counts short ones instead of writing them. A `sqlite` or `sql.transaction`
span is counted when it succeeded, ran for less than that many milliseconds, waited less than
0.1 ms for its connection, and carries no log message. Taking a free connection measures a few
microseconds, so a smaller `db.lock_wait_ms` is not a wait. Every other span is still written,
including failed, interrupted, slow, and lock-waited SQL spans.

Every minute, and when the server stops, it writes one `span-summary` record per span name it
counted: `name`, `count`, `totalDurationMs`, `maxDurationMs`, the `thresholdMs`, and
`startTimeUnixNano` and `endTimeUnixNano` from the first counted start to the last counted end. It
has no `durationMs`, `traceId`, or `spanId`. `t3 trace summary` and Settings > Diagnostics add
these counts to the span counts; a percentile that falls among them prints as `<2ms` for a 2 ms
threshold. A crash or kill loses up to a minute of these counts; slow, failed, and lock-waited
spans are still written as they end.

With the filter on:

- A written span can name a counted span as its `parentSpanId`, for example a span inside a fast
  transaction. Treat a parent that is missing from the file as a counted SQL span.
- Heap attribution skips SQL spans, because a span is only known to be fast when it ends. Their
  heap growth shows on the enclosing span.
- OTLP export still receives every span.
- It is off while `T3CODE_TRACE_TIMING_ENABLED` is `false`, since every span then lasts 0 ms.

#### Summarize the trace file

`t3 trace summary` reads the trace file and its rotated backups directly, so it works while the
server is stalled or stopped. It prints counts, rates, and latency percentiles per span name. Use
it to measure background work or to compare two builds.

```bash
t3 trace summary --since 30m --limit 40
```

It reads `T3CODE_TRACE_FILE` if set, else `<home>/userdata/logs/server.trace.ndjson` for
`--base-dir` or `T3CODE_HOME`, plus the `T3CODE_TRACE_MAX_FILES` rotated backups. For a dev run or
a copied file, set `T3CODE_TRACE_FILE`. `--since 30m` keeps spans that ended in the last 30
minutes. The rate is per minute between the first and last span end. A `span-summary` record
(see [Fast SQL spans](#fast-sql-spans)) counts from its start, and `--since` keeps the whole
record when it ends inside the window.

### Metrics

Metrics are not written to a local file.

- local persistence: none
- remote export: OTLP only, when configured
- current definitions: `apps/server/src/observability/Metrics.ts`

If OTLP is not configured, metrics still exist in-process, but you will not have a local artifact to inspect.

### Event Loop Stalls

`apps/server/src/observability/EventLoopMonitor.ts` samples the server's event loop every 30 s. When
the loop stalled for more than 2 s since the previous sample, it records a root
`server.eventLoop.stall` span with a warning. The span has trace level `Warn`, so it stays when
`T3CODE_TRACE_MIN_LEVEL` is `Warn`. The warning shows in Settings > Diagnostics unless OTLP logs are
on. The span time is when the sample ran, not when the stall happened.

The trace file rotates in well under an hour on a busy server, so each stall is also appended as one
NDJSON line to `event-loop-stalls.ndjson`, next to the trace file. A line has `time` and the span
attributes. The file keeps the last 1000 or more stalls, with one rotated backup
(`event-loop-stalls.ndjson.1`). Use it to see how often a server stalls over days.

Some delay is not recorded:

- `delayMaxMs` is the longest stall, and can undercount it by up to 1 s. The 2 s threshold applies to
  this value, so a stall over 3 s is normally recorded, and a shorter one can be missed. A stall
  that ends just as a sample runs can be missed too.
- Time the computer spends asleep reads as delay on macOS and Windows. So a sample only counts when
  the loop was busy, not waiting for events, for at least `delayMaxMs`. Busy time covers the whole
  window, so a short sleep in an otherwise busy window can still record a false stall. The span then
  shows CPU time far below `delayMaxMs`.
- The first sample after launch is skipped. Startup work such as migrations and projection bootstrap
  can block the loop for seconds on a large database.

CPU times and page faults cover the whole process over the whole window since the previous sample.
The window is nominally 30 s, but a long stall delays the sample and makes the window longer. Other
work in the window can hide a wait, so only CPU time far below `delayMaxMs` proves the thread was
waiting. Read CPU together with page faults:

- High `cpuSystemMs` with many page faults means memory pressure. Major faults are reads from disk or swap.
  On macOS, reads from compressed memory are minor faults plus system CPU.
- High `cpuUserMs` with few page faults means JavaScript work or garbage collection.
- Low CPU with few major page faults points at synchronous disk I/O, such as SQLite reads or trace
  file writes.
- Many `involuntaryContextSwitches` mean other processes were competing for the CPU.

Every Git command is a child process, and Node creates each one synchronously on the thread that
spawns it (`posix_spawn` on macOS, `fork` on Linux). On Linux each `fork` copies the page tables of
the whole server, so its cost grows with the server's memory. On Linux and macOS the commands of
`VcsProcess` and `GitVcsDriver` (checkpoints, status, refs, source control CLIs) therefore start from a
small spawner process, started once while the server is small, so the server never forks for them:
each fork copies only the spawner's page tables. Output, exit codes and errors are the same. Git run
through other services, such as the cached repository identity lookup, still spawns on the event
loop. macOS spawns with `posix_spawn`, which copies no page tables, so the gain there is only the
event-loop time.

`T3CODE_PROCESS_SPAWNER` (any case) chooses where those commands start:

- unset, `child`, `1`, `true` or `on`: the spawner process;
- `worker`: a worker thread in the server, which still forks the server's address space on Linux
  (`T3CODE_VCS_SPAWN_WORKER=true` selects it too while `T3CODE_PROCESS_SPAWNER` is unset, so a host
  that still sets it keeps the worker until it is removed or `T3CODE_PROCESS_SPAWNER=child` is set);
- `default`, `0`, `false` or `off`: Node's spawner on the event loop, as Windows always does.

Any other value is logged as an error and keeps Node's spawner. A spawner process that stops is
restarted, which forks the server once. If it stops again before starting a command, or more than 3
times in 10 minutes, an error is logged and commands fail with "The process spawner stopped" for a
cool-down of 15 seconds, then a spawner is started again; while starts keep failing, the cool-down
doubles up to 5 minutes. Git never falls back to Node's spawner meanwhile. Each command's unread
output waits in the server, up to 1 MiB per stream, where it would otherwise wait in a 64 KiB pipe.
Release archives ship the spawner as its own small executable, `t3-process-spawner`, beside `t3`;
without it, `t3` runs the spawner from its own bundle, which takes about four times the memory, and
logs a warning at start.

The spawner kills its children and its own process group, then exits, when the server exits however
it exits, and when it fails or receives SIGTERM, SIGINT or SIGHUP. Once the server has begun to shut
down, a Git command still running or started later fails with "The process spawner stopped" instead of
running on the server. Stop the server with systemd's `KillMode=mixed`: with `control-group`, the
spawner gets SIGTERM while the server is still shutting down, and the server logs a warning and forks
once more if another command arrives.

The spawner process and the worker run real processes even when a test injects a fake
`ChildProcessSpawner` below these services; such a test sets `T3CODE_PROCESS_SPAWNER=default`.

The server runs with a 64 MiB V8 young generation (`--max-semi-space-size=64`; Node's default is
16 MiB). At 250 sessions that halves the scavenges, cuts GC time by 42 % and the event-loop delay
maximum by 59 %, for about 128 MiB more RSS; below 100 sessions it changes nothing measurable. V8
reads the flag only at startup, so it travels in `NODE_OPTIONS`: a `t3 serve`, `t3 start`, `t3` or
`t3 <dir>` (with any server flags) started without it relaunches itself once (`execve`, same pid,
before any other work), the
background service launcher passes it to the server it starts, and the server's children (agents,
helpers) get the `NODE_OPTIONS` the server itself was given. `T3CODE_V8_MAX_SEMI_SPACE_MB=0` keeps
Node's default, a positive integer sets another cap, and an explicit `--max-semi-space-size` in
`NODE_OPTIONS` or on the command line is never overridden. A server started with an IPC channel or
a bootstrap descriptor (`--bootstrap-fd`, `T3CODE_BOOTSTRAP_FD`) by a launcher that did not set the
flag runs with Node's default, as does Windows. `t3 __node-flags` prints what the current process runs with.

### Related Artifacts

Provider event NDJSON files still exist for provider runtime streams. Those are separate from the main server trace file.

## Run The Server In Instrumented Mode

There are two useful modes:

- local-only: stdout + local `server.trace.ndjson`
- full local observability: stdout + local trace file + OTLP export to Grafana/Tempo/Prometheus

The local trace file is always on. OTLP export is opt-in.

### Option 1: Local Traces Only

You do not need any extra env vars. Just run the app normally and inspect `server.trace.ndjson`.

Examples:

```bash
npx t3
```

```bash
node --run dev
```

```bash
node --run dev:desktop
```

### Option 2: Run With A Local LGTM Stack

#### 1. Start Grafana LGTM

```bash
docker run --name lgtm \
  -p 3000:3000 \
  -p 4317:4317 \
  -p 4318:4318 \
  --rm -ti \
  grafana/otel-lgtm
```

Then open `http://localhost:3000`.

Default Grafana login:

- username: `admin`
- password: `admin`

#### 2. Export OTLP env vars

For endpoint settings and startup precedence, see the
[user telemetry guide](../user/telemetry.md#export-diagnostics-to-your-own-receiver).
In a worktree, use `vp run dev` instead of `npx t3` to keep state isolated.

```bash
export T3CODE_OTLP_TRACES_URL=http://localhost:4318/v1/traces
export T3CODE_OTLP_METRICS_URL=http://localhost:4318/v1/metrics
export T3CODE_OTLP_LOGS_URL=http://localhost:4318/v1/logs
export OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=development
```

Optional:

```bash
export T3CODE_TRACE_MIN_LEVEL=Info
export T3CODE_TRACE_TIMING_ENABLED=true
```

#### 3. Launch the app from that same shell

CLI:

```bash
npx t3
```

Monorepo web/server dev:

```bash
node --run dev
```

Monorepo desktop dev:

```bash
node --run dev:desktop
```

Packaged desktop app:

Launch the actual app executable from the same shell so the desktop app and embedded backend inherit `T3CODE_OTLP_*`.

macOS app bundle example:

```bash
T3CODE_OTLP_TRACES_URL=http://localhost:4318/v1/traces \
T3CODE_OTLP_METRICS_URL=http://localhost:4318/v1/metrics \
T3CODE_OTLP_LOGS_URL=http://localhost:4318/v1/logs \
"/Applications/T3 Code.app/Contents/MacOS/T3 Code"
```

Direct binary example:

```bash
T3CODE_OTLP_TRACES_URL=http://localhost:4318/v1/traces \
T3CODE_OTLP_METRICS_URL=http://localhost:4318/v1/metrics \
T3CODE_OTLP_LOGS_URL=http://localhost:4318/v1/logs \
./path/to/your/desktop-app-binary
```

Do not rely on launching from Finder, Spotlight, the dock, or the Start menu after setting shell env vars. Those launches usually will not pick them up.

#### 4. Fully restart after changing env

The backend reads observability config at process start. If you change OTLP env vars, stop the app completely and start it again.

## How To Use Traces And Metrics To Debug The Server

### Start With The Local Trace File

The trace file is the fastest way to inspect raw span data.

Resolve the path for the launch mode once. Production and explicitly configured homes store runtime
state under the base directory's `userdata` folder:

```bash
TRACE_FILE="${T3CODE_HOME:-$HOME/.t3}/userdata/logs/server.trace.ndjson"
```

A dev server started from a linked worktree defaults to that worktree's local home:

```bash
TRACE_FILE="$WORKTREE/.t3/userdata/logs/server.trace.ndjson"
```

Only an implicit dev run outside a linked worktree uses the shared dev directory:

```bash
TRACE_FILE="$HOME/.t3/dev/logs/server.trace.ndjson"
```

Tail the selected file:

```bash
tail -f "$TRACE_FILE"
```

Show failed spans:

```bash
jq -c 'select(.type == "effect-span" and .exit._tag != "Success") | {
  name,
  durationMs,
  exit,
  attributes
}' "$TRACE_FILE"
```

Show slow spans:

```bash
jq -c 'select(.durationMs > 1000) | {
  name,
  durationMs,
  traceId,
  spanId
}' "$TRACE_FILE"
```

Inspect embedded log events:

```bash
jq -c 'select(any(.events[]?; .attributes["effect.logLevel"] != null)) | {
  name,
  durationMs,
  events: [
    .events[]
    | select(.attributes["effect.logLevel"] != null)
    | {
        message: .name,
        level: .attributes["effect.logLevel"]
      }
  ]
}' "$TRACE_FILE"
```

Follow one trace:

```bash
jq -r 'select(.traceId == "TRACE_ID_HERE") | [
  .name,
  .spanId,
  (.parentSpanId // "-"),
  .durationMs
] | @tsv' "$TRACE_FILE"
```

Filter orchestration commands:

```bash
jq -c 'select(.attributes["orchestration_v2.command_type"] != null) | {
  name,
  durationMs,
  commandType: .attributes["orchestration_v2.command_type"],
  threadId: .attributes["orchestration_v2.thread_id"]
}' "$TRACE_FILE"
```

Filter git activity:

```bash
jq -c 'select(.attributes["git.operation"] != null) | {
  name,
  durationMs,
  operation: .attributes["git.operation"],
  cwd: .attributes["git.cwd"],
  hookEvents: [
    .events[]
    | select(.name == "git.hook.started" or .name == "git.hook.finished")
  ]
}' "$TRACE_FILE"
```

### Use Tempo When You Need A Real Trace Viewer

Tempo is better than raw NDJSON when you want to:

- search across many traces
- inspect parent/child relationships visually
- compare many slow traces
- drill into one failing request without hand-joining by `traceId`

Recommended flow in Grafana:

1. Open `Explore`.
2. Pick the `Tempo` data source.
3. Set the time range to something recent like `Last 15 minutes`.
4. Start broad. Do not begin with a very narrow query.
5. Look for spans from the `t3code-server` or `t3code-desktop` service, then narrow by span name or
   attributes.

Good first searches:

- service name `t3code-server` or `t3code-desktop`, plus a resource attribute such as
  `deployment.environment.name`
- span names like `sendTurn` or a Git operation such as `GitVcsDriver.statusDetails.status`
- Git spans whose `git.operation` attribute identifies the operation
- orchestration spans with attributes like `orchestration_v2.command_type`

Once you know traces are arriving, narrower TraceQL queries for names such as `sendTurn` or Git
operation names become useful.

### Use Metrics To See Systemic Problems

Traces are best for one request. Metrics are best for trends.

Good metric families to watch:

- `t3_rpc_request_duration`
- `t3_orchestration_command_duration`
- `t3_provider_turn_duration` (how long the provider adapter takes to start a turn, not the turn's run time)
- `t3_git_command_duration`

Counters tell you volume and failure rate:

- `t3_rpc_requests_total`
- `t3_orchestration_commands_total`
- `t3_provider_turns_total`
- `t3_git_commands_total`

Webhooks have their own families:

- `t3_webhook_deliveries_total` by `outcome` and `source` (`relay` or `direct`). Beyond what the
  sender sees, `queue_full` means a task already had its limit of deliveries waiting,
  `prompt_too_long` means the filled-in prompt passed the provider limit, and `duplicate` means
  the relay delivered a request this environment had already run.
- `t3_webhook_runs_total` by `outcome` (`started`, `skipped`, `failed`) for the runs those
  deliveries start, which happen after the sender has its answer.
- `t3_webhook_held_delay` for how long requests the relay held waited before arriving.

- `t3_secret_requests_total` by `status` (`saved`, `declined`, `cancelled`, `timed_out`) for secrets
  agents asked users for, and `t3_secret_refs_consumed_total` by `result` (`used`, `rejected`) for
  tools redeeming them. Neither ever carries a value.

`ScheduledTaskService.triggerWebhook` spans carry the same outcome per request, and each run
started from a delivery is its own `ScheduledTaskService.runWebhookDelivery` trace. For a request
the relay forwarded, the span also goes to the T3 Connect trace export as a child of the relay's
span; requests that reach the environment directly never join a sender's trace.

Use metrics when the question is:

- "is this always slow?"
- "did this get worse after a change?"
- "which command type is failing most often?"

Use traces when the question is:

- "what happened in this specific request?"
- "which child span caused this one slow interaction?"
- "what logs were emitted inside the failing flow?"

## Common Workflows

### "Why did this request fail?"

1. Start with the local NDJSON file.
2. Find `effect-span` records where `exit._tag != "Success"`.
3. Group by `traceId`.
4. Inspect sibling spans and span events.
5. If needed, move to Tempo for the full trace tree.

### "Why is the UI feeling slow?"

1. Search for slow top-level spans in the trace file or Tempo.
2. Check child spans for sqlite, git, provider, or terminal work.
3. Look at the matching duration metrics to see whether the slowness is systemic.

### "Are git hooks causing latency?"

1. Filter `git.operation` spans.
2. Inspect `git.hook.started` and `git.hook.finished` events.
3. Compare hook timing to the enclosing git span duration.

### "Why do I have spans locally but nothing in Grafana?"

Usually one of these is true:

- `T3CODE_OTLP_TRACES_URL` was not set
- the app was launched from a different environment than the one where you exported the vars
- the app was not fully restarted after changing env
- Grafana is looking at the wrong time range or service name

If the local NDJSON file is updating, local tracing is working. The problem is almost always OTLP export configuration or process startup.

## How To Think About Adding Tracing To Future Code

### Prefer Boundaries Over Tiny Helpers

Good span boundaries:

- RPC methods
- orchestration command handling
- provider adapter calls
- external process calls
- persistence writes
- queue handoffs

Avoid tracing every tiny helper. Most helpers should inherit the active span rather than create a new one.

### Reuse `Effect.fn(...)` Where It Already Exists

The codebase already uses `Effect.fn("name")` heavily. That should usually be your first tracing boundary.

For ad hoc work:

```ts
import { Effect } from "effect";

const runThing = Effect.gen(function* () {
  yield* Effect.annotateCurrentSpan({
    "thing.id": "abc123",
    "thing.kind": "example",
  });

  yield* Effect.logInfo("starting thing");
  return yield* doWork();
}).pipe(Effect.withSpan("thing.run"));
```

### Put High-Cardinality Detail On Spans

Use span annotations for IDs, paths, and other detailed context:

```ts
yield *
  Effect.annotateCurrentSpan({
    "provider.thread_id": input.threadId,
    "provider.request_id": input.requestId,
    "git.cwd": input.cwd,
  });
```

### Keep Metric Labels Low Cardinality

Good metric labels:

- operation kind
- method name
- provider kind
- aggregate kind
- outcome

Bad metric labels:

- raw thread IDs
- command IDs
- file paths
- cwd
- full prompts
- full model strings when a normalized family label would do

Detailed context belongs on spans, not metrics.

### Use Logs As Span Events

Logs inside a span become part of the trace story:

```ts
yield * Effect.logInfo("starting provider turn");
yield * Effect.logDebug("waiting for approval response");
```

Those messages show up as span events because `Logger.tracerLogger` is installed.

### Use The Pipeable Metrics API

`withMetrics(...)` is the default way to attach a counter and timer to an effect:

```ts
import { someCounter, someDuration, withMetrics } from "../observability/Metrics.ts";

const program = doWork().pipe(
  withMetrics({
    counter: someCounter,
    timer: someDuration,
    attributes: {
      operation: "work",
    },
  }),
);
```

## Detailed API Reference

### Runtime Wiring

The server observability layer is assembled in `apps/server/src/observability/Observability.ts`.

It provides:

- pretty stdout logger
- `Logger.tracerLogger`
- local NDJSON tracer
- optional OTLP trace exporter
- optional OTLP metrics exporter
- optional OTLP log exporter
- Effect trace-level and timing refs

The desktop main process is a second producer, assembled in
`apps/desktop/src/app/DesktopObservability.ts`. It reads the same `T3CODE_OTLP_*` names and the same
Settings entries as the backend it supervises, and covers work the backend cannot see: app startup,
window and menu handling, backend supervision, and updates. It reports as service
`t3code-desktop`, so a collector shows it alongside the backend rather than mixed into it. It
exports traces and logs only; the main process records no metrics, so the metrics endpoint applies
to the backend alone.

### Env Vars

Local trace file:

- `T3CODE_TRACE_FILE`: override trace file path
- `T3CODE_TRACE_MAX_BYTES`: per-file rotation size, default `10485760`
- `T3CODE_TRACE_MAX_FILES`: rotated file count, default `10`
- `T3CODE_TRACE_BATCH_WINDOW_MS`: flush window, default `1000`
- `T3CODE_TRACE_MIN_LEVEL`: minimum trace level, default `Info`
- `T3CODE_TRACE_TIMING_ENABLED`: enable timing metadata, default `true`
- `T3CODE_TRACE_SQL_MIN_DURATION_MS`: count successful SQL spans shorter than this many
  milliseconds instead of writing them, default `0` (write all), at most `1000`, the Diagnostics
  slow-span cutoff. See [Fast SQL spans](#fast-sql-spans).

OTLP export:

- `T3CODE_OTLP_TRACES_URL`: OTLP trace endpoint
- `T3CODE_OTLP_METRICS_URL`: OTLP metric endpoint
- `T3CODE_OTLP_LOGS_URL`: OTLP log endpoint
- `T3CODE_OTLP_EXPORT_INTERVAL_MS`: export interval, default `10000`
- `T3CODE_OTLP_HEADERS`: extra headers for all three exporters, same format as
  `OTEL_EXPORTER_OTLP_HEADERS`: comma-separated `key=value` pairs with percent-encoded values.
- `T3CODE_OTLP_PROTOCOL`: `http/json` (default) or `http/protobuf`

The server and the desktop app also read the standard
`OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_ENDPOINT` and generic `OTEL_EXPORTER_OTLP_ENDPOINT` (with
`/v1/traces`, `/v1/metrics`, or `/v1/logs` appended), for a collector expecting those instead. A
non-blank `T3CODE_OTLP_*_URL` wins over either, and a per-signal endpoint wins over the generic one
for its signal. A blank value counts as unset. A signal with an OTEL endpoint takes its headers from
`OTEL_EXPORTER_OTLP_HEADERS` and its protocol from `OTEL_EXPORTER_OTLP_PROTOCOL` (default
`http/protobuf`, read case-insensitively), and a per-signal
`OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_HEADERS` or `_PROTOCOL` wins over the generic one for its
signal. `T3CODE_OTLP_HEADERS` and `T3CODE_OTLP_PROTOCOL` never apply to it. An endpoint that is not
an `http` or `https` URL, a protocol other than `http/protobuf` or `http/json` such as `grpc`, or
headers that are not `key=value` pairs with percent-encoded values turn that signal's export off
with a startup warning, rather than sending it to the Settings endpoint.

Service names are fixed: `t3code-server` for the backend and `t3code-desktop` for the desktop main
process, both in `service.namespace` `t3code`. `OTEL_SERVICE_NAME` and a `service.name` or
`service.namespace` in `OTEL_RESOURCE_ATTRIBUTES` are ignored. Tell installations apart with other
resource attributes, such as `OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=staging`.

Each process reports its own build, not the one it talks to: the server, the desktop main process,
the UI, and the mobile app each carry `t3code.release.channel` (`dev`, `stable`, `nightly`, or
`preview`, from their own version; desktop PR builds count as `preview`, and a server not built by a
release workflow reports `dev`) and `deployment.environment.name` (`development`, `production`, or
`staging` for `preview`). On the server and the desktop main process either key in
`OTEL_RESOURCE_ATTRIBUTES` wins. The UI and mobile app never read that variable: Vercel PR previews
set `VITE_RELEASE_CHANNEL=preview` because their `APP_VERSION` must stay comparable with the
server's, and the mobile app maps its build variant (`development`, `preview`, `production`), or
reports `dev` from a development bundle.

Attributes specific to T3 Code use the `t3code.` prefix. The UI runs as `t3code-web` in both a
browser and the desktop window, since it is the same code, and `t3code.client.surface` is `desktop`
or `web`. The server's `t3code.server.managed_by` is `desktop` when the desktop app launched it and
`standalone` otherwise. The desktop main process reports `t3code.desktop.mode` as `development` or
`packaged` (formerly `service.mode`; dashboards filtering on that key must switch). Node processes
also report the semantic `process.runtime.name`, `process.runtime.version`, and
`process.runtime.description`, and UI traces report `browser.language`, `user_agent.original`, and,
in Chromium, `browser.platform`, `browser.mobile`, and `browser.brands`.

Every resource also carries a `service.instance.id`: random per process for the server and desktop,
per page load for the UI, and the Expo session on mobile. A `service.instance.id` in
`OTEL_RESOURCE_ATTRIBUTES` replaces the generated one on the server and desktop.

If the OTLP URLs are unset, local tracing still works, metrics stay in-process only, and logs stay
on stdout only.

SQLite memory, in MiB. Unset or `0` keeps SQLite's default: a 2000 KiB page cache per connection and
no memory map.

- `T3CODE_SQLITE_CACHE_MB`: page cache of the main connection, which does every write
- `T3CODE_SQLITE_READER_CACHE_MB`: page cache of each reader worker
- `T3CODE_SQLITE_MMAP_MB`: experimental memory map for the main connection and each reader worker, at
  most `2047` with Node's bundled SQLite

Each connection allocates its cache as it reads pages, up to the limit, so the cost is the main
connection's plus each reader worker's, up to `T3CODE_SQLITE_READER_POOL` (default 4) of them.
Readers drop their cache and memory map whenever another connection commits, so the main connection
gains most. While the database is mapped, an I/O error or a truncation from outside SQLite raises
SIGBUS and terminates the process. CLI commands that open the database, such as `t3 project`,
`t3 auth`, `t3 environment`, and `t3 thread`, use the same settings, and an invalid value stops them
as well as the server.

Reads run on up to `T3CODE_SQLITE_READER_POOL` (default 4) reader workers, and an idle reader always
takes a queued read. Reads come in three classes: small reads (thread shells, settlement, the thread
row, runs and other bookkeeping), page reads (snapshot windows, history and timeline pages), and
reads of a thread's whole history (full snapshots and projections, turn starts, thread search, and
records of turn items, nodes, messages, subagents, plans, handoffs or transfers). When reads of
several classes queue, a freed reader goes first to a class that has been passed over once for each
reader in the pool, the most passed over first, so none starves; then to small or page reads whose class holds no reader; then
to small, page and history reads in that order. History reads that already hold all readers but two
(at least one) go after every other queued read. Within a class, reads go in the order they queued.
The order counts turns, not reader time, and no reader is kept for small reads: when history reads
hold every reader, a small read waits for one of them to finish. In a review probe (two looping full
snapshots of an 86,755-item thread beside history, window and small reads) small reads waited up to
1.7 s at p95 with one reader, 0.5 s with two, 0.2-0.33 s with four and 95-209 ms with eight; before
this order they waited 36-79 ms with four, but snapshot windows starved (6 in 8 s against 149). A
read's statements go to a reader worker together; while reads fall back to the main connection they
all share it and run their statements in turn.
History reads each hold one of `T3CODE_SQLITE_READER_POOL` minus one permits, at least one, from
before their read transaction until their rows are decoded, which bounds the history rows held at
once. Waiting for a permit is not part of `db.lock_wait_ms`; waiting for a reader is.

Provider I/O workers. `T3CODE_PROVIDER_IO_WORKERS` (`0` to `32`; unset or `0` is off) starts worker
threads that parse long lines of Codex app-server and ACP agent output off the main thread and, for
Codex, build their provider-log records. The main thread still reads and splits the output, decodes
each message against its schema and handles the messages in order. If the workers fail to start,
provider output stays on the main thread for the rest of the process, with one warning.

- `T3CODE_PROVIDER_IO_CODEX_OFFLOAD_MIN_CHARS` (default `65536`): Codex lines at least this long go to
  a worker
- `T3CODE_PROVIDER_IO_ACP_OFFLOAD_MIN_CHARS` (default `65536`): the same for ACP lines; JSON-RPC
  batches always stay on the main thread

With few busy sessions, handing a shorter line to a worker and taking it back can cost the main
thread more than handling it there. With `T3CODE_PROVIDER_IO_STAGE_TIMING=1`, every line sent to a
worker is counted under the `offload` stage.

Each idle worker uses about 48 MiB (a V8 heap of about 24 MiB). About 30 MiB of that is the Claude
Agent SDK stage, which every worker carries even with `T3CODE_PROVIDER_IO_CLAUDE_SDK` off.

`T3CODE_PROVIDER_IO_CLAUDE_SDK` (default off; nothing while `T3CODE_PROVIDER_IO_WORKERS` is `0`) also runs
each Claude session's Agent SDK on those workers. The server still starts, watches, and stops the Claude CLI; a
worker reads its output. A session whose launch settings the worker cannot reproduce (an abort controller, a
session store or other non-data options, an in-process MCP server, or no Claude binary path) stays on the main
thread. A worker that has run a Claude session uses about 16 MiB more, once the Agent SDK has loaded on it.

A Claude session on a worker asks you what it would on the main thread: the worker passes each tool
approval prompt, question, MCP elicitation, resume-compaction dialog and hook call to the server,
which answers it as it does without workers, and passes on Claude's cancellation of one. The server
handles such a request once it has handled the output Claude sent before it.

### The Kill Switch

`T3CODE_OTEL_SDK_DISABLED` and `OTEL_SDK_DISABLED` turn off every OTLP export in both the server and
the desktop main process, overriding any endpoint from the environment or Settings. Local trace
files and stdout logs are unaffected.

`T3CODE_OTEL_SDK_DISABLED` wins when set, so `T3CODE_OTEL_SDK_DISABLED=false` re-enables export on a
machine that sets `OTEL_SDK_DISABLED` for everything else. It accepts the usual boolean spellings
(`true`/`false`, `yes`/`no`, `on`/`off`, `1`/`0`, `y`/`n`). `OTEL_SDK_DISABLED` follows the
OpenTelemetry specification and only `true` disables export, so `OTEL_SDK_DISABLED=1` does not.
Values are case-insensitive and trimmed. An unrecognized value is ignored with a startup warning.

`OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, or `OTEL_LOGS_EXPORTER` set to `none` turns off
just that signal, overriding an OTEL endpoint and the Settings endpoint. A `T3CODE_OTLP_*_URL` still
wins for its signal. `otlp` is the default, and any other exporter name, such as `console` or
`prometheus`, is ignored with a startup warning.

### What Is Instrumented Today

Current high-value span and metric boundaries include:

- WebSocket RPC request spans (`ws.rpc.<method>`) and metrics in
  `apps/server/src/observability/RpcInstrumentation.ts`
- startup phases
- orchestration command processing
- provider session and turn operations
- git command execution and git hook events
- terminal session lifecycle
- sqlite query execution, except the statements inside `orchestrationV2.EventSink.write`, which
  its `sql.transaction` span times as one
- event loop stalls (`server.eventLoop.stall`)

### Current Constraints

- logs outside spans are not persisted in the trace file; SSH-managed launch stdout/stderr is still
  captured in its launcher log
- metrics are not snapshotted locally

## Heap Snapshots

To see what a long-running server holds in memory, send it `SIGUSR2`. The server writes a V8 heap
snapshot to its logs dir and logs the path. This works for desktop, `npx t3`, and service installs
on macOS and Linux. Windows has no `SIGUSR2`.

Send the signal to the server pid in `server-runtime.json`, which sits in the server's state dir
next to the `logs` dir. For a dev server or a `--home-dir` launch, use that server's state dir from
[Traces](#traces). Do not send it to the desktop app or the service launcher: a process without the
handler exits on `SIGUSR2`. After a crash the file can keep a stale pid that now belongs to a
different process, so check the pid first.

```bash
pid="$(jq .pid "${T3CODE_HOME:-$HOME/.t3}/userdata/server-runtime.json")"
ps -p "$pid" -o command=
```

If `ps` shows the T3 Code server, send the signal:

```bash
kill -USR2 "$pid"
```

The file is `<logsDir>/server-<pid>-<timestamp>.heapsnapshot`, next to `server.trace.ndjson`. To
open it, use the Memory tab in Chrome DevTools and select Load.

Before you take one:

- The server stops while it writes the file. For a large heap this can take a minute or more.
  Connected clients can reconnect during the pause, and an event loop monitor, if the server has
  one, records the pause as a stall. Send the signal once. A second signal sent during a write
  takes another snapshot after the first one finishes.
- The write needs about as much free memory as the heap uses. On a machine that is already
  swapping, it can make the problem worse or crash the server.
- The file contains everything in server memory, including tokens, secrets, and thread content. Do
  not share it publicly. Delete it when you are done, because storage cleanup does not remove it.

# Observability in the performance fork

What this fork adds to T3 Code's observability and process handling. Upstream's
[observability guide](04_observability.md) covers the trace file, `t3 trace`, metrics and OTLP export;
the sections below cover only what the fork adds.

The `TraceRecord`, `EffectTraceRecord`, `OtlpTraceRecord`, and `SpanSummaryTraceRecord` schemas
live in `packages/shared/src/observability.ts`.

## Fast SQL spans

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

`T3CODE_TRACE_SQL_SAMPLE_EVERY=N` (default `1`, every span) keeps one in N of the `sqlite`
statement spans that the rule above would count, but that ran for anything under the 1 s
Diagnostics cutoff, so the file reaches further back. Transactions are never sampled. The rest
count in a `span-summary` record with `sampleEvery: N` and `sampleBelowMs`, the cutoff.
`t3 trace summary` counts them, and its percentiles let each written statement that could have
been sampled stand for its share of them; slow, failed and lock-waited ones stand for themselves.

With either on:

- A written span can name a counted span as its `parentSpanId`, for example a span inside a fast
  transaction. Treat a parent that is missing from the file as a counted SQL span.
- Heap attribution skips SQL spans, because a span is only known to be fast when it ends. Their
  heap growth shows on the enclosing span.
- OTLP export still receives every span.
- It is off while `T3CODE_TRACE_TIMING_ENABLED` is `false`, since every span then lasts 0 ms.

## Span minutes

The trace file rotates within minutes on a busy server. To see what the server spent its time on
hours or days ago, it also writes one line a minute to `server.span-minutes.ndjson`, next to the
trace file. A line counts every span that ended in that minute: sampled or not, and including the
SQL spans that `T3CODE_TRACE_SQL_MIN_DURATION_MS` only counts.

- `start`, `end`: the window, aligned to wall-clock minutes. The first line after a start and the
  line written when the server stops cover part of a minute. A window much longer than a minute
  means the event loop was blocked.
- `pid`: the server process. It changes with each restart.
- `spans`: `[count, totalMs, maxMs, p95Ms]` per span name. The p95 is within about 6% of the exact
  value, and exact up to 19 spans. Names only: no attributes, events, ids or messages. Past 2,000
  names in a minute the rest count under `(other)`.
- `spanPercentiles`: `[p50Ms, p90Ms, p99Ms]` per span name that ended ten times or more, from the
  same histogram (below that the p90 and p99 are the max). It is a separate field so readers of the
  four-field tuple keep working.
- `loop`: event loop delay p50, p99 and max beyond a 20 ms timer, and the share of the minute the
  loop was busy (`utilization`).
- `cpu`: user and system CPU time of the whole process, worker threads included, in ms.
- `memory`: resident memory, and JavaScript heap used and allocated, in bytes.
- `waits`: per site, the waits that ended in the minute: `n`, `totalMs`, `maxMs`, `p50Ms`, `p90Ms`,
  `p99Ms`. Where a site knows them, `holders` (what held the resource when the wait began),
  `waiters` (the operation that waited) and `outcomes` give `[n, totalMs]` for the top five, with
  the rest under `(other)`, and `depth` is `[p50, p90, max]` of the queue length arrivals found.
  An uncontended acquisition counts as a zero wait. The sites:
  - `ThreadCommandExecutor.lock` and `Orchestrator.messageIntake`, the per-thread command lock
    and the message intake lock. A holder is the span that took the lock.
  - `ProviderCommitBatch.queue`, from a write joining the provider commit queue to its batch's
    flush, with `.snapshot` (a provider snapshot batched with others) and `.barrier` (any other
    write, a provider control's included) by kind. A holder is the operation of the batch
    flushing when it joined. `ProviderCommitBatch.batch` has only `depth`: writes per commit.
  - The reaction pools by name (`Orchestrator.runUpdateReactions`, `.deferredSteerReactions`,
    `.childWorkReactions`, `ProviderContinuationService.wakes`, `.completions`), from offer to
    handling; the holder is `(key busy)` or `(pool full)`.
  - `EffectOutbox.<effect type>`, from an effect becoming due to its claim.
  - `sqlite.writer` and `sqlite.reader`, connection waits; a writer's holder is its owner's span.
  - `ProviderIngest.lag.<class>` (`delta`, `lifecycle`, `subagent`, `other`): from the server
    taking a provider event to its writes committing. `ProviderIngest.fanout` has only `depth`:
    the run subscriptions each event went to, as every run on a shared session takes every
    thread's events. `ProviderIngest.lagFromAdapter.<class>`
    runs from the adapter's own stamp instead (`updatedAt` of a message, turn item, subagent,
    provider thread or session; `createdAt` of a runtime request), so the difference is the time
    before the server took it, the adapter's queue included. Turn, node and plan updates,
    terminals and work notices carry no stamp. `ProviderIngest.terminalBacklog` has only
    `depth`: what each run's subscription held ahead of a turn terminal.
  - `McpToolAccess.wakeRunWait`, an MCP call from a wake turn waiting for its run, only when
    there was one to wait for, with `outcomes` `became-active`, `timed-out`, `refused-dead-run`
    or `other`.
- `gauges`: read at the minute's end. `ProviderCommitBatch.depth`, each reaction pool's
  `.pendingKeys`, and `ProviderIngest.backlog` (events published to runs and not taken yet) as
  they stand; `EffectAdmission.closedMs` (the gate held gated effects back) and
  `EffectAdmission.saturatedMs` (it ran at the saturated allowance) as the minute's share;
  `ProviderIngest.fanout.dropped` (events runs dropped before ingestion because another run on
  their shared provider session owns them, so never backlog or lag) as the minute's increase.
- `threads`: the five threads with the most wait (`byWait`) and hold (`byHold`) at each thread
  lock, and the most backlog (`byDepth`) at `ProviderIngest.backlog`, as anonymous ranks: no id,
  no title, nothing kept past the minute. Each rank has `waitMs`, `holdMs` and `n`, or `depth`,
  and `runs` and `subagents`, the thread's run and subagent-row counts as decade floors (0, 1,
  10, 100 ...). A hold counts in the minute it ends.
- `gc`: `[count, totalMs, maxMs]` per garbage collection kind that ran (`minor`, `major`,
  `incremental`, `weakcb`).
- `cpuProfile`, only with `T3CODE_TRACE_CPU_PROFILE_INTERVAL_MS`: the minute's `samples`,
  `sampledMs`, the top 25 functions by self time as `[name, selfMs]` (function, file without its
  directory, line) and the rest as `otherMs`. An in-process inspector session samples the main
  thread; it opens no port and needs no signal. Two profiles overlap at each minute's end, as
  restarting one makes V8 walk the whole heap again (seconds on a large heap). The first start
  logs the code already compiled, so set it before the heap grows. Sampling keeps the event loop
  out of idle: at 10 ms it added 0.14–0.20 to event loop utilization under a heavy synthetic load,
  with little extra CPU. A 50 ms interval cost about 0.04 and 100 ms about 0.01 on a synthetic
  load. Profile at 10 ms only for a short, deliberate window; for longer ones use 100 ms.

At 4 MiB the file is gzipped to `server.span-minutes.ndjson.<UTC close time>.gz`. The newest 96
archives are kept, at most 96 MiB together: several days on a server that ends a few hundred span
names a minute (a line of tens of KB), more on a quieter one. To list the five span names with the most time each minute:

```bash
cd ~/.t3/userdata/logs
(gzip -dc server.span-minutes.ndjson.*.gz; cat server.span-minutes.ndjson) |
  jq -c '{start, top: (.spans | to_entries | sort_by(-.value[1]) | .[:5] | map([.key, .value[1]]))}'
```

A crash or kill loses the current minute. `T3CODE_TRACE_SPAN_MINUTES=false` turns the file off.

## Child processes and the process spawner

Every child process Node starts is created synchronously on the thread that spawns it
(`posix_spawn` on macOS, `fork` on Linux). On Linux each `fork` copies the page tables of the whole
server, so its cost grows with the server's memory: about 10–17 ms per fork at 1–2 GB, and an
estimated few hundred ms for a server many times larger. On Linux and macOS the server therefore starts these commands from a
small spawner process, started once while the server is small, so the server does not fork for them
(each fork copies only the spawner's page tables):

- the commands of `VcsProcess` and `GitVcsDriver`: checkpoints, status, refs, diffs, worktrees and the
  source control CLIs, about 1,400–3,200 a minute at 100–250 busy sessions;
- the server's `ProcessRunner`: the repository identity lookup's Git commands (about 8 a minute, plus
  2 per project added), agent scope `systemctl` and `systemd-run` calls (one per session that ends),
  host probes such as `hostnamectl`, port scans, the terminal process-table fallback, device tools and
  update checks;
- the Claude and Codex instances' own commands: version checks and maintenance lookups (a few per
  health check, every 5 minutes by default) and text generation (thread titles, branch names, commit
  messages, pull request content and review guides, one or two per request);
- document text extraction (one per document attachment);
- Muse's checks and maintenance lookups (its SDK starts its sessions);
- Claude's history worker (resuming an interrupted turn, forking a session), keep-awake's sleep
  inhibitor (`systemd-inhibit` or `caffeinate`, when runs start after a quiet minute) and source
  control discovery's login-shell `PATH` refresh (at most one `$SHELL -ilc` per connection every 10 s).

Output, exit codes, errors, timeouts and kills are the same.

On Linux, provider processes start from the spawner process too, by hand-off: the spawner starts each
one with ordinary pipes as its stdin, stdout and stderr and passes their ends to the server over its
channel, keeping no copy, so the provider's I/O never passes through the spawner, and a burst of
session starts no longer holds the event loop for one fork each:

- Claude sessions, through the Agent SDK's spawn hook: one per session start or restart (100 or 250 in
  a burst when as many sessions start);
- the shared `codex app-server` of each Codex instance;
- OpenCode's `opencode serve` servers and commands, and Pi's RPC sessions and checks;
- the CLI of Claude's capability probe: one per health check whose capabilities expired and one per
  workspace a client shows, every 5 minutes.

Each of these was run with its real CLI both ways and behaves the same: its stdio are sockets in both
cases (no terminal, blocking mode), with the same end of input, 4 MiB written and 2 MiB read,
standard error and exit; and it keeps working when the spawner process dies.

Everything else the server starts still forks it on Linux:

- the sessions and checks of Grok, which exits about 2 s after its parent process dies (so a spawner
  that died would end its sessions); of Fx, Antigravity and Kiro, whose CLIs could not be run to
  verify them; and of ACP registry agents, which can be any agent the registry or a local command names
  (Gemini, verified, behaves);
- terminals: one `node-pty` process per terminal opened, which needs a pseudo-terminal in the server;
- HTML render: one headless Chrome per `html_render` or `html_preview` call, which needs extra pipes
  (file descriptors 3 and 4) the spawner does not pass;
- the preview browser and the Cursor and Muse SDKs, which spawn inside their libraries;
- the login-shell `PATH` probe (once, synchronously, before the server builds its services), the
  resource monitor, the `env -C` support check, `chattr` on btrfs and the spawner itself (once each
  at start-up), and the few commands whose output is not piped or that need extra file descriptors
  (opening an editor or a folder, `xdg-mime`, `wslpath`, and on macOS `ps`, `vm_stat` and `plutil`):
  the spawner process starts only commands whose stdin, stdout and stderr are all pipes, and hands
  any other command to Node's spawner;
- commands other services still start on Node's spawner, each per user action, setting change or
  health check: `ssh -G` for device hosts, `tailscale`, `cloudflared` and the 1Password CLI.

macOS spawns with `posix_spawn`, which copies no page tables, so the gain there is only the
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
doubles up to 5 minutes. These commands never fall back to Node's spawner meanwhile. Each command's unread
output waits in the server, up to 1 MiB per stream, where it would otherwise wait in a 64 KiB pipe.
Release archives ship the spawner as its own small executable, `t3-process-spawner`, beside `t3`;
without it, `t3` runs the spawner from its own bundle, which takes about four times the memory, and
logs a warning at start.

`T3CODE_PROVIDER_SPAWNER` (any case) chooses where provider processes start:

- unset, `handoff`, `1`, `true` or `on`: hand-off from the spawner process, on Linux when
  `T3CODE_PROCESS_SPAWNER` selects the spawner process, else Node's spawner;
- `direct`, `default`, `0`, `false` or `off`: Node's spawner on the event loop, as before.

Any other value is logged as an error and keeps Node's spawner. Unlike a command, a provider whose
hand-off fails (the spawner stopped or cooling down, a descriptor it could not pass) starts on Node's
spawner, with a warning, instead of failing. No path or name leads to a provider's stdio: only the
spawner, briefly, and then the server hold them, so no other process, of this user or another, can
read or write them. The one exception is a process of the same user that may trace them (`ptrace`,
which Yama's `ptrace_scope` 0 allows any process of that user): it can reach the spawner's channel,
as it can reach the server's own memory, so hand-off gives it nothing it did not already have.

Each provider starts as a small shim, the spawner executable itself (`t3-process-spawner
__exec-after-go`, or the server's Node with the shim's script outside single executables), with an
environment holding only `ELECTRON_RUN_AS_NODE=1`, so nothing of the provider's own (`NODE_OPTIONS`
included) applies to it. The shim
waits until the server holds every end, then receives the file, argv and environment on descriptor 3
and becomes the provider with `execve`: same PID and process group, exactly its environment,
descriptor 3 closed, and SIGPIPE and SIGXFSZ at their defaults, as after Node's spawn. The
environment is never on any process's argv (`/proc/<pid>/cmdline`, which every user can read). Each
shim costs about 8 MiB of private memory and 10–20 ms of CPU, outside the server, until it becomes the
provider. The spawner runs at most one shim per core at a time; further starts in a burst (sessions
resuming after a restart) wait in the spawner before their shim exists, so 1,000 simultaneous starts
hold about 150 MiB of shims rather than 8.6 GiB, and are all handed off in 12–14 s on 16 cores (the
channel passes one descriptor at a time, about 75 starts a second). The queue must keep moving: if the
spawner begins none of the starts waiting there for 10 s (checked every 10 s, so 10–20 s after the last
one began), the server stops it and they start on Node's spawner, with a warning. A single executable
without `t3-process-spawner` beside it hands off nothing, since its shim would load the whole server
bundle.

If the spawner process dies, or the server stops it for a fault (a start it did not answer, a queue that
did not move: the server sends it SIGTERM, not the channel's close that means the server's exit), the
providers it handed off keep running in their own process groups, their stdio still held by the server. The server then checks each one's process identity (PID and
start time) every second and reports its exit once it ends, as "exit status unknown"; a kill checks
that identity first, so a reused PID is never signalled. Until it ends, the port tools still treat
such a provider as the server's own child and do not stop its group. Resource telemetry, which
samples only what descends from the server, no longer shows it.

The spawner kills its children and its own process group, then exits, when the server exits however
it exits, and when it fails or receives SIGTERM, SIGINT or SIGHUP; providers it handed off are
killed only in the first case. Once the server has begun to shut down, a command still running or
started later fails with "The process spawner stopped" instead of running on the server. Stop the
server with systemd's `KillMode=mixed`: with `control-group`, the spawner gets SIGTERM while the
server is still shutting down, and the server logs a warning and forks once more if another command
arrives.

The spawner process and the worker run real processes even when a test injects a fake
`ChildProcessSpawner` below these services; such a test sets `T3CODE_PROCESS_SPAWNER=default`.

The server runs with a 64 MiB V8 young generation (`--max-semi-space-size=64`; Node 26's default
behaves like 32 MiB). At 250 sessions that halves the scavenges, cuts GC time by 42 % and the
event-loop delay maximum by 59 %, for about 128 MiB more RSS; at 50 sessions nothing measurable
changes. The flag is process-wide, so worker isolates share the cap and may each grow by up to 64 MiB
under load. V8
reads the flag only at startup. The single executable has it built in (the SEA's `execArgv`, with
`execArgvExtension: "env"`), so no start of the packaged binary ever relaunches: Node reads `NODE_OPTIONS` itself,
with its own guards (the allowlist refuses what it would refuse anywhere; nothing is read under AT_SECURE), and
the built-in 64 MiB stays whatever `NODE_OPTIONS`, the knob or the hatch say — by decision, the packaged binary's
cap cannot be changed or turned off, and the server logs `server.v8` at Warn (`capSettingIneffective`) when the
environment tried. A `node bin.mjs` start (an npm install) and the desktop's backend (Electron's Node, given the
cap in `NODE_OPTIONS` with the marker) keep the knob, the hatch and the relaunch fallback: such a start without
the cap relaunches itself once (`execve`, same pid, before any other work) with the cap in `NODE_OPTIONS`, the
background service launcher passes it the same way, and in every case the server's children (agents, helpers)
get exactly the `NODE_OPTIONS` the server itself was given. `T3CODE_V8_MAX_SEMI_SPACE_MB=0` keeps
Node's default, a positive integer sets another cap, and an explicit `--max-semi-space-size` in `NODE_OPTIONS` or
on the command line is never overridden (values above 1024 MiB are ignored) — on node-run installs; the packaged
binary's built-in cap takes none of them. The packaged binary keeps its built-in cap on every one
of these paths. On a node-run or unbaked install, a server started with an IPC channel or a bootstrap
descriptor (`--bootstrap-fd`, `T3CODE_BOOTSTRAP_FD`) by a launcher that did not set the flag, or with the
inspector (`--inspect*`), runs with Node's default; so does a direct `node bin.mjs serve` on Windows (no
`execve`), while a server the service launcher starts gets the flag through its environment. A direct start with a preload (`--require`, `--import`, `--loader` on the
command line or in `NODE_OPTIONS`) keeps Node's default too, with the reason "started with a preload": what
`NODE_OPTIONS` says after a preload ran is not what Node started with, so neither an explicit cap nor the
user's options could be carried into a relaunch; a server the launcher starts gets the flag regardless. On Linux the
decision also checks `NODE_OPTIONS` as Node started with, read from `/proc/self/environ`: when that record differs
from `NODE_OPTIONS` as it reads, a preload rewrote it and the process keeps Node's default ("NODE_OPTIONS changed
since startup"); a `node --env-file` that supplies `NODE_OPTIONS` reads the same way (Node applies the file after
exec, so the record lacks it), so such a server keeps Node's default — safe but lossy; put the cap in the file or the
environment instead. macOS and Windows keep no exact record, so the rules above decide on their own; the one case they
cannot see is an ES-module preload (`--import`, `--loader`) in `NODE_OPTIONS` that deletes its own entry: a `node`
start (an npm install) is then relaunched once without that preload, and an explicit cap the preload also deleted
becomes the default — the single executable runs no `--import` preload from `NODE_OPTIONS`, so the release binary
cannot hit it. A process relaunches at most once (the relaunch marks its own pid), whatever a preload does to
`NODE_OPTIONS`. `t3 __node-flags` prints what the current process runs with; `server.v8` logs the parsed sizes once
the server listens — at Warn, with the reason, whenever the server wanted the cap and lost it to something changeable
(`youngGenerationLost`: a preload, a failed relaunch, a changed `NODE_OPTIONS`) or to a service launcher that
predates the default (`launcherPredatesDefault`: a remote update under a launcher that supplied no cap, until the
service restarts), or when a packaged binary's built-in cap could not take what the environment asked
(`capSettingIneffective`),
and at Info when the start itself rules a relaunch out (`structuralKeep`: an IPC channel without a launcher, a bootstrap
descriptor as the desktop app's backend uses, the inspector, no `execve`).

## "Why were an agent's T3 tool calls refused while it kept working?"

When a background task ends, Claude starts a wake turn on its own. T3 shows that turn only once
its continuation run starts, which a busy thread can queue behind its previous run. Until a run
starts on the thread, its own or one ahead of it, a run-scoped T3 tool call from the turn waits, for
at most ten minutes; one that still finds no run fails with `parent_not_active`. A continuation that
Stop cancels or holds ends the wait at once. These lines are at Warn and carry ids only, so a
server logging at Warn keeps them. Each wake line is written at most once a minute per thread and decision; `suppressed`
counts the ones skipped since the last.

- `orchestration-v2.claude-wake-turn-detected`: a wake turn began and its continuation was offered.
- `orchestration-v2.claude-wake-latch-released`: the wake is settled. `decision` says how:
  `continuation-started` (with its `runId`), `drained-by-resumed-run` (a user run resumed after a
  restart replayed it, with that run's `runId`), `run-ended-unstarted`, `dispatch-cleared`,
  `absorbed-by-user-turn`, `retired-wake-ended`, `next-notification` or `process-reset`.
- `orchestration-v2.claude-wake-latch-retired`: the continuation ended or was cleared while the wake
  turn still ran (`state` `opened` or `unopened`), or a request for an older wake arrived
  (`stale`).
- `orchestrator-mcp.caller-not-active`: a refused call, with its `operation` (`write`,
  `cross-thread write`, `cross-project write`, `delegate_task`, `remote delegate_task`,
  `create_threads`, `request_secret` or `t3_environment_link`), the calling `threadId`, the
  provider instance of its credential and of the thread, the thread's active run and whether it is
  archived.
- `orchestrator-mcp.wake-run-wait-timed-out`: a call from a thread with no run waited the most it
  may, ten minutes, for that thread's pending continuations (`pending`) to start a run, then went on
  to its run check.

A refusal between a thread's `claude-wake-turn-detected` and its `continuation-started` came from
the wake turn.

## Environment variables

Trace file settings the fork adds or changes:

- `T3CODE_TRACE_MAX_FILES`: rotated file count, default `10`. Settings > Diagnostics reads only the
  newest files that fit 128 MiB; `t3 trace summary` reads them all.
- `T3CODE_TRACE_SQL_MIN_DURATION_MS`: count successful SQL spans shorter than this many
  milliseconds instead of writing them, default `0` (write all), at most `1000`, the Diagnostics
  slow-span cutoff. See [Fast SQL spans](#fast-sql-spans).
- `T3CODE_TRACE_SQL_SAMPLE_EVERY`: keep one in N plain SQL statement spans, default `1` (all), at
  most `10000`. See [Fast SQL spans](#fast-sql-spans).
- `T3CODE_TRACE_SPAN_MINUTES`: write the per-minute span aggregate, default `true`. See
  [Span minutes](#span-minutes).
- `T3CODE_TRACE_CPU_PROFILE_INTERVAL_MS`: add a CPU profile sampled at this interval to each span
  minute, `1` to `1000`; unset or `0` profiles nothing. It costs event loop utilization; see
  [Span minutes](#span-minutes).

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
hold every reader, a small read waits for one of them to finish. In a probe (two looping full
snapshots of a thread with tens of thousands of items beside history, window and small reads) small reads waited up to
1.7 s at p95 with one reader, 0.5 s with two, 0.2-0.33 s with four and 95-209 ms with eight; before
this order they waited 36-79 ms with four, but snapshot windows starved (6 in 8 s against 149). A
read's statements go to a reader worker together; while reads fall back to the main connection they
all share it and run their statements in turn.
History reads each hold one of `T3CODE_SQLITE_READER_POOL` minus one permits, at least one, from
before their read transaction until their rows are decoded, which bounds the history rows held at
once. Waiting for a permit is not part of `db.lock_wait_ms`; waiting for a reader is.

Orchestration effect workers. Turn starts, checkpoint captures, interrupts, steers, restarts, runtime
request answers and title generation are outbox effects that `T3CODE_EFFECT_WORKERS` (`1` to `256`,
default `4`) fibers claim and run, one effect per thread at a time, so this is how many threads can be
starting a turn at once. The default stays at 4 whatever the host: a turn start's history reads hold
reader permits (three with the default reader pool) and are decoded on the main thread, and on the
bench more workers than that moved the queue from the outbox to the readers — at 50 sessions a gain,
from about 120 sessions on every small read (thread shells, dispatch, thread switches) waited behind
the extra turn starts and the heap grew with the history they held. Raise it only together with more
read capacity and watch `db.lock_wait_ms`. The trace shows each effect's wait in `EffectWorker.runEffect`
(`effect.type`, `effect.queue_wait_ms`, `effect.attempt`) and each git command's wait for one of the 8
process slots in `VcsProcess.run` (`vcs.permit_wait_ms`); the server also exports the timer
`t3_orchestration_effect_queue_wait` by `effect_type`.

Effect admission. While the main thread's latency suffers, the workers leave the effects that start
turn work — turn starts, restarted-run continuations, checkpoint captures and title generation — in the
outbox beyond an allowance, so that user-facing work keeps its latency: interrupts, steers, runtime
request answers, detaches, stops and cleanups are always claimed (behind an older effect of their own
thread, as ever). The signal is the event loop's delay — the p99 of a 10 ms-resolution histogram over
each second, or the sampler's own lateness when the histogram holds fewer than ten samples because
a block spanned its ticks — not CPU time or
event loop utilization: a loop can be busy for most of a second yet schedule interactive work
promptly, and utilization counts every moment it is not idle. After two seconds at or above
`T3CODE_EFFECT_ADMISSION_DELAY_MS` (default `40`; `0` turns the gate off; otherwise at least `30`, so
that the reopen mark at three quarters stays above the histogram's resolution and an idle loop's
delay) only half the workers, at least one, may run such effects, and after two seconds below three
quarters of that delay
all of them may again. With one worker no gate is built: the saturated allowance equals the worker
count, so it could change nothing. The histogram and its 1 Hz sampler run only while the gate can act — from the
saturated allowance's worth of such effects in flight until the gate is open with fewer — so an idle
server or a single session pays no wake-ups for it; the price is that the first close after the
sampler starts takes its two samples from then. Effects already running are never stopped, a slot
counts against the allowance for ten minutes at most (a safety valve against hung starts, well past
the slow tail of turn starts, which run a minute or two as a rule — not a tuning value), the last
slot given back wakes one idle worker — after its claim took work, gated
or not, or when it expired; a claim that found nothing returns it without a wake-up — and a
reopened gate wakes them all. The time an
effect waited in the outbox is its queue wait (`effect.queue_wait_ms` on `EffectWorker.runEffect`,
which also carries `effect.gated`; the timer `t3_orchestration_effect_queue_wait`), and each switch
is logged ("Orchestration effect admission changed", with the delay that caused it).

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

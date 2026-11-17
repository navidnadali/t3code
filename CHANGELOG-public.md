# Changelog

Public release notes of this performance fork of T3 Code. Each release is one commit whose parents are the previous release and the upstream commit it is based on.

## v0.1.9-perf.12 (2026-11-17)

Hotfix: linear id reads for a run's provider turn and a rewind's off-branch runs

Upstream base: 7d32075e3757.

Same upstream main as 0.1.9-perf.10 (2026-11-16); no new migrations.

- A run's provider turn and a rewind's off-branch runs are found in one pass over the turns and attempts instead of one per pair, with the same result for every input. A follow-up on a parent with a long history no longer blocks the event loop on this lookup.

## v0.1.9-perf.11 (2026-11-17)

Wait and backlog tracing, promoted follow-ups and a fairer effect claim

Upstream base: 7d32075e3757.

Same upstream main as 0.1.9-perf.10 (2026-11-16); no new migrations.

- Tracing: span minutes carry p50, p90 and p99 per span name (for names that ended ten times or more), the waits at shared resources (keyed locks, SQLite connections, the commit queue, reactors, the effect outbox and the admission gate), provider ingest lag, backlog and batch sizes, garbage collections per kind and each provider event's subscription fan-out. A sampled CPU profile and one in N plain SQL statement spans are behind knobs. Span-minute archives keep six days.
- Ingestion: a run hands its ingestion mailbox's whole backlog over in one chunk and refills it only once that is ingested; a backlog of streamed text commits only its newest snapshot; a running turn's usage reports share the snapshot commit; a provider control waits only for its own writes; a run drops another run's provider events before its commit path.
- Effect claim: the oldest effect is taken unless a capture nobody waits on heads the queue, and a claim looks at most 64 pending effects past a deferred capture. A turn start passes checkpoint captures nobody waits on, for two minutes at most.
- Follow-ups promoted to Steer settle with the turn they join, keep their task through self-sends, cancels and shared results, and can be cancelled while held; a cancel drops their undelivered steers. A missed steer's follow-up ends a wait at once and keeps its place in the queue.
- Wakes: a wake turn's MCP calls wait up to ten minutes for its queued run to start; a seat's completion wakes for the same task merge into one; a wake's watch sees a run its dispatch already started.
- Claude: a turn start publishes and resumes with the background roster Claude reports now, and an older run's roster lands until the run that claimed the thread starts.
- A spawned child that exited without reading its stdin no longer leaves the write waiting.

## v0.1.9-perf.10 (2026-11-17)

Upstream main of 2026-11-16 and run fixes

Upstream base: 7d32075e3757.

Upstream T3 Code 0.1.9 at main of 2026-11-16, with upstream migration 96; Node 26.13.1.

- Upstream: everything on upstream's main up to 2026-11-16, among it steering queued messages into a running Grok turn (upstream #37335), one busy thread's lock no longer stalling other threads' queued runs (upstream #37417) and the title repair migration 96.
- A child agent's checkpoint capture skips the runs of its settled tasks.
- A start whose schema is already current takes no write lock in the fork's migration chain.
- A wake's lineage read no longer holds up the continuation intake.
- Trace diagnostics read only the newest trace files, up to 128 MiB.
- Cmd+Enter on an edited message with a fresh context starts it when the server can end the wait.

## v0.1.9-perf.9 (2026-11-16)

Queued-message delivery, completion wakes and restart recovery

Upstream base: c2e54e3ac9c8.

Upstream T3 Code 0.1.9 at main of 2026-11-15, with upstream migrations 93 to 95; Node 26.13.1.

- Migrations: upstream's migrations 93 to 95 apply in order, and the fork's migrations leave the message-ordinal index to upstream's migration 94.
- Queued messages: a message queued while a run is busy steers into it after 60 seconds, whether a person, an automation or another thread queued it. Due messages merge into one steer, oldest first and within the provider's text limit, with slash commands sent on their own; a turn takes at most one automatic steer a minute. Send now is offered on a waiting run only when the server can end its wait.
- Completion and pull request wakes: a thread's queued completion wakes, and its pull request watch wakes, merge into one wake of at most 25 results. A wake whose start failed, or that a restart cut, sends its results again with backoff, at most eight times; merged pull request news survives a restart.
- Restart recovery holds only the messages a person queued (unattended work only when continuation is off) and wakes a delegated seat whose waiter the restart cut.
- Run ends are handled per thread, in commit order and with bounded workers, so one busy thread no longer holds up the others; provider wakes dispatch in a pool of their own.
- A full thread snapshot is capped at 32 MiB of JSON by default and refused when too long for one socket frame.
- MCP tools in a wake turn wait for the turn's run (port of upstream #33330); a run that ends its wait early still captures its checkpoint.
- Traces: a per-minute span aggregate is written beside the trace file.
- Includes 0.1.9-perf.8's checkpoint cohort and descendant process fixes.
- Bench parameters in tools/server-bench set to chosen defaults.

## v0.1.9-perf.8 (2026-11-15)

Hotfix: checkpoint cohorts and descendant process tracking

Upstream base: e82050b8b97c.

No upstream change and no new migrations.

- A child agent's checkpoint capture offers only its own tasks' cohorts (upstream #34568).
- A Claude CLI's descendant record drops jobs that have ended, and only a process-table read that began after they ended prunes it (upstream #34505).
- Bench parameters in tools/server-bench set to chosen defaults.

## v0.1.9-perf.7 (2026-11-15)

Upstream 0.1.9, Node 26.13.1 and a round of correctness fixes

Upstream base: e82050b8b97c.

Upstream T3 Code 0.1.9; Node 26.13.1.

- Migrations: the fork's migration chain follows upstream migration 85; upstream's message-ordinal index migration is a no-op on a database that already has the fork's.
- Database: reader transactions are not folded into one BEGIN/COMMIT.
- Worker path parity with in-thread provider sessions: hook matchers (onFailure included), MCP servers in the query options and SDK error fields reach the worker; a failed worker session stops its Claude CLI.
- Spawner process: shell commands refuse separate arguments, no process-group signal for an exited child, a failing stdin source fails the command, upstream's hung-mount guard applies, commands are found as execvp finds them, and a stopped spawner's cleanup ends only what was sent to it.
- Checkpoints: baselines and checkpoints are taken at the scope's real path; a refused workspace lookup records an error checkpoint and completes the run; a later workspace's full-thread diff starts at its own first baseline.
- Steering: follow-ups of an archived or deleted thread are dropped; Stop and steer delivery fixes.
- Claude descendant processes share one poll (upstream #34505); pull request sync backs off; Codex and ACP output offload match in-thread behaviour; Codex's line framer flushes its decoder at end of input.
- Bench parameters in tools/server-bench set to chosen defaults.

## v0.1.7-perf.6 (2026-11-09)

Durable inter-agent steering and in-order delivery

Upstream base: d5290e9167cc.

- Automatic message delivery is resolved under the dispatch lock, in first-in, first-out order for ordinary and automatic work, without starving cross-agent messages.
- Native steering outcomes are tracked durably; confirmed undelivered messages are kept through Stop and restart, and worker capacity is reserved for Stop and dialog replies.
- Restart input is claimed when the native agent is admitted, cancellation stays with its owner through provider startup, and unsubmitted messages held through Stop are released once on Resume.
- Provider request artifacts are published atomically.
- Bench parameters in tools/server-bench set to chosen defaults.

## v0.1.6-perf.5 (2026-11-05)

Bounded reads and quieter pull request sync

Upstream base: 12ec17ce4c64.

- Message dispatch, subagent finalisation, continuation and turn start read only the rows they use.
- Pull request sync: GitLab mergeability is kept while unchanged, quiet merge requests are read less often, and branch lookups are shared within a turn.
- Bench parameters in tools/server-bench set to chosen defaults.

## v0.1.6-perf.4 (2026-11-04)

Hotfix: forge CLIs in their own pool, runs finish without waiting for PR refresh

Upstream base: 12ec17ce4c64.

- Forge CLIs (GitHub and GitLab) run in their own process pool; git's pool follows the machine's size.
- A run finishes without waiting for its pull request refresh.
- Bench parameters in tools/server-bench set to chosen defaults.

## v0.1.6-perf.3 (2026-11-04)

Admission gate, encode-once fan-out and fewer git commands per turn

Upstream base: 12ec17ce4c64.

- Server: an admission gate on event-loop delay, on by default.
- Events: each live update is encoded once for all subscribers; the live stream's hub owns resynchronisation.
- Memory: a scratch store for compaction; a larger V8 young generation by default, also for the packaged executable.
- Database: a new reader protocol between the server and its reader workers.
- Git: fewer git commands per turn; every command, not only git, goes through the spawner process.
- Checkpoints: a run records its workspace directory, and checkpoint scopes are per workspace.
- Background task interrupts settle cleanly.
- Fixes that came with the upstream merge: the Claude Agent SDK update and the spawner's refusal of a root working directory.
- Bench parameters in tools/server-bench set to chosen defaults.

## v0.1.6-perf.2 (2026-11-03)

Reader scheduling, a git spawner process and a shell delta hub

Upstream base: 60aec28a9c6f.

- Database: a work-conserving reader scheduler with aging and fair classes; a read's statements go to one reader worker together; the writer's page-cache size and tuning pragmas wait for another process's lock when the database opens.
- Processes: the server process no longer forks; a small spawner process runs git (on by default on Linux and macOS).
- Events: live shell deltas are projected once per batch and fanned out to every subscriber.
- Migrations: the fork's own migrations run in their own chain, apart from upstream's ids.
- Fix (upstream bug): a late checkpoint capture attributes its baselines to the run it captures.
- ACP: stdout lines are framed in linear time.
- Provider I/O: the Codex and ACP worker offload keeps in-thread timing and bounds pending reads by bytes.
- Bench parameters in tools/server-bench set to chosen defaults.

## v0.1.6-perf.1 (2026-11-01)

First performance release: database reads, event fan-out and provider I/O

Upstream base: 1d626d02d95f.

The first release of the performance fork.

- Database: the reader pool recovers from a failed start, sizes and routes itself, and serves thread projections; long reads no longer hold the write-ahead log open (short read transactions, decoding outside the transaction under a bounded permit); a message-ordinal index and maintained turn-item counts for thread shells (fork migrations); optional SQLite cache and mmap settings, off by default.
- Events: project events fan out once to every subscriber; shell deltas share one lookup; full thread snapshots are streamed.
- Provider I/O: provider output is parsed in pure stages, with optional stage timing; an optional worker pool runs the Claude Agent SDK, Codex and ACP output off the main thread (off by default); optional batched provider-event ingest; ACP requests reach the agent in issue order whatever the logging; sessions close within a bounded time.
- Git: config and remote lookups are cached; redundant git commands are gone; VCS processes can be spawned off the main thread, and the xcrun lookup is asynchronous.
- HTTP: a request whose client disconnects mid-response is released (a backport of an Effect fix).
- Fixes from a security review of the fork's changes.
- A server benchmark harness under tools/server-bench: a load driver with session, subscriber and poller load, fake Claude, Codex and ACP CLIs, a metrics collector, a snapshot client and a synthetic database fixture generator.
- Bench parameters in tools/server-bench set to chosen defaults.

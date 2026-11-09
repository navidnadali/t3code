# Changelog

Public release notes of this performance fork of T3 Code. Each release is one commit whose parents are the previous release and the upstream commit it is based on.

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

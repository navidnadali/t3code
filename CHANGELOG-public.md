# Changelog

Public release notes of this performance fork of T3 Code. Each release is one commit whose parents are the previous release and the upstream commit it is based on.

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

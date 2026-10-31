# Fake Codex app-server for server benchmarks

`codex` is a stand-in for `codex app-server` that the T3 server drives exactly like the real one:
through a `codex` provider instance, the Codex App Server client and the Codex adapter. It replays
recorded turns, so a bench can run many Codex sessions with no model and no network.
`codex --help` prints this file.

Register it in the server's `settings.json` (or let `node tools/server-bench/providers.ts settings`
write it, with the built-in instances disabled):

```json
"codexBench": {
  "driver": "codex",
  "config": { "binaryPath": "/path/to/tools/server-bench/fake-codex/codex", "homePath": "/scratch/codex-home" },
  "environment": [{ "name": "BENCH_CODEX_TRANSCRIPT", "value": "command_output_streaming:3,large-tool-payload:1" }]
}
```

Keep the launcher as `binaryPath`: the server wraps native binaries in the agent scope it gives the
real CLI, and a `.mjs` path would skip that.

## What it does

- Answers `--version`, the provider's probe (`initialize`, `account/read`, `model/list`,
  `skills/list`, `config/read`, `account/rateLimits/read`) and the session's requests
  (`thread/start`, `thread/resume`, `thread/fork`, `thread/read`, `turn/start`, `turn/steer`,
  `turn/interrupt`, `thread/unsubscribe`, `thread/goal/get|clear`, `thread/inject_items`,
  `mcpServerStatus/list`). Any other method answers `-32601`.
- One app-server serves every thread of its provider instance, as the real one does. After
  `initialized` it sends the recording's process-level notifications once; `thread/start` sends
  the thread's setup frames; `thread/resume` sends only MCP startup status.
- Each `turn/start` plays the thread's next recorded completed turn with the live thread id, turn
  id, cwd, model and prompt, fresh item ids and live timestamps. `turn/steer` and
  `turn/interrupt` hold to the turn they name; an interrupted turn ends `interrupted`.
- `BENCH_TURN_LOOPS` repeats a turn's work (everything between the prompt's echo and the final
  answer) with fresh item ids; the final answer plays once. A turn whose items are keyed by its
  turn id (Codex plans) plays once.

Not generated: anything that needs the client's answer. Recorded approval and question requests
are sent and wait for it, so bench transcripts should not hold any. `thread/read` returns no turns.
For the load driver, `BENCH_MCP_CREDENTIALS_DIR` hands over what the server puts in a thread's
config (`mcp_servers.t3-code`): the thread's T3 MCP URL and `Authorization` header, as
`{"url", "authorization"}` in `<dir>/<thread-id>.json` (replaced atomically, mode 0600 in a 0700
directory, never logged; a later thread/resume replaces it).

## Knobs

| Variable                    | Default                                                 | Meaning                                                                                                                                                                                                                                                             |
| --------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BENCH_CODEX_TRANSCRIPT`    | `command_output_streaming`                              | The mix: comma-separated `name` or `name:weight` (weight 1 if omitted). A name is a generator in `transcripts/` (`large-tool-payload`), a fixture with a `codex_transcript.ndjson` under `apps/server/src/orchestration-v2/testkit/fixtures/`, or an absolute path. |
| `BENCH_SEED`                | `0`                                                     | Rotates where the assignment cycle starts.                                                                                                                                                                                                                          |
| `BENCH_STATE_DIR`           | `$CODEX_HOME/fake-codex`, else `<tmpdir>/t3-fake-codex` | Where threads claim their ordinals.                                                                                                                                                                                                                                 |
| `BENCH_SPEED`               | `1`                                                     | Time scale for every wait; `0` never waits.                                                                                                                                                                                                                         |
| `BENCH_DELTAS_PER_SEC`      | `40`                                                    | Delta rate (`…/delta`, `…Delta` notifications) at speed 1.                                                                                                                                                                                                          |
| `BENCH_MAX_GAP_MS`          | unset                                                   | Caps every recorded wait between frames, before `BENCH_SPEED` scales it.                                                                                                                                                                                            |
| `BENCH_TURN_LOOPS`          | `1`                                                     | Times a turn's work plays before its final answer.                                                                                                                                                                                                                  |
| `BENCH_TOOL_PAYLOAD_BYTES`  | `262144`                                                | Size of each payload in the `large-tool-payload` transcript.                                                                                                                                                                                                        |
| `BENCH_STATS_DIR`           | unset                                                   | Append one JSON line per played turn, thread start and app-server start to `<dir>/fake-codex-<pid>.ndjson`.                                                                                                                                                         |
| `BENCH_MCP_CREDENTIALS_DIR` | unset                                                   | Write each thread's T3 MCP URL and authorization to `<dir>/<thread-id>.json` (see above).                                                                                                                                                                           |
| `BENCH_FAULT`               | unset                                                   | At the middle frame of the first turn an app-server plays: `truncated-frame` (a cut-off JSON line, then the turn goes on), `truncated-frame-exit` (then exit status 1), `bad-shape` (an item the schema rejects), `exit-mid-turn` (exit status 1).                  |
| `BENCH_NODE`                | `node` on `PATH`                                        | The node binary the launcher runs.                                                                                                                                                                                                                                  |

### Pacing

At speed 1 a frame waits the gap recorded before it (from the recording's `emittedAtMs`, capped at
3 s, or at `BENCH_MAX_GAP_MS`), and each delta waits 1/`BENCH_DELTAS_PER_SEC` s instead.
`BENCH_DELTAS_PER_SEC` is therefore a burst rate: a session's sustained rate is bounded by the
recorded gaps between bursts, a few frames a second for most fixtures at speed 1. To push more
frames per session, raise `BENCH_SPEED` or set `BENCH_MAX_GAP_MS`; to push bigger frames, use
`large-tool-payload`.

## Transcript assignment

A thread claims the next free ordinal in `BENCH_STATE_DIR` at `thread/start` (a resumed thread
keeps its ordinal, also across an app-server restart) and plays transcript
`cycle[(ordinal + BENCH_SEED) % cycle.length]`, its completed turns in order, where the cycle
spreads the weights evenly (smooth weighted round-robin). Whatever ids the server picks, the same
mix, seed and thread count from a fresh state directory play the same transcripts and turns; start
each run with a fresh state directory (a fresh `homePath` does that).

## Stats

Each line has `kind`: `turn` (one played turn: `ordinal`, `seed`, `mix`, `transcript`, `turn`,
`loops`, `payloadBytes`, `aborted`, `frames` by method, `bytes`, `reply`, `ms`,
`sessionRequests`), `thread-start` (a thread's answer and setup frames) or `process-start` (the
frames after `initialized`). The lines add up to every byte the app-server sends. `process`
holds the app-server's RSS and request counts, which cover every thread it serves: never sum
them across lines.

## Tests

`vp test run tools/server-bench/fake-codex/codex.test.mjs`

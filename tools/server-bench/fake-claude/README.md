# Fake Claude CLI for server benchmarks

`claude` is a stand-in for Claude Code that the T3 server drives exactly like the real CLI: through
the Agent SDK, the Claude adapter and the server's process wrapping. It replays recorded turns, so a
bench can run many Claude sessions with no model and no network. `claude --help` prints this file.

Point a `claudeAgent` provider instance at the launcher in the server's `settings.json`:

```json
"claudeBench": {
  "driver": "claudeAgent",
  "config": { "binaryPath": "/path/to/tools/server-bench/fake-claude/claude", "homePath": "/scratch/claude-home" },
  "environment": [{ "name": "BENCH_TRANSCRIPT", "value": "command_output_streaming:3,large-tool-payload:1" }]
}
```

Keep the launcher as `binaryPath`: the server wraps native binaries in the agent scope it gives the
real CLI, and a `.mjs` path would skip that. To keep a bench free of real providers, also disable
the default instances (`codex`, `claudeAgent`, …) and set `textGenerationModelSelection` to the
bench instance; the fake answers `-p --json-schema` with a minimal schema-valid `structured_output`.

## What it does

- Answers `--version` and the SDK's stream-json control requests (`initialize`, `get_usage`,
  `mcp_set_servers`, `set_model`, `set_permission_mode`, `interrupt`, `background_tasks`, …;
  unknown ones succeed).
- Each stdin user message plays the session's next recorded turn, then the wake turns recorded
  after it. The session id comes from `--session-id` or `--resume`, the prompt uuid the result
  echoes is the live prompt's, and every other recorded id is fresh on each play and each loop.
- A prompt sent with `priority: "now"` or an `interrupt` ends the running turn with an
  `error_during_execution` result, as the CLI does.

Not generated: the per-session MCP traffic a real agent sends to the server's `/mcp` (the load
driver, P1-004, models it), permission prompts (run bench threads in full access), hook callbacks
and SDK MCP calls.

## Knobs

| Variable                   | Default                                                          | Meaning                                                                                                                                                                                                                                             |
| -------------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BENCH_TRANSCRIPT`         | `command_output_streaming`                                       | The mix: comma-separated `name` or `name:weight` (weight 1 if omitted). A name is a file in `transcripts/` (`.ndjson`, or a `.mjs` generator), a fixture under `apps/server/src/orchestration-v2/testkit/fixtures/`, or an absolute `.ndjson` path. |
| `BENCH_SEED`               | `0`                                                              | Rotates where the assignment cycle starts.                                                                                                                                                                                                          |
| `BENCH_STATE_DIR`          | `$CLAUDE_CONFIG_DIR/fake-claude`, else `<tmpdir>/t3-fake-claude` | Where sessions claim their ordinals.                                                                                                                                                                                                                |
| `BENCH_SPEED`              | `1`                                                              | Time scale for every wait; `0` never waits.                                                                                                                                                                                                         |
| `BENCH_DELTAS_PER_SEC`     | `40`                                                             | `stream_event` delta rate at speed 1.                                                                                                                                                                                                               |
| `BENCH_TURN_LOOPS`         | `1`                                                              | Times a turn's body plays before its result.                                                                                                                                                                                                        |
| `BENCH_TOOL_PAYLOAD_BYTES` | `262144`                                                         | Payload size of the `large-tool-payload` transcript.                                                                                                                                                                                                |
| `BENCH_STATS_DIR`          | unset                                                            | Append one JSON line per played turn to `<dir>/fake-claude-<pid>.ndjson`.                                                                                                                                                                           |
| `BENCH_FAULT`              | unset                                                            | One malformed frame halfway through the first turn's body: `truncated-frame` (a cut-off JSON line, then the turn goes on), `truncated-frame-exit` (then exit status 1), `bad-shape` (a JSON assistant frame without a message).                     |
| `BENCH_NODE`               | `node` on `PATH`                                                 | The node binary the launcher runs.                                                                                                                                                                                                                  |

At speed 1 a turn waits its recorded time to first token (capped at 3 s, 800 ms when unrecorded)
before each message, 1/`BENCH_DELTAS_PER_SEC` s before each delta, 250 ms before each tool result,
and 500 ms before each assistant message of a transcript without deltas.

## Transcript assignment

A session claims the next free ordinal in `BENCH_STATE_DIR` at its first prompt (a resumed session
keeps its ordinal) and plays transcript `cycle[(ordinal + BENCH_SEED) % cycle.length]` for all its
turns, where the cycle spreads the weights evenly. Whatever ids the server picks, every whole cycle
of `total weights` sessions plays each transcript exactly `weight` times, and N sessions from a
fresh state directory play each within one of `N × weight / total weights` (within two when
`BENCH_SEED` moves the start). Start each run with a fresh state directory (a fresh `homePath` does
that) and the same seed for the same counts. Every stats line records the session's `ordinal`,
`transcript`, `turn`, `seed` and the configured `mix`.

The fake loads every transcript in the mix before it reads stdin. A bad knob or transcript makes it
print an error `result` carrying the cause and exit 1, so the server reports that cause.

## Tests

`vp test run tools/server-bench/fake-claude/claude.test.mjs`

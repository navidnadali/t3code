# Replaying ACP agent for server benchmarks

`acp-agent` is an ACP agent the T3 server drives exactly like a real one: through an `acpRegistry`
provider instance with a local command, the ACP client and the ACP adapter. It replays recorded
prompt turns, so a bench can run many ACP sessions with no model and no network.
`acp-agent --help` prints this file.

Register it in the server's `settings.json` (or let `node tools/server-bench/providers.ts settings`
write it, with the built-in instances disabled):

```json
"acpBench": {
  "driver": "acpRegistry",
  "config": { "source": "local", "commandPath": "/path/to/tools/server-bench/fake-acp/acp-agent", "commandArgs": [] },
  "environment": [{ "name": "BENCH_ACP_TRANSCRIPT", "value": "todo_list:3,large-tool-payload:1" }]
}
```

Threads on it use the model `bench-replay`.

## What it does

- Answers `initialize`, `authenticate`, `session/new`, `session/load`, `session/set_model`,
  `session/set_mode`, `session/set_config_option`, `session/prompt`, `session/close` and the
  `session/cancel` notification. The server starts one agent process per session.
- Each `session/prompt` plays the session's next recorded, uncancelled prompt turn: the frames
  the dev replay agent (`apps/server/scripts/dev-replay-agent.ts`) parses and replays from a
  registry transcript (message, thought, tool, plan and usage updates), text streamed in small
  chunks, with the live session id and cwd and fresh tool call ids. Prompts on a session play in
  order, each answered before the next starts; `session/cancel` stops every prompt sent before it.
- `BENCH_TURN_LOOPS` repeats the whole turn before the prompt's answer, with fresh tool call ids.

Not generated: anything that needs the client's answer. Recorded permission and elicitation
requests are sent and wait for it, so bench transcripts should not hold any. For the load driver,
`BENCH_MCP_CREDENTIALS_DIR` hands over the `t3-code` MCP server the server sends in
`session/new` or `session/load` (HTTP, or the stdio bridge's `T3_ACP_MCP_*` env): its URL and
`Authorization`, as `{"url", "authorization"}` in `<dir>/<session-id>.json` (replaced atomically,
mode 0600 in a 0700 directory, never logged).

## Knobs

| Variable                    | Default                | Meaning                                                                                                                                                                                                                                                                |
| --------------------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BENCH_ACP_TRANSCRIPT`      | `todo_list`            | The mix: comma-separated `name` or `name:weight` (weight 1 if omitted). A name is a generator in `transcripts/` (`large-tool-payload`), a fixture with a `registry_transcript.ndjson` under `apps/server/src/orchestration-v2/testkit/fixtures/`, or an absolute path. |
| `BENCH_SEED`                | `0`                    | Rotates where the assignment cycle starts.                                                                                                                                                                                                                             |
| `BENCH_STATE_DIR`           | `<tmpdir>/t3-fake-acp` | Where sessions claim their ordinals; the load driver sets a fresh one per run.                                                                                                                                                                                         |
| `BENCH_SPEED`               | `1`                    | Time scale for every wait; `0` never waits.                                                                                                                                                                                                                            |
| `BENCH_DELTAS_PER_SEC`      | `40`                   | Text chunk rate at speed 1.                                                                                                                                                                                                                                            |
| `BENCH_MAX_GAP_MS`          | unset                  | Caps every other wait, before `BENCH_SPEED` scales it.                                                                                                                                                                                                                 |
| `BENCH_TURN_LOOPS`          | `1`                    | Times a prompt turn plays before its answer.                                                                                                                                                                                                                           |
| `BENCH_TOOL_PAYLOAD_BYTES`  | `262144`               | Size of each tool output in the `large-tool-payload` transcript.                                                                                                                                                                                                       |
| `BENCH_STATS_DIR`           | unset                  | Append one JSON line per played prompt to `<dir>/fake-acp-<pid>.ndjson`.                                                                                                                                                                                               |
| `BENCH_MCP_CREDENTIALS_DIR` | unset                  | Write each session's T3 MCP URL and authorization to `<dir>/<session-id>.json` (see above).                                                                                                                                                                            |
| `BENCH_FAULT`               | unset                  | At the middle frame of the first prompt the process plays: `truncated-frame`, `truncated-frame-exit`, `bad-shape` (an update the schema rejects) or `exit-mid-turn` (exit status 1).                                                                                   |
| `BENCH_NODE`                | `node` on `PATH`       | The node binary the launcher runs.                                                                                                                                                                                                                                     |

### Pacing

At speed 1 a non-text update waits its recorded `afterMs` (capped at 2 s) or 250 ms, and each
text chunk 1/`BENCH_DELTAS_PER_SEC` s. `BENCH_DELTAS_PER_SEC` is therefore a burst rate: the
waits between updates bound a session's sustained rate. Raise `BENCH_SPEED`, set
`BENCH_MAX_GAP_MS`, or use `large-tool-payload` for bigger frames.

## Transcript assignment

A session claims the next free ordinal in `BENCH_STATE_DIR` at its first prompt (a probe's session
never prompts) and plays transcript `cycle[(ordinal + BENCH_SEED) % cycle.length]`, its prompt
turns in order, where the cycle spreads the weights evenly (smooth weighted round-robin). The same
mix, seed and session count from a fresh state directory play the same transcripts and turns.
The load driver (`driver.ts`) owns it: before any session opens it sets each planned bench instance's `BENCH_STATE_DIR` to a fresh `<--out>/fake-state/<instance>`, so runs that restore the same settings replay the same plan. Outside the driver, give every run a fresh one.

## Stats

Each line is one played prompt (`kind: "turn"`): `ordinal`, `seed`, `mix`, `transcript`, `turn`,
`loops`, `payloadBytes`, `aborted`, `frames` by update kind, `bytes`, `reply`, `ms` and
`sessionRequests`. `process` holds the agent process's RSS and request counts, which include the
client's probes.

## Tests

`vp test run tools/server-bench/fake-acp/acp-agent.test.mjs`

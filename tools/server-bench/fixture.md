# Bench fixtures

`node tools/server-bench/fixture.ts <command>` (Node 26.12.1, after `vp i`). Every command prints `--help`.
Server code runs in-process: the server's own migrations, event sink, projection store and id rules.

| Command                                               | Does                                                                                                          |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `sanitise <source> <destination>`                     | Copies a database with `VACUUM INTO` (the source is opened read-only), makes it safe to boot, self-checks it. |
| `synth --like <shape.json> --scale <f> <destination>` | Builds a database of a shape's schema and shape.                                                              |
| `shape <database>`                                    | Prints a database's shape as JSON (counts and sizes only, no row content).                                    |
| `scan <database>`                                     | The self-check on its own: exits 1 unless the database is clean.                                              |

Both writers refuse a destination (or `--report` file) inside `~/.t3` or `$T3CODE_HOME` (symlinks resolved), an
existing file, the source itself or its `-wal`/`-shm`/`-journal`, and a report path the run writes as a database
(exit 2). A failed run leaves no destination behind.

## sanitise

1. `VACUUM INTO` a scratch file beside the destination; run the server's migrations on it.
2. Find live threads with the server's own queries (`getRecoveryThreadIds` for queued runs, runtime, subagent
   results and delegated completions; `getLimitRecoveryCandidates`) plus, as `migrate-dev-db` does, threads whose
   latest run failed, that carry `limitRecovery`, or that keep a pull request watch (resumed on boot). Drop their
   whole families (lineage root) from every table with
   a `thread_id`, their provider sessions, context transfers, subagent rows and `orchestration_events`, and keep
   `orchestration_v2_projection_metadata` at the remaining high-water mark.
3. Clear the `migrate-dev-db` tables (`scheduled_tasks`, `orchestration_v2_effect_outbox`,
   `orchestration_v2_thread_launch_workflows`, `orchestration_command_receipts`, `provider_session_runtime`,
   `auth_sessions`, `auth_pairing_links`, `peer_environment_links`) plus `scheduled_task_webhook_deliveries` and
   `scheduled_task_webhook_relay_deliveries`, and every table whose foreign keys reach one of them.
4. Set `projection_projects.auto_pull = 0` (auto-pull fetches remotes on boot). Through the server's event sink
   (one `thread.metadata-updated` per thread, `updatedAt` kept): turn automatic settlement off on every thread the
   settle sweep could pick (no settle event, no settle hook), and clear pull request links and the branch, which
   the pull request sync and branch discovery read from the host and git.
5. Redact credential-shaped text (`fixture/secrets.ts`) in every text column: letters and digits become `*`,
   punctuation and escapes stay (JSON keeps parsing at any nesting depth, sizes keep their shape); private keys go
   as whole blocks inside the decoded string that holds them (to the footer, else to the end of that string, JSON
   encodings unwrapped) and an assignment loses only its value.
6. Rebuild the message search index, `VACUUM INTO` the destination (no free pages keep deleted text), `chmod 600`.
7. Self-check, else delete the destination and fail: no pattern hit in any text column or in the raw file bytes, no
   non-null credential-named column, and nothing a server would run on boot (recovery threads, scheduled tasks,
   outbox, launch workflows, peer links, sessions, pairing links, provider runtime, auto-pull, `limitRecovery`,
   pull request watches, pull request links or branches, settlement candidates).

Settings and `secrets/` live outside the database; copy only `statev2.sqlite`. Never boot a sanitised copy on the
machine its data came from: thread worktree paths still point at real folders.

## synth

Plans every row from the shape, then writes it through the server's `EventSink` (event store, projections, turn
item positions, search index triggers) on a database created by the server's own `Sqlite.layerFromPath`, then
compacts it with `VACUUM INTO`. It reports rows and bytes per table against the shape times `--scale`, and fails
if any table differs from its own plan or the result is not clean (as `scan`).

- `--scale` multiplies projects, threads and every row count; each thread keeps the shape's turn-items-per-thread
  distribution (stratified draws), so long-history threads stay long at any scale.
- Threads: roots, forks (branched from a run of a root, with a consumed `fork` transfer), subagent children (spawned by a `subagent` item of a larger root, sharing its project, closed by
  a consumed `subagent_result` transfer), v1 imports (run-less history with the importer's ids, its
  `orchestration_v2_legacy_imports` row and the v1 `projection_threads` / `projection_thread_messages` rows) and
  run-less children. Every thread is settled the way the server's sweep settles it, so a boot writes nothing.
- Runs carry attempts, a root node and child nodes, provider turns, a provider thread and stopped sessions; turn
  items follow the shape's type mix (types this generator does not write become `system_notice`, reported) and its
  payload size spread; checkpoints share one root scope per thread.
- Ids follow the server's `IdAllocator` (`derive.run`, `derive.providerTurn`, …) and Claude-style native ids, so
  index and payload sizes match. Text comes from a fixed word list: no data, nothing credential-shaped.
- Events: one per entity, plus the shape's extra updates per event type (compactable types stay at first + latest,
  so the store's compaction finds nothing to delete).
- Not written: tables the generator has no model for (pull request links, v1 sessions, …) show as `0 / target`.
- `--provider`, `--driver`, `--model` set the instance threads use (default `claudeAgent`, `claude-sonnet-4-6`);
  project folders sit under `--workspace-root` (default `/t3-bench-synthetic`, never created).

## Shape file

JSON, `format: "t3-server-bench-shape"`, `version: 1`; `shape <database>` writes it, `fixture/example-shape.json`
is a small example. Quantiles are `[q, value]` pairs, q ascending in 0..1, ending at `q = 1` (the maximum).

| Field                              | Meaning                                                                             | synth uses it for                                  |
| ---------------------------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------- |
| `source`                           | `label`, `capturedAt` (ISO)                                                         | newest synthetic activity time (`--now` overrides) |
| `pageSize`, `pageCount`            | main file after `VACUUM INTO`                                                       | the size target                                    |
| `tables.<name>.rows`               | row count per table                                                                 | per-table targets                                  |
| `tables.<name>.bytes`              | table plus its indexes, from `dbstat`                                               | reported bytes target                              |
| `tables.<name>.payload`            | `{ column, mean, quantiles }` of `length(payload_json)` (or `text`, …)              | item sizes, checkpoint sizes, v1 message text      |
| `stat1`                            | `sqlite_stat1` rows (`ANALYZE` the copy first; a server boot empties it)            | recorded only                                      |
| `distributions.turnItemsPerThread` | items per thread, over all threads                                                  | thread lengths                                     |
| `distributions.threadsPerProject`  | threads per project                                                                 | project sizes                                      |
| `lineage`                          | thread counts: `root`, `fork`, `subagent`, `v1Import`, `runless` (items but no run) | thread roles                                       |
| `mix.turnItemTypes`                | `{ rows, meanBytes }` per turn item type                                            | item kinds and sizes                               |
| `mix.eventTypes`                   | `{ rows, meanBytes }` per v2 thread event type                                      | extra events, first-event sizes                    |
| `mix.applicationEvents`            | `orchestration_events` by `v<version>/<aggregate>`                                  | recorded only                                      |

Only `format`, `version`, `pageSize`, `pageCount` and `tables.*.rows` are required; missing distributions and
mixes fall back to heavy-tailed defaults.

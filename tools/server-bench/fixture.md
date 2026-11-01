# Bench fixtures

`node tools/server-bench/fixture.ts <command>` (Node 26.12.1, after `vp i`). Every command prints `--help`.
Server code runs in-process: the server's own migrations, event sink, projection store and id rules.

| Command                                               | Does                                                                                                                         |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `sanitise <source> <destination>`                     | Copies a database with `VACUUM INTO` (the source is opened read-only), blanks its content, makes it safe to boot, checks it. |
| `synth --like <shape.json> --scale <f> <destination>` | Builds a database of a shape's schema and shape.                                                                             |
| `shape <database>`                                    | Prints a database's shape as JSON (counts and sizes only, no row content).                                                   |
| `scan <database>`                                     | The self-check on its own: exits 1 unless the database is clean.                                                             |

Both writers resolve every path physically (each component through its symlinks before the next, so
`link/../x` is beside the link's target), then refuse a destination or `--report` file inside `~/.t3` or
`$T3CODE_HOME`, a dangling symlink, an existing file, the source itself or its `-wal`/`-shm`/`-journal`, and a
report path the run writes as a database (exit 2). They write exactly the resolved path, created exclusively
(`O_EXCL`, so not through a symlink) and `0600`. A failed or interrupted run (SIGINT exits 130, SIGTERM 143)
removes what it created; SIGKILL cannot, and leaves a `.fixture-sanitise-*` (or `.fixture-synth-*`) directory
beside the destination, which holds an unsanitised copy until deleted. A signal is handled between batches of
the long passes (2,000 rows); a single SQLite step (`VACUUM INTO`, a migration, the server's recovery) finishes
first.

The source may be the live `~/.t3/userdata/statev2.sqlite`: `VACUUM INTO` reads one consistent snapshot through a
read-only connection while the server writes. For a fixture that leaves the machine, copy it first (as P1-002
does) and sanitise the copy.

`tsc -p tools/server-bench --noEmit` runs in no script or CI job: run it by hand after changing these files.

## sanitise

Every thread, run, item, message and event stays. The work a boot would do on start is done now, the way the
server does it, so the fixture keeps its production scale and a boot writes nothing:

1. Make a private (`0700`) scratch directory beside the destination and `VACUUM INTO` a `0600` scratch file
   there; run the server's migrations on it.
2. Delete what is unsafe or unneeded, and nothing else (counts in the report's `clearedRows`):
   - credentials and links: `auth_sessions`, `auth_pairing_links`, `peer_environment_links`,
     `provider_session_runtime` (live provider handles);
   - queued work, as `migrate-dev-db` does: `scheduled_tasks` (and every table whose foreign keys reach it:
     `scheduled_task_webhook_pending`, `scheduled_task_webhook_replay_keys`), `orchestration_v2_effect_outbox`,
     `orchestration_v2_thread_launch_workflows`, `orchestration_command_receipts`;
   - webhook requests (headers, bodies, signatures): `scheduled_task_webhook_deliveries`,
     `scheduled_task_webhook_relay_deliveries`;
   - pull request state read from the host: `projection_thread_pull_requests`, `pull_request_files_viewed`,
     `pull_request_review_guides`.

   Then `projection_projects.auto_pull = 0` (auto-pull fetches remotes on start) and v1
   `projection_thread_sessions` to `stopped`.

3. Run the server's own start-up work in-process: the v1 importer (`reconcileShells`,
   `importPendingTranscripts`), then the restart recovery (`ProviderRuntimeRecoveryService.reconcile("startup")`,
   continuation off): active runs, attempts, nodes, provider turns and items cancelled, pending runtime requests
   closed, provider threads idle, sessions stopped, unheld queues held.
4. Settle what a boot would still deliver or finish, as the server leaves it once done (P1-002's production
   sanitiser is the reference): remote delegated completions lose their pending delivery, subagent completion
   deliveries are disposed, messages still streaming are final, a finished app-owned subagent child gets its
   task's result (`[bench fixture] Result withheld…`) and a consumed `subagent_result` transfer to its parent,
   and the task, its node and the parent's mirrored item end as the child's run did; a task delegated to another
   machine gets a result and `stoppedThere` (its follower would resume it). What the restart recovery leaves open
   and the runtime recovery query still lists is then closed: pending runtime requests (answerable by a message)
   expire with the nodes they hold, and open tool and subagent items, run-less subagent root turns,
   provider-native subagents, live provider threads and sessions are cancelled or stopped. Rows no boot would
   touch keep their state.
   A v1 transcript the importer cannot copy is marked imported (the server retries it every start).
5. Through the server's event sink (one `thread.metadata-updated` per thread, `updatedAt` kept): automatic
   settlement off on every thread the settle sweep could pick (no settle event, hook or `git`), pull request
   links and the branch cleared (the pull request sync and branch discovery read the host and `git`), snoozes
   removed, and a usage-limit failure marked handled with auto-resume off (`limitRecovery` set to its run and
   reset time, as a Stop leaves it), so the recovery worker never picks it.
6. Run the server's event-store compaction (`ProjectionMaintenance.compactEventStore`), as a first boot would.
   Then the server's own recovery queries must find nothing to resume, recover or deliver, and nothing else may
   be due on start (the list under Self-check), else the run fails here.
7. Content pass (below): every string outside the allowlist becomes filler, BLOBs become zeros of their length.
   It streams each table in rowid order, a batch at a time (at most 2,000 rows and 64 MiB plus one row), rewrites
   in JavaScript and writes rows back, committing each batch, so its memory does not grow with the database (a
   user function in one UPDATE would hold the whole table: node:sqlite keeps every value it sees until the
   statement ends). It prints rows and MiB per table every 50,000 rows and at each table's end. Triggers are
   dropped for the pass and restored, then the message search index is rebuilt from the filler, and P1-014's
   `orchestration_v2_projection_turn_item_counts` (kept, not blanked) is recounted from the turn items.
   Every v2 projection payload and thread event is decoded with the server's schemas before and after, row by
   row (flat memory on threads with millions of items); a row that decoded before and not after fails the run
   (nothing written).
8. `VACUUM INTO` the destination: no free pages, so no deleted or blanked text survives on disk.
9. Self-check, else delete the destination and fail (below).

The report (`--report`) lists every table's rows in the source and the fixture. On ws1's database every table but
the ones in step 2 keeps its row count; events change by the step-5 updates less the compaction (−14 of 107,221),
and the file is 1.6 % smaller. Settings and `secrets/` live outside the database; copy only `statev2.sqlite`.

### Content blanking (default)

A denylist of credential patterns can never be shown complete, so content goes by construction: every stored
string, in every column of every table, is replaced unless the allowlist keeps it. JSON (any object or array,
including JSON encoded inside a string, at any depth) is walked: numbers, booleans, nulls, key order and
formatting stay; each string value is judged by the key it sits under (array items inherit it). Text that is not
JSON is judged by its column name the same way.

Kept as written (`fixture/content.ts`):

| Kept                          | When                                                                                                                                                                                                           |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Known literals                | the whole string is a string literal or enum value declared in `@t3tools/contracts` (≤ 64 chars)                                                                                                               |
| Timestamps                    | ISO 8601 date or date-time                                                                                                                                                                                     |
| Ids                           | key `id`/`ids`/`uuid`, `…Id`/`…Ids`, `…_id`/`…_ids`; value without whitespace, ≤ 4096 chars                                                                                                                    |
| `type`/`kind`/`status`/`role` | key is one of these or ends in one (`event_type`, `itemKind`); value `[A-Za-z][A-Za-z0-9_.:/-]{0,63}`                                                                                                          |
| Model and provider names      | key ends in `model`/`provider`/`driver` (also `…Name`, `…Slug`); value without spaces, ≤ 128                                                                                                                   |
| Git hashes                    | key ends in `sha`/`hash`/`oid`/`commit`/`revision`; value 7–64 hex digits                                                                                                                                      |
| Object keys                   | identifiers `[A-Za-z_$@][A-Za-z0-9_$@.:-]{0,63}` the pattern layer does not flag; any other key becomes key filler: a seed and the letters it fixes, a `.` every eighth character, one per byte (at least ten) |
| Structural tables             | `effect_sql_migrations`, `orchestration_v2_projection_metadata` and `orchestration_v2_projection_turn_item_counts`, whole                                                                                      |

A value under a credential-named key (`password`, `token`, `secret`, `api_key`, `auth`, …) keeps only a known
literal. Under a credential-holding ancestor (`credentials`, `auth`, `token`, `secret`, `cookie`, `pin`, …; not
`author`, token counts or a bare `pass`) nothing is kept but literals and timestamps, ids included, and JSON
numbers become 0 (a PIN, an OTP); numbers under such a key itself are zeroed too, and the check reports any left
(`credential-number`). Object keys shaped like tokens (an unprefixed run of 16+ letters and digits mixed like
noise) are rewritten; camelCase names, MCP tool names and vendor-prefixed ids (`toolu_…`) stay. The pattern layer
(`fixture/secrets.ts`) still reads every kept value and key, and a kept value it flags is blanked after all.

What the allowlist still keeps by design: any value without whitespace under an id key (up to 4,096 characters),
an identifier under `type`/`kind`/`status`/`role`, a name under `model`/`provider`/`driver`, 7–64 hex digits under
`sha`/`commit`/…, and identifier object keys up to 64 characters. An unprefixed random secret placed in one of
those (outside a credential-holding ancestor, and not shaped like a known token) survives. So do ids that act as
access links: a Google Drive or Docs file id kept as an id opens the file for anyone it is shared with by link.
Treat a fixture as data that still carries ids, not as public.

Filler keeps the size: the same UTF-8 bytes and UTF-16 length, and in place every control character (newlines,
tabs, escapes), `"` and `\`, so lines and JSON escaping survive. Between those, text becomes pseudo-words: an
all-ASCII segment of 13 or more characters starts with a seed (three short words) that fixes the rest, a shorter
one is a fixed string, and a segment with wider characters is a fixed stream where a 2, 3 or 4 byte character
becomes `é`, `あ` or `𝑥` (a no-break or ideographic space where the stream has a space). So the check can
regenerate any filler without the original. Seeds come from an HMAC with a random key per run, never stored:
equal texts get equal filler within a run (duplicates stay duplicates) and filler cannot be matched against a
guessed original. The letters cannot spell a credential word (no c, h, k, t, w or y, never two consonants
together), words run at most seven letters, and there are no digits, capitals, `:` or `=`, so no pattern of
either check can match in or right after filler. On ws1's database payload size quantiles are unchanged; thread
payloads lose their pull request snapshots (step 5), and the message search index is 14 % smaller on
ws1 (14–19 % on the production copy: fewer distinct words), so search benches on a fixture will not match production.

`--keep-content` skips blanking and runs the pattern layer alone (every visible character of a finding becomes
`*`). It fails open by nature: use it only for a copy that never leaves the machine. `scan --keep-content`
checks such a copy; synth output is checked the same way.

### Pattern layer

Matched on decoded strings (JSON unwrapped, ANSI colour codes read as spaces), so a token right after `\n`, a tab
or a colour code is found: private key blocks (whole, to the footer or the end of the string, and markers and
60–76 column base64 body lines on their own, for a key split across strings), token formats
(GitHub, GitLab, Anthropic, OpenAI, AWS, Google API and OAuth `ya29.`, Slack `xox?-`/`xapp-`/webhooks, Atlassian
`ATATT`, Stripe, npm, Hugging Face, SendGrid, Mailgun, TokenFeed, JWT, `Bearer`, `Basic` that decodes to
`user:password`, URLs with a password, the user name possibly empty), assignments whose name contains
`password`, `passwd`, `passphrase`, `secret`, `token`, `credential`, `api_key`, `access_key`, `private_key` or
`auth`, or is `key`/`pass`/`pwd` between separators (`DB_PASSWORD=`, `export CF_API_TOKEN=`, `"refresh_token":`,
`session_token:`; a plain number is not a credential), and high-entropy runs: 24 or more of `[A-Za-z0-9+/=_-]`
with upper case, lower case and digits, Shannon entropy ≥ 3.5 bits and no run of seven lower-case letters (a word,
so a path or a name; URL escapes split runs). Hex digests and UUIDs have one letter case and pass as ids.

### Self-check (`scan`)

Three independent gates, on every value of every table:

- Content: each string is allowlisted and unflagged by the pattern layer, or filler; every key is an identifier or
  rewritten; every BLOB is zeros (`notBlanked`, `secretHits`).
- P1-002's extended check (`fixture/extended.ts`, a rule-for-rule port of `p1002_extended_check.py` with the 52
  patterns of `p1002_sanitise.py`): every value as stored and, when JSON, every decoded string with keys and
  nested JSON, the search index's shadow tables included; keyword, env-var and name=value high-entropy
  assignments, extra token forms, PEM markers, base64 body lines and arrays of them (`extendedHits`, by
  `table:scope|kind`). On ws1's source copy the port and the Python script give the same count in every row.
- Raw file bytes: the token patterns find nothing (free pages and indexes included, escapes read as spaces), and
  no credential-named column is non-null.

And nothing a server would run on start: recovery threads (runtime, subagent results, delegated completions),
startable queued runs, settlement candidates, usage-limit recovery and snoozes, open remote delegations, pending subagent deliveries,
streaming messages, live v1 sessions, scheduled tasks, outbox, launch workflows, peer links, sessions, pairing
links, provider runtime, auto-pull, pull request watches, links or branches, and pending v1 shell and transcript
imports (`runnable`). Anything planted after sanitising, a token in an id or prose in a message, fails it.

### Booting a fixture

Boot it with its own `--base-dir` and `HOME`, never on the machine its data came from without one. With content
blanked, project and worktree paths point nowhere. A first boot of a sanitised fixture writes no event: the
start-up work (step 3), deliveries (4), settle sweep, pull request sync and timers (5) and compaction (6) are
done, and the recovery queries find nothing (on ws1's fixture: no event, row or sequence change, all recovery
counts 0). It may still run `git`: read-only repository identity probes per project (the re-review counted 81 on
the production copy; on ws1's fixture `GIT_TRACE` recorded none in the first 20 s after ready). At the blanked
paths they find no repository. For bench runs (P1-007) set, in `<base-dir>/userdata/settings.json`,
`"enableProviderUpdateChecks": false` (no model manifest fetch), `"sidebarAutoSettleAfterDays": null` and
`"sidebarAutoSettleOnMerge": false` (no settle sweep on threads the bench creates), and the environment variable
`T3CODE_TELEMETRY_ENABLED=false`. The identity probes cannot be switched off.

## synth

Plans every row from the shape, then writes it through the server's `EventSink` (event store, projections, turn
item positions, search index triggers) on a database created by the server's own `Sqlite.layerFromPath`, then
compacts it with `VACUUM INTO`. It reports rows and bytes per table against the shape times `--scale`, and fails
if any table differs from its own plan or the result is not clean (as `scan --keep-content`: generated text is
not filler). "A boot writes nothing" holds for synth output as written and for sanitised copies (steps 3–6).

- `--scale` multiplies projects, threads and every row count; each thread keeps the shape's turn-items-per-thread
  distribution (stratified draws), so long-history threads stay long at any scale.
- Threads: roots, forks (branched from a run of a root, with a consumed `fork` transfer), subagent children (spawned by a `subagent` item of a larger root, sharing its project, closed by
  a consumed `subagent_result` transfer), v1 imports (run-less history with the importer's ids, its
  `orchestration_v2_legacy_imports` row and the v1 `projection_threads` / `projection_thread_messages` rows) and
  run-less children. Every thread is settled the way the server's sweep settles it, so a boot writes nothing.
- Runs carry attempts, a root node and child nodes, provider turns, a provider thread and stopped sessions; turn
  items follow the shape's type mix (types this generator does not write become `system_notice`, reported) and its
  payload size spread; checkpoints share one root scope per thread.
- Ids follow the server's `IdAllocator` (`derive.run`, `derive.providerTurn`, …) and Claude-style native ids
  (14 random base62 characters, redrawn while they spell a credential word, then an 8-digit counter), so index
  and payload sizes match. Text comes from a fixed word list: no data, nothing credential-shaped.
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

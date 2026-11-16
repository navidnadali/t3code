# Rolling back from 0.1.9-perf.9 with merged completion wakes

0.1.9-perf.9 merges delegated-completion wakes across parent runs: one queued wake can carry the results
of several parent runs. Its message records the first parent run in
`delegatedCompletion` and the others in `delegatedCompletion.merged`, and each of those parent runs
points its delivery at that one message.

A build before 0.1.9-perf.9 (such as `0.1.9-perf.7` or `0.1.9-perf.8`) ignores `merged`. It settles only
the wake's own parent run. The other parent runs keep their results claimed and are never offered
again, so those results are lost to the agent. Before such a build starts on a database that 0.1.9-perf.9
ran, settle the merged wakes with
[`scripts/merged-completion-wake-settle.sql`](../../scripts/merged-completion-wake-settle.sql).

## Steps

1. Stop the server and anything else that opens the database, such as `t3` CLI commands.
2. Copy `<T3 home>/userdata/statev2.sqlite` with its `-wal` and `-shm` files.
3. Count the merged wakes (prints `0` when there is nothing to do):

   ```sh
   sqlite3 <T3 home>/userdata/statev2.sqlite \
     "SELECT COUNT(*) FROM orchestration_v2_projection_messages
      WHERE json_type(payload_json, '$.delegatedCompletion.merged') = 'array'"
   ```

4. Run the script:

   ```sh
   sqlite3 -bail <T3 home>/userdata/statev2.sqlite < scripts/merged-completion-wake-settle.sql
   ```

   Without the `sqlite3` CLI, use the Node the server runs on:

   ```sh
   node -e 'new (require("node:sqlite").DatabaseSync)(process.argv[1]).exec(require("node:fs").readFileSync(process.argv[2], "utf8"))' \
     <T3 home>/userdata/statev2.sqlite scripts/merged-completion-wake-settle.sql
   ```

5. Run step 3 again. It prints `0`.
6. Start the older build.

The script runs in one `BEGIN IMMEDIATE` transaction. A second run changes nothing.

## What it does

For each merged wake, it decides from the wake's run whether the provider received the prompt. The
only proof it accepts is a turn that ended normally: the run is `completed` or `waiting` (only its
checkpoint capture was left), with a start time.

A checkpoint rollback rewrites every later run that ended to `rolled_back` and keeps its start
time, whether it completed, failed, was interrupted or was cancelled. It leaves the run's attempt as
it was, so a `rolled_back` run counts only if its active attempt is `completed`.

Nothing a run still going, failed or interrupted has recorded proves it, because adapters record
the turn, and sometimes output, before the prompt goes out:

- **The provider turn itself.** Claude, OpenCode, Muse and the ACP agents (Antigravity, Fx, Grok,
  Kiro, the ACP registry) record the turn before they send the prompt. Claude also records a notice
  there when it cannot confirm bypass permissions.
- **Output in the turn.** Some adapters write the agent's own output into the turn before they send
  the prompt:
  - **Pi** holds the prompt while it compacts and replays an adopted native wake into the turn.
  - **OpenCode** replays a native wake's buffered output before it submits.
  - **Muse** joins a held native wake before `turn/start`.
  - **ACP agents that refuse a prompt while busy** get it again every second while their
    background work streams in.
  - **Claude** can receive frames between the turn's announcement and the prompt's enqueue.
- **An attempt marked `invoked`.** That mark is written as the prompt is handed to the transport,
  before the provider reads it.

Only a message that is still its run's input (`userMessageId`) counts. A steering restart gives
the run the user's message and settles the wake it replaced (Orchestrator's
`acceptDelegatedCompletionDelivery`), so the run is the user's turn and is never cancelled.

What the script does with each kind:

- **Delivered:** every other parent run settles as 0.1.9-perf.9 would at the wake's end:
  - its claimed results become `delivered`;
  - results of it still waiting go to a new wake, `message:merged-wake-settle:<wake run>:<parent run>`;
  - otherwise its delivery is cleared.

  The wake's own parent run is settled the same way if the run is `waiting`, because the older
  build can cancel a waiting run at startup and would then send its results again. Otherwise it is
  left for the older build to settle.

- **Not delivered:** every parent run the wake still holds, its own included, gets a new wake with
  the same results, plus any results of it still waiting. A wake that has not ended, and is still
  its run's input, is cancelled with its attempt, root node and provider turns.

| The wake's run when 0.1.9-perf.9 stops                               | Evidence the script reads                     | Delivered              | Own parent run         | Merged parent runs     | The run      | Outcome                                                      |
| ------------------------------------------------------------- | --------------------------------------------- | ---------------------- | ---------------------- | ---------------------- | ------------ | ------------------------------------------------------------ |
| Queued, held or not                                           | `queued`                                      | No                     | New wake               | New wake               | Cancelled    | Exactly once                                                 |
| Preparing or starting                                         | `preparing`, `starting`                       | No                     | New wake               | New wake               | Cancelled    | Exactly once                                                 |
| Running, before the provider had the prompt                   | `running`                                     | No                     | New wake               | New wake               | Cancelled    | Exactly once                                                 |
| Running, after the provider had the prompt                    | `running`                                     | No                     | New wake               | New wake               | Cancelled    | A duplicate, as any restart of 0.1.9-perf.9 or upstream gives       |
| Waiting                                                       | `waiting`                                     | Yes                    | Settled                | Settled                | Left waiting | Exactly once                                                 |
| Steered or restarted                                          | `userMessageId` is another message            | Settled by the restart | New wake if still held | New wake if still held | Left alone   | Exactly once                                                 |
| Completed                                                     | `completed`                                   | Yes                    | Older build            | Settled                | Ended        | Exactly once                                                 |
| Rolled back after its turn completed                          | `rolled_back`, active attempt `completed`     | Yes                    | Older build            | Settled                | Ended        | Exactly once                                                 |
| Rolled back after it failed, was interrupted or was cancelled | `rolled_back`, active attempt not `completed` | No                     | New wake               | New wake               | Ended        | Exactly once, or a duplicate if the provider had the prompt  |
| Failed before its start                                       | `failed`, no start time                       | No                     | New wake               | New wake               | Ended        | Exactly once                                                 |
| Failed in its preparation                                     | `failed`                                      | No                     | New wake               | New wake               | Ended        | Exactly once                                                 |
| Failed or interrupted before the provider had it              | `failed`, `interrupted`                       | No                     | New wake               | New wake               | Ended        | Exactly once                                                 |
| Failed or interrupted after the provider had it               | `failed`, `interrupted`                       | No                     | New wake               | New wake               | Ended        | A rare duplicate (below)                                     |
| Cancelled                                                     | `cancelled`                                   | No                     | New wake               | New wake               | Ended        | Exactly once; a duplicate if cut after the provider answered |

The steering restart has already settled every parent run of the wake it replaced, so a steered
wake normally holds none.

The new wakes have no message yet. At startup the older build offers each one, and its own dispatch
creates the message and queues the wake.

Then the script rewrites each merged wake's message to its own parent run alone (no `merged`, and
`taskIds` its own results), as the older build reads it.

It changes the projection tables only. The server reads its state from them and never rebuilds
them from the event log.

### Duplicates and losses

The script never counts a wake delivered on evidence an adapter can record before it sends the
prompt, so it loses no result. It can deliver one twice:

- **A wake running when 0.1.9-perf.9 stopped, whose provider had the prompt.** Its results go out again in
  a new wake. Every build does the same at any restart: the startup cancels a wake cut mid-turn,
  and a cancelled wake's results are delivered again.
- **A wake that failed or was interrupted after its provider had the prompt.** This happens only if
  0.1.9-perf.9 stopped before it settled the run, which it does as soon as the run ends.
- **A wake cancelled after its provider answered.** 0.1.9-perf.9 does the same.
- **A wake rolled back after it failed, was interrupted or was cancelled, whose provider had the
  prompt.** A rollback that came before 0.1.9-perf.9 settled the wake leaves only the run's attempt to say
  how the turn ended, and only a completed one proves the prompt arrived.

A completed turn counts as delivered because an adapter reports a turn completed only once its
provider answered the prompt. A turn completed without its prompt would lose the message without
any rollback.

The script is stricter on purpose than the running server, which treats a wake that failed after a
successful provider start call as delivered. The database records no evidence of that start call,
so do not align the two.

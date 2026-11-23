# Performance fork notes

How the performance fork's server differs from upstream where the internals guides
([performance regressions](16_performance-regressions.md), [server updates](07_server-updates.md))
and the orchestrator MCP tools describe upstream's behaviour.

## Full thread snapshots

A thread subscription without `acceptBoundedSnapshot` sends the whole thread as one frame, built
by one `JSON.stringify` on the event loop. Its JSON is measured in parts first, and past
`ThreadFullSnapshotMaxJsonLength` (default 32 MiB, about 0.6 s of stringify) the request fails
with `OrchestrationV2GetThreadProjectionError` naming `acceptBoundedSnapshot`. Shipped web,
desktop and mobile clients always send that flag. The default is a signed-off exception to the
fork's rule that new limits keep the previous behaviour: before it, such a request stalled the
event loop for seconds and failed past V8's string limit. Coverage:
`boundedSnapshotTransport.test.ts`.

## Turn item counts

Upstream's migration 85 creates the per-run turn item counts table,
`orchestration_v2_projection_turn_item_counts`, and its three triggers. The fork's migration 1
created them earlier under the same names, keyed by `run_id`; a start drops those in the same
transaction as it runs migration 85, and migration 1 now does nothing but stays recorded in
`t3_perf_fork_migrations` (see
[Leaving the performance fork's schema](../operations/perf-fork-schema-exit.md)). The triggers write
to that table: dropping the table while they remain makes every turn-item insert fail, so drop the
three triggers first.

## Delegated task and dispatch reads

Finalizing a delegated task reads from the parent only that child's tasks and the transfers it
sent, through the child and source indexes, and the node, card, runs and delivery message it
changes, by id. A message dispatch does not read the command projection: the store's
`getMessageDispatchRecords` returns only the rows dispatch decides on (the live runs, the runs whose
state it derives from the whole history, and their attempts, turns, nodes, provider threads and
messages), and a run's model switch walks the runs back from the latest. A dispatch change that
reads more of the projection must add those rows there, or send that path to the whole command
read in `readMessageDispatchProjection`. `MessageDispatchReads.test.ts` fails when a dispatch read
returns more rows on a longer history. A turn start reads its tasks' status and threads as columns,
not their payloads, and only the transfers its handoffs name, any into its run, and a run fork's own
fork transfer. The open-task filter selects row ids through the settled index, which covers it: the
delivery state column sits past each task's payload, so reading it from the table reads every
prompt and result.

## Restart recovery of queued runs and delegated seats

Queued runs never started. Recovery holds them and continues the run they wait behind. With
restart continuation on, it holds only the ones a person queued, for review: the queue does not
start while that run's continuation is pending in the outbox, so the continuation goes first, and
a hold on unattended work would outlive it.

A delegated seat (a `delegate_task` child) is an exception to the rule that an unmarked shell or
monitor does not wake a settled run: its parent reaches it through such a waiter, and the restart
killed it. Its settled run is continued with the note when its parent still waits on its task, or
when the run ended within the last 4 hours, the idle pin's bound: a seat idle longer would have
lost its session, and the waiter with it, anyway. The note leaves the task's outcome alone: the
task settles from the completed run, and the note's turn stays on the seat's thread. Seat wakes are
due at once, like other continuations; the admission gate bounds how many turn starts run together.

## Delegated task follow-ups and cancellation

A queued follow-up sent into the child's running turn (Send now, or automatically after 60 s)
keeps its task: the task follows that turn and settles once, with that turn's result.

When a task is cancelled, a message of the task sent into a turn whose steer has not landed goes
with the task and is not queued again.

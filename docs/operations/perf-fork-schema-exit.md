# Leaving the performance fork's schema

The performance fork runs its own migrations as a separate chain, recorded in
`t3_perf_fork_migrations` rather than upstream's `effect_sql_migrations`:

- 1 `ProjectionTurnItemCounts`: changes nothing now. In fork builds up to 0.1.7-perf.6 it created the
  `orchestration_v2_projection_turn_item_counts` table, keyed by `run_id`, and the three
  `orchestration_v2_turn_item_counts_*` triggers on `orchestration_v2_projection_turn_items` that
  keep it. Upstream's `085_TurnItemCounts` creates both under the same names, keyed by `run_key`. A
  later fork build drops the fork's in the transaction that runs upstream's 85, which then creates
  and fills its own.
- 2 `TurnItemMessageOrdinalIndex`: the `orchestration_v2_projection_turn_items_message_ordinal_idx`
  index.

The fork's first release candidate recorded them in `effect_sql_migrations` as 77 and 78, ids
upstream uses for its own migrations. Any later fork build moves those two rows to
`t3_perf_fork_migrations` when it starts. Until then, an upstream build that ships its own 77 or 78
refuses to start on the database. An older one is blocked by `t3 service`, whose preflight finds
migrations it does not know, and only warns when started directly.

Until upstream's migration 85 has run on a database with the fork's counts, they stand in its way:
an upstream build that ships 85 stops there with "table orchestration_v2_projection_turn_item_counts
already exists". Start a fork build that ships 85 first, or remove the fork's schema as below.

Once both are done (rows moved, 85 run), an upstream build starts on the database and runs its own
migrations. It ignores `t3_perf_fork_migrations` and leaves the fork's index in place, where writes
keep it current. An upstream migration that rebuilds `orchestration_v2_projection_turn_items` drops
the index with that table, or leaves it on the old table if it keeps that aside. On its next start
the fork runs migration 2 again (dropping an index left on the old table) and logs a warning that
names it. Remove the fork's schema when you go back to an upstream build for good:

1. Stop the server and anything else that opens the database, such as `t3` CLI commands.
2. Copy `<T3 home>/userdata/statev2.sqlite` with its `-wal` and `-shm` files.
3. Choose the script. This prints `1` while the database still has the fork's counts, from before
   upstream's migration 85:

   ```sh
   sqlite3 <T3 home>/userdata/statev2.sqlite \
     "SELECT 1 FROM pragma_table_info('orchestration_v2_projection_turn_item_counts') WHERE name = 'run_id'"
   ```

   With `1`, use [`scripts/perf-fork-schema-exit-before-085.sql`](../../scripts/perf-fork-schema-exit-before-085.sql);
   with no output, [`scripts/perf-fork-schema-exit.sql`](../../scripts/perf-fork-schema-exit.sql).
   Each script refuses the other's database, names the one to use and changes nothing.

4. Run it on the database, for example:

   ```sh
   sqlite3 -bail <T3 home>/userdata/statev2.sqlite < scripts/perf-fork-schema-exit.sql
   ```

   Without the `sqlite3` CLI, use the Node the server runs on:

   ```sh
   node -e 'new (require("node:sqlite").DatabaseSync)(process.argv[1]).exec(require("node:fs").readFileSync(process.argv[2], "utf8"))' \
     <T3 home>/userdata/statev2.sqlite scripts/perf-fork-schema-exit.sql
   ```

5. Start the upstream build.

## Rolling back to a fork build before upstream's migration 85

Fork builds up to 0.1.7-perf.6 read the counts by `run_id`. Once a later fork build has run
upstream's 85, they cannot read them: `t3 service` blocks them, since the database records upstream
migrations they do not know, and a direct start fails every read of the counts. Restore the backup
taken before that upgrade.

## Rolling back to the first release candidate

A database that a later fork build started needs nothing while that build's upstream migrations
end at 76: the first release candidate records 77 and 78 again (running both, which recounts the
turn items), and the next later start moves them back.

Once a fork build with upstream migrations past 76 has run on the database, such as upstream's
`077_RepairBooleanSettledOverride`, the first release candidate refuses it. Its service preflight
reports ready, then startup fails with "would skip required schema migrations:
77_ProjectionTurnItemCounts (recorded: RepairBooleanSettledOverride)". Restore the backup taken before
that upgrade. While upstream's 077 is the only upstream migration past 76 that the database records,
deleting its row also works, after steps 1 and 2 above:

```sh
sqlite3 -bail <T3 home>/userdata/statev2.sqlite \
  "DELETE FROM effect_sql_migrations WHERE migration_id = 77 AND name = 'RepairBooleanSettledOverride'"
```

The release candidate then runs its own 77 and 78 again. The next later fork start moves them back and
runs upstream's 077 again, a data repair that is safe to repeat. With any other upstream migration
past 76 recorded, only the backup is safe.

## What the scripts do

Each script runs in one transaction and keeps every other row. First it checks the counts table and
stops, with nothing changed, on the other script's database. Both drop the index and
`t3_perf_fork_migrations`, and delete only the first release candidate's records of 77 and 78.
`perf-fork-schema-exit-before-085.sql` also drops the fork's counts table and its triggers, the
triggers first, since a turn-item write fails while they outlive the table they write.
`perf-fork-schema-exit.sql` leaves upstream's counts and triggers. Running either twice, or on a
database the fork never migrated, changes nothing. Starting the fork again re-applies migration 2.
After `perf-fork-schema-exit-before-085.sql`, the first build to start that ships upstream's 85,
upstream's or the fork's, creates the counts again and fills them from the turn items.

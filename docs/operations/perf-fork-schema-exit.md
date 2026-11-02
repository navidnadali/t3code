# Leaving the performance fork's schema

The performance fork runs its own migrations as a separate chain, recorded in
`t3_perf_fork_migrations` rather than upstream's `effect_sql_migrations`:

- 1 `ProjectionTurnItemCounts`: the `orchestration_v2_projection_turn_item_counts` table and the
  three `orchestration_v2_turn_item_counts_*` triggers on `orchestration_v2_projection_turn_items`
  that keep it.
- 2 `TurnItemMessageOrdinalIndex`: the `orchestration_v2_projection_turn_items_message_ordinal_idx`
  index.

The fork's first release candidate recorded them in `effect_sql_migrations` as 77 and 78, ids
upstream uses for its own migrations. Any later fork build moves those two rows to
`t3_perf_fork_migrations` when it starts. Until then, an upstream build that ships its own 77 or 78
refuses to start on the database, and an older one only warns.

Once they are moved, an upstream build starts on the database and runs its own migrations. It
ignores `t3_perf_fork_migrations` and leaves the fork's objects in place, where writes keep them
current. Remove them when you go back to an upstream build for good, or before you start the fork
again after an upstream migration rebuilt `orchestration_v2_projection_turn_items`: a rebuild drops
the triggers and the index, and the fork does not re-apply migrations it has recorded.

1. Stop the server and anything else that opens the database, such as `t3` CLI commands.
2. Copy `<T3 home>/userdata/statev2.sqlite` with its `-wal` and `-shm` files.
3. Run [`scripts/perf-fork-schema-exit.sql`](../../scripts/perf-fork-schema-exit.sql) on it:

   ```sh
   sqlite3 -bail <T3 home>/userdata/statev2.sqlite < scripts/perf-fork-schema-exit.sql
   ```

   Without the `sqlite3` CLI, use the Node the server runs on:

   ```sh
   node -e 'new (require("node:sqlite").DatabaseSync)(process.argv[1]).exec(require("node:fs").readFileSync(process.argv[2], "utf8"))' \
     <T3 home>/userdata/statev2.sqlite scripts/perf-fork-schema-exit.sql
   ```

4. Start the upstream build.

The script runs in one transaction and keeps every other row. It drops the triggers before the table
they write, since a turn-item write fails while they outlive it. It drops `t3_perf_fork_migrations`
and deletes only the first release candidate's records of 77 and 78. Running it twice, or on a
database the fork never migrated, changes nothing. Starting the fork again re-applies both
migrations and recounts the turn items.

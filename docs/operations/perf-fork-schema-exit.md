# Leaving the performance fork's schema

The performance fork adds two migrations upstream does not have:

- 77 `ProjectionTurnItemCounts`: the `orchestration_v2_projection_turn_item_counts` table and the
  three `orchestration_v2_turn_item_counts_*` triggers on `orchestration_v2_projection_turn_items`
  that keep it.
- 78 `TurnItemMessageOrdinalIndex`: the `orchestration_v2_projection_turn_items_message_ordinal_idx`
  index.

They take ids upstream will use for its own migrations. An upstream build that ships its own 77 or
78 refuses to start on a database the fork has migrated, and an older one only warns. Remove them
before you go back to an upstream build:

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
they write, since a turn-item write fails while they outlive it. It deletes only the fork's own
records of 77 and 78. Running it twice, or on a database the fork never migrated, changes nothing.
Starting the fork again re-applies both migrations and recounts the turn items.

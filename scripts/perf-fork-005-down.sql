-- Takes fork migration 5 (RetireThreadItemTotals) back out, so a build whose fork chain ends at 4
-- (0.1.9-perf.13 to 0.1.9-perf.15 and their fleet builds) passes its fork-ledger check on this
-- database. That build's next start finds fork migration 3 recorded with its totals table and
-- triggers missing, runs it again and rebuilds the totals from upstream's per-run counts. Upstream's
-- own totals (migration 102) stay. Stop the server first. A rollback to such a build also needs
-- upstream's migrations after its head taken out, or its service preflight refuses the database.
BEGIN IMMEDIATE;
DELETE FROM t3_perf_fork_migrations WHERE migration_id = 5 AND name = 'RetireThreadItemTotals';
COMMIT;

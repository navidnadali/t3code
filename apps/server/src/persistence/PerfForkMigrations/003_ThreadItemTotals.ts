import * as Effect from "effect/Effect";

/** The performance fork's per-thread item total, which fork migration 3 kept from 0.1.9-perf.13. */
export const THREAD_ITEM_TOTALS = "t3_perf_thread_item_totals";
export const THREAD_ITEM_TOTALS_TRIGGERS = [
  ["t3_perf_thread_item_totals_item_insert", "orchestration_v2_projection_turn_items"],
  ["t3_perf_thread_item_totals_item_update", "orchestration_v2_projection_turn_items"],
  ["t3_perf_thread_item_totals_item_delete", "orchestration_v2_projection_turn_items"],
  ["t3_perf_thread_item_totals_run_insert", "orchestration_v2_projection_runs"],
  ["t3_perf_thread_item_totals_run_delete", "orchestration_v2_projection_runs"],
  ["t3_perf_thread_item_totals_run_update", "orchestration_v2_projection_runs"],
] as const;

/**
 * Each thread's item total for the shell's count. Upstream's migration 102 (ThreadItemCounts) keeps
 * the same total now, so this migration creates nothing; where it ran, fork migration 5 drops its
 * table and triggers. It stays recorded.
 */
export default Effect.void;

import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import { THREAD_ITEM_TOTALS, THREAD_ITEM_TOTALS_TRIGGERS } from "./003_ThreadItemTotals.ts";

/**
 * Drops fork migration 3's per-thread item totals and their six triggers, triggers first: upstream's
 * migration 102 keeps the same total, which the shell reads, so every item write would keep both.
 * Upstream's chain runs before the fork's, so 102 has built its table by then.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [trigger] of THREAD_ITEM_TOTALS_TRIGGERS) {
    yield* sql.unsafe(`DROP TRIGGER IF EXISTS main."${trigger}"`);
  }
  yield* sql.unsafe(`DROP TABLE IF EXISTS main."${THREAD_ITEM_TOTALS}"`);
});

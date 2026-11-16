import { assert, describe, it } from "@effect/vitest";
import {
  DATABASE_MIGRATIONS_FINISHED_MESSAGE,
  DATABASE_MIGRATIONS_STARTED_MESSAGE,
  isRunningDatabaseMigrations,
} from "@t3tools/shared/databaseMigrationLog";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Logger from "effect/Logger";
import * as References from "effect/References";
import * as Tracer from "effect/Tracer";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import {
  hasPendingMigrations,
  migrationManifest,
  runMigrationChains,
  runMigrations,
} from "./Migrations.ts";
import TurnItemCounts from "./Migrations/085_TurnItemCounts.ts";
import ProviderTurnAttemptIndex from "./Migrations/093_ProviderTurnAttemptIndex.ts";
import TurnItemMessageOrdinalIndex from "./Migrations/094_TurnItemMessageOrdinalIndex.ts";
import TurnItemInlineWorkOutput from "./Migrations/095_TurnItemInlineWorkOutput.ts";
import {
  replacePerfForkTurnItemCounts,
  runPerfForkMigrations,
  UPSTREAM_TURN_ITEM_COUNTS_MIGRATION_ID,
} from "./PerfForkMigrations.ts";
import {
  normalizeSql,
  ORDINAL_INDEX,
  PERF_FORK_ORDINAL_INDEX,
} from "./testkit/PerfForkOrdinalIndex.ts";
import { PERF_FORK_RUN_ID_TURN_ITEM_COUNTS } from "./testkit/PerfForkRunIdTurnItemCounts.ts";

const FORK_TABLE = "t3_perf_fork_migrations";
const FORK_CHAIN: Array<[number, string]> = [
  [1, "ProjectionTurnItemCounts"],
  [2, "TurnItemMessageOrdinalIndex"],
];
// Release candidate 1 recorded the fork's chain in upstream's ledger, under upstream's next ids.
const RC1_IDS: Record<number, number> = { 1: 77, 2: 78 };
const RC1_RAN_AT = "2026-11-01 00:00:00";
const upstreamChain = migrationManifest;
const UPSTREAM_HEAD = Math.max(...upstreamChain.map(([id]) => id));
// Upstream's last migration when release candidate 1 recorded the fork's at 77 and 78; any later
// upstream migration runs on a release candidate 1 database.
const RC1_UPSTREAM_HEAD = 76;
const upstreamSinceRc1 = upstreamChain.filter(([id]) => id > RC1_UPSTREAM_HEAD);
// Upstream's last migration in the last fork build before upstream's migration 85.
const BEFORE_085_UPSTREAM_HEAD = 80;
// Upstream's last migration in 0.1.9-perf.7, the last fork build before upstream's migration 94.
const BEFORE_094_UPSTREAM_HEAD = 92;
const FORK_RAN_AT = "2026-11-15 13:25:00";
const UPSTREAM_RAN_AT = "2026-11-15 13:20:00";

const ledger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
  `;
  return rows.map((row) => [row.migration_id, row.name] as const);
});

const ledgerRuns = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    readonly migration_id: number;
    readonly name: string;
    readonly created_at: string;
  }>`SELECT migration_id, name, created_at FROM effect_sql_migrations ORDER BY migration_id`;
});

const forkLedger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const table = yield* sql`
    SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ${FORK_TABLE}
  `;
  if (table.length === 0) return [];
  return yield* sql<{
    readonly migration_id: number;
    readonly name: string;
    readonly created_at: string;
  }>`SELECT migration_id, name, created_at FROM ${sql(FORK_TABLE)} ORDER BY migration_id`;
});

const schema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name`;
});

const counts = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`
    SELECT thread_id, run_key, item_count FROM orchestration_v2_projection_turn_item_counts
    ORDER BY thread_id, run_key
  `;
});

const forkCounts = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`
    SELECT thread_id, run_id, item_count FROM orchestration_v2_projection_turn_item_counts
    ORDER BY thread_id, run_id
  `;
});

const countedFromItems = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`
    SELECT thread_id, COALESCE(run_id, '') AS run_key, COUNT(*) AS item_count
    FROM orchestration_v2_projection_turn_items
    GROUP BY thread_id, COALESCE(run_id, '') ORDER BY thread_id, run_key
  `;
});

// A count row no turn item backs: it stays only while nothing recounts the table.
const insertStrayCount = (key: "run_id" | "run_key") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO orchestration_v2_projection_turn_item_counts (thread_id, ${sql(key)}, item_count)
      VALUES ('thread-stray', '', 7)
    `;
  });

const insertTurnItems = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [id, threadId, runId] of [
    ["a1", "thread-a", "run-1"],
    ["a2", "thread-a", "run-1"],
    ["a3", "thread-a", null],
    ["b1", "thread-b", "run-2"],
  ] as const) {
    yield* sql`
      INSERT INTO orchestration_v2_projection_turn_items (
        turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
      ) VALUES (
        ${id}, ${threadId}, ${runId}, 1, 'assistant_message', 'completed',
        '2026-11-01T00:00:00.000Z', '{"messageId":"m1"}'
      )
    `;
  }
});

// A database as release candidate 1 left it: upstream's chain through 76, the fork's objects (its
// counts keyed by run_id, its index) and the given fork migrations recorded in upstream's ledger.
const atRc1 = (forkIds: ReadonlyArray<number>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: RC1_UPSTREAM_HEAD });
    for (const statement of PERF_FORK_RUN_ID_TURN_ITEM_COUNTS) yield* sql.unsafe(statement);
    yield* sql.unsafe(PERF_FORK_ORDINAL_INDEX);
    for (const [id, name] of FORK_CHAIN) {
      if (!forkIds.includes(id)) continue;
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name, created_at)
        VALUES (${RC1_IDS[id]!}, ${name}, ${RC1_RAN_AT})
      `;
    }
  });

// A database as a fork build with upstream's chain through `upstreamHead` left it: the fork chain
// recorded at FORK_RAN_AT, its message-ordinal index and, before upstream's 85, its counts keyed
// by run_id.
const atForkBuild = (upstreamHead: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: upstreamHead });
    if (upstreamHead < UPSTREAM_TURN_ITEM_COUNTS_MIGRATION_ID) {
      for (const statement of PERF_FORK_RUN_ID_TURN_ITEM_COUNTS) yield* sql.unsafe(statement);
    }
    yield* sql.unsafe(PERF_FORK_ORDINAL_INDEX);
    yield* runPerfForkMigrations();
    yield* sql`UPDATE ${sql(FORK_TABLE)} SET created_at = ${FORK_RAN_AT}`;
  });

const beforeUpstream085 = atForkBuild(BEFORE_085_UPSTREAM_HEAD);

const insertMoreTurnItems = (prefix: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    for (const [index, runId] of ["run-1", "run-9", null].entries()) {
      yield* sql`
        INSERT INTO orchestration_v2_projection_turn_items (
          turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
        ) VALUES (
          ${`${prefix}-${index}`}, 'thread-a', ${runId}, ${index + 10}, 'user_message', 'completed',
          '2026-11-02T00:00:00.000Z', '{"messageId":"m2"}'
        )
      `;
    }
  });

const usesTheOrdinalIndex = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const plan = yield* sql<{ readonly detail: string }>`
    EXPLAIN QUERY PLAN
    SELECT json_extract(payload_json, '$.messageId') AS message_id, MIN(ordinal) AS ordinal
    FROM orchestration_v2_projection_turn_items
    WHERE thread_id = 'thread-a' AND type IN ('user_message', 'assistant_message')
      AND json_valid(payload_json)
    GROUP BY json_extract(payload_json, '$.messageId')
  `;
  assert.include(
    plan[0]?.detail,
    "USING INDEX orchestration_v2_projection_turn_items_message_ordinal_idx",
  );
});

// Every index over `$.messageId`, on whichever table, its statement normalised.
const messageOrdinalIndexes = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{
    readonly name: string;
    readonly tbl_name: string;
    readonly sql: string;
  }>`
    SELECT name, tbl_name, sql FROM sqlite_master
    WHERE type = 'index' AND instr(sql, '$.messageId') > 0
    ORDER BY name
  `;
  return rows.map((row) => ({ ...row, sql: normalizeSql(row.sql) }));
});

// The index as upstream's chain alone creates it, through its migration 94.
const upstream094Index = Effect.gen(function* () {
  yield* runMigrations({ toMigrationInclusive: 94 });
  return yield* messageOrdinalIndexes;
}).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));

// Turn items the upgrade migrates: one whose payload is not JSON, and a tool item upstream's
// migration 95 flags as inline work output.
const insertUpgradeItems = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* insertTurnItems;
  for (const [id, type, payloadJson] of [
    ["c1", "user_message", "{not json"],
    ["c2", "dynamic_tool", '{"toolName":"htmlRender"}'],
  ] as const) {
    yield* sql`
      INSERT INTO orchestration_v2_projection_turn_items (
        turn_item_id, thread_id, run_id, ordinal, type, status, updated_at, payload_json
      ) VALUES (
        ${id}, 'thread-c', 'run-4', 1, ${type}, 'completed', '2026-11-01T00:00:00.000Z',
        ${payloadJson}
      )
    `;
  }
});

// A real case: upstream's 0.1.10-nightly started on 0.1.9-perf.7's database. Its migrator ran 93 and
// 94, a transaction each, and was killed during 95, whose transaction rolled back.
const upstreamNightlyKilledIn095 = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const upstreamRun = <E>(migration: string, body: Effect.Effect<void, E, SqlClient.SqlClient>) =>
    Migrator.make({})({ loader: Migrator.fromRecord({ [migration]: body }) });
  yield* upstreamRun("93_ProviderTurnAttemptIndex", ProviderTurnAttemptIndex);
  yield* upstreamRun("94_TurnItemMessageOrdinalIndex", TurnItemMessageOrdinalIndex);
  const killed = yield* Effect.exit(
    upstreamRun(
      "95_TurnItemInlineWorkOutput",
      TurnItemInlineWorkOutput.pipe(Effect.andThen(Effect.fail("killed"))),
    ),
  );
  assert.isTrue(Exit.isFailure(killed));
  assert.deepStrictEqual((yield* ledger).slice(-2), [
    [93, "ProviderTurnAttemptIndex"],
    [94, "TurnItemMessageOrdinalIndex"],
  ]);
  assert.deepStrictEqual(
    yield* sql`
      SELECT name FROM pragma_table_info('orchestration_v2_projection_turn_items')
      WHERE name = 'inline_work_output'
    `,
    [],
  );
  assert.lengthOf(yield* messageOrdinalIndexes, 1);
});

const captureWarnings = (warnings: Array<unknown>) =>
  Logger.layer([
    Logger.make(({ logLevel, message, fiber }) => {
      if (logLevel === "Warn") {
        warnings.push([message, fiber.getRef(References.CurrentLogAnnotations)]);
      }
    }),
  ]);

const freshSchema = Effect.gen(function* () {
  yield* runMigrations();
  return yield* schema;
}).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));

describe("performance fork migrations", () => {
  it.effect(
    "record in their own table, so upstream's ledger ends at upstream's last migration",
    () =>
      Effect.gen(function* () {
        yield* runMigrations({ toMigrationInclusive: UPSTREAM_HEAD - 1 });
        assert.deepStrictEqual(yield* forkLedger, []);

        yield* runMigrations();
        assert.deepStrictEqual(yield* ledger, upstreamChain);
        assert.deepStrictEqual(
          (yield* forkLedger).map((row) => [row.migration_id, row.name]),
          FORK_CHAIN,
        );
        assert.deepStrictEqual(yield* runMigrations(), []);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect.each([
    { recorded: [1, 2], moved: [1, 2], dropIndex: false },
    // Builds with the turn item counts but before the message-ordinal index.
    { recorded: [1], moved: [1], dropIndex: true },
    // Row 77 deleted by hand: recording 2 alone would make the migrator skip 1.
    { recorded: [2], moved: [], dropIndex: false },
  ])(
    "moves release candidate 1's rows $recorded to the fork's table without running them again",
    ({ recorded, moved, dropIndex }) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const expectedSchema = yield* freshSchema;
        yield* atRc1(recorded);
        if (dropIndex) yield* sql.unsafe(`DROP INDEX ${ORDINAL_INDEX}`);
        yield* insertTurnItems;
        yield* insertStrayCount("run_id");

        assert.deepStrictEqual(yield* runMigrations(), upstreamSinceRc1);
        assert.deepStrictEqual(yield* ledger, upstreamChain);
        const forkRows = yield* forkLedger;
        assert.deepStrictEqual(
          forkRows.map((row) => [row.migration_id, row.name]),
          FORK_CHAIN,
        );
        assert.deepStrictEqual(
          forkRows.filter((row) => row.created_at === RC1_RAN_AT).map((row) => row.migration_id),
          moved,
        );
        // Upstream's migration 85 replaced the fork's counts with its own, counted from the items.
        assert.deepStrictEqual(yield* counts, yield* countedFromItems);
        assert.deepStrictEqual(yield* schema, expectedSchema);
        assert.deepStrictEqual(yield* runMigrations(), []);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("keeps the fork's own rows when release candidate 1 ran again on the database", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* atRc1([1, 2]);
      yield* sql`
        CREATE TABLE ${sql(FORK_TABLE)} (
          migration_id integer PRIMARY KEY NOT NULL,
          created_at datetime NOT NULL DEFAULT current_timestamp,
          name VARCHAR(255) NOT NULL
        )
      `;
      for (const [id, name] of FORK_CHAIN) {
        yield* sql`
          INSERT INTO ${sql(FORK_TABLE)} (migration_id, name, created_at)
          VALUES (${id}, ${name}, '2026-11-02 00:00:00')
        `;
      }

      assert.deepStrictEqual(yield* runMigrations(), upstreamSinceRc1);
      assert.deepStrictEqual(yield* ledger, upstreamChain);
      assert.deepStrictEqual(
        yield* forkLedger,
        FORK_CHAIN.map(([id, name]) => ({
          migration_id: id,
          name,
          created_at: "2026-11-02 00:00:00",
        })),
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("let upstream's next migration run on a fork database", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const forkRan = yield* forkLedger;
      const next = UPSTREAM_HEAD + 1;

      // Upstream's next build runs its chain, one id past this one's, on upstream's ledger.
      const ran = yield* Migrator.make({})({
        loader: Migrator.fromRecord({
          [`${next}_NextUpstreamMigration`]: Effect.asVoid(
            sql`CREATE TABLE next_upstream_migration (id INTEGER)`,
          ),
        }),
      });
      assert.deepStrictEqual(ran, [[next, "NextUpstreamMigration"]]);
      assert.lengthOf(
        yield* sql`SELECT 1 FROM sqlite_master WHERE name = 'next_upstream_migration'`,
        1,
      );

      // This build still starts on it, keeps upstream's row and has nothing of its own to run.
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* ledger, [...upstreamChain, [next, "NextUpstreamMigration"]]);
      assert.deepStrictEqual(yield* forkLedger, forkRan);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect.each([
    { from: "0.1.9-perf.7's (upstream 92)", upstreamHead: BEFORE_094_UPSTREAM_HEAD },
    { from: "0.1.7-perf.6's (upstream 80)", upstreamHead: BEFORE_085_UPSTREAM_HEAD },
    // The fork's chain recorded before upstream's 77, as on an older database.
    { from: "an older database's (upstream 76)", upstreamHead: 76 },
    {
      from: "a nightly-migrated database's (nightly ran 93-94)",
      upstreamHead: BEFORE_094_UPSTREAM_HEAD,
      upstreamBuild: upstreamNightlyKilledIn095,
    },
  ])(
    "upgrade a database in $from state: upstream's migration 94 keeps the fork's index",
    ({ upstreamHead, upstreamBuild = Effect.void }) =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const expectedSchema = yield* freshSchema;
        const expectedIndex = yield* upstream094Index;
        yield* atForkBuild(upstreamHead);
        yield* insertUpgradeItems;
        yield* upstreamBuild;
        // Earlier starts' time, so a start that rewrites a recorded row shows.
        yield* sql`UPDATE effect_sql_migrations SET created_at = ${UPSTREAM_RAN_AT}`;
        const upstreamRan = yield* ledgerRuns;
        const startHead = upstreamRan.at(-1)!.migration_id;
        const forkRan = yield* forkLedger;
        assert.deepStrictEqual(
          forkRan.map((row) => [row.migration_id, row.name, row.created_at]),
          FORK_CHAIN.map(([id, name]) => [id, name, FORK_RAN_AT]),
        );

        const warnings: Array<unknown> = [];
        assert.deepStrictEqual(
          yield* runMigrationChains().pipe(Effect.provide(captureWarnings(warnings))),
          { upstream: upstreamChain.filter(([id]) => id > startHead), perfFork: [] },
        );
        assert.deepStrictEqual(warnings, []);
        assert.deepStrictEqual(yield* ledger, upstreamChain);
        const upgraded = yield* ledgerRuns;
        assert.deepStrictEqual(upgraded.slice(0, upstreamRan.length), upstreamRan);
        assert.deepStrictEqual(yield* forkLedger, forkRan);
        assert.lengthOf(expectedIndex, 1);
        assert.deepStrictEqual(yield* messageOrdinalIndexes, expectedIndex);
        assert.deepStrictEqual(yield* schema, expectedSchema);
        assert.lengthOf(
          yield* sql`
            SELECT 1 FROM sqlite_master
            WHERE type = 'index' AND name = 'orchestration_v2_projection_provider_turns_attempt_idx'
          `,
          1,
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT turn_item_id FROM orchestration_v2_projection_turn_items
            WHERE inline_work_output = 1
          `,
          [{ turn_item_id: "c2" }],
        );
        assert.deepStrictEqual(yield* counts, yield* countedFromItems);
        assert.deepStrictEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
        yield* usesTheOrdinalIndex;
        yield* insertMoreTurnItems("after");
        assert.deepStrictEqual(yield* counts, yield* countedFromItems);

        assert.isFalse(yield* hasPendingMigrations());
        assert.deepStrictEqual(yield* runMigrationChains(), { upstream: [], perfFork: [] });
        assert.deepStrictEqual(yield* ledgerRuns, upgraded);
        assert.deepStrictEqual(yield* forkLedger, forkRan);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("leave the message-ordinal index to upstream's migration 94 when it is missing", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      const forkRan = yield* forkLedger;
      yield* insertTurnItems;
      yield* dropOrdinalIndex;
      const schemaWithout = yield* schema;

      assert.isFalse(yield* hasPendingMigrations());
      const warnings: Array<unknown> = [];
      assert.deepStrictEqual(
        yield* runMigrationChains().pipe(Effect.provide(captureWarnings(warnings))),
        { upstream: [], perfFork: [] },
      );
      assert.deepStrictEqual(yield* messageOrdinalIndexes, []);
      assert.deepStrictEqual(warnings, []);
      assert.deepStrictEqual(yield* schema, schemaWithout);
      assert.deepStrictEqual(yield* forkLedger, forkRan);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("create no index of their own: upstream's migration 94 creates it, once", () =>
    Effect.gen(function* () {
      const expectedIndex = yield* upstream094Index;
      yield* runMigrations({ toMigrationInclusive: 93 });

      assert.deepStrictEqual(yield* runPerfForkMigrations(), FORK_CHAIN);
      assert.deepStrictEqual(yield* messageOrdinalIndexes, []);
      assert.deepStrictEqual(yield* runMigrationChains(), {
        upstream: upstreamChain.filter(([id]) => id > 93),
        perfFork: [],
      });
      assert.deepStrictEqual(yield* messageOrdinalIndexes, expectedIndex);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("warns about fork migrations that a newer fork build recorded", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`
        INSERT INTO ${sql(FORK_TABLE)} (migration_id, name)
        VALUES (${FORK_CHAIN.length + 1}, 'FromNewerFork')
      `;
      const warnings: Array<unknown> = [];

      assert.deepStrictEqual(
        yield* runMigrations().pipe(Effect.provide(captureWarnings(warnings))),
        [],
      );
      assert.include(
        JSON.stringify(warnings),
        `${FORK_TABLE}:${FORK_CHAIN.length + 1}_FromNewerFork`,
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

const dropOrdinalIndex = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`DROP INDEX ${ORDINAL_INDEX}`);
});

// Upstream's chain current, and the fork's chain left with work of each kind it does.
const upstreamCurrentWithForkWork = [
  { work: "no fork ledger yet", setup: runMigrations({ toPerfForkMigrationInclusive: 0 }) },
  { work: "a fork migration to run", setup: runMigrations({ toPerfForkMigrationInclusive: 1 }) },
];

describe("pending migration work", () => {
  it.effect.each(upstreamCurrentWithForkWork)(
    "counts the fork's chain when upstream's is current: $work",
    ({ setup }) =>
      Effect.gen(function* () {
        yield* setup;
        assert.isTrue(yield* hasPendingMigrations());
        yield* runMigrations();
        assert.isFalse(yield* hasPendingMigrations());
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("includes a release candidate 1 row the fork's chain would move", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      // Hand-edited: upstream's own row at 78 under the name release candidate 1 recorded there.
      yield* sql`
        UPDATE effect_sql_migrations SET name = ${FORK_CHAIN[1]![1]} WHERE migration_id = ${RC1_IDS[2]!}
      `;
      assert.isTrue(yield* hasPendingMigrations());
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

// The start's output as the desktop reads it, while the fork's chain runs and once it is done.
const migrationLines = <A, E>(start: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  Effect.gen(function* () {
    const lines: Array<string> = [];
    const whileForkChainRuns: Array<boolean> = [];
    const tracer = Tracer.make({
      span(options) {
        if (options.name === "runPerfForkMigrations") {
          whileForkChainRuns.push(isRunningDatabaseMigrations(lines.join("\n")));
        }
        return new Tracer.NativeSpan(options);
      },
    });
    yield* start.pipe(
      Effect.withTracer(tracer),
      Effect.provide(
        Logger.layer([
          Logger.make(({ message }) => {
            lines.push(Array.isArray(message) ? message.join(" ") : String(message));
          }),
        ]),
      ),
    );
    const count = (marker: string) => lines.filter((line) => line.includes(marker)).length;
    return {
      whileForkChainRuns,
      afterwards: isRunningDatabaseMigrations(lines.join("\n")),
      started: count(DATABASE_MIGRATIONS_STARTED_MESSAGE),
      finished: count(DATABASE_MIGRATIONS_FINISHED_MESSAGE),
    };
  });

describe("migration lifecycle lines", () => {
  it.effect.each([
    {
      work: "upstream's last migration and the whole fork chain",
      setup: runMigrations({ toMigrationInclusive: UPSTREAM_HEAD - 1 }),
    },
    { work: "upstream's migration 85 over the fork's counts", setup: beforeUpstream085 },
    ...upstreamCurrentWithForkWork,
  ])("keep the desktop waiting until the fork's chain is done: $work", ({ setup }) =>
    Effect.gen(function* () {
      yield* setup;

      assert.deepStrictEqual(yield* migrationLines(runMigrations()), {
        whileForkChainRuns: [true],
        afterwards: false,
        started: 1,
        finished: 1,
      });
      assert.deepStrictEqual(yield* migrationLines(runMigrations()), {
        whileForkChainRuns: [false],
        afterwards: false,
        started: 0,
        finished: 0,
      });
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

describe("replacing the fork's turn-item counts with upstream's migration 85", () => {
  const countMigration = <E>(migration: Effect.Effect<void, E, SqlClient.SqlClient>) =>
    Migrator.make({})({
      loader: Migrator.fromRecord({
        [`${UPSTREAM_TURN_ITEM_COUNTS_MIGRATION_ID}_TurnItemCounts`]: migration,
      }),
    });

  it.effect("rolls the drop back when the migration it wraps fails", () =>
    Effect.gen(function* () {
      yield* beforeUpstream085;
      yield* insertTurnItems;
      yield* insertStrayCount("run_id");
      const schemaBefore = yield* schema;
      const countsBefore = yield* forkCounts;

      const exit = yield* Effect.exit(
        replacePerfForkTurnItemCounts(
          countMigration(
            TurnItemCounts.pipe(Effect.andThen(Effect.fail("after creating the counts"))),
          ),
        ),
      );
      assert.isTrue(Exit.isFailure(exit));
      assert.include(String(exit), `${UPSTREAM_TURN_ITEM_COUNTS_MIGRATION_ID}_TurnItemCounts`);
      // The fork's table, its triggers and its counts, stray row included, as they were.
      assert.deepStrictEqual(yield* schema, schemaBefore);
      assert.deepStrictEqual(yield* forkCounts, countsBefore);
      assert.deepStrictEqual(
        (yield* ledger).at(-1),
        upstreamChain.find(([id]) => id === BEFORE_085_UPSTREAM_HEAD),
      );

      assert.deepStrictEqual(
        yield* runMigrations(),
        upstreamChain.filter(([id]) => id > BEFORE_085_UPSTREAM_HEAD),
      );
      assert.deepStrictEqual(yield* counts, yield* countedFromItems);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("leaves upstream's counts when another start already ran migration 85", () =>
    Effect.gen(function* () {
      yield* runMigrations();
      yield* insertTurnItems;
      yield* insertStrayCount("run_key");
      const schemaBefore = yield* schema;
      const countsBefore = yield* counts;

      assert.deepStrictEqual(
        yield* replacePerfForkTurnItemCounts(countMigration(TurnItemCounts)),
        [],
      );
      assert.deepStrictEqual(yield* schema, schemaBefore);
      assert.deepStrictEqual(yield* counts, countsBefore);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - opens SQLite files and prints JSON directly.
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Cause from "effect/Cause";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as References from "effect/References";
import { Argument, Command, Flag } from "effect/cli";

import { checkFixture, isClean, runSanitise } from "./sanitise.ts";
import { failed } from "./server.ts";
import { measureShape } from "./shape.ts";
import { runSynth } from "./synth.ts";

const percent = (value: number | null) =>
  value === null ? "n/a" : `${value >= 0 ? "+" : ""}${(value * 100).toFixed(1)}%`;
const mib = (bytes: number | null) =>
  bytes === null ? "n/a" : `${(bytes / 1048576).toFixed(1)} MiB`;

const reportFlag = Flag.String("report").pipe(
  Flag.optional,
  Flag.withDescription(
    "Also write the full result as JSON to this new file (checked like the destination, created exclusively).",
  ),
);

const keepContentFlag = Flag.Boolean("keep-content").pipe(
  Flag.withDefault(false),
  Flag.withDescription(
    "Keep text and only redact credential patterns. Fails open by nature: for a fixture that never leaves this machine.",
  ),
);

const sanitiseCommand = Command.make(
  "sanitise",
  {
    source: Argument.String("source").pipe(
      Argument.withDescription("Database to copy (read-only, VACUUM INTO). Never modified."),
    ),
    destination: Argument.String("destination").pipe(
      Argument.withDescription("New file outside ~/.t3 and $T3CODE_HOME; must not exist."),
    ),
    report: reportFlag,
    keepContent: keepContentFlag,
  },
  ({ source, destination, report, keepContent }) =>
    Effect.gen(function* () {
      const result = yield* runSanitise({
        source,
        destination,
        report: Option.getOrUndefined(report),
        keepContent,
      });
      yield* Console.log(
        [
          `Sanitised fixture: ${result.destination} (${mib(result.sizeBytes)})`,
          `  start-up work on ${result.threadsWithStartupWork} threads, done in place: ${JSON.stringify(result.restartRecovery)}; deliveries settled ${JSON.stringify(result.deliveriesSettled)}; v1 imports ${JSON.stringify(result.legacyImport)}`,
          `  cleared ${JSON.stringify(result.clearedRows)}`,
          `  auto-pull off on ${result.autoPullDisabled} projects, auto-settle off on ${result.autoSettleDisabled} threads, pull request links and branch cleared on ${result.pullRequestInputsCleared}, timers cleared on ${result.timersCleared}`,
          `  event store compacted: ${result.compaction.deletedEventCount} events, ${result.compaction.deletedReceiptCount} receipts`,
          result.mode === "blank"
            ? `  content blanked: ${result.content.changed} values (${result.content.flaggedKept} allowlisted but flagged), ${result.content.keys} keys`
            : `  --keep-content: ${result.content.changed} values redacted, ${result.content.keys} keys; text kept`,
          `  self-check: every value allowlisted or filler, 0 pattern hits, nothing runnable`,
        ].join("\n"),
      );
    }),
).pipe(
  Command.withDescription(
    "Copy a T3 database with VACUUM INTO, run the server's migrations, drop thread families with live work, a pull request watch or a pending v1 import, clear credentials and queued work (scheduled tasks, outbox, launch workflows, peer links, sessions, pairing, provider runtime, pull request state), turn off auto-pull and auto-settle, compact the event store, blank every string outside the allowlist (see fixture.md), compact, and check both layers.",
  ),
);

const synthCommand = Command.make(
  "synth",
  {
    like: Flag.String("like").pipe(
      Flag.withDescription("Shape file (see tools/server-bench/fixture.md)."),
    ),
    scale: Flag.String("scale").pipe(
      Flag.withDefault("1"),
      Flag.withDescription(
        "Multiplies projects, threads and every row count; thread lengths keep their distribution.",
      ),
    ),
    seed: Flag.Int("seed").pipe(
      Flag.withDefault(1),
      Flag.withDescription("Same seed, same database."),
    ),
    provider: Flag.String("provider").pipe(
      Flag.withDefault("claudeAgent"),
      Flag.withDescription("Provider instance id of every thread, run and session."),
    ),
    driver: Flag.String("driver").pipe(Flag.withDefault("claudeAgent")),
    model: Flag.String("model").pipe(Flag.withDefault("claude-sonnet-4-6")),
    workspaceRoot: Flag.String("workspace-root").pipe(
      Flag.withDefault("/t3-bench-synthetic"),
      Flag.withDescription(
        "Parent of the (nonexistent) project folders; the driver re-roots the ones it uses.",
      ),
    ),
    now: Flag.String("now").pipe(
      Flag.optional,
      Flag.withDescription("Newest activity time (default: the shape's capturedAt)."),
    ),
    report: reportFlag,
    destination: Argument.String("destination").pipe(
      Argument.withDescription("New database file."),
    ),
  },
  (input) =>
    Effect.gen(function* () {
      const result = yield* runSynth({
        shapePath: input.like,
        scale: Number(input.scale),
        seed: input.seed,
        destination: input.destination,
        providerInstanceId: input.provider,
        driver: input.driver,
        model: input.model,
        workspaceRoot: input.workspaceRoot,
        now: Option.getOrUndefined(input.now),
        report: Option.getOrUndefined(input.report),
      });
      const lines = result.tables
        .filter((row) => row.target > 0 || row.actual > 0)
        .map(
          (row) =>
            `  ${row.table.padEnd(56)} ${String(row.actual).padStart(10)} / ${String(row.target).padStart(10)} ${percent(row.deviation).padStart(8)}  ${mib(row.actualBytes).padStart(12)} / ${mib(row.targetBytes)}`,
        );
      yield* Console.log(
        [
          `Synthetic database: ${result.destination}`,
          `  size ${mib(result.sizeBytes)} / target ${mib(result.targetBytes)} (${percent(result.sizeDeviation)})`,
          `  table rows actual / target, bytes actual / target:`,
          ...lines,
          ...(Object.keys(result.substituted).length > 0
            ? [`  item types written as system_notice: ${JSON.stringify(result.substituted)}`]
            : []),
        ].join("\n"),
      );
    }),
).pipe(
  Command.withDescription(
    "Generate a database with a shape's schema and shape through the server's own migrations and event sink: projects, threads with subagent children, runs, attempts, nodes, provider threads, turns and sessions, messages, turn items, plans, checkpoints and events.",
  ),
);

const shapeCommand = Command.make(
  "shape",
  {
    database: Argument.String("database").pipe(
      Argument.withDescription("Database to measure (opened read-only)."),
    ),
    label: Flag.String("label").pipe(Flag.optional),
    capturedAt: Flag.String("captured-at").pipe(Flag.optional),
  },
  ({ database, label, capturedAt }) =>
    Effect.sync(() => {
      const db = new NodeSqlite.DatabaseSync(database, { readOnly: true });
      try {
        const shape = measureShape(db, {
          ...(Option.isSome(label) ? { label: label.value } : {}),
          ...(Option.isSome(capturedAt) ? { capturedAt: capturedAt.value } : {}),
        });
        process.stdout.write(`${JSON.stringify(shape, null, 2)}\n`);
      } finally {
        db.close();
      }
    }),
).pipe(
  Command.withDescription(
    "Print a database's shape as JSON: row counts, bytes, payload size quantiles, sqlite_stat1, per-thread and per-project distributions, lineage and type mixes. No row content.",
  ),
);

const scanCommand = Command.make(
  "scan",
  {
    database: Argument.String("database"),
    keepContent: Flag.Boolean("keep-content").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "The fixture was made with --keep-content (or by synth): run the pattern layer, not the filler check.",
      ),
    ),
  },
  ({ database, keepContent }) =>
    Effect.gen(function* () {
      const check = yield* checkFixture(database, keepContent ? "redact" : "blank");
      yield* Console.log(JSON.stringify(check, null, 2));
      if (!isClean(check)) {
        return yield* failed(`${database} is not a clean fixture.`);
      }
    }),
).pipe(
  Command.withDescription(
    "Check a fixture: every value allowlisted or filler, credential patterns in decoded content and in the raw file, P1-002's extended check, credential-named columns, and work a server would run on start.",
  ),
);

const fixture = Command.make("fixture").pipe(
  Command.withDescription("T3 server bench fixtures: sanitised copies and synthetic databases."),
  Command.withSubcommands([sanitiseCommand, synthCommand, shapeCommand, scanCommand]),
);

const exit = await Effect.runPromiseExit(
  Command.run(fixture, { version: "0.0.0" }).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(References.MinimumLogLevel, "Warn"),
  ),
);
if (Exit.isFailure(exit)) {
  const error = Cause.squash(exit.cause) as { readonly _tag?: string; readonly message?: string };
  const tag = error._tag;
  process.stderr.write(
    tag === "FixtureRefusedError" || tag === "FixtureFailedError"
      ? `fixture: ${error.message}\n`
      : `${Cause.pretty(exit.cause)}\n`,
  );
  process.exitCode = tag === "FixtureRefusedError" ? 2 : 1;
}

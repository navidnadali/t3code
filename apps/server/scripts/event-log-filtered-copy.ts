#!/usr/bin/env node
// Offline filtered copy of a stopped server's database, from a source checkout:
//   node apps/server/scripts/event-log-filtered-copy.ts copy <abs db> [--no-swap] [--integrity full]
//   node apps/server/scripts/event-log-filtered-copy.ts restore <abs db>
// One JSON line out; a refusal exits 2, a usage error 64.
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as FilteredCopy from "../src/persistence/EventLogFilteredCopy.ts";

class FilteredCopyUsageError extends Schema.TaggedError<FilteredCopyUsageError>()(
  "FilteredCopyUsageError",
  { reason: Schema.String },
) {
  override get message() {
    return `Usage: ${this.reason}`;
  }
}

interface FilteredCopyStep {
  readonly step: "copy" | "restore";
  readonly databasePath: string;
  readonly swap?: boolean;
  readonly integrity?: "quick" | "full";
}

export const runFilteredCopyStep = Effect.fn("runFilteredCopyStep")(function* (
  input: FilteredCopyStep,
) {
  const path = yield* Path.Path;
  if (!path.isAbsolute(input.databasePath)) {
    return yield* new FilteredCopyUsageError({ reason: "the database path must be absolute" });
  }
  if (input.step === "restore") {
    yield* FilteredCopy.restoreBeforeFilteredCopy(input.databasePath);
    return { step: "restore" as const, restored: true };
  }
  const result = yield* FilteredCopy.compactByFilteredCopy(input.databasePath, {
    swap: input.swap ?? true,
    integrity: input.integrity ?? "quick",
  });
  return { step: "copy" as const, ...result };
});

const parseArguments = Effect.fn("parseFilteredCopyArguments")(function* (
  argv: ReadonlyArray<string>,
) {
  const [step, databasePath, ...flags] = argv;
  if ((step !== "copy" && step !== "restore") || databasePath === undefined) {
    return yield* new FilteredCopyUsageError({
      reason: "copy|restore <absolute db path> [--no-swap] [--integrity quick|full]",
    });
  }
  let swap = true;
  let integrity: "quick" | "full" = "quick";
  for (let index = 0; index < flags.length; index += 1) {
    const flag = flags[index];
    if (flag === "--no-swap") {
      swap = false;
    } else if (
      flag === "--integrity" &&
      (flags[index + 1] === "quick" || flags[index + 1] === "full")
    ) {
      integrity = flags[index + 1] as "quick" | "full";
      index += 1;
    } else {
      return yield* new FilteredCopyUsageError({ reason: `unknown argument ${flag}` });
    }
  }
  return { step, databasePath, swap, integrity } satisfies FilteredCopyStep;
});

if (import.meta.main) {
  const program = parseArguments(process.argv.slice(2)).pipe(
    Effect.flatMap(runFilteredCopyStep),
    Effect.flatMap((result) => Console.log(JSON.stringify(result))),
    Effect.catchTags({
      FilteredCopyUsageError: (error) =>
        Console.error(error.message).pipe(
          Effect.andThen(Effect.sync(() => (process.exitCode = 64))),
        ),
      FilteredCopyError: (error) =>
        Console.log(JSON.stringify({ failedStep: error.step, reason: error.reason })).pipe(
          Effect.andThen(Effect.sync(() => (process.exitCode = 2))),
        ),
    }),
  );
  NodeRuntime.runMain(program.pipe(Effect.provide(NodeServices.layer)));
}

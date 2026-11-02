import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeUtil from "node:util";

import * as BootService from "../cloud/bootService.ts";
import { runDoctor } from "./doctor.ts";
import { DatabaseCompactError, DatabaseMaintenance } from "../persistence/DatabaseMaintenance.ts";

it.effect(
  "--fix reclaims space with the service paused and keeps the V1 database, and --dry-run changes nothing",
  () =>
    Effect.gen(function* () {
      const calls: string[] = [];
      const doctor = (dryRun: boolean) =>
        runDoctor("/test/statev2.sqlite", { fix: true, dryRun, interactive: false }).pipe(
          Effect.provide(
            Layer.mergeAll(
              NodeServices.layer,
              Layer.mock(BootService.BootService)({
                withStopped: (operation) =>
                  Effect.sync(() => calls.push("pause")).pipe(
                    Effect.andThen(operation),
                    Effect.ensuring(Effect.sync(() => calls.push("resume"))),
                  ),
              }),
              Layer.succeed(DatabaseMaintenance, {
                inspect: () =>
                  Effect.succeed({
                    databaseBytes: 2000,
                    reclaimableBytes: 1000,
                    incrementalVacuumEnabled: false,
                    legacyDatabaseBytes: 4000,
                  }),
                compact: () =>
                  Effect.sync(() => {
                    calls.push("compact");
                    return {
                      beforeBytes: 2000,
                      afterBytes: 1000,
                      deletedEventCount: 1,
                      deletedReceiptCount: 0,
                      repairedItemCountThreads: 0,
                    };
                  }),
                removeLegacyDatabase: () => Effect.sync(() => calls.push("remove legacy")),
              }),
            ),
          ),
        );
      yield* doctor(true);
      assert.deepEqual(calls, []);
      yield* doctor(false);
      assert.deepEqual(calls, ["pause", "compact", "resume"]);
    }),
);

it.effect("says when compaction was skipped for lack of disk space, and what it needed", () =>
  Effect.gen(function* () {
    const printed: Array<string> = [];
    const testConsole = {
      ...globalThis.console,
      log: (...args: ReadonlyArray<unknown>) => {
        printed.push(NodeUtil.format(...args));
      },
    } satisfies Console.Console;
    yield* runDoctor("/test/statev2.sqlite", { fix: true, dryRun: false, interactive: false }).pipe(
      Effect.provide(
        Layer.mergeAll(
          NodeServices.layer,
          Layer.mock(BootService.BootService)({ withStopped: (operation) => operation }),
          Layer.succeed(DatabaseMaintenance, {
            inspect: () =>
              Effect.succeed({
                databaseBytes: 2000,
                reclaimableBytes: 1000,
                incrementalVacuumEnabled: false,
                legacyDatabaseBytes: 0,
              }),
            compact: () =>
              Effect.succeed({
                beforeBytes: 2000,
                afterBytes: 2000,
                deletedEventCount: 0,
                deletedReceiptCount: 0,
                repairedItemCountThreads: 0,
                skipped: {
                  scratchPath: "/test/compaction-scratch.sqlite",
                  requiredBytes: 300 * 1024 * 1024,
                  freeBytes: 12 * 1024 * 1024,
                  stage: "before",
                },
              }),
            removeLegacyDatabase: () => Effect.void,
          }),
        ),
      ),
      Effect.provideService(Console.Console, testConsole),
    );
    const output = printed.join("\n");
    assert.include(output, "skipped");
    assert.include(output, "/test/compaction-scratch.sqlite");
    assert.include(output, "300");
    assert.include(output, "12");
  }),
);

it.effect("still says why compaction was skipped when the VACUUM after it fails", () =>
  Effect.gen(function* () {
    const printed: Array<string> = [];
    const testConsole = {
      ...globalThis.console,
      log: (...args: ReadonlyArray<unknown>) => {
        printed.push(NodeUtil.format(...args));
      },
    } satisfies Console.Console;
    const error = yield* runDoctor("/test/statev2.sqlite", {
      fix: true,
      dryRun: false,
      interactive: false,
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          NodeServices.layer,
          Layer.mock(BootService.BootService)({ withStopped: (operation) => operation }),
          Layer.succeed(DatabaseMaintenance, {
            inspect: () =>
              Effect.succeed({
                databaseBytes: 2000,
                reclaimableBytes: 1000,
                incrementalVacuumEnabled: false,
                legacyDatabaseBytes: 0,
              }),
            compact: () =>
              Effect.fail(
                new DatabaseCompactError({
                  path: "/test/statev2.sqlite",
                  cause: new Error("database or disk is full"),
                  compaction: {
                    deletedEventCount: 0,
                    deletedReceiptCount: 0,
                    repairedItemCountThreads: 0,
                    skipped: {
                      scratchPath: "/test/compaction-scratch.sqlite",
                      requiredBytes: 64 * 1024 * 1024,
                      freeBytes: 12 * 1024 * 1024,
                      stage: "before",
                    },
                  },
                }),
              ),
            removeLegacyDatabase: () => Effect.void,
          }),
        ),
      ),
      Effect.provideService(Console.Console, testConsole),
      Effect.flip,
    );
    assert.equal(error._tag, "DatabaseCompactError");
    const output = printed.join("\n");
    assert.include(output, "skipped");
    assert.include(output, "/test/compaction-scratch.sqlite");
    assert.include(output, "64 MiB");
    assert.include(output, "12 MiB");
  }),
);

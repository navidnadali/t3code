import {
  AcpRegistryDriver,
  type AcpRegistryDriverEnv,
} from "@t3tools/provider-acp-registry/server";
import { AntigravityDriver, type AntigravityDriverEnv } from "./Drivers/AntigravityDriver.ts";
import { ClaudeDriver, type ClaudeDriverEnv } from "./Drivers/ClaudeDriver.ts";
import { CodexDriver, type CodexDriverEnv } from "./Drivers/CodexDriver.ts";
import { CursorDriver, type CursorDriverEnv } from "@t3tools/provider-cursor/server";
import { FxDriver, type FxDriverEnv } from "@t3tools/provider-fx/server";
import { GrokDriver, type GrokDriverEnv } from "@t3tools/provider-grok/server";
import { OpenCodeDriver, type OpenCodeDriverEnv } from "@t3tools/provider-opencode/server";
import { MuseDriver, type MuseDriverEnv } from "@t3tools/provider-muse/server";
import { PiDriver, type PiDriverEnv } from "@t3tools/provider-pi/server";
import type {
  AnyProviderDriver,
  ProviderDriver,
  ProviderUsageReaderEnv,
} from "@t3tools/provider-core/server/driver";
import { KiroDriver, type KiroDriverEnv } from "@t3tools/provider-kiro/server";
import * as Effect from "effect/Effect";
import { ChildProcessSpawner } from "effect/process";

import * as WorkerChildProcessSpawner from "../process/workerChildProcessSpawner.ts";

export type BuiltInDriversEnv =
  | AcpRegistryDriverEnv
  | AntigravityDriverEnv
  | ClaudeDriverEnv
  | CodexDriverEnv
  | CursorDriverEnv
  | FxDriverEnv
  | GrokDriverEnv
  | KiroDriverEnv
  | OpenCodeDriverEnv
  | PiDriverEnv
  | MuseDriverEnv;

const withSpawner =
  (
    spawner: Effect.Effect<
      ChildProcessSpawner.ChildProcessSpawner["Service"],
      never,
      ChildProcessSpawner.ChildProcessSpawner
    >,
  ) =>
  <Config, R, UsageR>(
    driver: ProviderDriver<Config, R | ChildProcessSpawner.ChildProcessSpawner, UsageR>,
  ): ProviderDriver<Config, R | ChildProcessSpawner.ChildProcessSpawner, UsageR> => ({
    ...driver,
    create: (input) =>
      driver
        .create(input)
        .pipe(Effect.provideServiceEffect(ChildProcessSpawner.ChildProcessSpawner, spawner)),
  });

/**
 * Sessions and checks of a provider verified on hand-off (real CLI, socket stdio, survives the
 * spawner's death) start from the server's provider launcher. Claude and Codex pick their spawners
 * themselves; the others stay on Node's spawner (docs/operations/observability.md says why).
 */
const launchedByServer = withSpawner(WorkerChildProcessSpawner.providerSpawner);

/** Muse's driver starts only checks (its SDK starts its sessions): the server's command spawner. */
const checkedByServer = withSpawner(WorkerChildProcessSpawner.commandSpawner);

export const BUILT_IN_DRIVERS: ReadonlyArray<AnyProviderDriver<BuiltInDriversEnv>> = [
  CodexDriver,
  ClaudeDriver,
  CursorDriver,
  GrokDriver,
  FxDriver,
  launchedByServer(OpenCodeDriver),
  AntigravityDriver,
  KiroDriver,
  launchedByServer(PiDriver),
  checkedByServer(MuseDriver),
  AcpRegistryDriver,
];

export type BuiltInUsageReadersEnv =
  | ProviderUsageReaderEnv<typeof ClaudeDriver>
  | ProviderUsageReaderEnv<typeof CodexDriver>
  | ProviderUsageReaderEnv<typeof GrokDriver>
  | ProviderUsageReaderEnv<typeof OpenCodeDriver>
  | ProviderUsageReaderEnv<typeof AntigravityDriver>
  | ProviderUsageReaderEnv<typeof CursorDriver>
  | ProviderUsageReaderEnv<typeof PiDriver>
  | ProviderUsageReaderEnv<typeof MuseDriver>;

/** Order matters: aggregation keeps the first duplicate record. */
export const BUILT_IN_USAGE_DRIVERS: ReadonlyArray<
  AnyProviderDriver<BuiltInDriversEnv, BuiltInUsageReadersEnv>
> = [
  ClaudeDriver,
  CodexDriver,
  GrokDriver,
  PiDriver,
  OpenCodeDriver,
  MuseDriver,
  AntigravityDriver,
  CursorDriver,
];

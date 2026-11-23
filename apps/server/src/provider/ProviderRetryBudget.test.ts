import { assert, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";

import * as ProviderRetryBudget from "./ProviderRetryBudget.ts";

const CODEX = ProviderRetryBudget.PROVIDER_RETRY_BUDGET_VARIABLES.codex;
const CLAUDE = ProviderRetryBudget.PROVIDER_RETRY_BUDGET_VARIABLES.claudeAgent;
const DEFAULTS = ProviderRetryBudget.DEFAULT_PROVIDER_RETRY_BUDGETS;
const minutes = (count: number) => count * 60 * 1000;

const readBudgets = (env: Record<string, string>) =>
  Effect.gen(function* () {
    const warnings: Array<string> = [];
    const logger = Logger.make(({ logLevel, message }) => {
      if (logLevel === "Warn") warnings.push(String(message));
    });
    const budgets = yield* ProviderRetryBudget.ProviderRetryBudget.pipe(
      Effect.provide(
        ProviderRetryBudget.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              ConfigProvider.layer(ConfigProvider.fromEnv({ env })),
              Logger.layer([logger], { mergeWithExisting: false }),
            ),
          ),
        ),
      ),
    );
    return { budgets, warnings };
  });

it.effect.each([
  { case: "unset", env: {} },
  { case: "empty", env: { [CODEX]: "", [CLAUDE]: "  " } },
])("keeps every driver's default budget when its variable is $case", ({ env }) =>
  Effect.gen(function* () {
    const { budgets, warnings } = yield* readBudgets(env);
    assert.deepStrictEqual(budgets, { codex: minutes(10), claudeAgent: minutes(10) });
    assert.deepStrictEqual(warnings, []);
  }),
);

it.effect.each([
  { case: "Codex alone", env: { [CODEX]: "60" }, set: { codex: minutes(60) } },
  { case: "Claude alone", env: { [CLAUDE]: "25" }, set: { claudeAgent: minutes(25) } },
  {
    case: "both, padded, at the bounds",
    env: { [CODEX]: " 1440 ", [CLAUDE]: "1" },
    set: { codex: minutes(1440), claudeAgent: minutes(1) },
  },
])("gives a valid budget to its own driver only: $case", ({ env, set }) =>
  Effect.gen(function* () {
    const { budgets, warnings } = yield* readBudgets(env);
    assert.deepStrictEqual(budgets, { ...DEFAULTS, ...set });
    assert.deepStrictEqual(warnings, []);
  }),
);

it.effect.each([
  { variable: CODEX, value: "abc" },
  { variable: CODEX, value: "0" },
  { variable: CODEX, value: "-5" },
  { variable: CODEX, value: "1.5" },
  { variable: CODEX, value: "1441" },
  { variable: CLAUDE, value: "60m" },
])("keeps the default and warns once naming $variable for $value", ({ variable, value }) =>
  Effect.gen(function* () {
    const other = variable === CODEX ? CLAUDE : CODEX;
    const { budgets, warnings } = yield* readBudgets({ [variable]: value, [other]: "25" });
    assert.lengthOf(warnings, 1);
    assert.include(warnings[0], variable);
    assert.notInclude(warnings[0], other);
    assert.deepStrictEqual(
      budgets,
      variable === CODEX
        ? { ...DEFAULTS, claudeAgent: minutes(25) }
        : { ...DEFAULTS, codex: minutes(25) },
    );
  }),
);

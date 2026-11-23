import { PROVIDER_RETRY_BUDGET_MS } from "@t3tools/provider-core/server/failure";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

/** Per driver, the longest a turn may stay in provider retries before its adapter fails it, in ms. */
export interface ProviderRetryBudgets {
  readonly codex: number;
  readonly claudeAgent: number;
}

export const PROVIDER_RETRY_BUDGET_VARIABLES = {
  codex: "T3CODE_PROVIDER_RETRY_BUDGET_CODEX_MINUTES",
  claudeAgent: "T3CODE_PROVIDER_RETRY_BUDGET_CLAUDE_MINUTES",
} as const satisfies Record<keyof ProviderRetryBudgets, string>;

export const DEFAULT_PROVIDER_RETRY_BUDGETS: ProviderRetryBudgets = {
  codex: PROVIDER_RETRY_BUDGET_MS,
  claudeAgent: PROVIDER_RETRY_BUDGET_MS,
};

export const ProviderRetryBudget = Context.Reference<ProviderRetryBudgets>(
  "t3/provider/ProviderRetryBudget",
  { defaultValue: () => DEFAULT_PROVIDER_RETRY_BUDGETS },
);

const MINUTE_MS = 60_000;
const MAX_MINUTES = 24 * 60;

const readBudgetMs = Effect.fn("ProviderRetryBudget.read")(function* (
  variable: string,
  defaultMs: number,
) {
  const value = Option.getOrUndefined(
    yield* Config.String(variable).pipe(Config.option, Effect.orElseSucceed(Option.none)),
  )?.trim();
  if (value === undefined || value.length === 0) return defaultMs;
  const minutes = /^\d+$/.test(value) ? Number(value) : Number.NaN;
  if (minutes >= 1 && minutes <= MAX_MINUTES) return minutes * MINUTE_MS;
  yield* Effect.logWarning(
    `Ignoring ${variable}="${value}": expected whole minutes from 1 to ${MAX_MINUTES}. Using the default of ${defaultMs / MINUTE_MS} minutes.`,
  );
  return defaultMs;
});

/** Read once when the server builds its providers; a bad value warns and keeps the default. */
export const layer = Layer.effect(
  ProviderRetryBudget,
  Effect.all({
    codex: readBudgetMs(
      PROVIDER_RETRY_BUDGET_VARIABLES.codex,
      DEFAULT_PROVIDER_RETRY_BUDGETS.codex,
    ),
    claudeAgent: readBudgetMs(
      PROVIDER_RETRY_BUDGET_VARIABLES.claudeAgent,
      DEFAULT_PROVIDER_RETRY_BUDGETS.claudeAgent,
    ),
  }),
);

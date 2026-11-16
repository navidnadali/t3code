/** The planned provider instances must be ready before the driver starts sessions on them. */

export interface ProviderEntry {
  readonly instanceId: string;
  readonly driver?: string;
  readonly status?: string;
  readonly enabled: boolean;
  readonly installed: boolean;
  readonly availability?: string;
  readonly unavailableReason?: string;
  readonly message?: string;
}

/** Why an instance cannot take sessions; null when it can. */
export function providerProblem(
  providers: ReadonlyArray<ProviderEntry>,
  instanceId: string,
): string | null {
  const provider = providers.find((entry) => entry.instanceId === instanceId);
  if (provider === undefined) {
    return `is not configured (configured: ${providers.map((entry) => entry.instanceId).join(", ") || "none"})`;
  }
  if (!provider.enabled) return "is disabled";
  if (!provider.installed || provider.availability === "unavailable") {
    return `is unavailable: ${provider.unavailableReason ?? provider.message ?? "not installed"}`;
  }
  return null;
}

/**
 * Polls `fetchConfig` until every planned instance can take sessions. A server reports a provider it has not probed yet
 * as not installed ("status has not been checked in this session yet") for its first moments, so a problem is retried
 * every `intervalMs` (default 500) until `timeoutMs` is spent; then the last problem is the error.
 */
export async function awaitProviders<
  C extends { readonly providers: ReadonlyArray<ProviderEntry> },
>(
  fetchConfig: () => Promise<C>,
  instanceIds: ReadonlyArray<string>,
  options: {
    readonly timeoutMs: number;
    readonly intervalMs?: number;
    readonly sleep?: (ms: number) => Promise<void>;
    readonly now?: () => number;
    /** Called once, on the first wait, with the problems that made it wait. */
    readonly onWait?: (problems: ReadonlyArray<string>) => void;
  },
): Promise<C> {
  const interval = options.intervalMs ?? 500;
  const sleep =
    options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  // A deadline on the clock: a slow getConfig spends the time as much as the sleeps between calls do.
  const now = options.now ?? (() => performance.now());
  const deadline = now() + options.timeoutMs;
  let waiting = false;
  for (;;) {
    const config = await fetchConfig();
    const problems = instanceIds.flatMap((instanceId) => {
      const problem = providerProblem(config.providers, instanceId);
      return problem === null ? [] : [`provider instance "${instanceId}" ${problem}`];
    });
    if (problems.length === 0) return config;
    if (now() >= deadline) throw new Error(problems.join("; "));
    if (!waiting) options.onWait?.(problems);
    waiting = true;
    await sleep(interval);
  }
}

const LISTED = [
  "instanceId",
  "driver",
  "enabled",
  "installed",
  "status",
  "availability",
  "unavailableReason",
  "message",
] as const;

/**
 * Every instance the server reports, as driver.json records it: identity and availability only (models and the
 * rest are left out). An instance whose driver the build does not know shows here as unavailable, with its reason.
 */
export function providerListing(providers: ReadonlyArray<ProviderEntry>): Array<ProviderEntry> {
  return providers.map(
    (provider) =>
      Object.fromEntries(
        LISTED.filter((key) => provider[key] !== undefined).map((key) => [key, provider[key]]),
      ) as unknown as ProviderEntry,
  );
}

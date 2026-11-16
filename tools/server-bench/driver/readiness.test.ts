import { describe, expect, it } from "vite-plus/test";

import {
  awaitProviders,
  providerListing,
  providerProblem,
  type ProviderEntry,
} from "./readiness.ts";

const pending = (instanceId: string): ProviderEntry => ({
  instanceId,
  enabled: true,
  installed: false,
  message: "Claude provider status has not been checked in this session yet.",
});
const ready = (instanceId: string): ProviderEntry => ({
  instanceId,
  enabled: true,
  installed: true,
});

/**
 * Serves the given getConfig answers in turn (the last one repeats) on a fake clock: each sleep moves it on, and so
 * does each getConfig by `fetchMs` (a slow server). Counts the sleeps and the calls.
 */
const server = (answers: ReadonlyArray<ReadonlyArray<ProviderEntry>>, fetchMs = 0) => {
  const sleeps: Array<number> = [];
  let call = 0;
  let clock = 1_000;
  return {
    sleeps,
    calls: () => call,
    now: () => clock,
    fetchConfig: async () => {
      clock += fetchMs;
      return { providers: answers[Math.min(call++, answers.length - 1)]! };
    },
    sleep: async (ms: number) => {
      sleeps.push(ms);
      clock += ms;
    },
  };
};

describe("providerProblem", () => {
  it("names a missing, disabled or unavailable instance and passes an installed one", () => {
    const providers = [
      ready("claudeBench"),
      { instanceId: "off", enabled: false, installed: true },
      {
        instanceId: "gone",
        enabled: true,
        installed: true,
        availability: "unavailable",
        unavailableReason: "no binary",
      },
      pending("codexBench"),
    ];
    expect(providerProblem(providers, "claudeBench")).toBeNull();
    expect(providerProblem(providers, "off")).toBe("is disabled");
    expect(providerProblem(providers, "gone")).toBe("is unavailable: no binary");
    expect(providerProblem(providers, "codexBench")).toBe(
      "is unavailable: Claude provider status has not been checked in this session yet.",
    );
    expect(providerProblem(providers, "acpBench")).toBe(
      "is not configured (configured: claudeBench, off, gone, codexBench)",
    );
  });
});

describe("awaitProviders (the server probes its providers shortly after it starts)", () => {
  it("polls getConfig while an instance's status has not been checked yet", async () => {
    const fake = server([
      [pending("claudeBench")],
      [pending("claudeBench")],
      [ready("claudeBench")],
    ]);
    const config = await awaitProviders(fake.fetchConfig, ["claudeBench"], {
      timeoutMs: 60_000,
      sleep: fake.sleep,
      now: fake.now,
    });
    expect(config.providers).toEqual([ready("claudeBench")]);
    expect(fake.sleeps).toEqual([500, 500]);
  });

  it("does not wait when every planned instance is ready", async () => {
    const fake = server([[ready("claudeBench"), ready("codexBench")]]);
    await awaitProviders(fake.fetchConfig, ["claudeBench", "codexBench"], {
      timeoutMs: 60_000,
      sleep: fake.sleep,
      now: fake.now,
    });
    expect(fake.sleeps).toEqual([]);
  });

  it("fails with the last problem once the timeout is spent", async () => {
    const fake = server([[pending("claudeBench")]]);
    await expect(
      awaitProviders(fake.fetchConfig, ["claudeBench"], {
        timeoutMs: 2000,
        sleep: fake.sleep,
        now: fake.now,
      }),
    ).rejects.toThrow(
      'provider instance "claudeBench" is unavailable: Claude provider status has not been checked in this session yet.',
    );
    expect(fake.sleeps).toEqual([500, 500, 500, 500]);
  });
});

describe("awaitProviders keeps to its deadline on the clock", () => {
  it("gives up when slow getConfig calls spend the time, not after the sum of its sleeps", async () => {
    // Each getConfig takes 30 s: the 60 s deadline passes on the second call, after one sleep.
    const fake = server([[pending("claudeBench")]], 30_000);
    await expect(
      awaitProviders(fake.fetchConfig, ["claudeBench"], {
        timeoutMs: 60_000,
        sleep: fake.sleep,
        now: fake.now,
      }),
    ).rejects.toThrow(/has not been checked in this session yet/);
    expect(fake.calls()).toBe(2);
    expect(fake.sleeps).toEqual([500]);
  });
});

describe("providerListing (what the server reports for every instance)", () => {
  it("keeps each instance's identity and availability, unknown drivers included", () => {
    expect(
      providerListing([
        {
          instanceId: "claudeBench",
          driver: "claudeAgent",
          enabled: true,
          installed: true,
          status: "ready",
        },
        {
          instanceId: "fx",
          driver: "fx",
          enabled: false,
          installed: false,
          availability: "unavailable",
          unavailableReason: "Driver 'fx' is not registered in this build.",
          models: [{ id: "m" }],
          message: "x",
        } as ProviderEntry,
      ]),
    ).toEqual([
      {
        instanceId: "claudeBench",
        driver: "claudeAgent",
        enabled: true,
        installed: true,
        status: "ready",
      },
      {
        instanceId: "fx",
        driver: "fx",
        enabled: false,
        installed: false,
        availability: "unavailable",
        unavailableReason: "Driver 'fx' is not registered in this build.",
        message: "x",
      },
    ]);
  });
});

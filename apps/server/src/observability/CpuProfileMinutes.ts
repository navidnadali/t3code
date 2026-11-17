// @effect-diagnostics nodeBuiltinImport:off - node:inspector's in-process session is the only CPU profiler; node:module loads it where the build has one.
import * as NodeModule from "node:module";

import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

import { roundMs } from "./SpanMinutes.ts";

/** The functions a minute line names; the rest count together. */
export const TOP_FUNCTIONS = 25;

export interface ProfileNode {
  readonly id: number;
  readonly callFrame: {
    readonly functionName: string;
    readonly url: string;
    readonly lineNumber: number;
  };
  readonly hitCount?: number;
}

/** The parts of a V8 `Profiler.Profile` the aggregate reads; times in microseconds. */
export interface Profile {
  readonly nodes: ReadonlyArray<ProfileNode>;
  readonly startTime: number;
  readonly endTime: number;
  readonly samples?: ReadonlyArray<number>;
}

/** Self time in ms by `function file:line`, the file without its directory; most first. */
export interface CpuProfileMinute {
  readonly samples: number;
  readonly sampledMs: number;
  readonly top: ReadonlyArray<readonly [name: string, selfMs: number]>;
  readonly otherMs: number;
}

// Code-derived names only: a script's directory can name the user, so it is dropped.
const frameName = ({ functionName, url, lineNumber }: ProfileNode["callFrame"]) => {
  const name = functionName === "" ? "(anonymous)" : functionName;
  if (url === "") return name;
  const file = url.split(/[?#]/, 1)[0]!.split(/[/\\]/).pop();
  return `${name} ${file}:${lineNumber + 1}`;
};

export const aggregateProfile = (profile: Profile, topCount: number): CpuProfileMinute => {
  const samples = profile.samples?.length ?? 0;
  const sampledMs = (profile.endTime - profile.startTime) / 1000;
  const sampleMs = samples === 0 ? 0 : sampledMs / samples;
  const selfMs = new Map<string, number>();
  for (const node of profile.nodes) {
    if (!node.hitCount) continue;
    const name = frameName(node.callFrame);
    selfMs.set(name, (selfMs.get(name) ?? 0) + node.hitCount * sampleMs);
  }
  const ranked = Array.from(selfMs).toSorted(([, left], [, right]) => right - left);
  return {
    samples,
    sampledMs: roundMs(sampledMs),
    top: ranked.slice(0, topCount).map(([name, ms]) => [name, roundMs(ms)] as const),
    otherMs: roundMs(ranked.slice(topCount).reduce((sum, [, ms]) => sum + ms, 0)),
  };
};

interface InspectorSession {
  connect(): void;
  disconnect(): void;
  post(method: string, params?: object, callback?: (error: Error | null) => void): void;
  on(event: string, listener: (message: { readonly params: unknown }) => void): void;
}

const TITLES = ["t3-span-minutes-0", "t3-span-minutes-1"] as const;

/**
 * Two console profiles overlap at each minute's end: the next starts before the last ends, so
 * V8 keeps its code map and never walks the heap again, which a restart does (seconds on a large
 * heap). An in-process session answers synchronously and opens no port.
 */
const startProfiler = (intervalMs: number) => {
  if (process.features.inspector !== true) return undefined;
  // Its own console, not the global one, which a host may have replaced.
  const inspector = NodeModule.createRequire(process.execPath)("node:inspector") as {
    readonly Session: new () => InspectorSession;
    readonly console: Pick<Console, "profile" | "profileEnd">;
  };
  const session = new inspector.Session();
  session.connect();
  const finished = new Map<string, Profile>();
  session.on("Profiler.consoleProfileFinished", ({ params }) => {
    const { title, profile } = params as { readonly title?: string; readonly profile: Profile };
    if (title !== undefined) finished.set(title, profile);
  });
  session.post("Profiler.enable");
  session.post("Profiler.setSamplingInterval", { interval: Math.round(intervalMs * 1000) });
  let current = 0;
  inspector.console.profile(TITLES[current]);
  return {
    /** Ends the running minute's profile, once the next one runs. */
    next(): Profile | undefined {
      const ended = TITLES[current]!;
      current = 1 - current;
      inspector.console.profile(TITLES[current]);
      inspector.console.profileEnd(ended);
      const profile = finished.get(ended);
      finished.delete(ended);
      return profile;
    },
    stop(): void {
      inspector.console.profileEnd(TITLES[current]);
      session.post("Profiler.disable");
      session.disconnect();
    },
  };
};

/** Each minute's CPU profile, sampled every `intervalMs`; no profile where the build has no inspector. */
export const makeSection = Effect.fnUntraced(function* (options: {
  readonly intervalMs: number;
}): Effect.fn.Return<
  Effect.Effect<{ readonly cpuProfile?: CpuProfileMinute }>,
  never,
  Scope.Scope
> {
  const profiler = yield* Effect.acquireRelease(
    Effect.sync(() => startProfiler(options.intervalMs)).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("CPU profile for span minutes unavailable", cause).pipe(
          Effect.as(undefined),
        ),
      ),
    ),
    // A failed stop leaves nothing to clean up: the session ends with the process.
    (started) => Effect.sync(() => started?.stop()).pipe(Effect.catchDefect(() => Effect.void)),
  );
  return Effect.sync(() => {
    const profile = profiler?.next();
    return profile === undefined ? {} : { cpuProfile: aggregateProfile(profile, TOP_FUNCTIONS) };
  });
});

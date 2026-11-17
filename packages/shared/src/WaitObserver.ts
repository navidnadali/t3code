import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Tracer from "effect/Tracer";

/** Size classes for keys that ranked, such as a thread's runs; a key missing from it has none. */
export type KeySizer = (
  keys: ReadonlyArray<unknown>,
) => Effect.Effect<ReadonlyMap<unknown, Readonly<Record<string, number>>>>;

/**
 * Receives the waits at shared resources: locks, queues and connection pools. Every call runs on
 * the path it measures, so an implementation stays O(1) and keeps no reference to a key past the
 * minute it reports.
 */
export interface WaitObserver {
  /**
   * An acquisition at `site` began after `waitNs`, zero when nothing held it. `holder` names what
   * held it when the wait began; `key`, the keyed resource, ranks keys at a site; `waiter` names
   * the operation that waited, and `outcome` how the wait ended.
   */
  readonly wait: (
    site: string,
    waitNs: number,
    holder?: string,
    key?: unknown,
    waiter?: string,
    outcome?: string,
  ) => void;
  /** A keyed resource at `site` was held for `holdNs`. */
  readonly hold: (site: string, key: unknown, holdNs: number) => void;
  /** The queue length an arrival at `site` found. */
  readonly depth: (site: string, depth: number) => void;
  /**
   * Read once a minute: a gauge as it stands, a counter as its increase since the last read.
   * Returns the unregistration.
   */
  readonly register: (name: string, kind: "gauge" | "counter", read: () => number) => () => void;
  /**
   * A gauge per key at `site`, such as a thread's backlog, read once a minute: the line writes
   * the sum and ranks the largest keys. Returns the unregistration.
   */
  readonly registerKeyed: (site: string, key: unknown, read: () => number) => () => void;
  /** Sizes the keys of `site` that ranked in a minute. Returns the unregistration. */
  readonly sizeKeys: (site: string, sizer: KeySizer) => () => void;
}

/** Undefined unless the server records waits; every site then skips its measurement. */
export const CurrentWaitObserver = Context.Reference<WaitObserver | undefined>(
  "@t3tools/shared/WaitObserver/CurrentWaitObserver",
  { defaultValue: () => undefined },
);

const SQL_SPAN_NAMES: ReadonlySet<string> = new Set(["sqlite", "sql.transaction"]);

/** The current span's name, past SQL spans: the operation a fiber in `context` is running. */
export const operationName = (context: Context.Context<never>): string | undefined => {
  let span = Context.getOrUndefined(context, Tracer.ParentSpan);
  while (span?._tag === "Span" && SQL_SPAN_NAMES.has(span.name)) {
    const parent: Tracer.AnySpan | undefined = Option.getOrUndefined(span.parent);
    if (parent?._tag !== "Span") break;
    span = parent;
  }
  return span?._tag === "Span" ? span.name : undefined;
};

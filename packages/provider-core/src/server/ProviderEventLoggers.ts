import type { ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

/** A record serialized away from the logger, for the stream it was serialized for. */
export interface SerializedProviderEventRecord {
  readonly stream: string;
  readonly payload: string;
}

export interface EventNdjsonLogger {
  readonly filePath: string;
  readonly write: (event: unknown, threadId: ThreadId | null) => Effect.Effect<void>;
  /** Appends a serialized record as `write` would; the logger adds the timestamp and stream label. */
  readonly writeSerialized?: (
    record: SerializedProviderEventRecord,
    threadId: ThreadId | null,
  ) => Effect.Effect<void>;
  readonly close: () => Effect.Effect<void>;
}

export class ProviderEventLoggers extends Context.Service<
  ProviderEventLoggers,
  {
    readonly native: EventNdjsonLogger | undefined;
    readonly canonical: EventNdjsonLogger | undefined;
  }
>()("@t3tools/provider-core/server/ProviderEventLoggers") {}

export const NoOpProviderEventLoggers: ProviderEventLoggers["Service"] = {
  native: undefined,
  canonical: undefined,
};

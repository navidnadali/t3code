/**
 * A pure transform over one session's ordered output chunks. The same stage runs on a provider
 * I/O worker or in-thread, so it must not touch services, timers, or the process.
 */
export interface ProviderIoStage<Options, Output, Request = never, Reply = never, Event = never> {
  readonly name: string;
  readonly open: (
    options: Options,
    host: ProviderIoStageHost<Output, Event>,
  ) => ProviderIoStageSession<Output, Request, Reply>;
}

/** For a stage whose outputs do not follow from its chunks, such as one that runs a library. */
export interface ProviderIoStageHost<Output, Event> {
  /** Emits an output outside `push`; it is batched with the others. */
  readonly emit: (output: Output) => void;
  /** Sends an event to main after the outputs emitted before it, ahead of later ones. */
  readonly signal: (event: Event) => void;
  /** Ends the output after what was emitted before (failing it with `error` when given); calls go on. */
  readonly finish: (...error: [] | [unknown]) => void;
}

export interface ProviderIoStageSession<Output, Request = never, Reply = never> {
  readonly push: (chunk: Uint8Array, emit: (output: Output) => void) => void;
  readonly end: (emit: (output: Output) => void) => void;
  /** Runs after every chunk sent before it, on the same state (e.g. encoding that shares decode state). */
  readonly call?: (request: Request) => Reply | PromiseLike<Reply>;
  /** Runs when main closes the session. */
  readonly close?: () => void;
}

export type ProviderIoMainMessage =
  | {
      readonly type: "open";
      readonly session: number;
      readonly stage: string;
      readonly options: unknown;
    }
  | { readonly type: "bytes"; readonly session: number; readonly chunk: Uint8Array }
  | { readonly type: "eof"; readonly session: number }
  | { readonly type: "close"; readonly session: number }
  | {
      readonly type: "call";
      readonly session: number;
      readonly call: number;
      readonly request: unknown;
    }
  // A call whose reply main does not wait for.
  | { readonly type: "notify"; readonly session: number; readonly request: unknown };

// `bytes` is the input credit main returns once these outputs are taken (at once when there are none).
export type ProviderIoWorkerMessage =
  | { readonly type: "ready" }
  | {
      readonly type: "output";
      readonly session: number;
      readonly outputs: ReadonlyArray<unknown>;
      readonly bytes: number;
    }
  | {
      readonly type: "end";
      readonly session: number;
      readonly failed?: true;
      readonly error?: unknown;
    }
  | { readonly type: "fail"; readonly session: number; readonly error: unknown }
  | {
      readonly type: "reply";
      readonly session: number;
      readonly call: number;
      readonly ok: boolean;
      readonly value: unknown;
    }
  | { readonly type: "event"; readonly session: number; readonly event: unknown };

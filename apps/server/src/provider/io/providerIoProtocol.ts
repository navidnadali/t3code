/**
 * A pure transform over one session's ordered output chunks. The same stage runs on a provider
 * I/O worker or in-thread, so it must not touch services, timers, or the process.
 */
export interface ProviderIoStage<Options, Output, Request = never, Reply = never> {
  readonly name: string;
  readonly open: (options: Options) => ProviderIoStageSession<Output, Request, Reply>;
}

export interface ProviderIoStageSession<Output, Request = never, Reply = never> {
  readonly push: (chunk: Uint8Array, emit: (output: Output) => void) => void;
  readonly end: (emit: (output: Output) => void) => void;
  /** Runs after every chunk sent before it, on the same state (e.g. encoding that shares decode state). */
  readonly call?: (request: Request) => Reply;
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
    };

// `bytes` is the input credit main returns once these outputs are taken (at once when there are none).
export type ProviderIoWorkerMessage =
  | { readonly type: "ready" }
  | {
      readonly type: "output";
      readonly session: number;
      readonly outputs: ReadonlyArray<unknown>;
      readonly bytes: number;
    }
  | { readonly type: "end"; readonly session: number }
  | { readonly type: "fail"; readonly session: number; readonly error: unknown }
  | {
      readonly type: "reply";
      readonly session: number;
      readonly call: number;
      readonly ok: boolean;
      readonly value: unknown;
    };

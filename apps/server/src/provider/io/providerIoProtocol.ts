/**
 * A pure transform over one session's ordered output chunks. The same stage runs on a provider
 * I/O worker or in-thread, so it must not touch services, timers, or the process.
 */
export interface ProviderIoStage<Options, Output> {
  readonly name: string;
  readonly open: (options: Options) => ProviderIoStageSession<Output>;
}

export interface ProviderIoStageSession<Output> {
  readonly push: (chunk: Uint8Array, emit: (output: Output) => void) => void;
  readonly end: (emit: (output: Output) => void) => void;
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
  | { readonly type: "close"; readonly session: number };

// `bytes` returns the session's input credit; outputs are batched per event-loop turn.
export type ProviderIoWorkerMessage =
  | { readonly type: "ready" }
  | {
      readonly type: "output";
      readonly session: number;
      readonly outputs: ReadonlyArray<unknown>;
      readonly bytes: number;
    }
  | { readonly type: "end"; readonly session: number }
  | { readonly type: "fail"; readonly session: number; readonly error: unknown };

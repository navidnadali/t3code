import type {
  CanUseTool,
  HookCallback,
  HookCallbackMatcher,
  HookEvent,
  OnElicitation,
  OnUserDialog,
  Options,
  PermissionMode,
  Query,
  SDKMessage,
  SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";

import type { ProviderIoStage } from "./providerIoProtocol.ts";

export const CLAUDE_AGENT_SDK_STAGE = "claude-agent-sdk.query";

/** Callbacks main passes to the SDK; the worker relays each call to main, which answers it. */
export type ClaudeAgentSdkWorkerCallback = "canUseTool" | "onElicitation" | "onUserDialog";

/** Main's hooks with each matcher's callbacks replaced by their count. */
export type ClaudeAgentSdkWorkerHooks = Partial<
  Record<HookEvent, ReadonlyArray<Omit<HookCallbackMatcher, "hooks"> & { readonly hooks: number }>>
>;

// A callback's arguments as they cross to main: its options lose the abort signal.
type Relayed<Args extends ReadonlyArray<unknown>> = Args extends readonly [
  ...infer Head,
  infer Last,
]
  ? readonly [...Head, Omit<Last, "signal">]
  : never;

export type ClaudeAgentSdkCallbackCall =
  | { readonly name: "canUseTool"; readonly args: Relayed<Parameters<CanUseTool>> }
  | { readonly name: "onElicitation"; readonly args: Relayed<Parameters<OnElicitation>> }
  | { readonly name: "onUserDialog"; readonly args: Relayed<Parameters<OnUserDialog>> }
  | {
      readonly name: "hook";
      readonly hook: readonly [event: HookEvent, matcher: number, index: number];
      readonly args: Relayed<Parameters<HookCallback>>;
    };

export type ClaudeAgentSdkWorkerQueryOptions = Omit<
  Options,
  | "abortController"
  | "canUseTool"
  | "hooks"
  | "mcpServers"
  | "onElicitation"
  | "onUserDialog"
  | "spawnClaudeCodeProcess"
  | "stderr"
>;

export interface ClaudeAgentSdkStageOptions {
  readonly options: ClaudeAgentSdkWorkerQueryOptions;
  readonly callbacks: ReadonlyArray<ClaudeAgentSdkWorkerCallback>;
  readonly hooks?: ClaudeAgentSdkWorkerHooks;
}

export interface ClaudeAgentSdkProcessError {
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
  readonly code?: string;
  readonly errno?: number;
  readonly syscall?: string;
  readonly path?: string;
}

export type ClaudeAgentSdkWorkerRequest =
  | { readonly type: "start" }
  | { readonly type: "prompt"; readonly message: SDKUserMessage }
  | { readonly type: "promptEnd" }
  | { readonly type: "setMcpServers"; readonly servers: Parameters<Query["setMcpServers"]>[0] }
  | { readonly type: "setModel"; readonly model: Parameters<Query["setModel"]>[0] }
  | {
      readonly type: "applyFlagSettings";
      readonly settings: Parameters<Query["applyFlagSettings"]>[0];
    }
  | { readonly type: "setPermissionMode"; readonly mode: PermissionMode }
  | { readonly type: "stopTask"; readonly taskId: string }
  | {
      readonly type: "backgroundTasks";
      readonly toolUseId: Parameters<Query["backgroundTasks"]>[0];
    }
  | { readonly type: "interrupt" }
  | { readonly type: "close" }
  | { readonly type: "asyncDispose" }
  | { readonly type: "return" }
  | { readonly type: "stdoutEnd" }
  | { readonly type: "stdoutError"; readonly error: ClaudeAgentSdkProcessError }
  | { readonly type: "stdinError"; readonly error: ClaudeAgentSdkProcessError }
  | {
      readonly type: "exit";
      readonly code: number | null;
      readonly signal: NodeJS.Signals | null;
    }
  | { readonly type: "error"; readonly error: ClaudeAgentSdkProcessError }
  // The SDK's response field for the answer as JSON (`{"response":…}`), undefined for a null answer.
  | { readonly type: "callbackResult"; readonly id: number; readonly json: string | undefined }
  // The SDK's error field for a callback that threw as JSON (`{"error":…}`), undefined when the SDK
  // could not write it.
  | { readonly type: "callbackError"; readonly id: number; readonly json: string | undefined };

export type ClaudeAgentSdkWorkerEvent =
  | {
      readonly type: "spawn";
      readonly command: string;
      readonly args: ReadonlyArray<string>;
      readonly cwd: string | undefined;
      readonly env: Readonly<Record<string, string | undefined>>;
    }
  | { readonly type: "stdin"; readonly data: string | Uint8Array }
  | { readonly type: "stdinEnd" }
  | { readonly type: "kill"; readonly signal: NodeJS.Signals }
  | { readonly type: "abort" }
  // `after`: how many SDK messages the query had read before it raised the callback.
  | {
      readonly type: "callback";
      readonly id: number;
      readonly after: number;
      readonly call: ClaudeAgentSdkCallbackCall;
    }
  | { readonly type: "callbackAbort"; readonly id: number };

// Errors cross threads without their own fields; the SDK reads code and syscall on spawn errors.
export const toClaudeAgentSdkProcessError = (error: unknown): ClaudeAgentSdkProcessError => {
  if (!(error instanceof Error)) return { name: "Error", message: String(error) };
  const fields = error as Error & {
    readonly code?: unknown;
    readonly errno?: unknown;
    readonly syscall?: unknown;
    readonly path?: unknown;
  };
  return {
    name: error.name,
    message: error.message,
    ...(error.stack === undefined ? {} : { stack: error.stack }),
    ...(typeof fields.code === "string" ? { code: fields.code } : {}),
    ...(typeof fields.errno === "number" ? { errno: fields.errno } : {}),
    ...(typeof fields.syscall === "string" ? { syscall: fields.syscall } : {}),
    ...(typeof fields.path === "string" ? { path: fields.path } : {}),
  };
};

/**
 * The stage as main opens it. Only its name crosses to the worker, whose bundle holds the
 * implementation (claudeAgentSdkStage.ts), so main never loads that code.
 */
export const claudeAgentSdkStageOnMain: ProviderIoStage<
  ClaudeAgentSdkStageOptions,
  SDKMessage,
  ClaudeAgentSdkWorkerRequest,
  unknown,
  ClaudeAgentSdkWorkerEvent
> = {
  name: CLAUDE_AGENT_SDK_STAGE,
  open: () => {
    throw new Error("The Claude Agent SDK stage runs only on a worker.");
  },
};

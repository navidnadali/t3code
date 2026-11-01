import type {
  Options,
  PermissionMode,
  Query,
  SDKMessage,
  SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";

import type { ProviderIoStage } from "./providerIoProtocol.ts";

export const CLAUDE_AGENT_SDK_STAGE = "claude-agent-sdk.query";

/** Callbacks main passes to the SDK; until main answers them (P1-028), the worker refuses them. */
export type ClaudeAgentSdkWorkerCallback = "canUseTool" | "onElicitation" | "onUserDialog";

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
  readonly refuse: ReadonlyArray<ClaudeAgentSdkWorkerCallback>;
  /** The context of T3's SubagentStart hook, which returns it unchanged. */
  readonly subagentStartContext?: string;
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
  | { readonly type: "error"; readonly error: ClaudeAgentSdkProcessError };

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
  | { readonly type: "refused"; readonly callback: ClaudeAgentSdkWorkerCallback };

export const REFUSED_PERMISSION_MESSAGE =
  "T3 Code cannot answer permission requests while Claude runs in a provider I/O worker.";

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

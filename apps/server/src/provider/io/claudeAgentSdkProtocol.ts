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
import { MAX_OFFLOADED_RESULT_DEPTH, nestsDeeperThan } from "./resultDepth.ts";

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
  | { readonly type: "initializationResult" }
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
  | { readonly type: "mcpServerStatus" }
  | { readonly type: "readMcpResource"; readonly server: string; readonly uri: string }
  | { readonly type: "controlRequest"; readonly payload: Record<string, unknown> }
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
  // The SDK's error field for a callback that threw as JSON (`{"error":…}`).
  | { readonly type: "callbackError"; readonly id: number; readonly json: string };

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
      /** The arguments that cross as their JSON text, by index (see toWorkerCallback). */
      readonly jsonArgs?: ReadonlyArray<number>;
    }
  | { readonly type: "callbackAbort"; readonly id: number };

// Main cannot read back a structured clone nested past about 2,400 objects (resultDepth.ts), so a value
// the SDK hands main that nests deeper than the offload limit crosses as its JSON text. The SDK's
// values are the JSON the CLI wrote, which reads back the same.
const nestsTooDeep = (value: unknown) => nestsDeeperThan(value, MAX_OFFLOADED_RESULT_DEPTH);

/** An SDK message as it crosses to main, or the JSON text of one nested too deep. */
export type ClaudeAgentSdkWorkerOutput = SDKMessage | string;

export const toWorkerOutput = (message: SDKMessage): ClaudeAgentSdkWorkerOutput =>
  nestsTooDeep(message) ? JSON.stringify(message) : message;

export const fromWorkerOutput = (output: ClaudeAgentSdkWorkerOutput): SDKMessage =>
  typeof output === "string" ? (JSON.parse(output) as SDKMessage) : output;

/** What a call answered as it crosses to main, or the JSON text of an answer nested too deep. */
export type ClaudeAgentSdkWorkerReply = { readonly value: unknown } | { readonly json: string };

export const toWorkerReply = (value: unknown): ClaudeAgentSdkWorkerReply =>
  nestsTooDeep(value) ? { json: JSON.stringify(value) } : { value };

export const fromWorkerReply = (reply: ClaudeAgentSdkWorkerReply): unknown =>
  "json" in reply ? JSON.parse(reply.json) : reply.value;

/** A callback as it crosses to main: each argument nested too deep goes as its JSON text. */
export const toWorkerCallback = (
  call: ClaudeAgentSdkCallbackCall,
): { readonly call: ClaudeAgentSdkCallbackCall; readonly jsonArgs?: ReadonlyArray<number> } => {
  const jsonArgs = call.args.flatMap((arg, index) => (nestsTooDeep(arg) ? [index] : []));
  if (jsonArgs.length === 0) return { call };
  const args = call.args.map((arg, index) =>
    jsonArgs.includes(index) ? JSON.stringify(arg) : arg,
  );
  return { call: { ...call, args } as unknown as ClaudeAgentSdkCallbackCall, jsonArgs };
};

export const fromWorkerCallback = (
  call: ClaudeAgentSdkCallbackCall,
  jsonArgs: ReadonlyArray<number> | undefined,
): ClaudeAgentSdkCallbackCall =>
  jsonArgs === undefined
    ? call
    : ({
        ...call,
        args: call.args.map((arg, index) =>
          jsonArgs.includes(index) ? JSON.parse(arg as string) : arg,
        ),
      } as unknown as ClaudeAgentSdkCallbackCall);

/** An Error the worker's SDK raised, with the own fields a structured clone of it drops. */
export interface ClaudeAgentSdkWorkerError {
  readonly sdkError: Error;
  readonly fields: Readonly<Record<string, string | number | boolean>>;
}

const isWorkerError = (error: unknown): error is ClaudeAgentSdkWorkerError =>
  typeof error === "object" &&
  error !== null &&
  Object.hasOwn(error, "sdkError") &&
  (error as { readonly sdkError: unknown }).sdkError instanceof Error;

// A structured clone keeps only an Error's name, message, stack and cause, so an Error with fields of
// its own (the SDK's errorClass, telemetryMessage, exitCode, signal) crosses with them beside it.
export const toWorkerError = (error: unknown): unknown => {
  if (!(error instanceof Error)) return error;
  try {
    const fields = Object.fromEntries(
      Object.entries(error).filter(([, value]) =>
        ["string", "number", "boolean"].includes(typeof value),
      ),
    ) as ClaudeAgentSdkWorkerError["fields"];
    return Object.keys(fields).length === 0 ? error : { sdkError: error, fields };
  } catch {
    return error;
  }
};

export const fromWorkerError = (error: unknown): unknown =>
  isWorkerError(error) ? Object.assign(error.sdkError, error.fields) : error;

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
  ClaudeAgentSdkWorkerOutput,
  ClaudeAgentSdkWorkerRequest,
  ClaudeAgentSdkWorkerReply,
  ClaudeAgentSdkWorkerEvent
> = {
  name: CLAUDE_AGENT_SDK_STAGE,
  open: () => {
    throw new Error("The Claude Agent SDK stage runs only on a worker.");
  },
};

// @effect-diagnostics nodeBuiltinImport:off - The SDK takes Node streams for the CLI it drives.
import * as NodeEvents from "node:events";
import * as NodeStream from "node:stream";

import type {
  Options,
  Query,
  SDKMessage,
  SDKUserMessage,
  SpawnedProcess,
  SpawnOptions,
} from "@anthropic-ai/claude-agent-sdk";

import {
  CLAUDE_AGENT_SDK_STAGE,
  type ClaudeAgentSdkProcessError,
  type ClaudeAgentSdkStageOptions,
  type ClaudeAgentSdkWorkerCallback,
  type ClaudeAgentSdkWorkerEvent,
  type ClaudeAgentSdkWorkerRequest,
  REFUSED_PERMISSION_MESSAGE,
} from "./claudeAgentSdkProtocol.ts";
import type { ProviderIoStage, ProviderIoStageHost } from "./providerIoProtocol.ts";

const fromProcessError = (error: ClaudeAgentSdkProcessError) => {
  const { name, message, stack, ...fields } = error;
  const restored = Object.assign(new Error(message), fields);
  restored.name = name;
  if (stack !== undefined) restored.stack = stack;
  return restored;
};

type Signal = (event: ClaudeAgentSdkWorkerEvent) => void;

// The SDK's view of the CLI that main spawned: stdout is fed from main, stdin and kills go to main.
class FacadeProcess extends NodeEvents.EventEmitter implements SpawnedProcess {
  readonly stdin: NodeStream.Writable;
  readonly stdout = new NodeStream.Readable({ read: () => {} });
  killed = false;
  exitCode: number | null = null;
  signalCode: NodeJS.Signals | null = null;
  readonly #signal: Signal;

  constructor(signal: Signal) {
    super();
    this.#signal = signal;
    this.stdin = new NodeStream.Writable({
      decodeStrings: false,
      write: (chunk: string | Uint8Array, _encoding, callback) => {
        signal({ type: "stdin", data: chunk });
        callback();
      },
      final: (callback) => {
        signal({ type: "stdinEnd" });
        callback();
      },
    });
  }

  kill(signal: NodeJS.Signals) {
    if (this.exitCode !== null || this.signalCode !== null) return false;
    this.killed = true;
    this.#signal({ type: "kill", signal });
    return true;
  }

  exited(code: number | null, signal: NodeJS.Signals | null) {
    this.exitCode = code;
    this.signalCode = signal;
    this.emit("exit", code, signal);
  }

  // An unhandled "error" would end the worker and every session on it.
  failed(error: Error) {
    if (this.listenerCount("error") > 0) this.emit("error", error);
  }
}

interface PromptQueue {
  readonly push: (message: SDKUserMessage) => void;
  readonly end: () => void;
  readonly iterable: AsyncIterable<SDKUserMessage>;
}

// Like the in-thread prompt queue: ending it drops what the SDK has not taken.
const makePromptQueue = (): PromptQueue => {
  const buffered: Array<SDKUserMessage> = [];
  const waiting: Array<(result: IteratorResult<SDKUserMessage, undefined>) => void> = [];
  let ended = false;
  const end = () => {
    ended = true;
    buffered.length = 0;
    for (const resolve of waiting.splice(0)) resolve({ done: true, value: undefined });
  };
  const iterator: AsyncIterator<SDKUserMessage, undefined> = {
    next: () => {
      const message = buffered.shift();
      if (message !== undefined) return Promise.resolve({ done: false, value: message });
      if (ended) return Promise.resolve({ done: true, value: undefined });
      return new Promise((resolve) => waiting.push(resolve));
    },
    return: () => {
      end();
      return Promise.resolve({ done: true, value: undefined });
    },
  };
  return {
    push: (message) => {
      if (ended) return;
      const resolve = waiting.shift();
      if (resolve === undefined) buffered.push(message);
      else resolve({ done: false, value: message });
    },
    end,
    iterable: { [Symbol.asyncIterator]: () => iterator },
  };
};

const loadClaudeAgentSdk = () => import("@anthropic-ai/claude-agent-sdk");

const refusingCallbacks = (
  refuse: ReadonlyArray<ClaudeAgentSdkWorkerCallback>,
  signal: Signal,
): Pick<Options, ClaudeAgentSdkWorkerCallback> => {
  const refused = (callback: ClaudeAgentSdkWorkerCallback) => signal({ type: "refused", callback });
  return {
    ...(refuse.includes("canUseTool")
      ? {
          canUseTool: async () => {
            refused("canUseTool");
            return { behavior: "deny" as const, message: REFUSED_PERMISSION_MESSAGE };
          },
        }
      : {}),
    ...(refuse.includes("onElicitation")
      ? {
          onElicitation: async () => {
            refused("onElicitation");
            return { action: "decline" as const };
          },
        }
      : {}),
    ...(refuse.includes("onUserDialog")
      ? {
          onUserDialog: async () => {
            refused("onUserDialog");
            return { behavior: "cancelled" as const };
          },
        }
      : {}),
  };
};

const subagentStartHooks = (context: string): Pick<Options, "hooks"> => ({
  hooks: {
    SubagentStart: [
      {
        hooks: [
          async () => ({
            hookSpecificOutput: { hookEventName: "SubagentStart", additionalContext: context },
          }),
        ],
      },
    ],
  },
});

/** Runs the SDK's `query()` over a CLI that main spawns and owns; SDK messages are its outputs. */
export const claudeAgentSdkStage: ProviderIoStage<
  ClaudeAgentSdkStageOptions,
  SDKMessage,
  ClaudeAgentSdkWorkerRequest,
  unknown,
  ClaudeAgentSdkWorkerEvent
> = {
  name: CLAUDE_AGENT_SDK_STAGE,
  open: (stageOptions, host?: ProviderIoStageHost<SDKMessage, ClaudeAgentSdkWorkerEvent>) => {
    if (host === undefined) throw new Error("The Claude Agent SDK stage runs only on a worker.");
    const prompts = makePromptQueue();
    let child: FacadeProcess | undefined;
    let query: Query | undefined;
    // Main sends the close after every byte it read before closing; what comes later is unread.
    let closed = false;

    const spawnClaudeCodeProcess = ({ command, args, cwd, env, signal }: SpawnOptions) => {
      const spawned = new FacadeProcess(host.signal);
      child = spawned;
      host.signal({ type: "spawn", command, args, cwd, env });
      // Main spawns with its own signal and aborts it when this one aborts.
      if (signal.aborted) host.signal({ type: "abort" });
      else signal.addEventListener("abort", () => host.signal({ type: "abort" }), { once: true });
      return spawned;
    };

    const readMessages = async (running: Query) => {
      try {
        while (true) {
          const next = await running.next();
          if (next.done === true) break;
          host.emit(next.value);
        }
      } catch (error) {
        host.finish(error);
        return;
      }
      host.finish();
    };

    const start = async () => {
      const sdk = await loadClaudeAgentSdk();
      query = sdk.query({
        prompt: prompts.iterable,
        options: {
          ...stageOptions.options,
          ...refusingCallbacks(stageOptions.refuse, host.signal),
          ...(stageOptions.subagentStartContext === undefined
            ? {}
            : subagentStartHooks(stageOptions.subagentStartContext)),
          spawnClaudeCodeProcess,
        },
      });
      void readMessages(query);
    };

    const started = () => {
      if (query === undefined) throw new Error("The Claude query has not started.");
      return query;
    };

    const call = (request: ClaudeAgentSdkWorkerRequest): unknown => {
      switch (request.type) {
        case "start":
          return start();
        case "prompt":
          prompts.push(request.message);
          return undefined;
        case "promptEnd":
          prompts.end();
          return undefined;
        case "setMcpServers":
          return started().setMcpServers(request.servers);
        case "setModel":
          return started().setModel(request.model);
        case "applyFlagSettings":
          return started().applyFlagSettings(request.settings);
        case "setPermissionMode":
          return started().setPermissionMode(request.mode);
        case "stopTask":
          return started().stopTask(request.taskId);
        case "backgroundTasks":
          return started().backgroundTasks(request.toolUseId);
        case "interrupt":
          return started().interrupt();
        case "close":
          closed = true;
          query?.close();
          return undefined;
        case "asyncDispose":
          return query?.[Symbol.asyncDispose]();
        case "return":
          return query?.return(undefined);
        case "stdoutEnd":
          child?.stdout.push(null);
          return undefined;
        case "stdoutError":
          child?.stdout.destroy(fromProcessError(request.error));
          return undefined;
        case "stdinError":
          child?.stdin.destroy(fromProcessError(request.error));
          return undefined;
        case "exit":
          child?.exited(request.code, request.signal);
          return undefined;
        case "error":
          child?.failed(fromProcessError(request.error));
          return undefined;
      }
    };

    return {
      push: (chunk) => {
        if (!closed) child?.stdout.push(chunk);
      },
      end: () => {
        child?.stdout.push(null);
      },
      call,
      close: () => {
        prompts.end();
        query?.close();
      },
    };
  },
};

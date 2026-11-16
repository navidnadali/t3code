const CLAUDE_CAN_USE_TOOL_SHADOWED_WARNING = "CLAUDE_SDK_CAN_USE_TOOL_SHADOWED";

// query() warns when canUseTool meets bypassPermissions or bare allowedTools. T3 installs
// canUseTool in every mode on purpose (AskUserQuestion, ask rules), so it is per-session noise.
export function withoutClaudeCanUseToolShadowedWarning<A>(run: () => A): A {
  const emitWarning = process.emitWarning;
  process.emitWarning = function (this: NodeJS.Process, ...args: Parameters<typeof emitWarning>) {
    const [, options] = args;
    if (typeof options === "object" && options.code === CLAUDE_CAN_USE_TOOL_SHADOWED_WARNING) {
      return;
    }
    return Reflect.apply(emitWarning, this, args);
  } as typeof emitWarning;
  try {
    return run();
  } finally {
    process.emitWarning = emitWarning;
  }
}

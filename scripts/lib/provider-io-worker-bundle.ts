// @effect-diagnostics nodeBuiltinImport:off - Vite's build plugin runs before an Effect runtime exists.
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import { Rolldown } from "vite-plus/pack";

// The build sees only the imports Rolldown resolved; computed names (`globalThis[key]`) and code
// built from strings are invisible to it. So the worker resolves its few built-ins first and then
// registers a loader hook that refuses every later `require`, `import()` and `createRequire`.
// Process-level loaders are disabled too, and worker_threads is visible only as its own port.
const guard = (code: string, builtins: ReadonlyArray<string>) => `"use strict";
{
  const NodeModule = require("node:module");
  const threads = require("node:worker_threads");
  const port = Object.freeze({
    parentPort: threads.parentPort,
    workerData: threads.workerData,
    threadId: threads.threadId,
    isMainThread: threads.isMainThread,
  });
  const allowed = new Map(
    ${JSON.stringify(builtins)}.map((id) => [id, /^(node:)?worker_threads$/.test(id) ? port : require(id)]),
  );
  const refuse = (what) => {
    throw new Error("The provider I/O worker may load only the built-ins it was built with, not " + what + ".");
  };
  const load = (id) => (typeof id === "string" && allowed.has(id) ? allowed.get(id) : refuse(String(id)));
  NodeModule.registerHooks({ resolve: (specifier) => refuse(String(specifier)) });
  for (const name of ["dlopen", "binding", "_linkedBinding"]) process[name] = () => refuse("process." + name);
  process.getBuiltinModule = load;
  ((require, module, exports, __filename, __dirname) => {
${code}
  })(load, { exports: {} }, {}, "", "");
}
`;

// An `eval` worker resolves any other module from the server's working directory.
const bundleProviderIoWorker = async (entry: string) => {
  const unresolved: Array<string> = [];
  const { output } = await Rolldown.build({
    input: entry,
    platform: "node",
    write: false,
    output: { format: "cjs", codeSplitting: false },
    onLog: (level, log, handler) => {
      if (log.code === "UNRESOLVED_IMPORT") unresolved.push(log.message);
      else handler(level, log);
    },
  });
  // String contents are never scanned: bundled code may carry loader text as data (ajv).
  const chunk = output[0];
  const imports = [...new Set([...chunk.imports, ...chunk.dynamicImports])];
  const problems = [
    ...unresolved,
    ...imports
      .filter((specifier) => !NodeModule.isBuiltin(specifier))
      .map((specifier) => `an external import of "${specifier}"`),
  ];
  if (problems.length > 0) {
    throw new Error(
      `The provider I/O worker bundle may load only Node built-ins:\n${problems.join("\n")}`,
    );
  }
  return guard(chunk.code, imports.sort());
};

const realpathOrSelf = (path: string) => {
  try {
    return NodeFS.realpathSync(path);
  } catch {
    return NodePath.resolve(path);
  }
};

/**
 * Replaces the `placeholder` module with `loadEmbeddedProviderIoWorkerSource`, returning `entry`
 * bundled as one CommonJS string for a worker run with `eval`: executables resolve only
 * built-ins and have no module files. The function keeps the string out of memory until used.
 */
export const providerIoWorkerBundle = (options: {
  readonly entry: string;
  readonly placeholder: string;
}) => {
  const placeholder = realpathOrSelf(options.placeholder);
  const placeholderName = NodePath.basename(placeholder);
  let embedded = false;
  return {
    name: "t3code:provider-io-worker",
    buildStart() {
      embedded = false;
    },
    async load(id: string) {
      if (!id.endsWith(placeholderName) || realpathOrSelf(id) !== placeholder) return null;
      const code = await bundleProviderIoWorker(options.entry);
      embedded = true;
      return `export const loadEmbeddedProviderIoWorkerSource = () => ${JSON.stringify(code)};`;
    },
    // A placeholder path that never matches would otherwise ship a server with no worker.
    buildEnd(error?: Error) {
      if (error === undefined && !embedded) {
        throw new Error(
          `The provider I/O worker placeholder ${options.placeholder} was never loaded, so its bundle is missing.`,
        );
      }
    },
  };
};

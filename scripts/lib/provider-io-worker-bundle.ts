// @effect-diagnostics nodeBuiltinImport:off - Vite's build plugin runs before an Effect runtime exists.
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import { Rolldown } from "vite-plus/pack";

// The build sees only what Rolldown resolved, so the worker guards its loaders itself: any
// built-in, lazily and however prefixed (`node:module` and `worker_threads` only as facades),
// loader hooks that resolve and load nothing else, and the eval wrapper's globals deleted.
const guard = (code: string) => `"use strict";
{
  const builtin = require;
  const NodeModule = builtin("node:module");
  const threads = builtin("node:worker_threads");
  const refuse = (what) => {
    throw new Error("The provider I/O worker may load only Node built-ins, not " + what + ".");
  };
  const prefixed = (id) => (id.startsWith("node:") ? id : "node:" + id);
  let load;
  const facades = new Map([
    [
      "node:module",
      Object.freeze({
        builtinModules: NodeModule.builtinModules,
        isBuiltin: NodeModule.isBuiltin,
        createRequire: () => load,
      }),
    ],
    [
      "node:worker_threads",
      Object.freeze({
        parentPort: threads.parentPort,
        workerData: threads.workerData,
        threadId: threads.threadId,
        isMainThread: threads.isMainThread,
      }),
    ],
  ]);
  load = (id) => {
    if (typeof id !== "string" || !NodeModule.isBuiltin(id)) return refuse(String(id));
    return facades.get(prefixed(id)) ?? builtin(prefixed(id));
  };
  // The load hook also stops a recovered Module class (a REPL context has one) loading a file.
  NodeModule.registerHooks({
    resolve: (specifier, context, nextResolve) =>
      typeof specifier === "string" && NodeModule.isBuiltin(specifier) && !facades.has(prefixed(specifier))
        ? nextResolve(specifier, context)
        : refuse(String(specifier)),
    load: (url, context, nextLoad) =>
      typeof url === "string" && url.startsWith("node:") ? nextLoad(url, context) : refuse(String(url)),
  });
  for (const name of ["module", "require", "exports", "__filename", "__dirname"]) delete globalThis[name];
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
  return guard(chunk.code);
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

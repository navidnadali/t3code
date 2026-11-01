// @effect-diagnostics nodeBuiltinImport:off - Vite's build plugin runs before an Effect runtime exists.
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { Rolldown } from "vite-plus/pack";

// Threat model: bundled code must not resolve or load a module from the server's directory (nor
// reach the main thread or native code); it is not a sandbox for hostile code. The build sees only
// what Rolldown resolved, so the worker guards itself: built-ins only, through facades and loader
// hooks, the eval wrapper's globals deleted, and process-level escapes refused.
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
  // A REPL context holds the real Module class, the inspector reaches the main thread, and ffi
  // and sqlite (loadExtension) load native code.
  const refused = new Set([
    "node:repl",
    "node:inspector",
    "node:inspector/promises",
    "node:ffi",
    "node:sqlite",
  ]);
  const allowed = (id) =>
    typeof id === "string" && NodeModule.isBuiltin(id) && !refused.has(prefixed(id));
  load = (id) => {
    if (!allowed(id)) return refuse(String(id));
    return facades.get(prefixed(id)) ?? builtin(prefixed(id));
  };
  // The load hook also stops a recovered Module class (a REPL context has one) loading a file.
  NodeModule.registerHooks({
    resolve: (specifier, context, nextResolve) =>
      allowed(specifier) && !facades.has(prefixed(specifier))
        ? nextResolve(specifier, context)
        : refuse(String(specifier)),
    load: (url, context, nextLoad) =>
      typeof url === "string" && url.startsWith("node:") ? nextLoad(url, context) : refuse(String(url)),
  });
  for (const name of ["module", "require", "exports", "__filename", "__dirname"]) delete globalThis[name];
  for (const name of ["dlopen", "binding", "_linkedBinding", "execve"]) {
    process[name] = () => refuse("process." + name);
  }
  process.getBuiltinModule = load;
  // A signal to this process or its group (SIGUSR1) would start main's inspector.
  const kill = process.kill.bind(process);
  process.kill = (pid, signal) =>
    [process.pid, 0, -process.pid].includes(Number(pid))
      ? refuse("a signal to its own process")
      : kill(pid, signal);
  // The bundle's text comes from a function that never runs, so it is not escaped into a string.
  const text = function bundle() {
${code}
  }.toString();
  // Compiled in the global scope, so not even a direct eval in the bundle reaches these bindings.
  const run = new Function(
    "exports",
    "require",
    "module",
    "__filename",
    "__dirname",
    '"use strict";' + text.slice(text.indexOf("{") + 1, text.lastIndexOf("}")) +
      "\\n//# sourceURL=t3-provider-io-worker.js",
  );
  const exported = { exports: {} };
  run.call(exported.exports, exported.exports, load, exported, "", "");
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
  // A dynamic import Rolldown inlined (the lazily loaded Claude Agent SDK) names this chunk.
  const dynamicImports = chunk.dynamicImports.filter((specifier) => specifier !== chunk.fileName);
  const imports = [...new Set([...chunk.imports, ...dynamicImports])];
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
 * built-ins and have no module files. The function inflates the string only when a worker starts.
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
      // Compressed, a 2 MB bundle that is partly two-byte text costs a server with the pool off a
      // short ASCII string.
      const compressed = NodeZlib.brotliCompressSync(code, {
        params: { [NodeZlib.constants.BROTLI_PARAM_QUALITY]: 11 },
      }).toString("base64");
      // zlib loads only when a worker starts. The base64 text is read from the source of a function
      // that never runs, so main never compiles it into a heap string (as the guard reads the bundle).
      return [
        `function packedBundle() { return ${JSON.stringify(compressed)}; }`,
        "export const loadEmbeddedProviderIoWorkerSource = () => {",
        "  const text = packedBundle.toString();",
        "  const start = text.search(/[\"'`]/) + 1;",
        "  const packed = text.slice(start, text.indexOf(text[start - 1], start));",
        '  return process.getBuiltinModule("node:zlib").brotliDecompressSync(Buffer.from(packed, "base64")).toString("utf8");',
        "};",
      ].join("\n");
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

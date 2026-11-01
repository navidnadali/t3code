// @effect-diagnostics nodeBuiltinImport:off - Vite's build plugin runs before an Effect runtime exists.
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import ts from "typescript-legacy";
import { Rolldown } from "vite-plus/pack";

// Names that load code at run time (`require`, `__require`, `createRequire`, `Module._load`,
// `process.dlopen`). Only a direct call on a literal built-in may use one; any other reference
// (`module["require"]`, `require.call`, an alias) could load from the working directory.
const isLoaderName = (name: string) =>
  /require$/i.test(name) || name === "_load" || name === "dlopen";

const findLoads = (code: string) => {
  const found = new Set<string>();
  const builtins = new Set<string>();
  const allowed = new Set<ts.Node>();
  const source = ts.createSourceFile(
    "worker.cjs",
    code,
    ts.ScriptTarget.Latest,
    false,
    ts.ScriptKind.JS,
  );
  const checkLoad = (kind: string, call: ts.CallExpression) => {
    const [argument] = call.arguments;
    if (argument === undefined || !ts.isStringLiteralLike(argument)) {
      found.add(`${kind} of a computed name`);
    } else if (!NodeModule.isBuiltin(argument.text)) {
      found.add(`${kind} of "${argument.text}"`);
    } else {
      builtins.add(argument.text);
    }
  };
  const visit = (node: ts.Node): void => {
    if (ts.isCallExpression(node)) {
      const callee = node.expression;
      if (callee.kind === ts.SyntaxKind.ImportKeyword) checkLoad("import()", node);
      const name = ts.isIdentifier(callee)
        ? callee
        : ts.isPropertyAccessExpression(callee)
          ? callee.name
          : undefined;
      if (name !== undefined && isLoaderName(name.text)) {
        allowed.add(name);
        checkLoad(`${name.text}()`, node);
      }
      if (ts.isIdentifier(callee) && (callee.text === "eval" || callee.text === "Function")) {
        found.add(`${callee.text}() of run-time code`);
      }
    } else if (ts.isNewExpression(node)) {
      if (ts.isIdentifier(node.expression) && node.expression.text === "Function") {
        found.add("new Function() of run-time code");
      }
    } else if (ts.isElementAccessExpression(node)) {
      const key = node.argumentExpression;
      if (ts.isStringLiteralLike(key) && isLoaderName(key.text)) {
        found.add(`a computed reference to ${key.text}`);
      }
    } else if (ts.isIdentifier(node) && isLoaderName(node.text) && !allowed.has(node)) {
      found.add(`a reference to ${node.text} other than a call`);
    }
    ts.forEachChild(node, visit);
  };
  visit(source);
  return { problems: [...found].sort(), builtins: [...builtins].sort() };
};

// Static checks cannot see computed names (`module[key]`), so the worker also runs with a
// `require`, `module` and `process` that reach only the built-ins found above, and
// worker_threads only as its own port: nothing can load a file from the working directory.
const guard = (code: string, builtins: ReadonlyArray<string>) => `"use strict";
{
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
  const chunk = output[0];
  const loads = findLoads(chunk.code);
  const problems = [
    ...unresolved,
    ...[...chunk.imports, ...chunk.dynamicImports]
      .filter((specifier) => !NodeModule.isBuiltin(specifier))
      .map((specifier) => `an external import of "${specifier}"`),
    ...loads.problems,
  ];
  if (problems.length > 0) {
    throw new Error(
      `The provider I/O worker bundle may load only Node built-ins:\n${problems.join("\n")}`,
    );
  }
  return guard(chunk.code, loads.builtins);
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

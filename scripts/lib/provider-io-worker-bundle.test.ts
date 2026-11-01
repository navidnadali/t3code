import * as NodeURL from "node:url";
import * as NodeWorkerThreads from "node:worker_threads";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { Rolldown } from "vite-plus/pack";

import { providerIoWorkerBundle } from "./provider-io-worker-bundle.ts";

const placeholderSource = "export const placeholder = (): string | undefined => undefined;\n";

// Builds a host that imports the placeholder, with the plugin bundling `worker` into it.
const buildHost = (worker: string, options: { readonly pluginPlaceholder?: string } = {}) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-provider-io-bundle-" });
    yield* fs.writeFileString(path.join(dir, "worker.ts"), worker);
    yield* fs.writeFileString(path.join(dir, "placeholder.ts"), placeholderSource);
    yield* fs.writeFileString(
      path.join(dir, "host.ts"),
      'import * as Placeholder from "./placeholder.ts";\nconsole.log(Placeholder);\n',
    );
    const plugin = providerIoWorkerBundle({
      entry: path.join(dir, "worker.ts"),
      placeholder: options.pluginPlaceholder ?? path.join(dir, "placeholder.ts"),
    });
    return yield* Effect.tryPromise(() =>
      Rolldown.build({
        input: path.join(dir, "host.ts"),
        platform: "node",
        write: false,
        logLevel: "silent",
        plugins: [plugin],
      }),
    ).pipe(Effect.map(({ output }) => output[0].code));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

const rejection = (worker: string) =>
  buildHost(worker).pipe(
    Effect.flip,
    Effect.map((error) => String(error.cause)),
  );

it.effect("embeds a worker that loads only built-ins", () =>
  Effect.gen(function* () {
    const code = yield* buildHost(
      'import * as NodeWorkerThreads from "node:worker_threads";\n' +
        'NodeWorkerThreads.parentPort?.postMessage("worker-marker");\n',
    );
    assert.include(code, "worker-marker");
  }),
);

it.effect("rejects a worker whose dynamic import does not resolve", () =>
  Effect.gen(function* () {
    assert.include(
      yield* rejection('void import("p1-024-not-a-package");\n'),
      "p1-024-not-a-package",
    );
  }),
);

it.effect("rejects a worker whose static import does not resolve", () =>
  Effect.gen(function* () {
    assert.include(
      yield* rejection('import * as Missing from "p1-024-not-a-package";\nconsole.log(Missing);\n'),
      "p1-024-not-a-package",
    );
  }),
);

// ajv (in the Claude Agent SDK) carries loader text as data and compiles validators at run time.
it.effect("builds a worker whose code holds loader text as data and compiles code", () =>
  Effect.gen(function* () {
    const code = yield* buildHost(
      'import * as NodeWorkerThreads from "node:worker_threads";\n' +
        'const generated = "require(\\"ajv/dist/runtime/uri\\").default";\n' +
        'const compile = new Function("x", "return x");\n' +
        "NodeWorkerThreads.parentPort?.postMessage([generated, compile(1)]);\n",
    );
    assert.include(code, "ajv/dist/runtime/uri");
  }),
);

it.effect("fails the build when the plugin never replaces its placeholder", () =>
  Effect.gen(function* () {
    const error = yield* buildHost('console.log("worker");\n', {
      pluginPlaceholder: "/nonexistent/placeholder.ts",
    }).pipe(Effect.flip);
    assert.include(String(error.cause), "placeholder");
  }),
);

// Builds the worker through the plugin and returns the embedded source a server would run.
const embeddedSource = (worker: string, files: Record<string, string> = {}) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-provider-io-embedded-" });
    yield* fs.writeFileString(path.join(dir, "worker.ts"), worker);
    for (const [file, content] of Object.entries(files)) {
      yield* fs.writeFileString(path.join(dir, file), content);
    }
    yield* fs.writeFileString(path.join(dir, "placeholder.ts"), placeholderSource);
    yield* fs.writeFileString(
      path.join(dir, "host.ts"),
      'import * as Placeholder from "./placeholder.ts";\n' +
        "export const source = (Placeholder as unknown as { loadEmbeddedProviderIoWorkerSource: () => string }).loadEmbeddedProviderIoWorkerSource();\n",
    );
    const { output } = yield* Effect.promise(() =>
      Rolldown.build({
        input: path.join(dir, "host.ts"),
        platform: "node",
        write: false,
        logLevel: "silent",
        plugins: [
          providerIoWorkerBundle({
            entry: path.join(dir, "worker.ts"),
            placeholder: path.join(dir, "placeholder.ts"),
          }),
        ],
      }),
    );
    yield* fs.writeFileString(path.join(dir, "host.mjs"), output[0].code);
    const host = yield* Effect.promise(
      () =>
        import(NodeURL.pathToFileURL(path.join(dir, "host.mjs")).href) as Promise<{
          readonly source: string;
        }>,
    );
    return host.source;
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

// The worker's first message, or its startup error.
const runEmbedded = (source: string, workerData?: unknown) =>
  Effect.callback<unknown>((resume) => {
    const worker = new NodeWorkerThreads.Worker(source, { eval: true, workerData });
    worker.once("message", (message) => {
      void worker.terminate();
      resume(Effect.succeed(message));
    });
    worker.once("error", (error) => resume(Effect.succeed(`worker error: ${String(error)}`)));
  });

it.effect("refuses at run time a load the build could not see", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-provider-io-planted-" });
    yield* fs.writeFileString(path.join(dir, "planted.cjs"), 'module.exports = "PLANTED";\n');
    yield* fs.writeFileString(path.join(dir, "planted.mjs"), 'export default "PLANTED";\n');
    yield* fs.makeDirectory(path.join(dir, "node_modules", "planted"), { recursive: true });
    yield* fs.writeFileString(
      path.join(dir, "node_modules", "planted", "index.js"),
      'module.exports = "PLANTED-BARE";\n',
    );
    // Every name is computed at run time, so only the worker's own guard can refuse these.
    const source = yield* embeddedSource(
      'import * as NodeModule from "node:module";\n' +
        'import * as NodeFS from "node:fs";\n' +
        'import * as NodeStream from "node:stream";\n' +
        'import * as NodeURL from "node:url";\n' +
        'import * as NodeWorkerThreads from "node:worker_threads";\n' +
        "const dir = NodeWorkerThreads.workerData as string;\n" +
        'const name = (...parts: Array<string>) => parts.join("");\n' +
        "const global = globalThis as any;\n" +
        "const code = (function () {}).constructor as any;\n" +
        'const planted = dir + "/planted.cjs";\n' +
        "const viaModule = (Module: any, file: string) => {\n" +
        "  const loaded = new Module(file);\n" +
        "  loaded.load(file);\n" +
        "  return loaded.exports;\n" +
        "};\n" +
        'const globalModule = () => global[name("mod", "ule")].constructor;\n' +
        "const attempts = [\n" +
        '  () => require(name(dir, "/planted.cjs")),\n' +
        "  () => (require as any).call(null, planted),\n" +
        '  () => import(name("file://", dir, "/planted.mjs")).then((m: any) => m.default),\n' +
        '  () => (module as any)[name("re", "quire")](planted),\n' +
        '  () => global[name("re", "quire")](planted),\n' +
        '  () => global[name("re", "quire")]("node:module")[name("create", "Require")](dir + "/")("./planted.cjs"),\n' +
        '  () => (process as any)[name("getBuiltin", "Module")]("module")[name("create", "Require")](dir + "/")("./planted.cjs"),\n' +
        '  () => code("p", "return imp" + "ort(p)")("file://" + dir + "/planted.mjs").then((m: any) => m.default),\n' +
        // The real Module class: bare resolution from node_modules, any file, a second hook.
        '  () => viaModule(globalModule(), globalModule()._resolveFilename("planted", null, false, { paths: [dir] })),\n' +
        "  () => viaModule(globalModule(), planted),\n" +
        "  () => {\n" +
        "    globalModule().registerHooks({\n" +
        '      resolve: (specifier: string, context: unknown, next: any) => specifier === "p1-024-hooked"\n' +
        '        ? { url: NodeURL.pathToFileURL(dir + "/planted.mjs").href, shortCircuit: true }\n' +
        "        : next(specifier, context),\n" +
        "    });\n" +
        '    return import(name("p1-024-", "hooked")).then((m: any) => m.default);\n' +
        "  },\n" +
        '  () => viaModule(code("return mod" + "ule")().constructor, planted),\n' +
        // The node:module the bundle imports exposes no loader.
        '  () => NodeModule.createRequire(dir + "/")("./planted.cjs"),\n' +
        "  () => viaModule((NodeModule as any).Module, planted),\n" +
        "  () => (NodeModule as any).registerHooks({ resolve: () => undefined }),\n" +
        '  () => import(name("node:", "module")).then((m: any) => viaModule(m.Module, planted)),\n' +
        // Built-ins that hand out the real Module class or reach the main thread are refused.
        '  () => import(name("node:", "repl")).then((NodeRepl: any) => {\n' +
        "    const session = NodeRepl.start({\n" +
        "      input: new NodeStream.PassThrough(),\n" +
        "      output: new NodeStream.PassThrough(),\n" +
        "    });\n" +
        "    const Module = session.context.module.constructor;\n" +
        "    session.close();\n" +
        "    Module.registerHooks({\n" +
        "      resolve: (specifier: string, context: unknown, next: any) =>\n" +
        '        specifier === "p1-024-repl"\n' +
        '          ? { url: NodeURL.pathToFileURL(planted).href, format: "commonjs", shortCircuit: true }\n' +
        "          : next(specifier, context),\n" +
        "      load: (url: string, context: unknown, next: any) =>\n" +
        '        url.endsWith("/planted.cjs")\n' +
        '          ? { format: "commonjs", source: NodeFS.readFileSync(planted, "utf8"), shortCircuit: true }\n' +
        "          : next(url, context),\n" +
        "    });\n" +
        '    return import(name("p1-024-", "repl")).then((m: any) => m.default);\n' +
        "  }),\n" +
        '  () => (process as any).getBuiltinModule(name("re", "pl")).start,\n' +
        '  () => import(name("node:", "inspector")).then((m: any) => m.Session),\n' +
        '  () => (process as any).getBuiltinModule(name("inspector", "/promises")).Session,\n' +
        // A direct eval sees the scope the bundle is compiled in: the guard's own bindings must
        // not be part of it.
        '  () => eval(name("Node", "Module")).registerHooks,\n' +
        '  () => eval(name("buil", "tin"))(name("node:", "module")).Module,\n' +
        '  () => eval(name("thr", "eads")).Worker,\n' +
        "];\n" +
        "void Promise.all(attempts.map(async (attempt) => {\n" +
        "  try {\n" +
        "    const loaded = await attempt();\n" +
        '    return typeof loaded === "string" ? loaded : `loaded ${typeof loaded}`;\n' +
        '  } catch { return "refused"; }\n' +
        "})).then((results) => NodeWorkerThreads.parentPort?.postMessage(results));\n",
    );
    const results = yield* runEmbedded(source, dir);
    assert.deepEqual(
      results,
      Array.from({ length: 23 }, () => "refused"),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("lets the worker use any built-in, however a bundled module names it", () =>
  Effect.gen(function* () {
    const source = yield* embeddedSource(
      'import * as NodeOS from "node:os";\n' +
        'import * as NodeWorkerThreads from "node:worker_threads";\n' +
        'import fromCommonJs from "./dep.cjs";\n' +
        'void import("node:fs").then((fs) =>\n' +
        "  NodeWorkerThreads.parentPort?.postMessage([fromCommonJs === NodeOS.EOL, typeof fs.readFileSync]),\n" +
        ");\n",
      { "dep.cjs": 'module.exports = require("os").EOL;\n' },
    );
    assert.deepEqual(yield* runEmbedded(source), [true, "function"]);
  }),
);

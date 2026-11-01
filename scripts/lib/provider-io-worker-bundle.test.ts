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

it.effect("rejects a worker that requires or imports a computed name", () =>
  Effect.gen(function* () {
    const name = 'const name = ["p1-024", "pkg"].join("-");\n';
    assert.include(yield* rejection(`${name}require(name);\n`), "require");
    assert.include(yield* rejection(`${name}void import(name);\n`), "import");
  }),
);

it.effect("rejects a worker that reaches a loader indirectly", () =>
  Effect.gen(function* () {
    for (const [worker, problem] of [
      ['module["require"]("./package.json");\n', "computed reference to require"],
      ['require.call(null, "./package.json");\n', "reference to require other than a call"],
      // Rolldown follows the alias and fails to resolve the module itself.
      ['const load = require;\nload("./package.json");\n', "Could not resolve './package.json'"],
    ] as const) {
      assert.include(yield* rejection(worker), problem);
    }
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
const embeddedSource = (worker: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-provider-io-embedded-" });
    yield* fs.writeFileString(path.join(dir, "worker.ts"), worker);
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

it.effect("refuses at run time a load the build could not see", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-provider-io-planted-" });
    const planted = path.join(dir, "planted.cjs");
    yield* fs.writeFileString(planted, 'module.exports = "PLANTED";\n');
    yield* fs.writeFileString(path.join(dir, "planted.mjs"), 'export default "PLANTED";\n');
    // Every name is computed at run time, so only the worker's own guard can refuse these.
    const source = yield* embeddedSource(
      'import * as NodeWorkerThreads from "node:worker_threads";\n' +
        "const dir = NodeWorkerThreads.workerData as string;\n" +
        'const name = (...parts: Array<string>) => parts.join("");\n' +
        "const global = globalThis as any;\n" +
        "const code = (function () {}).constructor as any;\n" +
        "const attempts = [\n" +
        '  () => require(name(dir, "/planted.cjs")),\n' +
        '  () => (require as any).call(null, dir + "/planted.cjs"),\n' +
        '  () => import(name("file://", dir, "/planted.mjs")).then((m: any) => m.default),\n' +
        '  () => (module as any)[name("re", "quire")](dir + "/planted.cjs"),\n' +
        '  () => global[name("re", "quire")](dir + "/planted.cjs"),\n' +
        '  () => global[name("re", "quire")]("node:module")[name("create", "Require")](dir + "/")("./planted.cjs"),\n' +
        '  () => (process as any)[name("getBuiltin", "Module")]("module")[name("create", "Require")](dir + "/")("./planted.cjs"),\n' +
        '  () => code("p", "return imp" + "ort(p)")("file://" + dir + "/planted.mjs").then((m: any) => m.default),\n' +
        "];\n" +
        "void Promise.all(attempts.map(async (attempt) => {\n" +
        '  try { return await attempt(); } catch { return "refused"; }\n' +
        "})).then((results) => NodeWorkerThreads.parentPort?.postMessage(results));\n",
    );
    const results = yield* Effect.callback<unknown>((resume) => {
      const worker = new NodeWorkerThreads.Worker(source, { eval: true, workerData: dir });
      worker.once("message", (message) => {
        void worker.terminate();
        resume(Effect.succeed(message));
      });
      worker.once("error", (error) => resume(Effect.succeed(String(error))));
    });
    assert.deepEqual(
      results,
      Array.from({ length: 8 }, () => "refused"),
    );
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

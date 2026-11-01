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

it.effect("rejects a worker that requires or imports a computed name", () =>
  Effect.gen(function* () {
    const name = 'const name = ["p1-024", "pkg"].join("-");\n';
    assert.include(yield* rejection(`${name}require(name);\n`), "require");
    assert.include(yield* rejection(`${name}void import(name);\n`), "import");
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

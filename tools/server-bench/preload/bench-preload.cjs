// CommonJS shim for the bench preload: a single executable honours --require but skips --import, so run.ts loads
// bench-preload.mjs through this file (require(esm); the module has no top-level await). A load failure goes to the
// server log and never stops the server; run.ts then records the preload as not loaded.
"use strict";
try {
  require("./bench-preload.mjs");
} catch (error) {
  process.stderr.write(
    `bench-preload.cjs: could not load bench-preload.mjs: ${String(error && error.message ? error.message : error)}\n`,
  );
}

#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - a host CLI entry: it installs the module resolve hook.
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

// The bench sits outside the workspace: resolve its packages as the server does.
const serverPackage = NodeURL.pathToFileURL(
  NodePath.join(import.meta.dirname, "..", "..", "apps", "server", "package.json"),
).href;
const benchDirectory = NodeURL.pathToFileURL(`${import.meta.dirname}${NodePath.sep}`).href;
const isBareSpecifier = (specifier: string) =>
  !/^(?:\.{0,2}\/|[a-z][a-z0-9+.-]*:|#)/iu.test(specifier);

NodeModule.registerHooks({
  resolve(specifier, context, nextResolve) {
    if (isBareSpecifier(specifier) && context.parentURL?.startsWith(benchDirectory) === true) {
      return nextResolve(specifier, { ...context, parentURL: serverPackage });
    }
    return nextResolve(specifier, context);
  },
});

await import("./fixture/cli.ts");

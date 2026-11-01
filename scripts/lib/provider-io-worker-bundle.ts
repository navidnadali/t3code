// @effect-diagnostics nodeBuiltinImport:off - Vite's build plugin runs before an Effect runtime exists.
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import { Rolldown } from "vite-plus/pack";

/**
 * Replaces `placeholder` with the source of `entry` bundled as one CommonJS string, which the
 * server runs with `eval`: executables resolve only built-ins and have no module files.
 */
export const providerIoWorkerBundle = (options: {
  readonly entry: string;
  readonly placeholder: string;
}) => ({
  name: "t3code:provider-io-worker",
  async load(id: string) {
    if (NodePath.normalize(id) !== NodePath.normalize(options.placeholder)) return null;
    const { output } = await Rolldown.build({
      input: options.entry,
      platform: "node",
      write: false,
      output: { format: "cjs", codeSplitting: false },
    });
    const code = output[0].code;
    const external = [...code.matchAll(/\brequire\(\s*["']([^"']+)["']\s*\)/g)]
      .map((match) => match[1]!)
      .filter((specifier) => !NodeModule.isBuiltin(specifier));
    if (external.length > 0) {
      throw new Error(
        `The provider I/O worker bundle requires non-built-ins: ${external.join(", ")}`,
      );
    }
    return `export const embeddedProviderIoWorkerSource = ${JSON.stringify(code)};`;
  },
});

// @effect-diagnostics nodeBuiltinImport:off - synchronous path checks that run before any database opens.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

export class UnsafePathError extends Error {}

/** Resolves symlinks in the longest existing prefix, so a path not yet created still resolves. */
export const canonicalPath = (path: string): string => {
  const absolute = NodePath.resolve(path);
  const missing: Array<string> = [];
  let existing = absolute;
  while (!NodeFS.existsSync(existing)) {
    const parent = NodePath.dirname(existing);
    if (parent === existing) break;
    missing.unshift(NodePath.basename(existing));
    existing = parent;
  }
  return NodePath.join(NodeFS.realpathSync.native(existing), ...missing);
};

const isWithin = (path: string, directory: string) => {
  const relative = NodePath.relative(directory, path);
  return relative === "" || (!relative.startsWith("..") && !NodePath.isAbsolute(relative));
};

/** The real T3 homes a fixture must never be written into: `~/.t3` and `$T3CODE_HOME`. */
export const protectedHomes = (
  env: NodeJS.ProcessEnv = process.env,
  home: string = NodeOS.homedir(),
): Array<string> => {
  const homes = [NodePath.join(home, ".t3")];
  const configured = env.T3CODE_HOME?.trim();
  if (configured) homes.push(configured);
  return homes;
};

/**
 * Refuses a destination inside a protected home, an existing destination, and one that is
 * (or whose scratch copies are) the source. `scratch` lists the side files the command writes.
 */
export const assertSafeDestination = (input: {
  readonly destination: string;
  readonly source?: string;
  readonly scratch?: ReadonlyArray<string>;
  readonly homes?: ReadonlyArray<string>;
}): string => {
  const destination = canonicalPath(input.destination);
  for (const home of input.homes ?? protectedHomes()) {
    const variants = new Set([NodePath.resolve(home), canonicalPath(home)]);
    for (const variant of variants) {
      if (isWithin(destination, variant)) {
        throw new UnsafePathError(
          `Refusing to write ${destination}: it is inside the T3 home ${variant}. Pick a path outside it.`,
        );
      }
    }
  }
  const written = [destination, ...(input.scratch ?? []).map(canonicalPath)];
  if (input.source !== undefined) {
    const source = canonicalPath(input.source);
    const sourceFiles = new Set([source, `${source}-wal`, `${source}-shm`, `${source}-journal`]);
    for (const path of written) {
      if (sourceFiles.has(path)) {
        throw new UnsafePathError(`Refusing to overwrite the source database ${source}.`);
      }
    }
  }
  for (const path of written) {
    if (NodeFS.existsSync(path)) {
      throw new UnsafePathError(`Refusing to overwrite ${path}: it already exists.`);
    }
  }
  return destination;
};

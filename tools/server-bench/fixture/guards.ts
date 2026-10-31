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
  const escapes = relative === ".." || relative.startsWith(`..${NodePath.sep}`);
  return !escapes && !NodePath.isAbsolute(relative);
};

/** A database path and the side files SQLite may create beside it. */
export const withSidecars = (path: string) =>
  [path, `${path}-wal`, `${path}-shm`, `${path}-journal`] as const;

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
 * Refuses to write inside a protected home, over an existing file, over the source (or its side
 * files), or onto a path the command writes for something else. `scratch` lists its side files.
 */
export const assertSafeDestination = (input: {
  readonly destination: string;
  readonly source?: string;
  readonly scratch?: ReadonlyArray<string>;
  readonly distinctFrom?: ReadonlyArray<string>;
  readonly homes?: ReadonlyArray<string>;
}): string => {
  const destination = canonicalPath(input.destination);
  const written = [destination, ...(input.scratch ?? []).map(canonicalPath)];
  const homes = (input.homes ?? protectedHomes()).flatMap((home) => [
    NodePath.resolve(home),
    canonicalPath(home),
  ]);
  for (const path of written) {
    const home = homes.find((candidate) => isWithin(path, candidate));
    if (home !== undefined) {
      throw new UnsafePathError(
        `Refusing to write ${path}: it is inside the T3 home ${home}. Pick a path outside it.`,
      );
    }
  }
  if (input.source !== undefined) {
    const source = canonicalPath(input.source);
    const sourceFiles = new Set(withSidecars(source));
    if (written.some((path) => sourceFiles.has(path))) {
      throw new UnsafePathError(`Refusing to overwrite the source database ${source}.`);
    }
  }
  const taken = new Set((input.distinctFrom ?? []).map(canonicalPath));
  const clash = written.find((path) => taken.has(path));
  if (clash !== undefined) {
    throw new UnsafePathError(
      `Refusing to write ${clash}: this command writes it for something else.`,
    );
  }
  for (const path of written) {
    if (NodeFS.existsSync(path)) {
      throw new UnsafePathError(`Refusing to overwrite ${path}: it already exists.`);
    }
  }
  return destination;
};

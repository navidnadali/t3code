// @effect-diagnostics nodeBuiltinImport:off - synchronous path checks that run before any database opens.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

export class UnsafePathError extends Error {}

/**
 * Resolves a path as the OS does: each component is followed through symlinks before the next,
 * so `link/..` is the parent of the link's target, not the directory holding the link (text
 * normalisation, as `path.resolve` does, gets that wrong). Missing components are appended as
 * written; a dangling symlink is refused, since writing through it lands wherever it points.
 */
export const canonicalPath = (path: string): string => {
  const absolute = NodePath.isAbsolute(path) ? path : `${process.cwd()}${NodePath.sep}${path}`;
  let current = NodePath.parse(absolute).root;
  for (const part of absolute.slice(current.length).split(NodePath.sep)) {
    if (part === "" || part === ".") continue;
    if (part === "..") {
      current = NodePath.dirname(current);
      continue;
    }
    const next = NodePath.join(current, part);
    const entry = NodeFS.lstatSync(next, { throwIfNoEntry: false });
    if (entry === undefined) {
      current = next;
    } else if (entry.isSymbolicLink() && !NodeFS.existsSync(next)) {
      throw new UnsafePathError(`Refusing ${path}: ${next} is a symlink to nothing.`);
    } else {
      current = NodeFS.realpathSync.native(next);
    }
  }
  return current;
};

/**
 * Creates `path` empty and private (0600), failing if anything is already there, a symlink
 * included. Callers pass the canonical path the guard checked, so the file lands where checked.
 */
export const createExclusive = (path: string) =>
  NodeFS.closeSync(NodeFS.openSync(path, "wx", 0o600));

/** A private (0700) directory beside `destination` for the copy that is not yet sanitised. */
export const privateScratchDirectory = (destination: string, prefix: string) =>
  NodeFS.mkdtempSync(NodePath.join(NodePath.dirname(destination), prefix));

/** Writes into a file `createExclusive` made; refuses anything else that took its place. */
export const fillExclusive = (path: string, text: string) => {
  const handle = NodeFS.openSync(path, NodeFS.constants.O_WRONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    NodeFS.writeFileSync(handle, text);
  } finally {
    NodeFS.closeSync(handle);
  }
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

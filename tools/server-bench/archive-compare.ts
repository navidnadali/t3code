// @effect-diagnostics nodeBuiltinImport:off - standalone bench tooling outside the Effect runtime.
/**
 * An unpacked archive's tree against the archive, in one pass over the tar.gz: each file's bytes, size and permission
 * bits, each directory and link, and nothing on disk the archive lacks (owners and times are not compared). Reads
 * ustar, GNU long names and pax headers; refuses other member types, a second top directory and unsafe paths.
 */
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeStreamPromises from "node:stream/promises";
import * as NodeZlib from "node:zlib";

export interface TreeComparison {
  /** The archive's single top directory. */
  readonly top: string;
  /** sha256 of each regular member asked for in `hash` (its path under the top directory), from the archive. */
  readonly hashes: ReadonlyMap<string, string>;
  /** Every difference, "<path under the top>: <what>", sorted by path. Empty when the tree is the archive's. */
  readonly differences: ReadonlyArray<string>;
  /** Members compared (directories, files and links). */
  readonly members: number;
}

const BLOCK = 512;
const MODE_BITS = 0o7777;

interface Header {
  /** The member's path under the top directory ("" for the top directory itself). */
  readonly path: string;
  readonly type: "file" | "dir" | "symlink" | "hardlink";
  readonly mode: number;
  readonly size: number;
  /** A symbolic link's target as written; a hard link's target path under the top directory. */
  readonly linkname: string;
}

/** A member's data, as it streams. */
interface Sink {
  readonly data: (chunk: Buffer) => void;
  readonly end: () => void;
}

/** A tar field: NUL-terminated text. */
const text = (block: Buffer, start: number, length: number) => {
  const field = block.subarray(start, start + length);
  const nul = field.indexOf(0);
  return field.subarray(0, nul === -1 ? field.length : nul).toString("utf8");
};

/** A tar number field: octal text, or GNU base-256 (high bit of the first byte set). */
function numeric(block: Buffer, start: number, length: number, what: string): number {
  const field = block.subarray(start, start + length);
  if ((field[0]! & 0x80) !== 0) {
    let value = field[0]! & 0x7f;
    for (let index = 1; index < field.length; index++) value = value * 256 + field[index]!;
    if (!Number.isSafeInteger(value)) throw new Error(`${what} is too large`);
    return value;
  }
  const digits = field
    .toString("ascii")
    .replace(/[\0 ]+$/u, "")
    .trim();
  if (digits === "") return 0;
  if (!/^[0-7]+$/u.test(digits))
    throw new Error(`${what} is not an octal number (${JSON.stringify(digits)})`);
  return Number.parseInt(digits, 8);
}

/** A header block's checksum is the sum of its bytes with the checksum field read as spaces (signed, by old tars). */
function checksumMatches(block: Buffer): boolean {
  const stored = numeric(block, 148, 8, "header checksum");
  let unsigned = 0;
  let signed = 0;
  for (let index = 0; index < BLOCK; index++) {
    const byte = index >= 148 && index < 156 ? 0x20 : block[index]!;
    unsigned += byte;
    signed += byte > 127 ? byte - 256 : byte;
  }
  return stored === unsigned || stored === signed;
}

/** pax extended header records: "<length> <key>=<value>\n", each length counting the whole record. */
function paxRecords(data: Buffer): Map<string, string> {
  const records = new Map<string, string>();
  let offset = 0;
  while (offset < data.length) {
    const space = data.indexOf(0x20, offset);
    if (space === -1) break;
    const length = Number.parseInt(data.subarray(offset, space).toString("ascii"), 10);
    if (
      !Number.isSafeInteger(length) ||
      length <= space - offset ||
      offset + length > data.length
    ) {
      throw new Error("a pax record has a bad length");
    }
    const record = data.subarray(space + 1, offset + length - 1).toString("utf8");
    const eq = record.indexOf("=");
    if (eq > 0) records.set(record.slice(0, eq), record.slice(eq + 1));
    offset += length;
  }
  return records;
}

/**
 * A tar stream read block by block: each member's header goes to `onMember`, whose sink gets the member's data. Long
 * names, long links and pax records apply to the member after them.
 */
class TarReader {
  private readonly header = Buffer.alloc(BLOCK);
  private headerFill = 0;
  /** Data bytes left in the current member, then its padding to the block. */
  private remaining = 0;
  /** The current member's data size (its padding follows it). */
  private memberSize = 0;
  private padding = 0;
  private sink: Sink | null = null;
  /** A metadata member being read ('L', 'K', 'x', 'g') and its data. */
  private meta: { readonly type: string; readonly chunks: Array<Buffer> } | null = null;
  private longName: string | null = null;
  private longLink: string | null = null;
  private pax: Map<string, string> | null = null;
  private memberName = "";
  private ended = false;

  private readonly onMember: (header: Header) => Sink;

  constructor(onMember: (header: Header) => Sink) {
    this.onMember = onMember;
  }

  push(chunk: Buffer): void {
    let offset = 0;
    while (offset < chunk.length && !this.ended) {
      if (this.remaining > 0) {
        const take = Math.min(this.remaining, chunk.length - offset);
        const part = chunk.subarray(offset, offset + take);
        if (this.meta !== null) this.meta.chunks.push(Buffer.from(part));
        else this.sink?.data(part);
        this.remaining -= take;
        offset += take;
        if (this.remaining === 0) this.endMember();
        continue;
      }
      if (this.padding > 0) {
        const take = Math.min(this.padding, chunk.length - offset);
        this.padding -= take;
        offset += take;
        continue;
      }
      const take = Math.min(BLOCK - this.headerFill, chunk.length - offset);
      chunk.copy(this.header, this.headerFill, offset, offset + take);
      this.headerFill += take;
      offset += take;
      if (this.headerFill === BLOCK) {
        this.headerFill = 0;
        this.readHeader(this.header);
      }
    }
  }

  /** At the end of the stream: a member cut short is an error; a stream without its end blocks is not. */
  finish(): void {
    if (this.remaining > 0 || this.padding > 0 || this.headerFill > 0 || this.meta !== null) {
      throw new Error(`the archive ends inside member ${this.memberName || "(a header)"}`);
    }
  }

  private endMember(): void {
    if (this.meta !== null) {
      const data = Buffer.concat(this.meta.chunks);
      const value = () => {
        const nul = data.indexOf(0);
        return data.subarray(0, nul === -1 ? data.length : nul).toString("utf8");
      };
      if (this.meta.type === "L") this.longName = value();
      else if (this.meta.type === "K") this.longLink = value();
      else if (this.meta.type === "x") this.pax = paxRecords(data);
      this.meta = null;
    } else {
      this.sink?.end();
      this.sink = null;
    }
    this.padding = (BLOCK - (this.memberSize % BLOCK)) % BLOCK;
  }

  private readHeader(block: Buffer): void {
    if (block.every((byte) => byte === 0)) {
      // End of archive (two zero blocks; what follows is padding).
      this.ended = true;
      return;
    }
    if (!checksumMatches(block)) {
      throw new Error(`bad header checksum (the member after ${this.memberName || "the start"})`);
    }
    const type = String.fromCharCode(block[156]!);
    const magic = block.subarray(257, 263).toString("ascii");
    const prefix = magic.startsWith("ustar") ? text(block, 345, 155) : "";
    const rawName = text(block, 0, 100);
    const name =
      this.pax?.get("path") ?? this.longName ?? (prefix !== "" ? `${prefix}/${rawName}` : rawName);
    const paxSize = this.pax?.get("size");
    const size =
      paxSize !== undefined
        ? Number.parseInt(paxSize, 10)
        : numeric(block, 124, 12, `the size of ${name}`);
    if (!Number.isSafeInteger(size) || size < 0) throw new Error(`member ${name} has a bad size`);
    this.memberName = name;
    this.memberSize = size;
    if (type === "L" || type === "K" || type === "x" || type === "g") {
      this.meta = { type, chunks: [] };
      this.remaining = size;
      if (size === 0) this.endMember();
      return;
    }
    const linkname = this.pax?.get("linkpath") ?? this.longLink ?? text(block, 157, 100);
    this.longName = null;
    this.longLink = null;
    this.pax = null;
    const kind =
      type === "0" || type === "\0" || type === "7"
        ? "file"
        : type === "5"
          ? "dir"
          : type === "2"
            ? "symlink"
            : type === "1"
              ? "hardlink"
              : null;
    if (kind === null) {
      throw new Error(`member ${name} has type ${type}, which a release does not use`);
    }
    if (kind !== "file" && size !== 0)
      throw new Error(`member ${name} (type ${type}) carries data`);
    this.sink = this.onMember({
      path: name,
      type: kind,
      mode: numeric(block, 100, 8, `the mode of ${name}`) & MODE_BITS,
      size,
      linkname,
    });
    this.remaining = size;
    if (size === 0) this.endMember();
  }
}

/** A member path's components without "./" and trailing slashes; absolute paths and "..", "." are refused. */
function components(path: string): Array<string> {
  if (path.startsWith("/")) throw new Error(`unsafe member path ${path}`);
  const parts = path
    .replace(/^(\.\/)+/u, "")
    .replace(/\/+$/u, "")
    .split("/");
  if (parts.some((part) => part === "" || part === "." || part === "..")) {
    throw new Error(`unsafe member path ${path}`);
  }
  return parts;
}

const octalMode = (mode: number) => mode.toString(8).padStart(4, "0");

/** lstat, or undefined when nothing is there (also when a parent on the way is not a directory). */
function lstatOrNone(path: string): NodeFS.Stats | undefined {
  try {
    return NodeFS.lstatSync(path);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT" || code === "ENOTDIR") return undefined;
    throw error;
  }
}

/**
 * Compares `dir` (the archive's top directory, unpacked under any name) with `archive` (a tar.gz with one top
 * directory). `extras` are names allowed beside the members at the top of `dir` (regular files only, e.g. release.json);
 * `hash` names members whose sha256 is wanted from the archive. Throws when the archive cannot be read as one tree;
 * returns every difference otherwise.
 */
export async function compareTree(input: {
  readonly archive: string;
  readonly dir: string;
  readonly extras: ReadonlySet<string>;
  readonly hash: ReadonlySet<string>;
}): Promise<TreeComparison> {
  const differences: Array<[string, string]> = [];
  const differ = (path: string, what: string) => differences.push([path, what]);
  /** Members by path under the top, and every directory a member implies. */
  const members = new Map<string, Header>();
  const implied = new Set<string>([""]);
  const hashes = new Map<string, string>();
  /** Regular members whose file on disk has the archive's bytes (what a hard link to them must have too). */
  const verified = new Set<string>();
  /** The file being compared, closed even when the archive turns out unreadable halfway. */
  let openFd: number | null = null;
  let top: string | null = null;
  let compared = 0;

  const reader = new TarReader((header): Sink => {
    const parts = components(header.path);
    if (top === null) top = parts[0]!;
    else if (parts[0] !== top) {
      throw new Error(`the archive has more than one top directory (${top}, ${parts[0]})`);
    }
    const path = parts.slice(1).join("/");
    for (let depth = 1; depth < parts.length - 1; depth++)
      implied.add(parts.slice(1, depth + 1).join("/"));
    let linkname = header.linkname;
    if (header.type === "hardlink") {
      const target = components(header.linkname);
      if (target[0] !== top) throw new Error(`hard link ${header.path} points out of ${top}`);
      linkname = target.slice(1).join("/");
    }
    members.set(path, { ...header, path, linkname });
    compared++;
    const absolute = path === "" ? input.dir : NodePath.join(input.dir, path);
    const stat = lstatOrNone(absolute);
    const hasher =
      input.hash.has(path) && header.type === "file" ? NodeCrypto.createHash("sha256") : null;
    const ignore: Sink = {
      data: (chunk) => hasher?.update(chunk),
      end: () => {
        if (hasher !== null) hashes.set(path, hasher.digest("hex"));
      },
    };
    const label = path === "" ? "." : path;
    if (stat === undefined) {
      differ(label, "missing");
      return ignore;
    }
    switch (header.type) {
      case "dir":
        if (!stat.isDirectory()) differ(label, "not a directory");
        else if ((stat.mode & MODE_BITS) !== header.mode) {
          differ(
            label,
            `mode ${octalMode(stat.mode & MODE_BITS)} differs from the archive's ${octalMode(header.mode)}`,
          );
        }
        return ignore;
      case "symlink": {
        if (!stat.isSymbolicLink()) differ(label, "not a symbolic link");
        else {
          const target = NodeFS.readlinkSync(absolute);
          if (target !== header.linkname)
            differ(label, `links to ${target}, the archive's to ${header.linkname}`);
        }
        return ignore;
      }
      case "hardlink": {
        const target = members.get(linkname);
        if (target === undefined || target.type !== "file") {
          throw new Error(
            `hard link ${header.path} names ${header.linkname}, not a file before it in the archive`,
          );
        }
        if (!stat.isFile()) differ(label, "not a regular file");
        else if ((stat.mode & MODE_BITS) !== target.mode) {
          differ(
            label,
            `mode ${octalMode(stat.mode & MODE_BITS)} differs from the archive's ${octalMode(target.mode)}`,
          );
        } else {
          // The same file as its target (one inode), whose bytes were compared with the archive's when it went by.
          const targetStat = lstatOrNone(NodePath.join(input.dir, linkname));
          if (
            targetStat === undefined ||
            targetStat.dev !== stat.dev ||
            targetStat.ino !== stat.ino
          ) {
            differ(label, `not a hard link to ${linkname}`);
          } else if (!verified.has(linkname)) {
            differ(label, "contents differ from the archive's");
          }
        }
        return ignore;
      }
      case "file": {
        if (!stat.isFile()) {
          differ(label, "not a regular file");
          return ignore;
        }
        if ((stat.mode & MODE_BITS) !== header.mode) {
          differ(
            label,
            `mode ${octalMode(stat.mode & MODE_BITS)} differs from the archive's ${octalMode(header.mode)}`,
          );
        }
        if (stat.size !== header.size) {
          differ(label, `size ${stat.size} differs from the archive's ${header.size}`);
          return ignore;
        }
        // Read the file on disk alongside the member's data and compare as they come.
        const fd = NodeFS.openSync(absolute, "r");
        openFd = fd;
        let position = 0;
        let same = true;
        let buffer = Buffer.alloc(1 << 16);
        return {
          data: (chunk) => {
            hasher?.update(chunk);
            if (!same) return;
            if (buffer.length < chunk.length) buffer = Buffer.alloc(chunk.length);
            const read = NodeFS.readSync(fd, buffer, 0, chunk.length, position);
            if (read !== chunk.length || !buffer.subarray(0, read).equals(chunk)) same = false;
            position += chunk.length;
          },
          end: () => {
            NodeFS.closeSync(fd);
            openFd = null;
            if (hasher !== null) hashes.set(path, hasher.digest("hex"));
            if (same) verified.add(path);
            else differ(label, "contents differ from the archive's");
          },
        };
      }
    }
  });

  try {
    await NodeStreamPromises.pipeline(
      NodeFS.createReadStream(input.archive),
      NodeZlib.createGunzip(),
      async (source: AsyncIterable<Buffer>) => {
        for await (const chunk of source) reader.push(chunk);
        reader.finish();
      },
    );
  } catch (error) {
    throw new Error(`${input.archive}: ${error instanceof Error ? error.message : String(error)}`, {
      cause: error,
    });
  } finally {
    if (openFd !== null) NodeFS.closeSync(openFd);
  }
  if (top === null) throw new Error(`${input.archive}: the archive has no members`);

  // Every path on disk must be a member (or a directory a member implies), or an allowed name at the top.
  const walk = (relative: string) => {
    const absolute = relative === "" ? input.dir : NodePath.join(input.dir, relative);
    for (const entry of NodeFS.readdirSync(absolute, { withFileTypes: true })) {
      const path = relative === "" ? entry.name : `${relative}/${entry.name}`;
      const known = members.has(path) || implied.has(path);
      if (!known) {
        if (relative === "" && input.extras.has(entry.name)) {
          if (!entry.isFile()) differ(path, "not a regular file");
        } else differ(path, "not in the archive");
        continue;
      }
      // A directory the archive only implies (no header of its own) must be one, not a link to one elsewhere.
      if (!members.has(path) && !entry.isDirectory()) {
        differ(path, "not a directory");
        continue;
      }
      if (entry.isDirectory()) walk(path);
    }
  };
  if (NodeFS.lstatSync(input.dir, { throwIfNoEntry: false })?.isDirectory() === true) walk("");

  return {
    top,
    hashes,
    differences: differences
      .toSorted(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([path, what]) => `${path}: ${what}`),
    members: compared,
  };
}

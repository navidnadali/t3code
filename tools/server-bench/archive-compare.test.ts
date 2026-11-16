// @effect-diagnostics nodeBuiltinImport:off - Tests build archives and the trees they unpack to on disk.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";

import { describe, expect, it } from "vite-plus/test";

import { compareTree } from "./archive-compare.ts";

/** No AppleDouble (._*) members from the macOS tar: the archive holds the tree and nothing else. */
const TAR_OPTIONS = { env: { ...process.env, COPYFILE_DISABLE: "1" } };

const withTempDir = async (run: (directory: string) => Promise<void>) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "archive-compare-"));
  try {
    await run(directory);
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
};

const sha256 = (data: string | Buffer) =>
  NodeCrypto.createHash("sha256").update(data).digest("hex");

/** One tar member, as a ustar header (and its data blocks) written by hand. */
interface Member {
  readonly name: string;
  readonly type: string;
  readonly mode?: number;
  readonly data?: string | Buffer;
  readonly linkname?: string;
  /** ustar prefix field: the member's path is prefix/name. */
  readonly prefix?: string;
  /** Size written in the header, when it must differ from the data's. */
  readonly size?: number;
  /** Corrupts the header checksum. */
  readonly badChecksum?: boolean;
}

const octal = (value: number, width: number) => `${value.toString(8).padStart(width - 1, "0")}\0`;

function header(member: Member, size: number): Buffer {
  const block = Buffer.alloc(512);
  block.write(member.name, 0, 100, "utf8");
  block.write(octal(member.mode ?? 0o644, 8), 100, 8, "ascii");
  block.write(octal(1000, 8), 108, 8, "ascii");
  block.write(octal(1000, 8), 116, 8, "ascii");
  block.write(octal(size, 12), 124, 12, "ascii");
  block.write(octal(1_700_000_000, 12), 136, 12, "ascii");
  block.write(member.type, 156, 1, "ascii");
  if (member.linkname !== undefined) block.write(member.linkname, 157, 100, "utf8");
  block.write("ustar\0", 257, 6, "ascii");
  block.write("00", 263, 2, "ascii");
  if (member.prefix !== undefined) block.write(member.prefix, 345, 155, "utf8");
  block.fill(0x20, 148, 156);
  let sum = 0;
  for (const byte of block) sum += byte;
  if (member.badChecksum === true) sum += 1;
  block.write(`${sum.toString(8).padStart(6, "0")}\0 `, 148, 8, "ascii");
  return block;
}

function entry(member: Member): Buffer {
  const data =
    member.data === undefined
      ? Buffer.alloc(0)
      : Buffer.isBuffer(member.data)
        ? member.data
        : Buffer.from(member.data);
  const padded = Buffer.alloc(Math.ceil(data.length / 512) * 512);
  data.copy(padded);
  return Buffer.concat([header(member, member.size ?? data.length), padded]);
}

/** A pax extended header ('x') carrying `records` for the member after it. */
function pax(records: Record<string, string>): Buffer {
  let body = "";
  for (const [key, value] of Object.entries(records)) {
    const line = ` ${key}=${value}\n`;
    let length = line.length + 1;
    while (`${length}${line}`.length !== length) length = `${length}${line}`.length;
    body += `${length}${line}`;
  }
  return entry({ name: "PaxHeaders/x", type: "x", data: body });
}

/** A GNU long name ('L') or long link ('K') record for the member after it. */
const gnuLong = (type: "L" | "K", value: string) =>
  entry({ name: "././@LongLink", type, data: `${value}\0` });

const archiveOf = (parts: ReadonlyArray<Buffer>) =>
  NodeZlib.gzipSync(Buffer.concat([...parts, Buffer.alloc(1024)]));

const T3 = "single executable bytes\n";
const MODULE = Buffer.from([0x7f, 0x45, 0x4c, 0x46, 1, 2, 3, 4, 5]);

/** The archive every case starts from, and the tree it unpacks to (written by hand, as tar would). */
function standard(root: string) {
  const top = "t3-1.2.3-linux-x64";
  const archive = NodePath.join(root, "release.tar.gz");
  NodeFS.writeFileSync(
    archive,
    archiveOf([
      entry({ name: `${top}/`, type: "5", mode: 0o755 }),
      entry({ name: `${top}/t3`, type: "0", mode: 0o755, data: T3 }),
      entry({ name: `${top}/node_modules/`, type: "5", mode: 0o755 }),
      entry({ name: `${top}/node_modules/m/`, type: "5", mode: 0o755 }),
      entry({
        name: `${top}/node_modules/m/index.js`,
        type: "0",
        mode: 0o644,
        data: "exports.x = 1;\n",
      }),
      entry({ name: `${top}/node_modules/m/addon.node`, type: "0", mode: 0o755, data: MODULE }),
      entry({ name: `${top}/node_modules/m/empty`, type: "0", mode: 0o644, data: "" }),
      entry({ name: `${top}/node_modules/m/main.js`, type: "2", linkname: "index.js" }),
      entry({
        name: `${top}/node_modules/m/same.js`,
        type: "1",
        linkname: `${top}/node_modules/m/index.js`,
      }),
      entry({
        name: `${top}/resource-monitor/linux-x64/t3-resource-monitor`,
        type: "0",
        mode: 0o755,
        data: "monitor\n",
      }),
    ]),
  );
  const dir = NodePath.join(root, "unpacked");
  const m = NodePath.join(dir, "node_modules", "m");
  NodeFS.mkdirSync(m, { recursive: true });
  NodeFS.mkdirSync(NodePath.join(dir, "resource-monitor", "linux-x64"), { recursive: true });
  NodeFS.writeFileSync(NodePath.join(dir, "t3"), T3);
  NodeFS.writeFileSync(NodePath.join(m, "index.js"), "exports.x = 1;\n");
  NodeFS.writeFileSync(NodePath.join(m, "addon.node"), MODULE);
  NodeFS.writeFileSync(NodePath.join(m, "empty"), "");
  NodeFS.symlinkSync("index.js", NodePath.join(m, "main.js"));
  NodeFS.linkSync(NodePath.join(m, "index.js"), NodePath.join(m, "same.js"));
  NodeFS.writeFileSync(
    NodePath.join(dir, "resource-monitor", "linux-x64", "t3-resource-monitor"),
    "monitor\n",
  );
  for (const [path, mode] of [
    [dir, 0o755],
    [NodePath.join(dir, "t3"), 0o755],
    [NodePath.join(dir, "node_modules"), 0o755],
    [m, 0o755],
    [NodePath.join(m, "index.js"), 0o644],
    [NodePath.join(m, "addon.node"), 0o755],
    [NodePath.join(m, "empty"), 0o644],
    [NodePath.join(dir, "resource-monitor", "linux-x64", "t3-resource-monitor"), 0o755],
  ] as const) {
    NodeFS.chmodSync(path, mode);
  }
  return { top, archive, dir, m };
}

const compare = (archive: string, dir: string, extras: ReadonlyArray<string> = []) =>
  compareTree({ archive, dir, extras: new Set(extras), hash: new Set(["t3"]) });

describe("compareTree (an unpacked tree against its archive, member by member)", () => {
  it("accepts the tree its archive unpacks to: files, directories, a symbolic link and a hard link", async () => {
    await withTempDir(async (root) => {
      const { top, archive, dir } = standard(root);
      const result = await compare(archive, dir);
      expect(result.differences).toEqual([]);
      expect(result.top).toBe(top);
      expect(result.hashes.get("t3")).toBe(sha256(T3));
      expect(result.members).toBe(10);
    });
  });

  it("names a file whose bytes, size or mode differ, a link with another target and a member missing on disk", async () => {
    await withTempDir(async (root) => {
      const { archive, dir, m } = standard(root);
      // A native module one byte longer, a script with the same size and other bytes, a mode change.
      NodeFS.appendFileSync(NodePath.join(m, "addon.node"), "X");
      NodeFS.rmSync(NodePath.join(m, "same.js"));
      NodeFS.writeFileSync(NodePath.join(m, "index.js"), "exports.y = 1;\n");
      NodeFS.linkSync(NodePath.join(m, "index.js"), NodePath.join(m, "same.js"));
      NodeFS.chmodSync(
        NodePath.join(dir, "resource-monitor", "linux-x64", "t3-resource-monitor"),
        0o775,
      );
      NodeFS.rmSync(NodePath.join(m, "main.js"));
      NodeFS.symlinkSync("addon.node", NodePath.join(m, "main.js"));
      NodeFS.rmSync(NodePath.join(m, "empty"));
      const { differences } = await compare(archive, dir);
      expect(differences).toEqual([
        "node_modules/m/addon.node: size 10 differs from the archive's 9",
        "node_modules/m/empty: missing",
        "node_modules/m/index.js: contents differ from the archive's",
        "node_modules/m/main.js: links to addon.node, the archive's to index.js",
        "node_modules/m/same.js: contents differ from the archive's",
        "resource-monitor/linux-x64/t3-resource-monitor: mode 0775 differs from the archive's 0755",
      ]);
    });
  });

  it("names a file where the archive has a link, a directory or a file, and the other way round", async () => {
    await withTempDir(async (root) => {
      const { archive, dir, m } = standard(root);
      NodeFS.rmSync(NodePath.join(m, "main.js"));
      NodeFS.writeFileSync(NodePath.join(m, "main.js"), "exports.x = 1;\n");
      NodeFS.rmSync(NodePath.join(dir, "t3"));
      NodeFS.symlinkSync("/bin/sh", NodePath.join(dir, "t3"));
      NodeFS.rmSync(NodePath.join(dir, "node_modules", "m"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(dir, "node_modules", "m"), "");
      const { differences } = await compare(archive, dir);
      expect(differences).toContain("t3: not a regular file");
      expect(differences).toContain("node_modules/m: not a directory");
      expect(differences).toContain("node_modules/m/index.js: missing");
    });
  });

  it("names every path on disk the archive does not have, except the names allowed at the top", async () => {
    await withTempDir(async (root) => {
      const { archive, dir, m } = standard(root);
      NodeFS.writeFileSync(NodePath.join(dir, "release.json"), "{}");
      NodeFS.writeFileSync(NodePath.join(m, "release.json"), "{}");
      NodeFS.writeFileSync(NodePath.join(m, "extra.js"), "");
      NodeFS.mkdirSync(NodePath.join(dir, "node_modules", "added", "deep"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(dir, "node_modules", "added", "deep", "x.js"), "");
      NodeFS.mkdirSync(NodePath.join(dir, "exec-argv.json"));
      const { differences } = await compare(archive, dir, ["release.json", "exec-argv.json"]);
      expect(differences).toEqual([
        "exec-argv.json: not a regular file",
        "node_modules/added: not in the archive",
        "node_modules/m/extra.js: not in the archive",
        "node_modules/m/release.json: not in the archive",
      ]);
    });
  });

  it("reads long names and links from GNU and pax records and from the ustar prefix", async () => {
    await withTempDir(async (root) => {
      const top = "t3-1.2.3-linux-x64";
      const long = `${"d".repeat(60)}/${"e".repeat(60)}/file.js`;
      const target = `${"t".repeat(120)}.js`;
      const archive = NodePath.join(root, "a.tar.gz");
      NodeFS.writeFileSync(
        archive,
        archiveOf([
          entry({ name: `${top}/t3`, type: "0", mode: 0o755, data: T3 }),
          gnuLong("L", `${top}/${long}`),
          entry({ name: "truncated-by-gnu", type: "0", mode: 0o644, data: "gnu\n" }),
          pax({ path: `${top}/pax/${"p".repeat(110)}.js` }),
          entry({ name: "truncated-by-pax", type: "0", mode: 0o644, data: "pax\n" }),
          gnuLong("K", target),
          entry({ name: `${top}/k`, type: "2", linkname: "short" }),
          pax({ linkpath: target }),
          entry({ name: `${top}/x`, type: "2", linkname: "short" }),
          entry({
            name: "u.js",
            prefix: `${top}/ustar/${"u".repeat(90)}`,
            type: "0",
            mode: 0o644,
            data: "u\n",
          }),
        ]),
      );
      const dir = NodePath.join(root, "tree");
      NodeFS.mkdirSync(NodePath.join(dir, NodePath.dirname(long)), { recursive: true });
      NodeFS.mkdirSync(NodePath.join(dir, "pax"), { recursive: true });
      NodeFS.mkdirSync(NodePath.join(dir, "ustar", "u".repeat(90)), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(dir, "t3"), T3, { mode: 0o755 });
      NodeFS.chmodSync(NodePath.join(dir, "t3"), 0o755);
      for (const [path, data] of [
        [long, "gnu\n"],
        [`pax/${"p".repeat(110)}.js`, "pax\n"],
        [`ustar/${"u".repeat(90)}/u.js`, "u\n"],
      ] as const) {
        NodeFS.writeFileSync(NodePath.join(dir, path), data);
        NodeFS.chmodSync(NodePath.join(dir, path), 0o644);
      }
      NodeFS.symlinkSync(target, NodePath.join(dir, "k"));
      NodeFS.symlinkSync(target, NodePath.join(dir, "x"));
      const result = await compare(archive, dir);
      expect(result.differences).toEqual([]);
      expect(result.members).toBe(6);
    });
  });

  it("refuses an archive it cannot trust to describe one tree: two tops, an unsafe path, a bad checksum, other member types", async () => {
    await withTempDir(async (root) => {
      const dir = NodePath.join(root, "tree");
      NodeFS.mkdirSync(dir);
      const cases: Array<[ReadonlyArray<Buffer>, RegExp]> = [
        [
          [
            entry({ name: "a/t3", type: "0", data: "x" }),
            entry({ name: "b/t3", type: "0", data: "x" }),
          ],
          /more than one top directory \(a, b\)/,
        ],
        [
          [entry({ name: "a/../../etc/passwd", type: "0", data: "x" })],
          /unsafe member path a\/\.\.\/\.\.\/etc\/passwd/,
        ],
        [
          [entry({ name: "/etc/passwd", type: "0", data: "x" })],
          /unsafe member path \/etc\/passwd/,
        ],
        [[entry({ name: "a/t3", type: "0", data: "x", badChecksum: true })], /header checksum/],
        [
          [entry({ name: "a/dev", type: "3" })],
          /member a\/dev has type 3, which a release does not use/,
        ],
        [[entry({ name: "a/fifo", type: "6" })], /member a\/fifo has type 6/],
        [
          [entry({ name: "a/t3", type: "0", data: "x".repeat(1000) }).subarray(0, 600)],
          /ends inside member a\/t3/,
        ],
      ];
      for (const [parts, error] of cases) {
        const archive = NodePath.join(root, "bad.tar.gz");
        const body = Buffer.concat(parts);
        NodeFS.writeFileSync(
          archive,
          NodeZlib.gzipSync(
            error.source.includes("ends inside") ? body : Buffer.concat([body, Buffer.alloc(1024)]),
          ),
        );
        await expect(compare(archive, dir)).rejects.toThrow(error);
      }
      NodeFS.writeFileSync(NodePath.join(root, "plain.tar.gz"), "not gzip at all");
      await expect(compare(NodePath.join(root, "plain.tar.gz"), dir)).rejects.toThrow(
        /gzip|header/i,
      );
    });
  });

  it("agrees with the system tar: its archive of a tree compares equal, and one changed byte does not", async () => {
    await withTempDir(async (root) => {
      const top = "t3-9.9.9-linux-x64";
      const src = NodePath.join(root, "src", top);
      NodeFS.mkdirSync(NodePath.join(src, "node_modules", "@scope", "pkg"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(src, "t3"), T3, { mode: 0o755 });
      NodeFS.writeFileSync(
        NodePath.join(src, "node_modules", "@scope", "pkg", "index.js"),
        "x".repeat(70_000),
      );
      NodeFS.writeFileSync(NodePath.join(src, `${"n".repeat(120)}.txt`), "long\n");
      NodeFS.symlinkSync("t3", NodePath.join(src, "t3-link"));
      const archive = NodePath.join(root, "sys.tar.gz");
      const made = NodeChildProcess.spawnSync(
        "tar",
        ["-czf", archive, "-C", NodePath.join(root, "src"), top],
        TAR_OPTIONS,
      );
      expect(made.status).toBe(0);
      const dest = NodePath.join(root, "dest");
      NodeFS.mkdirSync(dest);
      const unpacked = NodeChildProcess.spawnSync("tar", ["-xzpf", archive, "-C", dest]);
      expect(unpacked.status).toBe(0);
      const dir = NodePath.join(dest, top);
      expect((await compare(archive, dir)).differences).toEqual([]);
      const file = NodePath.join(dir, "node_modules", "@scope", "pkg", "index.js");
      const bytes = NodeFS.readFileSync(file);
      bytes[65_537] = 0x79;
      NodeFS.writeFileSync(file, bytes);
      expect((await compare(archive, dir)).differences).toEqual([
        "node_modules/@scope/pkg/index.js: contents differ from the archive's",
      ]);
    });
  });

  it("names a directory the archive only implies when a symbolic link stands in its place", async () => {
    await withTempDir(async (root) => {
      const top = "t3-1.2.3-linux-x64";
      const archive = NodePath.join(root, "a.tar.gz");
      // No directory headers: lib/ is implied by lib/x.js alone.
      NodeFS.writeFileSync(
        archive,
        archiveOf([
          entry({ name: `${top}/t3`, type: "0", mode: 0o755, data: T3 }),
          entry({ name: `${top}/lib/x.js`, type: "0", mode: 0o644, data: "x\n" }),
        ]),
      );
      const elsewhere = NodePath.join(root, "elsewhere", "lib");
      NodeFS.mkdirSync(elsewhere, { recursive: true });
      NodeFS.writeFileSync(NodePath.join(elsewhere, "x.js"), "x\n");
      NodeFS.chmodSync(NodePath.join(elsewhere, "x.js"), 0o644);
      NodeFS.writeFileSync(NodePath.join(elsewhere, "unarchived.js"), "");
      const dir = NodePath.join(root, "tree");
      NodeFS.mkdirSync(dir);
      NodeFS.writeFileSync(NodePath.join(dir, "t3"), T3);
      NodeFS.chmodSync(NodePath.join(dir, "t3"), 0o755);
      NodeFS.symlinkSync(elsewhere, NodePath.join(dir, "lib"));
      expect((await compare(archive, dir)).differences).toEqual(["lib: not a directory"]);
    });
  });

  it("names a hard link that is an independent copy, though its bytes are the same", async () => {
    await withTempDir(async (root) => {
      const { archive, m } = standard(root);
      NodeFS.rmSync(NodePath.join(m, "same.js"));
      NodeFS.copyFileSync(NodePath.join(m, "index.js"), NodePath.join(m, "same.js"));
      NodeFS.chmodSync(NodePath.join(m, "same.js"), 0o644);
      expect((await compare(archive, NodePath.join(root, "unpacked"))).differences).toEqual([
        "node_modules/m/same.js: not a hard link to node_modules/m/index.js",
      ]);
    });
  });
});

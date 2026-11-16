// @effect-diagnostics nodeBuiltinImport:off - Tests build real release archives on disk.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { describe, expect, it } from "vite-plus/test";

import {
  RELEASE_SCHEMA,
  checkReleaseDigests,
  checkTree,
  parseSha256Sums,
  releaseAssetName,
  topDirOf,
  unpackRelease,
} from "./release.ts";
import { seaTarget } from "./run.ts";

/** No AppleDouble (._*) members from the macOS tar: the archive holds the tree and nothing else. */
const TAR_OPTIONS = { env: { ...process.env, COPYFILE_DISABLE: "1" } };

const sha256 = (data: string | Buffer) =>
  NodeCrypto.createHash("sha256").update(data).digest("hex");

const withTempDir = async (run: (directory: string) => Promise<void>) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "server-bench-release-"));
  try {
    await run(directory);
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
};

const version = "1.2.3-nightly.20261115.9";
const top = `t3-${version}-linux-x64`;
const binary = "#!/bin/sh\necho '1.2.3-nightly.20261115.9'\n";
const commit = "295a0a7332aa1b0cc2f737dae71bd5e607a6ebb9";

/** A release-shaped archive (`t3-<v>-linux-x64/` with t3, node_modules, resource-monitor) built with tar. */
const makeArchive = (root: string, t3: string | Buffer = binary) => {
  const src = NodePath.join(root, "src");
  NodeFS.mkdirSync(NodePath.join(src, top, "node_modules", "node-pty"), { recursive: true });
  NodeFS.mkdirSync(NodePath.join(src, top, "resource-monitor", "linux-x64"), { recursive: true });
  NodeFS.mkdirSync(NodePath.join(src, top, "client"), { recursive: true });
  NodeFS.writeFileSync(NodePath.join(src, top, "t3"), t3, { mode: 0o755 });
  NodeFS.writeFileSync(NodePath.join(src, top, "node_modules", "node-pty", "package.json"), "{}");
  NodeFS.writeFileSync(NodePath.join(src, top, "client", "index.html"), "<!doctype html>\n");
  NodeFS.writeFileSync(
    NodePath.join(src, top, "resource-monitor", "linux-x64", "t3-resource-monitor"),
    "#!/bin/sh\n",
    { mode: 0o755 },
  );
  const archive = NodePath.join(root, `${top}.tar.gz`);
  const tar = NodeChildProcess.spawnSync("tar", ["-czf", archive, "-C", src, top], TAR_OPTIONS);
  if (tar.status !== 0) throw new Error(`tar failed: ${tar.stderr.toString()}`);
  NodeFS.rmSync(src, { recursive: true, force: true });
  return archive;
};

describe("release asset names and digests", () => {
  it("names the server archive as upstream does (no leading v)", () => {
    expect(releaseAssetName("v0.1.1-nightly.20261017.3272", "linux-x64")).toBe(
      "t3-0.1.1-nightly.20261017.3272-linux-x64.tar.gz",
    );
    expect(releaseAssetName("0.1.9", "darwin-arm64")).toBe("t3-0.1.9-darwin-arm64.tar.gz");
  });

  it("parses SHA256SUMS in text and binary mode", () => {
    const sums = parseSha256Sums(
      `${"a".repeat(64)}  t3-1-linux-x64.tar.gz\n${"B".repeat(64)} *t3-1-darwin-arm64.tar.gz\n\nnot a line\n`,
    );
    expect(sums.get("t3-1-linux-x64.tar.gz")).toBe("a".repeat(64));
    expect(sums.get("t3-1-darwin-arm64.tar.gz")).toBe("b".repeat(64));
    expect(sums.size).toBe(2);
  });

  it("requires the archive sha256 to match both SHA256SUMS and the API asset digest", () => {
    const name = "t3-1-linux-x64.tar.gz";
    const good = "c".repeat(64);
    const sums = `${good}  ${name}\n`;
    expect(
      checkReleaseDigests({ archiveName: name, sha256: good, sums, apiDigest: `sha256:${good}` }),
    ).toEqual({ sha256sums: true, api_digest: true });
    expect(checkReleaseDigests({ archiveName: name, sha256: good, sums, apiDigest: null })).toEqual(
      { sha256sums: true, api_digest: null },
    );
    expect(() =>
      checkReleaseDigests({ archiveName: name, sha256: "d".repeat(64), sums, apiDigest: null }),
    ).toThrow(/sha256 mismatch.*SHA256SUMS/);
    expect(() =>
      checkReleaseDigests({
        archiveName: name,
        sha256: good,
        sums,
        apiDigest: `sha256:${"e".repeat(64)}`,
      }),
    ).toThrow(/sha256 mismatch.*API asset digest/);
    expect(() =>
      checkReleaseDigests({ archiveName: "other.tar.gz", sha256: good, sums, apiDigest: null }),
    ).toThrow(/SHA256SUMS has no entry for other.tar.gz/);
  });
});

describe("topDirOf (archive members stay in one top directory)", () => {
  it("returns the single top directory", () => {
    expect(topDirOf([`${top}/`, `${top}/t3`, `${top}/node_modules/x/package.json`])).toBe(top);
    expect(topDirOf([`./${top}/t3`, `./${top}/client/`])).toBe(top);
  });

  it("refuses absolute paths, parent references and a second top directory", () => {
    expect(() => topDirOf([`${top}/t3`, "/etc/passwd"])).toThrow(/absolute/);
    expect(() => topDirOf([`${top}/t3`, `${top}/../evil`])).toThrow(/\.\./);
    expect(() => topDirOf([`${top}/t3`, "other/t3"])).toThrow(/more than one top/);
    expect(() => topDirOf([])).toThrow(/empty/);
  });
});

describe("unpackRelease", () => {
  it("verifies the archive, unpacks it and writes release.json that run.ts accepts", async () => {
    await withTempDir(async (root) => {
      const archive = makeArchive(root);
      const dest = NodePath.join(root, "releases");
      const { dir, release } = await unpackRelease({
        archive,
        sha256: sha256(NodeFS.readFileSync(archive)),
        tag: `v${version}`,
        commit,
        platform: "linux-x64",
        dest,
      });
      expect(dir).toBe(NodePath.join(dest, top));
      expect(release).toMatchObject({
        schema: RELEASE_SCHEMA,
        tag: `v${version}`,
        version,
        commit,
        platform: "linux-x64",
        archive_name: `${top}.tar.gz`,
        archive_sha256: sha256(NodeFS.readFileSync(archive)),
        archive_bytes: NodeFS.statSync(archive).size,
        t3_sha256: sha256(binary),
      });
      expect(
        JSON.parse(NodeFS.readFileSync(NodePath.join(dir, "release.json"), "utf8")),
      ).toMatchObject({ t3_sha256: sha256(binary) });
      const target = seaTarget(dir);
      expect(target.archive?.layout).toBe("release");
      expect(target.archive?.binary_sha256).toBe(sha256(binary));
      expect(target.archive?.archive_sha256).toBe(sha256(NodeFS.readFileSync(archive)));
    });
  });

  it("refuses an archive with one byte of t3 changed before unpacking anything, naming the sha256 mismatch", async () => {
    await withTempDir(async (root) => {
      const original = makeArchive(NodePath.join(root, "a"));
      const expected = sha256(NodeFS.readFileSync(original));
      const changed = Buffer.from(binary);
      changed[changed.length - 3] = changed[changed.length - 3]! ^ 0x01;
      const tampered = makeArchive(NodePath.join(root, "b"), changed);
      const dest = NodePath.join(root, "releases");
      await expect(
        unpackRelease({
          archive: tampered,
          sha256: expected,
          tag: `v${version}`,
          commit,
          platform: "linux-x64",
          dest,
        }),
      ).rejects.toThrow(
        new RegExp(`sha256 mismatch: ${sha256(NodeFS.readFileSync(tampered))} .*${expected}`),
      );
      expect(NodeFS.existsSync(NodePath.join(dest, top))).toBe(false);
    });
  });

  it("refuses an archive whose top directory is not t3-<version>-<platform>, and an existing destination", async () => {
    await withTempDir(async (root) => {
      const archive = makeArchive(root);
      const hash = sha256(NodeFS.readFileSync(archive));
      const dest = NodePath.join(root, "releases");
      await expect(
        unpackRelease({
          archive,
          sha256: hash,
          tag: "v9.9.9",
          commit,
          platform: "linux-x64",
          dest,
        }),
      ).rejects.toThrow(/expected t3-9.9.9-linux-x64/);
      NodeFS.mkdirSync(NodePath.join(dest, top), { recursive: true });
      await expect(
        unpackRelease({
          archive,
          sha256: hash,
          tag: `v${version}`,
          commit,
          platform: "linux-x64",
          dest,
        }),
      ).rejects.toThrow(/already exists/);
    });
  });

  it("keeps the archive's own permission bits, whatever the umask, so run.ts can compare the tree with its archive", async () => {
    await withTempDir(async (root) => {
      const src = NodePath.join(root, "src");
      NodeFS.mkdirSync(NodePath.join(src, top, "shared"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(src, top, "t3"), binary);
      NodeFS.writeFileSync(NodePath.join(src, top, "shared", "open.js"), "");
      // Bits a umask would strip (group and other write).
      NodeFS.chmodSync(NodePath.join(src, top, "t3"), 0o777);
      NodeFS.chmodSync(NodePath.join(src, top, "shared", "open.js"), 0o666);
      NodeFS.chmodSync(NodePath.join(src, top, "shared"), 0o777);
      const archive = NodePath.join(root, `${top}.tar.gz`);
      const tar = NodeChildProcess.spawnSync("tar", ["-czf", archive, "-C", src, top], TAR_OPTIONS);
      expect(tar.status).toBe(0);
      const { dir } = await unpackRelease({
        archive,
        sha256: sha256(NodeFS.readFileSync(archive)),
        tag: `v${version}`,
        commit,
        platform: "linux-x64",
        dest: NodePath.join(root, "releases"),
      });
      const mode = (path: string) => NodeFS.statSync(NodePath.join(dir, path)).mode & 0o7777;
      expect([mode("t3"), mode("shared"), mode("shared/open.js")]).toEqual([0o777, 0o777, 0o666]);
    });
  });
});

describe("checkTree (no link out of the unpacked archive)", () => {
  it("resolves every link through the links it passes, so a chain cannot step outside", async () => {
    await withTempDir(async (root) => {
      const tree = NodePath.join(root, "tree");
      NodeFS.mkdirSync(NodePath.join(tree, "lib"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(root, "escape"), "outside\n");
      NodeFS.writeFileSync(NodePath.join(tree, "lib", "a.so"), "inside\n");
      NodeFS.symlinkSync("lib/a.so", NodePath.join(tree, "inside"));
      expect(() => checkTree(tree)).not.toThrow();
      // y -> . makes "y/.." the tree's parent, which reading the link text alone does not show.
      NodeFS.symlinkSync(".", NodePath.join(tree, "y"));
      NodeFS.symlinkSync("y/../escape", NodePath.join(tree, "x"));
      expect(() => checkTree(tree)).toThrow(/archive link x points out of the archive/);
    });
  });

  it("refuses a link that does not resolve", async () => {
    await withTempDir(async (root) => {
      const tree = NodePath.join(root, "tree");
      NodeFS.mkdirSync(tree);
      NodeFS.symlinkSync("missing", NodePath.join(tree, "dangling"));
      expect(() => checkTree(tree)).toThrow(/archive link dangling does not resolve/);
    });
  });
});

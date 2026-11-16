#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalFetch:off preferSchemaOverJson:off - standalone bench tooling outside the Effect runtime.
/**
 * Upstream release archives for run.ts --ref sea:<dir>: fetch one from the public GitHub release, check its sha256
 * against the release's SHA256SUMS and the GitHub API's asset digest, unpack it, and write release.json beside t3
 * (tag, commit, archive name and sha256, t3's own sha256). run.ts reads release.json and refuses a t3 that changed.
 */
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

export const RELEASE_SCHEMA = "t3-server-bench/release/1";
export const RELEASE_FILE = "release.json";
const DEFAULT_REPO = "pingdotgg/t3code";

export const HELP = `Usage:
  node tools/server-bench/release.ts fetch --tag <tag> [--platform linux-x64] --dest <dir> [--repo owner/name]
  node tools/server-bench/release.ts unpack --archive <file> --sha256 <hex> --tag <tag> --commit <sha>
                                            [--platform linux-x64] --dest <dir>

fetch   downloads t3-<version>-<platform>.tar.gz and SHA256SUMS of release <tag> from GitHub (public, no
        token needed; GITHUB_TOKEN is used when set), reads the release's commit and the asset's
        sha256 digest from the GitHub API, and requires the archive's sha256 to match both. Then unpack.
unpack  an archive already on disk: its sha256 must be --sha256 (refused before anything is
        extracted otherwise); its members must sit in one top directory t3-<version>-<platform>/ with
        no absolute path, no .. and no link out of it.
Both write <dest>/t3-<version>-<platform>/release.json and print that directory. The directory must
not exist yet. The archive is data: nothing in it is run here.`;

/** The server archive's asset name: upstream drops the tag's leading v. */
export function releaseAssetName(tag: string, platform: string): string {
  return `t3-${tag.replace(/^v/, "")}-${platform}.tar.gz`;
}

/** `<sha256>  <name>` (text) or `<sha256> *<name>` (binary) lines; anything else is skipped. */
export function parseSha256Sums(text: string): Map<string, string> {
  const sums = new Map<string, string>();
  for (const line of text.split("\n")) {
    const match = /^([0-9a-fA-F]{64}) [ *](.+)$/.exec(line.trim());
    if (match !== null) sums.set(match[2]!.trim(), match[1]!.toLowerCase());
  }
  return sums;
}

/**
 * The archive's sha256 must equal its SHA256SUMS entry and, when the API gave one, the asset's `sha256:<hex>` digest.
 * Returns which checks were made.
 */
export function checkReleaseDigests(input: {
  readonly archiveName: string;
  readonly sha256: string;
  readonly sums: string;
  readonly apiDigest: string | null;
}): { readonly sha256sums: true; readonly api_digest: true | null } {
  const expected = parseSha256Sums(input.sums).get(input.archiveName);
  if (expected === undefined) {
    throw new Error(`SHA256SUMS has no entry for ${input.archiveName}`);
  }
  if (expected !== input.sha256) {
    throw new Error(
      `${input.archiveName}: sha256 mismatch: ${input.sha256} downloaded, ${expected} in SHA256SUMS`,
    );
  }
  if (input.apiDigest === null) return { sha256sums: true, api_digest: null };
  const digest = /^sha256:([0-9a-f]{64})$/.exec(input.apiDigest.trim().toLowerCase());
  if (digest === null)
    throw new Error(`${input.archiveName}: API digest ${input.apiDigest} is not sha256:<hex>`);
  if (digest[1] !== input.sha256) {
    throw new Error(
      `${input.archiveName}: sha256 mismatch: ${input.sha256} downloaded, ${digest[1]} in the API asset digest`,
    );
  }
  return { sha256sums: true, api_digest: true };
}

/** The one top directory every archive member sits in; refuses absolute paths, `..` and a second top directory. */
export function topDirOf(names: ReadonlyArray<string>): string {
  let top: string | null = null;
  for (const raw of names) {
    const name = raw.replace(/^(\.\/)+/, "");
    if (name === "" || name === ".") continue;
    if (name.startsWith("/") || /^[A-Za-z]:/.test(name)) {
      throw new Error(`archive member ${raw} is an absolute path`);
    }
    const parts = name.split("/").filter((part) => part.length > 0 && part !== ".");
    if (parts.includes("..")) throw new Error(`archive member ${raw} has a .. component`);
    const first = parts[0]!;
    if (top === null) top = first;
    else if (first !== top) {
      throw new Error(`archive has more than one top directory (${top}, ${first})`);
    }
  }
  if (top === null) throw new Error("archive is empty");
  return top;
}

async function sha256File(path: string): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  for await (const chunk of NodeFS.createReadStream(path)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}

const run = (command: string, args: ReadonlyArray<string>) => {
  const result = NodeChildProcess.spawnSync(command, args, {
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
  });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} failed: ${(result.stderr ?? "").trim().slice(-400)}`,
    );
  }
  return result.stdout;
};

/** Every path under root must stay in it: a symlink that resolves outside, or any special file, is refused. */
export function checkTree(root: string): void {
  // realpathSync.native resolves as the kernel does (each link before the next component); the JS realpathSync
  // normalises a link's text first, so "y/../z" behind y -> . would look inside.
  const real = NodeFS.realpathSync.native(root);
  const walk = (directory: string) => {
    for (const name of NodeFS.readdirSync(directory)) {
      const path = NodePath.join(directory, name);
      const stat = NodeFS.lstatSync(path);
      if (stat.isSymbolicLink()) {
        // Resolved through every link it passes (a chain such as y -> . then x -> y/../z), not by its text alone.
        let target: string;
        try {
          target = NodeFS.realpathSync.native(path);
        } catch {
          throw new Error(`archive link ${NodePath.relative(root, path)} does not resolve`);
        }
        if (target !== real && !target.startsWith(`${real}${NodePath.sep}`)) {
          throw new Error(
            `archive link ${NodePath.relative(root, path)} points out of the archive`,
          );
        }
      } else if (stat.isDirectory()) {
        walk(path);
      } else if (!stat.isFile()) {
        throw new Error(
          `archive member ${NodePath.relative(root, path)} is not a file, directory or link`,
        );
      }
    }
  };
  walk(root);
}

export interface ReleaseRecord {
  readonly schema: string;
  readonly tag: string;
  readonly version: string;
  readonly commit: string;
  readonly platform: string;
  readonly archive_name: string;
  readonly archive_bytes: number;
  readonly archive_sha256: string;
  readonly checks: {
    /** The caller's expected sha256 (unpack --sha256) matched. */
    readonly expected_sha256: true;
    readonly sha256sums: true | null;
    readonly api_digest: true | null;
  };
  readonly t3_sha256: string;
  readonly unpacked_at: string;
}

/**
 * Checks the archive's sha256 against `sha256` before anything is extracted, unpacks it into a scratch directory beside
 * the destination, checks its members, writes release.json beside t3 and moves it into place.
 */
export async function unpackRelease(input: {
  readonly archive: string;
  readonly sha256: string;
  readonly tag: string;
  readonly commit: string;
  readonly platform: string;
  readonly dest: string;
  readonly checks?: { readonly sha256sums: true | null; readonly api_digest: true | null };
}): Promise<{ readonly dir: string; readonly release: ReleaseRecord }> {
  const expected = input.sha256.trim().toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(expected)) throw new Error(`--sha256 ${input.sha256} is not 64 hex`);
  if (!/^[0-9a-f]{40}$/.test(input.commit))
    throw new Error(`--commit ${input.commit} is not 40 hex`);
  const version = input.tag.replace(/^v/, "");
  const top = `t3-${version}-${input.platform}`;
  const dir = NodePath.join(NodePath.resolve(input.dest), top);
  if (NodeFS.existsSync(dir)) throw new Error(`${dir} already exists`);
  const archiveSha = await sha256File(input.archive);
  if (archiveSha !== expected) {
    throw new Error(
      `${NodePath.basename(input.archive)}: sha256 mismatch: ${archiveSha} on disk, ${expected} expected; refusing to unpack it`,
    );
  }
  const members = run("tar", ["-tzf", input.archive])
    .split("\n")
    .filter((line) => line.length > 0);
  const found = topDirOf(members);
  if (found !== top) throw new Error(`archive top directory is ${found}, expected ${top}`);
  NodeFS.mkdirSync(NodePath.resolve(input.dest), { recursive: true });
  const scratch = NodeFS.mkdtempSync(NodePath.join(NodePath.resolve(input.dest), ".unpack-"));
  try {
    // The archive's own permission bits whatever the umask (-p), so run.ts can compare the tree with the archive.
    run("tar", ["-xzpf", input.archive, "-C", scratch, "--no-same-owner"]);
    const unpacked = NodePath.join(scratch, top);
    if (NodeFS.readdirSync(scratch).length !== 1 || !NodeFS.statSync(unpacked).isDirectory()) {
      throw new Error(`archive did not unpack to ${top}/ alone`);
    }
    checkTree(unpacked);
    const t3 = NodePath.join(unpacked, "t3");
    const t3Stat = NodeFS.lstatSync(t3, { throwIfNoEntry: false });
    if (t3Stat === undefined || !t3Stat.isFile())
      throw new Error(`archive has no t3 executable in ${top}/`);
    if (NodeFS.existsSync(NodePath.join(unpacked, RELEASE_FILE))) {
      throw new Error(`archive already carries a ${RELEASE_FILE}; refusing to trust it`);
    }
    const release: ReleaseRecord = {
      schema: RELEASE_SCHEMA,
      tag: input.tag,
      version,
      commit: input.commit,
      platform: input.platform,
      archive_name: NodePath.basename(input.archive),
      archive_bytes: NodeFS.statSync(input.archive).size,
      archive_sha256: archiveSha,
      checks: {
        expected_sha256: true,
        sha256sums: input.checks?.sha256sums ?? null,
        api_digest: input.checks?.api_digest ?? null,
      },
      t3_sha256: await sha256File(t3),
      unpacked_at: new Date().toISOString(),
    };
    NodeFS.writeFileSync(
      NodePath.join(unpacked, RELEASE_FILE),
      `${JSON.stringify(release, null, 2)}\n`,
    );
    NodeFS.renameSync(unpacked, dir);
    return { dir, release };
  } finally {
    NodeFS.rmSync(scratch, { recursive: true, force: true });
  }
}

interface GithubAsset {
  readonly name: string;
  readonly browser_download_url: string;
  readonly size: number;
  readonly digest?: string | null;
}

async function githubJson(url: string): Promise<unknown> {
  const headers: Record<string, string> = {
    accept: "application/vnd.github+json",
    "user-agent": "t3cp-harness",
  };
  if (process.env.GITHUB_TOKEN) headers.authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  const response = await fetch(url, { headers, signal: AbortSignal.timeout(60_000) });
  if (!response.ok) throw new Error(`GET ${url}: HTTP ${response.status}`);
  return (await response.json()) as unknown;
}

async function download(url: string, file: string): Promise<void> {
  const response = await fetch(url, {
    headers: { "user-agent": "t3cp-harness" },
    redirect: "follow",
    signal: AbortSignal.timeout(30 * 60_000),
  });
  if (!response.ok || response.body === null)
    throw new Error(`GET ${url}: HTTP ${response.status}`);
  const out = NodeFS.createWriteStream(file, { mode: 0o600 });
  for await (const chunk of response.body as unknown as AsyncIterable<Uint8Array>) {
    if (!out.write(chunk)) await new Promise<void>((resolve) => out.once("drain", resolve));
  }
  await new Promise<void>((resolve, reject) =>
    out.end((error?: Error | null) => (error ? reject(error) : resolve())),
  );
}

/**
 * Downloads the release's server archive and SHA256SUMS into a new scratch directory under `dest`, checks the archive
 * against SHA256SUMS and the API asset digest, and unpacks it.
 */
export async function fetchRelease(input: {
  readonly tag: string;
  readonly platform: string;
  readonly dest: string;
  readonly repo?: string;
}): Promise<{ readonly dir: string; readonly release: ReleaseRecord }> {
  const repo = input.repo ?? DEFAULT_REPO;
  const api = `https://api.github.com/repos/${repo}/releases/tags/${encodeURIComponent(input.tag)}`;
  const release = (await githubJson(api)) as {
    readonly tag_name?: string;
    readonly target_commitish?: string;
    readonly assets?: ReadonlyArray<GithubAsset>;
  };
  if (release.tag_name !== input.tag)
    throw new Error(`${api}: tag_name ${String(release.tag_name)}`);
  let commit = release.target_commitish ?? "";
  if (!/^[0-9a-f]{40}$/.test(commit)) {
    // A branch name: the tag's own commit is the release's.
    const ref = (await githubJson(
      `https://api.github.com/repos/${repo}/commits/${encodeURIComponent(input.tag)}`,
    )) as { readonly sha?: string };
    commit = ref.sha ?? "";
  }
  if (!/^[0-9a-f]{40}$/.test(commit)) throw new Error(`${input.tag}: no commit SHA from the API`);
  const name = releaseAssetName(input.tag, input.platform);
  const asset = release.assets?.find((candidate) => candidate.name === name);
  const sumsAsset = release.assets?.find((candidate) => candidate.name === "SHA256SUMS");
  if (asset === undefined) throw new Error(`${input.tag}: no asset ${name}`);
  if (sumsAsset === undefined) throw new Error(`${input.tag}: no SHA256SUMS asset`);
  NodeFS.mkdirSync(NodePath.resolve(input.dest), { recursive: true });
  const scratch = NodeFS.mkdtempSync(NodePath.join(NodePath.resolve(input.dest), ".download-"));
  try {
    const archive = NodePath.join(scratch, name);
    const sumsFile = NodePath.join(scratch, "SHA256SUMS");
    await download(sumsAsset.browser_download_url, sumsFile);
    await download(asset.browser_download_url, archive);
    const size = NodeFS.statSync(archive).size;
    if (size !== asset.size)
      throw new Error(`${name}: ${size} bytes downloaded, ${asset.size} in the API`);
    const sha256 = await sha256File(archive);
    const checks = checkReleaseDigests({
      archiveName: name,
      sha256,
      sums: NodeFS.readFileSync(sumsFile, "utf8"),
      apiDigest: asset.digest ?? null,
    });
    return await unpackRelease({
      archive,
      sha256,
      tag: input.tag,
      commit,
      platform: input.platform,
      dest: input.dest,
      checks,
    });
  } finally {
    NodeFS.rmSync(scratch, { recursive: true, force: true });
  }
}

async function main(argv: ReadonlyArray<string>): Promise<number> {
  const [command, ...rest] = argv;
  const { values } = NodeUtil.parseArgs({
    args: rest,
    options: {
      tag: { type: "string" },
      // oxlint-disable-next-line t3code/no-global-process-runtime -- Standalone bench tooling has no Effect runtime.
      platform: { type: "string", default: `${process.platform}-${process.arch}` },
      dest: { type: "string" },
      repo: { type: "string" },
      archive: { type: "string" },
      sha256: { type: "string" },
      commit: { type: "string" },
      help: { type: "boolean", short: "h" },
    },
  });
  if (values.help || command === undefined || command === "help") {
    console.log(HELP);
    return command === undefined ? 2 : 0;
  }
  if (values.tag === undefined || values.dest === undefined)
    throw new Error("--tag and --dest are required");
  let result: { readonly dir: string; readonly release: ReleaseRecord };
  if (command === "fetch") {
    result = await fetchRelease({
      tag: values.tag,
      platform: values.platform,
      dest: values.dest,
      ...(values.repo === undefined ? {} : { repo: values.repo }),
    });
  } else if (command === "unpack") {
    if (
      values.archive === undefined ||
      values.sha256 === undefined ||
      values.commit === undefined
    ) {
      throw new Error("unpack needs --archive, --sha256 and --commit");
    }
    result = await unpackRelease({
      archive: values.archive,
      sha256: values.sha256,
      tag: values.tag,
      commit: values.commit,
      platform: values.platform,
      dest: values.dest,
    });
  } else {
    throw new Error(`unknown command ${command} (fetch or unpack)`);
  }
  console.error(
    `${result.release.tag}: archive sha256 ${result.release.archive_sha256} (SHA256SUMS ${String(result.release.checks.sha256sums)}, API digest ${String(result.release.checks.api_digest)}), t3 sha256 ${result.release.t3_sha256}`,
  );
  console.log(result.dir);
  return 0;
}

if (import.meta.main) {
  main(process.argv.slice(2)).then(
    (code) => process.exit(code),
    (error: unknown) => {
      console.error(`release: ${error instanceof Error ? error.message : String(error)}`);
      process.exit(2);
    },
  );
}

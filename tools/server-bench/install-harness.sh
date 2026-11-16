#!/usr/bin/env bash
# Installs the bench harness from a `git archive` of its branch into a ready, offline harness directory:
#   install-harness.sh --archive <harness.tar|.tar.gz> --dest <dir> [--sha <commit>] [--source-sha <commit>]
#                      [--state-dir <dir>] [--node <path>] [--install-node] [--node-version <x.y.z>]
# - --source-sha: when the archive is of a snapshot commit (same tree, published copy), the branch commit it holds.
# - Run-time commands: git (the driver initialises workspace repositories; the server runs git), ps, getconf and,
#   for CPU pinning on Linux, taskset. Missing ones are an error (Debian: apt-get install git procps).
# - Node: the archive's package.json engines.node (exact). --node uses that binary, else `node` on PATH; with
#   --install-node a missing or different Node is installed from nodejs.org into /opt/node-v<x.y.z>-linux-<arch>
#   (the tarball's sha256 checked against SHASUMS256.txt; needs root or sudo, and network for this step only).
# - The harness runs on Node alone (no npm packages at run time), so nothing else is downloaded.
# - <dest> must be new or empty. It gets the archive's tree, t3cp-harness.json (version, commit, files sha256, Node)
#   and tools/server-bench/settings/std.json (the bench provider instances for run.ts --settings). <dest> can be made
#   read-only afterwards; run.ts writes only to its --out and --work directories and the fakes to --state-dir.
# - --state-dir (default <dest>/state, must be writable by the bench user): the fakes' stats and provider homes that
#   std.json names.
# Prints the harness directory on success.
set -euo pipefail

ARCHIVE="" DEST="" SHA="" SOURCE_SHA="" STATE="" NODE_BIN="" INSTALL_NODE=0 NODE_VERSION=""
die() {
  echo "install-harness: $*" >&2
  exit 1
}
while [ $# -gt 0 ]; do
  case "$1" in
    --archive) ARCHIVE=${2:?}; shift 2 ;;
    --dest) DEST=${2:?}; shift 2 ;;
    --sha) SHA=${2:?}; shift 2 ;;
    --source-sha) SOURCE_SHA=${2:?}; shift 2 ;;
    --state-dir) STATE=${2:?}; shift 2 ;;
    --node) NODE_BIN=${2:?}; shift 2 ;;
    --install-node) INSTALL_NODE=1; shift ;;
    --node-version) NODE_VERSION=${2:?}; shift 2 ;;
    -h | --help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument $1 (see --help)" ;;
  esac
done
[ -n "$ARCHIVE" ] && [ -n "$DEST" ] || die "--archive and --dest are required"
[ -f "$ARCHIVE" ] || die "$ARCHIVE is not a file"
for tool in tar gzip sha256sum head od tr git ps getconf; do
  command -v "$tool" >/dev/null || die "$tool is missing (Debian: apt-get install git procps)"
done
if [ "$(uname -s)" = Linux ]; then
  command -v taskset >/dev/null || die "taskset is missing (util-linux): run.ts --server-cpus/--driver-cpus need it"
else
  echo "install-harness: not Linux; the run.ts --server-cpus/--driver-cpus pinning needs taskset" >&2
fi
[ -z "$SOURCE_SHA" ] || printf '%s' "$SOURCE_SHA" | grep -Eq '^[0-9a-f]{40}$' || die "--source-sha $SOURCE_SHA is not a 40-hex commit"

# The commit a `git archive` was made from: its pax global header (the first 512-byte header, type g) holds
# "52 comment=<40-hex>\n" in the block after it.
# head closes the pipe early: gzip's SIGPIPE is expected there, not a failure.
case "$ARCHIVE" in
  *.tar.gz | *.tgz) head_bytes() { { gzip -dc -- "$ARCHIVE" 2>/dev/null || true; } | head -c 1024; } ;;
  *.tar) head_bytes() { head -c 1024 -- "$ARCHIVE"; } ;;
  *) die "$ARCHIVE: expected .tar, .tar.gz or .tgz (git archive --format=tar or tar.gz)" ;;
esac
typeflag=$(head_bytes | od -An -c -j 156 -N 1 | tr -d ' \n')
archive_sha=""
if [ "$typeflag" = g ]; then
  archive_sha=$(head_bytes | tail -c 512 | tr -d '\0' | sed -n 's/^[0-9]* comment=\([0-9a-f]\{40\}\)$/\1/p' | head -1)
fi
if [ -z "$archive_sha" ] && [ -z "$SHA" ]; then
  die "$ARCHIVE carries no commit id (not made by git archive from a commit); pass --sha"
fi
if [ -n "$archive_sha" ] && [ -n "$SHA" ] && [ "$archive_sha" != "$SHA" ]; then
  die "$ARCHIVE was made from $archive_sha, not --sha $SHA"
fi
SHA=${SHA:-$archive_sha}
printf '%s' "$SHA" | grep -Eq '^[0-9a-f]{40}$' || die "--sha $SHA is not a 40-hex commit"

if [ -e "$DEST" ] && [ -n "$(ls -A "$DEST" 2>/dev/null)" ]; then die "$DEST exists and is not empty"; fi
mkdir -p "$DEST"
DEST=$(cd "$DEST" && pwd -P)
tar -xf "$ARCHIVE" -C "$DEST" --no-same-owner
STATE=${STATE:-$DEST/state}

# The run-time closure: the harness, the fakes, the replay recordings and dev-replay-agent the fakes load, and the
# root package.json ("type": "module" makes tools/server-bench/*.ts ES modules).
for path in package.json tools/server-bench/run.ts tools/server-bench/driver.ts tools/server-bench/collect.ts \
  tools/server-bench/report.ts tools/server-bench/release.ts tools/server-bench/providers.ts \
  tools/server-bench/preload/bench-preload.cjs tools/server-bench/preload/bench-preload.mjs \
  tools/server-bench/fake-claude/claude tools/server-bench/fake-codex/codex tools/server-bench/fake-acp/acp-agent \
  tools/server-bench/fake-common/replay.mjs apps/server/scripts/dev-replay-agent.ts \
  apps/server/src/orchestration-v2/testkit/fixtures; do
  [ -e "$DEST/$path" ] || die "the archive has no $path (archive the whole branch: git archive <commit>)"
done
for launcher in fake-claude/claude fake-codex/codex fake-acp/acp-agent; do
  [ -x "$DEST/tools/server-bench/$launcher" ] || die "tools/server-bench/$launcher is not executable in the archive"
done

want=$(sed -n 's/.*"node": *"\([0-9][0-9.]*\)".*/\1/p' "$DEST/package.json" | head -1)
NODE_VERSION=${NODE_VERSION:-$want}
[ -n "$NODE_VERSION" ] || die "no engines.node in package.json; pass --node-version"
node_ok() { [ -x "$1" ] && [ "$("$1" --version 2>/dev/null)" = "v$NODE_VERSION" ]; }
[ -n "$NODE_BIN" ] || NODE_BIN=$(command -v node || true)
if ! node_ok "${NODE_BIN:-/nonexistent}"; then
  [ "$INSTALL_NODE" = 1 ] || die "Node v$NODE_VERSION is required (found ${NODE_BIN:-none}: $("${NODE_BIN:-true}" --version 2>/dev/null || echo none)); pass --node or --install-node"
  case "$(uname -m)" in x86_64) arch=x64 ;; aarch64 | arm64) arch=arm64 ;; *) die "unsupported machine $(uname -m)" ;; esac
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  name="node-v$NODE_VERSION-$os-$arch"
  SUDO=""
  [ "$(id -u)" = 0 ] || SUDO=sudo
  if ! node_ok "/opt/$name/bin/node"; then
    command -v curl >/dev/null || die "curl is needed for --install-node"
    command -v xz >/dev/null || die "xz is needed for --install-node (xz-utils)"
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    curl -fsSLo "$tmp/$name.tar.xz" "https://nodejs.org/dist/v$NODE_VERSION/$name.tar.xz"
    curl -fsSLo "$tmp/SHASUMS256.txt" "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt"
    expected=$(awk -v f="$name.tar.xz" '$2 == f { print $1 }' "$tmp/SHASUMS256.txt")
    got=$(sha256sum "$tmp/$name.tar.xz" | cut -d' ' -f1)
    [ -n "$expected" ] && [ "$expected" = "$got" ] || die "node tarball sha256 mismatch: SHASUMS256 $expected, downloaded $got"
    $SUDO tar -C /opt -xJf "$tmp/$name.tar.xz" --no-same-owner
  fi
  NODE_BIN=/opt/$name/bin/node
  node_ok "$NODE_BIN" || die "$NODE_BIN is not v$NODE_VERSION after install"
fi
NODE_BIN=$(cd "$(dirname "$NODE_BIN")" && pwd -P)/$(basename "$NODE_BIN")

# Identity: the version run.ts declares, the archive's commit and the sha256 of the harness files (as run.ts
# computes it for every result).
"$NODE_BIN" --input-type=module - "$DEST" "$SHA" "$NODE_BIN" "$SOURCE_SHA" <<'JS'
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
const [dest, sha, node, sourceSha] = process.argv.slice(2);
const run = await import(NodePath.join(dest, "tools", "server-bench", "run.ts"));
const identity = {
  schema: "t3cp-harness/identity/1",
  version: run.HARNESS_VERSION,
  sha,
  source_sha: sourceSha === "" ? null : sourceSha,
  files_sha256: await run.harnessFilesSha256(dest),
  node: process.version,
  node_path: node,
  installed_at: new Date().toISOString(),
};
NodeFS.writeFileSync(NodePath.join(dest, run.HARNESS_IDENTITY_FILE), `${JSON.stringify(identity, null, 2)}\n`);
JS

# The std bench settings (run.ts --settings): the three fake providers with the standard transcript mix, built-in
# providers disabled, update checks and sidebar auto-settle off.
mkdir -p "$STATE" "$DEST/tools/server-bench/settings"
STATE=$(cd "$STATE" && pwd -P)
SETTINGS=$DEST/tools/server-bench/settings/std.json
printf '%s\n' '{"enableProviderUpdateChecks": false, "sidebarAutoSettleAfterDays": null, "sidebarAutoSettleOnMerge": false}' >"$SETTINGS"
(cd "$DEST" && "$NODE_BIN" tools/server-bench/providers.ts settings --out "$SETTINGS" --stats-dir "$STATE/fake-stats" \
  --kinds claude,codex,acp --seed 1 --speed 1 --claude-home "$STATE/claude-home" --codex-home "$STATE/codex-home" \
  --claude-transcripts "command_output_streaming:4,claude_mcp_tool_presentation:2,claude_background_task_wake:1,tool_call_read_only:1,web_search:1,large-tool-payload:1" \
  --codex-transcripts "command_output_streaming:3,large-tool-payload:1" \
  --acp-transcripts "todo_list:3,large-tool-payload:1" \
  --node "$NODE_BIN") >/dev/null
chmod 0644 "$SETTINGS"

# Smoke: both entry points answer --help, and the identity reads back.
(cd "$DEST" && "$NODE_BIN" tools/server-bench/run.ts --help >/dev/null) || die "run.ts --help failed"
(cd "$DEST" && "$NODE_BIN" tools/server-bench/release.ts help >/dev/null) || die "release.ts help failed"
"$NODE_BIN" --input-type=module -e "const r = await import('$DEST/tools/server-bench/run.ts'); console.error('installed', JSON.stringify(await r.harnessIdentity('$DEST')));" ||
  die "harness identity does not read back"
echo "$DEST"

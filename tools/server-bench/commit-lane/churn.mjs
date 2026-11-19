// Commit-lane bench: page-cache churn standing in for a busy server's history readers. Random preads of the
// database file (--chunk-kib; a --recent share inside the newest --recent-span) at --mibps push the writer's pages out
// of a memory-limited cgroup's cache, as busy readers do. Reads only.
// usage: node churn.mjs --db <file> [--mibps 40] [--chunk-kib 16] [--recent 0.3] [--recent-span 0.1] [--seconds 0]
import * as NodeFS from "node:fs";

const args = process.argv.slice(2);
const take = (name, fallback) => {
  const at = args.indexOf(name);
  return at < 0 ? fallback : args[at + 1];
};
const file = take("--db");
const mibps = Number(take("--mibps", "40"));
const chunk = Number(take("--chunk-kib", "16")) * 1024;
const recent = Number(take("--recent", "0.3"));
const recentSpan = Number(take("--recent-span", "0.1"));
const seconds = Number(take("--seconds", "0"));
let seed = Number(take("--seed", "99")) >>> 0 || 1;
const random = () => {
  seed = (seed + 0x6d2b79f5) >>> 0;
  let t = seed;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};
const fd = NodeFS.openSync(file, "r");
const size = NodeFS.fstatSync(fd).size;
const chunks = Math.floor(size / chunk);
const buffer = Buffer.allocUnsafe(chunk);
const started = Date.now();
let bytes = 0;
let reported = started;
let reportedBytes = 0;
const tick = () => {
  const now = Date.now();
  if (seconds > 0 && now - started >= seconds * 1000) {
    NodeFS.closeSync(fd);
    console.log(`CHURN-OK ${(bytes / 2 ** 20 / ((now - started) / 1000)).toFixed(1)} MiB/s mean`);
    process.exit(0);
  }
  const due = ((now - started) / 1000) * mibps * 2 ** 20;
  while (bytes < due) {
    const span = random() < recent ? Math.max(1, Math.floor(chunks * recentSpan)) : chunks;
    const index = chunks - span + Math.floor(random() * span);
    bytes += NodeFS.readSync(fd, buffer, 0, chunk, index * chunk);
    if (Date.now() - now > 50) break;
  }
  if (now - reported >= 10_000) {
    console.log(
      `churn ${((bytes - reportedBytes) / 2 ** 20 / ((now - reported) / 1000)).toFixed(1)} MiB/s`,
    );
    reported = now;
    reportedBytes = bytes;
  }
  setTimeout(tick, 5);
};
process.on("SIGTERM", () => {
  console.log(
    `CHURN-STOP ${(bytes / 2 ** 20 / ((Date.now() - started) / 1000)).toFixed(1)} MiB/s mean`,
  );
  process.exit(0);
});
tick();

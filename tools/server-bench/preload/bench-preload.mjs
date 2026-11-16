// Bench preload, loaded into the server by run.ts (NODE_OPTIONS --require=bench-preload.cjs). Active only in the server's
// main thread (`serve --base-dir <runDir>/base`); a no-op in the fakes, the driver and workers. Writes <runDir>/heap-config.json
// once, loop-1hz.ndjson every second, and loop-window.json between run.ts's ctl/window-start and ctl/window-end touches;
// ctl/prof-start and prof-stop record a V8 sampling profile (window.cpuprofile).
import * as NodeFS from "node:fs";
import * as NodeInspector from "node:inspector";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeSea from "node:sea";
import * as NodeV8 from "node:v8";
import * as NodeWorkerThreads from "node:worker_threads";
// No top-level await here: bench-preload.cjs loads this module with require() (the only preload path a single executable
// honours is --require; --import is skipped there), and require(esm) needs a module graph without top-level await.

const argv = process.argv;
const baseIndex = argv.indexOf("--base-dir");
// The server is `node .../apps/server/dist/bin.mjs serve --base-dir ...` (a dist ref) or the shipped single executable
// `.../t3 serve --base-dir ...` (a SEA ref); the fakes, the driver and the CLI never pass --base-dir.
let isSea = false;
try {
  isSea = NodeSea.isSea();
} catch {
  isSea = false;
}
const entryIsDist =
  typeof argv[1] === "string" &&
  argv[1].endsWith(NodePath.join("apps", "server", "dist", "bin.mjs"));
const entryIsSea = isSea || NodePath.basename(process.execPath) === "t3";
const isServer =
  NodeWorkerThreads.isMainThread &&
  (entryIsDist || entryIsSea) &&
  argv.includes("serve") &&
  baseIndex > 0 &&
  typeof argv[baseIndex + 1] === "string";

if (isServer) {
  const runDir = NodePath.dirname(NodePath.resolve(argv[baseIndex + 1]));
  const ctl = NodePath.join(runDir, "ctl");
  const out = NodePath.join(runDir, "loop-1hz.ndjson");

  // What the process actually runs with: the entry kind, the V8 flags in effect and the new-space (semi-space) size, so
  // a default such as --max-semi-space-size=64 baked into a build's execArgv is proven per run.
  try {
    const newSpace = NodeV8.getHeapSpaceStatistics().find(
      (space) => space.space_name === "new_space",
    );
    NodeFS.writeFileSync(
      NodePath.join(runDir, "heap-config.json"),
      `${JSON.stringify({
        schema: "t3-server-bench/heap-config/1",
        entry_kind: entryIsSea ? "sea" : "dist",
        exec_path: process.execPath,
        exec_argv: process.execArgv,
        node_options: process.env.NODE_OPTIONS ?? null,
        node: process.version,
        heap_size_limit_mib: Math.round(NodeV8.getHeapStatistics().heap_size_limit / 1048576),
        // Exact: the harness measures the semi-space size as (limit - --max-old-space-size) / 3.
        heap_size_limit_bytes: NodeV8.getHeapStatistics().heap_size_limit,
        new_space_size_mib: newSpace
          ? Math.round((newSpace.space_size / 1048576) * 100) / 100
          : null,
        new_space_available_mib: newSpace
          ? Math.round((newSpace.space_available_size / 1048576) * 100) / 100
          : null,
      })}\n`,
    );
  } catch (error) {
    process.stderr.write(`bench-preload heap-config: ${String(error?.message ?? error)}\n`);
  }

  const ns = (value) => (Number.isFinite(value) ? Math.round(value / 1e3) / 1e3 : null); // ns -> ms, 3 dp
  const summary = (h) => ({
    count: h.count,
    p50_ms: ns(h.percentile(50)),
    p90_ms: ns(h.percentile(90)),
    p99_ms: ns(h.percentile(99)),
    p999_ms: ns(h.percentile(99.9)),
    max_ms: ns(h.max),
    mean_ms: ns(h.mean),
  });
  const delay = NodePerfHooks.monitorEventLoopDelay({ resolution: 10 });
  delay.enable();
  let window = null; // { histogram (its own 10 ms monitor, never reset), startedAt, elu0, gc }
  const gcSecond = new Map();
  const gcKinds = { 1: "minor", 2: "major", 4: "incremental", 8: "weakcb" };
  const observer = new NodePerfHooks.PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
      const kind = gcKinds[entry.detail?.kind] ?? String(entry.detail?.kind ?? "unknown");
      const current = gcSecond.get(kind) ?? { n: 0, ms: 0, max_ms: 0 };
      current.n += 1;
      current.ms += entry.duration;
      current.max_ms = Math.max(current.max_ms, entry.duration);
      gcSecond.set(kind, current);
      if (window !== null) {
        const total = window.gc.get(kind) ?? { n: 0, ms: 0, max_ms: 0 };
        total.n += 1;
        total.ms += entry.duration;
        total.max_ms = Math.max(total.max_ms, entry.duration);
        window.gc.set(kind, total);
      }
    }
  });
  observer.observe({ entryTypes: ["gc"] });
  const round = (value) => Math.round(value * 1000) / 1000;
  const gcObject = (map) =>
    Object.fromEntries(
      [...map].map(([kind, value]) => [
        kind,
        { n: value.n, ms: round(value.ms), max_ms: round(value.max_ms) },
      ]),
    );
  const exists = (name) => {
    try {
      return NodeFS.existsSync(NodePath.join(ctl, name));
    } catch {
      return false;
    }
  };
  let lastElu = NodePerfHooks.performance.eventLoopUtilization();
  let lastCpu = process.cpuUsage();
  let lastUsage = process.resourceUsage();
  let lastMono = NodePerfHooks.performance.now();
  let profiler = "idle";
  let session = null;
  let windowDone = false;

  setInterval(() => {
    const mono = NodePerfHooks.performance.now();
    const elu = NodePerfHooks.performance.eventLoopUtilization();
    const eluDelta = NodePerfHooks.performance.eventLoopUtilization(elu, lastElu);
    const cpu = process.cpuUsage(lastCpu);
    const usage = process.resourceUsage();
    const memory = process.memoryUsage();
    // Young generation: V8's new space and old space.
    const spaces = {};
    for (const sp of NodeV8.getHeapSpaceStatistics()) spaces[sp.space_name] = sp;
    const newSpace = spaces.new_space;
    const oldSpace = spaces.old_space;
    const record = {
      t: Date.now(),
      dt_ms: round(mono - lastMono),
      elu: round(eluDelta.utilization),
      elu_active_ms: round(eluDelta.active),
      delay: summary(delay),
      gc: gcObject(gcSecond),
      proc_cpu_user_ms: round(cpu.user / 1000),
      proc_cpu_sys_ms: round(cpu.system / 1000),
      proc_minflt: usage.minorPageFault - lastUsage.minorPageFault,
      proc_majflt: usage.majorPageFault - lastUsage.majorPageFault,
      proc_nvcsw: usage.voluntaryContextSwitches - lastUsage.voluntaryContextSwitches,
      proc_nivcsw: usage.involuntaryContextSwitches - lastUsage.involuntaryContextSwitches,
      rss_mib: round(memory.rss / 1048576),
      heap_used_mib: round(memory.heapUsed / 1048576),
      heap_total_mib: round(memory.heapTotal / 1048576),
      external_mib: round(memory.external / 1048576),
      array_buffers_mib: round(memory.arrayBuffers / 1048576),
      new_space_size_mib: newSpace ? round(newSpace.space_size / 1048576) : null,
      new_space_used_mib: newSpace ? round(newSpace.space_used_size / 1048576) : null,
      old_space_used_mib: oldSpace ? round(oldSpace.space_used_size / 1048576) : null,
    };
    delay.reset();
    gcSecond.clear();
    lastElu = elu;
    lastCpu = process.cpuUsage();
    lastUsage = usage;
    lastMono = mono;
    try {
      NodeFS.appendFileSync(out, `${JSON.stringify(record)}\n`);
    } catch {
      // The run directory is gone (run.ts cleaned up); nothing to record.
    }

    if (window === null && !windowDone && exists("window-start")) {
      const histogram = NodePerfHooks.monitorEventLoopDelay({ resolution: 10 });
      histogram.enable();
      window = { histogram, startedAt: Date.now(), elu0: elu, gc: new Map() };
    } else if (window !== null && exists("window-end")) {
      const eluWindow = NodePerfHooks.performance.eventLoopUtilization(elu, window.elu0);
      try {
        NodeFS.writeFileSync(
          NodePath.join(runDir, "loop-window.json"),
          `${JSON.stringify({
            schema: "t3-server-bench/loop-window/1",
            start: window.startedAt,
            end: Date.now(),
            resolution_ms: 10,
            delay: summary(window.histogram),
            elu: round(eluWindow.utilization),
            elu_active_ms: round(eluWindow.active),
            gc: gcObject(window.gc),
          })}\n`,
        );
      } catch {
        // ignore
      }
      window.histogram.disable();
      window = null;
      windowDone = true;
    }

    if (profiler === "idle" && exists("prof-start")) {
      profiler = "starting";
      session = new NodeInspector.Session();
      session.connect();
      session.post("Profiler.enable", () =>
        session.post("Profiler.setSamplingInterval", { interval: 1000 }, () =>
          session.post("Profiler.start", () => {
            profiler = "running";
            try {
              NodeFS.writeFileSync(NodePath.join(ctl, "prof-started"), String(Date.now()));
            } catch {
              // ignore
            }
          }),
        ),
      );
    } else if (profiler === "running" && exists("prof-stop")) {
      profiler = "stopping";
      session.post("Profiler.stop", (error, result) => {
        try {
          NodeFS.writeFileSync(
            NodePath.join(runDir, "window.cpuprofile"),
            error ? JSON.stringify({ error: String(error) }) : JSON.stringify(result.profile),
          );
          NodeFS.writeFileSync(NodePath.join(ctl, "prof-stopped"), String(Date.now()));
        } catch {
          // ignore
        }
        profiler = "done";
        session.disconnect();
      });
    }
  }, 1000).unref();
}

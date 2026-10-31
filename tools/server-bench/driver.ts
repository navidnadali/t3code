#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off globalFetch:off globalConsole:off preferSchemaOverJson:off - Standalone wall-clock load driver; it runs outside Effect on purpose.
/**
 * Bench load driver: against a running scratch server it keeps sessions streaming, holds
 * WebSocket subscribers and runs MCP/HTTP pollers at fixed rates, then writes `driver.json`
 * with the measured window's client-side latencies and event rates. A run that cannot finish
 * cleanly writes `driver.failed` and a result without any table.
 */
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodePerfHooks from "node:perf_hooks";

import { type DriverOptions, parseDriverOptions, USAGE, UsageError } from "./driver/options.ts";
import {
  CallError,
  classifyFetchError,
  FatalError,
  httpJson,
  type HttpTarget,
  McpSession,
  RpcSocket,
} from "./driver/protocol.ts";
import {
  compareDistributions,
  createRng,
  type DistributionMatch,
  pickMatching,
  type Rng,
  shuffled,
  summarizeDistribution,
} from "./driver/sampling.ts";
import { CredentialCallers, readCredentials } from "./driver/sessionMcp.ts";
import { Recorder } from "./driver/stats.ts";
import { judgeWindow, type WindowEvidence } from "./driver/verdict.ts";
import {
  newId,
  now,
  Poller,
  type RunContext,
  Session,
  ShellWatcher,
  type ShellSnapshot,
  Subscriber,
  type ThreadShell,
} from "./driver/clients.ts";

const RESULT_FILE = "driver.json";
const FAILED_MARKER = "driver.failed";
const SCHEMA_VERSION = 1;
const REQUIRED_SCOPES = ["orchestration:read", "orchestration:operate", "access:write"];

const log = (message: string) =>
  process.stderr.write(`[driver ${new Date().toISOString()}] ${message}\n`);

const delay = (milliseconds: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, milliseconds));

function git(args: ReadonlyArray<string>, cwd: string): string {
  return NodeChildProcess.execFileSync("git", args, { cwd, encoding: "utf8" }).trim();
}

/** An empty git repository: sessions check out and checkpoint here, never in fixture paths. */
function createWorkspace(directory: string): void {
  NodeFS.mkdirSync(directory, { recursive: true });
  if (NodeFS.existsSync(NodePath.join(directory, ".git"))) return;
  git(["init", "-q"], directory);
  git(
    [
      "-c",
      "user.name=Bench",
      "-c",
      "user.email=bench@localhost",
      "commit",
      "--allow-empty",
      "-qm",
      "Bench workspace",
    ],
    directory,
  );
}

function driverRevision(): { readonly sha: string | null; readonly dirty: boolean | null } {
  try {
    const directory = import.meta.dirname;
    return {
      sha: git(["rev-parse", "HEAD"], directory),
      dirty: git(["status", "--porcelain", "--", "."], directory).length > 0,
    };
  } catch {
    return { sha: null, dirty: null };
  }
}

function cpusAllowed(pid: number | "self"): string | null {
  try {
    const status = NodeFS.readFileSync(`/proc/${pid}/status`, "utf8");
    return /^Cpus_allowed_list:\s*(.+)$/mu.exec(status)?.[1]?.trim() ?? null;
  } catch {
    return null;
  }
}

function expandCpuList(list: string): Set<number> {
  const cpus = new Set<number>();
  for (const part of list.split(",")) {
    const [first, last] = part.split("-").map(Number);
    if (first === undefined || !Number.isInteger(first)) continue;
    for (let cpu = first; cpu <= (last ?? first); cpu++) cpus.add(cpu);
  }
  return cpus;
}

/** Pins every thread of this process, so the driver never competes with the server's cores. */
function applyAffinity(options: DriverOptions) {
  if (options.taskset !== null) {
    if (NodeOS.type() !== "Linux") throw new UsageError("--taskset needs Linux");
    try {
      NodeChildProcess.execFileSync(
        "taskset",
        ["-a", "-cp", options.taskset, String(process.pid)],
        { stdio: "ignore" },
      );
    } catch (error) {
      throw new UsageError(
        `taskset -a -cp ${options.taskset} failed: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  const driver = cpusAllowed("self");
  const server = options.serverPid === null ? null : cpusAllowed(options.serverPid);
  const overlap =
    driver !== null && server !== null
      ? [...expandCpuList(driver)].filter((cpu) => expandCpuList(server).has(cpu))
      : [];
  if (overlap.length > 0) {
    log(`warning: the driver shares CPUs ${overlap.join(",")} with the server`);
  }
  return { requested: options.taskset, driver, server, overlap };
}

function readToken(options: DriverOptions): string {
  const token =
    options.tokenFile === null
      ? process.env.T3_BENCH_TOKEN
      : NodeFS.readFileSync(options.tokenFile, "utf8");
  if (token === undefined || token.trim().length === 0) {
    throw new UsageError("Set T3_BENCH_TOKEN or pass --token-file");
  }
  return token.trim();
}

/** Signs a read-only MCP client in through the server's OAuth flow with a one-time pairing code. */
async function mintMcpToken(http: HttpTarget, label: string): Promise<string> {
  const pairing = await httpJson<{ credential: string }>(http, "/api/auth/pairing-token", {
    body: JSON.stringify({ label, scopes: ["orchestration:read"] }),
  });
  const post = async <T>(
    path: string,
    init: { body: string | URLSearchParams; json?: boolean },
  ) => {
    try {
      const response = await fetch(`${http.origin}${path}`, {
        method: "POST",
        headers: init.json === true ? { "content-type": "application/json" } : {},
        body: init.body,
        signal: AbortSignal.timeout(http.timeoutMs),
      });
      const text = await response.text();
      if (!response.ok) throw new CallError(`${path} answered ${response.status}: ${text}`);
      return JSON.parse(text) as T;
    } catch (error) {
      throw classifyFetchError(error, path);
    }
  };
  const redirectUri = "http://127.0.0.1/bench-driver-callback";
  const client = await post<{ client_id: string }>("/oauth/mcp/register", {
    json: true,
    body: JSON.stringify({ client_name: label, redirect_uris: [redirectUri] }),
  });
  const verifier = NodeCrypto.randomBytes(32).toString("base64url");
  const decision = await post<{ redirectTo: string }>("/oauth/mcp/decision", {
    json: true,
    body: JSON.stringify({
      authorization: {
        response_type: "code",
        client_id: client.client_id,
        redirect_uri: redirectUri,
        code_challenge: NodeCrypto.createHash("sha256").update(verifier).digest("base64url"),
        code_challenge_method: "S256",
      },
      decision: { _tag: "pairing-code", access: "read-only", code: pairing.value.credential },
    }),
  });
  const code = new URL(decision.redirectTo).searchParams.get("code");
  if (code === null) throw new CallError(`MCP sign-in was refused: ${decision.redirectTo}`);
  const token = await post<{ access_token: string }>("/oauth/mcp/token", {
    body: new URLSearchParams({
      grant_type: "authorization_code",
      code,
      redirect_uri: redirectUri,
      client_id: client.client_id,
      code_verifier: verifier,
    }),
  });
  return token.access_token;
}

interface Workload {
  readonly subscriberPool: ReadonlyArray<string>;
  readonly readPool: ReadonlyArray<string>;
  readonly details: Record<string, unknown>;
  /** History mode: the fixture's turn items per thread, keyed by thread. */
  readonly itemCounts: ReadonlyMap<string, number> | null;
}

interface Failure {
  readonly phase: string;
  readonly reason: string;
  readonly errors?: Record<string, unknown>;
}

class BenchRun implements RunContext {
  readonly options: DriverOptions;
  readonly http: HttpTarget;
  readonly recorder = new Recorder();
  private readonly rng: Rng;
  private phase = "setup";
  private stopping = false;
  private failure: Failure | null = null;
  private serverLost = false;
  private readonly fatal = Promise.withResolvers<never>();
  private readonly sockets: Array<RpcSocket> = [];
  /** Every session, registered before it sends anything, so teardown can stop it. */
  private readonly sessions: Array<Session> = [];
  /** Setup work that may still be in flight when the run ends early. */
  private readonly pendingWork = new Set<Promise<void>>();
  /** Sessions' agents calling their MCP tools, each signed in as its own MCP client. */
  private readonly clientCallers: Array<Poller> = [];
  /** Sessions' agents calling their MCP tools with the credentials their providers publish. */
  private credentialCallers: CredentialCallers<Poller> | null = null;
  private sessionCallersCreated = 0;
  private sessionCallersAtWindowStart = 0;
  private readonly startedAtEpochMs = Date.now();
  private readonly timeline: Record<string, string> = {};
  /** Keeps each fresh run's workspaces apart; a folder can back only one project. */
  private readonly runId = NodeCrypto.randomBytes(4).toString("hex");

  constructor(options: DriverOptions, token: string, protocolVersion: number) {
    this.options = options;
    this.http = {
      origin: options.url,
      token,
      timeoutMs: options.callTimeoutMs,
      protocolVersion,
    };
    this.rng = createRng(options.seed);
    this.fatal.promise.catch(() => undefined);
  }

  isStopping = () => this.stopping;

  abort = (error: FatalError) => {
    if (this.failure !== null) return;
    this.failure = { phase: this.phase, reason: error.message };
    this.serverLost = error.serverLost;
    this.stopping = true;
    this.recorder.setOpen(false);
    log(`run failed during ${this.phase}: ${error.message}`);
    this.fatal.reject(error);
  };

  private race<T>(promise: Promise<T>): Promise<T> {
    return Promise.race([promise, this.fatal.promise]);
  }

  /**
   * Runs setup steps `--setup-concurrency` at a time. After the first failure (or once the run
   * is stopping) no new step starts; the promise settles only when every started step has.
   */
  private parallel<T>(
    items: ReadonlyArray<T>,
    run: (item: T, index: number) => Promise<void>,
  ): Promise<void> {
    let next = 0;
    let failure: { readonly error: unknown } | null = null;
    const lanes = Array.from(
      { length: Math.min(this.options.setupConcurrency, items.length) },
      async () => {
        while (next < items.length && failure === null && !this.stopping) {
          const index = next++;
          try {
            await run(items[index] as T, index);
          } catch (error) {
            failure ??= { error };
          }
        }
      },
    );
    const settled = Promise.all(lanes).then(() => {
      if (failure !== null) throw failure.error;
    });
    this.pendingWork.add(settled.catch(() => undefined));
    return settled;
  }

  private mark(phase: string): void {
    this.phase = phase;
    this.timeline[phase] = new Date().toISOString();
    log(`phase: ${phase}`);
  }

  openSocket = async (): Promise<RpcSocket> => {
    const ticket = await httpJson<{ ticket: string }>(this.http, "/api/auth/websocket-ticket", {
      method: "POST",
    });
    const url = new URL("/ws", this.options.url);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("orchestrationProtocol", String(this.http.protocolVersion));
    url.searchParams.set("wsTicket", ticket.value.ticket);
    const socket = await RpcSocket.open(url.href, this.options.callTimeoutMs);
    socket.onConnectionLost((error) => {
      if (!this.stopping) this.abort(error);
    });
    this.sockets.push(socket);
    return socket;
  };

  /** A dead server process or a socket that stops answering pings ends the run at once. */
  private watchServer(): ReturnType<typeof setInterval> {
    const pid = this.options.serverPid;
    return setInterval(() => {
      if (pid !== null) {
        try {
          process.kill(pid, 0);
        } catch (error) {
          if ((error as { code?: unknown }).code === "ESRCH") {
            this.abort(new FatalError(`server process ${pid} exited`, { serverLost: true }));
          }
        }
      }
      const at = now();
      const silent = Math.max(0, ...this.sockets.map((socket) => socket.silentFor(at)));
      if (silent > this.options.stallTimeoutMs) {
        this.abort(
          new FatalError(`the server left a ping unanswered for ${Math.round(silent / 1000)}s`, {
            serverLost: true,
          }),
        );
      }
    }, 250);
  }

  /** The window only counts if the server is still there once it closes. */
  private async confirmServerAlive(): Promise<void> {
    try {
      await httpJson(this.http, "/.well-known/t3/environment");
    } catch (error) {
      if (error instanceof FatalError) throw error;
      throw new FatalError(
        `the server did not answer after the window: ${error instanceof Error ? error.message : String(error)}`,
        { serverLost: true },
      );
    }
  }

  private async checkScopes(): Promise<void> {
    const session = await httpJson<{ authenticated: boolean; scopes?: ReadonlyArray<string> }>(
      this.http,
      "/api/auth/session",
    );
    const missing = REQUIRED_SCOPES.filter((scope) => !session.value.scopes?.includes(scope));
    if (!session.value.authenticated || missing.length > 0) {
      throw new FatalError(
        `The bench token is not accepted or lacks scopes: ${missing.join(", ") || "unauthenticated"}`,
      );
    }
  }

  private workspaceDir(): string {
    return NodePath.resolve(
      this.options.workspaceDir ?? NodePath.join(this.options.out, "workspaces"),
    );
  }

  private async mutateProject(body: Record<string, unknown>): Promise<void> {
    await httpJson(this.http, "/api/projects/mutate", {
      body: JSON.stringify({ commandId: newId(), ...body }),
    });
  }

  private sessionMcpMode(): "off" | "provider-credentials" | "client-oauth" {
    if (this.options.sessions === 0 || this.options.sessionMcpRatePerMinute === 0) return "off";
    return this.options.sessionMcpCredentials === null ? "client-oauth" : "provider-credentials";
  }

  private sessionCallers(): Array<Poller> {
    return [...this.clientCallers, ...(this.credentialCallers?.callers() ?? [])];
  }

  private makeSessionCaller(mcp: McpSession, pool: ReadonlyArray<string>): Poller {
    const index = this.sessionCallersCreated++;
    const mix = this.options.pollMix.filter(
      ([call, weight]) => call.startsWith("mcp.") && weight > 0,
    );
    return new Poller({
      index,
      context: this,
      mcp,
      pool,
      rng: this.rng.fork(`session-mcp/${index}`),
      prefix: "session",
      ratePerSecond: this.options.sessionMcpRatePerMinute / 60,
      mix: mix.length > 0 ? mix : [["mcp.t3_thread_read", 1]],
    });
  }

  /** Gives each provider session that publishes a credential its own caller, signed in with it. */
  private followSessionCredentials(pool: ReadonlyArray<string>): ReturnType<typeof setInterval> {
    const directory = this.options.sessionMcpCredentials as string;
    const callers = new CredentialCallers<Poller>(
      this.options.sessions,
      async (name, credential) => {
        const mcp = new McpSession(
          credential.url,
          credential.authorization,
          this.options.callTimeoutMs,
        );
        try {
          await mcp.initialize(`bench-session-${name}`);
        } catch (error) {
          log(
            `session MCP sign-in for ${name} failed: ${error instanceof Error ? error.message : String(error)}`,
          );
          throw error;
        }
        return this.makeSessionCaller(mcp, pool);
      },
    );
    this.credentialCallers = callers;
    const tick = () => {
      if (!this.stopping) callers.update(readCredentials(directory, this.startedAtEpochMs));
    };
    tick();
    return setInterval(tick, 1000);
  }

  private async prepareFresh(socket: RpcSocket, watcher: ShellWatcher): Promise<Workload> {
    const { options } = this;
    const projectCount = Math.max(1, Math.min(options.projects, options.sessions));
    const projectIds: Array<string> = [];
    for (let index = 0; index < projectCount; index++) {
      const directory = NodePath.join(this.workspaceDir(), `fresh-${this.runId}-${index}`);
      createWorkspace(directory);
      const projectId = newId();
      await this.mutateProject({
        type: "project.create",
        projectId,
        title: `Bench project ${index}`,
        workspaceRoot: directory,
      });
      projectIds.push(projectId);
    }
    const sessionRng = this.rng.fork("sessions");
    for (let index = 0; index < options.sessions; index++) {
      this.sessions.push(
        new Session({
          index,
          threadId: newId(),
          context: this,
          socket,
          watcher,
          rng: sessionRng.fork(String(index)),
          switchProvider: false,
        }),
      );
    }
    await this.parallel(this.sessions, (session, index) =>
      session.launch(projectIds[index % projectIds.length] as string, `Bench session ${index}`),
    );
    const threadIds = this.sessions.map((session) => session.threadId);
    return {
      subscriberPool: threadIds,
      readPool: threadIds,
      details: { projects: projectIds.length },
      itemCounts: null,
    };
  }

  private async prepareHistory(
    snapshot: ShellSnapshot,
    socket: RpcSocket,
    watcher: ShellWatcher,
  ): Promise<Workload> {
    const { options } = this;
    const projects = new Set(snapshot.projects.map((project) => project.id));
    const all = [...snapshot.threads, ...(snapshot.archivedThreads ?? [])];
    const population = all.filter((thread) => thread.itemCount >= 1);
    if (population.length === 0) {
      throw new FatalError("history mode needs a fixture with threads that have turn items");
    }
    const target = population.map((thread) => thread.itemCount);
    const weighted = (thread: ThreadShell) => ({ id: thread.id, value: thread.itemCount, thread });
    const candidates = snapshot.threads.filter(
      (thread) =>
        thread.itemCount >= 1 &&
        thread.archivedAt === null &&
        thread.worktreePath === null &&
        thread.activeRunId === null &&
        thread.pendingRuntimeRequest === null &&
        thread.lineage.relationshipToParent !== "subagent" &&
        thread.lineage.sideChat !== true &&
        projects.has(thread.projectId),
    );
    if (candidates.length < options.sessions) {
      throw new FatalError(
        `history mode needs ${options.sessions} idle root-workspace threads; the fixture has ${candidates.length}`,
      );
    }
    const chosen = shuffled(
      this.rng.fork("assignment"),
      pickMatching(this.rng.fork("sessions"), candidates.map(weighted), target, options.sessions),
    ).map((entry) => entry.thread);
    const readPool = pickMatching(
      this.rng.fork("reads"),
      population.map(weighted),
      target,
      Math.min(options.readPool, population.length),
    ).map((entry) => entry.id);
    const itemCounts = new Map(all.map((thread) => [thread.id, thread.itemCount]));
    const planned = compareDistributions(
      summarizeDistribution(target),
      summarizeDistribution(
        [...new Set([...chosen.map((thread) => thread.id), ...readPool])].map(
          (id) => itemCounts.get(id) ?? 0,
        ),
      ),
      options.distributionTolerance,
    );
    if (!planned.withinTolerance) {
      throw new FatalError(
        `the fixture's eligible threads cannot match its turn-item distribution (p50 ratio ${planned.p50Ratio}, p95 ratio ${planned.p95Ratio}); raise --read-pool or --distribution-tolerance`,
      );
    }

    // Turns check out and checkpoint in the project root: point each at a scratch repository.
    const rerooted = [...new Set(chosen.map((thread) => thread.projectId))];
    await this.parallel(rerooted, async (projectId) => {
      const directory = NodePath.join(this.workspaceDir(), projectId.replaceAll(/[^\w-]/gu, "_"));
      createWorkspace(directory);
      await this.mutateProject({ type: "project.update", projectId, workspaceRoot: directory });
    });
    let modeChanges = 0;
    await this.parallel(chosen, async (thread) => {
      const commands: Array<Record<string, unknown>> = [];
      if (thread.runtimeMode !== options.runtimeMode) {
        commands.push({ type: "thread.runtime-mode.set", runtimeMode: options.runtimeMode });
      }
      if (thread.interactionMode !== "default") {
        commands.push({ type: "thread.interaction-mode.set", interactionMode: "default" });
      }
      for (const command of commands) {
        await socket.call(
          "orchestration.dispatchCommand",
          { ...command, commandId: newId(), threadId: thread.id },
          options.callTimeoutMs,
        );
        modeChanges++;
      }
    });
    const sessionRng = this.rng.fork("session-text");
    for (const [index, thread] of chosen.entries()) {
      this.sessions.push(
        new Session({
          index,
          threadId: thread.id,
          context: this,
          socket,
          watcher,
          rng: sessionRng.fork(String(index)),
          switchProvider: thread.modelSelection.instanceId !== options.provider,
        }),
      );
    }
    return {
      subscriberPool: chosen.length > 0 ? chosen.map((thread) => thread.id) : readPool,
      readPool,
      details: {
        populationThreads: population.length,
        emptyThreadsExcluded: all.length - population.length,
        sessionCandidates: candidates.length,
        rerootedProjects: rerooted.length,
        modeChanges,
        providerSwitches: chosen.filter(
          (thread) => thread.modelSelection.instanceId !== options.provider,
        ).length,
        sessionThreadIds: chosen.map((thread) => thread.id),
      },
      itemCounts,
    };
  }

  private distribution(
    workload: Workload,
    watcher: ShellWatcher,
    subscribers: ReadonlyArray<Subscriber>,
    pollers: ReadonlyArray<Poller>,
  ) {
    const sessionIds = this.sessions.map((session) => session.threadId);
    if (workload.itemCounts === null) {
      return {
        sessionsAtWindowEnd: summarizeDistribution(
          sessionIds.map((id) => watcher.get(id)?.itemCount ?? 0),
        ),
        match: null,
      };
    }
    const counts = workload.itemCounts;
    const of = (ids: Iterable<string>) =>
      summarizeDistribution([...ids].map((id) => counts.get(id) ?? 0));
    const polled = new Set(pollers.flatMap((poller) => [...poller.touched]));
    const subscribed = new Set(subscribers.flatMap((subscriber) => [...subscriber.touched]));
    const fixture = summarizeDistribution([...counts.values()].filter((count) => count >= 1));
    const exercised = of(new Set([...sessionIds, ...polled, ...subscribed]));
    return {
      fixture,
      exercised,
      sessions: of(sessionIds),
      readPool: of(workload.readPool),
      polled: of(polled),
      subscribed: of(subscribed),
      match: compareDistributions(fixture, exercised, this.options.distributionTolerance),
    };
  }

  async execute(server: Record<string, unknown>) {
    const { options, recorder, sessions } = this;
    const subscribers: Array<Subscriber> = [];
    const pollers: Array<Poller> = [];
    const watchdog = this.watchServer();
    let credentialWatch: ReturnType<typeof setInterval> | undefined;
    let measured: Record<string, unknown> | null = null;
    this.mark("setup");
    try {
      await this.race(this.checkScopes());
      const snapshot = await this.race(
        httpJson<ShellSnapshot>(this.http, "/api/orchestration/shell"),
      );
      const control = await this.race(this.openSocket());
      const watcher = new ShellWatcher();
      watcher.seed(snapshot.value);
      watcher.follow(this, control, snapshot.value.snapshotSequence);
      const workload = await this.race(
        options.mode === "history"
          ? this.prepareHistory(snapshot.value, control, watcher)
          : this.prepareFresh(control, watcher),
      );
      for (const session of sessions) session.watch();
      log(`sessions ready: ${sessions.length}`);

      const subscriberRng = this.rng.fork("subscribers");
      subscribers.push(
        ...Array.from(
          { length: options.subscribers },
          (_, index) =>
            new Subscriber(index, this, workload.subscriberPool, subscriberRng.fork(String(index))),
        ),
      );
      await this.race(this.parallel(subscribers, (subscriber) => subscriber.open()));
      log(`subscribers ready: ${subscribers.length}`);

      const usesMcp = options.pollMix.some(
        ([call, weight]) => weight > 0 && call.startsWith("mcp."),
      );
      const mcpToken =
        options.pollers > 0 && usesMcp
          ? await this.race(mintMcpToken(this.http, "Bench pollers"))
          : null;
      const pollerRng = this.rng.fork("pollers");
      const mcpSessions = Array.from({ length: options.pollers }, () =>
        mcpToken === null
          ? null
          : new McpSession(`${options.url}/mcp`, `Bearer ${mcpToken}`, options.callTimeoutMs),
      );
      await this.race(
        this.parallel(mcpSessions, async (session, index) => {
          await session?.initialize(`bench-poller-${index}`);
        }),
      );
      pollers.push(
        ...mcpSessions.map(
          (mcp, index) =>
            new Poller({
              index,
              context: this,
              mcp,
              pool: workload.readPool,
              rng: pollerRng.fork(String(index)),
              prefix: "poll",
              ratePerSecond: options.pollRate,
              mix: options.pollMix,
            }),
        ),
      );
      log(`pollers ready: ${pollers.length}`);
      const sessionMcpMode = this.sessionMcpMode();
      if (sessionMcpMode === "client-oauth") {
        await this.race(
          this.parallel(sessions, async (_, index) => {
            const token = await mintMcpToken(this.http, `Bench session ${index}`);
            const mcp = new McpSession(
              `${options.url}/mcp`,
              `Bearer ${token}`,
              options.callTimeoutMs,
            );
            await mcp.initialize(`bench-session-${index}`);
            this.clientCallers.push(this.makeSessionCaller(mcp, workload.readPool));
          }),
        );
      }

      this.mark("warmup");
      if (options.mode === "history") for (const session of sessions) session.start();
      for (const subscriber of subscribers) subscriber.startSwitching();
      for (const poller of pollers) poller.start();
      for (const caller of this.clientCallers) caller.start();
      credentialWatch =
        sessionMcpMode === "provider-credentials"
          ? this.followSessionCredentials(workload.readPool)
          : undefined;
      await this.race(delay(options.warmupMs));

      this.mark("window");
      const eluStart = NodePerfHooks.performance.eventLoopUtilization();
      const cpuStart = process.cpuUsage();
      const windowStart = now();
      recorder.setOpen(true);
      for (const client of [...sessions, ...subscribers]) client.windowOpened();
      this.sessionCallersAtWindowStart = this.sessionCallers().length;
      await this.race(delay(options.windowMs));
      recorder.setOpen(false);
      const windowSeconds = (now() - windowStart) / 1000;
      const elu = NodePerfHooks.performance.eventLoopUtilization(eluStart);
      const cpu = process.cpuUsage(cpuStart);
      this.mark("measured");
      const summary = recorder.summarize(windowSeconds);
      await this.race(this.confirmServerAlive());
      measured = {
        windowSeconds: Math.round(windowSeconds * 1000) / 1000,
        ...summary,
        sessions: {
          count: sessions.length,
          ownerStreams: sessions.length,
          withOutputInWindow: sessions.filter((session) => session.producedOutput).length,
          inFlightAtWindowEnd: sessions.filter((session) => session.inFlight).length,
        },
        workload: workload.details,
        sessionMcp: {
          mode: sessionMcpMode,
          ratePerMinute: options.sessionMcpRatePerMinute,
          callersAtWindowStart: this.sessionCallersAtWindowStart,
        },
        distribution: this.distribution(workload, watcher, subscribers, [
          ...pollers,
          ...this.sessionCallers(),
        ]),
        driverHealth: {
          eventLoopUtilization: Math.round(elu.utilization * 1000) / 1000,
          cpuPercent: Math.round((cpu.user + cpu.system) / windowSeconds / 1000) / 10,
          saturated: elu.utilization > 0.9,
        },
      };
      if (elu.utilization > 0.9) {
        log(
          "warning: the driver's event loop was saturated; client latencies include driver delay",
        );
      }
      this.validate(measured, {
        worstErrorRate: recorder.worstErrorRate(),
        maxErrorRate: options.maxErrorRate,
        sessions: sessions.length,
        sessionsWithOutput: sessions.filter((session) => session.producedOutput).length,
        distribution: (measured.distribution as { match: DistributionMatch | null }).match,
        sessionMcpRequired: sessionMcpMode === "off" ? 0 : sessions.length,
        sessionMcpCallers: this.sessionCallersAtWindowStart,
      });
    } catch (error) {
      if (this.failure === null) {
        const reason = error instanceof Error ? error.message : String(error);
        this.failure = { phase: this.phase, reason };
        log(`run failed during ${this.phase}: ${reason}`);
      }
    } finally {
      this.stopping = true;
      clearInterval(watchdog);
      clearInterval(credentialWatch);
      this.mark("teardown");
      if (this.serverLost) {
        for (const caller of [...pollers, ...this.clientCallers]) void caller.stop(false);
        void this.credentialCallers?.close(false);
      } else {
        // A launch still in flight would start a run after the stop below.
        await Promise.race([Promise.all(this.pendingWork), delay(options.callTimeoutMs)]);
        await Promise.race([
          Promise.all([
            ...pollers.map((poller) => poller.stop(true)),
            ...this.clientCallers.map((caller) => caller.stop(true)),
            this.credentialCallers?.close(true),
            ...sessions.map((session) => session.stop(options.stopSessions)),
          ]),
          delay(15_000),
        ]);
      }
      for (const subscriber of subscribers) subscriber.stop();
      for (const socket of this.sockets) socket.close();
    }
    return this.result(server, measured);
  }

  /** A complete window is published only if `judgeWindow` accepts it. */
  private validate(measured: Record<string, unknown>, evidence: WindowEvidence): void {
    const reason = judgeWindow(evidence);
    if (reason === null) return;
    const calls = measured.calls as Record<
      string,
      { errors: number; count: number; errorSamples: unknown }
    >;
    const errors = Object.fromEntries(
      Object.entries(calls)
        .filter(([, summary]) => summary.errors > 0)
        .map(([type, summary]) => [
          type,
          { errors: summary.errors, succeeded: summary.count, samples: summary.errorSamples },
        ]),
    );
    this.failure = { phase: "window", reason, errors };
  }

  private result(server: Record<string, unknown>, measured: Record<string, unknown> | null) {
    const { options } = this;
    const base = {
      schemaVersion: SCHEMA_VERSION,
      status: this.failure === null ? "ok" : "failed",
      driver: {
        ...driverRevision(),
        node: process.version,
        platform: `${NodeOS.type()}-${NodeOS.machine()}`,
        host: NodeOS.hostname(),
        pid: process.pid,
      },
      server,
      knobs: {
        ...options,
        tokenFile: options.tokenFile,
        pollMix: Object.fromEntries(options.pollMix),
      },
      timeline: this.timeline,
    };
    if (this.failure !== null) return { ...base, failure: this.failure };
    return { ...base, ...measured };
  }
}

function writeResult(out: string, result: { readonly status: string }): void {
  const write = (file: string, content: string) => {
    const temporary = NodePath.join(out, `.${file}.${process.pid}.tmp`);
    NodeFS.writeFileSync(temporary, content);
    NodeFS.renameSync(temporary, NodePath.join(out, file));
  };
  if (result.status !== "ok") {
    write(FAILED_MARKER, `${JSON.stringify((result as { failure?: unknown }).failure)}\n`);
  }
  write(RESULT_FILE, `${JSON.stringify(result, null, 2)}\n`);
}

async function describeServer(options: DriverOptions) {
  const http = {
    origin: options.url,
    token: "",
    timeoutMs: options.callTimeoutMs,
    protocolVersion: 0,
  };
  const descriptor = await httpJson<{
    serverVersion?: string;
    orchestrationProtocolVersion?: number;
    databaseMigration?: number;
  }>(http, "/.well-known/t3/environment");
  return {
    url: options.url,
    pid: options.serverPid,
    serverVersion: descriptor.value.serverVersion ?? null,
    orchestrationProtocolVersion: descriptor.value.orchestrationProtocolVersion ?? 2,
    databaseMigration: descriptor.value.databaseMigration ?? null,
  };
}

async function main(): Promise<number> {
  let options: DriverOptions;
  let token: string;
  let affinity: ReturnType<typeof applyAffinity>;
  try {
    options = parseDriverOptions(process.argv.slice(2));
    token = readToken(options);
    affinity = applyAffinity(options);
  } catch (error) {
    if (!(error instanceof UsageError)) throw error;
    if (error.help) {
      process.stdout.write(USAGE);
      return 0;
    }
    process.stderr.write(`${error.message}\n\n${USAGE}`);
    return 2;
  }
  NodeFS.mkdirSync(options.out, { recursive: true });
  for (const file of [RESULT_FILE, FAILED_MARKER]) {
    NodeFS.rmSync(NodePath.join(options.out, file), { force: true });
  }
  let server: Awaited<ReturnType<typeof describeServer>>;
  try {
    server = await describeServer(options);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    log(`run failed during setup: ${reason}`);
    writeResult(options.out, {
      schemaVersion: SCHEMA_VERSION,
      status: "failed",
      failure: { phase: "setup", reason },
    } as { status: string });
    return 1;
  }
  const run = new BenchRun(options, token, server.orchestrationProtocolVersion);
  const interrupt = (signal: string) => run.abort(new FatalError(`interrupted by ${signal}`));
  process.once("SIGINT", () => interrupt("SIGINT"));
  process.once("SIGTERM", () => interrupt("SIGTERM"));
  // A driver bug must still end in a failed run, never in a crash that leaves no marker.
  const crashed = (error: unknown) =>
    run.abort(
      new FatalError(
        `driver error: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`,
      ),
    );
  process.on("uncaughtException", crashed);
  process.on("unhandledRejection", crashed);
  const result = await run.execute({ ...server, cpuAffinity: affinity });
  writeResult(options.out, result);
  log(
    `${result.status === "ok" ? "wrote" : "failed run marker written beside"} ${NodePath.join(options.out, RESULT_FILE)}`,
  );
  return result.status === "ok" ? 0 : 1;
}

process.exit(await main());

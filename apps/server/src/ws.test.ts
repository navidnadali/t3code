import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentId,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  TurnItemId,
  ProviderDriverKind,
  ProviderInstanceId,
  WS_METHODS,
  WsServerRefreshProvidersRpc,
  WsSubscribeServerConfigRpc,
  OrchestrationV2RpcSchemas,
  type OrchestrationV2ShellStreamItem,
  type OrchestrationV2StoredEvent,
  type OrchestrationV2ThreadProjection,
  OrchestrationV2ThreadShell,
  type OrchestrationV2ThreadStreamItem,
  type OrchestrationProjectShell,
  type RepositoryIdentity,
  type ServerConfig,
  type ServerConfigStreamEvent,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { Headers } from "effect/http";
import { RpcGroup, RpcMessage, RpcServer } from "effect/rpc";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
import { vi } from "vite-plus/test";

import * as PreviewAutomationBroker from "./mcp/PreviewAutomationBroker.ts";
import * as ServerBrowser from "./preview/ServerBrowser.ts";
import * as AcpRegistryRuntimeCoordinator from "@t3tools/provider-acp-registry/server/AcpRegistryRuntimeCoordinator";
import * as AcpRegistrySupport from "@t3tools/provider-acp-registry/server/AcpRegistrySupport";
import * as AgentSessionImporter from "./project/AgentSessionImporter.ts";
import * as AgentSessionScanner from "./project/AgentSessionScanner.ts";
import * as AnalyticsService from "./telemetry/AnalyticsService.ts";
import * as AntigravityInstallation from "./provider/AntigravityInstallation.ts";
import * as BackgroundPolicy from "./background/BackgroundPolicy.ts";
import * as CheckpointDiffQuery from "./checkpointing/CheckpointDiffQuery.ts";
import * as NativeChildWorkQuery from "./orchestration-v2/NativeChildWorkQuery.ts";
import * as CommandOutputHub from "./orchestration-v2/CommandOutputHub.ts";
import * as CommandReceiptStore from "./orchestration-v2/CommandReceiptStore.ts";
import * as CodexInstallation from "./provider/CodexInstallation.ts";
import * as DeviceService from "./device/DeviceService.ts";
import * as DocumentText from "./provider/DocumentText.ts";
import * as DirectEndpoints from "./environment/DirectEndpoints.ts";
import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import * as EnvironmentTheme from "./environmentTheme.ts";
import * as GitWorkflowService from "./git/GitWorkflowService.ts";
import * as HostResources from "./resourceTelemetry/HostResources.ts";
import * as ManagedProjectFolders from "./project/ManagedProjectFolders.ts";
import * as ScratchThreadPromotion from "./project/ScratchThreadPromotion.ts";
import * as McpAppRequests from "./mcpApps/McpAppRequests.ts";
import * as ModelManifest from "./provider/ModelManifest.ts";
import * as Orchestrator from "./orchestration-v2/Orchestrator.ts";
import * as OtlpEndpointCheck from "./observability/OtlpEndpointCheck.ts";
import * as PairingGrantStore from "./auth/PairingGrantStore.ts";
import * as PeerLinkRequests from "./peer/PeerLinkRequests.ts";
import * as PeerLinks from "./peer/PeerLinks.ts";
import * as PortScanner from "./preview/PortScanner.ts";
import * as PreviewManager from "./preview/Manager.ts";
import * as ProcessDiagnostics from "./diagnostics/ProcessDiagnostics.ts";
import * as ProcessResourceMonitor from "./diagnostics/ProcessResourceMonitor.ts";
import * as ProjectCloneTracker from "./project/ProjectCloneTracker.ts";
import * as ProjectSetupScriptRunner from "./project/ProjectSetupScriptRunner.ts";
import * as ProviderAuthService from "./provider/ProviderAuthService.ts";
import * as ProviderInstanceRegistry from "./provider/ProviderInstanceRegistry.ts";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as ProviderMaintenanceRunner from "./provider/providerMaintenanceRunner.ts";
import * as ProviderRegistry from "./provider/ProviderRegistry.ts";
import * as ProviderSessionManager from "./orchestration-v2/ProviderSessionManager.ts";
import * as PullRequestReviewGuideService from "./pullRequest/PullRequestReviewGuideService.ts";
import * as PullRequestService from "./pullRequest/PullRequestService.ts";
import * as PullRequestSyncReactor from "./orchestration-v2/PullRequestSyncReactor.ts";
import * as RelayClient from "@t3tools/shared/relayClient";
import * as RemoteOpenTargets from "./environment/RemoteOpenTargets.ts";
import * as RepositoryIdentityResolver from "./project/RepositoryIdentityResolver.ts";
import * as ResourceTelemetry from "./resourceTelemetry/ResourceTelemetry.ts";
import * as ReviewService from "./review/ReviewService.ts";
import * as ScheduledTasks from "./scheduledTasks/ScheduledTaskService.ts";
import * as SecretRequests from "./secrets/SecretRequests.ts";
import * as GuestShareService from "./guest/GuestShareService.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import * as ServerLifecycleEvents from "./serverLifecycleEvents.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import * as ServerSelfUpdate from "./cloud/selfUpdate.ts";
import * as NpmRelease from "./cloud/npmRelease.ts";
import * as SessionStore from "./auth/SessionStore.ts";
import * as SourceControlDiscovery from "./sourceControl/SourceControlDiscovery.ts";
import * as SourceControlRepositoryService from "./sourceControl/SourceControlRepositoryService.ts";
import * as SourceControlSignIn from "./sourceControl/SourceControlSignIn.ts";
import * as StorageCleanup from "./storageCleanup.ts";
import * as DatabaseStorage from "./persistence/DatabaseStorage.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as ThreadLaunchService from "./orchestration-v2/ThreadLaunchService.ts";
import * as ThreadSearch from "./orchestration-v2/ThreadSearch.ts";
import * as UsageLimitSources from "./usage/UsageLimitSources.ts";
import * as UsageService from "./usage/UsageService.ts";
import * as VcsProvisioningService from "./vcs/VcsProvisioningService.ts";
import * as VcsStatusBroadcaster from "./vcs/VcsStatusBroadcaster.ts";
import * as WorkspaceEntries from "./workspace/WorkspaceEntries.ts";
import * as WorkspaceFileSystem from "./workspace/WorkspaceFileSystem.ts";
import * as WorktreeSetupTracker from "./project/WorktreeSetupTracker.ts";
import * as ProjectFaviconResolver from "./project/ProjectFaviconResolver.ts";
import * as TraceDiagnostics from "./diagnostics/TraceDiagnostics.ts";
import * as TriageService from "./triage/TriageService.ts";
import * as WorkspacePaths from "./workspace/WorkspacePaths.ts";
import * as ServerSecretStore from "./auth/ServerSecretStore.ts";
import * as ServerConfigModule from "./config.ts";
import * as Keybindings from "./keybindings.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as ManagedPolicy from "./policy/ManagedPolicy.ts";

import * as EventStore from "./orchestration-v2/EventStore.ts";
import { LIVE_STREAM_MAX_ITEMS } from "./orchestration-v2/LiveStreamBudget.ts";
import * as ProjectionMaintenance from "./orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "./orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "./orchestration-v2/ThreadManagementService.ts";
import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import * as ShellDeltaHub from "./orchestration-v2/ShellDeltaHub.ts";
import { UNCHANGED_THREAD_SHELL_RESEND_MS } from "./orchestration-v2/ShellStream.ts";
import * as OrchestrationEventStore from "./persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "./persistence/Sqlite.ts";
import * as ExternalLauncher from "./process/externalLauncher.ts";
import * as ProjectEnrichment from "./project/ProjectEnrichmentService.ts";
import * as ProjectService from "./project/ProjectService.ts";
import {
  encodeShellStreamItem,
  encodeThreadStreamItem,
  layerWsRpc,
  resolveOpenDiscoveryForConfig,
  subscribeOrchestrationV2Shell,
  subscribeOrchestrationV2Thread,
  WS_RPC_SERVER_OPTIONS,
  withLateConfigDiscovery,
} from "./ws.ts";

const snapshotConfig = (fields: Partial<ServerConfig>) =>
  ({ availableEditors: [], settings: {}, ...fields }) as unknown as ServerConfig;

const settingsUpdated = (settings: object): ServerConfigStreamEvent => ({
  version: 1,
  type: "settingsUpdated",
  payload: { settings: settings as ServerConfig["settings"] },
});

it.effect("resends late editors without rolling back updates already sent", () =>
  Effect.gen(function* () {
    const settingsSent = yield* Deferred.make<void>();
    const events = yield* withLateConfigDiscovery(
      snapshotConfig({ settings: { enableProviderUpdateChecks: true } as never }),
      Stream.make(settingsUpdated({ enableProviderUpdateChecks: false })),
      {
        resolveAvailableEditors: () => Effect.succeed(["file-manager"]),
        resolveFileManagerRevealKind: () =>
          Deferred.await(settingsSent).pipe(Effect.as("file-explorer" as const)),
      },
      { resolveTargets: () => Effect.succeed([]) },
      { resolve: () => Effect.succeed([]) },
    ).pipe(
      Stream.tap((event) =>
        event.type === "settingsUpdated" ? Deferred.succeed(settingsSent, undefined) : Effect.void,
      ),
      Stream.runCollect,
    );

    const [first, second] = Array.from(events);
    assert.equal(events.length, 2);
    assert.equal(first?.type, "settingsUpdated");
    assert.equal(second?.type, "snapshot");
    if (second?.type === "snapshot") {
      assert.deepEqual(second.config.availableEditors, ["file-manager"]);
      assert.equal(second.config.shellRevealInFileManagerKind, "file-explorer");
      assert.deepEqual(second.config.settings, { enableProviderUpdateChecks: false } as never);
    }
  }),
);

it.effect("resends a reveal kind and remote targets that outlast the snapshot timeout", () =>
  Effect.gen(function* () {
    const targets = [{ kind: "mdns" as const, host: "test.local" }];
    const finishDiscovery = yield* Deferred.make<typeof targets>();
    const collecting = yield* withLateConfigDiscovery(
      snapshotConfig({ availableEditors: ["file-manager"] }),
      Stream.empty,
      {
        resolveAvailableEditors: () => Effect.succeed(["file-manager"]),
        resolveFileManagerRevealKind: () => Effect.succeed("file-explorer"),
      },
      { resolveTargets: () => Deferred.await(finishDiscovery) },
      { resolve: () => Effect.succeed([]) },
    ).pipe(Stream.runCollect, Effect.forkScoped({ startImmediately: true }));
    yield* TestClock.adjust("6 seconds");
    yield* Deferred.succeed(finishDiscovery, targets);
    const events = yield* Fiber.join(collecting);

    const [late, remote] = Array.from(events);
    assert.equal(events.length, 2);
    assert.equal(late?.type, "snapshot");
    if (late?.type === "snapshot") {
      assert.equal(late.config.shellRevealInFileManagerKind, "file-explorer");
    }
    assert.equal(remote?.type, "snapshot");
    if (remote?.type === "snapshot") {
      assert.deepEqual(remote.config.remoteOpenTargets, targets);
      assert.equal(remote.config.shellRevealInFileManagerKind, "file-explorer");
    }
  }).pipe(Effect.scoped),
);

it.effect("runs editor, remote open target and direct endpoint discovery side by side", () =>
  Effect.gen(function* () {
    // Each discovery finishes only once the other two have started, so a
    // config that ran them one after another would never resolve.
    const editorsStarted = yield* Deferred.make<void>();
    const targetsStarted = yield* Deferred.make<void>();
    const endpointsStarted = yield* Deferred.make<void>();
    const startThenAwait = <A>(started: Deferred.Deferred<void>, value: A) =>
      Deferred.succeed(started, undefined).pipe(
        Effect.andThen(
          Effect.forEach([editorsStarted, targetsStarted, endpointsStarted], Deferred.await),
        ),
        Effect.as(value),
      );

    const discovery = yield* resolveOpenDiscoveryForConfig({
      editors: startThenAwait(editorsStarted, ["file-manager" as const]),
      fileManagerRevealKind: Effect.succeed("finder" as const),
      remoteOpenTargets: startThenAwait(targetsStarted, []),
      directEndpoints: startThenAwait(endpointsStarted, []),
    });

    assert.deepEqual(discovery, {
      availableEditors: ["file-manager"],
      shellRevealInFileManager: true,
      shellRevealInFileManagerKind: "finder",
      remoteOpenTargets: [],
      directEndpoints: [],
    });
  }),
);

const ReplayTestLayer = OrchestrationEventStore.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

it.live.each(["thread", "shell"] as const)(
  "%s resume skips unknown types, synchronizes, and receives the live tail",
  (scope) =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-01-05T00:00:00.000Z";
      const threadId = ThreadId.make("thread:replay");
      const projectId = ProjectId.make("project:replay");
      const project = {
        id: projectId,
        title: "Replay",
        workspaceRoot: "/tmp/replay",
        defaultModelSelection: null,
        scripts: [],
        createdAt: now,
        updatedAt: now,
      } satisfies OrchestrationProjectShell;
      const insert = (version: number, unknown = false) => sql<{ sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`replay:${version}`}, ${scope === "thread" ? "thread" : "project"},
          ${scope === "thread" ? threadId : projectId}, ${version},
          ${unknown ? "future.event" : scope === "thread" ? "provider-session.detached" : "project.meta-updated"},
          ${now}, 'server', ${JSON.stringify(
            scope === "thread"
              ? { providerSessionId: "session:replay", detachedAt: now }
              : { projectId, updatedAt: now },
          )}, '{}', 2
        ) RETURNING sequence
      `;
      const [before] = yield* insert(0);
      yield* insert(1, true);
      const [after] = yield* insert(2);
      const [highWater] = yield* insert(3, true);
      const updates = yield* PubSub.unbounded();
      let liveSequence = 0;
      const consume = <A extends { readonly kind: string; readonly sequence?: number }, E, R>(
        stream: Stream.Stream<A, E, R>,
      ) =>
        stream.pipe(
          Stream.tap((item) =>
            Effect.gen(function* () {
              if (item.kind !== "synchronized") return;
              const [live] = yield* insert(4);
              liveSequence = live!.sequence;
              const events = yield* store
                .readApplicationEvents({
                  afterSequence: highWater!.sequence,
                  throughSequence: liveSequence,
                })
                .pipe(Stream.runCollect);
              yield* store.publishCommitted(events);
            }),
          ),
          Stream.takeUntil((item) => item.sequence === liveSequence),
          Stream.map((item) => ({ kind: item.kind as string, sequence: item.sequence })),
          Stream.runCollect,
        );
      const subscription = Effect.gen(function* () {
        return scope === "thread"
          ? yield* subscribeOrchestrationV2Thread({
              threadId,
              afterSequence: before!.sequence,
              requestCompletionMarker: true,
            }).pipe(Effect.flatMap(consume))
          : yield* subscribeOrchestrationV2Shell({
              afterSequence: before!.sequence,
              requestCompletionMarker: true,
            }).pipe(Effect.flatMap(consume));
      });
      const items = yield* subscription.pipe(
        Effect.provide(ShellDeltaHub.layer),
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ensureLegacyTranscript: () => Effect.void,
          getShellSnapshot: () => Effect.die("shell resume must not load a full snapshot"),
          listActiveSubagentThreadIds: () => Effect.succeed([]),
          streamStoredEventsFrom: (
            input: Parameters<
              ThreadManagement.ThreadManagementService["Service"]["streamStoredEventsFrom"]
            >[0],
          ) => {
            assert.equal(input?.afterSequence, highWater!.sequence);
            return store
              .streamApplicationEvents({ afterSequence: input?.afterSequence ?? 0 })
              .pipe(
                Stream.filter((event): event is OrchestrationV2StoredEvent => "event" in event),
              );
          },
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
        Effect.provideService(ProjectStore.ProjectStoreV2, {
          listShells: () => Effect.succeed([]),
          getShell: () => Effect.succeedSome(project),
        } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        Effect.provideService(ProjectEnrichment.ProjectEnrichmentService, {
          subscribeChanges: PubSub.subscribe(updates),
          getAvailable: () =>
            Effect.succeed({
              repositoryIdentity: null,
              faviconPath: null,
              repositoryIdentityResolved: true,
            }),
        } as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"]),
      );
      assert.deepEqual(
        items.map((item) => item.kind),
        scope === "thread"
          ? ["event", "synchronized", "event"]
          : ["project.updated", "synchronized", "project.updated"],
      );
      assert.deepEqual(
        items.flatMap((item) => (item.sequence === undefined ? [] : [item.sequence])),
        [after!.sequence, liveSequence],
      );
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("coalesces streaming replay before synchronization and the live tail", () =>
  Effect.gen(function* () {
    const store = yield* OrchestrationEventStore.OrchestrationEventStore;
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:streaming-replay");
    const now = "2026-10-27T00:00:00.000Z";
    const insert = (version: number, text: string, streaming: boolean) => sql<{
      sequence: number;
    }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      ) VALUES (
        ${`streaming-replay:${version}`}, 'thread', ${threadId}, ${version}, 'message.updated',
        ${now}, 'server', ${JSON.stringify({
          createdBy: "agent",
          creationSource: "web",
          id: "message:streaming-replay",
          threadId,
          runId: null,
          nodeId: null,
          role: "assistant",
          text,
          attachments: [],
          streaming,
          createdAt: now,
          updatedAt: now,
        })}, '{}', 2
      ) RETURNING sequence
    `;
    yield* insert(0, "H", true);
    yield* insert(1, "He", true);
    const [latest] = yield* insert(2, "Hello", true);
    const [completed] = yield* insert(3, "Hello!", false);
    const items = yield* subscribeOrchestrationV2Thread({
      threadId,
      afterSequence: 0,
      requestCompletionMarker: true,
    }).pipe(
      Effect.flatMap(Stream.runCollect),
      Effect.provideService(ThreadManagement.ThreadManagementService, {
        ensureLegacyTranscript: () => Effect.void,
        streamStoredEventsFrom: () =>
          Stream.unwrap(
            insert(4, "Next", false).pipe(
              Effect.map(() =>
                store.readAgentEvents({ threadId, afterSequence: completed!.sequence }),
              ),
            ),
          ),
      } as unknown as ThreadManagement.ThreadManagementService["Service"]),
    );
    assert.deepEqual(
      items.map((item) =>
        item.kind === "event"
          ? [item.sequence, item.event.type === "message.updated" ? item.event.payload.text : null]
          : [item.kind],
      ),
      [
        [latest!.sequence, "Hello"],
        [completed!.sequence, "Hello!"],
        ["synchronized"],
        [completed!.sequence + 1, "Next"],
      ],
    );
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("shares each live event item across subscribers and dedupes bodies per subscriber", () =>
  Effect.gen(function* () {
    const threadId = ThreadId.make("thread:shared-live");
    const messageId = MessageId.make("message:shared-live");
    const at = DateTime.makeUnsafe("2026-10-31T00:00:00.000Z");
    const owner = { threadId, runId: null, nodeId: null, createdAt: at, updatedAt: at };
    // DateTime memoizes its formatted parts, so only the plain records are frozen.
    const freeze = <A>(value: A): A => {
      if (typeof value === "object" && value !== null && !DateTime.isDateTime(value)) {
        for (const child of Object.values(value)) freeze(child);
        Object.freeze(value);
      }
      return value;
    };
    const live: ReadonlyArray<OrchestrationV2StoredEvent> = freeze([
      {
        sequence: 1,
        commandId: null,
        event: {
          id: EventId.make("event:shared-live:message"),
          type: "message.updated",
          threadId,
          occurredAt: at,
          payload: {
            ...owner,
            id: messageId,
            role: "assistant",
            text: "Done",
            attachments: [],
            streaming: false,
            createdBy: "agent",
            creationSource: "provider",
          },
        },
      },
      {
        sequence: 2,
        commandId: null,
        event: {
          id: EventId.make("event:shared-live:reply"),
          type: "turn-item.updated",
          threadId,
          occurredAt: at,
          payload: {
            ...owner,
            id: TurnItemId.make("item:shared-live:reply"),
            type: "assistant_message",
            providerThreadId: null,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 1,
            title: null,
            status: "completed",
            messageId,
            text: "Done",
            streaming: false,
            startedAt: at,
            completedAt: at,
          },
        },
      },
    ] as ReadonlyArray<OrchestrationV2StoredEvent>);
    const stringify = yield* Effect.acquireRelease(
      Effect.sync(() => vi.spyOn(JSON, "stringify")),
      (spy) => Effect.sync(() => spy.mockRestore()),
    );
    // The EventSink hands every subscriber the same projected objects.
    const subscribe = (acceptDedupedBodies: boolean) =>
      subscribeOrchestrationV2Thread({ threadId, afterSequence: 0, acceptDedupedBodies }).pipe(
        Effect.flatMap(Stream.runCollect),
        Effect.map((items) => Array.from(items)),
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ensureLegacyTranscript: () => Effect.void,
          streamStoredEventsFrom: () => Stream.fromIterable(live),
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
      );
    const [deduped, first, second] = yield* Effect.forEach([true, false, false], subscribe);

    assert.lengthOf(first!, live.length);
    first!.forEach((item, index) => {
      assert.strictEqual(item, second![index]);
      assert.strictEqual(item.kind === "event" && item.event, live[index]!.event);
    });
    assert.strictEqual(deduped![0], first![0]);
    const reply = deduped![1];
    assert.deepInclude(reply?.kind === "event" ? reply.event.payload : undefined, {
      text: "",
      textOmitted: true,
    });
    // The live buffer sizes each shared item once, not once per subscriber.
    assert.lengthOf(
      stringify.mock.calls.filter(([value]) => value?.kind === "event"),
      live.length,
    );
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("shell snapshots omit idle children and older settled threads only on request", () =>
  Effect.gen(function* () {
    const shell = (
      id: string,
      parent: string | null,
      activityRunStatus: "running" | null,
      settledDaysAgo: number | null = null,
    ) =>
      ({
        id: ThreadId.make(id),
        projectId: "project",
        archivedAt: null,
        status: activityRunStatus ?? "completed",
        activityRunStatus,
        pendingRuntimeRequest: null,
        pendingBackgroundTasks: [],
        settledOverride: settledDaysAgo === null ? null : "settled",
        settledAt:
          settledDaysAgo === null ? null : DateTime.makeUnsafe(-settledDaysAgo * 86_400_000),
        lineage: {
          parentThreadId: parent === null ? null : ThreadId.make(parent),
          relationshipToParent: parent === null ? null : "subagent",
          rootThreadId: ThreadId.make(parent ?? id),
        },
      }) as unknown as OrchestrationV2ThreadShell;
    const threads = [
      shell("parent", null, null),
      shell("idle", "parent", null),
      shell("live", "parent", "running"),
      ...Array.from({ length: 25 }, (_, index) => shell(`recent-${index}`, null, null, 1)),
      shell("old", null, null, 30),
    ];
    const firstSnapshot = (input: {
      readonly omitIdleSubagents?: boolean;
      readonly omitOlderSettled?: boolean;
    }) =>
      subscribeOrchestrationV2Shell(input).pipe(
        Effect.flatMap((stream) => stream.pipe(Stream.take(1), Stream.runCollect)),
        Effect.map(([item]) => (item?.kind === "snapshot" ? item.snapshot : null)),
        Effect.provide(ShellDeltaHub.layer),
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          listActiveSubagentThreadIds: () => Effect.succeed([]),
          listActiveShellIndex: () =>
            Effect.succeed(
              threads.map((thread) => ({
                ...thread,
                pinnedAt: null,
                snoozedUntil: null,
                mayBeLive: thread.activityRunStatus !== null,
              })),
            ),
          readShellSnapshot: (options?: { readonly threadIds?: ReadonlyArray<string> }) =>
            Effect.succeed(
              Effect.succeed({
                schemaVersion: 1,
                snapshotSequence: 0,
                threads: threads.filter(
                  (thread) => options?.threadIds?.includes(thread.id) ?? true,
                ),
                archivedThreads: [],
              }),
            ),
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
        Effect.provideService(ProjectStore.ProjectStoreV2, {
          listShells: () => Effect.succeed([]),
          getShell: () => Effect.succeedNone,
        } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        Effect.provideServiceEffect(
          ProjectEnrichment.ProjectEnrichmentService,
          PubSub.unbounded().pipe(
            Effect.map(
              (changes) =>
                ({
                  subscribeChanges: PubSub.subscribe(changes),
                }) as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"],
            ),
          ),
        ),
      );

    const recent = threads.slice(3, 28).map((thread) => thread.id);
    const full = yield* firstSnapshot({});
    assert.deepEqual(
      full?.threads.map((thread) => thread.id),
      threads.map((thread) => thread.id),
    );
    assert.isUndefined(full?.olderSettled);
    const withoutChildren = yield* firstSnapshot({ omitIdleSubagents: true });
    assert.deepEqual(
      withoutChildren?.threads.map((thread) => thread.id),
      ["parent", "live", ...recent, "old"],
    );
    const withoutOld = yield* firstSnapshot({ omitIdleSubagents: true, omitOlderSettled: true });
    assert.deepEqual(
      withoutOld?.threads.map((thread) => thread.id),
      ["parent", "live", ...recent],
    );
    assert.deepEqual(withoutOld?.olderSettled?.counts, [
      { projectId: ProjectId.make("project"), count: 1 },
    ]);
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("shell subscribers share one thread shell read per event", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:shared-shell-read");
    const now = "2026-10-31T00:00:00.000Z";
    const [event] = yield* sql<{ sequence: number }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      ) VALUES (
        'shared-shell-read:0', 'thread', ${threadId}, 0, 'provider-session.detached',
        ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:shared", detachedAt: now })},
        '{}', 2
      ) RETURNING sequence
    `;
    const reads: Array<ThreadId> = [];
    const shell = { id: threadId, archivedAt: null } as unknown as OrchestrationV2ThreadShell;
    const subscriber = subscribeOrchestrationV2Shell({
      afterSequence: 0,
      requestCompletionMarker: true,
    }).pipe(
      Effect.flatMap((stream) =>
        stream.pipe(
          Stream.takeUntil((item) => item.kind === "synchronized"),
          Stream.runCollect,
        ),
      ),
    );

    const [first, second] = yield* Effect.all([subscriber, subscriber], {
      concurrency: "unbounded",
    }).pipe(
      Effect.provide(
        ShellDeltaHub.layer.pipe(
          Layer.provideMerge(
            ThreadManagement.layer.pipe(
              Layer.provide(
                Layer.mock(Orchestrator.OrchestratorV2)({
                  listActiveSubagentThreadIds: () => Effect.succeed([]),
                  getThreadShell: (id) => Effect.sync(() => reads.push(id)).pipe(Effect.as(shell)),
                }),
              ),
              Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
              Layer.provide(ServerSettings.layerTest()),
              Layer.provide(CommandReceiptStore.layer),
            ),
          ),
        ),
      ),
      Effect.provideService(ProjectStore.ProjectStoreV2, {
        listShells: () => Effect.succeed([]),
        getShell: () => Effect.succeedNone,
      } as unknown as ProjectStore.ProjectStoreV2["Service"]),
      Effect.provideServiceEffect(
        ProjectEnrichment.ProjectEnrichmentService,
        PubSub.unbounded().pipe(
          Effect.map(
            (changes) =>
              ({
                subscribeChanges: PubSub.subscribe(changes),
              }) as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"],
          ),
        ),
      ),
    );

    for (const items of [first, second]) {
      assert.deepEqual(
        items.flatMap((item) =>
          item.kind === "thread.updated" ? [[item.sequence, item.thread.id]] : [],
        ),
        [[event!.sequence, threadId]],
      );
    }
    assert.deepEqual(reads, [threadId]);
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// The shell delta hub over mocked shell reads; `onStart` runs when the hub subscribes to committed events.
const layerShellSubscribers = (input: {
  readonly getThreadShell: Orchestrator.OrchestratorV2["Service"]["getThreadShell"];
  readonly getProjectRow?: ProjectStore.ProjectStoreV2["Service"]["getShell"];
  readonly listShells?: ProjectStore.ProjectStoreV2["Service"]["listShells"];
  readonly getAvailable?: ProjectEnrichment.ProjectEnrichmentService["Service"]["getAvailable"];
  readonly enrichmentChanges?: PubSub.PubSub<ProjectEnrichment.ProjectEnrichmentChange>;
  readonly onStart?: () => void;
}) =>
  ShellDeltaHub.layer.pipe(
    Layer.provide(
      Layer.effect(
        OrchestrationEventStore.OrchestrationEventStore,
        Effect.gen(function* () {
          const store = yield* OrchestrationEventStore.OrchestrationEventStore;
          return {
            ...store,
            subscribeCommitted: Effect.tap(store.subscribeCommitted, () =>
              Effect.sync(() => input.onStart?.()),
            ),
          };
        }),
      ),
    ),
    Layer.provideMerge(
      ThreadManagement.layer.pipe(
        Layer.provide(
          Layer.mock(Orchestrator.OrchestratorV2)({
            listActiveSubagentThreadIds: () => Effect.succeed([]),
            getThreadShell: input.getThreadShell,
          }),
        ),
        Layer.provide(Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({})),
        Layer.provide(ServerSettings.layerTest()),
        Layer.provide(CommandReceiptStore.layer),
      ),
    ),
    Layer.provideMerge(
      Layer.succeed(ProjectStore.ProjectStoreV2, {
        listShells: input.listShells ?? (() => Effect.succeed([])),
        getShell: input.getProjectRow ?? (() => Effect.succeedNone),
      } as unknown as ProjectStore.ProjectStoreV2["Service"]),
    ),
    Layer.provideMerge(
      Layer.effect(
        ProjectEnrichment.ProjectEnrichmentService,
        (input.enrichmentChanges === undefined
          ? PubSub.unbounded<ProjectEnrichment.ProjectEnrichmentChange>()
          : Effect.succeed(input.enrichmentChanges)
        ).pipe(
          Effect.map(
            (changes) =>
              ({
                subscribeChanges: PubSub.subscribe(changes),
                getAvailable:
                  input.getAvailable ??
                  (() =>
                    Effect.succeed({
                      repositoryIdentity: null,
                      faviconPath: null,
                      repositoryIdentityResolved: true,
                    })),
              }) as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"],
          ),
        ),
      ),
    ),
  );

const insertShellEvent = (sql: SqlClient.SqlClient, threadId: ThreadId, version: number) => {
  const now = "2026-10-31T00:00:00.000Z";
  return sql<{ sequence: number }>`
    INSERT INTO orchestration_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, actor_kind, payload_json, metadata_json, application_event_version
    ) VALUES (
      ${`${threadId}:${version}`}, 'thread', ${threadId}, ${version}, 'provider-session.detached',
      ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:hub", detachedAt: now })},
      '{}', 2
    ) RETURNING sequence
  `.pipe(Effect.map(([row]) => row!.sequence));
};

// Commits and publishes one event, then lets the hub's batch window pass.
const publishShellEvent = (sql: SqlClient.SqlClient, threadId: ThreadId, version: number) =>
  Effect.gen(function* () {
    const store = yield* OrchestrationEventStore.OrchestrationEventStore;
    const sequence = yield* insertShellEvent(sql, threadId, version);
    const events = yield* store
      .readApplicationEvents({ afterSequence: sequence - 1, throughSequence: sequence })
      .pipe(Stream.runCollect);
    yield* store.publishCommitted(events);
    yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
    return sequence;
  });

// A shell subscriber resumed at `afterSequence`, drained into a queue once synchronized.
const openShellSubscriber = (afterSequence: number) =>
  Effect.gen(function* () {
    const stream = yield* subscribeOrchestrationV2Shell({
      afterSequence,
      requestCompletionMarker: true,
    });
    const items = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
    const exit = yield* Deferred.make<Exit.Exit<void, unknown>>();
    yield* stream.pipe(
      Stream.runForEach((item) => Queue.offer(items, item)),
      Effect.exit,
      Effect.flatMap((result) => Deferred.succeed(exit, result)),
      Effect.forkScoped,
    );
    while ((yield* Queue.take(items)).kind !== "synchronized") {
      // drain the replay
    }
    return { items, exit };
  });

const threadUpdate = (item: OrchestrationV2ShellStreamItem) =>
  item.kind === "thread.updated" ? [item.sequence, item.thread.id] : item.kind;

it.effect("live shell deltas are projected once and shared by every shell subscriber", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:shell-hub:shared");
    const seeded = yield* insertShellEvent(sql, threadId, 0);
    const reads: Array<ThreadId> = [];
    const shell = { id: threadId, archivedAt: null } as unknown as OrchestrationV2ThreadShell;
    yield* Effect.gen(function* () {
      const first = yield* openShellSubscriber(seeded);
      const second = yield* openShellSubscriber(seeded);
      const live = yield* publishShellEvent(sql, threadId, 1);
      const a = yield* Queue.take(first.items);
      const b = yield* Queue.take(second.items);
      assert.strictEqual(a, b);
      assert.deepEqual(threadUpdate(a), [live, threadId]);
      assert.deepEqual(reads, [threadId]);
    }).pipe(
      Effect.provide(
        layerShellSubscribers({
          getThreadShell: (id) => Effect.sync(() => reads.push(id)).pipe(Effect.as(shell)),
        }),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("a shared unchanged shell is skipped per subscriber from its own first item", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:shell-hub:unchanged");
    const seeded = yield* insertShellEvent(sql, threadId, 0);
    let reads = 0;
    const shell = { id: threadId, archivedAt: null } as unknown as OrchestrationV2ThreadShell;
    yield* Effect.gen(function* () {
      const first = yield* openShellSubscriber(seeded);
      const live1 = yield* publishShellEvent(sql, threadId, 1);
      assert.deepEqual(threadUpdate(yield* Queue.take(first.items)), [live1, threadId]);
      // Joins after the hub already sent this shell once: its snapshot predates nothing it is told.
      const second = yield* openShellSubscriber(live1);
      const live2 = yield* publishShellEvent(sql, threadId, 2);
      assert.deepEqual(threadUpdate(yield* Queue.take(second.items)), [live2, threadId]);
      assert.equal(yield* Queue.size(first.items), 0);
      yield* TestClock.adjust(UNCHANGED_THREAD_SHELL_RESEND_MS);
      const live3 = yield* publishShellEvent(sql, threadId, 3);
      assert.deepEqual(threadUpdate(yield* Queue.take(first.items)), [live3, threadId]);
      assert.deepEqual(threadUpdate(yield* Queue.take(second.items)), [live3, threadId]);
      assert.equal(reads, 3);
    }).pipe(
      Effect.provide(
        layerShellSubscribers({
          getThreadShell: () => Effect.sync(() => ++reads).pipe(Effect.as(shell)),
        }),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("the shell delta hub runs only while a shell subscriber exists", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:shell-hub:lifecycle");
    const seeded = yield* insertShellEvent(sql, threadId, 0);
    let starts = 0;
    yield* Effect.gen(function* () {
      const first = yield* Scope.make();
      const second = yield* Scope.make();
      yield* openShellSubscriber(seeded).pipe(Scope.provide(first));
      yield* openShellSubscriber(seeded).pipe(Scope.provide(second));
      yield* TestClock.adjust(0);
      assert.equal(starts, 1);
      yield* Scope.close(first, Exit.void);
      yield* openShellSubscriber(seeded).pipe(Scope.provide(second));
      assert.equal(starts, 1);
      yield* Scope.close(second, Exit.void);
      const third = yield* Scope.make();
      yield* openShellSubscriber(seeded).pipe(Scope.provide(third));
      yield* TestClock.adjust(0);
      assert.equal(starts, 2);
      yield* Scope.close(third, Exit.void);
    }).pipe(
      Effect.provide(
        layerShellSubscribers({
          getThreadShell: () => Effect.succeed(null),
          onStart: () => {
            starts += 1;
          },
        }),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.effect("a failed shared shell read ends every shell subscriber's stream", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const threadId = ThreadId.make("thread:shell-hub:failure");
    const seeded = yield* insertShellEvent(sql, threadId, 0);
    yield* Effect.gen(function* () {
      const first = yield* openShellSubscriber(seeded);
      const second = yield* openShellSubscriber(seeded);
      yield* publishShellEvent(sql, threadId, 1);
      for (const subscriber of [first, second]) {
        const exit = yield* Deferred.await(subscriber.exit);
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause) as { readonly _tag?: string };
          assert.equal(error._tag, "OrchestrationV2GetShellSnapshotError");
        }
        assert.equal(yield* Queue.size(subscriber.items), 0);
      }
    }).pipe(
      Effect.provide(
        layerShellSubscribers({
          getThreadShell: (id) =>
            Effect.fail(
              new Orchestrator.OrchestratorProjectionError({
                threadId: id,
                cause: new Error("reader gone"),
              }),
            ),
        }),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.live(
  "a shell subscriber outlives a live burst larger than its buffer while its shell reads wait",
  () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-11-01T00:00:00.000Z";
      const threadIds = Array.from({ length: 3 }, (_, index) =>
        ThreadId.make(`thread:live-burst-${index}`),
      );
      const [row] = yield* sql<{ sequence: number }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      ) VALUES (
        'live-burst:0', 'thread', ${threadIds[0]!}, 0, 'provider-session.detached',
        ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:live-burst", detachedAt: now })},
        '{}', 2
      ) RETURNING sequence
    `;
      const [stored] = yield* store
        .readApplicationEvents({ afterSequence: row!.sequence - 1, throughSequence: row!.sequence })
        .pipe(Stream.runCollect);
      if (stored === undefined || !("event" in stored))
        return yield* Effect.die("Expected a thread event");
      // More events than the live buffer holds, spread over three threads whose shell reads
      // wait until every event has been retained: the stream must stay up and end with the
      // newest shell of each thread.
      const burst = 3 * LIVE_STREAM_MAX_ITEMS + 3;
      const lastSequence = row!.sequence + burst;
      const events = Array.from({ length: burst }, (_, index) => ({
        ...stored,
        sequence: row!.sequence + index + 1,
        event: { ...stored.event, threadId: threadIds[index % threadIds.length]! },
      }));
      // Shell reads wait until the whole burst is published; a buffer that gives up logs it.
      const burstPublished = yield* Deferred.make<void>();
      const overflows = overflowCounter();
      const shell = (threadId: ThreadId, sequence: number) =>
        ({
          id: threadId,
          projectId: "project",
          archivedAt: null,
          title: `Shell at ${sequence}`,
          status: "completed",
          activityRunStatus: null,
          pendingRuntimeRequest: null,
          pendingBackgroundTasks: [],
          settledOverride: null,
          settledAt: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        }) as unknown as OrchestrationV2ThreadShell;
      const threadManagement = {
        listActiveShellIndex: () => Effect.succeed([]),
        readShellSnapshot: () =>
          Effect.succeed(
            Effect.succeed({
              schemaVersion: 1,
              snapshotSequence: 0,
              threads: [],
              archivedThreads: [],
            }),
          ),
        listActiveSubagentThreadIds: () => Effect.succeed([]),
        getThreadShellReadForEvent: (input: {
          readonly threadId: ThreadId;
          readonly sequence: number;
        }) =>
          Deferred.await(burstPublished).pipe(
            Effect.as({
              shell: shell(input.threadId, input.sequence),
              readOrdinal: input.sequence,
            }),
          ),
      } as unknown as ThreadManagement.ThreadManagementService["Service"];
      const projectService = {
        getShell: () => Effect.succeedNone,
      } as unknown as ProjectService.ProjectService["Service"];
      const hub = yield* buildShellHub({ store, threadManagement, projectService });
      const items = yield* subscribeOrchestrationV2Shell({ requestCompletionMarker: true }).pipe(
        Effect.flatMap((stream) =>
          stream.pipe(
            // The hub subscription exists before the snapshot: publish once the client is live.
            Stream.tap((item) =>
              item.kind === "synchronized"
                ? store
                    .publishCommitted(events)
                    .pipe(Effect.andThen(Deferred.succeed(burstPublished, undefined)))
                : Effect.void,
            ),
            Stream.takeUntil(
              (item) => item.kind === "thread.updated" && item.sequence === lastSequence,
            ),
            Stream.runCollect,
          ),
        ),
        Effect.provide(hub),
        Effect.provide(Logger.layer([overflows.logger], { mergeWithExisting: true })),
        Effect.provideService(ThreadManagement.ThreadManagementService, threadManagement),
        Effect.provideService(ProjectStore.ProjectStoreV2, {
          listShells: () => Effect.succeed([]),
        } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        Effect.provideService(ProjectService.ProjectService, projectService),
        Effect.provideServiceEffect(
          ProjectEnrichment.ProjectEnrichmentService,
          PubSub.unbounded().pipe(
            Effect.map(
              (changes) =>
                ({
                  subscribeChanges: PubSub.subscribe(changes),
                }) as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"],
            ),
          ),
        ),
      );
      const updates = items.flatMap((item) =>
        item.kind === "thread.updated" ? [[item.thread.id, item.sequence] as const] : [],
      );
      const newestByThread = new Map(updates);
      assert.deepEqual(
        threadIds.map((threadId) => newestByThread.get(threadId)),
        [lastSequence - 2, lastSequence - 1, lastSequence],
      );
      // Coalesced before the shell reads, not one shell per event; the buffer never filled.
      assert.isBelow(updates.length, 3 * 4);
      assert.equal(overflows.count, 0);
      assert.isTrue(
        updates.every(([, sequence], index) => index === 0 || sequence > updates[index - 1]![1]),
      );
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

/** The shell delta hub over a test's mocked reads, built in the test's scope so it outlives the subscribe effect. */
const buildShellHub = (input: {
  readonly store: OrchestrationEventStore.OrchestrationEventStore["Service"];
  readonly threadManagement: ThreadManagement.ThreadManagementService["Service"];
  readonly projectService: ProjectService.ProjectService["Service"];
  readonly projectStore?: ProjectStore.ProjectStoreV2["Service"];
}) =>
  Layer.build(
    ShellDeltaHub.layer.pipe(
      Layer.provide(Layer.succeed(OrchestrationEventStore.OrchestrationEventStore, input.store)),
      Layer.provide(
        Layer.succeed(ThreadManagement.ThreadManagementService, input.threadManagement),
      ),
      Layer.provide(Layer.succeed(ProjectService.ProjectService, input.projectService)),
      Layer.provide(
        Layer.succeed(
          ProjectStore.ProjectStoreV2,
          input.projectStore ??
            ({
              listShells: () => Effect.succeed([]),
            } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        ),
      ),
    ),
  );

/** Folds shell items like the client: a snapshot replaces, deltas apply when newer than the cursor. */
function makeShellFold() {
  const threads = new Map<string, string>();
  let cursor = 0;
  let dropped = 0;
  let snapshots = 0;
  const waiters: Array<{ readonly target: number; readonly done: Deferred.Deferred<void> }> = [];
  const apply = (item: OrchestrationV2ShellStreamItem) => {
    if (item.kind === "synchronized") return;
    if (item.kind === "snapshot") {
      if (item.resolvedRepositoryIdentityRoots !== undefined) return;
      snapshots += 1;
      threads.clear();
      for (const thread of item.snapshot.threads) threads.set(thread.id, thread.title);
      cursor = item.snapshot.snapshotSequence;
    } else {
      if (item.sequence <= cursor) {
        dropped += 1;
        return;
      }
      cursor = item.sequence;
      if (item.kind === "thread.updated") threads.set(item.thread.id, item.thread.title);
      if (item.kind === "thread.removed") threads.delete(item.threadId);
    }
    for (let index = waiters.length - 1; index >= 0; index--) {
      if (cursor >= waiters[index]!.target) {
        Deferred.doneUnsafe(waiters[index]!.done, Effect.void);
        waiters.splice(index, 1);
      }
    }
  };
  const reach = (target: number) =>
    Effect.suspend(() => {
      if (cursor >= target) return Effect.void;
      const done = Deferred.makeUnsafe<void>();
      waiters.push({ target, done });
      return Deferred.await(done);
    });
  return {
    apply,
    reach,
    get state() {
      return { threads, cursor, dropped, snapshots };
    },
  };
}

/** Completes once the budget has logged its n-th overflow. */
function overflowCounter() {
  const state = { count: 0, waiters: [] as Array<Deferred.Deferred<void>> };
  const logger = Logger.make(({ message }) => {
    if (/live event buffer is full|live buffer reset for a resync/.test(String(message))) {
      state.count += 1;
      for (const waiter of state.waiters.splice(0)) Deferred.doneUnsafe(waiter, Effect.void);
    }
  });
  const reach = (count: number): Effect.Effect<void> =>
    Effect.suspend(() => {
      if (state.count >= count) return Effect.void;
      const done = Deferred.makeUnsafe<void>();
      state.waiters.push(done);
      return Deferred.await(done).pipe(Effect.andThen(reach(count)));
    });
  return {
    logger,
    reach,
    get count() {
      return state.count;
    },
  };
}

it.live(
  "a shell subscriber resyncs after every overflow: one snapshot each, folded state = store",
  () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-11-01T00:00:00.000Z";
      // The store's state: every committed thread at its newest title.
      const threads = new Map<string, string>();
      const versions = new Map<string, number>();
      let eventCounter = 0;
      // Real rows first, so the catch-up after a resync snapshot reads them; then publish.
      const commit = (threadIds: ReadonlyArray<string>) =>
        Effect.gen(function* () {
          let first: number | undefined;
          let last = 0;
          for (const threadId of threadIds) {
            const version = (versions.get(threadId) ?? -1) + 1;
            versions.set(threadId, version);
            const [inserted] = yield* sql<{ sequence: number }>`
              INSERT INTO orchestration_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type,
                occurred_at, actor_kind, payload_json, metadata_json, application_event_version
              ) VALUES (
                ${`shell-storm:${eventCounter++}`}, 'thread', ${threadId}, ${version}, 'provider-session.detached',
                ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:storm", detachedAt: now })},
                '{}', 2
              ) RETURNING sequence
            `;
            first ??= inserted!.sequence;
            last = inserted!.sequence;
            threads.set(threadId, `${threadId}@${inserted!.sequence}`);
          }
          if (first === undefined) return 0;
          const stored = yield* store
            .readApplicationEvents({ afterSequence: first - 1, throughSequence: last })
            .pipe(Stream.runCollect);
          yield* store.publishCommitted(Array.from(stored));
          return last;
        });
      yield* commit(["thread:sentinel"]);
      const shell = (threadId: string) =>
        ({
          id: threadId,
          projectId: "project",
          archivedAt: null,
          title: threads.get(threadId),
          status: "completed",
          activityRunStatus: null,
          pendingRuntimeRequest: null,
          pendingBackgroundTasks: [],
          settledOverride: null,
          settledAt: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        }) as unknown as OrchestrationV2ThreadShell;
      const overflows = overflowCounter();
      let snapshotLoads = 0;
      // Committed while the resync base is being read: after its sequence, so they arrive live
      // through the subscription the hub emptied before that read.
      const raceThreads: Array<string> = [];
      const fold = makeShellFold();
      const threadManagement = {
        listActiveShellIndex: () => Effect.succeed([]),
        readShellSnapshot: () =>
          Effect.sync(() => {
            snapshotLoads += 1;
            const rows = Array.from(threads.keys(), shell);
            const loads = snapshotLoads;
            return Effect.gen(function* () {
              if (loads > 1 && raceThreads.length > 0) yield* commit(raceThreads.splice(0));
              return { schemaVersion: 1, snapshotSequence: 0, threads: rows, archivedThreads: [] };
            }).pipe(Effect.orDie);
          }),
        listActiveSubagentThreadIds: () => Effect.succeed([]),
        getThreadShellReadForEvent: (input: {
          readonly threadId: string;
          readonly sequence: number;
        }) =>
          Effect.sync(() => ({
            shell: (() => shell(input.threadId))(),
            readOrdinal: input.sequence,
          })),
      } as unknown as ThreadManagement.ThreadManagementService["Service"];
      const projectService = {
        getShell: () => Effect.succeedNone,
      } as unknown as ProjectService.ProjectService["Service"];
      const hub = yield* buildShellHub({ store, threadManagement, projectService });
      const stream = yield* subscribeOrchestrationV2Shell({ requestCompletionMarker: true }).pipe(
        Effect.provide(hub),
        Effect.provideService(ThreadManagement.ThreadManagementService, threadManagement),
        Effect.provideService(ProjectStore.ProjectStoreV2, {
          listShells: () => Effect.succeed([]),
        } as unknown as ProjectStore.ProjectStoreV2["Service"]),
        Effect.provideService(ProjectService.ProjectService, projectService),
        Effect.provideServiceEffect(
          ProjectEnrichment.ProjectEnrichmentService,
          PubSub.unbounded().pipe(
            Effect.map(
              (changes) =>
                ({
                  subscribeChanges: PubSub.subscribe(changes),
                }) as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"],
            ),
          ),
        ),
        Effect.provide(Logger.layer([overflows.logger], { mergeWithExisting: true })),
      );
      const pull = yield* Stream.toPull(
        stream.pipe(Stream.provide(Logger.layer([overflows.logger], { mergeWithExisting: true }))),
      );
      const pullOnce = pull.pipe(Effect.map((items) => items.forEach(fold.apply)));
      while (fold.state.snapshots === 0) yield* pullOnce;
      let consumer: Fiber.Fiber<unknown, unknown> | undefined;
      const startConsumer = pullOnce.pipe(
        Effect.forever,
        Effect.forkScoped,
        Effect.tap((fiber) => Effect.sync(() => void (consumer = fiber))),
      );
      yield* startConsumer;
      let distinct = 0;
      const rounds = 3;
      for (let round = 0; round < rounds; round++) {
        const first = yield* commit(["thread:sentinel"]);
        yield* fold.reach(first);
        // The client stops pulling while more distinct threads than its buffer holds change.
        yield* Fiber.interrupt(consumer!);
        const loadsBefore = snapshotLoads;
        const burst = Array.from(
          { length: LIVE_STREAM_MAX_ITEMS + 300 },
          () => `thread:d${distinct++}`,
        );
        const touched = Array.from(threads.keys()).slice(0, 25);
        yield* commit(burst.concat(touched));
        raceThreads.push(...touched.slice(0, 10), `thread:race${round}`);
        yield* overflows.reach(round + 1);
        // Nothing pulled, so nothing loaded: a stalled client costs no snapshot.
        assert.equal(snapshotLoads, loadsBefore);
        yield* startConsumer;
        const after = yield* commit(["thread:sentinel"]);
        yield* fold.reach(after);
        assert.equal(snapshotLoads, loadsBefore + 1, `one snapshot per overflow (round ${round})`);
      }
      const last = yield* commit(["thread:sentinel"]);
      yield* fold.reach(last);
      assert.equal(fold.state.dropped, 0);
      assert.equal(fold.state.snapshots, 1 + rounds);
      assert.deepEqual(
        Array.from(fold.state.threads.entries()).sort(([a], [b]) => a.localeCompare(b)),
        Array.from(threads.entries()).sort(([a], [b]) => a.localeCompare(b)),
      );
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.live(
  "a project first seen in a resync snapshot keeps the identity a refresh resolved while the snapshot loaded",
  () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-11-01T00:00:00.000Z";
      let eventCounter = 0;
      const versions = new Map<string, number>();
      const commit = (threadIds: ReadonlyArray<string>) =>
        Effect.gen(function* () {
          let first: number | undefined;
          let last = 0;
          for (const threadId of threadIds) {
            const version = (versions.get(threadId) ?? -1) + 1;
            versions.set(threadId, version);
            const [inserted] = yield* sql<{ sequence: number }>`
              INSERT INTO orchestration_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type,
                occurred_at, actor_kind, payload_json, metadata_json, application_event_version
              ) VALUES (
                ${`enrich-race:${eventCounter++}`}, 'thread', ${threadId}, ${version}, 'provider-session.detached',
                ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:enrich", detachedAt: now })},
                '{}', 2
              ) RETURNING sequence
            `;
            first ??= inserted!.sequence;
            last = inserted!.sequence;
          }
          const stored = yield* store
            .readApplicationEvents({ afterSequence: first! - 1, throughSequence: last })
            .pipe(Stream.runCollect);
          yield* store.publishCommitted(Array.from(stored));
          return last;
        });
      const shell = (threadId: string) =>
        ({
          id: threadId,
          projectId: "project:p0",
          archivedAt: null,
          title: `${threadId}@${versions.get(threadId)}`,
          status: "completed",
          activityRunStatus: null,
          pendingRuntimeRequest: null,
          pendingBackgroundTasks: [],
          settledOverride: null,
          settledAt: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        }) as unknown as OrchestrationV2ThreadShell;
      const workspaceRoot = "/tmp/enrich-race";
      const identity = { canonicalKey: "github.com/t3/enrich-race", locator: { kind: "github" } };
      // A project created after the subscription started: the client first sees it in the
      // resync snapshot.
      const newProject = {
        id: ProjectId.make("project:new"),
        title: "New",
        workspaceRoot,
        repositoryIdentity: null,
        defaultModelSelection: null,
        scripts: [],
        createdAt: now,
        updatedAt: now,
      } as unknown as OrchestrationProjectShell;
      let projects: ReadonlyArray<OrchestrationProjectShell> = [];
      let resolved = false;
      const changes = yield* PubSub.unbounded<ProjectEnrichment.ProjectEnrichmentChange>();
      const refreshSeen = yield* Deferred.make<void>();
      let gated = false;
      const enrichment = {
        subscribeChanges: PubSub.subscribe(changes),
        // The resync's first read of the new root: its identity resolves and its refresh goes
        // out (and reaches the client) while this read still reports the root unresolved.
        getAvailable: (root: string) =>
          Effect.gen(function* () {
            if (root === workspaceRoot && !gated) {
              gated = true;
              resolved = true;
              yield* PubSub.publish(changes, {
                workspaceRoot,
                enrichment: {
                  repositoryIdentity: identity,
                  faviconPath: null,
                  repositoryIdentityResolved: true,
                },
                repositoryIdentityResolved: true,
              } as unknown as ProjectEnrichment.ProjectEnrichmentChange);
              yield* Deferred.await(refreshSeen);
              return {
                repositoryIdentity: null,
                faviconPath: null,
                repositoryIdentityResolved: false,
              };
            }
            return {
              repositoryIdentity: root === workspaceRoot && resolved ? identity : null,
              faviconPath: null,
              repositoryIdentityResolved: root === workspaceRoot && resolved,
            };
          }),
      } as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"];
      const overflows = overflowCounter();
      const threadManagement = {
        listActiveShellIndex: () => Effect.succeed([]),
        readShellSnapshot: () =>
          Effect.succeed(
            Effect.succeed({
              schemaVersion: 1,
              snapshotSequence: 0,
              threads: [],
              archivedThreads: [],
            }),
          ),
        listActiveSubagentThreadIds: () => Effect.succeed([]),
        getThreadShellReadForEvent: (input: {
          readonly threadId: string;
          readonly sequence: number;
        }) =>
          Effect.sync(() => ({
            shell: (() => shell(input.threadId))(),
            readOrdinal: input.sequence,
          })),
      } as unknown as ThreadManagement.ThreadManagementService["Service"];
      const projectService = {
        getShell: () => Effect.succeedNone,
      } as unknown as ProjectService.ProjectService["Service"];
      const projectStore = {
        listShells: () => Effect.sync(() => projects),
      } as unknown as ProjectStore.ProjectStoreV2["Service"];
      const hub = yield* buildShellHub({ store, threadManagement, projectService, projectStore });
      const stream = yield* subscribeOrchestrationV2Shell({ requestCompletionMarker: true }).pipe(
        Effect.provide(hub),
        Effect.provideService(ThreadManagement.ThreadManagementService, threadManagement),
        Effect.provideService(ProjectStore.ProjectStoreV2, projectStore),
        Effect.provideService(ProjectService.ProjectService, projectService),
        Effect.provideService(ProjectEnrichment.ProjectEnrichmentService, enrichment),
        Effect.provide(Logger.layer([overflows.logger], { mergeWithExisting: true })),
      );
      const pull = yield* Stream.toPull(
        stream.pipe(Stream.provide(Logger.layer([overflows.logger], { mergeWithExisting: true }))),
      );
      // The client: authoritative snapshots replace the projects; refreshes update known ones.
      const known = new Map<string, unknown>();
      const resynced = yield* Deferred.make<void>();
      let snapshots = 0;
      let refreshesForNew = 0;
      const apply = (item: OrchestrationV2ShellStreamItem) => {
        if (item.kind !== "snapshot") return;
        if (item.resolvedRepositoryIdentityRoots === undefined) {
          snapshots += 1;
          known.clear();
          for (const project of item.snapshot.projects)
            known.set(project.id, project.repositoryIdentity);
          if (snapshots === 2) Deferred.doneUnsafe(resynced, Effect.void);
          return;
        }
        for (const project of item.snapshot.projects) {
          if (project.id === newProject.id) {
            refreshesForNew += 1;
            Deferred.doneUnsafe(refreshSeen, Effect.void);
          }
          if (known.has(project.id)) known.set(project.id, project.repositoryIdentity);
        }
      };
      const fold = makeShellFold();
      // Project state first: the fold's waiters may resume the test on the same item.
      const applyAll = (items: ReadonlyArray<OrchestrationV2ShellStreamItem>) => {
        for (const item of items) {
          apply(item);
          fold.apply(item);
        }
      };
      const consumer = yield* pull.pipe(Effect.map(applyAll), Effect.forever, Effect.forkScoped);
      yield* fold.reach(yield* commit(["thread:seed"]));
      // Nothing pulls while the burst overflows the buffer; the project appears meanwhile.
      yield* Fiber.interrupt(consumer);
      projects = [newProject];
      yield* commit(
        Array.from({ length: LIVE_STREAM_MAX_ITEMS + 300 }, (_, index) => `thread:d${index}`),
      );
      yield* overflows.reach(1);
      yield* pull.pipe(Effect.map(applyAll), Effect.forever, Effect.forkScoped);
      yield* Deferred.await(resynced);
      // A live item committed after the resync snapshot follows whatever the resync sent after it.
      yield* fold.reach(yield* commit(["thread:seed"]));
      assert.equal(snapshots, 2);
      assert.isAtLeast(refreshesForNew, 1);
      assert.deepEqual(known.get(newProject.id), identity);
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.live.each([20, 100, 250])(
  "one large batch resets %d shell subscribers at once (half of them stalled), who share one base read",
  (subscribers) =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-11-01T00:00:00.000Z";
      const threads = new Map<string, string>();
      const versions = new Map<string, number>();
      let eventCounter = 0;
      const commit = (threadIds: ReadonlyArray<string>) =>
        Effect.gen(function* () {
          let first: number | undefined;
          let last = 0;
          for (const threadId of threadIds) {
            const version = (versions.get(threadId) ?? -1) + 1;
            versions.set(threadId, version);
            const [inserted] = yield* sql<{ sequence: number }>`
              INSERT INTO orchestration_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type,
                occurred_at, actor_kind, payload_json, metadata_json, application_event_version
              ) VALUES (
                ${`herd:${subscribers}:${eventCounter++}`}, 'thread', ${threadId}, ${version}, 'provider-session.detached',
                ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:herd", detachedAt: now })},
                '{}', 2
              ) RETURNING sequence
            `;
            first ??= inserted!.sequence;
            last = inserted!.sequence;
            threads.set(threadId, `${threadId}@${inserted!.sequence}`);
          }
          const stored = yield* store
            .readApplicationEvents({ afterSequence: first! - 1, throughSequence: last })
            .pipe(Stream.runCollect);
          yield* store.publishCommitted(Array.from(stored));
          return last;
        });
      yield* commit(["thread:sentinel"]);
      const shell = (threadId: string) =>
        ({
          id: threadId,
          projectId: "project",
          archivedAt: null,
          title: threads.get(threadId),
          status: "completed",
          activityRunStatus: null,
          pendingRuntimeRequest: null,
          pendingBackgroundTasks: [],
          settledOverride: null,
          settledAt: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        }) as unknown as OrchestrationV2ThreadShell;
      let snapshotReads = 0;
      const threadManagement = {
        listActiveShellIndex: () => Effect.succeed([]),
        readShellSnapshot: () =>
          Effect.sync(() => {
            snapshotReads += 1;
            return Effect.succeed({
              schemaVersion: 1,
              snapshotSequence: 0,
              threads: Array.from(threads.keys(), shell),
              archivedThreads: [],
            });
          }),
        listActiveSubagentThreadIds: () => Effect.succeed([]),
        getThreadShellReadForEvent: (input: {
          readonly threadId: string;
          readonly sequence: number;
        }) =>
          Effect.sync(() => ({
            shell: (() => shell(input.threadId))(),
            readOrdinal: input.sequence,
          })),
      } as unknown as ThreadManagement.ThreadManagementService["Service"];
      const projectService = {
        getShell: () => Effect.succeedNone,
      } as unknown as ProjectService.ProjectService["Service"];
      const overflows = overflowCounter();
      const logging = Logger.layer([overflows.logger], { mergeWithExisting: true });
      const hub = yield* buildShellHub({ store, threadManagement, projectService });
      const folds: Array<ReturnType<typeof makeShellFold>> = [];
      // A subscriber that pulls by hand: its consumer is interrupted while it stalls.
      const pullInto = (
        stream: Effect.Success<ReturnType<typeof subscribeOrchestrationV2Shell>>,
        fold: ReturnType<typeof makeShellFold>,
      ) =>
        Stream.toPull(stream.pipe(Stream.provide(logging))).pipe(
          Effect.map((pull) =>
            pull.pipe(
              Effect.map((items) => {
                for (const item of items) fold.apply(item);
              }),
            ),
          ),
        );
      const stalled: Array<{
        readonly pull: Effect.Success<ReturnType<typeof pullInto>>;
        consumer: Fiber.Fiber<unknown, unknown>;
      }> = [];
      // Project rows a subscriber reads itself (its initial snapshot); a resync has them in the base.
      let projectListReads = 0;
      for (let index = 0; index < subscribers; index++) {
        const fold = makeShellFold();
        folds.push(fold);
        const stream = yield* subscribeOrchestrationV2Shell({ requestCompletionMarker: true }).pipe(
          Effect.provide(hub),
          Effect.provideService(ThreadManagement.ThreadManagementService, threadManagement),
          Effect.provideService(ProjectStore.ProjectStoreV2, {
            listShells: () =>
              Effect.sync(() => {
                projectListReads += 1;
                return [];
              }),
          } as unknown as ProjectStore.ProjectStoreV2["Service"]),
          Effect.provideService(ProjectService.ProjectService, projectService),
          Effect.provideServiceEffect(
            ProjectEnrichment.ProjectEnrichmentService,
            PubSub.unbounded<ProjectEnrichment.ProjectEnrichmentChange>().pipe(
              Effect.map(
                (changes) =>
                  ({
                    subscribeChanges: PubSub.subscribe(changes),
                  }) as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"],
              ),
            ),
          ),
          Effect.provide(logging),
        );
        if (index % 2 === 0) {
          yield* stream.pipe(
            Stream.provide(logging),
            Stream.runForEach((item) => Effect.sync(() => fold.apply(item))),
            Effect.forkScoped,
          );
        } else {
          const pull = yield* pullInto(stream, fold);
          stalled.push({ pull, consumer: yield* pull.pipe(Effect.forever, Effect.forkScoped) });
        }
      }
      const ready = yield* commit(["thread:sentinel"]);
      yield* Effect.forEach(folds, (fold) => fold.reach(ready), { discard: true });
      // Half the subscribers stop pulling; one batch with more distinct threads than a buffer
      // holds empties every buffer at once, and the pulling half resyncs.
      for (const entry of stalled) yield* Fiber.interrupt(entry.consumer);
      const readsBefore = snapshotReads;
      const projectListReadsBefore = projectListReads;
      yield* commit(
        Array.from({ length: LIVE_STREAM_MAX_ITEMS + 300 }, (_, index) => `thread:bulk${index}`),
      );
      const pulled = yield* commit(["thread:sentinel"]);
      yield* Effect.forEach(
        folds.filter((_, index) => index % 2 === 0),
        (fold) => fold.reach(pulled),
        { discard: true },
      );
      const readsForPulling = snapshotReads - readsBefore;
      // More changes before the stalled half pulls again: their resync shares the wave's base.
      yield* commit(Array.from({ length: 50 }, (_, index) => `thread:late${index}`));
      for (const entry of stalled) {
        entry.consumer = yield* entry.pull.pipe(Effect.forever, Effect.forkScoped);
      }
      const after = yield* commit(["thread:sentinel"]);
      yield* Effect.forEach(folds, (fold) => fold.reach(after), { discard: true });
      const baseReads = snapshotReads - readsBefore;
      yield* Effect.log(
        `shell resync herd: subscribers=${subscribers} resets=${overflows.count} baseReads=${baseReads} (pulling half: ${readsForPulling})`,
      );
      const truth = Array.from(threads.entries()).sort(([a], [b]) => a.localeCompare(b));
      for (const fold of folds) {
        assert.equal(fold.state.dropped, 0);
        assert.equal(fold.state.snapshots, 2);
        assert.deepEqual(
          Array.from(fold.state.threads.entries()).sort(([a], [b]) => a.localeCompare(b)),
          truth,
        );
      }
      // The resets of one batch share one base read, however the resets and pulls interleave,
      // and a subscriber that pulls late finds the base its reset is covered by.
      assert.equal(readsForPulling, 1);
      assert.equal(baseReads, 1);
      assert.equal(overflows.count, subscribers);
      // The resync's project rows, and the identities re-sent after its snapshot, come from the
      // base: no subscriber lists the projects again.
      assert.equal(projectListReads, projectListReadsBefore);
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.live(
  "a subscriber cancelled mid-read leaves the wave's read to the others; a restarted hub reads anew",
  () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-11-01T00:00:00.000Z";
      const threads = new Map<string, string>();
      const versions = new Map<string, number>();
      let eventCounter = 0;
      const commit = (threadIds: ReadonlyArray<string>) =>
        Effect.gen(function* () {
          let first: number | undefined;
          let last = 0;
          for (const threadId of threadIds) {
            const version = (versions.get(threadId) ?? -1) + 1;
            versions.set(threadId, version);
            const [inserted] = yield* sql<{ sequence: number }>`
              INSERT INTO orchestration_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type,
                occurred_at, actor_kind, payload_json, metadata_json, application_event_version
              ) VALUES (
                ${`cancel:${eventCounter++}`}, 'thread', ${threadId}, ${version}, 'provider-session.detached',
                ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:cancel", detachedAt: now })},
                '{}', 2
              ) RETURNING sequence
            `;
            first ??= inserted!.sequence;
            last = inserted!.sequence;
            threads.set(threadId, `${threadId}@${inserted!.sequence}`);
          }
          const stored = yield* store
            .readApplicationEvents({ afterSequence: first! - 1, throughSequence: last })
            .pipe(Stream.runCollect);
          yield* store.publishCommitted(Array.from(stored));
          return last;
        });
      yield* commit(["thread:sentinel"]);
      const shell = (threadId: string) =>
        ({
          id: threadId,
          projectId: "project",
          archivedAt: null,
          title: threads.get(threadId),
          status: "completed",
          activityRunStatus: null,
          pendingRuntimeRequest: null,
          pendingBackgroundTasks: [],
          settledOverride: null,
          settledAt: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        }) as unknown as OrchestrationV2ThreadShell;
      let snapshotReads = 0;
      let hubStarts = 0;
      // A gated base read signals when it starts and completes when the gate opens.
      let gate:
        | { readonly started: Deferred.Deferred<void>; readonly open: Deferred.Deferred<void> }
        | undefined;
      const threadManagement = {
        listActiveShellIndex: () => Effect.succeed([]),
        readShellSnapshot: () =>
          Effect.suspend(() => {
            snapshotReads += 1;
            const rows = Array.from(threads.keys(), shell);
            const held = gate;
            return (
              held === undefined
                ? Effect.void
                : Deferred.succeed(held.started, undefined).pipe(
                    Effect.andThen(Deferred.await(held.open)),
                  )
            ).pipe(
              Effect.map(() =>
                Effect.succeed({
                  schemaVersion: 1,
                  snapshotSequence: 0,
                  threads: rows,
                  archivedThreads: [],
                }),
              ),
            );
          }),
        listActiveSubagentThreadIds: () =>
          Effect.sync(() => {
            hubStarts += 1;
            return [];
          }),
        getThreadShellReadForEvent: (input: {
          readonly threadId: string;
          readonly sequence: number;
        }) =>
          Effect.sync(() => ({
            shell: (() => shell(input.threadId))(),
            readOrdinal: input.sequence,
          })),
      } as unknown as ThreadManagement.ThreadManagementService["Service"];
      const projectService = {
        getShell: () => Effect.succeedNone,
      } as unknown as ProjectService.ProjectService["Service"];
      const hub = yield* buildShellHub({ store, threadManagement, projectService });
      const subscriber = (scope: Scope.Scope) =>
        Effect.gen(function* () {
          const fold = makeShellFold();
          const stream = yield* subscribeOrchestrationV2Shell({
            requestCompletionMarker: true,
          }).pipe(
            Effect.provide(hub),
            Effect.provideService(ThreadManagement.ThreadManagementService, threadManagement),
            Effect.provideService(ProjectStore.ProjectStoreV2, {
              listShells: () => Effect.succeed([]),
            } as unknown as ProjectStore.ProjectStoreV2["Service"]),
            Effect.provideService(ProjectService.ProjectService, projectService),
            Effect.provideServiceEffect(
              ProjectEnrichment.ProjectEnrichmentService,
              PubSub.unbounded<ProjectEnrichment.ProjectEnrichmentChange>().pipe(
                Effect.map(
                  (changes) =>
                    ({
                      subscribeChanges: PubSub.subscribe(changes),
                    }) as unknown as ProjectEnrichment.ProjectEnrichmentService["Service"],
                ),
              ),
            ),
            Scope.provide(scope),
          );
          const pull = (yield* Stream.toPull(stream).pipe(Scope.provide(scope))).pipe(
            Effect.map((items) => {
              for (const item of items) fold.apply(item);
            }),
          );
          const consume = pull.pipe(Effect.forever, Effect.forkIn(scope));
          return { fold, consume, consumer: yield* consume };
        });
      const truth = () => Array.from(threads.entries()).sort(([a], [b]) => a.localeCompare(b));
      const folded = (fold: ReturnType<typeof makeShellFold>) =>
        Array.from(fold.state.threads.entries()).sort(([a], [b]) => a.localeCompare(b));
      const scopeA = yield* Scope.make();
      const scopeB = yield* Scope.make();
      const a = yield* subscriber(scopeA);
      const b = yield* subscriber(scopeB);
      const ready = yield* commit(["thread:sentinel"]);
      yield* a.fold.reach(ready);
      yield* b.fold.reach(ready);
      yield* Fiber.interrupt(a.consumer);
      yield* Fiber.interrupt(b.consumer);
      // One batch resets both. A pulls first and starts the wave's read, held by the gate; A is
      // cancelled while it waits. B's resync waits on that same read, which the hub keeps running.
      yield* commit(
        Array.from({ length: LIVE_STREAM_MAX_ITEMS + 300 }, (_, index) => `thread:x${index}`),
      );
      const held = { started: yield* Deferred.make<void>(), open: yield* Deferred.make<void>() };
      gate = held;
      const readsBefore = snapshotReads;
      yield* a.consume;
      yield* Deferred.await(held.started);
      yield* Scope.close(scopeA, Exit.void);
      yield* b.consume;
      gate = undefined;
      yield* Deferred.succeed(held.open, undefined);
      const afterB = yield* commit(["thread:sentinel"]);
      yield* b.fold.reach(afterB);
      assert.equal(snapshotReads - readsBefore, 1);
      assert.equal(b.fold.state.snapshots, 2);
      assert.deepEqual(folded(b.fold), truth());
      // The last subscriber leaves and the hub stops. A new one restarts it; its own wave reads.
      yield* Scope.close(scopeB, Exit.void);
      const startsBefore = hubStarts;
      const scopeC = yield* Scope.make();
      const c = yield* subscriber(scopeC);
      yield* c.fold.reach(yield* commit(["thread:sentinel"]));
      assert.isAbove(hubStarts, startsBefore);
      yield* Fiber.interrupt(c.consumer);
      const readsC = snapshotReads;
      yield* commit(
        Array.from({ length: LIVE_STREAM_MAX_ITEMS + 300 }, (_, index) => `thread:y${index}`),
      );
      yield* c.consume;
      yield* c.fold.reach(yield* commit(["thread:sentinel"]));
      assert.equal(snapshotReads - readsC, 1);
      assert.equal(c.fold.state.snapshots, 2);
      assert.equal(c.fold.state.dropped, 0);
      assert.deepEqual(folded(c.fold), truth());
      yield* Scope.close(scopeC, Exit.void);
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

it.live(
  "a thread subscriber resyncs after every overflow: a stalled client loads no snapshot, repeats fold correctly",
  () =>
    Effect.gen(function* () {
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const sql = yield* SqlClient.SqlClient;
      const now = "2026-11-01T00:00:00.000Z";
      const threadId = ThreadId.make("thread:thread-resync");
      const [row] = yield* sql<{ sequence: number }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      ) VALUES (
        'thread-resync:0', 'thread', ${threadId}, 0, 'provider-session.detached',
        ${now}, 'server', ${JSON.stringify({ providerSessionId: "session:thread-resync", detachedAt: now })},
        '{}', 2
      ) RETURNING sequence
    `;
      const [stored] = yield* store
        .readApplicationEvents({ afterSequence: row!.sequence - 1, throughSequence: row!.sequence })
        .pipe(Stream.runCollect);
      if (stored === undefined || !("event" in stored))
        return yield* Effect.die("Expected a thread event");
      let sequence = row!.sequence;
      let newestPublished = sequence;
      const nextEvents = (count: number) =>
        Array.from({ length: count }, () => ({ ...stored, sequence: ++sequence }));
      const overflows = overflowCounter();
      // Each live tail reads the high-water mark right after subscribing: publish queued events there.
      let queued: Array<OrchestrationV2StoredEvent> = [];
      const publishOnLiveSubscription: Statement.Transformer = (statement) => {
        const [query] = statement.compile();
        if (!query.includes("FROM orchestration_events") || !query.includes("MAX(sequence)")) {
          return Effect.succeed(statement);
        }
        const batch = queued;
        queued = [];
        if (batch.length === 0) return Effect.succeed(statement);
        newestPublished = batch.at(-1)!.sequence;
        return store.publishCommitted(batch).pipe(Effect.as(statement));
      };
      const publish = (batch: ReadonlyArray<OrchestrationV2StoredEvent>) =>
        Effect.suspend(() => {
          newestPublished = batch.at(-1)!.sequence;
          return store.publishCommitted(batch);
        });
      let snapshotLoads = 0;
      const projection = (title: string) =>
        ({
          thread: {
            id: threadId,
            projectId: ProjectId.make("project"),
            title,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            deletedAt: null,
          },
          runs: [],
          attempts: [],
          nodes: [],
          subagents: [],
          providerSessions: [],
          providerThreads: [],
          providerTurns: [],
          runtimeRequests: [],
          messages: [],
          plans: [],
          turnItems: [],
          checkpointScopes: [],
          checkpoints: [],
          contextHandoffs: [],
          contextTransfers: [],
          visibleTurnItems: [],
          updatedAt: now,
        }) as unknown as OrchestrationV2ThreadProjection;
      const stream = yield* subscribeOrchestrationV2Thread({
        threadId,
        requestCompletionMarker: true,
      }).pipe(
        Effect.provideService(ThreadManagement.ThreadManagementService, {
          ensureLegacyTranscript: () => Effect.void,
          // The projection the store holds reflects every event published so far.
          getThreadSnapshot: () =>
            Effect.sync(() => {
              snapshotLoads += 1;
              return {
                snapshotSequence: newestPublished,
                projection: projection(`Events through ${newestPublished}`),
              };
            }),
          streamStoredEventsFrom: (input: { readonly afterSequence?: number }) =>
            store
              .streamApplicationEvents({ afterSequence: input.afterSequence ?? 0 })
              .pipe(
                Stream.filter((event): event is OrchestrationV2StoredEvent => "event" in event),
              ),
        } as unknown as ThreadManagement.ThreadManagementService["Service"]),
      );
      const pull = yield* Stream.toPull(
        stream.pipe(
          Stream.provideService(Statement.CurrentTransformer, publishOnLiveSubscription),
          Stream.provide(Logger.layer([overflows.logger], { mergeWithExisting: true })),
        ),
      );
      let cursor = 0;
      let title = "";
      let snapshots = 0;
      let dropped = 0;
      const apply = (items: ReadonlyArray<OrchestrationV2ThreadStreamItem>) => {
        for (const item of items) {
          if (item.kind === "snapshot") {
            snapshots += 1;
            cursor = item.snapshotSequence;
            title = item.projection.thread.title;
          } else if (item.kind === "event") {
            if (item.sequence <= cursor) dropped += 1;
            else cursor = item.sequence;
          }
        }
      };
      for (let items = yield* pull; ; items = yield* pull) {
        apply(items);
        if (items.some((item) => item.kind === "snapshot")) break;
      }
      const rounds = 3;
      for (let round = 0; round < rounds; round++) {
        // One event for the live tail that starts with this pull (initial, or after a resync).
        const marker = nextEvents(1);
        queued = marker;
        if (round > 0) {
          // The pull after a stall surfaces the overflow and loads exactly one snapshot.
          const loadsBefore = snapshotLoads;
          let gotSnapshot = false;
          while (!gotSnapshot) {
            const items = yield* pull;
            apply(items);
            gotSnapshot = items.some((item) => item.kind === "snapshot");
          }
          assert.equal(snapshotLoads, loadsBefore + 1);
        }
        while (cursor < marker[0]!.sequence) apply(yield* pull);
        // The client stalls: nothing pulls while more events than the buffer holds arrive.
        const loadsBefore = snapshotLoads;
        yield* publish(nextEvents(LIVE_STREAM_MAX_ITEMS + 200));
        yield* overflows.reach(round + 1);
        assert.equal(
          snapshotLoads,
          loadsBefore,
          `a stalled client loaded a snapshot (round ${round})`,
        );
      }
      const marker = nextEvents(1);
      queued = marker;
      while (cursor < marker[0]!.sequence) apply(yield* pull);
      assert.equal(dropped, 0);
      assert.equal(snapshots, 1 + rounds);
      assert.equal(snapshotLoads, 1 + rounds);
      assert.equal(overflows.count, rounds);
      assert.equal(title, `Events through ${marker[0]!.sequence - 1}`);
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);
const decodeThreadShellJson = Schema.decodeSync(Schema.toCodecJson(OrchestrationV2ThreadShell));
const encodeShellChunk = Schema.encodeUnknownSync(
  Schema.toCodecJson(Schema.NonEmptyArray(OrchestrationV2RpcSchemas.subscribeShell.output)),
);
const encodeThreadChunk = Schema.encodeUnknownSync(
  Schema.toCodecJson(Schema.NonEmptyArray(OrchestrationV2RpcSchemas.subscribeThread.output)),
);

it("encodes a shared stream item once, as the RPC JSON codec encodes a chunk", () => {
  const shell = decodeThreadShellJson({
    createdBy: "user",
    creationSource: "web",
    id: "4fce7cfc-16ef-42bb-bca2-000000000046",
    projectId: "d2e0512a-ce5d-42b9-b72d-000000000001",
    title: "Synthetic thread 63",
    providerInstanceId: "claudeAgent",
    modelSelection: { instanceId: "claudeAgent", model: "claude-sonnet-4-6" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    linkedPullRequest: null,
    pullRequests: [],
    branchPullRequest: null,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: "4fce7cfc-16ef-42bb-bca2-000000000046",
    },
    forkedFrom: null,
    activeProviderThreadId: null,
    latestRunId: "run:thread:4fce7cfc-16ef-42bb-bca2-000000000046:ordinal:4",
    latestRunRequestedAt: "2025-11-09T01:43:58.436Z",
    latestRunStartedAt: "2025-11-09T01:43:59.436Z",
    latestRunCompletedAt: "2025-11-09T01:44:33.436Z",
    latestRunStopRequested: false,
    activeRunId: null,
    activityRunStartedAt: null,
    activityRunStatus: null,
    blockedOnThreadWait: false,
    status: "completed",
    lastError: null,
    usageLimitResetAt: null,
    pendingRuntimeRequest: null,
    pendingGuestMessageCount: 0,
    latestVisibleMessage: null,
    latestUserMessageAt: "2025-11-09T01:43:58.436Z",
    hasActionableProposedPlan: false,
    pendingBackgroundTasks: [],
    providerInstanceHistory: ["claudeAgent"],
    itemCount: 25,
    visibleItemCount: 25,
    createdAt: "2025-11-09T01:09:02.473Z",
    updatedAt: "2025-11-09T01:45:33.436Z",
    archivedAt: null,
    settledOverride: "settled",
    settledAt: "2025-11-09T01:45:33.436Z",
    lastVisitedAt: "2025-11-09T01:45:33.436Z",
    deletedAt: null,
  });
  const shellItems: Array<OrchestrationV2ShellStreamItem> = [
    { kind: "thread.updated", sequence: 7, location: "active", thread: shell },
    { kind: "thread.removed", sequence: 8, location: "archive", threadId: shell.id },
    { kind: "synchronized" },
  ];
  assert.deepStrictEqual<unknown>(
    shellItems.map(encodeShellStreamItem),
    encodeShellChunk(shellItems),
  );
  assert.strictEqual(encodeShellStreamItem(shellItems[0]!), encodeShellStreamItem(shellItems[0]!));
  const synchronized = { kind: "synchronized" as const };
  assert.deepStrictEqual<unknown>(
    [encodeThreadStreamItem(synchronized)],
    encodeThreadChunk([synchronized]),
  );
});

// Review E1: each encoder keeps its own cache, so an object that reaches both streams is never
// served with the other stream's encoding.
it("an item encoded for the shell stream is not served to the thread stream from the cache", () => {
  const item: OrchestrationV2ShellStreamItem = {
    kind: "thread.removed",
    sequence: 9,
    location: "archive",
    threadId: ThreadId.make("4fce7cfc-16ef-42bb-bca2-000000000047"),
  };
  assert.deepStrictEqual<unknown>([encodeShellStreamItem(item)], encodeShellChunk([item]));
  // Not a thread stream item: the thread encoder must reject it, not return the shell encoding.
  assert.throws(() => encodeThreadStreamItem(item as never));
});

// Build the production RPC layer; unrelated capabilities fail if this test calls them.
const layerUnusedConfigRpcServices = Layer.mergeAll(
  Layer.mock(ShellDeltaHub.ShellDeltaHub)({
    inspect: () => ({ subscribers: 0, baseRetained: false }),
  }),
  Layer.mock(DocumentText.DocumentText)({}),
  Layer.mock(ServerSecretStore.ServerSecretStore)({}),
  Layer.mock(WorkspacePaths.WorkspacePaths)({}),
  Layer.mock(TraceDiagnostics.TraceDiagnostics)({}),
  Layer.mock(TriageService.TriageService)({}),
  Layer.mock(ProjectFaviconResolver.ProjectFaviconResolver)({}),
  Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({}),
  Layer.mock(AcpRegistryRuntimeCoordinator.AcpRegistryRuntimeCoordinator)({}),
  Layer.mock(AcpRegistrySupport.AcpRegistryCatalog)({}),
  Layer.mock(AgentSessionImporter.AgentSessionImporter)({}),
  Layer.mock(AgentSessionScanner.AgentSessionScanner)({}),
  Layer.mock(AnalyticsService.AnalyticsService)({}),
  Layer.mock(AntigravityInstallation.AntigravityInstallation)({ managedDirectory: "/unused" }),
  Layer.mock(BackgroundPolicy.BackgroundPolicy)({}),
  Layer.mock(CheckpointDiffQuery.CheckpointDiffQuery)({}),
  Layer.mock(NativeChildWorkQuery.NativeChildWorkQuery)({}),
  Layer.mock(CommandOutputHub.CommandOutputHub)({}),
  Layer.mock(CodexInstallation.CodexInstallation)({ managedDirectory: "/unused" }),
  Layer.mock(DeviceService.DeviceService)({}),
  Layer.mock(EnvironmentTheme.EnvironmentThemeService)({}),
  Layer.mock(GitWorkflowService.GitWorkflowService)({}),
  Layer.mock(HostResources.HostResources)({}),
  Layer.mock(McpAppRequests.McpAppRequests)({}),
  Layer.mock(ModelManifest.ModelManifest)({}),
  Layer.mock(OrchestrationEventStore.OrchestrationEventStore)({}),
  Layer.mock(Orchestrator.OrchestratorV2)({}),
  Layer.mock(OtlpEndpointCheck.OtlpEndpointCheck)({}),
  Layer.mock(PairingGrantStore.PairingGrantStore)({}),
  Layer.mock(PeerLinks.PeerLinks)({}),
  Layer.mock(PeerLinkRequests.PeerLinkRequests)({}),
  Layer.mock(PortScanner.PortDiscovery)({ trackAgent: () => () => undefined }),
  Layer.mock(PreviewManager.PreviewManager)({}),
  Layer.mock(ProcessDiagnostics.ProcessDiagnostics)({}),
  Layer.mock(ProcessResourceMonitor.ProcessResourceMonitor)({}),
  Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({}),
  Layer.mock(ProjectService.ProjectService)({}),
  Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({}),
  Layer.mock(ProjectStore.ProjectStoreV2)({}),
  Layer.mock(ProviderAuthService.ProviderAuthService)({}),
  Layer.mock(ProviderInstanceRegistry.ProviderInstanceRegistry)({}),
  Layer.mock(ProviderLatestVersions.ProviderLatestVersions)({}),
  Layer.mock(ProviderMaintenanceRunner.ProviderMaintenanceRunner)({}),
  Layer.mock(ProviderSessionManager.ProviderSessionManagerV2)({}),
  Layer.mock(PullRequestReviewGuideService.PullRequestReviewGuideService)({}),
  Layer.mock(PullRequestService.PullRequestService)({}),
  Layer.mock(PullRequestSyncReactor.PullRequestSyncReactor)({}),
  Layer.mock(RelayClient.RelayClient)({}),
  Layer.mock(RepositoryIdentityResolver.RepositoryIdentityResolver)({}),
  Layer.mock(ResourceTelemetry.ResourceTelemetry)({}),
  Layer.mock(ReviewService.ReviewService)({}),
  Layer.mock(ScheduledTasks.ScheduledTaskService)({}),
  Layer.mock(SecretRequests.SecretRequests)({}),
  Layer.mock(SecretResolver.SecretResolver)({}),
  Layer.mock(GuestShareService.GuestShareService)({}),
  Layer.mock(ServerLifecycleEvents.ServerLifecycleEvents)({}),
  Layer.mock(ServerRuntimeStartup.ServerRuntimeStartup)({}),
  Layer.mock(ServerSelfUpdate.ServerSelfUpdate)({}),
  Layer.mock(NpmRelease.NpmRelease)({}),
  Layer.mock(SessionStore.SessionStore)({ cookieName: "config-test", legacyCookieName: undefined }),
  Layer.mock(SourceControlDiscovery.SourceControlDiscovery)({}),
  Layer.mock(SourceControlRepositoryService.SourceControlRepositoryService)({}),
  Layer.mock(SourceControlSignIn.SourceControlSignIn)({}),
  Layer.mock(StorageCleanup.StorageCleanup)({}),
  Layer.mock(DatabaseStorage.DatabaseStorage)({}),
  Layer.mock(TerminalManager.TerminalManager)({}),
  Layer.mock(ThreadLaunchService.ThreadLaunchService)({}),
  Layer.mock(ThreadManagement.ThreadManagementService)({}),
  Layer.mock(ThreadSearch.ThreadSearch)({}),
  Layer.mock(UsageService.UsageService)({}),
  Layer.mock(VcsProvisioningService.VcsProvisioningService)({}),
  Layer.mock(VcsStatusBroadcaster.VcsStatusBroadcaster)({}),
  Layer.mock(WorkspaceEntries.WorkspaceEntries)({}),
  Layer.mock(WorkspaceFileSystem.WorkspaceFileSystem)({}),
  Layer.mock(WorktreeSetupTracker.WorktreeSetupTracker)({}),
);

const layerConfigSources = Layer.mergeAll(
  Keybindings.layer,
  ServerSettings.layer.pipe(
    Layer.provide(Layer.merge(ServerSecretStore.layer, ManagedPolicy.layerTest())),
  ),
).pipe(
  Layer.provideMerge(Layer.fresh(SqlitePersistence.layerMemory)),
  Layer.provideMerge(
    Layer.fresh(
      ServerConfigModule.layerTest(process.cwd(), { prefix: "t3-config-subscription-test-" }),
    ),
  ),
  Layer.provideMerge(NodeServices.layer),
);

it.effect.each([
  "pending",
  "cached",
  "direct-pending",
  "direct-cached",
  "direct-failed",
  "direct-interrupted",
] as const)(
  "%s endpoint discovery preserves initial config and updates through discovery and ACK",
  (discovery) =>
    Effect.gen(function* () {
      const keybindings = yield* Keybindings.Keybindings;
      const settings = yield* ServerSettings.ServerSettingsService;
      const keybindingsSubscribed = yield* Deferred.make<void>();
      const settingsSubscribed = yield* Deferred.make<void>();
      const keybindingsChanges = keybindings.subscribeChanges.pipe(
        Effect.tap(() => Deferred.succeed(keybindingsSubscribed, undefined)),
      );
      const settingsChanges = settings.subscribeChanges.pipe(
        Effect.tap(() => Deferred.succeed(settingsSubscribed, undefined)),
      );
      const initialKeybindings = (yield* keybindings.getSnapshot).keybindings;
      const discoveryStarted = yield* Deferred.make<void>();
      const finishDiscovery = yield* Deferred.make<void>();
      const remoteDiscoveryStarted = yield* Deferred.make<void>();
      const finishRemoteDiscovery = yield* Deferred.make<void>();
      const targets = [{ kind: "mdns" as const, host: "test.local" }];
      const direct = discovery.startsWith("direct-");
      const bound = [{ kind: "lan" as const, httpBaseUrl: "http://192.168.1.10:3773/" }];
      const endpoints = [
        ...bound,
        { kind: "tailnet" as const, httpBaseUrl: "https://test.ts.net/" },
      ];
      const directStarted = yield* Deferred.make<void>();
      const finishDirect = yield* Deferred.make<void>();
      const directStopped = yield* Deferred.make<void>();
      const responses = yield* Queue.unbounded<ServerConfigStreamEvent>();
      const configRpcServices = Layer.mergeAll(
        layerUnusedConfigRpcServices,
        Layer.mock(ExternalLauncher.ExternalLauncher)({
          resolveAvailableEditors: () =>
            Deferred.succeed(discoveryStarted, undefined).pipe(
              Effect.andThen(Deferred.await(finishDiscovery)),
              Effect.as([]),
            ),
          // @effect-diagnostics-next-line effectSucceedWithVoid:off - This optional result requires undefined, not the wider void type.
          resolveFileManagerRevealKind: () => Effect.succeed(undefined),
        }),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed([]),
          streamChanges: Stream.never,
        }),
        Layer.mock(UsageLimitSources.UsageLimitSources)({ streamChanges: Stream.make([]) }),
        Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({
          resolveTargets: () =>
            Deferred.succeed(remoteDiscoveryStarted, undefined).pipe(
              Effect.andThen(
                discovery === "cached" || direct
                  ? Effect.void
                  : Deferred.await(finishRemoteDiscovery),
              ),
              Effect.as(targets),
            ),
        }),
        Layer.mock(DirectEndpoints.DirectEndpoints)({
          resolveAvailable: () => Effect.succeed(discovery === "direct-cached" ? endpoints : bound),
          resolve: () =>
            direct
              ? Deferred.succeed(directStarted, undefined).pipe(
                  Effect.andThen(
                    discovery === "direct-cached" ? Effect.void : Deferred.await(finishDirect),
                  ),
                  Effect.andThen(
                    discovery === "direct-failed"
                      ? Effect.die("probe failed")
                      : Effect.succeed(endpoints),
                  ),
                  Effect.onExit(() => Deferred.succeed(directStopped, undefined)),
                )
              : Effect.succeed(bound),
        }),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
          scratchRoot: Effect.succeedNone,
          namedProjectsRoot: process.cwd(),
        }),
        Layer.mock(ScratchThreadPromotion.ScratchThreadPromotion)({}),
        Layer.mock(ServerEnvironment.ServerEnvironment)({
          getDescriptor: Effect.succeed({
            environmentId: EnvironmentId.make("config-test"),
            label: "Config test",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "0.0.0",
            capabilities: { repositoryIdentity: false },
          }),
        }),
        Layer.mock(EnvironmentAuth.EnvironmentAuth)({
          getDescriptor: () =>
            Effect.succeed({
              policy: "loopback-browser",
              bootstrapMethods: [],
              sessionMethods: ["browser-session-cookie"],
              sessionCookieName: "config-test-session",
            }),
        }),
        // Signal subscription on either accessor so the test also fails, rather
        // than hangs, against a handler that subscribes after the snapshot.
        Layer.succeed(Keybindings.Keybindings, {
          ...keybindings,
          streamChanges: Stream.unwrap(keybindingsChanges),
          subscribeChanges: keybindingsChanges,
        }),
        Layer.succeed(ServerSettings.ServerSettingsService, {
          ...settings,
          streamChanges: Stream.unwrap(settingsChanges),
          subscribeChanges: settingsChanges,
        }),
      );
      const server = yield* RpcServer.makeNoSerialization(
        RpcGroup.make(WsSubscribeServerConfigRpc),
        {
          ...WS_RPC_SERVER_OPTIONS,
          onFromServer: (response) =>
            response._tag === "Chunk"
              ? Effect.forEach(response.values, (event) => Queue.offer(responses, event), {
                  discard: true,
                })
              : Effect.void,
        },
      ).pipe(
        Effect.provide(
          layerWsRpc(
            {
              sessionId: AuthSessionId.make("config-test"),
              subject: "config-test",
              method: "browser-session-cookie",
              scopes: [],
            },
            {},
            yield* ServerBrowser.ServerBrowser,
            () => Effect.void,
          ).pipe(Layer.provide(configRpcServices)),
        ),
      );
      const requestId = RpcMessage.RequestId("config");
      const acknowledge = server.write(0, { _tag: "Ack", requestId });
      yield* server.write(0, {
        _tag: "Request",
        id: requestId,
        tag: WS_METHODS.subscribeServerConfig,
        payload: {},
        headers: Headers.empty,
      });
      yield* Deferred.await(discoveryStarted);
      assert.isTrue(yield* Deferred.isDone(remoteDiscoveryStarted));

      const duringDiscovery = yield* settings.updateSettings({ defaultAutoPull: true });
      yield* Deferred.succeed(finishDiscovery, undefined);
      const snapshot = yield* Queue.take(responses);
      assert.equal(snapshot.type, "snapshot");
      if (snapshot.type !== "snapshot") return yield* Effect.die("Missing initial snapshot");
      assert.deepEqual(
        snapshot.config.remoteOpenTargets,
        discovery === "cached" || direct ? targets : [],
      );
      assert.deepEqual(
        snapshot.config.directEndpoints,
        discovery === "direct-cached" ? endpoints : bound,
      );
      assert.isFalse(snapshot.config.settings.defaultAutoPull);
      assert.deepEqual(snapshot.config.keybindings, initialKeybindings);
      const duringAck = yield* keybindings.upsertKeybindingRule({
        key: "mod+alt+y",
        command: "terminal.toggle",
      });
      yield* acknowledge;
      yield* Deferred.await(keybindingsSubscribed);
      yield* Deferred.await(settingsSubscribed);

      // Later updates expose a dropped buffered event as a value mismatch, not a timeout.
      const latestSettings = yield* settings.updateSettings({ enableProviderUpdateChecks: false });
      const latestKeybindings = yield* keybindings.upsertKeybindingRule({
        key: "mod+alt+z",
        command: "terminal.split",
      });
      const expectedSettings = [duringDiscovery, latestSettings].map(
        ServerSettings.redactServerSettingsForClient,
      );
      const expectedKeybindings = [duringAck.keybindings, latestKeybindings.keybindings];
      // The two sources can interleave, but each must retain its own publication order.
      for (let i = 0; i < 4; i++) {
        const event = yield* Queue.take(responses);
        if (event.type === "settingsUpdated") {
          assert.deepEqual(event.payload.settings, expectedSettings.shift());
        } else {
          assert.equal(event.type, "keybindingsUpdated");
          if (event.type !== "keybindingsUpdated")
            return yield* Effect.die("Unexpected config event");
          assert.deepEqual(event.payload.keybindings, expectedKeybindings.shift());
          assert.deepEqual(event.payload.issues, []);
        }
        yield* acknowledge;
      }
      assert.isEmpty(expectedSettings);
      assert.isEmpty(expectedKeybindings);
      if (discovery === "pending") {
        yield* Deferred.await(remoteDiscoveryStarted);
        yield* Deferred.succeed(finishRemoteDiscovery, undefined);
        const resolved = yield* Queue.take(responses);
        assert.equal(resolved.type, "snapshot");
        if (resolved.type !== "snapshot") return yield* Effect.die("Missing remote targets");
        assert.deepEqual(resolved.config.remoteOpenTargets, targets);
        assert.deepEqual(
          resolved.config.settings,
          ServerSettings.redactServerSettingsForClient(latestSettings),
        );
        assert.deepEqual(resolved.config.keybindings, latestKeybindings.keybindings);
        yield* acknowledge;
      }
      if (direct) {
        yield* Deferred.await(directStarted);
        if (discovery === "direct-pending") {
          yield* Deferred.succeed(finishDirect, undefined);
          const resolved = yield* Queue.take(responses);
          assert.equal(resolved.type, "snapshot");
          if (resolved.type !== "snapshot") return yield* Effect.die("Missing direct endpoints");
          assert.deepEqual(resolved.config.directEndpoints, endpoints);
          assert.deepEqual(
            resolved.config.settings,
            ServerSettings.redactServerSettingsForClient(latestSettings),
          );
          assert.deepEqual(resolved.config.keybindings, latestKeybindings.keybindings);
          yield* acknowledge;
        } else if (discovery === "direct-failed") {
          yield* Deferred.succeed(finishDirect, undefined);
          yield* Deferred.await(directStopped);
          yield* settings.updateSettings({ defaultAutoPull: false });
          const event = yield* Queue.take(responses);
          assert.equal(event.type, "settingsUpdated");
          yield* acknowledge;
        }
      }
      yield* server.write(0, { _tag: "Interrupt", requestId, interruptors: [] });
      if (direct) yield* Deferred.await(directStopped);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.fresh(layerConfigSources),
          Layer.mock(PreviewAutomationBroker.PreviewAutomationBroker)({}),
          Layer.mock(ServerBrowser.ServerBrowser)({}),
        ),
      ),
      Effect.scoped,
    ),
);

it.effect("on-demand clients get provider config without workspace snapshots", () =>
  Effect.gen(function* () {
    const instanceId = ProviderInstanceId.make("claudeAgent");
    const snapshotFor = (cwd: string, checkedAt: string) => ({
      cwd,
      checkedAt,
      slashCommands: [{ name: `${cwd.slice(1)}-command` }],
      skills: [],
    });
    const provider: ServerProvider = {
      instanceId,
      driver: ProviderDriverKind.make("claudeAgent"),
      enabled: true,
      installed: true,
      version: "1.0.0",
      status: "ready",
      auth: { status: "authenticated" },
      checkedAt: "2026-10-25T00:00:00.000Z",
      models: [],
      slashCommands: [{ name: "machine-command" }],
      skills: [],
      workspaceSnapshots: [
        snapshotFor("/a", "2026-10-25T00:00:00.000Z"),
        snapshotFor("/b", "2026-10-25T00:00:00.000Z"),
      ],
    };
    const providerChanges = yield* Queue.unbounded<ReadonlyArray<ServerProvider>>();
    const pulled = yield* Queue.unbounded<void>();
    const responses = yield* Queue.unbounded<unknown>();
    const configRpcServices = Layer.mergeAll(
      layerUnusedConfigRpcServices,
      Layer.mock(ExternalLauncher.ExternalLauncher)({
        resolveAvailableEditors: () => Effect.succeed([]),
        // @effect-diagnostics-next-line effectSucceedWithVoid:off - This optional result requires undefined, not the wider void type.
        resolveFileManagerRevealKind: () => Effect.succeed(undefined),
      }),
      Layer.mock(ProviderRegistry.ProviderRegistry)({
        getProviders: Effect.succeed([provider]),
        streamChanges: Stream.fromQueue(providerChanges).pipe(
          Stream.tap(() => Queue.offer(pulled, undefined)),
        ),
        refreshWorkspaceSnapshot: () => Effect.succeed([provider]),
      }),
      Layer.mock(UsageLimitSources.UsageLimitSources)({
        current: Effect.succeed([]),
        streamChanges: Stream.make([]),
      }),
      Layer.mock(RemoteOpenTargets.RemoteOpenTargets)({
        resolveTargets: () => Effect.succeed([]),
      }),
      Layer.mock(DirectEndpoints.DirectEndpoints)({
        resolve: () => Effect.succeed([]),
        resolveAvailable: () => Effect.succeed([]),
      }),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        scratchRoot: Effect.succeedNone,
        namedProjectsRoot: process.cwd(),
      }),
      Layer.mock(ScratchThreadPromotion.ScratchThreadPromotion)({}),
      Layer.mock(ServerEnvironment.ServerEnvironment)({
        getDescriptor: Effect.succeed({
          environmentId: EnvironmentId.make("config-test"),
          label: "Config test",
          platform: { os: "linux", arch: "x64" },
          serverVersion: "0.0.0",
          capabilities: { repositoryIdentity: false },
        }),
      }),
      Layer.mock(EnvironmentAuth.EnvironmentAuth)({
        getDescriptor: () =>
          Effect.succeed({
            policy: "loopback-browser",
            bootstrapMethods: [],
            sessionMethods: ["browser-session-cookie"],
            sessionCookieName: "config-test-session",
          }),
      }),
    );
    const server = yield* RpcServer.makeNoSerialization(
      RpcGroup.make(WsSubscribeServerConfigRpc, WsServerRefreshProvidersRpc),
      {
        ...WS_RPC_SERVER_OPTIONS,
        onFromServer: (response) =>
          response._tag === "Chunk"
            ? Effect.forEach(response.values, (value) => Queue.offer(responses, value), {
                discard: true,
              })
            : response._tag === "Exit" && response.exit._tag === "Success"
              ? Queue.offer(responses, response.exit.value)
              : Effect.void,
      },
    ).pipe(
      Effect.provide(
        layerWsRpc(
          {
            sessionId: AuthSessionId.make("config-test"),
            subject: "config-test",
            method: "browser-session-cookie",
            scopes: [],
          },
          {},
          yield* ServerBrowser.ServerBrowser,
          () => Effect.void,
        ).pipe(Layer.provide(configRpcServices)),
      ),
    );
    const configRequest = RpcMessage.RequestId("config");
    yield* server.write(0, {
      _tag: "Request",
      id: configRequest,
      tag: WS_METHODS.subscribeServerConfig,
      payload: { workspaceSnapshotsOnDemand: true },
      headers: Headers.empty,
    });
    const snapshot = (yield* Queue.take(responses)) as ServerConfigStreamEvent;
    if (snapshot.type !== "snapshot") return yield* Effect.die("Missing initial snapshot");
    assert.isUndefined(snapshot.config.providers[0]?.workspaceSnapshots);
    yield* server.write(0, { _tag: "Ack", requestId: configRequest });

    // A rescan that only touches a snapshot sends nothing; the next real change does.
    yield* Queue.offer(providerChanges, [
      { ...provider, workspaceSnapshots: [snapshotFor("/a", "2026-10-25T00:05:00.000Z")] },
    ]);
    yield* Queue.take(pulled);
    yield* TestClock.adjust("1 second");
    yield* Queue.offer(providerChanges, [{ ...provider, status: "warning" }]);
    yield* Queue.take(pulled);
    yield* TestClock.adjust("1 second");
    const update = (yield* Queue.take(responses)) as ServerConfigStreamEvent;
    if (update.type !== "providerStatuses") return yield* Effect.die("Expected provider update");
    assert.equal(update.payload.providers[0]?.status, "warning");
    assert.isUndefined(update.payload.providers[0]?.workspaceSnapshots);

    yield* server.write(0, {
      _tag: "Request",
      id: RpcMessage.RequestId("refresh"),
      tag: WS_METHODS.serverRefreshProviders,
      payload: { instanceId, cwd: "/b", workspaceSnapshotsOnDemand: true },
      headers: Headers.empty,
    });
    const refreshed = (yield* Queue.take(responses)) as {
      readonly providers: ReadonlyArray<ServerProvider>;
    };
    assert.deepEqual(
      refreshed.providers.map((entry) => [entry.workspaceSnapshots, entry.slashCommands]),
      [[[snapshotFor("/b", "2026-10-25T00:00:00.000Z")], []]],
    );
    yield* server.write(0, { _tag: "Interrupt", requestId: configRequest, interruptors: [] });
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.fresh(layerConfigSources),
        Layer.mock(PreviewAutomationBroker.PreviewAutomationBroker)({}),
        Layer.mock(ServerBrowser.ServerBrowser)({}),
      ),
    ),
    Effect.scoped,
  ),
);

// Compaction keeps the oldest and newest event per message, so a cursor older
// than the deleted rows still replays each message where the client would
// first have seen it (reducers append on first sight) and reaches the same
// final state. No snapshot fallback is needed.
it.effect("thread resume across compacted history keeps message order and final state", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
    const now = "2026-10-13T00:00:00.000Z";
    const threadId = ThreadId.make("thread:compacted");
    const insertMessage = (version: number, id: string, text: string) => sql<{
      sequence: number;
    }>`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      ) VALUES (
        ${`compacted:${version}`}, 'thread', ${threadId}, ${version}, 'message.updated',
        ${now}, 'server', ${JSON.stringify({
          createdBy: "user",
          creationSource: "web",
          id,
          threadId,
          runId: null,
          nodeId: null,
          role: "user",
          text,
          attachments: [],
          streaming: false,
          createdAt: now,
          updatedAt: now,
        })}, '{}', 2
      ) RETURNING sequence
    `;
    const [first] = yield* insertMessage(0, "message:a", "a created");
    const [second] = yield* insertMessage(1, "message:b", "b created");
    yield* insertMessage(2, "message:a", "a superseded");
    const [head] = yield* insertMessage(3, "message:a", "a final");
    // thread.deleted is never superseded: clients switch on the event type.
    for (const version of [0, 1]) {
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`deleted:${version}`}, 'thread', 'thread:deleted-twice', ${version}, 'thread.deleted',
          ${now}, 'server', '{}', '{}', 2
        )
      `;
    }
    assert.equal((yield* maintenance.compactEventStore()).deletedEventCount, 1);

    const items = yield* subscribeOrchestrationV2Thread({
      threadId,
      afterSequence: first!.sequence - 1,
      requestCompletionMarker: true,
    }).pipe(
      Effect.flatMap((stream) =>
        stream.pipe(
          Stream.takeUntil((item) => item.kind === "synchronized"),
          Stream.runCollect,
        ),
      ),
      Effect.provideService(ThreadManagement.ThreadManagementService, {
        ensureLegacyTranscript: () => Effect.void,
        streamStoredEventsFrom: () => Stream.never,
      } as unknown as ThreadManagement.ThreadManagementService["Service"]),
    );
    assert.deepEqual(
      Array.from(items, (item) =>
        item.kind === "event"
          ? [item.sequence, item.event.type === "message.updated" ? item.event.payload.text : null]
          : [item.kind],
      ),
      [
        [first!.sequence, "a created"],
        [second!.sequence, "b created"],
        [head!.sequence, "a final"],
        ["synchronized"],
      ],
    );
  }).pipe(
    Effect.provide(
      ProjectionMaintenance.layer.pipe(
        Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
        Layer.provideMerge(ReplayTestLayer),
      ),
    ),
  ),
);

// Reviewer scenario (F2): the resume replay reads a thread after the hub's held delta for it was read.
it.effect("a resumed shell subscriber never receives a shell older than one its replay sent", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = yield* OrchestrationEventStore.OrchestrationEventStore;
    const threadId = ThreadId.make("thread:resume-order");
    let title = "c1";
    let version = 0;
    let published = 0;
    const insert = (id: ThreadId) =>
      sql<{ sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`resume-order:${++version}`}, 'thread', ${id}, ${version}, 'provider-session.detached',
          '2026-10-31T00:00:00.000Z', 'server',
          ${JSON.stringify({ providerSessionId: "session:order", detachedAt: "2026-10-31T00:00:00.000Z" })},
          '{}', 2
        ) RETURNING sequence
      `.pipe(Effect.asVoid);
    const publishAll = Effect.gen(function* () {
      const events = yield* store
        .readApplicationEvents({ afterSequence: published, throughSequence: version })
        .pipe(Stream.runCollect);
      published = version;
      yield* store.publishCommitted(events);
    });
    const settle = Effect.gen(function* () {
      for (let round = 0; round < 4; round++) {
        for (let step = 0; step < 200; step++) yield* Effect.yieldNow;
        yield* TestClock.adjust(0);
      }
    });
    // The replay's reads of these threads wait, so the replay outlives the 5 s shared-read TTL.
    const replayGate = yield* Deferred.make<void>();
    const seen: Array<string> = [];
    for (let index = 0; index < 8; index++)
      yield* insert(ThreadId.make(`thread:resume-slow-${index}`));
    yield* insert(threadId);
    published = version;
    yield* Effect.gen(function* () {
      const stream = yield* subscribeOrchestrationV2Shell({
        afterSequence: 0,
        requestCompletionMarker: true,
      });
      yield* stream.pipe(
        Stream.runForEach((item) =>
          Effect.sync(() => {
            seen.push(
              item.kind === "thread.updated"
                ? `${item.sequence}:${(item.thread as { title: string }).title}`
                : item.kind,
            );
          }),
        ),
        Effect.forkScoped,
      );
      yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
      yield* settle;
      title = "c2";
      yield* insert(threadId);
      yield* publishAll;
      yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
      yield* settle;
      yield* TestClock.adjust(ThreadManagement.SHARED_SHELL_READ_TTL_MS + 1_000);
      yield* settle;
      title = "c3";
      yield* insert(threadId);
      yield* publishAll;
      yield* Deferred.succeed(replayGate, undefined);
      yield* settle;
      yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
      yield* settle;
      yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
      yield* settle;
    }).pipe(
      Effect.provide(
        layerShellSubscribers({
          getThreadShell: (id) =>
            Effect.gen(function* () {
              if (id.startsWith("thread:resume-slow-")) {
                yield* Deferred.await(replayGate);
                return {
                  id,
                  archivedAt: null,
                  title: "slow",
                } as unknown as OrchestrationV2ThreadShell;
              }
              return { id, archivedAt: null, title } as unknown as OrchestrationV2ThreadShell;
            }),
        }),
      ),
    );
    const titles = seen
      .filter((item) => item.includes(":") && !item.endsWith(":slow"))
      .map((item) => item.split(":")[1]!);
    const order = ["c1", "c2", "c3"];
    for (let index = 1; index < titles.length; index++) {
      assert.isTrue(
        order.indexOf(titles[index]!) >= order.indexOf(titles[index - 1]!),
        `shell went backwards: ${seen.join(" ")}`,
      );
    }
    assert.include(titles, "c3");
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// Shell subscribers over gated reads: the shell read of a gated thread waits until released.
const withGatedShellReads = <A, E, R>(
  input: {
    readonly gates: ReadonlyMap<string, Deferred.Deferred<void>>;
    readonly title: (id: string) => string;
  },
  run: Effect.Effect<A, E, R>,
) =>
  run.pipe(
    Effect.provide(
      layerShellSubscribers({
        getThreadShell: (id) =>
          Effect.gen(function* () {
            const gate = input.gates.get(id);
            if (gate !== undefined) yield* Deferred.await(gate);
            return {
              id,
              archivedAt: null,
              title: input.title(id),
            } as unknown as OrchestrationV2ThreadShell;
          }),
      }),
    ),
  );

const settleHub = Effect.gen(function* () {
  for (let round = 0; round < 4; round++) {
    for (let step = 0; step < 200; step++) yield* Effect.yieldNow;
    yield* TestClock.adjust(0);
  }
});

const subscribeInto = (afterSequence: number, seen: Array<string>) =>
  Effect.gen(function* () {
    const stream = yield* subscribeOrchestrationV2Shell({
      afterSequence,
      requestCompletionMarker: true,
    });
    yield* stream.pipe(
      Stream.runForEach((item) =>
        Effect.sync(() => {
          seen.push(
            item.kind === "thread.updated"
              ? `${item.thread.id}@${item.sequence}:${(item.thread as { title: string }).title}`
              : item.kind === "project.updated"
                ? `project.updated@${item.sequence}:${item.project.title}`
                : item.kind,
          );
        }),
      ),
      Effect.forkScoped,
    );
  });

// Sol (hub-only, RC2 pass 2) SOL-1: the replay read A before the hub did, then waited on B; the hub's newer A must survive.
it.effect(
  "a hub delta read after the replay's read of its thread is delivered even if the replay finished later",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const a = ThreadId.make("thread:order-a");
      const b = ThreadId.make("thread:order-b");
      let titleA = "a1";
      const gateB = yield* Deferred.make<void>();
      const seen: Array<string> = [];
      yield* insertShellEvent(sql, a, 0);
      yield* insertShellEvent(sql, b, 0);
      yield* withGatedShellReads(
        {
          gates: new Map([[b, gateB]]),
          title: (id) => (id === a ? titleA : "b"),
        },
        Effect.gen(function* () {
          yield* subscribeInto(0, seen);
          yield* settleHub;
          // The replay has read A (a1) and waits on B; the hub now reads A anew.
          titleA = "a2";
          yield* publishShellEvent(sql, a, 1);
          yield* settleHub;
          yield* Deferred.succeed(gateB, undefined);
          yield* settleHub;
          yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
          yield* settleHub;
        }).pipe(Effect.scoped),
      );
      const afterSync = seen.slice(seen.indexOf("synchronized") + 1);
      assert.include(seen.join(" "), `${a}@`, seen.join(" "));
      assert.isTrue(
        afterSync.some((item) => item.endsWith(":a2")),
        `newer A lost: ${seen.join(" ")}`,
      );
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// SOL-2: a hub batch that read A before the replay did (the shared read then expired) and publishes
// after the live tail opened must not deliver the older A.
it.effect(
  "a hub batch published after the live tail opened cannot deliver a shell older than the replay's",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const a = ThreadId.make("thread:late-a");
      const c = ThreadId.make("thread:late-c");
      const slow = Array.from({ length: 9 }, (_, index) =>
        ThreadId.make(`thread:late-slow-${index}`),
      );
      let titleA = "a1";
      const gateC = yield* Deferred.make<void>();
      const gateSlow = yield* Deferred.make<void>();
      const keeper: Array<string> = [];
      const seen: Array<string> = [];
      yield* withGatedShellReads(
        {
          gates: new Map([[c, gateC], ...slow.map((id) => [id, gateSlow] as const)]),
          title: (id) => (id === a ? titleA : id === c ? "c" : "slow"),
        },
        Effect.gen(function* () {
          yield* subscribeInto(0, keeper);
          yield* settleHub;
          // The resuming subscriber's replay has nine gated threads ahead of A, so its read of A comes late.
          for (const [index, id] of slow.entries()) yield* insertShellEvent(sql, id, index);
          const last = yield* insertShellEvent(sql, a, 0);
          yield* subscribeInto(0, seen);
          yield* settleHub;
          // Meanwhile one hub batch reads A (a1) and then waits on C, so it publishes late.
          const cSeq = yield* insertShellEvent(sql, c, 1);
          const aSeq = yield* insertShellEvent(sql, a, 1);
          const events = yield* store
            .readApplicationEvents({ afterSequence: cSeq - 1, throughSequence: aSeq })
            .pipe(Stream.runCollect);
          yield* store.publishCommitted(events);
          yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
          yield* settleHub;
          // The shared read of A expires before the replay gets to A, which then reads a2.
          yield* TestClock.adjust(ThreadManagement.SHARED_SHELL_READ_TTL_MS + 1_000);
          titleA = "a2";
          yield* Deferred.succeed(gateSlow, undefined);
          yield* settleHub;
          assert.include(seen, "synchronized", seen.join(" "));
          yield* Deferred.succeed(gateC, undefined);
          yield* settleHub;
          yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
          yield* settleHub;
          assert.isAbove(aSeq, last);
        }).pipe(Effect.scoped),
      );
      // The late batch did publish: the keeper got C and the older A.
      assert.include(keeper.join(" "), `${c}@`, `keeper: ${keeper.join(" ")}`);
      const titlesA = seen
        .filter((item) => item.startsWith(`${a}@`))
        .map((item) => item.slice(item.lastIndexOf(":") + 1));
      assert.deepEqual(titlesA, ["a2"], `older A delivered after the newer one: ${seen.join(" ")}`);
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// Reviewer scenario (F2b): two project updates land while the resume replay is still reading that project.
it.effect(
  "a resumed shell subscriber never receives a project shell older than one its replay sent",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const threadId = ThreadId.make("thread:project-order");
      const projectId = ProjectId.make("project:order");
      const now = "2026-10-31T00:00:00.000Z";
      let title = "c1";
      let version = 0;
      let published = 0;
      const insertProject = () =>
        sql<{ sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          ${`project-order:${++version}`}, 'project', ${projectId}, ${version}, 'project.meta-updated',
          ${now}, 'server', ${JSON.stringify({ projectId, title, updatedAt: now })}, '{}', 2
        ) RETURNING sequence
      `.pipe(Effect.map(([row]) => row!.sequence));
      const publishAll = Effect.gen(function* () {
        const events = yield* store
          .readApplicationEvents({ afterSequence: published, throughSequence: 1_000 })
          .pipe(Stream.runCollect);
        published = events.at(-1)?.sequence ?? published;
        yield* store.publishCommitted(events);
      });
      // The replay's reads of the thread and of the project wait until released.
      const replayGate = yield* Deferred.make<void>();
      let threadReads = 0;
      let projectReads = 0;
      const seen: Array<string> = [];
      yield* insertShellEvent(sql, threadId, 0);
      published = yield* insertProject();
      yield* Effect.gen(function* () {
        yield* subscribeInto(0, seen);
        yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
        yield* settleHub;
        title = "c2";
        yield* insertProject();
        yield* publishAll;
        yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
        yield* settleHub;
        title = "c3";
        yield* insertProject();
        yield* publishAll;
        yield* Deferred.succeed(replayGate, undefined);
        yield* settleHub;
        yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
        yield* settleHub;
        yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
        yield* settleHub;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          layerShellSubscribers({
            getThreadShell: (id) =>
              Effect.gen(function* () {
                if (threadReads++ === 0) yield* Deferred.await(replayGate);
                return {
                  id,
                  archivedAt: null,
                  title: "t",
                } as unknown as OrchestrationV2ThreadShell;
              }),
            getProjectRow: () =>
              Effect.gen(function* () {
                if (projectReads++ === 0) yield* Deferred.await(replayGate);
                return Option.some({
                  id: projectId,
                  title,
                  workspaceRoot: "/tmp/project-order",
                  defaultModelSelection: null,
                  scripts: [],
                  createdAt: now,
                  updatedAt: now,
                } as unknown as OrchestrationProjectShell);
              }),
          }),
        ),
      );
      const titles = seen
        .filter((item) => item.startsWith("project.updated"))
        .map((item) => item.slice(item.lastIndexOf(":") + 1));
      const order = ["c1", "c2", "c3"];
      for (let index = 1; index < titles.length; index++) {
        assert.isTrue(
          order.indexOf(titles[index]!) >= order.indexOf(titles[index - 1]!),
          `project went backwards: ${seen.join(" ")}`,
        );
      }
      assert.include(titles, "c3", seen.join(" "));
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// Review R1: a project's repository identity changes without a project event. A shared project
// read must add it per caller, or a resume replay that joins the hub's read sends an identity
// older than the one the client's own initial items carried, until the next project event.
it.effect(
  "a resumed shell subscriber never receives a repository identity older than its initial items sent",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const projectId = ProjectId.make("project:identity");
      const now = "2026-10-31T00:00:00.000Z";
      const identity = {
        canonicalKey: "github.com/x/y",
        displayName: "x/y",
      } as unknown as RepositoryIdentity;
      let resolved = false;
      const row = {
        id: projectId,
        title: "p",
        workspaceRoot: "/tmp/project-identity",
        defaultModelSelection: null,
        scripts: [],
        createdAt: now,
        updatedAt: now,
      } as unknown as OrchestrationProjectShell;
      const enrichment = () => ({
        repositoryIdentity: resolved ? identity : null,
        faviconPath: null,
        repositoryIdentityResolved: resolved,
      });
      const [inserted] = yield* sql<{ sequence: number }>`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, actor_kind, payload_json, metadata_json, application_event_version
        ) VALUES (
          'project-identity:1', 'project', ${projectId}, 1, 'project.meta-updated',
          ${now}, 'server', ${JSON.stringify({ projectId, title: "p", updatedAt: now })}, '{}', 2
        ) RETURNING sequence
      `;
      const sequence = inserted!.sequence;
      const seen: Array<string> = [];
      yield* Effect.gen(function* () {
        // A live watcher keeps the hub running; the hub reads the project while its identity is unresolved.
        yield* subscribeInto(0, []);
        yield* settleHub;
        const events = yield* store
          .readApplicationEvents({ afterSequence: 0, throughSequence: sequence })
          .pipe(Stream.runCollect);
        yield* store.publishCommitted(events);
        yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
        yield* settleHub;
        // The workspace's repository identity resolves, well inside the shared-read TTL.
        yield* TestClock.adjust(1_000);
        resolved = true;
        // A client resumes from before the event; its replay joins the hub's read of the project.
        const stream = yield* subscribeOrchestrationV2Shell({
          afterSequence: 0,
          requestCompletionMarker: true,
        });
        yield* stream.pipe(
          Stream.runForEach((item) =>
            Effect.sync(() => {
              seen.push(
                item.kind === "project.updated"
                  ? `project.updated@${item.sequence}:${item.project.repositoryIdentity === null ? "unresolved" : "resolved"}`
                  : item.kind,
              );
            }),
          ),
          Effect.forkScoped,
        );
        yield* settleHub;
        yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
        yield* settleHub;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          layerShellSubscribers({
            getThreadShell: () => Effect.die("no thread is read"),
            getProjectRow: () => Effect.succeedSome(row),
            getAvailable: () => Effect.sync(enrichment),
          }),
        ),
      );
      assert.deepEqual(
        seen.filter((item) => item.startsWith("project.updated")),
        [`project.updated@${sequence}:resolved`],
        seen.join(" "),
      );
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// Sol SOL-5: a hub batch that waits on another aggregate must not deliver a project row with a
// repository identity older than the refresh this subscriber already sent (no project event in
// the replay, so no read ordinal guards the delta).
it.effect(
  "a delayed hub batch never sends a repository identity older than a refresh the subscriber already sent",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* OrchestrationEventStore.OrchestrationEventStore;
      const threadId = ThreadId.make("thread:identity-order");
      const projectId = ProjectId.make("project:identity-order");
      const now = "2026-10-31T00:00:00.000Z";
      const identityA = { canonicalKey: "repo:a" } as unknown as RepositoryIdentity;
      const identityB = { canonicalKey: "repo:b" } as unknown as RepositoryIdentity;
      let identity = identityA;
      const row = {
        id: projectId,
        title: "p",
        workspaceRoot: "/tmp/identity-order",
        defaultModelSelection: null,
        scripts: [],
        createdAt: now,
        updatedAt: now,
      } as unknown as OrchestrationProjectShell;
      const changes = yield* PubSub.unbounded<ProjectEnrichment.ProjectEnrichmentChange>();
      const threadGate = yield* Deferred.make<void>();
      let threadReads = 0;
      const keyOf = (project: OrchestrationProjectShell) =>
        project.repositoryIdentity?.canonicalKey ?? "none";
      const seen: Array<string> = [];
      const highWater = yield* insertShellEvent(sql, threadId, 0);
      yield* Effect.gen(function* () {
        // Resume at the high water: an empty replay, so no project read orders the live tail.
        const stream = yield* subscribeOrchestrationV2Shell({
          afterSequence: highWater,
          requestCompletionMarker: true,
        });
        yield* stream.pipe(
          Stream.runForEach((item) =>
            Effect.sync(() => {
              seen.push(
                item.kind === "project.updated"
                  ? `project.updated@${item.sequence}:${keyOf(item.project)}`
                  : item.kind === "snapshot"
                    ? `snapshot:${item.snapshot.projects.map(keyOf).join(",")}`
                    : item.kind,
              );
            }),
          ),
          Effect.forkScoped,
        );
        yield* settleHub;
        // A project event and a thread event share one hub batch; the thread's read waits.
        const [projectRow] = yield* sql<{ sequence: number }>`
          INSERT INTO orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, actor_kind, payload_json, metadata_json, application_event_version
          ) VALUES (
            'identity-order:1', 'project', ${projectId}, 1, 'project.meta-updated',
            ${now}, 'server', ${JSON.stringify({ projectId, title: "p", updatedAt: now })}, '{}', 2
          ) RETURNING sequence
        `;
        const projectSequence = projectRow!.sequence;
        yield* insertShellEvent(sql, threadId, 1);
        const events = yield* store
          .readApplicationEvents({ afterSequence: highWater, throughSequence: highWater + 2 })
          .pipe(Stream.runCollect);
        yield* store.publishCommitted(events);
        yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
        yield* settleHub;
        // The identity changes while the batch waits; the subscriber gets the refresh first.
        identity = identityB;
        yield* PubSub.publish(changes, {
          workspaceRoot: row.workspaceRoot,
          enrichment: {
            repositoryIdentity: identityB,
            faviconPath: null,
            repositoryIdentityResolved: true,
          },
          repositoryIdentityResolved: true,
        });
        yield* TestClock.adjust(25);
        yield* settleHub;
        yield* Deferred.succeed(threadGate, undefined);
        yield* settleHub;
        yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
        yield* settleHub;
        yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
        yield* settleHub;
        assert.isTrue(
          seen.some((item) => item.startsWith(`project.updated@${projectSequence}:`)),
          `the delayed delta was not delivered: ${seen.join(" ")}`,
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          layerShellSubscribers({
            getThreadShell: (id) =>
              Effect.gen(function* () {
                if (threadReads++ === 0) yield* Deferred.await(threadGate);
                return {
                  id,
                  archivedAt: null,
                  title: "t",
                } as unknown as OrchestrationV2ThreadShell;
              }),
            getProjectRow: () => Effect.succeedSome(row),
            listShells: () => Effect.succeed([row]),
            getAvailable: () =>
              Effect.sync(() => ({
                repositoryIdentity: identity,
                faviconPath: null,
                repositoryIdentityResolved: true,
              })),
            enrichmentChanges: changes,
          }),
        ),
      );
      const firstB = seen.findIndex((item) => item.includes("repo:b"));
      assert.isAtLeast(firstB, 0, seen.join(" "));
      const stale = seen.slice(firstB + 1).filter((item) => item.includes("repo:a"));
      assert.deepEqual(stale, [], `identity went back after the refresh: ${seen.join(" ")}`);
    }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

// Sol SOL-6: the live tail keeps the hub's batch boundaries; the RPC server waits for a client
// acknowledgement per chunk, so one chunk per delta would make a burst one round trip per delta.
it.effect("live thread deltas of one hub batch reach the subscriber in one chunk", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = yield* OrchestrationEventStore.OrchestrationEventStore;
    const threads = ["thread:chunk-a", "thread:chunk-b", "thread:chunk-c"].map((id) =>
      ThreadId.make(id),
    );
    const highWater = yield* insertShellEvent(sql, threads[0]!, 0);
    const chunks: Array<Array<string>> = [];
    yield* Effect.gen(function* () {
      const stream = yield* subscribeOrchestrationV2Shell({
        afterSequence: highWater,
        requestCompletionMarker: true,
      });
      yield* stream.pipe(
        Stream.runForEachArray((items) =>
          Effect.sync(() => {
            chunks.push(
              items.map((item) =>
                item.kind === "thread.updated" ? `${item.thread.id}@${item.sequence}` : item.kind,
              ),
            );
          }),
        ),
        Effect.forkScoped,
      );
      yield* settleHub;
      // Three threads change inside one batch window.
      for (const [index, threadId] of threads.entries()) {
        yield* insertShellEvent(sql, threadId, index === 0 ? 1 : 0);
      }
      const events = yield* store
        .readApplicationEvents({ afterSequence: highWater, throughSequence: highWater + 3 })
        .pipe(Stream.runCollect);
      yield* store.publishCommitted(events);
      yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
      yield* settleHub;
      yield* TestClock.adjust(ShellDeltaHub.SHELL_DELTA_BATCH_WINDOW);
      yield* settleHub;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        layerShellSubscribers({
          getThreadShell: (id) =>
            Effect.succeed({
              id,
              archivedAt: null,
              title: "t",
            } as unknown as OrchestrationV2ThreadShell),
        }),
      ),
    );
    const live = chunks.filter((chunk) => chunk.some((item) => item.includes("@")));
    assert.deepEqual(
      live,
      [threads.map((threadId, index) => `${threadId}@${highWater + 1 + index}`)],
      JSON.stringify(chunks),
    );
  }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
);

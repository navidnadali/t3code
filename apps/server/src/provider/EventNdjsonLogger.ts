// @effect-diagnostics nodeBuiltinImport:off
/** Native and canonical views share write state to avoid races on the same thread file. */
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import type { ThreadId } from "@t3tools/contracts";
import type * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import { ProviderIoStageTiming } from "@t3tools/provider-core/server/ProviderIoStageTiming";
import { makeAsyncRotatingFileSink } from "@t3tools/shared/logging";
import { errorTag } from "@t3tools/shared/observability";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SynchronizedRef from "effect/SynchronizedRef";

import { toSafeThreadAttachmentSegment } from "../attachmentStore.ts";
import type * as ResourceAttribution from "../resourceTelemetry/ResourceAttribution.ts";
import {
  type EventNdjsonStream,
  formatEventNdjsonLine,
  serializedRecordProblem,
  serializeEventNdjsonRecord,
} from "./EventNdjsonRecord.ts";

export {
  boundProviderEventForLogging,
  type EventNdjsonStream,
  shouldPersistProviderEvent,
} from "./EventNdjsonRecord.ts";

const MEBIBYTE = 1024 * 1024;
const DAY_MS = 24 * 60 * 60 * 1_000;
const DEFAULT_MAX_BYTES = 10 * MEBIBYTE;
const DEFAULT_MAX_FILES = 10;
const DEFAULT_BATCH_WINDOW_MS = 1_000;
const DEFAULT_MAX_TOTAL_BYTES = 512 * MEBIBYTE;
const DEFAULT_MAX_AGE_MS = 14 * DAY_MS;
const DEFAULT_RETENTION_CHECK_INTERVAL_MS = 5 * 60 * 1_000;
const DEFAULT_MAX_BUFFERED_BYTES = MEBIBYTE;
const DEFAULT_MAX_BUFFERED_RECORDS = 512;
const GLOBAL_THREAD_SEGMENT = "_global";
const LOG_SCOPE = "provider-observability";

export interface EventNdjsonLogStore {
  readonly filePath: string;
  readonly logger: (stream: EventNdjsonStream) => ProviderEventLoggers.EventNdjsonLogger;
  readonly close: () => Effect.Effect<void>;
}

export interface EventNdjsonLogStoreOptions {
  readonly maxBytes?: number;
  readonly maxFiles?: number;
  readonly batchWindowMs?: number;
  readonly attribution?: ResourceAttribution.ResourceAttribution["Service"];
}

export class EventNdjsonLogDirectoryError extends Schema.TaggedError<EventNdjsonLogDirectoryError>()(
  "EventNdjsonLogDirectoryError",
  {
    directory: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to create provider event log directory '${this.directory}'`;
  }
}

export type EventNdjsonLogStoreError = EventNdjsonLogDirectoryError;

interface ResolvedOptions {
  readonly maxBytes: number;
  readonly maxFiles: number;
  readonly batchWindowMs: number;
  readonly maxTotalBytes: number;
  readonly maxAgeMs: number;
  readonly retentionCheckIntervalMs: number;
  readonly maxBufferedBytes: number;
  readonly maxBufferedRecords: number;
  readonly attribution: ResourceAttribution.ResourceAttribution["Service"] | undefined;
}

export interface PendingRecord {
  readonly stream: EventNdjsonStream;
  readonly threadSegment: string;
  readonly line: string;
  readonly bytes: number;
}

type AsyncFileSink = Effect.Success<ReturnType<typeof makeAsyncRotatingFileSink>>;

interface StoreState {
  readonly pending: Array<PendingRecord>;
  readonly pendingBytes: number;
  readonly sinks: ReadonlyMap<string, AsyncFileSink>;
  readonly writtenSinceRetention: ReadonlySet<string>;
  readonly flushScheduled: boolean;
  readonly closed: boolean;
  readonly lastRetentionAt: number;
}

interface AttributionSummary {
  readonly stream: EventNdjsonStream;
  readonly count: number;
  readonly logicalWriteBytes: number;
}

interface FileOperationFailure {
  readonly filePath: string;
  readonly cause: unknown;
}

interface RetentionResult {
  readonly failures: ReadonlyArray<FileOperationFailure>;
}

interface DrainResult {
  readonly attributions: ReadonlyArray<AttributionSummary>;
  readonly failures: ReadonlyArray<FileOperationFailure>;
}

function logWarning(message: string, context: Record<string, unknown>): Effect.Effect<void> {
  return Effect.logWarning(message, context).pipe(Effect.annotateLogs({ scope: LOG_SCOPE }));
}

function resolveThreadSegment(raw: string | null | undefined): string {
  const normalized = typeof raw === "string" ? toSafeThreadAttachmentSegment(raw) : null;
  return normalized ?? GLOBAL_THREAD_SEGMENT;
}

function providerLogPrefix(filePath: string): string {
  const basename = NodePath.basename(filePath);
  const extension = NodePath.extname(basename);
  return `${extension.length > 0 ? basename.slice(0, -extension.length) : basename}.`;
}

function providerLogPath(directory: string, prefix: string, threadSegment: string): string {
  return NodePath.join(directory, `${prefix}${threadSegment}.log`);
}

function loggedProvider(event: unknown): string {
  if (typeof event !== "object" || event === null) return "unknown";
  const nested = Reflect.get(event, "event");
  const provider =
    Reflect.get(event, "provider") ??
    (typeof nested === "object" && nested !== null ? Reflect.get(nested, "provider") : undefined);
  return typeof provider === "string" ? provider : "unknown";
}

export const writeBatchedMessages = Effect.fnUntraced(function* (
  sink: AsyncFileSink,
  records: ReadonlyArray<PendingRecord>,
  maxBytes: number,
  onWritten: (records: ReadonlyArray<PendingRecord>) => void,
) {
  let pendingRecords: Array<PendingRecord> = [];
  let pendingBytes = 0;

  const flush = Effect.gen(function* () {
    if (pendingRecords.length === 0) return;
    const writtenRecords = pendingRecords;
    yield* sink.write(writtenRecords.map((record) => record.line).join(""));
    onWritten(writtenRecords);
    pendingRecords = [];
    pendingBytes = 0;
  });

  for (const record of records) {
    if (pendingBytes > 0 && pendingBytes + record.bytes > maxBytes) {
      yield* flush;
    }
    pendingRecords.push(record);
    pendingBytes += record.bytes;
    if (pendingBytes >= maxBytes) {
      yield* flush;
    }
  }
  yield* flush;
});

async function isProviderLogFile(
  filePath: string,
  fileName: string,
  filePrefix: string,
): Promise<boolean> {
  if (!/\.log(?:\.\d+)?$/u.test(fileName)) return false;
  if (fileName.startsWith(filePrefix)) return true;

  const descriptor = await NodeFS.promises.open(filePath, "r");
  try {
    const header = Buffer.alloc(256);
    const { bytesRead } = await descriptor.read(header, 0, header.byteLength, 0);
    return /^\[[^\]\r\n]+\] (?:NTIVE|CANON|ORCH): /u.test(header.toString("utf8", 0, bytesRead));
  } finally {
    await descriptor.close();
  }
}

async function enforceRetention(input: {
  readonly directory: string;
  readonly maxTotalBytes: number;
  readonly maxAgeMs: number;
  readonly activeFilePaths: ReadonlySet<string>;
  readonly filePrefix: string;
  readonly now: number;
}): Promise<RetentionResult> {
  const failures: Array<FileOperationFailure> = [];
  const files: Array<{ filePath: string; mtimeMs: number; size: number }> = [];

  let entries: ReadonlyArray<NodeFS.Dirent>;
  try {
    entries = await NodeFS.promises.readdir(input.directory, { withFileTypes: true });
  } catch (cause) {
    return { failures: [{ filePath: input.directory, cause }] };
  }

  for (const entry of entries) {
    if (!entry.isFile()) continue;
    const filePath = NodePath.join(input.directory, entry.name);
    try {
      if (!(await isProviderLogFile(filePath, entry.name, input.filePrefix))) continue;
      const stat = await NodeFS.promises.stat(filePath);
      files.push({ filePath, mtimeMs: stat.mtimeMs, size: stat.size });
    } catch (cause) {
      failures.push({ filePath, cause });
    }
  }

  let totalBytes = files.reduce((total, file) => total + file.size, 0);
  const remove = async (file: (typeof files)[number]) => {
    if (input.activeFilePaths.has(file.filePath)) return false;
    try {
      await NodeFS.promises.rm(file.filePath, { force: true });
      totalBytes -= file.size;
      return true;
    } catch (cause) {
      failures.push({ filePath: file.filePath, cause });
      return false;
    }
  };

  const retained: typeof files = [];
  for (const file of files) {
    if (input.now - file.mtimeMs <= input.maxAgeMs || !(await remove(file))) retained.push(file);
  }

  for (const file of retained.toSorted(
    (left, right) => left.mtimeMs - right.mtimeMs || left.filePath.localeCompare(right.filePath),
  )) {
    if (totalBytes <= input.maxTotalBytes) break;
    await remove(file);
  }

  return { failures };
}

function resolveOptions(options: EventNdjsonLogStoreOptions): Effect.Effect<ResolvedOptions> {
  const resolved = {
    maxBytes: options.maxBytes ?? DEFAULT_MAX_BYTES,
    maxFiles: options.maxFiles ?? DEFAULT_MAX_FILES,
    batchWindowMs: options.batchWindowMs ?? DEFAULT_BATCH_WINDOW_MS,
    maxTotalBytes: DEFAULT_MAX_TOTAL_BYTES,
    maxAgeMs: DEFAULT_MAX_AGE_MS,
    retentionCheckIntervalMs: DEFAULT_RETENTION_CHECK_INTERVAL_MS,
    maxBufferedBytes: DEFAULT_MAX_BUFFERED_BYTES,
    maxBufferedRecords: DEFAULT_MAX_BUFFERED_RECORDS,
    attribution: options.attribution,
  } satisfies ResolvedOptions;

  return Effect.succeed(resolved);
}

const drainPending = Effect.fnUntraced(function* (input: {
  readonly directory: string;
  readonly options: ResolvedOptions;
  readonly state: StoreState;
  readonly filePrefix: string;
  readonly now: number;
  readonly timerFired: boolean;
  readonly close: boolean;
  readonly makeSink: (
    filePath: string,
  ) => Effect.Effect<AsyncFileSink, Effect.Error<ReturnType<typeof makeAsyncRotatingFileSink>>>;
}): Effect.fn.Return<readonly [DrainResult, StoreState]> {
  if (input.state.closed) {
    return [{ attributions: [], failures: [] }, input.state];
  }

  const sinks = new Map(input.state.sinks);
  const writtenSegments = new Set(input.state.writtenSinceRetention);
  const failures: Array<FileOperationFailure> = [];
  const attributionByStream = new Map<
    EventNdjsonStream,
    { count: number; logicalWriteBytes: number }
  >();
  const recordsBySegment = new Map<string, Array<PendingRecord>>();

  for (const record of input.state.pending) {
    const records = recordsBySegment.get(record.threadSegment) ?? [];
    records.push(record);
    recordsBySegment.set(record.threadSegment, records);
  }

  for (const [threadSegment, records] of recordsBySegment) {
    const filePath = providerLogPath(input.directory, input.filePrefix, threadSegment);
    let sink = sinks.get(threadSegment);
    if (!sink) {
      const created = yield* input.makeSink(filePath).pipe(Effect.result);
      if (Result.isFailure(created)) {
        failures.push({ filePath, cause: created.failure });
        continue;
      }
      sink = created.success;
      sinks.set(threadSegment, sink);
    }
    writtenSegments.add(threadSegment);

    const written = yield* writeBatchedMessages(
      sink,
      records,
      input.options.maxBytes,
      (writtenRecords) => {
        for (const record of writtenRecords) {
          const current = attributionByStream.get(record.stream) ?? {
            count: 0,
            logicalWriteBytes: 0,
          };
          attributionByStream.set(record.stream, {
            count: current.count + 1,
            logicalWriteBytes: current.logicalWriteBytes + record.bytes,
          });
        }
      },
    ).pipe(Effect.result);
    if (Result.isFailure(written)) {
      sinks.delete(threadSegment);
      failures.push({ filePath, cause: written.failure });
    }
  }

  const retentionDue =
    input.now - input.state.lastRetentionAt >= input.options.retentionCheckIntervalMs;
  if (retentionDue) {
    // A sink idle for a whole retention interval reopens from disk on its next
    // write, so retention may remove its files without leaving a stale size.
    for (const threadSegment of sinks.keys()) {
      if (!writtenSegments.has(threadSegment)) sinks.delete(threadSegment);
    }
  }
  const retention = retentionDue
    ? yield* Effect.promise(() =>
        enforceRetention({
          directory: input.directory,
          maxTotalBytes: input.options.maxTotalBytes,
          maxAgeMs: input.options.maxAgeMs,
          activeFilePaths: new Set(
            Array.from(sinks.keys(), (threadSegment) =>
              providerLogPath(input.directory, input.filePrefix, threadSegment),
            ),
          ),
          filePrefix: input.filePrefix,
          now: input.now,
        }),
      )
    : { failures: [] };

  return [
    {
      attributions: Array.from(attributionByStream, ([stream, value]) => ({
        stream,
        ...value,
      })),
      failures: [...failures, ...retention.failures],
    },
    {
      pending: [],
      pendingBytes: 0,
      sinks,
      writtenSinceRetention: retentionDue ? new Set() : writtenSegments,
      flushScheduled: input.timerFired ? false : input.state.flushScheduled,
      closed: input.close,
      lastRetentionAt: retentionDue ? input.now : input.state.lastRetentionAt,
    },
  ];
});

export const makeEventNdjsonLogStore = Effect.fnUntraced(function* (
  filePath: string,
  options: EventNdjsonLogStoreOptions = {},
): Effect.fn.Return<
  EventNdjsonLogStore,
  EventNdjsonLogStoreError,
  FileSystem.FileSystem | Path.Path
> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const stageTiming = yield* ProviderIoStageTiming;
  const resolved = yield* resolveOptions(options);
  const directory = NodePath.dirname(filePath);
  const filePrefix = providerLogPrefix(filePath);

  yield* fs
    .makeDirectory(directory, { recursive: true })
    .pipe(Effect.mapError((cause) => new EventNdjsonLogDirectoryError({ directory, cause })));
  const makeSink = (filePath: string) =>
    makeAsyncRotatingFileSink({
      filePath,
      maxBytes: resolved.maxBytes,
      maxFiles: resolved.maxFiles,
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
    );

  const initializedAt = yield* Clock.currentTimeMillis;
  const initialRetention = yield* Effect.promise(() =>
    enforceRetention({
      directory,
      maxTotalBytes: resolved.maxTotalBytes,
      maxAgeMs: resolved.maxAgeMs,
      activeFilePaths: new Set(),
      filePrefix,
      now: initializedAt,
    }),
  );
  for (const failure of initialRetention.failures) {
    yield* logWarning("provider event log retention failed", {
      filePath: failure.filePath,
      errorTag: errorTag(failure.cause),
    });
  }

  const stateRef = yield* SynchronizedRef.make<StoreState>({
    pending: [],
    pendingBytes: 0,
    sinks: new Map(),
    writtenSinceRetention: new Set(),
    flushScheduled: false,
    closed: false,
    lastRetentionAt: initializedAt,
  });
  const timerScope = yield* Scope.make();

  const flush = Effect.fnUntraced(function* (timerFired: boolean, close: boolean) {
    const startedAt = yield* Clock.currentTimeMillis;
    const result = yield* SynchronizedRef.modifyEffect(stateRef, (state) =>
      drainPending({
        directory,
        options: resolved,
        state,
        filePrefix,
        now: startedAt,
        timerFired,
        close,
        makeSink,
      }),
    );

    for (const failure of result.failures) {
      yield* logWarning("provider event log write or retention failed", {
        filePath: failure.filePath,
        errorTag: errorTag(failure.cause),
      });
    }

    if (resolved.attribution && result.attributions.length > 0) {
      const completedAt = yield* Clock.currentTimeMillis;
      const durationMs = Math.max(0, completedAt - startedAt);
      const totalBytes = result.attributions.reduce(
        (total, entry) => total + entry.logicalWriteBytes,
        0,
      );
      yield* Effect.forEach(
        result.attributions,
        (entry) =>
          resolved.attribution?.record({
            component: "provider-event-log",
            operation: `${entry.stream}.append`,
            logicalWriteBytes: entry.logicalWriteBytes,
            count: entry.count,
            durationMs:
              totalBytes === 0
                ? 0
                : Math.round(durationMs * (entry.logicalWriteBytes / totalBytes)),
          }) ?? Effect.void,
        { discard: true },
      );
    }
  }, Effect.uninterruptible);

  const scheduleFlush = Effect.fnUntraced(function* () {
    yield* Effect.forkIn(
      Effect.sleep(resolved.batchWindowMs).pipe(Effect.andThen(flush(true, false))),
      timerScope,
      { startImmediately: true },
    );
  });

  const close = Effect.fnUntraced(function* () {
    yield* flush(false, true);
    yield* Scope.close(timerScope, Exit.void);
  });

  const loggerViews = new Map<EventNdjsonStream, ProviderEventLoggers.EventNdjsonLogger>();
  const logger = (stream: EventNdjsonStream): ProviderEventLoggers.EventNdjsonLogger => {
    const existing = loggerViews.get(stream);
    if (existing) return existing;

    const appendRecord = Effect.fnUntraced(function* (
      payload: string,
      threadId: ThreadId | null,
      timed: { readonly event: unknown; readonly startedAt: number } | undefined,
    ) {
      const observedAt = yield* DateTime.now.pipe(Effect.map(DateTime.formatIso));
      const line = formatEventNdjsonLine(observedAt, stream, payload);
      const bytes = Buffer.byteLength(line);
      if (timed !== undefined) {
        stageTiming
          ?.forProvider(loggedProvider(timed.event))
          .record("logSerialize", stageTiming.now() - timed.startedAt, bytes);
      }
      const action = yield* SynchronizedRef.modifyEffect(stateRef, (state) => {
        if (state.closed) {
          return Effect.succeed([{ flush: false }, state] as const);
        }
        const pending = state.pending;
        pending.push({ stream, threadSegment: resolveThreadSegment(threadId), line, bytes });
        const pendingBytes = state.pendingBytes + bytes;
        const flush =
          resolved.batchWindowMs === 0 ||
          pending.length >= resolved.maxBufferedRecords ||
          pendingBytes >= resolved.maxBufferedBytes;
        const schedule = !flush && !state.flushScheduled;
        const nextState = {
          ...state,
          pending,
          pendingBytes,
          flushScheduled: state.flushScheduled || schedule,
        };
        return (schedule ? scheduleFlush() : Effect.void).pipe(
          Effect.as([{ flush }, nextState] as const),
        );
      }).pipe(Effect.uninterruptible);

      if (action.flush) {
        yield* flush(false, false);
      }
    });

    const writeSerialized = (
      record: ProviderEventLoggers.SerializedProviderEventRecord,
      threadId: ThreadId | null,
    ) => {
      const problem = serializedRecordProblem(stream, record);
      return problem === undefined
        ? appendRecord(record.payload, threadId, undefined)
        : logWarning("rejected a serialized provider event log record", { stream, problem });
    };

    const write = Effect.fnUntraced(function* (event: unknown, threadId: ThreadId | null) {
      const startedAt = stageTiming?.now() ?? 0;
      const record = serializeEventNdjsonRecord(stream, event);
      if (record._tag === "Failed") {
        return yield* logWarning("failed to serialize provider event log record", {
          errorTag: errorTag(record.error),
        });
      }
      if (record._tag === "Serialized") {
        yield* appendRecord(
          record.payload,
          threadId,
          stageTiming === undefined ? undefined : { event, startedAt },
        );
      }
    });

    const view = {
      filePath,
      write,
      writeSerialized,
      close: () => Effect.void,
    } satisfies ProviderEventLoggers.EventNdjsonLogger;
    loggerViews.set(stream, view);
    return view;
  };

  return { filePath, logger, close } satisfies EventNdjsonLogStore;
});

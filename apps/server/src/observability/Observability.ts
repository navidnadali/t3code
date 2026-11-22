import * as HostProcess from "@t3tools/shared/HostProcess";
import * as HttpObservability from "@t3tools/shared/httpObservability";
import { makeLocalFileTracer, makeTraceSink } from "@t3tools/shared/observability";
import * as SharedObservability from "@t3tools/shared/observability";
import * as OtelEnvironment from "@t3tools/shared/otelEnvironment";
import { CurrentWaitObserver } from "@t3tools/shared/WaitObserver";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as References from "effect/References";
import * as Tracer from "effect/Tracer";
import * as OtlpExporter from "effect/observability/OtlpExporter";
import * as OtlpMetrics from "effect/observability/OtlpMetrics";
import * as OtlpTracer from "effect/observability/OtlpTracer";
import * as Statement from "effect/sql/Statement";

import * as ServerConfig from "../config.ts";
import * as ResourceAttribution from "../resourceTelemetry/ResourceAttribution.ts";
import * as ServerLogger from "../serverLogger.ts";
import { DEFAULT_SLOW_SPAN_THRESHOLD_MS } from "../diagnostics/TraceDiagnostics.ts";
import * as BrowserTraceCollector from "./BrowserTraceCollector.ts";
import * as CpuProfileMinutes from "./CpuProfileMinutes.ts";
import * as HeapAttribution from "./HeapAttribution.ts";
import * as OtlpExportFailures from "./OtlpExportFailures.ts";
import * as SpanMinutes from "./SpanMinutes.ts";
import * as WaitMinutes from "./WaitMinutes.ts";

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;

    const traces = config.otlpTracesExport;
    const metrics = config.otlpMetricsExport;
    // Keep serialization in context for the browser trace forwarder's exporter.
    const layerSerialization = SharedObservability.layerOtlpSerialization(traces.protocol);
    const resource = ServerConfig.otlpResource(config, yield* HostProcess.Hostname);
    const attribution = yield* ResourceAttribution.ResourceAttribution;

    const layerTraceReferences = Layer.mergeAll(
      Layer.succeed(Tracer.MinimumTraceLevel, config.traceMinLevel),
      Layer.succeed(References.TracerTimingEnabled, config.traceTimingEnabled),
      // Preserve SQL spans while awaiting connections so lock waits can be recorded on them.
      Layer.succeed(Statement.SpanPropagationEnabled, true),
      HttpObservability.layer,
    );

    const layerTracer = Layer.unwrap(
      Effect.gen(function* () {
        const sink = yield* makeTraceSink({
          filePath: config.serverTracePath,
          maxBytes: config.traceMaxBytes,
          maxFiles: config.traceMaxFiles,
          batchWindowMs: config.traceBatchWindowMs,
          onFlush: (stats) =>
            attribution.record({
              component: "server-trace",
              operation: "append",
              logicalWriteBytes: stats.logicalWriteBytes,
              count: stats.count,
              durationMs: stats.durationMs,
            }),
        });
        const delegate =
          config.otlpTracesUrl === undefined
            ? undefined
            : yield* OtlpTracer.make({
                url: config.otlpTracesUrl,
                exportInterval: `${traces.exportIntervalMs} millis`,
                headers: traces.headers,
                resource,
              }).pipe(Effect.provide(OtlpExportFailures.layer("traces")));

        // Without timing every span lasts 0 ms, so a slow SQL span would be summarised or
        // sampled out too.
        const sqlMinDurationMs = config.traceTimingEnabled
          ? (config.traceSqlMinDurationMs ?? 0)
          : 0;
        const sqlSampleEvery = config.traceTimingEnabled ? config.traceSqlSampleEvery : undefined;
        const path = yield* Path.Path;
        const waits =
          config.traceSpanMinutes === false ? undefined : WaitMinutes.makeWaitAggregate();
        const spanMinutes =
          waits === undefined
            ? undefined
            : yield* SpanMinutes.make(
                path.join(path.dirname(config.serverTracePath), SpanMinutes.SPAN_MINUTES_FILE_NAME),
                // The profile switches first: the waits may wait up to 2 s on thread sizes.
                [
                  ...(config.traceCpuProfileIntervalMs === undefined
                    ? []
                    : [
                        yield* CpuProfileMinutes.makeSection({
                          intervalMs: config.traceCpuProfileIntervalMs,
                        }),
                      ]),
                  waits.drain,
                ],
              ).pipe(
                Effect.catchCause((cause) =>
                  Effect.logWarning("Span minute log unavailable", cause).pipe(
                    Effect.as(undefined),
                  ),
                ),
              );
        const tracer = yield* makeLocalFileTracer({
          filePath: config.serverTracePath,
          maxBytes: config.traceMaxBytes,
          maxFiles: config.traceMaxFiles,
          batchWindowMs: config.traceBatchWindowMs,
          sink,
          ...(delegate ? { delegate } : {}),
          sqlMinDurationMs,
          ...(sqlSampleEvery === undefined
            ? {}
            : {
                sqlSample: {
                  every: sqlSampleEvery,
                  // A slow statement is always written, as the threshold's cap keeps it.
                  belowMs: DEFAULT_SLOW_SPAN_THRESHOLD_MS,
                },
              }),
          ...(spanMinutes ? { onSpanEnd: spanMinutes.record } : {}),
        });

        // Whether a SQL span is written is known only at its end, so none is measured.
        const heapAttribution = yield* HeapAttribution.make(
          tracer,
          sqlMinDurationMs > 0 || sqlSampleEvery !== undefined
            ? { skip: SharedObservability.isSqlSpanName }
            : {},
        );
        yield* heapAttribution.run;

        return Layer.mergeAll(
          Layer.succeed(Tracer.Tracer, heapAttribution.tracer),
          BrowserTraceCollector.layer(sink),
          // Sites measure waits only while the minute line can carry them.
          spanMinutes === undefined || waits === undefined
            ? Layer.empty
            : Layer.succeed(CurrentWaitObserver, waits.observer),
        );
      }),
    ).pipe(Layer.provide(OtlpExporter.layerFlusher), Layer.provideMerge(layerSerialization));

    const metricsLayer =
      config.otlpMetricsUrl === undefined
        ? Layer.empty
        : OtlpMetrics.layer({
            url: config.otlpMetricsUrl,
            exportInterval: `${metrics.exportIntervalMs} millis`,
            headers: metrics.headers,
            resource,
          }).pipe(
            Layer.provide(OtlpExportFailures.layer("metrics")),
            Layer.provide(SharedObservability.layerOtlpSerialization(metrics.protocol)),
          );

    const layerOtelWarnings = Layer.effectDiscard(
      Effect.forEach(config.otelEnvironment.warnings, (warning) => Effect.logWarning(warning)),
    );

    return layerOtelWarnings.pipe(
      Layer.provideMerge(
        Layer.mergeAll(ServerLogger.layer, layerTraceReferences, layerTracer, metricsLayer),
      ),
      Layer.provide(
        OtelEnvironment.layerResourceAttributes(config.otelEnvironment.resourceAttributes),
      ),
    );
  }),
);

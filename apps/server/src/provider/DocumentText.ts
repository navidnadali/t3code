// @effect-diagnostics nodeBuiltinImport:off -- the isolated parser process is an external runtime boundary.
import * as NodeURL from "node:url";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcess } from "effect/process";
import * as HostProcess from "@t3tools/shared/HostProcess";

import * as WorkerChildProcessSpawner from "../process/workerChildProcessSpawner.ts";

const extensions = new Set(["docx", "xlsx", "pptx", "odt", "ods", "odp", "rtf", "epub", "ipynb"]);

class DocumentTextError extends Schema.TaggedError<DocumentTextError>()("DocumentTextError", {
  operation: Schema.Literal("extract"),
  path: Schema.String,
  extension: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return `Failed to ${this.operation} ${this.extension} document text.`;
  }
}

// Exit code 2 is the extractor rejecting the document: not retried.
class DocumentTextExit extends Schema.TaggedError<DocumentTextExit>()("DocumentTextExit", {
  exitCode: Schema.NullOr(Schema.Number),
  outputLimit: Schema.Boolean,
}) {}
const isDocumentTextExit = Schema.is(DocumentTextExit);

export const resolveExtractedTextPath = Effect.fn("DocumentText.resolveExtractedTextPath")(
  function* (path: string) {
    const extension = path.split(".").at(-1)?.toLowerCase();
    if (!extension || !extensions.has(extension)) return;
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.exists(path + ".txt").pipe(
      Effect.map((exists) => (exists ? path + ".txt" : undefined)),
      Effect.orElseSucceed(() => undefined),
    );
  },
);

export class DocumentText extends Context.Service<
  DocumentText,
  {
    readonly ensure: (path: string) => Effect.Effect<string | undefined>;
  }
>()("t3/provider/DocumentText") {}

const MAX_TEXT_BYTES = 4 * 1024 * 1024 + 1024;

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* WorkerChildProcessSpawner.commandSpawner;
  const executable = yield* HostProcess.ExecutablePath;
  const isExecutable = yield* HostProcess.IsExecutable;
  const environment = yield* HostProcess.Environment;
  const clock = yield* Clock.Clock;
  const lock = yield* Semaphore.make(1);
  const failed = new Map<string, number>();
  // In a bundle import.meta.url is bin.mjs; development and tests use this source module.
  const entry = NodeURL.fileURLToPath(
    new URL(import.meta.url.endsWith(".ts") ? "../bin.ts" : "./bin.mjs", import.meta.url),
  );
  const extract = Effect.fn("DocumentText.extract")(function* (path: string, extension: string) {
    const sidecar = `${path}.txt`;
    if (yield* fs.exists(sidecar)) return sidecar;
    if ((failed.get(path) ?? 0) > clock.currentTimeMillisUnsafe()) return;
    const info = yield* fs.stat(path);
    if (Number(info.size) > 16 * 1024 * 1024) return;
    const content = yield* Effect.gen(function* () {
      const child = yield* spawner.spawn(
        ChildProcess.make(
          executable,
          [...(isExecutable ? [] : [entry]), "__document-text", path, `.${extension}`],
          {
            env: {
              ...environment,
              ELECTRON_RUN_AS_NODE: "1",
              NODE_OPTIONS: `${environment.NODE_OPTIONS ?? ""} --max-old-space-size=192`,
            },
            extendEnv: false,
            windowsHide: true,
            // A stuck extractor that ignores SIGTERM is killed after its timeout too.
            forceKillAfter: "1 second",
          },
        ),
      );
      const chunks: Array<Uint8Array> = [];
      // Each stream is capped, as execFile's maxBuffer did; stderr is read and dropped.
      const capped = (stream: typeof child.stdout, keep: boolean) => {
        let size = 0;
        return Stream.runForEach(stream, (chunk) => {
          size += chunk.byteLength;
          if (keep) chunks.push(chunk);
          return size > MAX_TEXT_BYTES
            ? Effect.fail(new DocumentTextExit({ exitCode: null, outputLimit: true }))
            : Effect.void;
        });
      };
      const [, , exitCode] = yield* Effect.all(
        [capped(child.stdout, true), capped(child.stderr, false), child.exitCode],
        { concurrency: "unbounded" },
      );
      if (exitCode !== 0) {
        return yield* new DocumentTextExit({ exitCode, outputLimit: false });
      }
      return Buffer.concat(chunks).toString("utf8");
    }).pipe(
      Effect.scoped,
      Effect.timeout("15 seconds"),
      Effect.mapError(
        (cause) => new DocumentTextError({ operation: "extract", path, extension, cause }),
      ),
      Effect.tapError(({ cause }) => {
        if (failed.size >= 1024) failed.delete(failed.keys().next().value!);
        failed.set(
          path,
          isDocumentTextExit(cause) && cause.exitCode === 2
            ? Infinity
            : clock.currentTimeMillisUnsafe() + 1_000,
        );
        return Effect.logWarning("Document text extraction failed.", { extension });
      }),
    );
    const pending = `${sidecar}.part`;
    yield* fs
      .writeFileString(pending, content)
      .pipe(
        Effect.andThen(fs.rename(pending, sidecar)),
        Effect.ensuring(fs.remove(pending, { force: true }).pipe(Effect.ignore)),
        Effect.uninterruptible,
      );
    return sidecar;
  }, lock.withPermits(1));
  const ensure = Effect.fn("DocumentText.ensure")(
    function* (path: string) {
      const extension = path.split(".").at(-1)?.toLowerCase();
      if (!extension || !extensions.has(extension)) return;
      const sidecar = `${path}.txt`;
      if (yield* fs.exists(sidecar)) return sidecar;
      return yield* extract(path, extension);
    },
    Effect.orElseSucceed(() => undefined),
  );
  return DocumentText.of({ ensure });
});

export const layer = Layer.effect(DocumentText, make);

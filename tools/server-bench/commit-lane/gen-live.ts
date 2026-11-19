// Commit-lane bench: one synth shard written through the server's own EventSink (the migration bench's gen.ts),
// except that some threads keep their latest runs back as a "live tail": the bench later streams those events
// through the commit lane, so live writes land on threads (and B-trees) that already hold a history.
// usage: node gen-live.ts --shape <shape.json> --scale <f> --seed <n> --now <iso> --live <threads>
//          --tail-events <per thread> --tails <out.ndjson> [--plan-only] <dest>
// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off globalTimers:off globalJsonInEffect:off globalConsoleInEffect:off preferSchemaOverJson:off - Standalone bench tooling outside the Effect runtime; it owns its clock, timers and output.
import * as NodeFS from "node:fs";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ApplicationProjectCreatedPayload,
  EventId,
  OrchestrationV2DomainEventJson,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { ClaudeProviderCapabilitiesV2 } from "../../../apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as EventSink from "../../../apps/server/src/orchestration-v2/EventSink.ts";
import * as EventStore from "../../../apps/server/src/orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../../../apps/server/src/orchestration-v2/ProjectStore.ts";
import * as Sqlite from "../../../apps/server/src/persistence/Sqlite.ts";
import { Builder } from "../fixture/entities.ts";
import { planSynth, type ThreadPlan } from "../fixture/plan.ts";
import { parseShape } from "../fixture/shape.ts";

const decodeEvents = Schema.decodeUnknownSync(Schema.Array(OrchestrationV2DomainEventJson));
const decodeProjectPayload = Schema.decodeUnknownSync(ApplicationProjectCreatedPayload);
const BATCH_EVENTS = 2_000;

const args = process.argv.slice(2);
const take = (name: string, fallback?: string) => {
  const at = args.indexOf(name);
  if (at < 0 || at + 1 >= args.length) {
    if (fallback !== undefined) return fallback;
    throw new Error(`missing ${name}`);
  }
  const value = args[at + 1] as string;
  args.splice(at, 2);
  return value;
};
const flag = (name: string) => {
  const at = args.indexOf(name);
  if (at < 0) return false;
  args.splice(at, 1);
  return true;
};
const shapePath = take("--shape");
const scale = Number(take("--scale"));
const seed = Number(take("--seed"));
const now = Date.parse(take("--now"));
const liveCount = Number(take("--live", "0"));
const tailEvents = Number(take("--tail-events", "300"));
const tailsPath = take("--tails", "");
const planOnly = flag("--plan-only");
const dest = args[0];
if (!planOnly && (!dest || NodeFS.existsSync(dest)))
  throw new Error(`destination missing or exists: ${dest}`);

const shape = parseShape(JSON.parse(NodeFS.readFileSync(shapePath, "utf8")));
const plan = planSynth(shape, scale, seed);
const itemsOf = (thread: ThreadPlan) => thread.runs.reduce((acc, run) => acc + run.items.length, 0);
console.log(
  `plan seed=${seed} scale=${scale}: ${plan.threads.length} threads, ${plan.totals.turnItems} turn items, ${plan.expected.orchestration_events} events, expected ${JSON.stringify(plan.expected)}`,
);
const sizes = plan.threads.map(itemsOf).toSorted((a, b) => b - a);
console.log(
  `items per thread: max ${sizes[0]}, top5 ${sizes.slice(0, 5).join(",")}, p50 ${sizes[Math.floor(sizes.length / 2)]}`,
);

// Live threads: the largest root threads with runs (huge threads are where production writes land), then a
// seeded spread of the rest, so the bench writes to histories of every size.
let state = seed >>> 0 || 1;
const rand = () => {
  state = (state + 0x6d2b79f5) >>> 0;
  let t = state;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};
const eligible = plan.threads.filter(
  (thread) =>
    !thread.runless && !thread.imported && thread.fork === null && thread.runs.length >= 3,
);
const bySize = eligible.toSorted((a, b) => itemsOf(b) - itemsOf(a));
const live = new Set<number>();
for (const thread of bySize.slice(0, Math.ceil(liveCount / 4))) live.add(thread.index);
const rest = bySize.slice(Math.ceil(liveCount / 4));
while (live.size < Math.min(liveCount, eligible.length) && rest.length > 0) {
  const at = Math.floor(rand() * rest.length);
  live.add((rest.splice(at, 1)[0] as ThreadPlan).index);
}
console.log(
  `live threads: ${live.size} (items ${[...live].map((i) => itemsOf(plan.threads[i] as ThreadPlan)).join(",")})`,
);
if (planOnly) process.exit(0);

const builder = new Builder(plan, {
  providerInstanceId: "claudeAgent",
  driver: "claudeAgent",
  model: "claude-sonnet-4-6",
  workspaceRoot: "/t3-bench-synthetic",
  now,
  capabilities: ClaudeProviderCapabilitiesV2,
});
const started = Date.now();
const stamp = () => `${((Date.now() - started) / 1000).toFixed(0)}s`;
const tailsOut = tailsPath === "" ? undefined : NodeFS.openSync(tailsPath, "wx");

const write = Effect.gen(function* () {
  const eventStore = yield* EventStore.EventStoreV2;
  const projectStore = yield* ProjectStore.ProjectStoreV2;
  const sink = yield* EventSink.EventSinkV2;
  for (const project of builder.projectEvents()) {
    yield* eventStore
      .appendProjectEvent({
        eventId: EventId.make(project.eventId),
        aggregateKind: "project",
        aggregateId: ProjectId.make(project.projectId),
        occurredAt: project.occurredAt,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "project.created",
        payload: decodeProjectPayload(project.payload),
      })
      .pipe(Effect.flatMap((event) => projectStore.apply(event)));
  }
  const order = [
    ...plan.threads.filter((thread) => thread.parent === null && thread.fork === null),
    ...plan.threads.filter((thread) => thread.fork !== null),
    ...plan.threads.filter((thread) => thread.parent !== null),
  ];
  let written = 0;
  let held = 0;
  let nextReport = 0;
  for (const thread of order) {
    const built = builder.threadEvents(thread);
    let head = built.events;
    if (live.has(thread.index) && tailsOut !== undefined) {
      // Cut at the newest run start that leaves at least tailEvents behind it.
      let cut = -1;
      for (let at = head.length - 1; at > 0; at--) {
        const type = (head[at] as { type?: string }).type;
        if (type === "run.created" && head.length - at >= tailEvents) {
          cut = at;
          break;
        }
      }
      if (cut > 0) {
        NodeFS.writeSync(
          tailsOut,
          `${JSON.stringify({ thread: thread.index, items: itemsOf(thread), events: head.slice(cut) })}\n`,
        );
        held += head.length - cut;
        head = head.slice(0, cut);
      }
    }
    const events = decodeEvents(head);
    for (let start = 0; start < events.length; start += BATCH_EVENTS) {
      yield* sink.write({ events: events.slice(start, start + BATCH_EVENTS) });
    }
    written += events.length;
    if (written >= nextReport) {
      console.log(`  ${stamp()} ${written} events written, ${held} held as live tails`);
      nextReport = written + 500_000;
    }
  }
  return { written, held };
});

const result = await Effect.runPromise(
  write.pipe(
    Effect.provide(
      EventSink.layer.pipe(
        Layer.provideMerge(
          Layer.mergeAll(EventStore.layer, ProjectionStore.layer, ProjectStore.layer),
        ),
        Layer.provideMerge(Sqlite.layerFromPath(dest as string)),
        Layer.provide(NodeServices.layer),
      ),
    ),
  ),
);
if (tailsOut !== undefined) NodeFS.closeSync(tailsOut);
console.log(`GEN-OK seed=${seed} events=${result.written} tails=${result.held} ${stamp()}`);

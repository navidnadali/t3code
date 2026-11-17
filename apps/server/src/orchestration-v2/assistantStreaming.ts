import type { ResponseStreamingMode } from "@t3tools/contracts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";

// List fences can open at any indentation; closers may be at most three spaces deeper.
// A backtick fence's info string cannot contain a backtick, so "```npm test``` runs" is inline code.
const MARKDOWN_FENCE_PATTERN = /^( *)(`{3,}(?![^`\r\n]*`)|~{3,})/;
// CommonMark blank lines contain only spaces and tabs.
const BLANK_LINE_PATTERN = /^[ \t]*$/;
// Require trailing space so partial list markers cannot match during streaming.
const LIST_ITEM_START_PATTERN = /^[ \t]*(?:[-*+]|\d{1,9}[.)])[ \t]/;
const SECTION_TITLE_PATTERN = /^ {0,3}(?:#{1,6}(?:[ \t]|$)|\*\*(?:[^*]|\*(?!\*))+\*\*:?$)/;
// Unindented ATX headings end preceding paragraphs and lists without blank lines; bold lines do not.
const TOP_LEVEL_HEADING_PATTERN = /^#{1,6}(?:[ \t]|$)/;

// Matches LIST_ITEM_START_PATTERN on the trimmed partial line without copying it.
const PARTIAL_LIST_ITEM_START_PATTERN = /[ \t]*(?:[-*+]|\d{1,9}[.)])[ \t][ \t\r]*[^ \t\r]/y;

interface AssistantTextScan {
  readonly text: string;
  // Start of the partial line; every line before it has been scanned.
  readonly lineStart: number;
  readonly openFence: { readonly marker: string; readonly indent: number } | null;
  readonly boundary: number;
  readonly titleAwaitingContent: boolean;
}

const EMPTY_SCAN: AssistantTextScan = {
  text: "",
  lineStart: 0,
  openFence: null,
  boundary: -1,
  titleAwaitingContent: false,
};

function trimLineEnd(text: string, start: number, end: number): string {
  while (end > start) {
    const char = text.charCodeAt(end - 1);
    if (char !== 32 && char !== 9 && char !== 13) break;
    end--;
  }
  return text.slice(start, end);
}

/** Scans the terminated lines of `text`, resuming after `previous` when `text` extends it. */
function advanceScan(previous: AssistantTextScan, text: string): AssistantTextScan {
  const resumed = text.startsWith(previous.text);
  let { openFence, boundary, lineStart, titleAwaitingContent } = resumed ? previous : EMPTY_SCAN;
  let searchFrom = resumed ? previous.text.length : 0;
  for (;;) {
    const newline = text.indexOf("\n", searchFrom);
    if (newline === -1) break;
    const line = trimLineEnd(text, lineStart, newline);
    if (
      openFence === null &&
      lineStart > 0 &&
      !titleAwaitingContent &&
      LIST_ITEM_START_PATTERN.test(line)
    ) {
      boundary = lineStart;
    }
    const fenceMatch = MARKDOWN_FENCE_PATTERN.exec(line);
    if (fenceMatch) {
      const indent = fenceMatch[1]!.length;
      const marker = fenceMatch[2]!;
      if (openFence === null) {
        openFence = { marker, indent };
        titleAwaitingContent = false;
      } else if (
        marker[0] === openFence.marker[0] &&
        marker.length >= openFence.marker.length &&
        indent <= openFence.indent + 3 &&
        line.length === indent + marker.length
      ) {
        // CommonMark: a closing fence carries no info string.
        openFence = null;
        boundary = newline + 1;
      }
    } else if (openFence === null && BLANK_LINE_PATTERN.test(line) && lineStart > 0) {
      if (!titleAwaitingContent) {
        boundary = newline + 1;
      }
    } else if (openFence === null) {
      if (lineStart > 0 && !titleAwaitingContent && TOP_LEVEL_HEADING_PATTERN.test(line)) {
        boundary = lineStart;
      }
      titleAwaitingContent = SECTION_TITLE_PATTERN.test(line);
    }
    lineStart = newline + 1;
    searchFrom = lineStart;
  }
  return { text, lineStart, openFence, boundary, titleAwaitingContent };
}

// The partial line may still change, so its list item start is checked here, not kept in the scan.
function readyBoundary(scan: AssistantTextScan): number {
  if (scan.openFence === null && scan.lineStart > 0 && !scan.titleAwaitingContent) {
    PARTIAL_LIST_ITEM_START_PATTERN.lastIndex = scan.lineStart;
    if (PARTIAL_LIST_ITEM_START_PATTERN.test(scan.text)) return scan.lineStart;
  }
  return scan.boundary;
}

/** Emit terminated lines only, except partial list-start lookahead for tight lists. Hold section titles until content follows. */
export function splitBufferedAssistantText(text: string): { ready: string; rest: string } {
  const boundary = readyBoundary(advanceScan(EMPTY_SCAN, text));
  if (boundary === -1) {
    return { ready: "", rest: text };
  }
  return { ready: text.slice(0, boundary), rest: text.slice(boundary) };
}

// Long enough to ride out normal gaps between chunks; short enough that text written before a
// long tool call or background wait does not look lost.
const ASSISTANT_IDLE_FLUSH_MS = 800;

type StreamedMessage = {
  readonly id: string;
  readonly text: string;
  readonly streaming: boolean;
};

interface StreamTrack {
  readonly text: string;
  readonly at: number;
  // Latest snapshot whose text has not all been delivered yet.
  readonly held: ProviderAdapter.ProviderAdapterV2Event | null;
  readonly dueAt: number;
  readonly scan: AssistantTextScan;
}

function streamedMessage(event: ProviderAdapter.ProviderAdapterV2Event): StreamedMessage | null {
  if (event.type === "message.updated" && event.message.role === "assistant") return event.message;
  if (
    event.type === "turn_item.updated" &&
    (event.turnItem.type === "assistant_message" || event.turnItem.type === "reasoning")
  )
    return event.turnItem;
  return null;
}

/** The filter's key for a snapshot of streamed text, and whether that snapshot is still streaming. */
function streamedSnapshot(
  event: ProviderAdapter.ProviderAdapterV2Event,
): { readonly key: string; readonly streaming: boolean } | null {
  if (event.type === "node.updated" && event.node.kind === "assistant_message")
    return { key: `${event.type}:${event.node.id}`, streaming: event.node.status === "running" };
  const message = streamedMessage(event);
  return message === null
    ? null
    : { key: `${event.type}:${message.id}`, streaming: message.streaming };
}

/**
 * A run's provider events between the subscription and ingestion. A newer streaming snapshot of
 * assistant text replaces the item's waiting one in place unless an event of another kind arrived
 * between them, so a backlog commits its newest text once and each item keeps its place.
 */
export function makeIngestionMailbox() {
  let entries: Array<{
    readonly event: ProviderAdapter.ProviderAdapterV2Event;
    readonly key: string | null;
  }> = [];
  // Streaming snapshots waiting since the last event of another kind, by key.
  const open = new Map<string, number>();
  const waiting = new Map<string, number>();
  let unsettled = 0;
  return {
    /** Queues `event`; returns the waiting snapshot it replaced in place, which is gone. */
    offer(
      event: ProviderAdapter.ProviderAdapterV2Event,
    ): ProviderAdapter.ProviderAdapterV2Event | undefined {
      const snapshot = streamedSnapshot(event);
      if (snapshot?.streaming === true) {
        const waitingAt = open.get(snapshot.key);
        // Text takes its display position when first ingested, so the newer step keeps the place.
        if (waitingAt !== undefined) {
          const replaced = entries[waitingAt]!.event;
          entries[waitingAt] = { event, key: snapshot.key };
          return replaced;
        }
        open.set(snapshot.key, entries.length);
      } else open.clear();
      if (snapshot !== null) waiting.set(snapshot.key, (waiting.get(snapshot.key) ?? 0) + 1);
      entries.push({ event, key: snapshot?.key ?? null });
      unsettled++;
      return undefined;
    },
    /** Takes everything queued, in order; a later snapshot no longer replaces what was taken. */
    takeAll(): ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event> {
      const taken = entries.map((entry) => entry.event);
      entries = [];
      open.clear();
      return taken;
    },
    /** Marks a taken event as ingested; until then it is backlog, and its text counts as waiting. */
    settle(event: ProviderAdapter.ProviderAdapterV2Event): void {
      unsettled = Math.max(0, unsettled - 1);
      const snapshot = streamedSnapshot(event);
      if (snapshot === null) return;
      const count = (waiting.get(snapshot.key) ?? 0) - 1;
      if (count > 0) waiting.set(snapshot.key, count);
      else waiting.delete(snapshot.key);
    },
    /** Whether a newer snapshot of the text the filter tracks under `key` is not ingested yet. */
    isWaiting: (key: string): boolean => waiting.has(key),
    /** Events offered and not ingested yet; a coalesced snapshot is gone, not pending. */
    pending: (): number => unsettled,
  };
}

function withText(
  event: ProviderAdapter.ProviderAdapterV2Event,
  text: string,
): ProviderAdapter.ProviderAdapterV2Event {
  if (event.type === "message.updated") return { ...event, message: { ...event.message, text } };
  if (
    event.type === "turn_item.updated" &&
    (event.turnItem.type === "assistant_message" || event.turnItem.type === "reasoning")
  )
    return { ...event, turnItem: { ...event.turnItem, text } };
  return event;
}

/**
 * Paragraph mode delivers streamed text at Markdown boundaries, at most every 400ms. A snapshot
 * held back that way is delivered whole once its message has been idle for `idleFlushMs`, so a
 * provider that pauses mid-turn still shows everything it wrote.
 */
export function makeAssistantStreamingFilter(
  mode: ResponseStreamingMode,
  idleFlushMs = ASSISTANT_IDLE_FLUSH_MS,
) {
  const tracks = new Map<string, StreamTrack>();
  // Earliest deadline of any held snapshot, or earlier: a newer snapshot only pushes its own
  // deadline later, so the cache stays O(1) per event and is recomputed when it comes due.
  let nextDueAt: number | null = null;
  const track = (key: string, value: StreamTrack) => {
    tracks.set(key, value);
    if (value.held !== null && (nextDueAt === null || value.dueAt < nextDueAt))
      nextDueAt = value.dueAt;
  };
  return {
    filter(
      event: ProviderAdapter.ProviderAdapterV2Event,
      now: number,
    ): ProviderAdapter.ProviderAdapterV2Event | null {
      if (
        event.type === "node.updated" &&
        event.node.kind === "assistant_message" &&
        event.node.status === "running"
      )
        return null;
      const message = streamedMessage(event);
      if (!message) return event;
      const key = `${event.type}:${message.id}`;
      if (!message.streaming) {
        tracks.delete(key);
        return event;
      }
      if (mode === "turn") return null;
      const previous = tracks.get(key);
      const delivered = previous?.text ?? "";
      const hold = (scan = previous?.scan ?? EMPTY_SCAN) => {
        track(key, {
          text: delivered,
          at: previous?.at ?? Number.NEGATIVE_INFINITY,
          held: message.text !== delivered ? event : null,
          dueAt: now + idleFlushMs,
          scan,
        });
        return null;
      };
      if (previous && now - previous.at < 400) return hold();
      const scan = advanceScan(previous?.scan ?? EMPTY_SCAN, message.text);
      const boundary = readyBoundary(scan);
      const ready = boundary === -1 ? "" : message.text.slice(0, boundary);
      // An idle delivery may already show more than the next boundary; never shrink it. A
      // provider rewrite or truncation is not a prefix and replaces it.
      if (delivered.startsWith(ready)) return hold(scan);
      track(key, {
        text: ready,
        at: now,
        held: ready !== message.text ? event : null,
        dueAt: now + idleFlushMs,
        scan,
      });
      return withText(event, ready);
    },
    nextIdleFlushAt: (): number | null => nextDueAt,
    /** Returns the held snapshots due by `now` and counts them as delivered. */
    takeIdleFlushes(
      now: number,
      superseded: (key: string) => boolean = () => false,
    ): ReadonlyArray<ProviderAdapter.ProviderAdapterV2Event> {
      const due: Array<ProviderAdapter.ProviderAdapterV2Event> = [];
      nextDueAt = null;
      for (const [key, current] of tracks) {
        const event = current.held;
        if (event === null) continue;
        if (current.dueAt > now) {
          if (nextDueAt === null || current.dueAt < nextDueAt) nextDueAt = current.dueAt;
          continue;
        }
        // A newer snapshot waits for ingestion and decides what to show; the held one is stale.
        if (superseded(key)) {
          tracks.set(key, { ...current, held: null });
          continue;
        }
        due.push(event);
        tracks.set(key, {
          text: streamedMessage(event)?.text ?? current.text,
          at: now,
          held: null,
          dueAt: now,
          scan: current.scan,
        });
      }
      return due;
    },
  };
}

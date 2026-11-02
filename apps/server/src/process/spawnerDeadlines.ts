// @effect-diagnostics globalTimers:off -- Deadlines on host timers, kept apart so a test can run them in a quiet process.

/** Schedules the spawner's answer deadlines; tests fire them by hand. */
export interface SpawnerTimers {
  readonly schedule: (kind: "handshake" | "start", run: () => void) => () => void;
}

/**
 * The real deadlines: each expires `millis` after it is scheduled, but only after the I/O already
 * pending has been read, so a reply that arrived during an event-loop stall (a long start-up, for
 * instance) is still in time.
 */
export const answerDeadlines = (millis: number): SpawnerTimers => ({
  schedule: (_kind, run) => {
    let immediate: NodeJS.Immediate | undefined;
    const timer = setTimeout(() => {
      immediate = setImmediate(run);
      immediate.unref();
    }, millis);
    timer.unref();
    return () => {
      clearTimeout(timer);
      if (immediate !== undefined) clearImmediate(immediate);
    };
  },
});

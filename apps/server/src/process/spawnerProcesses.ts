const live = new Set<number>();

/**
 * PIDs of this server's spawner processes while they run. A command a spawner started is the
 * server's own in all but its parent: process attribution looks through them.
 */
export const spawnerProcessIds: ReadonlySet<number> = live;

/** Records a spawner process the server started; the returned function forgets it. */
export const registerSpawnerProcess = (pid: number) => {
  live.add(pid);
  return () => {
    live.delete(pid);
  };
};

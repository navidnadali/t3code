const live = new Set<number>();

/**
 * PIDs of this server's spawner processes while they run. A command a spawner started is the
 * server's own in all but its parent: process attribution looks through them.
 */
export const spawnerProcessIds: ReadonlySet<number> = live;

const adopted = new Set<number>();

/**
 * PIDs of handed-off providers that outlived their spawner process (re-parented elsewhere) while
 * they run: they stay the server's own children for process attribution and the port tools.
 */
export const adoptedProcessIds: ReadonlySet<number> = adopted;

/** Records a provider that outlived its spawner; the returned function forgets it. */
export const registerAdoptedProcess = (pid: number) => {
  adopted.add(pid);
  return () => {
    adopted.delete(pid);
  };
};

/** Records a spawner process the server started; the returned function forgets it. */
export const registerSpawnerProcess = (pid: number) => {
  live.add(pid);
  return () => {
    live.delete(pid);
  };
};

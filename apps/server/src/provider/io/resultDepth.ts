// Structured clone reads nesting recursively: main fails past about 2,400 nested objects and a
// worker cannot send past about 10,000, so a result nested deeper than this stays on main.
export const MAX_OFFLOADED_RESULT_DEPTH = 1000;

/** Whether `value` nests objects or arrays more than `limit` deep; walks iteratively. */
export function nestsDeeperThan(value: unknown, limit: number): boolean {
  const values: Array<unknown> = [value];
  const depths: Array<number> = [1];
  while (values.length > 0) {
    const current = values.pop();
    const depth = depths.pop()!;
    if (typeof current !== "object" || current === null) continue;
    if (depth > limit) return true;
    if (Array.isArray(current)) {
      for (const item of current) {
        if (typeof item === "object" && item !== null) {
          values.push(item);
          depths.push(depth + 1);
        }
      }
      continue;
    }
    for (const key in current) {
      const item: unknown = (current as Record<string, unknown>)[key];
      if (typeof item === "object" && item !== null) {
        values.push(item);
        depths.push(depth + 1);
      }
    }
  }
  return false;
}

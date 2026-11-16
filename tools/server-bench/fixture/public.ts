// @effect-diagnostics nodeBuiltinImport:off - checks a shape file on disk before the public build runs.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { type Shape, ShapeError, parseShape } from "./shape.ts";

/** The only shape the public recipe builds from, as published: `fixture/public-large.shape.json`. */
export const PUBLIC_SHAPE_DIRECTORY = "fixture";
export const PUBLIC_SHAPE_FILE = "public-large.shape.json";
export const PUBLIC_SHAPE_LABEL =
  "t3cp.io public large fixture: hand-written round numbers, not measured from any deployment";
export const PUBLIC_SHAPE_CAPTURED_AT = "2026-10-17T00:00:00.000Z";
export const PUBLIC_PAGE_SIZE = 4096;

/** The public recipe's fixed flags: the same shape and these give the same bytes on any machine. */
export const PUBLIC_RECIPE = {
  scale: 1,
  seed: 20261017,
  now: "2026-10-17T00:00:00Z",
  workspaceRoot: "/t3-bench-synthetic",
} as const;

/**
 * The SQLite the published fixtures were written with (bundled by Node 26.12.1). The file header records
 * the writing library's version, so another SQLite writes other bytes.
 */
export const PUBLIC_SQLITE_VERSION = "3.54.0";
export const PUBLIC_NODE_VERSION = "26.12.1";

/** The public recipe refuses to run: exit 2, nothing written. */
export class PublicRecipeError extends Error {}
export class PublicShapeError extends PublicRecipeError {}

/** Refuses any SQLite but the one the published fixtures were written with. */
export const checkSqliteVersion = (version: string | undefined): void => {
  if (version !== PUBLIC_SQLITE_VERSION) {
    throw new PublicRecipeError(
      `the public recipe writes with SQLite ${PUBLIC_SQLITE_VERSION} (Node ${PUBLIC_NODE_VERSION}); this Node has SQLite ${version ?? "unknown"}, which writes other bytes`,
    );
  }
};

/** A non-negative integer with at most two significant digits (0, 50, 8,500, 1,700,000). */
export const isRound = (value: unknown): boolean => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) return false;
  let mantissa = value;
  while (mantissa >= 100 && mantissa % 10 === 0) mantissa /= 10;
  return mantissa < 100;
};

/** A quantile position on a thousandths grid (0.5, 0.99, 0.997, 1). */
const isRoundQuantile = (q: number) => Math.abs(q * 1000 - Math.round(q * 1000)) < 1e-9;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * Refuses everything but the hand-written public shape: the file must be (after symlinks)
 * `fixture/public-large.shape.json`, carry the public label and date, and hold only round numbers
 * and none of the fields a measured shape carries (`bytes`, `stat1`). Throws PublicShapeError
 * listing every problem; returns the parsed shape.
 */
export const checkPublicShape = (path: string, text: string): Shape => {
  let real: string;
  try {
    real = NodeFS.realpathSync(path);
  } catch {
    real = NodePath.resolve(path);
  }
  const expected = `${PUBLIC_SHAPE_DIRECTORY}/${PUBLIC_SHAPE_FILE}`;
  if (
    NodePath.basename(real) !== PUBLIC_SHAPE_FILE ||
    NodePath.basename(NodePath.dirname(real)) !== PUBLIC_SHAPE_DIRECTORY
  ) {
    throw new PublicShapeError(`the public recipe builds only ${expected}; refusing ${real}`);
  }
  let shape: Shape;
  try {
    shape = parseShape(JSON.parse(text) as unknown);
  } catch (error) {
    if (error instanceof ShapeError || error instanceof SyntaxError) {
      throw new PublicShapeError(`${expected}: ${error.message}`);
    }
    throw error;
  }
  const problems: Array<string> = [];
  const round = (where: string, value: unknown) => {
    if (!isRound(value)) problems.push(`${where}: ${String(value)} is not a round number`);
  };
  const quantiles = (where: string, pairs: ReadonlyArray<readonly [number, number]>) =>
    pairs.forEach(([q, value], index) => {
      if (!isRoundQuantile(q)) problems.push(`${where}[${index}]: q ${q} is not on a 0.001 grid`);
      round(`${where}[${index}]`, value);
    });

  const source = (shape as { source?: unknown }).source;
  const label = isRecord(source) ? source.label : undefined;
  if (label !== PUBLIC_SHAPE_LABEL) {
    problems.push(`source.label: ${JSON.stringify(label)} is not the public label`);
  }
  if (!isRecord(source) || source.capturedAt !== PUBLIC_SHAPE_CAPTURED_AT) {
    problems.push(`source.capturedAt: expected ${PUBLIC_SHAPE_CAPTURED_AT}`);
  }
  if (isRecord(source)) {
    for (const key of Object.keys(source)) {
      if (key !== "label" && key !== "capturedAt") problems.push(`source.${key}: not allowed`);
    }
  }
  if (shape.pageSize !== PUBLIC_PAGE_SIZE) {
    problems.push(`pageSize: ${shape.pageSize} (expected ${PUBLIC_PAGE_SIZE})`);
  }
  round("pageCount", shape.pageCount);
  if (shape.stat1 !== undefined && shape.stat1.length > 0) {
    problems.push("stat1: a measured shape's statistics are not allowed");
  }
  for (const [name, table] of Object.entries(shape.tables)) {
    round(`tables.${name}.rows`, table.rows);
    if (table.bytes !== undefined) {
      problems.push(`tables.${name}.bytes: a measured size is not allowed`);
    }
    if (table.payload !== undefined) {
      round(`tables.${name}.payload.mean`, table.payload.mean);
      quantiles(`tables.${name}.payload.quantiles`, table.payload.quantiles);
    }
  }
  for (const [name, pairs] of Object.entries(shape.distributions ?? {})) {
    if (pairs !== undefined) quantiles(`distributions.${name}`, pairs);
  }
  for (const [name, value] of Object.entries(shape.lineage ?? {})) {
    round(`lineage.${name}`, value);
  }
  for (const [mixName, mix] of Object.entries(shape.mix ?? {})) {
    for (const [key, entry] of Object.entries(mix ?? {})) {
      round(`mix.${mixName}.${key}.rows`, entry.rows);
      round(`mix.${mixName}.${key}.meanBytes`, entry.meanBytes);
    }
  }
  if (problems.length > 0) {
    throw new PublicShapeError(
      `${expected} is not the public shape:\n${problems.map((line) => `  ${line}`).join("\n")}`,
    );
  }
  return shape;
};

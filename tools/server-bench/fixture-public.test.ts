// @effect-diagnostics nodeBuiltinImport:off - drives the public fixture build CLI as a subprocess.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { afterAll, describe, expect, it } from "vite-plus/test";

import {
  PUBLIC_SHAPE_LABEL,
  PUBLIC_SQLITE_VERSION,
  PublicRecipeError,
  PublicShapeError,
  checkPublicShape,
  checkSqliteVersion,
  isRound,
} from "./fixture/public.ts";

const CLI = NodePath.join(import.meta.dirname, "fixture-public.ts");
const ROOT = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "fixture-public-"));
afterAll(() => NodeFS.rmSync(ROOT, { recursive: true, force: true }));

let counter = 0;

/** A tiny shape in the public recipe's form (round numbers, the public label), so a wrong accept builds fast. */
const publicShape = (): Record<string, any> => ({
  format: "t3-server-bench-shape",
  version: 1,
  source: { label: PUBLIC_SHAPE_LABEL, capturedAt: "2026-10-17T00:00:00.000Z" },
  pageSize: 4096,
  pageCount: 100,
  tables: {
    projection_projects: { rows: 2 },
    orchestration_v2_projection_threads: { rows: 10 },
    orchestration_v2_projection_turn_items: {
      rows: 100,
      payload: {
        column: "payload_json",
        mean: 3_000,
        quantiles: [
          [0, 200],
          [0.5, 800],
          [1, 4_000],
        ],
      },
    },
  },
  distributions: {
    turnItemsPerThread: [
      [0, 1],
      [0.5, 10],
      [1, 400],
    ],
  },
  lineage: { root: 8, fork: 1, subagent: 1, v1Import: 1, runless: 1 },
  mix: {
    turnItemTypes: { assistant_message: { rows: 100, meanBytes: 2_000 } },
    eventTypes: { "turn-item.updated": { rows: 150, meanBytes: 2_500 } },
  },
});

/** Writes `shape` at `<fresh dir>/<relative>` and returns the path. */
const writeShape = (relative: string, shape: unknown): string => {
  const path = NodePath.join(ROOT, `case-${counter++}`, relative);
  NodeFS.mkdirSync(NodePath.dirname(path), { recursive: true });
  NodeFS.writeFileSync(path, `${JSON.stringify(shape, null, 2)}\n`);
  return path;
};

const PUBLIC_PATH = NodePath.join("fixture", "public-large.shape.json");

const refusal = (path: string): string => {
  try {
    checkPublicShape(path, NodeFS.readFileSync(path, "utf8"));
  } catch (error) {
    expect(error).toBeInstanceOf(PublicShapeError);
    return (error as Error).message;
  }
  throw new Error(`${path} was accepted`);
};

describe("isRound", () => {
  it("accepts integers with at most two significant digits", () => {
    expect([0, 1, 50, 900, 8_500, 15_000, 1_700_000, 100_000].map(isRound)).toEqual(
      Array.from({ length: 8 }, () => true),
    );
  });

  it("refuses measured-looking, fractional and negative numbers", () => {
    expect([4_096, 103_106, 2_281.3, 1.5, -100, Number.NaN].map(isRound)).toEqual(
      Array.from({ length: 6 }, () => false),
    );
  });
});

describe("checkPublicShape", () => {
  it("accepts the public shape at fixture/public-large.shape.json", () => {
    const path = writeShape(PUBLIC_PATH, publicShape());
    expect(checkPublicShape(path, NodeFS.readFileSync(path, "utf8")).pageCount).toBe(100);
  });

  it("refuses any other shape file name", () => {
    const path = writeShape(NodePath.join("fixture", "example-shape.json"), publicShape());
    expect(refusal(path)).toMatch(/fixture\/public-large\.shape\.json/);
  });

  it("refuses the right file name outside a fixture/ directory", () => {
    const path = writeShape(NodePath.join("shapes", "public-large.shape.json"), publicShape());
    expect(refusal(path)).toMatch(/fixture\/public-large\.shape\.json/);
  });

  it("refuses a symlink named like the public shape that points at another file", () => {
    const target = writeShape("other.shape.json", publicShape());
    const link = NodePath.join(NodePath.dirname(target), PUBLIC_PATH);
    NodeFS.mkdirSync(NodePath.dirname(link), { recursive: true });
    NodeFS.symlinkSync(target, link);
    expect(refusal(link)).toMatch(/fixture\/public-large\.shape\.json/);
  });

  it("refuses a label other than the public one", () => {
    const shape = publicShape();
    shape.source.label = "Measured from a workstation's database";
    expect(refusal(writeShape(PUBLIC_PATH, shape))).toMatch(/source\.label/);
  });

  it("refuses a shape without a source label", () => {
    const shape = publicShape();
    delete shape.source;
    expect(refusal(writeShape(PUBLIC_PATH, shape))).toMatch(/source\.label/);
  });

  it("refuses a non-round row count and names the field", () => {
    const shape = publicShape();
    shape.tables.orchestration_v2_projection_threads.rows = 103;
    expect(refusal(writeShape(PUBLIC_PATH, shape))).toMatch(
      /tables\.orchestration_v2_projection_threads\.rows: 103/,
    );
  });

  it("refuses a non-round page count, payload size, quantile, lineage or mix value", () => {
    const shape = publicShape();
    shape.pageCount = 6_412_345;
    shape.tables.orchestration_v2_projection_turn_items.payload.mean = 2_281.3;
    shape.distributions.turnItemsPerThread[1] = [0.5, 105];
    shape.lineage.fork = 312;
    shape.mix.eventTypes["turn-item.updated"].meanBytes = 2_095;
    const message = refusal(writeShape(PUBLIC_PATH, shape));
    expect(message).toMatch(/pageCount: 6412345/);
    expect(message).toMatch(/payload\.mean: 2281\.3/);
    expect(message).toMatch(/turnItemsPerThread\[1\]: 105/);
    expect(message).toMatch(/lineage\.fork: 312/);
    expect(message).toMatch(/eventTypes\.turn-item\.updated\.meanBytes: 2095/);
  });

  it("refuses the fields only a measured shape carries", () => {
    const shape = publicShape();
    shape.tables.projection_projects.bytes = 16_000;
    shape.stat1 = [{ tbl: "projection_projects", idx: null, stat: "2" }];
    const message = refusal(writeShape(PUBLIC_PATH, shape));
    expect(message).toMatch(/tables\.projection_projects\.bytes/);
    expect(message).toMatch(/stat1/);
  });
});

describe("the SQLite the recipe writes with", () => {
  it("is pinned to 3.54.0, the version the published fixtures were written with", () => {
    expect(PUBLIC_SQLITE_VERSION).toBe("3.54.0");
    expect(() => checkSqliteVersion("3.54.0")).not.toThrow();
  });

  it("refuses any other SQLite, whose file header bytes differ", () => {
    expect(() => checkSqliteVersion("3.54.1")).toThrow(PublicRecipeError);
    expect(() => checkSqliteVersion(undefined)).toThrow(/SQLite 3\.54\.0/);
  });
});

describe("fixture-public.ts build", () => {
  const build = (shape: string, out: string) =>
    NodeChildProcess.spawnSync(process.execPath, [CLI, "build", "--shape", shape, "--out", out], {
      encoding: "utf8",
    });

  it("exits 2 on another shape file before it writes anything", () => {
    const shape = writeShape(NodePath.join("fixture", "example-shape.json"), publicShape());
    const out = NodePath.join(NodePath.dirname(shape), "large.sqlite");
    const result = build(shape, out);
    expect([result.status, NodeFS.existsSync(out)]).toEqual([2, false]);
    expect(result.stderr).toMatch(/fixture\/public-large\.shape\.json/);
  });

  it("exits 2 under another SQLite before it writes anything", () => {
    const shape = writeShape(PUBLIC_PATH, publicShape());
    const out = NodePath.join(NodePath.dirname(shape), "large.sqlite");
    const otherSqlite = `Object.defineProperty(process.versions, "sqlite", { value: "3.54.1" });`;
    const result = NodeChildProcess.spawnSync(
      process.execPath,
      [
        "--import",
        `data:text/javascript,${encodeURIComponent(otherSqlite)}`,
        CLI,
        "build",
        "--shape",
        shape,
        "--out",
        out,
      ],
      { encoding: "utf8" },
    );
    expect([result.status, NodeFS.existsSync(out)]).toEqual([2, false]);
    expect(result.stderr).toMatch(/SQLite 3\.54\.1/);
  });

  it("exits 2 on a non-round shape before it writes anything", () => {
    const shapeJson = publicShape();
    shapeJson.tables.projection_projects.rows = 103;
    const shape = writeShape(PUBLIC_PATH, shapeJson);
    const out = NodePath.join(NodePath.dirname(shape), "large.sqlite");
    const result = build(shape, out);
    expect([result.status, NodeFS.existsSync(out)]).toEqual([2, false]);
    expect(result.stderr).toMatch(/tables\.projection_projects\.rows: 103/);
  });
});

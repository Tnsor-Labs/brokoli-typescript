import { describe, expect, test } from "bun:test";
import { PipelineError } from "../src/errors";
import { Pipeline } from "../src/pipeline";

describe("task(fn)/map(fn) default naming (ADR-034 item 4)", () => {
  test("task(fn) derives the node name from a named function declaration", () => {
    const p = new Pipeline("P");
    const src = p.sourceFile("Src");
    function normalizeOrders(rows: unknown[]) {
      return { columns: [], rows: [] as never[] };
    }
    const out = p.task(normalizeOrders, src);
    expect(p.toJSON().nodes.find((n) => n.id === out.nodeId)?.name).toBe("Normalize Orders");
  });

  test("task(fn) derives the node name from a named function expression", () => {
    const p = new Pipeline("P");
    const src = p.sourceFile("Src");
    const out = p.task(function scoreRows(rows: unknown[]) {
      return { columns: [], rows: [] as never[] };
    }, src);
    expect(p.toJSON().nodes.find((n) => n.id === out.nodeId)?.name).toBe("Score Rows");
  });

  test("an explicit name argument is unaffected and still wins", () => {
    const p = new Pipeline("P");
    const src = p.sourceFile("Src");
    function normalizeOrders(rows: unknown[]) {
      return { columns: [], rows: [] as never[] };
    }
    const out = p.task("Explicit Name", src, normalizeOrders);
    expect(p.toJSON().nodes.find((n) => n.id === out.nodeId)?.name).toBe("Explicit Name");
  });

  test("task(fn) rejects an arrow function — fn.name is not a reliable declared name", () => {
    const p = new Pipeline("P");
    const src = p.sourceFile("Src");
    const arrow = (rows: unknown[]) => ({ columns: [], rows: [] as never[] });
    expect(() => p.task(arrow, src)).toThrow(PipelineError);
  });

  test("task(fn) rejects an anonymous function expression", () => {
    const p = new Pipeline("P");
    const src = p.sourceFile("Src");
    expect(() =>
      p.task(function (rows: unknown[]) {
        return { columns: [], rows: [] as never[] };
      }, src),
    ).toThrow(PipelineError);
  });

  test("map(fn) derives the node name the same way", () => {
    const p = new Pipeline("P");
    const src = p.sourceFile("Src");
    function addRegionCode(row: Record<string, unknown>) {
      return row;
    }
    const out = p.map(addRegionCode, src);
    expect(p.toJSON().nodes.find((n) => n.id === out.nodeId)?.name).toBe("Add Region Code");
  });
});

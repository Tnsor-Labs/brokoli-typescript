import { describe, expect, test } from "bun:test";
import { PipelineError } from "../src/errors";
import { helpersPreamble, taskScript } from "../src/code";
import { Pipeline } from "../src/pipeline";

describe("helpersPreamble()", () => {
  test("no helpers -> empty preamble, script unchanged", () => {
    expect(helpersPreamble(undefined)).toBe("");
    expect(helpersPreamble({})).toBe("");
  });

  test("a function helper serializes via functionSource, assigned to a const", () => {
    function grossMargin(revenue: number, cost: number) {
      return revenue - cost;
    }
    const pre = helpersPreamble({ grossMargin });
    expect(pre).toContain("const grossMargin = (function grossMargin(revenue, cost)");
  });

  test("a JSON-serializable constant helper serializes as a literal", () => {
    const pre = helpersPreamble({ TAX_RATE: 0.21, LABEL: "q3" });
    expect(pre).toContain("const TAX_RATE = 0.21;");
    expect(pre).toContain('const LABEL = "q3";');
  });

  test("helper->helper references resolve: declaration order doesn't matter for deferred calls", () => {
    function seasonal(month: string) {
      return month === "12" ? 1.45 : 1;
    }
    function grossMargin(revenue: number, cost: number, month: string) {
      return (revenue - cost) * seasonal(month);
    }
    // seasonal declared after grossMargin in the object -- still resolves,
    // because neither helper calls the other until invoked, by which
    // point both consts are assigned.
    const pre = helpersPreamble({ grossMargin, seasonal });
    expect(pre.indexOf("grossMargin")).toBeLessThan(pre.indexOf("const seasonal"));
    // eslint-disable-next-line no-new-func
    const scope = new Function(`${pre}return grossMargin(100, 20, "12");`);
    expect(scope()).toBeCloseTo(80 * 1.45);
  });

  test("rejects a helper name colliding with the fixed wrapper namespace", () => {
    expect(() => helpersPreamble({ rows: [1, 2, 3] })).toThrow(PipelineError);
    expect(() => helpersPreamble({ emit: () => {} })).toThrow(PipelineError);
  });

  test("rejects an invalid identifier as a helper key", () => {
    expect(() => helpersPreamble({ "not-an-identifier": 1 })).toThrow(PipelineError);
  });

  test("rejects a helper that is neither a function nor JSON-serializable", () => {
    expect(() => helpersPreamble({ bad: undefined })).toThrow(PipelineError);
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    expect(() => helpersPreamble({ bad: circular })).toThrow(PipelineError);
  });

  test("rejects a helper function functionSource() already rejects (native function)", () => {
    expect(() => helpersPreamble({ bad: Math.max })).toThrow(PipelineError);
  });
});

describe("helpers wired through task()", () => {
  test("the preamble lands ahead of the task body in the emitted script", () => {
    function grossMargin(revenue: number, cost: number) {
      return revenue - cost;
    }
    const script = taskScript((rows) => ({ columns: [], rows }), { grossMargin, TAX_RATE: 0.21 });
    expect(script.indexOf("grossMargin")).toBeLessThan(script.indexOf("output_data"));
    expect(script).toContain("const TAX_RATE = 0.21;");
  });

  test("Pipeline.task(..., { helpers }) round-trips through toJSON()", () => {
    const p = new Pipeline("P");
    const src = p.sourceFile("Src");
    function grossMargin(revenue: number, cost: number) {
      return revenue - cost;
    }
    const out = p.task("Score", src, (rows) => ({ columns: [], rows }), { helpers: { grossMargin } });
    const node = p.toJSON().nodes.find((n) => n.id === out.nodeId);
    expect(node?.config.script).toContain("const grossMargin =");
  });

  test("a bad helper on Pipeline.task throws before any node is registered", () => {
    const p = new Pipeline("P");
    const src = p.sourceFile("Src");
    expect(() =>
      p.task("Score", src, (rows) => ({ columns: [], rows }), { helpers: { emit: 1 } }),
    ).toThrow(PipelineError);
    expect(p.toJSON().nodes).toHaveLength(1); // only Src -- Score never registered
  });
});

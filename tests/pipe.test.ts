import { describe, expect, test } from "bun:test";
import { Pipeline } from "../src/pipeline";
import { pipe } from "../src/pipe";

describe("pipe()", () => {
  test("emits byte-identical IR to the equivalent hand-written .then() chain", () => {
    const handWritten = new Pipeline("Quarterly Revenue", { pipelineId: "qrv-1" });
    const src = handWritten.sourceDb("Load Orders", { connId: "warehouse", query: "SELECT 1" });
    const scored = handWritten.task("Revenue Score", src, (rows) => ({
      columns: ["region", "margin"],
      rows,
    }));
    handWritten.sinkDb("Save", scored, { connId: "warehouse", table: "fact_revenue" });

    const piped = pipe(
      "Quarterly Revenue",
      { pipelineId: "qrv-1" },
      (p) => p.sourceDb("Load Orders", { connId: "warehouse", query: "SELECT 1" }),
      (p, orders) =>
        p.task("Revenue Score", orders, (rows) => ({
          columns: ["region", "margin"],
          rows,
        })),
      (p, scoredOrders) => p.sinkDb("Save", scoredOrders, { connId: "warehouse", table: "fact_revenue" }),
    );

    expect(piped.toJSON()).toEqual(handWritten.toJSON());
    expect(piped.digest()).toBe(handWritten.digest());
  });

  test("threads each step's output as the next step's input, in argument order", () => {
    const p = pipe(
      "Chain",
      {},
      (pipeline) => pipeline.sourceFile("A"),
      (pipeline, a) => pipeline.transform("B", a),
      (pipeline, b) => pipeline.transform("C", b),
    );
    const ir = p.toJSON();
    expect(ir.nodes.map((n) => n.name)).toEqual(["A", "B", "C"]);
    expect(ir.edges).toEqual([
      { from: ir.nodes[0].id, to: ir.nodes[1].id },
      { from: ir.nodes[1].id, to: ir.nodes[2].id },
    ]);
  });

  test("a single-step pipe is just the source, no dangling edges", () => {
    const p = pipe("Solo", {}, (pipeline) => pipeline.sourceFile("Only"));
    expect(p.toJSON().nodes).toHaveLength(1);
    expect(p.toJSON().edges).toHaveLength(0);
  });
});

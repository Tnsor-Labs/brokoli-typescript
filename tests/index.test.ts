import { describe, expect, test } from "bun:test";
import { Client, Connection, Param, Pipeline, cursorPages, irDigest, renderIR, validatePipeline } from "../src/index";

describe("Brokoli TypeScript compiler", () => {
  test("builds the declarative IR with deterministic IDs", async () => {
    const p = new Pipeline("Orders", { pipelineId: "orders", tags: ["etl"] });
    const source = p.sourceFile("Read orders", { path: "/tmp/orders.csv" });
    const cleaned = p.transform("Clean", source, { rules: [{ type: "rename", mapping: { id: "order_id" } }] });
    p.sinkFile("Export", cleaned, { path: "/tmp/out.json", format: "json" });
    expect(p.toJSON().nodes.map(n => n.id)).toEqual(["read_orders_1", "clean_1", "export_1"]);
    expect(validatePipeline(p).valid).toBe(true);
    expect((await irDigest(p.toJSON())).startsWith("sha256:")).toBe(true);
    expect(renderIR(p.toJSON())).toContain('"pipeline_id": "orders"');
  });
  test("serializes conditional edges as IR 2.1", () => {
    const p = new Pipeline("Branch"); const source = p.sourceFile("Input", { path: "in.csv" });
    const gate = p.conditionNode("Gate", source, "row_count > 0"); const yes = p.sinkFile("Yes", undefined, { path: "yes.csv" }); const no = p.sinkFile("No", undefined, { path: "no.csv" });
    gate.when(yes); gate.otherwise(no); expect(p.toJSON().ir_version).toBe("2.1"); expect(p.toJSON().edges.at(-1)).toEqual({ from: "gate_1", to: "no_1", condition: false });
  });
  test("rejects invalid credentials configuration", () => {
    expect(() => new Client("http://localhost", { apiKey: "key", username: "u", password: "p" })).toThrow();
  });
  test("compiles resources and pagination without leaking objects into IR", () => {
    const p = new Pipeline("API");
    const source = p.sourceApi("Fetch", { url: `https://example.test/${new Param("day")}`, connId: new Connection("warehouse"), pagination: cursorPages("meta.next", "cursor").withExecution({ max_concurrency: 2 }) });
    expect(source.pipeline.toJSON().nodes[0].config).toMatchObject({ conn_id: "warehouse", pagination: { strategy: "cursor", cursor_path: "meta.next" }, execution: { max_concurrency: 2 } });
    expect(source.pipeline.toJSON().nodes[0].config.url).toContain("${param.day}");
  });
});

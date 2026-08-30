import { describe, expect, test } from "bun:test";
import { createOrderIngestionPipeline, createPostgresOrderLoadPipeline } from "../examples/order-ingestion";

describe("production examples", () => {
  test("order ingestion compiles a resilient TypeScript DAG", () => {
    const pipeline = createOrderIngestionPipeline({
      ordersUrl: "https://orders.example.test/v1/orders",
      ordersConnection: "orders-api",
      warehouseConnection: "analytics-warehouse",
    });
    const ir = pipeline.compile();
    const types = ir.nodes.map((node) => node.type);
    const codeNodes = ir.nodes.filter((node) => node.type === "code");

    expect(types).toEqual(expect.arrayContaining(["source_api", "code", "quality_check", "sink_db"]));
    expect(ir.nodes.length).toBeGreaterThanOrEqual(8);
    const validId = ir.nodes.find((node) => node.name === "Keep valid orders")?.id;
    const enrichId = ir.nodes.find((node) => node.name === "Add operational fields")?.id;
    expect(validId).toBeDefined();
    expect(enrichId).toBeDefined();
    expect(ir.edges.some((edge) => edge.from === validId && edge.to === enrichId)).toBe(true);
    expect(codeNodes.every((node) => node.config.language === "typescript")).toBe(true);
    expect(ir.nodes.find((node) => node.type === "source_api")?.config.execution).toEqual({
      checkpoint: true,
      max_retries: 4,
      retry_backoff: "exponential",
    });
    expect(ir.nodes.find((node) => node.name === "Upsert orders")?.config).toMatchObject({
      mode: "upsert",
      key_columns: ["order_id"],
    });
  });

  test("PostgreSQL load pipeline keeps the million-row path in TypeScript", () => {
    const pipeline = createPostgresOrderLoadPipeline({
      sourceUri: "postgres://tsbench:secret@127.0.0.1:55432/orders",
      targetUri: "postgres://tsbench:secret@127.0.0.1:55432/orders",
      pipelineId: "postgres-order-load-test-example",
    });
    const ir = pipeline.compile();
    expect(ir.nodes.filter((node) => node.type === "code").map((node) => node.config.language)).toEqual(["typescript"]);
    expect(ir.nodes.find((node) => node.type === "source_db")?.config.query).toContain("WHERE amount >= 0");
    expect(ir.nodes.find((node) => node.name === "Upsert processed orders")?.config).toMatchObject({
      mode: "upsert",
      key_columns: ["order_id"],
    });
  });
});

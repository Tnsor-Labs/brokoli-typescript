import { expect, test } from "bun:test";
import { createPostgresPushdownPipeline } from "../examples/postgres-pushdown";

test("TypeScript pushdown example is a direct source-to-sink database segment", () => {
  const ir = createPostgresPushdownPipeline({
    databaseUri: "postgres://user:pass@db.example.test/orders",
  }).compile();
  expect(ir.nodes.map((node) => node.type)).toEqual(["source_db", "sink_db"]);
  expect(ir.edges).toHaveLength(1);
  expect(ir.nodes.some((node) => node.type === "code")).toBe(false);
  expect(ir.nodes.find((node) => node.type === "sink_db")?.config.mode).toBe("append");
});

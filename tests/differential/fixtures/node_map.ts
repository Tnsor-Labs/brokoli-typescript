import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const pipeline = new Pipeline("Node map", { pipelineId: "node-map" });
  const customers = pipeline.sourceDb("Customers", { query: "SELECT id, region FROM customers", connId: "shop" });
  const regions = pipeline.sourceDb("Regions", { query: "SELECT region FROM regions", connId: "shop" });
  pipeline.sourceDb("Orders", {
    query: "SELECT * FROM orders WHERE customer_id = ${item.id}",
    connId: "shop",
    map: { over: customers, key: "id", maxItems: 500, maxParallel: 8, onItemFailure: "continue" },
  });
  pipeline.sourceApi("Profile", { url: "https://api.example.com/customers/${item.id}", map: { over: customers } });
  pipeline.code("Pair", customers, {
    script: "output_data = {'columns': columns, 'rows': rows}",
    map: { over: { customer: customers, region: regions }, onItemFailure: "skip" },
  });
  return pipeline;
}

import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const p = new Pipeline("Warehouse Load", { pipelineId: "warehouse-load" });
  const orders = p.sourceDb("Orders", { connId: "prod_pg", query: "SELECT * FROM orders" });
  const users = p.sourceDb("Users", { connId: "prod_pg", query: "SELECT id, email FROM users" });
  const joined = p.join("Enrich", orders, users, { on: "user_id" });
  const gated = joined.then(
    p.qualityCheck("Gate", undefined, {
      rules: [{ rule: "not_null", column: "email", on_failure: "block" }],
    }),
  );
  gated.then(
    p.sinkDb("Load", undefined, {
      connId: "warehouse",
      table: "orders_enriched",
      mode: "upsert",
      keyColumns: ["id"],
    }),
  );
  return p;
}

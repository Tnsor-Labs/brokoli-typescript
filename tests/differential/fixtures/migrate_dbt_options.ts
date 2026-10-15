import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const pipeline = new Pipeline("Migrate and dbt", { pipelineId: "migrate-dbt" });
  const copied = pipeline.migrate("Copy events", {
    sourceConnId: "oltp",
    targetConnId: "dwh",
    query: "SELECT * FROM events",
    table: "events",
    mode: "upsert",
    keyColumns: ["id", "updated_at"],
    dialect: "postgres",
    chunkSize: 1000,
    createTable: false,
    retries: 2,
    retryDelay: 5000,
    timeout: 900,
    pool: "dwh",
    partition: {
      column: "updated_at",
      strategy: "date",
      boundaries: [new Date("2026-01-01T00:00:00Z"), "2026-02-01T00:00:00Z", "2026-03-01T00:00:00Z"],
      maxParallel: 2,
    },
  });
  pipeline.dbt("Models", {
    command: "build",
    select: "marts",
    vars: { run_date: "2026-10-14", full_refresh: true, nested: { b: 2, a: [1, "x"] } },
    connId: "dwh",
    targetSchema: "analytics",
    outputModel: "orders",
    input: copied,
    retries: 1,
    pool: "dwh",
  });
  return pipeline;
}

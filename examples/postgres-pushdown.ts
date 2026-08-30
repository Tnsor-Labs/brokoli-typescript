import { Pipeline } from "../src/index";

/**
 * Direct PostgreSQL table transfer. Because source and sink use the same
 * database address and have no engine transform between them, core compiles
 * this to one INSERT ... SELECT statement; no rows cross the worker.
 */
export function createPostgresPushdownPipeline(options: {
  databaseUri: string;
  sourceTable?: string;
  targetTable?: string;
  pipelineId?: string;
}): Pipeline {
  const pipeline = new Pipeline("PostgreSQL pushdown transfer", {
    pipelineId: options.pipelineId ?? "postgres-pushdown-transfer",
    description: "Move a PostgreSQL table with database-side INSERT SELECT pushdown.",
    tags: ["production", "postgres", "pushdown"],
  });
  const source = pipeline.sourceDb("Read source table", {
    uri: options.databaseUri,
    query: `SELECT order_id, customer_id, currency, amount, ordered_at, status FROM ${options.sourceTable ?? "orders"}`,
  });
  pipeline.sinkDb("Append target table", source, {
    uri: options.databaseUri,
    table: options.targetTable ?? "orders_pushdown_typescript",
    mode: "append",
  });
  return pipeline;
}

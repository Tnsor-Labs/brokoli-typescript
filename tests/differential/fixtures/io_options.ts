import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const pipeline = new Pipeline("IO options", { pipelineId: "io-options" });
  const file = pipeline.sourceFile("Read file", { path: "/data/in.csv", retries: 3, timeout: 60, pool: "files" });
  const api = pipeline.sourceApi("Fetch", { url: "https://api.example.com/graphql", sourceType: "graphql", pool: "api" });
  const db = pipeline.sourceDb("Query", { query: "SELECT 1", connId: "warehouse", pool: "warehouse" });
  const shaped = pipeline.code("Shape", file, {
    language: "typescript",
    script: "export default (rows) => rows",
    nodePath: "/usr/bin/node",
    maxMemoryMb: 256,
    maxCpuSeconds: 30,
    retries: 1,
    retryDelay: 2000,
    pool: "compute",
  });
  pipeline.sinkDb("Load", shaped, {
    table: "events",
    connId: "clickhouse",
    createTable: true,
    tableEngine: "ReplacingMergeTree",
    retries: 2,
    timeout: 300,
    pool: "clickhouse",
  });
  pipeline.sinkFile("Save", api, { path: "/data/out.csv", pool: "files" });
  pipeline.sinkApi("Post", db, { url: "https://hooks.example.com/x", retries: 1, pool: "api" });
  pipeline.notify("Tell", db, { webhookUrl: "https://hooks.example.com/notify", retries: 2, retryDelay: 1000 });
  return pipeline;
}

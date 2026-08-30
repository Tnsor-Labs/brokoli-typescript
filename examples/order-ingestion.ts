import { cursorPages, Pipeline } from "../src/index";

/**
 * Production-shaped order ingestion:
 *
 * API -> normalize -> split valid/quarantine -> enrich -> quality gate
 * -> idempotent database write, with a separate quarantine table.
 *
 * The source endpoint and database connection are deliberately configuration
 * values. Credentials belong in server-managed connections, not pipeline IR.
 */
export function createOrderIngestionPipeline(options: {
  ordersUrl: string;
  ordersConnection: string;
  warehouseConnection: string;
  schedule?: string;
}): Pipeline {
  const pipeline = new Pipeline("Production order ingestion", {
    pipelineId: "production-order-ingestion",
    description: "Ingest, validate, enrich, quarantine, and upsert orders.",
    schedule: options.schedule ?? "*/15 * * * *",
    scheduleTimezone: "UTC",
    catchUp: true,
    tags: ["production", "orders", "typescript"],
    sla: "00:20 UTC",
    hooks: {
      on_failure: {
        type: "webhook",
        url: "${ORDER_ALERT_WEBHOOK}",
        enabled: true,
      },
    },
  });

  const orders = pipeline.sourceApi("Fetch orders", {
    url: options.ordersUrl,
    connId: options.ordersConnection,
    response: "dataset",
    records: "orders",
    pagination: cursorPages("paging.next_cursor", "cursor").withExecution({
      checkpoint: true,
      max_retries: 4,
      retry_backoff: "exponential",
    }),
    retries: 4,
    retryBackoff: "exponential",
    timeout: 120,
  });

  const normalized = pipeline.task("Normalize orders", orders, (rows) => ({
    columns: ["order_id", "customer_id", "currency", "amount", "ordered_at"],
    rows: rows.map((row) => ({
      order_id: String(row.order_id ?? "").trim(),
      customer_id: String(row.customer_id ?? "").trim(),
      currency: String(row.currency ?? "").trim().toUpperCase(),
      amount: Number(row.amount),
      ordered_at: String(row.ordered_at ?? ""),
    })),
  }), {
    retries: 2,
    timeout: 60,
  });

  const valid = pipeline.filter("Keep valid orders", normalized, (row) =>
    typeof row.order_id === "string" && row.order_id.length > 0 &&
    typeof row.customer_id === "string" && row.customer_id.length > 0 &&
    typeof row.currency === "string" && /^[A-Z]{3}$/.test(row.currency) &&
    typeof row.amount === "number" && Number.isFinite(row.amount) && row.amount >= 0 &&
    typeof row.ordered_at === "string" && !Number.isNaN(Date.parse(row.ordered_at)),
  );

  const quarantine = pipeline.filter("Quarantine invalid orders", normalized, (row) =>
    !(typeof row.order_id === "string" && row.order_id.length > 0 &&
      typeof row.customer_id === "string" && row.customer_id.length > 0 &&
      typeof row.currency === "string" && /^[A-Z]{3}$/.test(row.currency) &&
      typeof row.amount === "number" && Number.isFinite(row.amount) && row.amount >= 0 &&
      typeof row.ordered_at === "string" && !Number.isNaN(Date.parse(row.ordered_at))),
  );

  const enriched = pipeline.map("Add operational fields", valid, (row) => ({
    ...row,
    amount_cents: Math.round(Number(row.amount) * 100),
    ingestion_source: "orders-api",
    ingested_at: new Date().toISOString(),
  }), { columns: ["order_id", "customer_id", "currency", "amount", "ordered_at", "amount_cents", "ingestion_source", "ingested_at"] });

  const quality = pipeline.qualityCheck("Check order quality", enriched, {
    rules: [
      { column: "order_id", rule: "not_null", on_failure: "block" },
      { column: "amount_cents", rule: "min", params: { min: 0 }, on_failure: "block" },
      { column: "currency", rule: "regex", params: { pattern: "^(USD|EUR|GBP)$" }, on_failure: "block" },
    ],
  });
  const stored = pipeline.sinkDb("Upsert orders", quality, {
    connId: options.warehouseConnection,
    table: "analytics.orders",
    mode: "upsert",
    keyColumns: ["order_id"],
  });
  stored;

  quarantine.then(pipeline.sinkDb("Store quarantined orders", undefined, {
    connId: options.warehouseConnection,
    table: "analytics.orders_quarantine",
    mode: "append",
  }));

  return pipeline;
}

/**
 * Local load-test variant. SQL performs set-based filtering and type
 * normalization, while the TypeScript node performs the application-level
 * enrichment. This avoids materializing the full million-row dataset through
 * several language workers while still exercising the TypeScript runtime at
 * production volume.
 */
export function createPostgresOrderLoadPipeline(options: {
  sourceUri: string;
  targetUri: string;
  inputTable?: string;
  outputTable?: string;
  pipelineId?: string;
}): Pipeline {
  const pipeline = new Pipeline("PostgreSQL order load test", {
    pipelineId: options.pipelineId ?? "postgres-order-load-test",
    description: "Process a large PostgreSQL order table with TypeScript nodes.",
    tags: ["load-test", "orders", "typescript"],
    sla: "01:00 UTC",
  });
  const source = pipeline.sourceDb("Read seeded orders", {
    uri: options.sourceUri,
    query: `SELECT order_id, customer_id, upper(currency) AS currency, amount, ordered_at, status FROM ${options.inputTable ?? "orders"} WHERE amount >= 0 AND order_id IS NOT NULL`,
  });
  const enriched = pipeline.task("Add amount cents", source, (rows) => ({
    columns: ["order_id", "customer_id", "currency", "amount", "ordered_at", "status", "amount_cents", "processed_by"],
    rows: rows.map((row) => ({
    ...row,
    amount_cents: Math.round(Number(row.amount) * 100),
    processed_by: "brokoli-typescript",
    })),
  }), { retries: 2, timeout: 300, maxMemoryMb: 1024 });
  const quality = pipeline.qualityCheck("Check processed orders", enriched, {
    rules: [
      { column: "order_id", rule: "not_null", on_failure: "block" },
      { column: "amount_cents", rule: "min", params: { min: 0 }, on_failure: "block" },
    ],
  });
  pipeline.sinkDb("Upsert processed orders", quality, {
    uri: options.targetUri,
    table: options.outputTable ?? "orders_processed",
    mode: "upsert",
    keyColumns: ["order_id"],
  });
  return pipeline;
}

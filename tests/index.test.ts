import { describe, expect, test } from "bun:test";
import { Client, Connection, Param, Pipeline, add, caseWhen, column, cursorPages, datasetSchema, eq, gte, highThroughput, irDigest, literal, publicApiSafe, renderIR, schema, strict, validatePipeline } from "../src/index";

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
  test("refuses branch targets that already have inputs (the then() chain footgun)", () => {
    // Mirrors brokoli-sdk#81: then() returns its target, so
    // when(a.then(b)) would route the branch to b and orphan a.
    const p = new Pipeline("Branch");
    const source = p.sourceFile("Input", { path: "in.csv" });
    const gate = p.conditionNode("Gate", source, "row_count > 0");
    const shaped = p.transform("Shape", undefined, { rules: [{ type: "rename", mapping: { a: "b" } }] });
    const sink = p.sinkFile("Load", undefined, { path: "out.csv" });
    expect(() => gate.when(shaped.then(sink))).toThrow(/already has input/);
    // The refused branch left no condition edge behind.
    expect(p.toJSON().edges.some((e) => "condition" in e)).toBe(false);
    // The explicit form still works, converging downstream included.
    gate.when(shaped);
    shaped.then(sink);
    const fallback = p.notify("Empty", undefined, { notifyType: "webhook", webhookUrl: "https://example.test" });
    gate.otherwise(fallback);
    expect(p.toJSON().edges).toContainEqual({ from: gate.nodeId, to: shaped.nodeId, condition: true });
    expect(p.toJSON().edges).toContainEqual({ from: shaped.nodeId, to: sink.nodeId });
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
  test("expands versioned execution profiles and merges page overrides", () => {
    const p = new Pipeline("Profiles");
    p.sourceApi("Fetch", {
      url: "https://example.test",
      executionProfile: publicApiSafe(),
      pagination: cursorPages("meta.next", "cursor").withExecution({ max_concurrency: 4 }),
    });
    expect(p.toJSON().nodes[0].config.execution).toMatchObject({
      profile: { name: "public_api_safe", version: 1, strict: false },
      max_concurrency: 4,
      requests_per_second: 2,
      page_max_retries: 3,
    });
    expect(p.toJSON().nodes[0].config).toMatchObject({ timeout: 30, max_retries: 3, retry_delay: 1000 });
    expect(highThroughput({ requestsPerSecond: 20 }).requests_per_second).toBe(20);
    expect(strict().profile).toEqual({ name: "strict", version: 1, strict: true });
    const strictPipeline = new Pipeline("Strict");
    strictPipeline.sourceApi("Fetch", { url: "https://example.test", profile: strict(), pagination: cursorPages("next", "cursor").withExecution({ max_concurrency: 2 }) });
    expect(validatePipeline(strictPipeline).errors.some((issue) => issue.field === "execution.max_concurrency")).toBe(true);
  });
  test("emits an explicit join collision policy and right alias", () => {
    const p = new Pipeline("Join");
    const left = p.sourceFile("Left", { path: "left.csv" });
    const right = p.sourceFile("Right", { path: "right.csv" });
    p.join("Merge", left, right, { on: "id", collisionPolicy: "alias", rightAlias: "customer" });
    expect(p.toJSON().nodes[2].config).toMatchObject({
      collision_policy: "alias",
      right_alias: "customer",
    });
  });
  test("emits a dataset schema on a source node", () => {
    const p = new Pipeline("Schema");
    p.sourceApi("Fetch", {
      url: "https://example.test",
      schema: datasetSchema({ id: schema.int64() }),
    });
    expect(p.toJSON().nodes[0].config.schema).toEqual({
      contract: "brokoli.dataset-schema/v1",
      columns: [{ name: "id", type: { kind: "int64" } }],
      additional_columns: "unknown",
    });
  });
  test("emits native project and aggregate rules", () => {
    const p = new Pipeline("Native");
    const source = p.sourceFile("Input", { path: "input.json" });
    const shaped = p.project("Project", source, { score: add(column("amount"), literal(1)) });
    p.aggregate("Totals", shaped, { groupBy: ["status"], aggregations: [{ column: "score", function: "count_distinct", alias: "scores" }] });
    expect(p.toJSON().nodes[1].type).toBe("project");
    expect(p.toJSON().nodes[1].config).toEqual({
      expression_version: 1,
      projections: [{ name: "score", expr: { op: "add", left: { op: "column", path: ["amount"] }, right: { op: "literal", value: 1 } } }],
    });
    expect(p.toJSON().nodes[2].type).toBe("aggregate");
    expect(p.toJSON().nodes[2].config).toEqual({ expression_version: 1, group_by: ["status"], agg_fields: [{ column: "score", function: "count_distinct", alias: "scores" }] });
  });
  test("emits native filter predicates and code output schemas", () => {
    const p = new Pipeline("Native filter");
    const source = p.sourceFile("Input", { path: "input.json" });
    const filtered = p.filterRows("Filter", source, eq(column("status"), literal("ready")));
    p.code("Enrich", filtered, { script: "output_data = { columns, rows }", outputSchema: datasetSchema({ id: schema.int64() }) });
    expect(p.toJSON().nodes[1]).toMatchObject({ type: "filter", config: { expression_version: 1, predicate: { op: "eq" } } });
    expect((p.toJSON().nodes[2].config.output_schema as { contract: string }).contract).toBe("brokoli.dataset-schema/v1");
    expect(caseWhen([[gte(column("amount"), 1), "positive"]], "zero").op).toBe("case_when");
  });
  test("rejects a dataset schema on a scalar API source", () => {
    const p = new Pipeline("Schema");
    expect(() => p.sourceApi("Fetch", {
      url: "https://example.test",
      response: "scalar",
      valuePath: "count",
     schema: datasetSchema({ id: schema.int64() }),
    })).toThrow(/response='dataset'/);
  });
  test("propagates an alias join schema from declared inputs", () => {
    const p = new Pipeline("Join Schema");
    const left = p.sourceApi("Left", {
      url: "https://example.test/left",
      schema: datasetSchema({ id: schema.int64(), name: schema.string() }, { additionalColumns: "closed" }),
    });
    const right = p.sourceApi("Right", {
      url: "https://example.test/right",
      schema: datasetSchema({ id: schema.int64(), name: schema.string() }, { additionalColumns: "closed" }),
    });
    p.join("Merge", left, right, { on: "id", collisionPolicy: "alias", rightAlias: "right_row" });
    expect(p.toJSON().nodes[2].config.schema).toEqual({
      contract: "brokoli.dataset-schema/v1",
      columns: [
        { name: "id", type: { kind: "int64" } },
        { name: "name", type: { kind: "string" } },
        { name: "right_row_name", type: { kind: "string" } },
      ],
      additional_columns: "closed",
    });
  });
  test("preserves decimal precision in a derived join schema", () => {
    const p = new Pipeline("Join Decimal Schema");
    const left = p.sourceApi("Left", {
      url: "https://example.test/left",
      schema: datasetSchema({ id: schema.int64(), amount: schema.decimal({ precision: 20, scale: 4 }) }),
    });
    const right = p.sourceApi("Right", {
      url: "https://example.test/right",
      schema: datasetSchema({ id: schema.int64(), label: schema.string() }),
    });
    p.join("Merge", left, right, { on: "id" });
    const derived = p.toJSON().nodes[2].config.schema as { columns: unknown[] };
    expect(derived.columns).toContainEqual({
      name: "amount",
      type: { kind: "decimal", precision: 20, scale: 4 },
    });
  });
  test("preserves referenced field schema in a native project", () => {
    const p = new Pipeline("Project Schema");
    const source = p.sourceApi("Source", {
      url: "https://example.test/source",
      schema: datasetSchema({ amount: schema.decimal({ precision: 20, scale: 4 }) }),
    });
    p.project("Project", source, { total: column("amount") });
    const derived = p.toJSON().nodes[1].config.schema as { columns: unknown[] };
    expect(derived.columns).toEqual([{ name: "total", type: { kind: "decimal", precision: 20, scale: 4 } }]);
  });
  test("rejects a native project that references a missing declared field", () => {
    const p = new Pipeline("Project Missing Field");
    const source = p.sourceApi("Source", {
      url: "https://example.test/source",
      schema: datasetSchema({ amount: schema.decimal({ precision: 20, scale: 4 }) }),
    });
    expect(() => p.project("Project", source, { total: column("missing") })).toThrow(/missing field/);
  });
  test("rejects incompatible declared join key types", () => {
    const p = new Pipeline("Join Schema");
    const left = p.sourceApi("Left", { url: "https://example.test/left", schema: datasetSchema({ id: schema.int64() }) });
    const right = p.sourceApi("Right", { url: "https://example.test/right", schema: datasetSchema({ id: schema.string() }) });
    expect(() => p.join("Merge", left, right, { on: "id" })).toThrow(/incompatible declared types/);
  });
  test("rejects an alias join without a right alias", () => {
    const p = new Pipeline("Join");
    const left = p.sourceFile("Left", { path: "left.csv" });
    const right = p.sourceFile("Right", { path: "right.csv" });
    expect(() => p.join("Merge", left, right, { on: "id", collisionPolicy: "alias" })).toThrow(/rightAlias/);
  });
  test("rejects an invalid runtime collision policy", () => {
    const p = new Pipeline("Join");
    const left = p.sourceFile("Left", { path: "left.csv" });
    const right = p.sourceFile("Right", { path: "right.csv" });
    expect(() => p.join("Merge", left, right, { on: "id", collisionPolicy: "rename" as "prefix" })).toThrow(/collisionPolicy/);
  });
  test("schema validation rejects fields the canonical IR does not declare", () => {
    const p = new Pipeline("Schema");
    p.sourceFile("Input", { path: "in.csv" });
    const original = p.toJSON.bind(p);
    p.toJSON = () => ({ ...original(), invented_field: true }) as never;
    const result = validatePipeline(p);
    expect(result.valid).toBe(false);
    expect(result.errors.some((issue) => issue.message.includes("additional properties"))).toBe(true);
  });
  test("schema-valid null config returns validation issues instead of throwing", () => {
    const p = new Pipeline("Null config");
    p.sourceFile("Input", { path: "in.csv" });
    const original = p.toJSON.bind(p);
    p.toJSON = () => ({ ...original(), nodes: [{ ...original().nodes[0], config: null }] }) as never;
    expect(() => validatePipeline(p)).not.toThrow();
    expect(validatePipeline(p).valid).toBe(false);
  });
});

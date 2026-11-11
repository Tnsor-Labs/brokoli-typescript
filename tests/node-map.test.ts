/** ADR-050: the `map` option runs a node once per item of an upstream
 * collection, and the deploy gate keeps it off a server that would
 * ignore it. The IR it compiles to is pinned against the Python SDK by
 * tests/differential/fixtures/node_map. */

import { describe, expect, test } from "bun:test";
import { Client } from "../src/client";
import { requiredExecutionFeatures } from "../src/ir";
import { Pipeline } from "../src/pipeline";

function node(p: Pipeline, id: string) {
  const found = p.toJSON().nodes.find((n) => n.id === id);
  if (!found) throw new Error(`no node ${id}`);
  return found;
}

function edgesTo(p: Pipeline, id: string): string[] {
  return p.toJSON().edges.filter((e) => e.to === id).map((e) => e.from).sort();
}

describe("compiling map", () => {
  test("every setting is written, in the IR's names", () => {
    const p = new Pipeline("m", { pipelineId: "m" });
    const customers = p.sourceDb("Customers", { query: "SELECT id FROM c", connId: "shop" });
    const orders = p.sourceDb("Orders", {
      query: "SELECT * FROM o WHERE customer_id = ${item.id}",
      connId: "shop",
      map: { over: customers, key: "id", maxItems: 500, maxParallel: 8, onItemFailure: "continue" },
    });
    expect(node(p, orders.nodeId).config.map).toEqual({
      over: customers.nodeId,
      key: "id",
      max_items: 500,
      max_parallel: 8,
      on_item_failure: "continue",
    });
    expect(edgesTo(p, orders.nodeId)).toEqual([customers.nodeId]);
  });

  test("only the settings given are written", () => {
    const p = new Pipeline("m", { pipelineId: "m" });
    const ids = p.sourceDb("Ids", { query: "SELECT id FROM t", connId: "db" });
    const page = p.sourceApi("Page", { url: "https://api.example/items/${item.id}", map: { over: ids } });
    expect(node(p, page.nodeId).config.map).toEqual({ over: ids.nodeId });
  });

  test("a record zips collections, with one edge from each", () => {
    const p = new Pipeline("m", { pipelineId: "m" });
    const files = p.sourceDb("Files", { query: "SELECT f FROM files", connId: "db" });
    const schemas = p.sourceDb("Schemas", { query: "SELECT s FROM schemas", connId: "db" });
    const parsed = p.code("Parse", files, { script: "pass", map: { over: { file: files, schema: schemas } } });
    expect(node(p, parsed.nodeId).config.map).toEqual({ over: { file: files.nodeId, schema: schemas.nodeId } });
    expect(edgesTo(p, parsed.nodeId)).toEqual([files.nodeId, schemas.nodeId].sort());
  });

  test("a task maps", () => {
    const p = new Pipeline("m", { pipelineId: "m" });
    const customers = p.sourceDb("Customers", { query: "SELECT id FROM c", connId: "shop" });
    const scored = p.task("Score", customers, function score(rows) {
      return { columns: ["id"], rows };
    }, { map: { over: customers, key: "id" } });
    const n = node(p, scored.nodeId);
    expect(n.type).toBe("code");
    expect(n.config.language).toBe("typescript");
    expect(n.config.map).toEqual({ over: customers.nodeId, key: "id" });
  });
});

describe("refusing a bad map", () => {
  const cases: Array<[string, Record<string, unknown>, RegExp]> = [
    ["over is not a node", { over: "customers" }, /upstream node or a record/],
    ["over is empty", { over: {} }, /at least one collection/],
    ["a zipped value is not a node", { over: { a: "x" } }, /map\.over\.a must be an upstream node/],
    ["key is empty", { key: " " }, /column name/],
    ["maxItems is zero", { maxItems: 0 }, /at least 1/],
    ["maxParallel is fractional", { maxParallel: 2.5 }, /at least 1/],
    ["onItemFailure is unknown", { onItemFailure: "retry" }, /fail, continue, skip/],
  ];
  for (const [label, bad, message] of cases) {
    test(label, () => {
      const p = new Pipeline("m", { pipelineId: "m" });
      const customers = p.sourceDb("Customers", { query: "SELECT id FROM c", connId: "shop" });
      const before = p.toJSON().nodes.length;
      expect(() => p.sourceDb("Orders", { query: "SELECT 1", connId: "shop", map: { over: customers, ...bad } as never })).toThrow(message);
      expect(p.toJSON().nodes.length).toBe(before);
    });
  }

  test("a node of another pipeline is refused and nothing is registered", () => {
    const other = new Pipeline("a", { pipelineId: "a" });
    const elsewhere = other.sourceDb("Elsewhere", { query: "SELECT 1", connId: "db" });
    const p = new Pipeline("b", { pipelineId: "b" });
    expect(() => p.sourceDb("Orders", { query: "SELECT 1", connId: "db", map: { over: elsewhere } })).toThrow();
    expect(p.toJSON().nodes).toEqual([]);
  });
});

function fakeServer(capabilities: Record<string, unknown>): typeof fetch {
  return (async (input: string | URL | Request) => {
    const path = new URL(String(input)).pathname;
    if (path === "/api/capabilities") return new Response(JSON.stringify(capabilities), { status: 200 });
    return new Response(JSON.stringify({ error: "nope" }), { status: 404 });
  }) as unknown as typeof fetch;
}

const MAPPING_SERVER = {
  supported_ir_versions: ["2.0", "2.1", "2.2"],
  supported_execution_features: ["node-map"],
  map_node_types: ["code", "source_api", "source_db", "task"],
};

function mapped(): Pipeline {
  const p = new Pipeline("mapped", { pipelineId: "mapped" });
  const customers = p.sourceDb("Customers", { query: "SELECT id FROM c", connId: "shop" });
  p.sourceDb("Orders", { query: "SELECT * FROM o WHERE id = ${item.id}", connId: "shop", map: { over: customers } });
  return p;
}

function client(caps: Record<string, unknown>): Client {
  return new Client("http://server", { apiKey: "k", fetch: fakeServer(caps) });
}

describe("the deploy gate", () => {
  test("a mapped node requires node-map", () => {
    expect(requiredExecutionFeatures(mapped().toJSON())).toContain("node-map");
  });

  test("a server that can map accepts it", async () => {
    await client(MAPPING_SERVER).preflight(mapped());
  });

  test("a server without node-map refuses it", async () => {
    // Such a server would ignore the block and run the node once.
    await expect(client({ ...MAPPING_SERVER, supported_execution_features: [], map_node_types: undefined }).preflight(mapped())).rejects.toThrow(/node-map/);
  });

  test("a server too old to list features refuses it", async () => {
    await expect(client({ supported_ir_versions: ["2.0", "2.1", "2.2"] }).preflight(mapped(), true)).rejects.toThrow(/node-map/);
  });

  test("a type the server cannot map is refused", async () => {
    await expect(client({ ...MAPPING_SERVER, map_node_types: ["code", "task"] }).preflight(mapped())).rejects.toThrow(/cannot map source_db nodes/);
  });

  test("a malformed map_node_types fails closed", async () => {
    await expect(client({ ...MAPPING_SERVER, map_node_types: "code" }).preflight(mapped())).rejects.toThrow(/Malformed server capabilities/);
  });

  test("a pipeline that does not map needs nothing new", async () => {
    const p = new Pipeline("plain", { pipelineId: "plain" });
    p.sourceDb("Customers", { query: "SELECT id FROM c", connId: "shop" });
    expect(requiredExecutionFeatures(p.toJSON())).not.toContain("node-map");
    await client({ ...MAPPING_SERVER, supported_execution_features: [], map_node_types: undefined }).preflight(p);
  });
});

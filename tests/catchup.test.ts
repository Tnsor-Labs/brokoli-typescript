/** Core v0.18.12 catch-up: the node options, validation rules, client
 * operations and node-type preflight added alongside the Python SDK. */

import { describe, expect, test } from "bun:test";
import { Client } from "../src/client";
import { Pipeline } from "../src/pipeline";
import { validatePipeline } from "../src/validate";

type Seen = { method: string; path: string; workspace: string | null; body: unknown };

function fakeServer(answers: Record<string, unknown>, seen: Seen[]): typeof fetch {
  return (async (input: string | URL | Request, init: RequestInit = {}) => {
    const url = new URL(String(input));
    const headers = new Headers(init.headers);
    seen.push({
      method: init.method || "GET",
      path: url.pathname,
      workspace: headers.get("x-workspace-id"),
      body: init.body ? JSON.parse(String(init.body)) : undefined,
    });
    const key = `${init.method || "GET"} ${url.pathname}`;
    const body = key in answers ? answers[key] : { error: "nope" };
    return new Response(JSON.stringify(body), { status: key in answers ? 200 : 404 });
  }) as unknown as typeof fetch;
}

const PIPES = [{ id: "uuid-1", pipeline_id: "orders", name: "Orders" }];

function errorsFor(build: (p: Pipeline) => void): Array<{ field: string; message: string }> {
  const pipeline = new Pipeline("t", { pipelineId: "t" });
  build(pipeline);
  return validatePipeline(pipeline).errors;
}

describe("validation mirrors the server", () => {
  test("wait conditions and durations", () => {
    expect(errorsFor((p) => p.wait("W", { condition: "http", url: "https://x", timeout: "2h30m", pollInterval: 60 }))).toEqual([]);
    expect(errorsFor((p) => p.wait("W", { condition: "interval_elapsed", offset: "-5m" }))).toEqual([]);
    for (const [options, field] of [
      [{}, "condition"],
      [{ condition: "file_exists" }, "path"],
      [{ condition: "pipeline" }, "pipeline_id"],
      [{ condition: "interval_elapsed", pollInterval: "0s" }, "poll_interval"],
      [{ condition: "interval_elapsed", timeout: "soon" }, "timeout"],
    ] as const) {
      expect(errorsFor((p) => p.wait("W", options as never)).map((e) => e.field)).toContain(field);
    }
  });

  test("contract_gate needs a contract IR", () => {
    const contract = { ir_version: "1.0", contract: { id: "c", version: "1" }, input: { kind: "record-stream" }, rules: [] };
    expect(errorsFor((p) => p.contractGate("G", undefined, { contract }))).toEqual([]);
    expect(errorsFor((p) => p.contractGate("G", undefined, { contract: { toIR: () => contract } }))).toEqual([]);
    expect(errorsFor((p) => p.contractGate("G", undefined, {})).map((e) => e.field)).toContain("contract");
    expect(errorsFor((p) => p.contractGate("G", undefined, { contract: { ir_version: "1.0" } })).map((e) => e.field)).toContain("contract");
  });

  test("partitioned migrate", () => {
    const base = { sourceConnId: "a", targetConnId: "b", query: "SELECT * FROM t", table: "t", mode: "upsert", keyColumns: ["id"] };
    const numeric = { column: "id", strategy: "numeric" as const, boundaries: [0, 10, 20] };
    expect(errorsFor((p) => p.migrate("M", { ...base, partition: numeric }))).toEqual([]);
    for (const [options, field] of [
      [{ ...base, partition: { ...numeric, boundaries: [10, 0] } }, "partition"],
      [{ ...base, partition: { ...numeric, maxParallel: 65 } }, "partition"],
      [{ ...base, partition: { column: "d", strategy: "date", boundaries: ["2026-01-01", "2026-02-01"] }, keyColumns: ["d"] }, "partition"],
      [{ ...base, mode: "append", partition: numeric }, "mode"],
      [{ ...base, keyColumns: ["other"], partition: numeric }, "key_columns"],
      [{ ...base, createTable: true, partition: numeric }, "create_table"],
      [{ table: "t" }, "conn"],
    ] as const) {
      expect(errorsFor((p) => p.migrate("M", options as never)).map((e) => e.field)).toContain(field);
    }
  });

  test("code: ${param} in a script and resource limits", () => {
    expect(errorsFor((p) => p.code("C", undefined, { script: 'const t = "${param.threshold}"' })).map((e) => e.field)).toContain("script");
    expect(errorsFor((p) => p.code("C", undefined, { script: "pass", maxMemoryMb: 0 })).map((e) => e.field)).toContain("max_memory_mb");
    expect(errorsFor((p) => p.code("C", undefined, { script: "pass", maxMemoryMb: 64, maxCpuSeconds: 5 }))).toEqual([]);
  });
});

describe("client", () => {
  test("workspace rides on every request", async () => {
    const seen: Seen[] = [];
    const client = new Client("http://server", {
      apiKey: "k",
      workspace: "team-a",
      fetch: fakeServer({ "GET /api/pipelines": PIPES, "POST /api/pipelines/uuid-1/run": { run_id: "r1" }, "GET /api/runs/r1": { status: "success" } }, seen),
    });
    await (await client.run("orders")).detail();
    expect(new Set(seen.map((s) => s.workspace))).toEqual(new Set(["team-a"]));
  });

  test("fromEnv reads BROKOLI_WORKSPACE; an explicit workspace wins", async () => {
    const saved = { ws: process.env.BROKOLI_WORKSPACE, token: process.env.BROKOLI_TOKEN };
    process.env.BROKOLI_WORKSPACE = "from-env";
    process.env.BROKOLI_TOKEN = "k";
    try {
      const seen: Seen[] = [];
      const fetch = fakeServer({ "GET /api/pipelines": PIPES }, seen);
      await (await Client.fromEnv("http://server", { fetch })).pipelines();
      await (await Client.fromEnv("http://server", { fetch, workspace: "explicit" })).pipelines();
      expect(seen.map((s) => s.workspace)).toEqual(["from-env", "explicit"]);
    } finally {
      if (saved.ws === undefined) delete process.env.BROKOLI_WORKSPACE; else process.env.BROKOLI_WORKSPACE = saved.ws;
      if (saved.token === undefined) delete process.env.BROKOLI_TOKEN; else process.env.BROKOLI_TOKEN = saved.token;
    }
  });

  test("dry run, resume from a node, backfill bounds and force", async () => {
    const seen: Seen[] = [];
    const client = new Client("http://server", {
      apiKey: "k",
      fetch: fakeServer({
        "GET /api/pipelines": PIPES,
        "POST /api/pipelines/uuid-1/dry-run": { results: [{ node_id: "n1" }] },
        "POST /api/runs/old/resume": { id: "new" },
        "POST /api/pipelines/uuid-1/backfill": { intervals: 3 },
      }, seen),
    });
    expect((await client.dryRun("orders")).results).toEqual([{ node_id: "n1" }]);
    expect((await client.runHandle("old").resume({ fromNode: "load" })).id).toBe("new");
    expect((await client.retry("old")).id).toBe("new");
    await client.backfill("orders", { start: "2026-10-01", end: new Date("2026-10-03T00:00:00Z"), force: true });
    const bodies = seen.filter((s) => s.method === "POST").map((s) => [s.path, s.body]);
    expect(bodies).toEqual([
      ["/api/pipelines/uuid-1/dry-run", {}],
      ["/api/runs/old/resume", { from_node: "load" }],
      ["/api/runs/old/resume", {}],
      ["/api/pipelines/uuid-1/backfill", { start_date: "2026-10-01", end: "2026-10-03T00:00:00.000Z", force: true }],
    ]);
  });

  test("preflight refuses node types the server predates or disabled", async () => {
    const caps = (extra: Record<string, unknown>) => ({
      "GET /api/capabilities": {
        supported_ir_versions: ["2.0", "2.1", "2.2"],
        supported_execution_features: [],
        node_type_capabilities: { source_file: ["source"], bash: ["compute"] },
        disabled_node_types: null,
        ...extra,
      },
    });
    const pipeline = new Pipeline("p", { pipelineId: "p" });
    pipeline.bash("Run", pipeline.sourceFile("Read", { path: "/d/x.csv" }), { command: "true" });

    const ok = new Client("http://server", { apiKey: "k", fetch: fakeServer(caps({}), []) });
    await ok.preflight(pipeline);

    const disabled = new Client("http://server", { apiKey: "k", fetch: fakeServer(caps({ disabled_node_types: ["bash"] }), []) });
    await expect(disabled.preflight(pipeline)).rejects.toThrow(/bash nodes are disabled on this server/);

    const old = new Client("http://server", { apiKey: "k", fetch: fakeServer(caps({ node_type_capabilities: { source_file: ["source"] } }), []) });
    await expect(old.preflight(pipeline)).rejects.toThrow(/does not know the bash node type/);

    const legacy = new Client("http://server", { apiKey: "k", fetch: fakeServer(caps({ node_type_capabilities: undefined, disabled_node_types: undefined }), []) });
    await legacy.preflight(pipeline);

    const malformed = new Client("http://server", { apiKey: "k", fetch: fakeServer(caps({ disabled_node_types: "bash" }), []) });
    await expect(malformed.preflight(pipeline)).rejects.toThrow(/Malformed server capabilities/);
  });
});

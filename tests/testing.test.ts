import { describe, expect, test } from "bun:test";
import { Client, Pipeline, expectGraph, livePipeline, snapshotRun, watch } from "../src";
import type { Run } from "../src";

describe("testing helpers", () => {
  test("asserts graph shape by id or display name", () => {
    const p = new Pipeline("Graph");
    const source = p.sourceFile("Input", { path: "in.csv" });
    const sink = p.sinkFile("Output", source, { path: "out.csv" });
    const graph = expectGraph(p);
    expect(() => graph.hasNode("Input").hasEdge(source.nodeId, "Output")).not.toThrow();
    expect(() => graph.hasEdge("missing", "Output")).toThrow(/missing ->/);
    expect(graph.roots()).toEqual([source.nodeId]);
    expect(graph.leaves()).toEqual([sink.nodeId]);
  });

  test("refuses ambiguous display names", () => {
    const p = new Pipeline("Ambiguous");
    const a = p.sourceFile("Same", { path: "a.csv" });
    const b = p.sourceFile("Same", { path: "b.csv" });
    p.sinkFile("Output", a, { path: "out.csv" });
    expect(() => expectGraph(p).hasEdge("Same", "Output")).toThrow(/ambiguous/);
    expect(() => expectGraph(p).hasNode(b.nodeId)).not.toThrow();
  });

  test("watch polls a selected run until terminal", async () => {
    const responses = [
      { items: [{ id: "run-1", status: "running", started_at: "2026-01-01" }] },
      { items: [{ id: "run-1", status: "success", started_at: "2026-01-01" }] },
    ];
    const client = {
      pipeline: async () => ({ id: "pipeline-1" }),
      request: async () => responses.shift(),
    } as unknown as Client;
    expect(await watch(client, "orders", { pollInterval: 0 })).toMatchObject({ id: "run-1", status: "success" });
  });

  test("watch filters by the run API started_at field", async () => {
    const client = {
      pipeline: async () => ({ id: "pipeline-1" }),
      request: async () => ({ items: [{ id: "run-2", status: "success", started_at: "2026-02-01" }] }),
    } as unknown as Client;
    expect(await watch(client, "orders", { after: "2026-01-01", pollInterval: 0 })).toMatchObject({ id: "run-2" });
  });

  test("snapshot aggregates detail, node runs, and logs", async () => {
    const run = {
      detail: async () => ({ id: "run-1", status: "success", node_runs: [{ id: "node-1" }] }),
      logs: async () => [{ message: "done" }],
    } as unknown as Run;
    expect(await snapshotRun(run)).toEqual({
      run: { id: "run-1", status: "success", node_runs: [{ id: "node-1" }] },
      nodeRuns: [{ id: "node-1" }],
      logs: [{ message: "done" }],
    });
  });

  test("live harness always deletes its deployment", async () => {
    const calls: string[] = [];
    const client = {
      deploy: async () => { calls.push("deploy"); },
      pipeline: async () => ({ id: "pipeline-1" }),
      run: async () => ({ id: "run-1" }),
      request: async (_path: string, init: RequestInit) => { calls.push(String(init.method)); },
    } as unknown as Client;
    const pipeline = new Pipeline("Live", { pipelineId: "live" });
    pipeline.sourceFile("Input", { path: "in.csv" });
    await expect(livePipeline(client, pipeline, async () => { throw new Error("assertion failed"); })).rejects.toThrow(/assertion failed/);
    expect(calls).toEqual(["deploy", "DELETE"]);
  });
});

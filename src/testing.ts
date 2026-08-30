import { APIError } from "./errors";
import type { Client, Run } from "./client";
import type { Edge, PipelineIR } from "./ir";
import type { Pipeline } from "./pipeline";

export class GraphExpectation {
  private readonly ir: PipelineIR;
  constructor(pipeline: Pipeline) { this.ir = pipeline.toJSON(); }
  private resolve(value: string): string {
    const idMatch = this.ir.nodes.find((node) => node.id === value);
    if (idMatch) return idMatch.id;
    const nameMatches = this.ir.nodes.filter((node) => node.name === value);
    if (nameMatches.length > 1) throw new Error(`Graph node name ${value} is ambiguous; use a node id`);
    return nameMatches[0]?.id || value;
  }
  hasNode(value: string): this {
    if (!this.ir.nodes.some((node) => node.id === value || node.name === value)) throw new Error(`Expected graph to contain node ${value}`);
    return this;
  }
  hasEdge(from: string, to: string, condition?: boolean): this {
    const expected: Edge = { from: this.resolve(from), to: this.resolve(to), ...(condition === undefined ? {} : { condition }) };
    if (!this.ir.edges.some((edge) => edge.from === expected.from && edge.to === expected.to && edge.condition === expected.condition)) {
      throw new Error(`Expected graph to contain edge ${expected.from} -> ${expected.to}${condition === undefined ? "" : ` (${condition})`}`);
    }
    return this;
  }
  roots(): string[] { const targets = new Set(this.ir.edges.map((edge) => edge.to)); return this.ir.nodes.filter((node) => !targets.has(node.id)).map((node) => node.id); }
  leaves(): string[] { const sources = new Set(this.ir.edges.map((edge) => edge.from)); return this.ir.nodes.filter((node) => !sources.has(node.id)).map((node) => node.id); }
}

export function expectGraph(pipeline: Pipeline): GraphExpectation {
  return new GraphExpectation(pipeline);
}

export async function snapshotRun(run: Run) {
  const [detail, logs] = await Promise.all([run.detail(), run.logs()]);
  return { run: detail, nodeRuns: (detail.node_runs as unknown[]) || [], logs };
}

export async function watch(
  client: Client,
  pipeline: string,
  options: { timeout?: number; pollInterval?: number; after?: string } = {},
): Promise<Record<string, unknown>> {
  const target = await client.pipeline(pipeline);
  const deadline = Date.now() + (options.timeout ?? 600) * 1000;
  const terminal = new Set(["success", "succeeded", "failed", "cancelled", "canceled", "blocked"]);
  let selectedId = "";
  while (Date.now() < deadline) {
    const body = await client.request(`/api/pipelines/${encodeURIComponent(target.id)}/runs?limit=100`);
    const items = (Array.isArray(body) ? body : body.items || body.runs || []) as Array<Record<string, unknown>>;
    const match = selectedId
      ? items.find((run) => run.id === selectedId)
      : items.find((run) => !options.after || String(run.started_at || "") > options.after);
    if (match) {
      selectedId = String(match.id || "");
      if (terminal.has(String(match.status || ""))) return match;
    }
    await Bun.sleep((options.pollInterval ?? 1) * 1000);
  }
  throw new APIError(`Timed out waiting for a run of pipeline ${pipeline}`, 0);
}

export async function livePipeline<T>(
  client: Client,
  pipeline: Pipeline,
  execute: (run: Run) => Promise<T>,
): Promise<T> {
  await client.deploy(pipeline);
  const target = await client.pipeline(pipeline.pipelineId);
  try {
    return await execute(await client.run(pipeline.pipelineId));
  } finally {
    await client.request(`/api/pipelines/${encodeURIComponent(target.id)}`, { method: "DELETE" }).catch(() => undefined);
  }
}

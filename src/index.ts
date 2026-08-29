export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Config = Record<string, unknown>;
export type Capability = "source" | "sink" | "compute" | "dataset-output" | "dynamic-expansion" | "collection-output";
export type Edge = { from: string; to: string; condition?: boolean };
export type IRNode = { id: string; type: string; name: string; config: Config; capabilities: Capability[]; position?: { x: number; y: number } };
export type PipelineIR = {
  pipeline_id: string; name: string; description: string; schedule: string; enabled: boolean;
  ir_version: "2.0" | "2.1"; nodes: IRNode[]; edges: Edge[]; tags: string[]; depends_on: string[];
  schedule_timezone?: string; sla_deadline?: string; sla_timezone?: string; hooks?: Record<string, unknown>; webhook_token?: string;
};

const CAPABILITIES: Record<string, Capability[]> = {
  source_file: ["source", "dataset-output"], source_api: ["source", "dataset-output"], source_db: ["source", "dataset-output"],
  dbt: ["source", "dataset-output"], migrate: ["source", "dataset-output"], sink_db: ["sink"], sink_file: ["sink"], sink_api: ["sink"],
  union: ["compute", "dataset-output"], dataset_map: ["compute", "dataset-output"], dataset_filter: ["compute", "dataset-output"]
};
const clone = <T>(value: T): T => structuredClone(value);
const clean = (name: string) => (name.toLowerCase().replace(/[^a-z0-9_]/g, "").slice(0, 20) || "node");
const slug = (name: string) => (name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""));
const optional = (config: Config, values: Record<string, unknown>) => {
  for (const [key, value] of Object.entries(values)) if (value !== undefined && value !== null) config[key] = clone(value);
  return config;
};

export class PipelineError extends Error {}

export class NodeRef {
  constructor(readonly nodeId: string, readonly pipeline: Pipeline, readonly kind = "node") {}
  then<T extends NodeRef>(target: T): T { this.pipeline.edge(this, target); return target; }
  connect<T extends NodeRef>(target: T): T { return this.then(target); }
  fanOut(...targets: NodeRef[]): NodeRef[] { targets.forEach(target => this.pipeline.edge(this, target)); return targets; }
}
export class DatasetRef extends NodeRef {
  map(name: string, fn: { name?: string; toString(): string }): DatasetRef { return this.pipeline.register("dataset_map", name || `dataset_map(${fn.name || "fn"})`, { function: { name: fn.name || fn.toString() } }, this, "dataset"); }
  filter(name: string, fn: { name?: string; toString(): string }): DatasetRef { return this.pipeline.register("dataset_filter", name || `dataset_filter(${fn.name || "fn"})`, { function: { name: fn.name || fn.toString() } }, this, "dataset"); }
}
export class ScalarRef extends NodeRef {}
export class ArtifactRef extends NodeRef {}
export class ConditionRef extends NodeRef {
  when<T extends NodeRef>(target: T): T { this.pipeline.branch(this, target, true); return target; }
  otherwise<T extends NodeRef>(target: T): T { this.pipeline.branch(this, target, false); return target; }
}

export type PipelineOptions = { pipelineId?: string; description?: string; schedule?: string; scheduleTimezone?: string; tags?: string[]; dependsOn?: string[]; sla?: string; webhook?: boolean; hooks?: Record<string, unknown> };
export class Pipeline {
  readonly pipelineId: string; readonly nodes: IRNode[] = []; readonly edges: Edge[] = [];
  private counters = new Map<string, number>();
  constructor(readonly name: string, options: PipelineOptions = {}) {
    this.pipelineId = options.pipelineId || slug(name);
    this.description = options.description || ""; this.schedule = options.schedule || "";
    this.scheduleTimezone = options.scheduleTimezone || ""; this.tags = options.tags || []; this.dependsOn = options.dependsOn || [];
    this.sla = options.sla || ""; this.webhook = options.webhook || false; this.hooks = options.hooks;
  }
  readonly description: string; readonly schedule: string; readonly scheduleTimezone: string; readonly tags: string[]; readonly dependsOn: string[]; readonly sla: string; readonly webhook: boolean; readonly hooks?: Record<string, unknown>;
  private id(name: string, nodeKey?: string) { if (nodeKey) { if (!/^[a-z][a-z0-9_-]{0,63}$/.test(nodeKey) || this.nodes.some(n => n.id === nodeKey)) throw new PipelineError(`Invalid or duplicate node_key ${nodeKey}`); return nodeKey; } const base = clean(name); let n = this.counters.get(base) || 0; do { n++; } while (this.nodes.some(node => node.id === `${base}_${n}`)); this.counters.set(base, n); return `${base}_${n}`; }
  register<T extends NodeRef = NodeRef>(type: string, name: string, config: Config = {}, ...args: (NodeRef | string | undefined)[]): T {
    const kind = typeof args.at(-1) === "string" ? args.pop() as string : "node"; const nodeKey = typeof args.at(-1) === "string" ? args.pop() as string : undefined;
    const inputs = args.filter((x): x is NodeRef => x instanceof NodeRef); inputs.forEach(x => this.assertRef(x));
    const id = this.id(name, nodeKey); this.nodes.push({ id, type, name, config: clone(config), capabilities: [...(CAPABILITIES[type] || ["compute"])] });
    inputs.forEach(input => this.edge(input, new NodeRef(id, this))); return this.ref(id, kind) as T;
  }
  private ref(id: string, kind: string): NodeRef { const base = kind === "dataset" ? DatasetRef : kind === "scalar" ? ScalarRef : kind === "artifact" ? ArtifactRef : kind === "condition" ? ConditionRef : NodeRef; return new base(id, this, kind); }
  private assertRef(ref: NodeRef) { if (ref.pipeline !== this || !this.nodes.some(n => n.id === ref.nodeId)) throw new PipelineError("Node belongs to another pipeline"); }
  edge(from: NodeRef, to: NodeRef, condition?: boolean) { this.assertRef(from); this.assertRef(to); const found = this.edges.find(e => e.from === from.nodeId && e.to === to.nodeId); if (found) { if (found.condition !== condition) throw new PipelineError("Edge cannot belong to multiple branches"); return; } this.edges.push({ from: from.nodeId, to: to.nodeId, ...(condition === undefined ? {} : { condition }) }); }
  branch(from: ConditionRef, to: NodeRef, condition: boolean) { if (this.edges.filter(e => e.to === from.nodeId).length !== 1) throw new PipelineError("Condition must have exactly one input"); this.edge(from, to, condition); }
  sourceFile(name: string, options: { path?: string; format?: string; nodeKey?: string } = {}) { return this.register<DatasetRef>("source_file", name, optional({ path: options.path || "", format: options.format || "csv" }, {}), options.nodeKey, "dataset"); }
  sourceDb(name: string, options: { query?: string; connId?: string; uri?: string; nodeKey?: string } = {}) { return this.register<DatasetRef>("source_db", name, optional({ query: options.query || "", conn_id: options.connId, uri: options.uri }, {}), options.nodeKey, "dataset"); }
  sourceApi(name: string, options: { url?: string; method?: string; headers?: Config; body?: unknown; connId?: string; params?: Config; response?: "dataset" | "scalar" | "artifact"; records?: string; valuePath?: string; pagination?: Config; nodeKey?: string } = {}) { const response = options.response || "dataset"; return this.register("source_api", name, optional({ url: options.url || "", method: options.method || "GET", response, headers: options.headers, body: options.body, conn_id: options.connId, params: options.params, records: options.records, value_path: options.valuePath, pagination: options.pagination }, {}), options.nodeKey, response); }
  transform(name: string, input?: NodeRef, options: { rules?: unknown[]; nodeKey?: string } = {}) { return this.register<DatasetRef>("transform", name, { ...(options.rules?.length ? { rules: options.rules } : {}) }, input, options.nodeKey, "dataset"); }
  join(name: string, left?: NodeRef, right?: NodeRef, options: { on?: string; leftKey?: string; rightKey?: string; how?: string; nodeKey?: string } = {}) { const leftKey = options.leftKey || options.on || "", rightKey = options.rightKey || leftKey; return this.register<DatasetRef>("join", name, { join_type: options.how || "inner", left_key: leftKey, right_key: rightKey }, left, right, options.nodeKey, "dataset"); }
  qualityCheck(name: string, input?: NodeRef, options: { rules?: unknown[]; nodeKey?: string } = {}) { return this.register("quality_check", name, options.rules?.length ? { rules: options.rules } : {}, input, options.nodeKey); }
  sinkFile(name: string, input?: NodeRef, options: { path?: string; format?: string; compress?: string; nodeKey?: string } = {}) { return this.register("sink_file", name, optional({ path: options.path || "", format: options.format || "csv", compress: options.compress }, {}), input, options.nodeKey); }
  sinkDb(name: string, input?: NodeRef, options: { table?: string; mode?: string; connId?: string; uri?: string; keyColumns?: string[]; nodeKey?: string } = {}) { return this.register("sink_db", name, optional({ table: options.table || "", mode: options.mode || "append", conn_id: options.connId, uri: options.uri, key_columns: options.keyColumns }, {}), input, options.nodeKey); }
  sinkApi(name: string, input?: NodeRef, options: { url?: string; method?: string; body?: unknown; headers?: Config; connId?: string; nodeKey?: string } = {}) { return this.register("sink_api", name, optional({ url: options.url || "", method: options.method || "POST", body_template: options.body, headers: options.headers, conn_id: options.connId }, {}), input, options.nodeKey); }
  migrate(name: string, options: Config = {}) { return this.register<DatasetRef>("migrate", name, { source_uri: options.sourceUri || "", dest_uri: options.targetUri || "", source_query: options.query || "", dest_table: options.table || "", mode: options.mode || "append" }, options.nodeKey as string | undefined, "dataset"); }
  dbt(name: string, options: Config = {}) { return this.register<DatasetRef>("dbt", name, optional({ command: options.command || "run", project_dir: options.projectDir, target: options.target, select: options.select, profiles_dir: options.profilesDir, vars: options.vars }, {}), options.nodeKey as string | undefined, "dataset"); }
  notify(name: string, input?: NodeRef, options: Config = {}) { return this.register("notify", name, optional({ notify_type: options.notifyType || "webhook", webhook_url: options.webhookUrl || "", message: options.message, channel: options.channel }, {}), input, options.nodeKey as string | undefined); }
  conditionNode(name: string, input: NodeRef | undefined, expression: string, nodeKey?: string) { return this.register<ConditionRef>("condition", name, { expression }, input, nodeKey, "condition"); }
  union(name: string, ...refs: NodeRef[]) { refs.forEach(ref => this.assertRef(ref)); return this.register<DatasetRef>("union", name, { mode: "union" }, ...refs, "dataset"); }
  toJSON(): PipelineIR { const sla = this.sla.split(" ", 2); return { pipeline_id: this.pipelineId, name: this.name, description: this.description, schedule: this.schedule, enabled: true, ir_version: this.edges.some(e => e.condition !== undefined) ? "2.1" : "2.0", nodes: clone(this.nodes), edges: clone(this.edges), tags: [...this.tags], depends_on: [...this.dependsOn], ...(this.scheduleTimezone ? { schedule_timezone: this.scheduleTimezone } : {}), ...(this.sla ? { sla_deadline: sla[0], sla_timezone: sla[1] || "UTC" } : {}), ...(this.hooks ? { hooks: clone(this.hooks) } : {}), ...(this.webhook ? { webhook_token: "" } : {}) }; }
  compile(): PipelineIR { return this.toJSON(); }
  digest(): string { return digest(this.toJSON()); }
}

const codepointCompare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
const sortValue = (value: unknown): unknown => Array.isArray(value) ? value.map(sortValue) : value && typeof value === "object" ? Object.fromEntries(Object.entries(value as Record<string, unknown>).sort(([a], [b]) => codepointCompare(a, b)).map(([k, v]) => [k, sortValue(v)])) : value;
export function normalizeIR(input: PipelineIR): PipelineIR { const ir = clone(input); for (const key of ["id", "source", "workspace_id", "org_id", "created_at", "updated_at"]) delete (ir as any)[key]; ir.nodes = ir.nodes.map(node => { const copy = { ...node }; delete copy.position; copy.capabilities = [...(copy.capabilities?.length ? copy.capabilities : CAPABILITIES[copy.type] || ["compute"])].sort(); return copy; }).sort((a, b) => a.id.localeCompare(b.id)); ir.tags.sort(); ir.depends_on.sort(); if (ir.schedule_timezone === "" || ir.schedule_timezone === "UTC") delete ir.schedule_timezone; if (ir.webhook_token !== undefined) ir.webhook_token = ""; return ir; }
export function canonicalJSON(value: unknown): string { return JSON.stringify(sortValue(value), null, 2) + "\n"; }
export function renderIR(ir: PipelineIR): string { return canonicalJSON(normalizeIR(ir)); }
function digest(ir: PipelineIR): string { const hash = new Bun.CryptoHasher("sha256"); hash.update(renderIR(ir)); return `sha256:${hash.digest("hex")}`; }
export function irDigest(ir: PipelineIR): string { return digest(ir); }

export type ValidationIssue = { nodeName: string; field: string; message: string; severity: "error" | "warning" };
export type ValidationResult = { errors: ValidationIssue[]; warnings: ValidationIssue[]; valid: boolean };
export function validatePipeline(pipeline: Pipeline): ValidationResult { const data = pipeline.toJSON(); const errors: ValidationIssue[] = [], warnings: ValidationIssue[] = []; const error = (nodeName: string, field: string, message: string) => errors.push({ nodeName, field, message, severity: "error" }); const warning = (nodeName: string, field: string, message: string) => warnings.push({ nodeName, field, message, severity: "warning" }); if (!data.name) error("", "name", "Pipeline name is required"); if (!data.nodes.length) error("", "nodes", "Pipeline must have at least one node"); const ids = new Set(data.nodes.map(n => n.id)); const indegree = new Map(data.nodes.map(n => [n.id, 0])); for (const edge of data.edges) { if (!ids.has(edge.from)) error("", "edge", `Edge references unknown source node: ${edge.from}`); if (!ids.has(edge.to)) error("", "edge", `Edge references unknown target node: ${edge.to}`); if (ids.has(edge.to)) indegree.set(edge.to, (indegree.get(edge.to) || 0) + 1); } for (const node of data.nodes) { const c = node.config; if (node.type === "source_file" && !c.path) error(node.name, "path", "Source File requires a 'path'"); if (node.type === "source_db" && (!c.query || (!c.conn_id && !c.uri))) error(node.name, "config", "Source DB requires query and conn_id or uri"); if (node.type === "source_api" && !c.url) error(node.name, "url", "Source API requires a 'url'"); if (node.type === "sink_file" && !c.path) error(node.name, "path", "Sink File requires a 'path'"); if (node.type === "sink_db" && (!c.table || (!c.conn_id && !c.uri))) error(node.name, "config", "Sink DB requires table and conn_id or uri"); if (node.type === "quality_check" && !c.rules) error(node.name, "rules", "Quality Check requires at least one rule"); if (node.type === "condition" && !c.expression) error(node.name, "expression", "Condition node requires an 'expression'"); if (node.type === "join" && indegree.get(node.id) !== 2) error(node.name, "inputs", `join requires exactly 2 inputs, got ${indegree.get(node.id)}`); if (node.type === "condition" && indegree.get(node.id) !== 1) error(node.name, "inputs", `condition requires exactly 1 input, got ${indegree.get(node.id)}`); } if (!data.nodes.some(n => n.capabilities.includes("source"))) warning("", "capabilities", "Pipeline has no source node"); return { errors, warnings, valid: errors.length === 0 }; }
export function diffIR(local: PipelineIR, remote?: PipelineIR): string { const a = remote ? renderIR(remote).split("\n") : [], b = renderIR(local).split("\n"); const lines = [`--- server`, `+++ local`]; const max = Math.max(a.length, b.length); for (let i = 0; i < max; i++) { if (a[i] === b[i]) lines.push(` ${b[i]}`); else { if (a[i] !== undefined) lines.push(`-${a[i]}`); if (b[i] !== undefined) lines.push(`+${b[i]}`); } } return `${lines.join("\n")}\n`; }

export const TERMINAL_RUN_STATUSES = new Set(["success", "succeeded", "failed", "cancelled", "canceled"]);
export class APIError extends Error { constructor(message: string, readonly status: number, readonly body?: unknown) { super(message); } }
export type ClientOptions = { apiKey?: string; username?: string; password?: string; fetch?: typeof globalThis.fetch };
export class Run {
  constructor(readonly client: Client, readonly id: string) {}
  status() { return this.client.request(`/api/runs/${encodeURIComponent(this.id)}`); }
  logs() { return this.client.request(`/api/runs/${encodeURIComponent(this.id)}/logs`); }
  cancel() { return this.client.request(`/api/runs/${encodeURIComponent(this.id)}/cancel`, { method: "POST" }); }
  async wait(options: { timeout?: number; pollInterval?: number; raiseOnFailure?: boolean } = {}) { const deadline = Date.now() + (options.timeout ?? 3600) * 1000; let detail: any = {}; while (Date.now() < deadline) { detail = await this.status(); if (TERMINAL_RUN_STATUSES.has(detail.status)) { if (options.raiseOnFailure && ["failed"].includes(detail.status)) throw new APIError(`Run ${this.id} failed`, 0, detail); return detail; } await new Promise(resolve => setTimeout(resolve, (options.pollInterval ?? 1) * 1000)); } throw new Error(`Timed out waiting for run ${this.id} (last status: ${detail.status || "unknown"})`); }
}
export class Client {
  private token?: string;
  private readonly doFetch: typeof globalThis.fetch;
  constructor(readonly baseUrl: string, private readonly options: ClientOptions = {}) { if (options.apiKey && (options.username || options.password)) throw new TypeError("apiKey and username/password are mutually exclusive"); this.doFetch = options.fetch || fetch; }
  private async authenticate() { if (this.options.apiKey) { this.token = this.options.apiKey; return; } if (!this.options.username || !this.options.password) throw new Error("Provide apiKey or username/password"); const response = await this.doFetch(`${this.baseUrl}/api/auth/login`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ username: this.options.username, password: this.options.password }) }); if (!response.ok) throw new APIError("Authentication failed", response.status, await response.text()); const body = await response.json() as { token?: string }; if (!body.token) throw new APIError("Authentication response did not contain a token", response.status, body); this.token = body.token; }
  async request(path: string, init: RequestInit = {}, retry = true): Promise<any> { if (!this.token) await this.authenticate(); const headers = new Headers(init.headers); headers.set("accept", "application/json"); headers.set("content-type", "application/json"); headers.set("authorization", `Bearer ${this.token}`); const response = await this.doFetch(`${this.baseUrl.replace(/\/$/, "")}${path}`, { ...init, headers }); if (response.status === 401 && retry && !this.options.apiKey) { this.token = undefined; return this.request(path, init, false); } const text = await response.text(); let body: any = undefined; try { body = text ? JSON.parse(text) : undefined; } catch { body = text; } if (!response.ok) throw new APIError(`Brokoli API request failed: ${response.status}`, response.status, body); return body; }
  pipelines() { return this.request("/api/pipelines").then(body => Array.isArray(body) ? body : body.items || []); }
  async pipeline(identifier: string) { const items = await this.pipelines(); const matches = items.filter((item: any) => item.id === identifier || item.pipeline_id === identifier || item.name === identifier); if (matches.length !== 1) throw new APIError(matches.length ? `Pipeline identifier is ambiguous: ${identifier}` : `Pipeline not found: ${identifier}`, matches.length ? 409 : 404); return matches[0]; }
  async deploy(pipeline: Pipeline) { const existing = await this.pipelines(); const match = existing.find((item: any) => item.pipeline_id === pipeline.pipelineId); const body = pipeline.toJSON(); return match ? this.request(`/api/pipelines/${encodeURIComponent(match.id)}`, { method: "PUT", body: JSON.stringify({ ...body, id: match.id }) }) : this.request("/api/pipelines", { method: "POST", body: JSON.stringify(body) }); }
  async run(identifier: string, params?: Record<string, string>) { const target = await this.pipeline(identifier); const body = params ? { params } : undefined; const response = await this.request(`/api/pipelines/${encodeURIComponent(target.id)}/run`, { method: "POST", ...(body ? { body: JSON.stringify(body) } : {}) }); const id = response.run_id || response.id || response.run?.id; if (!id) throw new APIError("Trigger response did not contain a run ID", 0, response); return new Run(this, id); }
}

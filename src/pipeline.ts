/**
 * Pipeline authoring: the builder, node references, and the factory
 * catalog. Compiles to the IR in ./ir; parity with the Python SDK's
 * emissions is enforced by the differential suite.
 */

import { PipelineError } from "./errors";
import {
  filterScript,
  helpersPreamble,
  mapScript,
  sensorScript,
  sinkScript,
  sourceScript,
  taskScript,
  validateScript,
} from "./code";
import type {
  Helpers,
  RowMapper,
  RowPredicate,
  SensorFunction,
  SinkFunction,
  SourceFunction,
  TaskFunction,
  ValidateFunction,
} from "./code";
import type { Capability, Config, Edge, IRNode, PipelineIR } from "./ir";
import { NODE_TYPE_CAPABILITIES, irDigest, renderIR } from "./ir";
import { PaginationStrategy } from "./pagination";
import type { Connection } from "./resources";

/** Node id base: lowercase, [a-z0-9_] only, max 20 chars, "node" fallback
 * — must match the Python SDK's allocator for cross-SDK id parity. */
// Mirrors the Python SDK's _make_id exactly: lowercase, spaces become
// underscores, every remaining non-alphanumeric is dropped (Python's
// str.isalnum is Unicode-aware, hence \p{L}\p{N}), truncate to 20 code
// points. "Read A" -> "read_a", not "reada".
function cleanName(name: string): string {
  const kept = Array.from(name.toLowerCase().replaceAll(" ", "_"))
    .filter((ch) => /[\p{L}\p{N}_]/u.test(ch))
    .slice(0, 20)
    .join("");
  return kept || "node";
}

/** Derive a display name from a function's own declared name, mirroring
 * the Python SDK's `func.__name__.replace("_", " ").title()` for the
 * fn.name-only overloads of task()/map() (ADR-034 item 4). Only function
 * declarations and named function expressions qualify: fn.name is
 * unreliable for arrow functions and is destroyed by bundler
 * minification either way, so anything else must still pass an explicit
 * name — this never guesses. */
function deriveNodeName(fn: Function): string {
  const source = Function.prototype.toString.call(fn).trim();
  if (!/^(?:async\s+)?function\b/.test(source) || !fn.name) {
    throw new PipelineError(
      "task(fn)/map(fn) can only derive a name from a function declaration or named function expression; " +
        "pass an explicit name for arrow functions, anonymous functions, or minified builds.",
    );
  }
  const words = fn.name
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2")
    .split(/[\s_]+/)
    .filter(Boolean);
  return words.map((w) => w[0].toUpperCase() + w.slice(1).toLowerCase()).join(" ");
}

// Mirrors the Python SDK's _generate_pipeline_id: lowercase, spaces to
// hyphens, strip everything outside [a-z0-9-], collapse hyphen runs,
// trim leading/trailing hyphens.
function slugify(name: string): string {
  return name
    .toLowerCase()
    .replaceAll(" ", "-")
    .replace(/[^a-z0-9-]/g, "")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

/** Unwrap ResourceRef/InterpolationRef values anywhere in a config tree. */
function wireValue(value: unknown): unknown {
  if (value && typeof value === "object" && "irValue" in value && typeof (value as { irValue: unknown }).irValue === "function") {
    return (value as { irValue: () => unknown }).irValue();
  }
  if (Array.isArray(value)) return value.map(wireValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, child]) => [key, wireValue(child)]),
    );
  }
  return value;
}

/** Build a config object, dropping null/undefined values — the SDK never
 * emits a key it has nothing to say about (fail-closed decoders reject
 * unknowns, and absent beats null everywhere in the IR). */
function buildConfig(entries: Record<string, unknown>): Config {
  const config: Config = {};
  for (const [key, value] of Object.entries(entries)) {
    if (value !== undefined && value !== null) config[key] = wireValue(value);
  }
  return config;
}

export type TaskOptions = { retries?: number; retryBackoff?: string; timeout?: number; maxMemoryMb?: number; maxCpuSeconds?: number; nodeKey?: string; helpers?: Helpers };
export type MapOptions = { columns?: string[]; nodeKey?: string; helpers?: Helpers };

export class NodeRef {
  constructor(
    readonly nodeId: string,
    readonly pipeline: Pipeline,
    readonly kind: string = "node",
  ) {}
  /** Draw an edge to `target` and return it, enabling a.then(b).then(c). */
  then<T extends NodeRef>(target: T): T {
    this.pipeline.edge(this, target);
    return target;
  }
  connect<T extends NodeRef>(target: T): T {
    return this.then(target);
  }
  fanOut(...targets: NodeRef[]): NodeRef[] {
    for (const target of targets) this.pipeline.edge(this, target);
    return targets;
  }
}

export class DatasetRef extends NodeRef {}
export class ScalarRef extends NodeRef {}
export class ArtifactRef extends NodeRef {}
export class CollectionRef extends NodeRef {
  collect(name = `${this.nodeId} (Collected)`): DatasetRef {
    return this.pipeline.union(name, this);
  }
}

export class ConditionRef extends NodeRef {
  /** The true branch. */
  when<T extends NodeRef>(target: T): T {
    this.pipeline.branch(this, target, true);
    return target;
  }
  /** The false branch. */
  otherwise<T extends NodeRef>(target: T): T {
    this.pipeline.branch(this, target, false);
    return target;
  }
}

const REF_KINDS: Record<string, new (id: string, p: Pipeline, kind: string) => NodeRef> = {
  node: NodeRef,
  dataset: DatasetRef,
  scalar: ScalarRef,
  artifact: ArtifactRef,
  condition: ConditionRef,
  collection: CollectionRef,
};

const HOOK_TYPES = ["webhook", "slack", "email"] as const;
const HOOK_NAMES = ["on_start", "on_success", "on_failure"] as const;

export type Hook = {
  type: (typeof HOOK_TYPES)[number];
  url: string;
  enabled: boolean;
  extra?: Record<string, unknown>;
};

/** What authors may pass: a bare URL (becomes an enabled webhook, as in
 * the Python SDK's _coerce_hook) or a partial hook object. */
export type HookInput = string | { type?: string; url: string; enabled?: boolean; extra?: Record<string, unknown> };

function coerceHook(name: string, value: HookInput): Hook {
  if (typeof value === "string") {
    return { type: "webhook", url: value, enabled: true };
  }
  const type = value.type ?? "webhook";
  if (!(HOOK_TYPES as readonly string[]).includes(type) || !value.url) {
    throw new PipelineError(`hooks.${name} needs a url and a type in ${HOOK_TYPES.join("/")}`);
  }
  const out: Hook = { type: type as Hook["type"], url: value.url, enabled: value.enabled ?? true };
  if (value.extra && Object.keys(value.extra).length) out.extra = value.extra;
  return out;
}

export type PipelineOptions = {
  pipelineId?: string;
  description?: string;
  schedule?: string;
  scheduleTimezone?: string;
  /** Replay missed schedule intervals after downtime (server v0.10.78+;
   * deploy preflight requires the data_intervals feature). */
  catchUp?: boolean;
  tags?: string[];
  dependsOn?: string[];
  /** "HH:MM" or "HH:MM Zone" — must complete by this time daily. */
  sla?: string;
  webhook?: boolean;
  hooks?: Partial<Record<(typeof HOOK_NAMES)[number], HookInput>>;
};

export class Pipeline {
  readonly pipelineId: string;
  readonly description: string;
  readonly schedule: string;
  readonly scheduleTimezone: string;
  readonly catchUp: boolean;
  readonly tags: string[];
  readonly dependsOn: string[];
  readonly sla: string;
  readonly webhook: boolean;
  readonly hooks?: Record<string, Hook>;
  readonly nodes: IRNode[] = [];
  readonly edges: Edge[] = [];
  private counters = new Map<string, number>();

  constructor(
    readonly name: string,
    options: PipelineOptions = {},
  ) {
    this.pipelineId = options.pipelineId || slugify(name);
    this.description = options.description || "";
    this.schedule = options.schedule || "";
    this.scheduleTimezone = options.scheduleTimezone || "";
    this.catchUp = options.catchUp || false;
    this.tags = options.tags ? [...options.tags] : [];
    this.dependsOn = options.dependsOn ? [...options.dependsOn] : [];
    this.sla = options.sla || "";
    this.webhook = options.webhook || false;
    if (options.hooks) {
      const hooks: Record<string, Hook> = {};
      for (const [hookName, value] of Object.entries(options.hooks)) {
        if (!(HOOK_NAMES as readonly string[]).includes(hookName)) {
          throw new PipelineError(`Unknown hook ${hookName}; supported: ${HOOK_NAMES.join(", ")}`);
        }
        if (value !== undefined) hooks[hookName] = coerceHook(hookName, value);
      }
      if (Object.keys(hooks).length) this.hooks = hooks;
    }
    if (this.catchUp && !this.schedule) {
      throw new PipelineError(
        "catchUp needs a schedule: catch-up replays missed schedule intervals, and without a schedule there is no interval grid to replay",
      );
    }
  }

  private allocateId(name: string, nodeKey?: string): string {
    if (nodeKey) {
      if (!/^[a-z][a-z0-9_-]{0,63}$/.test(nodeKey) || this.nodes.some((n) => n.id === nodeKey)) {
        throw new PipelineError(`Invalid or duplicate node_key ${nodeKey}`);
      }
      return nodeKey;
    }
    const base = cleanName(name);
    let n = this.counters.get(base) || 0;
    let id: string;
    do {
      n++;
      id = `${base}_${n}`;
    } while (this.nodes.some((node) => node.id === id));
    this.counters.set(base, n);
    return id;
  }

  private assertOwn(ref: NodeRef): void {
    if (ref.pipeline !== this || !this.nodes.some((n) => n.id === ref.nodeId)) {
      throw new PipelineError("Node belongs to another pipeline");
    }
  }

  /** Low-level node registration; the factories below are the API. */
  register<T extends NodeRef = NodeRef>(
    type: string,
    name: string,
    config: Config,
    inputs: NodeRef[],
    options: { nodeKey?: string; kind?: string; capabilities?: Capability[] } = {},
  ): T {
    for (const input of inputs) this.assertOwn(input);
    const id = this.allocateId(name, options.nodeKey);
    this.nodes.push({
      id,
      type,
      name,
      config: structuredClone(config),
      capabilities: [...(options.capabilities || NODE_TYPE_CAPABILITIES[type] || (["compute"] as Capability[]))],
    });
    const Ref = REF_KINDS[options.kind || "node"] || NodeRef;
    const ref = new Ref(id, this, options.kind || "node") as T;
    for (const input of inputs) this.edge(input, ref);
    return ref;
  }

  edge(from: NodeRef, to: NodeRef, condition?: boolean): void {
    this.assertOwn(from);
    this.assertOwn(to);
    const existing = this.edges.find((e) => e.from === from.nodeId && e.to === to.nodeId);
    if (existing) {
      if (existing.condition !== condition) throw new PipelineError("Edge cannot belong to multiple branches");
      return;
    }
    this.edges.push({ from: from.nodeId, to: to.nodeId, ...(condition === undefined ? {} : { condition }) });
  }

  branch(from: ConditionRef, to: NodeRef, condition: boolean): void {
    if (this.edges.filter((e) => e.to === from.nodeId).length !== 1) {
      throw new PipelineError("Condition must have exactly one input");
    }
    // The chain footgun (brokoli-sdk#81, same shape here): then() returns
    // its target, so when(a.then(b)) hands us b -- the branch edge would
    // land on the chain's tail and leave a with no input, silently. A
    // branch target that already has inputs is never what the author
    // meant, so refuse anything fed by nodes other than this gate.
    const fedFrom = [...new Set(this.edges.filter((e) => e.to === to.nodeId && e.from !== from.nodeId).map((e) => e.from))].sort();
    if (fedFrom.length) {
      throw new PipelineError(
        `Condition branch target ${to.nodeId} already has input(s) from ${fedFrom.join(", ")}. ` +
          "If you passed a then() chain to when()/otherwise(), note that then() returns its target, " +
          "so the branch would route to the chain's tail and leave its head unconnected. " +
          "Route to the branch's entry node and chain from it instead: " +
          "gate.when(shaped); shaped.then(sink).",
      );
    }
    this.edge(from, to, condition);
  }

  // ── Node factories ────────────────────────────────────────────────

  sourceFile(name: string, options: { path?: string; format?: string; nodeKey?: string } = {}): DatasetRef {
    return this.register("source_file", name, buildConfig({ path: options.path || "", format: options.format || "csv" }), [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  sourceDb(
    name: string,
    options: { query?: string; connId?: string | Connection; uri?: string; retries?: number; retryBackoff?: string; retryDelay?: number; timeout?: number; nodeKey?: string } = {},
  ): DatasetRef {
    const config = buildConfig({
      query: options.query || "",
      conn_id: options.connId,
      uri: options.uri,
      max_retries: options.retries,
      retry_backoff: options.retries === undefined ? undefined : options.retryBackoff || "exponential",
      retry_delay: options.retryDelay,
      timeout: options.timeout,
    });
    // Parity anchor: the reference SDK annotates source schema hints,
    // and they survive normalization into the digest.
    if (config.query) config._schema_hint = "query_result";
    return this.register("source_db", name, config, [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  sourceApi(
    name: string,
    options: {
      url?: string; method?: string; headers?: Config; body?: unknown; connId?: string | Connection;
      params?: Config; response?: "dataset" | "scalar" | "artifact"; records?: string; valuePath?: string;
      pagination?: Config | PaginationStrategy; retries?: number; retryBackoff?: string; retryDelay?: number;
      timeout?: number; nodeKey?: string;
    } = {},
  ): NodeRef {
    const response = options.response || "dataset";
    const pagination = options.pagination instanceof PaginationStrategy ? options.pagination.toConfig() : options.pagination;
    const execution = options.pagination instanceof PaginationStrategy ? options.pagination.executionConfig() : undefined;
    const config = buildConfig({
      url: options.url || "",
      method: options.method || "GET",
      response,
      headers: options.headers,
      body: options.body,
      conn_id: options.connId,
      params: options.params,
      records: options.records,
      value_path: options.valuePath,
      pagination,
      execution,
      max_retries: options.retries,
      retry_backoff: options.retries === undefined ? undefined : options.retryBackoff || "exponential",
      retry_delay: options.retryDelay,
      timeout: options.timeout,
    });
    config._schema_hint = "api_response";
    return this.register("source_api", name, config, [], { nodeKey: options.nodeKey, kind: response });
  }

  transform(name: string, input?: NodeRef, options: { rules?: unknown[]; nodeKey?: string } = {}): DatasetRef {
    return this.register("transform", name, options.rules?.length ? { rules: structuredClone(options.rules) } : {}, input ? [input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  join(name: string, left?: NodeRef, right?: NodeRef, options: { on?: string; leftKey?: string; rightKey?: string; how?: string; nodeKey?: string } = {}): DatasetRef {
    const leftKey = options.leftKey || options.on || "";
    const rightKey = options.rightKey || leftKey;
    return this.register("join", name, { join_type: options.how || "inner", left_key: leftKey, right_key: rightKey }, [left, right].filter((r): r is NodeRef => !!r), { nodeKey: options.nodeKey, kind: "dataset" });
  }

  qualityCheck(name: string, input?: NodeRef, options: { rules?: unknown[]; nodeKey?: string } = {}): NodeRef {
    return this.register("quality_check", name, options.rules?.length ? { rules: structuredClone(options.rules) } : {}, input ? [input] : [], { nodeKey: options.nodeKey });
  }

  sinkFile(name: string, input?: NodeRef, options: { path?: string; format?: string; compress?: string; retries?: number; retryBackoff?: string; retryDelay?: number; timeout?: number; nodeKey?: string } = {}): NodeRef {
    return this.register("sink_file", name, buildConfig({
      path: options.path || "",
      format: options.format || "csv",
      compress: options.compress,
      max_retries: options.retries,
      retry_backoff: options.retries === undefined ? undefined : options.retryBackoff || "exponential",
      retry_delay: options.retryDelay,
      timeout: options.timeout,
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  sinkDb(name: string, input?: NodeRef, options: { table?: string; mode?: string; connId?: string; uri?: string; keyColumns?: string[]; truncate?: boolean; nodeKey?: string } = {}): NodeRef {
    return this.register("sink_db", name, buildConfig({
      table: options.table || "",
      mode: options.mode || "append",
      conn_id: options.connId,
      uri: options.uri,
      key_columns: options.keyColumns,
      truncate: options.truncate,
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  sinkApi(name: string, input?: NodeRef, options: { url?: string; method?: string; body?: unknown; headers?: Config; connId?: string; nodeKey?: string } = {}): NodeRef {
    return this.register("sink_api", name, buildConfig({
      url: options.url || "",
      method: options.method || "POST",
      body_template: options.body,
      headers: options.headers,
      conn_id: options.connId,
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  migrate(name: string, options: { sourceUri?: string; targetUri?: string; query?: string; table?: string; mode?: string; nodeKey?: string } = {}): DatasetRef {
    return this.register("migrate", name, {
      source_uri: options.sourceUri || "",
      dest_uri: options.targetUri || "",
      source_query: options.query || "",
      dest_table: options.table || "",
      mode: options.mode || "append",
    }, [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  dbt(name: string, options: { command?: string; projectDir?: string; target?: string; select?: string; profilesDir?: string; vars?: Config; nodeKey?: string } = {}): DatasetRef {
    return this.register("dbt", name, buildConfig({
      command: options.command || "run",
      project_dir: options.projectDir,
      target: options.target,
      select: options.select,
      profiles_dir: options.profilesDir,
      vars: options.vars,
    }), [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  notify(name: string, input?: NodeRef, options: { notifyType?: string; webhookUrl?: string; message?: string; channel?: string; nodeKey?: string } = {}): NodeRef {
    return this.register("notify", name, buildConfig({
      notify_type: options.notifyType || "webhook",
      webhook_url: options.webhookUrl || "",
      message: options.message,
      channel: options.channel,
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  conditionNode(name: string, input: NodeRef | undefined, expression: string, nodeKey?: string): ConditionRef {
    return this.register("condition", name, { expression }, input ? [input] : [], { nodeKey, kind: "condition" });
  }

  /** Deferrable wait (server v0.10.79+): parks the run, holding no
   * worker slot, until the declarative condition fires. */
  wait(
    name: string,
    options: { condition?: "file_exists" | "http" | "interval_elapsed" | "pipeline"; path?: string; url?: string; expectStatus?: number; offset?: string; pipelineId?: string; pollInterval?: string; timeout?: string; nodeKey?: string } = {},
  ): NodeRef {
    return this.register("wait", name, buildConfig({
      condition: options.condition,
      path: options.path,
      url: options.url,
      expect_status: options.expectStatus,
      offset: options.offset,
      pipeline_id: options.pipelineId,
      poll_interval: options.pollInterval,
      timeout: options.timeout,
    }), [], { nodeKey: options.nodeKey });
  }

  code(name: string, input?: NodeRef, options: { language?: "python" | "typescript"; script?: string; helpers?: Helpers; pythonPath?: string; nodePath?: string; retries?: number; retryBackoff?: string; timeout?: number; maxMemoryMb?: number; maxCpuSeconds?: number; nodeKey?: string } = {}): NodeRef {
    return this.register("code", name, buildConfig({
      language: options.language || "python",
      script: `${helpersPreamble(options.helpers)}${options.script || ""}`,
      python_path: options.pythonPath,
      node_path: options.nodePath,
      max_retries: options.retries,
      retry_backoff: options.retries === undefined ? undefined : options.retryBackoff || "exponential",
      timeout: options.timeout,
      max_memory_mb: options.maxMemoryMb,
      max_cpu_seconds: options.maxCpuSeconds,
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  /** Serialize a self-contained function for the TypeScript code-node
   * runtime. Closure/import packaging is intentionally not supported in v1. */
  task(fn: TaskFunction, input?: NodeRef, options?: TaskOptions): DatasetRef;
  task(name: string, input: NodeRef | undefined, fn: TaskFunction, options?: TaskOptions): DatasetRef;
  task(
    nameOrFn: string | TaskFunction,
    inputOrFn?: NodeRef | TaskFunction,
    fnOrOptions?: TaskFunction | TaskOptions,
    maybeOptions: TaskOptions = {},
  ): DatasetRef {
    let name: string;
    let input: NodeRef | undefined;
    let fn: TaskFunction;
    let options: TaskOptions;
    if (typeof nameOrFn === "function") {
      fn = nameOrFn;
      input = inputOrFn as NodeRef | undefined;
      options = (fnOrOptions as TaskOptions) ?? {};
      name = deriveNodeName(fn);
    } else {
      name = nameOrFn;
      input = inputOrFn as NodeRef | undefined;
      fn = fnOrOptions as TaskFunction;
      options = maybeOptions;
    }
    return this.register("code", name, buildConfig({
      language: "typescript",
      script: taskScript(fn, options.helpers),
      max_retries: options.retries,
      retry_backoff: options.retries === undefined ? undefined : options.retryBackoff || "exponential",
      timeout: options.timeout,
      max_memory_mb: options.maxMemoryMb,
      max_cpu_seconds: options.maxCpuSeconds,
    }), input ? [input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  source(name: string, fn: SourceFunction, options: { retries?: number; timeout?: number; nodeKey?: string; helpers?: Helpers } = {}): DatasetRef {
    return this.register("code", name, buildConfig({ language: "typescript", script: sourceScript(fn, options.helpers), max_retries: options.retries, timeout: options.timeout }), [], {
      nodeKey: options.nodeKey,
      kind: "dataset",
      capabilities: ["source", "dataset-output"],
    });
  }

  sink(name: string, input: NodeRef | undefined, fn: SinkFunction, options: { retries?: number; timeout?: number; nodeKey?: string; helpers?: Helpers } = {}): NodeRef {
    return this.register("code", name, buildConfig({ language: "typescript", script: sinkScript(fn, options.helpers), max_retries: options.retries, timeout: options.timeout }), input ? [input] : [], {
      nodeKey: options.nodeKey,
      capabilities: ["sink"],
    });
  }

  filter(name: string, input: NodeRef | undefined, fn: RowPredicate, options: { nodeKey?: string; helpers?: Helpers } = {}): DatasetRef {
    return this.register("code", name, { language: "typescript", script: filterScript(fn, options.helpers) }, input ? [input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  map(fn: RowMapper, input?: NodeRef, options?: MapOptions): DatasetRef;
  map(name: string, input: NodeRef | undefined, fn: RowMapper, options?: MapOptions): DatasetRef;
  map(
    nameOrFn: string | RowMapper,
    inputOrFn?: NodeRef | RowMapper,
    fnOrOptions?: RowMapper | MapOptions,
    maybeOptions: MapOptions = {},
  ): DatasetRef {
    let name: string;
    let input: NodeRef | undefined;
    let fn: RowMapper;
    let options: MapOptions;
    if (typeof nameOrFn === "function") {
      fn = nameOrFn;
      input = inputOrFn as NodeRef | undefined;
      options = (fnOrOptions as MapOptions) ?? {};
      name = deriveNodeName(fn);
    } else {
      name = nameOrFn;
      input = inputOrFn as NodeRef | undefined;
      fn = fnOrOptions as RowMapper;
      options = maybeOptions;
    }
    return this.register("code", name, { language: "typescript", script: mapScript(fn, options.columns, options.helpers) }, input ? [input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  validate(name: string, input: NodeRef | undefined, fn: ValidateFunction, options: { onFailure?: "block" | "warn"; nodeKey?: string; helpers?: Helpers } = {}): NodeRef {
    return this.register("code", name, { language: "typescript", script: validateScript(fn, options.onFailure || "block", options.helpers) }, input ? [input] : [], { nodeKey: options.nodeKey });
  }

  sensor(name: string, fn: SensorFunction, options: { pollInterval?: number; timeout?: number; nodeKey?: string; helpers?: Helpers } = {}): NodeRef {
    return this.register("code", name, buildConfig({ language: "typescript", script: sensorScript(fn, options.pollInterval ?? 60, options.helpers), timeout: options.timeout ?? 3600 }), [], { nodeKey: options.nodeKey });
  }

  union(name: string, ...refs: NodeRef[]): DatasetRef {
    return this.register("union", name, { mode: "union" }, refs, { kind: "dataset" });
  }

  parallel(...nodes: NodeRef[]): NodeRef[] {
    for (const node of nodes) this.assertOwn(node);
    return nodes;
  }

  // ── Compilation ───────────────────────────────────────────────────

  /** Compile to IR. Key emission follows the parity anchors in the
   * canonicalization spec: optional top-level fields appear only when
   * set, and catchup only ever as `true`. */
  toJSON(): PipelineIR {
    const ir: PipelineIR = {
      pipeline_id: this.pipelineId,
      name: this.name,
      description: this.description,
      schedule: this.schedule,
      enabled: true,
      ir_version: this.edges.some((e) => e.condition !== undefined) ? "2.1" : "2.0",
      nodes: structuredClone(this.nodes),
      edges: structuredClone(this.edges),
      tags: [...this.tags],
      depends_on: [...this.dependsOn],
    };
    if (this.scheduleTimezone) ir.schedule_timezone = this.scheduleTimezone;
    if (this.catchUp) ir.catchup = true;
    if (this.sla) {
      const separator = this.sla.indexOf(" ");
      ir.sla_deadline = separator < 0 ? this.sla : this.sla.slice(0, separator);
      ir.sla_timezone = separator < 0 ? "UTC" : this.sla.slice(separator + 1);
    }
    if (this.hooks && Object.keys(this.hooks).length) ir.hooks = structuredClone(this.hooks);
    if (this.webhook) ir.webhook_token = "";
    return ir;
  }

  compile(): PipelineIR {
    return this.toJSON();
  }

  render(): string {
    return renderIR(this.toJSON());
  }

  digest(): string {
    return irDigest(this.toJSON());
  }
}

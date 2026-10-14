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
import { joinDatasetSchema, projectDatasetSchema } from "./schema";
import type { BptdType, DatasetSchema, ParameterDeclaration, TaskInterface } from "./schema";
import { buildTaskInterface } from "./schema";
import type { Expression } from "./expression";

/** The engine's generic per-node retry and timeout knobs. `retryDelay`
 * is in milliseconds and `timeout` in seconds, as the editor labels them. */
export type RetryOptions = {
  retries?: number;
  retryBackoff?: string;
  retryDelay?: number;
  timeout?: number;
};

/** `pool` puts a node in a named concurrency pool on the server; without
 * it a node joins the pool of its `connId`. */
export type PoolOption = { pool?: string };

function retryConfig(options: RetryOptions): Config {
  return {
    max_retries: options.retries,
    retry_backoff: options.retries === undefined ? undefined : options.retryBackoff || "exponential",
    retry_delay: options.retryDelay,
    timeout: options.timeout,
  };
}

export type BashOptions = RetryOptions & PoolOption & {
  command?: string;
  workingDir?: string;
  env?: Record<string, string>;
  maxMemoryMb?: number;
  maxCpuSeconds?: number;
  nodeKey?: string;
};

/** A migrate partition: ranges of one column copied in parallel, each
 * retried on its own. Dates are RFC3339 strings or Date objects. */
export type MigratePartition = {
  column: string;
  strategy: "numeric" | "date";
  boundaries: Array<number | string | Date>;
  maxParallel?: number;
};

export type WaitCondition = "file_exists" | "http" | "interval_elapsed" | "pipeline";

/** A wait duration: a Go duration string such as "90s" or "2h30m", or a
 * number of seconds. */
export type Duration = string | number;

function goDuration(value: Duration | undefined): string | undefined {
  if (value === undefined || typeof value === "string") return value;
  return Number.isInteger(value) ? `${value}s` : `${Math.round(value * 1000)}ms`;
}

/** An actually-fine contract: its IR, or an object that produces it
 * (actually-fine's TypeScript `Contract`). */
export type ContractInput = Record<string, unknown> | { toIR(): unknown };

function contractIR(contract: ContractInput | undefined): unknown {
  if (contract && typeof (contract as { toIR?: unknown }).toIR === "function") {
    return (contract as { toIR(): unknown }).toIR();
  }
  return contract === undefined ? undefined : structuredClone(contract);
}

/** JSON with object keys sorted at every level and no whitespace: the
 * same string the Python SDK's json.dumps(sort_keys=True,
 * separators=(",", ":")) produces for the same value. */
function stableJSON(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJSON).join(",")}]`;
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, child]) => child !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([key, child]) => `${JSON.stringify(key)}:${stableJSON(child)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

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

export type TaskOptions = {
  retries?: number;
  retryBackoff?: string;
  timeout?: number;
  maxMemoryMb?: number;
  maxCpuSeconds?: number;
  nodeKey?: string;
  helpers?: Helpers;
  /** ADR-032 rollout step 3: this task's input/output row schema,
   * built with `schema.record(...)` -- explicit, since TypeScript
   * cannot recover erased generic types at runtime. Omit either side
   * to leave it unknown; omit both for no interface at all. */
  input?: BptdType;
  output?: BptdType;
  /** ADR-032 rollout step 3: typed pipeline parameters this task reads,
   * built with `parameter.number(...)`/`parameter.string(...)`/etc.
   * Merged onto the owning Pipeline -- two task() calls sharing a name
   * must declare it identically, or Pipeline throws naming the
   * conflict. */
  parameters?: Record<string, ParameterDeclaration>;
};
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
  /** ADR-032 rollout step 3: pipeline-level parameter declarations from
   * task()'s `parameters` option, keyed by name. Distinct from any
   * node's own interface -- this is what toJSON() serializes as the
   * pipeline's top-level `parameters`. */
  readonly parameters: Record<string, ParameterDeclaration> = {};
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

  /** Register a pipeline-level parameter (ADR-032 rollout step 3), or
   * confirm an identical re-declaration from a second task() reusing the
   * same name. A same-name/different-declaration collision across two
   * tasks is almost certainly two unrelated concepts sharing a name by
   * accident -- raise rather than silently keep whichever came first. */
  private mergeParameter(name: string, declaration: ParameterDeclaration): void {
    const existing = this.parameters[name];
    if (existing !== undefined && JSON.stringify(existing) !== JSON.stringify(declaration)) {
      throw new PipelineError(
        `Parameter ${name} was declared with two different declarations ` +
          `(${JSON.stringify(existing)} vs ${JSON.stringify(declaration)}) from different task() calls. ` +
          "Give the parameters different names.",
      );
    }
    this.parameters[name] = declaration;
  }

  /** Low-level node registration; the factories below are the API. */
  register<T extends NodeRef = NodeRef>(
    type: string,
    name: string,
    config: Config,
    inputs: NodeRef[],
    options: { nodeKey?: string; kind?: string; capabilities?: Capability[]; taskInterface?: TaskInterface } = {},
  ): T {
    for (const input of inputs) this.assertOwn(input);
    const id = this.allocateId(name, options.nodeKey);
    this.nodes.push({
      id,
      type,
      name,
      config: structuredClone(config),
      capabilities: [...(options.capabilities || NODE_TYPE_CAPABILITIES[type] || (["compute"] as Capability[]))],
      ...(options.taskInterface ? { interface: structuredClone(options.taskInterface) } : {}),
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

  sourceFile(name: string, options: RetryOptions & PoolOption & { path?: string; format?: string; schema?: DatasetSchema; connId?: string | Connection; nodeKey?: string } = {}): DatasetRef {
    return this.register("source_file", name, buildConfig({
      path: options.path || "",
      format: options.format || "csv",
      schema: options.schema,
      conn_id: options.connId,
      ...retryConfig(options),
      pool: options.pool,
    }), [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  sourceDb(
    name: string,
    options: RetryOptions & PoolOption & { query?: string; connId?: string | Connection; uri?: string; schema?: DatasetSchema; nodeKey?: string } = {},
  ): DatasetRef {
    const config = buildConfig({
      query: options.query || "",
      conn_id: options.connId,
      uri: options.uri,
      ...retryConfig(options),
      schema: options.schema,
      pool: options.pool,
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
      pagination?: Config | PaginationStrategy; profile?: Config; executionProfile?: Config; retries?: number; retryBackoff?: string; retryDelay?: number;
      timeout?: number; schema?: DatasetSchema; nodeKey?: string;
      /** The fetcher: "rest" (default) or another the server registers. */
      sourceType?: string;
      pool?: string;
    } = {},
  ): NodeRef {
    const response = options.response || "dataset";
    if (options.schema && response !== "dataset") {
      throw new PipelineError("sourceApi schema is only valid when response='dataset'");
    }
    const pagination = options.pagination instanceof PaginationStrategy ? options.pagination.toConfig() : options.pagination;
    const execution = options.pagination instanceof PaginationStrategy ? options.pagination.executionConfig() : undefined;
    if (options.profile && options.executionProfile) throw new PipelineError("sourceApi accepts only one of profile or executionProfile");
    const profile = options.executionProfile || options.profile ? { ...(options.executionProfile || options.profile) } : undefined;
    const profileExecution = profile ? { ...profile } : undefined;
    for (const key of ["timeout", "max_retries", "retry_backoff", "retry_delay"]) {
      if (profileExecution && key in profileExecution) delete profileExecution[key];
    }
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
      execution: profileExecution ? { ...profileExecution, ...execution } : execution,
      max_retries: options.retries,
      retry_backoff: options.retries === undefined ? undefined : options.retryBackoff || "exponential",
      retry_delay: options.retryDelay,
      timeout: options.timeout,
      schema: options.schema,
      source_type: options.sourceType,
      pool: options.pool,
    });
    if (profile) {
      for (const [sourceKey, configKey] of [["timeout", "timeout"], ["max_retries", "max_retries"], ["retry_backoff", "retry_backoff"], ["retry_delay", "retry_delay"]] as const) {
        if (profile[sourceKey] !== undefined && config[configKey] === undefined) config[configKey] = profile[sourceKey];
      }
    }
    config._schema_hint = "api_response";
    return this.register("source_api", name, config, [], { nodeKey: options.nodeKey, kind: response });
  }

  transform(name: string, input?: NodeRef, options: { rules?: unknown[]; nodeKey?: string } = {}): DatasetRef {
    return this.register("transform", name, options.rules?.length ? { rules: structuredClone(options.rules) } : {}, input ? [input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  project(name: string, input: NodeRef | undefined, columns: Record<string, Expression>, options: { nodeKey?: string } = {}): DatasetRef {
    const projections = Object.entries(columns).map(([column, expr]) => {
      if (!column || !expr || typeof expr !== "object" || !("op" in expr)) throw new PipelineError("project columns require named expressions");
      return { name: column, expr: structuredClone(expr) };
    });
    if (!projections.length) throw new PipelineError("project requires a non-empty columns object");
    const inputSchema = input ? this.nodes.find((node) => node.id === input.nodeId)?.config.schema as DatasetSchema | undefined : undefined;
    return this.register("project", name, {
      expression_version: 1,
      projections,
      schema: projectDatasetSchema(inputSchema, projections),
    }, input ? [input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  aggregate(name: string, input: NodeRef | undefined, options: {
    groupBy: string[];
    aggregations: { column: string; function: string; alias?: string }[];
    nodeKey?: string;
  }): DatasetRef {
    if (!options.groupBy.length) throw new PipelineError("aggregate requires a non-empty groupBy array");
    if (!options.aggregations.length) throw new PipelineError("aggregate requires a non-empty aggregations array");
    return this.register("aggregate", name, { expression_version: 1, group_by: [...options.groupBy], agg_fields: structuredClone(options.aggregations) }, input ? [input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  filterRows(name: string, input: NodeRef | undefined, predicate: Expression, options: { nodeKey?: string } = {}): DatasetRef {
    if (!predicate || typeof predicate !== "object" || !("op" in predicate)) throw new PipelineError("filter requires a predicate expression");
    return this.register("filter", name, { expression_version: 1, predicate: structuredClone(predicate) }, input ? [input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  join(name: string, left?: NodeRef, right?: NodeRef, options: {
    on?: string;
    leftKey?: string;
    rightKey?: string;
    how?: string;
    nodeKey?: string;
    collisionPolicy?: "error" | "prefix" | "alias";
    rightAlias?: string;
  } = {}): DatasetRef {
    const leftKey = options.leftKey || options.on || "";
    const rightKey = options.rightKey || leftKey;
    const collisionPolicy = options.collisionPolicy || "prefix";
    if (collisionPolicy !== "error" && collisionPolicy !== "prefix" && collisionPolicy !== "alias") {
      throw new PipelineError(`join collisionPolicy must be 'error', 'prefix', or 'alias' (got ${String(collisionPolicy)})`);
    }
    if (collisionPolicy === "alias" && !options.rightAlias?.trim()) {
      throw new PipelineError("join collisionPolicy='alias' requires rightAlias");
    }
    if (collisionPolicy !== "alias" && options.rightAlias) {
      throw new PipelineError("join rightAlias is only valid with collisionPolicy='alias'");
    }
    const leftSchema = left ? this.nodes.find((node) => node.id === left.nodeId)?.config.schema as DatasetSchema | undefined : undefined;
    const rightSchema = right ? this.nodes.find((node) => node.id === right.nodeId)?.config.schema as DatasetSchema | undefined : undefined;
    const derivedSchema = joinDatasetSchema(leftSchema, rightSchema, leftKey, rightKey, collisionPolicy, options.rightAlias || "");
    return this.register("join", name, buildConfig({
      join_type: options.how || "inner",
      left_key: leftKey,
      right_key: rightKey,
      collision_policy: collisionPolicy,
      right_alias: options.rightAlias,
      schema: derivedSchema,
    }), [left, right].filter((r): r is NodeRef => !!r), { nodeKey: options.nodeKey, kind: "dataset" });
  }

  qualityCheck(name: string, input?: NodeRef, options: { rules?: unknown[]; nodeKey?: string } = {}): NodeRef {
    return this.register("quality_check", name, options.rules?.length ? { rules: structuredClone(options.rules) } : {}, input ? [input] : [], { nodeKey: options.nodeKey });
  }

  sinkFile(name: string, input?: NodeRef, options: RetryOptions & PoolOption & { path?: string; format?: string; compress?: string; connId?: string | Connection; nodeKey?: string } = {}): NodeRef {
    return this.register("sink_file", name, buildConfig({
      path: options.path || "",
      format: options.format || "csv",
      conn_id: options.connId,
      compress: options.compress,
      ...retryConfig(options),
      pool: options.pool,
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  /** `createTable` creates the table from the input's columns when it
   * does not exist; `tableEngine` sets the engine of a table created on
   * ClickHouse ("MergeTree" when unset). */
  sinkDb(name: string, input?: NodeRef, options: RetryOptions & PoolOption & { table?: string; mode?: string; connId?: string | Connection; uri?: string; keyColumns?: string[]; truncate?: boolean; createTable?: boolean; tableEngine?: string; nodeKey?: string } = {}): NodeRef {
    return this.register("sink_db", name, buildConfig({
      table: options.table || "",
      mode: options.mode || "append",
      conn_id: options.connId,
      uri: options.uri,
      key_columns: options.keyColumns,
      truncate: options.truncate,
      ...retryConfig(options),
      create_table: options.createTable,
      table_engine: options.tableEngine,
      pool: options.pool,
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  sinkApi(name: string, input?: NodeRef, options: RetryOptions & PoolOption & { url?: string; method?: string; body?: unknown; headers?: Config; connId?: string | Connection; nodeKey?: string } = {}): NodeRef {
    return this.register("sink_api", name, buildConfig({
      url: options.url || "",
      method: options.method || "POST",
      body_template: options.body,
      headers: options.headers,
      conn_id: options.connId,
      ...retryConfig(options),
      pool: options.pool,
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  /**
   * Copy rows from one database to another. `mode` "upsert" needs
   * `keyColumns`. `partition` splits the copy into ranges of one column
   * that run in parallel and retry independently; a partitioned copy must
   * be an upsert whose `keyColumns` include the partition column, into a
   * table that already exists.
   */
  migrate(name: string, options: RetryOptions & PoolOption & {
    sourceUri?: string; targetUri?: string; query?: string; table?: string; mode?: string;
    keyColumns?: string[]; sourceConnId?: string | Connection; targetConnId?: string | Connection;
    dialect?: string; chunkSize?: number; createTable?: boolean; partition?: MigratePartition;
    nodeKey?: string;
  } = {}): DatasetRef {
    const partition = options.partition && buildConfig({
      column: options.partition.column,
      strategy: options.partition.strategy,
      boundaries: options.partition.boundaries.map((b) => (b instanceof Date ? b.toISOString() : b)),
      max_parallel: options.partition.maxParallel,
    });
    return this.register("migrate", name, buildConfig({
      source_uri: options.sourceUri || "",
      dest_uri: options.targetUri || "",
      source_query: options.query || "",
      dest_table: options.table || "",
      mode: options.mode || "append",
      source_conn_id: options.sourceConnId,
      dest_conn_id: options.targetConnId,
      key_columns: options.keyColumns,
      dialect: options.dialect,
      chunk_size: options.chunkSize,
      create_table: options.createTable,
      ...retryConfig(options),
      partition,
      pool: options.pool,
    }), [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  /**
   * Run dbt. `vars` goes to `--vars`: an object is sent as compact JSON
   * with sorted keys (the server reads a string), a string as written.
   * `connId` generates the dbt profile from a Brokoli connection
   * (`profilesDir` still wins), `targetSchema` sets that profile's schema,
   * and `outputModel` names the model whose table this node hands on.
   */
  dbt(name: string, options: RetryOptions & PoolOption & {
    command?: string; projectDir?: string; target?: string; select?: string; profilesDir?: string;
    vars?: Config | string; connId?: string | Connection; targetSchema?: string; outputModel?: string;
    input?: NodeRef; nodeKey?: string;
  } = {}): DatasetRef {
    return this.register("dbt", name, buildConfig({
      command: options.command || "run",
      project_dir: options.projectDir,
      target: options.target,
      select: options.select,
      profiles_dir: options.profilesDir,
      vars: options.vars === undefined || typeof options.vars === "string" ? options.vars : stableJSON(wireValue(options.vars)),
      conn_id: options.connId,
      target_schema: options.targetSchema,
      output_model: options.outputModel,
      ...retryConfig(options),
      pool: options.pool,
    }), options.input ? [options.input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  notify(name: string, input?: NodeRef, options: RetryOptions & { notifyType?: string; webhookUrl?: string; message?: string; channel?: string; nodeKey?: string } = {}): NodeRef {
    return this.register("notify", name, buildConfig({
      notify_type: options.notifyType || "webhook",
      webhook_url: options.webhookUrl || "",
      message: options.message,
      channel: options.channel,
      ...retryConfig(options),
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  /** Enforce an actually-fine data contract: cleared records flow on,
   * quarantined ones are dropped, and a breach of a reject or halt rule
   * fails the node after its evidence is recorded. */
  contractGate(name: string, input: NodeRef | undefined, options: RetryOptions & { contract?: ContractInput; nodeKey?: string } = {}): DatasetRef {
    return this.register("contract_gate", name, buildConfig({
      contract: contractIR(options.contract),
      ...retryConfig(options),
    }), input ? [input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  conditionNode(name: string, input: NodeRef | undefined, expression: string, nodeKey?: string): ConditionRef {
    return this.register("condition", name, { expression }, input ? [input] : [], { nodeKey, kind: "condition" });
  }

  /** Deferrable wait (server v0.10.79+): parks the run, holding no
   * worker slot, until the declarative condition fires. */
  /**
   * Park the run until `condition` holds, without holding a concurrency
   * slot. The server re-checks every `pollInterval` (default 30s) and
   * fails the node after `timeout` (default 6h); `input`, if any, passes
   * through. Durations are Go duration strings ("90s", "2h30m") or
   * seconds.
   */
  wait(
    name: string,
    options: {
      condition?: WaitCondition; path?: string; url?: string; expectStatus?: number; offset?: Duration;
      pipelineId?: string; pollInterval?: Duration; timeout?: Duration; input?: NodeRef; nodeKey?: string;
    } = {},
  ): DatasetRef {
    return this.register("wait", name, buildConfig({
      condition: options.condition || "",
      path: options.path,
      url: options.url,
      expect_status: options.expectStatus,
      offset: goDuration(options.offset),
      pipeline_id: options.pipelineId,
      poll_interval: goDuration(options.pollInterval),
      timeout: goDuration(options.timeout),
    }), options.input ? [options.input] : [], { nodeKey: options.nodeKey, kind: "dataset" });
  }

  code(name: string, input?: NodeRef, options: RetryOptions & PoolOption & { language?: "python" | "typescript"; script?: string; helpers?: Helpers; pythonPath?: string; nodePath?: string; maxMemoryMb?: number; maxCpuSeconds?: number; outputSchema?: DatasetSchema; nodeKey?: string } = {}): NodeRef {
    return this.register("code", name, buildConfig({
      language: options.language || "python",
      script: `${helpersPreamble(options.helpers)}${options.script || ""}`,
      python_path: options.pythonPath,
      node_path: options.nodePath,
      ...retryConfig(options),
      max_memory_mb: options.maxMemoryMb,
      max_cpu_seconds: options.maxCpuSeconds,
      output_schema: options.outputSchema,
      pool: options.pool,
    }), input ? [input] : [], { nodeKey: options.nodeKey });
  }

  /** Run a trusted Bash command and pass the input dataset through unchanged. */
  bash(name: string, input?: NodeRef, options: BashOptions = {}): NodeRef {
    return this.register("bash", name, buildConfig({
      command: options.command || "",
      working_dir: options.workingDir,
      env: options.env,
      ...retryConfig(options),
      max_memory_mb: options.maxMemoryMb,
      max_cpu_seconds: options.maxCpuSeconds,
      pool: options.pool,
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
    if (options.parameters) {
      for (const [paramName, declaration] of Object.entries(options.parameters)) {
        this.mergeParameter(paramName, declaration);
      }
    }
    return this.register("code", name, buildConfig({
      language: "typescript",
      script: taskScript(fn, options.helpers),
      max_retries: options.retries,
      retry_backoff: options.retries === undefined ? undefined : options.retryBackoff || "exponential",
      timeout: options.timeout,
      max_memory_mb: options.maxMemoryMb,
      max_cpu_seconds: options.maxCpuSeconds,
    }), input ? [input] : [], {
      nodeKey: options.nodeKey,
      kind: "dataset",
      taskInterface: buildTaskInterface(options.input, options.output),
    });
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
    const usesTaskInterfaces = Object.keys(this.parameters).length > 0 || this.nodes.some((n) => n.interface !== undefined);
    const ir: PipelineIR = {
      pipeline_id: this.pipelineId,
      name: this.name,
      description: this.description,
      schedule: this.schedule,
      enabled: true,
      ir_version: usesTaskInterfaces ? "2.2" : this.edges.some((e) => e.condition !== undefined) ? "2.1" : "2.0",
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
    // ADR-032 rollout step 3: pipeline-level parameters from task()'s
    // `parameters` option. Omitted unless non-empty, matching every
    // other optional field here -- a pipeline that never used typed
    // task parameters compiles identically to before.
    if (Object.keys(this.parameters).length) ir.parameters = structuredClone(this.parameters);
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

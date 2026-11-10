/**
 * Pipeline IR: types, normalization, canonical rendering, digests.
 *
 * This module implements docs/schema/ir-canonicalization.md from the core
 * repository. The Python SDK (`brokoli.ir`) is the reference
 * implementation; the differential suite in tests/differential asserts
 * byte-identical canonical renderings and equal digests between the two.
 * Change nothing here without reading that spec first.
 */

import type { ParameterDeclaration, TaskInterface } from "./schema";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Config = Record<string, unknown>;

export type Capability =
  | "source"
  | "sink"
  | "compute"
  | "dataset-output"
  | "dynamic-expansion"
  | "collection-output";

// from_port/to_port name the specific ADR-032 output and input ports an
// edge connects. Optional and additive: absent means the node's default
// port, which is every edge this SDK builds today. They exist on the
// type so a hand-assembled or round-tripped IR carrying them is gated by
// requiredExecutionFeatures rather than silently stripped.
export type Edge = {
  from: string;
  to: string;
  condition?: boolean;
  from_port?: string;
  to_port?: string;
};

export type IRNode = {
  id: string;
  type: string;
  name: string;
  config: Config;
  capabilities: Capability[];
  position?: { x: number; y: number };
  /** ADR-032 rollout step 3: this node's portable task interface, built
   * by schema.ts's buildTaskInterface() from task()'s explicit
   * input/output schemas. Absent unless declared -- an untyped task
   * carries no interface key at all, matching the Python SDK. */
  interface?: TaskInterface;
};

export type PipelineIR = {
  pipeline_id: string;
  name: string;
  description: string;
  schedule: string;
  enabled: boolean;
  ir_version: "2.0" | "2.1" | "2.2";
  nodes: IRNode[];
  edges: Edge[];
  tags: string[];
  depends_on: string[];
  schedule_timezone?: string;
  catchup?: boolean;
  sla_deadline?: string;
  sla_timezone?: string;
  hooks?: Record<string, unknown>;
  params?: Record<string, unknown>;
  /** ADR-032 rollout step 3: pipeline-level typed parameter
   * declarations, keyed by name -- from task()'s `parameters` option. */
  parameters?: Record<string, ParameterDeclaration>;
  dependency_rules?: unknown[];
  webhook_url?: string;
  webhook_token?: string;
};

/** Built-in default capabilities per node type — mirrors the server's
 * node_type_capabilities table (served at /api/capabilities). */
export const NODE_TYPE_CAPABILITIES: Record<string, Capability[]> = {
  source_file: ["source", "dataset-output"],
  source_api: ["source", "dataset-output"],
  source_db: ["source", "dataset-output"],
  dbt: ["source", "dataset-output"],
  migrate: ["source", "dataset-output"],
  sink_db: ["sink"],
  sink_file: ["sink"],
  sink_api: ["sink"],
  union: ["compute", "dataset-output"],
  dataset_map: ["compute", "dataset-output"],
  dataset_filter: ["compute", "dataset-output"],
  project: ["compute", "dataset-output"],
  aggregate: ["compute", "dataset-output"],
  filter: ["compute", "dataset-output"],
  wait: ["compute", "dataset-output"],
  bash: ["compute"],
  contract_gate: ["compute"],
};

/** Fields the server assigns; normalization drops them (spec step 1). */
const SERVER_FIELDS = ["id", "source", "workspace_id", "org_id", "created_at", "updated_at"];
/** Absent-or-null list fields normalized to [] (spec step 2). */
const EMPTY_LIST_DEFAULTS = ["nodes", "edges", "tags", "depends_on", "dependency_rules"] as const;
/** Absent-or-null map fields normalized to {} (spec step 3). */
const EMPTY_MAP_DEFAULTS = ["params", "hooks"] as const;

/** Unicode code-point comparison. The spec explicitly forbids
 * locale-sensitive comparison: a Turkish locale must not change digests.
 * Not `<`, which compares UTF-16 code units and so puts a character above
 * U+FFFF (a surrogate pair, from U+D800) before one in U+E000..U+FFFF,
 * where Python's code-point order puts it after. */
export function codePointCompare(a: string, b: string): number {
  const x = a[Symbol.iterator]();
  const y = b[Symbol.iterator]();
  for (;;) {
    const p = x.next();
    const q = y.next();
    if (p.done || q.done) return p.done && q.done ? 0 : p.done ? -1 : 1;
    const cp = p.value.codePointAt(0) as number;
    const cq = q.value.codePointAt(0) as number;
    if (cp !== cq) return cp < cq ? -1 : 1;
  }
}

const INDENT = "  ";

/** One value as canonical JSON at the given depth. Written by hand rather
 * than through JSON.stringify of a key-sorted object, because a JavaScript
 * object puts integer-like keys ("9", "10") first in numeric order whatever
 * order they were inserted in, so the sorted order never reached the
 * output. Numbers use JSON.stringify's own Number-to-string, which is the
 * RFC 8785 rule the spec requires; NaN and the infinities are refused
 * rather than written as null. */
function render(value: unknown, level: number): string | undefined {
  if (value !== null && typeof value === "object" && typeof (value as { toJSON?: unknown }).toJSON === "function") {
    value = (value as { toJSON: () => unknown }).toJSON();
  }
  if (value === null) return "null";
  switch (typeof value) {
    case "boolean":
    case "string":
      return JSON.stringify(value);
    case "number":
      if (!Number.isFinite(value)) throw new RangeError(`Out of range float values are not JSON compliant: ${value}`);
      return JSON.stringify(value);
    case "undefined":
    case "function":
    case "symbol":
      return undefined;
    case "bigint":
      throw new TypeError("Do not know how to serialize a BigInt");
  }
  const inner = INDENT.repeat(level + 1);
  const close = INDENT.repeat(level);
  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    const items = value.map((item) => inner + (render(item, level + 1) ?? "null"));
    return `[\n${items.join(",\n")}\n${close}]`;
  }
  const entries: string[] = [];
  for (const [key, child] of Object.entries(value as Record<string, unknown>).sort(([a], [b]) => codePointCompare(a, b))) {
    const rendered = render(child, level + 1);
    if (rendered !== undefined) entries.push(`${inner}${JSON.stringify(key)}: ${rendered}`);
  }
  if (entries.length === 0) return "{}";
  return `{\n${entries.join(",\n")}\n${close}}`;
}

/** Normalize IR per the spec: drop server fields, fill defaults, drop
 * empty webhook_url / default schedule_timezone, blank webhook_token,
 * strip node positions, fill and sort capabilities, sort nodes/tags/
 * depends_on by code point. Operates on a deep copy. */
export function normalizeIR(input: PipelineIR): PipelineIR {
  const ir = structuredClone(input) as PipelineIR & Record<string, unknown>;

  for (const field of SERVER_FIELDS) delete ir[field];
  for (const field of EMPTY_LIST_DEFAULTS) {
    if (ir[field] === undefined || ir[field] === null) (ir as Record<string, unknown>)[field] = [];
  }
  for (const field of EMPTY_MAP_DEFAULTS) {
    if (ir[field] === undefined || ir[field] === null) (ir as Record<string, unknown>)[field] = {};
  }
  if (ir.webhook_url === "") delete ir.webhook_url;
  if (ir.schedule_timezone === undefined || ir.schedule_timezone === null || ir.schedule_timezone === "" || ir.schedule_timezone === "UTC") {
    delete ir.schedule_timezone;
  }
  if ("webhook_token" in ir) ir.webhook_token = "";

  ir.nodes = ir.nodes.map((node) => {
    const copy: IRNode = { ...node };
    delete copy.position;
    const capabilities =
      copy.capabilities && copy.capabilities.length
        ? copy.capabilities
        : NODE_TYPE_CAPABILITIES[copy.type] || ["compute" as Capability];
    copy.capabilities = [...capabilities].sort(codePointCompare);
    return copy;
  });
  ir.nodes.sort((a, b) => codePointCompare(a.id, b.id));
  ir.tags.sort(codePointCompare);
  ir.depends_on.sort(codePointCompare);
  return ir;
}

/** Canonical JSON (IR canonicalization spec, section 2): keys sorted by
 * code point, two-space indent, literal UTF-8, numbers by RFC 8785, one
 * trailing newline. Byte-identical with the Python SDK's canonical_json,
 * checked against core's shared rendering vectors. */
export function canonicalJSON(value: unknown): string {
  const rendered = render(value, 0);
  if (rendered === undefined) throw new TypeError("canonicalJSON: the value has no JSON form");
  return `${rendered}\n`;
}

/** Normalize then render canonically. */
export function renderIR(ir: PipelineIR): string {
  return canonicalJSON(normalizeIR(ir));
}

/** Stable content digest of the pipeline: sha256 over the canonical
 * rendering's UTF-8 bytes, framed as "sha256:<hex>". Identical between
 * a create and a later update of the same content, and unchanged by
 * server round-trips or editor layout. */
export function irDigest(ir: PipelineIR): string {
  const hash = new Bun.CryptoHasher("sha256");
  hash.update(renderIR(ir));
  return `sha256:${hash.digest("hex")}`;
}

/** Unified-style diff between two canonical renderings. */
export function diffIR(local: PipelineIR, remote?: PipelineIR): string {
  const before = remote ? renderIR(remote).split("\n") : [];
  const after = renderIR(local).split("\n");
  const lines = ["--- server", "+++ local"];
  const max = Math.max(before.length, after.length);
  for (let i = 0; i < max; i++) {
    if (before[i] === after[i]) {
      lines.push(` ${after[i]}`);
    } else {
      if (before[i] !== undefined) lines.push(`-${before[i]}`);
      if (after[i] !== undefined) lines.push(`+${after[i]}`);
    }
  }
  return `${lines.join("\n")}\n`;
}

/** Features naming a runtime that must actually EXIST on the server (a
 * wrapper idiom, a worker language, a harness, a bundle resolver). For
 * these, a capability response that simply omits the feature list must
 * fail closed: absence cannot prove the runtime exists. Purely
 * declarative features keep the legacy waiver. Mirrors the Python SDK's
 * RUNTIME_EXISTENCE_FEATURES. */
export const RUNTIME_EXISTENCE_FEATURES: readonly string[] = [
  "code-typescript",
  "code-streaming-emit",
  // A server old enough to omit the feature list entirely cannot have
  // the wait watcher, so absence means "unsupported" here rather than
  // "predates the field".
  "deferrable-waits",
  "task-parameters-v1",
  "task-runtime-v1",
  "task-bundle-v2",
  "task-ports-v1",
];

/** The execution features a compiled pipeline depends on — must match
 * the Python SDK's required_execution_features so both refuse the same
 * servers for the same reasons. */
export function requiredExecutionFeatures(ir: PipelineIR): string[] {
  const features = new Set<string>();
  if (ir.edges.some((edge) => edge.condition !== undefined)) features.add("conditional-routing");
  // ADR-032 named ports. No released server advertises task-ports-v1
  // yet, so this gate refuses every port-carrying deploy --
  // deliberately. The alternative is worse: a server that doesn't
  // understand from_port/to_port drops them and silently routes the edge
  // as if it were unported, which is a wrong pipeline rather than a
  // refused one.
  if (ir.edges.some((edge) => edge.from_port || edge.to_port)) features.add("task-ports-v1");
  for (const node of ir.nodes) {
    // ADR-033: a "task" node is a whole separate execution path
    // (task-bundle/v2 payload dispatched over the
    // brokoli.task-runtime/v1 protocol), not a variant of "code". A
    // server that predates it doesn't merely lack a feature -- its
    // validator hard-refuses any pipeline containing one, so without
    // this gate the deploy fails server-side with a structural error
    // that says nothing about which capability was missing. task_bundle
    // is mandatory on a task node (optional on a code node), so both
    // features always travel together.
    if (node.type === "task") {
      features.add("task-runtime-v1");
      features.add("task-bundle-v2");
    }
    // ADR-032 rollout step 3: a node's "interface" field is additive IR,
    // but a server that doesn't advertise task-interface-v1 may not
    // even accept IR 2.2 -- refuse at deploy preflight rather than let
    // a strict decoder 400 it.
    if (node.interface !== undefined) features.add("task-interface-v1");
    if (node.config.expansion) features.add("dynamic-expansion");
    if (node.type === "union") features.add("union");
    if (node.type === "dataset_map") features.add("dataset-map");
    if (node.type === "dataset_filter") features.add("dataset-filter");
    if (node.type === "source_api" && node.config.execution && node.config.pagination) features.add("pagination-checkpoints");
    // A deferrable wait parks the run in the scheduler rather than
    // occupying a worker. A server without the watcher has no handler
    // for the node type at all -- it reaches `built-in node type "wait"
    // has no runtime handler` mid-run, after a deploy that looked
    // clean. wait() has always documented "server v0.10.79+"; this is
    // what enforces it.
    if (node.type === "wait") features.add("deferrable-waits");
    if (node.type === "code" && node.config.language === "typescript") features.add("code-typescript");
    if (node.type === "code" && typeof node.config.script === "string" && /\b(?:emit|begin_emit)\s*\(/.test(node.config.script)) {
      features.add("code-streaming-emit");
    }
  }
  if (ir.catchup) features.add("data_intervals");
  // ADR-032 rollout step 3: a pipeline-level "parameters" declaration
  // needs the same gate as a node's own "interface" field above.
  if (ir.parameters && Object.keys(ir.parameters).length) {
    features.add("task-interface-v1");
    // ...and a second, stricter gate. task-interface-v1 only says the
    // server understands the declaration; it does not say the server
    // DELIVERS the resolved values to the task. Until brokoli#487 that
    // was exactly the gap: a submitted parameter was validated, recorded
    // on the run, and then ignored, so the task ran with its default and
    // produced a green run with a wrong answer.
    features.add("task-parameters-v1");
  }
  return [...features].sort(codePointCompare);
}

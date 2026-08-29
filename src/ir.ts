/**
 * Pipeline IR: types, normalization, canonical rendering, digests.
 *
 * This module implements docs/schema/ir-canonicalization.md from the core
 * repository. The Python SDK (`brokoli.ir`) is the reference
 * implementation; the differential suite in tests/differential asserts
 * byte-identical canonical renderings and equal digests between the two.
 * Change nothing here without reading that spec first.
 */

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Config = Record<string, unknown>;

export type Capability =
  | "source"
  | "sink"
  | "compute"
  | "dataset-output"
  | "dynamic-expansion"
  | "collection-output";

export type Edge = { from: string; to: string; condition?: boolean };

export type IRNode = {
  id: string;
  type: string;
  name: string;
  config: Config;
  capabilities: Capability[];
  position?: { x: number; y: number };
};

export type PipelineIR = {
  pipeline_id: string;
  name: string;
  description: string;
  schedule: string;
  enabled: boolean;
  ir_version: "2.0" | "2.1";
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
  wait: ["compute", "dataset-output"],
};

/** Fields the server assigns; normalization drops them (spec step 1). */
const SERVER_FIELDS = ["id", "source", "workspace_id", "org_id", "created_at", "updated_at"];
/** Absent-or-null list fields normalized to [] (spec step 2). */
const EMPTY_LIST_DEFAULTS = ["nodes", "edges", "tags", "depends_on", "dependency_rules"] as const;
/** Absent-or-null map fields normalized to {} (spec step 3). */
const EMPTY_MAP_DEFAULTS = ["params", "hooks"] as const;

/** Unicode code-point comparison. The spec explicitly forbids
 * locale-sensitive comparison: a Turkish locale must not change digests. */
function codePointCompare(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

function sortKeysDeep(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeysDeep);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([a], [b]) => codePointCompare(a, b))
        .map(([key, child]) => [key, sortKeysDeep(child)]),
    );
  }
  return value;
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

/** Canonical JSON: keys sorted by code point, two-space indent, literal
 * UTF-8, one trailing newline. Byte-compatible with Python's
 * json.dumps(indent=2, sort_keys=True, ensure_ascii=False) + "\n". */
export function canonicalJSON(value: unknown): string {
  return `${JSON.stringify(sortKeysDeep(value), null, 2)}\n`;
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

/** The execution features a compiled pipeline depends on — must match
 * the Python SDK's required_execution_features so both refuse the same
 * servers for the same reasons. */
export function requiredExecutionFeatures(ir: PipelineIR): string[] {
  const features = new Set<string>();
  if (ir.edges.some((edge) => edge.condition !== undefined)) features.add("conditional-routing");
  for (const node of ir.nodes) {
    if (node.config.expansion) features.add("dynamic-expansion");
    if (node.type === "union") features.add("union");
    if (node.type === "dataset_map") features.add("dataset-map");
    if (node.type === "dataset_filter") features.add("dataset-filter");
    if (node.config.execution) features.add("pagination-checkpoints");
  }
  if (ir.catchup) features.add("data_intervals");
  return [...features].sort(codePointCompare);
}

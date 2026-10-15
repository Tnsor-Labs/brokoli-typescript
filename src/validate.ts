/** Local structural validation — the checks that need no server. */

import Ajv2020 from "ajv/dist/2020";
import pipelineSchema from "../schema/pipeline-ir-2.2.json";
import taskInterfaceSchema from "../schema/task-interface-v1.json";
import type { Pipeline } from "./pipeline";

const platformReference = /\$\{(interval|env|param|secret|var|run)\.[^}]*\}/;
const paramReference = /\$\{param\.[^}]*\}/;
const bashEnvName = /^[A-Za-z_][A-Za-z0-9_]*$/;
// A Go time.ParseDuration string, which is what the server parses wait
// durations with.
const goDuration = /^-?([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|\u00b5s|ms|s|m|h)(([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|\u00b5s|ms|s|m|h))*$/;
const waitRequires: Record<string, string> = { file_exists: "path", http: "url", pipeline: "pipeline_id" };
const partitionColumn = /^[A-Za-z_][A-Za-z0-9_]*$/;
const contractIrKeys = ["ir_version", "contract", "input", "rules"];

function isPositiveNumber(value: unknown): boolean {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

/** Why a migrate partition spec is one the server refuses, or "" if it is
 * not. Mirrors the server's parseMigratePartitionConfig. */
function partitionError(partition: unknown): string {
  if (!partition || typeof partition !== "object" || Array.isArray(partition)) return "partition must be an object";
  const p = partition as Record<string, unknown>;
  if (typeof p.column !== "string" || !partitionColumn.test(p.column)) return "partition.column must be a simple SQL identifier";
  const strategy = String(p.strategy ?? "").trim().toLowerCase();
  if (strategy !== "numeric" && strategy !== "date") return "partition.strategy must be numeric or date";
  const boundaries = p.boundaries;
  if (!Array.isArray(boundaries) || boundaries.length < 2) return "partition.boundaries must contain at least two values";
  const keys: number[] = [];
  for (const [i, boundary] of boundaries.entries()) {
    if (strategy === "numeric") {
      if (typeof boundary !== "number" || !Number.isFinite(boundary)) return `partition.boundaries[${i}] must be a finite number`;
      keys.push(boundary);
    } else {
      const parsed = typeof boundary === "string" && /T/.test(boundary) && /(Z|[+-]\d{2}:\d{2})$/.test(boundary) ? Date.parse(boundary) : NaN;
      if (Number.isNaN(parsed)) return `partition.boundaries[${i}] must be an RFC3339 timestamp string`;
      keys.push(parsed);
    }
  }
  if (keys.some((key, i) => i > 0 && keys[i - 1] >= key)) return "partition.boundaries must be strictly increasing";
  if ("max_parallel" in p) {
    const max = p.max_parallel;
    if (typeof max !== "number" || !Number.isInteger(max) || max < 1 || max > 64) return "partition.max_parallel must be between 1 and 64";
  }
  return "";
}

// pipeline-ir-2.2.json's "interface"/"parameters" fields $ref into
// task-interface-v1.json by its own declared absolute $id -- register
// it first so Ajv can resolve that relative "task-interface-v1.json#/..."
// ref into the same https:// URL-space both schemas' $id declare (the
// same cross-file resolution the core Go test suite needs; see
// models/ir_schema_contract_test.go there). 2.2 is a strict superset of
// 2.0/2.1 (a document using neither field is a valid 2.2 document), so
// this is the one schema every pipeline validates against regardless of
// which ir_version it actually compiles to.
// strictRequired: false -- task-interface-v1.json's parameter_declaration
// enforces "required:true and default are mutually exclusive" with a
// `not: { required: [...] }` clause naming a property outside that
// sub-schema's own `properties` block (a deliberate, well-formed
// construct also used by the core repo's Go validator and the Python
// SDK's own schema tests); Ajv's strict mode alone flags this pattern
// as suspicious even though it isn't.
const ajv = new Ajv2020({ allErrors: true, strict: true, strictRequired: false });
ajv.addSchema(taskInterfaceSchema);
const validateSchema = ajv.compile(pipelineSchema);

export type ValidationIssue = {
  nodeName: string;
  field: string;
  message: string;
  severity: "error" | "warning";
};

export type ValidationResult = {
  errors: ValidationIssue[];
  warnings: ValidationIssue[];
  valid: boolean;
};

export function validatePipeline(pipeline: Pipeline): ValidationResult {
  const data = pipeline.toJSON();
  const errors: ValidationIssue[] = [];
  const warnings: ValidationIssue[] = [];
  const error = (nodeName: string, field: string, message: string) =>
    errors.push({ nodeName, field, message, severity: "error" });
  const warning = (nodeName: string, field: string, message: string) =>
    warnings.push({ nodeName, field, message, severity: "warning" });

  if (!validateSchema(data)) {
    for (const issue of validateSchema.errors || []) {
      const field = issue.instancePath || issue.params.missingProperty || "schema";
      error("", String(field), `IR schema ${issue.message || "validation failed"}`);
    }
  }

  if (!data.name) error("", "name", "Pipeline name is required");
  if (!data.nodes.length) error("", "nodes", "Pipeline must have at least one node");

  const ids = new Set(data.nodes.map((n) => n.id));
  const indegree = new Map<string, number>(data.nodes.map((n) => [n.id, 0]));
  for (const edge of data.edges) {
    if (!ids.has(edge.from)) error("", "edge", `Edge references unknown source node: ${edge.from}`);
    if (!ids.has(edge.to)) error("", "edge", `Edge references unknown target node: ${edge.to}`);
    if (ids.has(edge.to)) indegree.set(edge.to, (indegree.get(edge.to) || 0) + 1);
  }

  for (const node of data.nodes) {
    const c = node.config || {};
    switch (node.type) {
      case "source_file":
        if (!c.path) error(node.name, "path", "Source File requires a 'path'");
        break;
      case "source_db":
        if (!c.query || (!c.conn_id && !c.uri)) error(node.name, "config", "Source DB requires query and conn_id or uri");
        break;
      case "source_api":
        if (!c.url) error(node.name, "url", "Source API requires a 'url'");
        const execution = c.execution as Record<string, any> | undefined;
        if (execution?.profile) {
          const profile = execution.profile;
          if (profile === null || typeof profile !== "object" || profile.version !== 1 || typeof profile.name !== "string" || typeof profile.strict !== "boolean") {
            error(node.name, "execution.profile", "Execution profile must be a version 1 object with name and strict fields");
          }
          for (const field of ["max_concurrency", "requests_per_second", "retry_scope", "checkpoint_every", "page_max_retries", "page_retry_backoff"]) {
            if (!(field in execution)) error(node.name, `execution.${field}`, `Profile must expand execution.${field} explicitly`);
          }
          for (const field of ["timeout", "max_retries", "retry_backoff", "retry_delay"]) {
            if (!(field in c)) error(node.name, field, `Profile must expand ${field} explicitly`);
          }
          const pagination = c.pagination as Record<string, any> | undefined;
          const strategy = pagination?.strategy;
          if (profile.strict && ["cursor", "next_link", "link_header"].includes(strategy) && execution.max_concurrency > 1) {
            error(node.name, "execution.max_concurrency", `Strict profile cannot request concurrency for sequential pagination strategy ${strategy}`);
          }
        }
        break;
      case "sink_file":
        if (!c.path) error(node.name, "path", "Sink File requires a 'path'");
        break;
      case "sink_db":
        if (!c.table || (!c.conn_id && !c.uri)) error(node.name, "config", "Sink DB requires table and conn_id or uri");
        if (c.mode === "upsert" && !(Array.isArray(c.key_columns) && c.key_columns.length)) {
          error(node.name, "key_columns", "Upsert requires key_columns naming a unique index");
        }
        break;
      case "quality_check":
        if (!c.rules) error(node.name, "rules", "Quality Check requires at least one rule");
        break;
      case "condition":
        if (!c.expression) error(node.name, "expression", "Condition node requires an 'expression'");
        if (indegree.get(node.id) !== 1) error(node.name, "inputs", `condition requires exactly 1 input, got ${indegree.get(node.id)}`);
        break;
      case "join":
        if (indegree.get(node.id) !== 2) error(node.name, "inputs", `join requires exactly 2 inputs, got ${indegree.get(node.id)}`);
        break;
      case "wait": {
        // Mirrors the server's parseWaitCondition (engine/wait.go).
        const condition = String(c.condition || "");
        if (!condition) {
          error(node.name, "condition", "Wait requires a condition (file_exists, http, interval_elapsed, pipeline)");
          break;
        }
        if (!(condition in waitRequires) && condition !== "interval_elapsed") {
          error(node.name, "condition", `Unknown wait condition ${JSON.stringify(condition)} (known: file_exists, http, interval_elapsed, pipeline)`);
          break;
        }
        const required = waitRequires[condition];
        if (required && !c[required]) error(node.name, required, `Wait condition ${condition} needs a ${required}`);
        for (const key of ["poll_interval", "timeout", "offset"]) {
          const value = c[key];
          if (value === undefined || value === "") continue;
          if (typeof value !== "string" || !goDuration.test(value)) {
            error(node.name, key, `Wait ${key} ${JSON.stringify(value)} is not a duration such as '30s' or '2h'`);
          } else if (key !== "offset" && (value.startsWith("-") || !/[1-9]/.test(value))) {
            error(node.name, key, `Wait ${key} ${JSON.stringify(value)} must be a positive duration`);
          }
        }
        break;
      }
      case "contract_gate": {
        const contract = c.contract;
        if (contract === undefined) {
          error(node.name, "contract", "contract_gate requires a 'contract'");
        } else if (!contract || typeof contract !== "object" || Array.isArray(contract)) {
          error(node.name, "contract", "contract_gate 'contract' must be an actually-fine contract: its IR as an object, or a Contract");
        } else {
          const missing = contractIrKeys.filter((key) => !(key in (contract as Record<string, unknown>)));
          if (missing.length) error(node.name, "contract", `contract_gate contract is missing ${missing.join(", ")}`);
        }
        break;
      }
      case "migrate": {
        const hasUris = c.source_uri && c.dest_uri;
        const hasConns = c.source_conn_id && c.dest_conn_id;
        if (!hasUris && !hasConns) error(node.name, "conn", "Migrate requires sourceUri + targetUri or sourceConnId + targetConnId");
        if (c.mode === "upsert" && !(Array.isArray(c.key_columns) && c.key_columns.length)) {
          error(node.name, "key_columns", "Migrate mode 'upsert' requires keyColumns");
        }
        if ("partition" in c) {
          // Mirrors the partitioned-migrate rules in the server's validator.
          const problem = partitionError(c.partition);
          if (problem) {
            error(node.name, "partition", problem);
            break;
          }
          const column = String((c.partition as Record<string, unknown>).column).toLowerCase();
          const keys = Array.isArray(c.key_columns) ? c.key_columns.map((k) => String(k).toLowerCase()) : [];
          if (String(c.mode || "").toLowerCase() !== "upsert") error(node.name, "mode", "Partitioned migrate requires mode 'upsert'");
          if (!keys.length) error(node.name, "key_columns", "Partitioned migrate requires keyColumns");
          else if (!keys.includes(column)) error(node.name, "key_columns", `partition.column ${JSON.stringify(column)} must be included in keyColumns`);
          if (c.create_table === true) error(node.name, "create_table", "Partitioned migrate requires createTable false");
        }
        break;
      }
      case "code": {
        if (!c.script) error(node.name, "script", "Code node requires a script");
        if (c.language !== undefined && c.language !== "python" && c.language !== "typescript") {
          error(node.name, "language", "Code language must be python or typescript");
        }
        const reference = typeof c.script === "string" ? c.script.match(paramReference)?.[0] : undefined;
        if (reference) {
          // Mirrors the server's codeScriptParamError (core v0.15.0).
          error(node.name, "script", `Code script contains ${reference}, which the server does not substitute in code: whoever starts the run would choose the code. Read params instead`);
        }
        for (const key of ["max_memory_mb", "max_cpu_seconds"]) {
          if (c[key] !== undefined && !isPositiveNumber(c[key])) error(node.name, key, `Code '${key}' must be a positive number`);
        }
        break;
      }
      case "bash": {
        if (typeof c.command !== "string" || !c.command.trim()) {
          error(node.name, "command", "Bash node requires a non-empty 'command'");
        } else {
          const reference = c.command.match(platformReference)?.[0];
          if (reference) error(node.name, "command", `Bash command contains ${reference}, which must be passed through 'env' instead of shell source`);
        }
        if (c.working_dir !== undefined && typeof c.working_dir !== "string") {
          error(node.name, "working_dir", "Bash 'working_dir' must be a string");
        }
        if (c.env !== undefined) {
          if (c.env === null || typeof c.env !== "object" || Array.isArray(c.env)) {
            error(node.name, "env", "Bash 'env' must be an object of string values");
          } else {
            for (const [key, value] of Object.entries(c.env as Record<string, unknown>)) {
              if (!bashEnvName.test(key)) {
                error(node.name, "env", `Bash environment name ${JSON.stringify(key)} is not a valid shell variable name`);
                break;
              }
              if (typeof value !== "string" || value.includes("\0")) {
                error(node.name, "env", "Bash 'env' values must be strings without NUL bytes");
                break;
              }
            }
          }
        }
        break;
      }
    }
  }

  if (!data.nodes.some((n) => n.capabilities.includes("source"))) {
    warning("", "capabilities", "Pipeline has no source node");
  }
  return { errors, warnings, valid: errors.length === 0 };
}

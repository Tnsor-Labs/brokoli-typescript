/** Local structural validation — the checks that need no server. */

import Ajv2020 from "ajv/dist/2020";
import pipelineSchema from "../schema/pipeline-ir-2.2.json";
import taskInterfaceSchema from "../schema/task-interface-v1.json";
import type { Pipeline } from "./pipeline";

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
      case "wait":
        if (!c.condition) error(node.name, "condition", "Wait requires a condition (file_exists, http, interval_elapsed, pipeline)");
        break;
      case "code":
        if (!c.script) error(node.name, "script", "Code node requires a script");
        if (c.language !== undefined && c.language !== "python" && c.language !== "typescript") {
          error(node.name, "language", "Code language must be python or typescript");
        }
        break;
    }
  }

  if (!data.nodes.some((n) => n.capabilities.includes("source"))) {
    warning("", "capabilities", "Pipeline has no source node");
  }
  return { errors, warnings, valid: errors.length === 0 };
}

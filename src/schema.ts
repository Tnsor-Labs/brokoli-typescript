/**
 * BPTD (Brokoli Portable Type Descriptor) builders for ADR-032 rollout
 * step 3 -- explicit schema/parameter declarations for `task()`'s node
 * interface and a pipeline's typed parameters.
 *
 * TypeScript cannot recover erased generic types at runtime (ADR-032
 * section 8 rule 6), so unlike the Python SDK's annotation-driven
 * inference, this is purely explicit: nothing here reflects on a
 * function's type parameters. `schema`/`parameter` just build the same
 * BPTD JSON the Python SDK's `interface_inference` module infers.
 *
 * Deliberately scoped, matching the Python SDK's own step-3 scope: a
 * `union` BPTD kind isn't a builder here (a real discriminated union
 * needs per-variant tags a bare builder can't invent), and `parameter`
 * only builds the four common scalar shapes -- pass a `type` directly
 * to `TaskOptions.parameters` for anything else.
 */

export type BptdType =
  | { kind: "int64"; nullable?: boolean; description?: string }
  | { kind: "float64"; nullable?: boolean; description?: string }
  | { kind: "string"; nullable?: boolean; description?: string }
  | { kind: "boolean"; nullable?: boolean; description?: string }
  | { kind: "bytes"; nullable?: boolean; description?: string }
  | { kind: "decimal"; nullable?: boolean; description?: string }
  | { kind: "date"; nullable?: boolean; description?: string }
  | { kind: "timestamp"; nullable?: boolean; description?: string }
  | { kind: "duration"; nullable?: boolean; description?: string }
  | { kind: "json"; nullable?: boolean; description?: string }
  | { kind: "unknown"; nullable?: boolean; description?: string }
  | { kind: "enum"; values: string[]; nullable?: boolean; description?: string }
  | { kind: "array"; items: BptdType; nullable?: boolean; description?: string }
  | { kind: "map"; keys: "string"; values: BptdType; nullable?: boolean; description?: string }
  | { kind: "record"; fields: RecordField[]; additional_fields?: boolean; nullable?: boolean; description?: string };

export type RecordField = { name: string; type: BptdType; required?: boolean; description?: string };

/** A record field's type, or `{ type, required, description }` when it
 * needs to opt out of the default `required: true`. */
export type RecordFieldSpec = BptdType | { type: BptdType; required?: boolean; description?: string };

export type ParameterDeclaration = {
  type: BptdType;
  required?: boolean;
  default?: unknown;
  description?: string;
  sensitive?: boolean;
};

export type TaskInterface = {
  contract: "brokoli.task-interface/v1";
  inputs: { input: { value: { kind: "dataset"; row: BptdType | { kind: "unknown" } } } };
  outputs: { result: { value: { kind: "dataset"; row: BptdType | { kind: "unknown" } } } };
};

function isBareType(spec: RecordFieldSpec): spec is BptdType {
  return "kind" in spec;
}

export const schema = {
  int64: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "int64", ...opts }),
  float64: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "float64", ...opts }),
  string: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "string", ...opts }),
  boolean: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "boolean", ...opts }),
  bytes: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "bytes", ...opts }),
  decimal: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "decimal", ...opts }),
  date: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "date", ...opts }),
  timestamp: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "timestamp", ...opts }),
  duration: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "duration", ...opts }),
  json: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "json", ...opts }),
  unknown: (opts: { nullable?: boolean; description?: string } = {}): BptdType => ({ kind: "unknown", ...opts }),

  enum: (values: string[], opts: { nullable?: boolean; description?: string } = {}): BptdType => ({
    kind: "enum",
    values,
    ...opts,
  }),

  array: (items: BptdType, opts: { nullable?: boolean; description?: string } = {}): BptdType => ({
    kind: "array",
    items,
    ...opts,
  }),

  map: (values: BptdType, opts: { nullable?: boolean; description?: string } = {}): BptdType => ({
    kind: "map",
    keys: "string",
    values,
    ...opts,
  }),

  /** A closed record (`additional_fields: false`) -- BPTD's honesty
   * default. Every field is required unless given the `{ type,
   * required: false }` form. */
  record: (
    fields: Record<string, RecordFieldSpec>,
    opts: { additionalFields?: boolean; nullable?: boolean; description?: string } = {},
  ): BptdType => ({
    kind: "record",
    fields: Object.entries(fields).map(([name, spec]) =>
      isBareType(spec)
        ? { name, type: spec, required: true }
        : { name, type: spec.type, required: spec.required ?? true, ...(spec.description !== undefined ? { description: spec.description } : {}) },
    ),
    additional_fields: opts.additionalFields ?? false,
    ...(opts.nullable !== undefined ? { nullable: opts.nullable } : {}),
    ...(opts.description !== undefined ? { description: opts.description } : {}),
  }),

  /** A copy of `type` marked nullable -- present-but-null-allowed,
   * distinct from a record field's own `required` (may-be-absent). */
  nullable: (type: BptdType): BptdType => ({ ...type, nullable: true }),
};

function buildParameter(type: BptdType, opts: { default?: unknown; required?: boolean; description?: string; sensitive?: boolean } = {}): ParameterDeclaration {
  const declaration: ParameterDeclaration = { type };
  if (opts.default !== undefined) {
    declaration.default = opts.default;
    declaration.required = opts.required ?? false;
  } else {
    declaration.required = opts.required ?? true;
  }
  if (opts.description !== undefined) declaration.description = opts.description;
  if (opts.sensitive !== undefined) declaration.sensitive = opts.sensitive;
  return declaration;
}

type ParameterOptions = { default?: unknown; required?: boolean; description?: string; sensitive?: boolean };

export const parameter = {
  number: (opts: ParameterOptions = {}): ParameterDeclaration => buildParameter({ kind: "float64" }, opts),
  string: (opts: ParameterOptions = {}): ParameterDeclaration => buildParameter({ kind: "string" }, opts),
  boolean: (opts: ParameterOptions = {}): ParameterDeclaration => buildParameter({ kind: "boolean" }, opts),
  enum: (values: string[], opts: ParameterOptions = {}): ParameterDeclaration =>
    buildParameter({ kind: "enum", values }, opts),
};

/** Build a node's task interface from optional input/output row
 * schemas -- `undefined` on either side. `undefined` on both means no
 * interface at all (the base SDK stays boilerplate-free for a task with
 * no declared schema, matching the Python SDK's inference: an
 * unannotated `@task` never gets a vacuous interface either). */
export function buildTaskInterface(input?: BptdType, output?: BptdType): TaskInterface | undefined {
  if (!input && !output) return undefined;
  return {
    contract: "brokoli.task-interface/v1",
    inputs: { input: { value: { kind: "dataset", row: input ?? { kind: "unknown" } } } },
    outputs: { result: { value: { kind: "dataset", row: output ?? { kind: "unknown" } } } },
  };
}

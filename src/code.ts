import { PipelineError } from "./errors";

/** Version of the server-side JS wrapper contract these generators target. */
export const JS_WRAPPER_VERSION = 1;

export type Row = Record<string, unknown>;
export type OutputRows = Row[] | Iterable<Row> | AsyncIterable<Row>;
export type TaskOutput = { columns: string[]; rows: OutputRows };
export type TaskFunction = (rows: Row[]) => TaskOutput | Promise<TaskOutput>;
export type SourceFunction = () => TaskOutput | Promise<TaskOutput>;
export type SinkFunction = (rows: Row[]) => unknown | Promise<unknown>;
export type RowPredicate = (row: Row) => boolean | Promise<boolean>;
export type RowMapper = (row: Row) => Row | Promise<Row>;
export type ValidateResult = boolean | [boolean, string];
export type ValidateFunction = (rows: Row[]) => ValidateResult | Promise<ValidateResult>;
export type SensorFunction = () => boolean | Promise<boolean>;

/**
 * Serialize a function for the versioned JS-wrapper namespace.
 *
 * v1 deliberately supports only self-contained functions. JavaScript
 * provides no reliable closure introspection, so native/bound functions
 * are rejected here and free-variable failures remain local contract-test
 * failures. Closure/import packaging needs its own ADR.
 */
export function functionSource(fn: Function): string {
  if (typeof fn !== "function") throw new PipelineError("code-node authoring requires a function");
  const source = Function.prototype.toString.call(fn).trim();
  if (!source || source.includes("[native code]")) {
    throw new PipelineError("code-node functions must be self-contained source functions; native and bound functions cannot be deployed");
  }
  if (/^class\s/.test(source)) throw new PipelineError("code-node authoring accepts functions, not classes");
  const isFunctionExpression = /^(?:async\s+)?function\b/.test(source);
  const isParenthesizedAsyncArrow = /^async\s*\(/.test(source);
  if (!isFunctionExpression && !isParenthesizedAsyncArrow
    && /^(?:(?:async|get|set)\s+)?\*?\s*[A-Za-z_$][\w$]*\s*\(/.test(source)) {
    throw new PipelineError("object methods are not deployable function expressions; pass an arrow or function expression instead");
  }
  return source;
}

export type Helpers = Record<string, unknown>;

/** Fixed wrapper namespace names (ADR-030) a helper must never shadow —
 * these are not lexical scope, so a colliding const would silently break
 * the generated script's own contract rather than merely shadow a local. */
const RESERVED_WRAPPER_NAMES = new Set([
  "rows",
  "rowsStream",
  "columns",
  "config",
  "params",
  "emit",
  "begin_emit",
  "sleep",
  "output_data",
]);

function serializeHelperValue(name: string, value: unknown): string {
  if (typeof value === "function") {
    return `const ${name} = (${functionSource(value)});`;
  }
  let literal: string | undefined;
  try {
    literal = JSON.stringify(value);
  } catch {
    literal = undefined;
  }
  if (literal === undefined) {
    throw new PipelineError(
      `helpers.${name} is neither a function nor JSON-serializable (got ${typeof value}); ` +
        "only functions and JSON-safe constants can be captured as helpers",
    );
  }
  return `const ${name} = ${literal};`;
}

/**
 * Serialize a `helpers` map (ADR-034 item 3) into a preamble of `const`
 * declarations, so a task/map/filter/... body can reference module-level
 * helpers and constants without inlining them by hand. Rewriting happens
 * here, at authoring time — the worker still receives one opaque script,
 * so emitted IR is unaffected by this feature existing.
 *
 * Scope, stated plainly: this catches the checkable failure modes (an
 * invalid identifier, a name colliding with the fixed wrapper namespace,
 * a helper that is neither a function nor JSON-serializable, a function
 * `functionSource()` already rejects as non-self-contained). It does
 * **not** attempt general free-variable/closure analysis — detecting
 * that a helper function itself closes over some other module-scope
 * variable not present in this same `helpers` map needs real AST
 * inspection, which JavaScript's "no bytecode inspection" (the same
 * limitation the base v1 contract already has for an ordinary task
 * body) does not give us. That case still surfaces, just remotely, as a
 * `ReferenceError` inside the worker rather than a local `PipelineError`.
 */
export function helpersPreamble(helpers?: Helpers): string {
  if (!helpers) return "";
  const names = Object.keys(helpers);
  if (names.length === 0) return "";
  const lines: string[] = [];
  for (const name of names) {
    if (!/^[A-Za-z_$][\w$]*$/.test(name)) {
      throw new PipelineError(`helpers key ${JSON.stringify(name)} is not a valid identifier`);
    }
    if (RESERVED_WRAPPER_NAMES.has(name)) {
      throw new PipelineError(
        `helpers.${name} collides with the fixed wrapper namespace name "${name}"; rename the helper`,
      );
    }
    lines.push(serializeHelperValue(name, helpers[name]));
  }
  return `${lines.join("\n")}\n`;
}

export function taskScript(fn: TaskFunction, helpers?: Helpers): string {
  return `${helpersPreamble(helpers)}output_data = await (${functionSource(fn)})(rows);`;
}

export function sourceScript(fn: SourceFunction, helpers?: Helpers): string {
  return `${helpersPreamble(helpers)}output_data = await (${functionSource(fn)})();`;
}

export function sinkScript(fn: SinkFunction, helpers?: Helpers): string {
  return `${helpersPreamble(helpers)}await (${functionSource(fn)})(rows);\noutput_data = { columns, rows };`;
}

export function filterScript(fn: RowPredicate, helpers?: Helpers): string {
  return [
    `${helpersPreamble(helpers)}const __brokoli_filter = (${functionSource(fn)});`,
    "begin_emit(columns);",
    "for await (const row of rowsStream()) {",
    "  if (await __brokoli_filter(row)) emit(row);",
    "}",
  ].join("\n");
}

export function mapScript(fn: RowMapper, outputColumns?: string[], helpers?: Helpers): string {
  return [
    `${helpersPreamble(helpers)}const __brokoli_map = (${functionSource(fn)});`,
    outputColumns ? `begin_emit(${JSON.stringify(outputColumns)});` : "begin_emit();",
    "for await (const row of rowsStream()) emit(await __brokoli_map(row));",
  ].join("\n");
}

export function validateScript(fn: ValidateFunction, onFailure: "block" | "warn", helpers?: Helpers): string {
  return [
    `${helpersPreamble(helpers)}const __brokoli_validation = await (${functionSource(fn)})(rows);`,
    "const [__brokoli_passed, __brokoli_message] = Array.isArray(__brokoli_validation)",
    "  ? __brokoli_validation : [Boolean(__brokoli_validation), \"\"];",
    onFailure === "block"
      ? "if (!__brokoli_passed) throw new Error(__brokoli_message || \"validation failed\");"
      : "if (!__brokoli_passed) console.error(`validation warning: ${__brokoli_message || \"failed\"}`);",
    "output_data = { columns, rows };",
  ].join("\n");
}

export function sensorScript(fn: SensorFunction, pollInterval: number, helpers?: Helpers): string {
  return [
    `${helpersPreamble(helpers)}const __brokoli_sensor = (${functionSource(fn)});`,
    "while (!(await __brokoli_sensor())) {",
    `  await sleep(${Math.max(1, pollInterval) * 1000});`,
    "}",
    "output_data = { columns, rows };",
  ].join("\n");
}

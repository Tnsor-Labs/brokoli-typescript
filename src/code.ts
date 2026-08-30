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
  if (/^(?:(?:async|get|set)\s+)?\*?\s*[A-Za-z_$][\w$]*\s*\(/.test(source)) {
    throw new PipelineError("object methods are not deployable function expressions; pass an arrow or function expression instead");
  }
  return source;
}

export function taskScript(fn: TaskFunction): string {
  return `output_data = await (${functionSource(fn)})(rows);`;
}

export function sourceScript(fn: SourceFunction): string {
  return `output_data = await (${functionSource(fn)})();`;
}

export function sinkScript(fn: SinkFunction): string {
  return `await (${functionSource(fn)})(rows);\noutput_data = { columns, rows };`;
}

export function filterScript(fn: RowPredicate): string {
  return [
    `const __brokoli_filter = (${functionSource(fn)});`,
    "begin_emit(columns);",
    "for await (const row of rowsStream()) {",
    "  if (await __brokoli_filter(row)) emit(row);",
    "}",
  ].join("\n");
}

export function mapScript(fn: RowMapper, outputColumns?: string[]): string {
  return [
    `const __brokoli_map = (${functionSource(fn)});`,
    outputColumns ? `begin_emit(${JSON.stringify(outputColumns)});` : "begin_emit();",
    "for await (const row of rowsStream()) emit(await __brokoli_map(row));",
  ].join("\n");
}

export function validateScript(fn: ValidateFunction, onFailure: "block" | "warn"): string {
  return [
    `const __brokoli_validation = await (${functionSource(fn)})(rows);`,
    "const [__brokoli_passed, __brokoli_message] = Array.isArray(__brokoli_validation)",
    "  ? __brokoli_validation : [Boolean(__brokoli_validation), \"\"];",
    onFailure === "block"
      ? "if (!__brokoli_passed) throw new Error(__brokoli_message || \"validation failed\");"
      : "if (!__brokoli_passed) console.error(`validation warning: ${__brokoli_message || \"failed\"}`);",
    "output_data = { columns, rows };",
  ].join("\n");
}

export function sensorScript(fn: SensorFunction, pollInterval: number): string {
  return [
    `const __brokoli_sensor = (${functionSource(fn)});`,
    "while (!(await __brokoli_sensor())) {",
    `  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ${Math.max(1, pollInterval) * 1000});`,
    "}",
    "output_data = { columns, rows };",
  ].join("\n");
}

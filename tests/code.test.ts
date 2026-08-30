import { describe, expect, test } from "bun:test";
import { Client, Pipeline, filterScript, functionSource, mapScript, sensorScript, sinkScript, sourceScript, taskScript, validateScript } from "../src";

type ContractResult = { output: unknown; emitted: unknown[]; emitColumns?: string[] };

async function execute(script: string, rows: Record<string, unknown>[]): Promise<ContractResult> {
  const emitted: unknown[] = [];
  let emitColumns: string[] | undefined;
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor as FunctionConstructor;
  const run = new AsyncFunction(
    "rows", "columns", "config", "params", "emit", "begin_emit", "rowsStream", "console",
    `let output_data; ${script}\nreturn output_data;`,
  );
  const output = await run(
    rows,
    Object.keys(rows[0] || {}),
    {},
    {},
    (row: unknown) => emitted.push(row),
    (columns?: string[]) => { emitColumns = columns; },
    async function* () { for (const row of rows) yield row; },
    { log() {}, error() {} },
  );
  return { output, emitted, emitColumns };
}

describe("TypeScript code-node authoring", () => {
  test("task serializes and executes a self-contained function", async () => {
    const script = taskScript((rows) => ({ columns: ["id", "total"], rows: rows.map((row) => ({ id: row.id, total: Number(row.amt) * 2 })) }));
    const result = await execute(script, [{ id: 1, amt: "4" }]);
    expect(result.output).toEqual({ columns: ["id", "total"], rows: [{ id: 1, total: 8 }] });
  });

  test("filter and map use the streaming namespace", async () => {
    const filtered = await execute(filterScript((row) => Number(row.value) > 1), [{ value: 1 }, { value: 2 }]);
    expect(filtered.emitted).toEqual([{ value: 2 }]);
    expect(filtered.emitColumns).toEqual(["value"]);
    const mapped = await execute(mapScript((row) => ({ doubled: Number(row.value) * 2 }), ["doubled"]), [{ value: 3 }]);
    expect(mapped.emitted).toEqual([{ doubled: 6 }]);
    expect(mapped.emitColumns).toEqual(["doubled"]);
  });

  test("source, sink, validate, and sensor honor the wrapper contract", async () => {
    const sourced = await execute(sourceScript(() => ({ columns: ["id"], rows: [{ id: 1 }] })), []);
    expect(sourced.output).toEqual({ columns: ["id"], rows: [{ id: 1 }] });
    const sunk = await execute(sinkScript(() => undefined), [{ id: 1 }]);
    expect(sunk.output).toEqual({ columns: ["id"], rows: [{ id: 1 }] });
    await expect(execute(validateScript(() => [false, "bad rows"], "block"), [{ id: 1 }])).rejects.toThrow(/bad rows/);
    const warned = await execute(validateScript(() => [false, "warning"], "warn"), [{ id: 1 }]);
    expect(warned.output).toEqual({ columns: ["id"], rows: [{ id: 1 }] });
    const sensed = await execute(sensorScript(() => true, 1), []);
    expect(sensed.output).toEqual({ columns: [], rows: [] });
  });

  test("accepts arrow and function-expression sources", () => {
    const p = new Pipeline("Functions");
    expect(() => p.task("Arrow", undefined, (rows) => ({ columns: [], rows }))).not.toThrow();
    expect(() => p.task("Async arrow", undefined, async (rows) => ({ columns: [], rows }))).not.toThrow();
    expect(() => p.task("Named expression", undefined, function named(rows) { return { columns: [], rows }; })).not.toThrow();
    expect(() => p.task("Anonymous expression", undefined, function (rows) { return { columns: [], rows }; })).not.toThrow();
  });

  test("rejects native, bound, and object-method functions", () => {
    const p = new Pipeline("Functions");
    expect(() => p.task("Native", undefined, Math.max as never)).toThrow(/native and bound/);
    const bound = ((rows: unknown[]) => ({ columns: [], rows })).bind(null);
    expect(() => p.task("Bound", undefined, bound as never)).toThrow(/native and bound/);
    const methods = {
      get value() { return true; },
      set value(_next: boolean) {},
      *generate() { yield true; },
    };
    const valueDescriptor = Object.getOwnPropertyDescriptor(methods, "value")!;
    expect(() => functionSource(valueDescriptor.get!)).toThrow(/object methods/);
    expect(() => functionSource(valueDescriptor.set!)).toThrow(/object methods/);
    expect(() => functionSource(methods.generate)).toThrow(/object methods/);
  });

  test("gates TypeScript and streaming by exact feature names", async () => {
    const p = new Pipeline("Gate");
    const source = p.sourceFile("Input", { path: "in.csv" });
    p.filter("Filter", source, (row) => Boolean(row.ok));
    const responses = [
      new Response(JSON.stringify({ supported_ir_versions: ["2.0", "2.1"], supported_execution_features: [] })),
    ];
    const fetcher = (async () => responses.shift()!) as unknown as typeof fetch;
    const client = new Client("http://server", { fetch: fetcher });
    await expect(client.preflight(p)).rejects.toThrow(/code-streaming-emit, code-typescript/);
  });

  test("refuses TypeScript when a legacy capability response omits feature advertising", async () => {
    const p = new Pipeline("Legacy gate");
    p.task("Task", undefined, (rows) => ({ columns: [], rows }));
    const fetcher = (async () => new Response(JSON.stringify({ supported_ir_versions: ["2.0", "2.1"] }))) as unknown as typeof fetch;
    await expect(new Client("http://legacy", { fetch: fetcher }).preflight(p)).rejects.toThrow(/cannot prove compatibility/);
  });

  test("refuses an incompatible JS wrapper version", async () => {
    const p = new Pipeline("Wrapper gate");
    p.task("Task", undefined, (rows) => ({ columns: [], rows }));
    const fetcher = (async () => new Response(JSON.stringify({
      supported_ir_versions: ["2.0", "2.1"],
      supported_execution_features: ["code-typescript"],
      code_js_wrapper_version: 2,
    }))) as unknown as typeof fetch;
    await expect(new Client("http://server", { fetch: fetcher }).preflight(p)).rejects.toThrow(/require JS wrapper version 1/);
  });
});

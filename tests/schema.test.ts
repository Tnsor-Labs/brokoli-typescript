import { describe, expect, test } from "bun:test";
import { buildTaskInterface, parameter, schema } from "../src/schema";

describe("schema builders", () => {
  test("scalars build a bare kind", () => {
    expect(schema.int64()).toEqual({ kind: "int64" });
    expect(schema.float64()).toEqual({ kind: "float64" });
    expect(schema.string()).toEqual({ kind: "string" });
  });

  test("nullable wraps a type without mutating the original", () => {
    const t = schema.string();
    const n = schema.nullable(t);
    expect(n).toEqual({ kind: "string", nullable: true });
    expect(t).toEqual({ kind: "string" });
  });

  test("enum carries its values", () => {
    expect(schema.enum(["pending", "done", "failed"])).toEqual({
      kind: "enum",
      values: ["pending", "done", "failed"],
    });
  });

  test("array/map wrap an item/value type", () => {
    expect(schema.array(schema.string())).toEqual({ kind: "array", items: { kind: "string" } });
    expect(schema.map(schema.int64())).toEqual({ kind: "map", keys: "string", values: { kind: "int64" } });
  });

  test("record defaults every bare-type field to required, closed by default", () => {
    const row = schema.record({ id: schema.int64(), amount: schema.float64() });
    expect(row).toEqual({
      kind: "record",
      fields: [
        { name: "id", type: { kind: "int64" }, required: true },
        { name: "amount", type: { kind: "float64" }, required: true },
      ],
      additional_fields: false,
    });
  });

  test("record's { type, required: false } form opts a field out of required", () => {
    const row = schema.record({
      id: schema.int64(),
      label: { type: schema.string(), required: false },
    });
    const fields = (row as { fields: { name: string; required: boolean }[] }).fields;
    expect(fields.find((f) => f.name === "label")?.required).toBe(false);
  });

  test("record nests recursively", () => {
    const address = schema.record({ street: schema.string(), city: schema.string() });
    const customer = schema.record({ id: schema.int64(), address });
    const fields = (customer as { fields: { name: string; type: unknown }[] }).fields;
    expect(fields.find((f) => f.name === "address")?.type).toEqual(address);
  });

  test("nullable field inside a record stays required by default", () => {
    const row = schema.record({ name: schema.string(), nickname: schema.nullable(schema.string()) });
    const fields = (row as { fields: { name: string; type: unknown; required: boolean }[] }).fields;
    const nickname = fields.find((f) => f.name === "nickname");
    expect(nickname?.type).toEqual({ kind: "string", nullable: true });
    expect(nickname?.required).toBe(true);
  });
});

describe("parameter builders", () => {
  test("no default means required, no default key", () => {
    expect(parameter.string({ required: true })).toEqual({ type: { kind: "string" }, required: true });
  });

  test("a default implies optional and carries the default", () => {
    expect(parameter.number({ default: 0.5 })).toEqual({
      type: { kind: "float64" },
      required: false,
      default: 0.5,
    });
  });

  test("enum parameter carries its values alongside the declaration", () => {
    expect(parameter.enum(["a", "b"], { default: "a" })).toEqual({
      type: { kind: "enum", values: ["a", "b"] },
      required: false,
      default: "a",
    });
  });
});

describe("buildTaskInterface", () => {
  test("undefined on both sides yields no interface at all", () => {
    expect(buildTaskInterface()).toBeUndefined();
  });

  test("one side known, the other stays unknown", () => {
    const input = schema.record({ id: schema.int64() });
    const iface = buildTaskInterface(input);
    expect(iface?.inputs.input.value.row).toEqual(input);
    expect(iface?.outputs.result.value.row).toEqual({ kind: "unknown" });
    expect(iface?.contract).toBe("brokoli.task-interface/v1");
  });
});

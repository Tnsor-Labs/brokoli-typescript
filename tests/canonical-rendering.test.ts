// Canonical rendering matches core's cross-SDK vectors (#982), vendored
// from core's docs/schema/fixtures/ir-canonicalization/rendering.json.
// Each input is JSON text; parsed with JSON.parse and rendered canonically,
// it must give exactly the expected text, which is what the Python SDK
// produces from the same input.
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { canonicalJSON, codePointCompare } from "../src/ir";

const fixture = JSON.parse(
  readFileSync(join(import.meta.dir, "fixtures", "ir-canonicalization", "rendering.json"), "utf8"),
) as { cases: { name: string; input: string; canonical: string }[] };

describe("canonical rendering vectors", () => {
  for (const c of fixture.cases) {
    test(c.name, () => {
      expect(canonicalJSON(JSON.parse(c.input))).toBe(c.canonical);
    });
  }
});

test("integer-like keys keep code-point order, which a JavaScript object would not", () => {
  expect(canonicalJSON({ a: 1, "10": 2, "9": 3 })).toBe('{\n  "10": 2,\n  "9": 3,\n  "a": 1\n}\n');
});

test("code points, not UTF-16 units, decide the order", () => {
  expect(codePointCompare("\u{1F600}", "דּ")).toBe(1);
  expect(codePointCompare("דּ", "\u{1F600}")).toBe(-1);
  expect(codePointCompare("ab", "abc")).toBe(-1);
  expect(codePointCompare("abc", "abc")).toBe(0);
});

test("non-finite numbers are refused, not written as null", () => {
  for (const value of [NaN, Infinity, -Infinity]) {
    expect(() => canonicalJSON({ value })).toThrow("Out of range float values");
  }
});

test("matches JSON.stringify where JSON.stringify was right", () => {
  const value = { b: [1, "x", null, true, { z: 0.5, y: [] }], a: { c: {}, d: "café   \"q\"" }, u: undefined, f: [undefined] };
  const sorted = { a: { c: {}, d: "café   \"q\"" }, b: [1, "x", null, true, { y: [], z: 0.5 }], f: [undefined] };
  expect(canonicalJSON(value)).toBe(`${JSON.stringify(sorted, null, 2)}\n`);
});

import { describe, expect, test } from "bun:test";
import { bool, maybe, num, str } from "../src/cast";

describe("cast helpers", () => {
  test("num/str/bool are Number/String/Boolean", () => {
    expect(num("42")).toBe(42);
    expect(num("not a number")).toBeNaN();
    expect(str(42)).toBe("42");
    expect(str(null)).toBe("null");
    expect(bool(0)).toBe(false);
    expect(bool("0")).toBe(true);
  });

  test("maybe passes null/undefined through as undefined, casts otherwise", () => {
    expect(maybe(null, num)).toBeUndefined();
    expect(maybe(undefined, str)).toBeUndefined();
    expect(maybe("3.5", num)).toBe(3.5);
    expect(maybe(0, num)).toBe(0);
  });
});

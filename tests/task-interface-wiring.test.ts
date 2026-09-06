/**
 * Pipeline/task()-level wiring for ADR-032 rollout step 3: the schema/
 * parameter builders actually reach toJSON(), the IR version bumps
 * only when they're used, and a genuine cross-task parameter collision
 * raises rather than silently keeping whichever declaration came first.
 *
 * Pure builder behavior (schema.record/parameter.number/etc.) is
 * covered in tests/schema.test.ts.
 */

import { describe, expect, test } from "bun:test";
import { Pipeline, PipelineError } from "../src";
import { parameter, schema } from "../src/schema";

describe("task() interface/parameter wiring", () => {
  test("typed task bumps ir_version and carries node interface + pipeline parameters", () => {
    const p = new Pipeline("typed", { pipelineId: "typed" });
    const InputRow = schema.record({ id: schema.int64(), amount: schema.float64() });
    const ScoredRow = schema.record({ id: schema.int64(), score: schema.float64() });
    p.task(
      "score",
      undefined,
      (rows) => ({ columns: [], rows }),
      {
        input: InputRow,
        output: ScoredRow,
        parameters: { threshold: parameter.number({ default: 0.5 }) },
      },
    );

    const ir = p.toJSON();
    expect(ir.ir_version).toBe("2.2");
    expect(ir.parameters).toEqual({
      threshold: { type: { kind: "float64" }, required: false, default: 0.5 },
    });
    expect(ir.nodes[0]?.interface?.contract).toBe("brokoli.task-interface/v1");
    expect(ir.nodes[0]?.interface?.inputs.input.value.row).toEqual(InputRow);
  });

  test("untyped task stays at ir 2.0 with no interface or parameters keys", () => {
    const p = new Pipeline("plain", { pipelineId: "plain" });
    p.task("clean", undefined, (rows) => ({ columns: [], rows }));

    const ir = p.toJSON();
    expect(ir.ir_version).toBe("2.0");
    expect(ir.parameters).toBeUndefined();
    expect(ir.nodes[0]?.interface).toBeUndefined();
  });

  test("two tasks sharing a parameter name with the same declaration is fine", () => {
    const p = new Pipeline("shared", { pipelineId: "shared" });
    p.task("a", undefined, (rows) => ({ columns: [], rows }), {
      parameters: { threshold: parameter.number({ default: 0.5 }) },
    });
    p.task("b", undefined, (rows) => ({ columns: [], rows }), {
      parameters: { threshold: parameter.number({ default: 0.5 }) },
    });

    expect(p.toJSON().parameters).toEqual({
      threshold: { type: { kind: "float64" }, required: false, default: 0.5 },
    });
  });

  test("two tasks with conflicting parameter declarations throws", () => {
    const p = new Pipeline("collision", { pipelineId: "collision" });
    p.task("a", undefined, (rows) => ({ columns: [], rows }), {
      parameters: { threshold: parameter.number({ default: 0.5 }) },
    });

    expect(() =>
      p.task("b", undefined, (rows) => ({ columns: [], rows }), {
        parameters: { threshold: parameter.string({ required: true }) },
      }),
    ).toThrow(PipelineError);
  });
});

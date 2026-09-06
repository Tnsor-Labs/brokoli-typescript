/**
 * Cross-SDK differential conformance (ADR-032 section 14, rollout step 3).
 *
 * Loads the core-owned fixtures vendored under
 * tests/fixtures/task-interface-differential/ and asserts that a real
 * TypeScript declaration matching each vector's own `typescript` field
 * builds, via schema.ts, to exactly that vector's
 * expected_node_interface/expected_pipeline_parameters. The Python SDK
 * runs the equivalent test against the same fixture files.
 *
 * Not part of tests/differential's whole-pipeline oracle: a code/task
 * node's packaged script text is inherently language-specific (Python
 * source vs. JavaScript source) and can never render byte-identically
 * between the two SDKs, so that oracle's existing fixtures all avoid
 * code/task nodes entirely. This test compares only the interface/
 * parameters fragment schema.ts actually builds, side-stepping that
 * unrelated, permanent divergence.
 */

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { buildTaskInterface, parameter, schema } from "../src/schema";

const FIXTURE_DIR = join(import.meta.dir, "fixtures", "task-interface-differential");

function loadFixture(name: string): {
  expected_node_interface: unknown;
  expected_pipeline_parameters: unknown;
} {
  return JSON.parse(readFileSync(join(FIXTURE_DIR, `${name}.json`), "utf8"));
}

describe("differential fixtures", () => {
  test("dataset-record", () => {
    const fixture = loadFixture("dataset-record");
    const InputRow = schema.record({ id: schema.int64(), amount: schema.float64() });
    const ScoredRow = schema.record({ id: schema.int64(), score: schema.float64() });
    expect(buildTaskInterface(InputRow, ScoredRow)).toEqual(fixture.expected_node_interface as never);
  });

  test("nullable-required-field", () => {
    const fixture = loadFixture("nullable-required-field");
    const Row = schema.record({ name: schema.string(), nickname: schema.nullable(schema.string()) });
    expect(buildTaskInterface(Row, Row)).toEqual(fixture.expected_node_interface as never);
  });

  test("nested-record", () => {
    const fixture = loadFixture("nested-record");
    const Address = schema.record({ street: schema.string(), city: schema.string() });
    const Customer = schema.record({ id: schema.int64(), address: Address });
    expect(buildTaskInterface(Customer, Customer)).toEqual(fixture.expected_node_interface as never);
  });

  test("enum-field", () => {
    const fixture = loadFixture("enum-field");
    const Row = schema.record({ status: schema.enum(["pending", "done", "failed"]) });
    expect(buildTaskInterface(Row, Row)).toEqual(fixture.expected_node_interface as never);
  });

  test("required-parameter", () => {
    const fixture = loadFixture("required-parameter");
    const parameters = { region: parameter.string({ required: true }) };
    expect(parameters).toEqual(fixture.expected_pipeline_parameters as never);
  });

  test("defaulted-parameter", () => {
    const fixture = loadFixture("defaulted-parameter");
    const parameters = { threshold: parameter.number({ default: 0.5 }) };
    expect(parameters).toEqual(fixture.expected_pipeline_parameters as never);
  });
});

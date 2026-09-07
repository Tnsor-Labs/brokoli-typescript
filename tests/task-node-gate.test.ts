import { describe, expect, test } from "bun:test";
import { Client } from "../src";
import { requiredExecutionFeatures, RUNTIME_EXISTENCE_FEATURES } from "../src/ir";

// ADR-033's "task" node type and ADR-032's named edge ports. Neither is
// emitted by this SDK's authoring API yet -- p.task() compiles to a
// `code` node with language "typescript", a decorator-style node that
// has nothing to do with the ADR-033 runtime. These gates therefore
// exist for IR that was hand-assembled or round-tripped through the
// server, and they go live unchanged when the authoring API lands.
//
// The IR is built literally rather than through Pipeline for exactly
// that reason: no builder call can produce these shapes today.
function ir(overrides: Record<string, unknown>) {
  return {
    name: "t",
    ir_version: "2.0",
    nodes: [],
    edges: [],
    ...overrides,
  } as never;
}

const taskNode = {
  id: "n1",
  type: "task",
  name: "N",
  config: { task_bundle: { digest: `sha256:${"0".repeat(64)}`, format: "task-bundle/2" } },
};

const codeNode = { id: "n1", type: "code", name: "N", config: { language: "python", script: "pass" } };

describe("task-node and port feature gates", () => {
  test("a task node requires task-runtime-v1 and task-bundle-v2", () => {
    // task_bundle is mandatory on a task node (optional on a code node),
    // so the two features always travel together.
    expect(requiredExecutionFeatures(ir({ nodes: [taskNode] }))).toEqual([
      "task-bundle-v2",
      "task-runtime-v1",
    ]);
  });

  test("a task node does not claim the v1 task-bundles feature", () => {
    // task-bundles (ADR-031, task-bundle/1) and task-bundle-v2 name
    // different mount mechanisms. A server advertising only the older one
    // cannot run a task node.
    expect(requiredExecutionFeatures(ir({ nodes: [taskNode] }))).not.toContain("task-bundles");
  });

  test("a ported edge requires task-ports-v1", () => {
    const features = requiredExecutionFeatures(
      ir({ nodes: [codeNode], edges: [{ from: "n1", to: "n1", from_port: "rejected" }] }),
    );
    expect(features).toContain("task-ports-v1");
  });

  test("an unported edge stays ungated", () => {
    // The gate keys on a port actually being named -- an ordinary
    // pipeline must not suddenly require a feature no server advertises.
    const features = requiredExecutionFeatures(
      ir({ nodes: [codeNode], edges: [{ from: "n1", to: "n1" }] }),
    );
    expect(features).not.toContain("task-ports-v1");
  });

  test("these are runtime-existence features, so a legacy server is refused", async () => {
    // A server old enough to omit supported_execution_features entirely
    // cannot have a task-runtime harness. Without this the deploy would
    // PASS preflight and then be hard-refused by the server's validator
    // -- the "deployed, then fails at run time" case the gate exists to
    // prevent.
    expect(RUNTIME_EXISTENCE_FEATURES).toContain("task-runtime-v1");
    expect(RUNTIME_EXISTENCE_FEATURES).toContain("task-bundle-v2");

    const fetcher = (async () =>
      new Response(JSON.stringify({ supported_ir_versions: ["2.0", "2.1"] }))) as unknown as typeof fetch;
    const client = new Client("http://legacy", { fetch: fetcher });
    await expect(
      client.preflight({ toJSON: () => ir({ nodes: [taskNode] }) } as never),
    ).rejects.toThrow(/task-runtime-v1/);
  });

  test("a feature-advertising server missing the feature is refused by name", async () => {
    const fetcher = (async () =>
      new Response(
        JSON.stringify({
          supported_ir_versions: ["2.0", "2.1"],
          supported_execution_features: ["conditional-routing"],
        }),
      )) as unknown as typeof fetch;
    const client = new Client("http://old", { fetch: fetcher });
    await expect(
      client.preflight({ toJSON: () => ir({ nodes: [taskNode] }) } as never),
    ).rejects.toThrow(/task-bundle-v2, task-runtime-v1/);
  });
});

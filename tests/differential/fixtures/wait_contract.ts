import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const pipeline = new Pipeline("Wait and contract", { pipelineId: "wait-contract" });
  const ready = pipeline.wait("Export landed", {
    condition: "file_exists",
    path: "/data/exports/*.csv",
    pollInterval: 300,
    timeout: "4h",
    offset: 0.5,
    nodeKey: "export-landed",
  });
  const source = pipeline.sourceFile("Read export", { path: "/data/exports/orders.csv" });
  pipeline.edge(ready, source);
  const passed = pipeline.wait("Upstream done", { condition: "pipeline", pipelineId: "nightly", input: source });
  pipeline.contractGate("Orders contract", passed, {
    contract: {
      ir_version: "1.0",
      contract: { id: "orders", version: "1" },
      input: { kind: "record-stream" },
      rules: [
        {
          id: "email-valid",
          kind: "record",
          path: "$.email",
          predicate: { op: "format", format: "email" },
          on_breach: { severity: "error", action: "quarantine" },
        },
      ],
    },
    retries: 1,
  });
  return pipeline;
}

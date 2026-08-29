import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const p = new Pipeline("Guarded Load", { pipelineId: "guarded-load" });
  const a = p.sourceFile("Read A", { path: "/data/a.csv", format: "csv" });
  const b = p.sourceFile("Read B", { path: "/data/b.csv", format: "csv" });
  const merged = p.union("Merge", a, b);
  const gate = p.conditionNode("Any Rows", merged, "row_count > 0");
  const shaped = gate.when(p.transform("Shape", undefined, { rules: [{ type: "rename", mapping: { x: "y" } }] }));
  shaped.then(p.sinkFile("Load", undefined, { path: "/data/out.csv", format: "csv" }));
  gate.otherwise(p.notify("Nothing", undefined, { notifyType: "webhook", webhookUrl: "https://hooks.example/x" }));
  return p;
}

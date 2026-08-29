import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const p = new Pipeline("Hourly Slices", {
    pipelineId: "hourly-slices",
    schedule: "0 * * * *",
    scheduleTimezone: "Africa/Maputo",
    catchUp: true,
    sla: "07:30 Africa/Maputo",
    dependsOn: ["upstream-etl"],
    tags: ["intervals"],
    webhook: true,
    hooks: { on_failure: "https://hooks.example/oncall" },
  });
  const rows = p.sourceFile("Read", { path: "/data/in.csv", format: "csv" });
  rows.then(p.sinkFile("Slice", undefined, { path: "/data/slice-${interval.start}.csv", format: "csv" }));
  return p;
}

import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const p = new Pipeline("Sales ETL", {
    pipelineId: "sales-etl",
    description: "Clean the daily sales file",
    tags: ["finance", "daily"],
  });
  const rows = p.sourceFile("Read", { path: "/data/sales.csv", format: "csv" });
  const clean = rows.then(
    p.transform("Clean", undefined, {
      rules: [
        { type: "rename", mapping: { amt: "amount" } },
        { type: "filter", condition: "amount > 0" },
      ],
    }),
  );
  clean.then(p.sinkFile("Write", undefined, { path: "/data/out.csv", format: "csv" }));
  return p;
}

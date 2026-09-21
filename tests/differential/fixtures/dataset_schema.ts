import { datasetSchema, schema } from "../../../src/schema";
import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const p = new Pipeline("Dataset Schema", { pipelineId: "dataset-schema" });
  p.sourceApi("Fetch", {
    url: "https://example.test/data",
    schema: datasetSchema(
      { id: schema.int64(), name: schema.string({ nullable: true }) },
      { additionalColumns: "closed" },
    ),
  });
  return p;
}

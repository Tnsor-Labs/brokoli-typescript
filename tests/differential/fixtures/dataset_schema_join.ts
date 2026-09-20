import { datasetSchema, schema } from "../../../src/schema";
import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const p = new Pipeline("Dataset Schema Join", { pipelineId: "dataset-schema-join" });
  const left = p.sourceApi("Left", {
    url: "https://example.test/left",
    schema: datasetSchema({ id: schema.int64(), name: schema.string() }, { additionalColumns: "closed" }),
  });
  const right = p.sourceApi("Right", {
    url: "https://example.test/right",
    schema: datasetSchema({ id: schema.int64(), name: schema.string() }, { additionalColumns: "closed" }),
  });
  p.join("Merge", left, right, { on: "id", collisionPolicy: "alias", rightAlias: "right_row" });
  return p;
}

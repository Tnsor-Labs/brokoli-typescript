import { Pipeline } from "../../../src/pipeline";

export function build(): Pipeline {
  const p = new Pipeline("Join Collision Policy", { pipelineId: "join-collision-policy" });
  const left = p.sourceDb("Left", { connId: "warehouse", query: "SELECT id, name FROM left_table" });
  const right = p.sourceDb("Right", { connId: "warehouse", query: "SELECT id, name FROM right_table" });
  p.join("Merge", left, right, { on: "id", collisionPolicy: "alias", rightAlias: "right_row" });
  return p;
}

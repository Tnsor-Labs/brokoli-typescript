import { Pipeline } from "../../../src/pipeline";
import { offsetPages } from "../../../src/pagination";
import { Param, Secret } from "../../../src/resources";

export function build(): Pipeline {
  const p = new Pipeline("Item Sync", { pipelineId: "item-sync" });
  const items = p.sourceApi("Fetch", {
    url: "https://api.example.com/items",
    headers: { "X-Api-Key": new Secret("items_key") },
    params: { since: new Param("since"), status: "active" },
    records: "data.items",
    timeout: 30,
    retries: 3,
    pagination: offsetPages(100).withExecution({ page_max_retries: 3 }),
  });
  items.then(p.sinkApi("Push", undefined, { url: "https://sink.example.com/ingest", method: "POST" }));
  return p;
}

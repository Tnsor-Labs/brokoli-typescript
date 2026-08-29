# Brokoli TypeScript SDK

Bun-first TypeScript compiler for Brokoli's declarative pipeline IR. It is intentionally a compiler, not a runtime bridge: code-node bodies remain Python/server-side until the server advertises a Node runtime.

```ts
import { Pipeline } from "brokoli";

const pipeline = new Pipeline("Orders", { pipelineId: "orders" });
const raw = pipeline.sourceFile("Read orders", { path: "orders.csv" });
const clean = pipeline.transform("Clean", raw, { rules: [{ type: "drop_columns", columns: ["debug"] }] });
pipeline.sinkFile("Export", clean, { path: "orders.json", format: "json" });
console.log(pipeline.toJSON());
```

Run `bun test` and `bun run typecheck` from this directory.

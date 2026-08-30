# Production Example

`order-ingestion.ts` is a production-shaped, TypeScript-authored pipeline. It
uses a cursor-paginated API source with persisted page checkpoints, retries,
timeouts, normalization, validation, quarantine routing, enrichment, quality
checks, and idempotent warehouse upserts.

The API and warehouse credentials are referenced by server-managed connection
IDs. They are not embedded in the pipeline definition.

```ts
import { Client } from "../src/index";
import { createOrderIngestionPipeline } from "./order-ingestion";

const pipeline = createOrderIngestionPipeline({
  ordersUrl: process.env.ORDERS_URL!,
  ordersConnection: "orders-api",
  warehouseConnection: "analytics-warehouse",
});

const client = new Client(process.env.BROKOLI_SERVER!, {
  username: process.env.BROKOLI_USERNAME!,
  password: process.env.BROKOLI_PASSWORD!,
});
await client.deploy(pipeline);
```

Before production use, configure the referenced connections, webhook variable,
API response shape, and warehouse permissions in the target Brokoli server.

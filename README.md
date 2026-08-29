# Brokoli TypeScript SDK

Bun-first TypeScript compiler and client for Brokoli's declarative
pipeline IR. It is intentionally a compiler, not a runtime bridge:
code-node bodies remain Python/server-side until the server advertises a
Node runtime. Everything the compiler emits is byte-identical to what
the Python SDK emits for the same pipeline — enforced by a differential
oracle, not by convention.

## Authoring

```ts
import { Pipeline } from "brokoli";

const pipeline = new Pipeline("Orders", { pipelineId: "orders" });
const raw = pipeline.sourceFile("Read orders", { path: "orders.csv" });
const clean = pipeline.transform("Clean", raw, {
  rules: [{ type: "drop_columns", columns: ["debug"] }],
});
pipeline.sinkFile("Export", clean, { path: "orders.json", format: "json" });
console.log(pipeline.toJSON());
```

Branching routes explicitly — `gate.when(target)` marks the condition
edge, then chain from the target:

```ts
const gate = pipeline.conditionNode("Any rows", merged, "row_count > 0");
const shaped = pipeline.transform("Shape", undefined, { rules });
gate.when(shaped);
shaped.then(pipeline.sinkFile("Load", undefined, { path: "out.csv" }));
gate.otherwise(pipeline.notify("Nothing", undefined, { notifyType: "webhook", webhookUrl }));
```

## Operating

```ts
import { Client } from "brokoli";

const client = await Client.fromEnv("http://localhost:8090");
await client.deploy(pipeline); // capability preflight + create-or-update
const run = await client.run("orders");
console.log(await run.wait());
```

`Client.fromEnv` resolves credentials from `BROKOLI_TOKEN`, then from
the credentials file shared with the Python SDK
(`~/.config/brokoli/credentials.json`, `{"servers": {url: token}}`,
XDG- and `BROKOLI_CREDENTIALS`-aware) — a `brokoli auth` login from
either SDK works in both.

## Module map

| Module | Contents |
| --- | --- |
| `src/pipeline.ts` | `Pipeline`, node factories, typed refs, branching |
| `src/ir.ts` | IR types, normalization, canonical rendering, digests, diffs |
| `src/client.ts` | `Client`, `Run`, deploy/run/backfill/retry, capability preflight |
| `src/resources.ts` | `Secret`/`Variable`/`Param`/`EnvVar`/`Connection` interpolation refs |
| `src/pagination.ts` | API pagination strategies |
| `src/credentials.ts` | the Python-compatible shared credentials store |
| `src/validate.ts` | client-side pipeline validation |
| `src/cli.ts` | `compile`/`validate`/`deploy`/`diff`/`run`/`status`/... over IR files |
| `src/errors.ts` | `PipelineError`, `APIError` |

## Parity with the Python SDK

The canonical-form contract both SDKs implement lives in the core repo
at `docs/schema/ir-canonicalization.md`: normalization steps, canonical
JSON rendering, `sha256:` digests, node-id allocation, schema hints.
Two SDKs producing the same digest for the same pipeline is the whole
point of the IR.

The differential oracle in `tests/differential/` enforces it. Each
fixture exists twice — a `.py` that prints the Python SDK's rendered IR
and digest, and a `.ts` that builds the same pipeline here — and the
runner compares both byte-for-byte:

```sh
BROKOLI_PYTHON=/path/to/venv/bin/python bun test tests/differential
```

The oracle fails loudly when `BROKOLI_PYTHON` is unset (set
`BROKOLI_DIFFERENTIAL=skip` to opt out explicitly). It has already paid
for itself: its first run caught a node-id divergence (`read_a_1` vs
`reada_1`) and a hook-normalization gap, and exposed a branching footgun
in the Python SDK itself.

## Development

```sh
bun install
bun run typecheck
bun test        # unit + differential (differential needs BROKOLI_PYTHON)
```

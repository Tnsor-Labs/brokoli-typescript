# Brokoli TypeScript SDK

Bun-first TypeScript compiler and client for Brokoli's declarative
pipeline IR. TypeScript code nodes are capability-gated: the SDK emits
them only as `language: "typescript"` and deployment requires a server
advertising `code-typescript`. Everything the compiler emits is byte-identical to what
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

Self-contained functions can be authored as TypeScript code nodes. The
function is serialized into the JS wrapper's fixed namespace; v1 does
not package closure captures or imports:

```ts
const shaped = pipeline.task("Shape", raw, (rows) => ({
  columns: ["id", "total"],
  rows: rows.map((row) => ({ id: row.id, total: Number(row.amt) * 2 })),
}));
```

`source`, `sink`, `filter`, `map`, `validate`, and `sensor` provide the
Python decorator-equivalent surface as ordinary higher-order methods.
`filter` and `map` generate against `rowsStream()` plus
`begin_emit()`/`emit()` so they do not materialize the input. Raw code
strings may use the wrapper names `rows`, `rowsStream`, `columns`,
`config`, `params`, `emit`, `begin_emit`, `sleep`, and `output_data`; the
names remain snake_case because they are a cross-language protocol
contract.

TypeScript and streaming code nodes fail closed when a server omits
`supported_execution_features`; such a server cannot prove support for
`code-typescript` or `code-streaming-emit`. This intentionally differs
from Python SDK 0.8.0, which still permits streaming emit against legacy
capability responses. No released server currently advertises these
features, so TypeScript code-node deployment remains release-ordered
behind the core runtime. Wrapper contract v1 is also matched exactly;
compatible-version ranges can replace that conservative check when a v2
contract exists.

The `sensor` generator sleeps with `sleep(ms)`, the host-implemented
namespace function ADR-030 settled on (`sleep` resolves after `ms`,
clamped to >= 0; the Go-side exec timeout still governs). `Atomics` and
`SharedArrayBuffer` are explicitly not part of the wrapper contract and
are not exposed to user scripts.

Branching routes explicitly — `gate.when(target)` marks the condition
edge, then chain from the target:

```ts
const gate = pipeline.conditionNode("Any rows", merged, "row_count > 0");
const shaped = pipeline.transform("Shape", undefined, { rules });
gate.when(shaped);
shaped.then(pipeline.sinkFile("Load", undefined, { path: "out.csv" }));
gate.otherwise(pipeline.notify("Nothing", undefined, { notifyType: "webhook", webhookUrl }));
```

### Authoring sugar (ADR-034)

`pipe()` builds a strictly linear pipeline as one expression, threading
each step's return value into the next — for the common case where a
pipeline genuinely is one chain. It expands to ordinary builder calls
before compilation, so emitted IR is byte-identical to the equivalent
hand-written `.then()` chain:

```ts
import { pipe } from "brokoli";

const pipeline = pipe(
  "Quarterly Revenue", { pipelineId: "qrv-1" },
  (p) => p.sourceDb("Load Orders", { connId: "warehouse", query: "SELECT ..." }),
  (p, orders) => p.task("Revenue Score", orders, (rows) => ({ columns: [], rows: [] })),
  (p, scored) => p.sinkDb("Save", scored, { connId: "warehouse", table: "fact_revenue" }),
);
```

Non-linear shapes (branch, union, fan-out) need more than one upstream
ref per step and don't fit `pipe`'s signature by construction — use the
`Pipeline` builder directly for those.

`task(fn)` and `map(fn)` derive the node's display name from the
function's own declared name (`normalizeOrders` -> `"Normalize Orders"`,
matching the Python SDK's `func.__name__.replace("_", " ").title()`).
Only function declarations and named function expressions qualify — an
arrow function or a minified build still needs the explicit `name`
argument, which is otherwise unaffected:

```ts
function normalizeOrders(rows: Row[]) { /* ... */ }
pipeline.task(normalizeOrders, raw); // node name: "Normalize Orders"
```

`num`/`str`/`bool`/`maybe` are small `Number`/`String`/`Boolean`
coercions for **authoring-time** TypeScript only — never inside a
`task`/`map`/`filter`/`sink`/`source`/`validate`/`sensor` body. Those
bodies are serialized with no access to outer scope (v1: self-contained
functions only), so calling `str(...)` from inside one ships a dangling
reference the worker cannot resolve. Inside a task body, keep using
`Number(...)`/`String(...)` directly.

A `helpers` option on `task`/`source`/`sink`/`filter`/`map`/`validate`/
`sensor`/`code` solves the same problem `num`/`str`/`bool` cannot: it
captures module-level functions and constants a task body actually
calls, by serializing each one once into a `const` preamble ahead of
the body — rewriting happens here, at authoring time, so the worker
still receives one opaque script and emitted IR is unaffected:

```ts
const TAX_RATE = 0.21;
function grossMargin(revenue: number, cost: number) {
  return (revenue - cost) * (1 - TAX_RATE);
}

pipeline.task("Revenue Score", raw, (rows) => ({
  columns: ["id", "margin"],
  rows: rows.map((row) => ({ id: row.id, margin: grossMargin(row.revenue, row.cost) })),
}), { helpers: { grossMargin, TAX_RATE } });
```

A helper name colliding with the fixed wrapper namespace, an invalid
identifier, or a value that is neither a function nor JSON-serializable
is a `PipelineError` at authoring time. What this does **not** catch: a
helper function that itself closes over some other module-scope
variable not present in the same `helpers` map — detecting that needs
real AST inspection, which this SDK's "no bytecode inspection" v1
contract does not have, so it still surfaces as a remote
`ReferenceError` inside the worker rather than a local error.

### Waits, contracts, shell commands and migrations

`wait` parks a run until a condition holds without holding a concurrency
slot; the server re-checks every `pollInterval` (default 30s) and fails
the node after `timeout` (default 6h). Durations are Go duration strings
or seconds, and any `input` passes through:

```ts
const ready = pipeline.wait("Export landed", { condition: "file_exists", path: "/data/exports/*.csv", pollInterval: "5m", timeout: "4h" });
pipeline.edge(ready, pipeline.sourceFile("Read export", { path: "/data/exports/orders.csv" }));
pipeline.wait("Upstream done", { condition: "pipeline", pipelineId: "nightly-load" });
```

`contractGate` enforces an actually-fine contract: cleared records flow
on, quarantined ones are dropped, and a `reject`/`halt` breach fails the
node. Pass the contract IR, or actually-fine's TypeScript `Contract`:

```ts
const clean = pipeline.contractGate("Orders contract", orders, { contract: ordersContract });
```

`bash` runs a trusted command as the worker's OS user (not a sandbox).
Platform references such as `${param.day}` are refused inside `command`;
pass them through `env` and quote the variable:

```ts
pipeline.bash("Refresh", clean, { command: './refresh.sh "$DAY"', env: { DAY: "${param.day}" } });
```

`migrate` takes the full option set (`keyColumns`, `sourceConnId`,
`targetConnId`, `dialect`, `chunkSize`, `createTable`), and `partition`
splits the copy into ranges copied in parallel, each retried on its own.
A partitioned copy must be an upsert whose `keyColumns` include the
partition column, into a table that already exists:

```ts
pipeline.migrate("Copy events", {
  sourceConnId: "oltp", targetConnId: "dwh", query: "SELECT * FROM events", table: "events",
  mode: "upsert", keyColumns: ["id"],
  partition: { column: "id", strategy: "numeric", boundaries: [0, 1_000_000, 2_000_000], maxParallel: 4 },
});
```

Every node that reads or writes data takes `retries`, `retryBackoff`,
`retryDelay` (milliseconds), `timeout` (seconds) and `pool`, which puts
it in a named concurrency pool on the server (without it, the pool of
its `connId`). `dbt` takes `vars` as an object (sent as compact JSON
with sorted keys, the string the server reads), `connId`,
`targetSchema`, `outputModel` and an `input`.

## Operating

```ts
import { Client } from "brokoli";

const client = await Client.fromEnv("http://localhost:8090");
await client.deploy(pipeline); // capability preflight + create-or-update
const run = await client.run("orders");
console.log(await run.wait());
```

`deploy` refuses, before saving anything, a node type the server does
not know or has disabled (its `/api/capabilities` reports both).

```ts
await client.dryRun("orders");                                   // up to 10 rows per node; no run recorded
const again = await run.resume({ fromNode: "load" });            // a NEW run; omit fromNode to restart at the failure
await client.backfill("orders", { start: "2026-10-01", end: "2026-10-07" });
```

A dry run really executes the nodes, so a sink still writes. `backfill`
takes bare dates as whole days (end inclusive), RFC3339 strings or
`Date`s as instants, and `force: true` to re-run succeeded intervals or
backfill a pipeline that never references its interval.

On a multi-workspace server, `new Client(url, { workspace })`,
`BROKOLI_WORKSPACE` or the CLI's `--workspace` sends `X-Workspace-ID` on
every request; without it the server uses your default workspace.

`Client.fromEnv` resolves credentials from `BROKOLI_TOKEN`, then from
the credentials file shared with the Python SDK
(`~/.config/brokoli/credentials.json`, `{"servers": {url: token}}`,
XDG- and `BROKOLI_CREDENTIALS`-aware) — a `brokoli auth` login from
either SDK works in both.

`brokoli auth --server <url>` uses device authorization and stores the
result in that shared file. Username/password remains available through
`--username` and `--password`; `--no-browser` prints the confirmation URL
without opening it.

## Module map

| Module | Contents |
| --- | --- |
| `src/pipeline.ts` | `Pipeline`, node factories, typed refs, branching |
| `src/pipe.ts` | `pipe()` — linear-DAG authoring sugar (ADR-034) |
| `src/cast.ts` | `num`/`str`/`bool`/`maybe` — authoring-time scalar coercions (ADR-034) |
| `src/ir.ts` | IR types, normalization, canonical rendering, digests, diffs |
| `src/client.ts` | `Client`, `Run`, deploy/run/backfill/retry, capability preflight |
| `src/resources.ts` | `Secret`/`Variable`/`Param`/`EnvVar`/`Connection` interpolation refs |
| `src/pagination.ts` | API pagination strategies |
| `src/credentials.ts` | the Python-compatible shared credentials store |
| `src/validate.ts` | client-side pipeline validation |
| `src/cli.ts` | `compile`/`validate`/`deploy`/`diff`/`run`/`status`/... over IR files |
| `src/errors.ts` | `PipelineError`, `APIError` |
| `src/code.ts` | TypeScript function serialization and generated wrapper scripts |
| `src/device.ts` | device authorization and polling |
| `src/testing.ts` | graph assertions, run snapshots, watch, live harness |
| `schema/pipeline-ir-2.2.json`, `schema/task-interface-v1.json` | canonical IR schemas copied from core v0.18.12, used before semantic validation; `bun run check:schema` fails if they differ from that commit and warns if core has moved on |

`expectGraph(pipeline).hasEdge(from, to)` accepts any edge condition;
pass `true` or `false` as the third argument to assert a specific branch.
`watch()` ignores runs started before it was called by default. Pass an
explicit `after` timestamp when coordinating against an earlier trigger.

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

## Integration tests

`tests/integration/` exercises the client end to end against a live
server: capabilities, deploy (create and update-in-place), run/wait,
logs, node runs, cancel, retry/resume, interval-native backfill,
run-scoped node previews, dry runs, and a wait, contract gate and bash
pipeline end to end. The suite activates when `BROKOLI_SERVER` is
set and skips otherwise; its file-node fixtures assume the server
shares the local filesystem (a dev/demo instance):

```sh
BROKOLI_SERVER=http://localhost:8090 \
BROKOLI_USERNAME=admin BROKOLI_PASSWORD=... bun test tests/integration
```

Every pipeline it deploys carries a unique `ts-itest-*` id and is
deleted afterwards. Credentials also resolve from `BROKOLI_TOKEN` or
the shared credentials file when username/password are not set.

## Development

```sh
bun install
bun run typecheck
bun run check:schema
bun test        # unit + differential (differential needs BROKOLI_PYTHON)
```

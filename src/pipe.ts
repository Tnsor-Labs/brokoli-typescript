import { Pipeline, type PipelineOptions } from "./pipeline";
import type { NodeRef } from "./pipeline";

/**
 * Build a strictly linear pipeline as one expression: `source -> task ->
 * sink -> ...` in argument order, each step's return value threaded as
 * the next step's `prev` argument. This restores the "a >> b >> c"
 * ergonomics Python gets from operator chaining, for the common case
 * where a pipeline genuinely is one chain.
 *
 * Every step still calls the ordinary builder methods on the `pipeline`
 * it receives — `pipeline.sourceDb(...)`, `pipeline.task(...)`, and so
 * on — so the emitted IR is byte-identical to writing the chain out by
 * hand with `.then(...)`. `pipe` only removes the repeated variable
 * bindings.
 *
 * Non-linear shapes need more than one upstream ref per step (branch,
 * union, fan-out) and don't fit this signature by construction — use the
 * `Pipeline` builder directly for those, exactly as ADR-034 intends.
 *
 * ```ts
 * const p = pipe("Quarterly Revenue", { pipelineId: "qrv-1" },
 *   (pipeline) => pipeline.sourceDb("Load Orders", { connId: "warehouse", query: "..." }),
 *   (pipeline, orders) => pipeline.task("Revenue Score", orders, (rows) => ({ columns: [...], rows: [...] })),
 *   (pipeline, scored) => pipeline.sinkDb("Save", scored, { table: "fact_revenue", connId: "warehouse" }),
 * );
 * ```
 */
export function pipe(
  name: string,
  options: PipelineOptions,
  first: (pipeline: Pipeline) => NodeRef,
  ...rest: Array<(pipeline: Pipeline, prev: NodeRef) => NodeRef>
): Pipeline {
  const pipeline = new Pipeline(name, options);
  let prev = first(pipeline);
  for (const step of rest) {
    prev = step(pipeline, prev);
  }
  return pipeline;
}

#!/usr/bin/env bun
import { readFile } from "node:fs/promises";
import { canonicalJSON, diffIR, irDigest, normalizeIR, validatePipeline, Pipeline } from "./index";

const [command, file = "pipeline.json"] = Bun.argv.slice(2);
if (!command || !["compile", "validate", "diff"].includes(command)) { console.error("Usage: brokoli compile|validate|diff <pipeline.json>"); process.exit(2); }
const ir = JSON.parse(await readFile(file, "utf8"));
if (command === "compile") { console.log(canonicalJSON(normalizeIR(ir))); console.error(await irDigest(ir)); }
else if (command === "diff") console.log(diffIR(ir));
else { const result = validatePipeline(Object.assign(new Pipeline(ir.name), { toJSON: () => ir })); for (const issue of [...result.errors, ...result.warnings]) console.error(`[${issue.severity}] ${issue.nodeName}: ${issue.message}`); process.exit(result.valid ? 0 : 1); }

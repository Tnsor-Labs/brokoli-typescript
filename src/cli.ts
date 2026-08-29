#!/usr/bin/env bun
/**
 * The SDK CLI. Operates on compiled IR files (JSON) and a Brokoli
 * server; auth resolution mirrors the Python CLI: --api-key beats
 * BROKOLI_TOKEN beats the shared credentials file.
 */

import { readFile } from "node:fs/promises";
import { Client, login } from "./client";
import { canonicalJSON, irDigest, normalizeIR } from "./ir";
import type { PipelineIR } from "./ir";
import { diffIR } from "./ir";
import { Pipeline } from "./pipeline";
import { validatePipeline } from "./validate";

const argv = Bun.argv.slice(2);
const command = argv.shift();

function flag(name: string, fallback = ""): string {
  const index = argv.indexOf(name);
  if (index < 0) return fallback;
  const value = argv[index + 1];
  argv.splice(index, value?.startsWith("-") ? 1 : 2);
  return value?.startsWith("-") ? fallback : value || fallback;
}

function usage(): void {
  console.error("Usage: brokoli compile|validate|deploy|diff|run|status|logs|cancel|retry|backfill|auth ...");
}

if (!command) {
  usage();
  process.exit(2);
}

const server = flag("--server", process.env.BROKOLI_SERVER || "http://localhost:8080");
const apiKey = flag("--api-key", process.env.BROKOLI_TOKEN);

async function client(): Promise<Client> {
  return apiKey ? new Client(server, { apiKey }) : Client.fromEnv(server);
}

async function readIR(file: string): Promise<PipelineIR> {
  return JSON.parse(await readFile(file, "utf8"));
}

/** Wrap a compiled IR file so client methods that take a Pipeline can
 * operate on it without re-authoring. */
function asPipeline(ir: PipelineIR): Pipeline {
  const pipeline = new Pipeline(ir.name || "pipeline", { pipelineId: ir.pipeline_id });
  pipeline.toJSON = () => ir;
  return pipeline;
}

try {
  if (command === "auth") {
    const username = flag("--username");
    const password = flag("--password");
    if (!username || !password) throw new Error("auth requires --username and --password");
    await login(server, username, password);
    console.log(`Token stored for ${server}`);
  } else if (command === "compile" || command === "validate" || command === "deploy" || command === "diff") {
    const file = argv[0] || "pipeline.json";
    const ir = await readIR(file);
    if (command === "compile") {
      console.log(canonicalJSON(normalizeIR(ir)));
      console.error(irDigest(ir));
    } else if (command === "validate") {
      const result = validatePipeline(asPipeline(ir));
      for (const issue of [...result.errors, ...result.warnings]) {
        console.error(`[${issue.severity}] ${issue.nodeName}: ${issue.message}`);
      }
      process.exit(result.valid ? 0 : 1);
    } else if (command === "deploy") {
      console.log(JSON.stringify(await (await client()).deploy(asPipeline(ir)), null, 2));
    } else {
      const api = await client();
      const remote = await api.pipeline(ir.pipeline_id || ir.name);
      console.log(diffIR(ir, await api.request(`/api/pipelines/${encodeURIComponent(remote.id)}`)));
    }
  } else if (command === "run") {
    const api = await client();
    const params = Object.fromEntries(
      argv.filter((x) => x.startsWith("--param=")).map((x) => x.slice(8).split("=", 2)),
    );
    console.log((await api.run(argv[0], Object.keys(params).length ? params : undefined)).id);
  } else if (command === "status") {
    console.log(JSON.stringify(await (await client()).runHandle(argv[0]).detail(), null, 2));
  } else if (command === "logs") {
    console.log(JSON.stringify(await (await client()).runHandle(argv[0]).logs(), null, 2));
  } else if (command === "cancel") {
    console.log(JSON.stringify(await (await client()).runHandle(argv[0]).cancel(), null, 2));
  } else if (command === "retry") {
    console.log(JSON.stringify(await (await client()).retry(argv[0]), null, 2));
  } else if (command === "backfill") {
    const start = flag("--start");
    const end = flag("--end");
    if (!start || !end) throw new Error("backfill requires --start and --end");
    console.log(JSON.stringify(await (await client()).backfill(argv[0], { start, end }), null, 2));
  } else {
    usage();
    process.exit(2);
  }
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}

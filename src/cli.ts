#!/usr/bin/env bun
import { readFile } from "node:fs/promises";
import { canonicalJSON, Client, diffIR, irDigest, loadCredentials, login, normalizeIR, Pipeline, validatePipeline } from "./index";

const argv = Bun.argv.slice(2);
const command = argv.shift();
const flag = (name: string, fallback = "") => { const index = argv.indexOf(name); if (index < 0) return fallback; const value = argv[index + 1]; argv.splice(index, value?.startsWith("-") ? 1 : 2); return value?.startsWith("-") ? fallback : value || fallback; };
const usage = () => console.error("Usage: brokoli compile|validate|deploy|diff|run|status|logs|cancel|retry|backfill|auth ...");
if (!command) { usage(); process.exit(2); }

const server = flag("--server", process.env.BROKOLI_SERVER || "http://localhost:8090");
const token = flag("--api-key", process.env.BROKOLI_TOKEN);
const client = async () => token ? new Client(server, { apiKey: token }) : Client.fromEnv(server);
const readIR = async (file: string) => JSON.parse(await readFile(file, "utf8"));
const asPipeline = (ir: any) => Object.assign(new Pipeline(ir.name || "pipeline", { pipelineId: ir.pipeline_id }), { toJSON: () => ir });

try {
  if (command === "auth") {
    const username = flag("--username"); const password = flag("--password");
    if (!username || !password) throw new Error("auth requires --username and --password");
    console.log(JSON.stringify(await login(server, username, password), null, 2));
  } else if (["compile", "validate", "deploy", "diff"].includes(command)) {
    const file = argv[0] || "pipeline.json"; const ir = await readIR(file);
    if (command === "compile") { console.log(canonicalJSON(normalizeIR(ir))); console.error(irDigest(ir)); }
    else if (command === "validate") { const result = validatePipeline(asPipeline(ir)); for (const issue of [...result.errors, ...result.warnings]) console.error(`[${issue.severity}] ${issue.nodeName}: ${issue.message}`); process.exit(result.valid ? 0 : 1); }
    else if (command === "deploy") console.log(JSON.stringify(await (await client()).deploy(asPipeline(ir)), null, 2));
    else { const api = await client(); const remote = await api.pipeline(ir.pipeline_id || ir.name); console.log(diffIR(ir, await api.request(`/api/pipelines/${encodeURIComponent(remote.id)}`))); }
  } else if (command === "run") { const api = await client(); const params = Object.fromEntries(argv.filter(x => x.startsWith("--param=")).map(x => x.slice(8).split("=", 2))); console.log((await api.run(argv[0], params)).id); }
  else if (command === "status") console.log(JSON.stringify(await (await client()).runHandle(argv[0]).detail(), null, 2));
  else if (command === "logs") console.log(JSON.stringify(await (await client()).runHandle(argv[0]).logs(), null, 2));
  else if (command === "cancel") console.log(JSON.stringify(await (await client()).runHandle(argv[0]).cancel(), null, 2));
  else if (command === "retry") console.log(JSON.stringify(await (await client()).retry(argv[0]), null, 2));
  else if (command === "backfill") { const api = await client(); console.log(JSON.stringify(await api.backfill(argv[0], { startDate: flag("--start"), endDate: flag("--end") }), null, 2)); }
  else { usage(); process.exit(2); }
} catch (error) { console.error(error instanceof Error ? error.message : error); process.exit(1); }

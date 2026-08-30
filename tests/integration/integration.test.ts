import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client } from "../../src/client";
import { Pipeline } from "../../src/pipeline";

/**
 * Authenticated integration tests against a live Brokoli server.
 *
 * Enabled by BROKOLI_SERVER; credentials come from BROKOLI_TOKEN, the
 * shared credentials file, or BROKOLI_USERNAME/BROKOLI_PASSWORD. The
 * file-node fixtures assume the server shares this machine's
 * filesystem (a local dev/demo instance), which is exactly the setup
 * these tests exist for:
 *
 *   BROKOLI_SERVER=http://localhost:8090 \
 *   BROKOLI_USERNAME=admin BROKOLI_PASSWORD=... bun test tests/integration
 *
 * Every pipeline deployed here carries a unique ts-itest-* pipeline_id
 * and is deleted afterwards, so reruns never collide and the server is
 * left as found.
 */

const SERVER = process.env.BROKOLI_SERVER || "";
const RUN_TAG = `ts-itest-${Date.now().toString(36)}`;

const live = describe.if(!!SERVER);

live("live server integration", () => {
  let client: Client;
  let dir: string;
  const deployed: string[] = [];

  const pid = (name: string) => `${RUN_TAG}-${name}`;

  async function deploy(pipeline: Pipeline): Promise<void> {
    await client.deploy(pipeline);
    deployed.push(pipeline.pipelineId);
  }

  beforeAll(async () => {
    const { username, password } = {
      username: process.env.BROKOLI_USERNAME,
      password: process.env.BROKOLI_PASSWORD,
    };
    client =
      username && password
        ? new Client(SERVER.replace(/\/+$/, ""), { username, password })
        : await Client.fromEnv(SERVER);
    dir = mkdtempSync(join(process.env.BROKOLI_ITEST_DIR || tmpdir(), "brk-itest-"));
  });

  afterAll(async () => {
    for (const pipelineId of deployed) {
      try {
        const target = await client.pipeline(pipelineId);
        await client.request(`/api/pipelines/${encodeURIComponent(target.id)}`, { method: "DELETE" });
      } catch {
        // Best-effort cleanup; a failed test may not have deployed.
      }
    }
    rmSync(dir, { recursive: true, force: true });
  });

  test("capabilities advertises IR versions and execution features", async () => {
    const caps = await client.capabilities();
    expect(caps.supportedIrVersions).toContain("2.0");
    expect(Array.isArray(caps.supportedExecutionFeatures)).toBe(true);
  });

  test("TypeScript task is refused honestly or runs end to end when advertised", async () => {
    writeFileSync(join(dir, "typescript-in.csv"), "id,amt\n1,4\n");
    const p = new Pipeline("TS itest code", { pipelineId: pid("typescript-code") });
    const rows = p.sourceFile("Read", { path: join(dir, "typescript-in.csv"), format: "csv" });
    const shaped = p.task("Shape", rows, (input) => ({
      columns: ["id", "total"],
      rows: input.map((row) => ({ id: row.id, total: Number(row.amt) * 2 })),
    }));
    shaped.then(p.sinkFile("Write", undefined, { path: join(dir, "typescript-out.csv"), format: "csv" }));

    const caps = await client.capabilities();
    if (!caps.supportedExecutionFeatures?.includes("code-typescript")) {
      await expect(client.preflight(p)).rejects.toThrow(/code-typescript/);
      return;
    }

    await deploy(p);
    const run = await client.run(pid("typescript-code"));
    const detail = await run.wait({ timeout: 60, raiseOnFailure: true });
    expect(detail.status).toBe("success");
    expect(readFileSync(join(dir, "typescript-out.csv"), "utf8")).toContain("8");
  }, 90000);

  test("TypeScript code reports a memory ceiling breach", async () => {
    const caps = await client.capabilities();
    if (!caps.supportedExecutionFeatures?.includes("code-typescript")) return;

    const p = new Pipeline("TS itest code limit", { pipelineId: pid("typescript-limit") });
    const rows = p.sourceFile("Read", { path: join(dir, "typescript-in.csv"), format: "csv" });
    p.code("Balloon", rows, {
      language: "typescript",
      maxMemoryMb: 32,
      timeout: 30,
      script: "const held = []; while (true) held.push(new Array(100000).fill('x'));",
    });
    await deploy(p);
    const detail = await (await client.run(pid("typescript-limit"))).wait({ timeout: 60 });
    expect(detail.status).toBe("failed");
    expect(JSON.stringify(detail)).toMatch(/memory|heap|max_memory/i);
  }, 90000);

  test("deploy creates, redeploy updates in place (no duplicate)", async () => {
    const p = new Pipeline("TS itest deploy", { pipelineId: pid("deploy") });
    const rows = p.sourceFile("Read", { path: join(dir, "in.csv"), format: "csv" });
    rows.then(p.sinkFile("Write", undefined, { path: join(dir, "deploy-out.csv"), format: "csv" }));
    await deploy(p);
    const created = await client.pipeline(pid("deploy"));
    expect(created.pipeline_id).toBe(pid("deploy"));

    const p2 = new Pipeline("TS itest deploy", {
      pipelineId: pid("deploy"),
      description: "updated by the integration suite",
    });
    const rows2 = p2.sourceFile("Read", { path: join(dir, "in.csv"), format: "csv" });
    rows2.then(p2.sinkFile("Write", undefined, { path: join(dir, "deploy-out.csv"), format: "csv" }));
    await client.deploy(p2);
    // client.pipeline throws on ambiguity, so this doubles as the
    // no-duplicate assertion (the Python CLI once dup-created here).
    const updated = await client.pipeline(pid("deploy"));
    expect(updated.id).toBe(created.id);
    expect(updated.description).toBe("updated by the integration suite");
  }, 30000);

  test("run + wait: a file pipeline transforms end to end", async () => {
    writeFileSync(join(dir, "in.csv"), "name,amt\nada,5\ngrace,7\n");
    const p = new Pipeline("TS itest run", { pipelineId: pid("run") });
    const rows = p.sourceFile("Read", { path: join(dir, "in.csv"), format: "csv" });
    const shaped = p.transform("Shape", rows, {
      rules: [{ type: "rename", mapping: { amt: "amount" } }],
    });
    shaped.then(p.sinkFile("Write", undefined, { path: join(dir, "run-out.csv"), format: "csv" }));
    await deploy(p);

    const run = await client.run(pid("run"));
    const detail = await run.wait({ timeout: 60, raiseOnFailure: true });
    expect(detail.status).toBe("success");

    const out = readFileSync(join(dir, "run-out.csv"), "utf8");
    expect(out.split("\n")[0]).toContain("amount");
    expect(out).toContain("ada");

    // logs: the run produced retrievable log lines.
    const logs = await run.logs();
    expect(Array.isArray(logs)).toBe(true);
    expect(logs.length).toBeGreaterThan(0);

    // nodeRuns: one per node, all successful.
    const nodeRuns = (await run.nodeRuns()) as Array<Record<string, unknown>>;
    expect(nodeRuns.length).toBe(3);
    for (const nodeRun of nodeRuns) expect(nodeRun.status).toBe("success");
  }, 90000);

  test("cancel: a running pipeline reaches a cancelled terminal state", async () => {
    const p = new Pipeline("TS itest cancel", { pipelineId: pid("cancel") });
    // The server requires a source node in every pipeline.
    const rows = p.sourceFile("Read", { path: join(dir, "in.csv"), format: "csv" });
    p.code("Sleep", rows, { script: "import time\ntime.sleep(60)\n" });
    await deploy(p);

    const run = await client.run(pid("cancel"));
    // Give the run a moment to leave the queue so cancel hits a live run.
    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
      const status = await run.status().catch(() => "");
      if (status && status !== "pending" && status !== "queued") break;
      await Bun.sleep(300);
    }
    await run.cancel();
    const detail = await run.wait({ timeout: 60 });
    expect(["cancelled", "canceled"]).toContain(String(detail.status));
  }, 90000);

  test("retry/resume: a failed run succeeds after the cause is fixed", async () => {
    const marker = join(dir, "retry-marker");
    const p = new Pipeline("TS itest retry", { pipelineId: pid("retry") });
    const rows = p.sourceFile("Read", { path: join(dir, "in.csv"), format: "csv" });
    p.code("Gate", rows, {
      script: ["import os, sys", `if not os.path.exists("${marker}"):`, "    sys.exit(1)", ""].join("\n"),
    });
    await deploy(p);

    const run = await client.run(pid("retry"));
    const failed = await run.wait({ timeout: 60 });
    expect(failed.status).toBe("failed");

    writeFileSync(marker, "");
    // Resume creates a NEW run; the failed one stays failed.
    const resumedRun = await client.retry(run.id);
    expect(resumedRun.id).not.toBe(run.id);
    const resumed = await resumedRun.wait({ timeout: 60 });
    expect(resumed.status).toBe("success");
    expect(await run.status()).toBe("failed");
  }, 120000);

  test("backfill: an RFC3339 window materializes one run per interval", async () => {
    const p = new Pipeline("TS itest backfill", {
      pipelineId: pid("backfill"),
      schedule: "0 * * * *",
      catchUp: true,
    });
    const rows = p.sourceFile("Read", { path: join(dir, "in.csv"), format: "csv" });
    rows.then(
      p.sinkFile("Slice", undefined, {
        path: join(dir, "slice-${interval.start}.csv"),
        format: "csv",
      }),
    );
    await deploy(p);

    const plan = (await client.backfill(pid("backfill"), {
      start: "2026-08-01T00:00:00Z",
      end: "2026-08-01T02:00:00Z",
    })) as Record<string, unknown>;
    expect(plan).toBeTruthy();

    // The window covers two whole hourly intervals; wait for both runs.
    const target = await client.pipeline(pid("backfill"));
    const deadline = Date.now() + 60000;
    let terminal: Array<Record<string, unknown>> = [];
    while (Date.now() < deadline) {
      const body = await client.request(
        `/api/pipelines/${encodeURIComponent(target.id)}/runs?limit=20`,
      );
      const items = (body.items || []) as Array<Record<string, unknown>>;
      terminal = items.filter((run) => run.status === "success");
      if (terminal.length >= 2) break;
      await Bun.sleep(500);
    }
    expect(terminal.length).toBeGreaterThanOrEqual(2);
    const slice = readFileSync(join(dir, "slice-2026-08-01T00:00:00Z.csv"), "utf8");
    expect(slice).toContain("ada");
  }, 120000);

  test("preview returns a node's captured output sample from a run", async () => {
    // The run pipeline succeeded earlier in the suite; preview its
    // transform node's output from that run.
    const target = await client.pipeline(pid("run"));
    const body = await client.request(`/api/pipelines/${encodeURIComponent(target.id)}/runs?limit=1`);
    const runId = String((body.items as Array<Record<string, unknown>>)[0].id);
    const nodes = (target.nodes ||
      (await client.request(`/api/pipelines/${encodeURIComponent(target.id)}`)).nodes) as Array<Record<string, unknown>>;
    const shape = nodes.find((node) => node.type === "transform");
    expect(shape).toBeTruthy();
    const preview = await client.runHandle(runId).preview(String(shape!.id));
    expect(preview.columns).toContain("amount");
    expect(preview.rows.length).toBeGreaterThan(0);
  }, 30000);
});

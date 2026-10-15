/**
 * The server client: capability preflight, deploy, and the run
 * lifecycle. Mirrors the Python SDK's Client/Run semantics, including
 * the deploy-time capability refusal by feature name and the run-poll
 * visibility grace window.
 */

import { APIError } from "./errors";
import { JS_WRAPPER_VERSION } from "./code";
import type { PipelineIR } from "./ir";
import { requiredExecutionFeatures, RUNTIME_EXISTENCE_FEATURES } from "./ir";
import { loadToken } from "./credentials";
import type { Pipeline } from "./pipeline";

export const TERMINAL_RUN_STATUSES = new Set(["success", "succeeded", "failed", "cancelled", "canceled", "blocked"]);

export type Capabilities = {
  supportedIrVersions: string[];
  supportedExecutionFeatures?: string[];
  /** Node types the server knows (its node_type_capabilities keys), when
   * it reports them. */
  nodeTypes?: string[];
  /** Node types the server's operator disabled. */
  disabledNodeTypes?: string[];
  codeLanguages?: string[];
  codeJsWrapperVersion?: number;
  raw: Record<string, unknown>;
};

export type ClientOptions = {
  apiKey?: string;
  username?: string;
  password?: string;
  fetch?: typeof globalThis.fetch;
  /** Sent as X-Workspace-ID on every request, for a multi-workspace
   * server; without it the server uses your default workspace. */
  workspace?: string;
};

// The server lists every node type in node_type_capabilities except
// "task", which its execution features gate instead.
const UNLISTED_NODE_TYPES = new Set(["task"]);

/** The backfill request field and text for one bound: a bare date is a
 * whole day (end inclusive) in the legacy *_date field; an instant is an
 * RFC3339 start/end. */
function backfillBound(key: "start" | "end", value: string | Date): [string, string] {
  if (value instanceof Date) return [key, value.toISOString()];
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? [`${key}_date`, value] : [key, value];
}

export class Run {
  constructor(
    readonly client: Client,
    readonly id: string,
  ) {}

  detail(): Promise<Record<string, unknown>> {
    return this.client.request(`/api/runs/${encodeURIComponent(this.id)}`);
  }

  async status(): Promise<string> {
    return String((await this.detail()).status || "");
  }

  async nodeRuns(): Promise<unknown[]> {
    return ((await this.detail()).node_runs as unknown[]) || [];
  }

  async logs(options: { level?: string; node?: string } = {}): Promise<unknown[]> {
    const query = new URLSearchParams();
    if (options.level) query.set("level", options.level);
    if (options.node) query.set("node_id", options.node);
    const body = await this.client.request(
      `/api/runs/${encodeURIComponent(this.id)}/logs${query.size ? `?${query}` : ""}`,
    );
    if (Array.isArray(body)) return body;
    return (body.logs as unknown[]) || (body.items as unknown[]) || [];
  }

  /** A node's captured output sample from this run: {columns, rows}.
   * 404s ("no preview available") until the node has produced one. */
  preview(nodeId: string): Promise<{ columns: string[]; rows: unknown[] }> {
    return this.client.request(
      `/api/runs/${encodeURIComponent(this.id)}/nodes/${encodeURIComponent(nodeId)}/preview`,
    );
  }

  /** Run this run again from where it stopped, as a new run: from the
   * first failed node, or from `fromNode` onward. The other node outputs
   * are reused. Returns the new run's handle. */
  async resume(options: { fromNode?: string } = {}): Promise<Run> {
    const response = await this.client.request(`/api/runs/${encodeURIComponent(this.id)}/resume`, {
      method: "POST",
      body: JSON.stringify(options.fromNode ? { from_node: options.fromNode } : {}),
    });
    const id = response?.run_id || response?.id || response?.run?.id;
    if (!id) throw new APIError("Resume response did not contain a run ID", 0, response);
    return new Run(this.client, id);
  }

  cancel(): Promise<unknown> {
    return this.client.request(`/api/runs/${encodeURIComponent(this.id)}/cancel`, {
      method: "POST",
      body: JSON.stringify({}),
    });
  }

  /**
   * Poll until the run is terminal. Within `visibilityGrace` seconds a
   * 404 from the status poll means "not visible yet" — servers that
   * dispatch asynchronously can answer the trigger before the run row
   * is readable — and polling continues; afterwards a 404 is real.
   */
  async wait(
    options: { timeout?: number; pollInterval?: number; raiseOnFailure?: boolean; visibilityGrace?: number } = {},
  ): Promise<Record<string, unknown>> {
    const deadline = Date.now() + (options.timeout ?? 600) * 1000;
    const graceDeadline = Date.now() + (options.visibilityGrace ?? 2) * 1000;
    const ceiling = (options.pollInterval ?? 2) * 1000;
    let interval = Math.min(50, ceiling);
    let detail: Record<string, unknown> = {};
    while (Date.now() < deadline) {
      try {
        detail = await this.detail();
      } catch (error) {
        if (error instanceof APIError && error.status === 404 && Date.now() < graceDeadline) {
          await Bun.sleep(interval);
          interval = Math.min(interval * 1.6, ceiling);
          continue;
        }
        throw error;
      }
      const status = String(detail.status || "");
      if (TERMINAL_RUN_STATUSES.has(status)) {
        if (options.raiseOnFailure && status !== "success") {
          throw new APIError(`Run ${this.id} finished ${status}`, 0, detail);
        }
        return detail;
      }
      await Bun.sleep(interval);
      interval = Math.min(interval * 1.6, ceiling);
    }
    throw new Error(`Timed out waiting for run ${this.id} (last status: ${detail.status || "unknown"})`);
  }
}

export class Client {
  private token?: string;
  private readonly doFetch: typeof globalThis.fetch;

  constructor(
    readonly baseUrl: string,
    private readonly options: ClientOptions = {},
  ) {
    if (options.apiKey && (options.username || options.password)) {
      throw new TypeError("apiKey and username/password are mutually exclusive");
    }
    this.doFetch = options.fetch || fetch;
  }

  /** Build a client from the environment and the shared credentials
   * file: BROKOLI_TOKEN, then the token `brokoli auth` stored (Python
   * or TypeScript — same file). */
  static async fromEnv(server?: string, options: ClientOptions = {}): Promise<Client> {
    const target = (server || process.env.BROKOLI_SERVER || "").replace(/\/$/, "");
    if (!target) throw new TypeError("no server: pass one or set BROKOLI_SERVER");
    const token = process.env.BROKOLI_TOKEN || (await loadToken(target));
    const workspace = options.workspace ?? process.env.BROKOLI_WORKSPACE;
    return new Client(target, { ...options, ...(token ? { apiKey: token } : {}), ...(workspace ? { workspace } : {}) });
  }

  private async authenticate(): Promise<void> {
    if (this.options.apiKey) {
      this.token = this.options.apiKey;
      return;
    }
    if (!this.options.username || !this.options.password) return;
    const response = await this.doFetch(`${this.baseUrl.replace(/\/$/, "")}/api/auth/login`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username: this.options.username, password: this.options.password }),
    });
    if (!response.ok) throw new APIError("Authentication failed", response.status, await response.text());
    const body = (await response.json()) as { token?: string; access_token?: string };
    this.token = body.token || body.access_token;
    if (!this.token) throw new APIError("Authentication response did not contain a token", response.status, body);
  }

  async request(path: string, init: RequestInit = {}, retry = true): Promise<any> {
    if (!this.token) await this.authenticate();
    const headers = new Headers(init.headers);
    headers.set("accept", "application/json");
    if (init.body && !headers.has("content-type")) headers.set("content-type", "application/json");
    if (this.token) headers.set("authorization", `Bearer ${this.token}`);
    if (this.options.workspace) headers.set("x-workspace-id", this.options.workspace);
    const response = await this.doFetch(`${this.baseUrl.replace(/\/$/, "")}${path}`, { ...init, headers });
    if (response.status === 401 && retry && !this.options.apiKey && this.options.username) {
      this.token = undefined;
      return this.request(path, init, false);
    }
    const text = await response.text();
    let body: unknown;
    try {
      body = text ? JSON.parse(text) : undefined;
    } catch {
      body = text;
    }
    if (!response.ok) throw new APIError(`Brokoli API request failed: ${response.status}`, response.status, body);
    return body;
  }

  async capabilities(): Promise<Capabilities> {
    const raw = await this.request("/api/capabilities");
    if (!Array.isArray(raw.supported_ir_versions) || !raw.supported_ir_versions.length) {
      throw new APIError("Malformed server capabilities", 200, raw);
    }
    if (raw.supported_execution_features !== undefined && raw.supported_execution_features !== null && !Array.isArray(raw.supported_execution_features)) {
      throw new APIError("Malformed server capabilities", 200, raw);
    }
    const nodeTypeCapabilities = raw.node_type_capabilities;
    if (nodeTypeCapabilities !== undefined && nodeTypeCapabilities !== null && (typeof nodeTypeCapabilities !== "object" || Array.isArray(nodeTypeCapabilities))) {
      throw new APIError("Malformed server capabilities", 200, raw);
    }
    // A server that disables nothing may send null rather than [].
    const disabled = raw.disabled_node_types;
    if (disabled !== undefined && disabled !== null && (!Array.isArray(disabled) || disabled.some((t: unknown) => typeof t !== "string" || !t))) {
      throw new APIError("Malformed server capabilities", 200, raw);
    }
    return {
      supportedIrVersions: raw.supported_ir_versions,
      supportedExecutionFeatures: raw.supported_execution_features ?? undefined,
      nodeTypes: nodeTypeCapabilities ? Object.keys(nodeTypeCapabilities) : undefined,
      disabledNodeTypes: nodeTypeCapabilities || disabled ? disabled ?? [] : undefined,
      codeLanguages: raw.code_languages,
      codeJsWrapperVersion: raw.code_js_wrapper_version,
      raw,
    };
  }

  /** Refuse, by name, anything this server cannot actually execute. */
  async preflight(pipeline: Pipeline, allowLegacyServer = false): Promise<Capabilities | undefined> {
    let caps: Capabilities;
    try {
      caps = await this.capabilities();
    } catch (error) {
      if (allowLegacyServer && error instanceof APIError && [404, 405].includes(error.status)) return undefined;
      throw error;
    }
    const ir = pipeline.toJSON();
    if (!caps.supportedIrVersions.includes(ir.ir_version)) {
      throw new APIError(
        `Pipeline requires IR ${ir.ir_version}, server supports: ${caps.supportedIrVersions.join(", ")}`,
        409,
        caps.raw,
      );
    }
    // A node type the server predates, or one its operator disabled, would
    // be refused by its validator; say so before anything is saved.
    const problems: string[] = [];
    for (const type of [...new Set(ir.nodes.map((node) => node.type))].sort()) {
      if (caps.disabledNodeTypes?.includes(type)) problems.push(`${type} nodes are disabled on this server`);
      else if (caps.nodeTypes && !caps.nodeTypes.includes(type) && !UNLISTED_NODE_TYPES.has(type)) {
        problems.push(`this server does not know the ${type} node type; upgrade the server`);
      }
    }
    if (problems.length) throw new APIError(`Pipeline cannot run on this server: ${problems.join("; ")}`, 409, caps.raw);

    const required = requiredExecutionFeatures(ir);
    if (caps.supportedExecutionFeatures) {
      const missing = required.filter(
        (feature) => !caps.supportedExecutionFeatures?.includes(feature),
      );
      if (missing.length) {
        throw new APIError(
          `Server does not support execution feature(s): ${missing.join(", ")}. The server advertises what it can actually run; deploying anyway would persist a pipeline that fails at run time.`,
          409,
          caps.raw,
        );
      }
    } else {
      // Legacy servers omitted the feature list, so existing declarative
      // features retain the Python SDK's compatibility behavior. New code
      // runtime contracts are different: absence cannot prove that the
      // language/wrapper exists, and must fail closed.
      const missing = required.filter((feature) => RUNTIME_EXISTENCE_FEATURES.includes(feature));
      if (missing.length) {
        throw new APIError(
          `Server does not advertise execution feature(s): ${missing.join(", ")}. These name a runtime that must exist on the server; a legacy capability response cannot prove compatibility.`,
          409,
          caps.raw,
        );
      }
    }
    if (required.includes("code-typescript") && caps.codeJsWrapperVersion !== JS_WRAPPER_VERSION) {
      throw new APIError(
        `TypeScript code nodes require JS wrapper version ${JS_WRAPPER_VERSION}, but the server advertises ${caps.codeJsWrapperVersion ?? "none"}.`,
        409,
        caps.raw,
      );
    }
    return caps;
  }

  async pipelines(): Promise<Array<Record<string, any>>> {
    const all: Array<Record<string, any>> = [];
    let after = "";
    do {
      const body = await this.request(
        `/api/pipelines?limit=100${after ? `&after=${encodeURIComponent(after)}` : ""}`,
      );
      if (Array.isArray(body)) return all.concat(body);
      all.push(...(body.items || body.pipelines || []));
      after = body.has_next && body.cursor ? body.cursor : "";
    } while (after);
    return all;
  }

  async pipeline(identifier: string): Promise<Record<string, any>> {
    const items = await this.pipelines();
    const matches = items.filter(
      (item) => item.id === identifier || item.pipeline_id === identifier || item.name === identifier,
    );
    if (matches.length !== 1) {
      throw new APIError(
        matches.length ? `Pipeline identifier is ambiguous: ${identifier}` : `Pipeline not found: ${identifier}`,
        matches.length ? 409 : 404,
      );
    }
    return matches[0];
  }

  /** Create-or-update by pipeline_id, after capability preflight. */
  async deploy(pipeline: Pipeline): Promise<unknown> {
    await this.preflight(pipeline);
    const existing = await this.pipelines();
    const match = existing.find((item) => item.pipeline_id === pipeline.pipelineId);
    const body = pipeline.toJSON();
    if (match) {
      return this.request(`/api/pipelines/${encodeURIComponent(match.id)}`, {
        method: "PUT",
        body: JSON.stringify({ ...body, id: match.id }),
      });
    }
    return this.request("/api/pipelines", { method: "POST", body: JSON.stringify(body) });
  }

  async run(identifier: string, params?: Record<string, string>): Promise<Run> {
    const target = await this.pipeline(identifier);
    const response = await this.request(`/api/pipelines/${encodeURIComponent(target.id)}/run`, {
      method: "POST",
      ...(params ? { body: JSON.stringify({ params }) } : {}),
    });
    const id = response.run_id || response.id || response.run?.id;
    if (!id) throw new APIError("Trigger response did not contain a run ID", 0, response);
    return new Run(this, id);
  }

  runHandle(runId: string): Run {
    return new Run(this, runId);
  }

  /** Resume a failed run. The server creates a NEW run that reuses the
   * old run's successful node outcomes; the returned handle points at
   * that new run — the old one stays failed. `fromNode` re-runs that
   * node and everything downstream of it instead. */
  resume(runId: string, options: { fromNode?: string } = {}): Promise<Run> {
    return new Run(this, runId).resume(options);
  }

  /** The same as `resume`; kept for existing callers. */
  retry(runId: string, options: { fromNode?: string } = {}): Promise<Run> {
    return this.resume(runId, options);
  }

  /** Execute the pipeline on up to 10 rows per node without recording a
   * run. It really runs the nodes against their connections: a sink still
   * writes. Returns `{results}`, plus `error` when a node failed. */
  async dryRun(identifier: string): Promise<{ results: unknown; error?: string }> {
    const pipeline = await this.pipeline(identifier);
    return this.request(`/api/pipelines/${encodeURIComponent(pipeline.id)}/dry-run`, {
      method: "POST",
      body: JSON.stringify({}),
    });
  }

  /**
   * Backfill over the pipeline's own schedule. Bounds route by shape:
   * bare YYYY-MM-DD keeps its historical inclusive-day meaning through
   * the legacy fields; RFC3339 timestamps and Dates use the
   * interval-native start/end (server v0.10.78+). `force` re-runs
   * intervals that already succeeded, and lets a pipeline that never
   * references its interval be backfilled anyway.
   */
  async backfill(identifier: string, range: { start: string | Date; end: string | Date; force?: boolean }): Promise<unknown> {
    const pipeline = await this.pipeline(identifier);
    const body: Record<string, unknown> = Object.fromEntries([backfillBound("start", range.start), backfillBound("end", range.end)]);
    if (range.force) body.force = true;
    return this.request(`/api/pipelines/${encodeURIComponent(pipeline.id)}/backfill`, {
      method: "POST",
      body: JSON.stringify(body),
    });
  }

}

/** Log in with username/password and store the token in the shared
 * credentials file, exactly where `brokoli auth` puts device tokens. */
export async function login(server: string, username: string, password: string): Promise<string> {
  const client = new Client(server, { username, password });
  await client.request("/api/capabilities");
  const token = (client as unknown as { token?: string }).token;
  if (!token) throw new APIError("login did not produce a token", 0);
  const { storeToken } = await import("./credentials");
  await storeToken(server, token);
  return token;
}

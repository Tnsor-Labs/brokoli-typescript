/**
 * Stored credentials, byte-compatible with the Python SDK.
 *
 * One file for both SDKs: `~/.config/brokoli/credentials.json` (XDG- and
 * BROKOLI_CREDENTIALS-aware), shaped `{"servers": {"<url>": "<token>"}}`
 * and chmod 0600 — exactly what `brokoli auth` (Python, 0.7+) writes.
 * The earlier TypeScript-only single-server shape is gone; it could not
 * share a login with the Python CLI, which defeats the point.
 */

import { mkdir, readFile, writeFile, chmod } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

export function credentialsPath(): string {
  const override = process.env.BROKOLI_CREDENTIALS;
  if (override) return override;
  const base = process.env.XDG_CONFIG_HOME || join(homedir(), ".config");
  return join(base, "brokoli", "credentials.json");
}

async function loadAll(): Promise<Record<string, string>> {
  try {
    const data = JSON.parse(await readFile(credentialsPath(), "utf8"));
    return data && typeof data.servers === "object" && data.servers !== null ? data.servers : {};
  } catch {
    return {};
  }
}

/** The stored token for a server, or empty. */
export async function loadToken(server: string): Promise<string> {
  const servers = await loadAll();
  return servers[server.replace(/\/+$/, "")] || "";
}

/** Persist a token for a server; the file is user-only (0600). */
export async function storeToken(server: string, token: string): Promise<string> {
  const path = credentialsPath();
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const servers = await loadAll();
  servers[server.replace(/\/+$/, "")] = token;
  await writeFile(path, `${JSON.stringify({ servers }, null, 2)}\n`);
  await chmod(path, 0o600);
  return path;
}

/** Drop the stored token for a server, if any. */
export async function forgetToken(server: string): Promise<void> {
  const servers = await loadAll();
  if (delete servers[server.replace(/\/+$/, "")]) {
    await writeFile(credentialsPath(), `${JSON.stringify({ servers }, null, 2)}\n`);
  }
}

import { APIError } from "./errors";
import { storeToken } from "./credentials";

export type DeviceGrant = {
  device_code: string;
  user_code: string;
  verification_uri: string;
  interval?: number;
  expires_in?: number;
};

export type DeviceLoginOptions = {
  fetch?: typeof globalThis.fetch;
  openBrowser?: boolean;
  echo?: (line: string) => void;
  sleep?: (milliseconds: number) => Promise<void>;
};

async function post(fetcher: typeof globalThis.fetch, server: string, path: string, body: unknown): Promise<{ status: number; body: any }> {
  const response = await fetcher(`${server.replace(/\/+$/, "")}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  let payload: any = {};
  try { payload = await response.json(); } catch { /* error below carries an empty body */ }
  return { status: response.status, body: payload };
}

export async function requestDeviceAuthorization(server: string, fetcher = fetch): Promise<DeviceGrant> {
  const response = await post(fetcher, server, "/api/auth/oauth/device", {});
  if ([404, 405].includes(response.status)) {
    throw new APIError(`${server} does not support device authorization; use username/password or an API key instead`, response.status, response.body);
  }
  if (response.status !== 200 || !response.body.device_code) {
    throw new APIError(`Device authorization request failed: HTTP ${response.status}`, response.status, response.body);
  }
  return response.body as DeviceGrant;
}

export async function pollForToken(
  server: string,
  grant: DeviceGrant,
  options: Pick<DeviceLoginOptions, "fetch" | "sleep"> = {},
): Promise<{ token: string; username: string }> {
  const fetcher = options.fetch || fetch;
  const sleep = options.sleep || Bun.sleep;
  const deadline = Date.now() + Math.max(1, grant.expires_in ?? 600) * 1000;
  let wait = Math.max(1, grant.interval ?? 3);
  while (Date.now() <= deadline) {
    await sleep(wait * 1000);
    const response = await post(fetcher, server, "/api/auth/oauth/device/poll", { device_code: grant.device_code });
    if (response.status !== 200) throw new APIError(`Device authorization poll failed: HTTP ${response.status}`, response.status, response.body);
    switch (String(response.body.status || "")) {
      case "authorization_pending": continue;
      case "slow_down": wait += 2; continue;
      case "access_denied": throw new APIError("The device authorization request was denied", 403, response.body);
      case "expired": throw new APIError("The device authorization code expired", 410, response.body);
      case "approved": {
        const token = String(response.body.token || "");
        if (!token) throw new APIError("Device authorization approval carried no token", 200, response.body);
        return { token, username: String(response.body.username || "") };
      }
      default: throw new APIError(`Unexpected device authorization status: ${response.body.status}`, 200, response.body);
    }
  }
  throw new APIError("The device authorization code expired", 410);
}

export async function deviceLogin(server: string, options: DeviceLoginOptions = {}): Promise<string> {
  const echo = options.echo || ((line) => console.log(line));
  const grant = await requestDeviceAuthorization(server, options.fetch);
  echo(`Confirm this code in your browser: ${grant.user_code}`);
  echo(`  ${grant.verification_uri}`);
  if (options.openBrowser !== false) {
    try {
      const command = process.platform === "darwin" ? ["open", grant.verification_uri] : process.platform === "win32" ? ["cmd", "/c", "start", grant.verification_uri] : ["xdg-open", grant.verification_uri];
      Bun.spawn(command, { stdout: "ignore", stderr: "ignore" }).unref();
    } catch { /* printed URL is the headless fallback */ }
  }
  echo("Waiting for approval...");
  const { token, username } = await pollForToken(server, grant, options);
  const path = await storeToken(server, token);
  echo(`Authorized${username ? ` as ${username}` : ""}. Token stored in ${path}.`);
  return token;
}

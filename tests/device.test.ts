import { describe, expect, test } from "bun:test";
import { pollForToken, requestDeviceAuthorization } from "../src/device";

function sequence(...payloads: Array<[number, unknown]>): typeof fetch {
  return (async () => {
    const [status, body] = payloads.shift()!;
    return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
  }) as unknown as typeof fetch;
}

describe("device authorization", () => {
  test("requests a grant and handles pending/slow-down/approved", async () => {
    const grant = await requestDeviceAuthorization("http://server/", sequence([200, {
      device_code: "device", user_code: "ABCD", verification_uri: "http://server/confirm", interval: 1, expires_in: 60,
    }]));
    const waits: number[] = [];
    const result = await pollForToken("http://server", grant, {
      fetch: sequence(
        [200, { status: "authorization_pending" }],
        [200, { status: "slow_down" }],
        [200, { status: "approved", token: "brk_token", username: "ada" }],
      ),
      sleep: async (milliseconds) => { waits.push(milliseconds); },
    });
    expect(result).toEqual({ token: "brk_token", username: "ada" });
    expect(waits).toEqual([1000, 1000, 3000]);
  });

  test("backs off on a rate-limited or unavailable poll and keeps polling (#29)", async () => {
    const grant = { device_code: "d", user_code: "U", verification_uri: "http://s/c", interval: 1, expires_in: 600 };
    const waits: number[] = [];
    const result = await pollForToken("http://server", grant, {
      fetch: sequence(
        [429, { error: "too many authorization polls" }],
        [503, {}],
        [200, { status: "approved", token: "brk_429", username: "" }],
      ),
      sleep: async (milliseconds) => { waits.push(milliseconds); },
    });
    expect(result.token).toBe("brk_429");
    expect(waits).toEqual([1000, 2000, 4000]);
  });

  test("other poll failures still fail", async () => {
    const grant = { device_code: "d", user_code: "U", verification_uri: "http://s/c", interval: 1, expires_in: 600 };
    await expect(pollForToken("http://server", grant, { fetch: sequence([500, {}]), sleep: async () => {} })).rejects.toThrow(/HTTP 500/);
  });

  test("refuses servers without the grant", async () => {
    await expect(requestDeviceAuthorization("http://server", sequence([404, {}]))).rejects.toThrow(/does not support device authorization/);
  });
});

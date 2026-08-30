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

  test("refuses servers without the grant", async () => {
    await expect(requestDeviceAuthorization("http://server", sequence([404, {}]))).rejects.toThrow(/does not support device authorization/);
  });
});

import { afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { credentialsPath, forgetToken, loadToken, storeToken } from "../src/credentials";

// The credentials file is shared with the Python SDK: same path rules,
// same {"servers": {url: token}} shape, same 0600 mode. These tests pin
// the TypeScript half; when BROKOLI_PYTHON is set, the last test proves
// byte-level interoperability against the real Python implementation.

const dirs: string[] = [];
function isolate(): string {
  const dir = mkdtempSync(join(tmpdir(), "brk-creds-"));
  dirs.push(dir);
  process.env.BROKOLI_CREDENTIALS = join(dir, "credentials.json");
  return process.env.BROKOLI_CREDENTIALS;
}

afterEach(() => {
  delete process.env.BROKOLI_CREDENTIALS;
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe("shared credentials store", () => {
  test("BROKOLI_CREDENTIALS overrides the XDG path", () => {
    const path = isolate();
    expect(credentialsPath()).toBe(path);
  });

  test("round-trips tokens, normalizing trailing slashes like Python rstrip", async () => {
    isolate();
    await storeToken("http://localhost:8090/", "brk_abc");
    expect(await loadToken("http://localhost:8090")).toBe("brk_abc");
    expect(await loadToken("http://localhost:8090//")).toBe("brk_abc");
    await forgetToken("http://localhost:8090");
    expect(await loadToken("http://localhost:8090")).toBe("");
  });

  test("writes the file user-only (0600)", async () => {
    const path = isolate();
    await storeToken("http://localhost:8090", "brk_abc");
    expect(statSync(path).mode & 0o777).toBe(0o600);
  });

  test("keeps other servers' tokens intact", async () => {
    isolate();
    await storeToken("http://a.example", "brk_a");
    await storeToken("http://b.example", "brk_b");
    await forgetToken("http://a.example");
    expect(await loadToken("http://a.example")).toBe("");
    expect(await loadToken("http://b.example")).toBe("brk_b");
  });

  test.if(!!process.env.BROKOLI_PYTHON)("interoperates with the Python SDK byte-for-byte", async () => {
    const path = isolate();
    // TypeScript writes; Python reads it and adds its own entry.
    await storeToken("http://ts.example", "brk_from_ts");
    const py = Bun.spawnSync(
      [
        process.env.BROKOLI_PYTHON!,
        "-c",
        [
          "from brokoli.device import load_token, store_token",
          "assert load_token('http://ts.example/') == 'brk_from_ts', 'python cannot read the ts-written file'",
          "store_token('http://py.example', 'brk_from_py')",
        ].join("\n"),
      ],
      { env: { ...process.env, BROKOLI_CREDENTIALS: path } },
    );
    expect(py.exitCode).toBe(0);
    // TypeScript reads what Python wrote, into the same file.
    expect(await loadToken("http://py.example")).toBe("brk_from_py");
    expect(await loadToken("http://ts.example")).toBe("brk_from_ts");
  });
});

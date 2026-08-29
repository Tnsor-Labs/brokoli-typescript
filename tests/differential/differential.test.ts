/**
 * The cross-SDK oracle: the same pipelines authored in Python and
 * TypeScript must produce byte-identical canonical IR and equal digests
 * (docs/schema/ir-canonicalization.md in the core repository).
 *
 * Requires a Python with the brokoli package importable; point
 * BROKOLI_PYTHON at it (for example a venv's python3). Without it the
 * suite fails loudly rather than skipping silently -- parity IS the
 * product, and a green run that never compared anything would be a lie.
 * Set BROKOLI_DIFFERENTIAL=skip to opt out explicitly.
 */

import { describe, expect, test } from "bun:test";
import { readdirSync } from "node:fs";
import { join } from "node:path";
import { irDigest, renderIR } from "../../src/ir";
import type { Pipeline } from "../../src/pipeline";

const FIXTURES = join(import.meta.dir, "fixtures");
const python = process.env.BROKOLI_PYTHON || "python3";
const optOut = process.env.BROKOLI_DIFFERENTIAL === "skip";

function pythonRender(file: string): { rendered: string; digest: string } {
  const proc = Bun.spawnSync([python, file], { env: { ...process.env } });
  const stdout = proc.stdout.toString();
  if (proc.exitCode !== 0) {
    throw new Error(
      `python fixture ${file} failed (is brokoli importable via BROKOLI_PYTHON?):\n${proc.stderr.toString()}`,
    );
  }
  const marker = stdout.lastIndexOf("DIGEST ");
  return {
    rendered: stdout.slice(0, marker),
    digest: stdout.slice(marker + "DIGEST ".length).trim(),
  };
}

const names = readdirSync(FIXTURES)
  .filter((f) => f.endsWith(".py"))
  .map((f) => f.replace(/\.py$/, ""))
  .sort();

describe("differential oracle", () => {
  for (const name of names) {
    test.skipIf(optOut)(name, async () => {
      const py = pythonRender(join(FIXTURES, `${name}.py`));
      const { build } = (await import(join(FIXTURES, `${name}.ts`))) as { build: () => Pipeline };
      const pipeline = build();
      const tsRendered = renderIR(pipeline.toJSON());
      const tsDigest = irDigest(pipeline.toJSON());
      expect(tsRendered).toBe(py.rendered);
      expect(tsDigest).toBe(py.digest);
    });
  }
  test("fixtures exist", () => {
    expect(names.length).toBeGreaterThanOrEqual(5);
  });
});

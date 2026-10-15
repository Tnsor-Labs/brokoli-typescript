import { readFile } from "node:fs/promises";

// The core release the vendored schemas were copied from (v0.18.12). The
// check fails if a vendored file differs from that commit.
const CORE_COMMIT = "1d9964b47b31259a3c0cc3e27f59b830c8045136";
const FILES = ["pipeline-ir-2.2.json", "task-interface-v1.json"];

const raw = (ref: string, file: string) =>
  `https://raw.githubusercontent.com/Tnsor-Labs/brokoli/${ref}/docs/schema/${file}`;

for (const file of FILES) {
  const [local, response] = await Promise.all([
    readFile(new URL(`../schema/${file}`, import.meta.url), "utf8"),
    fetch(raw(CORE_COMMIT, file)),
  ]);
  if (!response.ok) throw new Error(`Could not fetch pinned core schema ${file}: HTTP ${response.status}`);
  const remote = await response.text();
  if (local !== remote) {
    throw new Error(`Vendored ${file} differs from core ${CORE_COMMIT}`);
  }
  console.log(`${file} matches core ${CORE_COMMIT}`);

  // A pin that matches can still be stale: core may have moved on since.
  // That is not this repo's failure to fix in CI, so it warns, naming the
  // file to re-vendor and the pin to move.
  const latest = await fetch(raw("main", file)).catch(() => undefined);
  if (latest?.ok && (await latest.text()) !== local) {
    console.warn(`warning: core main has changed ${file} since ${CORE_COMMIT.slice(0, 7)}; re-vendor it and move CORE_COMMIT`);
  }
}

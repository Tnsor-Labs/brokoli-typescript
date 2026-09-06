import { readFile } from "node:fs/promises";

const CORE_COMMIT = "b0795346cb204f9f753a6ae21bd3e12eb4486547";
const FILES = ["pipeline-ir-2.2.json", "task-interface-v1.json"];

for (const file of FILES) {
  const url = `https://raw.githubusercontent.com/Tnsor-Labs/brokoli/${CORE_COMMIT}/docs/schema/${file}`;
  const [local, response] = await Promise.all([
    readFile(new URL(`../schema/${file}`, import.meta.url), "utf8"),
    fetch(url),
  ]);
  if (!response.ok) throw new Error(`Could not fetch pinned core schema ${file}: HTTP ${response.status}`);
  const remote = await response.text();
  if (local !== remote) {
    throw new Error(`Vendored ${file} differs from core ${CORE_COMMIT}`);
  }
  console.log(`${file} matches core ${CORE_COMMIT}`);
}

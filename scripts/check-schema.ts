import { readFile } from "node:fs/promises";

const CORE_COMMIT = "2767ea4f8c56aaf39ca9fb0a7faf2b05e7c0d430";
const url = `https://raw.githubusercontent.com/Tnsor-Labs/brokoli/${CORE_COMMIT}/docs/schema/pipeline-ir-2.1.json`;
const [local, response] = await Promise.all([
  readFile(new URL("../schema/pipeline-ir-2.1.json", import.meta.url), "utf8"),
  fetch(url),
]);
if (!response.ok) throw new Error(`Could not fetch pinned core schema: HTTP ${response.status}`);
const remote = await response.text();
if (local !== remote) {
  throw new Error(`Vendored pipeline IR schema differs from core ${CORE_COMMIT}`);
}
console.log(`Schema matches core ${CORE_COMMIT}`);

/**
 * Small typed scalar coercions for authoring-time TypeScript — replacing
 * the `Number(...)`/`String(...)`/`Boolean(...)` ceremony `Row` values
 * need once they leave `Record<string, unknown>`.
 *
 * IMPORTANT — authoring-time only, never inside a serialized code-node
 * function body. `task`/`source`/`sink`/`filter`/`map`/`validate`/`sensor`
 * bodies are serialized with `Function.prototype.toString` and executed
 * with no access to outer scope (ADR-030: v1 supports only self-contained
 * functions). Calling `str(...)`/`num(...)` from inside such a body ships
 * a dangling reference the worker cannot resolve — a remote
 * `ReferenceError`, not an inlined helper. Inside a task body, keep using
 * `Number(...)`/`String(...)`/`Boolean(...)` directly, or wait for the
 * closure-capture mechanism ADR-034 explicitly defers past this
 * prototype. These helpers are for ordinary TypeScript around the
 * pipeline: computing option values, config passed into a node factory,
 * and similar — never for code that becomes part of a node's script.
 */

export function num(value: unknown): number {
  return Number(value);
}

export function str(value: unknown): string {
  return String(value);
}

export function bool(value: unknown): boolean {
  return Boolean(value);
}

/** Cast only when present; `null`/`undefined` pass through as `undefined`. */
export function maybe<T>(value: unknown, cast: (value: unknown) => T): T | undefined {
  return value === null || value === undefined ? undefined : cast(value);
}

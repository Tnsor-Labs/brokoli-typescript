/** JSON-only scalar expressions for native dataset operators. */
export type Expression =
  | { op: "column"; path: string[] }
  | { op: "literal"; value: unknown }
  | { op: "add" | "subtract" | "multiply" | "divide" | "concat"; left: Expression; right: Expression }
  | { op: "coalesce"; args: Expression[] };

export function column(...path: string[]): Expression {
  if (!path.length || path.some((part) => !part)) throw new Error("column requires non-empty path segments");
  return { op: "column", path: [...path] };
}

export function literal(value: unknown): Expression {
  return { op: "literal", value };
}

function expression(value: Expression | unknown): Expression {
  return value && typeof value === "object" && "op" in value
    ? structuredClone(value as Expression)
    : literal(value);
}

function binary(op: "add" | "subtract" | "multiply" | "divide" | "concat", left: Expression | unknown, right: Expression | unknown): Expression {
  return { op, left: expression(left), right: expression(right) };
}

export const add = (left: Expression | unknown, right: Expression | unknown): Expression => binary("add", left, right);
export const subtract = (left: Expression | unknown, right: Expression | unknown): Expression => binary("subtract", left, right);
export const multiply = (left: Expression | unknown, right: Expression | unknown): Expression => binary("multiply", left, right);
export const divide = (left: Expression | unknown, right: Expression | unknown): Expression => binary("divide", left, right);
export const concat = (left: Expression | unknown, right: Expression | unknown): Expression => binary("concat", left, right);

export function coalesce(...values: (Expression | unknown)[]): Expression {
  if (!values.length) throw new Error("coalesce requires at least one expression");
  return { op: "coalesce", args: values.map(expression) };
}

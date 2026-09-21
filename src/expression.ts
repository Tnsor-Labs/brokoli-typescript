/** JSON-only scalar expressions for native dataset operators. */
export type Expression =
  | { op: "column"; path: string[] }
  | { op: "literal"; value: unknown }
  | { op: "add" | "subtract" | "multiply" | "divide" | "concat"; left: Expression; right: Expression }
  | { op: "coalesce"; args: Expression[] }
  | { op: "eq" | "neq" | "lt" | "lte" | "gt" | "gte"; left: Expression; right: Expression }
  | { op: "and" | "or"; args: Expression[] }
  | { op: "not" | "is_null"; arg: Expression }
  | { op: "case_when"; branches: { when: Expression; then: Expression }[]; else: Expression };

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

function predicate(op: "eq" | "neq" | "lt" | "lte" | "gt" | "gte", left: Expression | unknown, right: Expression | unknown): Expression {
  return { op, left: expression(left), right: expression(right) };
}
export const eq = (left: Expression | unknown, right: Expression | unknown): Expression => predicate("eq", left, right);
export const neq = (left: Expression | unknown, right: Expression | unknown): Expression => predicate("neq", left, right);
export const lt = (left: Expression | unknown, right: Expression | unknown): Expression => predicate("lt", left, right);
export const lte = (left: Expression | unknown, right: Expression | unknown): Expression => predicate("lte", left, right);
export const gt = (left: Expression | unknown, right: Expression | unknown): Expression => predicate("gt", left, right);
export const gte = (left: Expression | unknown, right: Expression | unknown): Expression => predicate("gte", left, right);
export const isNull = (value: Expression | unknown): Expression => ({ op: "is_null", arg: expression(value) });
export const not = (value: Expression | unknown): Expression => ({ op: "not", arg: expression(value) });
export const and = (...values: (Expression | unknown)[]): Expression => {
  if (!values.length) throw new Error("and requires at least one expression");
  return { op: "and", args: values.map(expression) };
};
export const or = (...values: (Expression | unknown)[]): Expression => {
  if (!values.length) throw new Error("or requires at least one expression");
  return { op: "or", args: values.map(expression) };
};
export function caseWhen(branches: [Expression | unknown, Expression | unknown][], otherwise: Expression | unknown = null): Expression {
  if (!branches.length) throw new Error("caseWhen requires at least one branch");
  return { op: "case_when", branches: branches.map(([when, then]) => ({ when: expression(when), then: expression(then) })), else: expression(otherwise) };
}

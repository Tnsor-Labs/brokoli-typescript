/** Interpolation references: values resolved by the server at execution
 * time. The compiled IR carries `${namespace.name}`, never the value. */

export class ResourceRef {
  constructor(readonly name: string) {
    if (!/^[A-Za-z0-9_.-]+$/.test(name)) {
      throw new TypeError("Resource name may contain only letters, digits, dot, dash, and underscore");
    }
  }
  /** The value written into node config. */
  irValue(): string {
    return this.name;
  }
  toString(): string {
    return this.irValue();
  }
}

/** A connection by conn_id; credentials never leave the server. */
export class Connection extends ResourceRef {}

export class InterpolationRef extends ResourceRef {
  constructor(name: string, readonly namespace: string) {
    super(name);
  }
  override irValue(): string {
    return `\${${this.namespace}.${this.name}}`;
  }
}

export class Secret extends InterpolationRef {
  constructor(name: string) {
    super(name, "secret");
  }
}
export class Variable extends InterpolationRef {
  constructor(name: string) {
    super(name, "var");
  }
}
export class Param extends InterpolationRef {
  constructor(name: string) {
    super(name, "param");
  }
}
export class EnvVar extends InterpolationRef {
  constructor(name: string) {
    super(name, "env");
  }
}

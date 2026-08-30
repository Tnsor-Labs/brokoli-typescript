/** Brokoli TypeScript SDK: author pipelines as plain TypeScript, compile
 * them to the same IR the Python SDK emits, and operate runs against a
 * Brokoli server. See tests/differential for the cross-SDK oracle. */

export { APIError, PipelineError } from "./errors";
export {
  JS_WRAPPER_VERSION,
  filterScript,
  functionSource,
  mapScript,
  sensorScript,
  sinkScript,
  sourceScript,
  taskScript,
  validateScript,
} from "./code";
export type {
  OutputRows,
  Row,
  RowMapper,
  RowPredicate,
  SensorFunction,
  SinkFunction,
  SourceFunction,
  TaskFunction,
  TaskOutput,
  ValidateFunction,
  ValidateResult,
} from "./code";
export {
  NODE_TYPE_CAPABILITIES,
  canonicalJSON,
  diffIR,
  irDigest,
  normalizeIR,
  renderIR,
  requiredExecutionFeatures,
} from "./ir";
export type { Capability, Config, Edge, IRNode, Json, PipelineIR } from "./ir";
export {
  ArtifactRef,
  CollectionRef,
  ConditionRef,
  DatasetRef,
  NodeRef,
  Pipeline,
  ScalarRef,
} from "./pipeline";
export type { Hook, HookInput, PipelineOptions } from "./pipeline";
export { Connection, EnvVar, InterpolationRef, Param, ResourceRef, Secret, Variable } from "./resources";
export {
  PaginationStrategy,
  cursorPages,
  linkHeaderPages,
  nextLinkPages,
  numberedPages,
  offsetPages,
} from "./pagination";
export { credentialsPath, forgetToken, loadToken, storeToken } from "./credentials";
export { deviceLogin, pollForToken, requestDeviceAuthorization } from "./device";
export type { DeviceGrant, DeviceLoginOptions } from "./device";
export { Client, Run, TERMINAL_RUN_STATUSES, login } from "./client";
export type { Capabilities, ClientOptions } from "./client";
export { validatePipeline } from "./validate";
export type { ValidationIssue, ValidationResult } from "./validate";
export { expectGraph, livePipeline, snapshotRun, watch } from "./testing";
export { GraphExpectation } from "./testing";

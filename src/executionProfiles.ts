/** Portable source_api execution profiles.
 * Builders expand to explicit policy values; the marker is retained so
 * explain output can identify the authoring profile. */

import type { Config } from "./ir";

export const EXECUTION_PROFILE_VERSION = 1;

const profile = (name: string, strict: boolean, values: Config): Config => ({
  profile: { name, version: EXECUTION_PROFILE_VERSION, strict },
  ...values,
});

export const publicApiSafe = (options: { requestsPerSecond?: number; maxConcurrency?: number; checkpointEvery?: number } = {}): Config =>
  profile("public_api_safe", false, {
    timeout: 30, max_retries: 3, retry_backoff: "exponential", retry_delay: 1000,
    max_concurrency: options.maxConcurrency ?? 2,
    requests_per_second: options.requestsPerSecond ?? 2,
    retry_scope: "page", checkpoint_every: options.checkpointEvery ?? 10,
    page_max_retries: 3, page_retry_backoff: "exponential",
  });

export const highThroughput = (options: { requestsPerSecond: number; maxConcurrency?: number; checkpointEvery?: number }): Config =>
  profile("high_throughput", false, {
    timeout: 30, max_retries: 2, retry_backoff: "exponential", retry_delay: 500,
    max_concurrency: options.maxConcurrency ?? 8,
    requests_per_second: options.requestsPerSecond,
    retry_scope: "page", checkpoint_every: options.checkpointEvery ?? 25,
    page_max_retries: 2, page_retry_backoff: "exponential",
  });

export const strict = (options: { requestsPerSecond?: number; checkpointEvery?: number } = {}): Config =>
  profile("strict", true, {
    timeout: 30, max_retries: 2, retry_backoff: "exponential", retry_delay: 1000,
    max_concurrency: 1, requests_per_second: options.requestsPerSecond ?? 1,
    retry_scope: "page", checkpoint_every: options.checkpointEvery ?? 10,
    page_max_retries: 2, page_retry_backoff: "exponential",
  });

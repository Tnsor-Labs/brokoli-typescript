/** Pagination strategies for source_api, mirroring brokoli.pagination.
 * `.withExecution(...)` turns on persisted checkpoints and per-page
 * retry policy — the run resumes at the last checkpointed page. */

import type { Config } from "./ir";

export class PaginationStrategy {
  private execution?: Config;

  constructor(
    readonly strategy: string,
    private readonly fields: Config = {},
  ) {}

  /** A copy with the execution policy attached (checkpoints, per-page
   * retries); the original strategy is not mutated. */
  withExecution(execution: Config): PaginationStrategy {
    const next = new PaginationStrategy(this.strategy, this.fields);
    next.execution = { ...execution };
    return next;
  }

  toConfig(): Config {
    return { strategy: this.strategy, ...this.fields };
  }

  executionConfig(): Config | undefined {
    return this.execution;
  }
}

export const offsetPages = (pageSize: number, options: Config = {}) =>
  new PaginationStrategy("offset", {
    page_size: pageSize,
    max_records: options.maxRecords,
    offset_param: options.offsetParam || "offset",
    limit_param: options.limitParam || "limit",
    end_flag: options.endFlag,
  });

export const cursorPages = (cursorPath: string, cursorParam: string) =>
  new PaginationStrategy("cursor", { cursor_path: cursorPath, cursor_param: cursorParam });

export const numberedPages = (pageParam: string, options: Config = {}) =>
  new PaginationStrategy("numbered", {
    page_param: pageParam,
    start: options.start === undefined ? 1 : options.start,
    total_pages_path: options.totalPagesPath,
  });

export const nextLinkPages = (nextPath: string) =>
  new PaginationStrategy("next_link", { next_path: nextPath });

export const linkHeaderPages = (rel = "next") =>
  new PaginationStrategy("link_header", { rel });

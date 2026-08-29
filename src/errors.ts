/** Error types shared across the SDK. */

/** Authoring-time failure: the pipeline under construction is invalid. */
export class PipelineError extends Error {}

/** An HTTP-level failure talking to the server. `status` is 0 for
 * transport errors; `body` carries the parsed response when there was one. */
export class APIError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly body?: unknown,
  ) {
    super(message);
  }
}

import { Effect, Random } from "effect"
import { LLMError, type ProviderErrorEvent } from "./schema"

export type RetryableFailure = LLMError | ProviderErrorEvent

export const RETRY_MAX_RETRIES = 2
export const RETRY_BASE_DELAY_MS = 500
export const RETRY_MAX_DELAY_MS = 10_000

export const isRetryable = (failure: RetryableFailure) => {
  if (failure instanceof LLMError) return failure.retryable
  return failure.retryable === true && failure.classification !== "context-overflow"
}

export const retryDelay = (failure: RetryableFailure, attempt: number) => {
  if (failure instanceof LLMError && failure.retryAfterMs !== undefined)
    return Effect.succeed(Math.max(failure.retryAfterMs, 0))
  return Random.nextBetween(
    Math.min(RETRY_BASE_DELAY_MS * 2 ** attempt * 0.8, RETRY_MAX_DELAY_MS),
    Math.min(RETRY_BASE_DELAY_MS * 2 ** attempt * 1.2, RETRY_MAX_DELAY_MS),
  ).pipe(Effect.map(Math.round))
}

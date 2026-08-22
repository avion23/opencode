import { describe, expect } from "bun:test"
import { Effect, Fiber, Stream } from "effect"
import * as TestClock from "effect/testing/TestClock"
import { LLM, LLMClient, LLMError, LLMRetry } from "../src"
import * as OpenAIChat from "../src/protocols/openai-chat"
import { Auth } from "../src/route"
import { DEFAULT_STREAM_IDLE_TIMEOUT_MS } from "../src/route/transport/http"
import { dynamicResponse } from "./lib/http"
import { it } from "./lib/effect"

const model = OpenAIChat.route
  .with({ endpoint: { baseURL: "https://api.openai.test/v1/" }, auth: Auth.bearer("test") })
  .model({ id: "gpt-4o-mini" })

const request = LLM.request({ model, prompt: "Say hello." })
const encoder = new TextEncoder()
const keepAlive = ": keep-alive\n\n"

const idleResponse = (onStart: (controller: ReadableStreamDefaultController<Uint8Array>) => void) => {
  let cancelled = false
  const layer = dynamicResponse((input) =>
    Effect.sync(() =>
      input.respond(
        new ReadableStream<Uint8Array>({
          start: onStart,
          cancel: () => {
            cancelled = true
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      ),
    ),
  )
  return { layer, isCancelled: () => cancelled }
}

describe("stream inactivity watchdog", () => {
  it.effect("aborts an idle response and returns a retryable typed error", () => {
    let controller: ReadableStreamDefaultController<Uint8Array> | undefined
    const response = idleResponse((next) => {
      controller = next
    })

    return Effect.gen(function* () {
      const fiber = yield* LLMClient.stream(request).pipe(
        Stream.runDrain,
        Effect.provide(response.layer),
        Effect.forkChild,
      )
      while (!controller) yield* Effect.yieldNow

      yield* TestClock.adjust(DEFAULT_STREAM_IDLE_TIMEOUT_MS + 1)
      yield* Effect.yieldNow
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)

      expect(error).toBeInstanceOf(LLMError)
      if (!(error instanceof LLMError)) return
      expect(error.reason).toMatchObject({
        _tag: "StreamIdleTimeout",
        idleSeconds: DEFAULT_STREAM_IDLE_TIMEOUT_MS / 1000,
      })
      expect(error.message).toContain(`${DEFAULT_STREAM_IDLE_TIMEOUT_MS / 1000} seconds`)
      expect(LLMRetry.isRetryable(error)).toBe(true)
      expect(response.isCancelled()).toBe(true)
    })
  })

  it.effect("resets the timeout for every response body chunk", () => {
    let controller: ReadableStreamDefaultController<Uint8Array> | undefined
    const response = idleResponse((next) => {
      controller = next
    })

    return Effect.gen(function* () {
      const fiber = yield* LLMClient.stream(request).pipe(
        Stream.runDrain,
        Effect.provide(response.layer),
        Effect.forkChild,
      )
      while (!controller) yield* Effect.yieldNow
      if (!controller) throw new Error("response body was not started")

      controller.enqueue(encoder.encode(keepAlive))
      yield* Effect.yieldNow
      yield* TestClock.adjust(DEFAULT_STREAM_IDLE_TIMEOUT_MS - 1)
      yield* Effect.yieldNow
      expect(fiber.pollUnsafe()).toBeUndefined()

      controller.enqueue(encoder.encode(keepAlive))
      yield* Effect.yieldNow
      yield* TestClock.adjust(DEFAULT_STREAM_IDLE_TIMEOUT_MS - 1)
      yield* Effect.yieldNow
      expect(fiber.pollUnsafe()).toBeUndefined()

      yield* TestClock.adjust(1)
      yield* Effect.yieldNow
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)

      expect(error).toBeInstanceOf(LLMError)
      if (!(error instanceof LLMError)) return
      expect(error.reason._tag).toBe("StreamIdleTimeout")
      expect(response.isCancelled()).toBe(true)
    })
  })
})

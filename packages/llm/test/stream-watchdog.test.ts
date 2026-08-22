import { describe, expect } from "bun:test"
import { Effect, Fiber, Stream } from "effect"
import { TestClock } from "effect/testing"
import { LLM, LLMClient, LLMError, LLMRetry } from "../src"
import { OpenAIChat } from "../src/protocols/openai-chat"
import { Auth } from "../src/route"
import { DEFAULT_STREAM_IDLE_TIMEOUT_MS } from "../src/route/transport/http"
import { dynamicResponse } from "./lib/http"
import { it } from "./lib/effect"
import { deltaChunk } from "./lib/openai-chunks"
import { sseEvents } from "./lib/sse"

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

  it.effect("honors a positive environment timeout override", () => {
    const previous = process.env.OPENCODE_STREAM_IDLE_TIMEOUT_MS
    process.env.OPENCODE_STREAM_IDLE_TIMEOUT_MS = "1000"
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

      yield* TestClock.adjust(1001)
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)

      expect(error).toBeInstanceOf(LLMError)
      if (!(error instanceof LLMError)) return
      expect(error.reason).toMatchObject({ _tag: "StreamIdleTimeout", idleSeconds: 1 })
      expect(response.isCancelled()).toBe(true)
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          if (previous === undefined) delete process.env.OPENCODE_STREAM_IDLE_TIMEOUT_MS
          else process.env.OPENCODE_STREAM_IDLE_TIMEOUT_MS = previous
        }),
      ),
    )
  })

  it.effect("does not abort after the response completes", () => {
    const response = idleResponse((controller) => {
      controller.enqueue(encoder.encode(sseEvents(deltaChunk({}, "stop"))))
      controller.close()
    })

    return Effect.gen(function* () {
      const events = yield* LLMClient.stream(request).pipe(Stream.runCollect, Effect.provide(response.layer))
      yield* TestClock.adjust(DEFAULT_STREAM_IDLE_TIMEOUT_MS + 1)

      expect(events.at(-1)?.type).toBe("finish")
      expect(response.isCancelled()).toBe(false)
    })
  })

  it.effect("surfaces an upstream failure and cleans up the armed timer", () => {
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
      controller.error(new Error("upstream failure"))
      const error = yield* Fiber.join(fiber).pipe(Effect.flip)
      yield* TestClock.adjust(DEFAULT_STREAM_IDLE_TIMEOUT_MS + 1)

      expect(error).toBeInstanceOf(LLMError)
      if (!(error instanceof LLMError)) return
      expect(error.reason).toMatchObject({ _tag: "InvalidProviderOutput" })
      if (error.reason._tag === "InvalidProviderOutput") expect(error.reason.raw).toContain("Decode error")
      expect(response.isCancelled()).toBe(false)
    })
  })
})

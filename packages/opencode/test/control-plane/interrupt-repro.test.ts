import { describe, expect, it } from "bun:test"
import { Deferred, Effect, Exit, Fiber, Schedule, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/unstable/http"
import { HttpClient, HttpClientBody, HttpClientRequest } from "effect/unstable/http"
import { Effect as CoreEffect } from "effect"

describe("interrupt repro", () => {
  it.live("Fiber.interrupt kills a fiber blocked in http.execute", () => {
    return Effect.gen(function* () {
      const stealStarted = yield* Deferred.make<void>()
      const finishSteal = yield* Deferred.make<void>()
      yield* HttpServer.serveEffect()(
        Effect.gen(function* () {
          const req = yield* HttpServerRequest.HttpServerRequest
          yield* req.text
          yield* Deferred.succeed(stealStarted, undefined)
          yield* Deferred.await(finishSteal)
          return yield* HttpServerResponse.json({ sessionID: "late" })
        }),
      )
      const url = yield* HttpServer.formatAddress((yield* HttpServer.HttpServer).address)
      const fiber = yield* Effect.gen(function* () {
        const http = yield* HttpClient.HttpClient
        const response = yield* http
          .execute(HttpClientRequest.post(`${url}/sync/steal`, { body: HttpClientBody.json({ sessionID: "s" }) }))
          .pipe(
            Effect.flatMap((response) => Effect.succeed(response.status)),
            Effect.retry(Schedule.spaced("50 millis")),
            Effect.interruptible,
            Effect.timeoutOption("10 seconds"),
          )
        return response
      }).pipe(Effect.forkChild)

      yield* Deferred.await(stealStarted)
      const before = Date.now()
      yield* Fiber.interrupt(fiber)
      const interruptMs = Date.now() - before
      yield* Deferred.succeed(finishSteal, undefined)
      const exit = yield* Fiber.join(fiber)
      const joinMs = Date.now() - interruptMs
      console.log(`interrupt took ${interruptMs}ms, join took ${joinMs}ms, exit interrupted: ${Exit.isInterrupted(exit)}`)
      expect(Exit.isInterrupted(exit)).toBe(true)
    })
  })

  it.live("Fiber.interrupt kills a fiber blocked in raw fetch with signal", () => {
    return Effect.gen(function* () {
      const stealStarted = yield* Deferred.make<void>()
      const finishSteal = yield* Deferred.make<void>()
      yield* HttpServer.serveEffect()(
        Effect.gen(function* () {
          const req = yield* HttpServerRequest.HttpServerRequest
          yield* req.text
          yield* Deferred.succeed(stealStarted, undefined)
          yield* Deferred.await(finishSteal)
          return yield* HttpServerResponse.json({ ok: true })
        }),
      )
      const url = yield* HttpServer.formatAddress((yield* HttpServer.HttpServer).address)
      const fiber = yield* Effect.gen(function* () {
        return yield* Effect.tryPromise({
          try: (signal) => fetch(url, { signal }),
          catch: (error) => error as Error,
        })
      }).pipe(Effect.forkChild)

      yield* Deferred.await(stealStarted)
      const before = Date.now()
      yield* Fiber.interrupt(fiber)
      const interruptMs = Date.now() - before
      yield* Deferred.succeed(finishSteal, undefined)
      const exit = yield* Fiber.join(fiber)
      const joinMs = Date.now() - interruptMs
      console.log(`raw: interrupt took ${interruptMs}ms, join took ${joinMs}ms, exit interrupted: ${Exit.isInterrupted(exit)}`)
      expect(Exit.isInterrupted(exit)).toBe(true)
    })
  })
})

import { afterEach, describe, expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { AbsolutePath } from "@opencode-ai/schema/schema"
import { Effect, Exit, Layer, Option, Queue, Schema, Stream } from "effect"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { EventPaths } from "../../src/server/routes/instance/httpapi/groups/event"
import { eventResponse } from "../../src/server/routes/instance/httpapi/handlers/event"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

const EventData = Schema.Struct({
  id: Schema.optional(Schema.String),
  type: Schema.String,
  properties: Schema.Record(Schema.String, Schema.Any),
})

const OverflowEvent = EventV2.define({
  type: "test.httpapi.overflow",
  schema: { index: Schema.Int },
})

const readEvent = (reader: Queue.Dequeue<Uint8Array>) =>
  Effect.gen(function* () {
    const value = yield* Queue.take(reader).pipe(
      Effect.timeoutOrElse({
        duration: "5 seconds",
        orElse: () => Effect.fail(new Error("timed out waiting for event")),
      }),
    )
    return Schema.decodeUnknownSync(EventData)(JSON.parse(new TextDecoder().decode(value).replace(/^data: /, "")))
  })

const openEventStream = (directory: string) =>
  Effect.gen(function* () {
    const response = yield* requestInDirectory(EventPaths.event, directory)
    const reader = yield* Queue.unbounded<Uint8Array>()
    yield* response.stream.pipe(
      Stream.runForEach((value) => Queue.offer(reader, value)),
      Effect.forkScoped,
    )
    return { response, reader }
  })

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

const it = testEffect(Layer.merge(httpApiLayer, LayerNode.compile(EventV2Bridge.node)))

describe("event HttpApi", () => {
  it.instance(
    "serves event stream",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const { response, reader } = yield* openEventStream(directory)

        expect(response.status).toBe(200)
        expect(response.headers["content-type"]).toContain("text/event-stream")
        expect(response.headers["cache-control"]).toBe("no-cache, no-transform")
        expect(response.headers["x-accel-buffering"]).toBe("no")
        expect(response.headers["x-content-type-options"]).toBe("nosniff")
        expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected", properties: {} })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "keeps the event stream open after the initial event",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const { reader } = yield* openEventStream(directory)
        expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected", properties: {} })

        // If no second event arrives within 250ms, the stream is still open.
        const status = yield* Queue.take(reader).pipe(
          Effect.as("event" as const),
          Effect.timeoutOrElse({ duration: "250 millis", orElse: () => Effect.succeed("open" as const) }),
        )
        expect(status).toBe("open")
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "delivers instance events after the initial event",
    () =>
      Effect.gen(function* () {
        const { directory } = yield* TestInstance
        const { reader } = yield* openEventStream(directory)
        expect(yield* readEvent(reader)).toMatchObject({ type: "server.connected", properties: {} })

        const created = yield* requestInDirectory("/session", directory, { method: "POST" })
        expect(created.status).toBe(200)
        expect(yield* readEvent(reader)).toMatchObject({ type: "session.created" })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "does not overflow an instance subscriber with unrelated location events",
    () =>
      Effect.gen(function* () {
        const events = yield* EventV2Bridge.Service
        const { directory } = yield* TestInstance
        const response = yield* eventResponse(events)

        yield* Effect.forEach(
          Array.from({ length: 512 }, (_, index) => index),
          (index) =>
            events.publish(
              OverflowEvent,
              { index },
              { location: { directory: AbsolutePath.make(`${directory}-unrelated`) } },
            ),
          { discard: true },
        )
        yield* events.publish(OverflowEvent, { index: 512 }, { location: { directory: AbsolutePath.make(directory) } })

        if (response.body._tag !== "Stream") throw new Error("Expected streaming response")
        const chunks = yield* response.body.stream.pipe(Stream.take(2), Stream.runCollect)
        const received = Array.from(chunks, (value) =>
          Schema.decodeUnknownSync(EventData)(JSON.parse(new TextDecoder().decode(value).replace(/^data: /, ""))),
        )
        expect(received).toMatchObject([
          { type: "server.connected", properties: {} },
          { type: OverflowEvent.type, properties: { index: 512 } },
        ])
      }),
    { git: true, config: { formatter: false, lsp: false } },
    { timeout: 30_000 },
  )

  it.instance(
    "fails an overflowing subscriber without affecting another subscriber",
    () =>
      Effect.gen(function* () {
        const events = yield* EventV2Bridge.Service
        const response = yield* eventResponse(events)
        const received = new Array<number>()
        const unsubscribe = yield* events.listen((event) =>
          event.type === OverflowEvent.type
            ? Effect.sync(() => received.push((event.data as { index: number }).index))
            : Effect.void,
        )
        yield* Effect.addFinalizer(() => unsubscribe)

        yield* Effect.forEach(
          Array.from({ length: 512 }, (_, index) => index),
          (index) => events.publish(OverflowEvent, { index }),
          { discard: true },
        )
        expect(received).toEqual(Array.from({ length: 512 }, (_, index) => index))

        if (response.body._tag !== "Stream") throw new Error("Expected streaming response")
        const exit = yield* response.body.stream.pipe(Stream.runDrain, Effect.exit)
        expect(Exit.findErrorOption(exit).pipe(Option.getOrUndefined)).toBeInstanceOf(
          EventV2.SubscriberOverflowError,
        )
      }),
    { git: true, config: { formatter: false, lsp: false } },
    { timeout: 30_000 },
  )
})

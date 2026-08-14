import { describe, expect } from "bun:test"
import { BackgroundJob } from "@opencode-ai/core/background-job"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Schema, Scope } from "effect"
import { TestClock } from "effect/testing"
import { it } from "./lib/effect"

const jobsLayer = LayerNode.compile(BackgroundJob.node)

class MessageLessError extends Schema.TaggedErrorClass<MessageLessError>()("MessageLessError", {}) {}

describe("BackgroundJob", () => {
  it.effect("keeps completed jobs for five minutes before eviction", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({ type: "test", run: Effect.succeed("done") })
      expect(yield* jobs.wait({ id: job.id })).toMatchObject({ info: { status: "completed", output: "done" } })
      yield* Effect.yieldNow

      yield* TestClock.adjust(Duration.millis(299_000))
      expect((yield* jobs.get(job.id))?.status).toBe("completed")

      yield* TestClock.adjust("1 second")
      expect(yield* jobs.get(job.id)).toBeUndefined()
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.effect("keeps cancelled jobs for five minutes before eviction", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({ type: "test", run: Effect.never })
      expect((yield* jobs.cancel(job.id))?.status).toBe("cancelled")
      yield* Effect.yieldNow

      yield* TestClock.adjust(Duration.millis(299_000))
      expect((yield* jobs.get(job.id))?.status).toBe("cancelled")

      yield* TestClock.adjust("1 second")
      expect(yield* jobs.get(job.id)).toBeUndefined()
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.effect("evicts cancelled jobs while finalizers are blocked without evicting restarted jobs", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const blockedID = "job_blocked_cancel"
      const blockedStarted = yield* Deferred.make<void>()
      const blockedFinalizer = yield* Deferred.make<void>()
      const releaseBlockedFinalizer = yield* Deferred.make<void>()
      yield* jobs.start({
        id: blockedID,
        type: "test",
        run: Deferred.succeed(blockedStarted, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Deferred.succeed(blockedFinalizer, undefined).pipe(Effect.andThen(Deferred.await(releaseBlockedFinalizer))),
          ),
        ),
      })
      yield* Deferred.await(blockedStarted)
      const blockedObservation = yield* Effect.gen(function* () {
        yield* Deferred.await(blockedFinalizer)
        yield* TestClock.adjust("5 minutes")
        expect(yield* jobs.get(blockedID)).toBeUndefined()
      }).pipe(Effect.ensuring(Deferred.succeed(releaseBlockedFinalizer, undefined)), Effect.forkChild)
      expect((yield* jobs.cancel(blockedID))?.status).toBe("cancelled")
      yield* Fiber.join(blockedObservation)

      const restartedID = "job_restarted_during_cancel"
      const restartedStarted = yield* Deferred.make<void>()
      const restartedFinalizer = yield* Deferred.make<void>()
      const releaseRestartedFinalizer = yield* Deferred.make<void>()
      yield* jobs.start({
        id: restartedID,
        type: "test",
        run: Deferred.succeed(restartedStarted, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Deferred.succeed(restartedFinalizer, undefined).pipe(
              Effect.andThen(Deferred.await(releaseRestartedFinalizer)),
            ),
          ),
        ),
      })
      yield* Deferred.await(restartedStarted)
      const restartedObservation = yield* Effect.gen(function* () {
        yield* Deferred.await(restartedFinalizer)
        yield* jobs.start({ id: restartedID, type: "test", run: Effect.never })
        yield* TestClock.adjust("5 minutes")
        expect(yield* jobs.get(restartedID)).toMatchObject({ id: restartedID, status: "running" })
      }).pipe(Effect.ensuring(Deferred.succeed(releaseRestartedFinalizer, undefined)), Effect.forkChild)
      expect((yield* jobs.cancel(restartedID))?.status).toBe("cancelled")
      yield* Fiber.join(restartedObservation)
      yield* jobs.cancel(restartedID)
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.effect("does not evict a restarted job when the old timer expires", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const id = "job_restarted"
      yield* jobs.start({ id, type: "test", run: Effect.succeed("first") })
      expect((yield* jobs.wait({ id })).info?.status).toBe("completed")
      yield* Effect.yieldNow
      yield* TestClock.adjust("4 minutes")

      yield* jobs.start({ id, type: "test", run: Effect.never })
      yield* TestClock.adjust("1 minute")

      expect(yield* jobs.get(id)).toMatchObject({ id, status: "running" })
      yield* jobs.cancel(id)
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("renders a tag when a failed error has no message", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const tagged = yield* jobs.start({ type: "test", run: Effect.fail(new MessageLessError()) })
      const withMessage = yield* jobs.start({ type: "test", run: Effect.fail(new Error("real message")) })

      expect((yield* jobs.wait({ id: tagged.id })).info?.error).toBe("MessageLessError")
      expect((yield* jobs.wait({ id: withMessage.id })).info?.error).toBe("real message")
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("renders TimeoutError when a timed-out job settles", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const job = yield* jobs.start({
        type: "test",
        run: Effect.timeoutOrElse(Effect.never, {
          duration: "1 millis",
          orElse: () => Effect.fail(new Cause.TimeoutError()),
        }),
      })

      expect((yield* jobs.wait({ id: job.id, timeout: 1_000 })).info?.error).toBe("TimeoutError")
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("tracks process-local work through explicit observation", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service
      const latch = yield* Deferred.make<void>()
      const job = yield* jobs.start({
        type: "test",
        metadata: { durable: false },
        run: Deferred.await(latch).pipe(Effect.as("done")),
      })

      expect(job).toMatchObject({ type: "test", status: "running", metadata: { durable: false } })
      expect(yield* jobs.wait({ id: job.id, timeout: 0 })).toMatchObject({
        timedOut: true,
        info: { status: "running" },
      })

      yield* Deferred.succeed(latch, undefined)
      expect(yield* jobs.wait({ id: job.id })).toMatchObject({
        timedOut: false,
        info: { status: "completed", output: "done" },
      })
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("publishes jobs before starting immediately settling work", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service

      yield* Effect.forEach(Array.from({ length: 100 }), (_, index) => {
        const id = `job_immediate_start_${index}`
        return Effect.gen(function* () {
          const job = yield* jobs.start({
            id,
            type: "test",
            run: jobs
              .get(id)
              .pipe(
                Effect.flatMap((info) =>
                  info?.status === "running"
                    ? Effect.succeed(`done-${index}`)
                    : Effect.fail("job started before publish"),
                ),
              ),
          })

          expect(yield* jobs.wait({ id: job.id })).toMatchObject({
            timedOut: false,
            info: { status: "completed", output: `done-${index}` },
          })
        })
      })
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("increments pending work before starting immediately settling extensions", () =>
    Effect.gen(function* () {
      const jobs = yield* BackgroundJob.Service

      yield* Effect.forEach(Array.from({ length: 100 }), (_, index) =>
        Effect.gen(function* () {
          const first = yield* Deferred.make<void>()
          const job = yield* jobs.start({
            type: "test",
            run: Deferred.await(first).pipe(Effect.as(`first-${index}`)),
          })

          expect(yield* jobs.extend({ id: job.id, run: Effect.succeed(`second-${index}`) })).toBe(true)
          expect((yield* jobs.get(job.id))?.status).toBe("running")

          yield* Deferred.succeed(first, undefined)
          expect(yield* jobs.wait({ id: job.id })).toMatchObject({
            timedOut: false,
            info: { status: "completed", output: `second-${index}` },
          })
        }),
      )
    }).pipe(Effect.provide(jobsLayer)),
  )

  it.live("interrupts live work without promising settlement after the owning process-local scope closes", () =>
    Effect.gen(function* () {
      const scope = yield* Scope.make()
      const interrupted = yield* Deferred.make<void>()
      const jobs = yield* BackgroundJob.make.pipe(Scope.provide(scope))
      const job = yield* jobs.start({
        type: "test",
        run: Effect.never.pipe(Effect.ensuring(Deferred.succeed(interrupted, undefined))),
      })

      yield* Scope.close(scope, Exit.void)

      yield* Deferred.await(interrupted).pipe(Effect.timeout("1 second"))
      // The abandoned in-memory registry is not a durable observation channel.
      expect((yield* jobs.get(job.id))?.status).toBe("running")
    }),
  )
})

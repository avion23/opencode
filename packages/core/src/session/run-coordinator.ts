export * as SessionRunCoordinator from "./run-coordinator"

import { Cause, Deferred, Effect, Exit, Fiber, FiberSet, Schema, Scope } from "effect"

/** The drain did not settle as success or interrupt-only while being quiesced. */
export class QuiesceError extends Schema.TaggedErrorClass<QuiesceError>()("SessionRunCoordinator.QuiesceError", {
  message: Schema.String,
}) {}

/** A new run was requested while the key's old execution was still quiescing. */
export class QuiescingError extends Schema.TaggedErrorClass<QuiescingError>()(
  "SessionRunCoordinator.QuiescingError",
  { message: Schema.String },
) {}

/** Serializes execution for each key while allowing different keys to run concurrently. */
export interface Coordinator<Key, E> {
  /** Snapshots keys with an execution owned by this coordinator. */
  readonly active: Effect.Effect<ReadonlySet<Key>>
  /** Starts execution while idle or joins the active execution. */
  readonly run: (key: Key) => Effect.Effect<void, E | QuiescingError>
  /** Registers one coalesced follow-up after newly recorded work. */
  readonly wake: (key: Key) => Effect.Effect<void>
  /** Stops active execution and waits for its cleanup. */
  readonly interrupt: (key: Key) => Effect.Effect<void>
  /**
   * Stops active execution, suppresses follow-up wakes, and waits for the drain
   * to settle. The drain is interrupted: a failure it was carrying mid-flight is
   * masked by the interrupt and settles as interrupt-only, so quiesce completes
   * cleanly; a drain that already failed has left the active set and quiesce is
   * a no-op. QuiesceError is reachable only when the drain's exit still carries
   * a failure or defect that survived the interrupt — e.g. one raised in the
   * drain's own onInterrupt/finalizer cleanup.
   */
  readonly quiesce: (key: Key) => Effect.Effect<void, QuiesceError>
}

type Entry<E> = {
  readonly done: Deferred.Deferred<void, E>
  owner?: Fiber.Fiber<void, never>
  pendingWake: boolean
  stopping: boolean
  quiescing: boolean
  settled: boolean
  quiesceWaiters: number
}

export const make = <Key, E>(options: {
  readonly drain: (key: Key, force: boolean) => Effect.Effect<void, E>
}): Effect.Effect<Coordinator<Key, E>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const active = new Map<Key, Entry<E>>()
    // Keys with a quiesce caller still in flight. Outlives the entry: once the
    // drain settles, the entry can be deleted while the quiesce caller has not
    // yet returned, and the flag keeps run/wake fenced until every caller did.
    const quiescingKeys = new Set<Key>()
    const fork = yield* FiberSet.makeRuntime<never, void, never>()

    const makeEntry = (): Entry<E> => ({
      done: Deferred.makeUnsafe<void, E>(),
      pendingWake: false,
      stopping: false,
      quiescing: false,
      settled: false,
      quiesceWaiters: 0,
    })

    const start = (key: Key, entry: Entry<E>, force: boolean, successor = false) => {
      const ready = Deferred.makeUnsafe<void>()
      const owner = fork(
        (successor ? Effect.yieldNow : Deferred.await(ready)).pipe(
          Effect.andThen(Effect.suspend(() => options.drain(key, force))),
          Effect.onExit((exit) => Effect.sync(() => settle(key, entry, exit))),
          Effect.exit,
          Effect.asVoid,
        ),
      )
      entry.owner = owner
      if (!successor) Deferred.doneUnsafe(ready, Effect.void)
    }

    const settle = (key: Key, entry: Entry<E>, exit: Exit.Exit<void, E>) => {
      // A quiescing key never spawns a successor: the late wake that re-armed
      // pendingWake must not resurrect execution after quiesce settles.
      if (entry.quiescing) {
        entry.settled = true
        if (entry.quiesceWaiters === 0) active.delete(key)
        Deferred.doneUnsafe(entry.done, exit)
        return
      }

      if (Exit.isSuccess(exit) && !entry.stopping && entry.pendingWake) {
        entry.pendingWake = false
        start(key, entry, false, true)
        return
      }

      const successor = entry.pendingWake ? makeEntry() : undefined
      if (successor === undefined) active.delete(key)
      else {
        active.set(key, successor)
        start(key, successor, false, true)
      }
      Deferred.doneUnsafe(entry.done, exit)
    }

    const run = (key: Key): Effect.Effect<void, E | QuiescingError> =>
      Effect.uninterruptibleMask((restore): Effect.Effect<void, E | QuiescingError> => {
        // Fenced even when the settled entry was already deleted: a quiesce
        // caller is still in flight for this key, so a new run must not start.
        if (quiescingKeys.has(key))
          return Effect.fail(
            new QuiescingError({ message: `Execution for ${String(key)} is still quiescing` }),
          )
        const entry = active.get(key)
        if (entry !== undefined) {
          if (entry.quiescing)
            return Effect.fail(
              new QuiescingError({ message: `Execution for ${String(key)} is still quiescing` }),
            )
          if (entry.stopping) return restore(Deferred.await(entry.done).pipe(Effect.andThen(run(key))))
          return restore(Deferred.await(entry.done))
        }

        const next = makeEntry()
        active.set(key, next)
        start(key, next, true)
        return restore(Deferred.await(next.done))
      })

    const wake = (key: Key) =>
      Effect.sync(() => {
        // A wake for a key with a quiesce caller in flight is a no-op, even if
        // the settled entry was already deleted: the key is draining toward idle.
        if (quiescingKeys.has(key)) return
        const entry = active.get(key)
        if (entry !== undefined) {
          // Waking a quiescing key is a no-op: the key is draining toward idle.
          if (!entry.quiescing) entry.pendingWake = true
          return
        }

        const next = makeEntry()
        active.set(key, next)
        start(key, next, false)
      })

    const interrupt = (key: Key): Effect.Effect<void> =>
      Effect.suspend(() => {
        const entry = active.get(key)
        if (entry?.owner === undefined) return Effect.void
        entry.stopping = true
        entry.pendingWake = false
        return Fiber.interrupt(entry.owner)
      })

    const quiesce = (key: Key): Effect.Effect<void, QuiesceError> =>
      Effect.suspend(() => {
        const entry = active.get(key)
        if (entry === undefined) return Effect.void
        // Durable quiesced barrier: run/wake stay fenced for this key until the
        // last quiesce caller returns, even after the settled entry is deleted.
        quiescingKeys.add(key)
        entry.quiesceWaiters += 1
        if (!entry.quiescing) {
          entry.quiescing = true
          entry.stopping = true
          entry.pendingWake = false
        }
        const stop = entry.owner === undefined ? Effect.void : Fiber.interrupt(entry.owner)
        // Keep the entry in active until every quiesce caller has returned. This
        // is the barrier that rejects runs and drops wakes after the drain settles.
        const result = stop.pipe(
          Effect.andThen(Deferred.await(entry.done).pipe(Effect.exit)),
          Effect.flatMap((exit) => {
            if (Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)) return Effect.void
            return Effect.fail(
              new QuiesceError({ message: `Execution for ${String(key)} failed to settle while quiescing` }),
            )
          }),
        )
        return result.pipe(
          Effect.ensuring(
            Effect.sync(() => {
              entry.quiesceWaiters -= 1
              if (entry.quiesceWaiters === 0) {
                quiescingKeys.delete(key)
                if (entry.settled && active.get(key) === entry) active.delete(key)
              }
            }),
          ),
        )
      })

    return { active: Effect.sync(() => new Set(active.keys())), run, wake, interrupt, quiesce }
  })

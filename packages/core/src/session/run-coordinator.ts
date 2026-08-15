export * as SessionRunCoordinator from "./run-coordinator"

import { Cause, Deferred, Effect, Exit, Fiber, FiberSet, Schema, Scope } from "effect"

/** The coordinator could not quiesce the key's active execution cleanly. */
export class QuiesceError extends Schema.TaggedErrorClass<QuiesceError>()(
  "SessionRunCoordinator.QuiesceError",
  {
    message: Schema.String,
  },
) {}

/** Serializes execution for each key while allowing different keys to run concurrently. */
export interface Coordinator<Key, E> {
  /** Snapshots keys with an execution owned by this coordinator. */
  readonly active: Effect.Effect<ReadonlySet<Key>>
  /** Starts execution while idle or joins the active execution. */
  readonly run: (key: Key) => Effect.Effect<void, E>
  /** Registers one coalesced follow-up after newly recorded work. */
  readonly wake: (key: Key) => Effect.Effect<void>
  /** Stops active execution and waits for its cleanup. */
  readonly interrupt: (key: Key) => Effect.Effect<void>
  /** Stops active execution, suppresses follow-up wakes, and waits for a clean settle. */
  readonly quiesce: (key: Key) => Effect.Effect<void, QuiesceError>
}

type Entry<E> = {
  readonly done: Deferred.Deferred<void, E>
  owner?: Fiber.Fiber<void, never>
  pendingWake: boolean
  stopping: boolean
  quiescing: boolean
}

export const make = <Key, E>(options: {
  readonly drain: (key: Key, force: boolean) => Effect.Effect<void, E>
}): Effect.Effect<Coordinator<Key, E>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const active = new Map<Key, Entry<E>>()
    const fork = yield* FiberSet.makeRuntime<never, void, never>()

    const makeEntry = (): Entry<E> => ({
      done: Deferred.makeUnsafe<void, E>(),
      pendingWake: false,
      stopping: false,
      quiescing: false,
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
        active.delete(key)
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

    const run = (key: Key): Effect.Effect<void, E> =>
      Effect.uninterruptibleMask((restore) => {
        const entry = active.get(key)
        if (entry !== undefined) {
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
        if (!entry.quiescing) {
          entry.quiescing = true
          entry.stopping = true
          entry.pendingWake = false
        }
        const stop = entry.owner === undefined ? Effect.void : Fiber.interrupt(entry.owner)
        return stop.pipe(
          Effect.andThen(Deferred.await(entry.done).pipe(Effect.exit)),
          Effect.flatMap((exit) =>
            exit._tag === "Success" || (exit._tag === "Failure" && Cause.hasInterruptsOnly(exit.cause))
              ? Effect.void
              : new QuiesceError({ message: `Execution for ${String(key)} failed to settle while quiescing` }),
          ),
        )
      })

    return { active: Effect.sync(() => new Set(active.keys())), run, wake, interrupt, quiesce }
  })

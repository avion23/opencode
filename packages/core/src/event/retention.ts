export * as EventRetention from "./retention"

import { and, eq, inArray, isNull, like, lt } from "drizzle-orm"
import { Duration, Effect, Layer, Schedule } from "effect"
import { Database } from "../database/database"
import { makeGlobalNode } from "../effect/app-node"
import { SessionTable } from "../session/sql"
import { EventTable } from "./sql"

/**
 * Durable `message.part.updated.*` rows are write-only once projected into the `part` table, so bound how long
 * they are retained. Only settled sessions are touched, and only ones never bound to a workspace: the workspace
 * sync replays per-aggregate event streams in strict sequence order, so workspace-bound aggregates must keep their
 * rows contiguous. Unbound streams can become non-replayable after retention.
 */
export const RETENTION = Duration.days(7)
export const BATCH_SIZE = 5000

export const prune = Effect.fn("EventRetention.prune")(function* (options: {
  readonly olderThan?: Duration.Duration
  readonly batch?: number
} = {}) {
  const { db } = yield* Database.Service
  const olderThan = options.olderThan ?? RETENTION
  const batch = options.batch ?? BATCH_SIZE
  const cutoff = Date.now() - Duration.toMillis(olderThan)
  const sessions = yield* db
    .select({ id: SessionTable.id })
    .from(SessionTable)
    .where(and(lt(SessionTable.time_updated, cutoff), isNull(SessionTable.workspace_id)))
    .all()
    .pipe(Effect.orDie)
  let deleted = 0
  for (const session of sessions) {
    while (true) {
      const rows = yield* db
        .delete(EventTable)
        .where(
          and(
            eq(EventTable.aggregate_id, session.id),
            like(EventTable.type, "message.part.updated.%"),
            inArray(
              EventTable.aggregate_id,
              db
                .select({ id: SessionTable.id })
                .from(SessionTable)
                .where(and(lt(SessionTable.time_updated, cutoff), isNull(SessionTable.workspace_id))),
            ),
          ),
        )
        .returning({ id: EventTable.id })
        .limit(batch)
        .all()
        .pipe(Effect.orDie)
      deleted += rows.length
      if (rows.length < batch) break
    }
  }
  return deleted
})

/** Runs the retention sweep once at startup and then on a fixed interval for long-running instances. */
export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* prune()
      .pipe(
        Effect.tap((deleted) => (deleted > 0 ? Effect.logInfo(`pruned settled part-updated events: ${deleted}`) : Effect.void)),
        Effect.catchCause((cause) => Effect.logError("event retention sweep failed", { cause })),
        Effect.repeat(Schedule.spaced(Duration.hours(6))),
        Effect.forkScoped,
      )
  }),
)

export const node = makeGlobalNode({ name: "event-retention", layer, deps: [Database.node] })

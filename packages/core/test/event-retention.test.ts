import { describe, expect } from "bun:test"
import { Duration, Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { EventRetention } from "@opencode-ai/core/event/retention"
import { EventTable } from "@opencode-ai/core/event/sql"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { and, eq, like } from "drizzle-orm"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node])))

const part = (id: string, sessionID: string) => ({
  id: SessionV1.PartID.ascending(id),
  sessionID: SessionV2.ID.make(sessionID),
  messageID: SessionV1.MessageID.ascending(id.replace("prt_", "msg_")),
  type: "text" as const,
  text: "streamed content",
})

const sessionRow = (id: string, timeUpdated: number, workspaceID?: WorkspaceV2.ID) => ({
  id: SessionV2.ID.make(id),
  project_id: ProjectV2.ID.global,
  slug: id,
  directory: "/project",
  title: "retention",
  version: "test",
  workspace_id: workspaceID ?? null,
  time_updated: timeUpdated,
  time_created: timeUpdated,
})

describe("EventRetention", () => {
  it.effect("prunes only part-updated events of fully settled sessions, in bounded batches", () =>
    Effect.gen(function* () {
      const { db } = yield* Database.Service
      const events = yield* EventV2.Service
      const now = Date.now()
      const old = SessionV2.ID.make("ses_retention_old")
      const recent = SessionV2.ID.make("ses_retention_recent")
      const synced = SessionV2.ID.make("ses_retention_synced")
      const workspace = WorkspaceV2.ID.make("wrk_retention")

      yield* db
        .insert(ProjectTable)
        .values({ id: ProjectV2.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
      yield* db
        .insert(SessionTable)
        .values([
          sessionRow(old, now - Duration.toMillis(Duration.days(8))),
          sessionRow(recent, now),
          // Workspace-bound aggregates must keep their rows contiguous for the workspace sync replay.
          sessionRow(synced, now - Duration.toMillis(Duration.days(8)), workspace),
        ])
        .run()

      for (let i = 0; i < 3; i++) {
        yield* events.publish(SessionV1.Event.PartUpdated, {
          sessionID: old,
          part: part(`prt_old_${i}`, old),
          time: now - Duration.toMillis(Duration.days(8)),
        })
      }
      yield* events.publish(SessionV1.Event.PartUpdated, {
        sessionID: recent,
        part: part("prt_recent", recent),
        time: now,
      })
      yield* events.publish(SessionV1.Event.PartUpdated, {
        sessionID: synced,
        part: part("prt_synced", synced),
        time: now - Duration.toMillis(Duration.days(8)),
      })
      // A different durable event type for the same settled session must survive.
      yield* events.publish(SessionV1.Event.PartRemoved, {
        sessionID: old,
        messageID: SessionV1.MessageID.ascending("msg_removed"),
        partID: SessionV1.PartID.ascending("prt_removed"),
      })

      const deleted = yield* EventRetention.prune({ olderThan: Duration.days(7), batch: 2 })

      expect(deleted).toBe(3)
      expect(
        yield* db
          .select({ type: EventTable.type })
          .from(EventTable)
          .where(
            and(
              eq(EventTable.aggregate_id, old),
              like(EventTable.type, "message.part.updated.%"),
            ),
          )
          .all()
          .pipe(Effect.orDie),
      ).toEqual([])
      expect(
        yield* db
          .select({ type: EventTable.type })
          .from(EventTable)
          .where(and(eq(EventTable.aggregate_id, old), eq(EventTable.type, "message.part.removed.1")))
          .all()
          .pipe(Effect.orDie),
      ).toHaveLength(1)
      expect(
        yield* db
          .select({ type: EventTable.type })
          .from(EventTable)
          .where(and(eq(EventTable.aggregate_id, recent), like(EventTable.type, "message.part.updated.%")))
          .all()
          .pipe(Effect.orDie),
      ).toHaveLength(1)
      expect(
        yield* db
          .select({ type: EventTable.type })
          .from(EventTable)
          .where(and(eq(EventTable.aggregate_id, synced), like(EventTable.type, "message.part.updated.%")))
          .all()
          .pipe(Effect.orDie),
      ).toHaveLength(1)
    }),
  )
})

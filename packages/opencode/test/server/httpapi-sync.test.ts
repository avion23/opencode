import { afterEach, describe, expect, mock } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, Effect, Fiber, Layer } from "effect"
import { Flag } from "@opencode-ai/core/flag/flag"
import { SyncPaths } from "../../src/server/routes/instance/httpapi/groups/sync"
import { HttpApiApp } from "../../src/server/routes/instance/httpapi/server"
import { Session } from "@/session/session"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { EventV2 } from "@opencode-ai/core/event"
import { Database } from "@opencode-ai/core/database/database"
import { WorkspaceTable } from "@opencode-ai/core/control-plane/workspace.sql"
import { EventSequenceTable } from "@opencode-ai/core/event/sql"
import { eq } from "drizzle-orm"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, requireInstance, TestInstance } from "../fixture/fixture"
import { withFixedWorkspaceID } from "../fixture/flag"
import { testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

const originalWorkspaces = Flag.OPENCODE_EXPERIMENTAL_WORKSPACES
const context = Context.empty() as Context.Context<unknown>
const it = testEffect(
  Layer.mergeAll(LayerNode.compile(LayerNode.group([Session.node, EventV2.node, Database.node])), httpApiLayer),
)

function insertWorkspaceRow(id: WorkspaceV2.ID) {
  return Effect.gen(function* () {
    const instance = yield* requireInstance
    yield* Database.Service.use(({ db }) =>
      db
        .insert(WorkspaceTable)
        .values({ id, type: "local", project_id: instance.project.id })
        .run()
        .pipe(Effect.orDie),
    )
  })
}

// Durable ownership: the projection (Session.workspaceID) can recover from the
// event stream, but only event_sequence.owner_id fences later steals.
const sequenceOwner = (sessionID: string) =>
  Effect.gen(function* () {
    const rows = yield* Database.Service.use(({ db }) =>
      db
        .select({ owner_id: EventSequenceTable.owner_id })
        .from(EventSequenceTable)
        .where(eq(EventSequenceTable.aggregate_id, sessionID))
        .pipe(Effect.orDie),
    )
    return rows[0]?.owner_id
  })

afterEach(async () => {
  mock.restore()
  Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = originalWorkspaces
  await disposeAllInstances()
  await resetDatabase()
})

describe("sync HttpApi", () => {
  it.instance(
    "serves sync routes",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const workspaceID = WorkspaceV2.ID.ascending()
        yield* withFixedWorkspaceID(workspaceID)
        yield* insertWorkspaceRow(workspaceID)
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const session = yield* Session.use.create({ title: "sync" })
        const unrelated = yield* Session.use.create({ title: "unrelated" })
        yield* Session.use.setTitle({ sessionID: session.id, title: "sync updated" })
        yield* (yield* EventV2.Service).claim(session.id, workspaceID)

        const started = yield* requestInDirectory(SyncPaths.start, tmp.directory, { method: "POST", headers })
        expect(started.status).toBe(200)
        expect(yield* started.json).toBe(true)

        const history = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ scope: "aggregate", state: { [session.id]: 0 } }),
        })
        expect(history.status).toBe(200)
        const rows = (yield* history.json) as Array<{
          id: string
          aggregate_id: string
          seq: number
          type: string
          data: Record<string, unknown>
        }>
        expect(rows.map((row) => row.aggregate_id)).toEqual([session.id])
        expect(rows.map((row) => row.aggregate_id)).not.toContain(unrelated.id)

        const replayed = yield* requestInDirectory(SyncPaths.replay, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({
            directory: tmp.directory,
            ownerID: workspaceID,
            events: rows
              .filter((row) => row.aggregate_id === session.id)
              .map((row) => ({
                id: row.id,
                aggregateID: row.aggregate_id,
                seq: row.seq,
                type: row.type,
                data: row.data,
              })),
          }),
        })
        expect(replayed.status).toBe(200)
        expect(yield* replayed.json).toEqual({ sessionID: session.id })
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "waits for an aggregate commit before returning its history",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const workspaceID = WorkspaceV2.ID.ascending()
        yield* withFixedWorkspaceID(workspaceID)
        yield* insertWorkspaceRow(workspaceID)
        const events = yield* EventV2.Service
        const session = yield* Session.use.create({ title: "sync history lock" })

        const request = yield* events.exclusive(
          session.id,
          Effect.gen(function* () {
            const fiber = yield* requestInDirectory(SyncPaths.history, tmp.directory, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ scope: "aggregate", state: { [session.id]: 0 } }),
            }).pipe(Effect.forkChild)
            yield* Effect.sleep("100 millis")
            expect((yield* Fiber.join(fiber).pipe(Effect.timeoutOption("50 millis")))._tag).toBe("None")
            yield* Session.use.setTitle({ sessionID: session.id, title: "committed" })
            yield* events.claim(session.id, workspaceID)
            return fiber
          }),
        )

        const response = yield* Fiber.join(request)
        expect(response.status).toBe(200)
        const rows = (yield* response.json) as Array<{ aggregate_id: string; seq: number }>
        expect(rows).toMatchObject([{ aggregate_id: session.id, seq: 1 }])
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "validates seq values",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const cases = [
          {
            path: SyncPaths.history,
            body: { aggregate: -1 },
          },
          {
            path: SyncPaths.history,
            body: { aggregate: 1.5 },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: -1, type: "session.created", data: {} }],
            },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: 1.5, type: "session.created", data: {} }],
            },
          },
          {
            path: SyncPaths.replay,
            body: {
              directory: tmp.directory,
              events: [{ id: "event", aggregateID: "session", seq: 0, type: "session.created", data: {} }],
            },
          },
        ]

        for (const item of cases) {
          const response = yield* requestInDirectory(item.path, tmp.directory, {
            method: "POST",
            headers,
            body: JSON.stringify(item.body),
          })
          expect(response.status).toBe(400)
        }
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "rejects stale session steals",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const headers = { "x-opencode-directory": tmp.directory, "content-type": "application/json" }
        const session = yield* Session.use.create({ title: "sync steal" })
        const workspaceID = WorkspaceV2.ID.ascending()
        const warpID = EventV2.ID.create()
        yield* withFixedWorkspaceID(workspaceID)
        yield* insertWorkspaceRow(workspaceID)
        yield* (yield* EventV2.Service).claim(session.id, workspaceID)

        const stale = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ sessionID: session.id, seq: 1, warpID, ownerID: workspaceID }),
        })
        expect(stale.status).toBe(409)
        expect((yield* Session.use.get(session.id)).workspaceID).toBeUndefined()
        expect(yield* sequenceOwner(session.id)).toBe(workspaceID)

        const committed = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ sessionID: session.id, seq: 0, warpID, ownerID: workspaceID }),
        })
        expect(committed.status).toBe(200)
        const result = (yield* committed.json) as { sessionID: string; event: { id: string; seq: number } }
        expect(result).toMatchObject({ sessionID: session.id, event: { id: warpID, seq: 1 } })
        expect((yield* Session.use.get(session.id)).workspaceID).toBe(workspaceID)

        const retried = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ sessionID: session.id, seq: 0, warpID, ownerID: workspaceID }),
        })
        expect(retried.status).toBe(200)
        expect(yield* retried.json).toEqual(result)

        // D5: a warp commit is only recoverable by its exact warp event. A
        // fresh warpID after the warp already committed is a conflict, and
        // ownership survives the rejected steal.
        const recovered = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
          method: "POST",
          headers,
          body: JSON.stringify({ sessionID: session.id, seq: 0, warpID: EventV2.ID.create(), ownerID: workspaceID }),
        })
        expect(recovered.status).toBe(409)
        expect((yield* Session.use.get(session.id)).workspaceID).toBe(workspaceID)
        expect(yield* sequenceOwner(session.id)).toBe(workspaceID)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance(
    "does not hold the database write lock while waiting to steal a session",
    () =>
      Effect.gen(function* () {
        Flag.OPENCODE_EXPERIMENTAL_WORKSPACES = true
        const tmp = yield* TestInstance
        const events = yield* EventV2.Service
        const session = yield* Session.use.create({ title: "locked steal" })
        const unrelated = yield* Session.use.create({ title: "unrelated" })
        const workspaceID = WorkspaceV2.ID.ascending()
        yield* withFixedWorkspaceID(workspaceID)
        yield* insertWorkspaceRow(workspaceID)
        yield* events.claim(session.id, workspaceID)

        const request = yield* events.exclusive(
          session.id,
          Effect.gen(function* () {
            const fiber = yield* requestInDirectory(SyncPaths.steal, tmp.directory, {
              method: "POST",
              headers: { "x-opencode-directory": tmp.directory, "content-type": "application/json" },
              body: JSON.stringify({ sessionID: session.id, seq: 0, warpID: EventV2.ID.create(), ownerID: workspaceID }),
            }).pipe(Effect.forkChild)
            yield* Effect.sleep("100 millis")
            expect((yield* Fiber.join(fiber).pipe(Effect.timeoutOption("50 millis")))._tag).toBe("None")
            expect(
              (yield* Session.use
                .setTitle({ sessionID: unrelated.id, title: "write completed" })
                .pipe(Effect.timeoutOption("500 millis")))._tag,
            ).toBe("Some")
            return fiber
          }),
        )

        expect((yield* Fiber.join(request)).status).toBe(200)
        expect((yield* Session.use.get(unrelated.id)).title).toBe("write completed")
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )

  it.instance.skip(
    "returns structured validation errors",
    () =>
      Effect.gen(function* () {
        const tmp = yield* TestInstance
        const response = yield* Effect.promise(() =>
          HttpApiApp.webHandler().handler(
            new Request(`http://localhost${SyncPaths.history}`, {
              method: "POST",
              headers: { "x-opencode-directory": tmp.directory, "content-type": "application/json" },
              body: JSON.stringify({ aggregate: -1 }),
            }),
            context,
          ),
        )

        expect(response.status).toBe(400)
        expect(response.headers.get("content-type") ?? "").toContain("application/json")
        const body = (yield* Effect.promise(() => response.json())) as Record<string, unknown>
        expect(body.success).toBe(false)
        expect(Array.isArray(body.error) || Array.isArray(body.errors)).toBe(true)
      }),
    { git: true, config: { formatter: false, lsp: false } },
  )
})
